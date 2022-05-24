// Licensed to ClickHouse, Inc. under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. ClickHouse, Inc. licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package column

import (
	"fmt"
	"github.com/ClickHouse/clickhouse-go/v2/lib/binary"
	"reflect"
	"strings"
)

// inverse mapping - go types to clickhouse types
var typeMapping = map[reflect.Kind]string{
	reflect.String:  "String",
	reflect.Int:     "Int64",
	reflect.Int8:    "Int8",
	reflect.Int16:   "Int16",
	reflect.Int32:   "Int32",
	reflect.Int64:   "Int64",
	reflect.Uint:    "UInt64",
	reflect.Uint8:   "UInt8",
	reflect.Uint16:  "UInt16",
	reflect.Uint32:  "UInt32",
	reflect.Uint64:  "UInt64",
	reflect.Float32: "Float32",
	reflect.Float64: "Float64",
	reflect.Bool:    "Boolean",
}

type JSON interface {
	upsertValue(name string, kind reflect.Kind, isArray bool) (Interface, error)
	upsertList(name string) (*JSONList, error)
	upsertObject(name string) (*JSONObject, error)
}

func parseType(name string, kind reflect.Kind, values interface{}, isArray bool, jCol JSON) error {
	col, err := jCol.upsertValue(name, kind, isArray)
	if err != nil {
		return err
	}
	return col.AppendRow(values)
}

func (jCol *JSONList) createNewOffset() {
	//single depth so can take 1st
	if len(jCol.offsets[0].values.data) == 0 {
		// first entry in the column
		jCol.offsets[0].values.data = []uint64{0}
	} else {
		// entry for this object to see offset from last - offsets are cumulative
		jCol.offsets[0].values.data = append(jCol.offsets[0].values.data, jCol.offsets[0].values.data[len(jCol.offsets[0].values.data)-1])
	}
}

func getFieldName(field reflect.StructField) (string, bool) {
	name := field.Name
	jsonTag := field.Tag.Get("json")
	if jsonTag == "" {
		return name, false
	}
	// not a standard but we allow - to omit fields
	if jsonTag == "-" {
		return name, true
	}
	return jsonTag, false
}

// returns offset - 	col.Array.offsets[0].values
func parseSliceStruct(name string, structVal reflect.Value, jCol JSON, first bool) error {
	col, err := jCol.upsertList(name)
	if err != nil {
		return err
	}
	if first {
		col.createNewOffset()
	}
	// increment offset
	col.offsets[0].values.data[len(col.offsets[0].values.data)-1] += 1
	for i := 0; i < structVal.NumField(); i++ {
		fName, omit := getFieldName(structVal.Type().Field(i))
		if omit {
			continue
		}
		field := structVal.Field(i)
		kind := field.Kind()
		value := field.Interface()
		if kind == reflect.Struct {
			err := parseStruct(fName, field, col)
			if err != nil {
				return err
			}
		} else if kind == reflect.Slice {
			err := parseSlice(fName, value, col)
			if err != nil {
				return err
			}
		} else {
			err := parseType(fName, kind, value, false, col)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func parseSlice(name string, values interface{}, jCol JSON) error {
	sKind := reflect.TypeOf(values).Elem().Kind()
	if sKind == reflect.Struct {
		rValues := reflect.ValueOf(values)
		if rValues.Len() == 0 {
			//still need to compute an offset
			col, err := jCol.upsertList(name)
			if err != nil {
				return err
			}
			col.createNewOffset()
		}
		for i := 0; i < rValues.Len(); i++ {
			err := parseSliceStruct(name, rValues.Index(i), jCol, i == 0)
			if err != nil {
				return err
			}
		}
		return nil
	} else {
		return parseType(name, sKind, values, true, jCol)
	}
}

func parseStruct(name string, structVal reflect.Value, jCol JSON) error {
	col, err := jCol.upsertObject(name)
	if err != nil {
		return err
	}
	for i := 0; i < structVal.NumField(); i++ {
		fName, omit := getFieldName(structVal.Type().Field(i))
		if omit {
			continue
		}
		field := structVal.Field(i)
		kind := field.Kind()
		value := field.Interface()
		if kind == reflect.Struct {
			err = parseStruct(fName, field, col)
			if err != nil {
				return err
			}
		} else if kind == reflect.Slice {
			err := parseSlice(fName, value, col)
			if err != nil {
				return err
			}
		} else {
			err := parseType(fName, kind, value, false, col)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func AppendStruct(jCol *JSONObject, data interface{}) error {
	kind := reflect.ValueOf(data).Kind()
	if kind == reflect.Struct {
		rStruct := reflect.ValueOf(data)
		for i := 0; i < rStruct.NumField(); i++ {
			fName, omit := getFieldName(rStruct.Type().Field(i))
			if omit {
				continue
			}
			// handle the fields in the struct
			field := rStruct.Field(i)
			kind := field.Kind()
			value := field.Interface()
			if kind == reflect.Struct {
				err := parseStruct(fName, field, jCol)
				if err != nil {
					return err
				}
			} else if kind == reflect.Slice {
				err := parseSlice(fName, value, jCol)
				if err != nil {
					return err
				}
			} else {
				err := parseType(fName, kind, value, false, jCol)
				if err != nil {
					return err
				}
			}
		}
		return nil
	}
	return &UnsupportedColumnTypeError{
		t: Type(fmt.Sprint(kind)),
	}
}

type JSONList struct {
	Array
	name string
}

func (jCol *JSONList) Name() string {
	return jCol.name
}

func createJSONList(name string) (jCol *JSONList) {
	// lists are represented as Nested which are in turn encoded as Array(Tuple()). We thus pass a Array(JSONObject())
	// as this encodes like a tuple
	lCol := &JSONList{
		name: name,
	}
	lCol.values = &JSONObject{}
	// depth should always be one as nested arrays aren't possible
	lCol.depth = 1
	lCol.scanType = reflect.SliceOf(lCol.values.ScanType())
	offsetScanTypes := []reflect.Type{lCol.scanType}
	lCol.offsets = []*offset{{
		scanType: offsetScanTypes[0],
	}}
	return lCol
}

func (jCol *JSONList) upsertValue(name string, kind reflect.Kind, isArray bool) (Interface, error) {
	// lists are represented as Nested which are in turn encoded as Array(Tuple()). We thus pass a Array(JSONObject())
	// as this encodes like a tuple
	ct, ok := typeMapping[kind]
	if !ok {
		return nil, &UnsupportedColumnTypeError{
			t: Type(fmt.Sprint(kind)),
		}
	}
	if isArray {
		ct = fmt.Sprintf("Array(%s)", ct)
	}

	// check if column exists and reuse if same type, error if same name and different type
	cols := jCol.values.(*JSONObject).columns
	for i := range cols {
		sCol := cols[i]
		if sCol.Name() == name {
			if sCol.Type() != Type(ct) {
				return nil, &Error{
					ColumnType: fmt.Sprint(reflect.ValueOf(sCol).Kind()),
					Err:        fmt.Errorf("type mismatch in column %s", name),
				}
			}
			return sCol, nil
		}
	}
	col, err := Type(ct).Column(name)
	if err != nil {
		return nil, err
	}
	jCol.values.(*JSONObject).columns = append(cols, col)
	return col, nil
}

func (jCol *JSONList) upsertList(name string) (*JSONList, error) {
	// check if column exists and reuse if same type, error if same name and different type
	cols := jCol.values.(*JSONObject).columns
	for i := range cols {
		sCol := cols[i]
		if sCol.Name() == name {
			sCol, ok := cols[i].(*JSONList)
			if !ok {
				return nil, &Error{
					ColumnType: fmt.Sprint(reflect.ValueOf(sCol).Kind()),
					Err:        fmt.Errorf("type mismatch in column %s", name),
				}
			}
			return sCol, nil
		}
	}
	lCol := createJSONList(name)
	jCol.values.(*JSONObject).columns = append(cols, lCol)
	return lCol, nil

}

func (jCol *JSONList) upsertObject(name string) (*JSONObject, error) {
	// check if column exists and reuse if same type, error if same name and different type
	cols := jCol.values.(*JSONObject).columns
	for i := range cols {
		sCol := cols[i]
		if sCol.Name() == name {
			sCol, ok := cols[i].(*JSONObject)
			if !ok {
				return nil, &Error{
					ColumnType: fmt.Sprint(reflect.ValueOf(sCol).Kind()),
					Err:        fmt.Errorf("type mismatch in column %s", name),
				}
			}
			return sCol, nil
		}
	}
	// lists are represented as Nested which are in turn encoded as Array(Tuple()). We thus pass a Array(JSONObject())
	// as this encodes like a tuple
	oCol := &JSONObject{
		name: name,
	}
	jCol.values.(*JSONObject).columns = append(cols, oCol)
	return oCol, nil
}

func (jCol *JSONList) Type() Type {
	cols := jCol.values.(*JSONObject).columns
	subTypes := make([]string, len(cols))
	for i, v := range cols {
		subTypes[i] = string(v.Type())
	}
	return Type(fmt.Sprintf("%s Nested(%s)", jCol.name, strings.Join(subTypes, ", ")))
}

type JSONObject struct {
	columns []Interface
	name    string
}

func (jCol *JSONObject) Name() string {
	return jCol.name
}

func (jCol *JSONObject) upsertValue(name string, kind reflect.Kind, isArray bool) (Interface, error) {
	ct, ok := typeMapping[kind]
	if !ok {
		return nil, &UnsupportedColumnTypeError{
			t: Type(fmt.Sprint(kind)),
		}
	}
	if isArray {
		ct = fmt.Sprintf("Array(%s)", ct)
	}
	for i := range jCol.columns {
		sCol := jCol.columns[i]
		if sCol.Name() == name {
			if sCol.Type() != Type(ct) {
				return nil, &Error{
					ColumnType: fmt.Sprint(reflect.ValueOf(sCol).Kind()),
					Err:        fmt.Errorf("type mismatch in column %s", name),
				}
			}
			return sCol, nil
		}
	}

	col, err := Type(ct).Column(name)
	if err != nil {
		return nil, err
	}
	jCol.columns = append(jCol.columns, col)
	return col, nil
}

func (jCol *JSONObject) upsertList(name string) (*JSONList, error) {
	for i := range jCol.columns {
		sCol := jCol.columns[i]
		if sCol.Name() == name {
			sCol, ok := jCol.columns[i].(*JSONList)
			if !ok {
				return nil, &Error{
					ColumnType: fmt.Sprint(reflect.ValueOf(sCol).Kind()),
					Err:        fmt.Errorf("type mismatch in column %s", name),
				}
			}
			return sCol, nil
		}
	}
	lCol := createJSONList(name)
	jCol.columns = append(jCol.columns, lCol)
	return lCol, nil
}

func (jCol *JSONObject) upsertObject(name string) (*JSONObject, error) {
	// check if it exists
	for i := range jCol.columns {
		sCol := jCol.columns[i]
		if sCol.Name() == name {
			sCol, ok := jCol.columns[i].(*JSONObject)
			if !ok {
				return nil, &Error{
					ColumnType: fmt.Sprint(reflect.ValueOf(sCol).Kind()),
					Err:        fmt.Errorf("type mismatch in column %s", name),
				}
			}
			return sCol, nil
		}
	}
	// not present so create
	oCol := &JSONObject{
		name: name,
	}
	jCol.columns = append(jCol.columns, oCol)
	return oCol, nil
}

func (jCol *JSONObject) Type() Type {
	subTypes := make([]string, len(jCol.columns))
	for i, v := range jCol.columns {
		subTypes[i] = string(v.Type())
	}
	if jCol.name != "" {
		return Type(fmt.Sprintf("%s Tuple(%s)", jCol.name, strings.Join(subTypes, ", ")))
	}
	return Type(fmt.Sprintf("Tuple(%s)", strings.Join(subTypes, ", ")))
}

func (jCol *JSONObject) ScanType() reflect.Type {
	return scanTypeSlice
}

func (jCol *JSONObject) Rows() int {
	if len(jCol.columns) != 0 {
		return jCol.columns[0].Rows()
	}
	return 0
}

func (jCol *JSONObject) Row(i int, ptr bool) interface{} {
	tuple := make([]interface{}, 0, len(jCol.columns))
	for _, c := range jCol.columns {
		tuple = append(tuple, c.Row(i, ptr))
	}
	return tuple
}

func (jCol *JSONObject) ScanRow(dest interface{}, row int) error {
	switch d := dest.(type) {
	case *[]interface{}:
		tuple := make([]interface{}, 0, len(jCol.columns))
		for _, c := range jCol.columns {
			tuple = append(tuple, c.Row(row, false))
		}
		*d = tuple
	default:
		return &ColumnConverterError{
			Op:   "ScanRow",
			To:   fmt.Sprintf("%T", dest),
			From: string(jCol.Type()),
		}
	}
	return nil
}

func (jCol *JSONObject) Append(_ interface{}) (nulls []uint8, err error) {
	panic("Implement me")
}

func (jCol *JSONObject) AppendRow(v interface{}) error {
	if reflect.ValueOf(v).Kind() == reflect.Struct {
		return AppendStruct(jCol, v)
	}
	// TODO: support strings and maps
	return &Error{
		ColumnType: fmt.Sprint(reflect.ValueOf(v).Kind()),
		Err:        fmt.Errorf("unsupported error"),
	}
}

func (jCol *JSONObject) Decode(decoder *binary.Decoder, rows int) error {
	for _, c := range jCol.columns {
		if err := c.Decode(decoder, rows); err != nil {
			return err
		}
	}
	return nil
}

func (jCol *JSONObject) Encode(encoder *binary.Encoder) error {
	for _, c := range jCol.columns {
		if err := c.Encode(encoder); err != nil {
			return err
		}
	}
	return nil
}

func (jCol *JSONObject) ReadStatePrefix(decoder *binary.Decoder) error {
	for _, c := range jCol.columns {
		if serialize, ok := c.(CustomSerialization); ok {
			if err := serialize.ReadStatePrefix(decoder); err != nil {
				return err
			}
		}
	}
	return nil
}

func (jCol *JSONObject) WriteStatePrefix(encoder *binary.Encoder) error {
	for _, c := range jCol.columns {
		if serialize, ok := c.(CustomSerialization); ok {
			if err := serialize.WriteStatePrefix(encoder); err != nil {
				return err
			}
		}
	}
	return nil
}

var (
	_ Interface           = (*JSONObject)(nil)
	_ CustomSerialization = (*JSONObject)(nil)
)
