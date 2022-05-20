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

//TODO
func parseMap() {

}

type NamedInterface interface {
	Name() string
}

type JSON interface {
	upsertValue(name string, kind reflect.Kind, isArray bool) (*JSONValue, error)
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

func parseSliceStruct(name string, structVal reflect.Value, jCol JSON) error {
	col, err := jCol.upsertList(name)
	if err != nil {
		return err
	}
	for i := 0; i < structVal.NumField(); i++ {
		field := structVal.Field(i)
		kind := field.Kind()
		fName := structVal.Type().Field(i).Name
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
		//TODO: we need to process all
		if rValues.Len() > 0 {
			return parseSliceStruct(name, rValues.Index(0), jCol)
		}
		return nil
	} else {
		return parseType(name, sKind, values, true, jCol)
	}
	return &UnsupportedColumnTypeError{
		t: Type(fmt.Sprint(sKind)),
	}
}

func parseStruct(name string, structVal reflect.Value, jCol JSON) error {
	col, err := jCol.upsertObject(name)
	if err != nil {
		return err
	}
	for i := 0; i < structVal.NumField(); i++ {
		// handle the fields in the struct
		field := structVal.Field(i)
		kind := field.Kind()
		fName := structVal.Type().Field(i).Name
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
			// handle the fields in the struct
			field := rStruct.Field(i)
			kind := field.Kind()
			fName := rStruct.Type().Field(i).Name
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

type JSONValue struct {
	col  Interface
	name string
}

func (jCol *JSONValue) Name() string {
	return jCol.name
}

func (jCol *JSONValue) Type() Type {
	return Type(fmt.Sprintf("%s %s", jCol.name, jCol.col.Type()))
}

func (jCol *JSONValue) Rows() int {
	return jCol.col.Rows()
}

func (jCol *JSONValue) Row(i int, ptr bool) interface{} {
	return jCol.col.Row(i, ptr)
}

func (jCol *JSONValue) ScanRow(dest interface{}, row int) error {
	return jCol.col.ScanRow(dest, row)
}

func (jCol *JSONValue) Append(v interface{}) (nulls []uint8, err error) {
	return jCol.col.Append(v)
}

func (jCol *JSONValue) AppendRow(v interface{}) error {
	return jCol.col.AppendRow(v)
}

func (jCol *JSONValue) Decode(decoder *binary.Decoder, rows int) error {
	return jCol.col.Decode(decoder, rows)
}

func (jCol *JSONValue) Encode(encoder *binary.Encoder) error {
	return jCol.col.Encode(encoder)
}

func (jCol *JSONValue) ScanType() reflect.Type {
	return jCol.col.ScanType()
}

type JSONList struct {
	Array
	name string
}

func (jCol *JSONList) Name() string {
	return jCol.name
}

func (jCol *JSONList) upsertValue(name string, kind reflect.Kind, isArray bool) (*JSONValue, error) {
	// lists are represented as Nested which are in turn encoded as Array(Tuple()). We thus pass a Array(JSONObject())
	// as this encodes like a tuple
	ct, ok := typeMapping[kind]
	if !ok {
		return nil, &UnsupportedColumnTypeError{
			t: Type(fmt.Sprint(kind)),
		}
	}
	if isArray {
		ct = fmt.Sprintf("Array(%s)", typeMapping[kind])
	}
	col, err := Type(ct).Column()
	if err != nil {
		return nil, err
	}
	vCol := &JSONValue{
		col:  col,
		name: name,
	}
	if jCol.values == nil {
		//first insert
		jCol.values = &JSONObject{
			columns: []Interface{vCol},
		}
		return vCol, nil
	}
	// check if column exists and reuse if same type, error if same name and different type
	cols := jCol.values.(*JSONObject).columns
	for i := range cols {
		sCol, ok := cols[i].(NamedInterface)
		if !ok {
			return nil, &UnsupportedColumnTypeError{
				t: Type(fmt.Sprint(reflect.TypeOf(cols[i]).Kind())),
			}
		}
		if sCol.Name() == name {
			sCol, ok := cols[i].(*JSONValue)
			if !ok {
				return nil, &Error{
					ColumnType: fmt.Sprint(reflect.ValueOf(sCol).Kind()),
					Err:        fmt.Errorf("type mismatch in column %s", name),
				}
			}
			if sCol.col.Type() != Type(ct) {
				return nil, &Error{
					ColumnType: fmt.Sprint(reflect.ValueOf(sCol).Kind()),
					Err:        fmt.Errorf("type mismatch in column %s", name),
				}
			}
			return sCol, nil
		}
	}
	jCol.values.(*JSONObject).columns = append(cols, vCol)
	return vCol, nil
}

func (jCol *JSONList) upsertList(name string) (*JSONList, error) {
	// lists are represented as Nested which are in turn encoded as Array(Tuple()). We thus pass a Array(JSONObject())
	// as this encodes like a tuple
	vCol := &JSONList{
		name: name,
	}
	// depth should always be one as nested arrays aren't possible
	vCol.depth = 1
	if jCol.values == nil {
		//first insert
		jCol.values = &JSONObject{
			columns: []Interface{vCol},
		}
		return vCol, nil
	}
	// check if column exists and reuse if same type, error if same name and different type
	cols := jCol.values.(*JSONObject).columns
	for i := range cols {
		sCol, ok := cols[i].(NamedInterface)
		if !ok {
			return nil, &UnsupportedColumnTypeError{
				t: Type(fmt.Sprint(reflect.TypeOf(cols[i]).Kind())),
			}
		}
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
	jCol.values.(*JSONObject).columns = append(cols, vCol)
	return vCol, nil

}

func (jCol *JSONList) upsertObject(name string) (*JSONObject, error) {
	// lists are represented as Nested which are in turn encoded as Array(Tuple()). We thus pass a Array(JSONObject())
	// as this encodes like a tuple
	vCol := &JSONObject{
		columns: make([]Interface, 0),
		name:    name,
	}
	if jCol.values == nil {
		//first insert
		jCol.values = &JSONObject{
			columns: []Interface{vCol},
		}
		return vCol, nil
	}
	// check if column exists and reuse if same type, error if same name and different type
	cols := jCol.values.(*JSONObject).columns
	for i := range cols {
		sCol, ok := cols[i].(NamedInterface)
		if !ok {
			return nil, &UnsupportedColumnTypeError{
				t: Type(fmt.Sprint(reflect.TypeOf(cols[i]).Kind())),
			}
		}
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
	jCol.values.(*JSONObject).columns = append(cols, vCol)
	return vCol, nil
}

func (jCol *JSONList) Type() Type {
	cols := jCol.values.(*JSONObject).columns
	subTypes := make([]string, len(cols))
	for i, v := range cols {
		subTypes[i] = string(v.Type())
	}
	return Type(fmt.Sprintf("%s Nested(%s)", jCol.name, strings.Join(subTypes, ", ")))
}

func (jCol *JSONList) Encode(encoder *binary.Encoder) error {
	// update our offsets as we're now encoding the batch and we have all objects
	err := jCol.updateOffsets()
	if err != nil {
		return err
	}
	for _, offset := range jCol.offsets {
		if err := offset.values.Encode(encoder); err != nil {
			return err
		}
	}
	return jCol.values.Encode(encoder)
}

func (jCol *JSONList) updateOffsets() error {
	offsetScanTypes := make([]reflect.Type, 0, jCol.depth)
	jCol.offsets, jCol.scanType = make([]*offset, 0, jCol.depth), jCol.values.ScanType()
	for i := 0; i < jCol.depth; i++ {
		jCol.scanType = reflect.SliceOf(jCol.scanType)
		offsetScanTypes = append(offsetScanTypes, jCol.scanType)
	}
	for i := len(offsetScanTypes) - 1; i >= 0; i-- {
		jCol.offsets = append(jCol.offsets, &offset{
			scanType: offsetScanTypes[i],
		})
	}
	return nil
}

type JSONObject struct {
	columns []Interface
	name    string
}

func (jCol *JSONObject) Name() string {
	return jCol.name
}

func (jCol *JSONObject) upsertList(name string) (*JSONList, error) {
	for i := range jCol.columns {
		sCol, ok := jCol.columns[i].(NamedInterface)
		if !ok {
			return nil, &UnsupportedColumnTypeError{
				t: Type(fmt.Sprint(reflect.TypeOf(jCol.columns[i]).Kind())),
			}
		}
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
	// not present so create
	col := &JSONList{
		name: name,
	}
	// depth should always be one as nested arrays aren't possible
	col.depth = 1
	jCol.columns = append(jCol.columns, col)
	return col, nil
}

func (jCol *JSONObject) upsertObject(name string) (*JSONObject, error) {
	// check if it exists
	for i := range jCol.columns {
		sCol, ok := jCol.columns[i].(NamedInterface)
		if !ok {
			return nil, &UnsupportedColumnTypeError{
				t: Type(fmt.Sprint(reflect.TypeOf(jCol.columns[i]).Kind())),
			}
		}
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
	col := &JSONObject{
		columns: make([]Interface, 0),
		name:    name,
	}
	jCol.columns = append(jCol.columns, col)
	return col, nil
}

func (jCol *JSONObject) upsertValue(name string, kind reflect.Kind, isArray bool) (*JSONValue, error) {
	if ct, ok := typeMapping[kind]; ok {
		if isArray {
			ct = fmt.Sprintf("Array(%s)", ct)
		}
		for i := range jCol.columns {
			sCol, ok := jCol.columns[i].(NamedInterface)
			if !ok {
				return nil, &UnsupportedColumnTypeError{
					t: Type(fmt.Sprint(reflect.TypeOf(jCol.columns[i]).Kind())),
				}
			}
			if sCol.Name() == name {
				sCol, ok := jCol.columns[i].(*JSONValue)
				if !ok {
					return nil, &Error{
						ColumnType: fmt.Sprint(reflect.ValueOf(sCol).Kind()),
						Err:        fmt.Errorf("type mismatch in column %s", name),
					}
				}
				if sCol.col.Type() != Type(ct) {
					return nil, &Error{
						ColumnType: fmt.Sprint(reflect.ValueOf(sCol).Kind()),
						Err:        fmt.Errorf("type mismatch in column %s", name),
					}
				}
				return sCol, nil
			}
		}
		col, err := Type(ct).Column()
		if err != nil {
			return nil, err
		}
		vCol := &JSONValue{
			col:  col,
			name: name,
		}
		jCol.columns = append(jCol.columns, vCol)
		return vCol, nil
	}

	return nil, &UnsupportedColumnTypeError{
		t: Type(fmt.Sprint(kind)),
	}
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

func (jCol *JSONObject) Append(v interface{}) (nulls []uint8, err error) {
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
	_ Interface           = (*JSONValue)(nil)
	_ CustomSerialization = (*JSONObject)(nil)
)
