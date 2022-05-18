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

func parseMap() {

}

func parseType(name string, kind reflect.Kind) (*JSONValue, error) {
	if ct, ok := typeMapping[kind]; ok {
		col, err := Type(ct).Column()
		if err != nil {
			return nil, err
		}
		jsonCol := JSONValue{
			col:  col,
			name: name,
		}
		return &jsonCol, nil
	}
	return nil, &Error{
		ColumnType: fmt.Sprint(kind),
		Err:        fmt.Errorf("unsupported error"),
	}
}

func parseSliceStruct(name string, structVal reflect.Value) (*JSONObject, error) {
	t := make([]Interface, 0)
	for i := 0; i < structVal.NumField(); i++ {
		field := structVal.Field(i)
		kind := field.Kind()
		fName := structVal.Type().Field(i).Name
		value := field.Interface()
		if kind == reflect.Struct {
			col, err := ParseStruct(fName, field)
			if err != nil {
				return nil, err
			}
			t = append(t, col)
		} else if kind == reflect.Slice {
			cols, err := parseSlice(fName, value)
			if err != nil {
				return nil, err
			}
			t = append(t, cols)
		} else {
			col, err := parseType(fName, kind)
			if err != nil {
				return nil, err
			}
			t = append(t, col)
		}
	}
	return &JSONObject{
		name:    name,
		columns: t,
		colType: "Nested",
	}, nil
}

func parseSlice(name string, values interface{}) (Interface, error) {
	sKind := reflect.TypeOf(values).Elem().Kind()
	// need to handle array of structs ouch TODO - how to handle slices of objects
	if sKind == reflect.Struct {
		rValues := reflect.ValueOf(values)
		if rValues.Len() > 0 {
			col, err := parseSliceStruct(name, rValues.Index(0))
			if err != nil {
				return nil, err
			}
			return col, nil
		}
		return &JSONValue{}, nil
	} else if ct, ok := typeMapping[sKind]; ok {
		ct := fmt.Sprintf("Array(%s)", ct)
		// this is acceptable for an array with a primitive type has lowest depth of recursion
		col, err := Type(ct).Column()
		if err != nil {
			return nil, err
		}
		jsonCol := JSONValue{
			col:  col,
			name: name,
		}
		return &jsonCol, nil
	}
	return nil, &Error{
		ColumnType: fmt.Sprint(sKind),
		Err:        fmt.Errorf("unsupported error"),
	}
}

func ParseStruct(name string, structVal reflect.Value) (*JSONObject, error) {
	t := make([]Interface, 0)
	for i := 0; i < structVal.NumField(); i++ {
		// handle the fields in the struct
		field := structVal.Field(i)
		kind := field.Kind()
		fName := structVal.Type().Field(i).Name
		value := field.Interface()
		if kind == reflect.Struct {
			col, err := ParseStruct(fName, field)
			if err != nil {
				return nil, err
			}
			t = append(t, col)
		} else if kind == reflect.Slice {
			cols, err := parseSlice(fName, value)
			if err != nil {
				return nil, err
			}
			t = append(t, cols)
		} else {
			col, err := parseType(fName, kind)
			if err != nil {
				return nil, err
			}
			t = append(t, col)
		}
	}
	return &JSONObject{
		columns: t,
		name:    name,
		colType: "Tuple",
	}, nil
}

func ParseJSON(data interface{}) (*JSON, error) {
	kind := reflect.ValueOf(data).Kind()
	if kind == reflect.Struct {
		jCol, err := ParseStruct("", reflect.ValueOf(data))
		if err != nil {
			return nil, err
		}
		return &JSON{*jCol}, nil
	}
	return nil, &Error{
		ColumnType: fmt.Sprint(kind),
		Err:        fmt.Errorf("unsupported error"),
	}
}

type JSONValue struct {
	col  Interface
	name string
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
	// output name and type! TODO
	panic("implement me")
}

func (jCol *JSONValue) ScanType() reflect.Type {
	return jCol.col.ScanType()
}

type JSON struct {
	JSONObject
}

type JSONObject struct {
	columns []Interface
	name    string
	colType string
}

func (jCol *JSONObject) Type() Type {
	subTypes := make([]string, len(jCol.columns))
	for i, v := range jCol.columns {
		// needs NAME TODO
		subTypes[i] = string(v.Type())
	}
	if jCol.name != "" {
		return Type(fmt.Sprintf("%s %s(%s)", jCol.name, jCol.colType, strings.Join(subTypes, ", ")))
	}
	return Type(fmt.Sprintf("%s(%s)", jCol.colType, strings.Join(subTypes, ", ")))
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

		return nil
	} else {
		// handle string here and maybe map
		panic("Implement me")
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
