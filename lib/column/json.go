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

func (jCol *JSONObject) parseType(name string, kind reflect.Kind, values interface{}, isArray bool) error {
	col, err := jCol.upsetSubValue(name, kind, isArray)
	if err != nil {
		return err
	}
	err = col.AppendRow(values)
	return err
}

func (jCol *JSONObject) parseSliceStruct(name string, structVal reflect.Value) error {
	col := &JSONObject{
		name:    name,
		columns: make([]Interface, 0),
		colType: "Nested",
	}
	jCol.columns = append(jCol.columns, col)

	for i := 0; i < structVal.NumField(); i++ {
		field := structVal.Field(i)
		kind := field.Kind()
		fName := structVal.Type().Field(i).Name
		value := field.Interface()
		if kind == reflect.Struct {
			err := col.parseStruct(field)
			if err != nil {
				return err
			}
		} else if kind == reflect.Slice {
			err := col.parseSlice(fName, value)
			if err != nil {
				return err
			}
		} else {
			err := col.parseType(fName, kind, value, false)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (jCol *JSONObject) parseSlice(name string, values interface{}) error {
	sKind := reflect.TypeOf(values).Elem().Kind()
	if sKind == reflect.Struct {
		rValues := reflect.ValueOf(values)
		//TODO: we need to process all
		if rValues.Len() > 0 {
			err := jCol.parseSliceStruct(name, rValues.Index(0))
			if err != nil {
				return err
			}
		}
		return nil
	} else {
		err := jCol.parseType(name, sKind, values, true)
		if err != nil {
			return err
		}
		return nil
	}
	return &UnsupportedColumnTypeError{
		t: Type(fmt.Sprint(sKind)),
	}
}

func (jCol *JSONObject) parseStruct(structVal reflect.Value) error {
	for i := 0; i < structVal.NumField(); i++ {
		// handle the fields in the struct
		field := structVal.Field(i)
		kind := field.Kind()
		fName := structVal.Type().Field(i).Name
		value := field.Interface()
		if kind == reflect.Struct {
			col, err := jCol.upsertSubObject(fName, "Tuple")
			if err != nil {
				return err
			}
			err = col.parseStruct(field)
			if err != nil {
				return err
			}
		} else if kind == reflect.Slice {
			err := jCol.parseSlice(fName, value)
			if err != nil {
				return err
			}
		} else {
			err := jCol.parseType(fName, kind, value, false)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (jCol *JSON) AppendStruct(data interface{}) error {
	kind := reflect.ValueOf(data).Kind()
	if kind == reflect.Struct {
		err := jCol.parseStruct(reflect.ValueOf(data))
		if err != nil {
			return err
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
	if err := encoder.String(jCol.name); err != nil {
		return err
	}
	if err := encoder.String(string(jCol.col.Type())); err != nil {
		return err
	}
	return jCol.col.Encode(encoder)
}

func (jCol *JSONValue) ScanType() reflect.Type {
	return jCol.col.ScanType()
}

type JSON struct {
	JSONObject
}

func (jCol *JSON) Type() Type {
	return "Object('json')"
}

type JSONObject struct {
	columns []Interface
	name    string
	colType string
}

func (jCol *JSONObject) upsertSubObject(name string, fType string) (*JSONObject, error) {
	// check if it exists
	for i := range jCol.columns {
		switch sCol := (jCol.columns[i]).(type) {
		case *JSONValue:
			{
				if sCol.name == name {
					return nil, &Error{
						ColumnType: fmt.Sprint(reflect.ValueOf(sCol).Kind()),
						Err:        fmt.Errorf("type mismatch in column %s", name),
					}
				}
			}
		case *JSONObject:
			{
				if sCol.name == name {
					return sCol, nil
				}
			}
		default:
			return nil, &Error{
				Err:        fmt.Errorf("unexpected type in JSONObject"),
				ColumnType: string(reflect.ValueOf(sCol).Kind()),
			}
		}
	}
	// not present so create
	col := &JSONObject{
		columns: make([]Interface, 0),
		name:    name,
		colType: fType,
	}
	jCol.columns = append(jCol.columns, col)
	return col, nil
}

func (jCol *JSONObject) upsetSubValue(name string, kind reflect.Kind, isArray bool) (*JSONValue, error) {
	if ct, ok := typeMapping[kind]; ok {
		if isArray {
			ct = fmt.Sprintf("Array(%s)", ct)
		}
		for i := range jCol.columns {
			switch sCol := (jCol.columns[i]).(type) {
			case *JSONValue:
				{
					if sCol.name == name {
						if sCol.col.Type() != Type(ct) {
							return nil, &Error{
								ColumnType: fmt.Sprint(reflect.ValueOf(sCol).Kind()),
								Err:        fmt.Errorf("type mismatch in column %s", name),
							}
						}
						// return existing column
						return sCol, nil
					}
				}
			case *JSONObject:
				{
					if sCol.name == name {
						return nil, &Error{
							ColumnType: fmt.Sprint(reflect.ValueOf(sCol).Kind()),
							Err:        fmt.Errorf("type mismatch in column %s", name),
						}
					}
				}
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

// TypeMapping utility function to print detected type hierarchy
func (jCol *JSONObject) TypeMapping() Type {
	subTypes := make([]string, len(jCol.columns))
	for i, v := range jCol.columns {
		switch v.(type) {
		case *JSONObject:
			subTypes[i] = string(v.(*JSONObject).TypeMapping())
		default:
			subTypes[i] = string(v.Type())
		}
	}
	if jCol.name != "" {
		return Type(fmt.Sprintf("%s %s(%s)", jCol.name, jCol.colType, strings.Join(subTypes, ", ")))
	}
	return Type(fmt.Sprintf("%s(%s)", jCol.colType, strings.Join(subTypes, ", ")))
}

func (jCol *JSONObject) Type() Type {
	return Type(jCol.colType)
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
		panic("Implement me")
	} else {
		// handle string here and maybe map
		panic("Implement me")
	}
}

func (jCol *JSON) AppendRow(v interface{}) error {
	if reflect.ValueOf(v).Kind() == reflect.Struct {
		err := jCol.AppendStruct(v)
		if err != nil {
			return err
		}
		return nil
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
