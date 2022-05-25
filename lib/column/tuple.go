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
	"reflect"
	"strings"

	"github.com/ClickHouse/clickhouse-go/v2/lib/binary"
)

type Tuple struct {
	chType  Type
	columns []Interface
	name    string
}

func (col *Tuple) Name() string {
	return col.name
}

type namedCol struct {
	name    string
	colType Type
}

func (col *Tuple) parse(t Type) (_ Interface, err error) {
	col.chType = t
	var (
		element       []rune
		elements      []namedCol
		brackets      int
		appendElement = func() {
			if len(element) != 0 {
				cType := strings.TrimSpace(string(element))
				name := ""
				if parts := strings.SplitN(cType, " ", 2); len(parts) == 2 {
					if !strings.Contains(parts[0], "(") {
						name = parts[0]
						cType = parts[1]
					}
				}
				elements = append(elements, namedCol{
					name:    name,
					colType: Type(strings.TrimSpace(cType)),
				})
			}
		}
	)
	for _, r := range t.params() {
		switch r {
		case '(':
			brackets++
		case ')':
			brackets--
		case ',':
			if brackets == 0 {
				appendElement()
				element = element[:0]
				continue
			}
		}
		element = append(element, r)
	}
	appendElement()
	for _, ct := range elements {
		column, err := ct.colType.Column(ct.name)
		if err != nil {
			return nil, err
		}
		col.columns = append(col.columns, column)
	}
	if len(col.columns) != 0 {
		return col, nil
	}
	return nil, &UnsupportedColumnTypeError{
		t: t,
	}
}

func (col *Tuple) Type() Type {
	return col.chType
}

func (Tuple) ScanType() reflect.Type {
	return scanTypeSlice
}

func (col *Tuple) Rows() int {
	if len(col.columns) != 0 {
		return col.columns[0].Rows()
	}
	return 0
}

func (col *Tuple) Row(i int, ptr bool) interface{} {
	tuple := make([]interface{}, 0, len(col.columns))
	for _, c := range col.columns {
		tuple = append(tuple, c.Row(i, ptr))
	}
	return tuple
}

func getFieldValue(field reflect.Value, name string, cType Type) (reflect.Value, bool) {
	tField := field.Type()
	for i := 0; i < tField.NumField(); i++ {
		if jsonTag := tField.Field(i).Tag.Get("json"); jsonTag == name {
			return field.Field(i), true
		}
	}
	sField := field.FieldByName(name)
	return sField, sField.IsValid()
}

func (col *Array) scanJSONStruct(rStruct reflect.Value, row int) error {
	kind := rStruct.Kind()
	if kind != reflect.Slice {
		return &ColumnConverterError{
			Op:   "ScanRow",
			To:   fmt.Sprintf("%T", rStruct),
			From: string(col.Type()),
		}
	}
	tCol, ok := col.values.(*Tuple)
	if !ok {
		value := reflect.ValueOf(col.Row(row, false))
		if value.CanConvert(rStruct.Type()) {
			rStruct.Set(value.Convert(rStruct.Type()))
			return nil
		}
		return &ColumnConverterError{
			Op:   "ScanRow",
			To:   fmt.Sprintf("%T", rStruct),
			From: string(col.Type()),
		}
	}
	// Array(Tuple so depth 1 for JSON
	offset := col.offsets[0]
	var (
		end   = offset.values.data[row]
		start = uint64(0)
	)
	if row > 0 {
		start = offset.values.data[row-1]
	}

	if end-start > 0 {
		slice := reflect.MakeSlice(rStruct.Type(), int(end-start), int(end-start))
		si := 0
		for i := start; i < end; i++ {
			sStruct := reflect.New(rStruct.Type().Elem()).Elem()
			v := slice.Index(si)
			for _, c := range tCol.columns {
				sField, ok := getFieldValue(sStruct, c.Name(), c.Type())
				if !ok {
					return &Error{
						ColumnType: fmt.Sprint(c.Type()),
						Err:        fmt.Errorf("column %s is not present in the struct %s  - only JSON structures are supported", c.Name(), sStruct),
					}
				}
				switch d := c.(type) {
				case *Tuple:
					err := d.scanJSONStruct(sField, int(i))
					if err != nil {
						return err
					}
				case *Array:
					err := d.scanJSONStruct(sField, int(i))
					if err != nil {
						return err
					}
				default:
					value := reflect.ValueOf(c.Row(int(i), false))
					if value.CanConvert(sField.Type()) {
						sField.Set(value.Convert(sField.Type()))
					} else {
						return &ColumnConverterError{
							Op:   "ScanRow",
							To:   fmt.Sprintf("%T", sField),
							From: string(col.Type()),
						}
					}
				}
			}
			v.Set(sStruct)
			si++
		}
		rStruct.Set(slice)
	}

	return nil
}

func (col *Tuple) scanJSONStruct(rStruct reflect.Value, row int) error {
	kind := rStruct.Kind()
	if kind != reflect.Struct {
		return &ColumnConverterError{
			Op:   "ScanRow",
			To:   fmt.Sprintf("%s", kind),
			From: string(col.Type()),
		}
	}

	for _, c := range col.columns {
		// the column may be serialized using a different name due to a struct "json" tag
		sField, ok := getFieldValue(rStruct, c.Name(), c.Type())
		if !ok {
			return &Error{
				ColumnType: fmt.Sprint(c.Type()),
				Err:        fmt.Errorf("column %s is not present in the struct %s  - only JSON structures are supported", c.Name(), rStruct),
			}
		}
		switch d := c.(type) {
		case *Tuple:
			err := d.scanJSONStruct(sField, row)
			if err != nil {
				return err
			}
		case *Nested:
			jCol, ok := d.Interface.(*Array)
			if !ok {
				return &Error{
					ColumnType: fmt.Sprint(d.Interface),
					Err:        fmt.Errorf("expected Nested to be Array(Tuple) for column %s", c.Name()),
				}
			}
			err := jCol.scanJSONStruct(sField, row)
			if err != nil {
				return err
			}
		case *Array:
			// can contain array of tuple or primitive types - former happens due to rewrite of Nested to Array(Tuple)
			err := d.scanJSONStruct(sField, row)
			if err != nil {
				return err
			}
		default:
			value := reflect.ValueOf(c.Row(row, false))
			if value.CanConvert(sField.Type()) {
				sField.Set(value.Convert(sField.Type()))
			} else {
				return &ColumnConverterError{
					Op:   "ScanRow",
					To:   fmt.Sprintf("%T", sField),
					From: string(col.Type()),
				}
			}
		}
	}
	return nil
}

func (col *Tuple) ScanRow(dest interface{}, row int) error {
	switch d := dest.(type) {
	case *[]interface{}:
		tuple := make([]interface{}, 0, len(col.columns))
		for _, c := range col.columns {
			tuple = append(tuple, c.Row(row, false))
		}
		*d = tuple
	default:
		kind := reflect.Indirect(reflect.ValueOf(dest)).Kind()
		if kind != reflect.Struct {
			return &ColumnConverterError{
				Op:   "ScanRow",
				To:   fmt.Sprintf("%T", dest),
				From: string(col.chType),
			}
		}
		rStruct := reflect.ValueOf(dest).Elem()
		return col.scanJSONStruct(rStruct, row)
	}
	return nil
}

func (col *Tuple) Append(v interface{}) (nulls []uint8, err error) {
	switch v := v.(type) {
	case [][]interface{}:
		for _, v := range v {
			if err := col.AppendRow(v); err != nil {
				return nil, err
			}
		}
		return nil, nil
	}
	return nil, &ColumnConverterError{
		Op:   "Append",
		To:   string(col.chType),
		From: fmt.Sprintf("%T", v),
	}
}

func (col *Tuple) AppendRow(v interface{}) error {
	switch v := v.(type) {
	case []interface{}:
		if len(v) != len(col.columns) {
			return &Error{
				ColumnType: string(col.chType),
				Err:        fmt.Errorf("invalid size. expected %d got %d", len(col.columns), len(v)),
			}
		}
		for i, v := range v {
			if err := col.columns[i].AppendRow(v); err != nil {
				return err
			}
		}
		return nil
	}
	return &ColumnConverterError{
		Op:   "AppendRow",
		To:   string(col.chType),
		From: fmt.Sprintf("%T", v),
	}
}

func (col *Tuple) Decode(decoder *binary.Decoder, rows int) error {
	for _, c := range col.columns {
		if err := c.Decode(decoder, rows); err != nil {
			return err
		}
	}
	return nil
}

func (col *Tuple) Encode(encoder *binary.Encoder) error {
	for _, c := range col.columns {
		if err := c.Encode(encoder); err != nil {
			return err
		}
	}
	return nil
}

func (col *Tuple) ReadStatePrefix(decoder *binary.Decoder) error {
	for _, c := range col.columns {
		if serialize, ok := c.(CustomSerialization); ok {
			if err := serialize.ReadStatePrefix(decoder); err != nil {
				return err
			}
		}
	}
	return nil
}

func (col *Tuple) WriteStatePrefix(encoder *binary.Encoder) error {
	for _, c := range col.columns {
		if serialize, ok := c.(CustomSerialization); ok {
			if err := serialize.WriteStatePrefix(encoder); err != nil {
				return err
			}
		}
	}
	return nil
}

var (
	_ Interface           = (*Tuple)(nil)
	_ CustomSerialization = (*Tuple)(nil)
)
