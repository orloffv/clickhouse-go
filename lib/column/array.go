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

type offset struct {
	values   UInt64
	scanType reflect.Type
}

type Array struct {
	depth    int
	chType   Type
	values   Interface
	offsets  []*offset
	scanType reflect.Type
	name     string
}

func (col *Array) Name() string {
	return col.name
}

func (col *Array) parse(t Type) (_ Interface, err error) {
	col.chType = t
	var typeStr = string(t)

parse:
	for {
		switch {
		case strings.HasPrefix(typeStr, "Array("):
			col.depth++
			typeStr = strings.TrimPrefix(typeStr, "Array(")
			typeStr = strings.TrimSuffix(typeStr, ")")
		default:
			break parse
		}
	}
	if col.depth != 0 {
		if col.values, err = Type(typeStr).Column(col.name); err != nil {
			return nil, err
		}
		offsetScanTypes := make([]reflect.Type, 0, col.depth)
		col.offsets, col.scanType = make([]*offset, 0, col.depth), col.values.ScanType()
		for i := 0; i < col.depth; i++ {
			col.scanType = reflect.SliceOf(col.scanType)
			offsetScanTypes = append(offsetScanTypes, col.scanType)
		}
		for i := len(offsetScanTypes) - 1; i >= 0; i-- {
			col.offsets = append(col.offsets, &offset{
				scanType: offsetScanTypes[i],
			})
		}
		return col, nil
	}
	return nil, &UnsupportedColumnTypeError{
		t: t,
	}
}

func (col *Array) Base() Interface {
	return col.values
}

func (col *Array) Type() Type {
	return col.chType
}

func (col *Array) ScanType() reflect.Type {
	return col.scanType
}

func (col *Array) Rows() int {
	if len(col.offsets) != 0 {
		return len(col.offsets[0].values.data)
	}
	return 0
}

func (col *Array) Row(i int, ptr bool) interface{} {
	return col.make(uint64(i), 0).Interface()
}

func (col *Array) ScanRow(dest interface{}, row int) error {
	elem := reflect.Indirect(reflect.ValueOf(dest))
	if elem.Type() != col.scanType {
		return &ColumnConverterError{
			Op:   "ScanRow",
			To:   fmt.Sprintf("%T", dest),
			From: string(col.chType),
			Hint: fmt.Sprintf("try using *%s", col.scanType),
		}
	}
	{
		elem.Set(col.make(uint64(row), 0))
	}
	return nil
}

func (col *Array) Append(v interface{}) (nulls []uint8, err error) {
	value := reflect.Indirect(reflect.ValueOf(v))
	if value.Kind() != reflect.Slice {
		return nil, &ColumnConverterError{
			Op:   "Append",
			To:   string(col.chType),
			From: fmt.Sprintf("%T", v),
			Hint: "value must be a slice",
		}
	}
	for i := 0; i < value.Len(); i++ {
		if err := col.AppendRow(value.Index(i)); err != nil {
			return nil, err
		}
	}
	return
}

func (col *Array) AppendRow(v interface{}) error {
	var elem reflect.Value
	switch v := v.(type) {
	case reflect.Value:
		elem = reflect.Indirect(v)
	default:
		elem = reflect.Indirect(reflect.ValueOf(v))
	}
	if !elem.IsValid() || elem.Type() != col.scanType {
		from := fmt.Sprintf("%T", v)
		if !elem.IsValid() {
			from = fmt.Sprintf("%v", v)
		}
		return &ColumnConverterError{
			Op:   "AppendRow",
			To:   string(col.chType),
			From: from,
			Hint: fmt.Sprintf("try using %s", col.scanType),
		}
	}
	return col.append(elem, 0)
}

func (col *Array) append(elem reflect.Value, level int) error {
	if level < col.depth {
		offset := uint64(elem.Len())
		if ln := len(col.offsets[level].values.data); ln != 0 {
			offset += col.offsets[level].values.data[ln-1]
		}
		col.offsets[level].values.data = append(col.offsets[level].values.data, offset)
		for i := 0; i < elem.Len(); i++ {
			if err := col.append(elem.Index(i), level+1); err != nil {
				return err
			}
		}
		return nil
	}
	if elem.Kind() == reflect.Ptr && elem.IsNil() {
		return col.values.AppendRow(nil)
	}
	return col.values.AppendRow(elem.Interface())
}

func (col *Array) Decode(decoder *binary.Decoder, rows int) error {
	for _, offset := range col.offsets {
		if err := offset.values.Decode(decoder, rows); err != nil {
			return err
		}
		switch {
		case len(offset.values.data) > 0:
			rows = int(offset.values.data[len(offset.values.data)-1])
		default:
			rows = 0
		}
	}
	return col.values.Decode(decoder, rows)
}

func (col *Array) Encode(encoder *binary.Encoder) error {
	for _, offset := range col.offsets {
		if err := offset.values.Encode(encoder); err != nil {
			return err
		}
	}
	return col.values.Encode(encoder)
}

func (col *Array) ReadStatePrefix(decoder *binary.Decoder) error {
	if serialize, ok := col.values.(CustomSerialization); ok {
		if err := serialize.ReadStatePrefix(decoder); err != nil {
			return err
		}
	}
	return nil
}

func (col *Array) WriteStatePrefix(encoder *binary.Encoder) error {
	if serialize, ok := col.values.(CustomSerialization); ok {
		if err := serialize.WriteStatePrefix(encoder); err != nil {
			return err
		}
	}
	return nil
}

func (col *Array) make(row uint64, level int) reflect.Value {
	offset := col.offsets[level]
	var (
		end   = offset.values.data[row]
		start = uint64(0)
	)
	if row > 0 {
		start = offset.values.data[row-1]
	}
	var (
		base  = offset.scanType.Elem()
		isPtr = base.Kind() == reflect.Ptr
		slice = reflect.MakeSlice(offset.scanType, 0, int(end-start))
	)
	for i := start; i < end; i++ {
		var value reflect.Value
		switch {
		case level == len(col.offsets)-1:
			switch v := col.values.Row(int(i), isPtr); {
			case v == nil:
				value = reflect.Zero(base)
			default:
				value = reflect.ValueOf(v)
			}
		default:
			value = col.make(i, level+1)
		}
		slice = reflect.Append(slice, value)
	}
	return slice
}

func (col *Array) scanJSONSlice(jsonSlice reflect.Value, row int) error {
	kind := jsonSlice.Kind()
	if kind != reflect.Slice {
		return &ColumnConverterError{
			Op:   "ScanRow",
			To:   fmt.Sprintf("%T", jsonSlice),
			From: string(col.Type()),
		}
	}
	// check if we have an array(tuple. This represents a nested.
	tCol, ok := col.values.(*Tuple)
	if !ok {
		// if it is not an array of tuples it will be an array of primitives in json
		value := reflect.ValueOf(col.Row(row, false))
		if value.CanConvert(jsonSlice.Type()) {
			jsonSlice.Set(value.Convert(jsonSlice.Type()))
			return nil
		}
		return &ColumnConverterError{
			Op:   "ScanRow",
			To:   fmt.Sprintf("%T", jsonSlice),
			From: value.Type().String(),
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
		slice := reflect.MakeSlice(jsonSlice.Type(), int(end-start), int(end-start))
		si := 0
		for i := start; i < end; i++ {
			sStruct := reflect.New(slice.Type().Elem()).Elem()
			v := slice.Index(si)
			for _, c := range tCol.columns {
				sField, ok := getFieldValue(sStruct, c.Name())
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
					err := d.scanJSONSlice(sField, int(i))
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
		slice.Set(slice)
	}
	return nil
}

var (
	_ Interface           = (*Array)(nil)
	_ CustomSerialization = (*Array)(nil)
)
