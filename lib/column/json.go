package column

import (
	"reflect"

	"github.com/ClickHouse/clickhouse-go/v2/lib/binary"
)

type JSON struct {
}

func (J JSON) Type() Type {
	//TODO implement me
	panic("implement me")
}

func (J JSON) Rows() int {
	//TODO implement me
	panic("implement me")
}

func (J JSON) Row(i int, ptr bool) interface{} {
	//TODO implement me
	panic("implement me")
}

func (J JSON) ScanRow(dest interface{}, row int) error {
	//TODO implement me
	panic("implement me")
}

func (J JSON) Append(v interface{}) (nulls []uint8, err error) {
	//TODO implement me
	panic("implement me")
}

func (J JSON) AppendRow(v interface{}) error {
	//TODO implement me
	panic("implement me")
}

func (J JSON) Decode(decoder *binary.Decoder, rows int) error {
	//TODO implement me
	panic("implement me")
}

func (J JSON) Encode(encoder *binary.Encoder) error {
	//TODO implement me
	panic("implement me")
}

func (J JSON) ScanType() reflect.Type {
	//TODO implement me
	panic("implement me")
}
