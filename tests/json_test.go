package tests

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/column"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestJSON(t *testing.T) {
	var (
		ctx       = context.Background()
		conn, err = clickhouse.Open(&clickhouse.Options{
			Addr: []string{"127.0.0.1:9000"},
			Auth: clickhouse.Auth{
				Database: "default",
				Username: "default",
				Password: "",
			}, Settings: clickhouse.Settings{
				"allow_experimental_object_type": 1,
				"describe_extend_object_types":   1,
			},
		})
	)
	conn.Exec(ctx, "DROP TABLE json_test")
	ddl := `CREATE table json_test(event JSON) ENGINE=Memory;`
	if assert.NoError(t, err) {
		//defer func() {
		//	conn.Exec(ctx, "DROP TABLE json_test")
		//}()

		if err := conn.Exec(ctx, ddl); assert.NoError(t, err) {
			if batch, err := conn.PrepareBatch(ctx, "INSERT INTO json_test"); assert.NoError(t, err) {

				col1Data := TestJSONStruct{
					EventType: "PushEvent",
					Actor: Person{
						Id:      1244,
						Name:    "Geoff",
						Address: []Address{{City: "Chicago"}, {City: "NYC"}},
						Friend:  Friend{Id: 3244},
					},
					Repo: []string{"clickhouse/clickhouse-go", "clickhouse/clickhouse"},
					Contributors: []Person{
						{Id: 1244, Name: "Thom", Address: []Address{{City: "Denver"}}, Friend: Friend{Id: 3244}},
						{Id: 2244, Name: "Dale", Address: []Address{{City: "Lisbon"}, {City: "Edinburgh"}}, Friend: Friend{Id: 3244}},
						{Id: 3244, Name: "Melvyn", Address: []Address{{City: "Paris"}}, Friend: Friend{Id: 1244}},
					},
				}
				if err := batch.Append(col1Data); assert.NoError(t, err) {
					if assert.NoError(t, batch.Send()) {
						var (
							col1 []interface{}
						)
						if err := conn.QueryRow(ctx, "SELECT * FROM json_test").Scan(&col1); assert.NoError(t, err) {
							assert.Equal(t, "A", col1)
						}
					}
				}
			}
		}
	}

}

type Address struct {
	City string
}

type Friend struct {
	Id uint64
}
type Person struct {
	Id      uint64
	Name    string
	Address []Address
	Friend  Friend
}

type TestJSONStruct struct {
	EventType    string
	Actor        Person
	Repo         []string
	Contributors []Person
}

type InconsistentPerson struct {
	Id      string
	Name    string
	Address []Address
	Friend  Friend
}

type InconsistentTestJSONStruct struct {
	EventType    string
	Actor        Person
	Repo         []string
	Contributors []InconsistentPerson
}

func TestIterateStruct(t *testing.T) {
	col1Data := TestJSONStruct{
		EventType: "Notify",
		Actor: Person{
			Id:      1244,
			Name:    "Geoff",
			Address: []Address{{City: "Chicago"}, {City: "NYC"}},
		},
		Contributors: []Person{
			{Id: 1244, Name: "Thom", Address: []Address{{City: "Denver"}}, Friend: Friend{Id: 3244}},
			{Id: 2244, Name: "Dale", Address: []Address{{City: "Lisbon"}, {City: "Edinburgh"}}, Friend: Friend{Id: 3244}},
			{Id: 3244, Name: "Melvyn", Address: []Address{{City: "Paris"}}, Friend: Friend{Id: 1244}},
		},
	}

	fmt.Println()
	cols := &column.JSON{}
	err := cols.AppendStruct(col1Data)
	assert.NoError(t, err)
	fmt.Println(cols.TypeMapping())

	col2Data := TestJSONStruct{
		EventType: "PushEvent",
		Actor: Person{
			Id:      2244,
			Name:    "Dale",
			Address: []Address{{City: "Lisbon"}, {City: "Edinburgh"}},
			Friend:  Friend{Id: 3244},
		},
		Repo: []string{"clickhouse/clickhouse-go", "clickhouse/clickhouse"},
		Contributors: []Person{
			{Id: 1244, Name: "Thom", Address: []Address{{City: "Denver"}}, Friend: Friend{Id: 3244}},
			{Id: 1244, Name: "Geoff", Address: []Address{{City: "Chicago"}, {City: "NYC"}}, Friend: Friend{Id: 3244}},
			{Id: 3244, Name: "Melvyn", Address: []Address{{City: "Paris"}}, Friend: Friend{Id: 1244}},
		},
	}
	err = cols.AppendStruct(col2Data)
	assert.NoError(t, err)
	fmt.Println(cols.TypeMapping())
	fmt.Println()

	col3Data := InconsistentTestJSONStruct{
		EventType: "PushEvent",
		Actor: Person{
			Id:      2244,
			Name:    "Dale",
			Address: []Address{{City: "Lisbon"}, {City: "Edinburgh"}},
			Friend:  Friend{Id: 3244},
		},
		Repo: []string{"clickhouse/clickhouse-go", "clickhouse/clickhouse"},
		Contributors: []InconsistentPerson{
			{Id: "1244", Name: "Thom", Address: []Address{{City: "Denver"}}, Friend: Friend{Id: 3244}},
			{Id: "1244", Name: "Geoff", Address: []Address{{City: "Chicago"}, {City: "NYC"}}, Friend: Friend{Id: 3244}},
			{Id: "3244", Name: "Melvyn", Address: []Address{{City: "Paris"}}, Friend: Friend{Id: 1244}},
		},
	}
	err = cols.AppendStruct(col3Data)
	assert.Error(t, err)
	fmt.Println()
	bytes, _ := json.Marshal(col1Data)
	fmt.Println(string(bytes))

}
