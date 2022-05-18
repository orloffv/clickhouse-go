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
						Id: 93110249,
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

func parseJSON(key string, v interface{}, cols []column.Interface) {
	switch node := v.(type) {
	case map[string]interface{}:
		if key != "" {
			fmt.Printf("%s: (", key)
		}
		i := 0
		len := len(node)
		for key, sub := range node {
			parseJSON(key, sub, cols)
			i++
			if i != len {
				fmt.Printf(", ")
			}
		}
		if key != "" {
			fmt.Print(")")
		}
	case []interface{}:
		fmt.Printf("%s: [", key)
		for i, val := range node {
			switch tval := val.(type) {
			case string:
				fmt.Printf("%q", tval)
			default:
				fmt.Printf("%v", tval)
			}

			i++
			if i != len(node) {
				fmt.Printf(", ")
			}
		}
		fmt.Printf("]")
	case string:
		fmt.Printf("%s: %q", key, v)
	default:
		fmt.Printf("%s: %v", key, v)

	}
}

func TestJSONParse(t *testing.T) {
	src := `{"login":{"password":"secret","user":"bob", "random": 1},"name":"cmpA", "nested": {"value": 1, "value_2": 2}, "a_list":["1", 2, 4], "str_list": ["h", "a"]}`
	var v map[string]interface{}
	if err := json.Unmarshal([]byte(src), &v); err != nil {
		panic(err)
	}
	columns := make([]column.Interface, 0)
	parseJSON("", v, columns)
	fmt.Println()
}

type Address struct {
	City string
}

type Person struct {
	Id      uint64
	Name    string
	Address Address
}

type TestJSONStruct struct {
	EventType    string
	Actor        Person
	Repo         []string
	Contributors []Person
}

func TestIterateStruct(t *testing.T) {
	col1Data := TestJSONStruct{
		EventType: "PushEvent",
		Actor: Person{
			Id:      1244,
			Name:    "Geoff",
			Address: Address{City: "Chicago"},
		},
		Repo: []string{"clickhouse/clickhouse-go", "clickhouse/clickhouse"},
		Contributors: []Person{
			{Id: 1244, Name: "Thom", Address: Address{City: "Denver"}},
			{Id: 2244, Name: "Dale", Address: Address{City: "Lisbon"}},
			{Id: 3244, Name: "Melvyn", Address: Address{City: "Paris"}},
		},
	}

	fmt.Println()
	cols, err := column.ParseJSON(col1Data)
	assert.NoError(t, err)
	fmt.Println(cols.Type())
	fmt.Println()
	bytes, _ := json.Marshal(col1Data)
	fmt.Println(string(bytes))

}
