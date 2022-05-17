package tests

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/column"
	"github.com/stretchr/testify/assert"
	"reflect"
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
					Actor: Actor{
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

type Actor struct {
	Id uint64
}

type TestJSONStruct struct {
	EventType    string
	Actor        Actor
	Repo         []string
	Contributors []Actor
}

func TestIterateStruct(t *testing.T) {
	col1Data := TestJSONStruct{
		EventType: "PushEvent",
		Actor: Actor{
			Id: 1244,
		},
		Repo: []string{"clickhouse/clickhouse-go", "clickhouse/clickhouse"},
		Contributors: []Actor{
			{Id: 1244},
			{Id: 2244},
			{Id: 3244},
		},
	}

	if reflect.ValueOf(col1Data).Kind() == reflect.Struct {
		fmt.Println()
		v := reflect.ValueOf(&col1Data).Elem()
		cols, err := column.ParseStruct(v)
		assert.NoError(t, err)
		fmt.Println(cols.Type())
		fmt.Println()
	}
}
