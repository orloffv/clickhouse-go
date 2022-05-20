package tests

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/column"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

type SimpleStruct struct {
	Name string
}

func TestJSON(t *testing.T) {
	var (
		ctx       = context.Background()
		conn, err = clickhouse.Open(&clickhouse.Options{
			Addr:        []string{"127.0.0.1:9000"},
			DialTimeout: time.Hour,
			Auth: clickhouse.Auth{
				Database: "default",
				Username: "default",
				Password: "",
			}, Settings: clickhouse.Settings{
				"allow_experimental_object_type": 1,
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

				/*col1Data := TestJSONStruct{
					EventType: "Notify",
					Actor: Person{
						Id:      1244,
						Name:    "Geoff",
						Friend:  Friend{Id: 3244},
						Address: []Address{{City: "Denver", Devices: []Device{{Id: "dwsdswd"}, {Id: "dsdswd"}}}, {City: "Washington", Devices: []Device{{Id: "wwewe"}, {Id: "ewwewe"}}}},
						Jobs:    []string{"Support Engineer"},
					},
					Contributors: []Person{
						{Id: 2244, Name: "Dale", Address: []Address{{City: "Lisbon", Devices: []Device{{Id: "abchds"}, {Id: "erferwere"}}}, {City: "Edinburgh", Devices: []Device{{Id: "swadsds"}, {Id: "sdsd"}}}}, Friend: Friend{Id: 1244}},
						{Id: 3433, Name: "Melyvn", Address: []Address{{City: "Paris", Devices: []Device{{Id: "adsd"}}}, {City: "Amsterdam"}}, Friend: Friend{Id: 1244}},
					},
				}*/
				col2Data := TestJSONStruct{
					EventType: "PushEvent",
					Repo:      []string{"clickhouse/clickhouse-go", "clickhouse/clickhouse"},
					Actor: Person{
						Id:      2244,
						Name:    "Dale",
						Address: []Address{{City: "Lisbon", Devices: []Device{{Id: "abchds"}, {Id: "erferwere"}}}, {City: "Edinburgh", Devices: []Device{{Id: "swadsds"}, {Id: "sdsd"}}}},
						Friend:  Friend{Id: 1244},
						Jobs:    []string{"Go Driver developer"},
					},
					Contributors: []Person{
						{Id: 1233, Name: "Thom", Address: []Address{{City: "Denver", Devices: []Device{{Id: "rwe2e432"}}}}, Friend: Friend{Id: 3244}},
						{Id: 1244, Name: "Geoff", Address: []Address{{City: "Chicago", Devices: []Device{{Id: "dfsdfsd"}}}, {City: "NYC", Devices: []Device{{Id: "sdsdsd"}}}}, Friend: Friend{Id: 3244}},
						{Id: 3244, Name: "Melvyn", Address: []Address{{City: "Paris", Devices: []Device{{Id: "adsd"}}}, {City: "Amsterdam"}}, Friend: Friend{Id: 1244}},
					},
				}

				/*				assert.NoError(t, batch.Append(col1Data))
				 */assert.NoError(t, batch.Append(col2Data))
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

func TestJSONQuery(t *testing.T) {
	var (
		ctx       = context.Background()
		conn, err = clickhouse.Open(&clickhouse.Options{
			Addr:        []string{"127.0.0.1:9000"},
			DialTimeout: time.Hour,
			Auth: clickhouse.Auth{
				Database: "default",
				Username: "default",
				Password: "",
			}, Settings: clickhouse.Settings{
				"allow_experimental_object_type": 1,
			},
		})
	)
	if assert.NoError(t, err) {
		var (
			col1 []interface{}
		)
		if err := conn.QueryRow(ctx, "SELECT * FROM json_test").Scan(&col1); assert.NoError(t, err) {
			assert.Equal(t, "A", col1)
		}
	}
}

func TestJSONImitate(t *testing.T) {

	var (
		ctx       = context.Background()
		conn, err = clickhouse.Open(&clickhouse.Options{
			Addr:        []string{"127.0.0.1:9000"},
			DialTimeout: time.Hour,
			Auth: clickhouse.Auth{
				Database: "default",
				Username: "default",
				Password: "",
			}, Settings: clickhouse.Settings{
				"flatten_nested": 1,
			},
		})
	)
	conn.Exec(ctx, "DROP TABLE json_test")
	ddl := `CREATE table json_test(event Tuple(EventType String, Actor Tuple(Id UInt64, Name String, Jobs Array(String), Address Nested(City String, Devices Nested(Id String)), Friend Tuple(Id UInt64)), Repo Array(String), Contributors Nested(Id UInt64, Name String, Jobs Array(String), Address Nested(City String, Devices Nested(Id String)), Friend Tuple(Id UInt64)))) ENGINE=Memory;`
	if assert.NoError(t, err) {
		if err := conn.Exec(ctx, ddl); assert.NoError(t, err) {

			/*col1Data := TestJSONStruct{
				EventType: "Notify",
				Actor: Person{
					Id:      1244,
					Name:    "Geoff",
					Friend:  Friend{Id: 3244},
					Address: []Address{{City: "Denver", Devices: []Device{{Id: "dwsdswd"}, {Id: "dsdswd"}}}, {City: "Washington", Devices: []Device{{Id: "wwewe"}, {Id: "ewwewe"}}}},
					Jobs:    []string{"Support Engineer"},
				},
				Contributors: []Person{
					{Id: 2244, Name: "Dale", Address: []Address{{City: "Lisbon", Devices: []Device{{Id: "abchds"}, {Id: "erferwere"}}}, {City: "Edinburgh", Devices: []Device{{Id: "swadsds"}, {Id: "sdsd"}}}}, Friend: Friend{Id: 1244}},
					{Id: 3433, Name: "Melyvn", Address: []Address{{City: "Paris", Devices: []Device{{Id: "adsd"}}}, {City: "Amsterdam"}}, Friend: Friend{Id: 1244}},
				},
			}
			col2Data := TestJSONStruct{
					EventType: "PushEvent",
					Repo:      []string{"clickhouse/clickhouse-go", "clickhouse/clickhouse"},
					Actor: Person{
						Id:      2244,
						Name:    "Dale",
						Address: []Address{{City: "Lisbon", Devices: []Device{{Id: "abchds"}, {Id: "erferwere"}}}, {City: "Edinburgh", Devices: []Device{{Id: "swadsds"}, {Id: "sdsd"}}}},
						Friend:  Friend{Id: 1244},
						Jobs:    []string{"Go Driver developer"},
					},
					Contributors: []Person{
						{Id: 1233, Name: "Thom", Address: []Address{{City: "Denver", Devices: []Device{{Id: "rwe2e432"}}}}, Friend: Friend{Id: 3244}},
						{Id: 1244, Name: "Geoff", Address: []Address{{City: "Chicago", Devices: []Device{{Id: "dfsdfsd"}}}, {City: "NYC", Devices: []Device{{Id: "sdsdsd"}}}}, Friend: Friend{Id: 3244}},
						{Id: 3244, Name: "Melvyn", Address: []Address{{City: "Paris", Devices: []Device{{Id: "adsd"}}}, {City: "Amsterdam"}}, Friend: Friend{Id: 1244}},
					},
				}
			}*/

			/*			col1Data := []interface{}{"Notify", []interface{}{uint64(1244), "Geoff", []string{"Support Engineer"}, [][]interface{}{{"Denver"}, {"Washington"}}, []interface{}{uint64(3244)}}, []string{},
						[][]interface{}{{uint64(2244), "Dale", []string{}, [][]interface{}{{"Lisbon"}, {"Edinburgh"}}, []interface{}{uint64(1244)}},
							{uint64(3433), "Melyvn", []string{}, [][]interface{}{{"Paris"}, {"Amsterdam"}}, []interface{}{uint64(1244)}}}}*/
			col2Data := []interface{}{"PushEvent", []interface{}{uint64(2244), "Dale", []string{"Go Driver developer"}, [][]interface{}{{"Lisbon", [][]interface{}{{"abchds"}, {"erferwere"}}}, {"Edinburgh", [][]interface{}{{"swadsds"}, {"sdsd"}}}}, []interface{}{uint64(1244)}}, []string{"clickhouse/clickhouse-go", "clickhouse/clickhouse"},
				[][]interface{}{{uint64(1233), "Thom", []string{}, [][]interface{}{{"Denver", [][]interface{}{{"rwe2e432"}}}}, []interface{}{uint64(3244)}},
					{uint64(1244), "Geoff", []string{}, [][]interface{}{{"Chicago", [][]interface{}{{"dfsdfsd"}}}, {"NYC", [][]interface{}{{"sdsdsd"}}}}, []interface{}{uint64(3244)}},
					{uint64(3244), "Melvyn", []string{}, [][]interface{}{{"Paris", [][]interface{}{{"adsd"}}}, {"Amsterdam", [][]interface{}{}}}, []interface{}{uint64(1244)}},
				}}

			if batch, err := conn.PrepareBatch(ctx, "INSERT INTO json_test"); assert.NoError(t, err) {
				/*assert.NoError(t, batch.Append(col1Data))*/
				assert.NoError(t, batch.Append(col2Data))
				if assert.NoError(t, batch.Send()) {

				}
			}
		}
	}
}

type Device struct {
	Id string
}

type Address struct {
	City    string
	Devices []Device
}

type Friend struct {
	Id uint64
}
type Person struct {
	Id      uint64
	Name    string
	Jobs    []string
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
	EventType string
	Actor     InconsistentPerson
	Repo      []string
	//Contributors []InconsistentPerson
}

func TestIterateStruct(t *testing.T) {
	col1Data := TestJSONStruct{
		EventType: "Notify",
		Actor: Person{
			Id:      1244,
			Name:    "Geoff",
			Friend:  Friend{Id: 3244},
			Address: []Address{{City: "Denver", Devices: []Device{{Id: "dwsdswd"}, {Id: "dsdswd"}}}, {City: "Washington", Devices: []Device{{Id: "wwewe"}, {Id: "ewwewe"}}}},
			Jobs:    []string{"Support Engineer"},
		},
		Contributors: []Person{
			{Id: 2244, Name: "Dale", Address: []Address{{City: "Lisbon", Devices: []Device{{Id: "abchds"}, {Id: "erferwere"}}}, {City: "Edinburgh", Devices: []Device{{Id: "swadsds"}, {Id: "sdsd"}}}}, Friend: Friend{Id: 1244}},
			{Id: 3433, Name: "Melyvn", Address: []Address{{City: "Paris", Devices: []Device{{Id: "adsd"}}}, {City: "Amsterdam"}}, Friend: Friend{Id: 1244}},
		},
	}

	bytes, _ := json.Marshal(col1Data)
	fmt.Println(string(bytes))
	cols := &column.JSONObject{}
	err := column.AppendStruct(cols, col1Data)
	assert.NoError(t, err)
	fmt.Println(cols.Type())

	col2Data := TestJSONStruct{
		EventType: "PushEvent",
		Repo:      []string{"clickhouse/clickhouse-go", "clickhouse/clickhouse"},
		Actor: Person{
			Id:      2244,
			Name:    "Dale",
			Address: []Address{{City: "Lisbon", Devices: []Device{{Id: "abchds"}, {Id: "erferwere"}}}, {City: "Edinburgh", Devices: []Device{{Id: "swadsds"}, {Id: "sdsd"}}}},
			Friend:  Friend{Id: 1244},
			Jobs:    []string{"Go Driver developer"},
		},
		Contributors: []Person{
			{Id: 1233, Name: "Thom", Address: []Address{{City: "Denver", Devices: []Device{{Id: "rwe2e432"}}}}, Friend: Friend{Id: 3244}},
			{Id: 1244, Name: "Geoff", Address: []Address{{City: "Chicago", Devices: []Device{{Id: "dfsdfsd"}}}, {City: "NYC", Devices: []Device{{Id: "sdsdsd"}}}}, Friend: Friend{Id: 3244}},
			{Id: 3244, Name: "Melvyn", Address: []Address{{City: "Paris", Devices: []Device{{Id: "adsd"}}}, {City: "Amsterdam"}}, Friend: Friend{Id: 1244}},
		},
	}
	err = column.AppendStruct(cols, col2Data)
	assert.NoError(t, err)
	fmt.Println(cols.Type())
	fmt.Println()

	col3Data := InconsistentTestJSONStruct{
		EventType: "PushEvent",
		Actor: InconsistentPerson{
			Id:   "2244",
			Name: "Dale",
			/*			Address: []Address{{City: "Lisbon"}, {City: "Edinburgh"}},
			 */Friend: Friend{Id: 3244},
		},
		Repo: []string{"clickhouse/clickhouse-go", "clickhouse/clickhouse"},
		/*Contributors: []InconsistentPerson{
			{Id: "1244", Name: "Thom", Address: []Address{{City: "Denver"}}, Friend: Friend{Id: 3244}},
			{Id: "1244", Name: "Geoff", Address: []Address{{City: "Chicago"}, {City: "NYC"}}, Friend: Friend{Id: 3244}},
			{Id: "3244", Name: "Melvyn", Address: []Address{{City: "Paris"}}, Friend: Friend{Id: 1244}},
		},*/
	}
	err = column.AppendStruct(cols, col3Data)
	assert.Error(t, err)
	fmt.Println()

}
