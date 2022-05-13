package tests

import (
	"context"
	"github.com/ClickHouse/clickhouse-go/v2"
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
			},
			Compression: &clickhouse.Compression{
				Method: clickhouse.CompressionLZ4,
			},
			//Debug: true,
		})
	)
	ddl := `CREATE table json_test(event JSON) ENGINE=Memory;`
	if assert.NoError(t, err) {
		defer func() {
			conn.Exec(ctx, "DROP TABLE json_test")
		}()

		if err := conn.Exec(ctx, ddl); assert.NoError(t, err) {
			if batch, err := conn.PrepareBatch(ctx, "INSERT INTO json_test"); assert.NoError(t, err) {
				doc := `{"type":"PushEvent","actor":{"id":93110249},"repo":{"id":429298592,"name":"revacprogramming/pps-test1-Lakshmipatil2021","url":"https://api.github.com/repos/revacprogramming/pps-test1-Lakshmipatil2021"}}`
				if err := batch.Append(doc); assert.NoError(t, err) {
					if assert.NoError(t, batch.Send()) {

					}
				}
			}
		}
	}

}
