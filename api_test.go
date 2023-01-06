package anyrow

import (
	"context"
	"testing"

	"github.com/emicklei/anyrow/pb"
	pgx "github.com/jackc/pgx/v4"
	"github.com/patrickmn/go-cache"
)

func TestFetchObjects(t *testing.T) {
	set := new(pb.RowSet)
	set.TableName = "test"
	set.ColumnSchemas = append(set.ColumnSchemas, &pb.ColumnSchema{
		Name:         "str",
		TypeName:     "text",
		IsNullable:   true,
		IsPrimarykey: false,
	})
	metaCache.Set("test", set, cache.DefaultExpiration)
	ctx := context.Background()

	conn := new(mockQuerier)
	list, err := FilterObjects(ctx, conn, "test", "true")
	if err != nil {
		t.Fatal(err)
	}
	if got, want := len(list), 0; got != want {
		t.Errorf("got [%v:%T] want [%v:%T]", got, got, want, want)
	}
	if got, want := conn.sql, `SELECT to_json("str") FROM test WHERE true`; got != want {
		t.Errorf("got [%v:%T] want [%v:%T]", got, got, want, want)
	}
}

type mockQuerier struct {
	sql string
}
type mockRows struct {
	pgx.Rows
}

func (m mockRows) Next() bool { return false }
func (m mockRows) Close()     {}

func (m *mockQuerier) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	m.sql = sql
	return mockRows{}, nil
}
