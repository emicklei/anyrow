package anyrow

import (
	"context"
	"reflect"
	"testing"

	"github.com/emicklei/anyrow/pb"
	pgx "github.com/jackc/pgx/v4"
	"github.com/patrickmn/go-cache"
)

func TestMain(m *testing.M) {
	set := new(pb.RowSet)
	set.TableName = "test"
	set.ColumnSchemas = append(set.ColumnSchemas, &pb.ColumnSchema{
		Name:         "str",
		TypeName:     "text",
		IsNullable:   true,
		IsPrimarykey: false,
	})
	set.ColumnSchemas = append(set.ColumnSchemas, &pb.ColumnSchema{
		Name:         "num",
		TypeName:     "int64",
		IsNullable:   true,
		IsPrimarykey: false,
	})
	metaCache.Set("test", set, cache.DefaultExpiration)
	m.Run()
}

func TestFilterObjects(t *testing.T) {
	ctx := context.Background()
	conn := new(mockQuerier)
	list, err := FilterObjects(ctx, conn, "test", "id = 1")
	if err != nil {
		t.Fatal(err)
	}
	if got, want := len(list), 1; got != want {
		t.Errorf("got [%v:%T] want [%v:%T]", got, got, want, want)
	}
	if got, want := conn.sql, `SELECT to_json("str"),to_json("num") FROM test WHERE id = 1 LIMIT 1000`; got != want {
		t.Errorf("got [%v:%T] want [%v:%T]", got, got, want, want)
	}
}

func TestFilterObjectsEmptyWhere(t *testing.T) {
	ctx := context.Background()
	conn := new(mockQuerier)
	list, err := FilterObjects(ctx, conn, "test", "")
	if err != nil {
		t.Fatal(err)
	}
	if got, want := len(list), 1; got != want {
		t.Errorf("got [%v:%T] want [%v:%T]", got, got, want, want)
	}
	if got, want := conn.sql, `SELECT to_json("str"),to_json("num") FROM test WHERE true LIMIT 1000`; got != want {
		t.Errorf("got [%v:%T] want [%v:%T]", got, got, want, want)
	}
}

func TestFilterObjectsLimit(t *testing.T) {
	ctx := context.Background()
	conn := new(mockQuerier)
	_, err := FilterObjects(ctx, conn, "test", "", FilterLimit(3))
	if err != nil {
		t.Fatal(err)
	}
	if got, want := conn.sql, `SELECT to_json("str"),to_json("num") FROM test WHERE true LIMIT 3`; got != want {
		t.Errorf("got [%v:%T] want [%v:%T]", got, got, want, want)
	}
}

func TestFetchObjects(t *testing.T) {
	ctx := context.Background()
	conn := new(mockQuerier)
	pkv := MakePrimaryKeyAndValues("id", "1", "2")
	list, err := FetchObjects(ctx, conn, "test", pkv)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := len(list), 1; got != want {
		t.Errorf("got [%v:%T] want [%v:%T]", got, got, want, want)
	}
	if got, want := conn.sql, `SELECT to_json("str"),to_json("num") FROM test WHERE id IN ($1,$2)`; got != want {
		t.Errorf("got [%v:%T] want [%v:%T]", got, got, want, want)
	}
	if got, want := conn.args, []any{"1", "2"}; !reflect.DeepEqual(got, want) {
		t.Errorf("got [%v:%T] want [%v:%T]", got, got, want, want)
	}
}

func TestFetchRowSet(t *testing.T) {
	ctx := context.Background()
	conn := new(mockQuerier)
	pkv := MakePrimaryKeyAndValues("id", "1", "2")
	set, err := FetchRowSet(ctx, conn, "test", pkv)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := len(set.Rows), 1; got != want {
		t.Errorf("got [%v:%T] want [%v:%T]", got, got, want, want)
	}
	if got, want := conn.sql, `SELECT to_json("str"),to_json("num") FROM test WHERE id IN ($1,$2)`; got != want {
		t.Errorf("got [%v:%T] want [%v:%T]", got, got, want, want)
	}
	if got, want := conn.args, []any{"1", "2"}; !reflect.DeepEqual(got, want) {
		t.Errorf("got [%v:%T] want [%v:%T]", got, got, want, want)
	}
}

type mockQuerier struct {
	sql  string
	args []any
}
type mockRows struct {
	pgx.Rows
	values []any
	next   bool
}

func (m *mockRows) Next() bool {
	n := m.next
	m.next = false
	return n
}
func (m *mockRows) Close()                 {}
func (m *mockRows) Values() ([]any, error) { return m.values, nil }

func (m *mockQuerier) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	m.sql = sql
	m.args = args
	s := "shoesize"
	i := float64(42)
	return &mockRows{next: true, values: []any{s, i}}, nil
}
