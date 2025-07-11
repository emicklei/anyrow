package anyrow

import (
	"context"
	"reflect"
	"testing"

	"github.com/emicklei/anyrow/pb"
	pgx "github.com/jackc/pgx/v5"
)

func TestFilterObjects(t *testing.T) {
	ctx := context.Background()
	conn := new(mockQuerier)
	list, err := FilterRecords(ctx, conn, "testkey", "test", "id = 1")
	if err != nil {
		t.Fatal(err)
	}
	if got, want := len(list), 1; got != want {
		t.Errorf("got [%v:%T] want [%v:%T]", got, got, want, want)
	}
	if got, want := conn.sql, `SELECT "str","num" FROM public.test WHERE id = 1 LIMIT 1000`; got != want {
		t.Errorf("got [%v:%T] want [%v:%T]", got, got, want, want)
	}
}

func TestFilterObjectsEmptyWhere(t *testing.T) {
	ctx := context.Background()
	conn := new(mockQuerier)
	list, err := FilterRecords(ctx, conn, "testkey", "test", "")
	if err != nil {
		t.Fatal(err)
	}
	if got, want := len(list), 1; got != want {
		t.Errorf("got [%v:%T] want [%v:%T]", got, got, want, want)
	}
	if got, want := conn.sql, `SELECT "str","num" FROM public.test WHERE true LIMIT 1000`; got != want {
		t.Errorf("got [%v:%T] want [%v:%T]", got, got, want, want)
	}
}

func TestFilterObjectsLimit(t *testing.T) {
	ctx := context.Background()
	conn := new(mockQuerier)
	_, err := FilterRecords(ctx, conn, "testkey", "test", "", FilterLimit(3))
	if err != nil {
		t.Fatal(err)
	}
	if got, want := conn.sql, `SELECT "str","num" FROM public.test WHERE true LIMIT 3`; got != want {
		t.Errorf("got [%v:%T] want [%v:%T]", got, got, want, want)
	}
}

func TestFetchObjects(t *testing.T) {
	ctx := context.Background()
	conn := new(mockQuerier)
	pkv := NewPrimaryKeyAndValues("id", "1", "2")
	list, err := FetchRecords(ctx, conn, "testkey", "test", pkv)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := len(list), 1; got != want {
		t.Errorf("got [%v:%T] want [%v:%T]", got, got, want, want)
	}
	if got, want := conn.sql, `SELECT "str","num" FROM public.test WHERE id IN ($1,$2)`; got != want {
		t.Errorf("got [%v:%T] want [%v:%T]", got, got, want, want)
	}
	if got, want := conn.args, []any{"1", "2"}; !reflect.DeepEqual(got, want) {
		t.Errorf("got [%v:%T] want [%v:%T]", got, got, want, want)
	}
}

func TestFetchRowSet(t *testing.T) {
	ctx := context.Background()
	conn := new(mockQuerier)
	pkv := NewPrimaryKeyAndValues("id", "1", "2")
	set, err := FetchRowSet(ctx, conn, "testkey", "test", pkv)
	if err != nil {
		t.Fatal(err)
	}
	cachedSet, _ := metaCache.Get("testkey")
	if got, want := len((cachedSet.(*pb.RowSet)).Rows), 0; got != want {
		t.Errorf("got [%v:%T] want [%v:%T]", got, got, want, want)
	}
	if got, want := len(set.Rows), 1; got != want {
		t.Errorf("got [%v:%T] want [%v:%T]", got, got, want, want)
	}
	if got, want := conn.sql, `SELECT "str","num" FROM public.test WHERE id IN ($1,$2)`; got != want {
		t.Errorf("got [%v:%T] want [%v:%T]", got, got, want, want)
	}
	if got, want := conn.args, []any{"1", "2"}; !reflect.DeepEqual(got, want) {
		t.Errorf("got [%v:%T] want [%v:%T]", got, got, want, want)
	}
	mp := set.RowMap(0)
	if got, want := mp["num"], float32(42); got != want {
		t.Errorf("got [%v]:%T want [%v]:%T", got, got, want, want)
	}
	if got, want := mp["str"], "shoesize"; got != want {
		t.Errorf("got [%v]:%T want [%v]:%T", got, got, want, want)
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
