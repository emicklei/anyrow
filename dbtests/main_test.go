package dbtests

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

var testConnect *pgx.Conn

func TestMain(m *testing.M) {
	os.Setenv("ANYROW_CONN", "postgres://postgres:anyrowdb@localhost:7432/postgres")
	connectionString := os.Getenv("ANYROW_CONN")
	if len(connectionString) == 0 {
		println("no database env set")
		os.Exit(m.Run())
		return
	}
	fmt.Println("db open ...", connectionString)
	conn, err := pgx.Connect(context.Background(), connectionString)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Unable to connect to database: %v\n", err)
		println("no database available so tests in this package are skipped")
		os.Exit(0)
	}
	testConnect = conn
	if err := ensureTables(conn); err != nil {
		fmt.Println("DB WARN:", err)
	}
	uuid.EnableRandPool()
	code := m.Run()
	fmt.Println("... db close")
	conn.Close(context.Background())
	os.Exit(code)
}

func ensureTables(conn *pgx.Conn) error {
	ctx := context.Background()
	tx, err := conn.Begin(ctx)
	if err != nil {
		return err
	}
	_, err = conn.Exec(ctx, `
	drop table IF EXISTS fieldbags;
	create table fieldbags (
		id uuid,
		tdate date,
		ttimestamp timestamp without time zone,
		tjsonb jsonb,
		tjson json,
		ttext text,
		tNumeric numeric,
		tdecimal decimal,
		tdoubleprecision double precision,
		tfloat float,
		tinteger integer,
		tsmallint smallint,
		tbigint bigint,
		tboolean boolean
	);`)
	if err != nil {
		tx.Rollback(ctx)
		return err
	}
	return tx.Commit(ctx)
}
