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
	connectionString := os.Getenv("PGTALK_CONN") // "postgres://postgres:pgtalk@localhost:7432/postgres"
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
		tDate date,
		tTimestamp timestamp without time zone,
		TJSONB jsonb,
		TJSON json,
		tText text,
		tNumeric numeric,
		tDecimal decimal,
		tDoublePrecision double precision,
		tFloat float,
		tInteger integer,
		tSmallint smallint,
		tBigint bigint,
		tBoolean boolean
	);`)
	if err != nil {
		tx.Rollback(ctx)
		return err
	}
	return tx.Commit(ctx)
}
