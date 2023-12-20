package anyrow

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/emicklei/anyrow/pb"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/patrickmn/go-cache"
)

var metaCache *cache.Cache

var defaultExpiration = 5 * time.Minute

func init() {
	metaCache = cache.New(defaultExpiration, 10*time.Minute)
}

func getMetadata(ctx context.Context, conn Querier, tableName string) (*pb.RowSet, error) {
	query := `
SELECT column_name, data_type, is_nullable,
	EXISTS (
		SELECT 1
		FROM pg_constraint c
		JOIN pg_attribute a ON a.attnum = ANY(c.conkey) AND a.attrelid = c.conrelid
		WHERE c.contype = 'p'
		  AND c.conrelid = CAST($1 as regclass)
		  AND a.attname = isc.column_name
	) AS isPrimary
FROM information_schema.columns isc
WHERE table_name = $2;
`
	rows, err := conn.Query(ctx, query, tableName, tableName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	set := new(pb.RowSet)
	set.TableName = tableName
	for rows.Next() {
		var columnName, dataType, isNullable string
		var isPrimary bool
		if err := rows.Scan(&columnName, &dataType, &isNullable, &isPrimary); err != nil {
			var pgErr *pgconn.PgError
			if errors.As(err, &pgErr) {
				fmt.Println(pgErr.Message) // => syntax error at end of input
				fmt.Println(pgErr.Code)    // => 42601
			}
			return nil, err
		}
		//fmt.Println(columnName, dataType, isNullable, isPrimary)
		set.ColumnSchemas = append(set.ColumnSchemas, &pb.ColumnSchema{
			Name:         columnName,
			TypeName:     dataType,
			IsNullable:   isNullable == "YES",
			IsPrimarykey: isPrimary,
		})
	}
	return set, nil
}

func getTableNames(ctx context.Context, conn Querier, schema string) ([]string, error) {
	query := `
	SELECT table_name
	FROM information_schema.tables
   	WHERE table_schema=$1
		AND table_type='BASE TABLE'`

	rows, err := conn.Query(ctx, query, schema)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	names := []string{}
	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			var pgErr *pgconn.PgError
			if errors.As(err, &pgErr) {
				fmt.Println(pgErr.Message) // => syntax error at end of input
				fmt.Println(pgErr.Code)    // => 42601
			}
			return names, err
		}
		names = append(names, fmt.Sprintf("%s.%s", schema, tableName))
	}
	return names, nil
}

func getSchemaNames(ctx context.Context, conn Querier) ([]string, error) {
	query := `
	SELECT distinct(table_schema) 
	FROM information_schema.tables 
	WHERE table_schema not in('pg_catalog','information_schema')
	`
	rows, err := conn.Query(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	names := []string{}
	for rows.Next() {
		var schemaName string
		if err := rows.Scan(&schemaName); err != nil {
			var pgErr *pgconn.PgError
			if errors.As(err, &pgErr) {
				fmt.Println(pgErr.Message) // => syntax error at end of input
				fmt.Println(pgErr.Code)    // => 42601
			}
			return names, err
		}
		names = append(names, schemaName)
	}
	return names, nil
}
