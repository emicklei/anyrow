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

func init() {
	metaCache = cache.New(5*time.Minute, 10*time.Minute)
}

func getMetadata(ctx context.Context, conn Querier, tableName string) (*pb.RowSet, error) {
	query := `
SELECT column_name, data_type, is_nullable
FROM information_schema.columns
WHERE table_name = $1;
`
	rows, err := conn.Query(ctx, query, tableName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	set := new(pb.RowSet)
	set.TableName = tableName
	for rows.Next() {
		var columnName, dataType, isNullable string
		if err := rows.Scan(&columnName, &dataType, &isNullable); err != nil {
			var pgErr *pgconn.PgError
			if errors.As(err, &pgErr) {
				fmt.Println(pgErr.Message) // => syntax error at end of input
				fmt.Println(pgErr.Code)    // => 42601
			}
			return nil, err
		}
		set.ColumnSchemas = append(set.ColumnSchemas, &pb.ColumnSchema{
			Name:         columnName,
			TypeName:     dataType,
			IsNullable:   isNullable == "YES",
			IsPrimarykey: false,
		})
	}
	return set, nil
}
