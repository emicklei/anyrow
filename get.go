package anyrow

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/emicklei/anyrow/pb"
	"github.com/emicklei/tre"
	"github.com/jackc/pgconn"
	pgx "github.com/jackc/pgx/v4"
	"github.com/patrickmn/go-cache"
)

var metaCache *cache.Cache

func init() {
	metaCache = cache.New(5*time.Minute, 10*time.Minute)
}

// Object represents a Table row as a Map.
type Object map[string]any

func FilterObjects(ctx context.Context, conn Querier, tableName string, where string) ([]Object, error) {
	set, ok := metaCache.Get(tableName)
	if !ok {
		mset, err := getMetadata(ctx, conn, tableName)
		if err != nil {
			return nil, err
		}
		metaCache.Set(tableName, mset, cache.DefaultExpiration)
		set = mset
	}
	return filterObjects(ctx, conn, set.(*pb.RowSet), where)
}

func FetchObjects(ctx context.Context, conn Querier, tableName string, pkv PrimaryKeyAndValues) ([]Object, error) {
	set, ok := metaCache.Get(tableName)
	if !ok {
		mset, err := getMetadata(ctx, conn, tableName)
		if err != nil {
			return nil, err
		}
		metaCache.Set(tableName, mset, cache.DefaultExpiration)
		set = mset
	}
	return fetchObjects(ctx, conn, set.(*pb.RowSet), pkv)
}

func FetchRowSet(ctx context.Context, conn Querier, tableName string, pkv PrimaryKeyAndValues) (*pb.RowSet, error) {
	set, ok := metaCache.Get(tableName)
	if !ok {
		mset, err := getMetadata(ctx, conn, tableName)
		if err != nil {
			return nil, err
		}
		metaCache.Set(tableName, mset, cache.DefaultExpiration)
		set = mset
	}
	return fetchValues(ctx, conn, set.(*pb.RowSet), pkv)
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

func filterObjects(ctx context.Context, conn Querier, metaSet *pb.RowSet, where string) (list []Object, err error) {
	// get values
	qb := new(strings.Builder)
	qb.WriteString("SELECT ")
	for i, each := range metaSet.ColumnSchemas {
		if i > 0 {
			qb.WriteRune(',')
		}
		fmt.Fprintf(qb, "to_json(\"%s\")", each.Name) // always escape
	}
	qb.WriteString(" FROM ")
	qb.WriteString(metaSet.TableName)
	qb.WriteString(" WHERE ")
	qb.WriteString(where)

	dbrows, err := conn.Query(ctx, qb.String())
	if err != nil {
		return nil, tre.New(err, "conn.Query", "sql", qb.String())
	}
	defer dbrows.Close()

	for dbrows.Next() {
		all, err := dbrows.Values()
		if err != nil {
			var pgErr *pgconn.PgError
			if errors.As(err, &pgErr) {
				fmt.Println(pgErr.Message) // => syntax error at end of input
				fmt.Println(pgErr.Code)    // => 42601
			}
			return nil, err
		}
		obj := map[string]any{}
		for i, each := range all {
			if each == nil {
				continue
			}
			schema := metaSet.ColumnSchemas[i]
			switch each.(type) {
			case string:
				obj[schema.Name] = each.(string)
			case float64:
				// check for integer like
				tn := metaSet.ColumnSchemas[i].TypeName
				if strings.Contains("integer bigint smallint", tn) {
					f := each.(float64)
					fint, _ := math.Modf(f)
					obj[schema.Name] = int64(fint)
				} else {
					// is a float like
					obj[schema.Name] = float32(each.(float64))
				}
			case map[string]any, []any:
				obj[schema.Name] = each
			case bool:
				obj[schema.Name] = each.(bool)
			default:
				obj[schema.Name] = each
			}
		}
		list = append(list, obj)
	}
	return list, nil
}

func fetchObjects(ctx context.Context, conn Querier, metaSet *pb.RowSet, pkv PrimaryKeyAndValues) (list []Object, err error) {
	// get values
	qb := new(strings.Builder)
	qb.WriteString("SELECT ")
	for i, each := range metaSet.ColumnSchemas {
		if i > 0 {
			qb.WriteRune(',')
		}
		fmt.Fprintf(qb, "to_json(\"%s\")", each.Name) // always escape
	}
	qb.WriteString(" FROM ")
	qb.WriteString(metaSet.TableName)
	// if no values in pkv then fetch all
	if len(pkv.Values) > 0 {
		qb.WriteString(" WHERE ")
		qb.WriteString(pkv.Column)
		fmt.Fprintf(qb, " IN (%s)", composeQueryParams(len(pkv.Values)))
	}
	dbrows, err := conn.Query(ctx, qb.String(), pkv.Values...)
	if err != nil {
		return nil, err
	}
	defer dbrows.Close()

	for dbrows.Next() {
		all, err := dbrows.Values()
		if err != nil {
			var pgErr *pgconn.PgError
			if errors.As(err, &pgErr) {
				fmt.Println(pgErr.Message) // => syntax error at end of input
				fmt.Println(pgErr.Code)    // => 42601
			}
			return nil, err
		}
		obj := map[string]any{}
		for i, each := range all {
			if each == nil {
				continue
			}
			schema := metaSet.ColumnSchemas[i]
			switch each.(type) {
			case string:
				obj[schema.Name] = each.(string)
			case float64:
				// check for integer like
				tn := metaSet.ColumnSchemas[i].TypeName
				if strings.Contains("integer bigint smallint", tn) {
					f := each.(float64)
					fint, _ := math.Modf(f)
					obj[schema.Name] = int64(fint)
				} else {
					// is a float like
					obj[schema.Name] = float32(each.(float64))
				}
			case map[string]any, []any:
				obj[schema.Name] = each
			case bool:
				obj[schema.Name] = each.(bool)
			default:
				obj[schema.Name] = each
			}
		}
		list = append(list, obj)
	}
	return list, nil
}

func fetchValues(ctx context.Context, conn Querier, metaSet *pb.RowSet, pkv PrimaryKeyAndValues) (*pb.RowSet, error) {
	// get values
	qb := new(strings.Builder)
	qb.WriteString("SELECT ")
	for i, each := range metaSet.ColumnSchemas {
		if i > 0 {
			qb.WriteRune(',')
		}
		fmt.Fprintf(qb, "to_json(%s)", each.Name)
	}
	qb.WriteString(" FROM ")
	qb.WriteString(metaSet.TableName)
	// if no values in pkv then fetch all
	if len(pkv.Values) > 0 {
		qb.WriteString(" WHERE ")
		qb.WriteString(pkv.Column)
		fmt.Fprintf(qb, " IN (%s)", composeQueryParams(len(pkv.Values)))
	}

	dbrows, err := conn.Query(ctx, qb.String(), pkv.Values...)
	if err != nil {
		return nil, err
	}
	defer dbrows.Close()

	set := new(pb.RowSet)
	set.TableName = metaSet.TableName
	set.ColumnSchemas = metaSet.ColumnSchemas
	for dbrows.Next() {
		row := new(pb.Row)
		all, err := dbrows.Values()
		if err != nil {
			var pgErr *pgconn.PgError
			if errors.As(err, &pgErr) {
				fmt.Println(pgErr.Message) // => syntax error at end of input
				fmt.Println(pgErr.Code)    // => 42601
			}
			return nil, err
		}
		row.Columns = make([]*pb.ColumnValue, len(all))
		for i, each := range all {
			if each == nil {
				continue
			}
			cell := new(pb.ColumnValue)
			switch each.(type) {
			case string:
				cell.JsonValue = &pb.ColumnValue_StringValue{StringValue: each.(string)}
			case float64:
				// check for integer like
				tn := metaSet.ColumnSchemas[i].TypeName
				if strings.Contains("integer bigint smallint", tn) {
					f := each.(float64)
					fint, _ := math.Modf(f)
					cell.JsonValue = &pb.ColumnValue_NumberIntegerValue{NumberIntegerValue: int64(fint)}
				} else {
					// is a float like
					cell.JsonValue = &pb.ColumnValue_NumberFloatValue{NumberFloatValue: float32(each.(float64))}
				}
			case map[string]any, []any:
				data, _ := json.Marshal(each)
				cell.JsonValue = &pb.ColumnValue_ObjectValue{ObjectValue: string(data)}
			case bool:
				cell.JsonValue = &pb.ColumnValue_BoolValue{BoolValue: each.(bool)}
			default:
				fmt.Printf("[anyrow] handled as object: %v %T\n", each, each)
				data, _ := json.Marshal(each)
				cell.JsonValue = &pb.ColumnValue_ObjectValue{ObjectValue: string(data)}
			}
			row.Columns[i] = cell
		}
		set.Rows = append(set.Rows, row)
	}
	return set, nil
}

type Querier interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}

type PrimaryKeyAndValues struct {
	Column string
	Values []any
}

func MakePrimaryKeyAndValues(column string, value ...any) PrimaryKeyAndValues {
	return PrimaryKeyAndValues{Column: column, Values: value}
}

func composeQueryParams(count int) string {
	qb := new(strings.Builder)
	for i := 1; i <= count; i++ {
		if i > 1 {
			qb.WriteRune(',')
		}
		qb.WriteRune('$')
		qb.WriteString(strconv.Itoa(i))
	}
	return qb.String()
}
