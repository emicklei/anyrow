package anyrow

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/emicklei/anyrow/pb"
	"github.com/emicklei/tre"
	"github.com/jackc/pgconn"
	pgx "github.com/jackc/pgx/v4"
	"github.com/patrickmn/go-cache"
)

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

func fetchValues2(ctx context.Context, conn Querier, metaSet *pb.RowSet, filter fetchFilter, collector valueCollector) error {
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
	qb.WriteString(" WHERE ")
	filter.whereOn(qb)

	dbrows, err := conn.Query(ctx, qb.String(), filter.pkv.Values...)
	if err != nil {
		return err
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
			return err
		}
		collector.nextRow(len(all))
		for i, each := range all {
			if each == nil {
				continue
			}
			switch each.(type) {
			case string:
				collector.storeString(i, each.(string))
			case float64:
				// check for integer like
				tn := metaSet.ColumnSchemas[i].TypeName
				if strings.Contains("integer bigint smallint", tn) {
					f := each.(float64)
					fint, _ := math.Modf(f)
					collector.storeInt64(i, int64(fint))
				} else {
					// is a float like
					collector.storeFloat32(i, float32(each.(float64)))
				}
			case map[string]any, []any:
				collector.storeDefault(i, each)
			case bool:
				collector.storeBool(i, each.(bool))
			default:
				fmt.Printf("[anyrow] handled as object: %v %T\n", each, each)
				collector.storeDefault(i, each)
			}
		}
	}
	return nil
}
