package anyrow

import (
	"context"
	"errors"

	"github.com/emicklei/anyrow/pb"
	pgx "github.com/jackc/pgx/v5"
)

func FilterLimit(limit int) filterOption {
	return func(f fetchFilter) fetchFilter {
		f.limit = limit
		return f
	}
}

// Object represents a Table row as a Map.
type Object map[string]any

// FilterObjects queries a table using a WHERE clause. Unless option is given, the limit is 1000.
func FilterObjects(ctx context.Context, conn Querier, tableName string, where string, options ...filterOption) ([]Object, error) {
	set, ok := metaCache.Get(tableName)
	if !ok {
		mset, err := getMetadata(ctx, conn, tableName)
		if err != nil {
			return nil, err
		}
		metaCache.Set(tableName, mset, defaultExpiration)
		set = mset
	}
	collector := &objectCollector{
		set: set.(*pb.RowSet),
	}
	filter := fetchFilter{
		where: where,
		limit: 1000,
	}
	if filter.limit <= 0 {
		return nil, errors.New("limit parameter must be greater than zero")
	}
	for _, each := range options {
		filter = each(filter)
	}
	err := fetchValues(ctx, conn, collector.set, filter, collector)
	return collector.list, err
}

// FetchTablenames returns a list of public tablenames.
func FetchTableNames(ctx context.Context, conn Querier) ([]string, error) {
	return getTableNames(ctx, conn, "public")
}

// FetchColumns returns a list of column schemas for a public tablename.
func FetchColumns(ctx context.Context, conn Querier, tableName string) ([]*pb.ColumnSchema, error) {
	set, err := getMetadata(ctx, conn, tableName)
	if err != nil {
		return []*pb.ColumnSchema{}, err
	}
	return set.ColumnSchemas, nil
}

// FetchObjects returns a list of Objects (generic maps) for the given list primary key values.
func FetchObjects(ctx context.Context, conn Querier, tableName string, pkv PrimaryKeyAndValues) ([]Object, error) {
	set, ok := metaCache.Get(tableName)
	if !ok {
		mset, err := getMetadata(ctx, conn, tableName)
		if err != nil {
			return nil, err
		}
		metaCache.Set(tableName, mset, defaultExpiration)
		set = mset
	}
	collector := &objectCollector{
		set: set.(*pb.RowSet),
	}
	filter := fetchFilter{
		pkv: pkv,
	}
	err := fetchValues(ctx, conn, set.(*pb.RowSet), filter, collector)
	return collector.list, err
}

// FetchObjects returns a protobuf RowSet for the given list primary key values.
func FetchRowSet(ctx context.Context, conn Querier, tableName string, pkv PrimaryKeyAndValues) (*pb.RowSet, error) {
	set, ok := metaCache.Get(tableName)
	if !ok {
		mset, err := getMetadata(ctx, conn, tableName)
		if err != nil {
			return nil, err
		}
		metaCache.Set(tableName, mset, defaultExpiration)
		set = mset
	}
	tset := set.(*pb.RowSet)
	collector := &rowsetCollector{
		// create a new with metadata from the cached set
		set: &pb.RowSet{
			TableName:     tset.TableName,
			ColumnSchemas: tset.ColumnSchemas,
		},
	}
	filter := fetchFilter{
		pkv: pkv,
	}
	err := fetchValues(ctx, conn, tset, filter, collector)
	return collector.set, err
}

// Querier is the method used for a connection.
type Querier interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}

// PrimaryKeyAndValues is a parameter object holding the column name and one or more values.
type PrimaryKeyAndValues struct {
	Column string
	Values []any
}

// MakePrimaryKeyAndValues creates a parameter object.
func MakePrimaryKeyAndValues(column string, value ...any) PrimaryKeyAndValues {
	return PrimaryKeyAndValues{Column: column, Values: value}
}
