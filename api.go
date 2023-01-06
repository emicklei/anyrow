package anyrow

import (
	"context"

	"github.com/emicklei/anyrow/pb"
	pgx "github.com/jackc/pgx/v4"
	"github.com/patrickmn/go-cache"
)

func FilterLimit(limit int) filterOption {
	return func(f fetchFilter) fetchFilter {
		f.limit = limit
		return f
	}
}

// Object represents a Table row as a Map.
type Object map[string]any

// FilterObjects queries a table using a WHERE clause. DefaultLimit is used.
func FilterObjects(ctx context.Context, conn Querier, tableName string, where string, options ...filterOption) ([]Object, error) {
	set, ok := metaCache.Get(tableName)
	if !ok {
		mset, err := getMetadata(ctx, conn, tableName)
		if err != nil {
			return nil, err
		}
		metaCache.Set(tableName, mset, cache.DefaultExpiration)
		set = mset
	}
	collector := &objectCollector{
		set: set.(*pb.RowSet),
	}
	filter := fetchFilter{
		where: where,
		limit: 1000,
	}
	for _, each := range options {
		filter = each(filter)
	}
	err := fetchValues(ctx, conn, collector.set, filter, collector)
	return collector.list, err
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
	collector := &objectCollector{
		set: set.(*pb.RowSet),
	}
	filter := fetchFilter{
		pkv: pkv,
	}
	err := fetchValues(ctx, conn, set.(*pb.RowSet), filter, collector)
	return collector.list, err
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
	collector := &rowsetCollector{
		set: set.(*pb.RowSet),
	}
	filter := fetchFilter{
		pkv: pkv,
	}
	err := fetchValues(ctx, conn, collector.set, filter, collector)
	return collector.set, err
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
