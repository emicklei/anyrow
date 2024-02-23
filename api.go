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

// Record represents a Table row as a Map.
type Record map[string]any

// FilterRecords queries a table using a WHERE clause. Unless option is given, the limit is 1000.
func FilterRecords(ctx context.Context, conn Querier, metadataCacheKey, tableName string, where string, options ...filterOption) ([]Record, error) {
	set, ok := metaCache.Get(metadataCacheKey)
	if !ok {
		mset, err := getMetadata(ctx, conn, tableName)
		if err != nil {
			return nil, err
		}
		metaCache.Set(metadataCacheKey, mset, defaultExpiration)
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

func FetchSchemas(ctx context.Context, conn Querier) ([]string, error) {
	return getSchemaNames(ctx, conn)
}

// FetchTablenames returns a list of public tablenames.
func FetchTableNames(ctx context.Context, conn Querier, schema string) ([]string, error) {
	return getTableNames(ctx, conn, schema)
}

// FetchColumns returns a list of column schemas for a public tablename.
func FetchColumns(ctx context.Context, conn Querier, tableName string) ([]*pb.ColumnSchema, error) {
	set, err := getMetadata(ctx, conn, tableName)
	if err != nil {
		return []*pb.ColumnSchema{}, err
	}
	return set.ColumnSchemas, nil
}

// FetchRecords returns a list of Objects (generic maps) for the given list primary key values.
func FetchRecords(ctx context.Context, conn Querier, metadataCacheKey, tableName string, pkv PrimaryKeysAndValues) ([]Record, error) {
	set, ok := metaCache.Get(metadataCacheKey)
	if !ok {
		mset, err := getMetadata(ctx, conn, tableName)
		if err != nil {
			return nil, err
		}
		metaCache.Set(metadataCacheKey, mset, defaultExpiration)
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
func FetchRowSet(ctx context.Context, conn Querier, metadataCacheKey, tableName string, pkv PrimaryKeysAndValues) (*pb.RowSet, error) {
	set, ok := metaCache.Get(metadataCacheKey)
	if !ok {
		mset, err := getMetadata(ctx, conn, tableName)
		if err != nil {
			return nil, err
		}
		metaCache.Set(metadataCacheKey, mset, defaultExpiration)
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

// Querier is the interface that is used from a database connection.
// Known implementations are *pgx.Conn and *pgx.Tx and *pgxpool.Conn.
type Querier interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}

// PrimaryKeysAndValues is a parameter object holding the column name(s) and one or more values.
type PrimaryKeysAndValues struct {
	// either a single column with many values
	column string
	values []any
	// or pairs
	pairs []PrimaryKeyAndValue
}

func (pkv PrimaryKeysAndValues) hasValues() bool {
	return len(pkv.values) > 0 || len(pkv.pairs) > 0
}

func (pkv PrimaryKeysAndValues) parameterValues() (list []any) {
	if pkv.column != "" {
		return pkv.values
	}
	// use pairs
	for _, each := range pkv.pairs {
		list = append(list, each.Value)
	}
	return
}

type PrimaryKeyAndValue struct {
	Column string
	Value  any
}

// NewPrimaryKeyAndValue creates a parameter object.
func NewPrimaryKeyAndValue(column string, value any) PrimaryKeyAndValue {
	return PrimaryKeyAndValue{Column: column, Value: value}
}

// NewPrimaryKeyAndValues creates a parameter object.
func NewPrimaryKeyAndValues(column string, value ...any) PrimaryKeysAndValues {
	return PrimaryKeysAndValues{column: column, values: value}
}

// MewPrimaryKeysAndValues creates a parameter object.
func NewPrimaryKeysAndValues(pairs []PrimaryKeyAndValue) PrimaryKeysAndValues {
	return PrimaryKeysAndValues{pairs: pairs}
}
