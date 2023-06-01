package anyrow

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strings"

	"github.com/emicklei/anyrow/pb"
	"github.com/jackc/pgx/v5/pgconn"
)

var defaultFilterLimit = 1000

func fetchValues(ctx context.Context, conn Querier, metaSet *pb.RowSet, filter fetchFilter, collector valueCollector) error {
	// get values
	qb := new(strings.Builder)
	qb.WriteString("SELECT ")
	for i, each := range metaSet.ColumnSchemas {
		if i > 0 {
			qb.WriteRune(',')
		}
		fmt.Fprintf(qb, "to_json(\"%s\")", each.Name)
	}
	qb.WriteString(" FROM ")
	qb.WriteString(metaSet.TableName)
	qb.WriteString(" WHERE ")
	filter.whereOn(qb)
	if !filter.pkv.hasValues() {
		filter.limitOn(qb)
	}
	dbrows, err := conn.Query(ctx, qb.String(), filter.pkv.parameterValues()...) // parameterValues can be empty
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
