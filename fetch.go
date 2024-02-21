package anyrow

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"strings"

	"github.com/emicklei/anyrow/pb"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
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
		fmt.Fprint(qb, each.Name)
	}
	qb.WriteString(" FROM ")
	qb.WriteString(metaSet.TableName)
	qb.WriteString(" WHERE ")
	filter.whereOn(qb)
	if !filter.pkv.hasValues() {
		filter.limitOn(qb)
	}
	sql := qb.String()
	slog.Debug("fetchValues", "sql", sql, "params", filter.pkv.parameterValues())
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
			case int64:
				collector.storeInt64(i, each.(int64))
			case float64:
				// TODO why 64->32 ???
				f := each.(float64)
				// check for integer like
				tn := metaSet.ColumnSchemas[i].TypeName
				if strings.Contains("integer bigint smallint", tn) {
					fint, _ := math.Modf(f)
					collector.storeInt64(i, int64(fint))
				} else {
					// is a float like
					collector.storeFloat32(i, float32(f))
				}
			case map[string]any, []any:
				collector.storeDefault(i, each)
			case bool:
				collector.storeBool(i, each.(bool))
			case [16]uint8:
				// handle as pgtype.UUID
				collector.storeString(i, UUIDToString(each.([16]uint8)))
			case pgtype.Numeric:
				collector.storeDefault(i, each)
			default:
				slog.Debug("[anyrow] handled as object", "value", each, "value.type", fmt.Sprintf("%T", each))
				collector.storeDefault(i, each)
			}
		}
	}
	return nil
}

// UUIDToString returns format xxxx-yyyy-zzzz-rrrr-tttt
func UUIDToString(src [16]uint8) string {
	return fmt.Sprintf("%x-%x-%x-%x-%x", src[0:4], src[4:6], src[6:8], src[8:10], src[10:16])
}
