package anyrow

import (
	"fmt"
	"strconv"
	"strings"
)

type filterOption func(f fetchFilter) fetchFilter

type fetchFilter struct {
	pkv   PrimaryKeyAndValues
	where string
	limit int
}

func (f fetchFilter) whereOn(b *strings.Builder) {
	if len(f.pkv.Values) > 0 {
		b.WriteString(f.pkv.Column)
		fmt.Fprintf(b, " IN (%s)", composeQueryParams(len(f.pkv.Values)))
		return
	}
	if f.where != "" {
		b.WriteString(f.where)
		return
	}
	// both empty
	b.WriteString("true")
}

func (f fetchFilter) limitOn(b *strings.Builder) {
	if f.limit > 0 {
		b.WriteString(" LIMIT ")
		b.WriteString(strconv.Itoa(f.limit))
	}
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
