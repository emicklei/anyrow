package anyrow

import (
	"fmt"
	"strconv"
	"strings"
)

type filterOption func(f fetchFilter) fetchFilter

type fetchFilter struct {
	pkv   PrimaryKeysAndValues
	where string
	limit int
}

func (f fetchFilter) whereOn(b *strings.Builder) {
	// either one key with one or more values
	if f.pkv.column != "" {
		b.WriteString(f.pkv.column)
		fmt.Fprintf(b, " IN (%s)", composeQueryParams(len(f.pkv.values)))
		return
	}
	// or multiple keys with one value or more values
	if len(f.pkv.pairs) > 1 {
		// chain of ANDs:  (p1=v1 and p2=v2)
		b.WriteRune('(')
		p := 1
		for i, each := range f.pkv.pairs {
			if i > 0 {
				b.WriteString(" AND ")
			}
			fmt.Fprintf(b, "%s=$%d", each.Column, p)
			p++
		}
		b.WriteRune(')')
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
