package anyrow

import (
	"fmt"
	"strings"
)

type fetchFilter struct {
	pkv   PrimaryKeyAndValues
	where string
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
