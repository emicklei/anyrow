package anyrow

import (
	"strings"
	"testing"
)

func TestFetchFilter_WhereOn(t *testing.T) {
	var b strings.Builder

	// Test for one key with one or more values
	f := fetchFilter{pkv: PrimaryKeysAndValues{column: "name", values: []any{"Bob", "Alice"}}}
	f.whereOn(&b)
	if b.String() != "name IN ($1,$2)" {
		t.Errorf("unexpected query: %q", b.String())
	}

	// Test for multiple keys with one value or more values
	b.Reset()
	f = fetchFilter{pkv: PrimaryKeysAndValues{pairs: []PrimaryKeyAndValue{{"name", "Bob"}, {"age", 20}, {"city", "San Francisco"}}}}
	f.whereOn(&b)
	if b.String() != "(name=$1 AND age=$2 AND city=$3)" {
		t.Errorf("unexpected query: %q", b.String())
	}
	pvs := f.pkv.parameterValues()
	if pvs[0] != "Bob" || pvs[1] != 20 || pvs[2] != "San Francisco" {
		t.Errorf("unexpected parameter values: %v", pvs)
	}

	// Test for a custom WHERE condition
	b.Reset()
	f = fetchFilter{where: "created_at > '2021-01-01'"}
	f.whereOn(&b)
	if b.String() != "created_at > '2021-01-01'" {
		t.Errorf("unexpected query: %q", b.String())
	}

	// Test for both empty columns and WHERE condition
	b.Reset()
	f = fetchFilter{PrimaryKeysAndValues{}, "", 0}
	f.whereOn(&b)
	if b.String() != "true" {
		t.Errorf("unexpected query: %q", b.String())
	}
}
