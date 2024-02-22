package dbtests

import (
	"context"
	"strconv"
	"testing"

	"github.com/emicklei/anyrow"
	"github.com/google/uuid"
)

func check(t *testing.T, err error) {
	if err != nil {
		t.Fatal(err)
	}
}
func TestInsertThenFetch(t *testing.T) {
	if testConnect == nil {
		t.Skip("no connection")
	}
	ctx := context.Background()
	tx, err := testConnect.Begin(ctx)
	check(t, err)
	id := uuid.New()
	t.Log(id)
	tDoublePrecision := 1.7976931348623158e30 // math.MaxFloat64 too large, use the IEEE max
	t.Log(tDoublePrecision)
	// TODO
	tNumeric, err := strconv.ParseInt("9066261786704621", 10, 64)
	check(t, err)
	t.Log(tNumeric)
	_, err = tx.Exec(ctx, `insert into fieldbags (id,tdoublePrecision,tNumeric) values ($1,$2,$3)`, id, tDoublePrecision, tNumeric)
	check(t, err)
	tx.Commit(ctx)

	pkvs := anyrow.NewPrimaryKeyAndValues("id", id)
	rows, err := anyrow.FetchObjects(ctx, testConnect, "cache", "fieldbags", pkvs)
	check(t, err)
	t.Log(rows)
	t.Log(rows[0]["id"])
	ftDoublePrecision := rows[0]["tdoubleprecision"]
	t.Logf("%v->%v (%T)", tDoublePrecision, ftDoublePrecision, ftDoublePrecision)
	ftNumeric := rows[0]["tnumeric"]
	t.Logf("%v->%v (%T)", tNumeric, ftNumeric, ftNumeric)
}
