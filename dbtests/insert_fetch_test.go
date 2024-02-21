package dbtests

import (
	"context"
	"math"
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
	ctx := context.Background()
	tx, err := testConnect.Begin(ctx)
	check(t, err)
	id := uuid.New()
	t.Log(id)
	tDoublePrecision := math.MaxFloat64
	t.Log(tDoublePrecision)

	_, err = tx.Exec(ctx, `insert into fieldbags (id,tDoublePrecision) values ($1,$2)`, id, tDoublePrecision)
	check(t, err)
	tx.Commit(ctx)

	pkvs := anyrow.NewPrimaryKeyAndValues("id", id)
	rows, err := anyrow.FetchObjects(ctx, testConnect, "cache", "fieldbags", pkvs)
	check(t, err)
	t.Log(rows)
	t.Log(rows[0]["id"])
	ftDoublePrecision := rows[0]["tDoublePrecision"]
	t.Logf("%v (%T)", ftDoublePrecision, ftDoublePrecision)
}
