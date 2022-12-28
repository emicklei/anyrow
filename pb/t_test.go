package pb

import (
	"testing"

	"google.golang.org/protobuf/encoding/protojson"
)

func TestT(t *testing.T) {
	m := new(T)
	m.CustomValues = map[string]string{
		"key": "value",
	}
	t.Log("\n", protojson.Format(m))
}
