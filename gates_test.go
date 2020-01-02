package sfomuseum

import (
	"context"
	_ "fmt"
	"testing"
)

func TestGatesLookup(t *testing.T) {

	tests := map[string]int64{
		"E12":  int64(1477930403),
		"F15":  int64(1477930443),
		"F15M": int64(1477930443),
	}

	ctx := context.Background()

	c, err := NewGatesLookup(ctx)

	if err != nil {
		t.Fatal(err)
	}

	for gate_name, expected_id := range tests {

		id, ok := c.Load(gate_name)

		if !ok {
			t.Fatalf("Missing entry for key '%s'", gate_name)
		}

		id64 := id.(int64)

		if id64 != expected_id {
			t.Fatalf("Invalid result for key '%s'. Expected '%d' but got '%d'.", gate_name, id64, expected_id)
		}
	}
}
