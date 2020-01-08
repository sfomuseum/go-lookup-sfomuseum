package sfomuseum

import (
	"context"
	_ "fmt"
	"testing"
)

func TestAirportsGitLookup(t *testing.T) {

	tests := map[string]int64{
		"SFO": int64(102527513),
		"SYD": int64(102557783),
		"HND": int64(102541591),
		"PIT": int64(102528181),
		"DCA": int64(102532159),
		"CAN": int64(102550391),
	}

	ctx := context.Background()
	uri := DefaultAirportsGitURI()

	c, err := NewCatalog(ctx, uri)

	if err != nil {
		t.Fatal(err)
	}

	for airport_name, expected_id := range tests {

		id, ok := c.Load(airport_name)

		if !ok {
			t.Fatalf("Missing entry for key '%s'", airport_name)
		}

		id64 := id.(int64)

		if id64 != expected_id {
			t.Fatalf("Invalid result for key '%s'. Expected '%d' but got '%d'.", airport_name, id64, expected_id)
		}
	}
}
