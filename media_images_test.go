package sfomuseum

import (
	"context"
	_ "fmt"
	"testing"
)

func TestMediaImagesGitLookup(t *testing.T) {

	tests := map[string]int64{
		"64217": int64(1377455125),
	}

	ctx := context.Background()
	uri := DefaultMediaImagesGitURI()

	c, err := NewCatalog(ctx, uri)

	if err != nil {
		t.Fatalf("Failed to create catalog with '%s', %v", uri, err)
	}

	for img_id, expected_id := range tests {

		id, ok := c.Load(img_id)

		if !ok {
			t.Fatalf("Missing entry for key '%s'", img_id)
		}

		id64 := id.(int64)

		if id64 != expected_id {
			t.Fatalf("Invalid result for key '%s'. Expected '%d' but got '%d'.", img_id, id64, expected_id)
		}
	}
}
