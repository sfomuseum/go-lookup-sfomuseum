package sfomuseum

import (
	"context"
	"testing"
	"fmt"
)

func TestGatesLookup(t *testing.T) {

	ctx := context.Background()
	
	c, err := NewGatesLookup(ctx)

	if err != nil {
		t.Fatal(err)
	}

	fmt.Println(c)
}
