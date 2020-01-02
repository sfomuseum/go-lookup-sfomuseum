package main

import (
	"context"
	"fmt"
	"github.com/sfomuseum/go-lookup-sfomuseum"
	"log"
)

func main() {

	ctx := context.Background()

	c, err := sfomuseum.NewGatesLookup(ctx)

	if err != nil {
		log.Fatal(err)
	}

	body, err := sfomuseum.MarshalCatalog(c)

	if err != nil {
		log.Fatal(err)
	}

	fmt.Println(string(body))

	// TO DO: write to blob.Bucket...
}
