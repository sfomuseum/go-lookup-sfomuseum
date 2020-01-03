package main

import (
	"context"
	"flag"
	"fmt"
	"github.com/sfomuseum/go-lookup"	
	"github.com/sfomuseum/go-lookup-sfomuseum"
	_ "gocloud.dev/blob/fileblob"	
	"log"
)

func main() {

	source := flag.String("source", "", "")

	flag.Parse()
	
	ctx := context.Background()

	var c lookup.Catalog
	var err error
	
	switch *source {
	case "":
		c, err = sfomuseum.NewGatesLookupFromGit(ctx)
	default:
		c, err = sfomuseum.NewGatesLookupFromBlob(ctx, *source)	
	}
	
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
