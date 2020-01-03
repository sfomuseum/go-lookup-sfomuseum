package main

import (
	"context"
	"flag"
	"fmt"
	"github.com/sfomuseum/go-lookup-sfomuseum"
	_ "gocloud.dev/blob/fileblob"
	"log"
)

func main() {

	default_source := sfomuseum.DefaultGatesGitURI()
	source := flag.String("source", default_source, "")

	flag.Parse()

	ctx := context.Background()

	c, err := sfomuseum.NewCatalog(ctx, *source)

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
