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

	for _, gate_name := range flag.Args() {

		rsp, ok := c.Load(gate_name)

		if !ok {
			log.Printf("Unable to load %s\n", gate_name)
			continue
		}

		fmt.Printf("%s\t%d\n", gate_name, rsp.(int64))
	}
}
