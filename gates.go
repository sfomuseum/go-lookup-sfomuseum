package sfomuseum

import (
	"context"
	"github.com/sfomuseum/go-lookup"
	"github.com/sfomuseum/go-lookup/catalog"
	"github.com/sfomuseum/go-lookup-git"
	"github.com/sfomuseum/go-lookup-sfomuseum/append"	
)

const SFOMUSEUM_DATA_ARCHITECTURE string = "https://github.com/sfomuseum-data/sfomuseum-data-architecture.git"

func NewGatesLookup(ctx context.Context) (lookup.Catalog, error) {

	c, err := catalog.NewSyncMapCatalog()

	if err != nil {
		return nil, err
	}

	lu := git.NewGitLookerUpper(ctx)

	err = lu.Open(ctx, SFOMUSEUM_DATA_ARCHITECTURE)

	if err != nil {
		return nil, err
	}
	
	looker_uppers := []lookup.LookerUpper{
		lu,
	}

	append_funcs := []lookup.AppendLookupFunc{
		append.AppendGateFunc,
	}
	
	err = lookup.SeedCatalog(ctx, c, looker_uppers, append_funcs)

	if err != nil {
		return nil, err
	}

	return c, nil
}
