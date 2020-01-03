package sfomuseum

import (
	"context"
	"encoding/json"
	"github.com/sfomuseum/go-lookup"
	"github.com/sfomuseum/go-lookup/catalog"
	"github.com/tidwall/pretty"
)

type CatalogOptions struct {
	Catalog      lookup.Catalog
	AppendFuncs  []lookup.AppendLookupFunc
	LookerUppers []lookup.LookerUpper
}

func DefaultCatalogOptions() (*CatalogOptions, error) {

	c, err := catalog.NewSyncMapCatalog()

	if err != nil {
		return nil, err
	}

	funcs := make([]lookup.AppendLookupFunc, 0)
	lookers := make([]lookup.LookerUpper, 0)

	opts := &CatalogOptions{
		Catalog:      c,
		AppendFuncs:  funcs,
		LookerUppers: lookers,
	}

	return opts, nil
}

func NewCatalogWithOptions(ctx context.Context, opts *CatalogOptions) (lookup.Catalog, error) {

	err := lookup.SeedCatalog(ctx, opts.Catalog, opts.LookerUppers, opts.AppendFuncs)

	if err != nil {
		return nil, err
	}

	return opts.Catalog, nil
}

func MarshalCatalog(c lookup.Catalog) ([]byte, error) {

	lookup := make(map[string]int64)

	c.Range(func(key interface{}, value interface{}) bool {
		gate_name := key.(string)
		wof_id := value.(int64)
		lookup[gate_name] = wof_id
		return true
	})

	body, err := json.Marshal(lookup)

	if err != nil {
		return nil, err
	}

	body = pretty.Pretty(body)
	return body, nil
}
