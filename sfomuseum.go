package sfomuseum

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/sfomuseum/go-lookup"
	"github.com/sfomuseum/go-lookup-blob"
	"github.com/sfomuseum/go-lookup-git"
	"github.com/sfomuseum/go-lookup/catalog"
	"github.com/tidwall/pretty"
	"net/url"
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

func NewCatalog(ctx context.Context, uri string) (lookup.Catalog, error) {

	u, err := url.Parse(uri)

	if err != nil {
		return nil, err
	}

	var opts *CatalogOptions
	var opts_err error

	switch u.Scheme {
	case "airlines":
		opts, opts_err = DefaultAirlinesCatalogOptions()
	case "gates":
		opts, opts_err = DefaultGatesCatalogOptions()
	default:
		return nil, errors.New("Unsupported lookup")
	}

	if opts_err != nil {
		return nil, opts_err
	}

	var lu lookup.LookerUpper

	switch u.Host {
	case "blob":
		lu = blob.NewBlobLookerUpper(ctx)
	case "git":
		lu = git.NewGitLookerUpper(ctx)
	default:
		return nil, errors.New("Unsupported looker upper")
	}

	q := u.Query()
	lu_uri := q.Get("uri")

	err = lu.Open(ctx, lu_uri)

	if err != nil {
		return nil, err
	}

	opts.LookerUppers = append(opts.LookerUppers, lu)
	return NewCatalogWithOptions(ctx, opts)
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
