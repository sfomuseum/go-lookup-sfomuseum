package sfomuseum

import (
	_ "gocloud.dev/blob/fileblob"
)

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/sfomuseum/go-lookup"
	_ "github.com/sfomuseum/go-lookup-blob"
	_ "github.com/sfomuseum/go-lookup-git"
	"github.com/sfomuseum/go-lookup/catalog"
	"github.com/sfomuseum/go-lookup/iterator"
	"github.com/tidwall/pretty"
	"net/url"
)

const SFOMUSEUM_DATA_MEDIA string = "https://github.com/sfomuseum-data/sfomuseum-data-media.git"
const SFOMUSEUM_DATA_ARCHITECTURE string = "https://github.com/sfomuseum-data/sfomuseum-data-architecture.git"
const SFOMUSEUM_DATA_WHOSONFIRST string = "https://github.com/sfomuseum-data/sfomuseum-data-whosonfirst.git"
const SFOMUSEUM_DATA_ENTERPRISE string = "https://github.com/sfomuseum-data/sfomuseum-data-enterprise.git"
const SFOMUSEUM_DATA_FLIGHTS string = "https://github.com/sfomuseum-data/sfomuseum-data-flights-%s.git"
const SFOMUSEUM_DATA_FAA string = "https://github.com/sfomuseum-data/sfomuseum-data-faa-%s.git"

type CatalogOptions struct {
	Catalog     catalog.Catalog
	AppendFuncs []iterator.AppendLookupFunc
	Iterators   []iterator.Iterator
}

func DefaultCatalogOptions() (*CatalogOptions, error) {

	ctx := context.Background()
	c, err := catalog.NewCatalog(ctx, "syncmap://")

	if err != nil {
		return nil, err
	}

	funcs := make([]iterator.AppendLookupFunc, 0)
	lookers := make([]iterator.Iterator, 0)

	opts := &CatalogOptions{
		Catalog:     c,
		AppendFuncs: funcs,
		Iterators:   lookers,
	}

	return opts, nil
}

func NewIteratorURI(scheme string, lu_scheme string, uri string) string {

	u := url.URL{}
	u.Scheme = scheme
	u.Host = lu_scheme

	p := url.Values{}
	p.Set("uri", uri)

	u.RawQuery = p.Encode()
	return u.String()
}

func NewCatalog(ctx context.Context, uri string) (catalog.Catalog, error) {

	u, err := url.Parse(uri)

	if err != nil {
		return nil, err
	}

	var opts *CatalogOptions
	var opts_err error

	switch u.Scheme {
	case "airlines":
		opts, opts_err = DefaultAirlinesCatalogOptions()
	case "airports":
		opts, opts_err = DefaultAirportsCatalogOptions()
	case "faa":
		opts, opts_err = DefaultFAACatalogOptions()
	case "flights":
		opts, opts_err = DefaultFlightsCatalogOptions()
	case "gates":
		opts, opts_err = DefaultGatesCatalogOptions()
	case "images":
		opts, opts_err = DefaultMediaImagesCatalogOptions()
	default:
		return nil, errors.New("Unsupported iterator")
	}

	if opts_err != nil {
		return nil, opts_err
	}

	q := u.Query()
	lu_uri := q.Get("uri")

	lu, err := iterator.NewIterator(ctx, lu_uri)

	if err != nil {
		return nil, err
	}

	opts.Iterators = append(opts.Iterators, lu)
	return NewCatalogWithOptions(ctx, opts)
}

func NewCatalogWithOptions(ctx context.Context, opts *CatalogOptions) (catalog.Catalog, error) {

	err := lookup.SeedCatalog(ctx, opts.Catalog, opts.Iterators, opts.AppendFuncs)

	if err != nil {
		return nil, err
	}

	return opts.Catalog, nil
}

func MarshalCatalog(c catalog.Catalog) ([]byte, error) {

	lookup := make(map[string]interface{})

	c.Range(func(key interface{}, value interface{}) bool {
		gate_name := key.(string)
		lookup[gate_name] = value
		return true
	})

	body, err := json.Marshal(lookup)

	if err != nil {
		return nil, err
	}

	body = pretty.Pretty(body)
	return body, nil
}
