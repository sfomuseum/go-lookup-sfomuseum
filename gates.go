package sfomuseum

import (
	"context"
	"errors"
	"fmt"
	"github.com/sfomuseum/go-lookup"
	"github.com/sfomuseum/go-lookup-blob"
	"github.com/sfomuseum/go-lookup-git"
	"github.com/sfomuseum/go-lookup/catalog"
	"github.com/tidwall/gjson"
	"io"
	"io/ioutil"
	_ "log"
	"net/url"
)

const SFOMUSEUM_DATA_ARCHITECTURE string = "https://github.com/sfomuseum-data/sfomuseum-data-architecture.git"

type GatesCatalogOptions struct {
	Catalog      lookup.Catalog
	AppendFuncs  []lookup.AppendLookupFunc
	LookerUppers []lookup.LookerUpper
}

func DefaultGatesCatalogOptions() (*GatesCatalogOptions, error) {

	c, err := catalog.NewSyncMapCatalog()

	if err != nil {
		return nil, err
	}

	funcs := []lookup.AppendLookupFunc{
		AppendGateFunc,
	}

	lookers := make([]lookup.LookerUpper, 0)

	opts := &GatesCatalogOptions{
		Catalog:      c,
		AppendFuncs:  funcs,
		LookerUppers: lookers,
	}

	return opts, nil
}

func NewGatesCatalog(ctx context.Context, uri string) (lookup.Catalog, error) {

	u, err := url.Parse(uri)

	if err != nil {
		return nil, err
	}

	switch u.Scheme {
	case "git":
		return NewGatesCatalogFromGit(ctx)
	case "file", "s3":
		return NewGatesCatalogFromBlob(ctx, uri)
	default:
		// pass
	}

	return nil, errors.New("Unknown gates looker upper scheme")
}

func NewGatesGitLookerUpper(ctx context.Context) (lookup.LookerUpper, error) {

	lu := git.NewGitLookerUpper(ctx)

	err := lu.Open(ctx, SFOMUSEUM_DATA_ARCHITECTURE)

	if err != nil {
		return nil, err
	}

	return lu, nil
}

func NewGatesBlobLookerUpper(ctx context.Context, uri string) (lookup.LookerUpper, error) {

	lu := blob.NewBlobLookerUpper(ctx)

	err := lu.Open(ctx, uri)

	if err != nil {
		return nil, err
	}

	return lu, nil
}

func NewGatesCatalogFromGit(ctx context.Context) (lookup.Catalog, error) {

	opts, err := DefaultGatesCatalogOptions()

	if err != nil {
		return nil, err
	}

	lu, err := NewGatesGitLookerUpper(ctx)

	if err != nil {
		return nil, err
	}

	opts.LookerUppers = append(opts.LookerUppers, lu)

	return NewGatesCatalogWithOptions(ctx, opts)
}

func NewGatesCatalogFromBlob(ctx context.Context, uri string) (lookup.Catalog, error) {

	opts, err := DefaultGatesCatalogOptions()

	if err != nil {
		return nil, err
	}

	lu, err := NewGatesBlobLookerUpper(ctx, uri)

	if err != nil {
		return nil, err
	}

	opts.LookerUppers = append(opts.LookerUppers, lu)

	return NewGatesCatalogWithOptions(ctx, opts)
}

func NewGatesCatalogWithOptions(ctx context.Context, opts *GatesCatalogOptions) (lookup.Catalog, error) {

	err := lookup.SeedCatalog(ctx, opts.Catalog, opts.LookerUppers, opts.AppendFuncs)

	if err != nil {
		return nil, err
	}

	return opts.Catalog, nil
}

func AppendGateFunc(ctx context.Context, lu lookup.Catalog, fh io.ReadCloser) error {

	body, err := ioutil.ReadAll(fh)

	if err != nil {
		return err
	}

	id_rsp := gjson.GetBytes(body, "properties.wof:id")

	if !id_rsp.Exists() {
		return nil
	}

	pt_rsp := gjson.GetBytes(body, "properties.sfomuseum:placetype")

	if !pt_rsp.Exists() {
		return nil
	}

	if pt_rsp.String() != "gate" {
		return nil
	}

	c_rsp := gjson.GetBytes(body, "properties.mz:is_current")

	if !c_rsp.Exists() {
		return nil
	}

	if c_rsp.Int() != 1 {
		return nil
	}

	names := make([]string, 0)

	gt_rsp := gjson.GetBytes(body, "properties.flysfo:gate_code")

	if !gt_rsp.Exists() {
		return nil
	}

	names = append(names, gt_rsp.String())

	// because apron gates (F15M) rather than terminal gates (F15)

	alt_rsp := gjson.GetBytes(body, "properties.flysfo:gate_code_alt")

	if alt_rsp.Exists() {

		for _, rsp := range alt_rsp.Array() {
			names = append(names, rsp.String())
		}
	}

	id := id_rsp.Int()

	for _, gt := range names {

		// fmt.Printf("APPEND '%s' : '%d' is current: %d\n", gt, id, c_rsp.Int())

		has_id, exists := lu.LoadOrStore(gt, id)

		if exists && id != has_id.(int64) {
			msg := fmt.Sprintf("Existing gate key for '%s' (%d). Has ID: %d", gt, id, has_id.(int64))
			return errors.New(msg)
		}
	}

	return nil
}
