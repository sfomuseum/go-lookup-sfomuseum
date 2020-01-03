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
)

const SFOMUSEUM_DATA_ARCHITECTURE string = "https://github.com/sfomuseum-data/sfomuseum-data-architecture.git"

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

func NewGatesLookupFromGit(ctx context.Context) (lookup.Catalog, error) {

	lu, err := NewGatesGitLookerUpper(ctx)

	if err != nil {
		return nil, err
	}

	return NewGatesLookup(ctx, lu)
}

func NewGatesLookupFromBlob(ctx context.Context, uri string) (lookup.Catalog, error) {

	lu, err := NewGatesBlobLookerUpper(ctx, uri)

	if err != nil {
		return nil, err
	}

	return NewGatesLookup(ctx, lu)
}

func NewGatesLookup(ctx context.Context, looker_uppers ...lookup.LookerUpper) (lookup.Catalog, error) {

	c, err := catalog.NewSyncMapCatalog()

	if err != nil {
		return nil, err
	}

	append_funcs := []lookup.AppendLookupFunc{
		AppendGateFunc,
	}

	err = lookup.SeedCatalog(ctx, c, looker_uppers, append_funcs)

	if err != nil {
		return nil, err
	}

	return c, nil
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
