package sfomuseum

import (
	"context"
	"errors"
	"fmt"
	"github.com/sfomuseum/go-lookup"
	"github.com/tidwall/gjson"
	"io"
	"io/ioutil"
	_ "log"
)

func DefaultGatesGitURI() string {
	return NewGatesGitURI(SFOMUSEUM_DATA_ARCHITECTURE)
}

func NewGatesGitURI(uri string) string {
	return NewGatesURI("git", uri)
}

func NewGatesBlobURI(uri string) string {
	return NewGatesURI("blob", uri)
}

func NewGatesURI(lu_scheme string, uri string) string {
	return NewLookupURI("gates", lu_scheme, uri)
}

func DefaultGatesCatalogOptions() (*CatalogOptions, error) {

	opts, err := DefaultCatalogOptions()

	if err != nil {
		return nil, err
	}

	opts.AppendFuncs = append(opts.AppendFuncs, AppendGateFunc)

	return opts, nil
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
