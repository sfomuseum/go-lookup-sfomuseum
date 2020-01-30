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

const SFOMUSEUM_DATA_FAA string = "https://github.com/sfomuseum-data/sfomuseum-data-faa-%s.git"

// IMPORTANT: THIS DOES NOT SUPPORT MULTIPLE YYYYMM REPOS
// IT SHOULD (maybe...) BUT IT TODAY IT DOES NOT
// (20200108/thisisaaronland)

func DefaultFAAGitURI(yyyy string) string {
	uri := fmt.Sprintf(SFOMUSEUM_DATA_FAA, yyyy)
	return NewFAAGitURI(uri)
}

func NewFAAGitURI(uri string) string {
	return NewFAAURI("git", uri)
}

func NewFAABlobURI(uri string) string {
	return NewFAAURI("blob", uri)
}

func NewFAAURI(lu_scheme string, uri string) string {
	return NewLookupURI("FAA", lu_scheme, uri)
}

func DefaultFAACatalogOptions() (*CatalogOptions, error) {

	opts, err := DefaultCatalogOptions()

	if err != nil {
		return nil, err
	}

	opts.AppendFuncs = append(opts.AppendFuncs, AppendFAAFunc)

	return opts, nil
}

func AppendFAAFunc(ctx context.Context, lu lookup.Catalog, fh io.ReadCloser) error {

	body, err := ioutil.ReadAll(fh)

	if err != nil {
		return err
	}

	id_rsp := gjson.GetBytes(body, "properties.wof:id")

	if !id_rsp.Exists() {
		return nil
	}

	fl_rsp := gjson.GetBytes(body, "properties.sfomuseum:uri")

	if !fl_rsp.Exists() {
		return nil
	}

	wof_id := id_rsp.Int()
	fl_id := fl_rsp.String()

	has_id, exists := lu.LoadOrStore(fl_id, wof_id)

	if exists && wof_id != has_id.(int64) {
		msg := fmt.Sprintf("Existing FAA status for '%s' (%d). Has ID: %d", fl_id, wof_id, has_id.(int64))
		return errors.New(msg)
	}

	return nil
}
