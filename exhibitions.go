package sfomuseum

import (
	"context"
	"errors"
	"fmt"
	"github.com/sfomuseum/go-lookup/catalog"
	"github.com/tidwall/gjson"
	"io"
	"io/ioutil"
	_ "log"
)

func DefaultExhibitionsGitURI() string {
	return NewExhibitionsGitURI(SFOMUSEUM_DATA_EXHIBITION)
}

func NewExhibitionsGitURI(uri string) string {
	return NewExhibitionsURI("git", uri)
}

func NewExhibitionsBlobURI(uri string) string {
	return NewExhibitionsURI("blob", uri)
}

func NewExhibitionsURI(lu_scheme string, uri string) string {
	return NewIteratorURI("exhibitions", lu_scheme, uri)
}

func DefaultExhibitionsCatalogOptions() (*CatalogOptions, error) {

	opts, err := DefaultCatalogOptions()

	if err != nil {
		return nil, err
	}

	opts.AppendFuncs = append(opts.AppendFuncs, AppendExhibitionsFunc)
	return opts, nil
}

func AppendExhibitionsFunc(ctx context.Context, lu catalog.Catalog, fh io.ReadCloser) error {

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

	if pt_rsp.String() != "exhibition" {
		return nil
	}

	exh_rsp := gjson.GetBytes(body, "properties.sfomuseum:exhibition_id")

	if !exh_rsp.Exists() {
		return nil
	}

	c_rsp := gjson.GetBytes(body, "properties.mz:is_current")

	if !c_rsp.Exists() {
		return nil
	}

	if c_rsp.Int() != 1 {
		return nil
	}

	exh_id := exh_rsp.String()
	wof_id := id_rsp.Int()

	has_id, exists := lu.LoadOrStore(exh_id, wof_id)

	if exists && wof_id != has_id.(int64) {
		msg := fmt.Sprintf("Existing exhibition for '%s' (%d). Has ID: %d", exh_id, wof_id, has_id.(int64))
		return errors.New(msg)
	}

	return nil
}
