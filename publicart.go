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

func DefaultPublicArtGitURI() string {
	return NewPublicArtGitURI(SFOMUSEUM_DATA_PUBLICART)
}

func NewPublicArtGitURI(uri string) string {
	return NewPublicArtURI("git", uri)
}

func NewPublicArtBlobURI(uri string) string {
	return NewPublicArtURI("blob", uri)
}

func NewPublicArtURI(lu_scheme string, uri string) string {
	return NewIteratorURI("publicart", lu_scheme, uri)
}

func DefaultPublicArtCatalogOptions() (*CatalogOptions, error) {

	opts, err := DefaultCatalogOptions()

	if err != nil {
		return nil, err
	}

	opts.AppendFuncs = append(opts.AppendFuncs, AppendPublicArtFunc)
	return opts, nil
}

func AppendPublicArtFunc(ctx context.Context, lu catalog.Catalog, fh io.ReadCloser) error {

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

	if pt_rsp.String() != "publicart" {
		return nil
	}

	obj_rsp := gjson.GetBytes(body, "properties.sfomuseum:object_id")

	if !obj_rsp.Exists() {
		return nil
	}

	c_rsp := gjson.GetBytes(body, "properties.mz:is_current")

	if !c_rsp.Exists() {
		return nil
	}

	if c_rsp.Int() != 1 {
		return nil
	}

	obj_id := obj_rsp.String()
	wof_id := id_rsp.Int()

	has_id, exists := lu.LoadOrStore(obj_id, wof_id)

	if exists && wof_id != has_id.(int64) {
		msg := fmt.Sprintf("Existing public art work for '%s' (%d). Has ID: %d", obj_id, wof_id, has_id.(int64))
		return errors.New(msg)
	}

	return nil
}
