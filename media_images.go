package sfomuseum

import (
	"context"
	"fmt"
	"github.com/sfomuseum/go-lookup/catalog"
	"github.com/tidwall/gjson"
	"io"
	_ "log"
)

func DefaultMediaImagesGitURI() string {
	return NewMediaImagesGitURI(SFOMUSEUM_DATA_MEDIA)
}

func NewMediaImagesGitURI(uri string) string {
	return NewMediaImagesURI("git", uri)
}

func NewMediaImagesBlobURI(uri string) string {
	return NewMediaImagesURI("blob", uri)
}

func NewMediaImagesURI(lu_scheme string, uri string) string {
	return NewIteratorURI("images", lu_scheme, uri)
}

func DefaultMediaImagesCatalogOptions() (*CatalogOptions, error) {

	opts, err := DefaultCatalogOptions()

	if err != nil {
		return nil, err
	}

	opts.AppendFuncs = append(opts.AppendFuncs, AppendMediaImagesFunc)

	return opts, nil
}

func AppendMediaImagesFunc(ctx context.Context, lu catalog.Catalog, fh io.ReadCloser) error {

	body, err := io.ReadAll(fh)

	if err != nil {
		return err
	}

	deprecated_rsp := gjson.GetBytes(body, "properties.edtf:deprecated")

	if deprecated_rsp.Exists() && deprecated_rsp.String() != "" {
		return nil
	}

	id_rsp := gjson.GetBytes(body, "properties.wof:id")

	if !id_rsp.Exists() {
		return nil
	}

	pt_rsp := gjson.GetBytes(body, "properties.sfomuseum:placetype")

	if !pt_rsp.Exists() {
		return nil
	}

	if pt_rsp.String() != "image" {
		return nil
	}

	img_rsp := gjson.GetBytes(body, "properties.sfomuseum:image_id")

	if !img_rsp.Exists() {
		return nil
	}

	wof_id := id_rsp.Int()
	img_id := img_rsp.String()

	has_id, exists := lu.LoadOrStore(img_id, wof_id)

	if exists && wof_id != has_id.(int64) {
		return fmt.Errorf("Existing media key for '%s' (%d). Has ID: %d", img_id, wof_id, has_id.(int64))
	}

	return nil
}
