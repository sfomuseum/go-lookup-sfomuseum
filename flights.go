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

const SFOMUSEUM_DATA_FLIGHTS string = "https://github.com/sfomuseum-data/sfomuseum-data-flights-%s.git"

// IMPORTANT: THIS DOES NOT SUPPORT MULTIPLE YYYYMM REPOS
// IT SHOULD (maybe...) BUT IT TODAY IT DOES NOT
// (20200108/thisisaaronland)

func DefaultFlightsGitURI(yyyymm string) string {
	uri := fmt.Sprintf(SFOMUSEUM_DATA_FLIGHTS, yyyymm)
	return NewFlightsGitURI(uri)
}

func NewFlightsGitURI(uri string) string {
	return NewFlightsURI("git", uri)
}

func NewFlightsBlobURI(uri string) string {
	return NewFlightsURI("blob", uri)
}

func NewFlightsURI(lu_scheme string, uri string) string {
	return NewLookupURI("flights", lu_scheme, uri)
}

func DefaultFlightsCatalogOptions() (*CatalogOptions, error) {

	opts, err := DefaultCatalogOptions()

	if err != nil {
		return nil, err
	}

	opts.AppendFuncs = append(opts.AppendFuncs, AppendFlightFunc)

	return opts, nil
}

func AppendFlightFunc(ctx context.Context, lu lookup.Catalog, fh io.ReadCloser) error {

	body, err := ioutil.ReadAll(fh)

	if err != nil {
		return err
	}

	id_rsp := gjson.GetBytes(body, "properties.wof:id")

	if !id_rsp.Exists() {
		return nil
	}

	fl_rsp := gjson.GetBytes(body, "properties.sfomuseum:flight_id")

	if !fl_rsp.Exists() {
		return nil
	}

	wof_id := id_rsp.Int()
	fl_id := fl_rsp.String()

	has_id, exists := lu.LoadOrStore(fl_id, wof_id)

	if exists && wof_id != has_id.(int64) {
		msg := fmt.Sprintf("Existing flight for '%s' (%d). Has ID: %d", fl_id, wof_id, has_id.(int64))
		return errors.New(msg)
	}

	return nil
}
