package sfomuseum

import (
	"context"
	"errors"
	"fmt"
	"github.com/sfomuseum/go-lookup"
	"github.com/tidwall/gjson"
	"io"
	"io/ioutil"
	"net/url"
	"log"
)

// airlines://git?uri=
// airlines://blob?uri=

const SFOMUSEUM_DATA_AIRLINES string = "https://github.com/sfomuseum-data/sfomuseum-data-enterprise.git"

func DefaultAirlinesGitURI() string {
	return NewAirlinesGitURI(SFOMUSEUM_DATA_AIRLINES)
}

func NewAirlinesGitURI(uri string) string {	
	return NewAirlinesURI("git", uri)
}

func NewAirlinesBlobURI(uri string) string {
	return NewAirlinesURI("blob", uri)
}

func NewAirlinesURI(lu_scheme string, uri string) string {
	
	u := url.URL{}
	u.Scheme = "airlines"
	u.Host = lu_scheme

	p := url.Values{}
	p.Set("uri", uri)

	log.Println("URI", uri)
	
	u.RawQuery = p.Encode()
	return u.String()
}

func DefaultAirlinesCatalogOptions() (*CatalogOptions, error) {

	opts, err := DefaultCatalogOptions()

	if err != nil {
		return nil, err
	}

	opts.AppendFuncs = append(opts.AppendFuncs, AppendAirlineFunc)
	return opts, nil
}

func AppendAirlineFunc(ctx context.Context, lu lookup.Catalog, fh io.ReadCloser) error {

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

	if pt_rsp.String() != "airline" {
		return nil
	}

	c_rsp := gjson.GetBytes(body, "properties.mz:is_current")

	if !c_rsp.Exists() {
		return nil
	}

	if c_rsp.Int() != 1 {
		return nil
	}

	codes := make([]string, 0)

	possible_codes := []string{
		"iata:code",
		"icao:code",
	}

	for _, rel_path := range possible_codes {

		abs_path := fmt.Sprintf("properties.wof:concordances.%s", rel_path)

		rsp := gjson.GetBytes(body, abs_path)

		if rsp.Exists() {
			codes = append(codes, rsp.String())
		}
	}

	if len(codes) == 0 {
		return nil
	}

	id := id_rsp.Int()

	for _, c := range codes {

		fmt.Printf("APPEND '%s' : '%d'\n", c, id)

		has_id, exists := lu.LoadOrStore(c, id)

		if exists && id != has_id.(int64) {
			msg := fmt.Sprintf("Existing airline for '%s' (%d). Has ID: %d", c, id, has_id.(int64))
			return errors.New(msg)
		}
	}

	return nil
}
