package sfomuseum

import (
	"encoding/json"
	"github.com/sfomuseum/go-lookup"
	"github.com/tidwall/pretty"
)

func MarshalCatalog(c lookup.Catalog) ([]byte, error) {

	lookup := make(map[string]int64)

	c.Range(func(key interface{}, value interface{}) bool {
		gate_name := key.(string)
		wof_id := value.(int64)
		lookup[gate_name] = wof_id
		return true
	})

	body, err := json.Marshal(lookup)

	if err != nil {
		return nil, err
	}

	body = pretty.Pretty(body)
	return body, nil
}
