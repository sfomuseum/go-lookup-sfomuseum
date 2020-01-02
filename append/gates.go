package append

import (
	"context"
	"errors"
	"fmt"
	"github.com/sfomuseum/go-lookup"
	"github.com/tidwall/gjson"
	"io"
	"io/ioutil"
)

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

	if !pt_rsp.Exists(){
		return nil
	}

	if pt_rsp.String() != "gate"{
		return nil
	}
	
	gt_rsp := gjson.GetBytes(body, "properties.flysfo:gate_code")

	if !gt_rsp.Exists() {
		return nil
	}

	s_rsp := gjson.GetBytes(body, "properties.wof:superseded_by")

	if !s_rsp.Exists(){
		return nil
	}

	if len(s_rsp.Array()) > 0 {
		return nil
	}
	
	gt := gt_rsp.String()
	id := id_rsp.Int()

	has_id, exists := lu.LoadOrStore(gt, id)

	if exists && id != has_id.(int64) {
		msg := fmt.Sprintf("Existing fingerprint key for %s (%d). Has ID: %d", gt, id, has_id.(int64))
		return errors.New(msg)
	}

	fmt.Println(id, gt)
	return nil
}
