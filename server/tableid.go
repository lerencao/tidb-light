package server

import (
	"encoding/json"
	"fmt"
	"github.com/juju/errors"
	"io/ioutil"
	"net/http"
)

// TableId send a http req to remote tidb http endpoint to get table info
func TableId(tidbEndpoint string, schema, table string) (int64, error) {
	resp, err:= http.DefaultClient.Get(fmt.Sprintf("%s/schema/%s/%s", tidbEndpoint, schema, table))
	if err != nil {
		return 0, errors.Trace(err)
	}
	defer resp.Body.Close()
	data, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return 0, errors.Trace(err)
	}

	if resp.StatusCode > 300 {
		return 0, errors.Errorf("request table id failed, status: %s, error: %s", resp.Status, string(data))
	}


	respData := make(map[string]interface{})
	err = json.Unmarshal(data, &respData)
	if err != nil {
		return 0, errors.Trace(err)
	}


	return int64(respData["id"].(float64)), nil
}
