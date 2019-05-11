package server

import (
	"encoding/json"
	"github.com/gorilla/mux"
	"github.com/pingcap/kvproto/pkg/import_sstpb"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/satori/go.uuid"
	"github.com/unrolled/render"
	"net/http"
)

type ImportHandler struct {
	r   *render.Render
	svr *Server
}
type SwitchPdMode struct {
	PdAddr string                  `json:"pd_addr"`
	Mode   import_sstpb.SwitchMode `json:"mode"`
}

func (c *ImportHandler) SwitchMode(w http.ResponseWriter, r *http.Request) {
	param := &SwitchPdMode{}
	defer r.Body.Close()
	if err := json.NewDecoder(r.Body).Decode(param); err != nil {
		c.r.JSON(w, http.StatusBadRequest, err)
		return
	}

	importClient, err := c.svr.GetImportClient()
	if err != nil {
		c.r.JSON(w, http.StatusInternalServerError, err)
		return
	}

	if err = importClient.SwitchMode(r.Context(), param.PdAddr, param.Mode); err != nil {
		c.r.JSON(w, http.StatusInternalServerError, err)
		return
	}

	c.r.JSON(w, http.StatusOK, nil)
}

type ImportEngineParam struct {
	PdAddr string `json:"pd_addr"`
}

func (c *ImportHandler) ImportEngine(w http.ResponseWriter, r *http.Request) {
	engineid := mux.Vars(r)["engineid"]
	engineId, err := uuid.FromString(engineid)
	if err != nil {
		c.r.JSON(w, http.StatusBadRequest, err)
		return
	}

	param := &ImportEngineParam{}
	defer r.Body.Close()
	if err := json.NewDecoder(r.Body).Decode(param); err != nil {
		c.r.JSON(w, http.StatusBadRequest, err)
		return
	}

	importClient, err := c.svr.GetImportClient()
	if err != nil {
		c.r.JSON(w, http.StatusInternalServerError, err)
		return
	}
	if err = importClient.ImportEngine(r.Context(), engineId.Bytes(), param.PdAddr); err != nil {
		c.r.JSON(w, http.StatusInternalServerError, err)
		return
	}
	c.r.JSON(w, http.StatusOK, nil)
}

type CompactTableParam struct {
	PdAddr  string `json:"pd_addr"`
	TableId int64  `json:"table_id"`
}

func (c *ImportHandler) CompactTable(w http.ResponseWriter, r *http.Request) {
	param := &CompactTableParam{}
	defer r.Body.Close()
	if err := json.NewDecoder(r.Body).Decode(param); err != nil {
		c.r.JSON(w, http.StatusBadRequest, err)
		return
	}

	importClient, err := c.svr.GetImportClient()
	if err != nil {
		c.r.JSON(w, http.StatusInternalServerError, err)
		return
	}
	tablePrefix := tablecodec.EncodeTablePrefix(param.TableId)
	tablePrefixNext := tablePrefix.PrefixNext()
	req := &import_sstpb.CompactRequest{
		OutputLevel: -1,
		Range: &import_sstpb.Range{
			Start: tablePrefix,
			End:   tablePrefixNext,
		},
	}
	if err = importClient.CompactCluster(r.Context(), param.PdAddr, req); err != nil {
		c.r.JSON(w, http.StatusInternalServerError, err)
	}
	c.r.JSON(w, http.StatusOK, nil)
}
