package server

import (
	"encoding/json"
	"github.com/gorilla/mux"
	"github.com/pkg/errors"
	"github.com/satori/go.uuid"
	"github.com/unrolled/render"
	"net/http"
)

type EngineHandler struct {
	r   *render.Render
	svr *Server
}

func (s *EngineHandler) Open(w http.ResponseWriter, r *http.Request) {
	engineid := mux.Vars(r)["engineid"]
	engineId, err := uuid.FromString(engineid)
	if err != nil {
		s.r.JSON(w, http.StatusBadRequest, err)
		return
	}
	importClient, err := s.svr.GetImportClient()
	if err != nil {
		s.r.JSON(w, http.StatusInternalServerError, err)
		return
	}
	err = importClient.OpenEngine(r.Context(), engineId.Bytes())
	if err != nil {
		s.r.JSON(w, http.StatusInternalServerError, err)
		return
	}

	s.r.JSON(w, http.StatusOK, nil)
}

func (s *EngineHandler) Close(w http.ResponseWriter, r *http.Request) {
	engineid := mux.Vars(r)["engineid"]
	engineId, err := uuid.FromString(engineid)
	if err != nil {
		s.r.JSON(w, http.StatusBadRequest, err)
		return
	}
	importClient, err := s.svr.GetImportClient()
	if err != nil {
		s.r.JSON(w, http.StatusInternalServerError, err)
		return
	}
	err = importClient.CloseEngine(r.Context(), engineId.Bytes())
	if err != nil {
		s.r.JSON(w, http.StatusInternalServerError, err)
		return
	}

	s.r.JSON(w, http.StatusOK, nil)
}

func (s *EngineHandler) Import(w http.ResponseWriter, r *http.Request) {
	engineid := mux.Vars(r)["engineid"]
	engineId, err := uuid.FromString(engineid)
	if err != nil {
		s.r.JSON(w, http.StatusBadRequest, err)
		return
	}

	param := make(map[string]string, 1)
	defer r.Body.Close()
	if err = json.NewDecoder(r.Body).Decode(&param); err != nil {
		s.r.JSON(w, http.StatusBadRequest, err)
	}

	pdAddr, ok := param["pd_addr"]
	if !ok {
		s.r.JSON(w, http.StatusBadRequest, errors.Errorf("pd_addr is missing"))
	}

	importClient, err := s.svr.GetImportClient()
	if err != nil {
		s.r.JSON(w, http.StatusInternalServerError, err)
		return
	}
	err = importClient.ImportEngine(r.Context(), engineId.Bytes(), pdAddr)
	if err != nil {
		s.r.JSON(w, http.StatusInternalServerError, err)
		return
	}

	s.r.JSON(w, http.StatusOK, nil)

}

func (s *EngineHandler) Cleanup(w http.ResponseWriter, r *http.Request) {
	engineid := mux.Vars(r)["engineid"]
	engineId, err := uuid.FromString(engineid)
	if err != nil {
		s.r.JSON(w, http.StatusBadRequest, err)
		return
	}
	importClient, err := s.svr.GetImportClient()
	if err != nil {
		s.r.JSON(w, http.StatusInternalServerError, err)
		return
	}
	err = importClient.CleanupEngine(r.Context(), engineId.Bytes())
	if err != nil {
		s.r.JSON(w, http.StatusInternalServerError, err)
		return
	}

	s.r.JSON(w, http.StatusOK, nil)
}
