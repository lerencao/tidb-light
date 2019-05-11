package server

import (
	"encoding/json"
	"github.com/gorilla/mux"
	"github.com/satori/go.uuid"
	"github.com/sirupsen/logrus"
	"github.com/unrolled/render"
	"net/http"
)

type SessionHandler struct {
	r   *render.Render
	svr *Server
}
type OpenSessionParam struct {
	EngineId   string `json:"engine_id"`
	SchemaName string `json:"schema_name"`
	TableName  string `json:"table_name"`
}

func (s *SessionHandler) Open(w http.ResponseWriter, r *http.Request) {
	sessionid := mux.Vars(r)["sessionid"]
	param := &OpenSessionParam{}
	defer r.Body.Close()
	if err := json.NewDecoder(r.Body).Decode(param); err != nil {
		s.r.JSON(w, http.StatusBadRequest, err.Error())
		return
	}

	engineId, err := uuid.FromString(param.EngineId)
	if err != nil {
		s.r.JSON(w, http.StatusBadRequest, err.Error())
		return
	}

	session, err := s.svr.sessionManager.OpenSession(sessionid, engineId.Bytes(), param.SchemaName, param.TableName)

	if err != nil {
		logrus.Error(err)
		s.r.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}

	s.r.JSON(w, http.StatusOK, map[string]interface{}{
		"schema_name": session.schemaName,
		"table_name":  session.tableName,
		"table_id":    session.tableid,
		"ddl":         session.ddl,
	})
}

func (s *SessionHandler) Get(w http.ResponseWriter, r *http.Request) {
	// sessionid := mux.Vars(r)["sessionid"]
	// session := s.svr.sessionManager.GetSession(sessionid)
	// TODO
}

type SessionWriteParam struct {
	Sqls []string `json:"sqls"`
}

func (s *SessionHandler) Write(w http.ResponseWriter, r *http.Request) {
	sessionid := mux.Vars(r)["sessionid"]
	session := s.svr.sessionManager.GetSession(sessionid)
	if session == nil {
		s.r.JSON(w, http.StatusBadRequest, "session is not exist")
		return
	}

	param := &SessionWriteParam{}
	defer r.Body.Close()
	if err := json.NewDecoder(r.Body).Decode(param); err != nil {
		s.r.JSON(w, http.StatusBadRequest, err.Error())
		return
	}
	// we use local oracle, err is never returned
	ts, _ := s.svr.oracle.GetTimestamp(r.Context())
	rows, err := session.Write(r.Context(), param.Sqls, ts)
	if err != nil {
		s.r.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}

	s.r.JSON(w, http.StatusOK, map[string]interface{}{
		"rows": rows,
	})
}

func (s *SessionHandler) Close(w http.ResponseWriter, r *http.Request) {
	sessionid := mux.Vars(r)["sessionid"]
	session := s.svr.sessionManager.GetSession(sessionid)
	if session == nil {
		s.r.JSON(w, http.StatusBadRequest, "session is not exist")
		return
	}

	err := s.svr.sessionManager.CloseSession(sessionid)
	if err != nil {
		s.r.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}

	s.r.JSON(w, http.StatusOK, nil)
}
