package server

import (
	"github.com/gorilla/mux"
	"github.com/unrolled/render"
	"github.com/urfave/negroni"
	"net/http"
)

func CreateRouter(prefix string, s *Server) *mux.Router {
	router := mux.NewRouter()
	r := router.PathPrefix(prefix).Subrouter()

	render := render.New(render.Options{
		IndentJSON: true,
	})

	engineRouter := r.PathPrefix("/engines").Subrouter()
	engineHandler := &EngineHandler{
		r:   render,
		svr: s,
	}
	engineRouter.Path("/{engineid}/open").Methods("POST").HandlerFunc(engineHandler.Open)
	engineRouter.Path("/{engineid}/close").Methods("POST").HandlerFunc(engineHandler.Close)
	engineRouter.Path("/{engineid}/cleanup").Methods("POST").HandlerFunc(engineHandler.Cleanup)

	sessionRouter := r.PathPrefix("/sessions").Subrouter()
	sessionHandler := &SessionHandler{
		r:   render,
		svr: s,
	}
	sessionRouter.Methods(http.MethodPost).Path("/{sessionid}/open").HandlerFunc(sessionHandler.Open)
	sessionRouter.Methods(http.MethodGet).Path("/{sessionid}").HandlerFunc(sessionHandler.Get)
	sessionRouter.Methods(http.MethodPost).Path("/{sessionid}/write").HandlerFunc(sessionHandler.Write)
	sessionRouter.Methods(http.MethodPost).Path("/{sessionid}/close").HandlerFunc(sessionHandler.Close)

	importRouter := r.PathPrefix("/import").Subrouter()
	importHandler := &ImportHandler{
		r:   render,
		svr: s,
	}
	importRouter.Methods(http.MethodPost).Path("/switch_mode").HandlerFunc(importHandler.SwitchMode)
	importRouter.Methods(http.MethodPost).Path("/compact_table").HandlerFunc(importHandler.CompactTable)
	importRouter.Methods(http.MethodPost).Path("/engines/{engineid}").HandlerFunc(importHandler.ImportEngine)

	return router
}

func NewHandler(s *Server) http.Handler {
	engine := negroni.New()

	recovery := negroni.NewRecovery()

	engine.Use(recovery)
	engine.Use(negroni.NewLogger())
	r := CreateRouter("/sql2kv", s)
	engine.UseHandler(r)
	return engine
}
