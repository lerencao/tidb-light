package server

import (
	"context"
	"github.com/juju/errors"
	"github.com/kataras/iris"
	"github.com/lerencao/tidb-light/utils"
	"github.com/pingcap/kvproto/pkg/import_sstpb"
	"github.com/satori/go.uuid"
	"github.com/sirupsen/logrus"
)

type KvImporter interface {
	GetImportClient() (*KvImportClient, error)
	GetImportWriter(engineid []byte) (*EngineWriter, error)
}

type Server struct {
	app *iris.Application

	rpcClient *utils.RpcClient

	tikvImporterAddr string

	sessionManager *SessionManager
}

func NewServer(tikvImporterAddr string) (*Server, error) {
	rpcClient := utils.NewRPCClient()
	// Check tikv importer is connectable
	if _, err := rpcClient.GetConn(tikvImporterAddr); err != nil {
		rpcClient.Close()
		return nil, errors.Trace(err)
	}

	app := iris.Default()
	server := &Server{
		tikvImporterAddr: tikvImporterAddr,
		app:              app,
		rpcClient:        rpcClient,
		sessionManager: &SessionManager{
			sessions: make(map[string]*WriteSession, 10),
		},
	}

	server.sessionManager.kvimporter = server

	server.Init()
	return server, nil
}

func (s *Server) Start(addr string) error {
	return s.app.Run(iris.Addr(addr))
}

func (s *Server) Close() error {
	s.sessionManager.Close()
	return s.rpcClient.Close()
}

// for _, ddl := range initData.Ddls {
// err := encoder.ExecDDLSQL(ddl)
// if err != nil {
// writeError(httpctx, 500, err)
// return
// }
// }

func (s *Server) OpenEngine(httpctx iris.Context) {
	engineid := httpctx.Params().Get("engineid")
	engineId, err := uuid.FromString(engineid)
	if err != nil {
		writeError(httpctx, 400, err)
		return
	}

	importClient, err := s.GetImportClient()
	if err != nil {
		writeError(httpctx, 500, err)
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	httpctx.OnConnectionClose(func() {
		cancel()
	})

	err = importClient.OpenEngine(ctx, engineId.Bytes())
	if err != nil {
		writeError(httpctx, 500, err)
		return
	}

	httpctx.StatusCode(200)
}

func (s *Server) CloseEngine(httpctx iris.Context) {
	engineid := httpctx.Params().Get("engineid")
	engineId, err := uuid.FromString(engineid)
	if err != nil {
		writeError(httpctx, 400, err)
		return
	}

	importClient, err := s.GetImportClient()
	if err != nil {
		writeError(httpctx, 500, err)
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	httpctx.OnConnectionClose(func() {
		cancel()
	})

	err = importClient.CloseEngine(ctx, engineId.Bytes())
	if err != nil {
		writeError(httpctx, 500, err)
		return
	}

	httpctx.StatusCode(200)
}
func (s *Server) CleanupEngine(httpctx iris.Context) {
	engineid := httpctx.Params().Get("engineid")
	engineId, err := uuid.FromString(engineid)
	if err != nil {
		writeError(httpctx, 400, err)
		return
	}

	importClient, err := s.GetImportClient()
	if err != nil {
		writeError(httpctx, 500, err)
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	httpctx.OnConnectionClose(func() {
		cancel()
	})

	err = importClient.CleanupEngine(ctx, engineId.Bytes())
	if err != nil {
		writeError(httpctx, 500, err)
		return
	}

	httpctx.StatusCode(200)
}

type OpenSessionParam struct {
	EngineId   string `json:"engine_id"`
	SchemaName string `json:"schema_name"`
	TableName  string `json:"table_name"`
	TableId    int64  `json:"table_id"`
	DDL        string `json:"ddl"`
}

func (s *Server) OpenSession(httpctx iris.Context) {
	sessionid := httpctx.Params().Get("sessionid")

	param := &OpenSessionParam{}
	err := httpctx.ReadJSON(param)
	if err != nil {
		writeError(httpctx, 400, err)
		return
	}

	engineId, err := uuid.FromString(param.EngineId)
	if err != nil {
		writeError(httpctx, 400, err)
		return
	}

	_, err = s.sessionManager.OpenSession(
		sessionid, engineId.Bytes(),
		param.SchemaName, param.TableName, param.TableId, param.DDL)
	if err != nil {
		writeError(httpctx, 500, err)
		return
	}

	httpctx.StatusCode(200)
}

func (s *Server) SessionInfo(httpctx iris.Context) {
	sessionid := httpctx.Params().Get("sessionid")
	session := s.sessionManager.GetSession(sessionid)
	if session == nil {
		writeError(httpctx, 400, errors.Errorf("session %s not exists", sessionid))
		return
	}

	httpctx.JSON(map[string]interface{}{
		"schema_name": session.schemaName,
		"table_name":  session.tableName,
		"table_id":    session.tableid,
		"ddl":         session.ddl,
	})
}

type SessionWriteParam struct {
	Sqls []string `json:"sqls"`
}

func (s *Server) SessionWrite(httpctx iris.Context) {
	sessionid := httpctx.Params().Get("sessionid")
	session := s.sessionManager.GetSession(sessionid)
	if session == nil {
		writeError(httpctx, 400, errors.Errorf("session %s not exists", sessionid))
		return
	}

	writeData := &SessionWriteParam{}
	err := httpctx.ReadJSON(writeData)
	if err != nil {
		writeError(httpctx, 400, err)
		return
	}

	rows, err := session.Write(context.Background(), writeData.Sqls)

	if err != nil {
		writeError(httpctx, 400, err)
		return
	}

	httpctx.JSON(map[string]uint64{
		"rows": rows,
	})
}

func (s *Server) SessionClose(httpctx iris.Context) {
	sessionid := httpctx.Params().Get("sessionid")
	err := s.sessionManager.CloseSession(sessionid)
	if err != nil {
		writeError(httpctx, 500, err)
	}
}

func (s *Server) GetImportClient() (*KvImportClient, error) {
	conn, err := s.rpcClient.GetConn(s.tikvImporterAddr)
	if err != nil {
		return nil, err
	}
	return NewKvImportClient(conn), nil
}

func (s *Server) GetImportWriter(engineId []byte) (*EngineWriter, error) {
	conn, err := s.rpcClient.GetConn(s.tikvImporterAddr)
	if err != nil {
		return nil, err
	}

	return NewEngineWriter(conn, engineId), nil
}

type SwitchPdMode struct {
	PdAddr string                  `json:"pd_addr"`
	Mode   import_sstpb.SwitchMode `json:"mode"`
}

func (s *Server) SwitchPdMode(httpctx iris.Context) {
	param := &SwitchPdMode{}
	err := httpctx.ReadJSON(param)
	if err != nil {
		writeError(httpctx, 400, err)
		return
	}

	c, err := s.GetImportClient()
	if err != nil {
		writeError(httpctx, 500, err)
		return
	}
	err = c.SwitchMode(context.Background(), param.PdAddr, param.Mode)
	if err != nil {
		writeError(httpctx, 500, err)
		return
	}
}

type ImportEngineParam struct {
	PdAddr string `json:"pd_addr"`
}

func (s *Server) ImportEngine(httpctx iris.Context) {
	engineid := httpctx.Params().Get("engineid")
	engineId, err := uuid.FromString(engineid)
	if err != nil {
		writeError(httpctx, 400, err)
		return
	}

	param := &ImportEngineParam{}
	err = httpctx.ReadJSON(param)
	if err != nil {
		writeError(httpctx, 400, err)
		return
	}

	importClient, err := s.GetImportClient()
	if err != nil {
		writeError(httpctx, 500, err)
		return
	}
	err = importClient.ImportEngine(context.Background(), engineId.Bytes(), param.PdAddr)
	if err != nil {
		writeError(httpctx, 500, err)
		return
	}
}

type CompactTableParam struct {
	PdAddr  string `json:"pd_addr"`
	TableId int64  `json:"table_id"`
}

func (s *Server) CompactTable(httpctx iris.Context) {
	param := &CompactTableParam{}
	err := httpctx.ReadJSON(param)
	if err != nil {
		writeError(httpctx, 400, err)
		return
	}

	importClient, err := s.GetImportClient()
	if err != nil {
		writeError(httpctx, 500, err)
		return
	}

	// TODO: gen start/end from tableid
	req := &import_sstpb.CompactRequest{
		OutputLevel: -1,
		Range: &import_sstpb.Range{
			Start: make([]byte, 0),
			End:   make([]byte, 0),
		},
	}
	err = importClient.CompactCluster(context.Background(), param.PdAddr, req)
	if err != nil {
		writeError(httpctx, 500, err)
	}
}

func (s *Server) Init() {

	// engine api
	s.app.Post("/sql2kv/engines/{engineid}/open", s.OpenEngine)
	s.app.Post("/sql2kv/engines/{engineid}/close", s.CloseEngine)
	s.app.Post("/sql2kv/engines/{engineid}/cleanup", s.CleanupEngine)

	s.app.Post("/sql2kv/engines/{engineid}/import", s.ImportEngine)

	s.app.Post("/sql2kv/import/switch_mode", s.SwitchPdMode)
	s.app.Post("/sql2kv/import/compact_table", s.CompactTable)
	// write session
	s.app.Post("/sql2kv/sessions/{sessionid}/open", s.OpenSession)
	s.app.Get("/sql2kv/sessions/{sessionid}", s.SessionInfo)
	s.app.Post("/sql2kv/sessions/{sessionid}/write", s.SessionWrite)
	s.app.Post("/sql2kv/sessions/{sessionid}/close", s.SessionClose)
}

func writeError(ctx iris.Context, code int, err error) {
	ctx.StatusCode(code)
	_, e := ctx.WriteString(err.Error())
	if e != nil {
		logrus.Panicf("write error: %v", e)
	}
}
