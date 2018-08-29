package server

import (
	"context"
	"github.com/juju/errors"
	"github.com/kataras/iris"
	"github.com/lerencao/tidb-light/utils"
	"github.com/pingcap/kvproto/pkg/import_kvpb"
	"github.com/pingcap/tidb/util/kvencoder"
	"github.com/satori/go.uuid"
	"github.com/sirupsen/logrus"
	"time"
)

type Server struct {
	app *iris.Application

	rpcClient *utils.RpcClient

	tikvImporterAddr string

	sessions map[string]*ImportSession
}

func NewServer(tikvImporterAddr string) *Server {
	app := iris.Default()
	server := &Server{
		tikvImporterAddr: tikvImporterAddr,
		app:              app,
		rpcClient:        utils.NewRPCClient(),
		sessions:         make(map[string]*ImportSession),
	}
	server.Init()
	return server
}

func (s *Server) Start(addr string) error {
	// Check tikv importer is connectable
	if _, err := s.rpcClient.GetConn(s.tikvImporterAddr); err != nil {
		return errors.Trace(err)
	}
	return s.app.Run(iris.Addr(addr))
}

type ImportSession struct {
	importer *ImporterClient
	encoder  kvenc.KvEncoder

	writer *EngineWriter
}

func (s *ImportSession) Write(ctx context.Context, sqls []string, tableid int64) error {
	kvs := make([]*import_kvpb.Mutation, 0, 100)
	for _, sql := range sqls {
		kvPairs, affectedRows, err := s.encoder.Encode(sql, tableid)
		if err != nil {
			return errors.Trace(err)
		}
		if affectedRows != uint64(len(sqls)) {
			logrus.Warnf("affectedRows %d is not equal to incoming sql size %d", affectedRows, len(sqls))
		}

		for _, pair := range kvPairs {
			kvs = append(kvs, &import_kvpb.Mutation{
				Op:    import_kvpb.Mutation_Put,
				Key:   pair.Key,
				Value: pair.Val,
			})
		}
	}

	if s.writer == nil {
		s.writer = s.importer.NewEngineWriter()
		s.writer.Open()
	}
	wb := &import_kvpb.WriteBatch{
		CommitTs:  uint64(time.Now().UnixNano()),
		Mutations: kvs,
	}
	return s.writer.WriteEngine(ctx, wb)
}

func (s *ImportSession) Close(ctx context.Context) error {
	err1 := s.encoder.Close()
	if s.writer != nil {
		s.writer.Close()
	}

	if err := s.importer.CloseEngine(ctx); err != nil {
		if err1 != nil {
			return errors.Wrap(err1, err)
		}

		return errors.Trace(err)
	}

	return nil
}

type CreateSessionData struct {
	Uuid string   `json:"uuid"`
	Ddls []string `json:"ddls"`
}

// CreateSession create a session for write sql
func (s *Server) CreateSession(httpctx iris.Context) {
	initData := CreateSessionData{}
	if err := httpctx.ReadJSON(&initData); err != nil {
		httpctx.StatusCode(400)
		httpctx.WriteString(err.Error())
		return
	}

	thisUuid, err := uuid.FromString(initData.Uuid)
	sessionId := thisUuid.Bytes()
	if err != nil {
		writeError(httpctx, 400, err)
		return
	}

	if _, ok := s.sessions[initData.Uuid]; ok {
		writeError(httpctx, 400, errors.Errorf("session %s already exists", initData.Uuid))
		return
	}

	grpcConn, err := s.rpcClient.GetConn(s.tikvImporterAddr)
	if err != nil {
		writeError(httpctx, 500, err)
		return
	}

	importerClient := NewImportClient(grpcConn, sessionId)
	ctx := context.Background()
	err = importerClient.OpenEngine(ctx)
	if err != nil {
		writeError(httpctx, 500, err)
	}

	encoder, err := kvenc.New("test", kvenc.NewAllocator())
	if err != nil {
		writeError(httpctx, 500, err)
		return
	}
	for _, ddl := range initData.Ddls {
		err := encoder.ExecDDLSQL(ddl)
		if err != nil {
			writeError(httpctx, 500, err)
			return
		}
	}

	session := &ImportSession{
		importer: importerClient,
		encoder:  encoder,
	}

	s.sessions[initData.Uuid] = session

	httpctx.StatusCode(200)
}

type SessionWriteParam struct {
	TableId int64    `json:"table_id"`
	Sqls    []string `json:"sqls"`
}

func (s *Server) SessionWrite(httpctx iris.Context) {
	sessionid := httpctx.Params().Get("sessionid")
	session, ok := s.sessions[sessionid]
	if !ok {
		writeError(httpctx, 400, errors.Errorf("session %s not exists", sessionid))
		return
	}

	writeData := &SessionWriteParam{}
	err := httpctx.ReadJSON(writeData)
	if err != nil {
		writeError(httpctx, 400, err)
		return
	}

	session.Write(context.Background(), writeData.Sqls, writeData.TableId)
}

func (s *Server) CloseSession(httpctx iris.Context) {

	sessionid := httpctx.Params().Get("sessionid")
	session, ok := s.sessions[sessionid]
	if !ok {
		writeError(httpctx, 400, errors.Errorf("session %s not exists", sessionid))
		return
	}

	session.Close(context.Background())
	delete(s.sessions, sessionid)
}

func (s *Server) Init() {
	s.app.Post("/sql2kv/session", s.CreateSession)
	s.app.Post("/sql2kv/session/{sessionid}/write", s.SessionWrite)
	s.app.Post("/sql2kv/session/{sessionid}/close", s.CloseSession)
}

func writeError(ctx iris.Context, code int, err error) {
	ctx.StatusCode(code)
	_, e := ctx.WriteString(err.Error())
	if e != nil {
		logrus.Panicf("write error: %v", e)
	}
}

type KvTransformer struct {
	importer  ImporterClient
	kvencoder kvenc.KvEncoder
	ddls      []string
}

func (t *KvTransformer) AddDDL(ddl string) error {
	if err := t.kvencoder.ExecDDLSQL(ddl); err != nil {
		return err
	}

	t.ddls = append(t.ddls, ddl)
	return nil
}

func (t *KvTransformer) Transform(sql string, tableid int64) error {
	t.kvencoder.Encode(sql, tableid)
	// TODO: impl it
	return nil
}
