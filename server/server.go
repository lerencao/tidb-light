package server

import (
	"context"
	"github.com/juju/errors"
	"github.com/kataras/iris"
	"github.com/lerencao/tidb-light/utils"
	"github.com/pingcap/tidb/util/kvencoder"
	"github.com/satori/go.uuid"
	"github.com/sirupsen/logrus"
)

type Server struct {
	app *iris.Application

	rpcClient *utils.RpcClient

	tikvImporterAddr string

	sessions map[string]*ImportSession
}

func NewServer() {
	iris.Default()
}

type ImportSession struct {
	importer *ImporterClient
	encoder  kvenc.KvEncoder
}

type Sql2KvInitSessionData struct {
	Uuid string   `json:"uuid"`
	Ddls []string `json:"ddls"`
}

func (s *Server) Init() {
	s.app.Post("/sql2kv/init", func(httpctx iris.Context) {
		initData := Sql2KvInitSessionData{}
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
	})
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
