package server

import (
	_ "github.com/go-sql-driver/mysql"
	"github.com/lerencao/tidb-light/config"
	"github.com/lerencao/tidb-light/utils"
	"github.com/pingcap/tidb/store/tikv/oracle"
	"github.com/pingcap/tidb/store/tikv/oracle/oracles"
)

type KvImporter interface {
	GetImportClient() (*KvImportClient, error)
	GetImportWriter(engineid []byte) (*EngineWriter, error)
}

type Server struct {
	cfg *config.Config

	rpcClient *utils.RpcClient

	sessionManager *SessionManager
	oracle         oracle.Oracle
}

func NewServer(cfg *config.Config) *Server {
	rpcClient := utils.NewRPCClient()
	// Check tikv importer is connectable
	// if _, err := rpcClient.GetConn(cfg.ImporterAddr); err != nil {
	// 	rpcClient.Close()
	// 	return nil, errors.WithStack(err)
	// }

	server := &Server{
		cfg:            cfg,
		rpcClient:      rpcClient,
		sessionManager: NewSessionManager(cfg),
		oracle:         oracles.NewLocalOracle(),
	}

	return server
}

func (s *Server) Start() error {
	return s.sessionManager.Start(s)
}

func (s *Server) Close() error {
	s.sessionManager.Close()
	return s.rpcClient.Close()
}

func (s *Server) GetImportClient() (*KvImportClient, error) {
	conn, err := s.rpcClient.GetConn(s.cfg.ImporterAddr)
	if err != nil {
		return nil, err
	}
	return NewKvImportClient(conn), nil
}

func (s *Server) GetImportWriter(engineId []byte) (*EngineWriter, error) {
	conn, err := s.rpcClient.GetConn(s.cfg.ImporterAddr)
	if err != nil {
		return nil, err
	}

	return NewEngineWriter(conn, engineId), nil
}
