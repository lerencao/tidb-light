package server

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/lerencao/tidb-light/config"
	"github.com/pingcap/kvproto/pkg/import_kvpb"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/util/kvencoder"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"sync"
)

type SessionManager struct {
	sync.RWMutex
	cfg *config.Config

	db         *sql.DB
	kvimporter KvImporter
	sessions   map[string]*WriteSession
}

func NewSessionManager(cfg *config.Config) *SessionManager {
	return &SessionManager{
		cfg:      cfg,
		sessions: make(map[string]*WriteSession, 10),
	}
}
func (s *SessionManager) Start(importer KvImporter) error {
	db, err := OpenDB(s.cfg.TiDBAddr, s.cfg.TiDBUser, s.cfg.TiDBPass)
	if err != nil {
		return errors.WithStack(err)
	}
	s.db = db
	s.kvimporter = importer
	return nil
}

func OpenDB(tidbAddr, tidbUser, tidbPass string) (*sql.DB, error) {
	dsn := fmt.Sprintf("%s:%s@tcp(%s)/", tidbUser, tidbPass, tidbAddr)
	return sql.Open("mysql", dsn)
}

func (s *SessionManager) Close() {
	defer func() {
		if err := s.db.Close(); err != nil {
			logrus.Errorf("fail to close db, error: %v", err)
		}
	}()
	// TODO: return errors
	var err error
	for _, session := range s.sessions {
		err = session.Close()
		if err != nil {
			logrus.Errorf("fail to close session, error: %v", err)
		}
	}
}

func (s *SessionManager) GetSession(sessionid string) *WriteSession {
	s.RWMutex.RLock()
	defer s.RWMutex.RUnlock()
	return s.sessions[sessionid]
}

func (s *SessionManager) CloseSession(sessionid string) error {
	s.Lock()
	defer s.Unlock()

	if session, ok := s.sessions[sessionid]; ok {
		delete(s.sessions, sessionid)
		return session.Close()
	}

	return nil
}

func (s *SessionManager) OpenSession(sessionid string, engineid []byte, schemaName, tableName string) (*WriteSession, error) {
	s.Lock()
	defer s.Unlock()
	if session, ok := s.sessions[sessionid]; ok {
		return session, nil
	}

	tableid, err := TableId(s.cfg.TiDBHttpAddr, schemaName, tableName)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	ddl, err := TableDDL(s.db, schemaName, tableName)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	encoder, err := kvenc.New(schemaName, kvenc.NewAllocator())

	if err != nil {
		return nil, err
	}

	err = encoder.ExecDDLSQL(ddl)
	if err != nil {
		return nil, err
	}

	writer, err := s.kvimporter.GetImportWriter(engineid)

	if err != nil {
		encoder.Close()
		return nil, err
	}

	// open write to remote service
	writer.Open()

	session := &WriteSession{
		schemaName: schemaName,
		tableName:  tableName,
		tableid:    tableid,
		ddl:        ddl,
		encoder:    encoder,
		writer:     writer,
	}
	s.sessions[sessionid] = session
	return session, nil
}

type WriteSession struct {
	schemaName string
	tableName  string
	tableid    int64
	ddl        string
	encoder    kvenc.KvEncoder
	writer     *EngineWriter
}

func (s *WriteSession) Write(ctx context.Context, sqls []string, commitTs uint64) (uint64, error) {
	kvs := make([]*import_kvpb.Mutation, 0, 100)

	var rows uint64
	for _, sql := range sqls {
		kvPairs, affectedRows, err := s.encoder.Encode(sql, s.tableid)
		if err != nil {
			return 0, errors.WithStack(err)
		}
		rows = rows + affectedRows
		// if affectedRows != uint64(len(sqls)) {
		// 	logrus.Warnf("affectedRows %d is not equal to incoming sql size %d", affectedRows, len(sqls))
		// }

		for _, pair := range kvPairs {
			tableID, handle, err := tablecodec.DecodeRecordKey(pair.Key)
			if err != nil {
				return 0, err
			}
			if tableID != s.tableid {
				return 0, errors.Errorf("invalid encoded key, table id(%d) should be %d", tableID, s.tableid)
			}
			logrus.Infof("encode handle %d", handle)
			kvs = append(kvs, &import_kvpb.Mutation{
				Op:    import_kvpb.Mutation_Put,
				Key:   pair.Key,
				Value: pair.Val,
			})
		}
	}

	wb := &import_kvpb.WriteBatch{
		CommitTs:  commitTs,
		Mutations: kvs,
	}
	return rows, s.writer.WriteEngine(ctx, wb)
}

func (s *WriteSession) Close() error {
	err := s.encoder.Close()
	if s.writer != nil {
		s.writer.Close()
	}
	return err
}
