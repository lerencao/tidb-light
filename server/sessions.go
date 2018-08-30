package server

import (
	"context"
	"github.com/juju/errors"
	"github.com/pingcap/kvproto/pkg/import_kvpb"
	"github.com/pingcap/tidb/util/kvencoder"
	"github.com/sirupsen/logrus"
	"sync"
	"time"
)

type SessionManager struct {
	sync.RWMutex
	kvimporter KvImporter
	sessions   map[string]*WriteSession
}

func (s *SessionManager) Close() {
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

func (s *SessionManager) OpenSession(sessionid string, engineid []byte, schemaName, tableName string, tableid int64, ddl string) (*WriteSession, error) {
	s.Lock()
	defer s.Unlock()
	if session, ok := s.sessions[sessionid]; ok {
		return session, nil
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

func (s *WriteSession) Write(ctx context.Context, sqls []string) (uint64, error) {
	kvs := make([]*import_kvpb.Mutation, 0, 100)

	var rows uint64
	for _, sql := range sqls {
		kvPairs, affectedRows, err := s.encoder.Encode(sql, s.tableid)
		if err != nil {
			return 0, errors.Trace(err)
		}
		rows = rows + affectedRows
		// if affectedRows != uint64(len(sqls)) {
		// 	logrus.Warnf("affectedRows %d is not equal to incoming sql size %d", affectedRows, len(sqls))
		// }

		for _, pair := range kvPairs {
			kvs = append(kvs, &import_kvpb.Mutation{
				Op:    import_kvpb.Mutation_Put,
				Key:   pair.Key,
				Value: pair.Val,
			})
		}
	}

	wb := &import_kvpb.WriteBatch{
		CommitTs:  uint64(time.Now().UnixNano()),
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
