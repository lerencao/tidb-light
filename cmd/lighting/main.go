package main

import (
	"context"
	"flag"
	"github.com/Sirupsen/logrus"
	"github.com/juju/errors"
	"github.com/lerencao/tidb-light/utils"
	"github.com/pingcap/kvproto/pkg/import_kvpb"
	"github.com/satori/go.uuid"
	"os"
	"time"
)

func main() {
	cfg := newConfig()
	err := cfg.Parse(os.Args[1:])
	switch errors.Cause(err) {
	case nil:
	case flag.ErrHelp:
		os.Exit(0)
	default:
		logrus.Errorf("parse cmd flags err %s\n", err)
		os.Exit(2)
	}
	rpcClient := utils.NewRPCClient()
	clientConn, err := rpcClient.GetConn(cfg.ImporterAddr)
	if err != nil {
		logrus.Errorf("fail to get conn to %v, err: %s\n", cfg.ImporterAddr, err)
		os.Exit(1)
	}

	uuids, err := uuid.FromString(cfg.SessionId)
	if err != nil {
		logrus.Errorf("invalid session id %v, err: %s\n", cfg.SessionId, err)
		os.Exit(1)
	}

	importerClient, err := NewImportClient(clientConn, uuids.Bytes())
	if err != nil {
		logrus.Errorf("fail to get importer client, err: %s\n", err)
		os.Exit(1)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err = importerClient.OpenEngine(ctx)
	if err != nil {
		logrus.Errorf("fail to open engine, err: %s\n", err)
		os.Exit(1)
	}

	err = importerClient.WriteEngine(ctx, &import_kvpb.WriteBatch{
		CommitTs: uint64(time.Now().Unix()),
		Mutations: []*import_kvpb.Mutation{
			{
				Key:   []byte(cfg.Key),
				Value: []byte(cfg.Value),
			},
		},
	})
	if err != nil {
		logrus.Errorf("fail to write data, err: %s\n", err)
		os.Exit(1)
	}

	err = importerClient.Close(ctx)
	if err != nil {
		logrus.Errorf("fail to close engine, err: %s\n", err)
		os.Exit(1)
	}
}
