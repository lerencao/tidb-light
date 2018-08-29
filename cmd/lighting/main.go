package main

import (
	"flag"
	"github.com/juju/errors"
	"github.com/lerencao/tidb-light/server"
	"github.com/sirupsen/logrus"
	"os"
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

	server := server.NewServer(cfg.ImporterAddr)
	if err := server.Start(cfg.Addr); err != nil {
		logrus.Fatalf("fail to start server, error: %v", err)
		os.Exit(2)
	}
}

// func main() {
// 	cfg := newConfig()
// 	err := cfg.Parse(os.Args[1:])
// 	switch errors.Cause(err) {
// 	case nil:
// 	case flag.ErrHelp:
// 		os.Exit(0)
// 	default:
// 		logrus.Errorf("parse cmd flags err %s\n", err)
// 		os.Exit(2)
// 	}
// 	rpcClient := utils.NewRPCClient()
// 	clientConn, err := rpcClient.GetConn(cfg.ImporterAddr)
// 	if err != nil {
// 		logrus.Errorf("fail to get conn to %v, err: %s\n", cfg.ImporterAddr, err)
// 		os.Exit(1)
// 	}
//
// 	uuids, err := uuid.FromString(cfg.SessionId)
// 	if err != nil {
// 		logrus.Errorf("invalid session id %v, err: %s\n", cfg.SessionId, err)
// 		os.Exit(1)
// 	}
//
// 	importerClient := server.NewImportClient(clientConn, uuids.Bytes())
// 	if err != nil {
// 		logrus.Errorf("fail to get importer client, err: %s\n", err)
// 		os.Exit(1)
// 	}
//
// 	ctx, cancel := context.WithCancel(context.Background())
// 	defer cancel()
//
// 	err = importerClient.OpenEngine(ctx)
// 	if err != nil {
// 		logrus.Errorf("fail to open engine, err: %s\n", err)
// 		if strings.Contains(err.Error(), "FileExists") {
// 			logrus.Warnf("engine %s exists, cleanup now", importerClient.EngineId())
// 			err = importerClient.CleanupEngine(ctx)
// 		}
// 		if err != nil {
// 			logrus.Errorf("fail to open engine, err: %s\n", err)
// 			os.Exit(1)
// 		}
// 	}
//
// 	engineWriter := importerClient.NewEngineWriter()
// 	engineWriter.Open()
//
// 	mutations := make([]*import_kvpb.Mutation, 0, cfg.BatchSize)
// 	for i := uint64(0); i < cfg.KeyNum; i++ {
// 		id, _ := uuid.NewV4()
// 		mutations = append(mutations, &import_kvpb.Mutation{
// 			Op:    import_kvpb.Mutation_Put,
// 			Key:   id.Bytes(),
// 			Value: id.Bytes(),
// 		})
// 		if len(mutations) >= int(cfg.BatchSize) {
// 			start := time.Now()
// 			err = engineWriter.WriteEngine(ctx, &import_kvpb.WriteBatch{
// 				CommitTs:  uint64(time.Now().Unix()),
// 				Mutations: mutations,
// 			})
// 			if err != nil {
// 				logrus.Errorf("fail to write data, err: %s\n", err)
// 				os.Exit(1)
// 			}
// 			logrus.Infof("Write use %v", time.Now().Sub(start))
// 			mutations = mutations[:0]
// 		}
// 	}
// 	engineWriter.Close()
// }
