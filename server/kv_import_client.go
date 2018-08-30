package server

import (
	"context"
	"github.com/juju/errors"
	"github.com/pingcap/kvproto/pkg/import_kvpb"
	"github.com/pingcap/kvproto/pkg/import_sstpb"
	"google.golang.org/grpc"
)

type KvImportClient struct {
	conn   *grpc.ClientConn
	client import_kvpb.ImportKVClient
}

func NewKvImportClient(conn *grpc.ClientConn) *KvImportClient {
	importer := &KvImportClient{
		conn:   conn,
		client: import_kvpb.NewImportKVClient(conn),
	}

	return importer
}

func (c *KvImportClient) OpenEngine(ctx context.Context, engineId []byte) error {
	req := &import_kvpb.OpenEngineRequest{Uuid: engineId}
	_, err := c.client.OpenEngine(ctx, req)

	return err
}

func (c *KvImportClient) CloseEngine(ctx context.Context, engineId []byte) error {
	req := &import_kvpb.CloseEngineRequest{Uuid: engineId}
	resp, err := c.client.CloseEngine(ctx, req)
	if err != nil {
		return err
	}
	if resp.GetError() != nil {
		return errors.Errorf(resp.GetError().String())
	}
	return nil
}

func (c *KvImportClient) SwitchMode(ctx context.Context, pdAddr string, mode import_sstpb.SwitchMode) error {
	req := &import_kvpb.SwitchModeRequest{
		PdAddr: pdAddr,
		Request: &import_sstpb.SwitchModeRequest{
			Mode: mode,
		},
	}

	_, err := c.client.SwitchMode(ctx, req)
	return err

}

func (c *KvImportClient) ImportEngine(ctx context.Context, engineId []byte, pdAddr string) error {
	req := &import_kvpb.ImportEngineRequest{
		Uuid:   engineId,
		PdAddr: pdAddr,
	}

	_, err := c.client.ImportEngine(ctx, req)
	return err
}

func (c *KvImportClient) CompactCluster(ctx context.Context, pdAddr string, request *import_sstpb.CompactRequest) error {
	req := &import_kvpb.CompactClusterRequest{
		PdAddr:  pdAddr,
		Request: request,
	}
	_, err := c.client.CompactCluster(ctx, req)
	return err
}

func (c *KvImportClient) CleanupEngine(ctx context.Context, engineId []byte) error {
	req := &import_kvpb.CleanupEngineRequest{Uuid: engineId}
	_, err := c.client.CleanupEngine(ctx, req)
	return err
}
