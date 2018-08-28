package main

import (
	"context"
	"github.com/Sirupsen/logrus"
	"github.com/juju/errors"
	"github.com/pingcap/kvproto/pkg/import_kvpb"
	"google.golang.org/grpc"
	"sync"
	"time"
)

var (
	// errClosing is returned when request is canceled when client is closing.
	errClosing = errors.New("[importer] closing")
)

type ImporterClient struct {
	conn   *grpc.ClientConn
	client import_kvpb.ImportKVClient
	uuid   []byte

	ctx    context.Context
	cancel context.CancelFunc

	wg *sync.WaitGroup

	requestChan chan *writeReq
}

func NewImportClient(conn *grpc.ClientConn, uuid []byte) (ImporterClient, error) {
	ctx, cancel := context.WithCancel(context.Background())
	importer := ImporterClient{
		conn:   conn,
		client: import_kvpb.NewImportKVClient(conn),
		uuid:   uuid,

		ctx:    ctx,
		cancel: cancel,

		wg: &sync.WaitGroup{},

		requestChan: make(chan *writeReq, 100000),
	}
	importer.wg.Add(1)
	go importer.reqHandleLoop()

	return importer, nil
}

// Close close importer client
func (c *ImporterClient) Close(ctx context.Context) error {
	c.cancel()
	logrus.Infof("Wait loop")
	c.wg.Wait()
	logrus.Infof("Fail remaining reqs")
	c.failWriteReqs(errClosing)
	logrus.Infof("Close engine")
	return c.CloseEngine(ctx)
}

type writeReq struct {
	mutation *import_kvpb.WriteBatch
	ctx      context.Context
	done     chan error
}

func (c *ImporterClient) reqHandleLoop() {
	defer c.wg.Done()
	// when client is closed, loop context should be done
	loopCtx, loopCancel := context.WithCancel(c.ctx)
	defer loopCancel()

	var writeStream import_kvpb.ImportKV_WriteEngineClient
	var streamCancel context.CancelFunc
	// defer closeWriteStream(writeStream)
	for {
		var err error

		if writeStream == nil {
			var streamCtx context.Context
			streamCtx, streamCancel = context.WithCancel(c.ctx)
			writeStream, err = c.client.WriteEngine(streamCtx)
			logrus.Infof("create write steam")
			if err == nil {
				err = initialSend(writeStream, c.uuid)
			}
			if err != nil {
				logrus.Errorf("[importer] create importer client error: %v", err)
				streamCancel()
				c.failWriteReqs(err)
				select {
				case <-time.After(time.Second):
				case <-loopCtx.Done():
					return
				}

				continue
			}
			logrus.Infof("write init head")
		}

		select {
		case req := <-c.requestChan:
			err := c.processWriteReq(writeStream, req)
			if err != nil {
				logrus.Errorf("[importer] send write req error: %v", err)
				streamCancel()
				// closeWriteStream(writeStream)
				writeStream, streamCancel = nil, nil
			}
		case <-loopCtx.Done():
			logrus.Infof("client closed")
			streamCancel()
			logrus.Infof("return from loop")
			return
		}

	}
}

// func closeWriteStream(stream import_kvpb.ImportKV_WriteEngineClient) error {
//	if stream == nil {
//		return nil
//	}
//	writeResp, err := stream.CloseAndRecv()
//	if err != nil {
//		return err
//	}
//	if writeResp.GetError() == nil {
//		return nil
//	}
//
//	return errors.Errorf("%v", writeResp.GetError().GetEngineNotFound())
// }

func (c *ImporterClient) OpenEngine(ctx context.Context) error {
	req := &import_kvpb.OpenEngineRequest{Uuid: c.uuid}
	_, err := c.client.OpenEngine(ctx, req)

	return err
}

func (c *ImporterClient) CloseEngine(ctx context.Context) error {
	req := &import_kvpb.CloseEngineRequest{Uuid: c.uuid}
	_, err := c.client.CloseEngine(ctx, req)
	return err
}

func (c *ImporterClient) CleanupEngine(ctx context.Context) error {
	req := &import_kvpb.CleanupEngineRequest{Uuid: c.uuid}
	_, err := c.client.CleanupEngine(ctx, req)
	return err
}

func (c *ImporterClient) WriteEngine(ctx context.Context, mutation *import_kvpb.WriteBatch) error {
	req := &writeReq{mutation: mutation, ctx: ctx, done: make(chan error, 1)}
	c.requestChan <- req

	select {
	case err := <-req.done:
		return err
	case <-req.ctx.Done():
		return errors.Trace(req.ctx.Err())
	}
}

// client send stream header
func initialSend(stream import_kvpb.ImportKV_WriteEngineClient, uuid []byte) error {
	return stream.Send(&import_kvpb.WriteEngineRequest{
		Chunk: &import_kvpb.WriteEngineRequest_Head{
			Head: &import_kvpb.WriteHead{
				Uuid: uuid,
			},
		},
	})
}

func (c *ImporterClient) processWriteReq(client import_kvpb.ImportKV_WriteEngineClient, writeReq *writeReq) error {
	logrus.Infof("process write req")
	req := &import_kvpb.WriteEngineRequest{
		Chunk: &import_kvpb.WriteEngineRequest_Batch{
			Batch: writeReq.mutation,
		},
	}
	err := client.Send(req)
	finishWriteReq(writeReq, err)
	return errors.Trace(err)
}

func (c *ImporterClient) failWriteReqs(err error) {
	n := len(c.requestChan)
	for i := 0; i < n; i++ {
		req := <-c.requestChan
		finishWriteReq(req, err)
	}
}

func finishWriteReq(req *writeReq, err error) {
	req.done <- errors.Trace(err)
}

// func sendCall(ctx context.Context, rpcClient *utils.RpcClient, addr string, req *import_kvpb.OpenEngineRequest, timeout time.Duration) error {
//	conn, err := rpcClient.GetConn(addr)
//	if err != nil {
//		return errors.Trace(err)
//	}
//
//	client := import_kvpb.NewImportKVClient(conn)
//	openEngineResponse, err := client.OpenEngine(ctx, &import_kvpb.OpenEngineRequest{})
//	if err != nil {
//		return errors.Trace(err)
//	}
//
// }
