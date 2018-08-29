package server

import (
	"context"
	"github.com/juju/errors"
	"github.com/pingcap/kvproto/pkg/import_kvpb"
	"github.com/satori/go.uuid"
	"github.com/sirupsen/logrus"
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
}

func NewImportClient(conn *grpc.ClientConn, uuid []byte) *ImporterClient {
	ctx, cancel := context.WithCancel(context.Background())
	importer := &ImporterClient{
		conn:   conn,
		client: import_kvpb.NewImportKVClient(conn),
		uuid:   uuid,

		ctx:    ctx,
		cancel: cancel,
	}

	return importer
}

func (c *ImporterClient) EngineId() string {
	id, _ := uuid.FromBytes(c.uuid)
	return id.String()
}

func (c *ImporterClient) NewEngineWriter() *EngineWriter {
	ctx, cancel := context.WithCancel(c.ctx)
	writer := &EngineWriter{
		engine: c,
		ctx:    ctx,
		cancel: cancel,

		wg:          &sync.WaitGroup{},
		requestChan: make(chan *writeReq, 100000),
	}

	return writer
}

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

type writeReq struct {
	mutation *import_kvpb.WriteBatch
	ctx      context.Context
	done     chan error
}

type EngineWriter struct {
	engine *ImporterClient
	ctx    context.Context
	cancel context.CancelFunc

	wg          *sync.WaitGroup
	requestChan chan *writeReq

	loopCancel context.CancelFunc
}

func (w *EngineWriter) Open() {
	w.wg.Add(1)
	loopCtx, loopCancel := context.WithCancel(w.ctx)
	w.loopCancel = loopCancel
	go w.reqHandleLoop(loopCtx)
}

func (w *EngineWriter) Close() {
	if w.loopCancel != nil {
		w.loopCancel()
		logrus.Infof("Wait loop")
		w.wg.Wait()
	}
	w.cancel()
	w.failWriteReqs(errClosing)
}

func (c *EngineWriter) WriteEngine(ctx context.Context, mutation *import_kvpb.WriteBatch) error {
	req := &writeReq{mutation: mutation, ctx: ctx, done: make(chan error, 1)}
	c.requestChan <- req

	select {
	case err := <-req.done:
		return err
	case <-req.ctx.Done():
		return errors.Trace(req.ctx.Err())
	}
}

func (c *EngineWriter) reqHandleLoop(loopCtx context.Context) {
	defer c.wg.Done()

	var writeStream import_kvpb.ImportKV_WriteEngineClient
	var streamCancel context.CancelFunc

	for {
		var err error

		if writeStream == nil {
			var streamCtx context.Context
			streamCtx, streamCancel = context.WithCancel(c.ctx)
			writeStream, err = c.engine.client.WriteEngine(streamCtx)
			logrus.Infof("create write steam")
			if err == nil {
				err = initialSend(writeStream, c.engine.uuid)
			}
			if err != nil {
				logrus.Errorf("[importer_writer] create write stream error: %v", err)
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
			logrus.Infof("closing write stream")
			err := closeWriteStream(writeStream)
			if err != nil {
				logrus.Errorf("fail to close write stream, error: %v", err)
			}
			streamCancel()
			logrus.Infof("return from loop")
			return
		}
	}
}

func (c *EngineWriter) processWriteReq(client import_kvpb.ImportKV_WriteEngineClient, writeReq *writeReq) error {
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

func (c *EngineWriter) failWriteReqs(err error) {
	n := len(c.requestChan)
	if n > 0 {
		logrus.Errorf("fail %d write reqs with error: %v", n, err)
	}
	for i := 0; i < n; i++ {
		req := <-c.requestChan
		finishWriteReq(req, err)
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

func finishWriteReq(req *writeReq, err error) {
	req.done <- errors.Trace(err)
}

func closeWriteStream(stream import_kvpb.ImportKV_WriteEngineClient) error {
	if stream == nil {
		return nil
	}
	writeResp, err := stream.CloseAndRecv()
	if err != nil {
		return err
	}
	if writeResp.GetError() == nil {
		return nil
	}

	return errors.Errorf("%v", writeResp.GetError().GetEngineNotFound())
}
