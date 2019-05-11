package server

import (
	"context"
	"github.com/pingcap/kvproto/pkg/import_kvpb"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
)

type result struct {
	v *import_kvpb.WriteEngineResponse
	e error
}

type WriteTunnel struct {
	conn *grpc.ClientConn
	client     import_kvpb.ImportKV_WriteEngineClient
	sourceChan chan *import_kvpb.WriteEngineRequest
	doneChan   chan *result
}

func (w *WriteTunnel) NewWriteTunnel(dataChan chan *import_kvpb.WriteEngineRequest, conn *grpc.ClientConn) *WriteTunnel {
	return &WriteTunnel{
		sourceChan:dataChan,
		conn:conn,
		doneChan: make(chan *result, 1),
	}
}

func (w *WriteTunnel) Open(ctx context.Context) error {
	if w.client != nil {
		return errors.Errorf("write tunnel already opened")
	}

	client,err := import_kvpb.NewImportKVClient(w.conn).WriteEngine(ctx)

	if err != nil {
		return err
	}
	w.client = client
	go w.streamingLoop()
	return nil
}

// ResultChan return the channel which caller should loop on this to wait for streaming finish.
func (w *WriteTunnel) ResultChan() chan *result {
	return w.doneChan
}

func (w *WriteTunnel) streamingLoop() {
	defer close(w.doneChan)
	for {
		select {
		case req, ok := <-w.sourceChan:
			if ok {
				err := w.client.Send(req)
				if err != nil {
					if e := w.client.CloseSend(); e != nil {
						logrus.Errorf("fail to close stream, %v", e)
					}
					w.doneChan <- &result{e: err}
				}
			} else {
				// Close chan
				resp, err := w.client.CloseAndRecv()

				if err != nil {
					w.doneChan <- &result{e: err}
				} else if resp.GetError() != nil {
					w.doneChan <- &result{e: errors.Errorf(resp.GetError().String()) }
				} else {
					w.doneChan <- &result{v: resp}
				}

				return
			}
		}
	}
}
