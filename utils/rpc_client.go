package utils

import (
	"github.com/grpc-ecosystem/go-grpc-prometheus"
	"github.com/juju/errors"
	// "github.com/pingcap/tidb/store/tikv/tikvrpc"
	// "github.com/pingcap/tidb/terror"
	"github.com/sirupsen/logrus"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
	"sync"
	"sync/atomic"
	"time"
)

const (
	dialTimeout       = 5 * time.Second
	readTimeoutShort  = 20 * time.Second  // For requests that read/write several key-values.
	ReadTimeoutMedium = 60 * time.Second  // For requests that may need scan region.
	ReadTimeoutLong   = 150 * time.Second // For requests that may need scan region multiple times.

	grpcInitialWindowSize     = 1 << 30
	grpcInitialConnWindowSize = 1 << 30
)

// MaxConnectionCount is the max gRPC connections that will be established with
// each tikv-server.
var MaxConnectionCount uint = 16

// GrpcKeepAliveTime is the duration of time after which if the client doesn't see
// any activity it pings the server to see if the transport is still alive.
var GrpcKeepAliveTime = time.Duration(10) * time.Second

// GrpcKeepAliveTimeout is the duration of time for which the client waits after having
// pinged for keepalive check and if no activity is seen even after that the connection
// is closed.
var GrpcKeepAliveTimeout = time.Duration(3) * time.Second

// MaxSendMsgSize set max gRPC request message size sent to server. If any request message size is larger than
// current value, an error will be reported from gRPC.
var MaxSendMsgSize = 1<<31 - 1

// MaxCallMsgSize set max gRPC receive message size received from server. If any message size is larger than
// current value, an error will be reported from gRPC.
var MaxCallMsgSize = 1<<31 - 1

type RpcClient struct {
	sync.RWMutex
	isClosed bool
	conns    map[string]*connArray
}

func NewRPCClient() *RpcClient {
	return &RpcClient{
		conns: make(map[string]*connArray),
	}
}

// GetConn get a gpc conn to addr
func (c *RpcClient) GetConn(addr string) (*grpc.ClientConn, error) {
	connArray, err := c.getConnArray(addr)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return connArray.Get(), nil
}

func (c *RpcClient) Close() error {
	c.closeConns()
	return nil
}

func (c *RpcClient) getConnArray(addr string) (*connArray, error) {
	c.RLock()
	if c.isClosed {
		c.RUnlock()
		return nil, errors.Errorf("rpcClient is closed")
	}
	array, ok := c.conns[addr]
	c.RUnlock()
	if !ok {
		var err error
		array, err = c.createConnArray(addr)
		if err != nil {
			return nil, err
		}
	}
	return array, nil
}

func (c *RpcClient) createConnArray(addr string) (*connArray, error) {
	c.Lock()
	defer c.Unlock()
	array, ok := c.conns[addr]
	if !ok {
		var err error
		array, err = newConnArray(MaxConnectionCount, addr)
		if err != nil {
			return nil, err
		}
		c.conns[addr] = array
	}
	return array, nil
}

func (c *RpcClient) closeConns() {
	c.Lock()
	if !c.isClosed {
		c.isClosed = true
		// close all connections
		for _, array := range c.conns {
			array.Close()
		}
	}
	c.Unlock()
}

type connArray struct {
	index uint32
	v     []*grpc.ClientConn
	// Bind with a background goroutine to process coprocessor streaming timeout.
	// streamTimeout chan *tikvrpc.Lease
}

func newConnArray(maxSize uint, addr string) (*connArray, error) {
	a := &connArray{
		index: 0,
		v:     make([]*grpc.ClientConn, maxSize),
		// streamTimeout: make(chan *tikvrpc.Lease, 1024),
	}
	if err := a.Init(addr); err != nil {
		return nil, err
	}
	return a, nil
}

func (a *connArray) Init(addr string) error {
	opt := grpc.WithInsecure()

	unaryInterceptor := grpc_prometheus.UnaryClientInterceptor
	streamInterceptor := grpc_prometheus.StreamClientInterceptor
	for i := range a.v {
		ctx, cancel := context.WithTimeout(context.Background(), dialTimeout)
		conn, err := grpc.DialContext(
			ctx,
			addr,
			opt,
			grpc.WithInitialWindowSize(grpcInitialWindowSize),
			grpc.WithInitialConnWindowSize(grpcInitialConnWindowSize),
			grpc.WithUnaryInterceptor(unaryInterceptor),
			grpc.WithStreamInterceptor(streamInterceptor),
			grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(MaxCallMsgSize)),
			grpc.WithDefaultCallOptions(grpc.MaxCallSendMsgSize(MaxSendMsgSize)),
			grpc.WithBackoffMaxDelay(time.Second*3),
			grpc.WithKeepaliveParams(keepalive.ClientParameters{
				Time:                GrpcKeepAliveTime,
				Timeout:             GrpcKeepAliveTimeout,
				PermitWithoutStream: true,
			}),
		)
		cancel()
		if err != nil {
			// Cleanup if the initialization fails.
			a.Close()
			return errors.Trace(err)
		}
		a.v[i] = conn
	}

	// go tikvrpc.CheckStreamTimeoutLoop(a.streamTimeout)

	return nil
}

func (a *connArray) Get() *grpc.ClientConn {
	next := atomic.AddUint32(&a.index, 1) % uint32(len(a.v))
	return a.v[next]
}

func (a *connArray) Close() {
	for i, c := range a.v {
		if c != nil {
			err := c.Close()
			logrus.Error(errors.Trace(err))
			// terror.Log(errors.Trace(err))
			a.v[i] = nil
		}
	}
	// close(a.streamTimeout)
}
