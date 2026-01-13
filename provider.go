package nntppool

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

type ProviderMetrics struct {
	Host              string
	ActiveConnections int
	TotalBytesRead    uint64
	TotalBytesWritten uint64
	ThroughputMB      float64 // MB/s
}

type ProviderConfig struct {
	Address               string
	MaxConnections        int
	InitialConnections    int
	InflightPerConnection int
	Auth                  Auth
	TLSConfig             *tls.Config
	ConnFactory           ConnFactory
}

type Provider struct {
	Host string

	ctx    context.Context
	cancel context.CancelFunc

	reqCh chan *Request

	connsMu sync.RWMutex
	conns   []*NNTPConnection
	wg      sync.WaitGroup

	connCount int32
	config    ProviderConfig

	// Metrics
	bytesRead    uint64
	bytesWritten uint64
	currentSpeed uint64 // bytes per second
}

func NewProvider(ctx context.Context, config ProviderConfig) (*Provider, error) {
	if config.ConnFactory == nil {
		config.ConnFactory = func(_ context.Context) (net.Conn, error) {
			return newNetConn(config.Address, config.TLSConfig)
		}
	}

	if config.MaxConnections == 0 {
		return nil, fmt.Errorf("MaxConnections must be > 0")
	}

	if config.InflightPerConnection == 0 {
		config.InflightPerConnection = 1
	}

	// Extract host from address for deduplication
	host, _, err := net.SplitHostPort(config.Address)
	if err != nil {
		host = config.Address
	}

	if ctx == nil {
		ctx = context.Background()
	}
	ctx, cancel := context.WithCancel(ctx)

	c := &Provider{
		Host:   host,
		ctx:    ctx,
		cancel: cancel,
		reqCh:  make(chan *Request, config.MaxConnections),
		conns:  make([]*NNTPConnection, 0, config.MaxConnections),
		config: config,
	}

	// Start metrics monitor
	go c.monitorSpeed()

	for i := 0; i < config.InitialConnections; i++ {
		if err := c.addConnection(true); err != nil {
			c.Close()
			return nil, err
		}
	}

	return c, nil
}

func (c *Provider) monitorSpeed() {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	var lastBytesRead uint64

	for {
		select {
		case <-c.ctx.Done():
			return
		case <-ticker.C:
			current := atomic.LoadUint64(&c.bytesRead)
			diff := current - lastBytesRead
			lastBytesRead = current
			atomic.StoreUint64(&c.currentSpeed, diff)
		}
	}
}

func (c *Provider) Metrics() ProviderMetrics {
	speed := atomic.LoadUint64(&c.currentSpeed)
	return ProviderMetrics{
		Host:              c.Host,
		ActiveConnections: int(atomic.LoadInt32(&c.connCount)),
		TotalBytesRead:    atomic.LoadUint64(&c.bytesRead),
		TotalBytesWritten: atomic.LoadUint64(&c.bytesWritten),
		ThroughputMB:      float64(speed) / 1024 / 1024,
	}
}

func (c *Provider) addConnection(syncMode bool) error {
	select {
	case <-c.ctx.Done():
		return c.ctx.Err()
	default:
	}

	if !syncMode {
		current := atomic.LoadInt32(&c.connCount)
		if current >= int32(c.config.MaxConnections) {
			return nil
		}
		if atomic.AddInt32(&c.connCount, 1) > int32(c.config.MaxConnections) {
			atomic.AddInt32(&c.connCount, -1)
			return nil
		}
	} else {
		atomic.AddInt32(&c.connCount, 1)
	}

	conn, err := c.config.ConnFactory(c.ctx)
	if err != nil {
		atomic.AddInt32(&c.connCount, -1)
		return err
	}

	// Wrap with MeteredConn
	meteredConn := &MeteredConn{
		Conn:         conn,
		bytesRead:    &c.bytesRead,
		bytesWritten: &c.bytesWritten,
	}

	w, err := newNNTPConnectionFromConn(c.ctx, meteredConn, c.config.InflightPerConnection, c.reqCh, c.config.Auth)
	if err != nil {
		_ = conn.Close()
		atomic.AddInt32(&c.connCount, -1)
		return err
	}

	c.connsMu.Lock()
	c.conns = append(c.conns, w)
	c.connsMu.Unlock()

	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		defer atomic.AddInt32(&c.connCount, -1)
		w.Run()
	}()

	return nil
}

// Close cancels the provider, closes its request channel, and waits for all connections to stop.
func (c *Provider) Close() error {
	c.cancel()
	close(c.reqCh)

	c.connsMu.Lock()
	for _, cc := range c.conns {
		_ = cc.Close()
	}
	c.connsMu.Unlock()

	c.wg.Wait()
	return nil
}

// Date checks the server time.
func (c *Provider) Date(ctx context.Context) error {
	cmd := "DATE\r\n"
	ch := c.Send(ctx, []byte(cmd), nil)
	select {
	case <-ctx.Done():
		return ctx.Err()
	case resp, ok := <-ch:
		if !ok {
			return fmt.Errorf("response channel closed unexpectedly")
		}
		if resp.Err != nil {
			return resp.Err
		}
		if resp.StatusCode != 111 {
			return fmt.Errorf("unexpected status code: %d %s", resp.StatusCode, resp.Status)
		}
		return nil
	}
}

func (c *Provider) Send(ctx context.Context, payload []byte, bodyWriter io.Writer) <-chan Response {
	respCh := make(chan Response, 1)
	if ctx == nil {
		ctx = context.Background()
	}

	req := &Request{
		Ctx:        ctx,
		Payload:    payload,
		RespCh:     respCh,
		BodyWriter: bodyWriter,
	}

	// Trigger lazy growth if needed
	if atomic.LoadInt32(&c.connCount) < int32(c.config.MaxConnections) {
		go c.addConnection(false)
	}

	select {
	case <-c.ctx.Done():
		close(respCh)
		return respCh
	case <-ctx.Done():
		close(respCh)
		return respCh
	case c.reqCh <- req:
		return respCh
	}
}
