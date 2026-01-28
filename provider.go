package nntppool

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"net/url"
	"sync"
	"sync/atomic"
	"time"

	"github.com/javi11/nntppool/v3/internal"
	"golang.org/x/net/proxy"
)

const sendRequestTimeout = 5 * time.Second

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
	MaxConnIdleTime       time.Duration
	MaxConnLifetime       time.Duration
	Auth                  Auth
	TLSConfig             *tls.Config
	ConnFactory           ConnFactory
	// ProxyURL configures SOCKS proxy connection (e.g., "socks5://host:port" or "socks5://user:pass@host:port").
	// Supports socks4, socks4a, and socks5 protocols.
	// Only used when ConnFactory is nil.
	ProxyURL string
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

	closed  atomic.Bool
	closeMu sync.Mutex

	// Metrics
	bytesRead    uint64
	bytesWritten uint64
	currentSpeed uint64 // bytes per second
}

func NewProvider(ctx context.Context, config ProviderConfig) (*Provider, error) {
	if config.ConnFactory == nil {
		if config.ProxyURL != "" {
			// Parse and validate proxy URL
			proxyURL, err := url.Parse(config.ProxyURL)
			if err != nil {
				return nil, fmt.Errorf("invalid proxy URL: %w", err)
			}

			// Create proxy dialer based on scheme
			var dialer proxy.Dialer
			switch proxyURL.Scheme {
			case "socks5":
				var auth *proxy.Auth
				if proxyURL.User != nil {
					password, _ := proxyURL.User.Password()
					auth = &proxy.Auth{
						User:     proxyURL.User.Username(),
						Password: password,
					}
				}
				dialer, err = proxy.SOCKS5("tcp", proxyURL.Host, auth, proxy.Direct)
				if err != nil {
					return nil, fmt.Errorf("failed to create SOCKS5 proxy dialer: %w", err)
				}
			case "socks4", "socks4a":
				// Note: socks4/socks4a don't support authentication in golang.org/x/net/proxy
				// but we can still create the dialer
				var auth *proxy.Auth
				if proxyURL.User != nil {
					password, _ := proxyURL.User.Password()
					auth = &proxy.Auth{
						User:     proxyURL.User.Username(),
						Password: password,
					}
				}
				dialer, err = proxy.SOCKS5("tcp", proxyURL.Host, auth, proxy.Direct)
				if err != nil {
					return nil, fmt.Errorf("failed to create SOCKS proxy dialer: %w", err)
				}
			default:
				return nil, fmt.Errorf("unsupported proxy scheme: %s (supported: socks4, socks4a, socks5)", proxyURL.Scheme)
			}

			// Create ConnFactory that uses the proxy dialer
			config.ConnFactory = func(ctx context.Context) (net.Conn, error) {
				// Use context-aware dialing if available
				if contextDialer, ok := dialer.(proxy.ContextDialer); ok {
					conn, err := contextDialer.DialContext(ctx, "tcp", config.Address)
					if err != nil {
						return nil, fmt.Errorf("proxy dial failed: %w", err)
					}
					return internal.ApplyConnOptimizations(conn, config.TLSConfig)
				}

				// Fallback to non-context dialing
				conn, err := dialer.Dial("tcp", config.Address)
				if err != nil {
					return nil, fmt.Errorf("proxy dial failed: %w", err)
				}
				return internal.ApplyConnOptimizations(conn, config.TLSConfig)
			}
		} else {
			// No proxy, use direct connection
			config.ConnFactory = func(_ context.Context) (net.Conn, error) {
				return internal.NewNetConn(config.Address, config.TLSConfig)
			}
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
			if closeErr := c.Close(); closeErr != nil {
				// Log but don't return closeErr, return original err
				_ = closeErr
			}
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
	if c.closed.Load() {
		return c.ctx.Err()
	}

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
	meteredConn := &internal.MeteredConn{
		Conn:         conn,
		BytesRead:    &c.bytesRead,
		BytesWritten: &c.bytesWritten,
	}

	w, err := newNNTPConnectionFromConn(c.ctx, meteredConn, c.config.InflightPerConnection, c.reqCh, c.config.Auth, c.config.MaxConnIdleTime, c.config.MaxConnLifetime)
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
		// Remove connection from slice after it exits
		c.removeConnection(w)
	}()

	return nil
}

// removeConnection removes a connection from the conns slice.
// Called when a connection exits (lifetime, idle, error).
func (c *Provider) removeConnection(conn *NNTPConnection) {
	c.connsMu.Lock()
	defer c.connsMu.Unlock()

	for i, cc := range c.conns {
		if cc == conn {
			// Remove by swapping with last element and truncating
			c.conns[i] = c.conns[len(c.conns)-1]
			c.conns[len(c.conns)-1] = nil // Clear reference for GC
			c.conns = c.conns[:len(c.conns)-1]
			return
		}
	}
}

// Close cancels the provider, closes its request channel, and waits for all connections to stop.
func (c *Provider) Close() error {
	if !c.closed.CompareAndSwap(false, true) {
		return nil
	}

	c.cancel()

	c.closeMu.Lock()
	close(c.reqCh)
	// Drain any queued requests to prevent caller hangs
	for req := range c.reqCh {
		if req != nil && req.RespCh != nil {
			close(req.RespCh)
		}
	}
	c.closeMu.Unlock()

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
			return fmt.Errorf("DATE: response channel closed")
		}
		if resp.Err != nil {
			return resp.Err
		}
		if resp.StatusCode != 111 {
			return fmt.Errorf("DATE: unexpected status code %d: %s", resp.StatusCode, resp.Status)
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

	return c.SendRequest(req)
}

func (c *Provider) SendRequest(req *Request) <-chan Response {
	closeAndReturn := func() <-chan Response {
		if req.RespCh != nil {
			close(req.RespCh)
		}
		return req.RespCh
	}

	// Fast path: provider already closed
	if c.closed.Load() {
		return closeAndReturn()
	}

	// Trigger lazy connection growth
	if atomic.LoadInt32(&c.connCount) < int32(c.config.MaxConnections) {
		go func() {
			err := c.addConnection(false)
			if err != nil {
				req.RespCh <- Response{Err: err}
			}
		}()
	}

	// Synchronize with Close() to prevent send-on-closed-channel
	c.closeMu.Lock()
	defer c.closeMu.Unlock()

	if c.closed.Load() {
		return closeAndReturn()
	}

	timeout := time.NewTimer(sendRequestTimeout)
	defer timeout.Stop()

	select {
	case <-c.ctx.Done():
		return closeAndReturn()
	case <-req.Ctx.Done():
		return closeAndReturn()
	case c.reqCh <- req:
		return req.RespCh
	case <-timeout.C:
		if req.RespCh != nil {
			req.RespCh <- Response{Err: fmt.Errorf("send request timeout after %v", sendRequestTimeout)}
			close(req.RespCh)
		}
		return req.RespCh
	}
}
