package nntppool

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/url"
	"sync"
	"sync/atomic"
	"time"

	"github.com/javi11/nntppool/v3/internal"
	"golang.org/x/net/proxy"
)

const sendRequestTimeout = 5 * time.Second

// ErrProviderClosed is returned when a request is made to a closed provider.
var ErrProviderClosed = errors.New("provider closed")

// ErrProviderUnavailable is returned when all connections to a provider have failed.
// This error is retryable - the client should try other providers or retry later.
var ErrProviderUnavailable = errors.New("provider unavailable: all connections failed")

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
	// HealthCheckPeriod is how often the centralized health monitor scans connections
	// for idle/lifetime expiration. Default: 1s. Set to 0 to disable health checks
	// (connections will then manage their own timeouts via timers).
	HealthCheckPeriod time.Duration
	Auth              Auth
	TLSConfig         *tls.Config
	ConnFactory       ConnFactory
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

	closed atomic.Bool
	sendWg sync.WaitGroup // Tracks in-flight SendRequest calls for safe channel close

	// deadCh is closed when all connections die (connCount reaches 0)
	// and reopened when a new connection is established.
	// This allows SendRequest to immediately fail when provider is unavailable.
	deadCh   chan struct{}
	deadChMu sync.Mutex

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

	// Default health check period of 1 second if idle/lifetime timeouts are configured.
	// A value of -1 explicitly disables health checks.
	if config.HealthCheckPeriod == 0 && (config.MaxConnIdleTime > 0 || config.MaxConnLifetime > 0) {
		config.HealthCheckPeriod = 5 * time.Second
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
		deadCh: make(chan struct{}),
	}

	// Start metrics monitor
	go c.monitorSpeed()

	// Start centralized health check goroutine if enabled
	if config.HealthCheckPeriod > 0 {
		go c.healthCheckLoop()
	}

	for i := 0; i < config.InitialConnections; i++ {
		if err := c.addConnection(true); err != nil {
			if closeErr := c.Close(); closeErr != nil {
				// Log but don't return closeErr, return original err
				_ = closeErr
			}
			return nil, err
		}
	}

	// If no connections (InitialConnections was 0), ensure deadCh is closed.
	// This ensures signalAlive() will properly create a new open channel
	// when the first connection is established.
	if len(c.conns) == 0 {
		close(c.deadCh)
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

// healthCheckLoop periodically scans all connections for idle/lifetime expiration.
// This centralizes timeout detection, eliminating per-connection timer allocations.
func (c *Provider) healthCheckLoop() {
	ticker := time.NewTicker(c.config.HealthCheckPeriod)
	defer ticker.Stop()

	for {
		select {
		case <-c.ctx.Done():
			return
		case <-ticker.C:
			c.performHealthCheck()
		}
	}
}

// performHealthCheck scans all connections and requests graceful shutdown for those
// that have exceeded their idle or lifetime limits.
func (c *Provider) performHealthCheck() {
	now := time.Now()

	// Take a snapshot of connections under read lock
	c.connsMu.RLock()
	conns := make([]*NNTPConnection, len(c.conns))
	copy(conns, c.conns)
	c.connsMu.RUnlock()

	for _, conn := range conns {
		// Check lifetime expiration first (takes precedence)
		if c.config.MaxConnLifetime > 0 && now.Sub(conn.createdAt) > c.config.MaxConnLifetime {
			conn.requestClose()
			continue
		}

		// Check idle expiration
		if c.config.MaxConnIdleTime > 0 {
			lastActivity := time.Unix(atomic.LoadInt64(&conn.lastActivity), 0)
			if now.Sub(lastActivity) > c.config.MaxConnIdleTime {
				conn.requestClose()
			}
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

	w, err := newNNTPConnectionFromConn(c.ctx, meteredConn, c.config.InflightPerConnection, c.reqCh, c.config.Auth, c.config.MaxConnIdleTime, c.config.MaxConnLifetime, c)
	if err != nil {
		_ = conn.Close()
		atomic.AddInt32(&c.connCount, -1)
		return err
	}

	c.connsMu.Lock()
	wasEmpty := len(c.conns) == 0
	c.conns = append(c.conns, w)
	c.connsMu.Unlock()

	// If this is the first connection after being dead, re-enable the provider
	if wasEmpty {
		c.signalAlive()
	}

	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		defer atomic.AddInt32(&c.connCount, -1)
		w.Run()
		// Remove connection from slice after it exits
		c.removeConnection(w)
	}()

	// Wait for the connection to be ready to receive requests.
	// This prevents a race where addConnection returns before Run() starts
	// consuming from reqCh, causing concurrent requests to pile up and timeout.
	select {
	case <-w.Ready():
		// Connection is ready to receive requests
	case <-c.ctx.Done():
		return c.ctx.Err()
	}

	return nil
}

// removeConnection removes a connection from the conns slice.
// Called when a connection exits (lifetime, idle, error).
func (c *Provider) removeConnection(conn *NNTPConnection) {
	// Flush any accumulated metrics before removing
	if mc, ok := conn.conn.(*internal.MeteredConn); ok {
		mc.Flush()
	}

	c.connsMu.Lock()
	defer c.connsMu.Unlock()

	for i, cc := range c.conns {
		if cc == conn {
			// Remove by swapping with last element and truncating
			c.conns[i] = c.conns[len(c.conns)-1]
			c.conns[len(c.conns)-1] = nil // Clear reference for GC
			c.conns = c.conns[:len(c.conns)-1]
			break
		}
	}

	// Check if this was the last connection - signal unavailability and drain
	// so callers can retry with other providers or wait for reconnect
	if len(c.conns) == 0 && !c.closed.Load() {
		c.signalDead()
		c.drainOrphanedRequests()
	}
}

// signalDead closes deadCh to signal that provider is unavailable.
// This unblocks any SendRequest calls waiting to send to reqCh.
func (c *Provider) signalDead() {
	var shouldLog bool
	c.deadChMu.Lock()
	// Only close if not already closed (check by trying to receive)
	select {
	case <-c.deadCh:
		// Already closed
	default:
		close(c.deadCh)
		shouldLog = true
	}
	c.deadChMu.Unlock()

	if shouldLog {
		slog.Info("provider offline", "host", c.Host)
	}
}

// signalAlive creates a new deadCh to allow SendRequest calls to proceed.
// Called when a new connection is established after provider was dead.
func (c *Provider) signalAlive() {
	var shouldLog bool
	c.deadChMu.Lock()
	// Only recreate if currently closed
	select {
	case <-c.deadCh:
		// Was closed, recreate
		c.deadCh = make(chan struct{})
		shouldLog = true
	default:
		// Still open, nothing to do
	}
	c.deadChMu.Unlock()

	if shouldLog {
		slog.Info("provider online", "host", c.Host)
	}
}

// drainOrphanedRequests drains any requests from reqCh with ErrProviderUnavailable.
// Called when all connections have died but the provider is not closed.
// This allows callers to fail fast and retry with other providers.
func (c *Provider) drainOrphanedRequests() {
	for {
		select {
		case req := <-c.reqCh:
			if req != nil && req.RespCh != nil {
				select {
				case req.RespCh <- Response{Err: ErrProviderUnavailable, Request: req}:
				default:
				}
				internal.SafeClose(req.RespCh)
			}
			// Note: We don't call sendWg.Done() here because the request
			// was already tracked and the caller's SendRequest defer already
			// handled the Done() call when sending completed.
		default:
			return // Channel empty
		}
	}
}

// Close cancels the provider, closes its request channel, and waits for all connections to stop.
func (c *Provider) Close() error {
	if !c.closed.CompareAndSwap(false, true) {
		return nil
	}

	slog.Info("provider closing", "host", c.Host, "active_connections", len(c.conns))

	c.cancel()

	// Wait for all in-flight SendRequest calls to complete before closing channel.
	// This prevents the race between sending on reqCh and closing it.
	c.sendWg.Wait()

	close(c.reqCh)
	// Drain any queued requests to prevent caller hangs
	for req := range c.reqCh {
		if req != nil && req.RespCh != nil {
			// Send error response so caller can distinguish from unexpected closure
			select {
			case req.RespCh <- Response{Err: ErrProviderClosed, Request: req}:
			default:
			}
			close(req.RespCh)
		}
	}

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
	closeWithError := func(err error) <-chan Response {
		if req.RespCh != nil {
			// Send error response so caller can distinguish from unexpected closure
			select {
			case req.RespCh <- Response{Err: err, Request: req}:
			default:
			}
			close(req.RespCh)
		}
		return req.RespCh
	}

	// Fast path: provider already closed
	if c.closed.Load() {
		return closeWithError(ErrProviderClosed)
	}

	// Connection scaling logic using channel backpressure instead of activeConsumers.
	// This eliminates 2 atomic operations per request cycle in the connection Run() loop.
	//
	// Decision matrix:
	// - connCount == 0: Must add a connection synchronously (provider was idle)
	// - connCount > 0 && connCount < max: Opportunistically scale up to handle concurrent load
	connCount := atomic.LoadInt32(&c.connCount)
	if connCount == 0 {
		// No connections - must add one synchronously
		if err := c.addConnection(false); err != nil {
			return closeWithError(ErrProviderUnavailable)
		}
		// Re-check in case addConnection succeeded
		if atomic.LoadInt32(&c.connCount) == 0 {
			return closeWithError(ErrProviderUnavailable)
		}
	}

	// Track this send operation so Close() can wait for us
	c.sendWg.Add(1)
	defer c.sendWg.Done()

	// Opportunistically scale up when under max connections.
	// This ensures we grow capacity to handle concurrent requests.
	// addConnection(false) will no-op if we're already at max.
	if atomic.LoadInt32(&c.connCount) < int32(c.config.MaxConnections) {
		c.sendWg.Add(1)
		go func() {
			defer c.sendWg.Done()
			_ = c.addConnection(false)
		}()
	}

	// Re-check closed after adding to wait group
	if c.closed.Load() {
		return closeWithError(ErrProviderClosed)
	}

	timeout := time.NewTimer(sendRequestTimeout)
	defer timeout.Stop()

	// Get current deadCh - used to detect when all connections die while we're waiting.
	// This MUST be grabbed after addConnection to get the fresh channel if signalAlive was called.
	c.deadChMu.Lock()
	deadCh := c.deadCh
	c.deadChMu.Unlock()

	// Check if deadCh is already closed (stale reference from concurrent addConnection race).
	// If so, but we have connections (connCount > 0), skip deadCh monitoring and just try
	// to send - the connection is being initialized and will start consuming shortly.
	skipDeadCh := false
	select {
	case <-deadCh:
		// deadCh is closed - check if we actually have connections
		if atomic.LoadInt32(&c.connCount) > 0 {
			// Connection exists but deadCh is stale (signalAlive racing with us).
			// Don't fail on deadCh - proceed to send.
			skipDeadCh = true
		} else {
			// No connections and deadCh closed - truly unavailable
			return closeWithError(ErrProviderUnavailable)
		}
	default:
		// deadCh is open, normal path
	}

	if skipDeadCh {
		// Don't monitor deadCh - just try to send with timeout
		select {
		case <-c.ctx.Done():
			return closeWithError(c.ctx.Err())
		case <-req.Ctx.Done():
			return closeWithError(req.Ctx.Err())
		case c.reqCh <- req:
			return req.RespCh
		case <-timeout.C:
			return closeWithError(fmt.Errorf("send request timeout after %v", sendRequestTimeout))
		}
	}

	select {
	case <-c.ctx.Done():
		return closeWithError(c.ctx.Err())
	case <-req.Ctx.Done():
		return closeWithError(req.Ctx.Err())
	case <-deadCh:
		// Provider became unavailable while we were waiting (all connections died)
		return closeWithError(ErrProviderUnavailable)
	case c.reqCh <- req:
		// Request is now in the channel, waiting to be processed
		return req.RespCh
	case <-timeout.C:
		return closeWithError(fmt.Errorf("send request timeout after %v", sendRequestTimeout))
	}
}
