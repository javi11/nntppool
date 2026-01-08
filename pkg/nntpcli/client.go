package nntpcli

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"sync"
	"time"
)

// ClientConfig holds configuration for the NNTP client connection pool.
type ClientConfig struct {
	// MaxConnections is the maximum number of connections to maintain.
	MaxConnections int

	// InflightPerConn is the number of pipelined requests per connection.
	InflightPerConn int

	// Auth contains authentication credentials.
	Auth Auth

	// Factory creates new network connections.
	Factory ConnFactory

	// IdleTimeout is how long a connection can be idle before being closed.
	// Set to 0 to disable idle timeout.
	IdleTimeout time.Duration

	// IdleCheckInterval is how often to check for idle connections.
	// Defaults to 30 seconds if IdleTimeout is set but this is 0.
	IdleCheckInterval time.Duration

	// WarmupConnections is the number of connections to create at startup.
	// Set to 0 for fully lazy connection creation.
	WarmupConnections int
}

// Client manages a pool of NNTP connections to a single server.
// Connections are created lazily on-demand and closed after idle timeout.
type Client struct {
	ctx    context.Context
	cancel context.CancelFunc

	// Request distribution channel - connections pull from this
	reqCh chan *Request

	// Connection management
	mu          sync.Mutex
	conns       map[*NNTPConnection]struct{}
	activeCount int
	maxConns    int
	creating    int  // Number of connections being created (to prevent over-creation)
	closed      bool

	// Factory for creating connections
	factory         ConnFactory
	inflightPerConn int
	auth            Auth

	// Idle timeout configuration
	idleTimeout       time.Duration
	idleCheckInterval time.Duration

	// Coordination
	wg       sync.WaitGroup
	createCh chan struct{} // Signal to create new connection (buffered 1)

	// Group tracking - new connections will auto-select this group
	currentGroup string
}

// NewClient creates a new NNTP client with the specified number of connections.
// For backward compatibility, this creates all connections at startup with no idle timeout.
func NewClient(ctx context.Context, addr string, tlsConfig *tls.Config, connections, inflightPerConnection int, auth Auth) (*Client, error) {
	factory := func(_ context.Context) (net.Conn, error) {
		return newNetConn(addr, tlsConfig)
	}
	return NewClientWithConnFactory(ctx, connections, inflightPerConnection, auth, factory)
}

// NewClientWithConnFactory creates a new NNTP client using a custom connection factory.
// For backward compatibility, this creates all connections at startup with no idle timeout.
func NewClientWithConnFactory(ctx context.Context, connections, inflightPerConnection int, auth Auth, factory ConnFactory) (*Client, error) {
	cfg := ClientConfig{
		MaxConnections:    connections,
		InflightPerConn:   inflightPerConnection,
		Auth:              auth,
		Factory:           factory,
		IdleTimeout:       0, // No idle timeout for backward compatibility
		WarmupConnections: connections, // Create all connections at startup
	}
	return NewClientLazy(ctx, cfg)
}

// NewClientLazy creates a new NNTP client with lazy connection creation.
// Connections are created on-demand up to MaxConnections.
// If WarmupConnections > 0, that many connections are created at startup.
func NewClientLazy(ctx context.Context, cfg ClientConfig) (*Client, error) {
	if cfg.Factory == nil {
		return nil, fmt.Errorf("nntpcli: ConnFactory is required")
	}
	if cfg.MaxConnections <= 0 {
		return nil, fmt.Errorf("nntpcli: MaxConnections must be positive")
	}
	if cfg.InflightPerConn <= 0 {
		cfg.InflightPerConn = 8 // Enable pipelining by default for high throughput
	}
	if ctx == nil {
		ctx = context.Background()
	}

	// Default idle check interval
	if cfg.IdleTimeout > 0 && cfg.IdleCheckInterval == 0 {
		cfg.IdleCheckInterval = 30 * time.Second
	}

	ctx, cancel := context.WithCancel(ctx)
	c := &Client{
		ctx:               ctx,
		cancel:            cancel,
		reqCh:             make(chan *Request, cfg.MaxConnections*cfg.InflightPerConn),
		conns:             make(map[*NNTPConnection]struct{}),
		maxConns:          cfg.MaxConnections,
		factory:           cfg.Factory,
		inflightPerConn:   cfg.InflightPerConn,
		auth:              cfg.Auth,
		idleTimeout:       cfg.IdleTimeout,
		idleCheckInterval: cfg.IdleCheckInterval,
		createCh:          make(chan struct{}, 1),
	}

	// Start connection manager goroutine
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		c.runConnectionManager()
	}()

	// Start idle reaper if configured
	if c.idleTimeout > 0 {
		c.wg.Add(1)
		go func() {
			defer c.wg.Done()
			c.runIdleReaper()
		}()
	}

	// Warmup connections if configured
	for i := 0; i < cfg.WarmupConnections && i < cfg.MaxConnections; i++ {
		if !c.createConnection() {
			// If warmup fails, close what we have and return error
			c.cancel()
			c.wg.Wait()
			return nil, fmt.Errorf("nntpcli: failed to create warmup connection %d", i+1)
		}
	}

	return c, nil
}

// Close cancels the client, closes all connections, and waits for cleanup.
func (c *Client) Close() error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	c.closed = true
	c.mu.Unlock()

	c.cancel()
	close(c.reqCh)

	// Close all connections
	c.mu.Lock()
	for conn := range c.conns {
		_ = conn.Close()
	}
	c.mu.Unlock()

	c.wg.Wait()
	return nil
}

// Send submits a request to the NNTP server and returns a channel for the response.
// The payload should be a complete NNTP command including the trailing CRLF.
// If bodyWriter is provided, decoded body data will be streamed there instead of
// being buffered in the response.
func (c *Client) Send(ctx context.Context, payload []byte, bodyWriter io.Writer) <-chan Response {
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

	// Ensure at least one connection exists or is being created
	c.mu.Lock()
	needsConnection := c.activeCount == 0 && c.creating == 0 && !c.closed
	if needsConnection {
		// Mark that we're about to create connections to prevent other Send() calls
		// from also trying to be "first"
		c.creating = 1
	}
	c.mu.Unlock()
	if needsConnection {
		// Launch background connections first (they'll run in parallel)
		for i := 1; i < c.maxConns; i++ {
			go c.createConnection()
		}
		// Then create the first connection synchronously for immediate availability
		c.doCreateConnection()
	}

	// Try non-blocking send first
	select {
	case <-c.ctx.Done():
		close(respCh)
		return respCh
	case <-ctx.Done():
		close(respCh)
		return respCh
	case c.reqCh <- req:
		return respCh
	default:
		// Channel full - need more connections
		c.triggerConnectionCreation()
	}

	// Blocking send with context
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

// Connections returns the current number of active connections.
func (c *Client) Connections() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.activeCount
}

// MaxConnections returns the maximum number of connections.
func (c *Client) MaxConnections() int {
	return c.maxConns
}

// SetGroup sets the current newsgroup for this client.
// New connections will automatically select this group.
// This should be called after a successful GROUP command.
func (c *Client) SetGroup(group string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.currentGroup = group
}

// triggerConnectionCreation signals the connection manager to create a new connection.
func (c *Client) triggerConnectionCreation() {
	select {
	case c.createCh <- struct{}{}:
	default:
		// Already signaled, don't block
	}
}

// runConnectionManager handles connection lifecycle management.
func (c *Client) runConnectionManager() {
	for {
		select {
		case <-c.ctx.Done():
			return
		case <-c.createCh:
			// Scale connections aggressively based on demand
			c.scaleConnections()
		}
	}
}

// scaleConnections creates multiple connections in parallel based on queue depth.
// This ensures rapid scaling under high load.
func (c *Client) scaleConnections() {
	c.mu.Lock()
	queueLen := len(c.reqCh)
	activeConns := c.activeCount
	creating := c.creating
	closed := c.closed
	c.mu.Unlock()

	if closed {
		return
	}

	// Calculate how many connections we need
	currentCapacity := activeConns + creating
	available := c.maxConns - currentCapacity
	if available <= 0 {
		return
	}

	// Calculate connections needed based on queue depth and current state
	var toCreate int
	if activeConns == 0 && creating == 0 {
		// No connections at all - scale up aggressively but not to full max
		// to avoid overwhelming the server
		toCreate = (c.maxConns + 1) / 2 // Half of max, rounded up
		if toCreate > available {
			toCreate = available
		}
	} else if queueLen > 0 {
		// Scale proportionally to queue depth, minimum 2 at a time
		toCreate = queueLen
		if toCreate < 2 {
			toCreate = 2
		}
		if toCreate > available {
			toCreate = available
		}
	} else {
		return
	}

	for i := 0; i < toCreate; i++ {
		go c.createConnection()
	}
}

// createConnection creates a new connection if under the max limit.
// Returns true if a connection was created, false otherwise.
func (c *Client) createConnection() bool {
	c.mu.Lock()
	if c.closed || c.activeCount+c.creating >= c.maxConns {
		c.mu.Unlock()
		return false
	}
	c.creating++
	c.mu.Unlock()

	return c.doCreateConnection()
}

// doCreateConnection performs the actual connection creation.
// Caller must ensure c.creating has been incremented before calling.
func (c *Client) doCreateConnection() bool {
	// Create the network connection
	conn, err := c.factory(c.ctx)
	if err != nil {
		c.mu.Lock()
		c.creating--
		c.mu.Unlock()
		return false
	}

	// Create the NNTP connection
	nntpConn, err := newNNTPConnectionFromConn(c.ctx, conn, c.inflightPerConn, c.reqCh, c.auth)
	if err != nil {
		_ = conn.Close()
		c.mu.Lock()
		c.creating--
		c.mu.Unlock()
		return false
	}

	// If a group is set, send GROUP command to the new connection before it starts
	// processing from the shared channel
	c.mu.Lock()
	group := c.currentGroup
	c.mu.Unlock()
	if group != "" {
		if err := nntpConn.SendGroup(c.ctx, group); err != nil {
			_ = nntpConn.Close()
			c.mu.Lock()
			c.creating--
			c.mu.Unlock()
			return false
		}
	}

	// Set up the close callback for automatic removal
	nntpConn.SetOnClose(c.removeConnection)

	// Register the connection
	c.mu.Lock()
	c.creating--
	c.conns[nntpConn] = struct{}{}
	c.activeCount++
	c.mu.Unlock()

	// Start the connection's processing loop
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		nntpConn.Run()
	}()

	return true
}

// removeConnection removes a connection from the pool.
// This is called by the connection's onClose callback when it terminates.
func (c *Client) removeConnection(conn *NNTPConnection) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, exists := c.conns[conn]; exists {
		delete(c.conns, conn)
		c.activeCount--
	}
}

// runIdleReaper periodically checks for and closes idle connections.
func (c *Client) runIdleReaper() {
	ticker := time.NewTicker(c.idleCheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-c.ctx.Done():
			return
		case <-ticker.C:
			c.closeIdleConnections()
		}
	}
}

// closeIdleConnections closes connections that have been idle too long.
func (c *Client) closeIdleConnections() {
	now := time.Now()
	threshold := now.Add(-c.idleTimeout)

	c.mu.Lock()
	var toClose []*NNTPConnection
	for conn := range c.conns {
		lastActivity := conn.LastActivity()
		// Only close if connection has had activity (not just created)
		// and has been idle longer than threshold
		if !lastActivity.IsZero() && lastActivity.Before(threshold) {
			toClose = append(toClose, conn)
		}
	}
	c.mu.Unlock()

	// Close idle connections outside the lock
	for _, conn := range toClose {
		_ = conn.Close()
	}
}
