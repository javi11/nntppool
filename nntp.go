package nntppool

import (
	"bufio"
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

var ErrMaxConnections = errors.New("nntp: server max connections reached")
var ErrConnectionDied = errors.New("nntp: connection died")

const (
	// inflightDrainTimeout is the maximum time to wait for in-flight
	// responses to complete during idle disconnect.
	inflightDrainTimeout = 10 * time.Second

	// defaultThrottleRestore is the default duration before restoring
	// throttled connection slots after a server "max connections" error.
	defaultThrottleRestore = 30 * time.Second

	// connFailureBackoff is the delay before retrying after a connection
	// factory error.
	connFailureBackoff = time.Second

	// maxConnsBackoff is the longer delay used when the server reports
	// max connections reached (502/400).
	maxConnsBackoff = 5 * time.Second

	// defaultKeepAlive is the TCP keep-alive interval used when the
	// provider does not specify one. Negative disables keep-alive.
	defaultKeepAlive = 30 * time.Second

	// defaultHandshakeTimeout caps the TCP dial + TLS handshake phase
	// to avoid hanging against unresponsive servers.
	defaultHandshakeTimeout = 10 * time.Second
)

type greetingError struct {
	StatusCode int
	Message    string
}

func (e *greetingError) Error() string {
	return fmt.Sprintf("unexpected nntp greeting: %s", e.Message)
}

func (e *greetingError) Is(target error) bool {
	return target == ErrMaxConnections && (e.StatusCode == 502 || e.StatusCode == 400)
}

type Request struct {
	Ctx context.Context

	Payload []byte
	RespCh  chan Response

	// Optional: decoded body bytes are streamed here. If nil, they are buffered into Response.Body.
	BodyWriter io.Writer

	// Optional: called with yEnc metadata once =ybegin/=ypart headers are parsed, before body decoding.
	OnMeta func(YEncMeta)
}

type Response struct {
	StatusCode int
	Status     string

	// For non-body multiline responses (CAPABILITIES, etc).
	Lines []string

	// Decoded payload bytes (only if Request.BodyWriter == nil).
	Body bytes.Buffer

	// Decoder metadata/status gathered while parsing.
	Meta NNTPResponse

	Err     error
	Request *Request
}

type Auth struct {
	Username string
	Password string
}

// ConnFactory is used by Client to create connections.
type ConnFactory func(ctx context.Context) (net.Conn, error)

type NNTPConnection struct {
	conn net.Conn

	ctx    context.Context
	cancel context.CancelFunc

	reqCh      <-chan *Request
	prioCh     <-chan *Request // priority channel; nil for standalone connections
	hotReqCh   <-chan *Request // unbuffered; set by runConnSlot before Run()
	hotPrioCh  <-chan *Request // unbuffered; set by runConnSlot before Run()
	pending    chan *Request

	inflightSem chan struct{}

	rb readBuffer

	Greeting NNTPResponse

	firstReq    *Request      // bootstrap request from connection slot
	idleTimeout time.Duration // 0 = no idle timeout

	stats *providerStats // nil for standalone connections

	done   chan struct{}
	doneMu sync.Once

	failMu sync.Once
}

func newNetConn(ctx context.Context, addr string, tlsConfig *tls.Config, keepAlive time.Duration) (net.Conn, error) {
	if keepAlive == 0 {
		keepAlive = defaultKeepAlive
	}
	ctx, cancel := context.WithTimeout(ctx, defaultHandshakeTimeout)
	defer cancel()
	dialer := net.Dialer{KeepAlive: keepAlive}
	if tlsConfig != nil {
		conn, err := dialer.DialContext(ctx, "tcp", addr)
		if err != nil {
			return nil, err
		}
		tlsConn := tls.Client(conn, tlsConfig)
		if err := tlsConn.HandshakeContext(ctx); err != nil {
			_ = conn.Close()
			return nil, err
		}
		return tlsConn, nil
	}
	conn, err := dialer.DialContext(ctx, "tcp", addr)
	if err != nil {
		return nil, err
	}
	return conn, nil
}

func newNNTPConnectionFromConn(ctx context.Context, conn net.Conn, inflightLimit int, reqCh <-chan *Request, prioCh <-chan *Request, auth Auth, sharedBuf *readBuffer, stats *providerStats) (*NNTPConnection, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	cctx, cancel := context.WithCancel(ctx)

	var rb readBuffer
	if sharedBuf != nil && len(sharedBuf.buf) > 0 {
		// Reuse the buffer from a previous connection, reset read positions and deadline cache.
		rb = readBuffer{buf: sharedBuf.buf}
	} else {
		rb = readBuffer{buf: make([]byte, defaultReadBufSize)}
	}

	c := &NNTPConnection{
		conn:        conn,
		ctx:         cctx,
		cancel:      cancel,
		reqCh:       reqCh,
		prioCh:      prioCh,
		pending:     make(chan *Request, inflightLimit),
		inflightSem: make(chan struct{}, inflightLimit),
		rb:          rb,
		stats:       stats,
		done:        make(chan struct{}),
	}

	// Server greeting is sent immediately upon connect.
	greeting, err := c.readOneResponse(io.Discard)
	if err != nil {
		return nil, fmt.Errorf("nntp greeting: %w", err)
	}
	c.Greeting = greeting
	if greeting.StatusCode != 200 && greeting.StatusCode != 201 {
		return nil, &greetingError{StatusCode: greeting.StatusCode, Message: greeting.Message}
	}

	// Optional AUTHINFO handshake.
	if auth.Username != "" {
		if auth.Password == "" {
			return nil, fmt.Errorf("password required when username is set")
		}

		if err := c.auth(auth); err != nil {
			return nil, err
		}
	}

	return c, nil
}

func NewNNTPConnection(ctx context.Context, addr string, tlsConfig *tls.Config, inflightLimit int, reqCh <-chan *Request, auth Auth) (*NNTPConnection, error) {
	conn, err := newNetConn(ctx, addr, tlsConfig, 0)
	if err != nil {
		return nil, err
	}

	c, err := newNNTPConnectionFromConn(ctx, conn, inflightLimit, reqCh, nil, auth, nil, nil)
	if err != nil {
		_ = conn.Close()
		return nil, err
	}
	return c, nil
}

func (c *NNTPConnection) auth(auth Auth) error {
	// AUTHINFO USER
	if _, err := fmt.Fprintf(c.conn, "AUTHINFO USER %s\r\n", auth.Username); err != nil {
		return fmt.Errorf("authinfo user: %w", err)
	}
	resp, err := c.readOneResponse(io.Discard)
	if err != nil {
		return fmt.Errorf("authinfo user response: %w", err)
	}

	switch resp.StatusCode {
	case 281:
		return nil // authenticated
	case 381:
		// need pass
	default:
		return fmt.Errorf("authinfo user unexpected response: %s", resp.Message)
	}

	// AUTHINFO PASS
	if _, err := fmt.Fprintf(c.conn, "AUTHINFO PASS %s\r\n", auth.Password); err != nil {
		return fmt.Errorf("authinfo pass: %w", err)
	}
	resp, err = c.readOneResponse(io.Discard)
	if err != nil {
		return fmt.Errorf("authinfo pass response: %w", err)
	}
	if resp.StatusCode != 281 {
		return fmt.Errorf("authinfo pass unexpected response: %s", resp.Message)
	}
	return nil
}

func (c *NNTPConnection) Done() <-chan struct{} { return c.done }

func (c *NNTPConnection) closeDone() {
	c.doneMu.Do(func() { close(c.done) })
}

func safeClose[T any](ch chan T) {
	defer func() { _ = recover() }()
	close(ch)
}

func failRequest(ch chan Response, err error) {
	defer func() { _ = recover() }()
	select {
	case ch <- Response{Err: err}:
	default:
	}
	close(ch)
}

func (c *NNTPConnection) failOutstanding() {
	c.failMu.Do(func() {
		for {
			select {
			case req := <-c.pending:
				if req == nil {
					continue
				}
				failRequest(req.RespCh, ErrConnectionDied)
				// Best-effort inflight release (not strictly needed once we're shutting down).
				select {
				case <-c.inflightSem:
				default:
				}
			default:
				return
			}
		}
	})
}

func (c *NNTPConnection) Close() error {
	c.cancel()
	_ = c.conn.Close()
	<-c.done
	return nil
}

// waitForInflightDrain acquires all semaphore slots, blocking until each
// in-flight response completes. This ensures a clean idle disconnect with
// no lost requests. A 10s timeout prevents hanging if the server stops
// responding mid-response.
func (c *NNTPConnection) waitForInflightDrain() {
	timer := time.NewTimer(inflightDrainTimeout)
	defer timer.Stop()
	for range cap(c.inflightSem) {
		select {
		case c.inflightSem <- struct{}{}:
		case <-c.ctx.Done():
			return
		case <-timer.C:
			return
		}
	}
}

// connGate controls how many connection slots may be connecting/running
// simultaneously within a single provider. When the server returns a
// "max connections" greeting (502/400), throttle() reduces the allowed
// count to the number of currently running connections (min 1) and starts
// a restore timer.
type connGate struct {
	mu           sync.Mutex
	cond         *sync.Cond
	maxSlots     int // original p.Connections
	allowed      int // current limit (reduced during throttle)
	held         int // slots past enter() (connecting + running)
	running      int // slots inside nc.Run()
	restoreTimer *time.Timer
	restoreDur   time.Duration
	available    atomic.Int32 // allowed - held; updated under mu, read lock-free
}

func newConnGate(max int, restoreDur time.Duration) *connGate {
	if restoreDur <= 0 {
		restoreDur = defaultThrottleRestore
	}
	g := &connGate{
		maxSlots:   max,
		allowed:    max,
		restoreDur: restoreDur,
	}
	g.cond = sync.NewCond(&g.mu)
	g.available.Store(int32(max))
	return g
}

// enter blocks until held < allowed or one of the contexts is cancelled.
// Returns true if the slot was granted.
func (g *connGate) enter(slotCtx, reqCtx context.Context) bool {
	// Spin up a goroutine that broadcasts on context cancellation so
	// cond.Wait() can re-check.
	done := make(chan struct{})
	go func() {
		select {
		case <-slotCtx.Done():
		case <-reqCtx.Done():
		case <-done:
		}
		g.cond.Broadcast()
	}()

	g.mu.Lock()
	defer g.mu.Unlock()
	defer close(done)

	for g.held >= g.allowed {
		if slotCtx.Err() != nil || reqCtx.Err() != nil {
			return false
		}
		g.cond.Wait()
	}
	g.held++
	g.available.Store(int32(g.allowed - g.held))
	return true
}

func (g *connGate) exit() {
	g.mu.Lock()
	g.held--
	g.available.Store(int32(g.allowed - g.held))
	g.mu.Unlock()
	g.cond.Broadcast()
}

func (g *connGate) markRunning() {
	g.mu.Lock()
	g.running++
	g.mu.Unlock()
}

func (g *connGate) markNotRunning() {
	g.mu.Lock()
	g.running--
	g.mu.Unlock()
}

// throttle reduces allowed slots to max(1, running) and resets the restore timer.
func (g *connGate) throttle() {
	g.mu.Lock()
	defer g.mu.Unlock()

	newAllowed := max(1, g.running)
	// Only tighten, never loosen during throttle.
	if newAllowed < g.allowed {
		g.allowed = newAllowed
	}

	// Reset (or start) the restore timer.
	if g.restoreTimer != nil {
		g.restoreTimer.Stop()
	}
	g.restoreTimer = time.AfterFunc(g.restoreDur, g.restore)
	g.available.Store(int32(g.allowed - g.held))
}

func (g *connGate) restore() {
	g.mu.Lock()
	g.allowed = g.maxSlots
	g.restoreTimer = nil
	g.available.Store(int32(g.allowed - g.held))
	g.mu.Unlock()
	g.cond.Broadcast()
}

func (g *connGate) stop() {
	g.mu.Lock()
	if g.restoreTimer != nil {
		g.restoreTimer.Stop()
		g.restoreTimer = nil
	}
	g.mu.Unlock()
	g.cond.Broadcast()
}

func (g *connGate) snapshot() (maxSlots, running int) {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.maxSlots, g.running
}

// runConnSlot is the slot goroutine that manages the lifecycle of a single
// connection: IDLE → CONNECTING → ACTIVE → (death/idle) → IDLE.
func runConnSlot(ctx context.Context, reqCh <-chan *Request, prioCh <-chan *Request, hotReqCh <-chan *Request, hotPrioCh <-chan *Request, factory ConnFactory, inflight int, auth Auth, idleTimeout time.Duration, gate *connGate, stats *providerStats, wg *sync.WaitGroup) {
	defer wg.Done()

	// Shared read buffer persists across reconnections to avoid re-growing.
	var sharedBuf readBuffer

	for {
		// IDLE: wait for a request (zero TCP resources).
		// Prefer priority requests over normal ones.
		var firstReq *Request
		var ok bool
		select {
		case firstReq, ok = <-prioCh:
			if !ok {
				return
			}
		default:
			select {
			case firstReq, ok = <-prioCh:
				if !ok {
					return
				}
			case firstReq, ok = <-reqCh:
				if !ok {
					return // channel closed, shut down
				}
			case <-ctx.Done():
				return
			}
		}

		// Check if the request is already cancelled.
		select {
		case <-firstReq.Ctx.Done():
			failRequest(firstReq.RespCh, firstReq.Ctx.Err())
			continue
		default:
		}

		// GATE: block if we're at the throttled capacity limit.
		if !gate.enter(ctx, firstReq.Ctx) {
			// Slot or request context cancelled while waiting at the gate.
			failRequest(firstReq.RespCh, context.Canceled)
			continue
		}

		// CONNECTING: dial, greet, authenticate.
		conn, err := factory(ctx)
		if err != nil {
			gate.exit()
			failRequest(firstReq.RespCh, err)
			// Backoff before retrying to avoid thrashing.
			select {
			case <-time.After(connFailureBackoff):
			case <-ctx.Done():
				return
			}
			continue
		}

		nc, err := newNNTPConnectionFromConn(ctx, conn, inflight, reqCh, prioCh, auth, &sharedBuf, stats)
		if err != nil {
			_ = conn.Close()
			failRequest(firstReq.RespCh, err)

			if errors.Is(err, ErrMaxConnections) {
				// Server said "max connections" — throttle and use longer backoff.
				gate.throttle()
				gate.exit()
				select {
				case <-time.After(maxConnsBackoff):
				case <-ctx.Done():
					return
				}
			} else {
				gate.exit()
				select {
				case <-time.After(connFailureBackoff):
				case <-ctx.Done():
					return
				}
			}
			continue
		}

		// ACTIVE: run the connection with the bootstrap request.
		nc.firstReq = firstReq
		nc.idleTimeout = idleTimeout
		nc.hotReqCh = hotReqCh
		nc.hotPrioCh = hotPrioCh
		gate.markRunning()
		nc.Run() // blocks until death or idle timeout
		gate.markNotRunning()
		gate.exit()

		// Preserve the (possibly grown) read buffer for next connection.
		sharedBuf.buf = nc.rb.buf

		// Loop back to IDLE for automatic reconnection.
	}
}

type streamFeeder interface {
	Feed(in []byte, out io.Writer) (consumed int, done bool, err error)
}

type writerRef struct {
	w io.Writer
}

func (wr *writerRef) Write(p []byte) (int, error) {
	return wr.w.Write(p)
}

func (c *NNTPConnection) Run() {
	defer func() {
		c.cancel()
		_ = c.conn.Close()
		c.failOutstanding()
		c.closeDone()
	}()

	go func() {
		c.readerLoop()
		// ensure writer exits too
		c.cancel()
	}()

	// Buffered writer coalesces multiple small BODY commands into fewer
	// write syscalls when inflight > 1. Flushed before any blocking op.
	bw := bufio.NewWriterSize(c.conn, 4096)

	// Cached write deadline state to avoid redundant SetWriteDeadline syscalls.
	var lastWriteDL time.Time
	lastWriteHasDL := false
	writeDLSet := false

	setWriteDeadline := func(dl time.Time, hasDL bool) {
		if writeDLSet && lastWriteHasDL == hasDL && (!hasDL || dl.Equal(lastWriteDL)) {
			return
		}
		if hasDL {
			_ = c.conn.SetWriteDeadline(dl)
		} else {
			_ = c.conn.SetWriteDeadline(time.Time{})
		}
		lastWriteDL = dl
		lastWriteHasDL = hasDL
		writeDLSet = true
	}

	// Process the bootstrap request injected by runConnSlot, if any.
	if c.firstReq != nil {
		req := c.firstReq
		c.firstReq = nil

		if req.Ctx == nil {
			req.Ctx = context.Background()
		}

		// Check cancellation.
		select {
		case <-req.Ctx.Done():
			failRequest(req.RespCh, req.Ctx.Err())
			// Connection is still good — fall through to main loop.
			goto mainLoop
		default:
		}

		// Acquire inflight slot.
		select {
		case c.inflightSem <- struct{}{}:
		case <-c.ctx.Done():
			failRequest(req.RespCh, c.ctx.Err())
			return
		}

		c.pending <- req

		dl, hasDL := req.Ctx.Deadline()
		setWriteDeadline(dl, hasDL)

		if _, err := bw.Write(req.Payload); err != nil {
			<-c.inflightSem
			failRequest(req.RespCh, err)
			_ = c.conn.Close()
			c.failOutstanding()
			return
		}
	}

mainLoop:
	// Flush any buffered writes before blocking.
	if bw.Buffered() > 0 {
		if err := bw.Flush(); err != nil {
			return
		}
	}

	// Set up idle timer (nil if no idle timeout configured).
	var idleTimer *time.Timer
	var idleCh <-chan time.Time
	if c.idleTimeout > 0 {
		idleTimer = time.NewTimer(c.idleTimeout)
		idleCh = idleTimer.C
		defer idleTimer.Stop()
	}

	for {
		select {
		case <-c.ctx.Done():
			return
		default:
		}

		// Flush buffered writes before blocking on semaphore.
		if bw.Buffered() > 0 {
			if err := bw.Flush(); err != nil {
				return
			}
		}

		// wait until we have inflight capacity
		select {
		case c.inflightSem <- struct{}{}:
		case <-c.ctx.Done():
			return
		}

		// Flush buffered writes before blocking on channel read.
		if bw.Buffered() > 0 {
			if err := bw.Flush(); err != nil {
				<-c.inflightSem
				return
			}
		}

		// pull next request (with idle timeout)
		// Hot channels are tried first (non-blocking) so that requests
		// prefer already-connected connections over waking cold slots.
		// When hotReqCh/hotPrioCh are nil (standalone path), receives
		// from nil channels block forever in select and are excluded.
		var req *Request
		var ok bool
		if c.prioCh != nil {
			// Try hot priority (non-blocking).
			select {
			case req, ok = <-c.hotPrioCh:
			default:
				// Try hot normal (non-blocking).
				select {
				case req, ok = <-c.hotReqCh:
				default:
					// Blocking: wait on all channels.
					select {
					case req, ok = <-c.hotPrioCh:
					case req, ok = <-c.hotReqCh:
					case req, ok = <-c.prioCh:
					case req, ok = <-c.reqCh:
					case <-c.ctx.Done():
						<-c.inflightSem
						return
					case <-idleCh:
						<-c.inflightSem
						c.waitForInflightDrain()
						return
					}
				}
			}
		} else {
			select {
			case req, ok = <-c.reqCh:
			case <-c.ctx.Done():
				<-c.inflightSem
				return
			case <-idleCh:
				<-c.inflightSem
				c.waitForInflightDrain()
				return
			}
		}
		if !ok {
			<-c.inflightSem
			return
		}
		if req.Ctx == nil {
			req.Ctx = context.Background()
		}

		// Reset idle timer since we got a request.
		if idleTimer != nil {
			if !idleTimer.Stop() {
				select {
				case <-idleTimer.C:
				default:
				}
			}
			idleTimer.Reset(c.idleTimeout)
		}

		// Cancel before sending (queued-but-not-sent case)
		select {
		case <-req.Ctx.Done():
			<-c.inflightSem
			failRequest(req.RespCh, req.Ctx.Err())
			continue
		default:
		}

		// track FIFO ordering
		c.pending <- req

		// per-request write deadline (cached to avoid redundant syscalls)
		dl, hasDL := req.Ctx.Deadline()
		setWriteDeadline(dl, hasDL)

		// pipeline write (buffered; flushed at top of loop before blocking)
		if _, err := bw.Write(req.Payload); err != nil {
			<-c.inflightSem
			failRequest(req.RespCh, err)
			_ = c.conn.Close()
			c.failOutstanding()
			return
		}
	}
}

func (c *NNTPConnection) readerLoop() {
	for {
		select {
		case <-c.ctx.Done():
			c.failOutstanding()
			return
		default:
		}

		// Match FIFO request
		var req *Request
		select {
		case req = <-c.pending:
		case <-c.ctx.Done():
			return
		}
		if req.Ctx == nil {
			req.Ctx = context.Background()
		}

		resp := Response{
			Request: req,
		}
		decoder := NNTPResponse{
			onMeta: req.OnMeta,
		}

		// If the request is cancelled after send, we must still drain its response off the wire,
		// but we don't deliver it.
		deliver := true
		select {
		case <-req.Ctx.Done():
			deliver = false
		default:
		}

		out := req.BodyWriter
		if !deliver {
			out = io.Discard
		} else if out == nil {
			out = &resp.Body
		}

		// Allow us to switch output to io.Discard if the request is cancelled while
		// we are still draining the response.
		outRef := &writerRef{w: out}

		err := c.rb.feedUntilDone(c.conn, &decoder, outRef, func() (time.Time, bool) {
			if deliver {
				select {
				case <-req.Ctx.Done():
					deliver = false
					outRef.w = io.Discard
				default:
				}
			}
			return req.Ctx.Deadline()
		})
		if err != nil {
			resp.Err = err
		}

		resp.StatusCode = decoder.StatusCode
		resp.Status = decoder.Message
		resp.Lines = decoder.Lines
		resp.Meta = decoder

		if c.stats != nil {
			c.stats.BytesConsumed.Add(int64(decoder.BytesConsumed))
			if resp.Err != nil {
				c.stats.Errors.Add(1)
			} else if decoder.StatusCode == 430 || decoder.StatusCode == 423 {
				c.stats.Missing.Add(1)
			} else if decoder.StatusCode < 200 || decoder.StatusCode >= 400 {
				c.stats.Errors.Add(1)
			}
		}

		// Release inflight slot BEFORE delivering the response so the
		// writer goroutine can loop back to the hot-channel select before
		// the caller (unblocked by the delivery below) sends the next request.
		<-c.inflightSem

		if deliver {
			// Best effort: don't block forever if the receiver abandoned the channel.
			select {
			case req.RespCh <- resp:
			default:
			}
		}
		safeClose(req.RespCh)

		// If we hit a timeout or cancellation-related network error, close the connection.
		if resp.Err != nil {
			var ne net.Error
			if errors.As(resp.Err, &ne) && ne.Timeout() {
				_ = c.conn.Close()
				c.failOutstanding()
				return
			}
		}
	}
}

// readOneResponse reads a complete NNTP response from the stream.
// Any unread bytes remain buffered in c.rbuf[c.rstart:c.rend] for subsequent reads.
func (c *NNTPConnection) readOneResponse(out io.Writer) (NNTPResponse, error) {
	resp := NNTPResponse{}
	if err := c.rb.feedUntilDone(c.conn, &resp, out, func() (time.Time, bool) { return time.Time{}, false }); err != nil {
		return resp, err
	}
	return resp, nil
}

// DispatchStrategy controls how the client distributes requests across main providers.
type DispatchStrategy int

const (
	// DispatchRoundRobin distributes requests using dynamic weighted round-robin
	// based on each provider's available capacity. This is the default.
	DispatchRoundRobin DispatchStrategy = iota

	// DispatchFIFO sends all requests to the first provider that has capacity.
	// Overflow cascades to subsequent providers in declaration order.
	DispatchFIFO
)

// ClientOption configures optional Client behavior.
type ClientOption func(*clientConfig)

type clientConfig struct {
	dispatch DispatchStrategy
}

// WithDispatchStrategy sets the request distribution strategy for main providers.
// The default is DispatchRoundRobin.
func WithDispatchStrategy(s DispatchStrategy) ClientOption {
	return func(cfg *clientConfig) { cfg.dispatch = s }
}

// Provider describes a single NNTP server with its own credentials and connection count.
type Provider struct {
	Host            string
	TLSConfig       *tls.Config
	Auth            Auth
	Connections     int
	Inflight        int           // 0 defaults to 1
	Factory         ConnFactory   // overrides Host/TLSConfig when set
	Backup          bool          // if true, only used when main providers return 430
	IdleTimeout     time.Duration // 0 means no idle disconnect
	ThrottleRestore time.Duration // 0 defaults to 30s
	KeepAlive       time.Duration // TCP keep-alive interval; 0 defaults to 30s; negative disables
}

type providerGroup struct {
	name     string
	host     string // raw Provider.Host; empty for Factory-based providers
	maxConns int
	ctx      context.Context    // cancelled on removal/close
	reqCh    chan *Request
	prioCh   chan *Request // priority requests; connections prefer this over reqCh
	hotReqCh  chan *Request // unbuffered; hot (connected) connections read this
	hotPrioCh chan *Request // unbuffered; hot priority connections read this
	gate     *connGate
	stats    providerStats
	cancel   context.CancelFunc // cancels this group's slot goroutines
}

type Client struct {
	ctx    context.Context
	cancel context.CancelFunc

	mainGroups   atomic.Pointer[[]*providerGroup]
	backupGroups atomic.Pointer[[]*providerGroup]
	nextIdx      atomic.Uint64 // round-robin counter for mainGroups

	dispatch DispatchStrategy // set once by NewClient, read-only after

	providerIdx atomic.Int64 // monotonic counter for unnamed providers

	startTime time.Time
	wg        sync.WaitGroup
}

// parseDateResponse parses an NNTP DATE response message.
// message is the full status line, e.g. "111 20240315120000".
func parseDateResponse(message string) (time.Time, error) {
	// Skip "111 " prefix if present.
	ts := message
	if len(ts) > 4 && ts[3] == ' ' {
		ts = ts[4:]
	}
	if len(ts) < 14 {
		return time.Time{}, fmt.Errorf("nntp: DATE response too short: %q", message)
	}
	return time.Parse("20060102150405", ts[:14])
}

// pingProvider dials a temporary connection, authenticates, sends DATE, and
// measures RTT. The connection is always closed before returning.
func pingProvider(ctx context.Context, factory ConnFactory, auth Auth) PingResult {
	conn, err := factory(ctx)
	if err != nil {
		return PingResult{Err: fmt.Errorf("ping dial: %w", err)}
	}
	if conn == nil {
		return PingResult{Err: fmt.Errorf("ping dial: factory returned nil connection")}
	}
	defer func() { _ = conn.Close() }()

	rb := readBuffer{buf: make([]byte, defaultReadBufSize)}
	nc := &NNTPConnection{
		conn: conn,
		rb:   rb,
	}

	// Read greeting.
	greeting, err := nc.readOneResponse(io.Discard)
	if err != nil {
		return PingResult{Err: fmt.Errorf("ping greeting: %w", err)}
	}
	if greeting.StatusCode != 200 && greeting.StatusCode != 201 {
		return PingResult{Err: &greetingError{StatusCode: greeting.StatusCode, Message: greeting.Message}}
	}

	// Auth if needed.
	if auth.Username != "" {
		if err := nc.auth(auth); err != nil {
			return PingResult{Err: fmt.Errorf("ping auth: %w", err)}
		}
	}

	// Send DATE and measure RTT.
	start := time.Now()
	if _, err := conn.Write([]byte("DATE\r\n")); err != nil {
		return PingResult{Err: fmt.Errorf("ping write DATE: %w", err)}
	}
	resp, err := nc.readOneResponse(io.Discard)
	rtt := time.Since(start)
	if err != nil {
		return PingResult{Err: fmt.Errorf("ping read DATE: %w", err)}
	}
	if resp.StatusCode != 111 {
		return PingResult{Err: fmt.Errorf("ping DATE unexpected status: %d %s", resp.StatusCode, resp.Message)}
	}

	serverTime, err := parseDateResponse(resp.Message)
	if err != nil {
		return PingResult{RTT: rtt, Err: err}
	}
	return PingResult{RTT: rtt, ServerTime: serverTime}
}

// TestProvider dials the given provider, performs greeting + authentication +
// DATE, and returns the result. It is completely independent of Client/pool.
func TestProvider(ctx context.Context, p Provider) PingResult {
	factory := p.Factory
	if factory == nil {
		host := p.Host
		tlsCfg := p.TLSConfig
		keepAlive := p.KeepAlive
		factory = func(ctx context.Context) (net.Conn, error) {
			return newNetConn(ctx, host, tlsCfg, keepAlive)
		}
	}
	return pingProvider(ctx, factory, p.Auth)
}

// resolveProviderName builds a unique name for a provider based on host and auth.
func resolveProviderName(p Provider, index int) string {
	if p.Host != "" {
		if p.Auth.Username != "" {
			return p.Host + "+" + p.Auth.Username
		}
		return p.Host
	}
	return fmt.Sprintf("provider-%d", index)
}

// startProviderGroup creates a providerGroup, pings the provider, and launches
// connection slot goroutines. The caller is responsible for storing the group.
func (c *Client) startProviderGroup(p Provider, index int) *providerGroup {
	inflight := p.Inflight
	if inflight <= 0 {
		inflight = 1
	}

	factory := p.Factory
	if factory == nil {
		host := p.Host
		tlsCfg := p.TLSConfig
		keepAlive := p.KeepAlive
		factory = func(ctx context.Context) (net.Conn, error) {
			return newNetConn(ctx, host, tlsCfg, keepAlive)
		}
	}

	name := resolveProviderName(p, index)
	gate := newConnGate(p.Connections, p.ThrottleRestore)
	gctx, gcancel := context.WithCancel(c.ctx)

	g := &providerGroup{
		name:      name,
		host:      p.Host,
		maxConns:  p.Connections,
		ctx:       gctx,
		reqCh:     make(chan *Request, p.Connections),
		prioCh:    make(chan *Request, p.Connections),
		hotReqCh:  make(chan *Request),
		hotPrioCh: make(chan *Request),
		gate:      gate,
		cancel:    gcancel,
	}

	// Ping with a short timeout so we don't block forever.
	pingCtx, pingCancel := context.WithTimeout(c.ctx, defaultHandshakeTimeout)
	g.stats.Ping = pingProvider(pingCtx, factory, p.Auth)
	pingCancel()

	for range p.Connections {
		c.wg.Add(1)
		go runConnSlot(gctx, g.reqCh, g.prioCh, g.hotReqCh, g.hotPrioCh, factory, inflight, p.Auth, p.IdleTimeout, gate, &g.stats, &c.wg)
	}

	return g
}

func NewClient(ctx context.Context, providers []Provider, opts ...ClientOption) (*Client, error) {
	if len(providers) == 0 {
		return nil, fmt.Errorf("nntp: at least one provider is required")
	}

	// Require at least one main (non-backup) provider.
	hasMain := false
	for _, p := range providers {
		if !p.Backup {
			hasMain = true
			break
		}
	}
	if !hasMain {
		return nil, fmt.Errorf("nntp: at least one non-backup provider is required")
	}

	// Validation only — no TCP connections are created here.
	for _, p := range providers {
		if p.Connections <= 0 {
			return nil, fmt.Errorf("nntp: provider connections must be > 0")
		}
		if p.Factory == nil && p.Host == "" {
			return nil, fmt.Errorf("nntp: provider must have Host or Factory")
		}
	}

	if ctx == nil {
		ctx = context.Background()
	}
	ctx, cancel := context.WithCancel(ctx)

	var cfg clientConfig
	for _, opt := range opts {
		opt(&cfg)
	}

	c := &Client{
		ctx:       ctx,
		cancel:    cancel,
		dispatch:  cfg.dispatch,
		startTime: time.Now(),
	}
	// Initialize empty slices.
	c.mainGroups.Store(&[]*providerGroup{})
	c.backupGroups.Store(&[]*providerGroup{})

	var mainGs, backupGs []*providerGroup
	for pi, p := range providers {
		g := c.startProviderGroup(p, pi)
		if p.Backup {
			backupGs = append(backupGs, g)
		} else {
			mainGs = append(mainGs, g)
		}
	}
	c.mainGroups.Store(&mainGs)
	c.backupGroups.Store(&backupGs)

	return c, nil
}

// Close cancels the client, stops all provider gates, and waits for all
// connection slots to stop. Slots manage their own TCP connection cleanup.
// Context cancellation (c.cancel) cascades to all group contexts, so closing
// reqCh is unnecessary and avoids a race with stale-snapshot senders.
func (c *Client) Close() error {
	c.cancel()
	for _, gs := range []*[]*providerGroup{c.mainGroups.Load(), c.backupGroups.Load()} {
		for _, g := range *gs {
			g.gate.stop()
		}
	}
	c.wg.Wait()
	return nil
}

func (c *Client) Send(ctx context.Context, payload []byte, bodyWriter io.Writer, onMeta ...func(YEncMeta)) <-chan Response {
	respCh := make(chan Response, 1)
	if ctx == nil {
		ctx = context.Background()
	}

	var metaFn func(YEncMeta)
	if len(onMeta) > 0 {
		metaFn = onMeta[0]
	}

	go c.sendWithRetry(ctx, payload, bodyWriter, metaFn, respCh)
	return respCh
}

// SendPriority is like Send but enqueues the request on the priority channel,
// so idle connections will pick it up before normal requests.
func (c *Client) SendPriority(ctx context.Context, payload []byte, bodyWriter io.Writer, onMeta ...func(YEncMeta)) <-chan Response {
	respCh := make(chan Response, 1)
	if ctx == nil {
		ctx = context.Background()
	}

	var metaFn func(YEncMeta)
	if len(onMeta) > 0 {
		metaFn = onMeta[0]
	}

	go c.doSendWithRetry(ctx, payload, bodyWriter, metaFn, respCh, true)
	return respCh
}

// hostSkipped reports whether host is already in the skip list.
// Empty hosts (Factory-based providers) are never skipped.
func hostSkipped(host string, skipHosts *[4]string, count int) bool {
	if host == "" || count == 0 {
		return false
	}
	for i := range count {
		if skipHosts[i] == host {
			return true
		}
	}
	return false
}

func (c *Client) sendWithRetry(ctx context.Context, payload []byte, bodyWriter io.Writer, onMeta func(YEncMeta), respCh chan Response) {
	c.doSendWithRetry(ctx, payload, bodyWriter, onMeta, respCh, false)
}

func (c *Client) doSendWithRetry(ctx context.Context, payload []byte, bodyWriter io.Writer, onMeta func(YEncMeta), respCh chan Response, priority bool) {
	defer close(respCh)

	tryGroup := func(g *providerGroup) (resp Response, ok bool, done bool) {
		innerCh := make(chan Response, 1)
		req := &Request{
			Ctx:        ctx,
			Payload:    payload,
			RespCh:     innerCh,
			BodyWriter: bodyWriter,
			OnMeta:     onMeta,
		}

		var hotCh chan *Request
		var coldCh chan *Request
		if priority {
			hotCh = g.hotPrioCh
			coldCh = g.prioCh
		} else {
			hotCh = g.hotReqCh
			coldCh = g.reqCh
		}

		// Try hot channel first (non-blocking): succeeds only if a
		// hot connection is already waiting with inflight capacity.
		select {
		case hotCh <- req:
		default:
			// No hot connection available — send to cold channel.
			if c.dispatch == DispatchFIFO {
				// FIFO: non-blocking so full channels overflow to next provider.
				select {
				case <-c.ctx.Done():
					return Response{}, false, true
				case <-ctx.Done():
					return Response{}, false, true
				case <-g.ctx.Done():
					return Response{}, false, false
				case coldCh <- req:
				default:
					return Response{}, false, false
				}
			} else {
				select {
				case <-c.ctx.Done():
					return Response{}, false, true
				case <-ctx.Done():
					return Response{}, false, true
				case <-g.ctx.Done():
					return Response{}, false, false
				case coldCh <- req:
				}
			}
		}

		resp, ok = <-innerCh
		return resp, ok, false
	}

	var lastResp Response
	hasResp := false
	var lastErr error

	// Track hosts that returned 430 so we can skip other providers on
	// the same server (different credentials won't help).
	var skipHosts [4]string
	skipCount := 0

	// 1. Try all main providers.
	mains := *c.mainGroups.Load()
	n := len(mains)
	if n == 0 {
		respCh <- Response{Err: errors.New("nntp: no main providers")}
		return
	}

	// Pick start index based on dispatch strategy.
	var start int
	switch c.dispatch {
	case DispatchFIFO:
		// Priority order: first provider with available capacity,
		// falling back to provider 0 if all are saturated.
		for i, g := range mains {
			if g.gate.available.Load() > 0 {
				start = i
				break
			}
		}
	default: // DispatchRoundRobin
		// Dynamic weighted round-robin: each provider's weight equals
		// its available capacity (allowed - held).
		var cumWeights [8]int // stack-allocated; covers up to 8 providers
		totalW := 0
		for i, g := range mains {
			avail := max(1, int(g.gate.available.Load()))
			totalW += avail
			cumWeights[i] = totalW
		}
		slot := int(c.nextIdx.Add(1) % uint64(totalW))
		start = sort.SearchInts(cumWeights[:n], slot+1)
	}

	for attempt := range n {
		idx := (start + attempt) % n
		g := mains[idx]
		if hostSkipped(g.host, &skipHosts, skipCount) {
			continue
		}
		resp, ok, cancelled := tryGroup(g)
		if cancelled {
			err := ctx.Err()
			if err == nil {
				err = c.ctx.Err()
			}
			respCh <- Response{Err: err}
			return
		}
		if !ok {
			// Connection died — try next provider.
			continue
		}
		if resp.Err != nil {
			lastErr = fmt.Errorf("%s: %w", g.name, resp.Err)
			continue
		}
		if resp.StatusCode == 430 {
			c.nextIdx.Add(1) // bias next request away from this provider
			if g.host != "" && skipCount < len(skipHosts) {
				skipHosts[skipCount] = g.host
				skipCount++
			}
			lastResp = resp
			hasResp = true
			continue
		}
		// Success.
		respCh <- resp
		return
	}

	// 2. All main providers returned 430 (or died) — try backup providers in order.
	backups := *c.backupGroups.Load()
	for i := range backups {
		g := backups[i]
		if hostSkipped(g.host, &skipHosts, skipCount) {
			continue
		}
		resp, ok, cancelled := tryGroup(g)
		if cancelled {
			err := ctx.Err()
			if err == nil {
				err = c.ctx.Err()
			}
			respCh <- Response{Err: err}
			return
		}
		if !ok {
			continue
		}
		if resp.Err != nil {
			lastErr = fmt.Errorf("%s: %w", g.name, resp.Err)
			continue
		}
		// Deliver whatever backup returns (including 430).
		respCh <- resp
		return
	}

	// 3. All providers exhausted — deliver the last 430, the last error, or a fallback.
	if hasResp {
		respCh <- lastResp
	} else if lastErr != nil {
		respCh <- Response{Err: lastErr}
	} else {
		respCh <- Response{Err: errors.New("nntp: all providers exhausted")}
	}
}

// NumProviders returns the number of configured providers (main + backup).
func (c *Client) NumProviders() int {
	return len(*c.mainGroups.Load()) + len(*c.backupGroups.Load())
}

// Stats returns a snapshot of per-provider and aggregate metrics.
func (c *Client) Stats() ClientStats {
	elapsed := time.Since(c.startTime)
	secs := elapsed.Seconds()
	var cs ClientStats
	cs.Elapsed = elapsed
	var totalBytes int64
	for _, groups := range [...]*[]*providerGroup{c.mainGroups.Load(), c.backupGroups.Load()} {
		for _, g := range *groups {
			consumed := g.stats.BytesConsumed.Load()
			totalBytes += consumed
			maxSlots, running := g.gate.snapshot()
			ps := ProviderStats{
				Name:              g.name,
				Missing:           g.stats.Missing.Load(),
				Errors:            g.stats.Errors.Load(),
				ActiveConnections: running,
				MaxConnections:    maxSlots,
				Ping:              g.stats.Ping,
			}
			if secs > 0 {
				ps.AvgSpeed = float64(consumed) / secs
			}
			cs.Providers = append(cs.Providers, ps)
		}
	}
	if secs > 0 {
		cs.AvgSpeed = float64(totalBytes) / secs
	}
	return cs
}

// AddProvider validates, pings, and registers a new provider at runtime.
// Ping failures are recorded in the group's stats but do not cause an error return.
func (c *Client) AddProvider(p Provider) error {
	if p.Connections <= 0 {
		return fmt.Errorf("nntp: provider connections must be > 0")
	}
	if p.Factory == nil && p.Host == "" {
		return fmt.Errorf("nntp: provider must have Host or Factory")
	}

	idx := int(c.providerIdx.Add(1))
	name := resolveProviderName(p, idx)

	// Check for duplicate name.
	for _, gs := range [...]*[]*providerGroup{c.mainGroups.Load(), c.backupGroups.Load()} {
		for _, g := range *gs {
			if g.name == name {
				return fmt.Errorf("nntp: provider %q already exists", name)
			}
		}
	}

	g := c.startProviderGroup(p, idx)

	// Copy-on-write append.
	if p.Backup {
		old := c.backupGroups.Load()
		updated := make([]*providerGroup, len(*old)+1)
		copy(updated, *old)
		updated[len(*old)] = g
		c.backupGroups.Store(&updated)
	} else {
		old := c.mainGroups.Load()
		updated := make([]*providerGroup, len(*old)+1)
		copy(updated, *old)
		updated[len(*old)] = g
		c.mainGroups.Store(&updated)
	}
	return nil
}

// RemoveProvider stops and removes a provider by name.
// Goroutines wind down asynchronously; Client.Close still waits for all via c.wg.
func (c *Client) RemoveProvider(name string) error {
	for _, pair := range [...]struct {
		ptr *atomic.Pointer[[]*providerGroup]
	}{
		{&c.mainGroups},
		{&c.backupGroups},
	} {
		old := pair.ptr.Load()
		for i, g := range *old {
			if g.name != name {
				continue
			}
			// Found it — cancel context and stop gate. Context cancellation
			// is sufficient; runConnSlot goroutines exit via ctx.Done() and
			// tryGroup detects removal via g.ctx.Done().
			g.cancel()
			g.gate.stop()

			// Copy-on-write removal.
			updated := make([]*providerGroup, 0, len(*old)-1)
			updated = append(updated, (*old)[:i]...)
			updated = append(updated, (*old)[i+1:]...)
			pair.ptr.Store(&updated)
			return nil
		}
	}
	return fmt.Errorf("nntp: provider %q not found", name)
}
