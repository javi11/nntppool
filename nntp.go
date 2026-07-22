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

// isConnectionDeathError reports whether err indicates the underlying
// connection failed at the transport layer (as opposed to a protocol-level
// response like 430/502, which is delivered via StatusCode). These are
// retryable on a fresh connection: an established connection that goes stale
// surfaces ErrConnectionDied via failOutstanding, while a connection that dies
// on its bootstrap request surfaces the raw IO error (EOF, closed pipe, reset,
// timeout). Both mean "this socket is gone — open a new one."
func isConnectionDeathError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, ErrConnectionDied) ||
		errors.Is(err, io.EOF) ||
		errors.Is(err, io.ErrUnexpectedEOF) ||
		errors.Is(err, io.ErrClosedPipe) ||
		errors.Is(err, net.ErrClosed) {
		return true
	}
	var netErr net.Error
	return errors.As(err, &netErr)
}

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

	// maxConnDiedRetries bounds same-provider retries when a pooled connection
	// dies mid-request (typically a stale socket the server already closed).
	// The dead connection has drained by the time the error surfaces, so the
	// retry uses a fresh connection on the same provider.
	maxConnDiedRetries = 2

	// minAttemptTimeout is the floor (and default) for the per-attempt timeout
	// that bounds dispatch + time-to-first-response-byte. Once response bytes
	// start flowing, the rolling stall timeout takes over instead.
	minAttemptTimeout = 2 * time.Second

	// maxAttemptTimeout caps the adaptive per-attempt timeout derived from a
	// provider's measured round-trip time.
	maxAttemptTimeout = 10 * time.Second

	// defaultStallTimeout is the rolling progress deadline applied to a body
	// transfer once bytes are flowing: if no further bytes arrive within this
	// window the connection is considered stalled and torn down. A healthy but
	// slow transfer keeps extending the deadline and never trips it.
	defaultStallTimeout = 8 * time.Second

	// stallDeadlineQuantum coarsens stall-deadline updates so the read path
	// issues at most one SetReadDeadline syscall per quantum instead of one per
	// read.
	stallDeadlineQuantum = 250 * time.Millisecond
)

// Attempt lifecycle states, coordinating the race between tryGroup's attempt
// timer and the reader observing the first response byte. The CAS handshake
// guarantees exactly one of them "wins": if the reader commits first the
// attempt is never abandoned mid-stream; if the timer fires first the attempt
// fails over and the reader drops the (cancelled) request.
const (
	attemptPending   int32 = iota // no response byte seen yet
	attemptCommitted              // reader saw the first response byte
	attemptAbandoned              // tryGroup timed out before any byte arrived
)

type greetingError struct {
	StatusCode int
	Message    string
}

func (e *greetingError) Error() string {
	return fmt.Sprintf("nntp greeting: %d %s", e.StatusCode, e.Message)
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

	// PayloadBody is an optional reader streamed to the connection after Payload.
	// Used by POST to stream article content without buffering in memory.
	PayloadBody io.Reader

	// PostMode signals readerLoop to expect two NNTP responses (340 + 240/441).
	PostMode bool

	// postReadyCh is set by writeLoop for PostMode requests. The readerLoop
	// sends nil after reading 340 (proceed to write body) or a non-nil error
	// otherwise (e.g. 440 posting not allowed). Buffered with capacity 1.
	postReadyCh chan error

	// attemptDeadline bounds dispatch + time-to-first-response-byte for this
	// attempt. Zero means no such bound (e.g. POST and keepalive requests).
	// Once the reader sees the first byte the attempt is committed and this
	// deadline no longer applies — the connection's rolling stall timeout does.
	attemptDeadline time.Time

	// attemptState is one of attemptPending/attemptCommitted/attemptAbandoned.
	// The reader CASes pending→committed on the first response byte; tryGroup's
	// timer CASes pending→abandoned on expiry. Zero value is attemptPending.
	attemptState atomic.Int32

	// sentAt is the Unix-nanosecond timestamp at which the payload was handed
	// to the connection, used to measure time-to-first-byte. 0 = unset (the
	// request is not measured, e.g. POST/keepalive).
	sentAt atomic.Int64

	// heldBody is set by writeLoop when this (body-bearing) request acquired a
	// bodySem slot, so readerLoop releases exactly the slots that were taken.
	// Bodyless STAT requests never acquire bodySem and leave this false.
	heldBody bool
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

	reqCh     <-chan *Request
	prioCh    <-chan *Request // priority channel; nil for standalone connections
	hotReqCh  <-chan *Request // unbuffered; set by runConnSlot before Run()
	hotPrioCh <-chan *Request // unbuffered; set by runConnSlot before Run()
	pending   chan *Request

	// inflightSem bounds the total pipeline depth (cap = StatInflight, i.e.
	// max(Inflight, StatInflight)). bodySem additionally bounds concurrent
	// body-bearing commands (cap = Inflight) so raising the STAT pipeline depth
	// never increases the number of BODY responses buffered/streamed at once.
	// Bodyless STAT commands acquire only inflightSem and so pipeline to the
	// deeper StatInflight depth.
	inflightSem chan struct{}
	bodySem     chan struct{}

	rb readBuffer

	Greeting NNTPResponse

	firstReq          *Request      // bootstrap request from connection slot
	idleTimeout       time.Duration // 0 = no idle timeout
	stallTimeout      time.Duration // rolling body-progress deadline; 0 = disabled
	keepaliveInterval time.Duration // 0 = no keepalive
	keepaliveCommand  string        // NNTP command for keepalive probe (e.g. "DATE")
	providerName      string        // set by runConnSlot; used for error context
	userAgent         string

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

func newNNTPConnectionFromConn(ctx context.Context, conn net.Conn, inflightLimit int, reqCh <-chan *Request, prioCh <-chan *Request, auth Auth, userAgent string, sharedBuf *readBuffer, stats *providerStats) (*NNTPConnection, error) {
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
		// Default bodySem to the full pipeline depth (no separate BODY bound);
		// runConnSlot overrides this to Provider.Inflight when a deeper STAT
		// pipeline is configured. Standalone connections keep them equal.
		bodySem:   make(chan struct{}, inflightLimit),
		rb:        rb,
		stats:     stats,
		done:      make(chan struct{}),
		userAgent: userAgent,
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
			return nil, fmt.Errorf("nntp auth: password required when username is set")
		}

		if err := c.auth(auth); err != nil {
			return nil, err
		}
	}

	return c, nil
}

func NewNNTPConnection(ctx context.Context, addr string, tlsConfig *tls.Config, inflightLimit int, reqCh <-chan *Request, auth Auth, userAgent string) (*NNTPConnection, error) {
	conn, err := newNetConn(ctx, addr, tlsConfig, 0)
	if err != nil {
		return nil, err
	}

	c, err := newNNTPConnectionFromConn(ctx, conn, inflightLimit, reqCh, nil, auth, userAgent, nil, nil)
	if err != nil {
		_ = conn.Close()
		return nil, err
	}
	return c, nil
}

func (c *NNTPConnection) auth(auth Auth) error {
	// AUTHINFO USER
	if _, err := fmt.Fprintf(c.conn, "AUTHINFO USER %s\r\n", auth.Username); err != nil {
		return fmt.Errorf("nntp auth: AUTHINFO USER: %w", err)
	}
	resp, err := c.readOneResponse(io.Discard)
	if err != nil {
		return fmt.Errorf("nntp auth: AUTHINFO USER: %w", err)
	}

	switch resp.StatusCode {
	case 281:
		return nil // authenticated
	case 381:
		// need pass
	default:
		return fmt.Errorf("nntp auth: unexpected response to AUTHINFO USER: %s", resp.Message)
	}

	// AUTHINFO PASS
	if _, err := fmt.Fprintf(c.conn, "AUTHINFO PASS %s\r\n", auth.Password); err != nil {
		return fmt.Errorf("nntp auth: AUTHINFO PASS: %w", err)
	}
	resp, err = c.readOneResponse(io.Discard)
	if err != nil {
		return fmt.Errorf("nntp auth: AUTHINFO PASS: %w", err)
	}
	if resp.StatusCode != 281 {
		return fmt.Errorf("nntp auth: unexpected response to AUTHINFO PASS: %s", resp.Message)
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

// keepaliveProbeTimeout bounds a keepalive probe's round trip. The probe is a
// trivial command (DATE/HELP), so the connection's stall timeout is ample as a
// time-to-first-byte bound; with stall disabled, a fixed 30s fallback still
// guarantees the probe can never park the connection forever.
func (c *NNTPConnection) keepaliveProbeTimeout() time.Duration {
	if c.stallTimeout > 0 {
		return c.stallTimeout
	}
	return 30 * time.Second
}

// keepaliveExpectedCode returns the expected NNTP status code for the given
// keepalive command: DATE→111, HELP→100, CAPABILITIES→101, default→111.
func keepaliveExpectedCode(cmd string) int {
	switch cmd {
	case "HELP":
		return 100
	case "CAPABILITIES":
		return 101
	default:
		return 111
	}
}

// statCmdPrefix identifies STAT commands, the only bodyless request the pool
// issues through the normal write path (keepalive DATE has its own path). STAT
// has a single-line reply and no payload, so it may pipeline to the deeper
// StatInflight depth without acquiring a bodySem slot.
var statCmdPrefix = []byte("STAT ")

// isCheapCommand reports whether payload is a bodyless command that should
// bypass the BODY concurrency bound (bodySem) and pipeline to the full
// inflightSem (StatInflight) depth.
func isCheapCommand(payload []byte) bool {
	return bytes.HasPrefix(payload, statCmdPrefix)
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
		connErr := error(ErrConnectionDied)
		if c.providerName != "" {
			connErr = fmt.Errorf("%s: %w", c.providerName, ErrConnectionDied)
		}
		for {
			select {
			case req := <-c.pending:
				if req == nil {
					continue
				}
				failRequest(req.RespCh, connErr)
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
func runConnSlot(ctx context.Context, reqCh <-chan *Request, prioCh <-chan *Request, hotReqCh <-chan *Request, hotPrioCh <-chan *Request, factory ConnFactory, inflight int, statInflight int, auth Auth, userAgent string, idleTimeout time.Duration, stallTimeout time.Duration, keepaliveInterval time.Duration, keepaliveCommand string, gate *connGate, stats *providerStats, providerName string, wg *sync.WaitGroup) {
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
			failRequest(firstReq.RespCh, fmt.Errorf("%s: %w", providerName, err))
			// Backoff before retrying to avoid thrashing.
			select {
			case <-time.After(connFailureBackoff):
			case <-ctx.Done():
				return
			}
			continue
		}

		// Size the pipeline (inflightSem/pending) to statInflight so bodyless
		// STAT commands can pipeline deep; bodySem is overridden below to the
		// (smaller) Inflight so concurrent bodies stay bounded.
		nc, err := newNNTPConnectionFromConn(ctx, conn, statInflight, reqCh, prioCh, auth, userAgent, &sharedBuf, stats)
		if err != nil {
			_ = conn.Close()
			failRequest(firstReq.RespCh, fmt.Errorf("%s: %w", providerName, err))

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
		// Bound concurrent bodies to Inflight while the pipeline (inflightSem)
		// allows STAT to reach statInflight. When statInflight == inflight this
		// is identical to the default (both caps equal).
		nc.bodySem = make(chan struct{}, inflight)
		nc.firstReq = firstReq
		nc.idleTimeout = idleTimeout
		nc.stallTimeout = stallTimeout
		nc.providerName = providerName
		nc.hotReqCh = hotReqCh
		nc.hotPrioCh = hotPrioCh
		nc.keepaliveInterval = keepaliveInterval
		nc.keepaliveCommand = keepaliveCommand
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

		// Body-bearing requests additionally take a bodySem slot so concurrent
		// bodies stay bounded by Inflight even when the pipeline (inflightSem)
		// is deeper for STAT. Bodyless STAT skips this and pipelines deep.
		if !isCheapCommand(req.Payload) {
			select {
			case c.bodySem <- struct{}{}:
				req.heldBody = true
			case <-req.Ctx.Done():
				<-c.inflightSem
				failRequest(req.RespCh, req.Ctx.Err())
				goto mainLoop // connection still good
			case <-c.ctx.Done():
				<-c.inflightSem
				failRequest(req.RespCh, c.ctx.Err())
				return
			}
		}

		dl, hasDL := req.writeDeadline()
		setWriteDeadline(dl, hasDL)

		if _, err := bw.Write(req.Payload); err != nil {
			<-c.inflightSem
			failRequest(req.RespCh, err)
			_ = c.conn.Close()
			c.failOutstanding()
			return
		}
		if req.PostMode {
			// Two-phase POST: flush "POST\r\n" immediately so the server can
			// respond with 340/440 before we send the article body.
			if err := bw.Flush(); err != nil {
				<-c.inflightSem
				failRequest(req.RespCh, err)
				_ = c.conn.Close()
				c.failOutstanding()
				return
			}
			req.postReadyCh = make(chan error, 1)
			c.pending <- req
			// Block here — no other request can be written while we wait.
			select {
			case postErr := <-req.postReadyCh:
				if postErr != nil {
					// 440 or error: drain body to unblock the pipe-writer goroutine.
					if req.PayloadBody != nil {
						_, _ = io.Copy(io.Discard, req.PayloadBody)
					}
					goto mainLoop
				}
			case <-c.ctx.Done():
				if req.PayloadBody != nil {
					_, _ = io.Copy(io.Discard, req.PayloadBody)
				}
				return
			}
			// 340 received: send the article body.
			if req.PayloadBody != nil {
				if _, err := io.Copy(bw, req.PayloadBody); err != nil {
					_ = c.conn.Close()
					c.failOutstanding()
					return
				}
			}
		} else {
			if req.PayloadBody != nil {
				if _, err := io.Copy(bw, req.PayloadBody); err != nil {
					<-c.inflightSem
					failRequest(req.RespCh, err)
					_ = c.conn.Close()
					c.failOutstanding()
					return
				}
			}
			req.sentAt.Store(time.Now().UnixNano())
			c.pending <- req
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

	// Set up keepalive timer (nil if no keepalive configured).
	var keepaliveCh <-chan time.Time
	if c.keepaliveInterval > 0 {
		keepaliveCh = time.After(c.keepaliveInterval)
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
		var didKeepalive bool
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
					case <-keepaliveCh:
						didKeepalive = true
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
			case <-keepaliveCh:
				didKeepalive = true
			}
		}

		// Keepalive probe: send a lightweight command through the normal pipeline
		// so readerLoop can match the response in FIFO order.
		// inflightSem is already held; readerLoop releases it at line 1008.
		if didKeepalive {
			keepaliveCh = time.After(c.keepaliveInterval) // reset regardless of outcome
			kaCh := make(chan Response, 1)
			// The probe MUST carry an attempt deadline: with a background context
			// and no deadline, a server that accepts the command but never
			// answers (half-open connection, stale provider session) leaves the
			// reader in a deadline-less Read forever — the connection parks
			// "busy" holding its slot and its provider session, and the feature
			// meant to detect dead connections becomes the thing that wedges
			// them. On expiry the reader surfaces a timeout, closes the
			// connection, and the pool replaces it — exactly what a keepalive
			// is for.
			kaReq := &Request{
				Payload:         []byte(c.keepaliveCommand + "\r\n"),
				RespCh:          kaCh,
				Ctx:             context.Background(),
				attemptDeadline: time.Now().Add(c.keepaliveProbeTimeout()),
			}
			if _, err := bw.Write(kaReq.Payload); err != nil {
				_ = c.conn.Close()
				c.failOutstanding()
				return
			}
			if err := bw.Flush(); err != nil {
				_ = c.conn.Close()
				c.failOutstanding()
				return
			}
			c.pending <- kaReq
			select {
			case resp := <-kaCh:
				if resp.Err != nil || resp.StatusCode != keepaliveExpectedCode(c.keepaliveCommand) {
					_ = c.conn.Close()
					c.failOutstanding()
					return
				}
			case <-c.ctx.Done():
				return
			}
			continue
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

		// Body-bearing requests additionally take a bodySem slot so concurrent
		// bodies stay bounded by Inflight even when the pipeline (inflightSem)
		// is deeper for STAT. Bodyless STAT skips this and pipelines deep.
		if !isCheapCommand(req.Payload) {
			select {
			case c.bodySem <- struct{}{}:
				req.heldBody = true
			case <-req.Ctx.Done():
				<-c.inflightSem
				failRequest(req.RespCh, req.Ctx.Err())
				continue
			case <-c.ctx.Done():
				<-c.inflightSem
				return
			}
		}

		// per-request write deadline (cached to avoid redundant syscalls)
		dl, hasDL := req.writeDeadline()
		setWriteDeadline(dl, hasDL)

		// pipeline write (buffered; flushed at top of loop before blocking)
		if _, err := bw.Write(req.Payload); err != nil {
			<-c.inflightSem
			failRequest(req.RespCh, err)
			_ = c.conn.Close()
			c.failOutstanding()
			return
		}
		if req.PostMode {
			// Two-phase POST: flush "POST\r\n" immediately so the server can
			// respond with 340/440 before we send the article body. Blocking
			// here also prevents pipelining other requests during POST.
			if err := bw.Flush(); err != nil {
				<-c.inflightSem
				failRequest(req.RespCh, err)
				_ = c.conn.Close()
				c.failOutstanding()
				return
			}
			req.postReadyCh = make(chan error, 1)
			c.pending <- req
			select {
			case postErr := <-req.postReadyCh:
				if postErr != nil {
					// 440 or error: drain body to unblock the pipe-writer goroutine.
					if req.PayloadBody != nil {
						_, _ = io.Copy(io.Discard, req.PayloadBody)
					}
					continue
				}
			case <-c.ctx.Done():
				if req.PayloadBody != nil {
					_, _ = io.Copy(io.Discard, req.PayloadBody)
				}
				return
			}
			// 340 received: send the article body.
			if req.PayloadBody != nil {
				if _, err := io.Copy(bw, req.PayloadBody); err != nil {
					_ = c.conn.Close()
					c.failOutstanding()
					return
				}
			}
		} else {
			if req.PayloadBody != nil {
				if _, err := io.Copy(bw, req.PayloadBody); err != nil {
					<-c.inflightSem
					failRequest(req.RespCh, err)
					_ = c.conn.Close()
					c.failOutstanding()
					return
				}
			}
			// track FIFO ordering (after writes succeed to avoid send on closed channel)
			req.sentAt.Store(time.Now().UnixNano())
			c.pending <- req
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

		// Progress-aware deadline: before the first response byte we honor the
		// attempt deadline (dispatch + TTFB bound); once bytes flow we switch to
		// a rolling stall deadline that the wire progress keeps extending, so a
		// healthy-but-slow body never trips it. The caller's own ctx deadline
		// always still applies as an upper bound.
		stall := c.stallTimeout
		lastBytes := 0
		var stallDeadline time.Time
		var firstByteAt time.Time
		respStart := time.Now()
		err := c.rb.feedUntilDone(c.conn, &decoder, outRef, func(wireBytes int) (time.Time, bool) {
			if deliver {
				select {
				case <-req.Ctx.Done():
					deliver = false
					outRef.w = io.Discard
				default:
				}
			}
			if wireBytes > lastBytes {
				lastBytes = wireBytes
				if firstByteAt.IsZero() {
					firstByteAt = time.Now()
				}
				req.attemptState.CompareAndSwap(attemptPending, attemptCommitted)
				if stall > 0 {
					if dl := time.Now().Add(stall); dl.Sub(stallDeadline) >= stallDeadlineQuantum {
						stallDeadline = dl
					}
				}
			}
			parentDL, hasParent := req.Ctx.Deadline()
			switch {
			case lastBytes == 0 && !req.attemptDeadline.IsZero():
				return minDeadline(req.attemptDeadline, parentDL, hasParent)
			case lastBytes > 0 && stall > 0:
				return minDeadline(stallDeadline, parentDL, hasParent)
			default:
				return parentDL, hasParent
			}
		})
		if err != nil {
			if c.providerName != "" {
				resp.Err = fmt.Errorf("%s: %w", c.providerName, err)
			} else {
				resp.Err = err
			}
		}

		resp.StatusCode = decoder.StatusCode
		resp.Status = decoder.Message
		resp.Lines = decoder.Lines
		resp.Meta = decoder

		// Two-phase POST: coordinate with writeLoop via postReadyCh.
		if req.PostMode {
			if decoder.StatusCode == 340 {
				// Signal writeLoop to send the article body, then read the
				// final response (240/441) once the server acknowledges it.
				if req.postReadyCh != nil {
					req.postReadyCh <- nil
				}
				decoder2 := NNTPResponse{}
				err2 := c.rb.feedUntilDone(c.conn, &decoder2, io.Discard, func(wireBytes int) (time.Time, bool) {
					if deliver {
						select {
						case <-req.Ctx.Done():
							deliver = false
						default:
						}
					}
					return req.Ctx.Deadline()
				})
				if err2 != nil {
					if c.providerName != "" {
						resp.Err = fmt.Errorf("%s: %w", c.providerName, err2)
					} else {
						resp.Err = err2
					}
				}
				resp.StatusCode = decoder2.StatusCode
				resp.Status = decoder2.Message
				resp.Meta = decoder2
			} else if req.postReadyCh != nil {
				// 440 or other rejection: tell writeLoop not to send the body.
				req.postReadyCh <- fmt.Errorf("post rejected: %d %s", decoder.StatusCode, decoder.Message)
			}
		}

		if c.stats != nil {
			n := int64(decoder.BytesConsumed)
			c.stats.BytesConsumed.Add(n)
			if c.stats.quotaBytes > 0 {
				if c.stats.quotaUsed.Add(n) >= c.stats.quotaBytes {
					c.stats.quotaExceeded.Store(true)
				}
			}
			if resp.Err != nil {
				c.stats.Errors.Add(1)
			} else if decoder.StatusCode == 430 || decoder.StatusCode == 423 {
				c.stats.Missing.Add(1)
			} else if decoder.StatusCode < 200 || decoder.StatusCode >= 400 {
				c.stats.Errors.Add(1)
			} else {
				// Successful transfer: feed the TTFB and throughput EWMAs that
				// drive the adaptive attempt timeout and speed-aware dispatch.
				// firstByteAt is unset when the whole response arrived in a
				// single read; fall back to the read start. recordTTFB/Speed
				// ignore non-positive and sub-floor samples respectively.
				fb := firstByteAt
				if fb.IsZero() {
					fb = respStart
				}
				if sentAt := req.sentAt.Load(); sentAt != 0 {
					recordTTFB(c.stats, fb.Sub(time.Unix(0, sentAt)))
				}
				recordSpeed(c.stats, n, time.Since(fb))
			}
		}

		if deliver {
			// Best effort: don't block forever if the receiver abandoned the channel.
			select {
			case req.RespCh <- resp:
			default:
			}
		}
		safeClose(req.RespCh)

		// release inflight slot (and the body slot, if this request took one)
		<-c.inflightSem
		if req.heldBody {
			<-c.bodySem
		}

		// If we hit a timeout, cancellation-related network error, or protocol
		// desync, close the connection so the pool replaces it with a fresh one.
		if resp.Err != nil {
			var ne net.Error
			if errors.As(resp.Err, &ne) && ne.Timeout() {
				_ = c.conn.Close()
				c.failOutstanding()
				return
			}
			if errors.Is(resp.Err, ErrProtocolDesync) {
				_ = c.conn.Close()
				c.failOutstanding()
				return
			}
		}

		// 502 "service unavailable" mid-session: close the connection so
		// all pending requests fail fast instead of waiting in the pipeline.
		if decoder.StatusCode == 502 {
			_ = c.conn.Close()
			c.failOutstanding()
			return
		}
	}
}

// readOneResponse reads a complete NNTP response from the stream.
// Any unread bytes remain buffered in c.rbuf[c.rstart:c.rend] for subsequent reads.
func (c *NNTPConnection) readOneResponse(out io.Writer) (NNTPResponse, error) {
	resp := NNTPResponse{}
	if err := c.rb.feedUntilDone(c.conn, &resp, out, func(int) (time.Time, bool) { return time.Time{}, false }); err != nil {
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
	dispatch      DispatchStrategy
	statProbeOff  bool
	speedAwareOff bool
}

// WithDispatchStrategy sets the request distribution strategy for main providers.
// The default is DispatchRoundRobin.
func WithDispatchStrategy(s DispatchStrategy) ClientOption {
	return func(cfg *clientConfig) { cfg.dispatch = s }
}

// WithStatProbe enables or disables parallel STAT probing on 430 failover.
// When enabled (the default), after the first 430 response the remaining
// providers are probed concurrently with lightweight STAT commands; only the
// first provider that confirms article existence (223) receives the full
// request. This reduces "article missing on N providers" latency from
// sum-of-RTTs to max-of-RTTs.
func WithStatProbe(enabled bool) ClientOption {
	return func(cfg *clientConfig) { cfg.statProbeOff = !enabled }
}

// WithSpeedAwareDispatch enables or disables speed-aware weighting of the
// DispatchRoundRobin strategy. When enabled (the default), each provider's
// round-robin weight is scaled by its observed throughput so faster providers
// receive proportionally more traffic; available connection capacity still
// governs the base weight. Has no effect under DispatchFIFO.
func WithSpeedAwareDispatch(enabled bool) ClientOption {
	return func(cfg *clientConfig) { cfg.speedAwareOff = !enabled }
}

// Provider describes a single NNTP server with its own credentials and connection count.
type Provider struct {
	Host            string
	TLSConfig       *tls.Config
	Auth            Auth
	Connections     int
	Inflight        int           // 0 defaults to 1; max concurrent BODY (and other body-bearing) commands per connection
	StatInflight    int           // 0 defaults to Inflight; deeper pipeline depth for bodyless STAT commands. Because STAT carries no payload, many can be in flight per connection at negligible memory cost, amortising round-trips. Set higher than Inflight (e.g. 50-100) for STAT-heavy workloads without inflating BODY memory.
	Factory         ConnFactory   // overrides Host/TLSConfig when set
	Backup          bool          // if true, only used when main providers return 430
	StorageGroup    string        // optional label for providers sharing upstream storage (same backbone); a 430 from one skips the rest in the group for that request
	SkipPing        bool          // if true, skip the DATE ping on startup (for providers that don't support DATE)
	IdleTimeout     time.Duration // 0 means no idle disconnect
	ThrottleRestore time.Duration // 0 defaults to 30s
	KeepAlive       time.Duration // TCP keep-alive interval; 0 defaults to 30s; negative disables
	ReconnectDelay  time.Duration // 0 disables auto-reconnect after 502; when set, re-adds provider after this delay

	// AttemptTimeout bounds dispatch plus time-to-first-response-byte for each
	// attempt against this provider (it does NOT bound the body transfer, which
	// is governed by StallTimeout). 0 selects an adaptive value derived from the
	// provider's measured RTT, clamped to [2s, 10s]. Set explicitly to override.
	AttemptTimeout time.Duration

	// StallTimeout is the rolling progress deadline for a body transfer: once
	// bytes are flowing, the read deadline is extended by StallTimeout on each
	// chunk of progress, so a slow-but-healthy download never times out while a
	// truly stalled one is torn down. 0 defaults to 8s; negative disables it
	// (only the caller's context deadline applies).
	StallTimeout time.Duration

	// KeepaliveInterval, if non-zero, sends a lightweight NNTP command
	// periodically when the connection is idle, to detect zombie connections
	// before a real request arrives. Recommended: 30s–60s.
	// Disabled when SkipPing is true and KeepaliveCommand is empty.
	KeepaliveInterval time.Duration

	// KeepaliveCommand is the NNTP command sent as a keepalive probe.
	// Defaults to "DATE" (response 111). Use "HELP" (response 100) or
	// "CAPABILITIES" (response 101) for providers that do not support DATE.
	// Ignored when KeepaliveInterval is 0.
	KeepaliveCommand string

	// UserAgent identifies this client to the NNTP server. Empty string disables it.
	UserAgent string

	// QuotaBytes is the maximum number of bytes that may be downloaded from this
	// provider per QuotaPeriod. 0 means unlimited.
	QuotaBytes int64

	// QuotaPeriod is the rolling window after which the quota counter resets.
	// 0 means the quota never resets (lifetime cap).
	// Typical value: 30 * 24 * time.Hour  (≈ monthly)
	QuotaPeriod time.Duration

	// QuotaUsed is the number of bytes already consumed at startup.
	// Set this on restart to restore quota state from a previous run.
	// Read the current value from [ProviderStats.QuotaUsed] before shutting down.
	QuotaUsed int64

	// QuotaResetAt, if non-zero, overrides the quota period reset deadline on startup.
	// Set this on restart to restore the reset deadline from a previous run.
	// Read the current value from [ProviderStats.QuotaResetAt] before shutting down.
	// Ignored when QuotaPeriod is 0 or the time is in the past.
	QuotaResetAt time.Time
}

type providerGroup struct {
	name      string
	host      string // raw Provider.Host; empty for Factory-based providers
	skipID    string // Provider.StorageGroup when set, else host; identity used for 430 skipping
	maxConns  int
	ctx       context.Context // cancelled on removal/close
	reqCh     chan *Request
	prioCh    chan *Request // priority requests; connections prefer this over reqCh
	hotReqCh  chan *Request // unbuffered; hot (connected) connections read this
	hotPrioCh chan *Request // unbuffered; hot priority connections read this
	gate      *connGate
	stats     providerStats
	cancel    context.CancelFunc // cancels this group's slot goroutines
	p         Provider           // original config; used for auto-reconnect

	// Quota period configuration. quotaBytes/quotaUsed/quotaExceeded live in
	// stats so that NNTPConnection can update them via its *providerStats pointer.
	quotaPeriod  time.Duration // 0 = no auto-reset
	quotaResetAt atomic.Int64  // Unix nanoseconds of next reset; 0 = never
}

// attemptTimeout returns the per-attempt timeout (dispatch + time-to-first-byte
// bound). An explicit Provider.AttemptTimeout wins; otherwise it adapts to the
// provider's observed time-to-first-byte EWMA (seeded from the ping RTT) as
// 4×TTFB, clamped to [minAttemptTimeout, maxAttemptTimeout]. With no sample yet
// it falls back to minAttemptTimeout, preserving the historical 2s behavior.
func (g *providerGroup) attemptTimeout() time.Duration {
	if g.p.AttemptTimeout > 0 {
		return g.p.AttemptTimeout
	}
	ttfb := g.stats.ttfbEWMA.Load()
	if ttfb <= 0 {
		return minAttemptTimeout
	}
	d := time.Duration(ttfb) * 4
	if d < minAttemptTimeout {
		return minAttemptTimeout
	}
	if d > maxAttemptTimeout {
		return maxAttemptTimeout
	}
	return d
}

// isQuotaExceeded reports whether this provider has consumed its download quota
// for the current period.
//
// Fast path (quota not exceeded): single atomic.Bool load (~1 ns).
// Slow path (flag set, period elapsed): resets counters and returns false.
// The time.Now() call is deferred until the cached flag is actually set.
func (g *providerGroup) isQuotaExceeded() bool {
	if g.stats.quotaBytes <= 0 {
		return false // unlimited
	}
	if !g.stats.quotaExceeded.Load() {
		return false // fast path: quota not yet hit
	}
	// Flag is set. If a reset period is configured, check whether it has elapsed.
	if g.quotaPeriod > 0 {
		resetAt := g.quotaResetAt.Load()
		if resetAt > 0 && time.Now().UnixNano() >= resetAt {
			g.stats.quotaUsed.Store(0)
			g.stats.quotaExceeded.Store(false)
			g.quotaResetAt.Store(time.Now().Add(g.quotaPeriod).UnixNano())
			return false
		}
	}
	return true
}

type Client struct {
	ctx    context.Context
	cancel context.CancelFunc

	mainGroups   atomic.Pointer[[]*providerGroup]
	backupGroups atomic.Pointer[[]*providerGroup]
	nextIdx      atomic.Uint64 // round-robin counter for mainGroups

	dispatch   DispatchStrategy // set once by NewClient, read-only after
	statProbe  bool             // set once by NewClient; enables parallel STAT probing on 430
	speedAware bool             // set once by NewClient; weights round-robin dispatch by throughput

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
	// STAT (bodyless) may pipeline deeper than BODY. The overall pipeline cap is
	// max(Inflight, StatInflight); 0 or a smaller value means "same as Inflight"
	// (no separate STAT lane — fully backward compatible).
	statInflight := p.StatInflight
	if statInflight < inflight {
		statInflight = inflight
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
		name:        name,
		host:        p.Host,
		skipID:      providerSkipID(p),
		maxConns:    p.Connections,
		ctx:         gctx,
		reqCh:       make(chan *Request, p.Connections),
		prioCh:      make(chan *Request, p.Connections),
		hotReqCh:    make(chan *Request),
		hotPrioCh:   make(chan *Request),
		gate:        gate,
		cancel:      gcancel,
		p:           p,
		quotaPeriod: p.QuotaPeriod,
	}
	g.stats.quotaBytes = p.QuotaBytes
	if p.QuotaBytes > 0 {
		if p.QuotaUsed > 0 {
			g.stats.quotaUsed.Store(p.QuotaUsed)
			if p.QuotaUsed >= p.QuotaBytes {
				g.stats.quotaExceeded.Store(true)
			}
		}
		if p.QuotaPeriod > 0 {
			if !p.QuotaResetAt.IsZero() && p.QuotaResetAt.After(time.Now()) {
				g.quotaResetAt.Store(p.QuotaResetAt.UnixNano())
			} else {
				g.quotaResetAt.Store(time.Now().Add(p.QuotaPeriod).UnixNano())
			}
		}
	}

	// Ping with a short timeout so we don't block forever.
	if !p.SkipPing {
		pingCtx, pingCancel := context.WithTimeout(c.ctx, defaultHandshakeTimeout)
		g.stats.Ping = pingProvider(pingCtx, factory, p.Auth)
		pingCancel()
		// Seed the TTFB EWMA from the measured RTT so the adaptive attempt
		// timeout has a sensible starting point before any request completes.
		if g.stats.Ping.Err == nil && g.stats.Ping.RTT > 0 {
			g.stats.ttfbEWMA.Store(int64(g.stats.Ping.RTT))
		}
	}

	// Resolve the rolling stall timeout: 0 => default, negative => disabled.
	stall := p.StallTimeout
	if stall == 0 {
		stall = defaultStallTimeout
	} else if stall < 0 {
		stall = 0
	}

	// Resolve keepalive settings. If SkipPing is true and no explicit command
	// is set, keepalive is disabled (we don't know which command the server supports).
	kaInterval := p.KeepaliveInterval
	kaCmd := p.KeepaliveCommand
	if kaInterval > 0 {
		if kaCmd == "" {
			if p.SkipPing {
				kaInterval = 0 // disable: no safe probe command known
			} else {
				kaCmd = "DATE"
			}
		}
	}

	for range p.Connections {
		c.wg.Add(1)
		go runConnSlot(gctx, g.reqCh, g.prioCh, g.hotReqCh, g.hotPrioCh, factory, inflight, statInflight, p.Auth, p.UserAgent, p.IdleTimeout, stall, kaInterval, kaCmd, gate, &g.stats, name, &c.wg)
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
	seen := make(map[string]struct{}, len(providers))
	for i, p := range providers {
		if p.Connections <= 0 {
			return nil, fmt.Errorf("nntp: provider connections must be > 0")
		}
		if p.Factory == nil && p.Host == "" {
			return nil, fmt.Errorf("nntp: provider must have Host or Factory")
		}
		name := resolveProviderName(p, i)
		if _, dup := seen[name]; dup {
			return nil, fmt.Errorf("nntp: provider %q already exists", name)
		}
		seen[name] = struct{}{}
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
		ctx:        ctx,
		cancel:     cancel,
		dispatch:   cfg.dispatch,
		statProbe:  !cfg.statProbeOff,
		speedAware: !cfg.speedAwareOff,
		startTime:  time.Now(),
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

// extractProbeMsgID returns the "<id@host>" message-ID from a BODY, HEAD, or
// ARTICLE payload, or nil when the payload has no message-ID (GROUP, DATE, …)
// or when the command is already STAT or POST (probing would be redundant or
// inapplicable).
func extractProbeMsgID(payload []byte) []byte {
	if len(payload) == 0 {
		return nil
	}
	// Reject commands where probing is irrelevant or already done.
	switch {
	case len(payload) >= 4 && (payload[0]|0x20) == 's' && (payload[1]|0x20) == 't' &&
		(payload[2]|0x20) == 'a' && (payload[3]|0x20) == 't':
		return nil // STAT
	case len(payload) >= 4 && (payload[0]|0x20) == 'p' && (payload[1]|0x20) == 'o' &&
		(payload[2]|0x20) == 's' && (payload[3]|0x20) == 't':
		return nil // POST
	}
	open := bytes.IndexByte(payload, '<')
	if open < 0 {
		return nil
	}
	close := bytes.IndexByte(payload[open:], '>')
	if close < 0 {
		return nil
	}
	return payload[open : open+close+1]
}

// probeResult carries the outcome of one parallel STAT probe.
type probeResult struct {
	g         *providerGroup
	resp      Response
	ok        bool
	cancelled bool
}

// raceCandidates probes candidates in parallel with STAT on the priority lane,
// then sends the real payload to the first provider that confirms 223.
// All-miss latency is max-of-RTTs instead of sum-of-RTTs.
//
// Note: when all probes miss, respCh is NOT written; the caller must deliver
// the saved 430 response from the first provider that triggered the race.
func (c *Client) raceCandidates(
	ctx context.Context,
	candidates []*providerGroup,
	statPayload, payload []byte,
	bodyWriter io.Writer,
	onMeta func(YEncMeta),
	skipHosts *[4]string,
	skipCount *int,
	respCh chan<- Response,
) (delivered, cancelled bool, lastErr error) {
	// Filter to live candidates (skip same hosts and quota-exceeded).
	live := make([]*providerGroup, 0, len(candidates))
	seen := make(map[string]bool)
	for _, g := range candidates {
		if hostSkipped(g.skipID, skipHosts, *skipCount) {
			continue
		}
		if g.isQuotaExceeded() {
			lastErr = fmt.Errorf("%s: %w", g.name, ErrQuotaExceeded)
			continue
		}
		if g.host != "" && seen[g.host] {
			continue
		}
		if g.host != "" {
			seen[g.host] = true
		}
		live = append(live, g)
	}

	if len(live) == 0 {
		return false, false, lastErr
	}

	// Single live candidate: skip the probe RTT and send the real payload directly.
	if len(live) == 1 {
		g := live[0]
		resp, ok, done := c.tryGroup(ctx, g, payload, bodyWriter, onMeta, true)
		if done {
			return false, true, lastErr
		}
		if !ok {
			return false, false, lastErr
		}
		if resp.Err != nil {
			return false, false, resp.Err
		}
		if resp.StatusCode == 502 {
			_ = c.RemoveProvider(g.name)
			if g.p.ReconnectDelay > 0 {
				c.scheduleReconnect(g)
			}
			return false, false, fmt.Errorf("%s: %w", g.name, ErrServiceUnavailable)
		}
		if resp.StatusCode == 430 || resp.StatusCode == 423 {
			if g.skipID != "" && *skipCount < len(skipHosts) {
				skipHosts[*skipCount] = g.skipID
				*skipCount++
			}
			c.nextIdx.Add(1)
			return false, false, lastErr
		}
		respCh <- resp
		return true, false, lastErr
	}

	// ≥2 candidates: probe all in parallel.
	results := make(chan probeResult, len(live))
	for _, g := range live {
		go func(g *providerGroup) {
			resp, ok, done := c.tryGroup(ctx, g, statPayload, nil, nil, true)
			results <- probeResult{g: g, resp: resp, ok: ok, cancelled: done}
		}(g)
	}

	// Collect ALL probe results before acting on the winner, so that side
	// effects like 502 provider removal are applied regardless of order.
	var winner *providerGroup
	for range live {
		pr := <-results
		if pr.cancelled {
			cancelled = true
			continue
		}
		if !pr.ok {
			continue
		}
		if pr.resp.Err != nil {
			lastErr = pr.resp.Err
			continue
		}
		g := pr.g
		switch pr.resp.StatusCode {
		case 502:
			_ = c.RemoveProvider(g.name)
			if g.p.ReconnectDelay > 0 {
				c.scheduleReconnect(g)
			}
			lastErr = fmt.Errorf("%s: %w", g.name, ErrServiceUnavailable)
		case 430, 423:
			if g.skipID != "" && *skipCount < len(skipHosts) {
				skipHosts[*skipCount] = g.skipID
				*skipCount++
			}
			c.nextIdx.Add(1)
		case 223:
			if winner == nil {
				winner = g // first 223; keep collecting for 502s
			}
		default:
			lastErr = fmt.Errorf("%s: unexpected STAT probe status %d", g.name, pr.resp.StatusCode)
		}
	}

	if cancelled {
		return false, true, lastErr
	}

	if winner == nil {
		return false, false, lastErr
	}

	// Send the real payload to the winner on the priority lane.
	resp, ok, done := c.tryGroup(ctx, winner, payload, bodyWriter, onMeta, true)
	if done {
		return false, true, lastErr
	}
	if !ok {
		return false, false, lastErr
	}
	if resp.Err != nil {
		// A committed attempt with a caller writer already streamed partial
		// bytes; deliver the error rather than letting the caller re-stream
		// into the same writer on another provider.
		if bodyWriter != nil && attemptCommittedResp(resp) {
			respCh <- resp
			return true, false, nil
		}
		return false, false, resp.Err
	}
	if resp.StatusCode == 430 || resp.StatusCode == 423 {
		// Rare: article expired between STAT and BODY.
		if winner.skipID != "" && *skipCount < len(skipHosts) {
			skipHosts[*skipCount] = winner.skipID
			*skipCount++
		}
		c.nextIdx.Add(1)
		return false, false, lastErr
	}
	if resp.StatusCode == 502 {
		_ = c.RemoveProvider(winner.name)
		if winner.p.ReconnectDelay > 0 {
			c.scheduleReconnect(winner)
		}
		return false, false, fmt.Errorf("%s: %w", winner.name, ErrServiceUnavailable)
	}
	respCh <- resp
	return true, false, lastErr
}

// minDeadline returns d, unless other is an earlier deadline (when hasOther),
// in which case it returns other. The bool is always true (a deadline exists).
func minDeadline(d, other time.Time, hasOther bool) (time.Time, bool) {
	if hasOther && other.Before(d) {
		return other, true
	}
	return d, true
}

// writeDeadline is the deadline used while writing the request payload: the
// earlier of the caller's ctx deadline and the attempt deadline, so a dead
// socket cannot hang the writer. Payloads are tiny, so the attempt deadline is
// a safe upper bound.
func (req *Request) writeDeadline() (time.Time, bool) {
	dl, ok := req.Ctx.Deadline()
	if !req.attemptDeadline.IsZero() && (!ok || req.attemptDeadline.Before(dl)) {
		return req.attemptDeadline, true
	}
	return dl, ok
}

// tryGroup dispatches a single request to a provider group and waits for the
// response. priority=true routes through the priority channels.
//
// The attempt timeout bounds only dispatch + time-to-first-response-byte, not
// the whole body transfer: the request ctx carries no fixed deadline, and a
// timer races the response. If the timer fires before any byte arrives the
// attempt is abandoned (CAS pending→abandoned) and we fail over; if the reader
// committed first (it saw a byte) we keep waiting for the body to finish, since
// failing over after partial delivery would corrupt a caller's writer.
func (c *Client) tryGroup(
	ctx context.Context,
	g *providerGroup,
	payload []byte,
	bodyWriter io.Writer,
	onMeta func(YEncMeta),
	priority bool,
) (resp Response, ok bool, done bool) {
	reqCtx, reqCancel := context.WithCancel(ctx)
	defer reqCancel()

	attemptTimeout := g.attemptTimeout()
	innerCh := make(chan Response, 1)
	req := &Request{
		Ctx:             reqCtx,
		Payload:         payload,
		RespCh:          innerCh,
		BodyWriter:      bodyWriter,
		OnMeta:          onMeta,
		attemptDeadline: time.Now().Add(attemptTimeout),
	}

	timer := time.NewTimer(attemptTimeout)
	defer timer.Stop()

	var hotCh chan *Request
	var coldCh chan *Request
	if priority {
		hotCh = g.hotPrioCh
		coldCh = g.prioCh
	} else {
		hotCh = g.hotReqCh
		coldCh = g.reqCh
	}

	select {
	case hotCh <- req:
	default:
		select {
		case <-c.ctx.Done():
			return Response{}, false, true
		case <-reqCtx.Done():
			return Response{}, false, ctx.Err() != nil
		case <-g.ctx.Done():
			return Response{}, false, false
		case <-timer.C:
			// Could not be dispatched within the attempt window: the provider
			// is saturated. Fail over.
			return Response{}, false, false
		case coldCh <- req:
		}
	}

	for {
		select {
		case resp, ok = <-innerCh:
			return resp, ok, false
		case <-c.ctx.Done():
			return Response{}, false, true
		case <-g.ctx.Done():
			return Response{}, false, false
		case <-reqCtx.Done():
			return Response{}, false, ctx.Err() != nil
		case <-timer.C:
			if req.attemptState.CompareAndSwap(attemptPending, attemptAbandoned) {
				// No response byte arrived in time: hung or too-slow to start.
				// Cancel so the reader drops the request, and fail over.
				reqCancel()
				// done only when the caller's or the pool's context was
				// cancelled (true shutdown), not on a plain attempt timeout.
				return Response{}, false, ctx.Err() != nil || c.ctx.Err() != nil
			}
			// Reader already committed (first byte arrived): the body is
			// streaming. Do not fail over; keep waiting for it to finish. The
			// timer has fired and will not fire again.
		}
	}
}

// providerSkipID returns the identity used to suppress further attempts after a
// 430. Providers on the same host share article availability; so do providers
// that resell the same upstream storage under different hostnames (e.g. two
// brands on one backbone). StorageGroup lets an operator declare the latter.
func providerSkipID(p Provider) string {
	if p.StorageGroup != "" {
		return p.StorageGroup
	}
	return p.Host
}

// hostSkipped reports whether the skip identity is already in the skip list.
// Empty identities (Factory-based providers with no StorageGroup) are never skipped.
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

// attemptCommittedResp reports whether the response came from an attempt that
// had already started streaming bytes (the reader committed). Such an attempt
// must not be retried or failed over when a caller-supplied writer is in use.
func attemptCommittedResp(resp Response) bool {
	return resp.Request != nil && resp.Request.attemptState.Load() == attemptCommitted
}

// maxSpeedScore is the highest multiplier speed-aware dispatch applies to a
// provider's base (capacity) weight.
const maxSpeedScore = 4

// dispatchWeights computes cumulative round-robin weights for the given main
// providers. The base weight is each provider's available connection capacity
// (min 1 when live); quota-exceeded providers get weight 0. When speedAware is
// true the base weight is scaled by speedScore so faster providers receive
// proportionally more traffic. With no throughput samples this reduces to pure
// capacity weighting (the historical behavior).
func dispatchWeights(mains []*providerGroup, speedAware bool) (cum []int, total int) {
	cum = make([]int, len(mains))
	var maxSpeed float64
	if speedAware {
		for _, g := range mains {
			if s := speedEWMABytesPerSec(&g.stats); s > maxSpeed {
				maxSpeed = s
			}
		}
	}
	for i, g := range mains {
		w := 0
		if !g.isQuotaExceeded() {
			w = max(1, int(g.gate.available.Load()))
			if speedAware && maxSpeed > 0 {
				w *= speedScore(speedEWMABytesPerSec(&g.stats), maxSpeed)
			}
		}
		total += w
		cum[i] = total
	}
	return cum, total
}

// speedScore maps a provider's throughput to an integer multiplier in
// [1, maxSpeedScore] relative to the fastest provider. An unmeasured provider
// (speed 0) scores the maximum so it is not starved before it has a sample.
func speedScore(speed, maxSpeed float64) int {
	if speed <= 0 {
		return maxSpeedScore
	}
	s := int(float64(maxSpeedScore)*speed/maxSpeed + 0.5)
	if s < 1 {
		return 1
	}
	if s > maxSpeedScore {
		return maxSpeedScore
	}
	return s
}

func (c *Client) sendWithRetry(ctx context.Context, payload []byte, bodyWriter io.Writer, onMeta func(YEncMeta), respCh chan Response) {
	c.doSendWithRetry(ctx, payload, bodyWriter, onMeta, respCh, false)
}

// tryGroupResilient retries a single provider on a fresh connection when a
// pooled connection dies mid-request (stale socket the server already
// closed). Without this, a single-provider pool fails immediately with
// "all providers exhausted: ... connection died" because there is no next
// provider to fall back to. Bounded so a genuinely-down server still fails
// fast. Only transport-level connection death is retried (see
// isConnectionDeathError); 430/502/quota and provider removal (!ok) keep
// their existing behavior.
func (c *Client) tryGroupResilient(
	ctx context.Context,
	g *providerGroup,
	payload []byte,
	bodyWriter io.Writer,
	onMeta func(YEncMeta),
	priority bool,
) (resp Response, ok bool, cancelled bool) {
	for r := 0; ; r++ {
		resp, ok, cancelled = c.tryGroup(ctx, g, payload, bodyWriter, onMeta, priority)
		if cancelled || !ok {
			return
		}
		// If the attempt already streamed bytes into the caller's writer, never
		// retry: partial data was delivered and re-streaming would corrupt it.
		// Buffered requests (bodyWriter == nil) keep their per-attempt buffer,
		// so retrying them stays safe.
		if bodyWriter != nil && attemptCommittedResp(resp) {
			return
		}
		if r < maxConnDiedRetries && isConnectionDeathError(resp.Err) {
			continue // dead connection drained; retry fresh on same provider
		}
		return
	}
}

func (c *Client) doSendWithRetry(ctx context.Context, payload []byte, bodyWriter io.Writer, onMeta func(YEncMeta), respCh chan Response, priority bool) {
	defer close(respCh)

	// Precompute for STAT probe: extract message-ID once.
	msgID := extractProbeMsgID(payload)
	raceable := c.statProbe && msgID != nil
	var statPayload []byte
	if raceable {
		statPayload = append(append([]byte("STAT "), msgID...), "\r\n"...)
	}

	var lastResp Response
	hasResp := false
	var lastErr error
	post430 := false

	// Track providers that returned 430 so we can skip others sharing the same
	// article availability — same host (different credentials won't help), or
	// same declared StorageGroup (same upstream storage behind another brand).
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
		// Priority order: first provider with available capacity and within quota,
		// falling back to provider 0 if all are saturated or exceeded.
		for i, g := range mains {
			if g.gate.available.Load() > 0 && !g.isQuotaExceeded() {
				start = i
				break
			}
		}
	default: // DispatchRoundRobin
		// Dynamic weighted round-robin. Quota-exceeded providers get weight 0
		// so they are never selected during normal dispatch.
		cumWeights, totalW := dispatchWeights(mains, c.speedAware)
		if totalW == 0 {
			// All providers are quota-exceeded; start at 0 and let the main
			// loop below return ErrQuotaExceeded for each.
			start = 0
		} else {
			slot := int(c.nextIdx.Add(1) % uint64(totalW))
			start = sort.SearchInts(cumWeights, slot+1)
		}
	}

	for attempt := range n {
		idx := (start + attempt) % n
		g := mains[idx]
		if hostSkipped(g.skipID, &skipHosts, skipCount) {
			continue
		}
		if g.isQuotaExceeded() {
			lastErr = fmt.Errorf("%s: %w", g.name, ErrQuotaExceeded)
			continue
		}
		resp, ok, cancelled := c.tryGroupResilient(ctx, g, payload, bodyWriter, onMeta, priority || post430)
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
			// A committed attempt with a caller writer already streamed partial
			// bytes; deliver the error rather than re-streaming into the same
			// writer on another provider.
			if bodyWriter != nil && attemptCommittedResp(resp) {
				respCh <- resp
				return
			}
			lastErr = resp.Err
			continue
		}
		if resp.StatusCode == 502 {
			// Provider returned "service unavailable" — remove it from the
			// pool immediately so no further requests are routed to it.
			_ = c.RemoveProvider(g.name)
			if g.p.ReconnectDelay > 0 {
				c.scheduleReconnect(g)
			}
			lastErr = fmt.Errorf("%s: %w", g.name, ErrServiceUnavailable)
			continue
		}
		if resp.StatusCode == 430 {
			c.nextIdx.Add(1) // bias next request away from this provider
			if g.skipID != "" && skipCount < len(skipHosts) {
				skipHosts[skipCount] = g.skipID
				skipCount++
			}
			lastResp = resp
			hasResp = true
			post430 = true

			if raceable {
				// Build remaining mains and race them in parallel via STAT.
				rest := make([]*providerGroup, 0, n-attempt-1)
				for a := attempt + 1; a < n; a++ {
					rest = append(rest, mains[(start+a)%n])
				}
				delivered, cancelled, raceErr := c.raceCandidates(
					ctx, rest, statPayload, payload, bodyWriter, onMeta,
					&skipHosts, &skipCount, respCh,
				)
				if cancelled {
					err := ctx.Err()
					if err == nil {
						err = c.ctx.Err()
					}
					respCh <- Response{Err: err}
					return
				}
				if delivered {
					return
				}
				if raceErr != nil {
					lastErr = raceErr
				}
				break // all remaining mains were probed in the race
			}
			continue
		}
		// Success.
		respCh <- resp
		return
	}

	// 2. All main providers returned 430 (or died) — try backup providers.
	backups := *c.backupGroups.Load()
	if raceable && post430 {
		delivered, cancelled, raceErr := c.raceCandidates(
			ctx, backups, statPayload, payload, bodyWriter, onMeta,
			&skipHosts, &skipCount, respCh,
		)
		if cancelled {
			err := ctx.Err()
			if err == nil {
				err = c.ctx.Err()
			}
			respCh <- Response{Err: err}
			return
		}
		if delivered {
			return
		}
		if raceErr != nil {
			lastErr = raceErr
		}
	} else {
		for i := range backups {
			g := backups[i]
			if hostSkipped(g.skipID, &skipHosts, skipCount) {
				continue
			}
			if g.isQuotaExceeded() {
				lastErr = fmt.Errorf("%s: %w", g.name, ErrQuotaExceeded)
				continue
			}
			resp, ok, cancelled := c.tryGroupResilient(ctx, g, payload, bodyWriter, onMeta, priority || post430)
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
				// A committed attempt with a caller writer already streamed
				// partial bytes; deliver the error rather than re-streaming into
				// the same writer on another provider.
				if bodyWriter != nil && attemptCommittedResp(resp) {
					respCh <- resp
					return
				}
				lastErr = resp.Err
				continue
			}
			if resp.StatusCode == 502 {
				_ = c.RemoveProvider(g.name)
				if g.p.ReconnectDelay > 0 {
					c.scheduleReconnect(g)
				}
				lastErr = fmt.Errorf("%s: %w", g.name, ErrServiceUnavailable)
				continue
			}
			// Deliver whatever backup returns (including 430).
			respCh <- resp
			return
		}
	}

	// 3. All providers exhausted — deliver the last 430, the last error, or a fallback.
	if hasResp {
		respCh <- lastResp
	} else if lastErr != nil {
		respCh <- Response{Err: fmt.Errorf("nntp: all providers exhausted: %w", lastErr)}
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
			quotaUsed := g.stats.quotaUsed.Load()
			ps := ProviderStats{
				Name:              g.name,
				SpeedEWMA:         speedEWMABytesPerSec(&g.stats),
				BytesConsumed:     consumed,
				Missing:           g.stats.Missing.Load(),
				Errors:            g.stats.Errors.Load(),
				ActiveConnections: running,
				MaxConnections:    maxSlots,
				AvailableSlots:    int(g.gate.available.Load()),
				TTFB:              time.Duration(g.stats.ttfbEWMA.Load()),
				Ping:              g.stats.Ping,
				QuotaBytes:        g.stats.quotaBytes,
				QuotaUsed:         quotaUsed,
				QuotaExceeded:     g.stats.quotaBytes > 0 && quotaUsed >= g.stats.quotaBytes,
			}
			if g.stats.quotaBytes > 0 && g.quotaPeriod > 0 {
				resetAt := g.quotaResetAt.Load()
				if resetAt > 0 {
					ps.QuotaResetAt = time.Unix(0, resetAt)
				}
			}
			if secs > 0 {
				ps.AvgSpeed = float64(consumed) / secs
			}
			cs.Providers = append(cs.Providers, ps)
		}
	}
	cs.BytesConsumed = totalBytes
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
func (c *Client) scheduleReconnect(g *providerGroup) {
	go func() {
		select {
		case <-c.ctx.Done():
			return
		case <-time.After(g.p.ReconnectDelay):
		}
		_ = c.AddProvider(g.p) // no-op if client closed or duplicate
	}()
}

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

// ResetProviderQuota resets the download quota for the named provider without
// removing and re-adding it. The consumed-bytes counter and exceeded flag are
// cleared atomically, and a fresh reset deadline is scheduled when the provider
// has a non-zero quota period.
//
// Returns an error if no provider with that name is registered.
func (c *Client) ResetProviderQuota(name string) error {
	for _, ptr := range [...]*atomic.Pointer[[]*providerGroup]{
		&c.mainGroups,
		&c.backupGroups,
	} {
		for _, g := range *ptr.Load() {
			if g.name != name {
				continue
			}
			g.stats.quotaUsed.Store(0)
			g.stats.quotaExceeded.Store(false)
			if g.quotaPeriod > 0 {
				g.quotaResetAt.Store(time.Now().Add(g.quotaPeriod).UnixNano())
			} else {
				g.quotaResetAt.Store(0)
			}
			return nil
		}
	}
	return fmt.Errorf("nntp: provider %q not found", name)
}
