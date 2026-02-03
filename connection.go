package nntppool

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/javi11/nntppool/v3/internal"
)

// handshakeTimeout is the maximum time to wait for greeting and authentication responses.
// This is shorter than the general response timeout since handshake messages are small.
const handshakeTimeout = 30 * time.Second

type NNTPConnection struct {
	conn net.Conn

	ctx    context.Context
	cancel context.CancelFunc

	reqCh   <-chan *Request
	pending chan *Request

	inflightSem chan struct{}

	rb internal.ReadBuffer

	Greeting NNTPResponse

	done   chan struct{}
	doneMu sync.Once

	// ready is closed when Run() is ready to receive from reqCh.
	// This signals that the connection is fully initialized and consuming requests.
	ready chan struct{}

	maxIdleTime  time.Duration
	lastActivity int64
	maxLifeTime  time.Duration
	createdAt    time.Time
}

func newNNTPConnectionFromConn(ctx context.Context, conn net.Conn, inflightLimit int, reqCh <-chan *Request, auth Auth, maxIdleTime time.Duration, maxLifeTime time.Duration) (*NNTPConnection, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	cctx, cancel := context.WithCancel(ctx)

	// Apply 0-30% backward offset to createdAt to prevent thundering herd when
	// many connections are created at the same time (e.g., at startup).
	// This makes connections appear to have been created at different times,
	// spreading out their expiration over 30% of maxLifeTime.
	createdAt := time.Now()
	if maxLifeTime > 0 {
		ageOffset := time.Duration(float64(maxLifeTime) * 0.30 * rand.Float64())
		createdAt = createdAt.Add(-ageOffset)
	}

	c := &NNTPConnection{
		conn:         conn,
		ctx:          cctx,
		cancel:       cancel,
		reqCh:        reqCh,
		pending:      make(chan *Request, inflightLimit),
		inflightSem:  make(chan struct{}, inflightLimit),
		rb:           internal.ReadBuffer{},
		done:         make(chan struct{}),
		ready:        make(chan struct{}),
		maxIdleTime:  maxIdleTime,
		lastActivity: time.Now().Unix(),
		maxLifeTime:  maxLifeTime,
		createdAt:    createdAt,
	}

	if mc, ok := conn.(*internal.MeteredConn); ok {
		mc.LastActivity = &c.lastActivity
	}

	// Server greeting is sent immediately upon connect.
	greeting, err := c.readOneResponse(io.Discard, handshakeTimeout)
	if err != nil {
		return nil, fmt.Errorf("nntp greeting: %w", err)
	}
	c.Greeting = greeting
	if greeting.StatusCode != 200 && greeting.StatusCode != 201 {
		return nil, fmt.Errorf("unexpected nntp greeting: %s", greeting.Message)
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

func NewNNTPConnection(ctx context.Context, addr string, tlsConfig *tls.Config, inflightLimit int, reqCh <-chan *Request, auth Auth, maxIdleTime time.Duration, maxLifeTime time.Duration) (*NNTPConnection, error) {
	conn, err := internal.NewNetConn(addr, tlsConfig)
	if err != nil {
		return nil, err
	}

	c, err := newNNTPConnectionFromConn(ctx, conn, inflightLimit, reqCh, auth, maxIdleTime, maxLifeTime)
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
	resp, err := c.readOneResponse(io.Discard, handshakeTimeout)
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
	resp, err = c.readOneResponse(io.Discard, handshakeTimeout)
	if err != nil {
		return fmt.Errorf("authinfo pass response: %w", err)
	}
	if resp.StatusCode != 281 {
		return fmt.Errorf("authinfo pass unexpected response: %s", resp.Message)
	}
	return nil
}

func (c *NNTPConnection) Done() <-chan struct{}  { return c.done }
func (c *NNTPConnection) Ready() <-chan struct{} { return c.ready }

func (c *NNTPConnection) closeDone() {
	c.doneMu.Do(func() { close(c.done) })
}

// drainPending waits for all in-flight requests to complete before returning.
// This is used during graceful shutdown when connection lifetime expires,
// ensuring pending requests get their responses before the connection closes.
func (c *NNTPConnection) drainPending() {
	slots := cap(c.inflightSem)
	for i := 0; i < slots; i++ {
		select {
		case c.inflightSem <- struct{}{}:
			// Acquired a slot, continue
		case <-c.ctx.Done():
			return
		}
	}
	// All slots acquired = no in-flight requests, safe to close
}

func (c *NNTPConnection) failOutstanding() {
	// This can be called multiple times (from readerLoop and from Run).
	// Each call drains whatever is currently in pending at that moment.
	// The semaphore release uses select{default:} to avoid blocking if
	// the semaphore is already drained or if the reader already released
	// the slot for this request.
	for {
		select {
		case req := <-c.pending:
			if req == nil {
				continue
			}
			// Send error response so caller can distinguish from successful completion
			select {
			case req.RespCh <- Response{Err: context.Canceled, Request: req}:
			default:
			}
			internal.SafeClose(req.RespCh)
			// Release the inflight slot for this request (best-effort).
			// Uses select{default:} because:
			// 1. The reader may have already picked this request and released the slot
			// 2. Multiple calls to failOutstanding may race
			select {
			case <-c.inflightSem:
			default:
			}
		default:
			return
		}
	}
}

func (c *NNTPConnection) Close() error {
	c.cancel()
	_ = c.conn.Close()
	<-c.done
	return nil
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

	// Signal that we're ready to receive requests from reqCh.
	// This must happen before entering the loop so callers waiting on Ready()
	// know the connection is actively consuming from reqCh.
	close(c.ready)

	for {
		select {
		case <-c.ctx.Done():
			return
		default:
		}

		// wait until we have inflight capacity
		select {
		case c.inflightSem <- struct{}{}:
		case <-c.ctx.Done():
			return
		}

		// pull next request
		var req *Request
		var ok bool

		var timer *time.Timer
		var timeout <-chan time.Time

		if c.maxIdleTime > 0 {
			last := atomic.LoadInt64(&c.lastActivity)
			if last == 0 {
				last = time.Now().Unix()
			}
			elapsed := time.Since(time.Unix(last, 0))
			remaining := c.maxIdleTime - elapsed
			if remaining <= 0 {
				<-c.inflightSem
				return
			}
			timer = time.NewTimer(remaining)
			timeout = timer.C
		}

		var lifeTimer *time.Timer
		var lifeTimeout <-chan time.Time

		if c.maxLifeTime > 0 {
			remaining := c.maxLifeTime - time.Since(c.createdAt)
			if remaining <= 0 {
				if timer != nil {
					timer.Stop()
				}
				<-c.inflightSem
				c.drainPending()
				return
			}
			lifeTimer = time.NewTimer(remaining)
			lifeTimeout = lifeTimer.C
		}

		select {
		case req, ok = <-c.reqCh:
			if timer != nil {
				timer.Stop()
			}
			if lifeTimer != nil {
				lifeTimer.Stop()
			}
		case <-c.ctx.Done():
			if timer != nil {
				timer.Stop()
			}
			if lifeTimer != nil {
				lifeTimer.Stop()
			}
			<-c.inflightSem
			return
		case <-timeout:
			if lifeTimer != nil {
				lifeTimer.Stop()
			}
			<-c.inflightSem
			return
		case <-lifeTimeout:
			if timer != nil {
				timer.Stop()
			}
			<-c.inflightSem
			c.drainPending()
			return
		}
		if !ok {
			<-c.inflightSem
			return
		}
		if req.Ctx == nil {
			req.Ctx = context.Background()
		}

		// Cancel before sending (queued-but-not-sent case)
		select {
		case <-req.Ctx.Done():
			<-c.inflightSem
			// Send cancellation error so caller can distinguish from unexpected closure
			select {
			case req.RespCh <- Response{Err: req.Ctx.Err(), Request: req}:
			default:
			}
			close(req.RespCh)
			continue
		default:
		}

		// track FIFO ordering
		c.pending <- req

		// per-request write deadline
		if dl, ok := req.Ctx.Deadline(); ok {
			_ = c.conn.SetWriteDeadline(dl)
		} else {
			_ = c.conn.SetWriteDeadline(time.Time{})
		}

		// pipeline write
		if _, err := c.conn.Write(req.Payload); err != nil {
			// DON'T release inflightSem here - the request is already in pending.
			// failOutstanding() will handle releasing the slot when draining pending.
			// If the reader already picked it up, the reader will release it at line 440.
			// Releasing here would cause double-release when reader finishes normally,
			// which blocks the reader forever trying to release from an empty semaphore.
			_ = c.conn.Close()
			c.failOutstanding()
			return
		}
		_ = c.conn.SetWriteDeadline(time.Time{})
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
			c.failOutstanding()
			return
		}
		if req.Ctx == nil {
			req.Ctx = context.Background()
		}

		resp := Response{
			Request: req,
		}
		decoder := NNTPResponse{
			Lines:        make([]string, 0, 8), // Pre-allocate for typical response
			OnYencHeader: req.OnYencHeader,
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
		outRef := &internal.WriterRef{W: out}
		var feederOut io.Writer = outRef
		if _, ok := out.(io.WriterAt); ok {
			feederOut = &internal.WriterRefAt{WriterRef: outRef}
		}

		err := c.rb.FeedUntilDone(c.conn, &decoder, feederOut, func() (time.Time, bool) {
			if deliver {
				select {
				case <-req.Ctx.Done():
					deliver = false
					outRef.W = io.Discard
				default:
				}
			}
			return req.Ctx.Deadline()
		})
		_ = c.conn.SetReadDeadline(time.Time{})

		// Check for write errors (user closed their writer)
		// Prioritize write errors - response was drained but write failed
		if writeErr := outRef.Err(); writeErr != nil {
			resp.Err = writeErr // Return original error (e.g., io.ErrClosedPipe)
		} else if err != nil {
			resp.Err = err
		}

		resp.StatusCode = decoder.StatusCode
		resp.Status = decoder.Message
		resp.Lines = decoder.Lines
		resp.Meta = decoder

		// Always send a response so caller can distinguish cancellation from provider failure.
		// If request was cancelled mid-response, set the error to context.Canceled.
		if !deliver {
			resp.Err = context.Canceled
		}
		// Best effort: don't block forever if the receiver abandoned the channel.
		select {
		case req.RespCh <- resp:
		default:
		}
		internal.SafeClose(req.RespCh)

		// release inflight slot
		<-c.inflightSem

		// If we hit a network error or IO error, close the connection.
		// Don't close on:
		// - context.Canceled: just a cancelled request, not a connection problem
		// - io.ErrClosedPipe: user closed their writer early, response was drained successfully
		// This is critical for pipelining: if one request is cancelled (e.g., reader moved to next segment),
		// we don't want to kill the connection and fail all other pipelined requests.
		if resp.Err != nil &&
			!errors.Is(resp.Err, context.Canceled) &&
			!errors.Is(resp.Err, io.ErrClosedPipe) {
			_ = c.conn.Close()
			c.failOutstanding()
			return
		}
	}
}

// readOneResponse reads a complete NNTP response from the stream.
// Any unread bytes remain buffered in c.rbuf[c.rstart:c.rend] for subsequent reads.
// If timeout > 0, a deadline is applied to prevent indefinite hangs.
func (c *NNTPConnection) readOneResponse(out io.Writer, timeout time.Duration) (NNTPResponse, error) {
	resp := NNTPResponse{}

	var deadline time.Time
	hasDeadline := false
	if timeout > 0 {
		deadline = time.Now().Add(timeout)
		hasDeadline = true
	}

	if err := c.rb.FeedUntilDone(c.conn, &resp, out, func() (time.Time, bool) {
		return deadline, hasDeadline
	}); err != nil {
		return resp, err
	}
	return resp, nil
}
