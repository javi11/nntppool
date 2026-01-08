package nntpcli

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

// Request represents an NNTP command to be sent to the server.
type Request struct {
	Ctx context.Context

	Payload []byte
	RespCh  chan Response

	// Optional: decoded body bytes are streamed here. If nil, they are buffered into Response.Body.
	BodyWriter io.Writer
}

// Response represents an NNTP server response.
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

// Auth holds authentication credentials for NNTP connection.
type Auth struct {
	Username string
	Password string
}

// ConnFactory is used by Client to create connections.
type ConnFactory func(ctx context.Context) (net.Conn, error)

// NNTPConnection manages a single NNTP connection with pipelining support.
type NNTPConnection struct {
	conn net.Conn

	ctx    context.Context
	cancel context.CancelFunc

	reqCh   <-chan *Request
	pending chan *Request // capacity = inflightLimit, provides back-pressure

	rb readBuffer

	Greeting NNTPResponse

	done   chan struct{}
	doneMu sync.Once

	failMu sync.Once

	// Idle tracking
	lastActivity atomic.Int64 // Unix nanoseconds of last completed request

	// Callback invoked when connection closes (for pool management)
	onClose func(*NNTPConnection)
}

func newNetConn(addr string, tlsConfig *tls.Config) (net.Conn, error) {
	if tlsConfig != nil {
		return tls.Dial("tcp", addr, tlsConfig)
	}
	return net.Dial("tcp", addr)
}

func newNNTPConnectionFromConn(ctx context.Context, conn net.Conn, inflightLimit int, reqCh <-chan *Request, auth Auth) (*NNTPConnection, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	cctx, cancel := context.WithCancel(ctx)

	c := &NNTPConnection{
		conn:    conn,
		ctx:     cctx,
		cancel:  cancel,
		reqCh:   reqCh,
		pending: make(chan *Request, inflightLimit),
		rb:      readBuffer{buf: make([]byte, defaultReadBufSize)},
		done:    make(chan struct{}),
	}

	// Server greeting is sent immediately upon connect.
	greeting, err := c.readOneResponse(io.Discard)
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

// NewNNTPConnection creates a new NNTP connection to the specified address.
func NewNNTPConnection(ctx context.Context, addr string, tlsConfig *tls.Config, inflightLimit int, reqCh <-chan *Request, auth Auth) (*NNTPConnection, error) {
	conn, err := newNetConn(addr, tlsConfig)
	if err != nil {
		return nil, err
	}

	c, err := newNNTPConnectionFromConn(ctx, conn, inflightLimit, reqCh, auth)
	if err != nil {
		_ = conn.Close()
		return nil, err
	}
	return c, nil
}

func (c *NNTPConnection) auth(auth Auth) error {
	// AUTHINFO USER
	if _, err := c.conn.Write([]byte(fmt.Sprintf("AUTHINFO USER %s\r\n", auth.Username))); err != nil {
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
	if _, err := c.conn.Write([]byte(fmt.Sprintf("AUTHINFO PASS %s\r\n", auth.Password))); err != nil {
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

// Done returns a channel that is closed when the connection is done.
func (c *NNTPConnection) Done() <-chan struct{} { return c.done }

func (c *NNTPConnection) closeDone() {
	c.doneMu.Do(func() { close(c.done) })
}

func safeClose[T any](ch chan T) {
	defer func() { _ = recover() }()
	close(ch)
}

func safeSend[T any](ch chan T, v T) {
	defer func() { _ = recover() }()
	ch <- v
}

func (c *NNTPConnection) failOutstanding() {
	c.failMu.Do(func() {
		for {
			select {
			case req := <-c.pending:
				if req == nil {
					continue
				}
				safeClose(req.RespCh)
			default:
				return
			}
		}
	})
}

// Close closes the connection and waits for cleanup to complete.
func (c *NNTPConnection) Close() error {
	c.cancel()
	_ = c.conn.Close()
	<-c.done
	return nil
}

// LastActivity returns the time of the last completed request.
// Returns zero time if no requests have been completed yet.
func (c *NNTPConnection) LastActivity() time.Time {
	ns := c.lastActivity.Load()
	if ns == 0 {
		return time.Time{}
	}
	return time.Unix(0, ns)
}

// updateActivity updates the last activity timestamp to now.
func (c *NNTPConnection) updateActivity() {
	c.lastActivity.Store(time.Now().UnixNano())
}

// SetOnClose sets the callback invoked when the connection closes.
// This is used by the Client to remove the connection from its pool.
func (c *NNTPConnection) SetOnClose(fn func(*NNTPConnection)) {
	c.onClose = fn
}

// SendGroup sends a GROUP command and waits for the response.
// This is used to set the initial group on a connection before it starts
// processing requests from the shared channel.
// Must be called before Run().
func (c *NNTPConnection) SendGroup(ctx context.Context, group string) error {
	cmd := fmt.Sprintf("GROUP %s\r\n", group)
	if _, err := c.conn.Write([]byte(cmd)); err != nil {
		return fmt.Errorf("group command: %w", err)
	}

	resp, err := c.readOneResponse(io.Discard)
	if err != nil {
		return fmt.Errorf("group response: %w", err)
	}

	// 211 = group selected successfully
	if resp.StatusCode != 211 {
		return fmt.Errorf("group failed: %d %s", resp.StatusCode, resp.Message)
	}

	return nil
}

type writerRef struct {
	w io.Writer
}

func (wr *writerRef) Write(p []byte) (int, error) {
	return wr.w.Write(p)
}

// Run starts the connection's writer loop. It should be called in a goroutine.
func (c *NNTPConnection) Run() {
	defer func() {
		c.cancel()
		_ = c.conn.Close()
		c.failOutstanding()
		c.closeDone()
		// Notify the Client that this connection has closed
		if c.onClose != nil {
			c.onClose(c)
		}
	}()

	go func() {
		c.readerLoop()
		// ensure writer exits too
		c.cancel()
	}()

	for {
		// Pull next request - pending channel provides flow control
		var req *Request
		var ok bool
		select {
		case req, ok = <-c.reqCh:
		case <-c.ctx.Done():
			return
		}
		if !ok {
			return
		}
		if req.Ctx == nil {
			req.Ctx = context.Background()
		}

		// Cancel before sending (queued-but-not-sent case)
		select {
		case <-req.Ctx.Done():
			close(req.RespCh)
			continue
		default:
		}

		// per-request write deadline
		if dl, ok := req.Ctx.Deadline(); ok {
			_ = c.conn.SetWriteDeadline(dl)
		} else {
			_ = c.conn.SetWriteDeadline(time.Time{})
		}

		// pipeline write - must succeed BEFORE enqueuing to pending
		// to prevent response misalignment on write failure
		if _, err := c.conn.Write(req.Payload); err != nil {
			safeClose(req.RespCh)
			_ = c.conn.Close()
			c.failOutstanding()
			return
		}
		_ = c.conn.SetWriteDeadline(time.Time{})

		// track FIFO ordering - enqueue AFTER successful write
		// pending channel capacity = inflightLimit, provides back-pressure
		select {
		case c.pending <- req:
		case <-c.ctx.Done():
			safeClose(req.RespCh)
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
		decoder := NNTPResponse{}

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
		_ = c.conn.SetReadDeadline(time.Time{})

		// If feedUntilDone returned an error but response not complete,
		// drain remaining data to io.Discard to preserve connection for reuse.
		// This handles writer failures (broken pipe) where network is fine.
		if err != nil && !decoder.Done() {
			drainErr := c.rb.feedUntilDone(c.conn, &decoder, io.Discard, func() (time.Time, bool) {
				// Use a reasonable timeout for draining (5 seconds)
				return time.Now().Add(5 * time.Second), true
			})
			if drainErr != nil {
				// Drain failed (network error) - close connection
				resp.Err = err
				if deliver {
					safeSend(req.RespCh, resp)
				}
				safeClose(req.RespCh)
				c.updateActivity()
				_ = c.conn.Close()
				c.failOutstanding()
				return
			}
		}

		if err != nil {
			resp.Err = err
		}

		resp.StatusCode = decoder.StatusCode
		resp.Status = decoder.Message
		resp.Lines = decoder.Lines
		resp.Meta = decoder

		if deliver {
			// Best effort: use safeSend to handle closed channel race with failOutstanding()
			safeSend(req.RespCh, resp)
		}
		safeClose(req.RespCh)

		// Update last activity timestamp for idle tracking
		c.updateActivity()

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
