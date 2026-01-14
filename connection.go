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

type MeteredConn struct {
	net.Conn
	bytesRead    *uint64
	bytesWritten *uint64
	lastActivity *int64
}

func (m *MeteredConn) Read(b []byte) (n int, err error) {
	n, err = m.Conn.Read(b)
	if n > 0 {
		if m.bytesRead != nil {
			atomic.AddUint64(m.bytesRead, uint64(n))
		}
		if m.lastActivity != nil {
			atomic.StoreInt64(m.lastActivity, time.Now().Unix())
		}
	}
	return
}

func (m *MeteredConn) Write(b []byte) (n int, err error) {
	n, err = m.Conn.Write(b)
	if n > 0 {
		if m.bytesWritten != nil {
			atomic.AddUint64(m.bytesWritten, uint64(n))
		}
		if m.lastActivity != nil {
			atomic.StoreInt64(m.lastActivity, time.Now().Unix())
		}
	}
	return
}

type NNTPConnection struct {
	conn net.Conn

	ctx    context.Context
	cancel context.CancelFunc

	reqCh   <-chan *Request
	pending chan *Request

	inflightSem chan struct{}

	rb readBuffer

	Greeting NNTPResponse

	done   chan struct{}
	doneMu sync.Once

	failMu sync.Once

	maxIdleTime  time.Duration
	lastActivity int64
	maxLifeTime  time.Duration
	createdAt    time.Time
}

func newNetConn(addr string, tlsConfig *tls.Config) (net.Conn, error) {
	dialer := &net.Dialer{
		Timeout: 30 * time.Second,
	}

	conn, err := dialer.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}

	// Optimize socket buffers for high-speed downloads (10Gbps+)
	if tcpConn, ok := conn.(*net.TCPConn); ok {
		// 8MB receive buffer
		_ = tcpConn.SetReadBuffer(8 * 1024 * 1024)
		// 1MB send buffer
		_ = tcpConn.SetWriteBuffer(1024 * 1024)
	}

	if tlsConfig != nil {
		return tls.Client(conn, tlsConfig), nil
	}
	return conn, nil
}

func newNNTPConnectionFromConn(ctx context.Context, conn net.Conn, inflightLimit int, reqCh <-chan *Request, auth Auth, maxIdleTime time.Duration, maxLifeTime time.Duration) (*NNTPConnection, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	cctx, cancel := context.WithCancel(ctx)

	c := &NNTPConnection{
		conn:         conn,
		ctx:          cctx,
		cancel:       cancel,
		reqCh:        reqCh,
		pending:      make(chan *Request, inflightLimit),
		inflightSem:  make(chan struct{}, inflightLimit),
		rb:           readBuffer{buf: make([]byte, defaultReadBufSize)},
		done:         make(chan struct{}),
		maxIdleTime:  maxIdleTime,
		lastActivity: time.Now().Unix(),
		maxLifeTime:  maxLifeTime,
		createdAt:    time.Now(),
	}

	if mc, ok := conn.(*MeteredConn); ok {
		mc.lastActivity = &c.lastActivity
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

func NewNNTPConnection(ctx context.Context, addr string, tlsConfig *tls.Config, inflightLimit int, reqCh <-chan *Request, auth Auth, maxIdleTime time.Duration, maxLifeTime time.Duration) (*NNTPConnection, error) {
	conn, err := newNetConn(addr, tlsConfig)
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

func (c *NNTPConnection) Done() <-chan struct{} { return c.done }

func (c *NNTPConnection) closeDone() {
	c.doneMu.Do(func() { close(c.done) })
}

func safeClose[T any](ch chan T) {
	defer func() { _ = recover() }()
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
				safeClose(req.RespCh)
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

type streamFeeder interface {
	Feed(in []byte, out io.Writer) (consumed int, done bool, err error)
}

type writerRef struct {
	w io.Writer
}

func (wr *writerRef) Write(p []byte) (int, error) {
	return wr.w.Write(p)
}

type writerRefAt struct {
	*writerRef
}

func (wr *writerRefAt) WriteAt(p []byte, off int64) (int, error) {
	if wr.w == io.Discard {
		return len(p), nil
	}
	if wa, ok := wr.w.(io.WriterAt); ok {
		return wa.WriteAt(p, off)
	}
	return 0, fmt.Errorf("underlying writer does not support WriteAt")
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
			<-c.inflightSem
			// We don't close req.RespCh here because it might have been picked up by readerLoop
			// (since we pushed to pending above).
			// Closing connection below will cause readerLoop to error and handle req.RespCh.
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
			return
		}
		if req.Ctx == nil {
			req.Ctx = context.Background()
		}

		resp := Response{
			Request: req,
		}
		decoder := NNTPResponse{
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
		outRef := &writerRef{w: out}
		var feederOut io.Writer = outRef
		if _, ok := out.(io.WriterAt); ok {
			feederOut = &writerRefAt{outRef}
		}

		err := c.rb.feedUntilDone(c.conn, &decoder, feederOut, func() (time.Time, bool) {
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
		if err != nil {
			resp.Err = err
		}

		resp.StatusCode = decoder.StatusCode
		resp.Status = decoder.Message
		resp.Lines = decoder.Lines
		resp.Meta = decoder

		if deliver {
			// Best effort: don't block forever if the receiver abandoned the channel.
			select {
			case req.RespCh <- resp:
			default:
			}
		}
		safeClose(req.RespCh)

		// release inflight slot
		<-c.inflightSem

		// If we hit a network error or IO error, close the connection.
		if resp.Err != nil {
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
	if err := c.rb.feedUntilDone(c.conn, &resp, out, func() (time.Time, bool) { return time.Time{}, false }); err != nil {
		return resp, err
	}
	return resp, nil
}
