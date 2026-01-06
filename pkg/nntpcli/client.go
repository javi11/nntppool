package nntpcli

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"sync"
)

// Client manages a pool of NNTP connections to a single server.
// Requests are distributed across connections using a shared channel.
type Client struct {
	ctx    context.Context
	cancel context.CancelFunc

	reqCh chan *Request

	conns []*NNTPConnection
	wg    sync.WaitGroup
}

// NewClient creates a new NNTP client with the specified number of connections.
func NewClient(ctx context.Context, addr string, tlsConfig *tls.Config, connections, inflightPerConnection int, auth Auth) (*Client, error) {
	factory := func(_ context.Context) (net.Conn, error) {
		return newNetConn(addr, tlsConfig)
	}
	return NewClientWithConnFactory(ctx, connections, inflightPerConnection, auth, factory)
}

// NewClientWithConnFactory creates a new NNTP client using a custom connection factory.
// This is useful for testing or when custom connection logic is needed.
func NewClientWithConnFactory(ctx context.Context, connections, inflightPerConnection int, auth Auth, factory ConnFactory) (*Client, error) {
	if factory == nil {
		return nil, fmt.Errorf("nntpcli: ConnFactory is required")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	ctx, cancel := context.WithCancel(ctx)
	c := &Client{
		ctx:    ctx,
		cancel: cancel,
		reqCh:  make(chan *Request, connections),
		conns:  make([]*NNTPConnection, 0, connections),
	}

	for i := 0; i < connections; i++ {
		conn, err := factory(c.ctx)
		if err != nil {
			c.cancel()
			for _, cc := range c.conns {
				_ = cc.Close()
			}
			return nil, err
		}

		w, err := newNNTPConnectionFromConn(c.ctx, conn, inflightPerConnection, c.reqCh, auth)
		if err != nil {
			_ = conn.Close()
			c.cancel()
			for _, cc := range c.conns {
				_ = cc.Close()
			}
			return nil, err
		}
		c.conns = append(c.conns, w)
		c.wg.Add(1)
		go func() {
			defer c.wg.Done()
			w.Run()
		}()
	}

	return c, nil
}

// Close cancels the client, closes its request channel, and waits for all connections to stop.
func (c *Client) Close() error {
	c.cancel()
	close(c.reqCh)
	for _, cc := range c.conns {
		_ = cc.Close()
	}
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

// Connections returns the number of active connections.
func (c *Client) Connections() int {
	return len(c.conns)
}
