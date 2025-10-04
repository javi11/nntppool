//go:generate .tools/mockgen -source=./nntp.go -destination=./nntp_mock.go -package=nntpcli Client
package nntpcli

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"time"
)

type DialConfig struct {
	KeepAliveTime time.Duration
	DialTimeout   time.Duration
}

type TimeData struct {
	Milliseconds int64
	Bytes        int
}

type Client interface {
	Dial(
		ctx context.Context,
		host string,
		port int,
		config ...DialConfig,
	) (Connection, error)
	DialTLS(
		ctx context.Context,
		host string,
		port int,
		insecureSSL bool,
		config ...DialConfig,
	) (Connection, error)
}

type client struct {
	keepAliveTime time.Duration
}

// New creates a new NNTP client
//
// If no config is provided, the default config will be used
func New(
	c ...Config,
) Client {
	config := mergeWithDefault(c...)

	return &client{
		keepAliveTime: config.KeepAliveTime,
	}
}

// Dial connects to an NNTP server using a plain TCP connection.
//
// Parameters:
//   - ctx: Context for controlling the connection lifecycle
//   - host: The hostname or IP address of the NNTP server
//   - port: The port number of the NNTP server
//   - keepAliveTime: Optional duration to override the default keep-alive time
//   - dialTimeout: Optional timeout duration for the initial connection
//
// Returns:
//   - Connection: An NNTP connection interface if successful
//   - error: Any error encountered during connection
func (c *client) Dial(
	ctx context.Context,
	host string,
	port int,
	config ...DialConfig,
) (Connection, error) {
	var cfg DialConfig
	if len(config) > 0 {
		cfg = config[0]
	}

	var d net.Dialer
	if cfg.DialTimeout != 0 {
		d = net.Dialer{Timeout: cfg.DialTimeout}
	}

	conn, err := d.DialContext(ctx, "tcp", fmt.Sprintf("%s:%d", host, port))
	if err != nil {
		return nil, err
	}

	tcpConn, ok := conn.(*net.TCPConn)
	if !ok {
		return nil, fmt.Errorf("expected TCP connection, got %T", conn)
	}

	err = tcpConn.SetKeepAlive(true)
	if err != nil {
		return nil, err
	}

	keepAlive := c.keepAliveTime
	if cfg.KeepAliveTime != 0 {
		keepAlive = cfg.KeepAliveTime
	}

	err = tcpConn.SetKeepAlivePeriod(keepAlive)
	if err != nil {
		return nil, err
	}

	err = tcpConn.SetNoDelay(true)
	if err != nil {
		return nil, err
	}

	maxAgeTime := time.Now().Add(keepAlive)

	return newConnection(conn, maxAgeTime)
}

// DialTLS connects to an NNTP server using a TLS-encrypted connection.
//
// Parameters:
//   - ctx: Context for controlling the connection lifecycle
//   - host: The hostname or IP address of the NNTP server
//   - port: The port number of the NNTP server
//   - insecureSSL: If true, skips verification of the server's certificate chain and host name
//   - keepAliveTime: Optional duration to override the default keep-alive time
//   - dialTimeout: Optional timeout duration for the initial connection
//
// Returns:
//   - Connection: An NNTP connection interface if successful
//   - error: Any error encountered during connection
func (c *client) DialTLS(
	ctx context.Context,
	host string,
	port int,
	insecureSSL bool,
	config ...DialConfig,
) (Connection, error) {
	var cfg DialConfig
	if len(config) > 0 {
		cfg = config[0]
	}

	var d net.Dialer
	if cfg.DialTimeout != 0 {
		d = net.Dialer{Timeout: cfg.DialTimeout}
	}

	conn, err := d.DialContext(ctx, "tcp", fmt.Sprintf("%s:%d", host, port))
	if err != nil {
		return nil, err
	}

	tcpConn, ok := conn.(*net.TCPConn)
	if !ok {
		return nil, fmt.Errorf("expected TCP connection, got %T", conn)
	}

	err = tcpConn.SetKeepAlive(true)
	if err != nil {
		return nil, err
	}

	keepAlive := c.keepAliveTime
	if cfg.KeepAliveTime != 0 {
		keepAlive = cfg.KeepAliveTime
	}

	err = tcpConn.SetKeepAlivePeriod(keepAlive)
	if err != nil {
		return nil, err
	}

	err = tcpConn.SetNoDelay(true)
	if err != nil {
		return nil, err
	}

	tlsConn := tls.Client(conn, &tls.Config{
		ServerName:         host,
		InsecureSkipVerify: insecureSSL,
	})

	err = tlsConn.Handshake()
	if err != nil {
		return nil, err
	}

	maxAgeTime := time.Now().Add(keepAlive)

	return newConnection(tlsConn, maxAgeTime)
}
