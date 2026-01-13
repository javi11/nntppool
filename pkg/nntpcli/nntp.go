//go:generate go tool mockgen -source=./nntp.go -destination=./nntp_mock.go -package=nntpcli Client
package nntpcli

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"time"
)

// ContextDialer is a dialer that supports context.
// Use this for proxy connections or custom dialing behavior.
type ContextDialer interface {
	DialContext(ctx context.Context, network, address string) (net.Conn, error)
}

type DialConfig struct {
	KeepAliveTime time.Duration
	DialTimeout   time.Duration
	// Dialer is an optional custom dialer. If nil, net.Dialer is used.
	// Use this for proxy connections.
	Dialer ContextDialer
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
	config Config
}

// New creates a new NNTP client
//
// If no config is provided, the default config will be used
func New(
	c ...Config,
) Client {
	config := mergeWithDefault(c...)

	return &client{
		config: config,
	}
}

// setupTCPConn configures a TCP connection with keep-alive and other options.
// If cfg.Dialer is set, it will be used for dialing (e.g., for proxy connections).
func (c *client) setupTCPConn(ctx context.Context, host string, port int, cfg DialConfig) (net.Conn, time.Duration, error) {
	var dialer ContextDialer

	if cfg.Dialer != nil {
		dialer = cfg.Dialer
	} else {
		d := &net.Dialer{}
		if cfg.DialTimeout != 0 {
			d.Timeout = cfg.DialTimeout
		}
		dialer = d
	}

	conn, err := dialer.DialContext(ctx, "tcp", fmt.Sprintf("%s:%d", host, port))
	if err != nil {
		return nil, 0, err
	}

	keepAlive := c.config.KeepAliveTime
	if cfg.KeepAliveTime != 0 {
		keepAlive = cfg.KeepAliveTime
	}

	// Configure TCP options only for direct TCP connections (not proxied)
	if tcpConn, ok := conn.(*net.TCPConn); ok {
		// Set large socket buffers for high-bandwidth connections (10 Gbps+)
		if recvBuf := c.config.getSocketReceiveBuffer(); recvBuf > 0 {
			if err = tcpConn.SetReadBuffer(recvBuf); err != nil {
				return nil, 0, err
			}
		}

		if sendBuf := c.config.getSocketSendBuffer(); sendBuf > 0 {
			if err = tcpConn.SetWriteBuffer(sendBuf); err != nil {
				return nil, 0, err
			}
		}

		if err = tcpConn.SetKeepAlive(true); err != nil {
			return nil, 0, err
		}

		if err = tcpConn.SetKeepAlivePeriod(keepAlive); err != nil {
			return nil, 0, err
		}

		// Disable Nagle's algorithm for low latency
		if err = tcpConn.SetNoDelay(true); err != nil {
			return nil, 0, err
		}
	}

	return conn, keepAlive, nil
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

	conn, keepAlive, err := c.setupTCPConn(ctx, host, port, cfg)
	if err != nil {
		return nil, err
	}

	maxAgeTime := time.Now().Add(keepAlive)

	return newConnection(conn, maxAgeTime, c.config.OperationTimeout, c.config.getReadBufferSize())
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

	conn, keepAlive, err := c.setupTCPConn(ctx, host, port, cfg)
	if err != nil {
		return nil, err
	}

	var tlsConfig *tls.Config
	if c.config.TLSConfig != nil {
		// Use custom config if provided
		tlsConfig = c.config.TLSConfig.Clone()
		tlsConfig.ServerName = host
		if insecureSSL {
			tlsConfig.InsecureSkipVerify = true
		}
	} else {
		// Use optimized defaults
		tlsConfig = createOptimizedTLSConfig(host, insecureSSL)
	}

	tlsConn := tls.Client(conn, tlsConfig)

	err = tlsConn.Handshake()
	if err != nil {
		return nil, err
	}

	maxAgeTime := time.Now().Add(keepAlive)

	return newConnection(tlsConn, maxAgeTime, c.config.OperationTimeout, c.config.getReadBufferSize())
}

// createOptimizedTLSConfig creates a TLS config optimized for high throughput.
// Prefers TLS 1.3 and uses hardware-accelerated cipher suites (AES-GCM, ChaCha20).
func createOptimizedTLSConfig(serverName string, insecureSSL bool) *tls.Config {
	return &tls.Config{
		ServerName:         serverName,
		InsecureSkipVerify: insecureSSL,
		MinVersion:         tls.VersionTLS12,
		MaxVersion:         tls.VersionTLS13,
		// Hardware-accelerated cipher suites (AES-GCM uses AES-NI instructions)
		// TLS 1.3 ciphers are automatically preferred when available
		CipherSuites: []uint16{
			tls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,
			tls.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,
			tls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256,
			tls.TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384,
			tls.TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305,
			tls.TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305,
		},
	}
}
