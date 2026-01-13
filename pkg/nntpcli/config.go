package nntpcli

import (
	"crypto/tls"
	"time"
)

// Buffer size defaults optimized for 10 Gbps+ throughput
const (
	// DefaultReadBufferSize is the default size for the read buffer (256KB).
	// Increased from 64KB to match high-performance NNTP clients.
	DefaultReadBufferSize = 256 * 1024
	// DefaultSocketReceiveBuffer is the default size for the socket receive buffer (8MB).
	// Large buffer for high-bandwidth downloads on 10 Gbps connections.
	DefaultSocketReceiveBuffer = 8 * 1024 * 1024
	// DefaultSocketSendBuffer is the default size for the socket send buffer (1MB).
	DefaultSocketSendBuffer = 1024 * 1024
)

type Config struct {
	// KeepAliveTime is the time that the client will keep the connection alive.
	KeepAliveTime time.Duration
	// OperationTimeout is the timeout for NNTP operations.
	// Set to 0 to disable timeouts.
	OperationTimeout time.Duration
	// ReadBufferSize is the size of the read buffer for NNTP operations.
	// Larger buffers reduce syscalls but use more memory per connection.
	// Default: 256KB (262144). Set to 0 to use the default.
	ReadBufferSize int
	// SocketReceiveBuffer is the size of the TCP socket receive buffer.
	// Larger buffers improve throughput on high-bandwidth connections.
	// Default: 8MB (8388608). Set to 0 to use the default.
	SocketReceiveBuffer int
	// SocketSendBuffer is the size of the TCP socket send buffer.
	// Default: 1MB (1048576). Set to 0 to use the default.
	SocketSendBuffer int
	// TLSConfig is an optional custom TLS configuration.
	// If nil, an optimized TLS config will be used (TLS 1.3 preferred, hardware-accelerated ciphers).
	TLSConfig *tls.Config
}

// getReadBufferSize returns the configured read buffer size or the default.
func (c *Config) getReadBufferSize() int {
	if c.ReadBufferSize <= 0 {
		return DefaultReadBufferSize
	}
	return c.ReadBufferSize
}

// getSocketReceiveBuffer returns the configured socket receive buffer size or the default.
func (c *Config) getSocketReceiveBuffer() int {
	if c.SocketReceiveBuffer <= 0 {
		return DefaultSocketReceiveBuffer
	}
	return c.SocketReceiveBuffer
}

// getSocketSendBuffer returns the configured socket send buffer size or the default.
func (c *Config) getSocketSendBuffer() int {
	if c.SocketSendBuffer <= 0 {
		return DefaultSocketSendBuffer
	}
	return c.SocketSendBuffer
}

type Option func(*Config)

var configDefault = Config{
	KeepAliveTime:    10 * time.Minute,
	OperationTimeout: 30 * time.Second,
}

func mergeWithDefault(config ...Config) Config {
	if len(config) == 0 {
		return configDefault
	}

	cfg := config[0]

	if cfg.KeepAliveTime == 0 {
		cfg.KeepAliveTime = configDefault.KeepAliveTime
	}

	if cfg.OperationTimeout == 0 {
		cfg.OperationTimeout = configDefault.OperationTimeout
	}

	return cfg
}
