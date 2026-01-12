package nntpcli

import (
	"time"
)

// DefaultReadBufferSize is the default size for the read buffer (64KB).
const DefaultReadBufferSize = 64 * 1024

type Config struct {
	// KeepAliveTime is the time that the client will keep the connection alive.
	KeepAliveTime time.Duration
	// OperationTimeout is the timeout for NNTP operations.
	// Set to 0 to disable timeouts.
	OperationTimeout time.Duration
	// ReadBufferSize is the size of the read buffer for NNTP operations.
	// Larger buffers reduce syscalls but use more memory per connection.
	// Default: 64KB (65536). Set to 0 to use the default.
	ReadBufferSize int
}

// getReadBufferSize returns the configured read buffer size or the default.
func (c *Config) getReadBufferSize() int {
	if c.ReadBufferSize <= 0 {
		return DefaultReadBufferSize
	}
	return c.ReadBufferSize
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
