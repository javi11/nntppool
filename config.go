package nntppool

import (
	"crypto/tls"
	"fmt"
	"time"
)

// Default configuration values.
const (
	DefaultMaxConnections      = 10
	DefaultInflightPerConn     = 1
	DefaultConnectTimeout      = 30 * time.Second
	DefaultReadTimeout         = 60 * time.Second
	DefaultWriteTimeout        = 30 * time.Second
	DefaultHealthCheckInterval = 1 * time.Minute
	DefaultIdleTimeout         = 5 * time.Minute
	DefaultWarmupConnections   = 0 // Fully lazy by default
	DefaultPort                = 119
	DefaultTLSPort             = 563
)

// ProviderConfig defines the configuration for a single NNTP provider.
//
// # Performance Tuning
//
// For optimal throughput, configure MaxConnections based on your concurrency needs:
//
//	Total concurrent requests = MaxConnections × InflightPerConn
//
// If using N goroutines to download articles concurrently, ensure:
//
//	MaxConnections × InflightPerConn >= N
//
// When using io.Writer implementations with buffering (like bufio.Writer wrapping
// an io.Pipe), use larger buffer sizes (e.g., 256KB) and ensure Flush() is called
// after each article download completes. The pool automatically flushes writers
// that implement the Flush() method.
type ProviderConfig struct {
	// Name is a human-readable identifier for the provider.
	Name string

	// Host is the NNTP server hostname.
	Host string

	// Port is the NNTP server port. Defaults to 119 (or 563 for TLS).
	Port int

	// TLS enables TLS encryption for the connection.
	TLS bool

	// TLSConfig is optional custom TLS configuration.
	// If nil, a default config will be used when TLS is enabled.
	TLSConfig *tls.Config

	// Username for NNTP authentication.
	Username string

	// Password for NNTP authentication.
	Password string

	// MaxConnections is the maximum number of concurrent connections to this provider.
	// For best throughput, set this to at least the number of concurrent goroutines
	// that will be calling Body() or BodyReader().
	MaxConnections int

	// Priority determines the order in which providers are tried.
	// Lower values = higher priority. Providers with the same priority
	// are selected using round-robin.
	Priority int

	// IsBackup indicates this is a backup provider.
	// Backup providers are only used after the initial request fails on primary providers.
	IsBackup bool

	// InflightPerConn is the maximum number of pipelined requests per connection.
	// Default is 1 (no pipelining). Increase for providers that support pipelining
	// to improve throughput.
	InflightPerConn int

	// ConnectTimeout is the timeout for establishing a connection.
	ConnectTimeout time.Duration

	// ReadTimeout is the timeout for reading responses.
	ReadTimeout time.Duration

	// WriteTimeout is the timeout for sending requests.
	WriteTimeout time.Duration

	// IdleTimeout is how long a connection can be idle before being closed.
	// Set to 0 to disable idle timeout and keep connections open indefinitely.
	// Default: 5 minutes.
	IdleTimeout time.Duration

	// WarmupConnections is the number of connections to pre-create at startup.
	// Set to 0 for fully lazy connection creation (connections created on-demand).
	// Default: 0 (fully lazy).
	WarmupConnections int
}

// Validate checks the provider configuration for errors.
func (c *ProviderConfig) Validate() error {
	if c.Host == "" {
		return fmt.Errorf("%w: host is required", ErrInvalidConfig)
	}
	if c.MaxConnections < 0 {
		return fmt.Errorf("%w: max connections cannot be negative", ErrInvalidConfig)
	}
	if c.InflightPerConn < 0 {
		return fmt.Errorf("%w: inflight per connection cannot be negative", ErrInvalidConfig)
	}
	if c.Priority < 0 {
		return fmt.Errorf("%w: priority cannot be negative", ErrInvalidConfig)
	}
	return nil
}

// WithDefaults returns a copy of the config with default values applied.
func (c ProviderConfig) WithDefaults() ProviderConfig {
	if c.Port == 0 {
		if c.TLS {
			c.Port = DefaultTLSPort
		} else {
			c.Port = DefaultPort
		}
	}
	if c.MaxConnections == 0 {
		c.MaxConnections = DefaultMaxConnections
	}
	if c.InflightPerConn == 0 {
		c.InflightPerConn = DefaultInflightPerConn
	}
	if c.ConnectTimeout == 0 {
		c.ConnectTimeout = DefaultConnectTimeout
	}
	if c.ReadTimeout == 0 {
		c.ReadTimeout = DefaultReadTimeout
	}
	if c.WriteTimeout == 0 {
		c.WriteTimeout = DefaultWriteTimeout
	}
	if c.Name == "" {
		c.Name = c.Host
	}
	// Note: IdleTimeout defaults to 0 (no idle timeout) for backward compatibility.
	// Users can set IdleTimeout > 0 to enable automatic connection cleanup.
	// WarmupConnections defaults to 0 (fully lazy connection creation).
	return c
}

// Address returns the host:port address for this provider.
func (c *ProviderConfig) Address() string {
	return fmt.Sprintf("%s:%d", c.Host, c.Port)
}

// PoolConfig defines the configuration for the connection pool.
type PoolConfig struct {
	// Providers is the list of NNTP providers.
	Providers []ProviderConfig

	// HealthCheckInterval is how often to check provider health.
	// Set to 0 to disable health checks.
	HealthCheckInterval time.Duration

	// MaxRetries is the maximum number of retry attempts within the same provider
	// for transient errors. Set to 0 for no retries.
	MaxRetries int
}

// Validate checks the pool configuration for errors.
func (c *PoolConfig) Validate() error {
	if len(c.Providers) == 0 {
		return fmt.Errorf("%w: at least one provider is required", ErrInvalidConfig)
	}

	hasPrimary := false
	for i, p := range c.Providers {
		if err := p.Validate(); err != nil {
			return fmt.Errorf("%w: provider %d (%s): %v", ErrInvalidConfig, i, p.Name, err)
		}
		if !p.IsBackup {
			hasPrimary = true
		}
	}

	if !hasPrimary {
		return fmt.Errorf("%w: at least one primary provider is required", ErrInvalidConfig)
	}

	return nil
}

// WithDefaults returns a copy of the config with default values applied.
func (c PoolConfig) WithDefaults() PoolConfig {
	if c.HealthCheckInterval == 0 {
		c.HealthCheckInterval = DefaultHealthCheckInterval
	}

	providers := make([]ProviderConfig, len(c.Providers))
	for i, p := range c.Providers {
		providers[i] = p.WithDefaults()
	}
	c.Providers = providers

	return c
}
