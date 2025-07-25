package helpers

import (
	"context"
	"errors"
	"fmt"
	"net"
	"slices"
	"time"

	"github.com/hashicorp/go-multierror"
)

// Logger interface for logging operations
type Logger interface {
	Info(msg string, args ...interface{})
	Error(msg string, args ...interface{})
	Warn(msg string, args ...interface{})
	Debug(msg string, args ...interface{})
}

// NNTPClient interface for NNTP operations
type NNTPClient interface {
	Dial(ctx context.Context, host string, port int, config DialConfig) (NNTPConnection, error)
	DialTLS(ctx context.Context, host string, port int, insecureSSL bool, config DialConfig) (NNTPConnection, error)
}

// NNTPConnection interface for NNTP connection operations
type NNTPConnection interface {
	Capabilities() ([]string, error)
	Authenticate(username, password string) error
	Close() error
	MaxAgeTime() time.Time
}

// DialConfig represents configuration for dialing NNTP connections
type DialConfig struct {
	KeepAliveTime time.Duration
}

// ProviderConfig represents the configuration for a single provider
type ProviderConfig interface {
	ID() string
	GetHost() string
	GetUsername() string
	GetPassword() string
	GetPort() int
	GetMaxConnections() int
	GetMaxConnectionIdleTimeInSeconds() int
	GetMaxConnectionTTLInSeconds() int
	GetTLS() bool
	GetInsecureSSL() bool
	GetIsBackupProvider() bool
	GetVerifyCapabilities() []string
}

// ConnectionPoolInterface represents a connection pool
type ConnectionPoolInterface interface {
	Acquire(ctx context.Context) (ConnectionResourceInterface, error)
}

// ConnectionResourceInterface represents a pooled connection resource
type ConnectionResourceInterface interface {
	Value() ConnectionValueInterface
	Release()
}

// ConnectionValueInterface represents the actual connection value
type ConnectionValueInterface interface {
	GetNNTPConnection() NNTPConnection
	GetProvider() ProviderConfig
}

// ProviderPoolInterface represents a provider's connection pool
type ProviderPoolInterface interface {
	GetConnectionPool() ConnectionPoolInterface
	GetProvider() ProviderConfig
}

// InternalConnection represents an internal connection with additional metadata
type InternalConnection struct {
	nntp                 NNTPConnection
	provider             ProviderConfig
	leaseExpiry          time.Time
	markedForReplacement bool
}

// NewInternalConnection creates a new internal connection
func NewInternalConnection(nntp NNTPConnection, provider ProviderConfig, leaseExpiry time.Time) *InternalConnection {
	return &InternalConnection{
		nntp:                 nntp,
		provider:             provider,
		leaseExpiry:          leaseExpiry,
		markedForReplacement: false,
	}
}

// GetNNTPConnection returns the NNTP connection
func (ic *InternalConnection) GetNNTPConnection() NNTPConnection {
	return ic.nntp
}

// GetProvider returns the provider configuration
func (ic *InternalConnection) GetProvider() ProviderConfig {
	return ic.provider
}

// GetLeaseExpiry returns the lease expiry time
func (ic *InternalConnection) GetLeaseExpiry() time.Time {
	return ic.leaseExpiry
}

// IsMarkedForReplacement returns whether the connection is marked for replacement
func (ic *InternalConnection) IsMarkedForReplacement() bool {
	return ic.markedForReplacement
}

// SetMarkedForReplacement sets the replacement flag
func (ic *InternalConnection) SetMarkedForReplacement(marked bool) {
	ic.markedForReplacement = marked
}

// JoinGroup attempts to join one of the provided groups
func JoinGroup(c NNTPConnection, groups []string) error {
	var err error

	for _, g := range groups {
		if err = joinSingleGroup(c, g); err == nil {
			return nil
		}
	}

	return err
}

// joinSingleGroup is a placeholder for joining a single group
func joinSingleGroup(c NNTPConnection, group string) error {
	// This would need to be implemented with actual NNTP join logic
	// For now, return nil to indicate success
	return nil
}

// VerifyProviders verifies that all provider pools are working correctly
func VerifyProviders(pools []ProviderPoolInterface, log Logger) error {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	wg := multierror.Group{}

	for _, pool := range pools {
		wg.Go(func() error {
			c, err := pool.GetConnectionPool().Acquire(ctx)
			if err != nil {
				return fmt.Errorf("failed to verify download provider %s: %w", pool.GetProvider().GetHost(), err)
			}

			defer c.Release()

			conn := c.Value()
			caps, _ := conn.GetNNTPConnection().Capabilities()

			log.Info(fmt.Sprintf("capabilities for provider %s: %v", pool.GetProvider().GetHost(), caps))

			if len(pool.GetProvider().GetVerifyCapabilities()) > 0 {
				for _, cap := range pool.GetProvider().GetVerifyCapabilities() {
					if !slices.Contains(caps, cap) {
						return fmt.Errorf("provider %s does not support capability %s", pool.GetProvider().GetHost(), cap)
					}
				}
			}

			return nil
		})
	}

	return wg.Wait().ErrorOrNil()
}

// DialNNTP establishes an NNTP connection to a provider
func DialNNTP(ctx context.Context, cli NNTPClient, p ProviderConfig, log Logger) (NNTPConnection, error) {
	var (
		c   NNTPConnection
		err error
	)

	log.Debug(fmt.Sprintf("connecting to %s:%v", p.GetHost(), p.GetPort()))

	ttl := time.Duration(p.GetMaxConnectionTTLInSeconds()) * time.Second

	if p.GetTLS() {
		c, err = cli.DialTLS(
			ctx,
			p.GetHost(),
			p.GetPort(),
			p.GetInsecureSSL(),
			DialConfig{
				KeepAliveTime: ttl,
			},
		)
		if err != nil {
			var netErr net.Error

			if errors.As(err, &netErr) && netErr.Timeout() {
				log.Error(fmt.Sprintf("timeout connecting to %s:%v, retrying", p.GetHost(), p.GetPort()), "error", netErr)
			}

			return nil, fmt.Errorf("error dialing to %v/%v TLS: %w", p.GetHost(), p.GetUsername(), err)
		}
	} else {
		var err error

		c, err = cli.Dial(
			ctx,
			p.GetHost(),
			p.GetPort(),
			DialConfig{
				KeepAliveTime: ttl,
			},
		)
		if err != nil {
			var netErr net.Error

			if errors.As(err, &netErr) && netErr.Timeout() {
				log.Error(fmt.Sprintf("timeout connecting to %s:%v, retrying", p.GetHost(), p.GetPort()), "error", netErr)
			}

			return nil, fmt.Errorf("error dialing to %v/%v: %w", p.GetHost(), p.GetUsername(), err)
		}
	}

	if p.GetUsername() != "" && p.GetPassword() != "" {
		if err := c.Authenticate(p.GetUsername(), p.GetPassword()); err != nil {
			return nil, fmt.Errorf("error authenticating to %v/%v: %w", p.GetHost(), p.GetUsername(), err)
		}
	}

	return c, nil
}