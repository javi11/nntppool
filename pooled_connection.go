//go:generate go tool mockgen -source=./pooled_connection.go -destination=./pooled_connection_mock.go -package=nntppool PooledConnection

// Package nntppool provides a connection pooling mechanism for NNTP connections.
package nntppool

import (
	"fmt"
	"sync/atomic"
	"time"

	"github.com/jackc/puddle/v2"
	"github.com/javi11/nntppool/v2/pkg/nntpcli"
)

// globalConnectionID is an atomic counter used to generate unique connection IDs.
// Each new connection increments this counter to get a deterministic, human-readable ID.
var globalConnectionID atomic.Uint64

type internalConnection struct {
	nntp                 nntpcli.Connection   // 8 bytes
	provider             UsenetProviderConfig // 128 bytes
	leaseExpiry          time.Time            // When this connection can be replaced
	markedForReplacement bool                 // Whether this connection should be replaced when idle
}

var _ PooledConnection = (*pooledConnection)(nil)

// PooledConnection represents a managed NNTP connection from a connection pool.
// It wraps the underlying NNTP connection with pool management capabilities.
type PooledConnection interface {
	Connection() nntpcli.Connection
	Close() error
	Free() error
	Provider() ConnectionProviderInfo
	CreatedAt() time.Time
	IsLeaseExpired() bool
	IsMarkedForReplacement() bool
	MarkForReplacement()
	ExtendLease(duration time.Duration)
}

type pooledConnection struct {
	resource *puddle.Resource[*internalConnection]
	log      Logger
	metrics  *PoolMetrics
	id       string // Unique identifier for this connection (e.g., "conn-1", "conn-2")
}

// newPooledConnection creates a new pooled connection with a unique ID.
// The ID is generated using an atomic counter, ensuring deterministic and
// human-readable connection identifiers (e.g., "conn-1", "conn-2").
func newPooledConnection(resource *puddle.Resource[*internalConnection], log Logger, metrics *PoolMetrics) pooledConnection {
	// Generate a unique ID using atomic counter
	id := fmt.Sprintf("conn-%d", globalConnectionID.Add(1))

	return pooledConnection{
		resource: resource,
		log:      log,
		metrics:  metrics,
		id:       id,
	}
}

// connectionID returns the unique identifier for this connection.
// The ID is assigned when the connection is created and remains constant
// throughout the connection's lifetime.
func (p pooledConnection) connectionID() string {
	return p.id
}

// Close destroys the connection and removes it from the pool.
// This method should be used when the connection is known to be in a bad state
// and should not be reused. It implements proper panic recovery to handle
// cases where the connection was already released.
func (p pooledConnection) Close() error {
	var resultErr error
	destroyed := false

	defer func() {
		// Recover from panics
		if err := recover(); err != nil {
			errorMsg := fmt.Sprintf("can not close a connection already released: %v", err)
			p.log.Warn(errorMsg)
			if resultErr == nil {
				resultErr = fmt.Errorf("can not close a connection already released: %v", err)
			}
		}

		// Update metrics only if destruction succeeded (after defer recovery)
		if destroyed && p.metrics != nil {
			p.metrics.UnregisterActiveConnection(p.connectionID())
			p.metrics.RecordConnectionDestroyed()
		}
	}()

	p.resource.Destroy()
	destroyed = true

	return resultErr
}

// Free returns the connection to the pool for reuse.
// This method should be called when you're done using the connection but
// it's still in a good state. It implements proper panic recovery to handle
// cases where the connection was already released.
func (p pooledConnection) Free() error {
	var resultErr error
	released := false

	defer func() {
		// Recover from panics
		if err := recover(); err != nil {
			errorMsg := fmt.Sprintf("can not free a connection already released: %v", err)
			p.log.Warn(errorMsg)
			if resultErr == nil {
				resultErr = fmt.Errorf("can not free a connection already released: %v", err)
			}
		}

		// Update metrics only if release succeeded (after defer recovery)
		if released && p.metrics != nil {
			p.metrics.UnregisterActiveConnection(p.connectionID())
			p.metrics.RecordRelease()
		}
	}()

	p.resource.Release()
	released = true

	return resultErr
}

// Connection returns the underlying NNTP connection.
// This method provides access to the actual NNTP connection for performing
// NNTP operations. The returned connection should not be stored separately
// from the PooledConnection.
func (p pooledConnection) Connection() nntpcli.Connection {
	return p.resource.Value().nntp
}

// Raw returns the underlying resource from the connection pool.
// This method provides access to the underlying resource from the connection pool.
func (p pooledConnection) Raw() *puddle.Resource[*internalConnection] {
	return p.resource
}

// CreatedAt returns the time when the connection was created.
// This method provides information about when the connection was created.
func (p pooledConnection) CreatedAt() time.Time {
	return p.resource.CreationTime()
}

// Provider returns the provider information associated with the connection.
// This method provides information about the provider that created the connection.
func (p pooledConnection) Provider() ConnectionProviderInfo {
	prov := p.resource.Value().provider

	return ConnectionProviderInfo{
		Host:                     prov.Host,
		Username:                 prov.Username,
		MaxConnections:           prov.MaxConnections,
		MaxConnectionIdleTimeout: time.Duration(prov.MaxConnectionIdleTimeInSeconds) * time.Second,
		State:                    ProviderStateActive, // Default state, actual state managed by pool
	}
}

// IsLeaseExpired returns true if the connection's lease has expired and can be replaced
func (p pooledConnection) IsLeaseExpired() bool {
	conn := p.resource.Value()
	return time.Now().After(conn.leaseExpiry)
}

// IsMarkedForReplacement returns true if this connection should be replaced when it becomes idle
func (p pooledConnection) IsMarkedForReplacement() bool {
	conn := p.resource.Value()
	return conn.markedForReplacement
}

// MarkForReplacement marks this connection to be replaced when it becomes idle
func (p pooledConnection) MarkForReplacement() {
	conn := p.resource.Value()
	conn.markedForReplacement = true
	p.log.Debug("Connection marked for replacement",
		"provider", conn.provider.Host,
		"created_at", p.resource.CreationTime(),
	)
}

// ExtendLease extends the connection's lease by the specified duration
func (p pooledConnection) ExtendLease(duration time.Duration) {
	conn := p.resource.Value()
	conn.leaseExpiry = time.Now().Add(duration)
	p.log.Debug("Connection lease extended",
		"provider", conn.provider.Host,
		"new_expiry", conn.leaseExpiry,
	)
}
