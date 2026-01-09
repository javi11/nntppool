//go:generate go tool mockgen -source=./pooled_connection.go -destination=./pooled_connection_mock.go -package=nntppool PooledConnection

// Package nntppool provides a connection pooling mechanism for NNTP connections.
package nntppool

import (
	"fmt"
	"net"

	"github.com/javi11/nntppool/v2/internal/netconn"
	"github.com/javi11/nntppool/v2/pkg/nntpcli"
	"github.com/yudhasubki/netpool"
)

var _ PooledConnection = (*pooledConnection)(nil)

// PooledConnection represents a managed NNTP connection from a connection pool.
// It wraps the underlying NNTP connection with pool management capabilities.
type PooledConnection interface {
	Connection() nntpcli.Connection
	Close() error
	Free() error
	Provider() ConnectionProviderInfo
}

type pooledConnection struct {
	conn     net.Conn
	nntpConn nntpcli.Connection
	provider UsenetProviderConfig
	pool     netpool.Netpooler
	log      Logger
	metrics  *PoolMetrics
}

// newPooledConnection creates a new pooled connection.
// The connection wraps a net.Conn managed by netpool and provides automatic
// release/destroy functionality with metrics tracking.
func newPooledConnection(
	conn net.Conn,
	provider UsenetProviderConfig,
	pool netpool.Netpooler,
	log Logger,
	metrics *PoolMetrics,
) pooledConnection {
	// Extract NNTP connection from wrapper
	var nntpConn nntpcli.Connection
	if wrapper, ok := conn.(*netconn.NNTPConnWrapper); ok {
		nntpConn = wrapper.NNTPConnection()
	}

	return pooledConnection{
		conn:     conn,
		nntpConn: nntpConn,
		provider: provider,
		pool:     pool,
		log:      log,
		metrics:  metrics,
	}
}

// connectionID returns a unique identifier for this connection based on its pointer.
// This provides a stable ID for metrics tracking without storing extra state.
func (p pooledConnection) connectionID() string {
	return fmt.Sprintf("%p", p.conn)
}

// Close destroys the connection and removes it from the pool.
// This method should be used when the connection is known to be in a bad state
// and should not be reused.
func (p pooledConnection) Close() error {
	if p.metrics != nil {
		p.metrics.UnregisterActiveConnection(p.connectionID())
		p.metrics.RecordConnectionDestroyed()
	}
	// Use PutWithError to signal netpool to close the connection
	p.pool.PutWithError(p.conn, fmt.Errorf("connection closed due to error"))
	return nil
}

// Free returns the connection to the pool for reuse.
// This method should be called when you're done using the connection but
// it's still in a good state.
func (p pooledConnection) Free() error {
	if p.metrics != nil {
		p.metrics.UnregisterActiveConnection(p.connectionID())
		p.metrics.RecordRelease()
	}
	p.pool.Put(p.conn)
	return nil
}

// Connection returns the underlying NNTP connection.
// This method provides access to the actual NNTP connection for performing
// NNTP operations. The returned connection should not be stored separately
// from the PooledConnection.
func (p pooledConnection) Connection() nntpcli.Connection {
	return p.nntpConn
}

// Provider returns the provider information associated with the connection.
// This method provides information about the provider that created the connection.
func (p pooledConnection) Provider() ConnectionProviderInfo {
	return ConnectionProviderInfo{
		Host:           p.provider.Host,
		Username:       p.provider.Username,
		MaxConnections: p.provider.MaxConnections,
		PipelineDepth:  p.provider.PipelineDepth,
		State:          ProviderStateActive, // Default state, actual state managed by pool
	}
}
