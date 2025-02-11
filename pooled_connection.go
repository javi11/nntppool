//go:generate .tools/mockgen -source=./pooled_connection.go -destination=./pooled_connection_mock.go -package=connectionpool PooledConnection

// Package connectionpool provides a connection pooling mechanism for NNTP connections.
package connectionpool

import (
	"fmt"
	"time"

	"github.com/jackc/puddle/v2"
	"github.com/javi11/nntpcli"
)

type internalConnection struct {
	nntp     nntpcli.Connection   // 8 bytes
	provider UsenetProviderConfig // 128 bytes
}

var _ PooledConnection = (*pooledConnection)(nil)

// PooledConnection represents a managed NNTP connection from a connection pool.
// It wraps the underlying NNTP connection with pool management capabilities.
type PooledConnection interface {
	Connection() nntpcli.Connection
	Close() error
	Free()
	Provider() ConnectionProviderInfo
	CreatedAt() time.Time
}

type pooledConnection struct {
	resource *puddle.Resource[*internalConnection]
	log      Logger
}

// Close destroys the connection and removes it from the pool.
// This method should be used when the connection is known to be in a bad state
// and should not be reused. It implements proper panic recovery to handle
// cases where the connection was already released.
func (p pooledConnection) Close() error {
	defer func() { // recover from panics
		if err := recover(); err != nil {
			p.log.Warn(fmt.Sprintf("can not close a connection already released: %v", err))
		}
	}()

	p.resource.Destroy()

	return nil
}

// Free returns the connection to the pool for reuse.
// This method should be called when you're done using the connection but
// it's still in a good state. It implements proper panic recovery to handle
// cases where the connection was already released.
func (p pooledConnection) Free() {
	defer func() { // recover from panics
		if err := recover(); err != nil {
			p.log.Warn(fmt.Sprintf("can not free a connection already released: %v", err))
		}
	}()

	p.resource.Release()
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
	}
}
