package provider

import (
	"fmt"
	"sync"
	"time"
)

// State represents the lifecycle state of a provider during configuration changes
type State int

const (
	StateActive    State = iota // Normal operation
	StateDraining               // Accepting no new connections, existing connections finishing
	StateMigrating              // In process of updating configuration
	StateRemoving               // Being removed from pool
)

func (ps State) String() string {
	switch ps {
	case StateActive:
		return "active"
	case StateDraining:
		return "draining"
	case StateMigrating:
		return "migrating"
	case StateRemoving:
		return "removing"
	default:
		return "unknown"
	}
}

// Config represents the configuration for a single provider
type Config interface {
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
}

// ConnectionPoolInterface represents the underlying connection pool
type ConnectionPoolInterface interface {
	Acquire(ctx interface{}) (interface{}, error)
	AcquireAllIdle() []interface{}
	Close()
	Stat() PoolStats
}

// PoolStats represents connection pool statistics
type PoolStats struct {
	TotalResources    int
	AcquiredResources int
}

// Pool manages a connection pool for a single provider
type Pool struct {
	connectionPool ConnectionPoolInterface
	provider       Config
	state          State
	stateMu        sync.RWMutex // Protects state changes
	drainStarted   time.Time    // When draining started
}

// NewPool creates a new provider pool
func NewPool(connectionPool ConnectionPoolInterface, provider Config) *Pool {
	return &Pool{
		connectionPool: connectionPool,
		provider:       provider,
		state:          StateActive,
	}
}

// GetProvider returns the provider configuration
func (pp *Pool) GetProvider() Config {
	return pp.provider
}

// GetConnectionPool returns the underlying connection pool
func (pp *Pool) GetConnectionPool() ConnectionPoolInterface {
	return pp.connectionPool
}

// GetStats returns pool statistics
func (pp *Pool) GetStats() PoolStats {
	return pp.connectionPool.Stat()
}

// GetState returns the current state of the provider pool
func (pp *Pool) GetState() State {
	pp.stateMu.RLock()
	defer pp.stateMu.RUnlock()
	return pp.state
}

// SetState sets the provider pool state
func (pp *Pool) SetState(state State) {
	pp.stateMu.Lock()
	defer pp.stateMu.Unlock()

	oldState := pp.state
	pp.state = state

	// Track drain start time
	if state == StateDraining && oldState != StateDraining {
		pp.drainStarted = time.Now()
	}
}

// IsAcceptingConnections returns whether the pool is accepting new connections
func (pp *Pool) IsAcceptingConnections() bool {
	state := pp.GetState()
	return state == StateActive || state == StateMigrating
}

// GetDrainDuration returns how long the provider has been draining
func (pp *Pool) GetDrainDuration() time.Duration {
	pp.stateMu.RLock()
	defer pp.stateMu.RUnlock()

	if pp.state != StateDraining || pp.drainStarted.IsZero() {
		return 0
	}
	return time.Since(pp.drainStarted)
}

// Close closes the provider pool
func (pp *Pool) Close() {
	pp.connectionPool.Close()
}

// ID generates a unique identifier for a provider based on host and username
func ID(host, username string) string {
	return fmt.Sprintf("%s_%s", host, username)
}
