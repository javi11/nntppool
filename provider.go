package nntppool

import (
	"fmt"
	"sync"
	"time"

	"github.com/jackc/puddle/v2"
)

// ProviderState represents the lifecycle state of a provider during configuration changes
type ProviderState int

const (
	ProviderStateActive   ProviderState = iota // Normal operation
	ProviderStateDraining                      // Accepting no new connections, existing connections finishing
	ProviderStateMigrating                     // In process of updating configuration
	ProviderStateRemoving                      // Being removed from pool
)

func (ps ProviderState) String() string {
	switch ps {
	case ProviderStateActive:
		return "active"
	case ProviderStateDraining:
		return "draining"
	case ProviderStateMigrating:
		return "migrating"
	case ProviderStateRemoving:
		return "removing"
	default:
		return "unknown"
	}
}

type ConnectionProviderInfo struct {
	Host                     string        `json:"host"`
	Username                 string        `json:"username"`
	MaxConnections           int           `json:"maxConnections"`
	MaxConnectionIdleTimeout time.Duration `json:"maxConnectionIdleTimeout"`
	State                    ProviderState `json:"state"`
}

func (p ConnectionProviderInfo) ID() string {
	return providerID(p.Host, p.Username)
}

type ProviderInfo struct {
	Host                     string        `json:"host"`
	Username                 string        `json:"username"`
	UsedConnections          int           `json:"usedConnections"`
	MaxConnections           int           `json:"maxConnections"`
	MaxConnectionIdleTimeout time.Duration `json:"maxConnectionIdleTimeout"`
}

func (p ProviderInfo) ID() string {
	return providerID(p.Host, p.Username)
}

type providerPool struct {
	connectionPool *puddle.Pool[*internalConnection]
	provider       UsenetProviderConfig
	state          ProviderState
	stateMu        sync.RWMutex  // Protects state changes
	drainStarted   time.Time     // When draining started
	migrationID    string        // ID for tracking migration operations
}

func providerID(host, username string) string {
	return fmt.Sprintf("%s_%s", host, username)
}

type Provider struct {
	Host                           string
	Username                       string
	Password                       string
	Port                           int
	MaxConnections                 int
	MaxConnectionIdleTimeInSeconds int
	IsBackupProvider               bool
}

func (p *Provider) ID() string {
	return p.Host
}

// Provider pool state management methods
func (pp *providerPool) GetState() ProviderState {
	pp.stateMu.RLock()
	defer pp.stateMu.RUnlock()
	return pp.state
}

func (pp *providerPool) SetState(state ProviderState) {
	pp.stateMu.Lock()
	defer pp.stateMu.Unlock()
	
	oldState := pp.state
	pp.state = state
	
	// Track drain start time
	if state == ProviderStateDraining && oldState != ProviderStateDraining {
		pp.drainStarted = time.Now()
	}
}

func (pp *providerPool) IsAcceptingConnections() bool {
	state := pp.GetState()
	return state == ProviderStateActive || state == ProviderStateMigrating
}

func (pp *providerPool) GetDrainDuration() time.Duration {
	pp.stateMu.RLock()
	defer pp.stateMu.RUnlock()
	
	if pp.state != ProviderStateDraining || pp.drainStarted.IsZero() {
		return 0
	}
	return time.Since(pp.drainStarted)
}

func (pp *providerPool) SetMigrationID(id string) {
	pp.stateMu.Lock()
	defer pp.stateMu.Unlock()
	pp.migrationID = id
}

func (pp *providerPool) GetMigrationID() string {
	pp.stateMu.RLock()
	defer pp.stateMu.RUnlock()
	return pp.migrationID
}
