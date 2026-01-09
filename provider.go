package nntppool

import (
	"fmt"
	"sync"
	"time"

	"github.com/yudhasubki/netpool"
)

// ProviderState represents the lifecycle state of a provider during configuration changes
type ProviderState int

const (
	ProviderStateActive               ProviderState = iota // Normal operation
	ProviderStateOffline                                   // Provider is offline/unreachable
	ProviderStateReconnecting                              // Currently attempting to reconnect
	ProviderStateAuthenticationFailed                      // Authentication failed, won't retry
)

func (ps ProviderState) String() string {
	switch ps {
	case ProviderStateActive:
		return "active"
	case ProviderStateOffline:
		return "offline"
	case ProviderStateReconnecting:
		return "reconnecting"
	case ProviderStateAuthenticationFailed:
		return "authentication_failed"
	default:
		return "unknown"
	}
}

type ConnectionProviderInfo struct {
	Host           string        `json:"host"`
	Username       string        `json:"username"`
	MaxConnections int           `json:"maxConnections"`
	PipelineDepth  int           `json:"pipelineDepth"`
	State          ProviderState `json:"state"`
}

func (p ConnectionProviderInfo) ID() string {
	return providerID(p.Host, p.Username)
}

type ProviderInfo struct {
	Host                  string        `json:"host"`
	Username              string        `json:"username"`
	UsedConnections       int           `json:"usedConnections"`
	MaxConnections        int           `json:"maxConnections"`
	State                 ProviderState `json:"state"`
	LastConnectionAttempt time.Time     `json:"lastConnectionAttempt"`
	LastSuccessfulConnect time.Time     `json:"lastSuccessfulConnect"`
	FailureReason         string        `json:"failureReason"`
	RetryCount            int           `json:"retryCount"`
	NextRetryAt           time.Time     `json:"nextRetryAt"`
}

func (p ProviderInfo) ID() string {
	return providerID(p.Host, p.Username)
}

type providerPool struct {
	connectionPool        netpool.Netpooler
	provider              UsenetProviderConfig
	state                 ProviderState
	stateMu               sync.RWMutex // Protects state changes
	lastConnectionAttempt time.Time    // Last time connection was attempted
	lastSuccessfulConnect time.Time    // Last successful connection
	failureReason         string       // Reason for last failure
	retryCount            int          // Number of retry attempts
	nextRetryAt           time.Time    // When next retry should happen
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

	// Reset retry scheduling when state changes
	if oldState != state {
		pp.nextRetryAt = time.Time{}
	}
}

func (pp *providerPool) IsAcceptingConnections() bool {
	state := pp.GetState()
	return state == ProviderStateActive
}

// SetConnectionAttempt records a connection attempt
func (pp *providerPool) SetConnectionAttempt(err error) {
	pp.stateMu.Lock()
	defer pp.stateMu.Unlock()

	pp.lastConnectionAttempt = time.Now()
	if err != nil {
		pp.failureReason = err.Error()
		pp.retryCount++
	} else {
		pp.lastSuccessfulConnect = time.Now()
		pp.failureReason = ""
		pp.retryCount = 0
	}
}

// SetNextRetryAt sets when the next retry should happen
func (pp *providerPool) SetNextRetryAt(retryAt time.Time) {
	pp.stateMu.Lock()
	defer pp.stateMu.Unlock()
	pp.nextRetryAt = retryAt
}

// GetConnectionStatus returns connectivity status information
func (pp *providerPool) GetConnectionStatus() (lastAttempt, lastSuccess, nextRetry time.Time, reason string, retries int) {
	pp.stateMu.RLock()
	defer pp.stateMu.RUnlock()
	return pp.lastConnectionAttempt, pp.lastSuccessfulConnect, pp.nextRetryAt, pp.failureReason, pp.retryCount
}

// CanRetry returns true if the provider can be retried (not authentication failed)
func (pp *providerPool) CanRetry() bool {
	state := pp.GetState()
	return state != ProviderStateAuthenticationFailed
}

// ShouldRetryNow returns true if enough time has passed for a retry
func (pp *providerPool) ShouldRetryNow() bool {
	pp.stateMu.RLock()
	defer pp.stateMu.RUnlock()
	return time.Now().After(pp.nextRetryAt)
}
