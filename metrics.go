package nntppool

import (
	"sync/atomic"
	"time"
)

// PingResult holds the outcome of a DATE-based ping to a provider.
type PingResult struct {
	RTT        time.Duration
	ServerTime time.Time
	Err        error
}

// providerStats holds internal atomic counters for a single provider group.
// Used on the hot path — no mutex, atomic only.
type providerStats struct {
	BytesConsumed atomic.Int64 // wire bytes consumed (used to compute AvgSpeed)
	Missing       atomic.Int64 // 430/423 responses
	Errors        atomic.Int64 // network errors, bad status codes
	Ping          PingResult   // result of initial DATE ping

	// Quota tracking. quotaBytes is set once at group init (0 = unlimited).
	quotaBytes    int64
	quotaUsed     atomic.Int64 // bytes consumed in the current quota period
	quotaExceeded atomic.Bool  // cached flag: set when quotaUsed >= quotaBytes; cleared on period reset
}

// ProviderStats is a public snapshot of one provider's metrics.
type ProviderStats struct {
	Name              string
	AvgSpeed          float64 // bytes/sec average since client start
	BytesConsumed     int64   // raw wire bytes consumed since client start
	Missing           int64
	Errors            int64
	ActiveConnections int // currently running connections
	MaxConnections    int // configured connection slots
	Ping              PingResult

	// Quota fields. QuotaBytes is 0 when no quota is configured.
	QuotaBytes    int64     // configured limit per period (0 = unlimited)
	QuotaUsed     int64     // bytes consumed in the current period
	QuotaResetAt  time.Time // when the quota period resets; zero if no period
	QuotaExceeded bool      // true when QuotaUsed >= QuotaBytes > 0
}

// ClientStats is an aggregate snapshot of all provider metrics.
type ClientStats struct {
	Providers     []ProviderStats
	AvgSpeed      float64       // total bytes/sec across all providers
	BytesConsumed int64         // raw wire bytes consumed across all providers
	Elapsed       time.Duration // time since client creation
}
