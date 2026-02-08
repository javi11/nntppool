package nntppool

import (
	"sync/atomic"
	"time"
)

// providerStats holds internal atomic counters for a single provider group.
// Used on the hot path — no mutex, atomic only.
type providerStats struct {
	BytesConsumed atomic.Int64 // wire bytes consumed (used to compute AvgSpeed)
	Missing       atomic.Int64 // 430/423 responses
	Errors        atomic.Int64 // network errors, bad status codes
}

// ProviderStats is a public snapshot of one provider's metrics.
type ProviderStats struct {
	Name              string
	AvgSpeed          float64 // bytes/sec average since client start
	Missing           int64
	Errors            int64
	ActiveConnections int // currently running connections
	MaxConnections    int // configured connection slots
}

// ClientStats is an aggregate snapshot of all provider metrics.
type ClientStats struct {
	Providers []ProviderStats
	AvgSpeed  float64       // total bytes/sec across all providers
	Elapsed   time.Duration // time since client creation
}
