package nntppool

import (
	"sync"
	"sync/atomic"
	"time"
)

// PoolMetrics provides simple atomic counters for pool operations
// All operations use atomic operations for thread-safety with minimal overhead
type PoolMetrics struct {
	// Article counters
	articlesDownloaded int64 // Total articles successfully downloaded
	articlesPosted     int64 // Total articles successfully posted

	// Traffic counters (bytes)
	bytesDownloaded int64 // Total bytes downloaded
	bytesUploaded   int64 // Total bytes uploaded

	// Error tracking
	totalErrors int64 // Total errors across all operations

	// Per-provider error tracking (thread-safe map)
	// Key: provider host, Value: pointer to atomic error counter
	providerErrors sync.Map // map[string]*int64
}

// PoolMetricsSnapshot provides a point-in-time view of all metrics
// Simplified version: only tracks essential counters
type PoolMetricsSnapshot struct {
	// Article counts
	ArticlesDownloaded int64 `json:"articles_downloaded"`
	ArticlesPosted     int64 `json:"articles_posted"`

	// Traffic totals
	BytesDownloaded int64 `json:"bytes_downloaded"`
	BytesUploaded   int64 `json:"bytes_uploaded"`

	// Error counts
	TotalErrors    int64            `json:"total_errors"`
	ProviderErrors map[string]int64 `json:"provider_errors"` // host -> error count

	// Metadata
	Timestamp time.Time `json:"timestamp"`
}

// NewPoolMetrics creates a new metrics instance with zero counters
func NewPoolMetrics() *PoolMetrics {
	return &PoolMetrics{}
}

// RecordArticleDownloaded increments the downloaded article counter
func (m *PoolMetrics) RecordArticleDownloaded() {
	atomic.AddInt64(&m.articlesDownloaded, 1)
}

// RecordArticlePosted increments the posted article counter
func (m *PoolMetrics) RecordArticlePosted() {
	atomic.AddInt64(&m.articlesPosted, 1)
}

// RecordDownload adds bytes to the download counter
func (m *PoolMetrics) RecordDownload(bytes int64) {
	atomic.AddInt64(&m.bytesDownloaded, bytes)
}

// RecordUpload adds bytes to the upload counter
func (m *PoolMetrics) RecordUpload(bytes int64) {
	atomic.AddInt64(&m.bytesUploaded, bytes)
}

// RecordError increments error counters
// If providerHost is non-empty, also increments that provider's error count
func (m *PoolMetrics) RecordError(providerHost string) {
	atomic.AddInt64(&m.totalErrors, 1)

	// Record per-provider error if host is provided
	if providerHost != "" {
		// Get or create atomic counter for this provider
		val, _ := m.providerErrors.LoadOrStore(providerHost, new(int64))
		counter := val.(*int64)
		atomic.AddInt64(counter, 1)
	}
}

// GetSnapshot returns a point-in-time snapshot of all metrics
// The pools parameter is ignored in the simplified version
// The returned snapshot is a copy and won't change as metrics continue to update
func (m *PoolMetrics) GetSnapshot(pools []*providerPool) PoolMetricsSnapshot {
	snapshot := PoolMetricsSnapshot{
		ArticlesDownloaded: atomic.LoadInt64(&m.articlesDownloaded),
		ArticlesPosted:     atomic.LoadInt64(&m.articlesPosted),
		BytesDownloaded:    atomic.LoadInt64(&m.bytesDownloaded),
		BytesUploaded:      atomic.LoadInt64(&m.bytesUploaded),
		TotalErrors:        atomic.LoadInt64(&m.totalErrors),
		ProviderErrors:     make(map[string]int64),
		Timestamp:          time.Now(),
	}

	// Copy per-provider error counts
	m.providerErrors.Range(func(key, value interface{}) bool {
		host := key.(string)
		counter := value.(*int64)
		snapshot.ProviderErrors[host] = atomic.LoadInt64(counter)
		return true
	})

	return snapshot
}

// Reset zeros all counters
// Optional method for periodic reporting or maintenance
// Not required for normal operation - counters work indefinitely without memory leaks
func (m *PoolMetrics) Reset() {
	atomic.StoreInt64(&m.articlesDownloaded, 0)
	atomic.StoreInt64(&m.articlesPosted, 0)
	atomic.StoreInt64(&m.bytesDownloaded, 0)
	atomic.StoreInt64(&m.bytesUploaded, 0)
	atomic.StoreInt64(&m.totalErrors, 0)

	// Reset all provider error counters to zero
	m.providerErrors.Range(func(key, value interface{}) bool {
		counter := value.(*int64)
		atomic.StoreInt64(counter, 0)
		return true
	})
}

// No-op methods to maintain backward compatibility with existing code
// These methods exist but do nothing in the simplified metrics

func (m *PoolMetrics) RecordAcquire()                              {}
func (m *PoolMetrics) RecordRelease()                              {}
func (m *PoolMetrics) RecordRetry()                                {}
func (m *PoolMetrics) RecordConnectionCreated()                    {}
func (m *PoolMetrics) RecordConnectionDestroyed()                  {}
func (m *PoolMetrics) RecordAcquireWaitTime(duration time.Duration) {}
func (m *PoolMetrics) RegisterActiveConnection(id string, conn interface{}) {
}
func (m *PoolMetrics) UnregisterActiveConnection(id string) {}
