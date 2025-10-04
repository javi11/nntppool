package nntpcli

import (
	"sync/atomic"
	"time"
)

// Metrics contains lightweight connection metrics using atomic operations
// All operations are non-blocking and have minimal performance overhead
type Metrics struct {
	// Connection timing (unix timestamps for minimal overhead)
	connectedAt  int64
	lastActivity int64

	// Counters (atomic int64 for thread-safety without locks)
	totalCommands     int64
	commandErrors     int64
	bytesDownloaded   int64
	bytesUploaded     int64
	articlesRetrieved int64
	articlesPosted    int64
	authAttempts      int64
	authFailures      int64
	groupJoins        int64

	// Optional: only calculated when requested to avoid continuous overhead
	isEnabled int32 // 0 = disabled, 1 = enabled
}

// NewMetrics creates a new lightweight metrics instance
func NewMetrics() *Metrics {
	now := time.Now().Unix()
	return &Metrics{
		connectedAt:  now,
		lastActivity: now,
		isEnabled:    1, // enabled by default
	}
}

// Enable/Disable metrics collection
func (m *Metrics) SetEnabled(enabled bool) {
	if enabled {
		atomic.StoreInt32(&m.isEnabled, 1)
	} else {
		atomic.StoreInt32(&m.isEnabled, 0)
	}
}

func (m *Metrics) IsEnabled() bool {
	return atomic.LoadInt32(&m.isEnabled) == 1
}

// Fast inline metric recording (single atomic operation each)
// These functions are designed to be called frequently with minimal overhead

func (m *Metrics) RecordCommand(success bool) {
	if !m.IsEnabled() {
		return
	}
	atomic.StoreInt64(&m.lastActivity, time.Now().Unix())
	atomic.AddInt64(&m.totalCommands, 1)
	if !success {
		atomic.AddInt64(&m.commandErrors, 1)
	}
}

func (m *Metrics) RecordAuth(success bool) {
	if !m.IsEnabled() {
		return
	}
	atomic.StoreInt64(&m.lastActivity, time.Now().Unix())
	atomic.AddInt64(&m.authAttempts, 1)
	if !success {
		atomic.AddInt64(&m.authFailures, 1)
	}
}

func (m *Metrics) RecordDownload(bytes int64) {
	if !m.IsEnabled() {
		return
	}
	atomic.StoreInt64(&m.lastActivity, time.Now().Unix())
	atomic.AddInt64(&m.bytesDownloaded, bytes)
}

func (m *Metrics) RecordUpload(bytes int64) {
	if !m.IsEnabled() {
		return
	}
	atomic.StoreInt64(&m.lastActivity, time.Now().Unix())
	atomic.AddInt64(&m.bytesUploaded, bytes)
}

func (m *Metrics) RecordArticle() {
	if !m.IsEnabled() {
		return
	}
	atomic.StoreInt64(&m.lastActivity, time.Now().Unix())
	atomic.AddInt64(&m.articlesRetrieved, 1)
}

func (m *Metrics) RecordArticlePosted() {
	if !m.IsEnabled() {
		return
	}
	atomic.StoreInt64(&m.lastActivity, time.Now().Unix())
	atomic.AddInt64(&m.articlesPosted, 1)
}

func (m *Metrics) RecordGroupJoin() {
	if !m.IsEnabled() {
		return
	}
	atomic.StoreInt64(&m.lastActivity, time.Now().Unix())
	atomic.AddInt64(&m.groupJoins, 1)
}

// Snapshot returns current metrics values (calculated on-demand)
type MetricsSnapshot struct {
	ConnectedAt       time.Time `json:"connected_at"`
	LastActivity      time.Time `json:"last_activity"`
	ConnectionAge     string    `json:"connection_age"`
	TotalCommands     int64     `json:"total_commands"`
	CommandErrors     int64     `json:"command_errors"`
	BytesDownloaded   int64     `json:"bytes_downloaded"`
	BytesUploaded     int64     `json:"bytes_uploaded"`
	ArticlesRetrieved int64     `json:"articles_retrieved"`
	ArticlesPosted    int64     `json:"articles_posted"`
	AuthAttempts      int64     `json:"auth_attempts"`
	AuthFailures      int64     `json:"auth_failures"`
	GroupJoins        int64     `json:"group_joins"`
	SuccessRate       float64   `json:"success_rate_percent"`
	AuthSuccessRate   float64   `json:"auth_success_rate_percent"`
}

// GetSnapshot returns a snapshot of current metrics (only when explicitly requested)
func (m *Metrics) GetSnapshot() MetricsSnapshot {
	connectedAt := time.Unix(atomic.LoadInt64(&m.connectedAt), 0)
	lastActivity := time.Unix(atomic.LoadInt64(&m.lastActivity), 0)
	totalCommands := atomic.LoadInt64(&m.totalCommands)
	commandErrors := atomic.LoadInt64(&m.commandErrors)
	authAttempts := atomic.LoadInt64(&m.authAttempts)
	authFailures := atomic.LoadInt64(&m.authFailures)

	// Calculate success rates
	successRate := 100.0
	if totalCommands > 0 {
		successRate = float64(totalCommands-commandErrors) / float64(totalCommands) * 100.0
	}

	authSuccessRate := 100.0
	if authAttempts > 0 {
		authSuccessRate = float64(authAttempts-authFailures) / float64(authAttempts) * 100.0
	}

	return MetricsSnapshot{
		ConnectedAt:       connectedAt,
		LastActivity:      lastActivity,
		ConnectionAge:     time.Since(connectedAt).String(),
		TotalCommands:     totalCommands,
		CommandErrors:     commandErrors,
		BytesDownloaded:   atomic.LoadInt64(&m.bytesDownloaded),
		BytesUploaded:     atomic.LoadInt64(&m.bytesUploaded),
		ArticlesRetrieved: atomic.LoadInt64(&m.articlesRetrieved),
		ArticlesPosted:    atomic.LoadInt64(&m.articlesPosted), // Assuming articles posted is same as retrieved for simplicity
		AuthAttempts:      authAttempts,
		AuthFailures:      authFailures,
		GroupJoins:        atomic.LoadInt64(&m.groupJoins),
		SuccessRate:       successRate,
		AuthSuccessRate:   authSuccessRate,
	}
}

// Quick getters for commonly needed values (single atomic load each)
func (m *Metrics) GetBytesDownloaded() int64 {
	return atomic.LoadInt64(&m.bytesDownloaded)
}

func (m *Metrics) GetBytesUploaded() int64 {
	return atomic.LoadInt64(&m.bytesUploaded)
}

func (m *Metrics) GetTotalCommands() int64 {
	return atomic.LoadInt64(&m.totalCommands)
}

func (m *Metrics) GetConnectionAge() time.Duration {
	return time.Since(time.Unix(atomic.LoadInt64(&m.connectedAt), 0))
}
