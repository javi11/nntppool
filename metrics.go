package nntppool

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/javi11/nntpcli"
)

// PoolMetrics provides high-performance metrics for the entire connection pool
// All operations use atomic operations for thread-safety and minimal overhead
type PoolMetrics struct {
	// Pool-level metrics (atomic counters)
	totalConnectionsCreated   int64 // Total connections created across all providers
	totalConnectionsDestroyed int64 // Total connections destroyed
	totalAcquires             int64 // Total connection acquisitions
	totalReleases             int64 // Total connection releases
	totalErrors               int64 // Total operation errors
	totalRetries              int64 // Total retry attempts

	// Performance metrics (atomic counters)
	totalAcquireWaitTime int64 // Total time spent waiting for connections (nanoseconds)

	// Timing
	startTime int64 // Pool start time (Unix nanoseconds)

	// Active connections registry (thread-safe map)
	// Key: connection identifier, Value: nntpcli.Connection with metrics
	activeConnections sync.Map // map[string]nntpcli.Connection
}

// NewPoolMetrics creates a new metrics instance
func NewPoolMetrics() *PoolMetrics {
	return &PoolMetrics{
		startTime: time.Now().UnixNano(),
	}
}

// Connection lifecycle metrics
func (m *PoolMetrics) RecordConnectionCreated() {
	atomic.AddInt64(&m.totalConnectionsCreated, 1)
}

func (m *PoolMetrics) RecordConnectionDestroyed() {
	atomic.AddInt64(&m.totalConnectionsDestroyed, 1)
}

func (m *PoolMetrics) RecordAcquire() {
	atomic.AddInt64(&m.totalAcquires, 1)
}

func (m *PoolMetrics) RecordRelease() {
	atomic.AddInt64(&m.totalReleases, 1)
}

func (m *PoolMetrics) RecordError() {
	atomic.AddInt64(&m.totalErrors, 1)
}

func (m *PoolMetrics) RecordRetry() {
	atomic.AddInt64(&m.totalRetries, 1)
}

// Performance metrics
func (m *PoolMetrics) RecordAcquireWaitTime(duration time.Duration) {
	atomic.AddInt64(&m.totalAcquireWaitTime, int64(duration))
}

// Fast getters (single atomic load each)
func (m *PoolMetrics) GetTotalConnectionsCreated() int64 {
	return atomic.LoadInt64(&m.totalConnectionsCreated)
}

func (m *PoolMetrics) GetTotalConnectionsDestroyed() int64 {
	return atomic.LoadInt64(&m.totalConnectionsDestroyed)
}

func (m *PoolMetrics) GetActiveConnections() int64 {
	return m.GetTotalConnectionsCreated() - m.GetTotalConnectionsDestroyed()
}

func (m *PoolMetrics) GetTotalAcquires() int64 {
	return atomic.LoadInt64(&m.totalAcquires)
}

func (m *PoolMetrics) GetTotalReleases() int64 {
	return atomic.LoadInt64(&m.totalReleases)
}

func (m *PoolMetrics) GetTotalErrors() int64 {
	return atomic.LoadInt64(&m.totalErrors)
}

func (m *PoolMetrics) GetTotalRetries() int64 {
	return atomic.LoadInt64(&m.totalRetries)
}


func (m *PoolMetrics) GetAverageAcquireWaitTime() time.Duration {
	totalWait := atomic.LoadInt64(&m.totalAcquireWaitTime)
	totalAcquires := atomic.LoadInt64(&m.totalAcquires)
	if totalAcquires == 0 {
		return 0
	}
	return time.Duration(totalWait / totalAcquires)
}

func (m *PoolMetrics) GetUptime() time.Duration {
	startTime := atomic.LoadInt64(&m.startTime)
	return time.Duration(time.Now().UnixNano() - startTime)
}

// RegisterActiveConnection registers a connection as active for metrics tracking
// This should be called when a connection is acquired from the pool
func (m *PoolMetrics) RegisterActiveConnection(connectionID string, conn nntpcli.Connection) {
	m.activeConnections.Store(connectionID, conn)
}

// UnregisterActiveConnection removes a connection from active tracking
// This should be called when a connection is released back to the pool or destroyed
func (m *PoolMetrics) UnregisterActiveConnection(connectionID string) {
	m.activeConnections.Delete(connectionID)
}

// GetActiveConnectionsCount returns the number of actively tracked connections
func (m *PoolMetrics) GetActiveConnectionsCount() int {
	count := 0
	m.activeConnections.Range(func(key, value interface{}) bool {
		count++
		return true
	})
	return count
}

// ActiveConnectionMetrics represents metrics for active connections only
type ActiveConnectionMetrics struct {
	Count                int           `json:"count"`
	TotalBytesDownloaded int64         `json:"total_bytes_downloaded"`
	TotalBytesUploaded   int64         `json:"total_bytes_uploaded"`
	TotalCommands        int64         `json:"total_commands"`
	TotalCommandErrors   int64         `json:"total_command_errors"`
	SuccessRate          float64       `json:"success_rate_percent"`
	AverageConnectionAge time.Duration `json:"average_connection_age"`
}

// GetActiveConnectionMetrics returns real-time metrics for currently active connections
func (m *PoolMetrics) GetActiveConnectionMetrics() ActiveConnectionMetrics {
	var totalBytesDownloaded, totalBytesUploaded int64
	var totalCommands, totalCommandErrors int64
	var totalConnectionAge time.Duration
	var count int

	m.activeConnections.Range(func(key, value interface{}) bool {
		conn, ok := value.(nntpcli.Connection)
		if !ok || conn == nil {
			return true // Continue iteration
		}

		metrics := conn.GetMetrics()
		if metrics != nil {
			snapshot := metrics.GetSnapshot()
			totalBytesDownloaded += snapshot.BytesDownloaded
			totalBytesUploaded += snapshot.BytesUploaded
			totalCommands += snapshot.TotalCommands
			totalCommandErrors += snapshot.CommandErrors
			totalConnectionAge += metrics.GetConnectionAge()
			count++
		}
		return true // Continue iteration
	})

	var successRate float64
	if totalCommands > 0 {
		successRate = float64(totalCommands-totalCommandErrors) / float64(totalCommands) * 100
	}

	var averageConnectionAge time.Duration
	if count > 0 {
		averageConnectionAge = totalConnectionAge / time.Duration(count)
	}

	return ActiveConnectionMetrics{
		Count:                count,
		TotalBytesDownloaded: totalBytesDownloaded,
		TotalBytesUploaded:   totalBytesUploaded,
		TotalCommands:        totalCommands,
		TotalCommandErrors:   totalCommandErrors,
		SuccessRate:          successRate,
		AverageConnectionAge: averageConnectionAge,
	}
}

// PoolMetricsSnapshot provides a comprehensive view of pool metrics
// Only computed when explicitly requested to minimize overhead
type PoolMetricsSnapshot struct {
	// Timestamp and uptime
	Timestamp time.Time     `json:"timestamp"`
	Uptime    time.Duration `json:"uptime"`

	// Connection metrics
	TotalConnectionsCreated   int64 `json:"total_connections_created"`
	TotalConnectionsDestroyed int64 `json:"total_connections_destroyed"`
	ActiveConnections         int64 `json:"active_connections"`
	TotalAcquires             int64 `json:"total_acquires"`
	TotalReleases             int64 `json:"total_releases"`

	// Pool utilization
	AcquiredConnections int32 `json:"acquired_connections"`
	IdleConnections     int32 `json:"idle_connections"`
	TotalConnections    int32 `json:"total_connections"`

	// Traffic metrics
	TotalBytesDownloaded   int64   `json:"total_bytes_downloaded"`
	TotalBytesUploaded     int64   `json:"total_bytes_uploaded"`
	TotalArticlesRetrieved int64   `json:"total_articles_retrieved"`
	TotalArticlesPosted    int64   `json:"total_articles_posted"`
	DownloadSpeed          float64 `json:"download_speed_bytes_per_sec"`
	UploadSpeed            float64 `json:"upload_speed_bytes_per_sec"`

	// Performance metrics
	TotalCommandCount      int64         `json:"total_command_count"`
	TotalCommandErrors     int64         `json:"total_command_errors"`
	CommandSuccessRate     float64       `json:"command_success_rate_percent"`
	AverageAcquireWaitTime time.Duration `json:"average_acquire_wait_time"`

	// Error metrics
	TotalErrors  int64   `json:"total_errors"`
	TotalRetries int64   `json:"total_retries"`
	ErrorRate    float64 `json:"error_rate_percent"`

	// Provider-specific metrics
	ProviderMetrics []ProviderMetricsSnapshot `json:"provider_metrics"`
}

// ProviderMetricsSnapshot contains metrics for a specific provider
type ProviderMetricsSnapshot struct {
	ProviderID string `json:"provider_id"`
	Host       string `json:"host"`
	Username   string `json:"username"`
	State      string `json:"state"`

	// Pool statistics from puddle
	MaxConnections          int32         `json:"max_connections"`
	TotalConnections        int32         `json:"total_connections"`
	AcquiredConnections     int32         `json:"acquired_connections"`
	IdleConnections         int32         `json:"idle_connections"`
	ConstructingConnections int32         `json:"constructing_connections"`
	AcquireCount            int64         `json:"acquire_count"`
	AcquireDuration         time.Duration `json:"acquire_duration"`
	EmptyAcquireCount       int64         `json:"empty_acquire_count"`
	EmptyAcquireWaitTime    time.Duration `json:"empty_acquire_wait_time"`
	CanceledAcquireCount    int64         `json:"canceled_acquire_count"`

	// Connection-level aggregated metrics
	TotalBytesDownloaded int64         `json:"total_bytes_downloaded"`
	TotalBytesUploaded   int64         `json:"total_bytes_uploaded"`
	TotalCommands        int64         `json:"total_commands"`
	TotalCommandErrors   int64         `json:"total_command_errors"`
	SuccessRate          float64       `json:"success_rate_percent"`
	AverageConnectionAge time.Duration `json:"average_connection_age"`
}

type AggregatedMetrics struct {
	TotalBytesDownloaded int64
	TotalBytesUploaded   int64
	TotalCommands        int64
	TotalCommandErrors   int64
	SuccessRate          float64
	AverageConnectionAge time.Duration
}

// GetSnapshot returns a comprehensive snapshot of all metrics
// This method aggregates data from puddle pools and individual connections
func (m *PoolMetrics) GetSnapshot(pools []*providerPool) PoolMetricsSnapshot {
	timestamp := time.Now()
	uptime := m.GetUptime()

	// Aggregate all connection metrics first to get totals
	var globalTotalBytesDownloaded, globalTotalBytesUploaded int64
	var globalTotalCommands, globalTotalCommandErrors int64

	for _, pool := range pools {
		aggregated := m.aggregateConnectionMetrics(pool)
		globalTotalBytesDownloaded += aggregated.TotalBytesDownloaded
		globalTotalBytesUploaded += aggregated.TotalBytesUploaded
		globalTotalCommands += aggregated.TotalCommands
		globalTotalCommandErrors += aggregated.TotalCommandErrors
	}

	// Calculate speed metrics from aggregated connection data
	uptimeSeconds := uptime.Seconds()
	var downloadSpeed, uploadSpeed float64
	if uptimeSeconds > 0 {
		downloadSpeed = float64(globalTotalBytesDownloaded) / uptimeSeconds
		uploadSpeed = float64(globalTotalBytesUploaded) / uptimeSeconds
	}

	// Calculate success rates from aggregated connection data
	var commandSuccessRate float64
	if globalTotalCommands > 0 {
		commandSuccessRate = float64(globalTotalCommands-globalTotalCommandErrors) / float64(globalTotalCommands) * 100
	}

	totalOperations := m.GetTotalAcquires()
	var errorRate float64
	if totalOperations > 0 {
		errorRate = float64(m.GetTotalErrors()) / float64(totalOperations) * 100
	}

	// Aggregate pool utilization
	var totalAcquired, totalIdle, totalConnections int32
	providerMetrics := make([]ProviderMetricsSnapshot, 0, len(pools))

	for _, pool := range pools {
		stat := pool.connectionPool.Stat()
		providerSnapshot := ProviderMetricsSnapshot{
			ProviderID:              pool.provider.ID(),
			Host:                    pool.provider.Host,
			Username:                pool.provider.Username,
			State:                   pool.GetState().String(),
			MaxConnections:          stat.MaxResources(),
			TotalConnections:        stat.TotalResources(),
			AcquiredConnections:     stat.AcquiredResources(),
			IdleConnections:         stat.IdleResources(),
			ConstructingConnections: stat.ConstructingResources(),
			AcquireCount:            stat.AcquireCount(),
			AcquireDuration:         stat.AcquireDuration(),
			EmptyAcquireCount:       stat.EmptyAcquireCount(),
			EmptyAcquireWaitTime:    stat.EmptyAcquireWaitTime(),
			CanceledAcquireCount:    stat.CanceledAcquireCount(),
		}

		// Aggregate connection metrics for this provider
		aggregatedMetrics := m.aggregateConnectionMetrics(pool)
		providerSnapshot.TotalBytesDownloaded = aggregatedMetrics.TotalBytesDownloaded
		providerSnapshot.TotalBytesUploaded = aggregatedMetrics.TotalBytesUploaded
		providerSnapshot.TotalCommands = aggregatedMetrics.TotalCommands
		providerSnapshot.TotalCommandErrors = aggregatedMetrics.TotalCommandErrors
		providerSnapshot.SuccessRate = aggregatedMetrics.SuccessRate
		providerSnapshot.AverageConnectionAge = aggregatedMetrics.AverageConnectionAge

		providerMetrics = append(providerMetrics, providerSnapshot)

		// Add to totals
		totalAcquired += stat.AcquiredResources()
		totalIdle += stat.IdleResources()
		totalConnections += stat.TotalResources()
	}

	return PoolMetricsSnapshot{
		Timestamp:                 timestamp,
		Uptime:                    uptime,
		TotalConnectionsCreated:   m.GetTotalConnectionsCreated(),
		TotalConnectionsDestroyed: m.GetTotalConnectionsDestroyed(),
		ActiveConnections:         m.GetActiveConnections(),
		TotalAcquires:             m.GetTotalAcquires(),
		TotalReleases:             m.GetTotalReleases(),
		AcquiredConnections:       totalAcquired,
		IdleConnections:           totalIdle,
		TotalConnections:          totalConnections,
		TotalBytesDownloaded:      globalTotalBytesDownloaded,
		TotalBytesUploaded:        globalTotalBytesUploaded,
		TotalArticlesRetrieved:    0, // Will be calculated from connections if needed
		TotalArticlesPosted:       0, // Will be calculated from connections if needed  
		DownloadSpeed:             downloadSpeed,
		UploadSpeed:               uploadSpeed,
		TotalCommandCount:         globalTotalCommands,
		TotalCommandErrors:        globalTotalCommandErrors,
		CommandSuccessRate:        commandSuccessRate,
		AverageAcquireWaitTime:    m.GetAverageAcquireWaitTime(),
		TotalErrors:               m.GetTotalErrors(),
		TotalRetries:              m.GetTotalRetries(),
		ErrorRate:                 errorRate,
		ProviderMetrics:           providerMetrics,
	}
}

// aggregateConnectionMetrics aggregates metrics from all connections in a provider pool
// This includes both idle connections and active connections for complete visibility
func (m *PoolMetrics) aggregateConnectionMetrics(pool *providerPool) AggregatedMetrics {
	var totalBytesDownloaded, totalBytesUploaded int64
	var totalCommands, totalCommandErrors int64
	var totalConnectionAge time.Duration
	var connectionCount int

	// First, collect metrics from idle connections
	idleResources := pool.connectionPool.AcquireAllIdle()
	defer func() {
		// Release all idle connections back to the pool
		for _, res := range idleResources {
			res.ReleaseUnused()
		}
	}()

	connectionCount += len(idleResources)

	for _, res := range idleResources {
		conn := res.Value()
		if conn.nntp != nil {
			metrics := conn.nntp.GetMetrics()
			if metrics != nil {
				snapshot := metrics.GetSnapshot()
				totalBytesDownloaded += snapshot.BytesDownloaded
				totalBytesUploaded += snapshot.BytesUploaded
				totalCommands += snapshot.TotalCommands
				totalCommandErrors += snapshot.CommandErrors
				totalConnectionAge += metrics.GetConnectionAge()
			}
		}
	}

	// Second, collect metrics from active connections
	m.activeConnections.Range(func(key, value interface{}) bool {
		conn, ok := value.(nntpcli.Connection)
		if !ok || conn == nil {
			return true // Continue iteration
		}

		metrics := conn.GetMetrics()
		if metrics != nil {
			snapshot := metrics.GetSnapshot()
			totalBytesDownloaded += snapshot.BytesDownloaded
			totalBytesUploaded += snapshot.BytesUploaded
			totalCommands += snapshot.TotalCommands
			totalCommandErrors += snapshot.CommandErrors
			totalConnectionAge += metrics.GetConnectionAge()
			connectionCount++
		}
		return true // Continue iteration
	})

	var successRate float64
	if totalCommands > 0 {
		successRate = float64(totalCommands-totalCommandErrors) / float64(totalCommands) * 100
	}

	var averageConnectionAge time.Duration
	if connectionCount > 0 {
		averageConnectionAge = totalConnectionAge / time.Duration(connectionCount)
	}

	return AggregatedMetrics{
		TotalBytesDownloaded: totalBytesDownloaded,
		TotalBytesUploaded:   totalBytesUploaded,
		TotalCommands:        totalCommands,
		TotalCommandErrors:   totalCommandErrors,
		SuccessRate:          successRate,
		AverageConnectionAge: averageConnectionAge,
	}
}
