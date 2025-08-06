package nntppool

import (
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/javi11/nntpcli"
)

// speedCache holds cached speed calculation results
type speedCache struct {
	mu             sync.RWMutex
	lastCalculated time.Time
	downloadSpeed  float64
	uploadSpeed    float64
	cacheDuration  time.Duration
}

// MetricRetentionConfig defines how long different types of metrics are retained
type MetricRetentionConfig struct {
	// How long to keep detailed metrics before rotation
	DetailedRetentionDuration time.Duration
	
	// How often to perform metric rotation
	RotationInterval time.Duration
	
	// Maximum number of historical windows to keep
	MaxHistoricalWindows int
	
	// Memory threshold (bytes) that triggers aggressive cleanup
	MemoryThresholdBytes uint64
	
	// Enable automatic cleanup when memory thresholds are exceeded
	AutoCleanupEnabled bool
}

// MetricWindow represents metrics for a specific time window
type MetricWindow struct {
	StartTime time.Time `json:"start_time"`
	EndTime   time.Time `json:"end_time"`
	
	// Core metrics for this window
	ConnectionsCreated   int64 `json:"connections_created"`
	ConnectionsDestroyed int64 `json:"connections_destroyed"`
	Acquires             int64 `json:"acquires"`
	Releases             int64 `json:"releases"`
	Errors               int64 `json:"errors"`
	Retries              int64 `json:"retries"`
	AcquireWaitTime      int64 `json:"acquire_wait_time_ns"`
	
	// Traffic metrics
	BytesDownloaded   int64 `json:"bytes_downloaded"`
	BytesUploaded     int64 `json:"bytes_uploaded"`
	ArticlesRetrieved int64 `json:"articles_retrieved"`
	ArticlesPosted    int64 `json:"articles_posted"`
	CommandCount      int64 `json:"command_count"`
	CommandErrors     int64 `json:"command_errors"`
}

// RollingMetrics manages metrics across multiple time windows
type RollingMetrics struct {
	mu sync.RWMutex
	
	// Current active window (receiving new metrics)
	currentWindow *MetricWindow
	
	// Historical windows (completed time periods)
	historicalWindows []*MetricWindow
	
	// Configuration
	config MetricRetentionConfig
	
	// Memory usage tracking
	lastMemoryCheck    time.Time
	memoryCheckInterval time.Duration
}

// ConnectionCleanupTracker tracks connections for automatic cleanup
type ConnectionCleanupTracker struct {
	mu               sync.RWMutex
	connections      map[string]time.Time // connectionID -> last seen time
	cleanupInterval  time.Duration
	connectionTimeout time.Duration
	lastCleanup      time.Time
}

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

	// Speed calculation configuration
	speedWindowDuration time.Duration // Time window for speed calculations (default: 60 seconds)

	// Cached speed calculations to avoid blocking pool operations
	speedCache speedCache
	
	// Rolling metrics for memory management
	rollingMetrics *RollingMetrics
	
	// Connection cleanup tracking
	cleanupTracker *ConnectionCleanupTracker
}

// NewPoolMetrics creates a new metrics instance
func NewPoolMetrics() *PoolMetrics {
	now := time.Now()
	
	// Default retention configuration
	defaultConfig := MetricRetentionConfig{
		DetailedRetentionDuration: 24 * time.Hour,   // Keep detailed metrics for 24 hours
		RotationInterval:          1 * time.Hour,    // Rotate metrics every hour  
		MaxHistoricalWindows:      168,              // Keep 7 days of hourly windows (7*24=168)
		MemoryThresholdBytes:      100 * 1024 * 1024, // 100MB threshold
		AutoCleanupEnabled:        true,
	}
	
	return &PoolMetrics{
		startTime:           now.UnixNano(),
		speedWindowDuration: 60 * time.Second, // Default 60 second window for speed calculations
		speedCache: speedCache{
			cacheDuration: 5 * time.Second, // Default 5 second cache duration
		},
		rollingMetrics: &RollingMetrics{
			currentWindow: &MetricWindow{
				StartTime: now,
				EndTime:   now.Add(defaultConfig.RotationInterval),
			},
			historicalWindows:   make([]*MetricWindow, 0, defaultConfig.MaxHistoricalWindows),
			config:              defaultConfig,
			memoryCheckInterval: 5 * time.Minute, // Check memory every 5 minutes
		},
		cleanupTracker: &ConnectionCleanupTracker{
			connections:       make(map[string]time.Time),
			cleanupInterval:   30 * time.Second, // Check for stale connections every 30 seconds
			connectionTimeout: 5 * time.Minute,  // Consider connections stale after 5 minutes
			lastCleanup:       now,
		},
	}
}

// Connection lifecycle metrics
func (m *PoolMetrics) RecordConnectionCreated() {
	atomic.AddInt64(&m.totalConnectionsCreated, 1)
	m.recordToCurrentWindow("connectionsCreated", 1)
}

func (m *PoolMetrics) RecordConnectionDestroyed() {
	atomic.AddInt64(&m.totalConnectionsDestroyed, 1)
	m.recordToCurrentWindow("connectionsDestroyed", 1)
}

func (m *PoolMetrics) RecordAcquire() {
	atomic.AddInt64(&m.totalAcquires, 1)
	m.recordToCurrentWindow("acquires", 1)
}

func (m *PoolMetrics) RecordRelease() {
	atomic.AddInt64(&m.totalReleases, 1)
	m.recordToCurrentWindow("releases", 1)
}

func (m *PoolMetrics) RecordError() {
	atomic.AddInt64(&m.totalErrors, 1)
	m.recordToCurrentWindow("errors", 1)
}

func (m *PoolMetrics) RecordRetry() {
	atomic.AddInt64(&m.totalRetries, 1)
	m.recordToCurrentWindow("retries", 1)
}

// Performance metrics
func (m *PoolMetrics) RecordAcquireWaitTime(duration time.Duration) {
	atomic.AddInt64(&m.totalAcquireWaitTime, int64(duration))
	m.recordToCurrentWindow("acquireWaitTime", int64(duration))
}

// recordToCurrentWindow safely records a metric to the current window
func (m *PoolMetrics) recordToCurrentWindow(metricType string, value int64) {
	if m.rollingMetrics == nil {
		return
	}
	
	now := time.Now()
	
	m.rollingMetrics.mu.Lock()
	defer m.rollingMetrics.mu.Unlock()
	
	// Check if we need to rotate the current window
	if m.rollingMetrics.currentWindow != nil && now.After(m.rollingMetrics.currentWindow.EndTime) {
		m.rotateCurrentWindow(now)
	}
	
	// Ensure we have a current window
	if m.rollingMetrics.currentWindow == nil {
		m.createNewCurrentWindow(now)
	}
	
	// Record the metric to the current window
	switch metricType {
	case "connectionsCreated":
		atomic.AddInt64(&m.rollingMetrics.currentWindow.ConnectionsCreated, value)
	case "connectionsDestroyed":
		atomic.AddInt64(&m.rollingMetrics.currentWindow.ConnectionsDestroyed, value)
	case "acquires":
		atomic.AddInt64(&m.rollingMetrics.currentWindow.Acquires, value)
	case "releases":
		atomic.AddInt64(&m.rollingMetrics.currentWindow.Releases, value)
	case "errors":
		atomic.AddInt64(&m.rollingMetrics.currentWindow.Errors, value)
	case "retries":
		atomic.AddInt64(&m.rollingMetrics.currentWindow.Retries, value)
	case "acquireWaitTime":
		atomic.AddInt64(&m.rollingMetrics.currentWindow.AcquireWaitTime, value)
	}
}

// rotateCurrentWindow moves the current window to historical and creates a new one
func (m *PoolMetrics) rotateCurrentWindow(now time.Time) {
	if m.rollingMetrics.currentWindow == nil {
		return
	}
	
	// Complete the current window
	m.rollingMetrics.currentWindow.EndTime = now
	
	// Add to historical windows
	m.rollingMetrics.historicalWindows = append(m.rollingMetrics.historicalWindows, m.rollingMetrics.currentWindow)
	
	// Trim historical windows if we exceed the limit
	maxWindows := m.rollingMetrics.config.MaxHistoricalWindows
	if len(m.rollingMetrics.historicalWindows) > maxWindows {
		// Remove oldest windows
		excess := len(m.rollingMetrics.historicalWindows) - maxWindows
		m.rollingMetrics.historicalWindows = m.rollingMetrics.historicalWindows[excess:]
	}
	
	// Create new current window
	m.createNewCurrentWindow(now)
}

// createNewCurrentWindow creates a new current window starting at the given time
func (m *PoolMetrics) createNewCurrentWindow(startTime time.Time) {
	m.rollingMetrics.currentWindow = &MetricWindow{
		StartTime: startTime,
		EndTime:   startTime.Add(m.rollingMetrics.config.RotationInterval),
	}
}

// PerformRotationCheck checks if metric rotation is needed and performs memory cleanup
func (m *PoolMetrics) PerformRotationCheck() {
	if m.rollingMetrics == nil {
		return
	}
	
	now := time.Now()
	
	// Check if rotation is needed
	m.rollingMetrics.mu.Lock()
	needsRotation := m.rollingMetrics.currentWindow != nil && now.After(m.rollingMetrics.currentWindow.EndTime)
	if needsRotation {
		m.rotateCurrentWindow(now)
	}
	m.rollingMetrics.mu.Unlock()
	
	// Check memory usage and perform cleanup if needed
	if m.rollingMetrics.config.AutoCleanupEnabled {
		m.checkMemoryUsageAndCleanup(now)
	}
	
	// Perform connection cleanup
	m.performConnectionCleanup(now)
}

// checkMemoryUsageAndCleanup monitors memory usage and triggers cleanup when thresholds are exceeded
func (m *PoolMetrics) checkMemoryUsageAndCleanup(now time.Time) {
	// Only check memory periodically to avoid overhead
	if now.Sub(m.rollingMetrics.lastMemoryCheck) < m.rollingMetrics.memoryCheckInterval {
		return
	}
	
	m.rollingMetrics.lastMemoryCheck = now
	
	// Get current memory stats
	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)
	
	// If we're exceeding the threshold, perform aggressive cleanup
	if memStats.Alloc > m.rollingMetrics.config.MemoryThresholdBytes {
		m.performAggressiveCleanup()
	}
}

// performAggressiveCleanup removes old historical windows and resets counters if memory is high
func (m *PoolMetrics) performAggressiveCleanup() {
	m.rollingMetrics.mu.Lock()
	defer m.rollingMetrics.mu.Unlock()
	
	// Reduce historical windows to 50% of max when under memory pressure
	maxWindows := m.rollingMetrics.config.MaxHistoricalWindows / 2
	if len(m.rollingMetrics.historicalWindows) > maxWindows {
		// Keep only the most recent windows
		keepCount := maxWindows
		if keepCount < 12 { // Always keep at least 12 hours of data
			keepCount = 12
		}
		m.rollingMetrics.historicalWindows = m.rollingMetrics.historicalWindows[len(m.rollingMetrics.historicalWindows)-keepCount:]
	}
}

// performConnectionCleanup removes stale connections from the active tracking
func (m *PoolMetrics) performConnectionCleanup(now time.Time) {
	if m.cleanupTracker == nil {
		return
	}
	
	// Only perform cleanup at specified intervals
	if now.Sub(m.cleanupTracker.lastCleanup) < m.cleanupTracker.cleanupInterval {
		return
	}
	
	m.cleanupTracker.mu.Lock()
	m.cleanupTracker.lastCleanup = now
	
	// Clean up stale connections from activeConnections sync.Map
	staleConnections := make([]string, 0)
	
	// First, identify connections that haven't been seen recently
	for connectionID, lastSeen := range m.cleanupTracker.connections {
		if now.Sub(lastSeen) > m.cleanupTracker.connectionTimeout {
			staleConnections = append(staleConnections, connectionID)
		}
	}
	m.cleanupTracker.mu.Unlock()
	
	// Remove stale connections from both maps
	for _, connectionID := range staleConnections {
		m.activeConnections.Delete(connectionID)
		
		m.cleanupTracker.mu.Lock()
		delete(m.cleanupTracker.connections, connectionID)
		m.cleanupTracker.mu.Unlock()
	}
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

// SetSpeedWindowDuration configures the time window used for speed calculations
func (m *PoolMetrics) SetSpeedWindowDuration(duration time.Duration) {
	if duration <= 0 {
		duration = 60 * time.Second // Default fallback
	}
	m.speedWindowDuration = duration
}

// SetSpeedCacheDuration configures how long speed calculations are cached
// This reduces performance impact by avoiding frequent expensive calculations
func (m *PoolMetrics) SetSpeedCacheDuration(duration time.Duration) {
	if duration <= 0 {
		duration = 5 * time.Second // Default fallback
	}
	m.speedCache.mu.Lock()
	m.speedCache.cacheDuration = duration
	m.speedCache.mu.Unlock()
}

// getSpeedCacheDuration returns the current cache duration (thread-safe)
func (m *PoolMetrics) getSpeedCacheDuration() time.Duration {
	m.speedCache.mu.RLock()
	defer m.speedCache.mu.RUnlock()
	return m.speedCache.cacheDuration
}

// getSpeedCacheAge returns the age of the current cached values (thread-safe)
func (m *PoolMetrics) getSpeedCacheAge() time.Duration {
	m.speedCache.mu.RLock()
	defer m.speedCache.mu.RUnlock()
	if m.speedCache.lastCalculated.IsZero() {
		return 0
	}
	return time.Since(m.speedCache.lastCalculated)
}

// RegisterActiveConnection registers a connection as active for metrics tracking
// This should be called when a connection is acquired from the pool
func (m *PoolMetrics) RegisterActiveConnection(connectionID string, conn nntpcli.Connection) {
	now := time.Now()
	m.activeConnections.Store(connectionID, conn)
	
	// Update cleanup tracker
	if m.cleanupTracker != nil {
		m.cleanupTracker.mu.Lock()
		m.cleanupTracker.connections[connectionID] = now
		m.cleanupTracker.mu.Unlock()
	}
}

// UnregisterActiveConnection removes a connection from active tracking
// This should be called when a connection is released back to the pool or destroyed
func (m *PoolMetrics) UnregisterActiveConnection(connectionID string) {
	m.activeConnections.Delete(connectionID)
	
	// Remove from cleanup tracker
	if m.cleanupTracker != nil {
		m.cleanupTracker.mu.Lock()
		delete(m.cleanupTracker.connections, connectionID)
		m.cleanupTracker.mu.Unlock()
	}
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

// UpdateConnectionActivity updates the last-seen time for an active connection
// This should be called when a connection is used to prevent it from being cleaned up as stale
func (m *PoolMetrics) UpdateConnectionActivity(connectionID string) {
	if m.cleanupTracker != nil {
		now := time.Now()
		m.cleanupTracker.mu.Lock()
		m.cleanupTracker.connections[connectionID] = now
		m.cleanupTracker.mu.Unlock()
	}
}

// ConnectionCleanupStats represents statistics about connection cleanup
type ConnectionCleanupStats struct {
	TrackedConnections  int           `json:"tracked_connections"`
	LastCleanupTime     time.Time     `json:"last_cleanup_time"`
	CleanupInterval     time.Duration `json:"cleanup_interval"`
	ConnectionTimeout   time.Duration `json:"connection_timeout"`
	NextCleanupTime     time.Time     `json:"next_cleanup_time"`
}

// GetConnectionCleanupStats returns statistics about the connection cleanup system
func (m *PoolMetrics) GetConnectionCleanupStats() ConnectionCleanupStats {
	if m.cleanupTracker == nil {
		return ConnectionCleanupStats{}
	}
	
	m.cleanupTracker.mu.RLock()
	defer m.cleanupTracker.mu.RUnlock()
	
	return ConnectionCleanupStats{
		TrackedConnections: len(m.cleanupTracker.connections),
		LastCleanupTime:    m.cleanupTracker.lastCleanup,
		CleanupInterval:    m.cleanupTracker.cleanupInterval,
		ConnectionTimeout:  m.cleanupTracker.connectionTimeout,
		NextCleanupTime:    m.cleanupTracker.lastCleanup.Add(m.cleanupTracker.cleanupInterval),
	}
}

// SetConnectionCleanupConfig updates the connection cleanup configuration
func (m *PoolMetrics) SetConnectionCleanupConfig(cleanupInterval, connectionTimeout time.Duration) {
	if m.cleanupTracker == nil {
		return
	}
	
	m.cleanupTracker.mu.Lock()
	defer m.cleanupTracker.mu.Unlock()
	
	if cleanupInterval > 0 {
		m.cleanupTracker.cleanupInterval = cleanupInterval
	}
	if connectionTimeout > 0 {
		m.cleanupTracker.connectionTimeout = connectionTimeout
	}
}

// ForceConnectionCleanup forces an immediate cleanup of stale connections
func (m *PoolMetrics) ForceConnectionCleanup() int {
	if m.cleanupTracker == nil {
		return 0
	}
	
	now := time.Now()
	m.cleanupTracker.mu.Lock()
	
	staleConnections := make([]string, 0)
	for connectionID, lastSeen := range m.cleanupTracker.connections {
		if now.Sub(lastSeen) > m.cleanupTracker.connectionTimeout {
			staleConnections = append(staleConnections, connectionID)
		}
	}
	
	m.cleanupTracker.lastCleanup = now
	m.cleanupTracker.mu.Unlock()
	
	// Remove stale connections
	for _, connectionID := range staleConnections {
		m.activeConnections.Delete(connectionID)
		
		m.cleanupTracker.mu.Lock()
		delete(m.cleanupTracker.connections, connectionID)
		m.cleanupTracker.mu.Unlock()
	}
	
	return len(staleConnections)
}

// MetricSummary represents aggregated metrics for a longer time period
type MetricSummary struct {
	StartTime time.Time `json:"start_time"`
	EndTime   time.Time `json:"end_time"`
	
	// Aggregated core metrics
	TotalConnectionsCreated   int64 `json:"total_connections_created"`
	TotalConnectionsDestroyed int64 `json:"total_connections_destroyed"`
	TotalAcquires             int64 `json:"total_acquires"`
	TotalReleases             int64 `json:"total_releases"`
	TotalErrors               int64 `json:"total_errors"`
	TotalRetries              int64 `json:"total_retries"`
	TotalAcquireWaitTime      int64 `json:"total_acquire_wait_time_ns"`
	
	// Aggregated traffic metrics
	TotalBytesDownloaded   int64 `json:"total_bytes_downloaded"`
	TotalBytesUploaded     int64 `json:"total_bytes_uploaded"`
	TotalArticlesRetrieved int64 `json:"total_articles_retrieved"`
	TotalArticlesPosted    int64 `json:"total_articles_posted"`
	TotalCommandCount      int64 `json:"total_command_count"`
	TotalCommandErrors     int64 `json:"total_command_errors"`
	
	// Computed metrics
	AverageConnectionsPerHour float64 `json:"average_connections_per_hour"`
	AverageErrorRate          float64 `json:"average_error_rate_percent"`
	AverageSuccessRate        float64 `json:"average_success_rate_percent"`
	AverageAcquireWaitTime    int64   `json:"average_acquire_wait_time_ns"`
	
	// Number of windows that were summarized
	WindowCount int `json:"window_count"`
}

// SummarizeHistoricalWindows creates a compressed summary of historical metric windows
func (m *PoolMetrics) SummarizeHistoricalWindows(startTime, endTime time.Time) *MetricSummary {
	if m.rollingMetrics == nil {
		return nil
	}
	
	m.rollingMetrics.mu.RLock()
	defer m.rollingMetrics.mu.RUnlock()
	
	summary := &MetricSummary{
		StartTime: startTime,
		EndTime:   endTime,
	}
	
	windowCount := 0
	
	// Aggregate metrics from all windows within the time range
	for _, window := range m.rollingMetrics.historicalWindows {
		// Check if window overlaps with our time range
		if window.EndTime.Before(startTime) || window.StartTime.After(endTime) {
			continue
		}
		
		summary.TotalConnectionsCreated += window.ConnectionsCreated
		summary.TotalConnectionsDestroyed += window.ConnectionsDestroyed
		summary.TotalAcquires += window.Acquires
		summary.TotalReleases += window.Releases
		summary.TotalErrors += window.Errors
		summary.TotalRetries += window.Retries
		summary.TotalAcquireWaitTime += window.AcquireWaitTime
		summary.TotalBytesDownloaded += window.BytesDownloaded
		summary.TotalBytesUploaded += window.BytesUploaded
		summary.TotalArticlesRetrieved += window.ArticlesRetrieved
		summary.TotalArticlesPosted += window.ArticlesPosted
		summary.TotalCommandCount += window.CommandCount
		summary.TotalCommandErrors += window.CommandErrors
		
		windowCount++
	}
	
	// Calculate averages and rates
	durationHours := endTime.Sub(startTime).Hours()
	if durationHours > 0 {
		summary.AverageConnectionsPerHour = float64(summary.TotalConnectionsCreated) / durationHours
	}
	
	if summary.TotalAcquires > 0 {
		summary.AverageErrorRate = float64(summary.TotalErrors) / float64(summary.TotalAcquires) * 100
		summary.AverageAcquireWaitTime = summary.TotalAcquireWaitTime / summary.TotalAcquires
	}
	
	if summary.TotalCommandCount > 0 {
		successfulCommands := summary.TotalCommandCount - summary.TotalCommandErrors
		summary.AverageSuccessRate = float64(successfulCommands) / float64(summary.TotalCommandCount) * 100
	}
	
	summary.WindowCount = windowCount
	
	return summary
}

// GetDailySummary returns a summary of metrics for the last 24 hours
func (m *PoolMetrics) GetDailySummary() *MetricSummary {
	now := time.Now()
	yesterday := now.Add(-24 * time.Hour)
	return m.SummarizeHistoricalWindows(yesterday, now)
}

// GetWeeklySummary returns a summary of metrics for the last 7 days
func (m *PoolMetrics) GetWeeklySummary() *MetricSummary {
	now := time.Now()
	weekAgo := now.Add(-7 * 24 * time.Hour)
	return m.SummarizeHistoricalWindows(weekAgo, now)
}

// CompressOldWindows removes detailed windows older than the retention period
// and optionally creates summaries before deletion
func (m *PoolMetrics) CompressOldWindows(createSummaries bool) []*MetricSummary {
	if m.rollingMetrics == nil {
		return nil
	}
	
	now := time.Now()
	retentionCutoff := now.Add(-m.rollingMetrics.config.DetailedRetentionDuration)
	
	m.rollingMetrics.mu.Lock()
	defer m.rollingMetrics.mu.Unlock()
	
	var summaries []*MetricSummary
	var keepWindows []*MetricWindow
	var compressWindows []*MetricWindow
	
	// Separate windows into keep vs compress
	for _, window := range m.rollingMetrics.historicalWindows {
		if window.EndTime.Before(retentionCutoff) {
			compressWindows = append(compressWindows, window)
		} else {
			keepWindows = append(keepWindows, window)
		}
	}
	
	// Create summaries if requested
	if createSummaries && len(compressWindows) > 0 {
		// Group old windows by day for daily summaries
		dayGroups := make(map[string][]*MetricWindow)
		for _, window := range compressWindows {
			dayKey := window.StartTime.Format("2006-01-02")
			dayGroups[dayKey] = append(dayGroups[dayKey], window)
		}
		
		// Create daily summaries
		for _, windows := range dayGroups {
			if len(windows) == 0 {
				continue
			}
			
			startTime := windows[0].StartTime
			endTime := windows[len(windows)-1].EndTime
			
			// Create summary for this day
			summary := &MetricSummary{
				StartTime: startTime,
				EndTime:   endTime,
			}
			
			// Aggregate all windows for this day
			for _, window := range windows {
				summary.TotalConnectionsCreated += window.ConnectionsCreated
				summary.TotalConnectionsDestroyed += window.ConnectionsDestroyed
				summary.TotalAcquires += window.Acquires
				summary.TotalReleases += window.Releases
				summary.TotalErrors += window.Errors
				summary.TotalRetries += window.Retries
				summary.TotalAcquireWaitTime += window.AcquireWaitTime
				summary.TotalBytesDownloaded += window.BytesDownloaded
				summary.TotalBytesUploaded += window.BytesUploaded
				summary.TotalArticlesRetrieved += window.ArticlesRetrieved
				summary.TotalArticlesPosted += window.ArticlesPosted
				summary.TotalCommandCount += window.CommandCount
				summary.TotalCommandErrors += window.CommandErrors
			}
			
			// Calculate computed metrics
			durationHours := endTime.Sub(startTime).Hours()
			if durationHours > 0 {
				summary.AverageConnectionsPerHour = float64(summary.TotalConnectionsCreated) / durationHours
			}
			
			if summary.TotalAcquires > 0 {
				summary.AverageErrorRate = float64(summary.TotalErrors) / float64(summary.TotalAcquires) * 100
				summary.AverageAcquireWaitTime = summary.TotalAcquireWaitTime / summary.TotalAcquires
			}
			
			if summary.TotalCommandCount > 0 {
				successfulCommands := summary.TotalCommandCount - summary.TotalCommandErrors
				summary.AverageSuccessRate = float64(successfulCommands) / float64(summary.TotalCommandCount) * 100
			}
			
			summary.WindowCount = len(windows)
			summaries = append(summaries, summary)
		}
	}
	
	// Keep only the recent windows
	m.rollingMetrics.historicalWindows = keepWindows
	
	return summaries
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
	TotalBytesDownloaded    int64   `json:"total_bytes_downloaded"`
	TotalBytesUploaded      int64   `json:"total_bytes_uploaded"`
	TotalArticlesRetrieved  int64   `json:"total_articles_retrieved"`
	TotalArticlesPosted     int64   `json:"total_articles_posted"`
	DownloadSpeed           float64 `json:"download_speed_bytes_per_sec"`            // Recent speed (based on time window)
	UploadSpeed             float64 `json:"upload_speed_bytes_per_sec"`              // Recent speed (based on time window)
	HistoricalDownloadSpeed float64 `json:"historical_download_speed_bytes_per_sec"` // Average since pool start
	HistoricalUploadSpeed   float64 `json:"historical_upload_speed_bytes_per_sec"`   // Average since pool start
	SpeedCalculationWindow  float64 `json:"speed_calculation_window_seconds"`        // Time window used for recent speed
	SpeedCacheDuration      float64 `json:"speed_cache_duration_seconds"`            // Cache duration for speed calculations
	SpeedCacheAge           float64 `json:"speed_cache_age_seconds"`                 // Age of current cached speed values

	// Performance metrics
	TotalCommandCount      int64         `json:"total_command_count"`
	TotalCommandErrors     int64         `json:"total_command_errors"`
	CommandSuccessRate     float64       `json:"command_success_rate_percent"`
	AverageAcquireWaitTime time.Duration `json:"average_acquire_wait_time"`

	// Error metrics
	TotalErrors  int64   `json:"total_errors"`
	TotalRetries int64   `json:"total_retries"`
	ErrorRate    float64 `json:"error_rate_percent"`

	// Rolling metrics information
	CurrentWindowStartTime   time.Time `json:"current_window_start_time"`
	CurrentWindowEndTime     time.Time `json:"current_window_end_time"`
	HistoricalWindowCount    int       `json:"historical_window_count"`
	WindowRotationInterval   float64   `json:"window_rotation_interval_seconds"`
	DetailedRetentionPeriod  float64   `json:"detailed_retention_period_hours"`
	MaxHistoricalWindows     int       `json:"max_historical_windows"`
	
	// Memory management
	MemoryThreshold          uint64    `json:"memory_threshold_bytes"`
	CurrentMemoryUsage       uint64    `json:"current_memory_usage_bytes"`
	AutoCleanupEnabled       bool      `json:"auto_cleanup_enabled"`
	LastMemoryCheck          time.Time `json:"last_memory_check"`
	
	// Connection cleanup statistics
	ConnectionCleanupStats ConnectionCleanupStats `json:"connection_cleanup_stats"`
	
	// Daily and weekly summaries
	DailySummary  *MetricSummary `json:"daily_summary,omitempty"`
	WeeklySummary *MetricSummary `json:"weekly_summary,omitempty"`

	// Provider-specific metrics
	ProviderMetrics []ProviderMetricsSnapshot `json:"provider_metrics"`
}

// ProviderMetricsSnapshot contains metrics for a specific provider
type ProviderMetricsSnapshot struct {
	ProviderID string        `json:"provider_id"`
	Host       string        `json:"host"`
	Username   string        `json:"username"`
	State      ProviderState `json:"state"`

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
	TotalBytesDownloaded   int64         `json:"total_bytes_downloaded"`
	TotalBytesUploaded     int64         `json:"total_bytes_uploaded"`
	TotalCommands          int64         `json:"total_commands"`
	TotalCommandErrors     int64         `json:"total_command_errors"`
	SuccessRate            float64       `json:"success_rate_percent"`
	AverageConnectionAge   time.Duration `json:"average_connection_age"`
	TotalArticlesRetrieved int64         `json:"total_articles_retrieved"`
	TotalArticlesPosted    int64         `json:"total_articles_posted"`
}

type AggregatedMetrics struct {
	TotalBytesDownloaded   int64
	TotalBytesUploaded     int64
	TotalCommands          int64
	TotalCommandErrors     int64
	TotalArticlesRetrieved int64
	TotalArticlesPosted    int64
	SuccessRate            float64
	AverageConnectionAge   time.Duration
}

// GetSnapshot returns a comprehensive snapshot of all metrics
// This method aggregates data from puddle pools and individual connections
func (m *PoolMetrics) GetSnapshot(pools []*providerPool) PoolMetricsSnapshot {
	timestamp := time.Now()
	uptime := m.GetUptime()

	// Aggregate all connection metrics first to get totals
	var (
		globalTotalBytesDownloaded,
		globalTotalBytesUploaded,
		globalTotalCommands,
		globalTotalCommandErrors int64
		globalArticlesRetrieved,
		globalArticlesPosted int64
	)

	for _, pool := range pools {
		aggregated := m.aggregateConnectionMetrics(pool)
		globalTotalBytesDownloaded += aggregated.TotalBytesDownloaded
		globalTotalBytesUploaded += aggregated.TotalBytesUploaded
		globalTotalCommands += aggregated.TotalCommands
		globalTotalCommandErrors += aggregated.TotalCommandErrors
		globalArticlesRetrieved += aggregated.TotalArticlesRetrieved
		globalArticlesPosted += aggregated.TotalArticlesPosted
	}

	// Calculate speed metrics using recent activity within the time window
	recentDownloadSpeed, recentUploadSpeed := m.calculateRecentSpeeds(pools)

	// Also provide historical average speeds for comparison
	uptimeSeconds := uptime.Seconds()
	var historicalDownloadSpeed, historicalUploadSpeed float64
	if uptimeSeconds > 0 {
		historicalDownloadSpeed = float64(globalTotalBytesDownloaded) / uptimeSeconds
		historicalUploadSpeed = float64(globalTotalBytesUploaded) / uptimeSeconds
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
			State:                   pool.GetState(),
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
		providerSnapshot.TotalArticlesRetrieved = aggregatedMetrics.TotalArticlesRetrieved
		providerSnapshot.TotalArticlesPosted = aggregatedMetrics.TotalArticlesPosted

		providerMetrics = append(providerMetrics, providerSnapshot)

		// Add to totals
		totalAcquired += stat.AcquiredResources()
		totalIdle += stat.IdleResources()
		totalConnections += stat.TotalResources()
	}

	// Get rolling metrics information
	var currentWindowStart, currentWindowEnd time.Time
	var historicalWindowCount int
	var windowRotationInterval, detailedRetentionPeriod float64
	var maxHistoricalWindows int
	var memoryThreshold uint64
	var autoCleanupEnabled bool
	var lastMemoryCheck time.Time
	
	if m.rollingMetrics != nil {
		m.rollingMetrics.mu.RLock()
		if m.rollingMetrics.currentWindow != nil {
			currentWindowStart = m.rollingMetrics.currentWindow.StartTime
			currentWindowEnd = m.rollingMetrics.currentWindow.EndTime
		}
		historicalWindowCount = len(m.rollingMetrics.historicalWindows)
		windowRotationInterval = m.rollingMetrics.config.RotationInterval.Seconds()
		detailedRetentionPeriod = m.rollingMetrics.config.DetailedRetentionDuration.Hours()
		maxHistoricalWindows = m.rollingMetrics.config.MaxHistoricalWindows
		memoryThreshold = m.rollingMetrics.config.MemoryThresholdBytes
		autoCleanupEnabled = m.rollingMetrics.config.AutoCleanupEnabled
		lastMemoryCheck = m.rollingMetrics.lastMemoryCheck
		m.rollingMetrics.mu.RUnlock()
	}
	
	// Get current memory usage
	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)
	
	// Get connection cleanup stats
	connectionCleanupStats := m.GetConnectionCleanupStats()
	
	// Get daily and weekly summaries
	dailySummary := m.GetDailySummary()
	weeklySummary := m.GetWeeklySummary()

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
		TotalArticlesRetrieved:    globalArticlesRetrieved,             // Will be calculated from connections if needed
		TotalArticlesPosted:       globalArticlesPosted,                // Will be calculated from connections if needed
		DownloadSpeed:             recentDownloadSpeed,                 // Use recent speed (current activity)
		UploadSpeed:               recentUploadSpeed,                   // Use recent speed (current activity)
		HistoricalDownloadSpeed:   historicalDownloadSpeed,             // Historical average since pool start
		HistoricalUploadSpeed:     historicalUploadSpeed,               // Historical average since pool start
		SpeedCalculationWindow:    m.speedWindowDuration.Seconds(),     // Time window used
		SpeedCacheDuration:        m.getSpeedCacheDuration().Seconds(), // Cache duration
		SpeedCacheAge:             m.getSpeedCacheAge().Seconds(),      // Cache age
		TotalCommandCount:         globalTotalCommands,
		TotalCommandErrors:        globalTotalCommandErrors,
		CommandSuccessRate:        commandSuccessRate,
		AverageAcquireWaitTime:    m.GetAverageAcquireWaitTime(),
		TotalErrors:               m.GetTotalErrors(),
		TotalRetries:              m.GetTotalRetries(),
		ErrorRate:                 errorRate,
		
		// Rolling metrics information
		CurrentWindowStartTime:    currentWindowStart,
		CurrentWindowEndTime:      currentWindowEnd,
		HistoricalWindowCount:     historicalWindowCount,
		WindowRotationInterval:    windowRotationInterval,
		DetailedRetentionPeriod:   detailedRetentionPeriod,
		MaxHistoricalWindows:      maxHistoricalWindows,
		
		// Memory management
		MemoryThreshold:           memoryThreshold,
		CurrentMemoryUsage:        memStats.Alloc,
		AutoCleanupEnabled:        autoCleanupEnabled,
		LastMemoryCheck:           lastMemoryCheck,
		
		// Connection cleanup statistics
		ConnectionCleanupStats:    connectionCleanupStats,
		
		// Daily and weekly summaries
		DailySummary:              dailySummary,
		WeeklySummary:             weeklySummary,
		
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
	var totalArticlesRetrieved, totalArticlesPosted int64

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
				totalArticlesRetrieved += snapshot.ArticlesRetrieved
				totalArticlesPosted += snapshot.ArticlesPosted
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
			totalArticlesRetrieved += snapshot.ArticlesRetrieved
			totalArticlesPosted += snapshot.ArticlesPosted
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
		TotalBytesDownloaded:   totalBytesDownloaded,
		TotalBytesUploaded:     totalBytesUploaded,
		TotalCommands:          totalCommands,
		TotalCommandErrors:     totalCommandErrors,
		SuccessRate:            successRate,
		AverageConnectionAge:   averageConnectionAge,
		TotalArticlesRetrieved: totalArticlesRetrieved,
		TotalArticlesPosted:    totalArticlesPosted,
	}
}

// calculateRecentSpeeds calculates current upload/download speeds with caching to avoid blocking pool operations
// This method uses cached values when available to minimize performance impact on the connection pool
func (m *PoolMetrics) calculateRecentSpeeds(pools []*providerPool) (downloadSpeed, uploadSpeed float64) {
	now := time.Now()

	// Check if we have fresh cached values first
	m.speedCache.mu.RLock()
	if !m.speedCache.lastCalculated.IsZero() &&
		now.Sub(m.speedCache.lastCalculated) < m.speedCache.cacheDuration {
		// Return cached values
		downloadSpeed = m.speedCache.downloadSpeed
		uploadSpeed = m.speedCache.uploadSpeed
		m.speedCache.mu.RUnlock()
		return downloadSpeed, uploadSpeed
	}
	m.speedCache.mu.RUnlock()

	// Cache is stale or empty, calculate fresh values
	downloadSpeed, uploadSpeed = m.calculateRecentSpeedsUncached()

	// Update cache with new values
	m.speedCache.mu.Lock()
	m.speedCache.lastCalculated = now
	m.speedCache.downloadSpeed = downloadSpeed
	m.speedCache.uploadSpeed = uploadSpeed
	m.speedCache.mu.Unlock()

	return downloadSpeed, uploadSpeed
}

// calculateRecentSpeedsUncached performs the actual speed calculation using only active connections
// This method provides maximum performance by completely avoiding any pool interference
func (m *PoolMetrics) calculateRecentSpeedsUncached() (downloadSpeed, uploadSpeed float64) {
	var recentBytesDownloaded, recentBytesUploaded int64
	var activeConnectionCount int

	// Use only active connections - zero pool blocking, maximum performance
	m.activeConnections.Range(func(key, value interface{}) bool {
		conn, ok := value.(nntpcli.Connection)
		if !ok || conn == nil {
			return true
		}

		metrics := conn.GetMetrics()
		if metrics != nil {
			// Active connections represent current activity
			snapshot := metrics.GetSnapshot()
			connectionAge := metrics.GetConnectionAge()

			if connectionAge <= m.speedWindowDuration {
				// Connection is newer than our time window - count all its bytes
				recentBytesDownloaded += snapshot.BytesDownloaded
				recentBytesUploaded += snapshot.BytesUploaded
			} else {
				// Connection is older than window - estimate recent activity
				// Since it's active, we can be generous with the estimation
				windowRatio := float64(m.speedWindowDuration) / float64(connectionAge)
				recentBytesDownloaded += int64(float64(snapshot.BytesDownloaded) * windowRatio * 0.8)
				recentBytesUploaded += int64(float64(snapshot.BytesUploaded) * windowRatio * 0.8)
			}
			activeConnectionCount++
		}
		return true
	})

	// Calculate speeds based on active connection activity
	windowSeconds := m.speedWindowDuration.Seconds()
	if windowSeconds > 0 && activeConnectionCount > 0 {
		downloadSpeed = float64(recentBytesDownloaded) / windowSeconds
		uploadSpeed = float64(recentBytesUploaded) / windowSeconds
	}

	return downloadSpeed, uploadSpeed
}

// SetRetentionConfig updates the metric retention configuration
func (m *PoolMetrics) SetRetentionConfig(config MetricRetentionConfig) {
	if m.rollingMetrics == nil {
		return
	}
	
	m.rollingMetrics.mu.Lock()
	defer m.rollingMetrics.mu.Unlock()
	
	// Validate and set configuration values
	if config.DetailedRetentionDuration > 0 {
		m.rollingMetrics.config.DetailedRetentionDuration = config.DetailedRetentionDuration
	}
	if config.RotationInterval > 0 {
		m.rollingMetrics.config.RotationInterval = config.RotationInterval
		
		// Update current window end time if needed
		if m.rollingMetrics.currentWindow != nil {
			m.rollingMetrics.currentWindow.EndTime = m.rollingMetrics.currentWindow.StartTime.Add(config.RotationInterval)
		}
	}
	if config.MaxHistoricalWindows > 0 {
		m.rollingMetrics.config.MaxHistoricalWindows = config.MaxHistoricalWindows
		
		// Trim historical windows if the new limit is smaller
		if len(m.rollingMetrics.historicalWindows) > config.MaxHistoricalWindows {
			excess := len(m.rollingMetrics.historicalWindows) - config.MaxHistoricalWindows
			m.rollingMetrics.historicalWindows = m.rollingMetrics.historicalWindows[excess:]
		}
	}
	if config.MemoryThresholdBytes > 0 {
		m.rollingMetrics.config.MemoryThresholdBytes = config.MemoryThresholdBytes
	}
	
	m.rollingMetrics.config.AutoCleanupEnabled = config.AutoCleanupEnabled
}

// GetRetentionConfig returns the current metric retention configuration
func (m *PoolMetrics) GetRetentionConfig() MetricRetentionConfig {
	if m.rollingMetrics == nil {
		return MetricRetentionConfig{}
	}
	
	m.rollingMetrics.mu.RLock()
	defer m.rollingMetrics.mu.RUnlock()
	
	return m.rollingMetrics.config
}

// SetRotationInterval updates how often metrics are rotated to new time windows
func (m *PoolMetrics) SetRotationInterval(interval time.Duration) {
	if m.rollingMetrics == nil || interval <= 0 {
		return
	}
	
	m.rollingMetrics.mu.Lock()
	defer m.rollingMetrics.mu.Unlock()
	
	m.rollingMetrics.config.RotationInterval = interval
	
	// Update current window end time if needed
	if m.rollingMetrics.currentWindow != nil {
		m.rollingMetrics.currentWindow.EndTime = m.rollingMetrics.currentWindow.StartTime.Add(interval)
	}
}

// SetDetailedRetentionDuration updates how long detailed metrics are kept before compression
func (m *PoolMetrics) SetDetailedRetentionDuration(duration time.Duration) {
	if m.rollingMetrics == nil || duration <= 0 {
		return
	}
	
	m.rollingMetrics.mu.Lock()
	defer m.rollingMetrics.mu.Unlock()
	
	m.rollingMetrics.config.DetailedRetentionDuration = duration
}

// SetMaxHistoricalWindows updates the maximum number of historical windows to keep
func (m *PoolMetrics) SetMaxHistoricalWindows(maxWindows int) {
	if m.rollingMetrics == nil || maxWindows <= 0 {
		return
	}
	
	m.rollingMetrics.mu.Lock()
	defer m.rollingMetrics.mu.Unlock()
	
	m.rollingMetrics.config.MaxHistoricalWindows = maxWindows
	
	// Trim historical windows if the new limit is smaller
	if len(m.rollingMetrics.historicalWindows) > maxWindows {
		excess := len(m.rollingMetrics.historicalWindows) - maxWindows
		m.rollingMetrics.historicalWindows = m.rollingMetrics.historicalWindows[excess:]
	}
}

// SetMemoryThreshold updates the memory threshold that triggers automatic cleanup
func (m *PoolMetrics) SetMemoryThreshold(thresholdBytes uint64) {
	if m.rollingMetrics == nil {
		return
	}
	
	m.rollingMetrics.mu.Lock()
	defer m.rollingMetrics.mu.Unlock()
	
	m.rollingMetrics.config.MemoryThresholdBytes = thresholdBytes
}

// EnableAutoCleanup enables or disables automatic cleanup when memory thresholds are exceeded
func (m *PoolMetrics) EnableAutoCleanup(enabled bool) {
	if m.rollingMetrics == nil {
		return
	}
	
	m.rollingMetrics.mu.Lock()
	defer m.rollingMetrics.mu.Unlock()
	
	m.rollingMetrics.config.AutoCleanupEnabled = enabled
}

// GetRollingMetricsStatus returns the current status of the rolling metrics system
type RollingMetricsStatus struct {
	Enabled                   bool      `json:"enabled"`
	CurrentWindowActive       bool      `json:"current_window_active"`
	CurrentWindowStartTime    time.Time `json:"current_window_start_time"`
	CurrentWindowEndTime      time.Time `json:"current_window_end_time"`
	CurrentWindowDuration     float64   `json:"current_window_duration_seconds"`
	HistoricalWindowCount     int       `json:"historical_window_count"`
	MaxHistoricalWindows      int       `json:"max_historical_windows"`
	RotationInterval          float64   `json:"rotation_interval_seconds"`
	DetailedRetentionDuration float64   `json:"detailed_retention_duration_hours"`
	MemoryThreshold           uint64    `json:"memory_threshold_bytes"`
	AutoCleanupEnabled        bool      `json:"auto_cleanup_enabled"`
	LastMemoryCheck           time.Time `json:"last_memory_check"`
	MemoryCheckInterval       float64   `json:"memory_check_interval_seconds"`
}

func (m *PoolMetrics) GetRollingMetricsStatus() RollingMetricsStatus {
	if m.rollingMetrics == nil {
		return RollingMetricsStatus{Enabled: false}
	}
	
	m.rollingMetrics.mu.RLock()
	defer m.rollingMetrics.mu.RUnlock()
	
	status := RollingMetricsStatus{
		Enabled:                   true,
		HistoricalWindowCount:     len(m.rollingMetrics.historicalWindows),
		MaxHistoricalWindows:      m.rollingMetrics.config.MaxHistoricalWindows,
		RotationInterval:          m.rollingMetrics.config.RotationInterval.Seconds(),
		DetailedRetentionDuration: m.rollingMetrics.config.DetailedRetentionDuration.Hours(),
		MemoryThreshold:           m.rollingMetrics.config.MemoryThresholdBytes,
		AutoCleanupEnabled:        m.rollingMetrics.config.AutoCleanupEnabled,
		LastMemoryCheck:           m.rollingMetrics.lastMemoryCheck,
		MemoryCheckInterval:       m.rollingMetrics.memoryCheckInterval.Seconds(),
	}
	
	if m.rollingMetrics.currentWindow != nil {
		status.CurrentWindowActive = true
		status.CurrentWindowStartTime = m.rollingMetrics.currentWindow.StartTime
		status.CurrentWindowEndTime = m.rollingMetrics.currentWindow.EndTime
		status.CurrentWindowDuration = m.rollingMetrics.currentWindow.EndTime.Sub(m.rollingMetrics.currentWindow.StartTime).Seconds()
	}
	
	return status
}

// ResetCounters resets all atomic counters while preserving rolling metrics
// This can be useful for periodic resets to prevent integer overflow
func (m *PoolMetrics) ResetCounters() {
	// Store current values in rolling metrics before reset
	m.PerformRotationCheck() // This will rotate current window if needed
	
	// Reset atomic counters
	atomic.StoreInt64(&m.totalConnectionsCreated, 0)
	atomic.StoreInt64(&m.totalConnectionsDestroyed, 0)
	atomic.StoreInt64(&m.totalAcquires, 0)
	atomic.StoreInt64(&m.totalReleases, 0)
	atomic.StoreInt64(&m.totalErrors, 0)
	atomic.StoreInt64(&m.totalRetries, 0)
	atomic.StoreInt64(&m.totalAcquireWaitTime, 0)
	
	// Update start time
	atomic.StoreInt64(&m.startTime, time.Now().UnixNano())
}

// GetMemoryUsage returns current memory usage information
type MemoryUsage struct {
	AllocatedBytes     uint64 `json:"allocated_bytes"`
	TotalAllocatedBytes uint64 `json:"total_allocated_bytes"`
	SystemBytes        uint64 `json:"system_bytes"`
	GCCount            uint32 `json:"gc_count"`
	ThresholdBytes     uint64 `json:"threshold_bytes"`
	ThresholdExceeded  bool   `json:"threshold_exceeded"`
}

func (m *PoolMetrics) GetMemoryUsage() MemoryUsage {
	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)
	
	var thresholdBytes uint64
	if m.rollingMetrics != nil {
		m.rollingMetrics.mu.RLock()
		thresholdBytes = m.rollingMetrics.config.MemoryThresholdBytes
		m.rollingMetrics.mu.RUnlock()
	}
	
	return MemoryUsage{
		AllocatedBytes:      memStats.Alloc,
		TotalAllocatedBytes: memStats.TotalAlloc,
		SystemBytes:         memStats.Sys,
		GCCount:             memStats.NumGC,
		ThresholdBytes:      thresholdBytes,
		ThresholdExceeded:   memStats.Alloc > thresholdBytes && thresholdBytes > 0,
	}
}
