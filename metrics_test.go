package nntppool

import (
	"fmt"
	"testing"
	"time"

	"github.com/javi11/nntppool/pkg/nntpcli"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

func TestPoolMetrics_Basic(t *testing.T) {
	metrics := NewPoolMetrics()
	require.NotNil(t, metrics)

	// Test initial state
	assert.Equal(t, int64(0), metrics.GetTotalConnectionsCreated())
	assert.Equal(t, int64(0), metrics.GetTotalConnectionsDestroyed())
	assert.Equal(t, int64(0), metrics.GetTotalActiveConnections())
	assert.Equal(t, int64(0), metrics.GetTotalAcquires())
	assert.Equal(t, int64(0), metrics.GetTotalReleases())
	assert.Equal(t, int64(0), metrics.GetTotalErrors())
	assert.Equal(t, int64(0), metrics.GetTotalRetries())
}

func TestPoolMetrics_ConnectionLifecycle(t *testing.T) {
	metrics := NewPoolMetrics()

	// Record connection creation
	metrics.RecordConnectionCreated()
	metrics.RecordConnectionCreated()
	assert.Equal(t, int64(2), metrics.GetTotalConnectionsCreated())
	// GetActiveConnections(nil) now returns 0 since no pools are provided
	assert.Equal(t, int64(0), metrics.GetTotalActiveConnections())

	// Record connection destruction
	metrics.RecordConnectionDestroyed()
	assert.Equal(t, int64(2), metrics.GetTotalConnectionsCreated())
	assert.Equal(t, int64(1), metrics.GetTotalConnectionsDestroyed())
	// GetActiveConnections(nil) still returns 0 since no pools are provided
	assert.Equal(t, int64(0), metrics.GetTotalActiveConnections())
}

func TestPoolMetrics_AcquireRelease(t *testing.T) {
	metrics := NewPoolMetrics()

	// Test acquire/release
	metrics.RecordAcquire()
	metrics.RecordAcquire()
	assert.Equal(t, int64(2), metrics.GetTotalAcquires())

	metrics.RecordRelease()
	assert.Equal(t, int64(2), metrics.GetTotalAcquires())
	assert.Equal(t, int64(1), metrics.GetTotalReleases())
}

func TestPoolMetrics_ErrorsAndRetries(t *testing.T) {
	metrics := NewPoolMetrics()

	// Test errors and retries
	metrics.RecordError()
	metrics.RecordError()
	assert.Equal(t, int64(2), metrics.GetTotalErrors())

	metrics.RecordRetry()
	metrics.RecordRetry()
	metrics.RecordRetry()
	assert.Equal(t, int64(3), metrics.GetTotalRetries())
}

func TestPoolMetrics_TrafficMetrics(t *testing.T) {
	// Traffic metrics are now aggregated from connections, not tracked at pool level
	// This test verifies the architectural change where pool only tracks pool operations
	metrics := NewPoolMetrics()

	// Pool-level metrics should only track pool operations
	assert.Equal(t, int64(0), metrics.GetTotalConnectionsCreated())
	assert.Equal(t, int64(0), metrics.GetTotalAcquires())
	assert.Equal(t, int64(0), metrics.GetTotalErrors())

	// Connection operations should not be tracked at pool level anymore
	// Traffic metrics will come from connection aggregation in GetSnapshot()
}

func TestPoolMetrics_CommandMetrics(t *testing.T) {
	// Command metrics are now aggregated from connections, not tracked at pool level
	// This test verifies the architectural change where pool only tracks pool operations
	metrics := NewPoolMetrics()

	// Pool-level metrics should only track pool operations
	assert.Equal(t, int64(0), metrics.GetTotalConnectionsCreated())
	assert.Equal(t, int64(0), metrics.GetTotalAcquires())
	assert.Equal(t, int64(0), metrics.GetTotalErrors())

	// Command metrics will come from connection aggregation in GetSnapshot()
}

func TestPoolMetrics_AcquireWaitTime(t *testing.T) {
	metrics := NewPoolMetrics()

	// Test wait time recording
	duration1 := 100 * time.Millisecond
	duration2 := 200 * time.Millisecond

	metrics.RecordAcquire()
	metrics.RecordAcquireWaitTime(duration1)

	metrics.RecordAcquire()
	metrics.RecordAcquireWaitTime(duration2)

	averageWait := metrics.GetAverageAcquireWaitTime()
	expected := (duration1 + duration2) / 2
	assert.Equal(t, expected, averageWait)
}

func TestPoolMetrics_Uptime(t *testing.T) {
	metrics := NewPoolMetrics()

	// Sleep for a short time to ensure uptime is measurable
	time.Sleep(10 * time.Millisecond)

	uptime := metrics.GetUptime()
	assert.True(t, uptime > 10*time.Millisecond)
	assert.True(t, uptime < 1*time.Second) // Should be reasonable
}

func TestPoolMetrics_ConcurrentAccess(t *testing.T) {
	metrics := NewPoolMetrics()

	// Test concurrent access to metrics (should not panic)
	done := make(chan bool, 10)

	for i := 0; i < 10; i++ {
		go func() {
			defer func() { done <- true }()

			for j := 0; j < 100; j++ {
				metrics.RecordConnectionCreated()
				metrics.RecordAcquire()

				// Read metrics
				_ = metrics.GetTotalConnectionsCreated()
				_ = metrics.GetTotalAcquires()
			}
		}()
	}

	// Wait for all goroutines to complete
	for i := 0; i < 10; i++ {
		<-done
	}

	// Verify final state
	assert.Equal(t, int64(1000), metrics.GetTotalConnectionsCreated())
	assert.Equal(t, int64(1000), metrics.GetTotalAcquires())
}

func TestPoolMetrics_EmptySnapshot(t *testing.T) {
	metrics := NewPoolMetrics()

	// Test snapshot with no pools
	snapshot := metrics.GetSnapshot(nil)

	assert.NotZero(t, snapshot.Timestamp)
	assert.True(t, snapshot.Uptime >= 0)
	assert.Equal(t, int64(0), snapshot.TotalConnectionsCreated)
	assert.Equal(t, int64(0), snapshot.TotalBytesDownloaded)
	assert.Equal(t, float64(0), snapshot.DownloadSpeed)
	assert.Equal(t, float64(0), snapshot.CommandSuccessRate)
	assert.Empty(t, snapshot.ProviderMetrics)
}

func TestPoolMetrics_SnapshotCalculations(t *testing.T) {
	metrics := NewPoolMetrics()

	// Add some pool-level metrics data
	metrics.RecordAcquire()
	metrics.RecordError()

	// Wait a bit to ensure uptime is measurable
	time.Sleep(10 * time.Millisecond)

	snapshot := metrics.GetSnapshot(nil)

	// Verify calculated fields
	assert.Equal(t, float64(0), snapshot.DownloadSpeed)           // No connection data = 0
	assert.Equal(t, float64(0), snapshot.UploadSpeed)             // No connection data = 0
	assert.Equal(t, float64(0), snapshot.HistoricalDownloadSpeed) // No connection data = 0
	assert.Equal(t, float64(0), snapshot.HistoricalUploadSpeed)   // No connection data = 0
	assert.Equal(t, float64(0), snapshot.CommandSuccessRate)      // No connection data = 0
	assert.Equal(t, float64(100), snapshot.ErrorRate)             // 1 error out of 1 acquire = 100%
	assert.Equal(t, int64(0), snapshot.TotalBytesDownloaded)      // No connection data = 0
	assert.Equal(t, int64(0), snapshot.TotalBytesUploaded)        // No connection data = 0
	assert.Equal(t, 60.0, snapshot.SpeedCalculationWindow)        // Default 60 second window
}

func TestPoolMetrics_SpeedCalculationWindow(t *testing.T) {
	metrics := NewPoolMetrics()

	// Test default window
	assert.Equal(t, 60*time.Second, metrics.speedWindowDuration)

	// Test setting custom window
	metrics.SetSpeedWindowDuration(30 * time.Second)
	assert.Equal(t, 30*time.Second, metrics.speedWindowDuration)

	// Test invalid duration defaults to 60 seconds
	metrics.SetSpeedWindowDuration(-5 * time.Second)
	assert.Equal(t, 60*time.Second, metrics.speedWindowDuration)

	metrics.SetSpeedWindowDuration(0)
	assert.Equal(t, 60*time.Second, metrics.speedWindowDuration)
}

func TestPoolMetrics_RecentSpeedCalculation(t *testing.T) {
	metrics := NewPoolMetrics()

	// Set a short window for testing
	metrics.SetSpeedWindowDuration(5 * time.Second)

	// Test with no pools - should return 0 speed
	downloadSpeed, uploadSpeed := metrics.calculateRecentSpeeds(nil)
	assert.Equal(t, float64(0), downloadSpeed)
	assert.Equal(t, float64(0), uploadSpeed)

	// Test with empty pools slice
	emptyPools := []*providerPool{}
	downloadSpeed, uploadSpeed = metrics.calculateRecentSpeeds(emptyPools)
	assert.Equal(t, float64(0), downloadSpeed)
	assert.Equal(t, float64(0), uploadSpeed)
}

func TestPoolMetrics_SnapshotSpeedFields(t *testing.T) {
	metrics := NewPoolMetrics()

	// Set a custom window duration
	metrics.SetSpeedWindowDuration(30 * time.Second)

	// Wait a bit for measurable uptime
	time.Sleep(10 * time.Millisecond)

	snapshot := metrics.GetSnapshot(nil)

	// Verify new speed fields are present and correct
	assert.Equal(t, float64(0), snapshot.DownloadSpeed)           // Recent speed
	assert.Equal(t, float64(0), snapshot.UploadSpeed)             // Recent speed
	assert.Equal(t, float64(0), snapshot.HistoricalDownloadSpeed) // Historical average
	assert.Equal(t, float64(0), snapshot.HistoricalUploadSpeed)   // Historical average
	assert.Equal(t, 30.0, snapshot.SpeedCalculationWindow)        // Custom window duration

	// Verify timestamp and uptime are still working
	assert.True(t, snapshot.Timestamp.After(time.Now().Add(-1*time.Second)))
	assert.True(t, snapshot.Uptime > 10*time.Millisecond)
	assert.True(t, snapshot.Uptime < 1*time.Second)
}

func TestPoolMetrics_SpeedFieldsInSnapshot(t *testing.T) {
	metrics := NewPoolMetrics()

	// Add some pool-level activity to ensure uptime calculation works
	metrics.RecordAcquire()
	metrics.RecordConnectionCreated()

	// Wait to ensure measurable uptime
	time.Sleep(20 * time.Millisecond)

	snapshot := metrics.GetSnapshot(nil)

	// Should have both recent and historical speeds (both 0 with no connections)
	assert.Equal(t, float64(0), snapshot.DownloadSpeed)
	assert.Equal(t, float64(0), snapshot.UploadSpeed)
	assert.Equal(t, float64(0), snapshot.HistoricalDownloadSpeed)
	assert.Equal(t, float64(0), snapshot.HistoricalUploadSpeed)

	// Should have speed calculation window and cache info
	assert.Equal(t, 60.0, snapshot.SpeedCalculationWindow)
	assert.Equal(t, 5.0, snapshot.SpeedCacheDuration) // Default 5 second cache
	assert.True(t, snapshot.SpeedCacheAge >= 0)       // Cache age should be non-negative

	// Other metrics should still work
	assert.Equal(t, int64(1), snapshot.TotalAcquires)
	assert.Equal(t, int64(1), snapshot.TotalConnectionsCreated)
	assert.True(t, snapshot.Uptime > 20*time.Millisecond)
}

func TestPoolMetrics_CachedSpeedCalculation(t *testing.T) {
	metrics := NewPoolMetrics()

	// Set short cache duration for testing
	metrics.SetSpeedCacheDuration(100 * time.Millisecond)

	// First call should calculate and cache
	downloadSpeed1, uploadSpeed1 := metrics.calculateRecentSpeeds(nil)
	assert.Equal(t, float64(0), downloadSpeed1)
	assert.Equal(t, float64(0), uploadSpeed1)

	// Second call immediately after should return cached values
	start := time.Now()
	downloadSpeed2, uploadSpeed2 := metrics.calculateRecentSpeeds(nil)
	elapsed := time.Since(start)

	assert.Equal(t, downloadSpeed1, downloadSpeed2)
	assert.Equal(t, uploadSpeed1, uploadSpeed2)
	assert.True(t, elapsed < 50*time.Microsecond) // Should be very fast (cached)

	// Wait for cache to expire
	time.Sleep(150 * time.Millisecond)

	// Third call should recalculate
	downloadSpeed3, uploadSpeed3 := metrics.calculateRecentSpeeds(nil)
	assert.Equal(t, float64(0), downloadSpeed3) // Still 0 with no connections
	assert.Equal(t, float64(0), uploadSpeed3)
}

func TestPoolMetrics_SpeedCacheConfiguration(t *testing.T) {
	metrics := NewPoolMetrics()

	// Test default cache duration
	assert.Equal(t, 5*time.Second, metrics.getSpeedCacheDuration())

	// Test setting cache duration
	metrics.SetSpeedCacheDuration(10 * time.Second)
	assert.Equal(t, 10*time.Second, metrics.getSpeedCacheDuration())

	// Test invalid duration defaults to 5 seconds
	metrics.SetSpeedCacheDuration(-1 * time.Second)
	assert.Equal(t, 5*time.Second, metrics.getSpeedCacheDuration())

	metrics.SetSpeedCacheDuration(0)
	assert.Equal(t, 5*time.Second, metrics.getSpeedCacheDuration())
}

func TestPoolMetrics_SpeedCacheAge(t *testing.T) {
	metrics := NewPoolMetrics()

	// Initially no cache, age should be 0
	assert.Equal(t, time.Duration(0), metrics.getSpeedCacheAge())

	// After first calculation, should have some age
	metrics.calculateRecentSpeeds(nil)
	age1 := metrics.getSpeedCacheAge()
	assert.True(t, age1 >= 0)
	assert.True(t, age1 < 10*time.Millisecond)

	// Wait a bit and check age increased
	time.Sleep(10 * time.Millisecond)
	age2 := metrics.getSpeedCacheAge()
	assert.True(t, age2 > age1)
	assert.True(t, age2 >= 10*time.Millisecond)
}

func TestPoolMetrics_SnapshotCacheFields(t *testing.T) {
	metrics := NewPoolMetrics()

	// Set custom cache duration
	metrics.SetSpeedCacheDuration(3 * time.Second)

	// Get snapshot (this will trigger speed calculation)
	snapshot := metrics.GetSnapshot(nil)

	// Verify cache fields are populated
	assert.Equal(t, 3.0, snapshot.SpeedCacheDuration)
	assert.True(t, snapshot.SpeedCacheAge >= 0)
	assert.True(t, snapshot.SpeedCacheAge < 1.0) // Should be very recent

	// Wait a bit and get another snapshot
	time.Sleep(50 * time.Millisecond)
	snapshot2 := metrics.GetSnapshot(nil)

	// Cache age should have increased (cached values used)
	assert.True(t, snapshot2.SpeedCacheAge > snapshot.SpeedCacheAge)
	assert.True(t, snapshot2.SpeedCacheAge >= 0.05) // At least 50ms old
}

func TestPoolMetrics_NonBlockingSpeedCalculation(t *testing.T) {
	metrics := NewPoolMetrics()

	// Test that speed calculation doesn't panic with nil pools
	downloadSpeed, uploadSpeed := metrics.calculateRecentSpeeds(nil)
	assert.Equal(t, float64(0), downloadSpeed)
	assert.Equal(t, float64(0), uploadSpeed)

	// Test with empty pool slice
	emptyPools := []*providerPool{}
	downloadSpeed, uploadSpeed = metrics.calculateRecentSpeeds(emptyPools)
	assert.Equal(t, float64(0), downloadSpeed)
	assert.Equal(t, float64(0), uploadSpeed)

	// Test performance - should be fast even when called repeatedly
	start := time.Now()
	for i := 0; i < 100; i++ {
		metrics.calculateRecentSpeeds(nil)
	}
	elapsed := time.Since(start)

	// Should be very fast due to caching
	assert.True(t, elapsed < 10*time.Millisecond, "100 cached calls took %v", elapsed)
}

func TestPoolMetrics_ActiveOnlySpeedCalculation(t *testing.T) {
	metrics := NewPoolMetrics()

	// Test with no activity - should return 0
	downloadSpeed, uploadSpeed := metrics.calculateRecentSpeedsUncached()
	assert.Equal(t, float64(0), downloadSpeed)
	assert.Equal(t, float64(0), uploadSpeed)

	// Record some download/upload activity at pool level
	metrics.RecordDownload(3000)
	metrics.RecordUpload(1300)

	// Calculate speed based on rolling window
	downloadSpeed, uploadSpeed = metrics.calculateRecentSpeedsUncached()

	// Should have non-zero speeds based on recorded bytes
	assert.True(t, downloadSpeed > 0, "Download speed should be > 0 after recording downloads")
	assert.True(t, uploadSpeed > 0, "Upload speed should be > 0 after recording uploads")

	// Verify the speeds match expected values (bytes / window duration)
	expectedDownloadSpeed := float64(3000) / metrics.speedWindowDuration.Seconds()
	expectedUploadSpeed := float64(1300) / metrics.speedWindowDuration.Seconds()
	assert.Equal(t, expectedDownloadSpeed, downloadSpeed)
	assert.Equal(t, expectedUploadSpeed, uploadSpeed)
}

// Benchmark tests to ensure minimal performance overhead
func BenchmarkPoolMetrics_RecordConnectionCreated(b *testing.B) {
	metrics := NewPoolMetrics()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		metrics.RecordConnectionCreated()
	}
}

func BenchmarkPoolMetrics_RecordAcquire(b *testing.B) {
	metrics := NewPoolMetrics()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		metrics.RecordAcquire()
	}
}

func BenchmarkPoolMetrics_GetTotalAcquires(b *testing.B) {
	metrics := NewPoolMetrics()
	metrics.RecordAcquire()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_ = metrics.GetTotalAcquires()
	}
}

func BenchmarkPoolMetrics_ConcurrentOperations(b *testing.B) {
	metrics := NewPoolMetrics()
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			metrics.RecordConnectionCreated()
			metrics.RecordAcquire()
			_ = metrics.GetTotalConnectionsCreated()
			_ = metrics.GetTotalAcquires()
		}
	})
}

// Tests for Active Connection Tracking functionality

func TestPoolMetrics_ActiveConnectionRegistry(t *testing.T) {
	metrics := NewPoolMetrics()

	// Test initial state
	assert.Equal(t, int64(0), metrics.GetTotalActiveConnections())
	activeMetrics := metrics.GetActiveConnectionMetrics()
	assert.Equal(t, 0, activeMetrics.Count)
	assert.Equal(t, int64(0), activeMetrics.TotalBytesDownloaded)
	assert.Equal(t, int64(0), activeMetrics.TotalBytesUploaded)
	assert.Equal(t, int64(0), activeMetrics.TotalCommands)
	assert.Equal(t, float64(0), activeMetrics.SuccessRate)
}

func TestPoolMetrics_ActiveConnectionRegistration(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	metrics := NewPoolMetrics()

	// Create mock connections
	mockConn1 := nntpcli.NewMockConnection(ctrl)
	mockConn2 := nntpcli.NewMockConnection(ctrl)

	// Register active connections
	metrics.RegisterActiveConnection("conn1", mockConn1)
	assert.Equal(t, int64(1), metrics.GetTotalActiveConnections())

	metrics.RegisterActiveConnection("conn2", mockConn2)
	assert.Equal(t, int64(2), metrics.GetTotalActiveConnections())

	// Get active metrics - now only returns count since connection metrics are removed
	activeMetrics := metrics.GetActiveConnectionMetrics()
	assert.Equal(t, 2, activeMetrics.Count)
	// Connection-level metrics no longer exist - these should all be zero
	assert.Equal(t, int64(0), activeMetrics.TotalBytesDownloaded)
	assert.Equal(t, int64(0), activeMetrics.TotalBytesUploaded)
	assert.Equal(t, int64(0), activeMetrics.TotalCommands)
	assert.Equal(t, int64(0), activeMetrics.TotalCommandErrors)
	assert.Equal(t, float64(0), activeMetrics.SuccessRate)
	assert.Equal(t, time.Duration(0), activeMetrics.AverageConnectionAge)

	// Unregister connections
	metrics.UnregisterActiveConnection("conn1")
	assert.Equal(t, int64(1), metrics.GetTotalActiveConnections())

	metrics.UnregisterActiveConnection("conn2")
	assert.Equal(t, int64(0), metrics.GetTotalActiveConnections())

	// Check final state
	finalMetrics := metrics.GetActiveConnectionMetrics()
	assert.Equal(t, 0, finalMetrics.Count)
	assert.Equal(t, int64(0), finalMetrics.TotalBytesDownloaded)
	assert.Equal(t, int64(0), finalMetrics.TotalBytesUploaded)
}

func TestPoolMetrics_ActiveConnectionConcurrency(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	metrics := NewPoolMetrics()

	// Test concurrent registration and unregistration
	const numGoroutines = 10
	const connectionsPerGoroutine = 5

	done := make(chan bool, numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		go func(routineID int) {
			defer func() { done <- true }()

			for j := 0; j < connectionsPerGoroutine; j++ {
				mockConn := nntpcli.NewMockConnection(ctrl)
				connID := fmt.Sprintf("routine%d-conn%d", routineID, j)
				metrics.RegisterActiveConnection(connID, mockConn)
			}

			// Unregister connections
			for j := 0; j < connectionsPerGoroutine; j++ {
				connID := fmt.Sprintf("routine%d-conn%d", routineID, j)
				metrics.UnregisterActiveConnection(connID)
			}
		}(i)
	}

	// Wait for all goroutines to complete
	for i := 0; i < numGoroutines; i++ {
		<-done
	}

	// Verify final state - all connections should be unregistered
	assert.Equal(t, int64(0), metrics.GetTotalActiveConnections())
	finalMetrics := metrics.GetActiveConnectionMetrics()
	assert.Equal(t, 0, finalMetrics.Count)
}

func TestPoolMetrics_ActiveConnectionWithNilMetrics(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	metrics := NewPoolMetrics()

	// Create mock connection
	mockConn := nntpcli.NewMockConnection(ctrl)

	// Register connection
	metrics.RegisterActiveConnection("conn1", mockConn)
	assert.Equal(t, int64(1), metrics.GetTotalActiveConnections())

	// Get active metrics - connection-level metrics no longer exist
	activeMetrics := metrics.GetActiveConnectionMetrics()
	assert.Equal(t, 1, activeMetrics.Count)
	assert.Equal(t, int64(0), activeMetrics.TotalBytesDownloaded)
	assert.Equal(t, int64(0), activeMetrics.TotalBytesUploaded)
}

// Benchmark tests for active connection functionality
func BenchmarkPoolMetrics_RegisterActiveConnection(b *testing.B) {
	ctrl := gomock.NewController(b)
	defer ctrl.Finish()

	metrics := NewPoolMetrics()
	mockConn := nntpcli.NewMockConnection(ctrl)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		connID := fmt.Sprintf("conn%d", i)
		metrics.RegisterActiveConnection(connID, mockConn)
	}
}

func BenchmarkPoolMetrics_GetActiveConnectionMetrics(b *testing.B) {
	ctrl := gomock.NewController(b)
	defer ctrl.Finish()

	metrics := NewPoolMetrics()

	// Register some connections
	for i := 0; i < 100; i++ {
		mockConn := nntpcli.NewMockConnection(ctrl)
		metrics.RegisterActiveConnection(fmt.Sprintf("conn%d", i), mockConn)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = metrics.GetActiveConnectionMetrics()
	}
}

func BenchmarkPoolMetrics_CalculateRecentSpeeds(b *testing.B) {
	metrics := NewPoolMetrics()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = metrics.calculateRecentSpeeds(nil)
	}
}

func BenchmarkPoolMetrics_CalculateRecentSpeedsUncached(b *testing.B) {
	metrics := NewPoolMetrics()

	// Set cache duration to 0 to force recalculation every time
	metrics.SetSpeedCacheDuration(1 * time.Nanosecond)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = metrics.calculateRecentSpeeds(nil)
	}
}

func BenchmarkPoolMetrics_GetSnapshotWithSpeedCalculation(b *testing.B) {
	metrics := NewPoolMetrics()

	// Add some basic metrics to ensure snapshot calculation works
	metrics.RecordConnectionCreated()
	metrics.RecordAcquire()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = metrics.GetSnapshot(nil)
	}
}
