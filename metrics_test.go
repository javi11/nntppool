package nntppool

import (
	"fmt"
	"testing"
	"time"

	"github.com/javi11/nntpcli"
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
	assert.Equal(t, int64(0), metrics.GetActiveConnections())
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
	assert.Equal(t, int64(2), metrics.GetActiveConnections())

	// Record connection destruction
	metrics.RecordConnectionDestroyed()
	assert.Equal(t, int64(2), metrics.GetTotalConnectionsCreated())
	assert.Equal(t, int64(1), metrics.GetTotalConnectionsDestroyed())
	assert.Equal(t, int64(1), metrics.GetActiveConnections())
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
	assert.Equal(t, float64(0), snapshot.DownloadSpeed)        // No connection data = 0
	assert.Equal(t, float64(0), snapshot.UploadSpeed)          // No connection data = 0
	assert.Equal(t, float64(0), snapshot.CommandSuccessRate)   // No connection data = 0
	assert.Equal(t, float64(100), snapshot.ErrorRate)          // 1 error out of 1 acquire = 100%
	assert.Equal(t, int64(0), snapshot.TotalBytesDownloaded)   // No connection data = 0
	assert.Equal(t, int64(0), snapshot.TotalBytesUploaded)     // No connection data = 0
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
	assert.Equal(t, 0, metrics.GetActiveConnectionsCount())
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

	// Create mock metrics for the connections
	mockMetrics1 := nntpcli.NewMetrics()
	mockMetrics2 := nntpcli.NewMetrics()

	// Setup expectations for mock connections to return our metrics
	mockConn1.EXPECT().GetMetrics().Return(mockMetrics1).AnyTimes()
	mockConn2.EXPECT().GetMetrics().Return(mockMetrics2).AnyTimes()

	// Simulate some activity on the connections
	mockMetrics1.RecordDownload(1000)
	mockMetrics1.RecordUpload(500)
	for i := 0; i < 10; i++ {
		mockMetrics1.RecordCommand(i < 9) // 9 successes, 1 failure
	}

	mockMetrics2.RecordDownload(2000)
	mockMetrics2.RecordUpload(800)
	for i := 0; i < 15; i++ {
		mockMetrics2.RecordCommand(i < 13) // 13 successes, 2 failures
	}

	// Register active connections
	metrics.RegisterActiveConnection("conn1", mockConn1)
	assert.Equal(t, 1, metrics.GetActiveConnectionsCount())

	metrics.RegisterActiveConnection("conn2", mockConn2)
	assert.Equal(t, 2, metrics.GetActiveConnectionsCount())

	// Get active metrics
	activeMetrics := metrics.GetActiveConnectionMetrics()
	assert.Equal(t, 2, activeMetrics.Count)
	assert.Equal(t, int64(3000), activeMetrics.TotalBytesDownloaded) // 1000 + 2000
	assert.Equal(t, int64(1300), activeMetrics.TotalBytesUploaded)   // 500 + 800
	assert.Equal(t, int64(25), activeMetrics.TotalCommands)          // 10 + 15
	assert.Equal(t, int64(3), activeMetrics.TotalCommandErrors)      // 1 + 2

	// Success rate should be (25-3)/25 * 100 = 88%
	assert.InDelta(t, 88.0, activeMetrics.SuccessRate, 0.1)

	// Connection age will be very small since we just created the metrics
	assert.True(t, activeMetrics.AverageConnectionAge > 0)

	// Unregister connections
	metrics.UnregisterActiveConnection("conn1")
	assert.Equal(t, 1, metrics.GetActiveConnectionsCount())

	metrics.UnregisterActiveConnection("conn2")
	assert.Equal(t, 0, metrics.GetActiveConnectionsCount())

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
				mockMetrics := nntpcli.NewMetrics()

				mockConn.EXPECT().GetMetrics().Return(mockMetrics).AnyTimes()

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
	assert.Equal(t, 0, metrics.GetActiveConnectionsCount())
	finalMetrics := metrics.GetActiveConnectionMetrics()
	assert.Equal(t, 0, finalMetrics.Count)
}

func TestPoolMetrics_ActiveConnectionWithNilMetrics(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	metrics := NewPoolMetrics()

	// Create mock connection that returns nil metrics
	mockConn := nntpcli.NewMockConnection(ctrl)
	mockConn.EXPECT().GetMetrics().Return(nil).AnyTimes()

	// Register connection
	metrics.RegisterActiveConnection("conn1", mockConn)
	assert.Equal(t, 1, metrics.GetActiveConnectionsCount())

	// Get active metrics - should handle nil metrics gracefully
	activeMetrics := metrics.GetActiveConnectionMetrics()
	assert.Equal(t, 0, activeMetrics.Count) // Should not count connections with nil metrics
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
		mockMetrics := nntpcli.NewMetrics()

		mockConn.EXPECT().GetMetrics().Return(mockMetrics).AnyTimes()

		metrics.RegisterActiveConnection(fmt.Sprintf("conn%d", i), mockConn)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = metrics.GetActiveConnectionMetrics()
	}
}
