package nntppool

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
	metrics := NewPoolMetrics()

	// Test traffic metrics
	metrics.RecordBytesDownloaded(1024)
	metrics.RecordBytesDownloaded(2048)
	assert.Equal(t, int64(3072), metrics.GetTotalBytesDownloaded())

	metrics.RecordBytesUploaded(512)
	metrics.RecordBytesUploaded(256)
	assert.Equal(t, int64(768), metrics.GetTotalBytesUploaded())

	metrics.RecordArticleRetrieved()
	metrics.RecordArticleRetrieved()
	assert.Equal(t, int64(2), metrics.GetTotalArticlesRetrieved())

	metrics.RecordArticlePosted()
	assert.Equal(t, int64(1), metrics.GetTotalArticlesPosted())
}

func TestPoolMetrics_CommandMetrics(t *testing.T) {
	metrics := NewPoolMetrics()

	// Test command metrics
	metrics.RecordCommand()
	metrics.RecordCommand()
	metrics.RecordCommand()
	assert.Equal(t, int64(3), metrics.GetTotalCommandCount())

	metrics.RecordCommandError()
	assert.Equal(t, int64(1), metrics.GetTotalCommandErrors())
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
				metrics.RecordBytesDownloaded(int64(j))
				metrics.RecordCommand()
				
				// Read metrics
				_ = metrics.GetTotalConnectionsCreated()
				_ = metrics.GetTotalAcquires()
				_ = metrics.GetTotalBytesDownloaded()
				_ = metrics.GetTotalCommandCount()
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
	assert.Equal(t, int64(49500), metrics.GetTotalBytesDownloaded()) // Sum of 0+1+...+99 = 4950, times 10 = 49500
	assert.Equal(t, int64(1000), metrics.GetTotalCommandCount())
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
	
	// Add some metrics data
	metrics.RecordBytesDownloaded(1000)
	metrics.RecordBytesUploaded(500)
	metrics.RecordCommand()
	metrics.RecordCommand()
	metrics.RecordCommandError()
	metrics.RecordAcquire()
	metrics.RecordError()
	
	// Wait a bit to ensure uptime is measurable
	time.Sleep(10 * time.Millisecond)
	
	snapshot := metrics.GetSnapshot(nil)
	
	// Verify calculated fields
	assert.True(t, snapshot.DownloadSpeed > 0) // Should be bytes/second
	assert.True(t, snapshot.UploadSpeed > 0)   // Should be bytes/second
	assert.Equal(t, float64(50), snapshot.CommandSuccessRate) // 1 success out of 2 commands = 50%
	assert.Equal(t, float64(100), snapshot.ErrorRate) // 1 error out of 1 acquire = 100%
	assert.Equal(t, int64(1000), snapshot.TotalBytesDownloaded)
	assert.Equal(t, int64(500), snapshot.TotalBytesUploaded)
}

// Benchmark tests to ensure minimal performance overhead
func BenchmarkPoolMetrics_RecordConnectionCreated(b *testing.B) {
	metrics := NewPoolMetrics()
	b.ResetTimer()
	
	for i := 0; i < b.N; i++ {
		metrics.RecordConnectionCreated()
	}
}

func BenchmarkPoolMetrics_RecordBytesDownloaded(b *testing.B) {
	metrics := NewPoolMetrics()
	b.ResetTimer()
	
	for i := 0; i < b.N; i++ {
		metrics.RecordBytesDownloaded(1024)
	}
}

func BenchmarkPoolMetrics_GetTotalBytesDownloaded(b *testing.B) {
	metrics := NewPoolMetrics()
	metrics.RecordBytesDownloaded(1024)
	b.ResetTimer()
	
	for i := 0; i < b.N; i++ {
		_ = metrics.GetTotalBytesDownloaded()
	}
}

func BenchmarkPoolMetrics_ConcurrentOperations(b *testing.B) {
	metrics := NewPoolMetrics()
	b.ResetTimer()
	
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			metrics.RecordConnectionCreated()
			metrics.RecordBytesDownloaded(1024)
			_ = metrics.GetTotalConnectionsCreated()
			_ = metrics.GetTotalBytesDownloaded()
		}
	})
}