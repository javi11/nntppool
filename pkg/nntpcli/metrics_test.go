package nntpcli

import (
	"testing"
	"time"
)

func TestMetricsBasicOperations(t *testing.T) {
	metrics := NewMetrics()
	
	// Test initial state
	if !metrics.IsEnabled() {
		t.Error("Metrics should be enabled by default")
	}
	
	snapshot := metrics.GetSnapshot()
	if snapshot.TotalCommands != 0 {
		t.Error("Initial command count should be 0")
	}
	
	// Test command recording
	metrics.RecordCommand(true)
	metrics.RecordCommand(false)
	
	if metrics.GetTotalCommands() != 2 {
		t.Errorf("Expected 2 total commands, got %d", metrics.GetTotalCommands())
	}
	
	snapshot = metrics.GetSnapshot()
	if snapshot.CommandErrors != 1 {
		t.Errorf("Expected 1 command error, got %d", snapshot.CommandErrors)
	}
	
	if snapshot.SuccessRate != 50.0 {
		t.Errorf("Expected 50%% success rate, got %.1f%%", snapshot.SuccessRate)
	}
}

func TestMetricsDataTransfer(t *testing.T) {
	metrics := NewMetrics()
	
	// Test data recording
	metrics.RecordDownload(1024)
	metrics.RecordUpload(512)
	metrics.RecordArticle()
	
	snapshot := metrics.GetSnapshot()
	if snapshot.BytesDownloaded != 1024 {
		t.Errorf("Expected 1024 bytes downloaded, got %d", snapshot.BytesDownloaded)
	}
	
	if snapshot.BytesUploaded != 512 {
		t.Errorf("Expected 512 bytes uploaded, got %d", snapshot.BytesUploaded)
	}
	
	if snapshot.ArticlesRetrieved != 1 {
		t.Errorf("Expected 1 article retrieved, got %d", snapshot.ArticlesRetrieved)
	}
}

func TestMetricsAuthentication(t *testing.T) {
	metrics := NewMetrics()
	
	// Test auth recording
	metrics.RecordAuth(true)
	metrics.RecordAuth(false)
	metrics.RecordAuth(true)
	
	snapshot := metrics.GetSnapshot()
	if snapshot.AuthAttempts != 3 {
		t.Errorf("Expected 3 auth attempts, got %d", snapshot.AuthAttempts)
	}
	
	if snapshot.AuthFailures != 1 {
		t.Errorf("Expected 1 auth failure, got %d", snapshot.AuthFailures)
	}
	
	expectedRate := 66.7 // 2/3 * 100
	if snapshot.AuthSuccessRate < expectedRate-0.1 || snapshot.AuthSuccessRate > expectedRate+0.1 {
		t.Errorf("Expected ~%.1f%% auth success rate, got %.1f%%", expectedRate, snapshot.AuthSuccessRate)
	}
}

func TestMetricsDisable(t *testing.T) {
	metrics := NewMetrics()
	
	// Record some data while enabled
	metrics.RecordCommand(true)
	
	// Disable metrics
	metrics.SetEnabled(false)
	if metrics.IsEnabled() {
		t.Error("Metrics should be disabled")
	}
	
	// Record more data while disabled (should not be recorded)
	metrics.RecordCommand(true)
	metrics.RecordDownload(1000)
	
	// Should still have only the first command
	if metrics.GetTotalCommands() != 1 {
		t.Errorf("Expected 1 total command (disabled recording), got %d", metrics.GetTotalCommands())
	}
	
	if metrics.GetBytesDownloaded() != 0 {
		t.Errorf("Expected 0 bytes downloaded (disabled recording), got %d", metrics.GetBytesDownloaded())
	}
}

func TestMetricsPerformance(t *testing.T) {
	metrics := NewMetrics()
	
	const iterations = 100000
	
	// Test enabled performance
	start := time.Now()
	for i := 0; i < iterations; i++ {
		metrics.RecordCommand(true)
	}
	enabledDuration := time.Since(start)
	
	// Reset and test disabled performance
	metrics = NewMetrics()
	metrics.SetEnabled(false)
	
	start = time.Now()
	for i := 0; i < iterations; i++ {
		metrics.RecordCommand(true)
	}
	disabledDuration := time.Since(start)
	
	t.Logf("Enabled metrics: %d operations in %v (%.2f ns/op)", 
		iterations, enabledDuration, float64(enabledDuration.Nanoseconds())/float64(iterations))
	t.Logf("Disabled metrics: %d operations in %v (%.2f ns/op)", 
		iterations, disabledDuration, float64(disabledDuration.Nanoseconds())/float64(iterations))
	
	// Both should be very fast (under 100 microseconds total for 100k operations)
	maxDuration := 100 * time.Millisecond
	if enabledDuration > maxDuration {
		t.Errorf("Enabled metrics too slow: %v > %v", enabledDuration, maxDuration)
	}
	
	if disabledDuration > maxDuration {
		t.Errorf("Disabled metrics too slow: %v > %v", disabledDuration, maxDuration)
	}
}

func BenchmarkMetricsRecordCommand(b *testing.B) {
	metrics := NewMetrics()
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		metrics.RecordCommand(true)
	}
}

func BenchmarkMetricsRecordCommandDisabled(b *testing.B) {
	metrics := NewMetrics()
	metrics.SetEnabled(false)
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		metrics.RecordCommand(true)
	}
}

func BenchmarkMetricsGetSnapshot(b *testing.B) {
	metrics := NewMetrics()
	
	// Pre-populate with some data
	for i := 0; i < 1000; i++ {
		metrics.RecordCommand(true)
		metrics.RecordDownload(int64(i))
	}
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = metrics.GetSnapshot()
	}
}