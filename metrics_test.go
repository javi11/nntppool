package nntppool

import (
	"testing"
	"time"
)

func TestPoolMetrics_Basic(t *testing.T) {
	metrics := NewPoolMetrics()
	if metrics == nil {
		t.Fatal("NewPoolMetrics() returned nil")
	}

	// Test initial state - all counters should be zero
	snapshot := metrics.GetSnapshot(nil)
	if snapshot.ArticlesDownloaded != 0 {
		t.Errorf("ArticlesDownloaded = %d, want 0", snapshot.ArticlesDownloaded)
	}
	if snapshot.ArticlesPosted != 0 {
		t.Errorf("ArticlesPosted = %d, want 0", snapshot.ArticlesPosted)
	}
	if snapshot.BytesDownloaded != 0 {
		t.Errorf("BytesDownloaded = %d, want 0", snapshot.BytesDownloaded)
	}
	if snapshot.BytesUploaded != 0 {
		t.Errorf("BytesUploaded = %d, want 0", snapshot.BytesUploaded)
	}
	if snapshot.TotalErrors != 0 {
		t.Errorf("TotalErrors = %d, want 0", snapshot.TotalErrors)
	}
	if len(snapshot.ProviderErrors) != 0 {
		t.Errorf("ProviderErrors length = %d, want 0", len(snapshot.ProviderErrors))
	}
}

func TestPoolMetrics_ArticleCounters(t *testing.T) {
	metrics := NewPoolMetrics()

	// Test article download counter
	metrics.RecordArticleDownloaded()
	metrics.RecordArticleDownloaded()
	metrics.RecordArticleDownloaded()

	snapshot := metrics.GetSnapshot(nil)
	if snapshot.ArticlesDownloaded != 3 {
		t.Errorf("ArticlesDownloaded = %d, want 3", snapshot.ArticlesDownloaded)
	}

	// Test article post counter
	metrics.RecordArticlePosted()
	metrics.RecordArticlePosted()

	snapshot = metrics.GetSnapshot(nil)
	if snapshot.ArticlesPosted != 2 {
		t.Errorf("ArticlesPosted = %d, want 2", snapshot.ArticlesPosted)
	}
	if snapshot.ArticlesDownloaded != 3 {
		t.Errorf("ArticlesDownloaded = %d, want 3", snapshot.ArticlesDownloaded)
	}
}

func TestPoolMetrics_TrafficCounters(t *testing.T) {
	metrics := NewPoolMetrics()

	// Test bytes downloaded counter
	metrics.RecordDownload(1024)
	metrics.RecordDownload(2048)
	metrics.RecordDownload(512)

	snapshot := metrics.GetSnapshot(nil)
	expectedDownload := int64(1024 + 2048 + 512)
	if snapshot.BytesDownloaded != expectedDownload {
		t.Errorf("BytesDownloaded = %d, want %d", snapshot.BytesDownloaded, expectedDownload)
	}

	// Test bytes uploaded counter
	metrics.RecordUpload(4096)
	metrics.RecordUpload(8192)

	snapshot = metrics.GetSnapshot(nil)
	expectedUpload := int64(4096 + 8192)
	if snapshot.BytesUploaded != expectedUpload {
		t.Errorf("BytesUploaded = %d, want %d", snapshot.BytesUploaded, expectedUpload)
	}
	if snapshot.BytesDownloaded != expectedDownload {
		t.Errorf("BytesDownloaded = %d, want %d", snapshot.BytesDownloaded, expectedDownload)
	}
}

func TestPoolMetrics_ErrorTracking(t *testing.T) {
	metrics := NewPoolMetrics()

	// Test total error counter
	metrics.RecordError("")
	metrics.RecordError("")

	snapshot := metrics.GetSnapshot(nil)
	if snapshot.TotalErrors != 2 {
		t.Errorf("TotalErrors = %d, want 2", snapshot.TotalErrors)
	}
	if len(snapshot.ProviderErrors) != 0 {
		t.Errorf("ProviderErrors should be empty when host is empty")
	}
}

func TestPoolMetrics_PerProviderErrors(t *testing.T) {
	metrics := NewPoolMetrics()

	// Test per-provider error tracking
	metrics.RecordError("provider1.example.com")
	metrics.RecordError("provider1.example.com")
	metrics.RecordError("provider2.example.com")

	snapshot := metrics.GetSnapshot(nil)
	if snapshot.TotalErrors != 3 {
		t.Errorf("TotalErrors = %d, want 3", snapshot.TotalErrors)
	}

	// Check provider-specific errors
	if snapshot.ProviderErrors["provider1.example.com"] != 2 {
		t.Errorf("Provider1 errors = %d, want 2", snapshot.ProviderErrors["provider1.example.com"])
	}
	if snapshot.ProviderErrors["provider2.example.com"] != 1 {
		t.Errorf("Provider2 errors = %d, want 1", snapshot.ProviderErrors["provider2.example.com"])
	}

	// Test mixed error recording
	metrics.RecordError("") // No provider
	snapshot = metrics.GetSnapshot(nil)
	if snapshot.TotalErrors != 4 {
		t.Errorf("TotalErrors = %d, want 4", snapshot.TotalErrors)
	}
	if snapshot.ProviderErrors["provider1.example.com"] != 2 {
		t.Errorf("Provider1 errors should remain 2, got %d", snapshot.ProviderErrors["provider1.example.com"])
	}
}

func TestPoolMetrics_Reset(t *testing.T) {
	metrics := NewPoolMetrics()

	// Populate metrics
	metrics.RecordArticleDownloaded()
	metrics.RecordArticlePosted()
	metrics.RecordDownload(1024)
	metrics.RecordUpload(2048)
	metrics.RecordError("provider.example.com")
	metrics.RecordError("")

	// Verify non-zero state
	snapshot := metrics.GetSnapshot(nil)
	if snapshot.ArticlesDownloaded == 0 {
		t.Error("ArticlesDownloaded should be non-zero before reset")
	}
	if snapshot.TotalErrors == 0 {
		t.Error("TotalErrors should be non-zero before reset")
	}

	// Reset all counters
	metrics.Reset()

	// Verify all counters are zero after reset
	snapshot = metrics.GetSnapshot(nil)
	if snapshot.ArticlesDownloaded != 0 {
		t.Errorf("ArticlesDownloaded = %d after reset, want 0", snapshot.ArticlesDownloaded)
	}
	if snapshot.ArticlesPosted != 0 {
		t.Errorf("ArticlesPosted = %d after reset, want 0", snapshot.ArticlesPosted)
	}
	if snapshot.BytesDownloaded != 0 {
		t.Errorf("BytesDownloaded = %d after reset, want 0", snapshot.BytesDownloaded)
	}
	if snapshot.BytesUploaded != 0 {
		t.Errorf("BytesUploaded = %d after reset, want 0", snapshot.BytesUploaded)
	}
	if snapshot.TotalErrors != 0 {
		t.Errorf("TotalErrors = %d after reset, want 0", snapshot.TotalErrors)
	}
	// Provider errors map should still exist but with zero counts
	if val, exists := snapshot.ProviderErrors["provider.example.com"]; exists && val != 0 {
		t.Errorf("Provider errors = %d after reset, want 0", val)
	}
}

func TestPoolMetrics_SnapshotTimestamp(t *testing.T) {
	metrics := NewPoolMetrics()

	before := time.Now()
	snapshot := metrics.GetSnapshot(nil)
	after := time.Now()

	// Verify timestamp is reasonable
	if snapshot.Timestamp.Before(before) || snapshot.Timestamp.After(after) {
		t.Errorf("Snapshot timestamp %v is outside expected range [%v, %v]",
			snapshot.Timestamp, before, after)
	}
}

func TestPoolMetrics_ConcurrentAccess(t *testing.T) {
	metrics := NewPoolMetrics()

	// Test concurrent writes
	const goroutines = 10
	const operations = 100

	done := make(chan bool, goroutines)

	for i := 0; i < goroutines; i++ {
		go func() {
			for j := 0; j < operations; j++ {
				metrics.RecordArticleDownloaded()
				metrics.RecordArticlePosted()
				metrics.RecordDownload(100)
				metrics.RecordUpload(200)
				metrics.RecordError("provider.example.com")
			}
			done <- true
		}()
	}

	// Wait for all goroutines
	for i := 0; i < goroutines; i++ {
		<-done
	}

	// Verify final counts
	snapshot := metrics.GetSnapshot(nil)
	expected := int64(goroutines * operations)

	if snapshot.ArticlesDownloaded != expected {
		t.Errorf("ArticlesDownloaded = %d, want %d", snapshot.ArticlesDownloaded, expected)
	}
	if snapshot.ArticlesPosted != expected {
		t.Errorf("ArticlesPosted = %d, want %d", snapshot.ArticlesPosted, expected)
	}
	if snapshot.BytesDownloaded != expected*100 {
		t.Errorf("BytesDownloaded = %d, want %d", snapshot.BytesDownloaded, expected*100)
	}
	if snapshot.BytesUploaded != expected*200 {
		t.Errorf("BytesUploaded = %d, want %d", snapshot.BytesUploaded, expected*200)
	}
	if snapshot.TotalErrors != expected {
		t.Errorf("TotalErrors = %d, want %d", snapshot.TotalErrors, expected)
	}
	if snapshot.ProviderErrors["provider.example.com"] != expected {
		t.Errorf("Provider errors = %d, want %d",
			snapshot.ProviderErrors["provider.example.com"], expected)
	}
}

func TestPoolMetrics_NoOpMethods(t *testing.T) {
	// Test that no-op methods don't panic
	metrics := NewPoolMetrics()

	// These methods should do nothing but not panic
	metrics.RecordAcquire()
	metrics.RecordRelease()
	metrics.RecordRetry()
	metrics.RecordConnectionCreated()
	metrics.RecordConnectionDestroyed()
	metrics.RecordAcquireWaitTime(time.Second)
	metrics.RegisterActiveConnection("conn1", nil)
	metrics.UnregisterActiveConnection("conn1")

	// Verify they don't affect the essential metrics
	snapshot := metrics.GetSnapshot(nil)
	if snapshot.ArticlesDownloaded != 0 {
		t.Error("No-op methods should not affect essential metrics")
	}
	if snapshot.TotalErrors != 0 {
		t.Error("No-op methods should not affect essential metrics")
	}
}

func TestPoolMetrics_SnapshotIsolation(t *testing.T) {
	metrics := NewPoolMetrics()

	// Record some metrics
	metrics.RecordArticleDownloaded()
	snapshot1 := metrics.GetSnapshot(nil)

	// Record more metrics
	metrics.RecordArticleDownloaded()
	snapshot2 := metrics.GetSnapshot(nil)

	// Verify snapshots are isolated
	if snapshot1.ArticlesDownloaded != 1 {
		t.Errorf("First snapshot ArticlesDownloaded = %d, want 1", snapshot1.ArticlesDownloaded)
	}
	if snapshot2.ArticlesDownloaded != 2 {
		t.Errorf("Second snapshot ArticlesDownloaded = %d, want 2", snapshot2.ArticlesDownloaded)
	}
}
