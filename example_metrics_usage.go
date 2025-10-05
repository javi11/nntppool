package nntppool

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"
)

// ExampleMetricsUsage demonstrates how to use the simplified pool metrics system
func ExampleMetricsUsage() {
	// Create a connection pool
	config := Config{
		Providers: []UsenetProviderConfig{
			{
				Host:           "news.example.com",
				Port:           563,
				Username:       "user",
				Password:       "pass",
				TLS:            true,
				MaxConnections: 10,
			},
		},
		Logger: slog.Default(),
	}

	pool, err := NewConnectionPool(config)
	if err != nil {
		panic(err)
	}
	defer pool.Quit()

	// Get metrics snapshot
	snapshot := pool.GetMetricsSnapshot()

	// Display basic metrics
	fmt.Printf("=== Pool Metrics ===\n")
	fmt.Printf("Articles downloaded: %d\n", snapshot.ArticlesDownloaded)
	fmt.Printf("Articles posted: %d\n", snapshot.ArticlesPosted)
	fmt.Printf("Bytes downloaded: %d\n", snapshot.BytesDownloaded)
	fmt.Printf("Bytes uploaded: %d\n", snapshot.BytesUploaded)
	fmt.Printf("Total errors: %d\n", snapshot.TotalErrors)
	fmt.Printf("Timestamp: %v\n", snapshot.Timestamp)

	// Display per-provider errors
	fmt.Printf("\n=== Provider Errors ===\n")
	for host, errorCount := range snapshot.ProviderErrors {
		fmt.Printf("Provider %s: %d errors\n", host, errorCount)
	}

	// JSON serialization for monitoring systems
	jsonData, err := json.MarshalIndent(snapshot, "", "  ")
	if err == nil {
		fmt.Printf("\n=== JSON Metrics ===\n")
		fmt.Printf("%s\n", jsonData)
	}
}

// PerformanceTrackingExample demonstrates tracking download/upload speeds
// by taking snapshots and calculating deltas
func PerformanceTrackingExample() {
	pool, err := NewConnectionPool(Config{
		Providers: []UsenetProviderConfig{
			{
				Host:           "news.example.com",
				Port:           563,
				Username:       "user",
				Password:       "pass",
				TLS:            true,
				MaxConnections: 10,
			},
		},
		Logger: slog.Default(),
	})
	if err != nil {
		panic(err)
	}
	defer pool.Quit()

	// Take initial snapshot
	startTime := time.Now()
	startSnapshot := pool.GetMetricsSnapshot()

	// Simulate some work (in real usage, perform actual operations)
	time.Sleep(1 * time.Second)

	// Take final snapshot
	endSnapshot := pool.GetMetricsSnapshot()
	duration := time.Since(startTime)

	// Calculate performance metrics from deltas
	bytesDownloaded := endSnapshot.BytesDownloaded - startSnapshot.BytesDownloaded
	bytesUploaded := endSnapshot.BytesUploaded - startSnapshot.BytesUploaded
	articlesDownloaded := endSnapshot.ArticlesDownloaded - startSnapshot.ArticlesDownloaded
	articlesPosted := endSnapshot.ArticlesPosted - startSnapshot.ArticlesPosted

	// Calculate speeds
	if duration.Seconds() > 0 {
		downloadSpeed := float64(bytesDownloaded) / duration.Seconds()
		uploadSpeed := float64(bytesUploaded) / duration.Seconds()
		downloadRate := float64(articlesDownloaded) / duration.Seconds()
		postRate := float64(articlesPosted) / duration.Seconds()

		fmt.Printf("=== Performance Summary ===\n")
		fmt.Printf("Duration: %v\n", duration)
		fmt.Printf("Download speed: %.2f bytes/sec (%.2f MB/sec)\n",
			downloadSpeed, downloadSpeed/1024/1024)
		fmt.Printf("Upload speed: %.2f bytes/sec (%.2f MB/sec)\n",
			uploadSpeed, uploadSpeed/1024/1024)
		fmt.Printf("Download rate: %.2f articles/sec\n", downloadRate)
		fmt.Printf("Post rate: %.2f articles/sec\n", postRate)
	}
}

// MonitoringExample shows how to implement continuous metrics monitoring
func MonitoringExample() {
	pool, err := NewConnectionPool(Config{
		Providers: []UsenetProviderConfig{
			{
				Host:           "news.example.com",
				Port:           563,
				Username:       "user",
				Password:       "pass",
				TLS:            true,
				MaxConnections: 10,
			},
		},
		Logger: slog.Default(),
	})
	if err != nil {
		panic(err)
	}
	defer pool.Quit()

	// Start monitoring goroutine
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Track previous snapshot for delta calculations
	prevSnapshot := pool.GetMetricsSnapshot()
	prevTime := time.Now()

	go func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				currentSnapshot := pool.GetMetricsSnapshot()
				currentTime := time.Now()
				duration := currentTime.Sub(prevTime).Seconds()

				// Calculate deltas and speeds
				if duration > 0 {
					bytesDownloaded := currentSnapshot.BytesDownloaded - prevSnapshot.BytesDownloaded
					downloadSpeed := float64(bytesDownloaded) / duration

					errorsDelta := currentSnapshot.TotalErrors - prevSnapshot.TotalErrors

					fmt.Printf("[%v] Speed: %.2f MB/sec, Errors: %d, Total Downloaded: %d articles\n",
						currentTime.Format("15:04:05"),
						downloadSpeed/1024/1024,
						errorsDelta,
						currentSnapshot.ArticlesDownloaded)
				}

				// Update for next iteration
				prevSnapshot = currentSnapshot
				prevTime = currentTime
			}
		}
	}()

	// Simulate some work
	time.Sleep(30 * time.Second)
}

// ResetExample demonstrates optional Reset() functionality
func ResetExample() {
	pool, err := NewConnectionPool(Config{
		Providers: []UsenetProviderConfig{
			{
				Host:           "news.example.com",
				Port:           563,
				Username:       "user",
				Password:       "pass",
				TLS:            true,
				MaxConnections: 10,
			},
		},
		Logger: slog.Default(),
	})
	if err != nil {
		panic(err)
	}
	defer pool.Quit()

	// Get metrics instance
	metrics := pool.GetMetrics()

	// Display current metrics
	snapshot := pool.GetMetricsSnapshot()
	fmt.Printf("Before reset: Downloaded=%d, Posted=%d, Errors=%d\n",
		snapshot.ArticlesDownloaded,
		snapshot.ArticlesPosted,
		snapshot.TotalErrors)

	// Optional: Reset metrics for a new reporting period
	metrics.Reset()

	// Verify reset
	snapshot = pool.GetMetricsSnapshot()
	fmt.Printf("After reset: Downloaded=%d, Posted=%d, Errors=%d\n",
		snapshot.ArticlesDownloaded,
		snapshot.ArticlesPosted,
		snapshot.TotalErrors)
}
