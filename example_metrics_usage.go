package nntppool

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"
)

// ExampleMetricsUsage demonstrates how to use the pool metrics system
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

	// Get real-time metrics
	metrics := pool.GetMetrics()

	fmt.Printf("Active connections: %d\n", metrics.GetActiveConnections())
	fmt.Printf("Total acquires: %d\n", metrics.GetTotalAcquires())
	fmt.Printf("Total bytes downloaded: %d\n", metrics.GetTotalBytesDownloaded())
	fmt.Printf("Total bytes uploaded: %d\n", metrics.GetTotalBytesUploaded())
	fmt.Printf("Pool uptime: %v\n", metrics.GetUptime())

	// Get comprehensive snapshot
	snapshot := pool.GetMetricsSnapshot()

	fmt.Printf("\n=== Pool Metrics Snapshot ===\n")
	fmt.Printf("Timestamp: %v\n", snapshot.Timestamp)
	fmt.Printf("Uptime: %v\n", snapshot.Uptime)
	fmt.Printf("Active connections: %d\n", snapshot.ActiveConnections)
	fmt.Printf("Download speed: %.2f bytes/sec\n", snapshot.DownloadSpeed)
	fmt.Printf("Upload speed: %.2f bytes/sec\n", snapshot.UploadSpeed)
	fmt.Printf("Command success rate: %.2f%%\n", snapshot.CommandSuccessRate)
	fmt.Printf("Error rate: %.2f%%\n", snapshot.ErrorRate)

	// Provider-specific metrics
	fmt.Printf("\n=== Provider Metrics ===\n")
	for _, provider := range snapshot.ProviderMetrics {
		fmt.Printf("Provider: %s (%s)\n", provider.Host, provider.Username)
		fmt.Printf("  State: %s\n", provider.State)
		fmt.Printf("  Total connections: %d/%d\n", provider.TotalConnections, provider.MaxConnections)
		fmt.Printf("  Acquired: %d, Idle: %d\n", provider.AcquiredConnections, provider.IdleConnections)
		fmt.Printf("  Total bytes downloaded: %d\n", provider.TotalBytesDownloaded)
		fmt.Printf("  Total bytes uploaded: %d\n", provider.TotalBytesUploaded)
		fmt.Printf("  Success rate: %.2f%%\n", provider.SuccessRate)
		fmt.Printf("  Average connection age: %v\n", provider.AverageConnectionAge)
	}

	// JSON serialization for monitoring systems
	jsonData, err := json.MarshalIndent(snapshot, "", "  ")
	if err == nil {
		fmt.Printf("\n=== JSON Metrics (for monitoring systems) ===\n")
		fmt.Printf("%s\n", jsonData)
	}
}

// MonitoringExample shows how to use metrics for continuous monitoring
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

	go func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				metrics := pool.GetMetrics()

				// Monitor key performance indicators
				activeConns := metrics.GetActiveConnections()
				errorRate := float64(metrics.GetTotalErrors()) / float64(metrics.GetTotalAcquires()) * 100
				avgWaitTime := metrics.GetAverageAcquireWaitTime()

				fmt.Printf("[%v] Active: %d, Error Rate: %.2f%%, Avg Wait: %v\n",
					time.Now().Format("15:04:05"), activeConns, errorRate, avgWaitTime)

				// Alert on high error rates
				if errorRate > 5.0 {
					fmt.Printf("ALERT: High error rate detected: %.2f%%\n", errorRate)
				}

				// Alert on slow connection acquisition
				if avgWaitTime > 100*time.Millisecond {
					fmt.Printf("ALERT: Slow connection acquisition: %v\n", avgWaitTime)
				}
			}
		}
	}()

	// Simulate some work
	time.Sleep(30 * time.Second)
}

// PerformanceTrackingExample demonstrates tracking operation performance
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

	// Track download performance
	startTime := time.Now()
	beforeSnapshot := pool.GetMetricsSnapshot()

	// Simulate downloads (in real usage, these would be actual operations)
	// pool.Body(ctx, "message-id", writer, groups)

	time.Sleep(1 * time.Second) // Simulate work

	afterSnapshot := pool.GetMetricsSnapshot()
	duration := time.Since(startTime)

	// Calculate performance metrics
	bytesDownloaded := afterSnapshot.TotalBytesDownloaded - beforeSnapshot.TotalBytesDownloaded
	articlesRetrieved := afterSnapshot.TotalArticlesRetrieved - beforeSnapshot.TotalArticlesRetrieved

	if duration.Seconds() > 0 {
		downloadRate := float64(bytesDownloaded) / duration.Seconds()
		articleRate := float64(articlesRetrieved) / duration.Seconds()

		fmt.Printf("Performance Summary:\n")
		fmt.Printf("  Duration: %v\n", duration)
		fmt.Printf("  Bytes downloaded: %d\n", bytesDownloaded)
		fmt.Printf("  Articles retrieved: %d\n", articlesRetrieved)
		fmt.Printf("  Download rate: %.2f bytes/sec\n", downloadRate)
		fmt.Printf("  Article rate: %.2f articles/sec\n", articleRate)
	}
}

// HealthCheckExample shows how to implement health checks using metrics
func HealthCheckExample() bool {
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
		return false
	}
	defer pool.Quit()

	snapshot := pool.GetMetricsSnapshot()

	// Health check criteria
	healthy := true
	issues := []string{}

	// Check error rate (should be < 10%)
	if snapshot.ErrorRate > 10.0 {
		healthy = false
		issues = append(issues, fmt.Sprintf("High error rate: %.2f%%", snapshot.ErrorRate))
	}

	// Check command success rate (should be > 90%)
	if snapshot.CommandSuccessRate < 90.0 && snapshot.TotalCommandCount > 0 {
		healthy = false
		issues = append(issues, fmt.Sprintf("Low command success rate: %.2f%%", snapshot.CommandSuccessRate))
	}

	// Check if any connections are available
	totalIdle := snapshot.IdleConnections
	if totalIdle == 0 && snapshot.TotalConnections > 0 {
		healthy = false
		issues = append(issues, "No idle connections available")
	}

	// Check provider states
	for _, provider := range snapshot.ProviderMetrics {
		if provider.State != "active" {
			issues = append(issues, fmt.Sprintf("Provider %s is %s", provider.Host, provider.State))
		}
	}

	if !healthy {
		fmt.Printf("Health check FAILED:\n")
		for _, issue := range issues {
			fmt.Printf("  - %s\n", issue)
		}
	} else {
		fmt.Printf("Health check PASSED\n")
	}

	return healthy
}
