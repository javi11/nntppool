package nntpcli

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"time"
)

// ExampleMetricsUsage demonstrates how to use the metrics system
func ExampleMetricsUsage() {
	// Create a new NNTP client
	client := New()

	// Connect to NNTP server
	conn, err := client.Dial(context.Background(), "news.example.com", 119)
	if err != nil {
		fmt.Printf("Connection failed: %v\n", err)
		return
	}
	defer func() {
		_ = conn.Close()
	}()

	// Get metrics interface
	metrics := conn.GetMetrics()

	// Optionally disable metrics for performance-critical sections
	// metrics.SetEnabled(false)

	// Perform some NNTP operations
	err = conn.Authenticate("username", "password")
	if err != nil {
		fmt.Printf("Auth failed: %v\n", err)
		return
	}

	err = conn.JoinGroup("misc.test")
	if err != nil {
		fmt.Printf("Group join failed: %v\n", err)
		return
	}

	// Download an article
	body, err := os.Create("article.txt")
	if err != nil {
		fmt.Printf("File create failed: %v\n", err)
		return
	}
	defer func() {
		_ = body.Close()
	}()

	_, err = conn.BodyDecoded("<message-id>", body, 0)
	if err != nil {
		fmt.Printf("Article download failed: %v\n", err)
		return
	}

	// Get metrics snapshot
	snapshot := metrics.GetSnapshot()

	// Display metrics in human-readable format
	fmt.Printf("=== NNTP Connection Metrics ===\n")
	fmt.Printf("Connected: %v ago\n", snapshot.ConnectionAge)
	fmt.Printf("Total Commands: %d\n", snapshot.TotalCommands)
	fmt.Printf("Command Success Rate: %.1f%%\n", snapshot.SuccessRate)
	fmt.Printf("Data Downloaded: %d bytes\n", snapshot.BytesDownloaded)
	fmt.Printf("Data Uploaded: %d bytes\n", snapshot.BytesUploaded)
	fmt.Printf("Articles Retrieved: %d\n", snapshot.ArticlesRetrieved)
	fmt.Printf("Auth Attempts: %d (%.1f%% success)\n",
		snapshot.AuthAttempts, snapshot.AuthSuccessRate)
	fmt.Printf("Group Joins: %d\n", snapshot.GroupJoins)
	fmt.Printf("Last Activity: %v\n", snapshot.LastActivity.Format(time.RFC3339))

	// Export metrics as JSON
	jsonData, err := json.MarshalIndent(snapshot, "", "  ")
	if err != nil {
		fmt.Printf("JSON marshal failed: %v\n", err)
		return
	}

	fmt.Printf("\n=== JSON Export ===\n%s\n", jsonData)

	// Quick access to commonly needed values
	fmt.Printf("\n=== Quick Stats ===\n")
	fmt.Printf("Connection age: %v\n", metrics.GetConnectionAge())
	fmt.Printf("Total bytes downloaded: %d\n", metrics.GetBytesDownloaded())
	fmt.Printf("Total commands: %d\n", metrics.GetTotalCommands())
}

// MonitoringExample shows how to use metrics for monitoring
func MonitoringExample() {
	client := New()
	conn, err := client.Dial(context.Background(), "news.example.com", 119)
	if err != nil {
		return
	}
	defer func() {
		_ = conn.Close()
	}()

	metrics := conn.GetMetrics()

	// Monitor connection health
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			snapshot := metrics.GetSnapshot()

			// Check for issues
			if snapshot.SuccessRate < 95.0 {
				fmt.Printf("WARNING: Low success rate: %.1f%%\n", snapshot.SuccessRate)
			}

			if snapshot.AuthSuccessRate < 100.0 {
				fmt.Printf("WARNING: Auth failures detected: %.1f%% success\n", snapshot.AuthSuccessRate)
			}

			// Log periodic stats
			fmt.Printf("Stats: %d commands, %.1f%% success, %d bytes downloaded\n",
				snapshot.TotalCommands, snapshot.SuccessRate, snapshot.BytesDownloaded)

		default:
			// Perform NNTP operations here
			return
		}
	}
}
