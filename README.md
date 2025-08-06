# nntppool

<a href="https://www.buymeacoffee.com/qbt52hh7sjd"><img src="https://img.buymeacoffee.com/button-api/?text=Buy me a coffee&emoji=☕&slug=qbt52hh7sjd&button_colour=FFDD00&font_colour=000000&font_family=Comic&outline_colour=000000&coffee_colour=ffffff" /></a>

A nntp pool connection with retry and provider rotation.

## Features

- Connection pooling
- Body download retry and yenc decode
- Post article with retry and yenc encode
- Stat article with retry
- TLS support
- Multiple providers with rotation. In case of failure for article not found the provider will be rotated.
- Backup providers. If all providers fail, the backup provider will be used for download. Useful for block accounts usage.
- **Dynamic reconfiguration** - Update provider settings, add/remove providers, or change connection limits without interrupting service
- **Intelligent metrics system** - Comprehensive metrics with rolling windows, automatic cleanup, and memory management to prevent infinite growth

## Installation

To install the `nntppool` package, you can use `go get`:

```sh
go get github.com/javi11/nntppool
```

Since this package uses [Rapidyenc](github.com/mnightingale/rapidyenc), you will need to build it with **CGO enabled**

## Usage Example

```go
package main

import (
    "context"
    "log"
    "os"
    "time"

    "github.com/javi11/nntppool"
)

func main() {
    // Configure the connection pool
    config := nntppool.Config{
        MinConnections: 5,
        MaxRetries:    3,
        Providers: []nntppool.UsenetProviderConfig{
            {
                Host:                          "news.example.com",
                Port:                          119,
                Username:                      "user",
                Password:                      "pass",
                MaxConnections:                10,
                MaxConnectionIdleTimeInSeconds: 300,
                TLS:                           false,
            },
            {
                Host:                          "news-backup.example.com",
                Port:                          119,
                Username:                      "user",
                Password:                      "pass",
                MaxConnections:                5,
                MaxConnectionIdleTimeInSeconds: 300,
                TLS:                           true,
                IsBackupProvider:              true,
            },
        },
    }

    // Create a new connection pool
    pool, err := nntppool.NewConnectionPool(config)
    if err != nil {
        log.Fatal(err)
    }
    defer pool.Quit()

    // Example: Download an article
    ctx := context.Background()
    msgID := "<example-message-id@example.com>"
    file, err := os.Create("article.txt")
    if err != nil {
        log.Fatal(err)
    }
    defer file.Close()

    written, err := pool.Body(ctx, msgID, file, nil)
    if err != nil {
        log.Fatal(err)
    }
    log.Printf("Downloaded %d bytes", written)

    // Example: Post an article
    article, err := os.Open("article.txt")
    if err != nil {
        log.Fatal(err)
    }
    defer article.Close()

    err = pool.Post(ctx, article)
    if err != nil {
        log.Fatal(err)
    }

    // Example: Check if an article exists
    msgNum, err := pool.Stat(ctx, msgID, []string{"alt.binaries.test"})
    if err != nil {
        log.Fatal(err)
    }
    log.Printf("Article number: %d", msgNum)
}
```

## Dynamic Reconfiguration

The connection pool supports dynamic reconfiguration, allowing you to update provider settings, add/remove providers, or change connection limits without interrupting ongoing operations. This is particularly useful for:

- Adding new providers when accounts become available
- Removing providers when accounts expire
- Updating connection limits based on load
- Modifying provider credentials or settings

### Using Reconfigure

```go
// Initial configuration
config := nntppool.Config{
    Providers: []nntppool.UsenetProviderConfig{
        {
            Host:           "news.example.com",
            Port:           563,
            Username:       "user1",
            Password:       "pass1",
            MaxConnections: 10,
            TLS:            true,
        },
    },
}

pool, err := nntppool.NewConnectionPool(config)
if err != nil {
    log.Fatal(err)
}
defer pool.Quit()

// Later, reconfigure with updated settings
newConfig := nntppool.Config{
    Providers: []nntppool.UsenetProviderConfig{
        {
            Host:           "news.example.com", 
            Port:           563,
            Username:       "user1",
            Password:       "pass1",
            MaxConnections: 15, // Increased connection limit
            TLS:            true,
        },
        {
            Host:           "news2.example.com", // New provider
            Port:           563,
            Username:       "user2", 
            Password:       "pass2",
            MaxConnections: 8,
            TLS:            true,
        },
    },
}

// Apply the reconfiguration - this is non-blocking and returns immediately
err = pool.Reconfigure(newConfig)
if err != nil {
    log.Printf("Reconfiguration failed: %v", err)
}
```

### Monitoring Reconfiguration Progress

You can monitor the progress of ongoing reconfigurations:

```go
// Get all active reconfigurations
reconfigurations := pool.GetActiveReconfigurations()
for migrationID, status := range reconfigurations {
    log.Printf("Migration %s: %s", migrationID, status.Status)
    log.Printf("  Started: %v", status.StartTime)
    log.Printf("  Changes: %d", len(status.Changes))
    
    // Check progress for each provider
    for providerID, providerStatus := range status.Progress {
        log.Printf("  Provider %s: %s (%d->%d connections)", 
            providerID, 
            providerStatus.Status,
            providerStatus.ConnectionsOld,
            providerStatus.ConnectionsNew)
    }
}

// Get status of a specific reconfiguration
if status, exists := pool.GetReconfigurationStatus("migration_id"); exists {
    log.Printf("Migration status: %s", status.Status)
    if status.Error != "" {
        log.Printf("Migration error: %s", status.Error)
    }
}
```

### How Reconfiguration Works

1. **Analysis**: The pool compares the new configuration with the current one to identify what changes are needed
2. **Incremental Migration**: Changes are applied incrementally to avoid service disruption:
   - **Add**: New providers are gradually added and connections established
   - **Update**: Existing providers have their settings updated and connections migrated
   - **Remove**: Providers are drained of connections before being removed
3. **Zero Downtime**: Existing connections continue serving requests while changes are applied
4. **Rollback**: If a migration fails, it can be rolled back to maintain service stability

### Reconfiguration States

- `"running"` - Migration is actively in progress
- `"completed"` - Migration finished successfully
- `"failed"` - Migration encountered an error
- `"rolled_back"` - Migration was rolled back due to failure

### Best Practices

- **Monitor Progress**: Always check reconfiguration status, especially in production
- **Gradual Changes**: Make incremental changes rather than large configuration overhauls
- **Error Handling**: Handle reconfiguration errors gracefully and consider rollback strategies
- **Testing**: Test configuration changes in development before applying to production

## Metrics System

The connection pool includes a comprehensive metrics system that provides detailed insights into connection pool performance while intelligently managing memory usage to prevent infinite growth over time.

### Key Features

- **Rolling Time Windows**: Metrics are organized into configurable time windows (default: 1 hour)
- **Automatic Cleanup**: Old metrics are automatically cleaned up based on retention policies
- **Memory Management**: Built-in memory monitoring with configurable thresholds and automatic cleanup
- **Connection Tracking**: Automatic detection and cleanup of stale connections
- **Data Compression**: Historical data is compressed into summaries for long-term storage
- **Real-time Monitoring**: Live metrics for active connections and pool performance

### Getting Metrics

```go
// Get comprehensive metrics snapshot
snapshot := pool.GetMetrics()

fmt.Printf("Active connections: %d\n", snapshot.ActiveConnections)
fmt.Printf("Total bytes downloaded: %d\n", snapshot.TotalBytesDownloaded)
fmt.Printf("Download speed: %.2f bytes/sec\n", snapshot.DownloadSpeed)
fmt.Printf("Error rate: %.2f%%\n", snapshot.ErrorRate)
fmt.Printf("Memory usage: %d bytes\n", snapshot.CurrentMemoryUsage)

// Check daily and weekly summaries
if snapshot.DailySummary != nil {
    fmt.Printf("Daily summary: %d connections created\n", snapshot.DailySummary.TotalConnectionsCreated)
}

if snapshot.WeeklySummary != nil {
    fmt.Printf("Weekly average: %.2f connections/hour\n", snapshot.WeeklySummary.AverageConnectionsPerHour)
}
```

### Configuring Metrics Retention

```go
// Configure metrics retention policy
config := nntppool.MetricRetentionConfig{
    DetailedRetentionDuration: 48 * time.Hour,        // Keep detailed metrics for 2 days
    RotationInterval:          30 * time.Minute,      // Create new windows every 30 minutes  
    MaxHistoricalWindows:      96,                    // Keep 96 windows (2 days of 30-min windows)
    MemoryThresholdBytes:      50 * 1024 * 1024,      // Trigger cleanup at 50MB
    AutoCleanupEnabled:        true,                  // Enable automatic cleanup
}

// Apply the configuration
metrics := pool.GetMetricsInstance() // You'll need to expose this method
metrics.SetRetentionConfig(config)
```

### Manual Maintenance

```go
// Perform manual cleanup and rotation check
metrics.PerformRotationCheck()

// Force connection cleanup
staleCount := metrics.ForceConnectionCleanup()
fmt.Printf("Cleaned up %d stale connections\n", staleCount)

// Get system status
status := metrics.GetRollingMetricsStatus()
fmt.Printf("Current window: %v to %v\n", status.CurrentWindowStartTime, status.CurrentWindowEndTime)
fmt.Printf("Historical windows: %d/%d\n", status.HistoricalWindowCount, status.MaxHistoricalWindows)

memory := metrics.GetMemoryUsage()
fmt.Printf("Memory: %d/%d bytes (%.1f%%)\n", 
    memory.AllocatedBytes, 
    memory.ThresholdBytes,
    float64(memory.AllocatedBytes)/float64(memory.ThresholdBytes)*100)
```

### Metrics Available

The system tracks comprehensive metrics including:

**Connection Metrics:**
- Total connections created/destroyed
- Active connection count
- Connection acquire/release operations
- Connection age and lifecycle

**Performance Metrics:**
- Download/upload speeds (recent and historical)
- Command success rates
- Error rates and retry counts
- Acquire wait times

**Traffic Metrics:**
- Bytes downloaded/uploaded
- Articles retrieved/posted
- Command counts and errors

**System Metrics:**
- Memory usage and thresholds
- Rolling window status
- Connection cleanup statistics
- Provider-specific metrics

### Automatic Cleanup Behavior

The metrics system automatically:

1. **Rotates windows** when time periods expire (e.g., every hour)
2. **Monitors memory usage** every 5 minutes by default
3. **Cleans up stale connections** every 30 seconds
4. **Compresses old data** when retention periods are exceeded
5. **Triggers aggressive cleanup** when memory thresholds are reached

This ensures that long-running applications maintain stable memory usage while preserving useful historical data for analysis and monitoring.

## Development Setup

To set up the project for development, follow these steps:

1. Clone the repository:

```sh
git clone https://github.com/javi11/nntppool.git
cd nntppool
```

2. Install dependencies:

```sh
go mod download
```

3. Run tests:

```sh
make test
```

4. Lint the code:

```sh
make lint
```

5. Generate mocks and other code:

```sh
make generate
```

## Contributing

Contributions are welcome! Please open an issue or submit a pull request. See the [CONTRIBUTING.md](CONTRIBUTING.md) file for details.

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.
