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
