# nntppool

[![Go Reference](https://pkg.go.dev/badge/github.com/javi11/nntppool/v3.svg)](https://pkg.go.dev/github.com/javi11/nntppool/v3)
[![Go Report Card](https://goreportcard.com/badge/github.com/javi11/nntppool/v3)](https://goreportcard.com/report/github.com/javi11/nntppool/v3)

High-performance, concurrent NNTP client library for Go with streaming support, automatic failover, and built-in metrics.

## Features

- **Streaming API** with memory-efficient `BodyReader` for large article downloads
- **Multi-provider support** with automatic failover (Primary/Backup tiers)
- **SOCKS proxy support** (socks4, socks4a, socks5) with authentication
- **Built-in metrics** with real-time throughput monitoring
- **Automatic yEnc decoding** with header callbacks for early metadata access
- **Connection lifecycle management** (idle timeout, max lifetime, automatic rotation)
- **High concurrency** with configurable inflight limits
- **Health checking** with automatic provider recovery
- **TLS support** for secure NNTPS connections
- **Context-aware** for timeout and cancellation support

## Installation

```bash
go get github.com/javi11/nntppool/v3
```

## Quick Start

```go
package main

import (
    "context"
    "crypto/tls"
    "fmt"
    "os"

    "github.com/javi11/nntppool/v3"
)

func main() {
    ctx := context.Background()

    // Create provider configuration
    provider, err := nntppool.NewProvider(ctx, nntppool.ProviderConfig{
        Address:        "news.example.com:563",
        MaxConnections: 10,
        Auth: nntppool.Auth{
            Username: "user",
            Password: "pass",
        },
        TLSConfig: &tls.Config{},
    })
    if err != nil {
        panic(err)
    }
    defer provider.Close()

    // Create client (implements NNTPClient interface)
    client := nntppool.NewClient()
    err = client.AddProvider(provider, nntppool.ProviderPrimary)
    if err != nil {
        panic(err)
    }
    defer client.Close()

    // Download article body
    file, err := os.Create("output.bin")
    if err != nil {
        panic(err)
    }
    defer file.Close()

    if err := client.Body(ctx, "<message-id@example.com>", file); err != nil {
        panic(err)
    }

    fmt.Println("Article downloaded successfully!")
}
```

**Note**: The `Client` type implements the `NNTPClient` interface. See `types.go` for the complete interface definition.

## Usage Examples

### Basic Article Retrieval

```go
// Simple download to io.Writer
err := client.Body(ctx, "<message-id>", outputFile)
```

### Streaming with yEnc Headers

Get early access to yEnc metadata while streaming the body:

```go
reader, err := client.BodyReader(ctx, "<message-id>")
if err != nil {
    log.Fatal(err)
}
defer reader.Close()

// Headers become available immediately after parsing (non-blocking)
headers := reader.YencHeaders()
if headers != nil {
    fmt.Printf("File: %s, Size: %d bytes\n", headers.FileName, headers.FileSize)
}

// Stream decoded body to file
_, err = io.Copy(outputFile, reader)
```

### Multi-Segment Downloads

Write directly to file offsets for parallel multi-segment downloads:

```go
// file must implement io.WriterAt (e.g., *os.File)
err := client.BodyAt(ctx, "<segment-message-id>", file)
```

### Multi-Provider Setup

Configure primary and backup providers with automatic failover:

```go
// Primary provider
primaryProvider, _ := nntppool.NewProvider(ctx, nntppool.ProviderConfig{
    Address:        "primary.news.com:563",
    MaxConnections: 10,
    Auth:           nntppool.Auth{Username: "user", Password: "pass"},
    TLSConfig:      &tls.Config{},
})

// Backup provider
backupProvider, _ := nntppool.NewProvider(ctx, nntppool.ProviderConfig{
    Address:        "backup.news.com:563",
    MaxConnections: 5,
    Auth:           nntppool.Auth{Username: "user2", Password: "pass2"},
    TLSConfig:      &tls.Config{},
})

// Add to client with tier priority
client := nntppool.NewClient()
err = client.AddProvider(primaryProvider, nntppool.ProviderPrimary)
if err != nil {
    panic(err)
}
err = client.AddProvider(backupProvider, nntppool.ProviderBackup)
if err != nil {
    panic(err)
}

// Client automatically tries primary first, falls back to backup on errors
```

### SOCKS Proxy Configuration

```go
config := nntppool.ProviderConfig{
    Address:        "news.example.com:119",
    MaxConnections: 10,
    ProxyURL:       "socks5://user:pass@proxy.example.com:1080",
    Auth:           nntppool.Auth{Username: "newsuser", Password: "newspass"},
}

provider, err := nntppool.NewProvider(ctx, config)
```

### Metrics Monitoring

Monitor real-time throughput and connection status:

```go
metrics := client.Metrics()
for host, m := range metrics {
    fmt.Printf("%s: %.2f MB/s (%d active connections)\n",
        host, m.ThroughputMB, m.ActiveConnections)
    fmt.Printf("  Total read: %d bytes, written: %d bytes\n",
        m.TotalBytesRead, m.TotalBytesWritten)
}
```

### Speed Testing

Measure download performance across multiple articles:

```go
articleIDs := []string{
    "<article1@example.com>",
    "<article2@example.com>",
    "<article3@example.com>",
}

stats, err := client.SpeedTest(ctx, articleIDs)
if err != nil {
    log.Fatal(err)
}

fmt.Printf("Speed: %.2f MB/s\n", stats.BytesPerSecond/1024/1024)
fmt.Printf("Success: %d, Failures: %d\n", stats.SuccessCount, stats.FailureCount)
fmt.Printf("Total: %d bytes in %v\n", stats.TotalBytes, stats.Duration)
```

## Configuration Reference

### ProviderConfig Options

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `Address` | `string` | ✅ | - | NNTP server address in `host:port` format |
| `MaxConnections` | `int` | ✅ | - | Maximum concurrent connections to maintain |
| `InitialConnections` | `int` | ❌ | `0` | Number of connections to create on startup (lazy if 0) |
| `InflightPerConnection` | `int` | ❌ | `1` | Maximum concurrent requests per connection |
| `MaxConnIdleTime` | `time.Duration` | ❌ | `0` | Auto-close connections idle longer than this duration |
| `MaxConnLifetime` | `time.Duration` | ❌ | `0` | Auto-rotate connections older than this duration |
| `Auth` | `Auth` | ❌ | empty | NNTP authentication credentials (`Username`, `Password`) |
| `TLSConfig` | `*tls.Config` | ❌ | `nil` | TLS configuration (nil = plaintext connection) |
| `ConnFactory` | `ConnFactory` | ❌ | `nil` | Custom connection factory (overrides proxy/direct dial) |
| `ProxyURL` | `string` | ❌ | `""` | SOCKS proxy URL (used when `ConnFactory` is nil) |

### Connection Lifecycle

Control connection behavior with lifecycle options:

- **`MaxConnIdleTime`**: Automatically close connections that have been idle (no requests) longer than the specified duration. Useful for preventing stale connections.
- **`MaxConnLifetime`**: Automatically rotate connections that have been active longer than the specified duration, regardless of usage. Useful for load balancing and preventing resource exhaustion.
- Both options are disabled by default (0 value = no limit).

### Proxy URL Formats

When `ConnFactory` is `nil`, the `ProxyURL` field supports SOCKS proxies:

- **SOCKS5 without auth**: `socks5://proxy.host:1080`
- **SOCKS5 with auth**: `socks5://user:pass@proxy.host:1080`
- **SOCKS4**: `socks4://proxy.host:1080`
- **SOCKS4a**: `socks4a://proxy.host:1080`

## Advanced Topics

### TLS & Custom Connections

```go
// TLS configuration for NNTPS (port 563)
config := nntppool.ProviderConfig{
    Address: "news.example.com:563",
    TLSConfig: &tls.Config{
        MinVersion: tls.VersionTLS12,
        ServerName: "news.example.com",
    },
}

// Custom connection factory for advanced dialing
config.ConnFactory = func(ctx context.Context) (net.Conn, error) {
    dialer := &net.Dialer{Timeout: 30 * time.Second}
    conn, err := dialer.DialContext(ctx, "tcp", "news.example.com:563")
    if err != nil {
        return nil, err
    }
    // Apply custom socket options, wrapping, etc.
    return conn, nil
}
```

### Automatic Features

The library includes several automatic features that require no configuration:

- **Health Monitoring**: Providers are automatically checked every 60 seconds using the `DATE` command. Dead providers are marked inactive and automatically recovered when they become responsive again.

- **yEnc Decoding**: All article bodies are automatically detected and decoded if they contain yEnc encoding. Includes CRC32 validation and support for multi-part articles with part metadata tracking.

- **Error Handling**: Status code interpretation follows NNTP standards:
  - `2xx` status codes indicate success
  - `430` indicates article not found (triggers failover to backup provider)
  - Other status codes indicate server or connection errors

## Performance Considerations

- **Connection Pooling**: Each provider maintains an independent connection pool. Connections are created lazily up to `MaxConnections` and reused across requests.

- **Concurrency Control**: Callers control concurrency externally using mechanisms like `errgroup.SetLimit()`, worker pools, or semaphores. Provider-level connection limits handle internal throttling via `MaxConnections` and `InflightPerConnection` settings.

- **Streaming**: Use `BodyReader()` or `BodyAt()` to avoid buffering large articles in memory. The streaming API decodes yEnc on-the-fly and writes directly to the destination.

- **TCP Buffers**: All connections are automatically optimized with 8MB read buffers and 1MB write buffers for high-speed downloads.

- **Sequential Readers with io.Pipe**: When implementing a sequential reader (`io.Reader` that must consume segments in order) using `io.Pipe`, download segment data to a buffer first, then copy to the pipe. Direct pipe writes can cause connection deadlock:
  1. Workers hold connections while blocked on pipe writes (pipes are synchronous)
  2. Pipe writes block until the reader consumes them sequentially
  3. Reader can't reach later pipes because connections are held by workers blocked on earlier pipes

  **Solution**: Download to buffer first, then write to pipe:
  ```go
  var buf bytes.Buffer
  client.Body(ctx, id, &buf)  // Connection released here
  io.Copy(pipeWriter, &buf)   // May block, but connection is free
  ```

- **Health Checks**: Provider health checks run every 60 seconds with minimal overhead (single `DATE` command per provider).

## Examples

See working example programs in the repository:

- **`examples/proxy_example.go`**: Complete examples of SOCKS proxy configuration (socks4, socks4a, socks5 with authentication)
- **`cmd/nzb-downloader/`**: Full-featured NZB downloader with concurrent downloads, progress tracking, and metrics display

## Testing

```bash
# Run all tests
go test -v ./...

# Run with race detector
go test -race ./...

# Run benchmarks
go test -bench=. -benchmem
```

## License

MIT License

---

**Full API Documentation**: See [pkg.go.dev](https://pkg.go.dev/github.com/javi11/nntppool/v3) for complete API reference and additional examples.
