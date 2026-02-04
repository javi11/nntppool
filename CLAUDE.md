# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

**nntppool** is a high-performance, concurrent NNTP (Network News Transfer Protocol) client library for Go. It provides connection pooling with automatic failover between multiple USENET providers, streaming downloads with yEnc decoding, and built-in metrics.

## Common Commands

```bash
# Full validation (generate → tidy → lint → test with race detector)
make check

# Run tests
make test              # Basic tests
make test-race         # With race detector (recommended)

# Run a single test
go test -v -run TestName ./...

# Run benchmarks
go test -bench=. -benchmem

# Linting
make golangci-lint           # Check for issues
make golangci-lint-fix       # Auto-fix issues

# Coverage
make coverage                # Generate coverage.out
make coverage-html           # Generate HTML report
```

## Architecture

### Core Components

```
Client (client.go)
    ├── Provider selection & failover logic
    ├── Health monitoring (60s DATE command checks)
    └── Public API: Body, BodyReader, BodyAt, Article, Head, Stat, Post

Provider (provider.go)
    ├── Connection pool for a single NNTP server
    ├── Lazy connection creation up to MaxConnections
    ├── Throttling on "max connections exceeded" (5-min backoff)
    └── Metrics collection (throughput, bytes read/written)

NNTPConnection (connection.go)
    ├── Individual TCP connection lifecycle
    ├── Authentication handshake
    ├── Request multiplexing via channel-based queue
    └── Idle/lifetime expiration

NNTPResponse (reader.go)
    ├── NNTP multiline response parser
    ├── yEnc format detection & decoding with CRC32 validation
    └── Streaming decode to io.Writer
```

### Request Flow

1. User calls `client.Body(ctx, messageID, writer)`
2. Client tries primary providers in order, then backups on failure
3. Provider selects an available connection from pool (or creates one)
4. Connection sends NNTP command, parses response via NNTPResponse
5. Response decoder handles yEnc/UU decoding, streams to writer

### Key Interfaces

```go
// types.go - Main public interface
type NNTPClient interface {
    AddProvider(provider *Provider, tier ProviderType) error
    Body(ctx context.Context, id string, w io.Writer) error
    BodyReader(ctx context.Context, id string) (YencReader, error)
    BodyAt(ctx context.Context, id string, w io.WriterAt) error
    // ... see types.go for full interface
}

// Provider tiers for failover
ProviderPrimary  // Tried first
ProviderBackup   // Tried after primaries fail
```

### Concurrency Patterns

- **Atomic values** for lock-free provider list reads (`Client.primaries/backups`)
- **Channel-based** request queues per connection (`reqCh`)
- **Semaphore channels** for inflight request limiting
- **Context propagation** for timeouts and cancellation

### Buffer Sizes

- 8MB read buffer per connection
- 1MB write buffer per connection
- Configured in `internal/connection.go`

## Testing

The `testutil/` package provides a mock NNTP server for integration tests:

```go
server := testutil.NewMockServer(t, testutil.DefaultHandlers())
defer server.Close()

provider, _ := nntppool.NewProvider(ctx, nntppool.ProviderConfig{
    Address: server.Addr(),
    // ...
})
```

## Key Files

| File | Purpose |
|------|---------|
| `types.go` | Public interfaces and types (NNTPClient, Request, Response, YencHeader) |
| `client.go` | Client implementation with provider failover |
| `provider.go` | Connection pool management per server |
| `connection.go` | Individual NNTP connection handling |
| `reader.go` | NNTP response parsing and yEnc decoding |
| `internal/` | TCP buffer optimization, read buffer, adapters |

## Error Handling

- `ArticleNotFoundError` - 430 status (article not found), triggers failover to backup
- Status codes 2xx indicate success
- Partial write tracking prevents corruption on failover mid-transfer
