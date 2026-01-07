# nntppool Project Guide for Claude Code

## Project Overview

**nntppool** is a Go library that provides intelligent NNTP (Network News Transfer Protocol) connection pooling with advanced features for reliable Usenet operations.

**Purpose**: Enable reliable, high-performance Usenet article downloads through connection pooling, provider failover, round-robin load balancing, and yenc streaming decoding.

**Key Features**:

- Connection pooling with configurable connections per provider
- Multiple provider support with priority-based selection
- Backup provider support for fallback scenarios
- Round-robin load balancing within same priority level
- Automatic health checks with configurable intervals
- TLS support for secure connections
- Pipelining support for high throughput
- Streaming yenc decoding with metadata extraction
- Automatic writer flushing for data integrity

**Module**: `github.com/javi11/nntppool/v3`

## Architecture

### Core Components

1. **Pool** (`pool.go`)

   - Main connection pool managing multiple NNTP providers
   - Implements provider failover with primary/backup separation
   - Round-robin load balancing per priority level
   - Periodic health checks using DATE command
   - Thread-safe with mutex protection and atomic operations

2. **Provider** (`provider.go`)

   - Wraps `nntpcli.Client` with health tracking and metrics
   - Manages provider-specific configuration and state
   - Tracks request counts, errors, and article-not-found statistics
   - Thread-safe health status with mutex protection

3. **Configuration** (`config.go`)

   - `PoolConfig`: Pool-level configuration with providers list
   - `ProviderConfig`: Per-provider settings (host, TLS, auth, connections)
   - Default values and validation for all settings

4. **BodyResult** (`body_reader.go`)

   - Streaming reader for article bodies with io.Pipe
   - `YencMeta`: Extracted yenc header metadata (filename, size, CRC, etc.)
   - Background goroutine for async download with proper cleanup
   - Thread-safe metadata access with channel synchronization

5. **Errors** (`errors.go`)

   - Sentinel errors: `ErrArticleNotFound`, `ErrPoolClosed`, `ErrNoProvidersAvailable`, etc.
   - `ProviderError`: Rich error type with provider context and status codes
   - NNTP status code constants (220, 221, 222, 430, 411, 412, etc.)
   - Helper functions: `IsArticleNotFound()`, `IsRetryable()`

6. **nntpcli Package** (`pkg/nntpcli/`)
   - Low-level NNTP client library with pipelining support
   - `Client`: Manages pool of connections with shared request channel
   - `NNTPConnection`: Single connection with inflight request management
   - `Request`/`Response`: Command/response structures
   - `NNTPResponse`: Parsed response with yenc metadata
   - `readBuffer`: Efficient buffered reading with windowing

### Package Structure

```
nntppool/
├── pool.go                      # Main Pool implementation (~460 lines)
├── provider.go                  # Provider wrapper with health tracking (~290 lines)
├── config.go                    # Configuration structures (~190 lines)
├── body_reader.go               # Streaming body reader with yenc metadata (~270 lines)
├── errors.go                    # Error types and helpers (~100 lines)
├── pool_throughput_test.go      # Throughput benchmarks (~760 lines)
├── pkg/
│   └── nntpcli/                 # Low-level NNTP client library
│       ├── client.go            # Client managing connection pool (~120 lines)
│       ├── connection.go        # NNTPConnection with pipelining (~400 lines)
│       ├── reader.go            # Response parsing & yenc decoding (~370 lines)
│       └── readbuffer.go        # Efficient read buffering (~150 lines)
├── .github/
│   └── workflows/
│       ├── go.yml               # CI/CD pipeline
│       └── release.yml          # Release automation
├── Makefile                     # Build & test automation
├── .golangci.yml                # Linter configuration
└── .goreleaser.yaml             # Release configuration
```

### Key Dependencies

- **rapidyenc** (`github.com/mnightingale/rapidyenc`): Fast yenc encoding/decoding (requires CGO)
- **nntp-server-mock** (`github.com/javi11/nntp-server-mock`): Testing mock server

### Design Patterns

1. **Connection Pooling**: Client manages connections with shared request channel
2. **Provider Abstraction**: Wraps client with health tracking and metrics
3. **Round-Robin Load Balancing**: Per-priority level rotation with atomic counters
4. **Streaming Pipeline**: io.Pipe for async body streaming with metadata
5. **Graceful Degradation**: Primary/backup provider separation with automatic fallback
6. **Health Monitoring**: Periodic DATE command checks with automatic recovery

## Development Guidelines

### Go Standards

- **Go Version**: 1.25.1 or later
- **CGO**: Required for rapidyenc (yenc encoding/decoding)
- **Build**: `CGO_ENABLED=1 go build`

### Code Style

1. **Follow Standard Go Conventions**

   - Use `gofmt` and `golangci-lint`
   - Idiomatic Go naming (PascalCase for exports, camelCase for private)
   - Clear, descriptive variable names

2. **Error Handling**

   - Always handle errors explicitly
   - Use `fmt.Errorf` with `%w` for error wrapping
   - Use `ProviderError` for provider-specific errors with context
   - Return errors rather than logging and continuing

3. **Concurrency**

   - Use mutexes for shared state protection
   - Prefer `sync.RWMutex` for read-heavy operations
   - Use `atomic` for simple counters and flags
   - Always use `defer` for mutex unlocks
   - Context-aware operations with proper cancellation
   - Channel-based coordination for request distribution

4. **Documentation**

   - Document all exported types, functions, and constants
   - Use complete sentences starting with the name
   - Include usage examples for complex functions

### Thread Safety Patterns

This codebase emphasizes thread safety:

```go
// Pattern 1: Mutex-protected state access
type Pool struct {
    mu     sync.RWMutex
    closed bool
}

func (p *Pool) executeWithFallback(ctx context.Context, payload []byte, w io.Writer) (int64, error) {
    p.mu.RLock()
    if p.closed {
        p.mu.RUnlock()
        return 0, ErrPoolClosed
    }
    p.mu.RUnlock()
    // ... continue with operation
}

// Pattern 2: Atomic operations for round-robin
type Pool struct {
    rrIndex map[int]*atomic.Uint64
}

func (p *Pool) orderProvidersWithRoundRobin(providers []*Provider, ...) []*Provider {
    startIdx := int(counter.Add(1)-1) % len(group)
    // ... rotate starting position
}

// Pattern 3: Channel-based request distribution
type Client struct {
    reqCh chan *Request
}

func (c *Client) Send(ctx context.Context, payload []byte, bodyWriter io.Writer) <-chan Response {
    req := &Request{Ctx: ctx, Payload: payload, RespCh: make(chan Response, 1)}
    c.reqCh <- req
    return req.RespCh
}
```

## Testing Philosophy

### Guidelines (from CONTRIBUTING.md)

- **No Assert Libraries**: Use standard `testing` package only
- **Descriptive Names**: Test functions should clearly describe what they test
- **Useful Failures**: Error messages must provide sufficient debug information
- **Table-Driven Tests**: Use when testing multiple scenarios
- **Fuzz Testing**: Apply when appropriate for input validation
- **Coverage Goal**: Aim for 100% test coverage where feasible
- **Follow Google's Guidelines**: [Go Testing Best Practices](https://google.github.io/styleguide/go/decisions.html#useful-test-failures)

### Test Structure

```go
func TestPool_Body(t *testing.T) {
    tests := []struct {
        name    string
        setup   func(*testing.T) *Pool
        msgID   string
        want    int64
        wantErr bool
    }{
        {
            name: "successful download",
            setup: func(t *testing.T) *Pool {
                // setup code
            },
            msgID:   "<test@example.com>",
            want:    1024,
            wantErr: false,
        },
        // more test cases...
    }

    for _, tt := range tests {
        t.Run(tt.name, func(t *testing.T) {
            pool := tt.setup(t)
            defer pool.Close()

            var buf bytes.Buffer
            got, err := pool.Body(context.Background(), tt.msgID, &buf)

            if (err != nil) != tt.wantErr {
                t.Errorf("Body() error = %v, wantErr %v", err, tt.wantErr)
                return
            }
            if got != tt.want {
                t.Errorf("Body() = %v, want %v", got, tt.want)
            }
        })
    }
}
```

### Mocking

- **Tool**: `go.uber.org/mock` (mockgen)
- **Generation**: Automated via `//go:generate` directives
- **Convention**: Mock files named `*_mock.go`
- **Command**: `make generate` to regenerate mocks

## Common Tasks

### Makefile Targets

```bash
# Development workflow
make check          # Run all checks (generate, tidy, lint, test-race)
make test           # Run all tests
make test-race      # Run tests with race detector
make coverage       # Generate coverage report
make coverage-html  # View coverage in browser
make lint           # Run tidy + golangci-lint
make generate       # Generate mocks and other code

# Code quality
make golangci-lint       # Run linter
make golangci-lint-fix   # Run linter with auto-fix
make govulncheck         # Check for vulnerabilities
make tidy                # Clean up go.mod

# CI/CD
make junit          # Generate JUnit test report
make coverage-ci    # Coverage for CI with race detector
make release        # Create release
make snapshot       # Create snapshot build
```

### Development Workflow

1. **Make Changes**: Edit code
2. **Generate**: `make generate` (if interfaces changed)
3. **Test**: `make test` or `make test-race`
4. **Lint**: `make golangci-lint-fix`
5. **Coverage**: `make coverage` (aim for 100%)
6. **Check**: `make check` (before committing)

### Running Tests

```bash
# All tests
CGO_ENABLED=1 go test ./...

# Specific test
CGO_ENABLED=1 go test -run TestPool_Body

# With race detector
CGO_ENABLED=1 go test -race ./...

# With coverage
CGO_ENABLED=1 go test -coverprofile=coverage.out ./...
go tool cover -html=coverage.out

# Verbose output
CGO_ENABLED=1 go test -v ./...
```

## Important Notes

### CGO Requirement

**Critical**: This project requires CGO to be enabled for the `rapidyenc` dependency.

```bash
# Building
CGO_ENABLED=1 go build

# Testing
CGO_ENABLED=1 go test ./...

# Installing
CGO_ENABLED=1 go get github.com/javi11/nntppool/v3
```

### Thread Safety Considerations

- All public methods on `Pool` are thread-safe
- Provider health status is mutex-protected
- Round-robin counters use atomic operations
- Request channel provides thread-safe distribution to connections
- BodyResult uses io.Pipe for thread-safe streaming

### Context Usage

- All operations accept `context.Context` for cancellation
- Respect context deadlines and cancellations
- Pass context to all downstream operations
- ReadTimeout from ProviderConfig creates operation-level context timeouts

### Provider Selection Algorithm

1. **Primary First**: Always try primary providers before backup providers
2. **Priority Order**: Lower priority value = higher priority (tried first)
3. **Round-Robin**: Within same priority, rotate starting position
4. **Health Filter**: Skip unhealthy providers automatically
5. **Host Dedup**: Skip providers with same host if article was not found
6. **Backup Fallback**: Use backup providers only after all primaries return "not found"

### Health Check System

- Health checks run periodically (default: 1 minute)
- Uses `DATE` command which returns status 111
- Failed providers are marked unhealthy and skipped
- Automatic recovery when health check succeeds
- Configure interval via `PoolConfig.HealthCheckInterval` (0 to disable)

### Performance Tuning

```go
// For optimal throughput, configure MaxConnections based on concurrency needs:
// Total concurrent requests = MaxConnections × InflightPerConn

cfg := nntppool.ProviderConfig{
    Host:            "news.example.com",
    MaxConnections:  20,      // Number of concurrent connections
    InflightPerConn: 1,       // Pipelined requests per connection
    // With 20 connections × 1 inflight = 20 concurrent requests
}
```

### Streaming Body Reader

```go
// BodyReader returns a streaming reader with yenc metadata
result, err := pool.BodyReader(ctx, "message-id@example.com")
if err != nil {
    return err
}
defer result.Close()

// Read decoded body data
_, err = io.Copy(outputFile, result)

// Access metadata (blocks until headers parsed)
meta := result.Meta()
fmt.Printf("File: %s, Part: %d/%d, CRC: %08x\n",
    meta.FileName, meta.Part, meta.Total, meta.ActualCRC)
```

## API Reference

### Pool Methods

| Method | Description |
|--------|-------------|
| `NewPool(ctx, cfg)` | Create new connection pool |
| `Body(ctx, msgID, w)` | Download article body to writer |
| `Article(ctx, msgID, w)` | Download complete article to writer |
| `Head(ctx, msgID)` | Get article headers (not yet implemented) |
| `Group(ctx, name)` | Select newsgroup on all connections |
| `BodyReader(ctx, msgID)` | Get streaming body reader with metadata |
| `Stats()` | Get statistics for all providers |
| `HealthyProviderCount()` | Count of healthy providers |
| `Close()` | Close pool and all connections |

### Configuration Defaults

| Setting | Default Value |
|---------|---------------|
| MaxConnections | 10 |
| InflightPerConn | 1 |
| ConnectTimeout | 30s |
| ReadTimeout | 60s |
| WriteTimeout | 30s |
| HealthCheckInterval | 1 minute |
| Port (non-TLS) | 119 |
| Port (TLS) | 563 |

## File Reference

### Main Package Files

| File | Lines | Purpose |
|------|-------|---------|
| `pool.go` | ~460 | Main Pool with provider management and failover |
| `provider.go` | ~290 | Provider wrapper with health tracking |
| `config.go` | ~190 | Configuration structures and validation |
| `body_reader.go` | ~270 | BodyResult streaming reader with YencMeta |
| `errors.go` | ~100 | Sentinel errors and ProviderError type |

### nntpcli Package Files

| File | Lines | Purpose |
|------|-------|---------|
| `client.go` | ~120 | Client managing connection pool |
| `connection.go` | ~400 | NNTPConnection with pipelining |
| `reader.go` | ~370 | Response parsing and yenc decoding |
| `readbuffer.go` | ~150 | Efficient read buffering |

### Test Files

| File | Lines | Purpose |
|------|-------|---------|
| `pool_throughput_test.go` | ~760 | Throughput benchmarks and concurrent tests |

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for pull request workflow and testing guidelines.

## Additional Resources

- [Project README](README.md): Installation and usage
- [nntpcli Package](https://pkg.go.dev/github.com/javi11/nntppool/v3/pkg/nntpcli): NNTP client library
- [Google Go Testing Guide](https://google.github.io/styleguide/go/decisions.html#useful-test-failures)
