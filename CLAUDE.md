# nntppool Project Guide for Claude Code

## Project Overview

**nntppool** is a Go library that provides intelligent NNTP (Network News Transfer Protocol) connection pooling with advanced features for reliable Usenet operations.

**Purpose**: Enable reliable, high-performance Usenet article downloads and posts through connection pooling, automatic retries, provider rotation, and yenc encoding/decoding.

**Key Features**:

- Connection pooling with configurable min/max connections per provider
- Multiple provider support with automatic rotation on failures
- Backup provider support for block account usage
- Dynamic reconfiguration without service interruption
- Comprehensive metrics system with rolling windows and memory management
- TLS support for secure connections
- Automatic retry mechanisms with configurable strategies
- Yenc encoding/decoding for binary data

**Module**: `github.com/javi11/nntppool/v2`

## Architecture

### Core Components

1. **ConnectionPool** (`connection_pool.go`)

   - Main interface and implementation for connection pooling
   - Manages lifecycle of connections across multiple providers
   - Implements retry logic and provider rotation
   - Handles dynamic reconfiguration
   - Thread-safe with extensive use of mutexes and atomic operations

2. **Provider** (`provider.go`, `internal/provider/`)

   - Represents a single NNTP provider (primary or backup)
   - Manages provider-specific connection pools using `puddle`
   - Tracks provider health and status
   - Implements connection lifecycle management

3. **Configuration** (`config.go`, `internal/config/`)

   - `Config`: Main pool configuration
   - `UsenetProviderConfig`: Per-provider settings
   - `MetricRetentionConfig`: Metrics system configuration
   - Supports validation and reconfiguration

4. **Metrics System** (`metrics.go`)

   - Comprehensive metrics with rolling time windows (default: 1 hour)
   - Automatic cleanup to prevent memory growth
   - Connection tracking with stale connection detection
   - Performance metrics (download/upload speeds, error rates)
   - Memory usage monitoring

5. **PooledConnection** (`pooled_connection.go`)

   - Lightweight wrapper around `puddle.Resource` connections
   - Provides automatic release on Close/Free with metrics tracking
   - Thread-safe operations
   - Simplified design: no panic recovery, no manual lease management
   - Connection IDs generated from resource pointers (no atomic counter)

6. **PooledBodyReader** (`pooled_body_reader.go`)

   - Wraps article body readers with automatic connection release
   - Handles yenc header extraction
   - Thread-safe with mutex protection

7. **Helper Functions** (`helpers.go`)
   - Retry logic for NNTP operations
   - Provider selection and rotation
   - Error handling utilities

### Package Structure

```
nntppool/
├── *.go                      # Main package implementation
├── *_test.go                 # Unit and integration tests
├── *_mock.go                 # Generated mocks
├── internal/
│   ├── config/              # Internal configuration utilities
│   ├── provider/            # Provider pooling implementation
│   ├── helpers/             # Internal helper functions
│   └── budget/              # Budget management utilities
├── .github/                 # CI/CD workflows
└── test-results/            # Test output directory
```

### Key Dependencies

- **nntpcli** (`github.com/javi11/nntppool/v2/pkg/nntpcli`): NNTP client library
- **puddle** (`github.com/jackc/puddle/v2`): Generic resource pool
- **rapidyenc** (`github.com/mnightingale/rapidyenc`): Fast yenc encoding/decoding (requires CGO)
- **retry-go** (`github.com/avast/retry-go/v4`): Retry mechanisms
- **multierror** (`github.com/hashicorp/go-multierror`): Error aggregation

### Design Patterns

1. **Resource Pooling**: Uses puddle for efficient connection reuse
2. **Provider Pattern**: Abstracts NNTP provider details
3. **Strategy Pattern**: Configurable retry strategies (fixed, random, exponential)
4. **Observer Pattern**: Metrics collection and monitoring
5. **State Machine**: Connection lifecycle management
6. **Graceful Degradation**: Backup providers for resilience

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
   - Use `multierror` for aggregating multiple errors
   - Return errors rather than logging and continuing

3. **Concurrency**

   - Use mutexes for shared state protection
   - Prefer `sync.RWMutex` for read-heavy operations
   - Use `atomic` for simple counters and flags
   - Always use `defer` for mutex unlocks
   - Context-aware operations with proper cancellation

4. **Documentation**

   - Document all exported types, functions, and constants
   - Use complete sentences starting with the name
   - Include usage examples for complex functions

5. **Logging**
   - Use structured logging via the `Logger` interface (slog-compatible)
   - Include relevant context (provider, connection ID, message ID)
   - Use appropriate log levels (Debug, Info, Warn, Error)

### Thread Safety Patterns

This codebase emphasizes thread safety:

```go
// Pattern 1: Mutex-protected state access
type ConnectionPool struct {
    mu sync.RWMutex
    providers []*provider
}

func (cp *ConnectionPool) GetProvider() *provider {
    cp.mu.RLock()
    defer cp.mu.RUnlock()
    return cp.providers[0]
}

// Pattern 2: Atomic operations for counters
type metrics struct {
    activeConnections atomic.Int32
}

// Pattern 3: Channel-based coordination
func (cp *ConnectionPool) shutdown(ctx context.Context) {
    done := make(chan struct{})
    go func() {
        // cleanup work
        close(done)
    }()
    select {
    case <-done:
    case <-ctx.Done():
    }
}
```

### Common Code Patterns

#### 1. Connection Acquisition with Retry

```go
conn, err := retry.Do(
    func() (PooledConnection, error) {
        return pool.GetConnection(ctx, skipProviders, useBackup)
    },
    retry.Context(ctx),
    retry.Attempts(maxRetries),
    retry.DelayType(retry.BackOffDelay),
)
```

#### 2. Provider Rotation on Failure

```go
skipProviders := []string{}
for attempt := 0; attempt < maxRetries; attempt++ {
    conn, err := pool.GetConnection(ctx, skipProviders, false)
    if err == nil {
        return conn, nil
    }
    skipProviders = append(skipProviders, lastProviderHost)
}
```

#### 3. Metrics Collection

```go
startTime := time.Now()
defer func() {
    pool.metrics.RecordOperation("body", time.Since(startTime), err)
}()
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
func TestConnectionPool_Body(t *testing.T) {
    tests := []struct {
        name    string
        setup   func(*testing.T) *ConnectionPool
        msgID   string
        want    int64
        wantErr bool
    }{
        {
            name: "successful download",
            setup: func(t *testing.T) *ConnectionPool {
                // setup code
            },
            msgID: "<test@example.com>",
            want: 1024,
            wantErr: false,
        },
        // more test cases...
    }

    for _, tt := range tests {
        t.Run(tt.name, func(t *testing.T) {
            pool := tt.setup(t)
            got, err := pool.Body(context.Background(), tt.msgID, io.Discard, nil)

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

Example:

```go
//go:generate go tool mockgen -source=./connection_pool.go -destination=./connection_pool_mock.go -package=nntppool UsenetConnectionPool
```

## Common Tasks

### Makefile Targets

```bash
# Development workflow
make check          # Run all checks (test, lint, coverage)
make test           # Run all tests
make test-race      # Run tests with race detector
make coverage       # Generate coverage report
make coverage-html  # View coverage in browser
make lint           # Run golangci-lint
make generate       # Generate mocks and other code

# Code quality
make golangci-lint       # Run linter
make golangci-lint-fix   # Run linter with auto-fix
make govulncheck         # Check for vulnerabilities
make tidy                # Clean up go.mod

# CI/CD
make junit          # Generate JUnit test report
make coverage-ci    # Coverage for CI
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
go test ./...

# Specific test
go test -run TestConnectionPool_Body

# With race detector
go test -race ./...

# With coverage
go test -coverprofile=coverage.out ./...
go tool cover -html=coverage.out

# Verbose output
go test -v ./...
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
CGO_ENABLED=1 go get github.com/javi11/nntppool/v2
```

### Thread Safety Considerations

- All public methods on `ConnectionPool` are thread-safe
- Connections from the pool are safe for concurrent use
- Metrics collection is thread-safe
- Provider reconfiguration uses careful locking to avoid deadlocks

### Context Usage

- All operations accept `context.Context` for cancellation
- Respect context deadlines and cancellations
- Pass context to all downstream operations
- Use `context.WithTimeout` for operations with time limits

### Provider Health Checks

- Health checks run periodically (default: 1 minute)
- Failed providers are marked and can be temporarily skipped
- Automatic recovery when providers become healthy again
- Configure via `HealthCheckInterval` in `Config`

### Memory Management

- Metrics system automatically cleans up old data
- Connection pools respect `MaxConnections` limits
- Idle connections are closed after timeout
- Configure retention via `MetricRetentionConfig`

## File Structure Reference

### Key Files

- `connection_pool.go`: Main pool implementation (1,300+ lines)
- `config.go`: Configuration structures and validation
- `provider.go`: Provider management
- `metrics.go`: Metrics system (1,600+ lines)
- `pooled_connection.go`: Simplified connection wrapper (111 lines, reduced from 198)
- `pooled_body_reader.go`: Body reader wrapper
- `helpers.go`: Retry and rotation logic
- `errors.go`: Error definitions

### Test Files

- `*_test.go`: Unit tests
- `*_integration_test.go`: Integration tests
- `standalone_connectivity_test.go`: Full end-to-end tests
- `example_metrics_usage.go`: Metrics usage examples

### Generated Files

- `*_mock.go`: Generated mocks (regenerate with `make generate`)

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for pull request workflow and testing guidelines.

## Additional Resources

- [Project README](README.md): Installation and usage
- [nntpcli Documentation](https://pkg.go.dev/github.com/javi11/nntppool/v2/pkg/nntpcli): NNTP client library
- [puddle Documentation](https://pkg.go.dev/github.com/jackc/puddle/v2): Resource pooling
- [Google Go Testing Guide](https://google.github.io/styleguide/go/decisions.html#useful-test-failures)
