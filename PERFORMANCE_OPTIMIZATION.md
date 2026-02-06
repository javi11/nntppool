# Performance Optimization Guide

## Current Performance Issue: 40 MB/s Limit

The current implementation achieves only ~40 MB/s due to several architectural bottlenecks in the connection management system.

## Root Causes

### 1. Excessive Channel Operations (PRIMARY BOTTLENECK)

**Current implementation: 6 channel operations per request**
- `provider.go:710` - Send to shared `reqCh`
- `connection.go:291` - Receive from `reqCh`
- `connection.go:278` - Acquire inflight semaphore
- `connection.go:325` - Send to `pending` queue
- `connection.go:361` - Receive from `pending` queue
- `connection.go:448` - Release inflight semaphore

**Impact:**
- At 40 MB/s with 100KB articles: ~41,000 requests/sec = **246,000 channel ops/sec**
- Each channel operation involves internal mutex locks
- Context switching overhead between goroutines

**Simple acquire/release pattern: 2-4 operations**
- Acquire connection from pool
- Return connection to pool
- Optional: semaphore for rate limiting

### 2. Small Request Queue Buffer

```go
// provider.go:194
reqCh: make(chan *Request, config.MaxConnections)  // Buffer size = 10-20
```

**Problem:**
- With 100 concurrent requests and 10 connection buffer, 90 requests block on send
- Blocked requests hit 5-second timeout (`provider.go:20`)
- Failed requests retry, adding latency

**Solution:**
- Increase buffer to `MaxConnections * InflightPerConnection * 2`
- Or use acquire/release pattern to eliminate queue entirely

### 3. Default Configuration Disables Pipelining

```go
// provider.go:169-171
if config.InflightPerConnection == 0 {
    config.InflightPerConnection = 1  // Serial mode
}
```

**Impact on throughput:**
- 10 connections × 1 inflight × 20 req/sec (50ms RTT) = **200 req/sec max**
- At 100KB/article = **20 MB/s theoretical maximum**

**Solution:**
- Set `InflightPerConnection = 5-10` for high throughput scenarios
- Benchmarks in `connection_bench_test.go` use 5-10 for 100+ MB/s

### 4. Aggressive 5-Second Timeout

```go
// provider.go:20
const sendRequestTimeout = 5 * time.Second
```

**Problem:**
- High concurrency scenarios with small buffer quickly hit timeout
- Causes request failures and retries
- Wasted work and reduced effective throughput

**Solution:**
- Increase to 30-60 seconds
- Or remove timeout and rely on context cancellation

### 5. Goroutine Creation on Every Request

```go
// provider.go:647-652
if atomic.LoadInt32(&c.connCount) < c.getEffectiveMaxConnections() {
    c.sendWg.Add(1)
    go func() {
        defer c.sendWg.Done()
        _ = c.addConnection(false)
    }()
}
```

**Problem:**
- Every `SendRequest()` spawns a goroutine to try scaling connections
- At 41,000 req/sec = **41,000 goroutines/sec created**
- Each goroutine allocation is ~2-4KB stack

**Solution:**
- Check if already at max before spawning goroutine
- Use connection pool initialization at startup

## Recommended Configurations

### For Maximum Throughput (100+ MB/s target)

```go
provider, err := nntppool.NewProvider(ctx, nntppool.ProviderConfig{
    Address:               "news.example.com:563",
    MaxConnections:        20,              // 20-30 connections
    InitialConnections:    20,              // Pre-create all connections
    InflightPerConnection: 10,              // 10 requests in-flight per connection
    MaxConnIdleTime:       5 * time.Minute,
    MaxConnLifetime:       30 * time.Minute,
})
```

**Effective concurrency: 20 × 10 = 200 requests in-flight**

### For Balanced Performance (Medium throughput, low resource usage)

```go
provider, err := nntppool.NewProvider(ctx, nntppool.ProviderConfig{
    Address:               "news.example.com:563",
    MaxConnections:        10,
    InitialConnections:    10,
    InflightPerConnection: 5,               // Moderate pipelining
    MaxConnIdleTime:       5 * time.Minute,
    MaxConnLifetime:       30 * time.Minute,
})
```

**Effective concurrency: 10 × 5 = 50 requests in-flight**

### For Low Latency (Minimize request queueing)

```go
provider, err := nntppool.NewProvider(ctx, nntppool.ProviderConfig{
    Address:               "news.example.com:563",
    MaxConnections:        30,              // More connections
    InitialConnections:    30,
    InflightPerConnection: 3,               // Low pipelining for predictable latency
    MaxConnIdleTime:       5 * time.Minute,
    MaxConnLifetime:       30 * time.Minute,
})
```

## Code Modifications for Better Performance

### Option 1: Increase Request Buffer Size

**File:** `provider.go:194`

```go
// Before:
reqCh: make(chan *Request, config.MaxConnections)

// After:
bufferSize := config.MaxConnections * config.InflightPerConnection
if bufferSize < config.MaxConnections * 2 {
    bufferSize = config.MaxConnections * 2  // Minimum 2x buffer
}
reqCh: make(chan *Request, bufferSize)
```

### Option 2: Increase SendRequest Timeout

**File:** `provider.go:20`

```go
// Before:
const sendRequestTimeout = 5 * time.Second

// After:
const sendRequestTimeout = 30 * time.Second
```

### Option 3: Optimize Goroutine Creation

**File:** `provider.go:647-652`

```go
// Before:
if atomic.LoadInt32(&c.connCount) < c.getEffectiveMaxConnections() {
    c.sendWg.Add(1)
    go func() {
        defer c.sendWg.Done()
        _ = c.addConnection(false)
    }()
}

// After:
connCount := atomic.LoadInt32(&c.connCount)
if connCount < c.getEffectiveMaxConnections() && connCount < c.config.MaxConnections {
    // Only spawn goroutine if truly below max and not already scaling
    select {
    case c.scalingLock <- struct{}{}:  // Try to acquire scaling lock
        c.sendWg.Add(1)
        go func() {
            defer c.sendWg.Done()
            _ = c.addConnection(false)
            <-c.scalingLock  // Release scaling lock
        }()
    default:
        // Another goroutine is already scaling, skip
    }
}
```

### Option 4: Alternative "Acquire Connection" Architecture

For maximum performance, consider implementing a direct connection acquisition pattern:

```go
// New method in provider.go
func (p *Provider) AcquireConnection(ctx context.Context) (*NNTPConnection, error) {
    // Select first available connection
    for {
        select {
        case <-ctx.Done():
            return nil, ctx.Err()
        case <-p.closeCh:
            return nil, ErrProviderClosed
        default:
        }

        p.mu.RLock()
        for _, conn := range p.connections {
            if conn.tryAcquire() {
                p.mu.RUnlock()
                return conn, nil
            }
        }
        p.mu.RUnlock()

        // No available connections, wait briefly and retry
        time.Sleep(time.Millisecond)
    }
}

func (p *Provider) ReleaseConnection(conn *NNTPConnection) {
    conn.release()
}

// Usage:
conn, err := provider.AcquireConnection(ctx)
if err != nil {
    return err
}
defer provider.ReleaseConnection(conn)

// Use connection directly
response, err := conn.SendCommand(ctx, "BODY", messageID)
```

**Benefits:**
- Eliminates shared `reqCh` and multiple channel operations
- Direct connection access reduces latency
- Simpler code path with fewer goroutine hops
- Better CPU cache locality

**Tradeoffs:**
- Loses automatic connection selection
- Requires manual connection management
- No built-in queuing (blocked requests must retry)

## Benchmarking

Run the performance comparison benchmark:

```bash
go test -bench=BenchmarkThroughputComparison -benchtime=10s -benchmem
```

Run the channel overhead benchmark:

```bash
go test -bench=BenchmarkChannelOverhead -benchtime=10s
```

Test SendRequest timeout impact:

```bash
go test -v -run TestSendRequestTimeoutImpact
```

## Expected Results

### With Optimized Configuration

| Configuration | Expected Throughput | Notes |
|--------------|-------------------|-------|
| 20 conn × 1 inflight | 40-60 MB/s | Your current performance |
| 20 conn × 5 inflight | 100-150 MB/s | Moderate pipelining |
| 20 conn × 10 inflight | 150-250 MB/s | High pipelining |
| 30 conn × 10 inflight | 200-350 MB/s | Maximum parallelism |

### With Acquire/Release Pattern

| Configuration | Expected Throughput | Notes |
|--------------|-------------------|-------|
| 20 conn × 1 inflight | 60-80 MB/s | Reduced channel overhead |
| 20 conn × 5 inflight | 150-200 MB/s | Better than channel approach |
| 20 conn × 10 inflight | 200-300+ MB/s | Minimal overhead |

## Profiling Commands

### CPU Profile
```bash
go test -cpuprofile=cpu.prof -bench=BenchmarkThroughputComparison -benchtime=30s
go tool pprof -http=:8080 cpu.prof
```

### Mutex Contention Profile
```bash
go test -mutexprofile=mutex.prof -bench=BenchmarkThroughputComparison -benchtime=30s
go tool pprof -http=:8080 mutex.prof
```

### Memory Allocation Profile
```bash
go test -memprofile=mem.prof -bench=BenchmarkThroughputComparison -benchtime=30s
go tool pprof -http=:8080 mem.prof
```

Look for:
- Hot paths in channel operations (`runtime.chansend`, `runtime.chanrecv`)
- Mutex contention (`sync.(*Mutex).Lock`, `runtime.semacquire`)
- Goroutine creation overhead (`runtime.newproc`)
- Memory allocations in hot paths

## Next Steps

1. **Immediate fix:** Update your provider configuration to enable pipelining:
   ```go
   InflightPerConnection: 10
   InitialConnections: 20
   ```

2. **Quick win:** Increase request buffer size in `provider.go:194`

3. **Medium-term:** Increase `sendRequestTimeout` to 30 seconds

4. **Long-term:** Consider implementing acquire/release pattern for maximum throughput

5. **Validation:** Run benchmarks before and after changes to measure impact
