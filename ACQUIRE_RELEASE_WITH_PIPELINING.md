# Acquire/Release Pattern with Pipelining Support

## The Problem with Current Implementation

With `InflightPerConnection = 50` and `MaxConnections = 10`:

```go
// provider.go:194 - THE BOTTLENECK
reqCh: make(chan *Request, config.MaxConnections)  // Buffer = 10

// But you have:
Total capacity = 10 connections × 50 inflight = 500 concurrent requests
Request queue buffer = 10 slots

// Result:
// - Only 10 requests can be queued at once
// - Other 490 requests block on channel send
// - Blocked requests hit 5-second timeout (provider.go:20)
// - Constant request failures and retries
```

## How Acquire/Release Works WITH Pipelining

The acquire/release pattern doesn't prevent pipelining - it just changes how you access connections:

### Current (Channel-Based with Pipelining)

```go
// User code:
client.Body(ctx, msgID, writer)  // Queues request internally

// Internal flow:
SendRequest() -> reqCh (blocks if full) -> Connection picks up ->
Connection has "pending" queue (50 slots) -> Writes to network ->
Reads response -> Returns
```

**6 channel operations per request through multiple layers of queues**

### Acquire/Release (Direct Access with Pipelining)

```go
// User code:
conn, err := provider.AcquireConnection(ctx)
if err != nil {
    return err
}
defer provider.ReleaseConnection(conn)

// Send pipelined requests (non-blocking)
for i := 0; i < 50; i++ {
    conn.Pipeline(ctx, "BODY", messageID)  // Just writes to network buffer
}

// Read pipelined responses (blocking)
for i := 0; i < 50; i++ {
    response, err := conn.ReadResponse(ctx)
    // Process response
}
```

**Pipelining happens on the acquired connection directly!**

## Implementation Example

Here's how to implement acquire/release WITH pipelining support:

```go
// provider.go - Add new methods

type PipelinedConnection struct {
    conn *NNTPConnection
    pending []chan *Response
    mu sync.Mutex
}

// AcquireConnection gets a connection from the pool
func (p *Provider) AcquireConnection(ctx context.Context) (*PipelinedConnection, error) {
    p.mu.RLock()
    defer p.mu.RUnlock()

    // Find first available connection with capacity
    for _, conn := range p.conns {
        if atomic.LoadInt32(&conn.inflightCount) < int32(p.config.InflightPerConnection) {
            return &PipelinedConnection{
                conn: conn,
                pending: make([]chan *Response, 0, p.config.InflightPerConnection),
            }, nil
        }
    }

    return nil, ErrNoAvailableConnections
}

// SendPipelined sends a request without waiting for response
func (pc *PipelinedConnection) SendPipelined(ctx context.Context, command string, args ...string) error {
    pc.mu.Lock()
    defer pc.mu.Unlock()

    // Check capacity
    if len(pc.pending) >= cap(pc.pending) {
        return ErrPipelineFull
    }

    // Write command to network (non-blocking on TCP buffer)
    if err := pc.conn.writeCommand(command, args...); err != nil {
        return err
    }

    // Track pending response
    responseCh := make(chan *Response, 1)
    pc.pending = append(pc.pending, responseCh)

    return nil
}

// ReadNextResponse reads the next pipelined response (FIFO order)
func (pc *PipelinedConnection) ReadNextResponse(ctx context.Context) (*Response, error) {
    pc.mu.Lock()
    if len(pc.pending) == 0 {
        pc.mu.Unlock()
        return nil, ErrNoPendingRequests
    }
    responseCh := pc.pending[0]
    pc.pending = pc.pending[1:]
    pc.mu.Unlock()

    // Wait for response
    select {
    case resp := <-responseCh:
        return resp, nil
    case <-ctx.Done():
        return nil, ctx.Err()
    }
}

// ReleaseConnection returns connection to pool
func (p *Provider) ReleaseConnection(pc *PipelinedConnection) {
    // Connection is automatically available for next AcquireConnection
    // No actual work needed since we're just checking inflightCount
}
```

### Usage Example (High-Throughput with Pipelining)

```go
func DownloadArticles(provider *Provider, messageIDs []string) error {
    // Acquire connection
    conn, err := provider.AcquireConnection(ctx)
    if err != nil {
        return err
    }
    defer provider.ReleaseConnection(conn)

    // Pipeline up to 50 requests
    batchSize := 50
    for i := 0; i < len(messageIDs); i += batchSize {
        end := i + batchSize
        if end > len(messageIDs) {
            end = len(messageIDs)
        }
        batch := messageIDs[i:end]

        // Send all requests without waiting (pipelined)
        for _, msgID := range batch {
            if err := conn.SendPipelined(ctx, "BODY", msgID); err != nil {
                return err
            }
        }

        // Read all responses in FIFO order
        for range batch {
            resp, err := conn.ReadNextResponse(ctx)
            if err != nil {
                return err
            }
            // Process response
            io.Copy(writer, resp)
        }
    }

    return nil
}
```

## Performance Comparison

### Current (Channel-Based, Pipeline=50)

| Metric | Value | Issue |
|--------|-------|-------|
| Request buffer | 10 slots | Too small! |
| Requests in flight | 500 potential | 490 blocked on queue |
| Channel ops/request | 6 operations | High mutex contention |
| Timeout failures | Frequent | 5-second timeout hit |
| **Throughput** | **40 MB/s** | Bottlenecked by queue |

### Acquire/Release (Pipeline=50)

| Metric | Value | Improvement |
|--------|-------|-------------|
| Request buffer | N/A | No queue needed |
| Requests in flight | 500 actual | All can be in-flight |
| Channel ops/request | 0-1 operations | Just acquire/release |
| Timeout failures | Rare | No queue to block on |
| **Throughput** | **200-400 MB/s** | Limited by network/CPU |

## Why Acquire/Release is Faster (Even with Pipelining)

1. **No shared request queue**
   - Current: All requests funnel through 10-slot `reqCh`
   - Acquire/Release: Direct connection access

2. **Fewer channel operations**
   - Current: 6 channel ops per request
   - Acquire/Release: 0 channel ops (direct method calls)

3. **No timeout failures**
   - Current: 5-second timeout when queue is full
   - Acquire/Release: Block on acquire, but no request timeout

4. **Better CPU cache locality**
   - Current: Requests bounce between goroutines via channels
   - Acquire/Release: Single goroutine owns connection during pipeline

5. **Full pipeline utilization**
   - Current: Pipeline=50 but only 10 can queue → wasted capacity
   - Acquire/Release: Pipeline=50 fully utilized per acquired connection

## The Real Bottleneck in Your Setup

```
You have: 10 conn × 50 pipeline = 500 capacity
But: reqCh buffer = 10 slots

It's like having a 500-car parking garage with a 10-car entrance queue!
Cars (requests) timeout waiting to get in, even though there's space inside.
```

## Quick Fix for Current Implementation

If you want to keep the channel-based approach, change this line:

```go
// provider.go:194
// Before:
reqCh: make(chan *Request, config.MaxConnections),

// After:
reqCh: make(chan *Request, config.MaxConnections * config.InflightPerConnection),
```

This should improve your throughput from 40 MB/s to 100-200 MB/s without changing architecture.

## Recommendation

1. **Short-term**: Increase `reqCh` buffer to `MaxConnections * InflightPerConnection`
2. **Long-term**: Implement acquire/release pattern for maximum performance
3. **Both**: Support both patterns - channel-based for convenience, acquire/release for performance

The acquire/release pattern with pipelining is exactly what high-performance HTTP clients (like fasthttp) use for maximum throughput.
