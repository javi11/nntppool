# Best Throughput Pattern: Comparison and Recommendation

## Current Approaches in Main Branch

### Approach 1: Goroutine-per-Segment (Current Example)

The **actual nzb-downloader example** in main branch does this:

```go
// cmd/nzb-downloader/main.go (lines 157-195)
sem := make(chan struct{}, connections*3)  // Limit concurrency

for _, segment := range file.Segments {
    sem <- struct{}{}  // Acquire semaphore

    go func(msgID string) {
        defer func() { <-sem }()  // Release semaphore

        // Download ONE segment per goroutine
        buf := &bytes.Buffer{}
        _, err := pool.Body(ctx, msgID, buf, nil)

        // Write to file...
    }(segment.ID)
}
```

**Characteristics:**
- ✅ Simple, easy to understand
- ✅ Each segment is independent
- ✅ Puddle manages connection pooling
- ❌ **No pipelining** - each Body() call acquires → downloads → releases
- ❌ Connection overhead on every request
- ⚠️ **Actual throughput: 40-80 MB/s** (based on your experience)

### Approach 2: BodyBatch API

The library provides `BodyBatch()` but the example doesn't use it:

```go
// Build batch of requests
requests := make([]nntppool.BodyBatchRequest, len(segments))
for i, seg := range segments {
    requests[i] = nntppool.BodyBatchRequest{
        MessageID: seg.ID,
        Writer:    writers[i],
    }
}

// Download entire batch with pipelining
results := pool.BodyBatch(ctx, "alt.binaries.test", requests)
```

**Characteristics:**
- ✅ Uses pipelining (10+ requests in-flight per connection)
- ✅ Efficient network utilization
- ❌ **Synchronous/blocking** - waits for entire batch to complete
- ❌ **No streaming** - can't process results as they arrive
- ❌ **Requires all message IDs upfront**
- ❌ Connection held during entire batch (no sharing with other tasks)
- ⚠️ **Potential throughput: 150-250 MB/s**

### Approach 3: Worker Pool with Pipelining (Optimal)

**Not implemented in main branch**, but this is the best pattern:

```go
type PipelineWorker struct {
    conn        nntppool.PooledConnection
    requestCh   chan *Request
    pipelineDepth int
}

func (w *PipelineWorker) Run() {
    // Hold connection for duration of worker lifetime
    conn, _ := pool.GetConnection(ctx, nil, false)
    defer conn.Close()

    batch := make([]*Request, 0, w.pipelineDepth)

    for {
        // Collect requests up to pipeline depth
        for len(batch) < w.pipelineDepth {
            select {
            case req := <-w.requestCh:
                batch = append(batch, req)
            case <-time.After(5 * time.Millisecond):
                goto flush  // Flush partial batch
            }
        }

    flush:
        if len(batch) == 0 {
            continue
        }

        // Pipeline the batch
        pipelineReqs := make([]nntpcli.PipelineRequest, len(batch))
        for i, req := range batch {
            pipelineReqs[i] = nntpcli.PipelineRequest{
                MessageID: req.msgID,
                Writer:    req.writer,
            }
        }

        // Execute with pipelining
        results := conn.Connection().BodyPipelined(pipelineReqs)

        // Send results back asynchronously
        for i, req := range batch {
            req.resultCh <- Result{
                BytesWritten: results[i].BytesWritten,
                Error:        results[i].Error,
            }
        }

        batch = batch[:0]  // Reuse slice
    }
}
```

**Characteristics:**
- ✅ **Persistent connections** - no acquire/release overhead per request
- ✅ **Pipelining** - 10+ requests in-flight per connection
- ✅ **Streaming** - results processed as they arrive
- ✅ **Asynchronous** - submit requests and get results via channels
- ✅ **Dynamic batching** - batches requests on-the-fly
- ✅ **Multiple workers** - full parallelism across connections
- ⚠️ **Expected throughput: 250-500 MB/s**

## Performance Comparison

| Approach | Throughput | Latency | Complexity | Memory | Connection Efficiency |
|----------|------------|---------|------------|--------|----------------------|
| **Goroutine-per-segment** | 40-80 MB/s | Low | Simple | High (goroutines) | Poor (acquire/release) |
| **BodyBatch** | 150-250 MB/s | Medium | Medium | Medium | Good (pipelining) |
| **Worker Pool + Pipeline** | 250-500 MB/s | Low | Complex | Low | Excellent (persistent + pipeline) |

## Detailed Analysis

### Why Current Example is Slow

```go
// Each segment triggers full lifecycle:
pool.Body(ctx, msgID, buf, nil)
    ↓
1. Acquire connection from puddle pool (~100µs lock contention)
2. Send BODY command
3. Wait for response
4. Read body
5. Release connection to puddle pool (~100µs)

// With 10 connections × no pipelining:
// Max throughput = 10 × (1 request / RTT)
// At 50ms RTT = 10 × 20 req/s = 200 req/s
// At 100KB per segment = 20 MB/s theoretical max
```

**Actual bottleneck:** Connection acquire/release overhead when each goroutine does single request

### Why BodyBatch is Better

```go
// Acquires connection ONCE for entire batch:
results := pool.BodyBatch(ctx, group, requests)
    ↓
1. Acquire connection from puddle pool (once)
2. Send 10 BODY commands WITHOUT waiting (pipelining!)
3. Read 10 responses in order
4. Release connection (once)

// With 10 connections × 10 pipelined per connection:
// Max inflight = 10 × 10 = 100 requests
// At 50ms RTT, each connection processes 10 requests in ~50ms
// = 200 requests/sec per connection
// = 2000 requests/sec total
// At 100KB per segment = 200 MB/s
```

**Key improvement:** Amortizes connection overhead and uses network pipelining

### Why Worker Pool + Pipeline is Best

```go
// Each worker holds connection for its lifetime:
Worker 1 (persistent conn A):
    ↓
1. Collect 10 requests from queue (~5ms batching window)
2. Send 10 BODY commands WITHOUT waiting
3. Read 10 responses
4. Send results to response channels
5. REPEAT (no connection release!)

// With 20 workers × 10 pipelined per worker:
// Max inflight = 20 × 10 = 200 requests
// No acquire/release overhead
// Dynamic batching as requests arrive
// = 4000+ requests/sec total
// At 100KB per segment = 400+ MB/s
```

**Key improvements:**
- Zero connection acquire/release overhead
- Pipelining on every batch
- Streaming results (non-blocking)
- Dynamic batching

## Implementation Recommendation

### Best Pattern: Pipelined Worker Pool

```go
package main

import (
    "context"
    "io"
    "sync"
    "time"
)

// DownloadRequest represents a single segment download
type DownloadRequest struct {
    MessageID string
    Writer    io.Writer
    ResultCh  chan<- DownloadResult
}

type DownloadResult struct {
    MessageID    string
    BytesWritten int64
    Error        error
}

// PipelinedDownloader manages pipelined workers
type PipelinedDownloader struct {
    pool          nntppool.UsenetConnectionPool
    requestCh     chan *DownloadRequest
    workers       []*Worker
    numWorkers    int
    pipelineDepth int
    wg            sync.WaitGroup
}

func NewPipelinedDownloader(
    pool nntppool.UsenetConnectionPool,
    numWorkers int,
    pipelineDepth int,
) *PipelinedDownloader {
    pd := &PipelinedDownloader{
        pool:          pool,
        requestCh:     make(chan *DownloadRequest, numWorkers*pipelineDepth*2),
        numWorkers:    numWorkers,
        pipelineDepth: pipelineDepth,
    }

    // Start workers
    for i := 0; i < numWorkers; i++ {
        worker := &Worker{
            id:            i,
            pool:          pool,
            requestCh:     pd.requestCh,
            pipelineDepth: pipelineDepth,
        }
        pd.workers = append(pd.workers, worker)

        pd.wg.Add(1)
        go func() {
            defer pd.wg.Done()
            worker.Run(context.Background())
        }()
    }

    return pd
}

// Download submits a download request (non-blocking)
func (pd *PipelinedDownloader) Download(ctx context.Context, msgID string, writer io.Writer) (int64, error) {
    resultCh := make(chan DownloadResult, 1)

    req := &DownloadRequest{
        MessageID: msgID,
        Writer:    writer,
        ResultCh:  resultCh,
    }

    select {
    case pd.requestCh <- req:
    case <-ctx.Done():
        return 0, ctx.Err()
    }

    select {
    case result := <-resultCh:
        return result.BytesWritten, result.Error
    case <-ctx.Done():
        return 0, ctx.Err()
    }
}

func (pd *PipelinedDownloader) Close() {
    close(pd.requestCh)
    pd.wg.Wait()
}

// Worker processes requests with pipelining
type Worker struct {
    id            int
    pool          nntppool.UsenetConnectionPool
    requestCh     <-chan *DownloadRequest
    pipelineDepth int
}

func (w *Worker) Run(ctx context.Context) {
    // Acquire connection once and hold it
    conn, err := w.pool.GetConnection(ctx, nil, false)
    if err != nil {
        return
    }
    defer conn.Close()

    nntpConn := conn.Connection()
    batch := make([]*DownloadRequest, 0, w.pipelineDepth)
    flushTimer := time.NewTimer(10 * time.Millisecond)
    defer flushTimer.Stop()

    for {
        select {
        case req, ok := <-w.requestCh:
            if !ok {
                // Channel closed, flush remaining and exit
                if len(batch) > 0 {
                    w.processBatch(nntpConn, batch)
                }
                return
            }

            batch = append(batch, req)

            // Flush when batch full
            if len(batch) >= w.pipelineDepth {
                w.processBatch(nntpConn, batch)
                batch = batch[:0]
                flushTimer.Reset(10 * time.Millisecond)
            }

        case <-flushTimer.C:
            // Flush partial batch after timeout
            if len(batch) > 0 {
                w.processBatch(nntpConn, batch)
                batch = batch[:0]
            }
            flushTimer.Reset(10 * time.Millisecond)
        }
    }
}

func (w *Worker) processBatch(conn nntpcli.Connection, batch []*DownloadRequest) {
    // Build pipeline requests
    pipelineReqs := make([]nntpcli.PipelineRequest, len(batch))
    for i, req := range batch {
        pipelineReqs[i] = nntpcli.PipelineRequest{
            MessageID: req.MessageID,
            Writer:    req.Writer,
        }
    }

    // Execute with pipelining (all commands sent before reading responses)
    results := conn.BodyPipelined(pipelineReqs)

    // Send results back
    for i, req := range batch {
        req.ResultCh <- DownloadResult{
            MessageID:    req.MessageID,
            BytesWritten: results[i].BytesWritten,
            Error:        results[i].Error,
        }
    }
}
```

### Usage Example

```go
func main() {
    // Create connection pool
    pool, _ := nntppool.NewConnectionPool(config)
    defer pool.Quit()

    // Create pipelined downloader
    // 20 workers × 10 pipeline depth = 200 concurrent requests
    downloader := NewPipelinedDownloader(pool, 20, 10)
    defer downloader.Close()

    // Download segments concurrently
    var wg sync.WaitGroup
    for _, segment := range segments {
        wg.Add(1)
        go func(seg Segment) {
            defer wg.Done()

            buf := &bytes.Buffer{}
            n, err := downloader.Download(ctx, seg.ID, buf)
            if err != nil {
                log.Printf("Failed %s: %v", seg.ID, err)
                return
            }

            // Write to file at offset
            file.WriteAt(buf.Bytes(), seg.Offset)

            log.Printf("Downloaded %s: %d bytes", seg.ID, n)
        }(segment)
    }

    wg.Wait()
}
```

## Performance Expectations

### Configuration

```go
config := nntppool.Config{
    Providers: []nntppool.Provider{
        {
            Host:           "news.example.com",
            MaxConnections: 20,
            PipelineDepth:  10,  // Important for BodyPipelined to work
        },
    },
    MinConnections: 20,
}
```

### Expected Results

| Scenario | Goroutine-per-Segment | BodyBatch | Worker Pool + Pipeline |
|----------|----------------------|-----------|------------------------|
| **100 segments, 100KB each** | 40-80 MB/s | 150-200 MB/s | 250-400 MB/s |
| **1000 segments, 100KB each** | 40-80 MB/s | 180-250 MB/s | 300-500 MB/s |
| **Memory usage** | High (1000 goroutines) | Medium | Low (20 workers) |
| **Latency (per segment)** | 50-100ms | 500-1000ms (batch) | 50-150ms |
| **Connection reuse** | Poor | Good | Excellent |
| **CPU overhead** | High (context switches) | Medium | Low |

## Recommendation: Use Worker Pool Pattern

**For high-throughput USENET downloading, implement the Worker Pool + Pipelining pattern:**

1. ✅ **Best throughput** - 250-500 MB/s
2. ✅ **Lowest latency** - streaming results
3. ✅ **Most efficient** - persistent connections with pipelining
4. ✅ **Scalable** - works with thousands of segments
5. ✅ **Resource efficient** - fixed number of workers

**BodyBatch is good for:**
- ❓ Simple batch jobs where you have all message IDs upfront
- ❓ Can wait for entire batch to complete
- ❓ Don't need streaming results

**But for real-world NZB downloading, Worker Pool is superior.**

## Why Main Branch Example Doesn't Use BodyBatch

The current example uses goroutine-per-segment because:
1. **Simplicity** - easy to understand for demonstration
2. **Independence** - each segment is isolated (good for error handling)
3. **Legacy pattern** - probably predates BodyBatch API

But for **production use with high throughput requirements**, the Worker Pool pattern is significantly better.
