# nntppool

A high-performance NNTP connection pool library for Go. It manages multiple NNTP provider connections with pipelining, automatic failover, backup providers, and yEnc decoding — designed for usenet download applications that need maximum throughput across many providers.

## Table of Contents

- [Key Features](#key-features)
- [Tech Stack](#tech-stack)
- [Prerequisites](#prerequisites)
- [Getting Started](#getting-started)
- [Architecture Overview](#architecture-overview)
- [API Reference](#api-reference)
- [Configuration Reference](#configuration-reference)
- [Testing](#testing)
- [Speed Test Tool](#speed-test-tool)
- [Troubleshooting](#troubleshooting)
- [Contributing](#contributing)

## Key Features

- Multi-provider connection pooling with configurable connection counts per provider
- NNTP command pipelining (configurable inflight requests per connection)
- Weighted round-robin and FIFO dispatch strategies across providers
- Automatic failover to backup providers on article-not-found (430)
- Same-host deduplication: a 430 from one account on a host skips other accounts on the same host
- Provider removal on permanent failure (502 service unavailable)
- yEnc decoding with CRC32 validation using the rapidyenc library
- UU encoding detection and handling
- Streaming body delivery to an `io.Writer` without buffering
- Priority channel for urgent requests that preempt normal queue
- Idle timeout for connection teardown under light load
- Per-provider stats (bytes consumed, missing articles, errors, ping RTT)
- Built-in speed test using NZB files

## Tech Stack

- **Language**: Go 1.25+
- **Module**: `github.com/javi11/nntppool/v4`
- **Key dependency**: `github.com/mnightingale/rapidyenc` — native yEnc decoder
- **Metrics**: atomic counters, no external monitoring framework required
- **Test tooling**: standard `testing` package, `go-junit-report`, `golangci-lint`

## Prerequisites

- Go 1.25 or later (uses `go tool` for linting and testing utilities)
- Access to one or more NNTP provider accounts for integration testing against real servers
- No additional system packages required — rapidyenc is a pure-Go-compatible library

## Getting Started

### 1. Add the dependency

```bash
go get github.com/javi11/nntppool/v4
```

### 2. Basic usage — single provider

```go
package main

import (
    "context"
    "fmt"
    "log"

    "github.com/javi11/nntppool/v4"
)

func main() {
    ctx := context.Background()

    providers := []nntppool.Provider{
        {
            Host:        "news.example.com:563",
            TLSConfig:   &tls.Config{ServerName: "news.example.com"},
            Auth:        nntppool.Auth{Username: "user", Password: "pass"},
            Connections: 20,
            Inflight:    4,
        },
    }

    client, err := nntppool.NewClient(ctx, providers)
    if err != nil {
        log.Fatal(err)
    }
    defer client.Close()

    // Fetch an article body (buffered)
    body, err := client.Body(ctx, "some-message-id@example.com")
    if err != nil {
        log.Fatal(err)
    }
    fmt.Printf("Downloaded %d bytes (encoding: %v, CRC valid: %v)\n",
        body.BytesDecoded, body.Encoding, body.CRCValid)
}
```

### 3. Multiple providers with backup

```go
providers := []nntppool.Provider{
    {
        Host:        "news.provider1.com:563",
        Auth:        nntppool.Auth{Username: "u1", Password: "p1"},
        Connections: 30,
        Inflight:    4,
    },
    {
        Host:        "news.provider2.com:119",
        Auth:        nntppool.Auth{Username: "u2", Password: "p2"},
        Connections: 20,
        Inflight:    2,
        Backup:      true, // only used when main providers return 430
    },
}

client, err := nntppool.NewClient(ctx, providers,
    nntppool.WithDispatchStrategy(nntppool.DispatchRoundRobin),
)
```

### 4. Streaming body to a writer

```go
var buf bytes.Buffer
body, err := client.BodyStream(ctx, "message-id@example.com", &buf)
if err != nil {
    log.Fatal(err)
}
// body.Bytes is nil; buf holds the decoded bytes
// body.YEnc contains metadata (filename, size, part info)
fmt.Printf("File: %s, Part: %d/%d\n", body.YEnc.FileName, body.YEnc.Part, body.YEnc.Total)
```

### 5. Check article existence

```go
stat, err := client.Stat(ctx, "message-id@example.com")
if errors.Is(err, nntppool.ErrArticleNotFound) {
    fmt.Println("article missing")
} else if err != nil {
    log.Fatal(err)
}
```

### 6. Fetch headers

```go
head, err := client.Head(ctx, "message-id@example.com")
if err != nil {
    log.Fatal(err)
}
fmt.Println(head.Headers["Subject"])
```

### 7. Post an article

```go
headers := nntppool.PostHeaders{
    From:       "poster@example.com",
    Subject:    "Test post [1/1]",
    Newsgroups: []string{"alt.test"},
    MessageID:  "<unique-id@example.com>",
}
meta := rapidyenc.Meta{
    Filename: "test.bin",
    Size:     int64(len(data)),
}
result, err := client.PostYenc(ctx, headers, bytes.NewReader(data), meta)
if err != nil {
    log.Fatal(err)
}
fmt.Printf("Posted: %d %s\n", result.StatusCode, result.Status)
```

---

## Architecture Overview

### Connection Lifecycle

Each provider is represented by a `providerGroup`, which owns:

1. `**reqCh**` — buffered channel (capacity = `Connections`) for normal requests
2. `**prioCh**` — buffered channel for priority requests (`SendPriority`)
3. `**hotReqCh` / `hotPrioCh**` — unbuffered channels; only connected (hot) connections listen here

When a request arrives at `Send()`:

```
Send() → doSendWithRetry() → round-robin/FIFO pick provider
    → try hotReqCh (non-blocking, zero-copy if connection idle)
    → fall back to reqCh (wakes a cold slot or queues behind in-flight)
```

Each connection slot runs as a goroutine pair:

- `**writeLoop**`: reads from `pending`, writes NNTP commands to the TCP connection, handles POST two-phase handshake
- `**readerLoop**`: reads NNTP responses via `readBuffer.feedUntilDone()`, decodes yEnc/UU, delivers to `Response.RespCh`

Both loops share a `pending` channel with capacity = `Inflight`, enforcing the pipelining depth.

### Request Dispatch

**Round-Robin (default)**: Uses dynamic weighted round-robin where weight = available inflight slots per provider. A provider with 10 connections contributes proportionally more than one with 2. The `nextIdx` atomic counter selects the start point using a cumulative-weight binary search.

**FIFO**: Scans providers in order and sends to the first one with available capacity. Under light load this keeps traffic concentrated on the primary provider, minimizing unnecessary connections.

### Failover and Retry Logic

```
Attempt main providers (round-robin or FIFO):
  → 2xx: success, return response
  → 430/423: article not found — try next provider (skip same-host duplicates)
  → 502: provider permanently unavailable — remove from pool, continue
  → connection error: try next provider

If all mains return 430: attempt backup providers
If all providers exhausted: return last error
```

### Read Buffer

The `readBuffer` starts at 128KB and grows in doublings up to 8MB when large yEnc segments require it. It caches the last `SetReadDeadline` call to avoid redundant syscalls on every read.

### yEnc Decoding

The parser in `reader.go` feeds raw NNTP response bytes incrementally through `NNTPResponse.Feed()`:

1. Reads the status line (e.g., `222 0 <id> body`)
2. Detects encoding format from the first data lines (`=ybegin`, `begin` , or UU line heuristics)
3. For yEnc: delegates to `rapidyenc.DecodeIncremental()` for SIMD-accelerated decoding
4. Accumulates CRC32 of decoded bytes, compares against `=yend pcrc32=` / `crc32=`
5. Fires the optional `onMeta` callback once `=ybegin`/`=ypart` headers are fully parsed

### Hot/Cold Connection Model

Connections are lazy: a cold slot only dials when a request actually arrives. Once connected, it registers on `hotReqCh` so subsequent requests can be dispatched without going through the buffered channel. A cold slot wakes when `reqCh` receives a request that overflows the current hot connections' inflight capacity.

---

## API Reference

### Creating a client

```go
func NewClient(ctx context.Context, providers []Provider, opts ...ClientOption) (*Client, error)
```

Returns an error if providers is empty or contains duplicate names. Provider names default to `"host:port"` or a monotonic integer for factory-based providers.

### Reading articles

| Method                                     | Description                               |
| ------------------------------------------ | ----------------------------------------- |
| `Body(ctx, messageID, onMeta...)`          | Fetch and decode body into memory         |
| `BodyStream(ctx, messageID, w, onMeta...)` | Decode and stream to `io.Writer`          |
| `BodyAsync(ctx, messageID, w, onMeta...)`  | Non-blocking; returns `<-chan BodyResult` |
| `BodyPriority(ctx, messageID, onMeta...)`  | Like `Body` but uses the priority queue   |
| `Head(ctx, messageID)`                     | Fetch RFC 5322 headers                    |
| `Stat(ctx, messageID)`                     | Check existence without transferring body |

### Low-level send

```go
// Send dispatches a raw NNTP command and returns a channel for the response.
func (c *Client) Send(ctx context.Context, payload []byte, bodyWriter io.Writer, onMeta ...func(YEncMeta)) <-chan Response

// SendPriority is like Send but uses the priority queue.
func (c *Client) SendPriority(ctx context.Context, payload []byte, bodyWriter io.Writer, onMeta ...func(YEncMeta)) <-chan Response
```

Both return immediately; the caller receives exactly one `Response` from the channel.

### Posting

```go
func (c *Client) PostYenc(ctx context.Context, headers PostHeaders, body io.Reader, meta rapidyenc.Meta) (*PostResult, error)
```

Encodes the body as yEnc on the fly and sends using the two-phase POST protocol (sends `POST`, waits for `340`, then streams the article). Uses the same dispatch strategy as normal requests so concurrent POSTs are spread across providers.

### Statistics

```go
func (c *Client) Stats() ClientStats
```

Returns a snapshot of per-provider metrics (bytes consumed, missing articles, errors, ping RTT, active/max connections) and aggregate totals.

### Lifecycle

```go
func (c *Client) Close() error        // cancel context, wait for goroutines
func (c *Client) NumProviders() int   // number of active providers (main + backup)
```

### Key types

```go
type ArticleBody struct {
    MessageID     string
    Bytes         []byte          // nil when BodyStream was used
    BytesDecoded  int
    BytesConsumed int             // wire bytes (pre-decode)
    Encoding      ArticleEncoding // EncodingYEnc, EncodingUU, EncodingUnknown
    YEnc          YEncMeta        // filename, filesize, part info
    CRC           uint32
    ExpectedCRC   uint32
    CRCValid      bool
}

type YEncMeta struct {
    FileName  string
    FileSize  int64
    Part      int64
    PartBegin int64
    PartSize  int64
    Total     int64
}

type PostHeaders struct {
    From       string
    Subject    string
    Newsgroups []string
    MessageID  string
    Extra      map[string][]string // additional RFC 5322 headers
}
```

---

## Configuration Reference

### Provider fields

| Field             | Type            | Default      | Description                                                    |
| ----------------- | --------------- | ------------ | -------------------------------------------------------------- |
| `Host`            | `string`        | —            | `host:port`, e.g. `news.example.com:563`                       |
| `TLSConfig`       | `*tls.Config`   | nil (no TLS) | Pass a config to enable TLS                                    |
| `Auth`            | `Auth`          | —            | `Username` and `Password` for AUTHINFO                         |
| `Connections`     | `int`           | —            | Number of connection slots for this provider                   |
| `Inflight`        | `int`           | 1            | Max pipelined requests per connection                          |
| `Factory`         | `ConnFactory`   | nil          | Custom dialer, overrides `Host`/`TLSConfig`                    |
| `Backup`          | `bool`          | false        | Only used when all main providers return 430                   |
| `SkipPing`        | `bool`          | false        | Skip the startup DATE ping (for servers that don't support it) |
| `IdleTimeout`     | `time.Duration` | 0 (disabled) | Disconnect idle connections after this duration                |
| `ThrottleRestore` | `time.Duration` | 30s          | Wait before restoring throttled slots after 502                |
| `KeepAlive`       | `time.Duration` | 30s          | TCP keep-alive interval; negative disables                     |

### Client options

```go
// Set dispatch strategy (default: DispatchRoundRobin)
nntppool.WithDispatchStrategy(nntppool.DispatchFIFO)
nntppool.WithDispatchStrategy(nntppool.DispatchRoundRobin)
```

### Dispatch strategies

| Strategy             | Behavior                                                                                                                                               |
| -------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `DispatchRoundRobin` | Weighted by available inflight capacity. Providers with more free slots receive proportionally more requests.                                          |
| `DispatchFIFO`       | First provider with available capacity gets the request. Under light load this keeps one provider "warm" and avoids unnecessary connections on others. |

### Sentinel errors

| Error                    | Meaning                                                   |
| ------------------------ | --------------------------------------------------------- |
| `ErrArticleNotFound`     | NNTP 430 or 423 — article does not exist on this provider |
| `ErrPostingNotPermitted` | NNTP 440 — server does not allow posting                  |
| `ErrPostingFailed`       | NNTP 441 — posting was rejected                           |
| `ErrAuthRequired`        | NNTP 480                                                  |
| `ErrAuthRejected`        | NNTP 481                                                  |
| `ErrServiceUnavailable`  | NNTP 502 — provider removed from pool                     |
| `ErrCRCMismatch`         | yEnc CRC32 check failed (body returned alongside error)   |
| `ErrMaxConnections`      | Server reported max connections reached during handshake  |
| `ErrConnectionDied`      | TCP connection closed unexpectedly                        |
| `ErrProtocolDesync`      | Binary data received where a status line was expected     |

`ErrArticleNotFound` uses semantic category matching: both 430 and 423 satisfy `errors.Is(err, ErrArticleNotFound)`.

---

## Testing

### Running tests

```bash
# All tests
go test ./...

# With race detector (recommended before committing)
go test -race ./...

# Specific package
go test ./nzb/...

# Specific test
go test -run TestClient_SendRetryRoundRobin ./...

# With verbose output
go test -v ./...

# Coverage
go test -coverprofile=coverage.out ./...
go tool cover -html=coverage.out
```

### Benchmarks

```bash
go test -bench=. -benchmem ./...

# Specific benchmark
go test -bench=BenchmarkSend -benchmem -benchtime=5s ./...
```

Built-in benchmarks cover:

- Equal-weight two-provider round-robin (3+3 connections)
- Weighted two-provider round-robin (5+1 connections)
- Single-provider baseline

---

## Speed Test Tool

`cmd/speedtest` is a command-line tool that measures download speed using an NZB file through the pool.

### Build

```bash
go build ./cmd/speedtest
```

### Usage — single provider (legacy flags)

```bash
./speedtest \
    --host news.example.com:563 \
    --tls \
    --user myuser \
    --pass mypassword \
    --conns 20 \
    --inflight 4
```

### Usage — multiple providers

```bash
./speedtest \
    --provider "host=news.provider1.com:563,tls,user=u1,pass=p1,conns=20,inflight=4" \
    --provider "host=news.provider2.com:119,user=u2,pass=p2,conns=10,inflight=2,backup"
```

### Provider flag options

| Option      | Example                     | Description                                    |
| ----------- | --------------------------- | ---------------------------------------------- |
| `host`      | `host=news.example.com:563` | Server host and port (required)                |
| `tls`       | `tls` or `tls=true`         | Enable TLS with SNI from host                  |
| `user`      | `user=myuser`               | NNTP username                                  |
| `pass`      | `pass=mypassword`           | NNTP password                                  |
| `conns`     | `conns=20`                  | Number of connections (default: 10)            |
| `inflight`  | `inflight=4`                | Pipelined requests per connection (default: 1) |
| `backup`    | `backup`                    | Mark as backup provider                        |
| `idle`      | `idle=30s`                  | Idle disconnect timeout                        |
| `throttle`  | `throttle=30s`              | Throttle restore duration                      |
| `keepalive` | `keepalive=60s`             | TCP keep-alive interval                        |

### Other flags

| Flag              | Default               | Description                           |
| ----------------- | --------------------- | ------------------------------------- |
| `--nzb`           | SABnzbd 10GB test NZB | Path or URL to NZB file               |
| `--max-segments`  | 0 (all)               | Limit number of segments              |
| `--provider-name` | (all)                 | Test only a specific provider by name |

### Example output

```
Provider 1: news.example.com:563 (TLS: yes, conns: 20, inflight: 4, idle: none, keepalive: 1m0s, main)
Creating client with 20 connection slots across 1 provider(s)...
Ready (connections on demand).

[ 15.3s]  450/1250 segs | 142.3 MB/s (avg 138.7 MB/s) | ETA 28s

=== Speed Test Results ===
Time:       45.123s
Segments:   1250 done, 0 missing, 0 errors
Wire:       1024.00 MB (22.70 MB/s)
Decoded:    981.44 MB (21.76 MB/s)
```

---

## Troubleshooting

### Connection refused or timeout

**Symptom:** `NewClient` hangs or connections fail immediately.

**Check:**

- Verify `host:port` is reachable: `nc -zv news.example.com 563`
- Confirm TLS settings match the port (563 = TLS, 119 = plain)
- For servers that reject the initial DATE ping, set `SkipPing: true`

### Authentication rejected (481)

**Symptom:** `ErrAuthRejected` on every request.

**Check:**

- Credentials are correct
- Some providers require the username in `user@domain.com` format
- Username/password must not contain commas if using the speedtest CLI `--provider` flag (use individual flags instead)

### All articles return 430

**Symptom:** `ErrArticleNotFound` for known-good articles.

**Check:**

- The provider may not carry the newsgroup or the retention period has passed
- Add a backup provider that carries the content: set `Backup: true`
- Verify the message ID format (should include angle brackets in the `Send` payload but not in `Body`/`Head`/`Stat` calls — the API adds them automatically)

### Max connections errors (502/400)

**Symptom:** Connections are throttled immediately; `ErrMaxConnections` during connect.

**Behaviour:** The pool automatically reduces active slots and restores them after `ThrottleRestore` (default 30 seconds). To adjust:

```go
nntppool.Provider{
    ThrottleRestore: 60 * time.Second,
}
```

### Provider removed from pool

**Symptom:** `NumProviders()` decreases; `ErrServiceUnavailable` returned.

**Cause:** A connection returned 502 at the command level (not just during handshake). This signals permanent unavailability. The provider is removed from main or backup groups atomically. To restart, create a new `Client`.

### CRC mismatch on decoded bodies

**Symptom:** `ErrCRCMismatch` returned alongside a non-nil `*ArticleBody`.

**Behaviour:** The body is returned even on CRC mismatch so callers can decide whether to discard or use the data. The `ArticleBody.CRCValid` field is `false` and `CRC != ExpectedCRC`.

### Race conditions in tests

**Symptom:** Test failures that only appear with `-race`.

**Fix:** Ensure any shared state accessed from the mock server goroutine is protected by a mutex. See `TestClient_SendRetryRoundRobin` for the correct pattern.

### Linter errors (`errcheck`)

**Symptom:** `golangci-lint` fails with unchecked error returns.

**Fix:** `io.PipeWriter.CloseWithError` and `io.PipeReader.Close` return values must be handled. Use the blank identifier explicitly:

```go
defer func() { _ = pw.CloseWithError(err) }()
_ = pr.Close()
```

---

## Contributing

In brief:

1. Create a topic branch
2. Add tests for your change (aim for 100% coverage on new code)
3. Run `make check` — this runs generate, tidy, lint, and the race-detector test suite
4. Open a pull request

The pre-commit hook (`make git-hooks`) runs the full `make` pipeline automatically.

## License

See [LICENSE](LICENSE).
