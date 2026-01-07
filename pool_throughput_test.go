package nntppool

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"net/textproto"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/javi11/nntp-server-mock/nntpserver"
	"github.com/mnightingale/rapidyenc"
)

// ThroughputResult holds the results of a throughput test
//
// Note: The MB/s measurement requires a real NNTP server with proper yenc-encoded
// articles. When using the mock server, the byte count may be 0 because the mock
// doesn't return properly yenc-encoded data that the decoder can process.
// The request/second metric is always accurate and useful for measuring pool performance.
type ThroughputResult struct {
	TotalBytes     int64
	TotalRequests  int64
	Duration       time.Duration
	MBPerSecond    float64
	RequestsPerSec float64
	Errors         int64
}

func (r ThroughputResult) String() string {
	if r.TotalBytes > 0 {
		return fmt.Sprintf(
			"Throughput: %.2f MB/s | Requests: %d | Duration: %v | Requests/sec: %.2f | Errors: %d",
			r.MBPerSecond, r.TotalRequests, r.Duration.Round(time.Millisecond), r.RequestsPerSec, r.Errors,
		)
	}
	// When using mock server without yenc, report request throughput only
	return fmt.Sprintf(
		"Requests: %d | Duration: %v | Requests/sec: %.2f | Errors: %d (use real server for MB/s)",
		r.TotalRequests, r.Duration.Round(time.Millisecond), r.RequestsPerSec, r.Errors,
	)
}

// mockServer wraps the nntpserver for easier testing
type mockServer struct {
	server   *nntpserver.Server
	backend  *nntpserver.DiskBackend
	listener net.Listener
	done     chan struct{}
	wg       sync.WaitGroup
}

func newMockServer(t testing.TB, dbPath string) *mockServer {
	t.Helper()

	backend := nntpserver.NewDiskBackend(true, dbPath)
	server := nntpserver.NewServer(backend)

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Failed to create listener: %v", err)
	}

	ms := &mockServer{
		server:   server,
		backend:  backend,
		listener: listener,
		done:     make(chan struct{}),
	}

	// Start accepting connections
	ms.wg.Add(1)
	go func() {
		defer ms.wg.Done()
		for {
			conn, err := listener.Accept()
			if err != nil {
				select {
				case <-ms.done:
					return
				default:
					continue
				}
			}
			go server.Process(conn)
		}
	}()

	return ms
}

func (ms *mockServer) Addr() net.Addr {
	return ms.listener.Addr()
}

func (ms *mockServer) Close() error {
	close(ms.done)
	err := ms.listener.Close()
	ms.wg.Wait()
	return err
}

func (ms *mockServer) AddArticle(msgID string, body []byte) error {
	header := textproto.MIMEHeader{}
	header.Set("Message-Id", msgID)
	header.Set("From", "test@throughput.test")
	header.Set("Subject", "Test Article")
	header.Set("Newsgroups", "test")
	header.Set("Date", time.Now().Format(time.RFC1123Z))

	article := &nntpserver.Article{
		Header: header,
		Body:   bytes.NewReader(body),
		Bytes:  len(body),
		Lines:  1,
	}
	return ms.backend.Post(article)
}

// TestPoolBodyThroughput measures the throughput of the Body command
func TestPoolBodyThroughput(t *testing.T) {
	tests := []struct {
		name           string
		articleSize    int // Size of each article in bytes
		articleCount   int // Number of articles to create
		concurrency    int // Number of concurrent requests
		maxConnections int // Max pool connections
		requestCount   int // Total requests to make
	}{
		{
			name:           "small_articles_low_concurrency",
			articleSize:    1024, // 1 KB
			articleCount:   100,
			concurrency:    1,
			maxConnections: 5,
			requestCount:   100,
		},
		{
			name:           "small_articles_high_concurrency",
			articleSize:    1024, // 1 KB
			articleCount:   100,
			concurrency:    10,
			maxConnections: 10,
			requestCount:   500,
		},
		{
			name:           "medium_articles_medium_concurrency",
			articleSize:    100 * 1024, // 100 KB
			articleCount:   50,
			concurrency:    5,
			maxConnections: 10,
			requestCount:   200,
		},
		{
			name:           "large_articles_high_concurrency",
			articleSize:    1024 * 1024, // 1 MB
			articleCount:   20,
			concurrency:    10,
			maxConnections: 10,
			requestCount:   100,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := runThroughputTest(t, tt.articleSize, tt.articleCount, tt.concurrency, tt.maxConnections, tt.requestCount)
			t.Logf("%s: %s", tt.name, result)
		})
	}
}

// BenchmarkPoolBodyThroughput provides Go benchmark for Body throughput
func BenchmarkPoolBodyThroughput(b *testing.B) {
	benchmarks := []struct {
		name           string
		articleSize    int
		maxConnections int
	}{
		{"1KB_1conn", 1024, 1},
		{"1KB_5conn", 1024, 5},
		{"1KB_10conn", 1024, 10},
		{"100KB_1conn", 100 * 1024, 1},
		{"100KB_5conn", 100 * 1024, 5},
		{"100KB_10conn", 100 * 1024, 10},
		{"1MB_1conn", 1024 * 1024, 1},
		{"1MB_5conn", 1024 * 1024, 5},
		{"1MB_10conn", 1024 * 1024, 10},
	}

	for _, bm := range benchmarks {
		b.Run(bm.name, func(b *testing.B) {
			ctx := context.Background()

			// Setup mock server
			server := newMockServer(b, filepath.Join(b.TempDir(), "bench.db"))
			defer server.Close()

			// Create test articles
			messageIDs := createTestArticles(b, server, bm.articleSize, 10)

			// Create pool
			host, port := parseAddr(server.Addr().String())
			pool, err := NewPool(ctx, PoolConfig{
				Providers: []ProviderConfig{{
					Host:           host,
					Port:           port,
					MaxConnections: bm.maxConnections,
					ConnectTimeout: 5 * time.Second,
					ReadTimeout:    30 * time.Second,
					WriteTimeout:   5 * time.Second,
				}},
			})
			if err != nil {
				b.Fatalf("Failed to create pool: %v", err)
			}
			defer pool.Close()

			// Select newsgroup (required by NNTP protocol before BODY commands)
			if _, err := pool.Group(ctx, "test"); err != nil {
				b.Fatalf("Failed to select group: %v", err)
			}

			// Reset timer after setup
			b.ResetTimer()

			var totalBytes int64
			for i := 0; i < b.N; i++ {
				msgID := messageIDs[i%len(messageIDs)]
				var buf bytes.Buffer
				_, err := pool.Body(ctx, msgID, &buf)
				if err != nil {
					b.Fatalf("Body failed: %v", err)
				}
				totalBytes += int64(buf.Len())
			}
			b.SetBytes(totalBytes / int64(b.N))
		})
	}
}

// BenchmarkPoolBodyThroughputParallel benchmarks parallel Body requests
func BenchmarkPoolBodyThroughputParallel(b *testing.B) {
	ctx := context.Background()

	// Setup mock server
	server := newMockServer(b, filepath.Join(b.TempDir(), "bench_parallel.db"))
	defer server.Close()

	// Create test articles (100KB each)
	articleSize := 100 * 1024
	messageIDs := createTestArticles(b, server, articleSize, 50)

	// Create pool with 10 connections
	host, port := parseAddr(server.Addr().String())
	pool, err := NewPool(ctx, PoolConfig{
		Providers: []ProviderConfig{{
			Host:           host,
			Port:           port,
			MaxConnections: 10,
			ConnectTimeout: 5 * time.Second,
			ReadTimeout:    30 * time.Second,
			WriteTimeout:   5 * time.Second,
		}},
	})
	if err != nil {
		b.Fatalf("Failed to create pool: %v", err)
	}
	defer pool.Close()

	// Select newsgroup (required by NNTP protocol before BODY commands)
	if _, err := pool.Group(ctx, "test"); err != nil {
		b.Fatalf("Failed to select group: %v", err)
	}

	b.ResetTimer()

	var totalBytes atomic.Int64
	var idx atomic.Int64
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			i := idx.Add(1)
			msgID := messageIDs[int(i)%len(messageIDs)]
			var buf bytes.Buffer
			_, err := pool.Body(ctx, msgID, &buf)
			if err != nil {
				b.Errorf("Body failed: %v", err)
			}
			totalBytes.Add(int64(buf.Len()))
		}
	})
	if b.N > 0 {
		b.SetBytes(totalBytes.Load() / int64(b.N))
	}
}

// runThroughputTest executes a throughput test with the given parameters
func runThroughputTest(t testing.TB, articleSize, articleCount, concurrency, maxConnections, requestCount int) ThroughputResult {
	t.Helper()
	ctx := context.Background()

	// Setup mock server
	server := newMockServer(t, filepath.Join(t.TempDir(), "throughput.db"))
	defer server.Close()

	// Create test articles
	messageIDs := createTestArticles(t, server, articleSize, articleCount)

	// Create pool
	host, port := parseAddr(server.Addr().String())
	pool, err := NewPool(ctx, PoolConfig{
		Providers: []ProviderConfig{{
			Host:           host,
			Port:           port,
			MaxConnections: maxConnections,
			ConnectTimeout: 5 * time.Second,
			ReadTimeout:    30 * time.Second,
			WriteTimeout:   5 * time.Second,
		}},
	})
	if err != nil {
		t.Fatalf("Failed to create pool: %v", err)
	}
	defer pool.Close()

	// Select newsgroup (required by NNTP protocol before BODY commands)
	if _, err := pool.Group(ctx, "test"); err != nil {
		t.Fatalf("Failed to select group: %v", err)
	}

	// Run throughput test
	var (
		totalBytes    atomic.Int64
		totalRequests atomic.Int64
		errors        atomic.Int64
		wg            sync.WaitGroup
	)

	// Create work channel
	work := make(chan string, requestCount)
	for i := 0; i < requestCount; i++ {
		work <- messageIDs[i%len(messageIDs)]
	}
	close(work)

	start := time.Now()

	// Start workers
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for msgID := range work {
				var buf bytes.Buffer
				_, err := pool.Body(ctx, msgID, &buf)
				if err != nil {
					errors.Add(1)
					continue
				}
				// Use actual buffer size since mock server returns plain text (not yenc)
				totalBytes.Add(int64(buf.Len()))
				totalRequests.Add(1)
			}
		}()
	}

	wg.Wait()
	duration := time.Since(start)

	bytesTotal := totalBytes.Load()
	reqTotal := totalRequests.Load()
	errTotal := errors.Load()

	mbPerSec := float64(bytesTotal) / (1024 * 1024) / duration.Seconds()
	reqPerSec := float64(reqTotal) / duration.Seconds()

	return ThroughputResult{
		TotalBytes:     bytesTotal,
		TotalRequests:  reqTotal,
		Duration:       duration,
		MBPerSecond:    mbPerSec,
		RequestsPerSec: reqPerSec,
		Errors:         errTotal,
	}
}

// createTestArticles creates test articles in the mock server and returns their message IDs
// Note: Returns message IDs WITHOUT angle brackets (Pool.Body adds them)
func createTestArticles(t testing.TB, server *mockServer, size, count int) []string {
	t.Helper()

	messageIDs := make([]string, count)
	body := generateTestBody(size)

	for i := 0; i < count; i++ {
		// Message-Id header requires angle brackets
		headerMsgID := fmt.Sprintf("<%d@throughput.test>", i)
		// But Pool.Body() adds angle brackets, so return without them
		messageIDs[i] = fmt.Sprintf("%d@throughput.test", i)

		if err := server.AddArticle(headerMsgID, body); err != nil {
			t.Fatalf("Failed to add article %d: %v", i, err)
		}
	}

	return messageIDs
}

// generateTestBody creates a yenc-encoded test body of the specified size
func generateTestBody(size int) []byte {
	// Create raw binary data
	rawData := make([]byte, size)
	for i := range rawData {
		rawData[i] = byte(i % 256)
	}

	// Encode as yenc using the proper Encoder API
	var buf bytes.Buffer
	enc, err := rapidyenc.NewEncoder(&buf, rapidyenc.Meta{
		FileName:   "test.bin",
		FileSize:   int64(size),
		PartNumber: 1,
		TotalParts: 1,
		PartSize:   int64(size),
	})
	if err != nil {
		// Fallback to plain data if encoding fails
		return rawData
	}
	if _, err := enc.Write(rawData); err != nil {
		return rawData
	}
	if err := enc.Close(); err != nil {
		return rawData
	}

	return buf.Bytes()
}

// countingWriter counts bytes written (for measuring throughput without buffering)
type countingWriter struct {
	count int64
}

func (cw *countingWriter) Write(p []byte) (n int, err error) {
	cw.count += int64(len(p))
	return len(p), nil
}

// parseAddr parses "host:port" string
func parseAddr(addr string) (string, int) {
	host, portStr, err := net.SplitHostPort(addr)
	if err != nil {
		return "127.0.0.1", 119
	}
	var port int
	fmt.Sscanf(portStr, "%d", &port)
	if host == "" {
		host = "127.0.0.1"
	}
	return host, port
}

// TestPoolBodyThroughputWithMultipleProviders tests throughput with failover
func TestPoolBodyThroughputWithMultipleProviders(t *testing.T) {
	ctx := context.Background()

	// Setup two mock servers
	server1 := newMockServer(t, filepath.Join(t.TempDir(), "server1.db"))
	defer server1.Close()

	server2 := newMockServer(t, filepath.Join(t.TempDir(), "server2.db"))
	defer server2.Close()

	// Create articles on both servers
	articleSize := 50 * 1024 // 50 KB
	messageIDs1 := createTestArticles(t, server1, articleSize, 25)
	messageIDs2 := createTestArticles(t, server2, articleSize, 25)

	// Add some shared articles to server2 (using same header format)
	body := generateTestBody(articleSize)
	for i := 0; i < 10; i++ {
		headerMsgID := fmt.Sprintf("<%d@throughput.test>", i)
		_ = server2.AddArticle(headerMsgID, body)
	}

	host1, port1 := parseAddr(server1.Addr().String())
	host2, port2 := parseAddr(server2.Addr().String())

	// Create pool with two providers
	pool, err := NewPool(ctx, PoolConfig{
		Providers: []ProviderConfig{
			{
				Name:           "primary",
				Host:           host1,
				Port:           port1,
				MaxConnections: 5,
				Priority:       0,
				ConnectTimeout: 5 * time.Second,
				ReadTimeout:    30 * time.Second,
			},
			{
				Name:           "secondary",
				Host:           host2,
				Port:           port2,
				MaxConnections: 5,
				Priority:       1,
				ConnectTimeout: 5 * time.Second,
				ReadTimeout:    30 * time.Second,
			},
		},
	})
	if err != nil {
		t.Fatalf("Failed to create pool: %v", err)
	}
	defer pool.Close()

	// Select newsgroup (required by NNTP protocol before BODY commands)
	if _, err := pool.Group(ctx, "test"); err != nil {
		t.Fatalf("Failed to select group: %v", err)
	}

	// Merge message IDs for testing
	allMessageIDs := append(messageIDs1, messageIDs2...)

	// Run concurrent requests
	var (
		totalBytes    atomic.Int64
		totalRequests atomic.Int64
		errors        atomic.Int64
		wg            sync.WaitGroup
	)

	concurrency := 10
	requestCount := 200

	work := make(chan string, requestCount)
	for i := 0; i < requestCount; i++ {
		work <- allMessageIDs[i%len(allMessageIDs)]
	}
	close(work)

	start := time.Now()

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for msgID := range work {
				var buf bytes.Buffer
				_, err := pool.Body(ctx, msgID, &buf)
				if err != nil {
					errors.Add(1)
					continue
				}
				// Use actual buffer size since mock server returns plain text (not yenc)
				totalBytes.Add(int64(buf.Len()))
				totalRequests.Add(1)
			}
		}()
	}

	wg.Wait()
	duration := time.Since(start)

	bytesTotal := totalBytes.Load()
	reqTotal := totalRequests.Load()
	errTotal := errors.Load()

	mbPerSec := float64(bytesTotal) / (1024 * 1024) / duration.Seconds()

	reqPerSec := float64(reqTotal) / duration.Seconds()
	if bytesTotal > 0 {
		t.Logf("Multi-provider throughput: %.2f MB/s | Requests: %d | Duration: %v | Requests/sec: %.2f | Errors: %d",
			mbPerSec, reqTotal, duration.Round(time.Millisecond), reqPerSec, errTotal)
	} else {
		t.Logf("Multi-provider: Requests: %d | Duration: %v | Requests/sec: %.2f | Errors: %d (use real server for MB/s)",
			reqTotal, duration.Round(time.Millisecond), reqPerSec, errTotal)
	}
}

// TestPoolBodySustainedThroughput tests sustained throughput over a longer period
func TestPoolBodySustainedThroughput(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping sustained throughput test in short mode")
	}

	ctx := context.Background()

	server := newMockServer(t, filepath.Join(t.TempDir(), "sustained.db"))
	defer server.Close()

	// Create 100 articles of 100KB each
	articleSize := 100 * 1024
	messageIDs := createTestArticles(t, server, articleSize, 100)

	host, port := parseAddr(server.Addr().String())
	pool, err := NewPool(ctx, PoolConfig{
		Providers: []ProviderConfig{{
			Host:           host,
			Port:           port,
			MaxConnections: 10,
			ConnectTimeout: 5 * time.Second,
			ReadTimeout:    30 * time.Second,
		}},
	})
	if err != nil {
		t.Fatalf("Failed to create pool: %v", err)
	}

	// Select newsgroup (required by NNTP protocol before BODY commands)
	if _, err := pool.Group(ctx, "test"); err != nil {
		t.Fatalf("Failed to select group: %v", err)
	}

	// Use a stop channel for graceful shutdown instead of context timeout
	// This prevents race conditions when closing the pool
	testDuration := 10 * time.Second
	stopCh := make(chan struct{})

	var (
		totalBytes    atomic.Int64
		totalRequests atomic.Int64
		errCount      atomic.Int64
		wg            sync.WaitGroup
	)

	concurrency := 10
	start := time.Now()

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			idx := 0
			for {
				select {
				case <-stopCh:
					return
				default:
					msgID := messageIDs[idx%len(messageIDs)]
					idx++

					var buf bytes.Buffer
					_, err := pool.Body(ctx, msgID, &buf)
					if err != nil {
						// Check if we're stopping
						select {
						case <-stopCh:
							return
						default:
						}
						errCount.Add(1)
						continue
					}
					// Use actual buffer size since mock server returns plain text (not yenc)
					totalBytes.Add(int64(buf.Len()))
					totalRequests.Add(1)
				}
			}
		}(i)
	}

	// Wait for test duration then signal stop
	time.Sleep(testDuration)
	close(stopCh)

	// Wait for all workers to finish
	wg.Wait()
	duration := time.Since(start)

	// Now it's safe to close the pool
	pool.Close()

	bytesTotal := totalBytes.Load()
	reqTotal := totalRequests.Load()
	errTotal := errCount.Load()

	mbPerSec := float64(bytesTotal) / (1024 * 1024) / duration.Seconds()
	reqPerSec := float64(reqTotal) / duration.Seconds()

	t.Logf("Sustained throughput over %v:", duration.Round(time.Millisecond))
	t.Logf("  Throughput: %.2f MB/s", mbPerSec)
	t.Logf("  Requests: %d (%.2f req/s)", reqTotal, reqPerSec)
	t.Logf("  Total data: %.2f MB", float64(bytesTotal)/(1024*1024))
	t.Logf("  Errors: %d", errTotal)
}

// TestPoolBodyThroughputWithDiscard tests throughput when discarding output (io.Discard)
func TestPoolBodyThroughputWithDiscard(t *testing.T) {
	ctx := context.Background()

	server := newMockServer(t, filepath.Join(t.TempDir(), "discard.db"))
	defer server.Close()

	// Create large articles (1MB each)
	articleSize := 1024 * 1024
	messageIDs := createTestArticles(t, server, articleSize, 10)

	host, port := parseAddr(server.Addr().String())
	pool, err := NewPool(ctx, PoolConfig{
		Providers: []ProviderConfig{{
			Host:           host,
			Port:           port,
			MaxConnections: 10,
			ConnectTimeout: 5 * time.Second,
			ReadTimeout:    60 * time.Second,
		}},
	})
	if err != nil {
		t.Fatalf("Failed to create pool: %v", err)
	}
	defer pool.Close()

	// Select newsgroup (required by NNTP protocol before BODY commands)
	if _, err := pool.Group(ctx, "test"); err != nil {
		t.Fatalf("Failed to select group: %v", err)
	}

	// Test with buffer
	t.Run("with_buffer", func(t *testing.T) {
		var totalBytes int64
		start := time.Now()

		for i := 0; i < 50; i++ {
			var buf bytes.Buffer
			_, err := pool.Body(ctx, messageIDs[i%len(messageIDs)], &buf)
			if err != nil {
				t.Fatalf("Body failed: %v", err)
			}
			// Use actual buffer size since mock server returns plain text (not yenc)
			totalBytes += int64(buf.Len())
		}

		duration := time.Since(start)
		mbPerSec := float64(totalBytes) / (1024 * 1024) / duration.Seconds()
		t.Logf("With buffer: %.2f MB/s (%.2f MB in %v)", mbPerSec, float64(totalBytes)/(1024*1024), duration.Round(time.Millisecond))
	})

	// Test with counting writer to measure io.Discard throughput
	t.Run("with_discard", func(t *testing.T) {
		var totalBytes int64
		start := time.Now()

		// Use a counting writer since io.Discard doesn't give us byte count
		cw := &countingWriter{}
		for i := 0; i < 50; i++ {
			cw.count = 0
			_, err := pool.Body(ctx, messageIDs[i%len(messageIDs)], cw)
			if err != nil {
				t.Fatalf("Body failed: %v", err)
			}
			totalBytes += cw.count
		}

		duration := time.Since(start)
		mbPerSec := float64(totalBytes) / (1024 * 1024) / duration.Seconds()
		t.Logf("With discard (counting): %.2f MB/s (%.2f MB in %v)", mbPerSec, float64(totalBytes)/(1024*1024), duration.Round(time.Millisecond))
	})
}
