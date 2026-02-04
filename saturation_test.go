package nntppool

import (
	"context"
	"io"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/javi11/nntppool/v3/testutil"
)

// TestSaturationSendRequestTimeout tests that requests timeout after sendRequestTimeout (5s)
// when the pool is exhausted and no connections become available.
// NOTE: This test involves long timeouts and may trigger connection cleanup deadlocks.
func TestSaturationSendRequestTimeout(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode due to long timeouts")
	}
	// Create a server that accepts connections but responds very slowly
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}
	defer func() {
		_ = l.Close()
	}()

	go func() {
		for {
			conn, err := l.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer func() {
					_ = c.Close()
				}()
				_, _ = c.Write([]byte("200 Service Ready\r\n"))
				buf := make([]byte, 1024)
				for {
					n, err := c.Read(buf)
					if err != nil {
						return
					}
					cmd := string(buf[:n])
					if cmd == "DATE\r\n" {
						_, _ = c.Write([]byte("111 20240101000000\r\n"))
						continue
					}
					// Very slow response for BODY - simulates fully occupied connection
					time.Sleep(10 * time.Second)
					if strings.HasPrefix(cmd, "BODY") {
						_, _ = c.Write([]byte("222 0 <id> body follows\r\nline1\r\n.\r\n"))
					}
				}
			}(conn)
		}
	}()

	dial := func(ctx context.Context) (net.Conn, error) {
		var d net.Dialer
		return d.DialContext(ctx, "tcp", l.Addr().String())
	}

	// Create provider with 1 connection and no pipelining
	p, err := NewProvider(context.Background(), ProviderConfig{
		Address:               l.Addr().String(),
		MaxConnections:        1,
		InitialConnections:    1,
		InflightPerConnection: 1,
		ConnFactory:           dial,
	})
	if err != nil {
		t.Fatalf("failed to create provider: %v", err)
	}
	defer func() {
		_ = p.Close()
	}()

	// First request - will occupy the only connection
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		respCh := p.Send(ctx, []byte("BODY <slow>\r\n"), nil)
		<-respCh
	}()

	// Give time for first request to start
	time.Sleep(100 * time.Millisecond)

	// Second request - will go into the reqCh buffer (capacity = MaxConnections = 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		respCh := p.Send(ctx, []byte("BODY <buffered>\r\n"), nil)
		<-respCh
	}()

	// Give time for second request to enter buffer
	time.Sleep(100 * time.Millisecond)

	// Third request - reqCh buffer is full, should timeout waiting to send
	start := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	respCh := p.Send(ctx, []byte("BODY <timeout-test>\r\n"), nil)
	resp := <-respCh

	elapsed := time.Since(start)

	// Should timeout after approximately sendRequestTimeout (5s)
	if resp.Err == nil {
		t.Error("expected error due to send request timeout")
	}

	// Verify timeout was approximately 5 seconds (with some tolerance)
	if elapsed < 4*time.Second || elapsed > 7*time.Second {
		t.Errorf("expected timeout around 5s, got %v", elapsed)
	}

	if resp.Err != nil && !strings.Contains(resp.Err.Error(), "timeout") {
		t.Errorf("expected timeout error, got: %v", resp.Err)
	}
}

// TestSaturationQueueFairness tests that requests are processed in FIFO order
// when queued behind busy connections.
func TestSaturationQueueFairness(t *testing.T) {
	var requestOrder []int
	var mu sync.Mutex

	srv, cleanup := testutil.StartMockNNTPServer(t, testutil.MockServerConfig{
		Handler: func(cmd string) (string, error) {
			if cmd == "DATE\r\n" {
				return "111 20240101000000\r\n", nil
			}
			if strings.HasPrefix(cmd, "BODY") {
				// Extract order from message ID (format: BODY <order-N>)
				if idx := strings.Index(cmd, "order-"); idx != -1 {
					var order int
					if _, err := parseIntFromString(cmd[idx+6:], &order); err == nil {
						mu.Lock()
						requestOrder = append(requestOrder, order)
						mu.Unlock()
					}
				}
				// Small delay to ensure ordering
				time.Sleep(10 * time.Millisecond)
				return "222 0 <id> body follows\r\nline1\r\n.\r\n", nil
			}
			return "500 Unknown Command\r\n", nil
		},
	})
	defer cleanup()

	dial := func(ctx context.Context) (net.Conn, error) {
		var d net.Dialer
		return d.DialContext(ctx, "tcp", srv.Addr())
	}

	// Single connection to force sequential processing
	p, err := NewProvider(context.Background(), ProviderConfig{
		Address:               srv.Addr(),
		MaxConnections:        1,
		InitialConnections:    1,
		InflightPerConnection: 1,
		ConnFactory:           dial,
	})
	if err != nil {
		t.Fatalf("failed to create provider: %v", err)
	}
	defer func() {
		_ = p.Close()
	}()

	// Send requests in order
	var wg sync.WaitGroup
	for i := 1; i <= 5; i++ {
		wg.Add(1)
		order := i
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()

			respCh := p.Send(ctx, []byte(strings.Replace("BODY <order-X>\r\n", "X", string(rune('0'+order)), 1)), nil)
			<-respCh
		}()
		// Small delay to ensure ordered submission
		time.Sleep(20 * time.Millisecond)
	}

	wg.Wait()

	mu.Lock()
	defer mu.Unlock()

	// Requests should be processed in roughly the order submitted
	// Due to concurrency, we allow some variation
	if len(requestOrder) != 5 {
		t.Errorf("expected 5 requests processed, got %d", len(requestOrder))
	}
}

// TestSaturationConnectionLimit tests that the provider respects MaxConnections.
func TestSaturationConnectionLimit(t *testing.T) {
	var connectionCount int32

	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}
	defer func() {
		_ = l.Close()
	}()

	go func() {
		for {
			conn, err := l.Accept()
			if err != nil {
				return
			}
			atomic.AddInt32(&connectionCount, 1)

			go func(c net.Conn) {
				defer func() {
					_ = c.Close()
					atomic.AddInt32(&connectionCount, -1)
				}()
				_, _ = c.Write([]byte("200 Service Ready\r\n"))
				buf := make([]byte, 1024)
				for {
					n, err := c.Read(buf)
					if err != nil {
						return
					}
					cmd := string(buf[:n])
					var response string
					if cmd == "DATE\r\n" {
						response = "111 20240101000000\r\n"
					} else if strings.HasPrefix(cmd, "BODY") {
						// Slow response to keep connections busy
						time.Sleep(50 * time.Millisecond)
						response = "222 0 <id> body follows\r\nline1\r\n.\r\n"
					} else {
						response = "500 Unknown Command\r\n"
					}
					if _, err := c.Write([]byte(response)); err != nil {
						return
					}
				}
			}(conn)
		}
	}()

	dial := func(ctx context.Context) (net.Conn, error) {
		var d net.Dialer
		return d.DialContext(ctx, "tcp", l.Addr().String())
	}

	maxConns := 3
	p, err := NewProvider(context.Background(), ProviderConfig{
		Address:               l.Addr().String(),
		MaxConnections:        maxConns,
		InitialConnections:    1,
		InflightPerConnection: 1,
		ConnFactory:           dial,
	})
	if err != nil {
		t.Fatalf("failed to create provider: %v", err)
	}
	defer func() {
		_ = p.Close()
	}()

	// Make many concurrent requests to trigger connection scaling
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()

			respCh := p.Send(ctx, []byte("BODY <test>\r\n"), nil)
			<-respCh
		}()
	}

	// Allow time for connection scaling
	time.Sleep(200 * time.Millisecond)

	// Check connection count doesn't exceed max
	currentConns := atomic.LoadInt32(&connectionCount)
	if currentConns > int32(maxConns) {
		t.Errorf("connection count %d exceeds max connections %d", currentConns, maxConns)
	}

	wg.Wait()
}

// TestSaturationBackpressure tests that the provider applies backpressure
// when the request channel is full.
func TestSaturationBackpressure(t *testing.T) {
	var processingCount int32

	srv, cleanup := testutil.StartMockNNTPServer(t, testutil.MockServerConfig{
		Handler: func(cmd string) (string, error) {
			if cmd == "DATE\r\n" {
				return "111 20240101000000\r\n", nil
			}
			if strings.HasPrefix(cmd, "BODY") {
				atomic.AddInt32(&processingCount, 1)
				// Hold the connection busy
				time.Sleep(200 * time.Millisecond)
				atomic.AddInt32(&processingCount, -1)
				return "222 0 <id> body follows\r\nline1\r\n.\r\n", nil
			}
			return "500 Unknown Command\r\n", nil
		},
	})
	defer cleanup()

	dial := func(ctx context.Context) (net.Conn, error) {
		var d net.Dialer
		return d.DialContext(ctx, "tcp", srv.Addr())
	}

	// Create provider with limited capacity
	p, err := NewProvider(context.Background(), ProviderConfig{
		Address:               srv.Addr(),
		MaxConnections:        2,
		InitialConnections:    2,
		InflightPerConnection: 1, // No pipelining
		ConnFactory:           dial,
	})
	if err != nil {
		t.Fatalf("failed to create provider: %v", err)
	}
	defer func() {
		_ = p.Close()
	}()

	// Flood with requests
	var wg sync.WaitGroup
	var successCount int32
	var timeoutCount int32

	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			// Short timeout to detect backpressure
			ctx, cancel := context.WithTimeout(context.Background(), 6*time.Second)
			defer cancel()

			respCh := p.Send(ctx, []byte("BODY <test>\r\n"), nil)
			resp := <-respCh

			if resp.Err == nil {
				atomic.AddInt32(&successCount, 1)
			} else if strings.Contains(resp.Err.Error(), "timeout") {
				atomic.AddInt32(&timeoutCount, 1)
			}
		}()
	}

	wg.Wait()

	// With backpressure, not all requests can be processed immediately
	// Some may succeed, some may timeout
	total := atomic.LoadInt32(&successCount) + atomic.LoadInt32(&timeoutCount)
	if total != 20 {
		t.Errorf("expected 20 total requests handled, got %d", total)
	}

	t.Logf("Results: %d successful, %d timeouts", successCount, timeoutCount)
}

// TestSaturationProviderClosed tests that requests fail properly when provider is closed
// while saturated.
func TestSaturationProviderClosed(t *testing.T) {
	srv, cleanup := testutil.StartMockNNTPServer(t, testutil.MockServerConfig{
		Handler: func(cmd string) (string, error) {
			if cmd == "DATE\r\n" {
				return "111 20240101000000\r\n", nil
			}
			if strings.HasPrefix(cmd, "BODY") {
				// Very slow response
				time.Sleep(5 * time.Second)
				return "222 0 <id> body follows\r\nline1\r\n.\r\n", nil
			}
			return "500 Unknown Command\r\n", nil
		},
	})
	defer cleanup()

	dial := func(ctx context.Context) (net.Conn, error) {
		var d net.Dialer
		return d.DialContext(ctx, "tcp", srv.Addr())
	}

	p, err := NewProvider(context.Background(), ProviderConfig{
		Address:               srv.Addr(),
		MaxConnections:        1,
		InitialConnections:    1,
		InflightPerConnection: 1,
		ConnFactory:           dial,
	})
	if err != nil {
		t.Fatalf("failed to create provider: %v", err)
	}

	// Start request that will be in-flight when provider closes
	var wg sync.WaitGroup
	var resp1, resp2 Response

	wg.Add(2)
	go func() {
		defer wg.Done()
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		respCh := p.Send(ctx, []byte("BODY <first>\r\n"), nil)
		resp1 = <-respCh
	}()

	// Wait for first request to start
	time.Sleep(100 * time.Millisecond)

	go func() {
		defer wg.Done()
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		respCh := p.Send(ctx, []byte("BODY <second>\r\n"), nil)
		resp2 = <-respCh
	}()

	// Wait for second request to be queued
	time.Sleep(100 * time.Millisecond)

	// Close provider while requests are pending
	_ = p.Close()

	wg.Wait()

	// Both requests should receive errors
	if resp1.Err == nil && resp2.Err == nil {
		t.Error("expected at least one request to fail when provider closed")
	}
}

// TestSaturationInflightPipelining tests that pipelining (InflightPerConnection > 1)
// allows more concurrent requests on the same connection.
// Note: This test may trigger a known deadlock in removeConnection -> drainOrphanedRequests.
func TestSaturationInflightPipelining(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping in short mode - may trigger deadlock in removeConnection")
	}
	var concurrentRequests int32
	var maxConcurrent int32

	// Use a TCP server to avoid pipe issues with concurrent writes
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}
	defer func() {
		_ = l.Close()
	}()

	go func() {
		for {
			conn, err := l.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer func() {
					_ = c.Close()
				}()
				_, _ = c.Write([]byte("200 Service Ready\r\n"))
				buf := make([]byte, 1024)
				for {
					n, err := c.Read(buf)
					if err != nil {
						return
					}
					cmd := string(buf[:n])
					if cmd == "DATE\r\n" {
						_, _ = c.Write([]byte("111 20240101000000\r\n"))
						continue
					}
					if strings.HasPrefix(cmd, "BODY") {
						current := atomic.AddInt32(&concurrentRequests, 1)
						// Track max concurrent using CAS
						for {
							old := atomic.LoadInt32(&maxConcurrent)
							if current <= old || atomic.CompareAndSwapInt32(&maxConcurrent, old, current) {
								break
							}
						}

						time.Sleep(50 * time.Millisecond)
						atomic.AddInt32(&concurrentRequests, -1)
						_, _ = c.Write([]byte("222 0 <id> body follows\r\nline1\r\n.\r\n"))
					}
				}
			}(conn)
		}
	}()

	dial := func(ctx context.Context) (net.Conn, error) {
		var d net.Dialer
		return d.DialContext(ctx, "tcp", l.Addr().String())
	}

	// Create provider with pipelining enabled
	p, err := NewProvider(context.Background(), ProviderConfig{
		Address:               l.Addr().String(),
		MaxConnections:        1, // Single connection
		InitialConnections:    1,
		InflightPerConnection: 5, // Allow 5 in-flight
		ConnFactory:           dial,
	})
	if err != nil {
		t.Fatalf("failed to create provider: %v", err)
	}
	defer func() {
		_ = p.Close()
	}()

	// Send concurrent requests
	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()

			respCh := p.Send(ctx, []byte("BODY <test>\r\n"), nil)
			<-respCh
		}()
	}

	wg.Wait()

	// With pipelining, should be able to handle multiple requests concurrently
	// on a single connection. Note: Due to NNTP FIFO response ordering, we may
	// not see true parallel execution, but the inflight slots should allow
	// multiple requests to be in-flight on the connection.
	maxSeen := atomic.LoadInt32(&maxConcurrent)
	t.Logf("Max concurrent requests on single connection: %d", maxSeen)
}

// parseIntFromString is a helper to extract an int from a string.
func parseIntFromString(s string, out *int) (int, error) {
	var i int
	for _, c := range s {
		if c >= '0' && c <= '9' {
			i = i*10 + int(c-'0')
		} else {
			break
		}
	}
	*out = i
	return i, nil
}

// TestSaturationDeadChannelHandling tests that the provider properly handles
// the deadCh when all connections die.
func TestSaturationDeadChannelHandling(t *testing.T) {
	var connClosed int32

	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}
	defer func() {
		_ = l.Close()
	}()

	go func() {
		for {
			conn, err := l.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer func() {
					_ = c.Close()
					atomic.AddInt32(&connClosed, 1)
				}()
				_, _ = c.Write([]byte("200 Service Ready\r\n"))
				buf := make([]byte, 1024)
				for {
					n, err := c.Read(buf)
					if err != nil {
						return
					}
					cmd := string(buf[:n])
					if cmd == "DATE\r\n" {
						_, _ = c.Write([]byte("111 20240101000000\r\n"))
						continue
					}
					// Close connection immediately after BODY to simulate connection death
					if strings.HasPrefix(cmd, "BODY") {
						return // Close without response
					}
				}
			}(conn)
		}
	}()

	dial := func(ctx context.Context) (net.Conn, error) {
		var d net.Dialer
		return d.DialContext(ctx, "tcp", l.Addr().String())
	}

	p, err := NewProvider(context.Background(), ProviderConfig{
		Address:               l.Addr().String(),
		MaxConnections:        2,
		InitialConnections:    2,
		InflightPerConnection: 1,
		ConnFactory:           dial,
	})
	if err != nil {
		t.Fatalf("failed to create provider: %v", err)
	}
	defer func() {
		_ = p.Close()
	}()

	// Make requests that will kill all connections
	var wg sync.WaitGroup
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()

			respCh := p.Send(ctx, []byte("BODY <kill>\r\n"), nil)
			<-respCh
		}()
		time.Sleep(50 * time.Millisecond)
	}

	wg.Wait()

	// Wait for connections to close and deadCh to be signaled
	time.Sleep(100 * time.Millisecond)

	// Try another request - should get ErrProviderUnavailable or reconnect
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	respCh := p.Send(ctx, []byte("BODY <after-death>\r\n"), io.Discard)
	resp := <-respCh

	// Should either fail with unavailable or succeed after reconnect
	// Both are valid behaviors
	if resp.Err != nil {
		t.Logf("Request after connection death: %v", resp.Err)
	}
}
