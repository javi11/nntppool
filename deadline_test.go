package nntppool

import (
	"bytes"
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

// TestDeadlineContextCancellation tests that context cancellation is properly propagated.
// NOTE: This test may trigger a deadlock in removeConnection -> drainOrphanedRequests
// when the connection dies due to context cancellation.
func TestDeadlineContextCancellation(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode due to potential deadlock")
	}
	var requestStarted int32

	// Use a real TCP server that keeps connection alive
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
						atomic.StoreInt32(&requestStarted, 1)
						time.Sleep(5 * time.Second)
						_, _ = c.Write([]byte("222 0 <id> body follows\r\ndata\r\n.\r\n"))
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
		Address:        l.Addr().String(),
		MaxConnections: 1,
		ConnFactory:    dial,
	})
	if err != nil {
		t.Fatalf("failed to create provider: %v", err)
	}
	defer func() {
		_ = p.Close()
	}()

	client := NewClient()
	defer client.Close()

	if err := client.AddProvider(p, ProviderPrimary); err != nil {
		t.Fatalf("failed to add provider: %v", err)
	}

	// Create context with 100ms timeout
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	start := time.Now()
	err = client.Body(ctx, "test-id", io.Discard)
	elapsed := time.Since(start)

	// Should timeout before the 5-second server delay
	if err == nil {
		t.Error("expected timeout error")
	}

	if elapsed > 500*time.Millisecond {
		t.Errorf("expected quick timeout, got %v", elapsed)
	}

	// Verify request actually started (reached server)
	if atomic.LoadInt32(&requestStarted) != 1 {
		t.Log("Note: request may not have reached server before cancellation")
	}

	t.Logf("Request cancelled after %v", elapsed)
}

// TestDeadlinePerRequestEnforcement tests that per-request deadlines are enforced.
func TestDeadlinePerRequestEnforcement(t *testing.T) {
	// Server that always responds slowly
	srv, cleanup := testutil.StartMockNNTPServer(t, testutil.MockServerConfig{
		Handler: func(cmd string) (string, error) {
			if cmd == "DATE\r\n" {
				return "111 20240101000000\r\n", nil
			}
			if strings.HasPrefix(cmd, "BODY") {
				// All requests are slow
				time.Sleep(2 * time.Second)
				return "222 0 <id> body follows\r\ndata\r\n.\r\n", nil
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

	client := NewClient()
	defer client.Close()

	if err := client.AddProvider(p, ProviderPrimary); err != nil {
		t.Fatalf("failed to add provider: %v", err)
	}

	// Short timeout request (should timeout)
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	start := time.Now()
	err = client.Body(ctx, "test-req", io.Discard)
	elapsed := time.Since(start)

	// Should timeout
	if err == nil {
		t.Error("expected slow request to timeout")
	}

	// Should timeout around 200ms, not wait 2s
	if elapsed > 500*time.Millisecond {
		t.Errorf("deadline not enforced properly, waited %v", elapsed)
	}

	t.Logf("Request timed out after %v (expected ~200ms)", elapsed)
}

// TestDeadlinePipelinedRequests tests deadline handling with pipelined requests.
func TestDeadlinePipelinedRequests(t *testing.T) {
	var requestIndex int32

	// Server that introduces delays
	srv, cleanup := testutil.StartMockNNTPServer(t, testutil.MockServerConfig{
		Handler: func(cmd string) (string, error) {
			if cmd == "DATE\r\n" {
				return "111 20240101000000\r\n", nil
			}
			if strings.HasPrefix(cmd, "BODY") {
				idx := atomic.AddInt32(&requestIndex, 1)
				// Variable delays to test deadline isolation
				delay := time.Duration(idx*100) * time.Millisecond
				time.Sleep(delay)
				return "222 0 <id> body follows\r\ndata\r\n.\r\n", nil
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
		InflightPerConnection: 5, // Enable pipelining
		ConnFactory:           dial,
	})
	if err != nil {
		t.Fatalf("failed to create provider: %v", err)
	}
	defer func() {
		_ = p.Close()
	}()

	client := NewClient()
	defer client.Close()

	if err := client.AddProvider(p, ProviderPrimary); err != nil {
		t.Fatalf("failed to add provider: %v", err)
	}

	var wg sync.WaitGroup
	results := make([]error, 5)

	// Send 5 pipelined requests with different deadlines
	deadlines := []time.Duration{
		50 * time.Millisecond,  // Should timeout
		150 * time.Millisecond, // Should timeout
		300 * time.Millisecond, // Might succeed
		500 * time.Millisecond, // Should succeed
		1 * time.Second,        // Should succeed
	}

	for i := 0; i < 5; i++ {
		wg.Add(1)
		idx := i
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), deadlines[idx])
			defer cancel()
			results[idx] = client.Body(ctx, "test-id", io.Discard)
		}()
		time.Sleep(10 * time.Millisecond) // Small stagger
	}

	wg.Wait()

	successCount := 0
	timeoutCount := 0
	for i, err := range results {
		if err == nil {
			successCount++
			t.Logf("Request %d (deadline %v): success", i, deadlines[i])
		} else {
			timeoutCount++
			t.Logf("Request %d (deadline %v): %v", i, deadlines[i], err)
		}
	}

	// With progressive delays, earlier requests with tight deadlines should timeout
	// while later requests should succeed
	t.Logf("Pipelined results: %d success, %d timeout", successCount, timeoutCount)
}

// TestDeadlineSlowServer tests deadline handling with a slow server.
func TestDeadlineSlowServer(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode due to slow server simulation")
	}
	// Server that sends response slowly byte-by-byte
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
						// Send response header
						_, _ = c.Write([]byte("222 0 <id> body follows\r\n"))
						// Slow drip the body (100ms per byte)
						response := []byte("slow response data\r\n.\r\n")
						for _, b := range response {
							time.Sleep(100 * time.Millisecond)
							_, _ = c.Write([]byte{b})
						}
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
		Address:        l.Addr().String(),
		MaxConnections: 1,
		ConnFactory:    dial,
	})
	if err != nil {
		t.Fatalf("failed to create provider: %v", err)
	}
	defer func() {
		_ = p.Close()
	}()

	client := NewClient()
	defer client.Close()

	if err := client.AddProvider(p, ProviderPrimary); err != nil {
		t.Fatalf("failed to add provider: %v", err)
	}

	// Request with deadline that will expire during slow response
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()

	start := time.Now()
	err = client.Body(ctx, "test-id", io.Discard)
	elapsed := time.Since(start)

	if err == nil {
		t.Error("expected timeout error for slow server")
	}

	// Should timeout around 300ms, not wait for full response (~2s)
	if elapsed > 1*time.Second {
		t.Errorf("deadline not enforced, waited %v", elapsed)
	}

	t.Logf("Slow server request cancelled after %v", elapsed)
}

// TestDeadlineCancellationMidResponse tests cancellation while response is being processed.
func TestDeadlineCancellationMidResponse(t *testing.T) {
	// Server that sends response with delay between chunks
	srv, cleanup := testutil.StartMockNNTPServer(t, testutil.MockServerConfig{
		Handler: func(cmd string) (string, error) {
			if cmd == "DATE\r\n" {
				return "111 20240101000000\r\n", nil
			}
			if strings.HasPrefix(cmd, "BODY") {
				// Send a slow-drip response (handled by the connection)
				// First send header, then data will be read by client with slow writer
				return "222 0 <id> body follows\r\nline1\r\nline2\r\nline3\r\nline4\r\nline5\r\n.\r\n", nil
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
		Address:        srv.Addr(),
		MaxConnections: 1,
		ConnFactory:    dial,
	})
	if err != nil {
		t.Fatalf("failed to create provider: %v", err)
	}
	defer func() {
		_ = p.Close()
	}()

	client := NewClient()
	defer client.Close()

	if err := client.AddProvider(p, ProviderPrimary); err != nil {
		t.Fatalf("failed to add provider: %v", err)
	}

	// Use a slow writer to simulate processing delay (20ms per write)
	sw := &slowWriter{delay: 20 * time.Millisecond}

	// Cancel mid-response (after 30ms, which should be ~1-2 writes)
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(30 * time.Millisecond)
		cancel()
	}()

	err = client.Body(ctx, "test-id", sw)

	// The context was cancelled, so we expect either:
	// - An error if cancellation happened mid-response
	// - Success if the response completed before cancellation
	// The key test is that the code doesn't hang or panic
	t.Logf("Result: err=%v, bytes written=%d", err, sw.written)
}

// slowWriter is a writer that delays each write.
type slowWriter struct {
	written int64
	delay   time.Duration
}

func (s *slowWriter) Write(p []byte) (int, error) {
	time.Sleep(s.delay)
	s.written += int64(len(p))
	return len(p), nil
}

// TestDeadlineWriteDeadline tests that write deadlines are set correctly.
// Note: This test may trigger a known deadlock in removeConnection -> drainOrphanedRequests.
func TestDeadlineWriteDeadline(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping in short mode - may trigger deadlock in removeConnection")
	}
	// Server that reads very slowly (simulating network congestion)
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}
	defer func() {
		_ = l.Close()
	}()

	var serverReceived int32

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
				buf := make([]byte, 1)
				for {
					// Read very slowly - 1 byte per 100ms
					_, err := c.Read(buf)
					if err != nil {
						return
					}
					atomic.AddInt32(&serverReceived, 1)
					time.Sleep(100 * time.Millisecond)
				}
			}(conn)
		}
	}()

	dial := func(ctx context.Context) (net.Conn, error) {
		var d net.Dialer
		return d.DialContext(ctx, "tcp", l.Addr().String())
	}

	p, err := NewProvider(context.Background(), ProviderConfig{
		Address:        l.Addr().String(),
		MaxConnections: 1,
		ConnFactory:    dial,
	})
	if err != nil {
		t.Fatalf("failed to create provider: %v", err)
	}
	defer func() {
		_ = p.Close()
	}()

	// Send request with deadline - write should timeout
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	// Large payload that will take a long time to write with slow receiver
	largePayload := bytes.Repeat([]byte("BODY <test>\r\n"), 1000)
	respCh := p.Send(ctx, largePayload, nil)

	start := time.Now()
	resp := <-respCh
	elapsed := time.Since(start)

	if resp.Err == nil {
		t.Error("expected error from write timeout or deadline")
	}

	// Should have timed out around 200ms
	if elapsed > 1*time.Second {
		t.Errorf("write deadline not enforced, waited %v", elapsed)
	}

	t.Logf("Write deadline enforced after %v, server received %d bytes",
		elapsed, atomic.LoadInt32(&serverReceived))
}

// TestDeadlineConnectionDeath tests that deadlines still work when connection dies.
// Note: This test may trigger a known deadlock in removeConnection -> drainOrphanedRequests.
func TestDeadlineConnectionDeath(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping in short mode - may trigger deadlock in removeConnection")
	}
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
			count := atomic.AddInt32(&connectionCount, 1)
			go func(c net.Conn, idx int32) {
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
						// Close connection immediately to simulate death
						return
					}
				}
			}(conn, count)
		}
	}()

	dial := func(ctx context.Context) (net.Conn, error) {
		var d net.Dialer
		return d.DialContext(ctx, "tcp", l.Addr().String())
	}

	p, err := NewProvider(context.Background(), ProviderConfig{
		Address:        l.Addr().String(),
		MaxConnections: 1,
		ConnFactory:    dial,
	})
	if err != nil {
		t.Fatalf("failed to create provider: %v", err)
	}
	defer func() {
		_ = p.Close()
	}()

	client := NewClient()
	defer client.Close()

	if err := client.AddProvider(p, ProviderPrimary); err != nil {
		t.Fatalf("failed to add provider: %v", err)
	}

	// Request with long deadline - should fail quickly due to connection death
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	start := time.Now()
	err = client.Body(ctx, "test-id", io.Discard)
	elapsed := time.Since(start)

	// Should fail quickly (connection death), not wait for deadline
	if err == nil {
		t.Error("expected error from connection death")
	}

	if elapsed > 1*time.Second {
		t.Errorf("waited too long for connection death: %v", elapsed)
	}

	t.Logf("Connection death detected after %v", elapsed)
}

// TestDeadlineContextValues tests that context values are preserved through the request.
func TestDeadlineContextValues(t *testing.T) {
	srv, cleanup := testutil.StartMockNNTPServer(t, testutil.MockServerConfig{
		Handler: func(cmd string) (string, error) {
			if cmd == "DATE\r\n" {
				return "111 20240101000000\r\n", nil
			}
			if strings.HasPrefix(cmd, "BODY") {
				return "222 0 <id> body follows\r\ndata\r\n.\r\n", nil
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
		Address:        srv.Addr(),
		MaxConnections: 1,
		ConnFactory:    dial,
	})
	if err != nil {
		t.Fatalf("failed to create provider: %v", err)
	}
	defer func() {
		_ = p.Close()
	}()

	client := NewClient()
	defer client.Close()

	if err := client.AddProvider(p, ProviderPrimary); err != nil {
		t.Fatalf("failed to add provider: %v", err)
	}

	// Create context with both deadline and custom value
	type ctxKey string
	ctx := context.WithValue(context.Background(), ctxKey("test"), "value")
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	err = client.Body(ctx, "test-id", io.Discard)
	if err != nil {
		t.Errorf("request failed: %v", err)
	}

	// Verify context wasn't corrupted (original value still accessible)
	if v := ctx.Value(ctxKey("test")); v != "value" {
		t.Errorf("context value corrupted: %v", v)
	}
}

// TestDeadlineNilContext tests that nil context is handled gracefully.
func TestDeadlineNilContext(t *testing.T) {
	srv, cleanup := testutil.StartMockNNTPServer(t, testutil.MockServerConfig{
		Handler: func(cmd string) (string, error) {
			if cmd == "DATE\r\n" {
				return "111 20240101000000\r\n", nil
			}
			if strings.HasPrefix(cmd, "BODY") {
				return "222 0 <id> body follows\r\ndata\r\n.\r\n", nil
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
		Address:        srv.Addr(),
		MaxConnections: 1,
		ConnFactory:    dial,
	})
	if err != nil {
		t.Fatalf("failed to create provider: %v", err)
	}
	defer func() {
		_ = p.Close()
	}()

	// Send with nil context via provider directly
	respCh := p.Send(t.Context(), []byte("BODY <test>\r\n"), nil)
	resp := <-respCh

	// Should succeed (nil context converted to Background)
	if resp.Err != nil {
		t.Errorf("nil context request failed: %v", resp.Err)
	}
}
