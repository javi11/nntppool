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

// TestThrottleMaxConnectionsExceeded tests that the provider throttles connections
// when receiving a "max connections exceeded" error (502).
func TestThrottleMaxConnectionsExceeded(t *testing.T) {
	var errorCount int32

	// Server that returns max connections exceeded for BODY commands
	srv, cleanup := testutil.StartMockNNTPServer(t, testutil.MockServerConfig{
		Handler: func(cmd string) (string, error) {
			if cmd == "DATE\r\n" {
				return "111 20240101000000\r\n", nil
			}
			if strings.HasPrefix(cmd, "BODY") {
				atomic.AddInt32(&errorCount, 1)
				return "502 Too many connections from your IP\r\n", nil
			}
			if strings.HasPrefix(cmd, "QUIT") {
				return "205 Bye\r\n", nil
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
		MaxConnections: 5,
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

	err = client.AddProvider(p, ProviderPrimary)
	if err != nil {
		t.Fatalf("failed to add provider: %v", err)
	}

	// Make a request that will trigger the throttle
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_ = client.Body(ctx, "test-id", io.Discard)

	// Check that throttle was applied
	throttled := atomic.LoadInt32(&p.throttledMaxConns)
	if throttled == 0 {
		t.Error("expected provider to be throttled after max connections error")
	}

	// The throttle should have reduced max connections
	effectiveMax := p.getEffectiveMaxConnections()
	if effectiveMax >= int32(p.config.MaxConnections) {
		t.Errorf("expected effective max connections (%d) to be less than configured (%d)",
			effectiveMax, p.config.MaxConnections)
	}
}

// TestThrottleExpiration tests that the throttle expires after the duration.
func TestThrottleExpiration(t *testing.T) {
	srv, cleanup := testutil.StartMockNNTPServer(t, testutil.MockServerConfig{
		Handler: testutil.DateHandler(),
	})
	defer cleanup()

	dial := func(ctx context.Context) (net.Conn, error) {
		var d net.Dialer
		return d.DialContext(ctx, "tcp", srv.Addr())
	}

	p, err := NewProvider(context.Background(), ProviderConfig{
		Address:        srv.Addr(),
		MaxConnections: 10,
		ConnFactory:    dial,
	})
	if err != nil {
		t.Fatalf("failed to create provider: %v", err)
	}
	defer func() {
		_ = p.Close()
	}()

	// Manually apply throttle with a short duration
	atomic.StoreInt32(&p.throttledMaxConns, 2)
	// Set expiration to 100ms from now (for testing, we can't easily modify the constant)
	p.throttleUntil.Store(time.Now().Add(100 * time.Millisecond).Unix())

	// Check throttle is active by checking the raw values (not getEffectiveMaxConnections
	// which would clear it if expired)
	if atomic.LoadInt32(&p.throttledMaxConns) != 2 {
		t.Errorf("expected throttledMaxConns to be 2, got %d", atomic.LoadInt32(&p.throttledMaxConns))
	}

	// Wait for expiration
	time.Sleep(150 * time.Millisecond)

	// Check throttle expired - getEffectiveMaxConnections should clear throttle and return max
	effectiveMax := p.getEffectiveMaxConnections()
	if effectiveMax != 10 {
		t.Errorf("expected throttle to expire and restore max to 10, got %d", effectiveMax)
	}

	// Check throttle values are cleared (getEffectiveMaxConnections clears on expiration)
	if atomic.LoadInt32(&p.throttledMaxConns) != 0 {
		t.Error("expected throttledMaxConns to be cleared after expiration")
	}
}

// TestThrottlePreventsNewConnections tests that throttling actually prevents
// new connections from being created.
func TestThrottlePreventsNewConnections(t *testing.T) {
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

	p, err := NewProvider(context.Background(), ProviderConfig{
		Address:            l.Addr().String(),
		MaxConnections:     10,
		InitialConnections: 1,
		ConnFactory:        dial,
	})
	if err != nil {
		t.Fatalf("failed to create provider: %v", err)
	}
	defer func() {
		_ = p.Close()
	}()

	// Wait for initial connection
	time.Sleep(50 * time.Millisecond)

	initialConns := atomic.LoadInt32(&connectionCount)
	if initialConns != 1 {
		t.Fatalf("expected 1 initial connection, got %d", initialConns)
	}

	// Apply throttle to limit to 1 connection
	atomic.StoreInt32(&p.throttledMaxConns, 1)
	p.throttleUntil.Store(time.Now().Add(5 * time.Minute).Unix())

	// Try to make concurrent requests - should not create new connections
	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()

			respCh := p.Send(ctx, []byte("BODY <test>\r\n"), nil)
			<-respCh
		}()
	}

	// Give some time for potential connection scaling
	time.Sleep(100 * time.Millisecond)

	wg.Wait()

	// Should still only have 1 connection due to throttle
	finalConns := atomic.LoadInt32(&p.connCount)
	if finalConns > 1 {
		t.Errorf("expected max 1 connection due to throttle, got %d", finalConns)
	}
}

// TestThrottleDetectsMultipleErrorFormats tests that throttling is triggered
// by various "max connections exceeded" error formats.
func TestThrottleDetectsMultipleErrorFormats(t *testing.T) {
	testCases := []struct {
		name       string
		statusCode int
		message    string
		expected   bool
	}{
		{"502 too many connections", 502, "502 Too many connections from your IP", true},
		{"502 connection limit", 502, "502 Connection limit reached", true},
		{"502 max connection", 502, "502 Max connection exceeded", true},
		{"481 too many connections", 481, "481 Too many connections", true},
		{"400 connection limit", 400, "400 Connection limit exceeded for user", true},
		{"502 generic error", 502, "502 Service unavailable", false},
		{"200 success", 200, "200 OK", false},
		{"430 not found", 430, "430 No Such Article", false},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := isMaxConnectionsExceededError(tc.statusCode, tc.message)
			if result != tc.expected {
				t.Errorf("isMaxConnectionsExceededError(%d, %q) = %v, want %v",
					tc.statusCode, tc.message, result, tc.expected)
			}
		})
	}
}

// TestThrottleConcurrentTriggering tests that concurrent requests all properly
// handle throttling when the server returns max connections error.
func TestThrottleConcurrentTriggering(t *testing.T) {
	var requestCount int32

	srv, cleanup := testutil.StartMockNNTPServer(t, testutil.MockServerConfig{
		Handler: func(cmd string) (string, error) {
			if cmd == "DATE\r\n" {
				return "111 20240101000000\r\n", nil
			}
			if strings.HasPrefix(cmd, "BODY") {
				atomic.AddInt32(&requestCount, 1)
				// First 3 requests succeed, then max connections error
				if atomic.LoadInt32(&requestCount) > 3 {
					return "502 Too many connections\r\n", nil
				}
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
		MaxConnections:        5,
		InitialConnections:    3,
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

	err = client.AddProvider(p, ProviderPrimary)
	if err != nil {
		t.Fatalf("failed to add provider: %v", err)
	}

	// Make concurrent requests
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_ = client.Body(ctx, "test-id", io.Discard)
		}()
	}

	wg.Wait()

	// Verify throttle was triggered
	if atomic.LoadInt32(&p.throttledMaxConns) == 0 {
		t.Error("expected throttle to be triggered after max connections errors")
	}
}

// TestThrottleRecoveryAfterExpiration tests that after throttle expires,
// the provider can scale back up to full capacity.
func TestThrottleRecoveryAfterExpiration(t *testing.T) {
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

	p, err := NewProvider(context.Background(), ProviderConfig{
		Address:            l.Addr().String(),
		MaxConnections:     5,
		InitialConnections: 1,
		ConnFactory:        dial,
	})
	if err != nil {
		t.Fatalf("failed to create provider: %v", err)
	}
	defer func() {
		_ = p.Close()
	}()

	// Apply throttle with short expiration
	atomic.StoreInt32(&p.throttledMaxConns, 1)
	p.throttleUntil.Store(time.Now().Add(100 * time.Millisecond).Unix())

	// Wait for expiration
	time.Sleep(150 * time.Millisecond)

	// Make concurrent requests to trigger scaling
	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()

			respCh := p.Send(ctx, []byte("BODY <test>\r\n"), nil)
			<-respCh
		}()
	}

	// Allow time for connection scaling
	time.Sleep(200 * time.Millisecond)

	wg.Wait()

	// Should have scaled up beyond the previous throttle limit
	finalConnCount := atomic.LoadInt32(&p.connCount)
	if finalConnCount <= 1 {
		t.Errorf("expected connections to scale up after throttle expiration, got %d", finalConnCount)
	}
}
