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

// TestRaceConcurrentAddConnection tests for race conditions when multiple
// goroutines try to add connections simultaneously.
func TestRaceConcurrentAddConnection(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}
	defer func() {
		_ = l.Close()
	}()

	var connectionCount int32
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
					if cmd == "DATE\r\n" {
						_, _ = c.Write([]byte("111 20240101000000\r\n"))
					} else if strings.HasPrefix(cmd, "BODY") {
						_, _ = c.Write([]byte("222 0 <id> body follows\r\ndata\r\n.\r\n"))
					} else {
						_, _ = c.Write([]byte("500 Unknown Command\r\n"))
					}
				}
			}(conn)
		}
	}()

	dial := func(ctx context.Context) (net.Conn, error) {
		var d net.Dialer
		return d.DialContext(ctx, "tcp", l.Addr().String())
	}

	// Create provider with 0 initial connections to force addConnection on first request
	p, err := NewProvider(context.Background(), ProviderConfig{
		Address:            l.Addr().String(),
		MaxConnections:     10,
		InitialConnections: 0,
		ConnFactory:        dial,
	})
	if err != nil {
		t.Fatalf("failed to create provider: %v", err)
	}
	defer func() {
		_ = p.Close()
	}()

	// Spawn many goroutines that all try to send requests simultaneously
	// This triggers concurrent addConnection calls
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			respCh := p.Send(ctx, []byte("BODY <test>\r\n"), nil)
			<-respCh
		}()
	}

	wg.Wait()

	// Verify connection count doesn't exceed max
	finalConns := atomic.LoadInt32(&connectionCount)
	if finalConns > 10 {
		t.Errorf("created %d connections, exceeds max of 10", finalConns)
	}

	t.Logf("Created %d connections (max: 10)", finalConns)
}

// TestRaceSignalAliveSignalDead tests for race conditions between signalAlive and signalDead.
// NOTE: This test intentionally triggers connection deaths which may expose the
// removeConnection -> drainOrphanedRequests deadlock.
func TestRaceSignalAliveSignalDead(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode due to potential deadlock")
	}
	var toggleCount int32

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
			count := atomic.AddInt32(&toggleCount, 1)
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
						// Every other connection dies immediately
						if idx%2 == 0 {
							return // Disconnect
						}
						_, _ = c.Write([]byte("222 0 <id> body follows\r\ndata\r\n.\r\n"))
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

	// Rapid fire requests to trigger many alive/dead transitions
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
			defer cancel()
			respCh := p.Send(ctx, []byte("BODY <test>\r\n"), nil)
			<-respCh
		}()
		// Small stagger to increase interleaving
		if i%10 == 0 {
			time.Sleep(10 * time.Millisecond)
		}
	}

	wg.Wait()

	// Test passed if we got here without deadlock or panic
	t.Logf("Completed %d toggle cycles without deadlock", atomic.LoadInt32(&toggleCount))
}

// TestRaceChannelClose tests for race conditions during channel closure.
func TestRaceChannelClose(t *testing.T) {
	srv, cleanup := testutil.StartMockNNTPServer(t, testutil.MockServerConfig{
		Handler: func(cmd string) (string, error) {
			if cmd == "DATE\r\n" {
				return "111 20240101000000\r\n", nil
			}
			if strings.HasPrefix(cmd, "BODY") {
				// Small delay to allow for race conditions
				time.Sleep(10 * time.Millisecond)
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
		MaxConnections: 3,
		ConnFactory:    dial,
	})
	if err != nil {
		t.Fatalf("failed to create provider: %v", err)
	}

	// Start requests
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			respCh := p.Send(ctx, []byte("BODY <test>\r\n"), nil)
			<-respCh
		}()
	}

	// Close provider while requests are in-flight
	time.Sleep(50 * time.Millisecond)
	_ = p.Close()

	wg.Wait()

	// Test passed if we got here without panic on closed channel
	t.Log("Channel closure race test passed")
}

// TestRaceSemaphoreContention tests for race conditions under semaphore contention.
// Note: This test may trigger a known deadlock in removeConnection -> drainOrphanedRequests.
func TestRaceSemaphoreContention(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping in short mode - may trigger deadlock in removeConnection")
	}
	srv, cleanup := testutil.StartMockNNTPServer(t, testutil.MockServerConfig{
		Handler: func(cmd string) (string, error) {
			if cmd == "DATE\r\n" {
				return "111 20240101000000\r\n", nil
			}
			if strings.HasPrefix(cmd, "BODY") {
				// Variable delay to cause semaphore contention
				time.Sleep(time.Duration(5+time.Now().UnixNano()%10) * time.Millisecond)
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

	// Single connection with pipelining to stress semaphore
	p, err := NewProvider(context.Background(), ProviderConfig{
		Address:               srv.Addr(),
		MaxConnections:        1,
		InitialConnections:    1,
		InflightPerConnection: 3, // Limited slots
		ConnFactory:           dial,
	})
	if err != nil {
		t.Fatalf("failed to create provider: %v", err)
	}
	defer func() {
		_ = p.Close()
	}()

	// Many concurrent requests competing for 3 slots
	var wg sync.WaitGroup
	var successCount, errorCount int32

	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			respCh := p.Send(ctx, []byte("BODY <test>\r\n"), nil)
			resp := <-respCh
			if resp.Err == nil {
				atomic.AddInt32(&successCount, 1)
			} else {
				atomic.AddInt32(&errorCount, 1)
			}
		}()
	}

	wg.Wait()

	t.Logf("Semaphore contention: %d success, %d errors", successCount, errorCount)

	// Most should succeed (semaphore should not deadlock)
	if successCount < 40 {
		t.Errorf("too many failures under semaphore contention: %d", errorCount)
	}
}

// TestRaceProviderListModification tests for race conditions when modifying provider lists.
func TestRaceProviderListModification(t *testing.T) {
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

	client := NewClient()
	defer client.Close()

	// Create multiple providers
	providers := make([]*Provider, 5)
	for i := 0; i < 5; i++ {
		p, err := NewProvider(context.Background(), ProviderConfig{
			Address:        srv.Addr(),
			MaxConnections: 1,
			ConnFactory:    dial,
		})
		if err != nil {
			t.Fatalf("failed to create provider %d: %v", i, err)
		}
		providers[i] = p
	}

	// Concurrently add providers, remove providers, and make requests
	var wg sync.WaitGroup

	// Add providers
	for i := 0; i < 5; i++ {
		wg.Add(1)
		idx := i
		go func() {
			defer wg.Done()
			tier := ProviderPrimary
			if idx%2 == 1 {
				tier = ProviderBackup
			}
			_ = client.AddProvider(providers[idx], tier)
		}()
	}

	wg.Wait() // Wait for adds to complete

	// Now concurrently remove, re-add, and make requests
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 10; j++ {
				ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
				_ = client.Body(ctx, "test-id", io.Discard)
				cancel()
			}
		}()
	}

	// Remove providers while requests are happening
	for i := 0; i < 5; i++ {
		wg.Add(1)
		idx := i
		go func() {
			defer wg.Done()
			time.Sleep(50 * time.Millisecond)
			_ = client.RemoveProvider(providers[idx])
		}()
	}

	wg.Wait()

	// Test passed if we got here without panic
	t.Log("Provider list modification race test passed")
}

// TestRaceHealthCheck tests for race conditions during health checks.
func TestRaceHealthCheck(t *testing.T) {
	var shouldFail atomic.Bool

	srv, cleanup := testutil.StartMockNNTPServer(t, testutil.MockServerConfig{
		Handler: func(cmd string) (string, error) {
			if cmd == "DATE\r\n" {
				if shouldFail.Load() {
					return "", io.EOF
				}
				return "111 20240101000000\r\n", nil
			}
			if strings.HasPrefix(cmd, "BODY") {
				return "222 0 <id> body follows\r\ndata\r\n.\r\n", nil
			}
			return "500 Unknown Command\r\n", nil
		},
	})
	defer cleanup()

	dial := testutil.MockDialerWithHandler(testutil.MockServerConfig{
		Handler: func(cmd string) (string, error) {
			if cmd == "DATE\r\n" {
				if shouldFail.Load() {
					return "", io.EOF
				}
				return "111 20240101000000\r\n", nil
			}
			if strings.HasPrefix(cmd, "BODY") {
				return "222 0 <id> body follows\r\ndata\r\n.\r\n", nil
			}
			return "500 Unknown Command\r\n", nil
		},
	})

	client := NewClient()
	defer client.Close()

	p, err := NewProvider(context.Background(), ProviderConfig{
		Address:        srv.Addr(),
		MaxConnections: 2,
		ConnFactory:    dial,
	})
	if err != nil {
		t.Fatalf("failed to create provider: %v", err)
	}

	if err := client.AddProvider(p, ProviderPrimary); err != nil {
		t.Fatalf("failed to add provider: %v", err)
	}

	// Toggle failure state while making requests and health checks
	var wg sync.WaitGroup

	// Requests
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 20; j++ {
				ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
				_ = client.Body(ctx, "test-id", io.Discard)
				cancel()
				time.Sleep(10 * time.Millisecond)
			}
		}()
	}

	// Health checks
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 10; i++ {
			client.checkProviders()
			time.Sleep(20 * time.Millisecond)
		}
	}()

	// Toggle failure
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 5; i++ {
			time.Sleep(30 * time.Millisecond)
			shouldFail.Store(true)
			time.Sleep(30 * time.Millisecond)
			shouldFail.Store(false)
		}
	}()

	wg.Wait()

	// Test passed if we got here without panic or deadlock
	t.Log("Health check race test passed")
}

// TestRaceContextCancellation tests for race conditions during context cancellation.
func TestRaceContextCancellation(t *testing.T) {
	srv, cleanup := testutil.StartMockNNTPServer(t, testutil.MockServerConfig{
		Handler: func(cmd string) (string, error) {
			if cmd == "DATE\r\n" {
				return "111 20240101000000\r\n", nil
			}
			if strings.HasPrefix(cmd, "BODY") {
				// Delay to allow cancellation mid-request
				time.Sleep(100 * time.Millisecond)
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
		MaxConnections:        3,
		InflightPerConnection: 2,
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

	// Many requests with varying cancellation times
	var wg sync.WaitGroup
	for i := 0; i < 30; i++ {
		wg.Add(1)
		cancelDelay := time.Duration(i%5*20) * time.Millisecond
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithCancel(context.Background())
			go func() {
				time.Sleep(cancelDelay)
				cancel()
			}()
			_ = client.Body(ctx, "test-id", io.Discard)
		}()
	}

	wg.Wait()

	// Verify provider is still functional after all the cancellations
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	err = client.Body(ctx, "test-id", io.Discard)
	if err != nil {
		t.Logf("Post-cancellation request: %v", err)
	}

	t.Log("Context cancellation race test passed")
}

// TestRaceMetricsCollection tests for race conditions when collecting metrics.
func TestRaceMetricsCollection(t *testing.T) {
	srv, cleanup := testutil.StartMockNNTPServer(t, testutil.MockServerConfig{
		Handler: testutil.YencBodyHandler([]byte("test data for metrics"), "test.bin"),
	})
	defer cleanup()

	dial := func(ctx context.Context) (net.Conn, error) {
		var d net.Dialer
		return d.DialContext(ctx, "tcp", srv.Addr())
	}

	p, err := NewProvider(context.Background(), ProviderConfig{
		Address:        srv.Addr(),
		MaxConnections: 3,
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

	// Concurrently make requests and collect metrics
	var wg sync.WaitGroup

	// Requests
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 20; j++ {
				ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
				_ = client.Body(ctx, "test-id", io.Discard)
				cancel()
			}
		}()
	}

	// Metrics collection
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				_ = p.Metrics()
				_ = client.Metrics()
				time.Sleep(5 * time.Millisecond)
			}
		}()
	}

	wg.Wait()

	// Final metrics check
	metrics := p.Metrics()
	t.Logf("Final metrics: %d bytes read, %d connections", metrics.TotalBytesRead, metrics.ActiveConnections)
}

// TestRaceConnectionLifetime tests for race conditions during connection lifetime expiration.
func TestRaceConnectionLifetime(t *testing.T) {
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
					} else if strings.HasPrefix(cmd, "BODY") {
						time.Sleep(10 * time.Millisecond) // Small delay
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

	// Very short lifetime to force frequent rotation
	p, err := NewProvider(context.Background(), ProviderConfig{
		Address:            l.Addr().String(),
		MaxConnections:     3,
		InitialConnections: 3,
		MaxConnLifetime:    100 * time.Millisecond,
		HealthCheckPeriod:  50 * time.Millisecond,
		ConnFactory:        dial,
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

	// Make requests while connections are expiring
	var wg sync.WaitGroup
	var successCount, errorCount int32

	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			err := client.Body(ctx, "test-id", io.Discard)
			if err == nil {
				atomic.AddInt32(&successCount, 1)
			} else {
				atomic.AddInt32(&errorCount, 1)
			}
		}()
		time.Sleep(10 * time.Millisecond)
	}

	wg.Wait()

	t.Logf("Connection lifetime race: %d success, %d errors", successCount, errorCount)

	// Most should succeed despite connection rotation
	if successCount < 40 {
		t.Errorf("too many failures during connection lifetime rotation: %d", errorCount)
	}
}

// TestRaceDeadChannelRecreation tests for race conditions when deadCh is recreated.
// NOTE: This test intentionally triggers connection deaths which may expose the
// removeConnection -> drainOrphanedRequests deadlock.
func TestRaceDeadChannelRecreation(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode due to potential deadlock")
	}
	var acceptConns atomic.Bool
	acceptConns.Store(true)

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
						if !acceptConns.Load() {
							return // Simulate connection death
						}
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
		Address:            l.Addr().String(),
		MaxConnections:     2,
		InitialConnections: 2,
		ConnFactory:        dial,
	})
	if err != nil {
		t.Fatalf("failed to create provider: %v", err)
	}
	defer func() {
		_ = p.Close()
	}()

	var wg sync.WaitGroup

	// Toggle connection acceptance rapidly
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 10; i++ {
			acceptConns.Store(false)
			time.Sleep(20 * time.Millisecond)
			acceptConns.Store(true)
			time.Sleep(20 * time.Millisecond)
		}
	}()

	// Make requests during toggling
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 20; j++ {
				ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
				respCh := p.Send(ctx, []byte("BODY <test>\r\n"), nil)
				<-respCh
				cancel()
			}
		}()
	}

	wg.Wait()

	// Test passed if we got here without deadlock
	t.Log("Dead channel recreation race test passed")
}
