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
)

// Helper to start a real mock server
func startMockServer(t *testing.T, id string) (string, func()) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}

	go func() {
		for {
			conn, err := l.Accept()
			if err != nil {
				return
			}

			go func(c net.Conn) {
				defer c.Close()
				if _, err := c.Write([]byte("200 Service Ready - " + id + "\r\n")); err != nil {
					return
				}

				buf := make([]byte, 1024)
				for {
					n, err := c.Read(buf)
					if err != nil {
						return
					}
					msg := string(buf[:n])
					if strings.HasPrefix(msg, "BODY") {
						if _, err := c.Write([]byte("222 0 <id> body follows\r\nline1\r\nline2\r\n.\r\n")); err != nil {
							return
						}
					} else if msg == "QUIT\r\n" {
						c.Write([]byte("205 Bye\r\n"))
						return
					}
				}
			}(conn)
		}
	}()

	return l.Addr().String(), func() { l.Close() }
}

func TestClientHotswapProviders(t *testing.T) {
	client := NewClient(10)
	defer client.Close()

	// 1. Add Provider A
	addr1, stop1 := startMockServer(t, "P1")
	defer stop1()

	dial1 := func(ctx context.Context) (net.Conn, error) {
		var d net.Dialer
		return d.DialContext(ctx, "tcp", addr1)
	}

	p1, err := NewProvider(context.Background(), ProviderConfig{
		Address: addr1, MaxConnections: 1, InflightPerConnection: 1, ConnFactory: dial1,
	})
	if err != nil {
		t.Fatalf("failed to create p1: %v", err)
	}
	client.AddProvider(p1, ProviderPrimary)

	// 2. Start load
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Add(1)
	errCh := make(chan error, 100)
	var successCount int64

	go func() {
		defer wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			default:
				// Use reasonable timeout
				reqCtx, reqCancel := context.WithTimeout(ctx, 2*time.Second)
				err := client.Body(reqCtx, "123", io.Discard)
				reqCancel()

				if err != nil {
					// Don't report context canceled errors as failures if they happen during shutdown
					if ctx.Err() == nil {
						select {
						case errCh <- err:
						default:
						}
					}
				} else {
					atomic.AddInt64(&successCount, 1)
				}
				time.Sleep(10 * time.Millisecond)
			}
		}
	}()

	// Allow some requests to flow
	time.Sleep(500 * time.Millisecond)

	if atomic.LoadInt64(&successCount) == 0 {
		cancel()
		wg.Wait()
		close(errCh)
		for err := range errCh {
			t.Logf("Error during initial phase: %v", err)
		}
		t.Fatal("expected some successful requests with P1")
	}

	// 3. Add Provider B
	addr2, stop2 := startMockServer(t, "P2")
	defer stop2()

	dial2 := func(ctx context.Context) (net.Conn, error) {
		var d net.Dialer
		return d.DialContext(ctx, "tcp", addr2)
	}

	p2, err := NewProvider(context.Background(), ProviderConfig{
		Address: addr2, MaxConnections: 1, InflightPerConnection: 1, ConnFactory: dial2,
	})
	if err != nil {
		t.Fatalf("failed to create p2: %v", err)
	}
	client.AddProvider(p2, ProviderPrimary)

	// 4. Remove Provider A
	time.Sleep(50 * time.Millisecond)
	client.RemoveProvider(p1)

	// 5. Verify flow continues
	startSuccess := atomic.LoadInt64(&successCount)
	time.Sleep(500 * time.Millisecond)
	endSuccess := atomic.LoadInt64(&successCount)

	if endSuccess <= startSuccess {
		// Log errors for debugging
		cancel()
		wg.Wait()
		close(errCh)
		for err := range errCh {
			t.Logf("Observed error: %v", err)
		}
		t.Fatal("expected requests to continue succeeding after removing P1")
	}

	cancel()
	wg.Wait()
	close(errCh)

	for err := range errCh {
		t.Logf("Observed error: %v", err)
	}
}

func TestClientHealthCheck(t *testing.T) {
	// Mock connection factory
	var shouldFail atomic.Bool
	shouldFail.Store(false)

	mockDial := func(ctx context.Context) (net.Conn, error) {
		c1, c2 := net.Pipe()

		go func() {
			defer c2.Close()
			// Send Greeting
			if _, err := c2.Write([]byte("200 Service Ready\r\n")); err != nil {
				return
			}

			buf := make([]byte, 1024)
			for {
				n, err := c2.Read(buf)
				if err != nil {
					return
				}

				// Simple parser to handle command boundaries roughly
				msg := string(buf[:n])

				// In a real scenario, we'd handle partial reads, but net.Pipe is atomic-ish for small writes usually.
				// However, client might write "DATE\r\n" in one go.

				if shouldFail.Load() {
					// Simulate failure by closing connection
					return
				}

				if msg == "DATE\r\n" {
					c2.Write([]byte("111 20240101000000\r\n"))
				}
			}
		}()
		return c1, nil
	}

	client := NewClient(10)
	defer client.Close()

	p, err := NewProvider(context.Background(), ProviderConfig{
		Address:               "example.com:119",
		MaxConnections:        1,
		InflightPerConnection: 1,
		ConnFactory:           mockDial,
	})
	if err != nil {
		t.Fatalf("failed to create provider: %v", err)
	}

	client.AddProvider(p, ProviderPrimary)

	// Verify initially active
	primaries := client.primaries.Load().([]*Provider)
	if len(primaries) != 1 {
		t.Fatalf("expected 1 primary, got %d", len(primaries))
	}

	// Mark as failing
	shouldFail.Store(true)

	// Trigger check
	// We need to wait a bit because the provider's connection might need to be closed by the failure first
	// Actually Date() handles the request.
	client.checkProviders()

	// Verify moved to dead
	primaries = client.primaries.Load().([]*Provider)
	if len(primaries) != 0 {
		t.Fatalf("expected 0 primaries, got %d", len(primaries))
	}
	if len(client.deadPrimaries) != 1 {
		t.Fatalf("expected 1 dead primary, got %d", len(client.deadPrimaries))
	}

	// Mark as working
	shouldFail.Store(false)

	// Trigger check
	// This will trigger reconnection inside the provider (as part of Send -> addConnection)
	client.checkProviders()

	// Verify moved to active
	primaries = client.primaries.Load().([]*Provider)
	if len(primaries) != 1 {
		t.Fatalf("expected 1 primary, got %d", len(primaries))
	}
	if len(client.deadPrimaries) != 0 {
		t.Fatalf("expected 0 dead primaries, got %d", len(client.deadPrimaries))
	}
}

func TestClientRemoveProvider(t *testing.T) {
	mockDial := func(ctx context.Context) (net.Conn, error) {
		c1, c2 := net.Pipe()
		go func() {
			c2.Write([]byte("200 Service Ready\r\n"))
			buf := make([]byte, 1024)
			c2.Read(buf)
			c2.Close()
		}()
		return c1, nil
	}

	client := NewClient(10)
	defer client.Close()

	// Create two providers
	p1, _ := NewProvider(context.Background(), ProviderConfig{
		Address: "p1.example.com:119", MaxConnections: 1, ConnFactory: mockDial,
	})
	p2, _ := NewProvider(context.Background(), ProviderConfig{
		Address: "p2.example.com:119", MaxConnections: 1, ConnFactory: mockDial,
	})

	client.AddProvider(p1, ProviderPrimary)
	client.AddProvider(p2, ProviderBackup)

	// Verify they are added
	if len(client.primaries.Load().([]*Provider)) != 1 {
		t.Fatal("expected 1 primary")
	}
	if len(client.backups.Load().([]*Provider)) != 1 {
		t.Fatal("expected 1 backup")
	}

	// Remove p1 (primary)
	client.RemoveProvider(p1)

	// Verify p1 removed and closed
	if len(client.primaries.Load().([]*Provider)) != 0 {
		t.Fatal("expected 0 primaries")
	}
	if p1.ctx.Err() == nil {
		t.Fatal("expected p1 to be closed (context cancelled)")
	}

	// Remove p2 (backup)
	client.RemoveProvider(p2)

	// Verify p2 removed and closed
	if len(client.backups.Load().([]*Provider)) != 0 {
		t.Fatal("expected 0 backups")
	}
	if p2.ctx.Err() == nil {
		t.Fatal("expected p2 to be closed (context cancelled)")
	}

	// Test removing dead provider
	p3, _ := NewProvider(context.Background(), ProviderConfig{
		Address: "p3.example.com:119", MaxConnections: 1, ConnFactory: mockDial,
	})
	client.AddProvider(p3, ProviderPrimary)

	// Manually move to dead for testing
	client.mu.Lock()
	client.primaries.Store([]*Provider{})
	client.deadPrimaries = append(client.deadPrimaries, p3)
	client.mu.Unlock()

	client.RemoveProvider(p3)

	client.mu.Lock()
	if len(client.deadPrimaries) != 0 {
		client.mu.Unlock()
		t.Fatal("expected 0 dead primaries")
	}
	client.mu.Unlock()

	if p3.ctx.Err() == nil {
		t.Fatal("expected p3 to be closed")
	}
}

type mockWriterAt struct {
	data []byte
	mu   sync.Mutex
}

func (m *mockWriterAt) WriteAt(p []byte, off int64) (n int, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if int64(len(m.data)) < off+int64(len(p)) {
		newData := make([]byte, off+int64(len(p)))
		copy(newData, m.data)
		m.data = newData
	}
	copy(m.data[off:], p)
	return len(p), nil
}

func TestClientBodyAt(t *testing.T) {
	// Mock YEnc response
	// We need to encode "hello world!" by adding 42 to each byte
	raw := []byte("hello world!")
	encoded := make([]byte, len(raw))
	for i, b := range raw {
		encoded[i] = b + 42
	}

	yEncBody := "=ybegin part=1 line=128 size=12 name=test.txt\r\n" +
		"=ypart begin=1 end=12\r\n" +
		string(encoded) + // 12 bytes
		"\r\n=yend size=12 pcrc32=00000000\r\n.\r\n"

	// Mock server that returns YEnc body
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}
	defer l.Close()

	go func() {
		for {
			conn, err := l.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer c.Close()
				c.Write([]byte("200 Service Ready\r\n"))
				buf := make([]byte, 1024)
				for {
					n, err := c.Read(buf)
					if err != nil {
						return
					}
					msg := string(buf[:n])
					if strings.HasPrefix(msg, "BODY") {
						c.Write([]byte("222 0 <id> body follows\r\n" + yEncBody))
					}
				}
			}(conn)
		}
	}()

	client := NewClient(1)
	defer client.Close()

	dial := func(ctx context.Context) (net.Conn, error) {
		var d net.Dialer
		return d.DialContext(ctx, "tcp", l.Addr().String())
	}

	p, err := NewProvider(context.Background(), ProviderConfig{
		Address: l.Addr().String(), MaxConnections: 1, ConnFactory: dial,
	})
	if err != nil {
		t.Fatalf("failed to create provider: %v", err)
	}
	client.AddProvider(p, ProviderPrimary)

	wa := &mockWriterAt{}
	err = client.BodyAt(context.Background(), "123", wa)
	if err != nil {
		t.Fatalf("BodyAt failed: %v", err)
	}

	wa.mu.Lock()
	got := string(wa.data)
	wa.mu.Unlock()

	// Remove null bytes that might be in the buffer if it was resized
	got = strings.TrimRight(got, "\x00")

	if got != "hello world!" {
		t.Errorf("expected 'hello world!', got '%s'", got)
	}
}
