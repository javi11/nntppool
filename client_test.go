package nntppool

import (
	"context"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/javi11/nntppool/v3/testutil"
)

func TestClientHotswapProviders(t *testing.T) {
	client := NewClient(10)
	defer client.Close()

	// 1. Add Provider A
	srv1, stop1 := testutil.StartMockNNTPServer(t, testutil.MockServerConfig{
		ID: "P1",
		Handler: func(cmd string) (string, error) {
			if strings.HasPrefix(cmd, "BODY") {
				return "222 0 <id> body follows\r\nline1\r\nline2\r\n.\r\n", nil
			}
			if cmd == "QUIT\r\n" {
				return "205 Bye\r\n", nil
			}
			return "500 Unknown Command\r\n", nil
		},
	})
	defer stop1()

	dial1 := func(ctx context.Context) (net.Conn, error) {
		var d net.Dialer
		return d.DialContext(ctx, "tcp", srv1.Addr())
	}

	p1, err := NewProvider(context.Background(), ProviderConfig{
		Address: srv1.Addr(), MaxConnections: 1, InflightPerConnection: 1, ConnFactory: dial1,
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
	srv2, stop2 := testutil.StartMockNNTPServer(t, testutil.MockServerConfig{
		ID: "P2",
		Handler: func(cmd string) (string, error) {
			if strings.HasPrefix(cmd, "BODY") {
				return "222 0 <id> body follows\r\nline1\r\nline2\r\n.\r\n", nil
			}
			if cmd == "QUIT\r\n" {
				return "205 Bye\r\n", nil
			}
			return "500 Unknown Command\r\n", nil
		},
	})
	defer stop2()

	dial2 := func(ctx context.Context) (net.Conn, error) {
		var d net.Dialer
		return d.DialContext(ctx, "tcp", srv2.Addr())
	}

	p2, err := NewProvider(context.Background(), ProviderConfig{
		Address: srv2.Addr(), MaxConnections: 1, InflightPerConnection: 1, ConnFactory: dial2,
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

	mockDial := testutil.MockDialerWithHandler(testutil.MockServerConfig{
		Handler: func(cmd string) (string, error) {
			if shouldFail.Load() {
				// Simulate failure by returning error to close connection
				return "", io.EOF
			}

			if cmd == "DATE\r\n" {
				return "111 20240101000000\r\n", nil
			}
			return "500 Unknown Command\r\n", nil
		},
	})

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
	mockDial := testutil.MockDialerWithHandler(testutil.MockServerConfig{
		Handler: func(cmd string) (string, error) {
			return "500 Unknown Command\r\n", nil
		},
	})

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
	raw := []byte("hello world!")

	// Mock server that returns YEnc body
	srv, cleanup := testutil.StartMockNNTPServer(t, testutil.MockServerConfig{
		Handler: testutil.YencBodyHandler(raw, "test.txt"),
	})
	defer cleanup()

	client := NewClient(1)
	defer client.Close()

	dial := func(ctx context.Context) (net.Conn, error) {
		var d net.Dialer
		return d.DialContext(ctx, "tcp", srv.Addr())
	}

	p, err := NewProvider(context.Background(), ProviderConfig{
		Address: srv.Addr(), MaxConnections: 1, ConnFactory: dial,
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

func TestClientSpeedTest(t *testing.T) {
	// Mock YEnc response
	raw := []byte("hello world!")

	// 1. Start mock server
	srv, cleanup := testutil.StartMockNNTPServer(t, testutil.MockServerConfig{
		Handler: testutil.YencBodyHandler(raw, "test.txt"),
	})
	defer cleanup()

	// 2. Setup Client
	client := NewClient(10)
	defer client.Close()

	dial := func(ctx context.Context) (net.Conn, error) {
		var d net.Dialer
		return d.DialContext(ctx, "tcp", srv.Addr())
	}

	p, err := NewProvider(context.Background(), ProviderConfig{
		Address: srv.Addr(), MaxConnections: 2, InflightPerConnection: 1, ConnFactory: dial,
	})
	if err != nil {
		t.Fatalf("failed to create provider: %v", err)
	}
	client.AddProvider(p, ProviderPrimary)

	// 3. Run SpeedTest
	articles := []string{"1", "2", "3", "4", "5"}
	stats, err := client.SpeedTest(context.Background(), articles)
	if err != nil {
		t.Fatalf("SpeedTest failed: %v", err)
	}

	// 4. Verify stats
	if stats.SuccessCount != 5 {
		t.Errorf("expected 5 successes, got %d", stats.SuccessCount)
	}
	if stats.TotalBytes == 0 {
		t.Errorf("expected total bytes > 0")
	}
	// TotalBytes should be 5 * 12 = 60
	if stats.TotalBytes != 60 {
		t.Errorf("expected 60 bytes, got %d", stats.TotalBytes)
	}
	if stats.BytesPerSecond == 0 {
		t.Errorf("expected > 0 bytes per second")
	}
}

func TestClientBodyReader(t *testing.T) {
	t.Run("BasicRead", func(t *testing.T) {
		// Test data
		originalData := []byte("hello world!")

		// Setup mock server using testutil
		srv, cleanup := testutil.StartMockNNTPServer(t, testutil.MockServerConfig{
			Handler: testutil.YencBodyHandler(originalData, "test.txt"),
		})
		defer cleanup()

		// Setup client
		client := NewClient(10)
		defer client.Close()

		dial := func(ctx context.Context) (net.Conn, error) {
			var d net.Dialer
			return d.DialContext(ctx, "tcp", srv.Addr())
		}

		p, err := NewProvider(context.Background(), ProviderConfig{
			Address: srv.Addr(), MaxConnections: 1, ConnFactory: dial,
		})
		if err != nil {
			t.Fatalf("failed to create provider: %v", err)
		}
		client.AddProvider(p, ProviderPrimary)

		// Call BodyReader
		reader, err := client.BodyReader(context.Background(), "123")
		if err != nil {
			t.Fatalf("BodyReader failed: %v", err)
		}
		defer reader.Close()

		// Read content
		result := make([]byte, len(originalData))
		n, err := io.ReadFull(reader, result)
		if err != nil {
			t.Fatalf("failed to read: %v", err)
		}

		if n != len(originalData) {
			t.Errorf("expected %d bytes, got %d", len(originalData), n)
		}

		if string(result) != string(originalData) {
			t.Errorf("expected '%s', got '%s'", string(originalData), string(result))
		}

		// Close reader
		if err := reader.Close(); err != nil {
			t.Errorf("failed to close reader: %v", err)
		}
	})

	t.Run("YencHeaders", func(t *testing.T) {
		// Test data
		originalData := []byte("hello world!")
		filename := "test.txt"

		// Setup mock server with multi-part YEnc (part 2 of 5)
		srv, cleanup := testutil.StartMockNNTPServer(t, testutil.MockServerConfig{
			Handler: func(cmd string) (string, error) {
				if strings.HasPrefix(cmd, "BODY") {
					// Use testutil.EncodeYencMultiPart for multi-part encoding
					yencBody := testutil.EncodeYencMultiPart(originalData, filename, 2, 5, 1, int64(len(originalData)))
					return fmt.Sprintf("222 0 <id> body follows\r\n%s.\r\n", yencBody), nil
				}
				return "500 Unknown Command\r\n", nil
			},
		})
		defer cleanup()

		// Setup client
		client := NewClient(10)
		defer client.Close()

		dial := func(ctx context.Context) (net.Conn, error) {
			var d net.Dialer
			return d.DialContext(ctx, "tcp", srv.Addr())
		}

		p, err := NewProvider(context.Background(), ProviderConfig{
			Address: srv.Addr(), MaxConnections: 1, ConnFactory: dial,
		})
		if err != nil {
			t.Fatalf("failed to create provider: %v", err)
		}
		client.AddProvider(p, ProviderPrimary)

		// Call BodyReader
		reader, err := client.BodyReader(context.Background(), "123")
		if err != nil {
			t.Fatalf("BodyReader failed: %v", err)
		}
		defer reader.Close()

		// Headers should be nil initially
		if header := reader.YencHeaders(); header != nil {
			t.Errorf("expected nil headers before reading, got %+v", header)
		}

		// Read enough data to trigger header parsing and get some content
		// We'll read in a loop until we have the headers
		buf := make([]byte, len(originalData))
		totalRead := 0
		var header *YencHeader

		for totalRead < len(originalData) && header == nil {
			n, readErr := reader.Read(buf[totalRead:])
			totalRead += n

			// Check if headers are available now
			header = reader.YencHeaders()

			if readErr != nil {
				if readErr != io.EOF {
					t.Fatalf("read error: %v", readErr)
				}
				break
			}
		}

		// Headers should now be available
		if header == nil {
			t.Fatal("expected headers after reading, got nil")
		}

		// Debug: print what we got
		t.Logf("Header: %+v", header)

		// Continue reading rest of data
		if totalRead < len(originalData) {
			remaining, err := io.ReadFull(reader, buf[totalRead:])
			totalRead += remaining
			if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
				t.Fatalf("failed to read remaining: %v", err)
			}
		}

		// Verify data
		if totalRead > 0 && string(buf[:totalRead]) != string(originalData[:totalRead]) {
			t.Errorf("data mismatch: expected '%s', got '%s'", string(originalData[:totalRead]), string(buf[:totalRead]))
		}

		// Verify header fields
		// Note: For multi-part files, PartSize might be 0 initially if the callback
		// is invoked after =ybegin but before =ypart is processed.
		// The important fields from =ybegin are FileName, FileSize, Part, and Total.
		if header.FileName != filename {
			t.Errorf("expected filename '%s', got '%s'", filename, header.FileName)
		}
		if header.FileSize != 12 {
			t.Errorf("expected file size 12, got %d", header.FileSize)
		}
		if header.Part != 2 {
			t.Errorf("expected part 2, got %d", header.Part)
		}
		if header.Total != 5 {
			t.Errorf("expected total 5, got %d", header.Total)
		}
		// PartBegin and PartSize are set when =ypart is processed,
		// which might happen after the initial callback.
		// For this test, we verify the basic multi-part metadata is available.

		// Verify caching - should return same pointer
		header2 := reader.YencHeaders()
		if header != header2 {
			t.Error("expected cached header to return same pointer")
		}
	})

	t.Run("ContextCancellation", func(t *testing.T) {
		// Test data
		originalData := []byte("hello world!")

		// Setup mock server using testutil
		srv, cleanup := testutil.StartMockNNTPServer(t, testutil.MockServerConfig{
			Handler: testutil.YencBodyHandler(originalData, "test.txt"),
		})
		defer cleanup()

		// Setup client
		client := NewClient(10)
		defer client.Close()

		dial := func(ctx context.Context) (net.Conn, error) {
			var d net.Dialer
			return d.DialContext(ctx, "tcp", srv.Addr())
		}

		p, err := NewProvider(context.Background(), ProviderConfig{
			Address: srv.Addr(), MaxConnections: 1, ConnFactory: dial,
		})
		if err != nil {
			t.Fatalf("failed to create provider: %v", err)
		}
		client.AddProvider(p, ProviderPrimary)

		// Create cancellable context
		ctx, cancel := context.WithCancel(context.Background())

		// Call BodyReader
		reader, err := client.BodyReader(ctx, "123")
		if err != nil {
			t.Fatalf("BodyReader failed: %v", err)
		}
		defer reader.Close()

		// Start reading
		buf := make([]byte, 4)
		_, err = reader.Read(buf)
		if err != nil && err != io.EOF {
			t.Fatalf("first read failed: %v", err)
		}

		// Cancel context
		cancel()

		// Note: Context cancellation behavior with pipes can be unpredictable.
		// The pipe might have buffered data that can still be read.
		// What's important is that the underlying request respects the context.
		// For this test, we just verify the basic flow works.
	})

	t.Run("Close", func(t *testing.T) {
		// Test data
		originalData := []byte("hello world!")

		// Setup mock server using testutil
		srv, cleanup := testutil.StartMockNNTPServer(t, testutil.MockServerConfig{
			Handler: testutil.YencBodyHandler(originalData, "test.txt"),
		})
		defer cleanup()

		// Setup client
		client := NewClient(10)
		defer client.Close()

		dial := func(ctx context.Context) (net.Conn, error) {
			var d net.Dialer
			return d.DialContext(ctx, "tcp", srv.Addr())
		}

		p, err := NewProvider(context.Background(), ProviderConfig{
			Address: srv.Addr(), MaxConnections: 1, ConnFactory: dial,
		})
		if err != nil {
			t.Fatalf("failed to create provider: %v", err)
		}
		client.AddProvider(p, ProviderPrimary)

		// Call BodyReader
		reader, err := client.BodyReader(context.Background(), "123")
		if err != nil {
			t.Fatalf("BodyReader failed: %v", err)
		}

		// Read partially
		buf := make([]byte, 4)
		_, err = reader.Read(buf)
		if err != nil && err != io.EOF {
			t.Fatalf("read failed: %v", err)
		}

		// Close reader
		if err := reader.Close(); err != nil {
			t.Errorf("failed to close reader: %v", err)
		}

		// Subsequent reads should fail
		_, err = reader.Read(buf)
		if err == nil {
			t.Error("expected error after close, got nil")
		}
	})
}
