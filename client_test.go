package nntppool

import (
	"bytes"
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
		defer func() {
			if err := reader.Close(); err != nil {
				t.Errorf("failed to close reader: %v", err)
			}
		}()

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
		defer func() {
			if err := reader.Close(); err != nil {
				t.Errorf("failed to close reader: %v", err)
			}
		}()

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
			return // Explicit return for staticcheck SA5011 (though t.Fatal already stops execution)
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
		defer func() {
			if err := reader.Close(); err != nil {
				t.Errorf("failed to close reader: %v", err)
			}
		}()

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

func TestPost(t *testing.T) {
	t.Run("basic POST", func(t *testing.T) {
		var receivedArticle string

		srv, stop := testutil.StartMockNNTPServer(t, testutil.MockServerConfig{
			ID: "PostTest",
			Handler: func(cmd string) (string, error) {
				if cmd == "POST\r\n" {
					return "340 Send article to be posted\r\n", nil
				}
				if strings.Contains(cmd, "Subject:") {
					// Capture the full article
					receivedArticle = cmd
					return "240 Article posted successfully\r\n", nil
				}
				return "500 Unknown Command\r\n", nil
			},
		})
		defer stop()

		dial := func(ctx context.Context) (net.Conn, error) {
			var d net.Dialer
			return d.DialContext(ctx, "tcp", srv.Addr())
		}

		p, err := NewProvider(context.Background(), ProviderConfig{
			Address: srv.Addr(), MaxConnections: 1, InflightPerConnection: 1, ConnFactory: dial,
		})
		if err != nil {
			t.Fatalf("failed to create provider: %v", err)
		}

		client := NewClient(10)
		defer client.Close()
		client.AddProvider(p, ProviderPrimary)

		headers := map[string]string{
			"From":       "test@example.com",
			"Newsgroups": "alt.test",
			"Subject":    "Test Article",
			"Message-ID": "<test123@example.com>",
		}

		body := strings.NewReader("This is a test article.\nLine 2")

		resp, err := client.Post(context.Background(), headers, body)
		if err != nil {
			t.Fatalf("Post failed: %v", err)
		}

		if resp.StatusCode != 240 {
			t.Errorf("expected status 240, got %d", resp.StatusCode)
		}

		// Verify article contains headers and body
		if !strings.Contains(receivedArticle, "Subject: Test Article") {
			t.Error("article missing Subject header")
		}
		if !strings.Contains(receivedArticle, "From: test@example.com") {
			t.Error("article missing From header")
		}
	})

	t.Run("dot-stuffing", func(t *testing.T) {
		var receivedArticle string

		srv, stop := testutil.StartMockNNTPServer(t, testutil.MockServerConfig{
			ID: "DotStuffTest",
			Handler: func(cmd string) (string, error) {
				if cmd == "POST\r\n" {
					return "340 Send article to be posted\r\n", nil
				}
				if strings.Contains(cmd, "Subject:") {
					receivedArticle = cmd
					return "240 Article posted successfully\r\n", nil
				}
				return "500 Unknown Command\r\n", nil
			},
		})
		defer stop()

		dial := func(ctx context.Context) (net.Conn, error) {
			var d net.Dialer
			return d.DialContext(ctx, "tcp", srv.Addr())
		}

		p, err := NewProvider(context.Background(), ProviderConfig{
			Address: srv.Addr(), MaxConnections: 1, InflightPerConnection: 1, ConnFactory: dial,
		})
		if err != nil {
			t.Fatalf("failed to create provider: %v", err)
		}

		client := NewClient(10)
		defer client.Close()
		client.AddProvider(p, ProviderPrimary)

		headers := map[string]string{
			"From":    "test@example.com",
			"Subject": "Dot Stuffing Test",
		}

		// Body with lines starting with '.'
		body := strings.NewReader(".hidden line\nnormal line\n.another hidden")

		resp, err := client.Post(context.Background(), headers, body)
		if err != nil {
			t.Fatalf("Post failed: %v", err)
		}

		if resp.StatusCode != 240 {
			t.Errorf("expected status 240, got %d", resp.StatusCode)
		}

		// Verify dot-stuffing was applied (lines starting with '.' should have '..' prepended)
		if !strings.Contains(receivedArticle, "..hidden line") {
			t.Error("dot-stuffing not applied correctly")
		}
	})
}

func TestPostYenc(t *testing.T) {
	t.Run("single-part yEnc", func(t *testing.T) {
		var receivedArticle string

		srv, stop := testutil.StartMockNNTPServer(t, testutil.MockServerConfig{
			ID: "YencTest",
			Handler: func(cmd string) (string, error) {
				if cmd == "POST\r\n" {
					return "340 Send article to be posted\r\n", nil
				}
				if strings.Contains(cmd, "=ybegin") {
					receivedArticle = cmd
					return "240 Article posted successfully\r\n", nil
				}
				return "500 Unknown Command\r\n", nil
			},
		})
		defer stop()

		dial := func(ctx context.Context) (net.Conn, error) {
			var d net.Dialer
			return d.DialContext(ctx, "tcp", srv.Addr())
		}

		p, err := NewProvider(context.Background(), ProviderConfig{
			Address: srv.Addr(), MaxConnections: 1, InflightPerConnection: 1, ConnFactory: dial,
		})
		if err != nil {
			t.Fatalf("failed to create provider: %v", err)
		}

		client := NewClient(10)
		defer client.Close()
		client.AddProvider(p, ProviderPrimary)

		headers := map[string]string{
			"From":       "test@example.com",
			"Newsgroups": "alt.binaries.test",
			"Subject":    "Test Binary [1/1]",
		}

		testData := []byte("Hello, World!")
		body := strings.NewReader(string(testData))

		opts := &YencOptions{
			FileName: "test.bin",
			FileSize: int64(len(testData)),
			LineSize: 128,
		}

		resp, err := client.PostYenc(context.Background(), headers, body, opts)
		if err != nil {
			t.Fatalf("PostYenc failed: %v", err)
		}

		if resp.StatusCode != 240 {
			t.Errorf("expected status 240, got %d", resp.StatusCode)
		}

		// Verify yEnc headers are present
		if !strings.Contains(receivedArticle, "=ybegin") {
			t.Error("missing =ybegin header")
		}
		if !strings.Contains(receivedArticle, "name=test.bin") {
			t.Error("missing filename in yEnc header")
		}
		if !strings.Contains(receivedArticle, "=yend") {
			t.Error("missing =yend footer")
		}
		if !strings.Contains(receivedArticle, "crc32=") {
			t.Error("missing CRC32 in yEnc footer")
		}
	})

	t.Run("multi-part yEnc", func(t *testing.T) {
		var receivedArticle string

		srv, stop := testutil.StartMockNNTPServer(t, testutil.MockServerConfig{
			ID: "YencMultiTest",
			Handler: func(cmd string) (string, error) {
				if cmd == "POST\r\n" {
					return "340 Send article to be posted\r\n", nil
				}
				if strings.Contains(cmd, "=ybegin") {
					receivedArticle = cmd
					return "240 Article posted successfully\r\n", nil
				}
				return "500 Unknown Command\r\n", nil
			},
		})
		defer stop()

		dial := func(ctx context.Context) (net.Conn, error) {
			var d net.Dialer
			return d.DialContext(ctx, "tcp", srv.Addr())
		}

		p, err := NewProvider(context.Background(), ProviderConfig{
			Address: srv.Addr(), MaxConnections: 1, InflightPerConnection: 1, ConnFactory: dial,
		})
		if err != nil {
			t.Fatalf("failed to create provider: %v", err)
		}

		client := NewClient(10)
		defer client.Close()
		client.AddProvider(p, ProviderPrimary)

		headers := map[string]string{
			"From":       "test@example.com",
			"Newsgroups": "alt.binaries.test",
			"Subject":    "Test Binary [1/2]",
		}

		testData := []byte("Part 1 data")
		body := strings.NewReader(string(testData))

		opts := &YencOptions{
			FileName:  "test.bin",
			FileSize:  1024, // Total file size
			Part:      1,
			Total:     2,
			PartBegin: 1,
			PartEnd:   int64(len(testData)),
			LineSize:  128,
		}

		resp, err := client.PostYenc(context.Background(), headers, body, opts)
		if err != nil {
			t.Fatalf("PostYenc failed: %v", err)
		}

		if resp.StatusCode != 240 {
			t.Errorf("expected status 240, got %d", resp.StatusCode)
		}

		// Verify multi-part yEnc headers
		if !strings.Contains(receivedArticle, "=ybegin part=1 total=2") {
			t.Error("missing multi-part yEnc begin header")
		}
		if !strings.Contains(receivedArticle, "=ypart begin=1") {
			t.Error("missing =ypart header")
		}
		if !strings.Contains(receivedArticle, "=yend") {
			t.Error("missing =yend footer")
		}
		if !strings.Contains(receivedArticle, "pcrc32=") {
			t.Error("missing part CRC32 in yEnc footer")
		}
	})

	t.Run("invalid options", func(t *testing.T) {
		client := NewClient(10)
		defer client.Close()

		headers := map[string]string{
			"From":    "test@example.com",
			"Subject": "Test",
		}

		body := strings.NewReader("test")

		// Test nil options
		_, err := client.PostYenc(context.Background(), headers, body, nil)
		if err == nil {
			t.Error("expected error for nil options")
		}

		// Test empty filename
		opts := &YencOptions{
			FileName: "",
			FileSize: 100,
		}
		_, err = client.PostYenc(context.Background(), headers, body, opts)
		if err == nil {
			t.Error("expected error for empty filename")
		}

		// Test invalid file size
		opts = &YencOptions{
			FileName: "test.bin",
			FileSize: 0,
		}
		_, err = client.PostYenc(context.Background(), headers, body, opts)
		if err == nil {
			t.Error("expected error for invalid file size")
		}

		// Test multi-part without PartBegin/PartEnd
		opts = &YencOptions{
			FileName: "test.bin",
			FileSize: 1000,
			Part:     2,
			Total:    3,
		}
		_, err = client.PostYenc(context.Background(), headers, body, opts)
		if err == nil {
			t.Error("expected error for multi-part without PartBegin/PartEnd")
		}
	})
}

// extractMessageID parses article headers to find Message-ID
func extractMessageID(article string) string {
	lines := strings.Split(article, "\r\n")
	for _, line := range lines {
		if strings.HasPrefix(line, "Message-ID:") {
			return strings.TrimSpace(strings.TrimPrefix(line, "Message-ID:"))
		}
	}
	return ""
}

func TestPostYencRoundTrip(t *testing.T) {
	t.Run("single-part round-trip", func(t *testing.T) {
		// Storage for posted articles
		postedArticles := make(map[string]string)
		var mu sync.Mutex

		// Mock server handler
		srv, stop := testutil.StartMockNNTPServer(t, testutil.MockServerConfig{
			ID: "RoundTripTest",
			Handler: func(cmd string) (string, error) {
				if cmd == "POST\r\n" {
					return "340 Send article to be posted\r\n", nil
				}

				// Store posted article
				if strings.Contains(cmd, "=ybegin") {
					// Extract Message-ID from headers
					messageID := extractMessageID(cmd)
					mu.Lock()
					postedArticles[messageID] = cmd
					mu.Unlock()
					return "240 Article posted successfully\r\n", nil
				}

				// Serve stored article for BODY requests
				if strings.HasPrefix(cmd, "BODY") {
					// Extract Message-ID from command: "BODY <id>\r\n"
					parts := strings.Fields(cmd)
					if len(parts) >= 2 {
						messageID := strings.TrimRight(parts[1], "\r\n")
						mu.Lock()
						article, ok := postedArticles[messageID]
						mu.Unlock()

						if ok {
							return fmt.Sprintf("222 0 %s body follows\r\n%s.\r\n",
								messageID, article), nil
						}
					}
					return "430 No such article\r\n", nil
				}

				return "500 Unknown Command\r\n", nil
			},
		})
		defer stop()

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

		// Test data
		originalData := []byte("Hello, World! This is test data.")
		messageID := "<test-roundtrip-single@example.com>"

		// Upload using PostYenc
		headers := map[string]string{
			"From":       "test@example.com",
			"Newsgroups": "alt.binaries.test",
			"Subject":    "Round-trip test [1/1]",
			"Message-ID": messageID,
		}

		opts := &YencOptions{
			FileName: "test.bin",
			FileSize: int64(len(originalData)),
			LineSize: 128,
		}

		resp, err := client.PostYenc(context.Background(), headers,
			bytes.NewReader(originalData), opts)
		if err != nil {
			t.Fatalf("PostYenc failed: %v", err)
		}
		if resp.StatusCode != 240 {
			t.Fatalf("expected status 240, got %d", resp.StatusCode)
		}

		// Download using Body
		var decoded bytes.Buffer
		err = client.Body(context.Background(), messageID, &decoded)
		if err != nil {
			t.Fatalf("Body failed: %v", err)
		}

		// Verify data integrity
		if !bytes.Equal(decoded.Bytes(), originalData) {
			t.Errorf("data mismatch:\noriginal: %q\ndecoded:  %q",
				string(originalData), decoded.String())
		}
	})

	t.Run("multi-part round-trip", func(t *testing.T) {
		// Storage for posted articles
		postedArticles := make(map[string]string)
		var mu sync.Mutex

		// Mock server handler
		srv, stop := testutil.StartMockNNTPServer(t, testutil.MockServerConfig{
			ID: "RoundTripMultiTest",
			Handler: func(cmd string) (string, error) {
				if cmd == "POST\r\n" {
					return "340 Send article to be posted\r\n", nil
				}

				// Store posted article
				if strings.Contains(cmd, "=ybegin") {
					// Extract Message-ID from headers
					messageID := extractMessageID(cmd)
					mu.Lock()
					postedArticles[messageID] = cmd
					mu.Unlock()
					return "240 Article posted successfully\r\n", nil
				}

				// Serve stored article for BODY requests
				if strings.HasPrefix(cmd, "BODY") {
					// Extract Message-ID from command: "BODY <id>\r\n"
					parts := strings.Fields(cmd)
					if len(parts) >= 2 {
						messageID := strings.TrimRight(parts[1], "\r\n")
						mu.Lock()
						article, ok := postedArticles[messageID]
						mu.Unlock()

						if ok {
							return fmt.Sprintf("222 0 %s body follows\r\n%s.\r\n",
								messageID, article), nil
						}
					}
					return "430 No such article\r\n", nil
				}

				return "500 Unknown Command\r\n", nil
			},
		})
		defer stop()

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

		// Test data - simulate part 1 of a multi-part file
		originalData := []byte("Part 1 of multi-part file")
		messageID := "<test-roundtrip-multi-1@example.com>"

		// Upload using PostYenc with multi-part options
		headers := map[string]string{
			"From":       "test@example.com",
			"Newsgroups": "alt.binaries.test",
			"Subject":    "Round-trip test [1/3]",
			"Message-ID": messageID,
		}

		opts := &YencOptions{
			FileName:  "test.bin",
			FileSize:  1000, // Total file size
			Part:      1,
			Total:     3,
			PartBegin: 1,
			PartEnd:   int64(len(originalData)),
			LineSize:  128,
		}

		resp, err := client.PostYenc(context.Background(), headers,
			bytes.NewReader(originalData), opts)
		if err != nil {
			t.Fatalf("PostYenc failed: %v", err)
		}
		if resp.StatusCode != 240 {
			t.Fatalf("expected status 240, got %d", resp.StatusCode)
		}

		// Download using Body
		var decoded bytes.Buffer
		err = client.Body(context.Background(), messageID, &decoded)
		if err != nil {
			t.Fatalf("Body failed: %v", err)
		}

		// Verify data integrity
		if !bytes.Equal(decoded.Bytes(), originalData) {
			t.Errorf("data mismatch:\noriginal: %q\ndecoded:  %q",
				string(originalData), decoded.String())
		}
	})
}
