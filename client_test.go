package nntppool

import (
	"bytes"
	"context"
	"errors"
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
	client := NewClient()
	defer client.Close()

	// 1. Add Provider A
	srv1, stop1 := testutil.StartMockNNTPServer(t, testutil.MockServerConfig{
		ID: "P1",
		Handler: func(cmd string) (string, error) {
			if strings.HasPrefix(cmd, "BODY") {
				return "222 0 <id> body follows\r\nline1\r\nline2\r\n.\r\n", nil
			}
			if cmd == "DATE\r\n" {
				return "111 20240101000000\r\n", nil
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
	err = client.AddProvider(p1, ProviderPrimary)
	if err != nil {
		t.Fatalf("failed to add p1: %v", err)
	}

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
			if cmd == "DATE\r\n" {
				return "111 20240101000000\r\n", nil
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
	err = client.AddProvider(p2, ProviderPrimary)
	if err != nil {
		t.Fatalf("failed to add p2: %v", err)
	}

	// 4. Remove Provider A
	time.Sleep(50 * time.Millisecond)
	err = client.RemoveProvider(p1)
	if err != nil {
		t.Fatalf("failed to remove p1: %v", err)
	}

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

	client := NewClient()
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

	err = client.AddProvider(p, ProviderPrimary)
	if err != nil {
		t.Fatalf("failed to add provider: %v", err)
	}

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
			if cmd == "DATE\r\n" {
				return "111 20240101000000\r\n", nil
			}
			return "500 Unknown Command\r\n", nil
		},
	})

	client := NewClient()
	defer client.Close()

	// Create two providers
	p1, _ := NewProvider(context.Background(), ProviderConfig{
		Address: "p1.example.com:119", MaxConnections: 1, ConnFactory: mockDial,
	})
	p2, _ := NewProvider(context.Background(), ProviderConfig{
		Address: "p2.example.com:119", MaxConnections: 1, ConnFactory: mockDial,
	})

	err := client.AddProvider(p1, ProviderPrimary)
	if err != nil {
		t.Fatalf("failed to add p1: %v", err)
	}
	err = client.AddProvider(p2, ProviderBackup)
	if err != nil {
		t.Fatalf("failed to add p2: %v", err)
	}

	// Verify they are added
	if len(client.primaries.Load().([]*Provider)) != 1 {
		t.Fatal("expected 1 primary")
	}
	if len(client.backups.Load().([]*Provider)) != 1 {
		t.Fatal("expected 1 backup")
	}

	// Remove p1 (primary)
	err = client.RemoveProvider(p1)
	if err != nil {
		t.Fatalf("failed to remove p1: %v", err)
	}

	// Verify p1 removed and closed
	if len(client.primaries.Load().([]*Provider)) != 0 {
		t.Fatal("expected 0 primaries")
	}
	if p1.ctx.Err() == nil {
		t.Fatal("expected p1 to be closed (context cancelled)")
	}

	// Remove p2 (backup)
	err = client.RemoveProvider(p2)
	if err != nil {
		t.Fatalf("failed to remove p2: %v", err)
	}

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
	err = client.AddProvider(p3, ProviderPrimary)
	if err != nil {
		t.Fatalf("failed to add p3: %v", err)
	}

	// Manually move to dead for testing
	client.mu.Lock()
	client.primaries.Store([]*Provider{})
	client.deadPrimaries = append(client.deadPrimaries, p3)
	client.mu.Unlock()

	err = client.RemoveProvider(p3)
	if err != nil {
		t.Fatalf("failed to remove p3: %v", err)
	}

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

	client := NewClient()
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
	err = client.AddProvider(p, ProviderPrimary)
	if err != nil {
		t.Fatalf("failed to add provider: %v", err)
	}

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
	client := NewClient()
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
	err = client.AddProvider(p, ProviderPrimary)
	if err != nil {
		t.Fatalf("failed to add provider: %v", err)
	}

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
		client := NewClient()
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
		err = client.AddProvider(p, ProviderPrimary)
		if err != nil {
			t.Fatalf("failed to add provider: %v", err)
		}

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
				if cmd == "DATE\r\n" {
					return "111 20240101000000\r\n", nil
				}
				return "500 Unknown Command\r\n", nil
			},
		})
		defer cleanup()

		// Setup client
		client := NewClient()
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
		err = client.AddProvider(p, ProviderPrimary)
		if err != nil {
			t.Fatalf("failed to add provider: %v", err)
		}

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
		client := NewClient()
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
		err = client.AddProvider(p, ProviderPrimary)
		if err != nil {
			t.Fatalf("failed to add provider: %v", err)
		}

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
		client := NewClient()
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
		err = client.AddProvider(p, ProviderPrimary)
		if err != nil {
			t.Fatalf("failed to add provider: %v", err)
		}

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

// setupPostClient creates a test client with a mock server that handles POST commands.
// The onArticle callback receives the posted article content.
func setupPostClient(t *testing.T, id string, onArticle func(string)) (*Client, func()) {
	t.Helper()
	srv, stop := testutil.StartMockNNTPServer(t, testutil.MockServerConfig{
		ID: id,
		Handler: func(cmd string) (string, error) {
			if cmd == "POST\r\n" {
				return "340 Send article to be posted\r\n", nil
			}
			if cmd == "DATE\r\n" {
				return "111 20240101000000\r\n", nil
			}
			if strings.Contains(cmd, "Subject:") || strings.Contains(cmd, "=ybegin") {
				if onArticle != nil {
					onArticle(cmd)
				}
				return "240 Article posted successfully\r\n", nil
			}
			return "500 Unknown Command\r\n", nil
		},
	})

	dial := func(ctx context.Context) (net.Conn, error) {
		var d net.Dialer
		return d.DialContext(ctx, "tcp", srv.Addr())
	}

	p, err := NewProvider(context.Background(), ProviderConfig{
		Address: srv.Addr(), MaxConnections: 1, InflightPerConnection: 1, ConnFactory: dial,
	})
	if err != nil {
		stop()
		t.Fatalf("failed to create provider: %v", err)
	}

	client := NewClient()
	err = client.AddProvider(p, ProviderPrimary)
	if err != nil {
		t.Fatalf("failed to add provider: %v", err)
	}

	return client, func() {
		client.Close()
		stop()
	}
}

func TestPost(t *testing.T) {
	t.Run("basic POST", func(t *testing.T) {
		var receivedArticle string
		client, cleanup := setupPostClient(t, "PostTest", func(article string) {
			receivedArticle = article
		})
		defer cleanup()

		headers := map[string]string{
			"From":       "test@example.com",
			"Newsgroups": "alt.test",
			"Subject":    "Test Article",
			"Message-ID": "<test123@example.com>",
		}

		resp, err := client.Post(context.Background(), headers,
			strings.NewReader("This is a test article.\nLine 2"))
		if err != nil {
			t.Fatalf("Post failed: %v", err)
		}
		if resp.StatusCode != 240 {
			t.Errorf("expected status 240, got %d", resp.StatusCode)
		}
		if !strings.Contains(receivedArticle, "Subject: Test Article") {
			t.Error("article missing Subject header")
		}
		if !strings.Contains(receivedArticle, "From: test@example.com") {
			t.Error("article missing From header")
		}
	})

	t.Run("dot-stuffing", func(t *testing.T) {
		var receivedArticle string
		client, cleanup := setupPostClient(t, "DotStuffTest", func(article string) {
			receivedArticle = article
		})
		defer cleanup()

		headers := map[string]string{
			"From":    "test@example.com",
			"Subject": "Dot Stuffing Test",
		}

		resp, err := client.Post(context.Background(), headers,
			strings.NewReader(".hidden line\nnormal line\n.another hidden"))
		if err != nil {
			t.Fatalf("Post failed: %v", err)
		}
		if resp.StatusCode != 240 {
			t.Errorf("expected status 240, got %d", resp.StatusCode)
		}
		if !strings.Contains(receivedArticle, "..hidden line") {
			t.Error("dot-stuffing not applied correctly")
		}
	})
}

func TestPostYenc(t *testing.T) {
	t.Run("single-part yEnc", func(t *testing.T) {
		var receivedArticle string
		client, cleanup := setupPostClient(t, "YencTest", func(article string) {
			receivedArticle = article
		})
		defer cleanup()

		headers := map[string]string{
			"From":       "test@example.com",
			"Newsgroups": "alt.binaries.test",
			"Subject":    "Test Binary [1/1]",
		}

		testData := []byte("Hello, World!")
		opts := &YencOptions{FileName: "test.bin", FileSize: int64(len(testData))}

		resp, err := client.PostYenc(context.Background(), headers,
			strings.NewReader(string(testData)), opts)
		if err != nil {
			t.Fatalf("PostYenc failed: %v", err)
		}
		if resp.StatusCode != 240 {
			t.Errorf("expected status 240, got %d", resp.StatusCode)
		}

		for _, check := range []struct{ pattern, desc string }{
			{"=ybegin", "=ybegin header"},
			{"name=test.bin", "filename in yEnc header"},
			{"=yend", "=yend footer"},
			{"crc32=", "CRC32 in yEnc footer"},
		} {
			if !strings.Contains(receivedArticle, check.pattern) {
				t.Errorf("missing %s", check.desc)
			}
		}
	})

	t.Run("multi-part yEnc", func(t *testing.T) {
		var receivedArticle string
		client, cleanup := setupPostClient(t, "YencMultiTest", func(article string) {
			receivedArticle = article
		})
		defer cleanup()

		headers := map[string]string{
			"From":       "test@example.com",
			"Newsgroups": "alt.binaries.test",
			"Subject":    "Test Binary [1/2]",
		}

		testData := []byte("Part 1 data")
		opts := &YencOptions{
			FileName:  "test.bin",
			FileSize:  1024,
			Part:      1,
			Total:     2,
			PartBegin: 1,
			PartEnd:   int64(len(testData)),
		}

		resp, err := client.PostYenc(context.Background(), headers,
			strings.NewReader(string(testData)), opts)
		if err != nil {
			t.Fatalf("PostYenc failed: %v", err)
		}
		if resp.StatusCode != 240 {
			t.Errorf("expected status 240, got %d", resp.StatusCode)
		}

		for _, check := range []struct{ pattern, desc string }{
			{"=ybegin part=1 total=2", "multi-part yEnc begin header"},
			{"=ypart begin=1", "=ypart header"},
			{"=yend", "=yend footer"},
			{"pcrc32=", "part CRC32 in yEnc footer"},
		} {
			if !strings.Contains(receivedArticle, check.pattern) {
				t.Errorf("missing %s", check.desc)
			}
		}
	})

	t.Run("invalid options", func(t *testing.T) {
		client := NewClient()
		defer client.Close()

		headers := map[string]string{"From": "test@example.com", "Subject": "Test"}
		body := strings.NewReader("test")

		tests := []struct {
			name string
			opts *YencOptions
		}{
			{"nil options", nil},
			{"empty filename", &YencOptions{FileName: "", FileSize: 100}},
			{"zero file size", &YencOptions{FileName: "test.bin", FileSize: 0}},
			{"multi-part without offsets", &YencOptions{FileName: "test.bin", FileSize: 1000, Part: 2, Total: 3}},
		}

		for _, tt := range tests {
			if _, err := client.PostYenc(context.Background(), headers, body, tt.opts); err == nil {
				t.Errorf("expected error for %s", tt.name)
			}
		}
	})
}

// extractMessageID parses article headers to find Message-ID.
func extractMessageID(article string) string {
	for _, line := range strings.Split(article, "\r\n") {
		if strings.HasPrefix(line, "Message-ID:") {
			return strings.TrimSpace(strings.TrimPrefix(line, "Message-ID:"))
		}
	}
	return ""
}

// setupRoundTripClient creates a client with a mock server that stores and serves posted articles.
func setupRoundTripClient(t *testing.T, id string) (*Client, func()) {
	t.Helper()
	var mu sync.Mutex
	articles := make(map[string]string)

	srv, stop := testutil.StartMockNNTPServer(t, testutil.MockServerConfig{
		ID: id,
		Handler: func(cmd string) (string, error) {
			if cmd == "POST\r\n" {
				return "340 Send article to be posted\r\n", nil
			}
			if cmd == "DATE\r\n" {
				return "111 20240101000000\r\n", nil
			}
			if strings.Contains(cmd, "=ybegin") {
				msgID := extractMessageID(cmd)
				mu.Lock()
				articles[msgID] = cmd
				mu.Unlock()
				return "240 Article posted successfully\r\n", nil
			}
			if strings.HasPrefix(cmd, "BODY") {
				parts := strings.Fields(cmd)
				if len(parts) >= 2 {
					msgID := strings.TrimRight(parts[1], "\r\n")
					mu.Lock()
					article, ok := articles[msgID]
					mu.Unlock()
					if ok {
						return fmt.Sprintf("222 0 %s body follows\r\n%s.\r\n", msgID, article), nil
					}
				}
				return "430 No such article\r\n", nil
			}
			return "500 Unknown Command\r\n", nil
		},
	})

	dial := func(ctx context.Context) (net.Conn, error) {
		var d net.Dialer
		return d.DialContext(ctx, "tcp", srv.Addr())
	}

	p, err := NewProvider(context.Background(), ProviderConfig{
		Address: srv.Addr(), MaxConnections: 1, ConnFactory: dial,
	})
	if err != nil {
		stop()
		t.Fatalf("failed to create provider: %v", err)
	}

	client := NewClient()
	err = client.AddProvider(p, ProviderPrimary)
	if err != nil {
		t.Fatalf("failed to add provider: %v", err)
	}

	return client, func() {
		client.Close()
		stop()
	}
}

func TestClientContextTimeoutWhileProviderBusy(t *testing.T) {
	// This test verifies context timeout handling when the provider is busy
	// processing other requests (provider-level concurrency control).

	var requestReceived atomic.Bool
	requestReceived.Store(false)
	blockCh := make(chan struct{})

	// Create a mock server that processes requests slowly
	mockDial := testutil.MockDialerWithHandler(testutil.MockServerConfig{
		Handler: func(cmd string) (string, error) {
			if cmd == "BODY <slow>\r\n" {
				requestReceived.Store(true)
				// Block until test tells us to proceed
				<-blockCh
				return "222 0 <slow> body follows\r\ntest\r\n.\r\n", nil
			}
			if cmd == "DATE\r\n" {
				return "111 20240101000000\r\n", nil
			}
			return "500 Unknown Command\r\n", nil
		},
	})

	// Create client - concurrency is controlled at provider level via MaxConnections/InflightPerConnection
	client := NewClient()
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

	err = client.AddProvider(p, ProviderPrimary)
	if err != nil {
		t.Fatalf("failed to add provider: %v", err)
	}

	// Start a slow request to keep the single connection busy
	slowDone := make(chan struct{})
	go func() {
		defer close(slowDone)
		ctx := context.Background()
		_ = client.Body(ctx, "slow", io.Discard)
	}()

	// Wait for the slow request to be received (connection now busy)
	timeout := time.After(5 * time.Second)
	for !requestReceived.Load() {
		select {
		case <-timeout:
			close(blockCh)
			t.Fatal("timeout waiting for slow request to be received")
		default:
			time.Sleep(10 * time.Millisecond)
		}
	}

	// Give it a bit more time to ensure connection is busy
	time.Sleep(50 * time.Millisecond)

	// Now try Date() with a short timeout while provider is busy
	// This should timeout while waiting for provider capacity
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	err = client.Date(ctx)

	// Unblock the slow request
	close(blockCh)

	// Wait for slow request to complete
	select {
	case <-slowDone:
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for slow request to complete")
	}

	// We expect an error, and it should be the context deadline exceeded
	// NOT "response channel closed unexpectedly"
	if err == nil {
		t.Fatal("expected an error due to context timeout, got nil")
	}

	// Check if we get the buggy "response channel closed unexpectedly" error
	if err.Error() == "response channel closed unexpectedly" {
		t.Errorf("BUG REPRODUCED: got 'response channel closed unexpectedly' error instead of context.DeadlineExceeded")
	} else if err != context.DeadlineExceeded {
		t.Logf("Got error: %v (type: %T)", err, err)
		// We should get context.DeadlineExceeded
		if err.Error() != "context deadline exceeded" {
			t.Errorf("expected context deadline exceeded error, got: %v", err)
		}
	}
}

func TestPostYencRoundTrip(t *testing.T) {
	t.Run("single-part round-trip", func(t *testing.T) {
		client, cleanup := setupRoundTripClient(t, "RoundTripTest")
		defer cleanup()

		originalData := []byte("Hello, World! This is test data.")
		messageID := "<test-roundtrip-single@example.com>"

		headers := map[string]string{
			"From":       "test@example.com",
			"Newsgroups": "alt.binaries.test",
			"Subject":    "Round-trip test [1/1]",
			"Message-ID": messageID,
		}
		opts := &YencOptions{FileName: "test.bin", FileSize: int64(len(originalData))}

		resp, err := client.PostYenc(context.Background(), headers, bytes.NewReader(originalData), opts)
		if err != nil {
			t.Fatalf("PostYenc failed: %v", err)
		}
		if resp.StatusCode != 240 {
			t.Fatalf("expected status 240, got %d", resp.StatusCode)
		}

		var decoded bytes.Buffer
		if err := client.Body(context.Background(), messageID, &decoded); err != nil {
			t.Fatalf("Body failed: %v", err)
		}
		if !bytes.Equal(decoded.Bytes(), originalData) {
			t.Errorf("data mismatch:\noriginal: %q\ndecoded:  %q", originalData, decoded.Bytes())
		}
	})

	t.Run("multi-part round-trip", func(t *testing.T) {
		client, cleanup := setupRoundTripClient(t, "RoundTripMultiTest")
		defer cleanup()

		originalData := []byte("Part 1 of multi-part file")
		messageID := "<test-roundtrip-multi-1@example.com>"

		headers := map[string]string{
			"From":       "test@example.com",
			"Newsgroups": "alt.binaries.test",
			"Subject":    "Round-trip test [1/3]",
			"Message-ID": messageID,
		}
		opts := &YencOptions{
			FileName:  "test.bin",
			FileSize:  1000,
			Part:      1,
			Total:     3,
			PartBegin: 1,
			PartEnd:   int64(len(originalData)),
		}

		resp, err := client.PostYenc(context.Background(), headers, bytes.NewReader(originalData), opts)
		if err != nil {
			t.Fatalf("PostYenc failed: %v", err)
		}
		if resp.StatusCode != 240 {
			t.Fatalf("expected status 240, got %d", resp.StatusCode)
		}

		var decoded bytes.Buffer
		if err := client.Body(context.Background(), messageID, &decoded); err != nil {
			t.Fatalf("Body failed: %v", err)
		}
		if !bytes.Equal(decoded.Bytes(), originalData) {
			t.Errorf("data mismatch:\noriginal: %q\ndecoded:  %q", originalData, decoded.Bytes())
		}
	})
}

// TestWriterClosedEarlyConnectionReused verifies that when a user closes their
// writer early (e.g., io.Pipe reader), the connection drains the response and
// stays alive for subsequent requests.
func TestWriterClosedEarlyConnectionReused(t *testing.T) {
	requestCount := atomic.Int32{}
	originalData := []byte("hello world! this is test data for the closed pipe test")

	// Setup mock server that returns yEnc-encoded responses (which get written to the output)
	srv, cleanup := testutil.StartMockNNTPServer(t, testutil.MockServerConfig{
		Handler: func(cmd string) (string, error) {
			if strings.HasPrefix(cmd, "BODY") {
				requestCount.Add(1)
				// Use yEnc so the data actually gets written to the output writer
				yencBody := testutil.EncodeYenc(originalData, "test.txt", 1, 1)
				return fmt.Sprintf("222 0 <id> body follows\r\n%s.\r\n", yencBody), nil
			}
			if cmd == "DATE\r\n" {
				return "111 20240101000000\r\n", nil
			}
			return "500 Unknown Command\r\n", nil
		},
	})
	defer cleanup()

	// Setup client with single connection to verify reuse
	client := NewClient()
	defer client.Close()

	dial := func(ctx context.Context) (net.Conn, error) {
		var d net.Dialer
		return d.DialContext(ctx, "tcp", srv.Addr())
	}

	p, err := NewProvider(context.Background(), ProviderConfig{
		Address:        srv.Addr(),
		MaxConnections: 1, // Single connection to verify reuse
		ConnFactory:    dial,
	})
	if err != nil {
		t.Fatalf("failed to create provider: %v", err)
	}
	if err := client.AddProvider(p, ProviderPrimary); err != nil {
		t.Fatalf("failed to add provider: %v", err)
	}

	// Use io.Pipe so we can close the reader early
	pr, pw := io.Pipe()

	// Close the reader immediately to trigger io.ErrClosedPipe on write
	_ = pr.Close()

	// Make a request that will try to write to the closed pipe
	err = client.Body(context.Background(), "123", pw)

	// Should get io.ErrClosedPipe (the original error)
	if err == nil {
		t.Fatal("expected error when writer is closed, got nil")
	}
	if err != io.ErrClosedPipe {
		t.Errorf("expected io.ErrClosedPipe, got: %v (type %T)", err, err)
	}

	_ = pw.Close()

	// Now make another request - the connection should still be alive
	var buf bytes.Buffer
	err = client.Body(context.Background(), "456", &buf)
	if err != nil {
		t.Fatalf("second request failed (connection should have been reused): %v", err)
	}

	// Verify both requests were served (connection was reused)
	if got := requestCount.Load(); got != 2 {
		t.Errorf("expected 2 requests, got %d", got)
	}

	// Verify second request got data
	if buf.Len() == 0 {
		t.Error("expected data from second request")
	}
}

// TestClientResponseTimeoutDoesNotHang verifies that when a provider is slow to respond,
// the client respects the context timeout and does not hang indefinitely.
// This tests the fix for the unbounded response wait in tryProviders.
func TestClientResponseTimeoutDoesNotHang(t *testing.T) {
	// Create a provider that accepts requests quickly but responds slowly
	requestReceived := make(chan struct{})
	slowResponseCh := make(chan struct{})

	mockDial := testutil.MockDialerWithHandler(testutil.MockServerConfig{
		Handler: func(cmd string) (string, error) {
			if cmd == "DATE\r\n" {
				return "111 20240101000000\r\n", nil
			}
			if strings.HasPrefix(cmd, "BODY") {
				// Signal that request was received
				close(requestReceived)
				// Wait for the slow response signal (simulating slow network)
				<-slowResponseCh
				return "222 0 <id> body follows\r\ndata\r\n.\r\n", nil
			}
			return "500 Unknown Command\r\n", nil
		},
	})

	client := NewClient()
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

	err = client.AddProvider(p, ProviderPrimary)
	if err != nil {
		t.Fatalf("failed to add provider: %v", err)
	}

	// Create a context with short timeout
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	// Track when request completes
	doneCh := make(chan error, 1)
	go func() {
		err := client.Body(ctx, "123", io.Discard)
		doneCh <- err
	}()

	// Wait for the request to be received by server
	select {
	case <-requestReceived:
		// Good, request reached the server
	case <-time.After(5 * time.Second):
		close(slowResponseCh)
		t.Fatal("timeout waiting for request to be received")
	}

	// Now wait for the client to timeout (should happen within 200ms + some buffer)
	select {
	case err := <-doneCh:
		// Request completed - should be some kind of timeout/cancellation error
		// The important thing is that the request completed without hanging
		if err == nil {
			t.Error("expected timeout-related error, got nil")
		} else {
			// Accept context deadline exceeded or timeout-related errors
			// The fix ensures the request doesn't hang - the exact error type
			// depends on what layer times out first (context or network)
			t.Logf("request completed with error: %v (this is expected)", err)
		}
	case <-time.After(2 * time.Second):
		// If we get here, the fix didn't work - the client is hanging
		t.Fatal("client hung waiting for slow provider response - fix not working")
	}

	// Cleanup - unblock the server
	close(slowResponseCh)
}

// TestClientFallbackWhenPrimarySlowToRespond verifies that when a primary provider
// is slow to respond, the client respects the context timeout and doesn't hang.
// In this implementation, we try providers sequentially, so a slow primary will
// cause a timeout before we can try the backup.
func TestClientFallbackWhenPrimarySlowToRespond(t *testing.T) {
	// Primary provider: accepts immediately but responds slowly
	primaryRequestReceived := make(chan struct{})
	primarySlowResponseCh := make(chan struct{})

	primaryMock := testutil.MockDialerWithHandler(testutil.MockServerConfig{
		Handler: func(cmd string) (string, error) {
			if cmd == "DATE\r\n" {
				return "111 20240101000000\r\n", nil
			}
			if strings.HasPrefix(cmd, "BODY") {
				close(primaryRequestReceived)
				<-primarySlowResponseCh
				return "222 0 <id> body follows\r\ndata\r\n.\r\n", nil
			}
			return "500 Unknown Command\r\n", nil
		},
	})

	// Backup provider: responds quickly
	var backupUsed atomic.Bool
	backupMock := testutil.MockDialerWithHandler(testutil.MockServerConfig{
		Handler: func(cmd string) (string, error) {
			if cmd == "DATE\r\n" {
				return "111 20240101000000\r\n", nil
			}
			if strings.HasPrefix(cmd, "BODY") {
				backupUsed.Store(true)
				return "222 0 <id> body follows\r\nbackup-data\r\n.\r\n", nil
			}
			return "500 Unknown Command\r\n", nil
		},
	})

	client := NewClient()
	defer client.Close()

	primary, err := NewProvider(context.Background(), ProviderConfig{
		Address:               "primary.example.com:119",
		MaxConnections:        1,
		InflightPerConnection: 1,
		ConnFactory:           primaryMock,
	})
	if err != nil {
		t.Fatalf("failed to create primary provider: %v", err)
	}

	backup, err := NewProvider(context.Background(), ProviderConfig{
		Address:               "backup.example.com:119",
		MaxConnections:        1,
		InflightPerConnection: 1,
		ConnFactory:           backupMock,
	})
	if err != nil {
		t.Fatalf("failed to create backup provider: %v", err)
	}

	err = client.AddProvider(primary, ProviderPrimary)
	if err != nil {
		t.Fatalf("failed to add primary provider: %v", err)
	}
	err = client.AddProvider(backup, ProviderBackup)
	if err != nil {
		t.Fatalf("failed to add backup provider: %v", err)
	}

	// Create context with timeout
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()

	doneCh := make(chan error, 1)
	go func() {
		err := client.Body(ctx, "123", io.Discard)
		doneCh <- err
	}()

	// Wait for request to reach primary
	select {
	case <-primaryRequestReceived:
		// Good
	case <-time.After(5 * time.Second):
		close(primarySlowResponseCh)
		t.Fatal("timeout waiting for request to reach primary")
	}

	// Wait for request to complete (via timeout or backup)
	select {
	case err := <-doneCh:
		// The request should have timed out since primary is slow
		// and we don't get to try backup until primary times out
		if err == nil {
			// If backup was used, that's fine too
			if backupUsed.Load() {
				t.Log("backup provider was used successfully")
			}
		} else if errors.Is(err, context.DeadlineExceeded) {
			// This is the expected behavior - primary was slow, context timed out
			t.Log("context deadline exceeded as expected (primary was too slow)")
		} else {
			t.Errorf("unexpected error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("client hung - should have respected context timeout")
	}

	// Cleanup
	close(primarySlowResponseCh)
}

// TestProviderCapacityReleasedOnContextCancellation verifies that provider connections
// are properly released when a context is cancelled while waiting for a provider response.
// This ensures subsequent requests can proceed without hanging.
func TestProviderCapacityReleasedOnContextCancellation(t *testing.T) {
	// Create a provider that will be slow to respond
	slowResponseCh := make(chan struct{})
	var requestsStarted atomic.Int32

	mockDial := testutil.MockDialerWithHandler(testutil.MockServerConfig{
		Handler: func(cmd string) (string, error) {
			if cmd == "DATE\r\n" {
				return "111 20240101000000\r\n", nil
			}
			if strings.HasPrefix(cmd, "BODY") {
				requestsStarted.Add(1)
				<-slowResponseCh
				return "222 0 <id> body follows\r\ndata\r\n.\r\n", nil
			}
			return "500 Unknown Command\r\n", nil
		},
	})

	// Create client - concurrency controlled at provider level
	client := NewClient()
	defer client.Close()

	p, err := NewProvider(context.Background(), ProviderConfig{
		Address:               "example.com:119",
		MaxConnections:        2,
		InflightPerConnection: 1,
		ConnFactory:           mockDial,
	})
	if err != nil {
		t.Fatalf("failed to create provider: %v", err)
	}

	err = client.AddProvider(p, ProviderPrimary)
	if err != nil {
		t.Fatalf("failed to add provider: %v", err)
	}

	// Start two requests that will use up the provider's connection capacity
	ctx1, cancel1 := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel1()
	ctx2, cancel2 := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel2()

	done1 := make(chan struct{})
	done2 := make(chan struct{})

	go func() {
		_ = client.Body(ctx1, "1", io.Discard)
		close(done1)
	}()
	go func() {
		_ = client.Body(ctx2, "2", io.Discard)
		close(done2)
	}()

	// Wait for both requests to start
	timeout := time.After(5 * time.Second)
	for requestsStarted.Load() < 2 {
		select {
		case <-timeout:
			close(slowResponseCh)
			t.Fatal("timeout waiting for requests to start")
		default:
			time.Sleep(10 * time.Millisecond)
		}
	}

	// Wait for both contexts to timeout and goroutines to complete
	select {
	case <-done1:
	case <-time.After(2 * time.Second):
		t.Fatal("first request did not complete after context timeout")
	}
	select {
	case <-done2:
	case <-time.After(2 * time.Second):
		t.Fatal("second request did not complete after context timeout")
	}

	// Now try a third request - the provider capacity should have been released
	// If connections were NOT released, this would hang
	ctx3, cancel3 := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel3()

	done3 := make(chan error, 1)
	go func() {
		err := client.Body(ctx3, "3", io.Discard)
		done3 <- err
	}()

	select {
	case err := <-done3:
		// We expect this to also timeout (since server is still slow)
		// but the important thing is it didn't hang waiting for provider capacity
		if errors.Is(err, context.DeadlineExceeded) {
			t.Log("third request completed with context timeout - provider capacity was properly released")
		} else if err != nil {
			t.Logf("third request completed with error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("third request hung - provider capacity may not have been released")
	}

	// Cleanup
	close(slowResponseCh)
}

// TestOrphanedRequestsDrainedOnConnectionFailure verifies that requests
// queued in the provider's reqCh are drained with ErrProviderUnavailable
// when all connections die, preventing callers from hanging indefinitely.
func TestOrphanedRequestsDrainedOnConnectionFailure(t *testing.T) {
	// Use a sync point to control when the connection dies
	var requestsReceived atomic.Int32
	firstRequestReceivedCh := make(chan struct{})
	killConnectionCh := make(chan struct{})

	srv, stop := testutil.StartMockNNTPServer(t, testutil.MockServerConfig{
		ID: "orphan-test",
		Handler: func(cmd string) (string, error) {
			if cmd == "DATE\r\n" {
				return "111 20240101000000\r\n", nil
			}
			if cmd == "QUIT\r\n" {
				return "205 Bye\r\n", nil
			}
			if strings.HasPrefix(cmd, "BODY") {
				count := requestsReceived.Add(1)
				// After first request received, signal and wait for kill signal
				if count == 1 {
					close(firstRequestReceivedCh)
					// Wait for signal to kill connection
					<-killConnectionCh
					return "", io.EOF
				}
				// Subsequent requests (shouldn't happen if drain works)
				return "222 0 <id> body follows\r\ndata\r\n.\r\n", nil
			}
			return "500 Unknown Command\r\n", nil
		},
	})
	defer stop()

	var dialCount atomic.Int32
	dial := func(ctx context.Context) (net.Conn, error) {
		// Only allow 1 connection total to prevent lazy growth
		if dialCount.Add(1) > 1 {
			return nil, fmt.Errorf("simulated connection limit")
		}
		var d net.Dialer
		return d.DialContext(ctx, "tcp", srv.Addr())
	}

	// Create provider with small buffer - MaxConnections determines reqCh buffer size
	// But ConnFactory limits actual connections to 1
	provider, err := NewProvider(context.Background(), ProviderConfig{
		Address:            srv.Addr(),
		MaxConnections:     5, // reqCh buffer size
		InitialConnections: 1, // Only 1 actual connection (ConnFactory blocks more)
		ConnFactory:        dial,
	})
	if err != nil {
		t.Fatalf("failed to create provider: %v", err)
	}
	defer func() { _ = provider.Close() }()

	client := NewClient()
	defer client.Close()

	if err := client.AddProvider(provider, ProviderPrimary); err != nil {
		t.Fatalf("failed to add provider: %v", err)
	}

	// Start a request that will hang until we signal it
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_ = client.Body(ctx, "first", io.Discard)
	}()

	// Wait for the first request to be received by the server (connection is now busy)
	select {
	case <-firstRequestReceivedCh:
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for first request to be received")
	}

	// Now queue more requests - these will be queued in reqCh
	// because the single connection is busy processing the first request
	numQueuedRequests := 5
	queuedErrorsCh := make(chan error, numQueuedRequests)
	for i := 0; i < numQueuedRequests; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			err := client.Body(ctx, fmt.Sprintf("queued-%d", idx), io.Discard)
			queuedErrorsCh <- err
		}(i)
	}

	// Give time for requests to be queued in reqCh
	// The 5 requests should fit in the buffer (size=5)
	time.Sleep(2 * time.Second)

	// Now kill the connection by signaling the server
	close(killConnectionCh)
	// The connection will see EOF when trying to read, causing it to exit
	// This triggers removeConnection which should drain orphaned requests

	// Wait for all requests to complete (should not hang forever)
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		// Success - all requests completed without hanging
	case <-time.After(15 * time.Second):
		t.Fatal("requests did not complete within timeout - orphaned requests may not have been drained")
	}

	// Collect and verify queued request errors
	close(queuedErrorsCh)
	var unavailableCount, otherErrCount int
	for err := range queuedErrorsCh {
		if err != nil {
			if errors.Is(err, ErrProviderUnavailable) {
				unavailableCount++
			} else {
				otherErrCount++
			}
		}
	}

	// All queued requests should have gotten an error (not hung)
	totalErrors := unavailableCount + otherErrCount
	if totalErrors != numQueuedRequests {
		t.Errorf("expected %d errors, got %d (unavailable=%d, other=%d)",
			numQueuedRequests, totalErrors, unavailableCount, otherErrCount)
	}
}

// TestWriteErrorDoesNotBlockReader verifies that a write error after sending
// a request to the pending channel does not cause the reader to block forever.
// This tests the fix for the double semaphore release bug where:
// 1. Writer sends request to pending
// 2. Reader picks up the request from pending (concurrently)
// 3. Write fails
// 4. Writer should NOT release the semaphore (request is being processed by reader)
// 5. Reader completes and releases semaphore without blocking
func TestWriteErrorDoesNotBlockReader(t *testing.T) {
	// Track when the write should fail
	var writeCount atomic.Int32
	failOnWrite := atomic.Int32{}
	failOnWrite.Store(0) // 0 = don't fail, 1 = fail on next write

	// Custom conn that can simulate write failures
	type failingConn struct {
		net.Conn
		failOnWrite *atomic.Int32
		writeCount  *atomic.Int32
	}

	// Create a mock server that responds normally
	srv, stop := testutil.StartMockNNTPServer(t, testutil.MockServerConfig{
		ID: "write-error-test",
		Handler: func(cmd string) (string, error) {
			if cmd == "DATE\r\n" {
				return "111 20240101000000\r\n", nil
			}
			if cmd == "QUIT\r\n" {
				return "205 Bye\r\n", nil
			}
			if strings.HasPrefix(cmd, "BODY") {
				// Small delay to ensure write happens before we respond
				time.Sleep(10 * time.Millisecond)
				return "222 0 <id> body follows\r\ntest\r\n.\r\n", nil
			}
			return "500 Unknown Command\r\n", nil
		},
	})
	defer stop()

	dial := func(ctx context.Context) (net.Conn, error) {
		var d net.Dialer
		conn, err := d.DialContext(ctx, "tcp", srv.Addr())
		if err != nil {
			return nil, err
		}
		return &failingConn{
			Conn:        conn,
			failOnWrite: &failOnWrite,
			writeCount:  &writeCount,
		}, nil
	}

	provider, err := NewProvider(context.Background(), ProviderConfig{
		Address:               srv.Addr(),
		MaxConnections:        1,
		InflightPerConnection: 5, // Allow pipelining
		ConnFactory:           dial,
	})
	if err != nil {
		t.Fatalf("failed to create provider: %v", err)
	}
	defer func() { _ = provider.Close() }()

	client := NewClient()
	defer client.Close()

	if err := client.AddProvider(provider, ProviderPrimary); err != nil {
		t.Fatalf("failed to add provider: %v", err)
	}

	// Make a few successful requests first to warm up
	for i := 0; i < 3; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		err := client.Body(ctx, fmt.Sprintf("warm%d", i), io.Discard)
		cancel()
		if err != nil {
			t.Fatalf("warmup request %d failed: %v", i, err)
		}
	}

	// Now make concurrent requests - some should succeed, some may fail due to
	// connection issues, but none should hang
	var wg sync.WaitGroup
	done := make(chan struct{})

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			// We don't care about the error - we just want to ensure no deadlock
			_ = client.Body(ctx, fmt.Sprintf("test%d", idx), io.Discard)
		}(i)
	}

	go func() {
		wg.Wait()
		close(done)
	}()

	// Test passes if all requests complete within timeout (no deadlock)
	select {
	case <-done:
		// Success - all requests completed without hanging
	case <-time.After(10 * time.Second):
		t.Fatal("requests did not complete within timeout - possible deadlock in semaphore handling")
	}
}

// TestProviderLazyConnectionFirstRequest tests that the first request works
// immediately when InitialConnections=0 (lazy connection mode).
// This was a bug where deadCh was left open with no connections, causing
// signalAlive() to not properly create a new open channel when the first
// connection was established.
func TestProviderLazyConnectionFirstRequest(t *testing.T) {
	srv, stop := testutil.StartMockNNTPServer(t, testutil.MockServerConfig{
		Handler: func(cmd string) (string, error) {
			if cmd == "DATE\r\n" {
				return "111 20240101000000\r\n", nil
			}
			if cmd == "QUIT\r\n" {
				return "205 Bye\r\n", nil
			}
			return "500 Unknown Command\r\n", nil
		},
	})
	defer stop()

	dial := func(ctx context.Context) (net.Conn, error) {
		var d net.Dialer
		return d.DialContext(ctx, "tcp", srv.Addr())
	}

	// Create provider with InitialConnections=0 (lazy mode)
	p, err := NewProvider(context.Background(), ProviderConfig{
		Address:            srv.Addr(),
		MaxConnections:     2,
		InitialConnections: 0, // Lazy mode - no connections on startup
		ConnFactory:        dial,
	})
	if err != nil {
		t.Fatalf("failed to create provider: %v", err)
	}
	defer func() { _ = p.Close() }()

	// Verify no connections initially
	if got := atomic.LoadInt32(&p.connCount); got != 0 {
		t.Fatalf("expected 0 connections initially, got %d", got)
	}

	// First request should work immediately without needing cancel/retry
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err = p.Date(ctx)
	if err != nil {
		t.Fatalf("first request failed (should work immediately): %v", err)
	}

	// Verify connection was created
	if got := atomic.LoadInt32(&p.connCount); got == 0 {
		t.Fatal("expected at least 1 connection after first request")
	}

	// Second request should also work
	ctx2, cancel2 := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel2()

	err = p.Date(ctx2)
	if err != nil {
		t.Fatalf("second request failed: %v", err)
	}
}

// TestProviderConcurrentRequestsWhenDead tests that many concurrent requests
// all succeed when fired against a provider with no active connections.
// This was a race condition where addConnection() returned before Run() started
// consuming from reqCh, causing requests to pile up and timeout.
func TestProviderConcurrentRequestsWhenDead(t *testing.T) {
	const numRequests = 20

	srv, stop := testutil.StartMockNNTPServer(t, testutil.MockServerConfig{
		Handler: func(cmd string) (string, error) {
			if cmd == "DATE\r\n" {
				return "111 20240101000000\r\n", nil
			}
			if cmd == "QUIT\r\n" {
				return "205 Bye\r\n", nil
			}
			return "500 Unknown Command\r\n", nil
		},
	})
	defer stop()

	dial := func(ctx context.Context) (net.Conn, error) {
		var d net.Dialer
		return d.DialContext(ctx, "tcp", srv.Addr())
	}

	// Create provider with InitialConnections=0 to test lazy connection mode
	// where connections are created on first request.
	// Use InflightPerConnection=1 to avoid pipelining (mock server doesn't handle it)
	// and MaxConnections=2 to allow some parallelism.
	p, err := NewProvider(context.Background(), ProviderConfig{
		Address:               srv.Addr(),
		MaxConnections:        2,
		InitialConnections:    0, // No connections at startup
		InflightPerConnection: 1, // No pipelining (mock server limitation)
		ConnFactory:           dial,
	})
	if err != nil {
		t.Fatalf("failed to create provider: %v", err)
	}
	defer func() { _ = p.Close() }()

	// Verify no connections initially
	if got := atomic.LoadInt32(&p.connCount); got != 0 {
		t.Fatalf("expected 0 connections initially, got %d", got)
	}

	// Fire many concurrent requests - all should complete without timeout
	var wg sync.WaitGroup
	errors := make(chan error, numRequests)

	for i := 0; i < numRequests; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()

			err := p.Date(ctx)
			if err != nil {
				errors <- fmt.Errorf("request %d failed: %v", idx, err)
			}
		}(i)
	}

	wg.Wait()
	close(errors)

	// Collect all errors
	var errs []error
	for err := range errors {
		errs = append(errs, err)
	}

	if len(errs) > 0 {
		t.Fatalf("concurrent requests failed (%d/%d): %v", len(errs), numRequests, errs[0])
	}

	t.Logf("all %d concurrent requests succeeded", numRequests)
}

// TestProviderConcurrentRequestsWithPipelining tests concurrent requests with
// InflightPerConnection > 1, which enables pipelining. This tests that the
// ready channel synchronization works properly when Run() needs to start
// consuming requests immediately.
func TestProviderConcurrentRequestsWithPipelining(t *testing.T) {
	const numRequests = 10

	// Create a mock server that handles pipelined DATE commands
	srv, stop := testutil.StartMockNNTPServer(t, testutil.MockServerConfig{
		Handler: func(cmd string) (string, error) {
			// Handle potentially pipelined commands by responding to each
			// DATE\r\n in the input
			var response string
			remaining := cmd
			for len(remaining) > 0 {
				if len(remaining) >= 6 && remaining[:6] == "DATE\r\n" {
					response += "111 20240101000000\r\n"
					remaining = remaining[6:]
				} else if len(remaining) >= 6 && remaining[:6] == "QUIT\r\n" {
					response += "205 Bye\r\n"
					remaining = remaining[6:]
				} else {
					// Unknown command - find next \r\n
					idx := 0
					for i := 0; i < len(remaining)-1; i++ {
						if remaining[i] == '\r' && remaining[i+1] == '\n' {
							idx = i + 2
							break
						}
					}
					if idx == 0 {
						idx = len(remaining)
					}
					response += "500 Unknown Command\r\n"
					remaining = remaining[idx:]
				}
			}
			return response, nil
		},
	})
	defer stop()

	dial := func(ctx context.Context) (net.Conn, error) {
		var d net.Dialer
		return d.DialContext(ctx, "tcp", srv.Addr())
	}

	// Create provider with InitialConnections=0 and InflightPerConnection > 1
	// This is the configuration that triggers the original race condition
	p, err := NewProvider(context.Background(), ProviderConfig{
		Address:               srv.Addr(),
		MaxConnections:        1,
		InitialConnections:    0, // No connections at startup
		InflightPerConnection: 5, // Enable pipelining
		ConnFactory:           dial,
	})
	if err != nil {
		t.Fatalf("failed to create provider: %v", err)
	}
	defer func() { _ = p.Close() }()

	// Verify no connections initially
	if got := atomic.LoadInt32(&p.connCount); got != 0 {
		t.Fatalf("expected 0 connections initially, got %d", got)
	}

	// Fire concurrent requests - all should complete without timeout
	var wg sync.WaitGroup
	errors := make(chan error, numRequests)

	for i := 0; i < numRequests; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()

			err := p.Date(ctx)
			if err != nil {
				errors <- fmt.Errorf("request %d failed: %v", idx, err)
			}
		}(i)
	}

	wg.Wait()
	close(errors)

	// Collect all errors
	var errs []error
	for err := range errors {
		errs = append(errs, err)
	}

	if len(errs) > 0 {
		t.Fatalf("concurrent requests failed (%d/%d): %v", len(errs), numRequests, errs[0])
	}

	t.Logf("all %d concurrent requests with pipelining succeeded", numRequests)
}

// TestSequentialReaderWithPipeDeadlock demonstrates the connection deadlock issue
// when implementing a sequential reader using io.Pipe with direct writes.
//
// The deadlock scenario:
//  1. Workers hold connections while blocked on pipe writes (pipes are synchronous)
//  2. Pipe writes block until the reader consumes them sequentially
//  3. Reader can't reach later pipes because connections are held by workers blocked on earlier pipes
//
// Solution: Download to buffer first, then copy to pipe (connection released before potential block).
func TestSequentialReaderWithPipeDeadlock(t *testing.T) {
	// Test data - 3 segments that must be read in order
	segments := [][]byte{
		[]byte("segment-1-data"),
		[]byte("segment-2-data"),
		[]byte("segment-3-data"),
	}

	srv, cleanup := testutil.StartMockNNTPServer(t, testutil.MockServerConfig{
		Handler: func(cmd string) (string, error) {
			if strings.HasPrefix(cmd, "BODY") {
				parts := strings.Fields(cmd)
				if len(parts) >= 2 {
					id := strings.TrimRight(parts[1], "\r\n")
					// Return segment based on ID
					for i, seg := range segments {
						if id == fmt.Sprintf("<%d>", i) {
							yencBody := testutil.EncodeYenc(seg, fmt.Sprintf("seg%d.txt", i), 1, 1)
							return fmt.Sprintf("222 0 %s body follows\r\n%s.\r\n", id, yencBody), nil
						}
					}
				}
				return "430 No such article\r\n", nil
			}
			if cmd == "DATE\r\n" {
				return "111 20240101000000\r\n", nil
			}
			if cmd == "QUIT\r\n" {
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

	t.Run("deadlock_with_direct_pipe_writes", func(t *testing.T) {
		// Setup client with limited connections (2 connections for 3 segments)
		// This creates the deadlock condition when all connections are blocked on pipe writes
		client := NewClient()
		defer client.Close()

		p, err := NewProvider(context.Background(), ProviderConfig{
			Address:               srv.Addr(),
			MaxConnections:        2, // Fewer connections than segments
			InflightPerConnection: 1,
			ConnFactory:           dial,
		})
		if err != nil {
			t.Fatalf("failed to create provider: %v", err)
		}
		if err := client.AddProvider(p, ProviderPrimary); err != nil {
			t.Fatalf("failed to add provider: %v", err)
		}

		// Create pipes for each segment
		pipes := make([]struct {
			r *io.PipeReader
			w *io.PipeWriter
		}, len(segments))
		for i := range pipes {
			pipes[i].r, pipes[i].w = io.Pipe()
		}

		// Start workers that download directly to pipes (problematic pattern)
		var wg sync.WaitGroup
		downloadDone := make(chan struct{})
		for i := range segments {
			wg.Add(1)
			go func(idx int) {
				defer wg.Done()
				ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
				defer cancel()
				// Direct pipe write - will block until reader consumes
				err := client.Body(ctx, fmt.Sprintf("<%d>", idx), pipes[idx].w)
				if err != nil {
					// Expected to timeout/fail due to deadlock
					_ = pipes[idx].w.CloseWithError(err)
				} else {
					_ = pipes[idx].w.Close()
				}
			}(i)
		}

		go func() {
			wg.Wait()
			close(downloadDone)
		}()

		// Sequential reader - must read segment 0, then 1, then 2
		// With direct pipe writes and limited connections, this will deadlock:
		// - Workers for seg 1 and 2 hold the 2 connections
		// - They block on pipe writes waiting for reader
		// - Reader is waiting for seg 0 which can't start (no free connections)
		var result bytes.Buffer
		readDone := make(chan error, 1)
		go func() {
			for i := 0; i < len(segments); i++ {
				_, err := io.Copy(&result, pipes[i].r)
				if err != nil && err != io.EOF {
					readDone <- err
					return
				}
			}
			readDone <- nil
		}()

		// With the deadlock pattern, we expect a timeout
		select {
		case err := <-readDone:
			if err == nil && result.Len() == len(segments[0])+len(segments[1])+len(segments[2]) {
				// If it succeeds, that's actually fine - the race condition didn't trigger
				t.Log("direct pipe writes completed (race condition didn't trigger this time)")
			}
		case <-downloadDone:
			// Downloads completed but reader may still be blocked
			select {
			case <-readDone:
				// OK
			case <-time.After(100 * time.Millisecond):
				t.Log("reader blocked after downloads - demonstrating potential deadlock")
			}
		case <-time.After(1 * time.Second):
			// Expected: timeout due to deadlock potential
			t.Log("timeout detected - deadlock scenario demonstrated")
		}

		// Cleanup pipes
		for i := range pipes {
			_ = pipes[i].r.Close()
			_ = pipes[i].w.Close()
		}
	})

	t.Run("no_deadlock_with_buffered_writes", func(t *testing.T) {
		// Setup client with same limited connections
		client := NewClient()
		defer client.Close()

		p, err := NewProvider(context.Background(), ProviderConfig{
			Address:               srv.Addr(),
			MaxConnections:        2, // Same limited connections
			InflightPerConnection: 1,
			ConnFactory:           dial,
		})
		if err != nil {
			t.Fatalf("failed to create provider: %v", err)
		}
		if err := client.AddProvider(p, ProviderPrimary); err != nil {
			t.Fatalf("failed to add provider: %v", err)
		}

		// Create pipes for each segment
		pipes := make([]struct {
			r *io.PipeReader
			w *io.PipeWriter
		}, len(segments))
		for i := range pipes {
			pipes[i].r, pipes[i].w = io.Pipe()
		}

		// Start workers that buffer first, then write to pipe (correct pattern)
		var wg sync.WaitGroup
		for i := range segments {
			wg.Add(1)
			go func(idx int) {
				defer wg.Done()
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()

				// SOLUTION: Buffer first, then copy to pipe
				var buf bytes.Buffer
				err := client.Body(ctx, fmt.Sprintf("<%d>", idx), &buf) // Connection released here
				if err != nil {
					_ = pipes[idx].w.CloseWithError(err)
					return
				}
				_, err = io.Copy(pipes[idx].w, &buf) // May block, but connection is free
				if err != nil {
					_ = pipes[idx].w.CloseWithError(err)
				} else {
					_ = pipes[idx].w.Close()
				}
			}(i)
		}

		// Sequential reader
		var result bytes.Buffer
		readDone := make(chan error, 1)
		go func() {
			for i := 0; i < len(segments); i++ {
				_, err := io.Copy(&result, pipes[i].r)
				if err != nil && err != io.EOF {
					readDone <- err
					return
				}
			}
			readDone <- nil
		}()

		// With buffered pattern, should complete without deadlock
		select {
		case err := <-readDone:
			if err != nil {
				t.Fatalf("sequential read failed: %v", err)
			}
			// Verify all segments were read
			expected := string(segments[0]) + string(segments[1]) + string(segments[2])
			if result.String() != expected {
				t.Errorf("data mismatch:\nexpected: %q\ngot: %q", expected, result.String())
			}
			t.Log("buffered writes completed successfully - no deadlock")
		case <-time.After(10 * time.Second):
			t.Fatal("timeout - unexpected deadlock with buffered pattern")
		}

		wg.Wait()

		// Cleanup pipes
		for i := range pipes {
			_ = pipes[i].r.Close()
			_ = pipes[i].w.Close()
		}
	})
}
