package nntppool

import (
	"bytes"
	"context"
	"errors"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/javi11/nntppool/v3/testutil"
)

// TestPartialWritePreventsFailover tests that once bytes are written to the BodyWriter,
// the client won't failover to another provider (which would corrupt data).
func TestPartialWritePreventsFailover(t *testing.T) {
	var provider1Attempts, provider2Attempts int32

	// Provider 1: Returns partial data then fails
	srv1, cleanup1 := testutil.StartMockNNTPServer(t, testutil.MockServerConfig{
		ID: "provider1",
		Handler: func(cmd string) (string, error) {
			if cmd == "DATE\r\n" {
				return "111 20240101000000\r\n", nil
			}
			if strings.HasPrefix(cmd, "BODY") {
				atomic.AddInt32(&provider1Attempts, 1)
				// Return partial response (will write some bytes) then disconnect
				// The yEnc header will cause bytes to be written before the connection dies
				yencData := testutil.EncodeYenc([]byte("hello world"), "test.txt", 1, 1)
				partialResponse := "222 0 <id> body follows\r\n" + yencData[:len(yencData)/2]
				return partialResponse, errors.New("simulate disconnect")
			}
			return "500 Unknown Command\r\n", nil
		},
	})
	defer cleanup1()

	// Provider 2: Would succeed if tried
	srv2, cleanup2 := testutil.StartMockNNTPServer(t, testutil.MockServerConfig{
		ID: "provider2",
		Handler: func(cmd string) (string, error) {
			if cmd == "DATE\r\n" {
				return "111 20240101000000\r\n", nil
			}
			if strings.HasPrefix(cmd, "BODY") {
				atomic.AddInt32(&provider2Attempts, 1)
				return "222 0 <id> body follows\r\n" + testutil.EncodeYenc([]byte("hello world"), "test.txt", 1, 1) + ".\r\n", nil
			}
			return "500 Unknown Command\r\n", nil
		},
	})
	defer cleanup2()

	dial1 := func(ctx context.Context) (net.Conn, error) {
		var d net.Dialer
		return d.DialContext(ctx, "tcp", srv1.Addr())
	}
	dial2 := func(ctx context.Context) (net.Conn, error) {
		var d net.Dialer
		return d.DialContext(ctx, "tcp", srv2.Addr())
	}

	p1, err := NewProvider(context.Background(), ProviderConfig{
		Address:        srv1.Addr(),
		MaxConnections: 1,
		ConnFactory:    dial1,
	})
	if err != nil {
		t.Fatalf("failed to create provider1: %v", err)
	}
	defer func() {
		_ = p1.Close()
	}()

	p2, err := NewProvider(context.Background(), ProviderConfig{
		Address:        srv2.Addr(),
		MaxConnections: 1,
		ConnFactory:    dial2,
	})
	if err != nil {
		t.Fatalf("failed to create provider2: %v", err)
	}
	defer func() {
		_ = p2.Close()
	}()

	client := NewClient()
	defer client.Close()

	err = client.AddProvider(p1, ProviderPrimary)
	if err != nil {
		t.Fatalf("failed to add provider1: %v", err)
	}
	err = client.AddProvider(p2, ProviderBackup)
	if err != nil {
		t.Fatalf("failed to add provider2: %v", err)
	}

	// Make request with a writer that will receive partial data
	var buf bytes.Buffer
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err = client.Body(ctx, "test-article", &buf)

	// The request should fail (partial write scenario with disconnect)
	if err == nil {
		t.Error("expected error from partial write with disconnect")
	}

	// Provider 2 should NOT be tried since bytes were already written
	if atomic.LoadInt32(&provider2Attempts) > 0 {
		t.Error("expected provider2 NOT to be tried after partial write to provider1")
	}

	// Provider 1 should have been attempted
	if atomic.LoadInt32(&provider1Attempts) == 0 {
		t.Error("expected provider1 to be attempted")
	}

	t.Logf("Provider1 attempts: %d, Provider2 attempts: %d",
		atomic.LoadInt32(&provider1Attempts), atomic.LoadInt32(&provider2Attempts))
}

// TestPartialWriteFailoverWhenNoBytesWritten tests that failover DOES occur
// when no bytes have been written yet (server error, not article-not-found).
// Note: We use different addresses (primary.test vs backup.test) to avoid
// the client's host-based deduplication which skips providers with same host.
func TestPartialWriteFailoverWhenNoBytesWritten(t *testing.T) {
	var provider1Attempts, provider2Attempts int32

	// Provider 1: Fails with server error before writing any body
	unavailableHandler := func(cmd string) (string, error) {
		if cmd == "DATE\r\n" {
			return "111 20240101000000\r\n", nil
		}
		if strings.HasPrefix(cmd, "BODY") {
			atomic.AddInt32(&provider1Attempts, 1)
			// Return 503 (service unavailable) - triggers failover
			return "503 Service Unavailable\r\n", nil
		}
		if strings.HasPrefix(cmd, "QUIT") {
			return "205 Bye\r\n", nil
		}
		return "500 Unknown Command\r\n", nil
	}

	// Provider 2: Should be tried after provider 1 fails
	workingHandler := func(cmd string) (string, error) {
		if cmd == "DATE\r\n" {
			return "111 20240101000000\r\n", nil
		}
		if strings.HasPrefix(cmd, "BODY") {
			atomic.AddInt32(&provider2Attempts, 1)
			return "222 0 <id> body follows\r\nhello world\r\n.\r\n", nil
		}
		if strings.HasPrefix(cmd, "QUIT") {
			return "205 Bye\r\n", nil
		}
		return "500 Unknown Command\r\n", nil
	}

	p1, err := NewProvider(context.Background(), ProviderConfig{
		Address:        "primary.test:119", // Different host to avoid deduplication
		MaxConnections: 1,
		ConnFactory: testutil.MockDialerWithHandler(testutil.MockServerConfig{
			Handler: unavailableHandler,
		}),
	})
	if err != nil {
		t.Fatalf("failed to create provider1: %v", err)
	}
	defer func() {
		_ = p1.Close()
	}()

	p2, err := NewProvider(context.Background(), ProviderConfig{
		Address:        "backup.test:119", // Different host to avoid deduplication
		MaxConnections: 1,
		ConnFactory: testutil.MockDialerWithHandler(testutil.MockServerConfig{
			Handler: workingHandler,
		}),
	})
	if err != nil {
		t.Fatalf("failed to create provider2: %v", err)
	}
	defer func() {
		_ = p2.Close()
	}()

	client := NewClient()
	defer client.Close()

	err = client.AddProvider(p1, ProviderPrimary)
	if err != nil {
		t.Fatalf("failed to add provider1: %v", err)
	}
	err = client.AddProvider(p2, ProviderBackup)
	if err != nil {
		t.Fatalf("failed to add provider2: %v", err)
	}

	// Make request
	var buf bytes.Buffer
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err = client.Body(ctx, "test-article", &buf)

	// Should succeed using provider 2
	if err != nil {
		t.Errorf("expected success after failover, got: %v", err)
	}

	// Provider 2 should have been tried
	if atomic.LoadInt32(&provider2Attempts) == 0 {
		t.Error("expected provider2 to be tried after provider1 failed with no bytes written")
	}

	// Both providers should have been attempted
	if atomic.LoadInt32(&provider1Attempts) == 0 {
		t.Error("expected provider1 to be attempted")
	}

	t.Logf("Provider1 attempts: %d, Provider2 attempts: %d, Buffer: %q",
		atomic.LoadInt32(&provider1Attempts), atomic.LoadInt32(&provider2Attempts), buf.String())
}

// TestPartialWriteWithWriterAt tests partial write tracking with WriterAt interface.
func TestPartialWriteWithWriterAt(t *testing.T) {
	var provider1Attempts, provider2Attempts int32

	// Provider 1: Returns partial yEnc data then disconnects
	srv1, cleanup1 := testutil.StartMockNNTPServer(t, testutil.MockServerConfig{
		ID: "provider1",
		Handler: func(cmd string) (string, error) {
			if cmd == "DATE\r\n" {
				return "111 20240101000000\r\n", nil
			}
			if strings.HasPrefix(cmd, "BODY") {
				atomic.AddInt32(&provider1Attempts, 1)
				// Partial yEnc data that will trigger WriteAt
				yencData := testutil.EncodeYenc([]byte("hello world data"), "test.txt", 1, 1)
				// Return just enough to parse headers and start decoding
				partialResponse := "222 0 <id> body follows\r\n" + yencData[:len(yencData)/2]
				return partialResponse, errors.New("disconnect")
			}
			return "500 Unknown Command\r\n", nil
		},
	})
	defer cleanup1()

	// Provider 2: Would succeed
	srv2, cleanup2 := testutil.StartMockNNTPServer(t, testutil.MockServerConfig{
		ID: "provider2",
		Handler: func(cmd string) (string, error) {
			if cmd == "DATE\r\n" {
				return "111 20240101000000\r\n", nil
			}
			if strings.HasPrefix(cmd, "BODY") {
				atomic.AddInt32(&provider2Attempts, 1)
				yencBody := testutil.EncodeYenc([]byte("hello world data"), "test.txt", 1, 1)
				return "222 0 <id> body follows\r\n" + yencBody + ".\r\n", nil
			}
			return "500 Unknown Command\r\n", nil
		},
	})
	defer cleanup2()

	dial1 := func(ctx context.Context) (net.Conn, error) {
		var d net.Dialer
		return d.DialContext(ctx, "tcp", srv1.Addr())
	}
	dial2 := func(ctx context.Context) (net.Conn, error) {
		var d net.Dialer
		return d.DialContext(ctx, "tcp", srv2.Addr())
	}

	p1, err := NewProvider(context.Background(), ProviderConfig{
		Address:        srv1.Addr(),
		MaxConnections: 1,
		ConnFactory:    dial1,
	})
	if err != nil {
		t.Fatalf("failed to create provider1: %v", err)
	}
	defer func() {
		_ = p1.Close()
	}()

	p2, err := NewProvider(context.Background(), ProviderConfig{
		Address:        srv2.Addr(),
		MaxConnections: 1,
		ConnFactory:    dial2,
	})
	if err != nil {
		t.Fatalf("failed to create provider2: %v", err)
	}
	defer func() {
		_ = p2.Close()
	}()

	client := NewClient()
	defer client.Close()

	err = client.AddProvider(p1, ProviderPrimary)
	if err != nil {
		t.Fatalf("failed to add provider1: %v", err)
	}
	err = client.AddProvider(p2, ProviderBackup)
	if err != nil {
		t.Fatalf("failed to add provider2: %v", err)
	}

	// Use WriterAt interface
	wa := &mockWriterAtWithTracking{data: make([]byte, 100)}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err = client.BodyAt(ctx, "test-article", wa)

	// The request should fail (partial write scenario with disconnect)
	if err == nil {
		t.Error("expected error from partial write with disconnect")
	}

	// If partial bytes were written via WriteAt, provider2 should NOT be tried
	if wa.bytesWritten > 0 && atomic.LoadInt32(&provider2Attempts) > 0 {
		t.Error("expected provider2 NOT to be tried after partial WriteAt to provider1")
	}

	t.Logf("Provider1 attempts: %d, Provider2 attempts: %d, bytes written: %d",
		atomic.LoadInt32(&provider1Attempts), atomic.LoadInt32(&provider2Attempts), wa.bytesWritten)
}

// mockWriterAt is a test implementation of io.WriterAt that tracks writes.
type mockWriterAtWithTracking struct {
	data         []byte
	mu           sync.Mutex
	bytesWritten int64
}

func (m *mockWriterAtWithTracking) WriteAt(p []byte, off int64) (n int, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if int64(len(m.data)) < off+int64(len(p)) {
		newData := make([]byte, off+int64(len(p)))
		copy(newData, m.data)
		m.data = newData
	}
	copy(m.data[off:], p)
	m.bytesWritten += int64(len(p))
	return len(p), nil
}

// TestTrackingWriterCounts tests that the tracking writer correctly counts bytes.
func TestTrackingWriterCounts(t *testing.T) {
	var buf bytes.Buffer
	tracker := &trackingWriter{w: &buf}

	// Write some data
	n, err := tracker.Write([]byte("hello"))
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if n != 5 {
		t.Errorf("expected n=5, got %d", n)
	}
	if tracker.written != 5 {
		t.Errorf("expected written=5, got %d", tracker.written)
	}

	// Write more data
	n, err = tracker.Write([]byte(" world"))
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if n != 6 {
		t.Errorf("expected n=6, got %d", n)
	}
	if tracker.written != 11 {
		t.Errorf("expected written=11, got %d", tracker.written)
	}

	// Verify underlying writer
	if buf.String() != "hello world" {
		t.Errorf("expected 'hello world', got %q", buf.String())
	}
}

// TestTrackingWriterAtCounts tests that the tracking WriterAt correctly counts bytes.
func TestTrackingWriterAtCounts(t *testing.T) {
	wa := &mockWriterAtWithTracking{data: make([]byte, 20)}
	var buf bytes.Buffer
	tracker := &trackingWriter{w: &buf}
	trackingWA := &trackingWriterAt{trackingWriter: tracker, wa: wa}

	// Write at offset
	n, err := trackingWA.WriteAt([]byte("hello"), 0)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if n != 5 {
		t.Errorf("expected n=5, got %d", n)
	}
	if tracker.written != 5 {
		t.Errorf("expected written=5, got %d", tracker.written)
	}

	// Write at different offset
	n, err = trackingWA.WriteAt([]byte(" world"), 5)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if n != 6 {
		t.Errorf("expected n=6, got %d", n)
	}
	if tracker.written != 11 {
		t.Errorf("expected written=11, got %d", tracker.written)
	}
}

// TestPartialWriteConnectionError tests that a connection error mid-transfer
// prevents failover when bytes have been written.
func TestPartialWriteConnectionError(t *testing.T) {
	var requestCount int32
	var provider2Called int32

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
						count := atomic.AddInt32(&requestCount, 1)
						if count == 1 {
							// First request: send partial response then close
							yencData := testutil.EncodeYenc([]byte("test data here"), "test.txt", 1, 1)
							partialResp := "222 0 <id> body follows\r\n" + yencData[:len(yencData)/3]
							_, _ = c.Write([]byte(partialResp))
							time.Sleep(10 * time.Millisecond)
							return // Close connection mid-transfer
						}
						// Subsequent requests: normal response
						yencBody := testutil.EncodeYenc([]byte("test data here"), "test.txt", 1, 1)
						_, _ = c.Write([]byte("222 0 <id> body follows\r\n" + yencBody + ".\r\n"))
					}
				}
			}(conn)
		}
	}()

	dial1 := func(ctx context.Context) (net.Conn, error) {
		var d net.Dialer
		return d.DialContext(ctx, "tcp", l.Addr().String())
	}

	// Provider 2 (backup)
	srv2, cleanup2 := testutil.StartMockNNTPServer(t, testutil.MockServerConfig{
		Handler: func(cmd string) (string, error) {
			if cmd == "DATE\r\n" {
				return "111 20240101000000\r\n", nil
			}
			if strings.HasPrefix(cmd, "BODY") {
				atomic.AddInt32(&provider2Called, 1)
				yencBody := testutil.EncodeYenc([]byte("test data here"), "test.txt", 1, 1)
				return "222 0 <id> body follows\r\n" + yencBody + ".\r\n", nil
			}
			return "500 Unknown Command\r\n", nil
		},
	})
	defer cleanup2()

	dial2 := func(ctx context.Context) (net.Conn, error) {
		var d net.Dialer
		return d.DialContext(ctx, "tcp", srv2.Addr())
	}

	p1, err := NewProvider(context.Background(), ProviderConfig{
		Address:        l.Addr().String(),
		MaxConnections: 1,
		ConnFactory:    dial1,
	})
	if err != nil {
		t.Fatalf("failed to create provider1: %v", err)
	}
	defer func() {
		_ = p1.Close()
	}()

	p2, err := NewProvider(context.Background(), ProviderConfig{
		Address:        srv2.Addr(),
		MaxConnections: 1,
		ConnFactory:    dial2,
	})
	if err != nil {
		t.Fatalf("failed to create provider2: %v", err)
	}
	defer func() {
		_ = p2.Close()
	}()

	client := NewClient()
	defer client.Close()

	err = client.AddProvider(p1, ProviderPrimary)
	if err != nil {
		t.Fatalf("failed to add provider1: %v", err)
	}
	err = client.AddProvider(p2, ProviderBackup)
	if err != nil {
		t.Fatalf("failed to add provider2: %v", err)
	}

	var buf bytes.Buffer
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err = client.Body(ctx, "test-article", &buf)

	// Should have an error (partial write, then connection closed)
	if err == nil {
		t.Error("expected error from partial write with connection close")
	}

	// Provider 2 should NOT be called if bytes were written
	p2Calls := atomic.LoadInt32(&provider2Called)
	if buf.Len() > 0 && p2Calls > 0 {
		t.Errorf("expected provider2 NOT to be called after partial write (buf has %d bytes), but it was called %d times",
			buf.Len(), p2Calls)
	}

	t.Logf("Request count: %d, Provider2 calls: %d, Buffer size: %d",
		atomic.LoadInt32(&requestCount), p2Calls, buf.Len())
}

// TestPartialWriteWriterError tests handling when the writer returns an error.
func TestPartialWriteWriterError(t *testing.T) {
	srv, cleanup := testutil.StartMockNNTPServer(t, testutil.MockServerConfig{
		Handler: func(cmd string) (string, error) {
			if cmd == "DATE\r\n" {
				return "111 20240101000000\r\n", nil
			}
			if strings.HasPrefix(cmd, "BODY") {
				yencBody := testutil.EncodeYenc([]byte("hello world test data"), "test.txt", 1, 1)
				return "222 0 <id> body follows\r\n" + yencBody + ".\r\n", nil
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

	err = client.AddProvider(p, ProviderPrimary)
	if err != nil {
		t.Fatalf("failed to add provider: %v", err)
	}

	// Use a writer that fails after some bytes
	fw := &failingWriter{failAfter: 5}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err = client.Body(ctx, "test-article", fw)

	// Should get an error from the failing writer
	if err == nil {
		t.Error("expected error from failing writer")
	}

	// Verify some bytes were written before failure
	if fw.written < 5 {
		t.Errorf("expected at least 5 bytes written, got %d", fw.written)
	}
}

// failingWriter is a writer that fails after a certain number of bytes.
type failingWriter struct {
	written   int
	failAfter int
}

func (f *failingWriter) Write(p []byte) (int, error) {
	if f.written >= f.failAfter {
		return 0, errors.New("simulated write error")
	}
	canWrite := f.failAfter - f.written
	if canWrite > len(p) {
		canWrite = len(p)
	}
	f.written += canWrite
	if f.written >= f.failAfter {
		return canWrite, errors.New("simulated write error")
	}
	return canWrite, nil
}
