package nntppool

import (
	"io"
	"sync/atomic"
	"testing"

	"github.com/javi11/nntppool/v2/pkg/nntpcli"
)

// TestPooledBodyReader_NilReader_GetYencHeaders verifies that GetYencHeaders
// handles nil reader gracefully without panicking
func TestPooledBodyReader_NilReader_GetYencHeaders(t *testing.T) {
	reader := &pooledBodyReader{
		reader:  nil, // Nil reader to test defensive check
		closeCh: make(chan struct{}),
	}

	headers, err := reader.GetYencHeaders()

	if err != io.EOF {
		t.Errorf("GetYencHeaders() with nil reader error = %v, want io.EOF", err)
	}

	// Verify empty headers are returned
	if headers.FileName != "" || headers.FileSize != 0 || headers.PartNumber != 0 || headers.TotalParts != 0 {
		t.Errorf("GetYencHeaders() with nil reader returned non-empty headers = %+v, want empty headers", headers)
	}
}

// TestPooledBodyReader_NilReader_Read verifies that Read
// handles nil reader gracefully without panicking
func TestPooledBodyReader_NilReader_Read(t *testing.T) {
	reader := &pooledBodyReader{
		reader:  nil, // Nil reader to test defensive check
		closeCh: make(chan struct{}),
	}

	buffer := make([]byte, 1024)
	n, err := reader.Read(buffer)

	if err != io.EOF {
		t.Errorf("Read() with nil reader error = %v, want io.EOF", err)
	}

	if n != 0 {
		t.Errorf("Read() with nil reader read %d bytes, want 0", n)
	}
}

// TestPooledBodyReader_NilReader_Close verifies that Close
// handles nil reader gracefully without panicking
func TestPooledBodyReader_NilReader_Close(t *testing.T) {
	reader := &pooledBodyReader{
		reader:  nil, // Nil reader to test defensive check
		closeCh: make(chan struct{}),
	}

	err := reader.Close()

	if err != nil {
		t.Errorf("Close() with nil reader returned error = %v, want nil", err)
	}

	// Verify closed flag is set
	if !reader.closed.Load() {
		t.Error("Close() with nil reader did not set closed flag")
	}
}

// TestPooledBodyReader_NilReader_MultipleOperations verifies that
// multiple operations on nil reader all handle it gracefully
func TestPooledBodyReader_NilReader_MultipleOperations(t *testing.T) {
	reader := &pooledBodyReader{
		reader:  nil, // Nil reader to test defensive check
		closeCh: make(chan struct{}),
	}

	// Try GetYencHeaders
	_, err := reader.GetYencHeaders()
	if err != io.EOF {
		t.Errorf("GetYencHeaders() error = %v, want io.EOF", err)
	}

	// Try Read
	buffer := make([]byte, 1024)
	n, err := reader.Read(buffer)
	if err != io.EOF {
		t.Errorf("Read() error = %v, want io.EOF", err)
	}
	if n != 0 {
		t.Errorf("Read() read %d bytes, want 0", n)
	}

	// Try Close
	err = reader.Close()
	if err != nil {
		t.Errorf("Close() error = %v, want nil", err)
	}

	// Verify operations after close still return EOF
	_, err = reader.GetYencHeaders()
	if err != io.EOF {
		t.Errorf("GetYencHeaders() after close error = %v, want io.EOF", err)
	}

	n, err = reader.Read(buffer)
	if err != io.EOF {
		t.Errorf("Read() after close error = %v, want io.EOF", err)
	}
	if n != 0 {
		t.Errorf("Read() after close read %d bytes, want 0", n)
	}
}

// TestPooledBodyReader_NilReader_ConcurrentAccess verifies that
// concurrent operations on nil reader are safe
func TestPooledBodyReader_NilReader_ConcurrentAccess(t *testing.T) {
	reader := &pooledBodyReader{
		reader:  nil, // Nil reader to test defensive check
		closeCh: make(chan struct{}),
	}

	const goroutines = 10
	done := make(chan bool, goroutines)

	// Launch concurrent operations
	for i := 0; i < goroutines; i++ {
		go func() {
			defer func() {
				if r := recover(); r != nil {
					t.Errorf("Concurrent operation panicked: %v", r)
				}
				done <- true
			}()

			// Try various operations
			_, _ = reader.GetYencHeaders()
			buffer := make([]byte, 100)
			_, _ = reader.Read(buffer)
		}()
	}

	// Wait for all goroutines
	for i := 0; i < goroutines; i++ {
		<-done
	}

	// Close should still work
	err := reader.Close()
	if err != nil {
		t.Errorf("Close() after concurrent operations error = %v, want nil", err)
	}
}

// TestPooledBodyReader_ClosedReader_Operations verifies behavior
// when operations are called after close
func TestPooledBodyReader_ClosedReader_Operations(t *testing.T) {
	// Create a mock reader
	mockReader := &mockArticleBodyReader{}

	reader := &pooledBodyReader{
		reader:  mockReader,
		closeCh: make(chan struct{}),
	}

	// Close the reader first
	err := reader.Close()
	if err != nil {
		t.Fatalf("Close() returned unexpected error: %v", err)
	}

	// Now try operations - they should return EOF due to closed flag
	_, err = reader.GetYencHeaders()
	if err != io.EOF {
		t.Errorf("GetYencHeaders() after close error = %v, want io.EOF", err)
	}

	buffer := make([]byte, 1024)
	n, err := reader.Read(buffer)
	if err != io.EOF {
		t.Errorf("Read() after close error = %v, want io.EOF", err)
	}
	if n != 0 {
		t.Errorf("Read() after close read %d bytes, want 0", n)
	}
}

// TestPooledBodyReader_BytesReadTracking verifies that bytes read
// are tracked correctly, even with nil reader
func TestPooledBodyReader_BytesReadTracking(t *testing.T) {
	tests := []struct {
		name   string
		reader nntpcli.ArticleBodyReader
		want   int64
	}{
		{
			name:   "nil reader - no bytes tracked",
			reader: nil,
			want:   0,
		},
		{
			name:   "mock reader - bytes tracked",
			reader: &mockArticleBodyReaderWithData{data: []byte("test data")},
			want:   9,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reader := &pooledBodyReader{
				reader:  tt.reader,
				closeCh: make(chan struct{}),
			}

			buffer := make([]byte, 1024)
			n, _ := reader.Read(buffer)

			got := reader.bytesRead.Load()
			if got != int64(n) {
				t.Errorf("bytesRead = %d, want %d", got, n)
			}

			if tt.reader != nil && got != tt.want {
				t.Errorf("bytesRead = %d, want %d", got, tt.want)
			}
		})
	}
}

// Mock implementations for testing

type mockArticleBodyReader struct {
	closed atomic.Bool
}

func (m *mockArticleBodyReader) GetYencHeaders() (nntpcli.YencHeaders, error) {
	if m.closed.Load() {
		return nntpcli.YencHeaders{}, io.EOF
	}
	return nntpcli.YencHeaders{}, nil
}

func (m *mockArticleBodyReader) Read(p []byte) (int, error) {
	if m.closed.Load() {
		return 0, io.EOF
	}
	return 0, io.EOF
}

func (m *mockArticleBodyReader) Close() error {
	m.closed.Store(true)
	return nil
}

type mockArticleBodyReaderWithData struct {
	data   []byte
	offset int
	closed atomic.Bool
}

func (m *mockArticleBodyReaderWithData) GetYencHeaders() (nntpcli.YencHeaders, error) {
	if m.closed.Load() {
		return nntpcli.YencHeaders{}, io.EOF
	}
	return nntpcli.YencHeaders{}, nil
}

func (m *mockArticleBodyReaderWithData) Read(p []byte) (int, error) {
	if m.closed.Load() {
		return 0, io.EOF
	}
	if m.offset >= len(m.data) {
		return 0, io.EOF
	}
	n := copy(p, m.data[m.offset:])
	m.offset += n
	return n, nil
}

func (m *mockArticleBodyReaderWithData) Close() error {
	m.closed.Store(true)
	return nil
}
