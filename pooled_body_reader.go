package nntppool

import (
	"io"
	"sync"
	"sync/atomic"

	"github.com/javi11/nntpcli"
)

// pooledBodyReader wraps an io.ReadCloser and manages the associated pooled connection
type pooledBodyReader struct {
	reader    nntpcli.ArticleBodyReader
	conn      PooledConnection
	closeOnce sync.Once  // Ensures Close is only called once
	closed    atomic.Bool // Tracks if reader has been closed (atomic for lock-free check)
	closeCh   chan struct{} // Signals when close is in progress
	mu        sync.Mutex    // Protects Close() operations only
}

func (r *pooledBodyReader) GetYencHeaders() (nntpcli.YencHeaders, error) {
	// Fast path: check if already closed (lock-free)
	if r.closed.Load() {
		return nntpcli.YencHeaders{}, io.EOF
	}

	// Check if close is in progress
	select {
	case <-r.closeCh:
		return nntpcli.YencHeaders{}, io.EOF
	default:
	}

	// Safe to call reader method - if Close() runs now, closeCh will signal
	return r.reader.GetYencHeaders()
}

func (r *pooledBodyReader) Read(p []byte) (n int, err error) {
	// Fast path: check if already closed (lock-free)
	if r.closed.Load() {
		return 0, io.EOF
	}

	// Check if close is in progress
	select {
	case <-r.closeCh:
		return 0, io.EOF
	default:
	}

	// Safe to call reader method - if Close() runs now, closeCh will signal
	return r.reader.Read(p)
}

func (r *pooledBodyReader) Close() error {
	var closeErr error

	r.closeOnce.Do(func() {
		// Set closed flag atomically (prevents new operations from starting)
		r.closed.Store(true)

		// Signal any in-progress Read/GetYencHeaders operations
		close(r.closeCh)

		// Close the reader first
		if r.reader != nil {
			closeErr = r.reader.Close()
		}

		// Handle connection cleanup based on reader close result
		if r.conn != nil {
			if closeErr != nil {
				// If reader failed to close properly, destroy the connection
				// to prevent potential corruption from being returned to pool
				_ = r.conn.Close()
			} else {
				// Reader closed successfully, connection can be reused
				if freeErr := r.conn.Free(); freeErr != nil {
					// If freeing failed, close the connection to prevent leak
					_ = r.conn.Close()
				}
			}
		}
	})

	return closeErr
}
