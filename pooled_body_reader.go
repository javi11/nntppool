package nntppool

import (
	"io"
	"sync"
	"sync/atomic"

	"github.com/javi11/nntppool/v2/pkg/nntpcli"
)

// pooledBodyReader wraps an io.ReadCloser and manages the associated pooled connection.
// It provides thread-safe access to the underlying reader using lock-free atomics.
// If Close() races with Read(), the underlying reader returns an error which propagates
// to the caller - this is acceptable behavior as the caller handles read errors.
type pooledBodyReader struct {
	reader    nntpcli.ArticleBodyReader
	conn      PooledConnection
	metrics   *PoolMetrics  // Reference to pool metrics for recording
	bytesRead atomic.Int64  // Total bytes read from the article
	closeOnce sync.Once     // Ensures Close is only called once
	closed    atomic.Bool   // Tracks if reader has been closed (atomic for lock-free check)
	closeCh   chan struct{} // Signals when close is in progress
}

func (r *pooledBodyReader) GetYencHeaders() (nntpcli.YencHeaders, error) {
	if r.reader == nil {
		return nntpcli.YencHeaders{}, io.EOF
	}

	if r.closed.Load() {
		return nntpcli.YencHeaders{}, io.EOF
	}

	// Check if close is in progress
	select {
	case <-r.closeCh:
		return nntpcli.YencHeaders{}, io.EOF
	default:
	}

	// If Close() races here, underlying reader returns error which propagates to caller
	return r.reader.GetYencHeaders()
}

func (r *pooledBodyReader) Read(p []byte) (n int, err error) {
	if r.reader == nil {
		return 0, io.EOF
	}

	if r.closed.Load() {
		return 0, io.EOF
	}

	// Check if close is in progress
	select {
	case <-r.closeCh:
		return 0, io.EOF
	default:
	}

	// If Close() races here, underlying reader returns error which propagates to caller
	n, err = r.reader.Read(p)

	if n > 0 {
		r.bytesRead.Add(int64(n))
	}

	return n, err
}

func (r *pooledBodyReader) Close() error {
	var closeErr error

	r.closeOnce.Do(func() {
		// Set closed flag atomically (prevents new operations from starting)
		r.closed.Store(true)

		// Signal any waiting operations
		close(r.closeCh)

		// Close the reader first
		if r.reader != nil {
			closeErr = r.reader.Close()
		}

		// Record metrics if close was successful
		if closeErr == nil && r.metrics != nil {
			bytesRead := r.bytesRead.Load()
			if bytesRead > 0 {
				// Get provider host if connection is available
				providerHost := ""
				if r.conn != nil {
					providerHost = r.conn.Provider().Host
				}
				r.metrics.RecordDownload(bytesRead, providerHost)
				r.metrics.RecordArticleDownloaded(providerHost)
			}
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
