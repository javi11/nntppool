package nntppool

import (
	"io"
	"sync"

	"github.com/javi11/nntpcli"
)

// pooledBodyReader wraps an io.ReadCloser and manages the associated pooled connection
type pooledBodyReader struct {
	reader    nntpcli.ArticleBodyReader
	conn      PooledConnection
	closeOnce sync.Once // Ensures Close is only called once
	closed    bool      // Tracks if reader has been closed
	mu        sync.RWMutex
}

func (r *pooledBodyReader) GetYencHeaders() (nntpcli.YencHeaders, error) {
	r.mu.RLock()
	defer r.mu.Unlock()

	if r.closed {
		return nntpcli.YencHeaders{}, io.EOF
	}

	return r.reader.GetYencHeaders()
}

func (r *pooledBodyReader) Read(p []byte) (n int, err error) {
	r.mu.RLock()
	defer r.mu.Unlock()

	if r.closed {
		return 0, io.EOF
	}
	return r.reader.Read(p)
}

func (r *pooledBodyReader) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	var closeErr error

	r.closeOnce.Do(func() {
		r.closed = true

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
