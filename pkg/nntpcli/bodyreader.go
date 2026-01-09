package nntpcli

import (
	"bytes"
	"fmt"
	"io"
	"sync"

	"github.com/mnightingale/rapidyenc"
)

// peekBufferSize is optimized for typical yenc header sizes (~50-150 bytes).
// 512 bytes covers edge cases while reducing memory usage 8x vs the previous 4KB.
const peekBufferSize = 512

// peekBufferPool provides reusable buffers for GetYencHeaders peek operation.
var peekBufferPool = sync.Pool{
	New: func() any {
		buf := make([]byte, peekBufferSize)
		return &buf
	},
}

func acquirePeekBuffer() *[]byte {
	return peekBufferPool.Get().(*[]byte)
}

func releasePeekBuffer(buf *[]byte) {
	if buf != nil && len(*buf) == peekBufferSize {
		peekBufferPool.Put(buf)
	}
}

type YencHeaders struct {
	FileName   string
	FileSize   int64
	PartNumber int64
	TotalParts int64
	Offset     int64
	PartSize   int64
	Hash       uint32
}

type ArticleBodyReader interface {
	io.ReadCloser
	GetYencHeaders() (YencHeaders, error)
}

type articleBodyReader struct {
	mu          sync.Mutex
	decoder     *rapidyenc.Decoder
	conn        *connection
	responseID  uint

	// reader is the effective reader after GetYencHeaders is called.
	// Before GetYencHeaders: reader == nil (use decoder directly)
	// After GetYencHeaders: reader == io.MultiReader(peekData, decoder)
	reader   io.Reader
	peekBuf  *[]byte       // Pooled buffer for GetYencHeaders (returned to pool on Close)
	peekData *bytes.Reader // Read-only view of peeked data

	headersRead bool
	yencHeaders *YencHeaders
	closed      bool
}

func (r *articleBodyReader) Read(p []byte) (n int, err error) {
	defer func() {
		if rec := recover(); rec != nil {
			err = fmt.Errorf("panic in Read: %v", rec)
			n = 0
		}
	}()

	r.mu.Lock()
	defer r.mu.Unlock()

	if r.closed {
		return 0, io.EOF
	}

	// Use effective reader if GetYencHeaders was called (MultiReader chains peeked data + decoder)
	if r.reader != nil {
		return r.reader.Read(p)
	}
	// GetYencHeaders not called, read directly from decoder
	return r.decoder.Read(p)
}

func (r *articleBodyReader) GetYencHeaders() (headers YencHeaders, err error) {
	defer func() {
		if rec := recover(); rec != nil {
			err = fmt.Errorf("panic in GetYencHeaders: %v", rec)
			headers = YencHeaders{}
		}
	}()

	r.mu.Lock()
	defer r.mu.Unlock()

	if r.yencHeaders != nil {
		return *r.yencHeaders, nil
	}

	if !r.headersRead {
		// Acquire buffer from pool (512 bytes, optimized for typical yenc headers)
		r.peekBuf = acquirePeekBuffer()
		buf := *r.peekBuf

		n, readErr := r.decoder.Read(buf)

		if n > 0 {
			// Create read-only view of peeked data (no buffer content allocation)
			r.peekData = bytes.NewReader(buf[:n])
			// Chain peeked data with remaining decoder data
			r.reader = io.MultiReader(r.peekData, r.decoder)
		} else {
			// No data read, use decoder directly
			r.reader = r.decoder
			// Return buffer to pool immediately if unused
			releasePeekBuffer(r.peekBuf)
			r.peekBuf = nil
		}

		r.headersRead = true

		if readErr != nil && readErr != io.EOF {
			return YencHeaders{}, readErr
		}
	}

	r.yencHeaders = &YencHeaders{
		FileName:   r.decoder.Meta.FileName,
		FileSize:   r.decoder.Meta.FileSize,
		PartNumber: r.decoder.Meta.PartNumber,
		TotalParts: r.decoder.Meta.TotalParts,
		Offset:     r.decoder.Meta.Offset,
		PartSize:   r.decoder.Meta.PartSize,
		Hash:       r.decoder.Meta.Hash,
	}

	return *r.yencHeaders, nil
}

func (r *articleBodyReader) Close() (err error) {
	defer func() {
		if rec := recover(); rec != nil {
			err = fmt.Errorf("panic in Close: %v", rec)
		}
	}()

	r.mu.Lock()
	defer r.mu.Unlock()

	if r.closed {
		return nil
	}

	r.closed = true

	// Return peek buffer to pool
	if r.peekBuf != nil {
		releasePeekBuffer(r.peekBuf)
		r.peekBuf = nil
	}

	// Clear references
	r.reader = nil
	r.peekData = nil

	if r.decoder != nil {
		_, _ = io.Copy(io.Discard, r.decoder)
		rapidyenc.ReleaseDecoder(r.decoder)
		r.decoder = nil
	}

	if r.conn != nil {
		r.conn.conn.EndResponse(r.responseID)
	}

	return nil
}
