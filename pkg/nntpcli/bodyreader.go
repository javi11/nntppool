package nntpcli

import (
	"bytes"
	"fmt"
	"io"
	"sync"

	"github.com/mnightingale/rapidyenc"
)

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
	buffer      *bytes.Buffer
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

	if r.buffer != nil && r.buffer.Len() > 0 {
		n, err = r.buffer.Read(p)
		if r.buffer.Len() == 0 {
			r.buffer = nil
		}
		if n > 0 || err != nil {
			return n, err
		}
	}

	n, err = r.decoder.Read(p)
	return n, err
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
		buf := make([]byte, 4096)
		n, readErr := r.decoder.Read(buf)
		if n > 0 {
			r.buffer = bytes.NewBuffer(buf[:n])
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

	if r.decoder != nil {
		_, _ = io.Copy(io.Discard, r.decoder)
		r.decoder = nil
	}

	if r.conn != nil {
		r.conn.conn.EndResponse(r.responseID)
	}

	return nil
}
