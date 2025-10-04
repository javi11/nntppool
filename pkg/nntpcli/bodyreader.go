package nntpcli

import (
	"bytes"
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
	metrics     *Metrics
	decoder     *rapidyenc.Decoder
	conn        *connection
	responseID  uint
	buffer      *bytes.Buffer
	headersRead bool
	yencHeaders *YencHeaders
	closed      bool
}

func (r *articleBodyReader) Read(p []byte) (n int, err error) {
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
			r.metrics.RecordDownload(int64(n))

			return n, err
		}
	}

	n, err = r.decoder.Read(p)
	if n > 0 {
		r.metrics.RecordDownload(int64(n))
	}

	return n, err
}

func (r *articleBodyReader) GetYencHeaders() (YencHeaders, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.yencHeaders != nil {
		return *r.yencHeaders, nil
	}

	if !r.headersRead {
		buf := make([]byte, 4096)
		n, err := r.decoder.Read(buf)
		if n > 0 {
			r.buffer = bytes.NewBuffer(buf[:n])
		}
		r.headersRead = true

		if err != nil && err != io.EOF {
			return YencHeaders{}, err
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

func (r *articleBodyReader) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.closed {
		return nil
	}

	r.closed = true

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
