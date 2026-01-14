package nntppool

import (
	"bytes"
	"context"
	"io"
	"net"
	"time"
)

type Request struct {
	Ctx context.Context

	Payload []byte
	RespCh  chan Response

	// Optional: decoded body bytes are streamed here. If nil, they are buffered into Response.Body.
	BodyWriter io.Writer

	// Optional: callback for when yEnc headers are parsed
	OnYencHeader func(*YencHeader)
}

type YencHeader struct {
	FileName  string
	FileSize  int64
	Part      int64
	PartBegin int64
	PartSize  int64
	Total     int64
}

type Response struct {
	StatusCode int
	Status     string

	// For non-body multiline responses (CAPABILITIES, etc).
	Lines []string

	// Decoded payload bytes (only if Request.BodyWriter == nil).
	Body bytes.Buffer

	// Decoder metadata/status gathered while parsing.
	Meta NNTPResponse

	Err     error
	Request *Request
}

type Auth struct {
	Username string
	Password string
}

// ConnFactory is used by Client to create connections.
type ConnFactory func(ctx context.Context) (net.Conn, error)

type ProviderType int

const (
	ProviderPrimary ProviderType = iota
	ProviderBackup
)

// SpeedTestStats contains metrics from a speed test run.
type SpeedTestStats struct {
	TotalBytes     int64
	Duration       time.Duration
	BytesPerSecond float64
	SuccessCount   int32
	FailureCount   int32
}

type YencReader interface {
	io.ReadCloser
	YencHeaders() *YencHeader
}

type yencReader struct {
	*io.PipeReader
	headerCh chan *YencHeader
	cached   *YencHeader
}

func (b *yencReader) YencHeaders() *YencHeader {
	if b.cached != nil {
		return b.cached
	}
	select {
	case h := <-b.headerCh:
		b.cached = h
		return h
	default:
		return nil
	}
}
