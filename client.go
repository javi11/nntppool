package nntppool

import (
	"context"
	"fmt"
	"io"
	"strconv"
	"strings"

	"github.com/mnightingale/rapidyenc"
)

// ArticleEncoding describes the transfer encoding detected in an article body.
type ArticleEncoding int

const (
	EncodingUnknown ArticleEncoding = iota
	EncodingYEnc
	EncodingUU
)

// ArticleBody holds the decoded result of a BODY command.
type ArticleBody struct {
	MessageID string

	// Decoded payload bytes. Nil when the body was streamed to an io.Writer.
	Bytes []byte

	BytesDecoded  int
	BytesConsumed int
	Encoding      ArticleEncoding

	// yEnc metadata from =ybegin/=ypart (zero values when not yEnc).
	YEnc YEncMeta

	CRC         uint32
	ExpectedCRC uint32
	CRCValid    bool // true when ExpectedCRC != 0 && CRC == ExpectedCRC

	byteBuf []byte // internal; transferred to Bytes in Body()
}

// ArticleHead holds the parsed result of a HEAD command.
type ArticleHead struct {
	MessageID string
	Headers   map[string][]string // RFC 5322 headers with folding resolved
}

// StatResult holds the parsed result of a STAT command.
type StatResult struct {
	MessageID string
	Number    int64 // article number from response (0 if no group selected)
}

// BodyResult is the result type for BodyAsync.
type BodyResult struct {
	Body *ArticleBody
	Err  error
}

// Body retrieves and decodes an article body, buffering the decoded bytes in memory.
// An optional onMeta callback is invoked with yEnc metadata before body decoding begins.
func (c *Client) Body(ctx context.Context, messageID string, onMeta ...func(YEncMeta)) (*ArticleBody, error) {
	var metaFn func(YEncMeta)
	if len(onMeta) > 0 {
		metaFn = onMeta[0]
	}
	body, err := c.doBody(ctx, messageID, nil, metaFn)
	if body != nil {
		body.Bytes = body.byteBuf
		body.byteBuf = nil
	}
	return body, err
}

// BodyStream retrieves and decodes an article body, streaming decoded bytes to w.
// The returned ArticleBody contains metadata but Bytes will be nil.
// An optional onMeta callback is invoked with yEnc metadata before body decoding begins.
func (c *Client) BodyStream(ctx context.Context, messageID string, w io.Writer, onMeta ...func(YEncMeta)) (*ArticleBody, error) {
	if w == nil {
		return nil, fmt.Errorf("nntp: BodyStream requires a non-nil writer")
	}
	var metaFn func(YEncMeta)
	if len(onMeta) > 0 {
		metaFn = onMeta[0]
	}
	return c.doBody(ctx, messageID, w, metaFn)
}

// BodyAsync returns a channel that will receive exactly one BodyResult.
// The body is streamed to w (use io.Discard to discard decoded bytes).
// This preserves the fan-out pattern used by Send.
// An optional onMeta callback is invoked with yEnc metadata before body decoding begins.
func (c *Client) BodyAsync(ctx context.Context, messageID string, w io.Writer, onMeta ...func(YEncMeta)) <-chan BodyResult {
	var metaFn func(YEncMeta)
	if len(onMeta) > 0 {
		metaFn = onMeta[0]
	}
	ch := make(chan BodyResult, 1)
	go func() {
		body, err := c.doBody(ctx, messageID, w, metaFn)
		ch <- BodyResult{Body: body, Err: err}
		close(ch)
	}()
	return ch
}

// Head retrieves the headers of an article.
func (c *Client) Head(ctx context.Context, messageID string) (*ArticleHead, error) {
	payload := []byte("HEAD <" + messageID + ">\r\n")
	respCh := c.Send(ctx, payload, nil)

	resp := <-respCh
	if resp.Err != nil {
		return nil, resp.Err
	}
	if err := toError(resp.StatusCode, resp.Status); err != nil {
		return nil, err
	}

	return &ArticleHead{
		MessageID: messageID,
		Headers:   parseHeaders(resp.Lines),
	}, nil
}

// Stat checks whether an article exists without transferring its contents.
func (c *Client) Stat(ctx context.Context, messageID string) (*StatResult, error) {
	payload := []byte("STAT <" + messageID + ">\r\n")
	respCh := c.Send(ctx, payload, nil)

	resp := <-respCh
	if resp.Err != nil {
		return nil, resp.Err
	}
	if err := toError(resp.StatusCode, resp.Status); err != nil {
		return nil, err
	}

	result := &StatResult{MessageID: messageID}

	// Parse "223 <number> <message-id>" from the status line.
	parts := strings.SplitN(resp.Status, " ", 4)
	if len(parts) >= 2 {
		result.Number, _ = strconv.ParseInt(parts[1], 10, 64)
	}
	if len(parts) >= 3 {
		result.MessageID = strings.Trim(parts[2], "<>")
	}

	return result, nil
}

// doBody is the shared implementation for Body, BodyStream, and BodyAsync.
// When w is nil, decoded bytes are buffered in the Response.Body field.
func (c *Client) doBody(ctx context.Context, messageID string, w io.Writer, onMeta func(YEncMeta)) (*ArticleBody, error) {
	payload := []byte("BODY <" + messageID + ">\r\n")
	var respCh <-chan Response
	if onMeta != nil {
		respCh = c.Send(ctx, payload, w, onMeta)
	} else {
		respCh = c.Send(ctx, payload, w)
	}

	resp := <-respCh
	if resp.Err != nil {
		return nil, resp.Err
	}
	if err := toError(resp.StatusCode, resp.Status); err != nil {
		return nil, err
	}

	body := &ArticleBody{
		MessageID:     messageID,
		BytesDecoded:  resp.Meta.BytesDecoded,
		BytesConsumed: resp.Meta.BytesConsumed,
		Encoding:      mapFormat(resp.Meta.Format),
		YEnc:          resp.Meta.YEnc,
		CRC:           resp.Meta.CRC,
		ExpectedCRC:   resp.Meta.ExpectedCRC,
	}
	body.CRCValid = body.ExpectedCRC != 0 && body.CRC == body.ExpectedCRC

	// When w was nil, the decoded bytes were buffered in resp.Body.
	if w == nil {
		buf := resp.Body.Bytes()
		if len(buf) > 0 {
			body.byteBuf = buf
		}
	}

	// Return both the body and a CRC error so callers get data but are warned.
	if body.ExpectedCRC != 0 && body.CRC != body.ExpectedCRC {
		return body, ErrCRCMismatch
	}

	return body, nil
}

// mapFormat converts from the internal rapidyenc.Format to the public ArticleEncoding.
func mapFormat(f rapidyenc.Format) ArticleEncoding {
	switch f {
	case rapidyenc.FormatYenc:
		return EncodingYEnc
	case rapidyenc.FormatUU:
		return EncodingUU
	default:
		return EncodingUnknown
	}
}

// parseHeaders parses RFC 5322 header lines, resolving continuation lines
// (lines starting with whitespace are folded into the previous header value).
func parseHeaders(lines []string) map[string][]string {
	headers := make(map[string][]string)
	var currentKey string

	for _, line := range lines {
		if len(line) == 0 {
			continue
		}

		// Continuation line: starts with space or tab.
		if line[0] == ' ' || line[0] == '\t' {
			if currentKey != "" {
				values := headers[currentKey]
				if len(values) > 0 {
					values[len(values)-1] += " " + strings.TrimSpace(line)
				}
			}
			continue
		}

		// New header line.
		key, value, found := strings.Cut(line, ":")
		if !found {
			continue
		}
		key = strings.TrimSpace(key)
		value = strings.TrimSpace(value)
		currentKey = key
		headers[key] = append(headers[key], value)
	}

	return headers
}
