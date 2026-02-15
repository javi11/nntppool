package nntppool

import (
	"context"
	"errors"
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

// BodyPriority is like Body but enqueues on the priority channel so idle
// connections pick it up before normal requests.
func (c *Client) BodyPriority(ctx context.Context, messageID string, onMeta ...func(YEncMeta)) (*ArticleBody, error) {
	payload := []byte("BODY <" + messageID + ">\r\n")
	var respCh <-chan Response
	if len(onMeta) > 0 {
		respCh = c.SendPriority(ctx, payload, nil, onMeta[0])
	} else {
		respCh = c.SendPriority(ctx, payload, nil)
	}
	body, err := c.finishBody(messageID, nil, respCh)
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
	return c.finishBody(messageID, w, respCh)
}

// finishBody waits on respCh and builds the ArticleBody result.
func (c *Client) finishBody(messageID string, w io.Writer, respCh <-chan Response) (*ArticleBody, error) {
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

// PostYenc sends a yEnc-encoded article to the server using the NNTP POST command.
// The body is yEnc-encoded on the fly using the provided metadata. yEnc encoding
// avoids '.' at start of lines, so no dot-stuffing is needed.
// The body reader is consumed exactly once; on failure, the caller must retry
// with a fresh reader.
func (c *Client) PostYenc(ctx context.Context, headers PostHeaders, body io.Reader, meta rapidyenc.Meta) (*PostResult, error) {
	pr, pw := io.Pipe()
	go func() {
		var err error
		defer func() { _ = pw.CloseWithError(err) }()

		if _, err = headers.WriteTo(pw); err != nil {
			return
		}
		var enc *rapidyenc.Encoder
		enc, err = rapidyenc.NewEncoder(pw, meta)
		if err != nil {
			return
		}
		if _, err = io.Copy(enc, body); err != nil {
			return
		}
		if err = enc.Close(); err != nil {
			return
		}
		_, err = pw.Write([]byte("\r\n.\r\n"))
	}()

	respCh := c.sendPost(ctx, pr)
	return c.finishPost(respCh)
}

// sendPost dispatches a POST request to the first available main provider.
// POST uses FIFO dispatch (no backup fallback, no retry on protocol errors).
func (c *Client) sendPost(ctx context.Context, payloadBody io.Reader) <-chan Response {
	respCh := make(chan Response, 1)
	if ctx == nil {
		ctx = context.Background()
	}
	go c.doSendPost(ctx, payloadBody, respCh)
	return respCh
}

func (c *Client) doSendPost(ctx context.Context, payloadBody io.Reader, respCh chan Response) {
	defer close(respCh)

	mains := *c.mainGroups.Load()
	if len(mains) == 0 {
		respCh <- Response{Err: errors.New("nntp: no main providers")}
		return
	}

	// FIFO: try providers in order until one accepts.
	var lastErr error
	for _, g := range mains {
		innerCh := make(chan Response, 1)
		req := &Request{
			Ctx:         ctx,
			Payload:     []byte("POST\r\n"),
			RespCh:      innerCh,
			PayloadBody: payloadBody,
			PostMode:    true,
		}

		// Try hot channel first (non-blocking), then cold channel.
		select {
		case g.hotReqCh <- req:
		default:
			select {
			case <-c.ctx.Done():
				respCh <- Response{Err: c.ctx.Err()}
				return
			case <-ctx.Done():
				respCh <- Response{Err: ctx.Err()}
				return
			case <-g.ctx.Done():
				continue
			case g.reqCh <- req:
			}
		}

		resp, ok := <-innerCh
		if !ok {
			continue
		}
		if resp.Err != nil {
			lastErr = resp.Err
			continue
		}
		// Deliver whatever status we got (240, 440, 441, etc.).
		respCh <- resp
		return
	}

	if lastErr != nil {
		respCh <- Response{Err: fmt.Errorf("nntp: post failed: %w", lastErr)}
	} else {
		respCh <- Response{Err: errors.New("nntp: post failed: all providers exhausted")}
	}
}

// finishPost waits for the POST response and maps status codes to errors.
func (c *Client) finishPost(respCh <-chan Response) (*PostResult, error) {
	resp := <-respCh
	if resp.Err != nil {
		return nil, resp.Err
	}
	if err := toError(resp.StatusCode, resp.Status); err != nil {
		return nil, err
	}
	return &PostResult{
		StatusCode: resp.StatusCode,
		Status:     resp.Status,
	}, nil
}
