package nntppool

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"
	"time"

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
	Number    int64  // article number from response (0 if no group selected)
	Provider  string // provider name that returned the successful response
}

// BodyResult is the result type for FetchAsync.
type BodyResult struct {
	Body *ArticleBody
	Err  error
}

// Req is one article request: which article, on which lane, and where its
// decoded bytes should go. Only MessageID is required — the zero value of
// every other field is a buffered fetch on the normal lane, so a caller with
// no opinion states none.
type Req struct {
	// MessageID is the article's message-ID without angle brackets.
	MessageID string

	// Lane selects the request queue. LaneNormal, the zero value, is the
	// default; see Lane for what each one is for.
	Lane Lane

	// Writer, when non-nil, receives decoded bytes as each wire read is
	// decoded, so a caller can serve the head of an article before its tail
	// has arrived. ArticleBody.Bytes is then nil. When Writer is nil the
	// decoded bytes are buffered and returned in ArticleBody.Bytes instead.
	//
	// A fetch that has already written bytes to Writer never fails over to
	// another provider, since re-streaming would duplicate them: the error is
	// returned instead, and a caller wanting a second attempt must supply a
	// fresh writer.
	//
	// Ignored by Exists, which transfers no payload.
	Writer io.Writer

	// OnMeta, when non-nil, is called with the yEnc metadata parsed from
	// =ybegin/=ypart, before body decoding begins.
	//
	// Ignored by Exists, which transfers no payload.
	OnMeta func(YEncMeta)
}

// ErrNoMessageID is returned by Fetch, FetchAsync, and Exists when Req omits
// the message-ID. Sending the resulting "BODY <>" would cost a round-trip to
// learn what the caller already knew.
var ErrNoMessageID = errors.New("nntp: Req.MessageID is required")

// Fetch retrieves and decodes an article body.
//
// With Req.Writer nil the decoded bytes are buffered and returned in
// ArticleBody.Bytes; with a writer set they are streamed to it as they decode
// and Bytes is nil. Req.Lane selects the queue.
//
// A 430 from every provider surfaces as ErrArticleNotFound. A body that
// arrived but failed its yEnc CRC is returned alongside ErrCRCMismatch, so a
// caller may inspect or salvage the payload rather than only learn it was bad.
func (c *Client) Fetch(ctx context.Context, r Req) (*ArticleBody, error) {
	if r.MessageID == "" {
		return nil, ErrNoMessageID
	}
	body, err := c.finishBody(r.MessageID, r.Writer, c.send(ctx, bodyPayload(r.MessageID), r.Writer, r.OnMeta, r.Lane))
	if body != nil && r.Writer == nil {
		body.Bytes = body.byteBuf
		body.byteBuf = nil
	}
	return body, err
}

// FetchAsync is Fetch on its own goroutine, returning a channel that receives
// exactly one BodyResult and is then closed. It is the fan-out form: a caller
// dispatching many segments at once collects them as they land instead of
// serialising on each.
func (c *Client) FetchAsync(ctx context.Context, r Req) <-chan BodyResult {
	ch := make(chan BodyResult, 1)
	go func() {
		defer close(ch)
		body, err := c.Fetch(ctx, r)
		ch <- BodyResult{Body: body, Err: err}
	}()
	return ch
}

// Exists reports whether an article is retrievable from at least one provider,
// without transferring its body. Req.Lane applies; Req.Writer and Req.OnMeta
// are ignored.
//
// An article no provider holds is not an error case for the pool but an
// answer: it returns a nil result wrapping ErrArticleNotFound, so a bulk
// liveness sweep reads a miss as a verdict rather than a failure.
func (c *Client) Exists(ctx context.Context, r Req) (*StatResult, error) {
	if r.MessageID == "" {
		return nil, ErrNoMessageID
	}
	return parseStat(r.MessageID, c.sendSync(ctx, statPayload(r.MessageID), r.Lane))
}

// ExistsAsync is Exists on its own goroutine, mirroring FetchAsync. For a
// slice of message-IDs prefer ExistsMany, which bounds concurrency to the
// pool's STAT pipeline capacity instead of dispatching all of them at once.
func (c *Client) ExistsAsync(ctx context.Context, r Req) <-chan ExistsResult {
	ch := make(chan ExistsResult, 1)
	go func() {
		defer close(ch)
		res, err := c.Exists(ctx, r)
		ch <- ExistsResult{MessageID: r.MessageID, Result: res, Err: err}
	}()
	return ch
}

// Head retrieves the headers of an article.
func (c *Client) Head(ctx context.Context, messageID string) (*ArticleHead, error) {
	if messageID == "" {
		return nil, ErrNoMessageID
	}
	respCh := c.send(ctx, []byte("HEAD <"+messageID+">\r\n"), nil, nil, LaneNormal)

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

// bodyPayload builds the wire payload for a BODY command.
func bodyPayload(messageID string) []byte {
	return []byte("BODY <" + messageID + ">\r\n")
}

// statPayload builds the wire payload for a STAT command.
func statPayload(messageID string) []byte {
	return []byte("STAT <" + messageID + ">\r\n")
}

// parseStat maps a STAT Response to a StatResult. A 430/423 (article not found)
// is returned as ErrArticleNotFound with a nil result; callers doing bulk
// existence checks treat that as a normal miss rather than a fatal error.
func parseStat(messageID string, resp Response) (*StatResult, error) {
	if resp.Err != nil {
		return nil, resp.Err
	}
	if err := toError(resp.StatusCode, resp.Status); err != nil {
		return nil, err
	}

	result := &StatResult{MessageID: messageID}
	if resp.Request != nil {
		result.Provider = resp.Request.providerName
	}

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
	return c.postYenc(ctx, headers, body, meta, nil)
}

// PostYencTo sends a yEnc-encoded article to a specific provider using the
// NNTP POST command. The provider name matches the provider's configured Name
// (or its derived name when Name is empty) and may identify either a main or
// backup provider. The request does not fall back to another provider.
// The body reader is consumed exactly once; on failure, the caller must retry
// with a fresh reader.
func (c *Client) PostYencTo(ctx context.Context, provider string, headers PostHeaders, body io.Reader, meta rapidyenc.Meta) (*PostResult, error) {
	target := c.findGroup(provider)
	if target == nil {
		return nil, fmt.Errorf("nntp: provider %q not found", provider)
	}
	return c.postYenc(ctx, headers, body, meta, target)
}

func (c *Client) postYenc(ctx context.Context, headers PostHeaders, body io.Reader, meta rapidyenc.Meta, target *providerGroup) (*PostResult, error) {
	pr, pw := io.Pipe()
	// Unblocks the writer goroutine below if pr is never read (e.g. dispatch
	// never reaches a connection); no-op once the body has been read to EOF.
	defer func() { _ = pr.CloseWithError(errors.New("nntp: post abandoned")) }()
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
		// rapidyenc's Close already terminates the last line with CRLF, so we
		// only need the dot-line terminator here. Writing "\r\n.\r\n" would
		// inject a stray blank line before the dot.
		_, err = pw.Write([]byte(".\r\n"))
	}()

	respCh := c.sendPost(ctx, pr, target)
	return c.finishPost(respCh)
}

// sendPost dispatches a POST request using the configured dispatch strategy
// so concurrent calls are spread across all provider connections.
// No backup fallback, no retry on protocol errors.
func (c *Client) sendPost(ctx context.Context, payloadBody io.Reader, target *providerGroup) <-chan Response {
	respCh := make(chan Response, 1)
	if ctx == nil {
		ctx = context.Background()
	}
	go c.doSendPost(ctx, payloadBody, target, respCh)
	return respCh
}

func (c *Client) doSendPost(ctx context.Context, payloadBody io.Reader, target *providerGroup, respCh chan Response) {
	defer close(respCh)

	mains := *c.mainGroups.Load()
	if target != nil {
		mains = []*providerGroup{target}
	}
	n := len(mains)
	if n == 0 {
		respCh <- Response{Err: errors.New("nntp: no main providers")}
		return
	}

	// Pick start index using the same dispatch strategy as normal requests so
	// concurrent POST calls are spread across all provider connections.
	var start int
	switch c.dispatch {
	case DispatchFIFO:
		for i, g := range mains {
			if g.gate.available.Load() > 0 {
				start = i
				break
			}
		}
	default: // DispatchRoundRobin
		// Sized to len(mains), unlike a fixed-size array — never overflows.
		cumWeights, totalW := dispatchWeights(mains, c.speedAware)
		if totalW == 0 {
			start = 0 // all providers quota-exceeded; let the loop below fail each
		} else {
			slot := int(c.nextIdx.Add(1) % uint64(totalW))
			start = sort.SearchInts(cumWeights, slot+1)
		}
	}

	var lastErr error
	for attempt := range n {
		idx := (start + attempt) % n
		g := mains[idx]
		innerCh := make(chan Response, 1)
		// attemptDeadline is what bounds the pre-first-byte read: with a
		// cancel-only caller context and a server that never answers the POST,
		// a deadline-less request would block the reader in Read forever.
		req := &Request{
			Ctx:             ctx,
			Payload:         []byte("POST\r\n"),
			RespCh:          innerCh,
			PayloadBody:     payloadBody,
			PostMode:        true,
			attemptDeadline: time.Now().Add(g.attemptTimeout()),
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

		// g.reqCh is buffered, so the send above can succeed with no
		// connection reading it (e.g. a stalled dial) — guard the receive on
		// ctx too, or it blocks forever. Abandoning innerCh here is safe: a
		// late write from the connection side is non-blocking and its close
		// is panic-safe (see safeClose in nntp.go).
		var resp Response
		var ok bool
		select {
		case resp, ok = <-innerCh:
		case <-c.ctx.Done():
			respCh <- Response{Err: c.ctx.Err()}
			return
		case <-ctx.Done():
			respCh <- Response{Err: ctx.Err()}
			return
		}
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
