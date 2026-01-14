package nntppool

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mnightingale/rapidyenc"
)

type Client struct {
	primaries   atomic.Value // []*Provider
	backups     atomic.Value // []*Provider
	maxInflight int
	sem         chan struct{}

	mu            sync.Mutex // Protects updates to atomic values and dead lists
	deadPrimaries []*Provider
	deadBackups   []*Provider
	closeCh       chan struct{}
}

// Compile-time check that Client implements NNTPClient interface
var _ NNTPClient = (*Client)(nil)

func NewClient(maxInflight int) *Client {
	c := &Client{
		maxInflight: maxInflight,
		closeCh:     make(chan struct{}),
	}
	c.primaries.Store(make([]*Provider, 0))
	c.backups.Store(make([]*Provider, 0))

	if maxInflight > 0 {
		c.sem = make(chan struct{}, maxInflight)
	}

	go c.healthCheckLoop()

	return c
}

func (c *Client) AddProvider(provider *Provider, tier ProviderType) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if tier == ProviderPrimary {
		current := c.primaries.Load().([]*Provider)
		newlist := make([]*Provider, len(current)+1)
		copy(newlist, current)
		newlist[len(current)] = provider
		c.primaries.Store(newlist)
	} else {
		current := c.backups.Load().([]*Provider)
		newlist := make([]*Provider, len(current)+1)
		copy(newlist, current)
		newlist[len(current)] = provider
		c.backups.Store(newlist)
	}
}

func (c *Client) RemoveProvider(provider *Provider) {
	c.mu.Lock()
	defer c.mu.Unlock()

	remove := func(current []*Provider) ([]*Provider, bool) {
		for i, p := range current {
			if p == provider {
				// Create new slice without this provider
				newlist := make([]*Provider, 0, len(current)-1)
				newlist = append(newlist, current[:i]...)
				newlist = append(newlist, current[i+1:]...)
				return newlist, true
			}
		}
		return current, false
	}

	var found bool

	// Check primaries
	currentPrimaries := c.primaries.Load().([]*Provider)
	newPrimaries, removed := remove(currentPrimaries)
	if removed {
		c.primaries.Store(newPrimaries)
		found = true
	}

	// Check backups
	currentBackups := c.backups.Load().([]*Provider)
	newBackups, removed := remove(currentBackups)
	if removed {
		c.backups.Store(newBackups)
		found = true
	}

	// Check dead lists
	newDeadPrimaries, removed := remove(c.deadPrimaries)
	if removed {
		c.deadPrimaries = newDeadPrimaries
		found = true
	}

	newDeadBackups, removed := remove(c.deadBackups)
	if removed {
		c.deadBackups = newDeadBackups
		found = true
	}

	if found {
		_ = provider.Close()
	}
}

func (c *Client) Close() {
	close(c.closeCh)

	primaries := c.primaries.Load().([]*Provider)
	for _, p := range primaries {
		_ = p.Close()
	}

	backups := c.backups.Load().([]*Provider)
	for _, p := range backups {
		_ = p.Close()
	}

	c.mu.Lock()
	for _, p := range c.deadPrimaries {
		_ = p.Close()
	}
	for _, p := range c.deadBackups {
		_ = p.Close()
	}
	c.mu.Unlock()
}

// IsArticleNotFound checks if an error is an ArticleNotFoundError.
// Returns true if the error (or any error in the chain) indicates an article was not found.
func IsArticleNotFound(err error) bool {
	var notFoundErr *ArticleNotFoundError
	return errors.As(err, &notFoundErr)
}

// Metrics returns metrics for all active providers.
func (c *Client) Metrics() map[string]ProviderMetrics {
	metrics := make(map[string]ProviderMetrics)

	// Collect from primaries
	primaries := c.primaries.Load().([]*Provider)
	for _, p := range primaries {
		metrics[p.Host] = p.Metrics()
	}

	// Collect from backups
	backups := c.backups.Load().([]*Provider)
	for _, p := range backups {
		metrics[p.Host] = p.Metrics()
	}

	return metrics
}

func (c *Client) Send(ctx context.Context, payload []byte, bodyWriter io.Writer) <-chan Response {
	respCh := make(chan Response, 1)

	if ctx == nil {
		ctx = context.Background()
	}

	req := &Request{
		Ctx:        ctx,
		Payload:    payload,
		BodyWriter: bodyWriter,
		RespCh:     respCh,
	}

	go c.sendRequest(req)

	return respCh
}

// BodyReader retrieves the body of an article by its message ID as a stream.
// It returns a reader that provides access to yEnc headers as soon as they are parsed.
func (c *Client) BodyReader(ctx context.Context, id string) (YencReader, error) {
	pr, pw := io.Pipe()
	headerCh := make(chan *YencHeader, 1)

	req := &Request{
		Ctx:        ctx,
		Payload:    []byte(fmt.Sprintf("BODY %s\r\n", c.formatID(id))),
		BodyWriter: pw,
		OnYencHeader: func(h *YencHeader) {
			select {
			case headerCh <- h:
			default:
			}
		},
		RespCh: make(chan Response, 1),
	}

	go c.sendRequest(req)

	return &yencReader{
		PipeReader: pr,
		headerCh:   headerCh,
	}, nil
}

// Body retrieves the body of an article by its message ID.
func (c *Client) Body(ctx context.Context, id string, w io.Writer) error {
	cmd := fmt.Sprintf("BODY %s\r\n", c.formatID(id))
	resp, err := c.sendSync(ctx, cmd, w)
	if err != nil {
		return err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("unexpected status code: %d %s", resp.StatusCode, resp.Status)
	}
	return nil
}

// BodyAt retrieves the body of an article by its message ID, writing to an io.WriterAt.
func (c *Client) BodyAt(ctx context.Context, id string, w io.WriterAt) error {
	cmd := fmt.Sprintf("BODY %s\r\n", c.formatID(id))
	resp, err := c.sendSync(ctx, cmd, &writerAtAdapter{w: w})
	if err != nil {
		return err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("unexpected status code: %d %s", resp.StatusCode, resp.Status)
	}
	return nil
}

// Article retrieves the header and body of an article by its message ID.
func (c *Client) Article(ctx context.Context, id string, w io.Writer) error {
	cmd := fmt.Sprintf("ARTICLE %s\r\n", c.formatID(id))
	resp, err := c.sendSync(ctx, cmd, w)
	if err != nil {
		return err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("unexpected status code: %d %s", resp.StatusCode, resp.Status)
	}
	return nil
}

// Head retrieves the headers of an article by its message ID.
func (c *Client) Head(ctx context.Context, id string) (*Response, error) {
	cmd := fmt.Sprintf("HEAD %s\r\n", c.formatID(id))
	return c.sendSync(ctx, cmd, nil)
}

// Stat checks if an article exists by its message ID.
func (c *Client) Stat(ctx context.Context, id string) (*Response, error) {
	cmd := fmt.Sprintf("STAT %s\r\n", c.formatID(id))
	return c.sendSync(ctx, cmd, nil)
}

// Group selects a newsgroup. Note: In a connection pool, this selection
// only applies to the specific connection used for this request and is
// not guaranteed to persist for subsequent requests.
func (c *Client) Group(ctx context.Context, group string) (*Response, error) {
	cmd := fmt.Sprintf("GROUP %s\r\n", group)
	return c.sendSync(ctx, cmd, nil)
}

// SpeedTest performs a download speed test using the provided article IDs.
// It downloads articles concurrently using the pool's configured concurrency,
// discards the content, and returns performance statistics.
func (c *Client) SpeedTest(ctx context.Context, articleIDs []string) (SpeedTestStats, error) {
	var stats SpeedTestStats
	var wg sync.WaitGroup
	counter := &countingDiscard{}

	start := time.Now()

	for _, id := range articleIDs {
		wg.Add(1)
		go func(msgID string) {
			defer wg.Done()
			err := c.Body(ctx, msgID, counter)
			if err != nil {
				atomic.AddInt32(&stats.FailureCount, 1)
			} else {
				atomic.AddInt32(&stats.SuccessCount, 1)
			}
		}(id)
	}

	wg.Wait()

	stats.Duration = time.Since(start)
	stats.TotalBytes = atomic.LoadInt64(&counter.n)
	if stats.Duration.Seconds() > 0 {
		stats.BytesPerSecond = float64(stats.TotalBytes) / stats.Duration.Seconds()
	}

	return stats, nil
}

func (c *Client) Date(ctx context.Context) error {
	cmd := "DATE\r\n"
	_, err := c.sendSync(ctx, cmd, nil)
	return err
}

func (c *Client) sendRequest(req *Request) {
	defer close(req.RespCh)

	// Acquire global capacity
	if c.sem != nil {
		select {
		case c.sem <- struct{}{}:
			defer func() { <-c.sem }()
		case <-req.Ctx.Done():
			return
		}
	}

	var visitedHosts []string
	var lastErr error
	var lastResp Response
	var attempted bool

	// Helper to try a list of providers
	tryProviders := func(providers []*Provider) bool { // returns true if successful
		for _, p := range providers {
			// Check if visited
			var visited bool
			for _, h := range visitedHosts {
				if h == p.Host {
					visited = true
					break
				}
			}
			if visited {
				continue
			}
			visitedHosts = append(visitedHosts, p.Host)
			attempted = true

			// Create a sub-request for each provider attempt
			attemptCh := make(chan Response, 1)
			subReq := &Request{
				Ctx:          req.Ctx,
				Payload:      req.Payload,
				BodyWriter:   req.BodyWriter,
				RespCh:       attemptCh,
				OnYencHeader: req.OnYencHeader,
			}

			ch := p.SendRequest(subReq)
			resp, ok := <-ch
			if !ok {
				lastErr = fmt.Errorf("provider %s closed unexpectedly", p.Host)
				continue
			}

			lastResp = resp
			lastErr = resp.Err

			// If success (2xx), return true
			if resp.Err == nil && resp.StatusCode >= 200 && resp.StatusCode < 300 {
				req.RespCh <- resp
				return true
			}

			// If 430 (Article Not Found) or error, we continue to next provider.
			// We store the last response/error to return if all fail.
		}
		return false
	}

	// Try primaries
	if tryProviders(c.primaries.Load().([]*Provider)) {
		return
	}

	// Try backups
	if tryProviders(c.backups.Load().([]*Provider)) {
		return
	}

	// All failed
	if attempted {
		if lastResp.Request != nil {
			// If last response was 430 (article not found), create custom error
			if lastResp.StatusCode == 430 {
				messageID := extractMessageIDFromPayload(lastResp.Request.Payload)
				lastResp.Err = &ArticleNotFoundError{
					MessageID:  messageID,
					StatusCode: lastResp.StatusCode,
					Status:     lastResp.Status,
				}
			}
			req.RespCh <- lastResp
		} else if lastErr != nil {
			req.RespCh <- Response{Err: lastErr}
		} else {
			// Should not happen if attempted is true and logic is correct
			req.RespCh <- Response{Err: fmt.Errorf("all providers failed")}
		}
	} else {
		req.RespCh <- Response{Err: fmt.Errorf("no providers available")}
	}
}

func (c *Client) healthCheckLoop() {
	ticker := time.NewTicker(60 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-c.closeCh:
			return
		case <-ticker.C:
			c.checkProviders()
		}
	}
}

func (c *Client) checkProviders() {
	c.mu.Lock()
	activePrimaries := c.primaries.Load().([]*Provider)
	activeBackups := c.backups.Load().([]*Provider)
	deadPrimaries := make([]*Provider, len(c.deadPrimaries))
	copy(deadPrimaries, c.deadPrimaries)
	deadBackups := make([]*Provider, len(c.deadBackups))
	copy(deadBackups, c.deadBackups)
	c.mu.Unlock()

	type checkResult struct {
		p     *Provider
		alive bool
	}
	results := make(chan checkResult)
	var wg sync.WaitGroup

	check := func(p *Provider) {
		defer wg.Done()
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		err := p.Date(ctx)
		results <- checkResult{p: p, alive: err == nil}
	}

	for _, p := range activePrimaries {
		wg.Add(1)
		go check(p)
	}
	for _, p := range activeBackups {
		wg.Add(1)
		go check(p)
	}
	for _, p := range deadPrimaries {
		wg.Add(1)
		go check(p)
	}
	for _, p := range deadBackups {
		wg.Add(1)
		go check(p)
	}

	go func() {
		wg.Wait()
		close(results)
	}()

	status := make(map[*Provider]bool)
	for res := range results {
		status[res.p] = res.alive
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	rebuild := func(current []*Provider, dead []*Provider) ([]*Provider, []*Provider, bool) {
		var newActive []*Provider
		var newDead []*Provider
		var changed bool

		for _, p := range current {
			isAlive, checked := status[p]
			if !checked {
				newActive = append(newActive, p)
				continue
			}
			if isAlive {
				newActive = append(newActive, p)
			} else {
				newDead = append(newDead, p)
				changed = true
			}
		}

		for _, p := range dead {
			isAlive, checked := status[p]
			if !checked {
				newDead = append(newDead, p)
				continue
			}
			if isAlive {
				newActive = append(newActive, p)
				changed = true
			} else {
				newDead = append(newDead, p)
			}
		}
		return newActive, newDead, changed
	}

	currentPrimaries := c.primaries.Load().([]*Provider)
	newPrimaries, newDeadPrimaries, changedPrimaries := rebuild(currentPrimaries, c.deadPrimaries)
	if changedPrimaries {
		c.primaries.Store(newPrimaries)
		c.deadPrimaries = newDeadPrimaries
	}

	currentBackups := c.backups.Load().([]*Provider)
	newBackups, newDeadBackups, changedBackups := rebuild(currentBackups, c.deadBackups)
	if changedBackups {
		c.backups.Store(newBackups)
		c.deadBackups = newDeadBackups
	}
}

// formatID ensures the message ID is wrapped in angle brackets.
func (c *Client) formatID(id string) string {
	if len(id) > 0 && id[0] == '<' {
		return id
	}
	return "<" + id + ">"
}

// extractMessageIDFromPayload extracts the message ID from a NNTP request payload.
func extractMessageIDFromPayload(payload []byte) string {
	// Payload format: "BODY <message-id>\r\n" or similar
	parts := bytes.Fields(payload)
	if len(parts) >= 2 {
		id := parts[1]
		// Remove angle brackets if present
		id = bytes.TrimPrefix(id, []byte("<"))
		id = bytes.TrimSuffix(id, []byte(">"))
		return string(id)
	}
	return ""
}

// sendSync sends a command and waits for the response.
func (c *Client) sendSync(ctx context.Context, cmd string, w io.Writer) (*Response, error) {
	respCh := c.Send(ctx, []byte(cmd), w)
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case resp, ok := <-respCh:
		if !ok {
			return nil, fmt.Errorf("response channel closed unexpectedly")
		}
		if resp.Err != nil {
			return nil, resp.Err
		}
		return &resp, nil
	}
}

type writerAtAdapter struct {
	w io.WriterAt
}

func (wa *writerAtAdapter) Write(p []byte) (int, error) {
	return 0, fmt.Errorf("non-sequential write not supported")
}

func (wa *writerAtAdapter) WriteAt(p []byte, off int64) (int, error) {
	return wa.w.WriteAt(p, off)
}

type countingDiscard struct {
	n int64
}

func (c *countingDiscard) Write(p []byte) (int, error) {
	atomic.AddInt64(&c.n, int64(len(p)))
	return len(p), nil
}

// Post posts an article to the NNTP server with the given headers and body.
// The headers map should contain standard NNTP headers like "From", "Newsgroups", "Subject", etc.
// The body reader provides the article content which will be transmitted with proper dot-stuffing.
//
// NNTP response codes:
//   - 240: Article posted successfully
//   - 340: Send article to be posted (intermediate response)
//   - 440: Posting not allowed
//   - 441: Posting failed
func (c *Client) Post(ctx context.Context, headers map[string]string, body io.Reader) (*Response, error) {
	if ctx == nil {
		ctx = context.Background()
	}

	// Build the complete article (headers + body + termination)
	var buf bytes.Buffer
	for key, value := range headers {
		buf.WriteString(key)
		buf.WriteString(": ")
		buf.WriteString(value)
		buf.WriteString("\r\n")
	}
	buf.WriteString("\r\n")

	if err := dotStuff(body, &buf); err != nil {
		return nil, fmt.Errorf("dotStuff: %w", err)
	}
	buf.WriteString(".\r\n")

	// Send POST command and wait for 340 response
	resp, err := c.awaitResponse(ctx, []byte("POST\r\n"))
	if err != nil {
		return nil, fmt.Errorf("POST: %w", err)
	}
	if resp.StatusCode != 340 {
		return resp, fmt.Errorf("POST: unexpected response %d %s", resp.StatusCode, resp.Status)
	}

	// Send the article
	return c.awaitResponse(ctx, buf.Bytes())
}

// awaitResponse sends a command and waits for the response.
func (c *Client) awaitResponse(ctx context.Context, cmd []byte) (*Response, error) {
	respCh := c.Send(ctx, cmd, nil)
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case resp, ok := <-respCh:
		if !ok {
			return nil, fmt.Errorf("channel closed")
		}
		if resp.Err != nil {
			return nil, resp.Err
		}
		return &resp, nil
	}
}

// PostYenc posts an article with automatic yEnc encoding.
// The body reader provides the raw binary content which will be yEnc encoded before posting.
// The opts parameter specifies yEnc encoding options including filename, file size, and part information.
//
// For single-part files, set opts.Part to 0 or 1 and opts.Total to 0 or 1.
// For multi-part files, set opts.Part and opts.Total appropriately, along with PartBegin and PartEnd.
//
// NNTP response codes:
//   - 240: Article posted successfully
//   - 340: Send article to be posted (intermediate response)
//   - 440: Posting not allowed
//   - 441: Posting failed
func (c *Client) PostYenc(ctx context.Context, headers map[string]string, body io.Reader, opts *YencOptions) (*Response, error) {
	meta, err := opts.toMeta()
	if err != nil {
		return nil, err
	}

	var encodedBody bytes.Buffer
	enc, err := rapidyenc.NewEncoder(&encodedBody, meta)
	if err != nil {
		return nil, fmt.Errorf("yEnc encoder: %w", err)
	}

	if _, err := io.Copy(enc, body); err != nil {
		return nil, fmt.Errorf("yEnc encode: %w", err)
	}

	if err := enc.Close(); err != nil {
		return nil, fmt.Errorf("yEnc close: %w", err)
	}

	return c.Post(ctx, headers, &encodedBody)
}

// dotStuff implements NNTP dot-stuffing according to RFC 3977 Section 3.1.1.
// Lines beginning with a period must have another period prepended.
// The termination sequence (single period on a line) is added by the caller.
func dotStuff(r io.Reader, w io.Writer) error {
	scanner := bufio.NewScanner(r)
	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) > 0 && line[0] == '.' {
			if _, err := w.Write([]byte{'.'}); err != nil {
				return err
			}
		}
		if _, err := w.Write(line); err != nil {
			return err
		}
		if _, err := io.WriteString(w, "\r\n"); err != nil {
			return err
		}
	}
	return scanner.Err()
}
