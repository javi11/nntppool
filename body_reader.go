package nntppool

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/javi11/nntppool/v3/pkg/nntpcli"
)

// YencMeta contains metadata extracted from yenc headers.
type YencMeta struct {
	// From =ybegin header
	FileName string // Original filename
	FileSize int64  // Total file size in bytes
	Part     int64  // Part number (0 if single-part)
	Total    int64  // Total number of parts (0 if single-part)

	// From =ypart header
	PartBegin int64 // Byte offset where this part starts (0-indexed)
	PartSize  int64 // Size of this part in bytes

	// From =yend header (available after reading completes)
	ExpectedCRC uint32 // CRC from yenc footer (pcrc32 or crc32)

	// Calculated during decoding
	ActualCRC    uint32 // CRC calculated from decoded data
	BytesDecoded int    // Total bytes decoded
}

// BodyResult wraps an article body reader with yenc metadata.
// The reader streams decoded article body data.
// Metadata is available via Meta() after headers are parsed.
type BodyResult struct {
	pipeReader *io.PipeReader
	pipeWriter *io.PipeWriter

	meta     YencMeta
	metaCh   chan struct{} // closed when metadata is available
	metaOnce sync.Once

	err    error
	errMu  sync.RWMutex
	cancel context.CancelFunc

	wg sync.WaitGroup
}

// Read reads decoded body data.
func (b *BodyResult) Read(p []byte) (int, error) {
	return b.pipeReader.Read(p)
}

// Close closes the body reader and releases resources.
// It's safe to call Close multiple times.
func (b *BodyResult) Close() error {
	if b.cancel != nil {
		b.cancel()
	}
	// Close the reader to unblock any pending reads
	err := b.pipeReader.Close()
	// Wait for the background goroutine to finish
	b.wg.Wait()
	return err
}

// Meta returns the yenc metadata.
// It blocks until the yenc headers have been parsed (typically very quick).
// Some fields like ExpectedCRC and ActualCRC are only available after
// reading completes.
func (b *BodyResult) Meta() *YencMeta {
	<-b.metaCh
	return &b.meta
}

// Err returns any error that occurred during the download.
// This is useful to check after reading completes.
func (b *BodyResult) Err() error {
	b.errMu.RLock()
	defer b.errMu.RUnlock()
	return b.err
}

// setError sets the error in a thread-safe manner.
func (b *BodyResult) setError(err error) {
	b.errMu.Lock()
	defer b.errMu.Unlock()
	if b.err == nil {
		b.err = err
	}
}

// setMeta sets the metadata and signals that it's available.
func (b *BodyResult) setMeta(resp *nntpcli.Response) {
	b.metaOnce.Do(func() {
		b.meta = YencMeta{
			FileName:     resp.Meta.FileName,
			FileSize:     resp.Meta.FileSize,
			Part:         resp.Meta.Part,
			Total:        resp.Meta.Total,
			PartBegin:    resp.Meta.PartBegin,
			PartSize:     resp.Meta.PartSize,
			ExpectedCRC:  resp.Meta.ExpectedCRC,
			ActualCRC:    resp.Meta.CRC,
			BytesDecoded: resp.Meta.BytesDecoded,
		}
		close(b.metaCh)
	})
}

// BodyReader retrieves the body of an article and returns a reader for streaming.
// The returned BodyResult provides access to the decoded body data and yenc metadata.
//
// Usage:
//
//	result, err := pool.BodyReader(ctx, "message-id@example.com")
//	if err != nil {
//	    return err
//	}
//	defer result.Close()
//
//	// Read decoded body
//	_, err = io.Copy(outputFile, result)
//
//	// Access metadata (available after headers parsed)
//	meta := result.Meta()
//	fmt.Printf("File: %s, Part: %d/%d\n", meta.FileName, meta.Part, meta.Total)
func (p *Pool) BodyReader(ctx context.Context, messageID string) (*BodyResult, error) {
	p.mu.RLock()
	if p.closed {
		p.mu.RUnlock()
		return nil, ErrPoolClosed
	}
	p.mu.RUnlock()

	// Create cancellable context
	ctx, cancel := context.WithCancel(ctx)

	// Create pipe for streaming
	pipeReader, pipeWriter := io.Pipe()

	result := &BodyResult{
		pipeReader: pipeReader,
		pipeWriter: pipeWriter,
		metaCh:     make(chan struct{}),
		cancel:     cancel,
	}

	// Start background download
	result.wg.Add(1)
	go func() {
		defer result.wg.Done()
		defer pipeWriter.Close()

		payload := []byte(fmt.Sprintf("BODY <%s>\r\n", messageID))
		resp, err := p.executeWithFallbackAndMeta(ctx, payload, pipeWriter)

		if err != nil {
			result.setError(err)
			pipeWriter.CloseWithError(err)
			// Still set meta if we have partial data
			if resp != nil {
				result.setMeta(resp)
			} else {
				// No response at all, close meta channel with empty meta
				result.metaOnce.Do(func() {
					close(result.metaCh)
				})
			}
			return
		}

		// Set final metadata including CRC
		result.setMeta(resp)
	}()

	return result, nil
}

// executeWithFallbackAndMeta is like executeWithFallback but returns the response for metadata.
func (p *Pool) executeWithFallbackAndMeta(ctx context.Context, payload []byte, w io.Writer) (*nntpcli.Response, error) {
	p.mu.RLock()
	if p.closed {
		p.mu.RUnlock()
		return nil, ErrPoolClosed
	}
	p.mu.RUnlock()

	// Track hosts that returned "not found" to skip same-backbone providers
	notFoundHosts := make(map[string]bool)
	triedProviders := make(map[*Provider]bool)

	// PHASE 1: Try primary providers first
	resp, err := p.tryProvidersWithMeta(ctx, p.primaryProviders, payload, w, notFoundHosts, triedProviders)
	if err == nil {
		return resp, nil
	}

	// If we got a non-"not found" error (connection error), continue trying
	if !IsArticleNotFound(err) {
		if len(triedProviders) < len(p.primaryProviders) {
			resp, err = p.tryProvidersWithMeta(ctx, p.primaryProviders, payload, w, notFoundHosts, triedProviders)
			if err == nil {
				return resp, nil
			}
		}
	}

	// PHASE 2: Try all remaining providers (primary + backup) sorted by priority
	allSorted := p.getAllProvidersSorted()
	resp, err = p.tryProvidersWithMeta(ctx, allSorted, payload, w, notFoundHosts, triedProviders)
	if err == nil {
		return resp, nil
	}

	// All providers exhausted
	if len(notFoundHosts) > 0 {
		return resp, ErrArticleNotFound
	}

	return resp, ErrAllProvidersFailed
}

// tryProvidersWithMeta is like tryProviders but returns the response for metadata.
func (p *Pool) tryProvidersWithMeta(
	ctx context.Context,
	providers []*Provider,
	payload []byte,
	w io.Writer,
	notFoundHosts map[string]bool,
	triedProviders map[*Provider]bool,
) (*nntpcli.Response, error) {
	var lastErr error
	var lastResp *nntpcli.Response

	orderedProviders := p.orderProvidersWithRoundRobin(providers, notFoundHosts, triedProviders)

	for _, provider := range orderedProviders {
		triedProviders[provider] = true

		resp, err := provider.Send(ctx, payload, w)
		if err == nil {
			// Flush buffered writers to ensure all data reaches underlying writer
			if f, ok := w.(interface{ Flush() error }); ok {
				_ = f.Flush()
			}
			return &resp, nil
		}

		lastErr = err
		lastResp = &resp

		if IsArticleNotFound(err) {
			notFoundHosts[provider.Host()] = true
			continue
		}

		var pe *ProviderError
		if errors.As(err, &pe) && pe.Temporary {
			provider.MarkUnhealthy(err)
		}
	}

	return lastResp, lastErr
}
