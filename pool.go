package nntppool

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/javi11/nntppool/v3/pkg/nntpcli"
)

// Pool manages multiple NNTP providers with automatic failover and load balancing.
type Pool struct {
	config PoolConfig

	primaryProviders []*Provider // Sorted by priority
	backupProviders  []*Provider // Sorted by priority
	allProviders     []*Provider // All providers for health checks

	mu     sync.RWMutex
	closed bool

	// Round-robin state per priority level
	rrIndex map[int]*atomic.Uint64

	// Health check management
	healthTicker *time.Ticker
	done         chan struct{}
	wg           sync.WaitGroup
}

// NewPool creates a new connection pool with the given configuration.
func NewPool(ctx context.Context, cfg PoolConfig) (*Pool, error) {
	cfg = cfg.WithDefaults()

	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	pool := &Pool{
		config:           cfg,
		primaryProviders: make([]*Provider, 0),
		backupProviders:  make([]*Provider, 0),
		allProviders:     make([]*Provider, 0, len(cfg.Providers)),
		rrIndex:          make(map[int]*atomic.Uint64),
		done:             make(chan struct{}),
	}

	// Create providers
	for _, provCfg := range cfg.Providers {
		provider, err := NewProvider(ctx, provCfg)
		if err != nil {
			// Clean up already created providers
			for _, p := range pool.allProviders {
				_ = p.Close()
			}
			return nil, err
		}

		pool.allProviders = append(pool.allProviders, provider)

		if provCfg.IsBackup {
			pool.backupProviders = append(pool.backupProviders, provider)
		} else {
			pool.primaryProviders = append(pool.primaryProviders, provider)
		}

		// Initialize round-robin counter for this priority
		if _, exists := pool.rrIndex[provCfg.Priority]; !exists {
			pool.rrIndex[provCfg.Priority] = &atomic.Uint64{}
		}
	}

	// Sort providers by priority
	sort.Slice(pool.primaryProviders, func(i, j int) bool {
		return pool.primaryProviders[i].Priority() < pool.primaryProviders[j].Priority()
	})
	sort.Slice(pool.backupProviders, func(i, j int) bool {
		return pool.backupProviders[i].Priority() < pool.backupProviders[j].Priority()
	})

	// Start health check loop if enabled
	if cfg.HealthCheckInterval > 0 {
		pool.healthTicker = time.NewTicker(cfg.HealthCheckInterval)
		pool.wg.Add(1)
		go pool.runHealthChecks()
	}

	return pool, nil
}

// Body retrieves the body of an article by message ID.
// It writes the decoded body to the provided writer.
func (p *Pool) Body(ctx context.Context, messageID string, w io.Writer) (int64, error) {
	payload := []byte(fmt.Sprintf("BODY <%s>\r\n", messageID))
	return p.executeWithFallback(ctx, payload, w)
}

// Article retrieves a complete article by message ID.
func (p *Pool) Article(ctx context.Context, messageID string, w io.Writer) (int64, error) {
	payload := []byte(fmt.Sprintf("ARTICLE <%s>\r\n", messageID))
	return p.executeWithFallback(ctx, payload, w)
}

// Head retrieves the headers of an article by message ID.
func (p *Pool) Head(ctx context.Context, messageID string) ([]string, error) {
	payload := []byte(fmt.Sprintf("HEAD <%s>\r\n", messageID))

	var buf bytes.Buffer
	_, err := p.executeWithFallback(ctx, payload, &buf)
	if err != nil {
		return nil, err
	}

	// Parse headers from buffer
	// The response is already parsed in NNTPResponse.Lines
	return nil, fmt.Errorf("not implemented")
}

// executeWithFallback executes a request with automatic failover across providers.
func (p *Pool) executeWithFallback(ctx context.Context, payload []byte, w io.Writer) (int64, error) {
	p.mu.RLock()
	if p.closed {
		p.mu.RUnlock()
		return 0, ErrPoolClosed
	}
	p.mu.RUnlock()

	// Track hosts that returned "not found" to skip same-backbone providers
	notFoundHosts := make(map[string]bool)
	triedProviders := make(map[*Provider]bool)

	// PHASE 1: Try primary providers first
	resp, bytesWritten, err := p.tryProviders(ctx, p.primaryProviders, payload, w, notFoundHosts, triedProviders)
	if err == nil {
		return bytesWritten, nil
	}

	// If we got a non-"not found" error (connection error), it might be retryable
	// but we'll continue to try other providers
	if !IsArticleNotFound(err) {
		// Check if we have more primaries to try
		if len(triedProviders) < len(p.primaryProviders) {
			// Continue trying primaries
			resp, bytesWritten, err = p.tryProviders(ctx, p.primaryProviders, payload, w, notFoundHosts, triedProviders)
			if err == nil {
				return bytesWritten, nil
			}
		}
	}

	// PHASE 2: Try all remaining providers (primary + backup) sorted by priority
	// Merge and sort all providers by priority
	allSorted := p.getAllProvidersSorted()
	resp, bytesWritten, err = p.tryProviders(ctx, allSorted, payload, w, notFoundHosts, triedProviders)
	if err == nil {
		return bytesWritten, nil
	}

	// All providers exhausted
	if len(notFoundHosts) > 0 {
		return 0, ErrArticleNotFound
	}

	_ = resp // suppress unused warning
	return 0, ErrAllProvidersFailed
}

// tryProviders attempts to execute the request on the given providers.
// It uses round-robin selection within the same priority level.
func (p *Pool) tryProviders(
	ctx context.Context,
	providers []*Provider,
	payload []byte,
	w io.Writer,
	notFoundHosts map[string]bool,
	triedProviders map[*Provider]bool,
) (nntpcli.Response, int64, error) {
	var lastErr error
	var lastResp nntpcli.Response

	// Get providers ordered by priority with round-robin within same priority
	orderedProviders := p.orderProvidersWithRoundRobin(providers, notFoundHosts, triedProviders)

	for _, provider := range orderedProviders {
		// Mark as tried
		triedProviders[provider] = true

		// Create a buffer to capture the response if we need to retry
		var buf *bytes.Buffer
		var writeTarget io.Writer = w
		if w != nil {
			buf = &bytes.Buffer{}
			writeTarget = buf
		}

		resp, err := provider.Send(ctx, payload, writeTarget)
		if err == nil {
			// Success! Write buffered data to original writer if applicable
			if buf != nil && w != nil {
				written, writeErr := io.Copy(w, buf)
				if writeErr != nil {
					return resp, 0, writeErr
				}
				return resp, written, nil
			}
			return resp, int64(resp.Meta.BytesDecoded), nil
		}

		lastErr = err
		lastResp = resp

		// Check if it's "article not found"
		if IsArticleNotFound(err) {
			notFoundHosts[provider.Host()] = true
			continue // Try next provider
		}

		// Connection or other error - mark unhealthy and try next
		var pe *ProviderError
		if errors.As(err, &pe) && pe.Temporary {
			provider.MarkUnhealthy(err)
		}
	}

	return lastResp, 0, lastErr
}

// getAllProvidersSorted returns all providers sorted by priority.
func (p *Pool) getAllProvidersSorted() []*Provider {
	all := make([]*Provider, 0, len(p.allProviders))
	all = append(all, p.allProviders...)

	sort.Slice(all, func(i, j int) bool {
		// Primary providers come before backup at same priority
		if all[i].Priority() == all[j].Priority() {
			if all[i].IsBackup() != all[j].IsBackup() {
				return !all[i].IsBackup()
			}
		}
		return all[i].Priority() < all[j].Priority()
	})

	return all
}

// orderProvidersWithRoundRobin returns providers ordered by priority,
// with round-robin rotation within each priority group.
// It filters out already-tried providers, unhealthy providers, and
// providers with hosts that returned "not found".
func (p *Pool) orderProvidersWithRoundRobin(
	providers []*Provider,
	notFoundHosts map[string]bool,
	triedProviders map[*Provider]bool,
) []*Provider {
	if len(providers) == 0 {
		return nil
	}

	// Group eligible providers by priority
	byPriority := make(map[int][]*Provider)
	for _, prov := range providers {
		// Skip if already tried
		if triedProviders[prov] {
			continue
		}
		// Skip if same host returned "not found"
		if notFoundHosts[prov.Host()] {
			continue
		}
		// Skip unhealthy providers
		if !prov.IsHealthy() {
			continue
		}
		byPriority[prov.Priority()] = append(byPriority[prov.Priority()], prov)
	}

	if len(byPriority) == 0 {
		return nil
	}

	// Get sorted priority levels
	priorities := make([]int, 0, len(byPriority))
	for pri := range byPriority {
		priorities = append(priorities, pri)
	}
	sort.Ints(priorities)

	// Build result with round-robin ordering within each priority group
	result := make([]*Provider, 0, len(providers))
	for _, pri := range priorities {
		group := byPriority[pri]
		if len(group) == 0 {
			continue
		}

		// Get round-robin starting index for this priority
		counter, exists := p.rrIndex[pri]
		if !exists {
			counter = &atomic.Uint64{}
			p.rrIndex[pri] = counter
		}
		startIdx := int(counter.Add(1)-1) % len(group)

		// Add providers starting from round-robin index
		for i := 0; i < len(group); i++ {
			idx := (startIdx + i) % len(group)
			result = append(result, group[idx])
		}
	}

	return result
}

// runHealthChecks periodically checks the health of all providers.
func (p *Pool) runHealthChecks() {
	defer p.wg.Done()

	for {
		select {
		case <-p.done:
			return
		case <-p.healthTicker.C:
			p.checkAllProviders()
		}
	}
}

// checkAllProviders performs health checks on all providers.
func (p *Pool) checkAllProviders() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	var wg sync.WaitGroup
	for _, provider := range p.allProviders {
		wg.Add(1)
		go func(prov *Provider) {
			defer wg.Done()
			_ = prov.HealthCheck(ctx)
		}(provider)
	}
	wg.Wait()
}

// Close closes the pool and all its providers.
func (p *Pool) Close() error {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil
	}
	p.closed = true
	p.mu.Unlock()

	// Stop health checks
	close(p.done)
	if p.healthTicker != nil {
		p.healthTicker.Stop()
	}
	p.wg.Wait()

	// Close all providers
	var firstErr error
	for _, provider := range p.allProviders {
		if err := provider.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}

	return firstErr
}

// Stats returns statistics for all providers.
func (p *Pool) Stats() []ProviderStats {
	p.mu.RLock()
	defer p.mu.RUnlock()

	stats := make([]ProviderStats, len(p.allProviders))
	for i, provider := range p.allProviders {
		stats[i] = provider.Stats()
	}
	return stats
}

// HealthyProviderCount returns the number of healthy providers.
func (p *Pool) HealthyProviderCount() int {
	p.mu.RLock()
	defer p.mu.RUnlock()

	count := 0
	for _, provider := range p.allProviders {
		if provider.IsHealthy() {
			count++
		}
	}
	return count
}
