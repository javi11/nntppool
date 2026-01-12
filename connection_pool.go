//go:generate go tool mockgen -source=./connection_pool.go -destination=./connection_pool_mock.go -package=nntppool UsenetConnectionPool

package nntppool

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/avast/retry-go/v4"
	"github.com/javi11/nntppool/v2/internal/netconn"
	"github.com/javi11/nntppool/v2/pkg/nntpcli"
)

const defaultHealthCheckInterval = 1 * time.Minute

// BodyBatchRequest represents a request in a batch download operation.
type BodyBatchRequest struct {
	// MessageID is the article message ID to retrieve.
	MessageID string
	// Writer is the destination for the decoded body content.
	Writer io.Writer
	// Discard is the number of bytes to discard from the beginning of the body.
	// Use 0 for full download.
	Discard int64
}

// BodyBatchResult represents the result of a batch download request.
type BodyBatchResult struct {
	// MessageID is the article message ID that was requested.
	MessageID string
	// BytesWritten is the number of bytes written to the writer.
	BytesWritten int64
	// Error is any error that occurred during the request.
	// nil indicates success.
	Error error
	// ProviderHost is the host of the provider that served this request.
	ProviderHost string
}

// retryRequest represents a single article retry request for the worker pool.
type retryRequest struct {
	originalIdx int              // Index in original requests slice
	request     BodyBatchRequest // The original request
	skipHosts   []string         // Hosts already tried for this article
	attempt     int              // Current retry attempt number
}

type UsenetConnectionPool interface {
	// GetConnection returns a connection from the pool, skipping providers with hosts in skipHosts.
	// Providers are tried in order: all primary providers first, then backup providers.
	// If a host is in skipHosts, ALL providers with that host are skipped (useful when
	// an article is not found - no point trying other accounts on the same host).
	GetConnection(
		ctx context.Context,
		skipHosts []string,
	) (PooledConnection, error)
	Body(
		ctx context.Context,
		msgID string,
		w io.Writer,
		nntpGroups []string,
	) (int64, error)
	BodyReader(
		ctx context.Context,
		msgID string,
		nntpGroups []string,
	) (nntpcli.ArticleBodyReader, error)
	Post(ctx context.Context, r io.Reader) error
	Stat(ctx context.Context, msgID string, nntpGroups []string) (int, error)
	// BodyBatch downloads multiple articles using pipelining if supported by the provider.
	// The group parameter specifies the newsgroup to join before downloading.
	// Articles are batched according to the provider's PipelineDepth setting.
	// Returns results in the same order as requests.
	// Failed requests are automatically retried with provider rotation.
	BodyBatch(ctx context.Context, group string, requests []BodyBatchRequest) []BodyBatchResult
	// TestProviderPipelineSupport tests if a specific provider supports pipelining.
	// Requires a known valid message ID for testing.
	// Returns true if pipelining is supported, along with a suggested pipeline depth.
	TestProviderPipelineSupport(ctx context.Context, providerHost string, testMsgID string) (supported bool, suggestedDepth int, err error)
	GetProvidersInfo() []ProviderInfo
	GetProviderStatus(providerID string) (*ProviderInfo, bool)
	GetMetrics() *PoolMetrics
	GetMetricsSnapshot() PoolMetricsSnapshot
	Quit()
}

type connectionPool struct {
	closeChan    chan struct{}
	connPoolsMu  sync.RWMutex // Protects connPools access
	connPools    []*providerPool
	log          Logger
	config       Config
	wg           sync.WaitGroup
	shutdownOnce sync.Once    // Ensures single shutdown
	isShutdown   int32        // Atomic flag for shutdown state
	metrics      *PoolMetrics // High-performance metrics collection
	// Note: providerHealthCheckIdx, lastProviderHealthCheck, providerHealthCheckMu were
	// removed as netpool handles connection health automatically via HealthCheck callback
}

func NewConnectionPool(c ...Config) (UsenetConnectionPool, error) {
	config := mergeWithDefault(c...)
	log := config.Logger

	// Create metrics early so we can pass it to pool creation
	metrics := NewPoolMetrics()

	pools, err := getPools(config.Providers, config.NntpCli, config.Logger, metrics)
	if err != nil {
		return nil, err
	}

	log.Info("verifying providers...")

	if err := verifyProviders(pools, log); err != nil {
		log.Error(fmt.Sprintf("failed to verify providers: %v", err))

		return nil, err
	}

	log.Info("providers verified")

	pool := &connectionPool{
		connPools: pools,
		log:       log,
		config:    config,
		closeChan: make(chan struct{}),
		wg:        sync.WaitGroup{},
		metrics:   metrics,
	}

	// Pre-warm pool with MinConnections to reduce latency for first requests
	if config.MinConnections > 0 {
		log.Info(fmt.Sprintf("Pre-warming pool with %d connections", config.MinConnections))

		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		if err := pool.createIdleResources(ctx, config.MinConnections); err != nil {
			log.Warn("Failed to pre-warm all connections, will continue with partial warmup", "error", err)
			// Don't fail pool creation, health checks will create more connections later
		}
	}

	healthCheckInterval := defaultHealthCheckInterval
	if config.HealthCheckInterval > 0 {
		healthCheckInterval = config.HealthCheckInterval
	}

	pool.wg.Add(1)
	go pool.connectionHealCheck(healthCheckInterval)

	// Start provider reconnection system
	pool.wg.Add(1)
	go pool.providerReconnectionSystem()

	return pool, nil
}

func (p *connectionPool) Quit() {
	p.shutdownOnce.Do(func() {
		startTime := time.Now()
		p.log.Info("Starting connection pool shutdown", "phase", "1-signal")

		// ========== PHASE 1: SIGNAL ==========
		// Set shutdown flag to stop accepting new connections
		atomic.StoreInt32(&p.isShutdown, 1)

		// Signal shutdown to background goroutines
		close(p.closeChan)

		p.log.Debug("Shutdown signal sent, rejecting new connections", "phase", "1-signal", "elapsed", time.Since(startTime))

		// ========== PHASE 2: DRAIN ==========
		// Wait for active operations to complete with drain timeout
		// Check every 100ms if there are any active connections
		p.log.Debug("Waiting for active operations to complete", "phase", "2-drain", "timeout", p.config.DrainTimeout)

		drainStart := time.Now()
		drainTicker := time.NewTicker(100 * time.Millisecond)
		defer drainTicker.Stop()

		drainTimeout := time.After(p.config.DrainTimeout)

	drainLoop:
		for {
			select {
			case <-drainTimeout:
				p.log.Debug("Drain timeout reached, proceeding with shutdown", "phase", "2-drain", "timeout", p.config.DrainTimeout)
				break drainLoop
			case <-drainTicker.C:
				// Check if there are any active connections
				p.connPoolsMu.RLock()
				pools := p.connPools
				p.connPoolsMu.RUnlock()

				if pools == nil {
					break drainLoop
				}

				hasActiveConnections := false
				for _, pool := range pools {
					if pool != nil && pool.connectionPool != nil {
						stats := pool.connectionPool.Stats()
						if stats.InUse > 0 {
							hasActiveConnections = true
							break
						}
					}
				}

				if !hasActiveConnections {
					p.log.Debug("No active connections detected, skipping drain period", "phase", "2-drain", "elapsed", time.Since(drainStart))
					break drainLoop
				}
			}
		}

		p.log.Debug("Drain period completed", "phase", "2-drain", "elapsed", time.Since(drainStart))

		// ========== PHASE 3: BACKGROUND CLEANUP ==========
		// Wait for background goroutines to finish
		p.log.Debug("Waiting for background goroutines to finish", "phase", "3-background")

		backgroundStart := time.Now()
		done := make(chan struct{})
		go func() {
			defer close(done)
			p.wg.Wait()
		}()

		select {
		case <-done:
			p.log.Debug("Background goroutines finished", "phase", "3-background", "elapsed", time.Since(backgroundStart))
		case <-time.After(p.config.ForceCloseTimeout):
			p.log.Debug("Background goroutine timeout exceeded, forcing shutdown", "phase", "3-background", "timeout", p.config.ForceCloseTimeout)
		}

		// ========== PHASE 4: CONNECTION POOL CLEANUP ==========
		// Close all provider connection pools with explicit connection destruction
		p.log.Debug("Cleaning up connection pools", "phase", "4-pools")

		poolCleanupStart := time.Now()
		poolCloseDone := make(chan struct{})
		go func() {
			defer close(poolCloseDone)

			// Lock to safely read connPools
			p.connPoolsMu.RLock()
			pools := p.connPools
			p.connPoolsMu.RUnlock()

			if pools == nil {
				return
			}

			for _, cp := range pools {
				if cp == nil || cp.connectionPool == nil {
					continue
				}

				providerStart := time.Now()

				// Close the pool (this will call destructors for remaining connections)
				cp.connectionPool.Close()

				p.log.Debug("Provider pool closed", "provider", cp.provider.Host, "elapsed", time.Since(providerStart))
			}
		}()

		// Wait for pool close operations with force close timeout
		select {
		case <-poolCloseDone:
			p.log.Debug("Connection pools cleaned up successfully", "phase", "4-pools", "elapsed", time.Since(poolCleanupStart))
		case <-time.After(p.config.ForceCloseTimeout):
			p.log.Debug("Pool cleanup timeout exceeded, forcing shutdown", "phase", "4-pools", "timeout", p.config.ForceCloseTimeout)
		}

		// ========== PHASE 5: FINAL CLEANUP ==========
		// Clean up metrics and nil out references
		p.log.Debug("Performing final cleanup", "phase", "5-final")

		finalCleanupStart := time.Now()

		// Clean up metrics to prevent memory leaks
		if p.metrics != nil {
			p.metrics.Cleanup()
		}

		// Lock to safely set connPools to nil
		p.connPoolsMu.Lock()
		p.connPools = nil
		p.connPoolsMu.Unlock()

		totalElapsed := time.Since(startTime)
		p.log.Info("Connection pool shutdown complete", "phase", "5-final", "cleanup_elapsed", time.Since(finalCleanupStart), "total_elapsed", totalElapsed)
	})
}

func (p *connectionPool) GetConnection(
	ctx context.Context,
	skipHosts []string,
) (PooledConnection, error) {
	if atomic.LoadInt32(&p.isShutdown) == 1 {
		return nil, ErrConnectionPoolShutdown
	}

	// Convert skipHosts to map for O(1) lookups
	skipHostsMap := make(map[string]struct{}, len(skipHosts))
	for _, host := range skipHosts {
		skipHostsMap[host] = struct{}{}
	}

	// Lock access to connPools to prevent TOCTOU race with Quit()
	p.connPoolsMu.RLock()
	if p.connPools == nil {
		p.connPoolsMu.RUnlock()
		return nil, fmt.Errorf("connection pool is not available")
	}

	// Build ordered candidate list: primary providers first, then backups
	// Pre-allocate with capacity to avoid reallocations
	cp := make([]*providerPool, 0, len(p.connPools))

	// First pass: add primaries
	for _, provider := range p.connPools {
		if _, skip := skipHostsMap[provider.provider.Host]; skip {
			continue // Skip all providers with this host
		}
		if !provider.IsAcceptingConnections() {
			continue
		}
		if !provider.provider.IsBackupProvider {
			cp = append(cp, provider)
		}
	}

	// Second pass: add backups
	for _, provider := range p.connPools {
		if _, skip := skipHostsMap[provider.provider.Host]; skip {
			continue
		}
		if !provider.IsAcceptingConnections() {
			continue
		}
		if provider.provider.IsBackupProvider {
			cp = append(cp, provider)
		}
	}
	p.connPoolsMu.RUnlock()

	if len(cp) == 0 {
		return nil, ErrArticleNotFoundInProviders
	}

	// Record acquire attempt
	start := time.Now()
	p.metrics.RecordAcquire()

	conn, pool, err := p.getConnection(ctx, cp, 0)
	if err != nil {
		p.metrics.RecordError("")
		return nil, err
	}

	// Record successful acquire timing
	p.metrics.RecordAcquireWaitTime(time.Since(start))

	// Create pooled connection with provider info and pool reference
	// newPooledConnection returns the extracted nntpConn to avoid duplicate type assertions
	pooledConn, nntpConn := newPooledConnection(conn, pool.provider, pool.connectionPool, p.log, p.metrics)

	// Register the connection as active for metrics tracking
	if nntpConn != nil {
		p.metrics.RegisterActiveConnection(pooledConn.connectionID(), nntpConn)
	}

	return pooledConn, nil
}

func (p *connectionPool) GetProvidersInfo() []ProviderInfo {
	// Snapshot pool references under lock, release early to reduce contention
	p.connPoolsMu.RLock()
	pools := make([]*providerPool, len(p.connPools))
	copy(pools, p.connPools)
	p.connPoolsMu.RUnlock()

	// Build results without holding connPoolsMu - Stats() calls happen outside lock
	pi := make([]ProviderInfo, 0, len(pools))

	for _, pool := range pools {
		lastAttempt, lastSuccess, nextRetry, reason, retries := pool.GetConnectionStatus()

		pi = append(pi, ProviderInfo{
			Host:                  pool.provider.Host,
			Username:              pool.provider.Username,
			MaxConnections:        pool.provider.MaxConnections,
			UsedConnections:       pool.connectionPool.Stats().Active,
			State:                 pool.GetState(),
			LastConnectionAttempt: lastAttempt,
			LastSuccessfulConnect: lastSuccess,
			FailureReason:         reason,
			RetryCount:            retries,
			NextRetryAt:           nextRetry,
		})
	}

	return pi
}

// GetProviderStatus returns detailed status for a specific provider
func (p *connectionPool) GetProviderStatus(providerID string) (*ProviderInfo, bool) {
	if atomic.LoadInt32(&p.isShutdown) == 1 {
		return nil, false
	}

	p.connPoolsMu.RLock()
	defer p.connPoolsMu.RUnlock()

	if p.connPools == nil {
		return nil, false
	}

	for _, pool := range p.connPools {
		if pool.provider.ID() == providerID {
			lastAttempt, lastSuccess, nextRetry, reason, retries := pool.GetConnectionStatus()

			providerInfo := &ProviderInfo{
				Host:                  pool.provider.Host,
				Username:              pool.provider.Username,
				MaxConnections:        pool.provider.MaxConnections,
				UsedConnections:       pool.connectionPool.Stats().Active,
				State:                 pool.GetState(),
				LastConnectionAttempt: lastAttempt,
				LastSuccessfulConnect: lastSuccess,
				FailureReason:         reason,
				RetryCount:            retries,
				NextRetryAt:           nextRetry,
			}

			return providerInfo, true
		}
	}

	return nil, false
}

// GetMetrics returns the pool's metrics instance for real-time monitoring
func (p *connectionPool) GetMetrics() *PoolMetrics {
	return p.metrics
}

// GetMetricsSnapshot returns a comprehensive snapshot of pool and connection metrics
func (p *connectionPool) GetMetricsSnapshot() PoolMetricsSnapshot {
	if atomic.LoadInt32(&p.isShutdown) == 1 {
		// Return empty snapshot if pool is shutdown
		return PoolMetricsSnapshot{
			Timestamp: time.Now(),
		}
	}

	p.connPoolsMu.RLock()
	pools := p.connPools
	p.connPoolsMu.RUnlock()

	if pools == nil {
		// Return empty snapshot if pools not available
		return PoolMetricsSnapshot{
			Timestamp: time.Now(),
		}
	}

	return p.metrics.GetSnapshot(pools)
}

// Helper that will implement retry mechanism and provider rotation on top of the connection.
// Body retrieves the body of a message with the given message ID from the NNTP server,
// writes it to the provided io.Writer.
//
// Parameters:
//   - ctx: The context for the operation.
//   - msgID: The message ID of the article to retrieve.
//   - w: The io.Writer to which the message body will be written.
//   - nntpGroups: The list of groups to join before retrieving the article.
//
// Returns:
//   - int64: The number of bytes written to the io.Writer.
//   - error: Any error encountered during the operation.
//
// The function sends the "BODY" command to the NNTP server, starts the response,
// and reads the response code. It uses a decoder to read the message body.
//
// If an error occurs during reading or writing,
// the function ensures that the decoder is fully read to avoid connection issues.
func (p *connectionPool) Body(
	ctx context.Context,
	msgID string,
	w io.Writer,
	nntpGroups []string,
) (int64, error) {
	var (
		bytesWritten int64
		providerHost string
	)

	err := p.retryWithProviderRotation(ctx, func(conn PooledConnection) error {
		nntpConn := conn.Connection()

		if len(nntpGroups) > 0 {
			if err := joinGroup(nntpConn, nntpGroups); err != nil {
				return fmt.Errorf("error joining group: %w", err)
			}
		}

		n, err := nntpConn.BodyDecoded(msgID, w, 0)
		if err != nil {
			return fmt.Errorf("error downloading body: %w", err)
		}

		bytesWritten = n
		providerHost = conn.Provider().Host

		if freeErr := conn.Free(); freeErr != nil {
			p.log.DebugContext(ctx, "Failed to free connection after successful body download", "error", freeErr)
		}

		return nil
	})

	if err != nil {
		if errors.Is(err, ErrArticleNotFoundInProviders) {
			p.log.DebugContext(ctx,
				"Segment Not Found in any of the providers",
				"error", err,
				"segment_id", msgID,
			)
		}
		return bytesWritten, err
	}

	// Record successful download metrics
	p.metrics.RecordDownload(bytesWritten, providerHost)
	p.metrics.RecordArticleDownloaded(providerHost)

	return bytesWritten, nil
}

// BodyBatch downloads multiple articles using pipelining if supported by the provider.
// The group parameter specifies the newsgroup to join before downloading.
// Returns results in the same order as requests.
// Failed requests are automatically retried in parallel with provider rotation -
// each failed article is independently retried on other providers without waiting
// for other articles to complete.
//
// Parameters:
//   - ctx: The context for the operation.
//   - group: The newsgroup to join before downloading (required).
//   - requests: The list of articles to download.
//
// Returns:
//   - []BodyBatchResult: Results in the same order as requests, each containing
//     bytes written, error (if any), and the provider host that served the request.
func (p *connectionPool) BodyBatch(
	ctx context.Context,
	group string,
	requests []BodyBatchRequest,
) []BodyBatchResult {
	results := make([]BodyBatchResult, len(requests))

	// Handle edge cases
	if len(requests) == 0 {
		return results
	}

	// For single request, use regular Body() method
	if len(requests) == 1 {
		n, err := p.Body(ctx, requests[0].MessageID, requests[0].Writer, []string{group})
		results[0] = BodyBatchResult{
			MessageID:    requests[0].MessageID,
			BytesWritten: n,
			Error:        err,
		}
		return results
	}

	// Check context cancellation early
	if ctx.Err() != nil {
		for i := range requests {
			results[i] = BodyBatchResult{
				MessageID: requests[i].MessageID,
				Error:     ctx.Err(),
			}
		}
		return results
	}

	// Setup for parallel retry workers
	var resultsMu sync.Mutex
	var wg sync.WaitGroup

	// Buffered channel for retry requests - size allows all requests to potentially need retry
	retryChan := make(chan retryRequest, len(requests))

	// Determine number of workers based on available providers and requests
	p.connPoolsMu.RLock()
	numProviders := len(p.connPools)
	p.connPoolsMu.RUnlock()

	numWorkers := min(numProviders, len(requests))
	if numWorkers < 1 {
		numWorkers = 1
	}

	// Start retry workers
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go p.bodyBatchRetryWorker(ctx, group, retryChan, results, &resultsMu, &wg)
	}

	// Get initial connection for pipelined batch
	conn, err := p.GetConnection(ctx, nil)
	if err != nil {
		close(retryChan)
		wg.Wait()
		// Mark all as failed
		for i := range requests {
			results[i] = BodyBatchResult{
				MessageID: requests[i].MessageID,
				Error:     fmt.Errorf("error getting nntp connection: %w", err),
			}
		}
		return results
	}

	nntpConn := conn.Connection()
	provider := conn.Provider()
	pipelineDepth := provider.PipelineDepth
	initialHost := provider.Host

	// Join the group
	if err := nntpConn.JoinGroup(group); err != nil {
		_ = conn.Close()
		close(retryChan)
		wg.Wait()
		for i := range requests {
			results[i] = BodyBatchResult{
				MessageID: requests[i].MessageID,
				Error:     fmt.Errorf("error joining group: %w", err),
			}
		}
		return results
	}

	// If pipelining is disabled (depth <= 1), fall back to sequential with parallel retries
	if pipelineDepth <= 1 {
		for idx, req := range requests {
			n, bodyErr := nntpConn.BodyDecoded(req.MessageID, req.Writer, req.Discard)

			if bodyErr != nil && nntpcli.IsArticleNotFoundError(bodyErr) {
				// Article not found - send to retry workers immediately
				p.metrics.RecordError(provider.Host)
				retryChan <- retryRequest{
					originalIdx: idx,
					request:     req,
					skipHosts:   []string{initialHost},
					attempt:     1,
				}
			} else {
				// Success or non-retryable error
				if bodyErr != nil {
					p.metrics.RecordError(provider.Host)
				} else {
					p.metrics.RecordDownload(n, provider.Host)
					p.metrics.RecordArticleDownloaded(provider.Host)
				}
				resultsMu.Lock()
				results[idx] = BodyBatchResult{
					MessageID:    req.MessageID,
					BytesWritten: n,
					Error:        bodyErr,
					ProviderHost: provider.Host,
				}
				resultsMu.Unlock()
			}
		}
		_ = conn.Free()
		close(retryChan)
		wg.Wait()
		return results
	}

	// Build pipeline requests for ALL requests
	pipelineReqs := make([]nntpcli.PipelineRequest, len(requests))
	for i, req := range requests {
		pipelineReqs[i] = nntpcli.PipelineRequest{
			MessageID: req.MessageID,
			Writer:    req.Writer,
			Discard:   req.Discard,
		}
	}

	// Execute pipelined batch
	pipelineResults := nntpConn.BodyPipelined(pipelineReqs)
	_ = conn.Free()

	// Process results - successes go to results, failures go to retry channel
	for i, pr := range pipelineResults {
		if pr.Error != nil && nntpcli.IsArticleNotFoundError(pr.Error) {
			// Article not found - send to retry workers immediately
			p.metrics.RecordError(provider.Host)
			retryChan <- retryRequest{
				originalIdx: i,
				request:     requests[i],
				skipHosts:   []string{initialHost},
				attempt:     1,
			}
		} else {
			// Success or non-retryable error
			if pr.Error != nil {
				p.metrics.RecordError(provider.Host)
			} else {
				p.metrics.RecordDownload(pr.BytesWritten, provider.Host)
				p.metrics.RecordArticleDownloaded(provider.Host)
			}
			resultsMu.Lock()
			results[i] = BodyBatchResult{
				MessageID:    pr.MessageID,
				BytesWritten: pr.BytesWritten,
				Error:        pr.Error,
				ProviderHost: provider.Host,
			}
			resultsMu.Unlock()
		}
	}

	// Close retry channel to signal workers to finish
	close(retryChan)

	// Wait for all retry workers to complete
	wg.Wait()

	return results
}

// bodyBatchRetryWorker processes retry requests for articles not found on initial provider.
// Workers run in parallel, each handling individual article retries independently.
func (p *connectionPool) bodyBatchRetryWorker(
	ctx context.Context,
	group string,
	retryChan chan retryRequest,
	results []BodyBatchResult,
	resultsMu *sync.Mutex,
	wg *sync.WaitGroup,
) {
	defer wg.Done()

	for req := range retryChan {
		// Check context cancellation
		if ctx.Err() != nil {
			resultsMu.Lock()
			results[req.originalIdx] = BodyBatchResult{
				MessageID: req.request.MessageID,
				Error:     ctx.Err(),
			}
			resultsMu.Unlock()
			continue
		}

		// Get connection from a different provider (skip hosts already tried)
		conn, err := p.GetConnection(ctx, req.skipHosts)
		if err != nil {
			// No more providers available
			resultsMu.Lock()
			results[req.originalIdx] = BodyBatchResult{
				MessageID: req.request.MessageID,
				Error:     ErrArticleNotFoundInProviders,
			}
			resultsMu.Unlock()
			continue
		}

		nntpConn := conn.Connection()
		provider := conn.Provider()

		// Join the group
		if err := nntpConn.JoinGroup(group); err != nil {
			_ = conn.Close()
			// Re-enqueue for retry with this host skipped
			newSkipHosts := append(append([]string{}, req.skipHosts...), provider.Host)
			if req.attempt < int(p.config.MaxRetries) {
				select {
				case retryChan <- retryRequest{
					originalIdx: req.originalIdx,
					request:     req.request,
					skipHosts:   newSkipHosts,
					attempt:     req.attempt + 1,
				}:
				default:
					resultsMu.Lock()
					results[req.originalIdx] = BodyBatchResult{
						MessageID: req.request.MessageID,
						Error:     fmt.Errorf("error joining group: %w", err),
					}
					resultsMu.Unlock()
				}
			} else {
				resultsMu.Lock()
				results[req.originalIdx] = BodyBatchResult{
					MessageID: req.request.MessageID,
					Error:     fmt.Errorf("error joining group: %w", err),
				}
				resultsMu.Unlock()
			}
			continue
		}

		// Try to download the article
		n, bodyErr := nntpConn.BodyDecoded(req.request.MessageID, req.request.Writer, req.request.Discard)

		if bodyErr != nil && nntpcli.IsArticleNotFoundError(bodyErr) {
			// Article not found on this provider, try next
			p.metrics.RecordError(provider.Host)
			_ = conn.Free()

			newSkipHosts := append(append([]string{}, req.skipHosts...), provider.Host)
			if req.attempt < int(p.config.MaxRetries) {
				select {
				case retryChan <- retryRequest{
					originalIdx: req.originalIdx,
					request:     req.request,
					skipHosts:   newSkipHosts,
					attempt:     req.attempt + 1,
				}:
				default:
					resultsMu.Lock()
					results[req.originalIdx] = BodyBatchResult{
						MessageID: req.request.MessageID,
						Error:     ErrArticleNotFoundInProviders,
					}
					resultsMu.Unlock()
				}
			} else {
				resultsMu.Lock()
				results[req.originalIdx] = BodyBatchResult{
					MessageID: req.request.MessageID,
					Error:     ErrArticleNotFoundInProviders,
				}
				resultsMu.Unlock()
			}
			continue
		}

		// Success or non-retryable error
		if bodyErr != nil {
			p.metrics.RecordError(provider.Host)
			_ = conn.Close()
		} else {
			p.metrics.RecordDownload(n, provider.Host)
			p.metrics.RecordArticleDownloaded(provider.Host)
			_ = conn.Free()
		}

		resultsMu.Lock()
		results[req.originalIdx] = BodyBatchResult{
			MessageID:    req.request.MessageID,
			BytesWritten: n,
			Error:        bodyErr,
			ProviderHost: provider.Host,
		}
		resultsMu.Unlock()
	}
}

// TestProviderPipelineSupport tests if a specific provider supports pipelining.
// Requires a known valid message ID for testing.
//
// Parameters:
//   - ctx: The context for the operation.
//   - providerHost: The host of the provider to test.
//   - testMsgID: A known valid message ID to use for testing.
//
// Returns:
//   - supported: true if pipelining is working correctly
//   - suggestedDepth: recommended pipeline depth based on latency (0 if not supported)
//   - err: any error that occurred during testing
func (p *connectionPool) TestProviderPipelineSupport(
	ctx context.Context,
	providerHost string,
	testMsgID string,
) (supported bool, suggestedDepth int, err error) {
	// Find the provider
	p.connPoolsMu.RLock()
	var targetPool *providerPool
	for _, pool := range p.connPools {
		if pool.provider.Host == providerHost {
			targetPool = pool
			break
		}
	}
	p.connPoolsMu.RUnlock()

	if targetPool == nil {
		return false, 0, fmt.Errorf("provider %s not found", providerHost)
	}

	// Acquire a connection from this specific provider
	conn, err := targetPool.connectionPool.GetWithContext(ctx)
	if err != nil {
		return false, 0, fmt.Errorf("failed to acquire connection from %s: %w", providerHost, err)
	}
	defer targetPool.connectionPool.Put(conn)

	// Get the NNTP connection from wrapper and test pipelining
	wrapper, ok := conn.(*netconn.NNTPConnWrapper)
	if !ok {
		return false, 0, fmt.Errorf("invalid connection type for provider %s", providerHost)
	}
	return wrapper.NNTPConnection().TestPipelineSupport(testMsgID)
}

// BodyReader retrieves the body reader of a message with the given message ID from the NNTP server.
//
// Parameters:
//   - ctx: The context for the operation.
//   - msgID: The message ID of the article to retrieve.
//   - nntpGroups: The list of groups to join before retrieving the article.
//
// Returns:
//   - io.ReadCloser: A reader for the message body.
//   - error: Any error encountered during the operation.
//
// The function sends the "BODY" command to the NNTP server and returns a reader
// for the response body. The caller is responsible for closing the reader.
func (p *connectionPool) BodyReader(
	ctx context.Context,
	msgID string,
	nntpGroups []string,
) (nntpcli.ArticleBodyReader, error) {
	skipHosts := make([]string, 0)

	for {
		// Check context before attempting
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}

		conn, err := p.GetConnection(ctx, skipHosts)
		if err != nil {
			// No more providers available
			if nntpcli.IsArticleNotFoundError(err) || errors.Is(err, ErrNoProviderAvailable) {
				p.log.DebugContext(ctx,
					"Segment Not Found in any of the providers",
					"segment_id", msgID,
				)
				return nil, ErrArticleNotFoundInProviders
			}
			return nil, err
		}

		nntpConn := conn.Connection()
		host := conn.Provider().Host

		// Join group if needed
		if len(nntpGroups) > 0 {
			if err := joinGroup(nntpConn, nntpGroups); err != nil {
				p.metrics.RecordError(host)
				if closeErr := conn.Close(); closeErr != nil {
					p.log.DebugContext(ctx, "Failed to close connection after join group error", "error", closeErr)
				}
				return nil, fmt.Errorf("error joining group: %w", err)
			}
		}

		// Get the body reader
		reader, err := nntpConn.BodyReader(msgID)
		if err != nil {
			if nntpcli.IsArticleNotFoundError(err) {
				// Article not found - skip this host and try next provider
				skipHosts = append(skipHosts, host)

				p.log.DebugContext(ctx,
					"Segment not found in provider, trying another one...",
					"provider", host,
					"segment_id", msgID,
				)

				p.metrics.RecordError(host)
				p.metrics.RecordRetry()

				// Free connection (it's still healthy, just doesn't have the article)
				if freeErr := conn.Free(); freeErr != nil {
					p.log.DebugContext(ctx, "Failed to free connection after article not found", "error", freeErr)
				}

				continue
			}

			// Other error - close connection and return
			p.metrics.RecordError(host)
			if closeErr := conn.Close(); closeErr != nil {
				p.log.DebugContext(ctx, "Failed to close connection on error", "error", closeErr)
			}

			return nil, fmt.Errorf("error getting body reader: %w", err)
		}

		// Success - wrap the reader to manage the connection lifecycle
		// Don't free the connection here since the reader needs it
		return &pooledBodyReader{
			reader:  reader,
			conn:    conn,
			metrics: p.metrics,
			closeCh: make(chan struct{}),
		}, nil
	}
}

// Helper that will implement retry mechanism on top of the connection.
// Post a new article
//
// The reader should contain the entire article, headers and body in
// RFC822ish format.
func (p *connectionPool) Post(ctx context.Context, r io.Reader) error {
	var (
		conn         PooledConnection
		skipHosts    []string
		bytesPosted  int64
		providerHost string // Provider host for metrics tracking
	)

	retryErr := retry.Do(func() error {
		c, err := p.GetConnection(ctx, skipHosts)
		if err != nil {
			if c != nil {
				_ = c.Close()
			}

			if errors.Is(err, context.Canceled) {
				return err
			}

			return fmt.Errorf("error getting nntp connection: %w", err)
		}

		conn = c
		nntpConn := conn.Connection()

		n, err := nntpConn.Post(r)
		if err != nil {
			return fmt.Errorf("error posting article: %w", err)
		}

		bytesPosted = n

		// Capture provider host before freeing connection
		providerHost = conn.Provider().Host

		_ = conn.Free()

		return nil
	},
		retry.Context(ctx),
		retry.Attempts(p.config.MaxRetries),
		retry.DelayType(p.config.delayTypeFn),
		retry.Delay(p.config.RetryDelay),
		retry.RetryIf(func(err error) bool {
			return isRetryableError(err) || errors.Is(err, ErrNoProviderAvailable)
		}),
		retry.OnRetry(func(n uint, err error) {
			p.metrics.RecordRetry()

			if conn != nil {
				provider := conn.Provider()

				// Record error for this provider
				p.metrics.RecordError(provider.Host)
				if nntpcli.IsSegmentAlreadyExistsError(err) {
					skipHosts = append(skipHosts, provider.Host)
					p.log.DebugContext(ctx,
						"Article posting failed in provider, trying another one...",
						"provider",
						provider.Host,
					)

					_ = conn.Free()
					conn = nil
				} else {
					p.log.DebugContext(ctx,
						"Closing connection",
						"error", err,
						"retry", n,
						"error_connection_host", provider.Host,
					)

					_ = conn.Close()
					conn = nil
				}
			}
		}),
	)

	if retryErr != nil {
		err := retryErr

		var e retry.Error

		if errors.As(err, &e) {
			err = errors.Join(e.WrappedErrors()...)
		}

		// Convert provider rotation error to a more specific error for POST
		if errors.Is(err, ErrArticleNotFoundInProviders) {
			return ErrFailedToPostInAllProviders
		}

		if conn != nil {
			if errors.Is(err, context.Canceled) {
				if freeErr := conn.Free(); freeErr != nil {
					p.log.DebugContext(ctx, "Failed to free connection after context cancellation", "error", freeErr)
				}
			} else {
				if closeErr := conn.Close(); closeErr != nil {
					p.log.DebugContext(ctx, "Failed to close connection after retry exhaustion", "error", closeErr)
				}
			}
		}

		if !errors.Is(err, context.Canceled) {
			p.log.DebugContext(ctx,
				"All post retries exhausted",
				"error", retryErr,
			)
		}

		return err
	}

	conn = nil

	// Record successful post metrics with provider host
	p.metrics.RecordUpload(bytesPosted, providerHost)
	p.metrics.RecordArticlePosted(providerHost)

	return nil
}

// Helper that will implement retry mechanism on top of the connection.
// Stat sends a STAT command to the NNTP server to check the status of a message
// with the given message ID. It returns the message number if the message exists.
//
// Parameters:
//
//	msgID - The message ID to check.
//	nntpGroups - The list of groups to join before checking the message.
//
// Returns:
//
//	int - The message number if the message exists.
//	error - An error if the command fails or the response is invalid.
func (p *connectionPool) Stat(
	ctx context.Context,
	msgID string,
	nntpGroups []string,
) (int, error) {
	var res int

	err := p.retryWithProviderRotation(ctx, func(conn PooledConnection) error {
		nntpConn := conn.Connection()

		// Join group if needed
		if len(nntpGroups) > 0 {
			if err := joinGroup(nntpConn, nntpGroups); err != nil {
				return fmt.Errorf("error joining group: %w", err)
			}
		}

		// Check article status
		r, err := nntpConn.Stat(msgID)
		if err != nil {
			return fmt.Errorf("error checking article: %w", err)
		}

		res = r

		// Free the connection after successful operation
		if freeErr := conn.Free(); freeErr != nil {
			p.log.DebugContext(ctx, "Failed to free connection after successful stat", "error", freeErr)
		}

		return nil
	})

	if err != nil {
		if errors.Is(err, ErrArticleNotFoundInProviders) {
			p.log.DebugContext(ctx,
				"Segment Not Found in any of the providers",
				"segment_id", msgID,
			)
		}
		return res, err
	}

	return res, nil
}

func (p *connectionPool) getConnection(
	ctx context.Context,
	cPool []*providerPool,
	poolNumber int,
) (net.Conn, *providerPool, error) {
	if poolNumber > len(cPool)-1 {
		poolNumber = 0
	}

	// First pass: try pools that are not at capacity
	for i := poolNumber; i < len(cPool); i++ {
		pool := cPool[i]
		stats := pool.connectionPool.Stats()

		// Skip pools at capacity
		if stats.InUse >= int(pool.provider.MaxConnections) {
			continue
		}

		conn, err := pool.connectionPool.GetWithContext(ctx)
		if err != nil {
			continue // Try next pool on error
		}

		return conn, pool, nil
	}

	// Second pass: try any pool (they might have capacity now, or we wait)
	for i := poolNumber; i < len(cPool); i++ {
		conn, err := cPool[i].connectionPool.GetWithContext(ctx)
		if err == nil {
			return conn, cPool[i], nil
		}
	}

	return nil, nil, ErrNoProviderAvailable
}

func (p *connectionPool) connectionHealCheck(healthCheckInterval time.Duration) {
	ticker := time.NewTicker(healthCheckInterval)
	defer ticker.Stop()
	defer p.wg.Done()

	// Create a context that can be cancelled for graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	for {
		select {
		case <-p.closeChan:
			cancel() // Cancel any ongoing operations
			return
		case <-ticker.C:
			// Create a timeout context for this health check cycle
			checkCtx, checkCancel := context.WithTimeout(ctx, healthCheckInterval/2)
			p.checkHealth(checkCtx)
			checkCancel()
		}
	}
}

func (p *connectionPool) checkHealth(ctx context.Context) {
	// Note: Provider and connection health checks are now handled by netpool's
	// built-in HealthCheck callback and MaxIdleTime configuration.
	// This routine now only maintains minimum connections.

	if err := p.checkMinConns(ctx); err != nil {
		p.log.Debug("Failed to maintain minimum connections", "error", err)
	}
}

// Note: checkConnsHealth and checkConnsHealthWithTime were removed as netpool handles
// connection health automatically via HealthCheck callback and MaxIdleTime configuration.

func (p *connectionPool) createIdleResources(ctx context.Context, toCreate int) error {
	for i := 0; i < toCreate; i++ {
		conn, err := p.GetConnection(ctx, []string{})
		if err != nil {
			return err
		}
		// Immediately free the connection back to the pool to keep it idle
		if freeErr := conn.Free(); freeErr != nil {
			p.log.Debug("Failed to free connection during warmup", "error", freeErr)
		}
	}

	return nil
}

func (p *connectionPool) checkMinConns(ctx context.Context) error {
	// Snapshot pool references under lock to avoid race condition
	p.connPoolsMu.RLock()
	pools := p.connPools
	p.connPoolsMu.RUnlock()

	if pools == nil {
		return nil
	}

	// TotalConns can include ones that are being destroyed but we should have
	// sleep(500ms) around all of the destroys to help prevent that from throwing
	// off this check
	totalConns := 0
	for _, pool := range pools {
		totalConns += int(pool.connectionPool.Stats().Active)
	}

	toCreate := p.config.MinConnections - totalConns
	if toCreate > 0 {
		return p.createIdleResources(ctx, toCreate)
	}

	return nil
}

// Note: isExpired, isExpiredAt, and isConnectionExpiredError were removed as netpool handles
// TTL and connection health automatically via MaxIdleTime and HealthCheck configuration.

// providerReconnectionSystem runs indefinitely trying to reconnect failed providers
func (p *connectionPool) providerReconnectionSystem() {
	interval := p.config.ProviderReconnectInterval
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	defer p.wg.Done()

	// Create a context that can be cancelled for graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	for {
		select {
		case <-p.closeChan:
			cancel()
			return
		case <-ticker.C:
			p.attemptProviderReconnections(ctx)
		}
	}
}

// attemptProviderReconnections tries to reconnect offline providers
func (p *connectionPool) attemptProviderReconnections(ctx context.Context) {
	if atomic.LoadInt32(&p.isShutdown) == 1 {
		return
	}

	p.connPoolsMu.RLock()
	pools := p.connPools
	p.connPoolsMu.RUnlock()

	if pools == nil {
		return
	}

	for _, pool := range pools {
		select {
		case <-ctx.Done():
			return
		case <-p.closeChan:
			return
		default:
		}

		state := pool.GetState()

		// Skip providers that are active or authentication failed
		if state == ProviderStateActive || state == ProviderStateAuthenticationFailed {
			continue
		}

		p.log.Debug(fmt.Sprintf("checking provider %s for reconnection", pool.provider.Host), "shouldRetry", pool.ShouldRetryNow(), "canRetry", pool.CanRetry())

		// Check if it's time to retry
		if !pool.ShouldRetryNow() {
			_, _, nextRetry, _, _ := pool.GetConnectionStatus()
			p.log.Debug(fmt.Sprintf("provider %s not ready for retry, next retry scheduled at %s", pool.provider.Host, nextRetry.Format(time.RFC3339)))
			continue
		}

		// Skip if provider can't be retried (authentication failed)
		if !pool.CanRetry() {
			continue
		}

		// Try to reconnect
		p.attemptProviderReconnection(ctx, pool)
	}
}

// attemptProviderReconnection tries to reconnect a single provider
func (p *connectionPool) attemptProviderReconnection(ctx context.Context, pool *providerPool) {
	// Set state to reconnecting
	pool.SetState(ProviderStateReconnecting)

	p.log.Debug(fmt.Sprintf("attempting to reconnect provider %s", pool.provider.Host))

	// Create a timeout context for this reconnection attempt
	reconCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	// Try to acquire a connection to test connectivity
	c, err := pool.connectionPool.GetWithContext(reconCtx)
	if err != nil {
		p.handleReconnectionFailure(pool, err)
		return
	}

	defer pool.connectionPool.Put(c)

	// Extract the NNTP connection from the wrapper
	wrapper, ok := c.(*netconn.NNTPConnWrapper)
	if !ok {
		p.handleReconnectionFailure(pool, fmt.Errorf("invalid connection type for provider %s", pool.provider.Host))
		return
	}
	nntpConn := wrapper.NNTPConnection()

	// Verify capabilities if needed
	if len(pool.provider.VerifyCapabilities) > 0 {
		caps, err := nntpConn.Capabilities()
		if err != nil {
			p.handleReconnectionFailure(pool, err)
			return
		}

		for _, cap := range pool.provider.VerifyCapabilities {
			if !slices.Contains(caps, cap) {
				err := fmt.Errorf("provider %s does not support capability %s", pool.provider.Host, cap)
				p.handleReconnectionFailure(pool, err)
				return
			}
		}
	}

	// Success! Mark as active and record the successful connection
	// We preserve the retry count to show how many attempts were made
	pool.stateMu.Lock()
	pool.lastConnectionAttempt = time.Now()
	pool.lastSuccessfulConnect = time.Now()
	pool.failureReason = ""
	// Don't reset retryCount here - preserve it to show reconnection attempts
	retryCount := pool.retryCount
	pool.stateMu.Unlock()

	pool.SetState(ProviderStateActive)

	p.log.Info(fmt.Sprintf("successfully reconnected to provider %s after %d attempts", pool.provider.Host, retryCount))
}

// handleReconnectionFailure handles failed reconnection attempts
func (p *connectionPool) handleReconnectionFailure(pool *providerPool, err error) {
	// Check if this is an authentication error
	if isAuthenticationError(err) {
		pool.SetState(ProviderStateAuthenticationFailed)
		pool.SetConnectionAttempt(err)
		p.log.Error(fmt.Sprintf("authentication failed for provider %s, stopping reconnection attempts", pool.provider.Host))
		return
	}

	// Mark as offline and record the failure
	pool.SetState(ProviderStateOffline)
	pool.SetConnectionAttempt(err)

	// Drop all connections from the offline provider
	p.dropAllProviderConnections(pool)

	// Calculate exponential backoff for next retry
	_, _, _, _, retryCount := pool.GetConnectionStatus()
	nextDelay := p.calculateBackoffDelay(retryCount)
	pool.SetNextRetryAt(time.Now().Add(nextDelay))

	p.log.Debug(fmt.Sprintf("failed to reconnect to provider %s (attempt %d): %v, next retry in %v",
		pool.provider.Host, retryCount, err, nextDelay))
}

// dropAllProviderConnections logs the drop action for an offline provider
// Note: netpool handles connection cleanup automatically via HealthCheck and MaxIdleTime.
// Active connections will fail health checks and be closed when returned to the pool.
func (p *connectionPool) dropAllProviderConnections(pool *providerPool) {
	if pool == nil || pool.connectionPool == nil {
		return
	}

	// Get current stats
	stats := pool.connectionPool.Stats()
	initialTotal := int(stats.Active)
	initialInUse := int(stats.InUse)
	initialIdle := initialTotal - initialInUse

	// Log the drop action
	// Netpool's HealthCheck will handle closing stale/failed connections
	// when they are returned to the pool or during idle time checks
	p.log.Info(fmt.Sprintf("provider %s marked offline: %d total connections (%d idle, %d in-use) will be recycled via health checks",
		pool.provider.Host, initialTotal, initialIdle, initialInUse))
}

// calculateBackoffDelay calculates exponential backoff delay with a maximum cap
func (p *connectionPool) calculateBackoffDelay(retryCount int) time.Duration {
	// Start with the base interval
	delay := p.config.ProviderReconnectInterval

	// Apply exponential backoff: delay = base * 2^retryCount
	for i := 0; i < retryCount && delay < p.config.ProviderMaxReconnectInterval; i++ {
		delay *= 2
	}

	// Cap at maximum interval
	if delay > p.config.ProviderMaxReconnectInterval {
		delay = p.config.ProviderMaxReconnectInterval
	}

	return delay
}

// Note: Provider health check functions (performLightweightProviderCheck, getProvidersToHealthCheck,
// checkProviderHealth, performProviderHealthCheck, handleProviderHealthCheckFailure,
// handleProviderHealthCheckSuccess) were removed as netpool handles connection health
// automatically via the HealthCheck callback configured in getPools().
// Provider state management is still handled by the reconnection system.
