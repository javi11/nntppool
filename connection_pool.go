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

type UsenetConnectionPool interface {
	GetConnection(
		ctx context.Context,
		// The list of provider host to skip
		skipProviders []string,
		// If true, backup providers will be used
		useBackupProviders bool,
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
	skipProviders []string,
	useBackupProviders bool,
) (PooledConnection, error) {
	if atomic.LoadInt32(&p.isShutdown) == 1 {
		return nil, ErrConnectionPoolShutdown
	}

	// Lock access to connPools to prevent TOCTOU race with Quit()
	p.connPoolsMu.RLock()
	if p.connPools == nil {
		p.connPoolsMu.RUnlock()
		return nil, fmt.Errorf("connection pool is not available")
	}

	cp := make([]*providerPool, 0, len(p.connPools))

	for _, provider := range p.connPools {
		if !slices.Contains(skipProviders, provider.provider.ID()) &&
			(!provider.provider.IsBackupProvider || useBackupProviders) &&
			provider.IsAcceptingConnections() {
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
	pooledConn := newPooledConnection(conn, pool.provider, pool.connectionPool, p.log, p.metrics)

	// Register the connection as active for metrics tracking
	if wrapper, ok := conn.(*netconn.NNTPConnWrapper); ok {
		p.metrics.RegisterActiveConnection(pooledConn.connectionID(), wrapper.NNTPConnection())
	}

	return pooledConn, nil
}

func (p *connectionPool) GetProvidersInfo() []ProviderInfo {
	p.connPoolsMu.RLock()
	defer p.connPoolsMu.RUnlock()

	pi := make([]ProviderInfo, 0)

	for _, pool := range p.connPools {
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
		totalBytesFromPreviousAttempts int64 // Cumulative bytes written across failed retry attempts
		conn                           PooledConnection
		finalBytesWritten              int64  // Total bytes written on successful attempt
		providerHost                   string // Provider host for metrics tracking
	)

	skipProviders := make([]string, 0)

	retryErr := retry.Do(func() error {
		// In case of skip provider it means that the article was not found,
		// in such case we want to use backup providers as well
		var useBackupProviders bool
		if len(skipProviders) > 0 {
			useBackupProviders = true
		}

		c, err := p.GetConnection(ctx, skipProviders, useBackupProviders)
		if err != nil {
			if c != nil {
				if closeErr := c.Close(); closeErr != nil {
					p.log.DebugContext(ctx, "Failed to close connection after GetConnection error", "error", closeErr)
				}
			}

			if errors.Is(err, context.Canceled) {
				return err
			}

			return fmt.Errorf("error getting nntp connection: %w", err)
		}

		conn = c
		nntpConn := conn.Connection()

		if len(nntpGroups) > 0 {
			err = joinGroup(nntpConn, nntpGroups)
			if err != nil {
				return fmt.Errorf("error joining group: %w", err)
			}
		}

		n, err := nntpConn.BodyDecoded(msgID, w, totalBytesFromPreviousAttempts)
		if err != nil {
			totalBytesFromPreviousAttempts += n
			return fmt.Errorf("error downloading body: %w", err)
		}

		finalBytesWritten = n

		// Capture provider host before freeing connection
		providerHost = conn.Provider().Host

		if err := conn.Free(); err != nil {
			p.log.DebugContext(ctx, "Failed to free connection after successful body download", "error", err)
		}

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

				if nntpcli.IsArticleNotFoundError(err) {
					skipProviders = append(skipProviders, provider.ID())
					p.log.DebugContext(ctx,
						"Article not found in provider, trying another one...",
						"provider",
						provider.Host,
						"segment_id", msgID,
						"retry", n,
					)

					if freeErr := conn.Free(); freeErr != nil {
						p.log.DebugContext(ctx, "Failed to free connection after article not found", "error", freeErr)
					}
					conn = nil
				} else {
					if closeErr := conn.Close(); closeErr != nil {
						p.log.DebugContext(ctx, "Failed to close connection on retry", "error", closeErr)
					}
					conn = nil
				}
			}
		}),
	)
	if retryErr != nil {
		err := retryErr

		var e retry.Error

		if errors.As(err, &e) {
			wrappedErrors := e.WrappedErrors()
			lastError := wrappedErrors[len(wrappedErrors)-1]

			if conn != nil {
				if errors.Is(lastError, context.Canceled) {
					if freeErr := conn.Free(); freeErr != nil {
						p.log.DebugContext(ctx, "Failed to free connection after context cancellation", "error", freeErr)
					}
				} else {
					if closeErr := conn.Close(); closeErr != nil {
						p.log.DebugContext(ctx, "Failed to close connection after retry exhaustion", "error", closeErr)
					}
				}
			}

			// Check only the last error to determine if article was not found
			if !errors.Is(lastError, context.Canceled) && nntpcli.IsArticleNotFoundError(lastError) {
				p.log.DebugContext(ctx,
					"Segment Not Found in any of the providers",
					"error", lastError,
					"segment_id", msgID,
				)

				// if article not found, we don't want to retry so mark it as corrupted
				return finalBytesWritten, ErrArticleNotFoundInProviders
			}

			return finalBytesWritten, lastError
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

		return finalBytesWritten, err
	}

	conn = nil

	// Record successful download metrics with provider host
	p.metrics.RecordDownload(finalBytesWritten, providerHost)
	p.metrics.RecordArticleDownloaded(providerHost)

	return finalBytesWritten, nil
}

// BodyBatch downloads multiple articles using pipelining if supported by the provider.
// The group parameter specifies the newsgroup to join before downloading.
// Articles are batched according to the provider's PipelineDepth setting.
// Returns results in the same order as requests.
// Failed requests are automatically retried with provider rotation.
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

	// Track which requests still need to be processed
	pendingIndices := make([]int, len(requests))
	for i := range pendingIndices {
		pendingIndices[i] = i
	}

	skipProviders := make([]string, 0)
	maxAttempts := int(p.config.MaxRetries)

	for attempt := 0; attempt < maxAttempts && len(pendingIndices) > 0; attempt++ {
		// Check context cancellation
		if ctx.Err() != nil {
			for _, idx := range pendingIndices {
				results[idx] = BodyBatchResult{
					MessageID: requests[idx].MessageID,
					Error:     ctx.Err(),
				}
			}
			return results
		}

		// Get connection
		useBackupProviders := len(skipProviders) > 0
		conn, err := p.GetConnection(ctx, skipProviders, useBackupProviders)
		if err != nil {
			if conn != nil {
				_ = conn.Close()
			}
			// Mark all pending as failed with this error
			for _, idx := range pendingIndices {
				results[idx] = BodyBatchResult{
					MessageID: requests[idx].MessageID,
					Error:     fmt.Errorf("error getting nntp connection: %w", err),
				}
			}
			return results
		}

		nntpConn := conn.Connection()
		provider := conn.Provider()
		pipelineDepth := provider.PipelineDepth

		// Join the group
		if err := nntpConn.JoinGroup(group); err != nil {
			_ = conn.Close()
			// Mark all pending as failed and try next provider
			for _, idx := range pendingIndices {
				results[idx].Error = fmt.Errorf("error joining group: %w", err)
			}
			continue
		}

		// If pipelining is disabled (depth <= 1), fall back to sequential
		if pipelineDepth <= 1 {
			// Process sequentially
			newPending := make([]int, 0)
			for _, idx := range pendingIndices {
				req := requests[idx]
				n, bodyErr := nntpConn.BodyDecoded(req.MessageID, req.Writer, req.Discard)
				results[idx] = BodyBatchResult{
					MessageID:    req.MessageID,
					BytesWritten: n,
					Error:        bodyErr,
					ProviderHost: provider.Host,
				}

				if bodyErr != nil {
					p.metrics.RecordError(provider.Host)
					if nntpcli.IsArticleNotFoundError(bodyErr) {
						// Article not found - will retry with another provider
						newPending = append(newPending, idx)
					}
				} else {
					p.metrics.RecordDownload(n, provider.Host)
					p.metrics.RecordArticleDownloaded(provider.Host)
				}
			}
			pendingIndices = newPending
			if len(newPending) > 0 {
				skipProviders = append(skipProviders, provider.ID())
			}
			_ = conn.Free()
			continue
		}

		// Process in batches according to pipeline depth
		newPending := make([]int, 0)

		for batchStart := 0; batchStart < len(pendingIndices); batchStart += pipelineDepth {
			batchEnd := batchStart + pipelineDepth
			if batchEnd > len(pendingIndices) {
				batchEnd = len(pendingIndices)
			}
			batchIndices := pendingIndices[batchStart:batchEnd]

			// Build pipeline requests for this batch
			pipelineReqs := make([]nntpcli.PipelineRequest, len(batchIndices))
			for i, idx := range batchIndices {
				pipelineReqs[i] = nntpcli.PipelineRequest{
					MessageID: requests[idx].MessageID,
					Writer:    requests[idx].Writer,
					Discard:   requests[idx].Discard,
				}
			}

			// Execute pipelined batch
			pipelineResults := nntpConn.BodyPipelined(pipelineReqs)

			// Process results
			for i, idx := range batchIndices {
				pr := pipelineResults[i]
				results[idx] = BodyBatchResult{
					MessageID:    pr.MessageID,
					BytesWritten: pr.BytesWritten,
					Error:        pr.Error,
					ProviderHost: provider.Host,
				}

				if pr.Error != nil {
					p.metrics.RecordError(provider.Host)
					if nntpcli.IsArticleNotFoundError(pr.Error) {
						// Article not found - will retry with another provider
						newPending = append(newPending, idx)
					}
				} else {
					p.metrics.RecordDownload(pr.BytesWritten, provider.Host)
					p.metrics.RecordArticleDownloaded(provider.Host)
				}
			}
		}

		pendingIndices = newPending
		if len(newPending) > 0 {
			skipProviders = append(skipProviders, provider.ID())
		}
		_ = conn.Free()
	}

	// Mark any remaining pending requests as failed
	for _, idx := range pendingIndices {
		if results[idx].Error == nil {
			results[idx] = BodyBatchResult{
				MessageID: requests[idx].MessageID,
				Error:     ErrArticleNotFoundInProviders,
			}
		}
	}

	return results
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
	var (
		conn   PooledConnection
		reader nntpcli.ArticleBodyReader
	)

	skipProviders := make([]string, 0)

	retryErr := retry.Do(func() error {
		// In case of skip provider it means that the article was not found,
		// in such case we want to use backup providers as well
		var useBackupProviders bool
		if len(skipProviders) > 0 {
			useBackupProviders = true
		}

		c, err := p.GetConnection(ctx, skipProviders, useBackupProviders)
		if err != nil {
			if c != nil {
				if closeErr := c.Close(); closeErr != nil {
					p.log.DebugContext(ctx, "Failed to close connection after GetConnection error", "error", closeErr)
				}
			}

			if errors.Is(err, context.Canceled) {
				return err
			}

			return fmt.Errorf("error getting nntp connection: %w", err)
		}

		conn = c
		nntpConn := conn.Connection()

		if len(nntpGroups) > 0 {
			err = joinGroup(nntpConn, nntpGroups)
			if err != nil {
				return fmt.Errorf("error joining group: %w", err)
			}
		}

		reader, err = nntpConn.BodyReader(msgID)
		if err != nil {
			return fmt.Errorf("error getting body reader: %w", err)
		}

		// Don't free the connection here since the reader needs it
		// The connection will be managed by a wrapper reader

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

				if nntpcli.IsArticleNotFoundError(err) {
					skipProviders = append(skipProviders, provider.ID())
					p.log.DebugContext(ctx,
						"Segment not found in provider, trying another one...",
						"provider",
						provider.Host,
						"segment_id", msgID,
					)

					if freeErr := conn.Free(); freeErr != nil {
						p.log.DebugContext(ctx, "Failed to free connection after segment not found", "error", freeErr)
					}
					conn = nil
				} else {
					if closeErr := conn.Close(); closeErr != nil {
						p.log.DebugContext(ctx, "Failed to close connection on retry", "error", closeErr)
					}
					conn = nil
				}
			}
		}),
	)

	if retryErr != nil {
		err := retryErr

		var e retry.Error

		if errors.As(err, &e) {
			wrappedErrors := e.WrappedErrors()
			lastError := wrappedErrors[len(wrappedErrors)-1]

			if conn != nil {
				if errors.Is(lastError, context.Canceled) {
					if freeErr := conn.Free(); freeErr != nil {
						p.log.DebugContext(ctx, "Failed to free connection after context cancellation", "error", freeErr)
					}
				} else {
					if closeErr := conn.Close(); closeErr != nil {
						p.log.DebugContext(ctx, "Failed to close connection after retry exhaustion", "error", closeErr)
					}
				}
			}

			// Check only the last error to determine if article was not found
			if !errors.Is(lastError, context.Canceled) && nntpcli.IsArticleNotFoundError(lastError) {
				p.log.DebugContext(ctx,
					"Segment Not Found in any of the providers",
					"error", lastError,
					"segment_id", msgID,
				)

				return nil, ErrArticleNotFoundInProviders
			}

			return nil, lastError
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

		return nil, err
	}

	// Wrap the reader to manage the connection lifecycle
	return &pooledBodyReader{
		reader:  reader,
		conn:    conn,
		metrics: p.metrics,
		closeCh: make(chan struct{}),
	}, nil
}

// Helper that will implement retry mechanism on top of the connection.
// Post a new article
//
// The reader should contain the entire article, headers and body in
// RFC822ish format.
func (p *connectionPool) Post(ctx context.Context, r io.Reader) error {
	var (
		conn          PooledConnection
		skipProviders []string
		bytesPosted   int64
		providerHost  string // Provider host for metrics tracking
	)

	retryErr := retry.Do(func() error {
		c, err := p.GetConnection(ctx, skipProviders, true)
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
					skipProviders = append(skipProviders, provider.ID())
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
	var (
		conn PooledConnection
		res  int
	)

	skipProviders := make([]string, 0)

	retryErr := retry.Do(func() error {
		useBackupProviders := true

		c, err := p.GetConnection(ctx, skipProviders, useBackupProviders)
		if err != nil {
			if c != nil {
				if closeErr := c.Close(); closeErr != nil {
					p.log.DebugContext(ctx, "Failed to close connection after GetConnection error", "error", closeErr)
				}
			}

			if errors.Is(err, context.Canceled) {
				return err
			}

			return fmt.Errorf("error getting nntp connection: %w", err)
		}

		conn = c
		nntpConn := conn.Connection()

		if len(nntpGroups) > 0 {
			err = joinGroup(nntpConn, nntpGroups)
			if err != nil {
				return fmt.Errorf("error joining group: %w", err)
			}
		}

		res, err = nntpConn.Stat(msgID)
		if err != nil {
			return fmt.Errorf("error checking article: %w", err)
		}

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
				if nntpcli.IsArticleNotFoundError(err) {
					p.log.DebugContext(ctx,
						"Segment not found in provider, trying another one...",
						"provider",
						provider.Host,
						"segment_id", msgID,
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
			wrappedErrors := e.WrappedErrors()
			lastError := wrappedErrors[len(wrappedErrors)-1]

			if conn != nil {
				if errors.Is(lastError, context.Canceled) {
					if freeErr := conn.Free(); freeErr != nil {
						p.log.DebugContext(ctx, "Failed to free connection after context cancellation", "error", freeErr)
					}
				} else {
					if closeErr := conn.Close(); closeErr != nil {
						p.log.DebugContext(ctx, "Failed to close connection after retry exhaustion", "error", closeErr)
					}
				}
			}

			// Check only the last error to determine if article was not found
			if !errors.Is(lastError, context.Canceled) && nntpcli.IsArticleNotFoundError(lastError) {
				p.log.DebugContext(ctx,
					"Segment Not Found in any of the providers",
					"error", lastError,
					"segment_id", msgID,
				)

				// if article not found, we don't want to retry so mark it as corrupted
				return res, ErrArticleNotFoundInProviders
			}

			return res, lastError
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

		return res, err
	}

	conn = nil

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

	pool := cPool[poolNumber]

	// Check if pool is at capacity using Stats()
	stats := pool.connectionPool.Stats()
	if stats.InUse >= int(pool.provider.MaxConnections) {
		if poolNumber < len(cPool)-1 {
			return p.getConnection(ctx, cPool, poolNumber+1)
		}
	}

	conn, err := pool.connectionPool.GetWithContext(ctx)
	if err != nil {
		return nil, nil, err
	}

	return conn, pool, nil
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
		conn, err := p.GetConnection(ctx, []string{}, false)
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
	// TotalConns can include ones that are being destroyed but we should have
	// sleep(500ms) around all of the destroys to help prevent that from throwing
	// off this check
	totalConns := 0
	for _, pool := range p.connPools {
		totalConns += int(pool.connectionPool.Stats().Active)
	}

	toCreate := p.config.MinConnections - totalConns
	if toCreate > 0 {
		return p.createIdleResources(ctx, toCreate)
	}

	return nil
}

// Note: isExpired and isExpiredAt were removed as netpool handles TTL via MaxIdleTime configuration.

// isConnectionExpiredError checks if the error is likely due to an expired/stale connection
func isConnectionExpiredError(err error) bool {
	if err == nil {
		return false
	}

	// Check for common expired connection error patterns
	return isRetryableError(err)
}

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
