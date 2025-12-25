//go:generate go tool mockgen -source=./connection_pool.go -destination=./connection_pool_mock.go -package=nntppool UsenetConnectionPool

package nntppool

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/textproto"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/avast/retry-go/v4"
	"github.com/jackc/puddle/v2"
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
	closeChan               chan struct{}
	connPoolsMu             sync.RWMutex // Protects connPools access
	connPools               []*providerPool
	log                     Logger
	config                  Config
	wg                      sync.WaitGroup
	shutdownOnce            sync.Once    // Ensures single shutdown
	isShutdown              int32        // Atomic flag for shutdown state
	metrics                 *PoolMetrics // High-performance metrics collection
	providerHealthCheckIdx  int          // Index for round-robin provider health checks
	lastProviderHealthCheck time.Time    // Last time provider health check was performed
	providerHealthCheckMu   sync.Mutex   // Protects provider health check state
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
						stats := pool.connectionPool.Stat()
						if stats.AcquiredResources() > 0 {
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

	conn, err := p.getConnection(ctx, cp, 0)
	if err != nil {
		p.metrics.RecordError("")
		return nil, err
	}

	// Record successful acquire timing
	p.metrics.RecordAcquireWaitTime(time.Since(start))

	// Create pooled connection with unique ID
	pooledConn := newPooledConnection(conn, p.log, p.metrics)

	// Register the connection as active for metrics tracking
	if conn.Value().nntp != nil {
		p.metrics.RegisterActiveConnection(pooledConn.connectionID(), conn.Value().nntp)
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
			UsedConnections:       int(pool.connectionPool.Stat().TotalResources()),
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
				UsedConnections:       int(pool.connectionPool.Stat().TotalResources()),
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
	resource, err := targetPool.connectionPool.Acquire(ctx)
	if err != nil {
		return false, 0, fmt.Errorf("failed to acquire connection from %s: %w", providerHost, err)
	}
	defer resource.Release()

	// Get the connection and test pipelining
	nntpConn := resource.Value().nntp
	return nntpConn.TestPipelineSupport(testMsgID)
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
						"error_connection_created_at", conn.CreatedAt(),
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
						"error_connection_created_at", conn.CreatedAt(),
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
) (*puddle.Resource[*internalConnection], error) {
	if poolNumber > len(cPool)-1 {
		poolNumber = 0
		pool := cPool[poolNumber]

		conn, err := pool.connectionPool.Acquire(ctx)
		if err != nil {
			return nil, err
		}

		return conn, nil
	}

	pool := cPool[poolNumber]
	if pool.connectionPool.Stat().AcquiredResources() == int32(pool.provider.MaxConnections) {
		return p.getConnection(ctx, cPool, poolNumber+1)
	}

	conn, err := pool.connectionPool.Acquire(ctx)
	if err != nil {
		return nil, err
	}

	return conn, nil
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
	// First, perform proactive provider health checks if enabled
	providersToCheck := p.getProvidersToHealthCheck()
	if len(providersToCheck) > 0 {
		p.checkProviderHealth(ctx, providersToCheck)
	}

	// Then perform the existing connection pool health checks
	for {
		// If checkMinConns failed we don't destroy any connections since we couldn't
		// even get to minConns
		if err := p.checkMinConns(ctx); err != nil {
			p.log.Debug("Failed to maintain minimum connections", "error", err)
			break
		}

		if !p.checkConnsHealth() {
			// Since we didn't destroy any connections we can stop looping
			break
		}

		// Technically Destroy is asynchronous but 500ms should be enough for it to
		// remove it from the underlying pool
		select {
		case <-ctx.Done():
			return
		case <-p.closeChan:
			return
		case <-time.After(500 * time.Millisecond):
		}
	}
}

func (p *connectionPool) checkConnsHealth() bool {
	var destroyed bool

	p.connPoolsMu.RLock()
	pools := p.connPools
	p.connPoolsMu.RUnlock()

	idle := make([]*puddle.Resource[*internalConnection], 0)

	for _, pool := range pools {
		idle = append(idle, pool.connectionPool.AcquireAllIdle()...)
	}

	for _, res := range idle {
		providerTimeout := time.Duration(res.Value().provider.MaxConnectionIdleTimeInSeconds) * time.Second

		if p.isExpired(res) || res.IdleDuration() > providerTimeout {
			res.Destroy()

			destroyed = true
		} else {
			res.ReleaseUnused()
		}
	}

	return destroyed
}

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
		totalConns += int(pool.connectionPool.Stat().TotalResources())
	}

	toCreate := p.config.MinConnections - totalConns
	if toCreate > 0 {
		return p.createIdleResources(ctx, toCreate)
	}

	return nil
}

func (p *connectionPool) isExpired(res *puddle.Resource[*internalConnection]) bool {
	return time.Now().After(res.Value().nntp.MaxAgeTime())
}

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
	c, err := pool.connectionPool.Acquire(reconCtx)
	if err != nil {
		p.handleReconnectionFailure(pool, err)
		return
	}

	defer c.Release()

	// Test the connection by getting capabilities
	conn := c.Value()

	// Verify capabilities if needed
	if len(pool.provider.VerifyCapabilities) > 0 {
		caps, err := conn.nntp.Capabilities()
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

// dropAllProviderConnections drops all connections (both idle and active) from a provider pool
func (p *connectionPool) dropAllProviderConnections(pool *providerPool) {
	if pool == nil || pool.connectionPool == nil {
		return
	}

	// Log the drop action
	p.log.Info(fmt.Sprintf("dropping all connections for offline provider %s", pool.provider.Host))

	// Get current stats before dropping connections
	initialTotal := int(pool.connectionPool.Stat().TotalResources())
	initialAcquired := int(pool.connectionPool.Stat().AcquiredResources())

	// Acquire all idle connections and destroy them safely
	idle := pool.connectionPool.AcquireAllIdle()
	destroyedIdle := 0
	for _, res := range idle {
		// Only destroy if the resource is still valid
		if res != nil && res.Value() != nil {
			res.Destroy()
			destroyedIdle++
		}
	}

	// For active connections, we can't forcibly destroy them as they're in use,
	// but they will be closed when returned to the pool since provider is offline
	activeConnections := initialAcquired

	p.log.Info(fmt.Sprintf("dropped connections for offline provider %s: %d idle destroyed, %d active (total was %d)",
		pool.provider.Host, destroyedIdle, activeConnections, initialTotal))
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

// performLightweightProviderCheck performs a connectivity test to a provider using the connection pool
// to respect connection limits and prevent exceeding provider quotas
func (p *connectionPool) performLightweightProviderCheck(ctx context.Context, pool *providerPool) error {
	const maxRealErrors = 2 // Allow 3 total real error attempts (0, 1, 2)

	safetyLimit := pool.provider.MaxConnections * 2

	realErrorAttempts := 0
	expiredConnectionsDestroyed := 0

	// Loop until we succeed, hit real error limit, or exceed safety limit for expired connections
	for expiredConnectionsDestroyed < safetyLimit {
		// Create a timeout context for this specific check
		checkCtx, cancel := context.WithTimeout(ctx, p.config.ProviderHealthCheckTimeout)

		// Try to acquire a connection from the pool to respect connection limits
		c, err := pool.connectionPool.Acquire(checkCtx)
		if err != nil {
			cancel()
			// If we can't acquire a connection, it could mean:
			// 1. Provider is offline/unreachable
			// 2. Provider is at max connections (busy but healthy)
			// 3. Pool creation failed

			// Check if this is a timeout/context error (likely busy)
			if checkCtx.Err() == context.DeadlineExceeded {
				// Provider is likely busy but healthy - treat as success
				p.log.Debug(fmt.Sprintf("provider %s health check timeout - treating as busy but healthy", pool.provider.Host))
				return nil
			}

			// This is a real acquisition error
			realErrorAttempts++
			if realErrorAttempts > maxRealErrors {
				return fmt.Errorf("failed to acquire connection for provider %s after %d attempts: %w", pool.provider.Host, realErrorAttempts, err)
			}
			// Try again - don't increment expired counter, this is a real error
			continue
		}

		// Get the underlying connection
		conn := c.Value()
		if conn == nil || conn.nntp == nil {
			c.ReleaseUnused()
			cancel()

			// This is a real error - invalid connection
			realErrorAttempts++
			if realErrorAttempts > maxRealErrors {
				return fmt.Errorf("provider %s has invalid connection after %d attempts", pool.provider.Host, realErrorAttempts)
			}
			continue
		}

		// Check if connection is expired before using it
		// Expired connections are normal lifecycle events, not health failures
		if p.isExpired(c) {
			expiredConnectionsDestroyed++
			c.Destroy() // Destroy expired connection instead of releasing
			cancel()
			// Don't count expired connections as failures - they're normal lifecycle
			// Continue immediately to try again without incrementing realErrorAttempts
			continue
		}

		// Test basic NNTP command to ensure the server is responsive
		err = conn.nntp.Ping()
		if err != nil {
			// If is not a textproto.Error, it could be a connection issue
			var nntpErr *textproto.Error
			if ok := errors.As(err, &nntpErr); !ok {
				// Check if this error is likely due to connection expiry/staleness
				if isConnectionExpiredError(err) {
					expiredConnectionsDestroyed++
					p.log.Debug(fmt.Sprintf("provider %s health check failed with retryable error (destroyed %d expired): %v, destroying connection and retrying",
						pool.provider.Host, expiredConnectionsDestroyed, err))
					c.Destroy() // Destroy the bad connection
					cancel()
					// Don't count expired connection errors as failures
					// Continue immediately to try again without incrementing realErrorAttempts
					continue
				}

				// This is a real connectivity error
				c.ReleaseUnused()
				cancel()

				realErrorAttempts++
				if realErrorAttempts > maxRealErrors {
					return fmt.Errorf("ping failed for provider %s after %d attempts: %w", pool.provider.Host, realErrorAttempts, err)
				}
				continue
			}
		}

		// Success! Clean up and return
		c.ReleaseUnused()
		cancel()

		return nil
	}

	// If we get here, we exceeded the safety limit for expired connections
	// This is a pathological case that shouldn't happen in normal operation
	return fmt.Errorf("provider %s health check exceeded safety limit after destroying %d expired connections (real errors: %d)",
		pool.provider.Host, expiredConnectionsDestroyed, realErrorAttempts)
}

// getProvidersToHealthCheck returns a list of providers that should be checked in this cycle
// using round-robin staggered checking to distribute load
func (p *connectionPool) getProvidersToHealthCheck() []*providerPool {
	p.providerHealthCheckMu.Lock()
	defer p.providerHealthCheckMu.Unlock()

	p.connPoolsMu.RLock()
	pools := p.connPools
	p.connPoolsMu.RUnlock()

	if len(pools) == 0 {
		return nil
	}

	now := time.Now()

	// Check if enough time has passed since last provider health check
	if now.Sub(p.lastProviderHealthCheck) < p.config.ProviderHealthCheckStagger {
		return nil
	}

	p.lastProviderHealthCheck = now

	// Calculate how many providers to check per cycle to stagger over the health check interval
	totalProviders := len(pools)
	if totalProviders == 0 {
		return nil
	}

	// Determine how many providers to check this cycle
	// We want to check all providers over multiple cycles within the health check interval
	checkInterval := p.config.HealthCheckInterval
	staggerInterval := p.config.ProviderHealthCheckStagger
	cyclesPerHealthCheck := checkInterval / staggerInterval

	providersPerCycle := 1
	if cyclesPerHealthCheck > 0 {
		providersPerCycle = int(cyclesPerHealthCheck)
		if totalProviders < providersPerCycle {
			providersPerCycle = totalProviders
		}
	}

	// Select providers using round-robin
	var providersToCheck []*providerPool
	for i := 0; i < providersPerCycle && len(providersToCheck) < totalProviders; i++ {
		idx := (p.providerHealthCheckIdx + i) % totalProviders
		pool := pools[idx]

		// Only check active providers (offline/reconnecting providers are handled by reconnection worker)
		state := pool.GetState()
		if state == ProviderStateActive {
			providersToCheck = append(providersToCheck, pool)
		}
	}

	// Advance the index for next cycle
	p.providerHealthCheckIdx = (p.providerHealthCheckIdx + providersPerCycle) % totalProviders

	return providersToCheck
}

// checkProviderHealth performs proactive health checking on selected providers
func (p *connectionPool) checkProviderHealth(ctx context.Context, providersToCheck []*providerPool) {
	if len(providersToCheck) == 0 {
		return
	}

	for _, pool := range providersToCheck {
		select {
		case <-ctx.Done():
			return
		case <-p.closeChan:
			return
		default:
		}

		// Skip authentication failed providers
		state := pool.GetState()
		if state == ProviderStateAuthenticationFailed {
			continue
		}

		p.performProviderHealthCheck(ctx, pool)
	}
}

// performProviderHealthCheck checks a single provider's health and updates its state
func (p *connectionPool) performProviderHealthCheck(ctx context.Context, pool *providerPool) {
	currentState := pool.GetState()

	err := p.performLightweightProviderCheck(ctx, pool)
	if err != nil {
		// Health check failed
		p.handleProviderHealthCheckFailure(pool, err, currentState)
	} else {
		// Health check succeeded
		p.handleProviderHealthCheckSuccess(pool, currentState)
	}
}

// handleProviderHealthCheckFailure handles a failed provider health check
func (p *connectionPool) handleProviderHealthCheckFailure(pool *providerPool, err error, currentState ProviderState) {
	// Check if this is an authentication error
	if isAuthenticationError(err) {
		if currentState != ProviderStateAuthenticationFailed {
			pool.SetState(ProviderStateAuthenticationFailed)
			pool.SetConnectionAttempt(err)
			p.log.Error(fmt.Sprintf("provider %s authentication failed during health check", pool.provider.Host))
		}
		return
	}

	// Mark as offline if it's currently active
	if currentState == ProviderStateActive {
		pool.SetState(ProviderStateOffline)
		pool.SetConnectionAttempt(err)

		// Drop all connections from the offline provider
		p.dropAllProviderConnections(pool)

		// Calculate backoff for reconnection system
		_, _, _, _, retryCount := pool.GetConnectionStatus()
		nextDelay := p.calculateBackoffDelay(retryCount)
		pool.SetNextRetryAt(time.Now().Add(nextDelay))

		p.log.Warn(fmt.Sprintf("provider %s marked as offline due to health check failure: %v", pool.provider.Host, err))
	} else {
		// Update failure tracking for offline providers
		pool.SetConnectionAttempt(err)
	}
}

// handleProviderHealthCheckSuccess handles a successful provider health check
func (p *connectionPool) handleProviderHealthCheckSuccess(pool *providerPool, currentState ProviderState) {
	// If provider was offline, mark it as active
	switch currentState {
	case ProviderStateOffline, ProviderStateReconnecting:
		pool.SetState(ProviderStateActive)
		pool.SetConnectionAttempt(nil) // Clear failure reason

		p.log.Info(fmt.Sprintf("provider %s marked as active after successful health check", pool.provider.Host))
	case ProviderStateActive:
		// Update successful connection time for active providers
		pool.SetConnectionAttempt(nil)
	}
}
