//go:generate go tool mockgen -source=./connection_pool.go -destination=./connection_pool_mock.go -package=nntppool UsenetConnectionPool

package nntppool

import (
	"context"
	"errors"
	"fmt"
	"io"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/avast/retry-go/v4"
	"github.com/jackc/puddle/v2"
	"github.com/javi11/nntpcli"

	"github.com/javi11/nntppool/internal/budget"
	"github.com/javi11/nntppool/internal/config"
	"github.com/javi11/nntppool/internal/migration"
)

const defaultHealthCheckInterval = 1 * time.Minute

// ReconfigurationStatus tracks the progress of a configuration reconfiguration
type ReconfigurationStatus struct {
	ID          string                    `json:"id"`
	StartTime   time.Time                 `json:"start_time"`
	Status      string                    `json:"status"` // "running", "completed", "failed", "rolled_back"
	Changes     []ProviderChange          `json:"changes"`
	Progress    map[string]ProviderStatus `json:"progress"` // providerID -> status
	Error       string                    `json:"error,omitempty"`
	CompletedAt *time.Time                `json:"completed_at,omitempty"`
}

// ProviderStatus tracks the migration status of a single provider
type ProviderStatus struct {
	ProviderID     string     `json:"provider_id"`
	Status         string     `json:"status"` // "pending", "migrating", "completed", "failed"
	ConnectionsOld int        `json:"connections_old"`
	ConnectionsNew int        `json:"connections_new"`
	StartTime      time.Time  `json:"start_time"`
	CompletedAt    *time.Time `json:"completed_at,omitempty"`
	Error          string     `json:"error,omitempty"`
}

// ProviderChange represents a change to be made to a provider during migration
type ProviderChange struct {
	ID         string                `json:"id"`
	ChangeType ProviderChangeType    `json:"change_type"`
	OldConfig  *UsenetProviderConfig `json:"old_config,omitempty"`
	NewConfig  *UsenetProviderConfig `json:"new_config,omitempty"`
	Priority   int                   `json:"priority"` // Migration priority (0 = highest)
}

// ProviderChangeType represents the type of change for a provider
type ProviderChangeType int

const (
	ProviderChangeKeep   ProviderChangeType = iota // No change needed
	ProviderChangeUpdate                           // Settings changed, migrate connections
	ProviderChangeAdd                              // New provider, add gradually
	ProviderChangeRemove                           // Remove provider, drain connections
)

func (pct ProviderChangeType) String() string {
	switch pct {
	case ProviderChangeKeep:
		return "keep"
	case ProviderChangeUpdate:
		return "update"
	case ProviderChangeAdd:
		return "add"
	case ProviderChangeRemove:
		return "remove"
	default:
		return "unknown"
	}
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
	GetProvidersInfo() []ProviderInfo
	Reconfigure(...Config) error
	GetReconfigurationStatus(migrationID string) (*ReconfigurationStatus, bool)
	GetActiveReconfigurations() map[string]*ReconfigurationStatus
	Quit()
}

type connectionPool struct {
	closeChan        chan struct{}
	connPools        []*providerPool
	log              Logger
	config           Config
	wg               sync.WaitGroup
	shutdownMu       sync.RWMutex       // Protects shutdown operations
	isShutdown       int32              // Atomic flag for shutdown state
	shutdownOnce     sync.Once          // Ensures single shutdown
	connectionBudget *budget.Budget     // Tracks connection limits per provider
	migrationManager *migration.Manager // Handles incremental migrations
}

func NewConnectionPool(c ...Config) (UsenetConnectionPool, error) {
	config := mergeWithDefault(c...)
	log := config.Logger

	pools, err := getPools(config.Providers, config.NntpCli, config.Logger)
	if err != nil {
		return nil, err
	}

	if !config.SkipProvidersVerificationOnCreation {
		log.Info("verifying providers...")

		if err := verifyProviders(pools, log); err != nil {
			log.Error(fmt.Sprintf("failed to verify providers: %v", err))

			return nil, err
		}

		log.Info("providers verified")
	}

	// Initialize connection budget
	budgetManager := budget.New()
	for _, provider := range config.Providers {
		budgetManager.SetProviderLimit(provider.ID(), provider.MaxConnections)
	}

	pool := &connectionPool{
		connPools:        pools,
		log:              log,
		config:           config,
		closeChan:        make(chan struct{}, 1),
		wg:               sync.WaitGroup{},
		connectionBudget: budgetManager,
	}

	// Initialize migration manager with adapter
	adapter := newMigrationAdapter(pool)
	pool.migrationManager = migration.New(adapter)

	healthCheckInterval := defaultHealthCheckInterval
	if config.HealthCheckInterval > 0 {
		healthCheckInterval = config.HealthCheckInterval
	}

	pool.wg.Add(1)
	go pool.connectionHealCheck(healthCheckInterval)

	return pool, nil
}

func (p *connectionPool) Quit() {
	p.shutdownOnce.Do(func() {
		atomic.StoreInt32(&p.isShutdown, 1)

		p.shutdownMu.Lock()
		defer p.shutdownMu.Unlock()

		// Signal shutdown to health checker
		select {
		case p.closeChan <- struct{}{}:
		default:
			// Channel might be full, that's ok
		}
		close(p.closeChan)

		// Wait for health checker to finish with timeout
		done := make(chan struct{})
		go func() {
			defer close(done)
			p.wg.Wait()
		}()

		select {
		case <-done:
			// Normal shutdown
		case <-time.After(p.config.ShutdownTimeout):
			p.log.Warn("Shutdown timeout exceeded, forcing close")
		}

		// Shutdown migration manager first to cancel any ongoing migrations
		if p.migrationManager != nil {
			p.migrationManager.Shutdown()
		}

		// Close all connection pools
		for _, cp := range p.connPools {
			cp.connectionPool.Close()
		}

		p.connPools = nil
	})
}

func (p *connectionPool) GetConnection(
	ctx context.Context,
	skipProviders []string,
	useBackupProviders bool,
) (PooledConnection, error) {
	if atomic.LoadInt32(&p.isShutdown) == 1 {
		return nil, fmt.Errorf("connection pool is shutdown")
	}

	// Add read lock to prevent shutdown during connection acquisition
	p.shutdownMu.RLock()
	defer p.shutdownMu.RUnlock()

	// Double-check after acquiring lock
	if atomic.LoadInt32(&p.isShutdown) == 1 {
		return nil, fmt.Errorf("connection pool is shutdown")
	}

	if p.connPools == nil {
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

	if len(cp) == 0 {
		return nil, ErrArticleNotFoundInProviders
	}

	conn, err := p.getConnection(ctx, cp, 0)
	if err != nil {
		return nil, err
	}

	pooledConn := pooledConnection{
		resource: conn,
		log:      p.log,
	}

	// Log connection acquisition for debugging
	p.log.Debug("Connection acquired",
		"provider", conn.Value().provider.Host,
		"total_connections", conn.Value().provider.MaxConnections,
	)

	return pooledConn, nil
}

func (p *connectionPool) GetProvidersInfo() []ProviderInfo {
	pi := make([]ProviderInfo, 0)

	for _, pool := range p.connPools {
		pi = append(pi, ProviderInfo{
			Host:                     pool.provider.Host,
			Username:                 pool.provider.Username,
			MaxConnections:           pool.provider.MaxConnections,
			UsedConnections:          int(pool.connectionPool.Stat().TotalResources()),
			MaxConnectionIdleTimeout: time.Duration(pool.provider.MaxConnectionIdleTimeInSeconds) * time.Second,
		})
	}

	return pi
}

func (p *connectionPool) Reconfigure(c ...Config) error {
	// Check if already shutdown
	if atomic.LoadInt32(&p.isShutdown) == 1 {
		return fmt.Errorf("connection pool is shutdown")
	}

	newConfig := mergeWithDefault(c...)

	if len(newConfig.Providers) == 0 {
		return ErrNoProviderAvailable
	}

	p.log.Info("starting incremental reconfiguration")

	// Analyze what needs to change
	diff := config.AnalyzeChanges(&p.config, &newConfig)

	if !diff.HasChanges {
		p.log.Info("no reconfiguration changes detected")
		return nil
	}

	p.log.Info("reconfiguration changes detected",
		"migration_id", diff.MigrationID,
		"changes", len(diff.Changes),
	)

	// Start incremental migration
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	if err := p.migrationManager.StartMigration(ctx, diff); err != nil {
		return fmt.Errorf("failed to start migration: %w", err)
	}

	// Update pool configuration
	p.config = newConfig

	p.log.Info("incremental migration started", "migration_id", diff.MigrationID)
	return nil
}

// GetReconfigurationStatus returns the status of a specific reconfiguration
func (p *connectionPool) GetReconfigurationStatus(migrationID string) (*ReconfigurationStatus, bool) {
	status, exists := p.migrationManager.GetReconfigurationStatus(migrationID)
	if !exists {
		return nil, false
	}
	return convertToInternalStatus(status), true
}

// GetActiveReconfigurations returns all currently active reconfigurations
func (p *connectionPool) GetActiveReconfigurations() map[string]*ReconfigurationStatus {
	internalReconfigurations := p.migrationManager.GetActiveReconfigurations()
	result := make(map[string]*ReconfigurationStatus)

	for k, v := range internalReconfigurations {
		result[k] = convertToInternalStatus(v)
	}

	return result
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
		written      int64
		conn         PooledConnection
		bytesWritten int64
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
				_ = c.Close()
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

		n, err := nntpConn.BodyDecoded(msgID, w, written)
		if err != nil && ctx.Err() == nil {
			written += n
			return fmt.Errorf("error downloading body: %w", err)
		}

		bytesWritten = n

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
			p.log.DebugContext(ctx,
				"Retrying body",
				"error", err,
				"retry", n,
			)

			if conn != nil {
				provider := conn.Provider()

				if nntpcli.IsArticleNotFoundError(err) {
					skipProviders = append(skipProviders, provider.ID())
					p.log.DebugContext(ctx,
						"Article not found in provider, trying another one...",
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

		if conn != nil {
			if errors.Is(err, context.Canceled) {
				_ = conn.Free()
			} else {
				_ = conn.Close()
			}
		}

		if !errors.Is(err, context.Canceled) {
			p.log.DebugContext(ctx,
				"All body retries exhausted",
				"error", retryErr,
			)
		}

		if nntpcli.IsArticleNotFoundError(err) {
			// if article not found, we don't want to retry so mark it as corrupted
			return bytesWritten, ErrArticleNotFoundInProviders
		}

		return bytesWritten, err
	}

	conn = nil

	return bytesWritten, nil
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
				_ = c.Close()
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
		if err != nil && ctx.Err() == nil {
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
			p.log.DebugContext(ctx,
				"Retrying body reader",
				"error", err,
				"retry", n,
			)

			if conn != nil {
				provider := conn.Provider()

				if nntpcli.IsArticleNotFoundError(err) {
					skipProviders = append(skipProviders, provider.ID())
					p.log.DebugContext(ctx,
						"Article not found in provider, trying another one...",
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

		if conn != nil {
			if errors.Is(err, context.Canceled) {
				_ = conn.Free()
			} else {
				_ = conn.Close()
			}
		}

		if !errors.Is(err, context.Canceled) {
			p.log.DebugContext(ctx,
				"All body reader retries exhausted",
				"error", retryErr,
			)
		}

		if nntpcli.IsArticleNotFoundError(err) {
			return nil, ErrArticleNotFoundInProviders
		}

		return nil, err
	}

	// Wrap the reader to manage the connection lifecycle
	return &pooledBodyReader{
		reader: reader,
		conn:   conn,
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

		err = nntpConn.Post(r)
		if err != nil {
			return fmt.Errorf("error posting article: %w", err)
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
			p.log.DebugContext(ctx,
				"Retrying post",
				"error", err,
				"retry", n,
			)

			if conn != nil {
				provider := conn.Provider()

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
				_ = conn.Free()
			} else {
				_ = conn.Close()
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
				_ = c.Close()
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
		if err != nil && ctx.Err() == nil {
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
			p.log.DebugContext(ctx,
				"Retrying stat",
				"error", err,
				"retry", n,
			)

			if conn != nil {
				provider := conn.Provider()

				if nntpcli.IsArticleNotFoundError(err) {
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

		if conn != nil {
			if errors.Is(err, context.Canceled) {
				_ = conn.Free()
			} else {
				_ = conn.Close()
			}
		}

		if !errors.Is(err, context.Canceled) {
			p.log.DebugContext(ctx,
				"All stat retries exhausted",
				"error", retryErr,
			)
		}

		if nntpcli.IsArticleNotFoundError(err) {
			// if article not found, we don't want to retry so mark it as corrupted
			return res, ErrArticleNotFoundInProviders
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

	idle := make([]*puddle.Resource[*internalConnection], 0)

	for _, pool := range p.connPools {
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
		if _, err := p.GetConnection(ctx, []string{}, false); err != nil {
			return err
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
