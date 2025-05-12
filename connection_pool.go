//go:generate mockgen -source=./connection_pool.go -destination=./connection_pool_mock.go -package=nntppool UsenetConnectionPool

package nntppool

import (
	"context"
	"errors"
	"fmt"
	"io"
	"slices"
	"sync"
	"time"

	"github.com/avast/retry-go"
	"github.com/jackc/puddle/v2"
	"github.com/javi11/nntpcli"
)

const defaultHealthCheckInterval = 1 * time.Minute

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
	Post(ctx context.Context, r io.Reader) error
	Stat(ctx context.Context, msgID string, nntpGroups []string) (int, error)
	GetProvidersInfo() []ProviderInfo
	HotReload(...Config) error
	Quit()
}

type connectionPool struct {
	closeChan chan struct{}
	connPools []providerPool
	log       Logger
	config    Config
	wg        sync.WaitGroup
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

	pool := &connectionPool{
		connPools: pools,
		log:       log,
		config:    config,
		closeChan: make(chan struct{}, 1),
		wg:        sync.WaitGroup{},
	}

	healthCheckInterval := defaultHealthCheckInterval
	if config.HealthCheckInterval > 0 {
		healthCheckInterval = config.HealthCheckInterval
	}

	pool.wg.Add(1)
	go pool.connectionHealCheck(healthCheckInterval)

	return pool, nil
}

func (p *connectionPool) Quit() {
	p.closeChan <- struct{}{}
	close(p.closeChan)

	p.wg.Wait()

	for _, cp := range p.connPools {
		cp.connectionPool.Close()
	}

	p.connPools = nil
}

func (p *connectionPool) GetConnection(
	ctx context.Context,
	skipProviders []string,
	useBackupProviders bool,
) (PooledConnection, error) {
	if p.connPools == nil {
		return nil, fmt.Errorf("connection pool is not available")
	}

	cp := make([]providerPool, 0, len(p.connPools))

	for _, provider := range p.connPools {
		if !slices.Contains(skipProviders, provider.provider.ID()) &&
			(!provider.provider.IsBackupProvider || useBackupProviders) {
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

	return pooledConnection{
		resource: conn,
		log:      p.log,
	}, nil
}

func (p *connectionPool) GetProvidersInfo() []ProviderInfo {
	pi := make([]ProviderInfo, 0)

	for _, pool := range p.connPools {
		pi = append(pi, ProviderInfo{
			Host:            pool.provider.Host,
			Username:        pool.provider.Username,
			MaxConnections:  pool.provider.MaxConnections,
			UsedConnections: int(pool.connectionPool.Stat().TotalResources()),
		})
	}

	return pi
}

func (p *connectionPool) HotReload(c ...Config) error {
	p.log.Info("reloading connection pool")
	defer p.log.Info("connection pool reloaded")

	config := mergeWithDefault(c...)

	if len(config.Providers) == 0 {
		return ErrNoProviderAvailable
	}

	p.closeChan <- struct{}{}
	close(p.closeChan)

	p.wg.Wait()

	oldConnPools := p.connPools
	for _, cp := range oldConnPools {
		idle := cp.connectionPool.AcquireAllIdle()
		for _, res := range idle {
			res.Destroy()
		}

		cp.connectionPool.Reset()
	}

	cp, err := getPools(config.Providers, config.NntpCli, config.Logger)
	if err != nil {
		return err
	}

	p.connPools = cp
	p.config = config

	p.closeChan = make(chan struct{}, 1)
	p.wg.Add(1)

	go p.connectionHealCheck(config.HealthCheckInterval)

	if !config.SkipProvidersVerificationOnCreation {
		p.log.Info("verifying providers...")

		if err := verifyProviders(cp, config.Logger); err != nil {
			return err
		}

		p.log.Info("providers verified")
	}

	for _, cp := range oldConnPools {
		cp.connectionPool.Close()
	}

	return nil
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
		useBackupProviders := false
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

		conn.Free()

		return nil
	},
		retry.Context(ctx),
		retry.Attempts(p.config.MaxRetries),
		retry.DelayType(p.config.delayTypeFn),
		retry.Delay(p.config.retryDelay),
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

					conn.Free()
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
				conn.Free()
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

		conn.Free()

		return nil
	},
		retry.Context(ctx),
		retry.Attempts(p.config.MaxRetries),
		retry.DelayType(p.config.delayTypeFn),
		retry.Delay(p.config.retryDelay),
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

					conn.Free()
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
				conn.Free()
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

		conn.Free()

		return nil
	},
		retry.Context(ctx),
		retry.Attempts(p.config.MaxRetries),
		retry.DelayType(p.config.delayTypeFn),
		retry.Delay(p.config.retryDelay),
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
					conn.Free()
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
				conn.Free()
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
	cPool []providerPool,
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

	for {
		select {
		case <-p.closeChan:
			return
		case <-ticker.C:
			p.checkHealth()
		}
	}
}

func (p *connectionPool) checkHealth() {
	for {
		// If checkMinConns failed we don't destroy any connections since we couldn't
		// even get to minConns
		if err := p.checkMinConns(); err != nil {
			break
		}

		if !p.checkConnsHealth() {
			// Since we didn't destroy any connections we can stop looping
			break
		}

		// Technically Destroy is asynchronous but 500ms should be enough for it to
		// remove it from the underlying pool
		select {
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

func (p *connectionPool) checkMinConns() error {
	// TotalConns can include ones that are being destroyed but we should have
	// sleep(500ms) around all of the destroys to help prevent that from throwing
	// off this check
	totalConns := 0
	for _, pool := range p.connPools {
		totalConns += int(pool.connectionPool.Stat().TotalResources())
	}

	toCreate := p.config.MinConnections - totalConns
	if toCreate > 0 {
		return p.createIdleResources(context.Background(), toCreate)
	}

	return nil
}

func (p *connectionPool) isExpired(res *puddle.Resource[*internalConnection]) bool {
	return time.Now().After(res.Value().nntp.MaxAgeTime())
}
