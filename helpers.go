package nntppool

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/textproto"
	"net/url"
	"slices"
	"strings"
	"time"

	"github.com/hashicorp/go-multierror"
	"github.com/javi11/nntppool/v3/internal/netconn"
	"github.com/javi11/nntppool/v3/pkg/nntpcli"
	"github.com/yudhasubki/netpool"
	"golang.org/x/net/proxy"
)

func joinGroup(c nntpcli.Connection, groups []string) error {
	var err error

	for _, g := range groups {
		if err = c.JoinGroup(g); err == nil {
			return nil
		}
	}

	return err
}
func getPools(
	p []UsenetProviderConfig,
	nttpCli nntpcli.Client,
	log Logger,
	metrics *PoolMetrics,
) ([]*providerPool, error) {
	pSlice := make([]*providerPool, 0)

	for _, provider := range p {
		providerCopy := provider // Capture for closure

		// Calculate idle timeout from config
		maxIdleTime := time.Duration(providerCopy.MaxConnectionIdleTimeInSeconds) * time.Second
		if maxIdleTime == 0 {
			maxIdleTime = 40 * time.Minute // Default idle timeout
		}

		pool, err := netpool.New(
			func(ctx context.Context) (net.Conn, error) {
				nntpCon, err := dialNNTP(ctx, nttpCli, providerCopy, log)
				if err != nil {
					return nil, err
				}

				// Record connection creation
				if metrics != nil {
					metrics.RecordConnectionCreated()
				}

				// Wrap the connection for netpool management
				return netconn.NewNNTPConnWrapper(nntpCon.NetConn(), nntpCon), nil
			},
			netpool.Config{
				MaxPool:     int32(providerCopy.MaxConnections),
				MinPool:     0,
				DialTimeout: 30 * time.Second,
				MaxIdleTime: maxIdleTime,
			},
		)
		if err != nil {
			return nil, err
		}

		pSlice = append(pSlice, &providerPool{
			connectionPool: pool,
			provider:       providerCopy,
			state:          ProviderStateActive,
		})
	}

	return pSlice, nil
}

func verifyProviders(pools []*providerPool, log Logger) error {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	wg := multierror.Group{}
	var offlineProviders []string

	for _, pool := range pools {
		poolCopy := pool // Capture the loop variable
		wg.Go(func() error {
			c, err := poolCopy.connectionPool.GetWithContext(ctx)
			if err != nil {
				log.Warn(fmt.Sprintf("failed to verify provider %s, marking as offline: %v", poolCopy.provider.Host, err))

				// Determine if this is an authentication error
				if isAuthenticationError(err) {
					poolCopy.SetState(ProviderStateAuthenticationFailed)
					log.Error(fmt.Sprintf("authentication failed for provider %s", poolCopy.provider.Host))
				} else {
					poolCopy.SetState(ProviderStateOffline)
				}

				poolCopy.SetConnectionAttempt(err)
				offlineProviders = append(offlineProviders, poolCopy.provider.Host)

				return nil
			}

			defer poolCopy.connectionPool.Put(c)

			// Extract the NNTP connection from the wrapper
			wrapper, ok := c.(*netconn.NNTPConnWrapper)
			if !ok {
				err := fmt.Errorf("invalid connection type for provider %s", poolCopy.provider.Host)
				poolCopy.SetState(ProviderStateOffline)
				poolCopy.SetConnectionAttempt(err)
				offlineProviders = append(offlineProviders, poolCopy.provider.Host)
				return nil
			}

			nntpConn := wrapper.NNTPConnection()

			if len(poolCopy.provider.VerifyCapabilities) > 0 {
				caps, _ := nntpConn.Capabilities()

				log.Info(fmt.Sprintf("capabilities for provider %s: %v", poolCopy.provider.Host, caps))

				for _, cap := range poolCopy.provider.VerifyCapabilities {
					if !slices.Contains(caps, cap) {
						err := fmt.Errorf("provider %s does not support capability %s", poolCopy.provider.Host, cap)
						log.Warn(fmt.Sprintf("capability check failed for provider %s, marking as offline: %v", poolCopy.provider.Host, err))
						poolCopy.SetState(ProviderStateOffline)
						poolCopy.SetConnectionAttempt(err)
						offlineProviders = append(offlineProviders, poolCopy.provider.Host)

						return nil
					}
				}
			}

			// Mark as successfully connected
			poolCopy.SetConnectionAttempt(nil)
			poolCopy.SetState(ProviderStateActive)

			log.Info(fmt.Sprintf("provider %s verified successfully", poolCopy.provider.Host))

			return nil
		})
	}

	err := wg.Wait().ErrorOrNil()

	if len(offlineProviders) > 0 {
		log.Info(fmt.Sprintf("pool created with %d offline providers: %v", len(offlineProviders), offlineProviders))
	}

	return err
}

// isAuthenticationError checks if the error is related to authentication failure
func isAuthenticationError(err error) bool {
	if err == nil {
		return false
	}

	errStr := strings.ToLower(err.Error())

	// Check for common authentication error messages
	if strings.Contains(errStr, "authentication") ||
		strings.Contains(errStr, "unauthorized") ||
		strings.Contains(errStr, "invalid username") ||
		strings.Contains(errStr, "invalid password") ||
		strings.Contains(errStr, "wrong username") ||
		strings.Contains(errStr, "wrong password") ||
		strings.Contains(errStr, "bad username") ||
		strings.Contains(errStr, "bad password") ||
		strings.Contains(errStr, "login failed") ||
		strings.Contains(errStr, "access denied") {
		return true
	}

	// Check for NNTP error codes related to authentication
	var nntpErr *textproto.Error
	if errors.As(err, &nntpErr) {
		// 401 Authentication Required, 403 Forbidden, 480 Authentication Failed
		return nntpErr.Code == AuthenticationRequiredCode ||
			nntpErr.Code == AuthenticationFailedCode ||
			nntpErr.Code == InvalidUsernamePasswordCode
	}

	return false
}

// TestProviderConnectivity tests connectivity to a provider without requiring a pool
// This is a standalone utility function that can be used independently
// If client is nil, a default NNTP client will be created
func TestProviderConnectivity(ctx context.Context, config UsenetProviderConfig, logger Logger, client nntpcli.Client) error {
	if client == nil {
		client = nntpcli.New()
	}

	if logger == nil {
		logger = slog.Default()
	}

	logger.Debug(fmt.Sprintf("testing connectivity to provider %s:%d", config.Host, config.Port))

	// Create connection using the same dialNNTP function used internally
	conn, err := dialNNTP(ctx, client, config, logger)
	if err != nil {
		if isAuthenticationError(err) {
			return fmt.Errorf("authentication failed for provider %s: %w", config.Host, err)
		}
		return fmt.Errorf("failed to connect to provider %s: %w", config.Host, err)
	}
	defer func() {
		if closeErr := conn.Close(); closeErr != nil {
			logger.DebugContext(ctx, "Failed to close test connection", "error", closeErr, "provider", config.Host)
		}
	}()

	// Verify required capabilities if specified
	if len(config.VerifyCapabilities) > 0 {
		// Test the connection by getting capabilities
		caps, err := conn.Capabilities()
		if err != nil {
			if isAuthenticationError(err) {
				return fmt.Errorf("authentication failed during capabilities check for provider %s: %w", config.Host, err)
			}
			return fmt.Errorf("failed to get capabilities from provider %s: %w", config.Host, err)
		}

		logger.Debug(fmt.Sprintf("capabilities for provider %s: %v", config.Host, caps))

		for _, requiredCap := range config.VerifyCapabilities {
			if !slices.Contains(caps, requiredCap) {
				return fmt.Errorf("provider %s does not support required capability %s", config.Host, requiredCap)
			}
		}
	}

	logger.Info(fmt.Sprintf("connectivity test successful for provider %s", config.Host))
	return nil
}

func dialNNTP(
	ctx context.Context,
	cli nntpcli.Client,
	p UsenetProviderConfig,
	log Logger,
) (nntpcli.Connection, error) {
	var (
		c   nntpcli.Connection
		err error
	)

	ttl := time.Duration(p.MaxConnectionTTLInSeconds) * time.Second

	// Create proxy dialer if configured
	var proxyDialer nntpcli.ContextDialer
	if p.ProxyURL != "" {
		proxyDialer, err = createProxyDialer(p.ProxyURL)
		if err != nil {
			return nil, fmt.Errorf("failed to create proxy dialer for %s: %w", p.Host, err)
		}
		log.Debug(fmt.Sprintf("using proxy for provider %s", p.Host))
	}

	dialConfig := nntpcli.DialConfig{
		KeepAliveTime: ttl,
		Dialer:        proxyDialer,
	}

	if p.TLS {
		c, err = cli.DialTLS(
			ctx,
			p.Host,
			p.Port,
			p.InsecureSSL,
			dialConfig,
		)
		if err != nil {
			var netErr net.Error

			if errors.As(err, &netErr) && netErr.Timeout() {
				log.Error(fmt.Sprintf("timeout connecting to %s:%v, retrying", p.Host, p.Port), "error", netErr)
			}

			return nil, fmt.Errorf("error dialing to %v/%v TLS: %w", p.Host, p.Username, err)
		}
	} else {
		c, err = cli.Dial(
			ctx,
			p.Host,
			p.Port,
			dialConfig,
		)
		if err != nil {
			var netErr net.Error

			if errors.As(err, &netErr) && netErr.Timeout() {
				log.Error(fmt.Sprintf("timeout connecting to %s:%v, retrying", p.Host, p.Port), "error", netErr)
			}

			return nil, fmt.Errorf("error dialing to %v/%v: %w", p.Host, p.Username, err)
		}
	}

	if p.Username != "" && p.Password != "" {
		if err := c.Authenticate(p.Username, p.Password); err != nil {
			return nil, fmt.Errorf("error authenticating to %v/%v: %w", p.Host, p.Username, err)
		}
	}

	return c, nil
}

// socks5ContextDialer wraps proxy.Dialer to support DialContext
type socks5ContextDialer struct {
	dialer proxy.Dialer
}

func (s *socks5ContextDialer) DialContext(ctx context.Context, network, address string) (net.Conn, error) {
	// Check if the dialer supports DialContext directly
	if ctxDialer, ok := s.dialer.(proxy.ContextDialer); ok {
		return ctxDialer.DialContext(ctx, network, address)
	}

	// Fallback: dial with basic context cancellation check
	type dialResult struct {
		conn net.Conn
		err  error
	}
	done := make(chan dialResult, 1)

	go func() {
		conn, err := s.dialer.Dial(network, address)
		done <- dialResult{conn, err}
	}()

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case result := <-done:
		return result.conn, result.err
	}
}

// createProxyDialer creates a SOCKS5 dialer from a proxy URL.
// Returns nil, nil if proxyURL is empty.
// Format: socks5://[user:password@]host:port
func createProxyDialer(proxyURL string) (nntpcli.ContextDialer, error) {
	if proxyURL == "" {
		return nil, nil
	}

	u, err := url.Parse(proxyURL)
	if err != nil {
		return nil, fmt.Errorf("invalid proxy URL: %w", err)
	}

	if u.Scheme != "socks5" {
		return nil, fmt.Errorf("unsupported proxy scheme: %s (only socks5 is supported)", u.Scheme)
	}

	var auth *proxy.Auth
	if u.User != nil {
		password, _ := u.User.Password()
		auth = &proxy.Auth{
			User:     u.User.Username(),
			Password: password,
		}
	}

	dialer, err := proxy.SOCKS5("tcp", u.Host, auth, proxy.Direct)
	if err != nil {
		return nil, fmt.Errorf("failed to create SOCKS5 dialer: %w", err)
	}

	return &socks5ContextDialer{dialer: dialer}, nil
}

// isProxyError checks if the error is related to proxy connection failure
func isProxyError(err error) bool {
	if err == nil {
		return false
	}
	errStr := strings.ToLower(err.Error())
	return strings.Contains(errStr, "socks") ||
		strings.Contains(errStr, "proxy")
}

// retryWithProviderRotation executes fn with automatic provider rotation on article-not-found errors.
// It tries providers in order (primaries first, then backups), skipping hosts that return article-not-found.
// The function handles connection acquisition and cleanup:
//   - On success: caller must free/close the connection in fn
//   - On article-not-found: connection is freed and next provider is tried
//   - On other errors: connection is closed and error is returned immediately
//
// Returns ErrArticleNotFoundInProviders if all providers return article-not-found.
// Returns the last error if all providers are exhausted for other reasons.
func (p *connectionPool) retryWithProviderRotation(
	ctx context.Context,
	fn func(conn PooledConnection) error,
) error {
	skipHosts := make([]string, 0)
	hadArticleNotFound := false

	for {
		// Check context before attempting
		if ctx.Err() != nil {
			return ctx.Err()
		}

		conn, err := p.GetConnection(ctx, skipHosts)
		if err != nil {
			// No more providers available
			if hadArticleNotFound {
				return ErrArticleNotFoundInProviders
			}
			return err
		}

		// Execute the operation
		err = fn(conn)
		if err == nil {
			// Success - caller is responsible for freeing the connection
			return nil
		}

		host := conn.Provider().Host

		if nntpcli.IsArticleNotFoundError(err) {
			// Article not found - skip this host and try next provider
			skipHosts = append(skipHosts, host)
			hadArticleNotFound = true

			p.log.DebugContext(ctx,
				"Article not found in provider, trying another one...",
				"provider", host,
			)

			// Record metrics
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

		return err
	}
}
