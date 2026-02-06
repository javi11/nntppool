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
	"github.com/jackc/puddle/v2"
	"github.com/javi11/nntppool/v2/pkg/nntpcli"
	"golang.org/x/net/proxy"
)

func getPools(
	p []UsenetProviderConfig,
	nttpCli nntpcli.Client,
	log Logger,
	metrics *PoolMetrics,
) ([]*providerPool, error) {
	pSlice := make([]*providerPool, 0)

	for _, provider := range p {
		pools, err := puddle.NewPool(
			&puddle.Config[*internalConnection]{
				Constructor: func(ctx context.Context) (*internalConnection, error) {
					nntpCon, err := dialNNTP(
						ctx,
						nttpCli,
						provider,
						log,
					)
					if err != nil {
						return nil, err
					}

					// Record connection creation
					if metrics != nil {
						metrics.RecordConnectionCreated()
					}

					return &internalConnection{
						nntp:     nntpCon,
						provider: provider,
					}, nil
				},
				Destructor: func(value *internalConnection) {
					_ = value.nntp.Close()

					// Record connection destruction
					if metrics != nil {
						metrics.RecordConnectionDestroyed()
					}
				},
				MaxSize: int32(provider.MaxConnections),
			},
		)
		if err != nil {
			return nil, err
		}

		pSlice = append(pSlice, &providerPool{
			connectionPool: pools,
			provider:       provider,
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
			c, err := poolCopy.connectionPool.Acquire(ctx)
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

			defer c.Release()

			if len(poolCopy.provider.VerifyCapabilities) > 0 {
				conn := c.Value()
				caps, _ := conn.nntp.Capabilities()

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

	// Fallback: dial with context cancellation handling.
	// The goroutine will complete even if context is cancelled, but we handle
	// the cleanup properly to avoid connection leaks.
	type dialResult struct {
		conn net.Conn
		err  error
	}
	// Use unbuffered channel so goroutine blocks until result is consumed or
	// we spawn a cleanup goroutine
	done := make(chan dialResult)

	go func() {
		conn, err := s.dialer.Dial(network, address)
		done <- dialResult{conn, err}
	}()

	select {
	case <-ctx.Done():
		// Context cancelled. The goroutine is still running Dial().
		// Spawn a cleanup goroutine to wait for the dial to complete
		// and close any connection that was established.
		go func() {
			result := <-done
			if result.conn != nil {
				_ = result.conn.Close()
			}
		}()
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
