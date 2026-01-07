package nntppool

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/javi11/nntppool/v3/pkg/nntpcli"
)

// Provider wraps an nntpcli.Client with health tracking and metrics.
type Provider struct {
	config ProviderConfig
	client *nntpcli.Client

	mu        sync.RWMutex
	healthy   bool
	lastError error
	lastCheck time.Time

	// Metrics
	requestCount  atomic.Int64
	errorCount    atomic.Int64
	notFoundCount atomic.Int64
}

// NewProvider creates a new provider from the given configuration.
func NewProvider(ctx context.Context, cfg ProviderConfig) (*Provider, error) {
	cfg = cfg.WithDefaults()

	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	// Create TLS config if needed
	var tlsConfig *tls.Config
	if cfg.TLS {
		if cfg.TLSConfig != nil {
			tlsConfig = cfg.TLSConfig.Clone()
		} else {
			tlsConfig = &tls.Config{
				ServerName: cfg.Host,
				MinVersion: tls.VersionTLS12,
			}
		}
	}

	// Create the underlying client
	client, err := nntpcli.NewClient(
		ctx,
		cfg.Address(),
		tlsConfig,
		cfg.MaxConnections,
		cfg.InflightPerConn,
		nntpcli.Auth{
			Username: cfg.Username,
			Password: cfg.Password,
		},
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create client for %s: %w", cfg.Name, err)
	}

	return &Provider{
		config:  cfg,
		client:  client,
		healthy: true,
	}, nil
}

// NewProviderWithConnFactory creates a new provider with a custom connection factory.
func NewProviderWithConnFactory(ctx context.Context, cfg ProviderConfig, factory nntpcli.ConnFactory) (*Provider, error) {
	cfg = cfg.WithDefaults()

	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	client, err := nntpcli.NewClientWithConnFactory(
		ctx,
		cfg.MaxConnections,
		cfg.InflightPerConn,
		nntpcli.Auth{
			Username: cfg.Username,
			Password: cfg.Password,
		},
		factory,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create client for %s: %w", cfg.Name, err)
	}

	return &Provider{
		config:  cfg,
		client:  client,
		healthy: true,
	}, nil
}

// Name returns the provider's name.
func (p *Provider) Name() string {
	return p.config.Name
}

// Host returns the provider's host.
func (p *Provider) Host() string {
	return p.config.Host
}

// Priority returns the provider's priority.
func (p *Provider) Priority() int {
	return p.config.Priority
}

// IsBackup returns whether this is a backup provider.
func (p *Provider) IsBackup() bool {
	return p.config.IsBackup
}

// IsHealthy returns whether the provider is currently healthy.
func (p *Provider) IsHealthy() bool {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.healthy
}

// MarkHealthy marks the provider as healthy.
func (p *Provider) MarkHealthy() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.healthy = true
	p.lastError = nil
	p.lastCheck = time.Now()
}

// MarkUnhealthy marks the provider as unhealthy with the given error.
func (p *Provider) MarkUnhealthy(err error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.healthy = false
	p.lastError = err
	p.lastCheck = time.Now()
	p.errorCount.Add(1)
}

// LastError returns the last error that caused the provider to be marked unhealthy.
func (p *Provider) LastError() error {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.lastError
}

// Send sends a request to the provider and returns the response.
func (p *Provider) Send(ctx context.Context, payload []byte, bodyWriter io.Writer) (nntpcli.Response, error) {
	p.requestCount.Add(1)

	// Apply timeout if configured
	if p.config.ReadTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, p.config.ReadTimeout)
		defer cancel()
	}

	respCh := p.client.Send(ctx, payload, bodyWriter)
	resp, ok := <-respCh
	if !ok {
		err := ctx.Err()
		if err == nil {
			err = fmt.Errorf("response channel closed")
		}
		return nntpcli.Response{}, &ProviderError{
			ProviderName: p.config.Name,
			Host:         p.config.Host,
			Err:          err,
			Temporary:    true,
		}
	}

	if resp.Err != nil {
		// Check if it's a network error (temporary)
		var netErr net.Error
		temporary := errors.As(resp.Err, &netErr) && netErr.Timeout()

		return resp, &ProviderError{
			ProviderName: p.config.Name,
			Host:         p.config.Host,
			Err:          resp.Err,
			Temporary:    temporary,
		}
	}

	// Verify we got a valid success response (2xx status codes)
	// For BODY/ARTICLE/HEAD commands, we expect 220/221/222
	isSuccess := resp.StatusCode >= 200 && resp.StatusCode < 300

	if !isSuccess {
		// Track article not found separately for metrics
		if resp.StatusCode == StatusArticleNotFound {
			p.notFoundCount.Add(1)
		}

		// Determine error message
		message := resp.Status
		if message == "" {
			message = fmt.Sprintf("unexpected status code: %d", resp.StatusCode)
		}

		return resp, &ProviderError{
			ProviderName: p.config.Name,
			Host:         p.config.Host,
			StatusCode:   resp.StatusCode,
			Message:      message,
			Temporary:    false,
		}
	}

	return resp, nil
}

// HealthCheck performs a health check on the provider using the DATE command.
func (p *Provider) HealthCheck(ctx context.Context) error {
	// Apply timeout
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	respCh := p.client.Send(ctx, []byte("DATE\r\n"), nil)
	resp, ok := <-respCh
	if !ok {
		err := fmt.Errorf("health check failed: channel closed")
		p.MarkUnhealthy(err)
		return err
	}

	if resp.Err != nil {
		p.MarkUnhealthy(resp.Err)
		return resp.Err
	}

	// DATE should return 111
	if resp.StatusCode != 111 {
		err := fmt.Errorf("unexpected status code: %d %s", resp.StatusCode, resp.Status)
		p.MarkUnhealthy(err)
		return err
	}

	p.MarkHealthy()
	return nil
}

// Close closes the provider's client.
func (p *Provider) Close() error {
	return p.client.Close()
}

// Stats returns the provider's statistics.
func (p *Provider) Stats() ProviderStats {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return ProviderStats{
		Name:          p.config.Name,
		Host:          p.config.Host,
		Healthy:       p.healthy,
		LastError:     p.lastError,
		LastCheck:     p.lastCheck,
		RequestCount:  p.requestCount.Load(),
		ErrorCount:    p.errorCount.Load(),
		NotFoundCount: p.notFoundCount.Load(),
	}
}

// ProviderStats contains statistics for a provider.
type ProviderStats struct {
	Name          string
	Host          string
	Healthy       bool
	LastError     error
	LastCheck     time.Time
	RequestCount  int64
	ErrorCount    int64
	NotFoundCount int64
}
