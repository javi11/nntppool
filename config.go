package nntppool

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/avast/retry-go/v4"
	"github.com/javi11/nntppool/pkg/nntpcli"

	"github.com/javi11/nntppool/internal/config"
)

// Logger interface compatible with slog.Logger
type Logger interface {
	Debug(msg string, args ...any)
	Info(msg string, args ...any)
	Warn(msg string, args ...any)
	Error(msg string, args ...any)
	DebugContext(ctx context.Context, msg string, args ...any)
	InfoContext(ctx context.Context, msg string, args ...any)
	WarnContext(ctx context.Context, msg string, args ...any)
	ErrorContext(ctx context.Context, msg string, args ...any)
}

type DelayType int

const (
	DelayTypeFixed DelayType = iota
	DelayTypeRandom
	DelayTypeExponential
)

type Config struct {
	delayTypeFn         retry.DelayTypeFunc
	Logger              Logger
	NntpCli             nntpcli.Client
	Providers           []UsenetProviderConfig
	HealthCheckInterval time.Duration
	MinConnections      int
	MaxRetries          uint
	DelayType           DelayType
	// Deprecated: now is always false
	SkipProvidersVerificationOnCreation bool
	RetryDelay                          time.Duration
	ShutdownTimeout                     time.Duration
	DrainTimeout                        time.Duration
	ForceCloseTimeout                   time.Duration
	DefaultConnectionLease              time.Duration
	ProviderReconnectInterval           time.Duration
	ProviderMaxReconnectInterval        time.Duration
	ProviderHealthCheckStagger          time.Duration
	ProviderHealthCheckTimeout          time.Duration
}

// Adapter methods for internal package interfaces
func (c *Config) GetProviders() []config.ProviderConfig {
	providers := make([]config.ProviderConfig, len(c.Providers))
	for i, p := range c.Providers {
		providers[i] = &p
	}
	return providers
}

// Helper interface for internal packages
type ProviderConfig interface {
	ID() string
	GetHost() string
	GetUsername() string
	GetPassword() string
	GetPort() int
	GetMaxConnections() int
	GetMaxConnectionIdleTimeInSeconds() int
	GetMaxConnectionTTLInSeconds() int
	GetTLS() bool
	GetInsecureSSL() bool
	GetIsBackupProvider() bool
	GetVerifyCapabilities() []string
}

type UsenetProviderConfig struct {
	Host                           string
	Username                       string
	Password                       string
	VerifyCapabilities             []string
	Port                           int
	MaxConnections                 int
	MaxConnectionIdleTimeInSeconds int
	MaxConnectionTTLInSeconds      int
	TLS                            bool
	InsecureSSL                    bool
	IsBackupProvider               bool
}

func (u *UsenetProviderConfig) ID() string {
	return fmt.Sprintf("%s_%s", u.Host, u.Username)
}

// Adapter methods for internal package interfaces
func (u *UsenetProviderConfig) GetHost() string        { return u.Host }
func (u *UsenetProviderConfig) GetUsername() string    { return u.Username }
func (u *UsenetProviderConfig) GetPassword() string    { return u.Password }
func (u *UsenetProviderConfig) GetPort() int           { return u.Port }
func (u *UsenetProviderConfig) GetMaxConnections() int { return u.MaxConnections }
func (u *UsenetProviderConfig) GetMaxConnectionIdleTimeInSeconds() int {
	return u.MaxConnectionIdleTimeInSeconds
}
func (u *UsenetProviderConfig) GetMaxConnectionTTLInSeconds() int { return u.MaxConnectionTTLInSeconds }
func (u *UsenetProviderConfig) GetTLS() bool                      { return u.TLS }
func (u *UsenetProviderConfig) GetInsecureSSL() bool              { return u.InsecureSSL }
func (u *UsenetProviderConfig) GetIsBackupProvider() bool         { return u.IsBackupProvider }
func (u *UsenetProviderConfig) GetVerifyCapabilities() []string   { return u.VerifyCapabilities }

type Option func(*Config)

var (
	configDefault = Config{
		HealthCheckInterval:          1 * time.Minute,
		MinConnections:               5,
		MaxRetries:                   4,
		RetryDelay:                   5 * time.Second,
		ShutdownTimeout:              30 * time.Second,
		DrainTimeout:                 10 * time.Second,
		ForceCloseTimeout:            5 * time.Second,
		DefaultConnectionLease:       10 * time.Minute,
		ProviderReconnectInterval:    30 * time.Second,
		ProviderMaxReconnectInterval: 5 * time.Minute,
		ProviderHealthCheckStagger:   10 * time.Second,
		ProviderHealthCheckTimeout:   5 * time.Second,
	}
	providerConfigDefault = UsenetProviderConfig{
		MaxConnections:                 10,
		MaxConnectionIdleTimeInSeconds: 2400,
		MaxConnectionTTLInSeconds:      2400,
	}
)

func mergeWithDefault(config ...Config) Config {
	if len(config) == 0 {
		return configDefault
	}

	cfg := config[0]

	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}

	if cfg.NntpCli == nil {
		cfg.NntpCli = nntpcli.New()
	}

	if cfg.HealthCheckInterval == 0 {
		cfg.HealthCheckInterval = configDefault.HealthCheckInterval
	}

	if cfg.MinConnections == 0 {
		cfg.MinConnections = configDefault.MinConnections
	}

	if cfg.MaxRetries == 0 {
		cfg.MaxRetries = configDefault.MaxRetries
	}

	if cfg.RetryDelay == 0 {
		cfg.RetryDelay = configDefault.RetryDelay
	}

	if cfg.ShutdownTimeout == 0 {
		cfg.ShutdownTimeout = configDefault.ShutdownTimeout
	}

	if cfg.DrainTimeout == 0 {
		cfg.DrainTimeout = configDefault.DrainTimeout
	}

	if cfg.ForceCloseTimeout == 0 {
		cfg.ForceCloseTimeout = configDefault.ForceCloseTimeout
	}

	if cfg.DefaultConnectionLease == 0 {
		cfg.DefaultConnectionLease = configDefault.DefaultConnectionLease
	}

	if cfg.ProviderReconnectInterval == 0 {
		cfg.ProviderReconnectInterval = configDefault.ProviderReconnectInterval
	}

	if cfg.ProviderMaxReconnectInterval == 0 {
		cfg.ProviderMaxReconnectInterval = configDefault.ProviderMaxReconnectInterval
	}

	// Provider health check options (use defaults if not set explicitly)
	if cfg.ProviderHealthCheckStagger == 0 {
		cfg.ProviderHealthCheckStagger = configDefault.ProviderHealthCheckStagger
	}

	if cfg.ProviderHealthCheckTimeout == 0 {
		cfg.ProviderHealthCheckTimeout = configDefault.ProviderHealthCheckTimeout
	}

	for i, p := range cfg.Providers {
		if p.MaxConnections == 0 {
			p.MaxConnections = providerConfigDefault.MaxConnections
		}

		if p.MaxConnectionIdleTimeInSeconds == 0 {
			p.MaxConnectionIdleTimeInSeconds = providerConfigDefault.MaxConnectionIdleTimeInSeconds
		}

		if p.MaxConnectionTTLInSeconds == 0 {
			p.MaxConnectionTTLInSeconds = providerConfigDefault.MaxConnectionTTLInSeconds
		}

		cfg.Providers[i] = p
	}

	switch cfg.DelayType {
	case DelayTypeFixed:
		cfg.delayTypeFn = retry.FixedDelay
	case DelayTypeRandom:
		cfg.delayTypeFn = retry.RandomDelay
	case DelayTypeExponential:
		cfg.delayTypeFn = retry.BackOffDelay
	default:
		cfg.delayTypeFn = retry.FixedDelay
	}

	return cfg
}
