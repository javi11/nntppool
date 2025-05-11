package nntppool

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/avast/retry-go"
	"github.com/javi11/nntpcli"
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
	Logger                              Logger
	NntpCli                             nntpcli.Client
	Providers                           []UsenetProviderConfig
	HealthCheckInterval                 time.Duration
	MinConnections                      int
	MaxRetries                          uint
	DelayType                           DelayType
	SkipProvidersVerificationOnCreation bool
	delayTypeFn                         retry.DelayTypeFunc
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

type Option func(*Config)

var (
	configDefault = Config{
		HealthCheckInterval: 1 * time.Minute,
		MinConnections:      5,
		MaxRetries:          4,
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
