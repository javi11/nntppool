package nntpcli

import (
	"time"
)

type Config struct {
	// KeepAliveTime is the time that the client will keep the connection alive.
	KeepAliveTime time.Duration
	// OperationTimeout is the timeout for NNTP operations.
	// Set to 0 to disable timeouts.
	OperationTimeout time.Duration
}

type Option func(*Config)

var configDefault = Config{
	KeepAliveTime:    10 * time.Minute,
	OperationTimeout: 30 * time.Second,
}

func mergeWithDefault(config ...Config) Config {
	if len(config) == 0 {
		return configDefault
	}

	cfg := config[0]

	if cfg.KeepAliveTime == 0 {
		cfg.KeepAliveTime = configDefault.KeepAliveTime
	}

	if cfg.OperationTimeout == 0 {
		cfg.OperationTimeout = configDefault.OperationTimeout
	}

	return cfg
}
