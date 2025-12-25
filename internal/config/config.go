package config

// ProviderConfig represents a provider configuration that can be compared
type ProviderConfig interface {
	ID() string
	GetHost() string
	GetUsername() string
	GetPassword() string
	GetPort() int
	GetMaxConnections() int
	GetMaxConnectionIdleTimeInSeconds() int
	GetMaxConnectionTTLInSeconds() int
	GetPipelineDepth() int
	GetTLS() bool
	GetInsecureSSL() bool
	GetIsBackupProvider() bool
}

// Config represents a configuration with providers
type Config interface {
	GetProviders() []ProviderConfig
}
