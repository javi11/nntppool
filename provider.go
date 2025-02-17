package nntppool

import (
	"fmt"
	"time"

	"github.com/jackc/puddle/v2"
)

type ConnectionProviderInfo struct {
	Host                     string        `json:"host"`
	Username                 string        `json:"username"`
	MaxConnections           int           `json:"maxConnections"`
	MaxConnectionIdleTimeout time.Duration `json:"maxConnectionIdleTimeout"`
}

func (p ConnectionProviderInfo) ID() string {
	return providerID(p.Host, p.Username)
}

type ProviderInfo struct {
	Host                     string        `json:"host"`
	Username                 string        `json:"username"`
	UsedConnections          int           `json:"usedConnections"`
	MaxConnections           int           `json:"maxConnections"`
	MaxConnectionIdleTimeout time.Duration `json:"maxConnectionIdleTimeout"`
}

func (p ProviderInfo) ID() string {
	return providerID(p.Host, p.Username)
}

type providerPool struct {
	connectionPool *puddle.Pool[*internalConnection]
	provider       UsenetProviderConfig
}

func providerID(host, username string) string {
	return fmt.Sprintf("%s_%s", host, username)
}

type Provider struct {
	Host                           string
	Username                       string
	Password                       string
	Port                           int
	MaxConnections                 int
	MaxConnectionIdleTimeInSeconds int
	IsBackupProvider               bool
}

func (p *Provider) ID() string {
	return p.Host
}
