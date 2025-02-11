package connectionpool

import (
	"context"
	"errors"
	"fmt"
	"net"
	"slices"
	"time"

	"github.com/hashicorp/go-multierror"
	"github.com/jackc/puddle/v2"
	"github.com/javi11/nntpcli"
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
) ([]providerPool, error) {
	pSlice := make([]providerPool, 0)

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

					return &internalConnection{
						nntp:     nntpCon,
						provider: provider,
					}, nil
				},
				Destructor: func(value *internalConnection) {
					err := value.nntp.Close()
					if err != nil {
						log.Debug(fmt.Sprintf("error closing connection: %v", err))
					}
				},
				MaxSize: int32(provider.MaxConnections),
			},
		)
		if err != nil {
			return nil, err
		}

		pSlice = append(pSlice, providerPool{
			connectionPool: pools,
			provider:       provider,
		})
	}

	return pSlice, nil
}

func verifyProviders(pools []providerPool, log Logger) error {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	wg := multierror.Group{}

	for _, pool := range pools {
		wg.Go(func() error {
			c, err := pool.connectionPool.Acquire(ctx)
			if err != nil {
				return fmt.Errorf("failed to verify download provider %s: %w", pool.provider.Host, err)
			}

			defer c.Release()

			conn := c.Value()
			caps, _ := conn.nntp.Capabilities()

			log.Info(fmt.Sprintf("capabilities for provider %s: %v", pool.provider.Host, caps))

			if len(pool.provider.VerifyCapabilities) > 0 {
				for _, cap := range pool.provider.VerifyCapabilities {
					if !slices.Contains(caps, cap) {
						return fmt.Errorf("provider %s does not support capability %s", pool.provider.Host, cap)
					}
				}
			}

			return nil
		})
	}

	return wg.Wait().ErrorOrNil()
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

	log.Debug(fmt.Sprintf("connecting to %s:%v", p.Host, p.Port))

	ttl := time.Duration(p.MaxConnectionTTLInSeconds) * time.Second

	if p.TLS {
		c, err = cli.DialTLS(
			ctx,
			p.Host,
			p.Port,
			p.InsecureSSL,
			nntpcli.DialConfig{
				KeepAliveTime: ttl,
			},
		)
		if err != nil {
			var netErr net.Error

			if errors.As(err, &netErr) && netErr.Timeout() {
				log.Error(fmt.Sprintf("timeout connecting to %s:%v, retrying", p.Host, p.Port), "error", netErr)
			}

			return nil, fmt.Errorf("error dialing to %v/%v TLS: %w", p.Host, p.Username, err)
		}
	} else {
		var err error

		c, err = cli.Dial(
			ctx,
			p.Host,
			p.Port,
			nntpcli.DialConfig{
				KeepAliveTime: ttl,
			},
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
