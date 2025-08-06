package nntppool

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/javi11/nntpcli"
	"github.com/stretchr/testify/assert"
	"go.uber.org/mock/gomock"
)

func TestBody(t *testing.T) {
	t.Run("successfully downloads article body", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mockClient := nntpcli.NewMockClient(ctrl)
		logger := slog.New(slog.NewTextHandler(io.Discard, nil))
		mockConn := nntpcli.NewMockConnection(ctrl)

		mockConn.EXPECT().Close().AnyTimes()

		providers := []UsenetProviderConfig{
			{
				Host:                      "primary.example.com",
				Port:                      119,
				MaxConnections:            1,
				MaxConnectionTTLInSeconds: 2400,
			},
		}

		ttl := time.Duration(2400) * time.Second

		// Allow multiple Dial calls (verification and operation)
		mockClient.EXPECT().Dial(
			gomock.Any(),
			"primary.example.com",
			119,
			nntpcli.DialConfig{
				KeepAliveTime: ttl,
			},
		).Return(mockConn, nil).AnyTimes()

		// Allow capabilities call during verification
		mockConn.EXPECT().Capabilities().Return([]string{}, nil).AnyTimes()

		// Allow JoinGroup and BodyDecoded for the actual operation
		mockConn.EXPECT().JoinGroup("alt.test").Return(nil)
		mockConn.EXPECT().BodyDecoded("<test@example.com>", gomock.Any(), int64(0)).Return(int64(100), nil)

		// Create the pool
		pool, err := NewConnectionPool(Config{
			Providers:           providers,
			NntpCli:             mockClient,
			Logger:              logger,
			HealthCheckInterval: time.Hour,
			RetryDelay:          100 * time.Millisecond,
		})
		assert.NoError(t, err)
		defer pool.Quit()

		// Test the Body method
		bytes, err := pool.Body(context.Background(), "<test@example.com>", io.Discard, []string{"alt.test"})
		assert.NoError(t, err)
		assert.Equal(t, int64(100), bytes)
	})
}

func TestGetConnection(t *testing.T) {
	t.Run("successfully gets connection from primary provider", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mockClient := nntpcli.NewMockClient(ctrl)
		logger := slog.Default()
		mockConn := nntpcli.NewMockConnection(ctrl)

		mockConn.EXPECT().Close().AnyTimes()

		providers := []UsenetProviderConfig{
			{
				Host:                      "primary.example.com",
				Port:                      119,
				MaxConnections:            1,
				MaxConnectionTTLInSeconds: 2400,
			},
		}

		ttl := time.Duration(2400) * time.Second

		// Allow multiple Dial calls (verification and operation)
		mockClient.EXPECT().Dial(
			gomock.Any(),
			"primary.example.com",
			119,
			nntpcli.DialConfig{
				KeepAliveTime: ttl,
			},
		).Return(mockConn, nil).AnyTimes()

		// Allow capabilities call during verification
		mockConn.EXPECT().Capabilities().Return([]string{}, nil).AnyTimes()

		// Create the pool
		pool, err := NewConnectionPool(Config{
			Providers:           providers,
			NntpCli:             mockClient,
			Logger:              logger,
			HealthCheckInterval: time.Hour,
			RetryDelay:          100 * time.Millisecond,
		})
		assert.NoError(t, err)
		defer pool.Quit()

		// Test GetConnection
		conn, err := pool.GetConnection(context.Background(), []string{}, false)
		assert.NoError(t, err)
		assert.NotNil(t, conn)

		_ = conn.Free()
	})
}
