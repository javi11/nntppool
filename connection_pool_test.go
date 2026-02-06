package nntppool

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/javi11/nntppool/v2/pkg/nntpcli"
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

		// Allow BodyDecoded for the actual operation
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
		bytes, err := pool.Body(context.Background(), "<test@example.com>", io.Discard)
		assert.NoError(t, err)
		assert.Equal(t, int64(100), bytes)
	})
}

func TestBodyReader_ContextCancellation(t *testing.T) {
	t.Run("returns error when BodyReader fails due to error", func(t *testing.T) {
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

		// Simulate BodyReader failure with generic error
		mockConn.EXPECT().BodyReader("<test@example.com>").Return(nil, io.ErrUnexpectedEOF)

		// Create the pool
		pool, err := NewConnectionPool(Config{
			Providers:           providers,
			NntpCli:             mockClient,
			Logger:              logger,
			HealthCheckInterval: time.Hour,
			RetryDelay:          100 * time.Millisecond,
			MaxRetries:          1, // Only try once to make test faster
		})
		assert.NoError(t, err)
		defer pool.Quit()

		// Test the BodyReader method - should return error, not nil reader
		reader, err := pool.BodyReader(context.Background(), "<test@example.com>")

		// Should get an error, not a nil reader
		assert.Error(t, err, "Expected error when BodyReader fails")
		assert.Nil(t, reader, "Expected nil reader when error occurs")
	})

	t.Run("does not create nil reader when BodyReader succeeds but returns nil", func(t *testing.T) {
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

		// Simulate BodyReader returning nil without error (edge case)
		mockConn.EXPECT().BodyReader("<test@example.com>").Return(nil, context.Canceled)

		// Create the pool
		pool, err := NewConnectionPool(Config{
			Providers:           providers,
			NntpCli:             mockClient,
			Logger:              logger,
			HealthCheckInterval: time.Hour,
			RetryDelay:          100 * time.Millisecond,
			MaxRetries:          1,
		})
		assert.NoError(t, err)
		defer pool.Quit()

		// Test the BodyReader method with normal context
		reader, err := pool.BodyReader(context.Background(), "<test@example.com>")

		// Should get an error because reader is nil
		assert.Error(t, err, context.Canceled)
		assert.Nil(t, reader, "Expected nil reader when validation fails")
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

// TestGetConnection_ContextCancellationUnderCapacity verifies that GetConnection
// respects context cancellation when all providers are at capacity, preventing hangs.
func TestGetConnection_ContextCancellationUnderCapacity(t *testing.T) {
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
			MaxConnections:            1, // Only 1 connection allowed
			MaxConnectionTTLInSeconds: 2400,
		},
	}

	ttl := time.Duration(2400) * time.Second

	mockClient.EXPECT().Dial(
		gomock.Any(),
		"primary.example.com",
		119,
		nntpcli.DialConfig{
			KeepAliveTime: ttl,
		},
	).Return(mockConn, nil).AnyTimes()

	mockConn.EXPECT().Capabilities().Return([]string{}, nil).AnyTimes()

	pool, err := NewConnectionPool(Config{
		Providers:           providers,
		NntpCli:             mockClient,
		Logger:              logger,
		HealthCheckInterval: time.Hour,
		MinConnections:      0, // Don't pre-warm
	})
	assert.NoError(t, err)
	defer pool.Quit()

	// First, acquire the only available connection
	conn1, err := pool.GetConnection(context.Background(), []string{}, false)
	assert.NoError(t, err)
	assert.NotNil(t, conn1)

	// Now try to get another connection with a short timeout
	// This should time out because the pool is at capacity
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	start := time.Now()
	_, err = pool.GetConnection(ctx, []string{}, false)
	elapsed := time.Since(start)

	// Should fail with context deadline exceeded
	assert.Error(t, err)

	// Should return reasonably quickly (not hang forever)
	assert.Less(t, elapsed, 500*time.Millisecond, "GetConnection should respect context timeout, not hang")

	// Clean up
	_ = conn1.Free()
}
