package nntppool

import (
	"io"
	"log/slog"
	"testing"

	"github.com/javi11/nntppool/pkg/nntpcli"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

func TestGetProviderStatus(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	mockClient := nntpcli.NewMockClient(ctrl)
	mockConn := nntpcli.NewMockConnection(ctrl)

	// Setup mock expectations
	mockClient.EXPECT().Dial(gomock.Any(), "test.example.com", 119, gomock.Any()).
		Return(mockConn, nil).AnyTimes()
	mockConn.EXPECT().Authenticate("testuser", "testpass").Return(nil).AnyTimes()
	mockConn.EXPECT().Capabilities().Return([]string{"READER"}, nil).AnyTimes()
	mockConn.EXPECT().Close().AnyTimes()

	config := Config{
		Logger:  logger,
		NntpCli: mockClient,
		Providers: []UsenetProviderConfig{
			{
				Host:           "test.example.com",
				Port:           119,
				Username:       "testuser",
				Password:       "testpass",
				MaxConnections: 5,
			},
		},
		SkipProvidersVerificationOnCreation: true,
	}

	pool, err := NewConnectionPool(config)
	require.NoError(t, err)
	require.NotNil(t, pool)
	defer pool.Quit()

	providerID := "test.example.com_testuser"

	// Test getting status for existing provider
	status, exists := pool.GetProviderStatus(providerID)
	assert.True(t, exists, "provider should exist")
	require.NotNil(t, status, "status should not be nil")

	assert.Equal(t, "test.example.com", status.Host)
	assert.Equal(t, "testuser", status.Username)
	assert.Equal(t, 5, status.MaxConnections)
	assert.Equal(t, ProviderStateActive, status.State)

	// Test getting status for non-existing provider
	status, exists = pool.GetProviderStatus("nonexistent_provider")
	assert.False(t, exists, "non-existent provider should not exist")
	assert.Nil(t, status, "status should be nil for non-existent provider")
}

func TestGetProviderStatusWithShutdownPool(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	mockClient := nntpcli.NewMockClient(ctrl)
	mockConn := nntpcli.NewMockConnection(ctrl)

	mockClient.EXPECT().Dial(gomock.Any(), "test.example.com", 119, gomock.Any()).
		Return(mockConn, nil).AnyTimes()
	mockConn.EXPECT().Authenticate("testuser", "testpass").Return(nil).AnyTimes()
	mockConn.EXPECT().Capabilities().Return([]string{"READER"}, nil).AnyTimes()
	mockConn.EXPECT().Close().AnyTimes()

	config := Config{
		Logger:  logger,
		NntpCli: mockClient,
		Providers: []UsenetProviderConfig{
			{
				Host:           "test.example.com",
				Port:           119,
				Username:       "testuser",
				Password:       "testpass",
				MaxConnections: 5,
			},
		},
		SkipProvidersVerificationOnCreation: true,
	}

	pool, err := NewConnectionPool(config)
	require.NoError(t, err)
	require.NotNil(t, pool)

	// Shutdown the pool
	pool.Quit()

	// Test getting status after shutdown
	status, exists := pool.GetProviderStatus("test.example.com_testuser")
	assert.False(t, exists, "should not find provider after shutdown")
	assert.Nil(t, status, "status should be nil after shutdown")
}
