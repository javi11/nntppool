package nntppool

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/textproto"
	"sync"
	"testing"
	"time"

	"github.com/javi11/nntpcli"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

func TestProviderReconnectionSystemIntegration(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	mockClient := nntpcli.NewMockClient(ctrl)

	// Create a mock connection that will succeed after failures
	var attemptCount int
	var mu sync.Mutex

	mockClient.EXPECT().Dial(gomock.Any(), "flaky.example.com", 119, gomock.Any()).
		DoAndReturn(func(ctx context.Context, host string, port int, config nntpcli.DialConfig) (nntpcli.Connection, error) {
			mu.Lock()
			defer mu.Unlock()
			attemptCount++

			// Fail first 3 attempts, then succeed
			if attemptCount <= 3 {
				return nil, errors.New("connection refused")
			}

			// Return successful connection
			mockConn := nntpcli.NewMockConnection(ctrl)
			mockConn.EXPECT().Authenticate("testuser", "testpass").Return(nil).AnyTimes()
			mockConn.EXPECT().Capabilities().Return([]string{"READER"}, nil).AnyTimes()
			mockConn.EXPECT().Close().AnyTimes()
			return mockConn, nil
		}).AnyTimes()

	config := Config{
		Logger:  logger,
		NntpCli: mockClient,
		Providers: []UsenetProviderConfig{
			{
				Host:           "flaky.example.com",
				Port:           119,
				Username:       "testuser",
				Password:       "testpass",
				MaxConnections: 1,
			},
		},
		ProviderReconnectInterval:    100 * time.Millisecond, // Fast reconnection for testing
		ProviderMaxReconnectInterval: 500 * time.Millisecond,
	}

	pool, err := NewConnectionPool(config)
	require.NoError(t, err)
	require.NotNil(t, pool)
	defer pool.Quit()

	providerID := "flaky.example.com_testuser"

	// Initially, provider should be offline due to connection failure
	status, exists := pool.GetProviderStatus(providerID)
	require.True(t, exists, "provider should exist")
	assert.Equal(t, ProviderStateOffline, status.State, "provider should initially be offline")

	// Wait for reconnection system to work (should eventually succeed)
	var finalStatus *ProviderInfo
	for i := 0; i < 50; i++ { // Wait up to 5 seconds
		time.Sleep(100 * time.Millisecond)
		status, exists := pool.GetProviderStatus(providerID)
		require.True(t, exists, "provider should exist")

		if status.State == ProviderStateActive {
			finalStatus = status
			break
		}
	}

	require.NotNil(t, finalStatus, "provider should eventually become active")
	assert.Equal(t, ProviderStateActive, finalStatus.State, "provider should be active after reconnection")
	assert.Greater(t, finalStatus.RetryCount, 0, "should have recorded retry attempts")
	assert.False(t, finalStatus.LastSuccessfulConnect.IsZero(), "should have recorded successful connection")
}

func TestProviderAuthenticationFailurePermanent(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	mockClient := nntpcli.NewMockClient(ctrl)

	// Mock connection that always fails authentication
	mockConn := nntpcli.NewMockConnection(ctrl)
	mockClient.EXPECT().Dial(gomock.Any(), "auth-fail.example.com", 119, gomock.Any()).
		Return(mockConn, nil).AnyTimes()
	mockConn.EXPECT().Authenticate("testuser", "wrongpass").
		Return(&textproto.Error{Code: AuthenticationFailedCode, Msg: "Authentication failed"}).AnyTimes()
	mockConn.EXPECT().Close().AnyTimes()

	config := Config{
		Logger:  logger,
		NntpCli: mockClient,
		Providers: []UsenetProviderConfig{
			{
				Host:           "auth-fail.example.com",
				Port:           119,
				Username:       "testuser",
				Password:       "wrongpass",
				MaxConnections: 1,
			},
		},
		ProviderReconnectInterval: 50 * time.Millisecond, // Fast reconnection for testing
	}

	pool, err := NewConnectionPool(config)
	require.NoError(t, err)
	require.NotNil(t, pool)
	defer pool.Quit()

	providerID := "auth-fail.example.com_testuser"

	// Provider should be marked as authentication failed
	status, exists := pool.GetProviderStatus(providerID)
	require.True(t, exists, "provider should exist")
	assert.Equal(t, ProviderStateAuthenticationFailed, status.State, "provider should be marked as authentication failed")

	// Wait and ensure it stays authentication failed (doesn't retry)
	time.Sleep(200 * time.Millisecond)
	status, exists = pool.GetProviderStatus(providerID)
	require.True(t, exists, "provider should exist")
	assert.Equal(t, ProviderStateAuthenticationFailed, status.State, "provider should remain authentication failed")
	// Get the actual provider pool to check CanRetry
	providers := pool.GetProvidersInfo()
	for _, provider := range providers {
		if provider.Host == "auth-fail.example.com" {
			assert.Equal(t, ProviderStateAuthenticationFailed, provider.State, "authentication failed provider should not be retryable")
			break
		}
	}
}

func TestProviderReconnectionBackoff(t *testing.T) {
	config := Config{
		ProviderReconnectInterval:    100 * time.Millisecond,
		ProviderMaxReconnectInterval: 800 * time.Millisecond,
	}

	pool := &connectionPool{config: config}

	// Test exponential backoff calculation
	tests := []struct {
		retryCount int
		expected   time.Duration
	}{
		{0, 100 * time.Millisecond},  // Base interval
		{1, 200 * time.Millisecond},  // 2x
		{2, 400 * time.Millisecond},  // 4x
		{3, 800 * time.Millisecond},  // 8x, but capped at max
		{4, 800 * time.Millisecond},  // Still capped at max
		{10, 800 * time.Millisecond}, // Still capped at max
	}

	for _, tt := range tests {
		actual := pool.calculateBackoffDelay(tt.retryCount)
		assert.Equal(t, tt.expected, actual, "retry count %d should produce delay %v", tt.retryCount, tt.expected)
	}
}

func TestProviderReconnectionSystemShutdown(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	mockClient := nntpcli.NewMockClient(ctrl)

	// Mock failing connection
	mockClient.EXPECT().Dial(gomock.Any(), "failing.example.com", 119, gomock.Any()).
		Return(nil, errors.New("connection refused")).AnyTimes()

	config := Config{
		Logger:  logger,
		NntpCli: mockClient,
		Providers: []UsenetProviderConfig{
			{
				Host:           "failing.example.com",
				Port:           119,
				Username:       "testuser",
				Password:       "testpass",
				MaxConnections: 1,
			},
		},
		ProviderReconnectInterval: 50 * time.Millisecond,
		ShutdownTimeout:           100 * time.Millisecond,
	}

	pool, err := NewConnectionPool(config)
	require.NoError(t, err)
	require.NotNil(t, pool)

	// Let it run for a bit
	time.Sleep(100 * time.Millisecond)

	// Shutdown should complete without hanging
	startTime := time.Now()
	pool.Quit()
	elapsed := time.Since(startTime)

	// Should shutdown reasonably quickly
	assert.Less(t, elapsed, 500*time.Millisecond, "shutdown should not hang")
}

func TestProviderInfoWithConnectivityStatus(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	mockClient := nntpcli.NewMockClient(ctrl)
	mockConn := nntpcli.NewMockConnection(ctrl)

	// Mock successful connection - verification will call these during pool creation
	mockClient.EXPECT().Dial(gomock.Any(), "working.example.com", 119, gomock.Any()).
		Return(mockConn, nil).AnyTimes()
	mockConn.EXPECT().Authenticate("testuser", "testpass").Return(nil).AnyTimes()
	mockConn.EXPECT().Capabilities().Return([]string{"READER"}, nil).AnyTimes()
	mockConn.EXPECT().Close().AnyTimes()

	config := Config{
		Logger:  logger,
		NntpCli: mockClient,
		Providers: []UsenetProviderConfig{
			{
				Host:           "working.example.com",
				Port:           119,
				Username:       "testuser",
				Password:       "testpass",
				MaxConnections: 5,
			},
		},
	}

	pool, err := NewConnectionPool(config)
	require.NoError(t, err)
	require.NotNil(t, pool)
	defer pool.Quit()

	// Test GetProvidersInfo includes new connectivity fields
	providers := pool.GetProvidersInfo()
	require.Len(t, providers, 1, "should have one provider")

	provider := providers[0]
	assert.Equal(t, "working.example.com", provider.Host)
	assert.Equal(t, "testuser", provider.Username)
	assert.Equal(t, 5, provider.MaxConnections)
	assert.Equal(t, ProviderStateActive, provider.State)

	// These should be set during verification
	assert.False(t, provider.LastConnectionAttempt.IsZero(), "should have last connection attempt")
	assert.False(t, provider.LastSuccessfulConnect.IsZero(), "should have last successful connect")
	assert.Empty(t, provider.FailureReason, "should have no failure reason for successful connection")
	assert.Equal(t, 0, provider.RetryCount, "should have 0 retry count for successful connection")
}

func TestProviderStateTransitions(t *testing.T) {
	provider := UsenetProviderConfig{
		Host:           "test.example.com",
		Username:       "testuser",
		MaxConnections: 5,
	}

	pool := &providerPool{
		provider: provider,
		state:    ProviderStateActive,
	}

	// Test typical failure and recovery cycle
	transitions := []struct {
		newState    ProviderState
		description string
	}{
		{ProviderStateOffline, "provider goes offline"},
		{ProviderStateReconnecting, "provider starts reconnecting"},
		{ProviderStateActive, "provider comes back online"},
		{ProviderStateOffline, "provider fails again"},
		{ProviderStateReconnecting, "provider tries to reconnect"},
		{ProviderStateAuthenticationFailed, "provider fails authentication"},
	}

	for _, transition := range transitions {
		pool.SetState(transition.newState)
		assert.Equal(t, transition.newState, pool.GetState(), transition.description)

		// Test IsAcceptingConnections for each state
		expectedAccepting := transition.newState == ProviderStateActive || transition.newState == ProviderStateMigrating
		assert.Equal(t, expectedAccepting, pool.IsAcceptingConnections(),
			"IsAcceptingConnections should be %v for state %s", expectedAccepting, transition.newState)
	}
}

func TestGetConnectionWithOfflineProviders(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	mockClient := nntpcli.NewMockClient(ctrl)

	// Mock one working and one failing provider
	workingConn := nntpcli.NewMockConnection(ctrl)
	mockClient.EXPECT().Dial(gomock.Any(), "working.example.com", 119, gomock.Any()).
		Return(workingConn, nil).AnyTimes()
	workingConn.EXPECT().Authenticate("user1", "pass1").Return(nil).AnyTimes()
	workingConn.EXPECT().Capabilities().Return([]string{"READER"}, nil).AnyTimes()
	workingConn.EXPECT().Close().AnyTimes()

	// Failing provider
	mockClient.EXPECT().Dial(gomock.Any(), "broken.example.com", 119, gomock.Any()).
		Return(nil, errors.New("connection refused")).AnyTimes()

	config := Config{
		Logger:  logger,
		NntpCli: mockClient,
		Providers: []UsenetProviderConfig{
			{
				Host:           "working.example.com",
				Port:           119,
				Username:       "user1",
				Password:       "pass1",
				MaxConnections: 1,
			},
			{
				Host:           "broken.example.com",
				Port:           119,
				Username:       "user2",
				Password:       "pass2",
				MaxConnections: 1,
			},
		},
	}

	pool, err := NewConnectionPool(config)
	require.NoError(t, err)
	require.NotNil(t, pool)
	defer pool.Quit()

	// Should be able to get connection from working provider only
	ctx := context.Background()
	conn, err := pool.GetConnection(ctx, []string{}, false)
	require.NoError(t, err, "should get connection from working provider")
	require.NotNil(t, conn, "connection should not be nil")

	// Verify it's from the working provider
	assert.Equal(t, "working.example.com", conn.Provider().Host)

	func() {
		_ = conn.Close()
	}()
}

func TestConnectionsDroppedWhenProviderGoesOffline(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	mockClient := nntpcli.NewMockClient(ctrl)

	// Create mock connections that will be dropped
	var connCount int
	var createdConnections []*nntpcli.MockConnection
	var mu sync.Mutex

	mockClient.EXPECT().Dial(gomock.Any(), "test.example.com", 119, gomock.Any()).
		DoAndReturn(func(ctx context.Context, host string, port int, config nntpcli.DialConfig) (nntpcli.Connection, error) {
			mu.Lock()
			defer mu.Unlock()
			connCount++

			mockConn := nntpcli.NewMockConnection(ctrl)
			mockConn.EXPECT().Authenticate("testuser", "testpass").Return(nil).AnyTimes()
			mockConn.EXPECT().Capabilities().Return([]string{"READER"}, nil).AnyTimes()
			mockConn.EXPECT().Close().AnyTimes()
			mockConn.EXPECT().MaxAgeTime().Return(time.Now().Add(time.Hour)).AnyTimes()

			createdConnections = append(createdConnections, mockConn)
			return mockConn, nil
		}).AnyTimes()

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
		MinConnections:               0, // Don't create initial connections automatically
		ProviderReconnectInterval:    100 * time.Millisecond,
		ProviderMaxReconnectInterval: 5 * time.Second,
		ProviderHealthCheckTimeout:   1 * time.Second,
		ProviderHealthCheckStagger:   50 * time.Millisecond,
	}

	pool, err := NewConnectionPool(config)
	require.NoError(t, err)
	require.NotNil(t, pool)
	defer pool.Quit()

	// Cast to internal type to access internal methods
	cp := pool.(*connectionPool)

	// Get the provider pool
	require.Len(t, cp.connPools, 1)
	providerPool := cp.connPools[0]

	// Verify provider starts as active
	assert.Equal(t, ProviderStateActive, providerPool.GetState())

	// Manually create and release some connections to make them idle
	ctx := context.Background()
	var testConnections []PooledConnection
	for i := 0; i < 3; i++ {
		conn, err := pool.GetConnection(ctx, []string{}, false)
		require.NoError(t, err)
		testConnections = append(testConnections, conn)
	}

	// Release all connections to make them idle
	for _, conn := range testConnections {
		err := conn.Free()
		require.NoError(t, err)
	}

	// Wait a moment for connections to be released and become idle
	time.Sleep(50 * time.Millisecond)

	// Check that we have idle connections
	initialTotal := int(providerPool.connectionPool.Stat().TotalResources())
	initialIdle := len(providerPool.connectionPool.AcquireAllIdle())

	// Release the acquired idle connections back (since AcquireAllIdle acquires them)
	idle := providerPool.connectionPool.AcquireAllIdle()
	for _, res := range idle {
		res.ReleaseUnused()
	}

	assert.Greater(t, initialTotal, 0, "should have some total connections")
	assert.Greater(t, initialIdle, 0, "should have some idle connections")

	// Simulate provider going offline by calling handleReconnectionFailure
	testError := errors.New("connection failed")
	cp.handleReconnectionFailure(providerPool, testError)

	// Verify provider is marked as offline
	assert.Equal(t, ProviderStateOffline, providerPool.GetState())

	// Wait a moment for any async destruction
	time.Sleep(100 * time.Millisecond)

	// Check that idle connections were dropped
	remainingIdle := len(providerPool.connectionPool.AcquireAllIdle())
	assert.Less(t, remainingIdle, initialIdle, "idle connections should be dropped when provider goes offline")

	// Release any remaining idle connections we acquired
	remaining := providerPool.connectionPool.AcquireAllIdle()
	for _, res := range remaining {
		res.ReleaseUnused()
	}

	// Try to get connection from offline provider (should fail)
	_, err = pool.GetConnection(ctx, []string{}, false)
	assert.Error(t, err, "should fail to get connection from offline provider")
}

func TestConnectionsDroppedWhenProviderFailsHealthCheck(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	mockClient := nntpcli.NewMockClient(ctrl)

	// Track connection creation
	var connCount int
	var mu sync.Mutex

	mockClient.EXPECT().Dial(gomock.Any(), "test.example.com", 119, gomock.Any()).
		DoAndReturn(func(ctx context.Context, host string, port int, config nntpcli.DialConfig) (nntpcli.Connection, error) {
			mu.Lock()
			defer mu.Unlock()
			connCount++

			mockConn := nntpcli.NewMockConnection(ctrl)
			mockConn.EXPECT().Authenticate("testuser", "testpass").Return(nil).AnyTimes()
			mockConn.EXPECT().Capabilities().Return([]string{"READER"}, nil).AnyTimes()
			mockConn.EXPECT().Close().AnyTimes()
			mockConn.EXPECT().MaxAgeTime().Return(time.Now().Add(time.Hour)).AnyTimes()

			return mockConn, nil
		}).AnyTimes()

	config := Config{
		Logger:  logger,
		NntpCli: mockClient,
		Providers: []UsenetProviderConfig{
			{
				Host:           "test.example.com",
				Port:           119,
				Username:       "testuser",
				Password:       "testpass",
				MaxConnections: 3,
			},
		},
		MinConnections:               0, // Don't create initial connections automatically
		ProviderReconnectInterval:    100 * time.Millisecond,
		ProviderMaxReconnectInterval: 5 * time.Second,
		ProviderHealthCheckTimeout:   1 * time.Second,
		ProviderHealthCheckStagger:   50 * time.Millisecond,
	}

	pool, err := NewConnectionPool(config)
	require.NoError(t, err)
	require.NotNil(t, pool)
	defer pool.Quit()

	// Cast to internal type
	cp := pool.(*connectionPool)

	// Get the provider pool
	require.Len(t, cp.connPools, 1)
	providerPool := cp.connPools[0]

	// Verify provider starts as active
	assert.Equal(t, ProviderStateActive, providerPool.GetState())

	// Manually create and release some connections to make them idle
	ctx := context.Background()
	var testConnections []PooledConnection
	for i := 0; i < 2; i++ {
		conn, err := pool.GetConnection(ctx, []string{}, false)
		require.NoError(t, err)
		testConnections = append(testConnections, conn)
	}

	// Release all connections to make them idle
	for _, conn := range testConnections {
		err := conn.Free()
		require.NoError(t, err)
	}

	// Wait a moment for connections to be released and become idle
	time.Sleep(50 * time.Millisecond)

	// Check that we have idle connections
	initialIdle := len(providerPool.connectionPool.AcquireAllIdle())

	// Release the acquired idle connections back
	idle := providerPool.connectionPool.AcquireAllIdle()
	for _, res := range idle {
		res.ReleaseUnused()
	}

	assert.Greater(t, initialIdle, 0, "should have some idle connections")

	// Simulate health check failure
	testError := errors.New("health check failed")
	cp.handleProviderHealthCheckFailure(providerPool, testError, ProviderStateActive)

	// Verify provider is marked as offline
	assert.Equal(t, ProviderStateOffline, providerPool.GetState())

	// Wait a moment for any async destruction
	time.Sleep(100 * time.Millisecond)

	// Check that idle connections were dropped
	remainingIdle := len(providerPool.connectionPool.AcquireAllIdle())
	assert.Less(t, remainingIdle, initialIdle, "idle connections should be dropped when provider fails health check")

	// Release any remaining idle connections we acquired
	remaining := providerPool.connectionPool.AcquireAllIdle()
	for _, res := range remaining {
		res.ReleaseUnused()
	}

	// Try to get connection from offline provider (should fail)
	_, err = pool.GetConnection(ctx, []string{}, false)
	assert.Error(t, err, "should fail to get connection from offline provider")
}
