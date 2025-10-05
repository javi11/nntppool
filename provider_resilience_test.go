package nntppool

import (
	"errors"
	"io"
	"log/slog"
	"net/textproto"
	"testing"
	"time"

	"github.com/javi11/nntppool/pkg/nntpcli"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

func TestProviderStateManagement(t *testing.T) {
	tests := []struct {
		name          string
		initialState  ProviderState
		newState      ProviderState
		expectedState ProviderState
	}{
		{
			name:          "active to offline",
			initialState:  ProviderStateActive,
			newState:      ProviderStateOffline,
			expectedState: ProviderStateOffline,
		},
		{
			name:          "offline to reconnecting",
			initialState:  ProviderStateOffline,
			newState:      ProviderStateReconnecting,
			expectedState: ProviderStateReconnecting,
		},
		{
			name:          "reconnecting to active",
			initialState:  ProviderStateReconnecting,
			newState:      ProviderStateActive,
			expectedState: ProviderStateActive,
		},
		{
			name:          "offline to authentication failed",
			initialState:  ProviderStateOffline,
			newState:      ProviderStateAuthenticationFailed,
			expectedState: ProviderStateAuthenticationFailed,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			provider := UsenetProviderConfig{
				Host:           "test.example.com",
				Username:       "testuser",
				MaxConnections: 5,
			}

			pool := &providerPool{
				provider: provider,
				state:    tt.initialState,
			}

			pool.SetState(tt.newState)
			assert.Equal(t, tt.expectedState, pool.GetState())
		})
	}
}

func TestProviderConnectionStatusTracking(t *testing.T) {
	provider := UsenetProviderConfig{
		Host:           "test.example.com",
		Username:       "testuser",
		MaxConnections: 5,
	}

	pool := &providerPool{
		provider: provider,
		state:    ProviderStateActive,
	}

	// Test successful connection attempt
	pool.SetConnectionAttempt(nil)
	lastAttempt, lastSuccess, _, reason, retries := pool.GetConnectionStatus()

	assert.False(t, lastAttempt.IsZero(), "last attempt time should be set")
	assert.False(t, lastSuccess.IsZero(), "last success time should be set")
	assert.Empty(t, reason, "failure reason should be empty on success")
	assert.Equal(t, 0, retries, "retry count should be 0 on success")

	// Test failed connection attempt
	testErr := errors.New("connection failed")
	pool.SetConnectionAttempt(testErr)
	lastAttempt2, lastSuccess2, _, reason2, retries2 := pool.GetConnectionStatus()

	assert.True(t, lastAttempt2.After(lastAttempt), "last attempt should be updated")
	assert.Equal(t, lastSuccess, lastSuccess2, "last success should not change on failure")
	assert.Equal(t, testErr.Error(), reason2, "failure reason should be set")
	assert.Equal(t, 1, retries2, "retry count should increment")

	// Test multiple failures
	pool.SetConnectionAttempt(testErr)
	_, _, _, _, retries3 := pool.GetConnectionStatus()
	assert.Equal(t, 2, retries3, "retry count should increment again")
}

func TestProviderCanRetry(t *testing.T) {
	provider := UsenetProviderConfig{
		Host:           "test.example.com",
		Username:       "testuser",
		MaxConnections: 5,
	}

	pool := &providerPool{
		provider: provider,
		state:    ProviderStateActive,
	}

	// Test various states
	testCases := []struct {
		state    ProviderState
		canRetry bool
	}{
		{ProviderStateActive, true},
		{ProviderStateOffline, true},
		{ProviderStateReconnecting, true},
		{ProviderStateAuthenticationFailed, false},
	}

	for _, tc := range testCases {
		pool.SetState(tc.state)
		assert.Equal(t, tc.canRetry, pool.CanRetry(),
			"CanRetry() for state %s should return %v", tc.state, tc.canRetry)
	}
}

func TestProviderShouldRetryNow(t *testing.T) {
	provider := UsenetProviderConfig{
		Host:           "test.example.com",
		Username:       "testuser",
		MaxConnections: 5,
	}

	pool := &providerPool{
		provider: provider,
		state:    ProviderStateOffline,
	}

	// Test with no retry time set (should retry immediately)
	assert.True(t, pool.ShouldRetryNow(), "should retry when no retry time is set")

	// Test with future retry time
	pool.SetNextRetryAt(time.Now().Add(5 * time.Minute))
	assert.False(t, pool.ShouldRetryNow(), "should not retry when retry time is in the future")

	// Test with past retry time
	pool.SetNextRetryAt(time.Now().Add(-1 * time.Minute))
	assert.True(t, pool.ShouldRetryNow(), "should retry when retry time is in the past")
}

func TestProviderIsAcceptingConnections(t *testing.T) {
	provider := UsenetProviderConfig{
		Host:           "test.example.com",
		Username:       "testuser",
		MaxConnections: 5,
	}

	pool := &providerPool{
		provider: provider,
		state:    ProviderStateActive,
	}

	testCases := []struct {
		state             ProviderState
		acceptConnections bool
	}{
		{ProviderStateActive, true},
		{ProviderStateOffline, false},
		{ProviderStateReconnecting, false},
		{ProviderStateAuthenticationFailed, false},
	}

	for _, tc := range testCases {
		pool.SetState(tc.state)
		assert.Equal(t, tc.acceptConnections, pool.IsAcceptingConnections(),
			"IsAcceptingConnections() for state %s should return %v", tc.state, tc.acceptConnections)
	}
}

func TestNewProviderStates(t *testing.T) {
	tests := []struct {
		state    ProviderState
		expected string
	}{
		{ProviderStateActive, "active"},
		{ProviderStateOffline, "offline"},
		{ProviderStateReconnecting, "reconnecting"},
		{ProviderStateAuthenticationFailed, "authentication_failed"},
	}

	for _, tt := range tests {
		t.Run(tt.expected, func(t *testing.T) {
			assert.Equal(t, tt.expected, tt.state.String())
		})
	}
}

func TestPartialProviderInitialization(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	mockClient := nntpcli.NewMockClient(ctrl)

	// Mock a successful connection for one provider
	mockConn1 := nntpcli.NewMockConnection(ctrl)
	mockConn1.EXPECT().Close().AnyTimes()
	mockConn1.EXPECT().Capabilities().Return([]string{"READER"}, nil).AnyTimes()

	// Mock a failed connection for another provider
	mockClient.EXPECT().Dial(gomock.Any(), "working.example.com", 119, gomock.Any()).
		Return(mockConn1, nil).AnyTimes()
	mockClient.EXPECT().Dial(gomock.Any(), "broken.example.com", 119, gomock.Any()).
		Return(nil, errors.New("connection refused")).AnyTimes()

	mockConn1.EXPECT().Authenticate("user1", "pass1").Return(nil).AnyTimes()

	// Test with AllowPartialProviderInitialization = true
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
	require.NoError(t, err, "pool creation should succeed with partial initialization")
	require.NotNil(t, pool, "pool should not be nil")

	// Check provider states
	providers := pool.GetProvidersInfo()
	require.Len(t, providers, 2, "should have 2 providers")

	var workingProvider, brokenProvider *ProviderInfo
	for i := range providers {
		switch providers[i].Host {
		case "working.example.com":
			workingProvider = &providers[i]
		case "broken.example.com":
			brokenProvider = &providers[i]
		}
	}

	require.NotNil(t, workingProvider, "working provider should be found")
	require.NotNil(t, brokenProvider, "broken provider should be found")

	assert.Equal(t, ProviderStateActive, workingProvider.State, "working provider should be active")
	assert.Equal(t, ProviderStateOffline, brokenProvider.State, "broken provider should be offline")

	pool.Quit()
}

func TestProviderAuthenticationFailureHandling(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	mockClient := nntpcli.NewMockClient(ctrl)
	mockConn := nntpcli.NewMockConnection(ctrl)

	// Mock connection that fails authentication
	mockClient.EXPECT().Dial(gomock.Any(), "auth-fail.example.com", 119, gomock.Any()).
		Return(mockConn, nil).AnyTimes()
	mockConn.EXPECT().Authenticate("user", "wrongpass").
		Return(&textproto.Error{Code: AuthenticationFailedCode, Msg: "Authentication failed"}).AnyTimes()
	mockConn.EXPECT().Close().AnyTimes()

	config := Config{
		Logger:  logger,
		NntpCli: mockClient,
		Providers: []UsenetProviderConfig{
			{
				Host:           "auth-fail.example.com",
				Port:           119,
				Username:       "user",
				Password:       "wrongpass",
				MaxConnections: 1,
			},
		},
	}

	pool, err := NewConnectionPool(config)
	require.NoError(t, err, "pool creation should succeed")
	require.NotNil(t, pool, "pool should not be nil")

	// Check that provider is marked as authentication failed
	providers := pool.GetProvidersInfo()
	require.Len(t, providers, 1, "should have 1 provider")

	provider := providers[0]
	assert.Equal(t, ProviderStateAuthenticationFailed, provider.State, "provider should be marked as authentication failed")
	assert.False(t, provider.FailureReason == "", "failure reason should be set")

	pool.Quit()
}

func TestExponentialBackoffCalculation(t *testing.T) {
	config := Config{
		ProviderReconnectInterval:    30 * time.Second,
		ProviderMaxReconnectInterval: 5 * time.Minute,
	}

	pool := &connectionPool{
		config: config,
	}

	tests := []struct {
		retryCount  int
		expectedMin time.Duration
		expectedMax time.Duration
		description string
	}{
		{0, 30 * time.Second, 30 * time.Second, "first retry"},
		{1, 60 * time.Second, 60 * time.Second, "second retry"},
		{2, 120 * time.Second, 120 * time.Second, "third retry"},
		{3, 240 * time.Second, 240 * time.Second, "fourth retry"},
		{10, 5 * time.Minute, 5 * time.Minute, "many retries (capped)"},
	}

	for _, tt := range tests {
		t.Run(tt.description, func(t *testing.T) {
			delay := pool.calculateBackoffDelay(tt.retryCount)
			assert.GreaterOrEqual(t, delay, tt.expectedMin, "delay should be at least expected minimum")
			assert.LessOrEqual(t, delay, tt.expectedMax, "delay should not exceed expected maximum")
		})
	}
}
