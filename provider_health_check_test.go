package nntppool

import (
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/javi11/nntpcli"
	"github.com/stretchr/testify/assert"
	"go.uber.org/mock/gomock"
)

func TestProviderHealthCheckConfiguration(t *testing.T) {
	t.Run("default configuration values", func(t *testing.T) {
		config := mergeWithDefault()

		assert.Equal(t, 10*time.Second, config.ProviderHealthCheckStagger)
		assert.Equal(t, 5*time.Second, config.ProviderHealthCheckTimeout)
	})

	t.Run("custom configuration values", func(t *testing.T) {
		config := mergeWithDefault(Config{
			ProviderHealthCheckStagger: 15 * time.Second,
			ProviderHealthCheckTimeout: 3 * time.Second,
		})

		assert.Equal(t, 15*time.Second, config.ProviderHealthCheckStagger)
		assert.Equal(t, 3*time.Second, config.ProviderHealthCheckTimeout)
	})

	t.Run("zero values get defaults", func(t *testing.T) {
		config := mergeWithDefault(Config{
			ProviderHealthCheckStagger: 0,
			ProviderHealthCheckTimeout: 0,
		})

		assert.Equal(t, 10*time.Second, config.ProviderHealthCheckStagger)
		assert.Equal(t, 5*time.Second, config.ProviderHealthCheckTimeout)
	})
}

func TestProviderHealthCheckStateFiltering(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := nntpcli.NewMockClient(ctrl)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	mockConn := nntpcli.NewMockConnection(ctrl)

	mockConn.EXPECT().Close().AnyTimes()
	mockConn.EXPECT().MaxAgeTime().Return(time.Now().Add(time.Hour)).AnyTimes()

	providers := []UsenetProviderConfig{
		{Host: "active.example.com", Port: 119, MaxConnections: 1, MaxConnectionTTLInSeconds: 2400},
		{Host: "offline.example.com", Port: 119, MaxConnections: 1, MaxConnectionTTLInSeconds: 2400},
		{Host: "reconnecting.example.com", Port: 119, MaxConnections: 1, MaxConnectionTTLInSeconds: 2400},
		{Host: "authfailed.example.com", Port: 119, MaxConnections: 1, MaxConnectionTTLInSeconds: 2400},
	}

	mockClient.EXPECT().Dial(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(mockConn, nil).AnyTimes()
	mockConn.EXPECT().Capabilities().Return([]string{}, nil).AnyTimes()

	pool, err := NewConnectionPool(Config{
		Providers:                  providers,
		NntpCli:                    mockClient,
		Logger:                     logger,
		HealthCheckInterval:        100 * time.Millisecond,
		ProviderHealthCheckStagger: 10 * time.Millisecond,
	})
	assert.NoError(t, err)

	// Cast to access internal methods
	cp := pool.(*connectionPool)

	// Set provider states manually for testing
	cp.connPools[0].SetState(ProviderStateActive)               // Should be checked
	cp.connPools[1].SetState(ProviderStateOffline)              // Should NOT be checked
	cp.connPools[2].SetState(ProviderStateReconnecting)         // Should NOT be checked
	cp.connPools[3].SetState(ProviderStateAuthenticationFailed) // Should NOT be checked

	// Wait for initial stagger time to pass
	time.Sleep(15 * time.Millisecond)

	// Test multiple cycles to verify filtering behavior
	foundProviders := make(map[string]bool)

	// Check several cycles to verify the behavior is consistent
	for i := 0; i < 3; i++ { // Reduced iterations to avoid timeout
		providersToCheck := cp.getProvidersToHealthCheck()

		for _, provider := range providersToCheck {
			foundProviders[provider.provider.Host] = true
		}

		// Wait for next stagger interval before next check
		if i < 2 { // Don't sleep after the last iteration
			time.Sleep(15 * time.Millisecond)
		}
	}

	// Cleanup pool before assertions to avoid timeout on defer
	pool.Quit()

	// Only active providers should be selected for health checks
	assert.True(t, foundProviders["active.example.com"], "Active provider should be selected for health check")

	// These providers should NEVER be selected (they're handled by reconnection worker)
	assert.False(t, foundProviders["offline.example.com"], "Offline provider should not be selected for health check")
	assert.False(t, foundProviders["reconnecting.example.com"], "Reconnecting provider should not be selected for health check")
	assert.False(t, foundProviders["authfailed.example.com"], "Auth failed provider should not be selected for health check")
}

func TestProviderHealthCheckResetsRetrySchedule(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := nntpcli.NewMockClient(ctrl)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	mockConn := nntpcli.NewMockConnection(ctrl)

	mockConn.EXPECT().Close().AnyTimes()
	mockConn.EXPECT().MaxAgeTime().Return(time.Now().Add(time.Hour)).AnyTimes()

	providers := []UsenetProviderConfig{
		{Host: "test.example.com", Port: 119, MaxConnections: 1, MaxConnectionTTLInSeconds: 2400},
	}

	mockClient.EXPECT().Dial(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(mockConn, nil).AnyTimes()
	mockConn.EXPECT().Capabilities().Return([]string{}, nil).AnyTimes()

	pool, err := NewConnectionPool(Config{
		Providers:                  providers,
		NntpCli:                    mockClient,
		Logger:                     logger,
		HealthCheckInterval:        100 * time.Millisecond,
		ProviderHealthCheckStagger: 10 * time.Millisecond,
	})
	assert.NoError(t, err)

	// Cast to access internal methods
	cp := pool.(*connectionPool)
	provider := cp.connPools[0]

	testCases := []struct {
		name         string
		initialState ProviderState
	}{
		{"offline provider becomes active", ProviderStateOffline},
		{"reconnecting provider becomes active", ProviderStateReconnecting},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Set initial state and schedule a retry
			provider.SetState(tc.initialState)
			futureTime := time.Now().Add(30 * time.Minute)
			provider.SetNextRetryAt(futureTime)

			// Verify retry is scheduled
			_, _, nextRetry, _, _ := provider.GetConnectionStatus()
			assert.Equal(t, futureTime, nextRetry, "nextRetryAt should be set to future time")
			assert.False(t, provider.ShouldRetryNow(), "should not retry when nextRetryAt is in future")

			// Simulate successful health check
			cp.handleProviderHealthCheckSuccess(provider, tc.initialState)

			// Verify state is now active
			assert.Equal(t, ProviderStateActive, provider.GetState())

			// Verify nextRetryAt is reset (zero time means immediate retry)
			_, _, nextRetry, _, _ = provider.GetConnectionStatus()
			assert.True(t, nextRetry.IsZero(), "nextRetryAt should be reset to zero time")
			assert.True(t, provider.ShouldRetryNow(), "should be able to retry immediately after reset")
		})
	}

	pool.Quit()
}

func TestProviderStateChangeResetsRetrySchedule(t *testing.T) {
	provider := UsenetProviderConfig{
		Host:           "test.example.com",
		Username:       "testuser",
		MaxConnections: 5,
	}

	pool := &providerPool{
		provider: provider,
		state:    ProviderStateActive,
	}

	// Set a future retry time
	futureTime := time.Now().Add(30 * time.Minute)
	pool.SetNextRetryAt(futureTime)

	// Verify retry is scheduled
	_, _, nextRetry, _, _ := pool.GetConnectionStatus()
	assert.Equal(t, futureTime, nextRetry, "nextRetryAt should be set to future time")
	assert.False(t, pool.ShouldRetryNow(), "should not retry when nextRetryAt is in future")

	// Test various state changes
	testCases := []struct {
		name     string
		newState ProviderState
	}{
		{"Active to Offline", ProviderStateOffline},
		{"Offline to Reconnecting", ProviderStateReconnecting},
		{"Reconnecting to Active", ProviderStateActive},
		{"Active to AuthenticationFailed", ProviderStateAuthenticationFailed},
		{"AuthenticationFailed to Offline", ProviderStateOffline},
		{"Offline to Draining", ProviderStateDraining},
		{"Draining to Migrating", ProviderStateMigrating},
		{"Migrating to Removing", ProviderStateRemoving},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Set a future retry time before state change
			futureTime := time.Now().Add(30 * time.Minute)
			pool.SetNextRetryAt(futureTime)

			// Verify retry is scheduled
			_, _, nextRetry, _, _ := pool.GetConnectionStatus()
			assert.Equal(t, futureTime, nextRetry, "nextRetryAt should be set to future time before state change")
			assert.False(t, pool.ShouldRetryNow(), "should not retry before state change")

			// Change state
			pool.SetState(tc.newState)

			// Verify nextRetryAt is reset (zero time means immediate retry)
			_, _, nextRetry, _, _ = pool.GetConnectionStatus()
			assert.True(t, nextRetry.IsZero(), "nextRetryAt should be reset to zero time after state change")
			assert.True(t, pool.ShouldRetryNow(), "should be able to retry immediately after state change")

			// Verify state actually changed
			assert.Equal(t, tc.newState, pool.GetState(), "state should have changed")
		})
	}
}

func TestProviderStateNoChangeDoesNotResetRetry(t *testing.T) {
	provider := UsenetProviderConfig{
		Host:           "test.example.com",
		Username:       "testuser",
		MaxConnections: 5,
	}

	pool := &providerPool{
		provider: provider,
		state:    ProviderStateActive,
	}

	// Set a future retry time
	futureTime := time.Now().Add(30 * time.Minute)
	pool.SetNextRetryAt(futureTime)

	// Verify retry is scheduled
	_, _, nextRetry, _, _ := pool.GetConnectionStatus()
	assert.Equal(t, futureTime, nextRetry, "nextRetryAt should be set to future time")

	// Set the same state (no change)
	pool.SetState(ProviderStateActive)

	// Verify nextRetryAt is NOT reset when state doesn't change
	_, _, nextRetry, _, _ = pool.GetConnectionStatus()
	assert.Equal(t, futureTime, nextRetry, "nextRetryAt should remain unchanged when state doesn't change")
	assert.False(t, pool.ShouldRetryNow(), "should still not retry when state doesn't change")
}

func TestProviderHealthCheckSuccessHandling(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := nntpcli.NewMockClient(ctrl)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	mockConn := nntpcli.NewMockConnection(ctrl)

	mockConn.EXPECT().Close().AnyTimes()
	mockConn.EXPECT().MaxAgeTime().Return(time.Now().Add(time.Hour)).AnyTimes()

	providers := []UsenetProviderConfig{
		{Host: "test.example.com", Port: 119, MaxConnections: 1, MaxConnectionTTLInSeconds: 2400},
	}

	mockClient.EXPECT().Dial(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(mockConn, nil).AnyTimes()
	mockConn.EXPECT().Capabilities().Return([]string{}, nil).AnyTimes()

	pool, err := NewConnectionPool(Config{
		Providers:                  providers,
		NntpCli:                    mockClient,
		Logger:                     logger,
		HealthCheckInterval:        100 * time.Millisecond,
		ProviderHealthCheckStagger: 10 * time.Millisecond,
	})
	assert.NoError(t, err)
	defer pool.Quit()

	// Cast to access internal methods
	cp := pool.(*connectionPool)

	testCases := []struct {
		name               string
		initialState       ProviderState
		expectedFinalState ProviderState
		shouldClearError   bool
	}{
		{
			name:               "offline provider becomes active",
			initialState:       ProviderStateOffline,
			expectedFinalState: ProviderStateActive,
			shouldClearError:   true,
		},
		{
			name:               "reconnecting provider becomes active",
			initialState:       ProviderStateReconnecting,
			expectedFinalState: ProviderStateActive,
			shouldClearError:   true,
		},
		{
			name:               "active provider stays active",
			initialState:       ProviderStateActive,
			expectedFinalState: ProviderStateActive,
			shouldClearError:   true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			provider := cp.connPools[0]

			// Set initial state
			provider.SetState(tc.initialState)
			provider.SetConnectionAttempt(assert.AnError) // Set an error initially

			// Simulate successful health check
			cp.handleProviderHealthCheckSuccess(provider, tc.initialState)

			// Verify final state
			assert.Equal(t, tc.expectedFinalState, provider.GetState())

			// Check if error was cleared
			if tc.shouldClearError {
				_, _, _, reason, _ := provider.GetConnectionStatus()
				assert.Empty(t, reason, "Error reason should be cleared after successful health check")
			}
		})
	}
}

func TestProviderHealthCheckFailureHandling(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := nntpcli.NewMockClient(ctrl)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	mockConn := nntpcli.NewMockConnection(ctrl)

	mockConn.EXPECT().Close().AnyTimes()
	mockConn.EXPECT().MaxAgeTime().Return(time.Now().Add(time.Hour)).AnyTimes()

	providers := []UsenetProviderConfig{
		{Host: "test.example.com", Port: 119, MaxConnections: 1, MaxConnectionTTLInSeconds: 2400},
	}

	mockClient.EXPECT().Dial(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(mockConn, nil).AnyTimes()
	mockConn.EXPECT().Capabilities().Return([]string{}, nil).AnyTimes()

	pool, err := NewConnectionPool(Config{
		Providers:                  providers,
		NntpCli:                    mockClient,
		Logger:                     logger,
		HealthCheckInterval:        100 * time.Millisecond,
		ProviderHealthCheckStagger: 10 * time.Millisecond,
	})
	assert.NoError(t, err)
	defer pool.Quit()

	// Cast to access internal methods
	cp := pool.(*connectionPool)

	testCases := []struct {
		name               string
		initialState       ProviderState
		errorType          error
		expectedFinalState ProviderState
	}{
		{
			name:               "active provider becomes offline on failure",
			initialState:       ProviderStateActive,
			errorType:          assert.AnError,
			expectedFinalState: ProviderStateOffline,
		},
		{
			name:               "offline provider stays offline on failure",
			initialState:       ProviderStateOffline,
			errorType:          assert.AnError,
			expectedFinalState: ProviderStateOffline,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			provider := cp.connPools[0]

			// Set initial state
			provider.SetState(tc.initialState)

			// Simulate failed health check
			cp.handleProviderHealthCheckFailure(provider, tc.errorType, tc.initialState)

			// Verify final state
			assert.Equal(t, tc.expectedFinalState, provider.GetState())

			// Check if error was recorded
			_, _, _, reason, _ := provider.GetConnectionStatus()
			assert.NotEmpty(t, reason, "Error reason should be recorded after failed health check")
		})
	}
}

func TestProviderHealthCheckStaggering(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := nntpcli.NewMockClient(ctrl)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	mockConn := nntpcli.NewMockConnection(ctrl)

	mockConn.EXPECT().Close().AnyTimes()
	mockConn.EXPECT().MaxAgeTime().Return(time.Now().Add(time.Hour)).AnyTimes()

	providers := []UsenetProviderConfig{
		{Host: "test1.example.com", Port: 119, MaxConnections: 1, MaxConnectionTTLInSeconds: 2400},
		{Host: "test2.example.com", Port: 119, MaxConnections: 1, MaxConnectionTTLInSeconds: 2400},
	}

	mockClient.EXPECT().Dial(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(mockConn, nil).AnyTimes()
	mockConn.EXPECT().Capabilities().Return([]string{}, nil).AnyTimes()

	staggerInterval := 50 * time.Millisecond
	pool, err := NewConnectionPool(Config{
		Providers:                  providers,
		NntpCli:                    mockClient,
		Logger:                     logger,
		HealthCheckInterval:        200 * time.Millisecond,
		ProviderHealthCheckStagger: staggerInterval,
	})
	assert.NoError(t, err)
	defer pool.Quit()

	// Cast to access internal methods
	cp := pool.(*connectionPool)

	// First call should work
	time.Sleep(staggerInterval + 5*time.Millisecond)
	providers1 := cp.getProvidersToHealthCheck()
	assert.NotEmpty(t, providers1, "Should return providers after stagger interval")

	// Immediate second call should return empty (within stagger interval)
	providers2 := cp.getProvidersToHealthCheck()
	assert.Empty(t, providers2, "Should return empty providers within stagger interval")

	// After stagger interval, should return providers again
	time.Sleep(staggerInterval + 5*time.Millisecond)
	providers3 := cp.getProvidersToHealthCheck()
	assert.NotEmpty(t, providers3, "Should return providers after next stagger interval")
}

func TestProviderHealthCheckConcurrency(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := nntpcli.NewMockClient(ctrl)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	mockConn := nntpcli.NewMockConnection(ctrl)

	mockConn.EXPECT().Close().AnyTimes()
	mockConn.EXPECT().MaxAgeTime().Return(time.Now().Add(time.Hour)).AnyTimes()

	providers := []UsenetProviderConfig{
		{Host: "test1.example.com", Port: 119, MaxConnections: 1, MaxConnectionTTLInSeconds: 2400},
		{Host: "test2.example.com", Port: 119, MaxConnections: 1, MaxConnectionTTLInSeconds: 2400},
	}

	mockClient.EXPECT().Dial(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(mockConn, nil).AnyTimes()
	mockConn.EXPECT().Capabilities().Return([]string{}, nil).AnyTimes()

	pool, err := NewConnectionPool(Config{
		Providers:                  providers,
		NntpCli:                    mockClient,
		Logger:                     logger,
		HealthCheckInterval:        50 * time.Millisecond,
		ProviderHealthCheckStagger: 10 * time.Millisecond,
	})
	assert.NoError(t, err)
	defer pool.Quit()

	// Cast to access internal methods
	cp := pool.(*connectionPool)

	// Test concurrent access to getProvidersToHealthCheck
	var wg sync.WaitGroup
	results := make(chan []*providerPool, 10)

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			time.Sleep(15 * time.Millisecond) // Wait past stagger
			providers := cp.getProvidersToHealthCheck()
			results <- providers
		}()
	}

	go func() {
		wg.Wait()
		close(results)
	}()

	// Collect results
	nonEmptyResults := 0
	for providers := range results {
		if len(providers) > 0 {
			nonEmptyResults++
		}
	}

	// Due to staggering, not all calls should return providers
	assert.Greater(t, nonEmptyResults, 0, "At least some calls should return providers")
	assert.Less(t, nonEmptyResults, 10, "Not all calls should return providers due to staggering")
}

func TestProviderHealthCheckExpiredConnectionRetry(t *testing.T) {
	// Test the helper function that detects expired connection errors
	t.Run("isConnectionExpiredError detects retryable errors", func(t *testing.T) {
		testCases := []struct {
			name     string
			err      error
			expected bool
		}{
			{"nil error", nil, false},
			{"io.EOF", io.EOF, true},
			{"generic error", assert.AnError, false},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				result := isConnectionExpiredError(tc.err)
				assert.Equal(t, tc.expected, result)
			})
		}
	})

	// Note: The actual retry logic in performLightweightProviderCheck is complex to test
	// due to connection pool interactions. The logic has been implemented and tested
	// manually. The key improvement is that expired connections are now detected
	// and retried up to 2 times before marking a provider as unhealthy.
}

