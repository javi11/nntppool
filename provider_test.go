package nntppool

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jackc/puddle/v2"
)

func TestProviderStateString(t *testing.T) {
	tests := []struct {
		state    ProviderState
		expected string
	}{
		{ProviderStateActive, "active"},
		{ProviderStateOffline, "offline"},
		{ProviderStateReconnecting, "reconnecting"},
		{ProviderStateAuthenticationFailed, "authentication_failed"},
		{ProviderState(999), "unknown"},
	}

	for _, tt := range tests {
		if got := tt.state.String(); got != tt.expected {
			t.Errorf("ProviderState(%d).String() = %q, want %q", tt.state, got, tt.expected)
		}
	}
}

func TestProviderPoolStateManagement(t *testing.T) {
	// Create a mock provider pool
	provider := UsenetProviderConfig{
		Host:           "test.example.com",
		Username:       "testuser",
		MaxConnections: 10,
	}

	pool := &providerPool{
		provider: provider,
		state:    ProviderStateActive,
	}

	// Test initial state
	if pool.GetState() != ProviderStateActive {
		t.Errorf("expected initial state to be Active, got %s", pool.GetState())
	}

	// Test state change
	pool.SetState(ProviderStateOffline)
	if pool.GetState() != ProviderStateOffline {
		t.Errorf("expected state to be Offline, got %s", pool.GetState())
	}

	// Test IsAcceptingConnections
	if pool.IsAcceptingConnections() {
		t.Error("offline provider should not accept connections")
	}

	pool.SetState(ProviderStateActive)
	if !pool.IsAcceptingConnections() {
		t.Error("active provider should accept connections")
	}

	// Test new offline states
	pool.SetState(ProviderStateOffline)
	if pool.IsAcceptingConnections() {
		t.Error("offline provider should not accept connections")
	}

	pool.SetState(ProviderStateReconnecting)
	if pool.IsAcceptingConnections() {
		t.Error("reconnecting provider should not accept connections")
	}

	pool.SetState(ProviderStateAuthenticationFailed)
	if pool.IsAcceptingConnections() {
		t.Error("authentication failed provider should not accept connections")
	}
}

func TestProviderPoolConcurrentStateAccess(t *testing.T) {
	pool := &providerPool{
		state: ProviderStateActive,
	}

	done := make(chan bool, 100)

	// Start multiple goroutines accessing state
	for i := 0; i < 50; i++ {
		go func(index int) {
			// Alternate between different operations
			switch index % 3 {
			case 0:
				pool.SetState(ProviderStateOffline)
			case 1:
				pool.GetState()
			case 2:
				pool.IsAcceptingConnections()
			}
			done <- true
		}(i)
	}

	// Start more goroutines reading state
	for i := 0; i < 50; i++ {
		go func(index int) {
			_ = pool.GetState()
			done <- true
		}(i)
	}

	// Wait for all goroutines to complete
	for i := 0; i < 100; i++ {
		<-done
	}

	// Just verify we can still access the state without panic
	_ = pool.GetState()
	_ = pool.IsAcceptingConnections()
}

func TestConnectionProviderInfoID(t *testing.T) {
	info := ConnectionProviderInfo{
		Host:     "test.example.com",
		Username: "testuser",
	}

	expectedID := "test.example.com_testuser"
	if id := info.ID(); id != expectedID {
		t.Errorf("expected ID %q, got %q", expectedID, id)
	}
}

func TestProviderInfoID(t *testing.T) {
	info := ProviderInfo{
		Host:     "test.example.com",
		Username: "testuser",
	}

	expectedID := "test.example.com_testuser"
	if id := info.ID(); id != expectedID {
		t.Errorf("expected ID %q, got %q", expectedID, id)
	}
}

func TestUsenetProviderConfigID(t *testing.T) {
	config := UsenetProviderConfig{
		Host:     "test.example.com",
		Username: "testuser",
	}

	expectedID := "test.example.com_testuser"
	if id := config.ID(); id != expectedID {
		t.Errorf("expected ID %q, got %q", expectedID, id)
	}
}

func TestProviderID(t *testing.T) {
	provider := Provider{
		Host: "test.example.com",
	}

	if id := provider.ID(); id != "test.example.com" {
		t.Errorf("expected ID %q, got %q", "test.example.com", id)
	}
}

func TestProviderIDFunction(t *testing.T) {
	tests := []struct {
		host     string
		username string
		expected string
	}{
		{"server1.example.com", "user1", "server1.example.com_user1"},
		{"server2.example.com", "user2", "server2.example.com_user2"},
		{"", "", "_"},
		{"host", "", "host_"},
		{"", "user", "_user"},
	}

	for _, tt := range tests {
		if got := providerID(tt.host, tt.username); got != tt.expected {
			t.Errorf("providerID(%q, %q) = %q, want %q", tt.host, tt.username, got, tt.expected)
		}
	}
}

func TestProviderPoolIntegration(t *testing.T) {
	// Create a provider pool with actual puddle pool for integration testing
	provider := UsenetProviderConfig{
		Host:           "test.example.com",
		Username:       "testuser",
		MaxConnections: 2,
	}

	// Create puddle pool with mock constructor
	puddlePool, err := puddle.NewPool(&puddle.Config[*internalConnection]{
		Constructor: func(ctx context.Context) (*internalConnection, error) {
			return &internalConnection{
				provider: provider,
			}, nil
		},
		Destructor: func(value *internalConnection) {
			// Mock destructor
		},
		MaxSize: int32(provider.MaxConnections),
	})

	if err != nil {
		t.Fatalf("failed to create puddle pool: %v", err)
	}

	pool := &providerPool{
		connectionPool: puddlePool,
		provider:       provider,
		state:          ProviderStateActive,
	}

	// Test that we can work with the actual puddle pool
	if !pool.IsAcceptingConnections() {
		t.Error("new provider pool should accept connections")
	}

	pool.SetState(ProviderStateOffline)
	if pool.IsAcceptingConnections() {
		t.Error("offline provider pool should not accept connections")
	}

	// Clean up
	puddlePool.Close()
}

// Test new provider connection tracking methods
func TestProviderConnectionTracking(t *testing.T) {
	provider := UsenetProviderConfig{
		Host:           "test.example.com",
		Username:       "testuser",
		MaxConnections: 5,
	}

	pool := &providerPool{
		provider: provider,
		state:    ProviderStateActive,
	}

	// Test initial state
	lastAttempt, lastSuccess, nextRetry, reason, retries := pool.GetConnectionStatus()
	if !lastAttempt.IsZero() {
		t.Error("initial last attempt should be zero")
	}
	if !lastSuccess.IsZero() {
		t.Error("initial last success should be zero")
	}
	if !nextRetry.IsZero() {
		t.Error("initial next retry should be zero")
	}
	if reason != "" {
		t.Error("initial failure reason should be empty")
	}
	if retries != 0 {
		t.Error("initial retry count should be 0")
	}

	// Test successful connection
	pool.SetConnectionAttempt(nil)
	lastAttempt, lastSuccess, _, reason, retries = pool.GetConnectionStatus()
	if lastAttempt.IsZero() {
		t.Error("last attempt should be set after successful connection")
	}
	if lastSuccess.IsZero() {
		t.Error("last success should be set after successful connection")
	}
	if reason != "" {
		t.Error("failure reason should be empty after successful connection")
	}
	if retries != 0 {
		t.Error("retry count should be 0 after successful connection")
	}

	// Test failed connection
	testErr := errors.New("test connection failure")
	pool.SetConnectionAttempt(testErr)
	_, _, _, reason, retries = pool.GetConnectionStatus()
	if reason != testErr.Error() {
		t.Errorf("expected failure reason %q, got %q", testErr.Error(), reason)
	}
	if retries != 1 {
		t.Errorf("expected retry count 1, got %d", retries)
	}
}

func TestProviderCanRetryLogic(t *testing.T) {
	provider := UsenetProviderConfig{
		Host:           "test.example.com",
		Username:       "testuser",
		MaxConnections: 5,
	}

	pool := &providerPool{
		provider: provider,
		state:    ProviderStateActive,
	}

	// Test states that can retry
	retryableStates := []ProviderState{
		ProviderStateActive,
		ProviderStateOffline,
		ProviderStateReconnecting,
	}

	for _, state := range retryableStates {
		pool.SetState(state)
		if !pool.CanRetry() {
			t.Errorf("state %s should allow retry", state)
		}
	}

	// Test state that cannot retry
	pool.SetState(ProviderStateAuthenticationFailed)
	if pool.CanRetry() {
		t.Error("authentication failed state should not allow retry")
	}
}

func TestProviderShouldRetryNowLogic(t *testing.T) {
	provider := UsenetProviderConfig{
		Host:           "test.example.com",
		Username:       "testuser",
		MaxConnections: 5,
	}

	pool := &providerPool{
		provider: provider,
		state:    ProviderStateOffline,
	}

	// Should retry when no next retry time is set
	if !pool.ShouldRetryNow() {
		t.Error("should retry when no next retry time is set")
	}

	// Should not retry when next retry time is in the future
	pool.SetNextRetryAt(time.Now().Add(1 * time.Hour))
	if pool.ShouldRetryNow() {
		t.Error("should not retry when next retry time is in the future")
	}

	// Should retry when next retry time is in the past
	pool.SetNextRetryAt(time.Now().Add(-1 * time.Hour))
	if !pool.ShouldRetryNow() {
		t.Error("should retry when next retry time is in the past")
	}
}

func TestProviderSetNextRetryAt(t *testing.T) {
	provider := UsenetProviderConfig{
		Host:           "test.example.com",
		Username:       "testuser",
		MaxConnections: 5,
	}

	pool := &providerPool{
		provider: provider,
		state:    ProviderStateOffline,
	}

	testTime := time.Now().Add(30 * time.Minute)
	pool.SetNextRetryAt(testTime)

	_, _, nextRetry, _, _ := pool.GetConnectionStatus()
	if !nextRetry.Equal(testTime) {
		t.Errorf("expected next retry time %v, got %v", testTime, nextRetry)
	}
}
