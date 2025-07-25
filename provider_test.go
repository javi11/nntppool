package nntppool

import (
	"context"
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
		{ProviderStateDraining, "draining"},
		{ProviderStateMigrating, "migrating"},
		{ProviderStateRemoving, "removing"},
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
	pool.SetState(ProviderStateDraining)
	if pool.GetState() != ProviderStateDraining {
		t.Errorf("expected state to be Draining, got %s", pool.GetState())
	}

	// Test IsAcceptingConnections
	if pool.IsAcceptingConnections() {
		t.Error("draining provider should not accept connections")
	}

	pool.SetState(ProviderStateActive)
	if !pool.IsAcceptingConnections() {
		t.Error("active provider should accept connections")
	}

	pool.SetState(ProviderStateMigrating)
	if !pool.IsAcceptingConnections() {
		t.Error("migrating provider should accept connections")
	}

	pool.SetState(ProviderStateRemoving)
	if pool.IsAcceptingConnections() {
		t.Error("removing provider should not accept connections")
	}
}

func TestProviderPoolDrainTracking(t *testing.T) {
	pool := &providerPool{
		state: ProviderStateActive,
	}

	// Initially no drain duration
	if duration := pool.GetDrainDuration(); duration != 0 {
		t.Errorf("expected drain duration 0, got %v", duration)
	}

	// Set to draining
	beforeDrain := time.Now()
	pool.SetState(ProviderStateDraining)
	afterDrain := time.Now()

	// Check drain duration is tracked
	duration := pool.GetDrainDuration()
	if duration < 0 {
		t.Error("drain duration should not be negative")
	}
	// Allow some tolerance for timing precision (1ms)
	maxExpected := afterDrain.Sub(beforeDrain) + time.Millisecond
	if duration > maxExpected {
		t.Errorf("drain duration %v should not exceed elapsed time %v (with 1ms tolerance)", duration, maxExpected)
	}

	// Setting to non-draining state should not reset drain time
	pool.SetState(ProviderStateActive)
	if pool.GetDrainDuration() != 0 {
		t.Error("drain duration should be 0 when not draining")
	}

	// Setting back to draining should start a new drain timer
	pool.SetState(ProviderStateDraining)
	newDuration := pool.GetDrainDuration()
	if newDuration >= duration {
		t.Error("new drain should have started fresh timer")
	}
}

func TestProviderPoolMigrationID(t *testing.T) {
	pool := &providerPool{}

	// Initially no migration ID
	if id := pool.GetMigrationID(); id != "" {
		t.Errorf("expected empty migration ID, got %q", id)
	}

	// Set migration ID
	testID := "migration_123456"
	pool.SetMigrationID(testID)
	if id := pool.GetMigrationID(); id != testID {
		t.Errorf("expected migration ID %q, got %q", testID, id)
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
			switch index % 4 {
			case 0:
				pool.SetState(ProviderStateDraining)
			case 1:
				pool.GetState()
			case 2:
				pool.IsAcceptingConnections()
			case 3:
				pool.GetDrainDuration()
			}
			done <- true
		}(i)
	}

	// Start more goroutines setting migration ID
	for i := 0; i < 50; i++ {
		go func(index int) {
			pool.SetMigrationID("migration_" + string(rune(index)))
			pool.GetMigrationID()
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
	_ = pool.GetDrainDuration()
	_ = pool.GetMigrationID()
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
				provider:             provider,
				leaseExpiry:          time.Now().Add(10 * time.Minute),
				markedForReplacement: false,
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

	pool.SetState(ProviderStateDraining)
	if pool.IsAcceptingConnections() {
		t.Error("draining provider pool should not accept connections")
	}

	// Clean up
	puddlePool.Close()
}
