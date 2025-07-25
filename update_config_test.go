package nntppool

import (
	"log/slog"
	"testing"
	"time"

	"github.com/javi11/nntpcli"
)

func TestNewReconfigurationBehavior(t *testing.T) {
	// Test the new incremental Reconfigure behavior
	
	// Create initial configuration
	initialConfig := Config{
		Logger: slog.Default(),
		NntpCli: nntpcli.New(),
		Providers: []UsenetProviderConfig{
			{
				Host:           "primary.example.com",
				Username:       "user1",
				Password:       "pass1",
				Port:           563,
				MaxConnections: 10,
				TLS:            true,
			},
		},
		SkipProvidersVerificationOnCreation: true,
		DefaultConnectionLease:              5 * time.Minute,
	}

	pool, err := NewConnectionPool(initialConfig)
	if err != nil {
		t.Fatalf("failed to create connection pool: %v", err)
	}
	defer pool.Quit()

	// Verify initial state
	providers := pool.GetProvidersInfo()
	if len(providers) != 1 {
		t.Fatalf("expected 1 provider, got %d", len(providers))
	}
	if providers[0].Host != "primary.example.com" {
		t.Errorf("expected host primary.example.com, got %s", providers[0].Host)
	}

	// Test Reconfigure with no changes
	err = pool.Reconfigure(initialConfig)
	if err != nil {
		t.Errorf("Reconfigure with no changes should not fail: %v", err)
	}

	// Verify no active reconfigurations
	reconfigurations := pool.GetActiveReconfigurations()
	if len(reconfigurations) > 0 {
		t.Error("should not have active reconfigurations for no-change reload")
	}

	// Test Reconfigure with changes
	newConfig := initialConfig
	newConfig.Providers = []UsenetProviderConfig{
		{
			Host:           "primary.example.com",
			Username:       "user1",
			Password:       "pass1",
			Port:           563,
			MaxConnections: 15, // Changed from 10 to 15
			TLS:            true,
		},
		{
			Host:           "secondary.example.com", // New provider
			Username:       "user2",
			Password:       "pass2",
			Port:           563,
			MaxConnections: 8,
			TLS:            true,
		},
	}

	err = pool.Reconfigure(newConfig)
	if err != nil {
		t.Errorf("Reconfigure with changes should not fail immediately: %v", err)
	}

	// Verify migration was started
	reconfigurations = pool.GetActiveReconfigurations()
	if len(reconfigurations) != 1 {
		t.Errorf("expected 1 active migration, got %d", len(reconfigurations))
	}

	// Get the migration status
	var migrationID string
	for id := range reconfigurations {
		migrationID = id
		break
	}

	status, exists := pool.GetReconfigurationStatus(migrationID)
	if !exists {
		t.Error("migration status should exist")
	}

	// Migration should be either running or might have been cancelled due to test shutdown
	if status.Status != "running" && status.Status != "failed" {
		t.Errorf("expected migration status 'running' or 'failed', got %s", status.Status)
	}

	// Verify that the migration includes the expected changes
	if len(status.Changes) == 0 {
		t.Error("migration should have changes")
	}

	// Since change types are now internal, we verify the reconfiguration
	// by checking that we have the expected number of changes (2: update + add)
	if len(status.Changes) != 2 {
		t.Errorf("expected 2 changes (update + add), got %d", len(status.Changes))
	}
}

func TestReconfigureWithShutdownPool(t *testing.T) {
	config := Config{
		Logger: slog.Default(),
		NntpCli: nntpcli.New(),
		Providers: []UsenetProviderConfig{
			{
				Host:           "test.example.com",
				Username:       "user1",
				MaxConnections: 5,
			},
		},
		SkipProvidersVerificationOnCreation: true,
	}

	pool, err := NewConnectionPool(config)
	if err != nil {
		t.Fatalf("failed to create connection pool: %v", err)
	}

	// Shutdown the pool
	pool.Quit()

	// Try to reload after shutdown
	err = pool.Reconfigure(config)
	if err == nil {
		t.Error("Reconfigure should fail on shutdown pool")
	}
	if err.Error() != "connection pool is shutdown" {
		t.Errorf("expected shutdown error, got: %v", err)
	}
}

func TestReconfigureEmptyProviders(t *testing.T) {
	config := Config{
		Logger: slog.Default(),
		NntpCli: nntpcli.New(),
		Providers: []UsenetProviderConfig{
			{
				Host:           "test.example.com",
				Username:       "user1",
				MaxConnections: 5,
			},
		},
		SkipProvidersVerificationOnCreation: true,
	}

	pool, err := NewConnectionPool(config)
	if err != nil {
		t.Fatalf("failed to create connection pool: %v", err)
	}
	defer pool.Quit()

	// Try to reload with empty providers
	emptyConfig := config
	emptyConfig.Providers = []UsenetProviderConfig{}

	err = pool.Reconfigure(emptyConfig)
	if err == nil {
		t.Error("Reconfigure should fail with empty providers")
	}
	if err != ErrNoProviderAvailable {
		t.Errorf("expected ErrNoProviderAvailable, got: %v", err)
	}
}

func TestReconfigureMultipleConcurrentAttempts(t *testing.T) {
	config := Config{
		Logger: slog.Default(),
		NntpCli: nntpcli.New(),
		Providers: []UsenetProviderConfig{
			{
				Host:           "test.example.com",
				Username:       "user1",
				MaxConnections: 5,
			},
		},
		SkipProvidersVerificationOnCreation: true,
	}

	pool, err := NewConnectionPool(config)
	if err != nil {
		t.Fatalf("failed to create connection pool: %v", err)
	}
	defer pool.Quit()

	// Prepare different configurations with actual changes
	config1 := Config{
		Logger: slog.Default(),
		NntpCli: nntpcli.New(),
		Providers: []UsenetProviderConfig{
			{
				Host:           "test.example.com",
				Username:       "user1",
				MaxConnections: 10, // Changed from 5 to 10
			},
		},
		SkipProvidersVerificationOnCreation: true,
	}

	config2 := Config{
		Logger: slog.Default(),
		NntpCli: nntpcli.New(),
		Providers: []UsenetProviderConfig{
			{
				Host:           "test.example.com",
				Username:       "user1",
				MaxConnections: 15, // Changed from 5 to 15
			},
		},
		SkipProvidersVerificationOnCreation: true,
	}

	// Start first migration
	err1 := pool.Reconfigure(config1)
	if err1 != nil {
		t.Errorf("first Reconfigure should succeed: %v", err1)
	}

	// Verify first migration was started (should have changes)
	reconfigurations := pool.GetActiveReconfigurations()
	if len(reconfigurations) == 0 {
		t.Error("should have started first migration with changes")
	}

	// Try second migration immediately
	err2 := pool.Reconfigure(config2)
	
	// One of these might fail if there's already a migration in progress
	// The exact behavior depends on the migration manager implementation
	if err1 == nil && err2 != nil {
		// This is acceptable - second migration rejected because first is in progress
		t.Logf("Second migration rejected as expected: %v", err2)
	}

	// Verify there's at least one migration (from the first change)
	reconfigurations = pool.GetActiveReconfigurations()
	if len(reconfigurations) == 0 {
		t.Error("should have at least one active migration")
	}
}

func TestReconfigureAsynchronousNature(t *testing.T) {
	// Test that Reconfigure returns immediately and doesn't block
	config := Config{
		Logger: slog.Default(),
		NntpCli: nntpcli.New(),
		Providers: []UsenetProviderConfig{
			{
				Host:           "test.example.com",
				Username:       "user1",
				MaxConnections: 5,
			},
		},
		SkipProvidersVerificationOnCreation: true,
	}

	pool, err := NewConnectionPool(config)
	if err != nil {
		t.Fatalf("failed to create connection pool: %v", err)
	}
	defer pool.Quit()

	// Create config with actual changes
	newConfig := Config{
		Logger: slog.Default(),
		NntpCli: nntpcli.New(),
		Providers: []UsenetProviderConfig{
			{
				Host:           "test.example.com",
				Username:       "user1",
				MaxConnections: 10, // Changed from 5 to 10
			},
		},
		SkipProvidersVerificationOnCreation: true,
	}

	// Measure time for Reconfigure to return
	start := time.Now()
	err = pool.Reconfigure(newConfig)
	duration := time.Since(start)

	if err != nil {
		t.Errorf("Reconfigure should not fail: %v", err)
	}

	// Reconfigure should return quickly (within 1 second) since it's async
	if duration > time.Second {
		t.Errorf("Reconfigure took too long: %v (should be < 1s for async operation)", duration)
	}

	// Verify migration was started (since there are changes)
	reconfigurations := pool.GetActiveReconfigurations()
	if len(reconfigurations) == 0 {
		t.Error("migration should have been started with configuration changes")
	}
}

func TestMigrationStatusTracking(t *testing.T) {
	config := Config{
		Logger: slog.Default(),
		NntpCli: nntpcli.New(),
		Providers: []UsenetProviderConfig{
			{
				Host:           "test.example.com",
				Username:       "user1",
				MaxConnections: 5,
			},
		},
		SkipProvidersVerificationOnCreation: true,
	}

	pool, err := NewConnectionPool(config)
	if err != nil {
		t.Fatalf("failed to create connection pool: %v", err)
	}
	defer pool.Quit()

	// Start migration with actual changes
	newConfig := Config{
		Logger: slog.Default(),
		NntpCli: nntpcli.New(),
		Providers: []UsenetProviderConfig{
			{
				Host:           "test.example.com",
				Username:       "user1",
				MaxConnections: 10, // Changed from 5 to 10
			},
		},
		SkipProvidersVerificationOnCreation: true,
	}

	err = pool.Reconfigure(newConfig)
	if err != nil {
		t.Fatalf("Reconfigure failed: %v", err)
	}

	// Get migration ID
	reconfigurations := pool.GetActiveReconfigurations()
	if len(reconfigurations) == 0 {
		t.Fatal("no active reconfigurations found - Reconfigure should have created one with changes")
	}

	var migrationID string
	for id := range reconfigurations {
		migrationID = id
		break
	}

	// Test status retrieval
	status, exists := pool.GetReconfigurationStatus(migrationID)
	if !exists {
		t.Error("migration status should exist")
	}

	// Verify status fields
	if status.ID != migrationID {
		t.Errorf("expected migration ID %s, got %s", migrationID, status.ID)
	}

	if status.StartTime.IsZero() {
		t.Error("migration should have start time")
	}

	if len(status.Changes) == 0 {
		t.Error("migration should have changes")
	}

	if len(status.Progress) == 0 {
		t.Error("migration should have progress tracking")
	}

	// Test non-existent migration
	_, exists = pool.GetReconfigurationStatus("nonexistent")
	if exists {
		t.Error("non-existent migration should not exist")
	}
}