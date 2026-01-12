package nntppool

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/javi11/nntppool/v3/pkg/nntpcli"
)

func TestSimplifiedQuit(t *testing.T) {
	config := Config{
		Logger:  slog.Default(),
		NntpCli: nntpcli.New(),
		Providers: []UsenetProviderConfig{
			{
				Host:           "test.example.com",
				Username:       "user1",
				MaxConnections: 5,
			},
		},
		SkipProvidersVerificationOnCreation: true,
		ShutdownTimeout:                     5 * time.Second,
	}

	pool, err := NewConnectionPool(config)
	if err != nil {
		t.Fatalf("failed to create connection pool: %v", err)
	}

	// Test shutdown
	start := time.Now()
	pool.Quit()
	duration := time.Since(start)

	// Shutdown should complete within the 5-second timeout
	if duration > 6*time.Second {
		t.Errorf("shutdown took too long: %v", duration)
	}

	// Test that further operations fail after shutdown
	_, err = pool.GetConnection(context.Background(), []string{})
	if err == nil {
		t.Error("GetConnection should fail after shutdown")
	}
	if err.Error() != "connection pool is shutdown" {
		t.Errorf("expected shutdown error, got: %v", err)
	}

	// Test that double quit is safe
	pool.Quit() // Should not panic or hang
}

func TestQuitWithActiveConnections(t *testing.T) {
	config := Config{
		Logger:  slog.Default(),
		NntpCli: nntpcli.New(),
		Providers: []UsenetProviderConfig{
			{
				Host:           "test.example.com",
				Username:       "user1",
				MaxConnections: 5,
			},
		},
		SkipProvidersVerificationOnCreation: true,
		ShutdownTimeout:                     5 * time.Second,
	}

	pool, err := NewConnectionPool(config)
	if err != nil {
		t.Fatalf("failed to create connection pool: %v", err)
	}

	// Start health checker and reconnection goroutines
	time.Sleep(100 * time.Millisecond)

	// Test shutdown with background goroutines running
	start := time.Now()
	pool.Quit()
	duration := time.Since(start)

	// Should shutdown within timeout
	if duration > 6*time.Second {
		t.Errorf("shutdown with active connections took too long: %v", duration)
	}
}

func TestQuitTimeout(t *testing.T) {
	// This test verifies that shutdown completes even if background goroutines are slow
	config := Config{
		Logger:  slog.Default(),
		NntpCli: nntpcli.New(),
		Providers: []UsenetProviderConfig{
			{
				Host:           "test.example.com",
				Username:       "user1",
				MaxConnections: 1,
			},
		},
		SkipProvidersVerificationOnCreation: true,
		HealthCheckInterval:                 1 * time.Millisecond, // Very frequent to simulate work
		ProviderReconnectInterval:           1 * time.Millisecond, // Very frequent
	}

	pool, err := NewConnectionPool(config)
	if err != nil {
		t.Fatalf("failed to create connection pool: %v", err)
	}

	// Let background workers start
	time.Sleep(50 * time.Millisecond)

	// Quit should complete within 5 seconds regardless of background activity
	start := time.Now()
	pool.Quit()
	duration := time.Since(start)

	if duration > 6*time.Second {
		t.Errorf("shutdown should complete within 5 seconds, took: %v", duration)
	}

	t.Logf("Shutdown completed in %v", duration)
}
