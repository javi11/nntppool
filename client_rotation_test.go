package nntppool

import (
	"context"
	"io"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/javi11/nntppool/v3/testutil"
)

func TestClientRotation_ArticleNotFound(t *testing.T) {
	// Scenario: Primary returns 430, Backup returns 200 (Success)

	client := NewClient()
	defer client.Close()

	// Add Primary (Failing)
	p1, _ := NewProvider(context.Background(), ProviderConfig{
		Address: "p1:119", MaxConnections: 1, ConnFactory: testutil.MockDialerWithHandler(testutil.MockServerConfig{
			Handler: testutil.NotFoundHandler(),
		}),
	})
	err := client.AddProvider(p1, ProviderPrimary)
	if err != nil {
		t.Fatalf("failed to add p1: %v", err)
	}

	// Add Backup (Succeeding)
	p2, _ := NewProvider(context.Background(), ProviderConfig{
		Address: "p2:119", MaxConnections: 1, ConnFactory: testutil.MockDialerWithHandler(testutil.MockServerConfig{
			Handler: testutil.SuccessfulBodyHandler("line1"),
		}),
	})
	err = client.AddProvider(p2, ProviderBackup)
	if err != nil {
		t.Fatalf("failed to add p2: %v", err)
	}

	// Execute
	err = client.Body(context.Background(), "123", io.Discard)
	if err != nil {
		t.Fatalf("expected success, got error: %v", err)
	}

	// Verify metrics to ensure rotation happened
	metrics := client.Metrics()
	// Note: metrics["p1:119"].ActiveConnections and metrics["p2:119"].ActiveConnections might be 0
	// because connections might be idle but instantiated.
	// We can't easily check 'calls made' without instrumentation, but success implies p2 was reached.
	_ = metrics
}

func TestClientRotation_OnlyBackups(t *testing.T) {
	// Scenario: No primaries, only Backup returns 200

	client := NewClient()
	defer client.Close()

	// Add Backup
	p1, _ := NewProvider(context.Background(), ProviderConfig{
		Address: "p1:119", MaxConnections: 1, ConnFactory: testutil.MockDialerWithHandler(testutil.MockServerConfig{
			Handler: testutil.SuccessfulBodyHandler("line1"),
		}),
	})
	err := client.AddProvider(p1, ProviderBackup)
	if err != nil {
		t.Fatalf("failed to add p1: %v", err)
	}

	// Execute
	err = client.Body(context.Background(), "123", io.Discard)
	if err != nil {
		t.Fatalf("expected success, got error: %v", err)
	}
}

func TestClientRotation_AllFail(t *testing.T) {
	// Scenario: Primary returns 430, Backup returns 430

	client := NewClient()
	defer client.Close()

	// Add Primary
	p1, _ := NewProvider(context.Background(), ProviderConfig{
		Address: "p1:119", MaxConnections: 1, ConnFactory: testutil.MockDialerWithHandler(testutil.MockServerConfig{
			Handler: testutil.NotFoundHandler(),
		}),
	})
	err := client.AddProvider(p1, ProviderPrimary)
	if err != nil {
		t.Fatalf("failed to add p1: %v", err)
	}

	// Add Backup
	p2, _ := NewProvider(context.Background(), ProviderConfig{
		Address: "p2:119", MaxConnections: 1, ConnFactory: testutil.MockDialerWithHandler(testutil.MockServerConfig{
			Handler: testutil.NotFoundHandler(),
		}),
	})
	err = client.AddProvider(p2, ProviderBackup)
	if err != nil {
		t.Fatalf("failed to add p2: %v", err)
	}

	// Execute
	err = client.Body(context.Background(), "123", io.Discard)
	if err == nil {
		t.Fatal("expected error, got success")
	}

	// Check using helper method
	if !IsArticleNotFound(err) {
		t.Fatalf("expected ArticleNotFoundError, got: %T %v", err, err)
	}

	// Verify error message
	if !strings.Contains(err.Error(), "article not found") {
		t.Fatalf("expected 'article not found' in error, got: %v", err)
	}

	// Verify error contains message ID
	if !strings.Contains(err.Error(), "123") {
		t.Fatalf("expected message ID '123' in error, got: %v", err)
	}
}

func TestClientFailoverOnProviderError(t *testing.T) {
	// Scenario: Primary returns 503 Service Unavailable, Backup works
	// This tests failover when a provider returns an error response (non-2xx, non-430).
	//
	// This complements TestClientRotation_ArticleNotFound which tests 430 responses.
	// Together they verify failover works for different error conditions.
	//
	// Note: We use different addresses (primary.example.com vs backup.example.com)
	// to ensure the client's host-based deduplication doesn't skip the backup.
	// The mock dialer ignores the actual address and uses in-memory pipes.

	// Track whether backup was used
	var backupUsed atomic.Bool

	client := NewClient()
	defer client.Close()

	// Primary: Passes DATE check but returns 503 on BODY (service unavailable)
	unavailableHandler := func(cmd string) (string, error) {
		if cmd == "DATE\r\n" {
			return "111 20240101000000\r\n", nil
		}
		if strings.HasPrefix(cmd, "BODY") {
			// Return 503 Service Unavailable - should trigger failover
			return "503 Service Unavailable\r\n", nil
		}
		if strings.HasPrefix(cmd, "QUIT") {
			return "205 Bye\r\n", nil
		}
		return "500 Unknown Command\r\n", nil
	}

	p1, _ := NewProvider(context.Background(), ProviderConfig{
		Address:        "primary.example.com:119",
		MaxConnections: 1,
		ConnFactory: testutil.MockDialerWithHandler(testutil.MockServerConfig{
			Handler: unavailableHandler,
		}),
	})
	err := client.AddProvider(p1, ProviderPrimary)
	if err != nil {
		t.Fatalf("failed to add primary: %v", err)
	}

	// Backup: Working normally, tracks when it's used
	backupHandler := func(cmd string) (string, error) {
		if cmd == "DATE\r\n" {
			return "111 20240101000000\r\n", nil
		}
		if strings.HasPrefix(cmd, "BODY") {
			backupUsed.Store(true)
			return "222 0 <id> body follows\r\nbackup-content\r\n.\r\n", nil
		}
		if strings.HasPrefix(cmd, "QUIT") {
			return "205 Bye\r\n", nil
		}
		return "500 Unknown Command\r\n", nil
	}

	p2, _ := NewProvider(context.Background(), ProviderConfig{
		Address:        "backup.example.com:119",
		MaxConnections: 1,
		ConnFactory: testutil.MockDialerWithHandler(testutil.MockServerConfig{
			Handler: backupHandler,
		}),
	})
	err = client.AddProvider(p2, ProviderBackup)
	if err != nil {
		t.Fatalf("failed to add backup: %v", err)
	}

	// Execute - should failover to backup and succeed
	// The primary will return 503 error, and the client should
	// automatically retry with the backup provider
	err = client.Body(context.Background(), "<test@example.com>", io.Discard)
	if err != nil {
		t.Fatalf("expected success via failover to backup, got error: %v", err)
	}

	// Verify backup was actually used for the failover
	if !backupUsed.Load() {
		t.Fatal("expected backup provider to handle the request after primary failed")
	}
}
