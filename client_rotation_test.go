package nntppool

import (
	"context"
	"io"
	"strings"
	"testing"

	"github.com/javi11/nntppool/v3/testutil"
)

func TestClientRotation_ArticleNotFound(t *testing.T) {
	// Scenario: Primary returns 430, Backup returns 200 (Success)

	client := NewClient(10)
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

	client := NewClient(10)
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

	client := NewClient(10)
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
