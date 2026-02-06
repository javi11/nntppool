package nntppool

import (
	"bytes"
	"context"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/javi11/nntppool/v3/testutil"
)

func TestBodyAsync_Basic(t *testing.T) {
	client := NewClient()
	defer client.Close()

	testData := []byte("hello world async")
	p, _ := NewProvider(context.Background(), ProviderConfig{
		Address: "p1:119", MaxConnections: 1, ConnFactory: testutil.MockDialerWithHandler(testutil.MockServerConfig{
			Handler: testutil.YencBodyHandler(testData, "test.bin"),
		}),
	})
	if err := client.AddProvider(p, ProviderPrimary); err != nil {
		t.Fatalf("AddProvider: %v", err)
	}

	var buf bytes.Buffer
	ch := client.BodyAsync(context.Background(), "test-id", &buf)

	resp := <-ch
	if resp.Err != nil {
		t.Fatalf("expected success, got: %v", resp.Err)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		t.Fatalf("unexpected status: %d %s", resp.StatusCode, resp.Status)
	}
	if !bytes.Equal(buf.Bytes(), testData) {
		t.Fatalf("body mismatch: got %q, want %q", buf.Bytes(), testData)
	}
}

func TestBodyAsync_Concurrent(t *testing.T) {
	client := NewClient()
	defer client.Close()

	testData := []byte("concurrent body data")
	p, _ := NewProvider(context.Background(), ProviderConfig{
		Address: "p1:119", MaxConnections: 4, ConnFactory: testutil.MockDialerWithHandler(testutil.MockServerConfig{
			Handler: testutil.YencBodyHandler(testData, "test.bin"),
		}),
	})
	if err := client.AddProvider(p, ProviderPrimary); err != nil {
		t.Fatalf("AddProvider: %v", err)
	}

	const n = 10
	writers := make([]*bytes.Buffer, n)
	channels := make([]<-chan Response, n)

	for i := range n {
		writers[i] = &bytes.Buffer{}
		channels[i] = client.BodyAsync(context.Background(), "msg-id", writers[i])
	}

	for i, ch := range channels {
		resp := <-ch
		if resp.Err != nil {
			t.Errorf("[%d] error: %v", i, resp.Err)
			continue
		}
		if !bytes.Equal(writers[i].Bytes(), testData) {
			t.Errorf("[%d] body mismatch: got %q, want %q", i, writers[i].Bytes(), testData)
		}
	}
}

func TestBodyAsync_Failover430(t *testing.T) {
	client := NewClient()
	defer client.Close()

	testData := []byte("failover data")

	// Primary returns 430
	p1, _ := NewProvider(context.Background(), ProviderConfig{
		Address: "p1:119", MaxConnections: 1, ConnFactory: testutil.MockDialerWithHandler(testutil.MockServerConfig{
			Handler: testutil.NotFoundHandler(),
		}),
	})
	if err := client.AddProvider(p1, ProviderPrimary); err != nil {
		t.Fatalf("AddProvider p1: %v", err)
	}

	// Backup succeeds
	p2, _ := NewProvider(context.Background(), ProviderConfig{
		Address: "p2:119", MaxConnections: 1, ConnFactory: testutil.MockDialerWithHandler(testutil.MockServerConfig{
			Handler: testutil.YencBodyHandler(testData, "test.bin"),
		}),
	})
	if err := client.AddProvider(p2, ProviderBackup); err != nil {
		t.Fatalf("AddProvider p2: %v", err)
	}

	var buf bytes.Buffer
	ch := client.BodyAsync(context.Background(), "missing-id", &buf)

	resp := <-ch
	if resp.Err != nil {
		t.Fatalf("expected failover success, got: %v", resp.Err)
	}
	if !bytes.Equal(buf.Bytes(), testData) {
		t.Fatalf("body mismatch: got %q, want %q", buf.Bytes(), testData)
	}
}

func TestBodyAsync_AllProvidersFail(t *testing.T) {
	client := NewClient()
	defer client.Close()

	p1, _ := NewProvider(context.Background(), ProviderConfig{
		Address: "p1:119", MaxConnections: 1, ConnFactory: testutil.MockDialerWithHandler(testutil.MockServerConfig{
			Handler: testutil.NotFoundHandler(),
		}),
	})
	if err := client.AddProvider(p1, ProviderPrimary); err != nil {
		t.Fatalf("AddProvider p1: %v", err)
	}

	p2, _ := NewProvider(context.Background(), ProviderConfig{
		Address: "p2:119", MaxConnections: 1, ConnFactory: testutil.MockDialerWithHandler(testutil.MockServerConfig{
			Handler: testutil.NotFoundHandler(),
		}),
	})
	if err := client.AddProvider(p2, ProviderBackup); err != nil {
		t.Fatalf("AddProvider p2: %v", err)
	}

	ch := client.BodyAsync(context.Background(), "nonexistent", io.Discard)

	resp := <-ch
	if resp.Err == nil {
		t.Fatal("expected error when all providers fail")
	}
	if !IsArticleNotFound(resp.Err) {
		t.Fatalf("expected ArticleNotFoundError, got: %v", resp.Err)
	}
}

func TestBodyAsync_ContextCancellation(t *testing.T) {
	client := NewClient()
	defer client.Close()

	// Use a slow handler so we can cancel mid-flight
	p, _ := NewProvider(context.Background(), ProviderConfig{
		Address: "p1:119", MaxConnections: 1, ConnFactory: testutil.MockDialerWithHandler(testutil.MockServerConfig{
			Handler: testutil.SlowResponseHandler(2*time.Second, "slow data"),
		}),
	})
	if err := client.AddProvider(p, ProviderPrimary); err != nil {
		t.Fatalf("AddProvider: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	ch := client.BodyAsync(ctx, "slow-id", io.Discard)

	// Cancel quickly
	cancel()

	// Should complete with context error
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		select {
		case resp := <-ch:
			if resp.Err == nil {
				t.Error("expected error after context cancellation")
			}
		case <-time.After(5 * time.Second):
			t.Error("timed out waiting for response after cancellation")
		}
	}()
	wg.Wait()
}
