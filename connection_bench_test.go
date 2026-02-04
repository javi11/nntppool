package nntppool

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/javi11/nntppool/v3/testutil"
)

// BenchmarkConnectionTimerAlloc measures allocations in the connection Run() loop.
// Before optimization: 2 timer allocs per request cycle (idle + lifetime), 2 atomic ops.
// After optimization: 0 timer allocs per request cycle (centralized health check), 0 atomic ops.
func BenchmarkConnectionTimerAlloc(b *testing.B) {
	payload := generateTestData(1024) // 1 KB

	mockDial := testutil.MockDialerWithHandler(testutil.MockServerConfig{
		Handler: testutil.YencBodyHandler(payload, "benchmark.dat"),
	})

	provider, err := NewProvider(context.Background(), ProviderConfig{
		Address:               "bench.example.com:119",
		MaxConnections:        1, // Single connection to isolate timer behavior
		InflightPerConnection: 1,
		MaxConnIdleTime:       5 * time.Minute,
		MaxConnLifetime:       30 * time.Minute,
		ConnFactory:           mockDial,
	})
	if err != nil {
		b.Fatalf("failed to create provider: %v", err)
	}
	defer func() { _ = provider.Close() }()

	ctx := context.Background()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		resp := <-provider.Send(ctx, []byte("BODY <test>\r\n"), nil)
		if resp.Err != nil {
			b.Fatalf("request failed: %v", resp.Err)
		}
	}
}

// BenchmarkHighConcurrencyThroughput measures throughput with multiple connections.
// Tests connection scaling under load after optimization (no per-request atomics).
func BenchmarkHighConcurrencyThroughput(b *testing.B) {
	payload := generateTestData(1024)

	mockDial := testutil.MockDialerWithHandler(testutil.MockServerConfig{
		Handler: testutil.YencBodyHandler(payload, "benchmark.dat"),
	})

	// Multiple connections to test parallel throughput
	provider, err := NewProvider(context.Background(), ProviderConfig{
		Address:               "bench.example.com:119",
		MaxConnections:        10,
		InitialConnections:    10,
		InflightPerConnection: 5,
		ConnFactory:           mockDial,
	})
	if err != nil {
		b.Fatalf("failed to create provider: %v", err)
	}
	defer func() { _ = provider.Close() }()

	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		ctx := context.Background()
		for pb.Next() {
			resp := <-provider.Send(ctx, []byte("BODY <test>\r\n"), nil)
			if resp.Err != nil {
				b.Errorf("request failed: %v", resp.Err)
				return
			}
		}
	})
}

// BenchmarkProviderSendRequest measures SendRequest overhead.
// This is the critical path for request submission.
func BenchmarkProviderSendRequest(b *testing.B) {
	payload := generateTestData(1024)

	mockDial := testutil.MockDialerWithHandler(testutil.MockServerConfig{
		Handler: testutil.YencBodyHandler(payload, "benchmark.dat"),
	})

	provider, err := NewProvider(context.Background(), ProviderConfig{
		Address:               "bench.example.com:119",
		MaxConnections:        5,
		InitialConnections:    5,
		InflightPerConnection: 10,
		ConnFactory:           mockDial,
	})
	if err != nil {
		b.Fatalf("failed to create provider: %v", err)
	}
	defer func() { _ = provider.Close() }()

	ctx := context.Background()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		req := &Request{
			Ctx:     ctx,
			Payload: []byte("BODY <test>\r\n"),
			RespCh:  make(chan Response, 1),
		}
		respCh := provider.SendRequest(req)
		resp := <-respCh
		if resp.Err != nil {
			b.Fatalf("request failed: %v", resp.Err)
		}
	}
}

// BenchmarkProviderThroughput_HighConcurrency measures throughput with many concurrent requests.
// Tests how well the connection management scales under load.
func BenchmarkProviderThroughput_HighConcurrency(b *testing.B) {
	payload := generateTestData(10 * 1024) // 10 KB

	mockDial := testutil.MockDialerWithHandler(testutil.MockServerConfig{
		Handler: testutil.YencBodyHandler(payload, "benchmark.dat"),
	})

	provider, err := NewProvider(context.Background(), ProviderConfig{
		Address:               "bench.example.com:119",
		MaxConnections:        20,
		InitialConnections:    20,
		InflightPerConnection: 10, // 200 total inflight capacity
		ConnFactory:           mockDial,
	})
	if err != nil {
		b.Fatalf("failed to create provider: %v", err)
	}
	defer func() { _ = provider.Close() }()

	b.SetBytes(int64(len(payload)))
	b.ResetTimer()

	var ops atomic.Int64

	b.RunParallel(func(pb *testing.PB) {
		ctx := context.Background()
		for pb.Next() {
			resp := <-provider.Send(ctx, []byte("BODY <test>\r\n"), nil)
			if resp.Err != nil {
				b.Errorf("request failed: %v", resp.Err)
				return
			}
			ops.Add(1)
		}
	})

	elapsed := b.Elapsed()
	opsPerSec := float64(ops.Load()) / elapsed.Seconds()
	mbPerSec := float64(len(payload)) * opsPerSec / (1024 * 1024)
	b.ReportMetric(opsPerSec, "ops/s")
	b.ReportMetric(mbPerSec, "MB/s")
}

// BenchmarkConnectionChurn measures performance when connections are frequently created/destroyed.
// Tests the centralized health check and connection lifecycle management.
func BenchmarkConnectionChurn(b *testing.B) {
	payload := generateTestData(1024)

	mockDial := testutil.MockDialerWithHandler(testutil.MockServerConfig{
		Handler: testutil.YencBodyHandler(payload, "benchmark.dat"),
	})

	// Short timeouts to trigger connection churn via centralized health check
	provider, err := NewProvider(context.Background(), ProviderConfig{
		Address:               "bench.example.com:119",
		MaxConnections:        5,
		InitialConnections:    1,
		InflightPerConnection: 5,
		MaxConnIdleTime:       100 * time.Millisecond, // Fast idle timeout
		MaxConnLifetime:       500 * time.Millisecond, // Fast lifetime
		HealthCheckPeriod:     50 * time.Millisecond,  // Fast health checks
		ConnFactory:           mockDial,
	})
	if err != nil {
		b.Fatalf("failed to create provider: %v", err)
	}
	defer func() { _ = provider.Close() }()

	ctx := context.Background()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		resp := <-provider.Send(ctx, []byte("BODY <test>\r\n"), nil)
		if resp.Err != nil {
			b.Fatalf("request failed: %v", resp.Err)
		}
		// Small sleep to allow connection churn
		if i%100 == 0 {
			time.Sleep(10 * time.Millisecond)
		}
	}
}
