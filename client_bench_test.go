package nntppool

import (
	"context"
	"crypto/rand"
	"io"
	"testing"
	"time"

	"github.com/javi11/nntppool/v3/testutil"
)

// generateTestData creates random test data of the specified size.
func generateTestData(size int) []byte {
	data := make([]byte, size)
	_, err := rand.Read(data)
	if err != nil {
		panic(err)
	}
	return data
}

// setupBenchmarkClient creates a client with a mock provider for benchmarking.
func setupBenchmarkClient(b *testing.B, payload []byte, maxInflight int, maxConnections int) *Client {
	b.Helper()

	// Create mock dialer with YEnc handler
	mockDial := testutil.MockDialerWithHandler(testutil.MockServerConfig{
		Handler: testutil.YencBodyHandler(payload, "benchmark.dat"),
	})

	// Create client
	client := NewClient(maxInflight)

	// Create provider
	provider, err := NewProvider(context.Background(), ProviderConfig{
		Address:               "bench.example.com:119",
		MaxConnections:        maxConnections,
		InflightPerConnection: 10,
		ConnFactory:           mockDial,
	})
	if err != nil {
		b.Fatalf("failed to create provider: %v", err)
	}

	err = client.AddProvider(provider, ProviderPrimary)
	if err != nil {
		b.Fatalf("failed to add provider: %v", err)
	}

	return client
}

// setupMultiProviderClient creates a client with multiple providers for benchmarking.
func setupMultiProviderClient(b *testing.B, payload []byte, maxInflight int) *Client {
	b.Helper()

	client := NewClient(maxInflight)

	// Add 2 primary providers
	for i := 0; i < 2; i++ {
		mockDial := testutil.MockDialerWithHandler(testutil.MockServerConfig{
			Handler: testutil.YencBodyHandler(payload, "benchmark.dat"),
		})

		provider, err := NewProvider(context.Background(), ProviderConfig{
			Address:               "primary.example.com:119",
			MaxConnections:        5,
			InflightPerConnection: 10,
			ConnFactory:           mockDial,
		})
		if err != nil {
			b.Fatalf("failed to create primary provider %d: %v", i, err)
		}

		err = client.AddProvider(provider, ProviderPrimary)
		if err != nil {
			b.Fatalf("failed to add provider: %v", err)
		}
	}

	// Add 1 backup provider
	mockDial := testutil.MockDialerWithHandler(testutil.MockServerConfig{
		Handler: testutil.YencBodyHandler(payload, "benchmark.dat"),
	})

	provider, err := NewProvider(context.Background(), ProviderConfig{
		Address:               "backup.example.com:119",
		MaxConnections:        5,
		InflightPerConnection: 10,
		ConnFactory:           mockDial,
	})
	if err != nil {
		b.Fatalf("failed to create backup provider: %v", err)
	}

	err = client.AddProvider(provider, ProviderBackup)
	if err != nil {
		b.Fatalf("failed to add backup provider: %v", err)
	}

	return client
}

// BenchmarkClientBody_SmallPayload benchmarks Body method with 1KB payload.
// Tests baseline overhead and small data throughput.
func BenchmarkClientBody_SmallPayload(b *testing.B) {
	payload := generateTestData(1024) // 1 KB
	client := setupBenchmarkClient(b, payload, 10, 2)
	defer client.Close()

	ctx := context.Background()

	b.ResetTimer()
	b.SetBytes(int64(len(payload)))

	for i := 0; i < b.N; i++ {
		err := client.Body(ctx, "test-id", io.Discard)
		if err != nil {
			b.Fatalf("Body failed: %v", err)
		}
	}

	elapsed := b.Elapsed()
	mbPerSec := float64(len(payload)*b.N) / elapsed.Seconds() / (1024 * 1024)
	b.ReportMetric(mbPerSec, "MB/s")
}

// BenchmarkClientBody_MediumPayload benchmarks Body method with 100KB payload.
// Tests typical text/article size performance.
func BenchmarkClientBody_MediumPayload(b *testing.B) {
	payload := generateTestData(100 * 1024) // 100 KB
	client := setupBenchmarkClient(b, payload, 50, 5)
	defer client.Close()

	ctx := context.Background()

	b.ResetTimer()
	b.SetBytes(int64(len(payload)))

	for i := 0; i < b.N; i++ {
		err := client.Body(ctx, "test-id", io.Discard)
		if err != nil {
			b.Fatalf("Body failed: %v", err)
		}
	}

	elapsed := b.Elapsed()
	mbPerSec := float64(len(payload)*b.N) / elapsed.Seconds() / (1024 * 1024)
	b.ReportMetric(mbPerSec, "MB/s")
}

// BenchmarkClientBody_LargePayload benchmarks Body method with 10MB payload.
// Tests binary file download performance with >100 MB/s target.
func BenchmarkClientBody_LargePayload(b *testing.B) {
	payload := generateTestData(10 * 1024 * 1024) // 10 MB
	client := setupBenchmarkClient(b, payload, 100, 10)
	defer client.Close()

	ctx := context.Background()

	b.ResetTimer()
	b.SetBytes(int64(len(payload)))

	for i := 0; i < b.N; i++ {
		err := client.Body(ctx, "test-id", io.Discard)
		if err != nil {
			b.Fatalf("Body failed: %v", err)
		}
	}

	elapsed := b.Elapsed()
	mbPerSec := float64(len(payload)*b.N) / elapsed.Seconds() / (1024 * 1024)
	b.ReportMetric(mbPerSec, "MB/s")

	// Validate throughput meets target
	if mbPerSec < 100.0 {
		b.Errorf("Throughput %.2f MB/s is below target of 100 MB/s", mbPerSec)
	}
}

// BenchmarkClientBody_Parallel_1KB benchmarks concurrent Body calls with 1KB payload.
func BenchmarkClientBody_Parallel_1KB(b *testing.B) {
	payload := generateTestData(1024) // 1 KB
	client := setupBenchmarkClient(b, payload, 10, 2)
	defer client.Close()

	b.ResetTimer()
	b.SetBytes(int64(len(payload)))

	b.RunParallel(func(pb *testing.PB) {
		ctx := context.Background()
		for pb.Next() {
			err := client.Body(ctx, "test-id", io.Discard)
			if err != nil {
				b.Errorf("Body failed: %v", err)
				return
			}
		}
	})

	elapsed := b.Elapsed()
	mbPerSec := float64(len(payload)*b.N) / elapsed.Seconds() / (1024 * 1024)
	b.ReportMetric(mbPerSec, "MB/s")
}

// BenchmarkClientBody_Parallel_100KB benchmarks concurrent Body calls with 100KB payload.
func BenchmarkClientBody_Parallel_100KB(b *testing.B) {
	payload := generateTestData(100 * 1024) // 100 KB
	client := setupBenchmarkClient(b, payload, 50, 5)
	defer client.Close()

	b.ResetTimer()
	b.SetBytes(int64(len(payload)))

	b.RunParallel(func(pb *testing.PB) {
		ctx := context.Background()
		for pb.Next() {
			err := client.Body(ctx, "test-id", io.Discard)
			if err != nil {
				b.Errorf("Body failed: %v", err)
				return
			}
		}
	})

	elapsed := b.Elapsed()
	mbPerSec := float64(len(payload)*b.N) / elapsed.Seconds() / (1024 * 1024)
	b.ReportMetric(mbPerSec, "MB/s")
}

// BenchmarkClientBody_Parallel_10MB benchmarks concurrent Body calls with 10MB payload.
// Tests concurrent large file downloads with >100 MB/s target.
func BenchmarkClientBody_Parallel_10MB(b *testing.B) {
	payload := generateTestData(10 * 1024 * 1024) // 10 MB
	client := setupBenchmarkClient(b, payload, 100, 10)
	defer client.Close()

	b.ResetTimer()
	b.SetBytes(int64(len(payload)))

	b.RunParallel(func(pb *testing.PB) {
		ctx := context.Background()
		for pb.Next() {
			err := client.Body(ctx, "test-id", io.Discard)
			if err != nil {
				b.Errorf("Body failed: %v", err)
				return
			}
		}
	})

	elapsed := b.Elapsed()
	mbPerSec := float64(len(payload)*b.N) / elapsed.Seconds() / (1024 * 1024)
	b.ReportMetric(mbPerSec, "MB/s")

	// Validate throughput meets target
	if mbPerSec < 100.0 {
		b.Errorf("Throughput %.2f MB/s is below target of 100 MB/s", mbPerSec)
	}
}

// BenchmarkClientBody_MultiProvider benchmarks Body method with multiple providers.
// Tests performance with primary + backup failover scenario.
func BenchmarkClientBody_MultiProvider(b *testing.B) {
	payload := generateTestData(1024 * 1024) // 1 MB
	client := setupMultiProviderClient(b, payload, 50)
	defer client.Close()

	ctx := context.Background()

	b.ResetTimer()
	b.SetBytes(int64(len(payload)))

	for i := 0; i < b.N; i++ {
		err := client.Body(ctx, "test-id", io.Discard)
		if err != nil {
			b.Fatalf("Body failed: %v", err)
		}
	}

	elapsed := b.Elapsed()
	mbPerSec := float64(len(payload)*b.N) / elapsed.Seconds() / (1024 * 1024)
	b.ReportMetric(mbPerSec, "MB/s")
}

// BenchmarkClientBody_WithTimeout benchmarks Body method with context timeout.
// Tests overhead of context deadline handling.
func BenchmarkClientBody_WithTimeout(b *testing.B) {
	payload := generateTestData(100 * 1024) // 100 KB
	client := setupBenchmarkClient(b, payload, 50, 5)
	defer client.Close()

	b.ResetTimer()
	b.SetBytes(int64(len(payload)))

	for i := 0; i < b.N; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		err := client.Body(ctx, "test-id", io.Discard)
		cancel()
		if err != nil {
			b.Fatalf("Body failed: %v", err)
		}
	}

	elapsed := b.Elapsed()
	mbPerSec := float64(len(payload)*b.N) / elapsed.Seconds() / (1024 * 1024)
	b.ReportMetric(mbPerSec, "MB/s")
}
