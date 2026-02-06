package nntppool

import (
	"context"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/javi11/nntppool/v3/testutil"
)

// BenchmarkThroughputComparison demonstrates the performance impact of different configurations
func BenchmarkThroughputComparison(b *testing.B) {
	// Create mock NNTP server
	handlers := testutil.DefaultHandlers()
	server := testutil.NewMockServer(b, handlers)
	defer server.Close()

	// 100KB article (typical USENET article size)
	articleSize := 100 * 1024

	testCases := []struct {
		name                  string
		maxConnections        int
		inflightPerConnection int
		initialConnections    int
		concurrency           int // number of concurrent goroutines making requests
	}{
		{
			name:                  "Default_Serial_10conn",
			maxConnections:        10,
			inflightPerConnection: 1, // No pipelining (default)
			initialConnections:    10,
			concurrency:           10,
		},
		{
			name:                  "Default_Serial_20conn",
			maxConnections:        20,
			inflightPerConnection: 1,
			initialConnections:    20,
			concurrency:           20,
		},
		{
			name:                  "Pipelined_5x_10conn",
			maxConnections:        10,
			inflightPerConnection: 5, // 5 requests in-flight per connection
			initialConnections:    10,
			concurrency:           50, // 10 conn × 5 pipeline = 50 concurrent
		},
		{
			name:                  "Pipelined_10x_10conn",
			maxConnections:        10,
			inflightPerConnection: 10,
			initialConnections:    10,
			concurrency:           100,
		},
		{
			name:                  "Pipelined_10x_20conn",
			maxConnections:        20,
			inflightPerConnection: 10,
			initialConnections:    20,
			concurrency:           200,
		},
		{
			name:                  "HighConcurrency_10x_30conn",
			maxConnections:        30,
			inflightPerConnection: 10,
			initialConnections:    30,
			concurrency:           300,
		},
	}

	for _, tc := range testCases {
		b.Run(tc.name, func(b *testing.B) {
			ctx := context.Background()

			// Create provider with test configuration
			provider, err := NewProvider(ctx, ProviderConfig{
				Address:               server.Addr(),
				MaxConnections:        tc.maxConnections,
				InitialConnections:    tc.initialConnections,
				InflightPerConnection: tc.inflightPerConnection,
			})
			if err != nil {
				b.Fatalf("Failed to create provider: %v", err)
			}
			defer provider.Close(context.Background())

			// Create client
			client := NewClient()
			if err := client.AddProvider(provider, ProviderPrimary); err != nil {
				b.Fatalf("Failed to add provider: %v", err)
			}

			// Wait for initial connections to be established
			time.Sleep(100 * time.Millisecond)

			b.ResetTimer()
			b.SetBytes(int64(articleSize))

			var totalBytes atomic.Uint64
			var successCount atomic.Uint64
			var errorCount atomic.Uint64

			start := time.Now()

			// Run with controlled concurrency
			var wg sync.WaitGroup
			for i := 0; i < tc.concurrency; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()

					// Each goroutine runs its share of operations
					opsPerWorker := b.N / tc.concurrency
					if opsPerWorker == 0 {
						opsPerWorker = 1
					}

					for j := 0; j < opsPerWorker; j++ {
						messageID := "<test@example.com>"

						err := client.Body(ctx, messageID, io.Discard)
						if err != nil {
							errorCount.Add(1)
							continue
						}

						successCount.Add(1)
						totalBytes.Add(uint64(articleSize))
					}
				}()
			}

			wg.Wait()
			duration := time.Since(start)

			b.StopTimer()

			// Calculate metrics
			throughputMBps := float64(totalBytes.Load()) / duration.Seconds() / (1024 * 1024)
			requestsPerSec := float64(successCount.Load()) / duration.Seconds()
			errorRate := float64(errorCount.Load()) / float64(successCount.Load()+errorCount.Load()) * 100

			b.ReportMetric(throughputMBps, "MB/s")
			b.ReportMetric(requestsPerSec, "req/s")
			b.ReportMetric(errorRate, "%err")
			b.ReportMetric(float64(tc.maxConnections*tc.inflightPerConnection), "max_inflight")

			// Print summary
			fmt.Printf("\n%s Results:\n", tc.name)
			fmt.Printf("  Throughput: %.2f MB/s\n", throughputMBps)
			fmt.Printf("  Requests/sec: %.0f\n", requestsPerSec)
			fmt.Printf("  Error rate: %.2f%%\n", errorRate)
			fmt.Printf("  Total operations: %d\n", successCount.Load()+errorCount.Load())
			fmt.Printf("  Max inflight capacity: %d\n", tc.maxConnections*tc.inflightPerConnection)
		})
	}
}

// BenchmarkChannelOverhead measures the overhead of the current channel-based approach
func BenchmarkChannelOverhead(b *testing.B) {
	b.Run("CurrentImplementation_6ChannelOps", func(b *testing.B) {
		// Simulate the 6 channel operations per request
		reqCh := make(chan int, 10)
		inflightSem := make(chan struct{}, 10)
		pendingCh := make(chan int, 10)
		responseCh := make(chan int, 10)

		// Fill semaphore
		for i := 0; i < 10; i++ {
			inflightSem <- struct{}{}
		}

		b.ResetTimer()

		var wg sync.WaitGroup
		wg.Add(2)

		// Simulate connection goroutine
		go func() {
			defer wg.Done()
			for i := 0; i < b.N; i++ {
				<-inflightSem        // 1. acquire semaphore
				req := <-reqCh       // 2. receive from reqCh
				pendingCh <- req     // 3. send to pending
			}
		}()

		// Simulate reader goroutine
		go func() {
			defer wg.Done()
			for i := 0; i < b.N; i++ {
				req := <-pendingCh   // 4. receive from pending
				responseCh <- req    // 5. send response
				inflightSem <- struct{}{} // 6. release semaphore
			}
		}()

		// Main goroutine sending requests
		for i := 0; i < b.N; i++ {
			reqCh <- i
		}

		// Wait for responses
		for i := 0; i < b.N; i++ {
			<-responseCh
		}

		wg.Wait()
	})

	b.Run("AcquireApproach_2ChannelOps", func(b *testing.B) {
		// Simulate simpler acquire/release with just a connection pool
		type conn struct {
			inflightSem chan struct{}
		}

		pool := make(chan *conn, 10)
		for i := 0; i < 10; i++ {
			c := &conn{
				inflightSem: make(chan struct{}, 1),
			}
			c.inflightSem <- struct{}{}
			pool <- c
		}

		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			c := <-pool               // 1. acquire from pool
			<-c.inflightSem           // 2. acquire inflight slot

			// Do work (simulated)

			c.inflightSem <- struct{}{} // 3. release inflight
			pool <- c                 // 4. return to pool
		}
	})
}

// TestSendRequestTimeoutImpact demonstrates how the 5-second timeout affects throughput
func TestSendRequestTimeoutImpact(t *testing.T) {
	handlers := testutil.DefaultHandlers()
	server := testutil.NewMockServer(t, handlers)
	defer server.Close()

	ctx := context.Background()

	// Create provider with small buffer (default)
	provider, err := NewProvider(ctx, ProviderConfig{
		Address:               server.Addr(),
		MaxConnections:        10,
		InitialConnections:    10,
		InflightPerConnection: 1,
	})
	if err != nil {
		t.Fatalf("Failed to create provider: %v", err)
	}
	defer provider.Close(context.Background())

	client := NewClient()
	if err := client.AddProvider(provider, ProviderPrimary); err != nil {
		t.Fatalf("Failed to add provider: %v", err)
	}

	time.Sleep(100 * time.Millisecond)

	// Flood with requests (much more than buffer size)
	concurrency := 100 // 10x the buffer size
	var wg sync.WaitGroup
	var successCount atomic.Int32
	var timeoutCount atomic.Int32

	start := time.Now()

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			err := client.Body(ctx, "<test@example.com>", io.Discard)
			if err != nil {
				if err.Error() == "failed to send request: context deadline exceeded" {
					timeoutCount.Add(1)
				}
			} else {
				successCount.Add(1)
			}
		}()
	}

	wg.Wait()
	duration := time.Since(start)

	successRate := float64(successCount.Load()) / float64(concurrency) * 100
	timeoutRate := float64(timeoutCount.Load()) / float64(concurrency) * 100

	t.Logf("Results with %d concurrent requests, buffer size %d:", concurrency, 10)
	t.Logf("  Success rate: %.1f%% (%d/%d)", successRate, successCount.Load(), concurrency)
	t.Logf("  Timeout rate: %.1f%% (%d/%d)", timeoutRate, timeoutCount.Load(), concurrency)
	t.Logf("  Duration: %v", duration)

	if timeoutRate > 50 {
		t.Logf("WARNING: High timeout rate indicates SendRequest buffer is too small!")
	}
}
