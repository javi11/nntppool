package main

import (
	"context"
	"fmt"
	"io"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/javi11/nntppool/v3"
)

// PoolBenchResult holds results from pool-based benchmarks.
type PoolBenchResult struct {
	WarmupTime       time.Duration
	BodyTimes        []time.Duration
	BodySizes        []int64
	TotalTime        time.Duration
	BytesTransferred int64
	Errors           []string
	Concurrency      int
}

// Stats returns statistical summary of body download times.
func (r *PoolBenchResult) Stats() (avg, p50, p95, p99 time.Duration) {
	if len(r.BodyTimes) == 0 {
		return
	}

	var total time.Duration
	for _, t := range r.BodyTimes {
		total += t
	}
	avg = total / time.Duration(len(r.BodyTimes))

	sorted := make([]time.Duration, len(r.BodyTimes))
	copy(sorted, r.BodyTimes)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })

	p50 = sorted[len(sorted)*50/100]
	p95 = sorted[len(sorted)*95/100]
	if len(sorted) > 1 {
		p99 = sorted[len(sorted)*99/100]
	} else {
		p99 = sorted[len(sorted)-1]
	}
	return
}

// PoolBench performs a pool-based benchmark.
func PoolBench(ctx context.Context, cfg BenchConfig, segments []SegmentInfo, limit int) (*PoolBenchResult, error) {
	result := &PoolBenchResult{
		Concurrency: cfg.Connections,
	}

	// Create pool config
	providerCfg := nntppool.ProviderConfig{
		Name:            cfg.Host,
		Host:            cfg.Host,
		Port:            cfg.Port,
		TLS:             cfg.TLS,
		Username:        cfg.Username,
		Password:        cfg.Password,
		MaxConnections:  cfg.Connections,
		InflightPerConn: 1,
		Priority:        0,
	}

	poolCfg := nntppool.PoolConfig{
		Providers:           []nntppool.ProviderConfig{providerCfg},
		HealthCheckInterval: 0, // Disable health checks for benchmark
	}

	// Create pool and measure warmup time
	warmupStart := time.Now()
	pool, err := nntppool.NewPool(ctx, poolCfg)
	if err != nil {
		return nil, fmt.Errorf("create pool: %w", err)
	}
	defer pool.Close()

	// Warmup: send a DATE command to establish connections
	if len(segments) > 0 && len(segments[0].Groups) > 0 {
		_, _ = pool.Group(ctx, segments[0].Groups[0])
	}
	result.WarmupTime = time.Since(warmupStart)

	// Determine how many segments to test
	count := limit
	if count <= 0 || count > len(segments) {
		count = len(segments)
	}

	// Run benchmark
	totalStart := time.Now()

	var mu sync.Mutex
	var wg sync.WaitGroup
	var bytesTotal atomic.Int64

	// Use a semaphore to limit concurrency
	sem := make(chan struct{}, cfg.Connections)

	for i := 0; i < count; i++ {
		seg := segments[i]
		wg.Add(1)
		sem <- struct{}{} // Acquire

		go func(seg SegmentInfo, idx int) {
			defer wg.Done()
			defer func() { <-sem }() // Release

			start := time.Now()
			n, err := pool.Body(ctx, seg.MessageID, io.Discard)
			elapsed := time.Since(start)

			mu.Lock()
			if err != nil {
				result.Errors = append(result.Errors, fmt.Sprintf("body %d: %v", idx, err))
			} else {
				result.BodyTimes = append(result.BodyTimes, elapsed)
				result.BodySizes = append(result.BodySizes, n)
				bytesTotal.Add(n)
			}
			mu.Unlock()
		}(seg, i)
	}

	wg.Wait()
	result.TotalTime = time.Since(totalStart)
	result.BytesTransferred = bytesTotal.Load()

	return result, nil
}

// PoolBenchSequential performs a sequential (non-concurrent) pool benchmark.
func PoolBenchSequential(ctx context.Context, cfg BenchConfig, segments []SegmentInfo, limit int) (*PoolBenchResult, error) {
	result := &PoolBenchResult{
		Concurrency: 1,
	}

	providerCfg := nntppool.ProviderConfig{
		Name:            cfg.Host,
		Host:            cfg.Host,
		Port:            cfg.Port,
		TLS:             cfg.TLS,
		Username:        cfg.Username,
		Password:        cfg.Password,
		MaxConnections:  1, // Single connection for sequential
		InflightPerConn: 1,
		Priority:        0,
	}

	poolCfg := nntppool.PoolConfig{
		Providers:           []nntppool.ProviderConfig{providerCfg},
		HealthCheckInterval: 0,
	}

	warmupStart := time.Now()
	pool, err := nntppool.NewPool(ctx, poolCfg)
	if err != nil {
		return nil, fmt.Errorf("create pool: %w", err)
	}
	defer pool.Close()

	// Warmup with GROUP
	if len(segments) > 0 && len(segments[0].Groups) > 0 {
		_, _ = pool.Group(ctx, segments[0].Groups[0])
	}
	result.WarmupTime = time.Since(warmupStart)

	count := limit
	if count <= 0 || count > len(segments) {
		count = len(segments)
	}

	totalStart := time.Now()

	for i := 0; i < count; i++ {
		seg := segments[i]
		start := time.Now()
		n, err := pool.Body(ctx, seg.MessageID, io.Discard)
		elapsed := time.Since(start)

		if err != nil {
			result.Errors = append(result.Errors, fmt.Sprintf("body %d: %v", i, err))
		} else {
			result.BodyTimes = append(result.BodyTimes, elapsed)
			result.BodySizes = append(result.BodySizes, n)
			result.BytesTransferred += n
		}
	}

	result.TotalTime = time.Since(totalStart)
	return result, nil
}

// PrintPoolResult prints the pool benchmark result.
func PrintPoolResult(r *PoolBenchResult, label string) {
	fmt.Printf("\n=== %s ===\n", label)
	fmt.Printf("Concurrency:     %d connections\n", r.Concurrency)
	fmt.Printf("Warmup Time:     %v\n", r.WarmupTime.Round(time.Millisecond))

	if len(r.BodyTimes) > 0 {
		avg, p50, p95, p99 := r.Stats()
		fmt.Printf("\nBody Downloads (%d articles):\n", len(r.BodyTimes))
		fmt.Printf("  Average:       %v\n", avg.Round(time.Millisecond))
		fmt.Printf("  P50:           %v\n", p50.Round(time.Millisecond))
		fmt.Printf("  P95:           %v\n", p95.Round(time.Millisecond))
		fmt.Printf("  P99:           %v\n", p99.Round(time.Millisecond))

		if r.TotalTime > 0 {
			mbps := float64(r.BytesTransferred) * 8 / r.TotalTime.Seconds() / 1_000_000
			fmt.Printf("\nThroughput:      %.2f Mbps\n", mbps)
		}
		fmt.Printf("Total Data:      %.2f MB\n", float64(r.BytesTransferred)/1_000_000)
	}

	fmt.Printf("Total Time:      %v\n", r.TotalTime.Round(time.Millisecond))

	if len(r.Errors) > 0 {
		fmt.Printf("\nErrors (%d):\n", len(r.Errors))
		for i, e := range r.Errors {
			if i >= 5 {
				fmt.Printf("  ... and %d more\n", len(r.Errors)-5)
				break
			}
			fmt.Printf("  - %s\n", e)
		}
	}
}

// CompareResults prints a comparison between raw and pool results.
func CompareResults(raw *RawBenchResult, pool *PoolBenchResult) {
	fmt.Println("\n=== Comparison (Pool vs Raw) ===")

	if len(raw.BodyTimes) > 0 && len(pool.BodyTimes) > 0 {
		rawAvg, _, _, _ := raw.Stats()
		poolAvg, _, _, _ := pool.Stats()

		overhead := poolAvg - rawAvg
		overheadPct := float64(overhead) / float64(rawAvg) * 100

		fmt.Printf("Raw Avg Latency:   %v\n", rawAvg.Round(time.Millisecond))
		fmt.Printf("Pool Avg Latency:  %v\n", poolAvg.Round(time.Millisecond))
		fmt.Printf("Pool Overhead:     %v (%.1f%%)\n", overhead.Round(time.Millisecond), overheadPct)

		if raw.BytesTransferred > 0 && pool.BytesTransferred > 0 {
			rawMbps := float64(raw.BytesTransferred) * 8 / raw.TotalTime.Seconds() / 1_000_000
			poolMbps := float64(pool.BytesTransferred) * 8 / pool.TotalTime.Seconds() / 1_000_000
			fmt.Printf("Raw Throughput:    %.2f Mbps\n", rawMbps)
			fmt.Printf("Pool Throughput:   %.2f Mbps\n", poolMbps)
		}
	}
}
