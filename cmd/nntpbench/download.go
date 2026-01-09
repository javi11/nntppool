package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/javi11/nntppool/v3"
)

// DownloadResult holds results from a real download test.
type DownloadResult struct {
	WarmupTime       time.Duration
	TotalTime        time.Duration
	BytesTransferred int64
	SegmentsTotal    int
	SegmentsSuccess  int
	SegmentsFailed   int
	Errors           []string
	Concurrency      int
}

// ProgressTracker tracks download progress and displays it.
type ProgressTracker struct {
	totalBytes     int64
	downloadedBytes atomic.Int64
	startTime      time.Time
	segments       int
	completed      atomic.Int32
	failed         atomic.Int32
	mu             sync.Mutex
	lastPrint      time.Time
	done           bool
}

// NewProgressTracker creates a new progress tracker.
func NewProgressTracker(totalBytes int64, segments int) *ProgressTracker {
	return &ProgressTracker{
		totalBytes: totalBytes,
		segments:   segments,
	}
}

// Start begins progress tracking.
func (p *ProgressTracker) Start() {
	p.startTime = time.Now()
	p.lastPrint = time.Now()
}

// Add adds downloaded bytes and optionally prints progress.
func (p *ProgressTracker) Add(bytes int64, success bool) {
	p.downloadedBytes.Add(bytes)
	if success {
		p.completed.Add(1)
	} else {
		p.failed.Add(1)
	}

	// Rate limit progress updates to every 100ms
	p.mu.Lock()
	if time.Since(p.lastPrint) > 100*time.Millisecond && !p.done {
		p.printProgress()
		p.lastPrint = time.Now()
	}
	p.mu.Unlock()
}

func (p *ProgressTracker) printProgress() {
	downloaded := p.downloadedBytes.Load()
	elapsed := time.Since(p.startTime)
	completed := p.completed.Load()
	failed := p.failed.Load()

	// Calculate speed in Mbps
	var speedMbps float64
	if elapsed.Seconds() > 0 {
		speedMbps = float64(downloaded) * 8 / elapsed.Seconds() / 1_000_000
	}

	// Calculate progress percentage
	var pct float64
	if p.totalBytes > 0 {
		pct = float64(downloaded) / float64(p.totalBytes) * 100
	}

	// Calculate ETA
	var eta string
	if speedMbps > 0 && p.totalBytes > 0 {
		remainingBytes := p.totalBytes - downloaded
		remainingSec := float64(remainingBytes) * 8 / (speedMbps * 1_000_000)
		if remainingSec < 60 {
			eta = fmt.Sprintf("%.0fs", remainingSec)
		} else if remainingSec < 3600 {
			eta = fmt.Sprintf("%.1fm", remainingSec/60)
		} else {
			eta = fmt.Sprintf("%.1fh", remainingSec/3600)
		}
	} else {
		eta = "---"
	}

	// Progress bar
	barWidth := 30
	filled := int(pct / 100 * float64(barWidth))
	if filled > barWidth {
		filled = barWidth
	}
	bar := ""
	for i := 0; i < barWidth; i++ {
		if i < filled {
			bar += "█"
		} else {
			bar += "░"
		}
	}

	// Print progress line (overwrite previous)
	fmt.Printf("\r[%s] %5.1f%% | %.2f/%.2f MB | %6.1f Mbps | %d/%d segs | ETA: %s   ",
		bar,
		pct,
		float64(downloaded)/1_000_000,
		float64(p.totalBytes)/1_000_000,
		speedMbps,
		completed+failed,
		p.segments,
		eta,
	)
}

// Finish prints final progress and newline.
func (p *ProgressTracker) Finish() {
	p.mu.Lock()
	p.done = true
	p.printProgress()
	p.mu.Unlock()
	fmt.Println() // Newline after progress
}

// DownloadBench performs a real download benchmark with progress display.
func DownloadBench(ctx context.Context, cfg BenchConfig, segments []SegmentInfo) (*DownloadResult, error) {
	result := &DownloadResult{
		Concurrency:   cfg.Connections,
		SegmentsTotal: len(segments),
	}

	// Calculate total bytes
	totalBytes := TotalBytes(segments)
	fmt.Printf("\nDownloading %d segments (%.2f MB) with %d connections...\n\n",
		len(segments), float64(totalBytes)/1_000_000, cfg.Connections)

	// Create pool config
	providerCfg := nntppool.ProviderConfig{
		Name:            cfg.Host,
		Host:            cfg.Host,
		Port:            cfg.Port,
		TLS:             cfg.TLS,
		Username:        cfg.Username,
		Password:        cfg.Password,
		MaxConnections:  cfg.Connections,
		InflightPerConn: 12, // SABnzbd recommends 8-15 for high latency connections
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

	// Warmup: select group if available
	if len(segments) > 0 && len(segments[0].Groups) > 0 {
		_, _ = pool.Group(ctx, segments[0].Groups[0])
	}
	result.WarmupTime = time.Since(warmupStart)

	// Create progress tracker
	progress := NewProgressTracker(totalBytes, len(segments))
	progress.Start()

	// Run downloads
	totalStart := time.Now()

	var mu sync.Mutex
	var wg sync.WaitGroup
	var bytesTotal atomic.Int64
	var successCount atomic.Int32
	var failCount atomic.Int32

	// Use a semaphore to limit concurrency
	sem := make(chan struct{}, cfg.Connections)

	for i := 0; i < len(segments); i++ {
		select {
		case <-ctx.Done():
			progress.Finish()
			return result, ctx.Err()
		default:
		}

		seg := segments[i]
		wg.Add(1)
		sem <- struct{}{} // Acquire

		go func(seg SegmentInfo, idx int) {
			defer wg.Done()
			defer func() { <-sem }() // Release

			n, err := pool.Body(ctx, seg.MessageID, io.Discard)

			if err != nil {
				failCount.Add(1)
				mu.Lock()
				result.Errors = append(result.Errors, fmt.Sprintf("segment %d: %v", idx, err))
				mu.Unlock()
				progress.Add(seg.Bytes, false) // Use expected bytes for progress
			} else {
				successCount.Add(1)
				bytesTotal.Add(n)
				progress.Add(n, true)
			}
		}(seg, i)
	}

	wg.Wait()
	progress.Finish()

	result.TotalTime = time.Since(totalStart)
	result.BytesTransferred = bytesTotal.Load()
	result.SegmentsSuccess = int(successCount.Load())
	result.SegmentsFailed = int(failCount.Load())

	return result, nil
}

// PrintDownloadResult prints the download result summary.
func PrintDownloadResult(r *DownloadResult) {
	fmt.Println("\n=== Download Complete ===")
	fmt.Printf("Connections:     %d\n", r.Concurrency)
	fmt.Printf("Warmup Time:     %v\n", r.WarmupTime.Round(time.Millisecond))
	fmt.Printf("Download Time:   %v\n", r.TotalTime.Round(time.Millisecond))

	fmt.Printf("\nSegments:\n")
	fmt.Printf("  Total:         %d\n", r.SegmentsTotal)
	fmt.Printf("  Success:       %d\n", r.SegmentsSuccess)
	if r.SegmentsFailed > 0 {
		fmt.Printf("  Failed:        %d\n", r.SegmentsFailed)
	}

	fmt.Printf("\nData:\n")
	fmt.Printf("  Downloaded:    %.2f MB\n", float64(r.BytesTransferred)/1_000_000)

	if r.TotalTime > 0 && r.BytesTransferred > 0 {
		mbps := float64(r.BytesTransferred) * 8 / r.TotalTime.Seconds() / 1_000_000
		mbytes := float64(r.BytesTransferred) / r.TotalTime.Seconds() / 1_000_000
		fmt.Printf("  Throughput:    %.2f Mbps (%.2f MB/s)\n", mbps, mbytes)
	}

	if len(r.Errors) > 0 {
		fmt.Printf("\nErrors (%d):\n", len(r.Errors))
		for i, e := range r.Errors {
			if i >= 10 {
				fmt.Printf("  ... and %d more\n", len(r.Errors)-10)
				break
			}
			fmt.Printf("  - %s\n", e)
		}
	}

	// Write to stderr if there were failures
	if r.SegmentsFailed > 0 {
		fmt.Fprintf(os.Stderr, "\nWarning: %d segments failed to download\n", r.SegmentsFailed)
	}
}
