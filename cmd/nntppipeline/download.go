package main

import (
	"context"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/javi11/nntppool/v3/pkg/nntpcli"
)

// BenchConfig holds benchmark configuration.
type BenchConfig struct {
	Host          string
	Port          int
	Username      string
	Password      string
	TLS           bool
	InsecureSSL   bool
	Connections   int  // Number of parallel connections
	PipelineDepth int  // Pipeline depth for BodyPipelined (0 = disabled)
	TestPipeline  bool // Test pipeline support before downloading
}

// DownloadResult holds results from a real download test.
type DownloadResult struct {
	ConnectTime      time.Duration
	TotalTime        time.Duration
	BytesTransferred int64
	SegmentsTotal    int
	SegmentsSuccess  int
	SegmentsFailed   int
	Errors           []string
	Connections      int  // Number of connections used
	PipelineDepth    int  // 0 = no pipelining
	Pipelined        bool // true if using BodyPipelined
}

// ProgressTracker tracks download progress and displays it.
type ProgressTracker struct {
	totalBytes      int64
	downloadedBytes atomic.Int64
	startTime       time.Time
	segments        int
	completed       atomic.Int32
	failed          atomic.Int32
	mu              sync.Mutex
	lastPrint       time.Time
	done            bool
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

// AddBytes adds downloaded bytes and optionally prints progress.
func (p *ProgressTracker) AddBytes(bytes int64) {
	p.downloadedBytes.Add(bytes)
	p.checkPrint()
}

// SegmentDone marks a segment as completed or failed and optionally prints progress.
func (p *ProgressTracker) SegmentDone(success bool) {
	if success {
		p.completed.Add(1)
	} else {
		p.failed.Add(1)
	}
	p.checkPrint()
}

func (p *ProgressTracker) checkPrint() {
	// Rate limit progress updates to every 100ms
	p.mu.Lock()
	if time.Since(p.lastPrint) > 100*time.Millisecond && !p.done {
		p.printProgress()
		p.lastPrint = time.Now()
	}
	p.mu.Unlock()
}

// ProgressWriter wraps io.Writer to track progress.
type ProgressWriter struct {
	tracker *ProgressTracker
}

func (pw *ProgressWriter) Write(p []byte) (int, error) {
	n := len(p)
	if n > 0 {
		pw.tracker.AddBytes(int64(n))
	}
	return n, nil
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
			bar += "="
		} else {
			bar += "-"
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

// createConnections creates N authenticated connections to the NNTP server.
func createConnections(ctx context.Context, cfg BenchConfig, n int) ([]nntpcli.Connection, error) {
	client := nntpcli.New(nntpcli.Config{
		OperationTimeout: 30 * time.Second,
	})

	conns := make([]nntpcli.Connection, 0, n)
	for i := 0; i < n; i++ {
		var conn nntpcli.Connection
		var err error
		if cfg.TLS {
			conn, err = client.DialTLS(ctx, cfg.Host, cfg.Port, cfg.InsecureSSL)
		} else {
			conn, err = client.Dial(ctx, cfg.Host, cfg.Port)
		}
		if err != nil {
			// Close already created connections
			closeConnections(conns)
			return nil, fmt.Errorf("connect (conn %d): %w", i+1, err)
		}

		// Authenticate if credentials provided
		if cfg.Username != "" {
			if err := conn.Authenticate(cfg.Username, cfg.Password); err != nil {
				if conn.NetConn() != nil {
					_ = conn.NetConn().Close()
				}
				_ = conn.Close()
				closeConnections(conns)
				return nil, fmt.Errorf("authenticate (conn %d): %w", i+1, err)
			}
		}

		conns = append(conns, conn)
	}

	return conns, nil
}

// closeConnections closes all connections.
func closeConnections(conns []nntpcli.Connection) {
	for _, c := range conns {
		// Close the underlying connection first to ensure any blocking operations
		// (like BodyDecoded) are interrupted immediately.
		if c.NetConn() != nil {
			_ = c.NetConn().Close()
		}
		_ = c.Close()
	}
}

// DownloadBench performs a download benchmark without pipelining.
// With multiple connections, segments are distributed across connections.
func DownloadBench(ctx context.Context, cfg BenchConfig, segments []SegmentInfo) (*DownloadResult, error) {
	numConns := cfg.Connections
	if numConns <= 0 {
		numConns = 1
	}

	result := &DownloadResult{
		SegmentsTotal: len(segments),
		Connections:   numConns,
	}

	// Calculate total bytes
	totalBytes := TotalBytes(segments)
	fmt.Printf("\nDownloading %d segments (%.2f MB) with %d connection(s)...\n\n",
		len(segments), float64(totalBytes)/1_000_000, numConns)

	// Create connections
	connectStart := time.Now()
	conns, err := createConnections(ctx, cfg, numConns)
	if err != nil {
		return nil, err
	}
	defer closeConnections(conns)

	result.ConnectTime = time.Since(connectStart)

	// Create connection pool
	connPool := make(chan nntpcli.Connection, numConns)
	for _, c := range conns {
		connPool <- c
	}

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

	for i := 0; i < len(segments); i++ {
		select {
		case <-ctx.Done():
			progress.Finish()
			return result, ctx.Err()
		default:
		}

		seg := segments[i]
		wg.Add(1)

		go func(seg SegmentInfo, idx int) {
			defer wg.Done()

			// Acquire connection
			conn := <-connPool
			defer func() { connPool <- conn }()

			// Download segment
			pw := &ProgressWriter{tracker: progress}
			n, err := conn.BodyDecoded(seg.MessageID, pw, 0)
			if err != nil {
				failCount.Add(1)
				mu.Lock()
				result.Errors = append(result.Errors, fmt.Sprintf("%s: %v", seg.MessageID, err))
				mu.Unlock()
				// Fill remaining bytes for progress bar completeness
				if n < seg.Bytes {
					progress.AddBytes(seg.Bytes - n)
				}
				progress.SegmentDone(false)
			} else {
				successCount.Add(1)
				bytesTotal.Add(n)
				progress.SegmentDone(true)
			}
		}(seg, i)
	}

	// Wait for completion or cancellation
	doneCh := make(chan struct{})
	go func() {
		wg.Wait()
		close(doneCh)
	}()

	select {
	case <-ctx.Done():
		// Force close connections immediately to unblock pending IO
		closeConnections(conns)
		progress.Finish()
		return result, ctx.Err()
	case <-doneCh:
		// All done normally
	}

	progress.Finish()

	result.TotalTime = time.Since(totalStart)
	result.BytesTransferred = bytesTotal.Load()
	result.SegmentsSuccess = int(successCount.Load())
	result.SegmentsFailed = int(failCount.Load())

	return result, nil
}

// DownloadBenchPipeline performs a download benchmark using BodyPipelined.
// With multiple connections, segments are distributed across connections,
// and each connection uses pipelining for its batch.
func DownloadBenchPipeline(ctx context.Context, cfg BenchConfig, segments []SegmentInfo) (*DownloadResult, error) {
	numConns := cfg.Connections
	if numConns <= 0 {
		numConns = 1
	}

	result := &DownloadResult{
		SegmentsTotal: len(segments),
		Connections:   numConns,
		PipelineDepth: cfg.PipelineDepth,
		Pipelined:     true,
	}

	// Calculate total bytes
	totalBytes := TotalBytes(segments)
	fmt.Printf("\nDownloading %d segments (%.2f MB) with %d connection(s), pipeline depth %d...\n\n",
		len(segments), float64(totalBytes)/1_000_000, numConns, cfg.PipelineDepth)

	// Create connections
	connectStart := time.Now()
	conns, err := createConnections(ctx, cfg, numConns)
	if err != nil {
		return nil, err
	}
	defer closeConnections(conns)

	result.ConnectTime = time.Since(connectStart)

	// Test pipeline support if requested (using first connection)
	if cfg.TestPipeline && len(segments) > 0 {
		_, _ = fmt.Println("Testing pipeline support...")
		testID := segments[0].MessageID

		supported, suggestedDepth, err := conns[0].TestPipelineSupport(testID)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Pipeline test failed: %v\n", err)
			fmt.Println("Continuing with download anyway...")
		} else if supported {
			fmt.Printf("Pipelining: SUPPORTED (suggested depth: %d)\n\n", suggestedDepth)
		} else {
			fmt.Println("Pipelining: NOT SUPPORTED")
		}
	}

	// Batch size = pipeline depth
	batchSize := cfg.PipelineDepth
	if batchSize <= 0 {
		batchSize = 8
	}

	// Create connection pool
	connPool := make(chan nntpcli.Connection, numConns)
	for _, c := range conns {
		connPool <- c
	}

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

	// Process segments in batches
	for i := 0; i < len(segments); i += batchSize {
		select {
		case <-ctx.Done():
			progress.Finish()
			return result, ctx.Err()
		default:
		}

		end := i + batchSize
		if end > len(segments) {
			end = len(segments)
		}
		batch := segments[i:end]

		wg.Add(1)

		go func(segs []SegmentInfo) {
			defer wg.Done()

			// Acquire connection
			conn := <-connPool
			defer func() { connPool <- conn }()

			// Build pipeline requests
			pw := &ProgressWriter{tracker: progress}
			requests := make([]nntpcli.PipelineRequest, len(segs))
			for j, seg := range segs {
				requests[j] = nntpcli.PipelineRequest{
					MessageID: seg.MessageID,
					Writer:    pw,
					Discard:   0,
				}
			}

			// Execute pipeline
			results := conn.BodyPipelined(requests)

			// Process results
			for j, res := range results {
				if res.Error != nil {
					failCount.Add(1)
					mu.Lock()
					result.Errors = append(result.Errors, fmt.Sprintf("%s: %v", res.MessageID, res.Error))
					mu.Unlock()
					if res.BytesWritten < segs[j].Bytes {
						progress.AddBytes(segs[j].Bytes - res.BytesWritten)
					}
					progress.SegmentDone(false)
				} else {
					successCount.Add(1)
					bytesTotal.Add(res.BytesWritten)
					progress.SegmentDone(true)
				}
			}
		}(batch)
	}

	// Wait for completion or cancellation
	doneCh := make(chan struct{})
	go func() {
		wg.Wait()
		close(doneCh)
	}()

	select {
	case <-ctx.Done():
		// Force close connections immediately to unblock pending IO
		closeConnections(conns)
		progress.Finish()
		return result, ctx.Err()
	case <-doneCh:
		// All done normally
	}

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
	fmt.Printf("Connections:     %d\n", r.Connections)
	if r.Pipelined {
		fmt.Printf("Pipeline Depth:  %d\n", r.PipelineDepth)
	}
	fmt.Printf("Connect Time:    %v\n", r.ConnectTime.Round(time.Millisecond))
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
