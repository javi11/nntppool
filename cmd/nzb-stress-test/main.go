package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"crypto/tls"

	"github.com/avast/retry-go/v4"
	"github.com/javi11/nntppool/v3"
	"github.com/javi11/nzbparser"
	"github.com/sourcegraph/conc/pool"
	"github.com/spf13/pflag"
)

// Simulates the UsenetReader pattern with pipes and early cancellation
func main() {
	var (
		host           string
		port           int
		username       string
		password       string
		nzbPath        string
		connections    int
		workers        int
		cancelPercent  int
		readPartial    bool
		maxLifetime    time.Duration
	)

	pflag.StringVar(&host, "host", "", "NNTP server hostname")
	pflag.IntVar(&port, "port", 563, "NNTP server port")
	pflag.StringVar(&username, "user", "", "Username")
	pflag.StringVar(&password, "pass", "", "Password")
	pflag.StringVar(&nzbPath, "nzb", "", "Path to the NZB file")
	pflag.IntVar(&connections, "connections", 10, "Number of concurrent connections")
	pflag.IntVar(&workers, "workers", 15, "Number of download workers")
	pflag.IntVar(&cancelPercent, "cancel-percent", 10, "Percentage of downloads to cancel early (0-100)")
	pflag.BoolVar(&readPartial, "read-partial", false, "Read only partial data from pipes (simulates early close)")
	pflag.DurationVar(&maxLifetime, "max-lifetime", 30*time.Second, "Max connection lifetime")
	pflag.Parse()

	if host == "" || nzbPath == "" {
		pflag.Usage()
		return
	}

	log.Printf("Parsing NZB file: %s", nzbPath)
	fileData, err := os.Open(nzbPath)
	if err != nil {
		log.Fatalf("Failed to open NZB file: %v", err)
	}
	defer func() { _ = fileData.Close() }()

	nzb, err := nzbparser.Parse(fileData)
	if err != nil {
		log.Fatalf("Failed to parse NZB file: %v", err)
	}

	// Collect all segments
	type segmentInfo struct {
		ID    string
		Index int
		Size  int
	}
	var segments []segmentInfo
	idx := 0
	for _, file := range nzb.Files {
		for _, seg := range file.Segments {
			segments = append(segments, segmentInfo{
				ID:    seg.ID,
				Index: idx,
				Size:  seg.Bytes,
			})
			idx++
		}
	}
	log.Printf("Total segments: %d", len(segments))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Create provider
	addr := fmt.Sprintf("%s:%d", host, port)
	tlsConfig := &tls.Config{InsecureSkipVerify: true}

	log.Printf("Connecting to %s with %d connections, %d workers, max lifetime %v...", addr, connections, workers, maxLifetime)
	provider, err := nntppool.NewProvider(ctx, nntppool.ProviderConfig{
		Address:               addr,
		MaxConnections:        connections,
		InflightPerConnection: connections * 2,
		Auth: nntppool.Auth{
			Username: username,
			Password: password,
		},
		TLSConfig:       tlsConfig,
		MaxConnIdleTime: 30 * time.Second,
		MaxConnLifetime: maxLifetime,
	})
	if err != nil {
		log.Fatalf("Failed to create provider: %v", err)
	}

	client := nntppool.NewClient()
	if err := client.AddProvider(provider, nntppool.ProviderPrimary); err != nil {
		log.Fatalf("Failed to add provider: %v", err)
	}
	defer client.Close()

	log.Println("Connected. Starting stress test...")

	// Stats
	var (
		successCount    int64
		cancelledCount  int64
		errorCount      int64
		retryCount      int64
		totalBytes      int64
		providerClosed  int64
		contextCanceled int64
	)

	startTime := time.Now()

	// Worker pool like UsenetReader
	p := pool.New().
		WithMaxGoroutines(workers).
		WithContext(ctx)

	// Download segments with pipe pattern
	for _, seg := range segments {
		seg := seg // capture

		p.Go(func(c context.Context) error {
			// Create pipe like UsenetReader does
			pr, pw := io.Pipe()

			// Context for this segment (may be cancelled early)
			segCtx, segCancel := context.WithCancel(c)
			defer segCancel()

			// Simulate early cancellation for some segments
			shouldCancel := cancelPercent > 0 && (seg.Index%100) < cancelPercent

			var wg sync.WaitGroup
			var downloadErr error

			// Download goroutine
			wg.Add(1)
			go func() {
				defer wg.Done()
				defer func() { _ = pw.Close() }()

				downloadErr = downloadWithRetry(segCtx, client, seg.ID, pw, func() {
					atomic.AddInt64(&retryCount, 1)
				})
			}()

			// Reader goroutine (simulates UsenetReader.Read)
			wg.Add(1)
			go func() {
				defer wg.Done()

				buf := make([]byte, 32*1024)
				bytesRead := 0

				for {
					n, err := pr.Read(buf)
					bytesRead += n
					atomic.AddInt64(&totalBytes, int64(n))

					if err != nil {
						if err == io.EOF {
							break
						}
						// Pipe closed or error
						break
					}

					// Simulate early close (reader doesn't need all data)
					if shouldCancel && bytesRead > 1024 {
						segCancel()    // Cancel the download context
						_ = pr.Close() // Close reader pipe
						break
					}

					// Partial read mode - close after reading some data
					if readPartial && bytesRead > seg.Size/2 {
						segCancel()
						_ = pr.Close()
						break
					}
				}
			}()

			wg.Wait()

			// Check result
			if downloadErr != nil {
				if errors.Is(downloadErr, context.Canceled) {
					atomic.AddInt64(&contextCanceled, 1)
					atomic.AddInt64(&cancelledCount, 1)
				} else if errors.Is(downloadErr, nntppool.ErrProviderClosed) {
					atomic.AddInt64(&providerClosed, 1)
					atomic.AddInt64(&errorCount, 1)
				} else if isProviderClosedUnexpectedly(downloadErr) {
					atomic.AddInt64(&providerClosed, 1)
					atomic.AddInt64(&errorCount, 1)
					log.Printf("UNEXPECTED CLOSE: segment %d: %v", seg.Index, downloadErr)
				} else {
					atomic.AddInt64(&errorCount, 1)
					log.Printf("ERROR: segment %d: %v", seg.Index, downloadErr)
				}
			} else if shouldCancel || readPartial {
				atomic.AddInt64(&cancelledCount, 1)
			} else {
				atomic.AddInt64(&successCount, 1)
			}

			return nil
		})
	}

	// Wait for all downloads
	if err := p.Wait(); err != nil {
		log.Printf("Pool error: %v", err)
	}

	duration := time.Since(startTime)

	// Print stats
	log.Println("=== STRESS TEST RESULTS ===")
	log.Printf("Duration: %v", duration)
	log.Printf("Total segments: %d", len(segments))
	log.Printf("Successful: %d", atomic.LoadInt64(&successCount))
	log.Printf("Cancelled (expected): %d", atomic.LoadInt64(&cancelledCount))
	log.Printf("Errors: %d", atomic.LoadInt64(&errorCount))
	log.Printf("  - Provider closed unexpectedly: %d", atomic.LoadInt64(&providerClosed))
	log.Printf("  - Context canceled: %d", atomic.LoadInt64(&contextCanceled))
	log.Printf("Total retries: %d", atomic.LoadInt64(&retryCount))
	log.Printf("Total bytes: %.2f MB", float64(atomic.LoadInt64(&totalBytes))/1024/1024)
	if duration.Seconds() > 0 {
		log.Printf("Throughput: %.2f MB/s", float64(atomic.LoadInt64(&totalBytes))/1024/1024/duration.Seconds())
	}

	if atomic.LoadInt64(&providerClosed) > 0 {
		log.Println("\n⚠️  PROBLEM: 'provider closed unexpectedly' errors detected!")
		log.Println("   This indicates channels being closed without sending a response.")
	} else {
		log.Println("\n✅ No 'provider closed unexpectedly' errors!")
	}
}

func downloadWithRetry(ctx context.Context, client *nntppool.Client, msgID string, w io.Writer, onRetry func()) error {
	return retry.Do(
		func() error {
			err := client.Body(ctx, msgID, w)
			if err != nil {
				if errors.Is(err, io.ErrClosedPipe) {
					return nil // Pipe closed by reader, not an error
				}
				return err
			}
			return nil
		},
		retry.Attempts(5),
		retry.Delay(50*time.Millisecond),
		retry.MaxDelay(2*time.Second),
		retry.DelayType(retry.BackOffDelay),
		retry.RetryIf(func(err error) bool {
			if errors.Is(err, context.Canceled) || ctx.Err() != nil {
				return false
			}
			// Check for article not found
			var articleErr *nntppool.ArticleNotFoundError
			return !errors.As(err, &articleErr)
		}),
		retry.OnRetry(func(n uint, err error) {
			onRetry()
			log.Printf("Retry %d for %s: %v", n+1, msgID, err)
		}),
		retry.Context(ctx),
	)
}

func isProviderClosedUnexpectedly(err error) bool {
	if err == nil {
		return false
	}
	errStr := err.Error()
	return errStr == "provider closed unexpectedly" ||
		errStr == "response channel closed"
}
