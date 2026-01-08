// Package main provides a benchmark tool for comparing raw NNTP provider latency
// against nntppool library overhead.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"
)

// BenchConfig holds benchmark configuration.
type BenchConfig struct {
	Host        string
	Port        int
	Username    string
	Password    string
	TLS         bool
	Connections int
}

func main() {
	// Flags
	host := flag.String("host", "", "NNTP server host")
	port := flag.Int("port", 119, "NNTP server port")
	user := flag.String("user", "", "NNTP username")
	pass := flag.String("pass", "", "NNTP password")
	useTLS := flag.Bool("tls", false, "Use TLS")
	connections := flag.Int("connections", 50, "Number of connections for pool test")

	nzbPath := flag.String("nzb", "", "Path to NZB file")
	messageID := flag.String("message-id", "", "Single message ID to test")
	group := flag.String("group", "", "Newsgroup to select")

	limit := flag.Int("limit", 10, "Number of articles to test (0 = all)")
	rawOnly := flag.Bool("raw", false, "Only run raw connection test")
	poolOnly := flag.Bool("pool", false, "Only run pool test")
	sequential := flag.Bool("sequential", false, "Run pool test sequentially (1 connection)")
	detailed := flag.Bool("detailed", false, "Show detailed single-body breakdown")
	download := flag.Bool("download", false, "Real download test with progress bar")

	timeout := flag.Duration("timeout", 5*time.Minute, "Overall timeout")

	flag.Parse()

	// Validate
	if *host == "" {
		fmt.Fprintln(os.Stderr, "Error: --host is required")
		flag.Usage()
		os.Exit(1)
	}

	if *nzbPath == "" && *messageID == "" {
		fmt.Fprintln(os.Stderr, "Error: --nzb or --message-id is required")
		flag.Usage()
		os.Exit(1)
	}

	cfg := BenchConfig{
		Host:        *host,
		Port:        *port,
		Username:    *user,
		Password:    *pass,
		TLS:         *useTLS,
		Connections: *connections,
	}

	// Get segments from NZB or create single segment
	var segments []SegmentInfo
	if *nzbPath != "" {
		var err error
		segments, err = ParseNZB(*nzbPath)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error parsing NZB: %v\n", err)
			os.Exit(1)
		}
		fmt.Printf("Loaded %d segments from NZB (%.2f MB total)\n", len(segments), float64(TotalBytes(segments))/1_000_000)
	} else {
		segments = []SegmentInfo{
			{
				MessageID: *messageID,
				Bytes:     0, // Unknown
				Groups:    nil,
			},
		}
		if *group != "" {
			segments[0].Groups = []string{*group}
		}
	}

	// Context with timeout and cancellation
	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()

	// Handle signals
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		fmt.Println("\nInterrupted, shutting down...")
		cancel()
	}()

	fmt.Printf("\nBenchmarking %s:%d\n", cfg.Host, cfg.Port)
	fmt.Printf("TLS: %v, Connections: %d\n", cfg.TLS, cfg.Connections)

	// Download mode with progress bar
	if *download {
		result, err := DownloadBench(ctx, cfg, segments)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Download error: %v\n", err)
			os.Exit(1)
		}
		PrintDownloadResult(result)
		fmt.Println("\nDownload benchmark complete.")
		return
	}

	// Detailed single-body breakdown
	if *detailed && len(segments) > 0 {
		seg := segments[0]
		grp := ""
		if len(seg.Groups) > 0 {
			grp = seg.Groups[0]
		}
		result, err := SingleBodyBench(cfg, seg.MessageID, grp)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Single body bench error: %v\n", err)
		} else {
			PrintSingleBodyResult(result)
		}
	}

	// Raw connection test
	if !*poolOnly {
		fmt.Println("\nRunning raw connection test...")
		rawResult, err := RawBench(cfg, segments, *limit)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Raw bench error: %v\n", err)
		} else {
			PrintRawResult(rawResult)

			// Pool test for comparison
			if !*rawOnly {
				fmt.Println("\nRunning pool test...")
				var poolResult *PoolBenchResult
				if *sequential {
					poolResult, err = PoolBenchSequential(ctx, cfg, segments, *limit)
				} else {
					poolResult, err = PoolBench(ctx, cfg, segments, *limit)
				}
				if err != nil {
					fmt.Fprintf(os.Stderr, "Pool bench error: %v\n", err)
				} else {
					label := "Pool Test"
					if *sequential {
						label = "Pool Test (Sequential)"
					}
					PrintPoolResult(poolResult, label)
					CompareResults(rawResult, poolResult)
				}
			}
		}
	} else {
		// Pool only
		fmt.Println("\nRunning pool test...")
		var poolResult *PoolBenchResult
		var err error
		if *sequential {
			poolResult, err = PoolBenchSequential(ctx, cfg, segments, *limit)
		} else {
			poolResult, err = PoolBench(ctx, cfg, segments, *limit)
		}
		if err != nil {
			fmt.Fprintf(os.Stderr, "Pool bench error: %v\n", err)
			os.Exit(1)
		}
		label := "Pool Test"
		if *sequential {
			label = "Pool Test (Sequential)"
		}
		PrintPoolResult(poolResult, label)
	}

	fmt.Println("\nBenchmark complete.")
}
