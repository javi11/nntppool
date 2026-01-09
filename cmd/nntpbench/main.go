// Package main provides a benchmark tool for testing NNTP downloads using the nntppool library.
package main

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	// Flags
	host := flag.String("host", "", "NNTP server host (required)")
	port := flag.Int("port", 119, "NNTP server port")
	user := flag.String("user", "", "NNTP username")
	pass := flag.String("pass", "", "NNTP password")
	useTLS := flag.Bool("tls", false, "Use TLS")
	connections := flag.Int("connections", 50, "Number of connections")

	nzbPath := flag.String("nzb", "", "Path to NZB file")
	messageID := flag.String("message-id", "", "Single message ID to test")
	group := flag.String("group", "", "Newsgroup to select")

	limit := flag.Int("limit", 0, "Number of segments to download (0 = all)")
	timeout := flag.Duration("timeout", 5*time.Minute, "Overall timeout")

	pipeline := flag.Bool("pipeline", false, "Enable pipelining mode using BodyBatch")
	pipelineDepth := flag.Int("pipeline-depth", 8, "Pipeline depth (commands in flight per connection)")

	profile := flag.Bool("profile", false, "Enable pprof profiling on :6060")
	profilePort := flag.String("profile-port", "6060", "Port for pprof HTTP server")

	flag.Parse()

	// Start pprof server if profiling is enabled
	if *profile {
		go func() {
			addr := "localhost:" + *profilePort
			fmt.Printf("pprof server listening on http://%s/debug/pprof/\n", addr)
			if err := http.ListenAndServe(addr, nil); err != nil {
				fmt.Fprintf(os.Stderr, "pprof server error: %v\n", err)
			}
		}()
	}

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
		Host:          *host,
		Port:          *port,
		Username:      *user,
		Password:      *pass,
		TLS:           *useTLS,
		Connections:   *connections,
		PipelineDepth: *pipelineDepth,
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

	// Apply limit if specified
	if *limit > 0 && *limit < len(segments) {
		segments = segments[:*limit]
		fmt.Printf("Limited to %d segments\n", *limit)
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
	if *pipeline {
		fmt.Printf("Pipeline: enabled (depth=%d)\n", cfg.PipelineDepth)
	}

	// Run download benchmark
	var result *DownloadResult
	var err error
	if *pipeline {
		result, err = DownloadBenchPipeline(ctx, cfg, segments)
	} else {
		result, err = DownloadBench(ctx, cfg, segments)
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "Download error: %v\n", err)
		os.Exit(1)
	}
	PrintDownloadResult(result)

	fmt.Println("\nBenchmark complete.")
}
