package main

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/javi11/nntppool/v3"
	"github.com/javi11/nzbparser"
	"github.com/schollz/progressbar/v3"
	"github.com/spf13/pflag"
)

func main() {
	var (
		host        string
		port        int
		username    string
		password    string
		nzbPath     string
		connections int
		output      string
	)

	pflag.StringVar(&host, "host", "", "NNTP server hostname")
	pflag.IntVar(&port, "port", 563, "NNTP server port")
	pflag.StringVar(&username, "user", "", "Username")
	pflag.StringVar(&password, "pass", "", "Password")
	pflag.StringVar(&nzbPath, "nzb", "", "Path to the NZB file")
	pflag.IntVar(&connections, "connections", 10, "Number of concurrent connections")
	pflag.StringVarP(&output, "output", "o", "", "Output directory path")
	pflag.Parse()

	if host == "" || nzbPath == "" {
		pflag.Usage()
		return
	}

	// Start pprof server for memory profiling:
	//   go tool pprof http://localhost:6060/debug/pprof/heap
	go func() { log.Println(http.ListenAndServe("localhost:6060", nil)) }()

	log.Printf("Parsing NZB file: %s", nzbPath)
	fileData, err := os.Open(nzbPath)
	if err != nil {
		log.Fatalf("Failed to open NZB file: %v", err)
	}
	defer func() {
		if err := fileData.Close(); err != nil {
			log.Printf("Failed to close NZB file: %v", err)
		}
	}()

	nzb, err := nzbparser.Parse(fileData)
	if err != nil {
		log.Fatalf("Failed to parse NZB file: %v", err)
	}

	log.Printf("NZB Parsed. Files: %d", len(nzb.Files))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	auth := nntppool.Auth{
		Username: username,
		Password: password,
	}

	addr := fmt.Sprintf("%s:%d", host, port)
	tlsConfig := &tls.Config{InsecureSkipVerify: true} // Usually needed for NNTP/S

	// Create client with connections and some inflight buffer
	log.Printf("Connecting to %s with %d connections...", addr, connections)
	provider, err := nntppool.NewProvider(ctx, nntppool.ProviderConfig{
		Address:               addr,
		MaxConnections:        connections,
		InflightPerConnection: connections * 2,
		Auth:                  auth,
		TLSConfig:             tlsConfig,
		MaxConnIdleTime:       30 * time.Second,
		MaxConnLifetime:       30 * time.Second,
	})
	if err != nil {
		log.Fatalf("Failed to create provider: %v", err)
	}

	client := nntppool.NewClient()
	err = client.AddProvider(provider, nntppool.ProviderPrimary)
	if err != nil {
		log.Fatalf("Failed to add provider: %v", err)
	}
	defer client.Close()

	log.Println("Connected. Starting download...")

	if output != "" {
		if err := os.MkdirAll(output, 0755); err != nil {
			log.Fatalf("Failed to create output directory: %v", err)
		}
	}

	count := 0
	startTime := time.Now()

	var totalBytes int64
	for _, file := range nzb.Files {
		for _, segment := range file.Segments {
			totalBytes += int64(segment.Bytes)
		}
	}
	bar := progressbar.DefaultBytes(
		totalBytes,
		"Downloading",
	)

	// Start metrics display
	stopMetrics := make(chan struct{})
	go func() {
		ticker := time.NewTicker(500 * time.Millisecond)
		defer ticker.Stop()

		for {
			select {
			case <-stopMetrics:
				return
			case <-ticker.C:
				metrics := client.Metrics()
				var parts []string
				for host, m := range metrics {
					parts = append(parts, fmt.Sprintf("[%s: %d conn, %.1f MB/s]", host, m.ActiveConnections, m.ThroughputMB))
				}
				if len(parts) > 0 {
					bar.Describe(strings.Join(parts, " "))
				}
			}
		}
	}()

	type pending struct {
		ch      <-chan nntppool.Response
		msgID   string
		segSize int64
	}

	for _, file := range nzb.Files {
		fmt.Printf("Downloading file: %s\n", file.Filename)
		fmt.Printf("Output directory: %s\n", output)

		var f *os.File
		if output != "" {
			fname := filepath.Base(file.Filename)
			if fname == "." || fname == "/" {
				fname = fmt.Sprintf("file_%d.bin", time.Now().UnixNano())
			}
			filePath := filepath.Join(output, fname)

			var err error
			f, err = os.OpenFile(filePath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
			if err != nil {
				log.Printf("Failed to open output file %s: %v", filePath, err)
			}
		}

		// Fire all segments for this file via BodyAsync.
		// The pool's reqCh + inflightSem provide natural backpressure.
		// *os.File implements io.WriterAt, so sendRequest uses WriteAt for
		// correct concurrent segment writes at yEnc-specified offsets.
		segments := make([]pending, 0, len(file.Segments))
		for _, segment := range file.Segments {
			count++
			var w io.Writer
			if f != nil {
				w = f
			} else {
				w = io.Discard
			}
			ch := client.BodyAsync(ctx, segment.ID, w)
			segments = append(segments, pending{ch: ch, msgID: segment.ID, segSize: int64(segment.Bytes)})
		}

		// Drain responses
		for _, seg := range segments {
			resp := <-seg.ch
			if resp.Err != nil {
				fmt.Printf("Error downloading %s: %v\n", seg.msgID, resp.Err)
			}
			_ = bar.Add(int(seg.segSize))
		}

		if f != nil {
			if err := f.Close(); err != nil {
				log.Printf("Failed to close file: %v", err)
			}
		}
	}

	close(stopMetrics)

	duration := time.Since(startTime)
	log.Printf("Downloaded %d segments in %v", count, duration)
	if duration.Seconds() > 0 {
		log.Printf("Rate: %.2f segments/sec", float64(count)/duration.Seconds())
	}
}
