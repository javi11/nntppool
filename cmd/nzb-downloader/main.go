package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/javi11/nntppool/v2"
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
		useTLS      bool
		insecureSSL bool
	)

	pflag.StringVar(&host, "host", "", "NNTP server hostname")
	pflag.IntVar(&port, "port", 563, "NNTP server port")
	pflag.StringVar(&username, "user", "", "Username")
	pflag.StringVar(&password, "pass", "", "Password")
	pflag.StringVar(&nzbPath, "nzb", "", "Path to the NZB file")
	pflag.IntVar(&connections, "connections", 10, "Number of concurrent connections")
	pflag.StringVarP(&output, "output", "o", "", "Output directory path")
	pflag.BoolVar(&useTLS, "tls", true, "Use TLS connection")
	pflag.BoolVar(&insecureSSL, "insecure", true, "Skip TLS certificate verification")
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

	// Create provider configuration
	providerConfig := nntppool.UsenetProviderConfig{
		Host:                           host,
		Port:                           port,
		Username:                       username,
		Password:                       password,
		MaxConnections:                 connections,
		MaxConnectionIdleTimeInSeconds: 30,
		MaxConnectionTTLInSeconds:      300,
		TLS:                            useTLS,
		InsecureSSL:                    insecureSSL,
	}

	// Create connection pool
	log.Printf("Connecting to %s:%d with %d connections...", host, port, connections)
	pool, err := nntppool.NewConnectionPool(nntppool.Config{
		Providers:      []nntppool.UsenetProviderConfig{providerConfig},
		MinConnections: connections,
	})
	if err != nil {
		log.Fatalf("Failed to create connection pool: %v", err)
	}
	defer pool.Quit()

	log.Println("Connected. Starting download...")

	if output != "" {
		if err := os.MkdirAll(output, 0755); err != nil {
			log.Fatalf("Failed to create output directory: %v", err)
		}
	}

	var wg sync.WaitGroup
	count := 0
	startTime := time.Now()

	// Calculate total bytes for progress bar
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

		var lastBytes int64
		lastTime := time.Now()

		for {
			select {
			case <-stopMetrics:
				return
			case <-ticker.C:
				snapshot := pool.GetMetricsSnapshot()
				now := time.Now()
				elapsed := now.Sub(lastTime).Seconds()

				var parts []string
				for host, m := range snapshot.ProviderMetrics {
					// Calculate throughput
					bytesDiff := m.BytesDownloaded - lastBytes
					throughputMB := float64(bytesDiff) / elapsed / 1024 / 1024

					parts = append(parts, fmt.Sprintf("[%s: %d/%d conn, %.1f MB/s]",
						host, m.ActiveConnections, m.MaxConnections, throughputMB))

					lastBytes = m.BytesDownloaded
				}
				lastTime = now

				if len(parts) > 0 {
					bar.Describe(strings.Join(parts, " "))
				}
			}
		}
	}()

	// Semaphore to limit concurrency to avoid high memory usage
	sem := make(chan struct{}, connections*3)

	// Iterate and download files
	for _, file := range nzb.Files {
		var outWriter io.WriterAt
		var f *os.File
		var fileWg sync.WaitGroup
		var writeMu sync.Mutex // Protects writes to the file

		fmt.Printf("Downloading file: %s\n", file.Filename)
		fmt.Printf("Output directory: %s\n", output)

		if output != "" {
			// Basic sanitization: take just the filename element to avoid directory traversal
			fname := filepath.Base(file.Filename)
			if fname == "." || fname == "/" {
				// Fallback if filename is weird
				fname = fmt.Sprintf("file_%d.bin", time.Now().UnixNano())
			}
			filePath := filepath.Join(output, fname)

			var err error
			f, err = os.OpenFile(filePath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
			if err != nil {
				log.Printf("Failed to open output file %s: %v", filePath, err)
			} else {
				outWriter = f
			}
		}

		// Track cumulative offset for each segment
		segmentOffsets := make(map[string]int64)
		var currentOffset int64
		for _, segment := range file.Segments {
			segmentOffsets[segment.ID] = currentOffset
			currentOffset += int64(segment.Bytes)
		}

		for _, segment := range file.Segments {
			wg.Add(1)
			fileWg.Add(1)
			count++

			// Acquire semaphore
			sem <- struct{}{}

			go func(msgID string, segSize int64, offset int64, w io.WriterAt) {
				defer wg.Done()
				defer fileWg.Done()
				defer func() { <-sem }() // Release semaphore

				// Use the Body method to download
				if output == "" {
					_, err := pool.Body(ctx, msgID, io.Discard, nil)
					if err != nil {
						fmt.Printf("Error downloading %s: %v\n", msgID, err)
					}
				} else if w != nil {
					// Download to buffer first, then write at position
					buf := &bytes.Buffer{}
					_, err := pool.Body(ctx, msgID, buf, nil)
					if err != nil {
						fmt.Printf("Error downloading %s: %v\n", msgID, err)
					} else {
						// Write to file at the correct offset
						writeMu.Lock()
						_, writeErr := w.WriteAt(buf.Bytes(), offset)
						writeMu.Unlock()
						if writeErr != nil {
							fmt.Printf("Error writing %s: %v\n", msgID, writeErr)
						}
					}
				}
				_ = bar.Add(int(segSize))
			}(segment.ID, int64(segment.Bytes), segmentOffsets[segment.ID], outWriter)
		}

		if f != nil {
			go func(fh *os.File) {
				fileWg.Wait()
				if err := fh.Close(); err != nil {
					log.Printf("Failed to close file: %v", err)
				}
			}(f)
		}
	}

	log.Println("Waiting for downloads to complete...")
	wg.Wait()
	close(stopMetrics)

	duration := time.Since(startTime)
	log.Printf("Downloaded %d segments in %v", count, duration)
	if duration.Seconds() > 0 {
		log.Printf("Rate: %.2f segments/sec", float64(count)/duration.Seconds())
	}
}
