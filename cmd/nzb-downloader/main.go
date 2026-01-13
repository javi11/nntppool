package main

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"log"
	"os"
	"sync"
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
	)

	pflag.StringVar(&host, "host", "", "NNTP server hostname")
	pflag.IntVar(&port, "port", 563, "NNTP server port")
	pflag.StringVar(&username, "user", "", "Username")
	pflag.StringVar(&password, "pass", "", "Password")
	pflag.StringVar(&nzbPath, "nzb", "", "Path to the NZB file")
	pflag.IntVar(&connections, "connections", 10, "Number of concurrent connections")
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
	defer fileData.Close()

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
		InflightPerConnection: 20,
		Auth:                  auth,
		TLSConfig:             tlsConfig,
	})
	if err != nil {
		log.Fatalf("Failed to create provider: %v", err)
	}

	client := nntppool.NewClient(0)
	client.AddProvider(provider, nntppool.ProviderPrimary)
	defer client.Close()

	log.Println("Connected. Starting download...")

	var wg sync.WaitGroup
	count := 0
	startTime := time.Now()

	// Wait group for all downloads
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

	// Semaphore to limit concurrency to avoid high memory usage
	// Using connections * 2 to keep the pipeline full
	sem := make(chan struct{}, connections*3)

	// Iterate and send
	for _, file := range nzb.Files {
		for _, segment := range file.Segments {
			wg.Add(1)
			count++

			// Acquire semaphore
			sem <- struct{}{}

			go func(msgID string, segSize int64) {
				defer wg.Done()
				defer func() { <-sem }() // Release semaphore

				// Use the high-level Body method which blocks until complete
				err := client.Body(ctx, msgID, io.Discard)
				if err != nil {
					// fmt.Printf("Error downloading %s: %v\n", msgID, err)
				}
				_ = bar.Add(int(segSize))
			}(segment.ID, int64(segment.Bytes))
		}
	}

	log.Println("Waiting for downloads to complete...")
	wg.Wait()

	duration := time.Since(startTime)
	log.Printf("Downloaded %d segments in %v", count, duration)
	if duration.Seconds() > 0 {
		log.Printf("Rate: %.2f segments/sec", float64(count)/duration.Seconds())
	}
}
