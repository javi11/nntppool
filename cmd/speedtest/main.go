package main

import (
	"context"
	"crypto/tls"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/javi11/nntppool/v4"
	"github.com/javi11/nntppool/v4/cmd/speedtest/nzb"
)

const defaultNZBURL = "https://sabnzbd.org/tests/test_download_10GB.nzb"

// providerFlag implements flag.Value to collect multiple --provider flags.
// Format: "host=news.example.com:563,tls,user=foo,pass=bar,conns=10,inflight=2"
type providerFlag []nntppool.Provider

func (pf *providerFlag) String() string { return fmt.Sprintf("%d providers", len(*pf)) }

func (pf *providerFlag) Set(val string) error {
	p := nntppool.Provider{
		Connections: 10,
		Inflight:    1,
		KeepAlive:   time.Minute,
	}

	for _, part := range strings.Split(val, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}

		k, v, hasEq := strings.Cut(part, "=")
		k = strings.ToLower(k)

		switch k {
		case "host":
			if !hasEq || v == "" {
				return fmt.Errorf("host requires a value")
			}
			p.Host = v
		case "tls":
			if !hasEq || v == "" || v == "true" {
				// Extract hostname for SNI.
				hostname := p.Host
				if idx := strings.LastIndex(hostname, ":"); idx != -1 {
					hostname = hostname[:idx]
				}
				p.TLSConfig = &tls.Config{
					ServerName:         hostname,
					ClientSessionCache: tls.NewLRUClientSessionCache(0),
				}
			}
		case "user":
			if !hasEq {
				return fmt.Errorf("user requires a value")
			}
			p.Auth.Username = v
		case "pass":
			if !hasEq {
				return fmt.Errorf("pass requires a value")
			}
			p.Auth.Password = v
		case "conns":
			n, err := strconv.Atoi(v)
			if err != nil || n <= 0 {
				return fmt.Errorf("conns must be a positive integer")
			}
			p.Connections = n
		case "inflight":
			n, err := strconv.Atoi(v)
			if err != nil || n <= 0 {
				return fmt.Errorf("inflight must be a positive integer")
			}
			p.Inflight = n
		case "backup":
			p.Backup = true
		case "idle":
			if !hasEq || v == "" {
				return fmt.Errorf("idle requires a duration value (e.g. idle=30s)")
			}
			d, err := time.ParseDuration(v)
			if err != nil {
				return fmt.Errorf("idle: %w", err)
			}
			p.IdleTimeout = d
		case "throttle":
			if !hasEq || v == "" {
				return fmt.Errorf("throttle requires a duration value (e.g. throttle=30s)")
			}
			d, err := time.ParseDuration(v)
			if err != nil {
				return fmt.Errorf("throttle: %w", err)
			}
			p.ThrottleRestore = d
		case "keepalive":
			if !hasEq || v == "" {
				return fmt.Errorf("keepalive requires a duration value (e.g. keepalive=60s)")
			}
			d, err := time.ParseDuration(v)
			if err != nil {
				return fmt.Errorf("keepalive: %w", err)
			}
			p.KeepAlive = d
		default:
			return fmt.Errorf("unknown provider option: %s", k)
		}
	}

	// TLS may reference host, so re-check if tls was set before host.
	if p.TLSConfig != nil && p.TLSConfig.ServerName == "" && p.Host != "" {
		hostname := p.Host
		if idx := strings.LastIndex(hostname, ":"); idx != -1 {
			hostname = hostname[:idx]
		}
		p.TLSConfig.ServerName = hostname
	}

	if p.Host == "" {
		return fmt.Errorf("provider must include host=...")
	}

	*pf = append(*pf, p)
	return nil
}

func main() {
	// Legacy single-provider flags.
	host := flag.String("host", "", "NNTP server host:port")
	useTLS := flag.Bool("tls", true, "Use TLS")
	user := flag.String("user", "", "NNTP username")
	pass := flag.String("pass", "", "NNTP password")
	conns := flag.Int("conns", 10, "Number of connections")
	inflight := flag.Int("inflight", 1, "Pipelined requests per connection")

	// Multi-provider flag.
	var providers providerFlag
	flag.Var(&providers, "provider", `Provider spec: "host=h:563,tls,user=u,pass=p,conns=10,inflight=1,idle=30s,throttle=30s,keepalive=60s,backup" (repeatable)`)

	nzbPath := flag.String("nzb", "", "Path or URL to NZB file (default: SABnzbd 1GB test)")
	maxSegs := flag.Int("max-segments", 0, "Limit segments (0 = all)")
	flag.Parse()

	// Build provider list from flags.
	if len(providers) == 0 {
		if *host == "" {
			fmt.Fprintln(os.Stderr, "error: --host or --provider is required")
			flag.Usage()
			os.Exit(1)
		}
		// Legacy mode: build a single provider from individual flags.
		p := nntppool.Provider{
			Host:        *host,
			Auth:        nntppool.Auth{Username: *user, Password: *pass},
			Connections: *conns,
			Inflight:    *inflight,
		}
		if *useTLS {
			hostname := *host
			if idx := strings.LastIndex(hostname, ":"); idx != -1 {
				hostname = hostname[:idx]
			}
			p.TLSConfig = &tls.Config{
				ServerName:         hostname,
				ClientSessionCache: tls.NewLRUClientSessionCache(0),
			}
		}
		providers = append(providers, p)
	}

	// Load NZB.
	nzbSource := *nzbPath
	if nzbSource == "" {
		nzbSource = defaultNZBURL
	}

	segments, err := loadNZB(nzbSource)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error loading NZB: %v\n", err)
		os.Exit(1)
	}

	if *maxSegs > 0 && *maxSegs < len(segments) {
		segments = segments[:*maxSegs]
	}

	// Print configuration.
	fmt.Printf("Segments:  %d\n", len(segments))
	totalConns := 0
	for i, p := range providers {
		tlsStr := "no"
		if p.TLSConfig != nil {
			tlsStr = "yes"
		}
		role := "main"
		if p.Backup {
			role = "backup"
		}
		idleStr := "none"
		if p.IdleTimeout > 0 {
			idleStr = p.IdleTimeout.String()
		}
		keepAliveStr := "default"
		if p.KeepAlive > 0 {
			keepAliveStr = p.KeepAlive.String()
		} else if p.KeepAlive < 0 {
			keepAliveStr = "off"
		}
		fmt.Printf("Provider %d: %s (TLS: %s, conns: %d, inflight: %d, idle: %s, keepalive: %s, %s)\n",
			i+1, p.Host, tlsStr, p.Connections, p.Inflight, idleStr, keepAliveStr, role)
		totalConns += p.Connections
	}
	if len(providers) > 1 {
		fmt.Printf("Total conns: %d (articles not found on one provider will be retried on others)\n", totalConns)
	}
	fmt.Println()

	// Create NNTP client.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	fmt.Printf("Creating client with %d connection slots across %d provider(s)...\n", totalConns, len(providers))
	client, err := nntppool.NewClient(ctx, providers)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error creating client: %v\n", err)
		os.Exit(1)
	}
	defer client.Close()
	fmt.Println("Ready (connections on demand).")
	fmt.Println()

	// Stats — segsDone counts caller-received responses (for ETA),
	// bytesDecoded tracks decoded throughput (not in library metrics).
	var bytesDecoded atomic.Int64
	var segsDone atomic.Int64
	totalSegs := int64(len(segments))

	// Progress reporter.
	progressDone := make(chan struct{})
	go func() {
		defer close(progressDone)
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		var lastConsumed int64
		for {
			select {
			case <-ticker.C:
				stats := client.Stats()
				done := segsDone.Load()
				consumed := int64(stats.AvgSpeed * stats.Elapsed.Seconds())
				elapsed := stats.Elapsed.Seconds()
				speedMBps := float64(consumed-lastConsumed) / 1024 / 1024
				lastConsumed = consumed
				avgMBps := stats.AvgSpeed / 1024 / 1024

				remaining := int64(0)
				if done > 0 {
					remaining = int64(float64(totalSegs-done) / float64(done) * elapsed)
				}

				fmt.Printf("\r\033[K[%5.1fs] %d/%d segs | %.1f MB/s (avg %.1f MB/s) | ETA %ds",
					elapsed, done, totalSegs, speedMBps, avgMBps, remaining)
			case <-ctx.Done():
				return
			}
		}
	}()

	// Fan out: send all segment requests.
	var wg sync.WaitGroup
	respChans := make([]<-chan nntppool.Response, len(segments))

	for i, seg := range segments {
		payload := append(append([]byte("BODY <"), seg.MessageID...), ">\r\n"...)
		respChans[i] = client.Send(ctx, payload, io.Discard)
	}

	// Collect responses.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for _, ch := range respChans {
			resp, ok := <-ch
			if !ok {
				segsDone.Add(1)
				continue
			}
			if resp.Err == nil && resp.StatusCode != 430 && resp.StatusCode != 423 {
				bytesDecoded.Add(int64(resp.Meta.BytesDecoded))
			}
			segsDone.Add(1)
		}
	}()

	wg.Wait()
	cancel() // stop progress reporter
	<-progressDone

	// Final summary.
	stats := client.Stats()
	decoded := bytesDecoded.Load()
	fmt.Printf("\r\033[K")
	fmt.Println("=== Speed Test Results ===")
	fmt.Printf("Time:       %s\n", stats.Elapsed.Round(time.Millisecond))

	var totalMissing, totalErrors int64
	for _, ps := range stats.Providers {
		totalMissing += ps.Missing
		totalErrors += ps.Errors
	}
	fmt.Printf("Segments:   %d done, %d missing, %d errors\n",
		segsDone.Load()-totalMissing-totalErrors, totalMissing, totalErrors)

	consumed := stats.AvgSpeed * stats.Elapsed.Seconds()
	fmt.Printf("Wire:       %.2f MB (%.2f MB/s)\n",
		consumed/1024/1024, stats.AvgSpeed/1024/1024)
	fmt.Printf("Decoded:    %.2f MB (%.2f MB/s)\n",
		float64(decoded)/1024/1024,
		float64(decoded)/1024/1024/stats.Elapsed.Seconds())

	if len(stats.Providers) > 1 {
		fmt.Println()
		fmt.Println("--- Per-Provider ---")
		for _, ps := range stats.Providers {
			fmt.Printf("  %-30s  %6.1f MB/s  conns %d/%d  missing %d  errors %d\n",
				ps.Name, ps.AvgSpeed/1024/1024, ps.ActiveConnections, ps.MaxConnections,
				ps.Missing, ps.Errors)
		}
	}
}

func loadNZB(source string) ([]nzb.Segment, error) {
	var r io.ReadCloser

	if strings.HasPrefix(source, "http://") || strings.HasPrefix(source, "https://") {
		fmt.Printf("Downloading NZB from %s...\n", source)
		resp, err := http.Get(source)
		if err != nil {
			return nil, err
		}
		defer resp.Body.Close()
		if resp.StatusCode != 200 {
			return nil, fmt.Errorf("HTTP %d", resp.StatusCode)
		}
		r = resp.Body
	} else {
		f, err := os.Open(source)
		if err != nil {
			return nil, err
		}
		defer f.Close()
		r = f
	}

	n, err := nzb.Parse(r)
	if err != nil {
		return nil, err
	}

	segments := n.AllSegments()
	if len(segments) == 0 {
		return nil, fmt.Errorf("no segments found in NZB")
	}
	return segments, nil
}
