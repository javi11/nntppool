package main

import (
	"bufio"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"sort"
	"time"
)

// RawBenchResult holds the results of a raw connection benchmark.
type RawBenchResult struct {
	TCPConnectTime   time.Duration
	GreetingTime     time.Duration
	AuthTime         time.Duration
	GroupSelectTime  time.Duration
	BodyTimes        []time.Duration
	BodySizes        []int64
	TotalTime        time.Duration
	BytesTransferred int64
	Errors           []string
}

// Stats returns statistical summary of body download times.
func (r *RawBenchResult) Stats() (avg, p50, p95, p99 time.Duration) {
	if len(r.BodyTimes) == 0 {
		return
	}

	// Calculate average
	var total time.Duration
	for _, t := range r.BodyTimes {
		total += t
	}
	avg = total / time.Duration(len(r.BodyTimes))

	// Sort for percentiles
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

// RawBench performs a raw connection benchmark bypassing the pool.
func RawBench(cfg BenchConfig, segments []SegmentInfo, limit int) (*RawBenchResult, error) {
	result := &RawBenchResult{}
	totalStart := time.Now()

	// TCP Connect
	tcpStart := time.Now()
	addr := net.JoinHostPort(cfg.Host, fmt.Sprintf("%d", cfg.Port))
	var conn net.Conn
	var err error

	if cfg.TLS {
		tlsConfig := &tls.Config{
			ServerName: cfg.Host,
			MinVersion: tls.VersionTLS12,
		}
		conn, err = tls.Dial("tcp", addr, tlsConfig)
	} else {
		conn, err = net.Dial("tcp", addr)
	}
	if err != nil {
		return nil, fmt.Errorf("tcp connect: %w", err)
	}
	defer conn.Close()
	result.TCPConnectTime = time.Since(tcpStart)

	reader := bufio.NewReader(conn)

	// Read greeting
	greetStart := time.Now()
	greeting, err := reader.ReadString('\n')
	if err != nil {
		return nil, fmt.Errorf("read greeting: %w", err)
	}
	result.GreetingTime = time.Since(greetStart)

	// Check greeting status
	if len(greeting) < 3 || (greeting[0] != '2') {
		return nil, fmt.Errorf("bad greeting: %s", greeting)
	}

	// Authentication
	if cfg.Username != "" {
		authStart := time.Now()

		// AUTHINFO USER
		if _, err := fmt.Fprintf(conn, "AUTHINFO USER %s\r\n", cfg.Username); err != nil {
			return nil, fmt.Errorf("send authinfo user: %w", err)
		}
		resp, err := reader.ReadString('\n')
		if err != nil {
			return nil, fmt.Errorf("read authinfo user response: %w", err)
		}

		// Check if password needed (381) or already authenticated (281)
		if len(resp) >= 3 && resp[0] == '3' && resp[1] == '8' && resp[2] == '1' {
			// AUTHINFO PASS
			if _, err := fmt.Fprintf(conn, "AUTHINFO PASS %s\r\n", cfg.Password); err != nil {
				return nil, fmt.Errorf("send authinfo pass: %w", err)
			}
			resp, err = reader.ReadString('\n')
			if err != nil {
				return nil, fmt.Errorf("read authinfo pass response: %w", err)
			}
		}

		if len(resp) < 3 || resp[0] != '2' || resp[1] != '8' || resp[2] != '1' {
			return nil, fmt.Errorf("auth failed: %s", resp)
		}
		result.AuthTime = time.Since(authStart)
	}

	// Select group if segments have groups
	if len(segments) > 0 && len(segments[0].Groups) > 0 {
		groupStart := time.Now()
		if _, err := fmt.Fprintf(conn, "GROUP %s\r\n", segments[0].Groups[0]); err != nil {
			return nil, fmt.Errorf("send group: %w", err)
		}
		resp, err := reader.ReadString('\n')
		if err != nil {
			return nil, fmt.Errorf("read group response: %w", err)
		}
		if len(resp) < 3 || resp[0] != '2' {
			return nil, fmt.Errorf("group failed: %s", resp)
		}
		result.GroupSelectTime = time.Since(groupStart)
	}

	// Download bodies
	count := limit
	if count <= 0 || count > len(segments) {
		count = len(segments)
	}

	for i := 0; i < count; i++ {
		seg := segments[i]
		bodyStart := time.Now()

		// Send BODY command
		if _, err := fmt.Fprintf(conn, "BODY <%s>\r\n", seg.MessageID); err != nil {
			result.Errors = append(result.Errors, fmt.Sprintf("send body %d: %v", i, err))
			continue
		}

		// Read response line
		resp, err := reader.ReadString('\n')
		if err != nil {
			result.Errors = append(result.Errors, fmt.Sprintf("read body response %d: %v", i, err))
			continue
		}

		// Check for success (222)
		if len(resp) < 3 || resp[0] != '2' || resp[1] != '2' || resp[2] != '2' {
			result.Errors = append(result.Errors, fmt.Sprintf("body %d failed: %s", i, resp))
			continue
		}

		// Read multiline response until dot-stuffed end
		var bytesRead int64
		for {
			line, err := reader.ReadBytes('\n')
			if err != nil {
				result.Errors = append(result.Errors, fmt.Sprintf("read body data %d: %v", i, err))
				break
			}
			bytesRead += int64(len(line))

			// Check for end of multiline response
			if len(line) >= 3 && line[0] == '.' && line[1] == '\r' && line[2] == '\n' {
				break
			}
		}

		elapsed := time.Since(bodyStart)
		result.BodyTimes = append(result.BodyTimes, elapsed)
		result.BodySizes = append(result.BodySizes, bytesRead)
		result.BytesTransferred += bytesRead
	}

	result.TotalTime = time.Since(totalStart)
	return result, nil
}

// PrintRawResult prints the raw benchmark result in a formatted way.
func PrintRawResult(r *RawBenchResult) {
	fmt.Println("\n=== Raw Connection Test ===")
	fmt.Printf("TCP Connect:     %v\n", r.TCPConnectTime.Round(time.Millisecond))
	fmt.Printf("NNTP Greeting:   %v\n", r.GreetingTime.Round(time.Millisecond))
	if r.AuthTime > 0 {
		fmt.Printf("Authentication:  %v\n", r.AuthTime.Round(time.Millisecond))
	}
	if r.GroupSelectTime > 0 {
		fmt.Printf("Group Select:    %v\n", r.GroupSelectTime.Round(time.Millisecond))
	}

	if len(r.BodyTimes) > 0 {
		avg, p50, p95, p99 := r.Stats()
		fmt.Printf("\nBody Downloads (%d articles):\n", len(r.BodyTimes))
		fmt.Printf("  Average:       %v\n", avg.Round(time.Millisecond))
		fmt.Printf("  P50:           %v\n", p50.Round(time.Millisecond))
		fmt.Printf("  P95:           %v\n", p95.Round(time.Millisecond))
		fmt.Printf("  P99:           %v\n", p99.Round(time.Millisecond))

		// Calculate effective throughput
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

// SingleBodyBench measures a single BODY command for detailed timing.
func SingleBodyBench(cfg BenchConfig, messageID, group string) (*SingleBodyResult, error) {
	result := &SingleBodyResult{}

	// TCP Connect
	tcpStart := time.Now()
	addr := net.JoinHostPort(cfg.Host, fmt.Sprintf("%d", cfg.Port))
	var conn net.Conn
	var err error

	if cfg.TLS {
		tlsConfig := &tls.Config{
			ServerName: cfg.Host,
			MinVersion: tls.VersionTLS12,
		}
		conn, err = tls.Dial("tcp", addr, tlsConfig)
	} else {
		conn, err = net.Dial("tcp", addr)
	}
	if err != nil {
		return nil, fmt.Errorf("tcp connect: %w", err)
	}
	defer conn.Close()
	result.TCPConnect = time.Since(tcpStart)

	reader := bufio.NewReader(conn)

	// Read greeting
	greetStart := time.Now()
	greeting, err := reader.ReadString('\n')
	if err != nil {
		return nil, fmt.Errorf("read greeting: %w", err)
	}
	result.Greeting = time.Since(greetStart)
	if len(greeting) < 3 || greeting[0] != '2' {
		return nil, fmt.Errorf("bad greeting: %s", greeting)
	}

	// Auth
	if cfg.Username != "" {
		authStart := time.Now()
		if _, err := fmt.Fprintf(conn, "AUTHINFO USER %s\r\n", cfg.Username); err != nil {
			return nil, fmt.Errorf("send authinfo user: %w", err)
		}
		resp, err := reader.ReadString('\n')
		if err != nil {
			return nil, fmt.Errorf("read authinfo user: %w", err)
		}
		if len(resp) >= 3 && resp[0] == '3' {
			if _, err := fmt.Fprintf(conn, "AUTHINFO PASS %s\r\n", cfg.Password); err != nil {
				return nil, fmt.Errorf("send authinfo pass: %w", err)
			}
			resp, err = reader.ReadString('\n')
			if err != nil {
				return nil, fmt.Errorf("read authinfo pass: %w", err)
			}
		}
		if len(resp) < 3 || resp[0] != '2' {
			return nil, fmt.Errorf("auth failed: %s", resp)
		}
		result.Auth = time.Since(authStart)
	}

	// Group
	if group != "" {
		groupStart := time.Now()
		if _, err := fmt.Fprintf(conn, "GROUP %s\r\n", group); err != nil {
			return nil, fmt.Errorf("send group: %w", err)
		}
		resp, err := reader.ReadString('\n')
		if err != nil {
			return nil, fmt.Errorf("read group: %w", err)
		}
		if len(resp) < 3 || resp[0] != '2' {
			return nil, fmt.Errorf("group failed: %s", resp)
		}
		result.Group = time.Since(groupStart)
	}

	// BODY command - send time
	sendStart := time.Now()
	if _, err := fmt.Fprintf(conn, "BODY <%s>\r\n", messageID); err != nil {
		return nil, fmt.Errorf("send body: %w", err)
	}
	result.BodySend = time.Since(sendStart)

	// First byte time (time to first response)
	firstByteStart := time.Now()
	resp, err := reader.ReadString('\n')
	if err != nil {
		return nil, fmt.Errorf("read body response: %w", err)
	}
	result.FirstByte = time.Since(firstByteStart)

	if len(resp) < 3 || resp[0] != '2' || resp[1] != '2' || resp[2] != '2' {
		return nil, fmt.Errorf("body failed: %s", resp)
	}

	// Read body data
	dataStart := time.Now()
	var bytesRead int64
	for {
		line, err := reader.ReadBytes('\n')
		if err != nil && err != io.EOF {
			return nil, fmt.Errorf("read body data: %w", err)
		}
		bytesRead += int64(len(line))
		if len(line) >= 3 && line[0] == '.' && line[1] == '\r' && line[2] == '\n' {
			break
		}
		if err == io.EOF {
			break
		}
	}
	result.DataTransfer = time.Since(dataStart)
	result.BytesRead = bytesRead

	result.Total = result.TCPConnect + result.Greeting + result.Auth + result.Group +
		result.BodySend + result.FirstByte + result.DataTransfer

	return result, nil
}

// SingleBodyResult holds detailed timing for a single BODY command.
type SingleBodyResult struct {
	TCPConnect   time.Duration
	Greeting     time.Duration
	Auth         time.Duration
	Group        time.Duration
	BodySend     time.Duration
	FirstByte    time.Duration
	DataTransfer time.Duration
	Total        time.Duration
	BytesRead    int64
}

// PrintSingleBodyResult prints detailed timing breakdown.
func PrintSingleBodyResult(r *SingleBodyResult) {
	fmt.Println("\n=== Single BODY Breakdown ===")
	fmt.Printf("TCP Connect:     %v\n", r.TCPConnect.Round(time.Millisecond))
	fmt.Printf("NNTP Greeting:   %v\n", r.Greeting.Round(time.Millisecond))
	if r.Auth > 0 {
		fmt.Printf("Authentication:  %v\n", r.Auth.Round(time.Millisecond))
	}
	if r.Group > 0 {
		fmt.Printf("Group Select:    %v\n", r.Group.Round(time.Millisecond))
	}
	fmt.Printf("BODY Send:       %v\n", r.BodySend.Round(time.Microsecond))
	fmt.Printf("First Byte:      %v\n", r.FirstByte.Round(time.Millisecond))
	fmt.Printf("Data Transfer:   %v (%.2f KB)\n", r.DataTransfer.Round(time.Millisecond), float64(r.BytesRead)/1024)
	fmt.Printf("Total:           %v\n", r.Total.Round(time.Millisecond))

	// Effective speed
	if r.DataTransfer > 0 {
		mbps := float64(r.BytesRead) * 8 / r.DataTransfer.Seconds() / 1_000_000
		fmt.Printf("Transfer Speed:  %.2f Mbps\n", mbps)
	}
}
