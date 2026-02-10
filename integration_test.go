package nntppool

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// --- NNTPConnection tests ---

func TestNNTPConnection_Greeting(t *testing.T) {
	conn := mockServer(t, func(s net.Conn) {
		_, _ = s.Write([]byte("200 server ready\r\n"))
		// Keep alive until client closes
		buf := make([]byte, 1)
		_, _ = s.Read(buf)
	})

	reqCh := make(chan *Request)
	nc, err := newNNTPConnectionFromConn(context.Background(), conn, 1, reqCh, Auth{}, nil, nil)
	if err != nil {
		t.Fatalf("newNNTPConnectionFromConn() error = %v", err)
	}
	if nc.Greeting.StatusCode != 200 {
		t.Errorf("Greeting.StatusCode = %d, want 200", nc.Greeting.StatusCode)
	}
}

func TestNNTPConnection_GreetingReject(t *testing.T) {
	conn := mockServer(t, func(s net.Conn) {
		_, _ = s.Write([]byte("502 service permanently unavailable\r\n"))
	})

	reqCh := make(chan *Request)
	_, err := newNNTPConnectionFromConn(context.Background(), conn, 1, reqCh, Auth{}, nil, nil)
	if err == nil {
		t.Fatal("expected error for 502 greeting")
	}
	if !errors.Is(err, ErrMaxConnections) {
		t.Errorf("error = %v, want ErrMaxConnections", err)
	}
}

func TestNNTPConnection_Auth(t *testing.T) {
	conn := mockServer(t, func(s net.Conn) {
		_, _ = s.Write([]byte("200 server ready\r\n"))

		buf := make([]byte, 1024)
		n, _ := s.Read(buf)
		got := string(buf[:n])
		if got != "AUTHINFO USER testuser\r\n" {
			t.Errorf("expected AUTHINFO USER, got %q", got)
		}
		_, _ = s.Write([]byte("381 password required\r\n"))

		n, _ = s.Read(buf)
		got = string(buf[:n])
		if got != "AUTHINFO PASS testpass\r\n" {
			t.Errorf("expected AUTHINFO PASS, got %q", got)
		}
		_, _ = s.Write([]byte("281 authentication accepted\r\n"))

		// Keep alive
		_, _ = s.Read(buf)
	})

	reqCh := make(chan *Request)
	nc, err := newNNTPConnectionFromConn(context.Background(), conn, 1, reqCh, Auth{
		Username: "testuser",
		Password: "testpass",
	}, nil, nil)
	if err != nil {
		t.Fatalf("auth error = %v", err)
	}
	if nc.Greeting.StatusCode != 200 {
		t.Errorf("Greeting.StatusCode = %d", nc.Greeting.StatusCode)
	}
}

func TestNNTPConnection_AuthReject(t *testing.T) {
	conn := mockServer(t, func(s net.Conn) {
		_, _ = s.Write([]byte("200 server ready\r\n"))

		buf := make([]byte, 1024)
		_, _ = s.Read(buf) // AUTHINFO USER
		_, _ = s.Write([]byte("381 password required\r\n"))

		_, _ = s.Read(buf) // AUTHINFO PASS
		_, _ = s.Write([]byte("481 authentication rejected\r\n"))
	})

	reqCh := make(chan *Request)
	_, err := newNNTPConnectionFromConn(context.Background(), conn, 1, reqCh, Auth{
		Username: "testuser",
		Password: "wrongpass",
	}, nil, nil)
	if err == nil {
		t.Fatal("expected auth rejection error")
	}
}

func TestNNTPConnection_RunSingleRequest(t *testing.T) {
	conn := mockServer(t, func(s net.Conn) {
		_, _ = s.Write([]byte("200 server ready\r\n"))

		buf := make([]byte, 1024)
		n, _ := s.Read(buf)
		got := string(buf[:n])
		if got != "STAT <test@example.com>\r\n" {
			t.Errorf("expected STAT command, got %q", got)
		}
		_, _ = s.Write([]byte("223 12345 <test@example.com> article exists\r\n"))
	})

	reqCh := make(chan *Request, 1)
	nc, err := newNNTPConnectionFromConn(context.Background(), conn, 1, reqCh, Auth{}, nil, nil)
	if err != nil {
		t.Fatalf("connection error = %v", err)
	}

	respCh := make(chan Response, 1)
	reqCh <- &Request{
		Ctx:     context.Background(),
		Payload: []byte("STAT <test@example.com>\r\n"),
		RespCh:  respCh,
	}

	go nc.Run()

	select {
	case resp := <-respCh:
		if resp.Err != nil {
			t.Fatalf("response error = %v", resp.Err)
		}
		if resp.StatusCode != 223 {
			t.Errorf("StatusCode = %d, want 223", resp.StatusCode)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for response")
	}

	_ = nc.Close()
}

func TestNNTPConnection_RunBodyRequest(t *testing.T) {
	original := []byte("Hello NNTP world! This is body content for decoding test.")

	conn := mockServer(t, func(s net.Conn) {
		_, _ = s.Write([]byte("200 server ready\r\n"))

		buf := make([]byte, 1024)
		_, _ = s.Read(buf) // BODY command

		_, _ = s.Write(yencSinglePart(original, "test.bin"))
	})

	reqCh := make(chan *Request, 1)
	nc, err := newNNTPConnectionFromConn(context.Background(), conn, 1, reqCh, Auth{}, nil, nil)
	if err != nil {
		t.Fatalf("connection error = %v", err)
	}

	var decoded bytes.Buffer
	respCh := make(chan Response, 1)
	reqCh <- &Request{
		Ctx:        context.Background(),
		Payload:    []byte("BODY <test@example.com>\r\n"),
		RespCh:     respCh,
		BodyWriter: &decoded,
	}

	go nc.Run()

	select {
	case resp := <-respCh:
		if resp.Err != nil {
			t.Fatalf("response error = %v", resp.Err)
		}
		if resp.StatusCode != 222 {
			t.Errorf("StatusCode = %d, want 222", resp.StatusCode)
		}
		if !bytes.Equal(decoded.Bytes(), original) {
			t.Errorf("decoded = %d bytes, want %d", decoded.Len(), len(original))
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for response")
	}

	_ = nc.Close()
}

func TestNNTPConnection_RunPipelined(t *testing.T) {
	conn := mockServer(t, func(s net.Conn) {
		_, _ = s.Write([]byte("200 server ready\r\n"))

		buf := make([]byte, 4096)
		// Read all pipelined commands (may arrive in one or multiple reads)
		var allData []byte
		for {
			n, err := s.Read(buf)
			if n > 0 {
				allData = append(allData, buf[:n]...)
			}
			// Check if we've received all 3 STAT commands
			if bytes.Count(allData, []byte("STAT")) >= 3 {
				break
			}
			if err != nil {
				break
			}
		}

		// Respond to all 3 in FIFO order
		for i := range 3 {
			_, _ = fmt.Fprintf(s, "223 %d <msg%d@id> exists\r\n", i+1, i+1)
		}
	})

	reqCh := make(chan *Request, 3)
	nc, err := newNNTPConnectionFromConn(context.Background(), conn, 3, reqCh, Auth{}, nil, nil)
	if err != nil {
		t.Fatalf("connection error = %v", err)
	}

	// Send 3 concurrent requests
	var respChs [3]chan Response
	for i := range 3 {
		respChs[i] = make(chan Response, 1)
		reqCh <- &Request{
			Ctx:     context.Background(),
			Payload: fmt.Appendf(nil, "STAT <msg%d@id>\r\n", i+1),
			RespCh:  respChs[i],
		}
	}

	go nc.Run()

	// Verify FIFO ordering
	for i := range 3 {
		select {
		case resp := <-respChs[i]:
			if resp.Err != nil {
				t.Errorf("request %d error = %v", i, resp.Err)
			}
			if resp.StatusCode != 223 {
				t.Errorf("request %d StatusCode = %d, want 223", i, resp.StatusCode)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("timeout on request %d", i)
		}
	}

	_ = nc.Close()
}

func TestNNTPConnection_CancelledRequest(t *testing.T) {
	conn := mockServer(t, func(s net.Conn) {
		_, _ = s.Write([]byte("200 server ready\r\n"))
		// Keep alive
		buf := make([]byte, 1024)
		_, _ = s.Read(buf)
	})

	reqCh := make(chan *Request, 1)
	nc, err := newNNTPConnectionFromConn(context.Background(), conn, 1, reqCh, Auth{}, nil, nil)
	if err != nil {
		t.Fatalf("connection error = %v", err)
	}

	// Already-cancelled context
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	respCh := make(chan Response, 1)
	nc.firstReq = &Request{
		Ctx:     ctx,
		Payload: []byte("STAT <test@id>\r\n"),
		RespCh:  respCh,
	}

	go nc.Run()

	// The RespCh should be closed (no response sent)
	select {
	case <-respCh:
		// Channel closed or response delivered — either is acceptable for cancelled ctx
	case <-time.After(2 * time.Second):
		t.Fatal("timeout: respCh should have been closed")
	}

	_ = nc.Close()
}

func TestNNTPConnection_IdleTimeout(t *testing.T) {
	conn := mockServer(t, func(s net.Conn) {
		_, _ = s.Write([]byte("200 server ready\r\n"))
		// Keep alive until client disconnects
		buf := make([]byte, 1024)
		for {
			if _, err := s.Read(buf); err != nil {
				return
			}
		}
	})

	reqCh := make(chan *Request)
	nc, err := newNNTPConnectionFromConn(context.Background(), conn, 1, reqCh, Auth{}, nil, nil)
	if err != nil {
		t.Fatalf("connection error = %v", err)
	}

	nc.idleTimeout = 50 * time.Millisecond
	go nc.Run()

	// Should shut down due to idle timeout
	select {
	case <-nc.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("timeout: connection should have closed due to idle timeout")
	}
}

// --- Client send/retry tests ---

func TestClient_SendRetryRoundRobin(t *testing.T) {
	// Track which providers receive requests
	var mu sync.Mutex
	hits := make(map[string]int)

	makeFactory := func(name string) ConnFactory {
		return func(ctx context.Context) (net.Conn, error) {
			client, server := net.Pipe()
			go func() {
				_, _ = server.Write([]byte("200 server ready\r\n"))

				buf := make([]byte, 4096)
				for {
					n, err := server.Read(buf)
					if err != nil {
						return
					}
					mu.Lock()
					hits[name]++
					mu.Unlock()
					_ = n
					// Respond with 223 STAT ok
					_, _ = server.Write([]byte("223 1 <id@test> exists\r\n"))
				}
			}()
			return client, nil
		}
	}

	c, err := NewClient(context.Background(), []Provider{
		{Factory: makeFactory("p1"), Connections: 1},
		{Factory: makeFactory("p2"), Connections: 1},
	})
	if err != nil {
		t.Fatalf("NewClient() error = %v", err)
	}
	defer func() { _ = c.Close() }()

	// Send a few requests
	for range 4 {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		resp := <-c.Send(ctx, []byte("STAT <id@test>\r\n"), nil)
		cancel()
		if resp.Err != nil {
			t.Fatalf("Send() error = %v", resp.Err)
		}
		if resp.StatusCode != 223 {
			t.Errorf("StatusCode = %d, want 223", resp.StatusCode)
		}
	}

	mu.Lock()
	defer mu.Unlock()
	// Both providers should have been hit
	if hits["p1"] == 0 || hits["p2"] == 0 {
		t.Errorf("expected round-robin: p1=%d, p2=%d", hits["p1"], hits["p2"])
	}
}

func TestClient_WeightedRoundRobin(t *testing.T) {
	// Track which providers receive first-attempt requests.
	var mu sync.Mutex
	hits := make(map[string]int)

	makeFactory := func(name string) ConnFactory {
		return func(ctx context.Context) (net.Conn, error) {
			client, server := net.Pipe()
			go func() {
				_, _ = server.Write([]byte("200 server ready\r\n"))

				buf := make([]byte, 4096)
				for {
					n, err := server.Read(buf)
					if err != nil {
						return
					}
					mu.Lock()
					hits[name]++
					mu.Unlock()
					_ = n
					_, _ = server.Write([]byte("223 1 <id@test> exists\r\n"))
				}
			}()
			return client, nil
		}
	}

	// Provider "big" has weight 50, "small" has weight 10 → total 60.
	// With load-aware routing, distribution shifts dynamically based on
	// available capacity, so we check that "big" gets the majority.
	c, err := NewClient(context.Background(), []Provider{
		{Factory: makeFactory("big"), Connections: 50},
		{Factory: makeFactory("small"), Connections: 10},
	})
	if err != nil {
		t.Fatalf("NewClient() error = %v", err)
	}
	defer func() { _ = c.Close() }()

	// Reset counters after NewClient — provider ping during init adds a hit.
	mu.Lock()
	clear(hits)
	mu.Unlock()

	const N = 60
	for range N {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		resp := <-c.Send(ctx, []byte("STAT <id@test>\r\n"), nil)
		cancel()
		if resp.Err != nil {
			t.Fatalf("Send() error = %v", resp.Err)
		}
		if resp.StatusCode != 223 {
			t.Errorf("StatusCode = %d, want 223", resp.StatusCode)
		}
	}

	mu.Lock()
	defer mu.Unlock()
	// With load-aware routing, "big" should get significantly more
	// requests than "small". Allow 20% tolerance from the ideal 50/10 split
	// because available capacity shifts as slots are held during requests.
	bigPct := float64(hits["big"]) / float64(N) * 100
	if bigPct < 60 {
		t.Errorf("big hits = %d (%.1f%%), want at least 60%% of %d", hits["big"], bigPct, N)
	}
	if hits["big"]+hits["small"] != N {
		t.Errorf("total hits = %d, want %d", hits["big"]+hits["small"], N)
	}
}

func TestClient_SendRetryFallbackBackup(t *testing.T) {
	makeFactory := func(statusCode int) ConnFactory {
		return func(ctx context.Context) (net.Conn, error) {
			client, server := net.Pipe()
			go func() {
				_, _ = server.Write([]byte("200 server ready\r\n"))

				buf := make([]byte, 4096)
				for {
					_, err := server.Read(buf)
					if err != nil {
						return
					}
					_, _ = fmt.Fprintf(server, "%d response\r\n", statusCode)
				}
			}()
			return client, nil
		}
	}

	c, err := NewClient(context.Background(), []Provider{
		{Factory: makeFactory(430), Connections: 1},        // main: always 430
		{Factory: makeFactory(223), Connections: 1, Backup: true}, // backup: 223
	})
	if err != nil {
		t.Fatalf("NewClient() error = %v", err)
	}
	defer func() { _ = c.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp := <-c.Send(ctx, []byte("STAT <id@test>\r\n"), nil)
	if resp.Err != nil {
		t.Fatalf("Send() error = %v", resp.Err)
	}
	if resp.StatusCode != 223 {
		t.Errorf("StatusCode = %d, want 223 (from backup)", resp.StatusCode)
	}
}

func TestClient_SendRetryAll430(t *testing.T) {
	makeFactory430 := func() ConnFactory {
		return func(ctx context.Context) (net.Conn, error) {
			client, server := net.Pipe()
			go func() {
				_, _ = server.Write([]byte("200 server ready\r\n"))

				buf := make([]byte, 4096)
				for {
					_, err := server.Read(buf)
					if err != nil {
						return
					}
					_, _ = server.Write([]byte("430 no such article\r\n"))
				}
			}()
			return client, nil
		}
	}

	c, err := NewClient(context.Background(), []Provider{
		{Factory: makeFactory430(), Connections: 1},
		{Factory: makeFactory430(), Connections: 1, Backup: true},
	})
	if err != nil {
		t.Fatalf("NewClient() error = %v", err)
	}
	defer func() { _ = c.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp := <-c.Send(ctx, []byte("STAT <id@test>\r\n"), nil)
	if resp.StatusCode != 430 {
		t.Errorf("StatusCode = %d, want 430 (all providers exhausted)", resp.StatusCode)
	}
}

func TestClient_Skip430SameHost(t *testing.T) {
	// Two main providers + one backup, all same host but different auth.
	// First returns 430 → others should be skipped.
	var counts [3]atomic.Int64
	make430Factory := func(idx int) ConnFactory {
		return func(ctx context.Context) (net.Conn, error) {
			client, server := net.Pipe()
			go func() {
				_, _ = server.Write([]byte("200 server ready\r\n"))
				buf := make([]byte, 4096)
				for {
					n, err := server.Read(buf)
					if err != nil {
						return
					}
					if bytes.Contains(buf[:n], []byte("STAT")) {
						counts[idx].Add(1)
					}
					_, _ = server.Write([]byte("430 no such article\r\n"))
				}
			}()
			return client, nil
		}
	}

	c, err := NewClient(context.Background(), []Provider{
		{Host: "news.example.com:563", Factory: make430Factory(0), Connections: 1},
		{Host: "news.example.com:563", Factory: make430Factory(1), Connections: 1},
		{Host: "news.example.com:563", Factory: make430Factory(2), Connections: 1, Backup: true},
	})
	if err != nil {
		t.Fatalf("NewClient() error = %v", err)
	}
	defer func() { _ = c.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp := <-c.Send(ctx, []byte("STAT <id@test>\r\n"), nil)
	if resp.StatusCode != 430 {
		t.Fatalf("StatusCode = %d, want 430", resp.StatusCode)
	}

	total := counts[0].Load() + counts[1].Load() + counts[2].Load()
	if total != 1 {
		t.Errorf("total requests = %d, want 1 (same-host providers should be skipped)", total)
	}
}

func TestClient_Skip430DifferentHosts(t *testing.T) {
	// Two providers on different hosts. Both should be tried.
	var counts [2]atomic.Int64
	make430Factory := func(idx int) ConnFactory {
		return func(ctx context.Context) (net.Conn, error) {
			client, server := net.Pipe()
			go func() {
				_, _ = server.Write([]byte("200 server ready\r\n"))
				buf := make([]byte, 4096)
				for {
					n, err := server.Read(buf)
					if err != nil {
						return
					}
					if bytes.Contains(buf[:n], []byte("STAT")) {
						counts[idx].Add(1)
					}
					_, _ = server.Write([]byte("430 no such article\r\n"))
				}
			}()
			return client, nil
		}
	}

	c, err := NewClient(context.Background(), []Provider{
		{Host: "news1.example.com:563", Factory: make430Factory(0), Connections: 1},
		{Host: "news2.example.com:563", Factory: make430Factory(1), Connections: 1},
	})
	if err != nil {
		t.Fatalf("NewClient() error = %v", err)
	}
	defer func() { _ = c.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp := <-c.Send(ctx, []byte("STAT <id@test>\r\n"), nil)
	if resp.StatusCode != 430 {
		t.Fatalf("StatusCode = %d, want 430", resp.StatusCode)
	}

	if counts[0].Load() != 1 || counts[1].Load() != 1 {
		t.Errorf("requests = [%d, %d], want [1, 1] (different hosts should both be tried)",
			counts[0].Load(), counts[1].Load())
	}
}

func TestClient_Skip430FactoryProviders(t *testing.T) {
	// Factory-based providers (no Host) are never skipped.
	var counts [2]atomic.Int64
	make430Factory := func(idx int) ConnFactory {
		return func(ctx context.Context) (net.Conn, error) {
			client, server := net.Pipe()
			go func() {
				_, _ = server.Write([]byte("200 server ready\r\n"))
				buf := make([]byte, 4096)
				for {
					n, err := server.Read(buf)
					if err != nil {
						return
					}
					if bytes.Contains(buf[:n], []byte("STAT")) {
						counts[idx].Add(1)
					}
					_, _ = server.Write([]byte("430 no such article\r\n"))
				}
			}()
			return client, nil
		}
	}

	c, err := NewClient(context.Background(), []Provider{
		{Factory: make430Factory(0), Connections: 1},
		{Factory: make430Factory(1), Connections: 1},
	})
	if err != nil {
		t.Fatalf("NewClient() error = %v", err)
	}
	defer func() { _ = c.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp := <-c.Send(ctx, []byte("STAT <id@test>\r\n"), nil)
	if resp.StatusCode != 430 {
		t.Fatalf("StatusCode = %d, want 430", resp.StatusCode)
	}

	if counts[0].Load() != 1 || counts[1].Load() != 1 {
		t.Errorf("requests = [%d, %d], want [1, 1] (factory providers should never be skipped)",
			counts[0].Load(), counts[1].Load())
	}
}

// --- Benchmarks ---

func benchSend(b *testing.B, providers []Provider) {
	b.Helper()

	c, err := NewClient(context.Background(), providers)
	if err != nil {
		b.Fatalf("NewClient() error = %v", err)
	}
	defer func() { _ = c.Close() }()

	payload := []byte("STAT <bench@test>\r\n")

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		resp := <-c.Send(ctx, payload, nil)
		cancel()
		if resp.Err != nil {
			b.Fatalf("Send() error = %v", resp.Err)
		}
	}
}

func BenchmarkSend(b *testing.B) {
	benchFactory := func() ConnFactory {
		return func(ctx context.Context) (net.Conn, error) {
			client, server := net.Pipe()
			go func() {
				_, _ = server.Write([]byte("200 server ready\r\n"))
				buf := make([]byte, 4096)
				for {
					_, err := server.Read(buf)
					if err != nil {
						return
					}
					_, _ = server.Write([]byte("223 1 <id@test> exists\r\n"))
				}
			}()
			return client, nil
		}
	}

	b.Run("EqualWeight_3_3", func(b *testing.B) {
		benchSend(b, []Provider{
			{Factory: benchFactory(), Connections: 3},
			{Factory: benchFactory(), Connections: 3},
		})
	})

	b.Run("Weighted_5_1", func(b *testing.B) {
		benchSend(b, []Provider{
			{Factory: benchFactory(), Connections: 5},
			{Factory: benchFactory(), Connections: 1},
		})
	})

	b.Run("SingleProvider", func(b *testing.B) {
		benchSend(b, []Provider{
			{Factory: benchFactory(), Connections: 6},
		})
	})
}

// --- readOneResponse helper (used in NNTPConnection setup) ---

func TestReadOneResponse(t *testing.T) {
	conn := mockServer(t, func(s net.Conn) {
		_, _ = s.Write([]byte("200 server ready\r\n"))
		_, _ = s.Write(mockNNTPResponse("221 0 <msg@id> head",
			"Subject: Test",
			"From: user@example.com",
		))
		// Keep alive
		buf := make([]byte, 1)
		_, _ = s.Read(buf)
	})

	reqCh := make(chan *Request)
	nc, err := newNNTPConnectionFromConn(context.Background(), conn, 1, reqCh, Auth{}, nil, nil)
	if err != nil {
		t.Fatalf("connection error = %v", err)
	}

	resp, err := nc.readOneResponse(io.Discard)
	if err != nil {
		t.Fatalf("readOneResponse() error = %v", err)
	}
	if resp.StatusCode != 221 {
		t.Errorf("StatusCode = %d, want 221", resp.StatusCode)
	}
	if len(resp.Lines) != 2 {
		t.Errorf("Lines = %d, want 2", len(resp.Lines))
	}
}

