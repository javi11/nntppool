package nntppool

import (
	"context"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestStatInflight_DeepPipeline verifies that with StatInflight > Inflight,
// bodyless STAT commands pipeline deeper than Inflight. The mock server withholds
// every reply until it has *received* `hold` commands; if the pipeline were
// capped at Inflight (2) the client could never get `hold` (8) STATs onto the
// wire and the sweep would deadlock (caught by the timeout).
func TestStatInflight_DeepPipeline(t *testing.T) {
	const hold = 8
	var maxDepth int32

	factory := func(ctx context.Context) (net.Conn, error) {
		client, server := net.Pipe()
		go func() {
			defer func() { _ = server.Close() }()
			_, _ = server.Write([]byte("200 server ready\r\n"))
			buf := make([]byte, 8192)
			received := 0
			flushed := false
			var pending [][]byte
			for {
				n, err := server.Read(buf)
				if err != nil {
					return
				}
				for _, line := range strings.Split(string(buf[:n]), "\r\n") {
					if line == "" {
						continue
					}
					received++
					pending = append(pending, []byte("223 0 <x@h> exists\r\n"))
				}
				if !flushed && received >= hold {
					atomic.StoreInt32(&maxDepth, int32(received))
					flushed = true
				}
				if flushed {
					for _, r := range pending {
						_, _ = server.Write(r)
					}
					pending = nil
				}
			}
		}()
		return client, nil
	}

	c, err := NewClient(context.Background(), []Provider{{
		Factory:      factory,
		Connections:  1,
		Inflight:     2,
		StatInflight: 16,
		SkipPing:     true,
	}})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close() }()

	ids := make([]string, 32)
	for i := range ids {
		ids[i] = "x@h"
	}

	done := make(chan int, 1)
	go func() {
		n := 0
		for r := range c.StatMany(context.Background(), ids, StatManyOptions{Concurrency: 32}) {
			if r.Err == nil && r.Result != nil {
				n++
			}
		}
		done <- n
	}()

	select {
	case n := <-done:
		if n != len(ids) {
			t.Fatalf("got %d successful STATs, want %d", n, len(ids))
		}
	case <-time.After(5 * time.Second):
		t.Fatal("StatMany timed out — STAT pipeline appears capped at Inflight, not StatInflight")
	}

	if d := atomic.LoadInt32(&maxDepth); d < hold {
		t.Errorf("max STAT pipeline depth = %d, want >= %d (Inflight=2, StatInflight=16)", d, hold)
	}
}

// TestStatInflight_BodyBounded verifies that raising StatInflight does NOT raise
// BODY concurrency: even with StatInflight=16, concurrent BODY commands stay
// bounded by Inflight (2). The server holds body replies until the test releases
// them, recording the peak number of concurrently-outstanding bodies.
func TestStatInflight_BodyBounded(t *testing.T) {
	const nBodies = 6
	const wantMax = 2 // == Inflight

	var cur, max int32
	release := make(chan struct{})
	reachedCap := make(chan struct{})
	var capOnce sync.Once
	var wmu sync.Mutex

	factory := func(ctx context.Context) (net.Conn, error) {
		client, server := net.Pipe()
		go func() {
			defer func() { _ = server.Close() }()
			wmu.Lock()
			_, _ = server.Write([]byte("200 server ready\r\n"))
			wmu.Unlock()
			buf := make([]byte, 8192)
			for {
				n, err := server.Read(buf)
				if err != nil {
					return
				}
				for _, line := range strings.Split(string(buf[:n]), "\r\n") {
					if line == "" {
						continue
					}
					go func() {
						v := atomic.AddInt32(&cur, 1)
						for {
							m := atomic.LoadInt32(&max)
							if v <= m || atomic.CompareAndSwapInt32(&max, m, v) {
								break
							}
						}
						if v >= wantMax {
							capOnce.Do(func() { close(reachedCap) })
						}
						<-release
						atomic.AddInt32(&cur, -1)
						wmu.Lock()
						_, _ = server.Write([]byte("222 0 <x@h> body follows\r\n.\r\n"))
						wmu.Unlock()
					}()
				}
			}
		}()
		return client, nil
	}

	c, err := NewClient(context.Background(), []Provider{{
		Factory:      factory,
		Connections:  1,
		Inflight:     2,
		StatInflight: 16,
		SkipPing:     true,
	}})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close() }()

	var wg sync.WaitGroup
	for range nBodies {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = c.Body(context.Background(), "x@h")
		}()
	}

	select {
	case <-reachedCap:
	case <-time.After(3 * time.Second):
		t.Fatal("bodies never reached expected concurrency")
	}
	close(release)

	waitDone := make(chan struct{})
	go func() { wg.Wait(); close(waitDone) }()
	select {
	case <-waitDone:
	case <-time.After(5 * time.Second):
		t.Fatal("Body calls did not all complete")
	}

	if m := atomic.LoadInt32(&max); m != wantMax {
		t.Errorf("peak concurrent bodies = %d, want %d (Inflight); StatInflight must not loosen the BODY bound", m, wantMax)
	}
}

// TestStatInflight_NoSemaphoreLeak interleaves STAT, BODY, cancelled, and 430
// requests through a client with a separate STAT lane, then confirms the pool
// stays healthy — a leaked inflightSem/bodySem slot would eventually stall the
// pipeline and hang the final sentinel STAT.
func TestStatInflight_NoSemaphoreLeak(t *testing.T) {
	factory := func(ctx context.Context) (net.Conn, error) {
		client, server := net.Pipe()
		go func() {
			defer func() { _ = server.Close() }()
			_, _ = server.Write([]byte("200 server ready\r\n"))
			buf := make([]byte, 8192)
			for {
				n, err := server.Read(buf)
				if err != nil {
					return
				}
				for _, line := range strings.Split(string(buf[:n]), "\r\n") {
					if line == "" {
						continue
					}
					switch {
					case strings.HasPrefix(line, "STAT <miss"):
						_, _ = server.Write([]byte("430 no such article\r\n"))
					case strings.HasPrefix(line, "STAT "):
						_, _ = server.Write([]byte("223 0 <x@h> exists\r\n"))
					case strings.HasPrefix(line, "BODY "):
						_, _ = server.Write([]byte("222 0 <x@h> body follows\r\n.\r\n"))
					default:
						_, _ = server.Write([]byte("500 unknown\r\n"))
					}
				}
			}
		}()
		return client, nil
	}

	c, err := NewClient(context.Background(), []Provider{{
		Factory:      factory,
		Connections:  2,
		Inflight:     2,
		StatInflight: 8,
		SkipPing:     true,
	}})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close() }()

	var wg sync.WaitGroup
	launch := func(fn func()) {
		wg.Add(1)
		go func() { defer wg.Done(); fn() }()
	}

	for i := range 80 {
		switch i % 4 {
		case 0:
			launch(func() { _, _ = c.Stat(context.Background(), "x@h") })
		case 1:
			launch(func() { _, _ = c.Body(context.Background(), "x@h") })
		case 2:
			launch(func() { _, _ = c.Stat(context.Background(), "miss@h") })
		case 3:
			launch(func() {
				ctx, cancel := context.WithCancel(context.Background())
				cancel() // pre-cancelled: exercises the cancel-before-send release path
				_, _ = c.Body(ctx, "x@h")
			})
		}
	}

	waitDone := make(chan struct{})
	go func() { wg.Wait(); close(waitDone) }()
	select {
	case <-waitDone:
	case <-time.After(10 * time.Second):
		t.Fatal("mixed workload stalled — likely a semaphore leak")
	}

	// Sentinel: if any slot leaked, the pipeline would be permanently narrowed
	// and this would hang.
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if _, err := c.Stat(ctx, "x@h"); err != nil {
		t.Fatalf("sentinel STAT failed after mixed workload: %v", err)
	}
}
