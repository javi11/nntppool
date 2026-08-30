package nntppool

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"testing"
	"time"
)

// TestStatStream_DispatchDecidedPerID is the capability StatMany cannot offer:
// the caller commits to an id only when it is about to be checked, so work a
// verdict has already settled is never put on the wire. StatMany takes the
// whole slice up front, so every id is dispatched no matter what the earlier
// results said.
func TestStatStream_DispatchDecidedPerID(t *testing.T) {
	var mu sync.Mutex
	var cmdLog []string
	replies := map[string]string{
		"probe@h": "223 1 <probe@h> exists",
		"next@h":  "223 2 <next@h> exists",
	}

	c, err := NewClient(context.Background(), []Provider{{
		Factory:     makeStatByIDFactory(t, &mu, &cmdLog, replies),
		Connections: 1,
		SkipPing:    true,
	}})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close() }()

	ids := make(chan string)
	out := c.StatStream(context.Background(), ids, StatManyOptions{Concurrency: 1})

	ids <- "dead@h" // absent from replies => 430
	first := <-out
	if !errors.Is(first.Err, ErrArticleNotFound) {
		t.Fatalf("first result Err = %v, want ErrArticleNotFound", first.Err)
	}

	// The verdict is in, so "abandoned@h" is never fed. Only ids the caller
	// still cared about reach the server.
	ids <- "next@h"
	if second := <-out; second.Err != nil {
		t.Fatalf("second result Err = %v, want nil", second.Err)
	}
	close(ids)
	for range out {
	}

	mu.Lock()
	defer mu.Unlock()
	joined := strings.Join(cmdLog, "|")
	if !strings.Contains(joined, "dead@h") || !strings.Contains(joined, "next@h") {
		t.Fatalf("expected both fed ids on the wire, got %q", joined)
	}
	if strings.Contains(joined, "abandoned@h") {
		t.Fatalf("id that was never fed reached the server: %q", joined)
	}
}

// TestStatStream_ReportsEveryFedID checks completeness and closure: one result
// per fed id, and the channel closes once the id stream does.
func TestStatStream_ReportsEveryFedID(t *testing.T) {
	replies := map[string]string{}
	want := make([]string, 0, 25)
	for i := range 25 {
		id := fmt.Sprintf("id%d@h", i)
		want = append(want, id)
		replies[id] = fmt.Sprintf("223 %d <%s> exists", i, id)
	}

	c, err := NewClient(context.Background(), []Provider{{
		Factory:     makeStatByIDFactory(t, nil, nil, replies),
		Connections: 3,
		SkipPing:    true,
	}})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close() }()

	ids := make(chan string)
	go func() {
		defer close(ids)
		for _, id := range want {
			ids <- id
		}
	}()

	got := collectStat(c.StatStream(context.Background(), ids, StatManyOptions{}))
	if len(got) != len(want) {
		t.Fatalf("got %d results, want %d", len(got), len(want))
	}
	for _, id := range want {
		if r, ok := got[id]; !ok || r.Err != nil {
			t.Errorf("%s: ok=%v err=%v", id, ok, r.Err)
		}
	}
}

// TestStatStream_BoundsOutstandingByConcurrency verifies the pool honours
// Concurrency even when the underlying connections could carry far more.
func TestStatStream_BoundsOutstandingByConcurrency(t *testing.T) {
	const conc = 2

	var mu sync.Mutex
	outstanding, highWater := 0, 0

	factory := func(ctx context.Context) (net.Conn, error) {
		client, server := net.Pipe()
		go func() {
			defer func() { _ = server.Close() }()
			_, _ = server.Write([]byte("200 server ready\r\n"))
			buf := make([]byte, 16384)
			for {
				n, err := server.Read(buf)
				if err != nil {
					return
				}
				cmds := strings.Count(string(buf[:n]), "\r\n")
				mu.Lock()
				outstanding += cmds
				if outstanding > highWater {
					highWater = outstanding
				}
				mu.Unlock()

				time.Sleep(5 * time.Millisecond)

				mu.Lock()
				outstanding -= cmds
				mu.Unlock()
				for range cmds {
					_, _ = server.Write([]byte("223 0 <x@h> exists\r\n"))
				}
			}
		}()
		return client, nil
	}

	c, err := NewClient(context.Background(), []Provider{{
		Factory:      factory,
		Connections:  4,
		Inflight:     8,
		StatInflight: 32,
		SkipPing:     true,
	}})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close() }()

	ids := make(chan string)
	go func() {
		defer close(ids)
		for i := range 20 {
			ids <- fmt.Sprintf("id%d@h", i)
		}
	}()

	got := collectStat(c.StatStream(context.Background(), ids, StatManyOptions{Concurrency: conc}))
	if len(got) != 20 {
		t.Fatalf("got %d results, want 20", len(got))
	}

	mu.Lock()
	defer mu.Unlock()
	if highWater > conc {
		t.Fatalf("peak outstanding STATs = %d, want <= %d", highWater, conc)
	}
}

// TestStatStream_ContextCancelStopsDispatch checks that cancelling mid-stream
// closes the result channel instead of hanging, and that a caller still
// blocked on a send is released.
func TestStatStream_ContextCancelStopsDispatch(t *testing.T) {
	c, err := NewClient(context.Background(), []Provider{{
		Factory:     makeStatByIDFactory(t, nil, nil, map[string]string{}),
		Connections: 1,
		SkipPing:    true,
	}})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close() }()

	ctx, cancel := context.WithCancel(context.Background())
	ids := make(chan string)
	out := c.StatStream(ctx, ids, StatManyOptions{Concurrency: 1})

	ids <- "a@h"
	<-out
	cancel()

	done := make(chan struct{})
	go func() {
		defer close(done)
		for range out {
		}
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("StatStream did not close its result channel after cancellation")
	}
}
