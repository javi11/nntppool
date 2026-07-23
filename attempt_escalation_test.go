package nntppool

import (
	"context"
	"errors"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// slowStatusFactory returns a healthy server that answers every BODY with a
// 430 — after `delay`. This is the real-world slow-spool-lookup shape: aged
// articles have been measured taking ~7.5s to a 430 on a healthy Newshosting
// connection while the TTFB EWMA (cache-hot serving) derives a 2s window.
func slowStatusFactory(delay time.Duration, dials *atomic.Int64) ConnFactory {
	return func(ctx context.Context) (net.Conn, error) {
		if dials != nil {
			dials.Add(1)
		}
		client, server := net.Pipe()
		go func() {
			_, _ = server.Write([]byte("200 ready\r\n"))
			buf := make([]byte, 4096)
			// The read loop never blocks on answering: each BODY is answered
			// from its own goroutine after `delay`, exactly like a real server
			// whose spool lookup runs while the connection stays responsive.
			// (net.Pipe is unbuffered — a server that sleeps inline would
			// deadlock a client command against its own pending answer.)
			var wmu sync.Mutex
			for {
				n, err := server.Read(buf)
				if err != nil {
					return
				}
				if strings.HasPrefix(string(buf[:n]), "BODY") {
					go func() {
						time.Sleep(delay)
						wmu.Lock()
						defer wmu.Unlock()
						_, _ = server.Write([]byte("430 No Such Article\r\n"))
					}()
				}
			}
		}()
		return client, nil
	}
}

// TestSlowStatusLineEscalates: a server needing longer than the attempt window
// to produce its status line must eventually be HEARD, not abandoned forever.
// With base 200ms the escalation ladder is 200→400→800ms; a 300ms-delayed 430
// fails the base attempt and is delivered by an escalated one. Before
// escalation this request could never complete at any retry count: every
// attempt re-asked the same question with the same too-short window.
func TestSlowStatusLineEscalates(t *testing.T) {
	c, err := NewClient(context.Background(), []Provider{
		{Factory: slowStatusFactory(300*time.Millisecond, nil), Connections: 1, SkipPing: true, AttemptTimeout: 200 * time.Millisecond},
	})
	if err != nil {
		t.Fatalf("NewClient() error = %v", err)
	}
	defer func() { _ = c.Close() }()

	start := time.Now()
	_, err = c.Body(context.Background(), "aged@spool")
	elapsed := time.Since(start)

	// The contract under test is DELIVERY: before escalation this request
	// could never complete at any retry count (every attempt re-asked with the
	// same too-short window). Exact timing is internal; boundedness is not.
	if !errors.Is(err, ErrArticleNotFound) {
		t.Fatalf("Body() error = %v, want ErrArticleNotFound (the slow 430 must be delivered, not abandoned)", err)
	}
	if elapsed > 5*time.Second {
		t.Errorf("elapsed = %v, want well under 5s", elapsed)
	}
}

// TestExpiredAttemptsKeepTheirReason: when every attempt expires, the terminal
// error must carry the attempt-timeout story — the bare "all providers
// exhausted" reads as total infrastructure death and once cost a 17-hour
// misdiagnosis chasing dead sockets and session caps.
func TestExpiredAttemptsKeepTheirReason(t *testing.T) {
	hung := func(ctx context.Context) (net.Conn, error) {
		client, server := net.Pipe()
		go func() {
			_, _ = server.Write([]byte("200 ready\r\n"))
			buf := make([]byte, 4096)
			_, _ = server.Read(buf) // swallow the command, never answer
			<-ctx.Done()
			_ = server.Close()
		}()
		return client, nil
	}
	c, err := NewClient(context.Background(), []Provider{
		{Factory: hung, Connections: 1, SkipPing: true, AttemptTimeout: 100 * time.Millisecond},
	})
	if err != nil {
		t.Fatalf("NewClient() error = %v", err)
	}
	defer func() { _ = c.Close() }()

	_, err = c.Body(context.Background(), "id@test")
	if err == nil {
		t.Fatal("Body() = nil error, want attempt-timeout failure")
	}
	var at *AttemptTimeoutError
	if !errors.As(err, &at) {
		t.Fatalf("Body() error = %v, want it to wrap *AttemptTimeoutError (never the bare exhausted form)", err)
	}
	if at.Phase != "response" {
		t.Errorf("AttemptTimeoutError.Phase = %q, want %q", at.Phase, "response")
	}
}

// TestEscalationBounded: a hung provider must still fail over in bounded time —
// base + 2× + 4× windows, never an unbounded ladder.
func TestEscalationBounded(t *testing.T) {
	hung := func(ctx context.Context) (net.Conn, error) {
		client, server := net.Pipe()
		go func() {
			_, _ = server.Write([]byte("200 ready\r\n"))
			buf := make([]byte, 4096)
			_, _ = server.Read(buf)
			<-ctx.Done()
			_ = server.Close()
		}()
		return client, nil
	}
	c, err := NewClient(context.Background(), []Provider{
		{Factory: hung, Connections: 1, SkipPing: true, AttemptTimeout: 100 * time.Millisecond},
	})
	if err != nil {
		t.Fatalf("NewClient() error = %v", err)
	}
	defer func() { _ = c.Close() }()

	start := time.Now()
	_, _ = c.Body(context.Background(), "id@test")
	elapsed := time.Since(start)
	// 100+200+400ms of windows plus scheduling slack — nowhere near unbounded.
	if elapsed > 3*time.Second {
		t.Errorf("elapsed = %v, want well under 3s (escalation must stay bounded)", elapsed)
	}
}
