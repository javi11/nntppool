package nntppool

import (
	"bytes"
	"context"
	"errors"
	"net"
	"strings"
	"sync"
	"testing"
	"time"
)

// slowBodyFactory is a HEALTHY provider that genuinely holds the article but
// needs `delay` before its first response byte — the slow-spool shape that
// escalation exists to rescue.
func slowBodyFactory(delay time.Duration) ConnFactory {
	payload := bytes.Repeat([]byte("X"), 256)
	return func(ctx context.Context) (net.Conn, error) {
		client, server := net.Pipe()
		go func() {
			_, _ = server.Write([]byte("200 ready\r\n"))
			buf := make([]byte, 4096)
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
						_, _ = server.Write(yencSinglePart(payload, "aged.bin"))
					}()
				}
			}
		}()
		return client, nil
	}
}

// TestEscalationBreakerStopsRepeatedOutageCost: during a sustained outage every
// request pays base window + the full escalation budget, so a multi-segment
// download runs ~5x slower than it did before escalation existed. After
// escalationBreakerThreshold consecutive fruitless escalations a provider must
// stop being escalated, collapsing each subsequent request back to one base
// window until the provider proves itself again.
func TestEscalationBreakerStopsRepeatedOutageCost(t *testing.T) {
	c, err := NewClient(context.Background(), []Provider{
		{Factory: hungFactory, Connections: 1, SkipPing: true, AttemptTimeout: 200 * time.Millisecond},
	})
	if err != nil {
		t.Fatalf("NewClient() error = %v", err)
	}
	defer func() { _ = c.Close() }()

	start := time.Now()
	for range 10 {
		_, _ = c.Body(context.Background(), "seg@test")
	}
	elapsed := time.Since(start)

	// Without the breaker: 10 x (200ms + 800ms budget) = ~10s.
	// With it (threshold 2): 2 x 1s + 8 x 200ms = ~3.6s.
	if elapsed > 6*time.Second {
		t.Errorf("10 requests during a sustained outage took %v, want under 6s "+
			"(escalation must stop repaying its budget once it is proven fruitless)", elapsed)
	}
}

// TestEscalationBreakerKeepsEscalatingWhenItPaysOff: the breaker must never
// suppress a provider whose escalations DELIVER. A slow-but-present provider
// answers on every escalated window, so its fruitless counter must reset each
// time — otherwise the breaker would re-introduce the exact data loss that
// escalation was added to fix.
func TestEscalationBreakerKeepsEscalatingWhenItPaysOff(t *testing.T) {
	c, err := NewClient(context.Background(), []Provider{
		{Factory: slowBodyFactory(300 * time.Millisecond), Connections: 1, SkipPing: true,
			AttemptTimeout: 200 * time.Millisecond},
	})
	if err != nil {
		t.Fatalf("NewClient() error = %v", err)
	}
	defer func() { _ = c.Close() }()

	for i := range 6 {
		body, err := c.Body(context.Background(), "aged@spool")
		if err != nil {
			t.Fatalf("request %d: slow-but-present article was LOST: %v", i, err)
		}
		if len(body.Bytes) != 256 {
			t.Fatalf("request %d: got %d bytes, want 256", i, len(body.Bytes))
		}
	}
}

// TestEscalationBreakerReprobesAfterCooldown: a tripped breaker must be
// half-open, not permanently open — otherwise a provider that goes silent and
// later recovers into the slow-spool shape could never be escalated again, and
// its articles would be lost forever.
func TestEscalationBreakerReprobesAfterCooldown(t *testing.T) {
	restore := escalationBreakerCooldown
	escalationBreakerCooldown = 300 * time.Millisecond
	defer func() { escalationBreakerCooldown = restore }()

	c, err := NewClient(context.Background(), []Provider{
		{Factory: hungFactory, Connections: 1, SkipPing: true, AttemptTimeout: 200 * time.Millisecond},
	})
	if err != nil {
		t.Fatalf("NewClient() error = %v", err)
	}
	defer func() { _ = c.Close() }()

	// Trip the breaker: escalations here can never pay off.
	for range 4 {
		_, _ = c.Body(context.Background(), "seg@test")
	}

	// Suppressed: one base window only.
	start := time.Now()
	_, _ = c.Body(context.Background(), "seg@test")
	if suppressed := time.Since(start); suppressed > 600*time.Millisecond {
		t.Fatalf("tripped breaker still escalated (%v); want ~one 200ms window", suppressed)
	}

	time.Sleep(escalationBreakerCooldown + 100*time.Millisecond)

	// Half-open: the cooldown lapsed, so one escalation must be re-attempted.
	start = time.Now()
	_, _ = c.Body(context.Background(), "seg@test")
	if reprobe := time.Since(start); reprobe < 700*time.Millisecond {
		t.Errorf("after cooldown the breaker did not re-probe (%v); want base + escalation budget", reprobe)
	}
}

// TestEscalationBreakerErrorStaysTyped: suppressing escalation must not cost
// the caller the typed reason — a suppressed request still expired awaiting a
// response and must say so.
func TestEscalationBreakerErrorStaysTyped(t *testing.T) {
	c, err := NewClient(context.Background(), []Provider{
		{Factory: hungFactory, Connections: 1, SkipPing: true, AttemptTimeout: 100 * time.Millisecond},
	})
	if err != nil {
		t.Fatalf("NewClient() error = %v", err)
	}
	defer func() { _ = c.Close() }()

	var last error
	for range 5 {
		_, last = c.Body(context.Background(), "seg@test")
	}
	var at *AttemptTimeoutError
	if !errors.As(last, &at) {
		t.Fatalf("suppressed request error = %v, want it to still wrap *AttemptTimeoutError", last)
	}
}
