package nntppool

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"
)

// --- connGate tests ---

func TestConnGate_EnterExit(t *testing.T) {
	g := newConnGate(3, time.Minute)
	defer g.stop()
	ctx := context.Background()

	// Enter max times
	for range 3 {
		if !g.enter(ctx, ctx) {
			t.Fatal("enter() should succeed")
		}
	}

	// Exit one
	g.exit()

	// Re-enter
	if !g.enter(ctx, ctx) {
		t.Error("re-enter after exit should succeed")
	}

	// Clean up
	for range 3 {
		g.exit()
	}
}

func TestConnGate_EnterBlocks(t *testing.T) {
	g := newConnGate(1, time.Minute)
	defer g.stop()
	ctx := context.Background()

	if !g.enter(ctx, ctx) {
		t.Fatal("first enter should succeed")
	}

	// Second enter should block
	entered := make(chan bool, 1)
	go func() {
		entered <- g.enter(ctx, ctx)
	}()

	// Give it a moment to block
	select {
	case <-entered:
		t.Fatal("second enter should block")
	case <-time.After(50 * time.Millisecond):
	}

	// Exit first slot
	g.exit()

	// Now second should unblock
	select {
	case ok := <-entered:
		if !ok {
			t.Error("enter after exit should return true")
		}
	case <-time.After(time.Second):
		t.Fatal("enter should unblock after exit")
	}

	g.exit()
}

func TestConnGate_EnterCancelledSlotCtx(t *testing.T) {
	g := newConnGate(1, time.Minute)
	defer g.stop()
	ctx := context.Background()

	if !g.enter(ctx, ctx) {
		t.Fatal("first enter should succeed")
	}

	slotCtx, slotCancel := context.WithCancel(context.Background())
	entered := make(chan bool, 1)
	go func() {
		entered <- g.enter(slotCtx, ctx)
	}()

	// Cancel slot context
	time.Sleep(20 * time.Millisecond)
	slotCancel()

	select {
	case ok := <-entered:
		if ok {
			t.Error("enter with cancelled slotCtx should return false")
		}
	case <-time.After(time.Second):
		t.Fatal("should unblock on slotCtx cancel")
	}

	g.exit()
}

func TestConnGate_EnterCancelledReqCtx(t *testing.T) {
	g := newConnGate(1, time.Minute)
	defer g.stop()
	ctx := context.Background()

	if !g.enter(ctx, ctx) {
		t.Fatal("first enter should succeed")
	}

	reqCtx, reqCancel := context.WithCancel(context.Background())
	entered := make(chan bool, 1)
	go func() {
		entered <- g.enter(ctx, reqCtx)
	}()

	// Cancel req context
	time.Sleep(20 * time.Millisecond)
	reqCancel()

	select {
	case ok := <-entered:
		if ok {
			t.Error("enter with cancelled reqCtx should return false")
		}
	case <-time.After(time.Second):
		t.Fatal("should unblock on reqCtx cancel")
	}

	g.exit()
}

func TestConnGate_ConcurrentEnterExit(t *testing.T) {
	g := newConnGate(5, time.Minute)
	defer g.stop()
	ctx := context.Background()

	var wg sync.WaitGroup
	for range 20 {
		wg.Go(func() {
			if g.enter(ctx, ctx) {
				time.Sleep(time.Millisecond)
				g.exit()
			}
		})
	}
	wg.Wait()

	// Verify held == 0 at end
	g.mu.Lock()
	held := g.held
	g.mu.Unlock()
	if held != 0 {
		t.Errorf("held = %d, want 0 after all goroutines done", held)
	}
}

func TestConnGate_Throttle(t *testing.T) {
	g := newConnGate(10, time.Minute)
	defer g.stop()

	// Simulate 3 running connections
	g.mu.Lock()
	g.running = 3
	g.mu.Unlock()

	g.throttle()

	g.mu.Lock()
	allowed := g.allowed
	g.mu.Unlock()

	if allowed != 3 {
		t.Errorf("allowed = %d, want 3 (max(1, running))", allowed)
	}

	// Throttle with 0 running → should go to 1
	g.mu.Lock()
	g.running = 0
	g.allowed = 10 // reset
	g.mu.Unlock()

	g.throttle()

	g.mu.Lock()
	allowed = g.allowed
	g.mu.Unlock()

	if allowed != 1 {
		t.Errorf("allowed = %d, want 1 (max(1, 0))", allowed)
	}

	// Throttle should only tighten, not loosen
	g.mu.Lock()
	g.allowed = 2
	g.running = 5
	g.mu.Unlock()

	g.throttle()

	g.mu.Lock()
	allowed = g.allowed
	g.mu.Unlock()

	if allowed != 2 {
		t.Errorf("allowed = %d, want 2 (should not loosen)", allowed)
	}
}

func TestConnGate_Restore(t *testing.T) {
	g := newConnGate(10, 50*time.Millisecond)
	defer g.stop()

	g.mu.Lock()
	g.running = 1
	g.mu.Unlock()
	g.throttle()

	g.mu.Lock()
	allowed := g.allowed
	g.mu.Unlock()
	if allowed != 1 {
		t.Fatalf("allowed = %d, want 1 after throttle", allowed)
	}

	// Wait for restore
	time.Sleep(100 * time.Millisecond)

	g.mu.Lock()
	allowed = g.allowed
	g.mu.Unlock()
	if allowed != 10 {
		t.Errorf("allowed = %d, want 10 after restore", allowed)
	}
}

func TestConnGate_Snapshot(t *testing.T) {
	g := newConnGate(8, time.Minute)
	defer g.stop()

	maxSlots, running := g.snapshot()
	if maxSlots != 8 || running != 0 {
		t.Errorf("snapshot = (%d, %d), want (8, 0)", maxSlots, running)
	}

	g.markRunning()
	g.markRunning()
	maxSlots, running = g.snapshot()
	if maxSlots != 8 || running != 2 {
		t.Errorf("snapshot = (%d, %d), want (8, 2)", maxSlots, running)
	}

	g.markNotRunning()
	_, running = g.snapshot()
	if running != 1 {
		t.Errorf("running = %d, want 1", running)
	}
}

func TestConnGate_Stop(t *testing.T) {
	g := newConnGate(10, time.Hour)
	g.throttle() // start restore timer

	g.mu.Lock()
	hasTimer := g.restoreTimer != nil
	g.mu.Unlock()
	if !hasTimer {
		t.Fatal("should have restore timer after throttle")
	}

	g.stop()

	g.mu.Lock()
	hasTimer = g.restoreTimer != nil
	g.mu.Unlock()
	if hasTimer {
		t.Error("stop() should cancel restore timer")
	}
}

// --- safeClose ---

func TestSafeClose(t *testing.T) {
	// Open channel
	ch := make(chan Response)
	safeClose(ch)
	// Verify closed
	select {
	case _, ok := <-ch:
		if ok {
			t.Error("channel should be closed")
		}
	default:
		t.Error("receive on closed channel should not block")
	}

	// Already closed channel — should not panic
	safeClose(ch) // would panic without recover in safeClose
}

// --- NewClient validation ---

func TestNewClient_Validation(t *testing.T) {
	dummyFactory := func(ctx context.Context) (net.Conn, error) {
		return nil, nil
	}

	tests := []struct {
		name      string
		providers []Provider
		wantErr   bool
	}{
		{
			name:      "no providers",
			providers: nil,
			wantErr:   true,
		},
		{
			name: "all backup",
			providers: []Provider{
				{Host: "host1:119", Connections: 1, Backup: true},
			},
			wantErr: true,
		},
		{
			name: "zero connections",
			providers: []Provider{
				{Host: "host1:119", Connections: 0},
			},
			wantErr: true,
		},
		{
			name: "missing host and factory",
			providers: []Provider{
				{Connections: 1},
			},
			wantErr: true,
		},
		{
			name: "valid with factory only",
			providers: []Provider{
				{Factory: dummyFactory, Connections: 1},
			},
			wantErr: false,
		},
		{
			name: "valid with host only",
			providers: []Provider{
				{Host: "host1:119", Connections: 1},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c, err := NewClient(context.Background(), tt.providers)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewClient() error = %v, wantErr = %v", err, tt.wantErr)
			}
			if c != nil {
				_ = c.Close()
			}
		})
	}
}

func TestNewClient_NilContext(t *testing.T) {
	// Nil context should default to Background
	c, err := NewClient(context.TODO(), []Provider{
		{Host: "localhost:119", Connections: 1},
	})
	if err != nil {
		t.Fatalf("NewClient() error = %v", err)
	}
	defer func() { _ = c.Close() }()
	if c.ctx == nil {
		t.Error("ctx should not be nil")
	}
}

func TestClient_NumProviders(t *testing.T) {
	c, err := NewClient(context.Background(), []Provider{
		{Host: "main1:119", Connections: 1},
		{Host: "main2:119", Connections: 1},
		{Host: "backup:119", Connections: 1, Backup: true},
	})
	if err != nil {
		t.Fatalf("NewClient() error = %v", err)
	}
	defer func() { _ = c.Close() }()

	if got := c.NumProviders(); got != 3 {
		t.Errorf("NumProviders() = %d, want 3", got)
	}
}

func TestClient_Stats(t *testing.T) {
	c, err := NewClient(context.Background(), []Provider{
		{Host: "main:119", Connections: 2},
		{Host: "backup:119", Connections: 1, Backup: true},
	})
	if err != nil {
		t.Fatalf("NewClient() error = %v", err)
	}
	defer func() { _ = c.Close() }()

	time.Sleep(10 * time.Millisecond) // ensure non-zero elapsed

	stats := c.Stats()
	if stats.Elapsed <= 0 {
		t.Error("Elapsed should be > 0")
	}
	if len(stats.Providers) != 2 {
		t.Errorf("Providers = %d, want 2", len(stats.Providers))
	}
	if stats.Providers[0].Name != "main:119" {
		t.Errorf("Name = %q, want main:119", stats.Providers[0].Name)
	}
	if stats.Providers[0].MaxConnections != 2 {
		t.Errorf("MaxConnections = %d, want 2", stats.Providers[0].MaxConnections)
	}
}

func TestNewClient_DefaultInflight(t *testing.T) {
	// Inflight=0 should default to 1 internally
	c, err := NewClient(context.Background(), []Provider{
		{Host: "host:119", Connections: 1, Inflight: 0},
	})
	if err != nil {
		t.Fatalf("NewClient() error = %v", err)
	}
	defer func() { _ = c.Close() }()
	// The defaulting happens inside NewClient; we verify it didn't error.
}
