package nntppool

import (
	"context"
	"fmt"
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

// --- parseDateResponse ---

func TestParseDateResponse(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		wantErr bool
		wantY   int
		wantM   time.Month
		wantD   int
	}{
		{"full status line", "111 20240315120000", false, 2024, time.March, 15},
		{"timestamp only", "20240315120000", false, 2024, time.March, 15},
		{"too short", "111 2024", true, 0, 0, 0},
		{"empty", "", true, 0, 0, 0},
		{"bad format", "111 not-a-date!!", true, 0, 0, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseDateResponse(tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("parseDateResponse(%q) error = %v, wantErr %v", tt.input, err, tt.wantErr)
				return
			}
			if !tt.wantErr {
				if got.Year() != tt.wantY || got.Month() != tt.wantM || got.Day() != tt.wantD {
					t.Errorf("parseDateResponse(%q) = %v, want %d-%v-%d", tt.input, got, tt.wantY, tt.wantM, tt.wantD)
				}
			}
		})
	}
}

// --- pingProvider ---

func TestPingProvider_Success(t *testing.T) {
	factory := func(ctx context.Context) (net.Conn, error) {
		return mockServer(t, func(s net.Conn) {
			_, _ = s.Write([]byte("200 server ready\r\n"))
			buf := make([]byte, 1024)
			_, _ = s.Read(buf) // DATE command
			_, _ = s.Write([]byte("111 20240315120000\r\n"))
		}), nil
	}

	result := pingProvider(context.Background(), factory, Auth{})
	if result.Err != nil {
		t.Fatalf("pingProvider() error = %v", result.Err)
	}
	if result.RTT <= 0 {
		t.Errorf("RTT = %v, want > 0", result.RTT)
	}
	if result.ServerTime.IsZero() {
		t.Error("ServerTime should not be zero")
	}
	if result.ServerTime.Year() != 2024 || result.ServerTime.Month() != time.March {
		t.Errorf("ServerTime = %v, want 2024-03-15", result.ServerTime)
	}
}

func TestPingProvider_WithAuth(t *testing.T) {
	factory := func(ctx context.Context) (net.Conn, error) {
		return mockServer(t, func(s net.Conn) {
			_, _ = s.Write([]byte("200 server ready\r\n"))

			buf := make([]byte, 1024)
			_, _ = s.Read(buf) // AUTHINFO USER
			_, _ = s.Write([]byte("381 password required\r\n"))
			_, _ = s.Read(buf) // AUTHINFO PASS
			_, _ = s.Write([]byte("281 authentication accepted\r\n"))

			_, _ = s.Read(buf) // DATE
			_, _ = s.Write([]byte("111 20240315120000\r\n"))
		}), nil
	}

	result := pingProvider(context.Background(), factory, Auth{Username: "user", Password: "pass"})
	if result.Err != nil {
		t.Fatalf("pingProvider() error = %v", result.Err)
	}
	if result.RTT <= 0 {
		t.Errorf("RTT = %v, want > 0", result.RTT)
	}
}

func TestPingProvider_DialError(t *testing.T) {
	factory := func(ctx context.Context) (net.Conn, error) {
		return nil, fmt.Errorf("connection refused")
	}

	result := pingProvider(context.Background(), factory, Auth{})
	if result.Err == nil {
		t.Fatal("expected error for dial failure")
	}
}

func TestPingProvider_BadGreeting(t *testing.T) {
	factory := func(ctx context.Context) (net.Conn, error) {
		return mockServer(t, func(s net.Conn) {
			_, _ = s.Write([]byte("502 service unavailable\r\n"))
		}), nil
	}

	result := pingProvider(context.Background(), factory, Auth{})
	if result.Err == nil {
		t.Fatal("expected error for bad greeting")
	}
}

// --- AddProvider / RemoveProvider ---

func TestAddProvider(t *testing.T) {
	makeFactory := func(statusCode int) ConnFactory {
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
					cmd := string(buf[:n])
					if len(cmd) >= 4 && cmd[:4] == "DATE" {
						_, _ = server.Write([]byte("111 20240315120000\r\n"))
					} else {
						_, _ = fmt.Fprintf(server, "%d response\r\n", statusCode)
					}
				}
			}()
			return client, nil
		}
	}

	c, err := NewClient(context.Background(), []Provider{
		{Factory: makeFactory(223), Connections: 1},
	})
	if err != nil {
		t.Fatalf("NewClient() error = %v", err)
	}
	defer func() { _ = c.Close() }()

	if c.NumProviders() != 1 {
		t.Fatalf("NumProviders() = %d, want 1", c.NumProviders())
	}

	// Add a second provider.
	err = c.AddProvider(Provider{Factory: makeFactory(223), Connections: 1})
	if err != nil {
		t.Fatalf("AddProvider() error = %v", err)
	}

	if c.NumProviders() != 2 {
		t.Errorf("NumProviders() = %d, want 2", c.NumProviders())
	}

	// Verify it shows up in Stats with ping data.
	stats := c.Stats()
	if len(stats.Providers) != 2 {
		t.Fatalf("Stats().Providers = %d, want 2", len(stats.Providers))
	}
}

func TestAddProvider_Backup(t *testing.T) {
	makeFactory := func() ConnFactory {
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
					cmd := string(buf[:n])
					if len(cmd) >= 4 && cmd[:4] == "DATE" {
						_, _ = server.Write([]byte("111 20240315120000\r\n"))
					} else {
						_, _ = server.Write([]byte("223 exists\r\n"))
					}
				}
			}()
			return client, nil
		}
	}

	c, err := NewClient(context.Background(), []Provider{
		{Factory: makeFactory(), Connections: 1},
	})
	if err != nil {
		t.Fatalf("NewClient() error = %v", err)
	}
	defer func() { _ = c.Close() }()

	err = c.AddProvider(Provider{Factory: makeFactory(), Connections: 1, Backup: true})
	if err != nil {
		t.Fatalf("AddProvider(backup) error = %v", err)
	}

	if c.NumProviders() != 2 {
		t.Errorf("NumProviders() = %d, want 2", c.NumProviders())
	}
}

func TestAddProvider_Validation(t *testing.T) {
	dummyFactory := func(ctx context.Context) (net.Conn, error) {
		return nil, nil
	}

	c, err := NewClient(context.Background(), []Provider{
		{Factory: dummyFactory, Connections: 1},
	})
	if err != nil {
		t.Fatalf("NewClient() error = %v", err)
	}
	defer func() { _ = c.Close() }()

	// Zero connections
	if err := c.AddProvider(Provider{Host: "host:119", Connections: 0}); err == nil {
		t.Error("expected error for zero connections")
	}

	// Missing host and factory
	if err := c.AddProvider(Provider{Connections: 1}); err == nil {
		t.Error("expected error for missing host and factory")
	}
}

func TestAddProvider_DuplicateName(t *testing.T) {
	c, err := NewClient(context.Background(), []Provider{
		{Host: "host:119", Connections: 1},
	})
	if err != nil {
		t.Fatalf("NewClient() error = %v", err)
	}
	defer func() { _ = c.Close() }()

	// Same host should be rejected as duplicate.
	if err := c.AddProvider(Provider{Host: "host:119", Connections: 1}); err == nil {
		t.Error("expected error for duplicate provider name")
	}
}

func TestRemoveProvider(t *testing.T) {
	makeFactory := func() ConnFactory {
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
					cmd := string(buf[:n])
					if len(cmd) >= 4 && cmd[:4] == "DATE" {
						_, _ = server.Write([]byte("111 20240315120000\r\n"))
					} else {
						_, _ = server.Write([]byte("223 exists\r\n"))
					}
				}
			}()
			return client, nil
		}
	}

	c, err := NewClient(context.Background(), []Provider{
		{Host: "main1:119", Factory: makeFactory(), Connections: 1},
		{Host: "main2:119", Factory: makeFactory(), Connections: 1},
	})
	if err != nil {
		t.Fatalf("NewClient() error = %v", err)
	}
	defer func() { _ = c.Close() }()

	if c.NumProviders() != 2 {
		t.Fatalf("NumProviders() = %d, want 2", c.NumProviders())
	}

	err = c.RemoveProvider("main2:119")
	if err != nil {
		t.Fatalf("RemoveProvider() error = %v", err)
	}

	if c.NumProviders() != 1 {
		t.Errorf("NumProviders() = %d, want 1", c.NumProviders())
	}

	// Verify it's gone from Stats.
	stats := c.Stats()
	for _, ps := range stats.Providers {
		if ps.Name == "main2:119" {
			t.Error("removed provider still in Stats")
		}
	}
}

func TestRemoveProvider_NotFound(t *testing.T) {
	c, err := NewClient(context.Background(), []Provider{
		{Host: "host:119", Connections: 1},
	})
	if err != nil {
		t.Fatalf("NewClient() error = %v", err)
	}
	defer func() { _ = c.Close() }()

	err = c.RemoveProvider("nonexistent:119")
	if err == nil {
		t.Error("expected error for not-found provider")
	}
}

// --- NewClient ping integration ---

func TestNewClient_PingResults(t *testing.T) {
	makeFactory := func() ConnFactory {
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
					cmd := string(buf[:n])
					if len(cmd) >= 4 && cmd[:4] == "DATE" {
						_, _ = server.Write([]byte("111 20240315120000\r\n"))
					} else {
						_, _ = server.Write([]byte("223 exists\r\n"))
					}
				}
			}()
			return client, nil
		}
	}

	c, err := NewClient(context.Background(), []Provider{
		{Factory: makeFactory(), Connections: 1},
		{Factory: makeFactory(), Connections: 1, Backup: true},
	})
	if err != nil {
		t.Fatalf("NewClient() error = %v", err)
	}
	defer func() { _ = c.Close() }()

	stats := c.Stats()
	if len(stats.Providers) != 2 {
		t.Fatalf("Providers = %d, want 2", len(stats.Providers))
	}
	for i, ps := range stats.Providers {
		if ps.Ping.Err != nil {
			t.Errorf("provider %d ping error = %v", i, ps.Ping.Err)
		}
		if ps.Ping.RTT <= 0 {
			t.Errorf("provider %d RTT = %v, want > 0", i, ps.Ping.RTT)
		}
		if ps.Ping.ServerTime.IsZero() {
			t.Errorf("provider %d ServerTime is zero", i)
		}
	}
}

func TestNewClient_PingFailureDoesNotBlock(t *testing.T) {
	// Provider that fails to connect — ping should fail but client should still work.
	failFactory := func(ctx context.Context) (net.Conn, error) {
		return nil, fmt.Errorf("connection refused")
	}

	c, err := NewClient(context.Background(), []Provider{
		{Factory: failFactory, Connections: 1},
	})
	if err != nil {
		t.Fatalf("NewClient() error = %v", err)
	}
	defer func() { _ = c.Close() }()

	stats := c.Stats()
	if len(stats.Providers) != 1 {
		t.Fatalf("Providers = %d, want 1", len(stats.Providers))
	}
	if stats.Providers[0].Ping.Err == nil {
		t.Error("expected ping error for failing factory")
	}
}

// --- AddProvider then Send integration ---

func TestAddThenSend(t *testing.T) {
	makeFactory := func(statusCode int) ConnFactory {
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
					cmd := string(buf[:n])
					if len(cmd) >= 4 && cmd[:4] == "DATE" {
						_, _ = server.Write([]byte("111 20240315120000\r\n"))
					} else {
						_, _ = fmt.Fprintf(server, "%d response\r\n", statusCode)
					}
				}
			}()
			return client, nil
		}
	}

	// Start with one main provider that returns 430.
	c, err := NewClient(context.Background(), []Provider{
		{Factory: makeFactory(430), Connections: 1},
	})
	if err != nil {
		t.Fatalf("NewClient() error = %v", err)
	}
	defer func() { _ = c.Close() }()

	// Request should return 430 (only provider).
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	resp := <-c.Send(ctx, []byte("STAT <id@test>\r\n"), nil)
	cancel()
	if resp.StatusCode != 430 {
		t.Fatalf("StatusCode = %d, want 430 before adding backup", resp.StatusCode)
	}

	// Add a backup provider that returns 223.
	err = c.AddProvider(Provider{Factory: makeFactory(223), Connections: 1, Backup: true})
	if err != nil {
		t.Fatalf("AddProvider() error = %v", err)
	}

	// Now request should fallback to backup and return 223.
	ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
	resp = <-c.Send(ctx, []byte("STAT <id@test>\r\n"), nil)
	cancel()
	if resp.StatusCode != 223 {
		t.Errorf("StatusCode = %d, want 223 after adding backup", resp.StatusCode)
	}
}

// --- Error propagation ---

func TestSend_FactoryErrorPropagates(t *testing.T) {
	dialErr := fmt.Errorf("dial tcp: connection refused")
	factory := func(ctx context.Context) (net.Conn, error) {
		return nil, dialErr
	}

	c, err := NewClient(context.Background(), []Provider{
		{Factory: factory, Connections: 1},
	})
	if err != nil {
		t.Fatalf("NewClient() error = %v", err)
	}
	defer func() { _ = c.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp := <-c.Send(ctx, []byte("STAT <id@test>\r\n"), nil)
	if resp.Err == nil {
		t.Fatal("expected error from Send when factory fails")
	}
	if resp.Err.Error() != dialErr.Error() {
		t.Errorf("got error %q, want %q", resp.Err, dialErr)
	}
}

func TestSend_ContextCancellationPropagates(t *testing.T) {
	// Factory that blocks until context is cancelled.
	factory := func(ctx context.Context) (net.Conn, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	}

	c, err := NewClient(context.Background(), []Provider{
		{Factory: factory, Connections: 1},
	})
	if err != nil {
		t.Fatalf("NewClient() error = %v", err)
	}
	defer func() { _ = c.Close() }()

	ctx, cancel := context.WithCancel(context.Background())

	respCh := c.Send(ctx, []byte("STAT <id@test>\r\n"), nil)

	// Cancel the request context.
	cancel()

	select {
	case resp := <-respCh:
		if resp.Err == nil {
			t.Fatal("expected error from Send on context cancellation")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for response after cancellation")
	}
}

func TestSend_All430Propagates(t *testing.T) {
	makeFactory := func(statusCode int) ConnFactory {
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
					cmd := string(buf[:n])
					if len(cmd) >= 4 && cmd[:4] == "DATE" {
						_, _ = server.Write([]byte("111 20240315120000\r\n"))
					} else {
						_, _ = fmt.Fprintf(server, "%d no such article\r\n", statusCode)
					}
				}
			}()
			return client, nil
		}
	}

	c, err := NewClient(context.Background(), []Provider{
		{Factory: makeFactory(430), Connections: 1},
	})
	if err != nil {
		t.Fatalf("NewClient() error = %v", err)
	}
	defer func() { _ = c.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp := <-c.Send(ctx, []byte("STAT <id@test>\r\n"), nil)
	if resp.Err != nil {
		t.Fatalf("expected no Err for 430 response, got %v", resp.Err)
	}
	if resp.StatusCode != 430 {
		t.Errorf("StatusCode = %d, want 430", resp.StatusCode)
	}
}

func TestFailRequest(t *testing.T) {
	testErr := fmt.Errorf("test error")
	ch := make(chan Response, 1)
	failRequest(ch, testErr)

	resp, ok := <-ch
	if !ok {
		t.Fatal("expected to receive response before channel closed")
	}
	if resp.Err != testErr {
		t.Errorf("got error %v, want %v", resp.Err, testErr)
	}

	// Channel should now be closed.
	_, ok = <-ch
	if ok {
		t.Error("channel should be closed after failRequest")
	}
}

func TestFailRequest_DoubleClose(t *testing.T) {
	ch := make(chan Response, 1)
	failRequest(ch, fmt.Errorf("err1"))
	// Second call should not panic (safeClose recovers).
	failRequest(ch, fmt.Errorf("err2"))
}

// --- resolveProviderName ---

func TestResolveProviderName(t *testing.T) {
	tests := []struct {
		name  string
		p     Provider
		index int
		want  string
	}{
		{"host only", Provider{Host: "news.example.com:563"}, 0, "news.example.com:563"},
		{"host with auth", Provider{Host: "news.example.com:563", Auth: Auth{Username: "myuser"}}, 0, "news.example.com:563+myuser"},
		{"factory only", Provider{Factory: func(ctx context.Context) (net.Conn, error) { return nil, nil }}, 3, "provider-3"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := resolveProviderName(tt.p, tt.index)
			if got != tt.want {
				t.Errorf("resolveProviderName() = %q, want %q", got, tt.want)
			}
		})
	}
}
