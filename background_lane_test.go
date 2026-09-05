package nntppool

import (
	"context"
	"net"
	"strings"
	"testing"
	"time"
)

func newBgLaneTestConn(prio, req, bg chan *Request) *NNTPConnection {
	return &NNTPConnection{prioCh: prio, reqCh: req, bgCh: bg}
}

func TestTryNextRequest_BackgroundAfterNormal(t *testing.T) {
	for i := range 100 {
		req := make(chan *Request, 1)
		bg := make(chan *Request, 1)
		normReq := &Request{Payload: []byte("BODY <norm>\r\n")}
		bg <- &Request{Payload: []byte("BODY <bg>\r\n"), lane: laneBackground}
		req <- normReq

		c := newBgLaneTestConn(nil, req, bg)
		got, ok, found := c.tryNextRequest()
		if !found || !ok {
			t.Fatalf("iteration %d: found=%v ok=%v, want both true", i, found, ok)
		}
		if got != normReq {
			t.Fatalf("iteration %d: background request outranked a normal one", i)
		}
	}
}

func TestTryNextRequest_BackgroundServedWhenAlone(t *testing.T) {
	bg := make(chan *Request, 1)
	bgReq := &Request{Payload: []byte("BODY <bg>\r\n"), lane: laneBackground}
	bg <- bgReq

	c := newBgLaneTestConn(nil, nil, bg)
	got, ok, found := c.tryNextRequest()
	if !found || !ok || got != bgReq {
		t.Fatalf("found=%v ok=%v got=%v, want the background request", found, ok, got)
	}
}

func TestBackgroundLaneGate(t *testing.T) {
	bg := make(chan *Request)
	cases := []struct {
		name         string
		foregroundAt time.Duration // age of the last foreground dispatch; <0 = never
		inflight     int32
		floor        int32
		wantOpen     bool
	}{
		{"idle pool, nothing in flight", -1, 0, 2, true},
		{"idle pool, over the floor", -1, 8, 2, true},
		{"recent foreground, under floor", 100 * time.Millisecond, 1, 2, true},
		{"recent foreground, at floor", 100 * time.Millisecond, 2, 2, false},
		{"recent foreground, over floor", 100 * time.Millisecond, 5, 2, false},
		{"stale foreground, over floor", backgroundYieldWindow + time.Second, 5, 2, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			stats := &providerStats{}
			stats.bgFloor = tc.floor
			stats.bgInflight.Store(tc.inflight)
			if tc.foregroundAt >= 0 {
				stats.lastForeground.Store(time.Now().Add(-tc.foregroundAt).UnixNano())
			}
			got := backgroundLaneFor(stats, bg) != nil
			if got != tc.wantOpen {
				t.Fatalf("lane open = %v, want %v", got, tc.wantOpen)
			}
		})
	}
}

func TestBackgroundLaneGateStandalone(t *testing.T) {
	bg := make(chan *Request)
	if backgroundLaneFor(nil, bg) == nil {
		t.Fatal("a standalone connection (no stats) must always read the background lane")
	}
}

// bgEchoServer answers BODY with a small yEnc article and STAT with 223.
type bgEchoServer struct{}

func (bgEchoServer) factory() ConnFactory {
	return func(ctx context.Context) (net.Conn, error) {
		client, server := net.Pipe()
		go func() {
			defer func() { _ = server.Close() }()
			_, _ = server.Write([]byte("200 ready\r\n"))
			buf := make([]byte, 4096)
			for {
				n, err := server.Read(buf)
				if err != nil {
					return
				}
				for _, cmd := range strings.Split(strings.TrimRight(string(buf[:n]), "\r\n"), "\r\n") {
					switch {
					case strings.HasPrefix(cmd, "DATE"):
						_, _ = server.Write([]byte("111 20240101000000\r\n"))
					case strings.HasPrefix(cmd, "STAT "):
						_, _ = server.Write([]byte("223 0 " + strings.TrimPrefix(cmd, "STAT ") + "\r\n"))
					case strings.HasPrefix(cmd, "BODY "):
						_, _ = server.Write(yencSinglePart([]byte("payload"), "f.bin"))
					default:
						_, _ = server.Write([]byte("500 unsupported\r\n"))
					}
				}
			}
		}()
		return client, nil
	}
}

func newBgTestClient(t *testing.T) (*Client, *providerGroup) {
	t.Helper()
	c, err := NewClient(context.Background(), []Provider{{
		Factory:     bgEchoServer{}.factory(),
		Connections: 2,
		SkipPing:    true,
	}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c, (*c.mainGroups.Load())[0]
}

func TestClient_BodyBackgroundRoundTrip(t *testing.T) {
	c, g := newBgTestClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	body, err := c.BodyBackground(ctx, "bg@h")
	if err != nil {
		t.Fatalf("BodyBackground: %v", err)
	}
	if string(body.Bytes) != "payload" {
		t.Fatalf("decoded %q, want %q", body.Bytes, "payload")
	}
	if got := g.stats.bgInflight.Load(); got != 0 {
		t.Fatalf("bgInflight after completion = %d, want 0", got)
	}
	if g.stats.lastForeground.Load() != 0 {
		t.Fatal("a background body must not stamp lastForeground")
	}

	if _, err := c.Body(ctx, "fg@h"); err != nil {
		t.Fatalf("Body: %v", err)
	}
	if g.stats.lastForeground.Load() == 0 {
		t.Fatal("a normal-lane body must stamp lastForeground")
	}
	if got := g.stats.bgInflight.Load(); got != 0 {
		t.Fatalf("bgInflight after a foreground body = %d, want 0", got)
	}
}

func TestClient_StatBackgroundAndStatManyBackground(t *testing.T) {
	c, g := newBgTestClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if _, err := c.StatBackground(ctx, "one@h"); err != nil {
		t.Fatalf("StatBackground: %v", err)
	}
	n := 0
	for r := range c.StatMany(ctx, []string{"a@h", "b@h", "c@h"}, StatManyOptions{Background: true}) {
		if r.Err != nil {
			t.Fatalf("StatMany background: %v", r.Err)
		}
		n++
	}
	if n != 3 {
		t.Fatalf("got %d results, want 3", n)
	}
	if g.stats.lastForeground.Load() != 0 {
		t.Fatal("background STATs must not stamp lastForeground")
	}
	if got := g.stats.bgInflight.Load(); got != 0 {
		t.Fatalf("bgInflight after sweep = %d, want 0", got)
	}
}

func TestBackgroundDispatchUsesBackgroundChannel(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	g := &providerGroup{
		ctx:           context.Background(),
		reqCh:         make(chan *Request, 1),
		prioCh:        make(chan *Request, 1),
		hotReqCh:      make(chan *Request),
		hotPrioCh:     make(chan *Request),
		hotIdleBodyCh: make(chan *Request),
		bgCh:          make(chan *Request, 1),
	}
	cl := &Client{ctx: context.Background()}

	go func() {
		_, _, _ = cl.tryGroupTimeout(ctx, g, []byte("BODY <bg@h>\r\n"), nil, nil, laneBackground, 500*time.Millisecond)
	}()

	select {
	case req := <-g.bgCh:
		if req.lane != laneBackground {
			t.Fatalf("request lane = %v, want laneBackground", req.lane)
		}
		req.RespCh <- Response{}
	case <-g.hotIdleBodyCh:
		t.Fatal("background body was steered onto the idle-body channel")
	case <-g.hotPrioCh:
		t.Fatal("background body was dispatched on the priority lane")
	case <-g.hotReqCh:
		t.Fatal("background body was dispatched on the normal lane")
	case <-g.reqCh:
		t.Fatal("background body was dispatched on the cold normal lane")
	case <-time.After(2 * time.Second):
		t.Fatal("background body was never dispatched")
	}
}

func TestProviderBackgroundFloorDefaultsToQuarter(t *testing.T) {
	cases := []struct{ conns, floor, want int32 }{
		{8, 0, 2}, {10, 0, 2}, {3, 0, 1}, {1, 0, 1}, {40, 5, 5}, {8, 100, 7},
	}
	for _, tc := range cases {
		if got := resolveBackgroundFloor(int(tc.conns), int(tc.floor)); got != tc.want {
			t.Errorf("conns=%d floor=%d: got %d, want %d", tc.conns, tc.floor, got, tc.want)
		}
	}
}
