package nntppool

import (
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
