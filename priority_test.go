package nntppool

import (
	"context"
	"net"
	"strings"
	"sync"
	"testing"
	"time"
)

// newLaneTestConn builds a bare NNTPConnection wired only with request
// channels. Nothing else is touched, because tryNextRequest reads nothing else.
func newLaneTestConn(hotPrio, prio, hotReq, req chan *Request) *NNTPConnection {
	return &NNTPConnection{
		hotPrioCh: hotPrio,
		prioCh:    prio,
		hotReqCh:  hotReq,
		reqCh:     req,
	}
}

func TestTryNextRequest_PriorityBeatsNormal(t *testing.T) {
	// The bias is a coin flip today, so a single trial proves nothing.
	// 100 trials make a pre-fix pass astronomically unlikely.
	for i := range 100 {
		prio := make(chan *Request, 1)
		req := make(chan *Request, 1)
		prioReq := &Request{Payload: []byte("BODY <prio>\r\n")}
		normReq := &Request{Payload: []byte("BODY <norm>\r\n")}
		prio <- prioReq
		req <- normReq

		c := newLaneTestConn(nil, prio, nil, req)
		got, ok, found := c.tryNextRequest()
		if !found || !ok {
			t.Fatalf("iteration %d: found=%v ok=%v, want both true", i, found, ok)
		}
		if got != prioReq {
			t.Fatalf("iteration %d: got the normal-lane request, want the priority one", i)
		}
	}
}

func TestTryNextRequest_ColdPriorityBeatsHotNormal(t *testing.T) {
	// A priority request bound for a cold connection must outrank a normal
	// request bound for a hot one. Today hotReqCh is probed first.
	prio := make(chan *Request, 1)
	hotReq := make(chan *Request, 1)
	prioReq := &Request{Payload: []byte("BODY <prio>\r\n")}
	hotReq <- &Request{Payload: []byte("BODY <hotnorm>\r\n")}
	prio <- prioReq

	c := newLaneTestConn(nil, prio, hotReq, nil)
	got, ok, found := c.tryNextRequest()
	if !found || !ok {
		t.Fatalf("found=%v ok=%v, want both true", found, ok)
	}
	if got != prioReq {
		t.Fatal("hot normal request outranked a priority request")
	}
}

func TestTryNextRequest_HotPriorityFirst(t *testing.T) {
	hotPrio := make(chan *Request, 1)
	prio := make(chan *Request, 1)
	hotPrioReq := &Request{Payload: []byte("BODY <hotprio>\r\n")}
	hotPrio <- hotPrioReq
	prio <- &Request{Payload: []byte("BODY <coldprio>\r\n")}

	c := newLaneTestConn(hotPrio, prio, nil, nil)
	got, _, found := c.tryNextRequest()
	if !found || got != hotPrioReq {
		t.Fatal("hot priority channel must be probed before the cold one")
	}
}

func TestTryNextRequest_EmptyReturnsNotGot(t *testing.T) {
	c := newLaneTestConn(nil, nil, nil, nil)
	if _, _, found := c.tryNextRequest(); found {
		t.Fatal("all-nil channels must report got=false")
	}
}

// bodySteeringServer answers BODY with a small yEnc article and records which
// connection index served each message-id. The id in slowID blocks until
// release is closed, holding that connection's reader busy.
type bodySteeringServer struct {
	mu       sync.Mutex
	servedBy map[string]int // message-id -> connection index
	conns    int
	slowID   string
	release  chan struct{}
	started  chan struct{} // closed once slowID has reached the server
}

func (s *bodySteeringServer) factory(t *testing.T) ConnFactory {
	t.Helper()
	return func(ctx context.Context) (net.Conn, error) {
		client, server := net.Pipe()
		s.mu.Lock()
		idx := s.conns
		s.conns++
		s.mu.Unlock()

		go func() {
			defer func() { _ = server.Close() }()
			_, _ = server.Write([]byte("200 ready\r\n"))
			buf := make([]byte, 4096)
			for {
				n, err := server.Read(buf)
				if err != nil {
					return
				}
				cmd := strings.TrimRight(string(buf[:n]), "\r\n")
				if strings.HasPrefix(cmd, "DATE") {
					_, _ = server.Write([]byte("111 20240101000000\r\n"))
					continue
				}
				if !strings.HasPrefix(cmd, "BODY ") {
					_, _ = server.Write([]byte("500 unsupported\r\n"))
					continue
				}
				id := strings.Trim(strings.TrimPrefix(cmd, "BODY "), "<>")
				s.mu.Lock()
				s.servedBy[id] = idx
				s.mu.Unlock()

				if id == s.slowID {
					close(s.started) // the connection is now genuinely busy
					<-s.release      // hold its reader until the test releases it
				}
				_, _ = server.Write(yencSinglePart([]byte("payload"), "f.bin"))
			}
		}()
		return client, nil
	}
}

// TestPriorityBodyAvoidsBusyConnection pins the property phase 1b exists for:
// while one connection is draining a body, a priority body must be steered to a
// connection that is free, not queued behind the in-flight one.
func TestPriorityBodyAvoidsBusyConnection(t *testing.T) {
	srv := &bodySteeringServer{
		servedBy: map[string]int{},
		slowID:   "slow@h",
		release:  make(chan struct{}),
		started:  make(chan struct{}),
	}
	c, err := NewClient(context.Background(), []Provider{{
		Factory:        srv.factory(t),
		Connections:    2,
		MinConnections: 2, // pre-warm both so neither has to dial mid-test
		Inflight:       1,
		SkipPing:       true,
	}})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Occupy one connection with a normal-lane body that will not complete.
	slowDone := make(chan struct{})
	go func() {
		defer close(slowDone)
		_, _ = c.Body(ctx, "slow@h")
	}()

	// Block on a signal rather than polling: the test must not proceed until a
	// connection is genuinely busy, and a sleep would make that a race.
	select {
	case <-srv.started:
	case <-time.After(10 * time.Second):
		t.Fatal("slow body never reached the server")
	}

	if _, err := c.BodyPriority(ctx, "fast@h"); err != nil {
		t.Fatalf("priority body: %v", err)
	}

	srv.mu.Lock()
	slowConn, fastConn := srv.servedBy["slow@h"], srv.servedBy["fast@h"]
	srv.mu.Unlock()

	if slowConn == fastConn {
		t.Fatalf("priority body landed on connection %d, which was already draining a body", fastConn)
	}

	close(srv.release)
	<-slowDone
}

func TestIdleBodyChanNilWhenBusy(t *testing.T) {
	hotIdle := make(chan *Request, 1)
	c := &NNTPConnection{
		hotIdleBodyCh: hotIdle,
		bodySem:       make(chan struct{}, 1),
	}
	if c.idleBodyChan() == nil {
		t.Fatal("a body-free connection must offer its idle-body channel")
	}
	c.bodySem <- struct{}{} // now a body is in flight
	if c.idleBodyChan() != nil {
		t.Fatal("a busy connection must not offer its idle-body channel")
	}
}
