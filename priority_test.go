package nntppool

import "testing"

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
