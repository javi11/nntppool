package nntppool

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mnightingale/rapidyenc"
)

type countingReader struct {
	r io.Reader
	n atomic.Int64
}

func (r *countingReader) Read(p []byte) (int, error) {
	n, err := r.r.Read(p)
	r.n.Add(int64(n))
	return n, err
}

func postTestHeaders(messageID string) PostHeaders {
	return PostHeaders{
		From:       "user@example.com",
		Subject:    "max connections test",
		Newsgroups: []string{"alt.test"},
		MessageID:  messageID,
	}
}

func postTestMeta(size int) rapidyenc.Meta {
	return rapidyenc.Meta{
		FileName:   "test.bin",
		FileSize:   int64(size),
		PartNumber: 1,
		TotalParts: 1,
		PartSize:   int64(size),
	}
}

func serveAuth(t *testing.T, conn net.Conn, passStatus string) (*bufio.Reader, bool) {
	t.Helper()
	r := bufio.NewReader(conn)
	_, _ = io.WriteString(conn, "200 server ready\r\n")

	line, err := r.ReadString('\n')
	if err != nil {
		return r, false
	}
	if line != "AUTHINFO USER testuser\r\n" {
		t.Errorf("AUTHINFO USER = %q", line)
		return r, false
	}
	_, _ = io.WriteString(conn, "381 password required\r\n")

	line, err = r.ReadString('\n')
	if err != nil {
		return r, false
	}
	if line != "AUTHINFO PASS testpass\r\n" {
		t.Errorf("AUTHINFO PASS = %q", line)
		return r, false
	}
	_, _ = fmt.Fprintf(conn, "%s\r\n", passStatus)
	return r, strings.HasPrefix(passStatus, "281")
}

func TestRunConnSlot_MaxConnectionsPreservesPriorityLane(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	reqCh := make(chan *Request, 1)
	prioCh := make(chan *Request, 1)
	hotReqCh := make(chan *Request)
	hotPrioCh := make(chan *Request)
	hotIdleBodyCh := make(chan *Request)
	req := &Request{Ctx: ctx, RespCh: make(chan Response, 1)}
	prioCh <- req

	factory := func(context.Context) (net.Conn, error) {
		return mockServer(t, func(conn net.Conn) {
			_, _ = serveAuth(t, conn, "502 too many connections")
		}), nil
	}
	gate := newConnGate(2, time.Hour)
	gate.markRunning() // Model a separate established connection.
	var wg sync.WaitGroup
	wg.Add(1)
	go runConnSlot(ctx, reqCh, prioCh, hotReqCh, hotPrioCh, hotIdleBodyCh, factory, 1, 1, 0, 0,
		Auth{Username: "testuser", Password: "testpass"}, "", 0, 0, 0, "",
		gate, &providerStats{}, "posting", &wg, false)

	select {
	case got := <-hotPrioCh:
		if got != req {
			t.Fatalf("hot priority request = %p, want original %p", got, req)
		}
	case got := <-hotReqCh:
		t.Fatalf("priority request was demoted to normal lane: %p", got)
	case <-time.After(2 * time.Second):
		t.Fatal("priority request was not handed to a hot connection")
	}

	cancel()
	wg.Wait()
	select {
	case resp := <-req.RespCh:
		t.Fatalf("request was failed during safe handoff: %v", resp.Err)
	default:
	}
}

func TestRunConnSlot_ThrottledGateHandsRequestToActiveConnection(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	reqCh := make(chan *Request, 1)
	prioCh := make(chan *Request, 1)
	hotReqCh := make(chan *Request)
	hotPrioCh := make(chan *Request)
	hotIdleBodyCh := make(chan *Request)
	req := &Request{Ctx: ctx, RespCh: make(chan Response, 1), PostMode: true}
	reqCh <- req

	gate := newConnGate(1, time.Hour)
	if !gate.enter(ctx, ctx) {
		t.Fatal("could not reserve modeled active connection")
	}
	gate.markRunning()

	var dials atomic.Int32
	factory := func(context.Context) (net.Conn, error) {
		dials.Add(1)
		return nil, errors.New("cold slot must not dial")
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go runConnSlot(ctx, reqCh, prioCh, hotReqCh, hotPrioCh, hotIdleBodyCh, factory, 1, 1, 0, 0,
		Auth{}, "", 0, 0, 0, "", gate, &providerStats{}, "posting", &wg, false)

	select {
	case got := <-hotReqCh:
		if got != req {
			t.Fatalf("hot request = %p, want original %p", got, req)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("request remained stranded behind the throttled connection gate")
	}
	if got := dials.Load(); got != 0 {
		t.Fatalf("cold slot dialed %d times while active connection was available", got)
	}

	cancel()
	wg.Wait()
}

func TestPostYenc_MaxConnectionsBootstrapMovesToHotConnection(t *testing.T) {
	var dials atomic.Int32
	healthyReady := make(chan struct{})
	firstBodyReceived := make(chan struct{})
	releaseFirst := make(chan struct{})
	maxConnectionsSeen := make(chan struct{})
	var closeHealthyReady sync.Once

	factory := func(context.Context) (net.Conn, error) {
		switch dials.Add(1) {
		case 1:
			return mockServer(t, func(conn net.Conn) {
				r, ok := serveAuth(t, conn, "281 authentication accepted")
				if !ok {
					return
				}
				closeHealthyReady.Do(func() { close(healthyReady) })

				for article := 0; article < 2; article++ {
					line, err := r.ReadString('\n')
					if err != nil {
						return
					}
					if line != "POST\r\n" {
						t.Errorf("command = %q, want POST", line)
						return
					}
					_, _ = io.WriteString(conn, "340 send article\r\n")

					for {
						line, err = r.ReadString('\n')
						if err != nil {
							return
						}
						if line == ".\r\n" {
							break
						}
					}

					if article == 0 {
						close(firstBodyReceived)
						<-releaseFirst
					}
					_, _ = io.WriteString(conn, "240 article posted ok\r\n")
				}
			}), nil
		case 2:
			return mockServer(t, func(conn net.Conn) {
				_, _ = serveAuth(t, conn, "502 too many connections")
				close(maxConnectionsSeen)
			}), nil
		default:
			return nil, fmt.Errorf("unexpected dial %d", dials.Load())
		}
	}

	c, err := NewClient(context.Background(), []Provider{{
		Name:           "posting",
		Factory:        factory,
		Connections:    2,
		MinConnections: 1,
		Inflight:       1,
		SkipPing:       true,
		Auth:           Auth{Username: "testuser", Password: "testpass"},
	}})
	if err != nil {
		t.Fatalf("NewClient() error = %v", err)
	}
	defer func() { _ = c.Close() }()

	select {
	case <-healthyReady:
	case <-time.After(2 * time.Second):
		t.Fatal("pre-warmed connection did not become ready")
	}

	firstData := bytes.Repeat([]byte("first"), 100)
	firstDone := make(chan error, 1)
	go func() {
		_, err := c.PostYencTo(context.Background(), "posting", postTestHeaders("<first@example.com>"), bytes.NewReader(firstData), postTestMeta(len(firstData)))
		firstDone <- err
	}()

	select {
	case <-firstBodyReceived:
	case <-time.After(2 * time.Second):
		t.Fatal("first POST did not reach the healthy connection")
	}

	secondData := bytes.Repeat([]byte("second"), 100)
	secondBody := &countingReader{r: bytes.NewReader(secondData)}
	secondDone := make(chan error, 1)
	go func() {
		_, err := c.PostYencTo(context.Background(), "posting", postTestHeaders("<second@example.com>"), secondBody, postTestMeta(len(secondData)))
		secondDone <- err
	}()

	select {
	case <-maxConnectionsSeen:
	case <-time.After(2 * time.Second):
		t.Fatal("cold slot did not encounter max connections")
	}
	if got := secondBody.n.Load(); got != 0 {
		t.Fatalf("second body consumed during failed bootstrap: %d bytes", got)
	}

	close(releaseFirst)
	select {
	case err := <-firstDone:
		if err != nil {
			t.Fatalf("first PostYencTo() error = %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("first POST did not complete")
	}
	select {
	case err := <-secondDone:
		if err != nil {
			t.Fatalf("second PostYencTo() error = %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("requeued POST did not complete on the healthy connection")
	}
	if got := secondBody.n.Load(); got != int64(len(secondData)) {
		t.Fatalf("second body consumed %d bytes, want exactly %d", got, len(secondData))
	}
}

func TestPostYenc_MaxConnectionsBootstrapFailsWithoutHotConnection(t *testing.T) {
	factory := func(context.Context) (net.Conn, error) {
		return mockServer(t, func(conn net.Conn) {
			_, _ = serveAuth(t, conn, "502 too many connections")
		}), nil
	}

	c, err := NewClient(context.Background(), []Provider{{
		Name:        "posting",
		Factory:     factory,
		Connections: 1,
		SkipPing:    true,
		Auth:        Auth{Username: "testuser", Password: "testpass"},
	}})
	if err != nil {
		t.Fatalf("NewClient() error = %v", err)
	}
	defer func() { _ = c.Close() }()

	data := bytes.Repeat([]byte("body"), 100)
	body := &countingReader{r: bytes.NewReader(data)}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	start := time.Now()
	_, err = c.PostYencTo(ctx, "posting", postTestHeaders("<waiting@example.com>"), body, postTestMeta(len(data)))
	if !errors.Is(err, ErrMaxConnections) {
		t.Fatalf("PostYencTo() error = %v, want ErrMaxConnections", err)
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("PostYencTo() did not fail promptly without a hot connection: %v", elapsed)
	}
	if got := body.n.Load(); got != 0 {
		t.Fatalf("body consumed while waiting for capacity: %d bytes", got)
	}
}

func TestPostYenc_NonTransientBootstrapAuthErrorStillFails(t *testing.T) {
	factory := func(context.Context) (net.Conn, error) {
		return mockServer(t, func(conn net.Conn) {
			_, _ = serveAuth(t, conn, "481 authentication rejected")
		}), nil
	}

	c, err := NewClient(context.Background(), []Provider{{
		Name:        "posting",
		Factory:     factory,
		Connections: 1,
		SkipPing:    true,
		Auth:        Auth{Username: "testuser", Password: "testpass"},
	}})
	if err != nil {
		t.Fatalf("NewClient() error = %v", err)
	}
	defer func() { _ = c.Close() }()

	data := bytes.Repeat([]byte("body"), 100)
	body := &countingReader{r: bytes.NewReader(data)}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	start := time.Now()
	_, err = c.PostYencTo(ctx, "posting", postTestHeaders("<auth@example.com>"), body, postTestMeta(len(data)))
	if err == nil || !strings.Contains(err.Error(), "481") {
		t.Fatalf("PostYencTo() error = %v, want 481 authentication failure", err)
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("permanent authentication error did not fail promptly: %v", elapsed)
	}
	if got := body.n.Load(); got != 0 {
		t.Fatalf("body consumed after permanent authentication failure: %d bytes", got)
	}
}
