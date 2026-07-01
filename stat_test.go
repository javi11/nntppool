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

// makeStatByIDFactory returns a ConnFactory whose mock server answers STAT
// commands based on message-id. replies maps a bare message-id to the status
// line to return; ids absent from the map get a 430. Every received command
// (except DATE pings) is appended to cmdLog under mu.
func makeStatByIDFactory(t *testing.T, mu *sync.Mutex, cmdLog *[]string, replies map[string]string) ConnFactory {
	t.Helper()
	return func(ctx context.Context) (net.Conn, error) {
		client, server := net.Pipe()
		go func() {
			defer func() { _ = server.Close() }()
			_, _ = server.Write([]byte("200 server ready\r\n"))
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
				if mu != nil {
					mu.Lock()
					*cmdLog = append(*cmdLog, cmd)
					mu.Unlock()
				}
				reply := "430 no such article"
				if strings.HasPrefix(cmd, "STAT ") {
					id := strings.Trim(strings.TrimPrefix(cmd, "STAT "), "<>")
					if r, ok := replies[id]; ok {
						reply = r
					}
				}
				_, _ = fmt.Fprintf(server, "%s\r\n", reply)
			}
		}()
		return client, nil
	}
}

// collectStat drains a StatMany/StatAsync channel into a map keyed by message-id.
func collectStat(ch <-chan StatManyResult) map[string]StatManyResult {
	m := make(map[string]StatManyResult)
	for r := range ch {
		m[r.MessageID] = r
	}
	return m
}

func TestStatMany_AllExist(t *testing.T) {
	ids := []string{"a@h", "b@h", "c@h"}
	replies := map[string]string{
		"a@h": "223 1 <a@h> exists",
		"b@h": "223 2 <b@h> exists",
		"c@h": "223 3 <c@h> exists",
	}
	c, err := NewClient(context.Background(), []Provider{{
		Factory:     makeStatByIDFactory(t, nil, nil, replies),
		Connections: 3,
	}})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close() }()

	got := collectStat(c.StatMany(context.Background(), ids, StatManyOptions{}))
	if len(got) != len(ids) {
		t.Fatalf("got %d results, want %d", len(got), len(ids))
	}
	for _, id := range ids {
		r, ok := got[id]
		if !ok {
			t.Fatalf("missing result for %s", id)
		}
		if r.Err != nil {
			t.Errorf("%s: unexpected err %v", id, r.Err)
		}
		if r.Result == nil {
			t.Fatalf("%s: nil result", id)
		}
		if r.Result.MessageID != id {
			t.Errorf("%s: MessageID = %q", id, r.Result.MessageID)
		}
	}
}

func TestStatMany_MissingIsNonFatal(t *testing.T) {
	ids := []string{"hit@h", "miss@h"}
	replies := map[string]string{
		"hit@h": "223 1 <hit@h> exists",
		// miss@h absent => 430
	}
	c, err := NewClient(context.Background(), []Provider{{
		Factory:     makeStatByIDFactory(t, nil, nil, replies),
		Connections: 2,
	}})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close() }()

	got := collectStat(c.StatMany(context.Background(), ids, StatManyOptions{}))
	if len(got) != 2 {
		t.Fatalf("got %d results, want 2", len(got))
	}
	if r := got["hit@h"]; r.Err != nil || r.Result == nil {
		t.Errorf("hit: err=%v result=%v", r.Err, r.Result)
	}
	miss := got["miss@h"]
	if !errors.Is(miss.Err, ErrArticleNotFound) {
		t.Errorf("miss: err = %v, want ErrArticleNotFound", miss.Err)
	}
	if miss.Result != nil {
		t.Errorf("miss: result = %v, want nil", miss.Result)
	}
}

func TestStatMany_Completeness(t *testing.T) {
	const total = 200
	ids := make([]string, total)
	replies := make(map[string]string, total)
	for i := range ids {
		id := fmt.Sprintf("seg%d@h", i)
		ids[i] = id
		replies[id] = fmt.Sprintf("223 %d <%s> exists", i, id)
	}
	c, err := NewClient(context.Background(), []Provider{{
		Factory:     makeStatByIDFactory(t, nil, nil, replies),
		Connections: 8,
	}})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close() }()

	got := collectStat(c.StatMany(context.Background(), ids, StatManyOptions{Concurrency: 16}))
	if len(got) != total {
		t.Fatalf("got %d unique results, want %d", len(got), total)
	}
	for _, id := range ids {
		if r, ok := got[id]; !ok || r.Result == nil {
			t.Fatalf("bad/missing result for %s: %+v", id, r)
		}
	}
}

func TestStatMany_ContextCancel(t *testing.T) {
	const total = 500
	ids := make([]string, total)
	replies := make(map[string]string, total)
	for i := range ids {
		id := fmt.Sprintf("seg%d@h", i)
		ids[i] = id
		replies[id] = fmt.Sprintf("223 %d <%s> exists", i, id)
	}
	c, err := NewClient(context.Background(), []Provider{{
		Factory:     makeStatByIDFactory(t, nil, nil, replies),
		Connections: 4,
	}})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close() }()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ch := c.StatMany(ctx, ids, StatManyOptions{Concurrency: 4})
	// Read a few then cancel.
	count := 0
	for range ch {
		count++
		if count == 5 {
			cancel()
		}
	}
	// Channel must close (loop exits) without panic/deadlock. We don't assert an
	// exact count — cancellation is racy — only that it terminates.
	if count == 0 {
		t.Fatal("expected at least some results before cancel")
	}
}

func TestStatMany_ProviderTargeting(t *testing.T) {
	var mu1, mu2 sync.Mutex
	var p1Cmds, p2Cmds []string
	replies := map[string]string{"x@h": "223 1 <x@h> exists"}

	c, err := NewClient(context.Background(), []Provider{
		{
			Host:        "provider-one:119",
			Factory:     makeStatByIDFactory(t, &mu1, &p1Cmds, replies),
			Connections: 2,
		},
		{
			Host:        "provider-two:119",
			Factory:     makeStatByIDFactory(t, &mu2, &p2Cmds, replies),
			Connections: 2,
		},
	}, WithStatProbe(false))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close() }()

	got := collectStat(c.StatMany(context.Background(),
		[]string{"x@h"}, StatManyOptions{Provider: "provider-two:119"}))
	if r := got["x@h"]; r.Err != nil || r.Result == nil {
		t.Fatalf("x@h: err=%v result=%v", r.Err, r.Result)
	}

	mu1.Lock()
	defer mu1.Unlock()
	mu2.Lock()
	defer mu2.Unlock()
	if len(p1Cmds) != 0 {
		t.Errorf("provider-one received %v, want none (targeted provider-two)", p1Cmds)
	}
	if len(p2Cmds) == 0 {
		t.Errorf("provider-two received no STAT commands")
	}
}

func TestStatMany_UnknownProvider(t *testing.T) {
	c, err := NewClient(context.Background(), []Provider{{
		Factory:     makeStatByIDFactory(t, nil, nil, map[string]string{"a@h": "223 1 <a@h> exists"}),
		Connections: 1,
	}})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close() }()

	got := collectStat(c.StatMany(context.Background(),
		[]string{"a@h"}, StatManyOptions{Provider: "does-not-exist"}))
	if r := got["a@h"]; r.Err == nil {
		t.Errorf("want error for unknown provider, got result %v", r.Result)
	}
}

func TestStatPriority(t *testing.T) {
	c, err := NewClient(context.Background(), []Provider{{
		Factory:     makeStatByIDFactory(t, nil, nil, map[string]string{"p@h": "223 7 <p@h> exists"}),
		Connections: 1,
	}})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close() }()

	res, err := c.StatPriority(context.Background(), "p@h")
	if err != nil {
		t.Fatalf("StatPriority: %v", err)
	}
	if res.Number != 7 || res.MessageID != "p@h" {
		t.Errorf("got %+v, want Number=7 MessageID=p@h", res)
	}
}

func TestStatAsync(t *testing.T) {
	c, err := NewClient(context.Background(), []Provider{{
		Factory:     makeStatByIDFactory(t, nil, nil, map[string]string{"async@h": "223 9 <async@h> exists"}),
		Connections: 1,
	}})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close() }()

	select {
	case r := <-c.StatAsync(context.Background(), "async@h"):
		if r.Err != nil {
			t.Fatalf("StatAsync err: %v", r.Err)
		}
		if r.Result == nil || r.Result.Number != 9 {
			t.Errorf("got %+v, want Number=9", r.Result)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("StatAsync timed out")
	}
}
