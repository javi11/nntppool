package nntppool

import (
	"bytes"
	"context"
	"math/rand"
	"net"
	"strings"
	"sync"
	"testing"
	"time"
)

// bodyServerFactory returns a ConnFactory for a healthy server that answers
// every BODY at once with the same yEnc-encoded body.
func bodyServerFactory(body []byte) ConnFactory {
	return func(ctx context.Context) (net.Conn, error) {
		client, server := net.Pipe()
		go func() {
			defer func() { _ = server.Close() }()
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
						wmu.Lock()
						defer wmu.Unlock()
						_, _ = server.Write(yencSinglePart(body, "part.bin"))
					}()
				}
			}
		}()
		return client, nil
	}
}

// slowFirstWriteWriter is a caller's body writer whose first Write takes
// stall: the reader goroutine descheduled, or a consumer briefly holding its
// lock, right after the first bytes came off the wire.
type slowFirstWriteWriter struct {
	mu    sync.Mutex
	buf   bytes.Buffer
	once  sync.Once
	stall time.Duration
}

func (w *slowFirstWriteWriter) Write(p []byte) (int, error) {
	w.once.Do(func() { time.Sleep(w.stall) })
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.buf.Write(p)
}

func (w *slowFirstWriteWriter) Bytes() []byte {
	w.mu.Lock()
	defer w.mu.Unlock()
	return append([]byte(nil), w.buf.Bytes()...)
}

// An attempt timer must not fail a request over once the attempt's first
// decoded bytes are on their way into the caller's writer. The read loop used
// to mark the attempt committed only on the read after the first chunk had
// been decoded into the writer, so a timer firing in between abandoned the
// attempt and the next provider wrote the whole body again behind that chunk:
// the caller received the first chunk followed by the body from byte 0.
func TestAttemptTimerDoesNotSpliceBodiesInCallerWriter(t *testing.T) {
	body := make([]byte, 768_000)
	rand.New(rand.NewSource(1)).Read(body)

	c, err := NewClient(context.Background(), []Provider{
		{Factory: bodyServerFactory(body), Connections: 1, SkipPing: true, AttemptTimeout: 200 * time.Millisecond},
		{Factory: bodyServerFactory(body), Connections: 1, SkipPing: true},
	}, WithDispatchStrategy(DispatchFIFO))
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer func() { _ = c.Close() }()

	// The first Write outlasts the first provider's attempt window.
	w := &slowFirstWriteWriter{stall: 700 * time.Millisecond}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	resp := <-c.Send(ctx, SendReq{Payload: []byte("BODY <a@b>\r\n"), Writer: w})
	// Give a writer still held by an abandoned attempt time to land its bytes.
	time.Sleep(300 * time.Millisecond)

	if resp.Err != nil {
		t.Fatalf("Send: %v", resp.Err)
	}
	got := w.Bytes()
	if !bytes.Equal(got, body) {
		at := 0
		for at < min(len(got), len(body)) && got[at] == body[at] {
			at++
		}
		t.Fatalf("writer received %d bytes, want the %d-byte body; first wrong byte at offset %d (a second attempt's body appended behind the first attempt's chunk)",
			len(got), len(body), at)
	}
}

// An attempt the timer already abandoned must not write anything into the
// caller's writer; only the attempt that took over does.
func TestAbandonedAttemptWritesNothing(t *testing.T) {
	body := make([]byte, 64_000)
	rand.New(rand.NewSource(2)).Read(body)

	req := &Request{Ctx: context.Background(), BodyWriter: &bytes.Buffer{}}
	req.attemptState.Store(attemptAbandoned)
	var out bytes.Buffer
	ref := &writerRef{w: &out, claim: func() bool {
		return req.attemptState.CompareAndSwap(attemptPending, attemptCommitted) ||
			req.attemptState.Load() == attemptCommitted
	}}
	if _, err := ref.Write(body); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if out.Len() != 0 {
		t.Fatalf("abandoned attempt wrote %d bytes into the caller's writer, want 0", out.Len())
	}

	req2 := &Request{Ctx: context.Background()}
	var out2 bytes.Buffer
	ref2 := &writerRef{w: &out2, claim: func() bool {
		return req2.attemptState.CompareAndSwap(attemptPending, attemptCommitted) ||
			req2.attemptState.Load() == attemptCommitted
	}}
	if _, err := ref2.Write(body); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if got := req2.attemptState.Load(); got != attemptCommitted {
		t.Fatalf("attempt state after first write = %d, want attemptCommitted (%d)", got, attemptCommitted)
	}
	if !bytes.Equal(out2.Bytes(), body) {
		t.Fatalf("pending attempt wrote %d bytes, want the %d-byte body", out2.Len(), len(body))
	}
}
