package nntppool

import (
	"bytes"
	"context"
	"net"
	"testing"
	"time"
)

// bodyServer is a provider that answers every BODY with payload and anything
// else (the startup DATE ping, a STAT probe) with a terse success.
func bodyServer(t *testing.T, payload []byte) ConnFactory {
	t.Helper()
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
				switch {
				case bytes.Contains(buf[:n], []byte("BODY")):
					_, _ = server.Write(yencSinglePart(payload, "test.bin"))
				case bytes.Contains(buf[:n], []byte("STAT")):
					_, _ = server.Write([]byte("223 0 <buffered@test>\r\n"))
				default:
					_, _ = server.Write([]byte("111 20260101120000\r\n"))
				}
			}
		}()
		return client, nil
	}
}

func newTestClient(t *testing.T, f ConnFactory) *Client {
	t.Helper()
	c, err := NewClient(context.Background(), []Provider{{Factory: f, Connections: 1}})
	if err != nil {
		t.Fatalf("NewClient() error = %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

// FetchAsync with no Writer buffers into ArticleBody.Bytes. v4's BodyAsync
// took the writer as a positional argument and had no buffered form at all —
// callers passed io.Discard and lost the payload — so this contract is new and
// worth pinning.
func TestFetchAsyncNilWriterBuffersBytes(t *testing.T) {
	payload := bytes.Repeat([]byte("async buffered payload "), 512)
	c := newTestClient(t, bodyServer(t, payload))

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	res := <-c.FetchAsync(ctx, Req{MessageID: "buffered@test"})
	if res.Err != nil {
		t.Fatalf("FetchAsync() error = %v", res.Err)
	}
	if !bytes.Equal(res.Body.Bytes, payload) {
		t.Fatalf("Bytes = %d bytes, want %d", len(res.Body.Bytes), len(payload))
	}
}

// The same Req drives a buffered and a streamed fetch; only where the bytes
// land differs. Bytes and Writer are mutually exclusive by construction, and
// that is the one invariant the struct cannot express in its type.
func TestFetchWriterSelectsDelivery(t *testing.T) {
	payload := bytes.Repeat([]byte("delivery mode payload "), 512)
	c := newTestClient(t, bodyServer(t, payload))

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	buffered, err := c.Fetch(ctx, Req{MessageID: "buffered@test"})
	if err != nil {
		t.Fatalf("buffered Fetch() error = %v", err)
	}
	if !bytes.Equal(buffered.Bytes, payload) {
		t.Fatalf("buffered Bytes = %d, want %d", len(buffered.Bytes), len(payload))
	}

	var sink bytes.Buffer
	streamed, err := c.Fetch(ctx, Req{MessageID: "buffered@test", Writer: &sink})
	if err != nil {
		t.Fatalf("streamed Fetch() error = %v", err)
	}
	if streamed.Bytes != nil {
		t.Fatalf("Bytes must be nil when a Writer is set, got %d bytes", len(streamed.Bytes))
	}
	if !bytes.Equal(sink.Bytes(), payload) {
		t.Fatalf("writer got %d bytes, want %d", sink.Len(), len(payload))
	}
}

// Lane is the zero value of a Req that says nothing about lanes, so the
// default stays normal as the field is added to and reordered.
func TestLaneZeroValueIsNormal(t *testing.T) {
	if (Req{}).Lane != LaneNormal {
		t.Fatalf("Req{}.Lane = %v, want LaneNormal", (Req{}).Lane)
	}
	if (ManyOptions{}).Lane != LaneNormal {
		t.Fatalf("ManyOptions{}.Lane = %v, want LaneNormal", (ManyOptions{}).Lane)
	}
	for _, tc := range []struct {
		l    Lane
		want string
	}{{LaneNormal, "normal"}, {LanePriority, "priority"}, {LaneBackground, "background"}, {Lane(9), "lane(9)"}} {
		if got := tc.l.String(); got != tc.want {
			t.Errorf("Lane(%d).String() = %q, want %q", tc.l, got, tc.want)
		}
	}
}

// ExistsAsync reports the message-ID alongside the verdict, so a caller
// fanning several out can tell the answers apart.
func TestExistsAsyncCarriesMessageID(t *testing.T) {
	c := newTestClient(t, bodyServer(t, []byte("x")))

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	res := <-c.ExistsAsync(ctx, Req{MessageID: "buffered@test", Lane: LanePriority})
	if res.Err != nil {
		t.Fatalf("ExistsAsync() error = %v", res.Err)
	}
	if res.MessageID != "buffered@test" {
		t.Fatalf("MessageID = %q, want %q", res.MessageID, "buffered@test")
	}
}
