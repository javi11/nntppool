package nntppool

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/javi11/rapidyenc"
)

// --- PostHeaders tests ---

func TestPostHeaders_WriteTo(t *testing.T) {
	t.Run("required fields only", func(t *testing.T) {
		h := PostHeaders{
			From:       "user@example.com",
			Subject:    "Test article",
			Newsgroups: []string{"alt.test"},
		}
		var buf bytes.Buffer
		n, err := h.WriteTo(&buf)
		if err != nil {
			t.Fatalf("WriteTo error: %v", err)
		}
		if int64(buf.Len()) != n {
			t.Errorf("WriteTo returned n=%d, buf.Len()=%d", n, buf.Len())
		}

		got := buf.String()
		if !strings.Contains(got, "From: user@example.com\r\n") {
			t.Error("missing From header")
		}
		if !strings.Contains(got, "Subject: Test article\r\n") {
			t.Error("missing Subject header")
		}
		if !strings.Contains(got, "Newsgroups: alt.test\r\n") {
			t.Error("missing Newsgroups header")
		}
		if !strings.HasSuffix(got, "\r\n\r\n") {
			t.Error("missing blank line terminator")
		}
		// Should not have Message-ID when empty.
		if strings.Contains(got, "Message-ID") {
			t.Error("unexpected Message-ID header")
		}
	})

	t.Run("all fields", func(t *testing.T) {
		h := PostHeaders{
			From:       "User <user@example.com>",
			Subject:    "Test article",
			Newsgroups: []string{"alt.test", "alt.test2"},
			MessageID:  "<unique123@example.com>",
			Extra: map[string][]string{
				"Organization": {"Test Org"},
				"X-No-Archive": {"yes"},
			},
		}
		var buf bytes.Buffer
		_, err := h.WriteTo(&buf)
		if err != nil {
			t.Fatalf("WriteTo error: %v", err)
		}

		got := buf.String()
		if !strings.Contains(got, "From: User <user@example.com>\r\n") {
			t.Error("missing From header")
		}
		if !strings.Contains(got, "Newsgroups: alt.test,alt.test2\r\n") {
			t.Error("missing/wrong Newsgroups header")
		}
		if !strings.Contains(got, "Message-ID: <unique123@example.com>\r\n") {
			t.Error("missing Message-ID header")
		}
		if !strings.Contains(got, "Organization: Test Org\r\n") {
			t.Error("missing Organization header")
		}
		if !strings.Contains(got, "X-No-Archive: yes\r\n") {
			t.Error("missing X-No-Archive header")
		}
	})
}

// --- POST Client-level integration tests ---

// makePostFactory creates a ConnFactory suitable for Post tests.
// The first connection (from pingProvider) gets a greeting + DATE handler.
// Subsequent connections get a greeting + POST handler that responds with the given status flow.
// receivedArticle, if non-nil, captures the article data from the POST connection.
func makePostFactory(t *testing.T, postResponses []string, receivedArticle *bytes.Buffer) ConnFactory {
	t.Helper()
	var connCount atomic.Int32
	return func(ctx context.Context) (net.Conn, error) {
		n := connCount.Add(1)
		if n == 1 {
			// Ping connection: handle greeting + DATE.
			return mockServer(t, func(s net.Conn) {
				_, _ = s.Write([]byte("200 server ready\r\n"))
				buf := make([]byte, 4096)
				for {
					nr, err := s.Read(buf)
					if err != nil {
						return
					}
					cmd := string(buf[:nr])
					if strings.HasPrefix(cmd, "DATE") {
						_, _ = s.Write([]byte("111 20240315120000\r\n"))
					}
				}
			}), nil
		}
		// POST connection.
		return mockServer(t, func(s net.Conn) {
			_, _ = s.Write([]byte("200 server ready\r\n"))

			buf := make([]byte, 8192)
			var recv bytes.Buffer
			for {
				nr, err := s.Read(buf)
				if nr > 0 {
					recv.Write(buf[:nr])
				}
				if bytes.Contains(recv.Bytes(), []byte("\r\n.\r\n")) {
					break
				}
				if err != nil {
					return
				}
			}

			if receivedArticle != nil {
				receivedArticle.Write(recv.Bytes())
			}

			for _, resp := range postResponses {
				_, _ = fmt.Fprintf(s, "%s\r\n", resp)
			}
		}), nil
	}
}

func TestPostYenc_Success(t *testing.T) {
	var received bytes.Buffer
	factory := makePostFactory(t, []string{"340 send article", "240 article posted ok"}, &received)

	c, err := NewClient(context.Background(), []Provider{
		{Factory: factory, Connections: 1},
	})
	if err != nil {
		t.Fatalf("NewClient error: %v", err)
	}
	defer func() { _ = c.Close() }()

	headers := PostHeaders{
		From:       "user@example.com",
		Subject:    "yEnc test",
		Newsgroups: []string{"alt.binaries.test"},
		MessageID:  "<yenc-test@example.com>",
	}

	data := bytes.Repeat([]byte("ABCDEFGHIJ"), 100) // 1000 bytes
	meta := rapidyenc.Meta{
		FileName:   "test.bin",
		FileSize:   int64(len(data)),
		PartNumber: 1,
		TotalParts: 1,
		Offset:     0,
		PartSize:   int64(len(data)),
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	result, err := c.PostYenc(ctx, headers, bytes.NewReader(data), meta)
	if err != nil {
		t.Fatalf("PostYenc error: %v", err)
	}
	if result.StatusCode != 240 {
		t.Errorf("StatusCode = %d, want 240", result.StatusCode)
	}

	// Verify yEnc markers are present.
	got := received.String()
	if !strings.Contains(got, "=ybegin") {
		t.Error("missing =ybegin header")
	}
	if !strings.Contains(got, "=yend") {
		t.Error("missing =yend trailer")
	}
	if !strings.Contains(got, "name=test.bin") {
		t.Error("missing filename in yEnc header")
	}
}

func TestPostYenc_MultiPart(t *testing.T) {
	var received bytes.Buffer
	factory := makePostFactory(t, []string{"340 send article", "240 article posted ok"}, &received)

	c, err := NewClient(context.Background(), []Provider{
		{Factory: factory, Connections: 1},
	})
	if err != nil {
		t.Fatalf("NewClient error: %v", err)
	}
	defer func() { _ = c.Close() }()

	headers := PostHeaders{
		From:       "user@example.com",
		Subject:    "yEnc multipart test (1/3)",
		Newsgroups: []string{"alt.binaries.test"},
	}

	partData := bytes.Repeat([]byte("X"), 500)
	meta := rapidyenc.Meta{
		FileName:   "multi.bin",
		FileSize:   1500,
		PartNumber: 1,
		TotalParts: 3,
		Offset:     0,
		PartSize:   500,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	result, err := c.PostYenc(ctx, headers, bytes.NewReader(partData), meta)
	if err != nil {
		t.Fatalf("PostYenc error: %v", err)
	}
	if result.StatusCode != 240 {
		t.Errorf("StatusCode = %d, want 240", result.StatusCode)
	}

	got := received.String()
	if !strings.Contains(got, "=ybegin") {
		t.Error("missing =ybegin header")
	}
	if !strings.Contains(got, "=ypart") {
		t.Error("missing =ypart header for multipart")
	}
	if !strings.Contains(got, "=yend") {
		t.Error("missing =yend trailer")
	}
	if !strings.Contains(got, "total=3") {
		t.Error("missing total=3 in yEnc header")
	}
}

// --- NNTPConnection-level POST test ---

func TestNNTPConnection_PostTwoPhase(t *testing.T) {
	conn := mockServer(t, func(s net.Conn) {
		_, _ = s.Write([]byte("200 server ready\r\n"))

		buf := make([]byte, 8192)
		var recv bytes.Buffer
		for {
			n, err := s.Read(buf)
			if n > 0 {
				recv.Write(buf[:n])
			}
			if bytes.Contains(recv.Bytes(), []byte("\r\n.\r\n")) {
				break
			}
			if err != nil {
				return
			}
		}

		_, _ = s.Write([]byte("340 send article\r\n"))
		_, _ = s.Write([]byte("240 article posted\r\n"))
	})

	reqCh := make(chan *Request, 1)
	nc, err := newNNTPConnectionFromConn(context.Background(), conn, 1, reqCh, nil, Auth{}, nil, nil)
	if err != nil {
		t.Fatalf("connection error = %v", err)
	}

	// Build a simple article payload.
	pr, pw := io.Pipe()
	go func() {
		_, _ = pw.Write([]byte("From: test@example.com\r\nSubject: Test\r\nNewsgroups: alt.test\r\n\r\nBody text\r\n.\r\n"))
		_ = pw.Close()
	}()

	respCh := make(chan Response, 1)
	reqCh <- &Request{
		Ctx:         context.Background(),
		Payload:     []byte("POST\r\n"),
		RespCh:      respCh,
		PayloadBody: pr,
		PostMode:    true,
	}

	go nc.Run()

	select {
	case resp := <-respCh:
		if resp.Err != nil {
			t.Fatalf("response error = %v", resp.Err)
		}
		if resp.StatusCode != 240 {
			t.Errorf("StatusCode = %d, want 240", resp.StatusCode)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for response")
	}

	_ = nc.Close()
}
