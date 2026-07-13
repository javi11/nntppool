package nntppool

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

const regressionBodySize = 2 * 1024 * 1024

type blockingWriter struct {
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

func (w *blockingWriter) Write(p []byte) (int, error) {
	w.once.Do(func() { close(w.started) })
	<-w.release
	return len(p), nil
}

type failingWriter struct {
	err error
}

func (w failingWriter) Write([]byte) (int, error) {
	return 0, w.err
}

type regressionProvider struct {
	host        string
	connections atomic.Int32
	mu          sync.Mutex
	commands    []string
	respond     func(connection int, command string) []byte
}

func (p *regressionProvider) provider(backup bool) Provider {
	return Provider{
		Host:        p.host,
		Factory:     p.factory,
		Connections: 1,
		Inflight:    1,
		Backup:      backup,
		SkipPing:    true,
	}
}

func (p *regressionProvider) factory(context.Context) (net.Conn, error) {
	connection := int(p.connections.Add(1))
	client, server := net.Pipe()
	go func() {
		defer func() { _ = server.Close() }()
		if _, err := server.Write([]byte("200 regression server ready\r\n")); err != nil {
			return
		}
		reader := bufio.NewReader(server)
		for {
			command, err := reader.ReadString('\n')
			if err != nil {
				return
			}
			command = strings.TrimSuffix(strings.TrimSuffix(command, "\n"), "\r")
			p.mu.Lock()
			p.commands = append(p.commands, command)
			p.mu.Unlock()
			response := p.respond(connection, command)
			if len(response) == 0 {
				response = []byte("500 unexpected regression command\r\n")
			}
			if _, err := server.Write(response); err != nil {
				return
			}
		}
	}()
	return client, nil
}

func (p *regressionProvider) commandCount(prefix string) int {
	p.mu.Lock()
	defer p.mu.Unlock()
	count := 0
	for _, command := range p.commands {
		if strings.HasPrefix(command, prefix) {
			count++
		}
	}
	return count
}

func newRegressionClient(t *testing.T, providers ...Provider) *Client {
	t.Helper()
	client, err := NewClient(
		context.Background(),
		providers,
		WithDispatchStrategy(DispatchFIFO),
	)
	if err != nil {
		t.Fatalf("NewClient() error = %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })
	return client
}

func corruptCRC(response []byte) []byte {
	corrupt := bytes.Clone(response)
	marker := []byte("crc32=")
	index := bytes.Index(corrupt, marker)
	if index < 0 {
		panic("yEnc fixture did not contain a CRC")
	}
	copy(corrupt[index+len(marker):], "deadbeef")
	return corrupt
}

func removeYEnd(response []byte) []byte {
	truncated := bytes.Clone(response)
	start := bytes.Index(truncated, []byte("\r\n=yend "))
	if start < 0 {
		panic("yEnc fixture did not contain =yend")
	}
	end := bytes.Index(truncated[start+2:], []byte("\r\n"))
	if end < 0 {
		panic("yEnc fixture contained an unterminated =yend")
	}
	end += start + 4
	return append(truncated[:start], truncated[end:]...)
}

func TestPR1PrebufferedDecodedBytesCommitAttempt(t *testing.T) {
	firstBody := yencSinglePart([]byte("first response"), "first.bin")
	secondBody := yencSinglePart([]byte("second response"), "second.bin")

	commandsRead := make(chan struct{})
	conn := mockServer(t, func(server net.Conn) {
		_, _ = server.Write([]byte("200 regression server ready\r\n"))
		reader := bufio.NewReader(server)
		_, _ = reader.ReadString('\n')
		_, _ = reader.ReadString('\n')
		close(commandsRead)
		combined := append(bytes.Clone(firstBody), secondBody...)
		_, _ = server.Write(combined)
	})

	reqCh := make(chan *Request, 2)
	nc, err := newNNTPConnectionFromConn(
		context.Background(), conn, 2, reqCh, nil, Auth{}, "", nil, nil,
	)
	if err != nil {
		t.Fatalf("newNNTPConnectionFromConn() error = %v", err)
	}
	t.Cleanup(func() { _ = nc.Close() })

	blocked := &blockingWriter{started: make(chan struct{}), release: make(chan struct{})}
	first := &Request{
		Ctx:        context.Background(),
		Payload:    []byte("BODY <first@example.invalid>\r\n"),
		RespCh:     make(chan Response, 1),
		BodyWriter: io.Discard,
	}
	second := &Request{
		Ctx:        context.Background(),
		Payload:    []byte("BODY <second@example.invalid>\r\n"),
		RespCh:     make(chan Response, 1),
		BodyWriter: blocked,
	}
	reqCh <- first
	reqCh <- second
	go nc.Run()

	select {
	case <-commandsRead:
	case <-time.After(2 * time.Second):
		t.Fatal("server did not receive both pipelined BODY commands")
	}
	select {
	case <-blocked.started:
	case <-time.After(2 * time.Second):
		t.Fatal("second response did not deliver decoded bytes")
	}

	if state := second.attemptState.Load(); state != attemptCommitted {
		t.Errorf("attempt state after prebuffered decoded delivery = %d, want committed", state)
	}
	close(blocked.release)
}

func TestPR1WriterFailureRetiresConnection(t *testing.T) {
	writerErr := errors.New("regression writer failure")
	conn := mockServer(t, func(server net.Conn) {
		_, _ = server.Write([]byte("200 regression server ready\r\n"))
		reader := bufio.NewReader(server)
		_, _ = reader.ReadString('\n')
		_, _ = server.Write(yencSinglePart(bytes.Repeat([]byte("x"), 32*1024), "writer.bin"))
		_, _ = reader.ReadString('\n')
	})

	reqCh := make(chan *Request, 1)
	nc, err := newNNTPConnectionFromConn(
		context.Background(), conn, 1, reqCh, nil, Auth{}, "", nil, nil,
	)
	if err != nil {
		t.Fatalf("newNNTPConnectionFromConn() error = %v", err)
	}
	t.Cleanup(func() { _ = nc.Close() })

	respCh := make(chan Response, 1)
	reqCh <- &Request{
		Ctx:        context.Background(),
		Payload:    []byte("BODY <writer@example.invalid>\r\n"),
		RespCh:     respCh,
		BodyWriter: failingWriter{err: writerErr},
	}
	go nc.Run()

	select {
	case response := <-respCh:
		if !errors.Is(response.Err, writerErr) {
			t.Fatalf("BODY error = %v, want writer failure", response.Err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("BODY writer failure was not returned")
	}

	select {
	case <-nc.Done():
	case <-time.After(500 * time.Millisecond):
		t.Error("connection remained reusable after a BODY writer failure")
	}
}

func TestPR1CancelledBodyDrainIsBounded(t *testing.T) {
	body := yencSinglePart(bytes.Repeat([]byte("z"), regressionBodySize), "cancel.bin")

	for _, depth := range []int{1, 4, 10} {
		t.Run(strings.Repeat("pipeline_", depth)+"cancel", func(t *testing.T) {
			firstChunkWritten := make(chan struct{})
			writtenResult := make(chan int, 1)
			conn := mockServer(t, func(server net.Conn) {
				_, _ = server.Write([]byte("200 regression server ready\r\n"))
				reader := bufio.NewReader(server)
				for range depth {
					_, _ = reader.ReadString('\n')
				}

				written := 0
				signaled := false
				for range depth {
					for offset := 0; offset < len(body); {
						end := min(offset+32*1024, len(body))
						n, err := server.Write(body[offset:end])
						written += n
						if !signaled && written > 0 {
							close(firstChunkWritten)
							signaled = true
						}
						offset += n
						if err != nil {
							writtenResult <- written
							return
						}
					}
				}
				writtenResult <- written
			})

			reqCh := make(chan *Request, depth)
			nc, err := newNNTPConnectionFromConn(
				context.Background(), conn, depth, reqCh, nil, Auth{}, "", nil, nil,
			)
			if err != nil {
				t.Fatalf("newNNTPConnectionFromConn() error = %v", err)
			}
			t.Cleanup(func() { _ = nc.Close() })

			cancels := make([]context.CancelFunc, 0, depth)
			for index := range depth {
				ctx, cancel := context.WithCancel(context.Background())
				cancels = append(cancels, cancel)
				reqCh <- &Request{
					Ctx:        ctx,
					Payload:    []byte("BODY <cancel-" + string(rune('a'+index)) + "@example.invalid>\r\n"),
					RespCh:     make(chan Response, 1),
					BodyWriter: io.Discard,
				}
			}
			go nc.Run()

			select {
			case <-firstChunkWritten:
			case <-time.After(2 * time.Second):
				t.Fatal("server did not begin the first BODY response")
			}
			for _, cancel := range cancels {
				cancel()
			}

			var written int
			select {
			case written = <-writtenResult:
			case <-time.After(5 * time.Second):
				t.Fatal("cancelled BODY drain did not finish or retire the connection")
			}
			if full := len(body) * depth; written >= full {
				t.Errorf("cancelled depth %d drained %d bytes, want a bounded drain below full %d-byte tail", depth, written, full)
			}
		})
	}
}

func TestPR1ArticleAbsenceFallbackIncludes423(t *testing.T) {
	valid := []byte("valid fallback payload")

	for _, operation := range []string{"BODY", "STAT"} {
		t.Run(operation, func(t *testing.T) {
			first := &regressionProvider{
				host: "absence-primary.invalid:119",
				respond: func(_ int, _ string) []byte {
					return []byte("423 no article with that number\r\n")
				},
			}
			second := &regressionProvider{
				host: "available-primary.invalid:119",
				respond: func(_ int, command string) []byte {
					if strings.HasPrefix(command, "STAT") {
						return []byte("223 7 <fixture@example.invalid> article exists\r\n")
					}
					return yencSinglePart(valid, "valid.bin")
				},
			}
			client := newRegressionClient(t, first.provider(false), second.provider(false))
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()

			switch operation {
			case "BODY":
				body, err := client.Body(ctx, "fixture@example.invalid")
				if err != nil {
					t.Fatalf("Body() error = %v", err)
				}
				if !bytes.Equal(body.Bytes, valid) {
					t.Fatalf("Body() bytes = %q, want fallback payload", body.Bytes)
				}
			case "STAT":
				if _, err := client.Stat(ctx, "fixture@example.invalid"); err != nil {
					t.Fatalf("Stat() error = %v", err)
				}
			}
			if second.commandCount(operation) == 0 {
				t.Errorf("second provider did not receive %s after 423", operation)
			}
		})
	}
}

func TestPR1451RetriesFreshThenFallsBack(t *testing.T) {
	valid := []byte("temporary fallback payload")
	first := &regressionProvider{
		host: "temporary-primary.invalid:119",
		respond: func(_ int, _ string) []byte {
			return []byte("451 temporary server failure\r\n")
		},
	}
	second := &regressionProvider{
		host: "available-backup.invalid:119",
		respond: func(_ int, _ string) []byte {
			return yencSinglePart(valid, "temporary.bin")
		},
	}
	client := newRegressionClient(t, first.provider(false), second.provider(true))
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	body, err := client.Body(ctx, "fixture@example.invalid")
	if err != nil {
		t.Fatalf("Body() error = %v", err)
	}
	if !bytes.Equal(body.Bytes, valid) {
		t.Fatalf("Body() bytes = %q, want backup payload", body.Bytes)
	}
	if got := first.connections.Load(); got != 2 {
		t.Errorf("temporary provider connections = %d, want one fresh retry (2 total)", got)
	}
	if second.commandCount("BODY") != 1 {
		t.Errorf("backup BODY attempts = %d, want 1", second.commandCount("BODY"))
	}
}

func TestPR1MixedAbsenceAndTemporaryIsInconclusive(t *testing.T) {
	first := &regressionProvider{
		host: "missing-primary.invalid:119",
		respond: func(_ int, _ string) []byte {
			return []byte("430 no such article\r\n")
		},
	}
	second := &regressionProvider{
		host: "temporary-secondary.invalid:119",
		respond: func(_ int, _ string) []byte {
			return []byte("451 temporary server failure\r\n")
		},
	}
	third := &regressionProvider{
		host: "missing-tertiary.invalid:119",
		respond: func(_ int, _ string) []byte {
			return []byte("430 no such article\r\n")
		},
	}
	client := newRegressionClient(t, first.provider(false), second.provider(false), third.provider(false))
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	_, err := client.Body(ctx, "fixture@example.invalid")
	if err == nil {
		t.Fatal("Body() error = nil, want an inconclusive temporary outcome")
	}
	if errors.Is(err, ErrArticleNotFound) {
		t.Fatalf("Body() error = %v, mixed absence and temporary must not be hard absence", err)
	}
	if second.commandCount("STAT") == 0 || third.commandCount("STAT") == 0 {
		t.Fatalf("parallel probe commands: temporary=%d missing=%d, want both attempted", second.commandCount("STAT"), third.commandCount("STAT"))
	}
}

func TestPR1BufferedBodyCorruptionFallsBack(t *testing.T) {
	valid := []byte("validated payload")
	first := &regressionProvider{
		host: "corrupt-primary.invalid:119",
		respond: func(_ int, _ string) []byte {
			return corruptCRC(yencSinglePart([]byte("corrupt payload"), "corrupt.bin"))
		},
	}
	second := &regressionProvider{
		host: "valid-primary.invalid:119",
		respond: func(_ int, _ string) []byte {
			return yencSinglePart(valid, "valid.bin")
		},
	}
	client := newRegressionClient(t, first.provider(false), second.provider(false))
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	body, err := client.Body(ctx, "fixture@example.invalid")
	if err != nil {
		t.Fatalf("Body() error = %v", err)
	}
	if !bytes.Equal(body.Bytes, valid) {
		t.Fatalf("Body() bytes = %q, want validated fallback payload", body.Bytes)
	}
	if second.commandCount("BODY") != 1 {
		t.Errorf("valid provider BODY attempts = %d, want 1", second.commandCount("BODY"))
	}
}

func TestPR1BufferedBodyMissingTrailerFallsBack(t *testing.T) {
	valid := []byte("complete payload")
	first := &regressionProvider{
		host: "truncated-primary.invalid:119",
		respond: func(_ int, _ string) []byte {
			return removeYEnd(yencSinglePart([]byte("truncated payload"), "truncated.bin"))
		},
	}
	second := &regressionProvider{
		host: "complete-primary.invalid:119",
		respond: func(_ int, _ string) []byte {
			return yencSinglePart(valid, "complete.bin")
		},
	}
	client := newRegressionClient(t, first.provider(false), second.provider(false))
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	body, err := client.Body(ctx, "fixture@example.invalid")
	if err != nil {
		t.Fatalf("Body() error = %v", err)
	}
	if !bytes.Equal(body.Bytes, valid) {
		t.Fatalf("Body() bytes = %q, want complete fallback payload", body.Bytes)
	}
	if second.commandCount("BODY") != 1 {
		t.Errorf("complete provider BODY attempts = %d, want 1", second.commandCount("BODY"))
	}
}

func TestPR1SuccessfulResultsExposeProviderAndAttempts(t *testing.T) {
	valid := []byte("evidence payload")
	missing := &regressionProvider{
		host: "evidence-missing.invalid:119",
		respond: func(_ int, _ string) []byte {
			return []byte("430 no such article\r\n")
		},
	}
	serving := &regressionProvider{
		host: "evidence-serving.invalid:119",
		respond: func(_ int, command string) []byte {
			if strings.HasPrefix(command, "STAT") {
				return []byte("223 9 <fixture@example.invalid> article exists\r\n")
			}
			return yencSinglePart(valid, "evidence.bin")
		},
	}
	client, err := NewClient(
		context.Background(),
		[]Provider{missing.provider(false), serving.provider(false)},
		WithDispatchStrategy(DispatchFIFO),
		WithStatProbe(false),
	)
	if err != nil {
		t.Fatalf("NewClient() error = %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	body, bodyErr := client.Body(ctx, "fixture@example.invalid")
	if bodyErr != nil {
		t.Fatalf("Body() error = %v", bodyErr)
	}
	requireProviderAndAttempts(t, body, serving.host, 2)

	stat, statErr := client.Stat(ctx, "fixture@example.invalid")
	if statErr != nil {
		t.Fatalf("Stat() error = %v", statErr)
	}
	requireProviderAndAttempts(t, stat, serving.host, 2)
}

func requireProviderAndAttempts(t *testing.T, result any, providerID string, wantAttempts int) {
	t.Helper()
	value := reflect.Indirect(reflect.ValueOf(result))
	provider := value.FieldByName("ProviderID")
	if !provider.IsValid() || provider.Kind() != reflect.String {
		t.Errorf("%T does not expose additive string ProviderID", result)
		return
	}
	if got := provider.String(); got != providerID {
		t.Errorf("%T ProviderID = %q, want %q", result, got, providerID)
	}

	attempts := value.FieldByName("Attempts")
	if !attempts.IsValid() || attempts.Kind() != reflect.Slice {
		t.Errorf("%T does not expose additive Attempts evidence", result)
		return
	}
	if attempts.Len() != wantAttempts {
		t.Errorf("%T attempt count = %d, want %d", result, attempts.Len(), wantAttempts)
		return
	}
	for index := 0; index < attempts.Len(); index++ {
		attempt := reflect.Indirect(attempts.Index(index))
		for _, fieldName := range []string{
			"ProviderID",
			"Operation",
			"Outcome",
			"PoolQueueDuration",
			"PipelineHeadWaitDuration",
			"ResponseServiceDuration",
		} {
			if !attempt.FieldByName(fieldName).IsValid() {
				t.Errorf("attempt %d does not expose %s", index, fieldName)
			}
		}
		if attempt.FieldByName("ProviderGeneration").IsValid() {
			t.Errorf("attempt %d exposes AltMount-owned ProviderGeneration", index)
		}
	}
}
