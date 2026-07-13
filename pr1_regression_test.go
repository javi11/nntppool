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

	"github.com/mnightingale/rapidyenc"
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
	// Preserve the CRLF that terminates the encoded data so the NNTP dot line
	// remains a valid article terminator after removing only the trailer line.
	return append(truncated[:start+2], truncated[end:]...)
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

func TestPR1CancelledPipelineDoesNotDelayNewPriorityBody(t *testing.T) {
	const depth = 10
	largeBody := yencSinglePart(bytes.Repeat([]byte("q"), regressionBodySize), "obsolete.bin")
	priorityBody := yencSinglePart([]byte("priority payload"), "priority.bin")
	firstCommandsRead := make(chan struct{})
	var connections atomic.Int32
	factory := func(context.Context) (net.Conn, error) {
		connection := connections.Add(1)
		client, server := net.Pipe()
		go func() {
			defer func() { _ = server.Close() }()
			_, _ = server.Write([]byte("200 regression server ready\r\n"))
			reader := bufio.NewReader(server)
			if connection == 1 {
				for range depth {
					if _, err := reader.ReadString('\n'); err != nil {
						return
					}
				}
				close(firstCommandsRead)
				_, _ = server.Write(largeBody)
				return
			}
			if _, err := reader.ReadString('\n'); err != nil {
				return
			}
			_, _ = server.Write(priorityBody)
		}()
		return client, nil
	}
	client := newRegressionClient(t, Provider{
		Host:                      "cancel-priority.invalid:119",
		Factory:                   factory,
		Connections:               1,
		Inflight:                  depth,
		StatInflight:              depth,
		SkipPing:                  true,
		AbandonedBodyDrainBytes:   64 * 1024,
		AbandonedBodyDrainTimeout: 100 * time.Millisecond,
	})
	cancels := make([]context.CancelFunc, 0, depth)
	for index := range depth {
		ctx, cancel := context.WithCancel(context.Background())
		cancels = append(cancels, cancel)
		_ = client.Send(ctx, []byte("BODY <obsolete-"+string(rune('a'+index))+"@example.invalid>\r\n"), io.Discard)
	}
	select {
	case <-firstCommandsRead:
	case <-time.After(2 * time.Second):
		t.Fatal("obsolete BODY pipeline did not fill")
	}
	for _, cancel := range cancels {
		cancel()
	}
	start := time.Now()
	body, err := client.BodyPriority(context.Background(), "priority@example.invalid")
	if err != nil {
		t.Fatalf("priority Body() error = %v", err)
	}
	if !bytes.Equal(body.Bytes, []byte("priority payload")) {
		t.Fatalf("priority Body() bytes = %q", body.Bytes)
	}
	if elapsed := time.Since(start); elapsed > 750*time.Millisecond {
		t.Errorf("priority Body() waited %v behind canceled payload, want bounded recovery", elapsed)
	}
	if got := connections.Load(); got < 2 {
		t.Errorf("connections = %d, want canceled pipeline socket retired", got)
	}
	if errorsCount := client.Stats().Providers[0].Errors; errorsCount != 0 {
		t.Errorf("provider errors = %d, caller cancellation must not count as provider failure", errorsCount)
	}
}

func BenchmarkPR1AbandonedBodyDrainCaps(b *testing.B) {
	body := yencSinglePart(bytes.Repeat([]byte("b"), regressionBodySize), "benchmark.bin")
	cases := []struct {
		name       string
		byteCap    int
		timeCap    time.Duration
		writeDelay time.Duration
	}{
		{name: "bytes_64KiB", byteCap: 64 * 1024, timeCap: 2 * time.Second},
		{name: "bytes_256KiB", byteCap: 256 * 1024, timeCap: 2 * time.Second},
		{name: "bytes_1MiB", byteCap: 1024 * 1024, timeCap: 2 * time.Second},
		{name: "time_50ms", byteCap: 8 * 1024 * 1024, timeCap: 50 * time.Millisecond, writeDelay: 10 * time.Millisecond},
		{name: "time_100ms", byteCap: 8 * 1024 * 1024, timeCap: 100 * time.Millisecond, writeDelay: 10 * time.Millisecond},
		{name: "time_250ms", byteCap: 8 * 1024 * 1024, timeCap: 250 * time.Millisecond, writeDelay: 10 * time.Millisecond},
	}
	for _, benchmark := range cases {
		b.Run(benchmark.name, func(b *testing.B) {
			var totalWritten int64
			for range b.N {
				clientConn, serverConn := net.Pipe()
				firstChunkWritten := make(chan struct{})
				writtenResult := make(chan int, 1)
				go func() {
					defer func() { _ = serverConn.Close() }()
					_, _ = serverConn.Write([]byte("200 benchmark server ready\r\n"))
					reader := bufio.NewReader(serverConn)
					for range 10 {
						if _, err := reader.ReadString('\n'); err != nil {
							writtenResult <- 0
							return
						}
					}
					written := 0
					signaled := false
					for offset := 0; offset < len(body); {
						end := min(offset+16*1024, len(body))
						n, err := serverConn.Write(body[offset:end])
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
						if benchmark.writeDelay > 0 {
							time.Sleep(benchmark.writeDelay)
						}
					}
					writtenResult <- written
				}()

				reqCh := make(chan *Request, 10)
				nc, err := newNNTPConnectionFromConn(
					context.Background(), clientConn, 10, reqCh, nil, Auth{}, "", nil, nil,
				)
				if err != nil {
					b.Fatalf("newNNTPConnectionFromConn() error = %v", err)
				}
				nc.abandonedDrainBytes = benchmark.byteCap
				nc.abandonedDrainTimeout = benchmark.timeCap
				cancels := make([]context.CancelFunc, 0, 10)
				for index := range 10 {
					ctx, cancel := context.WithCancel(context.Background())
					cancels = append(cancels, cancel)
					reqCh <- &Request{
						Ctx:        ctx,
						Payload:    []byte("BODY <benchmark-" + string(rune('a'+index)) + "@example.invalid>\r\n"),
						RespCh:     make(chan Response, 1),
						BodyWriter: io.Discard,
					}
				}
				go nc.Run()
				<-firstChunkWritten
				for _, cancel := range cancels {
					cancel()
				}
				written := <-writtenResult
				totalWritten += int64(written)
				_ = nc.Close()
			}
			b.ReportMetric(float64(totalWritten)/float64(b.N), "drained_bytes/op")
		})
	}
}

func TestPR1RawSendPreservesUnvalidatedV4BodyBehavior(t *testing.T) {
	provider := &regressionProvider{
		host:    "raw-v4.invalid:119",
		respond: func(_ int, _ string) []byte { return []byte("222 body follows\r\n.\r\n") },
	}
	client := newRegressionClient(t, provider.provider(false))
	resp := <-client.Send(context.Background(), []byte("BODY <fixture@example.invalid>\r\n"), nil)
	if resp.Err != nil || resp.StatusCode != 222 {
		t.Fatalf("raw Send() response = %+v, want legacy unvalidated 222 success", resp)
	}
	if len(resp.Attempts) != 1 || resp.Attempts[0].BodyValidation != BodyValidationNotRequested {
		t.Errorf("raw BODY attempts = %+v, want validation not requested", resp.Attempts)
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
	if len(body.Attempts) != 3 {
		t.Fatalf("attempt count = %d, want 2 temporary attempts plus backup success", len(body.Attempts))
	}
	if body.Attempts[0].Outcome != OutcomeTemporaryFailure ||
		body.Attempts[1].Outcome != OutcomeTemporaryFailure ||
		body.Attempts[2].Outcome != OutcomeSuccess {
		t.Errorf("attempt outcomes = %v, want temporary, temporary, success", []OutcomeKind{
			body.Attempts[0].Outcome,
			body.Attempts[1].Outcome,
			body.Attempts[2].Outcome,
		})
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
	var transportErr *TransportError
	if !errors.As(err, &transportErr) || transportErr.Kind != OutcomeInconclusive {
		t.Fatalf("Body() error = %v, want structured inconclusive mixed outcome", err)
	}
	if second.commandCount("STAT") == 0 || third.commandCount("STAT") == 0 {
		t.Fatalf("parallel probe commands: temporary=%d missing=%d, want both attempted", second.commandCount("STAT"), third.commandCount("STAT"))
	}
}

func TestPR1UnavailableEligibilityDoesNotCollapseToHardAbsence(t *testing.T) {
	missing := &regressionProvider{
		host:    "quota-missing.invalid:119",
		respond: func(_ int, _ string) []byte { return []byte("430 no such article\r\n") },
	}
	unavailable := &regressionProvider{
		host: "quota-unavailable.invalid:119",
		respond: func(_ int, _ string) []byte {
			return yencSinglePart([]byte("must not be requested"), "quota.bin")
		},
	}
	unavailableConfig := unavailable.provider(false)
	unavailableConfig.QuotaBytes = 1
	unavailableConfig.QuotaUsed = 1
	client := newRegressionClient(t, missing.provider(false), unavailableConfig)
	_, err := client.Body(context.Background(), "fixture@example.invalid")
	var transportErr *TransportError
	if !errors.As(err, &transportErr) || transportErr.Kind != OutcomeInconclusive {
		t.Fatalf("Body() error = %v, want mixed absence/unavailable inconclusive", err)
	}
	if errors.Is(err, ErrArticleNotFound) {
		t.Fatalf("Body() error = %v, unavailable provider must prevent global hard absence", err)
	}
	if len(transportErr.Attempts) != 2 ||
		transportErr.Attempts[1].Outcome != OutcomeProviderUnavailable ||
		!errors.Is(transportErr.Attempts[1].Cause, ErrQuotaExceeded) {
		t.Errorf("attempts = %+v, want hard absence then quota unavailable", transportErr.Attempts)
	}
	if unavailable.commandCount("BODY") != 0 {
		t.Errorf("quota-exceeded provider received %d BODY commands", unavailable.commandCount("BODY"))
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
	missingConfig := missing.provider(false)
	missingConfig.ID = "provider-stable-missing"
	servingConfig := serving.provider(false)
	servingConfig.ID = "provider-stable-serving"
	client, err := NewClient(
		context.Background(),
		[]Provider{missingConfig, servingConfig},
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
	requireProviderAndAttempts(t, body, servingConfig.ID, 2)

	stat, statErr := client.Stat(ctx, "fixture@example.invalid")
	if statErr != nil {
		t.Fatalf("Stat() error = %v", statErr)
	}
	requireProviderAndAttempts(t, stat, servingConfig.ID, 2)
}

func TestPR1StructuredTerminalClassifications(t *testing.T) {
	t.Run("hard absence", func(t *testing.T) {
		first := &regressionProvider{
			host:    "hard-absence-primary.invalid:119",
			respond: func(_ int, _ string) []byte { return []byte("423 no article with that number\r\n") },
		}
		backup := &regressionProvider{
			host:    "hard-absence-backup.invalid:119",
			respond: func(_ int, _ string) []byte { return []byte("430 no such article\r\n") },
		}
		client := newRegressionClient(t, first.provider(false), backup.provider(true))
		_, err := client.Body(context.Background(), "fixture@example.invalid")
		var transportErr *TransportError
		if !errors.As(err, &transportErr) {
			t.Fatalf("Body() error = %v, want TransportError", err)
		}
		if transportErr.Kind != OutcomeHardArticleAbsence || !errors.Is(err, ErrArticleNotFound) {
			t.Fatalf("Body() error = %v, want structured hard absence preserving sentinel", err)
		}
		if len(transportErr.Attempts) != 2 || transportErr.Attempts[0].ResponseCode != 423 ||
			transportErr.Attempts[1].ResponseCode != 430 {
			t.Errorf("attempts = %+v, want ordered 423 then backup 430", transportErr.Attempts)
		}
	})

	t.Run("unavailable", func(t *testing.T) {
		provider := &regressionProvider{
			host:    "unavailable.invalid:119",
			respond: func(_ int, _ string) []byte { return []byte("502 service unavailable\r\n") },
		}
		client := newRegressionClient(t, provider.provider(false))
		_, err := client.Body(context.Background(), "fixture@example.invalid")
		var transportErr *TransportError
		if !errors.As(err, &transportErr) {
			t.Fatalf("Body() error = %v, want TransportError", err)
		}
		if transportErr.Kind != OutcomeProviderUnavailable || !errors.Is(err, ErrServiceUnavailable) {
			t.Fatalf("Body() error = %v, want structured provider unavailable preserving sentinel", err)
		}
		if len(transportErr.Attempts) != 1 || transportErr.Attempts[0].ResponseCode != 502 {
			t.Errorf("attempts = %+v, want one raw 502 record", transportErr.Attempts)
		}
	})

	t.Run("caller cancellation", func(t *testing.T) {
		provider := &regressionProvider{
			host: "cancelled.invalid:119",
			respond: func(_ int, _ string) []byte {
				return yencSinglePart([]byte("must not be accepted"), "cancelled.bin")
			},
		}
		client := newRegressionClient(t, provider.provider(false))
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		_, err := client.Body(ctx, "fixture@example.invalid")
		var transportErr *TransportError
		if !errors.As(err, &transportErr) {
			t.Fatalf("Body() error = %v, want TransportError", err)
		}
		if transportErr.Kind != OutcomeCancellation || !errors.Is(err, context.Canceled) {
			t.Fatalf("Body() error = %v, want structured caller cancellation", err)
		}
		if len(transportErr.Attempts) != 1 || transportErr.Attempts[0].ProviderResponseTimeout {
			t.Errorf("attempts = %+v, want one local cancellation and no provider timeout", transportErr.Attempts)
		}
	})
}

func TestPR1ProviderResponseTimeoutIsSeparateEvidence(t *testing.T) {
	hungFactory := func(ctx context.Context) (net.Conn, error) {
		client, server := net.Pipe()
		go func() {
			defer func() { _ = server.Close() }()
			_, _ = server.Write([]byte("200 regression server ready\r\n"))
			reader := bufio.NewReader(server)
			_, _ = reader.ReadString('\n')
			<-ctx.Done()
		}()
		return client, nil
	}
	healthy := &regressionProvider{
		host: "timeout-fallback.invalid:119",
		respond: func(_ int, _ string) []byte {
			return yencSinglePart([]byte("timeout fallback"), "timeout.bin")
		},
	}
	client := newRegressionClient(t,
		Provider{
			ID:             "provider-timeout",
			Host:           "timeout-primary.invalid:119",
			Factory:        hungFactory,
			Connections:    1,
			Inflight:       1,
			SkipPing:       true,
			AttemptTimeout: 50 * time.Millisecond,
		},
		healthy.provider(true),
	)
	body, err := client.Body(context.Background(), "fixture@example.invalid")
	if err != nil {
		t.Fatalf("Body() error = %v", err)
	}
	if len(body.Attempts) != 2 {
		t.Fatalf("attempt count = %d, want timeout then backup success", len(body.Attempts))
	}
	first := body.Attempts[0]
	if first.ProviderID != "provider-timeout" || first.Outcome != OutcomeTransportFailure ||
		!first.ProviderResponseTimeout {
		t.Errorf("timeout evidence = %+v, want provider transport timeout", first)
	}
	if first.PoolQueueDuration < 0 || first.PipelineHeadWaitDuration < 0 ||
		first.ResponseServiceDuration < 40*time.Millisecond {
		t.Errorf("timeout durations = queue %v, head %v, service %v", first.PoolQueueDuration,
			first.PipelineHeadWaitDuration, first.ResponseServiceDuration)
	}
	if body.Attempts[1].Outcome != OutcomeSuccess || body.ProviderID != healthy.host {
		t.Errorf("fallback result = provider %q attempts %+v", body.ProviderID, body.Attempts)
	}
}

func TestPR1BufferedBodyValidationFallbackMatrix(t *testing.T) {
	validPayload := []byte("strict validation fallback")
	base := yencSinglePart([]byte("invalid provider payload"), "invalid.bin")

	replaceRequired := func(input []byte, oldValue, newValue string) []byte {
		t.Helper()
		output := bytes.Replace(bytes.Clone(input), []byte(oldValue), []byte(newValue), 1)
		if bytes.Equal(output, input) {
			t.Fatalf("fixture does not contain %q", oldValue)
		}
		return output
	}

	badPart := replaceRequired(base, "part=1", "part=2")
	missingBegin := replaceRequired(base, "=ybegin", "=xbegin")
	badFileSize := replaceRequired(base, " size=24", " size=25")
	badTrailerSize := replaceRequired(base, "=yend size=24", "=yend size=99")
	malformedTrailer := replaceRequired(base, "=yend size=24", "=yend size=xx")
	malformedCRC := bytes.Clone(base)
	crcIndex := bytes.Index(malformedCRC, []byte("crc32="))
	if crcIndex < 0 {
		t.Fatal("fixture does not contain crc32")
	}
	copy(malformedCRC[crcIndex+len("crc32="):], "zzzzzzzz")
	zeroCRCMismatch := bytes.Clone(base)
	copy(zeroCRCMismatch[crcIndex+len("crc32="):], "00000000")
	shortPayload := bytes.Clone(base)
	trailerIndex := bytes.Index(shortPayload, []byte("\r\n=yend "))
	if trailerIndex < 2 {
		t.Fatal("fixture does not contain an encoded payload")
	}
	shortPayload = append(shortPayload[:trailerIndex-1], shortPayload[trailerIndex:]...)

	cases := map[string][]byte{
		"missing_ybegin":    missingBegin,
		"invalid_part":      badPart,
		"file_size":         badFileSize,
		"decoded_size":      badTrailerSize,
		"malformed_trailer": malformedTrailer,
		"malformed_crc":     malformedCRC,
		"zero_crc_mismatch": zeroCRCMismatch,
		"short_payload":     shortPayload,
	}
	for name, invalidResponse := range cases {
		t.Run(name, func(t *testing.T) {
			invalid := &regressionProvider{
				host: "invalid-" + name + ".invalid:119",
				respond: func(_ int, _ string) []byte {
					return invalidResponse
				},
			}
			valid := &regressionProvider{
				host: "valid-" + name + ".invalid:119",
				respond: func(_ int, _ string) []byte {
					return yencSinglePart(validPayload, "valid.bin")
				},
			}
			client := newRegressionClient(t, invalid.provider(false), valid.provider(false))
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()

			body, err := client.Body(ctx, "fixture@example.invalid")
			if err != nil {
				t.Fatalf("Body() error = %v", err)
			}
			if !bytes.Equal(body.Bytes, validPayload) {
				t.Fatalf("Body() bytes = %q, want validated fallback", body.Bytes)
			}
			if len(body.Attempts) != 2 || body.Attempts[0].Outcome != OutcomeCorruptBody {
				t.Errorf("attempts = %+v, want corrupt then success", body.Attempts)
			}
		})
	}
}

func TestPR1ZeroCRCIsValidated(t *testing.T) {
	zeroCRC := []byte{0x9d, 0x0a, 0xd9, 0x6d}
	provider := &regressionProvider{
		host: "zero-crc.invalid:119",
		respond: func(_ int, _ string) []byte {
			return yencSinglePart(zeroCRC, "zero.bin")
		},
	}
	client := newRegressionClient(t, provider.provider(false))
	body, err := client.Body(context.Background(), "fixture@example.invalid")
	if err != nil {
		t.Fatalf("Body() error = %v", err)
	}
	if body.CRC != 0 || body.ExpectedCRC != 0 || !body.CRCProvided || !body.CRCValid {
		t.Errorf("CRC fields = actual:%08x expected:%08x provided:%v valid:%v, want supplied zero CRC validated", body.CRC, body.ExpectedCRC, body.CRCProvided, body.CRCValid)
	}
}

func TestPR1NativeDecodeErrorPropagates(t *testing.T) {
	nativeErr := errors.New("native decoder regression")
	response := NNTPResponse{
		body:   true,
		Format: 1,
		decodeFn: func([]byte, []byte, *rapidyenc.State) (int, int, rapidyenc.End, error) {
			return 0, 0, rapidyenc.EndNone, nativeErr
		},
	}
	_, err := response.decodeYenc([]byte("encoded"), io.Discard)
	if !errors.Is(err, nativeErr) || !errors.Is(err, ErrBodyCorrupt) {
		t.Fatalf("decodeYenc() error = %v, want native error wrapped as corruption", err)
	}
}

func TestPR1ParallelProbePreservesConfiguredOrderAndEvidence(t *testing.T) {
	missing := &regressionProvider{
		host:    "probe-missing.invalid:119",
		respond: func(_ int, _ string) []byte { return []byte("430 no such article\r\n") },
	}
	preferred := &regressionProvider{
		host: "probe-preferred.invalid:119",
		respond: func(_ int, command string) []byte {
			if strings.HasPrefix(command, "STAT") {
				time.Sleep(80 * time.Millisecond)
				return []byte("223 1 <fixture@example.invalid> exists\r\n")
			}
			return yencSinglePart([]byte("preferred"), "preferred.bin")
		},
	}
	fasterLater := &regressionProvider{
		host: "probe-faster.invalid:119",
		respond: func(_ int, command string) []byte {
			if strings.HasPrefix(command, "STAT") {
				return []byte("223 1 <fixture@example.invalid> exists\r\n")
			}
			return yencSinglePart([]byte("later"), "later.bin")
		},
	}
	client := newRegressionClient(t, missing.provider(false), preferred.provider(false), fasterLater.provider(false))
	body, err := client.Body(context.Background(), "fixture@example.invalid")
	if err != nil {
		t.Fatalf("Body() error = %v", err)
	}
	if body.ProviderID != preferred.host || !bytes.Equal(body.Bytes, []byte("preferred")) {
		t.Fatalf("serving provider/body = %q/%q, want configured preferred provider", body.ProviderID, body.Bytes)
	}
	if fasterLater.commandCount("BODY") != 0 {
		t.Errorf("faster later provider received %d BODY commands, want 0", fasterLater.commandCount("BODY"))
	}
	wantProviders := []string{missing.host, preferred.host, fasterLater.host, preferred.host}
	if len(body.Attempts) != len(wantProviders) {
		t.Fatalf("attempt count = %d, want %d", len(body.Attempts), len(wantProviders))
	}
	for index, want := range wantProviders {
		if got := body.Attempts[index].ProviderID; got != want {
			t.Errorf("attempt %d provider = %q, want %q", index, got, want)
		}
	}
}

func TestPR1UnknownResponseRemainsInconclusive(t *testing.T) {
	provider := &regressionProvider{
		host:    "vendor-code.invalid:119",
		respond: func(_ int, _ string) []byte { return []byte("499 vendor-specific failure\r\n") },
	}
	client := newRegressionClient(t, provider.provider(false))
	_, err := client.Body(context.Background(), "fixture@example.invalid")
	var transportErr *TransportError
	if !errors.As(err, &transportErr) {
		t.Fatalf("Body() error = %v, want TransportError", err)
	}
	if transportErr.Kind != OutcomeInconclusive || transportErr.ResponseCode != 499 {
		t.Errorf("transport outcome/code = %s/%d, want inconclusive/499", transportErr.Kind, transportErr.ResponseCode)
	}
	if errors.Is(err, ErrArticleNotFound) {
		t.Fatalf("unknown vendor response matched hard absence: %v", err)
	}
}

func TestPR1TargetedBodyCanRequireFreshTransport(t *testing.T) {
	provider := &regressionProvider{
		host: "targeted-fresh.invalid:119",
		respond: func(_ int, _ string) []byte {
			return yencSinglePart([]byte("targeted"), "targeted.bin")
		},
	}
	client := newRegressionClient(t, provider.provider(false))
	if _, err := client.Body(context.Background(), "first@example.invalid"); err != nil {
		t.Fatalf("initial Body() error = %v", err)
	}
	body, err := client.BodyTargeted(context.Background(), "second@example.invalid", TargetedBodyOptions{
		Provider:       provider.host,
		FreshTransport: true,
		Priority:       true,
	})
	if err != nil {
		t.Fatalf("BodyTargeted() error = %v", err)
	}
	if body.ProviderID != provider.host {
		t.Errorf("BodyTargeted() provider = %q, want %q", body.ProviderID, provider.host)
	}
	if got := provider.connections.Load(); got != 2 {
		t.Errorf("provider connections = %d, want fresh second transport", got)
	}
}

func TestPR1PipelineWaitDoesNotConsumeResponseTimeout(t *testing.T) {
	response := yencSinglePart(bytes.Repeat([]byte("p"), 128*1024), "pipeline.bin")
	factory := func(context.Context) (net.Conn, error) {
		client, server := net.Pipe()
		go func() {
			defer func() { _ = server.Close() }()
			_, _ = server.Write([]byte("200 regression server ready\r\n"))
			reader := bufio.NewReader(server)
			_, _ = reader.ReadString('\n')
			chunk := len(response) / 8
			for offset := 0; offset < len(response); offset += chunk {
				end := min(offset+chunk, len(response))
				_, _ = server.Write(response[offset:end])
				if end < len(response) {
					time.Sleep(25 * time.Millisecond)
				}
			}
			_, _ = reader.ReadString('\n')
			_, _ = server.Write(yencSinglePart([]byte("second"), "second.bin"))
		}()
		return client, nil
	}
	client := newRegressionClient(t, Provider{
		Host:           "dual-clock.invalid:119",
		Factory:        factory,
		Connections:    1,
		Inflight:       2,
		SkipPing:       true,
		AttemptTimeout: 70 * time.Millisecond,
		StallTimeout:   300 * time.Millisecond,
	})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	type result struct {
		body *ArticleBody
		err  error
	}
	results := make(chan result, 2)
	for _, id := range []string{"first@example.invalid", "second@example.invalid"} {
		go func(messageID string) {
			body, err := client.Body(ctx, messageID)
			results <- result{body: body, err: err}
		}(id)
	}
	var maxHeadWait time.Duration
	for range 2 {
		result := <-results
		if result.err != nil {
			var transportErr *TransportError
			if errors.As(result.err, &transportErr) {
				t.Fatalf("pipelined Body() error = %v; attempts = %+v", result.err, transportErr.Attempts)
			}
			t.Fatalf("pipelined Body() error = %v", result.err)
		}
		if len(result.body.Attempts) != 1 {
			t.Fatalf("attempt count = %d, want 1", len(result.body.Attempts))
		}
		maxHeadWait = max(maxHeadWait, result.body.Attempts[0].PipelineHeadWaitDuration)
	}
	if maxHeadWait < 150*time.Millisecond {
		t.Errorf("maximum pipeline head wait = %v, want evidence of >150ms wait outside 70ms response timeout", maxHeadWait)
	}
	providerStats := client.Stats().Providers[0]
	if providerStats.TTFB >= 70*time.Millisecond {
		t.Errorf("provider TTFB = %v, pipeline wait contaminated response-head timing", providerStats.TTFB)
	}
	if providerStats.Errors != 0 {
		t.Errorf("provider errors = %d, pipeline wait must not count as provider failure", providerStats.Errors)
	}
}

func TestPR1UnsentPriorityPrecedesQueuedNormalWork(t *testing.T) {
	commands := make(chan string, 3)
	releaseFirst := make(chan struct{})
	factory := func(context.Context) (net.Conn, error) {
		client, server := net.Pipe()
		go func() {
			defer func() { _ = server.Close() }()
			_, _ = server.Write([]byte("200 regression server ready\r\n"))
			reader := bufio.NewReader(server)
			for index := 0; ; index++ {
				command, err := reader.ReadString('\n')
				if err != nil {
					return
				}
				command = strings.TrimSpace(command)
				commands <- command
				if index == 0 {
					<-releaseFirst
				}
				if strings.HasPrefix(command, "BODY") {
					_, _ = server.Write(yencSinglePart([]byte("priority"), "priority.bin"))
				} else {
					_, _ = server.Write([]byte("223 1 <fixture@example.invalid> exists\r\n"))
				}
			}
		}()
		return client, nil
	}
	client := newRegressionClient(t, Provider{
		Host:         "strict-priority.invalid:119",
		Factory:      factory,
		Connections:  1,
		Inflight:     1,
		StatInflight: 1,
		SkipPing:     true,
	})
	results := make(chan error, 3)
	go func() {
		_, err := client.Stat(context.Background(), "first@example.invalid")
		results <- err
	}()
	if first := <-commands; !strings.HasPrefix(first, "STAT") {
		t.Fatalf("first command = %q, want blocking STAT", first)
	}
	go func() {
		_, err := client.Stat(context.Background(), "normal@example.invalid")
		results <- err
	}()
	go func() {
		_, err := client.BodyPriority(context.Background(), "priority@example.invalid")
		results <- err
	}()
	group := (*client.mainGroups.Load())[0]
	deadline := time.Now().Add(time.Second)
	for (len(group.reqCh) == 0 || len(group.prioCh) == 0) && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	close(releaseFirst)
	if second := <-commands; !strings.HasPrefix(second, "BODY") {
		t.Errorf("second command = %q, want priority BODY before queued normal STAT", second)
	}
	for range 3 {
		if err := <-results; err != nil {
			t.Errorf("request error = %v", err)
		}
	}
}

func TestPR1BackgroundStatWindowPreservesPriorityHeadroom(t *testing.T) {
	commands := make(chan string, 5)
	release := make(chan struct{})
	factory := func(context.Context) (net.Conn, error) {
		client, server := net.Pipe()
		go func() {
			defer func() { _ = server.Close() }()
			_, _ = server.Write([]byte("200 regression server ready\r\n"))
			reader := bufio.NewReader(server)
			for index := 0; ; index++ {
				command, err := reader.ReadString('\n')
				if err != nil {
					return
				}
				commands <- strings.TrimSpace(command)
				if index == 2 {
					<-release
					for range 3 {
						_, _ = server.Write([]byte("223 1 <fixture@example.invalid> exists\r\n"))
					}
				} else if index > 2 {
					_, _ = server.Write([]byte("223 1 <fixture@example.invalid> exists\r\n"))
				}
			}
		}()
		return client, nil
	}
	client := newRegressionClient(t, Provider{
		Host:                   "occupancy.invalid:119",
		Factory:                factory,
		Connections:            1,
		Inflight:               1,
		StatInflight:           4,
		BackgroundStatInflight: 2,
		PriorityHeadroom:       1,
		SkipPing:               true,
	})
	results := make(chan error, 5)
	for index := range 4 {
		go func(index int) {
			_, err := client.Stat(context.Background(), "background-"+string(rune('a'+index))+"@example.invalid")
			results <- err
		}(index)
	}
	for range 2 {
		select {
		case command := <-commands:
			if !strings.HasPrefix(command, "STAT") {
				t.Fatalf("background command = %q, want STAT", command)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("background STAT window did not fill")
		}
	}
	select {
	case command := <-commands:
		t.Fatalf("ordinary background exceeded configured window before priority: %q", command)
	case <-time.After(50 * time.Millisecond):
	}
	go func() {
		_, err := client.StatPriority(context.Background(), "priority@example.invalid")
		results <- err
	}()
	select {
	case command := <-commands:
		if !strings.Contains(command, "priority@example.invalid") {
			t.Fatalf("third command = %q, want priority STAT in reserved headroom", command)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("priority STAT did not use reserved pipeline headroom")
	}
	stats := client.Stats().Providers[0]
	if stats.BackgroundStatInUse != 2 || stats.BackgroundStatLimit != 2 ||
		stats.PipelineInUse != 3 || stats.PipelineLimit != 4 || stats.PriorityHeadroom != 1 {
		t.Errorf("occupancy stats = %+v, want background 2/2, pipeline 3/4, headroom 1", stats)
	}
	close(release)
	for range 5 {
		if err := <-results; err != nil {
			t.Errorf("STAT error = %v", err)
		}
	}
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
