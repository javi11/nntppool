package nntpcli

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/javi11/nntppool/v2/pkg/nntpcli/test"

	"github.com/stretchr/testify/assert"
)

const examplepost = `From: <nobody@example.com>
Newsgroups: misc.test
Subject: Code test
Message-Id: <1234>
Organization: usenet drive

`

func TestConnection_Body(t *testing.T) {
	conn := articleReadyToDownload(t)

	w := bytes.NewBuffer(nil)

	n, err := conn.BodyDecoded("1234", w, 0)
	assert.NoError(t, err)

	assert.Equal(t, int64(9), n)
	assert.Equal(t, "test text", w.String())
}

func TestConnection_Body_Closed_Before_Full_Read_Drains_The_Buffer(t *testing.T) {
	conn := articleReadyToDownload(t)

	_, w := io.Pipe()
	_ = w.Close()

	n, err := conn.BodyDecoded("1234", w, 0)
	assert.ErrorIs(t, err, io.ErrClosedPipe)

	assert.Equal(t, int64(0), n)

	// The buffer should be drained
	buff := bytes.NewBuffer(nil)

	n, err = conn.BodyDecoded("1234", buff, 0)
	assert.NoError(t, err)

	assert.Equal(t, int64(9), n)
	assert.Equal(t, "test text", buff.String())
}

func TestConnection_Body_Discarding_Bytes(t *testing.T) {
	conn := articleReadyToDownload(t)

	w := bytes.NewBuffer(nil)

	n, err := conn.BodyDecoded("1234", w, 5)
	assert.NoError(t, err)

	// The article is 9 bytes long, so we should get 4 bytes since we discard 5
	assert.Equal(t, int64(4), n)
}

func TestConnection_Capabilities(t *testing.T) {
	conn := articleReadyToDownload(t)

	// Test getting capabilities
	caps, err := conn.Capabilities()
	assert.NoError(t, err)
	assert.NotEmpty(t, caps)

	// Test capabilities response format
	for _, cap := range caps {
		assert.NotEmpty(t, cap)
	}
}

func TestConnection_Stat(t *testing.T) {
	conn := articleReadyToDownload(t)

	// Test successful stat
	number, err := conn.Stat("1234")
	assert.NoError(t, err)
	assert.Greater(t, number, 0)

	// Test stat with invalid message ID
	_, err = conn.Stat("nonexistent")
	assert.Error(t, err)
}

func TestConnection_Post_Error(t *testing.T) {
	conn := articleReadyToDownload(t)

	// Test posting invalid article
	invalidPost := bytes.NewBufferString("invalid post content")
	_, err := conn.Post(invalidPost)
	assert.Error(t, err)

	// Test posting with closed writer
	r, w := io.Pipe()
	_ = w.Close()

	_, err = conn.Post(r)

	assert.Error(t, err)
}

func TestConnection_BodyReader(t *testing.T) {
	conn := articleReadyToDownload(t)

	reader, err := conn.BodyReader("1234")
	assert.NoError(t, err)
	assert.NotNil(t, reader)

	defer func() {
		_ = reader.Close()
	}()

	var result bytes.Buffer
	n, err := io.Copy(&result, reader)
	if err != nil && err != io.EOF {
		t.Fatalf("Unexpected error: %v", err)
	}
	assert.Equal(t, int64(9), n)
	assert.Equal(t, "test text", result.String())
}

func TestConnection_BodyReader_InvalidMessageID(t *testing.T) {
	conn := articleReadyToDownload(t)

	reader, err := conn.BodyReader("nonexistent")
	assert.Error(t, err)
	assert.Nil(t, reader)
}

func TestArticleBodyReader_GetYencHeaders(t *testing.T) {
	conn := articleReadyToDownload(t)

	reader, err := conn.BodyReader("1234")
	assert.NoError(t, err)
	assert.NotNil(t, reader)

	defer func() {
		_ = reader.Close()
	}()

	headers, err := reader.GetYencHeaders()
	assert.NoError(t, err)
	assert.Equal(t, "webutils_pl", headers.FileName)
	assert.Equal(t, int64(9), headers.FileSize)
	assert.Equal(t, uint32(0x4570fa16), headers.Hash)
}

func TestArticleBodyReader_GetYencHeaders_ReturnsBufferedData(t *testing.T) {
	conn := articleReadyToDownload(t)

	reader, err := conn.BodyReader("1234")
	assert.NoError(t, err)
	assert.NotNil(t, reader)

	defer func() {
		_ = reader.Close()
	}()

	_, err = reader.GetYencHeaders()
	assert.NoError(t, err)

	buf := make([]byte, 1024)
	n, err := reader.Read(buf)
	assert.NoError(t, err)
	assert.Equal(t, 9, n)
	assert.Equal(t, "test text", string(buf[:n]))
}

func TestArticleBodyReader_Read_MultipleReads(t *testing.T) {
	conn := articleReadyToDownload(t)

	reader, err := conn.BodyReader("1234")
	assert.NoError(t, err)
	assert.NotNil(t, reader)

	defer func() {
		_ = reader.Close()
	}()

	buf1 := make([]byte, 4)
	n1, err := reader.Read(buf1)
	assert.NoError(t, err)
	assert.Equal(t, 4, n1)

	buf2 := make([]byte, 10)
	n2, err := reader.Read(buf2)
	if err != nil && err != io.EOF {
		t.Fatalf("Unexpected error: %v", err)
	}
	assert.Equal(t, 5, n2)

	combined := string(buf1[:n1]) + string(buf2[:n2])
	assert.Equal(t, "test text", combined)
}

func TestArticleBodyReader_Close(t *testing.T) {
	conn := articleReadyToDownload(t)

	reader, err := conn.BodyReader("1234")
	assert.NoError(t, err)
	assert.NotNil(t, reader)

	err = reader.Close()
	assert.NoError(t, err)

	buf := make([]byte, 1024)
	n, err := reader.Read(buf)
	assert.Equal(t, 0, n)
	assert.Equal(t, io.EOF, err)

	err = reader.Close()
	assert.NoError(t, err)
}

func TestArticleBodyReader_ReadAfterGetYencHeaders(t *testing.T) {
	conn := articleReadyToDownload(t)

	reader, err := conn.BodyReader("1234")
	assert.NoError(t, err)
	assert.NotNil(t, reader)

	defer func() {
		_ = reader.Close()
	}()

	headers, err := reader.GetYencHeaders()
	assert.NoError(t, err)
	assert.Equal(t, "webutils_pl", headers.FileName)

	var result bytes.Buffer
	n, err := io.Copy(&result, reader)
	assert.NoError(t, err)
	assert.Equal(t, int64(9), n)
	assert.Equal(t, "test text", result.String())
}

func articleReadyToDownload(t *testing.T) Connection {
	wg := &sync.WaitGroup{}

	ctx, cancel := context.WithCancel(context.Background())

	s, err := test.NewServer()
	assert.NoError(t, err)

	t.Cleanup(func() {
		cancel()
		s.Close()

		wg.Wait()
	})

	port := s.Port()

	wg.Add(1)

	go func() {
		defer wg.Done()
		s.Serve(ctx)
	}()

	var d net.Dialer
	netConn, err := d.DialContext(ctx, "tcp", fmt.Sprintf(":%d", port))
	assert.NoError(t, err)

	conn, err := newConnection(netConn, time.Now().Add(time.Hour), configDefault.OperationTimeout, configDefault.DrainTimeout)
	assert.NoError(t, err)

	t.Cleanup(func() {
		_ = conn.Close()
	})

	err = conn.JoinGroup("misc.test")
	assert.NoError(t, err)

	buf := bytes.NewBuffer(make([]byte, 0))
	_, err = buf.WriteString(examplepost)
	assert.NoError(t, err)

	encoded, err := os.ReadFile("test/fixtures/test.yenc")
	assert.NoError(t, err)

	_, err = buf.Write(encoded)
	assert.NoError(t, err)

	_, err = conn.Post(buf)
	assert.NoError(t, err)

	return conn
}

func TestBodyDecoded_MultipleSequentialCalls(t *testing.T) {
	conn := articleReadyToDownload(t)

	for i := 0; i < 10; i++ {
		callNum := i + 1
		t.Logf("Starting BodyDecoded call %d", callNum)

		done := make(chan struct{})
		var n int64
		var err error

		go func() {
			buf := bytes.NewBuffer(nil)
			n, err = conn.BodyDecoded("1234", buf, 0)
			close(done)
		}()

		select {
		case <-done:
			t.Logf("Call %d completed: n=%d, err=%v", callNum, n, err)
			if err != nil {
				t.Fatalf("Call %d failed: %v", callNum, err)
			}
			if n != 9 {
				t.Fatalf("Call %d: expected 9 bytes, got %d", callNum, n)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("Call %d timed out after 5s - blocking detected!", callNum)
		}
	}
}

func TestBodyDecoded_MultipleSequentialCalls_WithDiscard(t *testing.T) {
	conn := articleReadyToDownload(t)

	// Test with various discard values
	discardValues := []int64{1, 2, 3, 4, 5}

	for i, discard := range discardValues {
		callNum := i + 1
		t.Logf("Starting BodyDecoded call %d with discard=%d", callNum, discard)

		done := make(chan struct{})
		var n int64
		var err error

		go func() {
			buf := bytes.NewBuffer(nil)
			n, err = conn.BodyDecoded("1234", buf, discard)
			close(done)
		}()

		expectedSize := int64(9) - discard

		select {
		case <-done:
			t.Logf("Call %d completed: n=%d, err=%v", callNum, n, err)
			if err != nil {
				t.Fatalf("Call %d (discard=%d) failed: %v", callNum, discard, err)
			}
			if n != expectedSize {
				t.Fatalf("Call %d (discard=%d): expected %d bytes, got %d", callNum, discard, expectedSize, n)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("Call %d (discard=%d) timed out after 5s - blocking detected!", callNum, discard)
		}
	}
}

func TestBodyDecoded_MultipleSequentialCalls_LargeWithDiscard(t *testing.T) {
	// Use large fixture if it exists
	largeFixture := "test/fixtures/large.yenc"
	if _, err := os.Stat(largeFixture); os.IsNotExist(err) {
		t.Skip("large.yenc fixture not found, skipping")
	}

	wg := &sync.WaitGroup{}
	ctx, cancel := context.WithCancel(context.Background())

	s, err := test.NewServer()
	if err != nil {
		t.Fatalf("failed to create server: %v", err)
	}

	t.Cleanup(func() {
		cancel()
		s.Close()
		wg.Wait()
	})

	port := s.Port()
	wg.Add(1)
	go func() {
		defer wg.Done()
		s.Serve(ctx)
	}()

	var d net.Dialer
	netConn, err := d.DialContext(ctx, "tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		t.Fatalf("failed to dial: %v", err)
	}

	conn, err := newConnection(netConn, time.Now().Add(time.Hour), configDefault.OperationTimeout, configDefault.DrainTimeout)
	if err != nil {
		t.Fatalf("failed to create connection: %v", err)
	}

	t.Cleanup(func() {
		_ = conn.Close()
	})

	if err := conn.JoinGroup("misc.test"); err != nil {
		t.Fatalf("failed to join group: %v", err)
	}

	// Post large article
	encoded, err := os.ReadFile(largeFixture)
	if err != nil {
		t.Fatalf("failed to read fixture: %v", err)
	}

	buf := bytes.NewBuffer(nil)
	buf.WriteString(examplepost)
	buf.Write(encoded)

	if _, err := conn.Post(buf); err != nil {
		t.Fatalf("failed to post article: %v", err)
	}

	totalSize := int64(100 * 1024) // 100KB
	discardValues := []int64{1024, 2048, 4096, 8192, 16384}

	for i, discard := range discardValues {
		callNum := i + 1
		t.Logf("Starting large BodyDecoded call %d with discard=%d", callNum, discard)

		done := make(chan struct{})
		var n int64
		var decodeErr error

		go func() {
			outBuf := bytes.NewBuffer(nil)
			n, decodeErr = conn.BodyDecoded("1234", outBuf, discard)
			close(done)
		}()

		expectedSize := totalSize - discard

		select {
		case <-done:
			t.Logf("Call %d completed: n=%d, err=%v", callNum, n, decodeErr)
			if decodeErr != nil {
				t.Fatalf("Call %d (discard=%d) failed: %v", callNum, discard, decodeErr)
			}
			if n != expectedSize {
				t.Fatalf("Call %d (discard=%d): expected %d bytes, got %d", callNum, discard, expectedSize, n)
			}
		case <-time.After(30 * time.Second):
			t.Fatalf("Call %d (discard=%d) timed out after 30s - blocking detected!", callNum, discard)
		}
	}
}

func TestBodyDecoded_MultipleSequentialCalls_LargeFixture(t *testing.T) {
	// Use large fixture if it exists
	largeFixture := "test/fixtures/large.yenc"
	if _, err := os.Stat(largeFixture); os.IsNotExist(err) {
		t.Skip("large.yenc fixture not found, skipping")
	}

	wg := &sync.WaitGroup{}
	ctx, cancel := context.WithCancel(context.Background())

	s, err := test.NewServer()
	if err != nil {
		t.Fatalf("failed to create server: %v", err)
	}

	t.Cleanup(func() {
		cancel()
		s.Close()
		wg.Wait()
	})

	port := s.Port()
	wg.Add(1)
	go func() {
		defer wg.Done()
		s.Serve(ctx)
	}()

	var d net.Dialer
	netConn, err := d.DialContext(ctx, "tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		t.Fatalf("failed to dial: %v", err)
	}

	conn, err := newConnection(netConn, time.Now().Add(time.Hour), configDefault.OperationTimeout, configDefault.DrainTimeout)
	if err != nil {
		t.Fatalf("failed to create connection: %v", err)
	}

	t.Cleanup(func() {
		_ = conn.Close()
	})

	if err := conn.JoinGroup("misc.test"); err != nil {
		t.Fatalf("failed to join group: %v", err)
	}

	// Post large article
	encoded, err := os.ReadFile(largeFixture)
	if err != nil {
		t.Fatalf("failed to read fixture: %v", err)
	}

	buf := bytes.NewBuffer(nil)
	buf.WriteString(examplepost)
	buf.Write(encoded)

	if _, err := conn.Post(buf); err != nil {
		t.Fatalf("failed to post article: %v", err)
	}

	expectedSize := int64(100 * 1024) // 100KB

	for i := 0; i < 5; i++ {
		callNum := i + 1
		t.Logf("Starting large BodyDecoded call %d", callNum)

		done := make(chan struct{})
		var n int64
		var decodeErr error

		go func() {
			outBuf := bytes.NewBuffer(nil)
			n, decodeErr = conn.BodyDecoded("1234", outBuf, 0)
			close(done)
		}()

		select {
		case <-done:
			t.Logf("Call %d completed: n=%d, err=%v", callNum, n, decodeErr)
			if decodeErr != nil {
				t.Fatalf("Call %d failed: %v", callNum, decodeErr)
			}
			if n != expectedSize {
				t.Fatalf("Call %d: expected %d bytes, got %d", callNum, expectedSize, n)
			}
		case <-time.After(30 * time.Second):
			t.Fatalf("Call %d timed out after 30s - blocking detected!", callNum)
		}
	}
}
