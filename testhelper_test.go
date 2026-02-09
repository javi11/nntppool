package nntppool

import (
	"bytes"
	"fmt"
	"io"
	"net"
	"testing"

	"github.com/mnightingale/rapidyenc"
)

// mockNNTPResponse builds a full NNTP response byte sequence.
// For multiline responses, bodyLines are terminated with ".\r\n".
func mockNNTPResponse(statusLine string, bodyLines ...string) []byte {
	var buf bytes.Buffer
	buf.WriteString(statusLine)
	buf.WriteString("\r\n")
	for _, line := range bodyLines {
		buf.WriteString(line)
		buf.WriteString("\r\n")
	}
	if len(bodyLines) > 0 || isMultilineStatus(statusLine) {
		buf.WriteString(".\r\n")
	}
	return buf.Bytes()
}

func isMultilineStatus(status string) bool {
	if len(status) < 3 {
		return false
	}
	code := 0
	for i := range 3 {
		if status[i] < '0' || status[i] > '9' {
			return false
		}
		code = code*10 + int(status[i]-'0')
	}
	return isMultiline(code)
}

// yencSinglePart produces a complete 222 BODY response with a yEnc single-part encoded payload.
func yencSinglePart(data []byte, fileName string) []byte {
	var encoded bytes.Buffer
	enc, err := rapidyenc.NewEncoder(&encoded, rapidyenc.Meta{
		FileName:   fileName,
		FileSize:   int64(len(data)),
		PartNumber: 1,
		TotalParts: 1,
		Offset:     0,
		PartSize:   int64(len(data)),
	})
	if err != nil {
		panic(fmt.Sprintf("yencSinglePart: NewEncoder: %v", err))
	}
	if _, err := enc.Write(data); err != nil {
		panic(fmt.Sprintf("yencSinglePart: Write: %v", err))
	}
	if err := enc.Close(); err != nil {
		panic(fmt.Sprintf("yencSinglePart: Close: %v", err))
	}

	var buf bytes.Buffer
	buf.WriteString("222 0 <test@example.com> body\r\n")
	buf.Write(encoded.Bytes())
	buf.WriteString(".\r\n")
	return buf.Bytes()
}

// yencMultiPart produces a complete 222 BODY response with a yEnc multi-part encoded payload.
func yencMultiPart(data []byte, fileName string, part, total int, offset int64) []byte {
	var encoded bytes.Buffer
	enc, err := rapidyenc.NewEncoder(&encoded, rapidyenc.Meta{
		FileName:   fileName,
		FileSize:   int64(len(data)*total), // approximate total
		PartNumber: int64(part),
		TotalParts: int64(total),
		Offset:     offset,
		PartSize:   int64(len(data)),
	})
	if err != nil {
		panic(fmt.Sprintf("yencMultiPart: NewEncoder: %v", err))
	}
	if _, err := enc.Write(data); err != nil {
		panic(fmt.Sprintf("yencMultiPart: Write: %v", err))
	}
	if err := enc.Close(); err != nil {
		panic(fmt.Sprintf("yencMultiPart: Close: %v", err))
	}

	var buf bytes.Buffer
	buf.WriteString("222 0 <test@example.com> body\r\n")
	buf.Write(encoded.Bytes())
	buf.WriteString(".\r\n")
	return buf.Bytes()
}

// mockServer creates a net.Pipe pair and runs handler on the server side.
// Returns the client-side conn. The server goroutine is cleaned up via t.Cleanup.
func mockServer(t *testing.T, handler func(net.Conn)) net.Conn {
	t.Helper()
	client, server := net.Pipe()
	done := make(chan struct{})
	go func() {
		defer close(done)
		handler(server)
		_ = server.Close()
	}()
	t.Cleanup(func() {
		_ = client.Close()
		<-done
	})
	return client
}

// mockFeeder implements streamFeeder for readBuffer tests.
type mockFeeder struct {
	feedFunc func(in []byte, out io.Writer) (consumed int, done bool, err error)
}

func (m *mockFeeder) Feed(in []byte, out io.Writer) (consumed int, done bool, err error) {
	return m.feedFunc(in, out)
}
