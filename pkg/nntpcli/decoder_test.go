package nntpcli

import (
	"bytes"
	"io"
	"os"
	"strings"
	"testing"
)

func TestIncrementalDecoder_BasicDecode(t *testing.T) {
	// Read the test yenc file
	data, err := os.ReadFile("test/fixtures/test.yenc")
	if err != nil {
		t.Fatalf("Failed to read test fixture: %v", err)
	}

	dec := newIncrementalDecoder(bytes.NewReader(data))
	output := new(bytes.Buffer)

	n, err := io.Copy(output, dec)
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	if n != 9 {
		t.Errorf("Expected 9 bytes, got %d", n)
	}

	if output.String() != "test text" {
		t.Errorf("Expected 'test text', got %q", output.String())
	}
}

func TestIncrementalDecoder_SkipsHeaders(t *testing.T) {
	// Test that =ybegin and =ypart headers are skipped
	input := "=ybegin line=128 size=9 name=test\r\n" +
		"=ypart begin=1 end=9\r\n" +
		"\x9e\x8f\x9d\x9eJ\x9e\x8f\xa2\x9e\r\n" + // encoded "test text"
		"=yend size=9 crc32=4570fa16\r\n"

	dec := newIncrementalDecoder(strings.NewReader(input))
	output := new(bytes.Buffer)

	_, err := io.Copy(output, dec)
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	if output.String() != "test text" {
		t.Errorf("Expected 'test text', got %q", output.String())
	}
}

func TestIncrementalDecoder_MultipleReads(t *testing.T) {
	data, err := os.ReadFile("test/fixtures/test.yenc")
	if err != nil {
		t.Fatalf("Failed to read test fixture: %v", err)
	}

	dec := newIncrementalDecoder(bytes.NewReader(data))

	// Read in small chunks
	var result []byte
	buf := make([]byte, 3) // Small buffer to force multiple reads

	for {
		n, err := dec.Read(buf)
		if n > 0 {
			result = append(result, buf[:n]...)
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("Read failed: %v", err)
		}
	}

	if string(result) != "test text" {
		t.Errorf("Expected 'test text', got %q", string(result))
	}
}

func TestIncrementalDecoder_EmptyInput(t *testing.T) {
	dec := newIncrementalDecoder(strings.NewReader(""))

	buf := make([]byte, 100)
	n, err := dec.Read(buf)

	if n != 0 {
		t.Errorf("Expected 0 bytes, got %d", n)
	}

	if err != io.EOF {
		t.Errorf("Expected EOF, got %v", err)
	}
}

func TestIncrementalDecoder_DetectsEnd(t *testing.T) {
	data, err := os.ReadFile("test/fixtures/test.yenc")
	if err != nil {
		t.Fatalf("Failed to read test fixture: %v", err)
	}

	dec := newIncrementalDecoder(bytes.NewReader(data))

	// Read all data
	output := new(bytes.Buffer)
	_, err = io.Copy(output, dec)
	if err != nil {
		t.Fatalf("Copy failed: %v", err)
	}

	// Subsequent reads should return EOF
	buf := make([]byte, 100)
	n, err := dec.Read(buf)

	if n != 0 {
		t.Errorf("Expected 0 bytes after EOF, got %d", n)
	}

	if err != io.EOF {
		t.Errorf("Expected EOF after end, got %v", err)
	}
}

func TestIncrementalDecoder_OnlyYbeginHeader(t *testing.T) {
	// Test with only =ybegin (no =ypart)
	input := "=ybegin line=128 size=9 name=test\r\n" +
		"\x9e\x8f\x9d\x9eJ\x9e\x8f\xa2\x9e\r\n" +
		"=yend size=9 crc32=4570fa16\r\n"

	dec := newIncrementalDecoder(strings.NewReader(input))
	output := new(bytes.Buffer)

	_, err := io.Copy(output, dec)
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	if output.String() != "test text" {
		t.Errorf("Expected 'test text', got %q", output.String())
	}
}
