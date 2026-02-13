package nntppool

import (
	"bytes"
	"fmt"
	"io"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/mnightingale/rapidyenc"
)

// --- Feed: single/multi-line responses ---

func TestFeed_SingleLineResponse(t *testing.T) {
	tests := []struct {
		name       string
		input      string
		wantCode   int
		wantDone   bool
		wantMsg    string
	}{
		{"200 ok", "200 server ready\r\n", 200, true, "200 server ready"},
		{"430 not found", "430 no such article\r\n", 430, true, "430 no such article"},
		{"223 stat", "223 12345 <msg@id> article exists\r\n", 223, true, "223 12345 <msg@id> article exists"},
		{"480 auth required", "480 authentication required\r\n", 480, true, "480 authentication required"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &NNTPResponse{}
			consumed, done, err := r.Feed([]byte(tt.input), io.Discard)
			if err != nil {
				t.Fatalf("Feed() error = %v", err)
			}
			if done != tt.wantDone {
				t.Errorf("done = %v, want %v", done, tt.wantDone)
			}
			if r.StatusCode != tt.wantCode {
				t.Errorf("StatusCode = %d, want %d", r.StatusCode, tt.wantCode)
			}
			if r.Message != tt.wantMsg {
				t.Errorf("Message = %q, want %q", r.Message, tt.wantMsg)
			}
			if consumed != len(tt.input) {
				t.Errorf("consumed = %d, want %d", consumed, len(tt.input))
			}
		})
	}
}

func TestFeed_BinaryData(t *testing.T) {
	// Simulate TLS/plaintext mismatch: binary garbage instead of a text response.
	binary := []byte{0x16, 0x03, 0x01, 0x02, 0x00, 0x01, 0x00, 0x01,
		0xfc, 0x03, 0x03, 0xab, 0xcd, 0xef, 0x12, 0x34,
		0x56, 0x78, 0x9a, 0xbc}
	input := append(binary, []byte("\r\n")...)

	r := &NNTPResponse{}
	consumed, done, err := r.Feed(input, io.Discard)
	if err != nil {
		t.Fatalf("Feed() error = %v", err)
	}
	if !done {
		t.Error("expected done=true for unparseable response")
	}
	if r.StatusCode != 0 {
		t.Errorf("StatusCode = %d, want 0", r.StatusCode)
	}
	if consumed != len(input) {
		t.Errorf("consumed = %d, want %d", consumed, len(input))
	}
	if !strings.Contains(r.Message, "unexpected binary data") {
		t.Errorf("Message = %q, should contain 'unexpected binary data'", r.Message)
	}
	if !strings.Contains(r.Message, "hex:") {
		t.Errorf("Message = %q, should contain hex dump", r.Message)
	}
	if !strings.Contains(r.Message, "TLS/plaintext mismatch") {
		t.Errorf("Message = %q, should contain diagnostic hint", r.Message)
	}
}

func TestFeed_MultilineHead(t *testing.T) {
	input := mockNNTPResponse("221 0 <test@id> head",
		"Subject: Test Article",
		"From: user@example.com",
		"Date: Mon, 01 Jan 2024 00:00:00 +0000",
	)

	r := &NNTPResponse{}
	var buf bytes.Buffer
	consumed, done, err := r.Feed(input, &buf)
	if err != nil {
		t.Fatalf("Feed() error = %v", err)
	}
	if !done {
		t.Error("expected done=true for complete HEAD response")
	}
	if r.StatusCode != 221 {
		t.Errorf("StatusCode = %d, want 221", r.StatusCode)
	}
	if consumed != len(input) {
		t.Errorf("consumed = %d, want %d", consumed, len(input))
	}
	if len(r.Lines) != 3 {
		t.Errorf("Lines = %d, want 3", len(r.Lines))
	}
}

func TestFeed_MultilineCapabilities(t *testing.T) {
	input := mockNNTPResponse("101 Capability list",
		"VERSION 2",
		"READER",
		"POST",
		"OVER",
	)

	r := &NNTPResponse{}
	consumed, done, err := r.Feed(input, io.Discard)
	if err != nil {
		t.Fatalf("Feed() error = %v", err)
	}
	if !done {
		t.Error("expected done=true")
	}
	if r.StatusCode != 101 {
		t.Errorf("StatusCode = %d, want 101", r.StatusCode)
	}
	if consumed != len(input) {
		t.Errorf("consumed = %d, want %d", consumed, len(input))
	}
	if len(r.Lines) != 4 {
		t.Errorf("Lines = %d, want 4: %v", len(r.Lines), r.Lines)
	}
}

// --- Feed: yEnc decoding (hot path) ---

func TestFeed_YencSinglePart(t *testing.T) {
	original := []byte("Hello, yEnc world! This is test data for single part encoding.")
	input := yencSinglePart(original, "test.bin")

	r := &NNTPResponse{}
	var decoded bytes.Buffer
	consumed, done, err := r.Feed(input, &decoded)
	if err != nil {
		t.Fatalf("Feed() error = %v", err)
	}
	if !done {
		t.Error("expected done=true")
	}
	if r.StatusCode != 222 {
		t.Errorf("StatusCode = %d, want 222", r.StatusCode)
	}
	if consumed != len(input) {
		t.Errorf("consumed = %d, want %d", consumed, len(input))
	}
	if r.Format != rapidyenc.FormatYenc {
		t.Errorf("Format = %d, want FormatYenc", r.Format)
	}
	if !bytes.Equal(decoded.Bytes(), original) {
		t.Errorf("decoded bytes don't match original: got %d bytes, want %d", decoded.Len(), len(original))
	}
	if r.BytesDecoded != len(original) {
		t.Errorf("BytesDecoded = %d, want %d", r.BytesDecoded, len(original))
	}
}

func TestFeed_YencMultiPart(t *testing.T) {
	original := []byte("Part data for multipart test content here.")
	input := yencMultiPart(original, "multi.bin", 2, 5, 1000)

	r := &NNTPResponse{}
	var decoded bytes.Buffer
	consumed, done, err := r.Feed(input, &decoded)
	if err != nil {
		t.Fatalf("Feed() error = %v", err)
	}
	if !done {
		t.Error("expected done=true")
	}
	if r.YEnc.Part != 2 {
		t.Errorf("Part = %d, want 2", r.YEnc.Part)
	}
	if r.YEnc.Total != 5 {
		t.Errorf("Total = %d, want 5", r.YEnc.Total)
	}
	if r.YEnc.FileName != "multi.bin" {
		t.Errorf("FileName = %q, want %q", r.YEnc.FileName, "multi.bin")
	}
	if consumed != len(input) {
		t.Errorf("consumed = %d, want %d", consumed, len(input))
	}
}

func TestFeed_YencCRC(t *testing.T) {
	original := []byte("CRC test data payload.")
	input := yencSinglePart(original, "crc.bin")

	r := &NNTPResponse{}
	var decoded bytes.Buffer
	_, done, err := r.Feed(input, &decoded)
	if err != nil {
		t.Fatalf("Feed() error = %v", err)
	}
	if !done {
		t.Fatal("expected done=true")
	}

	// Valid CRC: ExpectedCRC should be non-zero and match CRC
	if r.ExpectedCRC == 0 {
		t.Error("ExpectedCRC should be non-zero")
	}
	if r.CRC != r.ExpectedCRC {
		t.Errorf("CRC mismatch: CRC=%08x, ExpectedCRC=%08x", r.CRC, r.ExpectedCRC)
	}
}

// --- Feed: network fragmentation ---

func TestFeed_ByteAtATime(t *testing.T) {
	original := []byte("Byte by byte test data.")
	input := yencSinglePart(original, "byte.bin")

	// Use readBuffer + net.Pipe to deliver 1 byte at a time,
	// matching the real-world accumulation pattern.
	client, server := net.Pipe()
	defer func() { _ = client.Close() }()

	go func() {
		for _, b := range input {
			_, _ = server.Write([]byte{b})
		}
		_ = server.Close()
	}()

	r := &NNTPResponse{}
	var decoded bytes.Buffer
	var rb readBuffer
	err := rb.feedUntilDone(client, r, &decoded, func() (time.Time, bool) {
		return time.Time{}, false
	})
	if err != nil {
		t.Fatalf("feedUntilDone() error = %v", err)
	}
	if r.StatusCode != 222 {
		t.Errorf("StatusCode = %d, want 222", r.StatusCode)
	}
	if !bytes.Equal(decoded.Bytes(), original) {
		t.Errorf("decoded = %q, want %q", decoded.Bytes(), original)
	}
}

func TestFeed_ChunkedFeeding(t *testing.T) {
	original := []byte("Chunked feeding test data for various chunk sizes.")
	input := yencSinglePart(original, "chunk.bin")

	for _, chunkSize := range []int{3, 7, 13, 64, 128} {
		t.Run(fmt.Sprintf("chunk_%d", chunkSize), func(t *testing.T) {
			r := &NNTPResponse{}
			var decoded bytes.Buffer
			var accum []byte
			pos := 0
			for pos < len(input) {
				end := min(pos+chunkSize, len(input))
				accum = append(accum, input[pos:end]...)
				pos = end
				consumed, done, err := r.Feed(accum, &decoded)
				if err != nil {
					t.Fatalf("Feed(chunk at %d) error = %v", pos, err)
				}
				accum = accum[consumed:]
				if done {
					break
				}
			}
			if !bytes.Equal(decoded.Bytes(), original) {
				t.Errorf("chunk_%d decoded %d bytes, want %d", chunkSize, decoded.Len(), len(original))
			}
		})
	}
}

// --- Feed: edge cases ---

func TestFeed_NilWriter(t *testing.T) {
	input := []byte("200 ok\r\n")
	r := &NNTPResponse{}
	// nil out should be replaced with io.Discard, no panic
	_, done, err := r.Feed(input, nil)
	if err != nil {
		t.Fatalf("Feed(nil writer) error = %v", err)
	}
	if !done {
		t.Error("expected done=true")
	}
}

func TestFeed_OnMetaCallback(t *testing.T) {
	original := []byte("Meta callback test data.")
	input := yencSinglePart(original, "meta.bin")

	var metaCalled int
	var gotMeta YEncMeta
	r := &NNTPResponse{
		onMeta: func(m YEncMeta) {
			metaCalled++
			gotMeta = m
		},
	}

	var decoded bytes.Buffer
	_, _, err := r.Feed(input, &decoded)
	if err != nil {
		t.Fatalf("Feed() error = %v", err)
	}
	if metaCalled != 1 {
		t.Errorf("onMeta called %d times, want 1", metaCalled)
	}
	if gotMeta.FileName != "meta.bin" {
		t.Errorf("onMeta FileName = %q, want %q", gotMeta.FileName, "meta.bin")
	}
	if r.onMeta != nil {
		t.Error("onMeta should be set to nil after invocation")
	}
}

func TestFeed_EmptyBuffer(t *testing.T) {
	r := &NNTPResponse{
		body:   true,
		Format: rapidyenc.FormatYenc,
	}
	n, err := r.decodeYenc([]byte{}, io.Discard)
	if n != 0 || err != nil {
		t.Errorf("decodeYenc(empty) = (%d, %v), want (0, nil)", n, err)
	}
}

// --- detectFormat ---

func TestDetectFormat(t *testing.T) {
	tests := []struct {
		name       string
		statusCode int
		line       string
		wantFormat rapidyenc.Format
	}{
		{"ybegin", 222, "=ybegin line=128 size=100 name=test.bin", rapidyenc.FormatYenc},
		{"M-line 61 chars", 222, "M" + string(bytes.Repeat([]byte(" "), 60)), rapidyenc.FormatUU},
		{"non-body status", 200, "=ybegin line=128 size=100 name=test.bin", rapidyenc.FormatUnknown},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &NNTPResponse{StatusCode: tt.statusCode}
			r.detectFormat([]byte(tt.line))
			if r.Format != tt.wantFormat {
				t.Errorf("Format = %d, want %d", r.Format, tt.wantFormat)
			}
		})
	}

	// Empty line sets hasEmptyline
	t.Run("empty line sets hasEmptyline", func(t *testing.T) {
		r := &NNTPResponse{StatusCode: 222}
		r.detectFormat([]byte{})
		if !r.hasEmptyline {
			t.Error("empty line should set hasEmptyline")
		}
	})
}

func TestDetectFormat_ArticleVsBody(t *testing.T) {
	// For 60-char M-line UU detection, create a proper line
	uuLine := make([]byte, 61)
	uuLine[0] = 'M'
	for i := 1; i < 61; i++ {
		uuLine[i] = ' ' + byte(i%64)
		if uuLine[i] > 96 {
			uuLine[i] = ' '
		}
	}

	// StatusCode 222 (BODY) — UU detection works without empty line
	r222 := &NNTPResponse{StatusCode: 222}
	r222.detectFormat(uuLine)
	if r222.Format == rapidyenc.FormatYenc {
		t.Error("should not detect yenc from UU line")
	}

	// StatusCode 220 (ARTICLE) — needs empty line before UU detection kicks in
	r220noEmpty := &NNTPResponse{StatusCode: 220}
	r220noEmpty.detectFormat(uuLine)
	// Without hasEmptyline, should NOT detect UU (short lines not detected)
	// But 61-char M-line detection happens before the hasEmptyline check

	r220withEmpty := &NNTPResponse{StatusCode: 220, hasEmptyline: true}
	r220withEmpty.detectFormat(uuLine)
	// With hasEmptyline, detection should work the same as BODY
}

// --- processYencHeader ---

func TestProcessYencHeader(t *testing.T) {
	t.Run("ybegin single part", func(t *testing.T) {
		r := &NNTPResponse{}
		r.processYencHeader([]byte("=ybegin line=128 size=5000 name=test.bin"))
		if !r.body {
			t.Error("single-part =ybegin should set body=true")
		}
		if r.YEnc.FileSize != 5000 {
			t.Errorf("FileSize = %d, want 5000", r.YEnc.FileSize)
		}
		if r.YEnc.FileName != "test.bin" {
			t.Errorf("FileName = %q, want test.bin", r.YEnc.FileName)
		}
		if r.YEnc.PartSize != 5000 {
			t.Error("single-part PartSize should equal FileSize")
		}
	})

	t.Run("ybegin multi part", func(t *testing.T) {
		r := &NNTPResponse{}
		r.processYencHeader([]byte("=ybegin part=2 total=5 line=128 size=25000 name=multi.bin"))
		if r.body {
			t.Error("multi-part =ybegin should NOT set body=true (needs =ypart)")
		}
		if r.YEnc.Part != 2 {
			t.Errorf("Part = %d, want 2", r.YEnc.Part)
		}
		if r.YEnc.Total != 5 {
			t.Errorf("Total = %d, want 5", r.YEnc.Total)
		}
	})

	t.Run("ypart 1-based to 0-based", func(t *testing.T) {
		r := &NNTPResponse{}
		r.processYencHeader([]byte("=ypart begin=1 end=5000"))
		if !r.body {
			t.Error("=ypart should set body=true")
		}
		if r.YEnc.PartBegin != 0 {
			t.Errorf("PartBegin = %d, want 0 (1-based→0-based)", r.YEnc.PartBegin)
		}
		if r.YEnc.PartSize != 5000 {
			t.Errorf("PartSize = %d, want 5000", r.YEnc.PartSize)
		}
	})

	t.Run("yend pcrc32 priority", func(t *testing.T) {
		r := &NNTPResponse{}
		r.processYencHeader([]byte("=yend size=5000 pcrc32=AABBCCDD crc32=11223344"))
		if !r.hasCrc {
			t.Error("should have CRC")
		}
		if r.ExpectedCRC != 0xAABBCCDD {
			t.Errorf("ExpectedCRC = %08X, want AABBCCDD (pcrc32 takes priority)", r.ExpectedCRC)
		}
	})

	t.Run("yend crc32 fallback", func(t *testing.T) {
		r := &NNTPResponse{}
		r.processYencHeader([]byte("=yend size=5000 crc32=11223344"))
		if !r.hasCrc {
			t.Error("should have CRC")
		}
		if r.ExpectedCRC != 0x11223344 {
			t.Errorf("ExpectedCRC = %08X, want 11223344", r.ExpectedCRC)
		}
	})

	t.Run("yend no crc", func(t *testing.T) {
		r := &NNTPResponse{}
		r.processYencHeader([]byte("=yend size=5000"))
		if r.hasCrc {
			t.Error("should not have CRC")
		}
	})
}

// --- extractString ---

func TestExtractString(t *testing.T) {
	tests := []struct {
		name    string
		data    string
		substr  string
		want    string
		wantErr bool
	}{
		// extractString stops at \x00, \r, \n — NOT space (name= is always last on the line)
		{"normal", " name=test.bin size=100", " name=", "test.bin size=100", false},
		{"null-terminated", " name=test.bin\x00extra", " name=", "test.bin", false},
		{"not found", " size=100", " name=", "", true},
		{"at end", " name=test.bin", " name=", "test.bin", false},
		{"empty value at end", " name=", " name=", "", false},
		{"newline-terminated", " name=test.bin\r\n", " name=", "test.bin", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := extractString([]byte(tt.data), []byte(tt.substr))
			if (err != nil) != tt.wantErr {
				t.Errorf("err = %v, wantErr = %v", err, tt.wantErr)
			}
			if got != tt.want {
				t.Errorf("extractString() = %q, want %q", got, tt.want)
			}
		})
	}
}

// --- extractInt ---

func TestExtractInt(t *testing.T) {
	tests := []struct {
		name    string
		data    string
		substr  string
		want    int64
		wantErr bool
	}{
		{"normal", " size=5000 name=foo", " size=", 5000, false},
		{"at end", " size=5000", " size=", 5000, false},
		{"not found", " name=foo", " size=", 0, true},
		{"invalid", " size=abc", " size=", 0, true},
		{"negative", " offset=-100", " offset=", -100, false},
		{"zero", " size=0", " size=", 0, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := extractInt([]byte(tt.data), []byte(tt.substr))
			if (err != nil) != tt.wantErr {
				t.Errorf("err = %v, wantErr = %v", err, tt.wantErr)
			}
			if got != tt.want {
				t.Errorf("extractInt() = %d, want %d", got, tt.want)
			}
		})
	}
}

// --- extractCRC ---

func TestExtractCRC(t *testing.T) {
	tests := []struct {
		name    string
		data    string
		substr  string
		want    uint32
		wantErr bool
	}{
		{"full 8-char", " pcrc32=AABBCCDD", " pcrc32=", 0xAABBCCDD, false},
		{"short left-padded", " pcrc32=AABB", " pcrc32=", 0x0000AABB, false},
		{"not found", " crc32=AABB", " pcrc32=", 0, true},
		{"lowercase", " pcrc32=aabbccdd", " pcrc32=", 0xAABBCCDD, false},
		{">8 chars takes last 8", " pcrc32=00AABBCCDD", " pcrc32=", 0xAABBCCDD, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := extractCRC([]byte(tt.data), []byte(tt.substr))
			if (err != nil) != tt.wantErr {
				t.Errorf("err = %v, wantErr = %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && got != tt.want {
				t.Errorf("extractCRC() = %08X, want %08X", got, tt.want)
			}
		})
	}
}

// --- Helper functions ---

func TestAllInASCIIRange(t *testing.T) {
	tests := []struct {
		name string
		b    []byte
		lo   byte
		hi   byte
		want bool
	}{
		{"in range", []byte{32, 64, 96}, 32, 96, true},
		{"below", []byte{31, 64}, 32, 96, false},
		{"above", []byte{32, 97}, 32, 96, false},
		{"empty", []byte{}, 32, 96, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := allInASCIIRange(tt.b, tt.lo, tt.hi); got != tt.want {
				t.Errorf("allInASCIIRange() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestOnlySpaceOrBacktick(t *testing.T) {
	tests := []struct {
		name string
		b    []byte
		want bool
	}{
		{"spaces only", []byte("   "), true},
		{"backticks only", []byte("```"), true},
		{"mixed", []byte(" ` "), true},
		{"invalid char", []byte(" a "), false},
		{"empty", []byte{}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := onlySpaceOrBacktick(tt.b); got != tt.want {
				t.Errorf("onlySpaceOrBacktick() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDecodeUUChar(t *testing.T) {
	tests := []struct {
		c    byte
		want int
	}{
		{'`', 0},   // backtick → 0
		{' ', 0},   // space → 0
		{'!', 1},   // ! → 1
		{'M', 45},  // M → 45
	}
	for _, tt := range tests {
		if got := decodeUUChar(tt.c); got != tt.want {
			t.Errorf("decodeUUChar(%q) = %d, want %d", tt.c, got, tt.want)
		}
	}
}

func TestDecodeUUCharWorkaround(t *testing.T) {
	// Verify formula: ((int(c)-32)&63)*4+5)/3
	// For 'M' (77): ((77-32)&63)*4+5)/3 = (45*4+5)/3 = 185/3 = 61
	got := decodeUUCharWorkaround('M')
	want := int(((int('M') - 32) & 63 * 4) + 5) / 3
	if got != want {
		t.Errorf("decodeUUCharWorkaround('M') = %d, want %d", got, want)
	}
}

func TestIsMultiline(t *testing.T) {
	tests := []struct {
		code int
		want bool
	}{
		{222, true},  // BODY
		{220, true},  // ARTICLE
		{221, true},  // HEAD
		{101, true},  // CAPABILITIES
		{200, false},
		{430, false},
		{480, false},
		{0, false},
	}
	for _, tt := range tests {
		if got := isMultiline(tt.code); got != tt.want {
			t.Errorf("isMultiline(%d) = %v, want %v", tt.code, got, tt.want)
		}
	}
}
