package nntpcli

import (
	"errors"
	"io"
	"strings"
	"testing"
)

func TestNNTPReader_ReadLine(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		wantLine string
		wantErr  bool
	}{
		{
			name:     "simple line with CRLF",
			input:    "200 OK\r\n",
			wantLine: "200 OK",
			wantErr:  false,
		},
		{
			name:     "simple line with LF only",
			input:    "200 OK\n",
			wantLine: "200 OK",
			wantErr:  false,
		},
		{
			name:     "empty line",
			input:    "\r\n",
			wantLine: "",
			wantErr:  false,
		},
		{
			name:     "line with spaces",
			input:    "211 5 1 5 misc.test\r\n",
			wantLine: "211 5 1 5 misc.test",
			wantErr:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := newNNTPReader(strings.NewReader(tt.input))
			line, err := r.ReadLine()

			if (err != nil) != tt.wantErr {
				t.Errorf("ReadLine() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if line != tt.wantLine {
				t.Errorf("ReadLine() = %q, want %q", line, tt.wantLine)
			}
		})
	}
}

func TestNNTPReader_ReadCodeLine_Success(t *testing.T) {
	tests := []struct {
		name        string
		input       string
		expectCode  int
		wantCode    int
		wantMessage string
	}{
		{
			name:        "200 OK matching",
			input:       "200 OK\r\n",
			expectCode:  200,
			wantCode:    200,
			wantMessage: "OK",
		},
		{
			name:        "222 body follows",
			input:       "222 0 <msgid@example.com> article retrieved\r\n",
			expectCode:  222,
			wantCode:    222,
			wantMessage: "0 <msgid@example.com> article retrieved",
		},
		{
			name:        "no expect code",
			input:       "200 Welcome\r\n",
			expectCode:  0, // don't check
			wantCode:    200,
			wantMessage: "Welcome",
		},
		{
			name:        "code only no message",
			input:       "200\r\n",
			expectCode:  200,
			wantCode:    200,
			wantMessage: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := newNNTPReader(strings.NewReader(tt.input))
			code, message, err := r.ReadCodeLine(tt.expectCode)

			if err != nil {
				t.Errorf("ReadCodeLine() unexpected error = %v", err)
				return
			}

			if code != tt.wantCode {
				t.Errorf("ReadCodeLine() code = %d, want %d", code, tt.wantCode)
			}

			if message != tt.wantMessage {
				t.Errorf("ReadCodeLine() message = %q, want %q", message, tt.wantMessage)
			}
		})
	}
}

func TestNNTPReader_ReadCodeLine_WrongCode(t *testing.T) {
	r := newNNTPReader(strings.NewReader("430 No such article\r\n"))
	code, message, err := r.ReadCodeLine(222)

	if err == nil {
		t.Fatal("ReadCodeLine() expected error, got nil")
	}

	var nntpErr *NNTPError
	if !errors.As(err, &nntpErr) {
		t.Errorf("ReadCodeLine() error is not *NNTPError, got %T", err)
	}

	if code != 430 {
		t.Errorf("ReadCodeLine() code = %d, want 430", code)
	}

	if message != "No such article" {
		t.Errorf("ReadCodeLine() message = %q, want 'No such article'", message)
	}

	if nntpErr.Code != 430 {
		t.Errorf("NNTPError.Code = %d, want 430", nntpErr.Code)
	}
}

func TestNNTPReader_ReadCodeLine_Malformed(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{
			name:  "too short",
			input: "20\r\n",
		},
		{
			name:  "invalid code",
			input: "abc message\r\n",
		},
		{
			name:  "empty",
			input: "\r\n",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := newNNTPReader(strings.NewReader(tt.input))
			_, _, err := r.ReadCodeLine(200)

			if err == nil {
				t.Error("ReadCodeLine() expected error for malformed input")
			}

			var nntpErr *NNTPError
			if !errors.As(err, &nntpErr) {
				t.Errorf("ReadCodeLine() error is not *NNTPError, got %T", err)
			}
		})
	}
}

func TestDotReader_BasicRead(t *testing.T) {
	input := "Line 1\r\nLine 2\r\nLine 3\r\n.\r\n"
	r := newNNTPReader(strings.NewReader(input))
	dr := r.DotReader()

	output, err := io.ReadAll(dr)
	if err != nil {
		t.Fatalf("DotReader.Read() error = %v", err)
	}

	expected := "Line 1\r\nLine 2\r\nLine 3\r\n"
	if string(output) != expected {
		t.Errorf("DotReader output = %q, want %q", string(output), expected)
	}
}

func TestDotReader_DotUnstuffing(t *testing.T) {
	// ".." at start of line should become "."
	input := "..This line starts with a dot\r\n.\r\n"
	r := newNNTPReader(strings.NewReader(input))
	dr := r.DotReader()

	output, err := io.ReadAll(dr)
	if err != nil {
		t.Fatalf("DotReader.Read() error = %v", err)
	}

	expected := ".This line starts with a dot\r\n"
	if string(output) != expected {
		t.Errorf("DotReader output = %q, want %q", string(output), expected)
	}
}

func TestDotReader_MultipleDots(t *testing.T) {
	// Line with multiple dots (only first is unstuffed)
	input := "...multiple dots\r\n.\r\n"
	r := newNNTPReader(strings.NewReader(input))
	dr := r.DotReader()

	output, err := io.ReadAll(dr)
	if err != nil {
		t.Fatalf("DotReader.Read() error = %v", err)
	}

	expected := "..multiple dots\r\n"
	if string(output) != expected {
		t.Errorf("DotReader output = %q, want %q", string(output), expected)
	}
}

func TestDotReader_EmptyLines(t *testing.T) {
	input := "Line 1\r\n\r\nLine 3\r\n.\r\n"
	r := newNNTPReader(strings.NewReader(input))
	dr := r.DotReader()

	output, err := io.ReadAll(dr)
	if err != nil {
		t.Fatalf("DotReader.Read() error = %v", err)
	}

	expected := "Line 1\r\n\r\nLine 3\r\n"
	if string(output) != expected {
		t.Errorf("DotReader output = %q, want %q", string(output), expected)
	}
}

func TestDotReader_BareNewline(t *testing.T) {
	// Also accept ".\n" as terminator (bare LF)
	input := "Line 1\nLine 2\n.\n"
	r := newNNTPReader(strings.NewReader(input))
	dr := r.DotReader()

	output, err := io.ReadAll(dr)
	if err != nil {
		t.Fatalf("DotReader.Read() error = %v", err)
	}

	expected := "Line 1\nLine 2\n"
	if string(output) != expected {
		t.Errorf("DotReader output = %q, want %q", string(output), expected)
	}
}

func TestDotReader_SmallBuffer(t *testing.T) {
	input := "Line 1\r\nLine 2\r\n.\r\n"
	r := newNNTPReader(strings.NewReader(input))
	dr := r.DotReader()

	// Read with a small buffer to test partial reads
	var result []byte
	buf := make([]byte, 3)

	for {
		n, err := dr.Read(buf)
		if n > 0 {
			result = append(result, buf[:n]...)
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("DotReader.Read() error = %v", err)
		}
	}

	expected := "Line 1\r\nLine 2\r\n"
	if string(result) != expected {
		t.Errorf("DotReader output = %q, want %q", string(result), expected)
	}
}
