package nntpcli

import (
	"bufio"
	"io"
	"strings"
)

const (
	// defaultBufferSize is the default buffer size for reading (32KB).
	defaultBufferSize = 32 * 1024
)

// readBuffer wraps a bufio.Reader to provide buffered reading operations.
// This replaces the custom window-slicing implementation with standard bufio
// for better compatibility with rapidyenc and other readers.
type readBuffer struct {
	r *bufio.Reader
}

// newReadBuffer creates a new readBuffer with the default buffer size.
func newReadBuffer(r io.Reader) *readBuffer {
	return &readBuffer{
		r: bufio.NewReaderSize(r, defaultBufferSize),
	}
}

// ReadLine reads a single line from the buffer, stripping the trailing \r\n or \n.
func (b *readBuffer) ReadLine() (string, error) {
	line, err := b.r.ReadString('\n')
	if err != nil && len(line) == 0 {
		return "", err
	}

	// Strip trailing \r\n or \n
	line = strings.TrimSuffix(line, "\n")
	line = strings.TrimSuffix(line, "\r")

	return line, err
}

// Read implements io.Reader, reading from the buffer.
func (b *readBuffer) Read(p []byte) (int, error) {
	return b.r.Read(p)
}

// Reset resets the buffer to read from a new reader.
func (b *readBuffer) Reset(r io.Reader) {
	b.r.Reset(r)
}
