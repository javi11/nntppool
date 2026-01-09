package nntpcli

import (
	"bufio"
	"fmt"
	"io"
)

const (
	// writerBufferSize is the buffer size for the writer (4KB).
	writerBufferSize = 4 * 1024
)

// nntpWriter wraps a bufio.Writer to provide NNTP-specific writing operations.
type nntpWriter struct {
	w *bufio.Writer
}

// newNNTPWriter creates a new nntpWriter wrapping the given io.Writer.
func newNNTPWriter(w io.Writer) *nntpWriter {
	return &nntpWriter{
		w: bufio.NewWriterSize(w, writerBufferSize),
	}
}

// PrintfLine formats and writes a line ending with \r\n.
func (w *nntpWriter) PrintfLine(format string, args ...any) error {
	_, err := fmt.Fprintf(w.w, format+"\r\n", args...)
	return err
}

// Flush flushes the underlying buffer.
func (w *nntpWriter) Flush() error {
	return w.w.Flush()
}

// DotWriter returns an io.WriteCloser that handles dot-stuffing for POST operations.
// Lines starting with "." are doubled, and Close() writes the terminating ".\r\n".
func (w *nntpWriter) DotWriter() io.WriteCloser {
	return &dotWriter{
		w:     w.w,
		state: dotWriterStateBeginLine,
	}
}

// dotWriterState tracks position within a line for dot-stuffing.
type dotWriterState int

const (
	dotWriterStateBeginLine dotWriterState = iota // at start of line
	dotWriterStateMidLine                         // in middle of line
)

// dotWriter handles NNTP dot-stuffing for POST operations.
// According to RFC 3977, lines starting with a dot must have the dot doubled,
// and the message is terminated by a line containing only a dot.
type dotWriter struct {
	w      *bufio.Writer
	state  dotWriterState
	closed bool
}

func (d *dotWriter) Write(p []byte) (n int, err error) {
	if d.closed {
		return 0, io.ErrClosedPipe
	}

	for _, b := range p {
		// If at beginning of line and byte is a dot, double it
		if d.state == dotWriterStateBeginLine && b == '.' {
			if err := d.w.WriteByte('.'); err != nil {
				return n, err
			}
		}

		if err := d.w.WriteByte(b); err != nil {
			return n, err
		}
		n++

		// Track line position
		if b == '\n' {
			d.state = dotWriterStateBeginLine
		} else {
			d.state = dotWriterStateMidLine
		}
	}

	return n, nil
}

func (d *dotWriter) Close() error {
	if d.closed {
		return nil
	}
	d.closed = true

	// Ensure we're at the beginning of a line
	if d.state != dotWriterStateBeginLine {
		if _, err := d.w.WriteString("\r\n"); err != nil {
			return err
		}
	}

	// Write terminating dot line
	if _, err := d.w.WriteString(".\r\n"); err != nil {
		return err
	}

	return d.w.Flush()
}
