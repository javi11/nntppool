package nntpcli

import (
	"fmt"
	"io"
	"net"
	"time"
)

const (
	defaultReadBufSize = 32 * 1024
	maxReadBufSize     = 8 * 1024 * 1024
)

// readBuffer is an efficient buffer for reading from network connections.
// It supports windowing, compaction, and automatic growth.
type readBuffer struct {
	buf        []byte
	start, end int
}

func (rb *readBuffer) init() {
	if len(rb.buf) == 0 {
		rb.buf = make([]byte, defaultReadBufSize)
	}
}

// window returns the unread bytes in the buffer.
func (rb *readBuffer) window() []byte {
	return rb.buf[rb.start:rb.end]
}

// advance marks bytes as consumed.
func (rb *readBuffer) advance(consumed int) {
	if consumed <= 0 {
		return
	}
	rb.start += consumed
	if rb.start >= rb.end {
		rb.start, rb.end = 0, 0
	}
}

// compact moves unread bytes to the beginning of the buffer.
func (rb *readBuffer) compact() {
	if rb.start == 0 || rb.start == rb.end {
		return
	}
	copy(rb.buf, rb.buf[rb.start:rb.end])
	rb.end -= rb.start
	rb.start = 0
}

// ensureWriteSpace ensures there's room to write more data.
// It may compact or grow the buffer as needed.
func (rb *readBuffer) ensureWriteSpace() error {
	if rb.end < len(rb.buf) {
		return nil
	}
	if rb.start > 0 {
		rb.compact()
		if rb.end < len(rb.buf) {
			return nil
		}
	}

	// No space and cannot compact: grow.
	cur := len(rb.buf)
	if cur == 0 {
		cur = defaultReadBufSize
	}
	newLen := cur * 2
	if newLen > maxReadBufSize {
		newLen = maxReadBufSize
	}
	if newLen <= len(rb.buf) {
		return fmt.Errorf("nntp read buffer exceeded %d bytes", maxReadBufSize)
	}

	nb := make([]byte, newLen)
	copy(nb, rb.window())
	rb.end = rb.end - rb.start
	rb.start = 0
	rb.buf = nb
	return nil
}

// readMore reads more data from the connection into the buffer.
func (rb *readBuffer) readMore(conn net.Conn, deadline time.Time, hasDeadline bool) (int, error) {
	if hasDeadline {
		_ = conn.SetReadDeadline(deadline)
	} else {
		_ = conn.SetReadDeadline(time.Time{})
	}
	if err := rb.ensureWriteSpace(); err != nil {
		return 0, err
	}
	n, err := conn.Read(rb.buf[rb.end:])
	if n > 0 {
		rb.end += n
	}
	return n, err
}

// streamFeeder is an interface for incremental response parsing.
type streamFeeder interface {
	Feed(in []byte, out io.Writer) (consumed int, done bool, err error)
}

// feedUntilDone reads from the connection and feeds data to the decoder
// until the response is complete.
func (rb *readBuffer) feedUntilDone(conn net.Conn, feeder streamFeeder, out io.Writer, deadline func() (time.Time, bool)) error {
	rb.init()

	for {
		// Ensure we have some bytes to feed.
		if rb.start == rb.end {
			rb.start, rb.end = 0, 0
			dl, ok := deadline()
			if _, err := rb.readMore(conn, dl, ok); err != nil {
				return err
			}
		}

		consumed, done, err := feeder.Feed(rb.window(), out)
		if consumed > 0 {
			rb.advance(consumed)
		}
		if err != nil {
			return err
		}
		if done {
			return nil
		}

		// Need more data.
		// If decoder couldn't consume anything but we have buffered bytes,
		// compact them to the start so the next read appends contiguously.
		if consumed == 0 && (rb.end-rb.start) > 0 {
			rb.compact()
		}

		dl, ok := deadline()
		if _, err := rb.readMore(conn, dl, ok); err != nil {
			return err
		}
	}
}
