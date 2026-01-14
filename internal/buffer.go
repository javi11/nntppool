package internal

import (
	"fmt"
	"io"
	"net"
	"time"
)

const (
	DefaultReadBufSize = 32 * 1024
	MaxReadBufSize     = 8 * 1024 * 1024
)

type ReadBuffer struct {
	buf        []byte
	start, end int
}

func (rb *ReadBuffer) Init() {
	if len(rb.buf) == 0 {
		rb.buf = make([]byte, DefaultReadBufSize)
	}
}

func (rb *ReadBuffer) Window() []byte {
	return rb.buf[rb.start:rb.end]
}

func (rb *ReadBuffer) Advance(consumed int) {
	if consumed <= 0 {
		return
	}
	rb.start += consumed
	if rb.start >= rb.end {
		rb.start, rb.end = 0, 0
	}
}

func (rb *ReadBuffer) Compact() {
	if rb.start == 0 || rb.start == rb.end {
		return
	}
	copy(rb.buf, rb.buf[rb.start:rb.end])
	rb.end -= rb.start
	rb.start = 0
}

func (rb *ReadBuffer) ensureWriteSpace() error {
	if rb.end < len(rb.buf) {
		return nil
	}
	if rb.start > 0 {
		rb.Compact()
		if rb.end < len(rb.buf) {
			return nil
		}
	}

	// No space and cannot compact: grow.
	cur := len(rb.buf)
	if cur == 0 {
		cur = DefaultReadBufSize
	}
	newLen := cur * 2
	if newLen > MaxReadBufSize {
		newLen = MaxReadBufSize
	}
	if newLen <= len(rb.buf) {
		return fmt.Errorf("nntp read buffer exceeded %d bytes", MaxReadBufSize)
	}

	nb := make([]byte, newLen)
	copy(nb, rb.Window())
	rb.end = rb.end - rb.start
	rb.start = 0
	rb.buf = nb
	return nil
}

func (rb *ReadBuffer) readMore(conn net.Conn, deadline time.Time, hasDeadline bool) (int, error) {
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

func (rb *ReadBuffer) FeedUntilDone(conn net.Conn, feeder StreamFeeder, out io.Writer, deadline func() (time.Time, bool)) error {
	rb.Init()

	for {
		// Ensure we have some bytes to feed.
		if rb.start == rb.end {
			rb.start, rb.end = 0, 0
			dl, ok := deadline()
			if _, err := rb.readMore(conn, dl, ok); err != nil {
				return err
			}
		}

		consumed, done, err := feeder.Feed(rb.Window(), out)
		if consumed > 0 {
			rb.Advance(consumed)
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
			rb.Compact()
		}

		dl, ok := deadline()
		if _, err := rb.readMore(conn, dl, ok); err != nil {
			return err
		}
	}
}
