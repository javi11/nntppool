package internal

import (
	"fmt"
	"io"
	"net"
	"time"
)

const (
	// DefaultReadBufSize is the initial read buffer size.
	// 1MB matches TCP receive buffer better for high-throughput connections.
	DefaultReadBufSize = 1 * 1024 * 1024
	MaxReadBufSize     = 8 * 1024 * 1024

	// DefaultReadTimeout is used when no context deadline is set.
	// Prevents indefinite hangs on stalled connections.
	DefaultReadTimeout = 5 * time.Minute
)

type ReadBuffer struct {
	buf        []byte
	start, end int

	// Deadline caching to avoid redundant SetReadDeadline syscalls
	lastDeadline time.Time
	deadlineSet  bool
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
	// Only call SetReadDeadline if the deadline actually changed
	if hasDeadline {
		if !rb.deadlineSet || !deadline.Equal(rb.lastDeadline) {
			_ = conn.SetReadDeadline(deadline)
			rb.lastDeadline = deadline
			rb.deadlineSet = true
		}
	} else if rb.deadlineSet {
		_ = conn.SetReadDeadline(time.Time{})
		rb.deadlineSet = false
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

	// Helper to get deadline, using default timeout if none provided
	getDeadline := func() (time.Time, bool) {
		dl, ok := deadline()
		if ok {
			return dl, true
		}
		// No context deadline - use default timeout to prevent indefinite hangs
		return time.Now().Add(DefaultReadTimeout), true
	}

	for {
		// Ensure we have some bytes to feed.
		if rb.start == rb.end {
			rb.start, rb.end = 0, 0
			dl, ok := getDeadline()
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
		// compact only when write space is critically low (<10% remaining).
		// This reduces unnecessary memmove operations.
		if consumed == 0 && (rb.end-rb.start) > 0 {
			writeSpace := len(rb.buf) - rb.end
			if writeSpace < len(rb.buf)/10 {
				rb.Compact()
			}
		}

		dl, ok := getDeadline()
		if _, err := rb.readMore(conn, dl, ok); err != nil {
			return err
		}
	}
}
