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
	// Prevents indefinite hangs on stalled connections (e.g., after laptop sleep).
	DefaultReadTimeout = 60 * time.Second

	// DefaultResponseTimeout is the maximum time for a complete response.
	// Prevents slow-drip attacks where server sends minimal data to stay alive.
	// This deadline is calculated once at the start and does not reset on data received.
	DefaultResponseTimeout = 30 * time.Second
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
	// Always set deadline to ensure timeout is enforced.
	// Previous caching optimization could leave stale deadlines when the same
	// deadline value was reused across iterations, causing reads to hang indefinitely.
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

	// Calculate response deadline ONCE at start (prevents slow-drip resets).
	// This ensures a maximum total time for the response, regardless of how
	// much data trickles in.
	responseDeadline := time.Now().Add(DefaultResponseTimeout)

	// Helper to get the earliest applicable deadline
	getDeadline := func() (time.Time, bool) {
		// Per-read deadline (detects completely stalled connections)
		perReadDeadline := time.Now().Add(DefaultReadTimeout)

		// Context deadline from caller
		contextDeadline, hasContext := deadline()

		// Use the earliest applicable deadline
		earliest := responseDeadline
		if perReadDeadline.Before(earliest) {
			earliest = perReadDeadline
		}
		if hasContext && contextDeadline.Before(earliest) {
			earliest = contextDeadline
		}

		return earliest, true
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
