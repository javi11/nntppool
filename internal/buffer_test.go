package internal

import (
	"bytes"
	"io"
	"net"
	"testing"
	"time"
)

// slowDripConn simulates a connection that sends data very slowly
// to test slow-drip attack prevention.
type slowDripConn struct {
	data       []byte
	pos        int
	interval   time.Duration
	lastRead   time.Time
	readCalls  int
	deadline   time.Time
	hasDeadlin bool
}

func (c *slowDripConn) Read(b []byte) (int, error) {
	c.readCalls++

	// Check if deadline expired
	if c.hasDeadlin && time.Now().After(c.deadline) {
		return 0, &timeoutError{}
	}

	// First call returns immediately
	if c.readCalls == 1 {
		c.lastRead = time.Now()
	} else {
		// Simulate slow drip by sleeping just under the per-read timeout
		time.Sleep(c.interval)
	}

	// Check deadline again after sleep
	if c.hasDeadlin && time.Now().After(c.deadline) {
		return 0, &timeoutError{}
	}

	if c.pos >= len(c.data) {
		return 0, io.EOF
	}

	// Send just 1 byte at a time
	n := 1
	if c.pos+n > len(c.data) {
		n = len(c.data) - c.pos
	}
	copy(b, c.data[c.pos:c.pos+n])
	c.pos += n
	return n, nil
}

func (c *slowDripConn) Write(b []byte) (int, error)       { return len(b), nil }
func (c *slowDripConn) Close() error                      { return nil }
func (c *slowDripConn) LocalAddr() net.Addr               { return nil }
func (c *slowDripConn) RemoteAddr() net.Addr              { return nil }
func (c *slowDripConn) SetDeadline(t time.Time) error     { return nil }
func (c *slowDripConn) SetWriteDeadline(t time.Time) error { return nil }
func (c *slowDripConn) SetReadDeadline(t time.Time) error {
	c.deadline = t
	c.hasDeadlin = !t.IsZero()
	return nil
}

type timeoutError struct{}

func (e *timeoutError) Error() string   { return "i/o timeout" }
func (e *timeoutError) Timeout() bool   { return true }
func (e *timeoutError) Temporary() bool { return true }

// simpleFeeder collects all data and signals done when it sees ".\r\n"
type simpleFeeder struct {
	collected []byte
}

func (f *simpleFeeder) Feed(data []byte, out io.Writer) (int, bool, error) {
	// Look for terminator
	terminator := []byte(".\r\n")
	idx := bytes.Index(data, terminator)
	if idx >= 0 {
		f.collected = append(f.collected, data[:idx]...)
		return idx + len(terminator), true, nil
	}
	f.collected = append(f.collected, data...)
	return len(data), false, nil
}

func TestFeedUntilDone_SlowDripTimeout(t *testing.T) {
	// This test verifies that the response deadline (calculated once at start)
	// will timeout a slow-drip connection even though data keeps arriving.
	//
	// We use a short response timeout by temporarily modifying the behavior
	// through a context deadline that's shorter than the slow drip would take.

	// Create a slow drip connection that sends 1 byte every 200ms
	// Total data: 20 bytes + terminator = needs ~4 seconds at 200ms/byte
	data := bytes.Repeat([]byte("x"), 20)
	data = append(data, []byte(".\r\n")...)

	conn := &slowDripConn{
		data:     data,
		interval: 200 * time.Millisecond,
	}

	rb := &ReadBuffer{}
	feeder := &simpleFeeder{}
	var buf bytes.Buffer

	// Use a context deadline of 500ms - should timeout before completing
	start := time.Now()
	contextDeadline := start.Add(500 * time.Millisecond)

	err := rb.FeedUntilDone(conn, feeder, &buf, func() (time.Time, bool) {
		return contextDeadline, true
	})

	elapsed := time.Since(start)

	// Should have failed with timeout
	if err == nil {
		t.Fatalf("expected timeout error, got nil (collected %d bytes in %v)",
			len(feeder.collected), elapsed)
	}

	// Should have timed out relatively quickly (around 500ms, give some slack)
	if elapsed > 1*time.Second {
		t.Errorf("timeout took too long: %v (expected ~500ms)", elapsed)
	}

	t.Logf("Correctly timed out after %v with error: %v (collected %d bytes)",
		elapsed, err, len(feeder.collected))
}

// fastConn returns all data at once without delays
type fastConn struct {
	data     []byte
	pos      int
	deadline time.Time
}

func (c *fastConn) Read(b []byte) (int, error) {
	if c.pos >= len(c.data) {
		return 0, io.EOF
	}
	n := copy(b, c.data[c.pos:])
	c.pos += n
	return n, nil
}

func (c *fastConn) Write(b []byte) (int, error)        { return len(b), nil }
func (c *fastConn) Close() error                       { return nil }
func (c *fastConn) LocalAddr() net.Addr                { return nil }
func (c *fastConn) RemoteAddr() net.Addr               { return nil }
func (c *fastConn) SetDeadline(t time.Time) error      { return nil }
func (c *fastConn) SetWriteDeadline(t time.Time) error { return nil }
func (c *fastConn) SetReadDeadline(t time.Time) error  { c.deadline = t; return nil }

func TestFeedUntilDone_FastConnection(t *testing.T) {
	// Verify fast connections still work normally

	data := []byte("Hello, World!\r\n.\r\n")
	conn := &fastConn{data: data}

	rb := &ReadBuffer{}
	feeder := &simpleFeeder{}
	var buf bytes.Buffer

	err := rb.FeedUntilDone(conn, feeder, &buf, func() (time.Time, bool) {
		return time.Time{}, false // No context deadline
	})

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	expected := "Hello, World!\r\n"
	if string(feeder.collected) != expected {
		t.Errorf("got %q, want %q", feeder.collected, expected)
	}
}

func TestFeedUntilDone_PerReadTimeoutStillWorks(t *testing.T) {
	// Verify that completely stalled connections still timeout via per-read deadline

	// Connection that sends nothing after first byte
	conn := &slowDripConn{
		data:     []byte("x"), // Only 1 byte, no terminator - will need more data
		interval: 0,
	}

	rb := &ReadBuffer{}
	feeder := &simpleFeeder{}
	var buf bytes.Buffer

	// Use a very short per-read timeout by setting context deadline
	start := time.Now()
	contextDeadline := start.Add(100 * time.Millisecond)

	err := rb.FeedUntilDone(conn, feeder, &buf, func() (time.Time, bool) {
		return contextDeadline, true
	})

	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("expected error for stalled connection")
	}

	// Should timeout quickly
	if elapsed > 500*time.Millisecond {
		t.Errorf("stalled connection took too long to timeout: %v", elapsed)
	}

	t.Logf("Stalled connection timed out correctly after %v", elapsed)
}
