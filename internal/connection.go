package internal

import (
	"crypto/tls"
	"net"
	"sync/atomic"
	"time"
)

// flushThreshold controls how often accumulated bytes are flushed to atomic counters.
// 64KB provides a good balance between accuracy and syscall reduction.
const flushThreshold uint64 = 64 * 1024

type MeteredConn struct {
	net.Conn
	BytesRead    *uint64
	BytesWritten *uint64
	LastActivity *int64

	// Local accumulators to batch atomic operations (use atomics for thread safety)
	localRead  uint64
	localWrite uint64
}

func (m *MeteredConn) Read(b []byte) (n int, err error) {
	n, err = m.Conn.Read(b)
	if n > 0 {
		newVal := atomic.AddUint64(&m.localRead, uint64(n))
		// Batch flush to atomic counter
		if newVal >= flushThreshold {
			// Swap to zero and add the swapped value to global counter
			toFlush := atomic.SwapUint64(&m.localRead, 0)
			if toFlush > 0 && m.BytesRead != nil {
				atomic.AddUint64(m.BytesRead, toFlush)
			}
			if m.LastActivity != nil {
				atomic.StoreInt64(m.LastActivity, time.Now().Unix())
			}
		}
	}
	return
}

func (m *MeteredConn) Write(b []byte) (n int, err error) {
	n, err = m.Conn.Write(b)
	if n > 0 {
		newVal := atomic.AddUint64(&m.localWrite, uint64(n))
		// Batch flush to atomic counter
		if newVal >= flushThreshold {
			// Swap to zero and add the swapped value to global counter
			toFlush := atomic.SwapUint64(&m.localWrite, 0)
			if toFlush > 0 && m.BytesWritten != nil {
				atomic.AddUint64(m.BytesWritten, toFlush)
			}
			if m.LastActivity != nil {
				atomic.StoreInt64(m.LastActivity, time.Now().Unix())
			}
		}
	}
	return
}

// Flush writes any accumulated byte counts to the atomic counters.
// Should be called before closing the connection.
func (m *MeteredConn) Flush() {
	if toFlush := atomic.SwapUint64(&m.localRead, 0); toFlush > 0 && m.BytesRead != nil {
		atomic.AddUint64(m.BytesRead, toFlush)
	}
	if toFlush := atomic.SwapUint64(&m.localWrite, 0); toFlush > 0 && m.BytesWritten != nil {
		atomic.AddUint64(m.BytesWritten, toFlush)
	}
}

// ApplyConnOptimizations applies TCP buffer optimizations and optional TLS wrapping to a connection.
// This is used for both direct and proxy connections.
func ApplyConnOptimizations(conn net.Conn, tlsConfig *tls.Config) (net.Conn, error) {
	// Optimize socket buffers for high-speed downloads (10Gbps+)
	if tcpConn, ok := conn.(*net.TCPConn); ok {
		// 8MB receive buffer
		_ = tcpConn.SetReadBuffer(8 * 1024 * 1024)
		// 1MB send buffer
		_ = tcpConn.SetWriteBuffer(1024 * 1024)
	}

	if tlsConfig != nil {
		return tls.Client(conn, tlsConfig), nil
	}
	return conn, nil
}

func NewNetConn(addr string, tlsConfig *tls.Config) (net.Conn, error) {
	dialer := &net.Dialer{
		Timeout: 30 * time.Second,
	}

	conn, err := dialer.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}

	return ApplyConnOptimizations(conn, tlsConfig)
}

func SafeClose[T any](ch chan T) {
	defer func() { _ = recover() }()
	close(ch)
}
