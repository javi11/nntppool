// Package netconn provides a wrapper to bridge netpool's net.Conn management
// with nntppool's nntpcli.Connection requirements.
package netconn

import (
	"net"
	"time"

	"github.com/javi11/nntppool/v3/pkg/nntpcli"
)

// NNTPConnWrapper wraps a net.Conn and its associated nntpcli.Connection.
// It implements net.Conn to be managed by netpool, while providing
// access to the NNTP connection for protocol operations.
type NNTPConnWrapper struct {
	netConn  net.Conn
	nntpConn nntpcli.Connection
}

// NewNNTPConnWrapper creates a wrapper from the underlying connections.
func NewNNTPConnWrapper(netConn net.Conn, nntpConn nntpcli.Connection) *NNTPConnWrapper {
	return &NNTPConnWrapper{
		netConn:  netConn,
		nntpConn: nntpConn,
	}
}

// NNTPConnection returns the NNTP protocol connection.
func (w *NNTPConnWrapper) NNTPConnection() nntpcli.Connection {
	return w.nntpConn
}

// Implement net.Conn interface by delegating to netConn

func (w *NNTPConnWrapper) Read(b []byte) (n int, err error) {
	return w.netConn.Read(b)
}

func (w *NNTPConnWrapper) Write(b []byte) (n int, err error) {
	return w.netConn.Write(b)
}

// Close closes the NNTP connection (which sends QUIT command and closes underlying conn).
func (w *NNTPConnWrapper) Close() error {
	return w.nntpConn.Close()
}

func (w *NNTPConnWrapper) LocalAddr() net.Addr {
	return w.netConn.LocalAddr()
}

func (w *NNTPConnWrapper) RemoteAddr() net.Addr {
	return w.netConn.RemoteAddr()
}

func (w *NNTPConnWrapper) SetDeadline(t time.Time) error {
	return w.netConn.SetDeadline(t)
}

func (w *NNTPConnWrapper) SetReadDeadline(t time.Time) error {
	return w.netConn.SetReadDeadline(t)
}

func (w *NNTPConnWrapper) SetWriteDeadline(t time.Time) error {
	return w.netConn.SetWriteDeadline(t)
}

// Compile-time interface check
var _ net.Conn = (*NNTPConnWrapper)(nil)
