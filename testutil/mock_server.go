package testutil

import (
	"context"
	"net"
	"sync/atomic"
	"testing"
)

// Handler is a function that processes NNTP commands and returns responses.
// If it returns an error, the connection will be closed.
type Handler func(cmd string) (response string, err error)

// MockServerConfig configures a mock NNTP server.
type MockServerConfig struct {
	// ID is a label for the server (used in greeting if provided)
	ID string
	// Greeting is the initial greeting message. Defaults to "200 Service Ready\r\n"
	Greeting string
	// Handler processes commands
	Handler Handler
	// TrackConnections enables connection counting if true
	TrackConnections bool
}

// MockServer tracks a running mock NNTP server.
type MockServer struct {
	addr            string
	listener        net.Listener
	connectionCount int32
}

// Addr returns the server address.
func (m *MockServer) Addr() string {
	return m.addr
}

// ConnectionCount returns the number of connections accepted.
func (m *MockServer) ConnectionCount() int {
	return int(atomic.LoadInt32(&m.connectionCount))
}

// Close shuts down the server.
func (m *MockServer) Close() {
	if m.listener != nil {
		_ = m.listener.Close()
	}
}

// StartMockNNTPServer starts a real TCP mock NNTP server.
// It returns the server instance and a cleanup function.
func StartMockNNTPServer(t *testing.T, config MockServerConfig) (*MockServer, func()) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}

	greeting := config.Greeting
	if greeting == "" {
		greeting = "200 Service Ready"
		if config.ID != "" {
			greeting += " - " + config.ID
		}
		greeting += "\r\n"
	}

	srv := &MockServer{
		addr:     l.Addr().String(),
		listener: l,
	}

	go func() {
		for {
			conn, err := l.Accept()
			if err != nil {
				return
			}

			if config.TrackConnections {
				atomic.AddInt32(&srv.connectionCount, 1)
			}

			go func(c net.Conn) {
				defer func() {
					_ = c.Close()
				}()

				// Send greeting
				if _, err := c.Write([]byte(greeting)); err != nil {
					return
				}

				// Handle commands
				buf := make([]byte, 1024)
				for {
					n, err := c.Read(buf)
					if err != nil {
						return
					}

					cmd := string(buf[:n])
					response, handlerErr := config.Handler(cmd)

					// Write response first (even if partial) before checking error
					if response != "" {
						if _, err := c.Write([]byte(response)); err != nil {
							return
						}
					}

					// Now handle the error (close connection to simulate disconnect)
					if handlerErr != nil {
						return
					}
				}
			}(conn)
		}
	}()

	cleanup := func() {
		srv.Close()
	}

	return srv, cleanup
}

// MockDialerWithHandler creates a connection factory that uses net.Pipe for lightweight in-memory connections.
// This is useful for tests that don't need real TCP sockets.
// Returns a function compatible with nntppool.ConnFactory.
func MockDialerWithHandler(config MockServerConfig) func(ctx context.Context) (net.Conn, error) {
	greeting := config.Greeting
	if greeting == "" {
		greeting = "200 Service Ready"
		if config.ID != "" {
			greeting += " - " + config.ID
		}
		greeting += "\r\n"
	}

	return func(ctx context.Context) (net.Conn, error) {
		c1, c2 := net.Pipe()

		go func() {
			defer func() {
				_ = c2.Close()
			}()

			// Send initial greeting
			if _, err := c2.Write([]byte(greeting)); err != nil {
				return
			}

			buf := make([]byte, 1024)
			for {
				n, err := c2.Read(buf)
				if err != nil {
					return
				}

				cmd := string(buf[:n])
				response, handlerErr := config.Handler(cmd)

				// Write response first (even if partial) before checking error
				if response != "" {
					if _, err := c2.Write([]byte(response)); err != nil {
						return
					}
				}

				// Now handle the error (close connection to simulate disconnect)
				if handlerErr != nil {
					return
				}
			}
		}()

		return c1, nil
	}
}
