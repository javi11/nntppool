package test

import (
	"context"
	"log"
	"net"

	nntpserver "github.com/javi11/nntp-server-mock/nntpserver"
)

type Server struct {
	l       net.Listener
	s       *nntpserver.Server
	backend *nntpserver.DiskBackend
}

func NewServer() (*Server, error) {
	a, err := net.ResolveTCPAddr("tcp", "localhost:0")
	if err != nil {
		return nil, err
	}

	l, err := net.ListenTCP("tcp", a)
	if err != nil {
		return nil, err
	}

	backend := nntpserver.NewDiskBackend(
		true,
		"",
	)
	s := nntpserver.NewServer(backend)

	return &Server{
		l:       l,
		s:       s,
		backend: backend,
	}, nil
}

func (s Server) Serve(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
			c, err := s.l.Accept()

			if ctx.Err() != nil {
				return
			}

			maybefatal(err, "Error accepting connection: %v", err)

			go s.s.Process(c)
		}
	}
}

func (s Server) Port() int {
	return s.l.Addr().(*net.TCPAddr).Port
}

func (s Server) Close() {
	_ = s.l.Close()
	_ = s.backend.Close()
}

func maybefatal(err error, f string, a ...interface{}) {
	if err != nil {
		log.Fatalf(f, a...)
	}
}
