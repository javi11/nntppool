package internal

import (
	"fmt"
	"io"
)

type StreamFeeder interface {
	Feed(in []byte, out io.Writer) (consumed int, done bool, err error)
}

type WriterRef struct {
	W        io.Writer
	writeErr error // First write error that triggered discard mode
}

func (wr *WriterRef) Write(p []byte) (int, error) {
	if wr.writeErr != nil {
		// Already in discard mode - continue draining
		return len(p), nil
	}
	n, err := wr.W.Write(p)
	if err != nil {
		// Switch to discard mode, save original error
		wr.writeErr = err
		wr.W = io.Discard
		return len(p), nil // Don't propagate - continue draining
	}
	return n, nil
}

// Err returns the write error that triggered discard mode, or nil.
func (wr *WriterRef) Err() error {
	return wr.writeErr
}

type WriterRefAt struct {
	*WriterRef
}

func (wr *WriterRefAt) WriteAt(p []byte, off int64) (int, error) {
	if wr.writeErr != nil {
		// Already in discard mode - continue draining
		return len(p), nil
	}
	if wr.W == io.Discard {
		return len(p), nil
	}
	if wa, ok := wr.W.(io.WriterAt); ok {
		n, err := wa.WriteAt(p, off)
		if err != nil {
			// Switch to discard mode, save original error
			wr.writeErr = err
			wr.W = io.Discard
			return len(p), nil // Don't propagate - continue draining
		}
		return n, nil
	}
	return 0, fmt.Errorf("underlying writer does not support WriteAt")
}
