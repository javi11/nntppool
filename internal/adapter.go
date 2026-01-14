package internal

import (
	"fmt"
	"io"
)

type StreamFeeder interface {
	Feed(in []byte, out io.Writer) (consumed int, done bool, err error)
}

type WriterRef struct {
	W io.Writer
}

func (wr *WriterRef) Write(p []byte) (int, error) {
	return wr.W.Write(p)
}

type WriterRefAt struct {
	*WriterRef
}

func (wr *WriterRefAt) WriteAt(p []byte, off int64) (int, error) {
	if wr.W == io.Discard {
		return len(p), nil
	}
	if wa, ok := wr.W.(io.WriterAt); ok {
		return wa.WriteAt(p, off)
	}
	return 0, fmt.Errorf("underlying writer does not support WriteAt")
}
