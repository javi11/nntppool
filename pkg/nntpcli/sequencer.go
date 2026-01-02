package nntpcli

import (
	"sync"
)

// pipeline manages pipelined in-order request/response sequences.
// This replaces textproto.Pipeline to remove the dependency on net/textproto.
//
// A pipeline allows multiple goroutines to send requests concurrently while
// ensuring that responses are read in the same order requests were sent.
type pipeline struct {
	mu       sync.Mutex
	id       uint
	request  sequencer
	response sequencer
}

// newPipeline creates a new pipeline for request/response sequencing.
func newPipeline() *pipeline {
	return &pipeline{}
}

// Next returns the next sequence ID for a request.
// This should be called before StartRequest.
func (p *pipeline) Next() uint {
	p.mu.Lock()
	id := p.id
	p.id++
	p.mu.Unlock()
	return id
}

// StartRequest blocks until it is time to send request id.
// Requests must be sent in sequential order.
func (p *pipeline) StartRequest(id uint) {
	p.request.Start(id)
}

// EndRequest notifies that request id has been sent.
func (p *pipeline) EndRequest(id uint) {
	p.request.End(id)
}

// StartResponse blocks until it is time to read the response for request id.
// Responses must be read in the same order as requests were sent.
func (p *pipeline) StartResponse(id uint) {
	p.response.Start(id)
}

// EndResponse notifies that the response for request id has been read.
func (p *pipeline) EndResponse(id uint) {
	p.response.End(id)
}

// sequencer coordinates ordered execution of pipeline stages.
// It ensures that operations happen in sequential order by ID.
type sequencer struct {
	mu   sync.Mutex
	id   uint
	wait map[uint]chan struct{}
}

// Start blocks until it is this ID's turn to execute.
func (s *sequencer) Start(id uint) {
	s.mu.Lock()
	if s.id == id {
		s.mu.Unlock()
		return
	}
	if s.wait == nil {
		s.wait = make(map[uint]chan struct{})
	}
	c := make(chan struct{})
	s.wait[id] = c
	s.mu.Unlock()
	<-c
}

// End signals that this ID has finished, allowing the next ID to proceed.
func (s *sequencer) End(id uint) {
	s.mu.Lock()
	if s.id != id {
		s.mu.Unlock()
		panic("sequencer: out of order End")
	}
	s.id++
	if c, ok := s.wait[s.id]; ok {
		delete(s.wait, s.id)
		close(c)
	}
	s.mu.Unlock()
}
