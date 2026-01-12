package nntpcli

import (
	"fmt"
	"io"
	"time"

	"github.com/mnightingale/rapidyenc"
)

// PipelineRequest represents a single body request in a pipeline.
type PipelineRequest struct {
	// MessageID is the article message ID to retrieve.
	MessageID string
	// Writer is the destination for the decoded body content.
	Writer io.Writer
	// Discard is the number of bytes to discard from the beginning of the body.
	// Use 0 for full download.
	Discard int64
}

// PipelineResult represents the result of a pipelined body request.
type PipelineResult struct {
	// MessageID is the article message ID that was requested.
	MessageID string
	// BytesWritten is the number of bytes written to the writer.
	BytesWritten int64
	// Error is any error that occurred during the request.
	// nil indicates success.
	Error error
}

// BodyPipelined sends multiple BODY commands in a pipeline and returns results in order.
// This method sends all BODY commands before reading any responses, which can significantly
// improve throughput on high-latency connections.
//
// The method uses the textproto.Conn's native pipelining support via Cmd(), StartResponse(),
// and EndResponse() to properly sequence the requests and responses.
//
// If requests is empty, returns an empty slice.
// If requests has only one element, falls back to sequential BodyDecoded for simplicity.
func (c *connection) BodyPipelined(requests []PipelineRequest) (results []PipelineResult) {
	results = make([]PipelineResult, len(requests))

	// Handle edge cases
	if len(requests) == 0 {
		return results
	}

	// For single request, fall back to sequential for simplicity
	if len(requests) == 1 {
		n, err := c.BodyDecoded(requests[0].MessageID, requests[0].Writer, requests[0].Discard)
		results[0] = PipelineResult{
			MessageID:    requests[0].MessageID,
			BytesWritten: n,
			Error:        err,
		}
		return results
	}

	// Recover from panics to maintain consistent behavior
	defer func() {
		if r := recover(); r != nil {
			// Mark all remaining results as failed
			panicErr := fmt.Errorf("panic in BodyPipelined: %v", r)
			for i := range results {
				if results[i].Error == nil && results[i].BytesWritten == 0 {
					results[i].Error = panicErr
					results[i].MessageID = requests[i].MessageID
				}
			}
		}
	}()

	// Set deadline for entire batch operation (scale timeout by number of requests)
	batchTimeout := c.operationTimeout * time.Duration(len(requests))
	if err := c.setOperationDeadline(batchTimeout); err != nil {
		deadlineErr := fmt.Errorf("set deadline: %w", err)
		for i, req := range requests {
			results[i] = PipelineResult{MessageID: req.MessageID, Error: deadlineErr}
		}
		return results
	}
	defer func() {
		_ = c.clearDeadline()
	}()

	// Phase 1: Send all BODY commands (pipelining - no waiting for responses)
	ids := make([]uint, len(requests))
	sentCount := 0
	for i, req := range requests {
		id, err := c.conn.Cmd("BODY <%s>", req.MessageID)
		if err != nil {
			// Command send failed - mark this and remaining as failed
			cmdErr := fmt.Errorf("BODY <%s>: %w", req.MessageID, formatError(err))
			for j := i; j < len(requests); j++ {
				results[j] = PipelineResult{
					MessageID: requests[j].MessageID,
					Error:     cmdErr,
				}
			}
			break
		}
		ids[i] = id
		sentCount = i + 1
	}

	// Acquire pooled buffer for copy operations (shared across all responses)
	bufPtr := copyBufPool.Get().(*[]byte)
	defer copyBufPool.Put(bufPtr)
	copyBuf := *bufPtr

	// Phase 2: Read all responses in order (for commands that were successfully sent)
	for i := 0; i < sentCount; i++ {
		req := requests[i]
		results[i].MessageID = req.MessageID

		// If this result already has an error (shouldn't happen but defensive), skip
		if results[i].Error != nil {
			continue
		}

		c.conn.StartResponse(ids[i])

		// Read the response code
		_, _, err := c.conn.ReadCodeLine(StatusBodyFollows)
		if err != nil {
			results[i].Error = fmt.Errorf("BODY <%s>: %w", req.MessageID, err)
			c.conn.EndResponse(ids[i])
			continue
		}

		// Decode body with rapidyenc
		// DotReader handles NNTP dot-stuffed format and terminator (.\r\n)
		dec := rapidyenc.AcquireDecoder(c.conn.DotReader())

		// Discard the first n bytes if requested
		if req.Discard > 0 {
			if _, discardErr := io.CopyN(io.Discard, dec, req.Discard); discardErr != nil {
				// Attempt to drain the decoder to avoid connection issues
				_, _ = io.CopyBuffer(io.Discard, dec, copyBuf)
				rapidyenc.ReleaseDecoder(dec)
				c.conn.EndResponse(ids[i])
				results[i].Error = fmt.Errorf("BODY <%s>: discard %d bytes failed: %w", req.MessageID, req.Discard, discardErr)
				continue
			}
		}

		// Copy decoded body to writer
		n, copyErr := io.CopyBuffer(req.Writer, dec, copyBuf)
		if copyErr != nil {
			// Attempt to drain the decoder to avoid connection issues
			_, _ = io.CopyBuffer(io.Discard, dec, copyBuf)
			rapidyenc.ReleaseDecoder(dec)
			c.conn.EndResponse(ids[i])
			results[i].BytesWritten = n
			results[i].Error = fmt.Errorf("BODY <%s>: copy failed: %w", req.MessageID, copyErr)
			continue
		}

		rapidyenc.ReleaseDecoder(dec)
		c.conn.EndResponse(ids[i])

		results[i].BytesWritten = n
	}

	return results
}

// TestPipelineSupport tests if the server supports pipelining by sending multiple
// STAT commands for the same message ID and verifying all responses are received correctly.
//
// The test sends 3 STAT commands in a pipeline (without waiting for responses) and then
// reads all 3 responses. If all responses are received successfully in order, pipelining
// is supported.
//
// The function also measures the round-trip latency and suggests an appropriate pipeline depth:
//   - <50ms latency: suggests 2-4 depth
//   - 50-100ms latency: suggests 4-6 depth
//   - >100ms latency: suggests 6-10 depth
//
// Parameters:
//   - testMsgID: A known valid message ID to use for testing. The message must exist on the server.
//
// Returns:
//   - supported: true if pipelining is working correctly
//   - suggestedDepth: recommended pipeline depth based on latency (0 if not supported)
//   - err: any error that occurred during testing (connection errors, etc.)
func (c *connection) TestPipelineSupport(testMsgID string) (supported bool, suggestedDepth int, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic in TestPipelineSupport: %v", r)
			supported = false
			suggestedDepth = 0
		}
	}()

	// Set deadline for the test operation
	if err := c.setOperationDeadline(c.operationTimeout * 3); err != nil {
		return false, 0, fmt.Errorf("set deadline: %w", err)
	}
	defer func() {
		_ = c.clearDeadline()
	}()

	const testCount = 3

	// Measure start time for latency calculation
	startTime := time.Now()

	// Phase 1: Send all STAT commands (pipelining)
	ids := make([]uint, testCount)
	for i := 0; i < testCount; i++ {
		id, cmdErr := c.conn.Cmd("STAT <%s>", testMsgID)
		if cmdErr != nil {
			return false, 0, fmt.Errorf("send STAT command %d: %w", i+1, cmdErr)
		}
		ids[i] = id
	}

	// Phase 2: Read all responses in order
	for i := 0; i < testCount; i++ {
		c.conn.StartResponse(ids[i])
		_, _, respErr := c.conn.ReadCodeLine(StatusStatSuccess)
		c.conn.EndResponse(ids[i])

		if respErr != nil {
			// If any response fails, pipelining may not be supported
			// or the message ID is invalid
			return false, 0, fmt.Errorf("read STAT response %d: %w", i+1, respErr)
		}
	}

	// Calculate latency and suggest depth
	elapsed := time.Since(startTime)
	avgLatency := elapsed / testCount

	// Suggest pipeline depth based on latency
	switch {
	case avgLatency < 50*time.Millisecond:
		suggestedDepth = 3 // Low latency: 2-4, use middle
	case avgLatency < 100*time.Millisecond:
		suggestedDepth = 5 // Medium latency: 4-6, use middle
	default:
		suggestedDepth = 8 // High latency: 6-10, use middle-high
	}

	return true, suggestedDepth, nil
}
