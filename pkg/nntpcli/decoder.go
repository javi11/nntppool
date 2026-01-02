package nntpcli

import (
	"bufio"
	"errors"
	"io"
	"log"
	"os"
	"strconv"
	"strings"

	"github.com/mnightingale/rapidyenc"
)

// debugEnabled controls debug logging. Set NNTPCLI_DEBUG=1 to enable.
var debugEnabled = os.Getenv("NNTPCLI_DEBUG") == "1"

// debugLog logs a message if debug mode is enabled.
func debugLog(format string, args ...any) {
	if debugEnabled {
		log.Printf("[NNTPCLI DEBUG] "+format, args...)
	}
}

const (
	// decoderBufferSize is the buffer size for incremental decoding.
	decoderBufferSize = 32 * 1024
	// maxDecodeIterations prevents infinite loops on malformed data.
	maxDecodeIterations = 10000
)

// ErrDecoderMaxIterations is returned when the decoder exceeds max iterations.
var ErrDecoderMaxIterations = errors.New("decoder: max iterations exceeded, possible malformed data")

// incrementalDecoder wraps rapidyenc.DecodeIncremental to provide an io.Reader interface.
// It parses yenc headers (=ybegin, =ypart) to extract file size, then uses
// DecodeIncremental for the actual encoded body data.
type incrementalDecoder struct {
	r            *bufio.Reader
	state        rapidyenc.State
	buf          []byte // read buffer for body data
	bufStart     int    // start of unconsumed data in buf
	bufEnd       int    // end of data in buf
	decoded      []byte // decoded data not yet consumed by caller
	headersRead  bool   // true after headers have been parsed
	eof          bool   // true when end of yenc data reached
	readErr      error  // stored read error
	expectedSize int64  // expected decoded size from =ybegin size=
	decodedTotal int64  // total bytes decoded so far
}

// newIncrementalDecoder creates a new incremental yenc decoder.
func newIncrementalDecoder(r io.Reader) *incrementalDecoder {
	return &incrementalDecoder{
		r:   bufio.NewReaderSize(r, decoderBufferSize),
		buf: make([]byte, decoderBufferSize),
	}
}

// parseHeaders reads and parses the yenc header lines (=ybegin, =ypart).
// Extracts the expected file size from =ybegin size=N.
// After this, the reader is positioned at the start of the encoded body.
func (d *incrementalDecoder) parseHeaders() error {
	for {
		line, err := d.r.ReadString('\n')
		if err != nil {
			return err
		}

		trimmed := strings.TrimSpace(line)

		if strings.HasPrefix(trimmed, "=ybegin") {
			// Extract size= parameter from =ybegin line
			d.expectedSize = parseYencParam(trimmed, "size")
			debugLog("decoder.parseHeaders: =ybegin found, expectedSize=%d", d.expectedSize)
			continue
		}

		if strings.HasPrefix(trimmed, "=ypart") {
			// Multi-part article - could extract begin/end for partial downloads
			debugLog("decoder.parseHeaders: =ypart found")
			continue
		}

		// This line is not a header - it's the start of the body
		// Put this data back into our buffer for decoding
		d.bufEnd = copy(d.buf, []byte(line))
		debugLog("decoder.parseHeaders: first body line buffered, bufEnd=%d", d.bufEnd)
		break
	}

	d.headersRead = true
	return nil
}

// parseYencParam extracts a named parameter value from a yenc header line.
// For example, parseYencParam("=ybegin line=128 size=12345 name=file.txt", "size") returns 12345.
func parseYencParam(line, param string) int64 {
	// Look for "param=" in the line
	prefix := param + "="
	idx := strings.Index(line, prefix)
	if idx == -1 {
		return 0
	}

	// Extract value starting after "param="
	start := idx + len(prefix)
	end := start

	// Find end of value (space or end of string)
	for end < len(line) && line[end] != ' ' && line[end] != '\t' && line[end] != '\r' && line[end] != '\n' {
		end++
	}

	valueStr := line[start:end]
	value, err := strconv.ParseInt(valueStr, 10, 64)
	if err != nil {
		return 0
	}

	return value
}

// Read implements io.Reader. It reads yenc-encoded data from the underlying
// reader and returns decoded data.
func (d *incrementalDecoder) Read(p []byte) (int, error) {
	// Parse headers on first read
	if !d.headersRead {
		debugLog("decoder.Read: parsing headers")
		if err := d.parseHeaders(); err != nil {
			debugLog("decoder.Read: parseHeaders error: %v", err)
			return 0, err
		}
	}

	// If we have leftover decoded data, return that first
	if len(d.decoded) > 0 {
		n := copy(p, d.decoded)
		d.decoded = d.decoded[n:]
		debugLog("decoder.Read: returned %d bytes from leftover (remaining=%d)", n, len(d.decoded))
		return n, nil
	}

	// If we've reached EOF, return EOF
	if d.eof {
		debugLog("decoder.Read: already at EOF")
		return 0, io.EOF
	}

	// Check if we've decoded all expected data
	if d.expectedSize > 0 && d.decodedTotal >= d.expectedSize {
		debugLog("decoder.Read: reached expectedSize (%d >= %d)", d.decodedTotal, d.expectedSize)
		d.eof = true
		return 0, io.EOF
	}

	// Use bounded iteration instead of recursion to prevent infinite loops
	for iteration := 0; iteration < maxDecodeIterations; iteration++ {
		// Try to fill the buffer if we have space
		if d.bufEnd < len(d.buf) && d.readErr == nil {
			n, err := d.r.Read(d.buf[d.bufEnd:])
			debugLog("decoder.Read: iter=%d underlying read: n=%d err=%v bufEnd=%d->%d", iteration, n, err, d.bufEnd, d.bufEnd+n)
			d.bufEnd += n
			if err != nil {
				d.readErr = err
			}
		}

		// If we have no data to process, return
		if d.bufStart >= d.bufEnd {
			debugLog("decoder.Read: iter=%d no data to process, bufStart=%d bufEnd=%d readErr=%v", iteration, d.bufStart, d.bufEnd, d.readErr)
			if d.readErr != nil {
				return 0, d.readErr
			}
			return 0, io.EOF
		}

		// Get the data to decode - use in-place decoding (same buffer for input/output)
		input := d.buf[d.bufStart:d.bufEnd]

		// Decode in-place
		nDst, nSrc, end, decErr := rapidyenc.DecodeIncremental(input, input, &d.state)
		debugLog("decoder.Read: iter=%d decode: nDst=%d nSrc=%d end=%v decErr=%v inputLen=%d decodedTotal=%d", iteration, nDst, nSrc, end, decErr, len(input), d.decodedTotal)
		if decErr != nil {
			debugLog("decoder.Read: iter=%d decode error: %v", iteration, decErr)
			return 0, decErr
		}

		// Advance the buffer start by consumed bytes
		d.bufStart += nSrc

		// If we've consumed all data, reset the buffer positions
		if d.bufStart >= d.bufEnd {
			debugLog("decoder.Read: iter=%d buffer reset (all consumed)", iteration)
			d.bufStart = 0
			d.bufEnd = 0
		} else if d.bufStart > len(d.buf)/2 {
			// Compact the buffer if we've consumed more than half
			debugLog("decoder.Read: iter=%d buffer compact bufStart=%d bufEnd=%d", iteration, d.bufStart, d.bufEnd)
			copy(d.buf, d.buf[d.bufStart:d.bufEnd])
			d.bufEnd -= d.bufStart
			d.bufStart = 0
		}

		// Check if we reached the end marker
		if end != rapidyenc.EndNone {
			debugLog("decoder.Read: iter=%d reached yenc end marker (end=%v)", iteration, end)
			d.eof = true
		}

		// Return decoded data if we have any
		if nDst > 0 {
			// Track total decoded bytes
			d.decodedTotal += int64(nDst)

			// Copy to caller's buffer
			copied := copy(p, input[:nDst])
			if copied < nDst {
				// Save the rest for next Read call
				d.decoded = make([]byte, nDst-copied)
				copy(d.decoded, input[copied:nDst])
				debugLog("decoder.Read: iter=%d returning %d bytes, saved %d for later, decodedTotal=%d", iteration, copied, nDst-copied, d.decodedTotal)
			} else {
				debugLog("decoder.Read: iter=%d returning %d bytes, decodedTotal=%d", iteration, copied, d.decodedTotal)
			}

			// Check if we've reached the expected size
			if d.expectedSize > 0 && d.decodedTotal >= d.expectedSize {
				debugLog("decoder.Read: reached expectedSize after this read (%d >= %d)", d.decodedTotal, d.expectedSize)
				d.eof = true
			}

			return copied, nil
		}

		// Check for termination conditions
		if d.eof {
			debugLog("decoder.Read: iter=%d EOF after decode", iteration)
			return 0, io.EOF
		}

		if d.readErr != nil {
			debugLog("decoder.Read: iter=%d readErr after decode: %v", iteration, d.readErr)
			return 0, d.readErr
		}

		// Zero progress check: if no bytes consumed and no bytes produced,
		// we're stuck on malformed data - return error instead of looping forever
		if nSrc == 0 && nDst == 0 {
			debugLog("decoder.Read: iter=%d ZERO PROGRESS - nSrc=0 nDst=0, bufStart=%d bufEnd=%d", iteration, d.bufStart, d.bufEnd)
			return 0, io.ErrUnexpectedEOF
		}

		debugLog("decoder.Read: iter=%d continuing loop (nDst=0 but nSrc=%d)", iteration, nSrc)
		// Continue loop to process more data
	}

	// Max iterations exceeded - likely malformed data causing infinite loop
	debugLog("decoder.Read: MAX ITERATIONS EXCEEDED (%d)", maxDecodeIterations)
	return 0, ErrDecoderMaxIterations
}
