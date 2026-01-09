package nntpcli

import (
	"io"
	"strconv"
)

// nntpReader wraps a readBuffer to provide NNTP-specific reading operations.
type nntpReader struct {
	buf *readBuffer
}

// newNNTPReader creates a new nntpReader wrapping the given io.Reader.
func newNNTPReader(r io.Reader) *nntpReader {
	return &nntpReader{
		buf: newReadBuffer(r),
	}
}

// ReadLine reads a single line from the connection, stripping the trailing \r\n.
func (r *nntpReader) ReadLine() (string, error) {
	return r.buf.ReadLine()
}

// ReadCodeLine reads a response line from the server and parses the 3-digit status code.
// If expectCode > 0 and the actual code doesn't match, returns an *NNTPError.
func (r *nntpReader) ReadCodeLine(expectCode int) (code int, message string, err error) {
	line, err := r.ReadLine()
	if err != nil {
		return 0, "", err
	}
	return parseCodeLine(line, expectCode)
}

// parseCodeLine parses an NNTP response line into code and message.
// Returns an *NNTPError if expectCode > 0 and the code doesn't match.
func parseCodeLine(line string, expectCode int) (code int, message string, err error) {
	if len(line) < 3 {
		return 0, "", &NNTPError{Code: 0, Msg: "short response: " + line}
	}

	code, err = strconv.Atoi(line[:3])
	if err != nil {
		return 0, "", &NNTPError{Code: 0, Msg: "invalid response code: " + line}
	}

	message = ""
	if len(line) > 4 {
		message = line[4:]
	} else if len(line) > 3 {
		message = line[3:]
	}

	if expectCode > 0 && code != expectCode {
		return code, message, &NNTPError{Code: code, Msg: message}
	}

	return code, message, nil
}

// DotReader returns an io.Reader that reads a dot-terminated multi-line response.
// It handles dot-unstuffing (lines starting with ".." become ".") and
// terminates when it encounters a line containing only ".".
func (r *nntpReader) DotReader() io.Reader {
	return &dotReader{
		r:     r,
		state: dotReaderStateBeginLine,
	}
}

// Reader returns the underlying readBuffer as an io.Reader.
// This is used for rapidyenc.NewDecoder which needs direct access to the stream.
func (r *nntpReader) Reader() io.Reader {
	return r.buf
}

// dotReaderState tracks the state machine for dot-terminated reading.
type dotReaderState int

const (
	dotReaderStateBeginLine dotReaderState = iota // at start of line
	dotReaderStateDot                             // saw dot at start of line
	dotReaderStateDotCR                           // saw dot then CR
	dotReaderStateCR                              // saw CR (not at line start)
	dotReaderStateData                            // reading normal data
	dotReaderStateEOF                             // reached terminating ".\r\n"
)

// dotReader reads a dot-terminated multi-line NNTP response.
type dotReader struct {
	r     *nntpReader
	state dotReaderState
}

func (d *dotReader) Read(p []byte) (n int, err error) {
	if d.state == dotReaderStateEOF {
		return 0, io.EOF
	}

	for n < len(p) {
		// We need to read byte by byte for proper state tracking
		var buf [1]byte
		_, err := d.r.buf.Read(buf[:])
		if err != nil {
			return n, err
		}
		b := buf[0]

		switch d.state {
		case dotReaderStateBeginLine:
			switch b {
			case '.':
				d.state = dotReaderStateDot
			case '\r':
				d.state = dotReaderStateCR
				p[n] = b
				n++
			default:
				d.state = dotReaderStateData
				p[n] = b
				n++
			}

		case dotReaderStateDot:
			switch b {
			case '\r':
				d.state = dotReaderStateDotCR
			case '\n':
				// Bare ".\n" is also a terminator
				d.state = dotReaderStateEOF
				return n, io.EOF
			case '.':
				// Dot-stuffing: ".." at line start becomes "."
				d.state = dotReaderStateData
				p[n] = '.'
				n++
			default:
				// Regular line starting with dot
				d.state = dotReaderStateData
				p[n] = '.'
				n++
				if n < len(p) {
					p[n] = b
					n++
				}
			}

		case dotReaderStateDotCR:
			if b == '\n' {
				// ".\r\n" - end of response
				d.state = dotReaderStateEOF
				return n, io.EOF
			}
			// False alarm, output the dot and CR
			d.state = dotReaderStateData
			if n < len(p) {
				p[n] = '.'
				n++
			}
			if n < len(p) {
				p[n] = '\r'
				n++
			}
			if n < len(p) {
				p[n] = b
				n++
			}

		case dotReaderStateCR:
			if b == '\n' {
				// End of line
				d.state = dotReaderStateBeginLine
				p[n] = b
				n++
			} else {
				// CR not followed by LF
				d.state = dotReaderStateData
				p[n] = b
				n++
			}

		case dotReaderStateData:
			switch b {
			case '\r':
				d.state = dotReaderStateCR
			case '\n':
				d.state = dotReaderStateBeginLine
			}
			p[n] = b
			n++
		}
	}

	return n, nil
}
