package nntpcli

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"strconv"

	"github.com/mnightingale/rapidyenc"
)

// NNTP status codes for multiline responses.
const (
	nntpBody         = 222
	nntpArticle      = 220
	nntpHead         = 221
	nntpCapabilities = 101
)

// NNTPResponse holds the parsed response data from an NNTP command.
// It implements the streamFeeder interface for incremental parsing.
type NNTPResponse struct {
	BytesDecoded  int
	BytesConsumed int
	Lines         []string
	Format        rapidyenc.Format
	FileName      string
	FileSize      int64
	Part          int64
	PartBegin     int64
	PartSize      int64
	EndSize       int64
	Total         int64
	ExpectedCRC   uint32
	Message       string
	State         rapidyenc.State
	StatusCode    int
	CRC           uint32

	eof          bool
	body         bool
	hasPart      bool
	hasEnd       bool
	hasCrc       bool
	hasEmptyline bool
	hasBaddata   bool
}

// Done returns true if the response has been fully parsed.
func (r *NNTPResponse) Done() bool {
	return r.eof
}

// Feed implements streamFeeder for incremental response parsing.
func (r *NNTPResponse) Feed(buf []byte, out io.Writer) (consumed int, done bool, err error) {
	if out == nil {
		out = io.Discard
	}

	n, err := r.decode(buf, out)
	if err != nil {
		return n, false, err
	}
	if r.eof {
		return n, true, nil
	}
	return n, false, nil
}

func (r *NNTPResponse) decode(buf []byte, out io.Writer) (read int, err error) {
	if r.body && r.Format == rapidyenc.FormatYenc {
		n, err := r.decodeYenc(buf, out)
		if err != nil {
			return int(n), err
		}
		read += int(n)
		buf = buf[n:]
		if r.body {
			return int(n), err
		}
	}

	if !r.body {
		var line []byte
		var found bool
		for {
			if line, buf, found = bytes.Cut(buf, []byte("\r\n")); !found {
				break
			}
			read += len(line) + 2

			if bytes.Equal(line, []byte(".")) {
				r.eof = true
				break
			}

			if r.Format == rapidyenc.FormatUnknown {
				if r.StatusCode == 0 && len(line) >= 3 {
					r.Message = string(line)
					r.StatusCode, err = strconv.Atoi(string(line[:3]))
					if err != nil || !isMultiline(r.StatusCode) {
						r.eof = true
						break
					}
					continue
				}
				r.detectFormat(line)
			}

			switch r.Format {
			case rapidyenc.FormatUnknown:
				r.Lines = append(r.Lines, string(line))
			case rapidyenc.FormatYenc:
				r.processYencHeader(line)
				if r.body {
					n, err := r.decodeYenc(buf, out)
					read += int(n)
					buf = buf[n:]
					if err != nil {
						return read, err
					}
					if r.body {
						return read, nil
					}
				}
			case rapidyenc.FormatUU:
				// UU format handling - not implemented in detail
			}
		}
	}

	return read, nil
}

func (r *NNTPResponse) detectFormat(line []byte) {
	if r.StatusCode != nntpBody && r.StatusCode != nntpArticle {
		return
	}

	if len(line) == 0 {
		r.hasEmptyline = true
		return
	}

	if bytes.HasPrefix(line, []byte("=ybegin ")) {
		r.Format = rapidyenc.FormatYenc
		return
	}

	if (len(line) == 60 || len(line) == 61) && line[0] == 'M' {
		r.Format = rapidyenc.FormatUU
		return
	}

	if bytes.HasPrefix(line, []byte("begin ")) {
		line = bytes.TrimLeft(line[6:], " ")

		perms, found := bytes.CutPrefix(line, []byte(" "))
		if !found {
			return
		}

		valid := true
		for _, c := range perms {
			if c < '0' || c > '7' {
				valid = false
				break
			}
		}

		if valid {
			r.Format = rapidyenc.FormatUU
		}
		return
	}

	if bytes.HasPrefix(line, []byte("..")) {
		line = line[1:]
	}

	if len(line) <= 1 {
		return
	}

	if !(r.StatusCode == nntpBody || (r.StatusCode == nntpArticle && r.hasEmptyline)) {
		return
	}

	first := line[0]
	n := len(line)

	for _, length := range []int{
		decodeUUCharWorkaround(first),
		decodeUUChar(first),
	} {
		if n < length {
			continue
		}

		body := line[1:length]
		padding := line[length:]

		if !allInASCIIRange(body, 32, 96) || !onlySpaceOrBacktick(padding) {
			continue
		}

		r.Format = rapidyenc.FormatUU
		r.body = true
		return
	}
}

func allInASCIIRange(b []byte, lo, hi byte) bool {
	for _, c := range b {
		if c < lo || c > hi {
			return false
		}
	}
	return true
}

func onlySpaceOrBacktick(b []byte) bool {
	for _, c := range b {
		if c != ' ' && c != '`' {
			return false
		}
	}
	return true
}

func decodeUUCharWorkaround(c byte) int {
	return int(((int(c)-32)&63)*4+5) / 3
}

func decodeUUChar(c byte) int {
	if c == '`' {
		return 0
	}
	return int((c - ' ') & 0x3F)
}

func isMultiline(code int) bool {
	return code == nntpBody || code == nntpArticle || code == nntpHead || code == nntpCapabilities
}

func (r *NNTPResponse) decodeYenc(buf []byte, out io.Writer) (n int64, err error) {
	if len(buf) == 0 {
		return 0, nil
	}

	var produced, consumed int
	var end rapidyenc.End

	produced, consumed, end, err = rapidyenc.DecodeIncremental(buf, buf, &r.State)

	if produced > 0 {
		r.CRC = crc32.Update(r.CRC, crc32.IEEETable, buf[:produced])
		r.BytesDecoded += produced
		_, werr := out.Write(buf[:produced])
		if werr != nil {
			return n, werr
		}
	}
	n += int64(consumed)

	switch end {
	case rapidyenc.EndNone:
		if r.State == rapidyenc.StateCRLFEQ {
			r.State = rapidyenc.StateCRLF
			n -= 1
		}
	case rapidyenc.EndControl:
		r.body = false
		n -= 2
	case rapidyenc.EndArticle:
		r.body = false
		n -= 3
	}

	return n, nil
}

func (r *NNTPResponse) processYencHeader(line []byte) {
	var err error
	if bytes.HasPrefix(line, []byte("=ybegin ")) {
		line = line[len("=ybegin"):]
		r.FileSize, _ = extractInt(line, []byte(" size="))
		r.FileName, _ = extractString(line, []byte(" name="))
		if r.Part, err = extractInt(line, []byte(" part=")); err != nil {
			r.body = true
			r.PartSize = r.FileSize
		}
		r.Total, _ = extractInt(line, []byte(" total="))
	} else if bytes.HasPrefix(line, []byte("=ypart ")) {
		r.hasPart = true
		r.body = true
		line = line[len("=ypart"):]
		var begin int64
		if begin, err = extractInt(line, []byte(" begin=")); err == nil {
			r.PartBegin = begin - 1
		}
		if end, err := extractInt(line, []byte(" end=")); err == nil && end > begin {
			r.PartSize = end - r.PartBegin
		}
	} else if bytes.HasPrefix(line, []byte("=yend ")) {
		r.hasEnd = true
		line = line[len("=yend"):]
		if crc, err := extractCRC(line, []byte(" pcrc32=")); err == nil {
			r.ExpectedCRC = crc
			r.hasCrc = true
		} else if crc, err := extractCRC(line, []byte(" crc32=")); err == nil {
			r.ExpectedCRC = crc
			r.hasCrc = true
		}
		r.EndSize, _ = extractInt(line, []byte(" size="))
	}
}

func extractString(data, substr []byte) (string, error) {
	start := bytes.Index(data, substr)
	if start == -1 {
		return "", fmt.Errorf("substr not found: %s", substr)
	}

	data = data[start+len(substr):]
	if end := bytes.IndexAny(data, "\x00\r\n"); end != -1 {
		return string(data[:end]), nil
	}

	return string(data), nil
}

func extractInt(data, substr []byte) (int64, error) {
	start := bytes.Index(data, substr)
	if start == -1 {
		return 0, fmt.Errorf("substr not found: %s", substr)
	}

	data = data[start+len(substr):]
	if end := bytes.IndexAny(data, "\x00\x20\r\n"); end != -1 {
		return strconv.ParseInt(string(data[:end]), 10, 64)
	}

	return strconv.ParseInt(string(data), 10, 64)
}

var errCrcNotfound = errors.New("crc not found")

func extractCRC(data, substr []byte) (uint32, error) {
	start := bytes.Index(data, substr)
	if start == -1 {
		return 0, errCrcNotfound
	}

	data = data[start+len(substr):]
	end := bytes.IndexAny(data, "\x00\x20\r\n")
	if end != -1 {
		data = data[:end]
	}

	parsed := data[len(data)-min(8, len(data)):]

	if len(parsed) != 8 {
		padded := []byte("00000000")
		copy(padded[8-len(parsed):], parsed)
		parsed = padded
	}

	_, err := hex.Decode(parsed, parsed)
	return binary.BigEndian.Uint32(parsed), err
}
