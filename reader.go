package nntppool

import (
	"bytes"
	"hash/crc32"
	"io"
	"strconv"

	"github.com/javi11/nntppool/v3/internal"
	"github.com/mnightingale/rapidyenc"
)

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
	hasEmptyline bool // for article requests has the empty line separating headers and body been seen

	OnYencHeader func(*YencHeader)
}

const nntpBody = 222
const nntpArtiicle = 220

// Feed consumes raw NNTP protocol bytes from buf, writing any decoded payload bytes to out.
// It returns (bytesConsumedFromBuf, done, error).
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

	// Line by line processing
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
					if err != nil || !internal.IsMultiline(r.StatusCode) {
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
						// Still decoding, need more data
						return read, nil
					}
					// =ypart was encountered, switch to body decoding
				}
			case rapidyenc.FormatUU:
			}
		}
	}

	return read, nil
}

func (r *NNTPResponse) detectFormat(line []byte) {
	if r.StatusCode != nntpBody && r.StatusCode != nntpArtiicle {
		return
	}

	if len(line) == 0 {
		r.hasEmptyline = true
		return
	}

	// YEnc detection
	if bytes.HasPrefix(line, []byte("=ybegin ")) {
		r.Format = rapidyenc.FormatYenc
		return
	}

	// UUEncode detection: 60 or 61 chars, starts with 'M'
	if (len(line) == 60 || len(line) == 61) && line[0] == 'M' {
		r.Format = rapidyenc.FormatUU
		return
	}

	// UUEncode alternative header form: "begin "
	if bytes.HasPrefix(line, []byte("begin ")) {
		// Skip leading spaces
		line = bytes.TrimLeft(line[6:], " ")

		// Extract the next token (permission part)
		perms, found := bytes.CutPrefix(line, []byte(" "))
		if !found {
			return
		}

		// Check all characters are between '0' and '7'
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

	// Remove dot stuffing
	if bytes.HasPrefix(line, []byte("..")) {
		line = line[1:]
	}

	// Multipart UU with a short final part
	if len(line) <= 1 {
		return
	}

	// For Article responses only consider after the headers
	if r.StatusCode != nntpBody && (r.StatusCode != nntpArtiicle || !r.hasEmptyline) {
		return
	}

	first := line[0]
	n := len(line)

	for _, length := range []int{
		internal.DecodeUUCharWorkaround(first),
		internal.DecodeUUChar(first),
	} {
		if n < length {
			continue
		}

		body := line[1:length]
		padding := line[length:]

		if !internal.AllInASCIIRange(body, 32, 96) || !internal.OnlySpaceOrBacktick(padding) {
			continue
		}

		// Probably UU
		r.Format = rapidyenc.FormatUU
		r.body = true
		return
	}
}

func (r *NNTPResponse) decodeYenc(buf []byte, out io.Writer) (n int64, err error) {
	if len(buf) == 0 {
		return 0, nil
	}

	//dst := r.Data[:]

	var produced, consumed int
	var end rapidyenc.End

	produced, consumed, end, _ = rapidyenc.DecodeIncremental(buf, buf, &r.State)

	if produced > 0 {
		r.CRC = crc32.Update(r.CRC, crc32.IEEETable, buf[:produced])

		if wa, ok := out.(io.WriterAt); ok {
			if _, werr := wa.WriteAt(buf[:produced], r.PartBegin+int64(r.BytesDecoded)); werr != nil {
				return n, werr
			}
		} else {
			if _, werr := out.Write(buf[:produced]); werr != nil {
				return n, werr
			}
		}

		r.BytesDecoded += produced
	}
	n += int64(consumed)

	switch end {
	case rapidyenc.EndNone:
		if r.State == rapidyenc.StateCRLFEQ {
			// Special case: found "\r\n=" but no more data - might be start of =yend
			r.State = rapidyenc.StateCRLF
			n -= 1 // Back up to allow =yend detection
		}
	case rapidyenc.EndControl:
		// Found "\r\n=y" - likely =yend line, exit body mode
		r.body = false
		n -= 2 // Back up to include "=y" for header processing
	case rapidyenc.EndArticle:
		// Found ".\r\n" - NNTP article terminator, exit body mode
		r.body = false
		n -= 3 // Back up to include ".\r\n" for terminator detection
	}

	return n, nil
}

func (r *NNTPResponse) processYencHeader(line []byte) {
	var err error
	var isBegin, isPart bool

	if bytes.HasPrefix(line, []byte("=ybegin ")) {
		isBegin = true
		line = line[len("=ybegin"):]
		r.FileSize, _ = internal.ExtractInt(line, []byte(" size="))
		r.FileName, _ = internal.ExtractString(line, []byte(" name="))
		if r.Part, err = internal.ExtractInt(line, []byte(" part=")); err != nil {
			// Not multi-part, so body starts immediately after =ybegin
			r.body = true
			r.PartSize = r.FileSize
		}
		r.Total, _ = internal.ExtractInt(line, []byte(" total="))
	} else if bytes.HasPrefix(line, []byte("=ypart ")) {
		// =ypart signals start of body data in multi-part files
		isPart = true
		r.hasPart = true
		r.body = true
		line = line[len("=ypart"):]
		var begin int64
		// Convert from 1-based to 0-based indexing
		if begin, err = internal.ExtractInt(line, []byte(" begin=")); err == nil {
			r.PartBegin = begin - 1
		}
		if end, err := internal.ExtractInt(line, []byte(" end=")); err == nil && end > begin {
			r.PartSize = end - r.PartBegin
		}
	} else if bytes.HasPrefix(line, []byte("=yend ")) {
		r.hasEnd = true
		line = line[len("=yend"):]
		if crc, err := internal.ExtractCRC(line, []byte(" pcrc32=")); err == nil {
			r.ExpectedCRC = crc
			r.hasCrc = true
		} else if crc, err := internal.ExtractCRC(line, []byte(" crc32=")); err == nil {
			r.ExpectedCRC = crc
			r.hasCrc = true
		}
		r.EndSize, _ = internal.ExtractInt(line, []byte(" size="))
	}

	if r.OnYencHeader != nil && (isBegin || isPart) {
		r.OnYencHeader(&YencHeader{
			FileName:  r.FileName,
			FileSize:  r.FileSize,
			Part:      r.Part,
			PartBegin: r.PartBegin,
			PartSize:  r.PartSize,
			Total:     r.Total,
		})
	}
}
