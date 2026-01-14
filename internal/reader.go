package internal

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
)

// Parsing helper functions

func AllInASCIIRange(b []byte, lo, hi byte) bool {
	for _, c := range b {
		if c < lo || c > hi {
			return false
		}
	}
	return true
}

func OnlySpaceOrBacktick(b []byte) bool {
	for _, c := range b {
		if c != ' ' && c != '`' {
			return false
		}
	}
	return true
}

func DecodeUUCharWorkaround(c byte) int {
	return int(((int(c)-32)&63)*4+5) / 3
}

func DecodeUUChar(c byte) int {
	if c == '`' {
		return 0
	}
	return int((c - ' ') & 0x3F)
}

func IsMultiline(code int) bool {
	const nntpBody = 222
	const nntpArticle = 220
	const nntpHead = 221
	const nntpCapabilities = 101
	return code == nntpBody || code == nntpArticle || code == nntpHead || code == nntpCapabilities
}

func ExtractString(data, substr []byte) (string, error) {
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

func ExtractInt(data, substr []byte) (int64, error) {
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

var (
	ErrCrcNotfound = errors.New("crc not found")
)

// ExtractCRC converts a hexadecimal representation of a crc32 hash
func ExtractCRC(data, substr []byte) (uint32, error) {
	start := bytes.Index(data, substr)
	if start == -1 {
		return 0, ErrCrcNotfound
	}

	data = data[start+len(substr):]
	end := bytes.IndexAny(data, "\x00\x20\r\n")
	if end != -1 {
		data = data[:end]
	}

	// Take up to the last 8 characters
	parsed := data[len(data)-min(8, len(data)):]

	// Left pad unexpected length with 0
	if len(parsed) != 8 {
		padded := []byte("00000000")
		copy(padded[8-len(parsed):], parsed)
		parsed = padded
	}

	_, err := hex.Decode(parsed, parsed)
	return binary.BigEndian.Uint32(parsed), err
}
