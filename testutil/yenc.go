package testutil

import (
	"fmt"
)

// EncodeYenc creates a simple YEnc-encoded string for testing.
// It encodes the data by adding 42 to each byte (standard yEnc encoding).
// For multi-part files, specify part and total. For single part, use part=1, total=1.
func EncodeYenc(data []byte, filename string, part int64, total int64) string {
	size := len(data)
	encoded := make([]byte, size)

	// Simple yEnc encoding: add 42 to each byte
	for i, b := range data {
		encoded[i] = b + 42
	}

	var result string

	if total > 1 {
		// Multi-part yEnc
		result = fmt.Sprintf("=ybegin part=%d total=%d line=128 size=%d name=%s\r\n",
			part, total, size, filename)
		result += fmt.Sprintf("=ypart begin=1 end=%d\r\n", size)
		result += string(encoded)
		result += fmt.Sprintf("\r\n=yend size=%d part=%d pcrc32=00000000\r\n", size, part)
	} else {
		// Single-part yEnc
		result = fmt.Sprintf("=ybegin line=128 size=%d name=%s\r\n",
			size, filename)
		result += string(encoded)
		result += fmt.Sprintf("\r\n=yend size=%d pcrc32=00000000\r\n", size)
	}

	return result
}

// EncodeYencMultiPart creates a multi-part YEnc-encoded string.
// This is a convenience wrapper around EncodeYenc for multi-part files.
func EncodeYencMultiPart(data []byte, filename string, part int64, total int64, partBegin int64, partEnd int64) string {
	size := partEnd - partBegin + 1
	encoded := make([]byte, size)

	// Simple yEnc encoding: add 42 to each byte
	for i := int64(0); i < size; i++ {
		encoded[i] = data[partBegin-1+i] + 42
	}

	result := fmt.Sprintf("=ybegin part=%d total=%d line=128 size=%d name=%s\r\n",
		part, total, len(data), filename)
	result += fmt.Sprintf("=ypart begin=%d end=%d\r\n", partBegin, partEnd)
	result += string(encoded)
	result += fmt.Sprintf("\r\n=yend size=%d part=%d pcrc32=00000000\r\n", size, part)

	return result
}
