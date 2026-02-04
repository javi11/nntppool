package testutil

import (
	"fmt"
	"strings"
	"time"
)

// SuccessfulBodyHandler returns a handler that responds to BODY commands with the given content.
func SuccessfulBodyHandler(content string) Handler {
	return func(cmd string) (string, error) {
		if strings.HasPrefix(cmd, "BODY") {
			return fmt.Sprintf("222 0 <id> body follows\r\n%s\r\n.\r\n", content), nil
		}
		if cmd == "DATE\r\n" {
			return "111 20240101000000\r\n", nil
		}
		if strings.HasPrefix(cmd, "QUIT") {
			return "205 Bye\r\n", nil
		}
		return "500 Unknown Command\r\n", nil
	}
}

// YencBodyHandler returns a handler that responds to BODY commands with YEnc-encoded content.
func YencBodyHandler(data []byte, filename string) Handler {
	yencBody := EncodeYenc(data, filename, 1, 1)
	return func(cmd string) (string, error) {
		if strings.HasPrefix(cmd, "BODY") {
			return fmt.Sprintf("222 0 <id> body follows\r\n%s.\r\n", yencBody), nil
		}
		if cmd == "DATE\r\n" {
			return "111 20240101000000\r\n", nil
		}
		if strings.HasPrefix(cmd, "QUIT") {
			return "205 Bye\r\n", nil
		}
		return "500 Unknown Command\r\n", nil
	}
}

// NotFoundHandler returns a handler that responds with 430 (article not found).
func NotFoundHandler() Handler {
	return func(cmd string) (string, error) {
		if strings.HasPrefix(cmd, "BODY") || strings.HasPrefix(cmd, "ARTICLE") {
			return "430 No Such Article\r\n", nil
		}
		if cmd == "DATE\r\n" {
			return "111 20240101000000\r\n", nil
		}
		if strings.HasPrefix(cmd, "QUIT") {
			return "205 Bye\r\n", nil
		}
		return "500 Unknown Command\r\n", nil
	}
}

// DateHandler returns a handler that responds to DATE commands.
func DateHandler() Handler {
	return func(cmd string) (string, error) {
		if cmd == "DATE\r\n" {
			return "111 20240101000000\r\n", nil
		}
		return "500 Unknown Command\r\n", nil
	}
}

// CompositeHandler combines multiple handlers, routing by command prefix.
// The map key is the command prefix (e.g., "BODY", "DATE").
// If no prefix matches, returns "500 Unknown Command".
func CompositeHandler(handlers map[string]Handler) Handler {
	return func(cmd string) (string, error) {
		for prefix, handler := range handlers {
			if strings.HasPrefix(cmd, prefix) {
				return handler(cmd)
			}
		}
		return "500 Unknown Command\r\n", nil
	}
}

// DefaultHandler returns a handler that supports common NNTP commands.
// It responds to BODY with provided content, DATE, and QUIT.
func DefaultHandler(bodyContent string) Handler {
	return CompositeHandler(map[string]Handler{
		"BODY": SuccessfulBodyHandler(bodyContent),
		"DATE": DateHandler(),
		"QUIT": func(cmd string) (string, error) {
			return "205 Bye\r\n", nil
		},
	})
}

// MaxConnectionsExceededHandler returns a handler that responds with 502 "max connections exceeded".
// The count parameter tracks how many times this error has been returned.
func MaxConnectionsExceededHandler(count *int) Handler {
	return func(cmd string) (string, error) {
		if cmd == "DATE\r\n" {
			return "111 20240101000000\r\n", nil
		}
		if count != nil {
			*count++
		}
		return "502 Too many connections from your IP\r\n", nil
	}
}

// SlowResponseHandler returns a handler that delays responses by the given duration.
func SlowResponseHandler(delay time.Duration, content string) Handler {
	return func(cmd string) (string, error) {
		time.Sleep(delay)
		if strings.HasPrefix(cmd, "BODY") {
			return fmt.Sprintf("222 0 <id> body follows\r\n%s\r\n.\r\n", content), nil
		}
		if cmd == "DATE\r\n" {
			return "111 20240101000000\r\n", nil
		}
		if strings.HasPrefix(cmd, "QUIT") {
			return "205 Bye\r\n", nil
		}
		return "500 Unknown Command\r\n", nil
	}
}

// SlowDripHandler returns a handler that sends response bytes slowly (simulating slow network).
// This is useful for testing timeout handling and partial read scenarios.
// Note: This handler returns the full response string, but the actual slow-drip behavior
// must be implemented at the connection level (see SlowDripDialer).
func SlowDripHandler(byteDelay time.Duration, data []byte, filename string) Handler {
	yencBody := EncodeYenc(data, filename, 1, 1)
	return func(cmd string) (string, error) {
		if strings.HasPrefix(cmd, "BODY") {
			return fmt.Sprintf("222 0 <id> body follows\r\n%s.\r\n", yencBody), nil
		}
		if cmd == "DATE\r\n" {
			return "111 20240101000000\r\n", nil
		}
		if strings.HasPrefix(cmd, "QUIT") {
			return "205 Bye\r\n", nil
		}
		return "500 Unknown Command\r\n", nil
	}
}

// PartialResponseHandler returns a handler that disconnects mid-response after sending
// a partial response. Useful for testing partial write and failover scenarios.
func PartialResponseHandler(partialBytes int, data []byte, filename string) Handler {
	yencBody := EncodeYenc(data, filename, 1, 1)
	return func(cmd string) (string, error) {
		if strings.HasPrefix(cmd, "BODY") {
			fullResponse := fmt.Sprintf("222 0 <id> body follows\r\n%s.\r\n", yencBody)
			if partialBytes > 0 && partialBytes < len(fullResponse) {
				// Return partial response and signal to close connection
				return fullResponse[:partialBytes], fmt.Errorf("partial response")
			}
			return fullResponse, nil
		}
		if cmd == "DATE\r\n" {
			return "111 20240101000000\r\n", nil
		}
		if strings.HasPrefix(cmd, "QUIT") {
			return "205 Bye\r\n", nil
		}
		return "500 Unknown Command\r\n", nil
	}
}

// MalformedYencHandler returns a handler that responds with malformed yEnc content.
// malformationType can be: "missing_header", "invalid_crc", "missing_end", "bad_size"
func MalformedYencHandler(malformationType string, data []byte, filename string) Handler {
	return func(cmd string) (string, error) {
		if strings.HasPrefix(cmd, "BODY") {
			var body string
			size := len(data)
			encoded := make([]byte, size)
			for i, b := range data {
				encoded[i] = b + 42
			}

			switch malformationType {
			case "missing_header":
				// yEnc data without =ybegin line
				body = string(encoded) + "\r\n=yend size=" + fmt.Sprint(size) + "\r\n"
			case "invalid_crc":
				// yEnc with obviously wrong CRC
				body = fmt.Sprintf("=ybegin line=128 size=%d name=%s\r\n%s\r\n=yend size=%d crc32=DEADBEEF\r\n",
					size, filename, string(encoded), size)
			case "missing_end":
				// yEnc without =yend line
				body = fmt.Sprintf("=ybegin line=128 size=%d name=%s\r\n%s\r\n",
					size, filename, string(encoded))
			case "bad_size":
				// yEnc with mismatched size
				body = fmt.Sprintf("=ybegin line=128 size=%d name=%s\r\n%s\r\n=yend size=%d\r\n",
					size+1000, filename, string(encoded), size+1000)
			case "truncated":
				// yEnc that's truncated mid-data
				truncated := encoded[:len(encoded)/2]
				body = fmt.Sprintf("=ybegin line=128 size=%d name=%s\r\n%s",
					size, filename, string(truncated))
			default:
				// Normal yEnc
				body = fmt.Sprintf("=ybegin line=128 size=%d name=%s\r\n%s\r\n=yend size=%d\r\n",
					size, filename, string(encoded), size)
			}
			return fmt.Sprintf("222 0 <id> body follows\r\n%s.\r\n", body), nil
		}
		if cmd == "DATE\r\n" {
			return "111 20240101000000\r\n", nil
		}
		if strings.HasPrefix(cmd, "QUIT") {
			return "205 Bye\r\n", nil
		}
		return "500 Unknown Command\r\n", nil
	}
}

// CountingHandler wraps a handler and counts invocations per command prefix.
func CountingHandler(inner Handler, counts map[string]*int) Handler {
	return func(cmd string) (string, error) {
		for prefix, count := range counts {
			if strings.HasPrefix(cmd, prefix) && count != nil {
				*count++
			}
		}
		return inner(cmd)
	}
}

// ConditionalHandler returns different handlers based on the request count.
// handlerSequence maps request number (1-based) to handler.
// If a request number isn't in the map, defaultHandler is used.
func ConditionalHandler(handlerSequence map[int]Handler, defaultHandler Handler) Handler {
	count := 0
	return func(cmd string) (string, error) {
		count++
		if h, ok := handlerSequence[count]; ok {
			return h(cmd)
		}
		return defaultHandler(cmd)
	}
}

// ConnectionLimitHandler returns a handler that simulates connection limiting.
// It returns success for the first maxConns connections, then 502 for subsequent ones.
func ConnectionLimitHandler(maxConns *int, currentConns *int) Handler {
	return func(cmd string) (string, error) {
		if cmd == "DATE\r\n" {
			return "111 20240101000000\r\n", nil
		}
		if currentConns != nil && maxConns != nil && *currentConns > *maxConns {
			return "502 Too many connections from your IP\r\n", nil
		}
		if strings.HasPrefix(cmd, "BODY") {
			return "222 0 <id> body follows\r\nline1\r\n.\r\n", nil
		}
		if strings.HasPrefix(cmd, "QUIT") {
			return "205 Bye\r\n", nil
		}
		return "500 Unknown Command\r\n", nil
	}
}
