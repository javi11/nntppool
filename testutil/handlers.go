package testutil

import (
	"fmt"
	"strings"
)

// SuccessfulBodyHandler returns a handler that responds to BODY commands with the given content.
func SuccessfulBodyHandler(content string) Handler {
	return func(cmd string) (string, error) {
		if strings.HasPrefix(cmd, "BODY") {
			return fmt.Sprintf("222 0 <id> body follows\r\n%s\r\n.\r\n", content), nil
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
