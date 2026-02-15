package nntppool

import (
	"errors"
	"fmt"
)

// Error represents an NNTP protocol-level error with a status code.
type Error struct {
	Code    int
	Message string
}

func (e *Error) Error() string {
	return fmt.Sprintf("nntp: %d %s", e.Code, e.Message)
}

// Is matches by semantic category so that, for example, both 430 ("no such article")
// and 423 ("no article with that number") match ErrArticleNotFound.
func (e *Error) Is(target error) bool {
	var t *Error
	if !errors.As(target, &t) {
		return false
	}
	return errorCategory(e.Code) == errorCategory(t.Code)
}

func errorCategory(code int) int {
	switch code {
	case 423, 430:
		return 430 // article not found
	default:
		return code
	}
}

var (
	ErrArticleNotFound    = &Error{Code: 430, Message: "no such article"}
	ErrPostingNotPermitted = &Error{Code: 440, Message: "posting not permitted"}
	ErrPostingFailed       = &Error{Code: 441, Message: "posting failed"}
	ErrAuthRequired       = &Error{Code: 480, Message: "authentication required"}
	ErrAuthRejected       = &Error{Code: 481, Message: "authentication rejected"}
	ErrServiceUnavailable = &Error{Code: 502, Message: "service unavailable"}
	ErrCRCMismatch        = errors.New("nntp: yEnc CRC mismatch")
)

// toError maps an NNTP status code to a sentinel error, or returns nil for success codes.
func toError(code int, status string) error {
	switch {
	case code >= 200 && code < 400:
		return nil
	case code == 423 || code == 430:
		return ErrArticleNotFound
	case code == 440:
		return ErrPostingNotPermitted
	case code == 441:
		return ErrPostingFailed
	case code == 480:
		return ErrAuthRequired
	case code == 481:
		return ErrAuthRejected
	case code == 502:
		return ErrServiceUnavailable
	default:
		return &Error{Code: code, Message: status}
	}
}
