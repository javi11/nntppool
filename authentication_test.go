package nntppool

import (
	"errors"
	"net/textproto"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIsAuthenticationError(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		expected bool
	}{
		{
			name:     "nil error",
			err:      nil,
			expected: false,
		},
		{
			name:     "authentication error message",
			err:      errors.New("authentication failed"),
			expected: true,
		},
		{
			name:     "unauthorized error message",
			err:      errors.New("unauthorized access"),
			expected: true,
		},
		{
			name:     "invalid username error message",
			err:      errors.New("invalid username provided"),
			expected: true,
		},
		{
			name:     "invalid password error message",
			err:      errors.New("invalid password provided"),
			expected: true,
		},
		{
			name:     "wrong username error message",
			err:      errors.New("wrong username"),
			expected: true,
		},
		{
			name:     "wrong password error message",
			err:      errors.New("wrong password"),
			expected: true,
		},
		{
			name:     "bad username error message",
			err:      errors.New("bad username"),
			expected: true,
		},
		{
			name:     "bad password error message",
			err:      errors.New("bad password"),
			expected: true,
		},
		{
			name:     "login failed error message",
			err:      errors.New("login failed"),
			expected: true,
		},
		{
			name:     "access denied error message",
			err:      errors.New("access denied"),
			expected: true,
		},
		{
			name:     "case insensitive authentication",
			err:      errors.New("AUTHENTICATION FAILED"),
			expected: true,
		},
		{
			name:     "network error message",
			err:      errors.New("connection refused"),
			expected: false,
		},
		{
			name:     "timeout error message",
			err:      errors.New("connection timeout"),
			expected: false,
		},
		{
			name:     "NNTP 401 error code",
			err:      &textproto.Error{Code: AuthenticationRequiredCode, Msg: "Authentication Required"},
			expected: true,
		},
		{
			name:     "NNTP 403 error code",
			err:      &textproto.Error{Code: AuthenticationFailedCode, Msg: "Forbidden"},
			expected: true,
		},
		{
			name:     "NNTP 480 error code",
			err:      &textproto.Error{Code: InvalidUsernamePasswordCode, Msg: "Authentication Failed"},
			expected: true,
		},
		{
			name:     "NNTP 430 error code (not auth)",
			err:      &textproto.Error{Code: ArticleNotFoundErrCode, Msg: "Article not found"},
			expected: false,
		},
		{
			name:     "NNTP 502 error code (not auth)",
			err:      &textproto.Error{Code: ToManyConnectionsErrCode, Msg: "Too many connections"},
			expected: false,
		},
		{
			name:     "generic error",
			err:      errors.New("something went wrong"),
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := isAuthenticationError(tt.err)
			assert.Equal(t, tt.expected, result, "isAuthenticationError(%v) = %v, want %v", tt.err, result, tt.expected)
		})
	}
}
