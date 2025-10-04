// Package nntp provides base NNTP definitions.
package nntpcli

import (
	"fmt"
	"io"
	"net/textproto"
)

// NNTP response codes as defined in RFC 3977
const (
	// 2xx - Positive Completion
	StatusReady                = 200 // Service available, posting allowed
	StatusReadyNoPosting       = 201 // Service available, posting prohibited
	StatusQuit                 = 205 // Connection closing
	StatusGroupSelected        = 211 // Group selected
	StatusBodyFollows          = 222 // Body article retrieved
	StatusStatSuccess          = 223 // Article exists
	StatusArticlePosted        = 240 // Article posted successfully
	StatusAuthenticated        = 281 // Authentication successful

	// 3xx - Positive Intermediate
	StatusPasswordRequired     = 340 // Password required
	StatusMoreAuthInfoRequired = 381 // More authentication information required

	// 4xx - Transient Negative Completion
	StatusNoSuchGroup          = 411 // No such newsgroup
	StatusNoArticleSelected    = 420 // No article selected
	StatusNoSuchArticleNumber  = 423 // No article with that number
	StatusNoSuchArticle        = 430 // No article with that message-id

	// 5xx - Permanent Negative Completion
	StatusCommandUnknown       = 500 // Command not recognized
	StatusCommandSyntaxError   = 501 // Command syntax error
	StatusPermissionDenied     = 502 // Permission denied / Too many connections
	StatusProgramFault         = 503 // Program fault
)

// Legacy constants for backward compatibility
const (
	NumberOfStatResParams = 3
)

// PostingStatus type for groups.
type PostingStatus byte

// PostingStatus values.
const (
	Unknown             = PostingStatus(0)
	PostingPermitted    = PostingStatus('y')
	PostingNotPermitted = PostingStatus('n')
	PostingModerated    = PostingStatus('m')
)

func (ps PostingStatus) String() string {
	return fmt.Sprintf("%c", ps)
}

// Group represents a usenet newsgroup.
type Group struct {
	Name        string
	Description string
	Count       int64
	High        int64
	Low         int64
	Posting     PostingStatus
}

// An Article that may appear in one or more groups.
type Article struct {
	// The article's headers
	Header textproto.MIMEHeader
	// The article's body
	Body io.Reader
	// Number of bytes in the article body (used by OVER/XOVER)
	Bytes int
	// Number of lines in the article body (used by OVER/XOVER)
	Lines int
}

// MessageID provides convenient access to the article's Message ID.
func (a *Article) MessageID() string {
	return a.Header.Get("Message-Id")
}

func (a *Article) String() string {
	id, ok := a.Header["Message-Id"]
	if !ok {
		return "[NNTP article]"
	}

	return fmt.Sprintf("[NNTP article %s]", id[0])
}
