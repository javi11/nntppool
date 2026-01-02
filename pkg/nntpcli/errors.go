package nntpcli

import (
	"errors"
	"fmt"
)

// NNTPError represents a numeric error response from an NNTP server.
// It replaces textproto.Error to remove the dependency on net/textproto.
type NNTPError struct {
	Code int
	Msg  string
}

func (e *NNTPError) Error() string {
	return fmt.Sprintf("%d %s", e.Code, e.Msg)
}

var (
	ErrCapabilitiesUnpopulated = errors.New("capabilities unpopulated")
	ErrNoSuchCapability        = errors.New("no such capability")
	ErrNilNttpConn             = errors.New("nil nntp connection")
	ErrArticleNotFound         = errors.New("article not found")
	ErrSegmentAlreadyExists    = errors.New("segment already exists")
)

const (
	SegmentAlreadyExistsErrCode = 441
	TooManyConnectionsErrCode   = 502
	CanNotJoinGroup             = 411
	ArticleNotFoundErrCode      = 430
)

func IsArticleNotFoundError(err error) bool {
	var nntpErr *NNTPError
	if ok := errors.As(err, &nntpErr); ok {
		return nntpErr.Code == ArticleNotFoundErrCode
	}

	return false
}

func IsSegmentAlreadyExistsError(err error) bool {
	var nntpErr *NNTPError
	if ok := errors.As(err, &nntpErr); ok {
		return nntpErr.Code == SegmentAlreadyExistsErrCode
	}

	return false
}
