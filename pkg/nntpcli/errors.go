package nntpcli

import (
	"errors"
	"net/textproto"
)

var (
	ErrCapabilitiesUnpopulated = errors.New("capabilities unpopulated")
	ErrNoSuchCapability        = errors.New("no such capability")
	ErrNilNttpConn             = errors.New("nil nntp connection")
	ErrArticleNotFound         = errors.New("article not found")
	ErrSegmentAlreadyExists    = errors.New("segment already exists")
)

const (
	SegmentAlreadyExistsErrCode = 441
	ToManyConnectionsErrCode    = 502
	CanNotJoinGroup             = 411
	ArticleNotFoundErrCode      = 430
)

func IsArticleNotFoundError(err error) bool {
	var nntpErr *textproto.Error
	if ok := errors.As(err, &nntpErr); ok {
		return nntpErr.Code == 430
	}

	return false
}

func IsSegmentAlreadyExistsError(err error) bool {
	var nntpErr *textproto.Error
	if ok := errors.As(err, &nntpErr); ok {
		return nntpErr.Code == 441
	}

	return false
}
