package nntppool

import "errors"

// Sentinel errors for the connection pool.
var (
	// ErrArticleNotFound indicates the article does not exist on any provider.
	ErrArticleNotFound = errors.New("article not found")

	// ErrNoProvidersAvailable indicates no providers are currently healthy.
	ErrNoProvidersAvailable = errors.New("no providers available")

	// ErrAllProvidersFailed indicates all providers failed to retrieve the article.
	ErrAllProvidersFailed = errors.New("all providers failed")

	// ErrPoolClosed indicates the pool has been closed.
	ErrPoolClosed = errors.New("pool is closed")

	// ErrInvalidConfig indicates the pool configuration is invalid.
	ErrInvalidConfig = errors.New("invalid configuration")
)

// NNTP status codes.
const (
	StatusArticleFollows       = 220
	StatusHeadFollows          = 221
	StatusBodyFollows          = 222
	StatusArticleNotFound      = 430
	StatusNoSuchArticle        = 430
	StatusNoSuchGroup          = 411
	StatusNoNewsgroupSelected  = 412
	StatusAuthRequired         = 480
	StatusAuthRejected         = 482
	StatusPostingNotAllowed    = 440
	StatusPostingFailed        = 441
	StatusServiceUnavailable   = 400
	StatusCommandNotRecognized = 500
)

// IsArticleNotFound checks if the error indicates an article doesn't exist.
// This includes both the sentinel error and NNTP 430 responses.
func IsArticleNotFound(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, ErrArticleNotFound) {
		return true
	}
	// Check for ProviderError with 430 status
	var pe *ProviderError
	if errors.As(err, &pe) {
		return pe.StatusCode == StatusArticleNotFound
	}
	return false
}

// IsRetryable checks if the error is temporary and the request can be retried.
func IsRetryable(err error) bool {
	if err == nil {
		return false
	}
	// Pool closed is not retryable
	if errors.Is(err, ErrPoolClosed) {
		return false
	}
	// Article not found is retryable (different provider may have it)
	if IsArticleNotFound(err) {
		return true
	}
	// Check for temporary provider errors
	var pe *ProviderError
	if errors.As(err, &pe) {
		return pe.Temporary
	}
	return false
}

// IsNoGroupSelected checks if the error indicates no newsgroup is selected (412).
func IsNoGroupSelected(err error) bool {
	if err == nil {
		return false
	}
	var pe *ProviderError
	if errors.As(err, &pe) {
		return pe.StatusCode == StatusNoNewsgroupSelected
	}
	return false
}

// ProviderError wraps an error from a specific provider.
type ProviderError struct {
	ProviderName string
	Host         string
	StatusCode   int
	Message      string
	Temporary    bool
	Err          error
}

func (e *ProviderError) Error() string {
	if e.Err != nil {
		return e.ProviderName + ": " + e.Err.Error()
	}
	return e.ProviderName + ": " + e.Message
}

func (e *ProviderError) Unwrap() error {
	return e.Err
}
