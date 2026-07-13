package nntppool

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// OutcomeKind is the transport-owned classification of one provider attempt
// or a final pool result.
type OutcomeKind string

const (
	OutcomeSuccess             OutcomeKind = "success"
	OutcomeHardArticleAbsence  OutcomeKind = "hard_article_absence"
	OutcomeTemporaryFailure    OutcomeKind = "temporary_failure"
	OutcomeProviderUnavailable OutcomeKind = "provider_unavailable"
	OutcomeCorruptBody         OutcomeKind = "corrupt_body"
	OutcomeCancellation        OutcomeKind = "cancellation"
	OutcomeTransportFailure    OutcomeKind = "transport_failure"
	OutcomeInconclusive        OutcomeKind = "inconclusive"
)

// Operation identifies the NNTP command represented by attempt evidence.
type Operation string

const (
	OperationUnknown Operation = "UNKNOWN"
	OperationBody    Operation = "BODY"
	OperationStat    Operation = "STAT"
	OperationHead    Operation = "HEAD"
	OperationPost    Operation = "POST"
)

// BodyValidationStatus records whether a BODY attempt crossed the complete
// transport validation boundary.
type BodyValidationStatus string

const (
	BodyValidationNotApplicable BodyValidationStatus = "not_applicable"
	BodyValidationNotRequested  BodyValidationStatus = "not_requested"
	BodyValidationValid         BodyValidationStatus = "valid"
	BodyValidationInvalid       BodyValidationStatus = "invalid"
	BodyValidationIncomplete    BodyValidationStatus = "incomplete"
)

// AttemptEvidence is transport-owned evidence for one bounded provider
// attempt. The three durations intentionally remain separate.
type AttemptEvidence struct {
	ProviderID string
	Operation  Operation
	Outcome    OutcomeKind

	ResponseCode   int
	BodyValidation BodyValidationStatus
	Cause          error
	// ProviderResponseTimeout is true only when the response-head service
	// deadline expired. Caller cancellation and local queue expiry leave it false.
	ProviderResponseTimeout bool

	PoolQueueDuration        time.Duration
	PipelineHeadWaitDuration time.Duration
	ResponseServiceDuration  time.Duration
}

// TransportError is the structured final error returned after cancellation or
// provider exhaustion. Cause remains wrapped for errors.Is/errors.As callers.
type TransportError struct {
	Kind         OutcomeKind
	ProviderID   string
	ResponseCode int
	Attempts     []AttemptEvidence
	Cause        error
}

func (e *TransportError) Error() string {
	if e == nil {
		return "<nil>"
	}
	if e.ProviderID != "" {
		return fmt.Sprintf("nntp: %s from %s: %v", e.Kind, e.ProviderID, e.Cause)
	}
	return fmt.Sprintf("nntp: %s: %v", e.Kind, e.Cause)
}

func (e *TransportError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Cause
}

func operationFromPayload(payload []byte) Operation {
	for _, candidate := range []struct {
		prefix string
		op     Operation
	}{
		{"BODY ", OperationBody},
		{"STAT ", OperationStat},
		{"HEAD ", OperationHead},
		{"POST", OperationPost},
	} {
		if len(payload) >= len(candidate.prefix) && string(payload[:len(candidate.prefix)]) == candidate.prefix {
			return candidate.op
		}
	}
	return OperationUnknown
}

func classifyOutcome(code int, err error) OutcomeKind {
	switch {
	case err == nil && code >= 200 && code < 400:
		return OutcomeSuccess
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return OutcomeCancellation
	case errors.Is(err, ErrBodyCorrupt), errors.Is(err, ErrCRCMismatch):
		return OutcomeCorruptBody
	case errors.Is(err, ErrServiceUnavailable), errors.Is(err, ErrAuthRequired),
		errors.Is(err, ErrAuthRejected), errors.Is(err, ErrQuotaExceeded),
		errors.Is(err, ErrMaxConnections), code == 502:
		return OutcomeProviderUnavailable
	case code == 423 || code == 430 || errors.Is(err, ErrArticleNotFound):
		return OutcomeHardArticleAbsence
	case code == 451:
		return OutcomeTemporaryFailure
	case func() bool {
		var protocolErr *Error
		return errors.As(err, &protocolErr)
	}():
		return OutcomeInconclusive
	case err != nil:
		return OutcomeTransportFailure
	default:
		return OutcomeInconclusive
	}
}

func cloneAttempts(attempts []AttemptEvidence) []AttemptEvidence {
	if len(attempts) == 0 {
		return nil
	}
	return append([]AttemptEvidence(nil), attempts...)
}

func newTransportError(attempts []AttemptEvidence, cause error) *TransportError {
	kind := OutcomeInconclusive
	haveKind := false
	var providerID string
	responseCode := 0
	for _, attempt := range attempts {
		if attempt.Outcome == OutcomeSuccess {
			continue
		}
		providerID = attempt.ProviderID
		responseCode = attempt.ResponseCode
		if attempt.Outcome == OutcomeCancellation {
			kind = OutcomeCancellation
			haveKind = true
			continue
		}
		if kind == OutcomeCancellation {
			continue
		}
		if !haveKind {
			kind = attempt.Outcome
			haveKind = true
		} else if kind != attempt.Outcome {
			// A final result is conclusive only when every provider attempt
			// agrees on its outcome class. Per-provider detail remains in Attempts.
			kind = OutcomeInconclusive
		}
	}
	return &TransportError{
		Kind:         kind,
		ProviderID:   providerID,
		ResponseCode: responseCode,
		Attempts:     cloneAttempts(attempts),
		Cause:        cause,
	}
}

func responseError(resp Response) error {
	cause := resp.Err
	if cause == nil {
		cause = toError(resp.StatusCode, resp.Status)
	}
	if cause == nil {
		return nil
	}
	if _, ok := cause.(*TransportError); ok {
		return cause
	}
	if len(resp.Attempts) == 0 {
		return cause
	}
	return newTransportError(resp.Attempts, cause)
}

func cancellationResponse(attempts []AttemptEvidence, cause error) Response {
	providerID := ""
	if len(attempts) > 0 {
		providerID = attempts[len(attempts)-1].ProviderID
	}
	err := &TransportError{
		Kind:       OutcomeCancellation,
		ProviderID: providerID,
		Attempts:   cloneAttempts(attempts),
		Cause:      cause,
	}
	return Response{Err: err, Attempts: cloneAttempts(attempts)}
}

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
	ErrArticleNotFound     = &Error{Code: 430, Message: "no such article"}
	ErrPostingNotPermitted = &Error{Code: 440, Message: "posting not permitted"}
	ErrPostingFailed       = &Error{Code: 441, Message: "posting failed"}
	ErrAuthRequired        = &Error{Code: 480, Message: "authentication required"}
	ErrAuthRejected        = &Error{Code: 481, Message: "authentication rejected"}
	ErrServiceUnavailable  = &Error{Code: 502, Message: "service unavailable"}
	ErrCRCMismatch         = errors.New("nntp: yEnc CRC mismatch")
	ErrBodyCorrupt         = errors.New("nntp: corrupt article body")
	ErrProtocolDesync      = errors.New("nntp: protocol desync: expected status line, got binary data")
	ErrQuotaExceeded       = errors.New("nntp: download quota exceeded")
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
