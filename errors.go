package tephra

import (
	"errors"
	"fmt"

	"github.com/tephradb/tephra-go/internal/tephrapb"
)

// ErrClosed is returned when an operation is attempted on a Client (or one of its streams)
// that has already been closed.
var ErrClosed = errors.New("tephra: client is closed")

// ErrorCode is a protocol error code from the server, decoupled from the wire enum so callers
// do not depend on the generated protobuf type. Unrecognized or unspecified wire codes decode
// to ErrCodeUnknown (the wire enum is open), which is also the zero value.
type ErrorCode int

const (
	// ErrCodeUnknown is the unspecified code, or any code this build does not recognize.
	ErrCodeUnknown ErrorCode = iota
	// ErrCodeConflict means an append's boundary check failed: an event matching the condition
	// landed after the observed position. Rebuild the decision model and retry. See
	// ServerError.ConflictPosition.
	ErrCodeConflict
	// ErrCodeAlreadyExists means an append's FailIfExists existence check matched: the guarded
	// event already exists. Distinct from ErrCodeConflict so a client can treat it as "already
	// applied" (a no-op) rather than "rebuild and retry".
	ErrCodeAlreadyExists
	// ErrCodeAfterBeyondTip means a read cursor was past the server's durable tip.
	ErrCodeAfterBeyondTip
	// ErrCodeEmpty means an append carried zero events.
	ErrCodeEmpty
	// ErrCodeTooLarge means a frame or payload exceeded the server's limit.
	ErrCodeTooLarge
	// ErrCodeBadRequest means the request was malformed (for example an invalid name).
	ErrCodeBadRequest
	// ErrCodeInternal means the server hit an internal failure.
	ErrCodeInternal
	// ErrCodeShutdown means the server is shutting down.
	ErrCodeShutdown
	// ErrCodeUnauthenticated means the connection failed authentication: a missing or invalid
	// bearer token in the opening Hello (see WithAuthToken).
	ErrCodeUnauthenticated
)

func (c ErrorCode) String() string {
	switch c {
	case ErrCodeConflict:
		return "conflict"
	case ErrCodeAlreadyExists:
		return "already_exists"
	case ErrCodeAfterBeyondTip:
		return "after_beyond_tip"
	case ErrCodeEmpty:
		return "empty"
	case ErrCodeTooLarge:
		return "too_large"
	case ErrCodeBadRequest:
		return "bad_request"
	case ErrCodeInternal:
		return "internal"
	case ErrCodeShutdown:
		return "shutdown"
	case ErrCodeUnauthenticated:
		return "unauthenticated"
	default:
		return "unknown"
	}
}

// ServerError is an error response returned by the server. Retryable distinguishes an advisory
// same-batch append conflict (safe to retry) from a durable one (terminal), for either
// ErrCodeConflict or ErrCodeAlreadyExists. ConflictPosition is set only for a durable append
// conflict of either kind.
type ServerError struct {
	Code             ErrorCode
	Message          string
	Retryable        bool
	ConflictPosition *Position
}

func (e *ServerError) Error() string {
	return fmt.Sprintf("tephra: server error (%s, retryable=%t): %s", e.Code, e.Retryable, e.Message)
}

// ProtocolError means the peer sent something that does not fit the protocol: the wrong frame
// kind for a request, a response for an unexpected request id, or an event that fails to decode.
type ProtocolError struct {
	Message string
}

func (e *ProtocolError) Error() string {
	return "tephra: protocol error: " + e.Message
}

func protocolErrorf(format string, args ...any) *ProtocolError {
	return &ProtocolError{Message: fmt.Sprintf(format, args...)}
}

// FrameTooLargeError means a frame's length exceeded the configured maximum. On read it is
// reported before the body is allocated; on write, before any byte reaches the wire.
type FrameTooLargeError struct {
	Len uint32
	Max uint32
}

func (e *FrameTooLargeError) Error() string {
	return fmt.Sprintf("tephra: frame length %d exceeds the maximum of %d", e.Len, e.Max)
}

// ConnError reports that a connection failed with requests in flight. Every request outstanding
// on the socket is failed with this error so no caller hangs. The underlying cause (an I/O error,
// or an unattributed server error captured from the wire) is available via errors.Unwrap.
type ConnError struct {
	Reason string
	cause  error
}

func (e *ConnError) Error() string {
	if e.cause != nil {
		return "tephra: " + e.Reason + ": " + e.cause.Error()
	}
	return "tephra: " + e.Reason
}

func (e *ConnError) Unwrap() error { return e.cause }

// errorCodeFromPB maps a wire error code to the public ErrorCode.
func errorCodeFromPB(code tephrapb.ErrorCode) ErrorCode {
	switch code {
	case tephrapb.ErrorCode_ERROR_CODE_CONFLICT:
		return ErrCodeConflict
	case tephrapb.ErrorCode_ERROR_CODE_ALREADY_EXISTS:
		return ErrCodeAlreadyExists
	case tephrapb.ErrorCode_ERROR_CODE_AFTER_BEYOND_TIP:
		return ErrCodeAfterBeyondTip
	case tephrapb.ErrorCode_ERROR_CODE_EMPTY:
		return ErrCodeEmpty
	case tephrapb.ErrorCode_ERROR_CODE_TOO_LARGE:
		return ErrCodeTooLarge
	case tephrapb.ErrorCode_ERROR_CODE_BAD_REQUEST:
		return ErrCodeBadRequest
	case tephrapb.ErrorCode_ERROR_CODE_INTERNAL:
		return ErrCodeInternal
	case tephrapb.ErrorCode_ERROR_CODE_SHUTDOWN:
		return ErrCodeShutdown
	case tephrapb.ErrorCode_ERROR_CODE_UNAUTHENTICATED:
		return ErrCodeUnauthenticated
	default:
		return ErrCodeUnknown
	}
}

// serverErrorFromPB builds a ServerError from a wire error response.
func serverErrorFromPB(e *tephrapb.ErrorResponse) *ServerError {
	se := &ServerError{
		Code:      errorCodeFromPB(e.GetCode()),
		Message:   e.GetMessage(),
		Retryable: e.GetRetryable(),
	}
	if e.ConflictPosition != nil {
		p := Position(*e.ConflictPosition)
		se.ConflictPosition = &p
	}
	return se
}
