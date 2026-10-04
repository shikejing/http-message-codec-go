package httpcodec

import "fmt"

// ErrorReason classifies parse failures so callers can distinguish causes.
type ErrorReason int

const (
	ReasonNone ErrorReason = iota
	// ReasonMalformedStartLine: request line or status line is not well formed.
	ReasonMalformedStartLine
	// ReasonMalformedHeader: a header line has no colon, bad name, or bad value.
	ReasonMalformedHeader
	// ReasonHeaderLineTooLong: a single header line exceeded MaxHeaderLineLen.
	ReasonHeaderLineTooLong
	// ReasonHeadersTooLarge: the header section exceeded MaxHeaderBytes.
	ReasonHeadersTooLarge
	// ReasonConflictingFraming: Content-Length and Transfer-Encoding disagree
	// or multiple conflicting Content-Length values are present.
	ReasonConflictingFraming
	// ReasonMalformedChunkSize: the chunk-size line is not valid hex.
	ReasonMalformedChunkSize
	// ReasonMalformedChunkEnd: chunk data is not followed by CRLF.
	ReasonMalformedChunkEnd
	// ReasonTooManyChunks: chunk count exceeded MaxChunkCount.
	ReasonTooManyChunks
	// ReasonChunkTooLarge: a chunk's declared size exceeded MaxChunkSize.
	ReasonChunkTooLarge
	// ReasonBodyTooLarge: cumulative body size exceeded MaxBodyBytes.
	ReasonBodyTooLarge
	// ReasonTruncated: the stream ended (EOF) in the middle of a message.
	ReasonTruncated
)

func (r ErrorReason) String() string {
	switch r {
	case ReasonMalformedStartLine:
		return "malformed start line"
	case ReasonMalformedHeader:
		return "malformed header"
	case ReasonHeaderLineTooLong:
		return "header line too long"
	case ReasonHeadersTooLarge:
		return "header section too large"
	case ReasonConflictingFraming:
		return "conflicting message framing"
	case ReasonMalformedChunkSize:
		return "malformed chunk size"
	case ReasonMalformedChunkEnd:
		return "malformed chunk terminator"
	case ReasonTooManyChunks:
		return "too many chunks"
	case ReasonChunkTooLarge:
		return "chunk too large"
	case ReasonBodyTooLarge:
		return "body too large"
	case ReasonTruncated:
		return "truncated message"
	}
	return "unknown"
}

// ParseError describes a parse failure with a reason and the absolute byte
// offset in the connection stream where the problem was detected.
type ParseError struct {
	Reason ErrorReason
	Offset int64
	Detail string
}

func (e *ParseError) Error() string {
	if e.Detail == "" {
		return fmt.Sprintf("httpcodec: %s at offset %d", e.Reason, e.Offset)
	}
	return fmt.Sprintf("httpcodec: %s at offset %d: %s", e.Reason, e.Offset, e.Detail)
}
