package httpcodec

// Delimiter identifies how a message body is framed on the wire.
type Delimiter int

const (
	// DelimNone: no body (e.g. GET request, 204 response).
	DelimNone Delimiter = iota
	// DelimContentLength: body length fixed by Content-Length.
	DelimContentLength
	// DelimChunked: body framed with chunked transfer coding.
	DelimChunked
	// DelimUntilClose: body extends to connection close (HTTP/1.0 style).
	DelimUntilClose
)

func (d Delimiter) String() string {
	switch d {
	case DelimNone:
		return "none"
	case DelimContentLength:
		return "content-length"
	case DelimChunked:
		return "chunked"
	case DelimUntilClose:
		return "until-close"
	}
	return "unknown"
}

// EndReason explains why a message body ended, so callers can distinguish
// a clean delimiter from connection close or protocol upgrade.
type EndReason int

const (
	// EndNone: message has no body.
	EndNone EndReason = iota
	// EndContentLength: declared length fully consumed.
	EndContentLength
	// EndChunked: terminal zero chunk (and trailers) consumed.
	EndChunked
	// EndConnectionClose: body terminated by EOF (read-until-close).
	EndConnectionClose
	// EndUpgrade: connection upgraded; remaining bytes belong to another protocol.
	EndUpgrade
)

func (r EndReason) String() string {
	switch r {
	case EndNone:
		return "no-body"
	case EndContentLength:
		return "content-length"
	case EndChunked:
		return "chunked-end"
	case EndConnectionClose:
		return "connection-close"
	case EndUpgrade:
		return "upgrade"
	}
	return "unknown"
}

// Message is the head of one HTTP message: start line plus headers.
// Body bytes are delivered separately as parser events.
type Message struct {
	IsRequest bool

	// Request fields.
	Method string
	Target string

	// Response fields.
	StatusCode   int
	ReasonPhrase string

	// Proto is e.g. "HTTP/1.1".
	Proto string

	Header  *Headers
	Trailer *Headers // populated after a chunked body completes; nil otherwise

	// Framing decided from the headers.
	Delimiter     Delimiter
	ContentLength int64 // valid when Delimiter == DelimContentLength
	End           EndReason
	Upgraded      bool // true when a 101/Upgrade handshake switched protocols
}
