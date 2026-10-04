package httpcodec

// HeaderCasePolicy controls how header field names are written by the Writer.
type HeaderCasePolicy int

const (
	// CasePreserve writes header names exactly as stored.
	CasePreserve HeaderCasePolicy = iota
	// CaseLower writes header names in lower case.
	CaseLower
	// CaseCanonical writes header names in MIME canonical form (e.g. Content-Type).
	CaseCanonical
)

// HeaderOrderPolicy controls the order of header fields written by the Writer.
type HeaderOrderPolicy int

const (
	// OrderPreserve keeps the original insertion order.
	OrderPreserve HeaderOrderPolicy = iota
	// OrderSort sorts header fields by name (byte-wise, stable for duplicates).
	OrderSort
)

// Config carries safety limits and serialization policies.
// A zero Config is invalid for use; use DefaultConfig() as a baseline.
type Config struct {
	// MaxHeaderLineLen bounds a single header line (name + value, excluding CRLF).
	MaxHeaderLineLen int
	// MaxHeaderBytes bounds the total size of the header section (request/status
	// line plus all header lines, including CRLFs).
	MaxHeaderBytes int
	// MaxChunkCount bounds the number of chunks in a chunked body.
	MaxChunkCount int
	// MaxBodyBytes bounds the cumulative decoded body size of one message.
	MaxBodyBytes int64
	// MaxChunkSize bounds a single chunk's declared size.
	MaxChunkSize int64

	// HeaderCase selects the case policy used by the Writer.
	HeaderCase HeaderCasePolicy
	// HeaderOrder selects the ordering policy used by the Writer.
	HeaderOrder HeaderOrderPolicy
}

// DefaultConfig returns a Config with conservative defaults.
func DefaultConfig() Config {
	return Config{
		MaxHeaderLineLen: 8 << 10,
		MaxHeaderBytes:   64 << 10,
		MaxChunkCount:    1 << 16,
		MaxBodyBytes:     64 << 20,
		MaxChunkSize:     8 << 20,
		HeaderCase:       CasePreserve,
		HeaderOrder:      OrderPreserve,
	}
}

func (c Config) withDefaults() Config {
	d := DefaultConfig()
	if c.MaxHeaderLineLen <= 0 {
		c.MaxHeaderLineLen = d.MaxHeaderLineLen
	}
	if c.MaxHeaderBytes <= 0 {
		c.MaxHeaderBytes = d.MaxHeaderBytes
	}
	if c.MaxChunkCount <= 0 {
		c.MaxChunkCount = d.MaxChunkCount
	}
	if c.MaxBodyBytes <= 0 {
		c.MaxBodyBytes = d.MaxBodyBytes
	}
	if c.MaxChunkSize <= 0 {
		c.MaxChunkSize = d.MaxChunkSize
	}
	return c
}
