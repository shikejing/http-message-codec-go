package httpcodec

import (
	"bytes"
	"errors"
	"fmt"
	"strconv"
	"strings"
)

// EventKind identifies the type of a parser event.
type EventKind int

const (
	// EventHead: a message head (start line + headers) is complete.
	EventHead EventKind = iota
	// EventBody: a piece of decoded body data is available.
	EventBody
	// EventTrailers: trailer fields of a chunked body are complete. Emitted
	// for every chunked message, even when the trailer section is empty.
	EventTrailers
	// EventMessageEnd: the current message is fully consumed; Message.End
	// explains why the body ended.
	EventMessageEnd
)

// Event is one incremental parse result. Body payloads are copies and remain
// valid after the next Feed call.
type Event struct {
	Kind    EventKind
	Message *Message // set for EventHead and EventMessageEnd
	Body    []byte   // set for EventBody
	Trailer *Headers // set for EventTrailers
}

// ErrClosed is returned by Feed after Close was called.
var ErrClosed = errors.New("httpcodec: feed after close")

// Codec is an immutable factory; safe for concurrent use by many connections.
type Codec struct {
	cfg Config
}

// NewCodec builds a Codec from cfg, filling unspecified limits with defaults.
func NewCodec(cfg Config) *Codec {
	return &Codec{cfg: cfg.withDefaults()}
}

// Config returns the effective configuration.
func (c *Codec) Config() Config { return c.cfg }

// NewParser creates an independent Parser for one connection. Parsers from
// the same Codec never share mutable state.
func (c *Codec) NewParser() *Parser {
	return &Parser{cfg: c.cfg.withDefaults()}
}

// NewWriter builds a Writer from this Codec's configuration.
func (c *Codec) NewWriter() *Writer {
	return &Writer{cfg: c.cfg.withDefaults()}
}

type parserState int

const (
	stStartLine parserState = iota
	stHeaders
	stBodyFixed
	stBodyUntilClose
	stChunkSize
	stChunkData
	stChunkDataEnd
	stTrailers
	stUpgraded
)

// Parser incrementally decodes a byte stream into message events.
// A Parser belongs to a single connection and is not safe for concurrent use.
type Parser struct {
	cfg   Config
	buf   []byte
	off   int64 // absolute stream offset of buf[0]
	state parserState

	msg         *Message
	headerBytes int   // bytes consumed by the current header section
	bodyRemain  int64 // remaining bytes for DelimContentLength
	bodyTotal   int64 // cumulative decoded body bytes of current message
	chunkRemain int64 // remaining bytes of the current chunk
	chunkCount  int   // chunks seen in the current message

	err    *ParseError
	closed bool
}

// Feed appends data to the stream and returns all events that become
// decodable. data may contain any number of bytes, including parts of
// several pipelined messages; the undecodable remainder is buffered
// internally. After an error the parser enters the failed state: further
// Feed calls return the same error until Recover is called.
func (p *Parser) Feed(data []byte) ([]Event, error) {
	if p.err != nil {
		return nil, p.err
	}
	if p.closed {
		return nil, ErrClosed
	}
	if len(data) > 0 {
		p.buf = append(p.buf, data...)
	}
	var evts []Event
	if err := p.run(&evts); err != nil {
		if perr, ok := err.(*ParseError); ok {
			p.err = perr
		}
		return evts, err
	}
	if len(p.buf) == 0 {
		p.buf = nil // release backing array between messages
	}
	return evts, nil
}

// Close signals end of stream. A body delimited by connection close is
// completed with EndConnectionClose; any other incomplete message yields a
// ReasonTruncated error. Trailing garbage after the last complete message is
// reported as truncated rather than silently dropped.
func (p *Parser) Close() ([]Event, error) {
	if p.err != nil {
		return nil, p.err
	}
	p.closed = true
	var evts []Event
	switch p.state {
	case stStartLine:
		if len(p.buf) > 0 {
			err := &ParseError{Reason: ReasonTruncated, Offset: p.off,
				Detail: "stream ended with a partial start line"}
			p.err = err
			return nil, err
		}
		return nil, nil
	case stBodyUntilClose:
		if len(p.buf) > 0 {
			if p.bodyTotal+int64(len(p.buf)) > p.cfg.MaxBodyBytes {
				err := &ParseError{Reason: ReasonBodyTooLarge, Offset: p.off}
				p.err = err
				return evts, err
			}
			p.emitBody(&evts, len(p.buf))
		}
		p.endMessage(&evts, EndConnectionClose)
		return evts, nil
	case stUpgraded:
		return nil, nil
	default:
		err := &ParseError{Reason: ReasonTruncated, Offset: p.off,
			Detail: fmt.Sprintf("stream ended in state %v", p.stateName())}
		p.err = err
		return evts, err
	}
}

// Recover discards the failed message and every byte buffered for it, then
// resumes parsing at the next message boundary from subsequently fed data.
// Buffered bytes are never reinterpreted as a new message.
func (p *Parser) Recover() {
	p.buf = nil
	p.state = stStartLine
	p.msg = nil
	p.headerBytes = 0
	p.bodyRemain = 0
	p.bodyTotal = 0
	p.chunkRemain = 0
	p.chunkCount = 0
	p.err = nil
}

// Failed reports whether the parser is in the failed state awaiting Recover.
func (p *Parser) Failed() bool { return p.err != nil }

// Remaining returns the bytes buffered past the end of the last complete
// message. It is meaningful after an upgrade (EndUpgrade), where the
// remaining bytes belong to the switched protocol.
func (p *Parser) Remaining() []byte {
	out := make([]byte, len(p.buf))
	copy(out, p.buf)
	return out
}

func (p *Parser) stateName() string {
	switch p.state {
	case stStartLine:
		return "start-line"
	case stHeaders:
		return "headers"
	case stBodyFixed:
		return "fixed-body"
	case stBodyUntilClose:
		return "close-delimited-body"
	case stChunkSize:
		return "chunk-size"
	case stChunkData, stChunkDataEnd:
		return "chunk-data"
	case stTrailers:
		return "trailers"
	case stUpgraded:
		return "upgraded"
	}
	return "unknown"
}

// run executes the state machine until no more progress can be made.
func (p *Parser) run(evts *[]Event) error {
	for {
		progress, err := p.step(evts)
		if err != nil {
			return err
		}
		if !progress {
			return nil
		}
	}
}

func (p *Parser) consume(n int) {
	p.buf = p.buf[n:]
	p.off += int64(n)
}

// step performs one unit of work. It reports progress when it consumed bytes
// or advanced the state, guaranteeing run() terminates.
func (p *Parser) step(evts *[]Event) (bool, error) {
	switch p.state {
	case stStartLine:
		return p.stepStartLine(evts)
	case stHeaders:
		return p.stepHeaders(evts)
	case stBodyFixed:
		return p.stepBodyFixed(evts)
	case stBodyUntilClose:
		if len(p.buf) == 0 {
			return false, nil
		}
		n := len(p.buf)
		if p.bodyTotal+int64(n) > p.cfg.MaxBodyBytes {
			return false, &ParseError{Reason: ReasonBodyTooLarge, Offset: p.off}
		}
		return true, p.emitBodyErr(evts, n)
	case stChunkSize:
		return p.stepChunkSize(evts)
	case stChunkData:
		if len(p.buf) == 0 {
			return false, nil
		}
		n := int64(len(p.buf))
		if n > p.chunkRemain {
			n = p.chunkRemain
		}
		if p.bodyTotal+n > p.cfg.MaxBodyBytes {
			return false, &ParseError{Reason: ReasonBodyTooLarge, Offset: p.off}
		}
		p.emitBody(evts, int(n))
		p.chunkRemain -= n
		if p.chunkRemain == 0 {
			p.state = stChunkDataEnd
		}
		return true, nil
	case stChunkDataEnd:
		if len(p.buf) < 2 {
			return false, nil
		}
		if p.buf[0] != '\r' || p.buf[1] != '\n' {
			return false, &ParseError{Reason: ReasonMalformedChunkEnd, Offset: p.off,
				Detail: "chunk data not followed by CRLF"}
		}
		p.consume(2)
		p.state = stChunkSize
		return true, nil
	case stTrailers:
		return p.stepTrailers(evts)
	case stUpgraded:
		return false, nil
	}
	return false, nil
}

// findLine locates the next CRLF-terminated line. ok is false when more data
// is needed. A bare LF or an over-long line is an error.
func (p *Parser) findLine() (line []byte, ok bool, err error) {
	idx := bytes.Index(p.buf, []byte("\r\n"))
	if idx >= 0 {
		// Reject a bare LF before this CRLF.
		if lf := bytes.IndexByte(p.buf[:idx], '\n'); lf >= 0 {
			return nil, false, &ParseError{Reason: ReasonMalformedHeader,
				Offset: p.off + int64(lf), Detail: "bare LF in line"}
		}
		if idx > p.cfg.MaxHeaderLineLen {
			return nil, false, &ParseError{Reason: ReasonHeaderLineTooLong,
				Offset: p.off + int64(p.cfg.MaxHeaderLineLen)}
		}
		return p.buf[:idx], true, nil
	}
	if lf := bytes.IndexByte(p.buf, '\n'); lf >= 0 {
		return nil, false, &ParseError{Reason: ReasonMalformedHeader,
			Offset: p.off + int64(lf), Detail: "bare LF in line"}
	}
	if len(p.buf) > p.cfg.MaxHeaderLineLen {
		return nil, false, &ParseError{Reason: ReasonHeaderLineTooLong,
			Offset: p.off + int64(p.cfg.MaxHeaderLineLen)}
	}
	return nil, false, nil
}

func (p *Parser) countHeaderBytes(n int) error {
	p.headerBytes += n
	if p.headerBytes > p.cfg.MaxHeaderBytes {
		return &ParseError{Reason: ReasonHeadersTooLarge, Offset: p.off}
	}
	return nil
}

func (p *Parser) stepStartLine(evts *[]Event) (bool, error) {
	line, ok, err := p.findLine()
	if err != nil {
		return false, err
	}
	if !ok {
		return false, nil
	}
	if err := p.countHeaderBytes(len(line) + 2); err != nil {
		return false, err
	}
	msg, perr := parseStartLine(string(line))
	if perr != nil {
		perr.Offset += p.off
		return false, perr
	}
	p.consume(len(line) + 2)
	p.msg = msg
	p.state = stHeaders
	return true, nil
}

func (p *Parser) stepHeaders(evts *[]Event) (bool, error) {
	line, ok, err := p.findLine()
	if err != nil {
		return false, err
	}
	if !ok {
		return false, nil
	}
	if err := p.countHeaderBytes(len(line) + 2); err != nil {
		return false, err
	}
	p.consume(len(line) + 2)
	if len(line) == 0 {
		// End of header section: decide framing and emit the head.
		if err := p.decideFraming(); err != nil {
			return false, err
		}
		*evts = append(*evts, Event{Kind: EventHead, Message: p.msg})
		p.enterBody(evts)
		return true, nil
	}
	h, perr := parseHeaderLine(line)
	if perr != nil {
		perr.Offset += p.off - int64(len(line)+2)
		return false, perr
	}
	p.msg.Header.Add(h.Name, h.Value)
	return true, nil
}

func (p *Parser) stepBodyFixed(evts *[]Event) (bool, error) {
	if len(p.buf) == 0 {
		return false, nil
	}
	n := int64(len(p.buf))
	if n > p.bodyRemain {
		n = p.bodyRemain
	}
	if p.bodyTotal+n > p.cfg.MaxBodyBytes {
		return false, &ParseError{Reason: ReasonBodyTooLarge, Offset: p.off}
	}
	p.emitBody(evts, int(n))
	p.bodyRemain -= n
	if p.bodyRemain == 0 {
		p.endMessage(evts, EndContentLength)
	}
	return true, nil
}

func (p *Parser) stepChunkSize(evts *[]Event) (bool, error) {
	line, ok, err := p.findLine()
	if err != nil {
		return false, err
	}
	if !ok {
		return false, nil
	}
	size, perr := parseChunkSize(line)
	if perr != nil {
		perr.Offset += p.off
		return false, perr
	}
	p.consume(len(line) + 2)
	p.chunkCount++
	if p.chunkCount > p.cfg.MaxChunkCount {
		return false, &ParseError{Reason: ReasonTooManyChunks, Offset: p.off}
	}
	if size > p.cfg.MaxChunkSize {
		return false, &ParseError{Reason: ReasonChunkTooLarge, Offset: p.off,
			Detail: fmt.Sprintf("declared chunk size %d exceeds limit %d", size, p.cfg.MaxChunkSize)}
	}
	p.chunkRemain = size
	if size == 0 {
		p.msg.Trailer = &Headers{}
		p.state = stTrailers
	} else {
		p.state = stChunkData
	}
	return true, nil
}

func (p *Parser) stepTrailers(evts *[]Event) (bool, error) {
	line, ok, err := p.findLine()
	if err != nil {
		return false, err
	}
	if !ok {
		return false, nil
	}
	p.consume(len(line) + 2)
	if len(line) == 0 {
		*evts = append(*evts, Event{Kind: EventTrailers, Trailer: p.msg.Trailer})
		p.endMessage(evts, EndChunked)
		return true, nil
	}
	h, perr := parseHeaderLine(line)
	if perr != nil {
		perr.Offset += p.off - int64(len(line)+2)
		return false, perr
	}
	p.msg.Trailer.Add(h.Name, h.Value)
	return true, nil
}

// emitBody emits n buffered bytes as an EventBody and consumes them.
func (p *Parser) emitBody(evts *[]Event, n int) {
	b := make([]byte, n)
	copy(b, p.buf[:n])
	p.consume(n)
	p.bodyTotal += int64(n)
	*evts = append(*evts, Event{Kind: EventBody, Body: b})
}

// emitBodyErr adapts emitBody to a step signature returning error.
func (p *Parser) emitBodyErr(evts *[]Event, n int) error {
	p.emitBody(evts, n)
	return nil
}

// enterBody transitions from a completed head into the right body state and
// immediately finishes body-less messages.
func (p *Parser) enterBody(evts *[]Event) {
	p.bodyTotal = 0
	p.chunkCount = 0
	switch p.msg.Delimiter {
	case DelimNone:
		if p.msg.Upgraded {
			p.endMessage(evts, EndUpgrade)
			p.state = stUpgraded
			return
		}
		p.endMessage(evts, EndNone)
	case DelimContentLength:
		p.bodyRemain = p.msg.ContentLength
		if p.bodyRemain == 0 {
			p.endMessage(evts, EndContentLength)
			return
		}
		p.state = stBodyFixed
	case DelimChunked:
		p.state = stChunkSize
	case DelimUntilClose:
		p.state = stBodyUntilClose
	}
}

// endMessage finishes the current message, emits EventMessageEnd, resets
// per-message counters, and returns to the start-line state so pipelined
// bytes that follow belong to the next message. Callers may override the
// state afterwards (e.g. stUpgraded).
func (p *Parser) endMessage(evts *[]Event, end EndReason) {
	p.msg.End = end
	*evts = append(*evts, Event{Kind: EventMessageEnd, Message: p.msg})
	p.msg = nil
	p.headerBytes = 0
	p.bodyRemain = 0
	p.bodyTotal = 0
	p.chunkRemain = 0
	p.chunkCount = 0
	p.state = stStartLine
}

// decideFraming inspects the headers of p.msg and determines the body
// delimiter. Conflicting Content-Length and Transfer-Encoding are rejected.
func (p *Parser) decideFraming() error {
	m := p.msg
	te := transferCodings(m.Header)
	cl, clPresent, err := contentLengthValue(m.Header)
	if err != nil {
		err.Offset += p.off
		return err
	}
	if len(te) > 0 && clPresent {
		return &ParseError{Reason: ReasonConflictingFraming, Offset: p.off,
			Detail: "both Transfer-Encoding and Content-Length present"}
	}
	if len(te) > 0 {
		if len(te) != 1 || te[0] != "chunked" {
			return &ParseError{Reason: ReasonConflictingFraming, Offset: p.off,
				Detail: fmt.Sprintf("unsupported transfer coding %q", strings.Join(te, ", "))}
		}
		m.Delimiter = DelimChunked
		return nil
	}
	if !m.IsRequest {
		// Responses that never carry a body.
		if m.StatusCode == 101 {
			m.Delimiter = DelimNone
			m.Upgraded = true
			return nil
		}
		if (m.StatusCode >= 100 && m.StatusCode < 200) || m.StatusCode == 204 || m.StatusCode == 304 {
			m.Delimiter = DelimNone
			return nil
		}
	}
	if clPresent {
		m.Delimiter = DelimContentLength
		m.ContentLength = cl
		return nil
	}
	if m.IsRequest {
		m.Delimiter = DelimNone
	} else {
		m.Delimiter = DelimUntilClose
	}
	return nil
}

// transferCodings extracts lower-cased transfer codings, or nil if absent.
func transferCodings(h *Headers) []string {
	var out []string
	for _, v := range h.Values("Transfer-Encoding") {
		for _, part := range strings.Split(v, ",") {
			part = strings.ToLower(strings.TrimSpace(part))
			if part != "" {
				out = append(out, part)
			}
		}
	}
	return out
}

// contentLengthValue parses all Content-Length fields. Repeated identical
// values are accepted; differing values are a framing conflict.
func contentLengthValue(h *Headers) (int64, bool, *ParseError) {
	seen := false
	var value int64
	for _, line := range h.Values("Content-Length") {
		for _, part := range strings.Split(line, ",") {
			part = strings.TrimSpace(part)
			if part == "" {
				return 0, false, &ParseError{Reason: ReasonMalformedHeader,
					Detail: "empty Content-Length value"}
			}
			n, err := parseDecimal(part)
			if err != nil {
				return 0, false, &ParseError{Reason: ReasonMalformedHeader,
					Detail: fmt.Sprintf("invalid Content-Length %q", part)}
			}
			if seen && n != value {
				return 0, false, &ParseError{Reason: ReasonConflictingFraming,
					Detail: fmt.Sprintf("conflicting Content-Length values %d and %d", value, n)}
			}
			seen, value = true, n
		}
	}
	return value, seen, nil
}

func parseDecimal(s string) (int64, error) {
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return 0, fmt.Errorf("not a decimal number")
		}
	}
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return 0, err
	}
	return n, nil
}

// parseChunkSize parses a chunk-size line: hex digits optionally followed by
// ";" chunk extensions, which are ignored.
func parseChunkSize(line []byte) (int64, *ParseError) {
	s := string(line)
	if i := strings.IndexByte(s, ';'); i >= 0 {
		s = s[:i]
	}
	s = strings.TrimRight(s, " \t")
	if s == "" {
		return 0, &ParseError{Reason: ReasonMalformedChunkSize, Detail: "empty chunk size"}
	}
	if len(s) > 16 {
		return 0, &ParseError{Reason: ReasonMalformedChunkSize, Detail: "chunk size overflows int64"}
	}
	var n int64
	for i := 0; i < len(s); i++ {
		c := s[i]
		var d int64
		switch {
		case c >= '0' && c <= '9':
			d = int64(c - '0')
		case c >= 'a' && c <= 'f':
			d = int64(c-'a') + 10
		case c >= 'A' && c <= 'F':
			d = int64(c-'A') + 10
		default:
			return 0, &ParseError{Reason: ReasonMalformedChunkSize,
				Detail: fmt.Sprintf("invalid chunk size %q", s)}
		}
		n = n*16 + d
	}
	return n, nil
}

// parseStartLine parses a request or status line into a Message head.
func parseStartLine(line string) (*Message, *ParseError) {
	if strings.HasPrefix(line, "HTTP/") {
		return parseStatusLine(line)
	}
	return parseRequestLine(line)
}

func parseRequestLine(line string) (*Message, *ParseError) {
	bad := func(detail string) *ParseError {
		return &ParseError{Reason: ReasonMalformedStartLine, Detail: detail}
	}
	sp1 := strings.IndexByte(line, ' ')
	if sp1 <= 0 {
		return nil, bad("missing method or spaces in request line")
	}
	rest := line[sp1+1:]
	sp2 := strings.LastIndexByte(rest, ' ')
	if sp2 <= 0 {
		return nil, bad("malformed request line")
	}
	method, target, proto := line[:sp1], rest[:sp2], rest[sp2+1:]
	if !isToken(method) {
		return nil, bad(fmt.Sprintf("invalid method %q", method))
	}
	if target == "" || strings.ContainsAny(target, " \t") {
		return nil, bad(fmt.Sprintf("invalid request target %q", target))
	}
	if !validProto(proto) {
		return nil, bad(fmt.Sprintf("invalid protocol %q", proto))
	}
	return &Message{
		IsRequest: true,
		Method:    method,
		Target:    target,
		Proto:     proto,
		Header:    &Headers{},
	}, nil
}

func parseStatusLine(line string) (*Message, *ParseError) {
	bad := func(detail string) *ParseError {
		return &ParseError{Reason: ReasonMalformedStartLine, Detail: detail}
	}
	sp := strings.IndexByte(line, ' ')
	proto := line
	rest := ""
	if sp >= 0 {
		proto, rest = line[:sp], line[sp+1:]
	}
	if !validProto(proto) {
		return nil, bad(fmt.Sprintf("invalid protocol %q", proto))
	}
	if len(rest) < 3 {
		return nil, bad("missing status code")
	}
	code := 0
	for i := 0; i < 3; i++ {
		if rest[i] < '0' || rest[i] > '9' {
			return nil, bad(fmt.Sprintf("invalid status code %q", rest[:3]))
		}
		code = code*10 + int(rest[i]-'0')
	}
	reason := ""
	if len(rest) > 3 {
		if rest[3] != ' ' {
			return nil, bad("status code must be 3 digits")
		}
		reason = rest[4:]
	}
	return &Message{
		IsRequest:    false,
		StatusCode:   code,
		ReasonPhrase: reason,
		Proto:        proto,
		Header:       &Headers{},
	}, nil
}

func validProto(s string) bool {
	if len(s) != 8 || !strings.HasPrefix(s, "HTTP/") {
		return false
	}
	return isDigit(s[5]) && s[6] == '.' && isDigit(s[7])
}

func isDigit(c byte) bool { return c >= '0' && c <= '9' }

// isToken reports whether s is a valid HTTP token (tchar sequence).
func isToken(s string) bool {
	if s == "" {
		return false
	}
	for i := 0; i < len(s); i++ {
		if !isTchar(s[i]) {
			return false
		}
	}
	return true
}

func isTchar(c byte) bool {
	switch {
	case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9':
		return true
	}
	switch c {
	case '!', '#', '$', '%', '&', '\'', '*', '+', '-', '.', '^', '_', '`', '|', '~':
		return true
	}
	return false
}

// parseHeaderLine parses one header field line. obs-fold (line starting with
// SP/HTAB) is rejected, as are control characters in the value.
func parseHeaderLine(line []byte) (Header, *ParseError) {
	bad := func(detail string) *ParseError {
		return &ParseError{Reason: ReasonMalformedHeader, Detail: detail}
	}
	if len(line) == 0 {
		return Header{}, bad("empty header line")
	}
	if line[0] == ' ' || line[0] == '\t' {
		return Header{}, bad("obsolete line folding is not supported")
	}
	colon := bytes.IndexByte(line, ':')
	if colon <= 0 {
		return Header{}, bad("header line without colon")
	}
	name := string(line[:colon])
	if !isToken(name) {
		return Header{}, bad(fmt.Sprintf("invalid header name %q", name))
	}
	value := strings.Trim(string(line[colon+1:]), " \t")
	for i := 0; i < len(value); i++ {
		c := value[i]
		if c == '\t' || (c >= 0x20 && c != 0x7f) || c >= 0x80 {
			continue
		}
		return Header{}, bad(fmt.Sprintf("invalid byte 0x%02x in header value", c))
	}
	return Header{Name: name, Value: value}, nil
}
