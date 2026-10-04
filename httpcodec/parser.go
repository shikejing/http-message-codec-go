package httpcodec

import (
	"fmt"
	"io"
	"strconv"
	"strings"
)

// parserState 是增量解析状态机的内部状态。
type parserState int

const (
	stateStartLine parserState = iota
	stateHeaders
	stateBodyLength
	stateChunkSize
	stateChunkData
	stateChunkCRLF
	stateTrailer
	stateClosed // 由连接关闭定界的报文体
	stateDone   // 报文读完，待发出结束事件
	stateEnd    // 结束事件已发出，待复位
)

// Parser 是单条连接上的增量报文解析器。
// 每个连接应使用独立的 Parser；Config 可在多个 Parser 间共享。
// Parser 不是并发安全的，但不同 Parser 之间互不影响。
type Parser struct {
	kind   Kind
	cfg    Config
	buf    []byte // 未消费的输入
	offset int64  // 已消费字节的全局偏移（用于错误定位）
	state  parserState

	head    Head
	chunk   []byte // 最近一次 EventBody 的字节
	trailer []Field
	end     EndReason

	bodyLeft  int64 // DelimLength 剩余字节数
	bodySeen  int64 // 已输出的报文体字节数
	chunkLeft int   // 当前分块剩余字节数
	chunks    int   // 已见分块数
	hdrBytes  int   // 当前报文头部区已消费字节数
	eof       bool  // FeedEOF 已调用
}

// NewParser 创建一个解析器，kind 指明该连接上流动的是请求还是响应报文。
func NewParser(kind Kind, cfg Config) *Parser {
	return &Parser{kind: kind, cfg: cfg.withDefaults(), state: stateStartLine}
}

// Feed 向解析器追加一段输入字节，可以任意大小。
func (p *Parser) Feed(b []byte) { p.buf = append(p.buf, b...) }

// FeedEOF 告知解析器连接已关闭，之后不应再 Feed。
func (p *Parser) FeedEOF() { p.eof = true }

// Next 推进状态机一步，返回当前事件。
//   - EventNeedData：缓冲不足，需要继续 Feed；若已 FeedEOF 且处于
//     报文边界，返回 io.EOF 表示连接干净结束；若处于报文中间，
//     返回 ErrTruncated 错误。
//   - 其他事件通过 Head()/BodyChunk()/Trailer()/EndReason() 取数据。
//
// 出错后缓冲区保留未消费字节，可调用 Reset 丢弃后从下一条报文恢复。
func (p *Parser) Next() (Event, error) {
	p.chunk = nil
	switch p.state {
	case stateStartLine:
		return p.parseStartLine()
	case stateHeaders:
		return p.parseHeaders()
	case stateBodyLength:
		return p.parseBodyLength()
	case stateChunkSize:
		return p.parseChunkSize()
	case stateChunkData:
		return p.parseChunkData()
	case stateChunkCRLF:
		return p.parseChunkCRLF()
	case stateTrailer:
		return p.parseTrailer()
	case stateClosed:
		return p.parseBodyClose()
	case stateDone:
		return p.finishMessage()
	case stateEnd:
		// 上一条报文已结束，为下一条报文复位（保留残余缓冲）。
		p.resetMessage()
		return p.Next()
	}
	return Event{Type: EventNeedData}, nil
}

// Head 在 EventHead 之后返回报文头部。
func (p *Parser) Head() *Head { return &p.head }

// BodyChunk 在 EventBody 之后返回本次报文体片段。
// 返回的切片在下一次 Next 调用前有效。
func (p *Parser) BodyChunk() []byte { return p.chunk }

// Trailer 在 EventTrailer 之后返回尾部字段。
func (p *Parser) Trailer() []Field { return p.trailer }

// EndReason 在 EventMessageEnd 之后返回本条报文的结束原因。
func (p *Parser) EndReason() EndReason { return p.end }

// Reset 丢弃当前报文的全部残留状态与缓冲字节，
// 用于出错后从下一条报文边界恢复。调用方需保证后续 Feed 的字节
// 确实从下一条报文开始。
func (p *Parser) Reset() {
	p.buf = p.buf[:0]
	p.eof = false
	p.resetMessage()
}

// resetMessage 复位单条报文的状态，保留缓冲字节供下一条报文使用。
func (p *Parser) resetMessage() {
	p.state = stateStartLine
	p.head = Head{}
	p.chunk = nil
	p.trailer = nil
	p.end = EndNone
	p.bodyLeft, p.bodySeen, p.chunkLeft, p.chunks = 0, 0, 0, 0
	p.hdrBytes = 0
}

// consume 丢弃 buf 前 n 个字节并推进全局偏移。
func (p *Parser) consume(n int) {
	p.buf = p.buf[n:]
	p.offset += int64(n)
}

// readLine 取出一行（不含结尾 CRLF）。ok=false 表示数据不足。
// 严格只认 CRLF；裸 LF 视为畸形。
func (p *Parser) readLine(max int) (line []byte, ok bool, err error) {
	idx := indexCRLF(p.buf)
	if idx < 0 {
		if len(p.buf) > max {
			return nil, false, p.fail(ErrLineTooLong, "line exceeds configured limit")
		}
		if i := indexByte(p.buf, '\n'); i >= 0 {
			return nil, false, p.fail(ErrMalformedHeader, "bare LF in line ending")
		}
		if p.eof && len(p.buf) > 0 {
			return nil, false, p.fail(ErrTruncated, "connection closed mid-line")
		}
		return nil, false, nil
	}
	if idx > max {
		return nil, false, p.fail(ErrLineTooLong, "line exceeds configured limit")
	}
	line = p.buf[:idx]
	p.consume(idx + 2)
	return line, true, nil
}

func (p *Parser) parseStartLine() (Event, error) {
	if len(p.buf) == 0 {
		if p.eof {
			return Event{}, io.EOF // 报文边界上的干净结束
		}
		return Event{Type: EventNeedData}, nil
	}
	line, ok, err := p.readLine(p.cfg.MaxStartLine)
	if err != nil {
		return Event{}, err
	}
	if !ok {
		return Event{Type: EventNeedData}, nil
	}
	h := Head{Kind: p.kind, Proto: "HTTP/1.1", Delim: DelimNone}
	if p.kind == Request {
		// METHOD SP request-target SP HTTP/x.y
		parts := strings.Split(string(line), " ")
		if len(parts) != 3 || !isToken(parts[0]) || parts[1] == "" ||
			!strings.HasPrefix(parts[2], "HTTP/1.") {
			return Event{}, p.fail(ErrMalformedStartLine, "bad request line")
		}
		h.Method, h.Target, h.Proto = parts[0], parts[1], parts[2]
	} else {
		// HTTP/x.y SP status SP reason
		s := string(line)
		if !strings.HasPrefix(s, "HTTP/1.") || len(s) < 9 || s[8] != ' ' {
			return Event{}, p.fail(ErrMalformedStartLine, "bad status line")
		}
		rest := s[9:]
		code := rest
		if i := strings.IndexByte(rest, ' '); i >= 0 {
			code, h.Reason = rest[:i], rest[i+1:]
		}
		n, cerr := strconv.Atoi(code)
		if cerr != nil || n < 100 || n > 999 {
			return Event{}, p.fail(ErrMalformedStartLine, "bad status code")
		}
		h.Proto, h.StatusCode = s[:8], n
	}
	p.head = h
	p.state = stateHeaders
	return p.Next()
}

func (p *Parser) parseHeaders() (Event, error) {
	for {
		// 已消费的头部字节 + 尚未消费的缓冲，合计不得超过上限。
		if p.hdrBytes+len(p.buf) > p.cfg.MaxHeaderBytes {
			return Event{}, p.fail(ErrHeaderTooLarge, "header section exceeds configured limit")
		}
		line, ok, err := p.readLine(p.cfg.MaxHeaderLine)
		if err != nil {
			return Event{}, err
		}
		if !ok {
			return Event{Type: EventNeedData}, nil
		}
		p.hdrBytes += len(line) + 2
		if len(line) == 0 {
			return p.finishHeaders()
		}
		f, err := parseField(line)
		if err != nil {
			return Event{}, p.fail(ErrMalformedHeader, err.Error())
		}
		p.head.Header = append(p.head.Header, f)
	}
}

// finishHeaders 在头部区结束时决定报文体定界方式与升级语义。
func (p *Parser) finishHeaders() (Event, error) {
	h := &p.head
	var teSeen, clSeen bool
	var clValue int64
	for _, f := range h.Header {
		switch {
		case strings.EqualFold(f.Name, "Transfer-Encoding"):
			teSeen = true
			if !isChunkedTE(f.Value) {
				return Event{}, p.fail(ErrMalformedHeader,
					"unsupported transfer-encoding: "+f.Value)
			}
		case strings.EqualFold(f.Name, "Content-Length"):
			n, err := strconv.ParseInt(strings.TrimSpace(f.Value), 10, 64)
			if err != nil || n < 0 {
				return Event{}, p.fail(ErrMalformedHeader,
					"invalid content-length: "+f.Value)
			}
			if clSeen && n != clValue {
				return Event{}, p.fail(ErrConflictingLength,
					"conflicting content-length values")
			}
			clSeen, clValue = true, n
		case strings.EqualFold(f.Name, "Upgrade"):
			h.Upgrade = true
		}
	}
	if teSeen && clSeen {
		return Event{}, p.fail(ErrConflictingLength,
			"both transfer-encoding and content-length present")
	}
	switch {
	case teSeen:
		h.Delim = DelimChunked
		p.state = stateChunkSize
	case clSeen:
		h.Delim = DelimLength
		h.Length = clValue
		p.bodyLeft = clValue
		p.state = stateBodyLength
	case p.noBodyExpected():
		h.Delim = DelimNone
		p.state = stateDone
	case p.kind == Response:
		h.Delim = DelimClose
		p.state = stateClosed
	default:
		h.Delim = DelimNone
		p.state = stateDone
	}
	if h.Upgrade || (p.kind == Response && h.StatusCode == 101) ||
		(p.kind == Request && h.Method == "CONNECT") {
		h.Upgrade = true
	}
	return Event{Type: EventHead}, nil
}

// noBodyExpected 报告按语义该报文必然没有报文体。
func (p *Parser) noBodyExpected() bool {
	if p.kind == Response {
		c := p.head.StatusCode
		return (c >= 100 && c < 200) || c == 204 || c == 304
	}
	return false
}

func (p *Parser) parseBodyLength() (Event, error) {
	if p.bodyLeft == 0 {
		return p.finishMessage()
	}
	if len(p.buf) == 0 {
		return p.needBodyData()
	}
	n := int64(len(p.buf))
	if n > p.bodyLeft {
		n = p.bodyLeft // 多出的字节属于下一条报文，不得越界
	}
	if err := p.accountBody(n); err != nil {
		return Event{}, err
	}
	p.chunk = p.buf[:n]
	p.consume(int(n))
	p.bodyLeft -= n
	return Event{Type: EventBody}, nil
}

func (p *Parser) parseChunkSize() (Event, error) {
	line, ok, err := p.readLine(p.cfg.MaxHeaderLine)
	if err != nil {
		return Event{}, err
	}
	if !ok {
		return Event{Type: EventNeedData}, nil
	}
	s := string(line)
	if i := strings.IndexByte(s, ';'); i >= 0 {
		s = s[:i] // 忽略分块扩展
	}
	s = strings.TrimSpace(s)
	if s == "" || !isHex(s) {
		return Event{}, p.fail(ErrBadChunk, "invalid chunk size: "+string(line))
	}
	size, err := strconv.ParseInt(s, 16, 63)
	if err != nil {
		return Event{}, p.fail(ErrBadChunk, "chunk size out of range")
	}
	p.chunks++
	if p.chunks > p.cfg.MaxChunks {
		return Event{}, p.fail(ErrTooManyChunks, "chunk count exceeds configured limit")
	}
	if size == 0 {
		p.state = stateTrailer
		return p.Next()
	}
	p.chunkLeft = int(size)
	p.state = stateChunkData
	return p.Next()
}

func (p *Parser) parseChunkData() (Event, error) {
	if len(p.buf) == 0 {
		return p.needBodyData()
	}
	n := p.chunkLeft
	if len(p.buf) < n {
		n = len(p.buf)
	}
	if err := p.accountBody(int64(n)); err != nil {
		return Event{}, err
	}
	p.chunk = p.buf[:n]
	p.consume(n)
	p.chunkLeft -= n
	if p.chunkLeft == 0 {
		p.state = stateChunkCRLF
	}
	return Event{Type: EventBody}, nil
}

func (p *Parser) parseChunkCRLF() (Event, error) {
	if len(p.buf) < 2 {
		if p.eof {
			return Event{}, p.fail(ErrTruncated, "connection closed after chunk data")
		}
		return Event{Type: EventNeedData}, nil
	}
	if p.buf[0] != '\r' || p.buf[1] != '\n' {
		return Event{}, p.fail(ErrBadChunk, "missing CRLF after chunk data")
	}
	p.consume(2)
	p.state = stateChunkSize
	return p.Next()
}

func (p *Parser) parseTrailer() (Event, error) {
	for {
		line, ok, err := p.readLine(p.cfg.MaxHeaderLine)
		if err != nil {
			return Event{}, err
		}
		if !ok {
			return Event{Type: EventNeedData}, nil
		}
		if len(line) == 0 {
			if len(p.trailer) > 0 {
				p.state = stateDone
				return Event{Type: EventTrailer}, nil
			}
			return p.finishMessage()
		}
		f, err := parseField(line)
		if err != nil {
			return Event{}, p.fail(ErrMalformedHeader, "trailer: "+err.Error())
		}
		p.trailer = append(p.trailer, f)
	}
}

// parseBodyClose 处理以连接关闭为边界的报文体。
func (p *Parser) parseBodyClose() (Event, error) {
	if len(p.buf) > 0 {
		if err := p.accountBody(int64(len(p.buf))); err != nil {
			return Event{}, err
		}
		p.chunk = p.buf
		p.consume(len(p.buf))
		return Event{Type: EventBody}, nil
	}
	if p.eof {
		p.end = EndClose
		p.state = stateEnd
		return Event{Type: EventMessageEnd}, nil
	}
	return Event{Type: EventNeedData}, nil
}

// finishMessage 结束当前报文，决定结束原因。
func (p *Parser) finishMessage() (Event, error) {
	if p.head.Upgrade {
		p.end = EndUpgrade
	} else {
		p.end = EndComplete
	}
	p.state = stateEnd
	return Event{Type: EventMessageEnd}, nil
}

// needBodyData 在报文体中间数据不足时区分截断与等待。
func (p *Parser) needBodyData() (Event, error) {
	if p.eof {
		return Event{}, p.fail(ErrTruncated, "connection closed mid-body")
	}
	return Event{Type: EventNeedData}, nil
}

// accountBody 累计报文体大小并执行上限检查。
func (p *Parser) accountBody(n int64) error {
	p.bodySeen += n
	if p.bodySeen > p.cfg.MaxBodyBytes {
		return p.fail(ErrBodyTooLarge, "body exceeds configured limit")
	}
	return nil
}

// parseField 解析一行头部字段，拒绝 obs-fold 与非法字段名。
func parseField(line []byte) (Field, error) {
	if line[0] == ' ' || line[0] == '\t' {
		return Field{}, fmt.Errorf("obsolete line folding not allowed")
	}
	i := indexByte(line, ':')
	if i <= 0 {
		return Field{}, fmt.Errorf("missing colon in header field")
	}
	name := string(line[:i])
	if !isToken(name) {
		return Field{}, fmt.Errorf("invalid header field name %q", name)
	}
	return Field{Name: name, Value: strings.Trim(string(line[i+1:]), " \t")}, nil
}

func isChunkedTE(v string) bool {
	for _, part := range strings.Split(v, ",") {
		if !strings.EqualFold(strings.TrimSpace(part), "chunked") {
			return false
		}
	}
	return true
}

func isToken(s string) bool {
	if s == "" {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		ok := c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' ||
			strings.IndexByte("!#$%&'*+-.^_`|~", c) >= 0
		if !ok {
			return false
		}
	}
	return true
}

func isHex(s string) bool {
	for i := 0; i < len(s); i++ {
		c := s[i]
		if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f' || c >= 'A' && c <= 'F') {
			return false
		}
	}
	return len(s) > 0
}

func indexCRLF(b []byte) int {
	for i := 0; i+1 < len(b); i++ {
		if b[i] == '\r' && b[i+1] == '\n' {
			return i
		}
	}
	return -1
}

func indexByte(b []byte, c byte) int {
	for i, x := range b {
		if x == c {
			return i
		}
	}
	return -1
}
