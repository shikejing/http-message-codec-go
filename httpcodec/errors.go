package httpcodec

import "fmt"

// Reason 是可区分的错误原因。
type Reason int

const (
	ErrMalformedStartLine Reason = iota // 起始行畸形
	ErrMalformedHeader                  // 头部字段畸形
	ErrLineTooLong                      // 单行超长
	ErrHeaderTooLarge                   // 头部总大小超限
	ErrBadChunk                         // 分块编码非法
	ErrTooManyChunks                    // 分块数量超限
	ErrBodyTooLarge                     // 报文体超限
	ErrConflictingLength                // Content-Length 与 Transfer-Encoding 冲突或自相矛盾
	ErrTruncated                        // 输入提前结束（截断）
)

// ParseError 携带错误原因与出错位置（相对输入流的字节偏移）。
type ParseError struct {
	Reason Reason
	Offset int64
	Detail string
}

func (e *ParseError) Error() string {
	return fmt.Sprintf("httpcodec: %v at offset %d: %s", e.Reason, e.Offset, e.Detail)
}

func (r Reason) String() string {
	switch r {
	case ErrMalformedStartLine:
		return "malformed start line"
	case ErrMalformedHeader:
		return "malformed header"
	case ErrLineTooLong:
		return "line too long"
	case ErrHeaderTooLarge:
		return "header section too large"
	case ErrBadChunk:
		return "bad chunk framing"
	case ErrTooManyChunks:
		return "too many chunks"
	case ErrBodyTooLarge:
		return "body too large"
	case ErrConflictingLength:
		return "conflicting message delimiters"
	case ErrTruncated:
		return "truncated message"
	}
	return "unknown error"
}

func (p *Parser) fail(r Reason, detail string) *ParseError {
	return &ParseError{Reason: r, Offset: p.offset, Detail: detail}
}
