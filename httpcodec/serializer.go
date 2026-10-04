package httpcodec

import (
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"
)

// CasePolicy 控制序列化时头部字段名的大小写。
type CasePolicy int

const (
	CasePreserve  CasePolicy = iota // 保持原样
	CaseLower                       // 全部小写
	CaseCanonical                   // 每个 - 分段首字母大写
)

// OrderPolicy 控制序列化时头部字段的排列顺序。
type OrderPolicy int

const (
	OrderPreserve OrderPolicy = iota // 保持原顺序
	OrderSort                        // 按字段名排序（稳定）
)

// Serializer 把报文序列化到 io.Writer。
// 零配置下保持头部大小写与顺序；Case/Order 可按策略调整。
type Serializer struct {
	w     io.Writer
	Case  CasePolicy
	Order OrderPolicy
}

// NewSerializer 创建一个序列化器，默认保持头部大小写与顺序。
func NewSerializer(w io.Writer) *Serializer {
	return &Serializer{w: w}
}

// Message 是待序列化的完整报文。Body 为流式读取，不会被整体载入内存。
type Message struct {
	Head    Head
	Body    io.Reader // 可为 nil
	Trailer []Field   // 仅 DelimChunked 时输出
}

// Write 序列化一条报文。Head.Delim 决定报文体编码方式：
//   - DelimChunked：分块编码，绝不改写为 Content-Length 形式；
//   - DelimLength：按 Head.Length 声明 Content-Length，Body 必须恰好提供这么多字节；
//   - DelimNone：无报文体，忽略 Body；
//   - DelimClose：透传 Body，由连接关闭定界（仅响应）。
//
// 头部中原有的 Content-Length / Transfer-Encoding 会被按 Delim 重写，
// 保证序列化结果自洽且可被自身解析回等价报文。
func (s *Serializer) Write(m *Message) error {
	var b strings.Builder
	h := &m.Head
	if h.Kind == Request {
		fmt.Fprintf(&b, "%s %s %s\r\n", h.Method, h.Target, h.Proto)
	} else {
		reason := h.Reason
		if reason == "" {
			reason = statusReason(h.StatusCode)
		}
		fmt.Fprintf(&b, "%s %d %s\r\n", h.Proto, h.StatusCode, reason)
	}

	fields := s.framedFields(h)
	for _, f := range fields {
		b.WriteString(s.applyCase(f.Name))
		b.WriteString(": ")
		b.WriteString(f.Value)
		b.WriteString("\r\n")
	}
	b.WriteString("\r\n")
	if _, err := io.WriteString(s.w, b.String()); err != nil {
		return err
	}

	switch h.Delim {
	case DelimNone:
		return nil
	case DelimLength:
		return s.writeLengthBody(m.Body, h.Length)
	case DelimChunked:
		return s.writeChunkedBody(m.Body, m.Trailer)
	case DelimClose:
		_, err := io.Copy(s.w, orEmpty(m.Body))
		return err
	}
	return nil
}

// framedFields 按 Delim 重写定界相关头部，并应用排序策略。
func (s *Serializer) framedFields(h *Head) []Field {
	var out []Field
	for _, f := range h.Header {
		if strings.EqualFold(f.Name, "Content-Length") ||
			strings.EqualFold(f.Name, "Transfer-Encoding") {
			continue // 由 Delim 统一重写
		}
		out = append(out, f)
	}
	switch h.Delim {
	case DelimLength:
		out = append(out, Field{"Content-Length", strconv.FormatInt(h.Length, 10)})
	case DelimChunked:
		out = append(out, Field{"Transfer-Encoding", "chunked"})
	}
	if s.Order == OrderSort {
		sort.SliceStable(out, func(i, j int) bool {
			return strings.ToLower(out[i].Name) < strings.ToLower(out[j].Name)
		})
	}
	return out
}

func (s *Serializer) applyCase(name string) string {
	switch s.Case {
	case CaseLower:
		return strings.ToLower(name)
	case CaseCanonical:
		return canonicalName(name)
	}
	return name
}

func (s *Serializer) writeLengthBody(r io.Reader, n int64) error {
	written, err := io.CopyN(s.w, orEmpty(r), n)
	if err == io.EOF && written == n {
		err = nil
	}
	if err != nil {
		return fmt.Errorf("httpcodec: body shorter than declared length: %w", err)
	}
	return nil
}

// writeChunkedBody 以 32KiB 为块上限流式写出分块编码报文体。
func (s *Serializer) writeChunkedBody(r io.Reader, trailer []Field) error {
	buf := make([]byte, 32*1024)
	for {
		n, err := orEmpty(r).Read(buf)
		if n > 0 {
			if _, werr := fmt.Fprintf(s.w, "%x\r\n", n); werr != nil {
				return werr
			}
			if _, werr := s.w.Write(buf[:n]); werr != nil {
				return werr
			}
			if _, werr := io.WriteString(s.w, "\r\n"); werr != nil {
				return werr
			}
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
	}
	if _, err := io.WriteString(s.w, "0\r\n"); err != nil {
		return err
	}
	for _, f := range trailer {
		if _, err := fmt.Fprintf(s.w, "%s: %s\r\n", s.applyCase(f.Name), f.Value); err != nil {
			return err
		}
	}
	_, err := io.WriteString(s.w, "\r\n")
	return err
}

func orEmpty(r io.Reader) io.Reader {
	if r == nil {
		return strings.NewReader("")
	}
	return r
}

// canonicalName 把字段名转为每个 - 分段首字母大写的形式。
func canonicalName(name string) string {
	b := []byte(strings.ToLower(name))
	upper := true
	for i, c := range b {
		if upper && c >= 'a' && c <= 'z' {
			b[i] = c - 'a' + 'A'
		}
		upper = c == '-'
	}
	return string(b)
}

func statusReason(code int) string {
	switch code {
	case 200:
		return "OK"
	case 201:
		return "Created"
	case 204:
		return "No Content"
	case 301:
		return "Moved Permanently"
	case 302:
		return "Found"
	case 304:
		return "Not Modified"
	case 400:
		return "Bad Request"
	case 404:
		return "Not Found"
	case 500:
		return "Internal Server Error"
	}
	return "Status"
}
