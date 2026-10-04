package httpcodec

import (
	"errors"
	"io"
	"strings"
	"testing"
)

type msg struct {
	head    Head
	body    []byte
	trailer []Field
	end     EndReason
}

// collect 反复推进解析器直到需要更多数据，事件累积到 out 上。
// 报文可能跨多次 collect 调用，因此 out 由调用方持有。
func collect(p *Parser, out []msg) ([]msg, error) {
	for {
		ev, err := p.Next()
		if err != nil {
			return out, err
		}
		switch ev.Type {
		case EventNeedData:
			return out, nil
		case EventHead:
			h := *p.Head()
			h.Header = append([]Field(nil), h.Header...)
			out = append(out, msg{head: h})
		case EventBody:
			out[len(out)-1].body = append(out[len(out)-1].body, p.BodyChunk()...)
		case EventTrailer:
			out[len(out)-1].trailer = append([]Field(nil), p.Trailer()...)
		case EventMessageEnd:
			out[len(out)-1].end = p.EndReason()
		}
	}
}

// feedAll 把数据按给定片段依次喂给解析器，最后可选地标记 EOF。
// 报文边界上的 io.EOF（连接干净关闭）视为正常结束，不视为错误。
func feedAll(p *Parser, eof bool, chunks ...string) ([]msg, error) {
	var out []msg
	push := func() error {
		var err error
		out, err = collect(p, out)
		if errors.Is(err, io.EOF) {
			return nil
		}
		return err
	}
	for _, c := range chunks {
		p.Feed([]byte(c))
		if err := push(); err != nil {
			return out, err
		}
	}
	if eof {
		p.FeedEOF()
		if err := push(); err != nil {
			return out, err
		}
	}
	return out, nil
}

func splitEvery(s string, n int) []string {
	var out []string
	for i := 0; i < len(s); i += n {
		j := i + n
		if j > len(s) {
			j = len(s)
		}
		out = append(out, s[i:j])
	}
	return out
}

func TestRequestNoBody(t *testing.T) {
	p := NewParser(Request, Config{})
	ms, err := feedAll(p, true, "GET /index.html HTTP/1.1\r\nHost: example.com\r\n\r\n")
	if err != nil {
		t.Fatal(err)
	}
	if len(ms) != 1 {
		t.Fatalf("want 1 message, got %d", len(ms))
	}
	m := ms[0]
	if m.head.Method != "GET" || m.head.Target != "/index.html" || m.head.Proto != "HTTP/1.1" {
		t.Fatalf("bad start line: %+v", m.head)
	}
	if len(m.head.Header) != 1 || m.head.Header[0].Name != "Host" {
		t.Fatalf("bad headers: %+v", m.head.Header)
	}
	if m.head.Delim != DelimNone || m.end != EndComplete {
		t.Fatalf("bad delim/end: %v %v", m.head.Delim, m.end)
	}
}

func TestFragmentedFeed(t *testing.T) {
	raw := "POST /submit HTTP/1.1\r\nHost: a.b\r\nContent-Length: 11\r\n\r\nhello world"
	for _, size := range []int{1, 2, 3, 7, len(raw)} {
		p := NewParser(Request, Config{})
		ms, err := feedAll(p, true, splitEvery(raw, size)...)
		if err != nil {
			t.Fatalf("chunk size %d: %v", size, err)
		}
		if len(ms) != 1 || string(ms[0].body) != "hello world" {
			t.Fatalf("chunk size %d: %+v", size, ms)
		}
		if ms[0].head.Delim != DelimLength || ms[0].head.Length != 11 {
			t.Fatalf("chunk size %d: delim %+v", size, ms[0].head)
		}
	}
}

func TestChunkedWithTrailer(t *testing.T) {
	raw := "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n" +
		"5\r\nhello\r\n6;ext=1\r\n world\r\n0\r\nX-Checksum: abc\r\n\r\n"
	p := NewParser(Response, Config{})
	// 逐字节喂入，边界可能落在分块中间。
	ms, err := feedAll(p, true, splitEvery(raw, 1)...)
	if err != nil {
		t.Fatal(err)
	}
	if len(ms) != 1 || string(ms[0].body) != "hello world" {
		t.Fatalf("bad body: %+v", ms)
	}
	if len(ms[0].trailer) != 1 || ms[0].trailer[0].Name != "X-Checksum" ||
		ms[0].trailer[0].Value != "abc" {
		t.Fatalf("bad trailer: %+v", ms[0].trailer)
	}
	if ms[0].end != EndComplete {
		t.Fatalf("bad end: %v", ms[0].end)
	}
}

func TestPipelinedMessages(t *testing.T) {
	raw := "POST /a HTTP/1.1\r\nContent-Length: 3\r\n\r\nabc" +
		"GET /b HTTP/1.1\r\nHost: x\r\n\r\n"
	p := NewParser(Request, Config{})
	// 两条报文首尾相接一次喂入；第一条读尽后多出的字节不得越界。
	ms, err := feedAll(p, true, raw)
	if err != nil {
		t.Fatal(err)
	}
	if len(ms) != 2 {
		t.Fatalf("want 2 messages, got %d", len(ms))
	}
	if string(ms[0].body) != "abc" {
		t.Fatalf("first body over-read: %q", ms[0].body)
	}
	if ms[1].head.Method != "GET" || ms[1].head.Target != "/b" {
		t.Fatalf("second message corrupted: %+v", ms[1].head)
	}
}

func TestConflictingDelimiters(t *testing.T) {
	cases := []string{
		// TE 与 CL 同时出现
		"POST / HTTP/1.1\r\nContent-Length: 1\r\nTransfer-Encoding: chunked\r\n\r\n0\r\n\r\n",
		// 两个不同的 Content-Length
		"POST / HTTP/1.1\r\nContent-Length: 1\r\nContent-Length: 2\r\n\r\n",
	}
	for _, raw := range cases {
		p := NewParser(Request, Config{})
		_, err := feedAll(p, true, raw)
		pe, ok := err.(*ParseError)
		if !ok || pe.Reason != ErrConflictingLength {
			t.Fatalf("want ErrConflictingLength, got %v", err)
		}
	}
	// 相同的重复 Content-Length 合法。
	p := NewParser(Request, Config{})
	ms, err := feedAll(p, true,
		"POST / HTTP/1.1\r\nContent-Length: 2\r\nContent-Length: 2\r\n\r\nhi")
	if err != nil || len(ms) != 1 || string(ms[0].body) != "hi" {
		t.Fatalf("duplicate equal CL should pass: %v %+v", err, ms)
	}
}

func TestTruncated(t *testing.T) {
	// 报文体中间关闭。
	p := NewParser(Request, Config{})
	_, err := feedAll(p, true, "POST / HTTP/1.1\r\nContent-Length: 10\r\n\r\nabc")
	pe, ok := err.(*ParseError)
	if !ok || pe.Reason != ErrTruncated {
		t.Fatalf("want ErrTruncated, got %v", err)
	}
	// 头部中间关闭。
	p = NewParser(Request, Config{})
	_, err = feedAll(p, true, "GET / HTTP/1.1\r\nHost: x")
	if pe, ok = err.(*ParseError); !ok || pe.Reason != ErrTruncated {
		t.Fatalf("want ErrTruncated, got %v", err)
	}
	// 报文边界上关闭是干净的 io.EOF。
	p = NewParser(Request, Config{})
	if _, err = feedAll(p, true, "GET / HTTP/1.1\r\n\r\n"); err != nil {
		t.Fatalf("clean close at boundary: %v", err)
	}
	if _, err = p.Next(); !errors.Is(err, io.EOF) {
		t.Fatalf("want io.EOF, got %v", err)
	}
}

func TestMalformedInputs(t *testing.T) {
	cases := []struct {
		raw    string
		reason Reason
	}{
		{"GET  /x  HTTP/1.1\r\n\r\n", ErrMalformedStartLine},
		{"GET /x HTTP/1.1\r\nBadHeader\r\n\r\n", ErrMalformedHeader},
		{"GET /x HTTP/1.1\r\n Folded: 1\r\n\r\n", ErrMalformedHeader},
		{"GET /x HTTP/1.1\r\nBad Name: 1\r\n\r\n", ErrMalformedHeader},
		{"POST / HTTP/1.1\r\nTransfer-Encoding: chunked\r\n\r\nzz\r\nx\r\n", ErrBadChunk},
		{"POST / HTTP/1.1\r\nTransfer-Encoding: chunked\r\n\r\n1\r\nxXX", ErrBadChunk},
		{"POST / HTTP/1.1\r\nContent-Length: abc\r\n\r\n", ErrMalformedHeader},
	}
	for _, c := range cases {
		p := NewParser(Request, Config{})
		_, err := feedAll(p, true, c.raw)
		pe, ok := err.(*ParseError)
		if !ok || pe.Reason != c.reason {
			t.Fatalf("%q: want %v, got %v", c.raw, c.reason, err)
		}
		if pe.Offset < 0 {
			t.Fatalf("%q: offset not set", c.raw)
		}
	}
}

func TestLimits(t *testing.T) {
	// 单行超长。
	p := NewParser(Request, Config{MaxHeaderLine: 16})
	_, err := feedAll(p, true, "GET / HTTP/1.1\r\nX-Long: "+strings.Repeat("a", 64)+"\r\n\r\n")
	if pe, ok := err.(*ParseError); !ok || pe.Reason != ErrLineTooLong {
		t.Fatalf("want ErrLineTooLong, got %v", err)
	}
	// 头部总大小超限（逐字节喂入也要触发）。
	p = NewParser(Request, Config{MaxHeaderBytes: 64})
	var hdr strings.Builder
	hdr.WriteString("GET / HTTP/1.1\r\n")
	for hdr.Len() < 200 {
		hdr.WriteString("X-Pad: padding\r\n")
	}
	hdr.WriteString("\r\n")
	_, err = feedAll(p, true, splitEvery(hdr.String(), 3)...)
	if pe, ok := err.(*ParseError); !ok || pe.Reason != ErrHeaderTooLarge {
		t.Fatalf("want ErrHeaderTooLarge, got %v", err)
	}
	// 分块数量超限。
	p = NewParser(Request, Config{MaxChunks: 2})
	_, err = feedAll(p, true,
		"POST / HTTP/1.1\r\nTransfer-Encoding: chunked\r\n\r\n1\r\na\r\n1\r\nb\r\n1\r\nc\r\n0\r\n\r\n")
	if pe, ok := err.(*ParseError); !ok || pe.Reason != ErrTooManyChunks {
		t.Fatalf("want ErrTooManyChunks, got %v", err)
	}
	// 报文体超限。
	p = NewParser(Request, Config{MaxBodyBytes: 4})
	_, err = feedAll(p, true, "POST / HTTP/1.1\r\nContent-Length: 10\r\n\r\n0123456789")
	if pe, ok := err.(*ParseError); !ok || pe.Reason != ErrBodyTooLarge {
		t.Fatalf("want ErrBodyTooLarge, got %v", err)
	}
}

func TestCloseDelimitedResponse(t *testing.T) {
	raw := "HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\n\r\nstream-data-until-close"
	p := NewParser(Response, Config{})
	ms, err := feedAll(p, true, splitEvery(raw, 5)...)
	if err != nil {
		t.Fatal(err)
	}
	if len(ms) != 1 || string(ms[0].body) != "stream-data-until-close" {
		t.Fatalf("bad body: %+v", ms)
	}
	if ms[0].head.Delim != DelimClose || ms[0].end != EndClose {
		t.Fatalf("bad delim/end: %v %v", ms[0].head.Delim, ms[0].end)
	}
}

func TestUpgrade(t *testing.T) {
	// 101 响应 → 协议升级。
	p := NewParser(Response, Config{})
	ms, err := feedAll(p, false,
		"HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\n\r\n")
	if err != nil {
		t.Fatal(err)
	}
	if len(ms) != 1 || ms[0].end != EndUpgrade {
		t.Fatalf("want EndUpgrade, got %+v", ms)
	}
	// CONNECT 请求 → 隧道。
	p = NewParser(Request, Config{})
	ms, err = feedAll(p, false, "CONNECT example.com:443 HTTP/1.1\r\nHost: example.com\r\n\r\n")
	if err != nil {
		t.Fatal(err)
	}
	if len(ms) != 1 || ms[0].end != EndUpgrade {
		t.Fatalf("want EndUpgrade for CONNECT, got %+v", ms)
	}
}

func TestRecoverAfterError(t *testing.T) {
	p := NewParser(Request, Config{})
	_, err := feedAll(p, false, "GET / HTTP/1.1\r\nBadHeader\r\n\r\n")
	pe, ok := err.(*ParseError)
	if !ok || pe.Reason != ErrMalformedHeader {
		t.Fatalf("want ErrMalformedHeader, got %v", err)
	}
	// 丢弃残留，从下一条报文边界恢复。
	p.Reset()
	ms, err := feedAll(p, true, "GET /next HTTP/1.1\r\nHost: x\r\n\r\n")
	if err != nil {
		t.Fatal(err)
	}
	if len(ms) != 1 || ms[0].head.Target != "/next" {
		t.Fatalf("recovery failed: %+v", ms)
	}
}
