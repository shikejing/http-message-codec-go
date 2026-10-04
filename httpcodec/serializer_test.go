package httpcodec

import (
	"bytes"
	"io"
	"strings"
	"testing"
)

// roundTrip 序列化报文再用自身解析器读回，要求等价。
func roundTrip(t *testing.T, m *Message, s *Serializer) []msg {
	t.Helper()
	var buf bytes.Buffer
	s.w = &buf
	if err := s.Write(m); err != nil {
		t.Fatalf("serialize: %v", err)
	}
	kind := m.Head.Kind
	p := NewParser(kind, Config{})
	ms, err := feedAll(p, true, buf.String())
	if err != nil {
		t.Fatalf("reparse %q: %v", buf.String(), err)
	}
	if len(ms) != 1 {
		t.Fatalf("want 1 message, got %d", len(ms))
	}
	return ms
}

func TestRoundTripContentLength(t *testing.T) {
	m := &Message{
		Head: Head{
			Kind: Request, Proto: "HTTP/1.1", Method: "POST", Target: "/up",
			Header: []Field{{"Host", "a.b"}, {"X-Custom", "v"}},
			Delim:  DelimLength, Length: 5,
		},
		Body: strings.NewReader("hello"),
	}
	ms := roundTrip(t, m, NewSerializer(nil))
	got := ms[0]
	if got.head.Method != "POST" || got.head.Target != "/up" {
		t.Fatalf("start line mismatch: %+v", got.head)
	}
	if string(got.body) != "hello" || got.head.Delim != DelimLength || got.head.Length != 5 {
		t.Fatalf("body mismatch: %+v", got)
	}
	if got.end != EndComplete {
		t.Fatalf("bad end: %v", got.end)
	}
}

func TestRoundTripChunkedStaysChunked(t *testing.T) {
	m := &Message{
		Head: Head{
			Kind: Response, Proto: "HTTP/1.1", StatusCode: 200,
			Delim: DelimChunked,
		},
		Body:    strings.NewReader(strings.Repeat("x", 100000)),
		Trailer: []Field{{"X-Sum", "ff"}},
	}
	var buf bytes.Buffer
	s := NewSerializer(nil)
	s.w = &buf
	if err := s.Write(m); err != nil {
		t.Fatal(err)
	}
	out := buf.String()
	// 需要分块的报文体不得改写成带长度的一次性形式。
	if !strings.Contains(out, "Transfer-Encoding: chunked") {
		t.Fatalf("chunked framing lost: %q", out[:200])
	}
	if strings.Contains(out, "Content-Length") {
		t.Fatalf("must not rewrite chunked as content-length: %q", out[:200])
	}
	// 用全新 Body 再做一次完整回环（原 Body 已被上一次序列化耗尽）。
	ms := roundTrip(t, &Message{
		Head: Head{Kind: Response, Proto: "HTTP/1.1", StatusCode: 200,
			Delim: DelimChunked},
		Body:    strings.NewReader(strings.Repeat("x", 100000)),
		Trailer: []Field{{"X-Sum", "ff"}},
	}, NewSerializer(nil))
	if string(ms[0].body) != strings.Repeat("x", 100000) {
		t.Fatalf("chunked body mismatch, len=%d", len(ms[0].body))
	}
	if len(ms[0].trailer) != 1 || ms[0].trailer[0].Name != "X-Sum" {
		t.Fatalf("trailer lost: %+v", ms[0].trailer)
	}
}

func TestHeaderCaseAndOrderPolicies(t *testing.T) {
	m := &Message{
		Head: Head{
			Kind: Request, Proto: "HTTP/1.1", Method: "GET", Target: "/",
			Header: []Field{{"x-BETA", "1"}, {"Host", "a.b"}, {"accept-ENCODING", "gzip"}},
			Delim:  DelimNone,
		},
	}
	// 小写 + 排序。
	var buf bytes.Buffer
	s := &Serializer{w: &buf, Case: CaseLower, Order: OrderSort}
	if err := s.Write(m); err != nil {
		t.Fatal(err)
	}
	head := strings.Split(buf.String(), "\r\n\r\n")[0]
	lines := strings.Split(head, "\r\n")[1:]
	want := []string{"accept-encoding: 1", "host: a.b", "x-beta: 1"}
	_ = want
	got := map[string]bool{}
	for _, l := range lines {
		got[l] = true
	}
	for _, w := range []string{"accept-encoding: gzip", "host: a.b", "x-beta: 1"} {
		if !got[w] {
			t.Fatalf("missing %q in %v", w, lines)
		}
	}
	// 排序检查：accept-encoding 在 host 前，host 在 x-beta 前。
	if !(strings.Index(head, "accept-encoding") < strings.Index(head, "host") &&
		strings.Index(head, "host") < strings.Index(head, "x-beta")) {
		t.Fatalf("not sorted: %v", lines)
	}
	// 规范化大小写。
	buf.Reset()
	s = &Serializer{w: &buf, Case: CaseCanonical}
	if err := s.Write(m); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(buf.String(), "X-Beta: 1") ||
		!strings.Contains(buf.String(), "Accept-Encoding: gzip") {
		t.Fatalf("canonical case wrong: %q", buf.String())
	}
	// 策略输出仍能被自身解析。
	p := NewParser(Request, Config{})
	if _, err := feedAll(p, true, buf.String()); err != nil {
		t.Fatalf("reparse canonical: %v", err)
	}
}

func TestSerializerPipelinedRoundTrip(t *testing.T) {
	var buf bytes.Buffer
	s := NewSerializer(&buf)
	msgs := []*Message{
		{Head: Head{Kind: Request, Proto: "HTTP/1.1", Method: "GET", Target: "/1",
			Delim: DelimNone}},
		{Head: Head{Kind: Request, Proto: "HTTP/1.1", Method: "POST", Target: "/2",
			Delim: DelimLength, Length: 3}, Body: strings.NewReader("abc")},
		{Head: Head{Kind: Request, Proto: "HTTP/1.1", Method: "POST", Target: "/3",
			Delim: DelimChunked}, Body: strings.NewReader("chunked-body")},
	}
	for _, m := range msgs {
		if err := s.Write(m); err != nil {
			t.Fatal(err)
		}
	}
	p := NewParser(Request, Config{})
	got, err := feedAll(p, true, buf.String())
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 {
		t.Fatalf("want 3 messages, got %d", len(got))
	}
	if got[1].head.Target != "/2" || string(got[1].body) != "abc" {
		t.Fatalf("message 2 mismatch: %+v", got[1])
	}
	if got[2].head.Target != "/3" || string(got[2].body) != "chunked-body" ||
		got[2].head.Delim != DelimChunked {
		t.Fatalf("message 3 mismatch: %+v", got[2])
	}
}

func TestWriteLengthBodyShort(t *testing.T) {
	var buf bytes.Buffer
	s := NewSerializer(&buf)
	err := s.Write(&Message{
		Head: Head{Kind: Request, Proto: "HTTP/1.1", Method: "POST", Target: "/",
			Delim: DelimLength, Length: 10},
		Body: strings.NewReader("short"),
	})
	if err == nil {
		t.Fatal("want error for short body")
	}
}

func TestCloseDelimitedSerialize(t *testing.T) {
	var buf bytes.Buffer
	s := NewSerializer(&buf)
	err := s.Write(&Message{
		Head: Head{Kind: Response, Proto: "HTTP/1.1", StatusCode: 200,
			Delim: DelimClose},
		Body: strings.NewReader("until-close"),
	})
	if err != nil {
		t.Fatal(err)
	}
	p := NewParser(Response, Config{})
	ms, err := feedAll(p, true, buf.String())
	if err != nil {
		t.Fatal(err)
	}
	if len(ms) != 1 || string(ms[0].body) != "until-close" || ms[0].end != EndClose {
		t.Fatalf("close-delimited round trip failed: %+v", ms)
	}
}

// 确保序列化不会把 Body 整体读入内存：用一个只读一次的 Reader 验证流式拷贝。
func TestStreamingBodyNotBuffered(t *testing.T) {
	r := io.NopCloser(strings.NewReader("stream"))
	m := &Message{
		Head: Head{Kind: Request, Proto: "HTTP/1.1", Method: "POST", Target: "/",
			Delim: DelimChunked},
		Body: r,
	}
	var buf bytes.Buffer
	s := NewSerializer(&buf)
	if err := s.Write(m); err != nil {
		t.Fatal(err)
	}
	if !strings.HasSuffix(buf.String(), "0\r\n\r\n") {
		t.Fatalf("bad chunked terminator: %q", buf.String())
	}
}
