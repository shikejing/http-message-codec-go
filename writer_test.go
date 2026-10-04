package httpcodec

import (
	"bytes"
	"strings"
	"testing"
)

// roundTrip serializes msg+body and parses it back, returning the collected
// message.
func roundTrip(t *testing.T, c *Codec, msg *Message, body []byte) parsed {
	t.Helper()
	var buf bytes.Buffer
	w := c.NewWriter()
	var rdr *bytes.Reader
	if body != nil {
		rdr = bytes.NewReader(body)
	}
	if err := w.WriteMessage(&buf, msg, rdr); err != nil {
		t.Fatalf("WriteMessage: %v", err)
	}
	p := c.NewParser()
	out := collect(t, p, buf.Bytes())
	if len(out) != 1 {
		t.Fatalf("got %d messages, want 1", len(out))
	}
	if out[0].msg.Delimiter == DelimUntilClose {
		evts, err := p.Close()
		if err != nil {
			t.Fatalf("Close: %v", err)
		}
		for _, ev := range evts {
			if ev.Kind == EventBody {
				out[0].body = append(out[0].body, ev.Body...)
			}
		}
	}
	return out[0]
}

func TestRoundTripContentLength(t *testing.T) {
	c := NewCodec(DefaultConfig())
	h := &Headers{}
	h.Add("Host", "example.com")
	h.Add("X-Custom", "v1")
	msg := &Message{
		IsRequest: true, Method: "POST", Target: "/up", Proto: "HTTP/1.1",
		Header: h, Delimiter: DelimContentLength, ContentLength: 5,
	}
	got := roundTrip(t, c, msg, []byte("hello"))
	m := got.msg
	if m.Method != "POST" || m.Target != "/up" {
		t.Fatalf("bad start line: %+v", m)
	}
	if string(got.body) != "hello" {
		t.Fatalf("body = %q", got.body)
	}
	if m.End != EndContentLength {
		t.Fatalf("end = %v", m.End)
	}
	// All original fields survive, and the writer supplied the missing
	// Content-Length framing header.
	for _, f := range msg.Header.All() {
		if v, ok := m.Header.Get(f.Name); !ok || v != f.Value {
			t.Fatalf("header %q lost: %v", f.Name, m.Header.All())
		}
	}
	if v, ok := m.Header.Get("Content-Length"); !ok || v != "5" {
		t.Fatalf("Content-Length not added: %v", m.Header.All())
	}
}

func TestRoundTripChunkedStaysChunked(t *testing.T) {
	c := NewCodec(DefaultConfig())
	h := &Headers{}
	h.Add("Transfer-Encoding", "chunked")
	tr := &Headers{}
	tr.Add("X-Trailer", "t1")
	msg := &Message{
		IsRequest: false, StatusCode: 200, ReasonPhrase: "OK", Proto: "HTTP/1.1",
		Header: h, Trailer: tr, Delimiter: DelimChunked,
	}
	body := []byte(strings.Repeat("chunky-", 10000)) // spans many chunks
	var buf bytes.Buffer
	if err := c.NewWriter().WriteMessage(&buf, msg, bytes.NewReader(body)); err != nil {
		t.Fatal(err)
	}
	// The serialized form must use chunked framing, not a collapsed
	// Content-Length form.
	s := buf.String()
	if !strings.Contains(s, "Transfer-Encoding: chunked\r\n") {
		t.Fatalf("missing chunked TE header:\n%.200s", s)
	}
	if strings.Contains(s, "Content-Length") {
		t.Fatalf("chunked message rewritten with Content-Length")
	}
	if !strings.HasSuffix(s, "0\r\nX-Trailer: t1\r\n\r\n") {
		t.Fatalf("bad chunked terminator: %q", s[len(s)-40:])
	}
	got := roundTrip(t, c, msg, body)
	if !bytes.Equal(got.body, body) {
		t.Fatalf("body mismatch: %d vs %d bytes", len(got.body), len(body))
	}
	if got.msg.End != EndChunked {
		t.Fatalf("end = %v", got.msg.End)
	}
	if v, ok := got.trailer.Get("x-trailer"); !ok || v != "t1" {
		t.Fatalf("trailer lost: %v", got.trailer)
	}
}

func TestRoundTripNoBody(t *testing.T) {
	c := NewCodec(DefaultConfig())
	msg := &Message{
		IsRequest: true, Method: "GET", Target: "/", Proto: "HTTP/1.1",
		Header: &Headers{}, Delimiter: DelimNone,
	}
	got := roundTrip(t, c, msg, nil)
	if got.msg.End != EndNone || len(got.body) != 0 {
		t.Fatalf("end=%v body=%q", got.msg.End, got.body)
	}
}

func TestRoundTripUntilClose(t *testing.T) {
	c := NewCodec(DefaultConfig())
	msg := &Message{
		IsRequest: false, StatusCode: 200, Proto: "HTTP/1.0",
		Header: &Headers{}, Delimiter: DelimUntilClose,
	}
	got := roundTrip(t, c, msg, []byte("raw-body"))
	if string(got.body) != "raw-body" {
		t.Fatalf("body = %q", got.body)
	}
}

func TestContentLengthMismatch(t *testing.T) {
	c := NewCodec(DefaultConfig())
	msg := &Message{
		IsRequest: true, Method: "POST", Target: "/", Proto: "HTTP/1.1",
		Header: &Headers{}, Delimiter: DelimContentLength, ContentLength: 10,
	}
	var buf bytes.Buffer
	err := c.NewWriter().WriteMessage(&buf, msg, strings.NewReader("short"))
	if err == nil {
		t.Fatal("want length mismatch error")
	}
}

func TestHeaderCasePolicies(t *testing.T) {
	mk := func(policy HeaderCasePolicy) string {
		cfg := DefaultConfig()
		cfg.HeaderCase = policy
		c := NewCodec(cfg)
		h := &Headers{}
		h.Add("cOnTeNt-tYpE", "text/plain")
		h.Add("X-CUSTOM-hdr", "v")
		msg := &Message{
			IsRequest: true, Method: "GET", Target: "/", Proto: "HTTP/1.1",
			Header: h, Delimiter: DelimNone,
		}
		var buf bytes.Buffer
		if err := c.NewWriter().WriteMessage(&buf, msg, nil); err != nil {
			t.Fatal(err)
		}
		return buf.String()
	}
	if s := mk(CasePreserve); !strings.Contains(s, "cOnTeNt-tYpE: text/plain\r\n") {
		t.Fatalf("preserve: %q", s)
	}
	if s := mk(CaseLower); !strings.Contains(s, "content-type: text/plain\r\nx-custom-hdr: v\r\n") {
		t.Fatalf("lower: %q", s)
	}
	if s := mk(CaseCanonical); !strings.Contains(s, "Content-Type: text/plain\r\nX-Custom-Hdr: v\r\n") {
		t.Fatalf("canonical: %q", s)
	}
}

func TestHeaderOrderPolicies(t *testing.T) {
	build := func(policy HeaderOrderPolicy) string {
		cfg := DefaultConfig()
		cfg.HeaderOrder = policy
		c := NewCodec(cfg)
		h := &Headers{}
		h.Add("Zeta", "1")
		h.Add("alpha", "2")
		h.Add("Beta", "3")
		msg := &Message{
			IsRequest: true, Method: "GET", Target: "/", Proto: "HTTP/1.1",
			Header: h, Delimiter: DelimNone,
		}
		var buf bytes.Buffer
		if err := c.NewWriter().WriteMessage(&buf, msg, nil); err != nil {
			t.Fatal(err)
		}
		return buf.String()
	}
	if s := build(OrderPreserve); strings.Index(s, "Zeta") > strings.Index(s, "alpha") {
		t.Fatalf("preserve order broken: %q", s)
	}
	s := build(OrderSort)
	if !(strings.Index(s, "alpha") < strings.Index(s, "Beta") && strings.Index(s, "Beta") < strings.Index(s, "Zeta")) {
		t.Fatalf("sort order broken: %q", s)
	}
}

// TestSerializedOutputReparses feeds serialized output back in tiny pieces.
func TestSerializedOutputReparses(t *testing.T) {
	c := NewCodec(DefaultConfig())
	h := &Headers{}
	h.Add("Host", "x")
	msg1 := &Message{IsRequest: true, Method: "POST", Target: "/a", Proto: "HTTP/1.1",
		Header: h, Delimiter: DelimContentLength, ContentLength: 4}
	h2 := &Headers{}
	h2.Add("Transfer-Encoding", "chunked")
	msg2 := &Message{IsRequest: true, Method: "POST", Target: "/b", Proto: "HTTP/1.1",
		Header: h2, Delimiter: DelimChunked}
	var buf bytes.Buffer
	w := c.NewWriter()
	if err := w.WriteMessage(&buf, msg1, strings.NewReader("data")); err != nil {
		t.Fatal(err)
	}
	if err := w.WriteMessage(&buf, msg2, strings.NewReader("streamed")); err != nil {
		t.Fatal(err)
	}
	p := c.NewParser()
	out := feedEvery(t, p, buf.Bytes(), 3)
	if len(out) != 2 {
		t.Fatalf("got %d messages", len(out))
	}
	if string(out[0].body) != "data" || string(out[1].body) != "streamed" {
		t.Fatalf("bodies: %q %q", out[0].body, out[1].body)
	}
	if out[1].msg.Delimiter != DelimChunked {
		t.Fatalf("second delimiter = %v", out[1].msg.Delimiter)
	}
}
