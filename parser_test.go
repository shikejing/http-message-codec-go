package httpcodec

import (
	"bytes"
	"strings"
	"testing"
)

// parsed is one fully collected message.
type parsed struct {
	msg     *Message
	body    []byte
	trailer *Headers
}

// collect feeds every chunk through p and assembles events into messages.
func collect(t *testing.T, p *Parser, chunks ...[]byte) []parsed {
	t.Helper()
	var out []parsed
	for _, c := range chunks {
		evts, err := p.Feed(c)
		if err != nil {
			t.Fatalf("Feed(%q) failed: %v", c, err)
		}
		for _, ev := range evts {
			switch ev.Kind {
			case EventHead:
				out = append(out, parsed{msg: ev.Message})
			case EventBody:
				out[len(out)-1].body = append(out[len(out)-1].body, ev.Body...)
			case EventTrailers:
				out[len(out)-1].trailer = ev.Trailer
			case EventMessageEnd:
				if out[len(out)-1].msg != ev.Message {
					t.Fatalf("end message does not match head message")
				}
			}
		}
	}
	return out
}

// feedEvery splits data into every possible single split point and feeds the
// two parts, verifying the parse result is identical to one-shot parsing.
func feedEvery(t *testing.T, p *Parser, data []byte, size int) []parsed {
	t.Helper()
	var out []parsed
	for i := 0; i < len(data); i += size {
		j := i + size
		if j > len(data) {
			j = len(data)
		}
		evts, err := p.Feed(data[i:j])
		if err != nil {
			t.Fatalf("Feed failed at offset %d: %v", i, err)
		}
		for _, ev := range evts {
			switch ev.Kind {
			case EventHead:
				out = append(out, parsed{msg: ev.Message})
			case EventBody:
				out[len(out)-1].body = append(out[len(out)-1].body, ev.Body...)
			case EventTrailers:
				out[len(out)-1].trailer = ev.Trailer
			}
		}
	}
	return out
}

func TestSimpleRequest(t *testing.T) {
	c := NewCodec(DefaultConfig())
	p := c.NewParser()
	out := collect(t, p, []byte("GET /index.html HTTP/1.1\r\nHost: example.com\r\n\r\n"))
	if len(out) != 1 {
		t.Fatalf("got %d messages, want 1", len(out))
	}
	m := out[0].msg
	if !m.IsRequest || m.Method != "GET" || m.Target != "/index.html" || m.Proto != "HTTP/1.1" {
		t.Fatalf("bad request line: %+v", m)
	}
	if v, ok := m.Header.Get("host"); !ok || v != "example.com" {
		t.Fatalf("bad host header: %q", v)
	}
	if m.End != EndNone {
		t.Fatalf("end reason = %v, want %v", m.End, EndNone)
	}
	if evts, err := p.Close(); err != nil || len(evts) != 0 {
		t.Fatalf("Close = %v, %v", evts, err)
	}
}

func TestFragmentedRequestWithBody(t *testing.T) {
	wire := "POST /submit HTTP/1.1\r\nHost: a.b\r\nContent-Length: 11\r\n\r\nhello world"
	want := []byte("hello world")
	// Byte-by-byte: boundaries land inside method, headers and body.
	for _, size := range []int{1, 2, 3, 7, len(wire)} {
		c := NewCodec(DefaultConfig())
		p := c.NewParser()
		out := feedEvery(t, p, []byte(wire), size)
		if len(out) != 1 {
			t.Fatalf("size=%d: got %d messages", size, len(out))
		}
		if !bytes.Equal(out[0].body, want) {
			t.Fatalf("size=%d: body = %q", size, out[0].body)
		}
		if out[0].msg.End != EndContentLength || out[0].msg.ContentLength != 11 {
			t.Fatalf("size=%d: end = %v cl = %d", size, out[0].msg.End, out[0].msg.ContentLength)
		}
	}
}

func TestChunkedWithTrailers(t *testing.T) {
	wire := "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n" +
		"4\r\nWiki\r\n5;ext=1\r\npedia\r\n0\r\nX-Checksum: ab12\r\n\r\n"
	for _, size := range []int{1, 4, 9, len(wire)} {
		c := NewCodec(DefaultConfig())
		p := c.NewParser()
		out := feedEvery(t, p, []byte(wire), size)
		if len(out) != 1 {
			t.Fatalf("size=%d: got %d messages", size, len(out))
		}
		m := out[0].msg
		if m.IsRequest || m.StatusCode != 200 {
			t.Fatalf("size=%d: bad status line: %+v", size, m)
		}
		if string(out[0].body) != "Wikipedia" {
			t.Fatalf("size=%d: body = %q", size, out[0].body)
		}
		if m.End != EndChunked {
			t.Fatalf("size=%d: end = %v", size, m.End)
		}
		if out[0].trailer == nil {
			t.Fatalf("size=%d: no trailers", size)
		}
		if v, ok := out[0].trailer.Get("x-checksum"); !ok || v != "ab12" {
			t.Fatalf("size=%d: trailer = %v", size, out[0].trailer.All())
		}
		// Trailer must also be reachable from the completed message.
		if v, ok := m.Trailer.Get("X-Checksum"); !ok || v != "ab12" {
			t.Fatalf("size=%d: msg.Trailer missing", size)
		}
	}
}

func TestPipelinedMessages(t *testing.T) {
	wire := "GET /1 HTTP/1.1\r\nHost: x\r\n\r\n" +
		"POST /2 HTTP/1.1\r\nContent-Length: 3\r\n\r\nabc" +
		"GET /3 HTTP/1.1\r\n\r\n"
	for _, size := range []int{1, 5, len(wire)} {
		c := NewCodec(DefaultConfig())
		p := c.NewParser()
		out := feedEvery(t, p, []byte(wire), size)
		if len(out) != 3 {
			t.Fatalf("size=%d: got %d messages", size, len(out))
		}
		if out[0].msg.Target != "/1" || out[1].msg.Target != "/2" || out[2].msg.Target != "/3" {
			t.Fatalf("size=%d: wrong targets", size)
		}
		if string(out[1].body) != "abc" {
			t.Fatalf("size=%d: body of second = %q", size, out[1].body)
		}
		// No bytes may leak across message boundaries.
		if r := p.Remaining(); len(r) != 0 {
			t.Fatalf("size=%d: leftover %q", size, r)
		}
	}
}

func TestResponseUntilClose(t *testing.T) {
	c := NewCodec(DefaultConfig())
	p := c.NewParser()
	out := collect(t, p,
		[]byte("HTTP/1.0 200 OK\r\nServer: x\r\n\r\nhel"),
		[]byte("lo"),
	)
	if len(out) != 1 {
		t.Fatalf("got %d messages", len(out))
	}
	if out[0].msg.Delimiter != DelimUntilClose {
		t.Fatalf("delimiter = %v", out[0].msg.Delimiter)
	}
	evts, err := p.Close()
	if err != nil {
		t.Fatalf("Close failed: %v", err)
	}
	var end *Message
	for _, ev := range evts {
		if ev.Kind == EventBody {
			out[0].body = append(out[0].body, ev.Body...)
		}
		if ev.Kind == EventMessageEnd {
			end = ev.Message
		}
	}
	if end == nil || end.End != EndConnectionClose {
		t.Fatalf("end = %v", end)
	}
	if string(out[0].body) != "hello" {
		t.Fatalf("body = %q", out[0].body)
	}
}

func TestTruncatedMessage(t *testing.T) {
	cases := []string{
		"GET / HTTP/1.1\r\nHost: x\r\n",                                        // mid headers
		"POST / HTTP/1.1\r\nContent-Length: 5\r\n\r\nab",                       // short body
		"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n4\r\nWi",         // mid chunk
		"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n0\r\nX-A: 1\r\n", // mid trailers
		"GET / HT", // mid start line
	}
	for _, wire := range cases {
		c := NewCodec(DefaultConfig())
		p := c.NewParser()
		if _, err := p.Feed([]byte(wire)); err != nil {
			t.Fatalf("%q: Feed failed early: %v", wire, err)
		}
		_, err := p.Close()
		if err == nil {
			t.Fatalf("%q: Close succeeded, want truncation error", wire)
		}
		perr, ok := err.(*ParseError)
		if !ok || perr.Reason != ReasonTruncated {
			t.Fatalf("%q: err = %v, want ReasonTruncated", wire, err)
		}
	}
}

func TestConflictingFraming(t *testing.T) {
	cases := []struct {
		wire   string
		reason ErrorReason
	}{
		{"POST / HTTP/1.1\r\nContent-Length: 4\r\nTransfer-Encoding: chunked\r\n\r\n0\r\n\r\n", ReasonConflictingFraming},
		{"POST / HTTP/1.1\r\nContent-Length: 4\r\nContent-Length: 5\r\n\r\n", ReasonConflictingFraming},
		{"POST / HTTP/1.1\r\nContent-Length: 4, 5\r\n\r\n", ReasonConflictingFraming},
		{"POST / HTTP/1.1\r\nTransfer-Encoding: gzip\r\n\r\n", ReasonConflictingFraming},
	}
	for _, tc := range cases {
		c := NewCodec(DefaultConfig())
		p := c.NewParser()
		_, err := p.Feed([]byte(tc.wire))
		if err == nil {
			t.Fatalf("%q: no error", tc.wire)
		}
		perr, ok := err.(*ParseError)
		if !ok || perr.Reason != tc.reason {
			t.Fatalf("%q: err = %v, want reason %v", tc.wire, err, tc.reason)
		}
	}
	// Repeated identical Content-Length is legal.
	c := NewCodec(DefaultConfig())
	p := c.NewParser()
	out := collect(t, p, []byte("POST / HTTP/1.1\r\nContent-Length: 2\r\nContent-Length: 2\r\n\r\nok"))
	if len(out) != 1 || string(out[0].body) != "ok" {
		t.Fatalf("identical repeated CL rejected: %+v", out)
	}
}

func TestMalformedChunks(t *testing.T) {
	cases := []struct {
		wire   string
		reason ErrorReason
	}{
		{"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\nzz\r\nab\r\n0\r\n\r\n", ReasonMalformedChunkSize},
		{"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n\r\nab\r\n0\r\n\r\n", ReasonMalformedChunkSize},
		{"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n2\r\nabXX0\r\n\r\n", ReasonMalformedChunkEnd},
	}
	for _, tc := range cases {
		c := NewCodec(DefaultConfig())
		p := c.NewParser()
		_, err := p.Feed([]byte(tc.wire))
		perr, ok := err.(*ParseError)
		if !ok || perr.Reason != tc.reason {
			t.Fatalf("%q: err = %v, want reason %v", tc.wire, err, tc.reason)
		}
		if perr.Offset < 0 || perr.Offset > int64(len(tc.wire)) {
			t.Fatalf("%q: offset %d out of range", tc.wire, perr.Offset)
		}
	}
}

func TestMalformedHeaders(t *testing.T) {
	cases := []string{
		"GET / HTTP/1.1\r\nNoColon\r\n\r\n",
		"GET / HTTP/1.1\r\nBad Name: v\r\n\r\n",
		"GET / HTTP/1.1\r\n folded: v\r\n\r\n",
		"GET / HTTP/1.1\r\nX: a\x01b\r\n\r\n",
		"GET  /  HTTP/1.1\r\n\r\n",        // empty target segment
		"GET / HTTP/1.1\nHost: x\r\n\r\n", // bare LF
	}
	for _, wire := range cases {
		c := NewCodec(DefaultConfig())
		p := c.NewParser()
		_, err := p.Feed([]byte(wire))
		if err == nil {
			t.Fatalf("%q: no error", wire)
		}
		if _, ok := err.(*ParseError); !ok {
			t.Fatalf("%q: err type %T", wire, err)
		}
	}
}

func TestUpgrade(t *testing.T) {
	wire := "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n\r\nRAWBYTES"
	c := NewCodec(DefaultConfig())
	p := c.NewParser()
	out := collect(t, p, []byte(wire))
	if len(out) != 1 {
		t.Fatalf("got %d messages", len(out))
	}
	m := out[0].msg
	if !m.Upgraded || m.End != EndUpgrade {
		t.Fatalf("upgraded=%v end=%v", m.Upgraded, m.End)
	}
	if r := p.Remaining(); string(r) != "RAWBYTES" {
		t.Fatalf("remaining = %q", r)
	}
	if _, err := p.Close(); err != nil {
		t.Fatalf("Close after upgrade: %v", err)
	}
}

func TestRecover(t *testing.T) {
	c := NewCodec(DefaultConfig())
	p := c.NewParser()
	bad := "POST / HTTP/1.1\r\nContent-Length: 4\r\nTransfer-Encoding: chunked\r\n\r\n0\r\n\r\n"
	if _, err := p.Feed([]byte(bad)); err == nil {
		t.Fatal("want framing error")
	}
	if !p.Failed() {
		t.Fatal("parser not in failed state")
	}
	// Feeding while failed must return the same error.
	if _, err := p.Feed([]byte("GET / HTTP/1.1\r\n\r\n")); err == nil {
		t.Fatal("feed while failed must fail")
	}
	p.Recover()
	// Leftover bytes of the bad message were dropped; the next message
	// parses cleanly from newly fed data.
	out := collect(t, p, []byte("GET /next HTTP/1.1\r\nHost: y\r\n\r\n"))
	if len(out) != 1 || out[0].msg.Target != "/next" {
		t.Fatalf("after recover: %+v", out)
	}
}

func TestDeterminism(t *testing.T) {
	wire := "POST /a HTTP/1.1\r\nContent-Length: 5\r\n\r\nhello" +
		"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n3\r\nfoo\r\n0\r\n\r\n"
	var runs []string
	for i := 0; i < 5; i++ {
		c := NewCodec(DefaultConfig())
		p := c.NewParser()
		var sb strings.Builder
		for j := 0; j < len(wire); j += 3 {
			k := j + 3
			if k > len(wire) {
				k = len(wire)
			}
			evts, err := p.Feed([]byte(wire[j:k]))
			if err != nil {
				t.Fatal(err)
			}
			for _, ev := range evts {
				sb.WriteString(eventString(ev))
			}
		}
		runs = append(runs, sb.String())
	}
	for i := 1; i < len(runs); i++ {
		if runs[i] != runs[0] {
			t.Fatalf("run %d differs:\n%s\nvs\n%s", i, runs[i], runs[0])
		}
	}
}

func eventString(ev Event) string {
	switch ev.Kind {
	case EventHead:
		if ev.Message.IsRequest {
			return "H " + ev.Message.Method + " " + ev.Message.Target + "\n"
		}
		return "H " + ev.Message.Proto + " " + strings.Repeat("0", 0) +
			string(rune('0'+ev.Message.StatusCode/100)) + "\n"
	case EventBody:
		return "B " + string(ev.Body) + "\n"
	case EventTrailers:
		return "T\n"
	case EventMessageEnd:
		return "E " + ev.Message.End.String() + "\n"
	}
	return "?"
}
