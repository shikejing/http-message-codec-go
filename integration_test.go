package httpcodec

import (
	"bytes"
	"io"
	"net"
	"testing"
	"time"
)

// serve reads r in small irregular chunks (simulating arbitrary network
// fragmentation) and feeds the parser, invoking handle per complete message.
// It returns when the stream ends or a parse error occurs.
func serve(c *Codec, r io.Reader, chunk int, handle func(*Message, []byte)) error {
	p := c.NewParser()
	var body []byte
	buf := make([]byte, chunk)
	apply := func(evts []Event) {
		for _, ev := range evts {
			switch ev.Kind {
			case EventHead:
				body = body[:0]
			case EventBody:
				body = append(body, ev.Body...)
			case EventMessageEnd:
				handle(ev.Message, append([]byte(nil), body...))
			}
		}
	}
	for {
		n, err := r.Read(buf)
		if n > 0 {
			evts, ferr := p.Feed(buf[:n])
			apply(evts)
			if ferr != nil {
				return ferr
			}
		}
		if err != nil {
			evts, cerr := p.Close()
			apply(evts)
			return cerr
		}
	}
}

// TestOverInMemoryPipe runs pipelined requests and responses over net.Pipe
// with a tiny read buffer, verifying connection-level semantics end to end.
func TestOverInMemoryPipe(t *testing.T) {
	c := NewCodec(DefaultConfig())
	client, server := net.Pipe()
	defer client.Close()

	wire := "POST /one HTTP/1.1\r\nContent-Length: 3\r\n\r\nfoo" +
		"POST /two HTTP/1.1\r\nTransfer-Encoding: chunked\r\n\r\n3\r\nbar\r\n0\r\n\r\n" +
		"GET /three HTTP/1.1\r\n\r\n"
	go func() {
		// Write in irregular bursts to force arbitrary fragmentation.
		for i := 0; i < len(wire); i += 13 {
			j := i + 13
			if j > len(wire) {
				j = len(wire)
			}
			if _, err := client.Write([]byte(wire[i:j])); err != nil {
				return
			}
			time.Sleep(time.Millisecond)
		}
		client.Close()
	}()

	type got struct {
		target string
		body   string
		end    EndReason
	}
	var msgs []got
	err := serve(c, server, 7, func(m *Message, b []byte) {
		msgs = append(msgs, got{m.Target, string(b), m.End})
	})
	if err != nil {
		t.Fatalf("serve: %v", err)
	}
	want := []got{
		{"/one", "foo", EndContentLength},
		{"/two", "bar", EndChunked},
		{"/three", "", EndNone},
	}
	if len(msgs) != len(want) {
		t.Fatalf("got %d messages: %+v", len(msgs), msgs)
	}
	for i := range want {
		if msgs[i] != want[i] {
			t.Fatalf("msg %d = %+v, want %+v", i, msgs[i], want[i])
		}
	}
}

// TestTruncatedOverPipe verifies a connection that closes mid-message is
// reported as truncated, not as a clean end.
func TestTruncatedOverPipe(t *testing.T) {
	c := NewCodec(DefaultConfig())
	client, server := net.Pipe()
	go func() {
		client.Write([]byte("POST /x HTTP/1.1\r\nContent-Length: 100\r\n\r\npartial"))
		client.Close()
	}()
	var completed int
	err := serve(c, server, 16, func(*Message, []byte) { completed++ })
	if err == nil {
		t.Fatal("want truncation error")
	}
	perr, ok := err.(*ParseError)
	if !ok || perr.Reason != ReasonTruncated {
		t.Fatalf("err = %v", err)
	}
	if completed != 0 {
		t.Fatalf("truncated message counted as complete")
	}
}

// TestWriterReaderOverPipe serializes on one end of a pipe and parses on the
// other.
func TestWriterReaderOverPipe(t *testing.T) {
	c := NewCodec(DefaultConfig())
	client, server := net.Pipe()
	h := &Headers{}
	h.Add("Transfer-Encoding", "chunked")
	msg := &Message{IsRequest: false, StatusCode: 200, Proto: "HTTP/1.1",
		Header: h, Delimiter: DelimChunked}
	payload := bytes.Repeat([]byte("data"), 5000)
	go func() {
		w := c.NewWriter()
		w.WriteMessage(client, msg, bytes.NewReader(payload))
		client.Close()
	}()
	var body []byte
	var end EndReason
	err := serve(c, server, 1024, func(m *Message, b []byte) {
		body, end = b, m.End
	})
	if err != nil {
		t.Fatalf("serve: %v", err)
	}
	if !bytes.Equal(body, payload) {
		t.Fatalf("body mismatch: %d vs %d bytes", len(body), len(payload))
	}
	if end != EndChunked {
		t.Fatalf("end = %v", end)
	}
}
