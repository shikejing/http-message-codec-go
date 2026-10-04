package httpcodec

import (
	"fmt"
	"strings"
	"testing"
)

func cfgWith(mut func(*Config)) Config {
	c := DefaultConfig()
	mut(&c)
	return c
}

func expectReason(t *testing.T, err error, want ErrorReason) {
	t.Helper()
	if err == nil {
		t.Fatalf("no error, want %v", want)
	}
	perr, ok := err.(*ParseError)
	if !ok {
		t.Fatalf("err type %T, want *ParseError", err)
	}
	if perr.Reason != want {
		t.Fatalf("reason = %v, want %v (err=%v)", perr.Reason, want, perr)
	}
}

func TestMaxHeaderLineLen(t *testing.T) {
	cfg := cfgWith(func(c *Config) { c.MaxHeaderLineLen = 16 })
	c := NewCodec(cfg)
	p := c.NewParser()
	long := "X-Long: " + strings.Repeat("a", 32)
	_, err := p.Feed([]byte("GET / HTTP/1.1\r\n" + long + "\r\n\r\n"))
	expectReason(t, err, ReasonHeaderLineTooLong)

	// Just under the limit parses fine.
	p2 := c.NewParser()
	ok := "X-L: " + strings.Repeat("a", 5) // line len 10 <= 16
	if _, err := p2.Feed([]byte("GET / HTTP/1.1\r\n" + ok + "\r\n\r\n")); err != nil {
		t.Fatalf("under-limit line rejected: %v", err)
	}
}

func TestMaxHeaderBytes(t *testing.T) {
	cfg := cfgWith(func(c *Config) { c.MaxHeaderBytes = 64 })
	c := NewCodec(cfg)
	p := c.NewParser()
	var sb strings.Builder
	sb.WriteString("GET / HTTP/1.1\r\n")
	for i := 0; i < 10; i++ {
		fmt.Fprintf(&sb, "X-H%d: v%d\r\n", i, i)
	}
	sb.WriteString("\r\n")
	_, err := p.Feed([]byte(sb.String()))
	expectReason(t, err, ReasonHeadersTooLarge)
}

func TestMaxChunkCount(t *testing.T) {
	cfg := cfgWith(func(c *Config) { c.MaxChunkCount = 3 })
	c := NewCodec(cfg)
	p := c.NewParser()
	wire := "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n" +
		strings.Repeat("1\r\na\r\n", 4) + "0\r\n\r\n"
	_, err := p.Feed([]byte(wire))
	expectReason(t, err, ReasonTooManyChunks)
}

func TestMaxChunkSize(t *testing.T) {
	cfg := cfgWith(func(c *Config) { c.MaxChunkSize = 8 })
	c := NewCodec(cfg)
	p := c.NewParser()
	wire := "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n10\r\n" + strings.Repeat("a", 16) + "\r\n0\r\n\r\n"
	_, err := p.Feed([]byte(wire))
	expectReason(t, err, ReasonChunkTooLarge)
}

func TestMaxBodyBytes(t *testing.T) {
	cfg := cfgWith(func(c *Config) { c.MaxBodyBytes = 10 })
	// Content-Length framing.
	c := NewCodec(cfg)
	p := c.NewParser()
	_, err := p.Feed([]byte("POST / HTTP/1.1\r\nContent-Length: 20\r\n\r\n" + strings.Repeat("x", 20)))
	expectReason(t, err, ReasonBodyTooLarge)

	// Chunked framing, cumulative across chunks.
	p2 := c.NewParser()
	wire := "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n" +
		"6\r\naaaaaa\r\n6\r\nbbbbbb\r\n0\r\n\r\n"
	_, err = p2.Feed([]byte(wire))
	expectReason(t, err, ReasonBodyTooLarge)

	// Until-close framing.
	p3 := c.NewParser()
	if _, err := p3.Feed([]byte("HTTP/1.0 200 OK\r\n\r\n" + strings.Repeat("y", 11))); err != nil {
		expectReason(t, err, ReasonBodyTooLarge)
	}
}

// TestMemoryBoundedness feeds a hostile stream in tiny pieces and checks the
// parser never buffers more than the relevant limit.
func TestMemoryBoundedness(t *testing.T) {
	cfg := cfgWith(func(c *Config) {
		c.MaxHeaderLineLen = 1024
		c.MaxBodyBytes = 1 << 20
	})
	c := NewCodec(cfg)
	p := c.NewParser()
	// Endless header line without CRLF: must error at the line limit, not
	// buffer forever.
	chunk := []byte(strings.Repeat("z", 100))
	if _, err := p.Feed([]byte("GET / HTTP/1.1\r\nX: ")); err != nil {
		t.Fatal(err)
	}
	var err error
	for i := 0; i < 100; i++ {
		if _, err = p.Feed(chunk); err != nil {
			break
		}
	}
	expectReason(t, err, ReasonHeaderLineTooLong)
	if len(p.buf) > cfg.MaxHeaderLineLen+len(chunk) {
		t.Fatalf("buffer grew to %d bytes", len(p.buf))
	}

	// Huge declared body streamed in small pieces: buffer stays tiny.
	p2 := c.NewParser()
	if _, err := p2.Feed([]byte("POST / HTTP/1.1\r\nContent-Length: 524288\r\n\r\n")); err != nil {
		t.Fatal(err)
	}
	piece := make([]byte, 4096)
	for i := 0; i < 128; i++ {
		if _, err := p2.Feed(piece); err != nil {
			t.Fatal(err)
		}
		if len(p2.buf) != 0 {
			t.Fatalf("body data accumulated: %d bytes buffered", len(p2.buf))
		}
	}
}

func TestErrorOffsets(t *testing.T) {
	c := NewCodec(DefaultConfig())
	p := c.NewParser()
	wire := "GET / HTTP/1.1\r\nBad Header Line\r\n\r\n"
	_, err := p.Feed([]byte(wire))
	perr, ok := err.(*ParseError)
	if !ok {
		t.Fatalf("err type %T", err)
	}
	// The bad header line starts right after the request line (16 bytes).
	if perr.Offset != 16 {
		t.Fatalf("offset = %d, want 16 (err=%v)", perr.Offset, perr)
	}
	if !strings.Contains(perr.Error(), "offset 16") {
		t.Fatalf("error string lacks offset: %v", perr)
	}
}
