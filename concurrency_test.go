package httpcodec

import (
	"fmt"
	"strings"
	"sync"
	"testing"
)

// drain feeds data to p in pieces of the given size and returns a canonical
// transcript of all events.
func drain(t *testing.T, p *Parser, data []byte, size int) string {
	t.Helper()
	var sb strings.Builder
	for i := 0; i < len(data); i += size {
		j := i + size
		if j > len(data) {
			j = len(data)
		}
		evts, err := p.Feed(data[i:j])
		if err != nil {
			fmt.Fprintf(&sb, "ERR %v\n", err)
			return sb.String()
		}
		for _, ev := range evts {
			sb.WriteString(eventString(ev))
		}
	}
	return sb.String()
}

// TestInterleavedConnections drives several parsers from one Codec in
// lockstep and verifies each connection's transcript equals the transcript
// of an isolated parse of the same input.
func TestInterleavedConnections(t *testing.T) {
	c := NewCodec(DefaultConfig())
	inputs := []string{
		"GET /a HTTP/1.1\r\nHost: x\r\n\r\nGET /b HTTP/1.1\r\n\r\n",
		"POST /p HTTP/1.1\r\nContent-Length: 6\r\n\r\nabcdef",
		"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n2\r\nhi\r\n0\r\nX-T: 1\r\n\r\n",
		"HTTP/1.0 404 Not Found\r\n\r\nbody-to-close",
	}
	const conns = 4
	parsers := make([]*Parser, conns)
	for i := range parsers {
		parsers[i] = c.NewParser()
	}
	// Interleave: feed one small piece to each connection in turn.
	transcripts := make([]string, conns)
	const piece = 5
	pos := make([]int, conns)
	var sbs [conns]strings.Builder
	for done := 0; done < conns; {
		done = 0
		for i := 0; i < conns; i++ {
			data := inputs[i]
			if pos[i] >= len(data) {
				done++
				continue
			}
			j := pos[i] + piece
			if j > len(data) {
				j = len(data)
			}
			evts, err := parsers[i].Feed([]byte(data[pos[i]:j]))
			pos[i] = j
			if err != nil {
				t.Fatalf("conn %d: %v", i, err)
			}
			for _, ev := range evts {
				sbs[i].WriteString(eventString(ev))
			}
		}
	}
	for i := range transcripts {
		transcripts[i] = sbs[i].String()
	}
	// Reference: each input parsed alone.
	for i, in := range inputs {
		ref := drain(t, c.NewParser(), []byte(in), piece)
		if transcripts[i] != ref {
			t.Fatalf("conn %d polluted:\n%s\nwant:\n%s", i, transcripts[i], ref)
		}
	}
}

// TestConcurrentUse hammers one shared Codec from many goroutines, each with
// its own parser, and requires byte-identical transcripts across runs.
func TestConcurrentUse(t *testing.T) {
	c := NewCodec(DefaultConfig())
	wire := "POST /x HTTP/1.1\r\nContent-Length: 4\r\n\r\nping" +
		"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n4\r\npong\r\n0\r\n\r\n"
	want := drain(t, c.NewParser(), []byte(wire), 7)

	var wg sync.WaitGroup
	errs := make(chan string, 64)
	for g := 0; g < 32; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			p := c.NewParser()
			// Same input, same feed pattern: transcripts must be identical.
			got := drain(&testing.T{}, p, []byte(wire), 7)
			if got != want {
				errs <- fmt.Sprintf("goroutine %d mismatch", g)
			}
		}(g)
	}
	wg.Wait()
	close(errs)
	for e := range errs {
		t.Fatal(e)
	}
}

// TestReproducibleErrors checks that the same bad input yields the same
// error reason and offset on every parser.
func TestReproducibleErrors(t *testing.T) {
	c := NewCodec(DefaultConfig())
	bad := "POST / HTTP/1.1\r\nContent-Length: 3\r\nTransfer-Encoding: chunked\r\n\r\n"
	var first *ParseError
	for i := 0; i < 10; i++ {
		p := c.NewParser()
		_, err := p.Feed([]byte(bad))
		perr, ok := err.(*ParseError)
		if !ok {
			t.Fatalf("run %d: err type %T", i, err)
		}
		if first == nil {
			first = perr
			continue
		}
		if perr.Reason != first.Reason || perr.Offset != first.Offset || perr.Detail != first.Detail {
			t.Fatalf("run %d: %v != %v", i, perr, first)
		}
	}
}
