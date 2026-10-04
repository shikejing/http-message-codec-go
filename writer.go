package httpcodec

import (
	"bufio"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"
)

// writeChunkSize is the body read granularity used when streaming a chunked
// message; each read becomes at most one chunk.
const writeChunkSize = 32 << 10

// Writer serializes messages to an io.Writer according to a Codec config.
// A Writer holds no per-message state and may be reused sequentially.
type Writer struct {
	cfg Config
}

// NewWriter builds a Writer from a Codec.
func NewWriter(c *Codec) *Writer {
	return &Writer{cfg: c.cfg}
}

// WriteMessage serializes msg and its body to w. The body framing on the
// wire follows msg.Delimiter: a chunked message stays chunked (it is never
// collapsed into a Content-Length form), a Content-Length message is
// verified against the declared length, DelimUntilClose writes raw bytes,
// and DelimNone writes no body. body may be nil when msg.Delimiter is
// DelimNone. For chunked messages, msg.Trailer is written after the
// terminal chunk.
func (wr *Writer) WriteMessage(w io.Writer, msg *Message, body io.Reader) error {
	bw := bufio.NewWriter(w)
	if err := wr.writeStartLine(bw, msg); err != nil {
		return err
	}
	if err := wr.writeHeaders(bw, msg); err != nil {
		return err
	}
	if _, err := bw.WriteString("\r\n"); err != nil {
		return err
	}
	switch msg.Delimiter {
	case DelimNone:
		// no body
	case DelimContentLength:
		if body == nil {
			if msg.ContentLength != 0 {
				return fmt.Errorf("httpcodec: Content-Length %d but no body provided", msg.ContentLength)
			}
			break
		}
		n, err := io.Copy(bw, body)
		if err != nil {
			return err
		}
		if n != msg.ContentLength {
			return fmt.Errorf("httpcodec: Content-Length %d but body supplied %d bytes", msg.ContentLength, n)
		}
	case DelimChunked:
		if body != nil {
			if err := writeChunkedBody(bw, body); err != nil {
				return err
			}
		}
		if _, err := bw.WriteString("0\r\n"); err != nil {
			return err
		}
		if msg.Trailer != nil {
			for _, f := range wr.orderedFields(msg.Trailer) {
				if _, err := bw.WriteString(wr.fieldName(f.Name) + ": " + f.Value + "\r\n"); err != nil {
					return err
				}
			}
		}
		if _, err := bw.WriteString("\r\n"); err != nil {
			return err
		}
	case DelimUntilClose:
		if body != nil {
			if _, err := io.Copy(bw, body); err != nil {
				return err
			}
		}
	}
	return bw.Flush()
}

func (wr *Writer) writeStartLine(bw *bufio.Writer, msg *Message) error {
	var line string
	if msg.IsRequest {
		line = msg.Method + " " + msg.Target + " " + msg.Proto + "\r\n"
	} else {
		reason := msg.ReasonPhrase
		if reason == "" {
			reason = defaultReason(msg.StatusCode)
		}
		line = msg.Proto + " " + strconv.Itoa(msg.StatusCode) + " " + reason + "\r\n"
	}
	_, err := bw.WriteString(line)
	return err
}

func (wr *Writer) writeHeaders(bw *bufio.Writer, msg *Message) error {
	for _, f := range wr.orderedFields(msg.Header) {
		if _, err := bw.WriteString(wr.fieldName(f.Name) + ": " + f.Value + "\r\n"); err != nil {
			return err
		}
	}
	// Guarantee the wire framing matches msg.Delimiter.
	switch msg.Delimiter {
	case DelimContentLength:
		if _, ok := msg.Header.Get("Content-Length"); !ok {
			if _, err := bw.WriteString("Content-Length: " + strconv.FormatInt(msg.ContentLength, 10) + "\r\n"); err != nil {
				return err
			}
		}
	case DelimChunked:
		if _, ok := msg.Header.Get("Transfer-Encoding"); !ok {
			if _, err := bw.WriteString("Transfer-Encoding: chunked\r\n"); err != nil {
				return err
			}
		}
	}
	return nil
}

// orderedFields returns the header fields in the order selected by the
// configured HeaderOrderPolicy.
func (wr *Writer) orderedFields(h *Headers) []Header {
	fields := h.All()
	if wr.cfg.HeaderOrder == OrderSort {
		sort.SliceStable(fields, func(i, j int) bool {
			return strings.ToLower(fields[i].Name) < strings.ToLower(fields[j].Name)
		})
	}
	return fields
}

// fieldName applies the configured HeaderCasePolicy to a header name.
func (wr *Writer) fieldName(name string) string {
	switch wr.cfg.HeaderCase {
	case CaseLower:
		return strings.ToLower(name)
	case CaseCanonical:
		return canonicalHeaderName(name)
	}
	return name
}

// canonicalHeaderName returns MIME canonical form: first letter and letters
// after '-' upper-cased, the rest lower-cased.
func canonicalHeaderName(s string) string {
	b := []byte(s)
	upper := true
	for i, c := range b {
		if upper && c >= 'a' && c <= 'z' {
			b[i] = c - ('a' - 'A')
		} else if !upper && c >= 'A' && c <= 'Z' {
			b[i] = c + ('a' - 'A')
		}
		upper = c == '-'
	}
	return string(b)
}

// writeChunkedBody streams r to w as chunked transfer coding.
func writeChunkedBody(w *bufio.Writer, r io.Reader) error {
	buf := make([]byte, writeChunkSize)
	for {
		n, err := r.Read(buf)
		if n > 0 {
			if _, werr := fmt.Fprintf(w, "%x\r\n", n); werr != nil {
				return werr
			}
			if _, werr := w.Write(buf[:n]); werr != nil {
				return werr
			}
			if _, werr := w.WriteString("\r\n"); werr != nil {
				return werr
			}
		}
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
	}
}

func defaultReason(code int) string {
	switch code {
	case 100:
		return "Continue"
	case 101:
		return "Switching Protocols"
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
	case 401:
		return "Unauthorized"
	case 403:
		return "Forbidden"
	case 404:
		return "Not Found"
	case 405:
		return "Method Not Allowed"
	case 408:
		return "Request Timeout"
	case 413:
		return "Content Too Large"
	case 431:
		return "Request Header Fields Too Large"
	case 500:
		return "Internal Server Error"
	case 501:
		return "Not Implemented"
	case 502:
		return "Bad Gateway"
	case 503:
		return "Service Unavailable"
	}
	return "Status"
}
