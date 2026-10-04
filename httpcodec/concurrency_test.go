package httpcodec

import (
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"testing"
)

// corpus 生成一组确定性的测试报文流。
func corpus(n int) string {
	var b strings.Builder
	for i := 0; i < n; i++ {
		fmt.Fprintf(&b, "POST /r%d HTTP/1.1\r\nHost: h%d\r\nContent-Length: 4\r\n\r\nbody", i, i)
		fmt.Fprintf(&b, "GET /g%d HTTP/1.1\r\n\r\n", i)
	}
	return b.String()
}

// parseDeterministic 用固定配置与分片方式解析，返回完整结果与错误串。
func parseDeterministic(cfg Config, raw string, frag int) ([]msg, string) {
	p := NewParser(Request, cfg)
	ms, err := feedAll(p, true, splitEvery(raw, frag)...)
	errStr := ""
	if err != nil {
		errStr = err.Error()
	}
	return ms, errStr
}

func TestReproducible(t *testing.T) {
	raw := corpus(20)
	cfg := Config{MaxHeaderLine: 1024}
	base, baseErr := parseDeterministic(cfg, raw, 3)
	for i := 0; i < 5; i++ {
		ms, errStr := parseDeterministic(cfg, raw, 3)
		if errStr != baseErr {
			t.Fatalf("run %d: error mismatch %q vs %q", i, errStr, baseErr)
		}
		if len(ms) != len(base) {
			t.Fatalf("run %d: message count mismatch", i)
		}
		for j := range ms {
			if string(ms[j].body) != string(base[j].body) ||
				ms[j].head.Method != base[j].head.Method ||
				ms[j].end != base[j].end {
				t.Fatalf("run %d msg %d differs", i, j)
			}
		}
	}
	// 相同输入下错误也必须一致：构造一个超限输入。
	bad := "GET / HTTP/1.1\r\nX: " + strings.Repeat("a", 5000) + "\r\n\r\n"
	_, e1 := parseDeterministic(Config{MaxHeaderLine: 100}, bad, 7)
	_, e2 := parseDeterministic(Config{MaxHeaderLine: 100}, bad, 13)
	if e1 == "" || e1 != e2 {
		t.Fatalf("errors not reproducible: %q vs %q", e1, e2)
	}
}

func TestConcurrentParsers(t *testing.T) {
	// 多个连接各自驱动独立 Parser，共享同一份 Config，状态不得互相污染。
	cfg := Config{MaxHeaderLine: 4096}
	raw := corpus(10)
	want, _ := parseDeterministic(cfg, raw, 5)

	var wg sync.WaitGroup
	errs := make(chan string, 64)
	for g := 0; g < 32; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			// 每个 goroutine 用不同的分片大小模拟各自连接的到达模式。
			ms, errStr := parseDeterministic(cfg, raw, 1+g%17)
			if errStr != "" {
				errs <- fmt.Sprintf("goroutine %d: %s", g, errStr)
				return
			}
			if len(ms) != len(want) {
				errs <- fmt.Sprintf("goroutine %d: %d messages, want %d", g, len(ms), len(want))
				return
			}
			for i := range ms {
				if string(ms[i].body) != string(want[i].body) ||
					ms[i].head.Target != want[i].head.Target {
					errs <- fmt.Sprintf("goroutine %d msg %d polluted", g, i)
					return
				}
			}
		}(g)
	}
	wg.Wait()
	close(errs)
	for e := range errs {
		t.Fatal(e)
	}
}

// TestNetPipe 验证与内存管道的配合：一端序列化多条报文，
// 另一端按任意到达片段增量解析，读尽后剩余字节不越界。
func TestNetPipe(t *testing.T) {
	c1, c2 := net.Pipe()
	defer c1.Close()
	defer c2.Close()

	go func() {
		s := NewSerializer(c1)
		for i := 0; i < 3; i++ {
			m := &Message{
				Head: Head{Kind: Request, Proto: "HTTP/1.1", Method: "POST",
					Target: fmt.Sprintf("/p%d", i), Delim: DelimLength, Length: 3},
				Body: strings.NewReader("abc"),
			}
			if err := s.Write(m); err != nil {
				return
			}
		}
		c1.Close()
	}()

	p := NewParser(Request, Config{})
	var got []msg
	buf := make([]byte, 7) // 故意用小缓冲制造任意分片
	for {
		n, err := c2.Read(buf)
		if n > 0 {
			p.Feed(buf[:n])
			ms, cerr := collect(p, got)
			got = ms
			if cerr != nil && cerr != io.EOF {
				t.Fatalf("parse: %v", cerr)
			}
		}
		if err == io.EOF {
			p.FeedEOF()
			ms, _ := collect(p, got)
			got = ms
			break
		}
		if err != nil {
			t.Fatalf("read: %v", err)
		}
	}
	if len(got) != 3 {
		t.Fatalf("want 3 messages, got %d", len(got))
	}
	for i, m := range got {
		if m.head.Target != fmt.Sprintf("/p%d", i) || string(m.body) != "abc" {
			t.Fatalf("message %d mismatch: %+v", i, m)
		}
		if m.end != EndComplete {
			t.Fatalf("message %d bad end: %v", i, m.end)
		}
	}
}
