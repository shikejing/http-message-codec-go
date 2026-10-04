# http-message-codec-go

可增量解析与序列化的 HTTP/1.x 报文编解码库。纯进程内实现，不依赖任何外部服务。
输入可以来自本地回环连接或内存管道（如 `net.Pipe`），一条报文可以被拆成任意
大小的片段陆续到达，多条报文也可以首尾相接共用同一条连接。

## 快速上手

```go
p := httpcodec.NewParser(httpcodec.Request, httpcodec.Config{})
p.Feed(chunk1)            // 任意大小的片段，可逐字节喂入
p.Feed(chunk2)
for {
    ev, err := p.Next()
    if err != nil { /* *ParseError 或 io.EOF */ }
    switch ev.Type {
    case httpcodec.EventNeedData:   // 需要继续 Feed
    case httpcodec.EventHead:       // p.Head() 可取起始行与头部
    case httpcodec.EventBody:       // p.BodyChunk() 流式给出报文体片段
    case httpcodec.EventTrailer:    // p.Trailer() 给出尾部字段
    case httpcodec.EventMessageEnd: // p.EndReason() 给出结束原因
    }
}
```

序列化：

```go
s := httpcodec.NewSerializer(w) // w 为任意 io.Writer
s.Case = httpcodec.CaseCanonical // 头部大小写策略：Preserve / Lower / Canonical
s.Order = httpcodec.OrderSort    // 排列顺序策略：Preserve / Sort
s.Write(&httpcodec.Message{
    Head: httpcodec.Head{Kind: httpcodec.Request, Proto: "HTTP/1.1",
        Method: "POST", Target: "/up", Delim: httpcodec.DelimChunked},
    Body:    bodyReader,             // 流式读取，不会整体载入内存
    Trailer: []httpcodec.Field{{"X-Sum", "ff"}},
})
```

序列化结果可以被自身解析回等价报文；`DelimChunked` 的报文体始终按分块
编码输出，绝不改写为带 `Content-Length` 的一次性形式。

## 定界规则

报文体定界方式由头部决定，解析器在头部结束时判定：

| 条件 | 定界方式 |
| --- | --- |
| `Transfer-Encoding: chunked`（可含扩展参数外的逗号列表，每项都必须是 chunked） | `DelimChunked` |
| 一个或多个取值相同的 `Content-Length` | `DelimLength` |
| 响应状态码 1xx / 204 / 304 | 无报文体 |
| 响应且无上述指示 | `DelimClose`（连接关闭为界） |
| 请求且无上述指示 | 无报文体 |

**冲突即非法**：`Transfer-Encoding` 与 `Content-Length` 同时出现，或多个
`Content-Length` 取值不一致，都返回 `ErrConflictingLength`，不会按其中
一种静默放行。分块大小行必须是合法十六进制（`;` 后的扩展被忽略），每块
数据之后必须紧跟 CRLF，最后一块之后的尾部字段会被解析并可通过
`Trailer()` 查询。

## 与连接的配合语义

- 同一连接上多条报文依次处理：一条报文读尽后，缓冲中多出的字节严格属于
  下一条报文，绝不越界计入当前报文体。
- `EndReason()` 区分三种结束：`EndComplete`（按声明定界读完）、
  `EndClose`（连接关闭定界）、`EndUpgrade`（协议升级：101 响应、
  带 `Upgrade` 头部的报文、`CONNECT` 请求）。
- 连接提前关闭：报文中间收到 EOF 返回 `ErrTruncated`，不会当成正常结束；
  报文边界上收到 EOF 返回 `io.EOF`，表示干净关闭。
- 出错恢复：出错后调用 `Reset()` 丢弃当前报文的全部残留状态与缓冲字节，
  之后喂入的字节从下一条报文边界重新解析，残留字节不会被误当成新报文。

## 配置项（Config）

零值即默认配置，所有上限均可独立调整：

| 字段 | 默认 | 含义 |
| --- | --- | --- |
| `MaxStartLine` | 8192 | 起始行最大字节数 |
| `MaxHeaderLine` | 8192 | 单个头部行最大字节数 |
| `MaxHeaderBytes` | 1 MiB | 头部区总大小上限 |
| `MaxChunks` | 65536 | 分块数量上限 |
| `MaxBodyBytes` | 1 GiB | 报文体总大小上限 |

解析器只在内部保留尚未消费的输入片段，报文体以事件流式输出，恶意输入
不会造成内存无界增长；超限立即报错，不会长时间占用。

## 错误语义

所有协议错误都是 `*ParseError`，携带 `Reason`（可区分的原因）与
`Offset`（相对输入流的字节偏移，可定位出错位置）：

| Reason | 含义 |
| --- | --- |
| `ErrMalformedStartLine` | 起始行畸形 |
| `ErrMalformedHeader` | 头部字段畸形（缺冒号、非法字段名、obs-fold 折叠等） |
| `ErrLineTooLong` | 单行超过 `MaxStartLine`/`MaxHeaderLine` |
| `ErrHeaderTooLarge` | 头部区总大小超限 |
| `ErrBadChunk` | 分块大小行非法或块数据后缺少 CRLF |
| `ErrTooManyChunks` | 分块数量超限 |
| `ErrBodyTooLarge` | 报文体规模超限 |
| `ErrConflictingLength` | TE 与 CL 并存，或多个 CL 取值矛盾 |
| `ErrTruncated` | 报文中间连接关闭（截断） |

`io.EOF` 不通过 `ParseError` 返回：它只表示连接在报文边界上干净关闭。

## 并发与可复现

`Parser` 不是并发安全的，但每个连接使用独立的 `Parser`、共享同一份
`Config`（值类型，拷贝即用）时，状态完全隔离、互不污染。相同输入与
配置下，解析结果与错误（含偏移）逐条一致，可稳定复现。

## 测试

```sh
go test ./...        # 单元测试
go test -race ./...  # 竞态检测
```

覆盖：分片到达（逐字节/任意大小）、定界冲突、相邻报文、截断与畸形输入、
安全上限、序列化回环、并发复用与 `net.Pipe` 内存管道场景。
