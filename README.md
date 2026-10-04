# http-message-codec-go

增量式 HTTP/1.x 报文编解码库。输入可以来自本地回环连接或内存管道，一条报文
可以被拆成任意大小的片段陆续到达，多条报文可以首尾相接共用同一连接。整条
链路只在本进程内运行，不依赖任何外部服务。

## 基本用法

```go
c := httpcodec.NewCodec(httpcodec.DefaultConfig())

// 每个连接一个 Parser；Codec 本身不可变，可并发共享。
p := c.NewParser()
events, err := p.Feed(chunk)   // chunk 可以是任意大小的片段
// ... 连接关闭时：
events, err = p.Close()

// 序列化：
w := c.NewWriter()
err = w.WriteMessage(dst, msg, bodyReader)
```

`Feed` 返回事件流：`EventHead`（起始行+头部解析完成）、`EventBody`（一段解
码后的报文体，已拷贝，可安全持有）、`EventTrailers`（分块报文的尾部字段，
即使为空也会发出）、`EventMessageEnd`（报文读尽，`Message.End` 给出结束原
因）。报文体以事件流式交付，不会整体读入内存。

## 定界规则

解析器按以下优先级决定报文体定界方式（`Message.Delimiter`）：

1. **Transfer-Encoding 与 Content-Length 同时出现**：判定为非法
   （`ReasonConflictingFraming`），不按任何一种静默放行。
2. **Transfer-Encoding**：仅支持且必须恰好为 `chunked`；其他编码（如
   `gzip`）或组合编码判定为非法。
3. **Content-Length**：可以重复出现，但所有值必须相等；值不等判定为
   `ReasonConflictingFraming`。非十进制数字、空值判定为
   `ReasonMalformedHeader`。
4. **响应且状态码为 1xx / 204 / 304**：无报文体。状态码 **101** 表示协议
   升级（见下）。
5. 请求无上述字段：`DelimNone`（无报文体）；响应无上述字段：
   `DelimUntilClose`（读到连接关闭为止）。

分块编码严格校验：块大小行必须是合法十六进制（允许 `;` 后的扩展，扩展被
忽略），块数据后必须紧跟 CRLF（否则 `ReasonMalformedChunkEnd`），最后一块
之后的尾部字段会被解析，可通过 `Message.Trailer` 或 `EventTrailers` 查询。

### 结束原因（`Message.End`）

| EndReason | 含义 |
|---|---|
| `EndNone` | 报文无报文体 |
| `EndContentLength` | 声明长度已读尽 |
| `EndChunked` | 终止块与尾部字段已消费 |
| `EndConnectionClose` | 报文体由连接关闭（EOF）界定 |
| `EndUpgrade` | 101 升级，连接切换到其他协议 |

升级后 `Parser.Remaining()` 返回缓冲区中属于新协议的残留字节，解析器不再
消费后续数据。连接提前关闭时 `Close` 返回 `ReasonTruncated` 错误——被截断
的报文绝不会被当成正常结束。

### 序列化

`Writer.WriteMessage` 按 `Message.Delimiter` 输出线上格式：分块报文**始终
保持分块**，不会被改写成带 Content-Length 的一次性形式；定长报文会校验实
际字节数与声明长度一致；`DelimUntilClose` 原样写出。头部缺少定界字段时由
Writer 自动补齐（Content-Length 或 Transfer-Encoding: chunked）。头部字段
的大小写（`HeaderCase`：保留/全小写/规范式）与排列顺序（`HeaderOrder`：
保留/按名排序）可配置。序列化结果可以被自身解析器等价解析（见
`writer_test.go` 的往返测试）。

## 配置项（`Config`）

零值/负值字段由 `DefaultConfig()` 的默认值填充。

| 字段 | 默认 | 含义 |
|---|---|---|
| `MaxHeaderLineLen` | 8 KiB | 单个头部行（含起始行、块大小行）最大长度，超限报 `ReasonHeaderLineTooLong` |
| `MaxHeaderBytes` | 64 KiB | 头部区总大小（起始行+全部头部行），超限报 `ReasonHeadersTooLarge` |
| `MaxChunkCount` | 65536 | 单个报文最大分块数，超限报 `ReasonTooManyChunks` |
| `MaxChunkSize` | 8 MiB | 单个分块声明大小上限，超限报 `ReasonChunkTooLarge` |
| `MaxBodyBytes` | 64 MiB | 单条报文解码后累计报文体上限，超限报 `ReasonBodyTooLarge` |
| `HeaderCase` | 保留 | 序列化头部名大小写策略 |
| `HeaderOrder` | 保留 | 序列化头部排列策略 |

所有上限在数据到达时即时检查：超长行在凑齐之前就报错，报文体边到边发，
解析器内部缓冲不超过一个未完成的行，恶意输入不会造成内存无界增长。

## 错误语义

所有解析失败返回 `*ParseError`，包含：

- `Reason`：可区分的原因（`ErrorReason` 枚举，见下表）；
- `Offset`：错误在连接字节流中的绝对偏移；
- `Detail`：补充说明。

| Reason | 触发条件 |
|---|---|
| `ReasonMalformedStartLine` | 请求行/状态行畸形 |
| `ReasonMalformedHeader` | 头部行无冒号、名字非法、值含控制字符、obs-fold、裸 LF |
| `ReasonHeaderLineTooLong` | 单行超过 `MaxHeaderLineLen` |
| `ReasonHeadersTooLarge` | 头部区超过 `MaxHeaderBytes` |
| `ReasonConflictingFraming` | TE 与 CL 并存、CL 值互相矛盾、不支持的传输编码 |
| `ReasonMalformedChunkSize` | 块大小行不是合法十六进制 |
| `ReasonMalformedChunkEnd` | 块数据后缺少 CRLF |
| `ReasonTooManyChunks` / `ReasonChunkTooLarge` / `ReasonBodyTooLarge` | 超过对应上限 |
| `ReasonTruncated` | 连接在报文中途关闭 |

### 恢复

出错后解析器进入失败状态，后续 `Feed` 直接返回同一错误。调用
`Parser.Recover()` 会丢弃当前报文及其全部已缓冲字节（残留字节**不会**被
误当成新报文），之后从下一次 `Feed` 的数据开始、在下一条报文边界继续解析。

## 并发与可复现

`Codec` 创建后不可变，可被任意数量的 goroutine 共享；每个连接使用独立的
`Parser`（`Codec.NewParser()`），解析器之间不共享任何可变状态，交替驱动
互不污染。相同输入、相同配置、相同分片模式下，事件序列与错误逐条一致
（`concurrency_test.go` 在 `-race` 下验证）。

## 测试

```
go test -race ./...
```

覆盖：逐字节/任意分片到达、定界冲突、相邻（流水线）报文、截断与畸形输
入、安全上限、并发复用、net.Pipe 内存管道上的端到端收发与序列化往返。
