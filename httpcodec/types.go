// Package httpcodec 提供可增量解析与序列化的 HTTP/1.x 报文编解码能力。
package httpcodec

// Kind 表示报文种类。
type Kind int

const (
	Request Kind = iota
	Response
)

// Field 是一个头部字段，保持原始名称与值。
type Field struct {
	Name  string
	Value string
}

// Delim 表示报文体的定界方式。
type Delim int

const (
	DelimNone    Delim = iota // 无报文体
	DelimLength               // Content-Length 定界
	DelimChunked              // 分块传输编码
	DelimClose                // 由连接关闭定界（仅响应）
)

// EndReason 表示一条报文结束的原因，便于区分正常结束、连接关闭与协议升级。
type EndReason int

const (
	EndNone     EndReason = iota
	EndComplete           // 按声明的定界方式完整读完
	EndClose              // 连接关闭作为报文体边界
	EndUpgrade            // 协议升级（如 101 / CONNECT），后续字节不再按 HTTP 解析
)

// Head 是报文头部（起始行 + 头部字段），Body 通过事件流式输出。
type Head struct {
	Kind       Kind
	Proto      string
	Method     string // 仅请求
	Target     string // 仅请求
	StatusCode int    // 仅响应
	Reason     string // 仅响应
	Header     []Field
	Delim      Delim
	Length     int64 // DelimLength 时有效
	Upgrade    bool  // 是否协议升级
}

// EventType 是解析器吐出的事件类型。
type EventType int

const (
	EventNeedData   EventType = iota // 需要更多输入字节
	EventHead                        // 头部解析完成，可通过 Head() 获取
	EventBody                        // 产生一段报文体字节，可通过 BodyChunk() 获取
	EventTrailer                     // 尾部字段解析完成，可通过 Trailer() 获取
	EventMessageEnd                  // 当前报文结束，可通过 EndReason() 查询结束原因
)

// Event 是解析器单步推进的结果。
type Event struct {
	Type EventType
}

// Config 是编解码器的安全上限与行为配置，零值表示使用默认值。
type Config struct {
	MaxStartLine   int   // 起始行最大长度（字节），默认 8192
	MaxHeaderLine  int   // 单个头部行最大长度（字节），默认 8192
	MaxHeaderBytes int   // 头部区总大小上限（字节），默认 1<<20
	MaxChunks      int   // 分块数量上限，默认 1<<16
	MaxBodyBytes   int64 // 报文体总大小上限（字节），默认 1<<30
}

func (c Config) withDefaults() Config {
	if c.MaxStartLine <= 0 {
		c.MaxStartLine = 8192
	}
	if c.MaxHeaderLine <= 0 {
		c.MaxHeaderLine = 8192
	}
	if c.MaxHeaderBytes <= 0 {
		c.MaxHeaderBytes = 1 << 20
	}
	if c.MaxChunks <= 0 {
		c.MaxChunks = 1 << 16
	}
	if c.MaxBodyBytes <= 0 {
		c.MaxBodyBytes = 1 << 30
	}
	return c
}
