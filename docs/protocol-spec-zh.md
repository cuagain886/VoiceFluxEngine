# voicestream 线路协议规范（v1）

> 本文是 **规范性（normative）** 文档：它定义客户端与服务端之间在网络上交换
> 的**确切字节**。设计动机与取舍见 [M2-transport-design-zh.md](M2-transport-design-zh.md)
> 与 [M8-session-design.md](M8-session-design.md)；本文只讲"线上长什么样、各方
> 必须怎么做"。
>
> 文中 **MUST / MUST NOT / SHOULD / MAY** 按 RFC 2119 理解。
> 协议版本：`version = 1`（见帧头第 2 字节）。

---

## 0. 适用范围与不变量

| 维度 | v1 约定 |
|------|---------|
| 传输 | **WebSocket only**（`ws://` 或 `wss://`），二进制消息 |
| 端点 | `GET /ws`（HTTP Upgrade 到 WebSocket） |
| 音频 | PCM，**16 kHz / 16-bit 有符号 / 单声道**，假定在线上已是该格式 |
| 分帧 | **一条 WebSocket 二进制消息 = 恰好一个协议帧**（见 §1） |
| 字节序 | 帧头：**大端**（网络字节序）；PCM 采样：**小端**（见 §3.1） |
| 文本/控制编码 | **protobuf**（proto3，见 §4） |
| 音频编码 | **裸 PCM 字节**，不做 protobuf 包裹 |

被显式推迟、**不属于** v1 的内容：FFmpeg 编解码转换、gRPC / WebTransport 传输、
单连接内的乱序重排窗口（单条 WS/TCP 连接本身保序，见 §6）。

---

## 1. 分帧（framing）

WebSocket 已经提供了**消息边界**——这是它相对裸 TCP 的关键便利。因此 v1 直接令：

> **每条 WebSocket 二进制消息承载且仅承载一个完整帧。**

接收方 MUST 把整条消息当作一帧来 `Decode`；MUST NOT 在一条消息里拼接多帧，
也 MUST NOT 把一帧拆到多条消息里。文本类 WebSocket 帧（opcode text）不被使用，
收到 SHOULD 忽略。

> 帧头里仍带有 `length` 字段（§2），这不是给 WebSocket 用的冗余，而是为了让同一
> 套帧格式**也能跑在自身不分帧的字节流传输上**（裸 TCP / WebTransport datagram
> 拼接）。在 WS 上它是一道额外校验：`length` 必须与消息体长度一致，否则拒帧。

---

## 2. 帧头（fixed binary header，24 字节）

帧头**手写、定长 24 字节、大端**，不经 protobuf —— 解析便宜、零分配。

```
偏移  大小  字段      类型      含义
0     2     magic     uint8×2   同步/合法性校验，固定 0x56 0x53（ASCII "VS"）
2     1     version   uint8     协议版本，v1 = 1
3     1     type      uint8     FrameType：0=UNSPEC 1=AUDIO 2=TEXT 3=CONTROL
4     8     seq       uint64    单调递增序列号（每个方向各自计数）
12    8     ts_us     int64     采样时钟 PTS，单位微秒
20    4     length    uint32    payload 字节数
24    …     payload   bytes     负载（见 §3）
```

常量（见 `internal/transport/frame.go`）：

| 常量 | 值 | 含义 |
|------|----|------|
| `Magic0` `Magic1` | `0x56` `0x53` | 帧首魔数 "VS" |
| `Version` | `1` | 当前协议版本 |
| `HeaderSize` | `24` | 帧头固定长度 |
| `MaxPayload` | `65536`（64 KiB） | 单帧负载上限 |

### 2.1 字段语义

- **`seq`** —— 每个**方向**独立的单调递增序列号（上行一套、下行一套），从握手后
  的首帧起按 1 递增。它服务于重连去重（§6）：重连方上报"我已收到的最大 seq"，
  对端据此跳过重放。它**不是**用来做连接内重排的（单连接天然保序）。
- **`ts_us`** —— **采样时钟**的呈现时间戳（presentation timestamp），即"这段音频在
  说话人时间轴上的位置"，而非发送墙钟。20 ms 一帧时它名义上每帧 +20000。延迟测量
  以它为基准（见 [M9-metrics-design.md](M9-metrics-design.md)），所以整形/排队带来的
  到达抖动不会污染 PTS。
- **`length`** —— 仅描述 payload，不含帧头。MUST ≤ `MaxPayload`，否则收发两端都 MUST
  拒绝（防御损坏/恶意长度）。

---

## 3. 负载（payload）

负载编码**由 `type` 决定**：

| `type` | 取值 | 负载编码 |
|--------|------|----------|
| `AUDIO` | 1 | **裸 PCM 字节**，无任何包裹 |
| `TEXT` | 2 | protobuf `TextPayload`（§4） |
| `CONTROL` | 3 | protobuf `ControlPayload`（§4） |
| `UNSPECIFIED` | 0 | 非法，收到 MUST 拒帧 |

### 3.1 AUDIO 负载（裸 PCM）

- 采样格式：**16-bit 有符号整数，小端（little-endian）**，单声道，16 kHz。
- 一个 20 ms 帧 = 320 采样 = **640 字节**。
- **为什么音频不走 protobuf**：PCM 是定长数值流，protobuf 包裹只会徒增每帧的编码
  开销与一次分配，对内核的"每帧"热路径不可接受。控制面信息量小、需要演进，才用
  protobuf。
- 浏览器侧把 Web Audio 的 Float32 量化为 Int16 LE 上行；服务端按同样的 LE 解读
  （见 `web/app.js` 与 `internal/vad/vad.go`）。

### 3.2 TEXT / CONTROL 负载（protobuf）

按 §4 的 proto3 schema 序列化。字段都是 proto3 默认语义（未设 = 零值）。

---

## 4. protobuf schema（`internal/transport/transportpb/frame.proto`）

```proto
syntax = "proto3";
package voicestream.transport.v1;

// FrameType 标识帧所属的逻辑流；其值就写在定长帧头的 1 字节 type 字段里。
enum FrameType {
  FRAME_TYPE_UNSPECIFIED = 0;
  FRAME_TYPE_AUDIO   = 1;   // payload = 裸 PCM（非 protobuf）
  FRAME_TYPE_TEXT    = 2;   // payload = TextPayload
  FRAME_TYPE_CONTROL = 3;   // payload = ControlPayload
}

// TextSource 告诉客户端这条 TEXT 帧属于哪条"对话车道"，
// 好让"用户转写"与"Agent 字幕"分开渲染。
enum TextSource {
  TEXT_SOURCE_UNSPECIFIED = 0;
  TEXT_SOURCE_TRANSCRIPT  = 1; // ASR 结果：用户说了什么
  TEXT_SOURCE_TOKEN       = 2; // LLM token：Agent 正在说什么
}

// TextPayload 承载增量或最终的 转写 / token。
message TextPayload {
  string     text   = 1;
  bool       final  = 2; // true = 最终（如 ASR final / 一句话结束）
  TextSource source = 3;
}

// ControlKind 枚举控制面信号。
enum ControlKind {
  CONTROL_KIND_UNSPECIFIED = 0;
  CONTROL_KIND_START    = 1;
  CONTROL_KIND_STOP     = 2;
  CONTROL_KIND_BARGE_IN = 3;
  CONTROL_KIND_ERROR    = 4;
}

// ControlPayload 承载控制面信号。START 兼作会话握手（见 §5）。
message ControlPayload {
  ControlKind kind       = 1;
  string      detail     = 2; // 可选的人类可读信息（如错误消息）
  string      session_id = 3;
  uint64      epoch      = 4;
  uint64      last_seq   = 5;
}
```

重新生成（toolchain：`protoc` + `protoc-gen-go`）：

```
protoc --go_out=. --go_opt=paths=source_relative internal/transport/transportpb/frame.proto
```

---

## 5. 会话与握手（CONTROL = START）

`START` 控制帧**兼作会话握手**。详见 [M8-session-design.md](M8-session-design.md)；
线上契约如下。

### 5.1 新建会话

```
客户端 → 服务端   CONTROL{ kind=START, session_id="", epoch=0 }
服务端 → 客户端   CONTROL{ kind=START, session_id=<服务端分配>, epoch=<权威>, last_seq=0 }
```

服务端回的 START 即 **ack**：其中的 `session_id` / `epoch` 是**权威值**，客户端 MUST
采用。此后双方按各自方向的 `seq` 计数发送 AUDIO / TEXT / CONTROL。

### 5.2 重连续接（resume）

掉线后客户端用**同一个** `session_id` 重发 START，并：

- `epoch` MUST **严格大于**它已知的上一个 epoch（声明"我是更新的那条连接"，
  让服务端淘汰可能仍挂着的旧连接 —— 这是抢占式接管，避免双写）；
- `last_seq` = 客户端**已收到**的最大下行 `seq`。

```
客户端 → 服务端   CONTROL{ kind=START, session_id=<旧>, epoch=<旧+1 或更大>, last_seq=<已收下行 seq> }
服务端 → 客户端   CONTROL{ kind=START, session_id=<同>, epoch=<权威>,
                          last_seq=<服务端已交付的最大上行 seq> }
```

- 服务端据客户端的 `last_seq` **跳过重放**它已经收到的下行（≤ 该值的不再补发）。
- 客户端据服务端 ack 的 `last_seq` 跳过重发它已被收到的上行。
- 过期 epoch（≤ 当前）的 START MUST 被拒绝/忽略，并计入
  `voicestream_stale_epoch_claims_total`。
- 会话在断开后存活至多 `session.idle_timeout`（兼作重连宽限期）；超时即被回收
  （`voicestream_sessions_reclaimed_total`）。

> **去重为什么只用一条 seq 水位、而非重排窗口**：单条 WS/TCP 连接内的帧本就保序，
> 不存在连接内乱序，所以一条"最高已见 seq"水位足以在重连重放时去重。**不要**宣称
> 内核提供了 TCP/WS 本就提供的保序能力（设计 D12/D13 的诚实边界）。

### 5.3 其余控制信号

| `kind` | 方向 | 含义 |
|--------|------|------|
| `STOP` | 双向 | 优雅结束会话/当前轮 |
| `BARGE_IN` | 客户端 → 服务端 | 用户打断：立即取消当前 Agent 应答（见 §5.4） |
| `ERROR` | 服务端 → 客户端 | 错误通知，`detail` 为人类可读信息 |

### 5.4 打断（barge-in）—— 内核的核心职责之一

当用户在 Agent 应答途中开口，客户端发 `CONTROL{kind=BARGE_IN}`（也可由服务端内联
VAD 自行触发，见 [M6-vad-bargein-design-zh.md](M6-vad-bargein-design-zh.md)）。服务端 MUST：

1. 取消当前轮的 LLM→TTS 子链；
2. 冲刷下行音频出口（不再发已排队的旧应答帧）；
3. 计入 `voicestream_turns_cancelled_total`，并把"取消请求 → 子链拆除且出口冲刷
   完成"的时延记入 `voicestream_barge_in_cancel_seconds`。

预算口径：内核取消 p99 ≤ 200 ms。

---

## 6. 错误处理与拒帧

接收方遇到下列情况 MUST 拒绝该帧（对应 `internal/transport/frame.go` 的 sentinel
错误，可用 `errors.Is` 匹配）：

| 情况 | 错误 |
|------|------|
| 消息短于 24 字节帧头 | `ErrShortHeader` |
| 魔数不是 `0x56 0x53` | `ErrBadMagic` |
| `version` 与本端不兼容 | `ErrVersion` |
| `length > MaxPayload`（64 KiB） | `ErrPayloadTooLarge` |
| `length` 与实际 payload 长度不符 | `ErrFrameLength` |

服务端对致命情形 SHOULD 回一个 `CONTROL{kind=ERROR, detail=…}` 再关闭连接；
对单帧的非致命瑕疵 MAY 丢弃该帧并继续。

---

## 7. 版本演进

- 帧头的 `version` 字段是**前向兼容的总闸**：不兼容的破坏性改动 MUST 提升它，
  接收方对未知版本 MUST 拒帧（`ErrVersion`）。
- protobuf 消息按 proto3 规则演进：**只新增字段、不复用旧 tag、不改旧字段语义**，
  老客户端能安全忽略新字段。
- `FrameType` / `ControlKind` / `TextSource` 新增枚举值时，旧接收方 SHOULD 把未知
  值按"忽略/UNSPECIFIED"处理，不得崩溃。

---

## 8. 一帧的最小往返（示例时间线）

```
① 握手     C→S  CONTROL START(session_id="", epoch=0)
           S→C  CONTROL START(session_id="s-1", epoch=1, last_seq=0)
② 上行语音  C→S  AUDIO seq=1 ts_us=0      payload=640B PCM
           C→S  AUDIO seq=2 ts_us=20000   payload=640B PCM   … 持续 20ms/帧
③ 转写     S→C  TEXT  source=TRANSCRIPT final=true  text="今天天气怎么样"
④ 应答     S→C  TEXT  source=TOKEN  text="今"  text="天"…（流式 token）
           S→C  AUDIO seq=… ts_us=…   payload=PCM（TTS 合成的应答音频）
⑤ 打断     C→S  CONTROL BARGE_IN     → 服务端取消③④的在飞子链、冲刷出口
```

---

## 附：与 M2 设计文档的关系

- 本文 = **契约**（线上字节、各方义务）。
- [M2-transport-design-zh.md](M2-transport-design-zh.md) = **为什么这么设计**（手写帧头 vs
  全 protobuf、WebSocket vs 裸 TCP、传输无关 `Conn` 抽象等取舍）。
- 两者若出现分歧，以**代码**（`internal/transport/`）+ 本规范为准，并回填设计文档。
