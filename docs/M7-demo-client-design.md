# M7 — 浏览器演示客户端与语音会话接线（设计）

状态：**已实现**（7.1–7.5 完成；7.6 L0 人工验收待真实麦克风确认）。

对应 spec `demo-client`，设计决策 **D6**（客户端 AEC）、**D7**（采样时钟 PTS）、
**D12**（demo 客户端临时充当边缘）、**D13**（TCP 抖动 = 突发到达，用播放缓冲抹平）。
任务：7.1–7.6。

## 1. 目标与组成

M7 是北极星 L0 的本体：**浏览器里完成一轮语音对话，并能在 Agent 应答中自然打断**。
两块内容：

| 部分 | 位置 | 职责 |
|---|---|---|
| 语音会话 handler | `internal/session/session.go` | 替换 M2 echo：每连接一套 pipeline + 内联 VAD，上行读、下行写 |
| 浏览器客户端 | `web/`（原生 JS，无框架） | 采音上行、播放缓冲、打断停播、字幕对齐 |

服务端在 M5/M6 已全部就绪，M7 的服务端工作只是**接线**：transport 的读
goroutine 调 `ctrl.Ingest`（VAD 内联，入口保持严格 SPSC），egress 环 →
唯一写 goroutine。`cmd/server` 不再用 echo，静态目录 `web/` 由同一端口直接服务
（`http://localhost:8080/`）。

## 2. 每连接 goroutine 模型（服务端）

```
reader      ReadFrame → ctrl.Ingest（VAD 内联）→ 入口环
pipeline    ASR → LLM → TTS（M5 内部自有 goroutine）
audio pump  出口环 → wire 队列，盖会话下行采样时钟戳
writer      唯一 WS 写者：AUDIO/TEXT/CONTROL 帧 + 心跳 ping
```

- **seq 在 writer 出队时统一分配**——单写者 = 单一序号分配点，天然单调。
- **audio pump 阻塞在 wire 队列上是安全的**：上游是 drop-oldest 出口环，
  内存有界、最新帧获胜，泵慢不会撑爆任何东西。
- **TEXT/CONTROL 走非阻塞入队**：字幕是尽力而为的 UX，客户端慢时计数丢弃
  （`subtitleDrops`），绝不反向阻塞 LLM forwarder——音频才是产品本体。
- 客户端 `CONTROL STOP` → 会话干净收尾；读错误/心跳超时 → 连接级拆除。

## 3. 下行时序与字幕对齐（7.5 的服务端半边）

线上 `ts_us` 不是 TTS 内部的逐轮 PTS（每轮从 0 重起，客户端没法用），而是
**会话级连续采样时钟**：累计已下发音频的微秒数，由 audio pump 在出队时重新盖戳。

token 的对齐靠一个结构性事实：**token 在它的语音被合成之前经过 forwarder**。
此刻的会话时钟值 ≈ 该 token 语音在下行流中的起点。所以 token TEXT 帧直接盖
当前时钟值，客户端等播放进度到达该值再揭示字幕——无需任何额外的对齐协议。
近似误差（出口环/wire 队列中尚未下发的存量）通常在一两帧内，文档如实记录。

转写（用户说的话）直接盖上行帧的 PTS 锚点立即显示，不参与对齐——用户不需要
等着看自己刚说的话。

proto 扩展：`TextPayload` 增加 `TextSource`（TRANSCRIPT / TOKEN）区分两条字幕道，
proto3 加字段向后兼容。

## 4. 浏览器客户端（7.1–7.4）

- **采音（7.1）**：`getUserMedia({echoCancellation:true, noiseSuppression, autoGainControl})`
  —— AEC 是防"Agent 自己的声音触发打断"的第一道闸（D6）。`AudioContext({sampleRate:16000})`
  让浏览器在内部完成重采样，采集与播放同钟，**全程无手写重采样**。AudioWorklet
  在渲染线程逐 128 采样块转发，主线程攒成 320 采样（20ms）帧 → Int16 LE →
  24 字节头（与 `frame.go` 逐字节一致，BigInt 写 u64/i64）→ WS 二进制上行（7.2）。
  上行 PTS = 已发采样数推导的采样时钟（D7），不用 wall-clock。
- **播放缓冲（7.3 / 7.3b）**：TCP 上抖动表现为突发到达（D13），缓冲先预存
  `targetDepth`（初始 120ms）再开播，之后按 PTS 经 WebAudio 精确排期
  （`source.start(at)`，无定时器抖动）。**欠载不爆音**：队列耗尽时转入静音、
  重新预存，同时 `targetDepth += 40ms`（上限 400ms）——抖动大则加深，
  平滑与延迟之间的取舍是自适应的、可在页面状态栏观察。
- **打断（7.4）**：服务端取消子链并 flush 内核侧在途音频后下发
  `CONTROL BARGE_IN`；客户端立即 `stop()` 所有已排期 source、清队列、丢弃
  未揭示 token。会话播放时钟前跳到最后接收位置，不回退——后续字幕对齐不乱。
- **protobuf 解码**：客户端手写了 ~40 行最小 varint/length-delimited 解码器，
  只覆盖 `TextPayload`/`ControlPayload` 两个消息——引一个 JS protobuf 运行时
  对两个三字段消息是杀鸡用牛刀，且手写线格式正是本项目的题中之义。

## 5. 验收与测试

- **服务端 e2e**（`session_test.go`，`-race` 三连干净）：合成"浏览器"走真实
  WS 栈完成整条链——上行语音帧（过 VAD 门限）→ 收到 final 转写 + token 字幕 +
  音频帧（seq 与 ts_us 单调性逐帧断言）→ 应答中插话 → 收到 `BARGE_IN` 控制帧；
  另有 `CONTROL STOP` 干净收尾用例。
- **7.6 L0 人工验收**（待执行）：`go run ./cmd/server` → 浏览器开
  `http://localhost:8080/` → 允许麦克风 → 说一句话 → 听到 Agent 回应、字幕
  跟随 → 应答中开口 → Agent 立即住口。全 mock 即可验收（ASR 固定脚本、
  TTS 合成静音帧——验收的是**时序/背压/打断**，不是模型质量）；接真实 LLM
  把 `adapters.llm` 切到 `openai-compat` 并设 `VOICESTREAM_LLM_API_KEY`。

## 6. 已知取舍（如实记录）

- mock TTS 输出为静音 PCM——打断验收靠字幕/状态栏与 BARGE_IN 行为观察，
  "听见"声音需接真实 TTS（后续 change）。
- token 对齐是结构性近似（§3），不是逐音素对齐；对字幕场景足够。
- 浏览器要求支持 `AudioContext({sampleRate})` 与 AudioWorklet（Chrome/Edge
  现代版本均可；demo 以 Chrome 为准）。
- 上行帧 payload 直接引用 WS 读缓冲（coder/websocket 每消息独立分配，安全）；
  上行路径未接对象池，浏览器侧帧率仅 50fps，留给 M11 按 pprof 实据决定。

## 参考

- Spec：`openspec/changes/streaming-multimodal-agent-engine/specs/demo-client/spec.md`
- 设计决策：D6（客户端 AEC）、D7（采样时钟 PTS）、D12（demo 临时充当边缘，
  生产位置属于 WebRTC SFU/网关）、D13（突发到达用播放缓冲抹平）。
- 消费：M2 transport（帧协议/Conn）、M5 pipeline、M6 VAD。
- 被消费：M8 会话生命周期在 `internal/session` 上扩展（重连/去重/超时回收）。
