# Voicestream 项目完整分析文档

## 一、项目概述

### 1.1 项目定义

**Voicestream** 是一个实时多模态语音对话系统的**流式内核**——一个模型无关的中间件层，位于"麦克风/扬声器"与"AI 模型"之间。

本质上，它不是在造语音助手，而是在**基础设施层**解决语音对话系统中最难的三个技术问题：

- **时序（Timing）** — 音频帧与文本 Token 跨阶段对齐，不乱序、不丢失关键时间戳
- **背压（Backpressure）** — 过载时优雅降级而非 OOM/崩溃，内存始终有界
- **打断（Interruption）** — 用户说话时 Agent 能在 ~150ms 内停声并应答，提供自然的交互感

### 1.2 项目类比

```
voicestream 之于语音 Agent ≈ nginx 之于 Web 应用
    → 不写应用逻辑（模型），只负责低延迟、带流控、可中断地搬运数据
```

### 1.3 与现有方案的区别

**市面的语音 Agent 框架**：
- 多采用串行阻塞式调用链：录音 → 等待 ASR 完整句 → 等待 LLM 完整回复 → 等待 TTS 完整合成 → 播放
- 用户感受：3~5 秒延迟，中途无法打断

**voicestream 的做法**：
- ASR/LLM/TTS 并发流式处理，音频帧与文本 Token 互不阻塞
- 打断时即时中止下游链路，快速切回聆听状态
- 用户感受：<200ms 打断响应，对话自然流畅

---

## 二、核心设计理念

### 2.1 三大不可外包的事

项目通过"北极星"定义澄清范围：

| 维度 | 说明 |
|-----|------|
| **时序** | 音频帧编号稳定推进，文本 Token 可按采样点对齐（支持字幕同步） |
| **背压** | 任意并发数下内存有界；越过容量拐点时丢帧 > OOM；能标注"撞哪面墙" |
| **打断** | 用户开口到 Agent 停声 + 开始听新问题，全链路 <200ms |

### 2.2 范围试金石

评估一项工作是否进入 voicestream 范围的单一问题：

> **它是让【时序/背压/打断】更好，还是只是让 Agent 更聪明/把管子接通？**

答案为"前者"→ 核心，做；为"后者"→ 可插拔适配器或推迟。

### 2.3 范围边界

| 类别 | 内容 | 状态 |
|-----|------|------|
| **核心（造）** | 传输层、分帧、环形缓冲、背压、流水线调度、VAD、打断状态机、会话一致性、压测、指标 | ✅ v1 |
| **租客（Stub）** | ASR、LLM、TTS 模型智能 | ✅ mock + 早接真实 |
| **推迟/可选** | FFmpeg 转码、gRPC 栈、Redis/Kafka、产品级 UI | 📌 未来 change |

---

## 三、系统架构

### 3.1 整体架构图

```
┌─────────────────────────────────────────────────────────────┐
│                  浏览器演示客户端（web/）                      │
│  采音(PCM 16k/mono) + AEC + WebSocket 帧收发 + 字幕对齐播放   │
└──────────────────────────┬──────────────────────────────────┘
                           │ WebSocket (二进制帧)
                           ▼
┌─────────────────────────────────────────────────────────────┐
│              Transport 层 (M2)                                │
│  ┌──────────────────────────────────────────────────────┐   │
│  │  Frame: [magic|version|type|seq|ts_us|len|payload]  │   │
│  │  v1 仅 WebSocket；gRPC 栈为未来 change 预留         │   │
│  └──────────────────────────────────────────────────────┘   │
└──────────────────────────┬──────────────────────────────────┘
                           │ Frame 接口（传输无关）
                           ▼
┌─────────────────────────────────────────────────────────────┐
│              Session & VAD 层 (M6/M8)                        │
│  ┌──────────────────┐        ┌─────────────────────────┐   │
│  │  会话生命周期    │        │  VAD + 打断状态机        │   │
│  │  •Unique ID      │        │  •LISTENING             │   │
│  │  •Epoch 防陈旧   │        │  •SPEAKING_USER         │   │
│  │  •重连去重       │        │  •THINKING              │   │
│  └──────────────────┘        │  •RESPONDING_AGENT      │   │
│                              └─────────────────────────┘   │
└──────────────────────────┬──────────────────────────────────┘
                           │
┌──────────────────────────┴──────────────────────────────────┐
│              Pipeline 编排层 (M5)                           │
│                                                              │
│  ┌──────────┐   ┌──────────┐   ┌──────────┐               │
│  │ ASR 阶段 │──→│ LLM 阶段 │──→│ TTS 阶段 │               │
│  └──────────┘   └──────────┘   └──────────┘               │
│       ▲              │              │                       │
│       │              ▼              ▼                       │
│   SPSC 环      channel 背压    SPSC 环                      │
│   (drop-oldest) (阻塞回传)  (drop-oldest)                   │
│       │                           │                         │
│       └───────打断信号────────────┘                         │
│           (Cancel → flush → 丢帧)                           │
│                                                              │
└─────────────┬─────────────────────────┬─────────────────────┘
              │                         │
   ┌──────────▼────┐        ┌───────────▼──────┐
   │ 可插拔模型适配 │        │ 背压与流控机制    │
   │  (M4)         │        │  (M2/M3)         │
   │ • Mock        │        │ • 双背压         │
   │ • 云 API      │        │ • 内存有界       │
   │ • 本地 Stack  │        │ • 丢帧计数       │
   └────────────────┘        └──────────────────┘
```

### 3.2 数据流说明

#### 上行流（用户→Agent 聆听）

```
用户麦克风
    ▼
[PCM 16k/mono 帧]
    ▼
入口读 Goroutine ┐
    │            ├─ VAD（能量检测）─ speech_start/end 事件
    └─ SPSC 环 ─┘
    ▼
ASR 适配器（部分识别 + 最终识别）
    ▼
文本 Channel（低频，关键结果）
    ▼
LLM 阶段（缓冲输入，并发处理）
```

#### 下行流（Agent→用户播放）

```
LLM 生成文本 Token
    ▼
LLM→TTS Channel（传递文本片段）
    ▼
TTS 适配器（流式合成为音频帧）
    ▼
SPSC 环（出口缓冲）
    ▼
出口写 Goroutine
    ▼
WebSocket 发帧
    ▼
浏览器客户端（自适应播放缓冲）
    ▼
用户扬声器
```

### 3.3 并发模型

```
主 Goroutine
    │
    ├─ Transport Server ◄────── WebSocket 连接到来
    │  │
    │  └─ Session Manager（会话路由）
    │     │
    │     └─ per-Session Pipeline
    │        │
    │        ├─ 入口读 Goroutine（SPSC 环→环）+ VAD 内联
    │        ├─ ASR 处理 Goroutine（环→channel）
    │        ├─ LLM 处理 Goroutine（channel→channel）
    │        ├─ TTS 处理 Goroutine（channel→环）
    │        ├─ 出口写 Goroutine（环→SPSC）+ 发帧
    │        └─ 控制 Goroutine（状态机 + 打断信号分发）
    │
    └─ Metrics Hub（全局指标聚合）
```

---

## 四、核心模块设计

### 4.1 M2：传输层与帧协议

**文件位置**：`internal/transport/`

#### 关键设计点

1. **帧格式**（传输无关，刻意为 gRPC 预留）

```
Frame Header (24 bytes):
  magic:     uint32 (0xDEADBEEF) - 版本校验
  version:   uint8                - 协议版本
  type:      uint8                - 帧类型（音频/文本/控制）
  reserved:  uint16               - 预留位
  seq:       uint32               - 单调序列号（去重/乱序重排）
  ts_us:     uint64               - 采样时戳（微秒）
  len:       uint32               - 负载长度

Frame Payload:
  根据 type：
    • AUDIO: PCM 字节流（v1 固定 16k/16-bit/mono）
    • TEXT: protobuf Frame payload (partial/final token)
    • CONTROL: protobuf 控制消息（打断/状态等）
```

2. **WebSocket 具体化**
   - Binary 模式（避免 Base64 开销）
   - permessage-deflate **禁用**（延迟敏感，压缩收益不抵代价）
   - TCP_NODELAY 确认（Go 默认已开）
   - 心跳周期可配（默认 30s）

3. **抽象设计**
   ```go
   // 接口化，让上层完全不知道 WebSocket
   type Conn interface {
       SendFrame(ctx context.Context, f *Frame) error
       RecvFrame(ctx context.Context) (*Frame, error)
       Close(code int, reason string) error
   }
   ```

#### 为什么选 WebSocket

| 比较项 | WebSocket | 自研 TCP | gRPC |
|-------|----------|---------|------|
| 浏览器支持 | ✅ 原生 | ❌ 不行 | ❌（需 Envoy 代理） |
| 浏览器 AEC | ✅（免费 getUserMedia） | ❌ | ❌ |
| 实现复杂度 | 低 | 高（要自做心跳/重连） | 中等 |
| v1 优先级 | **一等** | 三等 | 二等（栈预留） |

---

### 4.2 M3：无锁环形缓冲

**文件位置**：`internal/ringbuf/`

#### 核心目的

- **音频入口/出口**两处高频热路径：每秒 50 帧 × 多并发 = 每秒数百万次读写
- 零堆分配（预分配槽位 + 对象池）
- 无锁（原子操作 + 单生产单消费 SPSC）
- 定界背压（环满→drop-oldest 或 reject）

#### 设计

```go
type Ring struct {
    // 只在初始化时修改，后续读写不涉及
    cap         int    // 2^N 的容量（位运算取模）
    mask        uint64
    slots       []AudioFrame // 预分配

    // 高频竞争，cache-line padding 隔离
    // 读游标（消费者唯一修改）
    readIdx  atomic.Uint64 [CACHE_LINE_PAD]
    
    // 写游标（生产者唯一修改）
    writeIdx atomic.Uint64 [CACHE_LINE_PAD]
    
    // 丢帧计数
    droppedCount atomic.Uint64
}

// 写（生产者） - O(1)，无锁
func (r *Ring) Write(frame AudioFrame) {
    w := r.writeIdx.Load()
    next_w := (w + 1) & r.mask
    
    // 环满时的动作取决于 DropPolicy
    if next_w == r.readIdx.Load() {
        if r.policy == DropOldest {
            // 覆盖最旧，推进读游标
            r.readIdx.Add(1)
            r.droppedCount.Add(1)
        } else {
            return ErrRingFull // 背压反馈
        }
    }
    
    r.slots[w] = frame
    r.writeIdx.Store(next_w)
}

// 读（消费者） - O(1)，无锁
func (r *Ring) Read() (AudioFrame, error) {
    r_idx := r.readIdx.Load()
    if r_idx == r.writeIdx.Load() {
        return AudioFrame{}, ErrRingEmpty
    }
    
    frame := r.slots[r_idx]
    r.readIdx.Store((r_idx + 1) & r.mask)
    return frame, nil
}
```

#### 性能承诺

- **稳态分配数**：0（allocs/op）- 预分配后再不触碰堆
- **存储阶段分配数**：N（N = 缓冲容量，仅在初始化）
- 吞吐对标：vs channels 的吞吐/延迟曲线（L3 交付物）

#### 为什么只在音频两端用

```
数据频率：
  音频：50 fps × 并发数  → 每秒数百万次读写 ✅ 无锁环收益巨大
  文本：几~几十 token/s  → 每秒低频操作      ❌ Channel 足够
  控制：事件驱动           → 极低频           ❌ Channel 是标准

结论：环形缓冲是**针对性优化**，不是通用工具
```

---

### 4.3 M4：可插拔模型适配器

**文件位置**：`internal/adapter/`

#### 统一接口

```go
// ASR: 音频流 → 增量文本识别
type ASRAdapter interface {
    Stream(ctx context.Context, audio <-chan AudioFrame) (
        <-chan *ASRResult, // 包含 Partial/Final 标记
        error,
    )
    Close() error
}

// LLM: 文本 → 流式 Token
type LLMAdapter interface {
    Stream(ctx context.Context, 
        systemPrompt string,
        turn Turn, // 单轮对话：用户输入 + 历史
    ) (<-chan *Token, error)
    Close() error
}

// TTS: 文本 → 音频帧流
type TTSAdapter interface {
    Stream(ctx context.Context, 
        text <-chan string, // 输入文本片段流
    ) (<-chan AudioFrame, error)
    Close() error
}
```

#### 实现家族

| 适配器 | 实现 | 状态 | 用途 |
|--------|------|------|------|
| **Mock** | 确定性延迟 + 抖动注入 | ✅ | 压测、集成测试 |
| **OpenAI Compat** | 云 SSE API（LLM） | ✅ | 真实对话感 |
| **Whisper.cpp** | 本地 ASR | 🔌 预留接口 | 离线场景 |
| **Llama.cpp** | 本地 LLM | 🔌 预留接口 | 离线场景 |

#### 装配工厂

```go
// 按配置组装，启动时校验（API 密钥有效性等）
func Build(cfg *config.Config) (*AdapterSet, error) {
    set := &AdapterSet{}
    
    asr, err := buildASR(cfg.Adapters.ASR)
    if err != nil { return nil, err } // 早失败
    set.ASR = asr
    
    // LLM / TTS 同理
    // ...
    
    return set, nil
}
```

#### v1 策略："云优先"

- **为什么**：最快出对话感（无需本地编译 whisper.cpp 等）
- **真实 LLM**：早接一个云流式 LLM（如 OpenAI Realtime 或兼容 API）验证接口形状
- **模型延迟不影响内核指标**：压测时用 mock 替换，度量的是内核本身（传输+编排开销）

---

### 4.4 M5：流水线编排与双背压

**文件位置**：`internal/pipeline/`

#### 阶段链路与并发

```
┌──────────────────────────────────────────────────────┐
│              Pipeline (M5)                            │
│                                                        │
│  ASR              LLM               TTS               │
│ ┌──────┐  ◄───┐ ┌──────┐  ◄──┐  ┌──────┐            │
│ │入口环│──────┤ │文本CH│──┤  │出口环│            │
│ └──────┘      │ └──────┘  │  └──────┘            │
│    ▲          │    ▲      │      │                 │
│    │          └────│──────┘      │                 │
│    │               │             ▼                 │
│ 音频帧       部分+最终识别   流式合成              │
│ (音声)       (关键结果)      (关键结果)            │
│                                                     │
│ 背压机制：                                         │
│  • ASR 入口环满→drop-oldest（音频可丢）            │
│  • LLM text channel 满→阻塞 ASR（背压回传）        │
│  • TTS 出口环满→drop-oldest（播放可丢）            │
│                                                     │
│ 打断时：                                           │
│  • 控制 Goroutine 发 Cancel 给 LLM→TTS 子链        │
│  • LLM 停止产 token → channel 清空                  │
│  • TTS 停止合成 → 出口环 flush                      │
│  • ASR 继续跑，重新聆听用户                        │
└──────────────────────────────────────────────────────┘
```

#### 双背压详解

**为什么分两种背压**

```
音频（实时、可丢）             文本（珍贵、不可丢）
-----------------             ----------------------
50 fps × N 并发               低频（最终识别等）
→ 过载时丢 N-M 最旧帧          → 过载时阻塞回传（背压）
→ 客户端自动重发或忍受          → 不丢识别结果
  一小段"沙沙声"               → 保证对话一致性

映射到数据结构：
  ASR→LLM: channel   (背压阻塞) ✅
  LLM→TTS: channel   (背压阻塞) ✅
  入口环：drop-oldest (丢帧)   ✅
  出口环：drop-oldest (丢帧)   ✅
```

#### 实现骨架

```go
type Pipeline struct {
    // 阶段
    asr    ASRAdapter
    llm    LLMAdapter
    tts    TTSAdapter
    
    // 缓冲
    audioInRing    *ringbuf.Ring        // 入口
    textASRtoLLM   chan *TextResult      // ASR→LLM
    textLLMtoTTS   chan string           // LLM→TTS
    audioOutRing   *ringbuf.Ring        // 出口
    
    // 控制
    controlCh      chan ControlSignal    // 打断等
    ctx            context.Context
    cancel         context.CancelFunc
}

// 启动并发处理链
func (p *Pipeline) Start(ctx context.Context) error {
    p.ctx, p.cancel = context.WithCancel(ctx)
    
    // 五个 Goroutine：入→ASR→LLM→TTS→出
    go p.runIngressReader()    // SPSC 环写端
    go p.runASRProcessor()     // 环读 → channel 写
    go p.runLLMProcessor()     // channel 读 → channel 写
    go p.runTTSProcessor()     // channel 读 → 环写
    go p.runEgressWriter()     // SPSC 环读端
    
    return nil
}

// 打断：中止 LLM→TTS 子链
func (p *Pipeline) Interrupt(ctx context.Context) error {
    return p.sendControl(ControlSignal{Type: "cancel"})
}
```

#### 时延分解

通过埋点将"首响延迟"分解：

```
总首响 = 传输(5ms) + ASR处理(200ms) + LLM等待(300ms) + TTS等待(100ms) + ...

允许工程师看清：
  • 瓶颈在哪（通常是 LLM 或 ASR）
  • 流水线重叠带来的收益
  • 是内核开销还是模型固有延迟
```

---

### 4.5 M6：VAD 与打断状态机

**文件位置**：`internal/vad/`

#### 能量 VAD

```go
// 简单快速的能量检测
type EnergyVAD struct {
    sampleRate     int       // 16000
    windowSize     int       // 512 (32ms @ 16k)
    energyThreshold float32  // 可配
    silenceDuration time.Duration  // 例如 300ms 判定说完
    hangover        time.Duration  // 例如 100ms 防误触
}

// 每帧处理（内联入口读 Goroutine）
func (vad *EnergyVAD) Process(frame AudioFrame) {
    energy := vad.computeEnergy(frame.Data)
    
    if energy > vad.threshold {
        // 有声
        vad.syllableCount++
        vad.silenceStart = time.Now()
    } else {
        // 无声
        if time.Since(vad.silenceStart) > vad.silenceDuration {
            vad.emit(SpeechEnd)
        }
    }
}
```

#### 打断状态机

```
状态图：

    ┌──────────────┐
    │  LISTENING   │◄───────────────┐
    └──────┬───────┘                │
           │ speech_start           │
           ▼                        │
    ┌──────────────┐                │
    │SPEAKING_USER │                │
    └──────┬───────┘                │
           │ speech_end → ASR收完   │
           ▼                        │
    ┌──────────────┐                │
    │  THINKING    │                │
    └──────┬───────┘                │
           │ LLM启动合成             │
           ▼                        │
   ┌──────────────────┐             │
   │RESPONDING_AGENT  │             │
   └──────┬───────────┘             │
          │                         │
          ├─ speech_start (用户打断)│
          │  → send Cancel → 清环    │
          │  → flush TTS pipeline   │
          │  → 转 LISTENING◄────────┘
          │
          ├─ TTS finish
          │  → 转 LISTENING◄────────┘
          │
          └─ timeout
             → 转 LISTENING◄────────┘

状态迁移规则：
  • 状态转移矩阵在代码中硬编码（拒绝非法迁移）
  • 例如：从 THINKING 不能跳到 LISTENING（必须先到 RESPONDING）
  • 每次迁移都 log 并更新 session 状态版本（防陈旧并发）
```

#### 客户端侧 AEC 的角色

```
为什么 getUserMedia({echoCancellation:true}) 关键：

无 AEC 时：
  Agent 说话 → 播放器放声 → 麦克风拾取播放声
  → VAD 以为"用户在说话" → 错误打断

有浏览器 AEC 时：
  浏览器/OS 先做回声消除（参考信号=本地播放信号）
  → 发出的音频不含回声
  → VAD 真正看到的是用户声音
  → 打断正确触发

这是"选择浏览器而非本地解决"的一等理由
```

---

### 4.6 M8：会话生命周期与一致性

**文件位置**：`internal/session/`

#### 会话管理

```go
type Session struct {
    ID       string           // 唯一标识（UUID）
    Epoch    uint32           // 单调递增，防陈旧帧
    State    SessionState     // CREATED/ACTIVE/IDLE/CLOSED
    
    // 资源
    pipeline *Pipeline
    mgr      *Manager
    
    // 生命周期
    createdAt  time.Time
    lastActive time.Time
    timeout    time.Duration
}

type Manager struct {
    sessions map[string]*Session
    mu       sync.RWMutex
    
    // 回收定时器
    ticker   *time.Ticker
    // 指标
    metrics  *metrics.SessionMetrics
}
```

#### 重连与去重

```
场景：客户端 WebSocket 断线 → 重连 → 重发同一批帧

解决：
  1. 客户端为帧携带 seq（递增）
  2. 服务端记录已处理的最大 seq
  3. 重连后：
     - 验证 epoch 与会话 ID 匹配（否则新建会话）
     - 对 seq < max_acked 的帧应答 ack 但不重处理
     - 对新帧正常处理并更新 max_acked

结果：网络不稳定下的容错、避免重复处理
```

#### 超时与回收

```go
// 定期扫描并回收僵尸会话
func (mgr *Manager) cleanupLoop() {
    ticker := time.NewTicker(10 * time.Second)
    for range ticker.C {
        mgr.mu.Lock()
        now := time.Now()
        for id, sess := range mgr.sessions {
            if sess.State == IDLE && 
               now.Sub(sess.lastActive) > sess.timeout {
                // 释放资源
                sess.pipeline.Close()
                delete(mgr.sessions, id)
            }
        }
        mgr.mu.Unlock()
    }
}
```

---

## 五、关键技术决策（Design Decisions）

### D1: 传输协议——v1 仅 WebSocket，gRPC 拆为未来 change

**取舍**
- ✅ WebSocket：浏览器原生支持，白送 AEC
- 📌 gRPC：栈预留，未来 change `add-grpc-transport`
- ❌ 自研 TCP：复杂度高，不抵收益

**理由**
- 浏览器是最快验证打断效果的环境（AEC 加持）
- 二进制帧 Schema 做成传输无关，栈切换时上层无感

---

### D2: 缓冲策略——环形缓冲只在音频两端，文本走 Channel

**取舍**
- 音频端（高频）：SPSC 无锁环 + drop-oldest
- 文本阶段（低频）：带缓冲 channel + 阻塞背压

**理由**
- 音频 50 fps × 并发 → 每秒数百万次读写，无锁环才赚钱
- 文本极低频，channel 足够且比环形缓冲好理解
- 对照基准（L3 交付物）量化取舍而非断言

---

### D3: 双背压——数据语义分治

**取舍**
- 音频（可丢）：环满 → drop-oldest，永不阻塞生产
- 文本（珍贵）：channel 满 → 阻塞回传

**理由**
- 映射实际语义：音频丢一帧听不出，文本丢一个词会乱
- 不需显式"信用机器"，原生行为就是背压

---

### D4: VAD 内联——入口读 Goroutine 直接做

**取舍**
- VAD 费用极低（能量计算 < 微秒）
- 直接内联，无独立 goroutine 开销

**理由**
- 最早点检测说话 → 打断延迟最小
- 保持入口严格 SPSC（不复制帧进两个缓冲）
- 零复制

---

### D5: 会话状态机——集中式、可测

**取舍**
- 状态转移矩阵在代码中硬编码
- 打断时的行为由当前状态决定

**理由**
- "何时允许打断"集中且易测
- 避免散落的 ad-hoc 逻辑导致边界条件bug

---

### D6: 客户端 AEC——借助浏览器 getUserMedia

**取舍**
- ✅ 浏览器 AEC：免费，质量好（OS/算法支持）
- ❌ 服务端 AEC：需对齐参考信号，复杂

**理由**
- 这是"选浏览器/WebSocket"优先的一等理由
- 直接解决 barge-in 的"播放声被拾取"难题

---

### D7: 时序对齐——采样时钟 PTS + 单调 seq

**取舍**
- 帧 PTS 由采样数推导（samples/rate）
- 帧 seq 递增，支撑去重和乱序重排

**理由**
- 采样时钟避免 wall-clock 抖动累积
- seq 支撑会话重连的去重

---

### D8: 模型策略——云优先，本地作可插拔后续

**取舍**
- v1：SSE 云 API（最快出对话感）
- 后续：whisper.cpp/llama.cpp 等本地栈

**理由**
- 模型不是内核重点，早接真实 API 验证接口
- 内核指标只约束内核开销（mock 替换模型）
- 云网络延迟不影响要证明的东西

---

### D9: 容量度量——交付曲线而非数字

**取舍**
- 不写死"支持 N 并发"
- 交付：负载/延迟曲线 + 拐点 + "撞哪面墙"

**理由**
- 单个数字是虚荣指标
- 带拐点的曲线 + 解释才是工程深度

---

### D10-D11: 范围拆分与可选外设

**取舍**
- FFmpeg + gRPC → 未来独立 change
- Redis/Kafka → 默认关，接口化切换

**理由**
- 一个 change = 一个内聚 apply 单元
- 清晰的"完成定义"

---

### D12: 系统定位——内核层（WS），最后一公里是 WebRTC 的活

**取舍**
- 内核定位：边缘之后的机房腿（干净、可靠）
- WebRTC SFU/网关：处理最后一公里的丢包/抖动

**理由**
- 业界生产拓扑本就是这样（Twilio/OpenAI Realtime）
- 演示客户端临时充当边缘，生产换成真网关

---

## 六、实现进度与任务状态

### 6.1 L0：北极星演示（已完成）

| 任务 | 状态 | 说明 |
|-----|------|------|
| M1 脚手架 | ✅ | Go module、配置 |
| M2 传输 | ✅ | WebSocket + 帧协议 |
| M3 环形缓冲 | ✅ | SPSC + 对象池 + 零分配 |
| M4 适配器 | ✅ | Mock + 早接真实 LLM |
| M5 流水线 | ✅ | ASR→LLM→TTS + 双背压 |
| M6 VAD + 打断 | ✅ | 能量 VAD + 状态机 |
| M7 浏览器客户端 | ✅ | 采音/播放/打断/字幕 |
| M8 会话管理 | ✅ | 生命周期 + 重连去重 |
| **L0 验收** | 🔄 | 浏览器演示完整一轮对话，能自然打断 |

### 6.2 L1：实时延迟仪表盘

| 任务 | 状态 | 说明 |
|-----|------|------|
| M9 指标体系 | ✅ | 首响/各阶段/丢帧/打断时延 |
| 延迟仪表盘 | 🔌 | 瀑布图、流水线重叠可视化 |

### 6.3 L2：负载与容量曲线（未完成）

| 任务 | 状态 | 说明 |
|-----|------|------|
| M10 负载发生器 | ❌ | ramp 并发、注入抖动 |
| 容量拐点检测 | ❌ | 绘制曲线、标注瓶颈 |
| 优雅降级验证 | ❌ | 越过拐点不崩溃、内存有界 |

### 6.4 L3：对照基准与优化（未完成）

| 任务 | 状态 | 说明 |
|-----|------|------|
| channels vs 环性能对比 | ❌ | 吞吐/延迟/分配曲线 |
| pprof 热点定位 | 🔌 | 分配/锁分析 |
| 调优至目标 | ❌ | 音频热路径零分配、延迟 <200ms |

### 6.5 后续 Change（未在本版本）

| Change | 说明 |
|--------|------|
| `add-ffmpeg-audio-codec` | PCM↔Opus/AAC 转码 |
| `add-grpc-transport` | gRPC 双向流栈 |
| `add-local-models` | whisper.cpp/llama.cpp 适配器 |
| `add-redis-session-routing` | 多机会话路由 |

---

## 七、性能指标与北极星

### 7.1 北极星定义（North Star）

30 秒 GIF 演示：
```
[ 用户问题 ]
    ↓
[ Agent 开始答 ]
    ↓
[ 用户话头打断 ]
    ↓
[ Agent 停声，听新问题 ] ← <200ms
    ↓
[ 完成新一轮对话 ]
```

**关键指标**
- 打断响应时延（p99）：<200ms
- 首响延迟：明细分解（传输+编排 vs 模型）
- 会话在真实负载压力下稳定运行

### 7.2 容量目标

不写死数字，交付**曲线**：

```
打断 p99 延迟 (ms)
    │
 250├─────────────────────── [崩溃点]
    │                      ╱
 200├─────────────────── ●  <- [目标：稳定<200ms]
    │                ●
 150├────────────── ●
    │          ●
 100├──────● ●
    │   ●
  50├ ●
    └────┴──────┴──────┴──────┴──── 并发会话数
       10    50    100   200   300
       
       ↑                    ↑
     线性阶段           拐点（例如：fd 用尽）
```

**拐点后的行为**
- ✅ 优雅降级：丢帧率↑，但不 OOM/段错误
- ❌ 恶性崩溃：内存炸、goroutine 溢出

---

### 7.3 热路径承诺

音频入口/出口稳态指标：

| 指标 | 目标 | 验证方法 |
|-----|------|---------|
| 堆分配数 (allocs/op) | 0 | go test -bench |
| 平均处理延迟 | <1ms | 埋点度量 |
| GC 暂停 | <100µs | 日志 + pprof |
| 缓冲丢帧率 | <0.1% 正常负载 | 丢帧计数器 |

---

## 八、系统定位与生产部署

### 8.1 架构定位

```
生产语音系统拓扑：

User Device                     Cloud / Datacenter
┌──────────┐                   ┌─────────────────────┐
│ 浏览器    │                   │  voicestream 内核   │
│ AEC + UAF │───【WebRTC SFU】──│  (传输/编排/背压)   │
│           │                   │                     │
└──────────┘                   ├─────────────────────┤
                               │ 适配器层             │
                               │  • ASR (Whisper)     │
                               │  • LLM (Claude/GPT)  │
                               │  • TTS (Google)      │
                               └─────────────────────┘

voicestream 的位置：边缘之后的机房腿（干净、低延迟）
WebRTC SFU 的职责：最后一公里（处理丢包/抖动/NAT）
```

### 8.2 部署模式

#### 单实例（v1）

```
docker run -e VOICESTREAM_CONFIG=/cfg.yaml voicestream
```

配置文件 `cfg.yaml`：
```yaml
server:
  addr: 0.0.0.0:8080
  static_dir: /web/  # 演示客户端

audio:
  sample_rate: 16000
  
ring_buffer:
  ingress_capacity: 4096   # 2^12
  egress_capacity: 8192    # 2^13

adapters:
  asr:
    type: mock
    latency_ms: 100
    
  llm:
    type: openai_compat
    base_url: https://api.openai.com/v1
    model: gpt-4o
    api_key: ${OPENAI_API_KEY}
    
  tts:
    type: mock
    latency_ms: 50

vad:
  energy_threshold: 500
  silence_duration_ms: 300
  hangover_ms: 100

session:
  timeout: 5m
  max_sessions: 1000
```

#### 多实例（后续 Redis change）

```
voicestream × N ──┐
                  ├─ Redis (会话路由/限流)
voicestream × N ──┘

会话粘性路由：hash(sessionID) mod N
```

---

## 九、测试与验证策略

### 9.1 单元测试覆盖

| 模块 | 测试类型 | 覆盖项 |
|-----|---------|--------|
| ringbuf | 单元 + 基准 | 读写一致、无锁正确性、零分配、吞吐 |
| transport | 单元 + 集成 | 编解码、版本校验、WS 回环 |
| pipeline | 单元 + 集成 | 阶段顺序、背压、取消、流控 |
| vad | 单元 | 能量计算、状态转移 |
| session | 单元 + 集成 | 生命周期、重连、去重、泄漏检测 |

### 9.2 集成测试

全 mock 装配 + 压力测试：
```go
// 测试：背压下内存有界
func TestPipelineBackpressure(t *testing.T) {
    cfg := configWithMock()
    p := NewPipeline(cfg)
    
    // 驱动远快于 TTS 处理的 ASR 产出
    // 验证：LLM channel 满 → ASR 阻塞 → 入口环 drop
    // 最终：RSS 稳定在预期范围内
}
```

### 9.3 e2e 验证（L0）

浏览器演示客户端 + 真实麦克风：
```
用户说："你好"
  ↓
[ASR 识别出"你好"]
  ↓
[LLM 生成回复]
  ↓
[TTS 合成播放]
  ↓
用户打断："等等，…"
  ↓
[Agent 停声] ← 时延检查 <200ms
  ↓
[识别新问题，重新应答]
```

---

## 十、文档与代码约定

### 10.1 代码约定

从 CLAUDE.md：

```
1. **Git per update** — 每个变更集都 commit
   • 提交前必须通过测试：
     go build ./...
     go vet ./...
     go test -race ./...
   • 无例外

2. **Docs per module** — 模块完成后写设计文档
   • 位置：docs/M{N}-{module}-design-zh.md
   • **必须中文**，代码标识和命令保留英文
   • 包含：设计原理、决策理由、权衡

3. **Decisions live in OpenSpec** — 设计决策进 OpenSpec
   • 设计决策 → design.md（D1-D12）
   • 新增/变更需求 → specs/
   • 不应无声发散

4. **Honesty over hype** — 诚实描述
   • 不夸大"支持 N 并发"
   • 不隐瞒 TCP/WS 已提供的能力
```

### 10.2 文档结构

```
docs/
  ├─ learning-plan-zh.md        （学习路线）
  ├─ requirement-zh.md          （需求总览）
  ├─ M2-transport-design-zh.md  （传输层详设）
  ├─ M3-ringbuf-design-zh.md    （环形缓冲详设）
  ├─ M4-adapters-design-zh.md   （适配器详设）
  ├─ M5-pipeline-design-zh.md   （流水线详设）
  ├─ M6-vad-bargein-design-zh.md（VAD 详设）
  ├─ M8-session-design.md       （会话设计）
  ├─ M9-metrics-design.md       （指标设计）
  └─ project-analysis-zh.md     （本文）

openspec/
  changes/
    streaming-multimodal-agent-engine/
      ├─ proposal.md            （提案）
      ├─ design.md              （设计决策 D1-D12）
      ├─ tasks.md               （分解任务）
      └─ specs/                 （各模块规范）
```

---

## 十一、学习与贡献路线

### 11.1 推荐学习顺序

1. **基础认知**（2h）
   - 阅读 CLAUDE.md（项目概述）
   - 阅读 proposal.md（为什么做）

2. **架构理解**（4h）
   - 读 design.md（设计决策）
   - 读 M2/M3/M5 设计文档
   - 跑 demo 客户端看现象

3. **核心代码**（8h）
   - cmd/server/main.go 入口
   - internal/transport 帧协议
   - internal/ringbuf SPSC 实现
   - internal/pipeline 流水线

4. **进阶主题**（可选）
   - 性能优化（pprof 热点）
   - 容量度量（负载曲线）
   - 本地模型接入（新适配器）

### 11.2 贡献指南

新 feature 前清单：

- [ ] OpenSpec 中新增/修改了吗？（design/specs）
- [ ] 单元测试覆盖了吗？
- [ ] `-race` 通过了吗？
- [ ] 没有在热路径引入堆分配吗？
- [ ] 提交前写了 commit message 吗？
- [ ] 对应模块的 design doc 更新了吗？

---

## 十二、总结与评价

### 12.1 项目的创新点

1. **时序对齐的系统设计**
   - 采样时钟 + 单调 seq，支撑精确字幕同步
   - 行业多数方案对此模糊

2. **双背压映射数据语义**
   - 音频可丢（environment）vs 文本珍贵（state）
   - 比通用信用机制更简洁、更好讲

3. **内联 VAD 的打断设计**
   - 在最早点检测说话 → 最短打断延迟
   - 结合浏览器 AEC，完美解决回声难题

4. **模型完全可插拔**
   - ASR/LLM/TTS 接口统一，mock/云/本地任意组合
   - 内核指标独立于模型能力

5. **北极星驱动的范围控制**
   - 三个不可外包的事明确了优先级
   - 拆分 change 保证版本定义清晰

### 12.2 工程深度

- ✅ 无锁编程与高频数据结构优化
- ✅ 流控与背压设计
- ✅ 打断状态机的形式化验证潜力
- ✅ 从指标设计看工程思维

### 12.3 学习价值

**适合学习的人**：
- 构建实时系统的工程师
- 关心延迟/背压/流控的后端开发
- 想理解语音系统实现的 AI 从业者
- Go 并发编程爱好者

**关键收获**：
- 如何用有限资源实现"响应式"系统
- 背压在微秒级系统中的实践
- 打断的正确姿势（状态机 + 子链取消）

---

## 十三、附录：快速参考

### A1. 关键文件导览

```
cmd/server/main.go                    入口点
internal/transport/                   帧协议与 WebSocket
  ├─ frame.proto                      (待查看)
  ├─ frame.go                         帧编解码
  └─ server.go                        WS 监听与连接管理

internal/ringbuf/                     无锁环形缓冲
  ├─ ring.go                          SPSC 实现
  └─ pool.go                          对象池

internal/pipeline/                    流水线编排
  ├─ pipeline.go                      三阶段装配
  ├─ edge.go                          背压与流控
  └─ turn.go                          一轮对话（提示+历史）

internal/adapter/                     可插拔模型
  ├─ adapter.go                       接口定义
  ├─ registry.go                      装配工厂
  ├─ mock.go                          确定性 mock
  └─ openaicompat/openaicompat.go     云 LLM 接入

internal/vad/                         VAD + 打断
  ├─ vad.go                           能量 VAD
  └─ machine.go                       状态机

internal/session/                     会话管理
  ├─ session.go                       会话对象
  └─ manager.go                       生命周期 + 回收

web/                                  浏览器客户端
  ├─ index.html                       页面
  ├─ app.js                           Web Socket 驱动
  └─ capture-worklet.js               音频采集

openspec/changes/.../                 设计文档
  ├─ proposal.md                      为什么做
  ├─ design.md                        12 个设计决策
  └─ specs/                           各模块规范
```

### A2. 常用命令

```bash
# 构建
go build ./cmd/server

# 测试（含竞争检测）
go test -race ./...

# 基准（查看分配）
go test -bench=. -benchmem ./internal/ringbuf/

# 代码审视
go vet ./...
golangci-lint run

# 启动服务
VOICESTREAM_CONFIG=configs/loadtest.yaml go run ./cmd/server

# 启动负载测试
go run ./cmd/loadgen --config=configs/loadtest.yaml
```

### A3. 术语速查

| 术语 | 意思 |
|-----|------|
| SPSC | Single Producer Single Consumer（单生产单消费） |
| VAD | Voice Activity Detection（语音活动检测） |
| AEC | Acoustic Echo Cancellation（回声消除） |
| barge-in | 用户在 Agent 说话时打断（打断打断） |
| PTS | Presentation Time Stamp（呈现时间戳） |
| seq | Sequence number（序列号） |
| epoch | 版本号（防陈旧帧） |
| drop-oldest | 环满时覆盖最旧数据 |
| channel 背压 | Channel 满时阻塞发送者 |
| 打断 p99 | 打断响应时延的 99 分位数 |

---

## 十四、反思与下一步

### 14.1 v1 的成就

- ✅ 完整的流式内核框架
- ✅ 可工作的北极星演示
- ✅ 明确的范围边界
- ✅ 可扩展的适配器模式
- ✅ 扎实的性能基础

### 14.2 已知限制

- 📌 L2/L3 未完成（负载曲线与对照基准）
- 📌 真实模型接入暂时有限
- 📌 多机会话路由推迟到 Redis change
- 📌 FFmpeg 转码推迟到专门 change

### 14.3 后续方向

1. **完成 L2/L3**（量化性能承诺）
2. **接入更多真实模型**（whisper/llama 本地栈）
3. **WebRTC 集成**（生产部署）
4. **监控与告警**（Prometheus/OTel）
5. **多语言客户端**（不仅浏览器）

---

## 附录：参考阅读

- [CLAUDE.md](CLAUDE.md) — 项目概览
- [openspec/changes/.../proposal.md](openspec/changes/streaming-multimodal-agent-engine/proposal.md) — 为什么做
- [openspec/changes/.../design.md](openspec/changes/streaming-multimodal-agent-engine/design.md) — 设计决策详解
- [docs/M2-transport-design-zh.md](docs/M2-transport-design-zh.md) — 传输层详设
- [docs/M3-ringbuf-design-zh.md](docs/M3-ringbuf-design-zh.md) — 环形缓冲详设

---

**文档生成日期**：2026-06-11  
**项目阶段**：L0 完成，L1 部分完成，L2/L3 规划中
