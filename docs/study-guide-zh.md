# voicestream 学习指南 —— 代码阅读顺序与完整学习计划

> 这份文档回答一个问题：**拿到这个仓库，怎么把它真正读懂、内化成自己的东西**。
> 目标读者是要把项目写进简历、并能在面试里经受任意深度追问的人。
> 配套：`project-deep-dive-zh.md`（讲"为什么"）、各 `M*-design*.md`（讲实现）。
> 本指南讲"按什么顺序、做什么练习"。

---

## 0. 学习目标分四层

| 层 | 标准 | 大致用时 |
|---|---|---|
| L1 能跑 | 起服务、浏览器对话、看仪表盘、跑压测、全量测试通过 | 半天 |
| L2 能讲 | 对着架构图把一帧音频/一次打断/一次重连的完整路径讲出来 | 3–4 天 |
| L3 能改 | 完成本文的动手实验；改坏一个性质并解释哪个测试抓住了它 | 1 周 |
| L4 能答辩 | deep-dive 文档 §6 的追问全部脱稿回答；自测题全过 | 2 周 |

**核心心法：这个项目的难点不在任何一段代码里，而在"全局性质"上**
（内存有界、打断不排队、计数器单调）。读代码时永远带着问题：
*这段代码在维护哪条全局性质？删掉它哪个测试会红？*

---

## 1. 前置知识自查（不够就先补，每项给最小够用范围）

| 知识点 | 最小够用范围 | 自查问题 |
|---|---|---|
| Go 并发 | goroutine、channel（含 nil channel 永不就绪、关闭语义）、select、context 取消树、sync/atomic | 往已关闭 channel 发送会怎样？nil channel 在 select 里什么行为？ |
| 内存模型 | happens-before 直觉、atomic Load/Store/CAS、伪共享（cache line） | 两个 goroutine 各自递增相邻的两个 int64，为什么可能互相拖慢？ |
| 网络 | TCP 有序性、WebSocket 二进制消息、SSE | 单条 TCP 连接上应用层会看到乱序吗？ |
| 音频 | PCM 本质（采样点数组）、16kHz/16bit/mono、20ms 帧 = 320 样本 = 640 字节、PTS | 为什么 640 字节？RMS 能量怎么算？ |
| 工具 | `go test -race / -bench -benchmem`、pprof 基本读法、protobuf 概念 | allocs/op 是什么？-race 抓什么类型的 bug？ |

补课资料就用本仓库：`docs/M3` 讲透伪共享，`docs/M2` 讲透分帧。

---

## 2. 第 0 天：先有体感（约 1 小时，不读任何实现代码）

```bash
go test -race ./...                 # 1. 全绿 —— 这是你之后做实验的安全网
go run ./cmd/server                 # 2. 起服务（默认全 mock）
# 浏览器开两个标签页：
#    http://localhost:8080/        说一句话 → 字幕逐 token 出现 → 应答中再开口 → 打断
#    http://localhost:8080/dash.html  看每一轮的瀑布图蹦出来
curl http://localhost:8080/metrics  # 3. 看指标长什么样
go run ./cmd/loadgen -steps 5,20 -step-dur 8s   # 4. 小压测，看汇总表
go test -run=^$ -bench SPSC -benchmem ./internal/ringbuf/   # 5. 跑个基准
```

记下三个此刻还回答不了的问题，读完代码回来收账：
①字幕的时间戳是谁、在哪里盖的？②打断时正在合成的音频去了哪？
③第 4 步的丢帧率为什么是 0？

---

## 3. 代码阅读顺序（九站，自底向上，每站带验收问题和动手实验)

> 原则：**先读地图，再按依赖方向自底向上**——每一站只依赖已读过的站。
> 每站格式：读什么 → 配套文档 → 读完必须能回答 → 动手实验。
> 代码量标注（不含测试），心里有数不慌。

### 站 0｜地图：`cmd/server/main.go`（154 行含 loadgen）

只看装配顺序：config → adapter.Build → metrics → session.Manager →
transport.Server → 路由挂载。**不深入任何调用**，画一张"谁创建谁"的框图。
以后每读完一站回这张图上涂一块。

### 站 1｜帧协议：`internal/transport/frame.go`（332 行/全包）

- 读：`frame.go` 的头布局注释 → `Decode` 的边界检查 → `AppendBinary`。
  暂时跳过 wsconn/server。
- 文档：`M2-transport-design-zh.md`
- 能回答：24 字节头六个字段各干什么？为什么音频 payload 不用 protobuf
  而文本用？`Decode` 返回的 Payload 为什么注明"别名 buf"？
- 实验：写个 10 行 main 手工拼一个帧再 Decode 回来；把 magic 改坏一个
  字节，确认错误路径。

### 站 2｜无锁环：`internal/ringbuf/`（231 行，测试 482 行——测试比代码多）★最硬

- 读：`ring.go` 槽结构与 `tryPush`/`pop` → **Push 的 DropOldest 分支**
  （全项目最高密度的 15 行）→ `pool.go` → 三个基准文件。
- 文档：`M3-ringbuf-design-zh.md`（先读"为什么经典双游标不行"一节再看代码）
- 能回答：槽序列号的取值如何编码"该槽归谁"？drop-oldest 为什么让生产者
  执行 CAS head？为什么消费者 pop 用 CAS 而单纯 SPSC 本不需要？填充买到
  了什么（背出 33.3 vs 76.1ns）？
- 实验：①删掉 `_pad` 字段跑 `-bench SPSC` 看吞吐掉多少；②把 Push 的
  驱逐分支改成"直接覆盖不 CAS"，跑 `-race` 测试看它怎么红。

### 站 3｜适配器契约：`internal/adapter/`（490 行）

- 读：`adapter.go` 接口与类型 → `mock.go`（重点 `send` 与 `Latency.wait`
  的 ctx 守卫）→ `registry.go` → 扫一眼 `openaicompat/`（真实 SSE 长啥样）。
- 文档：`M4-adapters-design-zh.md`
- 能回答：为什么是"同步函数+调用方的 channel"而不是"返回 channel"？
  谁关 in、谁绝不关 out、为什么？阻塞发送怎么就成了背压？mock 形状陷阱
  是什么、怎么被防的？
- 实验：写一个 5 行的假 LLM（永远卡住不产出），接进 pipeline 测试里，
  观察 ctx 取消把它救出来。

### 站 4｜流水线与双背压：`internal/pipeline/`（545 行）★最重要

- 读：`edge.go`（环+门铃=可等待边缘）→ `pipeline.go` 的 `Run` select
  循环（**盯住 finalsC 的 nil-gate**）→ `runASRLoop` 的 pump 循环 →
  `turn.go` 的 `startTurn`（4 个 goroutine 的接线）与 `cancelTurn`
  （打断计时的起止点）。
- 文档：`M5-pipeline-design-zh.md` + deep-dive §3.7
- 能回答：背压链五段（TTS慢→token满→LLM停→finals满→ASR停→入口丢）每段
  在哪行代码？为什么轮内不消费 finals，删掉 nil-gate 全局坏什么？
  FirstResponse = ModelLatency + KernelOverhead 的每个锚点在哪盖戳？
  ASR final 为什么在产出处而非领取处盖戳？
- 实验：①把 `TranscriptChanCap` 调成 100 跑 5.7 集成测试，看"入口丢帧"
  断言失败——亲手弄断背压链再修好；②在 `cancelTurn` 后故意不 drain
  egress，看哪个测试抓到残留。

### 站 5｜VAD 与打断状态机：`internal/vad/`（346 行）

- 读：`vad.go` 能量检测（双门限+min-speech+hangover 三个滤波器的先后）→
  `machine.go` 迁移表（**逐格读，每格问"为什么是这个动作"**）→
  `controller.go`（事件怎么接到 pipeline 的 BargeIn/EndUtterance）。
- 文档：`M6-vad-bargein-design-zh.md` + deep-dive §3.6
- 能回答：为什么 VAD 内联在读 goroutine（提示：SPSC）？四态各在什么事件
  下迁移？THINKING+SpeechStart 为什么取消？trailing ResponseDone 为什么
  必须 no-op？打断 200ms 预算的三段构成？
- 实验：把 MinSpeech 调成 0 跑 demo，对着麦克风咳嗽一声，观察误触发
  ——再调回来，体会"这 100ms 是防误触的价格，不是内核开销"。

### 站 6｜会话层：`internal/session/`（571 行）

- 读：`session.go` 的 Session/attachment 二分（**连接会换、会话不死**）→
  `attach` 的 epoch 检查 → `readLoop`（去重水位）→ `writeLoop`（单写者）→
  `audioPump`（下行时钟重戳）→ `manager.go`（握手、reaper、计数器折叠）。
- 文档：`M8-session-design.md` + deep-dive §3.8
- 能回答：epoch 为什么绑连接不绑帧？水位去重为什么不需要窗口？下行为什么
  刻意不重传？字幕的时间戳之谜（第 0 天问题①）——OnToken 里那个
  `clockUs.Load()` 为什么恰好≈该 token 的语音时刻？
- 实验：跑 `TestReconnectResume`，在 attach 处打断点/加日志，观察旧
  attachment 被标 stale 的瞬间；把 epoch 检查改成 `>=`，看哪个测试红。

### 站 7｜浏览器端：`web/`（579 行 JS）

- 读：`app.js` 的帧编解码（和站 1 对照，JS 版 BigInt 读 u64）→
  PlayoutBuffer（欠载垫静音+水位自适应）→ 重连状态机（epoch+1、
  lastDownSeq 回传）→ `capture-worklet.js`。
- 文档：`M7-demo-client-design.md`
- 能回答：为什么 `AudioContext({sampleRate:16000})` 省掉了手写重采样？
  WS 上"抖动"真实表现是什么、播放缓冲怎么抹平？收到 BARGE_IN 客户端
  做什么？
- 实验：给 PlayoutBuffer 的深度变量加一行 `console.log`，Chrome DevTools
  把网络调成 Slow 3G，亲眼看欠载→垫静音→水位上调的全过程。

### 站 8｜观测与压测：`internal/metrics/`（413 行）+ `internal/loadgen/`（1295 行）

- 读：metrics 的 `Histogram.Observe`（float64-bits CAS）→ `m.go` 的
  RecordTurn → hub 的非阻塞扇出；loadgen 的 `shaper.go`（保序整形）→
  `worker.go` 的 turn 脚本 → `report.go` 的拐点持续性规则。
- 文档：`M9-metrics-design.md`、`M10-loadgen-design.md`（**§2.4–2.6 的
  四个测量学 bug 必读——这是面试差异化素材**）
- 能回答：为什么观测者绝不反压被观测者、代码在哪保证？会话翻滚下计数器
  怎么保持单调？netem 整形器为什么必须保序？瞬态为什么不算拐点？
- 实验：复跑一条小曲线 `go run ./cmd/loadgen -steps 50,100,200 -step-dur 10s
  -out /tmp/cap`，开着 `dash.html` 看负载下的瀑布图变化。

### 站 9｜收官：重读 `cmd/server/main.go` + 三条主线走读

回到站 0 的框图，应该每块都涂满了。然后做三条**端到端走读**（口述或写下来，
这就是面试的答题模板）：

1. **一帧音频的一生**：浏览器 worklet → WS 二进制 → Decode → 水位去重 →
   VAD → 入口环 → ASR pump → …… → 它可能死在哪三个地方（去重/驱逐/hangover 丢弃）？
2. **一次打断的一生**：用户开口 → min-speech 滤波 → SpeechStart →
   RESPONDING 格子 → BargeIn latch → 子链 ctx → 排空 → BARGE_IN 帧 →
   浏览器停播。每跳的耗时量级？
3. **一次重连的一生**：断线 → 会话存活（idle 宽限）→ 新连接 START(epoch+1)
   → 旧 attachment 标 stale → ack 带水位 → 客户端重发 → 重复帧被水位拒收。

---

## 4. 十四天计划表（每天 2–3 小时；有基础可压缩到一周）

| 天 | 内容 | 产出 |
|---|---|---|
| 1 | §2 体感 + 站 0 地图 + 通读 README 与 proposal | 装配框图、三个待解问题 |
| 2 | 站 1 帧协议 + `design.md` 的 D1–D6 | 手工拼帧实验 |
| 3–4 | 站 2 无锁环（含删填充/破驱逐两个实验） | 能脱稿讲槽序列 |
| 5 | 站 3 适配器 + D7–D8 | 假 LLM 实验 |
| 6–7 | 站 4 流水线（含弄断背压链实验） | 背压链五段对应到行号 |
| 8 | 站 5 VAD/状态机 | 迁移表默写 |
| 9–10 | 站 6 会话层 + D12–D13 | 三条"诚实不做"各自的理由 |
| 11 | 站 7 浏览器端 | 抖动→播放缓冲演示 |
| 12 | 站 8 观测与压测 + M10 文档 | 复跑一条曲线 |
| 13 | 站 9 三条主线走读 + deep-dive §6 Q&A 自测 | 三条主线的口述稿 |
| 14 | 综合：7.6 真麦克风验收 + 自测题(§5) + 查漏 | 全部勾掉 |

---

## 5. 毕业自测题（全过 = L4；答案全部在代码或文档里，标注了去处）

1. 为什么 drop-oldest 让"教科书 SPSC 环"不再正确？槽序列号如何修复？（M3）
2. `Run` 循环里 `finalsC` 何时为 nil？删掉这个 gate 会破坏哪条规格场景？（M5/站4）
3. 打断 <200ms 的三段构成各是多少？哪段是参数选择哪段是内核开销？（M6/M10）
4. 单条 WS 上为什么不做重排窗口？什么时候这个决定会失效（提示：传输演进）？（D12/D13）
5. 下行音频为什么不重传？下行 seq 跨重连为什么要连续？（M8）
6. ASR final 的时间戳为什么在产出处盖？挪到编排器领取处会让哪个指标变好看、为什么这是作弊？（M5/M9）
7. 压测 harness 撒过的四个谎分别是什么、各自的修复如何固化成测试？（M10 §2.4–2.6）
8. 200 并发下服务端 CPU 和分配的真实去向是什么？为什么"自有代码不上榜"反而是个好答案？（M11.2）
9. channel 版 drop-oldest 为什么非原子？写出会交错的执行序列。（M11.1/chan_bench 注释）
10. 5000 会话过载后"回落 0 会话/7 goroutines"依赖哪些机制协同？（M8/M10）

## 6. 常见迷路点速查

- **goroutine 所有权**：pipeline.Run 拥有编排+ASR Runner；startTurn 派生
  4 个轮内 goroutine（LLM/TTS/搬运/closer）；session 拥有 audioPump 和
  每 attachment 的读写对；adapter 永远不自己开 goroutine。迷路就回这条。
- **channel 关闭规则**：只有数据所有者关（pipeline 关 in；adapter 绝不关
  out；wire chan 从不关闭——靠 ctx 退出）。
- **两个"时钟"**：帧的 `ts_us` 是采样时钟（数据面）；TurnStats 的锚点是
  wall clock（观测面）。下行 `clockUs` 是会话级采样时钟，audioPump 重戳。
- **三个 ctx 的层级**：进程 ctx → session runCtx → attachment ctx / 轮
  ctx。打断只取消轮 ctx；断线只取消 attachment ctx；谁也不杀 session。

## 7. 学完之后：两个"毕业项目"（任选其一，做完才算真懂）

1. **接一个本地 ASR**：用 whisper.cpp 的流式包装实现 `adapter.ASR`，
   注册进 registry——全程不许改内核任何一行。做得到，说明契约真的解耦；
   做不到，说明你发现了契约的缺陷（更有价值，提 issue 改契约）。
2. **读侧零分配改造**（M11.2 留下的硬骨头）：设计帧缓冲的回收纪律穿透
   adapter 契约（提示：drop-oldest 驱逐点需要回收钩子，`Ring.Push` 的
   被驱逐值要能交还池）。先写设计文档再动手，和 M3/M11 文档对齐口径。
