# M11 — 压测对照与调优（设计）

状态：**已实现**。

对应任务 11.1–11.3（L3 层交付物）。本里程碑不新增功能，回答三个问题：
无锁环到底买到了什么（11.1）、内核的 CPU 与内存到底花在哪（11.2）、
项目如何在 30 秒内向别人证明自己（11.3）。

## 1. channels vs 无锁环形缓冲（11.1）

基准：`internal/ringbuf/chan_bench_test.go`，与既有环基准逐场景镜像，
双方都按惯用法使用（channel 阻塞收发，环自旋）——唯一变量是跳点原语。
i7-13620H，`-count=5` 取中位数（原始数据 `docs/bench/chan-vs-ring.txt`，
图 `docs/assets/bench-chan-vs-ring.svg`）：

| 场景 | channel | ring | 比 |
|---|---|---|---|
| SPSC 跨核搬运（int） | 49.1 ns/op | 37.6 ns/op | 1.3× |
| SPSC 音频帧（640B + 同一缓冲池） | 95.6 ns/op | 61.4 ns/op | 1.6× |
| 饱和 drop-oldest（无消费者） | 29.3 ns/op | 27.6 ns/op | ~持平 |
| 跨核往返时延（ping-pong RTT） | 397.9 ns/op | 229.4 ns/op | 1.7× |
| allocs/op（全部场景） | 0 | 0 | 持平 |

**诚实结论**：

1. 环赢在 1.3–1.7×，不是数量级——音频两端 50fps/路的频率下这是有意义
   但不戏剧化的收益。真正不可替代的是 **drop-oldest 的原子性**：channel
   的 select 模拟（非阻塞发→失败驱逐一个→重试）在与活跃消费者并发时
   驱逐与重发可交错，"满了挤掉最旧"的语义不成立；环的逐槽序列号使该
   操作免竞争（M3 设计的核心论点，基准把它落成数字）。
2. **做错的无锁结构比 channel 更慢**：去掉伪共享填充的环 85.8 ns/op，
   劣于 channel 的 49.1。cache line 填充不是装饰，是这个数据结构成立的
   前提。
3. 双方稳态都零分配——"零分配"不构成选环的理由，时延与语义才是。

## 2. pprof 热点与零分配迭代（11.2）

`/debug/pprof` 挂到服务端（单租户开发内核，多租户暴露前需加门禁）。
在 200 并发稳态负载下采样（CPU 20s + allocs 累计）：

**CPU top-14 全部是 runtime**：网络系统调用（`runtime.cgocall`，Windows
下 WS 收发的 syscall 路径）29%，调度器锁/osyield/semasleep 合计 ~35%——
**自有内核代码不上榜**。这与 M10 的撞墙归因互证：拐点是 syscall+调度墙，
不是帧处理逻辑墙；帧路径本身已经便宜到测不出来。

**分配 top（1.14GB 累计窗口）**：

| 来源 | 占比 | 归属 |
|---|---|---|
| `io.ReadAll`（WS 库逐消息读缓冲） | 53% | 库 |
| `context.AfterFunc`（WS 库逐次读写超时管道） | 16% | 库 |
| `MockTTS.Stream`（合成 PCM） | 11% | 模型租户 |
| 自有内核（逐 utterance channel 等） | ~1% | 内核 |

**本轮迭代**：下行帧编码原本每帧 `MarshalBinary` 分配一次
（1 alloc / 704B / 帧 × 50fps/会话——500 并发即 ~25k allocs/s）。新增
`Frame.AppendBinary`（`encoding.BinaryAppender`），`wsConn` 复用每连接
scratch 缓冲（`Conn` 契约单写者，复用安全）：161→55 ns/op、0 allocs，
`AllocsPerRun=0` 单测做门禁。优化后自有代码从分配榜上消失。

**刻意不做**：入口读侧零分配需要把缓冲回收纪律穿透 adapter 契约——
帧从 WS 读出后存活于入口环直至 ASR 消费，且 drop-oldest 驱逐点没有
回收钩子；强行上池要改 `ASR.Stream` 的缓冲生命周期语义。留作未来
change，分析记录于此而非做一半。

延迟目标核对：打断 < 200ms（M6 mock 基准 + M10 负载下内核取消 p99
≤143ms @ 3×拐点）；音频热路径稳态零分配 = 环/池（M3 门禁）+ 下行编码
（本轮门禁）；读侧库分配如上述边界。

## 3. README 四件套（11.3）

| 件 | 工件 | 来源 |
|---|---|---|
| 架构图 | `docs/assets/architecture.svg` | 手绘矢量：数据面/控制面/背压/租户边界 |
| 延迟瀑布图 | `docs/assets/waterfall.svg` | `/debug/turns` 真实记录按 dash.html 视觉规格矢量重绘（完成轮 + 被打断轮） |
| 并发曲线图 | `docs/assets/capacity-curve.svg` | M10 实测 9 点，延迟对数轴 + 资源百分比双面板 |
| 打断 GIF | 待 7.6 真实麦克风录制 | README 中留 TODO 与录制指引 |

全部 SVG 白底（GitHub 明暗主题通吃）、零外部依赖、可直接 `<img>` 引用；
瀑布图保留仪表盘深色以保「截图感」，数据为真实轮记录并如此标注。

## 参考

- 任务：11.1–11.3。消费：M3 环、M9 指标、M10 曲线数据。
- 决策：D9（容量方法论）；CLAUDE.md「Honesty over hype」约束本文措辞。
