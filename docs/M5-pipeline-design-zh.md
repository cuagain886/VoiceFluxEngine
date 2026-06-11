# M5 — 管道编排与双背压（设计）

状态：**已实现**。

对应规格`pipeline-orchestration`和设计决策**D2/D3**(环在音频边界、双背压)、**D5邻接**(转生命周期M6状态机驱动)。任务：5.1–5.7。

> 范围注记：M5是引擎；它尚未接线到WebSocket传输。话语边界(`EndUtterance`)和打断(`BargeIn`)是这里的显式输入——M6的内联VAD变成它们的调用者，M7连接环到套接字。这不是事故：内核在没有网络或麦克风的情况下保持可测试。

## 1. 拓扑

```
PushAudio ─▶ [入口环 + 门铃] ─▶ ASR运行 ──finals通道──▶ 编排
 (永不阻塞，    (每话语          │ 一个转
  drop-oldest)   流调用)         ▼ 一次
                                LLM ──tokens──▶ 转发器 ──ttsIn──▶ TTS
                                                  │ (tee: OnToken,    │
                                                  │  回复构建)        ▼
AwaitDownlink ◀─ [出口环 + 门铃] ◀──────────── 出口泵 ◀──ttsOut──┘
```

- **音频边界是环**(M3，drop-oldest)：`PushAudio`和出口泵永不阻塞——实时生产者丢弃陈旧帧而非停滞。
- **文本跳是边界通道**：`finals`、`tokens`、`ttsIn`在满时阻塞。文本是珍贵；阻塞*就是*背压。
- 每边配对环与**容量1门铃**所以消费者停泊而非轮询：非阻塞发送在每推后，电平触发，所以"环空"和"停泊"之间到达的帧永不丢失。生产者成本：常见情况下一个失败通道发送。

## 2. 背压链(5.3) — 慢TTS如何成入口丢弃

规格要求：慢TTS → token通道满 → LLM阻塞 → 压力到达音频入口为帧丢弃，内存到处有界。这个实现中的链，逐链接：

1. TTS缓慢消费 → `ttsIn`(有界)满 → 转发器的发送阻塞 → `tokens`(有界)满 → LLM适配器的发送阻塞(真实提供商的中HTTP读：套接字本身停止排干)。
2. 转永不完成 → 编排器接受无新最终 → `finals`(有界)满 → **ASR运行阻塞**交付其下一最终。
3. 阻塞的ASR运行停止弹入口环 → 环满 → **drop-oldest驱逐，计数增量**。生产者(`PushAudio`)在任何点从未被阻塞。

这条链中的每个缓冲有固定配置的容量(`pipeline.{token,transcript,audio}_chan_cap`、`ring_buffer.*_capacity`)，所以内存由*构造*有界，不通过监视。`TestBackpressurePropagesToIngressDrop`驱动恰好这条链并断言丢弃出现且goroutine计数保持平。

**关键启用决策**：编排器严格一次处理一个转且在转运行时**不**消费下一最终。替代方案(新最终抢占运行转)看起来更"响应"但默默摧毁背压故事——编排器会立即排干`finals`且停滞永不能传播。抢占是故意的、单独的行为：`BargeIn()`，M6的VAD在`speech_start`触发。

## 3. 阶段模型(5.1)

`StageFunc[In, Out] = func(ctx, in <-chan In, out chan<- Out) error`是组成单元；`adapter.ASR.Stream`和`adapter.TTS.Stream`由形状满足它，所以任何符合函数替换一个阶段无编排器更改。Goroutine所有权完全生活在管道中：

- **ASR运行**(长生命周期)：每话语，打开一条新ASR流，泵环→`in`，在边界关闭`in`，并发收集partial(所以partial发射永不能死锁识别器对泵)，交付最终。
- **转子链**(每响应，4 goroutine)：LLM运行、token转发器、TTS运行、出口泵——全在一个转`context`下。一个`sync.WaitGroup`关闭发布统计并关闭`done`；`done`是编排器仅需的同步。
- "Cancel(ctx)"来自任务列表被实现为转context本身——一个`cancel()`到达每个阶段，M4适配器合约(每发送ctx卫)保证无阶段能被楔住在完整通道。

## 4. 打断：取消、冲洗、重启(5.4 / 5.5)

```go
cancelTurn: h.cancel()        // 全四个阶段goroutine及时展开
            <-h.done          // 无东西能写出口了
            egress.drain()    // 冲洗在途下行音频，计数
```

顺序事项：仅在`done`后排干，否则泵能重新填充环在冲洗后。在传输消费者活跃时排干是安全的因为环弹是CAS声称每槽——相同机制使生产者端驱逐安全(M3)使第二排干安全免费。

ASR循环生活在转context外，所以它在取消中保持聆听——`TestBargeInCancelsFlushesAndRestarts`在响应中间打断，然后运行第二话语并断言完整、无残留回复(新通道和转context意味着无共享可变状态泄漏；测试也在200ms预算下持转整个取消→冲洗序列)。

被取消的转保留代理托管说的什么进对话历史——用户听到它；下一转的context应反映现实。

## 5. 延迟分解(5.6)

边界时间戳每转(`TurnStats`)：话语结束(t0)、ASR最终、LLM开始、LLM第一token、第一token进TTS、第一帧进出口。从这些：

```
FirstResponse  = 第一出口帧 − t0          (用户感知)
ModelLatency   = ASR最终跨 + LLM第一token跨 + TTS第一帧跨
KernelOverhead = FirstResponse − ModelLatency     (这个项目所有)
```

一个细微性故意实现：ASR最终时间戳在ASR运行*在最终被生产时*捕获，不在编排器挑选它时——排队在`finals`中花费的时间是内核开销且绝不能漂白为模型延迟。用mock注入已知延迟，`TestLatencyDecomposition`断言ModelLatency反映注入的~40ms而KernelOverhead保持有界。每项队伍时间直方图和导出登陆M9；分解合约是M5固定的。

## 6. 其他决策

- **对话历史**：每管道保持(最后16条消息)，传递给LLM每转——这就是真实SSE适配器成为对话而非无状态一发的原因。编排goroutine仅；无锁。
- **闭锁信号**：`EndUtterance`/`BargeIn`是容量1非阻塞发送——从VAD/传输/控制路径可调用无从不停滞它们；重复合并。
- **空最终被跳过**(无转)——虚假话语边界(如M6过滤前VAD起伏)花费零。
- **出口在`reject`政策下**：泵丢弃环拒绝的帧(它无阻塞选项——它绝不阻塞TTS)。默认出口政策保持`drop_oldest`；那有`reject`用于实验。
- 当话语边界触发时已弹的帧被丢弃与边界(它由定义是hangover区域音频)。

## 7. 验收 / 测试(5.7)

全在`-race`下，重复运行清洁：

- `TestThreeStageStreaming`：第一下行帧到达而LLM中流(通过第一帧处的token计数断言)——流水线，不批；回复完整性和OnToken tee验证。
- `TestBackpressurePropagesToIngressDrop`：完整§2链；入口`Dropped() > 0`、goroutine计数平。
- `TestBargeInCancelsFlushesAndRestarts`：取消< 200ms、出口冲洗后可验证空、下一转完整且无残留。
- `TestLatencyDecomposition`：模型vs内核分割恰好求和到首响；所有边界时间戳捕获。
- `TestShutdownLeavesNoGoroutines`：转中管道的取消返回到基线goroutine计数。

## 参考

- 规格：`openspec/changes/streaming-multimodal-agent-engine/specs/pipeline-orchestration/spec.md`
- 设计决策：D2(环在音频边界)、D3(双背压)、D4(内联VAD保持入口SPSC——到达M6)、D7(PTS时序)。
- 消费：M3`ringbuf`(边界)、M4`adapter`(阶段)。
- 被消费：M6(VAD驱动EndUtterance/BargeIn)、M7(传输接线)、M9(提升TurnStats进导出指标)。
