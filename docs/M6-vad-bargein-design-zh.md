# M6 — 内联VAD与打断状态机（设计）

状态：**已实现**。

对应规格`vad-barge-in`和设计决策**D4**(内联VAD保持SPSC)、**D5**(显式状态机)、**D6**(客户AEC)。任务：6.1–6.6。

## 1. 目的

M6关闭打断支柱(打断)：检测用户何时说话、决定这对对话意味着什么、当代理在中答时，在200ms预算内将其切断。三个片：

| 片 | 文件 | 工作 |
|---|---|---|
| `Energy`检测器 | `vad.go` | 帧 → `speech_start`/`speech_end`，带假触发过滤 |
| `Machine` | `machine.go` | 对话状态，拒绝非法迁移 |
| `Controller` | `controller.go` | 内联胶水传输读者每帧调用 |

## 2. 内联放置 — 为什么VAD不是消费者(6.1, D4)

VAD和ASR都需要每个入口帧。让VAD成为第二个环消费者会强制入口进MPMC(更慢、更复杂)或扇出阶段(另一个goroutine + 队列)。替代是`Controller.Ingest`在**传输读goroutine本身**中运行检测器，然后转发帧：

```go
func (c *Controller) Ingest(f adapter.AudioFrame) {
    if ev := c.det.Process(f.PCM); ev != None { c.apply(ev) }  // 控制平面
    c.sink.PushAudio(f)                                        // 数据平面
}
```

环保持恰好一个生产者和一个消费者；VAD事件走控制平面——管道的闭锁、非阻塞`EndUtterance`/`BargeIn`信号——永不音频路径。`Ingest`不能阻塞，所以套接字读者的节奏未触及。成本：每帧一个RMS通过320个样本(~sub-µs)。

`Detector`是单方法接口；向`NewController`传递自定义实现交换进WebRTC/ML VAD零其他更改(v1`Energy`检测器在配置给不到时构建)。

## 3. 假触发抑制(6.2 / 6.5)

四层，最外层优先：

1. **客户AEC**(浏览器`getUserMedia({echoCancellation:true})`)，登陆M7)：从麦克信号移除代理自己的播放*在它到达内核前*——对自打断的唯一实际防守。
2. **双阈值(滞后)**：在`energy_threshold`(0.01)进入，在`exit_threshold`(0.005)维持。在两者之间悬停的电平能保持言语活跃但永不启动它，所以单线周围的闪动在结构上不可能。
3. **最小言语时长**：`min_speech`(100ms ≈ 5帧)连续大声帧在`speech_start`前——门和点击不合格。
4. **Hangover**：`hangover`(300ms)连续安静帧在`speech_end`前——自然中句停顿不分割话语。

检测器是小的逐帧状态机超(inSpeech, 运行长度)；全阈值和窗口是配置驱动，持续时间在构造转帧计数。确定由构造——测试馈养合成恒幅PCM并在确切事件位置断言。

## 4. 对话状态机(6.3)

事件：VAD的`speech_start`/`speech_end`加上管道转生命周期(`response_started`/`response_done`，通过M5`OnTurnStart`/`OnTurnEnd`钩)。表格( — = 拒绝，状态持，计数，可观测)：

| 状态 \ 事件 | speech_start | speech_end | response_started | response_done |
|---|---|---|---|---|
| LISTENING | →SPEAKING_USER | — | →RESPONDING | 无操作 |
| SPEAKING_USER | — | →THINKING **+EndUtterance** | 保持 **+CancelTurn** | 无操作 |
| THINKING | →SPEAKING_USER **+CancelTurn** | — | →RESPONDING | →LISTENING |
| RESPONDING | →SPEAKING_USER **+CancelTurn** | — | — | →LISTENING |

非明显单元是故意政策：

- **RESPONDING + speech_start**是*那个*打断(6.4)。
- **THINKING + speech_start**：用户在答开始前恢复——取消无论什么待定所以陈旧回复不谈话超过它们。
- **SPEAKING_USER + response_started**：对*旧*话语的队列转在用户已在说话时触发；它由定义过期，立即取消它而不让代理打断用户。
- **LISTENING / SPEAKING_USER中的response_done是无操作，不错**：在打断后被取消转的done事件总是在机器已移动后拖拉。把它当做非法会让正常打断序列"错误"。

非法事件离状态未触及，增量计数器，并通知观测者钩——规格的"记录该非法事件"无让坏事件败坏对话。

机器是互斥卫的因为它的两个事件源生活在不同goroutine(入口读者、编排)。每音频帧一个锁是~20ns无争用；不值无锁英雄，并诚实注记。

## 5. 打断端到端(6.4)

```
用户的第1大声帧 ─▶ +min_speech ─▶ speech_start ─▶ machine: RESPONDING→SPEAKING_USER
  ─▶ BargeIn()闭锁 ─▶ 编排：取消转ctx ─▶ 全4个阶段goroutine退出
  ─▶ 排干出口环 ─▶ 代理沉默
```

延迟预算分解为：最小言语过滤(配置，100ms默认——*调优选择*，不内核成本) + 机器迁移(~µs) + 适配器取消(M4合约快速) + 冲洗(µs)。`TestBargeInLatencyUnder200ms`在mock链上测全东西——首个打断帧到被取消且冲洗——在200ms下用60ms最小言语窗口。

## 6. 验收 / 测试(6.6)

全`-race`，重复运行清洁：

- 检测器：min持续后确切位置`speech_start`；短爆和噪声底永不触发；hangover桥接子阈值停顿；滞后在间阈带维持但不启动。
- 机器：正常周期；打断路径包括尾`response_done`无操作；用户在THINKING恢复；陈旧转在说话时；五个非法迁移案例(状态持、计数、观测)。
- 控制器 + 真实管道(M7接线形状，减去套接字)：全声音驱动转无显式信号调用；打断延迟< 200ms出口冲洗后可验证空；RESPONDING期间噪声和子最小言语击不取消转。

## 参考

- 规格：`openspec/changes/streaming-multimodal-agent-engine/specs/vad-barge-in/spec.md`
- 设计决策：D4(内联VAD / SPSC)、D5(状态机)、D6(客户AEC拥有回声抑制)。
- 消费：M5管道(`Sink` = PushAudio/EndUtterance/BargeIn，转钩)。
- 被消费：M7(传输读者调用`Ingest`；浏览器启用AEC)。
