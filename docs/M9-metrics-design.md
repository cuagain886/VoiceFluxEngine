# M9 — 可观测性与延迟仪表盘（设计）

状态：**已实现**。

对应任务 9.1–9.2（L1 层交付物），消费 M5 的 `TurnStats` 延迟分解与 M3/M8
的全部计数器。范围试金石提醒：指标是**背景板不是内核**——本里程碑的全部
实现都不允许触碰热路径的分配与阻塞行为。

## 1. 组成

| 部分 | 位置 | 职责 |
|---|---|---|
| 指标注册表 | `internal/metrics/metrics.go` | 手写 Prometheus 文本暴露格式：counter / histogram / gauge-func |
| SSE 集线器 | `internal/metrics/hub.go` | 逐轮记录广播给仪表盘订阅者，含 50 条历史回放 |
| 标准仪表组 | `internal/metrics/m.go` | 直方图桶、`RecordTurn`、TurnRecord JSON |
| 仪表盘 | `web/dash.html` + `dash.js` | 瀑布图前端（原生 JS，零依赖） |

HTTP 端点（与演示页同端口）：`/metrics`（Prometheus 抓取）、
`/debug/turns`（SSE）、`/dash.html`（仪表盘）。

## 2. 为什么手写文本格式而不引 client_golang

需要的表面积极小：若干 counter、七个固定桶 histogram、一组抓取期采样的
gauge。官方库带来的依赖树和注册表机制远超这点需求；Prometheus 文本格式本身
（`# HELP`/`# TYPE` + 累积桶 + `_sum`/`_count`）约 150 行就能正确实现——
且"手写线格式"正是本项目的题中之义。直方图的 `sum` 用 float64-bits CAS
累加，桶计数纯原子；`Observe` 全程无锁无分配，可以安全出现在编排 goroutine
上。OTel 导出留作未来增强（接口面没有任何阻碍）。

## 3. 指标清单（9.1）

延迟直方图（秒，桶 5ms–5s；打断取消专用更细的 1ms–500ms 桶）：

| 指标 | 含义 |
|---|---|
| `voicestream_first_response_seconds` | 用户停顿 → 首个下行音频帧 |
| `voicestream_kernel_overhead_seconds` | 首响减去模型固有跨度（**本项目的责任域**） |
| `voicestream_stage_asr_final_seconds` | 停顿 → final 转写 |
| `voicestream_stage_queue_wait_seconds` | final 产出 → 编排器领取（排队时延） |
| `voicestream_stage_llm_first_token_seconds` / `_tts_first_frame_seconds` | 各阶段首产出 |
| `voicestream_barge_in_cancel_seconds` | 取消发起 → 子链全停 + 出口已清 |

计数 / gauge：`turns_completed/cancelled_total`、`sessions_active/created/
reclaimed`、`ingress/egress_dropped_frames_total`（丢帧率原料）、
`dup/stale_frames_total`、`subtitle_dropped_total`、`stale_epoch_claims_total`、
`illegal_transitions_total`。

**会话翻滚下保持单调**的设计：丢帧等计数器的真身在各 Session 的环/原子里，
会话回收时折叠进进程级累计值，抓取时 = 累计值 + 在线会话实时求和——计数器
语义不因会话生灭而回退。

打断时延口径说明：`barge_in_cancel_seconds` 度量**内核侧**取消（cancel →
子链退出 → flush 完成）；从"用户开口"算起还要加 VAD 的 min-speech 滤波窗
（配置项，默认 100ms）——那是防误触的调参选择，不是内核开销，分开计量才
诚实（与 M6 文档口径一致）。

## 4. 数据通路

```
pipeline.publish ─▶ OnTurnStats（编排 goroutine）─▶ M.RecordTurn
                                   ├─ 直方图 Observe（原子，无锁）
                                   └─ Hub.Publish（JSON + 非阻塞扇出）─▶ SSE ─▶ dash.js
```

为支撑瀑布图，`TurnStats` 增补三个字段：`LLMLastTokenAt`/`TTSLastFrameAt`
（阶段**完整**跨度的右端点，此前只有首产出点）与 `BargeInLatency`。
慢仪表盘订阅者直接被丢事件（每订阅者带缓冲、非阻塞发送）——观测系统
绝不反向给被观测系统施压。

## 5. 瀑布图与「重叠 vs 串行」（9.2）

每轮渲染五条泳道（时间轴同尺度）：

```
ASR   ████              0 → ASR final
LLM       ███████████   LLM start → 最后一个 token
TTS        ██████████▌  TTS start → 最后一帧（橙线 = 首帧下行 = 首响时刻）
实际  ░░░░░░░░░░░░       0 → 轮结束（墙钟）
串行  ▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒ ASR跨度 + LLM完整跨度 + TTS完整跨度
```

LLM 与 TTS 两条天然大面积重叠——这就是流水线；灰色「串行」条是同样三段
跨度首尾相接的长度，每轮标注「流水线省时 N%」。时间轴量程取
max(墙钟, 串行)，两条都画得下。被打断的轮次红框标注取消耗时。顶部摘要卡
（平均首响 / 内核开销 / 打断耗时）由前端就最近 30 轮滚动计算，不引图表库。

## 6. 验收 / 测试

全部 `-race` 干净：

- 文本格式：counter/gauge 行、直方图**累积**桶边界逐条断言
  （`le="0.01"`=1、`le="0.1"`=3 …）、负值与 NaN 观测被忽略。
- Hub：历史回放 + 实时投递在同一条 SSE 流上端到端断言。
- `RecordTurn`：相对毫秒换算、会话 id 截短、串行总和的算术
  （固定夹具：串行 398ms > 墙钟 245ms，重叠收益可断言）。
- 会话集成（真实 WS 栈）：一轮对话后抓取 `/metrics`，断言
  `turns_completed_total 1`、`first_response_seconds_count 1`、
  `sessions_active 1` 等逐项出现。
- 冒烟：`/metrics` 与 `/dash.html` HTTP 200，文本格式首行正确。

## 7. 已知取舍（如实记录）

- 手写注册表不支持标签（label）——用独立指标名替代（如 completed/cancelled
  两个 counter）。需要多维标签时再换官方库，导出名可保持兼容。
- gauge-func 抓取期对 Manager 加锁遍历在线会话：抓取频率（秒级）下开销
  可忽略；绝不在帧路径上。
- 仪表盘单进程直连 SSE，无持久化：进程重启历史即清零。L2 压测的容量曲线
  （M10）才需要落盘聚合，届时由 loadgen 侧采集。

## 参考

- 任务：9.1 / 9.2（L1 交付物：延迟仪表盘）。
- 消费：M5 `TurnStats`（延迟分解契约）、M3 环计数、M6 状态机非法迁移计数、
  M8 会话计数器。
- 被消费：M10 压测读 `/metrics` 画容量曲线；README 四件套之「延迟瀑布图」
  直接截 `/dash.html`。
