# voicestream 部署 / 运行 / 压测手册

> 面向**运行这套内核**的人：怎么构建、怎么配、暴露哪些端点、盯哪些指标、怎么压测、
> 怎么做真机验收。设计原理散见各 `M*-design` 文档与
> [project-deep-dive-zh.md](project-deep-dive-zh.md)；本文只讲操作。
>
> 命令以 **PowerShell（Windows）** 为主；Linux/macOS 把 `$env:X = "y"; cmd` 换成
> `X=y cmd` 即可。

---

## 1. 构建与自检

```powershell
go build ./...           # 编译全部
go vet ./...             # 静态检查
go test -race ./...      # 竞态检测下的全量测试（需要 cgo/gcc）
golangci-lint run        # lint（可选）
```

> 约定：每次变更提交前，`build` / `vet` / `test -race` MUST 全绿——文档改动也不例外。

---

## 2. 运行服务器

最简（全 mock，自带浏览器演示页）：

```powershell
go run ./cmd/server
# 监听 :8080，静态页在 /，WebSocket 在 /ws
```

指定配置文件：

```powershell
$env:VOICESTREAM_CONFIG = "configs/loadtest.yaml"; go run ./cmd/server
```

接真实云端 LLM（OpenAI 风格端点，如 DeepSeek/Qwen/Moonshot）：

```powershell
# API key 只走环境变量，绝不写进配置文件
$env:VOICESTREAM_LLM_API_KEY = "sk-..."
$env:VOICESTREAM_CONFIG = "configs/your-cloud.yaml"  # adapters.llm: openai-compat
go run ./cmd/server
```

> **安全红线**：API key **只**通过环境变量（默认 `VOICESTREAM_LLM_API_KEY`，由
> `adapters.cloud_llm.api_key_env` 指定变量名）注入，**任何配置文件 / 仓库 / 日志里
> 都不得出现明文 key**。

---

## 3. 配置参考

配置加载顺序：**内置默认值 → YAML 文件覆盖 → 环境变量覆盖 → 校验**
（见 `internal/config/config.go`）。

### 3.1 各段含义与默认值

| 段 / 键 | 默认 | 说明 |
|---------|------|------|
| `server.addr` | `:8080` | 监听地址 |
| `server.heartbeat_period` | `10s` | WS 心跳周期 |
| `server.static_dir` | `web` | 静态演示页目录；置空 `""` 关闭静态服务（压测/纯后端用） |
| `audio.sample_rate` | `16000` | 采样率 Hz |
| `audio.frame_duration` | `20ms` | 帧时长（20ms = 320 采样 = 640 字节） |
| `audio.channels` | `1` | 声道（单声道） |
| `audio.bits_per_sample` | `16` | 位深 |
| `ring_buffer.ingress_capacity` | `64` | 入口环容量（帧），**必须 2 的幂** |
| `ring_buffer.egress_capacity` | `64` | 出口环容量（帧），**必须 2 的幂** |
| `ring_buffer.ingress_policy` | `drop_oldest` | 满策略：`drop_oldest`（驱逐最旧并计数）或 `reject`（拒写、形成背压） |
| `ring_buffer.egress_policy` | `drop_oldest` | 同上，出口侧 |
| `pipeline.token_chan_cap` | `32` | LLM→TTS token channel 容量 |
| `pipeline.transcript_chan_cap` | `2` | ASR finals→编排器 channel 容量 |
| `pipeline.audio_chan_cap` | `8` | 每阶段音频跳点缓冲 |
| `vad.energy_threshold` | `0.01` | 进入"说话"门限 |
| `vad.exit_threshold` | `0.005` | 维持"说话"门限（双门限滞回，须 ≤ energy_threshold） |
| `vad.min_speech` | `100ms` | 最短有效语音 |
| `vad.hangover` | `300ms` | 静音挂起（须 ≥ min_speech） |
| `session.idle_timeout` | `60s` | 空闲回收 + 重连宽限期 |
| `adapters.asr/llm/tts` | `mock` | 各阶段适配器：`mock` / `openai-compat` / … |
| `adapters.cloud_llm.base_url` | `https://api.deepseek.com/v1` | 云 LLM 端点 |
| `adapters.cloud_llm.model` | `deepseek-chat` | 模型名 |
| `adapters.cloud_llm.api_key_env` | `VOICESTREAM_LLM_API_KEY` | **持有 key 的环境变量名** |
| `adapters.mock.*` | `0` | mock 注入时延（压测塑形用，见 §6） |
| `peripherals.redis_enabled` | `false` | 可选会话路由/限流（关闭时热路径零依赖） |
| `peripherals.kafka_enabled` | `false` | 可选录音落盘/回放（冷路径） |

### 3.2 环境变量覆盖

| 变量 | 作用 |
|------|------|
| `VOICESTREAM_CONFIG` | YAML 配置文件路径（不设则全用默认） |
| `VOICESTREAM_LLM_API_KEY` | 云 LLM 的 API key（见 §2 安全红线） |
| `VOICESTREAM_ADDR` | 覆盖 `server.addr` |
| `VOICESTREAM_SAMPLE_RATE` | 覆盖 `audio.sample_rate` |
| `VOICESTREAM_REDIS_ADDR` | 设置即**自动开启** Redis 并指定地址 |

### 3.3 启动期校验（会直接拒绝启动的非法配置）

`addr` 非空；`sample_rate`/`channels` > 0；两个环容量都是 **2 的幂**；环策略合法；
三个 pipeline channel 容量 > 0；`hangover ≥ min_speech`；
`0 < exit_threshold ≤ energy_threshold`；`idle_timeout > 0`；mock 时延非负。
适配器装配（含云 key 缺失）也在**启动时**失败，而不是对话中途。

---

## 4. 端点

| 路由 | 用途 |
|------|------|
| `GET /ws` | WebSocket 主通道（协议见 [protocol-spec-zh.md](protocol-spec-zh.md)） |
| `GET /` | 浏览器演示客户端（`server.static_dir` 非空时） |
| `GET /metrics` | Prometheus 文本格式指标（§5） |
| `GET /debug/turns` | 轮事件 SSE 源（实时延迟看板，见 [M9-metrics-design.md](M9-metrics-design.md)） |
| `GET /debug/pprof/` | pprof 索引 |
| `GET /debug/pprof/profile` | CPU profile（`?seconds=N`） |
| `GET /debug/pprof/trace` | execution trace |

> `/debug/*` 是单租户开发内核的便利口。**任何多租户/公网暴露前必须加鉴权门禁**。

抓 CPU 火焰图（压测时）：

```powershell
go tool pprof -http=:0 "http://127.0.0.1:8080/debug/pprof/profile?seconds=20"
```

---

## 5. 指标目录（`/metrics`）

前缀统一 `voicestream_`。直方图用于 `histogram_quantile()` 求分位。

### 5.1 轮 / 延迟（内核质量）

| 指标 | 类型 | 含义 |
|------|------|------|
| `voicestream_turns_completed_total` | counter | 自然跑完的轮数 |
| `voicestream_turns_cancelled_total` | counter | 被打断/被取代而取消的轮数 |
| `voicestream_first_response_seconds` | histogram | 说话结束 → 首个下行音频帧（**首响**） |
| `voicestream_kernel_overhead_seconds` | histogram | 首响减去模型固有耗时 = **内核净开销** |
| `voicestream_stage_asr_final_seconds` | histogram | 说话结束 → final 转写 |
| `voicestream_stage_queue_wait_seconds` | histogram | final 转写 → 轮被编排器取走（排队） |
| `voicestream_stage_llm_first_token_seconds` | histogram | LLM 流打开 → 首 token |
| `voicestream_stage_tts_first_frame_seconds` | histogram | 首 token 进 TTS → 首合成帧 |
| `voicestream_barge_in_cancel_seconds` | histogram | 打断请求 → 子链拆除且出口冲刷完（**预算 p99 ≤ 200ms**） |

### 5.2 会话 / 背压（健康度与降级）

| 指标 | 类型 | 含义 / 关注点 |
|------|------|---------------|
| `voicestream_sessions_active` | gauge | 当前活跃会话数 |
| `voicestream_sessions_created_total` | counter | 累计创建 |
| `voicestream_sessions_reclaimed_total` | counter | 空闲/超时回收 |
| `voicestream_stale_epoch_claims_total` | counter | 过期 epoch 的重连尝试（被拒） |
| `voicestream_ingress_dropped_frames_total` | counter | **入口丢帧**（过墙后的优雅降级信号） |
| `voicestream_egress_dropped_frames_total` | counter | 出口丢帧（应≈0，非 0 要查） |
| `voicestream_dup_frames_total` | counter | 重连重放去重命中 |
| `voicestream_stale_frames_total` | counter | 过期帧丢弃 |
| `voicestream_subtitle_dropped_total` | counter | 字幕降级丢弃（文本不阻塞音频） |
| `voicestream_illegal_transitions_total` | counter | VAD 状态机非法迁移（应恒 0，非 0 = bug） |

### 5.3 运行时（资源 / 容量归因）

| 指标 | 类型 | 含义 |
|------|------|------|
| `voicestream_goroutines` | gauge | goroutine 数（泄漏哨兵） |
| `voicestream_heap_alloc_bytes` | gauge | 堆活跃字节 |
| `voicestream_cpu_busy_seconds_total` | counter | 估计的非空闲 CPU 秒 |
| `voicestream_cpu_capacity_seconds_total` | counter | 进程可用 CPU 秒（GOMAXPROCS×墙钟） |

CPU 利用率 = `rate(cpu_busy_total) / rate(cpu_capacity_total)`。

### 5.4 建议告警（自用阈值，按机器调）

- `histogram_quantile(0.99, rate(voicestream_first_response_seconds_bucket[1m]))` 持续越界 → 首响劣化。
- `rate(voicestream_ingress_dropped_frames_total[1m]) > 0` 持续 → 已过容量墙（降级中）。
- `voicestream_egress_dropped_frames_total` 或 `voicestream_illegal_transitions_total` **任意增长** → 直接查 bug。
- `voicestream_goroutines` 单调上涨不回落 → goroutine 泄漏。

---

## 6. 压测手册（容量曲线 / L2）

方法论见 [M10-loadgen-design.md](M10-loadgen-design.md)：**交付一条带拐点的曲线，而不是
一个数字**。loadgen 驱动**真实热路径**（传输+会话+VAD+流水线），模型用 mock 并**注入
接近真实的时延**，让每轮响应有可被打断、可被排队拉长的"在飞"窗口。

### 6.1 起服务器（压测档）

```powershell
$env:VOICESTREAM_CONFIG = "configs/loadtest.yaml"; go run ./cmd/server
```

`configs/loadtest.yaml` 关掉静态页，并给 mock 注入：`asr_final_delay 60ms`、
`llm_token_delay 25ms (±10ms jitter)`、`tts_frame_delay 18ms`。

### 6.2 跑 ramp

```powershell
go run ./cmd/loadgen -steps 2,5,10,20,40,80,160,320,500 -step-dur 12s -out docs/load
```

常用参数（`go run ./cmd/loadgen -h` 看全量）：

| flag | 默认 | 含义 |
|------|------|------|
| `-url` | `ws://127.0.0.1:8080/ws` | 服务端 WS 端点 |
| `-metrics` | `http://127.0.0.1:8080/metrics` | 服务端指标端点（空 = 关闭服务端抓取） |
| `-steps` | `2,5,10,20,40,80` | 并发阶梯（逗号分隔） |
| `-step-dur` | `10s` | 每档测量窗 |
| `-warmup` | `2s` | 扩容后、测量前的安定时间 |
| `-frame-interval` | `20ms` | 上行节奏（20ms = 实时） |
| `-speech-frames` | `30` | 每句话帧数 |
| `-barge-every` | `4` | 每 N 轮打断一次（0 = 从不） |
| `-netem-delay` | `0` | 每帧基础附加上行延迟 |
| `-netem-jitter` | `0` | 均匀附加抖动上限 |
| `-netem-burst-every` / `-netem-burst-hold` | `0` | 突发窗口周期 / 每窗起始扣留时长（成串释放） |
| `-out` | （空，仅 stdout） | 输出目录，写 `capacity.{csv,json,html}` |

> **netem 等效整形器**：在 WS/TCP 之上，网络损伤只表现为**延迟/突发到达**，
> 整形器**保序、不丢、不乱**（不伪造 TCP 不会发生的乱序/丢包，诚实边界）。

### 6.3 读曲线（`docs/load/capacity.html`）

自包含 HTML，四张图：首响 vs 并发、打断时延 vs 并发、资源与降级 vs 并发、吞吐 vs 并发，
并自动标注**拐点**与**先到的墙**。拐点判定带**持续性规则**：劣化必须延续到下一档
（或本身是最后一档）才算拐点，单档瞬时尖峰不算（见 M10）。

### 6.4 本项目已测基线（参考，非保证）

机器 i7-13620H、安静模式功耗、**loadgen 与服务器同机**（co-located，属于**下界**）：

- 拐点 ~ **400–500 并发**，**CPU 墙先到**（壁先于内存/goroutine）；
- 内核净开销 p99 在拐点前稳定 ~ **5ms** 平台；
- 3× 拐点负载下打断取消 p99 ≤ **143ms**（200ms 预算内）；
- 过墙后**入口丢帧 21–38%、出口丢帧 0、无 OOM**（优雅降级）；
- 5000 会话过载冲击后回落到 **0 会话 / 7 goroutine**（干净恢复，无泄漏）。

---

## 7. 真机验收（L0 North Star / 任务 7.6）

代码与服务端 e2e 已通过；这一步是**人工**确认"浏览器里能自然对话并打断"：

1. `go run ./cmd/server`
2. Chrome 打开 `http://localhost:8080`（用 Chrome：getUserMedia + AEC 回声消除最稳）
3. 允许麦克风，对它说一句话 → 应看到转写、听到 Agent 应答；
4. **在 Agent 还在说时开口打断** → 应立即停止旧应答、转去响应你的新话；
5. （可选）录一段 barge-in 的 GIF 放进 README（占位符在 `docs/assets/barge-in.gif`）。

排障：无声音先查浏览器麦克风权限与系统输入设备；回声/啸叫确认用的是 Chrome（AEC）
且戴耳机；`/debug/turns` 可实时看每轮的延迟分解。

---

## 8. 可选外设（M12，默认关闭）

| 外设 | 开关 | 定位 |
|------|------|------|
| Redis | `peripherals.redis_enabled` / `VOICESTREAM_REDIS_ADDR` | 会话路由/限流；**关闭时热路径零依赖** |
| Kafka | `peripherals.kafka_enabled` | 录音落盘/离线回放的**冷路径**旁路 |

设计原则：可选外设**绝不进入每帧热路径**——开了也只在冷路径/控制面起作用，关了
内核照常自洽运行。

---

## 附：相关文档

- 线路协议契约 → [protocol-spec-zh.md](protocol-spec-zh.md)
- 指标与看板设计 → [M9-metrics-design.md](M9-metrics-design.md)
- 压测方法论 → [M10-loadgen-design.md](M10-loadgen-design.md)
- 基准与零分配调优 → [M11-bench-tuning-design.md](M11-bench-tuning-design.md)
- 设计全景 / 面试向 → [project-deep-dive-zh.md](project-deep-dive-zh.md)
- 概念扫盲（ASR/VAD/TTS/ring buffer/WebSocket…） → [concepts-zh.md](concepts-zh.md)
