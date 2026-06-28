<div align="center">

# VoiceFluxEngine

**实时、模型无关的语音 Agent _流式内核_（Go）。**

它夹在麦克风/扬声器与 AI 模型之间，只负责三件事 —— **时序 · 背压 · 打断**。
ASR / LLM / TTS 是可插拔的租户，**不是**内核的一部分。

[![Go 1.26](https://img.shields.io/badge/Go-1.26-00ADD8?logo=go&logoColor=white)](go.mod)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
![Transport: WebSocket](https://img.shields.io/badge/transport-WebSocket-blue)
![Dependencies: near-zero](https://img.shields.io/badge/deps-near--zero-success)

[English](README.md) · **简体中文**

</div>

![架构图](docs/assets/architecture.svg)

---

## 目录

- [为什么存在](#为什么存在)
- [它证明了什么](#它证明了什么)
- [特性](#特性)
- [快速开始](#快速开始)
- [基准与曲线](#基准与曲线)
- [设计要点](#设计要点)
- [文档](#文档)
- [范围（v1）](#范围v1)
- [许可证](#许可证)

## 为什么存在

多数「语音 Agent」方案把 ASR → LLM → TTS 拼到一起，指望时序自己对得上。难的从来不是模型，而是夹在麦克风/扬声器与模型之间的那个**内核**：什么时候打断用户、模型卡住时如何卸载压力、用户一开口能多快取消正在生成的应答。

VoiceFluxEngine **只**做这个内核。模型是你按配置插进来的租户 —— 把 mock 换成 sherpa-onnx ASR、把 DeepSeek 换成本地 Ollama，都不用动内核。

> Go 模块名是 `voicestream`，仓库名是 `VoiceFluxEngine`。

## 它证明了什么

| 指标 | 实测 | 出处 |
|---|---|---|
| 打断（内核取消）p99 | **&lt;2ms** 平台区 · **≤143ms** @ 3×拐点负载（预算 200ms） | M10 容量曲线 |
| 内核开销 p99（首响 − 模型固有时延） | **5ms**，平台区恒定 | M9 / M10 分解口径 |
| 容量拐点 | **~500** 并发会话（i7-13620H · 功率受限 · loadgen 同机 → **下界**） | M10 |
| 过墙行为 | 入口 drop-oldest 卸载 21→38%、**出口零丢帧**、无崩溃无 OOM；5000 会话过载后回落 0 会话 / 7 goroutines | M10 |
| 音频热路径分配 | 环 SPSC **0 allocs/op** · 下行编码 **0 allocs/op**（测试门禁） | M3 / M11 |

> **诚实优先于吹嘘。** 数字均为单机、功率受限、loadgen 与服务同机所得 —— 请当作**下界**，而非打榜成绩。

## 特性

- 🎙️ **自然打断。** 内联能量 VAD（在入口读 goroutine 内，保持严格 SPSC）驱动四态状态机；`RESPONDING + speech_start` → 取消响应子链 + 清空在途音频。取消路径不经过拥塞队列，所以 200ms 预算在过载下依然成立。
- ⚖️ **双背压按数据语义分流。** 音频两端高频（50fps/路）走无锁 **SPSC 环 + drop-oldest**（实时音频丢旧帧是 feature）；文本中段低频走**有界阻塞 channel**（文本不可丢）。两者在入口汇合：慢模型 → 链式阻塞 → 表现为**被计数**的入口丢帧，内存有界是构造性质。
- 🔌 **模型无关适配器。** 切换 ASR / LLM / TTS 全凭配置、不改内核。**LLM** 接任意 OpenAI 兼容 SSE 端点（云端或本地）；**ASR/TTS** 接开源 sherpa-onnx（本地 sidecar）或内置 mock。
- 📊 **可观测性是背景板。** 手写 Prometheus 文本格式（~150 行、零依赖）+ SSE 延迟**瀑布**仪表盘。首响拆成「模型固有时延 + 内核开销」分开计量 —— 绝不给内核贴模型的金。
- 🧪 **负载 harness + 容量曲线。** ramp 并发虚拟会话驱动**真实**热路径；找到拐点、证明优雅降级。
- 🪶 **音频热路径零分配。** SPSC 环与下行编码 0 allocs/op，由测试门禁守住。
- 🌐 **WebSocket + 二进制帧协议。** TEXT/CONTROL 用 protobuf，音频是裸 PCM；线上假设 16k / 16-bit / 单声道。

## 快速开始

```bash
go build ./...   # Go 1.26+
```

### 1 · 最快上手：全 mock，零外部依赖

```bash
go run ./cmd/server   # 浏览器打开 http://localhost:8080/
```

对着麦克风说话 → 听到 mock 回声应答 → **在应答途中再开口即可打断**。这一步不需要任何模型或密钥，验证的就是内核本身：时序 / 背压 / 打断。

### 2 · 接真实 ASR / TTS：开源 sherpa-onnx（本地 sidecar）

内核不内嵌模型 —— 它只当 WebSocket 客户端，连一个 sherpa-onnx 驱动的 sidecar（这里用 [ruzhila/voiceapi](https://github.com/ruzhila/voiceapi)）。**完整步骤（含模型下载、国内镜像加速）见 [docs/sherpa-adapter-zh.md](docs/sherpa-adapter-zh.md)。**

**① 首次准备（一次性）** —— 建 venv、装依赖、下模型：

```powershell
# Windows（PowerShell）
git clone https://github.com/ruzhila/voiceapi
cd voiceapi
python -m venv .venv
.venv\Scripts\python -m pip install -r requirements.txt
```
```bash
# Linux / macOS（bash）
git clone https://github.com/ruzhila/voiceapi
cd voiceapi
python3 -m venv .venv
.venv/bin/python -m pip install -r requirements.txt
```

> 再把模型下到 `voiceapi/models/`（国内用 `https://ghfast.top/` 前缀加速 GitHub），详见 [docs/sherpa-adapter-zh.md](docs/sherpa-adapter-zh.md)。

**② 每次运行（开两个终端，缺一不可）：**

**终端 1 —— sidecar**：启动后「保持运行、别关这个窗口」，监听 `:8000`。

```powershell
# Windows（PowerShell）
cd voiceapi
.venv\Scripts\python app.py --asr-model zipformer-bilingual --threads 8
```
```bash
# Linux / macOS（bash）
cd voiceapi
.venv/bin/python app.py --asr-model zipformer-bilingual --threads 8
```

**终端 2 —— 内核**：回到项目根目录，监听 `:8080`。把 `VOICESTREAM_CONFIG=configs/sherpa.yaml` 写进项目根 `.env`（启动自动读取），两个平台就都只需 `go run ./cmd/server`；或临时设环境变量，按平台二选一：

```powershell
# Windows（PowerShell）—— 注意是 `;` 连接，不是 bash 的行内前缀
$env:VOICESTREAM_CONFIG = "configs/sherpa.yaml"; go run ./cmd/server
```
```bash
# Linux / macOS（bash）
VOICESTREAM_CONFIG=configs/sherpa.yaml go run ./cmd/server
```

浏览器 `http://localhost:8080/` 即为：**真 ASR 转写 → LLM 应答 → 真 TTS 合成**，仍可自然打断。

> **依赖关系（务必记住）：** `浏览器 ⇄ voicestream(:8080) ⇄ voiceapi(:8000) ⇄ sherpa-onnx 模型`。voiceapi 必须**全程开着** —— sherpa 适配器是「说话时才懒连接」，所以内核能正常启动，但只要 voiceapi 没在跑，一说话就报 `connection refused / 目标拒绝`。遇到这个错，就是去把终端 1 起起来。

### 3 · 接真实大模型：任何 OpenAI 兼容端点

LLM 适配器 `openai-compat` 已现成（`configs/sherpa.yaml` 默认就用它），接入 = 填密钥即可（**密钥只走环境变量**）：

1. 确认 `configs/sherpa.yaml` 里是 `llm: openai-compat`（默认已是）；
2. 在项目根 `.env` 里填密钥（**启动自动读取**）：`VOICESTREAM_LLM_API_KEY=sk-...`（云端密钥；本地模型填任意非空值）；
3. 跑（同终端 2，按平台二选一）。

默认 `cloud_llm` 指向 DeepSeek（`deepseek-chat`），换厂商/本地只改 `cloud_llm.base_url` + `model`：

- **云端**：DeepSeek `https://api.deepseek.com/v1` · 通义 `https://dashscope.aliyuncs.com/compatible-mode/v1` · Kimi `https://api.moonshot.cn/v1`
- **本地开源**：Ollama `http://localhost:11434/v1`、vLLM、LM Studio（同一适配器，无需改代码）

内置了一段**语音助手系统提示词**，让回复保持纯口语、不输出会被 TTS 逐字读出的 Markdown / emoji。想改就覆盖 `cloud_llm.system_prompt`。

至此即 **真 ASR → 真 LLM → 真 TTS** 全链路，且全程可打断。

### 4 · 测试与压测

```bash
go test -race ./...                                             # 全量测试（含竞态检测）
go run ./cmd/loadgen -steps 50,100,200,400,800 -out docs/load   # 复现容量曲线
```

> **`.env` 自动加载：** 服务器启动会读取项目根 `.env`（真实环境变量优先）。模板见 [.env.example](.env.example)。把 `VOICESTREAM_CONFIG`、`VOICESTREAM_LLM_API_KEY` 都写进去，就能彻底避开下面的跨平台环境变量语法差异。
>
> **环境变量语法（两平台不同）：** PowerShell 用 `$env:NAME = "value"`，同一行接命令用 `;` 分隔 —— **不能**用 bash 的 `NAME=value 命令` 行内前缀（会报 `无法将"NAME=value"项识别为 cmdlet`）。bash 用 `NAME=value 命令` 前缀或 `export`。
>
> **venv 里的 Python：** Windows 在 `.venv\Scripts\python`，Linux/macOS 在 `.venv/bin/python`。

## 基准与曲线

### 容量曲线（L2）

![容量曲线](docs/assets/capacity-curve.svg)

平台区一切平直；拐点处 CPU 墙先到；过墙后**双背压按设计在入口卸载** —— 压力沿 TTS → token → finals → ASR 链传回，表现为入口丢帧（计数）而非内存增长或崩溃。交互式版本：`docs/load/capacity.html`；复现：`go run ./cmd/loadgen -h`。

### 延迟瀑布图（L1）

![瀑布图](docs/assets/waterfall.svg)

LLM 与 TTS 时间条天然大面积重叠 —— 这就是流水线；灰色串行条是同样三段跨度首尾相接的长度。被打断的轮红框标注内核取消耗时。实时版本跑在 `http://localhost:8080/dash.html`（SSE 逐轮推送）。

### channels vs 无锁环形缓冲（L3）

![对照基准](docs/assets/bench-chan-vs-ring.svg)

环不是为了打榜：drop-oldest 速度几乎持平，真正的差异是**驱逐语义的原子性**（channel 的 select 模拟在并发下不成立）。另一个诚实发现：去掉伪共享填充的环比 channel 还慢 —— 无锁结构做错了不如不做。

## 设计要点

- **双背压按数据语义分流。** 音频两端高频（50fps/路）走无锁 SPSC 环 + drop-oldest（实时音频丢旧帧是 feature）；文本中段低频走有界 channel 阻塞回传（文本不可丢）。两者在入口汇合：慢模型 → 链式阻塞 → 表现为入口计数丢帧，内存有界为构造性质。
- **打断是控制面。** 内联能量 VAD（入口读 goroutine 内，保持严格 SPSC）驱动四态状态机；`RESPONDING + speech_start` → 子链 ctx 取消 + 在途清空。取消路径不经过拥塞队列，所以 200ms 预算在过载下依然成立。
- **会话与连接解耦。** 单调 epoch 防陈旧帧污染、seq 水位去重（TCP 有序使重排窗口不必要 —— 诚实地不做）、断线重连续传、idle 回收兜底。
- **可观测性是背景板。** 手写 Prometheus 文本格式（~150 行零依赖）、SSE 瀑布仪表盘；首响 = 模型固有时延 + 内核开销，分开计量，绝不给内核贴模型的金。

## 文档

> `docs/` 下的深入设计文档按项目约定均为**中文**；代码标识符、命令与规格术语保留英文。

| 文档 | 内容 |
|---|---|
| `docs/M2-transport-design*.md` | 帧协议与 WS 传输 |
| `docs/M3-ringbuf-design*.md` | Vyukov 槽序列 SPSC 环、drop-oldest 无竞争驱逐 |
| `docs/M5-pipeline-design*.md` | 编排与双背压、延迟分解 |
| `docs/M6-vad-bargein-design*.md` | 内联 VAD 与打断状态机 |
| `docs/M8-session-design.md` | 会话生命周期、epoch、重放去重 |
| `docs/M9-metrics-design.md` | 指标与瀑布仪表盘 |
| `docs/M10-loadgen-design.md` | 负载 harness、容量曲线、撞墙归因 |
| `docs/M11-bench-tuning-design.md` | channels vs 环形缓冲基准、pprof 零分配迭代 |
| [`docs/sherpa-adapter-zh.md`](docs/sherpa-adapter-zh.md) | 接入开源 sherpa-onnx ASR/TTS（sidecar + WS 客户端） |
| [`docs/protocol-spec-zh.md`](docs/protocol-spec-zh.md) | 线路协议规范（WS 帧定义 + `.proto`） |
| [`docs/ops-manual-zh.md`](docs/ops-manual-zh.md) | 部署 / 运行 / 指标 / 压测手册 |

零基础上手：`docs/concepts-zh.md`（术语扫盲）→ `docs/study-guide-zh.md`（代码阅读顺序）→ `docs/project-deep-dive-zh.md`（设计动机与取舍）。完整提案/设计/规格/任务：`openspec/changes/streaming-multimodal-agent-engine/`。

## 范围（v1）

WebSocket + PCM 16k/16-bit/mono。模型可插拔：**LLM** 接任意 OpenAI 兼容 SSE 端点（云端或本地）、**ASR/TTS** 接开源 sherpa-onnx（本地 sidecar），切换全凭配置、不改内核。FFmpeg 编解码、gRPC/WebTransport 传输拆为未来独立 change。

## 许可证

[MIT](LICENSE) © 2026 cuagain886
