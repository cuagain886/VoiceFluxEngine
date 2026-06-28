# 接入 sherpa-onnx 流式 ASR / TTS

> 本文讲怎么把开源的 [sherpa-onnx](https://github.com/k2-fsa/sherpa-onnx)（k2-fsa，
> ONNX 运行时、无需 PyTorch/联网）作为 ASR 与 TTS 接进 voicestream。选型分析见
> [project-deep-dive-zh.md](project-deep-dive-zh.md) 之外的对话记录；适配器机制见
> [adapter.go](../internal/adapter/adapter.go) 与 [registry.go](../internal/adapter/registry.go)。

## 1. 架构：模型是租户，内核只当客户端

内核不内嵌任何模型/推理依赖。`internal/adapter/sherpa` 只是一个 **WebSocket 客户端**
（与 `openaicompat` 拨 SSE 同构），连向一个**独立进程的 sidecar**——sidecar 内部用
sherpa-onnx 做推理。进程隔离、内核零新增依赖（复用既有的 `github.com/coder/websocket`）。

```
浏览器 ⇄ voicestream 内核 ⇄  [sherpa 适配器 = WS 客户端]  ⇄  sherpa-onnx sidecar
        (时序/背压/打断)                                      (ASR/TTS 推理，可换)
```

## 2. 目标协议（sidecar 必须暴露的接口）

适配器对接 **ruzhila/voiceapi**（FastAPI + sherpa-onnx）所暴露的形态。两端均为
**16-bit PCM**，采样率经 `?samplerate=` 协商——与内核线上格式（16k/16-bit/mono）一致，
**无需重采样**。

| 通道 | URL | 客户端发 | 服务端回 |
|------|-----|----------|----------|
| ASR | `ws://host/asr?samplerate=16000` | 二进制 PCM 帧（**无握手**，连上即发）；**空二进制帧** = 输入结束 | 文本 `{"text":"...","finished":bool,"idx":int}` |
| TTS | `ws://host/tts?samplerate=16000&sid=0&speed=1.0&interrupt=false&split=false` | 文本（每次一句） | 二进制 PCM 块 → 每句一条文本 JSON 收尾 |

> 以上协议**已对照 voiceapi 源码核实、并用真实 sherpa-onnx 服务端联调通过**
> （`internal/adapter/sherpa/sherpa_test.go` 的 `TestLiveTTS`/`TestLiveASR`）。两个
> 易踩的坑：① `/asr` **没有** `{"sid":0}` 握手，连上直接发 PCM，否则服务端首个
> `receive_bytes()` 会抛 `KeyError`；② `/tts` 的 `interrupt` 默认 **true**，多句顺序
> 合成必须显式 `interrupt=false`，否则后一句会打断前一句。
>
> 想换成 sherpa-onnx **官方** 的 `sherpa-onnx-online-websocket-server`，或 FunASR、
> CosyVoice 等其它 sidecar，只需让它们暴露同形态接口，或在本适配器里改一处协议
> 解析——内核与流水线完全无感知。

## 3. 适配器如何映射到流式契约

适配器严守 [adapter.go:40-65](../internal/adapter/adapter.go:40) 的四条铁律
（`select` on `ctx.Done()`、不关 `out`、`in` 关=语句结束、满 `out` 即背压）。三个要点：

- **谁拥有端点判定**：voicestream 自己的 **VAD** 拥有——它在说话结束时关闭 ASR 的
  `in`。因此 sidecar 自身回传的 `finished` 段在适配器里**一律按 partial 处理**，唯一的
  `final` 在 `in` 关闭时以「最近一次文本」落地，满足「每次 `Stream` 恰好一个 final」。
- **token 聚合**：OSS TTS 多为**句/块级**合成而非逐 token 输入。适配器把流入的
  `Token` 攒成句子（命中句末标点即先合成整句，让音频尽早开始），`in` 关闭时合成残余。
  这与 `Token` 契约不冲突（[adapter.go:23](../internal/adapter/adapter.go:23) 已写明可聚合）。
- **PTS（采样时钟）**：TTS 回传的 PCM 按帧大小切分，时间戳走「已合成累计采样点 ÷ 采样率」
  推进——与 `MockTTS` 同源，下游计时看到的是无缝音频时间轴。不足一帧的尾部零填充补齐。

代码：[asr.go](../internal/adapter/sherpa/asr.go)（并发读写：上行 PCM 的同时转发 partial）、
[tts.go](../internal/adapter/sherpa/tts.go)（按句合成 + carry 切帧）。

## 4. 跑起来（四步）

**Step 1 — 起一个 sherpa-onnx 流式 sidecar**（以 voiceapi 为例，以下为实测可行的流程）：
```bash
git clone https://github.com/ruzhila/voiceapi && cd voiceapi

# a) 建虚拟环境并装依赖（Windows 路径为 .venv/Scripts/，Linux/macOS 为 .venv/bin/）
python -m venv .venv
.venv/Scripts/python -m pip install -r requirements.txt
#   注意：requirements 里 sherpa-onnx 的固定版本可能不在你的 pip 镜像上；
#   报「Could not find a version」时改成镜像上存在的同 minor 补丁版即可（实测 1.12.40 可用）。

# b) 下载模型到 voiceapi/models/（zipformer-bilingual 无需额外 VAD 模型）
#   国内直连 github.com 的 release 会被重置，用 https://ghfast.top/ 前缀加速：
mkdir -p models
ASR=sherpa-onnx-streaming-zipformer-bilingual-zh-en-2023-02-20
TTS=vits-zh-hf-theresa
curl -SL -o models/asr.tar.bz2 https://ghfast.top/https://github.com/k2-fsa/sherpa-onnx/releases/download/asr-models/$ASR.tar.bz2
curl -SL -o models/tts.tar.bz2 https://ghfast.top/https://github.com/k2-fsa/sherpa-onnx/releases/download/tts-models/$TTS.tar.bz2
tar xf models/asr.tar.bz2 -C models && tar xf models/tts.tar.bz2 -C models

# c) 启动（须在 voiceapi/ 目录内，app.py 用相对路径 ./models、./assets）
.venv/Scripts/python app.py --asr-model zipformer-bilingual   # 监听 :8000，暴露 /asr 与 /tts
```

> 验证 sidecar（无需麦克风）：起好后设 `SHERPA_TTS_URL=ws://127.0.0.1:8000/tts`、
> `SHERPA_ASR_URL=ws://127.0.0.1:8000/asr`、`SHERPA_ASR_WAV=<上面 ASR 模型里的 test_wavs/0.wav>`，
> 跑 `go test -run TestLive ./internal/adapter/sherpa/` —— 两个联调测试应通过。

**Step 2 — 用 sherpa 配置启动内核**：
```powershell
$env:VOICESTREAM_CONFIG = "configs/sherpa.yaml"; go run ./cmd/server
```
（[configs/sherpa.yaml](../configs/sherpa.yaml) 已选 `asr: sherpa` / `tts: sherpa`，
LLM 仍走 mock；要接云端 LLM 就把 `llm` 改 `openai-compat` 并设 `VOICESTREAM_LLM_API_KEY`。）

**Step 3 — 浏览器验收**：Chrome 打开 `http://localhost:8080`，对它说话 → 看转写、
听合成应答、试打断（与真机验收 7.6 同流程，见 [ops-manual-zh.md](ops-manual-zh.md) §7）。

**Step 4 — 切回全 mock**：不设 `VOICESTREAM_CONFIG` 即可（默认全 mock，无需 sidecar）。

## 5. 配置项

`adapters.sherpa`（见 [config.go](../internal/config/config.go) 的 `SherpaConfig`）：

| 键 | 默认 | 含义 |
|----|------|------|
| `asr_url` | `ws://127.0.0.1:8000/asr` | ASR sidecar 端点；选 `asr: sherpa` 时不可为空 |
| `tts_url` | `ws://127.0.0.1:8000/tts` | TTS sidecar 端点；选 `tts: sherpa` 时不可为空 |
| `tts_speaker_id` | `0` | 多说话人模型的 sid |
| `tts_speed` | `1.0` | 语速；`<=0` 回落为 `1.0` |

空 URL 只在**真的选用** sherpa 时才于装配期（启动时）报错——与 openaicompat 缺 key
的「启动即失败、而非对话中途」一致。

## 6. 已知假设与取舍（诚实边界）

- **句级合成**：低延迟来自「整句一出就播」，而非逐字；逐 token 输入的 OSS TTS 极少。
- **TTS 单连接顺序合成**：适配器在一条 TTS 连接上顺序发送多段文本，靠 `interrupt=false`
  让多句排队不互相打断——已用真实 sidecar 验证（每句各回一个 `finished`，与锁步 `synth`
  匹配）。若你的 sidecar 是「一段文本一连接」，改 `tts.go` 的 `synth` 为每段重拨即可。
- **端点判定归内核 VAD**，不依赖 sidecar 的 `finished`——避免两套端点逻辑打架。
- **已联调验证**：除内存 mock 单测（并发读写、carry 切帧、PTS、partial/final 契约）外，
  `TestLiveTTS`/`TestLiveASR` 已对真实 sherpa-onnx 服务端跑通（流式 ASR 返回非空 final、
  TTS 返回音频帧）。运行方式：起好 sidecar 后设环境变量
  `SHERPA_TTS_URL`/`SHERPA_ASR_URL`/`SHERPA_ASR_WAV`，再
  `go test -run TestLive ./internal/adapter/sherpa/`。完整浏览器对话仍需你本地真机验收。

## 7. 相关文档

- 线路协议（内核 ⇄ 浏览器，非本 sidecar）→ [protocol-spec-zh.md](protocol-spec-zh.md)
- 部署/运行/指标/压测 → [ops-manual-zh.md](ops-manual-zh.md)
- 适配器契约与 registry → [adapter.go](../internal/adapter/adapter.go) · [registry.go](../internal/adapter/registry.go)
