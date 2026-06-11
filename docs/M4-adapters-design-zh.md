# M4 — 模型适配器：接口、Mock、注册、真实LLM（设计）

状态：**已实现**。

对应规格`model-adapters`和设计决策**D8**(统一流式接口；云优先；早期真实LLM)。任务：4.1–4.5。

> 实时验收注记：真实提供商测试(`TestLiveProvider`)门控于`VOICESTREAM_LLM_API_KEY`且无它跳过。CI根据罐装SSE服务器针对适配器执行合约，所以每次运行都验证；实时通道需要密钥。

## 1. 目的

模型是**租户，不是内核**。M4固定它们插入的墙壁插座：三个流式接口、其后的确定性mock(所以M5/M6能被构建和基准测试而零外部依赖)、名字→工厂注册(所以交换模型是配置编辑)、一个真实云流式LLM早期接线——规格对设计一个在第一次与真实token时序接触时崩溃的接口的对冲。

## 2. 接口形状(4.1)

```go
ASR.Stream(ctx, in <-chan AudioFrame, out chan<- Transcript) error
LLM.Stream(ctx, turn Turn,            out chan<- Token)      error
TTS.Stream(ctx, in <-chan Token,      out chan<- AudioFrame) error
```

故意选择，按重要性递减顺序：

1. **同步调用，调用者提供channel。** `Stream`在调用者的goroutine中运行且在完成时返回。管道(M5)无论如何给每个阶段它自己的goroutine；把goroutine所有权放在*管道*中而不是适配器意味着适配器保持愚蠢管子且编排层拥有所有并发——一个地方推理生命周期，一个地方取消。
2. **阻塞发送是背压。** 完整的`out`通道阻塞适配器，它停止消费`in`(或停止读提供商HTTP流)。那恰好是D3的文本背压——无信用协议，通道*就是*机制。
3. **每次发送是`select { out <- v; <-ctx.Done() }`。** 被取消的适配器永不会在完整通道上死锁；这使打断的子链取消快速不管下游状态。
4. **关闭约定**：调用者关闭`in`终止输入(ASR：话语结束；TTS：回复结束)；适配器永不关闭`out`——调用者在`Stream`返回后做。每通道端一个所有者，无双关闭类错误。
5. **一个ASR `Stream` = 一个话语。** 持续聆听与VAD门控话语边界是管道的工作(M5/M6)，重启ASR流每话语——匹配子链取消/重启模型(5.5)。

类型按目的最小化(`Token`仅文本；`Transcript`携带`Final` + PTS锚；`AudioFrame`携带PCM + 采样时钟PTS每D7)。线路级`seq`/`ts_us`盖章属于管道/传输，不模型边界；后来增加字段非破坏。

## 3. 确定性mock(4.2)

全三个mock共享一个`Latency{Delay, Jitter, Seed}`旋钮：固定延迟加上从**播种PCG PRNG**抽取的均匀抖动——相同种子重现相同发射计划，所以基准能分离内核开销从"模型"延迟(D9的负载 = 真实热路径 + mock模型)同时仍锻炼不规则时序。

| mock | 确定行为 |
|---|---|
| `MockASR` | `Script`的递增词前缀partial每N帧；在输入关闭时最终；PTS锚定到最后消费帧 |
| `MockLLM` | 固定回复的标记化(或`echo: <prompt>`)；空白词，否则CJK的3符文块；串联始终等于回复 |
| `MockTTS` | 符文计数×节奏 → 清零PCM切成帧大小片；PTS从累积合成采样时钟，跨token无缝隙 |

## 4. 注册(4.3)

`database/sql`驱动程序模式：`Register{ASR,LLM,TTS}(name, factory)`进包级映射；mock自注册在`init()`；外部实现自注册从它们自己的包并通过`cmd/server`中的空白导入变成可选的。`adapter.Build(cfg)`解析`adapters.{asr,llm,tts}`名字到`Set{ASR, LLM, TTS}`——未知名字失败列出已注册选项。

`cmd/server`在启动时构建集合所以坏名字或丢失API密钥死于启动，不是中间对话。工厂签名取整个`config.Config`；每实现拿它需要的(mock TTS读音频格式；云LLM读`adapters.cloud_llm`)。

## 5. 真实云LLM：`openai-compat`经SSE(4.4)

一个适配器覆盖许多提供商：DeepSeek、Qwen(兼容模式)、Moonshot、GLM、OpenAI全说OpenAI聊天完成方言——POST`{base_url}/chat/completions`带`stream: true`，SSE`data:`行携带`choices[0].delta.content`，由`data: [DONE]`终止。配置：

```yaml
adapters:
  llm: openai-compat
  cloud_llm:
    base_url: https://api.deepseek.com/v1   # 默认；任何兼容端点
    model: deepseek-chat
    api_key_env: VOICESTREAM_LLM_API_KEY    # env var NAME — 密钥从不在文件中
```

实现决策：

- **手写在`net/http` + `bufio.Scanner`上，无SDK。** 流式协议解析器字面上是这个项目的主题；零新依赖；解析器容忍真实提供商噪声(`:` 注释，空白保活，`event:`字段)。
- **取消 = 中止HTTP请求**(`NewRequestWithContext`)，所以打断实际上停止提供商端生成和套接字，不仅仅是我们的读。由中流取消引起的身体读错误被报告为`ctx.Err()`，保持与mock的合约统一。
- **无客户端级超时**——流是长生命周期；`ctx`拥有生命周期。拨号/头部相被单独边界(`ResponseHeaderTimeout`)。
- 非200响应表面提供商的错误身体(截断)——"坏密钥"和"坏模型名"之间的差异不应需要包捕获。

## 6. 验收 / 测试(4.5)

- **增量语义**：ASR partial是前缀然后恰好一个最终；LLM发射>1个token其串联等于回复(词和CJK分块)；TTS PTS遵循合成采样时钟逐帧。
- **取消即停**：在中流取消与不排干、无缓冲的输出通道——`Stream`必须迅速返回`context.Canceled`(mock和真实适配器两者；真实对串流永远的SSE服务器)。
- **全mock装配**：`Build(config.Default())`产生工作集；完整音频→ASR→LLM→TTS→音频链仅通过接口跑单调输出PTS。
- **真实接口兼容**：罐装SSE`httptest`服务器验证解析、历史序列化、认证头、提供商错误表面——每CI运行。`TestLiveProvider`(密钥门控)验证真实token时序端到端。

## 参考

- 规格：`openspec/changes/streaming-multimodal-agent-engine/specs/model-adapters/spec.md`
- 设计决策：D3(通道块背压)、D7(采样时钟PTS)、D8(云优先适配器、早期真实LLM)、D9(负载 = 热路径 + mock)。
- 本地栈(whisper.cpp / llama.cpp / Piper)后来作为更多注册登陆——注册是接缝使那个无核心更改事件。
