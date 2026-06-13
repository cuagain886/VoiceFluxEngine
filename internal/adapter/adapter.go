package adapter

import "context"

// AudioFrame 是穿过适配器边界的一帧 PCM。TsUs 是采样时钟 PTS（D7）：作为
// ASR 输入时它在入口处盖戳；作为 TTS 输出时它由「已合成的累计采样点数」
// 推算而来。
type AudioFrame struct {
	PCM  []byte
	TsUs int64
}

// Transcript 是一个增量 ASR 结果。一个流是零或多个 partial（Final=false，
// 每个都替换前一个假设）后跟恰好一个 final。TsUs 把这条转写锚定到源音频的
// PTS 上。
type Transcript struct {
	Text  string
	Final bool
	TsUs  int64
}

// Token 是 LLM 输出文本的一个增量。它同时也是 TTS 的输入单元；流水线可以在
// 合成前把多个 token 聚合成更大的块，而不改变接口。
type Token struct {
	Text string
}

// Message 是对话历史里的一轮。
type Message struct {
	Role string // "system" | "user" | "assistant"
	Text string
}

// Turn 是一次 LLM 生成的输入。
type Turn struct {
	Prompt  string
	History []Message
}

// 三个接口共享的流式契约：
//
//   - Stream 在调用方的 goroutine 里同步运行（流水线给每个阶段各一个），
//     在流完成、输入 channel 关闭、或 ctx 被取消时返回。
//   - 适配器绝不关闭 out；调用方拥有它，并在 Stream 返回后关闭它。每次发送
//     都必须在 ctx 被取消时中止，这样一个被取消的适配器永远不会卡在满
//     channel 上——正是这一点让打断的子链取消能够及时完成。
//   - 卡在满的 out channel 上是有意为之：这就是流水线的文本背压（D3）向
//     上游传播。

// ASR 消费一句话的 PCM 帧，产出增量转写。调用方关闭 in 来标记语句结束；
// 适配器随后产出最终转写并返回。
type ASR interface {
	Stream(ctx context.Context, in <-chan AudioFrame, out chan<- Transcript) error
}

// LLM 为一个对话轮生成 token 流。
type LLM interface {
	Stream(ctx context.Context, turn Turn, out chan<- Token) error
}

// TTS 消费文本增量，一边产出合成好的 PCM 帧，使播放能在完整文本已知之前
// 就开始。
type TTS interface {
	Stream(ctx context.Context, in <-chan Token, out chan<- AudioFrame) error
}
