package adapter

import (
	"context"
	"fmt"
	"math/rand/v2"
	"strings"
	"time"
)

// Latency 在每次产出之前注入一个可配置的固定延迟外加有界抖动。抖动取自一个
// 带种子的 PRNG，所以同一个种子总能复现同一份时间表——基准保持确定性，又
// 仍能演练不规则时序（这正是 mock 按规格的用途：把内核开销与模型固有延迟
// 分开度量）。
type Latency struct {
	Delay  time.Duration // 固定分量，每次产出前都施加
	Jitter time.Duration // 额外延迟的上限，每次产出均匀抽取
	Seed   uint64        // PRNG 种子；同种子 -> 同抖动表
}

func (l Latency) newRNG() *rand.Rand {
	return rand.New(rand.NewPCG(l.Seed, l.Seed))
}

// wait 休眠到下一个排定的延迟，ctx 取消时提前中止。
func (l Latency) wait(ctx context.Context, rng *rand.Rand) error {
	d := l.Delay
	if l.Jitter > 0 {
		d += time.Duration(rng.Int64N(int64(l.Jitter) + 1))
	}
	if d <= 0 {
		return ctx.Err()
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// send 投递 v，且绝不活过一个已取消的 context。
func send[T any](ctx context.Context, out chan<- T, v T) error {
	select {
	case out <- v:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// MockASR 假装识别一句话：每消费 PartialEvery 帧就把 Script 的一个递增前缀
// 作为 partial 产出，输入关闭时把整个 Script 作为 final 产出。转写锚定到
// 最近一帧音频的 PTS。
type MockASR struct {
	Script       string // 「识别出」的文本
	PartialEvery int    // 每 N 帧产出一个 partial；0 表示禁用 partial
	Latency      Latency
}

func (m *MockASR) Stream(ctx context.Context, in <-chan AudioFrame, out chan<- Transcript) error {
	words := strings.Fields(m.Script)
	rng := m.Latency.newRNG()
	frames, emitted := 0, 0
	var lastTs int64
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case f, ok := <-in:
			if !ok {
				if err := m.Latency.wait(ctx, rng); err != nil {
					return err
				}
				return send(ctx, out, Transcript{Text: m.Script, Final: true, TsUs: lastTs})
			}
			lastTs = f.TsUs
			frames++
			if m.PartialEvery > 0 && frames%m.PartialEvery == 0 && emitted < len(words) {
				emitted++
				if err := m.Latency.wait(ctx, rng); err != nil {
					return err
				}
				partial := strings.Join(words[:emitted], " ")
				if err := send(ctx, out, Transcript{Text: partial, TsUs: lastTs}); err != nil {
					return err
				}
			}
		}
	}
}

// MockLLM 把一个确定性回复作为 token 流产出。Reply 未设时回声 prompt
//（"echo: <prompt>"），于是测试能仅凭输入推出预期输出。分词在有空格时按词
// 切分，否则按定长 3 个 rune 一块（接近中日韩的粒度）。
type MockLLM struct {
	Reply   string // 固定回复；空 -> "echo: <prompt>"
	Latency Latency
}

func (m *MockLLM) Stream(ctx context.Context, turn Turn, out chan<- Token) error {
	reply := m.Reply
	if reply == "" {
		reply = "echo: " + turn.Prompt
	}
	rng := m.Latency.newRNG()
	for _, tok := range tokenize(reply) {
		if err := m.Latency.wait(ctx, rng); err != nil {
			return err
		}
		if err := send(ctx, out, Token{Text: tok}); err != nil {
			return err
		}
	}
	return nil
}

// tokenize 把文本切成词 token（有空格的语言）或 3 个 rune 一块（无空格），
// 两种方式都精确保持拼接后等于原文。
func tokenize(text string) []string {
	if strings.ContainsRune(text, ' ') {
		fields := strings.SplitAfter(text, " ")
		out := fields[:0]
		for _, f := range fields {
			if f != "" {
				out = append(out, f)
			}
		}
		return out
	}
	runes := []rune(text)
	var toks []string
	for i := 0; i < len(runes); i += 3 {
		end := min(i+3, len(runes))
		toks = append(toks, string(runes[i:end]))
	}
	return toks
}

// MockTTS 以固定速率合成静音：每个输入 token 的每个 rune 变成 MsPerRune
// 毫秒的零值 PCM，再切成帧大小的片。PTS 走合成采样时钟推进（累计采样点 /
// 速率），所以下游计时逻辑看到的是真实、无缝的音频时间戳。
type MockTTS struct {
	SampleRate    int           // 如 16000
	FrameDuration time.Duration // 如 20ms
	MsPerRune     int           // 语速；默认每 rune 60ms
	Latency       Latency
}

func (m *MockTTS) Stream(ctx context.Context, in <-chan Token, out chan<- AudioFrame) error {
	if m.SampleRate <= 0 || m.FrameDuration <= 0 {
		return fmt.Errorf("adapter: MockTTS requires SampleRate and FrameDuration")
	}
	msPerRune := m.MsPerRune
	if msPerRune <= 0 {
		msPerRune = 60
	}
	samplesPerFrame := int(int64(m.SampleRate) * int64(m.FrameDuration) / int64(time.Second))
	rng := m.Latency.newRNG()
	var totalSamples int64
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case tok, ok := <-in:
			if !ok {
				return nil
			}
			dur := time.Duration(len([]rune(tok.Text))*msPerRune) * time.Millisecond
			frames := int((dur + m.FrameDuration - 1) / m.FrameDuration)
			for i := 0; i < frames; i++ {
				if err := m.Latency.wait(ctx, rng); err != nil {
					return err
				}
				f := AudioFrame{
					PCM:  make([]byte, samplesPerFrame*2), // 16 位单声道静音
					TsUs: totalSamples * 1_000_000 / int64(m.SampleRate),
				}
				if err := send(ctx, out, f); err != nil {
					return err
				}
				totalSamples += int64(samplesPerFrame)
			}
		}
	}
}
