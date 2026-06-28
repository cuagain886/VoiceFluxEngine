package adapter

import (
	"context"
	"strings"
	"time"
)

// PCMFramer 把 TTS 合成出的 PCM 流切成定长 AudioFrame：PTS 按合成采样时钟推进，
// 发送限速到接近实时。TTS 多半比实时快（RTF<1），整段 burst 会冲垮下游 drop-oldest
// 出口环（溢出丢最旧帧＝句首"吞字"），故只允许领先实时至多 Lead，喂满播放缓冲即可。
//
// 它是 TTS 适配器的共用件：sherpa（WS 回 PCM 块）与 openai-tts（HTTP 回 PCM 体）
// 的切帧/限速逻辑完全一致，只有「字节怎么拿回来」不同。一个 PCMFramer 服务一次
// Stream（跨多句保持 PTS 与限速锚点连续），非并发安全。
type PCMFramer struct {
	SampleRate    int
	FrameDuration time.Duration
	Lead          time.Duration // 发送可领先实时的上限；<=0 用 200ms

	carry []byte
	total int64     // 已合成的累计采样点数，用于推 PTS
	start time.Time // 首帧发送时刻；后续帧据此限速到接近实时
}

func (f *PCMFramer) samplesPerFrame() int {
	return int(int64(f.SampleRate) * int64(f.FrameDuration) / int64(time.Second))
}

func (f *PCMFramer) bytesPerFrame() int { return f.samplesPerFrame() * 2 } // 16-bit 单声道

// Emit 把新到的 PCM 接到内部 carry 上，凑满一帧就产出一帧（PTS 走合成采样时钟），
// 并把发送限速到接近实时——只领先实时至多 Lead，避免 burst 冲垮出口环。
func (f *PCMFramer) Emit(ctx context.Context, pcm []byte, out chan<- AudioFrame) error {
	lead := f.Lead
	if lead <= 0 {
		lead = 200 * time.Millisecond
	}
	f.carry = append(f.carry, pcm...)
	bpf := f.bytesPerFrame()
	for len(f.carry) >= bpf {
		frame := make([]byte, bpf)
		copy(frame, f.carry[:bpf])
		f.carry = f.carry[bpf:]

		if f.start.IsZero() {
			f.start = time.Now()
		}
		playPos := time.Duration(f.total) * time.Second / time.Duration(f.SampleRate)
		target := f.start.Add(playPos)
		// 跨句空档后 total 落后实时；重锚起点，避免一次性 burst 追赶整段空档
		// （那会冲垮出口环、丢句首帧）。重锚后最多再领先 Lead，仍是平滑发送。
		if now := time.Now(); now.After(target) {
			f.start = now.Add(-playPos)
			target = now
		}
		if d := time.Until(target) - lead; d > 0 {
			select {
			case <-time.After(d):
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		if err := send(ctx, out, AudioFrame{
			PCM:  frame,
			TsUs: f.total * 1_000_000 / int64(f.SampleRate),
		}); err != nil {
			return err
		}
		f.total += int64(f.samplesPerFrame())
	}
	return nil
}

// Flush 把不足一帧的尾部零填充补满一帧后产出（保证下游看到的是整帧）。
func (f *PCMFramer) Flush(ctx context.Context, out chan<- AudioFrame) error {
	if len(f.carry) == 0 {
		return nil
	}
	frame := make([]byte, f.bytesPerFrame())
	copy(frame, f.carry)
	f.carry = nil
	return send(ctx, out, AudioFrame{
		PCM:  frame,
		TsUs: f.total * 1_000_000 / int64(f.SampleRate),
	})
}

// EndsSentence 判断聚合缓冲是否以句末标点结尾（中英文 + 换行/分号）。TTS 适配器
// 据此把流入的 token 攒成整句再合成，让音频尽早开始（低延迟）。
func EndsSentence(s string) bool {
	r := []rune(strings.TrimRight(s, " \t"))
	if len(r) == 0 {
		return false
	}
	switch r[len(r)-1] {
	case '.', '!', '?', ';', '\n', '。', '！', '？', '；':
		return true
	}
	return false
}
