package sherpa

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/coder/websocket"

	"voicestream/internal/adapter"
)

const ttsReadLimit = 16 << 20 // 16 MiB：一句话的 PCM 可能较大

// TTS 是 sherpa-onnx 流式 TTS sidecar 的 WebSocket 客户端。
type TTS struct {
	url           string
	sampleRate    int
	frameDuration time.Duration
	speakerID     int
	speed         float64
}

// Stream 实现 adapter.TTS：把流入的 token 攒成句子送给 sidecar 合成，把回传的
// PCM 交给共用的 adapter.PCMFramer 切帧、推 PTS、限速到近实时后产出。命中句末
// 标点就先合成整句，让音频尽早开始（低延迟）；in 关闭时合成残余文本并冲刷尾部。
//
// 关于「token 聚合」：OSS TTS 多为句/块级合成，而非逐 token 输入——这与
// adapter.Token 的契约不冲突（见 adapter.go 注释：流水线可在合成前聚合）。
func (t *TTS) Stream(ctx context.Context, in <-chan adapter.Token, out chan<- adapter.AudioFrame) error {
	if t.sampleRate <= 0 || t.frameDuration <= 0 {
		return fmt.Errorf("sherpa tts: requires positive sample_rate and frame_duration")
	}
	c, err := dial(ctx, t.url, map[string]string{
		"samplerate": strconv.Itoa(t.sampleRate),
		"sid":        strconv.Itoa(t.speakerID),
		"speed":      strconv.FormatFloat(t.speed, 'f', -1, 64),
		"interrupt":  "false", // 多句共用一条连接顺序合成，新句不得打断旧句
		"split":      "false", // 已自行按句切分，服务端不再二次拆分（保证一句一个 finished）
	}, ttsReadLimit)
	if err != nil {
		return err
	}
	defer func() { _ = c.CloseNow() }()

	framer := &adapter.PCMFramer{SampleRate: t.sampleRate, FrameDuration: t.frameDuration}
	var buf strings.Builder
	flush := func(text string) error {
		if strings.TrimSpace(text) == "" {
			return nil
		}
		return t.synth(ctx, c, text, out, framer)
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case tok, ok := <-in:
			if !ok {
				if err := flush(buf.String()); err != nil {
					return err
				}
				return framer.Flush(ctx, out)
			}
			buf.WriteString(tok.Text)
			if s := buf.String(); adapter.EndsSentence(s) {
				if err := flush(s); err != nil {
					return err
				}
				buf.Reset()
			}
		}
	}
}

// synth 把一段文本送去合成，读回的二进制块交给 framer 切帧产出，遇到一条文本
// JSON（本段收尾）即返回。
func (t *TTS) synth(ctx context.Context, c *websocket.Conn, text string, out chan<- adapter.AudioFrame, framer *adapter.PCMFramer) error {
	if err := c.Write(ctx, websocket.MessageText, []byte(text)); err != nil {
		return fmt.Errorf("sherpa tts: write text: %w", wsErr(ctx, err))
	}
	for {
		typ, data, err := c.Read(ctx)
		if err != nil {
			return fmt.Errorf("sherpa tts: read: %w", wsErr(ctx, err))
		}
		switch typ {
		case websocket.MessageBinary:
			if err := framer.Emit(ctx, data, out); err != nil {
				return err
			}
		case websocket.MessageText:
			return nil // 文本 JSON = 本段合成结束
		}
	}
}
