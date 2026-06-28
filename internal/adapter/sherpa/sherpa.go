// Package sherpa 通过一个由 sherpa-onnx 驱动的流式 WebSocket sidecar，实现
// adapter.ASR 与 adapter.TTS。内核不内嵌任何模型/推理依赖：适配器只是这个
// sidecar 的 WebSocket 客户端（与 openaicompat 拨 SSE 同构），模型作为可替换
// 的租户独立进程运行。这正是「内核只管时序/背压/打断，模型是租户」的体现。
//
// 目标协议是 ruzhila/voiceapi（FastAPI + sherpa-onnx）所暴露的形态；两端均为
// 16-bit PCM，采样率经 ?samplerate= 协商（与内核线上格式一致，无需重采样）：
//
//	ASR  ws://host/asr?samplerate=16000
//	     C->S: 二进制 PCM 帧（无握手，连上即发）；空二进制帧 = 输入结束
//	     S->C: 文本 {"text":"...","finished":bool,"idx":int}
//	TTS  ws://host/tts?samplerate=16000&sid=0&speed=1.0&interrupt=false&split=false
//	     C->S: 文本（每次一句；interrupt=false 让多句在一条连接上排队不互相打断）
//	     S->C: 二进制 PCM 块（音频）；每句以一条文本 JSON 收尾
//
// 导入本包（cmd/server 里的空白导入）即把 "sherpa" 注册为可选 ASR/TTS。
// 端点判定（一句话何时结束）由 voicestream 自己的 VAD 拥有——它关闭 ASR 的
// 输入 channel；sidecar 自身的 finished 段在本适配器里一律按 partial 处理，
// 以满足「每次 Stream 恰好产出一个 final」的契约。
package sherpa

import (
	"context"
	"fmt"
	"net/url"
	"time"

	"github.com/coder/websocket"

	"voicestream/internal/adapter"
	"voicestream/internal/config"
)

// handshakeTimeout 限定 WebSocket 拨号/握手；连接本身的读写另用流的 ctx。
const handshakeTimeout = 10 * time.Second

func init() {
	adapter.RegisterASR("sherpa", func(cfg config.Config) (adapter.ASR, error) {
		s := cfg.Adapters.Sherpa
		if s.ASRURL == "" {
			return nil, fmt.Errorf("sherpa: adapters.sherpa.asr_url must be set")
		}
		tailMs := s.ASRSilenceTailMs
		if tailMs <= 0 {
			tailMs = 1500
		}
		finalWait := time.Duration(s.ASRFinalWaitMs) * time.Millisecond
		if finalWait <= 0 {
			finalWait = 4 * time.Second
		}
		return &ASR{
			url:           s.ASRURL,
			sampleRate:    cfg.Audio.SampleRate,
			silenceTailMs: tailMs,
			finalWait:     finalWait,
		}, nil
	})
	adapter.RegisterTTS("sherpa", func(cfg config.Config) (adapter.TTS, error) {
		if cfg.Adapters.Sherpa.TTSURL == "" {
			return nil, fmt.Errorf("sherpa: adapters.sherpa.tts_url must be set")
		}
		speed := cfg.Adapters.Sherpa.TTSSpeed
		if speed <= 0 {
			speed = 1.0
		}
		return &TTS{
			url:           cfg.Adapters.Sherpa.TTSURL,
			sampleRate:    cfg.Audio.SampleRate,
			frameDuration: cfg.Audio.FrameDuration,
			speakerID:     cfg.Adapters.Sherpa.TTSSpeakerID,
			speed:         speed,
		}, nil
	})
}

// dial 打开到 sidecar 的 WebSocket，并把 query 合并进 URL。拨号 ctx 只约束
// 握手；readLimit 抬高读上限以容纳较大的 TTS 音频块（coder/websocket 默认仅
// 32KiB）。
func dial(ctx context.Context, rawURL string, query map[string]string, readLimit int64) (*websocket.Conn, error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return nil, fmt.Errorf("sherpa: bad url %q: %w", rawURL, err)
	}
	q := u.Query()
	for k, v := range query {
		q.Set(k, v)
	}
	u.RawQuery = q.Encode()

	dctx, cancel := context.WithTimeout(ctx, handshakeTimeout)
	defer cancel()
	c, _, err := websocket.Dial(dctx, u.String(), nil)
	if err != nil {
		return nil, fmt.Errorf("sherpa: dial %s: %w", u.String(), err)
	}
	c.SetReadLimit(readLimit)
	return c, nil
}

// send 投递 v，且绝不活过一个已取消的 ctx——正是这一点让被打断的子链取消能够
// 及时完成（与 adapter 包内同名 helper 同义，因跨包不可复用故在此重写）。
func send[T any](ctx context.Context, out chan<- T, v T) error {
	select {
	case out <- v:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// wsErr 把「ctx 已取消时的读写错误」如实归一成 ctx.Err()，其余原样返回。
func wsErr(ctx context.Context, err error) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	return err
}
