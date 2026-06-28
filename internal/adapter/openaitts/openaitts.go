// Package openaitts 在任何「OpenAI 兼容」的 /audio/speech 端点上实现 adapter.TTS
// （OpenAI、本地 kokoro/edge-tts 代理、自建服务…）。它把自己注册为 "openai-tts"；
// 导入它（cmd/server 里的空白导入）即可在配置中可选——换 TTS 厂商只改配置、不改码，
// 正如 openaicompat 之于 LLM。
//
// 协议：POST {base_url}/audio/speech，体为 {model,input,voice,response_format,speed}，
// 把流入的 token 攒成整句逐句合成；响应体是裸 PCM（response_format=pcm），边下边经
// 共用的 adapter.PCMFramer 切帧、推 PTS、限速到近实时后产出。HTTP 请求挂在流的 ctx
// 上——ctx 取消即中止请求，这正是打断所需要的。
//
// 采样率约束（遵循 D11「v1 不做重采样」）：端点必须回 16-bit 单声道、采样率等于
// SampleRate 的裸 PCM。真实 OpenAI 的 pcm 固定 24kHz，需改用可配采样率的兼容服务，
// 或把 audio.sample_rate 设成 24000；否则音高/语速会错。
package openaitts

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"voicestream/internal/adapter"
	"voicestream/internal/config"
)

func init() {
	adapter.RegisterTTS("openai-tts", func(cfg config.Config) (adapter.TTS, error) {
		c := cfg.Adapters.OpenAITTS
		if c.BaseURL == "" || c.Model == "" {
			return nil, fmt.Errorf("openaitts: adapters.openai_tts.base_url and .model must be set")
		}
		sr := c.SampleRate
		if sr <= 0 {
			sr = cfg.Audio.SampleRate
		}
		speed := c.Speed
		if speed <= 0 {
			speed = 1.0
		}
		format := c.Format
		if format == "" {
			format = "pcm"
		}
		voice := c.Voice
		if voice == "" {
			voice = "alloy"
		}
		// key 可空：本地无鉴权端点不需要，云端点缺 key 会在首个请求清晰报 401。
		var key string
		if c.APIKeyEnv != "" {
			key = os.Getenv(c.APIKeyEnv)
		}
		return New(c.BaseURL, c.Model, voice, format, key, speed, sr, cfg.Audio.FrameDuration), nil
	})
}

// TTS 是 OpenAI 兼容 /audio/speech 端点的 HTTP 客户端。
type TTS struct {
	baseURL       string
	model         string
	voice         string
	format        string
	apiKey        string
	speed         float64
	sampleRate    int
	frameDuration time.Duration
	client        *http.Client
}

// New 返回针对给定端点的适配器。baseURL 是 API 根（如 https://api.openai.com/v1）；
// 路径 /audio/speech 按 OpenAI 线路约定追加在后面。
func New(baseURL, model, voice, format, apiKey string, speed float64, sampleRate int, frameDuration time.Duration) *TTS {
	return &TTS{
		baseURL:       strings.TrimRight(baseURL, "/"),
		model:         model,
		voice:         voice,
		format:        format,
		apiKey:        apiKey,
		speed:         speed,
		sampleRate:    sampleRate,
		frameDuration: frameDuration,
		// 不设整体超时：取消来自 ctx（打断）；只限响应头到达的时间。
		client: &http.Client{Transport: &http.Transport{ResponseHeaderTimeout: 30 * time.Second}},
	}
}

type speechRequest struct {
	Model          string  `json:"model"`
	Input          string  `json:"input"`
	Voice          string  `json:"voice"`
	ResponseFormat string  `json:"response_format"`
	Speed          float64 `json:"speed,omitempty"`
	SampleRate     int     `json:"sample_rate,omitempty"` // 兼容服务用；OpenAI 忽略未知字段
}

// Stream 实现 adapter.TTS：把流入的 token 攒成句子逐句 POST 合成，把回传的 PCM
// 交给共用 framer 切帧、推 PTS、限速后产出；in 关闭时合成残余文本并冲刷尾部。
func (t *TTS) Stream(ctx context.Context, in <-chan adapter.Token, out chan<- adapter.AudioFrame) error {
	if t.sampleRate <= 0 || t.frameDuration <= 0 {
		return fmt.Errorf("openaitts: requires positive sample_rate and frame_duration")
	}
	framer := &adapter.PCMFramer{SampleRate: t.sampleRate, FrameDuration: t.frameDuration}
	var buf strings.Builder
	flush := func(text string) error {
		if strings.TrimSpace(text) == "" {
			return nil
		}
		return t.synth(ctx, text, out, framer)
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

// synth POST 一段文本，把流式回来的 PCM 响应体边下边喂进 framer。
func (t *TTS) synth(ctx context.Context, text string, out chan<- adapter.AudioFrame, framer *adapter.PCMFramer) error {
	body, err := json.Marshal(speechRequest{
		Model:          t.model,
		Input:          text,
		Voice:          t.voice,
		ResponseFormat: t.format,
		Speed:          t.speed,
		SampleRate:     t.sampleRate,
	})
	if err != nil {
		return fmt.Errorf("openaitts: marshal request: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, t.baseURL+"/audio/speech", bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("openaitts: build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	if t.apiKey != "" {
		req.Header.Set("Authorization", "Bearer "+t.apiKey)
	}

	resp, err := t.client.Do(req)
	if err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return fmt.Errorf("openaitts: request: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		detail, _ := io.ReadAll(io.LimitReader(resp.Body, 2048))
		return fmt.Errorf("openaitts: %s: %s", resp.Status, strings.TrimSpace(string(detail)))
	}

	// 流式读 PCM 响应体，边下边切帧（低延迟、内存有界）。
	chunk := make([]byte, 16<<10)
	for {
		n, rerr := resp.Body.Read(chunk)
		if n > 0 {
			if err := framer.Emit(ctx, chunk[:n], out); err != nil {
				return err
			}
		}
		if rerr == io.EOF {
			return nil
		}
		if rerr != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return fmt.Errorf("openaitts: read audio: %w", rerr)
		}
	}
}
