// Package config 定义语音流式内核的全局运行时配置。它支持从 YAML 文件加载、
// 用环境变量覆盖，并校验各项不变量（例如环形缓冲容量必须是 2 的幂）。
package config

import (
	"fmt"
	"os"
	"strconv"
	"time"

	"gopkg.in/yaml.v3"

	"voicestream/internal/ringbuf"
)

// Config 是引擎的根运行时配置。
type Config struct {
	Server      ServerConfig      `yaml:"server"`
	Audio       AudioConfig       `yaml:"audio"`
	RingBuf     RingBufConfig     `yaml:"ring_buffer"`
	Pipeline    PipelineConfig    `yaml:"pipeline"`
	VAD         VADConfig         `yaml:"vad"`
	Session     SessionConfig     `yaml:"session"`
	Adapters    AdaptersConfig    `yaml:"adapters"`
	Peripherals PeripheralsConfig `yaml:"peripherals"`
}

// ServerConfig 保存传输/服务器设置。StaticDir 非空时在 "/" 提供静态文件
//（浏览器演示客户端）；为空则禁用静态服务。
type ServerConfig struct {
	Addr            string        `yaml:"addr"`
	HeartbeatPeriod time.Duration `yaml:"heartbeat_period"`
	StaticDir       string        `yaml:"static_dir"`
}

// AudioConfig 描述内部规范音频格式（v1 假设客户端直接交付该格式的 PCM；
// FFmpeg 转码被推迟）。
type AudioConfig struct {
	SampleRate    int           `yaml:"sample_rate"`    // 如 16000
	FrameDuration time.Duration `yaml:"frame_duration"` // 如 20ms
	Channels      int           `yaml:"channels"`       // 1 = 单声道
	BitsPerSample int           `yaml:"bits_per_sample"`// 16
}

// RingBufConfig 设定无锁音频边缘缓冲的大小。容量以帧为单位、且必须是 2 的幂
//（已校验），这样缓冲可以用掩码取下标。Policy 为每条边缘挑选满缓冲行为：
// "drop_oldest"（驱逐最旧帧并计数）或 "reject"（拒绝写入、形成背压）。
type RingBufConfig struct {
	IngressCapacity int    `yaml:"ingress_capacity"` // 传输 -> ASR
	EgressCapacity  int    `yaml:"egress_capacity"`  // TTS -> 传输
	IngressPolicy   string `yaml:"ingress_policy"`
	EgressPolicy    string `yaml:"egress_policy"`
}

// PipelineConfig 限定阶段之间文本侧 channel 的容量。这些上限既是内存边界、
// 又是背压机制：满 channel 阻塞其上游阶段（文本不可丢），把压力一路回传，
// 直到在音频入口环上以 drop-oldest 显现。
type PipelineConfig struct {
	TokenChanCap      int `yaml:"token_chan_cap"`      // LLM -> TTS 的 token
	TranscriptChanCap int `yaml:"transcript_chan_cap"` // ASR finals -> 编排器
	AudioChanCap      int `yaml:"audio_chan_cap"`      // 每阶段音频跳点缓冲
}

// VADConfig 保存内联能量 VAD 的门限与抖动滤波参数。EnergyThreshold 是进入
//（说话）门限；ExitThreshold 是更低的「维持说话」门限——两者之间的间隙就是
// 抑制在单条线附近抖动的滞回（双门限）。
type VADConfig struct {
	EnergyThreshold float64       `yaml:"energy_threshold"`
	ExitThreshold   float64       `yaml:"exit_threshold"`
	MinSpeech       time.Duration `yaml:"min_speech"`
	Hangover        time.Duration `yaml:"hangover"`
}

// SessionConfig 保存会话生命周期与重连设置。单条 WS/TCP 连接上帧是有序的，
// 所以没有会话内重排窗口；重连重放去重用一条序列号水位即可（TCP 的有序性
// 让它充分，无需窗口）。IdleTimeout 兼作重连宽限期：一个脱离附着的会话能
// 活这么久。
type SessionConfig struct {
	IdleTimeout time.Duration `yaml:"idle_timeout"`
}

// AdaptersConfig 为每个阶段选择模型适配器实现。
type AdaptersConfig struct {
	ASR string `yaml:"asr"` // "mock" | "cloud" | ...
	LLM string `yaml:"llm"` // "mock" | "openai-compat"
	TTS string `yaml:"tts"`

	CloudLLM CloudLLMConfig     `yaml:"cloud_llm"`
	Mock     MockAdaptersConfig `yaml:"mock"`
}

// MockAdaptersConfig 给内建 mock 适配器注入时延，让负载 harness 能塑造真实的
// 轮时序（零延迟的 mock 会近乎瞬间完成一轮，留不下可供打断的「在飞」窗口）。
// 所有值默认为零：除非显式要求，mock 保持瞬时。
type MockAdaptersConfig struct {
	ASRFinalDelay  time.Duration `yaml:"asr_final_delay"` // final 转写之前
	LLMTokenDelay  time.Duration `yaml:"llm_token_delay"` // 每个 token 之前
	LLMTokenJitter time.Duration `yaml:"llm_token_jitter"`
	TTSFrameDelay  time.Duration `yaml:"tts_frame_delay"` // 每帧合成之前
}

// CloudLLMConfig 把 "openai-compat" LLM 适配器指向任意 OpenAI 风格的
// chat-completions 端点（DeepSeek、Qwen、Moonshot、OpenAI…）。API key 绝不
// 存进文件：APIKeyEnv 指明持有它的环境变量名。
type CloudLLMConfig struct {
	BaseURL   string `yaml:"base_url"`
	Model     string `yaml:"model"`
	APIKeyEnv string `yaml:"api_key_env"`
}

// PeripheralsConfig 开关可选的、不在热路径上的外部系统。
type PeripheralsConfig struct {
	RedisEnabled bool   `yaml:"redis_enabled"`
	RedisAddr    string `yaml:"redis_addr"`
	KafkaEnabled bool   `yaml:"kafka_enabled"`
	KafkaBrokers string `yaml:"kafka_brokers"`
}

// Default 返回一份适合本地开发的合理默认配置：WebSocket 监听 :8080，
// 16kHz/16 位/单声道音频、20ms 一帧，所有模型适配器走 mock，可选外设关闭。
func Default() Config {
	return Config{
		Server: ServerConfig{
			Addr:            ":8080",
			HeartbeatPeriod: 10 * time.Second,
			StaticDir:       "web",
		},
		Audio: AudioConfig{
			SampleRate:    16000,
			FrameDuration: 20 * time.Millisecond,
			Channels:      1,
			BitsPerSample: 16,
		},
		RingBuf: RingBufConfig{
			IngressCapacity: 64,
			EgressCapacity:  64,
			IngressPolicy:   "drop_oldest",
			EgressPolicy:    "drop_oldest",
		},
		Pipeline: PipelineConfig{
			TokenChanCap:      32,
			TranscriptChanCap: 2,
			AudioChanCap:      8,
		},
		VAD: VADConfig{
			EnergyThreshold: 0.01,
			ExitThreshold:   0.005,
			MinSpeech:       100 * time.Millisecond,
			Hangover:        300 * time.Millisecond,
		},
		Session: SessionConfig{
			IdleTimeout: 60 * time.Second,
		},
		Adapters: AdaptersConfig{
			ASR: "mock", LLM: "mock", TTS: "mock",
			CloudLLM: CloudLLMConfig{
				BaseURL:   "https://api.deepseek.com/v1",
				Model:     "deepseek-chat",
				APIKeyEnv: "VOICESTREAM_LLM_API_KEY",
			},
		},
		Peripherals: PeripheralsConfig{
			RedisEnabled: false,
			KafkaEnabled: false,
		},
	}
}

// Load 在默认值之上读入一个 YAML 配置文件，再施加环境变量覆盖，最后校验结果。
func Load(path string) (Config, error) {
	cfg := Default()
	data, err := os.ReadFile(path)
	if err != nil {
		return Config{}, fmt.Errorf("read config %q: %w", path, err)
	}
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return Config{}, fmt.Errorf("parse config %q: %w", path, err)
	}
	cfg.applyEnvOverrides()
	if err := cfg.Validate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

// applyEnvOverrides 允许少数高价值设置从环境变量覆盖（便于容器和快速本地
// 运行）。
func (c *Config) applyEnvOverrides() {
	if v := os.Getenv("VOICESTREAM_ADDR"); v != "" {
		c.Server.Addr = v
	}
	if v := os.Getenv("VOICESTREAM_REDIS_ADDR"); v != "" {
		c.Peripherals.RedisEnabled = true
		c.Peripherals.RedisAddr = v
	}
	if v := os.Getenv("VOICESTREAM_SAMPLE_RATE"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			c.Audio.SampleRate = n
		}
	}
}

// Validate 检查引擎所要求的各项配置不变量。
func (c Config) Validate() error {
	if c.Server.Addr == "" {
		return fmt.Errorf("server.addr must not be empty")
	}
	if c.Audio.SampleRate <= 0 {
		return fmt.Errorf("audio.sample_rate must be positive, got %d", c.Audio.SampleRate)
	}
	if c.Audio.Channels <= 0 {
		return fmt.Errorf("audio.channels must be positive, got %d", c.Audio.Channels)
	}
	if !isPowerOfTwo(c.RingBuf.IngressCapacity) {
		return fmt.Errorf("ring_buffer.ingress_capacity must be a power of two, got %d", c.RingBuf.IngressCapacity)
	}
	if !isPowerOfTwo(c.RingBuf.EgressCapacity) {
		return fmt.Errorf("ring_buffer.egress_capacity must be a power of two, got %d", c.RingBuf.EgressCapacity)
	}
	if _, err := ringbuf.ParsePolicy(c.RingBuf.IngressPolicy); err != nil {
		return fmt.Errorf("ring_buffer.ingress_policy: %w", err)
	}
	if c.Pipeline.TokenChanCap <= 0 || c.Pipeline.TranscriptChanCap <= 0 || c.Pipeline.AudioChanCap <= 0 {
		return fmt.Errorf("pipeline channel caps must be positive, got %+v", c.Pipeline)
	}
	if _, err := ringbuf.ParsePolicy(c.RingBuf.EgressPolicy); err != nil {
		return fmt.Errorf("ring_buffer.egress_policy: %w", err)
	}
	if c.VAD.Hangover < c.VAD.MinSpeech {
		return fmt.Errorf("vad.hangover (%s) should be >= vad.min_speech (%s)", c.VAD.Hangover, c.VAD.MinSpeech)
	}
	if c.VAD.ExitThreshold <= 0 || c.VAD.ExitThreshold > c.VAD.EnergyThreshold {
		return fmt.Errorf("vad.exit_threshold must be in (0, energy_threshold], got %g vs %g",
			c.VAD.ExitThreshold, c.VAD.EnergyThreshold)
	}
	if c.Session.IdleTimeout <= 0 {
		return fmt.Errorf("session.idle_timeout must be positive, got %s", c.Session.IdleTimeout)
	}
	m := c.Adapters.Mock
	if m.ASRFinalDelay < 0 || m.LLMTokenDelay < 0 || m.LLMTokenJitter < 0 || m.TTSFrameDelay < 0 {
		return fmt.Errorf("adapters.mock delays must be non-negative, got %+v", m)
	}
	return nil
}

func isPowerOfTwo(n int) bool {
	return n > 0 && n&(n-1) == 0
}
