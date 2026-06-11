// Package config defines the global runtime configuration for the voice
// streaming kernel. It supports loading from a YAML file with environment
// variable overrides, plus validation of invariants (e.g. ring buffer
// capacities must be powers of two).
package config

import (
	"fmt"
	"os"
	"strconv"
	"time"

	"gopkg.in/yaml.v3"

	"voicestream/internal/ringbuf"
)

// Config is the root runtime configuration for the engine.
type Config struct {
	Server      ServerConfig      `yaml:"server"`
	Audio       AudioConfig       `yaml:"audio"`
	RingBuf     RingBufConfig     `yaml:"ring_buffer"`
	VAD         VADConfig         `yaml:"vad"`
	Session     SessionConfig     `yaml:"session"`
	Adapters    AdaptersConfig    `yaml:"adapters"`
	Peripherals PeripheralsConfig `yaml:"peripherals"`
}

// ServerConfig holds transport/server settings.
type ServerConfig struct {
	Addr            string        `yaml:"addr"`
	HeartbeatPeriod time.Duration `yaml:"heartbeat_period"`
}

// AudioConfig describes the canonical internal audio format (v1 assumes the
// client delivers PCM at this format directly; FFmpeg transcoding is deferred).
type AudioConfig struct {
	SampleRate    int           `yaml:"sample_rate"`    // e.g. 16000
	FrameDuration time.Duration `yaml:"frame_duration"` // e.g. 20ms
	Channels      int           `yaml:"channels"`       // 1 = mono
	BitsPerSample int           `yaml:"bits_per_sample"`// 16
}

// RingBufConfig sizes the lock-free audio edge buffers. Capacities are in
// frames and MUST be powers of two (validated) so the buffer can mask indices.
// Policies pick the full-buffer behaviour per edge: "drop_oldest" (evict the
// stalest frame, count it) or "reject" (refuse the write, backpressure).
type RingBufConfig struct {
	IngressCapacity int    `yaml:"ingress_capacity"` // transport -> ASR
	EgressCapacity  int    `yaml:"egress_capacity"`  // TTS -> transport
	IngressPolicy   string `yaml:"ingress_policy"`
	EgressPolicy    string `yaml:"egress_policy"`
}

// VADConfig holds the inline energy-VAD thresholds and jitter filters.
type VADConfig struct {
	EnergyThreshold float64       `yaml:"energy_threshold"`
	MinSpeech       time.Duration `yaml:"min_speech"`
	Hangover        time.Duration `yaml:"hangover"`
}

// SessionConfig holds session lifecycle and reconnect settings. Note: over a
// single WS/TCP connection frames are in-order, so there is no in-session
// reorder window; DedupWindow only guards reconnect-replay.
type SessionConfig struct {
	IdleTimeout time.Duration `yaml:"idle_timeout"`
	DedupWindow int           `yaml:"dedup_window"` // seq window for replay dedup
}

// AdaptersConfig selects the model adapter implementations per stage.
type AdaptersConfig struct {
	ASR string `yaml:"asr"` // "mock" | "cloud" | ...
	LLM string `yaml:"llm"` // "mock" | "openai-compat"
	TTS string `yaml:"tts"`

	CloudLLM CloudLLMConfig `yaml:"cloud_llm"`
}

// CloudLLMConfig points the "openai-compat" LLM adapter at any OpenAI-style
// chat-completions endpoint (DeepSeek, Qwen, Moonshot, OpenAI, ...). The API
// key is never stored in the file: APIKeyEnv names the environment variable
// that holds it.
type CloudLLMConfig struct {
	BaseURL   string `yaml:"base_url"`
	Model     string `yaml:"model"`
	APIKeyEnv string `yaml:"api_key_env"`
}

// PeripheralsConfig toggles optional, non-hot-path external systems.
type PeripheralsConfig struct {
	RedisEnabled bool   `yaml:"redis_enabled"`
	RedisAddr    string `yaml:"redis_addr"`
	KafkaEnabled bool   `yaml:"kafka_enabled"`
	KafkaBrokers string `yaml:"kafka_brokers"`
}

// Default returns a configuration with sensible defaults for local dev:
// WebSocket on :8080, 16kHz/16-bit/mono audio in 20ms frames, all model
// adapters mocked, optional peripherals disabled.
func Default() Config {
	return Config{
		Server: ServerConfig{
			Addr:            ":8080",
			HeartbeatPeriod: 10 * time.Second,
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
		VAD: VADConfig{
			EnergyThreshold: 0.01,
			MinSpeech:       100 * time.Millisecond,
			Hangover:        300 * time.Millisecond,
		},
		Session: SessionConfig{
			IdleTimeout: 60 * time.Second,
			DedupWindow: 256,
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

// Load reads a YAML config file over the defaults, then applies environment
// overrides, then validates the result.
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

// applyEnvOverrides lets a few high-value settings be overridden from the
// environment (handy for containers and quick local runs).
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

// Validate checks configuration invariants required by the engine.
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
	if _, err := ringbuf.ParsePolicy(c.RingBuf.EgressPolicy); err != nil {
		return fmt.Errorf("ring_buffer.egress_policy: %w", err)
	}
	if c.VAD.Hangover < c.VAD.MinSpeech {
		return fmt.Errorf("vad.hangover (%s) should be >= vad.min_speech (%s)", c.VAD.Hangover, c.VAD.MinSpeech)
	}
	return nil
}

func isPowerOfTwo(n int) bool {
	return n > 0 && n&(n-1) == 0
}
