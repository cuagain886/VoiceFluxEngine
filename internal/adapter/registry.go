package adapter

import (
	"fmt"
	"sort"
	"sync"

	"voicestream/internal/config"
)

// Factory 从完整的运行时配置构建一个适配器（每个实现各自挑出它需要的字段）。
type Factory[T any] func(cfg config.Config) (T, error)

// 这些注册表遵循 database/sql 驱动模式：实现按名字自注册（内建的在下方，
// 外部的通过空白导入在各自包的 init() 里注册），Build 按配置里的名字装配出
// 一个 Set——切换适配器是改配置，绝不是改核心代码。
var (
	regMu sync.Mutex
	asrs  = map[string]Factory[ASR]{}
	llms  = map[string]Factory[LLM]{}
	ttss  = map[string]Factory[TTS]{}
)

// RegisterASR 让一个 ASR 实现可按名字选用。重名时 panic：那是编程错误，在
// 启动时就被抓住。
func RegisterASR(name string, f Factory[ASR]) { register(asrs, "ASR", name, f) }

// RegisterLLM 让一个 LLM 实现可按名字选用。
func RegisterLLM(name string, f Factory[LLM]) { register(llms, "LLM", name, f) }

// RegisterTTS 让一个 TTS 实现可按名字选用。
func RegisterTTS(name string, f Factory[TTS]) { register(ttss, "TTS", name, f) }

func register[T any](reg map[string]Factory[T], kind, name string, f Factory[T]) {
	regMu.Lock()
	defer regMu.Unlock()
	if _, dup := reg[name]; dup {
		panic(fmt.Sprintf("adapter: duplicate %s registration %q", kind, name))
	}
	reg[name] = f
}

// Set 是流水线运行时所依赖的、装配好的三件套适配器。
type Set struct {
	ASR ASR
	LLM LLM
	TTS TTS
}

// Build 按 cfg.Adapters 里的名字装配适配器集合。未知名字会带着「已注册的
// 备选列表」失败。
func Build(cfg config.Config) (Set, error) {
	var (
		s   Set
		err error
	)
	if s.ASR, err = build(asrs, "ASR", cfg.Adapters.ASR, cfg); err != nil {
		return Set{}, err
	}
	if s.LLM, err = build(llms, "LLM", cfg.Adapters.LLM, cfg); err != nil {
		return Set{}, err
	}
	if s.TTS, err = build(ttss, "TTS", cfg.Adapters.TTS, cfg); err != nil {
		return Set{}, err
	}
	return s, nil
}

func build[T any](reg map[string]Factory[T], kind, name string, cfg config.Config) (T, error) {
	regMu.Lock()
	f, ok := reg[name]
	regMu.Unlock()
	if !ok {
		var zero T
		return zero, fmt.Errorf("adapter: unknown %s %q (registered: %v)", kind, name, names(reg))
	}
	return f(cfg)
}

func names[T any](reg map[string]Factory[T]) []string {
	regMu.Lock()
	defer regMu.Unlock()
	ns := make([]string, 0, len(reg))
	for n := range reg {
		ns = append(ns, n)
	}
	sort.Strings(ns)
	return ns
}

// 内建 mock，由 adapters.{asr,llm,tts} = "mock" 选用。可选的 adapters.mock
// 时延用来为负载 harness 塑造轮的时序；它们默认为零（瞬时 mock）。
func init() {
	RegisterASR("mock", func(cfg config.Config) (ASR, error) {
		return &MockASR{
			Script:       "hello voicestream",
			PartialEvery: 10,
			Latency:      Latency{Delay: cfg.Adapters.Mock.ASRFinalDelay},
		}, nil
	})
	RegisterLLM("mock", func(cfg config.Config) (LLM, error) {
		return &MockLLM{Latency: Latency{
			Delay:  cfg.Adapters.Mock.LLMTokenDelay,
			Jitter: cfg.Adapters.Mock.LLMTokenJitter,
			Seed:   1,
		}}, nil
	})
	RegisterTTS("mock", func(cfg config.Config) (TTS, error) {
		return &MockTTS{
			SampleRate:    cfg.Audio.SampleRate,
			FrameDuration: cfg.Audio.FrameDuration,
			Latency:       Latency{Delay: cfg.Adapters.Mock.TTSFrameDelay},
		}, nil
	})
}
