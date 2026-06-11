package adapter

import (
	"fmt"
	"sort"
	"sync"

	"voicestream/internal/config"
)

// Factory builds an adapter from the full runtime config (each implementation
// picks out the fields it needs).
type Factory[T any] func(cfg config.Config) (T, error)

// The registries follow the database/sql driver pattern: implementations
// register themselves by name (built-ins below, external ones from init() in
// their own package via blank import), and Build assembles a Set from config
// names — switching adapters is a config edit, never a core-code edit.
var (
	regMu sync.Mutex
	asrs  = map[string]Factory[ASR]{}
	llms  = map[string]Factory[LLM]{}
	ttss  = map[string]Factory[TTS]{}
)

// RegisterASR makes an ASR implementation selectable by name. It panics on a
// duplicate name: that is a programmer error, caught at startup.
func RegisterASR(name string, f Factory[ASR]) { register(asrs, "ASR", name, f) }

// RegisterLLM makes an LLM implementation selectable by name.
func RegisterLLM(name string, f Factory[LLM]) { register(llms, "LLM", name, f) }

// RegisterTTS makes a TTS implementation selectable by name.
func RegisterTTS(name string, f Factory[TTS]) { register(ttss, "TTS", name, f) }

func register[T any](reg map[string]Factory[T], kind, name string, f Factory[T]) {
	regMu.Lock()
	defer regMu.Unlock()
	if _, dup := reg[name]; dup {
		panic(fmt.Sprintf("adapter: duplicate %s registration %q", kind, name))
	}
	reg[name] = f
}

// Set is the assembled trio of adapters the pipeline runs against.
type Set struct {
	ASR ASR
	LLM LLM
	TTS TTS
}

// Build assembles the adapter set named in cfg.Adapters. Unknown names fail
// with the list of registered alternatives.
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

// Built-in mocks, selected by adapters.{asr,llm,tts} = "mock".
func init() {
	RegisterASR("mock", func(config.Config) (ASR, error) {
		return &MockASR{Script: "hello voicestream", PartialEvery: 10}, nil
	})
	RegisterLLM("mock", func(config.Config) (LLM, error) {
		return &MockLLM{}, nil
	})
	RegisterTTS("mock", func(cfg config.Config) (TTS, error) {
		return &MockTTS{
			SampleRate:    cfg.Audio.SampleRate,
			FrameDuration: cfg.Audio.FrameDuration,
		}, nil
	})
}
