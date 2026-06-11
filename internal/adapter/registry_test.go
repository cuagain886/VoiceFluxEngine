package adapter

import (
	"strings"
	"testing"

	"voicestream/internal/config"
)

func TestBuildAllMock(t *testing.T) {
	set, err := Build(config.Default())
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	if set.ASR == nil || set.LLM == nil || set.TTS == nil {
		t.Fatalf("Build left adapters nil: %+v", set)
	}
}

func TestBuildUnknownName(t *testing.T) {
	cfg := config.Default()
	cfg.Adapters.LLM = "no-such-llm"
	_, err := Build(cfg)
	if err == nil {
		t.Fatal("expected error for unknown adapter name")
	}
	if !strings.Contains(err.Error(), "mock") {
		t.Fatalf("error should list registered names, got: %v", err)
	}
}
