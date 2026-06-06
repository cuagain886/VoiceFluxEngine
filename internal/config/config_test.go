package config

import "testing"

func TestDefaultIsValid(t *testing.T) {
	if err := Default().Validate(); err != nil {
		t.Fatalf("default config should be valid: %v", err)
	}
}

func TestValidateRejectsNonPowerOfTwoCapacity(t *testing.T) {
	cfg := Default()
	cfg.RingBuf.IngressCapacity = 100 // not a power of two
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected validation error for non-power-of-two ingress capacity")
	}
}

func TestValidateRejectsHangoverBelowMinSpeech(t *testing.T) {
	cfg := Default()
	cfg.VAD.Hangover = cfg.VAD.MinSpeech - 1
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected validation error when hangover < min_speech")
	}
}

func TestEnvOverrideAddr(t *testing.T) {
	t.Setenv("VOICESTREAM_ADDR", ":9999")
	cfg := Default()
	cfg.applyEnvOverrides()
	if cfg.Server.Addr != ":9999" {
		t.Fatalf("expected addr override :9999, got %q", cfg.Server.Addr)
	}
}
