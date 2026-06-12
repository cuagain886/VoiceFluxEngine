// Command server is the entrypoint for the voicestream real-time voice kernel.
// It loads/validates configuration and runs the WebSocket transport (M2). The
// ASR->LLM->TTS pipeline replaces the echo handler in M5.
package main

import (
	"context"
	"log/slog"
	"net/http"
	"net/http/pprof"
	"os"
	"os/signal"

	"voicestream/internal/adapter"
	"voicestream/internal/config"
	"voicestream/internal/metrics"
	"voicestream/internal/session"
	"voicestream/internal/transport"

	// Self-registering adapters, selectable via config.
	_ "voicestream/internal/adapter/openaicompat"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	cfg := config.Default()
	if path := os.Getenv("VOICESTREAM_CONFIG"); path != "" {
		loaded, err := config.Load(path)
		if err != nil {
			logger.Error("failed to load config", "path", path, "err", err)
			os.Exit(1)
		}
		cfg = loaded
	}
	if err := cfg.Validate(); err != nil {
		logger.Error("invalid config", "err", err)
		os.Exit(1)
	}

	// Assemble adapters now so a bad selection or missing API key fails at
	// startup, not mid-conversation.
	set, err := adapter.Build(cfg)
	if err != nil {
		logger.Error("adapter assembly failed", "err", err)
		os.Exit(1)
	}

	logger.Info("voicestream starting",
		"addr", cfg.Server.Addr,
		"sample_rate", cfg.Audio.SampleRate,
		"frame", cfg.Audio.FrameDuration.String(),
		"asr", cfg.Adapters.ASR, "llm", cfg.Adapters.LLM, "tts", cfg.Adapters.TTS,
	)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	m := metrics.New()
	mgr := session.NewManager(cfg, set, logger, m)
	go mgr.Run(ctx) // idle reaper; reclaims all sessions on shutdown

	srv := transport.NewServerWithHandler(cfg.Server, logger, mgr.Handler())
	srv.RegisterRoute("/metrics", m.Registry.Handler()) // Prometheus scrape
	srv.RegisterRoute("/debug/turns", m.Hub)            // dashboard SSE feed
	// pprof for the 11.2 hot-path iteration (single-tenant dev kernel; gate
	// behind auth before any multi-tenant exposure).
	srv.RegisterRoute("/debug/pprof/", http.HandlerFunc(pprof.Index))
	srv.RegisterRoute("/debug/pprof/profile", http.HandlerFunc(pprof.Profile))
	srv.RegisterRoute("/debug/pprof/trace", http.HandlerFunc(pprof.Trace))
	if err := srv.Run(ctx); err != nil {
		logger.Error("transport server error", "err", err)
		os.Exit(1)
	}
	logger.Info("voicestream stopped")
}
