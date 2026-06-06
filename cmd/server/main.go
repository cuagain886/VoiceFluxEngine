// Command server is the entrypoint for the voicestream real-time voice kernel.
// It loads/validates configuration and runs the WebSocket transport (M2). The
// ASR->LLM->TTS pipeline replaces the echo handler in M5.
package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"

	"voicestream/internal/config"
	"voicestream/internal/transport"
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

	logger.Info("voicestream starting",
		"addr", cfg.Server.Addr,
		"sample_rate", cfg.Audio.SampleRate,
		"frame", cfg.Audio.FrameDuration.String(),
		"adapters", cfg.Adapters,
	)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	srv := transport.NewServer(cfg.Server, logger)
	if err := srv.Run(ctx); err != nil {
		logger.Error("transport server error", "err", err)
		os.Exit(1)
	}
	logger.Info("voicestream stopped")
}
