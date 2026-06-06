// Command server is the entrypoint for the voicestream real-time voice kernel.
// At the scaffold stage (M1) it loads and validates configuration and logs
// startup; the WebSocket transport server is wired in M2.
package main

import (
	"log/slog"
	"os"

	"voicestream/internal/config"
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
	logger.Info("scaffold ready — transport server not yet implemented (M2)")
}
