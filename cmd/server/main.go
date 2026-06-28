// Command server 是 voicestream 实时语音内核的入口。它加载/校验配置并运行
// WebSocket 传输（M2）。ASR->LLM->TTS 流水线在 M5 取代了 echo handler。
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

	// 自注册的适配器，可通过配置选用。
	_ "voicestream/internal/adapter/openaicompat"
	_ "voicestream/internal/adapter/openaitts"
	_ "voicestream/internal/adapter/sherpa"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	// 自动加载工作目录下的 .env（若存在），免去每次手动 source；真实环境变量优先。
	if err := config.LoadDotEnv(".env"); err != nil {
		logger.Warn("read .env", "err", err)
	}

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

	// 现在就装配适配器，让「错误的选择或缺失的 API key」在启动时失败，
	// 而不是在对话中途。
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
	go mgr.Run(ctx) // 空闲回收器；关停时回收所有会话

	srv := transport.NewServerWithHandler(cfg.Server, logger, mgr.Handler())
	srv.RegisterRoute("/metrics", m.Registry.Handler()) // Prometheus 抓取
	srv.RegisterRoute("/debug/turns", m.Hub)            // 仪表盘 SSE 源
	// pprof 供 11.2 的热路径迭代用（单租户开发内核；任何多租户暴露前需加
	// 鉴权门禁）。
	srv.RegisterRoute("/debug/pprof/", http.HandlerFunc(pprof.Index))
	srv.RegisterRoute("/debug/pprof/profile", http.HandlerFunc(pprof.Profile))
	srv.RegisterRoute("/debug/pprof/trace", http.HandlerFunc(pprof.Trace))
	if err := srv.Run(ctx); err != nil {
		logger.Error("transport server error", "err", err)
		os.Exit(1)
	}
	logger.Info("voicestream stopped")
}
