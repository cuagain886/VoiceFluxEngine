// Command loadgen is the M10 load harness: it ramps concurrent virtual
// sessions against a running voicestream server and writes the capacity
// curve (CSV / JSON / self-contained HTML) plus a knee analysis.
//
// Typical run (server first, with mock latencies shaped for load testing):
//
//	$env:VOICESTREAM_CONFIG = "configs/loadtest.yaml"; go run ./cmd/server
//	go run ./cmd/loadgen -steps 2,5,10,20,40,80,160,320 -out docs/load
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"time"

	"voicestream/internal/loadgen"
)

func main() {
	var (
		url        = flag.String("url", "ws://127.0.0.1:8080/ws", "server WebSocket endpoint")
		metricsURL = flag.String("metrics", "http://127.0.0.1:8080/metrics", "server metrics endpoint; empty disables server-side scraping")
		steps      = flag.String("steps", "2,5,10,20,40,80", "comma-separated concurrency ladder")
		stepDur    = flag.Duration("step-dur", 10*time.Second, "measurement window per step")
		warmup     = flag.Duration("warmup", 2*time.Second, "settle time after scaling before measuring")
		frameInt   = flag.Duration("frame-interval", 20*time.Millisecond, "uplink pacing per frame (20ms = real time)")
		speech     = flag.Int("speech-frames", 30, "frames per utterance")
		bargeEvery = flag.Int("barge-every", 4, "interrupt every Nth turn (0 = never)")
		netemDelay = flag.Duration("netem-delay", 0, "base extra uplink delay per frame")
		netemJit   = flag.Duration("netem-jitter", 0, "max extra uniform uplink jitter")
		burstEvery = flag.Duration("netem-burst-every", 0, "burst window period (with -netem-burst-hold)")
		burstHold  = flag.Duration("netem-burst-hold", 0, "hold span at each window start, released as a burst")
		out        = flag.String("out", "", "directory for capacity.{csv,json,html}; empty = stdout only")
	)
	flag.Parse()

	stepList, err := parseSteps(*steps)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}

	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	cfg := loadgen.Config{
		URL:           *url,
		MetricsURL:    *metricsURL,
		Steps:         stepList,
		StepDuration:  *stepDur,
		Warmup:        *warmup,
		FrameInterval: *frameInt,
		SpeechFrames:  *speech,
		BargeEvery:    *bargeEvery,
		Netem: loadgen.Netem{
			Delay: *netemDelay, Jitter: *netemJit,
			BurstEvery: *burstEvery, BurstHold: *burstHold,
		},
		Logger: logger,
	}

	rep, err := loadgen.Run(ctx, cfg)
	if rep != nil && len(rep.Records) > 0 {
		fmt.Println(rep.Table())
		if *out != "" {
			if werr := rep.WriteFiles(*out); werr != nil {
				logger.Error("write report", "err", werr)
				os.Exit(1)
			}
			fmt.Printf("报告已写入 %s/capacity.{csv,json,html}\n", *out)
		}
	}
	if err != nil && ctx.Err() == nil {
		logger.Error("loadgen failed", "err", err)
		os.Exit(1)
	}
}

func parseSteps(s string) ([]int, error) {
	var out []int
	for _, part := range strings.Split(s, ",") {
		n, err := strconv.Atoi(strings.TrimSpace(part))
		if err != nil || n <= 0 {
			return nil, fmt.Errorf("invalid -steps %q: each entry must be a positive integer", s)
		}
		out = append(out, n)
	}
	return out, nil
}
