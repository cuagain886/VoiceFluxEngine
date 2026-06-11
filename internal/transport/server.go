package transport

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"time"

	"voicestream/internal/config"

	"github.com/coder/websocket"
)

// Handler processes a single accepted connection. M2 ships an echo handler;
// M5 replaces it with the real ASR->LLM->TTS pipeline. It returns when the
// connection ends (the returned error is informational, e.g. io.EOF on a clean
// client close).
type Handler func(ctx context.Context, conn Conn) error

// Server terminates WebSocket connections and runs a Handler per connection.
type Server struct {
	cfg     config.ServerConfig
	log     *slog.Logger
	handler Handler
}

// NewServer returns a Server wired with the echo handler (the M2 default).
func NewServer(cfg config.ServerConfig, log *slog.Logger) *Server {
	return NewServerWithHandler(cfg, log, EchoHandler(cfg.HeartbeatPeriod, log))
}

// NewServerWithHandler returns a Server running the given per-connection
// handler (M7+: the voice session handler).
func NewServerWithHandler(cfg config.ServerConfig, log *slog.Logger, h Handler) *Server {
	return &Server{cfg: cfg, log: log, handler: h}
}

// Run starts the HTTP/WebSocket server and blocks until ctx is cancelled.
//
// Note on low-latency tuning: Go's net package enables TCP_NODELAY on all TCP
// connections by default, so accepted connections already disable Nagle — no
// action needed here beyond disabling WebSocket compression (see handleWS).
func (s *Server) Run(ctx context.Context) error {
	httpServer := &http.Server{Addr: s.cfg.Addr, Handler: s.HTTPHandler()}

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = httpServer.Shutdown(shutdownCtx)
	}()

	s.log.Info("transport listening", "addr", s.cfg.Addr, "path", "/ws")
	if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

// HTTPHandler returns the server's routing (WS endpoint + optional static
// demo client), usable directly with httptest in integration tests.
func (s *Server) HTTPHandler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/ws", s.handleWS)
	if dir := s.cfg.StaticDir; dir != "" {
		// Serve the demo client (web/). The /ws upgrade path keeps priority.
		mux.Handle("/", http.FileServer(http.Dir(dir)))
	}
	return mux
}

func (s *Server) handleWS(w http.ResponseWriter, r *http.Request) {
	c, err := websocket.Accept(w, r, &websocket.AcceptOptions{
		// Disable permessage-deflate: compression adds latency/CPU on small
		// real-time frames (design D13).
		CompressionMode: websocket.CompressionDisabled,
	})
	if err != nil {
		s.log.Debug("websocket accept failed", "err", err)
		return
	}
	conn := newWSConn(c)
	s.log.Debug("connection accepted", "remote", r.RemoteAddr)

	err = s.handler(r.Context(), conn)
	s.log.Debug("connection closed", "remote", r.RemoteAddr, "err", err)
	_ = conn.Close(StatusNormalClosure, "bye")
}

// EchoHandler returns a Handler that echoes every received frame back to the
// sender. It uses the per-connection read/write goroutine model: one goroutine
// reads, one goroutine is the sole writer (frames + heartbeat pings), which
// satisfies the transport's single-writer constraint.
func EchoHandler(heartbeat time.Duration, log *slog.Logger) Handler {
	if heartbeat <= 0 {
		heartbeat = 10 * time.Second
	}
	return func(ctx context.Context, conn Conn) error {
		ctx, cancel := context.WithCancel(ctx)
		defer cancel()

		egress := make(chan Frame, 16)
		errc := make(chan error, 2)

		// Read goroutine: decode frames and forward to the writer. In M2 the
		// "pipeline" is a direct forward (echo); M5 replaces it.
		go func() {
			for {
				f, err := conn.ReadFrame(ctx)
				if err != nil {
					errc <- err
					return
				}
				select {
				case egress <- f:
				case <-ctx.Done():
					errc <- ctx.Err()
					return
				}
			}
		}()

		// Write goroutine: sole writer — frames and heartbeat pings.
		go func() {
			t := time.NewTicker(heartbeat)
			defer t.Stop()
			for {
				select {
				case f := <-egress:
					if err := conn.WriteFrame(ctx, f); err != nil {
						errc <- err
						return
					}
				case <-t.C:
					pctx, pcancel := context.WithTimeout(ctx, heartbeat)
					err := conn.Ping(pctx)
					pcancel()
					if err != nil {
						errc <- err // heartbeat timeout => treat as disconnect
						return
					}
				case <-ctx.Done():
					errc <- ctx.Err()
					return
				}
			}
		}()

		return <-errc
	}
}
