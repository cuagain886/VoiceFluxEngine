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

// Handler 处理单个已 accept 的连接。M2 交付一个 echo handler；M5 把它换成
// 真正的 ASR->LLM->TTS 流水线。连接结束时返回（返回的 error 是信息性的，
// 例如客户端干净关闭时的 io.EOF）。
type Handler func(ctx context.Context, conn Conn) error

// Server 终结 WebSocket 连接，并为每个连接跑一个 Handler。
type Server struct {
	cfg     config.ServerConfig
	log     *slog.Logger
	handler Handler
	routes  map[string]http.Handler
}

// NewServer 返回一个接好 echo handler 的 Server（M2 默认）。
func NewServer(cfg config.ServerConfig, log *slog.Logger) *Server {
	return NewServerWithHandler(cfg, log, EchoHandler(cfg.HeartbeatPeriod, log))
}

// NewServerWithHandler 返回一个跑给定「每连接 handler」的 Server
//（M7+：语音会话 handler）。
func NewServerWithHandler(cfg config.ServerConfig, log *slog.Logger, h Handler) *Server {
	return &Server{cfg: cfg, log: log, handler: h, routes: map[string]http.Handler{}}
}

// RegisterRoute 在服务器的 mux 上挂一个额外的 HTTP handler（指标、仪表盘）。
// 必须在 Run / HTTPHandler 之前调用。
func (s *Server) RegisterRoute(pattern string, h http.Handler) {
	s.routes[pattern] = h
}

// Run 启动 HTTP/WebSocket 服务器并阻塞，直到 ctx 被取消。
//
// 关于低延迟调优：Go 的 net 包默认对所有 TCP 连接开启 TCP_NODELAY，所以
// accept 进来的连接已经禁用了 Nagle——除了关闭 WebSocket 压缩（见 handleWS）
// 之外，这里无需额外动作。
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

// HTTPHandler 返回服务器的路由（WS 端点 + 可选的静态演示客户端），可在集成
// 测试里直接配合 httptest 使用。
func (s *Server) HTTPHandler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/ws", s.handleWS)
	for pattern, h := range s.routes {
		mux.Handle(pattern, h)
	}
	if dir := s.cfg.StaticDir; dir != "" {
		// 提供演示客户端（web/）。/ws 升级路径优先级更高。
		mux.Handle("/", http.FileServer(http.Dir(dir)))
	}
	return mux
}

func (s *Server) handleWS(w http.ResponseWriter, r *http.Request) {
	c, err := websocket.Accept(w, r, &websocket.AcceptOptions{
		// 禁用 permessage-deflate：在小的实时帧上压缩只会增加延迟/CPU
		//（设计 D13）。
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

// EchoHandler 返回一个把收到的每个帧原样回送给发送方的 Handler。它采用
// 每连接「一读一写」goroutine 模型：一个 goroutine 读，一个 goroutine 是
// 唯一的写者（帧 + 心跳 ping），从而满足传输的单写者约束。
func EchoHandler(heartbeat time.Duration, log *slog.Logger) Handler {
	if heartbeat <= 0 {
		heartbeat = 10 * time.Second
	}
	return func(ctx context.Context, conn Conn) error {
		ctx, cancel := context.WithCancel(ctx)
		defer cancel()

		egress := make(chan Frame, 16)
		errc := make(chan error, 2)

		// 读 goroutine：解码帧并转发给写者。M2 里「流水线」就是直接转发
		//（echo）；M5 会替换它。
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

		// 写 goroutine：唯一的写者——帧与心跳 ping。
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
						errc <- err // 心跳超时 => 当作断连处理
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
