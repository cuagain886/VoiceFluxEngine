package metrics

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
)

const hubHistory = 50

// Hub 通过 SSE 把逐轮记录扇出给仪表盘订阅者。Publish 按契约是非阻塞的——
// 它跑在流水线的编排器 goroutine 上，所以一个慢仪表盘会丢事件，而不是拖住
// 轮。新订阅者会被回放近期历史，使瀑布图在打开时不至于空白。
type Hub struct {
	mu   sync.Mutex
	subs map[chan []byte]struct{}
	hist [][]byte
}

// NewHub 返回一个空的 hub。
func NewHub() *Hub {
	return &Hub{subs: map[chan []byte]struct{}{}}
}

// Publish 把 v 序列化，投递进历史以及每个订阅者。
func (h *Hub) Publish(v any) {
	b, err := json.Marshal(v)
	if err != nil {
		return
	}
	h.mu.Lock()
	h.hist = append(h.hist, b)
	if len(h.hist) > hubHistory {
		h.hist = h.hist[len(h.hist)-hubHistory:]
	}
	for ch := range h.subs {
		select {
		case ch <- b:
		default: // 慢订阅者：丢弃，绝不阻塞
		}
	}
	h.mu.Unlock()
}

func (h *Hub) subscribe() chan []byte {
	ch := make(chan []byte, 64)
	h.mu.Lock()
	for _, b := range h.hist {
		ch <- b // 装得下：历史长度 < channel 容量
	}
	h.subs[ch] = struct{}{}
	h.mu.Unlock()
	return ch
}

func (h *Hub) unsubscribe(ch chan []byte) {
	h.mu.Lock()
	delete(h.subs, ch)
	h.mu.Unlock()
}

// ServeHTTP 以 server-sent events 流式输出记录。
func (h *Hub) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	fl, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")

	ch := h.subscribe()
	defer h.unsubscribe(ch)
	for {
		select {
		case b := <-ch:
			if _, err := fmt.Fprintf(w, "data: %s\n\n", b); err != nil {
				return
			}
			fl.Flush()
		case <-r.Context().Done():
			return
		}
	}
}
