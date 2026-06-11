package metrics

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
)

const hubHistory = 50

// Hub fans turn records out to dashboard subscribers over SSE. Publishing is
// non-blocking by contract — it runs on the pipeline's orchestrator
// goroutine, so a slow dashboard sheds events rather than stalling turns.
// New subscribers get the recent history replayed so the waterfall is not
// empty on open.
type Hub struct {
	mu   sync.Mutex
	subs map[chan []byte]struct{}
	hist [][]byte
}

// NewHub returns an empty hub.
func NewHub() *Hub {
	return &Hub{subs: map[chan []byte]struct{}{}}
}

// Publish marshals v and delivers it to history and every subscriber.
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
		default: // slow subscriber: shed, never block
		}
	}
	h.mu.Unlock()
}

func (h *Hub) subscribe() chan []byte {
	ch := make(chan []byte, 64)
	h.mu.Lock()
	for _, b := range h.hist {
		ch <- b // fits: history < channel capacity
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

// ServeHTTP streams records as server-sent events.
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
