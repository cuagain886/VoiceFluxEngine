// Package metrics 提供内核的延迟/丢帧埋点（M9）：一个零依赖、Prometheus
// 文本格式的注册表供抓取，以及一个 SSE hub 喂给实时逐轮瀑布仪表盘。指标是
// 背景板而非内核（绝不可触碰热路径的分配或阻塞行为）：这里的一切都是原子
// 计数器与非阻塞扇出。
package metrics

import (
	"fmt"
	"math"
	"net/http"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
)

// Counter 是一个单调递增的值。
type Counter struct {
	name, help string
	v          atomic.Uint64
}

// Add 递增计数器。
func (c *Counter) Add(n uint64) { c.v.Add(n) }

// Value 返回当前计数。
func (c *Counter) Value() uint64 { return c.v.Load() }

// Histogram 是定桶延迟直方图（Prometheus 语义：累积桶、隐式 +Inf、sum 与
// count）。
type Histogram struct {
	name, help string
	bounds     []float64 // 上界，秒，升序
	counts     []atomic.Uint64
	inf        atomic.Uint64
	sumBits    atomic.Uint64 // float64 的位表示，CAS 累加
	count      atomic.Uint64
}

// Observe 记录一个以秒为单位的值。
func (h *Histogram) Observe(seconds float64) {
	if seconds < 0 || math.IsNaN(seconds) {
		return
	}
	idx := sort.SearchFloat64s(h.bounds, seconds)
	if idx < len(h.bounds) {
		h.counts[idx].Add(1)
	} else {
		h.inf.Add(1)
	}
	h.count.Add(1)
	for {
		old := h.sumBits.Load()
		newBits := math.Float64bits(math.Float64frombits(old) + seconds)
		if h.sumBits.CompareAndSwap(old, newBits) {
			return
		}
	}
}

// Count 返回观测次数。
func (h *Histogram) Count() uint64 { return h.count.Load() }

// gaugeFunc 在抓取期才采样——用于「真身在别处」的值（活动会话数、环丢帧
// 计数等）。
type gaugeFunc struct {
	name, help string
	fn         func() float64
}

// Registry 持有各仪表，并渲染 Prometheus 文本暴露格式。
type Registry struct {
	mu    sync.Mutex
	order []func(w *textWriter)
}

// NewRegistry 返回一个空注册表。
func NewRegistry() *Registry { return &Registry{} }

// NewCounter 注册并返回一个计数器。
func (r *Registry) NewCounter(name, help string) *Counter {
	c := &Counter{name: name, help: help}
	r.add(func(w *textWriter) {
		w.head(c.name, c.help, "counter")
		w.line(c.name, "", float64(c.v.Load()))
	})
	return c
}

// NewHistogram 注册并返回一个带给定上界（秒，升序）的直方图。
func (r *Registry) NewHistogram(name, help string, bounds []float64) *Histogram {
	h := &Histogram{name: name, help: help, bounds: bounds, counts: make([]atomic.Uint64, len(bounds))}
	r.add(func(w *textWriter) {
		w.head(h.name, h.help, "histogram")
		cum := uint64(0)
		for i, b := range h.bounds {
			cum += h.counts[i].Load()
			w.line(h.name+"_bucket", `le="`+formatFloat(b)+`"`, float64(cum))
		}
		cum += h.inf.Load()
		w.line(h.name+"_bucket", `le="+Inf"`, float64(cum))
		w.line(h.name+"_sum", "", math.Float64frombits(h.sumBits.Load()))
		w.line(h.name+"_count", "", float64(h.count.Load()))
	})
	return h
}

// NewGaugeFunc 注册一个抓取期采样的 gauge。
func (r *Registry) NewGaugeFunc(name, help string, fn func() float64) {
	g := &gaugeFunc{name: name, help: help, fn: fn}
	r.add(func(w *textWriter) {
		w.head(g.name, g.help, "gauge")
		w.line(g.name, "", g.fn())
	})
}

// NewCounterFunc 注册一个抓取期采样的计数器——用于「真身在别处」的单调值
//（如 runtime 的 CPU 秒数）。
func (r *Registry) NewCounterFunc(name, help string, fn func() float64) {
	g := &gaugeFunc{name: name, help: help, fn: fn}
	r.add(func(w *textWriter) {
		w.head(g.name, g.help, "counter")
		w.line(g.name, "", g.fn())
	})
}

func (r *Registry) add(render func(w *textWriter)) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.order = append(r.order, render)
}

// Handler 以 Prometheus 文本暴露格式提供注册表内容。
func (r *Registry) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		tw := &textWriter{w: w}
		r.mu.Lock()
		renders := make([]func(*textWriter), len(r.order))
		copy(renders, r.order)
		r.mu.Unlock()
		for _, render := range renders {
			render(tw)
		}
	})
}

type textWriter struct{ w http.ResponseWriter }

func (t *textWriter) head(name, help, typ string) {
	// 写失败意味着抓取方中途离开了；无事可做。
	_, _ = fmt.Fprintf(t.w, "# HELP %s %s\n# TYPE %s %s\n", name, help, name, typ)
}

func (t *textWriter) line(name, labels string, v float64) {
	if labels != "" {
		labels = "{" + labels + "}"
	}
	_, _ = fmt.Fprintf(t.w, "%s%s %s\n", name, labels, formatFloat(v))
}

func formatFloat(v float64) string {
	return strconv.FormatFloat(v, 'g', -1, 64)
}
