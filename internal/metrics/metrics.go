// Package metrics provides the kernel's latency/drop instrumentation (M9):
// a dependency-free Prometheus-text-format registry for scraping, and an SSE
// hub feeding the live per-turn waterfall dashboard. Metrics are backdrop,
// not kernel (they must never touch the hot path's allocation or blocking
// behaviour): everything here is atomic counters and non-blocking fan-out.
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

// Counter is a monotonically increasing value.
type Counter struct {
	name, help string
	v          atomic.Uint64
}

// Add increments the counter.
func (c *Counter) Add(n uint64) { c.v.Add(n) }

// Value returns the current count.
func (c *Counter) Value() uint64 { return c.v.Load() }

// Histogram is a fixed-bucket latency histogram (Prometheus semantics:
// cumulative buckets, +Inf implicit, sum and count).
type Histogram struct {
	name, help string
	bounds     []float64 // upper bounds, seconds, ascending
	counts     []atomic.Uint64
	inf        atomic.Uint64
	sumBits    atomic.Uint64 // float64 bits, CAS-add
	count      atomic.Uint64
}

// Observe records one value in seconds.
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

// Count returns the number of observations.
func (h *Histogram) Count() uint64 { return h.count.Load() }

// GaugeFunc is sampled at scrape time — used for values whose source of
// truth lives elsewhere (live session counts, ring drop counters).
type gaugeFunc struct {
	name, help string
	fn         func() float64
}

// Registry holds instruments and renders the Prometheus text exposition.
type Registry struct {
	mu     sync.Mutex
	order  []func(w *textWriter)
}

// NewRegistry returns an empty registry.
func NewRegistry() *Registry { return &Registry{} }

// NewCounter registers and returns a counter.
func (r *Registry) NewCounter(name, help string) *Counter {
	c := &Counter{name: name, help: help}
	r.add(func(w *textWriter) {
		w.head(c.name, c.help, "counter")
		w.line(c.name, "", float64(c.v.Load()))
	})
	return c
}

// NewHistogram registers and returns a histogram with the given upper
// bounds (seconds, ascending).
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

// NewGaugeFunc registers a scrape-time sampled gauge.
func (r *Registry) NewGaugeFunc(name, help string, fn func() float64) {
	g := &gaugeFunc{name: name, help: help, fn: fn}
	r.add(func(w *textWriter) {
		w.head(g.name, g.help, "gauge")
		w.line(g.name, "", g.fn())
	})
}

// NewCounterFunc registers a scrape-time sampled counter — for monotonic
// values whose source of truth lives elsewhere (e.g. runtime CPU seconds).
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

// Handler serves the registry in Prometheus text exposition format.
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
	// A failed write means the scraper went away mid-response; nothing to do.
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
