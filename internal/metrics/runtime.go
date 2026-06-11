package metrics

import (
	"runtime"
	rtmetrics "runtime/metrics"
)

// registerRuntimeGauges exposes the process resource signals the load
// harness uses to attribute the capacity knee to a wall (CPU / memory /
// scheduling). Everything is sampled at scrape time — never on a frame path.
//
// CPU comes from runtime/metrics: busy = total - idle, where total is
// GOMAXPROCS x wall seconds (the process's CPU capacity). Utilization over a
// window is delta(busy)/delta(capacity). These are runtime estimates, good
// for trend analysis, not accounting.
func registerRuntimeGauges(r *Registry) {
	r.NewGaugeFunc("voicestream_goroutines", "Current goroutine count.",
		func() float64 { return float64(runtime.NumGoroutine()) })
	r.NewGaugeFunc("voicestream_heap_alloc_bytes", "Bytes of allocated heap objects.",
		func() float64 {
			var ms runtime.MemStats
			runtime.ReadMemStats(&ms)
			return float64(ms.HeapAlloc)
		})
	r.NewCounterFunc("voicestream_cpu_busy_seconds_total",
		"Estimated CPU seconds spent non-idle (runtime/metrics: total - idle).",
		func() float64 { busy, _ := cpuSeconds(); return busy })
	r.NewCounterFunc("voicestream_cpu_capacity_seconds_total",
		"CPU seconds available to the process (GOMAXPROCS x wall).",
		func() float64 { _, capacity := cpuSeconds(); return capacity })
}

func cpuSeconds() (busy, capacity float64) {
	samples := []rtmetrics.Sample{
		{Name: "/cpu/classes/total:cpu-seconds"},
		{Name: "/cpu/classes/idle:cpu-seconds"},
	}
	rtmetrics.Read(samples)
	if samples[0].Value.Kind() != rtmetrics.KindFloat64 || samples[1].Value.Kind() != rtmetrics.KindFloat64 {
		return 0, 0
	}
	total := samples[0].Value.Float64()
	idle := samples[1].Value.Float64()
	return total - idle, total
}
