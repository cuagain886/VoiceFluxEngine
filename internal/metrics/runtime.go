package metrics

import (
	"runtime"
	rtmetrics "runtime/metrics"
)

// registerRuntimeGauges 暴露负载 harness 用来把容量拐点归因到某面墙
//（CPU / 内存 / 调度）的进程资源信号。一切都在抓取期采样——绝不在帧路径上。
//
// CPU 取自 runtime/metrics：busy = total - idle，其中 total 是
// GOMAXPROCS × 墙钟秒（进程的 CPU 容量）。一个窗口内的利用率 =
// delta(busy)/delta(capacity)。这些是 runtime 的估计值，适合趋势分析，
// 不是记账值。
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
