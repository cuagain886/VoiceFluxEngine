// Package loadgen is the load harness: it ramps concurrent sessions driving the
// real hot path with mock models, injects latency/jitter/burst, and produces
// the load/latency curve used to find the capacity knee (M10).
package loadgen
