// Package vad implements the inline energy-based voice activity detector and
// the session/barge-in state machine (M6). VAD runs inline in the ingress
// reader to keep the audio ingress strictly single-consumer.
package vad
