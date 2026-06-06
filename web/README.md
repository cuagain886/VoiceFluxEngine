# web/ — browser demo client (M7)

Placeholder. The North Star L0 demo client lives here:
microphone capture (PCM 16k/mono) with `getUserMedia({echoCancellation:true})`,
WebSocket uplink, streaming downlink playback with an adaptive de-jitter buffer,
barge-in, and `ts_us`-aligned subtitles.

In production this client stands in for the WebRTC SFU / telephony gateway "edge";
the kernel behind it is transport-agnostic.
