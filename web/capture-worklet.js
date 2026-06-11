// 采集 worklet：跑在音频渲染线程，把每个 128 采样块原样转发给主线程。
// AudioContext 以 16kHz 创建，浏览器负责把麦克风流重采样到该频率，
// 因此这里拿到的就已经是 16k/mono 的 Float32 数据。
class CaptureProcessor extends AudioWorkletProcessor {
  process(inputs) {
    const ch = inputs[0] && inputs[0][0];
    if (ch && ch.length > 0) {
      // 拷贝一份再 post：引擎会复用底层缓冲。
      this.port.postMessage(ch.slice(0));
    }
    return true;
  }
}
registerProcessor('capture', CaptureProcessor);
