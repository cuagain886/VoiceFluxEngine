// voicestream 浏览器演示客户端（M7，北极星 L0）。
// 职责：16k/16-bit/mono 采音上行（AEC 开启）、流式播放下行音频（自适应
// 去抖缓冲）、barge-in 即停、按 ts_us 对齐的字幕。
'use strict';

// ---------- 帧协议（与 internal/transport/frame.go 完全一致） ----------

const MAGIC0 = 0x56, MAGIC1 = 0x53, VERSION = 1, HEADER_SIZE = 24;
const FRAME_AUDIO = 1, FRAME_TEXT = 2, FRAME_CONTROL = 3;
const SRC_TRANSCRIPT = 1, SRC_TOKEN = 2;
const CTRL_START = 1, CTRL_STOP = 2, CTRL_BARGE_IN = 3, CTRL_ERROR = 4;

const SAMPLE_RATE = 16000;
const FRAME_SAMPLES = 320; // 20ms @ 16kHz
const US_PER_SAMPLE = 1e6 / SAMPLE_RATE;

function encodeFrame(type, seq, tsUs, payload) {
  const buf = new ArrayBuffer(HEADER_SIZE + payload.byteLength);
  const dv = new DataView(buf);
  dv.setUint8(0, MAGIC0);
  dv.setUint8(1, MAGIC1);
  dv.setUint8(2, VERSION);
  dv.setUint8(3, type);
  dv.setBigUint64(4, BigInt(seq));
  dv.setBigInt64(12, BigInt(Math.round(tsUs)));
  dv.setUint32(20, payload.byteLength);
  new Uint8Array(buf, HEADER_SIZE).set(payload);
  return buf;
}

function decodeFrame(buf) {
  const dv = new DataView(buf);
  if (buf.byteLength < HEADER_SIZE || dv.getUint8(0) !== MAGIC0 ||
      dv.getUint8(1) !== MAGIC1 || dv.getUint8(2) !== VERSION) {
    return null;
  }
  return {
    type: dv.getUint8(3),
    seq: dv.getBigUint64(4),
    tsUs: Number(dv.getBigInt64(12)),
    payload: new Uint8Array(buf, HEADER_SIZE),
  };
}

// ---------- 最小 protobuf 解码（仅覆盖 TextPayload / ControlPayload） ----------

function readVarint(u8, pos) {
  let v = 0, shift = 0;
  for (;;) {
    const b = u8[pos++];
    v |= (b & 0x7f) << shift;
    if ((b & 0x80) === 0) return [v >>> 0, pos];
    shift += 7;
  }
}

// TextPayload { 1: string text, 2: bool final, 3: enum source }
function decodeTextPayload(u8) {
  const out = { text: '', final: false, source: 0 };
  let pos = 0;
  const td = new TextDecoder();
  while (pos < u8.length) {
    let tag; [tag, pos] = readVarint(u8, pos);
    const field = tag >>> 3, wire = tag & 7;
    if (wire === 2) {
      let len; [len, pos] = readVarint(u8, pos);
      if (field === 1) out.text = td.decode(u8.subarray(pos, pos + len));
      pos += len;
    } else if (wire === 0) {
      let v; [v, pos] = readVarint(u8, pos);
      if (field === 2) out.final = v !== 0;
      if (field === 3) out.source = v;
    } else {
      break; // v1 协议中不会出现其他 wire type
    }
  }
  return out;
}

// ControlPayload { 1: enum kind, 2: string detail, 3: string session_id,
//                  4: uint64 epoch, 5: uint64 last_seq }
function decodeControlPayload(u8) {
  const out = { kind: 0, detail: '', sessionId: '', epoch: 0, lastSeq: 0 };
  let pos = 0;
  const td = new TextDecoder();
  while (pos < u8.length) {
    let tag; [tag, pos] = readVarint(u8, pos);
    const field = tag >>> 3, wire = tag & 7;
    if (wire === 0) {
      let v; [v, pos] = readVarint(u8, pos);
      if (field === 1) out.kind = v;
      if (field === 4) out.epoch = v;
      if (field === 5) out.lastSeq = v;
    } else if (wire === 2) {
      let len; [len, pos] = readVarint(u8, pos);
      const str = td.decode(u8.subarray(pos, pos + len));
      if (field === 2) out.detail = str;
      if (field === 3) out.sessionId = str;
      pos += len;
    } else {
      break;
    }
  }
  return out;
}

function writeVarint(bytes, v) {
  while (v > 0x7f) { bytes.push((v & 0x7f) | 0x80); v >>>= 7; }
  bytes.push(v);
}

// 编码 ControlPayload（握手 START / 收尾 STOP 用）。
function encodeControlPayload(kind, sessionId, epoch, lastSeq) {
  const bytes = [];
  bytes.push(0x08); writeVarint(bytes, kind); // field 1 varint
  if (sessionId) {
    const idb = new TextEncoder().encode(sessionId);
    bytes.push(0x1a); writeVarint(bytes, idb.length); // field 3 len-delim
    for (const b of idb) bytes.push(b);
  }
  if (epoch > 0) { bytes.push(0x20); writeVarint(bytes, epoch); }   // field 4
  if (lastSeq > 0) { bytes.push(0x28); writeVarint(bytes, lastSeq); } // field 5
  return new Uint8Array(bytes);
}

// ---------- 自适应播放/去抖缓冲（7.3 / 7.3b） ----------
//
// TCP 上抖动表现为「突发到达」：帧成簇涌入而非匀速。本缓冲先预存
// targetDepth 时长的音频再开播，之后按 PTS 节奏经 WebAudio 精确排期；
// 欠载时不硬放（不会爆音），转入静音并重新预存，同时加深 targetDepth
//（抖动大→更深；深度有上限，平滑与延迟之间取舍）。

class PlayoutBuffer {
  constructor(ctx, onStateChange) {
    this.ctx = ctx;
    this.onStateChange = onStateChange;
    this.queue = [];          // {tsUs, samples: Float32Array}
    this.active = new Set();  // 已排期未播完的 source 节点
    this.playing = false;
    this.nextPlayTime = 0;    // ctx 时钟上的下一个排期点
    this.targetDepthMs = 120; // 初始预存深度
    this.maxDepthMs = 400;
    this.underruns = 0;
    this.playedBaseUs = 0;    // 已确定播完/冲掉的会话时钟位置
    this.scheduled = [];      // {endsAtCtx, endTsUs} 用于换算播放进度
  }

  queuedMs() {
    return this.queue.reduce((ms, f) => ms + f.samples.length / SAMPLE_RATE * 1000, 0);
  }

  push(tsUs, samples) {
    this.queue.push({ tsUs, samples });
    this.pump();
  }

  pump() {
    const now = this.ctx.currentTime;
    if (!this.playing) {
      if (this.queuedMs() < this.targetDepthMs) return; // 预存中
      this.playing = true;
      this.nextPlayTime = now + 0.02;
      this.onStateChange && this.onStateChange('playing');
    }
    // 欠载检测：到了该播的时间却没有后续帧可排。
    if (this.queue.length === 0) {
      if (this.nextPlayTime <= now) {
        this.playing = false;
        this.underruns++;
        this.targetDepthMs = Math.min(this.targetDepthMs + 40, this.maxDepthMs);
        this.onStateChange && this.onStateChange('buffering');
      }
      return;
    }
    while (this.queue.length > 0) {
      const f = this.queue.shift();
      const dur = f.samples.length / SAMPLE_RATE;
      const buf = this.ctx.createBuffer(1, f.samples.length, SAMPLE_RATE);
      buf.copyToChannel(f.samples, 0);
      const src = this.ctx.createBufferSource();
      src.buffer = buf;
      src.connect(this.ctx.destination);
      const at = Math.max(this.nextPlayTime, now + 0.005);
      src.start(at);
      this.nextPlayTime = at + dur;
      this.active.add(src);
      const endsAtCtx = this.nextPlayTime;
      const endTsUs = f.tsUs + f.samples.length * US_PER_SAMPLE;
      this.scheduled.push({ endsAtCtx, endTsUs });
      src.onended = () => {
        this.active.delete(src);
        this.pump(); // 续排 + 欠载检测
      };
    }
    // 只保留还有意义的进度锚点
    while (this.scheduled.length > 1 && this.scheduled[0].endsAtCtx < now) {
      this.playedBaseUs = this.scheduled.shift().endTsUs;
    }
  }

  // 当前播放进度对应的会话时钟（µs），驱动字幕揭示。
  playedUs() {
    const now = this.ctx.currentTime;
    let played = this.playedBaseUs;
    for (const s of this.scheduled) {
      if (s.endsAtCtx <= now) played = s.endTsUs;
    }
    return played;
  }

  // barge-in：立即停掉一切已排期音频并清空队列（7.4）。
  flush() {
    for (const src of this.active) {
      try { src.onended = null; src.stop(); } catch (e) { /* already stopped */ }
    }
    this.active.clear();
    // 冲掉即视为「播到了最后收到的位置」，会话时钟不回退。
    let base = this.playedBaseUs;
    for (const s of this.scheduled) base = Math.max(base, s.endTsUs);
    for (const f of this.queue) base = Math.max(base, f.tsUs + f.samples.length * US_PER_SAMPLE);
    this.playedBaseUs = base;
    this.queue = [];
    this.scheduled = [];
    this.playing = false;
    this.nextPlayTime = 0;
    this.onStateChange && this.onStateChange('flushed');
  }
}

// ---------- 主流程 ----------

const ui = {
  btn: document.getElementById('btn'),
  status: document.getElementById('status'),
  user: document.getElementById('user-line'),
  agent: document.getElementById('agent-line'),
  stats: document.getElementById('stats'),
};

let running = false;
let ws, audioCtx, mediaStream, workletNode;
let playout;
let uplinkSeq = 0, samplesSent = 0;
let pendingTokens = []; // {tsUs, text} 等待播放进度揭示的字幕
let captureBuf = new Float32Array(0);
// 会话恢复状态（M8）：断线重连时凭 sessionId + 递增 epoch 接管同一会话。
let sessionId = '', epoch = 0, lastDownSeq = 0;
let reconnectTries = 0;
const MAX_RECONNECT = 8;

function setStatus(text) { ui.status.textContent = text; }

function refreshStats() {
  if (!playout) return;
  ui.stats.textContent =
    `缓冲深度 ${playout.targetDepthMs}ms · 欠载 ${playout.underruns} 次 · 待显示 token ${pendingTokens.length}`;
}

async function start() {
  // AEC 是防自打断的第一道闸（D6）：必须由浏览器在采集端消回声。
  mediaStream = await navigator.mediaDevices.getUserMedia({
    audio: {
      echoCancellation: true,
      noiseSuppression: true,
      autoGainControl: true,
      channelCount: 1,
    },
  });

  // 16kHz 上下文：采集与播放同钟，浏览器内部完成重采样（7.1）。
  audioCtx = new AudioContext({ sampleRate: SAMPLE_RATE });
  await audioCtx.audioWorklet.addModule('capture-worklet.js');

  playout = new PlayoutBuffer(audioCtx, () => refreshStats());

  await connectWS();

  const source = audioCtx.createMediaStreamSource(mediaStream);
  workletNode = new AudioWorkletNode(audioCtx, 'capture');
  workletNode.port.onmessage = (ev) => onCapture(ev.data);
  source.connect(workletNode);
  // 不连 destination：上行音频不需要本地监听。

  running = true;
  ui.btn.textContent = '结束对话';
  setStatus('请说话…');
  setInterval(revealTokens, 50);
}

// 建立 WS 并完成 START 握手；重连时复用 sessionId、epoch+1（M8）。
async function connectWS() {
  ws = new WebSocket(`${location.protocol === 'https:' ? 'wss' : 'ws'}://${location.host}/ws`);
  ws.binaryType = 'arraybuffer';
  ws.onmessage = (ev) => onFrame(ev.data);
  ws.onclose = () => { if (running) scheduleReconnect(); };

  await new Promise((res, rej) => { ws.onopen = res; ws.onerror = rej; });
  // 握手：新会话 sessionId 为空、epoch 1；重连带上次 id 与递增 epoch。
  ws.send(encodeFrame(FRAME_CONTROL, ++uplinkSeq, 0,
    encodeControlPayload(CTRL_START, sessionId, epoch + 1, lastDownSeq)));
}

function scheduleReconnect() {
  if (reconnectTries >= MAX_RECONNECT) { stop('重连失败，会话结束'); return; }
  reconnectTries++;
  setStatus(`连接中断，第 ${reconnectTries} 次重连…`);
  setTimeout(() => {
    if (!running) return;
    connectWS().catch(() => scheduleReconnect());
  }, 600);
}

// 把 worklet 的 128 采样块攒成 320 采样（20ms）帧，转 Int16 LE 上行（7.1/7.2）。
function onCapture(block) {
  if (!running || ws.readyState !== WebSocket.OPEN) return;
  const merged = new Float32Array(captureBuf.length + block.length);
  merged.set(captureBuf); merged.set(block, captureBuf.length);
  captureBuf = merged;

  while (captureBuf.length >= FRAME_SAMPLES) {
    const chunk = captureBuf.subarray(0, FRAME_SAMPLES);
    captureBuf = captureBuf.slice(FRAME_SAMPLES);
    const pcm = new Uint8Array(FRAME_SAMPLES * 2);
    const dv = new DataView(pcm.buffer);
    for (let i = 0; i < FRAME_SAMPLES; i++) {
      const s = Math.max(-1, Math.min(1, chunk[i]));
      dv.setInt16(2 * i, s < 0 ? s * 0x8000 : s * 0x7fff, true); // LE
    }
    // 上行 PTS 用采样时钟（D7），不用 wall-clock。
    const tsUs = samplesSent * US_PER_SAMPLE;
    samplesSent += FRAME_SAMPLES;
    ws.send(encodeFrame(FRAME_AUDIO, ++uplinkSeq, tsUs, pcm));
  }
}

function onFrame(buf) {
  const f = decodeFrame(buf);
  if (!f) return;
  if (f.seq > 0n) lastDownSeq = Number(f.seq);
  switch (f.type) {
    case FRAME_AUDIO: {
      const n = f.payload.byteLength / 2;
      const dv = new DataView(f.payload.buffer, f.payload.byteOffset, f.payload.byteLength);
      const samples = new Float32Array(n);
      for (let i = 0; i < n; i++) samples[i] = dv.getInt16(2 * i, true) / 0x8000;
      playout.push(f.tsUs, samples);
      setStatus('Agent 正在回应…（开口即可打断）');
      break;
    }
    case FRAME_TEXT: {
      const t = decodeTextPayload(f.payload);
      if (t.source === SRC_TRANSCRIPT) {
        ui.user.textContent = t.text;
        ui.user.classList.toggle('final', t.final);
        if (t.final) { // 新一问开始：清空上一轮 Agent 字幕
          ui.agent.textContent = '';
          pendingTokens = [];
          setStatus('思考中…');
        }
      } else if (t.source === SRC_TOKEN) {
        // 字幕对齐（7.5）：按 ts_us 等播放进度到了再显示。
        pendingTokens.push({ tsUs: f.tsUs, text: t.text });
      }
      break;
    }
    case FRAME_CONTROL: {
      const c = decodeControlPayload(f.payload);
      if (c.kind === CTRL_START) {
        // 握手回执：记下会话身份，重连计数清零（M8）。
        sessionId = c.sessionId;
        epoch = c.epoch;
        reconnectTries = 0;
        setStatus(c.detail === 'resumed' ? '已恢复会话，请继续…' : '请说话…');
      } else if (c.kind === CTRL_BARGE_IN) {
        // 子链已取消：立刻停播、丢弃未揭示字幕（7.4）。
        playout.flush();
        pendingTokens = [];
        setStatus('你打断了 Agent，请继续说…');
      } else if (c.kind === CTRL_ERROR) {
        stop(`服务端错误：${c.detail}`);
      }
      break;
    }
  }
  refreshStats();
}

function revealTokens() {
  if (!playout || pendingTokens.length === 0) return;
  const played = playout.playedUs();
  while (pendingTokens.length > 0 && pendingTokens[0].tsUs <= played) {
    ui.agent.textContent += pendingTokens.shift().text;
  }
  refreshStats();
}

function stop(reason) {
  running = false;
  try {
    ws && ws.send(encodeFrame(FRAME_CONTROL, ++uplinkSeq, 0,
      encodeControlPayload(CTRL_STOP, '', 0, 0)));
  } catch (e) { /* ignore */ }
  try { ws && ws.close(); } catch (e) { /* ignore */ }
  try { workletNode && workletNode.disconnect(); } catch (e) { /* ignore */ }
  try { mediaStream && mediaStream.getTracks().forEach((t) => t.stop()); } catch (e) { /* ignore */ }
  try { audioCtx && audioCtx.close(); } catch (e) { /* ignore */ }
  sessionId = ''; epoch = 0; lastDownSeq = 0; reconnectTries = 0;
  ui.btn.textContent = '开始对话';
  setStatus(reason || '已结束');
}

ui.btn.addEventListener('click', () => {
  if (running) { stop(); return; }
  start().catch((err) => setStatus(`启动失败：${err.message}`));
});
