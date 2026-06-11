// voicestream 延迟仪表盘（M9）。
// 订阅 /debug/turns 的 SSE 逐轮记录，渲染瀑布图：三条阶段时间条天然重叠，
// 灰色「朴素串行」对照条把流水线省下的时间画在眼前。
'use strict';

const turnsEl = document.getElementById('turns');
const summaryEl = document.getElementById('summary');
const MAX_ROWS = 30;
const records = [];

const es = new EventSource('/debug/turns');
es.onmessage = (ev) => {
  let rec;
  try { rec = JSON.parse(ev.data); } catch (e) { return; }
  records.push(rec);
  if (records.length > MAX_ROWS) records.shift();
  render();
};
es.onerror = () => {
  // EventSource 自动重连；这里只提示。
  setSummaryNote('数据流断开，重连中…');
};

function fmt(ms) {
  if (ms < 0) return '—';
  return ms >= 1000 ? (ms / 1000).toFixed(2) + 's' : Math.round(ms) + 'ms';
}

function setSummaryNote(text) {
  summaryEl.innerHTML = `<div class="card"><div class="v">…</div><div class="k">${text}</div></div>`;
}

function render() {
  renderSummary();
  renderTurns();
}

function renderSummary() {
  const done = records.filter((r) => !r.cancelled && r.firstResponseMs > 0);
  const cancelled = records.filter((r) => r.cancelled);
  const avg = (arr, f) => arr.length ? arr.reduce((s, r) => s + f(r), 0) / arr.length : -1;

  const cards = [
    { v: records.length, k: '总轮数（最近）' },
    { v: fmt(avg(done, (r) => r.firstResponseMs)), k: '平均首响' },
    { v: fmt(avg(done, (r) => r.kernelMs)), k: '平均内核开销' },
    { v: cancelled.length, k: '被打断轮数' },
    { v: fmt(avg(cancelled.filter((r) => r.bargeInMs > 0), (r) => r.bargeInMs)), k: '平均打断取消耗时' },
  ];
  summaryEl.innerHTML = cards.map((c) =>
    `<div class="card"><div class="v">${c.v}</div><div class="k">${c.k}</div></div>`).join('');
}

function bar(cls, fromMs, toMs, scaleMs) {
  if (fromMs < 0 || toMs < 0 || toMs <= fromMs) return '';
  const l = (fromMs / scaleMs * 100).toFixed(2);
  const w = ((toMs - fromMs) / scaleMs * 100).toFixed(2);
  return `<div class="bar ${cls}" style="left:${l}%;width:${w}%"></div>`;
}

function marker(ms, scaleMs) {
  if (ms < 0) return '';
  return `<div class="marker" style="left:${(ms / scaleMs * 100).toFixed(2)}%"></div>`;
}

function lane(tag, inner) {
  return `<div class="lane"><div class="tag">${tag}</div><div class="track">${inner}</div></div>`;
}

function renderTurns() {
  const rows = records.slice().reverse().map((r) => {
    // 时间轴量程取实际墙钟与串行总和的较大者，两根条都画得下。
    const scale = Math.max(r.wallMs, r.serialMs || 0, 1);
    const saved = (r.serialMs > 0 && r.wallMs > 0)
      ? Math.round((1 - r.wallMs / r.serialMs) * 100) : null;

    const lanes =
      lane('ASR', bar('asr', 0, r.asrFinalMs, scale)) +
      lane('LLM', bar('llm', r.llmStartMs, r.llmLastTokenMs, scale)) +
      lane('TTS', bar('tts', r.ttsStartMs, r.ttsLastFrameMs, scale) + marker(r.ttsFirstFrameMs, scale)) +
      lane('实际', bar('wall', 0, r.wallMs, scale)) +
      lane('串行', r.serialMs > 0 ? bar('serial', 0, r.serialMs, scale) : '');

    const labels = [
      `<span>首响 <b>${fmt(r.firstResponseMs)}</b></span>`,
      `<span>内核开销 <b>${fmt(r.kernelMs)}</b></span>`,
      r.serialMs > 0 && saved !== null && !r.cancelled
        ? `<span class="save">流水线省时 ${saved}%（串行需 ${fmt(r.serialMs)}）</span>` : '',
      r.cancelled
        ? `<span class="cancelnote">被打断${r.bargeInMs > 0 ? `，取消耗时 ${fmt(r.bargeInMs)}` : ''}</span>` : '',
    ].join('');

    return `<div class="turn${r.cancelled ? ' cancelled' : ''}">
      <div class="meta">
        <span>${r.time}</span><span>会话 <b>${r.session}</b></span>
        <span>你：<b>${escapeHTML(r.prompt)}</b></span>
        <span>Agent：${escapeHTML(r.reply)}</span>
      </div>
      ${lanes}
      <div class="labels">${labels}</div>
    </div>`;
  });
  turnsEl.innerHTML = rows.join('') || '<div id="empty">等待第一轮对话…</div>';
}

function escapeHTML(s) {
  return (s || '').replace(/[&<>"']/g, (ch) => ({
    '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;',
  })[ch]);
}
