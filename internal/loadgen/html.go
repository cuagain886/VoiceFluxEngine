package loadgen

import (
	"encoding/json"
	"strings"
)

// HTML renders a self-contained capacity-curve page: the run's records are
// inlined as JSON and drawn as SVG polylines by ~100 lines of vanilla JS.
// No external assets — the file works from disk, in CI artifacts, in a
// README link.
func (r *Report) HTML() (string, error) {
	data, err := json.Marshal(r)
	if err != nil {
		return "", err
	}
	return strings.Replace(capacityHTML, "__REPORT_JSON__", string(data), 1), nil
}

const capacityHTML = `<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>voicestream · 容量曲线</title>
<style>
  :root { color-scheme: dark; }
  * { box-sizing: border-box; }
  body { margin: 0; padding: 28px; background: #0d1117; color: #e6edf3;
         font-family: system-ui, "Segoe UI", "Microsoft YaHei", sans-serif; }
  h1 { font-size: 1.1rem; color: #7ee0c2; margin: 0 0 2px; }
  .sub { color: #8b949e; font-size: 0.8rem; margin-bottom: 6px; }
  .verdict { background: #161b22; border: 1px solid #30363d; border-radius: 8px;
             padding: 10px 16px; margin: 14px 0 20px; font-size: 0.9rem; }
  .verdict b { color: #f0883e; }
  .chart { background: #161b22; border: 1px solid #30363d; border-radius: 8px;
           padding: 14px 16px 8px; margin-bottom: 18px; }
  .chart h2 { font-size: 0.85rem; color: #79c0ff; margin: 0 0 8px; font-weight: 600; }
  .legend { font-size: 0.72rem; color: #8b949e; margin: 6px 0 0; }
  .legend i { display: inline-block; width: 14px; height: 3px; border-radius: 2px;
              margin: 0 5px 0 14px; vertical-align: 3px; }
  svg text { font-family: inherit; }
</style>
</head>
<body>
<h1>voicestream 容量曲线</h1>
<div class="sub" id="meta"></div>
<div class="verdict" id="verdict"></div>
<div id="charts"></div>
<script>
'use strict';
const REPORT = __REPORT_JSON__;
const R = REPORT.records;

document.getElementById('meta').textContent =
  REPORT.config + ' · ' + new Date(REPORT.startedAt).toLocaleString();
const A = REPORT.analysis;
document.getElementById('verdict').innerHTML = (A.kneeConcurrency > 0
  ? '容量拐点: <b>并发 ' + A.kneeConcurrency + '</b>（墙: ' + A.wall + '）— '
  : '本次 ramp 未达拐点 — ') + esc(A.reason);

function esc(s) {
  return (s || '').replace(/[&<>"']/g, ch => ({
    '&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'})[ch]);
}

// series: {label, color, get(rec) -> value or null}
function chart(title, unit, series, opts) {
  opts = opts || {};
  const W = 860, H = 300, L = 64, Rm = 16, T = 14, B = 40;
  const xs = R.map(r => r.concurrency);
  let ymax = opts.budget || 0;
  for (const s of series) for (const r of R) {
    const v = s.get(r); if (v != null && v >= 0 && v > ymax) ymax = v;
  }
  if (ymax <= 0) ymax = 1;
  ymax *= 1.12;
  const xpos = i => L + (W - L - Rm) * (xs.length === 1 ? 0.5 : i / (xs.length - 1));
  const ypos = v => T + (H - T - B) * (1 - v / ymax);

  let g = '';
  // y grid + ticks
  for (let k = 0; k <= 4; k++) {
    const v = ymax * k / 4, y = ypos(v);
    g += '<line x1="'+L+'" y1="'+y+'" x2="'+(W-Rm)+'" y2="'+y+'" stroke="#21262d"/>';
    g += '<text x="'+(L-8)+'" y="'+(y+4)+'" text-anchor="end" font-size="11" fill="#8b949e">'
       + (v >= 100 ? Math.round(v) : v.toFixed(v >= 10 ? 0 : 1)) + '</text>';
  }
  // x ticks at each step
  xs.forEach((x, i) => {
    g += '<text x="'+xpos(i)+'" y="'+(H-B+18)+'" text-anchor="middle" font-size="11" fill="#8b949e">'+x+'</text>';
  });
  g += '<text x="'+((L+W-Rm)/2)+'" y="'+(H-6)+'" text-anchor="middle" font-size="11" fill="#8b949e">并发会话数</text>';
  g += '<text x="14" y="'+(T+6)+'" font-size="11" fill="#8b949e">'+unit+'</text>';
  if (opts.budget) {
    const y = ypos(opts.budget);
    g += '<line x1="'+L+'" y1="'+y+'" x2="'+(W-Rm)+'" y2="'+y+'" stroke="#f85149" stroke-dasharray="5 4"/>'
       + '<text x="'+(W-Rm-4)+'" y="'+(y-5)+'" text-anchor="end" font-size="11" fill="#f85149">'
       + opts.budgetLabel + '</text>';
  }
  if (A.kneeConcurrency > 0) {
    const ki = xs.indexOf(A.kneeConcurrency);
    if (ki >= 0) {
      const x = xpos(ki);
      g += '<line x1="'+x+'" y1="'+T+'" x2="'+x+'" y2="'+(H-B)+'" stroke="#f0883e" stroke-dasharray="3 4"/>';
    }
  }
  let legend = '';
  for (const s of series) {
    const pts = [];
    R.forEach((r, i) => {
      const v = s.get(r);
      if (v != null && v >= 0) pts.push(xpos(i).toFixed(1) + ',' + ypos(v).toFixed(1));
    });
    g += '<polyline points="'+pts.join(' ')+'" fill="none" stroke="'+s.color+'" stroke-width="2"/>';
    R.forEach((r, i) => {
      const v = s.get(r);
      if (v != null && v >= 0)
        g += '<circle cx="'+xpos(i)+'" cy="'+ypos(v)+'" r="3" fill="'+s.color+'"/>';
    });
    legend += '<i style="background:'+s.color+'"></i>' + esc(s.label);
  }
  const div = document.createElement('div');
  div.className = 'chart';
  div.innerHTML = '<h2>'+esc(title)+'</h2>'
    + '<svg viewBox="0 0 '+W+' '+H+'" width="100%">'+g+'</svg>'
    + '<div class="legend">'+legend+'</div>';
  document.getElementById('charts').appendChild(div);
}

chart('首响延迟 vs 并发', 'ms', [
  { label: '客户端端到端 p50', color: '#1f6feb', get: r => r.e2eFirstP50 },
  { label: '客户端端到端 p99', color: '#79c0ff', get: r => r.e2eFirstP99 },
  { label: '服务端 first_response p99', color: '#a371f7', get: r => r.srvFirstP99 },
  { label: '服务端内核开销 p99', color: '#2ea043', get: r => r.srvKernelP99 },
]);

chart('打断时延 vs 并发', 'ms', [
  { label: '客户端端到端打断 p99（含 VAD 滤波窗）', color: '#79c0ff', get: r => r.e2eBargeP99 },
  { label: '内核取消 p99（200ms 预算口径）', color: '#f0883e', get: r => r.srvBargeP99 },
], { budget: 200, budgetLabel: '200ms 预算' });

chart('资源与降级 vs 并发', '%', [
  { label: 'CPU 利用率', color: '#a371f7', get: r => r.cpuUtil >= 0 ? r.cpuUtil * 100 : null },
  { label: '入口丢帧率', color: '#f85149', get: r => r.ingressDropRate >= 0 ? r.ingressDropRate * 100 : null },
]);

chart('吞吐 vs 并发', '每秒', [
  { label: '轮速率（轮/s）', color: '#2ea043', get: r => r.serverTurnRate },
  { label: '上行帧率 ÷ 100（帧/s）', color: '#6e7681', get: r => r.uplinkFPS >= 0 ? r.uplinkFPS / 100 : null },
]);
</script>
</body>
</html>
`
