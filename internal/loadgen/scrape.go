package loadgen

import (
	"bufio"
	"context"
	"fmt"
	"math"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"
)

// snapshot 是对一次 Prometheus 文本暴露的一次解析：按名字索引的标量样本，
// 以及按 名字 -> 上界 -> 累积计数 索引的直方图桶。
type snapshot struct {
	at      time.Time
	scalars map[string]float64
	hists   map[string]map[float64]float64 // 累积值，如暴露的那样
}

func scrape(ctx context.Context, url string) (*snapshot, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("loadgen: scrape %s: HTTP %d", url, resp.StatusCode)
	}
	snap := &snapshot{
		at:      time.Now(),
		scalars: map[string]float64{},
		hists:   map[string]map[float64]float64{},
	}
	sc := bufio.NewScanner(resp.Body)
	sc.Buffer(make([]byte, 64*1024), 1024*1024)
	for sc.Scan() {
		parseLine(snap, sc.Text())
	}
	return snap, sc.Err()
}

func parseLine(snap *snapshot, line string) {
	if line == "" || strings.HasPrefix(line, "#") {
		return
	}
	sp := strings.LastIndexByte(line, ' ')
	if sp <= 0 {
		return
	}
	key, valStr := line[:sp], line[sp+1:]
	val, err := strconv.ParseFloat(valStr, 64)
	if err != nil {
		return
	}
	if name, le, ok := bucketKey(key); ok {
		m := snap.hists[name]
		if m == nil {
			m = map[float64]float64{}
			snap.hists[name] = m
		}
		m[le] = val
		return
	}
	snap.scalars[key] = val
}

// bucketKey 识别形如 `name_bucket{le="0.05"}` 的键。
func bucketKey(key string) (name string, le float64, ok bool) {
	i := strings.Index(key, `_bucket{le="`)
	if i < 0 || !strings.HasSuffix(key, `"}`) {
		return "", 0, false
	}
	leStr := key[i+len(`_bucket{le="`) : len(key)-2]
	if leStr == "+Inf" {
		return key[:i], inf, true
	}
	le, err := strconv.ParseFloat(leStr, 64)
	if err != nil {
		return "", 0, false
	}
	return key[:i], le, true
}

var inf = math.Inf(1)

// scalarDelta 返回一个计数器在两次快照之间的增量。
func scalarDelta(s0, s1 *snapshot, name string) float64 {
	return s1.scalars[name] - s0.scalars[name]
}

// histDelta 返回「本窗口」的直方图：排好序的上界，以及窗口期间落入每个桶的
//（非累积）计数。
func histDelta(s0, s1 *snapshot, name string) (bounds []float64, counts []float64) {
	h1 := s1.hists[name]
	if h1 == nil {
		return nil, nil
	}
	h0 := s0.hists[name]
	for le := range h1 {
		bounds = append(bounds, le)
	}
	sort.Float64s(bounds)
	prev := 0.0
	for _, le := range bounds {
		cum := h1[le]
		if h0 != nil {
			cum -= h0[le]
		}
		counts = append(counts, cum-prev)
		prev = cum
	}
	return bounds, counts
}

// histQuantile 用「目标桶内线性插值」从分桶直方图估计分位数 q（0..1）
//（标准的 Prometheus histogram_quantile 方法）。直方图为空时返回 -1。结果被
// 钳制在最高有限上界，意为「至少这么多」。
func histQuantile(bounds, counts []float64, q float64) float64 {
	total := 0.0
	for _, c := range counts {
		total += c
	}
	if total <= 0 || len(bounds) == 0 {
		return -1
	}
	target := q * total
	cum := 0.0
	for i, c := range counts {
		cum += c
		if cum >= target {
			lower := 0.0
			if i > 0 {
				lower = bounds[i-1]
			}
			upper := bounds[i]
			if upper == inf {
				// 观测落在最后一个有限上界之外：就报告那个上界。
				return lower
			}
			if c <= 0 {
				return upper
			}
			return lower + (upper-lower)*(target-(cum-c))/c
		}
	}
	last := bounds[len(bounds)-1]
	if last == inf && len(bounds) > 1 {
		return bounds[len(bounds)-2]
	}
	return last
}
