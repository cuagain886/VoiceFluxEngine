package sherpa

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/coder/websocket"

	"voicestream/internal/adapter"
)

const (
	asrReadLimit  = 1 << 20 // 1 MiB：ASR 结果是小 JSON，留足余量即可
	silenceStepMs = 100     // 收尾补静音的分块大小：每块 100ms 全零
)

// ASR 是 sherpa-onnx 流式 ASR sidecar 的 WebSocket 客户端。
//
// silenceTailMs / finalWait 由配置注入（见 SherpaConfig）：voiceapi 的
// OnlineRecognizer 只在「音频里出现足够长的尾静音」时才吐 finished=true（其
// rule2_min_trailing_silence≈1.2s），而 VAD 在 speech_end（300ms 尾挂）就切断了
// 上行——sidecar 因此永远等不到尾静音、只吐 partial。于是 in 关闭后主动补一段
// silenceTailMs 的静音逼出端点定稿，再等 finalWait 拿它的 finished=true。
type ASR struct {
	url           string
	sampleRate    int
	silenceTailMs int           // 收尾补发的尾静音时长：要大于 sidecar 的 rule2（≈1.2s）
	finalWait     time.Duration // 补完静音后等 sidecar 端点 final 的上限
}

// asrResult 是 sidecar 回传的转写 JSON。finished=true 表示这是 sidecar 端点
// 检测触发的「整段定稿」，是我们最想要的 final；finished=false 是中途 partial。
type asrResult struct {
	Text     string `json:"text"`
	Finished bool   `json:"finished"`
}

// Stream 实现 adapter.ASR：把上行 PCM 帧流给 sidecar，同时把回传的 partial
// 产出；当调用方关闭 in（一句话结束）时，补一段尾静音逼 sidecar 定稿，以它的
// finished=true 文本（拿不到则回落到最后一条 partial）作为唯一 final 落地后返回。
// ctx 取消会即时中止读写——这正是打断所需要的。
func (a *ASR) Stream(ctx context.Context, in <-chan adapter.AudioFrame, out chan<- adapter.Transcript) error {
	c, err := dial(ctx, a.url, map[string]string{"samplerate": strconv.Itoa(a.sampleRate)}, asrReadLimit)
	if err != nil {
		return err
	}
	defer func() { _ = c.CloseNow() }()

	// 协议：连接后直接流式发送二进制 PCM，无 JSON 握手（服务端首个动作就是
	// receive_bytes）。注意：发空二进制帧会让 sidecar 的 task_recv_pcm 直接退出、
	// 不再读音频，所以收尾「补静音」必须在发空帧之前，本实现干脆不发空帧。

	var (
		lastTs   atomic.Int64 // 写侧更新、读侧读取，故用原子
		lastText string       // 仅读侧写；读侧停妥后主协程才读，靠 channel 同步
	)
	readCtx, cancelRead := context.WithCancel(ctx)
	defer cancelRead()
	readDone := make(chan error, 1)
	finalCh := make(chan string, 1) // 读侧拿到 finished=true 时把整段文本投来

	// 读侧：partial 转发为 adapter.Transcript（live 字幕）；finished=true 的整段
	// 文本走 finalCh 交给主协程当 final。
	go func() {
		readDone <- func() error {
			for {
				typ, data, err := c.Read(readCtx)
				if err != nil {
					return err
				}
				if typ != websocket.MessageText {
					continue
				}
				var r asrResult
				if json.Unmarshal(data, &r) != nil || r.Text == "" {
					continue
				}
				if r.Finished {
					select {
					case finalCh <- r.Text:
					default:
					}
					continue
				}
				lastText = r.Text
				if err := send(readCtx, out, adapter.Transcript{Text: r.Text, TsUs: lastTs.Load()}); err != nil {
					return err
				}
			}
		}()
	}()

	// 写循环：上行 PCM，直到 in 关闭（端点）或被取消，或读侧先退。
	var readerDone bool
writeLoop:
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case rerr := <-readDone:
			readerDone = true
			// 读侧先退：若是真错误（非取消）且尚无任何文本，则上报失败。
			if e := wsErr(ctx, rerr); e != nil && lastText == "" {
				return fmt.Errorf("sherpa asr: read: %w", e)
			}
			break writeLoop
		case f, ok := <-in:
			if !ok {
				break writeLoop // 句末：到下方补静音逼定稿
			}
			lastTs.Store(f.TsUs)
			if err := c.Write(ctx, websocket.MessageBinary, f.PCM); err != nil {
				return fmt.Errorf("sherpa asr: write pcm: %w", wsErr(ctx, err))
			}
		}
	}

	// in 关闭：补一段尾静音触发 sidecar 端点检测，再等它吐 finished=true。
	finalText := ""
	if !readerDone {
		if err := a.feedSilence(ctx, c); err != nil {
			return fmt.Errorf("sherpa asr: feed silence: %w", wsErr(ctx, err))
		}
		deadline := time.NewTimer(a.finalWait)
		defer deadline.Stop()
	wait:
		for {
			select {
			case t := <-finalCh: // sidecar 端点触发的真正 final
				finalText = t
				break wait
			case <-deadline.C: // 兜底：用最后一条 partial
				break wait
			case <-readDone: // 连接已关，读侧退出
				readerDone = true
				break wait
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}
	cancelRead()
	if !readerDone {
		<-readDone // 等读侧彻底退出（其错误即取消，不作为失败）
	}

	// 收尾后 finalCh 里可能刚好躺着一条还没取的 final，优先用它。
	if finalText == "" {
		select {
		case t := <-finalCh:
			finalText = t
		default:
		}
	}
	text := finalText
	if text == "" {
		text = lastText // sidecar 没来得及定稿：回落到最后一条 partial
	}
	return send(ctx, out, adapter.Transcript{Text: text, Final: true, TsUs: lastTs.Load()})
}

// feedSilence 向 sidecar 补发 silenceTailMs 的全零 PCM，分块发出。sherpa 的端点
// 检测按「尾静音采样时长」判定（非墙钟），所以这段静音一旦被解码够 rule2 的阈值
// 就会触发 finished=true；静音解码很快，墙钟开销远小于 silenceTailMs。
func (a *ASR) feedSilence(ctx context.Context, c *websocket.Conn) error {
	step := make([]byte, a.sampleRate/1000*silenceStepMs*2) // silenceStepMs 全零，16-bit 单声道
	for sent := 0; sent < a.silenceTailMs; sent += silenceStepMs {
		if err := c.Write(ctx, websocket.MessageBinary, step); err != nil {
			return err
		}
	}
	return nil
}
