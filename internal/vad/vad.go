package vad

import (
	"encoding/binary"
	"math"
)

// Event 是检测器与会话状态机之间的「共同语言」。SpeechStart 和 SpeechEnd
// 来自检测器；ResponseStarted 和 ResponseDone 来自流水线的轮生命周期。
type Event uint8

const (
	None Event = iota
	SpeechStart
	SpeechEnd
	ResponseStarted
	ResponseDone
)

// String 实现 fmt.Stringer。
func (e Event) String() string {
	switch e {
	case None:
		return "none"
	case SpeechStart:
		return "speech_start"
	case SpeechEnd:
		return "speech_end"
	case ResponseStarted:
		return "response_started"
	case ResponseDone:
		return "response_done"
	default:
		return "event(?)"
	}
}

// Detector 是可插拔的「逐帧语音活动」接口。Process 在音频入口 goroutine 里
// 被内联调用——所以实现必须零分配且快（将来换成 WebRTC 或 ML VAD 是一次
// drop-in 替换）。它在语句边界返回 SpeechStart/SpeechEnd，其余时刻返回 None。
type Detector interface {
	Process(pcm []byte) Event
}

// Energy 是 v1 检测器：RMS 能量 + 三道防误触发滤波。
//
//	双门限    高门限（enter）才开始算说话，跌破低门限（exit）才算退出——
//	          滞回，使能量在某一条线附近徘徊时不会反复抖出 start/end 事件
//	最小语音  能量必须连续 N 帧高于 enter，speech_start 才触发——咔哒声、
//	          爆破音不算数
//	尾挂      能量必须连续 M 帧低于 exit，speech_end 才触发——句子中间的
//	          自然停顿不会把一句话切碎
//
// （第四道抑制层「客户端 AEC」按规格住在浏览器里——它在 Agent 自己的回放
// 到达我们之前就把它消掉了。）
type Energy struct {
	Enter           float64 // 够格算作说话的 RMS（归一化 0..1）
	Exit            float64 // 低于此值则语音衰减；Exit <= Enter
	MinSpeechFrames int     // speech_start 前需连续多少帧「响」
	HangoverFrames  int     // speech_end 前需连续多少帧「静」

	inSpeech bool
	run      int // 「相反条件」已连续多少帧
}

// Process 对一帧 PCM（16 位小端单声道）做分类。
func (e *Energy) Process(pcm []byte) Event {
	level := rms(pcm)
	if !e.inSpeech {
		if level >= e.Enter {
			e.run++
			if e.run >= e.MinSpeechFrames {
				e.inSpeech = true
				e.run = 0
				return SpeechStart
			}
		} else {
			e.run = 0
		}
		return None
	}
	if level < e.Exit {
		e.run++
		if e.run >= e.HangoverFrames {
			e.inSpeech = false
			e.run = 0
			return SpeechEnd
		}
	} else {
		e.run = 0
	}
	return None
}

// rms 计算一帧 16 位小端 PCM 的归一化均方根能量。一帧 20ms 约 320 个采样点：
// 亚微秒级，放在内联热路径里毫无压力。
func rms(pcm []byte) float64 {
	n := len(pcm) / 2
	if n == 0 {
		return 0
	}
	var sum float64
	for i := 0; i < n; i++ {
		s := int16(binary.LittleEndian.Uint16(pcm[2*i:]))
		v := float64(s) / 32768
		sum += v * v
	}
	return math.Sqrt(sum / float64(n))
}
