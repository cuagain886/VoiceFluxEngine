package transport

import (
	"encoding/binary"
	"errors"
	"fmt"

	"voicestream/internal/transport/transportpb"
)

// 固定二进制帧头的线路常量。
//
// 布局（大端 / 网络字节序），共 24 字节：
//
//	偏移 大小 字段    类型        含义
//	0    2    magic   0x56 0x53   同步 / 合法性校验（"VS"）
//	2    1    version uint8       协议版本
//	3    1    type    uint8       FrameType（AUDIO/TEXT/CONTROL）
//	4    8    seq     uint64      单调递增的序列号
//	12   8    ts_us   int64       采样时钟 PTS，微秒
//	20   4    length  uint32      负载字节数
//	24   ...  payload bytes       AUDIO=裸 PCM；TEXT/CONTROL=protobuf
//
// 帧头是手写的（不用 protobuf），所以解析便宜、零分配，且能跑在任何传输上
// ——包括自身不提供消息分帧的字节流传输（裸 TCP / WebTransport）。
const (
	Magic0     = 0x56 // 'V'
	Magic1     = 0x53 // 'S'
	Version    = 1
	HeaderSize = 24
	// MaxPayload 限定单帧负载，防御恶意或损坏的 length 字段。64 KiB 远超一个
	// 20ms 的 PCM 帧。
	MaxPayload = 64 * 1024
)

// 帧相关错误。调用方可用 errors.Is 匹配。
var (
	ErrShortHeader     = errors.New("transport: buffer shorter than frame header")
	ErrBadMagic        = errors.New("transport: bad frame magic")
	ErrVersion         = errors.New("transport: incompatible frame version")
	ErrPayloadTooLarge = errors.New("transport: payload exceeds MaxPayload")
	ErrFrameLength     = errors.New("transport: declared length does not match payload")
)

// Frame 是一个解码后的协议帧：固定头字段 + 负载。AUDIO 帧的 Payload 是裸
// PCM；TEXT/CONTROL 的是序列化后的 protobuf 消息（见 transportpb）。
type Frame struct {
	Type    transportpb.FrameType
	Seq     uint64
	TsUs    int64
	Payload []byte
}

// EncodedLen 返回 Frame.MarshalBinary 将产出的字节数。
func (f Frame) EncodedLen() int { return HeaderSize + len(f.Payload) }

// MarshalBinary 把帧编码进一块新分配的字节切片。它实现 encoding.BinaryMarshaler。
func (f Frame) MarshalBinary() ([]byte, error) {
	buf, err := f.AppendBinary(make([]byte, 0, f.EncodedLen()))
	if err != nil {
		return nil, err
	}
	return buf, nil
}

// AppendBinary 把编码后的帧追加到 buf 并返回扩展后的切片
//（encoding.BinaryAppender）。传输写路径为每个连接保留一块 scratch 缓冲并
// 重复编码进去，于是稳态下行编码零分配（11.2 的零分配门禁）。
func (f Frame) AppendBinary(buf []byte) ([]byte, error) {
	if len(f.Payload) > MaxPayload {
		return nil, fmt.Errorf("%w: %d > %d", ErrPayloadTooLarge, len(f.Payload), MaxPayload)
	}
	var hdr [HeaderSize]byte
	f.encodeHeader(hdr[:])
	buf = append(buf, hdr[:]...)
	return append(buf, f.Payload...), nil
}

func (f Frame) encodeHeader(buf []byte) {
	buf[0] = Magic0
	buf[1] = Magic1
	buf[2] = Version
	buf[3] = byte(f.Type)
	binary.BigEndian.PutUint64(buf[4:12], f.Seq)
	binary.BigEndian.PutUint64(buf[12:20], uint64(f.TsUs))
	binary.BigEndian.PutUint32(buf[20:24], uint32(len(f.Payload)))
}

// Decode 从 buf 解析出一个完整帧（头 + 负载）；在 WebSocket 传输上 buf 恰好
// 是一条二进制消息。返回的 Frame 的 Payload 是 buf 的别名（共享底层数组）；
// 若需在 buf 生命周期之外保留它，请自行拷贝。
func Decode(buf []byte) (Frame, error) {
	if len(buf) < HeaderSize {
		return Frame{}, ErrShortHeader
	}
	if buf[0] != Magic0 || buf[1] != Magic1 {
		return Frame{}, ErrBadMagic
	}
	if buf[2] != Version {
		return Frame{}, fmt.Errorf("%w: got %d want %d", ErrVersion, buf[2], Version)
	}
	length := binary.BigEndian.Uint32(buf[20:24])
	if length > MaxPayload {
		return Frame{}, fmt.Errorf("%w: %d > %d", ErrPayloadTooLarge, length, MaxPayload)
	}
	if len(buf) != HeaderSize+int(length) {
		return Frame{}, fmt.Errorf("%w: header says %d, have %d", ErrFrameLength, length, len(buf)-HeaderSize)
	}
	return Frame{
		Type:    transportpb.FrameType(buf[3]),
		Seq:     binary.BigEndian.Uint64(buf[4:12]),
		TsUs:    int64(binary.BigEndian.Uint64(buf[12:20])),
		Payload: buf[HeaderSize : HeaderSize+int(length)],
	}, nil
}
