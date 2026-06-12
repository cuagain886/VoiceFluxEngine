package transport

import (
	"encoding/binary"
	"errors"
	"fmt"

	"voicestream/internal/transport/transportpb"
)

// Wire constants for the fixed binary frame header.
//
// Layout (big-endian / network byte order), 24 bytes total:
//
//	offset size field   type        meaning
//	0      2    magic   0x56 0x53   sync / sanity ("VS")
//	2      1    version uint8       protocol version
//	3      1    type    uint8       FrameType (AUDIO/TEXT/CONTROL)
//	4      8    seq     uint64      monotonic sequence number
//	12     8    ts_us   int64       sample-clock PTS, microseconds
//	20     4    length  uint32      payload length in bytes
//	24     ...  payload bytes       AUDIO=raw PCM; TEXT/CONTROL=protobuf
//
// The header is hand-rolled (not protobuf) so it is cheap to parse with zero
// allocation and works over any transport, including byte-stream transports
// (raw TCP / WebTransport) that provide no message framing of their own.
const (
	Magic0     = 0x56 // 'V'
	Magic1     = 0x53 // 'S'
	Version    = 1
	HeaderSize = 24
	// MaxPayload bounds a single frame's payload to guard against malicious or
	// corrupt length fields. 64 KiB comfortably exceeds a 20ms PCM frame.
	MaxPayload = 64 * 1024
)

// Frame errors. Callers can match these with errors.Is.
var (
	ErrShortHeader     = errors.New("transport: buffer shorter than frame header")
	ErrBadMagic        = errors.New("transport: bad frame magic")
	ErrVersion         = errors.New("transport: incompatible frame version")
	ErrPayloadTooLarge = errors.New("transport: payload exceeds MaxPayload")
	ErrFrameLength     = errors.New("transport: declared length does not match payload")
)

// Frame is a decoded protocol frame: the fixed header fields plus the payload.
// For AUDIO frames Payload is raw PCM; for TEXT/CONTROL it is a marshaled
// protobuf message (see transportpb).
type Frame struct {
	Type    transportpb.FrameType
	Seq     uint64
	TsUs    int64
	Payload []byte
}

// EncodedLen returns the number of bytes Frame.MarshalBinary will produce.
func (f Frame) EncodedLen() int { return HeaderSize + len(f.Payload) }

// MarshalBinary encodes the frame into a freshly allocated byte slice.
// It implements encoding.BinaryMarshaler.
func (f Frame) MarshalBinary() ([]byte, error) {
	buf, err := f.AppendBinary(make([]byte, 0, f.EncodedLen()))
	if err != nil {
		return nil, err
	}
	return buf, nil
}

// AppendBinary appends the encoded frame to buf and returns the extended
// slice (encoding.BinaryAppender). The transport write path keeps one
// scratch buffer per connection and re-encodes into it, so steady-state
// downlink marshaling allocates nothing (11.2 zero-alloc gate).
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

// Decode parses a complete frame (header + payload) from buf, which over a
// WebSocket transport is exactly one binary message. The returned Frame's
// Payload aliases buf; copy it if you need to retain it past buf's lifetime.
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
