package transport

import (
	"bytes"
	"errors"
	"testing"

	"voicestream/internal/transport/transportpb"
)

func TestFrameRoundTrip(t *testing.T) {
	cases := []Frame{
		{Type: transportpb.FrameType_FRAME_TYPE_TEXT, Seq: 7, TsUs: 12345, Payload: []byte("hello")},
		{Type: transportpb.FrameType_FRAME_TYPE_AUDIO, Seq: 0, TsUs: 0, Payload: bytes.Repeat([]byte{0x01, 0x02}, 320)},
		{Type: transportpb.FrameType_FRAME_TYPE_CONTROL, Seq: 1 << 40, TsUs: -1, Payload: nil},
	}
	for i, in := range cases {
		buf, err := in.MarshalBinary()
		if err != nil {
			t.Fatalf("case %d: marshal: %v", i, err)
		}
		out, err := Decode(buf)
		if err != nil {
			t.Fatalf("case %d: decode: %v", i, err)
		}
		if out.Type != in.Type || out.Seq != in.Seq || out.TsUs != in.TsUs {
			t.Fatalf("case %d: header mismatch: got %+v want %+v", i, out, in)
		}
		if !bytes.Equal(out.Payload, in.Payload) {
			t.Fatalf("case %d: payload mismatch", i)
		}
	}
}

func TestDecodeRejectsBadMagic(t *testing.T) {
	buf, _ := Frame{Type: transportpb.FrameType_FRAME_TYPE_TEXT, Payload: []byte("x")}.MarshalBinary()
	buf[0] = 0x00
	if _, err := Decode(buf); !errors.Is(err, ErrBadMagic) {
		t.Fatalf("expected ErrBadMagic, got %v", err)
	}
}

func TestDecodeRejectsBadVersion(t *testing.T) {
	buf, _ := Frame{Type: transportpb.FrameType_FRAME_TYPE_TEXT, Payload: []byte("x")}.MarshalBinary()
	buf[2] = Version + 1
	if _, err := Decode(buf); !errors.Is(err, ErrVersion) {
		t.Fatalf("expected ErrVersion, got %v", err)
	}
}

func TestDecodeRejectsShortHeader(t *testing.T) {
	if _, err := Decode(make([]byte, HeaderSize-1)); !errors.Is(err, ErrShortHeader) {
		t.Fatalf("expected ErrShortHeader, got %v", err)
	}
}

func TestDecodeRejectsLengthMismatch(t *testing.T) {
	buf, _ := Frame{Type: transportpb.FrameType_FRAME_TYPE_TEXT, Payload: []byte("hello")}.MarshalBinary()
	// Truncate the payload without fixing the length field.
	if _, err := Decode(buf[:len(buf)-1]); !errors.Is(err, ErrFrameLength) {
		t.Fatalf("expected ErrFrameLength, got %v", err)
	}
}

func TestMarshalRejectsOversizedPayload(t *testing.T) {
	f := Frame{Type: transportpb.FrameType_FRAME_TYPE_AUDIO, Payload: make([]byte, MaxPayload+1)}
	if _, err := f.MarshalBinary(); !errors.Is(err, ErrPayloadTooLarge) {
		t.Fatalf("expected ErrPayloadTooLarge, got %v", err)
	}
}
