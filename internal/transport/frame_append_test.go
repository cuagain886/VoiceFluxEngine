package transport

import (
	"bytes"
	"errors"
	"testing"

	"voicestream/internal/transport/transportpb"
)

// TestAppendBinaryRoundTrip: appending after existing bytes leaves them
// intact, and the appended region decodes back to the same frame.
func TestAppendBinaryRoundTrip(t *testing.T) {
	in := Frame{Type: transportpb.FrameType_FRAME_TYPE_AUDIO, Seq: 42, TsUs: 99, Payload: bytes.Repeat([]byte{0xAB}, 640)}
	prefix := []byte("prefix")
	buf, err := in.AppendBinary(append([]byte(nil), prefix...))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.HasPrefix(buf, prefix) {
		t.Fatal("prefix clobbered")
	}
	out, err := Decode(buf[len(prefix):])
	if err != nil {
		t.Fatal(err)
	}
	if out.Seq != in.Seq || out.TsUs != in.TsUs || !bytes.Equal(out.Payload, in.Payload) {
		t.Fatalf("round trip mismatch: %+v", out)
	}
}

func TestAppendBinaryRejectsOversizedPayload(t *testing.T) {
	f := Frame{Payload: make([]byte, MaxPayload+1)}
	if _, err := f.AppendBinary(nil); !errors.Is(err, ErrPayloadTooLarge) {
		t.Fatalf("expected ErrPayloadTooLarge, got %v", err)
	}
}

// TestAppendBinaryZeroAllocSteadyState is the 11.2 gate for the downlink
// write path: once the scratch buffer has grown to frame size, re-encoding
// into it must not allocate.
func TestAppendBinaryZeroAllocSteadyState(t *testing.T) {
	f := Frame{Type: transportpb.FrameType_FRAME_TYPE_AUDIO, Seq: 1, TsUs: 1, Payload: make([]byte, 640)}
	scratch := make([]byte, 0, f.EncodedLen())
	allocs := testing.AllocsPerRun(1000, func() {
		b, err := f.AppendBinary(scratch[:0])
		if err != nil {
			t.Fatal(err)
		}
		scratch = b
	})
	if allocs != 0 {
		t.Fatalf("AppendBinary allocs/run = %v, want 0", allocs)
	}
}

func BenchmarkFrameMarshalBinary(b *testing.B) {
	f := Frame{Type: transportpb.FrameType_FRAME_TYPE_AUDIO, Seq: 1, TsUs: 1, Payload: make([]byte, 640)}
	b.ReportAllocs()
	for n := 0; n < b.N; n++ {
		if _, err := f.MarshalBinary(); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkFrameAppendBinary(b *testing.B) {
	f := Frame{Type: transportpb.FrameType_FRAME_TYPE_AUDIO, Seq: 1, TsUs: 1, Payload: make([]byte, 640)}
	scratch := make([]byte, 0, f.EncodedLen())
	b.ReportAllocs()
	for n := 0; n < b.N; n++ {
		buf, err := f.AppendBinary(scratch[:0])
		if err != nil {
			b.Fatal(err)
		}
		scratch = buf
	}
}
