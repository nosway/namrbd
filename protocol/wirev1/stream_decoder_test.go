package wirev1

import (
	"errors"
	"testing"
)

func TestStreamDecoder_PartialAndMultipleFrames(t *testing.T) {
	dec := NewStreamDecoder(1 << 20)

	w1, err := EncodeMessage(NewRequestHeader(OpRead, 1, 100, 7, 0, 0), nil)
	if err != nil {
		t.Fatalf("EncodeMessage w1 failed: %v", err)
	}
	w2, err := EncodeMessage(NewRequestHeader(OpWrite, 2, 100, 7, 4096, FlagChecksum), []byte("abc"))
	if err != nil {
		t.Fatalf("EncodeMessage w2 failed: %v", err)
	}

	dec.Feed(w1[:10])
	if _, _, ok, err := dec.Next(); err != nil || ok {
		t.Fatalf("expected incomplete frame, got ok=%v err=%v", ok, err)
	}

	dec.Feed(append(w1[10:], w2...))

	h, p, ok, err := dec.Next()
	if err != nil || !ok {
		t.Fatalf("first Next failed: ok=%v err=%v", ok, err)
	}
	if h.RequestID != 1 || len(p) != 0 {
		t.Fatalf("unexpected first frame: header=%+v payload=%q", h, p)
	}

	h, p, ok, err = dec.Next()
	if err != nil || !ok {
		t.Fatalf("second Next failed: ok=%v err=%v", ok, err)
	}
	if h.RequestID != 2 || string(p) != "abc" {
		t.Fatalf("unexpected second frame: header=%+v payload=%q", h, p)
	}

	if _, _, ok, err := dec.Next(); err != nil || ok {
		t.Fatalf("expected empty decoder state, got ok=%v err=%v", ok, err)
	}
}

func TestStreamDecoder_HeaderOnlyRangeRequestDoesNotConsumeNextFrame(t *testing.T) {
	dec := NewStreamDecoder(1 << 20)
	w1 := encodeHeaderOnlyRequestForTest(t, OpWriteZeroes, 1, 65536)
	w2, err := EncodeMessage(NewRequestHeader(OpWrite, 2, 100, 7, 4096, FlagChecksum), []byte("abc"))
	if err != nil {
		t.Fatalf("EncodeMessage w2 failed: %v", err)
	}

	dec.Feed(append(w1, w2...))

	h, p, ok, err := dec.Next()
	if err != nil || !ok {
		t.Fatalf("first Next failed: ok=%v err=%v", ok, err)
	}
	if h.Op != OpWriteZeroes || h.LengthBytes != 65536 || len(p) != 0 {
		t.Fatalf("unexpected first frame: header=%+v payload=%q", h, p)
	}

	h, p, ok, err = dec.Next()
	if err != nil || !ok {
		t.Fatalf("second Next failed: ok=%v err=%v", ok, err)
	}
	if h.Op != OpWrite || h.RequestID != 2 || string(p) != "abc" {
		t.Fatalf("unexpected second frame: header=%+v payload=%q", h, p)
	}
}

func TestStreamDecoder_HeaderOnlyRangeMayExceedPayloadLimit(t *testing.T) {
	dec := NewStreamDecoder(2)
	wire := encodeHeaderOnlyRequestForTest(t, OpDiscard, 1, 256<<20)
	dec.Feed(wire)

	h, p, ok, err := dec.Next()
	if err != nil || !ok {
		t.Fatalf("Next failed: ok=%v err=%v", ok, err)
	}
	if h.Op != OpDiscard || h.LengthBytes != 256<<20 || len(p) != 0 {
		t.Fatalf("unexpected frame: header=%+v payload=%q", h, p)
	}
}

func TestStreamDecoder_TooLarge(t *testing.T) {
	dec := NewStreamDecoder(2)
	wire, err := EncodeMessage(NewRequestHeader(OpWrite, 1, 1, 1, 0, FlagChecksum), []byte("abc"))
	if err != nil {
		t.Fatalf("EncodeMessage failed: %v", err)
	}
	dec.Feed(wire)

	_, _, ok, err := dec.Next()
	if ok {
		t.Fatalf("expected no frame due to too-large payload")
	}
	if !errors.Is(err, ErrFrameTooLarge) {
		t.Fatalf("expected ErrFrameTooLarge, got %v", err)
	}
}

func TestStreamDecoder_BadMagicResync(t *testing.T) {
	dec := NewStreamDecoder(1 << 20)
	wire, err := EncodeMessage(NewRequestHeader(OpRead, 9, 1, 1, 0, 0), nil)
	if err != nil {
		t.Fatalf("EncodeMessage failed: %v", err)
	}

	garbage := append([]byte{0x00}, wire...)
	dec.Feed(garbage)

	_, _, ok, err := dec.Next()
	if ok {
		t.Fatalf("expected no frame on first bad byte")
	}
	if !errors.Is(err, ErrBadMagicValue) {
		t.Fatalf("expected ErrBadMagicValue, got %v", err)
	}

	h, _, ok, err := dec.Next()
	if err != nil || !ok {
		t.Fatalf("expected successful decode after resync, ok=%v err=%v", ok, err)
	}
	if h.RequestID != 9 {
		t.Fatalf("unexpected request id after resync: %d", h.RequestID)
	}
}
