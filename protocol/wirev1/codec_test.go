package wirev1

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"testing"
)

func TestEncodeDecodeMessage_HeaderCRC(t *testing.T) {
	h := NewRequestHeader(OpRead, 101, 9, 77, 4096, 0)
	wire, err := EncodeMessage(h, nil)
	if err != nil {
		t.Fatalf("EncodeMessage failed: %v", err)
	}

	decoded, payload, err := DecodeMessage(wire)
	if err != nil {
		t.Fatalf("DecodeMessage failed: %v", err)
	}
	if len(payload) != 0 {
		t.Fatalf("unexpected payload length: %d", len(payload))
	}
	if decoded.RequestID != 101 || decoded.Op != OpRead || decoded.Generation != 77 {
		t.Fatalf("unexpected decoded header: %+v", decoded)
	}
}

func TestDecodeMessage_HeaderOnlyRangeRequests(t *testing.T) {
	for _, tc := range []struct {
		name string
		op   uint32
	}{
		{name: "read", op: OpRead},
		{name: "discard", op: OpDiscard},
		{name: "write_zeroes", op: OpWriteZeroes},
	} {
		t.Run(tc.name, func(t *testing.T) {
			wire := encodeHeaderOnlyRequestForTest(t, tc.op, 101, 65536)
			decoded, payload, err := DecodeMessage(wire)
			if err != nil {
				t.Fatalf("DecodeMessage failed: %v", err)
			}
			if decoded.Op != tc.op || decoded.LengthBytes != 65536 {
				t.Fatalf("unexpected decoded header: %+v", decoded)
			}
			if len(payload) != 0 {
				t.Fatalf("unexpected payload length: %d", len(payload))
			}
		})
	}
}

func TestEncodeDecodeMessage_PayloadCRC(t *testing.T) {
	payload := []byte("abcd1234payload")
	h := NewRequestHeader(OpWrite, 7, 10, 3, 8192, FlagChecksum|FlagIdempotent)

	wire, err := EncodeMessage(h, payload)
	if err != nil {
		t.Fatalf("EncodeMessage failed: %v", err)
	}
	decoded, gotPayload, err := DecodeMessage(wire)
	if err != nil {
		t.Fatalf("DecodeMessage failed: %v", err)
	}
	if decoded.Flags&FlagChecksum == 0 {
		t.Fatalf("expected checksum flag")
	}
	if string(gotPayload) != string(payload) {
		t.Fatalf("payload mismatch: got=%q want=%q", gotPayload, payload)
	}
}

func TestDecodeMessage_BadMagic(t *testing.T) {
	h := NewRequestHeader(OpRead, 1, 1, 1, 0, 0)
	wire, err := EncodeMessage(h, nil)
	if err != nil {
		t.Fatalf("EncodeMessage failed: %v", err)
	}
	wire[0] = 0x00

	_, _, err = DecodeMessage(wire)
	if !errors.Is(err, ErrBadMagicValue) {
		t.Fatalf("expected ErrBadMagicValue, got %v", err)
	}
}

func TestDecodeMessage_BadPayloadCRC(t *testing.T) {
	h := NewRequestHeader(OpWrite, 5, 3, 2, 0, FlagChecksum)
	wire, err := EncodeMessage(h, []byte("hello"))
	if err != nil {
		t.Fatalf("EncodeMessage failed: %v", err)
	}
	wire[len(wire)-1] ^= 0xFF

	_, _, err = DecodeMessage(wire)
	if !errors.Is(err, ErrPayloadCRC32C) {
		t.Fatalf("expected ErrPayloadCRC32C, got %v", err)
	}
}

func TestDecodeMessage_BadHeaderCRC(t *testing.T) {
	h := NewRequestHeader(OpRead, 5, 3, 2, 0, 0)
	wire, err := EncodeMessage(h, nil)
	if err != nil {
		t.Fatalf("EncodeMessage failed: %v", err)
	}
	wire[12] ^= 0x01 // mutate flags field while keeping old header CRC

	_, _, err = DecodeMessage(wire)
	if !errors.Is(err, ErrHeaderCRC32C) {
		t.Fatalf("expected ErrHeaderCRC32C, got %v", err)
	}
}

func TestResponseHeaderRoundTrip(t *testing.T) {
	r := ResponseHeader{
		Base: Header{
			Op:         OpReadResp,
			Flags:      0,
			RequestID:  42,
			VolumeID:   100,
			Generation: 9,
		},
		StatusCode:       StatusOK,
		BackendLatencyUS: 100,
		GatewayLatencyUS: 150,
		PathID:           2,
	}
	wire, err := EncodeResponseHeader(r)
	if err != nil {
		t.Fatalf("EncodeResponseHeader failed: %v", err)
	}
	got, err := DecodeResponseHeader(wire)
	if err != nil {
		t.Fatalf("DecodeResponseHeader failed: %v", err)
	}
	if got.Base.RequestID != 42 || got.PathID != 2 || got.StatusCode != StatusOK {
		t.Fatalf("unexpected response decode: %+v", got)
	}
}

func TestResponseMessageRoundTrip(t *testing.T) {
	payload := []byte("read-response")
	r := ResponseHeader{
		Base: Header{
			Op:         OpReadResp,
			Flags:      FlagChecksum,
			RequestID:  11,
			VolumeID:   101,
			Generation: 3,
		},
		StatusCode: StatusOK,
		PathID:     1,
	}
	wire, err := EncodeResponseMessage(r, payload)
	if err != nil {
		t.Fatalf("EncodeResponseMessage failed: %v", err)
	}
	got, gotPayload, err := DecodeResponseMessage(wire)
	if err != nil {
		t.Fatalf("DecodeResponseMessage failed: %v", err)
	}
	if got.PathID != 1 || got.Base.RequestID != 11 || string(gotPayload) != string(payload) {
		t.Fatalf("unexpected response message: hdr=%+v payload=%q", got, gotPayload)
	}
}

func encodeHeaderOnlyRequestForTest(t *testing.T, op uint32, requestID uint64, length uint32) []byte {
	t.Helper()

	h := NewRequestHeader(op, requestID, 9, 77, 4096, 0)
	h.LengthBytes = length
	wire := make([]byte, HeaderSize)
	putHeaderWithoutCRC(wire, h)
	crc := crc32.Checksum(wire[:HeaderSize-4], crc32cTable)
	binary.LittleEndian.PutUint32(wire[HeaderSize-4:HeaderSize], crc)
	return wire
}
