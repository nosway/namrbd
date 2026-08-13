package service

import "testing"

func TestSBSRequestContextValidate(t *testing.T) {
	ctx := SBSRequestContext{
		RequestID:      "req-1",
		GatewayID:      "gw-a",
		AttachmentID:   "att-00000065-0001",
		Generation:     7,
		IdempotencyKey: "idem-1",
	}
	if err := ctx.Validate(true, true); err != nil {
		t.Fatalf("Validate returned error: %v", err)
	}
}

func TestSBSWriteRequestValidate(t *testing.T) {
	req := WriteRequest{
		VolumeID:    "00000065",
		OffsetBytes: 0,
		LengthBytes: 4096,
		Data:        make([]byte, 4096),
		Context: SBSRequestContext{
			RequestID:      "req-write-1",
			GatewayID:      "gw-a",
			AttachmentID:   "att-00000065-0001",
			Generation:     7,
			IdempotencyKey: "idem-write-1",
		},
	}
	if err := req.Validate(); err != nil {
		t.Fatalf("Validate returned error: %v", err)
	}
}

func TestSBSWriteRequestValidateRejectsMismatchedLength(t *testing.T) {
	req := WriteRequest{
		VolumeID:    "00000065",
		OffsetBytes: 0,
		LengthBytes: 4096,
		Data:        make([]byte, 1024),
		Context: SBSRequestContext{
			RequestID:      "req-write-1",
			GatewayID:      "gw-a",
			AttachmentID:   "att-00000065-0001",
			Generation:     7,
			IdempotencyKey: "idem-write-1",
		},
	}
	if err := req.Validate(); err != ErrSBSDataLengthMismatch {
		t.Fatalf("expected ErrSBSDataLengthMismatch, got %v", err)
	}
}

func TestSBSReadRequestValidateRejectsInvalidVolumeID(t *testing.T) {
	req := ReadRequest{
		VolumeID:    "bad-id",
		OffsetBytes: 0,
		LengthBytes: 4096,
		Context: SBSRequestContext{
			RequestID:    "req-read-1",
			GatewayID:    "gw-a",
			AttachmentID: "att-00000065-0001",
			Generation:   7,
		},
	}
	if err := req.Validate(); err != ErrSBSVolumeIDInvalid {
		t.Fatalf("expected ErrSBSVolumeIDInvalid, got %v", err)
	}
}
