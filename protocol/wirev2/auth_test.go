package wirev2

import (
	"testing"
)

func TestComputeAndVerifyAuthTag(t *testing.T) {
	key := []byte("session-key-32-bytes-long!!!!!")
	h := &Header{
		Version:     VersionV2,
		Op:          OpRead,
		RequestID:   1,
		VolumeID:    101,
		Generation:  7,
		SessionID:   12345,
		SeqNo:       1,
		OffsetBytes: 0,
		LengthBytes: 4096,
	}
	payload := []byte("read-payload-placeholder")
	tag := ComputeAuthTag(key, h, payload)
	if len(tag) != AuthTagSize {
		t.Fatalf("tag len want %d got %d", AuthTagSize, len(tag))
	}
	if err := VerifyAuthTag(key, h, payload, tag); err != nil {
		t.Fatalf("verify: %v", err)
	}
	if err := VerifyAuthTag(key, h, payload, make([]byte, AuthTagSize)); err != ErrAuthTagMismatch {
		t.Fatalf("wrong tag should fail: %v", err)
	}
	if err := VerifyAuthTag([]byte("wrong-key"), h, payload, tag); err != ErrAuthTagMismatch {
		t.Fatalf("wrong key should fail: %v", err)
	}
}

func TestAADDeterministic(t *testing.T) {
	h := &Header{Version: VersionV2, Op: OpWrite, RequestID: 2, VolumeID: 102, Generation: 1, SessionID: 99, SeqNo: 5, OffsetBytes: 8192, LengthBytes: 4096}
	aad1 := AAD(h)
	aad2 := AAD(h)
	if len(aad1) != 58 {
		t.Fatalf("AAD len want 58 got %d", len(aad1))
	}
	for i := range aad1 {
		if aad1[i] != aad2[i] {
			t.Fatalf("AAD not deterministic at %d", i)
		}
	}
}
