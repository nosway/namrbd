package wirev2

import (
	"bytes"
	"testing"
)

func TestEncodeDecodeMessageHELLO(t *testing.T) {
	h := &Header{
		Op:        OpHello,
		SessionID: 0,
		SeqNo:     0,
	}
	payload := []byte(`{"token":"x","client_nonce":"y","device_id":0,"host_id":"h","supported_auth":["token-hmac-v1"],"requested_path_id":0}`)
	frame, err := EncodeMessage(h, payload, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(frame) != HeaderSizeV2+len(payload) {
		t.Fatalf("frame len want %d got %d", HeaderSizeV2+len(payload), len(frame))
	}
	dec, p, tag, err := DecodeMessage(frame)
	if err != nil {
		t.Fatal(err)
	}
	if dec.Op != OpHello || dec.SessionID != 0 || dec.SeqNo != 0 || dec.AuthLen != 0 {
		t.Fatalf("header: %+v", dec)
	}
	if !bytes.Equal(p, payload) {
		t.Fatalf("payload mismatch")
	}
	if len(tag) != 0 {
		t.Fatalf("expected no auth tag")
	}
}

func TestEncodeDecodeMessageWithAuthTag(t *testing.T) {
	key := []byte("session-key-32-bytes-long!!!!!")
	payload := []byte("read-data")
	h := &Header{
		Version:     VersionV2,
		Op:          OpRead,
		RequestID:   1,
		VolumeID:    101,
		Generation:  7,
		SessionID:   999,
		SeqNo:       1,
		OffsetBytes: 0,
		LengthBytes: uint32(len(payload)), // must match payload for AAD
	}
	tag := ComputeAuthTag(key, h, payload)
	frame, err := EncodeMessage(h, payload, tag)
	if err != nil {
		t.Fatal(err)
	}
	dec, p, gotTag, err := DecodeMessage(frame)
	if err != nil {
		t.Fatal(err)
	}
	if dec.AuthLen != AuthTagSize || !bytes.Equal(p, payload) || !bytes.Equal(gotTag, tag) {
		t.Fatalf("decode: auth_len=%d payload_eq=%v tag_eq=%v", dec.AuthLen, bytes.Equal(p, payload), bytes.Equal(gotTag, tag))
	}
	if err := VerifyAuthTag(key, &dec, p, gotTag); err != nil {
		t.Fatalf("verify after decode: %v", err)
	}
}

func TestEncodeDecodeResponseMessage(t *testing.T) {
	r := &ResponseHeader{
		Base:       Header{Op: OpHelloAck, SessionID: 123, SeqNo: 0},
		StatusCode: 0,
		PathID:     0,
	}
	payload := []byte(`{"session_id":123,"server_nonce":"n","selected_auth":"token-hmac-v1","expires_at":"2026-03-12T10:05:00Z","path_id":0,"max_inflight_requests":128,"max_inflight_bytes":8388608}`)
	frame, err := EncodeResponseMessage(r, payload, nil)
	if err != nil {
		t.Fatal(err)
	}
	dec, p, tag, err := DecodeResponseMessage(frame)
	if err != nil {
		t.Fatal(err)
	}
	if dec.Base.Op != OpHelloAck || dec.StatusCode != 0 || !bytes.Equal(p, payload) || len(tag) != 0 {
		t.Fatalf("response: op=%d status=%d payload_eq=%v tag_len=%d", dec.Base.Op, dec.StatusCode, bytes.Equal(p, payload), len(tag))
	}
}
