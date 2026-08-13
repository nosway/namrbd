package wirev2

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/binary"
	"errors"
)

var ErrAuthTagMismatch = errors.New("auth_tag mismatch")

// AAD returns the authenticated additional data for auth_tag computation (design 7.4).
// Order: version, op, request_id, volume_id, generation, session_id, seq_no, offset_bytes, length_bytes.
func AAD(h *Header) []byte {
	buf := make([]byte, 0, 58)
	buf = binary.LittleEndian.AppendUint16(buf, h.Version)
	buf = binary.LittleEndian.AppendUint32(buf, h.Op)
	buf = binary.LittleEndian.AppendUint64(buf, h.RequestID)
	buf = binary.LittleEndian.AppendUint64(buf, h.VolumeID)
	buf = binary.LittleEndian.AppendUint64(buf, h.Generation)
	buf = binary.LittleEndian.AppendUint64(buf, h.SessionID)
	buf = binary.LittleEndian.AppendUint64(buf, h.SeqNo)
	buf = binary.LittleEndian.AppendUint64(buf, h.OffsetBytes)
	buf = binary.LittleEndian.AppendUint32(buf, h.LengthBytes)
	return buf
}

// ComputeAuthTag returns HMAC-SHA256(sessionKey, AAD(h) || payload), length AuthTagSize.
func ComputeAuthTag(sessionKey []byte, h *Header, payload []byte) []byte {
	mac := hmac.New(sha256.New, sessionKey)
	mac.Write(AAD(h))
	if len(payload) > 0 {
		mac.Write(payload)
	}
	return mac.Sum(nil)
}

// VerifyAuthTag returns nil if tag matches HMAC-SHA256(sessionKey, AAD(h)||payload).
func VerifyAuthTag(sessionKey []byte, h *Header, payload, tag []byte) error {
	if len(tag) != AuthTagSize {
		return ErrAuthTagMismatch
	}
	expected := ComputeAuthTag(sessionKey, h, payload)
	if !hmac.Equal(tag, expected) {
		return ErrAuthTagMismatch
	}
	return nil
}
