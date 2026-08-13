package wirev2

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
)

var crc32cTable = crc32.MakeTable(crc32.Castagnoli)

var (
	ErrShortMessage   = errors.New("short message")
	ErrBadMagicValue  = errors.New("bad magic")
	ErrBadVersion     = errors.New("bad version")
	ErrBadHeaderLen   = errors.New("bad header length")
	ErrHeaderCRC32C   = errors.New("header crc32c mismatch")
	ErrLengthMismatch = errors.New("payload length mismatch")
	ErrAuthLen        = errors.New("auth_len mismatch")
)

// putHeaderWithoutCRC writes header fields to dst (HeaderSizeV2 bytes); CRC field is written as 0.
func putHeaderWithoutCRC(dst []byte, h *Header) {
	binary.LittleEndian.PutUint32(dst[0:4], h.Magic)
	binary.LittleEndian.PutUint16(dst[4:6], h.Version)
	binary.LittleEndian.PutUint16(dst[6:8], h.HeaderLen)
	binary.LittleEndian.PutUint32(dst[8:12], h.Op)
	binary.LittleEndian.PutUint32(dst[12:16], h.Flags)
	binary.LittleEndian.PutUint64(dst[16:24], h.RequestID)
	binary.LittleEndian.PutUint64(dst[24:32], h.VolumeID)
	binary.LittleEndian.PutUint64(dst[32:40], h.Generation)
	binary.LittleEndian.PutUint64(dst[40:48], h.SessionID)
	binary.LittleEndian.PutUint64(dst[48:56], h.SeqNo)
	binary.LittleEndian.PutUint64(dst[56:64], h.OffsetBytes)
	binary.LittleEndian.PutUint32(dst[64:68], h.LengthBytes)
	binary.LittleEndian.PutUint16(dst[68:70], h.AuthLen)
	binary.LittleEndian.PutUint16(dst[70:72], h.Reserved)
	binary.LittleEndian.PutUint32(dst[72:76], 0)
}

func parseHeader(src []byte) Header {
	return Header{
		Magic:       binary.LittleEndian.Uint32(src[0:4]),
		Version:     binary.LittleEndian.Uint16(src[4:6]),
		HeaderLen:   binary.LittleEndian.Uint16(src[6:8]),
		Op:          binary.LittleEndian.Uint32(src[8:12]),
		Flags:       binary.LittleEndian.Uint32(src[12:16]),
		RequestID:   binary.LittleEndian.Uint64(src[16:24]),
		VolumeID:    binary.LittleEndian.Uint64(src[24:32]),
		Generation:  binary.LittleEndian.Uint64(src[32:40]),
		SessionID:   binary.LittleEndian.Uint64(src[40:48]),
		SeqNo:       binary.LittleEndian.Uint64(src[48:56]),
		OffsetBytes: binary.LittleEndian.Uint64(src[56:64]),
		LengthBytes: binary.LittleEndian.Uint32(src[64:68]),
		AuthLen:     binary.LittleEndian.Uint16(src[68:70]),
		Reserved:    binary.LittleEndian.Uint16(src[70:72]),
		CRC32C:      binary.LittleEndian.Uint32(src[72:76]),
	}
}

// EncodeMessage produces header + payload + auth_tag. Header CRC covers header bytes 0..71; auth_tag follows payload.
// HELLO and HELLO_ACK use auth_len=0 and no auth_tag; authenticated ops require auth_tag of length AuthTagSize.
func EncodeMessage(h *Header, payload []byte, authTag []byte) ([]byte, error) {
	needTag := h.Op != OpHello && h.Op != OpHelloAck
	if needTag && len(authTag) != AuthTagSize {
		return nil, fmt.Errorf("auth_tag must be %d bytes for authenticated op", AuthTagSize)
	}
	if !needTag && len(authTag) > 0 {
		authTag = nil
	}
	h.Magic = MagicV2
	h.Version = VersionV2
	h.HeaderLen = uint16(HeaderSizeV2)
	h.LengthBytes = uint32(len(payload))
	if needTag && len(authTag) == AuthTagSize {
		h.AuthLen = AuthTagSize
	} else {
		h.AuthLen = 0
	}

	frameLen := HeaderSizeV2 + len(payload) + int(h.AuthLen)
	out := make([]byte, frameLen)
	putHeaderWithoutCRC(out[:HeaderSizeV2], h)
	crc := crc32.Checksum(out[:72], crc32cTable)
	binary.LittleEndian.PutUint32(out[72:76], crc)
	if len(payload) > 0 {
		copy(out[HeaderSizeV2:HeaderSizeV2+len(payload)], payload)
	}
	if h.AuthLen > 0 {
		copy(out[HeaderSizeV2+len(payload):], authTag)
	}
	return out, nil
}

// DecodeMessage parses header, payload and auth_tag from a frame. Does not verify CRC or auth_tag.
func DecodeMessage(buf []byte) (Header, []byte, []byte, error) {
	if len(buf) < HeaderSizeV2 {
		return Header{}, nil, nil, ErrShortMessage
	}
	h := parseHeader(buf[:HeaderSizeV2])
	if h.Magic != MagicV2 {
		return Header{}, nil, nil, ErrBadMagicValue
	}
	if h.Version != VersionV2 {
		return Header{}, nil, nil, ErrBadVersion
	}
	if h.HeaderLen != uint16(HeaderSizeV2) {
		return Header{}, nil, nil, fmt.Errorf("%w: got=%d want=%d", ErrBadHeaderLen, h.HeaderLen, HeaderSizeV2)
	}
	expectedLen := HeaderSizeV2 + int(h.LengthBytes) + int(h.AuthLen)
	if len(buf) < expectedLen {
		return Header{}, nil, nil, ErrShortMessage
	}
	if len(buf) > expectedLen {
		return Header{}, nil, nil, fmt.Errorf("%w: expected=%d actual=%d", ErrLengthMismatch, expectedLen, len(buf))
	}
	// Verify header CRC
	wantCRC := crc32.Checksum(buf[:72], crc32cTable)
	if h.CRC32C != wantCRC {
		return Header{}, nil, nil, ErrHeaderCRC32C
	}
	payload := buf[HeaderSizeV2 : HeaderSizeV2+int(h.LengthBytes)]
	var tag []byte
	if h.AuthLen > 0 {
		tag = make([]byte, h.AuthLen)
		copy(tag, buf[HeaderSizeV2+int(h.LengthBytes):])
	}
	return h, payload, tag, nil
}

// EncodeResponseMessage builds response frame: ResponseHeader (96 bytes) + payload + optional auth_tag.
func EncodeResponseMessage(r *ResponseHeader, payload []byte, authTag []byte) ([]byte, error) {
	r.Base.Magic = MagicV2
	r.Base.Version = VersionV2
	r.Base.HeaderLen = uint16(ResponseHeaderSizeV2)
	r.Base.LengthBytes = uint32(len(payload))
	if len(authTag) == AuthTagSize {
		r.Base.AuthLen = AuthTagSize
	} else {
		r.Base.AuthLen = 0
	}
	frameLen := ResponseHeaderSizeV2 + len(payload) + int(r.Base.AuthLen)
	out := make([]byte, frameLen)
	putHeaderWithoutCRC(out[:HeaderSizeV2], &r.Base)
	crc := crc32.Checksum(out[:72], crc32cTable)
	binary.LittleEndian.PutUint32(out[72:76], crc)
	binary.LittleEndian.PutUint32(out[76:80], uint32(r.StatusCode))
	binary.LittleEndian.PutUint32(out[80:84], r.BackendLatencyUS)
	binary.LittleEndian.PutUint32(out[84:88], r.GatewayLatencyUS)
	binary.LittleEndian.PutUint32(out[88:92], r.PathID)
	binary.LittleEndian.PutUint32(out[92:96], r.Reserved)
	if len(payload) > 0 {
		copy(out[ResponseHeaderSizeV2:ResponseHeaderSizeV2+len(payload)], payload)
	}
	if r.Base.AuthLen > 0 {
		copy(out[ResponseHeaderSizeV2+len(payload):], authTag)
	}
	return out, nil
}

// DecodeResponseMessage parses response frame into ResponseHeader, payload, auth_tag.
func DecodeResponseMessage(buf []byte) (ResponseHeader, []byte, []byte, error) {
	if len(buf) < ResponseHeaderSizeV2 {
		return ResponseHeader{}, nil, nil, ErrShortMessage
	}
	base := parseHeader(buf[:HeaderSizeV2])
	if base.Magic != MagicV2 || base.Version != VersionV2 {
		return ResponseHeader{}, nil, nil, ErrBadMagicValue
	}
	wantCRC := crc32.Checksum(buf[:72], crc32cTable)
	if base.CRC32C != wantCRC {
		return ResponseHeader{}, nil, nil, ErrHeaderCRC32C
	}
	r := ResponseHeader{
		Base:             base,
		StatusCode:       int32(binary.LittleEndian.Uint32(buf[76:80])),
		BackendLatencyUS: binary.LittleEndian.Uint32(buf[80:84]),
		GatewayLatencyUS: binary.LittleEndian.Uint32(buf[84:88]),
		PathID:           binary.LittleEndian.Uint32(buf[88:92]),
		Reserved:         binary.LittleEndian.Uint32(buf[92:96]),
	}
	expectedLen := ResponseHeaderSizeV2 + int(r.Base.LengthBytes) + int(r.Base.AuthLen)
	if len(buf) < expectedLen {
		return ResponseHeader{}, nil, nil, ErrShortMessage
	}
	payload := buf[ResponseHeaderSizeV2 : ResponseHeaderSizeV2+int(r.Base.LengthBytes)]
	var tag []byte
	if r.Base.AuthLen > 0 {
		tag = make([]byte, r.Base.AuthLen)
		copy(tag, buf[ResponseHeaderSizeV2+int(r.Base.LengthBytes):])
	}
	return r, payload, tag, nil
}
