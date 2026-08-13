package wirev1

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
)

var (
	ErrShortMessage      = errors.New("short message")
	ErrBadHeaderLen      = errors.New("bad header length")
	ErrBadMagicValue     = errors.New("bad magic")
	ErrBadVersionValue   = errors.New("bad version")
	ErrLengthMismatch    = errors.New("payload length mismatch")
	ErrHeaderCRC32C      = errors.New("header crc32c mismatch")
	ErrPayloadCRC32C     = errors.New("payload crc32c mismatch")
	ErrResponseHeaderLen = errors.New("bad response header length")
)

var crc32cTable = crc32.MakeTable(crc32.Castagnoli)

func NewRequestHeader(op uint32, requestID, volumeID, generation, offset uint64, flags uint32) Header {
	return Header{
		Magic:       Magic,
		Version:     VersionV1,
		HeaderLen:   HeaderSize,
		Op:          op,
		Flags:       flags,
		RequestID:   requestID,
		VolumeID:    volumeID,
		Generation:  generation,
		OffsetBytes: offset,
	}
}

// EncodeMessage serializes header+payload using little-endian encoding.
// CRC32C behavior follows spec:
// - with FlagChecksum: crc field is payload CRC32C
// - without FlagChecksum: crc field is header CRC32C over magic..length and zero crc field
func EncodeMessage(h Header, payload []byte) ([]byte, error) {
	h.Magic = Magic
	h.Version = VersionV1
	h.HeaderLen = HeaderSize
	h.LengthBytes = uint32(len(payload))

	msg := make([]byte, int(HeaderSize)+len(payload))
	putHeaderWithoutCRC(msg[:HeaderSize], h)

	if len(payload) > 0 {
		copy(msg[HeaderSize:], payload)
	}

	if h.Flags&FlagChecksum != 0 {
		h.CRC32C = crc32.Checksum(payload, crc32cTable)
	} else {
		h.CRC32C = crc32.Checksum(msg[:HeaderSize-4], crc32cTable)
	}
	binary.LittleEndian.PutUint32(msg[HeaderSize-4:HeaderSize], h.CRC32C)
	return msg, nil
}

func DecodeMessage(msg []byte) (Header, []byte, error) {
	if len(msg) < int(HeaderSize) {
		return Header{}, nil, ErrShortMessage
	}

	h := parseHeader(msg[:HeaderSize])
	if h.Magic != Magic {
		return Header{}, nil, ErrBadMagicValue
	}
	if h.Version != VersionV1 {
		return Header{}, nil, ErrBadVersionValue
	}
	if h.HeaderLen != HeaderSize {
		return Header{}, nil, fmt.Errorf("%w: got=%d want=%d", ErrBadHeaderLen, h.HeaderLen, HeaderSize)
	}

	if opUsesLengthAsRangeWithoutRequestPayload(h.Op) && len(msg) == int(HeaderSize) {
		payload := []byte(nil)
		// Validate header CRC32C against magic..length fields. For these
		// requests, length is an affected range, not a request payload size.
		tmp := make([]byte, HeaderSize)
		copy(tmp, msg[:HeaderSize])
		for i := HeaderSize - 4; i < HeaderSize; i++ {
			tmp[i] = 0
		}
		want := crc32.Checksum(tmp[:HeaderSize-4], crc32cTable)
		if h.CRC32C != want {
			return Header{}, nil, ErrHeaderCRC32C
		}
		return h, payload, nil
	}

	if len(msg) != int(HeaderSize)+int(h.LengthBytes) {
		return Header{}, nil, fmt.Errorf("%w: header=%d actual=%d", ErrLengthMismatch, h.LengthBytes, len(msg)-int(HeaderSize))
	}
	payload := msg[HeaderSize:]

	if h.Flags&FlagChecksum != 0 {
		want := crc32.Checksum(payload, crc32cTable)
		if h.CRC32C != want {
			return Header{}, nil, ErrPayloadCRC32C
		}
	} else {
		tmp := make([]byte, HeaderSize)
		copy(tmp, msg[:HeaderSize])
		for i := HeaderSize - 4; i < HeaderSize; i++ {
			tmp[i] = 0
		}
		want := crc32.Checksum(tmp[:HeaderSize-4], crc32cTable)
		if h.CRC32C != want {
			return Header{}, nil, ErrHeaderCRC32C
		}
	}
	return h, payload, nil
}

func EncodeResponseHeader(r ResponseHeader) ([]byte, error) {
	r.Base.Magic = Magic
	r.Base.Version = VersionV1
	r.Base.HeaderLen = ResponseHdrLen
	r.Base.LengthBytes = 0

	buf := make([]byte, ResponseHdrLen)
	putHeaderWithoutCRC(buf[:HeaderSize], r.Base)

	// Response header carries no payload in this helper.
	r.Base.CRC32C = crc32.Checksum(buf[:HeaderSize-4], crc32cTable)
	binary.LittleEndian.PutUint32(buf[HeaderSize-4:HeaderSize], r.Base.CRC32C)

	binary.LittleEndian.PutUint32(buf[56:60], uint32(r.StatusCode))
	binary.LittleEndian.PutUint32(buf[60:64], r.BackendLatencyUS)
	binary.LittleEndian.PutUint32(buf[64:68], r.GatewayLatencyUS)
	binary.LittleEndian.PutUint32(buf[68:72], r.PathID)
	binary.LittleEndian.PutUint32(buf[72:76], r.Reserved)
	return buf, nil
}

func EncodeResponseMessage(r ResponseHeader, payload []byte) ([]byte, error) {
	r.Base.Magic = Magic
	r.Base.Version = VersionV1
	r.Base.HeaderLen = ResponseHdrLen
	r.Base.LengthBytes = uint32(len(payload))

	buf := make([]byte, int(ResponseHdrLen)+len(payload))
	putHeaderWithoutCRC(buf[:HeaderSize], r.Base)
	binary.LittleEndian.PutUint32(buf[56:60], uint32(r.StatusCode))
	binary.LittleEndian.PutUint32(buf[60:64], r.BackendLatencyUS)
	binary.LittleEndian.PutUint32(buf[64:68], r.GatewayLatencyUS)
	binary.LittleEndian.PutUint32(buf[68:72], r.PathID)
	binary.LittleEndian.PutUint32(buf[72:76], r.Reserved)
	copy(buf[ResponseHdrLen:], payload)

	if r.Base.Flags&FlagChecksum != 0 {
		r.Base.CRC32C = crc32.Checksum(payload, crc32cTable)
	} else {
		tmp := make([]byte, ResponseHdrLen)
		copy(tmp, buf[:ResponseHdrLen])
		for i := HeaderSize - 4; i < HeaderSize; i++ {
			tmp[i] = 0
		}
		r.Base.CRC32C = crc32.Checksum(tmp[:HeaderSize-4], crc32cTable)
	}
	binary.LittleEndian.PutUint32(buf[HeaderSize-4:HeaderSize], r.Base.CRC32C)
	return buf, nil
}

func DecodeResponseHeader(buf []byte) (ResponseHeader, error) {
	if len(buf) < int(ResponseHdrLen) {
		return ResponseHeader{}, ErrShortMessage
	}
	base := parseHeader(buf[:HeaderSize])
	if base.Magic != Magic {
		return ResponseHeader{}, ErrBadMagicValue
	}
	if base.Version != VersionV1 {
		return ResponseHeader{}, ErrBadVersionValue
	}
	if base.HeaderLen != ResponseHdrLen {
		return ResponseHeader{}, fmt.Errorf("%w: got=%d want=%d", ErrResponseHeaderLen, base.HeaderLen, ResponseHdrLen)
	}

	// Response header CRC is always header CRC for this helper.
	tmp := make([]byte, HeaderSize)
	copy(tmp, buf[:HeaderSize])
	for i := HeaderSize - 4; i < HeaderSize; i++ {
		tmp[i] = 0
	}
	want := crc32.Checksum(tmp[:HeaderSize-4], crc32cTable)
	if base.CRC32C != want {
		return ResponseHeader{}, ErrHeaderCRC32C
	}

	return ResponseHeader{
		Base:             base,
		StatusCode:       int32(binary.LittleEndian.Uint32(buf[56:60])),
		BackendLatencyUS: binary.LittleEndian.Uint32(buf[60:64]),
		GatewayLatencyUS: binary.LittleEndian.Uint32(buf[64:68]),
		PathID:           binary.LittleEndian.Uint32(buf[68:72]),
		Reserved:         binary.LittleEndian.Uint32(buf[72:76]),
	}, nil
}

func DecodeResponseMessage(buf []byte) (ResponseHeader, []byte, error) {
	if len(buf) < int(ResponseHdrLen) {
		return ResponseHeader{}, nil, ErrShortMessage
	}
	base := parseHeader(buf[:HeaderSize])
	if base.Magic != Magic {
		return ResponseHeader{}, nil, ErrBadMagicValue
	}
	if base.Version != VersionV1 {
		return ResponseHeader{}, nil, ErrBadVersionValue
	}
	if base.HeaderLen != ResponseHdrLen {
		return ResponseHeader{}, nil, fmt.Errorf("%w: got=%d want=%d", ErrResponseHeaderLen, base.HeaderLen, ResponseHdrLen)
	}
	r := ResponseHeader{
		Base:             base,
		StatusCode:       int32(binary.LittleEndian.Uint32(buf[56:60])),
		BackendLatencyUS: binary.LittleEndian.Uint32(buf[60:64]),
		GatewayLatencyUS: binary.LittleEndian.Uint32(buf[64:68]),
		PathID:           binary.LittleEndian.Uint32(buf[68:72]),
		Reserved:         binary.LittleEndian.Uint32(buf[72:76]),
	}
	if len(buf) != int(ResponseHdrLen)+int(r.Base.LengthBytes) {
		return ResponseHeader{}, nil, fmt.Errorf("%w: header=%d actual=%d", ErrLengthMismatch, r.Base.LengthBytes, len(buf)-int(ResponseHdrLen))
	}
	payload := buf[ResponseHdrLen:]
	if r.Base.Flags&FlagChecksum != 0 {
		want := crc32.Checksum(payload, crc32cTable)
		if r.Base.CRC32C != want {
			return ResponseHeader{}, nil, ErrPayloadCRC32C
		}
	} else {
		tmp := make([]byte, ResponseHdrLen)
		copy(tmp, buf[:ResponseHdrLen])
		for i := HeaderSize - 4; i < HeaderSize; i++ {
			tmp[i] = 0
		}
		want := crc32.Checksum(tmp[:HeaderSize-4], crc32cTable)
		if r.Base.CRC32C != want {
			return ResponseHeader{}, nil, ErrHeaderCRC32C
		}
	}
	return r, payload, nil
}

func putHeaderWithoutCRC(dst []byte, h Header) {
	binary.LittleEndian.PutUint32(dst[0:4], h.Magic)
	binary.LittleEndian.PutUint16(dst[4:6], h.Version)
	binary.LittleEndian.PutUint16(dst[6:8], h.HeaderLen)
	binary.LittleEndian.PutUint32(dst[8:12], h.Op)
	binary.LittleEndian.PutUint32(dst[12:16], h.Flags)
	binary.LittleEndian.PutUint64(dst[16:24], h.RequestID)
	binary.LittleEndian.PutUint64(dst[24:32], h.VolumeID)
	binary.LittleEndian.PutUint64(dst[32:40], h.Generation)
	binary.LittleEndian.PutUint64(dst[40:48], h.OffsetBytes)
	binary.LittleEndian.PutUint32(dst[48:52], h.LengthBytes)
	binary.LittleEndian.PutUint32(dst[52:56], 0)
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
		OffsetBytes: binary.LittleEndian.Uint64(src[40:48]),
		LengthBytes: binary.LittleEndian.Uint32(src[48:52]),
		CRC32C:      binary.LittleEndian.Uint32(src[52:56]),
	}
}

func opUsesLengthAsRangeWithoutRequestPayload(op uint32) bool {
	switch op {
	case OpRead, OpDiscard, OpWriteZeroes:
		return true
	default:
		return false
	}
}
