package wirev1

import (
	"encoding/binary"
	"fmt"
)

const (
	FeatureCompression uint32 = 1 << 0
	FeatureIntegrity   uint32 = 1 << 1
)

type Hello struct {
	HostID       uint64
	BootIDHash   uint64
	SessionNonce uint64
	ClientCaps   uint32
}

type PathCapability struct {
	MaxIOSize           uint32
	MaxSegments         uint32
	SupportedOpsMask    uint64
	Features            uint32
	MaxInflightRequests uint32
	MaxInflightBytes    uint64
	MaxZeroLikeIOSize   uint32
}

type ErrorDetail struct {
	RetryAfterMS uint32
	DetailCode   uint32
	Message      string
}

func EncodeHelloPayload(h Hello) []byte {
	buf := make([]byte, 8+8+8+4)
	binary.LittleEndian.PutUint64(buf[0:8], h.HostID)
	binary.LittleEndian.PutUint64(buf[8:16], h.BootIDHash)
	binary.LittleEndian.PutUint64(buf[16:24], h.SessionNonce)
	binary.LittleEndian.PutUint32(buf[24:28], h.ClientCaps)
	return buf
}

func DecodeHelloPayload(payload []byte) (Hello, error) {
	if len(payload) != 28 {
		return Hello{}, fmt.Errorf("hello payload length: got=%d want=28", len(payload))
	}
	return Hello{
		HostID:       binary.LittleEndian.Uint64(payload[0:8]),
		BootIDHash:   binary.LittleEndian.Uint64(payload[8:16]),
		SessionNonce: binary.LittleEndian.Uint64(payload[16:24]),
		ClientCaps:   binary.LittleEndian.Uint32(payload[24:28]),
	}, nil
}

func EncodePathCapabilityPayload(c PathCapability) []byte {
	buf := make([]byte, 4+4+8+4+4+8+4)
	binary.LittleEndian.PutUint32(buf[0:4], c.MaxIOSize)
	binary.LittleEndian.PutUint32(buf[4:8], c.MaxSegments)
	binary.LittleEndian.PutUint64(buf[8:16], c.SupportedOpsMask)
	binary.LittleEndian.PutUint32(buf[16:20], c.Features)
	binary.LittleEndian.PutUint32(buf[20:24], c.MaxInflightRequests)
	binary.LittleEndian.PutUint64(buf[24:32], c.MaxInflightBytes)
	binary.LittleEndian.PutUint32(buf[32:36], c.MaxZeroLikeIOSize)
	return buf
}

func DecodePathCapabilityPayload(payload []byte) (PathCapability, error) {
	if len(payload) != 32 && len(payload) != 36 {
		return PathCapability{}, fmt.Errorf("path capability payload length: got=%d want=32 or 36", len(payload))
	}
	c := PathCapability{
		MaxIOSize:           binary.LittleEndian.Uint32(payload[0:4]),
		MaxSegments:         binary.LittleEndian.Uint32(payload[4:8]),
		SupportedOpsMask:    binary.LittleEndian.Uint64(payload[8:16]),
		Features:            binary.LittleEndian.Uint32(payload[16:20]),
		MaxInflightRequests: binary.LittleEndian.Uint32(payload[20:24]),
		MaxInflightBytes:    binary.LittleEndian.Uint64(payload[24:32]),
	}
	if len(payload) >= 36 {
		c.MaxZeroLikeIOSize = binary.LittleEndian.Uint32(payload[32:36])
	}
	if c.MaxZeroLikeIOSize == 0 {
		c.MaxZeroLikeIOSize = c.MaxIOSize
	}
	return c, nil
}

// EncodeWritePayload serializes WriteTag followed by raw data.
func EncodeWritePayload(tag WriteTag, data []byte) []byte {
	buf := make([]byte, 24+len(data))
	binary.LittleEndian.PutUint64(buf[0:8], tag.HostID)
	binary.LittleEndian.PutUint64(buf[8:16], tag.BootIDHash)
	binary.LittleEndian.PutUint64(buf[16:24], tag.SequenceNo)
	copy(buf[24:], data)
	return buf
}

func DecodeWritePayload(payload []byte) (WriteTag, []byte, error) {
	if len(payload) < 24 {
		return WriteTag{}, nil, fmt.Errorf("write payload too short: %d", len(payload))
	}
	tag := WriteTag{
		HostID:     binary.LittleEndian.Uint64(payload[0:8]),
		BootIDHash: binary.LittleEndian.Uint64(payload[8:16]),
		SequenceNo: binary.LittleEndian.Uint64(payload[16:24]),
	}
	return tag, payload[24:], nil
}

func EncodeErrorDetailPayload(e ErrorDetail) ([]byte, error) {
	msgBytes := []byte(e.Message)
	if len(msgBytes) > 0xFFFF {
		return nil, fmt.Errorf("error detail message too long: %d", len(msgBytes))
	}
	buf := make([]byte, 4+4+2+len(msgBytes))
	binary.LittleEndian.PutUint32(buf[0:4], e.RetryAfterMS)
	binary.LittleEndian.PutUint32(buf[4:8], e.DetailCode)
	binary.LittleEndian.PutUint16(buf[8:10], uint16(len(msgBytes)))
	copy(buf[10:], msgBytes)
	return buf, nil
}

func DecodeErrorDetailPayload(payload []byte) (ErrorDetail, error) {
	if len(payload) < 10 {
		return ErrorDetail{}, fmt.Errorf("error detail payload too short: %d", len(payload))
	}
	msgLen := int(binary.LittleEndian.Uint16(payload[8:10]))
	if len(payload) != 10+msgLen {
		return ErrorDetail{}, fmt.Errorf("error detail message length mismatch: header=%d actual=%d", msgLen, len(payload)-10)
	}
	return ErrorDetail{
		RetryAfterMS: binary.LittleEndian.Uint32(payload[0:4]),
		DetailCode:   binary.LittleEndian.Uint32(payload[4:8]),
		Message:      string(payload[10:]),
	}, nil
}
