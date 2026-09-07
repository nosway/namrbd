//go:build !enterprise

package compress

import (
	"encoding/binary"
	"errors"
	"fmt"
)

const HeaderMagic uint64 = 0x4e414d52434d5002
const LegacyHeaderMagic uint16 = 0x4e43

const (
	HeaderSize                 = 32
	LegacyHeaderSize           = 16
	MaxUncompressedPayloadSize = 64 * 1024 * 1024
)

type Codec uint8

const (
	CodecNone Codec = iota
	CodecLZ4
	CodecZSTD
)

func (c Codec) String() string {
	switch c {
	case CodecNone:
		return "NONE"
	case CodecLZ4:
		return "LZ4 (Enterprise Edition Only)"
	case CodecZSTD:
		return "ZSTD (Enterprise Edition Only)"
	default:
		return fmt.Sprintf("UNKNOWN(%d)", c)
	}
}

// Valid reports whether the codec can be activated in this build. Community
// deliberately exposes only the pass-through CodecNone mode.
func (c Codec) Valid() bool { return c == CodecNone }

type ChunkHeader struct {
	Magic           uint64
	Codec           Codec
	Flags           uint8
	UncompressedLen uint32
	CompressedLen   uint32
	ChecksumCRC32C  uint32
	HeaderCRC32C    uint32
}

var (
	ErrEnterpriseOnly   = errors.New("inline compression is available only in Enterprise Edition")
	ErrCorruptedHeader  = errors.New("corrupted payload header")
	ErrChecksumMismatch = errors.New("payload checksum mismatch: data corruption detected")
	ErrTruncatedPayload = errors.New("truncated payload")
	ErrDecompressFailed = errors.New("decompression failed: invalid compressed data")
	ErrBufferOverflow   = errors.New("uncompressed size exceeds buffer allocation limit")
	ErrUnsupportedCodec = errors.New("unsupported compression codec")
)

func CalculateEntropy([]byte) float64 { return 0 }

func (h *ChunkHeader) MarshalHeader([]byte) {}

func UnmarshalHeader([]byte) (ChunkHeader, error) { return ChunkHeader{}, ErrEnterpriseOnly }

func IsEnvelope(data []byte) bool {
	return len(data) >= HeaderSize && binary.BigEndian.Uint64(data[:8]) == HeaderMagic
}

func IsLegacyEnvelope(data []byte) bool {
	return len(data) >= LegacyHeaderSize && binary.BigEndian.Uint16(data[:2]) == LegacyHeaderMagic
}

func CompressPayload(raw []byte, codec Codec) ([]byte, ChunkHeader, error) {
	if codec != CodecNone {
		return nil, ChunkHeader{}, ErrEnterpriseOnly
	}
	return append([]byte(nil), raw...), ChunkHeader{Codec: CodecNone, UncompressedLen: uint32(len(raw)), CompressedLen: uint32(len(raw))}, nil
}

func DecompressPayload([]byte) ([]byte, ChunkHeader, error) {
	return nil, ChunkHeader{}, ErrEnterpriseOnly
}
