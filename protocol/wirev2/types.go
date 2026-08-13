package wirev2

// Wire v2 constants (Phase C3).
const (
	MagicV2   uint32 = 0x4E4D4252 // "NMBR" same as v1
	VersionV2 uint16 = 2

	HeaderSizeV2  = 76
	AuthTagSize   = 32 // HMAC-SHA256 full tag
	MaxAuthTagLen = 32
)

// Opcodes: handshake and auth (v2); data ops reuse v1 values.
const (
	OpHello    uint32 = 0x0010
	OpHelloAck uint32 = 0x0011
	OpAuthErr  uint32 = 0x0012

	// Data ops (same as wirev1 for compatibility)
	OpRead          uint32 = 0x0001
	OpWrite         uint32 = 0x0002
	OpFlush         uint32 = 0x0003
	OpDiscard       uint32 = 0x0004
	OpWriteZeroes   uint32 = 0x0005
	OpHeartbeat     uint32 = 0x0006
	OpPathProbe     uint32 = 0x0007
	OpGetVolumeInfo uint32 = 0x0008
)

// Auth/status codes (v2).
const (
	ErrTokenExpired    int32 = 20
	ErrBadAuthTag      int32 = 21
	ErrReplayDetected  int32 = 22
	ErrSessionClosed   int32 = 23
	ErrGenerationMatch int32 = 24
)

// Header is the wire v2 frame header (76 bytes).
// CRC32C covers magic..reserved (header_crc32c field zeroed for checksum).
// auth_tag (auth_len bytes) follows payload in the frame.
type Header struct {
	Magic       uint32
	Version     uint16
	HeaderLen   uint16
	Op          uint32
	Flags       uint32
	RequestID   uint64
	VolumeID    uint64
	Generation  uint64
	SessionID   uint64
	SeqNo       uint64
	OffsetBytes uint64
	LengthBytes uint32
	AuthLen     uint16
	Reserved    uint16
	CRC32C      uint32
}

// ResponseHeader extends Header with response-specific fields (same layout as v1 response tail).
const ResponseHeaderSizeV2 = HeaderSizeV2 + 20 // 76 + status_code(4)+backend_latency(4)+gateway_latency(4)+path_id(4)+reserved(4)

// ResponseHeader is the full v2 response header (96 bytes).
type ResponseHeader struct {
	Base             Header
	StatusCode       int32
	BackendLatencyUS uint32
	GatewayLatencyUS uint32
	PathID           uint32
	Reserved         uint32
}
