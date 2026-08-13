package wirev1

const (
	Magic uint32 = 0x4E4D4252 // "NMBR"

	VersionV1 uint16 = 1

	HeaderSize     uint16 = 56
	ResponseHdrLen uint16 = 76
)

// Opcodes.
const (
	OpRead          uint32 = 0x0001
	OpWrite         uint32 = 0x0002
	OpFlush         uint32 = 0x0003
	OpDiscard       uint32 = 0x0004
	OpWriteZeroes   uint32 = 0x0005
	OpHeartbeat     uint32 = 0x0006
	OpPathProbe     uint32 = 0x0007
	OpGetVolumeInfo uint32 = 0x0008
	OpBarrier       uint32 = 0x0009

	OpReadResp        uint32 = 0x8001
	OpWriteResp       uint32 = 0x8002
	OpFlushResp       uint32 = 0x8003
	OpDiscardResp     uint32 = 0x8004
	OpWriteZeroesResp uint32 = 0x8005
	OpErrorResp       uint32 = 0x8FFF
)

// Request flags.
const (
	FlagSync       uint32 = 1 << 0 // F_SYNC
	FlagFUA        uint32 = 1 << 1 // F_FUA
	FlagChecksum   uint32 = 1 << 2 // F_CHECKSUM
	FlagIdempotent uint32 = 1 << 3 // F_IDEMPOTENT
	FlagTracing    uint32 = 1 << 4 // F_TRACING
	FlagCompressed uint32 = 1 << 5 // F_COMPRESSED (reserved)
)

// Status and error codes.
const (
	StatusOK int32 = 0

	ErrBadMagic           int32 = 1
	ErrUnsupportedVersion int32 = 2
	ErrUnauthorized       int32 = 3
	ErrNoSuchVolume       int32 = 4
	ErrGenerationMismatch int32 = 5
	ErrInvalidRange       int32 = 6
	ErrPathDraining       int32 = 7
	ErrNoHealthyReplica   int32 = 8
	ErrQuorumFailed       int32 = 9
	ErrTimeout            int32 = 10
	ErrRetryable          int32 = 11
	ErrBusy               int32 = 12
	ErrChecksum           int32 = 13
	ErrInternal           int32 = 14
)

type Header struct {
	Magic       uint32
	Version     uint16
	HeaderLen   uint16
	Op          uint32
	Flags       uint32
	RequestID   uint64
	VolumeID    uint64
	Generation  uint64
	OffsetBytes uint64
	LengthBytes uint32
	CRC32C      uint32
}

type ResponseHeader struct {
	Base             Header
	StatusCode       int32
	BackendLatencyUS uint32
	GatewayLatencyUS uint32
	PathID           uint32
	Reserved         uint32
}

type WriteTag struct {
	HostID     uint64
	BootIDHash uint64
	SequenceNo uint64
}
