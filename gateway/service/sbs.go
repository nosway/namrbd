package service

import (
	"context"
	"errors"
	"fmt"

	"github.com/nosway/namrbd/volumeid"
)

type SBSAccessMode string

const (
	SBSAccessModeExclusiveWriter SBSAccessMode = "exclusive-writer"
)

const (
	RedundancyBackendReplicated = "replicated"
	RedundancyBackendEC         = "ec"
)

type SBSVolumeState string

const (
	SBSVolumeStateReady       SBSVolumeState = "ready"
	SBSVolumeStateDegraded    SBSVolumeState = "degraded"
	SBSVolumeStateRecovering  SBSVolumeState = "recovering"
	SBSVolumeStateUnavailable SBSVolumeState = "unavailable"
)

type SBSErrorCode string

const (
	SBSErrorCodeNotFound            SBSErrorCode = "not_found"
	SBSErrorCodeBadRequest          SBSErrorCode = "bad_request"
	SBSErrorCodeStaleGeneration     SBSErrorCode = "stale_generation"
	SBSErrorCodeAttachmentMismatch  SBSErrorCode = "attachment_mismatch"
	SBSErrorCodeIdempotencyConflict SBSErrorCode = "idempotency_conflict"
	SBSErrorCodeSecurityRejected    SBSErrorCode = "security_rejected"
	SBSErrorCodeFenced              SBSErrorCode = "fenced"
	SBSErrorCodeUnavailable         SBSErrorCode = "unavailable"
	SBSErrorCodeTimeout             SBSErrorCode = "timeout"
	SBSErrorCodeInternal            SBSErrorCode = "internal"
)

var (
	ErrSBSRequestIDRequired      = errors.New("sbs request_id is required")
	ErrSBSGatewayIDRequired      = errors.New("sbs gateway_id is required")
	ErrSBSAttachmentIDRequired   = errors.New("sbs attachment_id is required")
	ErrSBSGenerationRequired     = errors.New("sbs generation must be >= 1")
	ErrSBSIdempotencyKeyRequired = errors.New("sbs idempotency_key is required")
	ErrSBSVolumeIDRequired       = errors.New("sbs volume_id is required")
	ErrSBSVolumeIDInvalid        = errors.New("sbs volume_id must be 8 lowercase hex chars")
	ErrSBSLengthRequired         = errors.New("sbs length_bytes must be > 0")
	ErrSBSDataLengthMismatch     = errors.New("sbs data length does not match length_bytes")
)

type SBSRequestContext struct {
	RequestID          string `json:"request_id"`
	GatewayID          string `json:"gateway_id"`
	HostID             string `json:"host_id,omitempty"`
	SessionID          string `json:"session_id,omitempty"`
	AttachmentID       string `json:"attachment_id,omitempty"`
	Generation         uint64 `json:"generation,omitempty"`
	IdempotencyKey     string `json:"idempotency_key,omitempty"`
	DeadlineUnixMS     int64  `json:"deadline_unix_ms,omitempty"`
	TraceID            string `json:"trace_id,omitempty"`
	ISCSIExportID      string `json:"iscsi_export_id,omitempty"`
	ISCSIExportLeaseID string `json:"iscsi_export_lease_id,omitempty"`
	ISCSIExportEpoch   uint64 `json:"iscsi_export_epoch,omitempty"`
}

func (c SBSRequestContext) Validate(requireWriter bool, requireIdempotency bool) error {
	if c.RequestID == "" {
		return ErrSBSRequestIDRequired
	}
	if c.GatewayID == "" {
		return ErrSBSGatewayIDRequired
	}
	if requireWriter {
		if c.AttachmentID == "" {
			return ErrSBSAttachmentIDRequired
		}
		if c.Generation == 0 {
			return ErrSBSGenerationRequired
		}
	}
	if requireIdempotency && c.IdempotencyKey == "" {
		return ErrSBSIdempotencyKeyRequired
	}
	return nil
}

type SBSVolumeProfile struct {
	SizeBytes       uint64 `json:"size_bytes"`
	BlockSize       uint32 `json:"block_size"`
	MaxIOSize       uint32 `json:"max_io_size"`
	SupportsFlush   bool   `json:"supports_flush"`
	SupportsDiscard bool   `json:"supports_discard"`
	SupportsZero    bool   `json:"supports_zero"`
	ConsistencyMode string `json:"consistency_mode"`
}

// MaterializeVolumeRequest carries a cluster-control-plane-approved immutable
// volume shape to one sbs-data node. It is separate from SBSClient so ordinary
// data-plane fakes and adapters do not implicitly gain provisioning authority.
type MaterializeVolumeRequest struct {
	Spec    VolumeSpec        `json:"spec"`
	Context SBSRequestContext `json:"context"`
}

func (r MaterializeVolumeRequest) Validate() error {
	if uint64(r.Spec.ID) == 0 {
		return ErrSBSVolumeIDRequired
	}
	if r.Spec.SizeBytes == 0 {
		return fmt.Errorf("sbs materialize size_bytes must be > 0")
	}
	if r.Spec.BlockSize == 0 {
		return fmt.Errorf("sbs materialize block_size must be > 0")
	}
	return r.Context.Validate(false, false)
}

type MaterializeVolumeResponse struct {
	Status string     `json:"status"`
	Spec   VolumeSpec `json:"spec"`
}

type VolumeMaterializerSBSClient interface {
	MaterializeVolume(context.Context, *MaterializeVolumeRequest) (*MaterializeVolumeResponse, error)
}

type SBSError struct {
	Code      SBSErrorCode `json:"code"`
	Message   string       `json:"message"`
	Retryable bool         `json:"retryable"`
}

func (e *SBSError) Error() string {
	if e == nil {
		return ""
	}
	if e.Message != "" {
		return e.Message
	}
	return string(e.Code)
}

type OpenVolumeRequest struct {
	VolumeID   string            `json:"volume_id"`
	AccessMode SBSAccessMode     `json:"access_mode"`
	Context    SBSRequestContext `json:"context"`
}

func (r OpenVolumeRequest) Validate() error {
	if err := validateSBSVolumeID(r.VolumeID); err != nil {
		return err
	}
	if r.AccessMode == "" {
		return fmt.Errorf("sbs access_mode is required")
	}
	return r.Context.Validate(true, false)
}

type OpenVolumeResponse struct {
	Status         string           `json:"status"`
	VolumeHandle   string           `json:"volume_handle,omitempty"`
	VolumeID       string           `json:"volume_id"`
	VolumeRevision uint64           `json:"volume_revision"`
	Profile        SBSVolumeProfile `json:"profile"`
	ServerVersion  string           `json:"server_version,omitempty"`
}

type CloseVolumeRequest struct {
	VolumeID     string            `json:"volume_id"`
	VolumeHandle string            `json:"volume_handle,omitempty"`
	Context      SBSRequestContext `json:"context"`
}

func (r CloseVolumeRequest) Validate() error {
	if err := validateSBSVolumeID(r.VolumeID); err != nil {
		return err
	}
	return r.Context.Validate(true, false)
}

type CloseVolumeResponse struct {
	Status string `json:"status"`
}

type GetVolumeProfileRequest struct {
	VolumeID string            `json:"volume_id"`
	Context  SBSRequestContext `json:"context"`
}

func (r GetVolumeProfileRequest) Validate() error {
	if err := validateSBSVolumeID(r.VolumeID); err != nil {
		return err
	}
	return r.Context.Validate(false, false)
}

type GetVolumeProfileResponse struct {
	VolumeID string           `json:"volume_id"`
	Profile  SBSVolumeProfile `json:"profile"`
}

type GetVolumeStatusRequest struct {
	VolumeID string            `json:"volume_id"`
	Context  SBSRequestContext `json:"context"`
}

func (r GetVolumeStatusRequest) Validate() error {
	if err := validateSBSVolumeID(r.VolumeID); err != nil {
		return err
	}
	return r.Context.Validate(false, false)
}

type GetVolumeStatusResponse struct {
	VolumeID       string         `json:"volume_id"`
	State          SBSVolumeState `json:"state"`
	Readable       bool           `json:"readable"`
	Writable       bool           `json:"writable"`
	VolumeRevision uint64         `json:"volume_revision"`
}

type ReadRequest struct {
	VolumeID     string            `json:"volume_id"`
	VolumeHandle string            `json:"volume_handle,omitempty"`
	OffsetBytes  uint64            `json:"offset_bytes"`
	LengthBytes  uint64            `json:"length_bytes"`
	Context      SBSRequestContext `json:"context"`
}

func (r ReadRequest) Validate() error {
	if err := validateSBSVolumeID(r.VolumeID); err != nil {
		return err
	}
	if r.LengthBytes == 0 {
		return ErrSBSLengthRequired
	}
	return r.Context.Validate(true, false)
}

type ReadResponse struct {
	VolumeID       string `json:"volume_id"`
	OffsetBytes    uint64 `json:"offset_bytes"`
	LengthBytes    uint64 `json:"length_bytes"`
	Data           []byte `json:"-"`
	ZeroData       bool   `json:"zero_data,omitempty"`
	VolumeRevision uint64 `json:"volume_revision"`
}

type WriteRequest struct {
	VolumeID     string            `json:"volume_id"`
	VolumeHandle string            `json:"volume_handle,omitempty"`
	OffsetBytes  uint64            `json:"offset_bytes"`
	LengthBytes  uint64            `json:"length_bytes"`
	Data         []byte            `json:"-"`
	Context      SBSRequestContext `json:"context"`
}

func (r WriteRequest) Validate() error {
	if err := validateSBSVolumeID(r.VolumeID); err != nil {
		return err
	}
	if r.LengthBytes == 0 {
		return ErrSBSLengthRequired
	}
	if uint64(len(r.Data)) != r.LengthBytes {
		return ErrSBSDataLengthMismatch
	}
	return r.Context.Validate(true, true)
}

type WriteResponse struct {
	Status         string `json:"status"`
	VolumeID       string `json:"volume_id"`
	OffsetBytes    uint64 `json:"offset_bytes"`
	LengthBytes    uint64 `json:"length_bytes"`
	CommitID       string `json:"commit_id"`
	VolumeRevision uint64 `json:"volume_revision"`
}

type ReadPhysicalChunkRequest struct {
	VolumeID         string            `json:"volume_id"`
	VolumeHandle     string            `json:"volume_handle,omitempty"`
	PhysicalChunkID  uint64            `json:"physical_chunk_id"`
	ChunkOffsetBytes uint64            `json:"chunk_offset_bytes"`
	LengthBytes      uint64            `json:"length_bytes"`
	Context          SBSRequestContext `json:"context"`
}

func (r ReadPhysicalChunkRequest) Validate() error {
	if err := validateSBSVolumeID(r.VolumeID); err != nil {
		return err
	}
	if r.PhysicalChunkID == 0 || r.LengthBytes == 0 {
		return ErrSBSLengthRequired
	}
	return r.Context.Validate(false, false)
}

type ReadPhysicalChunkResponse struct {
	VolumeID         string `json:"volume_id"`
	PhysicalChunkID  uint64 `json:"physical_chunk_id"`
	ChunkOffsetBytes uint64 `json:"chunk_offset_bytes"`
	LengthBytes      uint64 `json:"length_bytes"`
	Data             []byte `json:"-"`
	VolumeRevision   uint64 `json:"volume_revision"`
}

type WritePhysicalChunkRequest struct {
	VolumeID         string            `json:"volume_id"`
	VolumeHandle     string            `json:"volume_handle,omitempty"`
	PhysicalChunkID  uint64            `json:"physical_chunk_id"`
	ChunkOffsetBytes uint64            `json:"chunk_offset_bytes"`
	LengthBytes      uint64            `json:"length_bytes"`
	Data             []byte            `json:"-"`
	Context          SBSRequestContext `json:"context"`
}

func (r WritePhysicalChunkRequest) Validate() error {
	if err := validateSBSVolumeID(r.VolumeID); err != nil {
		return err
	}
	if r.PhysicalChunkID == 0 || r.LengthBytes == 0 {
		return ErrSBSLengthRequired
	}
	if uint64(len(r.Data)) != r.LengthBytes {
		return ErrSBSDataLengthMismatch
	}
	return r.Context.Validate(true, true)
}

type WritePhysicalChunkResponse struct {
	Status           string `json:"status"`
	VolumeID         string `json:"volume_id"`
	PhysicalChunkID  uint64 `json:"physical_chunk_id"`
	ChunkOffsetBytes uint64 `json:"chunk_offset_bytes"`
	LengthBytes      uint64 `json:"length_bytes"`
	CommitID         string `json:"commit_id"`
	VolumeRevision   uint64 `json:"volume_revision"`
}

type WriteECShardRequest struct {
	VolumeID         string            `json:"volume_id"`
	VolumeHandle     string            `json:"volume_handle,omitempty"`
	ObjectID         string            `json:"object_id"`
	StripeID         string            `json:"stripe_id"`
	StripeGeneration uint64            `json:"stripe_generation"`
	ShardID          uint32            `json:"shard_id"`
	Role             string            `json:"role"`
	RoleIndex        uint32            `json:"role_index"`
	StoreID          string            `json:"store_id"`
	Data             []byte            `json:"-"`
	Checksum         string            `json:"checksum,omitempty"`
	Context          SBSRequestContext `json:"context"`
}

func (r WriteECShardRequest) Validate() error {
	if err := validateSBSVolumeID(r.VolumeID); err != nil {
		return err
	}
	if r.ObjectID == "" {
		return fmt.Errorf("sbs object_id is required")
	}
	if r.StripeID == "" {
		return fmt.Errorf("sbs stripe_id is required")
	}
	if r.StripeGeneration == 0 {
		return fmt.Errorf("sbs stripe_generation must be >= 1")
	}
	if r.StoreID == "" {
		return fmt.Errorf("sbs store_id is required")
	}
	if r.Role == "" {
		return fmt.Errorf("sbs shard role is required")
	}
	if len(r.Data) == 0 {
		return ErrSBSLengthRequired
	}
	return r.Context.Validate(true, true)
}

type WriteECShardResponse struct {
	Status           string `json:"status"`
	VolumeID         string `json:"volume_id"`
	ObjectID         string `json:"object_id"`
	StripeID         string `json:"stripe_id"`
	StripeGeneration uint64 `json:"stripe_generation"`
	ShardID          uint32 `json:"shard_id"`
	Role             string `json:"role"`
	RoleIndex        uint32 `json:"role_index"`
	StoreID          string `json:"store_id"`
	LengthBytes      uint64 `json:"length_bytes"`
	Checksum         string `json:"checksum,omitempty"`
}

type ReadECShardRequest struct {
	VolumeID         string            `json:"volume_id"`
	VolumeHandle     string            `json:"volume_handle,omitempty"`
	ObjectID         string            `json:"object_id"`
	StripeID         string            `json:"stripe_id"`
	StripeGeneration uint64            `json:"stripe_generation"`
	ShardID          uint32            `json:"shard_id"`
	StoreID          string            `json:"store_id"`
	OffsetBytes      uint64            `json:"offset_bytes"`
	LengthBytes      uint64            `json:"length_bytes"`
	Context          SBSRequestContext `json:"context"`
}

func (r ReadECShardRequest) Validate() error {
	if err := validateSBSVolumeID(r.VolumeID); err != nil {
		return err
	}
	if r.ObjectID == "" {
		return fmt.Errorf("sbs object_id is required")
	}
	if r.StripeID == "" {
		return fmt.Errorf("sbs stripe_id is required")
	}
	if r.StripeGeneration == 0 {
		return fmt.Errorf("sbs stripe_generation must be >= 1")
	}
	if r.StoreID == "" {
		return fmt.Errorf("sbs store_id is required")
	}
	if r.LengthBytes == 0 {
		return ErrSBSLengthRequired
	}
	return r.Context.Validate(false, false)
}

type ReadECShardResponse struct {
	VolumeID         string `json:"volume_id"`
	ObjectID         string `json:"object_id"`
	StripeID         string `json:"stripe_id"`
	StripeGeneration uint64 `json:"stripe_generation"`
	ShardID          uint32 `json:"shard_id"`
	StoreID          string `json:"store_id"`
	OffsetBytes      uint64 `json:"offset_bytes"`
	LengthBytes      uint64 `json:"length_bytes"`
	Data             []byte `json:"-"`
	Checksum         string `json:"checksum,omitempty"`
}

type DeleteECShardRequest struct {
	VolumeID         string            `json:"volume_id"`
	VolumeHandle     string            `json:"volume_handle,omitempty"`
	ObjectID         string            `json:"object_id"`
	StripeID         string            `json:"stripe_id"`
	StripeGeneration uint64            `json:"stripe_generation"`
	ShardID          uint32            `json:"shard_id"`
	StoreID          string            `json:"store_id"`
	Context          SBSRequestContext `json:"context"`
}

func (r DeleteECShardRequest) Validate() error {
	if err := validateSBSVolumeID(r.VolumeID); err != nil {
		return err
	}
	if r.ObjectID == "" {
		return fmt.Errorf("sbs object_id is required")
	}
	if r.StripeID == "" {
		return fmt.Errorf("sbs stripe_id is required")
	}
	if r.StripeGeneration == 0 {
		return fmt.Errorf("sbs stripe_generation must be >= 1")
	}
	if r.StoreID == "" {
		return fmt.Errorf("sbs store_id is required")
	}
	return r.Context.Validate(true, true)
}

type DeleteECShardResponse struct {
	Status           string `json:"status"`
	VolumeID         string `json:"volume_id"`
	ObjectID         string `json:"object_id"`
	StripeID         string `json:"stripe_id"`
	StripeGeneration uint64 `json:"stripe_generation"`
	ShardID          uint32 `json:"shard_id"`
	StoreID          string `json:"store_id"`
}

type FlushRequest struct {
	VolumeID     string            `json:"volume_id"`
	VolumeHandle string            `json:"volume_handle,omitempty"`
	Context      SBSRequestContext `json:"context"`
}

func (r FlushRequest) Validate() error {
	if err := validateSBSVolumeID(r.VolumeID); err != nil {
		return err
	}
	return r.Context.Validate(true, true)
}

type FlushResponse struct {
	Status         string `json:"status"`
	VolumeRevision uint64 `json:"volume_revision"`
}

type DiscardRequest struct {
	VolumeID    string            `json:"volume_id"`
	OffsetBytes uint64            `json:"offset_bytes"`
	LengthBytes uint64            `json:"length_bytes"`
	Context     SBSRequestContext `json:"context"`
}

func (r DiscardRequest) Validate() error {
	if err := validateSBSVolumeID(r.VolumeID); err != nil {
		return err
	}
	if r.LengthBytes == 0 {
		return ErrSBSLengthRequired
	}
	return r.Context.Validate(true, true)
}

type DiscardResponse struct {
	Status         string `json:"status"`
	VolumeRevision uint64 `json:"volume_revision"`
}

type ZeroRequest struct {
	VolumeID    string            `json:"volume_id"`
	OffsetBytes uint64            `json:"offset_bytes"`
	LengthBytes uint64            `json:"length_bytes"`
	Context     SBSRequestContext `json:"context"`
}

func (r ZeroRequest) Validate() error {
	if err := validateSBSVolumeID(r.VolumeID); err != nil {
		return err
	}
	if r.LengthBytes == 0 {
		return ErrSBSLengthRequired
	}
	return r.Context.Validate(true, true)
}

type ZeroResponse struct {
	Status         string `json:"status"`
	VolumeRevision uint64 `json:"volume_revision"`
}

type SBSClient interface {
	OpenVolume(ctx context.Context, req *OpenVolumeRequest) (*OpenVolumeResponse, error)
	CloseVolume(ctx context.Context, req *CloseVolumeRequest) (*CloseVolumeResponse, error)
	GetVolumeProfile(ctx context.Context, req *GetVolumeProfileRequest) (*GetVolumeProfileResponse, error)
	GetVolumeStatus(ctx context.Context, req *GetVolumeStatusRequest) (*GetVolumeStatusResponse, error)
	Read(ctx context.Context, req *ReadRequest) (*ReadResponse, error)
	Write(ctx context.Context, req *WriteRequest) (*WriteResponse, error)
	Flush(ctx context.Context, req *FlushRequest) (*FlushResponse, error)
	Discard(ctx context.Context, req *DiscardRequest) (*DiscardResponse, error)
	Zero(ctx context.Context, req *ZeroRequest) (*ZeroResponse, error)
}

// ISCSIWriterFence is the receiver-side authority projected by sbs-service
// before an iSCSI failover revision becomes visible to gateways.
type ISCSIWriterFence struct {
	VolumeID         string `json:"volume_id"`
	ExportID         string `json:"export_id"`
	ExportLeaseID    string `json:"export_lease_id"`
	ExportEpoch      uint64 `json:"export_epoch"`
	ActiveGatewayID  string `json:"active_gateway_id"`
	RegistryRevision uint64 `json:"registry_revision"`
}

func (f ISCSIWriterFence) Validate() error {
	if err := validateSBSVolumeID(f.VolumeID); err != nil {
		return err
	}
	if f.ExportID == "" {
		return fmt.Errorf("iscsi export_id is required")
	}
	if f.ExportLeaseID == "" {
		return fmt.Errorf("iscsi export_lease_id is required")
	}
	if f.ExportEpoch == 0 {
		return fmt.Errorf("iscsi export_epoch must be >= 1")
	}
	if f.ActiveGatewayID == "" {
		return fmt.Errorf("iscsi active_gateway_id is required")
	}
	if f.RegistryRevision == 0 {
		return fmt.Errorf("iscsi registry_revision must be >= 1")
	}
	return nil
}

type ApplyISCSIWriterFenceRequest struct {
	Fence ISCSIWriterFence `json:"fence"`
}

type ApplyISCSIWriterFenceResponse struct {
	Status                   string           `json:"status"`
	Applied                  bool             `json:"applied"`
	Fence                    ISCSIWriterFence `json:"fence"`
	StaleWriterRejectedCount uint64           `json:"stale_writer_rejected_count"`
}

type ISCSIWriterFenceClient interface {
	ApplyISCSIWriterFence(context.Context, *ApplyISCSIWriterFenceRequest) (*ApplyISCSIWriterFenceResponse, error)
}

// CompressionPolicy is the revisioned write policy projected by sbs-service
// to each sbs-data receiver. Reads remain envelope-driven so a rolling policy
// change never requires a metadata lookup on the payload hot path.
type CompressionPolicy struct {
	VolumeID          string `json:"volume_id"`
	PolicyID          string `json:"policy_id"`
	PolicyRevision    uint64 `json:"policy_revision"`
	Codec             string `json:"codec"`
	Level             int32  `json:"level"`
	MinimumInputBytes uint32 `json:"minimum_input_bytes"`
	ChecksumEnabled   bool   `json:"checksum_enabled"`
	Enabled           bool   `json:"enabled"`
}

func (p CompressionPolicy) Validate() error {
	if err := validateSBSVolumeID(p.VolumeID); err != nil {
		return err
	}
	if p.PolicyID == "" {
		return fmt.Errorf("compression policy_id is required")
	}
	if p.PolicyRevision == 0 {
		return fmt.Errorf("compression policy_revision must be >= 1")
	}
	switch p.Codec {
	case "NONE", "LZ4", "ZSTD":
	default:
		return fmt.Errorf("compression codec must be NONE, LZ4, or ZSTD")
	}
	if p.Enabled && !p.ChecksumEnabled {
		return fmt.Errorf("compression checksum must be enabled")
	}
	return nil
}

type CompressionRuntimeStatus struct {
	VolumeID             string `json:"volume_id"`
	PolicyID             string `json:"policy_id"`
	PolicyRevision       uint64 `json:"policy_revision"`
	Applied              bool   `json:"applied"`
	CompressedBytes      uint64 `json:"compressed_bytes"`
	UncompressedBytes    uint64 `json:"uncompressed_bytes"`
	LegacyDecodeCount    uint64 `json:"legacy_decode_count"`
	ChecksumFailureCount uint64 `json:"checksum_failure_count"`
}

type ApplyCompressionPolicyRequest struct {
	Policy CompressionPolicy `json:"policy"`
}

type ApplyCompressionPolicyResponse struct {
	Status  string                   `json:"status"`
	Applied bool                     `json:"applied"`
	Runtime CompressionRuntimeStatus `json:"runtime"`
}

type GetCompressionRuntimeStatusRequest struct {
	VolumeID string `json:"volume_id"`
}

type GetCompressionRuntimeStatusResponse struct {
	Runtime CompressionRuntimeStatus `json:"runtime"`
}

type CompressionPolicySBSClient interface {
	ApplyCompressionPolicy(context.Context, *ApplyCompressionPolicyRequest) (*ApplyCompressionPolicyResponse, error)
	GetCompressionRuntimeStatus(context.Context, *GetCompressionRuntimeStatusRequest) (*GetCompressionRuntimeStatusResponse, error)
}

type PhysicalChunkSBSClient interface {
	ReadPhysicalChunk(ctx context.Context, req *ReadPhysicalChunkRequest) (*ReadPhysicalChunkResponse, error)
	WritePhysicalChunk(ctx context.Context, req *WritePhysicalChunkRequest) (*WritePhysicalChunkResponse, error)
}

type ECShardSBSClient interface {
	WriteECShard(ctx context.Context, req *WriteECShardRequest) (*WriteECShardResponse, error)
	ReadECShard(ctx context.Context, req *ReadECShardRequest) (*ReadECShardResponse, error)
	DeleteECShard(ctx context.Context, req *DeleteECShardRequest) (*DeleteECShardResponse, error)
}

func validateSBSVolumeID(volumeID string) error {
	if volumeID == "" {
		return ErrSBSVolumeIDRequired
	}
	if _, err := ParseVolumeID(volumeID); err != nil {
		return ErrSBSVolumeIDInvalid
	}
	return nil
}

func ParseVolumeID(volumeID string) (uint64, error) {
	return volumeid.ParseLowercase(volumeID)
}
