package service

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/nosway/namrbd/volumeid"
)

const (
	DefaultBlockSize           = 4096
	DefaultAllocationChunkSize = 64 * 1024
	DefaultAllocationPageSize  = 4 * 1024 * 1024
	DefaultChunkSize           = DefaultAllocationChunkSize
)

type VolumeAccessMode string

const (
	VolumeAccessModeExclusive VolumeAccessMode = "exclusive"
	VolumeAccessModeShared    VolumeAccessMode = "shared"
)

type VolumeLifecycleState string

const (
	VolumeStateAvailable VolumeLifecycleState = "available"
	VolumeStateInUse     VolumeLifecycleState = "in_use"
	VolumeStateDisabled  VolumeLifecycleState = "disabled"
)

type VolumeProtectedStateKind string

const (
	VolumeProtectedStateSealed         VolumeProtectedStateKind = "sealed"
	ProtectedWriteReasonSealedReadOnly                          = "worm_sealed_read_only"
)

type VolumeProtectedState struct {
	State            VolumeProtectedStateKind `json:"state,omitempty"`
	ReasonCode       string                   `json:"reason_code,omitempty"`
	SealedObjectID   string                   `json:"sealed_object_id,omitempty"`
	SealOperationID  string                   `json:"seal_operation_id,omitempty"`
	PolicySnapshotID string                   `json:"policy_snapshot_id,omitempty"`
	LifecycleState   string                   `json:"lifecycle_state,omitempty"`
	SourceVolumeID   string                   `json:"source_volume_id,omitempty"`
}

func (p VolumeProtectedState) Normalize() VolumeProtectedState {
	p.State = VolumeProtectedStateKind(strings.TrimSpace(string(p.State)))
	p.ReasonCode = strings.TrimSpace(p.ReasonCode)
	p.SealedObjectID = strings.TrimSpace(p.SealedObjectID)
	p.SealOperationID = strings.TrimSpace(p.SealOperationID)
	p.PolicySnapshotID = strings.TrimSpace(p.PolicySnapshotID)
	p.LifecycleState = strings.TrimSpace(p.LifecycleState)
	p.SourceVolumeID = strings.TrimSpace(p.SourceVolumeID)
	return p
}

func (p VolumeProtectedState) IsZero() bool {
	p = p.Normalize()
	return p.State == "" &&
		p.ReasonCode == "" &&
		p.SealedObjectID == "" &&
		p.SealOperationID == "" &&
		p.PolicySnapshotID == "" &&
		p.LifecycleState == "" &&
		p.SourceVolumeID == ""
}

func (p VolumeProtectedState) WriteRejectionReason() (string, bool) {
	p = p.Normalize()
	if p.State != VolumeProtectedStateSealed {
		return "", false
	}
	if p.ReasonCode == "" {
		return ProtectedWriteReasonSealedReadOnly, true
	}
	return p.ReasonCode, true
}

type GatewayConnectionState string

const (
	GatewayStateUnknown  GatewayConnectionState = "unknown"
	GatewayStateUp       GatewayConnectionState = "up"
	GatewayStateDegraded GatewayConnectionState = "degraded"
	GatewayStateDown     GatewayConnectionState = "down"
	GatewayStateDetached GatewayConnectionState = "detached"
)

// HexVolumeID is a volume id marshaled as an eight-digit lowercase hex string in JSON.
type HexVolumeID uint64

func (id HexVolumeID) Uint64() uint64 { return uint64(id) }

func (id HexVolumeID) MarshalJSON() ([]byte, error) {
	return json.Marshal(volumeid.Format(uint64(id)))
}

func (id *HexVolumeID) UnmarshalJSON(data []byte) error {
	var s string
	if err := json.Unmarshal(data, &s); err != nil {
		return fmt.Errorf("volume_id: %w", err)
	}
	v, err := volumeid.Parse(s)
	if err != nil {
		return fmt.Errorf("volume_id: %w", err)
	}
	*id = HexVolumeID(v)
	return nil
}

type VolumeSpec struct {
	ID              HexVolumeID           `json:"volume_id"`
	Name            string                `json:"volume_name"`
	Prefix          string                `json:"data_key_prefix"`
	SizeBytes       uint64                `json:"size_bytes"`
	BlockSize       uint32                `json:"block_size"`
	ChunkSizeBytes  uint32                `json:"chunk_size_bytes,omitempty"`
	ExtentPageBytes uint32                `json:"extent_page_bytes,omitempty"`
	AccessMode      VolumeAccessMode      `json:"access_mode"`
	State           VolumeLifecycleState  `json:"state"`
	ProtectedState  *VolumeProtectedState `json:"protected_state,omitempty"`

	RedundancyBackend              string `json:"redundancy_backend,omitempty"`
	TopologyMode                   string `json:"topology_mode,omitempty"`
	ECProfileID                    string `json:"ec_profile_id,omitempty"`
	ECCodecID                      string `json:"ec_codec_id,omitempty"`
	ECDataShards                   uint32 `json:"ec_data_shards,omitempty"`
	ECParityShards                 uint32 `json:"ec_parity_shards,omitempty"`
	ECStripeUnitBytes              uint32 `json:"ec_stripe_unit_bytes,omitempty"`
	ECFailureDomain                string `json:"ec_failure_domain,omitempty"`
	ECMaxUnavailableFailureDomains uint32 `json:"ec_max_unavailable_failure_domains,omitempty"`
	ECMaxShardsPerFailureDomain    uint32 `json:"ec_max_shards_per_failure_domain,omitempty"`
	WeakPlacementAllowed           bool   `json:"weak_placement_allowed,omitempty"`
}

type AttachmentRecord struct {
	Generation     uint64 `json:"generation"`
	HostID         string `json:"host_id"`
	AttachmentID   string `json:"attachment_id"`
	DeviceID       uint32 `json:"device_id"`
	OwnerGatewayID string `json:"owner_gateway_id"`
	LeaseID        string `json:"lease_id"`
	AttachedAtUnix int64  `json:"attached_at_unix"`
}

type VolumeStatusRecord struct {
	VolumeID                           HexVolumeID            `json:"volume_id"`
	InUse                              bool                   `json:"in_use"`
	CurrentAttachmentID                string                 `json:"current_attachment_id"`
	CurrentHostID                      string                 `json:"current_host_id"`
	CurrentGatewayID                   string                 `json:"current_gateway_id"`
	GatewayConnectionState             GatewayConnectionState `json:"gateway_connection_state"`
	DesiredActiveGatewaySet            []string               `json:"desired_active_gateway_set,omitempty"`
	ObservedActiveGatewaySet           []string               `json:"observed_active_gateway_set,omitempty"`
	PathPlanRevision                   uint64                 `json:"path_plan_revision,omitempty"`
	AttachmentGeneration               uint64                 `json:"attachment_generation,omitempty"`
	WriterFencingEpoch                 uint64                 `json:"writer_fencing_epoch,omitempty"`
	PathPlanNeedsAttention             bool                   `json:"path_plan_needs_attention,omitempty"`
	PathPlanAttentionReasons           []string               `json:"path_plan_attention_reasons,omitempty"`
	PathPlanRecommendedActions         []string               `json:"path_plan_recommended_actions,omitempty"`
	PathPlanReapplyRequested           bool                   `json:"path_plan_reapply_requested,omitempty"`
	PathPlanReapplyReason              string                 `json:"path_plan_reapply_reason,omitempty"`
	PathPlanReapplyRequestedAtUnix     int64                  `json:"path_plan_reapply_requested_at_unix,omitempty"`
	RuntimePathNeedsAttention          bool                   `json:"runtime_path_needs_attention,omitempty"`
	RuntimePathAttentionReasons        []string               `json:"runtime_path_attention_reasons,omitempty"`
	RuntimePathRecommendedActions      []string               `json:"runtime_path_recommended_actions,omitempty"`
	RuntimePathFeedbackCount           uint64                 `json:"runtime_path_feedback_count,omitempty"`
	LastRuntimePathFeedbackUnix        int64                  `json:"last_runtime_path_feedback_unix,omitempty"`
	RuntimePathFeedbackSourceHost      string                 `json:"runtime_path_feedback_source_host,omitempty"`
	RuntimePathReductionHoldUntilUnix  int64                  `json:"runtime_path_reduction_hold_until_unix,omitempty"`
	RuntimePathExpansionBackoffLevel   uint32                 `json:"runtime_path_expansion_backoff_level,omitempty"`
	RuntimePathExpansionEligibleAtUnix int64                  `json:"runtime_path_expansion_eligible_at_unix,omitempty"`
	RuntimeAppliedPathPlanRevision     uint64                 `json:"runtime_applied_path_plan_revision,omitempty"`
	RuntimeAppliedPathReportedAtUnix   int64                  `json:"runtime_applied_path_reported_at_unix,omitempty"`
	RuntimeNoPathState                 string                 `json:"runtime_no_path_state,omitempty"`
	RuntimeNoPathRetryMode             string                 `json:"runtime_no_path_retry_mode,omitempty"`
	RuntimeNoPathRetrySeconds          uint32                 `json:"runtime_no_path_retry_seconds,omitempty"`
	RuntimeNoPathQueuedReqs            uint64                 `json:"runtime_no_path_queued_reqs,omitempty"`
	RuntimeNoPathRequeuedReqs          uint64                 `json:"runtime_no_path_requeued_reqs,omitempty"`
	RuntimeNoPathFailedReqs            uint64                 `json:"runtime_no_path_failed_reqs,omitempty"`
	RuntimeNoPathRecoveredReqs         uint64                 `json:"runtime_no_path_recovered_reqs,omitempty"`
	RuntimeNoPathEnterCount            uint64                 `json:"runtime_no_path_enter_count,omitempty"`
	RuntimeNoPathLastReason            string                 `json:"runtime_no_path_last_reason,omitempty"`
	RuntimeNoPathLastFeedbackUnix      int64                  `json:"runtime_no_path_last_feedback_unix,omitempty"`
	ControllerReconcileRequestedAtUnix int64                  `json:"controller_reconcile_requested_at_unix,omitempty"`
	ControllerReconcileReason          string                 `json:"controller_reconcile_reason,omitempty"`
	ControllerReconcileScheduledAtUnix int64                  `json:"controller_reconcile_scheduled_at_unix,omitempty"`
	ControllerReconcileScheduledReason string                 `json:"controller_reconcile_scheduled_reason,omitempty"`
	HandoffRequired                    bool                   `json:"handoff_required,omitempty"`
	HandoffRequestedAtUnix             int64                  `json:"handoff_requested_at_unix,omitempty"`
	HandoffAckedAtUnix                 int64                  `json:"handoff_acked_at_unix,omitempty"`
	HandoffAckedGeneration             uint64                 `json:"handoff_acked_generation,omitempty"`
	HandoffCompletionEligibleAtUnix    int64                  `json:"handoff_completion_eligible_at_unix,omitempty"`
	HandoffEscalationCount             uint64                 `json:"handoff_escalation_count,omitempty"`
	HandoffNextEscalationAtUnix        int64                  `json:"handoff_next_escalation_at_unix,omitempty"`
	HandoffStage                       string                 `json:"handoff_stage,omitempty"`
	HandoffReason                      string                 `json:"handoff_reason,omitempty"`
	HandoffTargetGatewaySet            []string               `json:"handoff_target_gateway_set,omitempty"`
}

type EndpointSpec struct {
	Address    string `json:"address"`
	Port       uint16 `json:"port"`
	UseTLS     bool   `json:"use_tls"`
	ServerName string `json:"server_name"`
	AuthMode   string `json:"auth_mode"`
	PathID     uint32 `json:"path_id,omitempty"`
	Priority   uint32 `json:"priority,omitempty"`
}

type GatewayRecord struct {
	GatewayID                 string                 `json:"gateway_id"`
	ClusterID                 string                 `json:"cluster_id,omitempty"`
	SBSClusterID              string                 `json:"sbs_cluster_id,omitempty"`
	ConnectionState           GatewayConnectionState `json:"connection_state"`
	LastSeenUnix              int64                  `json:"last_seen_unix"`
	LeaseID                   string                 `json:"lease_id"`
	LeaseExpiresAtUnix        int64                  `json:"lease_expires_at_unix,omitempty"`
	StartedAtUnix             int64                  `json:"started_at_unix,omitempty"`
	BuildVersion              string                 `json:"build_version,omitempty"`
	MetadataBackend           string                 `json:"metadata_backend,omitempty"`
	MetadataRoot              string                 `json:"metadata_root,omitempty"`
	SBSClusterMetadataBackend string                 `json:"sbs_cluster_metadata_backend,omitempty"`
	SBSClusterMetadataRoot    string                 `json:"sbs_cluster_metadata_root,omitempty"`
	FailureDomain             string                 `json:"failure_domain,omitempty"`
	Capabilities              []string               `json:"capabilities,omitempty"`
	ControlEndpoints          []EndpointSpec         `json:"control_endpoints"`
	DataplaneEndpoints        []EndpointSpec         `json:"dataplane_endpoints"`
}

type AllocationChunkKind string

const (
	AllocationChunkKindData AllocationChunkKind = "data"
	AllocationChunkKindZero AllocationChunkKind = "zero"
	ExtentKindData                              = AllocationChunkKindData
	ExtentKindZero                              = AllocationChunkKindZero
)

type ExtentKind = AllocationChunkKind

type ChunkRef struct {
	VolumeID  HexVolumeID `json:"volume_id"`
	ChunkID   uint64      `json:"chunk_id"`
	SizeBytes uint32      `json:"size_bytes"`
}

type PhysicalChunkRef struct {
	StoreID string `json:"store_id,omitempty"`
	ShardID uint32 `json:"shard_id,omitempty"`
	ChunkID uint64 `json:"chunk_id,omitempty"`
}

func physicalChunkObjectIDForMetadata(volume VolumeSpec, ref PhysicalChunkRef) string {
	base := fmt.Sprintf("replicated:%s:%d", CanonicalVolumeID(uint64(volume.ID)), ref.ChunkID)
	if ref.StoreID == "" && ref.ShardID == 0 {
		return base
	}
	return fmt.Sprintf("%s:%s:%d", base, ref.StoreID, ref.ShardID)
}

const PhasePChunkPayloadEncryptionHeaderVersion = 1

type ChunkPayloadEncryptionHeader struct {
	HeaderVersion    int    `json:"header_version"`
	CipherSuite      string `json:"cipher_suite"`
	EncryptionScope  string `json:"encryption_scope"`
	SecurityPolicyID string `json:"security_policy_id"`
	PolicyGeneration uint64 `json:"policy_generation"`
	KeyProviderID    string `json:"key_provider_id"`
	DataKeyID        string `json:"data_key_id"`
	KeyID            string `json:"key_id"`
	KeyVersion       uint64 `json:"key_version"`
	KeyGeneration    uint64 `json:"key_generation"`
	ObjectID         string `json:"object_id"`
	BackendType      string `json:"backend_type"`
	NonceHex         string `json:"nonce_hex"`
	NonceSource      string `json:"nonce_source"`
	AADDigest        string `json:"aad_digest"`
	LogicalOffset    uint64 `json:"logical_offset"`
	PlaintextLength  uint64 `json:"plaintext_length"`
	CiphertextLength uint64 `json:"ciphertext_length"`
	AuthTagBytes     int    `json:"auth_tag_bytes"`
}

func (h ChunkPayloadEncryptionHeader) ValidateForChunk(volume VolumeSpec, ref PhysicalChunkRef) error {
	h = h.normalize()
	if h.HeaderVersion != PhasePChunkPayloadEncryptionHeaderVersion {
		return fmt.Errorf("unsupported chunk encryption header version=%d", h.HeaderVersion)
	}
	if h.CipherSuite != "aes_256_gcm" {
		return fmt.Errorf("unsupported chunk cipher_suite %q", h.CipherSuite)
	}
	if h.EncryptionScope == "" {
		return fmt.Errorf("chunk encryption_scope is required")
	}
	if h.SecurityPolicyID == "" {
		return fmt.Errorf("chunk security_policy_id is required")
	}
	if h.PolicyGeneration == 0 {
		return fmt.Errorf("chunk policy_generation is required")
	}
	if h.KeyProviderID == "" {
		return fmt.Errorf("chunk key_provider_id is required")
	}
	if h.DataKeyID == "" {
		return fmt.Errorf("chunk data_key_id is required")
	}
	if h.KeyID == "" {
		return fmt.Errorf("chunk key_id is required")
	}
	if h.KeyVersion == 0 {
		return fmt.Errorf("chunk key_version is required")
	}
	if h.ObjectID == "" {
		return fmt.Errorf("chunk object_id is required")
	}
	if h.ObjectID != physicalChunkObjectIDForMetadata(volume, ref) {
		return fmt.Errorf("chunk object_id=%q want %q", h.ObjectID, physicalChunkObjectIDForMetadata(volume, ref))
	}
	if h.BackendType != "replicated" {
		return fmt.Errorf("unsupported chunk backend_type %q", h.BackendType)
	}
	if len(h.NonceHex) != 24 {
		return fmt.Errorf("chunk nonce_hex must encode 12 bytes")
	}
	if _, err := hex.DecodeString(h.NonceHex); err != nil {
		return fmt.Errorf("decode chunk nonce_hex: %w", err)
	}
	if h.NonceSource == "" {
		return fmt.Errorf("chunk nonce_source is required")
	}
	if len(h.AADDigest) != 64 {
		return fmt.Errorf("chunk aad_digest must be sha256 hex")
	}
	if _, err := hex.DecodeString(h.AADDigest); err != nil {
		return fmt.Errorf("decode chunk aad_digest: %w", err)
	}
	chunkSize := uint64(volume.ChunkSizeBytes)
	if chunkSize == 0 {
		chunkSize = DefaultAllocationChunkSize
	}
	if h.LogicalOffset != ref.ChunkID*chunkSize {
		return fmt.Errorf("chunk logical_offset=%d want %d", h.LogicalOffset, ref.ChunkID*chunkSize)
	}
	if h.PlaintextLength != chunkSize {
		return fmt.Errorf("chunk plaintext_length=%d want %d", h.PlaintextLength, chunkSize)
	}
	if h.CiphertextLength <= h.PlaintextLength {
		return fmt.Errorf("chunk ciphertext_length=%d must exceed plaintext_length=%d", h.CiphertextLength, h.PlaintextLength)
	}
	if h.AuthTagBytes != 16 {
		return fmt.Errorf("chunk auth_tag_bytes=%d want=16", h.AuthTagBytes)
	}
	return nil
}

func (h ChunkPayloadEncryptionHeader) normalize() ChunkPayloadEncryptionHeader {
	h.CipherSuite = strings.TrimSpace(h.CipherSuite)
	h.EncryptionScope = strings.TrimSpace(h.EncryptionScope)
	h.SecurityPolicyID = strings.TrimSpace(h.SecurityPolicyID)
	h.KeyProviderID = strings.TrimSpace(h.KeyProviderID)
	h.DataKeyID = strings.TrimSpace(h.DataKeyID)
	h.KeyID = strings.TrimSpace(h.KeyID)
	h.ObjectID = strings.TrimSpace(h.ObjectID)
	h.BackendType = strings.TrimSpace(h.BackendType)
	h.NonceHex = strings.TrimSpace(h.NonceHex)
	h.NonceSource = strings.TrimSpace(h.NonceSource)
	h.AADDigest = strings.TrimSpace(h.AADDigest)
	return h
}

func cloneChunkPayloadEncryptionHeader(header *ChunkPayloadEncryptionHeader) *ChunkPayloadEncryptionHeader {
	if header == nil {
		return nil
	}
	cloned := *header
	return &cloned
}

type AllocationChunkRecord struct {
	LogicalChunkStart  uint64                        `json:"logical_chunk_start"`
	ChunkCount         uint32                        `json:"chunk_count"`
	Kind               AllocationChunkKind           `json:"kind"`
	StoreID            string                        `json:"store_id,omitempty"`
	ShardID            uint32                        `json:"shard_id,omitempty"`
	PhysicalChunkStart uint64                        `json:"physical_chunk_start,omitempty"`
	PayloadEncryption  *ChunkPayloadEncryptionHeader `json:"payload_encryption,omitempty"`
}

type AllocationPageRecord struct {
	VolumeID       HexVolumeID             `json:"volume_id"`
	PageNo         uint64                  `json:"page_no"`
	PageBytes      uint32                  `json:"page_bytes"`
	ChunkSizeBytes uint32                  `json:"chunk_size_bytes"`
	Revision       int64                   `json:"revision,omitempty"`
	Extents        []AllocationChunkRecord `json:"extents"`
}

type AllocationChunkGarbageRecord struct {
	VolumeID       HexVolumeID `json:"volume_id"`
	StoreID        string      `json:"store_id,omitempty"`
	ShardID        uint32      `json:"shard_id,omitempty"`
	ChunkID        uint64      `json:"chunk_id"`
	EnqueuedAtUnix int64       `json:"enqueued_at_unix"`
}

type AttachRequest struct {
	VolumeID  uint64
	HostID    string
	DeviceID  uint32
	GatewayID string
}

type DetachRequest struct {
	VolumeID     uint64
	HostID       string
	AttachmentID string
}

type VolumeCreateRequest struct {
	Name            string
	SizeBytes       uint64
	BlockSize       uint32
	ChunkSizeBytes  uint32
	ExtentPageBytes uint32
	AccessMode      VolumeAccessMode
	State           VolumeLifecycleState
}

type VolumeUpdateRequest struct {
	Name            *string
	SizeBytes       *uint64
	BlockSize       *uint32
	ChunkSizeBytes  *uint32
	ExtentPageBytes *uint32
	AccessMode      *VolumeAccessMode
	State           *VolumeLifecycleState
}

type MetadataRepository interface {
	EnsureVolume(ctx context.Context, spec VolumeSpec) error
	CreateVolume(ctx context.Context, req VolumeCreateRequest) (VolumeSpec, error)
	UpdateVolume(ctx context.Context, volumeID uint64, req VolumeUpdateRequest) (VolumeSpec, error)
	DeleteVolume(ctx context.Context, volumeID uint64) error
	GetVolume(ctx context.Context, volumeID uint64) (VolumeSpec, error)
	GetVolumeStatus(ctx context.Context, volumeID uint64) (VolumeStatusRecord, error)
	PutVolumeStatus(ctx context.Context, status VolumeStatusRecord) error
	ListVolumes(ctx context.Context) ([]VolumeSpec, error)
	SetVolumeState(ctx context.Context, volumeID uint64, state VolumeLifecycleState) (VolumeSpec, error)
	GetAttachment(ctx context.Context, volumeID uint64) (AttachmentRecord, error)
	GetGeneration(ctx context.Context, volumeID uint64) (uint64, error)
	UnsafeClearAttachment(ctx context.Context, volumeID uint64) (AttachmentRecord, error)
	UnsafeSetGeneration(ctx context.Context, volumeID uint64, generation uint64) (uint64, error)
	Attach(ctx context.Context, req AttachRequest) (AttachmentRecord, error)
	Detach(ctx context.Context, req DetachRequest) (AttachmentRecord, error)
	GetGateway(ctx context.Context, gatewayID string) (GatewayRecord, error)
	ListGateways(ctx context.Context) ([]GatewayRecord, error)
	PutGateway(ctx context.Context, rec GatewayRecord) error
	GetExtentPage(ctx context.Context, volumeID, pageNo uint64) (AllocationPageRecord, error)
	ListExtentPages(ctx context.Context, volumeID uint64) ([]AllocationPageRecord, error)
	PutExtentPage(ctx context.Context, rec AllocationPageRecord, expectedRevision int64) (AllocationPageRecord, error)
	AllocateChunkIDs(ctx context.Context, volumeID uint64, count uint32) (uint64, error)
	PutChunkGarbage(ctx context.Context, rec AllocationChunkGarbageRecord) error
	ListChunkGarbage(ctx context.Context, volumeID uint64, limit int) ([]AllocationChunkGarbageRecord, error)
	DeleteChunkGarbage(ctx context.Context, volumeID, chunkID uint64) error
}

type FreshVolumeMetadataRepository interface {
	RefreshVolume(ctx context.Context, volumeID uint64) (VolumeSpec, error)
}

type VolumeSpecSyncRepository interface {
	SyncVolumeSpec(ctx context.Context, spec VolumeSpec) error
}

type DataRepository interface {
	ReadAt(ctx context.Context, volume VolumeSpec, offsetBytes, lengthBytes uint64) ([]byte, error)
	WriteAt(ctx context.Context, volume VolumeSpec, offsetBytes, lengthBytes uint64, data []byte) error
}

type ReadResult struct {
	Data     []byte
	ZeroData bool
}

type ReadResultDataRepository interface {
	ReadAtResult(ctx context.Context, volume VolumeSpec, offsetBytes, lengthBytes uint64) (ReadResult, error)
}

type CloneDataRepository interface {
	ReadCloneAt(ctx context.Context, volume VolumeSpec, cloneID string, offsetBytes, lengthBytes uint64) ([]byte, error)
	WriteCloneAt(ctx context.Context, volume VolumeSpec, cloneID string, offsetBytes, lengthBytes uint64, data []byte) error
}

type SnapshotDataRepository interface {
	ReadSnapshotAt(ctx context.Context, volume VolumeSpec, snapshotID string, offsetBytes, lengthBytes uint64) ([]byte, error)
}

type DataWriteStats struct {
	PageLockWaitDuration   time.Duration
	ExtentPageGetDuration  time.Duration
	ChunkReadDuration      time.Duration
	ChunkAllocateDuration  time.Duration
	ChunkPayloadDuration   time.Duration
	ExtentPagePutDuration  time.Duration
	ChunkGarbageDuration   time.Duration
	Pages                  int
	Attempts               int
	ChunksRead             int
	ChunksWritten          int
	FullChunkOverwrites    int
	ChunkGarbageRecordsPut int
}

type InstrumentedDataRepository interface {
	WriteAtWithStats(ctx context.Context, volume VolumeSpec, offsetBytes, lengthBytes uint64, data []byte) (DataWriteStats, error)
}

type PhysicalChunkWriteStats struct {
	ChunkReadDuration    time.Duration
	ChunkPayloadDuration time.Duration
	ChunksRead           int
	ChunksWritten        int
	FullChunkOverwrites  int
}

type DetachCapableDataRepository interface {
	CloseAttachment(ctx context.Context, volumeID uint64, attachment AttachmentRecord) error
}

type LocalAttachmentDataRepository interface {
	CloseLocalAttachment(ctx context.Context, volumeID uint64, hostID, attachmentID string) error
}

type ReloadCapableDataRepository interface {
	ReloadAttachment(ctx context.Context, volume VolumeSpec) error
}

type FlushCapableDataRepository interface {
	FlushVolume(ctx context.Context, volume VolumeSpec) error
}

type DiscardCapableDataRepository interface {
	DiscardAt(ctx context.Context, volume VolumeSpec, offsetBytes, lengthBytes uint64) error
}

type DiscardObservationProvider interface {
	DiscardObservationFor(volume VolumeSpec, offsetBytes, lengthBytes uint64) DiscardObservation
}

type ZeroCapableDataRepository interface {
	ZeroAt(ctx context.Context, volume VolumeSpec, offsetBytes, lengthBytes uint64) error
}

func NormalizeVolumeSpec(spec VolumeSpec) VolumeSpec {
	if spec.BlockSize == 0 {
		spec.BlockSize = DefaultBlockSize
	}
	if spec.ChunkSizeBytes == 0 {
		spec.ChunkSizeBytes = DefaultAllocationChunkSize
	}
	if spec.ExtentPageBytes == 0 {
		spec.ExtentPageBytes = DefaultAllocationPageSize
	}
	if spec.AccessMode == "" {
		spec.AccessMode = VolumeAccessModeExclusive
	}
	if spec.State == "" {
		spec.State = VolumeStateAvailable
	}
	if spec.RedundancyBackend == "" {
		spec.RedundancyBackend = RedundancyBackendReplicated
	}
	if spec.Name == "" {
		spec.Name = spec.Prefix
	}
	if spec.ProtectedState != nil {
		protectedState := spec.ProtectedState.Normalize()
		if protectedState.IsZero() {
			spec.ProtectedState = nil
		} else {
			spec.ProtectedState = &protectedState
		}
	}
	return spec
}

func cloneVolumeSpec(spec VolumeSpec) VolumeSpec {
	if spec.ProtectedState != nil {
		protectedState := *spec.ProtectedState
		spec.ProtectedState = &protectedState
	}
	return spec
}

func ValidateImmutableVolumeGeometry(current VolumeSpec, req VolumeUpdateRequest) error {
	current = NormalizeVolumeSpec(current)
	if req.BlockSize != nil {
		next := *req.BlockSize
		if next == 0 {
			next = DefaultBlockSize
		}
		if next != current.BlockSize {
			return ErrVolumeGeometryChange
		}
	}
	if req.ChunkSizeBytes != nil {
		next := *req.ChunkSizeBytes
		if next == 0 {
			next = DefaultAllocationChunkSize
		}
		if next != current.ChunkSizeBytes {
			return ErrVolumeGeometryChange
		}
	}
	if req.ExtentPageBytes != nil {
		next := *req.ExtentPageBytes
		if next == 0 {
			next = DefaultAllocationPageSize
		}
		if next != current.ExtentPageBytes {
			return ErrVolumeGeometryChange
		}
	}
	return nil
}

func CanonicalVolumeID(volumeID uint64) string {
	return volumeid.Format(volumeID)
}

func BuildVolumePrefix(name string, volumeID uint64) string {
	return fmt.Sprintf("%s-%s", strings.TrimSpace(name), CanonicalVolumeID(volumeID))
}

func NewGatewayRecord(gatewayID, buildVersion string, controlEndpoints, dataplaneEndpoints []EndpointSpec) GatewayRecord {
	now := time.Now().Unix()
	return GatewayRecord{
		GatewayID:          gatewayID,
		ConnectionState:    GatewayStateUp,
		LastSeenUnix:       now,
		StartedAtUnix:      now,
		BuildVersion:       buildVersion,
		Capabilities:       []string{"control-plane", "dataplane", "volume-spec-cache", "etcd-metadata"},
		ControlEndpoints:   controlEndpoints,
		DataplaneEndpoints: dataplaneEndpoints,
	}
}
