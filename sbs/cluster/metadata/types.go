package metadata

import "time"

type VolumeStatus string

const (
	VolumeStatusHealthy     VolumeStatus = "healthy"
	VolumeStatusDegraded    VolumeStatus = "degraded"
	VolumeStatusRepairing   VolumeStatus = "repairing"
	VolumeStatusRebalancing VolumeStatus = "rebalancing"
	VolumeStatusBlocked     VolumeStatus = "blocked"
)

const (
	RedundancyBackendReplicated = "replicated"
	RedundancyBackendEC         = "ec"
)

type ReplicaRole string

const (
	ReplicaRolePrimary   ReplicaRole = "primary"
	ReplicaRoleSecondary ReplicaRole = "secondary"
)

type IdempotencyResultState string

const (
	IdempotencyPending   IdempotencyResultState = "pending"
	IdempotencyCommitted IdempotencyResultState = "committed"
	IdempotencyFailed    IdempotencyResultState = "failed"
)

type NodeLifecycleState string

const (
	NodeLifecycleJoining  NodeLifecycleState = "joining"
	NodeLifecycleActive   NodeLifecycleState = "active"
	NodeLifecycleDraining NodeLifecycleState = "draining"
	NodeLifecycleRemoved  NodeLifecycleState = "removed"
)

type NodeHealthState string

const (
	NodeHealthHealthy NodeHealthState = "healthy"
	NodeHealthSuspect NodeHealthState = "suspect"
	NodeHealthDown    NodeHealthState = "down"
)

type PlacementTransitionState string

const (
	PlacementTransitionQueued    PlacementTransitionState = "queued"
	PlacementTransitionRunning   PlacementTransitionState = "running"
	PlacementTransitionPaused    PlacementTransitionState = "paused"
	PlacementTransitionCompleted PlacementTransitionState = "completed"
	PlacementTransitionFailed    PlacementTransitionState = "failed"
)

type AllocationKind string

const (
	AllocationKindZero   AllocationKind = "zero"
	AllocationKindData   AllocationKind = "data"
	AllocationKindShared AllocationKind = "shared"
)

type MutationOperationState string

const (
	MutationOperationPending    MutationOperationState = "pending"
	MutationOperationRunning    MutationOperationState = "running"
	MutationOperationCommitted  MutationOperationState = "committed"
	MutationOperationFailed     MutationOperationState = "failed"
	MutationOperationRolledBack MutationOperationState = "rolled_back"
)

type SnapshotState string

const (
	SnapshotStateCreating  SnapshotState = "creating"
	SnapshotStateAvailable SnapshotState = "available"
	SnapshotStateDeleting  SnapshotState = "deleting"
	SnapshotStateDeleted   SnapshotState = "deleted"
	SnapshotStateFailed    SnapshotState = "failed"
)

type CloneState string

const (
	CloneStateCreating      CloneState = "creating"
	CloneStateAvailable     CloneState = "available"
	CloneStateMaterializing CloneState = "materializing"
	CloneStateMaterialized  CloneState = "materialized"
	CloneStateDeleting      CloneState = "deleting"
	CloneStateDeleted       CloneState = "deleted"
	CloneStateFailed        CloneState = "failed"
)

type HealthUpdatedBy string

const (
	HealthUpdatedByManual     HealthUpdatedBy = "manual"
	HealthUpdatedByReconciler HealthUpdatedBy = "reconciler"
	HealthUpdatedByJoin       HealthUpdatedBy = "join"
	HealthUpdatedByDrain      HealthUpdatedBy = "drain"
	HealthUpdatedByRemove     HealthUpdatedBy = "remove"
)

type TopologyZoneLifecycle string

const (
	TopologyZoneLifecycleActive   TopologyZoneLifecycle = "active"
	TopologyZoneLifecycleDisabled TopologyZoneLifecycle = "disabled"
	TopologyZoneLifecycleRetiring TopologyZoneLifecycle = "retiring"
)

type TopologyZoneRecord struct {
	ZoneID        string                `json:"zone_id"`
	DisplayName   string                `json:"display_name,omitempty"`
	Lifecycle     TopologyZoneLifecycle `json:"lifecycle"`
	Labels        map[string]string     `json:"labels,omitempty"`
	CreatedAtUnix int64                 `json:"created_at_unix"`
	UpdatedAtUnix int64                 `json:"updated_at_unix,omitempty"`
}

// VolumeState stores volume-wide health and policy state.
// Extent placement remains authoritative through ExtentMappingRecord.
type VolumeState struct {
	VolumeID          string       `json:"volume_id"`
	Epoch             uint64       `json:"epoch"`
	Revision          uint64       `json:"revision"`
	PlacementPolicyID string       `json:"placement_policy_id,omitempty"`
	TopologyMode      string       `json:"topology_mode,omitempty"`
	ProtectionPolicy  string       `json:"protection_policy,omitempty"`
	RedundancyBackend string       `json:"redundancy_backend,omitempty"`
	Status            VolumeStatus `json:"status"`
}

type VolumeSpecRecord struct {
	VolumeID                       string                      `json:"volume_id"`
	SizeBytes                      uint64                      `json:"size_bytes"`
	BlockSize                      uint32                      `json:"block_size"`
	ChunkSizeBytes                 uint32                      `json:"chunk_size_bytes,omitempty"`
	ExtentPageBytes                uint32                      `json:"extent_page_bytes,omitempty"`
	ExtentSizeBytes                uint64                      `json:"extent_size_bytes,omitempty"`
	ReplicationFactor              uint32                      `json:"replication_factor"`
	PolicyName                     string                      `json:"policy_name,omitempty"`
	TopologyMode                   string                      `json:"topology_mode,omitempty"`
	RedundancyBackend              string                      `json:"redundancy_backend,omitempty"`
	ECProfileID                    string                      `json:"ec_profile_id,omitempty"`
	ECCodecID                      string                      `json:"ec_codec_id,omitempty"`
	ECDataShards                   uint32                      `json:"ec_data_shards,omitempty"`
	ECParityShards                 uint32                      `json:"ec_parity_shards,omitempty"`
	ECStripeUnitBytes              uint32                      `json:"ec_stripe_unit_bytes,omitempty"`
	ECFailureDomain                string                      `json:"ec_failure_domain,omitempty"`
	ECMaxUnavailableFailureDomains uint32                      `json:"ec_max_unavailable_failure_domains,omitempty"`
	ECMaxShardsPerFailureDomain    uint32                      `json:"ec_max_shards_per_failure_domain,omitempty"`
	WeakPlacementAllowed           bool                        `json:"weak_placement_allowed,omitempty"`
	CreatedBy                      string                      `json:"created_by,omitempty"`
	CreatedReason                  string                      `json:"created_reason,omitempty"`
	CreatedAtUnix                  int64                       `json:"created_at_unix"`
	ProtectedState                 *VolumeProtectedStateRecord `json:"protected_state,omitempty"`
}

type VolumeProtectedStateRecord struct {
	State            string `json:"state,omitempty"`
	ReasonCode       string `json:"reason_code,omitempty"`
	SealedObjectID   string `json:"sealed_object_id,omitempty"`
	SealOperationID  string `json:"seal_operation_id,omitempty"`
	PolicySnapshotID string `json:"policy_snapshot_id,omitempty"`
	LifecycleState   string `json:"lifecycle_state,omitempty"`
	SourceVolumeID   string `json:"source_volume_id,omitempty"`
}

type SnapshotRecord struct {
	SnapshotID               string        `json:"snapshot_id"`
	SourceVolumeID           string        `json:"source_volume_id"`
	SnapshotRootID           string        `json:"snapshot_root_id"`
	State                    SnapshotState `json:"state"`
	CreatedBy                string        `json:"created_by,omitempty"`
	CreatedReason            string        `json:"created_reason,omitempty"`
	CreatedAtUnix            int64         `json:"created_at_unix"`
	UpdatedAtUnix            int64         `json:"updated_at_unix,omitempty"`
	CutVolumeRevision        uint64        `json:"cut_volume_revision"`
	AllocationChunkSizeBytes uint32        `json:"allocation_chunk_size_bytes"`
	AllocationPageSizeBytes  uint32        `json:"allocation_page_size_bytes"`
	SourceSizeBytes          uint64        `json:"source_size_bytes"`
	CloneReferenceCount      uint64        `json:"clone_reference_count,omitempty"`
	IdempotencyKey           string        `json:"idempotency_key,omitempty"`
	ErrorMessage             string        `json:"error_message,omitempty"`
}

type SnapshotIdempotencyRecord struct {
	SourceVolumeID   string `json:"source_volume_id"`
	IdempotencyKey   string `json:"idempotency_key"`
	SnapshotID       string `json:"snapshot_id"`
	CreatedAtUnix    int64  `json:"created_at_unix"`
	LastObservedUnix int64  `json:"last_observed_unix,omitempty"`
}

type CloneRecord struct {
	CloneID                  string     `json:"clone_id"`
	SourceSnapshotID         string     `json:"source_snapshot_id"`
	SourceVolumeID           string     `json:"source_volume_id"`
	CloneBaseRootID          string     `json:"clone_base_root_id"`
	State                    CloneState `json:"state"`
	CreatedBy                string     `json:"created_by,omitempty"`
	CreatedReason            string     `json:"created_reason,omitempty"`
	CreatedAtUnix            int64      `json:"created_at_unix"`
	UpdatedAtUnix            int64      `json:"updated_at_unix,omitempty"`
	MaterializedVolumeID     string     `json:"materialized_volume_id,omitempty"`
	AllocationChunkSizeBytes uint32     `json:"allocation_chunk_size_bytes"`
	AllocationPageSizeBytes  uint32     `json:"allocation_page_size_bytes"`
	SizeBytes                uint64     `json:"size_bytes"`
	SourceSizeBytes          uint64     `json:"source_size_bytes"`
	DeltaPageCount           uint64     `json:"delta_page_count,omitempty"`
	DeltaObjectCount         uint64     `json:"delta_object_count,omitempty"`
	IdempotencyKey           string     `json:"idempotency_key,omitempty"`
	ErrorMessage             string     `json:"error_message,omitempty"`
}

type CloneIdempotencyRecord struct {
	SourceSnapshotID string `json:"source_snapshot_id"`
	IdempotencyKey   string `json:"idempotency_key"`
	CloneID          string `json:"clone_id"`
	CreatedAtUnix    int64  `json:"created_at_unix"`
	LastObservedUnix int64  `json:"last_observed_unix,omitempty"`
}

// ExtentMappingRecord is the initial Phase E placement unit.
// Each extent points to a placement_ref, which resolves to a replica set.
type ExtentMappingRecord struct {
	VolumeID      string `json:"volume_id"`
	ExtentID      uint64 `json:"extent_id"`
	LogicalOffset uint64 `json:"logical_offset"`
	LengthBytes   uint64 `json:"length_bytes"`
	ChunkID       uint64 `json:"chunk_id"`
	PlacementRef  string `json:"placement_ref"`
	Revision      uint64 `json:"revision"`
}

func (r ExtentMappingRecord) GetLogicalOffset() uint64 { return r.LogicalOffset }
func (r ExtentMappingRecord) GetLengthBytes() uint64   { return r.LengthBytes }

type ReplicaDescriptor struct {
	NodeID        string      `json:"node_id"`
	ReplicaID     string      `json:"replica_id"`
	Role          ReplicaRole `json:"role"`
	FailureDomain string      `json:"failure_domain,omitempty"`
}

type SBSEndpoint struct {
	Address    string `json:"address"`
	Port       uint16 `json:"port"`
	UseTLS     bool   `json:"use_tls,omitempty"`
	ServerName string `json:"server_name,omitempty"`
}

type ReplicaSetState struct {
	ReplicaSetID     string              `json:"replica_set_id"`
	VolumeID         string              `json:"volume_id"`
	PlacementRef     string              `json:"placement_ref"`
	Epoch            uint64              `json:"epoch"`
	Replicas         []ReplicaDescriptor `json:"replicas"`
	PrimaryReplicaID string              `json:"primary_replica_id"`
	WriteQuorum      uint32              `json:"write_quorum"`
	ReadQuorum       uint32              `json:"read_quorum"`
	FailureDomains   []string            `json:"failure_domains,omitempty"`
}

type IdempotencyRecord struct {
	IdempotencyKey string                 `json:"idempotency_key"`
	VolumeID       string                 `json:"volume_id"`
	AttachmentID   string                 `json:"attachment_id"`
	Generation     uint64                 `json:"generation"`
	Epoch          uint64                 `json:"epoch"`
	Revision       uint64                 `json:"revision"`
	Operation      string                 `json:"operation"`
	ResultState    IdempotencyResultState `json:"result_state"`
}

type NodeMembershipRecord struct {
	SchemaVersion      string             `json:"schema_version,omitempty"`
	ClusterID          string             `json:"cluster_id,omitempty"`
	SBSClusterID       string             `json:"sbs_cluster_id,omitempty"`
	NodeID             string             `json:"node_id"`
	ReplicaID          string             `json:"replica_id,omitempty"`
	StoreIDs           []string           `json:"store_ids,omitempty"`
	Roles              []string           `json:"roles,omitempty"`
	LifecycleState     NodeLifecycleState `json:"lifecycle_state"`
	HealthState        NodeHealthState    `json:"health_state"`
	DesiredState       string             `json:"desired_state,omitempty"`
	ObservedState      string             `json:"observed_state,omitempty"`
	Zone               string             `json:"zone,omitempty"`
	Host               string             `json:"host,omitempty"`
	CapacityBytes      uint64             `json:"capacity_bytes,omitempty"`
	UsedBytes          uint64             `json:"used_bytes,omitempty"`
	LastHeartbeatUnix  int64              `json:"last_heartbeat_unix,omitempty"`
	Version            string             `json:"version,omitempty"`
	Capabilities       []string           `json:"capabilities,omitempty"`
	AdminHTTPEndpoint  string             `json:"admin_http_endpoint,omitempty"`
	SBSEndpoints       []SBSEndpoint      `json:"sbs_endpoints,omitempty"`
	Generation         uint64             `json:"generation,omitempty"`
	MembershipRevision uint64             `json:"membership_revision,omitempty"`
	Tombstone          bool               `json:"tombstone,omitempty"`
	CreatedAtUnix      int64              `json:"created_at_unix,omitempty"`
	UpdatedAtUnix      int64              `json:"updated_at_unix,omitempty"`
	UpdatedBy          string             `json:"updated_by,omitempty"`
	UpdateReason       string             `json:"update_reason,omitempty"`
}

const (
	MembershipRecordSchemaV1          = "sbs-membership/v1"
	MembershipProjectionPageDefault   = 128
	MembershipProjectionPageMaximum   = 512
	MembershipProjectionDegradedAfter = 5 * time.Second
	MembershipProjectionBlockedAfter  = 15 * time.Second
)

type MembershipProjectionState struct {
	SchemaVersion                string `json:"schema_version"`
	MembershipRevision           uint64 `json:"membership_revision"`
	MembershipProjectionRevision uint64 `json:"membership_projection_revision"`
	MembershipUpdatedAtUnixNano  int64  `json:"membership_updated_at_unix_nano,omitempty"`
	ProjectionUpdatedAtUnixNano  int64  `json:"projection_updated_at_unix_nano,omitempty"`
	ProjectionRebuildCount       uint64 `json:"projection_rebuild_count,omitempty"`
	ProjectionResyncCount        uint64 `json:"projection_resync_count,omitempty"`
	FirstError                   string `json:"first_error,omitempty"`
	LastError                    string `json:"last_error,omitempty"`
}

type MembershipProjectionStatus struct {
	MembershipRevision           uint64 `json:"membership_revision"`
	MembershipProjectionRevision uint64 `json:"membership_projection_revision"`
	ProjectionLagMS              int64  `json:"projection_lag_ms"`
	ProjectionHealth             string `json:"projection_health"`
	Stale                        bool   `json:"stale"`
	ProjectionRebuildCount       uint64 `json:"projection_rebuild_count"`
	ProjectionResyncCount        uint64 `json:"projection_resync_count"`
	FirstError                   string `json:"first_error,omitempty"`
	LastError                    string `json:"last_error,omitempty"`
}

type MembershipProjectionPage struct {
	Records    []NodeMembershipRecord     `json:"records"`
	NextCursor string                     `json:"next_cursor,omitempty"`
	Status     MembershipProjectionStatus `json:"status"`
}

type PlacementTransitionRecord struct {
	VolumeID            string                   `json:"volume_id"`
	PlacementRef        string                   `json:"placement_ref"`
	State               PlacementTransitionState `json:"state"`
	Reason              string                   `json:"reason,omitempty"`
	CurrentReplicaSetID string                   `json:"current_replica_set_id,omitempty"`
	TargetReplicaSetID  string                   `json:"target_replica_set_id,omitempty"`
	StartedAtUnix       int64                    `json:"started_at_unix,omitempty"`
	LastProgressAtUnix  int64                    `json:"last_progress_at_unix,omitempty"`
	Attempt             uint32                   `json:"attempt,omitempty"`
}

// AllocationPageRecord stores Phase G allocation-map state for one logical page.
type AllocationPageRecord struct {
	VolumeID       string                   `json:"volume_id"`
	PageNo         uint64                   `json:"page_no"`
	PageBytes      uint32                   `json:"page_bytes"`
	ChunkSizeBytes uint32                   `json:"chunk_size_bytes"`
	Revision       uint64                   `json:"revision"`
	Extents        []AllocationExtentRecord `json:"extents"`
}

type AllocationExtentRecord struct {
	LogicalChunkStart  uint64                   `json:"logical_chunk_start"`
	ChunkCount         uint32                   `json:"chunk_count"`
	Kind               AllocationKind           `json:"kind"`
	PhysicalChunkStart uint64                   `json:"physical_chunk_start,omitempty"`
	BackingRef         string                   `json:"backing_ref,omitempty"`
	Generation         uint64                   `json:"generation,omitempty"`
	Checksum           string                   `json:"checksum,omitempty"`
	Encryption         *PayloadEncryptionHeader `json:"encryption,omitempty"`
}

type MutationPageWindowRecord struct {
	ExtentID    uint64 `json:"extent_id"`
	StartPageNo uint64 `json:"start_page_no"`
	EndPageNo   uint64 `json:"end_page_no"`
	DataBytes   uint64 `json:"data_bytes,omitempty"`
	DataChunks  uint64 `json:"data_chunks,omitempty"`
}

type MutationOperationRecord struct {
	OperationID             string                     `json:"operation_id"`
	VolumeID                string                     `json:"volume_id"`
	Kind                    string                     `json:"kind"`
	State                   MutationOperationState     `json:"state"`
	PlacementRevision       uint64                     `json:"placement_revision,omitempty"`
	AllocationRevision      uint64                     `json:"allocation_revision,omitempty"`
	WriterFencingEpoch      uint64                     `json:"writer_fencing_epoch,omitempty"`
	IdempotencyKey          string                     `json:"idempotency_key,omitempty"`
	AffectedExtentIDs       []uint64                   `json:"affected_extent_ids,omitempty"`
	AffectedPageNos         []uint64                   `json:"affected_page_nos,omitempty"`
	CompletedPageNos        []uint64                   `json:"completed_page_nos,omitempty"`
	RetryPageWindows        []MutationPageWindowRecord `json:"retry_page_windows,omitempty"`
	RetiredPhysicalChunkIDs []uint64                   `json:"retired_physical_chunk_ids,omitempty"`
	StartedAtUnix           int64                      `json:"started_at_unix,omitempty"`
	LastUpdatedAtUnix       int64                      `json:"last_updated_at_unix,omitempty"`
	ErrorMessage            string                     `json:"error_message,omitempty"`
}

type NodeHealthDetailRecord struct {
	NodeID                         string          `json:"node_id"`
	LastProbeUnix                  int64           `json:"last_probe_unix,omitempty"`
	LastProbeError                 string          `json:"last_probe_error,omitempty"`
	ConsecutiveProbeFailures       uint32          `json:"consecutive_probe_failures,omitempty"`
	ConsecutiveProbeSuccesses      uint32          `json:"consecutive_probe_successes,omitempty"`
	HealthReason                   string          `json:"health_reason,omitempty"`
	HealthUpdatedBy                HealthUpdatedBy `json:"health_updated_by,omitempty"`
	OverrideExpiresAtUnix          int64           `json:"override_expires_at_unix,omitempty"`
	RecoveryEligibleAtUnix         int64           `json:"recovery_eligible_at_unix,omitempty"`
	StoreCount                     int             `json:"store_count,omitempty"`
	HealthyStoreCount              int             `json:"healthy_store_count,omitempty"`
	WritableStoreCount             int             `json:"writable_store_count,omitempty"`
	AllocatableStoreCount          int             `json:"allocatable_store_count,omitempty"`
	StoreAllocationWeightTotal     int             `json:"store_allocation_weight_total,omitempty"`
	StoreAllocationWeightObserved  bool            `json:"store_allocation_weight_observed,omitempty"`
	StoreCapacityBytes             uint64          `json:"store_capacity_bytes,omitempty"`
	StoreAvailableBytes            uint64          `json:"store_available_bytes,omitempty"`
	StoreUsedBytes                 uint64          `json:"store_used_bytes,omitempty"`
	StoreCompactionPendingBytes    uint64          `json:"store_compaction_pending_bytes,omitempty"`
	StoreCompactionInProgressBytes uint64          `json:"store_compaction_in_progress_bytes,omitempty"`
}

func (r NodeHealthDetailRecord) StorePlacementEligible() bool {
	if r.StoreCount == 0 {
		return true
	}
	if r.WritableStoreCount == 0 {
		return false
	}
	if r.StoreAllocationWeightObserved && (r.AllocatableStoreCount == 0 || r.StoreAllocationWeightTotal <= 0) {
		return false
	}
	return r.StoreCapacityBytes == 0 || r.StoreAvailableBytes > 0
}
