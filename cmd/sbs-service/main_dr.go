package main

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	adminv1 "github.com/nosway/namrbd/sbs/admin/v1"
	clustermeta "github.com/nosway/namrbd/sbs/cluster/metadata"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	drReplicationLinkStateConfigured = "configured"
	drRecoveryPointStateRecorded     = "recorded"
	drShippingManifestStateBound     = "bound"
	drShippingManifestStateRecorded  = "recorded"
	drShippingWorkerStateAdmitted    = "admitted"
	drShippingWorkerStateHeartbeat   = "heartbeat_observed"
	drStandbyVolumeStateReadOnly     = "imported_read_only"
	drStandbyVolumeStatePromoted     = "promoted_writable"
	drStandbyVolumeStateDemoted      = "demoted_read_only"
	drStandbyWriteRejectionReason    = "dr_standby_read_only"
	drOldPrimaryWriteRejectionReason = "old_primary_fenced"
	drOldPrimaryRejoinPolicyExplicit = "explicit_fenced_rejoin_required"
	drFailoverDrillStateValidated    = "control_plane_validated"
	drDefaultRecoveryPointPolicy     = "manual"
	drDefaultShippingMode            = "manual"
)

type drJSONStore interface {
	Get(context.Context, string) ([]byte, bool, error)
	Set(context.Context, string, []byte) error
}

type drReplicationLinkRecord struct {
	ReplicationLinkID         string `json:"replication_link_id"`
	SourceClusterID           string `json:"source_cluster_id"`
	TargetClusterID           string `json:"target_cluster_id"`
	SourceVolumeID            string `json:"source_volume_id"`
	TargetVolumeID            string `json:"target_volume_id"`
	ReplicationLinkGeneration uint64 `json:"replication_link_generation"`
	ReplicationLinkState      string `json:"replication_link_state"`
	RecoveryPointPolicy       string `json:"recovery_point_policy"`
	ShippingMode              string `json:"shipping_mode"`
	ShippingWorkerDeployed    bool   `json:"shipping_worker_deployed"`
	StandbyImportSupported    bool   `json:"standby_import_supported"`
	PromoteSupported          bool   `json:"promote_supported"`
	FailoverSupported         bool   `json:"failover_supported"`
	CreatedBy                 string `json:"created_by,omitempty"`
	CreatedReason             string `json:"created_reason,omitempty"`
	CreatedAtUnix             int64  `json:"created_at_unix"`
	UpdatedAtUnix             int64  `json:"updated_at_unix"`
}

type drRecoveryPointRecord struct {
	RecoveryPointID              string `json:"recovery_point_id"`
	ReplicationLinkID            string `json:"replication_link_id"`
	SourceClusterID              string `json:"source_cluster_id"`
	TargetClusterID              string `json:"target_cluster_id"`
	SourceVolumeID               string `json:"source_volume_id"`
	TargetVolumeID               string `json:"target_volume_id"`
	SourceSnapshotID             string `json:"source_snapshot_id"`
	SourceSnapshotRootID         string `json:"source_snapshot_root_id"`
	ConsistencyPointID           string `json:"consistency_point_id"`
	BackupArtifactID             string `json:"backup_artifact_id,omitempty"`
	RecoveryPointGeneration      uint64 `json:"recovery_point_generation"`
	RecoveryPointState           string `json:"recovery_point_state"`
	RemoteTransferWorkerDeployed bool   `json:"remote_transfer_worker_deployed"`
	CreatedBy                    string `json:"created_by,omitempty"`
	CreatedReason                string `json:"created_reason,omitempty"`
	CreatedAtUnix                int64  `json:"created_at_unix"`
	UpdatedAtUnix                int64  `json:"updated_at_unix"`
}

type drShippingManifestRecord struct {
	ShippedManifestID            string `json:"shipped_manifest_id"`
	ReplicationLinkID            string `json:"replication_link_id"`
	RecoveryPointID              string `json:"recovery_point_id"`
	SourceClusterID              string `json:"source_cluster_id"`
	TargetClusterID              string `json:"target_cluster_id"`
	SourceVolumeID               string `json:"source_volume_id"`
	TargetVolumeID               string `json:"target_volume_id"`
	SourceSnapshotID             string `json:"source_snapshot_id"`
	SourceSnapshotRootID         string `json:"source_snapshot_root_id"`
	BackupArtifactID             string `json:"backup_artifact_id,omitempty"`
	ManifestGeneration           uint64 `json:"manifest_generation"`
	ManifestState                string `json:"manifest_state"`
	ManifestDigest               string `json:"manifest_digest"`
	PayloadRootsDigest           string `json:"payload_roots_digest"`
	ReadViewDigest               string `json:"read_view_digest"`
	KeyPolicyDigest              string `json:"key_policy_digest"`
	GovernanceDigest             string `json:"governance_digest"`
	ManifestIntegrityVerified    bool   `json:"manifest_integrity_verified"`
	PayloadRootsBound            bool   `json:"payload_roots_bound"`
	ReadViewIdentityBound        bool   `json:"read_view_identity_bound"`
	KeyPolicyBound               bool   `json:"key_policy_bound"`
	GovernanceMetadataBound      bool   `json:"governance_metadata_bound"`
	RemoteTransferWorkerDeployed bool   `json:"remote_transfer_worker_deployed"`
	CreatedBy                    string `json:"created_by,omitempty"`
	CreatedReason                string `json:"created_reason,omitempty"`
	CreatedAtUnix                int64  `json:"created_at_unix"`
	UpdatedAtUnix                int64  `json:"updated_at_unix"`
}

type drShippingWorkerRecord struct {
	ShippingWorkerID             string `json:"shipping_worker_id"`
	ReplicationLinkID            string `json:"replication_link_id"`
	ShippedManifestID            string `json:"shipped_manifest_id"`
	RecoveryPointID              string `json:"recovery_point_id"`
	SourceClusterID              string `json:"source_cluster_id"`
	TargetClusterID              string `json:"target_cluster_id"`
	SourceVolumeID               string `json:"source_volume_id"`
	TargetVolumeID               string `json:"target_volume_id"`
	WorkerGeneration             uint64 `json:"worker_generation"`
	WorkerState                  string `json:"worker_state"`
	WorkerNodeID                 string `json:"worker_node_id"`
	SourceNodeID                 string `json:"source_node_id,omitempty"`
	TargetNodeID                 string `json:"target_node_id,omitempty"`
	TargetEndpoint               string `json:"target_endpoint"`
	CredentialBoundaryID         string `json:"credential_boundary_id"`
	TransferPlanDigest           string `json:"transfer_plan_digest"`
	ManifestIntegrityVerified    bool   `json:"manifest_integrity_verified"`
	PayloadRootsBound            bool   `json:"payload_roots_bound"`
	ReadViewIdentityBound        bool   `json:"read_view_identity_bound"`
	KeyPolicyBound               bool   `json:"key_policy_bound"`
	GovernanceMetadataBound      bool   `json:"governance_metadata_bound"`
	RemoteTransferWorkerDeployed bool   `json:"remote_transfer_worker_deployed"`
	RemoteTransferStarted        bool   `json:"remote_transfer_started"`
	RemoteTransferCompleted      bool   `json:"remote_transfer_completed"`
	LastHeartbeatMessage         string `json:"last_heartbeat_message,omitempty"`
	LastHeartbeatAtUnix          int64  `json:"last_heartbeat_at_unix,omitempty"`
	CreatedBy                    string `json:"created_by,omitempty"`
	CreatedReason                string `json:"created_reason,omitempty"`
	CreatedAtUnix                int64  `json:"created_at_unix"`
	UpdatedAtUnix                int64  `json:"updated_at_unix"`
}

type drStandbyVolumeRecord struct {
	StandbyVolumeID                  string `json:"standby_volume_id"`
	ReplicationLinkID                string `json:"replication_link_id"`
	ShippedManifestID                string `json:"shipped_manifest_id"`
	RecoveryPointID                  string `json:"recovery_point_id"`
	ShippingWorkerID                 string `json:"shipping_worker_id"`
	SourceClusterID                  string `json:"source_cluster_id"`
	TargetClusterID                  string `json:"target_cluster_id"`
	SourceVolumeID                   string `json:"source_volume_id"`
	TargetVolumeID                   string `json:"target_volume_id"`
	SourceSnapshotID                 string `json:"source_snapshot_id"`
	SourceSnapshotRootID             string `json:"source_snapshot_root_id"`
	BackupArtifactID                 string `json:"backup_artifact_id,omitempty"`
	StandbyGeneration                uint64 `json:"standby_generation"`
	StandbyState                     string `json:"standby_state"`
	ImportNodeID                     string `json:"import_node_id"`
	ImportEndpoint                   string `json:"import_endpoint,omitempty"`
	ReadViewDigest                   string `json:"read_view_digest"`
	KeyPolicyDigest                  string `json:"key_policy_digest"`
	GovernanceDigest                 string `json:"governance_digest"`
	ManifestIntegrityVerified        bool   `json:"manifest_integrity_verified"`
	PayloadRootsBound                bool   `json:"payload_roots_bound"`
	ReadViewIdentityBound            bool   `json:"read_view_identity_bound"`
	KeyPolicyBound                   bool   `json:"key_policy_bound"`
	GovernanceMetadataBound          bool   `json:"governance_metadata_bound"`
	RemoteTransferWorkerDeployed     bool   `json:"remote_transfer_worker_deployed"`
	RemoteTransferStarted            bool   `json:"remote_transfer_started"`
	RemoteTransferCompleted          bool   `json:"remote_transfer_completed"`
	StandbyImportVerified            bool   `json:"standby_import_verified"`
	StandbyReadOnlyVerified          bool   `json:"standby_read_only_verified"`
	StandbyWriteRejectionRequired    bool   `json:"standby_write_rejection_required"`
	StandbyWriteRejected             bool   `json:"standby_write_rejected"`
	StandbyWriteRejectionReason      string `json:"standby_write_rejection_reason"`
	PromoteRequiredBeforeTargetWrite bool   `json:"promote_required_before_target_write"`
	TargetWriteAdmitted              bool   `json:"target_write_admitted"`
	LastWriteCheckAtUnix             int64  `json:"last_write_check_at_unix,omitempty"`
	PromoteGeneration                uint64 `json:"promote_generation"`
	DemoteGeneration                 uint64 `json:"demote_generation"`
	FencingEpoch                     uint64 `json:"fencing_epoch"`
	PromoteFencingVerified           bool   `json:"promote_fencing_verified"`
	DemoteFencingVerified            bool   `json:"demote_fencing_verified"`
	StaleOldPrimaryRejected          bool   `json:"stale_old_primary_rejected"`
	OldPrimaryWriteRejectionReason   string `json:"old_primary_write_rejection_reason,omitempty"`
	OldPrimaryRejoinPolicyDefined    bool   `json:"old_primary_rejoin_policy_defined"`
	OldPrimaryRejoinPolicy           string `json:"old_primary_rejoin_policy,omitempty"`
	LastPromoteAtUnix                int64  `json:"last_promote_at_unix,omitempty"`
	LastDemoteAtUnix                 int64  `json:"last_demote_at_unix,omitempty"`
	LastOldPrimaryWriteCheckAtUnix   int64  `json:"last_old_primary_write_check_at_unix,omitempty"`
	LastOldPrimaryRejoinPolicyAtUnix int64  `json:"last_old_primary_rejoin_policy_at_unix,omitempty"`
	CreatedBy                        string `json:"created_by,omitempty"`
	CreatedReason                    string `json:"created_reason,omitempty"`
	CreatedAtUnix                    int64  `json:"created_at_unix"`
	UpdatedAtUnix                    int64  `json:"updated_at_unix"`
}

type drFailoverDrillRecord struct {
	FailoverDrillID                    string `json:"failover_drill_id"`
	StandbyVolumeID                    string `json:"standby_volume_id"`
	ReplicationLinkID                  string `json:"replication_link_id"`
	RecoveryPointID                    string `json:"recovery_point_id"`
	ShippedManifestID                  string `json:"shipped_manifest_id"`
	ShippingWorkerID                   string `json:"shipping_worker_id"`
	SourceClusterID                    string `json:"source_cluster_id"`
	TargetClusterID                    string `json:"target_cluster_id"`
	SourceVolumeID                     string `json:"source_volume_id"`
	TargetVolumeID                     string `json:"target_volume_id"`
	FailoverDrillGeneration            uint64 `json:"failover_drill_generation"`
	FailoverDrillState                 string `json:"failover_drill_state"`
	SourceUnavailableObserved          bool   `json:"source_unavailable_observed"`
	TargetPromoteObserved              bool   `json:"target_promote_observed"`
	TargetMounted                      bool   `json:"target_mounted"`
	MountReadbackMatched               bool   `json:"mount_readback_matched"`
	ApplicationVisibleIdentityVerified bool   `json:"application_visible_identity_verified"`
	CleanupRejoinStateVerified         bool   `json:"cleanup_rejoin_state_verified"`
	OldPrimaryRejoinPolicyVerified     bool   `json:"old_primary_rejoin_policy_verified"`
	FailoverDrillCompleted             bool   `json:"failover_drill_completed"`
	PromoteGeneration                  uint64 `json:"promote_generation"`
	DemoteGeneration                   uint64 `json:"demote_generation"`
	FencingEpoch                       uint64 `json:"fencing_epoch"`
	StaleOldPrimaryRejected            bool   `json:"stale_old_primary_rejected"`
	OldPrimaryWriteRejectionReason     string `json:"old_primary_write_rejection_reason,omitempty"`
	OldPrimaryRejoinPolicyDefined      bool   `json:"old_primary_rejoin_policy_defined"`
	OldPrimaryRejoinPolicy             string `json:"old_primary_rejoin_policy,omitempty"`
	CreatedBy                          string `json:"created_by,omitempty"`
	CreatedReason                      string `json:"created_reason,omitempty"`
	CreatedAtUnix                      int64  `json:"created_at_unix"`
	UpdatedAtUnix                      int64  `json:"updated_at_unix"`
}

func (s *server) CreateDRReplicationLink(ctx context.Context, req *adminv1.CreateDRReplicationLinkRequest) (*adminv1.CreateDRReplicationLinkResponse, error) {
	cluster, err := s.clusterRef(req.GetCluster())
	if err != nil {
		return nil, err
	}
	linkID := strings.TrimSpace(req.GetReplicationLinkId())
	if linkID == "" {
		return nil, status.Error(codes.InvalidArgument, "replication_link_id is required")
	}
	sourceClusterID := strings.TrimSpace(req.GetSourceClusterId())
	if sourceClusterID == "" {
		sourceClusterID = cluster.GetClusterId()
	}
	targetClusterID := strings.TrimSpace(req.GetTargetClusterId())
	if targetClusterID == "" {
		return nil, status.Error(codes.InvalidArgument, "target_cluster_id is required")
	}
	sourceVolumeID := strings.TrimSpace(req.GetSourceVolumeId())
	if sourceVolumeID == "" {
		return nil, status.Error(codes.InvalidArgument, "source_volume_id is required")
	}
	targetVolumeID := strings.TrimSpace(req.GetTargetVolumeId())
	if targetVolumeID == "" {
		return nil, status.Error(codes.InvalidArgument, "target_volume_id is required")
	}
	recoveryPointPolicy := strings.TrimSpace(req.GetRecoveryPointPolicy())
	if recoveryPointPolicy == "" {
		recoveryPointPolicy = drDefaultRecoveryPointPolicy
	}
	shippingMode := strings.TrimSpace(req.GetShippingMode())
	if shippingMode == "" {
		shippingMode = drDefaultShippingMode
	}

	now := time.Now().UTC().Unix()
	var rec drReplicationLinkRecord
	err = clustermeta.RunInTransaction(ctx, s.kv, func(tx clustermeta.ReadWriter) error {
		existing, err := getDRJSON[drReplicationLinkRecord](ctx, tx, drReplicationLinkKey(s.root, linkID))
		if err != nil && !errorsIsNotFound(err) {
			return err
		}
		createdAt := now
		createdBy := req.GetMeta().GetActor()
		createdReason := req.GetMeta().GetReason()
		generation := uint64(1)
		shippingWorkerDeployed := false
		standbyImportSupported := false
		promoteSupported := false
		failoverSupported := false
		if err == nil && existing.CreatedAtUnix > 0 {
			createdAt = existing.CreatedAtUnix
			createdBy = existing.CreatedBy
			createdReason = existing.CreatedReason
			generation = existing.ReplicationLinkGeneration + 1
			shippingWorkerDeployed = existing.ShippingWorkerDeployed
			standbyImportSupported = existing.StandbyImportSupported
			promoteSupported = existing.PromoteSupported
			failoverSupported = existing.FailoverSupported
		}
		rec = drReplicationLinkRecord{
			ReplicationLinkID:         linkID,
			SourceClusterID:           sourceClusterID,
			TargetClusterID:           targetClusterID,
			SourceVolumeID:            sourceVolumeID,
			TargetVolumeID:            targetVolumeID,
			ReplicationLinkGeneration: generation,
			ReplicationLinkState:      drReplicationLinkStateConfigured,
			RecoveryPointPolicy:       recoveryPointPolicy,
			ShippingMode:              shippingMode,
			ShippingWorkerDeployed:    shippingWorkerDeployed,
			StandbyImportSupported:    standbyImportSupported,
			PromoteSupported:          promoteSupported,
			FailoverSupported:         failoverSupported,
			CreatedBy:                 createdBy,
			CreatedReason:             createdReason,
			CreatedAtUnix:             createdAt,
			UpdatedAtUnix:             now,
		}
		return putDRJSON(ctx, tx, drReplicationLinkKey(s.root, linkID), rec)
	})
	if err != nil {
		if _, ok := status.FromError(err); ok {
			return nil, err
		}
		return nil, status.Errorf(codes.Internal, "persist dr replication link: %v", err)
	}
	op, err := s.ops.create("dr.replication_link.create", "", sourceVolumeID, "replication-link-persisted", adminv1.OperationState_OPERATION_STATE_COMPLETED)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "record operation: %v", err)
	}
	return &adminv1.CreateDRReplicationLinkResponse{
		Cluster:   cluster,
		Operation: acceptedOperation(op, "dr replication link persisted"),
		Link:      rec.toProto(),
	}, nil
}

func (s *server) GetDRReplicationLink(ctx context.Context, req *adminv1.GetDRReplicationLinkRequest) (*adminv1.GetDRReplicationLinkResponse, error) {
	cluster, err := s.clusterRef(req.GetCluster())
	if err != nil {
		return nil, err
	}
	rec, err := s.getDRReplicationLinkRecord(ctx, req.GetReplicationLinkId())
	if err != nil {
		return nil, err
	}
	return &adminv1.GetDRReplicationLinkResponse{Cluster: cluster, Link: rec.toProto()}, nil
}

func (s *server) ListDRReplicationLinks(ctx context.Context, req *adminv1.ListDRReplicationLinksRequest) (*adminv1.ListDRReplicationLinksResponse, error) {
	cluster, err := s.clusterRef(req.GetCluster())
	if err != nil {
		return nil, err
	}
	records, err := s.listDRReplicationLinkRecords(ctx)
	if err != nil {
		return nil, err
	}
	sourceVolumeID := strings.TrimSpace(req.GetSourceVolumeId())
	targetClusterID := strings.TrimSpace(req.GetTargetClusterId())
	out := make([]*adminv1.DRReplicationLinkSummary, 0, len(records))
	for _, rec := range records {
		if sourceVolumeID != "" && rec.SourceVolumeID != sourceVolumeID {
			continue
		}
		if targetClusterID != "" && rec.TargetClusterID != targetClusterID {
			continue
		}
		out = append(out, rec.toProto())
	}
	return &adminv1.ListDRReplicationLinksResponse{Cluster: cluster, Links: out}, nil
}

func (s *server) CreateDRRecoveryPoint(ctx context.Context, req *adminv1.CreateDRRecoveryPointRequest) (*adminv1.CreateDRRecoveryPointResponse, error) {
	cluster, err := s.clusterRef(req.GetCluster())
	if err != nil {
		return nil, err
	}
	recoveryPointID := strings.TrimSpace(req.GetRecoveryPointId())
	if recoveryPointID == "" {
		return nil, status.Error(codes.InvalidArgument, "recovery_point_id is required")
	}
	linkID := strings.TrimSpace(req.GetReplicationLinkId())
	if linkID == "" {
		return nil, status.Error(codes.InvalidArgument, "replication_link_id is required")
	}
	sourceSnapshotID := strings.TrimSpace(req.GetSourceSnapshotId())
	if sourceSnapshotID == "" {
		return nil, status.Error(codes.InvalidArgument, "source_snapshot_id is required")
	}
	sourceSnapshotRootID := strings.TrimSpace(req.GetSourceSnapshotRootId())
	if sourceSnapshotRootID == "" {
		return nil, status.Error(codes.InvalidArgument, "source_snapshot_root_id is required")
	}
	consistencyPointID := strings.TrimSpace(req.GetConsistencyPointId())
	if consistencyPointID == "" {
		return nil, status.Error(codes.InvalidArgument, "consistency_point_id is required")
	}
	link, err := s.getDRReplicationLinkRecord(ctx, linkID)
	if err != nil {
		return nil, err
	}

	now := time.Now().UTC().Unix()
	var rec drRecoveryPointRecord
	err = clustermeta.RunInTransaction(ctx, s.kv, func(tx clustermeta.ReadWriter) error {
		existing, err := getDRJSON[drRecoveryPointRecord](ctx, tx, drRecoveryPointKey(s.root, recoveryPointID))
		if err != nil && !errorsIsNotFound(err) {
			return err
		}
		createdAt := now
		createdBy := req.GetMeta().GetActor()
		createdReason := req.GetMeta().GetReason()
		generation := uint64(1)
		remoteTransferWorkerDeployed := false
		if err == nil && existing.CreatedAtUnix > 0 {
			if existing.ReplicationLinkID != link.ReplicationLinkID {
				return status.Error(codes.InvalidArgument, "replication_link_id cannot change for an existing recovery point")
			}
			createdAt = existing.CreatedAtUnix
			createdBy = existing.CreatedBy
			createdReason = existing.CreatedReason
			generation = existing.RecoveryPointGeneration + 1
			remoteTransferWorkerDeployed = existing.RemoteTransferWorkerDeployed
		}
		rec = drRecoveryPointRecord{
			RecoveryPointID:              recoveryPointID,
			ReplicationLinkID:            link.ReplicationLinkID,
			SourceClusterID:              link.SourceClusterID,
			TargetClusterID:              link.TargetClusterID,
			SourceVolumeID:               link.SourceVolumeID,
			TargetVolumeID:               link.TargetVolumeID,
			SourceSnapshotID:             sourceSnapshotID,
			SourceSnapshotRootID:         sourceSnapshotRootID,
			ConsistencyPointID:           consistencyPointID,
			BackupArtifactID:             strings.TrimSpace(req.GetBackupArtifactId()),
			RecoveryPointGeneration:      generation,
			RecoveryPointState:           drRecoveryPointStateRecorded,
			RemoteTransferWorkerDeployed: remoteTransferWorkerDeployed,
			CreatedBy:                    createdBy,
			CreatedReason:                createdReason,
			CreatedAtUnix:                createdAt,
			UpdatedAtUnix:                now,
		}
		return putDRJSON(ctx, tx, drRecoveryPointKey(s.root, recoveryPointID), rec)
	})
	if err != nil {
		if _, ok := status.FromError(err); ok {
			return nil, err
		}
		return nil, status.Errorf(codes.Internal, "persist dr recovery point: %v", err)
	}
	op, err := s.ops.create("dr.recovery_point.create", "", rec.SourceVolumeID, "recovery-point-persisted", adminv1.OperationState_OPERATION_STATE_COMPLETED)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "record operation: %v", err)
	}
	return &adminv1.CreateDRRecoveryPointResponse{
		Cluster:       cluster,
		Operation:     acceptedOperation(op, "dr recovery point persisted"),
		RecoveryPoint: rec.toProto(),
	}, nil
}

func (s *server) GetDRRecoveryPoint(ctx context.Context, req *adminv1.GetDRRecoveryPointRequest) (*adminv1.GetDRRecoveryPointResponse, error) {
	cluster, err := s.clusterRef(req.GetCluster())
	if err != nil {
		return nil, err
	}
	rec, err := s.getDRRecoveryPointRecord(ctx, req.GetRecoveryPointId())
	if err != nil {
		return nil, err
	}
	return &adminv1.GetDRRecoveryPointResponse{Cluster: cluster, RecoveryPoint: rec.toProto()}, nil
}

func (s *server) ListDRRecoveryPoints(ctx context.Context, req *adminv1.ListDRRecoveryPointsRequest) (*adminv1.ListDRRecoveryPointsResponse, error) {
	cluster, err := s.clusterRef(req.GetCluster())
	if err != nil {
		return nil, err
	}
	records, err := s.listDRRecoveryPointRecords(ctx)
	if err != nil {
		return nil, err
	}
	linkID := strings.TrimSpace(req.GetReplicationLinkId())
	sourceVolumeID := strings.TrimSpace(req.GetSourceVolumeId())
	out := make([]*adminv1.DRRecoveryPointSummary, 0, len(records))
	for _, rec := range records {
		if linkID != "" && rec.ReplicationLinkID != linkID {
			continue
		}
		if sourceVolumeID != "" && rec.SourceVolumeID != sourceVolumeID {
			continue
		}
		out = append(out, rec.toProto())
	}
	return &adminv1.ListDRRecoveryPointsResponse{Cluster: cluster, RecoveryPoints: out}, nil
}

func (s *server) CreateDRShippingManifest(ctx context.Context, req *adminv1.CreateDRShippingManifestRequest) (*adminv1.CreateDRShippingManifestResponse, error) {
	cluster, err := s.clusterRef(req.GetCluster())
	if err != nil {
		return nil, err
	}
	manifestID := strings.TrimSpace(req.GetShippedManifestId())
	if manifestID == "" {
		return nil, status.Error(codes.InvalidArgument, "shipped_manifest_id is required")
	}
	recoveryPointID := strings.TrimSpace(req.GetRecoveryPointId())
	if recoveryPointID == "" {
		return nil, status.Error(codes.InvalidArgument, "recovery_point_id is required")
	}
	manifestDigest := strings.TrimSpace(req.GetManifestDigest())
	if manifestDigest == "" {
		return nil, status.Error(codes.InvalidArgument, "manifest_digest is required")
	}
	payloadRootsDigest := strings.TrimSpace(req.GetPayloadRootsDigest())
	if payloadRootsDigest == "" {
		return nil, status.Error(codes.InvalidArgument, "payload_roots_digest is required")
	}
	readViewDigest := strings.TrimSpace(req.GetReadViewDigest())
	if readViewDigest == "" {
		return nil, status.Error(codes.InvalidArgument, "read_view_digest is required")
	}
	keyPolicyDigest := strings.TrimSpace(req.GetKeyPolicyDigest())
	if keyPolicyDigest == "" {
		return nil, status.Error(codes.InvalidArgument, "key_policy_digest is required")
	}
	governanceDigest := strings.TrimSpace(req.GetGovernanceDigest())
	if governanceDigest == "" {
		return nil, status.Error(codes.InvalidArgument, "governance_digest is required")
	}
	recoveryPoint, err := s.getDRRecoveryPointRecord(ctx, recoveryPointID)
	if err != nil {
		return nil, err
	}

	now := time.Now().UTC().Unix()
	var rec drShippingManifestRecord
	err = clustermeta.RunInTransaction(ctx, s.kv, func(tx clustermeta.ReadWriter) error {
		existing, err := getDRJSON[drShippingManifestRecord](ctx, tx, drShippingManifestKey(s.root, manifestID))
		if err != nil && !errorsIsNotFound(err) {
			return err
		}
		createdAt := now
		createdBy := req.GetMeta().GetActor()
		createdReason := req.GetMeta().GetReason()
		generation := uint64(1)
		remoteTransferWorkerDeployed := false
		manifestState := drShippingManifestState(req)
		if err == nil && existing.CreatedAtUnix > 0 {
			if existing.RecoveryPointID != recoveryPoint.RecoveryPointID {
				return status.Error(codes.InvalidArgument, "recovery_point_id cannot change for an existing shipping manifest")
			}
			if existing.RemoteTransferWorkerDeployed && manifestState != drShippingManifestStateBound {
				return status.Error(codes.FailedPrecondition, "bound manifest cannot be downgraded after shipping worker admission")
			}
			createdAt = existing.CreatedAtUnix
			createdBy = existing.CreatedBy
			createdReason = existing.CreatedReason
			generation = existing.ManifestGeneration + 1
			remoteTransferWorkerDeployed = existing.RemoteTransferWorkerDeployed
		}
		rec = drShippingManifestRecord{
			ShippedManifestID:            manifestID,
			ReplicationLinkID:            recoveryPoint.ReplicationLinkID,
			RecoveryPointID:              recoveryPoint.RecoveryPointID,
			SourceClusterID:              recoveryPoint.SourceClusterID,
			TargetClusterID:              recoveryPoint.TargetClusterID,
			SourceVolumeID:               recoveryPoint.SourceVolumeID,
			TargetVolumeID:               recoveryPoint.TargetVolumeID,
			SourceSnapshotID:             recoveryPoint.SourceSnapshotID,
			SourceSnapshotRootID:         recoveryPoint.SourceSnapshotRootID,
			BackupArtifactID:             recoveryPoint.BackupArtifactID,
			ManifestGeneration:           generation,
			ManifestState:                manifestState,
			ManifestDigest:               manifestDigest,
			PayloadRootsDigest:           payloadRootsDigest,
			ReadViewDigest:               readViewDigest,
			KeyPolicyDigest:              keyPolicyDigest,
			GovernanceDigest:             governanceDigest,
			ManifestIntegrityVerified:    req.GetManifestIntegrityVerified(),
			PayloadRootsBound:            req.GetPayloadRootsBound(),
			ReadViewIdentityBound:        req.GetReadViewIdentityBound(),
			KeyPolicyBound:               req.GetKeyPolicyBound(),
			GovernanceMetadataBound:      req.GetGovernanceMetadataBound(),
			RemoteTransferWorkerDeployed: remoteTransferWorkerDeployed,
			CreatedBy:                    createdBy,
			CreatedReason:                createdReason,
			CreatedAtUnix:                createdAt,
			UpdatedAtUnix:                now,
		}
		return putDRJSON(ctx, tx, drShippingManifestKey(s.root, manifestID), rec)
	})
	if err != nil {
		if _, ok := status.FromError(err); ok {
			return nil, err
		}
		return nil, status.Errorf(codes.Internal, "persist dr shipping manifest: %v", err)
	}
	op, err := s.ops.create("dr.shipping_manifest.create", "", rec.SourceVolumeID, "shipping-manifest-persisted", adminv1.OperationState_OPERATION_STATE_COMPLETED)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "record operation: %v", err)
	}
	return &adminv1.CreateDRShippingManifestResponse{
		Cluster:   cluster,
		Operation: acceptedOperation(op, "dr shipping manifest persisted"),
		Manifest:  rec.toProto(),
	}, nil
}

func (s *server) GetDRShippingManifest(ctx context.Context, req *adminv1.GetDRShippingManifestRequest) (*adminv1.GetDRShippingManifestResponse, error) {
	cluster, err := s.clusterRef(req.GetCluster())
	if err != nil {
		return nil, err
	}
	rec, err := s.getDRShippingManifestRecord(ctx, req.GetShippedManifestId())
	if err != nil {
		return nil, err
	}
	return &adminv1.GetDRShippingManifestResponse{Cluster: cluster, Manifest: rec.toProto()}, nil
}

func (s *server) ListDRShippingManifests(ctx context.Context, req *adminv1.ListDRShippingManifestsRequest) (*adminv1.ListDRShippingManifestsResponse, error) {
	cluster, err := s.clusterRef(req.GetCluster())
	if err != nil {
		return nil, err
	}
	records, err := s.listDRShippingManifestRecords(ctx)
	if err != nil {
		return nil, err
	}
	linkID := strings.TrimSpace(req.GetReplicationLinkId())
	recoveryPointID := strings.TrimSpace(req.GetRecoveryPointId())
	out := make([]*adminv1.DRShippingManifestSummary, 0, len(records))
	for _, rec := range records {
		if linkID != "" && rec.ReplicationLinkID != linkID {
			continue
		}
		if recoveryPointID != "" && rec.RecoveryPointID != recoveryPointID {
			continue
		}
		out = append(out, rec.toProto())
	}
	return &adminv1.ListDRShippingManifestsResponse{Cluster: cluster, Manifests: out}, nil
}

func (s *server) AdmitDRShippingWorker(ctx context.Context, req *adminv1.AdmitDRShippingWorkerRequest) (*adminv1.AdmitDRShippingWorkerResponse, error) {
	cluster, err := s.clusterRef(req.GetCluster())
	if err != nil {
		return nil, err
	}
	workerID := strings.TrimSpace(req.GetShippingWorkerId())
	if workerID == "" {
		return nil, status.Error(codes.InvalidArgument, "shipping_worker_id is required")
	}
	manifestID := strings.TrimSpace(req.GetShippedManifestId())
	if manifestID == "" {
		return nil, status.Error(codes.InvalidArgument, "shipped_manifest_id is required")
	}
	workerNodeID := strings.TrimSpace(req.GetWorkerNodeId())
	if workerNodeID == "" {
		return nil, status.Error(codes.InvalidArgument, "worker_node_id is required")
	}
	targetEndpoint := strings.TrimSpace(req.GetTargetEndpoint())
	if targetEndpoint == "" {
		return nil, status.Error(codes.InvalidArgument, "target_endpoint is required")
	}
	credentialBoundaryID := strings.TrimSpace(req.GetCredentialBoundaryId())
	if credentialBoundaryID == "" {
		return nil, status.Error(codes.InvalidArgument, "credential_boundary_id is required")
	}
	transferPlanDigest := strings.TrimSpace(req.GetTransferPlanDigest())
	if transferPlanDigest == "" {
		return nil, status.Error(codes.InvalidArgument, "transfer_plan_digest is required")
	}

	now := time.Now().UTC().Unix()
	var worker drShippingWorkerRecord
	var link drReplicationLinkRecord
	var recoveryPoint drRecoveryPointRecord
	var manifest drShippingManifestRecord
	err = clustermeta.RunInTransaction(ctx, s.kv, func(tx clustermeta.ReadWriter) error {
		var err error
		manifest, err = getDRJSON[drShippingManifestRecord](ctx, tx, drShippingManifestKey(s.root, manifestID))
		if err != nil {
			return notFoundStatus(err, "dr shipping manifest not found")
		}
		if manifest.ManifestState != drShippingManifestStateBound ||
			!manifest.ManifestIntegrityVerified ||
			!manifest.PayloadRootsBound ||
			!manifest.ReadViewIdentityBound ||
			!manifest.KeyPolicyBound ||
			!manifest.GovernanceMetadataBound {
			return status.Error(codes.FailedPrecondition, "shipping worker admission requires a bound shipping manifest")
		}
		link, err = getDRJSON[drReplicationLinkRecord](ctx, tx, drReplicationLinkKey(s.root, manifest.ReplicationLinkID))
		if err != nil {
			return notFoundStatus(err, "dr replication link not found")
		}
		recoveryPoint, err = getDRJSON[drRecoveryPointRecord](ctx, tx, drRecoveryPointKey(s.root, manifest.RecoveryPointID))
		if err != nil {
			return notFoundStatus(err, "dr recovery point not found")
		}

		existing, err := getDRJSON[drShippingWorkerRecord](ctx, tx, drShippingWorkerKey(s.root, workerID))
		if err != nil && !errorsIsNotFound(err) {
			return err
		}
		createdAt := now
		createdBy := req.GetMeta().GetActor()
		createdReason := req.GetMeta().GetReason()
		generation := uint64(1)
		lastHeartbeatMessage := ""
		lastHeartbeatAt := int64(0)
		remoteTransferStarted := false
		if err == nil && existing.CreatedAtUnix > 0 {
			if existing.ShippedManifestID != manifest.ShippedManifestID {
				return status.Error(codes.InvalidArgument, "shipped_manifest_id cannot change for an existing shipping worker")
			}
			createdAt = existing.CreatedAtUnix
			createdBy = existing.CreatedBy
			createdReason = existing.CreatedReason
			generation = existing.WorkerGeneration + 1
			lastHeartbeatMessage = existing.LastHeartbeatMessage
			lastHeartbeatAt = existing.LastHeartbeatAtUnix
			remoteTransferStarted = existing.RemoteTransferStarted
		}
		worker = drShippingWorkerRecord{
			ShippingWorkerID:             workerID,
			ReplicationLinkID:            manifest.ReplicationLinkID,
			ShippedManifestID:            manifest.ShippedManifestID,
			RecoveryPointID:              manifest.RecoveryPointID,
			SourceClusterID:              manifest.SourceClusterID,
			TargetClusterID:              manifest.TargetClusterID,
			SourceVolumeID:               manifest.SourceVolumeID,
			TargetVolumeID:               manifest.TargetVolumeID,
			WorkerGeneration:             generation,
			WorkerState:                  drShippingWorkerStateAdmitted,
			WorkerNodeID:                 workerNodeID,
			SourceNodeID:                 strings.TrimSpace(req.GetSourceNodeId()),
			TargetNodeID:                 strings.TrimSpace(req.GetTargetNodeId()),
			TargetEndpoint:               targetEndpoint,
			CredentialBoundaryID:         credentialBoundaryID,
			TransferPlanDigest:           transferPlanDigest,
			ManifestIntegrityVerified:    manifest.ManifestIntegrityVerified,
			PayloadRootsBound:            manifest.PayloadRootsBound,
			ReadViewIdentityBound:        manifest.ReadViewIdentityBound,
			KeyPolicyBound:               manifest.KeyPolicyBound,
			GovernanceMetadataBound:      manifest.GovernanceMetadataBound,
			RemoteTransferWorkerDeployed: true,
			RemoteTransferStarted:        remoteTransferStarted,
			RemoteTransferCompleted:      false,
			LastHeartbeatMessage:         lastHeartbeatMessage,
			LastHeartbeatAtUnix:          lastHeartbeatAt,
			CreatedBy:                    createdBy,
			CreatedReason:                createdReason,
			CreatedAtUnix:                createdAt,
			UpdatedAtUnix:                now,
		}

		link.ShippingWorkerDeployed = true
		link.ReplicationLinkGeneration++
		link.UpdatedAtUnix = now
		recoveryPoint.RemoteTransferWorkerDeployed = true
		recoveryPoint.RecoveryPointGeneration++
		recoveryPoint.UpdatedAtUnix = now
		manifest.RemoteTransferWorkerDeployed = true
		manifest.ManifestGeneration++
		manifest.UpdatedAtUnix = now

		if err := putDRJSON(ctx, tx, drShippingWorkerKey(s.root, workerID), worker); err != nil {
			return err
		}
		if err := putDRJSON(ctx, tx, drReplicationLinkKey(s.root, link.ReplicationLinkID), link); err != nil {
			return err
		}
		if err := putDRJSON(ctx, tx, drRecoveryPointKey(s.root, recoveryPoint.RecoveryPointID), recoveryPoint); err != nil {
			return err
		}
		return putDRJSON(ctx, tx, drShippingManifestKey(s.root, manifest.ShippedManifestID), manifest)
	})
	if err != nil {
		if _, ok := status.FromError(err); ok {
			return nil, err
		}
		return nil, status.Errorf(codes.Internal, "persist dr shipping worker admission: %v", err)
	}
	op, err := s.ops.create("dr.shipping_worker.admit", "", worker.SourceVolumeID, "shipping-worker-admitted", adminv1.OperationState_OPERATION_STATE_COMPLETED)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "record operation: %v", err)
	}
	return &adminv1.AdmitDRShippingWorkerResponse{
		Cluster:       cluster,
		Operation:     acceptedOperation(op, "dr shipping worker admitted"),
		Worker:        worker.toProto(),
		Link:          link.toProto(),
		RecoveryPoint: recoveryPoint.toProto(),
		Manifest:      manifest.toProto(),
	}, nil
}

func (s *server) HeartbeatDRShippingWorker(ctx context.Context, req *adminv1.HeartbeatDRShippingWorkerRequest) (*adminv1.HeartbeatDRShippingWorkerResponse, error) {
	cluster, err := s.clusterRef(req.GetCluster())
	if err != nil {
		return nil, err
	}
	workerID := strings.TrimSpace(req.GetShippingWorkerId())
	if workerID == "" {
		return nil, status.Error(codes.InvalidArgument, "shipping_worker_id is required")
	}
	if req.GetRemoteTransferCompleted() {
		return nil, status.Error(codes.FailedPrecondition, "remote transfer completion requires a later remote shipping evidence slice")
	}
	observedState := strings.TrimSpace(req.GetObservedState())
	if observedState == "" {
		observedState = drShippingWorkerStateHeartbeat
	}
	now := time.Now().UTC().Unix()
	var rec drShippingWorkerRecord
	err = clustermeta.RunInTransaction(ctx, s.kv, func(tx clustermeta.ReadWriter) error {
		var err error
		rec, err = getDRJSON[drShippingWorkerRecord](ctx, tx, drShippingWorkerKey(s.root, workerID))
		if err != nil {
			return notFoundStatus(err, "dr shipping worker not found")
		}
		rec.WorkerGeneration++
		rec.WorkerState = observedState
		rec.RemoteTransferStarted = rec.RemoteTransferStarted || req.GetRemoteTransferStarted()
		rec.RemoteTransferCompleted = false
		rec.LastHeartbeatMessage = strings.TrimSpace(req.GetHeartbeatMessage())
		rec.LastHeartbeatAtUnix = now
		rec.UpdatedAtUnix = now
		return putDRJSON(ctx, tx, drShippingWorkerKey(s.root, workerID), rec)
	})
	if err != nil {
		if _, ok := status.FromError(err); ok {
			return nil, err
		}
		return nil, status.Errorf(codes.Internal, "persist dr shipping worker heartbeat: %v", err)
	}
	op, err := s.ops.create("dr.shipping_worker.heartbeat", "", rec.SourceVolumeID, "shipping-worker-heartbeat-observed", adminv1.OperationState_OPERATION_STATE_COMPLETED)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "record operation: %v", err)
	}
	return &adminv1.HeartbeatDRShippingWorkerResponse{
		Cluster:   cluster,
		Operation: acceptedOperation(op, "dr shipping worker heartbeat observed"),
		Worker:    rec.toProto(),
	}, nil
}

func (s *server) GetDRShippingWorker(ctx context.Context, req *adminv1.GetDRShippingWorkerRequest) (*adminv1.GetDRShippingWorkerResponse, error) {
	cluster, err := s.clusterRef(req.GetCluster())
	if err != nil {
		return nil, err
	}
	rec, err := s.getDRShippingWorkerRecord(ctx, req.GetShippingWorkerId())
	if err != nil {
		return nil, err
	}
	return &adminv1.GetDRShippingWorkerResponse{Cluster: cluster, Worker: rec.toProto()}, nil
}

func (s *server) ListDRShippingWorkers(ctx context.Context, req *adminv1.ListDRShippingWorkersRequest) (*adminv1.ListDRShippingWorkersResponse, error) {
	cluster, err := s.clusterRef(req.GetCluster())
	if err != nil {
		return nil, err
	}
	records, err := s.listDRShippingWorkerRecords(ctx)
	if err != nil {
		return nil, err
	}
	linkID := strings.TrimSpace(req.GetReplicationLinkId())
	manifestID := strings.TrimSpace(req.GetShippedManifestId())
	out := make([]*adminv1.DRShippingWorkerSummary, 0, len(records))
	for _, rec := range records {
		if linkID != "" && rec.ReplicationLinkID != linkID {
			continue
		}
		if manifestID != "" && rec.ShippedManifestID != manifestID {
			continue
		}
		out = append(out, rec.toProto())
	}
	return &adminv1.ListDRShippingWorkersResponse{Cluster: cluster, Workers: out}, nil
}

func (s *server) ImportDRStandbyVolume(ctx context.Context, req *adminv1.ImportDRStandbyVolumeRequest) (*adminv1.ImportDRStandbyVolumeResponse, error) {
	cluster, err := s.clusterRef(req.GetCluster())
	if err != nil {
		return nil, err
	}
	standbyID := strings.TrimSpace(req.GetStandbyVolumeId())
	if standbyID == "" {
		return nil, status.Error(codes.InvalidArgument, "standby_volume_id is required")
	}
	manifestID := strings.TrimSpace(req.GetShippedManifestId())
	if manifestID == "" {
		return nil, status.Error(codes.InvalidArgument, "shipped_manifest_id is required")
	}
	workerID := strings.TrimSpace(req.GetShippingWorkerId())
	if workerID == "" {
		return nil, status.Error(codes.InvalidArgument, "shipping_worker_id is required")
	}
	importNodeID := strings.TrimSpace(req.GetImportNodeId())
	if importNodeID == "" {
		return nil, status.Error(codes.InvalidArgument, "import_node_id is required")
	}

	now := time.Now().UTC().Unix()
	var standby drStandbyVolumeRecord
	var link drReplicationLinkRecord
	var recoveryPoint drRecoveryPointRecord
	var manifest drShippingManifestRecord
	var worker drShippingWorkerRecord
	err = clustermeta.RunInTransaction(ctx, s.kv, func(tx clustermeta.ReadWriter) error {
		var err error
		manifest, err = getDRJSON[drShippingManifestRecord](ctx, tx, drShippingManifestKey(s.root, manifestID))
		if err != nil {
			return notFoundStatus(err, "dr shipping manifest not found")
		}
		if manifest.ManifestState != drShippingManifestStateBound ||
			!manifest.ManifestIntegrityVerified ||
			!manifest.PayloadRootsBound ||
			!manifest.ReadViewIdentityBound ||
			!manifest.KeyPolicyBound ||
			!manifest.GovernanceMetadataBound {
			return status.Error(codes.FailedPrecondition, "standby import requires a bound shipping manifest")
		}
		worker, err = getDRJSON[drShippingWorkerRecord](ctx, tx, drShippingWorkerKey(s.root, workerID))
		if err != nil {
			return notFoundStatus(err, "dr shipping worker not found")
		}
		if worker.ShippedManifestID != manifest.ShippedManifestID {
			return status.Error(codes.InvalidArgument, "shipping_worker_id is not admitted for shipped_manifest_id")
		}
		if !worker.RemoteTransferWorkerDeployed || !worker.RemoteTransferStarted {
			return status.Error(codes.FailedPrecondition, "standby import requires an admitted shipping worker with remote transfer started")
		}
		if worker.RemoteTransferCompleted {
			return status.Error(codes.FailedPrecondition, "remote transfer completion is not admitted in this slice")
		}
		link, err = getDRJSON[drReplicationLinkRecord](ctx, tx, drReplicationLinkKey(s.root, manifest.ReplicationLinkID))
		if err != nil {
			return notFoundStatus(err, "dr replication link not found")
		}
		recoveryPoint, err = getDRJSON[drRecoveryPointRecord](ctx, tx, drRecoveryPointKey(s.root, manifest.RecoveryPointID))
		if err != nil {
			return notFoundStatus(err, "dr recovery point not found")
		}

		existing, err := getDRJSON[drStandbyVolumeRecord](ctx, tx, drStandbyVolumeKey(s.root, standbyID))
		if err != nil && !errorsIsNotFound(err) {
			return err
		}
		createdAt := now
		createdBy := req.GetMeta().GetActor()
		createdReason := req.GetMeta().GetReason()
		generation := uint64(1)
		standbyWriteRejected := false
		lastWriteCheckAt := int64(0)
		if err == nil && existing.CreatedAtUnix > 0 {
			if existing.ShippedManifestID != manifest.ShippedManifestID {
				return status.Error(codes.InvalidArgument, "shipped_manifest_id cannot change for an existing standby volume")
			}
			createdAt = existing.CreatedAtUnix
			createdBy = existing.CreatedBy
			createdReason = existing.CreatedReason
			generation = existing.StandbyGeneration + 1
			standbyWriteRejected = existing.StandbyWriteRejected
			lastWriteCheckAt = existing.LastWriteCheckAtUnix
		}

		standby = drStandbyVolumeRecord{
			StandbyVolumeID:                  standbyID,
			ReplicationLinkID:                manifest.ReplicationLinkID,
			ShippedManifestID:                manifest.ShippedManifestID,
			RecoveryPointID:                  manifest.RecoveryPointID,
			ShippingWorkerID:                 worker.ShippingWorkerID,
			SourceClusterID:                  manifest.SourceClusterID,
			TargetClusterID:                  manifest.TargetClusterID,
			SourceVolumeID:                   manifest.SourceVolumeID,
			TargetVolumeID:                   manifest.TargetVolumeID,
			SourceSnapshotID:                 manifest.SourceSnapshotID,
			SourceSnapshotRootID:             manifest.SourceSnapshotRootID,
			BackupArtifactID:                 manifest.BackupArtifactID,
			StandbyGeneration:                generation,
			StandbyState:                     drStandbyVolumeStateReadOnly,
			ImportNodeID:                     importNodeID,
			ImportEndpoint:                   strings.TrimSpace(req.GetImportEndpoint()),
			ReadViewDigest:                   manifest.ReadViewDigest,
			KeyPolicyDigest:                  manifest.KeyPolicyDigest,
			GovernanceDigest:                 manifest.GovernanceDigest,
			ManifestIntegrityVerified:        manifest.ManifestIntegrityVerified,
			PayloadRootsBound:                manifest.PayloadRootsBound,
			ReadViewIdentityBound:            manifest.ReadViewIdentityBound,
			KeyPolicyBound:                   manifest.KeyPolicyBound,
			GovernanceMetadataBound:          manifest.GovernanceMetadataBound,
			RemoteTransferWorkerDeployed:     worker.RemoteTransferWorkerDeployed,
			RemoteTransferStarted:            worker.RemoteTransferStarted,
			RemoteTransferCompleted:          false,
			StandbyImportVerified:            true,
			StandbyReadOnlyVerified:          true,
			StandbyWriteRejectionRequired:    true,
			StandbyWriteRejected:             standbyWriteRejected,
			StandbyWriteRejectionReason:      drStandbyWriteRejectionReason,
			PromoteRequiredBeforeTargetWrite: true,
			TargetWriteAdmitted:              false,
			LastWriteCheckAtUnix:             lastWriteCheckAt,
			CreatedBy:                        createdBy,
			CreatedReason:                    createdReason,
			CreatedAtUnix:                    createdAt,
			UpdatedAtUnix:                    now,
		}

		link.StandbyImportSupported = true
		link.PromoteSupported = false
		link.FailoverSupported = false
		link.ReplicationLinkGeneration++
		link.UpdatedAtUnix = now

		if err := putDRJSON(ctx, tx, drStandbyVolumeKey(s.root, standbyID), standby); err != nil {
			return err
		}
		return putDRJSON(ctx, tx, drReplicationLinkKey(s.root, link.ReplicationLinkID), link)
	})
	if err != nil {
		if _, ok := status.FromError(err); ok {
			return nil, err
		}
		return nil, status.Errorf(codes.Internal, "persist dr standby volume import: %v", err)
	}
	op, err := s.ops.create("dr.standby_volume.import", "", standby.SourceVolumeID, "standby-volume-imported-read-only", adminv1.OperationState_OPERATION_STATE_COMPLETED)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "record operation: %v", err)
	}
	return &adminv1.ImportDRStandbyVolumeResponse{
		Cluster:       cluster,
		Operation:     acceptedOperation(op, "dr standby volume imported read-only"),
		Standby:       standby.toProto(),
		Link:          link.toProto(),
		RecoveryPoint: recoveryPoint.toProto(),
		Manifest:      manifest.toProto(),
		Worker:        worker.toProto(),
	}, nil
}

func (s *server) GetDRStandbyVolume(ctx context.Context, req *adminv1.GetDRStandbyVolumeRequest) (*adminv1.GetDRStandbyVolumeResponse, error) {
	cluster, err := s.clusterRef(req.GetCluster())
	if err != nil {
		return nil, err
	}
	rec, err := s.getDRStandbyVolumeRecord(ctx, req.GetStandbyVolumeId())
	if err != nil {
		return nil, err
	}
	return &adminv1.GetDRStandbyVolumeResponse{Cluster: cluster, Standby: rec.toProto()}, nil
}

func (s *server) ListDRStandbyVolumes(ctx context.Context, req *adminv1.ListDRStandbyVolumesRequest) (*adminv1.ListDRStandbyVolumesResponse, error) {
	cluster, err := s.clusterRef(req.GetCluster())
	if err != nil {
		return nil, err
	}
	records, err := s.listDRStandbyVolumeRecords(ctx)
	if err != nil {
		return nil, err
	}
	linkID := strings.TrimSpace(req.GetReplicationLinkId())
	manifestID := strings.TrimSpace(req.GetShippedManifestId())
	out := make([]*adminv1.DRStandbyVolumeSummary, 0, len(records))
	for _, rec := range records {
		if linkID != "" && rec.ReplicationLinkID != linkID {
			continue
		}
		if manifestID != "" && rec.ShippedManifestID != manifestID {
			continue
		}
		out = append(out, rec.toProto())
	}
	return &adminv1.ListDRStandbyVolumesResponse{Cluster: cluster, Standbys: out}, nil
}

func (s *server) CheckDRStandbyVolumeWrite(ctx context.Context, req *adminv1.CheckDRStandbyVolumeWriteRequest) (*adminv1.CheckDRStandbyVolumeWriteResponse, error) {
	cluster, err := s.clusterRef(req.GetCluster())
	if err != nil {
		return nil, err
	}
	standbyID := strings.TrimSpace(req.GetStandbyVolumeId())
	if standbyID == "" {
		return nil, status.Error(codes.InvalidArgument, "standby_volume_id is required")
	}
	now := time.Now().UTC().Unix()
	var rec drStandbyVolumeRecord
	err = clustermeta.RunInTransaction(ctx, s.kv, func(tx clustermeta.ReadWriter) error {
		var err error
		rec, err = getDRJSON[drStandbyVolumeRecord](ctx, tx, drStandbyVolumeKey(s.root, standbyID))
		if err != nil {
			return notFoundStatus(err, "dr standby volume not found")
		}
		rec.StandbyGeneration++
		if rec.StandbyState == drStandbyVolumeStatePromoted && rec.TargetWriteAdmitted {
			rec.StandbyWriteRejected = false
			rec.StandbyWriteRejectionReason = ""
			rec.PromoteRequiredBeforeTargetWrite = false
			rec.TargetWriteAdmitted = true
		} else {
			rec.StandbyWriteRejected = true
			rec.StandbyWriteRejectionReason = drStandbyWriteRejectionReason
			rec.PromoteRequiredBeforeTargetWrite = true
			rec.TargetWriteAdmitted = false
		}
		rec.LastWriteCheckAtUnix = now
		rec.UpdatedAtUnix = now
		return putDRJSON(ctx, tx, drStandbyVolumeKey(s.root, standbyID), rec)
	})
	if err != nil {
		if _, ok := status.FromError(err); ok {
			return nil, err
		}
		return nil, status.Errorf(codes.Internal, "persist dr standby write check: %v", err)
	}
	writeCheckDetail := "standby-volume-write-rejected-read-only"
	if rec.TargetWriteAdmitted {
		writeCheckDetail = "standby-volume-write-admitted-after-promote"
	}
	op, err := s.ops.create("dr.standby_volume.write_check", "", rec.SourceVolumeID, writeCheckDetail, adminv1.OperationState_OPERATION_STATE_COMPLETED)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "record operation: %v", err)
	}
	return &adminv1.CheckDRStandbyVolumeWriteResponse{
		Cluster:                     cluster,
		Operation:                   acceptedOperation(op, "dr standby volume write authority checked"),
		Standby:                     rec.toProto(),
		StandbyWriteRejected:        rec.StandbyWriteRejected,
		StandbyWriteRejectionReason: rec.StandbyWriteRejectionReason,
		TargetWriteAdmitted:         rec.TargetWriteAdmitted,
	}, nil
}

func (s *server) PromoteDRStandbyVolume(ctx context.Context, req *adminv1.PromoteDRStandbyVolumeRequest) (*adminv1.PromoteDRStandbyVolumeResponse, error) {
	cluster, err := s.clusterRef(req.GetCluster())
	if err != nil {
		return nil, err
	}
	standbyID := strings.TrimSpace(req.GetStandbyVolumeId())
	if standbyID == "" {
		return nil, status.Error(codes.InvalidArgument, "standby_volume_id is required")
	}
	now := time.Now().UTC().Unix()
	var rec drStandbyVolumeRecord
	var link drReplicationLinkRecord
	err = clustermeta.RunInTransaction(ctx, s.kv, func(tx clustermeta.ReadWriter) error {
		var err error
		rec, err = getDRJSON[drStandbyVolumeRecord](ctx, tx, drStandbyVolumeKey(s.root, standbyID))
		if err != nil {
			return notFoundStatus(err, "dr standby volume not found")
		}
		if !rec.StandbyImportVerified || !rec.StandbyReadOnlyVerified {
			return status.Error(codes.FailedPrecondition, "dr standby volume must be imported read-only before promote")
		}
		if rec.StandbyState == drStandbyVolumeStatePromoted && rec.TargetWriteAdmitted {
			return status.Error(codes.FailedPrecondition, "dr standby volume is already promoted")
		}
		link, err = getDRJSON[drReplicationLinkRecord](ctx, tx, drReplicationLinkKey(s.root, rec.ReplicationLinkID))
		if err != nil {
			return notFoundStatus(err, "dr replication link not found")
		}

		rec.StandbyGeneration++
		rec.PromoteGeneration = nextDRTransitionGeneration(rec)
		rec.FencingEpoch++
		rec.StandbyState = drStandbyVolumeStatePromoted
		rec.PromoteFencingVerified = true
		rec.StandbyWriteRejected = false
		rec.StandbyWriteRejectionReason = ""
		rec.PromoteRequiredBeforeTargetWrite = false
		rec.TargetWriteAdmitted = true
		rec.LastPromoteAtUnix = now
		rec.UpdatedAtUnix = now

		link.PromoteSupported = true
		link.FailoverSupported = false
		link.ReplicationLinkGeneration++
		link.UpdatedAtUnix = now

		if err := putDRJSON(ctx, tx, drStandbyVolumeKey(s.root, standbyID), rec); err != nil {
			return err
		}
		return putDRJSON(ctx, tx, drReplicationLinkKey(s.root, link.ReplicationLinkID), link)
	})
	if err != nil {
		if _, ok := status.FromError(err); ok {
			return nil, err
		}
		return nil, status.Errorf(codes.Internal, "persist dr standby promote: %v", err)
	}
	op, err := s.ops.create("dr.standby_volume.promote", "", rec.SourceVolumeID, "standby-volume-promoted-with-fencing", adminv1.OperationState_OPERATION_STATE_COMPLETED)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "record operation: %v", err)
	}
	return &adminv1.PromoteDRStandbyVolumeResponse{
		Cluster:             cluster,
		Operation:           acceptedOperation(op, "dr standby volume promoted with fencing"),
		Standby:             rec.toProto(),
		Link:                link.toProto(),
		TargetWriteAdmitted: rec.TargetWriteAdmitted,
		PromoteGeneration:   rec.PromoteGeneration,
		FencingEpoch:        rec.FencingEpoch,
	}, nil
}

func (s *server) DemoteDRStandbyVolume(ctx context.Context, req *adminv1.DemoteDRStandbyVolumeRequest) (*adminv1.DemoteDRStandbyVolumeResponse, error) {
	cluster, err := s.clusterRef(req.GetCluster())
	if err != nil {
		return nil, err
	}
	standbyID := strings.TrimSpace(req.GetStandbyVolumeId())
	if standbyID == "" {
		return nil, status.Error(codes.InvalidArgument, "standby_volume_id is required")
	}
	now := time.Now().UTC().Unix()
	var rec drStandbyVolumeRecord
	var link drReplicationLinkRecord
	err = clustermeta.RunInTransaction(ctx, s.kv, func(tx clustermeta.ReadWriter) error {
		var err error
		rec, err = getDRJSON[drStandbyVolumeRecord](ctx, tx, drStandbyVolumeKey(s.root, standbyID))
		if err != nil {
			return notFoundStatus(err, "dr standby volume not found")
		}
		if rec.StandbyState != drStandbyVolumeStatePromoted || !rec.TargetWriteAdmitted {
			return status.Error(codes.FailedPrecondition, "dr standby volume must be promoted before demote")
		}
		link, err = getDRJSON[drReplicationLinkRecord](ctx, tx, drReplicationLinkKey(s.root, rec.ReplicationLinkID))
		if err != nil {
			return notFoundStatus(err, "dr replication link not found")
		}

		rec.StandbyGeneration++
		rec.DemoteGeneration = nextDRTransitionGeneration(rec)
		rec.FencingEpoch++
		rec.StandbyState = drStandbyVolumeStateDemoted
		rec.DemoteFencingVerified = true
		rec.StandbyWriteRejected = false
		rec.StandbyWriteRejectionReason = ""
		rec.PromoteRequiredBeforeTargetWrite = true
		rec.TargetWriteAdmitted = false
		rec.LastDemoteAtUnix = now
		rec.UpdatedAtUnix = now

		link.PromoteSupported = true
		link.FailoverSupported = false
		link.ReplicationLinkGeneration++
		link.UpdatedAtUnix = now

		if err := putDRJSON(ctx, tx, drStandbyVolumeKey(s.root, standbyID), rec); err != nil {
			return err
		}
		return putDRJSON(ctx, tx, drReplicationLinkKey(s.root, link.ReplicationLinkID), link)
	})
	if err != nil {
		if _, ok := status.FromError(err); ok {
			return nil, err
		}
		return nil, status.Errorf(codes.Internal, "persist dr standby demote: %v", err)
	}
	op, err := s.ops.create("dr.standby_volume.demote", "", rec.SourceVolumeID, "standby-volume-demoted-with-fencing", adminv1.OperationState_OPERATION_STATE_COMPLETED)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "record operation: %v", err)
	}
	return &adminv1.DemoteDRStandbyVolumeResponse{
		Cluster:             cluster,
		Operation:           acceptedOperation(op, "dr standby volume demoted with fencing"),
		Standby:             rec.toProto(),
		Link:                link.toProto(),
		TargetWriteAdmitted: rec.TargetWriteAdmitted,
		DemoteGeneration:    rec.DemoteGeneration,
		FencingEpoch:        rec.FencingEpoch,
	}, nil
}

func (s *server) CheckDROldPrimaryWrite(ctx context.Context, req *adminv1.CheckDROldPrimaryWriteRequest) (*adminv1.CheckDROldPrimaryWriteResponse, error) {
	cluster, err := s.clusterRef(req.GetCluster())
	if err != nil {
		return nil, err
	}
	standbyID := strings.TrimSpace(req.GetStandbyVolumeId())
	if standbyID == "" {
		return nil, status.Error(codes.InvalidArgument, "standby_volume_id is required")
	}
	now := time.Now().UTC().Unix()
	var rec drStandbyVolumeRecord
	err = clustermeta.RunInTransaction(ctx, s.kv, func(tx clustermeta.ReadWriter) error {
		var err error
		rec, err = getDRJSON[drStandbyVolumeRecord](ctx, tx, drStandbyVolumeKey(s.root, standbyID))
		if err != nil {
			return notFoundStatus(err, "dr standby volume not found")
		}
		if rec.PromoteGeneration == 0 {
			return status.Error(codes.FailedPrecondition, "dr standby volume must be promoted before old-primary fencing can be observed")
		}
		rec.StandbyGeneration++
		rec.StaleOldPrimaryRejected = true
		rec.OldPrimaryWriteRejectionReason = drOldPrimaryWriteRejectionReason
		rec.LastOldPrimaryWriteCheckAtUnix = now
		rec.UpdatedAtUnix = now
		return putDRJSON(ctx, tx, drStandbyVolumeKey(s.root, standbyID), rec)
	})
	if err != nil {
		if _, ok := status.FromError(err); ok {
			return nil, err
		}
		return nil, status.Errorf(codes.Internal, "persist dr old-primary write check: %v", err)
	}
	op, err := s.ops.create("dr.old_primary.write_check", "", rec.SourceVolumeID, "old-primary-write-rejected-after-promote", adminv1.OperationState_OPERATION_STATE_COMPLETED)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "record operation: %v", err)
	}
	return &adminv1.CheckDROldPrimaryWriteResponse{
		Cluster:                        cluster,
		Operation:                      acceptedOperation(op, "dr old-primary write rejected by fencing"),
		Standby:                        rec.toProto(),
		StaleOldPrimaryRejected:        rec.StaleOldPrimaryRejected,
		OldPrimaryWriteRejectionReason: rec.OldPrimaryWriteRejectionReason,
		TargetWriteAdmitted:            rec.TargetWriteAdmitted,
	}, nil
}

func (s *server) DefineDROldPrimaryRejoinPolicy(ctx context.Context, req *adminv1.DefineDROldPrimaryRejoinPolicyRequest) (*adminv1.DefineDROldPrimaryRejoinPolicyResponse, error) {
	cluster, err := s.clusterRef(req.GetCluster())
	if err != nil {
		return nil, err
	}
	standbyID := strings.TrimSpace(req.GetStandbyVolumeId())
	if standbyID == "" {
		return nil, status.Error(codes.InvalidArgument, "standby_volume_id is required")
	}
	policy := strings.TrimSpace(req.GetOldPrimaryRejoinPolicy())
	if policy == "" {
		policy = drOldPrimaryRejoinPolicyExplicit
	}
	now := time.Now().UTC().Unix()
	var rec drStandbyVolumeRecord
	err = clustermeta.RunInTransaction(ctx, s.kv, func(tx clustermeta.ReadWriter) error {
		var err error
		rec, err = getDRJSON[drStandbyVolumeRecord](ctx, tx, drStandbyVolumeKey(s.root, standbyID))
		if err != nil {
			return notFoundStatus(err, "dr standby volume not found")
		}
		if rec.PromoteGeneration == 0 {
			return status.Error(codes.FailedPrecondition, "dr standby volume must be promoted before old-primary rejoin policy")
		}
		rec.StandbyGeneration++
		rec.OldPrimaryRejoinPolicyDefined = true
		rec.OldPrimaryRejoinPolicy = policy
		rec.LastOldPrimaryRejoinPolicyAtUnix = now
		rec.UpdatedAtUnix = now
		return putDRJSON(ctx, tx, drStandbyVolumeKey(s.root, standbyID), rec)
	})
	if err != nil {
		if _, ok := status.FromError(err); ok {
			return nil, err
		}
		return nil, status.Errorf(codes.Internal, "persist dr old-primary rejoin policy: %v", err)
	}
	op, err := s.ops.create("dr.old_primary.rejoin_policy", "", rec.SourceVolumeID, "old-primary-rejoin-policy-defined", adminv1.OperationState_OPERATION_STATE_COMPLETED)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "record operation: %v", err)
	}
	return &adminv1.DefineDROldPrimaryRejoinPolicyResponse{
		Cluster:                       cluster,
		Operation:                     acceptedOperation(op, "dr old-primary rejoin policy defined"),
		Standby:                       rec.toProto(),
		OldPrimaryRejoinPolicyDefined: rec.OldPrimaryRejoinPolicyDefined,
		OldPrimaryRejoinPolicy:        rec.OldPrimaryRejoinPolicy,
		AutomaticOldPrimaryRejoin:     false,
	}, nil
}

func (s *server) RunDRFailoverDrill(ctx context.Context, req *adminv1.RunDRFailoverDrillRequest) (*adminv1.RunDRFailoverDrillResponse, error) {
	cluster, err := s.clusterRef(req.GetCluster())
	if err != nil {
		return nil, err
	}
	drillID := strings.TrimSpace(req.GetFailoverDrillId())
	if drillID == "" {
		return nil, status.Error(codes.InvalidArgument, "failover_drill_id is required")
	}
	standbyID := strings.TrimSpace(req.GetStandbyVolumeId())
	if standbyID == "" {
		return nil, status.Error(codes.InvalidArgument, "standby_volume_id is required")
	}
	now := time.Now().UTC().Unix()
	var drill drFailoverDrillRecord
	var standby drStandbyVolumeRecord
	var link drReplicationLinkRecord
	err = clustermeta.RunInTransaction(ctx, s.kv, func(tx clustermeta.ReadWriter) error {
		var err error
		standby, err = getDRJSON[drStandbyVolumeRecord](ctx, tx, drStandbyVolumeKey(s.root, standbyID))
		if err != nil {
			return notFoundStatus(err, "dr standby volume not found")
		}
		link, err = getDRJSON[drReplicationLinkRecord](ctx, tx, drReplicationLinkKey(s.root, standby.ReplicationLinkID))
		if err != nil {
			return notFoundStatus(err, "dr replication link not found")
		}
		targetPromoteObserved := standby.PromoteGeneration > 0 && standby.PromoteFencingVerified
		oldPrimaryRejoinPolicyVerified := standby.StaleOldPrimaryRejected && standby.OldPrimaryRejoinPolicyDefined
		drillCompleted := req.GetSourceUnavailableObserved() &&
			targetPromoteObserved &&
			req.GetTargetMounted() &&
			req.GetMountReadbackMatched() &&
			req.GetApplicationVisibleIdentityVerified() &&
			req.GetCleanupRejoinStateVerified() &&
			oldPrimaryRejoinPolicyVerified &&
			standby.DemoteGeneration > 0 &&
			standby.DemoteFencingVerified
		if !drillCompleted {
			return status.Error(codes.FailedPrecondition, "failover drill requires product promote/demote/rejoin evidence and observed lab readback")
		}

		existing, err := getDRJSON[drFailoverDrillRecord](ctx, tx, drFailoverDrillKey(s.root, drillID))
		if err != nil && !errorsIsNotFound(err) {
			return err
		}
		createdAt := now
		createdBy := req.GetMeta().GetActor()
		createdReason := req.GetMeta().GetReason()
		generation := uint64(1)
		if err == nil && existing.CreatedAtUnix > 0 {
			if existing.StandbyVolumeID != standby.StandbyVolumeID {
				return status.Error(codes.InvalidArgument, "standby_volume_id cannot change for an existing failover drill")
			}
			createdAt = existing.CreatedAtUnix
			createdBy = existing.CreatedBy
			createdReason = existing.CreatedReason
			generation = existing.FailoverDrillGeneration + 1
		}

		drill = drFailoverDrillRecord{
			FailoverDrillID:                    drillID,
			StandbyVolumeID:                    standby.StandbyVolumeID,
			ReplicationLinkID:                  standby.ReplicationLinkID,
			RecoveryPointID:                    standby.RecoveryPointID,
			ShippedManifestID:                  standby.ShippedManifestID,
			ShippingWorkerID:                   standby.ShippingWorkerID,
			SourceClusterID:                    standby.SourceClusterID,
			TargetClusterID:                    standby.TargetClusterID,
			SourceVolumeID:                     standby.SourceVolumeID,
			TargetVolumeID:                     standby.TargetVolumeID,
			FailoverDrillGeneration:            generation,
			FailoverDrillState:                 drFailoverDrillStateValidated,
			SourceUnavailableObserved:          req.GetSourceUnavailableObserved(),
			TargetPromoteObserved:              targetPromoteObserved,
			TargetMounted:                      req.GetTargetMounted(),
			MountReadbackMatched:               req.GetMountReadbackMatched(),
			ApplicationVisibleIdentityVerified: req.GetApplicationVisibleIdentityVerified(),
			CleanupRejoinStateVerified:         req.GetCleanupRejoinStateVerified(),
			OldPrimaryRejoinPolicyVerified:     oldPrimaryRejoinPolicyVerified,
			FailoverDrillCompleted:             drillCompleted,
			PromoteGeneration:                  standby.PromoteGeneration,
			DemoteGeneration:                   standby.DemoteGeneration,
			FencingEpoch:                       standby.FencingEpoch,
			StaleOldPrimaryRejected:            standby.StaleOldPrimaryRejected,
			OldPrimaryWriteRejectionReason:     standby.OldPrimaryWriteRejectionReason,
			OldPrimaryRejoinPolicyDefined:      standby.OldPrimaryRejoinPolicyDefined,
			OldPrimaryRejoinPolicy:             standby.OldPrimaryRejoinPolicy,
			CreatedBy:                          createdBy,
			CreatedReason:                      createdReason,
			CreatedAtUnix:                      createdAt,
			UpdatedAtUnix:                      now,
		}

		return putDRJSON(ctx, tx, drFailoverDrillKey(s.root, drillID), drill)
	})
	if err != nil {
		if _, ok := status.FromError(err); ok {
			return nil, err
		}
		return nil, status.Errorf(codes.Internal, "persist dr failover drill: %v", err)
	}
	op, err := s.ops.create("dr.failover.drill", "", drill.SourceVolumeID, "failover-drill-control-plane-validated", adminv1.OperationState_OPERATION_STATE_COMPLETED)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "record operation: %v", err)
	}
	return &adminv1.RunDRFailoverDrillResponse{
		Cluster:   cluster,
		Operation: acceptedOperation(op, "dr failover drill control-plane validated"),
		Drill:     drill.toProto(),
		Standby:   standby.toProto(),
		Link:      link.toProto(),
	}, nil
}

func (s *server) GetDRFailoverDrill(ctx context.Context, req *adminv1.GetDRFailoverDrillRequest) (*adminv1.GetDRFailoverDrillResponse, error) {
	cluster, err := s.clusterRef(req.GetCluster())
	if err != nil {
		return nil, err
	}
	rec, err := s.getDRFailoverDrillRecord(ctx, req.GetFailoverDrillId())
	if err != nil {
		return nil, err
	}
	return &adminv1.GetDRFailoverDrillResponse{Cluster: cluster, Drill: rec.toProto()}, nil
}

func (s *server) ListDRFailoverDrills(ctx context.Context, req *adminv1.ListDRFailoverDrillsRequest) (*adminv1.ListDRFailoverDrillsResponse, error) {
	cluster, err := s.clusterRef(req.GetCluster())
	if err != nil {
		return nil, err
	}
	records, err := s.listDRFailoverDrillRecords(ctx)
	if err != nil {
		return nil, err
	}
	linkID := strings.TrimSpace(req.GetReplicationLinkId())
	standbyID := strings.TrimSpace(req.GetStandbyVolumeId())
	out := make([]*adminv1.DRFailoverDrillSummary, 0, len(records))
	for _, rec := range records {
		if linkID != "" && rec.ReplicationLinkID != linkID {
			continue
		}
		if standbyID != "" && rec.StandbyVolumeID != standbyID {
			continue
		}
		out = append(out, rec.toProto())
	}
	return &adminv1.ListDRFailoverDrillsResponse{Cluster: cluster, Drills: out}, nil
}

func (s *server) getDRReplicationLinkRecord(ctx context.Context, linkID string) (drReplicationLinkRecord, error) {
	linkID = strings.TrimSpace(linkID)
	if linkID == "" {
		return drReplicationLinkRecord{}, status.Error(codes.InvalidArgument, "replication_link_id is required")
	}
	rec, err := getDRJSON[drReplicationLinkRecord](ctx, s.kv, drReplicationLinkKey(s.root, linkID))
	if err != nil {
		return drReplicationLinkRecord{}, notFoundStatus(err, "dr replication link not found")
	}
	return rec, nil
}

func (s *server) getDRRecoveryPointRecord(ctx context.Context, recoveryPointID string) (drRecoveryPointRecord, error) {
	recoveryPointID = strings.TrimSpace(recoveryPointID)
	if recoveryPointID == "" {
		return drRecoveryPointRecord{}, status.Error(codes.InvalidArgument, "recovery_point_id is required")
	}
	rec, err := getDRJSON[drRecoveryPointRecord](ctx, s.kv, drRecoveryPointKey(s.root, recoveryPointID))
	if err != nil {
		return drRecoveryPointRecord{}, notFoundStatus(err, "dr recovery point not found")
	}
	return rec, nil
}

func (s *server) getDRShippingManifestRecord(ctx context.Context, manifestID string) (drShippingManifestRecord, error) {
	manifestID = strings.TrimSpace(manifestID)
	if manifestID == "" {
		return drShippingManifestRecord{}, status.Error(codes.InvalidArgument, "shipped_manifest_id is required")
	}
	rec, err := getDRJSON[drShippingManifestRecord](ctx, s.kv, drShippingManifestKey(s.root, manifestID))
	if err != nil {
		return drShippingManifestRecord{}, notFoundStatus(err, "dr shipping manifest not found")
	}
	return rec, nil
}

func (s *server) getDRShippingWorkerRecord(ctx context.Context, workerID string) (drShippingWorkerRecord, error) {
	workerID = strings.TrimSpace(workerID)
	if workerID == "" {
		return drShippingWorkerRecord{}, status.Error(codes.InvalidArgument, "shipping_worker_id is required")
	}
	rec, err := getDRJSON[drShippingWorkerRecord](ctx, s.kv, drShippingWorkerKey(s.root, workerID))
	if err != nil {
		return drShippingWorkerRecord{}, notFoundStatus(err, "dr shipping worker not found")
	}
	return rec, nil
}

func (s *server) getDRStandbyVolumeRecord(ctx context.Context, standbyID string) (drStandbyVolumeRecord, error) {
	standbyID = strings.TrimSpace(standbyID)
	if standbyID == "" {
		return drStandbyVolumeRecord{}, status.Error(codes.InvalidArgument, "standby_volume_id is required")
	}
	rec, err := getDRJSON[drStandbyVolumeRecord](ctx, s.kv, drStandbyVolumeKey(s.root, standbyID))
	if err != nil {
		return drStandbyVolumeRecord{}, notFoundStatus(err, "dr standby volume not found")
	}
	return rec, nil
}

func (s *server) getDRFailoverDrillRecord(ctx context.Context, drillID string) (drFailoverDrillRecord, error) {
	drillID = strings.TrimSpace(drillID)
	if drillID == "" {
		return drFailoverDrillRecord{}, status.Error(codes.InvalidArgument, "failover_drill_id is required")
	}
	rec, err := getDRJSON[drFailoverDrillRecord](ctx, s.kv, drFailoverDrillKey(s.root, drillID))
	if err != nil {
		return drFailoverDrillRecord{}, notFoundStatus(err, "dr failover drill not found")
	}
	return rec, nil
}

func (s *server) listDRReplicationLinkRecords(ctx context.Context) ([]drReplicationLinkRecord, error) {
	keys, err := listKeys(ctx, s.kv, drReplicationLinkPrefix(s.root))
	if err != nil {
		return nil, status.Errorf(codes.Internal, "list dr replication links: %v", err)
	}
	out := make([]drReplicationLinkRecord, 0, len(keys))
	for _, key := range keys {
		rec, err := getDRJSON[drReplicationLinkRecord](ctx, s.kv, key)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "decode dr replication link %s: %v", key, err)
		}
		out = append(out, rec)
	}
	sort.SliceStable(out, func(i, j int) bool {
		return out[i].ReplicationLinkID < out[j].ReplicationLinkID
	})
	return out, nil
}

func (s *server) listDRRecoveryPointRecords(ctx context.Context) ([]drRecoveryPointRecord, error) {
	keys, err := listKeys(ctx, s.kv, drRecoveryPointPrefix(s.root))
	if err != nil {
		return nil, status.Errorf(codes.Internal, "list dr recovery points: %v", err)
	}
	out := make([]drRecoveryPointRecord, 0, len(keys))
	for _, key := range keys {
		rec, err := getDRJSON[drRecoveryPointRecord](ctx, s.kv, key)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "decode dr recovery point %s: %v", key, err)
		}
		out = append(out, rec)
	}
	sort.SliceStable(out, func(i, j int) bool {
		return out[i].RecoveryPointID < out[j].RecoveryPointID
	})
	return out, nil
}

func (s *server) listDRShippingManifestRecords(ctx context.Context) ([]drShippingManifestRecord, error) {
	keys, err := listKeys(ctx, s.kv, drShippingManifestPrefix(s.root))
	if err != nil {
		return nil, status.Errorf(codes.Internal, "list dr shipping manifests: %v", err)
	}
	out := make([]drShippingManifestRecord, 0, len(keys))
	for _, key := range keys {
		rec, err := getDRJSON[drShippingManifestRecord](ctx, s.kv, key)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "decode dr shipping manifest %s: %v", key, err)
		}
		out = append(out, rec)
	}
	sort.SliceStable(out, func(i, j int) bool {
		return out[i].ShippedManifestID < out[j].ShippedManifestID
	})
	return out, nil
}

func (s *server) listDRShippingWorkerRecords(ctx context.Context) ([]drShippingWorkerRecord, error) {
	keys, err := listKeys(ctx, s.kv, drShippingWorkerPrefix(s.root))
	if err != nil {
		return nil, status.Errorf(codes.Internal, "list dr shipping workers: %v", err)
	}
	out := make([]drShippingWorkerRecord, 0, len(keys))
	for _, key := range keys {
		rec, err := getDRJSON[drShippingWorkerRecord](ctx, s.kv, key)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "decode dr shipping worker %s: %v", key, err)
		}
		out = append(out, rec)
	}
	sort.SliceStable(out, func(i, j int) bool {
		return out[i].ShippingWorkerID < out[j].ShippingWorkerID
	})
	return out, nil
}

func (s *server) listDRStandbyVolumeRecords(ctx context.Context) ([]drStandbyVolumeRecord, error) {
	keys, err := listKeys(ctx, s.kv, drStandbyVolumePrefix(s.root))
	if err != nil {
		return nil, status.Errorf(codes.Internal, "list dr standby volumes: %v", err)
	}
	out := make([]drStandbyVolumeRecord, 0, len(keys))
	for _, key := range keys {
		rec, err := getDRJSON[drStandbyVolumeRecord](ctx, s.kv, key)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "decode dr standby volume %s: %v", key, err)
		}
		out = append(out, rec)
	}
	sort.SliceStable(out, func(i, j int) bool {
		return out[i].StandbyVolumeID < out[j].StandbyVolumeID
	})
	return out, nil
}

func (s *server) listDRFailoverDrillRecords(ctx context.Context) ([]drFailoverDrillRecord, error) {
	keys, err := listKeys(ctx, s.kv, drFailoverDrillPrefix(s.root))
	if err != nil {
		return nil, status.Errorf(codes.Internal, "list dr failover drills: %v", err)
	}
	out := make([]drFailoverDrillRecord, 0, len(keys))
	for _, key := range keys {
		rec, err := getDRJSON[drFailoverDrillRecord](ctx, s.kv, key)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "decode dr failover drill %s: %v", key, err)
		}
		out = append(out, rec)
	}
	sort.SliceStable(out, func(i, j int) bool {
		return out[i].FailoverDrillID < out[j].FailoverDrillID
	})
	return out, nil
}

func drShippingManifestState(req *adminv1.CreateDRShippingManifestRequest) string {
	if req.GetManifestIntegrityVerified() &&
		req.GetPayloadRootsBound() &&
		req.GetReadViewIdentityBound() &&
		req.GetKeyPolicyBound() &&
		req.GetGovernanceMetadataBound() {
		return drShippingManifestStateBound
	}
	return drShippingManifestStateRecorded
}

func nextDRTransitionGeneration(rec drStandbyVolumeRecord) uint64 {
	if rec.PromoteGeneration > rec.DemoteGeneration {
		return rec.PromoteGeneration + 1
	}
	return rec.DemoteGeneration + 1
}

func putDRJSON(ctx context.Context, kv drJSONStore, key string, record any) error {
	raw, err := json.Marshal(record)
	if err != nil {
		return err
	}
	return kv.Set(ctx, key, raw)
}

func getDRJSON[T any](ctx context.Context, kv drJSONStore, key string) (T, error) {
	var out T
	raw, found, err := kv.Get(ctx, key)
	if err != nil {
		return out, err
	}
	if !found {
		return out, clustermeta.ErrNotFound
	}
	if err := json.Unmarshal(raw, &out); err != nil {
		return out, err
	}
	return out, nil
}

func drReplicationLinkPrefix(root string) string {
	return fmt.Sprintf("%s/admin/dr/replication-links/", root)
}

func drReplicationLinkKey(root, linkID string) string {
	return drReplicationLinkPrefix(root) + linkID
}

func drRecoveryPointPrefix(root string) string {
	return fmt.Sprintf("%s/admin/dr/recovery-points/", root)
}

func drRecoveryPointKey(root, recoveryPointID string) string {
	return drRecoveryPointPrefix(root) + recoveryPointID
}

func drShippingManifestPrefix(root string) string {
	return fmt.Sprintf("%s/admin/dr/shipping-manifests/", root)
}

func drShippingManifestKey(root, manifestID string) string {
	return drShippingManifestPrefix(root) + manifestID
}

func drShippingWorkerPrefix(root string) string {
	return fmt.Sprintf("%s/admin/dr/shipping-workers/", root)
}

func drShippingWorkerKey(root, workerID string) string {
	return drShippingWorkerPrefix(root) + workerID
}

func drStandbyVolumePrefix(root string) string {
	return fmt.Sprintf("%s/admin/dr/standby-volumes/", root)
}

func drStandbyVolumeKey(root, standbyID string) string {
	return drStandbyVolumePrefix(root) + standbyID
}

func drFailoverDrillPrefix(root string) string {
	return fmt.Sprintf("%s/admin/dr/failover-drills/", root)
}

func drFailoverDrillKey(root, drillID string) string {
	return drFailoverDrillPrefix(root) + drillID
}

func (r drReplicationLinkRecord) toProto() *adminv1.DRReplicationLinkSummary {
	return &adminv1.DRReplicationLinkSummary{
		ReplicationLinkId:         r.ReplicationLinkID,
		SourceClusterId:           r.SourceClusterID,
		TargetClusterId:           r.TargetClusterID,
		SourceVolumeId:            r.SourceVolumeID,
		TargetVolumeId:            r.TargetVolumeID,
		ReplicationLinkGeneration: r.ReplicationLinkGeneration,
		ReplicationLinkState:      r.ReplicationLinkState,
		RecoveryPointPolicy:       r.RecoveryPointPolicy,
		ShippingMode:              r.ShippingMode,
		ShippingWorkerDeployed:    r.ShippingWorkerDeployed,
		StandbyImportSupported:    r.StandbyImportSupported,
		PromoteSupported:          r.PromoteSupported,
		FailoverSupported:         r.FailoverSupported,
		CreatedAt:                 unixTimestamp(r.CreatedAtUnix),
		UpdatedAt:                 unixTimestamp(r.UpdatedAtUnix),
	}
}

func (r drRecoveryPointRecord) toProto() *adminv1.DRRecoveryPointSummary {
	return &adminv1.DRRecoveryPointSummary{
		RecoveryPointId:              r.RecoveryPointID,
		ReplicationLinkId:            r.ReplicationLinkID,
		SourceClusterId:              r.SourceClusterID,
		TargetClusterId:              r.TargetClusterID,
		SourceVolumeId:               r.SourceVolumeID,
		TargetVolumeId:               r.TargetVolumeID,
		SourceSnapshotId:             r.SourceSnapshotID,
		SourceSnapshotRootId:         r.SourceSnapshotRootID,
		ConsistencyPointId:           r.ConsistencyPointID,
		BackupArtifactId:             r.BackupArtifactID,
		RecoveryPointGeneration:      r.RecoveryPointGeneration,
		RecoveryPointState:           r.RecoveryPointState,
		RemoteTransferWorkerDeployed: r.RemoteTransferWorkerDeployed,
		CreatedAt:                    unixTimestamp(r.CreatedAtUnix),
		UpdatedAt:                    unixTimestamp(r.UpdatedAtUnix),
	}
}

func (r drShippingWorkerRecord) toProto() *adminv1.DRShippingWorkerSummary {
	return &adminv1.DRShippingWorkerSummary{
		ShippingWorkerId:             r.ShippingWorkerID,
		ReplicationLinkId:            r.ReplicationLinkID,
		ShippedManifestId:            r.ShippedManifestID,
		RecoveryPointId:              r.RecoveryPointID,
		SourceClusterId:              r.SourceClusterID,
		TargetClusterId:              r.TargetClusterID,
		SourceVolumeId:               r.SourceVolumeID,
		TargetVolumeId:               r.TargetVolumeID,
		WorkerGeneration:             r.WorkerGeneration,
		WorkerState:                  r.WorkerState,
		WorkerNodeId:                 r.WorkerNodeID,
		SourceNodeId:                 r.SourceNodeID,
		TargetNodeId:                 r.TargetNodeID,
		TargetEndpoint:               r.TargetEndpoint,
		CredentialBoundaryId:         r.CredentialBoundaryID,
		TransferPlanDigest:           r.TransferPlanDigest,
		ManifestIntegrityVerified:    r.ManifestIntegrityVerified,
		PayloadRootsBound:            r.PayloadRootsBound,
		ReadViewIdentityBound:        r.ReadViewIdentityBound,
		KeyPolicyBound:               r.KeyPolicyBound,
		GovernanceMetadataBound:      r.GovernanceMetadataBound,
		RemoteTransferWorkerDeployed: r.RemoteTransferWorkerDeployed,
		RemoteTransferStarted:        r.RemoteTransferStarted,
		RemoteTransferCompleted:      r.RemoteTransferCompleted,
		LastHeartbeatMessage:         r.LastHeartbeatMessage,
		LastHeartbeatAt:              unixTimestamp(r.LastHeartbeatAtUnix),
		CreatedAt:                    unixTimestamp(r.CreatedAtUnix),
		UpdatedAt:                    unixTimestamp(r.UpdatedAtUnix),
	}
}

func (r drStandbyVolumeRecord) toProto() *adminv1.DRStandbyVolumeSummary {
	return &adminv1.DRStandbyVolumeSummary{
		StandbyVolumeId:                  r.StandbyVolumeID,
		ReplicationLinkId:                r.ReplicationLinkID,
		ShippedManifestId:                r.ShippedManifestID,
		RecoveryPointId:                  r.RecoveryPointID,
		ShippingWorkerId:                 r.ShippingWorkerID,
		SourceClusterId:                  r.SourceClusterID,
		TargetClusterId:                  r.TargetClusterID,
		SourceVolumeId:                   r.SourceVolumeID,
		TargetVolumeId:                   r.TargetVolumeID,
		SourceSnapshotId:                 r.SourceSnapshotID,
		SourceSnapshotRootId:             r.SourceSnapshotRootID,
		BackupArtifactId:                 r.BackupArtifactID,
		StandbyGeneration:                r.StandbyGeneration,
		StandbyState:                     r.StandbyState,
		ImportNodeId:                     r.ImportNodeID,
		ImportEndpoint:                   r.ImportEndpoint,
		ReadViewDigest:                   r.ReadViewDigest,
		KeyPolicyDigest:                  r.KeyPolicyDigest,
		GovernanceDigest:                 r.GovernanceDigest,
		ManifestIntegrityVerified:        r.ManifestIntegrityVerified,
		PayloadRootsBound:                r.PayloadRootsBound,
		ReadViewIdentityBound:            r.ReadViewIdentityBound,
		KeyPolicyBound:                   r.KeyPolicyBound,
		GovernanceMetadataBound:          r.GovernanceMetadataBound,
		RemoteTransferWorkerDeployed:     r.RemoteTransferWorkerDeployed,
		RemoteTransferStarted:            r.RemoteTransferStarted,
		RemoteTransferCompleted:          r.RemoteTransferCompleted,
		StandbyImportVerified:            r.StandbyImportVerified,
		StandbyReadOnlyVerified:          r.StandbyReadOnlyVerified,
		StandbyWriteRejectionRequired:    r.StandbyWriteRejectionRequired,
		StandbyWriteRejected:             r.StandbyWriteRejected,
		StandbyWriteRejectionReason:      r.StandbyWriteRejectionReason,
		PromoteRequiredBeforeTargetWrite: r.PromoteRequiredBeforeTargetWrite,
		TargetWriteAdmitted:              r.TargetWriteAdmitted,
		LastWriteCheckAt:                 unixTimestamp(r.LastWriteCheckAtUnix),
		CreatedAt:                        unixTimestamp(r.CreatedAtUnix),
		UpdatedAt:                        unixTimestamp(r.UpdatedAtUnix),
		PromoteGeneration:                r.PromoteGeneration,
		DemoteGeneration:                 r.DemoteGeneration,
		FencingEpoch:                     r.FencingEpoch,
		PromoteFencingVerified:           r.PromoteFencingVerified,
		DemoteFencingVerified:            r.DemoteFencingVerified,
		StaleOldPrimaryRejected:          r.StaleOldPrimaryRejected,
		OldPrimaryWriteRejectionReason:   r.OldPrimaryWriteRejectionReason,
		OldPrimaryRejoinPolicyDefined:    r.OldPrimaryRejoinPolicyDefined,
		OldPrimaryRejoinPolicy:           r.OldPrimaryRejoinPolicy,
		LastPromoteAt:                    unixTimestamp(r.LastPromoteAtUnix),
		LastDemoteAt:                     unixTimestamp(r.LastDemoteAtUnix),
		LastOldPrimaryWriteCheckAt:       unixTimestamp(r.LastOldPrimaryWriteCheckAtUnix),
		LastOldPrimaryRejoinPolicyAt:     unixTimestamp(r.LastOldPrimaryRejoinPolicyAtUnix),
	}
}

func (r drFailoverDrillRecord) toProto() *adminv1.DRFailoverDrillSummary {
	return &adminv1.DRFailoverDrillSummary{
		FailoverDrillId:                    r.FailoverDrillID,
		StandbyVolumeId:                    r.StandbyVolumeID,
		ReplicationLinkId:                  r.ReplicationLinkID,
		RecoveryPointId:                    r.RecoveryPointID,
		ShippedManifestId:                  r.ShippedManifestID,
		ShippingWorkerId:                   r.ShippingWorkerID,
		SourceClusterId:                    r.SourceClusterID,
		TargetClusterId:                    r.TargetClusterID,
		SourceVolumeId:                     r.SourceVolumeID,
		TargetVolumeId:                     r.TargetVolumeID,
		FailoverDrillGeneration:            r.FailoverDrillGeneration,
		FailoverDrillState:                 r.FailoverDrillState,
		SourceUnavailableObserved:          r.SourceUnavailableObserved,
		TargetPromoteObserved:              r.TargetPromoteObserved,
		TargetMounted:                      r.TargetMounted,
		MountReadbackMatched:               r.MountReadbackMatched,
		ApplicationVisibleIdentityVerified: r.ApplicationVisibleIdentityVerified,
		CleanupRejoinStateVerified:         r.CleanupRejoinStateVerified,
		OldPrimaryRejoinPolicyVerified:     r.OldPrimaryRejoinPolicyVerified,
		FailoverDrillCompleted:             r.FailoverDrillCompleted,
		PromoteGeneration:                  r.PromoteGeneration,
		DemoteGeneration:                   r.DemoteGeneration,
		FencingEpoch:                       r.FencingEpoch,
		StaleOldPrimaryRejected:            r.StaleOldPrimaryRejected,
		OldPrimaryWriteRejectionReason:     r.OldPrimaryWriteRejectionReason,
		OldPrimaryRejoinPolicyDefined:      r.OldPrimaryRejoinPolicyDefined,
		OldPrimaryRejoinPolicy:             r.OldPrimaryRejoinPolicy,
		SupportClaimed:                     false,
		DrSupportClaimed:                   false,
		FailoverClaimed:                    false,
		PublicApiRegistered:                false,
		PublicCliRegistered:                false,
		CreatedAt:                          unixTimestamp(r.CreatedAtUnix),
		UpdatedAt:                          unixTimestamp(r.UpdatedAtUnix),
	}
}

func (r drShippingManifestRecord) toProto() *adminv1.DRShippingManifestSummary {
	return &adminv1.DRShippingManifestSummary{
		ShippedManifestId:            r.ShippedManifestID,
		ReplicationLinkId:            r.ReplicationLinkID,
		RecoveryPointId:              r.RecoveryPointID,
		SourceClusterId:              r.SourceClusterID,
		TargetClusterId:              r.TargetClusterID,
		SourceVolumeId:               r.SourceVolumeID,
		TargetVolumeId:               r.TargetVolumeID,
		SourceSnapshotId:             r.SourceSnapshotID,
		SourceSnapshotRootId:         r.SourceSnapshotRootID,
		BackupArtifactId:             r.BackupArtifactID,
		ManifestGeneration:           r.ManifestGeneration,
		ManifestState:                r.ManifestState,
		ManifestDigest:               r.ManifestDigest,
		PayloadRootsDigest:           r.PayloadRootsDigest,
		ReadViewDigest:               r.ReadViewDigest,
		KeyPolicyDigest:              r.KeyPolicyDigest,
		GovernanceDigest:             r.GovernanceDigest,
		ManifestIntegrityVerified:    r.ManifestIntegrityVerified,
		PayloadRootsBound:            r.PayloadRootsBound,
		ReadViewIdentityBound:        r.ReadViewIdentityBound,
		KeyPolicyBound:               r.KeyPolicyBound,
		GovernanceMetadataBound:      r.GovernanceMetadataBound,
		RemoteTransferWorkerDeployed: r.RemoteTransferWorkerDeployed,
		CreatedAt:                    unixTimestamp(r.CreatedAtUnix),
		UpdatedAt:                    unixTimestamp(r.UpdatedAtUnix),
	}
}
