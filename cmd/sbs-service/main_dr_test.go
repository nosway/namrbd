package main

import (
	"context"
	"testing"
	"time"

	adminv1 "github.com/nosway/namrbd/sbs/admin/v1"
	clustermeta "github.com/nosway/namrbd/sbs/cluster/metadata"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestPhaseUDRReplicationLinkProductStateLifecycle(t *testing.T) {
	ctx := context.Background()
	srv := newTestDRServer(t)
	cluster := &adminv1.ClusterRef{ClusterId: "phase-u-source-cluster", SbsClusterId: "test-sbs"}
	meta := &adminv1.RequestMeta{Actor: "phase-u-test", Reason: "u-ctrl-001"}

	createResp, err := srv.CreateDRReplicationLink(ctx, &adminv1.CreateDRReplicationLinkRequest{
		Cluster:             cluster,
		Meta:                meta,
		ReplicationLinkId:   "phase-u-link-a",
		TargetClusterId:     "phase-u-target-cluster",
		SourceVolumeId:      "vol-source-a",
		TargetVolumeId:      "vol-target-a",
		RecoveryPointPolicy: "manual",
		ShippingMode:        "manual",
	})
	if err != nil {
		t.Fatalf("CreateDRReplicationLink: %v", err)
	}
	link := createResp.GetLink()
	if link.GetSourceClusterId() != "phase-u-source-cluster" {
		t.Fatalf("source cluster=%q want phase-u-source-cluster", link.GetSourceClusterId())
	}
	if link.GetReplicationLinkGeneration() != 1 {
		t.Fatalf("generation=%d want 1", link.GetReplicationLinkGeneration())
	}
	if link.GetReplicationLinkState() != drReplicationLinkStateConfigured {
		t.Fatalf("state=%q want %q", link.GetReplicationLinkState(), drReplicationLinkStateConfigured)
	}
	if link.GetShippingWorkerDeployed() || link.GetStandbyImportSupported() || link.GetPromoteSupported() || link.GetFailoverSupported() {
		t.Fatalf("U-CTRL-001 must not claim deployed DR support: %+v", link)
	}

	getResp, err := srv.GetDRReplicationLink(ctx, &adminv1.GetDRReplicationLinkRequest{
		Cluster:           cluster,
		ReplicationLinkId: "phase-u-link-a",
	})
	if err != nil {
		t.Fatalf("GetDRReplicationLink: %v", err)
	}
	if getResp.GetLink().GetTargetVolumeId() != "vol-target-a" {
		t.Fatalf("target volume=%q want vol-target-a", getResp.GetLink().GetTargetVolumeId())
	}

	updateResp, err := srv.CreateDRReplicationLink(ctx, &adminv1.CreateDRReplicationLinkRequest{
		Cluster:             cluster,
		Meta:                meta,
		ReplicationLinkId:   "phase-u-link-a",
		TargetClusterId:     "phase-u-target-cluster",
		SourceVolumeId:      "vol-source-a",
		TargetVolumeId:      "vol-target-b",
		RecoveryPointPolicy: "manual",
		ShippingMode:        "scheduled",
	})
	if err != nil {
		t.Fatalf("CreateDRReplicationLink update: %v", err)
	}
	if updateResp.GetLink().GetReplicationLinkGeneration() != 2 {
		t.Fatalf("updated generation=%d want 2", updateResp.GetLink().GetReplicationLinkGeneration())
	}
	if updateResp.GetLink().GetCreatedAt().AsTime() != link.GetCreatedAt().AsTime() {
		t.Fatalf("created_at changed: got %s want %s", updateResp.GetLink().GetCreatedAt().AsTime(), link.GetCreatedAt().AsTime())
	}

	if _, err := srv.CreateDRReplicationLink(ctx, &adminv1.CreateDRReplicationLinkRequest{
		Cluster:           cluster,
		Meta:              meta,
		ReplicationLinkId: "phase-u-link-b",
		TargetClusterId:   "other-target-cluster",
		SourceVolumeId:    "vol-source-b",
		TargetVolumeId:    "vol-target-c",
	}); err != nil {
		t.Fatalf("CreateDRReplicationLink second link: %v", err)
	}

	listResp, err := srv.ListDRReplicationLinks(ctx, &adminv1.ListDRReplicationLinksRequest{
		Cluster:        cluster,
		SourceVolumeId: "vol-source-a",
	})
	if err != nil {
		t.Fatalf("ListDRReplicationLinks: %v", err)
	}
	if got := len(listResp.GetLinks()); got != 1 {
		t.Fatalf("filtered links=%d want 1", got)
	}
	if listResp.GetLinks()[0].GetReplicationLinkId() != "phase-u-link-a" {
		t.Fatalf("filtered link=%q want phase-u-link-a", listResp.GetLinks()[0].GetReplicationLinkId())
	}

	if _, err := srv.CreateDRReplicationLink(ctx, &adminv1.CreateDRReplicationLinkRequest{
		Cluster:           cluster,
		Meta:              meta,
		ReplicationLinkId: "missing-target-cluster",
		SourceVolumeId:    "vol-source-a",
		TargetVolumeId:    "vol-target-a",
	}); err == nil {
		t.Fatalf("CreateDRReplicationLink accepted missing target_cluster_id")
	}
}

func TestPhaseUDRRecoveryPointAndShippingManifestProductState(t *testing.T) {
	ctx := context.Background()
	srv := newTestDRServer(t)
	cluster := &adminv1.ClusterRef{ClusterId: "phase-u-source-cluster", SbsClusterId: "test-sbs"}
	meta := &adminv1.RequestMeta{Actor: "phase-u-test", Reason: "u-ctrl-002"}

	if _, err := srv.CreateDRRecoveryPoint(ctx, &adminv1.CreateDRRecoveryPointRequest{
		Cluster:              cluster,
		Meta:                 meta,
		RecoveryPointId:      "rp-missing-link",
		ReplicationLinkId:    "missing-link",
		SourceSnapshotId:     "snap-a",
		SourceSnapshotRootId: "root-a",
		ConsistencyPointId:   "cp-a",
	}); err == nil {
		t.Fatalf("CreateDRRecoveryPoint accepted missing replication link")
	}

	if _, err := srv.CreateDRReplicationLink(ctx, &adminv1.CreateDRReplicationLinkRequest{
		Cluster:           cluster,
		Meta:              meta,
		ReplicationLinkId: "phase-u-link-a",
		TargetClusterId:   "phase-u-target-cluster",
		SourceVolumeId:    "vol-source-a",
		TargetVolumeId:    "vol-target-a",
	}); err != nil {
		t.Fatalf("CreateDRReplicationLink: %v", err)
	}

	rpResp, err := srv.CreateDRRecoveryPoint(ctx, &adminv1.CreateDRRecoveryPointRequest{
		Cluster:              cluster,
		Meta:                 meta,
		RecoveryPointId:      "rp-a",
		ReplicationLinkId:    "phase-u-link-a",
		SourceSnapshotId:     "snap-a",
		SourceSnapshotRootId: "snap-root-a",
		ConsistencyPointId:   "cp-a",
		BackupArtifactId:     "artifact-a",
	})
	if err != nil {
		t.Fatalf("CreateDRRecoveryPoint: %v", err)
	}
	rp := rpResp.GetRecoveryPoint()
	if rp.GetRecoveryPointGeneration() != 1 {
		t.Fatalf("recovery point generation=%d want 1", rp.GetRecoveryPointGeneration())
	}
	if rp.GetRecoveryPointState() != drRecoveryPointStateRecorded {
		t.Fatalf("recovery point state=%q want %q", rp.GetRecoveryPointState(), drRecoveryPointStateRecorded)
	}
	if rp.GetSourceVolumeId() != "vol-source-a" || rp.GetTargetVolumeId() != "vol-target-a" {
		t.Fatalf("recovery point did not inherit link volumes: %+v", rp)
	}
	if rp.GetRemoteTransferWorkerDeployed() {
		t.Fatalf("U-CTRL-002 must not claim remote transfer worker deployment: %+v", rp)
	}

	rpUpdate, err := srv.CreateDRRecoveryPoint(ctx, &adminv1.CreateDRRecoveryPointRequest{
		Cluster:              cluster,
		Meta:                 meta,
		RecoveryPointId:      "rp-a",
		ReplicationLinkId:    "phase-u-link-a",
		SourceSnapshotId:     "snap-b",
		SourceSnapshotRootId: "snap-root-b",
		ConsistencyPointId:   "cp-b",
		BackupArtifactId:     "artifact-b",
	})
	if err != nil {
		t.Fatalf("CreateDRRecoveryPoint update: %v", err)
	}
	if rpUpdate.GetRecoveryPoint().GetRecoveryPointGeneration() != 2 {
		t.Fatalf("updated recovery point generation=%d want 2", rpUpdate.GetRecoveryPoint().GetRecoveryPointGeneration())
	}

	if _, err := srv.CreateDRReplicationLink(ctx, &adminv1.CreateDRReplicationLinkRequest{
		Cluster:           cluster,
		Meta:              meta,
		ReplicationLinkId: "phase-u-link-b",
		TargetClusterId:   "phase-u-target-cluster-b",
		SourceVolumeId:    "vol-source-b",
		TargetVolumeId:    "vol-target-b",
	}); err != nil {
		t.Fatalf("CreateDRReplicationLink second link: %v", err)
	}
	if _, err := srv.CreateDRRecoveryPoint(ctx, &adminv1.CreateDRRecoveryPointRequest{
		Cluster:              cluster,
		Meta:                 meta,
		RecoveryPointId:      "rp-a",
		ReplicationLinkId:    "phase-u-link-b",
		SourceSnapshotId:     "snap-c",
		SourceSnapshotRootId: "snap-root-c",
		ConsistencyPointId:   "cp-c",
	}); err == nil {
		t.Fatalf("CreateDRRecoveryPoint allowed parent link change")
	}

	rpList, err := srv.ListDRRecoveryPoints(ctx, &adminv1.ListDRRecoveryPointsRequest{
		Cluster:           cluster,
		ReplicationLinkId: "phase-u-link-a",
	})
	if err != nil {
		t.Fatalf("ListDRRecoveryPoints: %v", err)
	}
	if got := len(rpList.GetRecoveryPoints()); got != 1 {
		t.Fatalf("recovery point list=%d want 1", got)
	}

	if _, err := srv.CreateDRShippingManifest(ctx, &adminv1.CreateDRShippingManifestRequest{
		Cluster:            cluster,
		Meta:               meta,
		ShippedManifestId:  "manifest-missing-rp",
		RecoveryPointId:    "missing-rp",
		ManifestDigest:     "sha256:manifest",
		PayloadRootsDigest: "sha256:payload",
		ReadViewDigest:     "sha256:read-view",
		KeyPolicyDigest:    "sha256:key-policy",
		GovernanceDigest:   "sha256:governance",
	}); err == nil {
		t.Fatalf("CreateDRShippingManifest accepted missing recovery point")
	}

	manifestResp, err := srv.CreateDRShippingManifest(ctx, &adminv1.CreateDRShippingManifestRequest{
		Cluster:                   cluster,
		Meta:                      meta,
		ShippedManifestId:         "manifest-a",
		RecoveryPointId:           "rp-a",
		ManifestDigest:            "sha256:manifest-a",
		PayloadRootsDigest:        "sha256:payload-a",
		ReadViewDigest:            "sha256:read-view-a",
		KeyPolicyDigest:           "sha256:key-policy-a",
		GovernanceDigest:          "sha256:governance-a",
		ManifestIntegrityVerified: true,
		PayloadRootsBound:         true,
		ReadViewIdentityBound:     true,
		KeyPolicyBound:            true,
		GovernanceMetadataBound:   true,
	})
	if err != nil {
		t.Fatalf("CreateDRShippingManifest: %v", err)
	}
	manifest := manifestResp.GetManifest()
	if manifest.GetManifestGeneration() != 1 {
		t.Fatalf("manifest generation=%d want 1", manifest.GetManifestGeneration())
	}
	if manifest.GetManifestState() != drShippingManifestStateBound {
		t.Fatalf("manifest state=%q want %q", manifest.GetManifestState(), drShippingManifestStateBound)
	}
	if !manifest.GetManifestIntegrityVerified() || !manifest.GetPayloadRootsBound() || !manifest.GetReadViewIdentityBound() || !manifest.GetKeyPolicyBound() || !manifest.GetGovernanceMetadataBound() {
		t.Fatalf("manifest binding booleans not preserved: %+v", manifest)
	}
	if manifest.GetRemoteTransferWorkerDeployed() {
		t.Fatalf("U-CTRL-002 must not claim remote transfer worker deployment: %+v", manifest)
	}

	manifestUpdate, err := srv.CreateDRShippingManifest(ctx, &adminv1.CreateDRShippingManifestRequest{
		Cluster:            cluster,
		Meta:               meta,
		ShippedManifestId:  "manifest-a",
		RecoveryPointId:    "rp-a",
		ManifestDigest:     "sha256:manifest-b",
		PayloadRootsDigest: "sha256:payload-b",
		ReadViewDigest:     "sha256:read-view-b",
		KeyPolicyDigest:    "sha256:key-policy-b",
		GovernanceDigest:   "sha256:governance-b",
		PayloadRootsBound:  true,
	})
	if err != nil {
		t.Fatalf("CreateDRShippingManifest update: %v", err)
	}
	if manifestUpdate.GetManifest().GetManifestGeneration() != 2 {
		t.Fatalf("updated manifest generation=%d want 2", manifestUpdate.GetManifest().GetManifestGeneration())
	}
	if manifestUpdate.GetManifest().GetManifestState() != drShippingManifestStateRecorded {
		t.Fatalf("partial manifest state=%q want %q", manifestUpdate.GetManifest().GetManifestState(), drShippingManifestStateRecorded)
	}

	if _, err := srv.CreateDRRecoveryPoint(ctx, &adminv1.CreateDRRecoveryPointRequest{
		Cluster:              cluster,
		Meta:                 meta,
		RecoveryPointId:      "rp-b",
		ReplicationLinkId:    "phase-u-link-a",
		SourceSnapshotId:     "snap-d",
		SourceSnapshotRootId: "snap-root-d",
		ConsistencyPointId:   "cp-d",
	}); err != nil {
		t.Fatalf("CreateDRRecoveryPoint rp-b: %v", err)
	}
	if _, err := srv.CreateDRShippingManifest(ctx, &adminv1.CreateDRShippingManifestRequest{
		Cluster:            cluster,
		Meta:               meta,
		ShippedManifestId:  "manifest-a",
		RecoveryPointId:    "rp-b",
		ManifestDigest:     "sha256:manifest-c",
		PayloadRootsDigest: "sha256:payload-c",
		ReadViewDigest:     "sha256:read-view-c",
		KeyPolicyDigest:    "sha256:key-policy-c",
		GovernanceDigest:   "sha256:governance-c",
	}); err == nil {
		t.Fatalf("CreateDRShippingManifest allowed parent recovery point change")
	}

	manifestList, err := srv.ListDRShippingManifests(ctx, &adminv1.ListDRShippingManifestsRequest{
		Cluster:         cluster,
		RecoveryPointId: "rp-a",
	})
	if err != nil {
		t.Fatalf("ListDRShippingManifests: %v", err)
	}
	if got := len(manifestList.GetManifests()); got != 1 {
		t.Fatalf("manifest list=%d want 1", got)
	}
}

func TestPhaseUDRShippingWorkerAdmissionProductState(t *testing.T) {
	ctx := context.Background()
	srv := newTestDRServer(t)
	cluster := &adminv1.ClusterRef{ClusterId: "phase-u-source-cluster", SbsClusterId: "test-sbs"}
	meta := &adminv1.RequestMeta{Actor: "phase-u-test", Reason: "u-ctrl-003"}

	if _, err := srv.CreateDRReplicationLink(ctx, &adminv1.CreateDRReplicationLinkRequest{
		Cluster:           cluster,
		Meta:              meta,
		ReplicationLinkId: "phase-u-link-a",
		TargetClusterId:   "phase-u-target-cluster",
		SourceVolumeId:    "vol-source-a",
		TargetVolumeId:    "vol-target-a",
	}); err != nil {
		t.Fatalf("CreateDRReplicationLink: %v", err)
	}
	if _, err := srv.CreateDRRecoveryPoint(ctx, &adminv1.CreateDRRecoveryPointRequest{
		Cluster:              cluster,
		Meta:                 meta,
		RecoveryPointId:      "rp-a",
		ReplicationLinkId:    "phase-u-link-a",
		SourceSnapshotId:     "snap-a",
		SourceSnapshotRootId: "snap-root-a",
		ConsistencyPointId:   "cp-a",
		BackupArtifactId:     "artifact-a",
	}); err != nil {
		t.Fatalf("CreateDRRecoveryPoint: %v", err)
	}
	if _, err := srv.CreateDRShippingManifest(ctx, &adminv1.CreateDRShippingManifestRequest{
		Cluster:            cluster,
		Meta:               meta,
		ShippedManifestId:  "manifest-a",
		RecoveryPointId:    "rp-a",
		ManifestDigest:     "sha256:manifest-a",
		PayloadRootsDigest: "sha256:payload-a",
		ReadViewDigest:     "sha256:read-view-a",
		KeyPolicyDigest:    "sha256:key-policy-a",
		GovernanceDigest:   "sha256:governance-a",
	}); err != nil {
		t.Fatalf("CreateDRShippingManifest unbound: %v", err)
	}
	if _, err := srv.AdmitDRShippingWorker(ctx, &adminv1.AdmitDRShippingWorkerRequest{
		Cluster:              cluster,
		Meta:                 meta,
		ShippingWorkerId:     "worker-a",
		ShippedManifestId:    "manifest-a",
		WorkerNodeId:         "u40",
		TargetEndpoint:       "http://u41:9899",
		CredentialBoundaryId: "cred-boundary-a",
		TransferPlanDigest:   "sha256:plan-a",
	}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("AdmitDRShippingWorker on unbound manifest code=%s err=%v want FailedPrecondition", status.Code(err), err)
	}

	manifestResp, err := srv.CreateDRShippingManifest(ctx, &adminv1.CreateDRShippingManifestRequest{
		Cluster:                   cluster,
		Meta:                      meta,
		ShippedManifestId:         "manifest-a",
		RecoveryPointId:           "rp-a",
		ManifestDigest:            "sha256:manifest-bound",
		PayloadRootsDigest:        "sha256:payload-bound",
		ReadViewDigest:            "sha256:read-view-bound",
		KeyPolicyDigest:           "sha256:key-policy-bound",
		GovernanceDigest:          "sha256:governance-bound",
		ManifestIntegrityVerified: true,
		PayloadRootsBound:         true,
		ReadViewIdentityBound:     true,
		KeyPolicyBound:            true,
		GovernanceMetadataBound:   true,
	})
	if err != nil {
		t.Fatalf("CreateDRShippingManifest bound: %v", err)
	}
	if manifestResp.GetManifest().GetManifestState() != drShippingManifestStateBound {
		t.Fatalf("manifest state=%q want %q", manifestResp.GetManifest().GetManifestState(), drShippingManifestStateBound)
	}

	admitResp, err := srv.AdmitDRShippingWorker(ctx, &adminv1.AdmitDRShippingWorkerRequest{
		Cluster:              cluster,
		Meta:                 meta,
		ShippingWorkerId:     "worker-a",
		ShippedManifestId:    "manifest-a",
		WorkerNodeId:         "u40",
		SourceNodeId:         "u40",
		TargetNodeId:         "u41",
		TargetEndpoint:       "http://u41:9899",
		CredentialBoundaryId: "cred-boundary-a",
		TransferPlanDigest:   "sha256:plan-a",
	})
	if err != nil {
		t.Fatalf("AdmitDRShippingWorker: %v", err)
	}
	worker := admitResp.GetWorker()
	if worker.GetWorkerGeneration() != 1 || worker.GetWorkerState() != drShippingWorkerStateAdmitted {
		t.Fatalf("unexpected worker generation/state: %+v", worker)
	}
	if !worker.GetRemoteTransferWorkerDeployed() || worker.GetRemoteTransferStarted() || worker.GetRemoteTransferCompleted() {
		t.Fatalf("worker crossed remote transfer completion boundary: %+v", worker)
	}
	if !worker.GetManifestIntegrityVerified() || !worker.GetPayloadRootsBound() || !worker.GetReadViewIdentityBound() || !worker.GetKeyPolicyBound() || !worker.GetGovernanceMetadataBound() {
		t.Fatalf("worker missed manifest binding observables: %+v", worker)
	}
	if !admitResp.GetLink().GetShippingWorkerDeployed() ||
		!admitResp.GetRecoveryPoint().GetRemoteTransferWorkerDeployed() ||
		!admitResp.GetManifest().GetRemoteTransferWorkerDeployed() {
		t.Fatalf("admission did not propagate worker-deployed observables: %+v", admitResp)
	}

	if _, err := srv.CreateDRShippingManifest(ctx, &adminv1.CreateDRShippingManifestRequest{
		Cluster:            cluster,
		Meta:               meta,
		ShippedManifestId:  "manifest-a",
		RecoveryPointId:    "rp-a",
		ManifestDigest:     "sha256:manifest-downgrade",
		PayloadRootsDigest: "sha256:payload-downgrade",
		ReadViewDigest:     "sha256:read-view-downgrade",
		KeyPolicyDigest:    "sha256:key-policy-downgrade",
		GovernanceDigest:   "sha256:governance-downgrade",
	}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("manifest downgrade after worker admission code=%s err=%v want FailedPrecondition", status.Code(err), err)
	}

	linkUpdate, err := srv.CreateDRReplicationLink(ctx, &adminv1.CreateDRReplicationLinkRequest{
		Cluster:           cluster,
		Meta:              meta,
		ReplicationLinkId: "phase-u-link-a",
		TargetClusterId:   "phase-u-target-cluster",
		SourceVolumeId:    "vol-source-a",
		TargetVolumeId:    "vol-target-a",
		ShippingMode:      "scheduled",
	})
	if err != nil {
		t.Fatalf("CreateDRReplicationLink after worker admission: %v", err)
	}
	if !linkUpdate.GetLink().GetShippingWorkerDeployed() {
		t.Fatalf("link update dropped shipping_worker_deployed: %+v", linkUpdate.GetLink())
	}
	rpUpdate, err := srv.CreateDRRecoveryPoint(ctx, &adminv1.CreateDRRecoveryPointRequest{
		Cluster:              cluster,
		Meta:                 meta,
		RecoveryPointId:      "rp-a",
		ReplicationLinkId:    "phase-u-link-a",
		SourceSnapshotId:     "snap-b",
		SourceSnapshotRootId: "snap-root-b",
		ConsistencyPointId:   "cp-b",
		BackupArtifactId:     "artifact-b",
	})
	if err != nil {
		t.Fatalf("CreateDRRecoveryPoint after worker admission: %v", err)
	}
	if !rpUpdate.GetRecoveryPoint().GetRemoteTransferWorkerDeployed() {
		t.Fatalf("recovery point update dropped remote_transfer_worker_deployed: %+v", rpUpdate.GetRecoveryPoint())
	}

	heartbeatResp, err := srv.HeartbeatDRShippingWorker(ctx, &adminv1.HeartbeatDRShippingWorkerRequest{
		Cluster:               cluster,
		Meta:                  meta,
		ShippingWorkerId:      "worker-a",
		ObservedState:         "heartbeat_observed",
		HeartbeatMessage:      "ready to ship manifest-a",
		RemoteTransferStarted: true,
	})
	if err != nil {
		t.Fatalf("HeartbeatDRShippingWorker: %v", err)
	}
	if heartbeatResp.GetWorker().GetWorkerGeneration() != 2 ||
		!heartbeatResp.GetWorker().GetRemoteTransferStarted() ||
		heartbeatResp.GetWorker().GetRemoteTransferCompleted() ||
		heartbeatResp.GetWorker().GetLastHeartbeatAt() == nil {
		t.Fatalf("unexpected heartbeat worker state: %+v", heartbeatResp.GetWorker())
	}
	if _, err := srv.HeartbeatDRShippingWorker(ctx, &adminv1.HeartbeatDRShippingWorkerRequest{
		Cluster:                 cluster,
		Meta:                    meta,
		ShippingWorkerId:        "worker-a",
		RemoteTransferCompleted: true,
	}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("completed heartbeat code=%s err=%v want FailedPrecondition", status.Code(err), err)
	}

	listResp, err := srv.ListDRShippingWorkers(ctx, &adminv1.ListDRShippingWorkersRequest{
		Cluster:           cluster,
		ReplicationLinkId: "phase-u-link-a",
		ShippedManifestId: "manifest-a",
	})
	if err != nil {
		t.Fatalf("ListDRShippingWorkers: %v", err)
	}
	if got := len(listResp.GetWorkers()); got != 1 {
		t.Fatalf("shipping worker list=%d want 1", got)
	}
}

func TestPhaseUDRStandbyVolumeImportReadOnlyProductState(t *testing.T) {
	ctx := context.Background()
	srv := newTestDRServer(t)
	cluster := &adminv1.ClusterRef{ClusterId: "phase-u-source-cluster", SbsClusterId: "test-sbs"}
	meta := &adminv1.RequestMeta{Actor: "phase-u-test", Reason: "u-ctrl-004"}

	if _, err := srv.CreateDRReplicationLink(ctx, &adminv1.CreateDRReplicationLinkRequest{
		Cluster:           cluster,
		Meta:              meta,
		ReplicationLinkId: "phase-u-link-a",
		TargetClusterId:   "phase-u-target-cluster",
		SourceVolumeId:    "vol-source-a",
		TargetVolumeId:    "vol-target-a",
	}); err != nil {
		t.Fatalf("CreateDRReplicationLink: %v", err)
	}
	if _, err := srv.CreateDRRecoveryPoint(ctx, &adminv1.CreateDRRecoveryPointRequest{
		Cluster:              cluster,
		Meta:                 meta,
		RecoveryPointId:      "rp-a",
		ReplicationLinkId:    "phase-u-link-a",
		SourceSnapshotId:     "snap-a",
		SourceSnapshotRootId: "snap-root-a",
		ConsistencyPointId:   "cp-a",
		BackupArtifactId:     "artifact-a",
	}); err != nil {
		t.Fatalf("CreateDRRecoveryPoint: %v", err)
	}
	if _, err := srv.CreateDRShippingManifest(ctx, &adminv1.CreateDRShippingManifestRequest{
		Cluster:                   cluster,
		Meta:                      meta,
		ShippedManifestId:         "manifest-a",
		RecoveryPointId:           "rp-a",
		ManifestDigest:            "sha256:manifest-bound",
		PayloadRootsDigest:        "sha256:payload-bound",
		ReadViewDigest:            "sha256:read-view-bound",
		KeyPolicyDigest:           "sha256:key-policy-bound",
		GovernanceDigest:          "sha256:governance-bound",
		ManifestIntegrityVerified: true,
		PayloadRootsBound:         true,
		ReadViewIdentityBound:     true,
		KeyPolicyBound:            true,
		GovernanceMetadataBound:   true,
	}); err != nil {
		t.Fatalf("CreateDRShippingManifest bound: %v", err)
	}
	if _, err := srv.AdmitDRShippingWorker(ctx, &adminv1.AdmitDRShippingWorkerRequest{
		Cluster:              cluster,
		Meta:                 meta,
		ShippingWorkerId:     "worker-a",
		ShippedManifestId:    "manifest-a",
		WorkerNodeId:         "u40",
		SourceNodeId:         "u40",
		TargetNodeId:         "u41",
		TargetEndpoint:       "u41:9443",
		CredentialBoundaryId: "cred-boundary-a",
		TransferPlanDigest:   "sha256:plan-a",
	}); err != nil {
		t.Fatalf("AdmitDRShippingWorker: %v", err)
	}
	if _, err := srv.ImportDRStandbyVolume(ctx, &adminv1.ImportDRStandbyVolumeRequest{
		Cluster:           cluster,
		Meta:              meta,
		StandbyVolumeId:   "standby-a",
		ShippedManifestId: "manifest-a",
		ShippingWorkerId:  "worker-a",
		ImportNodeId:      "u41",
		ImportEndpoint:    "u41:9443",
	}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("ImportDRStandbyVolume before transfer started code=%s err=%v want FailedPrecondition", status.Code(err), err)
	}
	if _, err := srv.HeartbeatDRShippingWorker(ctx, &adminv1.HeartbeatDRShippingWorkerRequest{
		Cluster:               cluster,
		Meta:                  meta,
		ShippingWorkerId:      "worker-a",
		ObservedState:         "heartbeat_observed",
		HeartbeatMessage:      "ready to import standby",
		RemoteTransferStarted: true,
	}); err != nil {
		t.Fatalf("HeartbeatDRShippingWorker: %v", err)
	}

	importResp, err := srv.ImportDRStandbyVolume(ctx, &adminv1.ImportDRStandbyVolumeRequest{
		Cluster:           cluster,
		Meta:              meta,
		StandbyVolumeId:   "standby-a",
		ShippedManifestId: "manifest-a",
		ShippingWorkerId:  "worker-a",
		ImportNodeId:      "u41",
		ImportEndpoint:    "u41:9443",
	})
	if err != nil {
		t.Fatalf("ImportDRStandbyVolume: %v", err)
	}
	standby := importResp.GetStandby()
	if standby.GetStandbyGeneration() != 1 || standby.GetStandbyState() != drStandbyVolumeStateReadOnly {
		t.Fatalf("unexpected standby generation/state: %+v", standby)
	}
	if !standby.GetStandbyImportVerified() || !standby.GetStandbyReadOnlyVerified() || !standby.GetStandbyWriteRejectionRequired() {
		t.Fatalf("standby import missed read-only observables: %+v", standby)
	}
	if standby.GetRemoteTransferCompleted() || standby.GetTargetWriteAdmitted() || !standby.GetPromoteRequiredBeforeTargetWrite() {
		t.Fatalf("standby crossed promote/transfer boundary: %+v", standby)
	}
	if standby.GetReadViewDigest() != "sha256:read-view-bound" || standby.GetKeyPolicyDigest() != "sha256:key-policy-bound" || standby.GetGovernanceDigest() != "sha256:governance-bound" {
		t.Fatalf("standby missed manifest binding digests: %+v", standby)
	}
	if !importResp.GetLink().GetStandbyImportSupported() || importResp.GetLink().GetPromoteSupported() || importResp.GetLink().GetFailoverSupported() {
		t.Fatalf("link crossed support boundary after standby import: %+v", importResp.GetLink())
	}

	checkResp, err := srv.CheckDRStandbyVolumeWrite(ctx, &adminv1.CheckDRStandbyVolumeWriteRequest{
		Cluster:         cluster,
		Meta:            meta,
		StandbyVolumeId: "standby-a",
		WriterId:        "phase-u-test-writer",
	})
	if err != nil {
		t.Fatalf("CheckDRStandbyVolumeWrite: %v", err)
	}
	if !checkResp.GetStandbyWriteRejected() ||
		checkResp.GetStandbyWriteRejectionReason() != drStandbyWriteRejectionReason ||
		checkResp.GetTargetWriteAdmitted() {
		t.Fatalf("standby write check did not reject before promote: %+v", checkResp)
	}
	if !checkResp.GetStandby().GetStandbyWriteRejected() || checkResp.GetStandby().GetLastWriteCheckAt() == nil {
		t.Fatalf("standby write rejection was not persisted: %+v", checkResp.GetStandby())
	}

	getResp, err := srv.GetDRStandbyVolume(ctx, &adminv1.GetDRStandbyVolumeRequest{
		Cluster:         cluster,
		StandbyVolumeId: "standby-a",
	})
	if err != nil {
		t.Fatalf("GetDRStandbyVolume: %v", err)
	}
	if !getResp.GetStandby().GetStandbyWriteRejected() {
		t.Fatalf("get standby dropped persisted write rejection: %+v", getResp.GetStandby())
	}
	listResp, err := srv.ListDRStandbyVolumes(ctx, &adminv1.ListDRStandbyVolumesRequest{
		Cluster:           cluster,
		ReplicationLinkId: "phase-u-link-a",
		ShippedManifestId: "manifest-a",
	})
	if err != nil {
		t.Fatalf("ListDRStandbyVolumes: %v", err)
	}
	if got := len(listResp.GetStandbys()); got != 1 {
		t.Fatalf("standby volume list=%d want 1", got)
	}

	promoteResp, err := srv.PromoteDRStandbyVolume(ctx, &adminv1.PromoteDRStandbyVolumeRequest{
		Cluster:           cluster,
		Meta:              meta,
		StandbyVolumeId:   "standby-a",
		AttachmentId:      "phase-u-target-attachment-a",
		SnapshotLineageId: "phase-u-snapshot-lineage-a",
		FencingReason:     "u-ctrl-005 promote authority test",
	})
	if err != nil {
		t.Fatalf("PromoteDRStandbyVolume: %v", err)
	}
	promoted := promoteResp.GetStandby()
	if promoted.GetStandbyState() != drStandbyVolumeStatePromoted ||
		promoteResp.GetPromoteGeneration() != 1 ||
		promoteResp.GetFencingEpoch() != 1 ||
		!promoted.GetPromoteFencingVerified() ||
		!promoteResp.GetTargetWriteAdmitted() ||
		promoted.GetPromoteRequiredBeforeTargetWrite() {
		t.Fatalf("promote did not advance fenced target write authority: %+v", promoteResp)
	}
	if !promoteResp.GetLink().GetPromoteSupported() || promoteResp.GetLink().GetFailoverSupported() {
		t.Fatalf("promote crossed failover support boundary: %+v", promoteResp.GetLink())
	}

	admittedResp, err := srv.CheckDRStandbyVolumeWrite(ctx, &adminv1.CheckDRStandbyVolumeWriteRequest{
		Cluster:         cluster,
		Meta:            meta,
		StandbyVolumeId: "standby-a",
		WriterId:        "phase-u-promoted-writer",
	})
	if err != nil {
		t.Fatalf("CheckDRStandbyVolumeWrite after promote: %v", err)
	}
	if admittedResp.GetStandbyWriteRejected() || !admittedResp.GetTargetWriteAdmitted() {
		t.Fatalf("promoted standby write check was not admitted: %+v", admittedResp)
	}

	oldPrimaryResp, err := srv.CheckDROldPrimaryWrite(ctx, &adminv1.CheckDROldPrimaryWriteRequest{
		Cluster:         cluster,
		Meta:            meta,
		StandbyVolumeId: "standby-a",
		WriterId:        "phase-u-old-primary-writer",
		SourceNodeId:    "u40",
	})
	if err != nil {
		t.Fatalf("CheckDROldPrimaryWrite: %v", err)
	}
	if !oldPrimaryResp.GetStaleOldPrimaryRejected() ||
		oldPrimaryResp.GetOldPrimaryWriteRejectionReason() != drOldPrimaryWriteRejectionReason ||
		!oldPrimaryResp.GetTargetWriteAdmitted() {
		t.Fatalf("old primary fencing was not observed after promote: %+v", oldPrimaryResp)
	}

	rejoinResp, err := srv.DefineDROldPrimaryRejoinPolicy(ctx, &adminv1.DefineDROldPrimaryRejoinPolicyRequest{
		Cluster:                cluster,
		Meta:                   meta,
		StandbyVolumeId:        "standby-a",
		OldPrimaryRejoinPolicy: drOldPrimaryRejoinPolicyExplicit,
	})
	if err != nil {
		t.Fatalf("DefineDROldPrimaryRejoinPolicy: %v", err)
	}
	if !rejoinResp.GetOldPrimaryRejoinPolicyDefined() ||
		rejoinResp.GetOldPrimaryRejoinPolicy() != drOldPrimaryRejoinPolicyExplicit ||
		rejoinResp.GetAutomaticOldPrimaryRejoin() {
		t.Fatalf("old primary rejoin policy crossed automatic rejoin boundary: %+v", rejoinResp)
	}

	demoteResp, err := srv.DemoteDRStandbyVolume(ctx, &adminv1.DemoteDRStandbyVolumeRequest{
		Cluster:         cluster,
		Meta:            meta,
		StandbyVolumeId: "standby-a",
		FencingReason:   "u-ctrl-005 demote authority test",
	})
	if err != nil {
		t.Fatalf("DemoteDRStandbyVolume: %v", err)
	}
	demoted := demoteResp.GetStandby()
	if demoted.GetStandbyState() != drStandbyVolumeStateDemoted ||
		demoteResp.GetDemoteGeneration() != 2 ||
		demoteResp.GetFencingEpoch() != 2 ||
		!demoted.GetDemoteFencingVerified() ||
		demoteResp.GetTargetWriteAdmitted() ||
		!demoted.GetPromoteRequiredBeforeTargetWrite() {
		t.Fatalf("demote did not revoke target write authority: %+v", demoteResp)
	}

	rejectedAfterDemote, err := srv.CheckDRStandbyVolumeWrite(ctx, &adminv1.CheckDRStandbyVolumeWriteRequest{
		Cluster:         cluster,
		Meta:            meta,
		StandbyVolumeId: "standby-a",
		WriterId:        "phase-u-demoted-writer",
	})
	if err != nil {
		t.Fatalf("CheckDRStandbyVolumeWrite after demote: %v", err)
	}
	if !rejectedAfterDemote.GetStandbyWriteRejected() ||
		rejectedAfterDemote.GetStandbyWriteRejectionReason() != drStandbyWriteRejectionReason ||
		rejectedAfterDemote.GetTargetWriteAdmitted() {
		t.Fatalf("demoted standby write check was not rejected: %+v", rejectedAfterDemote)
	}

	if _, err := srv.RunDRFailoverDrill(ctx, &adminv1.RunDRFailoverDrillRequest{
		Cluster:                    cluster,
		Meta:                       meta,
		FailoverDrillId:            "drill-a",
		StandbyVolumeId:            "standby-a",
		SourceUnavailableObserved:  true,
		TargetMounted:              true,
		MountReadbackMatched:       true,
		CleanupRejoinStateVerified: true,
	}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("RunDRFailoverDrill without application identity code=%s err=%v want FailedPrecondition", status.Code(err), err)
	}

	drillResp, err := srv.RunDRFailoverDrill(ctx, &adminv1.RunDRFailoverDrillRequest{
		Cluster:                            cluster,
		Meta:                               meta,
		FailoverDrillId:                    "drill-a",
		StandbyVolumeId:                    "standby-a",
		SourceUnavailableObserved:          true,
		TargetMounted:                      true,
		MountReadbackMatched:               true,
		ApplicationVisibleIdentityVerified: true,
		CleanupRejoinStateVerified:         true,
	})
	if err != nil {
		t.Fatalf("RunDRFailoverDrill: %v", err)
	}
	drill := drillResp.GetDrill()
	if drill.GetFailoverDrillState() != drFailoverDrillStateValidated ||
		!drill.GetFailoverDrillCompleted() ||
		!drill.GetSourceUnavailableObserved() ||
		!drill.GetTargetPromoteObserved() ||
		!drill.GetTargetMounted() ||
		!drill.GetMountReadbackMatched() ||
		!drill.GetApplicationVisibleIdentityVerified() ||
		!drill.GetCleanupRejoinStateVerified() ||
		!drill.GetOldPrimaryRejoinPolicyVerified() ||
		drill.GetPromoteGeneration() != 1 ||
		drill.GetDemoteGeneration() != 2 ||
		drill.GetFencingEpoch() != 2 ||
		!drill.GetStaleOldPrimaryRejected() ||
		drill.GetOldPrimaryWriteRejectionReason() != drOldPrimaryWriteRejectionReason ||
		!drill.GetOldPrimaryRejoinPolicyDefined() {
		t.Fatalf("failover drill did not preserve product observables: %+v", drill)
	}
	if drill.GetSupportClaimed() ||
		drill.GetDrSupportClaimed() ||
		drill.GetFailoverClaimed() ||
		drill.GetPublicApiRegistered() ||
		drill.GetPublicCliRegistered() ||
		drillResp.GetLink().GetFailoverSupported() {
		t.Fatalf("failover drill crossed support/public API boundary: drill=%+v link=%+v", drill, drillResp.GetLink())
	}

	getDrillResp, err := srv.GetDRFailoverDrill(ctx, &adminv1.GetDRFailoverDrillRequest{
		Cluster:         cluster,
		FailoverDrillId: "drill-a",
	})
	if err != nil {
		t.Fatalf("GetDRFailoverDrill: %v", err)
	}
	if !getDrillResp.GetDrill().GetFailoverDrillCompleted() {
		t.Fatalf("get failover drill dropped completion state: %+v", getDrillResp.GetDrill())
	}
	listDrillResp, err := srv.ListDRFailoverDrills(ctx, &adminv1.ListDRFailoverDrillsRequest{
		Cluster:           cluster,
		ReplicationLinkId: "phase-u-link-a",
		StandbyVolumeId:   "standby-a",
	})
	if err != nil {
		t.Fatalf("ListDRFailoverDrills: %v", err)
	}
	if got := len(listDrillResp.GetDrills()); got != 1 {
		t.Fatalf("failover drill list=%d want 1", got)
	}
}

func newTestDRServer(t *testing.T) *server {
	t.Helper()
	kv, err := clustermeta.OpenPebbleKV(t.TempDir())
	if err != nil {
		t.Fatalf("OpenPebbleKV: %v", err)
	}
	t.Cleanup(func() { _ = kv.Close() })

	repo := clustermeta.NewRepository(kv, defaultMetadataRoot)
	srv := &server{
		clusterID:    "phase-u-source-cluster",
		sbsClusterID: "test-sbs",
		nodeID:       "svc-1",
		root:         defaultMetadataRoot,
		startedAt:    time.Now(),
		kv:           kv,
		repo:         repo,
		ops:          newOperationStore(kv, defaultMetadataRoot),
		cache:        newReplicaClientCache(),
		maint:        newMaintenanceSettings(),
	}
	t.Cleanup(func() { srv.cache.Close() })
	return srv
}
