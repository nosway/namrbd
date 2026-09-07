package metadata

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestCaptureSummaryAuthoritySnapshotRebuildsExactQuiescentBaseline(t *testing.T) {
	ctx := context.Background()
	kv, err := OpenPebbleKV(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer kv.Close()
	repo := NewRepository(kv, "phase-ad-live-bootstrap")
	repo.now = func() time.Time { return time.Unix(5000, 123).UTC() }
	if err := repo.PutNodeMembership(ctx, NodeMembershipRecord{NodeID: "node1", LifecycleState: NodeLifecycleActive, HealthState: NodeHealthHealthy}); err != nil {
		t.Fatal(err)
	}
	state := VolumeState{VolumeID: "00a1b2c3", Epoch: 1, Revision: 1, Status: VolumeStatusHealthy}
	spec := VolumeSpecRecord{VolumeID: state.VolumeID, SizeBytes: 8192, BlockSize: 4096, ChunkSizeBytes: 1024}
	if err := repo.PutVolumeState(ctx, state); err != nil {
		t.Fatal(err)
	}
	if err := repo.PutVolumeSpec(ctx, spec); err != nil {
		t.Fatal(err)
	}
	adminRaw, _ := json.Marshal(summaryAdminOperation{OperationID: "op-000001", State: "OPERATION_STATE_COMPLETED"})
	if err := PutAdminOperationListRecord(ctx, kv, repo.root, "op-000001", adminRaw, repo.now()); err != nil {
		t.Fatal(err)
	}
	if err := repo.PutPlacementTransition(ctx, PlacementTransitionRecord{VolumeID: state.VolumeID, PlacementRef: "done", Reason: "repair", State: PlacementTransitionCompleted}); err != nil {
		t.Fatal(err)
	}
	if err := repo.PutMutationOperation(ctx, MutationOperationRecord{OperationID: "mutation-done", VolumeID: state.VolumeID, Kind: "write", State: MutationOperationCommitted}); err != nil {
		t.Fatal(err)
	}

	snapshot, evidence, err := repo.CaptureSummaryAuthoritySnapshot(ctx, 2, 3)
	if err != nil {
		t.Fatal(err)
	}
	if !evidence.Quiescent || evidence.SourceRevision != summaryMutationRevision(repo.now()) || evidence.NodeCount != 1 || !reflect.DeepEqual(evidence.NodeIDs, []string{"node1"}) || evidence.VolumeCount != 1 || evidence.AdminOperationCount != 1 || evidence.TransitionCount != 1 || evidence.MutationOperationCount != 1 || evidence.ContributionCount != 3 {
		t.Fatalf("evidence=%+v", evidence)
	}
	if evidence.RangePageCount == 0 || evidence.BatchGetCount == 0 || evidence.MaximumBatchGetKeyCount > 3 || evidence.BackendFullScanCount != 0 || evidence.FullCompletionCount != 1 || evidence.NestedCompletionCount != 0 {
		t.Fatalf("request evidence=%+v", evidence)
	}
	for {
		page, err := repo.RunSummaryRebuildPage(ctx, snapshot, SummaryKindCluster, "live-bootstrap", 2)
		if err != nil {
			t.Fatal(err)
		}
		if page.Completed {
			break
		}
	}
	if _, err := repo.PromoteSummaryRebuild(ctx, SummaryKindCluster, "live-bootstrap", evidence.SourceRevision); err != nil {
		t.Fatal(err)
	}
	comparison, err := repo.CompareSummaryShadowDiagnostic(ctx, snapshot, SummaryKindCluster, 2)
	if err != nil || !comparison.Match || comparison.ComparedSubjectCount != 3 {
		t.Fatalf("comparison=%+v err=%v", comparison, err)
	}
}

func TestCaptureSummaryAuthoritySnapshotRejectsUnrepresentedDynamicState(t *testing.T) {
	ctx := context.Background()
	kv, err := OpenPebbleKV(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer kv.Close()
	repo := NewRepository(kv, "phase-ad-live-bootstrap-reject")
	for _, nodeID := range []string{"node-a", "node-b", "node-c", "node-d", "node-y", "node-z"} {
		if err := repo.PutNodeMembership(ctx, NodeMembershipRecord{NodeID: nodeID, LifecycleState: NodeLifecycleActive, HealthState: NodeHealthHealthy}); err != nil {
			t.Fatal(err)
		}
	}
	state := VolumeState{VolumeID: "00a1b2c3", Epoch: 1, Revision: 1, Status: VolumeStatusDegraded}
	if err := repo.PutVolumeState(ctx, state); err != nil {
		t.Fatal(err)
	}
	if err := repo.PutVolumeSpec(ctx, VolumeSpecRecord{VolumeID: state.VolumeID, SizeBytes: 4096, BlockSize: 4096, ChunkSizeBytes: 4096}); err != nil {
		t.Fatal(err)
	}
	if err := repo.PutAllocationPage(ctx, AllocationPageRecord{
		VolumeID: state.VolumeID, PageNo: 0, PageBytes: 4096, ChunkSizeBytes: 4096, Revision: 300,
		Extents: []AllocationExtentRecord{{LogicalChunkStart: 0, ChunkCount: 1, Kind: AllocationKindData, PhysicalChunkStart: 7}},
	}); err != nil {
		t.Fatal(err)
	}
	if _, _, err := repo.CreateSnapshotRecord(ctx, SnapshotRecord{
		SnapshotID:               "snapshot-protected",
		SourceVolumeID:           state.VolumeID,
		SnapshotRootID:           "snapshot-protected",
		State:                    SnapshotStateAvailable,
		AllocationChunkSizeBytes: 4096,
		AllocationPageSizeBytes:  4096,
		SourceSizeBytes:          4096,
	}); err != nil {
		t.Fatal(err)
	}
	if err := repo.PutSnapshotAllocationPage(ctx, "snapshot-protected", AllocationPageRecord{
		VolumeID: state.VolumeID, PageNo: 0, PageBytes: 4096, ChunkSizeBytes: 4096, Revision: 200,
		Extents: []AllocationExtentRecord{{LogicalChunkStart: 0, ChunkCount: 1, Kind: AllocationKindData, PhysicalChunkStart: 8}},
	}); err != nil {
		t.Fatal(err)
	}
	if _, _, err := repo.CreateCloneRecord(ctx, CloneRecord{CloneID: "clone-protected", SourceSnapshotID: "snapshot-protected", SourceVolumeID: state.VolumeID, State: CloneStateAvailable}); err != nil {
		t.Fatal(err)
	}
	if err := repo.PutCloneDeltaAllocationPage(ctx, "clone-protected", AllocationPageRecord{
		VolumeID: state.VolumeID, PageNo: 0, PageBytes: 4096, ChunkSizeBytes: 4096, Revision: 400,
		Extents: []AllocationExtentRecord{{LogicalChunkStart: 0, ChunkCount: 1, Kind: AllocationKindData, PhysicalChunkStart: 9}},
	}); err != nil {
		t.Fatal(err)
	}
	if err := repo.PutExtentMapping(ctx, ExtentMappingRecord{VolumeID: state.VolumeID, ExtentID: 1, LogicalOffset: 0, LengthBytes: 4096, PlacementRef: "pl-target", Revision: 300}); err != nil {
		t.Fatal(err)
	}
	if err := repo.PutReplicaSet(ctx, ReplicaSetState{
		ReplicaSetID: "rs-source", VolumeID: state.VolumeID, PlacementRef: "pl-source", Epoch: 1,
		Replicas: []ReplicaDescriptor{{NodeID: "node-a", ReplicaID: "rep-a"}, {NodeID: "node-b", ReplicaID: "rep-b"}, {NodeID: "node-c", ReplicaID: "rep-c"}},
	}); err != nil {
		t.Fatal(err)
	}
	if err := repo.PutReplicaSet(ctx, ReplicaSetState{
		ReplicaSetID: "rs-target", VolumeID: state.VolumeID, PlacementRef: "pl-target", Epoch: 2,
		Replicas: []ReplicaDescriptor{{NodeID: "node-b", ReplicaID: "rep-b-target"}, {NodeID: "node-c", ReplicaID: "rep-c-target"}, {NodeID: "node-d", ReplicaID: "rep-d"}},
	}); err != nil {
		t.Fatal(err)
	}
	if err := repo.PutPlacementTransition(ctx, PlacementTransitionRecord{VolumeID: state.VolumeID, PlacementRef: "pl-source", CurrentReplicaSetID: "rs-source", TargetReplicaSetID: "rs-target", Reason: "rebalance", State: PlacementTransitionCompleted}); err != nil {
		t.Fatal(err)
	}
	if err := repo.PutPlacementTransition(ctx, PlacementTransitionRecord{VolumeID: state.VolumeID, PlacementRef: "active", Reason: "repair", State: PlacementTransitionQueued}); err != nil {
		t.Fatal(err)
	}
	if err := repo.PutMutationOperation(ctx, MutationOperationRecord{OperationID: "transition-pl-source", VolumeID: state.VolumeID, Kind: "transition", State: MutationOperationCommitted, AllocationRevision: 150, IdempotencyKey: "pl-source", AffectedExtentIDs: []uint64{1}, RetiredPhysicalChunkIDs: []uint64{7, 8, 9}, LastUpdatedAtUnix: 175}); err != nil {
		t.Fatal(err)
	}
	if err := repo.PutMutationOperation(ctx, MutationOperationRecord{
		OperationID: "transition-cleaned", VolumeID: state.VolumeID, Kind: "transition", State: MutationOperationCommitted,
		AllocationRevision: 140, IdempotencyKey: "pl-cleaned", AffectedExtentIDs: []uint64{1}, RetiredPhysicalChunkIDs: []uint64{7, 8, 9},
		RetiredReplicaTargetsResolved: true,
		RetiredReplicaTargets:         []MutationRetiredReplicaTarget{{NodeID: "node-z", SourceReplicaIDs: []string{"rep-z"}}},
		LastUpdatedAtUnix:             170,
	}); err != nil {
		t.Fatal(err)
	}
	if err := repo.PutMutationOperation(ctx, MutationOperationRecord{
		OperationID: "transition-missing", VolumeID: state.VolumeID, Kind: "transition", State: MutationOperationCommitted,
		AllocationRevision: 130, IdempotencyKey: "pl-missing", AffectedExtentIDs: []uint64{1}, RetiredPhysicalChunkIDs: []uint64{7, 8, 9}, LastUpdatedAtUnix: 160,
	}); err != nil {
		t.Fatal(err)
	}
	if err := repo.PutMutationOperation(ctx, MutationOperationRecord{OperationID: "payload-gc-active", VolumeID: state.VolumeID, Kind: "payload_gc", State: MutationOperationPending, AllocationRevision: 300, WriterFencingEpoch: 1, AffectedExtentIDs: []uint64{1}, RetiredPhysicalChunkIDs: []uint64{10, 9, 7, 8, 8, 0}, StartedAtUnix: 100, LastUpdatedAtUnix: 200}); err != nil {
		t.Fatal(err)
	}

	snapshot, evidence, err := repo.CaptureSummaryAuthoritySnapshot(ctx, 16, 16)
	if snapshot != nil || !errors.Is(err, ErrSummaryAuthorityNotQuiescent) {
		t.Fatalf("snapshot=%v error=%v", snapshot, err)
	}
	if evidence.Quiescent || evidence.UnrepresentedCount != 4 || len(evidence.UnrepresentedClasses) != 4 || evidence.FirstUnrepresented == "" || len(evidence.UnrepresentedDetails) != 4 || len(evidence.UnrepresentedClassCounts) != 4 {
		t.Fatalf("rejection evidence=%+v", evidence)
	}
	if len(evidence.RejectedMutationOperations) != 1 {
		t.Fatalf("mutation rejection evidence=%+v", evidence.RejectedMutationOperations)
	}
	operation := evidence.RejectedMutationOperations[0]
	if operation.OperationID != "payload-gc-active" || operation.AllocationRevision != 300 || operation.WriterFencingEpoch != 1 || operation.RetiredPhysicalChunkCount != 4 || !reflect.DeepEqual(operation.RetiredPhysicalChunkIDs, []uint64{7, 8, 9, 10}) || operation.ProtectedRetiredPhysicalChunkCount != 3 || !reflect.DeepEqual(operation.ProtectedRetiredPhysicalChunkIDs, []uint64{7, 8, 9}) || operation.UnprotectedRetiredPhysicalChunkCount != 1 || !reflect.DeepEqual(operation.UnprotectedRetiredPhysicalChunkIDs, []uint64{10}) || operation.LastUpdatedAtUnix != 200 {
		t.Fatalf("mutation rejection evidence=%+v", evidence.RejectedMutationOperations)
	}
	if len(operation.ProtectingReadViews) != 3 || operation.ProtectingReadViews[0].Kind != "live_volume" || operation.ProtectingReadViews[0].ID != state.VolumeID || operation.ProtectingReadViews[0].AllocationPageCount != 1 || operation.ProtectingReadViews[0].MinimumPageRevision != 300 || operation.ProtectingReadViews[0].MaximumPageRevision != 300 || !reflect.DeepEqual(operation.ProtectingReadViews[0].RetiredPhysicalChunkIDs, []uint64{7}) || operation.ProtectingReadViews[1].Kind != "snapshot" || operation.ProtectingReadViews[1].ID != "snapshot-protected" || operation.ProtectingReadViews[1].MinimumPageRevision != 200 || !reflect.DeepEqual(operation.ProtectingReadViews[1].RetiredPhysicalChunkIDs, []uint64{8}) || operation.ProtectingReadViews[2].Kind != "clone_delta" || operation.ProtectingReadViews[2].ID != "clone-protected" || operation.ProtectingReadViews[2].MinimumPageRevision != 400 || !reflect.DeepEqual(operation.ProtectingReadViews[2].RetiredPhysicalChunkIDs, []uint64{9}) {
		t.Fatalf("protecting read views=%+v", operation.ProtectingReadViews)
	}
	if operation.CandidateOrigin.OperationCount != 3 || operation.CandidateOrigin.KindCounts["transition"] != 3 || operation.CandidateOrigin.MinimumAllocationRevision != 130 || operation.CandidateOrigin.MaximumAllocationRevision != 150 || operation.CandidateOrigin.EarliestUpdatedAtUnix != 160 || operation.CandidateOrigin.LatestUpdatedAtUnix != 175 {
		t.Fatalf("candidate origin=%+v", operation.CandidateOrigin)
	}
	inspection := operation.StaleReplicaInspection
	if inspection.ResolvedCandidateCount != 3 || inspection.UnresolvedCandidateCount != 1 || !reflect.DeepEqual(inspection.UnresolvedCandidateIDs, []uint64{10}) || inspection.PersistedTargetCandidateCount != 3 || inspection.LiveTransitionTargetCandidateCount != 3 || inspection.ClusterExclusionTargetCandidateCount != 3 || inspection.TargetCount != 3 {
		t.Fatalf("stale replica inspection=%+v", inspection)
	}
	if inspection.Targets[0].NodeID != "node-a" || !reflect.DeepEqual(inspection.Targets[0].SourceReplicaIDs, []string{"rep-a"}) || !reflect.DeepEqual(inspection.Targets[0].AuthorityKinds, []string{"live_transition_source", "noncurrent_cluster_node"}) || !reflect.DeepEqual(inspection.Targets[0].RetiredPhysicalChunkIDs, []uint64{7, 8, 9}) || !reflect.DeepEqual(inspection.Targets[0].OriginOperationIDs, []string{"transition-missing", "transition-pl-source"}) {
		t.Fatalf("node-a inspection target=%+v", inspection.Targets[0])
	}
	if inspection.Targets[1].NodeID != "node-y" || len(inspection.Targets[1].SourceReplicaIDs) != 0 || !reflect.DeepEqual(inspection.Targets[1].AuthorityKinds, []string{"noncurrent_cluster_node"}) || !reflect.DeepEqual(inspection.Targets[1].OriginOperationIDs, []string{"transition-missing"}) {
		t.Fatalf("node-y inspection target=%+v", inspection.Targets[1])
	}
	if inspection.Targets[2].NodeID != "node-z" || !reflect.DeepEqual(inspection.Targets[2].SourceReplicaIDs, []string{"rep-z"}) || !reflect.DeepEqual(inspection.Targets[2].AuthorityKinds, []string{"noncurrent_cluster_node", "persisted_transition_source"}) || !reflect.DeepEqual(inspection.Targets[2].OriginOperationIDs, []string{"transition-cleaned", "transition-missing"}) {
		t.Fatalf("node-z inspection target=%+v", inspection.Targets[2])
	}
}

func TestAddProtectedPhysicalChunksRejectsOverflow(t *testing.T) {
	err := addProtectedPhysicalChunks(map[uint64]struct{}{}, AllocationPageRecord{Extents: []AllocationExtentRecord{{
		Kind: AllocationKindData, PhysicalChunkStart: ^uint64(0), ChunkCount: 2,
	}}})
	if err == nil || !strings.Contains(err.Error(), "overflows uint64") {
		t.Fatalf("overflow error=%v", err)
	}
}

func TestCaptureSummaryAuthoritySnapshotRequiresConsistentReader(t *testing.T) {
	repo := NewRepository(newFakeTransactionalKV(), "phase-ad-live-bootstrap-no-snapshot")
	if _, _, err := repo.CaptureSummaryAuthoritySnapshot(context.Background(), 16, 16); !errors.Is(err, ErrSummaryAuthoritySnapshotRequired) {
		t.Fatalf("error=%v", err)
	}
}

func TestCaptureSummaryAuthoritySnapshotRejectsOrphanVolumeState(t *testing.T) {
	ctx := context.Background()
	kv, err := OpenPebbleKV(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer kv.Close()
	repo := NewRepository(kv, "phase-ad-live-bootstrap-orphan")
	if err := repo.PutVolumeState(ctx, VolumeState{VolumeID: "00a1b2c3", Epoch: 1, Revision: 1, Status: VolumeStatusHealthy}); err != nil {
		t.Fatal(err)
	}
	transitionRaw, err := json.Marshal(PlacementTransitionRecord{VolumeID: "00a1b2c3", PlacementRef: "replica-a", State: PlacementTransitionCompleted})
	if err != nil {
		t.Fatal(err)
	}
	operations := []MutationOperationRecord{
		{OperationID: "op-a", VolumeID: "00a1b2c3", Kind: "write", State: MutationOperationCommitted},
		{OperationID: "op-failed", VolumeID: "00a1b2c3", Kind: "write", State: MutationOperationFailed},
		{OperationID: "op-pending", VolumeID: "00a1b2c3", Kind: "write", State: MutationOperationPending},
		{OperationID: "op-rolled-back", VolumeID: "00a1b2c3", Kind: "write", State: MutationOperationRolledBack},
		{OperationID: "op-running", VolumeID: "00a1b2c3", Kind: "write", State: MutationOperationRunning},
	}
	fixtures := map[string][]byte{
		repo.root + "/volumes/00a1b2c3/meta/next_chunk_id":                                   []byte(`{}`),
		repo.root + "/volumes/00a1b2c3/extents/00000000000000000001":                         []byte(`{}`),
		repo.root + "/volumes/00a1b2c3/allocation/pages/00000000000000000000":                []byte(`{}`),
		repo.root + "/volumes/00a1b2c3/write_state/pages/00000000000000000000":               []byte(`{}`),
		repo.root + "/volumes/00a1b2c3/replicasets/replica-a":                                []byte(`{}`),
		repo.root + "/volumes/00a1b2c3/idem/write-1":                                         []byte(`{}`),
		repo.root + "/volumes/00a1b2c3/snapshots/snapshot-a":                                 []byte(`{}`),
		repo.root + "/volumes/00a1b2c3/clones/clone-a":                                       []byte(`{}`),
		repo.root + "/volumes/00a1b2c3/placements/replica-a/transition":                      transitionRaw,
		repo.root + "/volumes/00a1b2c3/physical_objects/object-a":                            []byte(`{}`),
		repo.root + "/volumes/00a1b2c3/ec/stripes/stripe-a/generations/00000000000000000001": []byte(`{}`),
		repo.root + "/volumes/00a1b2c3/unclassified/value":                                   []byte(`{}`),
	}
	for _, operation := range operations {
		raw, err := json.Marshal(operation)
		if err != nil {
			t.Fatal(err)
		}
		fixtures[mutationOperationKey(repo.root, operation.VolumeID, operation.OperationID)] = raw
	}
	snapshotRaw, err := json.Marshal(SnapshotRecord{SnapshotID: "snapshot-orphan", SourceVolumeID: "00a1b2c3", State: SnapshotStateAvailable})
	if err != nil {
		t.Fatal(err)
	}
	fixtures[snapshotRecordKey(repo.root, "snapshot-orphan")] = snapshotRaw
	cloneRaw, err := json.Marshal(CloneRecord{CloneID: "clone-orphan", SourceSnapshotID: "snapshot-orphan", SourceVolumeID: "00a1b2c3", State: CloneStateFailed})
	if err != nil {
		t.Fatal(err)
	}
	fixtures[cloneRecordKey(repo.root, "clone-orphan")] = cloneRaw
	for key, raw := range fixtures {
		if err := kv.Set(ctx, key, raw); err != nil {
			t.Fatal(err)
		}
	}

	if _, evidence, err := repo.CaptureSummaryAuthoritySnapshot(ctx, 16, 16); err == nil || evidence.Quiescent || !strings.Contains(err.Error(), "has no spec") {
		t.Fatalf("evidence=%+v error=%v", evidence, err)
	} else if evidence.VolumeSpecCount != 0 || evidence.VolumeStateCount != 1 || evidence.OrphanVolumeStateCount != 1 || len(evidence.OrphanVolumeStateIDs) != 1 || evidence.OrphanVolumeStateIDs[0] != "00a1b2c3" || len(evidence.OrphanVolumeStates) != 1 || evidence.UnrepresentedClassCounts["incomplete_volume_authority"] != 1 {
		t.Fatalf("inventory evidence=%+v", evidence)
	} else if len(evidence.OrphanVolumeResiduals) != 1 {
		t.Fatalf("orphan residual evidence=%+v", evidence.OrphanVolumeResiduals)
	} else {
		residual := evidence.OrphanVolumeResiduals[0]
		if residual.VolumeID != "00a1b2c3" || residual.TotalKeyCount != 18 || residual.ResidualKeyCount != 17 || residual.AuthorityStateKeyCount != 1 || residual.ChunkSequenceKeyCount != 1 || residual.ExtentMappingKeyCount != 1 || residual.AllocationPageKeyCount != 1 || residual.WriteStateKeyCount != 1 || residual.ReplicaSetKeyCount != 1 || residual.IdempotencyKeyCount != 1 || residual.SnapshotIndexKeyCount != 1 || residual.CloneIndexKeyCount != 1 || residual.PlacementRecordKeyCount != 1 || residual.MutationOperationKeyCount != 5 || residual.PhysicalObjectKeyCount != 1 || residual.ECStripeKeyCount != 1 || residual.OtherKeyCount != 1 {
			t.Fatalf("orphan residual=%+v", residual)
		}
		if residual.ExactKeyValueCount != residual.TotalKeyCount || len(residual.ExactKeyValueDigestSHA256) != 64 {
			t.Fatalf("orphan exact key-value digest=%+v", residual)
		}
		wantStates := map[string]int{"committed": 1, "failed": 1, "pending": 1, "rolled_back": 1, "running": 1}
		if !reflect.DeepEqual(residual.MutationOperationStateCounts, wantStates) || residual.NonterminalMutationOperationCount != 3 || !reflect.DeepEqual(residual.NonterminalMutationOperationIDs, []string{"op-failed", "op-pending", "op-running"}) {
			t.Fatalf("orphan operation states=%+v", residual)
		}
		if residual.NondeletedSnapshotRecordCount != 1 || !reflect.DeepEqual(residual.NondeletedSnapshotRecordIDs, []string{"snapshot-orphan"}) || residual.NondeletedCloneRecordCount != 1 || !reflect.DeepEqual(residual.NondeletedCloneRecordIDs, []string{"clone-orphan"}) {
			t.Fatalf("orphan read-view references=%+v", residual)
		}
		_, repeated, repeatedErr := repo.CaptureSummaryAuthoritySnapshot(ctx, 16, 16)
		if !errors.Is(repeatedErr, ErrSummaryAuthorityNotQuiescent) || len(repeated.OrphanVolumeResiduals) != 1 || repeated.OrphanVolumeResiduals[0].ExactKeyValueDigestSHA256 != residual.ExactKeyValueDigestSHA256 {
			t.Fatalf("repeated exact digest=%+v error=%v", repeated.OrphanVolumeResiduals, repeatedErr)
		}
		if err := kv.Set(ctx, repo.root+"/volumes/00a1b2c3/idem/write-1", []byte(`{"changed":true}`)); err != nil {
			t.Fatal(err)
		}
		_, changed, changedErr := repo.CaptureSummaryAuthoritySnapshot(ctx, 16, 16)
		if !errors.Is(changedErr, ErrSummaryAuthorityNotQuiescent) || len(changed.OrphanVolumeResiduals) != 1 || changed.OrphanVolumeResiduals[0].ExactKeyValueDigestSHA256 == residual.ExactKeyValueDigestSHA256 {
			t.Fatalf("changed exact digest=%+v error=%v", changed.OrphanVolumeResiduals, changedErr)
		}
	}
}

func TestCaptureSummaryAuthoritySnapshotInventoriesAllVolumePairMismatches(t *testing.T) {
	ctx := context.Background()
	kv, err := OpenPebbleKV(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer kv.Close()
	repo := NewRepository(kv, "phase-ad-live-bootstrap-inventory")
	for _, volumeID := range []string{"00a1b2c3", "00a1b2c4"} {
		if err := repo.PutVolumeState(ctx, VolumeState{VolumeID: volumeID, Epoch: 1, Revision: 1, Status: VolumeStatusHealthy}); err != nil {
			t.Fatal(err)
		}
	}
	for _, volumeID := range []string{"00a1b2c5", "00a1b2c6"} {
		if err := repo.PutVolumeSpec(ctx, VolumeSpecRecord{VolumeID: volumeID, SizeBytes: 4096, BlockSize: 4096, ChunkSizeBytes: 4096}); err != nil {
			t.Fatal(err)
		}
	}

	_, evidence, err := repo.CaptureSummaryAuthoritySnapshot(ctx, 1, 1)
	if !errors.Is(err, ErrSummaryAuthorityNotQuiescent) {
		t.Fatalf("error=%v", err)
	}
	if evidence.VolumeSpecCount != 2 || evidence.VolumeStateCount != 2 || evidence.VolumeCount != 0 || evidence.MissingVolumeStateCount != 2 || evidence.OrphanVolumeStateCount != 2 {
		t.Fatalf("inventory evidence=%+v", evidence)
	}
	if got := strings.Join(evidence.MissingVolumeStateIDs, ","); got != "00a1b2c5,00a1b2c6" {
		t.Fatalf("missing states=%s", got)
	}
	if got := strings.Join(evidence.OrphanVolumeStateIDs, ","); got != "00a1b2c3,00a1b2c4" {
		t.Fatalf("orphan states=%s", got)
	}
}
