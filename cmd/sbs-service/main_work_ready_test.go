package main

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	clustermeta "github.com/nosway/namrbd/sbs/cluster/metadata"
)

type workReadyCountingKV struct {
	base                 clustermeta.KV
	legacyVolumeListCall int
	catalogListCall      int
	workListCall         int
	placementListCall    int
}

func (k *workReadyCountingKV) Get(ctx context.Context, key string) ([]byte, bool, error) {
	return k.base.Get(ctx, key)
}

func (k *workReadyCountingKV) Set(ctx context.Context, key string, value []byte) error {
	return k.base.Set(ctx, key, value)
}

func (k *workReadyCountingKV) Delete(ctx context.Context, key string) error {
	return k.base.Delete(ctx, key)
}

func (k *workReadyCountingKV) List(ctx context.Context, prefix, cursor string, limit int) ([]string, string, error) {
	if strings.HasSuffix(prefix, "/volumes/") && !strings.Contains(prefix, "/admin/volumes/") {
		k.legacyVolumeListCall++
	}
	if strings.Contains(prefix, "/admin/volumes/") {
		k.catalogListCall++
	}
	if strings.Contains(prefix, "/derived/ad/v1/work-ready/") {
		k.workListCall++
	}
	if strings.Contains(prefix, "/derived/ad/v1/placement-by-node/") {
		k.placementListCall++
	}
	return k.base.List(ctx, prefix, cursor, limit)
}

func (k *workReadyCountingKV) BatchGet(ctx context.Context, keys []string) (map[string][]byte, error) {
	return k.base.(interface {
		BatchGet(context.Context, []string) (map[string][]byte, error)
	}).BatchGet(ctx, keys)
}

func TestRunMaintenanceOnceUsesBoundedWorkReadyConsumerAfterPromotion(t *testing.T) {
	ctx := context.Background()
	base, err := clustermeta.OpenPebbleKV(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer base.Close()
	writer := clustermeta.NewRepository(base, defaultMetadataRoot)
	if err := writer.PutNodeMembership(ctx, clustermeta.NodeMembershipRecord{NodeID: "node-a", Zone: "zone-a", Host: "host-a", LifecycleState: clustermeta.NodeLifecycleActive, HealthState: clustermeta.NodeHealthHealthy}); err != nil {
		t.Fatal(err)
	}
	if page, err := writer.RunVolumeCatalogRebuildPage(ctx, clustermeta.VolumeCatalogPageDefault); err != nil || !page.Ready {
		t.Fatalf("volume catalog rebuild page=%+v err=%v", page, err)
	}
	for attempt := 0; attempt < 10; attempt++ {
		page, err := writer.RunMaintenanceIndexRebuildPage(ctx, "epoch-work-ready-test", 8, 8)
		if err != nil {
			t.Fatal(err)
		}
		if page.Completed {
			break
		}
		if attempt == 9 {
			t.Fatal("maintenance index rebuild did not complete")
		}
	}
	if state, err := writer.PromoteMaintenanceIndexRebuild(ctx, "epoch-work-ready-test"); err != nil || !state.WorkProjectionReady {
		t.Fatalf("promotion state=%+v err=%v", state, err)
	}
	guard := &workReadyCountingKV{base: base}
	srv := &server{
		nodeID: "service-a", root: defaultMetadataRoot, startedAt: time.Unix(1_800_002_000, 0),
		kv: guard, repo: clustermeta.NewRepository(guard, defaultMetadataRoot), maint: newMaintenanceSettings(),
	}
	srv.ready.Store(true)
	if err := srv.runMaintenanceOnce(ctx); err != nil {
		t.Fatal(err)
	}
	if !srv.workProjectionReady.Load() || guard.workListCall == 0 || guard.workListCall > 6 || guard.placementListCall != 1 || guard.legacyVolumeListCall != 0 || guard.catalogListCall != 1 {
		t.Fatalf("work_ready=%t work_pages=%d placement_pages=%d legacy_volume_lists=%d catalog_pages=%d", srv.workProjectionReady.Load(), guard.workListCall, guard.placementListCall, guard.legacyVolumeListCall, guard.catalogListCall)
	}
}

func TestRunMaintenanceWorkReadyOnceSkipsCooledVolume(t *testing.T) {
	ctx := context.Background()
	srv := newTestMaintenanceServer(t)
	baseNow := time.Unix(1_800_002_100, 0)
	srv.now = func() time.Time { return baseNow }
	srv.maintenanceVolumeCooldown = 10 * time.Second
	seedServerMaintenanceWork(t, ctx, srv.repo, "00a1b2e1", "pl-repair-a", "repair")
	seedServerMaintenanceWork(t, ctx, srv.repo, "00a1b2e2", "pl-repair-b", "repair")
	promoteServerMaintenanceProjection(t, ctx, srv.repo)

	ready, err := srv.repo.ListMaintenanceWorkPage(ctx, "repair", clustermeta.MaintenanceWorkStateReady, "", clustermeta.MaintenanceWorkPageDefault)
	if err != nil {
		t.Fatal(err)
	}
	if len(ready.Records) != 2 {
		t.Fatalf("ready records=%+v", ready.Records)
	}
	cooled := ready.Records[0]
	wantClaimed := ready.Records[1]
	srv.markVolumeMaintenanceRun(cooled.VolumeID, baseNow)

	settings := maintenanceSnapshot{
		maxConcurrentRepairs:        1,
		maxConcurrentRebalances:     1,
		maxConcurrentDrains:         1,
		maxTotalConcurrentMovements: 1,
		pauseRebalances:             true,
		pauseDrains:                 true,
	}
	if err := srv.runMaintenanceWorkReadyOnce(ctx, settings); err != nil {
		t.Fatal(err)
	}

	ready, err = srv.repo.ListMaintenanceWorkPage(ctx, "repair", clustermeta.MaintenanceWorkStateReady, "", clustermeta.MaintenanceWorkPageDefault)
	if err != nil {
		t.Fatal(err)
	}
	leased, err := srv.repo.ListMaintenanceWorkPage(ctx, "repair", clustermeta.MaintenanceWorkStateLeased, "", clustermeta.MaintenanceWorkPageDefault)
	if err != nil {
		t.Fatal(err)
	}
	if len(ready.Records) != 1 || ready.Records[0].WorkID != cooled.WorkID {
		t.Fatalf("ready records=%+v want cooled work=%s", ready.Records, cooled.WorkID)
	}
	if len(leased.Records) != 1 || leased.Records[0].WorkID != wantClaimed.WorkID {
		t.Fatalf("leased records=%+v want claimed work=%s", leased.Records, wantClaimed.WorkID)
	}
}

func TestRunMaintenanceOnceSweepsPayloadGCFromBoundedCatalogAfterWorkPromotion(t *testing.T) {
	ctx := context.Background()
	srv := newTestMaintenanceServer(t)
	srv.payloadRoot = t.TempDir()
	srv.maint.pauseRepairs = true
	srv.maint.pauseRebalances = true
	srv.maint.pauseDrains = true
	volumeID := "00a1b2c3"
	if err := srv.repo.PutVolumeSpec(ctx, clustermeta.VolumeSpecRecord{VolumeID: volumeID, SizeBytes: 4096, BlockSize: 4096, ChunkSizeBytes: 4096}); err != nil {
		t.Fatal(err)
	}
	if err := srv.repo.PutVolumeState(ctx, clustermeta.VolumeState{VolumeID: volumeID, Epoch: 1, Revision: 1, Status: clustermeta.VolumeStatusHealthy}); err != nil {
		t.Fatal(err)
	}
	if err := srv.repo.PutNodeMembership(ctx, clustermeta.NodeMembershipRecord{NodeID: "node-a", ReplicaID: "rep-a", LifecycleState: clustermeta.NodeLifecycleActive, HealthState: clustermeta.NodeHealthHealthy}); err != nil {
		t.Fatal(err)
	}
	if err := srv.repo.PutMutationOperation(ctx, clustermeta.MutationOperationRecord{OperationID: "write-retired", VolumeID: volumeID, Kind: "write", State: clustermeta.MutationOperationCommitted, RetiredPhysicalChunkIDs: []uint64{7}}); err != nil {
		t.Fatal(err)
	}
	if err := srv.repo.PutMutationOperation(ctx, clustermeta.MutationOperationRecord{OperationID: clustermeta.PayloadGCMutationOperationID(volumeID), VolumeID: volumeID, Kind: "payload_gc", State: clustermeta.MutationOperationPending, RetiredPhysicalChunkIDs: []uint64{7}}); err != nil {
		t.Fatal(err)
	}
	for attempt := 0; attempt < 100; attempt++ {
		page, err := srv.repo.RunMaintenanceIndexRebuildPage(ctx, "payload-gc-work-ready", 16, 16)
		if err != nil {
			t.Fatal(err)
		}
		if page.Completed {
			break
		}
		if attempt == 99 {
			t.Fatal("maintenance rebuild did not complete")
		}
	}
	if state, err := srv.repo.PromoteMaintenanceIndexRebuild(ctx, "payload-gc-work-ready"); err != nil || !state.WorkProjectionReady {
		t.Fatalf("promotion state=%+v err=%v", state, err)
	}

	if err := srv.runMaintenanceOnce(ctx); err != nil {
		t.Fatal(err)
	}
	operation, err := srv.repo.GetMutationOperation(ctx, volumeID, clustermeta.PayloadGCMutationOperationID(volumeID))
	if err != nil {
		t.Fatal(err)
	}
	if operation.State != clustermeta.MutationOperationCommitted {
		t.Fatalf("payload-gc state=%s", operation.State)
	}
	batch, err := srv.repo.GetMutationOperation(ctx, volumeID, clustermeta.PayloadGCBatchMutationOperationID(volumeID, 0))
	if err != nil {
		t.Fatal(err)
	}
	if batch.State != clustermeta.MutationOperationCommitted || len(batch.RetiredPhysicalChunkIDs) != 1 || batch.RetiredPhysicalChunkIDs[0] != 7 {
		t.Fatalf("payload-gc batch=%+v", batch)
	}
}

func TestPayloadGCCatalogCursorAdvancesOneBoundedPagePerTick(t *testing.T) {
	ctx := context.Background()
	srv := newTestMaintenanceServer(t)
	for i := 1; i <= clustermeta.VolumeCatalogPageDefault+1; i++ {
		volumeID := fmt.Sprintf("%08x", i)
		if err := srv.repo.PutVolumeSpec(ctx, clustermeta.VolumeSpecRecord{VolumeID: volumeID, SizeBytes: 4096, BlockSize: 4096, ChunkSizeBytes: 4096}); err != nil {
			t.Fatal(err)
		}
		if err := srv.repo.PutVolumeState(ctx, clustermeta.VolumeState{VolumeID: volumeID, Epoch: 1, Revision: 1, Status: clustermeta.VolumeStatusHealthy}); err != nil {
			t.Fatal(err)
		}
	}
	for attempt := 0; attempt < 3; attempt++ {
		page, err := srv.repo.RunVolumeCatalogRebuildPage(ctx, clustermeta.VolumeCatalogPageDefault)
		if err != nil {
			t.Fatal(err)
		}
		if page.Ready {
			break
		}
		if attempt == 2 {
			t.Fatal("volume catalog rebuild did not complete")
		}
	}
	settings := maintenanceSnapshot{pausePayloadGCs: true}
	if err := srv.runRetiredPayloadBacklogSweepReadyPage(ctx, settings); err != nil {
		t.Fatal(err)
	}
	if srv.payloadGCCatalogCursor == "" || srv.payloadGCCatalogRevision == 0 {
		t.Fatalf("first page cursor=%q revision=%d", srv.payloadGCCatalogCursor, srv.payloadGCCatalogRevision)
	}
	if err := srv.runRetiredPayloadBacklogSweepReadyPage(ctx, settings); err != nil {
		t.Fatal(err)
	}
	if srv.payloadGCCatalogCursor != "" || srv.payloadGCCatalogRevision != 0 {
		t.Fatalf("completed pass cursor=%q revision=%d", srv.payloadGCCatalogCursor, srv.payloadGCCatalogRevision)
	}
}

func TestMaintenanceWorkClaimLimitEnforcesGlobalMovementCap(t *testing.T) {
	settings := maintenanceSnapshot{
		maxConcurrentRepairs: 3, maxConcurrentRebalances: 2, maxConcurrentDrains: 2,
		maxTotalConcurrentMovements: 1,
	}
	if limit, paused := maintenanceWorkClaimLimit(settings, "drain", 0); limit != 1 || paused {
		t.Fatalf("initial drain limit=%d paused=%t", limit, paused)
	}
	for _, reason := range []string{"repair", "rebalance", "drain"} {
		if limit, paused := maintenanceWorkClaimLimit(settings, reason, 1); limit != 0 || !paused {
			t.Fatalf("reason=%s exhausted limit=%d paused=%t", reason, limit, paused)
		}
	}
}
