package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"testing"
	"time"

	clustermeta "github.com/nosway/namrbd/sbs/cluster/metadata"
	clusterpayload "github.com/nosway/namrbd/sbs/cluster/payload"
	clusterreplication "github.com/nosway/namrbd/sbs/cluster/replication"
)

func TestDebugPayloadGCDeletesAllocationOrphan(t *testing.T) {
	ctx := context.Background()
	metadataPath := filepath.Join(t.TempDir(), "cluster-meta")
	kv, err := clustermeta.OpenPebbleKV(metadataPath)
	if err != nil {
		t.Fatalf("OpenPebbleKV: %v", err)
	}
	defer kv.Close()

	repo := clustermeta.NewRepository(kv, defaultMetadataRoot)
	if err := repo.PutVolumeState(ctx, clustermeta.VolumeState{
		VolumeID: "00a1b2c3",
		Epoch:    5,
		Revision: 10,
		Status:   clustermeta.VolumeStatusHealthy,
	}); err != nil {
		t.Fatalf("PutVolumeState: %v", err)
	}
	if err := repo.PutExtentMapping(ctx, clustermeta.ExtentMappingRecord{
		VolumeID:      "00a1b2c3",
		ExtentID:      1,
		LogicalOffset: 0,
		LengthBytes:   8,
		ChunkID:       11,
		PlacementRef:  "pl-1",
		Revision:      10,
	}); err != nil {
		t.Fatalf("PutExtentMapping: %v", err)
	}
	if err := repo.PutAllocationPage(ctx, clustermeta.AllocationPageRecord{
		VolumeID:       "00a1b2c3",
		PageNo:         0,
		PageBytes:      8,
		ChunkSizeBytes: 4,
		Revision:       10,
		Extents: []clustermeta.AllocationExtentRecord{
			{LogicalChunkStart: 0, ChunkCount: 2, Kind: clustermeta.AllocationKindData, PhysicalChunkStart: 500},
		},
	}); err != nil {
		t.Fatalf("PutAllocationPage: %v", err)
	}
	if err := repo.PutNodeMembership(ctx, clustermeta.NodeMembershipRecord{
		NodeID:            "node-a",
		ReplicaID:         "rep-a",
		LifecycleState:    clustermeta.NodeLifecycleActive,
		HealthState:       clustermeta.NodeHealthHealthy,
		LastHeartbeatUnix: time.Now().Unix(),
	}); err != nil {
		t.Fatalf("PutNodeMembership: %v", err)
	}

	payloadRoot := filepath.Join(t.TempDir(), "payload")
	replicaStores, err := clusterpayload.OpenReplicaStores(payloadRoot, []string{"rep-a"})
	if err != nil {
		t.Fatalf("OpenReplicaStores: %v", err)
	}
	storeA := replicaStores.ObjectStores()["rep-a"]
	for _, key := range []string{
		"replicas/rep-a/volumes/00a1b2c3/extents/00000000000000000001/chunks/00000000000000000500",
		"replicas/rep-a/volumes/00a1b2c3/extents/00000000000000000001/chunks/00000000000000000501",
		"replicas/rep-a/volumes/00a1b2c3/extents/00000000000000000001/chunks/00000000000000000011",
	} {
		if err := storeA.Put(ctx, key, []byte("payload")); err != nil {
			t.Fatalf("Put(%s): %v", key, err)
		}
	}
	if err := replicaStores.Close(); err != nil {
		t.Fatalf("Close replica stores: %v", err)
	}

	srv := &server{
		clusterID:    "test-cluster",
		sbsClusterID: "test-sbs",
		nodeID:       "node-a",
		root:         defaultMetadataRoot,
		startedAt:    time.Now(),
		kv:           kv,
		repo:         repo,
		ops:          newOperationStore(kv, defaultMetadataRoot),
		cache:        newReplicaClientCache(),
		maint:        newMaintenanceSettings(),
	}
	defer srv.cache.Close()
	srv.ready.Store(true)

	q := url.Values{}
	q.Set("payload_root", payloadRoot)
	q.Set("volume_id", "00a1b2c3")
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "/debug/payload-gc?"+q.Encode(), nil)
	if err != nil {
		t.Fatalf("NewRequestWithContext: %v", err)
	}
	recorder := httptest.NewRecorder()
	observabilityMux(srv).ServeHTTP(recorder, req)
	if recorder.Code != http.StatusOK {
		t.Fatalf("status=%d want=200", recorder.Code)
	}

	var results []clusterreplication.LocalPayloadSweepResult
	if err := json.NewDecoder(recorder.Body).Decode(&results); err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if len(results) != 1 || results[0].DeletedCount != 1 || results[0].RetainedCount != 2 {
		t.Fatalf("results=%+v", results)
	}

	verifyStores, err := clusterpayload.OpenReplicaStores(payloadRoot, []string{"rep-a"})
	if err != nil {
		t.Fatalf("Reopen replica stores: %v", err)
	}
	defer verifyStores.Close()
	verify := verifyStores.ObjectStores()["rep-a"]
	if _, found, err := verify.Get(ctx, "replicas/rep-a/volumes/00a1b2c3/extents/00000000000000000001/chunks/00000000000000000011"); err != nil {
		t.Fatalf("Get orphan key err=%v", err)
	} else if found {
		t.Fatalf("orphan key was not deleted")
	}
}
