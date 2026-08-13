package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"path/filepath"
	"testing"

	clustermeta "github.com/nosway/namrbd/sbs/cluster/metadata"
	clusterpayload "github.com/nosway/namrbd/sbs/cluster/payload"
	clusterreplication "github.com/nosway/namrbd/sbs/cluster/replication"
)

func TestRunMaintenancePayloadGCSweepDeletesUnreferencedAllocationOrphan(t *testing.T) {
	ctx := context.Background()
	metadataPath := filepath.Join(t.TempDir(), "cluster-meta")
	kv, err := clustermeta.OpenPebbleKV(metadataPath)
	if err != nil {
		t.Fatalf("OpenPebbleKV: %v", err)
	}
	repo := clustermeta.NewRepository(kv, "sbs/cluster")

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
		NodeID:         "node-a",
		ReplicaID:      "rep-a",
		LifecycleState: clustermeta.NodeLifecycleActive,
		HealthState:    clustermeta.NodeHealthHealthy,
	}); err != nil {
		t.Fatalf("PutNodeMembership: %v", err)
	}
	if err := kv.Close(); err != nil {
		t.Fatalf("Close metadata kv: %v", err)
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

	results, err := runMaintenancePayloadGCSweep(ctx, metadataPath, "sbs/cluster", payloadRoot, "00a1b2c3")
	if err != nil {
		t.Fatalf("runMaintenancePayloadGCSweep: %v", err)
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
	for _, key := range []string{
		"replicas/rep-a/volumes/00a1b2c3/extents/00000000000000000001/chunks/00000000000000000500",
		"replicas/rep-a/volumes/00a1b2c3/extents/00000000000000000001/chunks/00000000000000000501",
	} {
		if _, found, err := verify.Get(ctx, key); err != nil || !found {
			t.Fatalf("Get(%s) found=%v err=%v", key, found, err)
		}
	}
}

func TestRunMaintenancePayloadGCRemoteDecodesSweepResults(t *testing.T) {
	client := &http.Client{
		Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
			if r.Method != http.MethodPost {
				t.Fatalf("method=%s want=POST", r.Method)
			}
			if r.URL.Path != "/debug/payload-gc" {
				t.Fatalf("path=%q", r.URL.Path)
			}
			if got := r.URL.Query().Get("payload_root"); got != "/srv/payload" {
				t.Fatalf("payload_root=%q", got)
			}
			if got := r.URL.Query().Get("volume_id"); got != "00a1b2c3" {
				t.Fatalf("volume_id=%q", got)
			}
			body, err := json.Marshal([]clusterreplication.LocalPayloadSweepResult{
				{
					VolumeID:       "00a1b2c3",
					ReplicaID:      "rep-a",
					CandidateCount: 3,
					DeletedCount:   1,
					RetainedCount:  2,
				},
			})
			if err != nil {
				t.Fatalf("Marshal: %v", err)
			}
			return &http.Response{
				StatusCode: http.StatusOK,
				Status:     "200 OK",
				Header:     make(http.Header),
				Body:       io.NopCloser(bytes.NewReader(body)),
			}, nil
		}),
	}

	results, err := runMaintenancePayloadGCRemoteWithClient(context.Background(), client, "http://admin.example", "/srv/payload", "00a1b2c3")
	if err != nil {
		t.Fatalf("runMaintenancePayloadGCRemote: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("len(results)=%d want=1", len(results))
	}
	if results[0].DeletedCount != 1 || results[0].RetainedCount != 2 {
		t.Fatalf("results=%+v", results)
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (fn roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) {
	return fn(r)
}
