package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	clustermeta "github.com/nosway/namrbd/sbs/cluster/metadata"
)

func TestDebugECInspectReportsTopology(t *testing.T) {
	ctx := context.Background()
	srv := newTestMaintenanceServer(t)
	srv.leader = &leaderLeaseManager{}
	srv.leader.isLeader.Store(true)

	const volumeID = "00a1b2c3"
	if err := srv.repo.PutVolumeState(ctx, clustermeta.VolumeState{
		VolumeID:          volumeID,
		Epoch:             1,
		Revision:          1,
		Status:            clustermeta.VolumeStatusHealthy,
		RedundancyBackend: clustermeta.RedundancyBackendEC,
	}); err != nil {
		t.Fatalf("PutVolumeState: %v", err)
	}
	if err := srv.putVolumeSpec(ctx, volumeSpecRecord{
		VolumeID:                       volumeID,
		SizeBytes:                      786432,
		BlockSize:                      4096,
		ChunkSizeBytes:                 4096,
		ExtentPageBytes:                786432,
		RedundancyBackend:              clustermeta.RedundancyBackendEC,
		ECProfileID:                    "ec-6-3",
		ECCodecID:                      "rs_vand_gf8",
		ECDataShards:                   6,
		ECParityShards:                 3,
		ECStripeUnitBytes:              131072,
		ECFailureDomain:                "zone",
		ECMaxShardsPerFailureDomain:    3,
		ECMaxUnavailableFailureDomains: 1,
	}); err != nil {
		t.Fatalf("putVolumeSpec: %v", err)
	}
	object := clustermeta.PhysicalObjectRecord{
		VolumeID:      volumeID,
		ObjectID:      "ec:00a1b2c3:0:1",
		BackendType:   clustermeta.PhysicalObjectBackendEC,
		LogicalLength: 786432,
		Generation:    1,
		State:         clustermeta.PhysicalObjectStateCommitted,
		EC: &clustermeta.ECPhysicalObjectDescriptor{
			ProfileID:        "ec-6-3",
			StripeID:         "0",
			StripeGeneration: 1,
			StripeUnitBytes:  131072,
			DataShards:       6,
			CodingShards:     3,
		},
	}
	if err := srv.repo.PutPhysicalObject(ctx, object); err != nil {
		t.Fatalf("PutPhysicalObject: %v", err)
	}
	stripe := clustermeta.ECStripeRecord{
		VolumeID:         volumeID,
		ObjectID:         object.ObjectID,
		ProfileID:        "ec-6-3",
		StripeID:         "0",
		StripeGeneration: 1,
		StripeUnitBytes:  131072,
		DataShards:       6,
		CodingShards:     3,
		TopologyRevision: 1,
		State:            clustermeta.ECStripeStateCommitted,
		Shards: []clustermeta.ECShardRecord{
			{ShardID: 0, Role: clustermeta.ECShardRoleData, RoleIndex: 0, Zone: "zone-a", NodeID: "u01", StoreID: "u01/default", SizeBytes: 131072},
			{ShardID: 1, Role: clustermeta.ECShardRoleData, RoleIndex: 1, Zone: "zone-a", NodeID: "u02", StoreID: "u02/default", SizeBytes: 131072},
			{ShardID: 2, Role: clustermeta.ECShardRoleData, RoleIndex: 2, Zone: "zone-a", NodeID: "u03", StoreID: "u03/default", SizeBytes: 131072},
			{ShardID: 3, Role: clustermeta.ECShardRoleData, RoleIndex: 3, Zone: "zone-b", NodeID: "u04", StoreID: "u04/default", SizeBytes: 131072},
			{ShardID: 4, Role: clustermeta.ECShardRoleData, RoleIndex: 4, Zone: "zone-b", NodeID: "u05", StoreID: "u05/default", SizeBytes: 131072},
			{ShardID: 5, Role: clustermeta.ECShardRoleData, RoleIndex: 5, Zone: "zone-b", NodeID: "u06", StoreID: "u06/default", SizeBytes: 131072},
			{ShardID: 6, Role: clustermeta.ECShardRoleCoding, RoleIndex: 0, Zone: "zone-c", NodeID: "u07", StoreID: "u07/default", SizeBytes: 131072},
			{ShardID: 7, Role: clustermeta.ECShardRoleCoding, RoleIndex: 1, Zone: "zone-c", NodeID: "u08", StoreID: "u08/default", SizeBytes: 131072},
			{ShardID: 8, Role: clustermeta.ECShardRoleCoding, RoleIndex: 2, Zone: "zone-c", NodeID: "u09", StoreID: "u09/default", SizeBytes: 131072},
		},
	}
	if err := srv.repo.PutECStripe(ctx, stripe); err != nil {
		t.Fatalf("PutECStripe: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "/debug/ec/inspect?volume_id="+volumeID+"&stripe_id=0", nil)
	rr := httptest.NewRecorder()
	srv.handleDebugECInspect(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", rr.Code, rr.Body.String())
	}
	var out map[string]any
	if err := json.Unmarshal(rr.Body.Bytes(), &out); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if out["backend_type"] != clustermeta.RedundancyBackendEC {
		t.Fatalf("backend_type=%v", out["backend_type"])
	}
	if out["zone_tolerance_ok"] != true {
		t.Fatalf("zone_tolerance_ok=%v", out["zone_tolerance_ok"])
	}
	if out["node_spread_ok"] != true || out["store_spread_ok"] != true {
		t.Fatalf("spread flags node=%v store=%v", out["node_spread_ok"], out["store_spread_ok"])
	}
	if got := out["degraded_shard_count"].(float64); got != 0 {
		t.Fatalf("degraded_shard_count=%v", got)
	}
}
