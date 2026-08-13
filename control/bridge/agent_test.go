package bridge

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/nosway/namrbd/control/netlinktlv"
)

type fakeKernel struct {
	configureErr error
	attachErr    error
	detachErr    error
	manifest     AttachManifest

	configured []RESTServer
}

func (f *fakeKernel) ConfigureRESTServers(ctx context.Context, servers []RESTServer) error {
	f.configured = append([]RESTServer(nil), servers...)
	return f.configureErr
}

func (f *fakeKernel) AttachVolumeViaREST(ctx context.Context, req AttachRequest) (AttachManifest, error) {
	if f.attachErr != nil {
		return AttachManifest{}, f.attachErr
	}
	return f.manifest, nil
}

func (f *fakeKernel) DetachVolumeViaREST(ctx context.Context, hostID string, volumeID uint64) error {
	return f.detachErr
}

func TestBuildNetlinkPayload(t *testing.T) {
	servers := []RESTServer{
		{ID: 1, Address: "10.0.0.10", Port: 9701, UseTLS: true, APIPrefix: "/api/v1"},
	}
	raw, err := BuildNetlinkPayload(servers)
	if err != nil {
		t.Fatalf("BuildNetlinkPayload failed: %v", err)
	}
	decoded, err := netlinktlv.DecodeConfigRESTRequest(raw)
	if err != nil {
		t.Fatalf("DecodeConfigRESTRequest failed: %v", err)
	}
	if decoded.DeviceID != 0 || len(decoded.Servers) != 1 || decoded.Servers[0].Address != "10.0.0.10" {
		t.Fatalf("unexpected payload decode: %+v", decoded)
	}
}

func TestAgentConfigureKernelREST(t *testing.T) {
	k := &fakeKernel{}
	a := Agent{Kernel: k}

	servers := []RESTServer{{ID: 1, Address: "127.0.0.1", Port: 9701}}
	if err := a.ConfigureKernelREST(context.Background(), servers); err != nil {
		t.Fatalf("ConfigureKernelREST failed: %v", err)
	}
	if len(k.configured) != 1 || k.configured[0].ID != 1 {
		t.Fatalf("configure not applied: %+v", k.configured)
	}
}

func TestAgentAttachDetach(t *testing.T) {
	k := &fakeKernel{
		manifest: AttachManifest{
			VolumeID:   10,
			Generation: 20,
		},
	}
	a := Agent{Kernel: k}
	got, err := a.AttachVolume(context.Background(), AttachRequest{HostID: "h1", VolumeID: 10})
	if err != nil {
		t.Fatalf("AttachVolume failed: %v", err)
	}
	if got.Generation != 20 {
		t.Fatalf("unexpected manifest: %+v", got)
	}
	if err := a.DetachVolume(context.Background(), "h1", 10); err != nil {
		t.Fatalf("DetachVolume failed: %v", err)
	}
}

func TestAgentErrors(t *testing.T) {
	k := &fakeKernel{attachErr: errors.New("attach failed")}
	a := Agent{Kernel: k}
	if _, err := a.AttachVolume(context.Background(), AttachRequest{HostID: "h", VolumeID: 1}); err == nil {
		t.Fatalf("expected attach error")
	}

	k = &fakeKernel{detachErr: errors.New("detach failed")}
	a = Agent{Kernel: k}
	if err := a.DetachVolume(context.Background(), "h", 1); err == nil {
		t.Fatalf("expected detach error")
	}
}

func TestPrepareAttachManifest(t *testing.T) {
	raw := `{
		"volume_id":"00000065",
		"generation":3,
		"attachment_generation":3,
		"size_bytes":8388608,
		"block_size":4096,
		"attachment_id":"att-00000065-0003",
		"attached_host_id":"host-a",
		"attached_device_id":0,
		"writer_fencing_epoch":9,
		"runtime_path_expansion_eligible_at_unix":4102444800,
		"handoff_required":true,
		"handoff_reason":"current_gateway_not_desired",
		"handoff_target_gateway_set":["gw-b"],
		"controller_priority_class":"handoff",
		"controller_recommended_actions":["complete_gateway_handoff","prefer_fewer_active_paths"],
		"cluster_priority_mismatch_actions":["complete_gateway_handoff_aggressively"],
		"max_inflight_requests":64,
		"max_inflight_bytes":1048576,
		"max_io_size":131072,
		"max_zero_like_io_size":268435456,
		"initial_zero_map_trusted":true,
		"initial_zero_map_all_zero":true,
		"initial_zero_map_granule_bytes":65536,
		"initial_zero_map_checked_pages":4,
		"initial_zero_map_checked_chunks":64,
		"dataplane_auth":{"mode":"bearer","token":"tok","session_key":"sess"},
		"control_endpoints":[
			{"address":"10.0.0.2","port":9801,"use_tls":true,"bearer_token":"tok-b"},
			{"address":"10.0.0.1","port":9701,"use_tls":true,"bearer_token":"tok-a"},
			{"address":"10.0.0.1","port":9701,"use_tls":true,"bearer_token":"tok-a"}
		],
		"dataplane_endpoints":[
			{"path_id":8,"address":"10.0.0.2","port":9800,"priority":90},
			{"path_id":7,"address":"10.0.0.1","port":9700,"priority":100}
		]
	}`

	normalized, servers, err := PrepareAttachManifest(raw)
	if err != nil {
		t.Fatalf("PrepareAttachManifest failed: %v", err)
	}
	if len(servers) != 2 {
		t.Fatalf("unexpected server count=%d", len(servers))
	}
	if servers[0].ID != 1 || servers[0].Address != "10.0.0.2" || servers[0].APIPrefix != "/api/v1" {
		t.Fatalf("unexpected first server: %+v", servers[0])
	}

	var doc AttachManifestDocument
	if err := json.Unmarshal([]byte(normalized), &doc); err != nil {
		t.Fatalf("unmarshal normalized manifest: %v", err)
	}
	if len(doc.ControlEndpoints) != 2 {
		t.Fatalf("unexpected control endpoints: %+v", doc.ControlEndpoints)
	}
	if len(doc.DataplaneEndpoints) != 2 {
		t.Fatalf("unexpected dataplane endpoints: %+v", doc.DataplaneEndpoints)
	}
	if doc.RuntimePathExpansionEligibleAtUnix != 4102444800 {
		t.Fatalf("unexpected runtime expansion eligibility: %+v", doc)
	}
	if doc.DataplaneEndpoints[0].PathID != 0 || doc.DataplaneEndpoints[0].Priority != 100 {
		t.Fatalf("unexpected dataplane ordering: %+v", doc.DataplaneEndpoints)
	}
	if doc.DataplaneEndpoints[1].PathID != 1 || doc.DataplaneEndpoints[1].Priority != 90 {
		t.Fatalf("unexpected dataplane ordering: %+v", doc.DataplaneEndpoints)
	}
	if doc.Generation != 3 || doc.AttachmentGeneration != 3 || doc.SizeBytes != 8388608 || doc.BlockSize != 4096 {
		t.Fatalf("unexpected core fields after normalization: %+v", doc)
	}
	if doc.AttachmentID != "att-00000065-0003" || doc.AttachedHostID != "host-a" || doc.AttachedDeviceID != 0 {
		t.Fatalf("unexpected attach identity fields after normalization: %+v", doc)
	}
	if doc.WriterFencingEpoch != 9 || !doc.HandoffRequired || doc.HandoffReason != "current_gateway_not_desired" || len(doc.HandoffTargetGatewaySet) != 1 || doc.HandoffTargetGatewaySet[0] != "gw-b" {
		t.Fatalf("unexpected handoff/fencing fields after normalization: %+v", doc)
	}
	if doc.ControllerPriorityClass != "handoff" || len(doc.ControllerRecommendedActions) != 2 || doc.ControllerRecommendedActions[0] != "complete_gateway_handoff" {
		t.Fatalf("unexpected controller fields after normalization: %+v", doc)
	}
	if len(doc.ClusterPriorityMismatchActions) != 1 || doc.ClusterPriorityMismatchActions[0] != "complete_gateway_handoff_aggressively" {
		t.Fatalf("unexpected cluster mismatch fields after normalization: %+v", doc)
	}
	if doc.MaxInflightReqs != 64 || doc.MaxInflightBytes != 1048576 || doc.MaxIOSize != 131072 || doc.MaxZeroLikeIOSize != 268435456 {
		t.Fatalf("unexpected limits after normalization: %+v", doc)
	}
	if !doc.InitialZeroMapTrusted || !doc.InitialZeroMapAllZero || doc.InitialZeroMapGranuleBytes != 65536 || doc.InitialZeroMapCheckedPages != 4 || doc.InitialZeroMapCheckedChunks != 64 {
		t.Fatalf("unexpected zero-map evidence after normalization: %+v", doc)
	}
	if doc.DataplaneAuth["token"] != "tok" || doc.DataplaneAuth["session_key"] != "sess" {
		t.Fatalf("unexpected dataplane auth after normalization: %+v", doc.DataplaneAuth)
	}
}

func TestPrepareAttachManifestRejectsInvalidDocument(t *testing.T) {
	if _, _, err := PrepareAttachManifest(`{"volume_id":"00000065","control_endpoints":[],"dataplane_endpoints":[]}`); err == nil {
		t.Fatalf("expected validation error")
	}
	if _, _, err := PrepareAttachManifest(`{"volume_id":"00000065","control_endpoints":[{"address":"10.0.0.1","port":9701}],"dataplane_endpoints":[{"path_id":1,"address":"","port":9700}]}`); err == nil {
		t.Fatalf("expected dataplane validation error")
	}
}

func TestBuildPathSelectionPlan(t *testing.T) {
	raw := `{
		"volume_id":"00000065",
		"control_endpoints":[{"address":"10.0.0.1","port":9701}],
		"dataplane_endpoints":[
			{"path_id":9,"address":"10.0.0.3","port":9900,"priority":80},
			{"path_id":7,"address":"10.0.0.1","port":9700,"priority":100},
			{"path_id":8,"address":"10.0.0.2","port":9800,"priority":90}
		]
	}`

	plan, err := BuildPathSelectionPlan(raw, map[uint32]PathHealthState{
		0: PathHealthHealthy,
		1: PathHealthDown,
		2: PathHealthSuspect,
	}, 2)
	if err != nil {
		t.Fatalf("BuildPathSelectionPlan failed: %v", err)
	}
	if len(plan.Active) != 2 || plan.Active[0].PathID != 0 || plan.Active[1].PathID != 2 {
		t.Fatalf("unexpected active plan: %+v", plan.Active)
	}
	if len(plan.Standby) != 0 {
		t.Fatalf("unexpected standby plan: %+v", plan.Standby)
	}
	if len(plan.Suppressed) != 1 || plan.Suppressed[0].PathID != 1 {
		t.Fatalf("unexpected suppressed plan: %+v", plan.Suppressed)
	}
	if ids := AllowedPathIDs(plan); len(ids) != 2 || ids[0] != 0 || ids[1] != 2 {
		t.Fatalf("unexpected allowed path ids: %+v", ids)
	}
}

func TestBuildPathSelectionPlanUsesHealthyBeforeSuspect(t *testing.T) {
	raw := `{
		"volume_id":"00000065",
		"control_endpoints":[{"address":"10.0.0.1","port":9701}],
		"dataplane_endpoints":[
			{"path_id":0,"address":"10.0.0.1","port":9700,"priority":100},
			{"path_id":1,"address":"10.0.0.2","port":9800,"priority":90},
			{"path_id":2,"address":"10.0.0.3","port":9900,"priority":80}
		]
	}`

	plan, err := BuildPathSelectionPlan(raw, map[uint32]PathHealthState{
		1: PathHealthSuspect,
	}, 1)
	if err != nil {
		t.Fatalf("BuildPathSelectionPlan failed: %v", err)
	}
	if len(plan.Active) != 1 || plan.Active[0].PathID != 0 {
		t.Fatalf("unexpected active plan: %+v", plan.Active)
	}
	if len(plan.Standby) != 2 || plan.Standby[0].PathID != 2 || plan.Standby[1].PathID != 1 {
		t.Fatalf("unexpected standby plan: %+v", plan.Standby)
	}
}

func TestBuildPathSelectionPlanPrefersGatewayDiversity(t *testing.T) {
	raw := `{
		"volume_id":"00000065",
		"control_endpoints":[{"address":"10.0.0.1","port":9701}],
		"dataplane_endpoints":[
			{"path_id":0,"gateway_id":"gw-a","address":"10.0.0.1","port":9700,"server_name":"gw-a","priority":100},
			{"path_id":1,"gateway_id":"gw-a","address":"10.0.0.11","port":9700,"server_name":"gw-a","priority":99},
			{"path_id":2,"gateway_id":"gw-b","address":"10.0.0.2","port":9800,"server_name":"gw-b","priority":90}
		]
	}`

	plan, err := BuildPathSelectionPlan(raw, nil, 2)
	if err != nil {
		t.Fatalf("BuildPathSelectionPlan failed: %v", err)
	}
	if len(plan.Active) != 2 {
		t.Fatalf("unexpected active plan count: %+v", plan.Active)
	}
	if plan.Active[0].GatewayID != "gw-a" || plan.Active[1].GatewayID != "gw-b" {
		t.Fatalf("expected gateway-diverse active plan, got: %+v", plan.Active)
	}
	if len(plan.Standby) != 1 || plan.Standby[0].GatewayID != "gw-a" {
		t.Fatalf("unexpected standby plan: %+v", plan.Standby)
	}
}
