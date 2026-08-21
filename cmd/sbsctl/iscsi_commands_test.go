package main

import (
	"context"
	"encoding/json"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/nosway/namrbd/iscsi"
	adminv1 "github.com/nosway/namrbd/sbs/admin/v1"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/proto"
)

func TestSBSCTLISCSIStatusGatewayUsesClusterAuthority(t *testing.T) {
	fake := &fakeSBSCTLISCSIAdminServer{
		status: &adminv1.GetClusterStatusResponse{
			Cluster:         &adminv1.ClusterRef{ClusterId: "cluster-a", SbsClusterId: "sbs-a"},
			LeaderNodeId:    "node-a",
			QuorumHealth:    adminv1.QuorumHealth_QUORUM_HEALTH_HEALTHY,
			ActiveNodes:     3,
			DrainingNodes:   1,
			RepairBacklog:   7,
			DrainBacklog:    2,
			DegradedExtents: 4,
		},
		registry: &adminv1.GetISCSIRegistryResponse{
			Cluster:          &adminv1.ClusterRef{ClusterId: "cluster-a", SbsClusterId: "sbs-a"},
			RegistryRevision: 12,
			ConfigGeneration: 3,
			Targets: []*adminv1.ISCSITargetSummary{
				{TargetIqn: "iqn.2026-06.io.namrbd:cluster", PortalId: "portal-a", PortalIds: []string{"portal-a"}, ExportId: "export-a", Enabled: true},
			},
			Luns: []*adminv1.ISCSILUNSummary{
				{TargetIqn: "iqn.2026-06.io.namrbd:cluster", LunId: 0, LunWwn: "namrbd-phase-q-export-a", ExportId: "export-a", VolumeId: "vol-a", ExportMode: "read_write", LogicalBlockSizeBytes: 4096, Enabled: true},
			},
			ObservabilityCounters: &adminv1.ISCSIObservabilityCounters{
				SessionCount:      2,
				ConnectedSessions: 1,
				BackendErrors:     5,
				FlushCount:        8,
			},
		},
	}
	installBufconnSBSCTLISCSIAdminClient(t, fake)

	output := captureSBSCTLStdout(t, func() {
		runISCSIStatusGateway([]string{
			"--admin-endpoint", "bufnet",
			"--cluster-id", "cluster-a",
			"--sbs-cluster-id", "sbs-a",
			"--json",
		})
	})
	out := decodeSBSCTLISCSIJSON(t, output)
	if out["entrypoint"] != "sbsctl iscsi status gateway" || out["control_plane_mode"] != "sbs_cluster" {
		t.Fatalf("unexpected iscsi gateway envelope: %#v", out)
	}
	if out["metadata_authority"] != "cluster_iscsi_control_plane" || out["storage_authority"] != "sbs_service" {
		t.Fatalf("unexpected authority fields: %#v", out)
	}
	if out["active_nodes"] != float64(3) || out["quorum_health"] != "healthy" || out["registry_status"] != "cluster_iscsi_registry_ready" {
		t.Fatalf("unexpected status projection: %#v", out)
	}
	if out["iscsi_registry_available"] != true || out["registry_revision"] != float64(12) || out["config_generation"] != float64(3) {
		t.Fatalf("unexpected registry metadata: %#v", out)
	}
	if out["iscsi_serving_registry_authority"] != "sbs_service_tikv" || out["iscsi_registry_storage_layout"] != "split_v2" || out["iscsi_registry_empty"] != false {
		t.Fatalf("unexpected serving registry evidence: %#v", out)
	}
	if out["target_count"] != float64(1) || out["lun_count"] != float64(1) || out["session_count"] != float64(2) || out["connected_sessions"] != float64(1) {
		t.Fatalf("unexpected registry counts: %#v", out)
	}
	if fake.lastCluster.GetClusterId() != "cluster-a" || fake.lastCluster.GetSbsClusterId() != "sbs-a" {
		t.Fatalf("admin request cluster ref=%#v", fake.lastCluster)
	}
}

func TestSBSCTLISCSILUNListUsesRegistryLUNs(t *testing.T) {
	installBufconnSBSCTLISCSIAdminClient(t, &fakeSBSCTLISCSIAdminServer{
		luns: []*adminv1.ISCSILUNSummary{
			{TargetIqn: "iqn.2026-06.io.namrbd:cluster", LunId: 0, LunWwn: "namrbd-phase-q-export-a", ExportId: "export-a", VolumeId: "vol-a", ExportMode: "read_write", LogicalBlockSizeBytes: 4096, Enabled: true},
			{TargetIqn: "iqn.2026-06.io.namrbd:other", LunId: 1, LunWwn: "namrbd-phase-q-export-b", ExportId: "export-b", VolumeId: "vol-b", ExportMode: "read_only", LogicalBlockSizeBytes: 4096, Enabled: true},
		},
		registryRevision: 9,
		configGeneration: 4,
	})

	output := captureSBSCTLStdout(t, func() {
		runISCSILUNList([]string{
			"--target-iqn", "iqn.2026-06.io.namrbd:cluster",
			"--admin-endpoint", "bufnet",
			"--json",
		})
	})
	out := decodeSBSCTLISCSIJSON(t, output)
	if out["count"] != float64(1) || out["registry_revision"] != float64(9) || out["config_generation"] != float64(4) {
		t.Fatalf("unexpected LUN registry list: %#v", out)
	}
	if out["iscsi_registry_available"] != true || out["registry_status"] != "cluster_iscsi_registry_ready" || out["sbs_volume_projection"] != false {
		t.Fatalf("registry availability must be explicit: %#v", out)
	}
	luns, ok := out["luns"].([]any)
	if !ok || len(luns) != 1 {
		t.Fatalf("unexpected luns rows: %#v", out["luns"])
	}
	lun, ok := luns[0].(map[string]any)
	if !ok || lun["volume_id"] != "vol-a" || lun["export_mode"] != "read_write" {
		t.Fatalf("unexpected lun row: %#v", luns[0])
	}
}

func TestSBSCTLISCSIAdminRPCErrorClassifiesNotFound(t *testing.T) {
	installBufconnSBSCTLISCSIAdminClient(t, &fakeSBSCTLISCSIAdminServer{})
	cfg := sbsctlISCSIConfig{AdminEndpoint: "bufnet", Timeout: 10 * time.Second}
	out, code := withISCSIAdmin(cfg, "lun", "get", func(context.Context, sbsctlISCSIAdminClient, sbsctlISCSIConfig) map[string]any {
		return sbsctlISCSIAdminRPCErrorResult(cfg, "lun", "get", status.Error(codes.NotFound, "missing"))
	})
	if code != 1 {
		t.Fatalf("sbsctl iscsi lun get code=%d want 1 output=%#v", code, out)
	}
	if out["result"] != "error" || out["rejection_reason"] != "cluster_iscsi_registry_not_found" || out["entrypoint"] != "sbsctl iscsi lun get" {
		t.Fatalf("unexpected structured error: %#v", out)
	}
}

func TestSBSCTLISCSIRegistryStateDistinguishesEmptyAndUnavailable(t *testing.T) {
	if got := sbsctlISCSIRegistryStatus(true); got != "cluster_iscsi_registry_empty" {
		t.Fatalf("empty status=%q", got)
	}
	cfg := sbsctlISCSIConfig{}
	out := sbsctlISCSIAdminRPCErrorResult(cfg, "status", "gateway", status.Error(codes.Unavailable, "tikv unavailable"))
	if out["rejection_reason"] != "cluster_iscsi_registry_unavailable" {
		t.Fatalf("unavailable classification=%#v", out)
	}
}

func TestSBSCTLISCSILUNGetUsesRegistryMapping(t *testing.T) {
	installBufconnSBSCTLISCSIAdminClient(t, &fakeSBSCTLISCSIAdminServer{
		lunsByKey: map[string]*adminv1.ISCSILUNSummary{
			"iqn.2026-06.io.namrbd:cluster#0": {TargetIqn: "iqn.2026-06.io.namrbd:cluster", LunId: 0, LunWwn: "namrbd-phase-q-export-a", ExportId: "export-a", VolumeId: "vol-a", ExportMode: "read_write", LogicalBlockSizeBytes: 4096, Enabled: true},
		},
		registryRevision: 11,
		configGeneration: 5,
	})

	output := captureSBSCTLStdout(t, func() {
		runISCSILUNGet([]string{
			"--target-iqn", "iqn.2026-06.io.namrbd:cluster",
			"--lun-id", "0",
			"--volume-id", "vol-a",
			"--admin-endpoint", "bufnet",
			"--json",
		})
	})
	out := decodeSBSCTLISCSIJSON(t, output)
	lun, ok := out["lun"].(map[string]any)
	if !ok || lun["volume_id"] != "vol-a" || lun["lun_wwn"] != "namrbd-phase-q-export-a" {
		t.Fatalf("unexpected registry lun: %#v", out)
	}
	if out["lun_found"] != true || out["sbs_volume_projection"] != false || out["registry_revision"] != float64(11) {
		t.Fatalf("lun registry claim should be authoritative: %#v", out)
	}
}

func TestSBSCTLISCSIPortalCreateSendsMutationEnvelope(t *testing.T) {
	fake := &fakeSBSCTLISCSIAdminServer{}
	installBufconnSBSCTLISCSIAdminClient(t, fake)

	output := captureSBSCTLStdout(t, func() {
		runISCSIPortalCreate([]string{
			"--admin-endpoint", "bufnet",
			"--cluster-id", "cluster-a",
			"--sbs-cluster-id", "sbs-a",
			"--portal-id", "portal-a",
			"--address", "10.0.0.11:3260",
			"--gateway-id", "gw-a",
			"--actor", "admin-a",
			"--reason", "unit-test",
			"--idempotency-key", "idem-portal",
			"--expected-registry-revision", "7",
			"--json",
		})
	})
	out := decodeSBSCTLISCSIJSON(t, output)
	if out["entrypoint"] != "sbsctl iscsi portal create" || out["registry_revision"] != float64(8) {
		t.Fatalf("unexpected portal create output: %#v", out)
	}
	op, ok := out["operation_handle"].(map[string]any)
	if !ok || op["accepted"] != true || op["operation_id"] != "op-portal-create" {
		t.Fatalf("unexpected operation handle: %#v", out["operation_handle"])
	}
	if fake.createPortalReq.GetMeta().GetActor() != "admin-a" || fake.createPortalReq.GetIdempotencyKey() != "idem-portal" || fake.createPortalReq.GetExpectedRegistryRevision() != 7 {
		t.Fatalf("unexpected create portal request: %#v", fake.createPortalReq)
	}
}

func TestSBSCTLISCSILUNExportSendsRegistryMutation(t *testing.T) {
	fake := &fakeSBSCTLISCSIAdminServer{}
	installBufconnSBSCTLISCSIAdminClient(t, fake)

	output := captureSBSCTLStdout(t, func() {
		runISCSILUNExport([]string{
			"--admin-endpoint", "bufnet",
			"--target-iqn", "iqn.2026-06.io.namrbd:cluster",
			"--lun-id", "0",
			"--volume-id", "00a1b2c3",
			"--export-id", "export-a",
			"--export-mode", "read_write",
			"--actor", "admin-a",
			"--idempotency-key", "idem-lun-export",
			"--json",
		})
	})
	out := decodeSBSCTLISCSIJSON(t, output)
	lun, ok := out["lun"].(map[string]any)
	if !ok || lun["volume_id"] != "00a1b2c3" || lun["export_id"] != "export-a" {
		t.Fatalf("unexpected lun export output: %#v", out)
	}
	if fake.exportLUNReq.GetTargetIqn() != "iqn.2026-06.io.namrbd:cluster" || fake.exportLUNReq.GetLunId() != 0 || fake.exportLUNReq.GetVolumeId() != "00a1b2c3" {
		t.Fatalf("unexpected export lun request: %#v", fake.exportLUNReq)
	}
}

func TestSBSCTLISCSIInitiatorAllowParsesLUNIDs(t *testing.T) {
	fake := &fakeSBSCTLISCSIAdminServer{}
	installBufconnSBSCTLISCSIAdminClient(t, fake)

	output := captureSBSCTLStdout(t, func() {
		runISCSIInitiatorAllow([]string{
			"--admin-endpoint", "bufnet",
			"--initiator-iqn", "iqn.1994-05.com.redhat:node-a",
			"--target-iqn", "iqn.2026-06.io.namrbd:cluster",
			"--lun-id", "0",
			"--lun-ids", "1,2",
			"--auth-mode", "chap",
			"--chap-secret-ref", "vault:iscsi/node-a",
			"--actor", "admin-a",
			"--idempotency-key", "idem-allow",
			"--json",
		})
	})
	out := decodeSBSCTLISCSIJSON(t, output)
	acl, ok := out["initiator_acl"].(map[string]any)
	if !ok || acl["auth_mode"] != "chap" || acl["chap_secret_set"] != true {
		t.Fatalf("unexpected allow output: %#v", out)
	}
	if got := fake.allowInitiatorReq.GetAllowedLunIds(); len(got) != 3 || got[0] != 0 || got[1] != 1 || got[2] != 2 {
		t.Fatalf("allowed_lun_ids=%v want [0 1 2]", got)
	}
}

func TestSBSCTLISCSISessionListShowsCounters(t *testing.T) {
	installBufconnSBSCTLISCSIAdminClient(t, &fakeSBSCTLISCSIAdminServer{
		registryRevision: 6,
		configGeneration: 2,
		sessions: []*adminv1.ISCSISessionSummary{
			{SessionId: "sess-a", TargetIqn: "iqn.2026-06.io.namrbd:cluster", InitiatorIqn: "iqn.1994-05.com.redhat:node-a", LunId: 0, Connected: true, State: "connected"},
		},
	})

	output := captureSBSCTLStdout(t, func() {
		runISCSISessionList([]string{
			"--admin-endpoint", "bufnet",
			"--target-iqn", "iqn.2026-06.io.namrbd:cluster",
			"--connected-only",
			"--json",
		})
	})
	out := decodeSBSCTLISCSIJSON(t, output)
	if out["entrypoint"] != "sbsctl iscsi session list" || out["count"] != float64(1) {
		t.Fatalf("unexpected session list output: %#v", out)
	}
	counters, ok := out["observability_counters"].(map[string]any)
	if !ok || counters["session_count"] != float64(1) || counters["connected_sessions"] != float64(1) {
		t.Fatalf("unexpected session counters: %#v", out["observability_counters"])
	}
}

func TestSBSCTLISCSISessionDisconnectSendsMutationEnvelope(t *testing.T) {
	fake := &fakeSBSCTLISCSIAdminServer{}
	installBufconnSBSCTLISCSIAdminClient(t, fake)

	output := captureSBSCTLStdout(t, func() {
		runISCSISessionDisconnect([]string{
			"--admin-endpoint", "bufnet",
			"--session-id", "sess-a",
			"--actor", "admin-a",
			"--idempotency-key", "idem-disconnect",
			"--expected-registry-revision", "6",
			"--yes",
			"--json",
		})
	})
	out := decodeSBSCTLISCSIJSON(t, output)
	if out["disconnect_requested"] != true || out["registry_revision"] != float64(7) {
		t.Fatalf("unexpected disconnect output: %#v", out)
	}
	if fake.disconnectSessionReq.GetSessionId() != "sess-a" || fake.disconnectSessionReq.GetIdempotencyKey() != "idem-disconnect" || fake.disconnectSessionReq.GetExpectedRegistryRevision() != 6 {
		t.Fatalf("unexpected disconnect request: %#v", fake.disconnectSessionReq)
	}
}

func TestSBSCTLISCSIFailoverStatusShowsRuntime(t *testing.T) {
	installBufconnSBSCTLISCSIAdminClient(t, &fakeSBSCTLISCSIAdminServer{
		registryRevision: 12,
		configGeneration: 5,
		failover: &adminv1.ISCSIFailoverRuntimeSummary{
			ExportId:               "export-a",
			ActiveIscsiGatewayId:   "gw-a",
			StandbyIscsiGatewayIds: []string{"gw-b"},
			ExportEpoch:            7,
			State:                  "active",
			WriterPolicy:           "single_active_writer_session",
			HaFailoverMode:         "manual_promote_demote",
			AluaMode:               iscsi.ALUAModeImplicit,
			AluaImplicitSupported:  true,
			AluaExplicitSupported:  false,
			ActiveAluaAccessState:  iscsi.ALUAAccessStateActiveOptimized,
			StandbyAluaAccessState: iscsi.ALUAAccessStateStandby,
		},
	})

	output := captureSBSCTLStdout(t, func() {
		runISCSIFailoverStatus([]string{
			"--admin-endpoint", "bufnet",
			"--export-id", "export-a",
			"--json",
		})
	})
	out := decodeSBSCTLISCSIJSON(t, output)
	failover, ok := out["failover"].(map[string]any)
	if !ok || failover["active_iscsi_gateway_id"] != "gw-a" || failover["export_epoch"] != float64(7) {
		t.Fatalf("unexpected failover status output: %#v", out)
	}
	standby, ok := failover["standby_iscsi_gateway_ids"].([]any)
	if !ok || len(standby) != 1 || standby[0] != "gw-b" {
		t.Fatalf("unexpected failover standby gateways: %#v", failover["standby_iscsi_gateway_ids"])
	}
	if failover["alua_mode"] != iscsi.ALUAModeImplicit ||
		failover["alua_implicit_supported"] != true ||
		failover["alua_explicit_supported"] != false ||
		failover["active_alua_access_state"] != iscsi.ALUAAccessStateActiveOptimized ||
		failover["standby_alua_access_state"] != iscsi.ALUAAccessStateStandby {
		t.Fatalf("failover status missing ALUA fields: %#v", failover)
	}
}

func TestSBSCTLISCSIFailoverPromoteSendsMutationEnvelope(t *testing.T) {
	fake := &fakeSBSCTLISCSIAdminServer{}
	installBufconnSBSCTLISCSIAdminClient(t, fake)

	output := captureSBSCTLStdout(t, func() {
		runISCSIFailoverPromote([]string{
			"--admin-endpoint", "bufnet",
			"--export-id", "export-a",
			"--gateway-id", "gw-b",
			"--export-lease-id", "lease-b",
			"--trigger", "manual_test",
			"--actor", "admin-a",
			"--reason", "unit-test",
			"--idempotency-key", "idem-promote",
			"--expected-registry-revision", "7",
			"--yes",
			"--json",
		})
	})
	out := decodeSBSCTLISCSIJSON(t, output)
	failover, ok := out["failover"].(map[string]any)
	if !ok || failover["active_iscsi_gateway_id"] != "gw-b" || failover["export_lease_id"] != "lease-b" || failover["export_epoch"] != float64(8) {
		t.Fatalf("unexpected failover promote output: %#v", out)
	}
	op, ok := out["operation_handle"].(map[string]any)
	if !ok || op["accepted"] != true || op["operation_id"] != "op-failover-promote" {
		t.Fatalf("unexpected promote operation handle: %#v", out["operation_handle"])
	}
	if fake.promoteFailoverReq.GetExportId() != "export-a" || fake.promoteFailoverReq.GetGatewayId() != "gw-b" || fake.promoteFailoverReq.GetExportLeaseId() != "lease-b" || fake.promoteFailoverReq.GetExpectedRegistryRevision() != 7 || fake.promoteFailoverReq.GetIdempotencyKey() != "idem-promote" {
		t.Fatalf("unexpected promote failover request: %#v", fake.promoteFailoverReq)
	}
}

func TestSBSCTLISCSIFailoverDemoteStandbyAndRevokeSendMutationEnvelope(t *testing.T) {
	fake := &fakeSBSCTLISCSIAdminServer{}
	installBufconnSBSCTLISCSIAdminClient(t, fake)

	demoteOutput := captureSBSCTLStdout(t, func() {
		runISCSIFailoverDemote([]string{
			"--admin-endpoint", "bufnet",
			"--export-id", "export-a",
			"--gateway-id", "gw-b",
			"--actor", "admin-a",
			"--idempotency-key", "idem-demote",
			"--expected-registry-revision", "8",
			"--yes",
			"--json",
		})
	})
	demoteOut := decodeSBSCTLISCSIJSON(t, demoteOutput)
	if demoteOut["operation"] != "demote" || demoteOut["active_iscsi_gateway_id"] != "" || demoteOut["export_epoch"] != float64(9) {
		t.Fatalf("unexpected demote output: %#v", demoteOut)
	}
	if fake.demoteFailoverReq.GetGatewayId() != "gw-b" || fake.demoteFailoverReq.GetIdempotencyKey() != "idem-demote" {
		t.Fatalf("unexpected demote request: %#v", fake.demoteFailoverReq)
	}

	standbyOutput := captureSBSCTLStdout(t, func() {
		runISCSIFailoverStandby([]string{
			"--admin-endpoint", "bufnet",
			"--export-id", "export-a",
			"--gateway-id", "gw-c",
			"--actor", "admin-a",
			"--idempotency-key", "idem-standby",
			"--expected-registry-revision", "9",
			"--json",
		})
	})
	standbyOut := decodeSBSCTLISCSIJSON(t, standbyOutput)
	if standbyOut["operation"] != "standby" || standbyOut["export_epoch"] != float64(9) {
		t.Fatalf("unexpected standby output: %#v", standbyOut)
	}
	if fake.standbyFailoverReq.GetGatewayId() != "gw-c" || fake.standbyFailoverReq.GetIdempotencyKey() != "idem-standby" {
		t.Fatalf("unexpected standby request: %#v", fake.standbyFailoverReq)
	}

	revokeOutput := captureSBSCTLStdout(t, func() {
		runISCSIFailoverRevokeStale([]string{
			"--admin-endpoint", "bufnet",
			"--export-id", "export-a",
			"--gateway-id", "gw-b",
			"--actor", "admin-a",
			"--idempotency-key", "idem-revoke",
			"--expected-registry-revision", "10",
			"--yes",
			"--json",
		})
	})
	revokeOut := decodeSBSCTLISCSIJSON(t, revokeOutput)
	revokeFailover, ok := revokeOut["failover"].(map[string]any)
	if !ok || revokeFailover["stale_gateway_revoked_id"] != "gw-b" || revokeFailover["stale_gateway_rejected"] != true || revokeOut["export_epoch"] != float64(11) {
		t.Fatalf("unexpected revoke output: %#v", revokeOut)
	}
	if fake.revokeStaleFailoverReq.GetGatewayId() != "gw-b" || fake.revokeStaleFailoverReq.GetIdempotencyKey() != "idem-revoke" {
		t.Fatalf("unexpected revoke request: %#v", fake.revokeStaleFailoverReq)
	}
}

type fakeSBSCTLISCSIAdminServer struct {
	adminv1.UnimplementedAdminServiceServer
	status                 *adminv1.GetClusterStatusResponse
	registry               *adminv1.GetISCSIRegistryResponse
	registryRevision       uint64
	configGeneration       uint64
	luns                   []*adminv1.ISCSILUNSummary
	lunsByKey              map[string]*adminv1.ISCSILUNSummary
	sessions               []*adminv1.ISCSISessionSummary
	volumes                []*adminv1.VolumeSummary
	volumesByID            map[string]*adminv1.VolumeSummary
	failover               *adminv1.ISCSIFailoverRuntimeSummary
	createPortalReq        *adminv1.CreateISCSIPortalRequest
	exportLUNReq           *adminv1.ExportISCSILUNRequest
	allowInitiatorReq      *adminv1.AllowISCSIInitiatorRequest
	disconnectSessionReq   *adminv1.DisconnectISCSISessionRequest
	promoteFailoverReq     *adminv1.PromoteISCSIFailoverRequest
	demoteFailoverReq      *adminv1.DemoteISCSIFailoverRequest
	standbyFailoverReq     *adminv1.StandbyISCSIFailoverRequest
	revokeStaleFailoverReq *adminv1.RevokeStaleISCSIFailoverRequest
	lastCluster            *adminv1.ClusterRef
}

func (f *fakeSBSCTLISCSIAdminServer) GetClusterStatus(_ context.Context, req *adminv1.GetClusterStatusRequest) (*adminv1.GetClusterStatusResponse, error) {
	f.lastCluster = req.GetCluster()
	if f.status != nil {
		return f.status, nil
	}
	return &adminv1.GetClusterStatusResponse{Cluster: req.GetCluster(), QuorumHealth: adminv1.QuorumHealth_QUORUM_HEALTH_HEALTHY}, nil
}

func (f *fakeSBSCTLISCSIAdminServer) GetISCSIRegistry(_ context.Context, req *adminv1.GetISCSIRegistryRequest) (*adminv1.GetISCSIRegistryResponse, error) {
	f.lastCluster = req.GetCluster()
	if f.registry != nil {
		resp := proto.Clone(f.registry).(*adminv1.GetISCSIRegistryResponse)
		if resp.Cluster == nil {
			resp.Cluster = req.GetCluster()
		}
		if req.GetSummaryOnly() {
			resp.PortalCount = uint64(len(resp.GetPortals()))
			resp.TargetCount = uint64(len(resp.GetTargets()))
			resp.LunCount = uint64(len(resp.GetLuns()))
			resp.ExportCount = uint64(len(resp.GetLuns()))
			resp.InitiatorAclCount = uint64(len(resp.GetInitiatorAcls()))
			resp.SessionCount = uint64(len(resp.GetSessions()))
			resp.FailoverCount = uint64(len(resp.GetFailovers()))
			resp.RegistryEmpty = resp.PortalCount+resp.TargetCount+resp.LunCount+
				resp.InitiatorAclCount+resp.SessionCount+resp.FailoverCount == 0
			resp.ServingRegistryAuthority = "sbs_service_tikv"
			resp.StorageLayout = "split_v2"
			resp.Portals = nil
			resp.Targets = nil
			resp.Luns = nil
			resp.InitiatorAcls = nil
			resp.Sessions = nil
			resp.Failovers = nil
		}
		return resp, nil
	}
	return &adminv1.GetISCSIRegistryResponse{
		Cluster:               req.GetCluster(),
		RegistryRevision:      f.registryRevision,
		ConfigGeneration:      f.configGeneration,
		Luns:                  filterFakeISCSILUNs(f.luns, ""),
		ObservabilityCounters: &adminv1.ISCSIObservabilityCounters{},
	}, nil
}

func (f *fakeSBSCTLISCSIAdminServer) ListISCSILUNs(_ context.Context, req *adminv1.ListISCSILUNsRequest) (*adminv1.ListISCSILUNsResponse, error) {
	f.lastCluster = req.GetCluster()
	return &adminv1.ListISCSILUNsResponse{
		Cluster:          req.GetCluster(),
		RegistryRevision: f.registryRevision,
		ConfigGeneration: f.configGeneration,
		Luns:             filterFakeISCSILUNs(f.luns, req.GetTargetIqn()),
	}, nil
}

func (f *fakeSBSCTLISCSIAdminServer) GetISCSILUN(_ context.Context, req *adminv1.GetISCSILUNRequest) (*adminv1.GetISCSILUNResponse, error) {
	f.lastCluster = req.GetCluster()
	key := req.GetTargetIqn() + "#" + strconv.FormatUint(req.GetLunId(), 10)
	if lun := f.lunsByKey[key]; lun != nil {
		return &adminv1.GetISCSILUNResponse{
			Cluster:          req.GetCluster(),
			RegistryRevision: f.registryRevision,
			ConfigGeneration: f.configGeneration,
			Lun:              lun,
		}, nil
	}
	for _, lun := range f.luns {
		if lun.GetTargetIqn() == req.GetTargetIqn() && lun.GetLunId() == req.GetLunId() {
			return &adminv1.GetISCSILUNResponse{
				Cluster:          req.GetCluster(),
				RegistryRevision: f.registryRevision,
				ConfigGeneration: f.configGeneration,
				Lun:              lun,
			}, nil
		}
	}
	return nil, status.Errorf(codes.NotFound, "iscsi lun %s not found", key)
}

func (f *fakeSBSCTLISCSIAdminServer) GetISCSIFailover(_ context.Context, req *adminv1.GetISCSIFailoverRequest) (*adminv1.GetISCSIFailoverResponse, error) {
	f.lastCluster = req.GetCluster()
	if f.failover == nil || f.failover.GetExportId() != req.GetExportId() {
		return nil, status.Errorf(codes.NotFound, "iscsi failover %q not found", req.GetExportId())
	}
	return &adminv1.GetISCSIFailoverResponse{
		Cluster:          req.GetCluster(),
		RegistryRevision: f.registryRevision,
		ConfigGeneration: f.configGeneration,
		Failover:         f.failover,
	}, nil
}

func (f *fakeSBSCTLISCSIAdminServer) CreateISCSIPortal(_ context.Context, req *adminv1.CreateISCSIPortalRequest) (*adminv1.CreateISCSIPortalResponse, error) {
	f.lastCluster = req.GetCluster()
	f.createPortalReq = req
	return &adminv1.CreateISCSIPortalResponse{
		Cluster:          req.GetCluster(),
		Operation:        &adminv1.OperationHandle{Accepted: true, OperationId: "op-portal-create", Message: "iscsi portal created"},
		RegistryRevision: req.GetExpectedRegistryRevision() + 1,
		ConfigGeneration: 3,
		Portal: &adminv1.ISCSIPortalSummary{
			PortalId:  req.GetPortalId(),
			Address:   req.GetAddress(),
			GatewayId: req.GetGatewayId(),
			Enabled:   req.GetEnabled(),
		},
	}, nil
}

func (f *fakeSBSCTLISCSIAdminServer) ExportISCSILUN(_ context.Context, req *adminv1.ExportISCSILUNRequest) (*adminv1.ExportISCSILUNResponse, error) {
	f.lastCluster = req.GetCluster()
	f.exportLUNReq = req
	return &adminv1.ExportISCSILUNResponse{
		Cluster:          req.GetCluster(),
		Operation:        &adminv1.OperationHandle{Accepted: true, OperationId: "op-lun-export", Message: "iscsi lun exported"},
		RegistryRevision: 1,
		ConfigGeneration: 1,
		Lun: &adminv1.ISCSILUNSummary{
			TargetIqn:             req.GetTargetIqn(),
			LunId:                 req.GetLunId(),
			LunWwn:                "namrbd-phase-q-" + req.GetExportId(),
			ExportId:              req.GetExportId(),
			VolumeId:              req.GetVolumeId(),
			ExportMode:            req.GetExportMode(),
			LogicalBlockSizeBytes: req.GetLogicalBlockSizeBytes(),
			Enabled:               req.GetEnabled(),
		},
	}, nil
}

func (f *fakeSBSCTLISCSIAdminServer) AllowISCSIInitiator(_ context.Context, req *adminv1.AllowISCSIInitiatorRequest) (*adminv1.AllowISCSIInitiatorResponse, error) {
	f.lastCluster = req.GetCluster()
	f.allowInitiatorReq = req
	return &adminv1.AllowISCSIInitiatorResponse{
		Cluster:          req.GetCluster(),
		Operation:        &adminv1.OperationHandle{Accepted: true, OperationId: "op-initiator-allow", Message: "iscsi initiator allowed"},
		RegistryRevision: 1,
		ConfigGeneration: 1,
		InitiatorAcl: &adminv1.ISCSIInitiatorACLSummary{
			InitiatorIqn:  req.GetInitiatorIqn(),
			TargetIqn:     req.GetTargetIqn(),
			AllowedLunIds: req.GetAllowedLunIds(),
			AuthMode:      req.GetAuthMode(),
			ChapSecretSet: req.GetChapSecretRef() != "",
			ChapSecretRef: req.GetChapSecretRef(),
			Enabled:       req.GetEnabled(),
		},
	}, nil
}

func (f *fakeSBSCTLISCSIAdminServer) ListISCSISessions(_ context.Context, req *adminv1.ListISCSISessionsRequest) (*adminv1.ListISCSISessionsResponse, error) {
	f.lastCluster = req.GetCluster()
	sessions := filterFakeISCSISessions(f.sessions, req.GetTargetIqn(), req.GetInitiatorIqn(), req.GetConnectedOnly())
	return &adminv1.ListISCSISessionsResponse{
		Cluster:          req.GetCluster(),
		RegistryRevision: f.registryRevision,
		ConfigGeneration: f.configGeneration,
		Sessions:         sessions,
		ObservabilityCounters: &adminv1.ISCSIObservabilityCounters{
			SessionCount:      uint32(len(f.sessions)),
			ConnectedSessions: uint32(countFakeConnectedISCSISessions(f.sessions)),
		},
	}, nil
}

func (f *fakeSBSCTLISCSIAdminServer) DisconnectISCSISession(_ context.Context, req *adminv1.DisconnectISCSISessionRequest) (*adminv1.DisconnectISCSISessionResponse, error) {
	f.lastCluster = req.GetCluster()
	f.disconnectSessionReq = req
	return &adminv1.DisconnectISCSISessionResponse{
		Cluster:             req.GetCluster(),
		Operation:           &adminv1.OperationHandle{Accepted: true, OperationId: "op-session-disconnect", Message: "iscsi session disconnect requested"},
		RegistryRevision:    req.GetExpectedRegistryRevision() + 1,
		ConfigGeneration:    3,
		DisconnectRequested: true,
		Session: &adminv1.ISCSISessionSummary{
			SessionId:  req.GetSessionId(),
			State:      "disconnect_requested",
			Connected:  false,
			ScsiStatus: "good",
		},
	}, nil
}

func (f *fakeSBSCTLISCSIAdminServer) PromoteISCSIFailover(_ context.Context, req *adminv1.PromoteISCSIFailoverRequest) (*adminv1.PromoteISCSIFailoverResponse, error) {
	f.lastCluster = req.GetCluster()
	f.promoteFailoverReq = req
	return &adminv1.PromoteISCSIFailoverResponse{
		Cluster:          req.GetCluster(),
		Operation:        &adminv1.OperationHandle{Accepted: true, OperationId: "op-failover-promote", Message: "iscsi failover gateway promoted"},
		RegistryRevision: req.GetExpectedRegistryRevision() + 1,
		ConfigGeneration: 3,
		Failover: &adminv1.ISCSIFailoverRuntimeSummary{
			ExportId:                     req.GetExportId(),
			ActiveIscsiGatewayId:         req.GetGatewayId(),
			PreviousActiveIscsiGatewayId: "gw-a",
			ExportLeaseId:                req.GetExportLeaseId(),
			ExportEpoch:                  req.GetExpectedRegistryRevision() + 1,
			State:                        "active",
			WriterPolicy:                 "single_active_writer_session",
			HaFailoverMode:               "manual_promote_demote",
			FailoverTrigger:              req.GetTrigger(),
			FailoverCompleted:            true,
		},
	}, nil
}

func (f *fakeSBSCTLISCSIAdminServer) DemoteISCSIFailover(_ context.Context, req *adminv1.DemoteISCSIFailoverRequest) (*adminv1.DemoteISCSIFailoverResponse, error) {
	f.lastCluster = req.GetCluster()
	f.demoteFailoverReq = req
	return &adminv1.DemoteISCSIFailoverResponse{
		Cluster:          req.GetCluster(),
		Operation:        &adminv1.OperationHandle{Accepted: true, OperationId: "op-failover-demote", Message: "iscsi failover gateway demoted"},
		RegistryRevision: req.GetExpectedRegistryRevision() + 1,
		ConfigGeneration: 3,
		Failover: &adminv1.ISCSIFailoverRuntimeSummary{
			ExportId:                     req.GetExportId(),
			StandbyIscsiGatewayIds:       []string{req.GetGatewayId()},
			PreviousActiveIscsiGatewayId: req.GetGatewayId(),
			ExportEpoch:                  req.GetExpectedRegistryRevision() + 1,
			State:                        "demoted",
			WriterPolicy:                 "single_active_writer_session",
			HaFailoverMode:               "manual_promote_demote",
			FailoverTrigger:              req.GetTrigger(),
			FailoverCompleted:            true,
		},
	}, nil
}

func (f *fakeSBSCTLISCSIAdminServer) StandbyISCSIFailover(_ context.Context, req *adminv1.StandbyISCSIFailoverRequest) (*adminv1.StandbyISCSIFailoverResponse, error) {
	f.lastCluster = req.GetCluster()
	f.standbyFailoverReq = req
	return &adminv1.StandbyISCSIFailoverResponse{
		Cluster:          req.GetCluster(),
		Operation:        &adminv1.OperationHandle{Accepted: true, OperationId: "op-failover-standby", Message: "iscsi failover standby gateway registered"},
		RegistryRevision: req.GetExpectedRegistryRevision() + 1,
		ConfigGeneration: 3,
		Failover: &adminv1.ISCSIFailoverRuntimeSummary{
			ExportId:               req.GetExportId(),
			ActiveIscsiGatewayId:   "gw-a",
			StandbyIscsiGatewayIds: []string{req.GetGatewayId()},
			ExportEpoch:            req.GetExpectedRegistryRevision(),
			State:                  "standby_registered",
			WriterPolicy:           "single_active_writer_session",
			HaFailoverMode:         "manual_promote_demote",
		},
	}, nil
}

func (f *fakeSBSCTLISCSIAdminServer) RevokeStaleISCSIFailover(_ context.Context, req *adminv1.RevokeStaleISCSIFailoverRequest) (*adminv1.RevokeStaleISCSIFailoverResponse, error) {
	f.lastCluster = req.GetCluster()
	f.revokeStaleFailoverReq = req
	return &adminv1.RevokeStaleISCSIFailoverResponse{
		Cluster:          req.GetCluster(),
		Operation:        &adminv1.OperationHandle{Accepted: true, OperationId: "op-failover-revoke-stale", Message: "iscsi failover stale gateway revoked"},
		RegistryRevision: req.GetExpectedRegistryRevision() + 1,
		ConfigGeneration: 3,
		Failover: &adminv1.ISCSIFailoverRuntimeSummary{
			ExportId:                   req.GetExportId(),
			StaleGatewayRevokedId:      req.GetGatewayId(),
			StaleGatewayRejected:       true,
			LastRejectedIscsiGatewayId: req.GetGatewayId(),
			LastWriteGatewayId:         req.GetGatewayId(),
			LastWriteRejectionReason:   "revoked_stale_gateway",
			LastWriteScsiStatus:        "check_condition",
			LastWriteSenseKey:          "data_protect",
			ExportEpoch:                req.GetExpectedRegistryRevision() + 1,
			State:                      "stale_revoked",
			WriterPolicy:               "single_active_writer_session",
			HaFailoverMode:             "manual_promote_demote",
			FailoverTrigger:            req.GetTrigger(),
			FailoverCompleted:          true,
		},
	}, nil
}

func (f *fakeSBSCTLISCSIAdminServer) ListVolumes(_ context.Context, req *adminv1.ListVolumesRequest) (*adminv1.ListVolumesResponse, error) {
	f.lastCluster = req.GetCluster()
	return &adminv1.ListVolumesResponse{Cluster: req.GetCluster(), Volumes: f.volumes}, nil
}

func (f *fakeSBSCTLISCSIAdminServer) GetVolume(_ context.Context, req *adminv1.GetVolumeRequest) (*adminv1.GetVolumeResponse, error) {
	f.lastCluster = req.GetCluster()
	if volume := f.volumesByID[req.GetVolumeId()]; volume != nil {
		return &adminv1.GetVolumeResponse{Cluster: req.GetCluster(), Volume: volume}, nil
	}
	return &adminv1.GetVolumeResponse{Cluster: req.GetCluster(), Volume: &adminv1.VolumeSummary{VolumeId: req.GetVolumeId()}}, nil
}

func filterFakeISCSILUNs(luns []*adminv1.ISCSILUNSummary, targetIQN string) []*adminv1.ISCSILUNSummary {
	targetIQN = strings.TrimSpace(targetIQN)
	out := make([]*adminv1.ISCSILUNSummary, 0, len(luns))
	for _, lun := range luns {
		if targetIQN != "" && lun.GetTargetIqn() != targetIQN {
			continue
		}
		out = append(out, lun)
	}
	return out
}

func filterFakeISCSISessions(sessions []*adminv1.ISCSISessionSummary, targetIQN, initiatorIQN string, connectedOnly bool) []*adminv1.ISCSISessionSummary {
	targetIQN = strings.TrimSpace(targetIQN)
	initiatorIQN = strings.TrimSpace(initiatorIQN)
	out := make([]*adminv1.ISCSISessionSummary, 0, len(sessions))
	for _, session := range sessions {
		if targetIQN != "" && session.GetTargetIqn() != targetIQN {
			continue
		}
		if initiatorIQN != "" && session.GetInitiatorIqn() != initiatorIQN {
			continue
		}
		if connectedOnly && !session.GetConnected() {
			continue
		}
		out = append(out, session)
	}
	return out
}

func countFakeConnectedISCSISessions(sessions []*adminv1.ISCSISessionSummary) int {
	count := 0
	for _, session := range sessions {
		if session.GetConnected() {
			count++
		}
	}
	return count
}

func installBufconnSBSCTLISCSIAdminClient(t *testing.T, server adminv1.AdminServiceServer) {
	t.Helper()
	listener := bufconn.Listen(1024 * 1024)
	grpcServer := grpc.NewServer()
	adminv1.RegisterAdminServiceServer(grpcServer, server)
	go func() {
		_ = grpcServer.Serve(listener)
	}()
	t.Cleanup(func() {
		grpcServer.Stop()
		_ = listener.Close()
	})

	oldFactory := newSBSCTLISCSIAdminClient
	newSBSCTLISCSIAdminClient = func(ctx context.Context, _ string) (sbsctlISCSIAdminClient, func() error, error) {
		conn, err := grpc.DialContext(ctx, "bufnet",
			grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
				return listener.Dial()
			}),
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		)
		if err != nil {
			return nil, nil, err
		}
		return adminv1.NewAdminServiceClient(conn), conn.Close, nil
	}
	t.Cleanup(func() {
		newSBSCTLISCSIAdminClient = oldFactory
	})
}

func captureSBSCTLStdout(t *testing.T, run func()) string {
	t.Helper()
	oldStdout := os.Stdout
	reader, writer, err := os.Pipe()
	if err != nil {
		t.Fatalf("pipe stdout: %v", err)
	}
	os.Stdout = writer
	defer func() {
		os.Stdout = oldStdout
	}()
	run()
	if err := writer.Close(); err != nil {
		t.Fatalf("close stdout writer: %v", err)
	}
	raw, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("read stdout: %v", err)
	}
	return string(raw)
}

func decodeSBSCTLISCSIJSON(t *testing.T, output string) map[string]any {
	t.Helper()
	var out map[string]any
	if err := json.Unmarshal([]byte(output), &out); err != nil {
		t.Fatalf("output is not JSON: %v\n%s", err, output)
	}
	for _, unexpected := range []string{"namrbd-iscsictl", "state_path"} {
		if strings.Contains(output, unexpected) {
			t.Fatalf("sbsctl iscsi output contains %q: %s", unexpected, output)
		}
	}
	return out
}
