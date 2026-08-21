package main

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/nosway/namrbd/iscsi"
	adminv1 "github.com/nosway/namrbd/sbs/admin/v1"

	"google.golang.org/grpc"
)

func TestGatewaySelfTestJSON(t *testing.T) {
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	code := run([]string{
		"--backend", "memory",
		"--portal", "127.0.0.1:3260",
		"--memory-lun-size", "1MiB",
		"--self-test",
		"--json",
	}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("run code=%d stderr=%s", code, stderr.String())
	}
	if stderr.Len() != 0 {
		t.Fatalf("unexpected stderr: %s", stderr.String())
	}
	var summary iscsi.Summary
	if err := json.Unmarshal(stdout.Bytes(), &summary); err != nil {
		t.Fatalf("stdout is not summary JSON: %v\n%s", err, stdout.String())
	}
	if summary.Result != "ok" || summary.Entrypoint != "namrbd-iscsi-gateway --self-test" {
		t.Fatalf("unexpected summary: %#v", summary)
	}
}

func TestObservabilityHandlerHealthAndMetrics(t *testing.T) {
	handler := newISCSIObservabilityHandler(iscsiObservabilityState{
		Backend:                  "sbs",
		ExportID:                 "export-a",
		TargetIQN:                "iqn.2026-08.io.namrbd:test",
		AuthMode:                 "none",
		RegistryLoaded:           true,
		VolumeID:                 "00000065",
		FleetRegistered:          true,
		FleetMembershipAuthority: "etcd",
		FleetHealthAuthority:     "etcd",
		LiveReloadSummary: func() iscsi.LiveReloadSummary {
			return iscsi.LiveReloadSummary{ReloadSnapshot: iscsi.ReloadSnapshot{RegistryReloadCount: 2, RegistryReloadRevision: 9}, ServedExportCount: 32, MaxExportsPerProcess: 64}
		},
	})

	healthReq := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	healthRec := httptest.NewRecorder()
	handler.ServeHTTP(healthRec, healthReq)
	if healthRec.Code != http.StatusOK || strings.TrimSpace(healthRec.Body.String()) != "ok" {
		t.Fatalf("healthz status=%d body=%q", healthRec.Code, healthRec.Body.String())
	}

	metricsReq := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	metricsRec := httptest.NewRecorder()
	handler.ServeHTTP(metricsRec, metricsReq)
	if metricsRec.Code != http.StatusOK {
		t.Fatalf("metrics status=%d body=%s", metricsRec.Code, metricsRec.Body.String())
	}
	body := metricsRec.Body.String()
	for _, want := range []string{
		"namrbd_iscsi_gateway_ready 1",
		`namrbd_iscsi_gateway_runtime_info{backend="sbs",export_id="export-a",target_iqn="iqn.2026-08.io.namrbd:test",auth_mode="none",volume_id="00000065"} 1`,
		"namrbd_iscsi_gateway_registry_loaded 1",
		`namrbd_iscsi_gateway_fleet_registered{membership_authority="etcd",health_authority="etcd"} 1`,
		"namrbd_iscsi_gateway_registry_reload_total 2",
		"namrbd_iscsi_gateway_served_exports 32",
		"namrbd_iscsi_gateway_max_exports 64",
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("metrics missing %q in\n%s", want, body)
		}
	}
	registryReq := httptest.NewRequest(http.MethodGet, "/debug/registry", nil)
	registryRec := httptest.NewRecorder()
	handler.ServeHTTP(registryRec, registryReq)
	if registryRec.Code != http.StatusOK || !strings.Contains(registryRec.Body.String(), `"served_export_count":32`) ||
		!strings.Contains(registryRec.Body.String(), `"max_exports_per_process":64`) {
		t.Fatalf("registry status=%d body=%s", registryRec.Code, registryRec.Body.String())
	}
}

func TestGatewayServeRejectsGotgtWildcardListenByDefault(t *testing.T) {
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	code := run([]string{
		"--backend", "memory",
		"--portal", "127.0.0.1:3260",
		"--memory-lun-size", "1MiB",
		"--serve",
	}, &stdout, &stderr)
	if code != 1 {
		t.Fatalf("run code=%d, want 1", code)
	}
	if stdout.Len() != 0 {
		t.Fatalf("unexpected stdout: %s", stdout.String())
	}
	if !strings.Contains(stderr.String(), "wildcard") {
		t.Fatalf("stderr=%q, want wildcard guard", stderr.String())
	}
}

func TestGatewayMemoryServeRejectsUnsupportedCHAPRuntime(t *testing.T) {
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	secretRef := "fixture-chap-sha256:0123456789abcdef"
	code := run([]string{
		"--backend", "memory",
		"--portal", "127.0.0.1:3260",
		"--memory-lun-size", "1MiB",
		"--serve",
		"--allow-gotgt-wildcard-listen",
		"--auth-mode", "chap",
		"--chap-secret-ref", secretRef,
		"--allowed-initiator-iqns", "iqn.1993-08.org.debian:01:unit",
		"--json",
	}, &stdout, &stderr)
	if code != 1 {
		t.Fatalf("run code=%d, want 1 stderr=%s stdout=%s", code, stderr.String(), stdout.String())
	}
	var summary iscsi.Summary
	if err := json.Unmarshal(stdout.Bytes(), &summary); err != nil {
		t.Fatalf("stdout is not summary JSON: %v\n%s", err, stdout.String())
	}
	if summary.Result != "error" || summary.AuthMode != "chap" || summary.AuthPolicy != iscsi.AuthPolicyCHAPRuntimeFailClosed {
		t.Fatalf("unexpected CHAP rejection summary: %#v", summary)
	}
	if summary.TargetStackAccepted {
		t.Fatalf("target stack accepted unsupported CHAP: %#v", summary)
	}
	if summary.RuntimeCHAPSupported {
		t.Fatalf("runtime_chap_supported=true, want false")
	}
	if summary.CHAPSecretRef != secretRef {
		t.Fatalf("chap_secret_ref=%q, want %q", summary.CHAPSecretRef, secretRef)
	}
	if !strings.Contains(summary.FirstError, "CHAP runtime is not supported") {
		t.Fatalf("first_error=%q, want CHAP unsupported diagnostic", summary.FirstError)
	}
	if !strings.Contains(stderr.String(), "CHAP runtime is not supported") {
		t.Fatalf("stderr=%q, want CHAP unsupported diagnostic", stderr.String())
	}
}

func TestGatewayServeRejectsCHAPWithoutSecretRef(t *testing.T) {
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	code := run([]string{
		"--backend", "memory",
		"--portal", "127.0.0.1:3260",
		"--memory-lun-size", "1MiB",
		"--serve",
		"--auth-mode", "chap",
		"--json",
	}, &stdout, &stderr)
	if code != 2 {
		t.Fatalf("run code=%d, want 2 stderr=%s stdout=%s", code, stderr.String(), stdout.String())
	}
	if stdout.Len() != 0 {
		t.Fatalf("unexpected stdout: %s", stdout.String())
	}
	if !strings.Contains(stderr.String(), "requires a CHAP secret reference") {
		t.Fatalf("stderr=%q, want missing secret ref diagnostic", stderr.String())
	}
}

func TestGatewayMemoryServeRejectsUnsupportedAllowlistRuntime(t *testing.T) {
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	initiator := "iqn.1993-08.org.debian:01:unit"
	code := run([]string{
		"--backend", "memory",
		"--portal", "127.0.0.1:3260",
		"--memory-lun-size", "1MiB",
		"--serve",
		"--allow-gotgt-wildcard-listen",
		"--auth-mode", "none",
		"--allowed-initiator-iqns", initiator,
		"--json",
	}, &stdout, &stderr)
	if code != 1 {
		t.Fatalf("run code=%d, want 1 stderr=%s stdout=%s", code, stderr.String(), stdout.String())
	}
	var summary iscsi.Summary
	if err := json.Unmarshal(stdout.Bytes(), &summary); err != nil {
		t.Fatalf("stdout is not summary JSON: %v\n%s", err, stdout.String())
	}
	if summary.Result != "error" || summary.AuthMode != "none" || summary.AuthPolicy != iscsi.AuthPolicyNoAuthAllowlistRuntimeFailClosed {
		t.Fatalf("unexpected allowlist rejection summary: %#v", summary)
	}
	if summary.TargetStackAccepted {
		t.Fatalf("target stack accepted unsupported allowlist: %#v", summary)
	}
	if summary.RuntimeInitiatorAllowlistSupported {
		t.Fatalf("runtime_initiator_allowlist_supported=true, want false")
	}
	if summary.InitiatorAllowlistRuntimeClaim != iscsi.InitiatorAllowlistRuntimeClaimGotgtNoHook {
		t.Fatalf("unexpected allowlist claim: %#v", summary)
	}
	if len(summary.AllowedInitiatorIQNs) != 1 || summary.AllowedInitiatorIQNs[0] != initiator {
		t.Fatalf("allowed initiators not recorded: %#v", summary.AllowedInitiatorIQNs)
	}
	if !strings.Contains(summary.FirstError, "initiator allowlist runtime enforcement is not supported") {
		t.Fatalf("first_error=%q, want allowlist unsupported diagnostic", summary.FirstError)
	}
}

func TestGatewaySBSSelfTestJSON(t *testing.T) {
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	code := run([]string{
		"--backend", "sbs",
		"--self-test",
		"--sbs-fixture-size", "1MiB",
		"--json",
	}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("run code=%d stderr=%s", code, stderr.String())
	}
	if stderr.Len() != 0 {
		t.Fatalf("unexpected stderr: %s", stderr.String())
	}
	var summary iscsi.SBSAdapterSummary
	if err := json.Unmarshal(stdout.Bytes(), &summary); err != nil {
		t.Fatalf("stdout is not SBS summary JSON: %v\n%s", err, stdout.String())
	}
	if summary.Result != "ok" || summary.BackendMode != "sbs" || summary.BackendAdapter != "sbs_client" {
		t.Fatalf("unexpected SBS summary: %#v", summary)
	}
	if summary.Entrypoint != "namrbd-iscsi-gateway --self-test" {
		t.Fatalf("entrypoint=%q", summary.Entrypoint)
	}
}

func TestGatewaySBSServeRejectsUnsupportedAllowlistBeforeOpen(t *testing.T) {
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	initiator := "iqn.1993-08.org.debian:01:sbs-unit"
	code := run([]string{
		"--backend", "sbs",
		"--portal", "127.0.0.1:3260",
		"--serve",
		"--sbs-fixture",
		"--volume-id", "00a1b2c3",
		"--iscsi-gateway-id", "gw-standby",
		"--active-iscsi-gateway-id", "gw-fixture",
		"--attachment-id", "att-00a1b2c3-0001",
		"--generation", "1",
		"--allow-gotgt-wildcard-listen",
		"--auth-mode", "none",
		"--allowed-initiator-iqns", initiator,
		"--json",
	}, &stdout, &stderr)
	if code != 1 {
		t.Fatalf("run code=%d, want 1 stderr=%s stdout=%s", code, stderr.String(), stdout.String())
	}
	var summary iscsi.SBSAdapterSummary
	if err := json.Unmarshal(stdout.Bytes(), &summary); err != nil {
		t.Fatalf("stdout is not SBS summary JSON: %v\n%s", err, stdout.String())
	}
	if summary.Result != "error" || summary.BackendMode != "sbs" || summary.AuthPolicy != iscsi.AuthPolicyNoAuthAllowlistRuntimeFailClosed {
		t.Fatalf("unexpected SBS allowlist rejection summary: %#v", summary)
	}
	if summary.BackendVolumeHandle != "" || summary.CloseRecorded {
		t.Fatalf("SBS volume appears opened despite allowlist rejection: %#v", summary)
	}
	if summary.TargetStackAccepted {
		t.Fatalf("target stack accepted unsupported allowlist: %#v", summary)
	}
	if summary.RuntimeInitiatorAllowlistSupported {
		t.Fatalf("runtime_initiator_allowlist_supported=true, want false")
	}
	if len(summary.AllowedInitiatorIQNs) != 1 || summary.AllowedInitiatorIQNs[0] != initiator {
		t.Fatalf("allowed initiators not recorded: %#v", summary.AllowedInitiatorIQNs)
	}
	if !strings.Contains(summary.FirstError, "initiator allowlist runtime enforcement is not supported") {
		t.Fatalf("first_error=%q, want allowlist unsupported diagnostic", summary.FirstError)
	}
}

func TestGatewaySBSServeRejectsUnsupportedCHAPBeforeOpen(t *testing.T) {
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	code := run([]string{
		"--backend", "sbs",
		"--portal", "127.0.0.1:3260",
		"--serve",
		"--sbs-fixture",
		"--volume-id", "00a1b2c3",
		"--iscsi-gateway-id", "gw-standby",
		"--active-iscsi-gateway-id", "gw-fixture",
		"--attachment-id", "att-00a1b2c3-0001",
		"--generation", "1",
		"--allow-gotgt-wildcard-listen",
		"--auth-mode", "chap",
		"--chap-secret-ref", "fixture-chap-sha256:feedfacecafebeef",
		"--json",
	}, &stdout, &stderr)
	if code != 1 {
		t.Fatalf("run code=%d, want 1 stderr=%s stdout=%s", code, stderr.String(), stdout.String())
	}
	var summary iscsi.SBSAdapterSummary
	if err := json.Unmarshal(stdout.Bytes(), &summary); err != nil {
		t.Fatalf("stdout is not SBS summary JSON: %v\n%s", err, stdout.String())
	}
	if summary.Result != "error" || summary.BackendMode != "sbs" || summary.AuthMode != "chap" {
		t.Fatalf("unexpected SBS CHAP rejection summary: %#v", summary)
	}
	if summary.BackendVolumeHandle != "" || summary.CloseRecorded {
		t.Fatalf("SBS volume appears opened despite CHAP rejection: %#v", summary)
	}
	if summary.TargetStackAccepted {
		t.Fatalf("target stack accepted unsupported CHAP: %#v", summary)
	}
	if !strings.Contains(summary.FirstError, "CHAP runtime is not supported") {
		t.Fatalf("first_error=%q, want CHAP unsupported diagnostic", summary.FirstError)
	}
}

func TestGatewaySBSServeFixtureReachesGotgtGuard(t *testing.T) {
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	code := run([]string{
		"--backend", "sbs",
		"--portal", "127.0.0.1:3260",
		"--serve",
		"--sbs-fixture",
		"--volume-id", "00a1b2c3",
		"--iscsi-gateway-id", "gw-standby",
		"--active-iscsi-gateway-id", "gw-fixture",
		"--attachment-id", "att-00a1b2c3-0001",
		"--generation", "1",
		"--json",
	}, &stdout, &stderr)
	if code != 1 {
		t.Fatalf("run code=%d, want 1 stderr=%s stdout=%s", code, stderr.String(), stdout.String())
	}
	var summary iscsi.SBSAdapterSummary
	if err := json.Unmarshal(stdout.Bytes(), &summary); err != nil {
		t.Fatalf("stdout is not SBS summary JSON: %v\n%s", err, stdout.String())
	}
	if summary.BackendMode != "sbs" || summary.BackendAdapter != "sbs_client" {
		t.Fatalf("unexpected backend summary: %#v", summary)
	}
	if summary.ISCSIGatewayID != "gw-standby" || summary.ActiveISCSIGatewayID != "gw-fixture" || summary.SBSDeviceID == 0 {
		t.Fatalf("summary did not preserve local/active identity and stable device id: %#v", summary)
	}
	if summary.ActivePathIOAllowed || summary.ActivePathWriteAllowed || summary.StandbyPathWriteAllowed {
		t.Fatalf("standby gateway summary advertised active I/O authority: %#v", summary)
	}
	if summary.ALUAMode != iscsi.ALUAModeImplicit || summary.ALUAAccessState != iscsi.ALUAAccessStateStandby || summary.ALUAPreferred {
		t.Fatalf("standby gateway summary advertised wrong ALUA state: %#v", summary)
	}
	if !summary.CloseRecorded {
		t.Fatalf("SBS close was not recorded: %#v", summary)
	}
	if !strings.Contains(summary.FirstError, "wildcard") {
		t.Fatalf("first_error=%q, want wildcard guard", summary.FirstError)
	}
	if !strings.Contains(stderr.String(), "wildcard") {
		t.Fatalf("stderr=%q, want wildcard guard", stderr.String())
	}
}

func TestGatewaySBSServeRequiresOneClientSource(t *testing.T) {
	baseArgs := []string{
		"--backend", "sbs",
		"--portal", "127.0.0.1:3260",
		"--serve",
		"--volume-id", "00a1b2c3",
		"--active-iscsi-gateway-id", "gw-fixture",
		"--attachment-id", "att-00a1b2c3-0001",
		"--generation", "1",
		"--allow-gotgt-wildcard-listen",
	}
	for _, tc := range []struct {
		name string
		args []string
	}{
		{name: "missing", args: baseArgs},
		{name: "both", args: append(append([]string{}, baseArgs...), "--sbs-fixture", "--sbs-endpoint", "127.0.0.1:9444")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var stdout bytes.Buffer
			var stderr bytes.Buffer
			code := run(tc.args, &stdout, &stderr)
			if code != 2 {
				t.Fatalf("run code=%d, want 2 stderr=%s", code, stderr.String())
			}
			if stdout.Len() != 0 {
				t.Fatalf("unexpected stdout: %s", stdout.String())
			}
			if !strings.Contains(stderr.String(), "requires exactly one of --sbs-fixture or --sbs-data-endpoint") {
				t.Fatalf("stderr=%q, want client source diagnostic", stderr.String())
			}
		})
	}
}

func TestGatewaySBSRegistryResolutionPopulatesServeArgs(t *testing.T) {
	oldFactory := newISCSIGatewayAdminClient
	newISCSIGatewayAdminClient = func(context.Context, string) (iscsiGatewayAdminClient, func() error, error) {
		return fakeGatewayAdminClient{
			registry: &adminv1.GetISCSIRegistryResponse{
				RegistryRevision: 7,
				ConfigGeneration: 3,
				Portals: []*adminv1.ISCSIPortalSummary{
					{PortalId: "portal-a", Address: "10.0.0.11:3260", GatewayId: "gw-a", Enabled: true},
					{PortalId: "portal-b", Address: "10.0.0.12:3260", GatewayId: "gw-b", Enabled: true},
				},
				Targets: []*adminv1.ISCSITargetSummary{
					{TargetIqn: "iqn.2026-06.io.namrbd:cluster", PortalId: "portal-a", PortalIds: []string{"portal-a", "portal-b"}, ExportId: "export-a", Enabled: true},
				},
				Luns: []*adminv1.ISCSILUNSummary{
					{TargetIqn: "iqn.2026-06.io.namrbd:cluster", LunId: 0, LunWwn: "namrbd-phase-q-export-a", ExportId: "export-a", VolumeId: "00a1b2c3", ExportMode: "read_write", LogicalBlockSizeBytes: 4096, Enabled: true},
				},
				Failovers: []*adminv1.ISCSIFailoverRuntimeSummary{
					{ExportId: "export-a", ActiveIscsiGatewayId: "gw-b", ExportLeaseId: "lease-a", ExportEpoch: 4, State: "active"},
				},
			},
		}, func() error { return nil }, nil
	}
	t.Cleanup(func() { newISCSIGatewayAdminClient = oldFactory })

	args := sbsGatewayArgs{
		targetIQN:        "iqn.2026-06.io.namrbd:cluster",
		lunID:            0,
		sbsAdminEndpoint: "127.0.0.1:9443",
		iscsiGatewayID:   "gw-b",
	}
	if err := resolveSBSGatewayRegistry(context.Background(), &args); err != nil {
		t.Fatalf("resolveSBSGatewayRegistry: %v", err)
	}
	if !args.registryLoaded || args.registryRevision != 7 || args.registryConfigGeneration != 3 {
		t.Fatalf("registry evidence not populated: %#v", args)
	}
	if args.volumeID != "00a1b2c3" || args.exportID != "export-a" || args.lunWWN != "namrbd-phase-q-export-a" {
		t.Fatalf("LUN fields not populated from registry: %#v", args)
	}
	if args.portal != "10.0.0.12:3260" || args.registryPortalID != "portal-b" {
		t.Fatalf("portal was not selected for local gateway: %#v", args)
	}
	if args.activeISCSIGatewayID != "gw-b" || args.exportLeaseID != "lease-a" || args.exportEpoch != 4 || !args.registryFailoverFound {
		t.Fatalf("failover fields not populated: %#v", args)
	}
	if args.aluaTargetPortGroupID != 2 || args.aluaAccessState != iscsi.ALUAAccessStateActiveOptimized || !args.aluaPreferred {
		t.Fatalf("ALUA fields not derived from registry portal/failover state: %#v", args)
	}
}

func TestGatewayALUARegistryStatePreservesExplicitOverrides(t *testing.T) {
	args := sbsGatewayArgs{
		iscsiGatewayID:        "gw-b",
		activeISCSIGatewayID:  "gw-b",
		aluaTargetPortGroupID: 42,
		aluaAccessState:       iscsi.ALUAAccessStateStandby,
		aluaPreferred:         false,
	}
	applyGatewayALUARegistryState(&args, []string{"portal-a", "portal-b"}, &adminv1.ISCSIPortalSummary{PortalId: "portal-b"})
	if args.aluaTargetPortGroupID != 42 || args.aluaAccessState != iscsi.ALUAAccessStateStandby || args.aluaPreferred {
		t.Fatalf("explicit ALUA overrides were not preserved: %#v", args)
	}
}

type fakeGatewayAdminClient struct {
	registry *adminv1.GetISCSIRegistryResponse
}

func (f fakeGatewayAdminClient) GetISCSIRegistry(context.Context, *adminv1.GetISCSIRegistryRequest, ...grpc.CallOption) (*adminv1.GetISCSIRegistryResponse, error) {
	return f.registry, nil
}
