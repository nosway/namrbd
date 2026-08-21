package serviceconfig

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func loadInstalled(t *testing.T, name string, edit func(string) string) *File {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(configsDir, name))
	if err != nil {
		t.Fatalf("read %s: %v", name, err)
	}
	body := string(raw)
	if edit != nil {
		body = edit(body)
	}
	p := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(p, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	res, err := Load(p, nil, noEnv, nil)
	if err != nil {
		t.Fatalf("load %s: %v", name, err)
	}
	return res.File
}

// Every field of every process config must be classified. Guessing "live" for
// an unclassified field is how a reload silently does nothing while reporting
// success.
func TestEveryFieldIsClassified(t *testing.T) {
	blocks := map[string]reflect.Type{
		ProcessGateway:      reflect.TypeOf(GatewayConfig{}),
		ProcessISCSIGateway: reflect.TypeOf(ISCSIGatewayConfig{}),
		ProcessSBSService:   reflect.TypeOf(SBSServiceConfig{}),
		ProcessSBSData:      reflect.TypeOf(SBSDataConfig{}),
		ProcessCSIDriver:    reflect.TypeOf(CSIDriverConfig{}),
		ProcessMCP:          reflect.TypeOf(MCPConfig{}),
	}
	prefixes := map[string]string{
		ProcessGateway: "gateway", ProcessISCSIGateway: "iscsi_gateway",
		ProcessSBSService: "sbs_service", ProcessSBSData: "sbs_data",
		ProcessCSIDriver: "csi_driver", ProcessMCP: "mcp",
	}
	for process, typ := range blocks {
		policy := ReloadPolicyFor(process)
		for _, path := range FieldPaths(typ, prefixes[process]) {
			if _, ok := classify(policy, path); !ok {
				t.Errorf("%s: field %s has no reload classification", process, path)
			}
		}
		for _, top := range []string{"schema_version", "process", "profile", "revision"} {
			if _, ok := classify(policy, top); !ok {
				t.Errorf("%s: top-level field %s has no reload classification", process, top)
			}
		}
	}
}

// A restart classification must say what makes the field different. "Restart
// required" with no reason is not actionable.
func TestRestartClassificationsExplainThemselves(t *testing.T) {
	for _, process := range []string{ProcessGateway, ProcessISCSIGateway, ProcessSBSService,
		ProcessSBSData, ProcessCSIDriver, ProcessMCP} {
		for path, p := range ReloadPolicyFor(process) {
			if p.Class == ReloadRestart && strings.TrimSpace(p.Why) == "" {
				t.Errorf("%s: %s is restart-class with no reason", process, path)
			}
			if p.Class == ReloadLive && p.Why != "" {
				t.Errorf("%s: %s is live but carries a restart reason", process, path)
			}
		}
	}
}

// A live-only change is accepted and names what it applied.
func TestLiveFieldChangeIsApplied(t *testing.T) {
	cur := loadInstalled(t, "sbs-service.yaml", nil)
	next := loadInstalled(t, "sbs-service.yaml", func(b string) string {
		b = strings.Replace(b, "revision: 1", "revision: 2", 1)
		return strings.Replace(b, "    timeout_seconds: 5", "    timeout_seconds: 8", 1)
	})
	res := Reload(cur, next, ProcessSBSService)
	if !res.Accepted {
		t.Fatalf("a live-only change was rejected: %+v", res)
	}
	if len(res.RestartRequired) != 0 {
		t.Errorf("restart required for a live change: %v", res.RestartRequired)
	}
	found := false
	for _, f := range res.Applied {
		if f == "sbs_service.tikv.timeout_seconds" {
			found = true
		}
	}
	if !found {
		t.Errorf("the changed field was not reported as applied: %v", res.Applied)
	}
	if res.FromRevision != 1 || res.ToRevision != 2 {
		t.Errorf("revisions not recorded: %d -> %d", res.FromRevision, res.ToRevision)
	}
}

// A reload is all or nothing. A file mixing a live and a restart change applies
// nothing, because a partial application would leave the process matching
// neither file and make the revision meaningless.
func TestReloadIsAllOrNothing(t *testing.T) {
	cur := loadInstalled(t, "sbs-service.yaml", nil)
	next := loadInstalled(t, "sbs-service.yaml", func(b string) string {
		b = strings.Replace(b, "revision: 1", "revision: 2", 1)
		b = strings.Replace(b, "    timeout_seconds: 5", "    timeout_seconds: 8", 1)
		return strings.Replace(b, "  grpc_listen: 0.0.0.0:9090", "  grpc_listen: 0.0.0.0:9099", 1)
	})
	res := Reload(cur, next, ProcessSBSService)
	if res.Accepted {
		t.Fatal("a reload changing a bound socket was accepted")
	}
	if len(res.Applied) != 0 {
		t.Errorf("fields were applied despite a restart-required change: %v", res.Applied)
	}
	if len(res.RestartRequired) == 0 {
		t.Fatal("the restart-required field was not reported")
	}
	if !strings.Contains(strings.Join(res.RestartRequired, " "), "already bound") {
		t.Errorf("the reason was not reported: %v", res.RestartRequired)
	}
	if !strings.Contains(res.RollbackNote, "remains in effect") {
		t.Errorf("the rollback note does not state what is running: %q", res.RollbackNote)
	}
}

// An invalid candidate leaves the running configuration in effect.
func TestInvalidCandidateIsRejectedAndOldConfigStays(t *testing.T) {
	cur := loadInstalled(t, "sbs-data.yaml", nil)
	next := loadInstalled(t, "sbs-data.yaml", func(b string) string {
		return strings.Replace(b, "revision: 1", "revision: 2", 1)
	})
	next.SBSData.DataPath = "" // required
	res := Reload(cur, next, ProcessSBSData)
	if res.Accepted {
		t.Fatal("an invalid candidate was accepted")
	}
	if len(res.Errors) == 0 {
		t.Error("no error was recorded")
	}
	if !strings.Contains(res.RollbackNote, "remains in effect") {
		t.Errorf("rollback note = %q", res.RollbackNote)
	}
}

// A candidate for a different process is refused.
func TestReloadRejectsDifferentProcess(t *testing.T) {
	cur := loadInstalled(t, "sbs-data.yaml", nil)
	next := loadInstalled(t, "sbs-service.yaml", nil)
	if Reload(cur, next, ProcessSBSData).Accepted {
		t.Fatal("a config for another process was accepted as a reload")
	}
}

// Reloading the same content is a no-op that still reports success, so a
// converging rollout does not look like a failure.
func TestNoChangeIsAcceptedAsNoOp(t *testing.T) {
	cur := loadInstalled(t, "namrbd-mcp.yaml", nil)
	next := loadInstalled(t, "namrbd-mcp.yaml", nil)
	res := Reload(cur, next, ProcessMCP)
	if !res.Accepted {
		t.Fatalf("an identical reload was rejected: %+v", res)
	}
	if len(res.Applied) != 0 {
		t.Errorf("an identical reload reported applied fields: %v", res.Applied)
	}
	if !strings.Contains(res.RollbackNote, "unchanged") {
		t.Errorf("rollback note = %q", res.RollbackNote)
	}
}

// Secret-bearing fields are restart-class, and a reload result never carries
// secret material even when one changes.
func TestReloadResultCarriesNoSecretMaterial(t *testing.T) {
	cur := loadInstalled(t, "namrbd-gateway.yaml", nil)
	next := loadInstalled(t, "namrbd-gateway.yaml", func(b string) string {
		b = strings.Replace(b, "revision: 1", "revision: 2", 1)
		return strings.Replace(b, "/etc/namrbd/secrets/dataplane-token.key",
			"/etc/namrbd/secrets/rotated-token.key", 1)
	})
	res := Reload(cur, next, ProcessGateway)
	if res.Accepted {
		t.Fatal("rotating token material under live sessions was accepted as a live reload")
	}
	blob, _ := json.Marshal(res)
	// The reference path is fine to name; material never appears because the
	// result reports field paths, not values.
	for _, forbidden := range []string{"BEGIN", "s3cr3t"} {
		if strings.Contains(string(blob), forbidden) {
			t.Errorf("reload result carried secret material: %s", blob)
		}
	}
	if !strings.Contains(strings.Join(res.RestartRequired, " "), "token_key") {
		t.Errorf("the rotated field was not named: %v", res.RestartRequired)
	}
}

// Identity, sockets, and connected clients must be restart-class everywhere.
// These are the classifications a later edit is most likely to get wrong.
func TestIdentityAndSocketsAreRestartClassEverywhere(t *testing.T) {
	mustRestart := map[string][]string{
		ProcessGateway:      {"gateway.gateway_id", "gateway.listen", "gateway.etcd.endpoints", "gateway.tls.enable"},
		ProcessISCSIGateway: {"iscsi_gateway.gateway_id", "iscsi_gateway.advertise_portals"},
		ProcessSBSService:   {"sbs_service.node_id", "sbs_service.grpc_listen", "sbs_service.tikv.pd_endpoints", "sbs_service.cluster_id"},
		ProcessSBSData:      {"sbs_data.cluster_id", "sbs_data.sbs_cluster_id", "sbs_data.node_id", "sbs_data.data_path", "sbs_data.grpc_listen"},
		ProcessCSIDriver:    {"csi_driver.node_id", "csi_driver.endpoint", "csi_driver.cluster_id"},
		ProcessMCP:          {"mcp.mode", "mcp.approval_policy"},
	}
	for process, paths := range mustRestart {
		policy := ReloadPolicyFor(process)
		for _, path := range paths {
			p, ok := classify(policy, path)
			if !ok {
				t.Errorf("%s: %s is unclassified", process, path)
				continue
			}
			if p.Class != ReloadRestart {
				t.Errorf("%s: %s is %s; identity, bound sockets, and connected clients cannot change under a running process",
					process, path, p.Class)
			}
		}
	}
}

// The profile decides what is admissible at all, so a reload must not change it.
func TestProfileAndProcessAreRestartClass(t *testing.T) {
	cur := loadInstalled(t, "sbs-data.yaml", nil)
	next := loadInstalled(t, "sbs-data.yaml", func(b string) string {
		b = strings.Replace(b, "revision: 1", "revision: 2", 1)
		return strings.Replace(b, "profile: large_scale", "profile: dev", 1)
	})
	res := Reload(cur, next, ProcessSBSData)
	if res.Accepted {
		t.Fatal("a reload changed the profile")
	}
	if !strings.Contains(strings.Join(res.RestartRequired, " "), "profile") {
		t.Errorf("the profile change was not reported: %v", res.RestartRequired)
	}
}

// A field nobody classified must refuse the reload rather than be assumed live.
//
// Every field is classified today, so this branch is otherwise never exercised
// and a regression that treated unknown fields as live would pass unnoticed.
// Reloading against a process with no policy reaches it directly.
func TestUnclassifiedFieldRefusesRatherThanAssumingLive(t *testing.T) {
	cur := loadInstalled(t, "sbs-data.yaml", nil)
	next := loadInstalled(t, "sbs-data.yaml", func(b string) string {
		b = strings.Replace(b, "revision: 1", "revision: 2", 1)
		return strings.Replace(b, "  http_listen: 127.0.0.1:9093", "  http_listen: 127.0.0.1:9099", 1)
	})
	res := Reload(cur, next, "namrbd-unknown-process")
	if res.Accepted {
		t.Fatal("a change with no classification was accepted as a live reload")
	}
	if len(res.Applied) != 0 {
		t.Errorf("unclassified fields were applied: %v", res.Applied)
	}
	joined := strings.Join(res.Errors, " ")
	if !strings.Contains(joined, "no reload classification") {
		t.Errorf("the refusal does not say the field is unclassified: %v", res.Errors)
	}
	if !strings.Contains(joined, "rather than guessing") {
		t.Errorf("the refusal does not explain why it refuses: %v", res.Errors)
	}
	if !strings.Contains(res.RollbackNote, "remains in effect") {
		t.Errorf("rollback note = %q", res.RollbackNote)
	}
}
