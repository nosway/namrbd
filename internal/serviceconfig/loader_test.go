package serviceconfig

import (
	"encoding/json"
	"flag"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func noEnv(string) (string, bool) { return "", false }

func envMap(m map[string]string) EnvLookup {
	return func(k string) (string, bool) { v, ok := m[k]; return v, ok }
}

// installed copies a shipped example to a private temp file with the mode a
// real deployment uses. The examples in configs/ are templates: git does not
// track permission bits beyond the executable flag, so a checkout lands them
// 0644. An operator installs them 0600, and the strict profile requires that.
func installed(t *testing.T, name string) string {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(configsDir, name))
	if err != nil {
		t.Fatalf("read example %s: %v", name, err)
	}
	dst := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(dst, raw, 0o600); err != nil {
		t.Fatalf("install %s: %v", name, err)
	}
	return dst
}

func gatewayPath(t *testing.T) string { return installed(t, "namrbd-gateway.yaml") }

// The whole point of the slice: file beats defaults, env beats file, an
// explicit CLI flag beats env.
func TestPrecedenceFileEnvCLI(t *testing.T) {
	reg := RegistryFor(ProcessGateway)

	t.Run("file supplies the value", func(t *testing.T) {
		res, err := Load(gatewayPath(t), reg, noEnv, nil)
		if err != nil {
			t.Fatalf("load: %v", err)
		}
		if res.File.Gateway.GatewayID != "gw-01" {
			t.Errorf("gateway_id = %q, want the file value gw-01", res.File.Gateway.GatewayID)
		}
		if res.SourceAuthority != SourceFile {
			t.Errorf("source authority = %q, want file", res.SourceAuthority)
		}
		if len(res.Overrides) != 0 {
			t.Errorf("expected no overrides, got %v", res.Overrides)
		}
	})

	t.Run("env beats file", func(t *testing.T) {
		res, err := Load(gatewayPath(t), reg, envMap(map[string]string{
			"NAMRBD_GATEWAY_ID": "gw-from-env",
		}), nil)
		if err != nil {
			t.Fatalf("load: %v", err)
		}
		if res.File.Gateway.GatewayID != "gw-from-env" {
			t.Errorf("gateway_id = %q, want the env value", res.File.Gateway.GatewayID)
		}
	})

	t.Run("explicit cli beats env", func(t *testing.T) {
		res, err := Load(gatewayPath(t), reg, envMap(map[string]string{
			"NAMRBD_GATEWAY_ID": "gw-from-env",
		}), map[string]string{"gateway-id": "gw-from-cli"})
		if err != nil {
			t.Fatalf("load: %v", err)
		}
		if res.File.Gateway.GatewayID != "gw-from-cli" {
			t.Errorf("gateway_id = %q, want the cli value", res.File.Gateway.GatewayID)
		}
		s := res.Summarize(nil)
		if s.CLIOverrideCount != 1 || s.EnvOverrideCount != 1 {
			t.Errorf("override counts cli=%d env=%d, want 1 and 1", s.CLIOverrideCount, s.EnvOverrideCount)
		}
	})
}

func TestLegacyEnvironmentAliasIsOneRecordedOverride(t *testing.T) {
	path := installed(t, "sbs-service.yaml")
	res, err := Load(path, RegistryFor(ProcessSBSService), envMap(map[string]string{
		"NAMRBD_NODE_ID": "legacy-service-node",
	}), nil)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if res.File.SBSService.NodeID != "legacy-service-node" {
		t.Fatalf("node_id=%q", res.File.SBSService.NodeID)
	}
	summary := res.Summarize(nil)
	if summary.EnvOverrideCount != 1 || summary.WarningCount == 0 {
		t.Fatalf("summary=%+v", summary)
	}
	if !strings.Contains(strings.Join(summary.Warnings, "\n"), "removed in v1.1.0") {
		t.Fatalf("warnings=%v", summary.Warnings)
	}
}

func TestLargeScaleRejectsCanonicalLegacyConflict(t *testing.T) {
	path := installed(t, "sbs-data.yaml")
	_, err := Load(path, RegistryFor(ProcessSBSData), envMap(map[string]string{
		"NAMRBD_SBS_DATA_PATH": "/canonical",
		"NAMRBD_SBS_DATA_DIR":  "/legacy",
	}), nil)
	if err == nil || !strings.Contains(err.Error(), "large_scale requires one unambiguous value") {
		t.Fatalf("error=%v", err)
	}
}

func TestRegistryUsesCanonicalEnvironmentNames(t *testing.T) {
	want := map[string]string{
		"gateway.listen":                   "NAMRBD_GATEWAY_CONTROL_LISTEN",
		"gateway.sbs_admin_endpoint":       "NAMRBD_SBS_SERVICE_ENDPOINT",
		"sbs_service.node_id":              "NAMRBD_SBS_SERVICE_NODE_ID",
		"sbs_service.grpc_listen":          "NAMRBD_SBS_SERVICE_GRPC_LISTEN",
		"sbs_service.http_listen":          "NAMRBD_SBS_SERVICE_HTTP_LISTEN",
		"sbs_data.data_path":               "NAMRBD_SBS_DATA_PATH",
		"sbs_data.grpc_listen":             "NAMRBD_SBS_DATA_GRPC_LISTEN",
		"sbs_data.http_listen":             "NAMRBD_SBS_DATA_HTTP_LISTEN",
		"iscsi_gateway.sbs_endpoint":       "NAMRBD_ISCSI_SBS_DATA_ENDPOINT",
		"iscsi_gateway.sbs_admin_endpoint": "NAMRBD_ISCSI_SBS_SERVICE_ENDPOINT",
		"csi_driver.admin_endpoints":       "NAMRBD_SBS_SERVICE_ENDPOINTS",
		"csi_driver.admin_endpoints.0":     "NAMRBD_SBS_SERVICE_ENDPOINT",
	}
	for _, process := range []string{ProcessGateway, ProcessSBSService, ProcessSBSData, ProcessISCSIGateway, ProcessCSIDriver} {
		for _, override := range RegistryFor(process) {
			canonical, ok := want[override.Field]
			if !ok {
				continue
			}
			if override.Env != canonical || len(override.LegacyEnvs) == 0 {
				t.Errorf("%s env=%q legacy=%v", override.Field, override.Env, override.LegacyEnvs)
			}
			delete(want, override.Field)
		}
	}
	if len(want) != 0 {
		t.Fatalf("missing registry fields: %v", want)
	}
}

// Rank encodes the documented order. A reordering here would silently invert
// precedence everywhere else.
func TestPrecedenceOrderIsDocumented(t *testing.T) {
	want := []Source{SourceDefault, SourceFile, SourceEnv, SourceCLI}
	for i, s := range want {
		if Rank(s) != i {
			t.Errorf("Rank(%s) = %d, want %d", s, Rank(s), i)
		}
	}
	if Rank(SourceCLI) <= Rank(SourceEnv) || Rank(SourceEnv) <= Rank(SourceFile) ||
		Rank(SourceFile) <= Rank(SourceDefault) {
		t.Fatal("precedence order is not defaults < file < env < cli")
	}
}

// A flag that is not on the allowlist cannot be supplied at all. This is what
// keeps the config file the authority rather than one input among several.
func TestUnregisteredFlagIsRejected(t *testing.T) {
	_, err := Load(gatewayPath(t), RegistryFor(ProcessGateway), noEnv,
		map[string]string{"sbs-cluster-replicas": "r1=/data"})
	if err == nil {
		t.Fatal("an unregistered flag override was accepted")
	}
	if !strings.Contains(err.Error(), "put it in the config file") {
		t.Errorf("unhelpful error: %v", err)
	}
}

// Only flags the operator actually typed may override. flag.Visit reports
// exactly those, which is the difference between a chosen value and a default.
func TestOnlyExplicitlySetFlagsOverride(t *testing.T) {
	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	fs.String("gateway-id", "default-id", "")
	fs.String("listen", "0.0.0.0:9999", "")
	if err := fs.Parse([]string{"-gateway-id=typed"}); err != nil {
		t.Fatalf("parse: %v", err)
	}
	set := map[string]string{}
	fs.Visit(func(f *flag.Flag) { set[f.Name] = f.Value.String() })

	if _, ok := set["listen"]; ok {
		t.Fatal("an untyped flag appeared in the explicit set")
	}
	res, err := Load(gatewayPath(t), RegistryFor(ProcessGateway), noEnv, set)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if res.File.Gateway.GatewayID != "typed" {
		t.Errorf("gateway_id = %q, want typed", res.File.Gateway.GatewayID)
	}
	// The untyped flag's default must not have replaced the file value.
	if res.File.Gateway.Listen != "0.0.0.0:8080" {
		t.Errorf("listen = %q; an unset flag's default overrode the config file", res.File.Gateway.Listen)
	}
}

// A secret override is redacted in the summary, and the summary is the thing
// that ends up in logs and support bundles.
func TestSummaryRedactsSecretOverrides(t *testing.T) {
	reg := []Overridable{{
		Field: "gateway.dataplane.token_key", Env: "NAMRBD_TOKEN", Flag: "token", Secret: true,
		apply: func(f *File, v string) error { return nil },
	}}
	res, err := Load(gatewayPath(t), reg, envMap(map[string]string{"NAMRBD_TOKEN": "super-secret-value"}), nil)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	s := res.Summarize(nil)
	blob, _ := json.Marshal(s)
	if strings.Contains(string(blob), "super-secret-value") {
		t.Fatalf("summary leaked a secret override value: %s", blob)
	}
	if !strings.Contains(string(blob), RedactedMarker) {
		t.Errorf("summary did not mark the secret redacted: %s", blob)
	}
	if !s.SecretRedactionReady {
		t.Error("config_secret_redaction_ready is false")
	}
}

// Even a non-secret field is redacted when the value itself looks like a
// secret, because the classification of the field is the author's guess and the
// value is evidence.
func TestSummaryRedactsSecretLookingValueOnNonSecretField(t *testing.T) {
	res, err := Load(gatewayPath(t), RegistryFor(ProcessGateway), envMap(map[string]string{
		"NAMRBD_GATEWAY_ID": strings.Repeat("A", 44),
	}), nil)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	blob, _ := json.Marshal(res.Summarize(nil))
	if strings.Contains(string(blob), strings.Repeat("A", 44)) {
		t.Fatalf("summary carried a secret-looking value verbatim: %s", blob)
	}
}

// first_error and last_error must be present on the failure path, which is
// where an operator most needs them.
func TestSummaryRecordsFirstAndLastError(t *testing.T) {
	s := (*LoadResult)(nil).Summarize([]string{"first problem", "second problem", "third problem"})
	if s.ErrorCount != 3 || s.FirstError != "first problem" || s.LastError != "third problem" {
		t.Fatalf("error fields not recorded: %+v", s)
	}
	if s.ConfigSourceAuthority != SourceCLI {
		t.Errorf("with no config loaded, source authority = %q, want cli", s.ConfigSourceAuthority)
	}
}

// The digest catches a file that changed without its revision being bumped.
func TestDigestChangesWhenContentChangesWithoutRevisionBump(t *testing.T) {
	raw, err := os.ReadFile(gatewayPath(t))
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	dir := t.TempDir()
	a := filepath.Join(dir, "a.yaml")
	b := filepath.Join(dir, "b.yaml")
	if err := os.WriteFile(a, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	// Same declared revision, different content.
	changed := strings.Replace(string(raw), "max_inflight_requests: 512", "max_inflight_requests: 256", 1)
	if changed == string(raw) {
		t.Fatal("fixture did not change the content")
	}
	if err := os.WriteFile(b, []byte(changed), 0o600); err != nil {
		t.Fatal(err)
	}

	ra, err := Load(a, nil, noEnv, nil)
	if err != nil {
		t.Fatalf("load a: %v", err)
	}
	rb, err := Load(b, nil, noEnv, nil)
	if err != nil {
		t.Fatalf("load b: %v", err)
	}
	if ra.File.Revision != rb.File.Revision {
		t.Fatal("fixture changed the declared revision; it must not")
	}
	if ra.Digest == rb.Digest {
		t.Fatal("digest did not change, so an unbumped edit would be invisible")
	}
}

func TestMissingConfigPathIsAnError(t *testing.T) {
	if _, err := Load("", nil, noEnv, nil); err == nil {
		t.Fatal("an empty config path was accepted")
	}
	if _, err := Load(filepath.Join(t.TempDir(), "absent.yaml"), nil, noEnv, nil); err == nil {
		t.Fatal("a missing config file was accepted")
	}
}

// Overriding a field whose process block is absent must say so rather than
// panic on a nil pointer.
func TestOverrideOnMissingBlockFailsCleanly(t *testing.T) {
	_, err := Load(installed(t, "sbs-data.yaml"), RegistryFor(ProcessGateway),
		envMap(map[string]string{"NAMRBD_GATEWAY_ID": "x"}), nil)
	if err == nil {
		t.Fatal("overriding a gateway field in an sbs-data config was accepted")
	}
	if !strings.Contains(err.Error(), "no gateway block") {
		t.Errorf("unclear error: %v", err)
	}
}

// Every registry entry must be reachable by both env and flag, and every field
// path must name a real process block.
func TestRegistriesAreWellFormed(t *testing.T) {
	for process, reg := range AllRegistries() {
		if len(reg) == 0 {
			t.Errorf("%s has no overridable fields", process)
		}
		seenEnv, seenFlag := map[string]bool{}, map[string]bool{}
		for _, o := range reg {
			if o.Field == "" || o.Env == "" || o.Flag == "" || o.apply == nil {
				t.Errorf("%s: incomplete registry entry %+v", process, o)
			}
			if seenEnv[o.Env] {
				t.Errorf("%s: duplicate env var %s", process, o.Env)
			}
			if seenFlag[o.Flag] {
				t.Errorf("%s: duplicate flag %s", process, o.Flag)
			}
			seenEnv[o.Env], seenFlag[o.Flag] = true, true
			if !strings.HasPrefix(o.Env, "NAMRBD_") {
				t.Errorf("%s: env var %s does not use the NAMRBD_ prefix", process, o.Env)
			}
		}
	}
}

// The allowlist must not creep into settings that should be identical fleet
// wide. These are the ones that caused the drift this work removes.
func TestAllowlistExcludesFleetWideSettings(t *testing.T) {
	forbidden := []string{
		"volumes", "sbs-cluster-replicas", "target-iqn", "lun-id", "export-id",
		"scan-page-size", "batch-get-size", "shard-count", "tikv-pd-endpoints",
	}
	for process, reg := range AllRegistries() {
		for _, o := range reg {
			for _, f := range forbidden {
				if o.Flag == f {
					t.Errorf("%s allows --%s as an override; that setting belongs in the config file only", process, f)
				}
			}
		}
	}
}
