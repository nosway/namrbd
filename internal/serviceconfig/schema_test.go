package serviceconfig

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

const configsDir = "../../configs"

func loadFile(t *testing.T, path string) *File {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	var f File
	// KnownFields makes an unrecognized key an error rather than a silently
	// ignored setting, which is how a typo becomes a production surprise.
	dec := yaml.NewDecoder(strings.NewReader(string(raw)))
	dec.KnownFields(true)
	if err := dec.Decode(&f); err != nil {
		t.Fatalf("parse %s: %v", path, err)
	}
	return &f
}

// Every shipped example must parse and validate. These are what operators copy,
// so a broken example is a broken deployment.
func TestShippedExamplesValidate(t *testing.T) {
	entries, err := filepath.Glob(filepath.Join(configsDir, "*.yaml"))
	if err != nil {
		t.Fatalf("glob: %v", err)
	}
	if len(entries) != 6 {
		t.Fatalf("expected 6 example configs, found %d: %v", len(entries), entries)
	}
	seen := map[string]bool{}
	for _, path := range entries {
		t.Run(filepath.Base(path), func(t *testing.T) {
			f := loadFile(t, path)
			res := Validate(f)
			if !res.OK() {
				t.Fatalf("example %s does not validate: %s", path, strings.Join(res.Errors, "; "))
			}
			if f.Profile != ProfileLargeScale {
				t.Errorf("example %s uses profile %q; examples ship the strict profile", path, f.Profile)
			}
			// The filename must match the process, or an operator copying by
			// name gets a config for a different service.
			want := strings.TrimSuffix(filepath.Base(path), ".yaml")
			if f.Process != want {
				t.Errorf("example %s declares process %q", path, f.Process)
			}
			seen[f.Process] = true
		})
	}
	for _, p := range sortedProcesses() {
		if !seen[p] {
			t.Errorf("no shipped example configures %s", p)
		}
	}
}

// No example may carry a secret value. This is the AA-IMPL-001A gate.
func TestShippedExamplesCarryNoSecretLiterals(t *testing.T) {
	entries, _ := filepath.Glob(filepath.Join(configsDir, "*.yaml"))
	for _, path := range entries {
		raw, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read %s: %v", path, err)
		}
		for i, line := range strings.Split(string(raw), "\n") {
			trimmed := strings.TrimSpace(line)
			if trimmed == "" || strings.HasPrefix(trimmed, "#") {
				continue
			}
			value := trimmed
			if idx := strings.Index(trimmed, ":"); idx >= 0 {
				value = strings.TrimSpace(trimmed[idx+1:])
			}
			value = strings.Trim(value, `"'`)
			if LooksLikeSecretLiteral(value) {
				t.Errorf("%s:%d carries what looks like a secret value: %q", path, i+1, trimmed)
			}
		}
	}
}

// A config that names one process but carries another's block is rejected,
// so a gateway cannot start from an sbs-service file.
func TestProcessBlockMustMatchDeclaredProcess(t *testing.T) {
	f := loadFile(t, filepath.Join(configsDir, "sbs-data.yaml"))
	f.Process = ProcessGateway
	res := Validate(f)
	if res.OK() {
		t.Fatal("a mismatched process and block was accepted")
	}
	if !strings.Contains(strings.Join(res.Errors, "; "), "carries a") {
		t.Errorf("unexpected errors: %v", res.Errors)
	}
}

func TestExactlyOneProcessBlock(t *testing.T) {
	f := loadFile(t, filepath.Join(configsDir, "sbs-data.yaml"))
	f.MCP = &MCPConfig{OperationsEndpoint: "x", Mode: MCPModeObserve, ApprovalPolicy: "dry-run"}
	res := Validate(f)
	if res.OK() {
		t.Fatal("a config with two process blocks was accepted")
	}
}

// A secret reference names exactly one source; two would need an undocumented
// precedence rule of its own.
func TestSecretRefRejectsMultipleSources(t *testing.T) {
	s := SecretRef{File: "/a", Env: "B"}
	if err := s.Validate("x.key"); err == nil {
		t.Fatal("a reference naming two sources was accepted")
	}
	if (SecretRef{File: "/a"}).Validate("x.key") != nil {
		t.Fatal("a single-source reference was rejected")
	}
}

func TestSecretRefStringDoesNotResolve(t *testing.T) {
	cases := map[SecretRef]string{
		{}:                      "<unset>",
		{File: "/etc/k"}:        "file:/etc/k",
		{Env: "NAMRBD_KEY"}:     "env:NAMRBD_KEY",
		{KMS: "projects/x/k/y"}: "kms:projects/x/k/y",
	}
	for ref, want := range cases {
		if got := ref.String(); got != want {
			t.Errorf("SecretRef%+v.String() = %q, want %q", ref, got, want)
		}
	}
}

func TestLooksLikeSecretLiteral(t *testing.T) {
	secrets := []string{
		"-----BEGIN RSA PRIVATE KEY-----",
		"-----BEGIN CERTIFICATE-----",
		"password: hunter2",
		"chap_secret=s3cr3tvalue",
		strings.Repeat("A", 44),
		strings.Repeat("a1", 40),
	}
	for _, s := range secrets {
		if !LooksLikeSecretLiteral(s) {
			t.Errorf("secret literal not detected: %q", s)
		}
	}
	safe := []string{
		"", "/etc/namrbd/tls/gateway.key", "NAMRBD_DATAPLANE_SESSION_KEY",
		"gw-01", "0.0.0.0:8080", "iqn.2026-01.com.example:host-a",
		"/namrbd/prod/gateways", "namrbd.csi.nosway.io",
	}
	for _, s := range safe {
		if LooksLikeSecretLiteral(s) {
			t.Errorf("false positive on a non-secret value: %q", s)
		}
	}
}

// The large_scale profile is where the AA-IMPL-001 gate lives. Each case is a
// setting that cannot be operated at t2_large.
func TestLargeScaleProfileRejectsUnoperableSettings(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*File)
		want   string
	}{
		{"gateway without etcd fleet membership",
			func(f *File) { f.Gateway.Etcd = nil }, "etcd.endpoints is required"},
		{"gateway with tracing on",
			func(f *File) { f.Gateway.Observability.Trace = true }, "trace must be false"},
		{"gateway with debug endpoints",
			func(f *File) { f.Gateway.Observability.DebugEndpoints = true }, "debug_endpoints must be false"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := loadFile(t, filepath.Join(configsDir, "namrbd-gateway.yaml"))
			tc.mutate(f)
			res := Validate(f)
			if res.OK() {
				t.Fatalf("large_scale profile accepted: %s", tc.name)
			}
			if !strings.Contains(strings.Join(res.Errors, "; "), tc.want) {
				t.Errorf("errors %v do not mention %q", res.Errors, tc.want)
			}
		})
	}
}

func TestLargeScaleISCSIRejectsRestartOnlyAndLowExportCap(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*File)
		want   string
	}{
		{"reload none", func(f *File) { f.ISCSIGetway.Reload.Mode = ReloadModeNone }, "must not be none"},
		{"export cap below tier need", func(f *File) { f.ISCSIGetway.Reload.MaxExportsPerProcess = 8 }, "at least 32"},
		{"no etcd fleet registry", func(f *File) { f.ISCSIGetway.Etcd = nil }, "etcd.endpoints is required"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := loadFile(t, filepath.Join(configsDir, "namrbd-iscsi-gateway.yaml"))
			tc.mutate(f)
			res := Validate(f)
			if res.OK() {
				t.Fatalf("large_scale profile accepted: %s", tc.name)
			}
			if !strings.Contains(strings.Join(res.Errors, "; "), tc.want) {
				t.Errorf("errors %v do not mention %q", res.Errors, tc.want)
			}
		})
	}
}

func TestLargeScaleSBSServiceEnforcesScanAndHealthBudgets(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*File)
		want   string
	}{
		{"scan page over budget", func(f *File) { f.SBSService.TiKV.ScanPageSize = 5000 }, "bounds it to 1..512"},
		{"batch get over budget", func(f *File) { f.SBSService.TiKV.BatchGetSize = 1000 }, "bounds it to 1..128"},
		{"tikv tracing in production", func(f *File) { f.SBSService.TiKV.OperationTrace = true }, "operation_trace must be false"},
		{"health not sharded", func(f *File) { f.SBSService.Health.ShardCount = 1 }, "at least 4 shards"},
		{"health concurrency unbounded", func(f *File) { f.SBSService.Health.ConcurrencyPerShard = 512 }, "bounds it to 1..16"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := loadFile(t, filepath.Join(configsDir, "sbs-service.yaml"))
			tc.mutate(f)
			res := Validate(f)
			if res.OK() {
				t.Fatalf("large_scale profile accepted: %s", tc.name)
			}
			if !strings.Contains(strings.Join(res.Errors, "; "), tc.want) {
				t.Errorf("errors %v do not mention %q", res.Errors, tc.want)
			}
		})
	}
}

// A renew interval at or past the lease duration means the lease expires before
// it renews. This is the kind of pair that is easy to get wrong across copied
// command lines and invisible until a leader flaps.
func TestLeaderRenewMustBeShorterThanLease(t *testing.T) {
	f := loadFile(t, filepath.Join(configsDir, "sbs-service.yaml"))
	f.SBSService.Leader.RenewIntervalSeconds = f.SBSService.Leader.LeaseDurationSeconds
	res := Validate(f)
	if res.OK() {
		t.Fatal("a renew interval equal to the lease duration was accepted")
	}
}

// The current supported MCP surface is read-only.
func TestLargeScaleMCPRefusesOperatePosture(t *testing.T) {
	f := loadFile(t, filepath.Join(configsDir, "namrbd-mcp.yaml"))
	f.MCP.Mode = MCPModeOperate
	res := Validate(f)
	if res.OK() {
		t.Fatal("large_scale profile accepted the MCP operate posture")
	}
}

// An unknown key must fail rather than be ignored.
func TestUnknownFieldIsRejected(t *testing.T) {
	var f File
	dec := yaml.NewDecoder(strings.NewReader(
		"schema_version: 1\nprofile: dev\nprocess: sbs-data\nsbs_data:\n  data_path: /x\n  grpc_listen: :1\n  typo_field: 1\n"))
	dec.KnownFields(true)
	if err := dec.Decode(&f); err == nil {
		t.Fatal("an unknown config key was silently ignored")
	}
}

func TestSchemaVersionMismatchIsRejected(t *testing.T) {
	f := loadFile(t, filepath.Join(configsDir, "sbs-data.yaml"))
	f.SchemaVersion = SchemaVersion + 1
	if Validate(f).OK() {
		t.Fatal("a future schema_version was accepted")
	}
}

// The write-effects batch decides how many keys a commit read asks for: two per
// item, a volume state key and an idempotency key. A batch larger than the
// BatchGet bound allows means the two budgets were set against each other, and
// every commit would be split into chunks by a limit the operator did not know
// they had crossed.
func TestWriteEffectsBatchMustFitTheBatchGetBound(t *testing.T) {
	f := loadFile(t, filepath.Join(configsDir, "sbs-service.yaml"))
	// The shipped example sits exactly at the boundary, which must be allowed.
	if res := Validate(f); !res.OK() {
		t.Fatalf("the shipped example does not validate: %v", res.Errors)
	}
	if f.SBSService.WriteEffects.BatchMax*2 != f.SBSService.TiKV.BatchGetSize {
		t.Logf("example batch_max=%d batch_get_size=%d", f.SBSService.WriteEffects.BatchMax, f.SBSService.TiKV.BatchGetSize)
	}

	f.SBSService.WriteEffects.BatchMax = f.SBSService.TiKV.BatchGetSize/2 + 1
	res := Validate(f)
	if res.OK() {
		t.Fatal("a write-effects batch that overruns the BatchGet bound was accepted")
	}
	joined := strings.Join(res.Errors, "; ")
	if !strings.Contains(joined, "keys per commit read") {
		t.Errorf("the error does not explain the key arithmetic: %v", res.Errors)
	}
	if !strings.Contains(joined, "so the two agree") {
		t.Errorf("the error does not say how to resolve it: %v", res.Errors)
	}
}
