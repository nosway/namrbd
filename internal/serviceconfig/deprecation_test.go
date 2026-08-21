package serviceconfig

import (
	"reflect"
	"strings"
	"testing"
)

// A deprecation record exists so a flag is never removed silently, which means
// each one has to be actionable on its own.
func TestEveryDeprecationIsActionable(t *testing.T) {
	all := AllDeprecations()
	if len(all) == 0 {
		t.Fatal("no deprecation records")
	}
	seen := map[string]bool{}
	for _, d := range all {
		key := d.Process + "/" + d.Flag
		if seen[key] {
			t.Errorf("duplicate deprecation record for %s", key)
		}
		seen[key] = true

		if !isKnownProcess(d.Process) {
			t.Errorf("%s names an unknown process", key)
		}
		if strings.TrimSpace(d.Flag) == "" {
			t.Errorf("%s has no flag name", key)
		}
		if strings.TrimSpace(d.DeprecatedIn) == "" {
			t.Errorf("%s does not say when it was deprecated", key)
		}
		// Either it says what replaces it, or it explains why nothing does.
		if d.ConfigKey == "" && strings.TrimSpace(d.Note) == "" {
			t.Errorf("%s has no replacement and no explanation; an operator cannot act on it", key)
		}
	}
}

// A named config key must exist in the schema, or the migration points an
// operator at a field that is not there.
func TestDeprecationConfigKeysExist(t *testing.T) {
	blocks := map[string]struct {
		typ    reflect.Type
		prefix string
	}{
		ProcessGateway:      {reflect.TypeOf(GatewayConfig{}), "gateway"},
		ProcessISCSIGateway: {reflect.TypeOf(ISCSIGatewayConfig{}), "iscsi_gateway"},
		ProcessSBSService:   {reflect.TypeOf(SBSServiceConfig{}), "sbs_service"},
		ProcessSBSData:      {reflect.TypeOf(SBSDataConfig{}), "sbs_data"},
		ProcessCSIDriver:    {reflect.TypeOf(CSIDriverConfig{}), "csi_driver"},
		ProcessMCP:          {reflect.TypeOf(MCPConfig{}), "mcp"},
	}
	known := map[string]map[string]bool{}
	for process, b := range blocks {
		known[process] = map[string]bool{}
		for _, p := range FieldPaths(b.typ, b.prefix) {
			known[process][p] = true
			// Record parents too, so a key naming a whole block resolves.
			for i := len(p) - 1; i >= 0; i-- {
				if p[i] == '.' {
					known[process][p[:i]] = true
				}
			}
		}
	}
	for _, d := range AllDeprecations() {
		if d.ConfigKey == "" {
			continue
		}
		if !known[d.Process][d.ConfigKey] {
			t.Errorf("%s/--%s points at config key %q, which does not exist in the schema",
				d.Process, d.Flag, d.ConfigKey)
		}
	}
}

// Nothing is removed in the release that only announces the deprecation. A flag
// that stops working in the same release an operator first hears about it is
// not a deprecation, it is a breaking change.
func TestNothingIsRemovedInTheAnnouncingRelease(t *testing.T) {
	for _, d := range AllDeprecations() {
		if d.RemovedIn != "" && d.RemovedIn == d.DeprecatedIn {
			t.Errorf("%s/--%s is removed in the same release it is deprecated in", d.Process, d.Flag)
		}
	}
}

// The flags the strict profile refuses must be recorded as having no config
// replacement, so an operator does not go looking for a key that is not there.
func TestProfileRejectedFlagsHaveNoConfigReplacement(t *testing.T) {
	noReplacement := map[string][]string{
		ProcessGateway:      {"volumes", "sbs-cluster-replicas"},
		ProcessISCSIGateway: {"target-iqn", "lun-id", "export-id", "volume-id"},
	}
	for process, flags := range noReplacement {
		for _, flag := range flags {
			d, ok := DeprecationFor(process, flag)
			if !ok {
				t.Errorf("%s/--%s has no deprecation record", process, flag)
				continue
			}
			if d.ConfigKey != "" {
				t.Errorf("%s/--%s claims a config key, but the setting comes from a registry", process, flag)
			}
			if !d.DevProfileOnly {
				t.Errorf("%s/--%s should remain available in the dev profile for fixtures", process, flag)
			}
		}
	}
}

// Every process that accepts --config must have deprecation records, or its
// migration has nothing to tell an operator.
func TestEveryProcessHasDeprecations(t *testing.T) {
	for _, p := range sortedProcesses() {
		if len(DeprecationsFor(p)) == 0 {
			t.Errorf("%s has no deprecation records", p)
		}
	}
}

// A generated config must never contain secret material, whatever it was built
// from. This is the last check before the bytes leave the process.
func TestGenerateRefusesToEmitSecretMaterial(t *testing.T) {
	f := &File{
		SchemaVersion: SchemaVersion, Revision: 1, Profile: ProfileDev, Process: ProcessSBSData,
		SBSData: &SBSDataConfig{
			DataPath:   "/var/lib/namrbd",
			GRPCListen: "0.0.0.0:9091",
			// A caller that pasted material into a plain field.
			StoreConfigPath: "-----BEGIN RSA PRIVATE KEY-----",
		},
	}
	if _, err := Generate(f, nil, nil); err == nil {
		t.Fatal("a config carrying key material was generated")
	}
}

func TestSecretRefForPathVersusLiteral(t *testing.T) {
	var report []string
	if got := SecretRefFor("x.key", "/etc/k.pem", true, &report); got.File != "/etc/k.pem" {
		t.Errorf("a path became %v", got)
	}
	if len(report) != 0 {
		t.Errorf("a path was reported as needing replacement: %v", report)
	}
	got := SecretRefFor("x.key", "LITERALMATERIAL", false, &report)
	if got.File != SecretPlaceholder {
		t.Errorf("literal material became %v", got)
	}
	if len(report) != 1 || report[0] != "x.key" {
		t.Errorf("the field was not reported: %v", report)
	}
	if SecretRefFor("x.key", "", false, &report) != (SecretRef{}) {
		t.Error("an unset flag produced a reference")
	}
}
