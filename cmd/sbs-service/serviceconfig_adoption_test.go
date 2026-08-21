package main

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/nosway/namrbd/internal/serviceconfig"
)

func installedSBSConfig(t *testing.T, edit func(string) string) string {
	t.Helper()
	raw, err := os.ReadFile("../../configs/sbs-service.yaml")
	if err != nil {
		t.Fatalf("read example: %v", err)
	}
	body := string(raw)
	if edit != nil {
		body = edit(body)
	}
	dst := filepath.Join(t.TempDir(), "sbs-service.yaml")
	if err := os.WriteFile(dst, []byte(body), 0o600); err != nil {
		t.Fatalf("install: %v", err)
	}
	return dst
}

func noEnv(string) (string, bool) { return "", false }

func envMap(m map[string]string) serviceconfig.EnvLookup {
	return func(k string) (string, bool) { v, ok := m[k]; return v, ok }
}

type sbsProbe struct {
	clusterID, nodeID, metadataBackend, grpcListen, pdEndpoints, keyspace string
	timeout, lease, renew, healthInterval, healthTimeout, healthCooldown  time.Duration
	trace                                                                 bool
	batchMax, healthShards, healthConcurrency, healthSuspect, healthDown  int
}

func (p *sbsProbe) binding() sbsServiceConfigBinding {
	return sbsServiceConfigBinding{
		ClusterID: &p.clusterID, NodeID: &p.nodeID, MetadataBackend: &p.metadataBackend,
		GRPCListen:      &p.grpcListen,
		TiKVPDEndpoints: &p.pdEndpoints, TiKVKeyspace: &p.keyspace,
		TiKVTimeout: &p.timeout, TiKVOperationTrace: &p.trace,
		LeaderLeaseDuration: &p.lease, LeaderRenewInterval: &p.renew,
		WriteEffectsBatchMax: &p.batchMax,
		HealthShardCount:     &p.healthShards, HealthConcurrency: &p.healthConcurrency,
		HealthInterval: &p.healthInterval, HealthTimeout: &p.healthTimeout,
		HealthSuspectAfter: &p.healthSuspect, HealthDownAfter: &p.healthDown,
		HealthRecoveryCooldown: &p.healthCooldown,
	}
}

// A config file supplies settings nothing else provided.
func TestConfigSuppliesSettings(t *testing.T) {
	var p sbsProbe
	if _, err := applySBSServiceConfig(installedSBSConfig(t, nil), p.binding(), map[string]string{}, noEnv); err != nil {
		t.Fatalf("apply: %v", err)
	}
	if p.clusterID != "namrbd-prod" || p.nodeID != "sbs-svc-01" {
		t.Errorf("identity not applied: %+v", p)
	}
	if p.metadataBackend != "tikv" {
		t.Errorf("metadata backend = %q", p.metadataBackend)
	}
	if p.grpcListen != "0.0.0.0:9090" {
		t.Errorf("grpc_listen = %q", p.grpcListen)
	}
	if !strings.Contains(p.pdEndpoints, "pd-01.namrbd.internal:2379") {
		t.Errorf("pd_endpoints = %q", p.pdEndpoints)
	}
	if p.timeout != 5*time.Second || p.lease != 15*time.Second || p.renew != 5*time.Second {
		t.Errorf("durations not applied: %+v", p)
	}
	if p.healthShards != 4 || p.healthConcurrency != 16 || p.healthInterval != 10*time.Second || p.healthTimeout != 2*time.Second || p.healthSuspect != 3 || p.healthDown != 6 || p.healthCooldown != 30*time.Second {
		t.Errorf("health config not applied: %+v", p)
	}
}

func TestLargeScaleRequiresTiKVMetadataBackend(t *testing.T) {
	path := installedSBSConfig(t, func(body string) string {
		return strings.Replace(body, "metadata_backend: tikv", "metadata_backend: pebble", 1)
	})
	var p sbsProbe
	if _, err := applySBSServiceConfig(path, p.binding(), map[string]string{}, noEnv); err == nil ||
		!strings.Contains(err.Error(), "metadata_backend must be tikv") {
		t.Fatalf("large_scale config accepted Pebble metadata authority: %v", err)
	}
}

// sbs-service reads these variables when it builds flag defaults, not as an
// override afterwards. Left alone a config file would outrank an environment
// variable that was already set, which inverts the documented order.
func TestEnvironmentOutranksConfigFile(t *testing.T) {
	var p sbsProbe
	p.nodeID = "from-env"
	summary, err := applySBSServiceConfig(installedSBSConfig(t, nil), p.binding(), map[string]string{},
		envMap(map[string]string{"NAMRBD_SBS_SERVICE_NODE_ID": "from-env"}))
	if err != nil {
		t.Fatalf("apply: %v", err)
	}
	if p.nodeID != "from-env" {
		t.Errorf("node_id = %q; the config file overrode an environment variable", p.nodeID)
	}
	if p.clusterID != "namrbd-prod" {
		t.Errorf("cluster_id = %q; an unset variable blocked the config value", p.clusterID)
	}
	// The deferral must be visible, or an operator cannot tell why the config
	// value did not take effect.
	found := false
	for _, o := range summary.Overrides {
		if o.Field == "sbs_service.node_id" && o.Source == serviceconfig.SourceEnv {
			found = true
		}
	}
	if !found {
		t.Errorf("the environment override was not recorded: %+v", summary.Overrides)
	}
	if summary.EnvOverrideCount != 1 {
		t.Errorf("env override count=%d want=1", summary.EnvOverrideCount)
	}
}

// Every env-backed flag must actually defer, not just node_id.
func TestEveryEnvBackedFlagDefers(t *testing.T) {
	for flagName, envName := range envBackedFlags {
		if !strings.HasPrefix(envName, "NAMRBD_") {
			t.Errorf("flag --%s maps to %s, which is not a NAMRBD_ variable", flagName, envName)
		}
	}
	for flagName, want := range map[string]string{
		"node-id":                 "NAMRBD_SBS_SERVICE_NODE_ID",
		"sbs-service-listen":      "NAMRBD_SBS_SERVICE_GRPC_LISTEN",
		"sbs-service-http-listen": "NAMRBD_SBS_SERVICE_HTTP_LISTEN",
	} {
		if got := envBackedFlags[flagName]; got != want {
			t.Errorf("--%s env=%q want=%q", flagName, got, want)
		}
	}
	// Spot-check the ones this binding covers end to end.
	for _, tc := range []struct{ envName, want string }{
		{"NAMRBD_CLUSTER_ID", "env-cluster"},
		{"NAMRBD_SBS_ADMIN_ADDR", "127.0.0.1:1"},
		{"NAMRBD_TIKV_KEYSPACE", "env-keyspace"},
	} {
		var p sbsProbe
		switch tc.envName {
		case "NAMRBD_CLUSTER_ID":
			p.clusterID = tc.want
		case "NAMRBD_SBS_ADMIN_ADDR":
			p.grpcListen = tc.want
		case "NAMRBD_TIKV_KEYSPACE":
			p.keyspace = tc.want
		}
		if _, err := applySBSServiceConfig(installedSBSConfig(t, nil), p.binding(), map[string]string{},
			envMap(map[string]string{tc.envName: tc.want})); err != nil {
			t.Fatalf("apply: %v", err)
		}
		got := map[string]string{
			"NAMRBD_CLUSTER_ID":     p.clusterID,
			"NAMRBD_SBS_ADMIN_ADDR": p.grpcListen,
			"NAMRBD_TIKV_KEYSPACE":  p.keyspace,
		}[tc.envName]
		if got != tc.want {
			t.Errorf("%s did not outrank the config file: got %q", tc.envName, got)
		}
	}
}

// An explicitly typed flag outranks both.
func TestExplicitFlagOutranksEnvAndConfig(t *testing.T) {
	var p sbsProbe
	p.nodeID = "typed"
	if _, err := applySBSServiceConfig(installedSBSConfig(t, nil), p.binding(),
		map[string]string{"node-id": "typed"},
		envMap(map[string]string{"NAMRBD_SBS_SERVICE_NODE_ID": "from-env"})); err != nil {
		t.Fatalf("apply: %v", err)
	}
	if p.nodeID != "typed" {
		t.Errorf("node_id = %q, want the typed value", p.nodeID)
	}
}

// A variable naming key material is recorded but not echoed.
func TestEnvOverrideValueIsRedactedForKeyMaterial(t *testing.T) {
	if redactEnvValue("NAMRBD_KEY_FILE", "/etc/x.key") != serviceconfig.RedactedMarker {
		t.Error("a KEY variable was not redacted")
	}
	if redactEnvValue("NAMRBD_CLUSTER_ID", "namrbd-prod") != "namrbd-prod" {
		t.Error("a non-secret variable was redacted")
	}
	if redactEnvValue("NAMRBD_CLUSTER_ID", strings.Repeat("A", 44)) != serviceconfig.RedactedMarker {
		t.Error("a secret-looking value was not redacted")
	}
}

// The gate: settings that must be identical across every service in a cluster
// cannot be supplied per instance.
func TestClusterWideFlagsRejectedAtScale(t *testing.T) {
	for name := range sbsLabFlagsRejectedAtScale {
		msgs := rejectSBSFlags(map[string]string{name: "x"})
		if len(msgs) != 1 {
			t.Errorf("--%s was not rejected", name)
		}
	}
	if len(rejectSBSFlags(map[string]string{"node-id": "n"})) != 0 {
		t.Error("a per-instance flag was rejected")
	}
}

func TestNamedClusterWideGateFlags(t *testing.T) {
	for _, name := range []string{"tikv-operation-trace", "write-effects-batch-max",
		"write-effects-lane-bucket-count", "async-write-mutation-finalize"} {
		if _, ok := sbsLabFlagsRejectedAtScale[name]; !ok {
			t.Errorf("--%s is missing from the large_scale rejection set", name)
		}
	}
}

// The gate must fire through the real load path, not only in its helper.
func TestClusterWideGateFiresThroughLoadPath(t *testing.T) {
	var p sbsProbe
	_, err := applySBSServiceConfig(installedSBSConfig(t, nil), p.binding(),
		map[string]string{"write-effects-batch-max": "9999"}, noEnv)
	if err == nil {
		t.Fatal("a large_scale config accepted --write-effects-batch-max")
	}
	if !strings.Contains(err.Error(), "identical across services") {
		t.Errorf("wrong failure: %v", err)
	}
}

func TestDevProfileAllowsClusterWideFlags(t *testing.T) {
	var p sbsProbe
	path := installedSBSConfig(t, func(b string) string {
		return strings.Replace(b, "profile: large_scale", "profile: dev", 1)
	})
	if _, err := applySBSServiceConfig(path, p.binding(),
		map[string]string{"write-effects-batch-max": "16"}, noEnv); err != nil {
		t.Fatalf("the dev profile rejected a lab flag: %v", err)
	}
}

// Every schema field must be bound or declared unbindable with a reason.
func TestEverySBSServiceConfigFieldIsAccountedFor(t *testing.T) {
	unbound := map[string]string{
		"sbs_service.dependency":           "the availability thresholds have no flag by design; they are fleet-wide policy, not per-host, and applySBSServiceConfig installs them into the dependency tracker",
		"sbs_service.tikv.scan_page_size":  "the scan budget is enforced by config validation and consumed by AA-IMPL-003; this binary has no flag for it",
		"sbs_service.tikv.batch_get_size":  "the batch budget is enforced by config validation and consumed by AA-IMPL-003; this binary has no flag for it",
		"sbs_service.tikv.tls.key.env":     "the process takes a key file path; other sources are not applied",
		"sbs_service.tikv.tls.key.kms":     "the process takes a key file path; other sources are not applied",
		"sbs_service.tikv.tls.server_name": "the TiKV client takes no server-name flag",
		"sbs_service.observability":        "sbs-service serves observability on its HTTP listener and has no trace or debug toggle flag",
	}
	bound := map[string]bool{}
	for _, f := range []string{
		"sbs_service.cluster_id", "sbs_service.sbs_cluster_id", "sbs_service.node_id", "sbs_service.metadata_backend",
		"sbs_service.grpc_listen", "sbs_service.http_listen", "sbs_service.payload_root",
		"sbs_service.tikv.pd_endpoints", "sbs_service.tikv.keyspace", "sbs_service.tikv.api_version",
		"sbs_service.tikv.timeout_seconds", "sbs_service.tikv.operation_trace",
		"sbs_service.tikv.tls.enable", "sbs_service.tikv.tls.cert_file", "sbs_service.tikv.tls.key.file",
		"sbs_service.leader.lease_duration_seconds", "sbs_service.leader.renew_interval_seconds",
		"sbs_service.health",
		"sbs_service.write_effects.service_owned", "sbs_service.write_effects.native_allocation_fast_path",
		"sbs_service.write_effects.batch_max", "sbs_service.write_effects.lane_bucket_count",
		"sbs_service.write_effects.async_mutation_finalize",
	} {
		bound[f] = true
	}
	covered := func(path string) bool {
		for f := range bound {
			if path == f || strings.HasPrefix(path, f+".") {
				return true
			}
		}
		for f := range unbound {
			if path == f || strings.HasPrefix(path, f+".") {
				return true
			}
		}
		return false
	}
	for _, path := range yamlFieldPaths(reflect.TypeOf(serviceconfig.SBSServiceConfig{}), "sbs_service") {
		if !covered(path) {
			t.Errorf("config field %s is neither bound nor declared unbindable; "+
				"an operator would set it and sbs-service would ignore it", path)
		}
	}
}

func yamlFieldPaths(t reflect.Type, prefix string) []string {
	var out []string
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		tag := strings.Split(f.Tag.Get("yaml"), ",")[0]
		if tag == "" || tag == "-" {
			continue
		}
		path := prefix + "." + tag
		ft := f.Type
		for ft.Kind() == reflect.Ptr {
			ft = ft.Elem()
		}
		if ft.Kind() == reflect.Struct {
			out = append(out, yamlFieldPaths(ft, path)...)
			continue
		}
		out = append(out, path)
	}
	return out
}

func TestConfigForAnotherProcessIsRejected(t *testing.T) {
	raw, err := os.ReadFile("../../configs/sbs-data.yaml")
	if err != nil {
		t.Fatal(err)
	}
	p := filepath.Join(t.TempDir(), "sbs-data.yaml")
	if err := os.WriteFile(p, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := applySBSServiceConfig(p, sbsServiceConfigBinding{}, map[string]string{}, noEnv); err == nil {
		t.Fatal("an sbs-data config started sbs-service")
	}
}
