package main

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/nosway/namrbd/internal/serviceconfig"
)

func noEnvLookup(string) (string, bool) { return "", false }

func envLookup(m map[string]string) serviceconfig.EnvLookup {
	return func(k string) (string, bool) { v, ok := m[k]; return v, ok }
}

// installedSBSDataConfig writes an installable config whose store-config path
// points at a real file, since the strict profile requires it to be readable.
func installedSBSDataConfig(t *testing.T, edit func(string) string) string {
	t.Helper()
	raw, err := os.ReadFile("../../configs/sbs-data.yaml")
	if err != nil {
		t.Fatalf("read example: %v", err)
	}
	dir := t.TempDir()
	store := filepath.Join(dir, "store-config.yaml")
	if err := os.WriteFile(store, []byte("stores:\n  - id: default\n    path: "+dir+"/store\n    shards: 1\n    weight: 100\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	body := strings.Replace(string(raw), "/etc/namrbd/store-config.yaml", store, 1)
	if edit != nil {
		body = edit(body)
	}
	dst := filepath.Join(dir, "sbs-data.yaml")
	if err := os.WriteFile(dst, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return dst
}

func TestSBSDataConfigSuppliesSettings(t *testing.T) {
	var clusterID, sbsClusterID, nodeID, dataPath, grpc, http, storeCfg string
	b := sbsDataConfigBinding{ClusterID: &clusterID, SBSClusterID: &sbsClusterID, NodeID: &nodeID, DataPath: &dataPath, GRPCListen: &grpc, HTTPListen: &http, StoreConfigPath: &storeCfg}
	if _, err := applySBSDataConfig(installedSBSDataConfig(t, nil), b, map[string]string{}, noEnvLookup); err != nil {
		t.Fatalf("apply: %v", err)
	}
	if dataPath != "/var/lib/namrbd/sbs-data" {
		t.Errorf("data_path = %q", dataPath)
	}
	if clusterID != "namrbd-prod" || sbsClusterID != "sbs-prod" || nodeID != "sbs-data-01" {
		t.Errorf("identity = %q/%q/%q", clusterID, sbsClusterID, nodeID)
	}
	if grpc != "0.0.0.0:9091" {
		t.Errorf("grpc_listen = %q", grpc)
	}
	if !strings.HasSuffix(storeCfg, "store-config.yaml") {
		t.Errorf("store_config_path = %q", storeCfg)
	}
}

// Store layout stays in its own document; the service config only points at it.
func TestStoreLayoutIsNotAbsorbedIntoServiceConfig(t *testing.T) {
	for _, path := range yamlFieldPaths(reflect.TypeOf(serviceconfig.SBSDataConfig{}), "sbs_data") {
		if strings.Contains(path, "store") && path != "sbs_data.store_config_path" {
			t.Errorf("field %s pulls store layout into the service config; "+
				"layout belongs to the separate reloadable document whose reload rejects store removals", path)
		}
	}
}

// The strict profile needs a readable store layout: a node with nowhere to
// place payload should fail at startup rather than accept writes it cannot
// place.
func TestLargeScaleRequiresReadableStoreConfig(t *testing.T) {
	path := installedSBSDataConfig(t, func(b string) string {
		return strings.Replace(b, "store_config_path: ", "store_config_path: /nonexistent/", 1)
	})
	_, err := applySBSDataConfig(path, sbsDataConfigBinding{}, map[string]string{}, noEnvLookup)
	if err == nil {
		t.Fatal("an unreadable store config was accepted")
	}
	if !strings.Contains(err.Error(), "not readable") {
		t.Errorf("unhelpful error: %v", err)
	}
}

// The gate: shortcuts that change the durability or revalidation contract are
// refused, and so are the debug mutation endpoints.
func TestDurabilityShortcutsRejectedAtScale(t *testing.T) {
	for name := range sbsDataLabFlagsRejectedAtScale {
		if len(rejectSBSDataFlags(map[string]string{name: "true"})) != 1 {
			t.Errorf("--%s was not rejected", name)
		}
	}
	if len(rejectSBSDataFlags(map[string]string{"grpc-listen": ":1"})) != 0 {
		t.Error("a listener flag was rejected")
	}
}

func TestNamedDurabilityGateFlags(t *testing.T) {
	for _, name := range []string{
		"enable-lab-store-debug", "lab-disable-idempotency-sync",
		"lab-cache-open-volume-spec", "lab-disable-physical-write-idempotency",
	} {
		if _, ok := sbsDataLabFlagsRejectedAtScale[name]; !ok {
			t.Errorf("--%s is missing from the large_scale rejection set", name)
		}
	}
}

func TestDurabilityGateFiresThroughLoadPath(t *testing.T) {
	_, err := applySBSDataConfig(installedSBSDataConfig(t, nil), sbsDataConfigBinding{},
		map[string]string{"lab-disable-physical-write-idempotency": "true"}, noEnvLookup)
	if err == nil {
		t.Fatal("a large_scale config accepted a durability shortcut")
	}
	if !strings.Contains(err.Error(), "trades correctness for speed") {
		t.Errorf("wrong failure: %v", err)
	}
}

func TestDevProfileAllowsLabFlags(t *testing.T) {
	path := installedSBSDataConfig(t, func(b string) string {
		return strings.Replace(b, "profile: large_scale", "profile: dev", 1)
	})
	if _, err := applySBSDataConfig(path, sbsDataConfigBinding{},
		map[string]string{"lab-disable-idempotency-sync": "true"}, noEnvLookup); err != nil {
		t.Fatalf("the dev profile rejected a lab flag: %v", err)
	}
}

// Environment variables build this binary's flag defaults, so they must keep
// outranking the config file.
func TestEnvironmentOutranksConfigFile(t *testing.T) {
	grpc := "127.0.0.1:1"
	b := sbsDataConfigBinding{GRPCListen: &grpc}
	summary, err := applySBSDataConfig(installedSBSDataConfig(t, nil), b, map[string]string{},
		envLookup(map[string]string{"NAMRBD_SBS_DATA_GRPC_LISTEN": "127.0.0.1:1"}))
	if err != nil {
		t.Fatalf("apply: %v", err)
	}
	if grpc != "127.0.0.1:1" {
		t.Errorf("grpc_listen = %q; the config file overrode an environment variable", grpc)
	}
	if summary.EnvOverrideCount == 0 {
		t.Error("the environment override was not recorded")
	}
}

func TestSBSDataEnvBackedFlagsUseCanonicalNames(t *testing.T) {
	want := map[string]string{
		"path":                 "NAMRBD_SBS_DATA_PATH",
		"sbs-data-listen":      "NAMRBD_SBS_DATA_GRPC_LISTEN",
		"sbs-data-http-listen": "NAMRBD_SBS_DATA_HTTP_LISTEN",
	}
	for flagName, envName := range want {
		if got := envBackedFlags[flagName]; got != envName {
			t.Errorf("--%s env=%q want=%q", flagName, got, envName)
		}
	}
}

// A reload must identify which configuration the node ended up on. Two nodes
// reporting the same store list can still be running different files.
func TestStoreConfigRevisionAdvancesAndIdentifiesContent(t *testing.T) {
	dir := t.TempDir()
	a := filepath.Join(dir, "a.yaml")
	if err := os.WriteFile(a, []byte("stores:\n  - id: default\n    weight: 100\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	var rev storeConfigRevision
	if n, d := rev.current(); n != 0 || d != "" {
		t.Fatalf("fresh tracker reports %d %q", n, d)
	}
	n1, d1 := rev.observe(a)
	if n1 != 1 || d1 == "" {
		t.Fatalf("first reload reported %d %q", n1, d1)
	}
	n2, d2 := rev.observe(a)
	if n2 != 2 {
		t.Errorf("second reload reported revision %d", n2)
	}
	if d2 != d1 {
		t.Errorf("unchanged content produced a different digest")
	}
	if err := os.WriteFile(a, []byte("stores:\n  - id: default\n    weight: 50\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	_, d3 := rev.observe(a)
	if d3 == d1 {
		t.Error("changed content produced the same digest, so a partial rollout would be invisible")
	}
}

// A digest that cannot be computed is empty rather than a guess: a wrong digest
// would make two different configurations look identical.
func TestUnreadableStoreConfigYieldsEmptyDigest(t *testing.T) {
	var rev storeConfigRevision
	n, d := rev.observe(filepath.Join(t.TempDir(), "absent.yaml"))
	if n != 1 {
		t.Errorf("revision did not advance: %d", n)
	}
	if d != "" {
		t.Errorf("digest = %q, want empty", d)
	}
}

func TestEverySBSDataConfigFieldIsAccountedFor(t *testing.T) {
	unbound := map[string]string{}
	bound := map[string]bool{}
	for _, f := range []string{
		"sbs_data.cluster_id", "sbs_data.sbs_cluster_id", "sbs_data.node_id",
		"sbs_data.data_path", "sbs_data.store_config_path",
		"sbs_data.grpc_listen", "sbs_data.http_listen",
		"sbs_data.observability.listen", "sbs_data.observability.trace",
		"sbs_data.observability.debug_endpoints",
	} {
		bound[f] = true
	}
	// observability.listen maps onto the existing HTTP listener rather than a
	// separate one.
	unbound["sbs_data.observability.listen"] = "sbs-data serves observability on its HTTP listener"
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
	for _, path := range yamlFieldPaths(reflect.TypeOf(serviceconfig.SBSDataConfig{}), "sbs_data") {
		if !covered(path) {
			t.Errorf("config field %s is neither bound nor declared unbindable", path)
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
	raw, err := os.ReadFile("../../configs/sbs-service.yaml")
	if err != nil {
		t.Fatal(err)
	}
	p := filepath.Join(t.TempDir(), "sbs-service.yaml")
	if err := os.WriteFile(p, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := applySBSDataConfig(p, sbsDataConfigBinding{}, map[string]string{}, noEnvLookup); err == nil {
		t.Fatal("an sbs-service config started sbs-data")
	}
}
