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

func installedCSIConfig(t *testing.T, edit func(string) string) string {
	t.Helper()
	raw, err := os.ReadFile("../../configs/namrbd-csi-driver.yaml")
	if err != nil {
		t.Fatalf("read example: %v", err)
	}
	body := string(raw)
	if edit != nil {
		body = edit(body)
	}
	dst := filepath.Join(t.TempDir(), "namrbd-csi-driver.yaml")
	if err := os.WriteFile(dst, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return dst
}

func TestCSIConfigSuppliesSettings(t *testing.T) {
	var driver, node, endpoint, admin, admins, cluster, sbs, gw string
	b := csiConfigBinding{DriverName: &driver, NodeID: &node, Endpoint: &endpoint,
		AdminEndpoint: &admin, AdminEndpoints: &admins, ClusterID: &cluster,
		SBSClusterID: &sbs, GatewayURL: &gw}
	if _, err := applyCSIConfig(installedCSIConfig(t, nil), b, map[string]string{}, noEnvLookup); err != nil {
		t.Fatalf("apply: %v", err)
	}
	if driver != "namrbd.csi.nosway.io" || cluster != "namrbd-prod" {
		t.Errorf("identity not applied: driver=%q cluster=%q", driver, cluster)
	}
	// The first entry is the primary the client dials; the list is what it
	// fails over across.
	if !strings.HasPrefix(admin, "sbs-service-01") {
		t.Errorf("admin_endpoint = %q, want the first list entry", admin)
	}
	if !strings.Contains(admins, ",") {
		t.Errorf("admin_endpoints = %q, want the full list", admins)
	}
}

// A single admin endpoint makes every volume operation on the node depend on
// one service instance.
func TestSingleAdminEndpointRejectedAtScale(t *testing.T) {
	path := installedCSIConfig(t, func(b string) string {
		return strings.Replace(b, "    - sbs-service-02.namrbd.internal:9090\n", "", 1)
	})
	_, err := applyCSIConfig(path, csiConfigBinding{}, map[string]string{}, noEnvLookup)
	if err == nil {
		t.Fatal("a single admin endpoint was accepted at scale")
	}
	if !strings.Contains(err.Error(), "single point of failure") {
		t.Errorf("unhelpful error: %v", err)
	}
}

// A CSI node that cannot identify itself cannot be addressed by the controller.
func TestMissingNodeIDRejectedAtScale(t *testing.T) {
	path := installedCSIConfig(t, func(b string) string {
		return strings.Replace(b, "  node_id: worker-01\n", "", 1)
	})
	if _, err := applyCSIConfig(path, csiConfigBinding{}, map[string]string{}, noEnvLookup); err == nil {
		t.Fatal("a driver with no node identity was accepted")
	}
	// The variable this binary already reads still satisfies it.
	if _, err := applyCSIConfig(path, csiConfigBinding{}, map[string]string{},
		envLookup(map[string]string{"NAMRBD_CSI_NODE_ID": "worker-09"})); err != nil {
		t.Errorf("NAMRBD_CSI_NODE_ID did not satisfy node identity: %v", err)
	}
}

// The gate: settings that must not vary between nodes of one cluster.
func TestClusterWideCSIFlagsRejectedAtScale(t *testing.T) {
	for name := range csiFlagsRejectedAtScale {
		if len(rejectCSIFlags(map[string]string{name: "x"})) != 1 {
			t.Errorf("--%s was not rejected", name)
		}
	}
	if len(rejectCSIFlags(map[string]string{"node-id": "worker-02"})) != 0 {
		t.Error("the per-node identity flag was rejected")
	}
	if len(rejectCSIFlags(map[string]string{"endpoint": "unix:///x"})) != 0 {
		t.Error("the per-node socket path was rejected")
	}
}

func TestClusterWideCSIGateFiresThroughLoadPath(t *testing.T) {
	_, err := applyCSIConfig(installedCSIConfig(t, nil), csiConfigBinding{},
		map[string]string{"cluster-id": "other"}, noEnvLookup)
	if err == nil {
		t.Fatal("a large_scale config accepted --cluster-id")
	}
	if !strings.Contains(err.Error(), "identical on every node") {
		t.Errorf("wrong failure: %v", err)
	}
}

func TestDevProfileAllowsClusterWideCSIFlags(t *testing.T) {
	path := installedCSIConfig(t, func(b string) string {
		return strings.Replace(b, "profile: large_scale", "profile: dev", 1)
	})
	if _, err := applyCSIConfig(path, csiConfigBinding{}, map[string]string{"cluster-id": "other"}, noEnvLookup); err != nil {
		t.Fatalf("the dev profile rejected a cluster-wide flag: %v", err)
	}
}

func TestCSIEnvironmentOutranksConfigFile(t *testing.T) {
	cluster := "env-cluster"
	b := csiConfigBinding{ClusterID: &cluster}
	summary, err := applyCSIConfig(installedCSIConfig(t, nil), b, map[string]string{},
		envLookup(map[string]string{"NAMRBD_CLUSTER_ID": "env-cluster"}))
	if err != nil {
		t.Fatalf("apply: %v", err)
	}
	if cluster != "env-cluster" {
		t.Errorf("cluster_id = %q; the config file overrode an environment variable", cluster)
	}
	if summary.EnvOverrideCount == 0 {
		t.Error("the environment override was not recorded")
	}
}

func TestCSIServiceEnvironmentNamesOverrideConfig(t *testing.T) {
	var primary, endpoints string
	b := csiConfigBinding{AdminEndpoint: &primary, AdminEndpoints: &endpoints}
	summary, err := applyCSIConfig(installedCSIConfig(t, nil), b, map[string]string{}, envLookup(map[string]string{
		"NAMRBD_SBS_SERVICE_ENDPOINT":  "svc-primary:9443",
		"NAMRBD_SBS_SERVICE_ENDPOINTS": "svc-a:9443,svc-b:9443",
	}))
	if err != nil {
		t.Fatalf("apply: %v", err)
	}
	if primary != "svc-primary:9443" || endpoints != "svc-primary:9443,svc-b:9443" {
		t.Fatalf("primary=%q endpoints=%q", primary, endpoints)
	}
	if summary.EnvOverrideCount != 2 || summary.WarningCount != 0 {
		t.Fatalf("summary=%+v", summary)
	}
}

func TestCSIEndpointListSuppliesPrimaryWhenSingularIsUnset(t *testing.T) {
	got := defaultPrimarySBSServiceEndpoint("127.0.0.1:9897", false,
		"svc-a=10.0.0.1:9443 svc-b=10.0.0.2:9443", true)
	if got != "10.0.0.1:9443" {
		t.Fatalf("primary=%q", got)
	}
	if got := defaultPrimarySBSServiceEndpoint("explicit:9443", true, "list:9443", true); got != "explicit:9443" {
		t.Fatalf("explicit primary=%q", got)
	}
}

func TestCSILegacyServiceEnvironmentNamesRemainV10Compatible(t *testing.T) {
	var primary, endpoints string
	b := csiConfigBinding{AdminEndpoint: &primary, AdminEndpoints: &endpoints}
	summary, err := applyCSIConfig(installedCSIConfig(t, nil), b, map[string]string{}, envLookup(map[string]string{
		"NAMRBD_ADMIN_ENDPOINT":  "legacy-primary:9443",
		"NAMRBD_ADMIN_ENDPOINTS": "legacy-a:9443 legacy-b:9443",
	}))
	if err != nil {
		t.Fatalf("apply: %v", err)
	}
	if primary != "legacy-primary:9443" || endpoints != "legacy-primary:9443,legacy-b:9443" {
		t.Fatalf("primary=%q endpoints=%q", primary, endpoints)
	}
	if summary.EnvOverrideCount != 2 || summary.WarningCount != 2 {
		t.Fatalf("summary=%+v", summary)
	}
}

func TestEveryCSIConfigFieldIsAccountedFor(t *testing.T) {
	unbound := map[string]string{
		"csi_driver.observability": "this driver exposes no observability listener, trace, or debug flag",
	}
	bound := map[string]bool{}
	for _, f := range []string{
		"csi_driver.driver_name", "csi_driver.node_id", "csi_driver.endpoint",
		"csi_driver.admin_endpoints", "csi_driver.cluster_id",
		"csi_driver.sbs_cluster_id", "csi_driver.gateway_url",
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
	for _, path := range yamlFieldPaths(reflect.TypeOf(serviceconfig.CSIDriverConfig{}), "csi_driver") {
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
	raw, _ := os.ReadFile("../../configs/sbs-data.yaml")
	p := filepath.Join(t.TempDir(), "sbs-data.yaml")
	if err := os.WriteFile(p, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := applyCSIConfig(p, csiConfigBinding{}, map[string]string{}, noEnvLookup); err == nil {
		t.Fatal("an sbs-data config started the CSI driver")
	}
}
