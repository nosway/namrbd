package main

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/nosway/namrbd/internal/serviceconfig"
)

func installedISCSIConfig(t *testing.T, edit func(string) string) string {
	t.Helper()
	raw, err := os.ReadFile("../../configs/namrbd-iscsi-gateway.yaml")
	if err != nil {
		t.Fatalf("read example: %v", err)
	}
	body := string(raw)
	if edit != nil {
		body = edit(body)
	}
	dst := filepath.Join(t.TempDir(), "namrbd-iscsi-gateway.yaml")
	if err := os.WriteFile(dst, []byte(body), 0o600); err != nil {
		t.Fatalf("install: %v", err)
	}
	return dst
}

func devConfig(body string) string {
	return strings.Replace(body, "profile: large_scale", "profile: dev", 1)
}

// The gate: every serving-map and failover flag is refused in the strict
// profile, because each names state the sbs-service iSCSI registry owns.
func TestServingMapFlagsRejectedAtScale(t *testing.T) {
	for name := range servingMapFlagsRejectedAtScale {
		t.Run(name, func(t *testing.T) {
			msgs := rejectISCSIFlags(map[string]string{name: "x"})
			if len(msgs) != 1 {
				t.Fatalf("--%s was not rejected", name)
			}
			if !strings.Contains(msgs[0], "registry") && !strings.Contains(msgs[0], "runtime state") {
				t.Errorf("rejection does not say where the value belongs: %s", msgs[0])
			}
		})
	}
	for name := range labFlagsRejectedAtScale {
		if len(rejectISCSIFlags(map[string]string{name: "x"})) != 1 {
			t.Errorf("fixture flag --%s was not rejected", name)
		}
	}
	if len(rejectISCSIFlags(map[string]string{"iscsi-gateway-id": "gw"})) != 0 {
		t.Error("an instance-identity flag was rejected")
	}
}

// The named gate flags must stay covered whatever else the list grows to hold.
func TestNamedServingMapGateFlags(t *testing.T) {
	for _, name := range []string{"target-iqn", "lun-id", "export-id", "volume-id", "export-epoch"} {
		if _, ok := servingMapFlagsRejectedAtScale[name]; !ok {
			t.Errorf("--%s is missing from the serving-map rejection set", name)
		}
	}
}

// The gate must fire through the real load path, not only in its helper.
func TestServingMapGateFiresThroughLoadPath(t *testing.T) {
	path := installedISCSIConfig(t, nil)
	_, err := applyISCSIServiceConfig(path, iscsiConfigBinding{}, map[string]string{"lun-id": "3"})
	if err == nil {
		t.Fatal("a large_scale config accepted --lun-id")
	}
	if !strings.Contains(err.Error(), "--lun-id is not supported") {
		t.Errorf("wrong failure: %v", err)
	}
}

// With no offending flags the strict profile starts from registry authority;
// receiver fencing is now enforced by sbs-data (AA-IMPL-012).
func TestLargeScaleEnablesRegistryServingAfterReceiverFenceGate(t *testing.T) {
	path := installedISCSIConfig(t, nil)
	large := false
	registryRequired := false
	summary, err := applyISCSIServiceConfig(path, iscsiConfigBinding{
		LargeScale: &large, RegistryRequired: &registryRequired,
	}, map[string]string{})
	if err != nil {
		t.Fatalf("large_scale config: %v", err)
	}
	if !large || !registryRequired {
		t.Fatalf("large_scale=%v registry_required=%v", large, registryRequired)
	}
	if summary.ErrorCount != 0 || summary.FirstError != "" {
		t.Errorf("summary recorded an unexpected failure: %+v", summary)
	}
	if summary.ConfigSourceAuthority != serviceconfig.SourceFile {
		t.Errorf("source authority = %q", summary.ConfigSourceAuthority)
	}
}

func TestCanonicalISCSIEnvironmentEndpointsOverrideConfig(t *testing.T) {
	t.Setenv("NAMRBD_ISCSI_SBS_DATA_ENDPOINT", "sbs-data-env:9444")
	t.Setenv("NAMRBD_ISCSI_SBS_SERVICE_ENDPOINT", "sbs-service-env:9443")
	var dataEndpoint, serviceEndpoint string
	summary, err := applyISCSIServiceConfig(installedISCSIConfig(t, nil), iscsiConfigBinding{
		SBSEndpoint: &dataEndpoint, SBSAdminEndpoint: &serviceEndpoint,
	}, map[string]string{})
	if err != nil {
		t.Fatalf("apply: %v", err)
	}
	if dataEndpoint != "sbs-data-env:9444" || serviceEndpoint != "sbs-service-env:9443" {
		t.Fatalf("endpoints=%q/%q", dataEndpoint, serviceEndpoint)
	}
	if summary.EnvOverrideCount != 2 || summary.WarningCount != 0 {
		t.Fatalf("summary=%+v", summary)
	}
}

// The dev profile keeps working, which is what fixtures and single-export
// deployments use today.
func TestDevProfileAppliesAndServes(t *testing.T) {
	path := installedISCSIConfig(t, devConfig)
	var (
		id            string
		portal        string
		adminEP       string
		authMode      string
		etcdRoot      string
		etcdEndpoints []string
		required      bool
		reloadMode    string
		reloadPoll    int
		maxExports    int
	)
	b := iscsiConfigBinding{
		GatewayID: &id, Portal: &portal, SBSAdminEndpoint: &adminEP,
		EtcdRoot: &etcdRoot, EtcdEndpoints: &etcdEndpoints,
		AuthMode: &authMode, RegistryRequired: &required,
		ReloadMode: &reloadMode, ReloadPollInterval: &reloadPoll, MaxExportsPerProcess: &maxExports,
	}
	if _, err := applyISCSIServiceConfig(path, b, map[string]string{}); err != nil {
		t.Fatalf("dev profile failed: %v", err)
	}
	if id != "iscsi-gw-01" {
		t.Errorf("gateway_id = %q", id)
	}
	if portal != "10.20.0.11:3260" {
		t.Errorf("portal = %q", portal)
	}
	if adminEP == "" {
		t.Error("sbs_admin_endpoint was not applied")
	}
	if authMode != "chap" {
		t.Errorf("auth mode = %q", authMode)
	}
	if len(etcdEndpoints) != 3 || etcdRoot != "/namrbd/prod/iscsi-gateways" {
		t.Errorf("etcd fleet config = %v root=%q", etcdEndpoints, etcdRoot)
	}
	if reloadMode != serviceconfig.ReloadModeWatch || maxExports != 64 {
		t.Errorf("reload config mode=%q poll=%d max=%d", reloadMode, reloadPoll, maxExports)
	}
}

// A fixture flag is fine in dev.
func TestDevProfileAllowsFixtureFlags(t *testing.T) {
	path := installedISCSIConfig(t, devConfig)
	_, err := applyISCSIServiceConfig(path, iscsiConfigBinding{}, map[string]string{"sbs-fixture": "true"})
	if err != nil && strings.Contains(err.Error(), "not supported in the large_scale") {
		t.Fatalf("the dev profile rejected a fixture flag: %v", err)
	}
}

// The registry must be mandatory at scale: falling back to flags is the
// behavior this slice removes.
func TestLargeScaleForcesRegistryRequired(t *testing.T) {
	path := installedISCSIConfig(t, nil)
	res, err := serviceconfig.Load(path, nil, func(string) (string, bool) { return "", false }, nil)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	required := false
	if err := applyISCSIGatewayConfig(res.File.ISCSIGetway, true, iscsiConfigBinding{RegistryRequired: &required}); err != nil {
		t.Fatalf("apply: %v", err)
	}
	if !required {
		t.Error("the large_scale profile did not force registry-required")
	}
}

// gotgt still binds one portal, but the fleet identity must publish every
// reachable portal for discovery and failover planning.
func TestMultiplePortalsArePublishedToFleet(t *testing.T) {
	path := installedISCSIConfig(t, func(body string) string {
		body = devConfig(body)
		return strings.Replace(body, "    - 10.20.0.11:3260\n",
			"    - 10.20.0.11:3260\n    - 10.20.0.12:3260\n", 1)
	})
	res, err := serviceconfig.Load(path, nil, func(string) (string, bool) { return "", false }, nil)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	var portal string
	var advertised []string
	err = applyISCSIGatewayConfig(res.File.ISCSIGetway, false, iscsiConfigBinding{Portal: &portal, AdvertisePortals: &advertised})
	if err != nil {
		t.Fatalf("apply: %v", err)
	}
	if portal != "10.20.0.11:3260" || len(advertised) != 2 || advertised[1] != "10.20.0.12:3260" {
		t.Fatalf("bound portal=%q advertised=%v", portal, advertised)
	}
}

// Every schema field must be bound or declared unbindable with a reason.
func TestEveryISCSIConfigFieldIsAccountedFor(t *testing.T) {
	unbound := map[string]string{
		"iscsi_gateway.dependency":                    "the availability thresholds have no flag by design; they are fleet-wide policy, not per-host, and applyISCSIGatewayConfig installs them into the dependency tracker",
		"iscsi_gateway.sbs_endpoint_tls.cert_file":    "the process takes no client certificate flag today",
		"iscsi_gateway.sbs_endpoint_tls.key":          "the process takes no client key flag today",
		"iscsi_gateway.auth.chap_secret.env":          "the process takes a file reference; other sources are rejected in applyISCSIGatewayConfig",
		"iscsi_gateway.auth.chap_secret.kms":          "the process takes a file reference; other sources are rejected in applyISCSIGatewayConfig",
		"iscsi_gateway.observability.trace":           "this process has no trace toggle",
		"iscsi_gateway.observability.debug_endpoints": "this process exposes no separate debug listener",
	}
	bound := map[string]bool{}
	for _, f := range []string{
		"iscsi_gateway.gateway_id", "iscsi_gateway.advertise_portals",
		"iscsi_gateway.etcd",
		"iscsi_gateway.sbs_endpoint", "iscsi_gateway.sbs_admin_endpoint",
		"iscsi_gateway.sbs_endpoint_tls.enable", "iscsi_gateway.sbs_endpoint_tls.server_name",
		"iscsi_gateway.auth.mode", "iscsi_gateway.auth.chap_secret.file",
		"iscsi_gateway.auth.allowed_initiator_iqns",
		"iscsi_gateway.reload",
		"iscsi_gateway.observability.listen",
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
	for _, path := range yamlFieldPaths(reflect.TypeOf(serviceconfig.ISCSIGatewayConfig{}), "iscsi_gateway") {
		if !covered(path) {
			t.Errorf("config field %s is neither bound nor declared unbindable; "+
				"an operator would set it and the gateway would ignore it", path)
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
	if _, err := applyISCSIServiceConfig(p, iscsiConfigBinding{}, map[string]string{}); err == nil {
		t.Fatal("an sbs-data config started the iSCSI gateway")
	}
}
