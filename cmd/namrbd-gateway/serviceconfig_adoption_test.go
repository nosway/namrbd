package main

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/nosway/namrbd/internal/depavail"
	"github.com/nosway/namrbd/internal/serviceconfig"
)

func gatewayEnvLookup(values map[string]string) serviceconfig.EnvLookup {
	return func(key string) (string, bool) {
		value, ok := values[key]
		return value, ok
	}
}

// installedGatewayConfig copies the shipped example to a private temp file with
// the mode a deployment uses. Git checkouts land 0644, which the strict profile
// refuses.
func installedGatewayConfig(t *testing.T, edit func(string) string) string {
	t.Helper()
	raw, err := os.ReadFile("../../configs/namrbd-gateway.yaml")
	if err != nil {
		t.Fatalf("read example: %v", err)
	}
	body := string(raw)
	// The gateway serves observability on its control listener, so the example
	// must not claim a separate one.
	body = strings.Replace(body, "    listen: 127.0.0.1:9100\n", "", 1)
	if edit != nil {
		body = edit(body)
	}
	dst := filepath.Join(t.TempDir(), "namrbd-gateway.yaml")
	if err := os.WriteFile(dst, []byte(body), 0o600); err != nil {
		t.Fatalf("install: %v", err)
	}
	return dst
}

func freshBinding() (gatewayConfigBinding, func() (string, string, uint, time.Duration)) {
	var (
		listen     = ":9701"
		gatewayID  = "default-id"
		inflight   = uint(128)
		volumeTTL  = 5 * time.Second
		dataListen = ":9700"
		etcdEP     = "127.0.0.1:2379"
		etcdRoot   = ""
		adminEP    = ""
		certFile   = ""
		keyFile    = ""
		serverName = ""
		tlsEnable  = false
	)
	b := gatewayConfigBinding{
		ListenAddr: &listen, DataListenAddr: &dataListen, GatewayID: &gatewayID,
		EtcdEndpoints: &etcdEP, EtcdRoot: &etcdRoot, SBSAdminEndpoint: &adminEP,
		MaxInflightRequests: &inflight, VolumeCacheTTL: &volumeTTL,
		TLSEnable: &tlsEnable, TLSCertFile: &certFile, TLSKeyFile: &keyFile, TLSServerName: &serverName,
	}
	return b, func() (string, string, uint, time.Duration) { return listen, gatewayID, inflight, volumeTTL }
}

// A config file supplies settings the operator did not type.
func TestConfigSuppliesUntypedSettings(t *testing.T) {
	path := installedGatewayConfig(t, nil)
	res, err := serviceconfig.Load(path, nil, func(string) (string, bool) { return "", false }, nil)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	b, read := freshBinding()
	if err := applyGatewayConfig(res.File.Gateway, res.File.Profile, b, map[string]string{}); err != nil {
		t.Fatalf("apply: %v", err)
	}
	listen, id, inflight, ttl := read()
	if listen != "0.0.0.0:8080" {
		t.Errorf("listen = %q, want the config value", listen)
	}
	if id != "gw-01" {
		t.Errorf("gateway_id = %q, want the config value", id)
	}
	if inflight != 512 {
		t.Errorf("max_inflight_requests = %d, want the config value 512", inflight)
	}
	if ttl != 30*time.Second {
		t.Errorf("volume_cache_ttl = %v, want 30s", ttl)
	}
}

// An explicitly typed flag outranks the config file. Without this the config
// would silently overwrite what the operator asked for on the command line.
func TestExplicitFlagOutranksConfig(t *testing.T) {
	path := installedGatewayConfig(t, nil)
	res, _ := serviceconfig.Load(path, nil, func(string) (string, bool) { return "", false }, nil)
	b, read := freshBinding()
	*b.ListenAddr = "127.0.0.1:1234"
	cliSet := map[string]string{"control-http-listen": "127.0.0.1:1234"}
	if err := applyGatewayConfig(res.File.Gateway, res.File.Profile, b, cliSet); err != nil {
		t.Fatalf("apply: %v", err)
	}
	listen, id, _, _ := read()
	if listen != "127.0.0.1:1234" {
		t.Errorf("listen = %q; the config overwrote an explicitly typed flag", listen)
	}
	if id != "gw-01" {
		t.Errorf("gateway_id = %q; an untyped flag did not take the config value", id)
	}
}

func TestGatewayCanonicalEnvironmentOverridesConfig(t *testing.T) {
	path := installedGatewayConfig(t, nil)
	res, err := serviceconfig.Load(path, serviceconfig.RegistryFor(serviceconfig.ProcessGateway),
		gatewayEnvLookup(map[string]string{
			"NAMRBD_GATEWAY_CONTROL_LISTEN": "127.0.0.1:19701",
			"NAMRBD_SBS_SERVICE_ENDPOINT":   "sbs-service:9443",
		}), nil)
	if err != nil {
		t.Fatalf("load canonical environment: %v", err)
	}
	if res.File.Gateway.Listen != "127.0.0.1:19701" || res.File.Gateway.SBSAdminEndpoint != "sbs-service:9443" {
		t.Fatalf("gateway=%+v", res.File.Gateway)
	}
}

func TestGatewayLegacyEnvironmentAliasesRemainV10Compatible(t *testing.T) {
	res, err := serviceconfig.Load(installedGatewayConfig(t, nil), serviceconfig.RegistryFor(serviceconfig.ProcessGateway),
		gatewayEnvLookup(map[string]string{
			"NAMRBD_GATEWAY_LISTEN":             "127.0.0.1:19701",
			"NAMRBD_GATEWAY_SBS_ADMIN_ENDPOINT": "legacy-service:9443",
		}), nil)
	if err != nil {
		t.Fatalf("load legacy environment: %v", err)
	}
	if res.File.Gateway.Listen != "127.0.0.1:19701" || res.File.Gateway.SBSAdminEndpoint != "legacy-service:9443" {
		t.Fatalf("gateway=%+v", res.File.Gateway)
	}
	if len(res.Warnings) != 2 {
		t.Fatalf("warnings=%v", res.Warnings)
	}
}

// The gate: the strict profile refuses the flags that cannot be operated at
// t2_large, and reports all of them at once.
func TestLargeScaleProfileRejectsLegacyFlags(t *testing.T) {
	for name := range legacyFlagsRejectedAtScale {
		t.Run(name, func(t *testing.T) {
			msgs := rejectLegacyFlags(map[string]string{name: "x"})
			if len(msgs) != 1 {
				t.Fatalf("--%s was not rejected", name)
			}
			if !strings.Contains(msgs[0], name) || !strings.Contains(msgs[0], "large_scale") {
				t.Errorf("unhelpful rejection: %s", msgs[0])
			}
		})
	}
	all := rejectLegacyFlags(map[string]string{
		"volumes": "v", "sbs-cluster-replicas": "r", "control-http-listen": ":1", "gateway-id": "g",
	})
	if len(all) != 2 {
		t.Fatalf("expected both legacy flags reported at once, got %v", all)
	}
	if len(rejectLegacyFlags(map[string]string{"control-http-listen": ":1"})) != 0 {
		t.Error("a supported flag was rejected")
	}
}

// The named gate flags must be covered, whatever else the list grows to hold.
func TestGateFlagsAreRejected(t *testing.T) {
	for _, name := range []string{"volumes", "sbs-cluster-replicas",
		"sbs-cluster-metadata-backend", "sbs-cluster-metadata-path", "sbs-cluster-metadata-root"} {
		if _, ok := legacyFlagsRejectedAtScale[name]; !ok {
			t.Errorf("--%s is not in the large_scale rejection set", name)
		}
	}
}

// Every field the schema models for the gateway must be bound to a flag or
// declared unbindable with a reason. A field that is neither is a setting an
// operator writes and reviews while the process ignores it.
func TestEveryGatewayConfigFieldIsAccountedFor(t *testing.T) {
	// Fields deliberately not applied, with why.
	unbound := map[string]string{
		"gateway.dependency":                    "the availability thresholds have no flag by design; they are fleet-wide policy, not per-host, and applyGatewayConfig installs them into the dependency tracker",
		"gateway.observability.listen":          "namrbd-gateway serves observability on its control listener; applyGatewayConfig rejects the field rather than ignoring it",
		"gateway.observability.debug_endpoints": "the gateway exposes no separate debug listener to gate",
		"gateway.tls.key.env":                   "the gateway takes a key file path; a non-file reference is rejected in applyGatewayConfig",
		"gateway.tls.key.kms":                   "the gateway takes a key file path; a non-file reference is rejected in applyGatewayConfig",
		"gateway.etcd.tls":                      "the gateway has no separate etcd TLS flags today",
	}
	bound := map[string]bool{}
	for _, f := range []string{
		"gateway.gateway_id", "gateway.listen", "gateway.data_listen",
		"gateway.advertise_control_address", "gateway.advertise_data_address",
		"gateway.data_disable",
		"gateway.tls.enable", "gateway.tls.cert_file", "gateway.tls.key.file", "gateway.tls.server_name",
		"gateway.etcd.endpoints", "gateway.etcd.root",
		"gateway.sbs_admin_endpoint", "gateway.metadata_backend", "gateway.data_backend_mode",
		"gateway.cache.volume_ttl_seconds", "gateway.cache.zero_evidence_ttl_seconds",
		"gateway.cache.open_reuse_ttl_seconds", "gateway.cache.chunk_id_allocation_cache_size",
		"gateway.cache.write_plan_ttl_seconds", "gateway.cache.begin_write_volume_state_ttl_seconds",
		"gateway.reconcile.path_plan_interval_seconds", "gateway.reconcile.lease_ttl_seconds",
		"gateway.reconcile.status_refresh_interval_seconds",
		"gateway.reconcile.chunk_gc_interval_seconds", "gateway.reconcile.chunk_gc_batch_size",
		"gateway.dataplane.max_inflight_requests", "gateway.dataplane.max_inflight_bytes",
		"gateway.dataplane.max_io_size", "gateway.dataplane.token_key", "gateway.dataplane.session_key",
		"gateway.dataplane.token_ttl_seconds", "gateway.dataplane.wire_version",
		"gateway.observability.trace",
	} {
		bound[f] = true
	}

	// An entry covers a field and everything under it. A secret reference is
	// bound as a whole, because the resolver handles its file, env, and kms
	// sources together rather than one leaf at a time.
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
	for _, path := range yamlFieldPaths(reflect.TypeOf(serviceconfig.GatewayConfig{}), "gateway") {
		if covered(path) {
			continue
		}
		t.Errorf("config field %s is neither bound to a flag nor declared unbindable; "+
			"an operator would set it and the gateway would ignore it", path)
	}
}

// yamlFieldPaths walks a config struct and returns dotted yaml paths for leaves.
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

// A separate observability listener is rejected rather than ignored.
func TestObservabilityListenIsRejectedNotIgnored(t *testing.T) {
	path := installedGatewayConfig(t, func(body string) string {
		return strings.Replace(body, "  observability:\n", "  observability:\n    listen: 127.0.0.1:9100\n", 1)
	})
	res, err := serviceconfig.Load(path, nil, func(string) (string, bool) { return "", false }, nil)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	b, _ := freshBinding()
	err = applyGatewayConfig(res.File.Gateway, res.File.Profile, b, map[string]string{})
	if err == nil {
		t.Fatal("a separate observability listener was silently ignored")
	}
	if !strings.Contains(err.Error(), "control listener") {
		t.Errorf("unhelpful error: %v", err)
	}
}

// A TLS key reference the gateway cannot honor must fail rather than start
// without TLS material.
func TestNonFileTLSKeyReferenceIsRejected(t *testing.T) {
	path := installedGatewayConfig(t, func(body string) string {
		return strings.Replace(body,
			"    key:\n      file: /etc/namrbd/tls/gateway.key\n",
			"    key:\n      env: NAMRBD_GATEWAY_TLS_KEY\n", 1)
	})
	res, err := serviceconfig.Load(path, nil, func(string) (string, bool) { return "", false }, nil)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	b, _ := freshBinding()
	err = applyGatewayConfig(res.File.Gateway, res.File.Profile, b, map[string]string{})
	if err == nil {
		t.Fatal("a non-file TLS key reference was accepted")
	}
	if strings.Contains(err.Error(), "NAMRBD_GATEWAY_TLS_KEY") && !strings.Contains(err.Error(), "env:") {
		t.Errorf("error should name the reference source, not resolve it: %v", err)
	}
}

// A config for a different process must not start the gateway.
func TestConfigForAnotherProcessIsRejected(t *testing.T) {
	raw, err := os.ReadFile("../../configs/sbs-data.yaml")
	if err != nil {
		t.Fatal(err)
	}
	p := filepath.Join(t.TempDir(), "sbs-data.yaml")
	if err := os.WriteFile(p, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	_, err = applyServiceConfig(p, gatewayConfigBinding{})
	if err == nil {
		t.Fatal("an sbs-data config started the gateway")
	}
	if !strings.Contains(err.Error(), "not namrbd-gateway") {
		t.Errorf("unhelpful error: %v", err)
	}
}

// The gate must fire through the real load path, not only in its helper.
// Testing rejectLegacyFlags alone passes even if applyServiceConfig never
// calls it, which a mutation confirmed.
func TestLegacyFlagGateFiresThroughLoadPath(t *testing.T) {
	path := installedGatewayConfig(t, nil)
	_, err := applyServiceConfigWith(path, gatewayConfigBinding{}, map[string]string{"volumes": "v1,p,100"})
	if err == nil {
		t.Fatal("a large_scale config accepted --volumes")
	}
	if !strings.Contains(err.Error(), "--volumes is not supported") {
		t.Errorf("wrong failure: %v", err)
	}
}

// The same command line is fine in the dev profile, where local fixtures need it.
func TestLegacyFlagAllowedInDevProfile(t *testing.T) {
	path := installedGatewayConfig(t, func(body string) string {
		return strings.Replace(body, "profile: large_scale", "profile: dev", 1)
	})
	_, err := applyServiceConfigWith(path, gatewayConfigBinding{}, map[string]string{"volumes": "v1,p,100"})
	if err != nil && strings.Contains(err.Error(), "--volumes is not supported") {
		t.Fatalf("the dev profile rejected a legacy flag: %v", err)
	}
}

// The dependency section is declared unbound to any flag on the grounds that
// applyGatewayConfig installs it into the tracker. That reason is a claim about
// behavior, so it is checked rather than trusted: without this, the section
// could be quietly dropped and the coverage test would still pass.
func TestDependencyThresholdsReachTheTracker(t *testing.T) {
	path := installedGatewayConfig(t, nil)
	res, err := serviceconfig.Load(path, nil, func(string) (string, bool) { return "", false }, nil)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	widened := depavail.DefaultThresholds()
	widened.EtcdUnavailableGraceSeconds = 90
	res.File.Gateway.Dependency = &widened

	b, _ := freshBinding()
	if err := applyGatewayConfig(res.File.Gateway, res.File.Profile, b, map[string]string{}); err != nil {
		t.Fatalf("apply: %v", err)
	}
	if got := dependencyTracker.Thresholds().EtcdUnavailableGraceSeconds; got != 90 {
		t.Errorf("etcd grace in force is %d, want the configured 90", got)
	}
}

// A threshold set that cannot express the matrix fails startup rather than
// falling back to the defaults, which would leave the running behavior
// disagreeing with the reviewed file that produced it.
func TestAnUnusableDependencySectionFailsStartup(t *testing.T) {
	path := installedGatewayConfig(t, nil)
	res, err := serviceconfig.Load(path, nil, func(string) (string, bool) { return "", false }, nil)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	bad := depavail.Thresholds{
		EtcdUnavailableGraceSeconds: 300, TiKVUnavailableGraceSeconds: 300,
		ProjectionStaleDegradedMS: 15000, ProjectionStaleBlockedMS: 5000,
	}
	res.File.Gateway.Dependency = &bad

	b, _ := freshBinding()
	err = applyGatewayConfig(res.File.Gateway, res.File.Profile, b, map[string]string{})
	if err == nil {
		t.Fatal("a blocked threshold below the degraded threshold was accepted")
	}
	if !strings.Contains(err.Error(), "gateway.dependency") {
		t.Errorf("error does not name the offending field: %v", err)
	}
}
