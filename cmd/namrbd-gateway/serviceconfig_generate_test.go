package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/nosway/namrbd/internal/serviceconfig"
)

// flagState is a running invocation's settings, used as both the source of a
// generated config and the expectation the round trip must reproduce.
type flagState struct {
	listen, dataListen, gatewayID, adminEP, etcdEP, etcdRoot string
	certFile, keyFile, serverName                            string
	tlsEnable                                                bool
	volumeTTL, leaseTTL, statusRefresh, reconcile            time.Duration
	inflight, ioSize, chunkCache                             uint
	inflightBytes                                            uint64
	gcBatch, wireVersion                                     int
}

func (f *flagState) binding() gatewayConfigBinding {
	return gatewayConfigBinding{
		ListenAddr: &f.listen, DataListenAddr: &f.dataListen, GatewayID: &f.gatewayID,
		SBSAdminEndpoint: &f.adminEP, EtcdEndpoints: &f.etcdEP, EtcdRoot: &f.etcdRoot,
		TLSEnable: &f.tlsEnable, TLSCertFile: &f.certFile, TLSKeyFile: &f.keyFile,
		TLSServerName:  &f.serverName,
		VolumeCacheTTL: &f.volumeTTL, GatewayLeaseTTL: &f.leaseTTL,
		GatewayStatusRefreshInterval: &f.statusRefresh,
		PathPlanReconcileInterval:    &f.reconcile,
		MaxInflightRequests:          &f.inflight, MaxIOSize: &f.ioSize,
		ChunkIDAllocationCacheSize: &f.chunkCache,
		MaxInflightBytes:           &f.inflightBytes,
		ChunkGCBatchSize:           &f.gcBatch, DataplaneWireVersion: &f.wireVersion,
	}
}

func realistic() flagState {
	return flagState{
		listen: "0.0.0.0:7000", dataListen: "0.0.0.0:7001", gatewayID: "gw-42",
		adminEP: "sbs.internal:9090", etcdEP: "e1:2379,e2:2379", etcdRoot: "/namrbd/prod",
		certFile: "/etc/tls/gw.crt", keyFile: "/etc/tls/gw.key", serverName: "gw.internal",
		tlsEnable: true,
		volumeTTL: 30 * time.Second, leaseTTL: 15 * time.Second, statusRefresh: 5 * time.Second, reconcile: 5 * time.Second,
		inflight: 512, ioSize: 4194304, chunkCache: 256, inflightBytes: 268435456,
		gcBatch: 256, wireVersion: 1,
	}
}

// The gate: a flag-started deployment converts to a config and starting from
// that config reproduces the same effective settings.
func TestFlagsRoundTripThroughGeneratedConfig(t *testing.T) {
	before := realistic()
	file, secrets, dropped := buildConfigFromFlags(before.binding(), map[string]string{})
	if len(secrets) != 0 || len(dropped) != 0 {
		t.Fatalf("this invocation carries no secrets or dropped flags, got %v %v", secrets, dropped)
	}
	out, err := serviceconfig.Generate(file, secrets, dropped)
	if err != nil {
		t.Fatalf("generate: %v", err)
	}

	p := filepath.Join(t.TempDir(), "generated.yaml")
	if err := os.WriteFile(p, []byte(out.YAML), 0o600); err != nil {
		t.Fatal(err)
	}
	res, err := serviceconfig.Load(p, nil, func(string) (string, bool) { return "", false }, nil)
	if err != nil {
		t.Fatalf("the generated config does not load: %v", err)
	}
	if vr := serviceconfig.Validate(res.File); !vr.OK() {
		t.Fatalf("the generated config does not validate: %v", vr.Errors)
	}

	// Start from the generated file into empty flags and compare.
	var after flagState
	if err := applyGatewayConfig(res.File.Gateway, res.File.Profile, after.binding(), map[string]string{}); err != nil {
		t.Fatalf("apply generated config: %v", err)
	}
	for _, c := range []struct {
		field     string
		got, want any
	}{
		{"listen", after.listen, before.listen},
		{"data_listen", after.dataListen, before.dataListen},
		{"gateway_id", after.gatewayID, before.gatewayID},
		{"sbs_admin_endpoint", after.adminEP, before.adminEP},
		{"etcd_root", after.etcdRoot, before.etcdRoot},
		{"tls_enable", after.tlsEnable, before.tlsEnable},
		{"tls_cert_file", after.certFile, before.certFile},
		{"tls_key_file", after.keyFile, before.keyFile},
		{"tls_server_name", after.serverName, before.serverName},
		{"volume_cache_ttl", after.volumeTTL, before.volumeTTL},
		{"gateway_lease_ttl", after.leaseTTL, before.leaseTTL},
		{"gateway_status_refresh_interval", after.statusRefresh, before.statusRefresh},
		{"path_plan_reconcile_interval", after.reconcile, before.reconcile},
		{"max_inflight_requests", after.inflight, before.inflight},
		{"max_io_size", after.ioSize, before.ioSize},
		{"chunk_id_allocation_cache_size", after.chunkCache, before.chunkCache},
		{"max_inflight_bytes", after.inflightBytes, before.inflightBytes},
		{"chunk_gc_batch_size", after.gcBatch, before.gcBatch},
		{"dataplane_wire_version", after.wireVersion, before.wireVersion},
	} {
		if c.got != c.want {
			t.Errorf("%s did not survive the round trip: got %v, want %v", c.field, c.got, c.want)
		}
	}
	// The endpoint list is normalized from comma-separated to a YAML list and
	// back, so compare the parsed form.
	if after.etcdEP != "e1:2379,e2:2379" {
		t.Errorf("etcd_endpoints = %q", after.etcdEP)
	}
}

// A flag holding literal key material cannot be carried into a config file, so
// the generated file must say so rather than emit the value or omit the field.
func TestLiteralSecretBecomesPlaceholderAndIsReported(t *testing.T) {
	st := realistic()
	tokenKey, sessionKey := "LITERAL-TOKEN-MATERIAL", "LITERAL-SESSION-MATERIAL"
	b := st.binding()
	b.DataplaneTokenKey = &tokenKey
	b.DataplaneSessionKey = &sessionKey

	file, secrets, _ := buildConfigFromFlags(b, map[string]string{})
	out, err := serviceconfig.Generate(file, secrets, nil)
	if err != nil {
		t.Fatalf("generate: %v", err)
	}
	if strings.Contains(out.YAML, tokenKey) || strings.Contains(out.YAML, sessionKey) {
		t.Fatalf("the generated config carried literal key material:\n%s", out.YAML)
	}
	if !strings.Contains(out.YAML, serviceconfig.SecretPlaceholder) {
		t.Error("no placeholder was emitted for the secret")
	}
	if len(secrets) != 2 {
		t.Errorf("expected both key fields reported, got %v", secrets)
	}
}

// A flag with no config replacement must be reported, not silently lost.
func TestFlagWithNoConfigKeyIsReportedAsDropped(t *testing.T) {
	st := realistic()
	_, _, dropped := buildConfigFromFlags(st.binding(),
		map[string]string{"volumes": "v1,p,100", "sbs-cluster-replicas": "r1=/d", "listen": ":1"})
	if len(dropped) != 2 {
		t.Fatalf("expected both unmappable flags reported, got %v", dropped)
	}
	joined := strings.Join(dropped, " ")
	for _, want := range []string{"--volumes", "--sbs-cluster-replicas", "sbs-service"} {
		if !strings.Contains(joined, want) {
			t.Errorf("dropped report does not mention %s: %v", want, dropped)
		}
	}
}

// A generated file starts in the dev profile: it has not been reviewed, and the
// strict profile refuses settings a flag-started deployment may still rely on.
func TestGeneratedConfigStartsUnreviewed(t *testing.T) {
	st := realistic()
	file, _, _ := buildConfigFromFlags(st.binding(), map[string]string{})
	if file.Profile != serviceconfig.ProfileDev {
		t.Errorf("generated profile = %q, want dev", file.Profile)
	}
	if file.Revision != 1 {
		t.Errorf("generated revision = %d, want 1", file.Revision)
	}
	out, _ := serviceconfig.Generate(file, nil, nil)
	if !strings.Contains(out.YAML, "Review before use") {
		t.Error("the generated file does not tell the operator to review it")
	}
	if !strings.Contains(out.YAML, "0600") {
		t.Error("the generated file does not state the install mode")
	}
}

// Every flag this process deprecates must name its replacement or say there is
// none. A record with neither is not actionable.
func TestGatewayDeprecationsAreComplete(t *testing.T) {
	records := serviceconfig.DeprecationsFor(serviceconfig.ProcessGateway)
	if len(records) == 0 {
		t.Fatal("no deprecation records for the gateway")
	}
	for _, d := range records {
		if d.DeprecatedIn == "" {
			t.Errorf("--%s has no deprecation release", d.Flag)
		}
		if d.ConfigKey == "" && d.Note == "" {
			t.Errorf("--%s has no replacement and no explanation", d.Flag)
		}
	}
	// The flags the strict profile rejects must be recorded as having no
	// replacement, or an operator would look for a config key that is not there.
	for _, flag := range []string{"volumes", "sbs-cluster-replicas"} {
		d, ok := serviceconfig.DeprecationFor(serviceconfig.ProcessGateway, flag)
		if !ok {
			t.Errorf("--%s has no deprecation record", flag)
			continue
		}
		if d.ConfigKey != "" {
			t.Errorf("--%s claims config key %q, but membership comes from sbs-service", flag, d.ConfigKey)
		}
	}
}
