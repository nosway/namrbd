package main

import (
	"flag"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/nosway/namrbd/internal/serviceconfig"
)

// gatewayConfigBinding holds the flag targets that a config file may set.
//
// A field present in the schema but absent here would be a setting an operator
// writes, reviews, and believes is in effect while the process ignores it.
// TestEveryGatewayConfigFieldIsAccountedFor fails when that happens.
type gatewayConfigBinding struct {
	ListenAddr           *string
	DataListenAddr       *string
	AdvertiseControlAddr *string
	AdvertiseDataAddr    *string
	DataDisable          *bool
	GatewayID            *string

	TLSEnable     *bool
	TLSCertFile   *string
	TLSKeyFile    *string
	TLSServerName *string

	EtcdEndpoints *string
	EtcdRoot      *string

	SBSAdminEndpoint *string
	MetadataBackend  *string
	DataBackendMode  *string

	VolumeCacheTTL             *time.Duration
	ZeroEvidenceCacheTTL       *time.Duration
	OpenReuseTTL               *time.Duration
	ChunkIDAllocationCacheSize *uint
	WritePlanCacheTTL          *time.Duration
	BeginWriteVolumeStateTTL   *time.Duration

	PathPlanReconcileInterval    *time.Duration
	GatewayLeaseTTL              *time.Duration
	GatewayStatusRefreshInterval *time.Duration
	ChunkGCInterval              *time.Duration
	ChunkGCBatchSize             *int

	MaxInflightRequests  *uint
	MaxInflightBytes     *uint64
	MaxIOSize            *uint
	DataplaneTokenKey    *string
	DataplaneSessionKey  *string
	DataplaneTokenTTL    *time.Duration
	DataplaneWireVersion *int

	DataplaneRequestTrace *bool
}

// legacyFlagsRejectedAtScale are the flags that cannot be operated at
// t2_large. Each names a per-process snapshot of state that belongs to a
// cluster authority, or a lab shortcut Phase X closed as unsupported.
//
// Rejecting them is the AA-IMPL-001D gate. They stay available in the dev
// profile, because local fixtures depend on them.
var legacyFlagsRejectedAtScale = map[string]string{
	"volumes":                                  "volume membership comes from sbs-service, not a per-gateway list",
	"sbs-cluster-replicas":                     "SBS replica membership comes from the sbs-service registry",
	"sbs-cluster-metadata-backend":             "raw SBS metadata access bypasses the sbs-service published view",
	"sbs-cluster-metadata-path":                "raw SBS metadata access bypasses the sbs-service published view",
	"sbs-cluster-metadata-root":                "raw SBS metadata access bypasses the sbs-service published view",
	"sbs-cluster-bootstrap-metadata":           "bootstrap metadata is a local fixture path",
	"sbs-local-path":                           "the local store backend is a development backend",
	"redis-addr":                               "the redis backend is a development backend",
	"sbs-unsafe-append-only-write-state":       "Phase X closed unsafe write-state shortcuts as unsupported",
	"sbs-unsafe-append-only-intentless-commit": "Phase X closed unsafe commit shortcuts as unsupported",
	"sbs-unsafe-zero-noop-skip-idempotency":    "Phase X closed unsafe idempotency shortcuts as unsupported",
	"sbs-unsafe-zero-replay-fast-path":         "Phase X closed unsafe replay shortcuts as unsupported",
	"sbs-quorum-early-replica-writes":          "Phase X left quorum-early replica writes disabled and unsupported",
	"sbs-parallel-begin-plan":                  "Phase X left parallel begin-plan disabled and unsupported",
}

// explicitlySetFlags reports the flags the operator actually typed. The Go flag
// package distinguishes a value someone chose from a default nobody did, which
// is what makes "explicit CLI overrides" mean anything.
func explicitlySetFlags() map[string]string {
	set := map[string]string{}
	flag.Visit(func(f *flag.Flag) { set[f.Name] = f.Value.String() })
	return set
}

// applyServiceConfig loads a config file and applies it to every bound flag the
// operator did not set explicitly.
//
// Precedence falls out of that rule: built-in flag defaults are what the flags
// already hold, the config file replaces them, the loader has already folded
// environment overrides into the config, and an explicitly typed flag is
// skipped here so it keeps its value.
func applyServiceConfig(path string, b gatewayConfigBinding) (serviceconfig.Summary, error) {
	return applyServiceConfigWith(path, b, explicitlySetFlags())
}

// applyServiceConfigWith takes the explicit flag set directly so the whole path,
// including the legacy-flag gate, is reachable from a test. Testing the gate
// helper alone would pass even if nothing called it.
func applyServiceConfigWith(path string, b gatewayConfigBinding, cliSet map[string]string) (serviceconfig.Summary, error) {
	// Only registry-backed flags are meaningful to the loader. The rest are
	// gateway flags it has never heard of, and passing them would be rejected
	// as unknown overrides.
	registry := serviceconfig.RegistryFor(serviceconfig.ProcessGateway)
	known := map[string]bool{}
	for _, o := range registry {
		known[o.Flag] = true
	}
	loaderCLI := map[string]string{}
	for name, v := range cliSet {
		if known[name] {
			loaderCLI[name] = v
		}
	}

	res, err := serviceconfig.Load(path, registry, serviceconfig.OSEnv, loaderCLI)
	if err != nil {
		return (*serviceconfig.LoadResult)(nil).Summarize([]string{err.Error()}), err
	}
	if res.File.Process != serviceconfig.ProcessGateway {
		e := fmt.Sprintf("config %s configures %q, not %s", path, res.File.Process, serviceconfig.ProcessGateway)
		return res.Summarize([]string{e}), fmt.Errorf("%s", e)
	}
	if vr := serviceconfig.Validate(res.File); !vr.OK() {
		return res.Summarize(vr.Errors), fmt.Errorf("config %s is not valid: %s", path, strings.Join(vr.Errors, "; "))
	}

	if res.File.Profile == serviceconfig.ProfileLargeScale {
		if rejected := rejectLegacyFlags(cliSet); len(rejected) > 0 {
			return res.Summarize(rejected), fmt.Errorf("%s", strings.Join(rejected, "; "))
		}
	}

	if err := applyGatewayConfig(res.File.Gateway, res.File.Profile, b, cliSet); err != nil {
		return res.Summarize([]string{err.Error()}), err
	}
	return res.Summarize(nil), nil
}

// rejectLegacyFlags returns one message per legacy flag the operator supplied.
// All of them are reported, so an operator fixes the command line once.
func rejectLegacyFlags(cliSet map[string]string) []string {
	names := make([]string, 0, len(cliSet))
	for name := range cliSet {
		if _, legacy := legacyFlagsRejectedAtScale[name]; legacy {
			names = append(names, name)
		}
	}
	sort.Strings(names)
	msgs := make([]string, 0, len(names))
	for _, name := range names {
		msgs = append(msgs, fmt.Sprintf("--%s is not supported in the %s profile: %s",
			name, serviceconfig.ProfileLargeScale, legacyFlagsRejectedAtScale[name]))
	}
	return msgs
}

func applyGatewayConfig(g *serviceconfig.GatewayConfig, profile string, b gatewayConfigBinding, cliSet map[string]string) error {
	if g == nil {
		return fmt.Errorf("config has no gateway block")
	}
	// The gateway serves observability on its control listener, so a separate
	// listener cannot be honored. Silently ignoring it would leave an operator
	// believing a port is bound.
	if strings.TrimSpace(g.Observability.Listen) != "" {
		return fmt.Errorf("gateway.observability.listen is set, but namrbd-gateway serves observability on its control listener; remove the field")
	}

	if err := installDependencyThresholds("gateway.dependency", g.Dependency); err != nil {
		return err
	}

	setString := func(flagName string, target *string, v string) {
		if target == nil || isSet(cliSet, flagName) || v == "" {
			return
		}
		*target = v
	}
	setBool := func(flagName string, target *bool, v bool) {
		if target == nil || isSet(cliSet, flagName) {
			return
		}
		*target = v
	}
	setDuration := func(flagName string, target *time.Duration, seconds int) {
		if target == nil || isSet(cliSet, flagName) || seconds < 0 {
			return
		}
		*target = time.Duration(seconds) * time.Second
	}
	setUint := func(flagName string, target *uint, v int) {
		if target == nil || isSet(cliSet, flagName) || v < 0 {
			return
		}
		*target = uint(v)
	}

	setString("gateway-id", b.GatewayID, g.GatewayID)
	setString("control-http-listen", b.ListenAddr, g.Listen)
	setString("data-listen", b.DataListenAddr, g.DataListen)
	setString("advertise-control-address", b.AdvertiseControlAddr, g.AdvertiseControlAddr)
	setString("advertise-data-address", b.AdvertiseDataAddr, g.AdvertiseDataAddr)
	setBool("data-disable", b.DataDisable, g.DataDisable)

	if g.TLS != nil {
		setBool("tls-enable", b.TLSEnable, g.TLS.Enable)
		setString("tls-cert-file", b.TLSCertFile, g.TLS.CertFile)
		setString("tls-server-name", b.TLSServerName, g.TLS.ServerName)
		// The TLS key is a reference. The gateway takes a file path today, so
		// only a file reference can be honored; anything else must fail rather
		// than start without TLS material.
		if !g.TLS.Key.Empty() {
			if strings.TrimSpace(g.TLS.Key.File) == "" {
				return fmt.Errorf("gateway.tls.key must be a file reference for namrbd-gateway, got %s", g.TLS.Key)
			}
			setString("tls-key-file", b.TLSKeyFile, g.TLS.Key.File)
		}
	}

	if g.Etcd != nil {
		if len(g.Etcd.Endpoints) > 0 {
			setString("etcd-endpoints", b.EtcdEndpoints, strings.Join(g.Etcd.Endpoints, ","))
		}
		setString("etcd-root", b.EtcdRoot, g.Etcd.Root)
	}

	setString("sbs-service-endpoint", b.SBSAdminEndpoint, g.SBSAdminEndpoint)
	setString("metadata-backend", b.MetadataBackend, g.MetadataBackend)
	setString("data-backend-mode", b.DataBackendMode, g.DataBackendMode)

	setDuration("volume-cache-ttl", b.VolumeCacheTTL, g.Cache.VolumeTTLSeconds)
	setDuration("sbs-zero-evidence-cache-ttl", b.ZeroEvidenceCacheTTL, g.Cache.ZeroEvidenceTTLSeconds)
	setDuration("sbs-open-reuse-ttl", b.OpenReuseTTL, g.Cache.OpenReuseTTLSeconds)
	setUint("sbs-chunk-id-allocation-cache-size", b.ChunkIDAllocationCacheSize, g.Cache.ChunkIDAllocationCacheSize)
	setDuration("sbs-write-plan-cache-ttl", b.WritePlanCacheTTL, g.Cache.WritePlanTTLSeconds)
	setDuration("sbs-begin-write-volume-state-cache-ttl", b.BeginWriteVolumeStateTTL, g.Cache.BeginWriteVolumeTTLSeconds)

	setDuration("path-plan-reconcile-interval", b.PathPlanReconcileInterval, g.Reconcile.PathPlanIntervalSeconds)
	setDuration("gateway-lease-ttl", b.GatewayLeaseTTL, g.Reconcile.LeaseTTLSeconds)
	setDuration("gateway-status-refresh-interval", b.GatewayStatusRefreshInterval, g.Reconcile.StatusRefreshIntervalSeconds)
	setDuration("chunk-gc-interval", b.ChunkGCInterval, g.Reconcile.ChunkGCIntervalSeconds)
	if b.ChunkGCBatchSize != nil && !isSet(cliSet, "chunk-gc-batch-size") && g.Reconcile.ChunkGCBatchSize > 0 {
		*b.ChunkGCBatchSize = g.Reconcile.ChunkGCBatchSize
	}

	setUint("max-inflight-requests", b.MaxInflightRequests, g.Dataplane.MaxInflightRequests)
	if b.MaxInflightBytes != nil && !isSet(cliSet, "max-inflight-bytes") && g.Dataplane.MaxInflightBytes > 0 {
		*b.MaxInflightBytes = uint64(g.Dataplane.MaxInflightBytes)
	}
	setUint("max-io-size", b.MaxIOSize, g.Dataplane.MaxIOSize)
	setDuration("dataplane-token-ttl", b.DataplaneTokenTTL, g.Dataplane.TokenTTLSeconds)
	if b.DataplaneWireVersion != nil && !isSet(cliSet, "dataplane-wire-version") && g.Dataplane.WireVersion > 0 {
		*b.DataplaneWireVersion = g.Dataplane.WireVersion
	}

	// Dataplane keys are references. Resolving them here keeps the material out
	// of the command line, which is where they live today.
	resolver := serviceconfig.NewResolver(profile, serviceconfig.OSEnv)
	if err := resolveInto("gateway.dataplane.token_key", resolver, g.Dataplane.TokenKey, b.DataplaneTokenKey, cliSet, "dataplane-token-key"); err != nil {
		return err
	}
	if err := resolveInto("gateway.dataplane.session_key", resolver, g.Dataplane.SessionKey, b.DataplaneSessionKey, cliSet, "dataplane-session-key"); err != nil {
		return err
	}

	setBool("dataplane-request-trace", b.DataplaneRequestTrace, g.Observability.Trace)
	return nil
}

func resolveInto(field string, r *serviceconfig.Resolver, ref serviceconfig.SecretRef, target *string, cliSet map[string]string, flagName string) error {
	if ref.Empty() || target == nil || isSet(cliSet, flagName) {
		return nil
	}
	secret, err := r.Resolve(field, ref)
	if err != nil {
		return err
	}
	*target = secret.Expose()
	return nil
}

func isSet(cliSet map[string]string, name string) bool {
	_, ok := cliSet[name]
	return ok
}
