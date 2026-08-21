package main

import (
	"flag"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/nosway/namrbd/internal/serviceconfig"
)

// sbsServiceConfigBinding holds the flag targets a config file may set.
type sbsServiceConfigBinding struct {
	ClusterID       *string
	SBSClusterID    *string
	NodeID          *string
	MetadataBackend *string

	GRPCListen  *string
	HTTPListen  *string
	PayloadRoot *string

	TiKVPDEndpoints    *string
	TiKVKeyspace       *string
	TiKVAPIVersion     *string
	TiKVTimeout        *time.Duration
	TiKVTLSEnabled     *bool
	TiKVCAFile         *string
	TiKVCertFile       *string
	TiKVKeyFile        *string
	TiKVOperationTrace *bool

	LeaderLeaseDuration    *time.Duration
	LeaderRenewInterval    *time.Duration
	HealthShardCount       *int
	HealthConcurrency      *int
	HealthInterval         *time.Duration
	HealthTimeout          *time.Duration
	HealthSuspectAfter     *int
	HealthDownAfter        *int
	HealthRecoveryCooldown *time.Duration

	ServiceOwnedWriteEffects   *bool
	NativeAllocationFastPath   *bool
	WriteEffectsBatchMax       *int
	WriteEffectsLaneBuckets    *int
	AsyncWriteMutationFinalize *bool
}

// envBackedFlags maps a flag to the environment variable that supplies its
// default.
//
// This matters for precedence. sbs-service reads these variables when it builds
// the flag defaults, not as an override applied afterwards. Left alone, a
// config file would outrank an environment variable that was already set, which
// is the opposite of the documented order:
//
//	built-in defaults < config file < environment overrides < explicit CLI
//
// So the config is applied only when the backing variable is absent, and a
// present variable is recorded as an env override in the summary.
var envBackedFlags = map[string]string{
	"cluster-id":                       "NAMRBD_CLUSTER_ID",
	"sbs-cluster-id":                   "NAMRBD_SBS_CLUSTER_ID",
	"node-id":                          "NAMRBD_SBS_SERVICE_NODE_ID",
	"metadata-backend":                 "NAMRBD_SBS_METADATA_BACKEND",
	"metadata-path":                    "NAMRBD_SBS_STATE_DIR",
	"leader-lease-duration":            "NAMRBD_SBS_LEADER_LEASE_DURATION",
	"leader-renew-interval":            "NAMRBD_SBS_LEADER_RENEW_INTERVAL",
	"tikv-pd-endpoints":                "NAMRBD_TIKV_PD_ENDPOINTS",
	"tikv-timeout":                     "NAMRBD_TIKV_TIMEOUT",
	"tikv-api-version":                 "NAMRBD_TIKV_API_VERSION",
	"tikv-keyspace":                    "NAMRBD_TIKV_KEYSPACE",
	"tikv-tls-enabled":                 "NAMRBD_TIKV_TLS_ENABLED",
	"tikv-ca-file":                     "NAMRBD_CA_FILE",
	"tikv-cert-file":                   "NAMRBD_CERT_FILE",
	"tikv-key-file":                    "NAMRBD_KEY_FILE",
	"tikv-operation-trace":             "NAMRBD_TIKV_OPERATION_TRACE",
	"sbs-service-listen":               "NAMRBD_SBS_SERVICE_GRPC_LISTEN",
	"sbs-service-http-listen":          "NAMRBD_SBS_SERVICE_HTTP_LISTEN",
	"payload-root":                     "NAMRBD_SBS_PAYLOAD_ROOT",
	"service-owned-write-effects":      "NAMRBD_SBS_SERVICE_OWNED_WRITE_EFFECTS",
	"native-allocation-fast-path":      "NAMRBD_SBS_NATIVE_ALLOCATION_FAST_PATH",
	"async-write-mutation-finalize":    "NAMRBD_SBS_ASYNC_WRITE_MUTATION_FINALIZE",
	"write-intent-batch-coalesce-wait": "NAMRBD_SBS_WRITE_INTENT_BATCH_COALESCE_WAIT",
	"health-shard-count":               "NAMRBD_SBS_DATA_HEALTH_SHARD_COUNT",
	"health-concurrency":               "NAMRBD_SBS_DATA_HEALTH_CONCURRENCY",
	"health-interval":                  "NAMRBD_SBS_DATA_HEALTH_CHECK_INTERVAL",
	"health-timeout":                   "NAMRBD_SBS_DATA_HEALTH_TIMEOUT",
	"health-suspect-after":             "NAMRBD_SBS_DATA_SUSPECT_AFTER",
	"health-down-after":                "NAMRBD_SBS_DATA_DOWN_AFTER",
	"health-recovery-cooldown":         "NAMRBD_SBS_DATA_RECOVER_COOLDOWN",
}

// labFlagsRejectedAtScale are settings that must be identical across every
// sbs-service instance in a cluster, or that Phase X left disabled.
//
// A per-instance TiKV or write-effect knob is how two services in one cluster
// end up with different metadata behavior, which is invisible until they
// disagree.
var sbsLabFlagsRejectedAtScale = map[string]string{
	"metadata-backend":                "the metadata backend is a cluster-wide decision, not a per-instance flag",
	"metadata-path":                   "the local metadata path is a bootstrap development backend",
	"tikv-operation-trace":            "verbose TiKV tracing is a scale hazard in production-like profiles",
	"tikv-async-commit":               "TiKV commit mode must be identical across services in a cluster",
	"tikv-one-phase-commit":           "TiKV commit mode must be identical across services in a cluster",
	"write-effects-batch-max":         "write-effect batching must be identical across services in a cluster",
	"write-effects-lane-bucket-count": "write-effect lane partitioning must be identical across services in a cluster",
	"async-write-mutation-finalize":   "Phase X left async mutation finalize disabled and unsupported",
}

func explicitlySetFlags(fs *flag.FlagSet) map[string]string {
	set := map[string]string{}
	fs.Visit(func(f *flag.Flag) { set[f.Name] = f.Value.String() })
	return set
}

// applySBSServiceConfig loads a config file and applies it to bound flags that
// neither an explicit command line nor an environment variable already supplied.
func applySBSServiceConfig(path string, b sbsServiceConfigBinding, cliSet map[string]string, env serviceconfig.EnvLookup) (serviceconfig.Summary, error) {
	if env == nil {
		env = serviceconfig.OSEnv
	}
	registry := serviceconfig.RegistryFor(serviceconfig.ProcessSBSService)
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

	res, err := serviceconfig.Load(path, registry, env, loaderCLI)
	if err != nil {
		return (*serviceconfig.LoadResult)(nil).Summarize([]string{err.Error()}), err
	}
	if res.File.Process != serviceconfig.ProcessSBSService {
		e := fmt.Sprintf("config %s configures %q, not %s", path, res.File.Process, serviceconfig.ProcessSBSService)
		return res.Summarize([]string{e}), fmt.Errorf("%s", e)
	}
	if vr := serviceconfig.Validate(res.File); !vr.OK() {
		return res.Summarize(vr.Errors), fmt.Errorf("config %s is not valid: %s", path, strings.Join(vr.Errors, "; "))
	}

	large := res.File.Profile == serviceconfig.ProfileLargeScale
	if large {
		if rejected := rejectSBSFlags(cliSet); len(rejected) > 0 {
			return res.Summarize(rejected), fmt.Errorf("%s", strings.Join(rejected, "; "))
		}
	}

	if res.File.SBSService != nil {
		if err := installDependencyThresholds("sbs_service.dependency", res.File.SBSService.Dependency); err != nil {
			return res.Summarize([]string{err.Error()}), err
		}
	}

	envOverrides := applySBSServiceBlock(res.File.SBSService, b, cliSet, env)
	res.Overrides = serviceconfig.MergeAppliedOverrides(res.Overrides, envOverrides)
	return res.Summarize(nil), nil
}

func rejectSBSFlags(cliSet map[string]string) []string {
	var names []string
	for name := range cliSet {
		if _, ok := sbsLabFlagsRejectedAtScale[name]; ok {
			names = append(names, name)
		}
	}
	sort.Strings(names)
	msgs := make([]string, 0, len(names))
	for _, name := range names {
		msgs = append(msgs, fmt.Sprintf("--%s is not supported in the %s profile: %s",
			name, serviceconfig.ProfileLargeScale, sbsLabFlagsRejectedAtScale[name]))
	}
	return msgs
}

// applySBSServiceBlock writes config values into flags and returns the
// environment overrides it deferred to, so the summary records them.
func applySBSServiceBlock(s *serviceconfig.SBSServiceConfig, b sbsServiceConfigBinding, cliSet map[string]string, env serviceconfig.EnvLookup) []serviceconfig.AppliedOverride {
	if s == nil {
		return nil
	}
	var overrides []serviceconfig.AppliedOverride

	// supersededBy reports whether something already outranks the config file:
	// an explicitly typed flag, or an environment variable that supplied the
	// flag's default before the config was read.
	supersededBy := func(flagName, field string) bool {
		if _, typed := cliSet[flagName]; typed {
			return true
		}
		envName, backed := envBackedFlags[flagName]
		if !backed {
			return false
		}
		v, present := env(envName)
		if !present {
			return false
		}
		overrides = append(overrides, serviceconfig.AppliedOverride{
			Field: field, Source: serviceconfig.SourceEnv, Value: redactEnvValue(envName, v),
		})
		return true
	}
	setStr := func(flagName, field string, target *string, v string) {
		if target == nil || v == "" || supersededBy(flagName, field) {
			return
		}
		*target = v
	}
	setBool := func(flagName, field string, target *bool, v bool) {
		if target == nil || supersededBy(flagName, field) {
			return
		}
		*target = v
	}
	setDur := func(flagName, field string, target *time.Duration, seconds int) {
		if target == nil || seconds <= 0 || supersededBy(flagName, field) {
			return
		}
		*target = time.Duration(seconds) * time.Second
	}
	setInt := func(flagName, field string, target *int, v int) {
		if target == nil || v <= 0 || supersededBy(flagName, field) {
			return
		}
		*target = v
	}

	setStr("cluster-id", "sbs_service.cluster_id", b.ClusterID, s.ClusterID)
	setStr("sbs-cluster-id", "sbs_service.sbs_cluster_id", b.SBSClusterID, s.SBSClusterID)
	setStr("node-id", "sbs_service.node_id", b.NodeID, s.NodeID)
	setStr("metadata-backend", "sbs_service.metadata_backend", b.MetadataBackend, s.MetadataBackend)
	setStr("sbs-service-listen", "sbs_service.grpc_listen", b.GRPCListen, s.GRPCListen)
	setStr("sbs-service-http-listen", "sbs_service.http_listen", b.HTTPListen, s.HTTPListen)
	setStr("payload-root", "sbs_service.payload_root", b.PayloadRoot, s.PayloadRoot)

	if len(s.TiKV.PDEndpoints) > 0 {
		setStr("tikv-pd-endpoints", "sbs_service.tikv.pd_endpoints", b.TiKVPDEndpoints, strings.Join(s.TiKV.PDEndpoints, ","))
	}
	setStr("tikv-keyspace", "sbs_service.tikv.keyspace", b.TiKVKeyspace, s.TiKV.Keyspace)
	setStr("tikv-api-version", "sbs_service.tikv.api_version", b.TiKVAPIVersion, s.TiKV.APIVersion)
	setDur("tikv-timeout", "sbs_service.tikv.timeout_seconds", b.TiKVTimeout, s.TiKV.TimeoutSeconds)
	setBool("tikv-operation-trace", "sbs_service.tikv.operation_trace", b.TiKVOperationTrace, s.TiKV.OperationTrace)
	if s.TiKV.TLS != nil {
		setBool("tikv-tls-enabled", "sbs_service.tikv.tls.enable", b.TiKVTLSEnabled, s.TiKV.TLS.Enable)
		setStr("tikv-cert-file", "sbs_service.tikv.tls.cert_file", b.TiKVCertFile, s.TiKV.TLS.CertFile)
		if strings.TrimSpace(s.TiKV.TLS.Key.File) != "" {
			setStr("tikv-key-file", "sbs_service.tikv.tls.key.file", b.TiKVKeyFile, s.TiKV.TLS.Key.File)
		}
	}

	setDur("leader-lease-duration", "sbs_service.leader.lease_duration_seconds", b.LeaderLeaseDuration, s.Leader.LeaseDurationSeconds)
	setDur("leader-renew-interval", "sbs_service.leader.renew_interval_seconds", b.LeaderRenewInterval, s.Leader.RenewIntervalSeconds)
	setInt("health-shard-count", "sbs_service.health.shard_count", b.HealthShardCount, s.Health.ShardCount)
	setInt("health-concurrency", "sbs_service.health.concurrency_per_shard", b.HealthConcurrency, s.Health.ConcurrencyPerShard)
	setDur("health-interval", "sbs_service.health.interval_seconds", b.HealthInterval, s.Health.IntervalSeconds)
	setDur("health-timeout", "sbs_service.health.timeout_seconds", b.HealthTimeout, s.Health.TimeoutSeconds)
	setInt("health-suspect-after", "sbs_service.health.suspect_threshold", b.HealthSuspectAfter, s.Health.SuspectThreshold)
	setInt("health-down-after", "sbs_service.health.down_threshold", b.HealthDownAfter, s.Health.DownThreshold)
	setDur("health-recovery-cooldown", "sbs_service.health.recovery_cooldown_seconds", b.HealthRecoveryCooldown, s.Health.RecoveryCooldownSeconds)

	setBool("service-owned-write-effects", "sbs_service.write_effects.service_owned", b.ServiceOwnedWriteEffects, s.WriteEffects.ServiceOwned)
	setBool("native-allocation-fast-path", "sbs_service.write_effects.native_allocation_fast_path", b.NativeAllocationFastPath, s.WriteEffects.NativeAllocationFastPath)
	setBool("async-write-mutation-finalize", "sbs_service.write_effects.async_mutation_finalize", b.AsyncWriteMutationFinalize, s.WriteEffects.AsyncMutationFinalize)
	setInt("write-effects-batch-max", "sbs_service.write_effects.batch_max", b.WriteEffectsBatchMax, s.WriteEffects.BatchMax)
	setInt("write-effects-lane-bucket-count", "sbs_service.write_effects.lane_bucket_count", b.WriteEffectsLaneBuckets, s.WriteEffects.LaneBucketCount)

	return overrides
}

// redactEnvValue hides the value of a variable that names key material. The
// variable name is recorded either way, which is what an operator needs to see.
func redactEnvValue(name, v string) string {
	upper := strings.ToUpper(name)
	if strings.Contains(upper, "KEY") || strings.Contains(upper, "SECRET") ||
		strings.Contains(upper, "TOKEN") || serviceconfig.LooksLikeSecretLiteral(v) {
		return serviceconfig.RedactedMarker
	}
	return v
}

// osEnvLookup is the production lookup.
func osEnvLookup(key string) (string, bool) { return os.LookupEnv(key) }
