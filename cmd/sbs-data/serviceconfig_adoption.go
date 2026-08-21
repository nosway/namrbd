package main

import (
	"flag"
	"fmt"
	"os"
	"sort"
	"strings"

	"github.com/nosway/namrbd/internal/serviceconfig"
)

// sbsDataConfigBinding holds the flag targets a config file may set.
//
// The store layout is not among them. It stays in the separate --store-config
// document, which already has a reload path that rejects store removals.
// Absorbing it here would mean either duplicating that safety or losing it.
type sbsDataConfigBinding struct {
	ClusterID       *string
	SBSClusterID    *string
	NodeID          *string
	DataPath        *string
	StoreConfigPath *string
	GRPCListen      *string
	HTTPListen      *string

	EnableLabStoreDebug *bool
	DataOperationTrace  *bool
}

// envBackedFlags maps a flag to the environment variable supplying its default.
// As in sbs-service, these are read when the defaults are built rather than
// applied afterwards, so a config file must not outrank a variable that is
// already set.
var envBackedFlags = map[string]string{
	"path":                                   "NAMRBD_SBS_DATA_PATH",
	"store-config":                           "NAMRBD_SBS_STORE_CONFIG",
	"sbs-data-listen":                        "NAMRBD_SBS_DATA_GRPC_LISTEN",
	"sbs-data-http-listen":                   "NAMRBD_SBS_DATA_HTTP_LISTEN",
	"enable-lab-store-debug":                 "NAMRBD_SBS_ENABLE_LAB_STORE_DEBUG",
	"lab-disable-idempotency-sync":           "NAMRBD_SBS_LAB_DISABLE_IDEMPOTENCY_SYNC",
	"lab-cache-open-volume-spec":             "NAMRBD_SBS_LAB_CACHE_OPEN_VOLUME_SPEC",
	"lab-disable-physical-write-idempotency": "NAMRBD_SBS_LAB_DISABLE_PHYSICAL_WRITE_IDEMPOTENCY",
	"data-operation-trace":                   "NAMRBD_SBS_DATA_OPERATION_TRACE",
}

// sbsDataLabFlagsRejectedAtScale name the shortcuts that trade durability or
// isolation for speed, plus the debug mutation endpoints.
//
// Each of these changes how a payload write is made durable or how a volume
// spec is trusted. On a node serving real data they are not tuning, they are a
// different correctness contract.
var sbsDataLabFlagsRejectedAtScale = map[string]string{
	"enable-lab-store-debug":                 "the debug store mutation endpoints let an operator change store state outside any audited path",
	"lab-disable-idempotency-sync":           "skipping the idempotency sync trades durability for speed",
	"lab-cache-open-volume-spec":             "reusing an opened volume spec on hot requests trades revalidation for speed",
	"lab-disable-physical-write-idempotency": "skipping durable idempotency lookup on physical writes trades correctness for speed",
	"data-operation-trace":                   "per-operation trace events are a scale hazard in production-like profiles",
}

func explicitlySetFlags(fs *flag.FlagSet) map[string]string {
	set := map[string]string{}
	fs.Visit(func(f *flag.Flag) { set[f.Name] = f.Value.String() })
	return set
}

func osEnvLookup(key string) (string, bool) { return os.LookupEnv(key) }

// applySBSDataConfig loads a config file and applies it to bound flags that
// neither an explicit command line nor an environment variable already supplied.
func applySBSDataConfig(path string, b sbsDataConfigBinding, cliSet map[string]string, env serviceconfig.EnvLookup) (serviceconfig.Summary, error) {
	if env == nil {
		env = serviceconfig.OSEnv
	}
	registry := serviceconfig.RegistryFor(serviceconfig.ProcessSBSData)
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
	if res.File.Process != serviceconfig.ProcessSBSData {
		e := fmt.Sprintf("config %s configures %q, not %s", path, res.File.Process, serviceconfig.ProcessSBSData)
		return res.Summarize([]string{e}), fmt.Errorf("%s", e)
	}
	if vr := serviceconfig.Validate(res.File); !vr.OK() {
		return res.Summarize(vr.Errors), fmt.Errorf("config %s is not valid: %s", path, strings.Join(vr.Errors, "; "))
	}

	large := res.File.Profile == serviceconfig.ProfileLargeScale
	if large {
		if rejected := rejectSBSDataFlags(cliSet); len(rejected) > 0 {
			return res.Summarize(rejected), fmt.Errorf("%s", strings.Join(rejected, "; "))
		}
	}

	overrides, err := applySBSDataBlock(res.File.SBSData, large, b, cliSet, env)
	if err != nil {
		return res.Summarize([]string{err.Error()}), err
	}
	res.Overrides = serviceconfig.MergeAppliedOverrides(res.Overrides, overrides)
	return res.Summarize(nil), nil
}

func rejectSBSDataFlags(cliSet map[string]string) []string {
	var names []string
	for name := range cliSet {
		if _, ok := sbsDataLabFlagsRejectedAtScale[name]; ok {
			names = append(names, name)
		}
	}
	sort.Strings(names)
	msgs := make([]string, 0, len(names))
	for _, name := range names {
		msgs = append(msgs, fmt.Sprintf("--%s is not supported in the %s profile: %s",
			name, serviceconfig.ProfileLargeScale, sbsDataLabFlagsRejectedAtScale[name]))
	}
	return msgs
}

func applySBSDataBlock(d *serviceconfig.SBSDataConfig, large bool, b sbsDataConfigBinding, cliSet map[string]string, env serviceconfig.EnvLookup) ([]serviceconfig.AppliedOverride, error) {
	if d == nil {
		return nil, fmt.Errorf("config has no sbs_data block")
	}
	var overrides []serviceconfig.AppliedOverride

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
			Field: field, Source: serviceconfig.SourceEnv, Value: v,
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

	if b.NodeID != nil && d.NodeID != "" {
		*b.NodeID = d.NodeID
	}
	if b.ClusterID != nil && d.ClusterID != "" {
		*b.ClusterID = d.ClusterID
	}
	if b.SBSClusterID != nil && d.SBSClusterID != "" {
		*b.SBSClusterID = d.SBSClusterID
	}
	setStr("path", "sbs_data.data_path", b.DataPath, d.DataPath)
	setStr("sbs-data-listen", "sbs_data.grpc_listen", b.GRPCListen, d.GRPCListen)
	setStr("sbs-data-http-listen", "sbs_data.http_listen", b.HTTPListen, d.HTTPListen)
	setStr("store-config", "sbs_data.store_config_path", b.StoreConfigPath, d.StoreConfigPath)

	setBool("enable-lab-store-debug", "sbs_data.observability.debug_endpoints", b.EnableLabStoreDebug, d.Observability.DebugEndpoints)
	setBool("data-operation-trace", "sbs_data.observability.trace", b.DataOperationTrace, d.Observability.Trace)

	if large {
		// A node with no store layout has nowhere to place payload. Failing at
		// startup is better than accepting writes that cannot be placed.
		if strings.TrimSpace(d.StoreConfigPath) == "" {
			return overrides, fmt.Errorf("sbs_data.store_config_path is required in the %s profile; "+
				"store layout stays in its own reloadable document", serviceconfig.ProfileLargeScale)
		}
		if _, err := os.Stat(strings.TrimSpace(d.StoreConfigPath)); err != nil {
			return overrides, fmt.Errorf("sbs_data.store_config_path %s is not readable: %v", d.StoreConfigPath, err)
		}
	}
	return overrides, nil
}
