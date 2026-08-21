package main

import (
	"flag"
	"fmt"
	"os"
	"sort"
	"strings"

	"github.com/nosway/namrbd/internal/serviceconfig"
)

// csiConfigBinding holds the flag targets a config file may set.
//
// This driver runs once per Kubernetes node, which is exactly the deployment
// shape where a copied command line drifts: the same eight settings repeated
// across every worker, with node identity as the only intended difference.
type csiConfigBinding struct {
	DriverName     *string
	NodeID         *string
	Endpoint       *string
	AdminEndpoint  *string
	AdminEndpoints *string
	ClusterID      *string
	SBSClusterID   *string
	GatewayURL     *string
}

// envBackedFlags maps a flag to the environment variable supplying its default.
// As elsewhere these are read while building defaults, so the config file must
// not outrank a variable that is already set.
var envBackedFlags = map[string]string{
	"cluster-id":     "NAMRBD_CLUSTER_ID",
	"sbs-cluster-id": "NAMRBD_SBS_CLUSTER_ID",
	"node-id":        "NAMRBD_CSI_NODE_ID",
	"gateway-url":    "NAMRBD_GATEWAY_URL",
}

// csiFlagsRejectedAtScale name settings that must not vary between nodes of one
// cluster. A driver on one worker pointed at a different cluster, or running a
// different driver name than its CSIDriver object declares, fails in ways that
// look like a storage problem rather than a configuration one.
var csiFlagsRejectedAtScale = map[string]string{
	"driver-name":    "the driver name must match the cluster's CSIDriver object on every node",
	"vendor-version": "the vendor version identifies the build, not a per-node setting",
	"cluster-id":     "cluster identity must be identical on every node of a cluster",
	"sbs-cluster-id": "SBS cluster identity must be identical on every node of a cluster",
	"namrbdctl":      "the helper binary path is a packaging decision, not a per-node flag",
}

func explicitlySetFlags(fs *flag.FlagSet) map[string]string {
	set := map[string]string{}
	fs.Visit(func(f *flag.Flag) { set[f.Name] = f.Value.String() })
	return set
}

func osEnvLookup(key string) (string, bool) { return os.LookupEnv(key) }

func applyCSIConfig(path string, b csiConfigBinding, cliSet map[string]string, env serviceconfig.EnvLookup) (serviceconfig.Summary, error) {
	if env == nil {
		env = serviceconfig.OSEnv
	}
	registry := serviceconfig.RegistryFor(serviceconfig.ProcessCSIDriver)
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
	if res.File.Process != serviceconfig.ProcessCSIDriver {
		e := fmt.Sprintf("config %s configures %q, not %s", path, res.File.Process, serviceconfig.ProcessCSIDriver)
		return res.Summarize([]string{e}), fmt.Errorf("%s", e)
	}
	if vr := serviceconfig.Validate(res.File); !vr.OK() {
		return res.Summarize(vr.Errors), fmt.Errorf("config %s is not valid: %s", path, strings.Join(vr.Errors, "; "))
	}

	large := res.File.Profile == serviceconfig.ProfileLargeScale
	if large {
		if rejected := rejectCSIFlags(cliSet); len(rejected) > 0 {
			return res.Summarize(rejected), fmt.Errorf("%s", strings.Join(rejected, "; "))
		}
	}
	overrides, err := applyCSIBlock(res.File.CSIDriver, large, b, cliSet, env)
	if err != nil {
		return res.Summarize([]string{err.Error()}), err
	}
	res.Overrides = append(res.Overrides, overrides...)
	return res.Summarize(nil), nil
}

func rejectCSIFlags(cliSet map[string]string) []string {
	var names []string
	for name := range cliSet {
		if _, ok := csiFlagsRejectedAtScale[name]; ok {
			names = append(names, name)
		}
	}
	sort.Strings(names)
	msgs := make([]string, 0, len(names))
	for _, name := range names {
		msgs = append(msgs, fmt.Sprintf("--%s is not supported in the %s profile: %s",
			name, serviceconfig.ProfileLargeScale, csiFlagsRejectedAtScale[name]))
	}
	return msgs
}

func applyCSIBlock(c *serviceconfig.CSIDriverConfig, large bool, b csiConfigBinding, cliSet map[string]string, env serviceconfig.EnvLookup) ([]serviceconfig.AppliedOverride, error) {
	if c == nil {
		return nil, fmt.Errorf("config has no csi_driver block")
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
	set := func(flagName, field string, target *string, v string) {
		if target == nil || v == "" || supersededBy(flagName, field) {
			return
		}
		*target = v
	}

	if b.DriverName != nil && c.DriverName != "" {
		*b.DriverName = c.DriverName
	}
	set("node-id", "csi_driver.node_id", b.NodeID, c.NodeID)
	set("endpoint", "csi_driver.endpoint", b.Endpoint, c.Endpoint)
	set("cluster-id", "csi_driver.cluster_id", b.ClusterID, c.ClusterID)
	set("sbs-cluster-id", "csi_driver.sbs_cluster_id", b.SBSClusterID, c.SBSClusterID)
	set("gateway-url", "csi_driver.gateway_url", b.GatewayURL, c.GatewayURL)

	if len(c.AdminEndpoints) > 0 {
		// The first entry is the primary the client dials; the full list is what
		// it fails over across.
		set("admin-endpoint", "csi_driver.admin_endpoints", b.AdminEndpoint, c.AdminEndpoints[0])
		set("admin-endpoints", "csi_driver.admin_endpoints", b.AdminEndpoints, strings.Join(c.AdminEndpoints, ","))
	}

	if large {
		// A single admin endpoint makes every volume operation on this node
		// depend on one service instance staying up.
		if len(c.AdminEndpoints) < 2 {
			return overrides, fmt.Errorf("csi_driver.admin_endpoints lists %d endpoint(s); the %s profile needs at least two "+
				"so a single sbs-service instance is not a per-node single point of failure",
				len(c.AdminEndpoints), serviceconfig.ProfileLargeScale)
		}
		if strings.TrimSpace(c.NodeID) == "" {
			if _, present := env("NAMRBD_CSI_NODE_ID"); !present {
				return overrides, fmt.Errorf("csi_driver.node_id is empty and NAMRBD_CSI_NODE_ID is unset; " +
					"a CSI node must identify itself")
			}
		}
	}
	return overrides, nil
}
