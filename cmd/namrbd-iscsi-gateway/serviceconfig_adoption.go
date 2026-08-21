package main

import (
	"flag"
	"fmt"
	"sort"
	"strings"

	"github.com/nosway/namrbd/internal/serviceconfig"
)

// iscsiConfigBinding holds the flag targets a config file may set.
//
// It is deliberately short. This process has 37 flags, but most of them name
// which export to serve or what failover state to assume, and those belong to
// the sbs-service iSCSI registry rather than to a config file. What is left is
// instance identity, where to reach the registry, how to authenticate
// initiators, and reload policy.
type iscsiConfigBinding struct {
	GatewayID        *string
	Portal           *string
	AdvertisePortals *[]string
	EtcdEndpoints    *[]string
	EtcdRoot         *string
	SBSEndpoint      *string
	SBSAdminEndpoint *string
	SBSEndpointTLS   *bool
	SBSServerName    *string
	RegistryRequired *bool
	LargeScale       *bool

	AuthMode             *string
	CHAPSecretRef        *string
	Allowlist            *string
	ReloadMode           *string
	ReloadPollInterval   *int
	MaxExportsPerProcess *int

	ObservabilityListen *string
}

// servingMapFlagsRejectedAtScale name the flags that carry serving-map or
// failover authority on the command line.
//
// Every one of them is state the TiKV-backed sbs-service iSCSI registry owns.
// Supplying them per process is how one gateway ends up serving one export,
// and how a stale epoch or ALUA state can be asserted by a writer instead of
// being handed to it.
var servingMapFlagsRejectedAtScale = map[string]string{
	"target-iqn":                "target IQN comes from the sbs-service iSCSI registry",
	"export-id":                 "export selection comes from the registry, not a per-process flag",
	"lun-id":                    "LUN identity comes from the registry",
	"volume-id":                 "the export-to-volume mapping comes from the registry",
	"export-lease-id":           "export leases are issued by the registry",
	"export-epoch":              "export epochs are issued by the registry and enforced at the receiver",
	"active-iscsi-gateway-id":   "active gateway assignment is a registry failover decision",
	"alua-access-state":         "ALUA access state follows the registry failover transition",
	"alua-preferred":            "ALUA preference follows the registry failover transition",
	"alua-target-port-group-id": "target port group identity comes from the registry",
	"attachment-id":             "attachment identity comes from the registry",
	"generation":                "config generation comes from the registry",
	"sbs-host-id":               "host identity comes from the registry",
	"sbs-device-id":             "device identity comes from the registry",
	"session-id":                "session identity is runtime state, not startup configuration",
}

// labFlagsRejectedAtScale name backends and shortcuts that exist for fixtures.
var labFlagsRejectedAtScale = map[string]string{
	"backend":                     "the memory backend is a fixture backend",
	"memory-lun-size":             "the memory backend is a fixture backend",
	"sbs-fixture":                 "the SBS fixture backend is not a product path",
	"sbs-fixture-size":            "the SBS fixture backend is not a product path",
	"self-test":                   "self-test is a fixture mode",
	"allow-gotgt-wildcard-listen": "wildcard listen is a fixture convenience",
	"run-for":                     "a bounded run is a fixture mode",
}

func explicitlySetFlags(fs *flag.FlagSet) map[string]string {
	set := map[string]string{}
	fs.Visit(func(f *flag.Flag) { set[f.Name] = f.Value.String() })
	return set
}

// applyISCSIServiceConfig loads a config file and applies it to bound flags the
// operator did not type.
func applyISCSIServiceConfig(path string, b iscsiConfigBinding, cliSet map[string]string) (serviceconfig.Summary, error) {
	registry := serviceconfig.RegistryFor(serviceconfig.ProcessISCSIGateway)
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
	if res.File.Process != serviceconfig.ProcessISCSIGateway {
		e := fmt.Sprintf("config %s configures %q, not %s", path, res.File.Process, serviceconfig.ProcessISCSIGateway)
		return res.Summarize([]string{e}), fmt.Errorf("%s", e)
	}
	if vr := serviceconfig.Validate(res.File); !vr.OK() {
		return res.Summarize(vr.Errors), fmt.Errorf("config %s is not valid: %s", path, strings.Join(vr.Errors, "; "))
	}

	large := res.File.Profile == serviceconfig.ProfileLargeScale
	if large {
		if rejected := rejectISCSIFlags(cliSet); len(rejected) > 0 {
			return res.Summarize(rejected), fmt.Errorf("%s", strings.Join(rejected, "; "))
		}
	}
	if err := applyISCSIGatewayConfig(res.File.ISCSIGetway, large, b); err != nil {
		return res.Summarize([]string{err.Error()}), err
	}
	return res.Summarize(nil), nil
}

// rejectISCSIFlags reports every serving-map, failover, and fixture flag the
// operator supplied, so the command line is fixed once.
func rejectISCSIFlags(cliSet map[string]string) []string {
	var names []string
	for name := range cliSet {
		if _, ok := servingMapFlagsRejectedAtScale[name]; ok {
			names = append(names, name)
			continue
		}
		if _, ok := labFlagsRejectedAtScale[name]; ok {
			names = append(names, name)
		}
	}
	sort.Strings(names)
	msgs := make([]string, 0, len(names))
	for _, name := range names {
		reason, ok := servingMapFlagsRejectedAtScale[name]
		if !ok {
			reason = labFlagsRejectedAtScale[name]
		}
		msgs = append(msgs, fmt.Sprintf("--%s is not supported in the %s profile: %s",
			name, serviceconfig.ProfileLargeScale, reason))
	}
	return msgs
}

func applyISCSIGatewayConfig(g *serviceconfig.ISCSIGatewayConfig, large bool, b iscsiConfigBinding) error {
	if g == nil {
		return fmt.Errorf("config has no iscsi_gateway block")
	}
	if err := installDependencyThresholds("iscsi_gateway.dependency", g.Dependency); err != nil {
		return err
	}

	set := func(target *string, v string) {
		if target != nil && v != "" {
			*target = v
		}
	}
	set(b.GatewayID, g.GatewayID)
	if len(g.AdvertisePortals) > 0 {
		// gotgt binds the first portal until the multi-export supervisor lands,
		// while the fleet record publishes every reachable portal now.
		set(b.Portal, g.AdvertisePortals[0])
		if b.AdvertisePortals != nil {
			*b.AdvertisePortals = append([]string(nil), g.AdvertisePortals...)
		}
	}
	if g.Etcd != nil {
		if b.EtcdEndpoints != nil {
			*b.EtcdEndpoints = append([]string(nil), g.Etcd.Endpoints...)
		}
		set(b.EtcdRoot, g.Etcd.Root)
	}
	set(b.SBSEndpoint, g.SBSEndpoint)
	set(b.SBSAdminEndpoint, g.SBSAdminEndpoint)
	if g.SBSEndpointTLS != nil {
		if b.SBSEndpointTLS != nil {
			*b.SBSEndpointTLS = g.SBSEndpointTLS.Enable
		}
		set(b.SBSServerName, g.SBSEndpointTLS.ServerName)
	}

	set(b.AuthMode, g.Auth.Mode)
	if !g.Auth.CHAPSecret.Empty() {
		// The process takes a secret reference string today, so the reference is
		// passed through rather than resolved here. Resolution stays in the
		// component that consumes it.
		if strings.TrimSpace(g.Auth.CHAPSecret.File) == "" {
			return fmt.Errorf("iscsi_gateway.auth.chap_secret must be a file reference for this process, got %s", g.Auth.CHAPSecret)
		}
		set(b.CHAPSecretRef, g.Auth.CHAPSecret.File)
	}
	if len(g.Auth.AllowedInitiatorIQNs) > 0 {
		set(b.Allowlist, strings.Join(g.Auth.AllowedInitiatorIQNs, ","))
	}
	set(b.ObservabilityListen, g.Observability.Listen)
	set(b.ReloadMode, g.Reload.Mode)
	if b.ReloadPollInterval != nil && g.Reload.PollIntervalSeconds > 0 {
		*b.ReloadPollInterval = g.Reload.PollIntervalSeconds
	}
	if b.MaxExportsPerProcess != nil {
		*b.MaxExportsPerProcess = g.Reload.MaxExportsPerProcess
	}
	if b.LargeScale != nil {
		*b.LargeScale = large
	}

	if large {
		// The registry is the mapping authority, so the process must fail if it
		// cannot load it rather than falling back to flags.
		if b.RegistryRequired != nil {
			*b.RegistryRequired = true
		}
		if strings.TrimSpace(g.SBSAdminEndpoint) == "" {
			return fmt.Errorf("iscsi_gateway.sbs_admin_endpoint is required in the %s profile; the registry is the mapping authority",
				serviceconfig.ProfileLargeScale)
		}
	}
	return nil
}
