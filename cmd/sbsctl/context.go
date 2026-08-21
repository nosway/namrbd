package main

import (
	"flag"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/nosway/namrbd/internal/cliux"
	"github.com/nosway/namrbd/internal/envcompat"
	"gopkg.in/yaml.v3"
)

type cliContextProfile struct {
	ClusterID        string   `yaml:"cluster_id"`
	SBSClusterID     string   `yaml:"sbs_cluster_id"`
	SBSAdminEPs      []string `yaml:"sbs_admin_endpoints"`
	SBSDataEPs       []string `yaml:"sbs_data_endpoints"`
	NodeID           string   `yaml:"node_id"`
	GatewayID        string   `yaml:"gateway_id"`
	Zone             string   `yaml:"zone"`
	Output           string   `yaml:"output"`
	Timeout          string   `yaml:"timeout"`
	SBSGRPCAddr      string   `yaml:"sbs_grpc_addr"`
	SBSNodeAdminHTTP string   `yaml:"sbs_node_admin_http"`
}

type cliContextFile struct {
	Context           string `yaml:"context"`
	cliContextProfile `yaml:",inline"`
	Contexts          map[string]cliContextProfile `yaml:"contexts"`
}

type cliDefaults struct {
	contextFile string
	contextName string
	profile     cliContextProfile
}

type resolvedSetting struct {
	Key    string
	Value  string
	Source string
}

var sbsctlCompatEnvSpecs = []envcompat.Spec{
	envcompat.SBSClusterID,
	envcompat.SBSCTLServiceEndpoints,
	envcompat.SBSCTLDataEndpoints,
	envcompat.SBSCTLTimeout,
	envcompat.SBSCTLOutput,
	envcompat.SBSCTLNodeID,
	envcompat.SBSCTLZone,
	envcompat.SBSCTLServiceHTTPEndpoint,
}

var emittedCompatEnvWarnings = map[string]bool{}
var emittedCompatEnvWarningsMu sync.Mutex

func resolveSBSCTLCompatEnv(keys ...string) (envcompat.Resolution, bool) {
	for _, spec := range sbsctlCompatEnvSpecs {
		matched := false
		for _, key := range keys {
			if spec.Matches(key) {
				matched = true
				break
			}
		}
		if !matched {
			continue
		}
		resolved, err := envcompat.ResolveCurrent(spec, os.LookupEnv)
		if err != nil {
			if globalJSONOutput {
				fmt.Fprintln(os.Stderr, err)
			}
			fatalf("environment configuration: %v", err)
		}
		for _, warning := range resolved.Warnings {
			emittedCompatEnvWarningsMu.Lock()
			if emittedCompatEnvWarnings[warning] {
				emittedCompatEnvWarningsMu.Unlock()
				continue
			}
			emittedCompatEnvWarnings[warning] = true
			emittedCompatEnvWarningsMu.Unlock()
			fmt.Fprintln(os.Stderr, warning)
		}
		return resolved, true
	}
	return envcompat.Resolution{}, false
}

func firstListItem(raw string) string {
	for _, part := range strings.Split(raw, ",") {
		if part = strings.TrimSpace(part); part != "" {
			return part
		}
	}
	return ""
}

var sbsctlDeprecatedFlags = []cliux.Alias{
	{Legacy: "admin-endpoint", Canonical: "sbs-service-endpoint", DeprecatedIn: "post-1.0"},
	{Legacy: "admin-http-endpoint", Canonical: "sbs-service-http-endpoint", DeprecatedIn: "post-1.0"},
}

func parseCommandFlags(fs *flag.FlagSet, args []string) {
	cliux.InstallStructuredUsage(fs, "sbsctl "+fs.Name(), nil)
	args = cliux.RewriteDeprecatedFlags(args, sbsctlDeprecatedFlags, os.Stderr)
	args = cliux.RewriteCommandArgs(args, fs.Lookup("output") != nil, fs.Lookup("json") != nil)
	if globalJSONOutput {
		if output := fs.Lookup("output"); output != nil {
			_ = output.Value.Set("json")
		}
		if jsonFlag := fs.Lookup("json"); jsonFlag != nil {
			_ = jsonFlag.Value.Set("true")
		}
	}
	if err := fs.Parse(args); err != nil {
		fatalf("parse %s flags: %v", fs.Name(), err)
	}
}

func mustResolveCLIDefaults(args []string) cliDefaults {
	defaults, err := resolveCLIDefaults(args)
	if err != nil {
		fatalf("%v", err)
	}
	return defaults
}

func resolveCLIDefaults(args []string) (cliDefaults, error) {
	contextFile, contextName := scanContextArgs(args)
	if contextFile == "" {
		contextFile = strings.TrimSpace(globalContextFile)
	}
	if contextName == "" {
		contextName = strings.TrimSpace(globalContextName)
	}
	if contextName == "" {
		contextName = strings.TrimSpace(os.Getenv("NAMRBD_CONTEXT"))
	}
	if contextFile == "" {
		return cliDefaults{contextName: contextName}, nil
	}
	profile, resolvedName, err := loadCLIContextFile(contextFile, contextName)
	if err != nil {
		return cliDefaults{}, err
	}
	return cliDefaults{
		contextFile: contextFile,
		contextName: resolvedName,
		profile:     profile,
	}, nil
}

func scanContextArgs(args []string) (string, string) {
	var contextFile string
	var contextName string
	for i := 0; i < len(args); i++ {
		arg := strings.TrimSpace(args[i])
		switch {
		case strings.HasPrefix(arg, "--context-file="):
			contextFile = strings.TrimSpace(strings.TrimPrefix(arg, "--context-file="))
		case arg == "--context-file" && i+1 < len(args):
			i++
			contextFile = strings.TrimSpace(args[i])
		case strings.HasPrefix(arg, "--context="):
			contextName = strings.TrimSpace(strings.TrimPrefix(arg, "--context="))
		case arg == "--context" && i+1 < len(args):
			i++
			contextName = strings.TrimSpace(args[i])
		}
	}
	return contextFile, contextName
}

func loadCLIContextFile(path, selected string) (cliContextProfile, string, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return cliContextProfile{}, "", fmt.Errorf("read context file %s: %w", path, err)
	}
	var cfg cliContextFile
	if err := yaml.Unmarshal(raw, &cfg); err != nil {
		return cliContextProfile{}, "", fmt.Errorf("parse context file %s: %w", path, err)
	}
	if len(cfg.Contexts) == 0 {
		return cfg.cliContextProfile, selected, nil
	}
	name := strings.TrimSpace(selected)
	if name == "" {
		name = strings.TrimSpace(cfg.Context)
	}
	if name == "" && len(cfg.Contexts) == 1 {
		for only := range cfg.Contexts {
			name = only
		}
	}
	if name == "" {
		return cliContextProfile{}, "", fmt.Errorf("context file %s requires --context or top-level context", path)
	}
	profile, ok := cfg.Contexts[name]
	if !ok {
		return cliContextProfile{}, "", fmt.Errorf("context %q not found in %s", name, path)
	}
	return profile, name, nil
}

func registerContextFlags(fs *flag.FlagSet, defaults cliDefaults) {
	fs.String("context-file", defaults.contextFile, "path to context file")
	fs.String("context", defaults.contextName, "context name inside context file")
	fs.Bool("show-config-sources", false, "print resolved config values and their sources")
}

func (d cliDefaults) stringValue(fallback string, envKeys ...string) string {
	if value := firstEnv(envKeys...); value != "" {
		return value
	}
	if fallback == "" {
		return ""
	}
	return fallback
}

func (d cliDefaults) fieldValue(field string, envKeys ...string) string {
	if value := firstEnv(envKeys...); value != "" {
		return value
	}
	switch field {
	case "cluster_id":
		return strings.TrimSpace(d.profile.ClusterID)
	case "sbs_cluster_id":
		return strings.TrimSpace(d.profile.SBSClusterID)
	case "node_id":
		return strings.TrimSpace(d.profile.NodeID)
	case "gateway_id":
		return strings.TrimSpace(d.profile.GatewayID)
	case "zone":
		return strings.TrimSpace(d.profile.Zone)
	case "output":
		return strings.TrimSpace(d.profile.Output)
	case "sbs_grpc_addr":
		return strings.TrimSpace(d.profile.SBSGRPCAddr)
	case "sbs_node_admin_http":
		return strings.TrimSpace(d.profile.SBSNodeAdminHTTP)
	default:
		return ""
	}
}

func (d cliDefaults) firstListValue(values []string, envKeys ...string) string {
	if value := firstEnvListFirst(envKeys...); value != "" {
		return value
	}
	for _, item := range values {
		if item = strings.TrimSpace(item); item != "" {
			return item
		}
	}
	return ""
}

func (d cliDefaults) adminEndpoint() string {
	return d.firstListValue(d.profile.SBSAdminEPs, "NAMRBD_SBS_SERVICE_ENDPOINTS")
}

func (d cliDefaults) dataEndpoint() string {
	if value := firstEnvListFirst("NAMRBD_SBS_DATA_ENDPOINTS"); value != "" {
		return value
	}
	if value := d.fieldValue("sbs_grpc_addr", "NAMRBD_SBS_DATA_ENDPOINTS"); value != "" {
		return value
	}
	if value := d.firstListValue(d.profile.SBSDataEPs); value != "" {
		return value
	}
	return ""
}

func (d cliDefaults) timeout(fallback time.Duration) time.Duration {
	raw := firstEnv("NAMRBD_SBSCTL_TIMEOUT")
	if raw == "" {
		raw = strings.TrimSpace(d.profile.Timeout)
	}
	if raw == "" {
		return fallback
	}
	parsed, err := time.ParseDuration(raw)
	if err != nil {
		return fallback
	}
	return parsed
}

func (d cliDefaults) fieldSetting(field, flagName string, fallback string, envKeys ...string) resolvedSetting {
	if value, source := d.fieldSettingValue(field, fallback, envKeys...); value != "" {
		return resolvedSetting{Key: flagName, Value: value, Source: source}
	}
	return resolvedSetting{Key: flagName, Value: "", Source: "default"}
}

func (d cliDefaults) fieldSettingValue(field, fallback string, envKeys ...string) (string, string) {
	if resolved, ok := resolveSBSCTLCompatEnv(envKeys...); ok {
		if value := strings.TrimSpace(resolved.Value); resolved.Present && value != "" {
			return value, "env:" + resolved.Source
		}
	} else {
		for _, key := range envKeys {
			if value := strings.TrimSpace(os.Getenv(key)); value != "" {
				return value, "env:" + key
			}
		}
	}
	if value := d.contextFieldValue(field); value != "" {
		return value, d.contextSource(field)
	}
	return fallback, "default"
}

func (d cliDefaults) adminEndpointSetting() resolvedSetting {
	if resolved, _ := resolveSBSCTLCompatEnv(envcompat.SBSCTLServiceEndpoints.Canonical); resolved.Present {
		if value := firstListItem(resolved.Value); value != "" {
			return resolvedSetting{Key: "sbs-service-endpoint", Value: value, Source: "env:" + resolved.Source}
		}
	}
	for _, item := range d.profile.SBSAdminEPs {
		if item = strings.TrimSpace(item); item != "" {
			return resolvedSetting{Key: "sbs-service-endpoint", Value: item, Source: d.contextSource("sbs_admin_endpoints")}
		}
	}
	return resolvedSetting{Key: "sbs-service-endpoint", Value: "", Source: "default"}
}

func (d cliDefaults) dataEndpointSetting() resolvedSetting {
	if resolved, _ := resolveSBSCTLCompatEnv(envcompat.SBSCTLDataEndpoints.Canonical); resolved.Present {
		if value := firstListItem(resolved.Value); value != "" {
			return resolvedSetting{Key: "data-endpoint", Value: value, Source: "env:" + resolved.Source}
		}
	}
	if value := strings.TrimSpace(d.profile.SBSGRPCAddr); value != "" {
		return resolvedSetting{Key: "data-endpoint", Value: value, Source: d.contextSource("sbs_grpc_addr")}
	}
	for _, item := range d.profile.SBSDataEPs {
		if item = strings.TrimSpace(item); item != "" {
			return resolvedSetting{Key: "data-endpoint", Value: item, Source: d.contextSource("sbs_data_endpoints")}
		}
	}
	return resolvedSetting{Key: "data-endpoint", Value: "", Source: "default"}
}

func (d cliDefaults) timeoutSetting(fallback time.Duration) resolvedSetting {
	if resolved, _ := resolveSBSCTLCompatEnv(envcompat.SBSCTLTimeout.Canonical); resolved.Present {
		if raw := strings.TrimSpace(resolved.Value); raw != "" {
			return resolvedSetting{Key: "timeout", Value: raw, Source: "env:" + resolved.Source}
		}
	}
	if raw := strings.TrimSpace(d.profile.Timeout); raw != "" {
		return resolvedSetting{Key: "timeout", Value: raw, Source: d.contextSource("timeout")}
	}
	return resolvedSetting{Key: "timeout", Value: fallback.String(), Source: "default"}
}

func visitedFlags(fs *flag.FlagSet) map[string]struct{} {
	out := make(map[string]struct{})
	fs.Visit(func(f *flag.Flag) {
		out[f.Name] = struct{}{}
	})
	return out
}

func sourceForFlag(fs *flag.FlagSet, defaults resolvedSetting, flagName string) resolvedSetting {
	if flagValue := fs.Lookup(flagName); flagValue != nil {
		if _, ok := visitedFlags(fs)[flagName]; ok {
			return resolvedSetting{Key: flagName, Value: flagValue.Value.String(), Source: "flag:--" + flagName}
		}
	}
	return defaults
}

func printResolvedSettings(fs *flag.FlagSet, settings ...resolvedSetting) {
	show := fs.Lookup("show-config-sources")
	if show == nil || show.Value.String() != "true" {
		return
	}
	for _, setting := range settings {
		current := sourceForFlag(fs, setting, setting.Key)
		fmt.Fprintf(os.Stderr, "%s=%s (source=%s)\n", current.Key, current.Value, current.Source)
	}
}

func (d cliDefaults) contextFieldValue(field string) string {
	switch field {
	case "cluster_id":
		return strings.TrimSpace(d.profile.ClusterID)
	case "sbs_cluster_id":
		return strings.TrimSpace(d.profile.SBSClusterID)
	case "node_id":
		return strings.TrimSpace(d.profile.NodeID)
	case "gateway_id":
		return strings.TrimSpace(d.profile.GatewayID)
	case "zone":
		return strings.TrimSpace(d.profile.Zone)
	case "output":
		return strings.TrimSpace(d.profile.Output)
	case "timeout":
		return strings.TrimSpace(d.profile.Timeout)
	case "sbs_grpc_addr":
		return strings.TrimSpace(d.profile.SBSGRPCAddr)
	case "sbs_node_admin_http":
		return strings.TrimSpace(d.profile.SBSNodeAdminHTTP)
	default:
		return ""
	}
}

func (d cliDefaults) contextSource(field string) string {
	if d.contextFile == "" {
		return "context"
	}
	if d.contextName != "" {
		return fmt.Sprintf("context:%s[%s].%s", d.contextFile, d.contextName, field)
	}
	return fmt.Sprintf("context:%s.%s", d.contextFile, field)
}

func firstEnvListFirst(keys ...string) string {
	if resolved, ok := resolveSBSCTLCompatEnv(keys...); ok {
		if resolved.Present {
			return firstListItem(resolved.Value)
		}
		return ""
	}
	for _, key := range keys {
		if raw := strings.TrimSpace(os.Getenv(key)); raw != "" {
			parts := strings.Split(raw, ",")
			for _, part := range parts {
				part = strings.TrimSpace(part)
				if part != "" {
					return part
				}
			}
		}
	}
	return ""
}
