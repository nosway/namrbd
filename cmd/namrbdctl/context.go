package main

import (
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

type cliContextProfile struct {
	ClusterID     string   `yaml:"cluster_id"`
	GatewayEPs    []string `yaml:"gateway_endpoints"`
	GatewayCA     string   `yaml:"gateway_ca_file"`
	EtcdEndpoints []string `yaml:"etcd_endpoints"`
	EtcdRoot      string   `yaml:"etcd_root"`
	HostID        string   `yaml:"host_id"`
	Output        string   `yaml:"output"`
	Timeout       string   `yaml:"timeout"`
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

func mustResolveCLIDefaults(args []string) cliDefaults {
	defaults, err := resolveCLIDefaults(args)
	if err != nil {
		fatalf("%v", err)
	}
	return defaults
}

func resolveCLIDefaults(args []string) (cliDefaults, error) {
	contextFile, contextName := scanContextArgs(args)
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
	fs.String("etcd-endpoints", defaults.etcdEndpoints(), "comma-separated etcd endpoints used for gateway discovery")
	fs.String("etcd-root", defaults.etcdRoot(), "etcd metadata root used for gateway discovery")
	fs.Int("gateway-discovery-limit", 128, "maximum gateway fleet records examined during endpoint discovery (1-512)")
}

func (d cliDefaults) etcdEndpoints() string {
	if value := firstEnv("NAMRBD_ETCD_ENDPOINTS"); value != "" {
		return value
	}
	if len(d.profile.EtcdEndpoints) > 0 {
		return strings.Join(d.profile.EtcdEndpoints, ",")
	}
	return "127.0.0.1:2379"
}

func (d cliDefaults) etcdRoot() string {
	if value := firstEnv("NAMRBD_ETCD_ROOT"); value != "" {
		return value
	}
	if value := strings.TrimSpace(d.profile.EtcdRoot); value != "" {
		return value
	}
	return "/namrbd"
}

func (d cliDefaults) gatewayEndpoint() string {
	for _, key := range []string{"NAMRBD_GATEWAY_ENDPOINTS"} {
		if value := firstEnvListFirst(key); value != "" {
			return value
		}
	}
	for _, item := range d.profile.GatewayEPs {
		if item = strings.TrimSpace(item); item != "" {
			return item
		}
	}
	return "http://127.0.0.1:9701"
}

func (d cliDefaults) gatewayCAFile() string {
	if value := firstEnv("NAMRBD_CA_FILE"); value != "" {
		return value
	}
	return strings.TrimSpace(d.profile.GatewayCA)
}

func (d cliDefaults) hostID() string {
	if value := firstEnv("NAMRBD_HOST_ID"); value != "" {
		return value
	}
	return strings.TrimSpace(d.profile.HostID)
}

func (d cliDefaults) timeout(fallback time.Duration) time.Duration {
	raw := firstEnv("NAMRBD_TIMEOUT")
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

func (d cliDefaults) gatewayEndpointSetting() resolvedSetting {
	if value := firstEnvListFirst("NAMRBD_GATEWAY_ENDPOINTS"); value != "" {
		return resolvedSetting{Key: "gateway", Value: value, Source: "env:NAMRBD_GATEWAY_ENDPOINTS"}
	}
	for _, item := range d.profile.GatewayEPs {
		if item = strings.TrimSpace(item); item != "" {
			return resolvedSetting{Key: "gateway", Value: item, Source: d.contextSource("gateway_endpoints")}
		}
	}
	return resolvedSetting{Key: "gateway", Value: "http://127.0.0.1:9701", Source: "default"}
}

func (d cliDefaults) gatewayCASetting() resolvedSetting {
	if value := firstEnv("NAMRBD_CA_FILE"); value != "" {
		return resolvedSetting{Key: "gateway-ca-file", Value: value, Source: "env:NAMRBD_CA_FILE"}
	}
	if value := strings.TrimSpace(d.profile.GatewayCA); value != "" {
		return resolvedSetting{Key: "gateway-ca-file", Value: value, Source: d.contextSource("gateway_ca_file")}
	}
	return resolvedSetting{Key: "gateway-ca-file", Value: "", Source: "default"}
}

func (d cliDefaults) hostSetting() resolvedSetting {
	if value := firstEnv("NAMRBD_HOST_ID"); value != "" {
		return resolvedSetting{Key: "host", Value: value, Source: "env:NAMRBD_HOST_ID"}
	}
	if value := strings.TrimSpace(d.profile.HostID); value != "" {
		return resolvedSetting{Key: "host", Value: value, Source: d.contextSource("host_id")}
	}
	return resolvedSetting{Key: "host", Value: "", Source: "default"}
}

func (d cliDefaults) timeoutSetting(fallback time.Duration) resolvedSetting {
	if raw := firstEnv("NAMRBD_TIMEOUT"); raw != "" {
		return resolvedSetting{Key: "timeout", Value: raw, Source: "env:NAMRBD_TIMEOUT"}
	}
	if raw := strings.TrimSpace(d.profile.Timeout); raw != "" {
		return resolvedSetting{Key: "timeout", Value: raw, Source: d.contextSource("timeout")}
	}
	return resolvedSetting{Key: "timeout", Value: fallback.String(), Source: "default"}
}

func sourceForFlag(fs *flag.FlagSet, defaults resolvedSetting, flagName string) resolvedSetting {
	visited := make(map[string]struct{})
	fs.Visit(func(f *flag.Flag) {
		visited[f.Name] = struct{}{}
	})
	if flagValue := fs.Lookup(flagName); flagValue != nil {
		if _, ok := visited[flagName]; ok {
			return resolvedSetting{Key: flagName, Value: flagValue.Value.String(), Source: "flag:--" + flagName}
		}
	}
	return defaults
}

func printResolvedSettings(fs *flag.FlagSet, settings ...resolvedSetting) {
	resolveDefaultGatewayFlag(fs, settings)
	show := fs.Lookup("show-config-sources")
	if show == nil || show.Value.String() != "true" {
		return
	}
	for _, setting := range settings {
		current := sourceForFlag(fs, setting, setting.Key)
		fmt.Fprintf(os.Stderr, "%s=%s (source=%s)\n", current.Key, current.Value, current.Source)
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

func firstEnv(keys ...string) string {
	for _, key := range keys {
		if v := strings.TrimSpace(os.Getenv(key)); v != "" {
			return v
		}
	}
	return ""
}

func firstEnvListFirst(keys ...string) string {
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
