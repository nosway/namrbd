package main

import (
	"flag"
	"fmt"
	"strings"
	"time"

	"github.com/nosway/namrbd/internal/mcpops"
	"github.com/nosway/namrbd/internal/serviceconfig"
)

// mcpFlagsRejectedAtScale is empty of tuning knobs on purpose: this process has
// five flags and none of them is a per-host setting. What the strict profile
// enforces here is posture, not flag hygiene.
var mcpFlagsRejectedAtScale = map[string]string{}

func explicitlySetFlags(fs *flag.FlagSet) map[string]string {
	set := map[string]string{}
	fs.Visit(func(f *flag.Flag) { set[f.Name] = f.Value.String() })
	return set
}

// applyMCPConfig loads a config file into the MCP runtime config.
//
// The posture rule is the point. Phase Y closed MCP as read-only, so the
// large_scale profile refuses the operate posture. Config validation already
// rejects it, and this path re-checks after overrides because an environment
// variable or a typed flag could otherwise reintroduce it after the file
// validated cleanly.
func applyMCPConfig(path string, cfg *mcpops.Config, cliSet map[string]string, env serviceconfig.EnvLookup) (serviceconfig.Summary, error) {
	if env == nil {
		env = serviceconfig.OSEnv
	}
	registry := serviceconfig.RegistryFor(serviceconfig.ProcessMCP)
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
	if res.File.Process != serviceconfig.ProcessMCP {
		e := fmt.Sprintf("config %s configures %q, not %s", path, res.File.Process, serviceconfig.ProcessMCP)
		return res.Summarize([]string{e}), fmt.Errorf("%s", e)
	}
	if vr := serviceconfig.Validate(res.File); !vr.OK() {
		return res.Summarize(vr.Errors), fmt.Errorf("config %s is not valid: %s", path, strings.Join(vr.Errors, "; "))
	}

	large := res.File.Profile == serviceconfig.ProfileLargeScale
	if err := applyMCPBlock(res.File.MCP, cfg, cliSet); err != nil {
		return res.Summarize([]string{err.Error()}), err
	}

	// Re-check posture after overrides. Validation ran against the file; a typed
	// flag or a registry override could still have turned observe into operate.
	if large && cfg.Mode == mcpops.ModeOperate {
		e := fmt.Sprintf("mcp.mode %q is not admissible in the %s profile; Phase Y closed MCP support as read-only",
			mcpops.ModeOperate, serviceconfig.ProfileLargeScale)
		return res.Summarize([]string{e}), fmt.Errorf("%s", e)
	}
	return res.Summarize(nil), nil
}

func applyMCPBlock(m *serviceconfig.MCPConfig, cfg *mcpops.Config, cliSet map[string]string) error {
	if m == nil {
		return fmt.Errorf("config has no mcp block")
	}
	if cfg == nil {
		return fmt.Errorf("mcp runtime config is nil")
	}
	set := func(flagName string, target *string, v string) {
		if v == "" {
			return
		}
		if _, typed := cliSet[flagName]; typed {
			return
		}
		*target = v
	}
	set("operations-endpoint", &cfg.OperationsEndpoint, m.OperationsEndpoint)
	set("mode", &cfg.Mode, m.Mode)
	set("approval-policy", &cfg.ApprovalPolicy, m.ApprovalPolicy)
	set("operation-output-dir", &cfg.OperationOutputDir, m.OperationOutputDir)
	if m.HTTPTimeoutSeconds > 0 {
		if _, typed := cliSet["http-timeout"]; !typed {
			cfg.HTTPTimeout = time.Duration(m.HTTPTimeoutSeconds) * time.Second
		}
	}
	return nil
}
