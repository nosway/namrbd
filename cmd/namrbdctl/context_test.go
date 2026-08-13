package main

import (
	"flag"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestLoadCLIContextFileGatewayDefaults(t *testing.T) {
	path := filepath.Join(t.TempDir(), "ctx.yaml")
	if err := os.WriteFile(path, []byte(`
gateway_endpoints:
  - https://gw-a:9701
gateway_ca_file: /etc/namrbd/ca.pem
host_id: host-a
timeout: 9s
`), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	profile, name, err := loadCLIContextFile(path, "")
	if err != nil {
		t.Fatalf("loadCLIContextFile: %v", err)
	}
	if name != "" {
		t.Fatalf("name=%q want empty", name)
	}
	if len(profile.GatewayEPs) != 1 || profile.GatewayEPs[0] != "https://gw-a:9701" {
		t.Fatalf("gateway endpoints=%v", profile.GatewayEPs)
	}
	if profile.HostID != "host-a" {
		t.Fatalf("host_id=%q want=host-a", profile.HostID)
	}
}

func TestResolveCLIDefaultsNamedContext(t *testing.T) {
	path := filepath.Join(t.TempDir(), "ctx.yaml")
	if err := os.WriteFile(path, []byte(`
contexts:
  lab:
    gateway_endpoints:
      - https://gw-lab:9701
    host_id: host-lab
    timeout: 8s
`), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	defaults, err := resolveCLIDefaults([]string{"--context-file", path, "--context", "lab"})
	if err != nil {
		t.Fatalf("resolveCLIDefaults: %v", err)
	}
	if got := defaults.gatewayEndpoint(); got != "https://gw-lab:9701" {
		t.Fatalf("gatewayEndpoint=%q want=https://gw-lab:9701", got)
	}
	if got := defaults.hostID(); got != "host-lab" {
		t.Fatalf("hostID=%q want=host-lab", got)
	}
	if got := defaults.timeout(10 * time.Second); got != 8*time.Second {
		t.Fatalf("timeout=%v want=8s", got)
	}
}

func TestGatewaySourceForFlagPrefersFlag(t *testing.T) {
	defaults := cliDefaults{
		contextFile: "/tmp/context.yaml",
		contextName: "lab",
		profile: cliContextProfile{
			GatewayEPs: []string{"https://gw-from-context:9701"},
		},
	}
	t.Setenv("NAMRBD_GATEWAY_ENDPOINTS", "https://gw-from-env:9701")

	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	fs.String("gateway", defaults.gatewayEndpoint(), "")
	if err := fs.Parse([]string{"--gateway", "https://gw-from-flag:9701"}); err != nil {
		t.Fatalf("Parse: %v", err)
	}

	setting := sourceForFlag(fs, defaults.gatewayEndpointSetting(), "gateway")
	if setting.Source != "flag:--gateway" {
		t.Fatalf("source=%q want=flag:--gateway", setting.Source)
	}
	if setting.Value != "https://gw-from-flag:9701" {
		t.Fatalf("value=%q want=https://gw-from-flag:9701", setting.Value)
	}
}
