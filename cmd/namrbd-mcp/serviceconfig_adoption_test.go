package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/nosway/namrbd/internal/mcpops"
	"github.com/nosway/namrbd/internal/serviceconfig"
)

func noEnvLookup(string) (string, bool) { return "", false }

func installedMCPConfig(t *testing.T, edit func(string) string) string {
	t.Helper()
	raw, err := os.ReadFile("../../configs/namrbd-mcp.yaml")
	if err != nil {
		t.Fatalf("read example: %v", err)
	}
	body := string(raw)
	if edit != nil {
		body = edit(body)
	}
	dst := filepath.Join(t.TempDir(), "namrbd-mcp.yaml")
	if err := os.WriteFile(dst, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return dst
}

func TestMCPConfigSuppliesSettings(t *testing.T) {
	cfg := mcpops.DefaultConfig()
	if _, err := applyMCPConfig(installedMCPConfig(t, nil), &cfg, map[string]string{}, noEnvLookup); err != nil {
		t.Fatalf("apply: %v", err)
	}
	if cfg.Mode != mcpops.ModeObserve {
		t.Errorf("mode = %q", cfg.Mode)
	}
	if cfg.ApprovalPolicy != "dry-run" {
		t.Errorf("approval_policy = %q", cfg.ApprovalPolicy)
	}
	if cfg.HTTPTimeout != 10*time.Second {
		t.Errorf("http_timeout = %v", cfg.HTTPTimeout)
	}
	if !strings.Contains(cfg.OperationsEndpoint, "sbs-service") {
		t.Errorf("operations_endpoint = %q", cfg.OperationsEndpoint)
	}
}

// Phase Y closed MCP support as read-only, so the strict profile refuses the
// operate posture in the file.
func TestOperatePostureRefusedInFile(t *testing.T) {
	path := installedMCPConfig(t, func(b string) string {
		return strings.Replace(b, "mode: observe", "mode: operate", 1)
	})
	cfg := mcpops.DefaultConfig()
	if _, err := applyMCPConfig(path, &cfg, map[string]string{}, noEnvLookup); err == nil {
		t.Fatal("the operate posture was accepted at scale")
	}
}

// And refuses it when a typed flag reintroduces it after the file validated.
// Checking only the file would leave this path open.
func TestOperatePostureRefusedWhenReintroducedByFlag(t *testing.T) {
	cfg := mcpops.DefaultConfig()
	cfg.Mode = mcpops.ModeOperate
	_, err := applyMCPConfig(installedMCPConfig(t, nil), &cfg, map[string]string{"mode": "operate"}, noEnvLookup)
	if err == nil {
		t.Fatal("a typed --mode operate survived the strict profile")
	}
	if !strings.Contains(err.Error(), "read-only") {
		t.Errorf("failure does not cite the Phase Y boundary: %v", err)
	}
}

// The dev profile keeps operate available for development.
func TestDevProfileAllowsOperatePosture(t *testing.T) {
	path := installedMCPConfig(t, func(b string) string {
		b = strings.Replace(b, "profile: large_scale", "profile: dev", 1)
		return strings.Replace(b, "mode: observe", "mode: operate", 1)
	})
	cfg := mcpops.DefaultConfig()
	if _, err := applyMCPConfig(path, &cfg, map[string]string{}, noEnvLookup); err != nil {
		t.Fatalf("the dev profile refused the operate posture: %v", err)
	}
}

func TestTypedFlagOutranksMCPConfig(t *testing.T) {
	cfg := mcpops.DefaultConfig()
	cfg.ApprovalPolicy = "local-confirmation"
	if _, err := applyMCPConfig(installedMCPConfig(t, nil), &cfg,
		map[string]string{"approval-policy": "local-confirmation"}, noEnvLookup); err != nil {
		t.Fatalf("apply: %v", err)
	}
	if cfg.ApprovalPolicy != "local-confirmation" {
		t.Errorf("approval_policy = %q; the config overrode a typed flag", cfg.ApprovalPolicy)
	}
}

func TestMCPConfigForAnotherProcessIsRejected(t *testing.T) {
	raw, _ := os.ReadFile("../../configs/sbs-data.yaml")
	p := filepath.Join(t.TempDir(), "sbs-data.yaml")
	if err := os.WriteFile(p, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	cfg := mcpops.DefaultConfig()
	if _, err := applyMCPConfig(p, &cfg, map[string]string{}, noEnvLookup); err == nil {
		t.Fatal("an sbs-data config started the MCP server")
	}
}

var _ = serviceconfig.ProcessMCP
