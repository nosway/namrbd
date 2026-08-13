package main

import (
	"flag"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestLoadCLIContextFileFlat(t *testing.T) {
	path := filepath.Join(t.TempDir(), "ctx.yaml")
	if err := os.WriteFile(path, []byte(`
cluster_id: namrbd-dev
sbs_cluster_id: sbs-dev
sbs_admin_endpoints:
  - admin-a:9443
sbs_data_endpoints:
  - data-a:9460
node_id: node-a
timeout: 12s
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
	if profile.ClusterID != "namrbd-dev" || profile.SBSClusterID != "sbs-dev" {
		t.Fatalf("profile ids=%+v", profile)
	}
	if len(profile.SBSAdminEPs) != 1 || profile.SBSAdminEPs[0] != "admin-a:9443" {
		t.Fatalf("admin endpoints=%v", profile.SBSAdminEPs)
	}
}

func TestLoadCLIContextFileNamedContext(t *testing.T) {
	path := filepath.Join(t.TempDir(), "ctx.yaml")
	if err := os.WriteFile(path, []byte(`
context: prod
contexts:
  dev:
    cluster_id: namrbd-dev
    sbs_cluster_id: sbs-dev
  prod:
    cluster_id: namrbd-prod
    sbs_cluster_id: sbs-prod
    sbs_admin_endpoints:
      - prod-admin:9443
`), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	profile, name, err := loadCLIContextFile(path, "")
	if err != nil {
		t.Fatalf("loadCLIContextFile: %v", err)
	}
	if name != "prod" {
		t.Fatalf("name=%q want=prod", name)
	}
	if profile.ClusterID != "namrbd-prod" || profile.SBSClusterID != "sbs-prod" {
		t.Fatalf("profile ids=%+v", profile)
	}
}

func TestResolveCLIDefaultsUsesContextFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "ctx.yaml")
	if err := os.WriteFile(path, []byte(`
contexts:
  lab:
    cluster_id: namrbd-lab
    sbs_cluster_id: sbs-lab
    sbs_admin_endpoints:
      - admin-lab:9443
    timeout: 15s
`), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	defaults, err := resolveCLIDefaults([]string{"--context-file", path, "--context", "lab"})
	if err != nil {
		t.Fatalf("resolveCLIDefaults: %v", err)
	}
	if defaults.profile.ClusterID != "namrbd-lab" {
		t.Fatalf("cluster_id=%q want=namrbd-lab", defaults.profile.ClusterID)
	}
	if got := defaults.adminEndpoint(); got != "admin-lab:9443" {
		t.Fatalf("adminEndpoint=%q want=admin-lab:9443", got)
	}
	if got := defaults.timeout(10 * time.Second); got != 15*time.Second {
		t.Fatalf("timeout=%v want=15s", got)
	}
}

func TestContextEnvOverridesFile(t *testing.T) {
	defaults := cliDefaults{
		profile: cliContextProfile{
			SBSClusterID: "sbs-from-file",
		},
	}
	t.Setenv("SBS_CLUSTER_ID", "sbs-from-env")
	if got := defaults.fieldValue("sbs_cluster_id", "SBS_CLUSTER_ID", "NAMRBD_SBS_CLUSTER_ID"); got != "sbs-from-env" {
		t.Fatalf("fieldValue=%q want=sbs-from-env", got)
	}
}

func TestSourceForFlagPrefersFlagOverEnvAndContext(t *testing.T) {
	defaults := cliDefaults{
		contextFile: "/tmp/test-context.yaml",
		contextName: "lab",
		profile: cliContextProfile{
			SBSClusterID: "sbs-from-context",
		},
	}
	t.Setenv("SBS_CLUSTER_ID", "sbs-from-env")

	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	fs.String("sbs-cluster-id", defaults.fieldValue("sbs_cluster_id", "SBS_CLUSTER_ID", "NAMRBD_SBS_CLUSTER_ID"), "")
	if err := fs.Parse([]string{"--sbs-cluster-id", "sbs-from-flag"}); err != nil {
		t.Fatalf("Parse: %v", err)
	}

	setting := sourceForFlag(fs, defaults.fieldSetting("sbs_cluster_id", "sbs-cluster-id", "", "SBS_CLUSTER_ID", "NAMRBD_SBS_CLUSTER_ID"), "sbs-cluster-id")
	if setting.Source != "flag:--sbs-cluster-id" {
		t.Fatalf("source=%q want=%q", setting.Source, "flag:--sbs-cluster-id")
	}
	if setting.Value != "sbs-from-flag" {
		t.Fatalf("value=%q want=%q", setting.Value, "sbs-from-flag")
	}
}
