package main

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"sort"
	"strings"

	"github.com/nosway/namrbd/internal/clustermanifest"
)

type repeatedStringFlag []string

func (v *repeatedStringFlag) String() string { return strings.Join(*v, ",") }

func (v *repeatedStringFlag) Set(value string) error {
	value = strings.TrimSpace(value)
	if value == "" {
		return fmt.Errorf("value must not be empty")
	}
	*v = append(*v, value)
	return nil
}

type manifestValidationSummary struct {
	Phase                         string         `json:"phase"`
	Slice                         string         `json:"slice"`
	Result                        string         `json:"result"`
	Entrypoint                    string         `json:"entrypoint"`
	ValidationBoundary            string         `json:"validation_boundary"`
	ManifestName                  string         `json:"manifest_name"`
	ManifestRevision              string         `json:"manifest_revision"`
	ManifestDigest                string         `json:"manifest_digest"`
	ArtifactDigest                string         `json:"artifact_digest"`
	ArtifactApprovalChecked       bool           `json:"artifact_approval_checked"`
	NodeCount                     int            `json:"node_count"`
	ZoneCount                     int            `json:"zone_count"`
	NodesPerZone                  map[string]int `json:"nodes_per_zone"`
	ActiveServiceHosts            []string       `json:"active_service_hosts"`
	StandbyServiceHosts           []string       `json:"standby_service_hosts"`
	CanonicalizationDeterministic bool           `json:"canonicalization_deterministic"`
	PureValidation                bool           `json:"pure_validation"`
	TiKVMutationCount             int            `json:"tikv_mutation_count"`
	DaemonActionCount             int            `json:"daemon_action_count"`
	StorageMutationCount          int            `json:"storage_mutation_count"`
	RemoteLabUsed                 bool           `json:"remote_lab_used"`
	Physical160Claimed            bool           `json:"physical_160_claimed"`
	OKCount                       int            `json:"ok_count"`
	ErrorCount                    int            `json:"error_count"`
	FirstError                    string         `json:"first_error"`
	LastError                     string         `json:"last_error"`
}

func runClusterManifest(args []string) {
	if len(args) < 1 {
		clusterManifestUsage()
		os.Exit(2)
	}
	switch args[0] {
	case "validate":
		runClusterManifestValidate(args[1:])
	case "render":
		runClusterManifestRender(args[1:])
	case "plan":
		runClusterManifestPlan(args[1:])
	case "export":
		runClusterManifestExport(args[1:])
	case "admit":
		runClusterManifestAdmission(args[1:])
	case "rollout":
		runClusterManifestRollout(args[1:])
	case "standby":
		runClusterManifestStandby(args[1:])
	default:
		clusterManifestUsage()
		os.Exit(2)
	}
}

type manifestPlanSummary struct {
	Phase                    string         `json:"phase"`
	Slice                    string         `json:"slice"`
	Result                   string         `json:"result"`
	Entrypoint               string         `json:"entrypoint"`
	ValidationBoundary       string         `json:"validation_boundary"`
	PlanID                   string         `json:"plan_id"`
	PlanDigest               string         `json:"plan_digest"`
	DesiredManifestDigest    string         `json:"desired_manifest_digest"`
	CurrentManifestDigest    string         `json:"current_manifest_digest,omitempty"`
	ActionCounts             map[string]int `json:"action_counts"`
	ActionCount              int            `json:"action_count"`
	ProjectedDaemonRestarts  int            `json:"projected_daemon_restarts"`
	ExecutedTiKVMutations    int            `json:"executed_tikv_mutations"`
	ExecutedDaemonActions    int            `json:"executed_daemon_actions"`
	ExecutedStorageMutations int            `json:"executed_storage_mutations"`
	NoMutationDryRun         bool           `json:"no_mutation_dry_run"`
	SameDigestReapplyNoOp    bool           `json:"same_digest_reapply_no_op"`
	PlanUnblocked            bool           `json:"plan_unblocked"`
	PlanArtifactWritten      bool           `json:"plan_artifact_written"`
	RemoteLabUsed            bool           `json:"remote_lab_used"`
	Physical160Claimed       bool           `json:"physical_160_claimed"`
	OKCount                  int            `json:"ok_count"`
	ErrorCount               int            `json:"error_count"`
	FirstError               string         `json:"first_error"`
	LastError                string         `json:"last_error"`
}

func runClusterManifestPlan(args []string) {
	fs := flag.NewFlagSet("cluster manifest plan", flag.ExitOnError)
	fs.SetOutput(os.Stderr)
	file := fs.String("file", "", "desired cluster manifest YAML path")
	currentManifest := fs.String("current-manifest", "", "optional current canonical manifest for pure reapply comparison")
	currentState := fs.String("current-state", "", "optional observed-state JSON for exact impact comparison")
	planOutput := fs.String("plan-output", "", "optional new path for the full JSON plan")
	output := fs.String("output", "table", "output format: table|json")
	var approved repeatedStringFlag
	fs.Var(&approved, "approved-artifact-digest", "externally approved sha256 digest; repeatable")
	parseCommandFlags(fs, args)
	entrypoint := "sbsctl cluster manifest plan"
	if *output != "table" && *output != "json" {
		manifestCommandErrorFor("AD-IMPL-001C", entrypoint, *output, fmt.Errorf("unsupported output format %q", *output))
	}
	if strings.TrimSpace(*file) == "" {
		manifestCommandErrorFor("AD-IMPL-001C", entrypoint, *output, errors.New("--file is required"))
	}
	if *currentManifest != "" && *currentState != "" {
		manifestCommandErrorFor("AD-IMPL-001C", entrypoint, *output, errors.New("--current-manifest and --current-state are mutually exclusive"))
	}
	desired, err := clustermanifest.Load(*file)
	if err != nil {
		manifestCommandErrorFor("AD-IMPL-001C", entrypoint, *output, err)
	}
	policy := clustermanifest.ValidationPolicy{ApprovedArtifactDigests: []string(approved), RequireArtifactApproval: true}
	var current *clustermanifest.ObservedState
	if *currentManifest != "" {
		currentDesired, err := clustermanifest.Load(*currentManifest)
		if err != nil {
			manifestCommandErrorFor("AD-IMPL-001C", entrypoint, *output, err)
		}
		currentRendered, err := clustermanifest.Render(currentDesired, policy)
		if err != nil {
			manifestCommandErrorFor("AD-IMPL-001C", entrypoint, *output, err)
		}
		current, err = clustermanifest.ObservedStateFromRenderSet(currentRendered, "running")
		if err != nil {
			manifestCommandErrorFor("AD-IMPL-001C", entrypoint, *output, err)
		}
	} else if *currentState != "" {
		current, err = clustermanifest.LoadObservedState(*currentState)
		if err != nil {
			manifestCommandErrorFor("AD-IMPL-001C", entrypoint, *output, err)
		}
	}
	plan, err := clustermanifest.BuildPlan(desired, current, policy)
	if err != nil {
		manifestCommandErrorFor("AD-IMPL-001C", entrypoint, *output, err)
	}
	written := false
	if strings.TrimSpace(*planOutput) != "" {
		raw, err := clustermanifest.MarshalPlan(plan)
		if err != nil {
			manifestCommandErrorFor("AD-IMPL-001C", entrypoint, *output, err)
		}
		if err := clustermanifest.WriteNewFile(*planOutput, raw, 0o640); err != nil {
			manifestCommandErrorFor("AD-IMPL-001C", entrypoint, *output, err)
		}
		written = true
	}
	sameDigestNoOp := current != nil && current.ManifestDigest == plan.DesiredManifestDigest &&
		plan.ActionCounts[clustermanifest.ActionNoOp] == clustermanifest.ExactNodeCount && len(plan.Actions) == clustermanifest.ExactNodeCount
	summary := manifestPlanSummary{
		Phase: "AD", Slice: "AD-IMPL-001C", Result: "ok", Entrypoint: entrypoint,
		ValidationBoundary: "phase_ad_cluster_manifest_no_mutation_plan",
		PlanID:             plan.PlanID, PlanDigest: plan.PlanDigest, DesiredManifestDigest: plan.DesiredManifestDigest,
		CurrentManifestDigest: plan.CurrentManifestDigest, ActionCounts: plan.ActionCounts, ActionCount: len(plan.Actions),
		ProjectedDaemonRestarts: plan.ProjectedDaemonRestarts,
		ExecutedTiKVMutations:   plan.ExecutedTiKVMutations, ExecutedDaemonActions: plan.ExecutedDaemonActions,
		ExecutedStorageMutations: plan.ExecutedStorageMutations, NoMutationDryRun: plan.NoMutationDryRun,
		SameDigestReapplyNoOp: sameDigestNoOp, PlanUnblocked: plan.PlanUnblocked, PlanArtifactWritten: written,
		RemoteLabUsed: false, Physical160Claimed: false, OKCount: 1,
	}
	if *output == "json" {
		writeJSON(summary)
		return
	}
	fmt.Printf("plan_id: %s\n", summary.PlanID)
	fmt.Printf("plan_digest: %s\n", summary.PlanDigest)
	for _, action := range []string{clustermanifest.ActionCreate, clustermanifest.ActionUpdate, clustermanifest.ActionRestart, clustermanifest.ActionNoOp, clustermanifest.ActionBlocked} {
		fmt.Printf("%s: %d\n", action, summary.ActionCounts[action])
	}
	fmt.Println("executed_tikv_mutations: 0")
	fmt.Println("executed_daemon_actions: 0")
}

type manifestExportSummary struct {
	Phase                   string `json:"phase"`
	Slice                   string `json:"slice"`
	Result                  string `json:"result"`
	Entrypoint              string `json:"entrypoint"`
	ValidationBoundary      string `json:"validation_boundary"`
	ManifestDigest          string `json:"manifest_digest"`
	Format                  string `json:"format"`
	Canonical               bool   `json:"canonical"`
	NodeCount               int    `json:"node_count"`
	TiKVMutationCount       int    `json:"tikv_mutation_count"`
	DaemonActionCount       int    `json:"daemon_action_count"`
	StorageMutationCount    int    `json:"storage_mutation_count"`
	ExistingFileOverwritten bool   `json:"existing_file_overwritten"`
	RemoteLabUsed           bool   `json:"remote_lab_used"`
	Physical160Claimed      bool   `json:"physical_160_claimed"`
	OKCount                 int    `json:"ok_count"`
	ErrorCount              int    `json:"error_count"`
	FirstError              string `json:"first_error"`
	LastError               string `json:"last_error"`
}

func runClusterManifestExport(args []string) {
	fs := flag.NewFlagSet("cluster manifest export", flag.ExitOnError)
	fs.SetOutput(os.Stderr)
	file := fs.String("file", "", "cluster manifest YAML path")
	outputFile := fs.String("output-file", "", "new path for canonical manifest export")
	format := fs.String("format", "yaml", "export format: yaml|json")
	output := fs.String("output", "table", "summary output format: table|json")
	var approved repeatedStringFlag
	fs.Var(&approved, "approved-artifact-digest", "externally approved sha256 digest; repeatable")
	parseCommandFlags(fs, args)
	entrypoint := "sbsctl cluster manifest export"
	if *output != "table" && *output != "json" {
		manifestCommandErrorFor("AD-IMPL-001C", entrypoint, *output, fmt.Errorf("unsupported output format %q", *output))
	}
	if strings.TrimSpace(*file) == "" || strings.TrimSpace(*outputFile) == "" {
		manifestCommandErrorFor("AD-IMPL-001C", entrypoint, *output, errors.New("--file and --output-file are required"))
	}
	manifest, err := clustermanifest.Load(*file)
	if err != nil {
		manifestCommandErrorFor("AD-IMPL-001C", entrypoint, *output, err)
	}
	canonical, err := clustermanifest.Canonicalize(manifest)
	if err != nil {
		manifestCommandErrorFor("AD-IMPL-001C", entrypoint, *output, err)
	}
	policy := clustermanifest.ValidationPolicy{ApprovedArtifactDigests: []string(approved), RequireArtifactApproval: true}
	if err := clustermanifest.Validate(canonical, policy); err != nil {
		manifestCommandErrorFor("AD-IMPL-001C", entrypoint, *output, err)
	}
	var raw []byte
	switch *format {
	case "yaml":
		raw, err = clustermanifest.CanonicalYAML(canonical)
	case "json":
		raw, err = clustermanifest.CanonicalJSON(canonical)
	default:
		err = fmt.Errorf("unsupported export format %q", *format)
	}
	if err != nil {
		manifestCommandErrorFor("AD-IMPL-001C", entrypoint, *output, err)
	}
	if err := clustermanifest.WriteNewFile(*outputFile, raw, 0o644); err != nil {
		manifestCommandErrorFor("AD-IMPL-001C", entrypoint, *output, err)
	}
	digest, err := clustermanifest.Digest(canonical)
	if err != nil {
		manifestCommandErrorFor("AD-IMPL-001C", entrypoint, *output, err)
	}
	summary := manifestExportSummary{
		Phase: "AD", Slice: "AD-IMPL-001C", Result: "ok", Entrypoint: entrypoint,
		ValidationBoundary: "phase_ad_cluster_manifest_canonical_export", ManifestDigest: digest,
		Format: *format, Canonical: true, NodeCount: len(canonical.Spec.Nodes),
		TiKVMutationCount: 0, DaemonActionCount: 0, StorageMutationCount: 0, ExistingFileOverwritten: false,
		RemoteLabUsed: false, Physical160Claimed: false, OKCount: 1,
	}
	if *output == "json" {
		writeJSON(summary)
		return
	}
	fmt.Printf("manifest_digest: %s\n", summary.ManifestDigest)
	fmt.Printf("format: %s\n", summary.Format)
	fmt.Println("canonical: true")
	fmt.Println("tikv_mutation_count: 0")
	fmt.Println("daemon_action_count: 0")
}

type manifestRenderSummary struct {
	Phase                string `json:"phase"`
	Slice                string `json:"slice"`
	Result               string `json:"result"`
	Entrypoint           string `json:"entrypoint"`
	ValidationBoundary   string `json:"validation_boundary"`
	ManifestDigest       string `json:"manifest_digest"`
	RenderDigest         string `json:"render_digest"`
	NodeBundleCount      int    `json:"node_bundle_count"`
	RenderedFileCount    int    `json:"rendered_file_count"`
	DataConfigCount      int    `json:"data_config_count"`
	ServiceConfigCount   int    `json:"service_config_count"`
	StoreConfigCount     int    `json:"store_config_count"`
	SystemdUnitCount     int    `json:"systemd_unit_count"`
	SecretLiteralCount   int    `json:"secret_literal_count"`
	ByteIdentical        bool   `json:"byte_identical_rerender"`
	TiKVMutationCount    int    `json:"tikv_mutation_count"`
	DaemonActionCount    int    `json:"daemon_action_count"`
	StorageMutationCount int    `json:"storage_mutation_count"`
	RemoteLabUsed        bool   `json:"remote_lab_used"`
	Physical160Claimed   bool   `json:"physical_160_claimed"`
	OKCount              int    `json:"ok_count"`
	ErrorCount           int    `json:"error_count"`
	FirstError           string `json:"first_error"`
	LastError            string `json:"last_error"`
}

func runClusterManifestRender(args []string) {
	fs := flag.NewFlagSet("cluster manifest render", flag.ExitOnError)
	fs.SetOutput(os.Stderr)
	file := fs.String("file", "", "cluster manifest YAML path")
	outputDir := fs.String("output-dir", "", "empty directory for deterministic node bundles")
	output := fs.String("output", "table", "output format: table|json")
	var approved repeatedStringFlag
	fs.Var(&approved, "approved-artifact-digest", "externally approved sha256 digest; repeatable")
	parseCommandFlags(fs, args)
	if *output != "table" && *output != "json" {
		manifestCommandErrorFor("AD-IMPL-001B", "sbsctl cluster manifest render", *output, fmt.Errorf("unsupported output format %q", *output))
	}
	if strings.TrimSpace(*file) == "" {
		manifestCommandErrorFor("AD-IMPL-001B", "sbsctl cluster manifest render", *output, errors.New("--file is required"))
	}
	if strings.TrimSpace(*outputDir) == "" {
		manifestCommandErrorFor("AD-IMPL-001B", "sbsctl cluster manifest render", *output, errors.New("--output-dir is required"))
	}
	manifest, err := clustermanifest.Load(*file)
	if err != nil {
		manifestCommandErrorFor("AD-IMPL-001B", "sbsctl cluster manifest render", *output, err)
	}
	policy := clustermanifest.ValidationPolicy{ApprovedArtifactDigests: []string(approved), RequireArtifactApproval: true}
	set, err := clustermanifest.Render(manifest, policy)
	if err != nil {
		manifestCommandErrorFor("AD-IMPL-001B", "sbsctl cluster manifest render", *output, err)
	}
	rerendered, err := clustermanifest.Render(manifest, policy)
	if err != nil {
		manifestCommandErrorFor("AD-IMPL-001B", "sbsctl cluster manifest render", *output, err)
	}
	if !clustermanifest.RenderSetsByteIdentical(set, rerendered) {
		manifestCommandErrorFor("AD-IMPL-001B", "sbsctl cluster manifest render", *output, errors.New("same manifest did not produce byte-identical bundles"))
	}
	if err := clustermanifest.WriteRenderSet(*outputDir, set); err != nil {
		manifestCommandErrorFor("AD-IMPL-001B", "sbsctl cluster manifest render", *output, err)
	}
	summary := manifestRenderSummary{
		Phase: "AD", Slice: "AD-IMPL-001B", Result: "ok", Entrypoint: "sbsctl cluster manifest render",
		ValidationBoundary: "phase_ad_cluster_manifest_deterministic_render",
		ManifestDigest:     set.ManifestDigest, RenderDigest: set.RenderDigest, NodeBundleCount: len(set.Bundles),
		ByteIdentical: true, TiKVMutationCount: 0, DaemonActionCount: 0, StorageMutationCount: 0,
		RemoteLabUsed: false, Physical160Claimed: false, OKCount: 1,
	}
	for _, bundle := range set.Bundles {
		for _, rendered := range bundle.Files {
			summary.RenderedFileCount++
			switch {
			case strings.HasSuffix(rendered.RelativePath, "/sbs-data.yaml"):
				summary.DataConfigCount++
			case strings.HasSuffix(rendered.RelativePath, "/sbs-service.yaml"):
				summary.ServiceConfigCount++
			case strings.HasSuffix(rendered.RelativePath, "/store-config.yaml"):
				summary.StoreConfigCount++
			case strings.HasSuffix(rendered.RelativePath, ".service"):
				summary.SystemdUnitCount++
			}
		}
	}
	if *output == "json" {
		writeJSON(summary)
		return
	}
	fmt.Printf("manifest_digest: %s\n", summary.ManifestDigest)
	fmt.Printf("render_digest: %s\n", summary.RenderDigest)
	fmt.Printf("node_bundles: %d\n", summary.NodeBundleCount)
	fmt.Printf("rendered_files: %d\n", summary.RenderedFileCount)
	fmt.Println("tikv_mutation_count: 0")
	fmt.Println("daemon_action_count: 0")
}

func runClusterManifestValidate(args []string) {
	fs := flag.NewFlagSet("cluster manifest validate", flag.ExitOnError)
	fs.SetOutput(os.Stderr)
	file := fs.String("file", "", "cluster manifest YAML path")
	output := fs.String("output", "table", "output format: table|json")
	var approved repeatedStringFlag
	fs.Var(&approved, "approved-artifact-digest", "externally approved sha256 digest; repeatable")
	parseCommandFlags(fs, args)
	if strings.TrimSpace(*file) == "" {
		manifestCommandError(*output, errors.New("--file is required"))
	}
	if *output != "table" && *output != "json" {
		manifestCommandError(*output, fmt.Errorf("unsupported output format %q", *output))
	}

	manifest, err := clustermanifest.Load(*file)
	if err != nil {
		manifestCommandError(*output, err)
	}
	canonical, err := clustermanifest.Canonicalize(manifest)
	if err != nil {
		manifestCommandError(*output, err)
	}
	policy := clustermanifest.ValidationPolicy{
		ApprovedArtifactDigests: []string(approved),
		RequireArtifactApproval: true,
	}
	if err := clustermanifest.Validate(canonical, policy); err != nil {
		manifestCommandError(*output, err)
	}
	digest, err := clustermanifest.Digest(canonical)
	if err != nil {
		manifestCommandError(*output, err)
	}

	zoneCounts := map[string]int{}
	for _, node := range canonical.Spec.Nodes {
		zoneCounts[node.Location.Zone]++
	}
	summary := manifestValidationSummary{
		Phase:                         "AD",
		Slice:                         "AD-IMPL-001A",
		Result:                        "ok",
		Entrypoint:                    "sbsctl cluster manifest validate",
		ValidationBoundary:            "phase_ad_cluster_manifest_pure_validation",
		ManifestName:                  canonical.Metadata.Name,
		ManifestRevision:              canonical.Metadata.Revision,
		ManifestDigest:                digest,
		ArtifactDigest:                canonical.Spec.Artifact.Digest,
		ArtifactApprovalChecked:       true,
		NodeCount:                     len(canonical.Spec.Nodes),
		ZoneCount:                     len(zoneCounts),
		NodesPerZone:                  zoneCounts,
		ActiveServiceHosts:            append([]string(nil), canonical.Spec.ServicePlacement.ActiveHosts...),
		StandbyServiceHosts:           append([]string(nil), canonical.Spec.ServicePlacement.StandbyCandidates...),
		CanonicalizationDeterministic: true,
		PureValidation:                true,
		TiKVMutationCount:             0,
		DaemonActionCount:             0,
		StorageMutationCount:          0,
		RemoteLabUsed:                 false,
		Physical160Claimed:            false,
		OKCount:                       1,
		ErrorCount:                    0,
	}
	if *output == "json" {
		writeJSON(summary)
		return
	}
	fmt.Printf("manifest: %s\n", summary.ManifestName)
	fmt.Printf("revision: %s\n", summary.ManifestRevision)
	fmt.Printf("digest: %s\n", summary.ManifestDigest)
	fmt.Printf("nodes: %d\n", summary.NodeCount)
	zones := make([]string, 0, len(zoneCounts))
	for zone := range zoneCounts {
		zones = append(zones, zone)
	}
	sort.Strings(zones)
	for _, zone := range zones {
		fmt.Printf("%s: %d\n", zone, zoneCounts[zone])
	}
	fmt.Println("pure_validation: true")
	fmt.Println("tikv_mutation_count: 0")
	fmt.Println("daemon_action_count: 0")
}

func manifestCommandError(output string, err error) {
	manifestCommandErrorFor("AD-IMPL-001A", "sbsctl cluster manifest validate", output, err)
}

func manifestCommandErrorFor(slice, entrypoint, output string, err error) {
	if output == "json" || globalJSONOutput {
		issues := []clustermanifest.Issue(nil)
		var validationErr *clustermanifest.ValidationError
		if errors.As(err, &validationErr) {
			issues = validationErr.Issues
		}
		writeJSON(map[string]any{
			"phase":                  "AD",
			"slice":                  slice,
			"result":                 "error",
			"entrypoint":             entrypoint,
			"pure_validation":        true,
			"tikv_mutation_count":    0,
			"daemon_action_count":    0,
			"storage_mutation_count": 0,
			"issues":                 issues,
			"ok_count":               0,
			"error_count":            1,
			"first_error":            err.Error(),
			"last_error":             err.Error(),
		})
		os.Exit(1)
	}
	fatalf("cluster manifest validation failed: %v", err)
}

func clusterManifestUsage() {
	fmt.Fprintln(os.Stderr, "usage: sbsctl cluster manifest validate|render|plan|export|admit|rollout|standby ...")
}
