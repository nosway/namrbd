package main

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/nosway/namrbd/internal/clustermanifest"
	"github.com/nosway/namrbd/internal/installpreflight"
)

type manifestAdmissionSummary struct {
	Phase                    string                            `json:"phase"`
	Slice                    string                            `json:"slice"`
	Result                   string                            `json:"result"`
	Entrypoint               string                            `json:"entrypoint"`
	ValidationBoundary       string                            `json:"validation_boundary"`
	PlanID                   string                            `json:"plan_id"`
	JoinPlanID               string                            `json:"join_plan_id"`
	JoinPlanDigest           string                            `json:"join_plan_digest"`
	ManifestDigest           string                            `json:"manifest_digest"`
	AcceptedReportCount      int                               `json:"accepted_report_count"`
	RejectedReportCount      int                               `json:"rejected_report_count"`
	ZoneCount                int                               `json:"zone_count"`
	NodesPerZone             map[string]int                    `json:"nodes_per_zone"`
	JoinPlanArtifactWritten  bool                              `json:"join_plan_artifact_written"`
	NoMutationAdmission      bool                              `json:"no_mutation_admission"`
	ExecutedTiKVMutations    int                               `json:"executed_tikv_mutations"`
	ExecutedDaemonActions    int                               `json:"executed_daemon_actions"`
	ExecutedStorageMutations int                               `json:"executed_storage_mutations"`
	Issues                   []installpreflight.AdmissionIssue `json:"issues,omitempty"`
	RemoteLabUsed            bool                              `json:"remote_lab_used"`
	Physical160Claimed       bool                              `json:"physical_160_claimed"`
	OKCount                  int                               `json:"ok_count"`
	ErrorCount               int                               `json:"error_count"`
	FirstError               string                            `json:"first_error"`
	LastError                string                            `json:"last_error"`
}

func runClusterManifestAdmission(args []string) {
	fs := flag.NewFlagSet("cluster manifest admit", flag.ExitOnError)
	fs.SetOutput(os.Stderr)
	file := fs.String("file", "", "reviewed cluster manifest YAML path")
	planID := fs.String("plan-id", "", "deterministic reviewed manifest plan identity")
	reportsDirectory := fs.String("reports-dir", "", "directory containing only signed host report JSON files")
	trustBundleFile := fs.String("trust-bundle", "", "reviewed node-to-Ed25519 public key trust bundle JSON")
	admissionTimeValue := fs.String("admission-time", "", "coordinator admission time in RFC3339 format")
	maxReportAge := fs.Duration("max-report-age", installpreflight.DefaultMaxReportAge, "maximum signed report age")
	maxFutureSkew := fs.Duration("max-future-skew", installpreflight.DefaultFutureSkew, "maximum future report timestamp skew")
	joinPlanOutput := fs.String("join-plan-output", "", "new path for the admitted no-mutation join plan")
	output := fs.String("output", "table", "summary output format: table|json")
	var approved repeatedStringFlag
	fs.Var(&approved, "approved-artifact-digest", "externally approved sha256 digest; repeatable")
	parseCommandFlags(fs, args)
	entrypoint := "sbsctl cluster manifest admit"
	if *output != "table" && *output != "json" {
		manifestAdmissionError(*output, fmt.Errorf("unsupported output format %q", *output))
	}
	for name, value := range map[string]string{
		"--file": *file, "--plan-id": *planID, "--reports-dir": *reportsDirectory,
		"--trust-bundle": *trustBundleFile, "--admission-time": *admissionTimeValue,
		"--join-plan-output": *joinPlanOutput,
	} {
		if strings.TrimSpace(value) == "" {
			manifestAdmissionError(*output, fmt.Errorf("%s is required", name))
		}
	}
	admissionTime, err := time.Parse(time.RFC3339, *admissionTimeValue)
	if err != nil {
		manifestAdmissionError(*output, fmt.Errorf("parse --admission-time: %w", err))
	}
	manifest, err := clustermanifest.Load(*file)
	if err != nil {
		manifestAdmissionError(*output, err)
	}
	trustRaw, err := os.ReadFile(*trustBundleFile)
	if err != nil {
		manifestAdmissionError(*output, fmt.Errorf("read trust bundle: %w", err))
	}
	trustBundle, err := installpreflight.ParseTrustBundle(trustRaw)
	if err != nil {
		manifestAdmissionError(*output, err)
	}
	reports, err := loadSignedReportDirectory(*reportsDirectory)
	if err != nil {
		manifestAdmissionError(*output, err)
	}
	policy := clustermanifest.ValidationPolicy{ApprovedArtifactDigests: []string(approved), RequireArtifactApproval: true}
	joinPlan, err := installpreflight.BuildJoinPlan(installpreflight.AdmissionRequest{
		Manifest: manifest, Policy: policy, PlanID: strings.TrimSpace(*planID), Reports: reports,
		TrustBundle: trustBundle, AdmissionTime: admissionTime,
		MaxReportAge: *maxReportAge, MaxFutureSkew: *maxFutureSkew,
	})
	if err != nil {
		manifestAdmissionError(*output, err)
	}
	raw, err := installpreflight.MarshalJoinPlan(joinPlan)
	if err != nil {
		manifestAdmissionError(*output, err)
	}
	if err := clustermanifest.WriteNewFile(*joinPlanOutput, raw, 0o640); err != nil {
		manifestAdmissionError(*output, err)
	}
	nodesPerZone := map[string]int{}
	for _, zone := range joinPlan.Zones {
		nodesPerZone[zone.Zone] = len(zone.Nodes)
	}
	summary := manifestAdmissionSummary{
		Phase: "AD", Slice: "AD-IMPL-002B", Result: "ok", Entrypoint: entrypoint,
		ValidationBoundary: "phase_ad_signed_host_report_central_admission",
		PlanID:             joinPlan.PlanID, JoinPlanID: joinPlan.JoinPlanID, JoinPlanDigest: joinPlan.JoinPlanDigest,
		ManifestDigest: joinPlan.ManifestDigest, AcceptedReportCount: joinPlan.AcceptedReportCount,
		RejectedReportCount: joinPlan.RejectedReportCount, ZoneCount: len(joinPlan.Zones),
		NodesPerZone: nodesPerZone, JoinPlanArtifactWritten: true, NoMutationAdmission: true,
		ExecutedTiKVMutations: 0, ExecutedDaemonActions: 0, ExecutedStorageMutations: 0,
		RemoteLabUsed: false, Physical160Claimed: false, OKCount: 1,
	}
	if *output == "json" || globalJSONOutput {
		writeJSON(summary)
		return
	}
	fmt.Printf("join_plan_id: %s\n", summary.JoinPlanID)
	fmt.Printf("accepted_reports: %d\n", summary.AcceptedReportCount)
	for _, zone := range joinPlan.Zones {
		fmt.Printf("%s: %d\n", zone.Zone, len(zone.Nodes))
	}
	fmt.Println("executed_tikv_mutations: 0")
	fmt.Println("executed_daemon_actions: 0")
}

func loadSignedReportDirectory(directory string) ([]*installpreflight.SignedReport, error) {
	entries, err := os.ReadDir(directory)
	if err != nil {
		return nil, fmt.Errorf("read signed report directory: %w", err)
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Name() < entries[j].Name() })
	reports := make([]*installpreflight.SignedReport, 0, len(entries))
	for _, entry := range entries {
		if filepath.Ext(entry.Name()) != ".json" {
			return nil, fmt.Errorf("signed report directory contains unexpected entry %q", entry.Name())
		}
		info, err := entry.Info()
		if err != nil {
			return nil, fmt.Errorf("inspect signed report %q: %w", entry.Name(), err)
		}
		if !info.Mode().IsRegular() {
			return nil, fmt.Errorf("signed report %q must be a regular file without symlinks", entry.Name())
		}
		raw, err := os.ReadFile(filepath.Join(directory, entry.Name()))
		if err != nil {
			return nil, fmt.Errorf("read signed report %q: %w", entry.Name(), err)
		}
		report, err := installpreflight.ParseSignedReport(raw)
		if err != nil {
			return nil, fmt.Errorf("parse signed report %q: %w", entry.Name(), err)
		}
		reports = append(reports, report)
	}
	return reports, nil
}

func manifestAdmissionError(output string, err error) {
	issues := []installpreflight.AdmissionIssue(nil)
	var admissionErr *installpreflight.AdmissionError
	if errors.As(err, &admissionErr) {
		issues = admissionErr.Issues
	} else {
		issues = []installpreflight.AdmissionIssue{{Code: "AD_ADMISSION_INPUT", Message: err.Error()}}
	}
	firstError, lastError := err.Error(), err.Error()
	if len(issues) > 0 {
		firstError = issues[0].Code + ": " + issues[0].Message
		last := issues[len(issues)-1]
		lastError = last.Code + ": " + last.Message
	}
	if output == "json" || globalJSONOutput {
		nodesPerZone := map[string]int(nil)
		acceptedReportCount, rejectedReportCount := 0, 0
		if admissionErr != nil {
			nodesPerZone = admissionErr.AdmittedNodesByZone
			acceptedReportCount = admissionErr.AcceptedReportCount
			rejectedReportCount = admissionErr.RejectedReportCount
		}
		writeJSON(manifestAdmissionSummary{
			Phase: "AD", Slice: "AD-IMPL-002B", Result: "blocked",
			Entrypoint:          "sbsctl cluster manifest admit",
			ValidationBoundary:  "phase_ad_signed_host_report_central_admission",
			AcceptedReportCount: acceptedReportCount, RejectedReportCount: rejectedReportCount,
			NodesPerZone: nodesPerZone, Issues: issues, NoMutationAdmission: true,
			ExecutedTiKVMutations: 0, ExecutedDaemonActions: 0, ExecutedStorageMutations: 0,
			RemoteLabUsed: false, Physical160Claimed: false,
			ErrorCount: len(issues), FirstError: firstError, LastError: lastError,
		})
		os.Exit(1)
	}
	fatalf("cluster manifest report admission failed: %v", err)
}
