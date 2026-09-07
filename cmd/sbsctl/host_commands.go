package main

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/nosway/namrbd/internal/clustermanifest"
	"github.com/nosway/namrbd/internal/installpreflight"
)

type hostCheckSummary struct {
	Phase                string `json:"phase"`
	Slice                string `json:"slice"`
	Result               string `json:"result"`
	Entrypoint           string `json:"entrypoint"`
	ValidationBoundary   string `json:"validation_boundary"`
	PlanID               string `json:"plan_id"`
	ManifestDigest       string `json:"manifest_digest"`
	BundleDigest         string `json:"bundle_digest"`
	ReportDigest         string `json:"report_digest"`
	SignedReportDigest   string `json:"signed_report_digest,omitempty"`
	SigningKeyID         string `json:"signing_key_id,omitempty"`
	NodeID               string `json:"node_id"`
	EvidenceSource       string `json:"evidence_source"`
	PassCount            int    `json:"pass_count"`
	ErrorCount           int    `json:"error_count"`
	FirstError           string `json:"first_error"`
	LastError            string `json:"last_error"`
	ReportWritten        bool   `json:"report_written"`
	SignedReportWritten  bool   `json:"signed_report_written"`
	TiKVMutationCount    int    `json:"tikv_mutation_count"`
	DaemonActionCount    int    `json:"daemon_action_count"`
	StorageMutationCount int    `json:"storage_mutation_count"`
	RemoteLabUsed        bool   `json:"remote_lab_used"`
	Physical160Claimed   bool   `json:"physical_160_claimed"`
}

func runHost(args []string) {
	if len(args) < 1 {
		hostUsage()
		os.Exit(2)
	}
	switch args[0] {
	case "check":
		runHostCheck(args[1:])
	case "maintenance":
		runHostMaintenance(args[1:])
	default:
		hostUsage()
		os.Exit(2)
	}
}

func runHostCheck(args []string) {
	fs := flag.NewFlagSet("host check", flag.ExitOnError)
	fs.SetOutput(os.Stderr)
	manifestFile := fs.String("manifest", "", "reviewed cluster manifest YAML path")
	nodeID := fs.String("node-id", "", "exact local node id")
	planID := fs.String("plan-id", "", "manifest plan identity")
	bundleDirectory := fs.String("bundle", "", "node-specific rendered bundle directory")
	reportOutput := fs.String("report-output", "", "new path for the full preflight report")
	signedReportOutput := fs.String("signed-report-output", "", "optional new path for an Ed25519 signed report envelope")
	signingPrivateKey := fs.String("signing-private-key", "", "owner-only PKCS#8 Ed25519 private key PEM")
	signingKeyID := fs.String("signing-key-id", "", "host key identity bound by the central trust bundle")
	local := fs.Bool("local", false, "collect read-only facts from this host")
	factsFile := fs.String("facts-file", "", "fixture-only observed host facts JSON instead of --local")
	referenceTimeValue := fs.String("reference-time", "", "coordinator reference time in RFC3339 format")
	maxClockSkew := fs.Duration("max-clock-skew", installpreflight.DefaultMaxClockSkew, "maximum absolute clock skew")
	minimumMTU := fs.Int("minimum-mtu", installpreflight.DefaultMinimumMTU, "minimum management and data interface MTU")
	output := fs.String("output", "table", "summary output format: table|json")
	var approved repeatedStringFlag
	fs.Var(&approved, "approved-artifact-digest", "externally approved sha256 digest; repeatable")
	parseCommandFlags(fs, args)
	entrypoint := "sbsctl host check"
	if *output != "table" && *output != "json" {
		hostCheckError(*output, fmt.Errorf("unsupported output format %q", *output))
	}
	if strings.TrimSpace(*manifestFile) == "" || strings.TrimSpace(*nodeID) == "" || strings.TrimSpace(*planID) == "" || strings.TrimSpace(*bundleDirectory) == "" || strings.TrimSpace(*reportOutput) == "" || strings.TrimSpace(*referenceTimeValue) == "" {
		hostCheckError(*output, errors.New("--manifest, --node-id, --plan-id, --bundle, --report-output, and --reference-time are required"))
	}
	if *local == (strings.TrimSpace(*factsFile) != "") {
		hostCheckError(*output, errors.New("exactly one of --local or --facts-file is required"))
	}
	signingFieldCount := 0
	for _, value := range []string{*signedReportOutput, *signingPrivateKey, *signingKeyID} {
		if strings.TrimSpace(value) != "" {
			signingFieldCount++
		}
	}
	if signingFieldCount != 0 && signingFieldCount != 3 {
		hostCheckError(*output, errors.New("--signed-report-output, --signing-private-key, and --signing-key-id must be provided together"))
	}
	referenceTime, err := time.Parse(time.RFC3339, *referenceTimeValue)
	if err != nil {
		hostCheckError(*output, fmt.Errorf("parse --reference-time: %w", err))
	}
	manifest, err := clustermanifest.Load(*manifestFile)
	if err != nil {
		hostCheckError(*output, err)
	}
	policy := clustermanifest.ValidationPolicy{ApprovedArtifactDigests: []string(approved), RequireArtifactApproval: true}
	var facts *installpreflight.HostFacts
	if *local {
		facts, err = installpreflight.CollectLocalFacts(installpreflight.CollectRequest{
			Manifest: manifest, Policy: policy, NodeID: strings.TrimSpace(*nodeID), HostRoot: "/",
		})
	} else {
		raw, readErr := os.ReadFile(*factsFile)
		if readErr != nil {
			err = fmt.Errorf("read host facts %s: %w", *factsFile, readErr)
		} else {
			facts, err = installpreflight.ParseFacts(raw)
			if err == nil {
				facts.EvidenceSource = "fixture-file:" + facts.EvidenceSource
			}
		}
	}
	if err != nil {
		hostCheckError(*output, err)
	}
	report, err := installpreflight.CheckHost(installpreflight.Request{
		Manifest: manifest, Policy: policy, NodeID: strings.TrimSpace(*nodeID), PlanID: strings.TrimSpace(*planID),
		BundleDirectory: *bundleDirectory, ReferenceTime: referenceTime, MaxClockSkew: *maxClockSkew,
		MinimumMTU: *minimumMTU, Facts: *facts,
	})
	if err != nil {
		hostCheckError(*output, err)
	}
	raw, err := installpreflight.MarshalReport(report)
	if err != nil {
		hostCheckError(*output, err)
	}
	if err := clustermanifest.WriteNewFile(*reportOutput, raw, 0o640); err != nil {
		hostCheckError(*output, err)
	}
	signedWritten := false
	signedDigest := ""
	if signingFieldCount == 3 {
		privateKey, err := installpreflight.LoadEd25519PrivateKey(*signingPrivateKey)
		if err != nil {
			hostCheckError(*output, err)
		}
		signed, err := installpreflight.SignReport(report, strings.TrimSpace(*signingKeyID), privateKey)
		if err != nil {
			hostCheckError(*output, err)
		}
		signedRaw, err := installpreflight.MarshalSignedReport(signed)
		if err != nil {
			hostCheckError(*output, err)
		}
		if err := clustermanifest.WriteNewFile(*signedReportOutput, signedRaw, 0o640); err != nil {
			hostCheckError(*output, err)
		}
		signedDigest, err = installpreflight.SignedReportDigest(signed)
		if err != nil {
			hostCheckError(*output, err)
		}
		signedWritten = true
	}
	summary := hostCheckSummary{
		Phase: "AD", Slice: "AD-IMPL-002A", Result: report.Result, Entrypoint: entrypoint,
		ValidationBoundary: "phase_ad_node_local_host_storage_preflight",
		PlanID:             report.PlanID, ManifestDigest: report.ManifestDigest, BundleDigest: report.BundleDigest,
		ReportDigest: report.ReportDigest, SignedReportDigest: signedDigest, SigningKeyID: strings.TrimSpace(*signingKeyID),
		NodeID: report.NodeID, EvidenceSource: report.EvidenceSource,
		PassCount: report.PassCount, ErrorCount: report.ErrorCount, FirstError: report.FirstError, LastError: report.LastError,
		ReportWritten: true, SignedReportWritten: signedWritten,
		TiKVMutationCount: 0, DaemonActionCount: 0, StorageMutationCount: 0,
		RemoteLabUsed: false, Physical160Claimed: false,
	}
	if *output == "json" || globalJSONOutput {
		writeJSON(summary)
	} else {
		fmt.Printf("result: %s\n", summary.Result)
		fmt.Printf("node_id: %s\n", summary.NodeID)
		fmt.Printf("report_digest: %s\n", summary.ReportDigest)
		fmt.Printf("pass_count: %d\n", summary.PassCount)
		fmt.Printf("error_count: %d\n", summary.ErrorCount)
	}
	if report.Result != installpreflight.ResultOK {
		os.Exit(1)
	}
}

func hostCheckError(output string, err error) {
	if output == "json" || globalJSONOutput {
		writeJSON(map[string]any{
			"phase": "AD", "slice": "AD-IMPL-002A", "result": "error", "entrypoint": "sbsctl host check",
			"validation_boundary": "phase_ad_node_local_host_storage_preflight",
			"tikv_mutation_count": 0, "daemon_action_count": 0, "storage_mutation_count": 0,
			"remote_lab_used": false, "physical_160_claimed": false,
			"pass_count": 0, "error_count": 1, "first_error": err.Error(), "last_error": err.Error(),
		})
		os.Exit(1)
	}
	fatalf("host preflight failed: %v", err)
}
