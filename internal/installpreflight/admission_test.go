package installpreflight

import (
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/nosway/namrbd/internal/clustermanifest"
)

func TestBuildJoinPlanAcceptsExactSigned160NodeSet(t *testing.T) {
	request, _ := exactAdmissionRequest(t)
	plan, err := BuildJoinPlan(request)
	if err != nil {
		t.Fatalf("BuildJoinPlan: %v", err)
	}
	if plan.AcceptedReportCount != clustermanifest.ExactNodeCount || plan.ProjectedNodeJoinCount != clustermanifest.ExactNodeCount || plan.RejectedReportCount != 0 {
		t.Fatalf("join counts=%+v", plan)
	}
	if len(plan.Zones) != clustermanifest.ExactZoneCount {
		t.Fatalf("zones=%d", len(plan.Zones))
	}
	for i, zone := range plan.Zones {
		if got, want := zone.Zone, fmt.Sprintf("zone-%02d", i+1); got != want {
			t.Fatalf("zone[%d]=%q want %q", i, got, want)
		}
		if len(zone.Nodes) != clustermanifest.ExactNodesPerZone {
			t.Fatalf("zone %s nodes=%d", zone.Zone, len(zone.Nodes))
		}
	}
	if plan.Zones[0].Nodes[0].NodeID != "node1" || plan.Zones[7].Nodes[19].NodeID != "node160" {
		t.Fatalf("unexpected exact node boundaries: first=%s last=%s", plan.Zones[0].Nodes[0].NodeID, plan.Zones[7].Nodes[19].NodeID)
	}
	if !plan.NoMutationAdmission || plan.ExecutedTiKVMutations != 0 || plan.ExecutedDaemonActions != 0 || plan.ExecutedStorageMutations != 0 {
		t.Fatalf("admission plan reported mutations: %+v", plan)
	}
	second, err := BuildJoinPlan(request)
	if err != nil {
		t.Fatalf("BuildJoinPlan second: %v", err)
	}
	if plan.JoinPlanID != second.JoinPlanID || plan.JoinPlanDigest != second.JoinPlanDigest {
		t.Fatalf("join plan changed: %s/%s != %s/%s", plan.JoinPlanID, plan.JoinPlanDigest, second.JoinPlanID, second.JoinPlanDigest)
	}
}

func TestBuildJoinPlanRejectsUntrustedStaleOrMismatchedReports(t *testing.T) {
	tests := []struct {
		name string
		code string
		edit func(*testing.T, *AdmissionRequest, map[string]ed25519.PrivateKey)
	}{
		{name: "stale", code: CodeReportStale, edit: func(t *testing.T, r *AdmissionRequest, keys map[string]ed25519.PrivateKey) {
			r.Reports[0].Report.ObservedAt = r.AdmissionTime.Add(-10 * time.Minute)
			resignAdmissionReport(t, &r.Reports[0].Report, "host-node1", keys["node1"], &r.Reports[0])
		}},
		{name: "wrong manifest digest", code: CodeReportManifestDigest, edit: func(t *testing.T, r *AdmissionRequest, keys map[string]ed25519.PrivateKey) {
			r.Reports[0].Report.ManifestDigest = "sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"
			resignAdmissionReport(t, &r.Reports[0].Report, "host-node1", keys["node1"], &r.Reports[0])
		}},
		{name: "wrong plan id", code: CodeReportPlanID, edit: func(t *testing.T, r *AdmissionRequest, keys map[string]ed25519.PrivateKey) {
			r.Reports[0].Report.PlanID = "ad-plan-0000000000000000"
			resignAdmissionReport(t, &r.Reports[0].Report, "host-node1", keys["node1"], &r.Reports[0])
		}},
		{name: "wrong bundle digest", code: CodeReportBundleDigest, edit: func(t *testing.T, r *AdmissionRequest, keys map[string]ed25519.PrivateKey) {
			r.Reports[0].Report.BundleDigest = "sha256:eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
			resignAdmissionReport(t, &r.Reports[0].Report, "host-node1", keys["node1"], &r.Reports[0])
		}},
		{name: "fixture evidence", code: CodeReportEvidenceSource, edit: func(t *testing.T, r *AdmissionRequest, keys map[string]ed25519.PrivateKey) {
			r.Reports[0].Report.EvidenceSource = "fixture-file:fixture"
			resignAdmissionReport(t, &r.Reports[0].Report, "host-node1", keys["node1"], &r.Reports[0])
		}},
		{name: "incomplete check set", code: CodeReportCheckSet, edit: func(t *testing.T, r *AdmissionRequest, keys map[string]ed25519.PrivateKey) {
			report := &r.Reports[0].Report
			report.Checks = nil
			report.PassCount, report.ErrorCount = 0, 0
			report.Result, report.FirstError, report.LastError = ResultOK, "", ""
			resignAdmissionReport(t, report, "host-node1", keys["node1"], &r.Reports[0])
		}},
		{name: "blocked report", code: CodeReportResult, edit: func(t *testing.T, r *AdmissionRequest, keys map[string]ed25519.PrivateKey) {
			report := &r.Reports[0].Report
			report.Checks[0].Status = StatusFail
			report.Checks[0].Message = "fixture rejection"
			resetAndFinalizeReport(report)
			signed, err := SignReport(report, "host-node1", keys["node1"])
			if err != nil {
				t.Fatal(err)
			}
			r.Reports[0] = signed
		}},
		{name: "untrusted self signed", code: CodeReportUntrustedKey, edit: func(t *testing.T, r *AdmissionRequest, _ map[string]ed25519.PrivateKey) {
			seed := sha256.Sum256([]byte("untrusted-node1"))
			key := ed25519.NewKeyFromSeed(seed[:])
			resignAdmissionReport(t, &r.Reports[0].Report, "untrusted-node1", key, &r.Reports[0])
		}},
		{name: "wrong signer node binding", code: CodeReportSignerBinding, edit: func(t *testing.T, r *AdmissionRequest, keys map[string]ed25519.PrivateKey) {
			resignAdmissionReport(t, &r.Reports[0].Report, "host-node2", keys["node2"], &r.Reports[0])
		}},
		{name: "tampered signature", code: CodeReportSignature, edit: func(_ *testing.T, r *AdmissionRequest, _ map[string]ed25519.PrivateKey) {
			r.Reports[0].Signature = base64.StdEncoding.EncodeToString(make([]byte, ed25519.SignatureSize))
		}},
		{name: "duplicate report", code: CodeReportDuplicateNode, edit: func(_ *testing.T, r *AdmissionRequest, _ map[string]ed25519.PrivateKey) {
			r.Reports = append(r.Reports, r.Reports[0])
		}},
		{name: "missing report", code: CodeReportMissingNode, edit: func(_ *testing.T, r *AdmissionRequest, _ map[string]ed25519.PrivateKey) {
			r.Reports = r.Reports[:len(r.Reports)-1]
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			request, keys := exactAdmissionRequest(t)
			test.edit(t, &request, keys)
			plan, err := BuildJoinPlan(request)
			if err == nil || plan != nil {
				t.Fatalf("BuildJoinPlan unexpectedly accepted plan=%+v", plan)
			}
			admissionErr, ok := err.(*AdmissionError)
			if !ok {
				t.Fatalf("error type=%T err=%v", err, err)
			}
			if !hasAdmissionIssue(admissionErr, test.code) {
				t.Fatalf("issues=%+v, want %s", admissionErr.Issues, test.code)
			}
		})
	}
}

func TestParseJoinPlanIsStrictAndDigestBound(t *testing.T) {
	request, _ := exactAdmissionRequest(t)
	plan, err := BuildJoinPlan(request)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := MarshalJoinPlan(plan)
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := ParseJoinPlan(raw)
	if err != nil {
		t.Fatal(err)
	}
	if parsed.JoinPlanID != plan.JoinPlanID || parsed.JoinPlanDigest != plan.JoinPlanDigest {
		t.Fatalf("parsed join plan=%+v", parsed)
	}
	withUnknown := strings.Replace(string(raw), "{", "{\"unknown\":true,", 1)
	if _, err := ParseJoinPlan([]byte(withUnknown)); err == nil || !strings.Contains(err.Error(), "unknown field") {
		t.Fatalf("unknown field error=%v", err)
	}
	var object map[string]any
	if err := json.Unmarshal(raw, &object); err != nil {
		t.Fatal(err)
	}
	object["manifest_digest"] = "sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"
	tampered, _ := json.Marshal(object)
	if _, err := ParseJoinPlan(tampered); err == nil || !strings.Contains(err.Error(), "digest") {
		t.Fatalf("tampered join plan error=%v", err)
	}
}

func exactAdmissionRequest(t *testing.T) (AdmissionRequest, map[string]ed25519.PrivateKey) {
	t.Helper()
	manifest, err := clustermanifest.Load(filepath.Join("..", "..", "configs", "sbs-cluster-160.example.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	policy := clustermanifest.ValidationPolicy{ApprovedArtifactDigests: []string{testArtifactDigest}, RequireArtifactApproval: true}
	rendered, err := clustermanifest.Render(manifest, policy)
	if err != nil {
		t.Fatal(err)
	}
	manifestPlan, err := clustermanifest.BuildPlan(manifest, nil, policy)
	if err != nil {
		t.Fatal(err)
	}
	canonical, err := clustermanifest.Canonicalize(manifest)
	if err != nil {
		t.Fatal(err)
	}
	bundles := map[string]clustermanifest.NodeBundle{}
	for _, bundle := range rendered.Bundles {
		bundles[bundle.NodeID] = bundle
	}
	admissionTime := time.Date(2026, 9, 3, 6, 0, 0, 0, time.UTC)
	trust := &TrustBundle{APIVersion: APIVersion, Kind: TrustBundleKind}
	reports := make([]*SignedReport, 0, len(canonical.Spec.Nodes))
	keys := make(map[string]ed25519.PrivateKey, len(canonical.Spec.Nodes))
	for _, node := range canonical.Spec.Nodes {
		seed := sha256.Sum256([]byte("phase-ad-admission-fixture/" + node.ID))
		privateKey := ed25519.NewKeyFromSeed(seed[:])
		keys[node.ID] = privateKey
		keyID := "host-" + node.ID
		trust.Signers = append(trust.Signers, TrustedSigner{
			KeyID: keyID, NodeID: node.ID,
			PublicKey: base64.StdEncoding.EncodeToString(privateKey.Public().(ed25519.PublicKey)),
		})
		report := exactPassingAdmissionReport(manifestPlan.PlanID, rendered.ManifestDigest, canonical, node, bundles[node.ID], admissionTime.Add(-time.Minute))
		signed, err := SignReport(report, keyID, privateKey)
		if err != nil {
			t.Fatalf("SignReport %s: %v", node.ID, err)
		}
		reports = append(reports, signed)
	}
	return AdmissionRequest{
		Manifest: manifest, Policy: policy, PlanID: manifestPlan.PlanID, Reports: reports,
		TrustBundle: trust, AdmissionTime: admissionTime,
		MaxReportAge: DefaultMaxReportAge, MaxFutureSkew: DefaultFutureSkew,
	}, keys
}

func exactPassingAdmissionReport(planID, manifestDigest string, manifest *clustermanifest.Manifest, node clustermanifest.Node, bundle clustermanifest.NodeBundle, observedAt time.Time) *Report {
	report := &Report{
		APIVersion: APIVersion, Kind: ReportKind, PlanID: planID,
		ManifestDigest: manifestDigest, BundleDigest: bundle.BundleDigest, NodeID: node.ID,
		ObservedAt: observedAt, ReferenceTime: observedAt, EvidenceSource: "local-os",
	}
	add := func(id, subject string) {
		report.Checks = append(report.Checks, Check{ID: id, Subject: subject, Status: StatusPass})
	}
	add(CheckHostname, node.ID)
	add(CheckClock, node.ID)
	add(CheckKernel, node.ID)
	add(CheckManagementNetwork, node.ManagementAddress)
	add(CheckDataNetwork, node.DataAddress)
	for _, endpoint := range []string{node.DataEndpoint, node.AdminEndpoint, node.MetricsEndpoint, node.ServiceGRPCEndpoint, node.ServiceHTTPEndpoint, node.ServiceMetricsEndpoint} {
		if endpoint != "" {
			add(CheckPort, endpoint)
		}
	}
	add(CheckBundleFileSet, node.ID)
	for _, file := range bundle.Files {
		add(CheckBundleFile, file.RelativePath)
		add(CheckInstalledFile, file.InstallPath)
	}
	add(CheckBinary, filepath.Join(manifest.Spec.Bundle.BinaryDirectory, "sbs-data"))
	if containsRole(node.Roles, "sbs-service-active") || containsRole(node.Roles, "sbs-service-standby") {
		add(CheckBinary, filepath.Join(manifest.Spec.Bundle.BinaryDirectory, "sbs-service"))
	}
	for _, claim := range node.StorageClaims {
		add(CheckDevice, claim.ID)
		add(CheckFilesystemUUID, claim.ID)
		add(CheckMount, claim.ID)
		add(CheckMountOwnership, claim.ID)
		add(CheckCapacity, claim.ID)
	}
	finalizeReport(report)
	return report
}

func resignAdmissionReport(t *testing.T, report *Report, keyID string, privateKey ed25519.PrivateKey, target **SignedReport) {
	t.Helper()
	report.ReportDigest = reportDigest(report)
	signed, err := SignReport(report, keyID, privateKey)
	if err != nil {
		t.Fatal(err)
	}
	*target = signed
}

func resetAndFinalizeReport(report *Report) {
	report.Result = ""
	report.PassCount, report.ErrorCount = 0, 0
	report.FirstError, report.LastError, report.ReportDigest = "", "", ""
	finalizeReport(report)
}

func hasAdmissionIssue(err *AdmissionError, code string) bool {
	for _, issue := range err.Issues {
		if issue.Code == code {
			return true
		}
	}
	return false
}

func cloneSignedReports(t *testing.T, reports []*SignedReport) []*SignedReport {
	t.Helper()
	raw, err := json.Marshal(reports)
	if err != nil {
		t.Fatal(err)
	}
	var clone []*SignedReport
	if err := json.Unmarshal(raw, &clone); err != nil {
		t.Fatal(err)
	}
	return clone
}
