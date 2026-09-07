package installpreflight

import (
	"bytes"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/nosway/namrbd/internal/clustermanifest"
)

const (
	JoinPlanKind        = "SBSHostJoinPlan"
	DefaultMaxReportAge = 5 * time.Minute
	DefaultFutureSkew   = 30 * time.Second
)

const (
	CodeReportCount          = "AD_REPORT_COUNT"
	CodeReportEnvelope       = "AD_REPORT_ENVELOPE"
	CodeReportIntegrity      = "AD_REPORT_INTEGRITY"
	CodeReportUntrustedKey   = "AD_REPORT_UNTRUSTED_KEY"
	CodeReportSignature      = "AD_REPORT_SIGNATURE"
	CodeReportSignerBinding  = "AD_REPORT_SIGNER_NODE_BINDING"
	CodeReportDuplicateNode  = "AD_REPORT_DUPLICATE_NODE"
	CodeReportUnknownNode    = "AD_REPORT_UNKNOWN_NODE"
	CodeReportPlanID         = "AD_REPORT_PLAN_ID"
	CodeReportManifestDigest = "AD_REPORT_MANIFEST_DIGEST"
	CodeReportBundleDigest   = "AD_REPORT_BUNDLE_DIGEST"
	CodeReportEvidenceSource = "AD_REPORT_EVIDENCE_SOURCE"
	CodeReportCheckSet       = "AD_REPORT_CHECK_SET"
	CodeReportResult         = "AD_REPORT_RESULT"
	CodeReportStale          = "AD_REPORT_STALE"
	CodeReportFuture         = "AD_REPORT_FUTURE"
	CodeReportMutationClaim  = "AD_REPORT_MUTATION_CLAIM"
	CodeReportMissingNode    = "AD_REPORT_MISSING_NODE"
	CodeReportZoneCount      = "AD_REPORT_ZONE_COUNT"
)

type AdmissionRequest struct {
	Manifest      *clustermanifest.Manifest
	Policy        clustermanifest.ValidationPolicy
	PlanID        string
	Reports       []*SignedReport
	TrustBundle   *TrustBundle
	AdmissionTime time.Time
	MaxReportAge  time.Duration
	MaxFutureSkew time.Duration
}

type AdmissionIssue struct {
	Code        string `json:"code"`
	ReportIndex int    `json:"report_index,omitempty"`
	NodeID      string `json:"node_id,omitempty"`
	KeyID       string `json:"key_id,omitempty"`
	Message     string `json:"message"`
}

type AdmissionError struct {
	Issues              []AdmissionIssue `json:"issues"`
	AcceptedReportCount int              `json:"accepted_report_count"`
	RejectedReportCount int              `json:"rejected_report_count"`
	AdmittedNodesByZone map[string]int   `json:"admitted_nodes_by_zone"`
}

func (e *AdmissionError) Error() string {
	if e == nil || len(e.Issues) == 0 {
		return "host preflight report admission failed"
	}
	first := e.Issues[0]
	return fmt.Sprintf("host preflight report admission failed: %s: %s", first.Code, first.Message)
}

type JoinPlan struct {
	APIVersion               string     `json:"api_version"`
	Kind                     string     `json:"kind"`
	PlanID                   string     `json:"plan_id"`
	JoinPlanID               string     `json:"join_plan_id"`
	JoinPlanDigest           string     `json:"join_plan_digest"`
	ManifestDigest           string     `json:"manifest_digest"`
	AdmissionTime            time.Time  `json:"admission_time"`
	Zones                    []JoinZone `json:"zones"`
	AcceptedReportCount      int        `json:"accepted_report_count"`
	RejectedReportCount      int        `json:"rejected_report_count"`
	ProjectedNodeJoinCount   int        `json:"projected_node_join_count"`
	ExecutedTiKVMutations    int        `json:"executed_tikv_mutations"`
	ExecutedDaemonActions    int        `json:"executed_daemon_actions"`
	ExecutedStorageMutations int        `json:"executed_storage_mutations"`
	NoMutationAdmission      bool       `json:"no_mutation_admission"`
	Physical160Claimed       bool       `json:"physical_160_claimed"`
}

type JoinZone struct {
	Zone  string     `json:"zone"`
	Nodes []JoinNode `json:"nodes"`
}

type JoinNode struct {
	NodeID             string    `json:"node_id"`
	Hostname           string    `json:"hostname"`
	BundleDigest       string    `json:"bundle_digest"`
	ReportDigest       string    `json:"report_digest"`
	SignedReportDigest string    `json:"signed_report_digest"`
	SignerKeyID        string    `json:"signer_key_id"`
	ObservedAt         time.Time `json:"observed_at"`
}

type trustedSignerKey struct {
	nodeID    string
	publicKey ed25519.PublicKey
}

// BuildJoinPlan performs central report admission and returns a deterministic
// projected join plan. It does not have a TiKV, daemon, SSH, mount, or storage
// client and all executed counters are invariants fixed at zero.
func BuildJoinPlan(request AdmissionRequest) (*JoinPlan, error) {
	if request.Manifest == nil {
		return nil, fmt.Errorf("manifest is required")
	}
	if strings.TrimSpace(request.PlanID) == "" {
		return nil, fmt.Errorf("plan ID is required")
	}
	if request.AdmissionTime.IsZero() {
		return nil, fmt.Errorf("admission time is required")
	}
	if request.MaxReportAge <= 0 {
		request.MaxReportAge = DefaultMaxReportAge
	}
	if request.MaxFutureSkew <= 0 {
		request.MaxFutureSkew = DefaultFutureSkew
	}
	if err := ValidateTrustBundle(request.TrustBundle); err != nil {
		return nil, err
	}
	rendered, err := clustermanifest.Render(request.Manifest, request.Policy)
	if err != nil {
		return nil, err
	}
	manifestPlan, err := clustermanifest.BuildPlan(request.Manifest, nil, request.Policy)
	if err != nil {
		return nil, err
	}
	if request.PlanID != manifestPlan.PlanID {
		return nil, fmt.Errorf("requested plan ID %q does not match deterministic manifest plan %q", request.PlanID, manifestPlan.PlanID)
	}
	canonical, err := clustermanifest.Canonicalize(request.Manifest)
	if err != nil {
		return nil, err
	}
	nodes := make(map[string]clustermanifest.Node, len(canonical.Spec.Nodes))
	bundles := make(map[string]clustermanifest.NodeBundle, len(rendered.Bundles))
	for _, node := range canonical.Spec.Nodes {
		nodes[node.ID] = node
	}
	for _, bundle := range rendered.Bundles {
		bundles[bundle.NodeID] = bundle
	}
	trusted, err := trustedSignerKeys(request.TrustBundle)
	if err != nil {
		return nil, err
	}

	issues := make([]AdmissionIssue, 0)
	if len(request.Reports) != clustermanifest.ExactNodeCount {
		issues = append(issues, AdmissionIssue{Code: CodeReportCount, Message: fmt.Sprintf("got %d signed reports, want exactly %d", len(request.Reports), clustermanifest.ExactNodeCount)})
	}
	seen := map[string]int{}
	accepted := map[string]JoinNode{}
	acceptedZoneCounts := map[string]int{}
	for i, signed := range request.Reports {
		reportIndex := i + 1
		issueStart := len(issues)
		if err := validateSignedReportShape(signed); err != nil {
			issues = append(issues, AdmissionIssue{Code: CodeReportEnvelope, ReportIndex: reportIndex, Message: err.Error()})
			continue
		}
		report := &signed.Report
		nodeID, keyID := report.NodeID, signed.KeyID
		seen[nodeID]++
		if seen[nodeID] > 1 {
			issues = append(issues, AdmissionIssue{Code: CodeReportDuplicateNode, ReportIndex: reportIndex, NodeID: nodeID, KeyID: keyID, Message: "more than one signed report names the same node"})
		}
		node, knownNode := nodes[nodeID]
		if !knownNode {
			issues = append(issues, AdmissionIssue{Code: CodeReportUnknownNode, ReportIndex: reportIndex, NodeID: nodeID, KeyID: keyID, Message: "report node is not in the exact manifest"})
		}
		signer, trustedKey := trusted[keyID]
		if !trustedKey {
			issues = append(issues, AdmissionIssue{Code: CodeReportUntrustedKey, ReportIndex: reportIndex, NodeID: nodeID, KeyID: keyID, Message: "signing key is not in the reviewed trust bundle"})
		} else {
			if signer.nodeID != nodeID {
				issues = append(issues, AdmissionIssue{Code: CodeReportSignerBinding, ReportIndex: reportIndex, NodeID: nodeID, KeyID: keyID, Message: fmt.Sprintf("trusted signer is bound to %s", signer.nodeID)})
			}
			payload, payloadErr := reportSignaturePayload(report)
			signature, signatureErr := base64.StdEncoding.DecodeString(signed.Signature)
			if payloadErr != nil || signatureErr != nil || !ed25519.Verify(signer.publicKey, payload, signature) {
				issues = append(issues, AdmissionIssue{Code: CodeReportSignature, ReportIndex: reportIndex, NodeID: nodeID, KeyID: keyID, Message: "Ed25519 signature verification failed"})
			}
		}
		if err := validateReportIntegrity(report); err != nil {
			issues = append(issues, AdmissionIssue{Code: CodeReportIntegrity, ReportIndex: reportIndex, NodeID: nodeID, KeyID: keyID, Message: err.Error()})
		}
		if report.PlanID != request.PlanID {
			issues = append(issues, AdmissionIssue{Code: CodeReportPlanID, ReportIndex: reportIndex, NodeID: nodeID, KeyID: keyID, Message: "report plan ID does not match the reviewed plan"})
		}
		if report.ManifestDigest != rendered.ManifestDigest {
			issues = append(issues, AdmissionIssue{Code: CodeReportManifestDigest, ReportIndex: reportIndex, NodeID: nodeID, KeyID: keyID, Message: "report manifest digest does not match the reviewed manifest"})
		}
		if knownNode && report.BundleDigest != bundles[nodeID].BundleDigest {
			issues = append(issues, AdmissionIssue{Code: CodeReportBundleDigest, ReportIndex: reportIndex, NodeID: nodeID, KeyID: keyID, Message: "report bundle digest does not match the node bundle"})
		}
		if report.EvidenceSource != "local-os" {
			issues = append(issues, AdmissionIssue{Code: CodeReportEvidenceSource, ReportIndex: reportIndex, NodeID: nodeID, KeyID: keyID, Message: "central admission requires evidence_source=local-os"})
		}
		if knownNode && !hasExactReportCheckSet(report, canonical, node, bundles[nodeID]) {
			issues = append(issues, AdmissionIssue{Code: CodeReportCheckSet, ReportIndex: reportIndex, NodeID: nodeID, KeyID: keyID, Message: "report checks do not exactly cover the current node preflight contract"})
		}
		if report.Result != ResultOK || report.ErrorCount != 0 {
			issues = append(issues, AdmissionIssue{Code: CodeReportResult, ReportIndex: reportIndex, NodeID: nodeID, KeyID: keyID, Message: "only a successful zero-error host preflight report is admissible"})
		}
		age := request.AdmissionTime.Sub(report.ObservedAt)
		if age > request.MaxReportAge {
			issues = append(issues, AdmissionIssue{Code: CodeReportStale, ReportIndex: reportIndex, NodeID: nodeID, KeyID: keyID, Message: fmt.Sprintf("report age %s exceeds %s", age, request.MaxReportAge)})
		}
		if age < -request.MaxFutureSkew {
			issues = append(issues, AdmissionIssue{Code: CodeReportFuture, ReportIndex: reportIndex, NodeID: nodeID, KeyID: keyID, Message: fmt.Sprintf("report timestamp is %s in the future", -age)})
		}
		if report.TiKVMutationCount != 0 || report.DaemonActionCount != 0 || report.StorageMutationCount != 0 {
			issues = append(issues, AdmissionIssue{Code: CodeReportMutationClaim, ReportIndex: reportIndex, NodeID: nodeID, KeyID: keyID, Message: "preflight report claims a forbidden mutation or daemon action"})
		}
		if len(issues) == issueStart && knownNode {
			signedDigest, digestErr := SignedReportDigest(signed)
			if digestErr != nil {
				issues = append(issues, AdmissionIssue{Code: CodeReportEnvelope, ReportIndex: reportIndex, NodeID: nodeID, KeyID: keyID, Message: digestErr.Error()})
				continue
			}
			accepted[nodeID] = JoinNode{
				NodeID: nodeID, Hostname: node.Hostname, BundleDigest: report.BundleDigest,
				ReportDigest: report.ReportDigest, SignedReportDigest: signedDigest,
				SignerKeyID: keyID, ObservedAt: report.ObservedAt.UTC(),
			}
			acceptedZoneCounts[node.Location.Zone]++
		}
	}
	for _, node := range canonical.Spec.Nodes {
		if seen[node.ID] == 0 {
			issues = append(issues, AdmissionIssue{Code: CodeReportMissingNode, NodeID: node.ID, Message: "the exact manifest node has no signed report"})
		}
	}
	for _, zone := range canonical.Spec.Rollout.Order {
		if acceptedZoneCounts[zone] != clustermanifest.ExactNodesPerZone {
			issues = append(issues, AdmissionIssue{Code: CodeReportZoneCount, NodeID: zone, Message: fmt.Sprintf("zone has %d admitted reports, want exactly %d", acceptedZoneCounts[zone], clustermanifest.ExactNodesPerZone)})
		}
	}
	if len(issues) > 0 {
		sortAdmissionIssues(issues)
		return nil, &AdmissionError{
			Issues: issues, AcceptedReportCount: len(accepted),
			RejectedReportCount: len(request.Reports) - len(accepted),
			AdmittedNodesByZone: acceptedZoneCounts,
		}
	}

	joinPlan := &JoinPlan{
		APIVersion: APIVersion, Kind: JoinPlanKind, PlanID: request.PlanID,
		ManifestDigest: rendered.ManifestDigest, AdmissionTime: request.AdmissionTime.UTC(),
		AcceptedReportCount: len(accepted), RejectedReportCount: 0,
		ProjectedNodeJoinCount: len(accepted), ExecutedTiKVMutations: 0,
		ExecutedDaemonActions: 0, ExecutedStorageMutations: 0,
		NoMutationAdmission: true, Physical160Claimed: false,
	}
	for _, zoneName := range canonical.Spec.Rollout.Order {
		zone := JoinZone{Zone: zoneName}
		for _, node := range canonical.Spec.Nodes {
			if node.Location.Zone == zoneName {
				zone.Nodes = append(zone.Nodes, accepted[node.ID])
			}
		}
		joinPlan.Zones = append(joinPlan.Zones, zone)
	}
	joinPlan.JoinPlanDigest = digestJoinPlan(joinPlan)
	joinPlan.JoinPlanID = "ad-join-" + strings.TrimPrefix(joinPlan.JoinPlanDigest, "sha256:")[:16]
	return joinPlan, nil
}

func MarshalJoinPlan(plan *JoinPlan) ([]byte, error) {
	if err := ValidateJoinPlan(plan); err != nil {
		return nil, err
	}
	raw, err := json.MarshalIndent(plan, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("marshal join plan: %w", err)
	}
	return append(raw, '\n'), nil
}

func ParseJoinPlan(raw []byte) (*JoinPlan, error) {
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	var plan JoinPlan
	if err := dec.Decode(&plan); err != nil {
		return nil, fmt.Errorf("decode join plan: %w", err)
	}
	var extra any
	if err := dec.Decode(&extra); err != io.EOF {
		if err == nil {
			return nil, fmt.Errorf("decode join plan: multiple JSON documents are not allowed")
		}
		return nil, fmt.Errorf("decode join plan trailing data: %w", err)
	}
	if err := ValidateJoinPlan(&plan); err != nil {
		return nil, err
	}
	return &plan, nil
}

func ValidateJoinPlan(plan *JoinPlan) error {
	if plan == nil {
		return fmt.Errorf("join plan is nil")
	}
	if plan.APIVersion != APIVersion || plan.Kind != JoinPlanKind {
		return fmt.Errorf("join plan identity must be api_version=%s kind=%s", APIVersion, JoinPlanKind)
	}
	if strings.TrimSpace(plan.PlanID) == "" || !canonicalSHA256Digest(plan.ManifestDigest) || plan.AdmissionTime.IsZero() {
		return fmt.Errorf("join plan plan, manifest, and admission identity are required")
	}
	if plan.AcceptedReportCount != clustermanifest.ExactNodeCount || plan.RejectedReportCount != 0 || plan.ProjectedNodeJoinCount != clustermanifest.ExactNodeCount {
		return fmt.Errorf("join plan report/join counts are not the exact admitted %d-node set", clustermanifest.ExactNodeCount)
	}
	if len(plan.Zones) != clustermanifest.ExactZoneCount {
		return fmt.Errorf("join plan has %d zones, want %d", len(plan.Zones), clustermanifest.ExactZoneCount)
	}
	seenZones, seenNodes := map[string]bool{}, map[string]bool{}
	for _, zone := range plan.Zones {
		if strings.TrimSpace(zone.Zone) == "" || seenZones[zone.Zone] {
			return fmt.Errorf("join plan has an empty or duplicate zone %q", zone.Zone)
		}
		seenZones[zone.Zone] = true
		if len(zone.Nodes) != clustermanifest.ExactNodesPerZone {
			return fmt.Errorf("join plan zone %s has %d nodes, want %d", zone.Zone, len(zone.Nodes), clustermanifest.ExactNodesPerZone)
		}
		for _, node := range zone.Nodes {
			if strings.TrimSpace(node.NodeID) == "" || seenNodes[node.NodeID] {
				return fmt.Errorf("join plan has an empty or duplicate node %q", node.NodeID)
			}
			if strings.TrimSpace(node.Hostname) == "" || !canonicalSHA256Digest(node.BundleDigest) || !canonicalSHA256Digest(node.ReportDigest) || !canonicalSHA256Digest(node.SignedReportDigest) || strings.TrimSpace(node.SignerKeyID) == "" || node.ObservedAt.IsZero() {
				return fmt.Errorf("join plan node %s has incomplete admitted identity", node.NodeID)
			}
			seenNodes[node.NodeID] = true
		}
	}
	if len(seenNodes) != clustermanifest.ExactNodeCount {
		return fmt.Errorf("join plan has %d unique nodes, want %d", len(seenNodes), clustermanifest.ExactNodeCount)
	}
	if !plan.NoMutationAdmission || plan.ExecutedTiKVMutations != 0 || plan.ExecutedDaemonActions != 0 || plan.ExecutedStorageMutations != 0 {
		return fmt.Errorf("join plan violates the no-mutation admission boundary")
	}
	if plan.Physical160Claimed {
		return fmt.Errorf("join plan must not claim physical-160 qualification")
	}
	expectedDigest := digestJoinPlan(plan)
	if plan.JoinPlanDigest != expectedDigest {
		return fmt.Errorf("join plan digest does not match canonical content")
	}
	expectedID := "ad-join-" + strings.TrimPrefix(expectedDigest, "sha256:")[:16]
	if plan.JoinPlanID != expectedID {
		return fmt.Errorf("join plan ID does not match canonical digest")
	}
	return nil
}

func trustedSignerKeys(bundle *TrustBundle) (map[string]trustedSignerKey, error) {
	if err := ValidateTrustBundle(bundle); err != nil {
		return nil, err
	}
	result := make(map[string]trustedSignerKey, len(bundle.Signers))
	for _, signer := range bundle.Signers {
		decoded, _ := base64.StdEncoding.DecodeString(signer.PublicKey)
		result[signer.KeyID] = trustedSignerKey{nodeID: signer.NodeID, publicKey: ed25519.PublicKey(decoded)}
	}
	return result, nil
}

func canonicalSHA256Digest(value string) bool {
	if !strings.HasPrefix(value, "sha256:") || len(value) != len("sha256:")+sha256.Size*2 {
		return false
	}
	_, err := hex.DecodeString(strings.TrimPrefix(value, "sha256:"))
	return err == nil && value == strings.ToLower(value)
}

func digestJoinPlan(plan *JoinPlan) string {
	copyPlan := *plan
	copyPlan.JoinPlanID = ""
	copyPlan.JoinPlanDigest = ""
	raw, _ := json.Marshal(copyPlan)
	sum := sha256.Sum256(raw)
	return "sha256:" + hex.EncodeToString(sum[:])
}

func hasExactReportCheckSet(report *Report, manifest *clustermanifest.Manifest, node clustermanifest.Node, bundle clustermanifest.NodeBundle) bool {
	expected := map[string]int{}
	add := func(id, subject string) { expected[id+"\x00"+subject]++ }
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
	observed := map[string]int{}
	for _, check := range report.Checks {
		observed[check.ID+"\x00"+check.Subject]++
	}
	if len(observed) != len(expected) {
		return false
	}
	for key, count := range expected {
		if observed[key] != count {
			return false
		}
	}
	return true
}

func sortAdmissionIssues(issues []AdmissionIssue) {
	sort.SliceStable(issues, func(i, j int) bool {
		priority := func(issue AdmissionIssue) int {
			if issue.Code == CodeReportCount {
				return 0
			}
			if issue.ReportIndex > 0 {
				return 1
			}
			return 2
		}
		if priority(issues[i]) != priority(issues[j]) {
			return priority(issues[i]) < priority(issues[j])
		}
		if issues[i].ReportIndex != issues[j].ReportIndex {
			return issues[i].ReportIndex < issues[j].ReportIndex
		}
		if issues[i].NodeID != issues[j].NodeID {
			return issues[i].NodeID < issues[j].NodeID
		}
		if issues[i].Code != issues[j].Code {
			return issues[i].Code < issues[j].Code
		}
		return issues[i].KeyID < issues[j].KeyID
	})
}
