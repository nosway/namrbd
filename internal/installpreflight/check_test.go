package installpreflight

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/nosway/namrbd/internal/clustermanifest"
)

const testArtifactDigest = "sha256:c3bd6d1fdc4b1a1239e4036e294b71fb7ea0880ba990a486d1cedaede3f4eecf"

func TestCheckHostAcceptsExactReadOnlyFacts(t *testing.T) {
	request := exactRequest(t)
	report, err := CheckHost(request)
	if err != nil {
		t.Fatalf("CheckHost: %v", err)
	}
	if report.Result != ResultOK || report.ErrorCount != 0 || report.PassCount == 0 {
		t.Fatalf("report=%+v", report)
	}
	if report.TiKVMutationCount != 0 || report.DaemonActionCount != 0 || report.StorageMutationCount != 0 {
		t.Fatalf("preflight reported mutations: %+v", report)
	}
	second, err := CheckHost(request)
	if err != nil {
		t.Fatalf("CheckHost second: %v", err)
	}
	if second.ReportDigest != report.ReportDigest {
		t.Fatalf("report digest changed: %s != %s", second.ReportDigest, report.ReportDigest)
	}
}

func TestCheckHostRejectsUnsafeFactsWithStructuredCheckIDs(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*Request)
		id     string
	}{
		{name: "wrong hostname", mutate: func(r *Request) { r.Facts.Hostname = "node2" }, id: CheckHostname},
		{name: "stale clock", mutate: func(r *Request) { r.Facts.ObservedAt = r.ReferenceTime.Add(10 * time.Minute) }, id: CheckClock},
		{name: "low mtu", mutate: func(r *Request) { r.Facts.Interfaces[0].MTU = 1400 }, id: CheckManagementNetwork},
		{name: "port occupied", mutate: func(r *Request) { r.Facts.ListeningPorts = []int{9091} }, id: CheckPort},
		{name: "installed digest drift", mutate: func(r *Request) { r.Facts.Files[0].Digest = strings.Repeat("0", 64) }, id: CheckInstalledFile},
		{name: "wrong uuid", mutate: func(r *Request) { r.Facts.Devices[0].FilesystemUUID = "00000000-0000-4000-8000-000000000999" }, id: CheckFilesystemUUID},
		{name: "wrong mount", mutate: func(r *Request) { r.Facts.Mounts[0].ResolvedSource = "/dev/sdz" }, id: CheckMount},
		{name: "wrong owner", mutate: func(r *Request) { r.Facts.Mounts[0].Owner = "root" }, id: CheckMountOwnership},
		{name: "insufficient capacity", mutate: func(r *Request) { r.Facts.Mounts[0].FreeBytes = 1 }, id: CheckCapacity},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			request := exactRequest(t)
			test.mutate(&request)
			report, err := CheckHost(request)
			if err != nil {
				t.Fatalf("CheckHost: %v", err)
			}
			if report.Result != ResultBlocked || !hasFailedCheck(report, test.id) {
				t.Fatalf("report result=%s checks=%+v, want failed %s", report.Result, report.Checks, test.id)
			}
			if report.TiKVMutationCount != 0 || report.DaemonActionCount != 0 || report.StorageMutationCount != 0 {
				t.Fatalf("blocked preflight reported mutations: %+v", report)
			}
		})
	}
}

func TestCheckHostRejectsTamperedBundle(t *testing.T) {
	request := exactRequest(t)
	path := filepath.Join(request.BundleDirectory, "etc", "namrbd", "sbs-data.yaml")
	if err := os.WriteFile(path, []byte("tampered\n"), 0o600); err != nil {
		t.Fatalf("tamper bundle: %v", err)
	}
	report, err := CheckHost(request)
	if err != nil {
		t.Fatalf("CheckHost: %v", err)
	}
	if !hasFailedCheck(report, CheckBundleFile) {
		t.Fatalf("tampered bundle checks=%+v", report.Checks)
	}
}

func TestCheckHostRejectsUnexpectedBundleFile(t *testing.T) {
	request := exactRequest(t)
	path := filepath.Join(request.BundleDirectory, "etc", "systemd", "system", "unexpected.service")
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	if err := os.WriteFile(path, []byte("[Service]\nExecStart=/bin/false\n"), 0o644); err != nil {
		t.Fatalf("write unexpected bundle file: %v", err)
	}
	report, err := CheckHost(request)
	if err != nil {
		t.Fatalf("CheckHost: %v", err)
	}
	if !hasFailedCheck(report, CheckBundleFileSet) {
		t.Fatalf("unexpected bundle checks=%+v", report.Checks)
	}
}

func TestParseFactsIsStrict(t *testing.T) {
	facts := exactRequest(t).Facts
	raw, err := json.Marshal(facts)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	if _, err := ParseFacts(raw); err != nil {
		t.Fatalf("ParseFacts: %v", err)
	}
	withUnknown := strings.Replace(string(raw), "{", `{"unknown":true,`, 1)
	if _, err := ParseFacts([]byte(withUnknown)); err == nil || !strings.Contains(err.Error(), "unknown field") {
		t.Fatalf("ParseFacts unknown error=%v", err)
	}
	facts.Files = append(facts.Files, facts.Files[0])
	raw, err = json.Marshal(facts)
	if err != nil {
		t.Fatalf("Marshal duplicate: %v", err)
	}
	if _, err := ParseFacts(raw); err == nil || !strings.Contains(err.Error(), "duplicate file path") {
		t.Fatalf("ParseFacts duplicate error=%v", err)
	}
}

func exactRequest(t *testing.T) Request {
	t.Helper()
	manifest, err := clustermanifest.Load(filepath.Join("..", "..", "configs", "sbs-cluster-160.example.yaml"))
	if err != nil {
		t.Fatalf("Load manifest: %v", err)
	}
	policy := clustermanifest.ValidationPolicy{ApprovedArtifactDigests: []string{testArtifactDigest}, RequireArtifactApproval: true}
	rendered, err := clustermanifest.Render(manifest, policy)
	if err != nil {
		t.Fatalf("Render: %v", err)
	}
	node, bundle, err := desiredNodeAndBundle(manifest, rendered, "node1")
	if err != nil {
		t.Fatalf("desiredNodeAndBundle: %v", err)
	}
	bundleDirectory := filepath.Join(t.TempDir(), "node1")
	materializeNodeBundle(t, bundleDirectory, bundle)
	referenceTime := time.Date(2026, 9, 3, 2, 0, 0, 0, time.UTC)
	facts := HostFacts{
		APIVersion: APIVersion, Kind: FactsKind, Hostname: node.Hostname,
		ObservedAt: referenceTime, KernelRelease: "6.8.0-test", EvidenceSource: "fixture",
		PortObservationSupported: true,
		Interfaces: []InterfaceFact{
			{Name: "mgmt0", Up: true, MTU: 1500, Addresses: []string{node.ManagementAddress}},
			{Name: "data0", Up: true, MTU: 9000, Addresses: []string{node.DataAddress}},
		},
	}
	for _, file := range bundle.Files {
		owner := manifest.Spec.Bundle.ServiceUser
		if strings.HasSuffix(file.InstallPath, ".service") {
			owner = "root"
		}
		facts.Files = append(facts.Files, FileFact{Path: file.InstallPath, Exists: true, Digest: file.Digest, Mode: file.Mode, Owner: owner})
	}
	facts.Files = append(facts.Files,
		FileFact{Path: filepath.Join(manifest.Spec.Bundle.BinaryDirectory, "sbs-data"), Exists: true, Digest: manifest.Spec.Artifact.BinaryDigests["sbs-data"], Mode: 0o755, Owner: "root"},
		FileFact{Path: filepath.Join(manifest.Spec.Bundle.BinaryDirectory, "sbs-service"), Exists: true, Digest: manifest.Spec.Artifact.BinaryDigests["sbs-service"], Mode: 0o755, Owner: "root"},
	)
	for _, claim := range node.StorageClaims {
		facts.Devices = append(facts.Devices, DeviceFact{DeviceByID: claim.DeviceByID, Exists: true, ResolvedPath: "/dev/sda", FilesystemUUID: claim.FilesystemUUID})
		facts.Mounts = append(facts.Mounts, MountFact{
			Path: claim.MountPath, Mounted: true, Source: claim.DeviceByID, ResolvedSource: "/dev/sda", FilesystemType: "xfs",
			Owner: manifest.Spec.Bundle.ServiceUser, Mode: 0o750,
			TotalBytes: uint64(claim.MinimumFreeBytes + claim.ReservedBytes + 1), FreeBytes: uint64(claim.MinimumFreeBytes + 1),
		})
	}
	return Request{
		Manifest: manifest, Policy: policy, NodeID: node.ID, PlanID: "ad-plan-fixture",
		BundleDirectory: bundleDirectory, ReferenceTime: referenceTime, MaxClockSkew: DefaultMaxClockSkew,
		MinimumMTU: DefaultMinimumMTU, Facts: facts,
	}
}

func materializeNodeBundle(t *testing.T, directory string, bundle clustermanifest.NodeBundle) {
	t.Helper()
	prefix := filepath.ToSlash(filepath.Join("nodes", bundle.NodeID)) + "/"
	for _, file := range bundle.Files {
		relative := strings.TrimPrefix(file.RelativePath, prefix)
		path := filepath.Join(directory, filepath.FromSlash(relative))
		if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
			t.Fatalf("MkdirAll: %v", err)
		}
		if err := os.WriteFile(path, file.Content, os.FileMode(file.Mode)); err != nil {
			t.Fatalf("WriteFile: %v", err)
		}
	}
}

func hasFailedCheck(report *Report, id string) bool {
	for _, check := range report.Checks {
		if check.ID == id && check.Status == StatusFail {
			return true
		}
	}
	return false
}
