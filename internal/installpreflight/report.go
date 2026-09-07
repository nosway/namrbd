// Package installpreflight implements the read-only Phase AD node-local host
// and storage admission contract. It consumes a reviewed cluster manifest,
// deterministic node bundle, and observed host facts; it never provisions a
// device, changes a mount, starts a daemon, or mutates cluster metadata.
package installpreflight

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strings"
	"time"
)

const (
	APIVersion = "namrbd.io/v1alpha1"
	ReportKind = "SBSHostPreflightReport"
	FactsKind  = "SBSHostFacts"

	ResultOK      = "ok"
	ResultBlocked = "blocked"
	StatusPass    = "pass"
	StatusFail    = "fail"
)

const (
	CheckHostname          = "AD_HOST_HOSTNAME_MATCH"
	CheckClock             = "AD_HOST_CLOCK_SKEW"
	CheckKernel            = "AD_HOST_KERNEL_PRESENT"
	CheckManagementNetwork = "AD_HOST_MANAGEMENT_NETWORK"
	CheckDataNetwork       = "AD_HOST_DATA_NETWORK"
	CheckPort              = "AD_HOST_PORT_AVAILABLE"
	CheckBundleFileSet     = "AD_HOST_BUNDLE_FILE_SET"
	CheckBundleFile        = "AD_HOST_BUNDLE_FILE_DIGEST"
	CheckInstalledFile     = "AD_HOST_INSTALLED_FILE"
	CheckBinary            = "AD_HOST_BINARY_DIGEST"
	CheckDevice            = "AD_STORAGE_DEVICE_CLAIM"
	CheckFilesystemUUID    = "AD_STORAGE_FILESYSTEM_UUID"
	CheckMount             = "AD_STORAGE_MOUNT_SOURCE"
	CheckMountOwnership    = "AD_STORAGE_MOUNT_OWNERSHIP"
	CheckCapacity          = "AD_STORAGE_CAPACITY"
)

type Check struct {
	ID       string `json:"id"`
	Subject  string `json:"subject"`
	Status   string `json:"status"`
	Expected string `json:"expected,omitempty"`
	Observed string `json:"observed,omitempty"`
	Message  string `json:"message,omitempty"`
}

type Report struct {
	APIVersion           string    `json:"api_version"`
	Kind                 string    `json:"kind"`
	PlanID               string    `json:"plan_id"`
	ManifestDigest       string    `json:"manifest_digest"`
	BundleDigest         string    `json:"bundle_digest"`
	NodeID               string    `json:"node_id"`
	ObservedAt           time.Time `json:"observed_at"`
	ReferenceTime        time.Time `json:"reference_time"`
	EvidenceSource       string    `json:"evidence_source"`
	Result               string    `json:"result"`
	Checks               []Check   `json:"checks"`
	PassCount            int       `json:"pass_count"`
	ErrorCount           int       `json:"error_count"`
	FirstError           string    `json:"first_error"`
	LastError            string    `json:"last_error"`
	ReportDigest         string    `json:"report_digest"`
	TiKVMutationCount    int       `json:"tikv_mutation_count"`
	DaemonActionCount    int       `json:"daemon_action_count"`
	StorageMutationCount int       `json:"storage_mutation_count"`
	RemoteLabUsed        bool      `json:"remote_lab_used"`
	Physical160Claimed   bool      `json:"physical_160_claimed"`
}

type HostFacts struct {
	APIVersion               string          `json:"api_version"`
	Kind                     string          `json:"kind"`
	Hostname                 string          `json:"hostname"`
	ObservedAt               time.Time       `json:"observed_at"`
	KernelRelease            string          `json:"kernel_release"`
	Interfaces               []InterfaceFact `json:"interfaces"`
	PortObservationSupported bool            `json:"port_observation_supported"`
	ListeningPorts           []int           `json:"listening_ports"`
	Files                    []FileFact      `json:"files"`
	Devices                  []DeviceFact    `json:"devices"`
	Mounts                   []MountFact     `json:"mounts"`
	EvidenceSource           string          `json:"evidence_source"`
}

type InterfaceFact struct {
	Name      string   `json:"name"`
	Up        bool     `json:"up"`
	MTU       int      `json:"mtu"`
	Addresses []string `json:"addresses"`
}

type FileFact struct {
	Path   string `json:"path"`
	Exists bool   `json:"exists"`
	Digest string `json:"digest,omitempty"`
	Mode   uint32 `json:"mode,omitempty"`
	Owner  string `json:"owner,omitempty"`
	Error  string `json:"error,omitempty"`
}

type DeviceFact struct {
	DeviceByID     string `json:"device_by_id"`
	Exists         bool   `json:"exists"`
	ResolvedPath   string `json:"resolved_path,omitempty"`
	FilesystemUUID string `json:"filesystem_uuid,omitempty"`
	Error          string `json:"error,omitempty"`
}

type MountFact struct {
	Path           string `json:"path"`
	Mounted        bool   `json:"mounted"`
	Source         string `json:"source,omitempty"`
	ResolvedSource string `json:"resolved_source,omitempty"`
	FilesystemType string `json:"filesystem_type,omitempty"`
	Owner          string `json:"owner,omitempty"`
	Mode           uint32 `json:"mode,omitempty"`
	TotalBytes     uint64 `json:"total_bytes,omitempty"`
	FreeBytes      uint64 `json:"free_bytes,omitempty"`
	Error          string `json:"error,omitempty"`
}

func ParseFacts(raw []byte) (*HostFacts, error) {
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	var facts HostFacts
	if err := dec.Decode(&facts); err != nil {
		return nil, fmt.Errorf("decode host facts: %w", err)
	}
	var extra any
	if err := dec.Decode(&extra); err != io.EOF {
		if err == nil {
			return nil, fmt.Errorf("decode host facts: multiple JSON documents are not allowed")
		}
		return nil, fmt.Errorf("decode host facts trailing data: %w", err)
	}
	if facts.APIVersion != APIVersion || facts.Kind != FactsKind {
		return nil, fmt.Errorf("host facts identity must be api_version=%s kind=%s", APIVersion, FactsKind)
	}
	if strings.TrimSpace(facts.EvidenceSource) == "" {
		return nil, fmt.Errorf("host facts evidence_source is required")
	}
	if facts.ObservedAt.IsZero() {
		return nil, fmt.Errorf("host facts observed_at is required")
	}
	if err := rejectDuplicateFacts(&facts); err != nil {
		return nil, err
	}
	return &facts, nil
}

func rejectDuplicateFacts(facts *HostFacts) error {
	seen := map[string]bool{}
	for _, iface := range facts.Interfaces {
		key := strings.TrimSpace(iface.Name)
		if key == "" || seen[key] {
			return fmt.Errorf("host facts contain an empty or duplicate interface name %q", iface.Name)
		}
		seen[key] = true
	}
	for _, group := range []struct {
		kind   string
		values []string
	}{
		{kind: "file path", values: factFilePaths(facts.Files)},
		{kind: "device claim", values: factDeviceIDs(facts.Devices)},
		{kind: "mount path", values: factMountPaths(facts.Mounts)},
	} {
		seen = map[string]bool{}
		for _, value := range group.values {
			value = strings.TrimSpace(value)
			if value == "" || seen[value] {
				return fmt.Errorf("host facts contain an empty or duplicate %s %q", group.kind, value)
			}
			seen[value] = true
		}
	}
	seenPorts := map[int]bool{}
	for _, port := range facts.ListeningPorts {
		if port < 1 || port > 65535 || seenPorts[port] {
			return fmt.Errorf("host facts contain an invalid or duplicate listening port %d", port)
		}
		seenPorts[port] = true
	}
	return nil
}

func factFilePaths(values []FileFact) []string {
	result := make([]string, 0, len(values))
	for _, value := range values {
		result = append(result, value.Path)
	}
	return result
}

func factDeviceIDs(values []DeviceFact) []string {
	result := make([]string, 0, len(values))
	for _, value := range values {
		result = append(result, value.DeviceByID)
	}
	return result
}

func factMountPaths(values []MountFact) []string {
	result := make([]string, 0, len(values))
	for _, value := range values {
		result = append(result, value.Path)
	}
	return result
}

func MarshalReport(report *Report) ([]byte, error) {
	if report == nil {
		return nil, fmt.Errorf("preflight report is nil")
	}
	raw, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("marshal preflight report: %w", err)
	}
	return append(raw, '\n'), nil
}

func finalizeReport(report *Report) {
	report.Result = ResultOK
	for _, check := range report.Checks {
		if check.Status == StatusPass {
			report.PassCount++
			continue
		}
		report.Result = ResultBlocked
		report.ErrorCount++
		message := check.ID + " " + check.Subject
		if check.Message != "" {
			message += ": " + check.Message
		}
		if report.FirstError == "" {
			report.FirstError = message
		}
		report.LastError = message
	}
	report.ReportDigest = reportDigest(report)
}

func reportDigest(report *Report) string {
	copyReport := *report
	copyReport.ReportDigest = ""
	raw, _ := json.Marshal(copyReport)
	sum := sha256.Sum256(raw)
	return "sha256:" + hex.EncodeToString(sum[:])
}

func sortFacts(facts *HostFacts) {
	sort.Slice(facts.Interfaces, func(i, j int) bool { return facts.Interfaces[i].Name < facts.Interfaces[j].Name })
	for i := range facts.Interfaces {
		sort.Strings(facts.Interfaces[i].Addresses)
	}
	sort.Ints(facts.ListeningPorts)
	sort.Slice(facts.Files, func(i, j int) bool { return facts.Files[i].Path < facts.Files[j].Path })
	sort.Slice(facts.Devices, func(i, j int) bool { return facts.Devices[i].DeviceByID < facts.Devices[j].DeviceByID })
	sort.Slice(facts.Mounts, func(i, j int) bool { return facts.Mounts[i].Path < facts.Mounts[j].Path })
}
