package installpreflight

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io/fs"
	"net"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/nosway/namrbd/internal/clustermanifest"
)

const (
	DefaultMinimumMTU   = 1500
	DefaultMaxClockSkew = 2 * time.Minute
)

type Request struct {
	Manifest        *clustermanifest.Manifest
	Policy          clustermanifest.ValidationPolicy
	NodeID          string
	PlanID          string
	BundleDirectory string
	ReferenceTime   time.Time
	MaxClockSkew    time.Duration
	MinimumMTU      int
	Facts           HostFacts
}

func CheckHost(request Request) (*Report, error) {
	if request.Manifest == nil {
		return nil, fmt.Errorf("manifest is required")
	}
	if strings.TrimSpace(request.PlanID) == "" {
		return nil, fmt.Errorf("plan ID is required")
	}
	if request.ReferenceTime.IsZero() {
		return nil, fmt.Errorf("reference time is required")
	}
	if request.MaxClockSkew <= 0 {
		request.MaxClockSkew = DefaultMaxClockSkew
	}
	if request.MinimumMTU <= 0 {
		request.MinimumMTU = DefaultMinimumMTU
	}
	rendered, err := clustermanifest.Render(request.Manifest, request.Policy)
	if err != nil {
		return nil, err
	}
	node, bundle, err := desiredNodeAndBundle(request.Manifest, rendered, request.NodeID)
	if err != nil {
		return nil, err
	}
	if strings.TrimSpace(request.BundleDirectory) == "" {
		return nil, fmt.Errorf("node bundle directory is required")
	}
	if request.Facts.APIVersion != APIVersion || request.Facts.Kind != FactsKind {
		return nil, fmt.Errorf("host facts identity must be api_version=%s kind=%s", APIVersion, FactsKind)
	}
	sortFacts(&request.Facts)
	report := &Report{
		APIVersion: APIVersion, Kind: ReportKind, PlanID: request.PlanID,
		ManifestDigest: rendered.ManifestDigest, BundleDigest: bundle.BundleDigest, NodeID: node.ID,
		ObservedAt: request.Facts.ObservedAt.UTC(), ReferenceTime: request.ReferenceTime.UTC(),
		EvidenceSource: request.Facts.EvidenceSource,
	}
	add := func(id, subject string, passed bool, expected, observed, message string) {
		status := StatusFail
		if passed {
			status = StatusPass
		}
		report.Checks = append(report.Checks, Check{ID: id, Subject: subject, Status: status, Expected: expected, Observed: observed, Message: message})
	}

	add(CheckHostname, node.ID, request.Facts.Hostname == node.Hostname, node.Hostname, request.Facts.Hostname, "hostname must match the exact manifest node")
	skew := request.Facts.ObservedAt.Sub(request.ReferenceTime)
	if skew < 0 {
		skew = -skew
	}
	add(CheckClock, node.ID, !request.Facts.ObservedAt.IsZero() && skew <= request.MaxClockSkew,
		"absolute skew <= "+request.MaxClockSkew.String(), skew.String(), "host evidence timestamp is outside the coordinator window")
	add(CheckKernel, node.ID, strings.TrimSpace(request.Facts.KernelRelease) != "", "non-empty kernel release", request.Facts.KernelRelease, "kernel release is not observable")
	checkNetwork(add, CheckManagementNetwork, node.ManagementAddress, request.MinimumMTU, request.Facts.Interfaces)
	checkNetwork(add, CheckDataNetwork, node.DataAddress, request.MinimumMTU, request.Facts.Interfaces)
	checkPorts(add, node, request.Facts)
	checkBundleFiles(add, request.BundleDirectory, node.ID, bundle)
	checkInstalledFiles(add, request.Manifest, node, bundle, request.Facts.Files)
	checkStorage(add, request.Manifest, node, request.Facts)
	finalizeReport(report)
	return report, nil
}

func desiredNodeAndBundle(manifest *clustermanifest.Manifest, rendered *clustermanifest.RenderSet, nodeID string) (clustermanifest.Node, clustermanifest.NodeBundle, error) {
	canonical, err := clustermanifest.Canonicalize(manifest)
	if err != nil {
		return clustermanifest.Node{}, clustermanifest.NodeBundle{}, err
	}
	var node clustermanifest.Node
	foundNode := false
	for _, candidate := range canonical.Spec.Nodes {
		if candidate.ID == nodeID {
			node, foundNode = candidate, true
			break
		}
	}
	if !foundNode {
		return clustermanifest.Node{}, clustermanifest.NodeBundle{}, fmt.Errorf("node %q is not in the exact manifest", nodeID)
	}
	for _, bundle := range rendered.Bundles {
		if bundle.NodeID == nodeID {
			return node, bundle, nil
		}
	}
	return clustermanifest.Node{}, clustermanifest.NodeBundle{}, fmt.Errorf("node %q has no rendered bundle", nodeID)
}

func checkNetwork(add func(string, string, bool, string, string, string), checkID, address string, minimumMTU int, interfaces []InterfaceFact) {
	for _, iface := range interfaces {
		for _, observed := range iface.Addresses {
			if observed == address {
				add(checkID, address, iface.Up && iface.MTU >= minimumMTU,
					fmt.Sprintf("up interface with MTU >= %d", minimumMTU), fmt.Sprintf("%s up=%t mtu=%d", iface.Name, iface.Up, iface.MTU), "declared address must be on an eligible local interface")
				return
			}
		}
	}
	add(checkID, address, false, fmt.Sprintf("up interface with MTU >= %d", minimumMTU), "address not found", "declared address is not local")
}

func checkPorts(add func(string, string, bool, string, string, string), node clustermanifest.Node, facts HostFacts) {
	listening := map[int]bool{}
	for _, port := range facts.ListeningPorts {
		listening[port] = true
	}
	endpoints := []string{node.DataEndpoint, node.AdminEndpoint, node.MetricsEndpoint}
	if node.ServiceGRPCEndpoint != "" {
		endpoints = append(endpoints, node.ServiceGRPCEndpoint, node.ServiceHTTPEndpoint, node.ServiceMetricsEndpoint)
	}
	for _, endpoint := range endpoints {
		_, rawPort, err := net.SplitHostPort(endpoint)
		port, parseErr := strconv.Atoi(rawPort)
		passed := facts.PortObservationSupported && err == nil && parseErr == nil && !listening[port]
		observed := "unavailable"
		if facts.PortObservationSupported && err == nil && parseErr == nil {
			observed = fmt.Sprintf("listening=%t", listening[port])
		}
		add(CheckPort, endpoint, passed, "not listening before daemon start", observed, "port ownership must be clear before apply")
	}
}

func checkBundleFiles(add func(string, string, bool, string, string, string), bundleDirectory, nodeID string, bundle clustermanifest.NodeBundle) {
	prefix := filepath.ToSlash(filepath.Join("nodes", nodeID)) + "/"
	expectedPaths := make(map[string]bool, len(bundle.Files))
	for _, file := range bundle.Files {
		expectedPaths[strings.TrimPrefix(file.RelativePath, prefix)] = true
	}
	var unexpected []string
	walkErr := filepath.WalkDir(bundleDirectory, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if path == bundleDirectory || entry.IsDir() {
			return nil
		}
		relative, err := filepath.Rel(bundleDirectory, path)
		if err != nil {
			return err
		}
		relative = filepath.ToSlash(relative)
		if entry.Type()&os.ModeSymlink != 0 || !expectedPaths[relative] {
			unexpected = append(unexpected, relative)
		}
		return nil
	})
	sort.Strings(unexpected)
	add(CheckBundleFileSet, nodeID, walkErr == nil && len(unexpected) == 0,
		"exact deterministic file set without symlinks", strings.Join(unexpected, ","), errorString(walkErr))
	for _, file := range bundle.Files {
		relative := strings.TrimPrefix(file.RelativePath, prefix)
		path := filepath.Join(bundleDirectory, filepath.FromSlash(relative))
		info, statErr := os.Lstat(path)
		raw, err := os.ReadFile(path)
		observed := "missing"
		passed := false
		if err == nil && statErr == nil {
			sum := sha256.Sum256(raw)
			digest := "sha256:" + hex.EncodeToString(sum[:])
			observed = fmt.Sprintf("digest=%s mode=%04o", digest, info.Mode().Perm())
			passed = info.Mode().IsRegular() && digest == file.Digest && uint32(info.Mode().Perm()) == file.Mode
		}
		message := "bundle file content, type, and mode must match the deterministic renderer"
		if err != nil {
			message = err.Error()
		} else if statErr != nil {
			message = statErr.Error()
		}
		add(CheckBundleFile, file.RelativePath, passed, fmt.Sprintf("digest=%s mode=%04o", file.Digest, file.Mode), observed, message)
	}
}

func errorString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

func checkInstalledFiles(add func(string, string, bool, string, string, string), manifest *clustermanifest.Manifest, node clustermanifest.Node, bundle clustermanifest.NodeBundle, files []FileFact) {
	byPath := map[string]FileFact{}
	for _, file := range files {
		byPath[file.Path] = file
	}
	for _, expected := range bundle.Files {
		observed, ok := byPath[expected.InstallPath]
		owner := manifest.Spec.Bundle.ServiceUser
		if strings.HasSuffix(expected.InstallPath, ".service") {
			owner = "root"
		}
		passed := ok && observed.Exists && observed.Digest == expected.Digest && observed.Mode == expected.Mode && observed.Owner == owner
		add(CheckInstalledFile, expected.InstallPath, passed,
			fmt.Sprintf("digest=%s mode=%04o owner=%s", expected.Digest, expected.Mode, owner),
			fmt.Sprintf("exists=%t digest=%s mode=%04o owner=%s", observed.Exists, observed.Digest, observed.Mode, observed.Owner), observed.Error)
	}
	binaries := []string{filepath.Join(manifest.Spec.Bundle.BinaryDirectory, "sbs-data")}
	if containsRole(node.Roles, "sbs-service-active") || containsRole(node.Roles, "sbs-service-standby") {
		binaries = append(binaries, filepath.Join(manifest.Spec.Bundle.BinaryDirectory, "sbs-service"))
	}
	for _, path := range binaries {
		observed, ok := byPath[path]
		expectedDigest := manifest.Spec.Artifact.BinaryDigests[filepath.Base(path)]
		passed := ok && observed.Exists && observed.Digest == expectedDigest && observed.Mode&0o111 != 0 && observed.Owner == "root"
		add(CheckBinary, path, passed, "approved digest="+expectedDigest+" executable owner=root",
			fmt.Sprintf("exists=%t digest=%s mode=%04o owner=%s", observed.Exists, observed.Digest, observed.Mode, observed.Owner), observed.Error)
	}
}

func checkStorage(add func(string, string, bool, string, string, string), manifest *clustermanifest.Manifest, node clustermanifest.Node, facts HostFacts) {
	devices := map[string]DeviceFact{}
	for _, device := range facts.Devices {
		devices[device.DeviceByID] = device
	}
	mounts := map[string]MountFact{}
	for _, mount := range facts.Mounts {
		mounts[mount.Path] = mount
	}
	for _, claim := range node.StorageClaims {
		device, deviceFound := devices[claim.DeviceByID]
		add(CheckDevice, claim.ID, deviceFound && device.Exists && device.ResolvedPath != "", claim.DeviceByID,
			fmt.Sprintf("exists=%t resolved=%s", device.Exists, device.ResolvedPath), device.Error)
		add(CheckFilesystemUUID, claim.ID, deviceFound && strings.EqualFold(device.FilesystemUUID, claim.FilesystemUUID), claim.FilesystemUUID, device.FilesystemUUID, "filesystem UUID must match the explicit claim")
		mount, mountFound := mounts[claim.MountPath]
		mountOK := mountFound && mount.Mounted && device.Exists && mount.ResolvedSource != "" && mount.ResolvedSource == device.ResolvedPath
		add(CheckMount, claim.ID, mountOK, "mounted from "+device.ResolvedPath, fmt.Sprintf("mounted=%t source=%s resolved=%s", mount.Mounted, mount.Source, mount.ResolvedSource), "root-filesystem fallback and unclaimed devices are forbidden")
		ownershipOK := mountFound && mount.Owner == manifest.Spec.Bundle.ServiceUser && mount.Mode&0o022 == 0 && mount.Mode&0o500 == 0o500
		add(CheckMountOwnership, claim.ID, ownershipOK, "owner="+manifest.Spec.Bundle.ServiceUser+" mode without group/other write", fmt.Sprintf("owner=%s mode=%04o", mount.Owner, mount.Mode), mount.Error)
		capacityOK := mountFound && mount.TotalBytes >= uint64(claim.MinimumFreeBytes+claim.ReservedBytes) && mount.FreeBytes >= uint64(claim.MinimumFreeBytes)
		add(CheckCapacity, claim.ID, capacityOK,
			fmt.Sprintf("total>=%d free>=%d", claim.MinimumFreeBytes+claim.ReservedBytes, claim.MinimumFreeBytes),
			fmt.Sprintf("total=%d free=%d", mount.TotalBytes, mount.FreeBytes), mount.Error)
	}
}

func containsRole(roles []string, want string) bool {
	for _, role := range roles {
		if role == want {
			return true
		}
	}
	return false
}

func RequiredInstalledPaths(manifest *clustermanifest.Manifest, policy clustermanifest.ValidationPolicy, nodeID string) ([]string, error) {
	rendered, err := clustermanifest.Render(manifest, policy)
	if err != nil {
		return nil, err
	}
	node, bundle, err := desiredNodeAndBundle(manifest, rendered, nodeID)
	if err != nil {
		return nil, err
	}
	paths := make([]string, 0, len(bundle.Files)+2)
	for _, file := range bundle.Files {
		paths = append(paths, file.InstallPath)
	}
	paths = append(paths, filepath.Join(manifest.Spec.Bundle.BinaryDirectory, "sbs-data"))
	if containsRole(node.Roles, "sbs-service-active") || containsRole(node.Roles, "sbs-service-standby") {
		paths = append(paths, filepath.Join(manifest.Spec.Bundle.BinaryDirectory, "sbs-service"))
	}
	sort.Strings(paths)
	return paths, nil
}
