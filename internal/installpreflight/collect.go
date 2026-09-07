package installpreflight

import (
	"bufio"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net"
	"os"
	"os/user"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/nosway/namrbd/internal/clustermanifest"
)

type CollectRequest struct {
	Manifest *clustermanifest.Manifest
	Policy   clustermanifest.ValidationPolicy
	NodeID   string
	HostRoot string
	Now      func() time.Time
}

// CollectLocalFacts reads local OS state only. HostRoot exists for isolated
// fixtures; production callers leave it as "/". No file, mount, network, or
// process state is changed.
func CollectLocalFacts(request CollectRequest) (*HostFacts, error) {
	if request.Manifest == nil {
		return nil, fmt.Errorf("manifest is required")
	}
	root := filepath.Clean(strings.TrimSpace(request.HostRoot))
	if root == "." || root == "" {
		root = "/"
	}
	if request.Now == nil {
		request.Now = time.Now
	}
	rendered, err := clustermanifest.Render(request.Manifest, request.Policy)
	if err != nil {
		return nil, err
	}
	node, _, err := desiredNodeAndBundle(request.Manifest, rendered, request.NodeID)
	if err != nil {
		return nil, err
	}
	hostname, err := os.Hostname()
	if err != nil {
		return nil, fmt.Errorf("read local hostname: %w", err)
	}
	facts := &HostFacts{
		APIVersion: APIVersion, Kind: FactsKind, Hostname: hostname,
		ObservedAt: request.Now().UTC(), KernelRelease: localKernelRelease(root), EvidenceSource: "local-os",
	}
	facts.Interfaces, err = localInterfaces()
	if err != nil {
		return nil, err
	}
	facts.ListeningPorts, facts.PortObservationSupported = localListeningPorts(root)
	paths, err := RequiredInstalledPaths(request.Manifest, request.Policy, request.NodeID)
	if err != nil {
		return nil, err
	}
	for _, logicalPath := range paths {
		facts.Files = append(facts.Files, inspectFile(root, logicalPath))
	}
	for _, claim := range node.StorageClaims {
		device := inspectDevice(root, claim)
		facts.Devices = append(facts.Devices, device)
		facts.Mounts = append(facts.Mounts, inspectMount(root, claim.MountPath))
	}
	sortFacts(facts)
	return facts, nil
}

func localInterfaces() ([]InterfaceFact, error) {
	interfaces, err := net.Interfaces()
	if err != nil {
		return nil, fmt.Errorf("list local interfaces: %w", err)
	}
	result := make([]InterfaceFact, 0, len(interfaces))
	for _, iface := range interfaces {
		fact := InterfaceFact{Name: iface.Name, Up: iface.Flags&net.FlagUp != 0, MTU: iface.MTU}
		addresses, err := iface.Addrs()
		if err != nil {
			return nil, fmt.Errorf("list addresses for interface %s: %w", iface.Name, err)
		}
		for _, address := range addresses {
			raw := address.String()
			if host, _, err := net.ParseCIDR(raw); err == nil {
				raw = host.String()
			}
			fact.Addresses = append(fact.Addresses, raw)
		}
		result = append(result, fact)
	}
	return result, nil
}

func localKernelRelease(root string) string {
	if raw, err := os.ReadFile(rootedPath(root, "/proc/sys/kernel/osrelease")); err == nil {
		if value := strings.TrimSpace(string(raw)); value != "" {
			return value
		}
	}
	return runtime.GOOS + "/" + runtime.GOARCH
}

func localListeningPorts(root string) ([]int, bool) {
	seen := map[int]bool{}
	supported := false
	for _, name := range []string{"/proc/net/tcp", "/proc/net/tcp6"} {
		file, err := os.Open(rootedPath(root, name))
		if err != nil {
			continue
		}
		supported = true
		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			fields := strings.Fields(scanner.Text())
			if len(fields) < 4 || fields[3] != "0A" {
				continue
			}
			_, rawPort, ok := strings.Cut(fields[1], ":")
			if !ok {
				continue
			}
			port, err := strconv.ParseInt(rawPort, 16, 32)
			if err == nil && port > 0 {
				seen[int(port)] = true
			}
		}
		_ = file.Close()
	}
	ports := make([]int, 0, len(seen))
	for port := range seen {
		ports = append(ports, port)
	}
	return ports, supported
}

func inspectFile(root, logicalPath string) FileFact {
	fact := FileFact{Path: logicalPath}
	path := rootedPath(root, logicalPath)
	raw, err := os.ReadFile(path)
	if err != nil {
		fact.Error = err.Error()
		return fact
	}
	info, err := os.Stat(path)
	if err != nil {
		fact.Error = err.Error()
		return fact
	}
	sum := sha256.Sum256(raw)
	fact.Exists = true
	fact.Digest = "sha256:" + hex.EncodeToString(sum[:])
	fact.Mode = uint32(info.Mode().Perm())
	fact.Owner = fileOwner(info)
	return fact
}

func inspectDevice(root string, claim clustermanifest.StorageClaim) DeviceFact {
	fact := DeviceFact{DeviceByID: claim.DeviceByID}
	resolved, err := filepath.EvalSymlinks(rootedPath(root, claim.DeviceByID))
	if err != nil {
		fact.Error = err.Error()
		return fact
	}
	fact.Exists = true
	fact.ResolvedPath = logicalResolvedPath(root, resolved)
	uuidPath := rootedPath(root, filepath.Join("/dev/disk/by-uuid", claim.FilesystemUUID))
	if uuidResolved, err := filepath.EvalSymlinks(uuidPath); err == nil && logicalResolvedPath(root, uuidResolved) == fact.ResolvedPath {
		fact.FilesystemUUID = strings.ToLower(claim.FilesystemUUID)
	}
	return fact
}

func inspectMount(root, logicalPath string) MountFact {
	fact := MountFact{Path: logicalPath}
	mounts, err := readMountInfo(root)
	if err != nil {
		fact.Error = err.Error()
		return fact
	}
	for _, mount := range mounts {
		if mount.Path == logicalPath {
			fact = mount
			break
		}
	}
	if !fact.Mounted {
		fact.Error = "mount point is not present in mountinfo"
		return fact
	}
	physicalPath := rootedPath(root, logicalPath)
	info, err := os.Stat(physicalPath)
	if err != nil {
		fact.Error = err.Error()
		return fact
	}
	fact.Owner = fileOwner(info)
	fact.Mode = uint32(info.Mode().Perm())
	total, free, err := filesystemCapacity(physicalPath)
	if err != nil {
		fact.Error = err.Error()
		return fact
	}
	fact.TotalBytes, fact.FreeBytes = total, free
	return fact
}

func readMountInfo(root string) ([]MountFact, error) {
	file, err := os.Open(rootedPath(root, "/proc/self/mountinfo"))
	if err != nil {
		return nil, fmt.Errorf("read mountinfo: %w", err)
	}
	defer file.Close()
	var result []MountFact
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		separator := -1
		for i, field := range fields {
			if field == "-" {
				separator = i
				break
			}
		}
		if len(fields) < 7 || separator < 0 || separator+2 >= len(fields) {
			continue
		}
		path := unescapeMountInfo(fields[4])
		source := unescapeMountInfo(fields[separator+2])
		resolvedSource := source
		if resolved, err := filepath.EvalSymlinks(rootedPath(root, source)); err == nil {
			resolvedSource = logicalResolvedPath(root, resolved)
		}
		result = append(result, MountFact{Path: path, Mounted: true, Source: source, ResolvedSource: resolvedSource, FilesystemType: fields[separator+1]})
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("scan mountinfo: %w", err)
	}
	return result, nil
}

func unescapeMountInfo(value string) string {
	replacer := strings.NewReplacer(`\040`, " ", `\011`, "\t", `\012`, "\n", `\134`, `\`)
	return replacer.Replace(value)
}

func rootedPath(root, logicalPath string) string {
	if root == "/" {
		return filepath.Clean(logicalPath)
	}
	return filepath.Join(root, strings.TrimPrefix(filepath.Clean(logicalPath), string(filepath.Separator)))
}

func logicalResolvedPath(root, physicalPath string) string {
	physicalPath = filepath.Clean(physicalPath)
	if root == "/" {
		return physicalPath
	}
	relative, err := filepath.Rel(root, physicalPath)
	if err != nil || strings.HasPrefix(relative, "..") {
		return physicalPath
	}
	return string(filepath.Separator) + relative
}

func fileOwner(info os.FileInfo) string {
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return ""
	}
	rawUID := strconv.FormatUint(uint64(stat.Uid), 10)
	owner, err := user.LookupId(rawUID)
	if err != nil {
		return rawUID
	}
	return owner.Username
}
