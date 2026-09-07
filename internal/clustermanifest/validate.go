package clustermanifest

import (
	"fmt"
	"net"
	"net/netip"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"unicode"
)

const (
	CodeSchema             = "AD_MANIFEST_SCHEMA_INVALID"
	CodeIdentity           = "AD_MANIFEST_IDENTITY_INVALID"
	CodeTopology           = "AD_MANIFEST_TOPOLOGY_INVALID"
	CodeDuplicate          = "AD_MANIFEST_DUPLICATE"
	CodeServicePlacement   = "AD_MANIFEST_SERVICE_PLACEMENT_INVALID"
	CodeArtifact           = "AD_MANIFEST_ARTIFACT_INVALID"
	CodeArtifactUnapproved = "AD_MANIFEST_ARTIFACT_UNAPPROVED"
	CodeMetadataAuthority  = "AD_MANIFEST_METADATA_AUTHORITY_INVALID"
	CodeStorageClaim       = "AD_MANIFEST_STORAGE_CLAIM_INVALID"
	CodeDestructive        = "AD_MANIFEST_DESTRUCTIVE_PROVISIONING_FORBIDDEN"
	CodeSecretReference    = "AD_MANIFEST_SECRET_REFERENCE_INVALID"
	CodeSecretLiteral      = "AD_MANIFEST_SECRET_LITERAL_FORBIDDEN"
)

var (
	nodeIDPattern      = regexp.MustCompile(`^node([1-9][0-9]{0,2})$`)
	sha256Pattern      = regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)
	uuidPattern        = regexp.MustCompile(`^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[1-5][0-9a-fA-F]{3}-[89abAB][0-9a-fA-F]{3}-[0-9a-fA-F]{12}$`)
	namePattern        = regexp.MustCompile(`^[a-z0-9](?:[a-z0-9.-]*[a-z0-9])?$`)
	serviceUserPattern = regexp.MustCompile(`^[a-z_][a-z0-9_-]{0,31}$`)
)

type Issue struct {
	Code    string `json:"code"`
	Field   string `json:"field"`
	Message string `json:"message"`
}

type ValidationError struct {
	Issues []Issue `json:"issues"`
}

func (e *ValidationError) Error() string {
	if e == nil || len(e.Issues) == 0 {
		return "cluster manifest validation failed"
	}
	parts := make([]string, 0, len(e.Issues))
	for _, issue := range e.Issues {
		parts = append(parts, fmt.Sprintf("%s %s: %s", issue.Code, issue.Field, issue.Message))
	}
	return "cluster manifest validation failed: " + strings.Join(parts, "; ")
}

// ValidationPolicy carries approval data that must come from outside the
// manifest. A manifest cannot approve its own artifact digest.
type ValidationPolicy struct {
	ApprovedArtifactDigests []string
	RequireArtifactApproval bool
}

func Validate(manifest *Manifest, policy ValidationPolicy) error {
	var issues []Issue
	add := func(code, field, format string, args ...any) {
		issues = append(issues, Issue{Code: code, Field: field, Message: fmt.Sprintf(format, args...)})
	}
	if manifest == nil {
		return &ValidationError{Issues: []Issue{{Code: CodeSchema, Field: "manifest", Message: "manifest is nil"}}}
	}

	if manifest.APIVersion != APIVersion {
		add(CodeSchema, "apiVersion", "got %q, want %q", manifest.APIVersion, APIVersion)
	}
	if manifest.Kind != Kind {
		add(CodeSchema, "kind", "got %q, want %q", manifest.Kind, Kind)
	}
	if !namePattern.MatchString(manifest.Metadata.Name) {
		add(CodeIdentity, "metadata.name", "must be a non-empty lowercase DNS-style name")
	}
	if strings.TrimSpace(manifest.Metadata.Revision) == "" {
		add(CodeIdentity, "metadata.revision", "is required")
	}
	if strings.TrimSpace(manifest.Spec.ClusterID) == "" {
		add(CodeIdentity, "spec.clusterId", "is required")
	}
	if strings.TrimSpace(manifest.Spec.SBSClusterID) == "" {
		add(CodeIdentity, "spec.sbsClusterId", "is required")
	}
	if manifest.Spec.ExpectedNodeCount != ExactNodeCount {
		add(CodeTopology, "spec.expectedNodeCount", "got %d, want %d", manifest.Spec.ExpectedNodeCount, ExactNodeCount)
	}
	if manifest.Spec.ExpectedZones != ExactZoneCount {
		add(CodeTopology, "spec.expectedZones", "got %d, want %d", manifest.Spec.ExpectedZones, ExactZoneCount)
	}

	validateArtifact(manifest, policy, add)
	validateMetadataAuthority(manifest, add)
	validateBundleSettings(manifest, add)
	validateServiceSettings(manifest, add)
	secretRefs := validateSecretReferences(manifest, add)
	if manifest.Spec.MetadataAuthority.TLS.Enabled {
		if _, ok := secretRefs[manifest.Spec.MetadataAuthority.TLS.KeyRef]; !ok {
			add(CodeSecretReference, "spec.metadataAuthority.tls.keyRef", "does not name a declared secret reference")
		}
	}
	validateServicePlacement(manifest, add)
	validateRollout(manifest, add)
	validateNodes(manifest, add)

	sort.SliceStable(issues, func(i, j int) bool {
		if issues[i].Field != issues[j].Field {
			return issues[i].Field < issues[j].Field
		}
		if issues[i].Code != issues[j].Code {
			return issues[i].Code < issues[j].Code
		}
		return issues[i].Message < issues[j].Message
	})
	if len(issues) > 0 {
		return &ValidationError{Issues: issues}
	}
	return nil
}

func validateBundleSettings(manifest *Manifest, add func(string, string, string, ...any)) {
	b := manifest.Spec.Bundle
	if b.ConfigRevision <= 0 {
		add(CodeSchema, "spec.bundle.configRevision", "must be greater than zero")
	}
	for field, value := range map[string]string{
		"spec.bundle.binaryDirectory": b.BinaryDirectory,
		"spec.bundle.configDirectory": b.ConfigDirectory,
		"spec.bundle.dataStatePath":   b.DataStatePath,
	} {
		if !filepath.IsAbs(value) || filepath.Clean(value) == "/" {
			add(CodeSchema, field, "must be an absolute non-root path")
		} else if containsSpaceOrControl(value) {
			add(CodeSchema, field, "must not contain whitespace or control characters")
		}
	}
	if !serviceUserPattern.MatchString(b.ServiceUser) || b.ServiceUser == "root" {
		add(CodeSchema, "spec.bundle.serviceUser", "must name a non-root service account using lowercase account characters")
	}
}

func validateServiceSettings(manifest *Manifest, add func(string, string, string, ...any)) {
	s := manifest.Spec.ServiceConfig
	if !filepath.IsAbs(s.PayloadRoot) || filepath.Clean(s.PayloadRoot) == "/" {
		add(CodeSchema, "spec.serviceConfig.payloadRoot", "must be an absolute non-root path")
	}
	if s.TiKV.TimeoutSeconds <= 0 || s.TiKV.TimeoutSeconds > 60 {
		add(CodeSchema, "spec.serviceConfig.tikv.timeoutSeconds", "must be in 1..60")
	}
	if s.TiKV.ScanPageSize <= 0 || s.TiKV.ScanPageSize > 512 {
		add(CodeSchema, "spec.serviceConfig.tikv.scanPageSize", "must be in 1..512")
	}
	if s.TiKV.BatchGetSize <= 0 || s.TiKV.BatchGetSize > 128 {
		add(CodeSchema, "spec.serviceConfig.tikv.batchGetSize", "must be in 1..128")
	}
	if s.Leader.LeaseDurationSeconds <= 0 || s.Leader.RenewIntervalSeconds <= 0 || s.Leader.RenewIntervalSeconds >= s.Leader.LeaseDurationSeconds {
		add(CodeSchema, "spec.serviceConfig.leader", "renew interval must be positive and less than the lease duration")
	}
	if s.Health.ShardCount < 4 || s.Health.ConcurrencyPerShard <= 0 || s.Health.ConcurrencyPerShard > 16 || s.Health.IntervalSeconds <= 0 || s.Health.TimeoutSeconds <= 0 || s.Health.SuspectThreshold <= 0 || s.Health.DownThreshold < s.Health.SuspectThreshold || s.Health.RecoveryCooldownSeconds <= 0 {
		add(CodeSchema, "spec.serviceConfig.health", "must satisfy the large-scale bounded health profile")
	}
	if !s.WriteEffects.ServiceOwned || !s.WriteEffects.NativeAllocationFastPath || s.WriteEffects.BatchMax <= 0 || s.WriteEffects.LaneBucketCount <= 0 || s.WriteEffects.AsyncMutationFinalize {
		add(CodeSchema, "spec.serviceConfig.writeEffects", "must preserve the accepted service-owned write-effect profile")
	}
}

func validateArtifact(manifest *Manifest, policy ValidationPolicy, add func(string, string, string, ...any)) {
	a := manifest.Spec.Artifact
	if strings.TrimSpace(a.Version) == "" {
		add(CodeArtifact, "spec.artifact.version", "is required")
	}
	if !sha256Pattern.MatchString(a.Digest) {
		add(CodeArtifact, "spec.artifact.digest", "must be sha256 followed by 64 lowercase hexadecimal characters")
	}
	if strings.TrimSpace(a.ApprovalRef) == "" {
		add(CodeArtifact, "spec.artifact.approvalRef", "is required")
	}
	requiredBinaries := []string{"sbs-data", "sbs-service", "sbsctl"}
	if len(a.BinaryDigests) != len(requiredBinaries) {
		add(CodeArtifact, "spec.artifact.binaryDigests", "must contain exactly %v", requiredBinaries)
	}
	for _, name := range requiredBinaries {
		digest, ok := a.BinaryDigests[name]
		if !ok || !sha256Pattern.MatchString(digest) {
			add(CodeArtifact, "spec.artifact.binaryDigests."+name, "must be a canonical sha256 digest")
		}
	}
	for name := range a.BinaryDigests {
		if !containsExactString(requiredBinaries, name) {
			add(CodeArtifact, "spec.artifact.binaryDigests."+name, "is not an approved Phase AD binary name")
		}
	}
	if !policy.RequireArtifactApproval && len(policy.ApprovedArtifactDigests) == 0 {
		return
	}
	approved := false
	for _, digest := range policy.ApprovedArtifactDigests {
		if strings.TrimSpace(digest) == a.Digest {
			approved = true
			break
		}
	}
	if !approved {
		add(CodeArtifactUnapproved, "spec.artifact.digest", "digest is not in the external approval policy")
	}
}

func validateMetadataAuthority(manifest *Manifest, add func(string, string, string, ...any)) {
	a := manifest.Spec.MetadataAuthority
	if len(a.PDEndpoints) != 3 {
		add(CodeMetadataAuthority, "spec.metadataAuthority.pdEndpoints", "got %d endpoints, want exactly 3", len(a.PDEndpoints))
	}
	seen := map[string]bool{}
	for i, endpoint := range a.PDEndpoints {
		field := fmt.Sprintf("spec.metadataAuthority.pdEndpoints[%d]", i)
		endpoint = strings.TrimSpace(endpoint)
		if !strings.HasPrefix(endpoint, "https://") {
			add(CodeMetadataAuthority, field, "must use https://")
		}
		if seen[endpoint] {
			add(CodeDuplicate, field, "duplicates metadata endpoint %q", endpoint)
		}
		seen[endpoint] = true
	}
	if strings.TrimSpace(a.Keyspace) == "" {
		add(CodeMetadataAuthority, "spec.metadataAuthority.keyspace", "is required")
	}
	if a.APIVersion != "v2" {
		add(CodeMetadataAuthority, "spec.metadataAuthority.apiVersion", "got %q, want %q", a.APIVersion, "v2")
	}
	if !a.TLS.Enabled {
		add(CodeMetadataAuthority, "spec.metadataAuthority.tls.enabled", "must be true")
	}
	if !filepath.IsAbs(a.TLS.CertFile) {
		add(CodeMetadataAuthority, "spec.metadataAuthority.tls.certFile", "must be an absolute path")
	}
	if strings.TrimSpace(a.TLS.KeyRef) == "" {
		add(CodeSecretReference, "spec.metadataAuthority.tls.keyRef", "is required")
	}
}

func validateSecretReferences(manifest *Manifest, add func(string, string, string, ...any)) map[string]SecretReference {
	refs := make(map[string]SecretReference, len(manifest.Spec.SecretRefs))
	for i, ref := range manifest.Spec.SecretRefs {
		field := fmt.Sprintf("spec.secretRefs[%d]", i)
		if strings.TrimSpace(ref.Name) == "" {
			add(CodeSecretReference, field+".name", "is required")
		}
		if _, ok := refs[ref.Name]; ok {
			add(CodeDuplicate, field+".name", "duplicates secret reference %q", ref.Name)
		}
		refs[ref.Name] = ref
		set := 0
		for _, value := range []string{ref.File, ref.Env, ref.KMS} {
			if strings.TrimSpace(value) != "" {
				set++
			}
		}
		if set != 1 {
			add(CodeSecretReference, field, "must name exactly one of file, env, or kms")
		}
		if ref.File != "" && !filepath.IsAbs(ref.File) {
			add(CodeSecretReference, field+".file", "must be an absolute path")
		}
	}
	return refs
}

func validateServicePlacement(manifest *Manifest, add func(string, string, string, ...any)) {
	p := manifest.Spec.ServicePlacement
	if !sameStringSet(p.ActiveHosts, ExactActiveServiceHosts) {
		add(CodeServicePlacement, "spec.servicePlacement.activeHosts", "must be exactly %v", ExactActiveServiceHosts)
	}
	if !sameStringSet(p.StandbyCandidates, ExactStandbyServiceHosts) {
		add(CodeServicePlacement, "spec.servicePlacement.standbyCandidates", "must be exactly %v", ExactStandbyServiceHosts)
	}
	seen := map[string]string{}
	for _, item := range []struct {
		kind  string
		hosts []string
	}{
		{kind: "active", hosts: p.ActiveHosts},
		{kind: "standby", hosts: p.StandbyCandidates},
	} {
		for _, host := range item.hosts {
			if prior, ok := seen[host]; ok {
				add(CodeServicePlacement, "spec.servicePlacement", "host %q is both %s and %s or is duplicated", host, prior, item.kind)
			}
			seen[host] = item.kind
		}
	}
}

func validateRollout(manifest *Manifest, add func(string, string, string, ...any)) {
	want := make([]string, 0, ExactZoneCount)
	for i := 1; i <= ExactZoneCount; i++ {
		want = append(want, zoneID(i))
	}
	if !sameStrings(manifest.Spec.Rollout.Order, want) {
		add(CodeTopology, "spec.rollout.order", "must be exactly %v", want)
	}
	if manifest.Spec.Rollout.MaxParallelPerZone != DefaultMaxParallelPerZone {
		add(CodeTopology, "spec.rollout.maxParallelPerZone", "got %d, want %d", manifest.Spec.Rollout.MaxParallelPerZone, DefaultMaxParallelPerZone)
	}
	if !manifest.Spec.Rollout.PauseOnFailure {
		add(CodeTopology, "spec.rollout.pauseOnFailure", "must be true")
	}
}

func validateNodes(manifest *Manifest, add func(string, string, string, ...any)) {
	if len(manifest.Spec.Nodes) != ExactNodeCount {
		add(CodeTopology, "spec.nodes", "got %d nodes, want exactly %d", len(manifest.Spec.Nodes), ExactNodeCount)
	}
	nodeIDs := map[string]bool{}
	hostnames := map[string]bool{}
	addresses := map[string]string{}
	endpoints := map[string]string{}
	devices := map[string]string{}
	filesystemUUIDs := map[string]string{}
	zoneCounts := map[string]int{}
	active := stringSet(manifest.Spec.ServicePlacement.ActiveHosts)
	standby := stringSet(manifest.Spec.ServicePlacement.StandbyCandidates)

	for i, node := range manifest.Spec.Nodes {
		base := fmt.Sprintf("spec.nodes[%d]", i)
		match := nodeIDPattern.FindStringSubmatch(node.ID)
		nodeNumber := 0
		if match == nil {
			add(CodeTopology, base+".id", "must be node1 through node160, got %q", node.ID)
		} else {
			nodeNumber, _ = strconv.Atoi(match[1])
			if nodeNumber > ExactNodeCount {
				add(CodeTopology, base+".id", "node number %d exceeds %d", nodeNumber, ExactNodeCount)
			}
		}
		if nodeIDs[node.ID] {
			add(CodeDuplicate, base+".id", "duplicates node ID %q", node.ID)
		}
		nodeIDs[node.ID] = true
		if node.Hostname != node.ID {
			add(CodeTopology, base+".hostname", "got %q, want exact hostname %q", node.Hostname, node.ID)
		}
		if hostnames[node.Hostname] {
			add(CodeDuplicate, base+".hostname", "duplicates hostname %q", node.Hostname)
		}
		hostnames[node.Hostname] = true

		validateAddress(node.ManagementAddress, base+".managementAddress", addresses, add)
		validateAddress(node.DataAddress, base+".dataAddress", addresses, add)
		validateEndpoint(node.DataEndpoint, node.DataAddress, base+".dataEndpoint", endpoints, add)
		validateEndpoint(node.AdminEndpoint, node.ManagementAddress, base+".adminEndpoint", endpoints, add)
		validateEndpoint(node.MetricsEndpoint, node.ManagementAddress, base+".metricsEndpoint", endpoints, add)

		if nodeNumber >= 1 && nodeNumber <= ExactNodeCount {
			wantZone := zoneID((nodeNumber-1)/ExactNodesPerZone + 1)
			if node.Location.Zone != wantZone {
				add(CodeTopology, base+".location.zone", "node %s must be in %s, got %q", node.ID, wantZone, node.Location.Zone)
			}
		}
		if strings.TrimSpace(node.Location.Rack) == "" {
			add(CodeTopology, base+".location.rack", "is required")
		}
		zoneCounts[node.Location.Zone]++

		wantRoles := []string{"sbs-data"}
		if active[node.ID] {
			wantRoles = append(wantRoles, "sbs-service-active")
		}
		if standby[node.ID] {
			wantRoles = append(wantRoles, "sbs-service-standby")
		}
		if !sameStringSet(node.Roles, wantRoles) {
			add(CodeServicePlacement, base+".roles", "got %v, want exactly %v", node.Roles, wantRoles)
		}
		isServiceHost := active[node.ID] || standby[node.ID]
		for field, endpoint := range map[string]string{
			base + ".serviceGrpcEndpoint":    node.ServiceGRPCEndpoint,
			base + ".serviceHttpEndpoint":    node.ServiceHTTPEndpoint,
			base + ".serviceMetricsEndpoint": node.ServiceMetricsEndpoint,
		} {
			if isServiceHost {
				validateEndpoint(endpoint, node.ManagementAddress, field, endpoints, add)
			} else if strings.TrimSpace(endpoint) != "" {
				add(CodeServicePlacement, field, "must be empty on a data-only host")
			}
		}
		validateStorageClaims(node, base, devices, filesystemUUIDs, add)
	}

	for i := 1; i <= ExactNodeCount; i++ {
		id := fmt.Sprintf("node%d", i)
		if !nodeIDs[id] {
			add(CodeTopology, "spec.nodes", "missing required node %q", id)
		}
	}
	for i := 1; i <= ExactZoneCount; i++ {
		zone := zoneID(i)
		if zoneCounts[zone] != ExactNodesPerZone {
			add(CodeTopology, "spec.nodes.location.zone", "%s has %d nodes, want %d", zone, zoneCounts[zone], ExactNodesPerZone)
		}
	}
}

func validateAddress(value, field string, seen map[string]string, add func(string, string, string, ...any)) {
	addr, err := netip.ParseAddr(strings.TrimSpace(value))
	if err != nil {
		add(CodeTopology, field, "must be a literal IP address")
		return
	}
	canonical := addr.String()
	if prior, ok := seen[canonical]; ok {
		add(CodeDuplicate, field, "address %q is already used by %s", canonical, prior)
	}
	seen[canonical] = field
}

func validateEndpoint(value, wantHost, field string, seen map[string]string, add func(string, string, string, ...any)) {
	host, port, err := net.SplitHostPort(strings.TrimSpace(value))
	if err != nil {
		add(CodeTopology, field, "must be host:port with an explicit numeric port")
		return
	}
	if host != wantHost {
		add(CodeTopology, field, "host %q must match declared address %q", host, wantHost)
	}
	p, err := strconv.Atoi(port)
	if err != nil || p < 1 || p > 65535 {
		add(CodeTopology, field, "port %q is not in 1..65535", port)
	}
	canonical := net.JoinHostPort(host, port)
	if prior, ok := seen[canonical]; ok {
		add(CodeDuplicate, field, "endpoint %q is already used by %s", canonical, prior)
	}
	seen[canonical] = field
}

func validateStorageClaims(node Node, base string, devices, filesystemUUIDs map[string]string, add func(string, string, string, ...any)) {
	if len(node.StorageClaims) == 0 {
		add(CodeStorageClaim, base+".storageClaims", "at least one explicit storage claim is required")
		return
	}
	claimIDs := map[string]bool{}
	mounts := map[string]bool{}
	for i, claim := range node.StorageClaims {
		field := fmt.Sprintf("%s.storageClaims[%d]", base, i)
		if strings.TrimSpace(claim.ID) == "" {
			add(CodeStorageClaim, field+".id", "is required")
		}
		if claimIDs[claim.ID] {
			add(CodeDuplicate, field+".id", "duplicates claim ID %q on node %s", claim.ID, node.ID)
		}
		claimIDs[claim.ID] = true
		if !strings.HasPrefix(claim.DeviceByID, "/dev/disk/by-id/") || strings.ContainsAny(claim.DeviceByID, "<>") {
			add(CodeStorageClaim, field+".deviceById", "must be a concrete /dev/disk/by-id path")
		}
		if prior, ok := devices[claim.DeviceByID]; ok {
			add(CodeDuplicate, field+".deviceById", "device claim %q is already used by %s", claim.DeviceByID, prior)
		}
		devices[claim.DeviceByID] = node.ID
		if !uuidPattern.MatchString(claim.FilesystemUUID) {
			add(CodeStorageClaim, field+".filesystemUUID", "must be a concrete UUID")
		}
		if prior, ok := filesystemUUIDs[strings.ToLower(claim.FilesystemUUID)]; ok {
			add(CodeDuplicate, field+".filesystemUUID", "filesystem UUID is already used by %s", prior)
		}
		filesystemUUIDs[strings.ToLower(claim.FilesystemUUID)] = node.ID
		cleanMount := filepath.Clean(claim.MountPath)
		if !filepath.IsAbs(claim.MountPath) || cleanMount == "/" || !strings.HasPrefix(cleanMount, "/srv/namrbd/") {
			add(CodeStorageClaim, field+".mountPath", "must be an absolute path below /srv/namrbd and not root")
		}
		if mounts[cleanMount] {
			add(CodeDuplicate, field+".mountPath", "duplicates mount path %q on node %s", cleanMount, node.ID)
		}
		mounts[cleanMount] = true
		if claim.MinimumFreeBytes <= 0 {
			add(CodeStorageClaim, field+".minimumFreeBytes", "must be greater than zero")
		}
		if claim.ReservedBytes <= 0 {
			add(CodeStorageClaim, field+".reservedBytes", "must be greater than zero")
		}
		if claim.Shards <= 0 {
			add(CodeStorageClaim, field+".shards", "must be greater than zero")
		}
		if claim.Weight <= 0 || claim.Weight > 1000 {
			add(CodeStorageClaim, field+".weight", "must be in 1..1000")
		}
		if claim.MinimumFreeBytes <= claim.ReservedBytes {
			add(CodeStorageClaim, field, "minimumFreeBytes must be greater than reservedBytes")
		}
		if claim.DestructiveProvisioning {
			add(CodeDestructive, field+".destructiveProvisioning", "must remain false; formatting and mounting are outside AD-IMPL-001")
		}
	}
}

func sameStringSet(got, want []string) bool {
	if len(got) != len(want) {
		return false
	}
	a := append([]string(nil), got...)
	b := append([]string(nil), want...)
	sort.Strings(a)
	sort.Strings(b)
	return sameStrings(a, b)
}

func sameStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func stringSet(values []string) map[string]bool {
	result := make(map[string]bool, len(values))
	for _, value := range values {
		result[value] = true
	}
	return result
}

func containsSpaceOrControl(value string) bool {
	return strings.IndexFunc(value, func(r rune) bool {
		return unicode.IsSpace(r) || unicode.IsControl(r)
	}) >= 0
}

func containsExactString(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

func zoneID(number int) string { return fmt.Sprintf("zone-%02d", number) }
