package clustermanifest

import (
	"encoding/json"
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

const testArtifactDigest = "sha256:c3bd6d1fdc4b1a1239e4036e294b71fb7ea0880ba990a486d1cedaede3f4eecf"

func TestShippedExactManifestFixtureValidates(t *testing.T) {
	manifest, err := Load(filepath.Join("..", "..", "configs", "sbs-cluster-160.example.yaml"))
	if err != nil {
		t.Fatalf("Load shipped manifest: %v", err)
	}
	canonical, err := Canonicalize(manifest)
	if err != nil {
		t.Fatalf("Canonicalize shipped manifest: %v", err)
	}
	if err := Validate(canonical, testPolicy()); err != nil {
		t.Fatalf("Validate shipped manifest: %v", err)
	}
}

func TestExactManifestValidatesAndCanonicalDigestIsStable(t *testing.T) {
	manifest := exactManifestForTest()
	policy := testPolicy()
	if err := Validate(manifest, policy); err != nil {
		t.Fatalf("Validate: %v", err)
	}
	digestA, err := Digest(manifest)
	if err != nil {
		t.Fatalf("Digest: %v", err)
	}

	reversed := exactManifestForTest()
	for left, right := 0, len(reversed.Spec.Nodes)-1; left < right; left, right = left+1, right-1 {
		reversed.Spec.Nodes[left], reversed.Spec.Nodes[right] = reversed.Spec.Nodes[right], reversed.Spec.Nodes[left]
	}
	for left, right := 0, len(reversed.Spec.MetadataAuthority.PDEndpoints)-1; left < right; left, right = left+1, right-1 {
		reversed.Spec.MetadataAuthority.PDEndpoints[left], reversed.Spec.MetadataAuthority.PDEndpoints[right] = reversed.Spec.MetadataAuthority.PDEndpoints[right], reversed.Spec.MetadataAuthority.PDEndpoints[left]
	}
	reversed.Spec.Nodes[0].Roles[0], reversed.Spec.Nodes[0].Roles[len(reversed.Spec.Nodes[0].Roles)-1] = reversed.Spec.Nodes[0].Roles[len(reversed.Spec.Nodes[0].Roles)-1], reversed.Spec.Nodes[0].Roles[0]
	digestB, err := Digest(reversed)
	if err != nil {
		t.Fatalf("Digest reversed: %v", err)
	}
	if digestA != digestB {
		t.Fatalf("canonical digest changed with set ordering: %s != %s", digestA, digestB)
	}
}

func TestParseRejectsUnknownFieldAndSecretLiteral(t *testing.T) {
	manifest := exactManifestForTest()
	raw, err := yaml.Marshal(manifest)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	withUnknown := strings.Replace(string(raw), "spec:\n", "spec:\n    password: pasted-secret\n", 1)
	_, err = Parse([]byte(withUnknown))
	assertValidationCode(t, err, CodeSecretLiteral)

	withUnknown = strings.Replace(string(raw), "spec:\n", "spec:\n    mysteryField: true\n", 1)
	if _, err := Parse([]byte(withUnknown)); err == nil || !strings.Contains(err.Error(), "field mysteryField not found") {
		t.Fatalf("Parse unknown field error=%v", err)
	}
}

func TestValidationRejectionMatrix(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*Manifest)
		code   string
	}{
		{name: "duplicate node", mutate: func(m *Manifest) { m.Spec.Nodes[36].ID = "node36" }, code: CodeDuplicate},
		{name: "node161", mutate: func(m *Manifest) { m.Spec.Nodes[159].ID = "node161"; m.Spec.Nodes[159].Hostname = "node161" }, code: CodeTopology},
		{name: "wrong zone", mutate: func(m *Manifest) { m.Spec.Nodes[39].Location.Zone = "zone-01" }, code: CodeTopology},
		{name: "duplicate address", mutate: func(m *Manifest) { m.Spec.Nodes[1].DataAddress = m.Spec.Nodes[0].DataAddress }, code: CodeDuplicate},
		{name: "duplicate endpoint", mutate: func(m *Manifest) { m.Spec.Nodes[1].DataEndpoint = m.Spec.Nodes[0].DataEndpoint }, code: CodeDuplicate},
		{name: "duplicate device", mutate: func(m *Manifest) {
			m.Spec.Nodes[1].StorageClaims[0].DeviceByID = m.Spec.Nodes[0].StorageClaims[0].DeviceByID
		}, code: CodeDuplicate},
		{name: "duplicate filesystem", mutate: func(m *Manifest) {
			m.Spec.Nodes[1].StorageClaims[0].FilesystemUUID = m.Spec.Nodes[0].StorageClaims[0].FilesystemUUID
		}, code: CodeDuplicate},
		{name: "active standby overlap", mutate: func(m *Manifest) { m.Spec.ServicePlacement.StandbyCandidates[0] = "node1" }, code: CodeServicePlacement},
		{name: "active same zone", mutate: func(m *Manifest) { m.Spec.ServicePlacement.ActiveHosts[2] = "node22" }, code: CodeServicePlacement},
		{name: "role drift", mutate: func(m *Manifest) { m.Spec.Nodes[0].Roles = []string{"sbs-data"} }, code: CodeServicePlacement},
		{name: "destructive provisioning", mutate: func(m *Manifest) { m.Spec.Nodes[0].StorageClaims[0].DestructiveProvisioning = true }, code: CodeDestructive},
		{name: "missing capacity", mutate: func(m *Manifest) { m.Spec.Nodes[0].StorageClaims[0].MinimumFreeBytes = 0 }, code: CodeStorageClaim},
		{name: "systemd path injection", mutate: func(m *Manifest) { m.Spec.Bundle.BinaryDirectory = "/usr/lib/namrbd\nExecStart=/bin/false" }, code: CodeSchema},
		{name: "systemd user injection", mutate: func(m *Manifest) { m.Spec.Bundle.ServiceUser = "namrbd\nExecStart" }, code: CodeSchema},
		{name: "missing binary digest", mutate: func(m *Manifest) { delete(m.Spec.Artifact.BinaryDigests, "sbs-data") }, code: CodeArtifact},
		{name: "unapproved artifact", mutate: func(m *Manifest) {
			m.Spec.Artifact.Digest = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
		}, code: CodeArtifactUnapproved},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			manifest := exactManifestForTest()
			tt.mutate(manifest)
			assertValidationCode(t, Validate(manifest, testPolicy()), tt.code)
		})
	}
}

func TestCanonicalJSONContainsNoRuntimeOrObservedState(t *testing.T) {
	raw, err := CanonicalJSON(exactManifestForTest())
	if err != nil {
		t.Fatalf("CanonicalJSON: %v", err)
	}
	var decoded map[string]any
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatalf("Unmarshal canonical JSON: %v", err)
	}
	for _, forbidden := range []string{"operationId", "membershipRevision", "daemonState", "observedCapacity", "secretValue"} {
		if strings.Contains(string(raw), forbidden) {
			t.Fatalf("canonical manifest contains observed/runtime field %q", forbidden)
		}
	}
}

func assertValidationCode(t *testing.T, err error, code string) {
	t.Helper()
	validationErr, ok := err.(*ValidationError)
	if !ok {
		t.Fatalf("error=%T %v, want *ValidationError with %s", err, err, code)
	}
	for _, issue := range validationErr.Issues {
		if issue.Code == code {
			return
		}
	}
	t.Fatalf("issues=%+v, want code %s", validationErr.Issues, code)
}

func testPolicy() ValidationPolicy {
	return ValidationPolicy{ApprovedArtifactDigests: []string{testArtifactDigest}, RequireArtifactApproval: true}
}

func exactManifestForTest() *Manifest {
	manifest := &Manifest{
		APIVersion: APIVersion,
		Kind:       Kind,
		Metadata:   Metadata{Name: "sbs-prod-160", Revision: "20260903-001"},
		Spec: Spec{
			ClusterID:         "namrbd-prod",
			SBSClusterID:      "sbs-prod-160",
			ExpectedNodeCount: ExactNodeCount,
			ExpectedZones:     ExactZoneCount,
			Artifact: Artifact{
				Version: "1.0.0", Digest: testArtifactDigest, ApprovalRef: "release/1.0.0/enterprise",
				BinaryDigests: map[string]string{
					"sbs-data":    "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
					"sbs-service": "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
					"sbsctl":      "sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
				},
			},
			MetadataAuthority: MetadataAuthority{
				PDEndpoints: []string{"https://pd-01.namrbd.internal:2379", "https://pd-02.namrbd.internal:2379", "https://pd-03.namrbd.internal:2379"},
				Keyspace:    "namrbd-prod", APIVersion: "v2",
				TLS: MetadataAuthorityTLS{Enabled: true, CertFile: "/etc/namrbd/tls/tikv-client.crt", KeyRef: "tikv-client-key"},
			},
			Bundle: BundleSettings{
				ConfigRevision: 20260903, BinaryDirectory: "/usr/lib/namrbd",
				ConfigDirectory: "/etc/namrbd", DataStatePath: "/var/lib/namrbd/sbs-data", ServiceUser: "namrbd",
			},
			ServiceConfig: ServiceSettings{
				PayloadRoot:  "/var/lib/namrbd/sbs",
				TiKV:         TiKVSettings{TimeoutSeconds: 5, ScanPageSize: 512, BatchGetSize: 128},
				Leader:       LeaderSettings{LeaseDurationSeconds: 15, RenewIntervalSeconds: 5},
				Health:       HealthSettings{ShardCount: 8, ConcurrencyPerShard: 16, IntervalSeconds: 10, TimeoutSeconds: 2, SuspectThreshold: 3, DownThreshold: 6, RecoveryCooldownSeconds: 30},
				WriteEffects: WriteEffectSettings{ServiceOwned: true, NativeAllocationFastPath: true, BatchMax: 64, LaneBucketCount: 8, AsyncMutationFinalize: false},
			},
			ServicePlacement: ServicePlacement{ActiveHosts: append([]string(nil), ExactActiveServiceHosts...), StandbyCandidates: append([]string(nil), ExactStandbyServiceHosts...)},
			Rollout:          Rollout{Order: []string{"zone-01", "zone-02", "zone-03", "zone-04", "zone-05", "zone-06", "zone-07", "zone-08"}, MaxParallelPerZone: DefaultMaxParallelPerZone, PauseOnFailure: true},
			SecretRefs:       []SecretReference{{Name: "tikv-client-key", File: "/etc/namrbd/tls/tikv-client.key"}},
		},
	}
	for i := 1; i <= ExactNodeCount; i++ {
		zone := (i-1)/ExactNodesPerZone + 1
		roles := []string{"sbs-data"}
		if containsString(ExactActiveServiceHosts, fmt.Sprintf("node%d", i)) {
			roles = append(roles, "sbs-service-active")
		}
		if containsString(ExactStandbyServiceHosts, fmt.Sprintf("node%d", i)) {
			roles = append(roles, "sbs-service-standby")
		}
		manifest.Spec.Nodes = append(manifest.Spec.Nodes, Node{
			ID: fmt.Sprintf("node%d", i), Hostname: fmt.Sprintf("node%d", i),
			ManagementAddress: fmt.Sprintf("10.10.%d.%d", zone, i-(zone-1)*ExactNodesPerZone),
			DataAddress:       fmt.Sprintf("10.20.%d.%d", zone, i-(zone-1)*ExactNodesPerZone),
			DataEndpoint:      fmt.Sprintf("10.20.%d.%d:9091", zone, i-(zone-1)*ExactNodesPerZone),
			AdminEndpoint:     fmt.Sprintf("10.10.%d.%d:9093", zone, i-(zone-1)*ExactNodesPerZone),
			MetricsEndpoint:   fmt.Sprintf("10.10.%d.%d:9103", zone, i-(zone-1)*ExactNodesPerZone),
			Location:          Location{Zone: zoneID(zone), Rack: fmt.Sprintf("rack-%02d", (i-1)%20+1)},
			Roles:             roles,
			StorageClaims: []StorageClaim{{
				ID: "payload-primary", DeviceByID: fmt.Sprintf("/dev/disk/by-id/namrbd-node%d-payload-primary", i),
				FilesystemUUID: fmt.Sprintf("00000000-0000-4000-8000-%012x", i), MountPath: "/srv/namrbd/payload-primary",
				MinimumFreeBytes: 107374182400, ReservedBytes: 10737418240, DestructiveProvisioning: false,
				Shards: 8, Weight: 100,
			}},
		})
		if containsString(ExactActiveServiceHosts, fmt.Sprintf("node%d", i)) || containsString(ExactStandbyServiceHosts, fmt.Sprintf("node%d", i)) {
			node := &manifest.Spec.Nodes[len(manifest.Spec.Nodes)-1]
			node.ServiceGRPCEndpoint = fmt.Sprintf("10.10.%d.%d:9090", zone, localNodeNumber(i, zone))
			node.ServiceHTTPEndpoint = fmt.Sprintf("10.10.%d.%d:9092", zone, localNodeNumber(i, zone))
			node.ServiceMetricsEndpoint = fmt.Sprintf("10.10.%d.%d:9102", zone, localNodeNumber(i, zone))
		}
	}
	return manifest
}

func localNodeNumber(node, zone int) int { return node - (zone-1)*ExactNodesPerZone }

func containsString(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}
