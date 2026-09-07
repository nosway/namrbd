// Package clustermanifest defines the Phase AD desired-state contract for an
// SBS fleet. Parsing, canonicalization, validation, rendering, and planning in
// this package are deliberately local operations: this package does not import
// a metadata repository, an admin client, or a process supervisor.
package clustermanifest

import (
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/nosway/namrbd/internal/serviceconfig"
	"gopkg.in/yaml.v3"
)

const (
	APIVersion = "namrbd.io/v1alpha1"
	Kind       = "SBSCluster"

	ExactNodeCount               = 160
	ExactZoneCount               = 8
	ExactNodesPerZone            = 20
	ExactActiveServiceCount      = 3
	ExactStandbyServiceCount     = 2
	DefaultMaxParallelPerZone    = 5
	DefaultManifestFileMode      = 0o644
	DefaultRenderedSecretMode    = 0o600
	DefaultRenderedConfigMode    = 0o600
	DefaultRenderedUnitMode      = 0o644
	DefaultRenderedDirectoryMode = 0o750
)

var (
	ExactActiveServiceHosts  = []string{"node1", "node21", "node41"}
	ExactStandbyServiceHosts = []string{"node61", "node81"}
)

// Manifest is the reviewed desired state. It intentionally contains no
// observed membership, capacity, daemon state, or credential material.
type Manifest struct {
	APIVersion string   `yaml:"apiVersion" json:"apiVersion"`
	Kind       string   `yaml:"kind" json:"kind"`
	Metadata   Metadata `yaml:"metadata" json:"metadata"`
	Spec       Spec     `yaml:"spec" json:"spec"`
}

type Metadata struct {
	Name     string `yaml:"name" json:"name"`
	Revision string `yaml:"revision" json:"revision"`
}

type Spec struct {
	ClusterID         string            `yaml:"clusterId" json:"clusterId"`
	SBSClusterID      string            `yaml:"sbsClusterId" json:"sbsClusterId"`
	ExpectedNodeCount int               `yaml:"expectedNodeCount" json:"expectedNodeCount"`
	ExpectedZones     int               `yaml:"expectedZones" json:"expectedZones"`
	Artifact          Artifact          `yaml:"artifact" json:"artifact"`
	MetadataAuthority MetadataAuthority `yaml:"metadataAuthority" json:"metadataAuthority"`
	Bundle            BundleSettings    `yaml:"bundle" json:"bundle"`
	ServiceConfig     ServiceSettings   `yaml:"serviceConfig" json:"serviceConfig"`
	ServicePlacement  ServicePlacement  `yaml:"servicePlacement" json:"servicePlacement"`
	Rollout           Rollout           `yaml:"rollout" json:"rollout"`
	SecretRefs        []SecretReference `yaml:"secretRefs,omitempty" json:"secretRefs,omitempty"`
	Nodes             []Node            `yaml:"nodes" json:"nodes"`
}

type BundleSettings struct {
	ConfigRevision  int    `yaml:"configRevision" json:"configRevision"`
	BinaryDirectory string `yaml:"binaryDirectory" json:"binaryDirectory"`
	ConfigDirectory string `yaml:"configDirectory" json:"configDirectory"`
	DataStatePath   string `yaml:"dataStatePath" json:"dataStatePath"`
	ServiceUser     string `yaml:"serviceUser" json:"serviceUser"`
}

type ServiceSettings struct {
	PayloadRoot  string              `yaml:"payloadRoot" json:"payloadRoot"`
	TiKV         TiKVSettings        `yaml:"tikv" json:"tikv"`
	Leader       LeaderSettings      `yaml:"leader" json:"leader"`
	Health       HealthSettings      `yaml:"health" json:"health"`
	WriteEffects WriteEffectSettings `yaml:"writeEffects" json:"writeEffects"`
}

type TiKVSettings struct {
	TimeoutSeconds int `yaml:"timeoutSeconds" json:"timeoutSeconds"`
	ScanPageSize   int `yaml:"scanPageSize" json:"scanPageSize"`
	BatchGetSize   int `yaml:"batchGetSize" json:"batchGetSize"`
}

type LeaderSettings struct {
	LeaseDurationSeconds int `yaml:"leaseDurationSeconds" json:"leaseDurationSeconds"`
	RenewIntervalSeconds int `yaml:"renewIntervalSeconds" json:"renewIntervalSeconds"`
}

type HealthSettings struct {
	ShardCount              int `yaml:"shardCount" json:"shardCount"`
	ConcurrencyPerShard     int `yaml:"concurrencyPerShard" json:"concurrencyPerShard"`
	IntervalSeconds         int `yaml:"intervalSeconds" json:"intervalSeconds"`
	TimeoutSeconds          int `yaml:"timeoutSeconds" json:"timeoutSeconds"`
	SuspectThreshold        int `yaml:"suspectThreshold" json:"suspectThreshold"`
	DownThreshold           int `yaml:"downThreshold" json:"downThreshold"`
	RecoveryCooldownSeconds int `yaml:"recoveryCooldownSeconds" json:"recoveryCooldownSeconds"`
}

type WriteEffectSettings struct {
	ServiceOwned             bool `yaml:"serviceOwned" json:"serviceOwned"`
	NativeAllocationFastPath bool `yaml:"nativeAllocationFastPath" json:"nativeAllocationFastPath"`
	BatchMax                 int  `yaml:"batchMax" json:"batchMax"`
	LaneBucketCount          int  `yaml:"laneBucketCount" json:"laneBucketCount"`
	AsyncMutationFinalize    bool `yaml:"asyncMutationFinalize" json:"asyncMutationFinalize"`
}

type Artifact struct {
	Version       string            `yaml:"version" json:"version"`
	Digest        string            `yaml:"digest" json:"digest"`
	ApprovalRef   string            `yaml:"approvalRef" json:"approvalRef"`
	BinaryDigests map[string]string `yaml:"binaryDigests" json:"binaryDigests"`
}

type MetadataAuthority struct {
	PDEndpoints []string             `yaml:"pdEndpoints" json:"pdEndpoints"`
	Keyspace    string               `yaml:"keyspace" json:"keyspace"`
	APIVersion  string               `yaml:"apiVersion" json:"apiVersion"`
	TLS         MetadataAuthorityTLS `yaml:"tls" json:"tls"`
}

type MetadataAuthorityTLS struct {
	Enabled  bool   `yaml:"enabled" json:"enabled"`
	CertFile string `yaml:"certFile,omitempty" json:"certFile,omitempty"`
	KeyRef   string `yaml:"keyRef,omitempty" json:"keyRef,omitempty"`
}

type ServicePlacement struct {
	ActiveHosts       []string `yaml:"activeHosts" json:"activeHosts"`
	StandbyCandidates []string `yaml:"standbyCandidates" json:"standbyCandidates"`
}

type Rollout struct {
	Order              []string `yaml:"order" json:"order"`
	MaxParallelPerZone int      `yaml:"maxParallelPerZone" json:"maxParallelPerZone"`
	PauseOnFailure     bool     `yaml:"pauseOnFailure" json:"pauseOnFailure"`
}

// SecretReference names exactly one external credential source. It never
// resolves that source and therefore remains safe to canonicalize and export.
type SecretReference struct {
	Name string `yaml:"name" json:"name"`
	File string `yaml:"file,omitempty" json:"file,omitempty"`
	Env  string `yaml:"env,omitempty" json:"env,omitempty"`
	KMS  string `yaml:"kms,omitempty" json:"kms,omitempty"`
}

type Node struct {
	ID                     string         `yaml:"id" json:"id"`
	Hostname               string         `yaml:"hostname" json:"hostname"`
	ManagementAddress      string         `yaml:"managementAddress" json:"managementAddress"`
	DataAddress            string         `yaml:"dataAddress" json:"dataAddress"`
	DataEndpoint           string         `yaml:"dataEndpoint" json:"dataEndpoint"`
	AdminEndpoint          string         `yaml:"adminEndpoint" json:"adminEndpoint"`
	MetricsEndpoint        string         `yaml:"metricsEndpoint" json:"metricsEndpoint"`
	ServiceGRPCEndpoint    string         `yaml:"serviceGrpcEndpoint,omitempty" json:"serviceGrpcEndpoint,omitempty"`
	ServiceHTTPEndpoint    string         `yaml:"serviceHttpEndpoint,omitempty" json:"serviceHttpEndpoint,omitempty"`
	ServiceMetricsEndpoint string         `yaml:"serviceMetricsEndpoint,omitempty" json:"serviceMetricsEndpoint,omitempty"`
	Location               Location       `yaml:"location" json:"location"`
	Roles                  []string       `yaml:"roles" json:"roles"`
	StorageClaims          []StorageClaim `yaml:"storageClaims" json:"storageClaims"`
}

type Location struct {
	Zone string `yaml:"zone" json:"zone"`
	Rack string `yaml:"rack" json:"rack"`
}

type StorageClaim struct {
	ID                      string `yaml:"id" json:"id"`
	DeviceByID              string `yaml:"deviceById" json:"deviceById"`
	FilesystemUUID          string `yaml:"filesystemUUID" json:"filesystemUUID"`
	MountPath               string `yaml:"mountPath" json:"mountPath"`
	MinimumFreeBytes        int64  `yaml:"minimumFreeBytes" json:"minimumFreeBytes"`
	ReservedBytes           int64  `yaml:"reservedBytes" json:"reservedBytes"`
	Shards                  int    `yaml:"shards" json:"shards"`
	Weight                  int    `yaml:"weight" json:"weight"`
	DestructiveProvisioning bool   `yaml:"destructiveProvisioning" json:"destructiveProvisioning"`
}

// Parse rejects unknown fields, multiple YAML documents, and pasted secret
// material before returning a manifest. It performs no filesystem or network
// access beyond the caller-provided bytes.
func Parse(raw []byte) (*Manifest, error) {
	if hits := serviceconfig.ScanForSecretLiterals(string(raw)); len(hits) > 0 {
		return nil, &ValidationError{Issues: []Issue{{
			Code:    CodeSecretLiteral,
			Field:   "manifest",
			Message: fmt.Sprintf("secret material is not allowed; use a reference (%s)", strings.Join(hits, "; ")),
		}}}
	}

	dec := yaml.NewDecoder(strings.NewReader(string(raw)))
	dec.KnownFields(true)
	var manifest Manifest
	if err := dec.Decode(&manifest); err != nil {
		return nil, fmt.Errorf("decode cluster manifest: %w", err)
	}
	var extra any
	if err := dec.Decode(&extra); err != io.EOF {
		if err == nil {
			return nil, fmt.Errorf("decode cluster manifest: multiple YAML documents are not allowed")
		}
		return nil, fmt.Errorf("decode cluster manifest trailing document: %w", err)
	}
	return &manifest, nil
}

// Load reads and parses one manifest. File permission and signature policy are
// intentionally outside this pure schema operation.
func Load(path string) (*Manifest, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read cluster manifest %s: %w", path, err)
	}
	manifest, err := Parse(raw)
	if err != nil {
		return nil, fmt.Errorf("parse cluster manifest %s: %w", path, err)
	}
	return manifest, nil
}
