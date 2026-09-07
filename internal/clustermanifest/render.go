package clustermanifest

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/nosway/namrbd/internal/serviceconfig"
	"gopkg.in/yaml.v3"
)

type RenderedFile struct {
	RelativePath string `json:"relative_path"`
	InstallPath  string `json:"install_path"`
	Mode         uint32 `json:"mode"`
	Digest       string `json:"digest"`
	Content      []byte `json:"-"`
}

type NodeBundle struct {
	NodeID         string         `json:"node_id"`
	Zone           string         `json:"zone"`
	Roles          []string       `json:"roles"`
	ManifestDigest string         `json:"manifest_digest"`
	BundleDigest   string         `json:"bundle_digest"`
	Files          []RenderedFile `json:"files"`
}

type RenderSet struct {
	ManifestDigest string       `json:"manifest_digest"`
	RenderDigest   string       `json:"render_digest"`
	Bundles        []NodeBundle `json:"bundles"`
}

type renderedStorageClaims struct {
	APIVersion string                   `yaml:"apiVersion"`
	Kind       string                   `yaml:"kind"`
	Metadata   renderedStorageMetadata  `yaml:"metadata"`
	Spec       renderedStorageClaimSpec `yaml:"spec"`
}

type renderedStorageMetadata struct {
	ManifestDigest string `yaml:"manifestDigest"`
	Revision       string `yaml:"revision"`
}

type renderedStorageClaimSpec struct {
	NodeID string         `yaml:"nodeId"`
	Claims []StorageClaim `yaml:"claims"`
}

type renderedStoreConfig struct {
	Stores []renderedStore `yaml:"stores"`
}

type renderedStore struct {
	ID     string `yaml:"id"`
	Path   string `yaml:"path"`
	Shards int    `yaml:"shards"`
	Weight int    `yaml:"weight"`
}

type desiredNodeRecord struct {
	APIVersion     string   `json:"api_version"`
	ManifestDigest string   `json:"manifest_digest"`
	ArtifactDigest string   `json:"artifact_digest"`
	Node           Node     `json:"node"`
	SecretRefs     []string `json:"secret_refs"`
}

// Render produces every node bundle in memory. It validates the manifest
// first, resolves secret references only to their reference form, and does not
// write files or contact any runtime authority.
func Render(manifest *Manifest, policy ValidationPolicy) (*RenderSet, error) {
	canonical, err := Canonicalize(manifest)
	if err != nil {
		return nil, err
	}
	if err := Validate(canonical, policy); err != nil {
		return nil, err
	}
	manifestDigest, err := Digest(canonical)
	if err != nil {
		return nil, err
	}
	refs := make(map[string]SecretReference, len(canonical.Spec.SecretRefs))
	for _, ref := range canonical.Spec.SecretRefs {
		refs[ref.Name] = ref
	}

	set := &RenderSet{ManifestDigest: manifestDigest}
	for _, node := range canonical.Spec.Nodes {
		bundle, err := renderNodeBundle(canonical, node, refs, manifestDigest)
		if err != nil {
			return nil, fmt.Errorf("render node %s: %w", node.ID, err)
		}
		set.Bundles = append(set.Bundles, bundle)
	}
	set.RenderDigest = digestRenderSet(set.Bundles)
	return set, nil
}

func RenderSetsByteIdentical(a, b *RenderSet) bool {
	if a == nil || b == nil || a.ManifestDigest != b.ManifestDigest || a.RenderDigest != b.RenderDigest || len(a.Bundles) != len(b.Bundles) {
		return false
	}
	for i := range a.Bundles {
		left, right := a.Bundles[i], b.Bundles[i]
		if left.NodeID != right.NodeID || left.Zone != right.Zone || left.BundleDigest != right.BundleDigest || !sameStrings(left.Roles, right.Roles) || len(left.Files) != len(right.Files) {
			return false
		}
		for j := range left.Files {
			lf, rf := left.Files[j], right.Files[j]
			if lf.RelativePath != rf.RelativePath || lf.InstallPath != rf.InstallPath || lf.Mode != rf.Mode || lf.Digest != rf.Digest || !bytes.Equal(lf.Content, rf.Content) {
				return false
			}
		}
	}
	return true
}

func renderNodeBundle(manifest *Manifest, node Node, refs map[string]SecretReference, manifestDigest string) (NodeBundle, error) {
	bundle := NodeBundle{
		NodeID: node.ID, Zone: node.Location.Zone, Roles: append([]string(nil), node.Roles...), ManifestDigest: manifestDigest,
	}
	configDir := manifest.Spec.Bundle.ConfigDirectory
	binaryDir := manifest.Spec.Bundle.BinaryDirectory
	dataConfig := &serviceconfig.File{
		SchemaVersion: serviceconfig.SchemaVersion,
		Revision:      manifest.Spec.Bundle.ConfigRevision,
		Profile:       serviceconfig.ProfileLargeScale,
		Process:       serviceconfig.ProcessSBSData,
		SBSData: &serviceconfig.SBSDataConfig{
			ClusterID: manifest.Spec.ClusterID, SBSClusterID: manifest.Spec.SBSClusterID, NodeID: node.ID,
			DataPath: manifest.Spec.Bundle.DataStatePath, StoreConfigPath: filepath.Join(configDir, "store-config.yaml"),
			GRPCListen: node.DataEndpoint, HTTPListen: node.AdminEndpoint,
			Observability: serviceconfig.ObservabilityConfig{Listen: node.MetricsEndpoint, Trace: false, DebugEndpoints: false},
		},
	}
	if result := serviceconfig.Validate(dataConfig); !result.OK() {
		return NodeBundle{}, fmt.Errorf("rendered sbs-data config is invalid: %s", strings.Join(result.Errors, "; "))
	}
	dataYAML, err := yaml.Marshal(dataConfig)
	if err != nil {
		return NodeBundle{}, fmt.Errorf("marshal sbs-data config: %w", err)
	}
	bundle.Files = append(bundle.Files, renderedFile(node.ID, "etc/namrbd/sbs-data.yaml", filepath.Join(configDir, "sbs-data.yaml"), DefaultRenderedConfigMode, dataYAML))

	stores := renderedStoreConfig{}
	for _, claim := range node.StorageClaims {
		stores.Stores = append(stores.Stores, renderedStore{ID: claim.ID, Path: claim.MountPath, Shards: claim.Shards, Weight: claim.Weight})
	}
	storeYAML, err := yaml.Marshal(stores)
	if err != nil {
		return NodeBundle{}, fmt.Errorf("marshal store config: %w", err)
	}
	bundle.Files = append(bundle.Files, renderedFile(node.ID, "etc/namrbd/store-config.yaml", filepath.Join(configDir, "store-config.yaml"), DefaultRenderedConfigMode, storeYAML))

	claimDocument := renderedStorageClaims{
		APIVersion: APIVersion, Kind: "SBSHostStorageClaims",
		Metadata: renderedStorageMetadata{ManifestDigest: manifestDigest, Revision: manifest.Metadata.Revision},
		Spec:     renderedStorageClaimSpec{NodeID: node.ID, Claims: append([]StorageClaim(nil), node.StorageClaims...)},
	}
	claimYAML, err := yaml.Marshal(claimDocument)
	if err != nil {
		return NodeBundle{}, fmt.Errorf("marshal storage claims: %w", err)
	}
	bundle.Files = append(bundle.Files, renderedFile(node.ID, "etc/namrbd/storage-claims.yaml", filepath.Join(configDir, "storage-claims.yaml"), DefaultRenderedConfigMode, claimYAML))

	dataUnit := renderSystemdUnit("NAMRBD SBS data service", manifest.Spec.Bundle.ServiceUser,
		filepath.Join(binaryDir, "sbs-data")+" --config "+filepath.Join(configDir, "sbs-data.yaml"))
	bundle.Files = append(bundle.Files, renderedFile(node.ID, "etc/systemd/system/namrbd-sbs-data.service", "/etc/systemd/system/namrbd-sbs-data.service", DefaultRenderedUnitMode, dataUnit))

	secretNames := make([]string, 0, len(manifest.Spec.SecretRefs))
	for _, ref := range manifest.Spec.SecretRefs {
		secretNames = append(secretNames, ref.Name)
	}
	desiredRecord := desiredNodeRecord{APIVersion: APIVersion, ManifestDigest: manifestDigest, ArtifactDigest: manifest.Spec.Artifact.Digest, Node: node, SecretRefs: secretNames}
	desiredJSON, err := json.MarshalIndent(desiredRecord, "", "  ")
	if err != nil {
		return NodeBundle{}, fmt.Errorf("marshal desired node record: %w", err)
	}
	desiredJSON = append(desiredJSON, '\n')
	bundle.Files = append(bundle.Files, renderedFile(node.ID, "desired-node.json", filepath.Join(configDir, "desired-node.json"), DefaultRenderedConfigMode, desiredJSON))

	if containsStringValue(node.Roles, "sbs-service-active") || containsStringValue(node.Roles, "sbs-service-standby") {
		keyRef := refs[manifest.Spec.MetadataAuthority.TLS.KeyRef]
		serviceFile := &serviceconfig.File{
			SchemaVersion: serviceconfig.SchemaVersion,
			Revision:      manifest.Spec.Bundle.ConfigRevision,
			Profile:       serviceconfig.ProfileLargeScale,
			Process:       serviceconfig.ProcessSBSService,
			SBSService: &serviceconfig.SBSServiceConfig{
				ClusterID: manifest.Spec.ClusterID, SBSClusterID: manifest.Spec.SBSClusterID, NodeID: node.ID,
				MetadataBackend: "tikv", GRPCListen: node.ServiceGRPCEndpoint, HTTPListen: node.ServiceHTTPEndpoint,
				PayloadRoot: manifest.Spec.ServiceConfig.PayloadRoot,
				TiKV: serviceconfig.TiKVConfig{
					PDEndpoints: append([]string(nil), manifest.Spec.MetadataAuthority.PDEndpoints...),
					Keyspace:    manifest.Spec.MetadataAuthority.Keyspace, APIVersion: manifest.Spec.MetadataAuthority.APIVersion,
					TimeoutSeconds: manifest.Spec.ServiceConfig.TiKV.TimeoutSeconds,
					TLS:            &serviceconfig.TLSConfig{Enable: true, CertFile: manifest.Spec.MetadataAuthority.TLS.CertFile, Key: serviceconfig.SecretRef{File: keyRef.File, Env: keyRef.Env, KMS: keyRef.KMS}},
					ScanPageSize:   manifest.Spec.ServiceConfig.TiKV.ScanPageSize, BatchGetSize: manifest.Spec.ServiceConfig.TiKV.BatchGetSize, OperationTrace: false,
				},
				Leader: serviceconfig.SBSLeaderConfig{
					LeaseDurationSeconds: manifest.Spec.ServiceConfig.Leader.LeaseDurationSeconds,
					RenewIntervalSeconds: manifest.Spec.ServiceConfig.Leader.RenewIntervalSeconds,
				},
				Health: serviceconfig.SBSHealthConfig{
					ShardCount: manifest.Spec.ServiceConfig.Health.ShardCount, ConcurrencyPerShard: manifest.Spec.ServiceConfig.Health.ConcurrencyPerShard,
					IntervalSeconds: manifest.Spec.ServiceConfig.Health.IntervalSeconds, TimeoutSeconds: manifest.Spec.ServiceConfig.Health.TimeoutSeconds,
					SuspectThreshold: manifest.Spec.ServiceConfig.Health.SuspectThreshold, DownThreshold: manifest.Spec.ServiceConfig.Health.DownThreshold,
					RecoveryCooldownSeconds: manifest.Spec.ServiceConfig.Health.RecoveryCooldownSeconds,
				},
				WriteEffects: serviceconfig.SBSWriteEffects{
					ServiceOwned:             manifest.Spec.ServiceConfig.WriteEffects.ServiceOwned,
					NativeAllocationFastPath: manifest.Spec.ServiceConfig.WriteEffects.NativeAllocationFastPath,
					BatchMax:                 manifest.Spec.ServiceConfig.WriteEffects.BatchMax, LaneBucketCount: manifest.Spec.ServiceConfig.WriteEffects.LaneBucketCount,
					AsyncMutationFinalize: manifest.Spec.ServiceConfig.WriteEffects.AsyncMutationFinalize,
				},
				Observability: serviceconfig.ObservabilityConfig{Listen: node.ServiceMetricsEndpoint, Trace: false, DebugEndpoints: false},
			},
		}
		if result := serviceconfig.Validate(serviceFile); !result.OK() {
			return NodeBundle{}, fmt.Errorf("rendered sbs-service config is invalid: %s", strings.Join(result.Errors, "; "))
		}
		serviceYAML, err := yaml.Marshal(serviceFile)
		if err != nil {
			return NodeBundle{}, fmt.Errorf("marshal sbs-service config: %w", err)
		}
		bundle.Files = append(bundle.Files, renderedFile(node.ID, "etc/namrbd/sbs-service.yaml", filepath.Join(configDir, "sbs-service.yaml"), DefaultRenderedSecretMode, serviceYAML))
		serviceUnit := renderSystemdUnit("NAMRBD SBS metadata service", manifest.Spec.Bundle.ServiceUser,
			filepath.Join(binaryDir, "sbs-service")+" --config "+filepath.Join(configDir, "sbs-service.yaml"))
		bundle.Files = append(bundle.Files, renderedFile(node.ID, "etc/systemd/system/namrbd-sbs-service.service", "/etc/systemd/system/namrbd-sbs-service.service", DefaultRenderedUnitMode, serviceUnit))
	}

	sort.Slice(bundle.Files, func(i, j int) bool { return bundle.Files[i].RelativePath < bundle.Files[j].RelativePath })
	for _, file := range bundle.Files {
		if hits := serviceconfig.ScanForSecretLiterals(string(file.Content)); len(hits) > 0 {
			return NodeBundle{}, fmt.Errorf("%s contains secret material: %s", file.RelativePath, strings.Join(hits, "; "))
		}
	}
	bundle.BundleDigest = digestRenderedFiles(bundle.Files)
	return bundle, nil
}

func renderedFile(nodeID, suffix, installPath string, mode uint32, content []byte) RenderedFile {
	relative := filepath.ToSlash(filepath.Join("nodes", nodeID, suffix))
	sum := sha256.Sum256(content)
	return RenderedFile{RelativePath: relative, InstallPath: filepath.ToSlash(installPath), Mode: mode, Digest: "sha256:" + hex.EncodeToString(sum[:]), Content: append([]byte(nil), content...)}
}

func renderSystemdUnit(description, user, command string) []byte {
	return []byte(fmt.Sprintf(`[Unit]
Description=%s
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=%s
ExecStart=%s
Restart=on-failure
RestartSec=5s
NoNewPrivileges=true

[Install]
WantedBy=multi-user.target
`, description, user, command))
}

func digestRenderedFiles(files []RenderedFile) string {
	h := sha256.New()
	for _, file := range files {
		_, _ = h.Write([]byte(file.RelativePath))
		_, _ = h.Write([]byte{0})
		_, _ = h.Write([]byte(fmt.Sprintf("%04o", file.Mode)))
		_, _ = h.Write([]byte{0})
		_, _ = h.Write(file.Content)
		_, _ = h.Write([]byte{0})
	}
	return "sha256:" + hex.EncodeToString(h.Sum(nil))
}

func digestRenderSet(bundles []NodeBundle) string {
	h := sha256.New()
	for _, bundle := range bundles {
		_, _ = h.Write([]byte(bundle.NodeID))
		_, _ = h.Write([]byte{0})
		_, _ = h.Write([]byte(bundle.BundleDigest))
		_, _ = h.Write([]byte{0})
	}
	return "sha256:" + hex.EncodeToString(h.Sum(nil))
}

// WriteRenderSet materializes a render set below an empty output directory.
// It refuses a non-empty directory rather than overwriting operator files.
func WriteRenderSet(outputDir string, set *RenderSet) error {
	outputDir = strings.TrimSpace(outputDir)
	if outputDir == "" || filepath.Clean(outputDir) == "/" {
		return fmt.Errorf("render output directory must be a non-root path")
	}
	if set == nil {
		return fmt.Errorf("render set is nil")
	}
	if entries, err := os.ReadDir(outputDir); err == nil && len(entries) > 0 {
		return fmt.Errorf("render output directory %s is not empty", outputDir)
	} else if err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("inspect render output directory %s: %w", outputDir, err)
	}
	for _, bundle := range set.Bundles {
		for _, file := range bundle.Files {
			target := filepath.Join(outputDir, filepath.FromSlash(file.RelativePath))
			if err := os.MkdirAll(filepath.Dir(target), DefaultRenderedDirectoryMode); err != nil {
				return fmt.Errorf("create render directory for %s: %w", file.RelativePath, err)
			}
			if err := WriteNewFile(target, file.Content, os.FileMode(file.Mode)); err != nil {
				return fmt.Errorf("write rendered file %s: %w", file.RelativePath, err)
			}
		}
	}
	return nil
}

// WriteNewFile writes a local artifact without overwriting an existing path.
func WriteNewFile(path string, content []byte, mode os.FileMode) error {
	path = strings.TrimSpace(path)
	if path == "" {
		return fmt.Errorf("output path is required")
	}
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, mode)
	if err != nil {
		return fmt.Errorf("create output %s without overwrite: %w", path, err)
	}
	if _, err := file.Write(content); err != nil {
		_ = file.Close()
		_ = os.Remove(path)
		return fmt.Errorf("write output %s: %w", path, err)
	}
	if err := file.Close(); err != nil {
		_ = os.Remove(path)
		return fmt.Errorf("close output %s: %w", path, err)
	}
	return nil
}

func containsStringValue(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}
