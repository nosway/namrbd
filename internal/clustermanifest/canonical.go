package clustermanifest

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net"
	"net/netip"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"gopkg.in/yaml.v3"
)

// Canonicalize returns a normalized deep copy. Ordered rollout waves retain
// their declared order; sets and node collections are sorted deterministically.
func Canonicalize(manifest *Manifest) (*Manifest, error) {
	if manifest == nil {
		return nil, fmt.Errorf("canonicalize cluster manifest: manifest is nil")
	}
	raw, err := json.Marshal(manifest)
	if err != nil {
		return nil, fmt.Errorf("copy cluster manifest: %w", err)
	}
	var out Manifest
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, fmt.Errorf("copy cluster manifest: %w", err)
	}

	out.APIVersion = strings.TrimSpace(out.APIVersion)
	out.Kind = strings.TrimSpace(out.Kind)
	out.Metadata.Name = strings.TrimSpace(out.Metadata.Name)
	out.Metadata.Revision = strings.TrimSpace(out.Metadata.Revision)
	out.Spec.ClusterID = strings.TrimSpace(out.Spec.ClusterID)
	out.Spec.SBSClusterID = strings.TrimSpace(out.Spec.SBSClusterID)
	out.Spec.Artifact.Version = strings.TrimSpace(out.Spec.Artifact.Version)
	out.Spec.Artifact.Digest = strings.ToLower(strings.TrimSpace(out.Spec.Artifact.Digest))
	out.Spec.Artifact.ApprovalRef = strings.TrimSpace(out.Spec.Artifact.ApprovalRef)
	canonicalBinaryDigests := make(map[string]string, len(out.Spec.Artifact.BinaryDigests))
	for name, digest := range out.Spec.Artifact.BinaryDigests {
		canonicalBinaryDigests[strings.TrimSpace(name)] = strings.ToLower(strings.TrimSpace(digest))
	}
	out.Spec.Artifact.BinaryDigests = canonicalBinaryDigests
	out.Spec.MetadataAuthority.Keyspace = strings.TrimSpace(out.Spec.MetadataAuthority.Keyspace)
	out.Spec.MetadataAuthority.APIVersion = strings.TrimSpace(out.Spec.MetadataAuthority.APIVersion)
	out.Spec.MetadataAuthority.TLS.CertFile = filepath.Clean(strings.TrimSpace(out.Spec.MetadataAuthority.TLS.CertFile))
	out.Spec.MetadataAuthority.TLS.KeyRef = strings.TrimSpace(out.Spec.MetadataAuthority.TLS.KeyRef)
	out.Spec.Bundle.BinaryDirectory = filepath.Clean(strings.TrimSpace(out.Spec.Bundle.BinaryDirectory))
	out.Spec.Bundle.ConfigDirectory = filepath.Clean(strings.TrimSpace(out.Spec.Bundle.ConfigDirectory))
	out.Spec.Bundle.DataStatePath = filepath.Clean(strings.TrimSpace(out.Spec.Bundle.DataStatePath))
	out.Spec.Bundle.ServiceUser = strings.TrimSpace(out.Spec.Bundle.ServiceUser)
	out.Spec.ServiceConfig.PayloadRoot = filepath.Clean(strings.TrimSpace(out.Spec.ServiceConfig.PayloadRoot))
	for i := range out.Spec.MetadataAuthority.PDEndpoints {
		out.Spec.MetadataAuthority.PDEndpoints[i] = strings.TrimSpace(out.Spec.MetadataAuthority.PDEndpoints[i])
	}
	sort.Strings(out.Spec.MetadataAuthority.PDEndpoints)
	trimAndSortNodeIDs(out.Spec.ServicePlacement.ActiveHosts)
	trimAndSortNodeIDs(out.Spec.ServicePlacement.StandbyCandidates)
	for i := range out.Spec.Rollout.Order {
		out.Spec.Rollout.Order[i] = strings.TrimSpace(out.Spec.Rollout.Order[i])
	}
	for i := range out.Spec.SecretRefs {
		ref := &out.Spec.SecretRefs[i]
		ref.Name = strings.TrimSpace(ref.Name)
		ref.File = cleanOptionalPath(ref.File)
		ref.Env = strings.TrimSpace(ref.Env)
		ref.KMS = strings.TrimSpace(ref.KMS)
	}
	sort.Slice(out.Spec.SecretRefs, func(i, j int) bool { return out.Spec.SecretRefs[i].Name < out.Spec.SecretRefs[j].Name })

	for i := range out.Spec.Nodes {
		node := &out.Spec.Nodes[i]
		node.ID = strings.TrimSpace(node.ID)
		node.Hostname = strings.TrimSpace(node.Hostname)
		node.ManagementAddress = canonicalAddress(node.ManagementAddress)
		node.DataAddress = canonicalAddress(node.DataAddress)
		node.DataEndpoint = canonicalEndpoint(node.DataEndpoint)
		node.AdminEndpoint = canonicalEndpoint(node.AdminEndpoint)
		node.MetricsEndpoint = canonicalEndpoint(node.MetricsEndpoint)
		node.ServiceGRPCEndpoint = canonicalEndpoint(node.ServiceGRPCEndpoint)
		node.ServiceHTTPEndpoint = canonicalEndpoint(node.ServiceHTTPEndpoint)
		node.ServiceMetricsEndpoint = canonicalEndpoint(node.ServiceMetricsEndpoint)
		node.Location.Zone = strings.TrimSpace(node.Location.Zone)
		node.Location.Rack = strings.TrimSpace(node.Location.Rack)
		for j := range node.Roles {
			node.Roles[j] = strings.TrimSpace(node.Roles[j])
		}
		sort.Strings(node.Roles)
		for j := range node.StorageClaims {
			claim := &node.StorageClaims[j]
			claim.ID = strings.TrimSpace(claim.ID)
			claim.DeviceByID = filepath.Clean(strings.TrimSpace(claim.DeviceByID))
			claim.FilesystemUUID = strings.ToLower(strings.TrimSpace(claim.FilesystemUUID))
			claim.MountPath = filepath.Clean(strings.TrimSpace(claim.MountPath))
		}
		sort.Slice(node.StorageClaims, func(i, j int) bool { return node.StorageClaims[i].ID < node.StorageClaims[j].ID })
	}
	sort.Slice(out.Spec.Nodes, func(i, j int) bool { return nodeLess(out.Spec.Nodes[i].ID, out.Spec.Nodes[j].ID) })
	return &out, nil
}

func CanonicalJSON(manifest *Manifest) ([]byte, error) {
	canonical, err := Canonicalize(manifest)
	if err != nil {
		return nil, err
	}
	raw, err := json.Marshal(canonical)
	if err != nil {
		return nil, fmt.Errorf("marshal canonical cluster manifest JSON: %w", err)
	}
	return append(raw, '\n'), nil
}

func CanonicalYAML(manifest *Manifest) ([]byte, error) {
	canonical, err := Canonicalize(manifest)
	if err != nil {
		return nil, err
	}
	raw, err := yaml.Marshal(canonical)
	if err != nil {
		return nil, fmt.Errorf("marshal canonical cluster manifest YAML: %w", err)
	}
	return raw, nil
}

func Digest(manifest *Manifest) (string, error) {
	raw, err := CanonicalJSON(manifest)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(raw)
	return "sha256:" + hex.EncodeToString(sum[:]), nil
}

func cleanOptionalPath(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return ""
	}
	return filepath.Clean(value)
}

func canonicalAddress(value string) string {
	value = strings.TrimSpace(value)
	addr, err := netip.ParseAddr(value)
	if err != nil {
		return value
	}
	return addr.String()
}

func canonicalEndpoint(value string) string {
	value = strings.TrimSpace(value)
	host, port, err := net.SplitHostPort(value)
	if err != nil {
		return value
	}
	return net.JoinHostPort(canonicalAddress(host), port)
}

func trimAndSortNodeIDs(values []string) {
	for i := range values {
		values[i] = strings.TrimSpace(values[i])
	}
	sort.Slice(values, func(i, j int) bool { return nodeLess(values[i], values[j]) })
}

func nodeLess(a, b string) bool {
	parse := func(value string) (int, bool) {
		match := nodeIDPattern.FindStringSubmatch(value)
		if match == nil {
			return 0, false
		}
		n, err := strconv.Atoi(match[1])
		return n, err == nil
	}
	an, aok := parse(a)
	bn, bok := parse(b)
	if aok && bok {
		return an < bn
	}
	if aok != bok {
		return aok
	}
	return a < b
}
