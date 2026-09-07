package installpreflight

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestSignedReportAndTrustBundleParsingAreStrict(t *testing.T) {
	report := exactRequest(t)
	report.Facts.EvidenceSource = "local-os"
	checked, err := CheckHost(report)
	if err != nil {
		t.Fatal(err)
	}
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	signed, err := SignReport(checked, "host-node1", privateKey)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := MarshalSignedReport(signed)
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := ParseSignedReport(raw)
	if err != nil {
		t.Fatal(err)
	}
	if parsed.KeyID != "host-node1" || parsed.Report.ReportDigest != checked.ReportDigest {
		t.Fatalf("parsed=%+v", parsed)
	}
	withUnknown := strings.Replace(string(raw), "{", "{\"unknown\":true,", 1)
	if _, err := ParseSignedReport([]byte(withUnknown)); err == nil || !strings.Contains(err.Error(), "unknown field") {
		t.Fatalf("unknown signed report field error=%v", err)
	}

	bundle := &TrustBundle{APIVersion: APIVersion, Kind: TrustBundleKind, Signers: []TrustedSigner{{
		KeyID: "host-node1", NodeID: "node1", PublicKey: base64.StdEncoding.EncodeToString(publicKey),
	}}}
	bundleRaw, err := MarshalTrustBundle(bundle)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ParseTrustBundle(bundleRaw); err != nil {
		t.Fatal(err)
	}
	bundle.Signers = append(bundle.Signers, bundle.Signers[0])
	if err := ValidateTrustBundle(bundle); err == nil || !strings.Contains(err.Error(), "duplicates key_id") {
		t.Fatalf("duplicate trust signer error=%v", err)
	}
}

func TestLoadEd25519PrivateKeyRequiresPKCS8AndOwnerOnlyMode(t *testing.T) {
	_, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	der, err := x509.MarshalPKCS8PrivateKey(privateKey)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "host-key.pem")
	if err := os.WriteFile(path, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der}), 0o600); err != nil {
		t.Fatal(err)
	}
	loaded, err := LoadEd25519PrivateKey(path)
	if err != nil {
		t.Fatal(err)
	}
	if !loaded.Equal(privateKey) {
		t.Fatal("loaded private key differs")
	}
	if err := os.Chmod(path, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadEd25519PrivateKey(path); err == nil || !strings.Contains(err.Error(), "permissions") {
		t.Fatalf("insecure mode error=%v", err)
	}
}

func TestSignReportRejectsDigestDrift(t *testing.T) {
	request := exactRequest(t)
	report, err := CheckHost(request)
	if err != nil {
		t.Fatal(err)
	}
	_, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	report.NodeID = "node2"
	if _, err := SignReport(report, "host-node1", privateKey); err == nil || !strings.Contains(err.Error(), "digest") {
		t.Fatalf("digest drift error=%v", err)
	}
	raw, err := json.Marshal(report)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(raw), string(privateKey)) {
		t.Fatal("private key leaked into report JSON")
	}
}
