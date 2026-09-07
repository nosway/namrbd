package installpreflight

import (
	"bytes"
	"crypto/ed25519"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"io"
	"os"
	"strings"
)

const (
	SignedReportKind  = "SBSHostPreflightSignedReport"
	TrustBundleKind   = "SBSHostPreflightTrustBundle"
	SignatureEd25519  = "Ed25519"
	privateKeyMaxSize = 16 * 1024
)

// SignedReport binds an immutable node-local report to one configured host
// identity. The private key and raw host facts are deliberately not embedded.
type SignedReport struct {
	APIVersion string `json:"api_version"`
	Kind       string `json:"kind"`
	KeyID      string `json:"key_id"`
	Algorithm  string `json:"algorithm"`
	Report     Report `json:"report"`
	Signature  string `json:"signature"`
}

type TrustBundle struct {
	APIVersion string          `json:"api_version"`
	Kind       string          `json:"kind"`
	Signers    []TrustedSigner `json:"signers"`
}

type TrustedSigner struct {
	KeyID     string `json:"key_id"`
	NodeID    string `json:"node_id"`
	PublicKey string `json:"public_key"`
}

func SignReport(report *Report, keyID string, privateKey ed25519.PrivateKey) (*SignedReport, error) {
	if err := validateReportIntegrity(report); err != nil {
		return nil, err
	}
	if !validKeyID(keyID) {
		return nil, fmt.Errorf("signing key ID is empty or contains unsupported characters")
	}
	if len(privateKey) != ed25519.PrivateKeySize {
		return nil, fmt.Errorf("Ed25519 private key has %d bytes, want %d", len(privateKey), ed25519.PrivateKeySize)
	}
	payload, err := reportSignaturePayload(report)
	if err != nil {
		return nil, err
	}
	signed := &SignedReport{
		APIVersion: APIVersion,
		Kind:       SignedReportKind,
		KeyID:      keyID,
		Algorithm:  SignatureEd25519,
		Report:     *report,
		Signature:  base64.StdEncoding.EncodeToString(ed25519.Sign(privateKey, payload)),
	}
	return signed, nil
}

func ParseSignedReport(raw []byte) (*SignedReport, error) {
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	var signed SignedReport
	if err := dec.Decode(&signed); err != nil {
		return nil, fmt.Errorf("decode signed preflight report: %w", err)
	}
	var extra any
	if err := dec.Decode(&extra); err != io.EOF {
		if err == nil {
			return nil, fmt.Errorf("decode signed preflight report: multiple JSON documents are not allowed")
		}
		return nil, fmt.Errorf("decode signed preflight report trailing data: %w", err)
	}
	if err := validateSignedReportShape(&signed); err != nil {
		return nil, err
	}
	return &signed, nil
}

func MarshalSignedReport(signed *SignedReport) ([]byte, error) {
	if err := validateSignedReportShape(signed); err != nil {
		return nil, err
	}
	raw, err := json.MarshalIndent(signed, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("marshal signed preflight report: %w", err)
	}
	return append(raw, '\n'), nil
}

func SignedReportDigest(signed *SignedReport) (string, error) {
	if err := validateSignedReportShape(signed); err != nil {
		return "", err
	}
	raw, err := json.Marshal(signed)
	if err != nil {
		return "", fmt.Errorf("marshal signed preflight report digest input: %w", err)
	}
	sum := sha256.Sum256(raw)
	return "sha256:" + hex.EncodeToString(sum[:]), nil
}

func ParseTrustBundle(raw []byte) (*TrustBundle, error) {
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	var bundle TrustBundle
	if err := dec.Decode(&bundle); err != nil {
		return nil, fmt.Errorf("decode host preflight trust bundle: %w", err)
	}
	var extra any
	if err := dec.Decode(&extra); err != io.EOF {
		if err == nil {
			return nil, fmt.Errorf("decode host preflight trust bundle: multiple JSON documents are not allowed")
		}
		return nil, fmt.Errorf("decode host preflight trust bundle trailing data: %w", err)
	}
	if err := ValidateTrustBundle(&bundle); err != nil {
		return nil, err
	}
	return &bundle, nil
}

func MarshalTrustBundle(bundle *TrustBundle) ([]byte, error) {
	if err := ValidateTrustBundle(bundle); err != nil {
		return nil, err
	}
	raw, err := json.MarshalIndent(bundle, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("marshal host preflight trust bundle: %w", err)
	}
	return append(raw, '\n'), nil
}

func ValidateTrustBundle(bundle *TrustBundle) error {
	if bundle == nil {
		return fmt.Errorf("host preflight trust bundle is nil")
	}
	if bundle.APIVersion != APIVersion || bundle.Kind != TrustBundleKind {
		return fmt.Errorf("host preflight trust bundle identity must be api_version=%s kind=%s", APIVersion, TrustBundleKind)
	}
	if len(bundle.Signers) == 0 {
		return fmt.Errorf("host preflight trust bundle has no signers")
	}
	seenKey, seenNode := map[string]bool{}, map[string]bool{}
	for i, signer := range bundle.Signers {
		if !validKeyID(signer.KeyID) {
			return fmt.Errorf("host preflight trust signer %d has invalid key_id", i)
		}
		if strings.TrimSpace(signer.NodeID) == "" {
			return fmt.Errorf("host preflight trust signer %d has empty node_id", i)
		}
		if seenKey[signer.KeyID] {
			return fmt.Errorf("host preflight trust bundle duplicates key_id %q", signer.KeyID)
		}
		if seenNode[signer.NodeID] {
			return fmt.Errorf("host preflight trust bundle duplicates node_id %q", signer.NodeID)
		}
		seenKey[signer.KeyID], seenNode[signer.NodeID] = true, true
		decoded, err := base64.StdEncoding.DecodeString(signer.PublicKey)
		if err != nil || len(decoded) != ed25519.PublicKeySize {
			return fmt.Errorf("host preflight trust signer %q has invalid Ed25519 public_key", signer.KeyID)
		}
	}
	return nil
}

// LoadEd25519PrivateKey reads a PKCS#8 PEM key without following a symlink and
// rejects group/other permissions. It never returns or logs the source bytes.
func LoadEd25519PrivateKey(path string) (ed25519.PrivateKey, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return nil, fmt.Errorf("inspect signing private key: %w", err)
	}
	if !info.Mode().IsRegular() {
		return nil, fmt.Errorf("signing private key must be a regular file, not mode %s", info.Mode())
	}
	if info.Mode().Perm()&0o077 != 0 {
		return nil, fmt.Errorf("signing private key permissions must not allow group or other access")
	}
	if info.Size() <= 0 || info.Size() > privateKeyMaxSize {
		return nil, fmt.Errorf("signing private key size %d is outside the allowed range", info.Size())
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open signing private key: %w", err)
	}
	defer file.Close()
	openedInfo, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("stat opened signing private key: %w", err)
	}
	if !os.SameFile(info, openedInfo) || !openedInfo.Mode().IsRegular() || openedInfo.Mode().Perm()&0o077 != 0 {
		return nil, fmt.Errorf("signing private key changed identity, type, or permissions while opening")
	}
	raw, err := io.ReadAll(io.LimitReader(file, privateKeyMaxSize+1))
	if err != nil {
		return nil, fmt.Errorf("read signing private key: %w", err)
	}
	if len(raw) > privateKeyMaxSize {
		return nil, fmt.Errorf("signing private key exceeds the allowed size")
	}
	block, rest := pem.Decode(raw)
	if block == nil || block.Type != "PRIVATE KEY" || len(bytes.TrimSpace(rest)) != 0 {
		return nil, fmt.Errorf("signing private key must contain exactly one PKCS#8 PRIVATE KEY PEM block")
	}
	parsed, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	if err != nil {
		return nil, fmt.Errorf("parse signing private key: %w", err)
	}
	privateKey, ok := parsed.(ed25519.PrivateKey)
	if !ok || len(privateKey) != ed25519.PrivateKeySize {
		return nil, fmt.Errorf("signing private key is not Ed25519")
	}
	return privateKey, nil
}

func validateSignedReportShape(signed *SignedReport) error {
	if signed == nil {
		return fmt.Errorf("signed preflight report is nil")
	}
	if signed.APIVersion != APIVersion || signed.Kind != SignedReportKind {
		return fmt.Errorf("signed preflight report identity must be api_version=%s kind=%s", APIVersion, SignedReportKind)
	}
	if !validKeyID(signed.KeyID) {
		return fmt.Errorf("signed preflight report has invalid key_id")
	}
	if signed.Algorithm != SignatureEd25519 {
		return fmt.Errorf("signed preflight report algorithm must be %s", SignatureEd25519)
	}
	signature, err := base64.StdEncoding.DecodeString(signed.Signature)
	if err != nil || len(signature) != ed25519.SignatureSize {
		return fmt.Errorf("signed preflight report has invalid Ed25519 signature encoding")
	}
	return nil
}

func validateReportIntegrity(report *Report) error {
	if report == nil {
		return fmt.Errorf("preflight report is nil")
	}
	if report.APIVersion != APIVersion || report.Kind != ReportKind {
		return fmt.Errorf("preflight report identity must be api_version=%s kind=%s", APIVersion, ReportKind)
	}
	if strings.TrimSpace(report.PlanID) == "" || strings.TrimSpace(report.ManifestDigest) == "" || strings.TrimSpace(report.BundleDigest) == "" || strings.TrimSpace(report.NodeID) == "" {
		return fmt.Errorf("preflight report plan, manifest, bundle, and node identity are required")
	}
	if report.ObservedAt.IsZero() || report.ReferenceTime.IsZero() || strings.TrimSpace(report.EvidenceSource) == "" {
		return fmt.Errorf("preflight report timestamps and evidence_source are required")
	}
	passCount, errorCount := 0, 0
	firstError, lastError := "", ""
	for _, check := range report.Checks {
		switch check.Status {
		case StatusPass:
			passCount++
		case StatusFail:
			errorCount++
			message := check.ID + " " + check.Subject
			if check.Message != "" {
				message += ": " + check.Message
			}
			if firstError == "" {
				firstError = message
			}
			lastError = message
		default:
			return fmt.Errorf("preflight report check %q has invalid status %q", check.ID, check.Status)
		}
	}
	expectedResult := ResultOK
	if errorCount > 0 {
		expectedResult = ResultBlocked
	}
	if report.PassCount != passCount || report.ErrorCount != errorCount || report.Result != expectedResult || report.FirstError != firstError || report.LastError != lastError {
		return fmt.Errorf("preflight report result/count/error summary is inconsistent")
	}
	if report.ReportDigest != reportDigest(report) {
		return fmt.Errorf("preflight report digest does not match its canonical content")
	}
	return nil
}

func reportSignaturePayload(report *Report) ([]byte, error) {
	raw, err := json.Marshal(report)
	if err != nil {
		return nil, fmt.Errorf("marshal preflight report signature payload: %w", err)
	}
	return raw, nil
}

func validKeyID(value string) bool {
	if value == "" || len(value) > 128 {
		return false
	}
	for _, r := range value {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '-' || r == '_' || r == '.' {
			continue
		}
		return false
	}
	return true
}
