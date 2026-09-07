package dr

import (
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

const (
	FencingDecisionVersion      = "namrbd.dr.fencing.v1"
	FencingActionPromote        = "promote"
	MaxFencingDecisionLifetime  = time.Hour
	MaxFencingDecisionClockSkew = 30 * time.Second
)

var ErrFencingDecisionInvalid = errors.New("dr fencing decision invalid")

// FencingDecision is the immutable statement signed by an authority that is
// independent from both the source and target metadata authorities. Every
// identity that can redirect write ownership is part of the signature.
type FencingDecision struct {
	Version                    string `json:"version"`
	AuthorityID                string `json:"authority_id"`
	DecisionID                 string `json:"decision_id"`
	Action                     string `json:"action"`
	ReplicationLinkID          string `json:"replication_link_id"`
	StandbyVolumeID            string `json:"standby_volume_id"`
	SourceClusterID            string `json:"source_cluster_id"`
	TargetClusterID            string `json:"target_cluster_id"`
	SourceVolumeID             string `json:"source_volume_id"`
	TargetVolumeID             string `json:"target_volume_id"`
	ExpectedPromoteGeneration  uint64 `json:"expected_promote_generation"`
	FencingEpoch               uint64 `json:"fencing_epoch"`
	SourceFenced               bool   `json:"source_fenced"`
	DependencyAuthorityHealthy bool   `json:"dependency_authority_healthy"`
	TargetIntegrityVerified    bool   `json:"target_integrity_verified"`
	KeyAccessVerified          bool   `json:"key_access_verified"`
	SourceFencingReceipt       string `json:"source_fencing_receipt"`
	IssuedAtUnix               int64  `json:"issued_at_unix"`
	ExpiresAtUnix              int64  `json:"expires_at_unix"`
}

type SignedFencingDecision struct {
	Decision        FencingDecision `json:"decision"`
	SignatureBase64 string          `json:"signature_base64"`
}

func SignFencingDecision(privateKey ed25519.PrivateKey, decision FencingDecision) (SignedFencingDecision, error) {
	if len(privateKey) != ed25519.PrivateKeySize {
		return SignedFencingDecision{}, fmt.Errorf("%w: Ed25519 private key length=%d", ErrFencingDecisionInvalid, len(privateKey))
	}
	canonical, err := canonicalFencingDecision(decision)
	if err != nil {
		return SignedFencingDecision{}, err
	}
	return SignedFencingDecision{
		Decision:        decision,
		SignatureBase64: base64.StdEncoding.EncodeToString(ed25519.Sign(privateKey, canonical)),
	}, nil
}

// VerifyFencingDecision verifies the signature, lifetime, fail-closed proof
// flags, and the complete caller-supplied expected identity. It returns a
// stable digest suitable for durable replay evidence.
func VerifyFencingDecision(publicKey ed25519.PublicKey, signed SignedFencingDecision, expected FencingDecision, now time.Time) (string, error) {
	if len(publicKey) != ed25519.PublicKeySize {
		return "", fmt.Errorf("%w: Ed25519 public key length=%d", ErrFencingDecisionInvalid, len(publicKey))
	}
	canonical, err := canonicalFencingDecision(signed.Decision)
	if err != nil {
		return "", err
	}
	signature, err := base64.StdEncoding.Strict().DecodeString(strings.TrimSpace(signed.SignatureBase64))
	if err != nil || len(signature) != ed25519.SignatureSize || !ed25519.Verify(publicKey, canonical, signature) {
		return "", fmt.Errorf("%w: signature verification failed", ErrFencingDecisionInvalid)
	}
	decision := signed.Decision
	if decision != expected {
		return "", fmt.Errorf("%w: signed identity does not match the promotion request", ErrFencingDecisionInvalid)
	}
	now = now.UTC()
	issued := time.Unix(decision.IssuedAtUnix, 0).UTC()
	expires := time.Unix(decision.ExpiresAtUnix, 0).UTC()
	if decision.IssuedAtUnix <= 0 || decision.ExpiresAtUnix <= decision.IssuedAtUnix || expires.Sub(issued) > MaxFencingDecisionLifetime {
		return "", fmt.Errorf("%w: invalid decision lifetime", ErrFencingDecisionInvalid)
	}
	if issued.After(now.Add(MaxFencingDecisionClockSkew)) {
		return "", fmt.Errorf("%w: decision is not active yet", ErrFencingDecisionInvalid)
	}
	if !expires.After(now) {
		return "", fmt.Errorf("%w: decision expired", ErrFencingDecisionInvalid)
	}
	if !decision.SourceFenced || !decision.DependencyAuthorityHealthy || !decision.TargetIntegrityVerified || !decision.KeyAccessVerified || strings.TrimSpace(decision.SourceFencingReceipt) == "" {
		return "", fmt.Errorf("%w: decision did not prove every promotion precondition", ErrFencingDecisionInvalid)
	}
	digest := sha256.Sum256(canonical)
	return "sha256:" + hex.EncodeToString(digest[:]), nil
}

type SourceFenceReceiptInput struct {
	AuthorityID           string `json:"authority_id"`
	DecisionID            string `json:"decision_id"`
	ReplicationLinkID     string `json:"replication_link_id"`
	SourceClusterID       string `json:"source_cluster_id"`
	TargetClusterID       string `json:"target_cluster_id"`
	SourceVolumeID        string `json:"source_volume_id"`
	TargetVolumeID        string `json:"target_volume_id"`
	SourceFenceGeneration uint64 `json:"source_fence_generation"`
	FencingEpoch          uint64 `json:"fencing_epoch"`
}

func SourceFencingReceipt(input SourceFenceReceiptInput) (string, error) {
	input.AuthorityID = strings.TrimSpace(input.AuthorityID)
	input.DecisionID = strings.TrimSpace(input.DecisionID)
	input.ReplicationLinkID = strings.TrimSpace(input.ReplicationLinkID)
	input.SourceClusterID = strings.TrimSpace(input.SourceClusterID)
	input.TargetClusterID = strings.TrimSpace(input.TargetClusterID)
	input.SourceVolumeID = strings.TrimSpace(input.SourceVolumeID)
	input.TargetVolumeID = strings.TrimSpace(input.TargetVolumeID)
	if input.AuthorityID == "" || input.DecisionID == "" || input.ReplicationLinkID == "" || input.SourceClusterID == "" || input.TargetClusterID == "" || input.SourceVolumeID == "" || input.TargetVolumeID == "" || input.SourceFenceGeneration == 0 || input.FencingEpoch == 0 {
		return "", fmt.Errorf("%w: incomplete source fencing receipt input", ErrFencingDecisionInvalid)
	}
	canonical, err := json.Marshal(input)
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(canonical)
	return "sha256:" + hex.EncodeToString(digest[:]), nil
}

func FencingPublicKeyFingerprint(publicKey ed25519.PublicKey) (string, error) {
	if len(publicKey) != ed25519.PublicKeySize {
		return "", fmt.Errorf("%w: Ed25519 public key length=%d", ErrFencingDecisionInvalid, len(publicKey))
	}
	digest := sha256.Sum256(publicKey)
	return "sha256:" + hex.EncodeToString(digest[:]), nil
}

func canonicalFencingDecision(decision FencingDecision) ([]byte, error) {
	decision.Version = strings.TrimSpace(decision.Version)
	decision.AuthorityID = strings.TrimSpace(decision.AuthorityID)
	decision.DecisionID = strings.TrimSpace(decision.DecisionID)
	decision.Action = strings.TrimSpace(decision.Action)
	decision.ReplicationLinkID = strings.TrimSpace(decision.ReplicationLinkID)
	decision.StandbyVolumeID = strings.TrimSpace(decision.StandbyVolumeID)
	decision.SourceClusterID = strings.TrimSpace(decision.SourceClusterID)
	decision.TargetClusterID = strings.TrimSpace(decision.TargetClusterID)
	decision.SourceVolumeID = strings.TrimSpace(decision.SourceVolumeID)
	decision.TargetVolumeID = strings.TrimSpace(decision.TargetVolumeID)
	if decision.Version != FencingDecisionVersion || decision.AuthorityID == "" || decision.DecisionID == "" || decision.Action != FencingActionPromote ||
		decision.ReplicationLinkID == "" || decision.StandbyVolumeID == "" || decision.SourceClusterID == "" || decision.TargetClusterID == "" ||
		decision.SourceVolumeID == "" || decision.TargetVolumeID == "" || decision.ExpectedPromoteGeneration == 0 || decision.FencingEpoch == 0 {
		return nil, fmt.Errorf("%w: incomplete or unsupported decision", ErrFencingDecisionInvalid)
	}
	return json.Marshal(decision)
}
