package dr

import (
	"crypto/ed25519"
	"crypto/rand"
	"testing"
	"time"
)

func TestFencingDecisionBindsIdentityAndFailsClosed(t *testing.T) {
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	now := time.Unix(1_800_000_000, 0).UTC()
	decision := FencingDecision{
		Version: FencingDecisionVersion, AuthorityID: "witness-a", DecisionID: "decision-a", Action: FencingActionPromote,
		ReplicationLinkID: "link-a", StandbyVolumeID: "standby-a", SourceClusterID: "source-a", TargetClusterID: "target-a",
		SourceVolumeID: "volume-a", TargetVolumeID: "volume-b", ExpectedPromoteGeneration: 1, FencingEpoch: 1,
		SourceFenced: true, DependencyAuthorityHealthy: true, TargetIntegrityVerified: true, KeyAccessVerified: true,
		SourceFencingReceipt: "sha256:source-fenced-a",
		IssuedAtUnix:         now.Add(-time.Second).Unix(), ExpiresAtUnix: now.Add(time.Minute).Unix(),
	}
	signed, err := SignFencingDecision(privateKey, decision)
	if err != nil {
		t.Fatalf("sign: %v", err)
	}
	digest, err := VerifyFencingDecision(publicKey, signed, decision, now)
	if err != nil || digest == "" {
		t.Fatalf("verify digest=%q err=%v", digest, err)
	}

	t.Run("identity mismatch", func(t *testing.T) {
		expected := decision
		expected.TargetVolumeID = "other-volume"
		if _, err := VerifyFencingDecision(publicKey, signed, expected, now); err == nil {
			t.Fatal("identity mismatch was admitted")
		}
	})
	t.Run("expired", func(t *testing.T) {
		if _, err := VerifyFencingDecision(publicKey, signed, decision, now.Add(2*time.Minute)); err == nil {
			t.Fatal("expired decision was admitted")
		}
	})
	t.Run("missing key proof", func(t *testing.T) {
		unsafe := decision
		unsafe.KeyAccessVerified = false
		unsafeSigned, signErr := SignFencingDecision(privateKey, unsafe)
		if signErr != nil {
			t.Fatalf("sign unsafe decision: %v", signErr)
		}
		if _, err := VerifyFencingDecision(publicKey, unsafeSigned, unsafe, now); err == nil {
			t.Fatal("decision without key proof was admitted")
		}
	})
	t.Run("wrong authority key", func(t *testing.T) {
		otherPublic, _, keyErr := ed25519.GenerateKey(rand.Reader)
		if keyErr != nil {
			t.Fatalf("generate other key: %v", keyErr)
		}
		if _, err := VerifyFencingDecision(otherPublic, signed, decision, now); err == nil {
			t.Fatal("decision signed by an unpinned key was admitted")
		}
	})
}

func TestSourceFencingReceiptBindsGenerationAndEpoch(t *testing.T) {
	input := SourceFenceReceiptInput{
		AuthorityID: "witness-a", DecisionID: "decision-a", ReplicationLinkID: "link-a",
		SourceClusterID: "source-a", TargetClusterID: "target-a", SourceVolumeID: "volume-a", TargetVolumeID: "volume-b",
		SourceFenceGeneration: 1, FencingEpoch: 1,
	}
	receipt, err := SourceFencingReceipt(input)
	if err != nil || receipt == "" {
		t.Fatalf("receipt=%q err=%v", receipt, err)
	}
	input.FencingEpoch++
	other, err := SourceFencingReceipt(input)
	if err != nil || other == receipt {
		t.Fatalf("receipt did not bind fencing epoch: first=%q second=%q err=%v", receipt, other, err)
	}
}
