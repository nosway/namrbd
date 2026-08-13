package metadata

import (
	"encoding/hex"
	"fmt"
	"strings"
)

const (
	PayloadEncryptionHeaderVersion = 1
	PayloadCipherSuiteAES256GCM    = "aes_256_gcm"
)

type PayloadEncryptionHeader struct {
	HeaderVersion    int                       `json:"header_version"`
	CipherSuite      string                    `json:"cipher_suite"`
	EncryptionScope  string                    `json:"encryption_scope"`
	SecurityPolicyID string                    `json:"security_policy_id"`
	PolicyGeneration uint64                    `json:"policy_generation"`
	KeyProviderID    string                    `json:"key_provider_id"`
	DataKeyID        string                    `json:"data_key_id"`
	KeyID            string                    `json:"key_id"`
	KeyVersion       uint64                    `json:"key_version"`
	KeyGeneration    uint64                    `json:"key_generation"`
	ObjectID         string                    `json:"object_id"`
	BackendType      PhysicalObjectBackendType `json:"backend_type"`
	NonceHex         string                    `json:"nonce_hex"`
	NonceSource      string                    `json:"nonce_source"`
	AADDigest        string                    `json:"aad_digest"`
	LogicalOffset    uint64                    `json:"logical_offset,omitempty"`
	StripeID         string                    `json:"stripe_id,omitempty"`
	ShardID          uint32                    `json:"shard_id,omitempty"`
	ShardIDPresent   bool                      `json:"shard_id_present,omitempty"`
	PlaintextLength  uint64                    `json:"plaintext_length"`
	CiphertextLength uint64                    `json:"ciphertext_length"`
	AuthTagBytes     int                       `json:"auth_tag_bytes"`
	AuthTagHex       string                    `json:"auth_tag_hex,omitempty"`
}

func (h PayloadEncryptionHeader) Validate() error {
	h = h.normalize()
	if h.HeaderVersion != PayloadEncryptionHeaderVersion {
		return fmt.Errorf("unsupported encryption header version=%d", h.HeaderVersion)
	}
	if h.CipherSuite != PayloadCipherSuiteAES256GCM {
		return fmt.Errorf("unsupported cipher_suite %q", h.CipherSuite)
	}
	if h.EncryptionScope == "" {
		return fmt.Errorf("encryption_scope is required")
	}
	if h.SecurityPolicyID == "" {
		return fmt.Errorf("security_policy_id is required")
	}
	if h.PolicyGeneration == 0 {
		return fmt.Errorf("policy_generation is required")
	}
	if h.KeyProviderID == "" {
		return fmt.Errorf("key_provider_id is required")
	}
	if h.DataKeyID == "" {
		return fmt.Errorf("data_key_id is required")
	}
	if h.KeyID == "" {
		return fmt.Errorf("key_id is required")
	}
	if h.KeyVersion == 0 {
		return fmt.Errorf("key_version is required")
	}
	if h.ObjectID == "" {
		return fmt.Errorf("object_id is required")
	}
	switch h.BackendType {
	case PhysicalObjectBackendReplicated, PhysicalObjectBackendEC:
	default:
		return fmt.Errorf("unsupported backend_type %q", h.BackendType)
	}
	if len(h.NonceHex) != 24 {
		return fmt.Errorf("nonce_hex must encode 12 bytes")
	}
	if _, err := hex.DecodeString(h.NonceHex); err != nil {
		return fmt.Errorf("decode nonce_hex: %w", err)
	}
	if h.NonceSource == "" {
		return fmt.Errorf("nonce_source is required")
	}
	if len(h.AADDigest) != 64 {
		return fmt.Errorf("aad_digest must be sha256 hex")
	}
	if _, err := hex.DecodeString(h.AADDigest); err != nil {
		return fmt.Errorf("decode aad_digest: %w", err)
	}
	if h.PlaintextLength == 0 {
		return fmt.Errorf("plaintext_length is required")
	}
	if h.CiphertextLength <= h.PlaintextLength {
		return fmt.Errorf("ciphertext_length=%d must exceed plaintext_length=%d", h.CiphertextLength, h.PlaintextLength)
	}
	if h.AuthTagBytes != 16 {
		return fmt.Errorf("auth_tag_bytes=%d want=16", h.AuthTagBytes)
	}
	if h.AuthTagHex != "" {
		if len(h.AuthTagHex) != h.AuthTagBytes*2 {
			return fmt.Errorf("auth_tag_hex must encode %d bytes", h.AuthTagBytes)
		}
		if _, err := hex.DecodeString(h.AuthTagHex); err != nil {
			return fmt.Errorf("decode auth_tag_hex: %w", err)
		}
	}
	return nil
}

func (h PayloadEncryptionHeader) ValidateForPhysicalObject(objectID string, backend PhysicalObjectBackendType, logicalLength uint64) error {
	h = h.normalize()
	if err := h.Validate(); err != nil {
		return err
	}
	if h.ObjectID != strings.TrimSpace(objectID) {
		return fmt.Errorf("object_id=%q want %q", h.ObjectID, strings.TrimSpace(objectID))
	}
	if h.BackendType != backend {
		return fmt.Errorf("backend_type=%q want %q", h.BackendType, backend)
	}
	if logicalLength == 0 {
		return fmt.Errorf("logical_length is required")
	}
	if h.PlaintextLength != logicalLength {
		return fmt.Errorf("plaintext_length=%d want logical_length=%d", h.PlaintextLength, logicalLength)
	}
	return nil
}

func (h PayloadEncryptionHeader) ValidateForECShard(shardObjectID, stripeID string, shardID uint32, sizeBytes uint32) error {
	h = h.normalize()
	if err := h.Validate(); err != nil {
		return err
	}
	if strings.TrimSpace(shardObjectID) == "" {
		return fmt.Errorf("shard_object_id is required for encrypted shard")
	}
	if h.ObjectID != strings.TrimSpace(shardObjectID) {
		return fmt.Errorf("object_id=%q want shard_object_id=%q", h.ObjectID, strings.TrimSpace(shardObjectID))
	}
	if h.BackendType != PhysicalObjectBackendEC {
		return fmt.Errorf("backend_type=%q want %q", h.BackendType, PhysicalObjectBackendEC)
	}
	if h.StripeID != strings.TrimSpace(stripeID) {
		return fmt.Errorf("stripe_id=%q want %q", h.StripeID, strings.TrimSpace(stripeID))
	}
	if !h.ShardIDPresent {
		return fmt.Errorf("shard_id_present is required for encrypted shard")
	}
	if h.ShardID != shardID {
		return fmt.Errorf("shard_id=%d want %d", h.ShardID, shardID)
	}
	if sizeBytes == 0 {
		return fmt.Errorf("size_bytes is required for encrypted shard")
	}
	if h.PlaintextLength != uint64(sizeBytes) {
		return fmt.Errorf("plaintext_length=%d want size_bytes=%d", h.PlaintextLength, sizeBytes)
	}
	return nil
}

func (h PayloadEncryptionHeader) normalize() PayloadEncryptionHeader {
	h.CipherSuite = strings.TrimSpace(h.CipherSuite)
	h.EncryptionScope = strings.TrimSpace(h.EncryptionScope)
	h.SecurityPolicyID = strings.TrimSpace(h.SecurityPolicyID)
	h.KeyProviderID = strings.TrimSpace(h.KeyProviderID)
	h.DataKeyID = strings.TrimSpace(h.DataKeyID)
	h.KeyID = strings.TrimSpace(h.KeyID)
	h.ObjectID = strings.TrimSpace(h.ObjectID)
	h.NonceHex = strings.TrimSpace(h.NonceHex)
	h.NonceSource = strings.TrimSpace(h.NonceSource)
	h.AADDigest = strings.TrimSpace(h.AADDigest)
	h.StripeID = strings.TrimSpace(h.StripeID)
	h.AuthTagHex = strings.TrimSpace(h.AuthTagHex)
	return h
}

func clonePayloadEncryptionHeader(header *PayloadEncryptionHeader) *PayloadEncryptionHeader {
	if header == nil {
		return nil
	}
	cloned := *header
	return &cloned
}
