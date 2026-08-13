//go:build !enterprise

package replication

import (
	"context"
	"fmt"
	"reflect"

	"github.com/nosway/namrbd/sbs/cluster/metadata"
	phasesecurity "github.com/nosway/namrbd/sbs/cluster/security"
)

type PhasePReplicaEncryptionConfig struct {
	Enabled                  bool
	ProviderID               string
	ProviderType             string
	PolicyID                 string
	PolicyGeneration         uint64
	DataKeyID                string
	KeyID                    string
	KeyVersion               uint64
	KeyAccessLeaseRequired   bool
	KeyAccessLeaseIssuer     phasesecurity.KeyAccessLeaseIssuer
	DataKeyUnwrapper         phasesecurity.DataKeyUnwrapper
	KeyAccessLeaseTTLSeconds uint64
	KeyAccessLeaseID         string
	KeyAccessLeaseIssuedTo   string
	KeyAccessLeaseRevoked    bool
}

type localReplicaPayloadEncryptor struct {
	enabled bool
}

type phasePReplicaFixtureModel struct{}

type phasePReplicaDataKeySource interface {
	unwrapPhasePReplicaDataKey(context.Context, phasePReplicaFixtureModel, string, string) ([]byte, error)
}

type phasePReplicaDataKeyRequestCache struct {
	base *localReplicaPayloadEncryptor
}

func newLocalReplicaPayloadEncryptor(cfg PhasePReplicaEncryptionConfig) *localReplicaPayloadEncryptor {
	if !cfg.Enabled {
		return nil
	}
	return &localReplicaPayloadEncryptor{enabled: true}
}

func newPhasePReplicaDataKeyRequestCache(base *localReplicaPayloadEncryptor) *phasePReplicaDataKeyRequestCache {
	if base == nil {
		return nil
	}
	return &phasePReplicaDataKeyRequestCache{base: base}
}

func (c *phasePReplicaDataKeyRequestCache) unwrapPhasePReplicaDataKey(context.Context, phasePReplicaFixtureModel, string, string) ([]byte, error) {
	return nil, errPhasePReplicaEncryptionEnterpriseOnly()
}

func (e *localReplicaPayloadEncryptor) encryptChunk(_ context.Context, _ string, _, _ uint64, _ uint64, plaintext []byte) ([]byte, *metadata.PayloadEncryptionHeader, error) {
	if e == nil {
		return append([]byte(nil), plaintext...), nil, nil
	}
	return nil, nil, errPhasePReplicaEncryptionEnterpriseOnly()
}

func (e *localReplicaPayloadEncryptor) encryptFixedChunk(ctx context.Context, volumeID string, logicalChunk, physicalChunkID, chunkSize uint64, plaintext []byte) ([]byte, *metadata.PayloadEncryptionHeader, error) {
	return e.encryptFixedChunkWithKeySource(ctx, volumeID, logicalChunk, physicalChunkID, chunkSize, plaintext, nil)
}

func (e *localReplicaPayloadEncryptor) encryptFixedChunkWithKeySource(_ context.Context, _ string, _, _ uint64, _ uint64, plaintext []byte, _ phasePReplicaDataKeySource) ([]byte, *metadata.PayloadEncryptionHeader, error) {
	if e == nil {
		return append([]byte(nil), plaintext...), nil, nil
	}
	return nil, nil, errPhasePReplicaEncryptionEnterpriseOnly()
}

func (e *localReplicaPayloadEncryptor) decryptChunk(_ context.Context, _ string, _, _ uint64, _ uint64, expected *metadata.PayloadEncryptionHeader, stored []byte) ([]byte, error) {
	if expected == nil {
		return append([]byte(nil), stored...), nil
	}
	return nil, errPhasePReplicaEncryptionEnterpriseOnly()
}

func (e *localReplicaPayloadEncryptor) decryptFixedChunk(ctx context.Context, volumeID string, logicalChunk, physicalChunkID, chunkSize uint64, expected *metadata.PayloadEncryptionHeader, stored []byte) ([]byte, error) {
	return e.decryptChunk(ctx, volumeID, logicalChunk, physicalChunkID, chunkSize, expected, stored)
}

func samePayloadEncryptionHeader(a, b *metadata.PayloadEncryptionHeader) bool {
	return reflect.DeepEqual(a, b)
}

func errPhasePReplicaEncryptionEnterpriseOnly() error {
	return fmt.Errorf("Phase P replicated payload encryption is Enterprise edition only")
}
