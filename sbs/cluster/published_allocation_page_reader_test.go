package cluster

import (
	"context"
	"errors"
	"testing"

	"github.com/nosway/namrbd/sbs/cluster/metadata"
)

type fakeAllocationPageViewStore struct {
	volume metadata.VolumeState
	page   metadata.AllocationPageRecord
	err    error
}

func (s fakeAllocationPageViewStore) GetVolumeState(context.Context, string) (metadata.VolumeState, error) {
	return s.volume, s.err
}

func (s fakeAllocationPageViewStore) GetCompatibleAllocationPage(context.Context, string, uint64, uint32, uint32) (metadata.AllocationPageRecord, error) {
	return s.page, s.err
}

type ecNativeAllocationPageViewStore struct {
	volume      metadata.VolumeState
	page        metadata.AllocationPageRecord
	nativeErr   error
	compatCalls int
}

func (s *ecNativeAllocationPageViewStore) GetVolumeState(context.Context, string) (metadata.VolumeState, error) {
	return s.volume, nil
}

func (s *ecNativeAllocationPageViewStore) GetAllocationPage(context.Context, string, uint64) (metadata.AllocationPageRecord, error) {
	if s.nativeErr != nil {
		return metadata.AllocationPageRecord{}, s.nativeErr
	}
	return s.page, nil
}

func (s *ecNativeAllocationPageViewStore) GetCompatibleAllocationPage(context.Context, string, uint64, uint32, uint32) (metadata.AllocationPageRecord, error) {
	s.compatCalls++
	return metadata.AllocationPageRecord{}, errors.New("compatible allocation page reader should not be called")
}

func TestBuildVolumeAllocationPageViewReadsRevisionAndPage(t *testing.T) {
	store := fakeAllocationPageViewStore{
		volume: metadata.VolumeState{VolumeID: "00a1b2c3", Revision: 7},
		page: metadata.AllocationPageRecord{
			VolumeID:       "00a1b2c3",
			PageNo:         2,
			PageBytes:      4096,
			ChunkSizeBytes: 1024,
			Revision:       6,
			Extents: []metadata.AllocationExtentRecord{
				{
					LogicalChunkStart:  4,
					ChunkCount:         2,
					Kind:               metadata.AllocationKindData,
					PhysicalChunkStart: 100,
					BackingRef:         "payload",
					Generation:         3,
					Checksum:           "sum",
					Encryption: &metadata.PayloadEncryptionHeader{
						HeaderVersion:    metadata.PayloadEncryptionHeaderVersion,
						CipherSuite:      "aes_256_gcm",
						EncryptionScope:  "volume",
						KeyProviderID:    "provider-a",
						DataKeyID:        "data-key-a",
						KeyID:            "key-a",
						ObjectID:         "replicated:00a1b2c3:100",
						BackendType:      metadata.PhysicalObjectBackendReplicated,
						NonceHex:         "00112233445566778899aabb",
						NonceSource:      "random_stored",
						LogicalOffset:    4096,
						PlaintextLength:  2048,
						CiphertextLength: 2048,
						AuthTagBytes:     16,
						AuthTagHex:       "00112233445566778899aabbccddeeff",
					},
				},
			},
		},
	}
	revision, page, err := BuildVolumeAllocationPageView(context.Background(), store, store, "00a1b2c3", 2, 4096, 1024)
	if err != nil {
		t.Fatalf("BuildVolumeAllocationPageView: %v", err)
	}
	if revision != 7 {
		t.Fatalf("revision=%d want 7", revision)
	}
	if page.GetVolumeId() != "00a1b2c3" || page.GetPageNo() != 2 || page.GetRevision() != 6 {
		t.Fatalf("unexpected page: %+v", page)
	}
	if len(page.GetExtents()) != 1 || page.GetExtents()[0].GetPhysicalChunkStart() != 100 {
		t.Fatalf("unexpected extents: %+v", page.GetExtents())
	}
	header := page.GetExtents()[0].GetEncryption()
	if header == nil || header.GetAuthTagHex() != "00112233445566778899aabbccddeeff" || header.GetBackendType() != string(metadata.PhysicalObjectBackendReplicated) {
		t.Fatalf("unexpected admin encryption header: %+v", header)
	}
	roundTrip := AllocationPageRecordFromAdmin(page)
	if roundTrip.Extents[0].Encryption == nil || roundTrip.Extents[0].Encryption.AuthTagHex != "00112233445566778899aabbccddeeff" {
		t.Fatalf("admin allocation page conversion lost encryption header: %+v", roundTrip.Extents[0].Encryption)
	}
}

func TestBuildVolumeAllocationPageViewECMissingNativePageReturnsZeroWithoutCompatibleScan(t *testing.T) {
	store := &ecNativeAllocationPageViewStore{
		volume: metadata.VolumeState{
			VolumeID:          "00a1b2c3",
			Revision:          7,
			RedundancyBackend: metadata.RedundancyBackendEC,
		},
		nativeErr: metadata.ErrNotFound,
	}
	revision, page, err := BuildVolumeAllocationPageView(context.Background(), store, store, "00a1b2c3", 2, 4096, 1024)
	if err != nil {
		t.Fatalf("BuildVolumeAllocationPageView: %v", err)
	}
	if revision != 7 {
		t.Fatalf("revision=%d want 7", revision)
	}
	if store.compatCalls != 0 {
		t.Fatalf("compatible allocation reader calls=%d want 0", store.compatCalls)
	}
	if page.GetVolumeId() != "00a1b2c3" || page.GetPageNo() != 2 || page.GetRevision() != 0 {
		t.Fatalf("unexpected zero page header: %+v", page)
	}
	if len(page.GetExtents()) != 1 {
		t.Fatalf("zero page extents=%d want 1", len(page.GetExtents()))
	}
	extent := page.GetExtents()[0]
	if extent.GetKind() != string(metadata.AllocationKindZero) || extent.GetLogicalChunkStart() != 8 || extent.GetChunkCount() != 4 {
		t.Fatalf("unexpected zero extent: %+v", extent)
	}
}

func TestBuildVolumeAllocationPageViewPropagatesStoreError(t *testing.T) {
	wantErr := errors.New("store failed")
	_, _, err := BuildVolumeAllocationPageView(context.Background(), fakeAllocationPageViewStore{err: wantErr}, fakeAllocationPageViewStore{}, "00a1b2c3", 2, 4096, 1024)
	if !errors.Is(err, wantErr) {
		t.Fatalf("err=%v want %v", err, wantErr)
	}
}
