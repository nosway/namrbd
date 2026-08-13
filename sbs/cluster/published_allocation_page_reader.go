package cluster

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/nosway/namrbd/internal/adminclient"
	adminv1 "github.com/nosway/namrbd/sbs/admin/v1"
	"github.com/nosway/namrbd/sbs/cluster/metadata"
)

type AllocationPageReader interface {
	GetCompatibleAllocationPage(ctx context.Context, volumeID string, pageNo uint64, allocationPageBytes, allocationChunkSizeBytes uint32) (metadata.AllocationPageRecord, error)
}

type nativeAllocationPageReader interface {
	GetAllocationPage(ctx context.Context, volumeID string, pageNo uint64) (metadata.AllocationPageRecord, error)
}

type AllocationPageVolumeStateReader interface {
	GetVolumeState(ctx context.Context, volumeID string) (metadata.VolumeState, error)
}

type PublishedAllocationPageReaderOptions struct {
	Endpoint         string
	ClusterID        string
	SBSClusterID     string
	Fallback         AllocationPageReader
	AllowRawFallback bool
}

type publishedAllocationPageReader struct {
	endpoint         string
	clusterRef       *adminv1.ClusterRef
	fallback         AllocationPageReader
	allowRawFallback bool
}

func NewPublishedAllocationPageReader(opts PublishedAllocationPageReaderOptions) AllocationPageReader {
	adminEndpoint := strings.TrimSpace(opts.Endpoint)
	if adminEndpoint == "" && opts.Fallback != nil && opts.AllowRawFallback {
		return opts.Fallback
	}
	return &publishedAllocationPageReader{
		endpoint: adminEndpoint,
		clusterRef: &adminv1.ClusterRef{
			ClusterId:    strings.TrimSpace(opts.ClusterID),
			SbsClusterId: strings.TrimSpace(opts.SBSClusterID),
		},
		fallback:         opts.Fallback,
		allowRawFallback: opts.AllowRawFallback,
	}
}

func (r *publishedAllocationPageReader) GetCompatibleAllocationPage(ctx context.Context, volumeID string, pageNo uint64, allocationPageBytes, allocationChunkSizeBytes uint32) (metadata.AllocationPageRecord, error) {
	if r == nil {
		return metadata.AllocationPageRecord{}, fmt.Errorf("allocation page reader is not configured")
	}
	if strings.TrimSpace(r.endpoint) != "" {
		page, err := r.getAllocationPageFromAdmin(ctx, volumeID, pageNo, allocationPageBytes, allocationChunkSizeBytes)
		if err == nil {
			return page, nil
		}
		if r.allowRawFallback && r.fallback != nil {
			log.Printf("gateway runtime falling back to legacy raw allocation page metadata: %v", err)
			return r.fallback.GetCompatibleAllocationPage(ctx, volumeID, pageNo, allocationPageBytes, allocationChunkSizeBytes)
		}
		return metadata.AllocationPageRecord{}, err
	}
	if r.allowRawFallback && r.fallback != nil {
		return r.fallback.GetCompatibleAllocationPage(ctx, volumeID, pageNo, allocationPageBytes, allocationChunkSizeBytes)
	}
	return metadata.AllocationPageRecord{}, fmt.Errorf("allocation page reader requires reachable --sbs-admin-endpoint")
}

func (r *publishedAllocationPageReader) getAllocationPageFromAdmin(ctx context.Context, volumeID string, pageNo uint64, allocationPageBytes, allocationChunkSizeBytes uint32) (metadata.AllocationPageRecord, error) {
	dialCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	client, err := adminclient.Dial(dialCtx, r.endpoint)
	if err != nil {
		return metadata.AllocationPageRecord{}, err
	}
	defer client.Close()
	resp, err := client.Admin.GetVolumeAllocationPageView(ctx, &adminv1.GetVolumeAllocationPageViewRequest{
		Cluster:        r.clusterRef,
		VolumeId:       volumeID,
		PageNo:         pageNo,
		PageBytes:      allocationPageBytes,
		ChunkSizeBytes: allocationChunkSizeBytes,
	})
	if err != nil {
		return metadata.AllocationPageRecord{}, err
	}
	page := resp.GetAllocationPage()
	if page == nil {
		return metadata.AllocationPageRecord{}, fmt.Errorf("allocation page view missing page payload")
	}
	return AllocationPageRecordFromAdmin(page), nil
}

func AllocationPageRecordFromAdmin(page *adminv1.AllocationPageSummary) metadata.AllocationPageRecord {
	out := metadata.AllocationPageRecord{
		VolumeID:       strings.TrimSpace(page.GetVolumeId()),
		PageNo:         page.GetPageNo(),
		PageBytes:      page.GetPageBytes(),
		ChunkSizeBytes: page.GetChunkSizeBytes(),
		Revision:       page.GetRevision(),
	}
	for _, extent := range page.GetExtents() {
		if extent == nil {
			continue
		}
		out.Extents = append(out.Extents, metadata.AllocationExtentRecord{
			LogicalChunkStart:  extent.GetLogicalChunkStart(),
			ChunkCount:         extent.GetChunkCount(),
			Kind:               metadata.AllocationKind(strings.TrimSpace(extent.GetKind())),
			PhysicalChunkStart: extent.GetPhysicalChunkStart(),
			BackingRef:         strings.TrimSpace(extent.GetBackingRef()),
			Generation:         extent.GetGeneration(),
			Checksum:           strings.TrimSpace(extent.GetChecksum()),
			Encryption:         payloadEncryptionHeaderFromAdminSummary(extent.GetEncryption()),
		})
	}
	return out
}

func BuildVolumeAllocationPageView(ctx context.Context, volumeReader AllocationPageVolumeStateReader, pageReader AllocationPageReader, volumeID string, pageNo uint64, allocationPageBytes, allocationChunkSizeBytes uint32) (uint64, *adminv1.AllocationPageSummary, error) {
	if volumeReader == nil {
		return 0, nil, fmt.Errorf("volume state reader is not configured")
	}
	if pageReader == nil {
		return 0, nil, fmt.Errorf("allocation page reader is not configured")
	}
	volume, err := volumeReader.GetVolumeState(ctx, volumeID)
	if err != nil {
		return 0, nil, err
	}
	if strings.TrimSpace(volume.RedundancyBackend) == metadata.RedundancyBackendEC {
		if allocationPageBytes == 0 || allocationChunkSizeBytes == 0 || allocationPageBytes%allocationChunkSizeBytes != 0 {
			return 0, nil, fmt.Errorf("invalid allocation geometry: page_bytes=%d chunk_size_bytes=%d", allocationPageBytes, allocationChunkSizeBytes)
		}
		if nativeReader, ok := pageReader.(nativeAllocationPageReader); ok {
			page, err := nativeReader.GetAllocationPage(ctx, volumeID, pageNo)
			if err == nil {
				return volume.Revision, AllocationPageRecordToAdmin(page), nil
			}
			if errors.Is(err, metadata.ErrNotFound) {
				page = zeroAllocationPageView(volumeID, pageNo, allocationPageBytes, allocationChunkSizeBytes)
				return volume.Revision, AllocationPageRecordToAdmin(page), nil
			}
			return 0, nil, err
		}
	}
	page, err := pageReader.GetCompatibleAllocationPage(ctx, volumeID, pageNo, allocationPageBytes, allocationChunkSizeBytes)
	if err != nil {
		return 0, nil, err
	}
	return volume.Revision, AllocationPageRecordToAdmin(page), nil
}

func zeroAllocationPageView(volumeID string, pageNo uint64, pageBytes, chunkSizeBytes uint32) metadata.AllocationPageRecord {
	chunkCount := uint32(0)
	if chunkSizeBytes != 0 {
		chunkCount = pageBytes / chunkSizeBytes
	}
	return metadata.AllocationPageRecord{
		VolumeID:       volumeID,
		PageNo:         pageNo,
		PageBytes:      pageBytes,
		ChunkSizeBytes: chunkSizeBytes,
		Extents: []metadata.AllocationExtentRecord{
			{
				LogicalChunkStart: pageNo * uint64(chunkCount),
				ChunkCount:        chunkCount,
				Kind:              metadata.AllocationKindZero,
			},
		},
	}
}

func AllocationPageRecordToAdmin(page metadata.AllocationPageRecord) *adminv1.AllocationPageSummary {
	out := &adminv1.AllocationPageSummary{
		VolumeId:       page.VolumeID,
		PageNo:         page.PageNo,
		PageBytes:      page.PageBytes,
		ChunkSizeBytes: page.ChunkSizeBytes,
		Revision:       page.Revision,
	}
	for _, extent := range page.Extents {
		out.Extents = append(out.Extents, &adminv1.AllocationExtentSummary{
			LogicalChunkStart:  extent.LogicalChunkStart,
			ChunkCount:         extent.ChunkCount,
			Kind:               string(extent.Kind),
			PhysicalChunkStart: extent.PhysicalChunkStart,
			BackingRef:         extent.BackingRef,
			Generation:         extent.Generation,
			Checksum:           extent.Checksum,
			Encryption:         payloadEncryptionHeaderToAdminSummary(extent.Encryption),
		})
	}
	return out
}

func payloadEncryptionHeaderToAdminSummary(header *metadata.PayloadEncryptionHeader) *adminv1.PayloadEncryptionHeaderSummary {
	if header == nil {
		return nil
	}
	return &adminv1.PayloadEncryptionHeaderSummary{
		HeaderVersion:    int32(header.HeaderVersion),
		CipherSuite:      header.CipherSuite,
		EncryptionScope:  header.EncryptionScope,
		SecurityPolicyId: header.SecurityPolicyID,
		PolicyGeneration: header.PolicyGeneration,
		KeyProviderId:    header.KeyProviderID,
		DataKeyId:        header.DataKeyID,
		KeyId:            header.KeyID,
		KeyVersion:       header.KeyVersion,
		KeyGeneration:    header.KeyGeneration,
		ObjectId:         header.ObjectID,
		BackendType:      string(header.BackendType),
		NonceHex:         header.NonceHex,
		NonceSource:      header.NonceSource,
		AadDigest:        header.AADDigest,
		LogicalOffset:    header.LogicalOffset,
		StripeId:         header.StripeID,
		ShardId:          header.ShardID,
		PlaintextLength:  header.PlaintextLength,
		CiphertextLength: header.CiphertextLength,
		AuthTagBytes:     uint32(header.AuthTagBytes),
		AuthTagHex:       header.AuthTagHex,
		ShardIdPresent:   header.ShardIDPresent,
	}
}

func payloadEncryptionHeaderFromAdminSummary(header *adminv1.PayloadEncryptionHeaderSummary) *metadata.PayloadEncryptionHeader {
	if header == nil {
		return nil
	}
	return &metadata.PayloadEncryptionHeader{
		HeaderVersion:    int(header.GetHeaderVersion()),
		CipherSuite:      header.GetCipherSuite(),
		EncryptionScope:  header.GetEncryptionScope(),
		SecurityPolicyID: header.GetSecurityPolicyId(),
		PolicyGeneration: header.GetPolicyGeneration(),
		KeyProviderID:    header.GetKeyProviderId(),
		DataKeyID:        header.GetDataKeyId(),
		KeyID:            header.GetKeyId(),
		KeyVersion:       header.GetKeyVersion(),
		KeyGeneration:    header.GetKeyGeneration(),
		ObjectID:         header.GetObjectId(),
		BackendType:      metadata.PhysicalObjectBackendType(header.GetBackendType()),
		NonceHex:         header.GetNonceHex(),
		NonceSource:      header.GetNonceSource(),
		AADDigest:        header.GetAadDigest(),
		LogicalOffset:    header.GetLogicalOffset(),
		StripeID:         header.GetStripeId(),
		ShardID:          header.GetShardId(),
		ShardIDPresent:   header.GetShardIdPresent(),
		PlaintextLength:  header.GetPlaintextLength(),
		CiphertextLength: header.GetCiphertextLength(),
		AuthTagBytes:     int(header.GetAuthTagBytes()),
		AuthTagHex:       header.GetAuthTagHex(),
	}
}
