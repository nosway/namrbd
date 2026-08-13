package service

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nosway/namrbd/internal/structuredlog"
	clustermeta "github.com/nosway/namrbd/sbs/cluster/metadata"
)

type InitialZeroMapAllocationResolver interface {
	ResolveAllocationPages(ctx context.Context, volumeID string, offsetBytes, lengthBytes uint64, pageBytes, chunkSizeBytes uint32) ([]clustermeta.ResolvedAllocationPage, error)
}

type sbsDataRepository struct {
	meta               MetadataRepository
	client             SBSClient
	gatewayID          string
	version            string
	allocationResolver InitialZeroMapAllocationResolver

	mu            sync.RWMutex
	handles       map[uint64]openVolumeState
	openReuseTTL  time.Duration
	retryMu       sync.Mutex
	retryCounters map[string]uint64
	nextReq       uint64
	sessionNonce  string
}

type openVolumeState struct {
	handle       string
	attachmentID string
	hostID       string
	generation   uint64
	validatedAt  time.Time
	validUntil   time.Time
}

type cloneSBSClient interface {
	ReadClone(ctx context.Context, cloneID string, req *ReadRequest) (*ReadResponse, error)
	WriteClone(ctx context.Context, cloneID string, req *WriteRequest) (*WriteResponse, error)
}

type snapshotSBSClient interface {
	ReadSnapshot(ctx context.Context, snapshotID string, req *ReadRequest) (*ReadResponse, error)
}

func NewSBSDataRepository(meta MetadataRepository, client SBSClient, gatewayID string, clientVersion ...string) DataRepository {
	return newSBSDataRepository(meta, client, gatewayID, 0, clientVersion...)
}

func NewSBSDataRepositoryWithOpenReuseTTL(meta MetadataRepository, client SBSClient, gatewayID string, openReuseTTL time.Duration, clientVersion ...string) DataRepository {
	if openReuseTTL < 0 {
		openReuseTTL = 0
	}
	return newSBSDataRepository(meta, client, gatewayID, openReuseTTL, clientVersion...)
}

func NewSBSDataRepositoryWithOpenReuseTTLAndAllocationResolver(meta MetadataRepository, client SBSClient, gatewayID string, openReuseTTL time.Duration, allocationResolver InitialZeroMapAllocationResolver, clientVersion ...string) DataRepository {
	if openReuseTTL < 0 {
		openReuseTTL = 0
	}
	repo := newSBSDataRepository(meta, client, gatewayID, openReuseTTL, clientVersion...)
	repo.allocationResolver = allocationResolver
	return repo
}

func newSBSDataRepository(meta MetadataRepository, client SBSClient, gatewayID string, openReuseTTL time.Duration, clientVersion ...string) *sbsDataRepository {
	version := ""
	if len(clientVersion) > 0 {
		version = clientVersion[0]
	}
	return &sbsDataRepository{
		meta:          meta,
		client:        client,
		gatewayID:     gatewayID,
		version:       version,
		handles:       make(map[uint64]openVolumeState),
		openReuseTTL:  openReuseTTL,
		retryCounters: map[string]uint64{},
		sessionNonce:  fmt.Sprintf("%x", time.Now().UnixNano()),
	}
}

func (r *sbsDataRepository) InitialZeroMapEvidence(ctx context.Context, volume VolumeSpec) (InitialZeroMapEvidence, error) {
	if r == nil || r.allocationResolver == nil {
		return InitialZeroMapEvidence{}, nil
	}
	if volume.SizeBytes == 0 || volume.ExtentPageBytes == 0 || volume.ChunkSizeBytes == 0 || volume.ExtentPageBytes%volume.ChunkSizeBytes != 0 {
		return InitialZeroMapEvidence{}, nil
	}
	pages, err := r.allocationResolver.ResolveAllocationPages(ctx, CanonicalVolumeID(uint64(volume.ID)), 0, volume.SizeBytes, volume.ExtentPageBytes, volume.ChunkSizeBytes)
	if err != nil {
		return InitialZeroMapEvidence{}, err
	}
	if !resolvedAllocationPagesCoverZeroRange(pages, 0, volume.SizeBytes, volume.ChunkSizeBytes) {
		return InitialZeroMapEvidence{Trusted: true, AllZero: false, CheckedPageCount: len(pages)}, nil
	}
	return InitialZeroMapEvidence{
		Trusted:           true,
		AllZero:           true,
		GranuleBytes:      64 << 10,
		CheckedPageCount:  len(pages),
		CheckedChunkCount: (volume.SizeBytes + uint64(volume.ChunkSizeBytes) - 1) / uint64(volume.ChunkSizeBytes),
	}, nil
}

func resolvedAllocationPagesCoverZeroRange(pages []clustermeta.ResolvedAllocationPage, offsetBytes, lengthBytes uint64, chunkSizeBytes uint32) bool {
	if lengthBytes == 0 || chunkSizeBytes == 0 {
		return false
	}
	startChunk := offsetBytes / uint64(chunkSizeBytes)
	endChunk := (offsetBytes + lengthBytes + uint64(chunkSizeBytes) - 1) / uint64(chunkSizeBytes)
	for logicalChunk := startChunk; logicalChunk < endChunk; logicalChunk++ {
		if !resolvedAllocationPagesChunkIsZero(pages, logicalChunk) {
			return false
		}
	}
	return true
}

func resolvedAllocationPagesChunkIsZero(pages []clustermeta.ResolvedAllocationPage, logicalChunk uint64) bool {
	for _, resolved := range pages {
		if logicalChunk < resolved.RangeStartChunk || logicalChunk >= resolved.RangeEndChunk {
			continue
		}
		return allocationPageChunkIsZero(resolved.Page, logicalChunk)
	}
	return false
}

func allocationPageChunkIsZero(page clustermeta.AllocationPageRecord, logicalChunk uint64) bool {
	for _, extent := range page.Extents {
		start := extent.LogicalChunkStart
		end := start + uint64(extent.ChunkCount)
		if logicalChunk >= start && logicalChunk < end {
			return extent.Kind == clustermeta.AllocationKindZero
		}
	}
	return false
}

func (r *sbsDataRepository) CloseAttachment(ctx context.Context, volumeID uint64, attachment AttachmentRecord) error {
	r.mu.RLock()
	current, ok := r.handles[volumeID]
	r.mu.RUnlock()
	if !ok {
		return nil
	}
	if current.attachmentID != attachment.AttachmentID || current.generation != attachment.Generation {
		r.evictHandle(volumeID)
		return nil
	}
	return r.closeHandle(ctx, volumeID, current, attachment.HostID, "sbs_close_on_detach")
}

func (r *sbsDataRepository) CloseLocalAttachment(ctx context.Context, volumeID uint64, hostID, attachmentID string) error {
	r.mu.RLock()
	current, ok := r.handles[volumeID]
	r.mu.RUnlock()
	if !ok || current.attachmentID != attachmentID {
		return nil
	}
	return r.closeHandle(ctx, volumeID, current, hostID, "sbs_close_local_on_detach")
}

func (r *sbsDataRepository) ReloadAttachment(ctx context.Context, volume VolumeSpec) error {
	volumeID := uint64(volume.ID)
	attachment, err := r.meta.GetAttachment(ctx, volumeID)
	if err != nil {
		return err
	}
	if attachment.AttachmentID == "" || attachment.Generation == 0 {
		return nil
	}

	r.mu.RLock()
	current, ok := r.handles[volumeID]
	r.mu.RUnlock()
	if ok && current.attachmentID == attachment.AttachmentID && current.generation == attachment.Generation {
		if err := r.closeHandle(ctx, volumeID, current, attachment.HostID, "sbs_close_on_reload"); err != nil {
			return err
		}
	}
	_, _, err = r.ensureOpen(ctx, volume)
	return err
}

func (r *sbsDataRepository) ReadAt(ctx context.Context, volume VolumeSpec, offsetBytes, lengthBytes uint64) ([]byte, error) {
	result, err := r.ReadAtResult(ctx, volume, offsetBytes, lengthBytes)
	if err != nil {
		return nil, err
	}
	if result.ZeroData && result.Data == nil {
		return make([]byte, lengthBytes), nil
	}
	return append([]byte(nil), result.Data...), nil
}

func (r *sbsDataRepository) ReadAtResult(ctx context.Context, volume VolumeSpec, offsetBytes, lengthBytes uint64) (ReadResult, error) {
	start := time.Now()
	loadMeta := LoadMetadataFromContext(ctx)
	ensureStart := time.Now()
	openState, reqCtx, err := r.ensureOpen(ctx, volume)
	ensureDuration := time.Since(ensureStart)
	if err != nil {
		structuredlog.Error("gateway.sbs", "sbs_read_prepare_failed", err,
			structuredlog.F("volume_id", CanonicalVolumeID(uint64(volume.ID))),
			structuredlog.F("offset_bytes", offsetBytes),
			structuredlog.F("length_bytes", lengthBytes),
			structuredlog.F("load_index", loadMeta.Index),
			structuredlog.F("load_phase", loadMeta.Phase),
			structuredlog.F("duration_ms", time.Since(start).Milliseconds()),
			structuredlog.F("ensure_open_duration_ms", ensureDuration.Milliseconds()),
		)
		return ReadResult{}, err
	}
	var rpcDuration time.Duration
	reopenRetry := false
	rpcStart := time.Now()
	resp, err := r.client.Read(ctx, &ReadRequest{
		VolumeID:     CanonicalVolumeID(uint64(volume.ID)),
		VolumeHandle: openState.handle,
		OffsetBytes:  offsetBytes,
		LengthBytes:  lengthBytes,
		Context:      reqCtx,
	})
	rpcDuration += time.Since(rpcStart)
	if err != nil && isRetryableReopenError(err) {
		r.recordRetry("read_reopen_retry")
		reopenRetry = true
		structuredlog.Info("gateway.sbs", "sbs_read_retrying_after_reopen_error",
			structuredlog.F("request_id", reqCtx.RequestID),
			structuredlog.F("trace_id", reqCtx.TraceID),
			structuredlog.F("volume_id", CanonicalVolumeID(uint64(volume.ID))),
			structuredlog.F("attachment_id", reqCtx.AttachmentID),
			structuredlog.F("generation", reqCtx.Generation),
		)
		r.evictHandle(uint64(volume.ID))
		ensureStart = time.Now()
		openState, reqCtx, err = r.ensureOpen(ctx, volume)
		ensureDuration += time.Since(ensureStart)
		if err != nil {
			structuredlog.Error("gateway.sbs", "sbs_read_prepare_failed", err,
				structuredlog.F("volume_id", CanonicalVolumeID(uint64(volume.ID))),
				structuredlog.F("offset_bytes", offsetBytes),
				structuredlog.F("length_bytes", lengthBytes),
				structuredlog.F("load_index", loadMeta.Index),
				structuredlog.F("load_phase", loadMeta.Phase),
				structuredlog.F("duration_ms", time.Since(start).Milliseconds()),
				structuredlog.F("ensure_open_duration_ms", ensureDuration.Milliseconds()),
				structuredlog.F("sbs_read_rpc_duration_ms", rpcDuration.Milliseconds()),
				structuredlog.F("reopen_retry", reopenRetry),
			)
			return ReadResult{}, err
		}
		rpcStart = time.Now()
		resp, err = r.client.Read(ctx, &ReadRequest{
			VolumeID:     CanonicalVolumeID(uint64(volume.ID)),
			VolumeHandle: openState.handle,
			OffsetBytes:  offsetBytes,
			LengthBytes:  lengthBytes,
			Context:      reqCtx,
		})
		rpcDuration += time.Since(rpcStart)
	}
	if err != nil {
		structuredlog.Error("gateway.sbs", "sbs_read_failed", err,
			structuredlog.F("request_id", reqCtx.RequestID),
			structuredlog.F("trace_id", reqCtx.TraceID),
			structuredlog.F("volume_id", CanonicalVolumeID(uint64(volume.ID))),
			structuredlog.F("attachment_id", reqCtx.AttachmentID),
			structuredlog.F("generation", reqCtx.Generation),
			structuredlog.F("offset_bytes", offsetBytes),
			structuredlog.F("length_bytes", lengthBytes),
			structuredlog.F("load_index", loadMeta.Index),
			structuredlog.F("load_phase", loadMeta.Phase),
			structuredlog.F("duration_ms", time.Since(start).Milliseconds()),
			structuredlog.F("ensure_open_duration_ms", ensureDuration.Milliseconds()),
			structuredlog.F("sbs_read_rpc_duration_ms", rpcDuration.Milliseconds()),
			structuredlog.F("reopen_retry", reopenRetry),
		)
		return ReadResult{}, err
	}
	structuredlog.Info("gateway.sbs", "sbs_read_completed",
		structuredlog.F("request_id", reqCtx.RequestID),
		structuredlog.F("trace_id", reqCtx.TraceID),
		structuredlog.F("volume_id", CanonicalVolumeID(uint64(volume.ID))),
		structuredlog.F("volume_handle", openState.handle),
		structuredlog.F("attachment_id", reqCtx.AttachmentID),
		structuredlog.F("generation", reqCtx.Generation),
		structuredlog.F("offset_bytes", offsetBytes),
		structuredlog.F("length_bytes", lengthBytes),
		structuredlog.F("load_index", loadMeta.Index),
		structuredlog.F("load_phase", loadMeta.Phase),
		structuredlog.F("duration_ms", time.Since(start).Milliseconds()),
		structuredlog.F("ensure_open_duration_ms", ensureDuration.Milliseconds()),
		structuredlog.F("sbs_read_rpc_duration_ms", rpcDuration.Milliseconds()),
		structuredlog.F("reopen_retry", reopenRetry),
		structuredlog.F("zero_data", resp.ZeroData),
	)
	if resp.ZeroData {
		return ReadResult{ZeroData: true}, nil
	}
	return ReadResult{Data: resp.Data, ZeroData: resp.ZeroData}, nil
}

func (r *sbsDataRepository) ReadCloneAt(ctx context.Context, volume VolumeSpec, cloneID string, offsetBytes, lengthBytes uint64) ([]byte, error) {
	cloneClient, ok := r.client.(cloneSBSClient)
	if !ok {
		return nil, ErrNotSupported
	}
	openState, reqCtx, err := r.ensureOpen(ctx, volume)
	if err != nil {
		return nil, err
	}
	resp, err := cloneClient.ReadClone(ctx, cloneID, &ReadRequest{
		VolumeID:     CanonicalVolumeID(uint64(volume.ID)),
		VolumeHandle: openState.handle,
		OffsetBytes:  offsetBytes,
		LengthBytes:  lengthBytes,
		Context:      reqCtx,
	})
	if err != nil && isRetryableReopenError(err) {
		r.recordRetry("clone_read_reopen_retry")
		structuredlog.Info("gateway.sbs", "sbs_clone_read_retrying_after_reopen_error",
			structuredlog.F("request_id", reqCtx.RequestID),
			structuredlog.F("trace_id", reqCtx.TraceID),
			structuredlog.F("volume_id", CanonicalVolumeID(uint64(volume.ID))),
			structuredlog.F("clone_id", cloneID),
			structuredlog.F("attachment_id", reqCtx.AttachmentID),
			structuredlog.F("generation", reqCtx.Generation),
		)
		r.evictHandle(uint64(volume.ID))
		openState, reqCtx, err = r.ensureOpen(ctx, volume)
		if err != nil {
			return nil, err
		}
		resp, err = cloneClient.ReadClone(ctx, cloneID, &ReadRequest{
			VolumeID:     CanonicalVolumeID(uint64(volume.ID)),
			VolumeHandle: openState.handle,
			OffsetBytes:  offsetBytes,
			LengthBytes:  lengthBytes,
			Context:      reqCtx,
		})
	}
	if err != nil {
		structuredlog.Error("gateway.sbs", "sbs_clone_read_failed", err,
			structuredlog.F("request_id", reqCtx.RequestID),
			structuredlog.F("trace_id", reqCtx.TraceID),
			structuredlog.F("volume_id", CanonicalVolumeID(uint64(volume.ID))),
			structuredlog.F("clone_id", cloneID),
			structuredlog.F("attachment_id", reqCtx.AttachmentID),
			structuredlog.F("generation", reqCtx.Generation),
			structuredlog.F("offset_bytes", offsetBytes),
			structuredlog.F("length_bytes", lengthBytes),
		)
		return nil, err
	}
	structuredlog.Info("gateway.sbs", "sbs_clone_read_completed",
		structuredlog.F("request_id", reqCtx.RequestID),
		structuredlog.F("trace_id", reqCtx.TraceID),
		structuredlog.F("volume_id", CanonicalVolumeID(uint64(volume.ID))),
		structuredlog.F("clone_id", cloneID),
		structuredlog.F("volume_handle", openState.handle),
		structuredlog.F("attachment_id", reqCtx.AttachmentID),
		structuredlog.F("generation", reqCtx.Generation),
		structuredlog.F("offset_bytes", offsetBytes),
		structuredlog.F("length_bytes", lengthBytes),
	)
	return append([]byte(nil), resp.Data...), nil
}

func (r *sbsDataRepository) ReadSnapshotAt(ctx context.Context, volume VolumeSpec, snapshotID string, offsetBytes, lengthBytes uint64) ([]byte, error) {
	snapshotClient, ok := r.client.(snapshotSBSClient)
	if !ok {
		return nil, ErrNotSupported
	}
	openState, reqCtx, err := r.ensureOpen(ctx, volume)
	if err != nil {
		return nil, err
	}
	resp, err := snapshotClient.ReadSnapshot(ctx, snapshotID, &ReadRequest{
		VolumeID:     CanonicalVolumeID(uint64(volume.ID)),
		VolumeHandle: openState.handle,
		OffsetBytes:  offsetBytes,
		LengthBytes:  lengthBytes,
		Context:      reqCtx,
	})
	if err != nil && isRetryableReopenError(err) {
		r.recordRetry("snapshot_read_reopen_retry")
		structuredlog.Info("gateway.sbs", "sbs_snapshot_read_retrying_after_reopen_error",
			structuredlog.F("request_id", reqCtx.RequestID),
			structuredlog.F("trace_id", reqCtx.TraceID),
			structuredlog.F("volume_id", CanonicalVolumeID(uint64(volume.ID))),
			structuredlog.F("snapshot_id", snapshotID),
			structuredlog.F("attachment_id", reqCtx.AttachmentID),
			structuredlog.F("generation", reqCtx.Generation),
		)
		r.evictHandle(uint64(volume.ID))
		openState, reqCtx, err = r.ensureOpen(ctx, volume)
		if err != nil {
			return nil, err
		}
		resp, err = snapshotClient.ReadSnapshot(ctx, snapshotID, &ReadRequest{
			VolumeID:     CanonicalVolumeID(uint64(volume.ID)),
			VolumeHandle: openState.handle,
			OffsetBytes:  offsetBytes,
			LengthBytes:  lengthBytes,
			Context:      reqCtx,
		})
	}
	if err != nil {
		structuredlog.Error("gateway.sbs", "sbs_snapshot_read_failed", err,
			structuredlog.F("request_id", reqCtx.RequestID),
			structuredlog.F("trace_id", reqCtx.TraceID),
			structuredlog.F("volume_id", CanonicalVolumeID(uint64(volume.ID))),
			structuredlog.F("snapshot_id", snapshotID),
			structuredlog.F("attachment_id", reqCtx.AttachmentID),
			structuredlog.F("generation", reqCtx.Generation),
			structuredlog.F("offset_bytes", offsetBytes),
			structuredlog.F("length_bytes", lengthBytes),
		)
		return nil, err
	}
	structuredlog.Info("gateway.sbs", "sbs_snapshot_read_completed",
		structuredlog.F("request_id", reqCtx.RequestID),
		structuredlog.F("trace_id", reqCtx.TraceID),
		structuredlog.F("volume_id", CanonicalVolumeID(uint64(volume.ID))),
		structuredlog.F("snapshot_id", snapshotID),
		structuredlog.F("volume_handle", openState.handle),
		structuredlog.F("attachment_id", reqCtx.AttachmentID),
		structuredlog.F("generation", reqCtx.Generation),
		structuredlog.F("offset_bytes", offsetBytes),
		structuredlog.F("length_bytes", lengthBytes),
	)
	return append([]byte(nil), resp.Data...), nil
}

func (r *sbsDataRepository) WriteAt(ctx context.Context, volume VolumeSpec, offsetBytes, lengthBytes uint64, data []byte) error {
	start := time.Now()
	loadMeta := LoadMetadataFromContext(ctx)
	ensureStart := time.Now()
	openState, reqCtx, err := r.ensureOpen(ctx, volume)
	ensureDuration := time.Since(ensureStart)
	if err != nil {
		structuredlog.Error("gateway.sbs", "sbs_write_prepare_failed", err,
			structuredlog.F("volume_id", CanonicalVolumeID(uint64(volume.ID))),
			structuredlog.F("offset_bytes", offsetBytes),
			structuredlog.F("length_bytes", lengthBytes),
			structuredlog.F("load_index", loadMeta.Index),
			structuredlog.F("load_phase", loadMeta.Phase),
			structuredlog.F("duration_ms", time.Since(start).Milliseconds()),
			structuredlog.F("ensure_open_duration_ms", ensureDuration.Milliseconds()),
		)
		return err
	}
	var rpcDuration time.Duration
	rpcStart := time.Now()
	_, err = r.client.Write(ctx, &WriteRequest{
		VolumeID:     CanonicalVolumeID(uint64(volume.ID)),
		VolumeHandle: openState.handle,
		OffsetBytes:  offsetBytes,
		LengthBytes:  lengthBytes,
		Data:         data,
		Context:      reqCtx,
	})
	rpcDuration += time.Since(rpcStart)
	if err != nil && isRetryableReopenError(err) {
		r.recordRetry("write_reopen_retry")
		structuredlog.Info("gateway.sbs", "sbs_write_retrying_after_reopen_error",
			structuredlog.F("request_id", reqCtx.RequestID),
			structuredlog.F("trace_id", reqCtx.TraceID),
			structuredlog.F("volume_id", CanonicalVolumeID(uint64(volume.ID))),
			structuredlog.F("attachment_id", reqCtx.AttachmentID),
			structuredlog.F("generation", reqCtx.Generation),
			structuredlog.F("idempotency_key", reqCtx.IdempotencyKey),
		)
		r.evictHandle(uint64(volume.ID))
		openState, reqCtx, err = r.ensureOpen(ctx, volume)
		if err != nil {
			return err
		}
		rpcStart = time.Now()
		_, err = r.client.Write(ctx, &WriteRequest{
			VolumeID:     CanonicalVolumeID(uint64(volume.ID)),
			VolumeHandle: openState.handle,
			OffsetBytes:  offsetBytes,
			LengthBytes:  lengthBytes,
			Data:         data,
			Context:      reqCtx,
		})
		rpcDuration += time.Since(rpcStart)
	}
	if err != nil {
		structuredlog.Error("gateway.sbs", "sbs_write_failed", err,
			structuredlog.F("request_id", reqCtx.RequestID),
			structuredlog.F("trace_id", reqCtx.TraceID),
			structuredlog.F("volume_id", CanonicalVolumeID(uint64(volume.ID))),
			structuredlog.F("volume_handle", openState.handle),
			structuredlog.F("attachment_id", reqCtx.AttachmentID),
			structuredlog.F("generation", reqCtx.Generation),
			structuredlog.F("idempotency_key", reqCtx.IdempotencyKey),
			structuredlog.F("offset_bytes", offsetBytes),
			structuredlog.F("length_bytes", lengthBytes),
			structuredlog.F("load_index", loadMeta.Index),
			structuredlog.F("load_phase", loadMeta.Phase),
			structuredlog.F("duration_ms", time.Since(start).Milliseconds()),
			structuredlog.F("ensure_open_duration_ms", ensureDuration.Milliseconds()),
			structuredlog.F("sbs_write_rpc_duration_ms", rpcDuration.Milliseconds()),
		)
		return err
	}
	structuredlog.Info("gateway.sbs", "sbs_write_completed",
		structuredlog.F("request_id", reqCtx.RequestID),
		structuredlog.F("trace_id", reqCtx.TraceID),
		structuredlog.F("volume_id", CanonicalVolumeID(uint64(volume.ID))),
		structuredlog.F("volume_handle", openState.handle),
		structuredlog.F("attachment_id", reqCtx.AttachmentID),
		structuredlog.F("generation", reqCtx.Generation),
		structuredlog.F("idempotency_key", reqCtx.IdempotencyKey),
		structuredlog.F("offset_bytes", offsetBytes),
		structuredlog.F("length_bytes", lengthBytes),
		structuredlog.F("load_index", loadMeta.Index),
		structuredlog.F("load_phase", loadMeta.Phase),
		structuredlog.F("duration_ms", time.Since(start).Milliseconds()),
		structuredlog.F("ensure_open_duration_ms", ensureDuration.Milliseconds()),
		structuredlog.F("sbs_write_rpc_duration_ms", rpcDuration.Milliseconds()),
	)
	return err
}

func (r *sbsDataRepository) WriteCloneAt(ctx context.Context, volume VolumeSpec, cloneID string, offsetBytes, lengthBytes uint64, data []byte) error {
	cloneClient, ok := r.client.(cloneSBSClient)
	if !ok {
		return ErrNotSupported
	}
	start := time.Now()
	loadMeta := LoadMetadataFromContext(ctx)
	ensureStart := time.Now()
	openState, reqCtx, err := r.ensureOpen(ctx, volume)
	ensureDuration := time.Since(ensureStart)
	if err != nil {
		structuredlog.Error("gateway.sbs", "sbs_clone_write_prepare_failed", err,
			structuredlog.F("volume_id", CanonicalVolumeID(uint64(volume.ID))),
			structuredlog.F("clone_id", cloneID),
			structuredlog.F("offset_bytes", offsetBytes),
			structuredlog.F("length_bytes", lengthBytes),
			structuredlog.F("load_index", loadMeta.Index),
			structuredlog.F("load_phase", loadMeta.Phase),
			structuredlog.F("duration_ms", time.Since(start).Milliseconds()),
			structuredlog.F("ensure_open_duration_ms", ensureDuration.Milliseconds()),
		)
		return err
	}
	var rpcDuration time.Duration
	rpcStart := time.Now()
	_, err = cloneClient.WriteClone(ctx, cloneID, &WriteRequest{
		VolumeID:     CanonicalVolumeID(uint64(volume.ID)),
		VolumeHandle: openState.handle,
		OffsetBytes:  offsetBytes,
		LengthBytes:  lengthBytes,
		Data:         append([]byte(nil), data...),
		Context:      reqCtx,
	})
	rpcDuration += time.Since(rpcStart)
	if err != nil && isRetryableReopenError(err) {
		r.recordRetry("clone_write_reopen_retry")
		structuredlog.Info("gateway.sbs", "sbs_clone_write_retrying_after_reopen_error",
			structuredlog.F("request_id", reqCtx.RequestID),
			structuredlog.F("trace_id", reqCtx.TraceID),
			structuredlog.F("volume_id", CanonicalVolumeID(uint64(volume.ID))),
			structuredlog.F("clone_id", cloneID),
			structuredlog.F("attachment_id", reqCtx.AttachmentID),
			structuredlog.F("generation", reqCtx.Generation),
			structuredlog.F("idempotency_key", reqCtx.IdempotencyKey),
		)
		r.evictHandle(uint64(volume.ID))
		openState, reqCtx, err = r.ensureOpen(ctx, volume)
		if err != nil {
			return err
		}
		rpcStart = time.Now()
		_, err = cloneClient.WriteClone(ctx, cloneID, &WriteRequest{
			VolumeID:     CanonicalVolumeID(uint64(volume.ID)),
			VolumeHandle: openState.handle,
			OffsetBytes:  offsetBytes,
			LengthBytes:  lengthBytes,
			Data:         append([]byte(nil), data...),
			Context:      reqCtx,
		})
		rpcDuration += time.Since(rpcStart)
	}
	if err != nil {
		structuredlog.Error("gateway.sbs", "sbs_clone_write_failed", err,
			structuredlog.F("request_id", reqCtx.RequestID),
			structuredlog.F("trace_id", reqCtx.TraceID),
			structuredlog.F("volume_id", CanonicalVolumeID(uint64(volume.ID))),
			structuredlog.F("clone_id", cloneID),
			structuredlog.F("volume_handle", openState.handle),
			structuredlog.F("attachment_id", reqCtx.AttachmentID),
			structuredlog.F("generation", reqCtx.Generation),
			structuredlog.F("idempotency_key", reqCtx.IdempotencyKey),
			structuredlog.F("offset_bytes", offsetBytes),
			structuredlog.F("length_bytes", lengthBytes),
			structuredlog.F("load_index", loadMeta.Index),
			structuredlog.F("load_phase", loadMeta.Phase),
			structuredlog.F("duration_ms", time.Since(start).Milliseconds()),
			structuredlog.F("ensure_open_duration_ms", ensureDuration.Milliseconds()),
			structuredlog.F("sbs_write_rpc_duration_ms", rpcDuration.Milliseconds()),
		)
		return err
	}
	structuredlog.Info("gateway.sbs", "sbs_clone_write_completed",
		structuredlog.F("request_id", reqCtx.RequestID),
		structuredlog.F("trace_id", reqCtx.TraceID),
		structuredlog.F("volume_id", CanonicalVolumeID(uint64(volume.ID))),
		structuredlog.F("clone_id", cloneID),
		structuredlog.F("volume_handle", openState.handle),
		structuredlog.F("attachment_id", reqCtx.AttachmentID),
		structuredlog.F("generation", reqCtx.Generation),
		structuredlog.F("idempotency_key", reqCtx.IdempotencyKey),
		structuredlog.F("offset_bytes", offsetBytes),
		structuredlog.F("length_bytes", lengthBytes),
		structuredlog.F("load_index", loadMeta.Index),
		structuredlog.F("load_phase", loadMeta.Phase),
		structuredlog.F("duration_ms", time.Since(start).Milliseconds()),
		structuredlog.F("ensure_open_duration_ms", ensureDuration.Milliseconds()),
		structuredlog.F("sbs_write_rpc_duration_ms", rpcDuration.Milliseconds()),
	)
	return err
}

func (r *sbsDataRepository) FlushVolume(ctx context.Context, volume VolumeSpec) error {
	openState, reqCtx, err := r.ensureOpen(ctx, volume)
	if err != nil {
		return err
	}
	_, err = r.client.Flush(ctx, &FlushRequest{
		VolumeID:     CanonicalVolumeID(uint64(volume.ID)),
		VolumeHandle: openState.handle,
		Context:      reqCtx,
	})
	if err != nil && isRetryableReopenError(err) {
		r.recordRetry("flush_reopen_retry")
		structuredlog.Info("gateway.sbs", "sbs_flush_retrying_after_reopen_error",
			structuredlog.F("request_id", reqCtx.RequestID),
			structuredlog.F("trace_id", reqCtx.TraceID),
			structuredlog.F("volume_id", CanonicalVolumeID(uint64(volume.ID))),
			structuredlog.F("attachment_id", reqCtx.AttachmentID),
			structuredlog.F("generation", reqCtx.Generation),
		)
		r.evictHandle(uint64(volume.ID))
		openState, reqCtx, err = r.ensureOpen(ctx, volume)
		if err != nil {
			return err
		}
		_, err = r.client.Flush(ctx, &FlushRequest{
			VolumeID:     CanonicalVolumeID(uint64(volume.ID)),
			VolumeHandle: openState.handle,
			Context:      reqCtx,
		})
	}
	if err != nil {
		structuredlog.Error("gateway.sbs", "sbs_flush_failed", err,
			structuredlog.F("request_id", reqCtx.RequestID),
			structuredlog.F("trace_id", reqCtx.TraceID),
			structuredlog.F("volume_id", CanonicalVolumeID(uint64(volume.ID))),
			structuredlog.F("volume_handle", openState.handle),
			structuredlog.F("attachment_id", reqCtx.AttachmentID),
			structuredlog.F("generation", reqCtx.Generation),
		)
		return err
	}
	structuredlog.Info("gateway.sbs", "sbs_flush_completed",
		structuredlog.F("request_id", reqCtx.RequestID),
		structuredlog.F("trace_id", reqCtx.TraceID),
		structuredlog.F("volume_id", CanonicalVolumeID(uint64(volume.ID))),
		structuredlog.F("volume_handle", openState.handle),
		structuredlog.F("attachment_id", reqCtx.AttachmentID),
		structuredlog.F("generation", reqCtx.Generation),
	)
	return err
}

func (r *sbsDataRepository) DiscardAt(ctx context.Context, volume VolumeSpec, offsetBytes, lengthBytes uint64) error {
	if !DiscardRangeAligned(volume, offsetBytes, lengthBytes) {
		return NewDiscardAlignmentError(volume, offsetBytes, lengthBytes)
	}
	_, reqCtx, err := r.ensureOpen(ctx, volume)
	if err != nil {
		return err
	}
	_, err = r.client.Discard(ctx, &DiscardRequest{
		VolumeID:    CanonicalVolumeID(uint64(volume.ID)),
		OffsetBytes: offsetBytes,
		LengthBytes: lengthBytes,
		Context:     reqCtx,
	})
	if err != nil && isRetryableReopenError(err) {
		r.recordRetry("discard_reopen_retry")
		structuredlog.Info("gateway.sbs", "sbs_discard_retrying_after_reopen_error",
			structuredlog.F("request_id", reqCtx.RequestID),
			structuredlog.F("trace_id", reqCtx.TraceID),
			structuredlog.F("volume_id", CanonicalVolumeID(uint64(volume.ID))),
			structuredlog.F("attachment_id", reqCtx.AttachmentID),
			structuredlog.F("generation", reqCtx.Generation),
			structuredlog.F("idempotency_key", reqCtx.IdempotencyKey),
		)
		r.evictHandle(uint64(volume.ID))
		_, reqCtx, err = r.ensureOpen(ctx, volume)
		if err != nil {
			return err
		}
		_, err = r.client.Discard(ctx, &DiscardRequest{
			VolumeID:    CanonicalVolumeID(uint64(volume.ID)),
			OffsetBytes: offsetBytes,
			LengthBytes: lengthBytes,
			Context:     reqCtx,
		})
	}
	if err != nil {
		structuredlog.Error("gateway.sbs", "sbs_discard_failed", err,
			structuredlog.F("request_id", reqCtx.RequestID),
			structuredlog.F("trace_id", reqCtx.TraceID),
			structuredlog.F("volume_id", CanonicalVolumeID(uint64(volume.ID))),
			structuredlog.F("attachment_id", reqCtx.AttachmentID),
			structuredlog.F("generation", reqCtx.Generation),
			structuredlog.F("idempotency_key", reqCtx.IdempotencyKey),
			structuredlog.F("offset_bytes", offsetBytes),
			structuredlog.F("length_bytes", lengthBytes),
		)
		return err
	}
	structuredlog.Info("gateway.sbs", "sbs_discard_completed",
		structuredlog.F("request_id", reqCtx.RequestID),
		structuredlog.F("trace_id", reqCtx.TraceID),
		structuredlog.F("volume_id", CanonicalVolumeID(uint64(volume.ID))),
		structuredlog.F("attachment_id", reqCtx.AttachmentID),
		structuredlog.F("generation", reqCtx.Generation),
		structuredlog.F("idempotency_key", reqCtx.IdempotencyKey),
		structuredlog.F("offset_bytes", offsetBytes),
		structuredlog.F("length_bytes", lengthBytes),
	)
	return err
}

func (r *sbsDataRepository) DiscardObservationFor(volume VolumeSpec, offsetBytes, lengthBytes uint64) DiscardObservation {
	if provider, ok := r.client.(DiscardObservationProvider); ok {
		return provider.DiscardObservationFor(volume, offsetBytes, lengthBytes)
	}
	if !DiscardRangeAligned(volume, offsetBytes, lengthBytes) {
		return NewDiscardAlignmentZeroFallbackObservation(volume, offsetBytes, lengthBytes)
	}
	return NewDiscardZeroFallbackObservation(volume, offsetBytes, lengthBytes)
}

func (r *sbsDataRepository) ZeroAt(ctx context.Context, volume VolumeSpec, offsetBytes, lengthBytes uint64) error {
	_, reqCtx, err := r.ensureOpen(ctx, volume)
	if err != nil {
		return err
	}
	_, err = r.client.Zero(ctx, &ZeroRequest{
		VolumeID:    CanonicalVolumeID(uint64(volume.ID)),
		OffsetBytes: offsetBytes,
		LengthBytes: lengthBytes,
		Context:     reqCtx,
	})
	if err != nil && isRetryableReopenError(err) {
		r.recordRetry("zero_reopen_retry")
		structuredlog.Info("gateway.sbs", "sbs_zero_retrying_after_reopen_error",
			structuredlog.F("request_id", reqCtx.RequestID),
			structuredlog.F("trace_id", reqCtx.TraceID),
			structuredlog.F("volume_id", CanonicalVolumeID(uint64(volume.ID))),
			structuredlog.F("attachment_id", reqCtx.AttachmentID),
			structuredlog.F("generation", reqCtx.Generation),
			structuredlog.F("idempotency_key", reqCtx.IdempotencyKey),
		)
		r.evictHandle(uint64(volume.ID))
		_, reqCtx, err = r.ensureOpen(ctx, volume)
		if err != nil {
			return err
		}
		_, err = r.client.Zero(ctx, &ZeroRequest{
			VolumeID:    CanonicalVolumeID(uint64(volume.ID)),
			OffsetBytes: offsetBytes,
			LengthBytes: lengthBytes,
			Context:     reqCtx,
		})
	}
	if err != nil {
		structuredlog.Error("gateway.sbs", "sbs_zero_failed", err,
			structuredlog.F("request_id", reqCtx.RequestID),
			structuredlog.F("trace_id", reqCtx.TraceID),
			structuredlog.F("volume_id", CanonicalVolumeID(uint64(volume.ID))),
			structuredlog.F("attachment_id", reqCtx.AttachmentID),
			structuredlog.F("generation", reqCtx.Generation),
			structuredlog.F("idempotency_key", reqCtx.IdempotencyKey),
			structuredlog.F("offset_bytes", offsetBytes),
			structuredlog.F("length_bytes", lengthBytes),
		)
		return err
	}
	structuredlog.Info("gateway.sbs", "sbs_zero_completed",
		structuredlog.F("request_id", reqCtx.RequestID),
		structuredlog.F("trace_id", reqCtx.TraceID),
		structuredlog.F("volume_id", CanonicalVolumeID(uint64(volume.ID))),
		structuredlog.F("attachment_id", reqCtx.AttachmentID),
		structuredlog.F("generation", reqCtx.Generation),
		structuredlog.F("idempotency_key", reqCtx.IdempotencyKey),
		structuredlog.F("offset_bytes", offsetBytes),
		structuredlog.F("length_bytes", lengthBytes),
	)
	return err
}

func (r *sbsDataRepository) cachedOpenState(volumeID uint64, now time.Time) (openVolumeState, bool) {
	if r.openReuseTTL <= 0 {
		return openVolumeState{}, false
	}
	r.mu.RLock()
	current, ok := r.handles[volumeID]
	r.mu.RUnlock()
	if !ok || current.handle == "" || current.attachmentID == "" || current.generation == 0 {
		return openVolumeState{}, false
	}
	if current.validUntil.IsZero() || !now.Before(current.validUntil) {
		return openVolumeState{}, false
	}
	return current, true
}

func (r *sbsDataRepository) markOpenValidated(current *openVolumeState, now time.Time) {
	current.validatedAt = now
	if r.openReuseTTL <= 0 {
		current.validUntil = time.Time{}
		return
	}
	current.validUntil = now.Add(r.openReuseTTL)
}

func (r *sbsDataRepository) ensureOpen(ctx context.Context, volume VolumeSpec) (openVolumeState, SBSRequestContext, error) {
	volumeID := uint64(volume.ID)
	if current, ok := r.cachedOpenState(volumeID, time.Now()); ok {
		attachment := AttachmentRecord{
			AttachmentID: current.attachmentID,
			HostID:       current.hostID,
			Generation:   current.generation,
		}
		reqCtx := r.newRequestContext(attachment, true)
		structuredlog.Info("gateway.sbs", "sbs_open_reused",
			structuredlog.F("volume_id", CanonicalVolumeID(volumeID)),
			structuredlog.F("volume_handle", current.handle),
			structuredlog.F("attachment_id", current.attachmentID),
			structuredlog.F("generation", current.generation),
			structuredlog.F("request_id", reqCtx.RequestID),
			structuredlog.F("trace_id", reqCtx.TraceID),
			structuredlog.F("open_reuse_cache_hit", true),
			structuredlog.F("open_reuse_ttl_ms", r.openReuseTTL.Milliseconds()),
		)
		return current, reqCtx, nil
	}

	status, err := r.meta.GetVolumeStatus(ctx, uint64(volume.ID))
	if err != nil {
		return openVolumeState{}, SBSRequestContext{}, err
	}
	if status.HandoffRequired && !gatewayAllowedDuringHandoff(status, r.gatewayID) {
		return openVolumeState{}, SBSRequestContext{}, fmt.Errorf("%w: volume=%s fencing_epoch=%d handoff_stage=%s handoff_reason=%s", ErrWriterFenced, CanonicalVolumeID(uint64(volume.ID)), status.WriterFencingEpoch, status.HandoffStage, status.HandoffReason)
	}

	attachment, err := r.meta.GetAttachment(ctx, uint64(volume.ID))
	if err != nil {
		return openVolumeState{}, SBSRequestContext{}, err
	}
	if attachment.AttachmentID == "" || attachment.Generation == 0 {
		return openVolumeState{}, SBSRequestContext{}, fmt.Errorf("sbs repository requires attached volume")
	}

	reqCtx := r.newRequestContext(attachment, true)

	r.mu.Lock()
	defer r.mu.Unlock()

	current, ok := r.handles[uint64(volume.ID)]
	if ok && current.attachmentID == attachment.AttachmentID && current.generation == attachment.Generation {
		current.hostID = attachment.HostID
		r.markOpenValidated(&current, time.Now())
		r.handles[uint64(volume.ID)] = current
		structuredlog.Info("gateway.sbs", "sbs_open_reused",
			structuredlog.F("volume_id", CanonicalVolumeID(uint64(volume.ID))),
			structuredlog.F("volume_handle", current.handle),
			structuredlog.F("attachment_id", attachment.AttachmentID),
			structuredlog.F("generation", attachment.Generation),
			structuredlog.F("request_id", reqCtx.RequestID),
			structuredlog.F("trace_id", reqCtx.TraceID),
			structuredlog.F("open_reuse_cache_hit", false),
			structuredlog.F("open_reuse_ttl_ms", r.openReuseTTL.Milliseconds()),
		)
		return current, reqCtx, nil
	}

	if ok {
		_ = r.closeHandleLocked(ctx, uint64(volume.ID), current, attachment.HostID, "sbs_close_stale")
	}

	resp, err := r.client.OpenVolume(ctx, &OpenVolumeRequest{
		VolumeID:   CanonicalVolumeID(uint64(volume.ID)),
		AccessMode: SBSAccessModeExclusiveWriter,
		Context:    reqCtx,
	})
	if err != nil {
		if isRetryableUnavailable(err) {
			r.recordRetry("open_unavailable_retry")
			structuredlog.Info("gateway.sbs", "sbs_open_retrying_after_unavailable",
				structuredlog.F("volume_id", CanonicalVolumeID(uint64(volume.ID))),
				structuredlog.F("attachment_id", attachment.AttachmentID),
				structuredlog.F("generation", attachment.Generation),
				structuredlog.F("request_id", reqCtx.RequestID),
				structuredlog.F("trace_id", reqCtx.TraceID),
			)
			delete(r.handles, uint64(volume.ID))
			resp, err = r.client.OpenVolume(ctx, &OpenVolumeRequest{
				VolumeID:   CanonicalVolumeID(uint64(volume.ID)),
				AccessMode: SBSAccessModeExclusiveWriter,
				Context:    reqCtx,
			})
		}
	}
	if err != nil {
		structuredlog.Error("gateway.sbs", "sbs_open_failed", err,
			structuredlog.F("volume_id", CanonicalVolumeID(uint64(volume.ID))),
			structuredlog.F("attachment_id", attachment.AttachmentID),
			structuredlog.F("generation", attachment.Generation),
			structuredlog.F("request_id", reqCtx.RequestID),
			structuredlog.F("trace_id", reqCtx.TraceID),
		)
		return openVolumeState{}, SBSRequestContext{}, err
	}
	if err := CheckSBSMajorVersionCompatibility(r.version, resp.ServerVersion); err != nil {
		structuredlog.Error("gateway.sbs", "sbs_version_incompatible", err,
			structuredlog.F("volume_id", CanonicalVolumeID(uint64(volume.ID))),
			structuredlog.F("gateway_version", r.version),
			structuredlog.F("server_version", resp.ServerVersion),
			structuredlog.F("attachment_id", attachment.AttachmentID),
			structuredlog.F("generation", attachment.Generation),
		)
		return openVolumeState{}, SBSRequestContext{}, err
	}
	current = openVolumeState{
		handle:       resp.VolumeHandle,
		attachmentID: attachment.AttachmentID,
		hostID:       attachment.HostID,
		generation:   attachment.Generation,
	}
	r.markOpenValidated(&current, time.Now())
	r.handles[uint64(volume.ID)] = current
	structuredlog.Info("gateway.sbs", "sbs_open_completed",
		structuredlog.F("volume_id", CanonicalVolumeID(uint64(volume.ID))),
		structuredlog.F("volume_handle", current.handle),
		structuredlog.F("attachment_id", attachment.AttachmentID),
		structuredlog.F("generation", attachment.Generation),
		structuredlog.F("request_id", reqCtx.RequestID),
		structuredlog.F("trace_id", reqCtx.TraceID),
	)
	return current, reqCtx, nil
}

func (r *sbsDataRepository) closeHandle(ctx context.Context, volumeID uint64, current openVolumeState, hostID, eventPrefix string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.closeHandleLocked(ctx, volumeID, current, hostID, eventPrefix)
}

func (r *sbsDataRepository) closeHandleLocked(ctx context.Context, volumeID uint64, current openVolumeState, hostID, eventPrefix string) error {
	closeReq := &CloseVolumeRequest{
		VolumeID:     CanonicalVolumeID(volumeID),
		VolumeHandle: current.handle,
		Context: SBSRequestContext{
			RequestID:    fmt.Sprintf("sbs-req-%d-close", atomic.AddUint64(&r.nextReq, 1)),
			GatewayID:    r.gatewayID,
			HostID:       hostID,
			SessionID:    current.attachmentID,
			AttachmentID: current.attachmentID,
			Generation:   current.generation,
		},
	}
	_, err := r.client.CloseVolume(ctx, closeReq)
	if err != nil {
		if isIgnorableCloseError(err) {
			delete(r.handles, volumeID)
			structuredlog.Info("gateway.sbs", eventPrefix+"_already_closed",
				structuredlog.F("volume_id", CanonicalVolumeID(volumeID)),
				structuredlog.F("volume_handle", current.handle),
				structuredlog.F("attachment_id", current.attachmentID),
				structuredlog.F("generation", current.generation),
				structuredlog.F("request_id", closeReq.Context.RequestID),
			)
			return nil
		}
		if isDeferredDetachCloseError(err) {
			delete(r.handles, volumeID)
			structuredlog.Info("gateway.sbs", eventPrefix+"_deferred",
				structuredlog.F("volume_id", CanonicalVolumeID(volumeID)),
				structuredlog.F("volume_handle", current.handle),
				structuredlog.F("attachment_id", current.attachmentID),
				structuredlog.F("generation", current.generation),
				structuredlog.F("request_id", closeReq.Context.RequestID),
				structuredlog.F("error", err.Error()),
			)
			return err
		}
		structuredlog.Error("gateway.sbs", eventPrefix+"_failed", err,
			structuredlog.F("volume_id", CanonicalVolumeID(volumeID)),
			structuredlog.F("volume_handle", current.handle),
			structuredlog.F("attachment_id", current.attachmentID),
			structuredlog.F("generation", current.generation),
			structuredlog.F("request_id", closeReq.Context.RequestID),
		)
		return err
	}
	delete(r.handles, volumeID)
	structuredlog.Info("gateway.sbs", eventPrefix+"_completed",
		structuredlog.F("volume_id", CanonicalVolumeID(volumeID)),
		structuredlog.F("volume_handle", current.handle),
		structuredlog.F("attachment_id", current.attachmentID),
		structuredlog.F("generation", current.generation),
		structuredlog.F("request_id", closeReq.Context.RequestID),
	)
	return nil
}

func isRetryableUnavailable(err error) bool {
	var sbsErr *SBSError
	return errors.As(err, &sbsErr) && sbsErr.Code == SBSErrorCodeUnavailable && sbsErr.Retryable
}

func isRetryableReopenError(err error) bool {
	var sbsErr *SBSError
	if !errors.As(err, &sbsErr) {
		return false
	}
	return sbsErr.Code == SBSErrorCodeAttachmentMismatch || (sbsErr.Code == SBSErrorCodeUnavailable && sbsErr.Retryable)
}

func isIgnorableCloseError(err error) bool {
	var sbsErr *SBSError
	if !errors.As(err, &sbsErr) {
		return false
	}
	return sbsErr.Code == SBSErrorCodeAttachmentMismatch || sbsErr.Code == SBSErrorCodeNotFound
}

func (r *sbsDataRepository) evictHandle(volumeID uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.handles, volumeID)
}

func (r *sbsDataRepository) recordRetry(kind string) {
	r.retryMu.Lock()
	defer r.retryMu.Unlock()
	r.retryCounters[kind]++
}

func (r *sbsDataRepository) RetryMetricsSnapshot() map[string]uint64 {
	r.retryMu.Lock()
	defer r.retryMu.Unlock()
	out := make(map[string]uint64, len(r.retryCounters))
	for key, value := range r.retryCounters {
		out[key] = value
	}
	return out
}

func (r *sbsDataRepository) newRequestContext(attachment AttachmentRecord, mutating bool) SBSRequestContext {
	reqID := atomic.AddUint64(&r.nextReq, 1)
	ctx := SBSRequestContext{
		RequestID:    fmt.Sprintf("sbs-req-%d", reqID),
		GatewayID:    r.gatewayID,
		HostID:       attachment.HostID,
		AttachmentID: attachment.AttachmentID,
		Generation:   attachment.Generation,
		SessionID:    attachment.AttachmentID,
		TraceID:      fmt.Sprintf("trace-%d", reqID),
	}
	if mutating {
		ctx.IdempotencyKey = fmt.Sprintf("sbs-idem-%s-%s-%d", r.sessionNonce, attachment.AttachmentID, reqID)
	}
	return ctx
}
