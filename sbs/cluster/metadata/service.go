package metadata

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"time"
)

type Service struct {
	placements       extentPlacementRepo
	pageReader       allocationPageReader
	pageLister       allocationPageLister
	cloneDeltaWriter cloneDeltaAllocationPageWriter
}

const resolveAllocationPagesListThreshold = 8

type extentPlacementRepo interface {
	ListExtentMappings(ctx context.Context, volumeID string) ([]ExtentMappingRecord, error)
	ListReplicaSets(ctx context.Context, volumeID string) ([]ReplicaSetState, error)
	ListNodeMemberships(ctx context.Context) ([]NodeMembershipRecord, error)
}

type allocationPageReader interface {
	// GetCompatibleAllocationPage is a legacy adapter reader. New Phase J
	// snapshot/clone code should resolve substrate AllocationEntries directly
	// rather than treating this compatibility view as the primary metadata
	// model.
	GetCompatibleAllocationPage(ctx context.Context, volumeID string, pageNo uint64, pageBytes, chunkSizeBytes uint32) (AllocationPageRecord, error)
}

type snapshotAllocationPageReader interface {
	GetSnapshotAllocationPage(ctx context.Context, snapshotID string, pageNo uint64) (AllocationPageRecord, error)
	GetSnapshotRecord(ctx context.Context, snapshotID string) (SnapshotRecord, error)
}

type snapshotRecordLister interface {
	ListSnapshotRecords(ctx context.Context, sourceVolumeID string, includeDeleted bool) ([]SnapshotRecord, error)
}

var ErrSnapshotRecordListerNotConfigured = errors.New("snapshot record lister is not configured")

type cloneAllocationPageReader interface {
	snapshotAllocationPageReader
	GetCloneRecord(ctx context.Context, cloneID string) (CloneRecord, error)
	GetCloneDeltaAllocationPage(ctx context.Context, cloneID string, pageNo uint64) (AllocationPageRecord, error)
}

type cloneDeltaAllocationPageWriter interface {
	PutCloneDeltaAllocationPages(ctx context.Context, cloneID string, pages []AllocationPageRecord) error
}

type allocationPageLister interface {
	// ListCompatibleAllocationPages is a legacy adapter lister. It may synthesize
	// allocation pages from older extent mappings for compatibility.
	ListCompatibleAllocationPages(ctx context.Context, volumeID string, pageBytes, chunkSizeBytes uint32) ([]AllocationPageRecord, error)
}

type resolverRepo interface {
	extentPlacementRepo
	allocationPageReader
	allocationPageLister
}

func NewService(repo resolverRepo) *Service {
	return NewServiceWithDependencies(repo, repo, repo)
}

func NewServiceWithDependencies(placements extentPlacementRepo, pageReader allocationPageReader, pageLister allocationPageLister) *Service {
	svc := &Service{
		placements: placements,
		pageReader: pageReader,
		pageLister: pageLister,
	}
	if writer, ok := pageReader.(cloneDeltaAllocationPageWriter); ok {
		svc.cloneDeltaWriter = writer
	}
	return svc
}

type ResolvedExtentPlacement struct {
	ExtentMapping ExtentMappingRecord
	ReplicaSet    ReplicaSetState
	Nodes         map[string]NodeMembershipRecord
}

type ResolveExtentPlacementsStats struct {
	MappingLookupDuration    time.Duration
	ReplicaSetLookupDuration time.Duration
	NodeLookupDuration       time.Duration
	IndexBuildDuration       time.Duration
	RangeFilterDuration      time.Duration
	MappingCountTotal        int
	MappingCountSelected     int
	ReplicaSetCount          int
	NodeCount                int
}

type ResolvedAllocationPage struct {
	Page            AllocationPageRecord
	RangeStartChunk uint64
	RangeEndChunk   uint64
	CoversWholePage bool
}

type ReconciledMutationScope struct {
	Operation         MutationOperationRecord
	AffectedExtentIDs []uint64
	AffectedPageNos   []uint64
	Changed           bool
}

func (s *Service) ResolveExtentPlacements(ctx context.Context, volumeID string, offsetBytes, lengthBytes uint64) ([]ResolvedExtentPlacement, error) {
	placements, _, err := s.ResolveExtentPlacementsDetailed(ctx, volumeID, offsetBytes, lengthBytes)
	return placements, err
}

func (s *Service) ResolveExtentPlacementsDetailed(ctx context.Context, volumeID string, offsetBytes, lengthBytes uint64) ([]ResolvedExtentPlacement, ResolveExtentPlacementsStats, error) {
	var stats ResolveExtentPlacementsStats
	if lengthBytes == 0 {
		return nil, stats, nil
	}
	if s.placements == nil {
		return nil, stats, fmt.Errorf("extent placement resolver is not configured")
	}
	phaseStart := time.Now()
	mappings, err := s.placements.ListExtentMappings(ctx, volumeID)
	stats.MappingLookupDuration = time.Since(phaseStart)
	stats.MappingCountTotal = len(mappings)
	if err != nil {
		return nil, stats, err
	}
	phaseStart = time.Now()
	replicaSets, err := s.placements.ListReplicaSets(ctx, volumeID)
	stats.ReplicaSetLookupDuration = time.Since(phaseStart)
	stats.ReplicaSetCount = len(replicaSets)
	if err != nil {
		return nil, stats, err
	}
	phaseStart = time.Now()
	nodes, err := s.placements.ListNodeMemberships(ctx)
	stats.NodeLookupDuration = time.Since(phaseStart)
	stats.NodeCount = len(nodes)
	if err != nil {
		return nil, stats, err
	}
	phaseStart = time.Now()
	byNodeID := make(map[string]NodeMembershipRecord, len(nodes))
	for _, node := range nodes {
		byNodeID[node.NodeID] = node
	}
	byPlacement := make(map[string]ReplicaSetState, len(replicaSets))
	for _, replicaSet := range replicaSets {
		byPlacement[replicaSet.PlacementRef] = replicaSet
	}
	stats.IndexBuildDuration = time.Since(phaseStart)

	phaseStart = time.Now()
	rangeEnd := offsetBytes + lengthBytes
	out := make([]ResolvedExtentPlacement, 0)
	for _, mapping := range mappings {
		extentStart := mapping.LogicalOffset
		extentEnd := mapping.LogicalOffset + mapping.LengthBytes
		if extentEnd <= offsetBytes || extentStart >= rangeEnd {
			continue
		}
		replicaSet, ok := byPlacement[mapping.PlacementRef]
		if !ok {
			stats.RangeFilterDuration = time.Since(phaseStart)
			stats.MappingCountSelected = len(out)
			return nil, stats, fmt.Errorf("placement_ref %q has no replica set", mapping.PlacementRef)
		}
		out = append(out, ResolvedExtentPlacement{
			ExtentMapping: mapping,
			ReplicaSet:    replicaSet,
			Nodes:         byNodeID,
		})
	}
	stats.RangeFilterDuration = time.Since(phaseStart)
	stats.MappingCountSelected = len(out)
	return out, stats, nil
}

func (s *Service) ResolveAllocationPages(ctx context.Context, volumeID string, offsetBytes, lengthBytes uint64, pageBytes, chunkSizeBytes uint32) ([]ResolvedAllocationPage, error) {
	if lengthBytes == 0 {
		return nil, nil
	}
	if pageBytes == 0 || chunkSizeBytes == 0 || pageBytes%chunkSizeBytes != 0 {
		return nil, fmt.Errorf("invalid allocation geometry: page_bytes=%d chunk_size_bytes=%d", pageBytes, chunkSizeBytes)
	}
	if s.pageReader == nil {
		return nil, fmt.Errorf("allocation page reader is not configured")
	}

	chunkSize := uint64(chunkSizeBytes)
	chunksPerPage := uint64(pageBytes / chunkSizeBytes)
	startChunk := offsetBytes / chunkSize
	endChunk := (offsetBytes + lengthBytes - 1) / chunkSize
	startPage := startChunk / chunksPerPage
	endPage := endChunk / chunksPerPage
	pageCount := endPage - startPage + 1

	if pageCount >= resolveAllocationPagesListThreshold && s.pageLister != nil {
		out, ok, err := s.resolveAllocationPagesFromList(ctx, volumeID, pageBytes, chunkSizeBytes, startChunk, endChunk, startPage, endPage, chunksPerPage)
		if ok || err != nil {
			return out, err
		}
	}

	out := make([]ResolvedAllocationPage, 0, endPage-startPage+1)
	for pageNo := startPage; pageNo <= endPage; pageNo++ {
		page, err := s.pageReader.GetCompatibleAllocationPage(ctx, volumeID, pageNo, pageBytes, chunkSizeBytes)
		if err != nil {
			return nil, err
		}
		out = appendResolvedAllocationPage(out, page, startChunk, endChunk, chunksPerPage)
	}
	return out, nil
}

func (s *Service) resolveAllocationPagesFromList(ctx context.Context, volumeID string, pageBytes, chunkSizeBytes uint32, startChunk, endChunk, startPage, endPage, chunksPerPage uint64) ([]ResolvedAllocationPage, bool, error) {
	pages, err := s.pageLister.ListCompatibleAllocationPages(ctx, volumeID, pageBytes, chunkSizeBytes)
	if err != nil {
		return nil, true, err
	}
	if len(pages) == 0 {
		return nil, false, nil
	}
	byPageNo := make(map[uint64]AllocationPageRecord, len(pages))
	for _, page := range pages {
		byPageNo[page.PageNo] = page
	}
	out := make([]ResolvedAllocationPage, 0, endPage-startPage+1)
	for pageNo := startPage; pageNo <= endPage; pageNo++ {
		page, ok := byPageNo[pageNo]
		if !ok {
			page = zeroAllocationPage(volumeID, pageNo, pageBytes, chunkSizeBytes)
		}
		out = appendResolvedAllocationPage(out, page, startChunk, endChunk, chunksPerPage)
	}
	return out, true, nil
}

func appendResolvedAllocationPage(out []ResolvedAllocationPage, page AllocationPageRecord, startChunk, endChunk, chunksPerPage uint64) []ResolvedAllocationPage {
	pageStartChunk := page.PageNo * chunksPerPage
	pageEndChunk := pageStartChunk + chunksPerPage
	rangeStartChunk := maxUint64(startChunk, pageStartChunk)
	rangeEndChunk := minUint64(endChunk+1, pageEndChunk)
	return append(out, ResolvedAllocationPage{
		Page:            page,
		RangeStartChunk: rangeStartChunk,
		RangeEndChunk:   rangeEndChunk,
		CoversWholePage: rangeStartChunk == pageStartChunk && rangeEndChunk == pageEndChunk,
	})
}

func (s *Service) ResolveSnapshotAllocationPages(ctx context.Context, snapshotID string, offsetBytes, lengthBytes uint64, pageBytes, chunkSizeBytes uint32) ([]ResolvedAllocationPage, error) {
	if lengthBytes == 0 {
		return nil, nil
	}
	if pageBytes == 0 || chunkSizeBytes == 0 || pageBytes%chunkSizeBytes != 0 {
		return nil, fmt.Errorf("invalid allocation geometry: page_bytes=%d chunk_size_bytes=%d", pageBytes, chunkSizeBytes)
	}
	reader, ok := s.pageReader.(snapshotAllocationPageReader)
	if !ok || reader == nil {
		return nil, fmt.Errorf("snapshot allocation page reader is not configured")
	}

	return resolveCapturedAllocationPages(ctx, reader, snapshotID, "", offsetBytes, lengthBytes, pageBytes, chunkSizeBytes)
}

func (s *Service) ListSnapshotRecords(ctx context.Context, sourceVolumeID string, includeDeleted bool) ([]SnapshotRecord, error) {
	lister, ok := s.pageReader.(snapshotRecordLister)
	if !ok || lister == nil {
		return nil, ErrSnapshotRecordListerNotConfigured
	}
	return lister.ListSnapshotRecords(ctx, sourceVolumeID, includeDeleted)
}

func (s *Service) ResolveCloneAllocationPages(ctx context.Context, cloneID string, offsetBytes, lengthBytes uint64, pageBytes, chunkSizeBytes uint32) ([]ResolvedAllocationPage, error) {
	if lengthBytes == 0 {
		return nil, nil
	}
	if pageBytes == 0 || chunkSizeBytes == 0 || pageBytes%chunkSizeBytes != 0 {
		return nil, fmt.Errorf("invalid allocation geometry: page_bytes=%d chunk_size_bytes=%d", pageBytes, chunkSizeBytes)
	}
	reader, ok := s.pageReader.(cloneAllocationPageReader)
	if !ok || reader == nil {
		return nil, fmt.Errorf("clone allocation page reader is not configured")
	}
	clone, err := reader.GetCloneRecord(ctx, cloneID)
	if err != nil {
		return nil, err
	}
	if clone.State != CloneStateAvailable && clone.State != CloneStateMaterializing {
		return nil, fmt.Errorf("clone %q is not available: state=%s", cloneID, clone.State)
	}
	if clone.AllocationPageSizeBytes != pageBytes || clone.AllocationChunkSizeBytes != chunkSizeBytes {
		return nil, fmt.Errorf("clone allocation page geometry mismatch: clone_id=%s page_bytes=%d chunk_size_bytes=%d expected_page_bytes=%d expected_chunk_size_bytes=%d",
			cloneID, pageBytes, chunkSizeBytes, clone.AllocationPageSizeBytes, clone.AllocationChunkSizeBytes)
	}
	basePages, err := resolveCapturedAllocationPages(ctx, reader, clone.CloneBaseRootID, clone.SourceVolumeID, offsetBytes, lengthBytes, pageBytes, chunkSizeBytes)
	if err != nil {
		return nil, err
	}
	for i := range basePages {
		delta, err := reader.GetCloneDeltaAllocationPage(ctx, cloneID, basePages[i].Page.PageNo)
		if err != nil {
			if errors.Is(err, ErrNotFound) {
				continue
			}
			return nil, err
		}
		if delta.PageBytes != pageBytes || delta.ChunkSizeBytes != chunkSizeBytes {
			return nil, fmt.Errorf("clone delta allocation page geometry mismatch: clone_id=%s page_no=%d page_bytes=%d chunk_size_bytes=%d expected_page_bytes=%d expected_chunk_size_bytes=%d",
				cloneID, delta.PageNo, delta.PageBytes, delta.ChunkSizeBytes, pageBytes, chunkSizeBytes)
		}
		basePages[i].Page = delta
	}
	return basePages, nil
}

func (s *Service) CommitCloneDeltaAllocationPages(ctx context.Context, cloneID string, pages []AllocationPageRecord) error {
	if len(pages) == 0 {
		return nil
	}
	if s.cloneDeltaWriter == nil {
		return fmt.Errorf("clone delta allocation page writer is not configured")
	}
	return s.cloneDeltaWriter.PutCloneDeltaAllocationPages(ctx, cloneID, pages)
}

func resolveCapturedAllocationPages(ctx context.Context, reader snapshotAllocationPageReader, rootID, zeroVolumeID string, offsetBytes, lengthBytes uint64, pageBytes, chunkSizeBytes uint32) ([]ResolvedAllocationPage, error) {
	chunkSize := uint64(chunkSizeBytes)
	chunksPerPage := uint64(pageBytes / chunkSizeBytes)
	startChunk := offsetBytes / chunkSize
	endChunk := (offsetBytes + lengthBytes - 1) / chunkSize
	startPage := startChunk / chunksPerPage
	endPage := endChunk / chunksPerPage

	out := make([]ResolvedAllocationPage, 0, endPage-startPage+1)
	var snapshot SnapshotRecord
	for pageNo := startPage; pageNo <= endPage; pageNo++ {
		page, err := reader.GetSnapshotAllocationPage(ctx, rootID, pageNo)
		if err != nil {
			if !errors.Is(err, ErrNotFound) {
				return nil, err
			}
			if zeroVolumeID == "" {
				if snapshot.SnapshotID == "" {
					snapshot, err = reader.GetSnapshotRecord(ctx, rootID)
					if err != nil {
						return nil, err
					}
				}
				zeroVolumeID = snapshot.SourceVolumeID
			}
			page = zeroAllocationPage(zeroVolumeID, pageNo, pageBytes, chunkSizeBytes)
		}
		if page.PageBytes != pageBytes || page.ChunkSizeBytes != chunkSizeBytes {
			return nil, fmt.Errorf("captured allocation page geometry mismatch: root_id=%s page_no=%d page_bytes=%d chunk_size_bytes=%d expected_page_bytes=%d expected_chunk_size_bytes=%d",
				rootID, pageNo, page.PageBytes, page.ChunkSizeBytes, pageBytes, chunkSizeBytes)
		}
		pageStartChunk := pageNo * chunksPerPage
		pageEndChunk := pageStartChunk + chunksPerPage
		rangeStartChunk := maxUint64(startChunk, pageStartChunk)
		rangeEndChunk := minUint64(endChunk+1, pageEndChunk)
		out = append(out, ResolvedAllocationPage{
			Page:            page,
			RangeStartChunk: rangeStartChunk,
			RangeEndChunk:   rangeEndChunk,
			CoversWholePage: rangeStartChunk == pageStartChunk && rangeEndChunk == pageEndChunk,
		})
	}
	return out, nil
}

func (s *Service) ReconcileMutationOperationScope(ctx context.Context, volumeID string, op MutationOperationRecord, pageBytes, chunkSizeBytes uint32) (ReconciledMutationScope, error) {
	result := ReconciledMutationScope{Operation: op}
	if volumeID == "" {
		volumeID = op.VolumeID
	}
	if volumeID == "" {
		return result, nil
	}
	result.Operation.VolumeID = volumeID

	if s.placements == nil {
		return result, fmt.Errorf("extent placement resolver is not configured")
	}
	mappings, err := s.placements.ListExtentMappings(ctx, volumeID)
	if err != nil {
		return result, err
	}
	extentByID := make(map[uint64]ExtentMappingRecord, len(mappings))
	for _, mapping := range mappings {
		extentByID[mapping.ExtentID] = mapping
	}

	extentSet := make(map[uint64]struct{})
	for _, extentID := range op.AffectedExtentIDs {
		if extentID != 0 {
			extentSet[extentID] = struct{}{}
		}
	}
	pageSet := make(map[uint64]struct{})
	for _, pageNo := range op.AffectedPageNos {
		pageSet[pageNo] = struct{}{}
	}

	if pageBytes != 0 && chunkSizeBytes != 0 {
		if len(pageSet) == 0 && len(extentSet) > 0 {
			for extentID := range extentSet {
				mapping, ok := extentByID[extentID]
				if !ok {
					continue
				}
				for _, pageNo := range pageNosForExtentMapping(mapping, pageBytes) {
					pageSet[pageNo] = struct{}{}
				}
			}
		}
		if len(pageSet) == 0 && len(op.RetiredPhysicalChunkIDs) > 0 {
			targetChunks := make(map[uint64]struct{})
			for _, chunkID := range op.RetiredPhysicalChunkIDs {
				if chunkID != 0 {
					targetChunks[chunkID] = struct{}{}
				}
			}
			if len(targetChunks) > 0 {
				if s.pageLister == nil {
					return result, fmt.Errorf("allocation page lister is not configured")
				}
				pages, err := s.pageLister.ListCompatibleAllocationPages(ctx, volumeID, pageBytes, chunkSizeBytes)
				if err != nil {
					return result, err
				}
				for _, page := range pages {
					if allocationPageContainsAnyPhysicalChunk(page, targetChunks) {
						pageSet[page.PageNo] = struct{}{}
					}
				}
			}
		}
		if len(extentSet) == 0 && len(pageSet) > 0 {
			for _, mapping := range mappings {
				if extentMappingTouchesAnyPage(mapping, pageSet, pageBytes) {
					extentSet[mapping.ExtentID] = struct{}{}
				}
			}
		}
	}

	reconciledExtents := sortedUint64Keys(extentSet)
	reconciledPages := sortedUint64Keys(pageSet)
	result.AffectedExtentIDs = reconciledExtents
	result.AffectedPageNos = reconciledPages
	if !slices.Equal(op.AffectedExtentIDs, reconciledExtents) {
		result.Operation.AffectedExtentIDs = reconciledExtents
		result.Changed = true
	}
	if !slices.Equal(op.AffectedPageNos, reconciledPages) {
		result.Operation.AffectedPageNos = reconciledPages
		result.Changed = true
	}
	return result, nil
}

func pageNosForExtentMapping(mapping ExtentMappingRecord, pageBytes uint32) []uint64 {
	if pageBytes == 0 || mapping.LengthBytes == 0 {
		return nil
	}
	pageBytes64 := uint64(pageBytes)
	startPage := mapping.LogicalOffset / pageBytes64
	endPage := (mapping.LogicalOffset + mapping.LengthBytes - 1) / pageBytes64
	out := make([]uint64, 0, endPage-startPage+1)
	for pageNo := startPage; pageNo <= endPage; pageNo++ {
		out = append(out, pageNo)
	}
	return out
}

func extentMappingTouchesAnyPage(mapping ExtentMappingRecord, pageNos map[uint64]struct{}, pageBytes uint32) bool {
	for _, pageNo := range pageNosForExtentMapping(mapping, pageBytes) {
		if _, ok := pageNos[pageNo]; ok {
			return true
		}
	}
	return false
}

func allocationPageContainsAnyPhysicalChunk(page AllocationPageRecord, targetChunks map[uint64]struct{}) bool {
	for _, extent := range page.Extents {
		if extent.Kind != AllocationKindData || extent.PhysicalChunkStart == 0 || extent.ChunkCount == 0 {
			continue
		}
		for chunkID := range targetChunks {
			if chunkID >= extent.PhysicalChunkStart && chunkID < extent.PhysicalChunkStart+uint64(extent.ChunkCount) {
				return true
			}
		}
	}
	return false
}

func sortedUint64Keys(values map[uint64]struct{}) []uint64 {
	if len(values) == 0 {
		return nil
	}
	out := make([]uint64, 0, len(values))
	for value := range values {
		out = append(out, value)
	}
	slices.Sort(out)
	return out
}
