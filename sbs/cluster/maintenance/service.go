package maintenance

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/nosway/namrbd/gateway/service"
	"github.com/nosway/namrbd/internal/structuredlog"
	"github.com/nosway/namrbd/sbs/cluster/metadata"
	"github.com/nosway/namrbd/sbs/cluster/replication"
)

type ExtentHealthState string

const (
	ExtentHealthHealthy          ExtentHealthState = "healthy"
	ExtentHealthDegradedWritable ExtentHealthState = "degraded_writable"
	ExtentHealthBlocked          ExtentHealthState = "blocked"
)

type metadataStore interface {
	GetVolumeState(ctx context.Context, volumeID string) (metadata.VolumeState, error)
	GetVolumeSpec(ctx context.Context, volumeID string) (metadata.VolumeSpecRecord, error)
	PutVolumeState(ctx context.Context, rec metadata.VolumeState) error
	metadata.ExtentMappingNormalizeStore
	metadata.AllocationPersistStore
	ListExtentMappings(ctx context.Context, volumeID string) ([]metadata.ExtentMappingRecord, error)
	ListAllocationPages(ctx context.Context, volumeID string) ([]metadata.AllocationPageRecord, error)
	ListCompatibleAllocationPages(ctx context.Context, volumeID string, pageBytes, chunkSizeBytes uint32) ([]metadata.AllocationPageRecord, error)
	ListReplicaSets(ctx context.Context, volumeID string) ([]metadata.ReplicaSetState, error)
	GetReplicaSet(ctx context.Context, volumeID, replicaSetID string) (metadata.ReplicaSetState, error)
	PutReplicaSet(ctx context.Context, rec metadata.ReplicaSetState) error
	GetNodeMembership(ctx context.Context, nodeID string) (metadata.NodeMembershipRecord, error)
	GetNodeHealthDetail(ctx context.Context, nodeID string) (metadata.NodeHealthDetailRecord, error)
	ListNodeMemberships(ctx context.Context) ([]metadata.NodeMembershipRecord, error)
	CommitPrimaryFailover(ctx context.Context, req metadata.CommitPrimaryFailoverRequest) (metadata.VolumeState, metadata.ReplicaSetState, error)
	PutPlacementTransition(ctx context.Context, rec metadata.PlacementTransitionRecord) error
	GetPlacementTransition(ctx context.Context, volumeID, placementRef string) (metadata.PlacementTransitionRecord, error)
	ListPlacementTransitions(ctx context.Context, volumeID string) ([]metadata.PlacementTransitionRecord, error)
	ListMutationOperations(ctx context.Context, volumeID string) ([]metadata.MutationOperationRecord, error)
	GetMutationOperation(ctx context.Context, volumeID, operationID string) (metadata.MutationOperationRecord, error)
	PutMutationOperation(ctx context.Context, rec metadata.MutationOperationRecord) error
}

type PlacementApplyRunner interface {
	ApplyPlacementChanges(ctx context.Context, req metadata.PlacementApplyRequest) error
}

type Service struct {
	store          metadataStore
	placementApply PlacementApplyRunner
	now            func() time.Time
	copyChunkBytes uint64
	batchMaxPages  uint64
}

type transitionCopySegment struct {
	OffsetBytes uint64
	LengthBytes uint64
	Zero        bool
}

type transitionPageBatch struct {
	PageNos     []uint64
	ActivePages []uint64
	Segments    []transitionCopySegment
}

type transitionAllocationView struct {
	enabled             bool
	authoritativeNative bool
	volumeID            string
	pageBytes           uint32
	chunkSizeBytes      uint32
	pagesByNo           map[uint64]metadata.AllocationPageRecord
}

type transitionReadViewAllocationSet struct {
	RootID string
	Pages  []metadata.AllocationPageRecord
}

type transitionSnapshotReadViewStore interface {
	ListSnapshotRecords(ctx context.Context, sourceVolumeID string, includeDeleted bool) ([]metadata.SnapshotRecord, error)
	ListSnapshotAllocationPages(ctx context.Context, snapshotID string) ([]metadata.AllocationPageRecord, error)
}

type transitionCloneReadViewStore interface {
	ListCloneRecords(ctx context.Context, sourceSnapshotID, sourceVolumeID string, includeDeleted bool) ([]metadata.CloneRecord, error)
	ListCloneDeltaAllocationPages(ctx context.Context, cloneID string) ([]metadata.AllocationPageRecord, error)
}

type transitionPreconditionError struct {
	scope string
	err   error
}

type transitionObsoleteError struct {
	err error
}

func (e *transitionPreconditionError) Error() string {
	return e.err.Error()
}

func (e *transitionPreconditionError) Unwrap() error {
	return e.err
}

func (e *transitionObsoleteError) Error() string {
	return e.err.Error()
}

func (e *transitionObsoleteError) Unwrap() error {
	return e.err
}

func transitionPreconditionf(scope, format string, args ...any) error {
	return &transitionPreconditionError{scope: scope, err: fmt.Errorf(format, args...)}
}

func transitionObsoletef(format string, args ...any) error {
	return &transitionObsoleteError{err: fmt.Errorf(format, args...)}
}

func isTransitionPreconditionError(err error) bool {
	var precondition *transitionPreconditionError
	return errors.As(err, &precondition)
}

func isTransitionObsoleteError(err error) bool {
	var obsolete *transitionObsoleteError
	return errors.As(err, &obsolete)
}

func transitionPreconditionScope(err error) string {
	var precondition *transitionPreconditionError
	if !errors.As(err, &precondition) {
		return ""
	}
	return precondition.scope
}

var transitionReasonMarkers = []string{
	"-repair-",
	"-rebalance-",
	"-drain-",
}

func normalizeTransitionBaseID(id string) string {
	base := id
	for {
		cut := -1
		for _, marker := range transitionReasonMarkers {
			if idx := strings.LastIndex(base, marker); idx > cut {
				cut = idx
			}
		}
		if cut <= 0 {
			return base
		}
		base = base[:cut]
	}
}

func buildTransitionDerivedIDs(currentReplicaSetID, currentPlacementRef, reason, replaceNodeID string) (string, string) {
	replicaSetBase := normalizeTransitionBaseID(currentReplicaSetID)
	placementBase := normalizeTransitionBaseID(currentPlacementRef)
	return fmt.Sprintf("%s-%s-%s", replicaSetBase, reason, replaceNodeID),
		fmt.Sprintf("%s-%s-%s", placementBase, reason, replaceNodeID)
}

const DefaultTransitionCopyChunkBytes uint64 = 1 << 20
const DefaultTransitionBatchMaxPages uint64 = 16

func NewService(store metadataStore) *Service {
	return NewServiceWithPlacementApply(store, metadata.NewPlacementApplyService(store))
}

func NewServiceWithPlacementApply(store metadataStore, placementApply PlacementApplyRunner) *Service {
	if placementApply == nil {
		placementApply = metadata.NewPlacementApplyService(store)
	}
	return &Service{
		store:          store,
		placementApply: placementApply,
		now:            time.Now,
		copyChunkBytes: DefaultTransitionCopyChunkBytes,
		batchMaxPages:  DefaultTransitionBatchMaxPages,
	}
}

func (s *Service) SetTransitionCopyChunkBytes(bytes uint64) {
	if bytes == 0 {
		bytes = DefaultTransitionCopyChunkBytes
	}
	s.copyChunkBytes = bytes
}

func (s *Service) SetTransitionBatchMaxPages(pages uint64) {
	if pages == 0 {
		pages = DefaultTransitionBatchMaxPages
	}
	s.batchMaxPages = pages
}

type EvaluatedExtent struct {
	Extent               metadata.ExtentMappingRecord
	ReplicaSet           metadata.ReplicaSetState
	HealthyReplicas      int
	State                ExtentHealthState
	ZeroOnly             bool
	DataPresent          bool
	DataBytes            uint64
	DataChunks           uint64
	IncompleteTransition bool
	RetryWindowCount     uint64
	RetryWindowBytes     uint64
	RetryWindowChunks    uint64
	IncompleteBatches    uint64
	IncompleteBytes      uint64
	IncompleteChunks     uint64
	IncompletePages      uint64
	IncompleteUpdatedAt  int64
	RecentMutation       bool
	RecentUpdatedAt      int64
}

func compareEvaluatedExtentPriority(left, right *EvaluatedExtent) int {
	if left == nil || right == nil {
		return 0
	}
	if left.IncompleteTransition != right.IncompleteTransition {
		if left.IncompleteTransition {
			return -1
		}
		return 1
	}
	if left.IncompleteUpdatedAt != right.IncompleteUpdatedAt {
		if left.IncompleteUpdatedAt > right.IncompleteUpdatedAt {
			return -1
		}
		return 1
	}
	if (left.RetryWindowCount > 0) != (right.RetryWindowCount > 0) {
		if left.RetryWindowCount > 0 {
			return -1
		}
		return 1
	}
	if left.RetryWindowBytes != right.RetryWindowBytes {
		if left.RetryWindowBytes < right.RetryWindowBytes {
			return -1
		}
		return 1
	}
	if left.RetryWindowChunks != right.RetryWindowChunks {
		if left.RetryWindowChunks < right.RetryWindowChunks {
			return -1
		}
		return 1
	}
	if left.RetryWindowCount != right.RetryWindowCount {
		if left.RetryWindowCount < right.RetryWindowCount {
			return -1
		}
		return 1
	}
	if left.IncompleteBatches != right.IncompleteBatches {
		if left.IncompleteBatches < right.IncompleteBatches {
			return -1
		}
		return 1
	}
	if left.IncompleteBytes != right.IncompleteBytes {
		if left.IncompleteBytes < right.IncompleteBytes {
			return -1
		}
		return 1
	}
	if left.IncompleteChunks != right.IncompleteChunks {
		if left.IncompleteChunks < right.IncompleteChunks {
			return -1
		}
		return 1
	}
	if left.IncompletePages != right.IncompletePages {
		if left.IncompletePages < right.IncompletePages {
			return -1
		}
		return 1
	}
	if left.RecentMutation != right.RecentMutation {
		if left.RecentMutation {
			return -1
		}
		return 1
	}
	if left.RecentUpdatedAt != right.RecentUpdatedAt {
		if left.RecentUpdatedAt > right.RecentUpdatedAt {
			return -1
		}
		return 1
	}
	if left.ZeroOnly != right.ZeroOnly {
		if left.ZeroOnly {
			return -1
		}
		return 1
	}
	if left.DataBytes != right.DataBytes {
		if left.DataBytes < right.DataBytes {
			return -1
		}
		return 1
	}
	if left.DataChunks != right.DataChunks {
		if left.DataChunks < right.DataChunks {
			return -1
		}
		return 1
	}
	if left.Extent.ExtentID < right.Extent.ExtentID {
		return -1
	}
	if left.Extent.ExtentID > right.Extent.ExtentID {
		return 1
	}
	return 0
}

func CompareEvaluatedExtentPriority(left, right *EvaluatedExtent) int {
	return compareEvaluatedExtentPriority(left, right)
}

type volumeEvaluationContext struct {
	volume                         metadata.VolumeState
	mappings                       []metadata.ExtentMappingRecord
	mappingByExtentID              map[uint64]metadata.ExtentMappingRecord
	replicaSets                    []metadata.ReplicaSetState
	replicaSetByPlacement          map[string]metadata.ReplicaSetState
	nodeByID                       map[string]metadata.NodeMembershipRecord
	nodeHealthDetailByID           map[string]metadata.NodeHealthDetailRecord
	allocationView                 transitionAllocationView
	recentMutationByExtentID       map[uint64]int64
	recentPageNosByExtentID        map[uint64]map[uint64]struct{}
	incompleteTransitionByExtentID map[uint64]int64
	retryWindowCountByExtentID     map[uint64]uint64
	retryWindowBytesByExtentID     map[uint64]uint64
	retryWindowChunksByExtentID    map[uint64]uint64
	incompleteBatchCountByExtentID map[uint64]uint64
	incompleteBytesByExtentID      map[uint64]uint64
	incompleteChunksByExtentID     map[uint64]uint64
	incompletePageNosByExtentID    map[uint64]map[uint64]struct{}
}

func (s *Service) loadVolumeEvaluationContext(ctx context.Context, volumeID string) (*volumeEvaluationContext, error) {
	volume, err := s.store.GetVolumeState(ctx, volumeID)
	if err != nil {
		return nil, err
	}
	mappings, err := s.store.ListExtentMappings(ctx, volumeID)
	if err != nil {
		return nil, err
	}
	replicaSets, err := s.store.ListReplicaSets(ctx, volumeID)
	if err != nil {
		return nil, err
	}
	allocationView, err := s.loadTransitionAllocationView(ctx, volumeID)
	if err != nil {
		return nil, err
	}

	vc := &volumeEvaluationContext{
		volume:                         volume,
		mappings:                       mappings,
		mappingByExtentID:              make(map[uint64]metadata.ExtentMappingRecord, len(mappings)),
		replicaSets:                    replicaSets,
		replicaSetByPlacement:          make(map[string]metadata.ReplicaSetState, len(replicaSets)),
		nodeByID:                       make(map[string]metadata.NodeMembershipRecord),
		nodeHealthDetailByID:           make(map[string]metadata.NodeHealthDetailRecord),
		allocationView:                 allocationView,
		recentMutationByExtentID:       make(map[uint64]int64),
		recentPageNosByExtentID:        make(map[uint64]map[uint64]struct{}),
		incompleteTransitionByExtentID: make(map[uint64]int64),
		retryWindowCountByExtentID:     make(map[uint64]uint64),
		retryWindowBytesByExtentID:     make(map[uint64]uint64),
		retryWindowChunksByExtentID:    make(map[uint64]uint64),
		incompleteBatchCountByExtentID: make(map[uint64]uint64),
		incompleteBytesByExtentID:      make(map[uint64]uint64),
		incompleteChunksByExtentID:     make(map[uint64]uint64),
		incompletePageNosByExtentID:    make(map[uint64]map[uint64]struct{}),
	}
	for _, mapping := range mappings {
		vc.mappingByExtentID[mapping.ExtentID] = mapping
	}
	for _, replicaSet := range replicaSets {
		vc.replicaSetByPlacement[replicaSet.PlacementRef] = replicaSet
	}
	if err := s.populateRecentMutationScope(ctx, volumeID, vc); err != nil {
		return nil, err
	}
	return vc, nil
}

func (s *Service) evaluateExtentHealthWithContext(ctx context.Context, vc *volumeEvaluationContext, extentID uint64) (*EvaluatedExtent, error) {
	mapping, found := vc.mappingByExtentID[extentID]
	if !found {
		return nil, metadata.ErrNotFound
	}

	replicaSet, found := vc.replicaSetByPlacement[mapping.PlacementRef]
	if !found {
		return nil, fmt.Errorf("extent %d placement_ref %q has no replica set", mapping.ExtentID, mapping.PlacementRef)
	}

	healthy, err := s.countHealthyReplicasWithContext(ctx, vc, replicaSet)
	if err != nil {
		return nil, err
	}
	zeroOnly, dataPresent, dataBytes, dataChunks, err := vc.extentAllocationShape(mapping)
	if err != nil {
		return nil, err
	}

	state := ExtentHealthBlocked
	switch {
	case healthy >= int(replicaSet.WriteQuorum):
		if healthy == len(replicaSet.Replicas) {
			state = ExtentHealthHealthy
		} else {
			state = ExtentHealthDegradedWritable
		}
	case zeroOnly:
		// Zero-only extents can be reconstructed via metadata-only transition
		// without requiring source payload availability.
		state = ExtentHealthDegradedWritable
	case healthy > 0:
		state = ExtentHealthBlocked
	}

	return &EvaluatedExtent{
		Extent:               mapping,
		ReplicaSet:           replicaSet,
		HealthyReplicas:      healthy,
		State:                state,
		ZeroOnly:             zeroOnly,
		DataPresent:          dataPresent,
		DataBytes:            dataBytes,
		DataChunks:           dataChunks,
		IncompleteTransition: len(vc.incompletePageNosByExtentID[mapping.ExtentID]) > 0,
		RetryWindowCount:     vc.retryWindowCountByExtentID[mapping.ExtentID],
		RetryWindowBytes:     vc.retryWindowBytesByExtentID[mapping.ExtentID],
		RetryWindowChunks:    vc.retryWindowChunksByExtentID[mapping.ExtentID],
		IncompleteBatches:    vc.incompleteBatchCountByExtentID[mapping.ExtentID],
		IncompleteBytes:      vc.incompleteBytesByExtentID[mapping.ExtentID],
		IncompleteChunks:     vc.incompleteChunksByExtentID[mapping.ExtentID],
		IncompletePages:      uint64(len(vc.incompletePageNosByExtentID[mapping.ExtentID])),
		IncompleteUpdatedAt:  vc.incompleteTransitionByExtentID[mapping.ExtentID],
		RecentMutation:       vc.recentMutationByExtentID[mapping.ExtentID] > 0,
		RecentUpdatedAt:      vc.recentMutationByExtentID[mapping.ExtentID],
	}, nil
}

func (s *Service) populateRecentMutationScope(ctx context.Context, volumeID string, vc *volumeEvaluationContext) error {
	operations, err := s.store.ListMutationOperations(ctx, volumeID)
	if err != nil {
		return err
	}
	parentTransitionIDs := make(map[string]struct{})
	for _, operation := range operations {
		if operation.Kind == "transition" {
			parentTransitionIDs[operation.OperationID] = struct{}{}
		}
		if operation.Kind != "write" && operation.Kind != "transition" {
			continue
		}
		if operation.State == metadata.MutationOperationRolledBack {
			continue
		}
		updatedAt := operation.LastUpdatedAtUnix
		if updatedAt == 0 {
			updatedAt = operation.StartedAtUnix
		}
		if updatedAt == 0 {
			continue
		}
		for _, extentID := range operation.AffectedExtentIDs {
			if updatedAt > vc.recentMutationByExtentID[extentID] {
				vc.recentMutationByExtentID[extentID] = updatedAt
			}
			if len(operation.AffectedPageNos) == 0 {
				continue
			}
			recentPages := vc.ensureRecentPageSet(extentID)
			for _, pageNo := range operation.AffectedPageNos {
				recentPages[pageNo] = struct{}{}
			}
		}
		if operation.Kind == "transition" && operation.State != metadata.MutationOperationCommitted && len(operation.AffectedPageNos) > 0 {
			incompletePages := subtractPageNos(operation.AffectedPageNos, operation.CompletedPageNos)
			if len(incompletePages) > 0 {
				for _, extentID := range operation.AffectedExtentIDs {
					if updatedAt > vc.incompleteTransitionByExtentID[extentID] {
						vc.incompleteTransitionByExtentID[extentID] = updatedAt
					}
					incompleteSet := vc.ensureIncompletePageSet(extentID)
					for _, pageNo := range incompletePages {
						incompleteSet[pageNo] = struct{}{}
					}
				}
			}
			if len(operation.RetryPageWindows) > 0 {
				for _, window := range operation.RetryPageWindows {
					if updatedAt > vc.incompleteTransitionByExtentID[window.ExtentID] {
						vc.incompleteTransitionByExtentID[window.ExtentID] = updatedAt
					}
					vc.retryWindowCountByExtentID[window.ExtentID]++
					vc.retryWindowBytesByExtentID[window.ExtentID] += window.DataBytes
					vc.retryWindowChunksByExtentID[window.ExtentID] += window.DataChunks
				}
			}
		}
		if len(operation.AffectedExtentIDs) > 0 || len(operation.AffectedPageNos) == 0 || vc.allocationView.pageBytes == 0 {
			continue
		}
		pageSet := make(map[uint64]struct{}, len(operation.AffectedPageNos))
		for _, pageNo := range operation.AffectedPageNos {
			pageSet[pageNo] = struct{}{}
		}
		for _, mapping := range vc.mappings {
			if !extentTouchesAnyPage(mapping, pageSet, vc.allocationView.pageBytes) {
				continue
			}
			if updatedAt > vc.recentMutationByExtentID[mapping.ExtentID] {
				vc.recentMutationByExtentID[mapping.ExtentID] = updatedAt
			}
			recentPages := vc.ensureRecentPageSet(mapping.ExtentID)
			for pageNo := range pageSet {
				recentPages[pageNo] = struct{}{}
			}
			if operation.Kind == "transition" && operation.State != metadata.MutationOperationCommitted {
				incompletePages := subtractPageNos(operation.AffectedPageNos, operation.CompletedPageNos)
				if len(incompletePages) == 0 {
					continue
				}
				if updatedAt > vc.incompleteTransitionByExtentID[mapping.ExtentID] {
					vc.incompleteTransitionByExtentID[mapping.ExtentID] = updatedAt
				}
				incompleteSet := vc.ensureIncompletePageSet(mapping.ExtentID)
				for _, pageNo := range incompletePages {
					if _, ok := pageSet[pageNo]; ok {
						incompleteSet[pageNo] = struct{}{}
					}
				}
			}
		}
	}
	for _, operation := range operations {
		if operation.Kind != "transition_batch" {
			continue
		}
		if _, ok := parentTransitionIDs[operation.IdempotencyKey]; !ok {
			continue
		}
		remainingPages := subtractPageNos(operation.AffectedPageNos, operation.CompletedPageNos)
		if len(remainingPages) == 0 {
			continue
		}
		for _, extentID := range operation.AffectedExtentIDs {
			vc.incompleteBatchCountByExtentID[extentID]++
		}
	}
	for extentID, pageSet := range vc.incompletePageNosByExtentID {
		if len(pageSet) == 0 {
			continue
		}
		if vc.incompleteBatchCountByExtentID[extentID] == 0 {
			vc.incompleteBatchCountByExtentID[extentID] = countTransitionPageWindows(pageSet, s.batchMaxPages)
		}
		mapping, ok := vc.mappingByExtentID[extentID]
		if !ok {
			continue
		}
		dataBytes, dataChunks, err := vc.extentAllocationShapeForPages(mapping, pageSet)
		if err != nil {
			return err
		}
		vc.incompleteBytesByExtentID[extentID] = dataBytes
		vc.incompleteChunksByExtentID[extentID] = dataChunks
	}
	return nil
}

func (vc *volumeEvaluationContext) ensureRecentPageSet(extentID uint64) map[uint64]struct{} {
	pageSet, ok := vc.recentPageNosByExtentID[extentID]
	if !ok {
		pageSet = make(map[uint64]struct{})
		vc.recentPageNosByExtentID[extentID] = pageSet
	}
	return pageSet
}

func (vc *volumeEvaluationContext) ensureIncompletePageSet(extentID uint64) map[uint64]struct{} {
	pageSet, ok := vc.incompletePageNosByExtentID[extentID]
	if !ok {
		pageSet = make(map[uint64]struct{})
		vc.incompletePageNosByExtentID[extentID] = pageSet
	}
	return pageSet
}

func subtractPageNos(affected, completed []uint64) []uint64 {
	if len(affected) == 0 {
		return nil
	}
	completedSet := make(map[uint64]struct{}, len(completed))
	for _, pageNo := range completed {
		completedSet[pageNo] = struct{}{}
	}
	out := make([]uint64, 0, len(affected))
	for _, pageNo := range affected {
		if _, ok := completedSet[pageNo]; ok {
			continue
		}
		out = append(out, pageNo)
	}
	return uniqueSortedUint64s(out)
}

func extentTouchesAnyPage(mapping metadata.ExtentMappingRecord, pageSet map[uint64]struct{}, pageBytes uint32) bool {
	if pageBytes == 0 || mapping.LengthBytes == 0 {
		return false
	}
	pageBytes64 := uint64(pageBytes)
	startPage := mapping.LogicalOffset / pageBytes64
	endPage := (mapping.LogicalOffset + mapping.LengthBytes - 1) / pageBytes64
	for pageNo := startPage; pageNo <= endPage; pageNo++ {
		if _, ok := pageSet[pageNo]; ok {
			return true
		}
	}
	return false
}

func (vc *volumeEvaluationContext) extentAllocationShape(mapping metadata.ExtentMappingRecord) (zeroOnly bool, dataPresent bool, dataBytes uint64, dataChunks uint64, err error) {
	if mapping.LengthBytes == 0 {
		return true, false, 0, 0, nil
	}
	if !vc.allocationView.enabled {
		if mapping.ChunkID == 0 {
			return true, false, 0, 0, nil
		}
		return false, true, mapping.LengthBytes, 0, nil
	}
	segments, err := vc.allocationView.segmentsForMapping(mapping)
	if err != nil {
		return false, false, 0, 0, err
	}
	if len(segments) == 0 {
		return true, false, 0, 0, nil
	}
	for _, segment := range segments {
		if segment.Zero {
			continue
		}
		dataPresent = true
		dataBytes += segment.LengthBytes
		dataChunks += countChunksForRange(segment.OffsetBytes, segment.LengthBytes, vc.allocationView.chunkSizeBytes)
	}
	return !dataPresent, dataPresent, dataBytes, dataChunks, nil
}

func (vc *volumeEvaluationContext) extentAllocationShapeForPages(mapping metadata.ExtentMappingRecord, pageSet map[uint64]struct{}) (dataBytes uint64, dataChunks uint64, err error) {
	if len(pageSet) == 0 || mapping.LengthBytes == 0 {
		return 0, 0, nil
	}
	if !vc.allocationView.enabled {
		return 0, 0, nil
	}
	segments, err := vc.allocationView.segmentsForMapping(mapping)
	if err != nil {
		return 0, 0, err
	}
	for _, segment := range segments {
		if segment.Zero || segment.LengthBytes == 0 {
			continue
		}
		for _, piece := range splitTransitionSegmentIntoPages(segment, vc.allocationView.pageBytes) {
			pageNos := pageNosForSegment(piece, vc.allocationView.pageBytes)
			if len(pageNos) != 1 {
				continue
			}
			if _, ok := pageSet[pageNos[0]]; !ok {
				continue
			}
			dataBytes += piece.LengthBytes
			dataChunks += countChunksForRange(piece.OffsetBytes, piece.LengthBytes, vc.allocationView.chunkSizeBytes)
		}
	}
	return dataBytes, dataChunks, nil
}

func countChunksForRange(offsetBytes, lengthBytes uint64, chunkSizeBytes uint32) uint64 {
	if lengthBytes == 0 || chunkSizeBytes == 0 {
		return 0
	}
	chunkSize := uint64(chunkSizeBytes)
	startChunk := offsetBytes / chunkSize
	endChunk := (offsetBytes + lengthBytes - 1) / chunkSize
	return endChunk - startChunk + 1
}

func countTransitionPageWindows(pageSet map[uint64]struct{}, maxPages uint64) uint64 {
	if len(pageSet) == 0 {
		return 0
	}
	if maxPages == 0 {
		maxPages = DefaultTransitionBatchMaxPages
	}
	pageNos := make([]uint64, 0, len(pageSet))
	for pageNo := range pageSet {
		pageNos = append(pageNos, pageNo)
	}
	sort.Slice(pageNos, func(i, j int) bool { return pageNos[i] < pageNos[j] })
	var windows uint64
	var currentLen uint64
	var prev uint64
	for i, pageNo := range pageNos {
		if i == 0 || pageNo != prev+1 || currentLen >= maxPages {
			windows++
			currentLen = 1
			prev = pageNo
			continue
		}
		currentLen++
		prev = pageNo
	}
	return windows
}

func (s *Service) countHealthyReplicasWithContext(ctx context.Context, vc *volumeEvaluationContext, replicaSet metadata.ReplicaSetState) (int, error) {
	healthy := 0
	for _, replica := range replicaSet.Replicas {
		node, ok := vc.nodeByID[replica.NodeID]
		if !ok {
			var err error
			node, err = s.store.GetNodeMembership(ctx, replica.NodeID)
			if err != nil {
				return 0, err
			}
			vc.nodeByID[replica.NodeID] = node
		}
		if node.LifecycleState == metadata.NodeLifecycleRemoved {
			continue
		}
		if (node.HealthState == metadata.NodeHealthHealthy || node.HealthState == metadata.NodeHealthSuspect) && s.nodeStoreEligibleForReplica(ctx, vc, node.NodeID) {
			healthy++
		}
	}
	return healthy, nil
}

func (s *Service) EvaluateExtentHealth(ctx context.Context, volumeID string, extentID uint64) (*EvaluatedExtent, error) {
	vc, err := s.loadVolumeEvaluationContext(ctx, volumeID)
	if err != nil {
		return nil, err
	}
	return s.evaluateExtentHealthWithContext(ctx, vc, extentID)
}

func (s *Service) ReconcileVolumeStatus(ctx context.Context, volumeID string) (metadata.VolumeState, error) {
	volumeState, err := s.store.GetVolumeState(ctx, volumeID)
	if err != nil {
		return metadata.VolumeState{}, err
	}
	vc, err := s.loadVolumeEvaluationContext(ctx, volumeID)
	if err != nil {
		return metadata.VolumeState{}, err
	}

	nextStatus := metadata.VolumeStatusHealthy
	for _, mapping := range vc.mappings {
		evaluated, err := s.evaluateExtentHealthWithContext(ctx, vc, mapping.ExtentID)
		if err != nil {
			return metadata.VolumeState{}, err
		}
		switch evaluated.State {
		case ExtentHealthBlocked:
			nextStatus = metadata.VolumeStatusBlocked
			goto done
		case ExtentHealthDegradedWritable:
			if nextStatus != metadata.VolumeStatusBlocked {
				nextStatus = metadata.VolumeStatusDegraded
			}
		}
	}

done:
	volumeState.Status = nextStatus
	if err := s.store.PutVolumeState(ctx, volumeState); err != nil {
		return metadata.VolumeState{}, err
	}
	return volumeState, nil
}

func (s *Service) EnqueueRepair(ctx context.Context, volumeID string, extentID uint64, targetReplicaSetID string) (metadata.PlacementTransitionRecord, error) {
	evaluated, err := s.EvaluateExtentHealth(ctx, volumeID, extentID)
	if err != nil {
		return metadata.PlacementTransitionRecord{}, err
	}
	return s.enqueueRepairFromEvaluated(ctx, evaluated, targetReplicaSetID)
}

func (s *Service) enqueueRepairFromEvaluated(ctx context.Context, evaluated *EvaluatedExtent, targetReplicaSetID string) (metadata.PlacementTransitionRecord, error) {
	record := metadata.PlacementTransitionRecord{
		VolumeID:            evaluated.Extent.VolumeID,
		PlacementRef:        evaluated.Extent.PlacementRef,
		State:               metadata.PlacementTransitionQueued,
		Reason:              "repair",
		CurrentReplicaSetID: evaluated.ReplicaSet.ReplicaSetID,
		TargetReplicaSetID:  targetReplicaSetID,
		StartedAtUnix:       s.now().Unix(),
		LastProgressAtUnix:  s.now().Unix(),
		Attempt:             1,
	}
	if err := s.store.PutPlacementTransition(ctx, record); err != nil {
		return metadata.PlacementTransitionRecord{}, err
	}
	structuredlog.Info("sbs.maintenance", "repair_enqueued",
		structuredlog.F("volume_id", evaluated.Extent.VolumeID),
		structuredlog.F("extent_id", evaluated.Extent.ExtentID),
		structuredlog.F("placement_ref", evaluated.Extent.PlacementRef),
		structuredlog.F("current_replica_set_id", evaluated.ReplicaSet.ReplicaSetID),
		structuredlog.F("target_replica_set_id", targetReplicaSetID),
		structuredlog.F("zero_only", evaluated.ZeroOnly),
		structuredlog.F("data_present", evaluated.DataPresent),
		structuredlog.F("data_bytes", evaluated.DataBytes),
		structuredlog.F("data_chunks", evaluated.DataChunks),
	)
	return record, nil
}

func (s *Service) ReplanRepairTransitionTarget(ctx context.Context, volumeID string, transition metadata.PlacementTransitionRecord) (bool, error) {
	return s.ReplanTransitionTarget(ctx, volumeID, transition)
}

func (s *Service) ReplanTransitionTarget(ctx context.Context, volumeID string, transition metadata.PlacementTransitionRecord) (bool, error) {
	switch transition.Reason {
	case "repair", "rebalance", "drain":
	default:
		return false, nil
	}
	if transition.State != metadata.PlacementTransitionQueued && transition.State != metadata.PlacementTransitionRunning {
		return false, nil
	}
	vc, err := s.loadVolumeEvaluationContext(ctx, volumeID)
	if err != nil {
		return false, err
	}
	var extentID uint64
	found := false
	for _, mapping := range vc.mappings {
		if mapping.PlacementRef == transition.PlacementRef {
			extentID = mapping.ExtentID
			found = true
			break
		}
	}
	if !found {
		return false, nil
	}
	evaluated, err := s.evaluateExtentHealthWithContext(ctx, vc, extentID)
	if err != nil {
		return false, err
	}
	if evaluated.State == ExtentHealthHealthy {
		transition.State = metadata.PlacementTransitionCompleted
		transition.LastProgressAtUnix = s.now().Unix()
		if err := s.store.PutPlacementTransition(ctx, transition); err != nil {
			return false, err
		}
		return true, nil
	}
	var targetReplicaSet metadata.ReplicaSetState
	var ok bool
	switch transition.Reason {
	case "repair":
		targetReplicaSet, ok, err = s.planRepairTargetReplicaSetWithContext(ctx, vc, evaluated)
		if err != nil {
			return false, err
		}
		if !ok {
			return false, nil
		}
	case "rebalance", "drain":
		existingTarget, targetErr := s.store.GetReplicaSet(ctx, volumeID, transition.TargetReplicaSetID)
		if targetErr != nil {
			if targetErr == metadata.ErrNotFound {
				return false, nil
			}
			return false, targetErr
		}
		replaceNodeID, replaceOK := replacedNodeID(evaluated.ReplicaSet, existingTarget)
		if !replaceOK {
			return false, nil
		}
		targetReplicaSet, ok, err = s.planReplacementReplicaSetWithContext(ctx, vc, evaluated.Extent.PlacementRef, evaluated.ReplicaSet, replaceNodeID, transition.Reason, evaluated)
		if err != nil {
			return false, err
		}
		if !ok {
			return false, nil
		}
	}
	if targetReplicaSet.ReplicaSetID == transition.TargetReplicaSetID &&
		evaluated.ReplicaSet.ReplicaSetID == transition.CurrentReplicaSetID {
		existingTarget, err := s.store.GetReplicaSet(ctx, volumeID, targetReplicaSet.ReplicaSetID)
		if err != nil && err != metadata.ErrNotFound {
			return false, err
		}
		if err == nil && replicaSetStateEqual(existingTarget, targetReplicaSet) {
			return false, nil
		}
	}
	if err := s.store.PutReplicaSet(ctx, targetReplicaSet); err != nil {
		return false, err
	}
	transition.CurrentReplicaSetID = evaluated.ReplicaSet.ReplicaSetID
	transition.TargetReplicaSetID = targetReplicaSet.ReplicaSetID
	transition.State = metadata.PlacementTransitionQueued
	transition.LastProgressAtUnix = s.now().Unix()
	transition.Attempt++
	if err := s.store.PutPlacementTransition(ctx, transition); err != nil {
		return false, err
	}
	structuredlog.Info("sbs.maintenance", "transition_replanned",
		structuredlog.F("volume_id", volumeID),
		structuredlog.F("placement_ref", transition.PlacementRef),
		structuredlog.F("current_replica_set_id", transition.CurrentReplicaSetID),
		structuredlog.F("target_replica_set_id", transition.TargetReplicaSetID),
		structuredlog.F("attempt", transition.Attempt),
	)
	return true, nil
}

func replicaSetStateEqual(left, right metadata.ReplicaSetState) bool {
	if left.ReplicaSetID != right.ReplicaSetID ||
		left.VolumeID != right.VolumeID ||
		left.PlacementRef != right.PlacementRef ||
		left.Epoch != right.Epoch ||
		left.PrimaryReplicaID != right.PrimaryReplicaID ||
		left.WriteQuorum != right.WriteQuorum ||
		left.ReadQuorum != right.ReadQuorum ||
		len(left.Replicas) != len(right.Replicas) ||
		len(left.FailureDomains) != len(right.FailureDomains) {
		return false
	}
	for i := range left.Replicas {
		if left.Replicas[i] != right.Replicas[i] {
			return false
		}
	}
	for i := range left.FailureDomains {
		if left.FailureDomains[i] != right.FailureDomains[i] {
			return false
		}
	}
	return true
}

func (s *Service) EnqueueRebalance(ctx context.Context, volumeID string, extentID uint64, targetReplicaSetID string) (metadata.PlacementTransitionRecord, error) {
	evaluated, err := s.EvaluateExtentHealth(ctx, volumeID, extentID)
	if err != nil {
		return metadata.PlacementTransitionRecord{}, err
	}
	record := metadata.PlacementTransitionRecord{
		VolumeID:            evaluated.Extent.VolumeID,
		PlacementRef:        evaluated.Extent.PlacementRef,
		State:               metadata.PlacementTransitionQueued,
		Reason:              "rebalance",
		CurrentReplicaSetID: evaluated.ReplicaSet.ReplicaSetID,
		TargetReplicaSetID:  targetReplicaSetID,
		StartedAtUnix:       s.now().Unix(),
		LastProgressAtUnix:  s.now().Unix(),
		Attempt:             1,
	}
	if err := s.store.PutPlacementTransition(ctx, record); err != nil {
		return metadata.PlacementTransitionRecord{}, err
	}
	structuredlog.Info("sbs.maintenance", "rebalance_enqueued",
		structuredlog.F("volume_id", evaluated.Extent.VolumeID),
		structuredlog.F("extent_id", evaluated.Extent.ExtentID),
		structuredlog.F("placement_ref", evaluated.Extent.PlacementRef),
		structuredlog.F("current_replica_set_id", evaluated.ReplicaSet.ReplicaSetID),
		structuredlog.F("target_replica_set_id", targetReplicaSetID),
		structuredlog.F("zero_only", evaluated.ZeroOnly),
		structuredlog.F("data_present", evaluated.DataPresent),
		structuredlog.F("data_bytes", evaluated.DataBytes),
		structuredlog.F("data_chunks", evaluated.DataChunks),
	)
	return record, nil
}

func (s *Service) ScanAndEnqueueRepairs(ctx context.Context, volumeID string) (int, error) {
	volumeState, err := s.store.GetVolumeState(ctx, volumeID)
	if err != nil {
		return 0, err
	}
	vc, err := s.loadVolumeEvaluationContext(ctx, volumeID)
	if err != nil {
		return 0, err
	}

	nextStatus := metadata.VolumeStatusHealthy
	for _, mapping := range vc.mappings {
		evaluated, err := s.evaluateExtentHealthWithContext(ctx, vc, mapping.ExtentID)
		if err != nil {
			return 0, err
		}
		switch evaluated.State {
		case ExtentHealthBlocked:
			nextStatus = metadata.VolumeStatusBlocked
			goto reconciled
		case ExtentHealthDegradedWritable:
			if nextStatus != metadata.VolumeStatusBlocked {
				nextStatus = metadata.VolumeStatusDegraded
			}
		}
	}

reconciled:
	volumeState.Status = nextStatus
	if err := s.store.PutVolumeState(ctx, volumeState); err != nil {
		return 0, err
	}

	transitions, err := s.store.ListPlacementTransitions(ctx, volumeID)
	if err != nil {
		return 0, err
	}

	activeTransitions := make(map[string]metadata.PlacementTransitionRecord, len(transitions))
	for _, transition := range transitions {
		switch transition.State {
		case metadata.PlacementTransitionQueued, metadata.PlacementTransitionRunning, metadata.PlacementTransitionFailed:
			activeTransitions[transition.PlacementRef] = transition
		}
	}

	enqueued := 0
	candidates := make([]*EvaluatedExtent, 0, len(vc.mappings))
	for _, mapping := range vc.mappings {
		evaluated, err := s.evaluateExtentHealthWithContext(ctx, vc, mapping.ExtentID)
		if err != nil {
			return enqueued, err
		}
		if evaluated.State == ExtentHealthHealthy {
			continue
		}
		if _, exists := activeTransitions[mapping.PlacementRef]; exists {
			continue
		}
		candidates = append(candidates, evaluated)
	}
	sort.SliceStable(candidates, func(i, j int) bool {
		return compareEvaluatedExtentPriority(candidates[i], candidates[j]) < 0
	})

	for _, evaluated := range candidates {
		targetReplicaSet, ok, err := s.planRepairTargetReplicaSetWithContext(ctx, vc, evaluated)
		if err != nil {
			return enqueued, err
		}
		if !ok {
			continue
		}
		if err := s.store.PutReplicaSet(ctx, targetReplicaSet); err != nil {
			return enqueued, err
		}
		vc.replicaSets = append(vc.replicaSets, targetReplicaSet)
		vc.replicaSetByPlacement[targetReplicaSet.PlacementRef] = targetReplicaSet
		if _, err := s.enqueueRepairFromEvaluated(ctx, evaluated, targetReplicaSet.ReplicaSetID); err != nil {
			return enqueued, err
		}
		enqueued++
	}
	return enqueued, nil
}

func (s *Service) ScanAndFailoverPrimaries(ctx context.Context, volumeID string) (int, error) {
	volumeState, err := s.store.GetVolumeState(ctx, volumeID)
	if err != nil {
		return 0, err
	}
	replicaSets, err := s.store.ListReplicaSets(ctx, volumeID)
	if err != nil {
		return 0, err
	}

	failovers := 0
	currentEpoch := volumeState.Epoch
	for _, replicaSet := range replicaSets {
		primaryHealthy, healthyReplicas, nextPrimary, err := s.evaluatePrimaryFailover(ctx, replicaSet)
		if err != nil {
			return failovers, err
		}
		if primaryHealthy || healthyReplicas < int(replicaSet.WriteQuorum) || nextPrimary == "" {
			continue
		}
		nextState, _, err := s.store.CommitPrimaryFailover(ctx, metadata.CommitPrimaryFailoverRequest{
			VolumeID:                 volumeID,
			ReplicaSetID:             replicaSet.ReplicaSetID,
			ExpectedVolumeEpoch:      currentEpoch,
			ExpectedReplicaSetEpoch:  replicaSet.Epoch,
			ExpectedPrimaryReplicaID: replicaSet.PrimaryReplicaID,
			NewPrimaryReplicaID:      nextPrimary,
		})
		if err != nil {
			if err == metadata.ErrCASConflict {
				continue
			}
			return failovers, err
		}
		currentEpoch = nextState.Epoch
		failovers++
		structuredlog.Info("sbs.maintenance", "primary_failover_committed",
			structuredlog.F("volume_id", volumeID),
			structuredlog.F("replica_set_id", replicaSet.ReplicaSetID),
			structuredlog.F("previous_primary_replica_id", replicaSet.PrimaryReplicaID),
			structuredlog.F("new_primary_replica_id", nextPrimary),
			structuredlog.F("volume_epoch", nextState.Epoch),
		)
	}
	return failovers, nil
}

func (s *Service) ApplyTransition(ctx context.Context, volumeID, placementRef string, replicaClients map[string]service.SBSClient, gatewayID, hostID string) (transition metadata.PlacementTransitionRecord, err error) {
	transition, err = s.store.GetPlacementTransition(ctx, volumeID, placementRef)
	if err != nil {
		return metadata.PlacementTransitionRecord{}, err
	}
	if transition.State != metadata.PlacementTransitionQueued && transition.State != metadata.PlacementTransitionRunning {
		return metadata.PlacementTransitionRecord{}, fmt.Errorf("transition %q is not runnable from state %q", placementRef, transition.State)
	}

	mappings, err := s.store.ListExtentMappings(ctx, volumeID)
	if err != nil {
		return metadata.PlacementTransitionRecord{}, err
	}
	targetMappings := make([]metadata.ExtentMappingRecord, 0)
	allMappingsByExtentID := make(map[uint64]metadata.ExtentMappingRecord, len(mappings))
	for _, mapping := range mappings {
		allMappingsByExtentID[mapping.ExtentID] = mapping
		if mapping.PlacementRef == placementRef {
			targetMappings = append(targetMappings, mapping)
		}
	}
	if len(targetMappings) == 0 {
		return metadata.PlacementTransitionRecord{}, transitionObsoletef("no extent mapping found for placement_ref %q", placementRef)
	}

	currentReplicaSet, err := s.store.GetReplicaSet(ctx, volumeID, transition.CurrentReplicaSetID)
	if err != nil {
		return metadata.PlacementTransitionRecord{}, err
	}
	targetReplicaSet, err := s.store.GetReplicaSet(ctx, volumeID, transition.TargetReplicaSetID)
	if err != nil {
		return metadata.PlacementTransitionRecord{}, err
	}
	vc, err := s.loadVolumeEvaluationContext(ctx, volumeID)
	if err != nil {
		return metadata.PlacementTransitionRecord{}, err
	}
	if err := s.validateTransitionTargetReplicaSetEligibility(ctx, vc, currentReplicaSet, targetReplicaSet); err != nil {
		return metadata.PlacementTransitionRecord{}, err
	}
	allocationView := vc.allocationView
	readViewSets, err := s.loadTransitionReadViewAllocationSets(ctx, volumeID, allocationView)
	if err != nil {
		return metadata.PlacementTransitionRecord{}, err
	}
	dataMappings := make([]metadata.ExtentMappingRecord, 0, len(targetMappings))
	retiredPhysicalChunkIDs := make([]uint64, 0)
	for _, mapping := range targetMappings {
		segments, err := allocationView.segmentsForMapping(mapping)
		if err != nil {
			return metadata.PlacementTransitionRecord{}, err
		}
		segments = prioritizeTransitionSegmentsForRecentPages(segments, vc.recentPageNosByExtentID[mapping.ExtentID], allocationView.pageBytes)
		for _, segment := range segments {
			if segment.Zero || segment.LengthBytes == 0 || allocationView.chunkSizeBytes == 0 {
				continue
			}
			startChunk := segment.OffsetBytes / uint64(allocationView.chunkSizeBytes)
			chunkCount := uint64(segment.LengthBytes) / uint64(allocationView.chunkSizeBytes)
			if segment.LengthBytes%uint64(allocationView.chunkSizeBytes) != 0 {
				chunkCount++
			}
			for i := uint64(0); i < chunkCount; i++ {
				_, zero, physicalChunkID := resolvedLogicalChunkAllocationState(allocationView.allocationPagesForMapping(mapping), startChunk+i)
				if zero || physicalChunkID == 0 {
					continue
				}
				retiredPhysicalChunkIDs = append(retiredPhysicalChunkIDs, physicalChunkID)
			}
		}
		if !segmentsContainData(segments) {
			readViewDataPresent, err := transitionReadViewDataPresentForMapping(volumeID, mapping, allocationView, readViewSets)
			if err != nil {
				return metadata.PlacementTransitionRecord{}, err
			}
			if !readViewDataPresent {
				continue
			}
		}
		dataMappings = append(dataMappings, mapping)
	}
	if len(dataMappings) > 0 {
		if err := s.validateTransitionSourceReplicaSetEligibility(ctx, vc, currentReplicaSet); err != nil {
			return metadata.PlacementTransitionRecord{}, err
		}
	}
	affectedExtentIDs := uniqueSortedExtentIDsFromMappings(dataMappings)
	affectedPageNos := uniqueSortedPageNosFromMappings(dataMappings, allocationView.chunkSizeBytes, allocationView.pageBytes)

	transition.State = metadata.PlacementTransitionRunning
	transition.LastProgressAtUnix = s.now().Unix()
	if err := s.store.PutPlacementTransition(ctx, transition); err != nil {
		return metadata.PlacementTransitionRecord{}, err
	}
	opNow := s.now().Unix()
	operationID := transitionMutationOperationID(transition)
	operation, _ := s.store.GetMutationOperation(ctx, volumeID, operationID)
	operation.OperationID = operationID
	operation.VolumeID = volumeID
	operation.Kind = "transition"
	operation.State = metadata.MutationOperationRunning
	operation.PlacementRevision = uint64(transition.Attempt)
	operation.WriterFencingEpoch = 1
	operation.IdempotencyKey = transition.PlacementRef
	operation.AffectedExtentIDs = unionSortedUint64s(operation.AffectedExtentIDs, affectedExtentIDs)
	operation.AffectedPageNos = unionSortedUint64s(operation.AffectedPageNos, affectedPageNos)
	operation.CompletedPageNos = uniqueSortedUint64s(operation.CompletedPageNos)
	operation.RetryPageWindows = nil
	if operation.StartedAtUnix == 0 {
		operation.StartedAtUnix = opNow
	}
	operation.LastUpdatedAtUnix = opNow
	operation.ErrorMessage = ""
	if err := s.store.PutMutationOperation(ctx, operation); err != nil {
		return metadata.PlacementTransitionRecord{}, err
	}
	completedPageSet := make(map[uint64]struct{}, len(operation.CompletedPageNos))
	for _, pageNo := range operation.CompletedPageNos {
		completedPageSet[pageNo] = struct{}{}
	}
	failedBatchPagesByExtent, err := s.loadFailedTransitionBatchPages(ctx, volumeID, operation.OperationID)
	if err != nil {
		return metadata.PlacementTransitionRecord{}, err
	}
	existingBatchWindowsByExtent, err := s.loadExistingTransitionBatchWindows(ctx, volumeID, operation.OperationID)
	if err != nil {
		return metadata.PlacementTransitionRecord{}, err
	}
	retryBatchWindowsByExtent, err := s.loadRetryableTransitionBatchWindows(ctx, volumeID, operation.OperationID)
	if err != nil {
		return metadata.PlacementTransitionRecord{}, err
	}
	defer func() {
		if err == nil {
			return
		}
		operation.State = metadata.MutationOperationFailed
		operation.LastUpdatedAtUnix = s.now().Unix()
		operation.ErrorMessage = err.Error()
		_ = s.store.PutMutationOperation(context.Background(), operation)
	}()
	structuredlog.Info("sbs.maintenance", "transition_started",
		structuredlog.F("volume_id", volumeID),
		structuredlog.F("placement_ref", placementRef),
		structuredlog.F("reason", transition.Reason),
		structuredlog.F("current_replica_set_id", transition.CurrentReplicaSetID),
		structuredlog.F("target_replica_set_id", transition.TargetReplicaSetID),
	)

	currentReplicas := map[string]replication.RemoteReplica{}
	if len(dataMappings) > 0 {
		currentReplicas, err = replication.OpenReplicaSessions(ctx, subsetReplicaClients(replicaClients, currentReplicaSet), replication.OpenReplicaSessionsRequest{
			VolumeID:      volumeID,
			GatewayID:     gatewayID,
			HostID:        hostID,
			AttachmentID:  "maintenance-" + transition.PlacementRef,
			Generation:    1,
			SessionPrefix: "repair-source-" + transition.PlacementRef,
			AccessMode:    service.SBSAccessModeExclusiveWriter,
			AllowNotFound: true,
		})
		if err != nil {
			return metadata.PlacementTransitionRecord{}, err
		}
		defer closeRemoteReplicas(context.Background(), currentReplicas)
	}
	var writer *replication.RemoteReplicaWriter
	var targetReplicas map[string]replication.RemoteReplica
	defer func() {
		if targetReplicas != nil {
			closeRemoteReplicas(context.Background(), targetReplicas)
		}
	}()
	copyChunkBytes := s.copyChunkBytes
	if copyChunkBytes == 0 {
		copyChunkBytes = DefaultTransitionCopyChunkBytes
	}
	for _, mapping := range targetMappings {
		segments, err := allocationView.segmentsForMapping(mapping)
		if err != nil {
			return metadata.PlacementTransitionRecord{}, err
		}
		segments = prioritizeTransitionSegmentsForPageSet(segments, failedBatchPagesByExtent[mapping.ExtentID], allocationView.pageBytes)
		segments = prioritizeTransitionSegmentsForRecentPages(segments, vc.recentPageNosByExtentID[mapping.ExtentID], allocationView.pageBytes)
		segments = filterTransitionSegmentsByCompletedPages(segments, completedPageSet, allocationView.pageBytes)
		if segmentsContainData(segments) {
			livePages := allocationView.allocationPagesForMapping(mapping)
			readPlan, err := buildTransitionReadPlan(mapping, currentReplicaSet, livePages, allocationView.chunkSizeBytes)
			if err != nil {
				return metadata.PlacementTransitionRecord{}, err
			}
			writePlan, err := buildTransitionWritePlan(mapping, targetReplicaSet, livePages, allocationView.chunkSizeBytes)
			if err != nil {
				return metadata.PlacementTransitionRecord{}, err
			}
			if err := s.copyTransitionSegments(ctx, volumeID, transition, operation, completedPageSet, true, mapping, allocationView.pageBytes, copyChunkBytes, segments, readPlan, writePlan, currentReplicas, &targetReplicas, &writer, replicaClients, targetReplicaSet, gatewayID, hostID, failedBatchPagesByExtent[mapping.ExtentID], retryBatchWindowsByExtent[mapping.ExtentID], existingBatchWindowsByExtent[mapping.ExtentID], vc.recentPageNosByExtentID[mapping.ExtentID]); err != nil {
				return metadata.PlacementTransitionRecord{}, err
			}
		}
		for _, readViewSet := range readViewSets {
			readView, err := newTransitionAllocationView(volumeID, readViewSet.Pages)
			if err != nil {
				return metadata.PlacementTransitionRecord{}, err
			}
			readViewSegments, err := readView.segmentsForMapping(mapping)
			if err != nil {
				return metadata.PlacementTransitionRecord{}, err
			}
			if !segmentsContainData(readViewSegments) {
				continue
			}
			readViewPages := readView.allocationPagesForMapping(mapping)
			readViewReadPlan, err := buildTransitionReadPlan(mapping, currentReplicaSet, readViewPages, readView.chunkSizeBytes)
			if err != nil {
				return metadata.PlacementTransitionRecord{}, err
			}
			readViewWritePlan, err := buildTransitionWritePlan(mapping, targetReplicaSet, readViewPages, readView.chunkSizeBytes)
			if err != nil {
				return metadata.PlacementTransitionRecord{}, err
			}
			if err := s.copyTransitionSegments(ctx, volumeID, transition, operation, completedPageSet, false, mapping, readView.pageBytes, copyChunkBytes, readViewSegments, readViewReadPlan, readViewWritePlan, currentReplicas, &targetReplicas, &writer, replicaClients, targetReplicaSet, gatewayID, hostID, failedBatchPagesByExtent[mapping.ExtentID], retryBatchWindowsByExtent[mapping.ExtentID], existingBatchWindowsByExtent[mapping.ExtentID], vc.recentPageNosByExtentID[mapping.ExtentID]); err != nil {
				return metadata.PlacementTransitionRecord{}, err
			}
		}

		mapping.PlacementRef = targetReplicaSet.PlacementRef
		mapping.Revision++
		if err := s.store.PutExtentMapping(ctx, mapping); err != nil {
			return metadata.PlacementTransitionRecord{}, err
		}
		allMappingsByExtentID[mapping.ExtentID] = mapping
	}
	volumeState, err := s.store.GetVolumeState(ctx, volumeID)
	if err != nil {
		return metadata.PlacementTransitionRecord{}, err
	}
	volumeState.Revision++
	if allocationView.enabled && !allocationView.authoritativeNative {
		if err := s.materializeLegacyAllocationView(ctx, allocationView, allMappingsByExtentID, volumeState.Revision); err != nil {
			return metadata.PlacementTransitionRecord{}, err
		}
	}
	if err := s.store.PutVolumeState(ctx, volumeState); err != nil {
		return metadata.PlacementTransitionRecord{}, err
	}

	transition.State = metadata.PlacementTransitionCompleted
	transition.LastProgressAtUnix = s.now().Unix()
	if err := s.store.PutPlacementTransition(ctx, transition); err != nil {
		return metadata.PlacementTransitionRecord{}, err
	}
	operation.State = metadata.MutationOperationCommitted
	operation.PlacementRevision = volumeState.Revision
	if allocationView.enabled {
		operation.AllocationRevision = volumeState.Revision
	}
	operation.AffectedExtentIDs = affectedExtentIDs
	operation.AffectedPageNos = affectedPageNos
	operation.CompletedPageNos = unionSortedUint64s(operation.CompletedPageNos, affectedPageNos)
	operation.RetryPageWindows = nil
	operation.RetiredPhysicalChunkIDs = uniqueSortedRetiredChunks(retiredPhysicalChunkIDs)
	operation.LastUpdatedAtUnix = s.now().Unix()
	operation.ErrorMessage = ""
	if err := s.store.PutMutationOperation(ctx, operation); err != nil {
		return metadata.PlacementTransitionRecord{}, err
	}
	_ = metadata.EnsurePendingPayloadGCMutationOperation(ctx, s.store, volumeID, operation.AffectedExtentIDs, operation.AffectedPageNos, retiredPhysicalChunkIDs, s.now())
	if _, err := s.ReconcileVolumeStatus(ctx, volumeID); err != nil {
		return metadata.PlacementTransitionRecord{}, err
	}
	structuredlog.Info("sbs.maintenance", "transition_completed",
		structuredlog.F("volume_id", volumeID),
		structuredlog.F("placement_ref", placementRef),
		structuredlog.F("reason", transition.Reason),
		structuredlog.F("current_replica_set_id", transition.CurrentReplicaSetID),
		structuredlog.F("target_replica_set_id", transition.TargetReplicaSetID),
		structuredlog.F("attempt", transition.Attempt),
	)
	return transition, nil
}

func transitionMutationOperationID(transition metadata.PlacementTransitionRecord) string {
	return fmt.Sprintf("transition-%s", transition.PlacementRef)
}

func transitionPageBatchMutationOperationID(transition metadata.PlacementTransitionRecord, pageNo uint64) string {
	return transitionPageRangeBatchMutationOperationID(transition, pageNo, pageNo)
}

func transitionPageRangeBatchMutationOperationID(transition metadata.PlacementTransitionRecord, startPageNo, endPageNo uint64) string {
	return fmt.Sprintf("%s-pages-%020d-%020d", transitionMutationOperationID(transition), startPageNo, endPageNo)
}

func (s *Service) copyTransitionSegments(
	ctx context.Context,
	volumeID string,
	transition metadata.PlacementTransitionRecord,
	operation metadata.MutationOperationRecord,
	completedPageSet map[uint64]struct{},
	trackPageBatches bool,
	mapping metadata.ExtentMappingRecord,
	pageBytes uint32,
	copyChunkBytes uint64,
	segments []transitionCopySegment,
	readPlan replication.ExtentReadPlan,
	writePlan replication.ExtentWritePlan,
	currentReplicas map[string]replication.RemoteReplica,
	targetReplicas *map[string]replication.RemoteReplica,
	writer **replication.RemoteReplicaWriter,
	replicaClients map[string]service.SBSClient,
	targetReplicaSet metadata.ReplicaSetState,
	gatewayID, hostID string,
	failedBatchPages map[uint64]struct{},
	retryBatchWindows [][]uint64,
	existingBatchWindows [][]uint64,
	recentPages map[uint64]struct{},
) error {
	reader := replication.NewRemoteReplicaReader(currentReplicas)
	for _, segment := range segments {
		if segment.Zero {
			continue
		}
		pageSegments := splitTransitionSegmentIntoPages(segment, pageBytes)
		batches := buildTransitionPageBatches(mapping, pageSegments, pageBytes, s.batchMaxPages, retryBatchWindows, existingBatchWindows)
		prioritizeTransitionPageBatches(batches, failedBatchPages, retryBatchWindows, recentPages)
		for _, batch := range batches {
			if len(batch.PageNos) == 0 || len(batch.ActivePages) == 0 || len(batch.Segments) == 0 {
				continue
			}
			pageOperation := metadata.MutationOperationRecord{}
			if trackPageBatches {
				var skipPage bool
				var err error
				pageOperation, skipPage, err = beginTransitionPageBatchMutationOperation(ctx, s.store, transition, operation, mapping.ExtentID, batch.PageNos, s.now())
				if err != nil {
					return err
				}
				if skipPage {
					if err := markTransitionPagesCompleted(ctx, s.store, &operation, completedPageSet, batch.PageNos, s.now); err != nil {
						return err
					}
					continue
				}
			}
			for _, pageSegment := range batch.Segments {
				if pageSegment.Zero || pageSegment.LengthBytes == 0 {
					continue
				}
				for offset := pageSegment.OffsetBytes; offset < pageSegment.OffsetBytes+pageSegment.LengthBytes; {
					chunkLength := minUint64(copyChunkBytes, pageSegment.OffsetBytes+pageSegment.LengthBytes-offset)
					payload := make([]byte, chunkLength)
					if len(currentReplicas) > 0 {
						readPayload, _, readErr := reader.ReadExtent(ctx, readPlan, replication.ReplicaReadRequest{
							VolumeID:    volumeID,
							OffsetBytes: offset,
							LengthBytes: chunkLength,
						})
						if readErr != nil {
							if trackPageBatches {
								pageOperation.State = metadata.MutationOperationFailed
								pageOperation.LastUpdatedAtUnix = s.now().Unix()
								pageOperation.ErrorMessage = readErr.Error()
								_ = s.store.PutMutationOperation(context.Background(), pageOperation)
							}
							return readErr
						}
						payload = readPayload
					}

					if isAllZeroBytes(payload) {
						offset += chunkLength
						continue
					}
					if *writer == nil {
						openedReplicas, openErr := openTransitionTargetReplicas(ctx, volumeID, transition, currentReplicas, replicaClients, targetReplicaSet, gatewayID, hostID)
						if openErr != nil {
							if trackPageBatches {
								pageOperation.State = metadata.MutationOperationFailed
								pageOperation.LastUpdatedAtUnix = s.now().Unix()
								pageOperation.ErrorMessage = openErr.Error()
								_ = s.store.PutMutationOperation(context.Background(), pageOperation)
							}
							return openErr
						}
						*targetReplicas = openedReplicas
						*writer = replication.NewStrictRemoteReplicaWriter(*targetReplicas)
					}
					writeResult, err := (*writer).WriteExtent(ctx, writePlan, replication.ReplicaWriteRequest{
						RequestID:      fmt.Sprintf("repair-%s-%020d-%020d", transition.PlacementRef, mapping.ExtentID, offset),
						VolumeID:       volumeID,
						AttachmentID:   "maintenance-" + transition.TargetReplicaSetID,
						Generation:     1,
						IdempotencyKey: fmt.Sprintf("repair-%s-%020d-%020d", transition.PlacementRef, mapping.ExtentID, offset),
						OffsetBytes:    offset,
						LengthBytes:    chunkLength,
						Data:           payload,
					})
					if err != nil {
						if trackPageBatches {
							pageOperation.State = metadata.MutationOperationFailed
							pageOperation.LastUpdatedAtUnix = s.now().Unix()
							pageOperation.ErrorMessage = err.Error()
							_ = s.store.PutMutationOperation(context.Background(), pageOperation)
						}
						return err
					}
					if len(writeResult.AckedReplicaIDs) != len(writePlan.WriteTargets) {
						err := fmt.Errorf("transition %q extent %d acked replicas=%d want=%d", transition.PlacementRef, mapping.ExtentID, len(writeResult.AckedReplicaIDs), len(writePlan.WriteTargets))
						if trackPageBatches {
							pageOperation.State = metadata.MutationOperationFailed
							pageOperation.LastUpdatedAtUnix = s.now().Unix()
							pageOperation.ErrorMessage = err.Error()
							_ = s.store.PutMutationOperation(context.Background(), pageOperation)
						}
						return err
					}
					offset += chunkLength
				}
			}
			if trackPageBatches {
				pageOperation.State = metadata.MutationOperationCommitted
				pageOperation.CompletedPageNos = unionSortedUint64s(pageOperation.CompletedPageNos, batch.ActivePages)
				pageOperation.LastUpdatedAtUnix = s.now().Unix()
				pageOperation.ErrorMessage = ""
				if err := s.store.PutMutationOperation(ctx, pageOperation); err != nil {
					return err
				}
				if err := markTransitionPagesCompleted(ctx, s.store, &operation, completedPageSet, batch.ActivePages, s.now); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func openTransitionTargetReplicas(ctx context.Context, volumeID string, transition metadata.PlacementTransitionRecord, currentReplicas map[string]replication.RemoteReplica, replicaClients map[string]service.SBSClient, targetReplicaSet metadata.ReplicaSetState, gatewayID, hostID string) (map[string]replication.RemoteReplica, error) {
	targetClients := subsetReplicaClients(replicaClients, targetReplicaSet)
	overlapReplicas := make(map[string]replication.RemoteReplica)
	for replicaID, replica := range currentReplicas {
		if _, ok := targetClients[replicaID]; !ok {
			continue
		}
		overlapReplicas[replicaID] = replica
		delete(targetClients, replicaID)
	}
	targetReplicas, err := replication.OpenReplicaSessions(ctx, targetClients, replication.OpenReplicaSessionsRequest{
		VolumeID:      volumeID,
		GatewayID:     gatewayID,
		HostID:        hostID,
		AttachmentID:  "maintenance-" + transition.TargetReplicaSetID,
		Generation:    1,
		SessionPrefix: "repair-target-" + transition.TargetReplicaSetID,
		AccessMode:    service.SBSAccessModeExclusiveWriter,
	})
	if err != nil {
		return nil, err
	}
	for replicaID, replica := range overlapReplicas {
		targetReplicas[replicaID] = replica
	}
	return targetReplicas, nil
}

func closeRemoteReplicas(ctx context.Context, replicas map[string]replication.RemoteReplica) {
	for _, replica := range replicas {
		_, _ = replica.Client.CloseVolume(ctx, &service.CloseVolumeRequest{
			VolumeID:     replica.VolumeID,
			VolumeHandle: replica.VolumeHandle,
			Context: service.SBSRequestContext{
				RequestID:    fmt.Sprintf("cluster-close-%s-%s", replica.VolumeID, replica.ReplicaID),
				GatewayID:    replica.GatewayID,
				HostID:       replica.HostID,
				SessionID:    replica.SessionID,
				AttachmentID: replica.AttachmentID,
				Generation:   replica.Generation,
			},
		})
	}
}

func isAllZeroBytes(data []byte) bool {
	for _, b := range data {
		if b != 0 {
			return false
		}
	}
	return true
}

func minUint64(a, b uint64) uint64 {
	if a < b {
		return a
	}
	return b
}

func maxUint64(a, b uint64) uint64 {
	if a > b {
		return a
	}
	return b
}

func subsetReplicaClients(all map[string]service.SBSClient, replicaSet metadata.ReplicaSetState) map[string]service.SBSClient {
	out := make(map[string]service.SBSClient, len(replicaSet.Replicas))
	for _, replica := range replicaSet.Replicas {
		if client, ok := all[replica.ReplicaID]; ok {
			out[replica.ReplicaID] = client
			continue
		}
		if client, ok := all[replica.NodeID]; ok {
			out[replica.ReplicaID] = client
		}
	}
	return out
}

func uniqueSortedRetiredChunks(values []uint64) []uint64 {
	if len(values) == 0 {
		return nil
	}
	seen := make(map[uint64]struct{}, len(values))
	out := make([]uint64, 0, len(values))
	for _, value := range values {
		if value == 0 {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

func uniqueSortedExtentIDsFromMappings(mappings []metadata.ExtentMappingRecord) []uint64 {
	if len(mappings) == 0 {
		return nil
	}
	seen := make(map[uint64]struct{}, len(mappings))
	out := make([]uint64, 0, len(mappings))
	for _, mapping := range mappings {
		if mapping.ExtentID == 0 {
			continue
		}
		if _, ok := seen[mapping.ExtentID]; ok {
			continue
		}
		seen[mapping.ExtentID] = struct{}{}
		out = append(out, mapping.ExtentID)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

func uniqueSortedPageNosFromMappings(mappings []metadata.ExtentMappingRecord, chunkSizeBytes, pageBytes uint32) []uint64 {
	if len(mappings) == 0 || chunkSizeBytes == 0 || pageBytes == 0 {
		return nil
	}
	chunksPerPage := uint64(pageBytes / chunkSizeBytes)
	if chunksPerPage == 0 {
		return nil
	}
	seen := make(map[uint64]struct{})
	out := make([]uint64, 0, len(mappings))
	for _, mapping := range mappings {
		if mapping.LengthBytes == 0 {
			continue
		}
		startChunk := mapping.LogicalOffset / uint64(chunkSizeBytes)
		endChunk := (mapping.LogicalOffset + mapping.LengthBytes - 1) / uint64(chunkSizeBytes)
		startPage := startChunk / chunksPerPage
		endPage := endChunk / chunksPerPage
		for pageNo := startPage; pageNo <= endPage; pageNo++ {
			if _, ok := seen[pageNo]; ok {
				continue
			}
			seen[pageNo] = struct{}{}
			out = append(out, pageNo)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

func uniqueSortedUint64s(values []uint64) []uint64 {
	if len(values) == 0 {
		return nil
	}
	seen := make(map[uint64]struct{}, len(values))
	out := make([]uint64, 0, len(values))
	for _, value := range values {
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

func unionSortedUint64s(left, right []uint64) []uint64 {
	combined := append(append([]uint64(nil), left...), right...)
	return uniqueSortedUint64s(combined)
}

func resolvedLogicalChunkAllocationState(pages []metadata.ResolvedAllocationPage, logicalChunk uint64) (covered bool, zero bool, physicalChunkID uint64) {
	for _, page := range pages {
		if logicalChunk < page.RangeStartChunk || logicalChunk >= page.RangeEndChunk {
			continue
		}
		for _, extent := range page.Page.Extents {
			start := extent.LogicalChunkStart
			end := start + uint64(extent.ChunkCount)
			if logicalChunk < start || logicalChunk >= end {
				continue
			}
			if extent.Kind == metadata.AllocationKindZero {
				return true, true, 0
			}
			if extent.Kind == metadata.AllocationKindData {
				return true, false, extent.PhysicalChunkStart + (logicalChunk - start)
			}
			return true, false, 0
		}
		return false, false, 0
	}
	return false, false, 0
}

func buildTransitionReadPlan(mapping metadata.ExtentMappingRecord, replicaSet metadata.ReplicaSetState, allocationPages []metadata.ResolvedAllocationPage, chunkSizeBytes uint32) (replication.ExtentReadPlan, error) {
	primary, fallbacks, err := buildReplicaReadTargets(replicaSet)
	if err != nil {
		return replication.ExtentReadPlan{}, err
	}
	return replication.ExtentReadPlan{
		Extent:            mapping,
		PlacementRef:      mapping.PlacementRef,
		ReplicaSetID:      replicaSet.ReplicaSetID,
		Preferred:         primary,
		Fallbacks:         fallbacks,
		ReplicaSetEpoch:   replicaSet.Epoch,
		CommittedRevision: mapping.Revision,
		AllocationPages:   allocationPages,
		ChunkSizeBytes:    chunkSizeBytes,
	}, nil
}

func buildTransitionWritePlan(mapping metadata.ExtentMappingRecord, replicaSet metadata.ReplicaSetState, allocationPages []metadata.ResolvedAllocationPage, chunkSizeBytes uint32) (replication.ExtentWritePlan, error) {
	primary, targets, err := buildReplicaWriteTargets(replicaSet)
	if err != nil {
		return replication.ExtentWritePlan{}, err
	}
	return replication.ExtentWritePlan{
		Extent:           mapping,
		PlacementRef:     replicaSet.PlacementRef,
		ReplicaSetID:     replicaSet.ReplicaSetID,
		Primary:          primary,
		WriteTargets:     targets,
		RequiredAcks:     uint32(len(targets)),
		ReplicaSetEpoch:  replicaSet.Epoch,
		MetadataRevision: mapping.Revision,
		AllocationPages:  allocationPages,
		ChunkSizeBytes:   chunkSizeBytes,
	}, nil
}

func (s *Service) loadTransitionAllocationView(ctx context.Context, volumeID string) (transitionAllocationView, error) {
	nativePages, err := s.store.ListAllocationPages(ctx, volumeID)
	if err != nil {
		return transitionAllocationView{}, err
	}
	if len(nativePages) > 0 {
		view, err := newTransitionAllocationView(volumeID, nativePages)
		if err != nil {
			return transitionAllocationView{}, err
		}
		view.authoritativeNative = true
		return view, nil
	}
	spec, specErr := s.store.GetVolumeSpec(ctx, volumeID)
	if specErr == nil && spec.ExtentPageBytes != 0 && spec.ChunkSizeBytes != 0 && spec.ExtentPageBytes%spec.ChunkSizeBytes == 0 {
		pages, err := s.store.ListCompatibleAllocationPages(ctx, volumeID, spec.ExtentPageBytes, spec.ChunkSizeBytes)
		if err != nil {
			return transitionAllocationView{}, err
		}
		if len(pages) == 0 {
			return transitionAllocationView{}, nil
		}
		return newTransitionAllocationView(volumeID, pages)
	}
	return transitionAllocationView{}, nil
}

func (s *Service) loadTransitionReadViewAllocationSets(ctx context.Context, volumeID string, liveView transitionAllocationView) ([]transitionReadViewAllocationSet, error) {
	if !liveView.enabled {
		return nil, nil
	}
	out := make([]transitionReadViewAllocationSet, 0)
	if snapshotStore, ok := s.store.(transitionSnapshotReadViewStore); ok && snapshotStore != nil {
		snapshots, err := snapshotStore.ListSnapshotRecords(ctx, volumeID, false)
		if err != nil {
			return nil, err
		}
		for _, snapshot := range snapshots {
			switch snapshot.State {
			case metadata.SnapshotStateCreating, metadata.SnapshotStateAvailable, metadata.SnapshotStateDeleting:
			default:
				continue
			}
			pages, err := snapshotStore.ListSnapshotAllocationPages(ctx, snapshot.SnapshotID)
			if err != nil {
				return nil, err
			}
			if transitionPagesMatchGeometry(pages, liveView.pageBytes, liveView.chunkSizeBytes) {
				out = append(out, transitionReadViewAllocationSet{RootID: snapshot.SnapshotID, Pages: pages})
			}
		}
	}
	if cloneStore, ok := s.store.(transitionCloneReadViewStore); ok && cloneStore != nil {
		clones, err := cloneStore.ListCloneRecords(ctx, "", volumeID, false)
		if err != nil {
			return nil, err
		}
		for _, clone := range clones {
			switch clone.State {
			case metadata.CloneStateAvailable, metadata.CloneStateMaterializing:
			default:
				continue
			}
			pages, err := cloneStore.ListCloneDeltaAllocationPages(ctx, clone.CloneID)
			if err != nil {
				return nil, err
			}
			if transitionPagesMatchGeometry(pages, liveView.pageBytes, liveView.chunkSizeBytes) {
				out = append(out, transitionReadViewAllocationSet{RootID: clone.CloneID, Pages: pages})
			}
		}
	}
	return out, nil
}

func transitionPagesMatchGeometry(pages []metadata.AllocationPageRecord, pageBytes, chunkSizeBytes uint32) bool {
	if len(pages) == 0 {
		return false
	}
	for _, page := range pages {
		if page.PageBytes != pageBytes || page.ChunkSizeBytes != chunkSizeBytes {
			return false
		}
	}
	return true
}

func transitionReadViewDataPresentForMapping(volumeID string, mapping metadata.ExtentMappingRecord, liveView transitionAllocationView, readViewSets []transitionReadViewAllocationSet) (bool, error) {
	for _, readViewSet := range readViewSets {
		readView, err := newTransitionAllocationView(volumeID, readViewSet.Pages)
		if err != nil {
			return false, err
		}
		if readView.pageBytes != liveView.pageBytes || readView.chunkSizeBytes != liveView.chunkSizeBytes {
			continue
		}
		segments, err := readView.segmentsForMapping(mapping)
		if err != nil {
			return false, err
		}
		if segmentsContainData(segments) {
			return true, nil
		}
	}
	return false, nil
}

func newTransitionAllocationView(volumeID string, pages []metadata.AllocationPageRecord) (transitionAllocationView, error) {
	view := transitionAllocationView{
		enabled:        true,
		volumeID:       volumeID,
		pageBytes:      pages[0].PageBytes,
		chunkSizeBytes: pages[0].ChunkSizeBytes,
		pagesByNo:      make(map[uint64]metadata.AllocationPageRecord, len(pages)),
	}
	if view.pageBytes == 0 || view.chunkSizeBytes == 0 || view.pageBytes%view.chunkSizeBytes != 0 {
		return transitionAllocationView{}, fmt.Errorf("invalid allocation geometry for volume %q: page_bytes=%d chunk_size_bytes=%d", volumeID, view.pageBytes, view.chunkSizeBytes)
	}
	for _, page := range pages {
		if page.PageBytes != view.pageBytes || page.ChunkSizeBytes != view.chunkSizeBytes {
			return transitionAllocationView{}, fmt.Errorf("inconsistent allocation geometry for volume %q", volumeID)
		}
		view.pagesByNo[page.PageNo] = page
	}
	return view, nil
}

func (s *Service) materializeLegacyAllocationView(ctx context.Context, view transitionAllocationView, mappingsByExtentID map[uint64]metadata.ExtentMappingRecord, revision uint64) error {
	pageNos := make([]uint64, 0, len(view.pagesByNo))
	for pageNo := range view.pagesByNo {
		pageNos = append(pageNos, pageNo)
	}
	sort.Slice(pageNos, func(i, j int) bool { return pageNos[i] < pageNos[j] })
	pages := make([]metadata.AllocationPageRecord, 0, len(pageNos))
	for _, pageNo := range pageNos {
		pages = append(pages, view.pagesByNo[pageNo])
	}
	extentIDs := make([]uint64, 0, len(mappingsByExtentID))
	for extentID := range mappingsByExtentID {
		extentIDs = append(extentIDs, extentID)
	}
	sort.Slice(extentIDs, func(i, j int) bool { return extentIDs[i] < extentIDs[j] })
	normalizeExtentIDs := make([]uint64, 0, len(extentIDs))
	for _, extentID := range extentIDs {
		mapping := mappingsByExtentID[extentID]
		if mapping.ChunkID == 0 && mapping.Revision >= revision {
			continue
		}
		normalizeExtentIDs = append(normalizeExtentIDs, extentID)
	}
	if err := s.placementApply.ApplyPlacementChanges(ctx, metadata.PlacementApplyRequest{
		VolumeID:           view.volumeID,
		CommittedRevision:  revision,
		AllocationPages:    pages,
		NormalizeExtentIDs: normalizeExtentIDs,
	}); err != nil {
		return err
	}
	return nil
}

func (v transitionAllocationView) allocationPagesForMapping(mapping metadata.ExtentMappingRecord) []metadata.ResolvedAllocationPage {
	if !v.enabled || mapping.LengthBytes == 0 {
		return nil
	}
	chunkSize := uint64(v.chunkSizeBytes)
	chunksPerPage := uint64(v.pageBytes / v.chunkSizeBytes)
	startChunk := mapping.LogicalOffset / chunkSize
	endChunk := (mapping.LogicalOffset + mapping.LengthBytes - 1) / chunkSize
	startPage := startChunk / chunksPerPage
	endPage := endChunk / chunksPerPage
	out := make([]metadata.ResolvedAllocationPage, 0, endPage-startPage+1)
	for pageNo := startPage; pageNo <= endPage; pageNo++ {
		page, ok := v.pagesByNo[pageNo]
		if !ok {
			page = zeroTransitionAllocationPage(mapping.VolumeID, pageNo, v.pageBytes, v.chunkSizeBytes)
		}
		pageStartChunk := pageNo * chunksPerPage
		pageEndChunk := pageStartChunk + chunksPerPage
		rangeStartChunk := maxUint64(startChunk, pageStartChunk)
		rangeEndChunk := minUint64(endChunk+1, pageEndChunk)
		out = append(out, metadata.ResolvedAllocationPage{
			Page:            page,
			RangeStartChunk: rangeStartChunk,
			RangeEndChunk:   rangeEndChunk,
			CoversWholePage: rangeStartChunk == pageStartChunk && rangeEndChunk == pageEndChunk,
		})
	}
	return out
}

func (v transitionAllocationView) segmentsForMapping(mapping metadata.ExtentMappingRecord) ([]transitionCopySegment, error) {
	if mapping.LengthBytes == 0 {
		return nil, nil
	}
	if !v.enabled {
		return []transitionCopySegment{{
			OffsetBytes: mapping.LogicalOffset,
			LengthBytes: mapping.LengthBytes,
			Zero:        mapping.ChunkID == 0,
		}}, nil
	}
	pages := v.allocationPagesForMapping(mapping)
	if len(pages) == 0 {
		return nil, nil
	}

	chunkSize := uint64(v.chunkSizeBytes)
	startChunk := mapping.LogicalOffset / chunkSize
	endChunk := (mapping.LogicalOffset + mapping.LengthBytes - 1) / chunkSize
	out := make([]transitionCopySegment, 0, endChunk-startChunk+1)
	var current transitionCopySegment
	haveCurrent := false

	for logicalChunk := startChunk; logicalChunk <= endChunk; logicalChunk++ {
		zero, covered := zeroStateForLogicalChunk(pages, logicalChunk)
		if !covered {
			return nil, fmt.Errorf("allocation page coverage missing for volume %q extent %d logical_chunk=%d", mapping.VolumeID, mapping.ExtentID, logicalChunk)
		}
		chunkStart := logicalChunk * chunkSize
		chunkEnd := chunkStart + chunkSize
		segmentStart := maxUint64(mapping.LogicalOffset, chunkStart)
		segmentEnd := minUint64(mapping.LogicalOffset+mapping.LengthBytes, chunkEnd)
		if segmentStart >= segmentEnd {
			continue
		}
		if haveCurrent && current.Zero == zero && current.OffsetBytes+current.LengthBytes == segmentStart {
			current.LengthBytes += segmentEnd - segmentStart
			continue
		}
		if haveCurrent {
			out = append(out, current)
		}
		current = transitionCopySegment{
			OffsetBytes: segmentStart,
			LengthBytes: segmentEnd - segmentStart,
			Zero:        zero,
		}
		haveCurrent = true
	}
	if haveCurrent {
		out = append(out, current)
	}
	return out, nil
}

func prioritizeTransitionSegmentsForRecentPages(segments []transitionCopySegment, recentPages map[uint64]struct{}, pageBytes uint32) []transitionCopySegment {
	return prioritizeTransitionSegmentsForPageSet(segments, recentPages, pageBytes)
}

func prioritizeTransitionSegmentsForPageSet(segments []transitionCopySegment, pageSet map[uint64]struct{}, pageBytes uint32) []transitionCopySegment {
	if len(segments) == 0 || len(pageSet) == 0 || pageBytes == 0 {
		return segments
	}
	recent := make([]transitionCopySegment, 0, len(segments))
	remaining := make([]transitionCopySegment, 0, len(segments))
	for _, segment := range segments {
		pieces := splitTransitionSegmentByPages(segment, pageSet, pageBytes)
		for _, piece := range pieces {
			if piece.LengthBytes == 0 {
				continue
			}
			if piece.Zero {
				remaining = append(remaining, piece)
				continue
			}
			if segmentTouchesRecentPage(piece, pageSet, pageBytes) {
				recent = append(recent, piece)
			} else {
				remaining = append(remaining, piece)
			}
		}
	}
	return append(recent, remaining...)
}

func splitTransitionSegmentByPages(segment transitionCopySegment, recentPages map[uint64]struct{}, pageBytes uint32) []transitionCopySegment {
	if len(recentPages) == 0 {
		return []transitionCopySegment{segment}
	}
	return splitTransitionSegmentIntoPages(segment, pageBytes)
}

func splitTransitionSegmentIntoPages(segment transitionCopySegment, pageBytes uint32) []transitionCopySegment {
	if segment.LengthBytes == 0 || pageBytes == 0 {
		return []transitionCopySegment{segment}
	}
	pageBytes64 := uint64(pageBytes)
	start := segment.OffsetBytes
	end := segment.OffsetBytes + segment.LengthBytes
	out := make([]transitionCopySegment, 0, 2)
	for start < end {
		pageNo := start / pageBytes64
		pageEnd := (pageNo + 1) * pageBytes64
		if pageEnd > end {
			pageEnd = end
		}
		out = append(out, transitionCopySegment{
			OffsetBytes: start,
			LengthBytes: pageEnd - start,
			Zero:        segment.Zero,
		})
		start = pageEnd
	}
	return out
}

func filterTransitionSegmentsByCompletedPages(segments []transitionCopySegment, completedPages map[uint64]struct{}, pageBytes uint32) []transitionCopySegment {
	if len(segments) == 0 || len(completedPages) == 0 || pageBytes == 0 {
		return segments
	}
	filtered := make([]transitionCopySegment, 0, len(segments))
	for _, segment := range segments {
		for _, piece := range splitTransitionSegmentIntoPages(segment, pageBytes) {
			pageNos := pageNosForSegment(piece, pageBytes)
			if len(pageNos) == 1 {
				if _, ok := completedPages[pageNos[0]]; ok {
					continue
				}
			}
			filtered = append(filtered, piece)
		}
	}
	return filtered
}

func pageNosForSegment(segment transitionCopySegment, pageBytes uint32) []uint64 {
	if segment.LengthBytes == 0 || pageBytes == 0 {
		return nil
	}
	pageBytes64 := uint64(pageBytes)
	startPage := segment.OffsetBytes / pageBytes64
	endPage := (segment.OffsetBytes + segment.LengthBytes - 1) / pageBytes64
	out := make([]uint64, 0, endPage-startPage+1)
	for pageNo := startPage; pageNo <= endPage; pageNo++ {
		out = append(out, pageNo)
	}
	return out
}

func transitionBatchPageNosForSegment(mapping metadata.ExtentMappingRecord, segment transitionCopySegment, pageBytes uint32) []uint64 {
	if pageBytes == 0 {
		if mapping.ExtentID == 0 {
			return nil
		}
		return []uint64{mapping.ExtentID}
	}
	return pageNosForSegment(segment, pageBytes)
}

func markTransitionPagesCompleted(ctx context.Context, store metadataStore, operation *metadata.MutationOperationRecord, completedPages map[uint64]struct{}, pageNos []uint64, now func() time.Time) error {
	if operation == nil || len(pageNos) == 0 {
		return nil
	}
	changed := false
	for _, pageNo := range pageNos {
		if _, ok := completedPages[pageNo]; ok {
			continue
		}
		completedPages[pageNo] = struct{}{}
		changed = true
	}
	if !changed {
		return nil
	}
	operation.CompletedPageNos = uniqueSortedUint64s(append(operation.CompletedPageNos, pageNos...))
	operation.LastUpdatedAtUnix = now().Unix()
	return store.PutMutationOperation(ctx, *operation)
}

func beginTransitionPageBatchMutationOperation(ctx context.Context, store metadataStore, transition metadata.PlacementTransitionRecord, parentOperation metadata.MutationOperationRecord, extentID uint64, pageNos []uint64, now time.Time) (metadata.MutationOperationRecord, bool, error) {
	if len(pageNos) == 0 {
		return metadata.MutationOperationRecord{}, false, nil
	}
	startPageNo := pageNos[0]
	endPageNo := pageNos[len(pageNos)-1]
	operationID := transitionPageRangeBatchMutationOperationID(transition, startPageNo, endPageNo)
	operation, err := store.GetMutationOperation(ctx, transition.VolumeID, operationID)
	switch {
	case err == nil:
		operation.OperationID = operationID
		operation.VolumeID = transition.VolumeID
		operation.Kind = "transition_batch"
		operation.IdempotencyKey = parentOperation.OperationID
		operation.AffectedExtentIDs = []uint64{extentID}
		operation.AffectedPageNos = append([]uint64(nil), pageNos...)
		operation.LastUpdatedAtUnix = now.Unix()
		switch operation.State {
		case metadata.MutationOperationCommitted:
			return operation, true, nil
		default:
			operation.State = metadata.MutationOperationRunning
			operation.ErrorMessage = ""
			if operation.StartedAtUnix == 0 {
				operation.StartedAtUnix = now.Unix()
			}
		}
	case errors.Is(err, metadata.ErrNotFound):
		operation = metadata.MutationOperationRecord{
			OperationID:       operationID,
			VolumeID:          transition.VolumeID,
			Kind:              "transition_batch",
			State:             metadata.MutationOperationRunning,
			IdempotencyKey:    parentOperation.OperationID,
			AffectedExtentIDs: []uint64{extentID},
			AffectedPageNos:   append([]uint64(nil), pageNos...),
			StartedAtUnix:     now.Unix(),
			LastUpdatedAtUnix: now.Unix(),
		}
	default:
		return metadata.MutationOperationRecord{}, false, err
	}
	if err := store.PutMutationOperation(ctx, operation); err != nil {
		return metadata.MutationOperationRecord{}, false, err
	}
	return operation, false, nil
}

func buildTransitionPageBatches(mapping metadata.ExtentMappingRecord, pageSegments []transitionCopySegment, pageBytes uint32, maxPages uint64, retryWindows [][]uint64, existingWindows [][]uint64) []transitionPageBatch {
	if len(pageSegments) == 0 {
		return nil
	}
	windowsByPage := make(map[uint64][]uint64)
	orderedWindowKeys := make([]string, 0, len(retryWindows)+len(existingWindows))
	registerWindow := func(window []uint64, overwrite bool) {
		window = uniqueSortedUint64s(window)
		if len(window) == 0 {
			return
		}
		key := transitionPageWindowKey(window)
		if key == "" {
			return
		}
		for _, existingKey := range orderedWindowKeys {
			if existingKey == key {
				goto pages
			}
		}
		orderedWindowKeys = append(orderedWindowKeys, key)
	pages:
		for _, pageNo := range window {
			if !overwrite {
				if _, ok := windowsByPage[pageNo]; ok {
					continue
				}
			}
			windowsByPage[pageNo] = window
		}
	}
	for _, window := range existingWindows {
		registerWindow(window, false)
	}
	for _, window := range retryWindows {
		registerWindow(window, true)
	}
	if maxPages == 0 {
		maxPages = DefaultTransitionBatchMaxPages
	}
	out := make([]transitionPageBatch, 0, len(pageSegments))
	existingBatches := make(map[string]*transitionPageBatch, len(retryWindows)+len(existingWindows))
	var current transitionPageBatch
	haveCurrent := false
	flushCurrent := func() {
		if !haveCurrent {
			return
		}
		current.PageNos = uniqueSortedUint64s(current.PageNos)
		current.ActivePages = uniqueSortedUint64s(current.ActivePages)
		out = append(out, current)
		haveCurrent = false
		current = transitionPageBatch{}
	}
	for _, segment := range pageSegments {
		pageNos := transitionBatchPageNosForSegment(mapping, segment, pageBytes)
		if len(pageNos) != 1 {
			flushCurrent()
			out = append(out, transitionPageBatch{
				PageNos:     pageNos,
				ActivePages: pageNos,
				Segments:    []transitionCopySegment{segment},
			})
			continue
		}
		pageNo := pageNos[0]
		if window, ok := windowsByPage[pageNo]; ok {
			flushCurrent()
			key := transitionPageWindowKey(window)
			batch := existingBatches[key]
			if batch == nil {
				batch = &transitionPageBatch{PageNos: append([]uint64(nil), window...)}
				existingBatches[key] = batch
			}
			batch.ActivePages = append(batch.ActivePages, pageNo)
			batch.Segments = append(batch.Segments, segment)
			continue
		}
		if haveCurrent &&
			len(current.PageNos) > 0 &&
			current.PageNos[len(current.PageNos)-1]+1 == pageNo &&
			uint64(len(current.PageNos)) < maxPages {
			current.PageNos = append(current.PageNos, pageNo)
			current.ActivePages = append(current.ActivePages, pageNo)
			current.Segments = append(current.Segments, segment)
			continue
		}
		flushCurrent()
		current = transitionPageBatch{
			PageNos:     []uint64{pageNo},
			ActivePages: []uint64{pageNo},
			Segments:    []transitionCopySegment{segment},
		}
		haveCurrent = true
	}
	flushCurrent()
	for _, key := range orderedWindowKeys {
		batch := existingBatches[key]
		if batch == nil || len(batch.ActivePages) == 0 || len(batch.Segments) == 0 {
			continue
		}
		batch.PageNos = uniqueSortedUint64s(batch.PageNos)
		batch.ActivePages = uniqueSortedUint64s(batch.ActivePages)
		out = append(out, *batch)
	}
	sort.SliceStable(out, func(i, j int) bool {
		leftStart := uint64(0)
		rightStart := uint64(0)
		if len(out[i].ActivePages) > 0 {
			leftStart = out[i].ActivePages[0]
		} else if len(out[i].PageNos) > 0 {
			leftStart = out[i].PageNos[0]
		}
		if len(out[j].ActivePages) > 0 {
			rightStart = out[j].ActivePages[0]
		} else if len(out[j].PageNos) > 0 {
			rightStart = out[j].PageNos[0]
		}
		return leftStart < rightStart
	})
	return out
}

func prioritizeTransitionPageBatches(batches []transitionPageBatch, failedPages map[uint64]struct{}, retryWindows [][]uint64, recentPages map[uint64]struct{}) {
	if len(batches) <= 1 {
		return
	}
	retryWindowSizeByPage := make(map[uint64]int)
	for _, window := range retryWindows {
		window = uniqueSortedUint64s(window)
		if len(window) == 0 {
			continue
		}
		for _, pageNo := range window {
			if existing, ok := retryWindowSizeByPage[pageNo]; !ok || len(window) < existing {
				retryWindowSizeByPage[pageNo] = len(window)
			}
		}
	}
	sort.SliceStable(batches, func(i, j int) bool {
		leftFailed := batchTouchesPageSet(batches[i], failedPages)
		rightFailed := batchTouchesPageSet(batches[j], failedPages)
		if leftFailed != rightFailed {
			return leftFailed
		}
		leftRetry, leftRetrySize := batchRetryWindowPriority(batches[i], retryWindowSizeByPage)
		rightRetry, rightRetrySize := batchRetryWindowPriority(batches[j], retryWindowSizeByPage)
		if leftRetry != rightRetry {
			return leftRetry
		}
		if leftRetry && rightRetry && leftRetrySize != rightRetrySize {
			return leftRetrySize < rightRetrySize
		}
		leftRecent := batchTouchesPageSet(batches[i], recentPages)
		rightRecent := batchTouchesPageSet(batches[j], recentPages)
		if leftRecent != rightRecent {
			return leftRecent
		}
		if len(batches[i].ActivePages) != len(batches[j].ActivePages) {
			return len(batches[i].ActivePages) < len(batches[j].ActivePages)
		}
		leftBytes := batchLengthBytes(batches[i])
		rightBytes := batchLengthBytes(batches[j])
		if leftBytes != rightBytes {
			return leftBytes < rightBytes
		}
		leftStart := uint64(0)
		rightStart := uint64(0)
		if len(batches[i].ActivePages) > 0 {
			leftStart = batches[i].ActivePages[0]
		} else if len(batches[i].PageNos) > 0 {
			leftStart = batches[i].PageNos[0]
		}
		if len(batches[j].ActivePages) > 0 {
			rightStart = batches[j].ActivePages[0]
		} else if len(batches[j].PageNos) > 0 {
			rightStart = batches[j].PageNos[0]
		}
		return leftStart < rightStart
	})
}

func batchRetryWindowPriority(batch transitionPageBatch, retryWindowSizeByPage map[uint64]int) (bool, int) {
	if len(retryWindowSizeByPage) == 0 {
		return false, 0
	}
	bestSize := 0
	for _, pageNo := range batch.ActivePages {
		size, ok := retryWindowSizeByPage[pageNo]
		if !ok {
			continue
		}
		if bestSize == 0 || size < bestSize {
			bestSize = size
		}
	}
	return bestSize > 0, bestSize
}

func batchTouchesPageSet(batch transitionPageBatch, pageSet map[uint64]struct{}) bool {
	if len(pageSet) == 0 {
		return false
	}
	for _, pageNo := range batch.ActivePages {
		if _, ok := pageSet[pageNo]; ok {
			return true
		}
	}
	return false
}

func batchLengthBytes(batch transitionPageBatch) uint64 {
	var total uint64
	for _, segment := range batch.Segments {
		total += segment.LengthBytes
	}
	return total
}

func transitionPageWindowKey(pageNos []uint64) string {
	if len(pageNos) == 0 {
		return ""
	}
	pageNos = uniqueSortedUint64s(pageNos)
	return fmt.Sprintf("%020d-%020d", pageNos[0], pageNos[len(pageNos)-1])
}

func (s *Service) loadExistingTransitionBatchWindows(ctx context.Context, volumeID, parentOperationID string) (map[uint64][][]uint64, error) {
	operations, err := s.store.ListMutationOperations(ctx, volumeID)
	if err != nil {
		return nil, err
	}
	out := make(map[uint64][][]uint64)
	seen := make(map[uint64]map[string]struct{})
	for _, operation := range operations {
		if operation.Kind != "transition_batch" || operation.IdempotencyKey != parentOperationID || len(operation.AffectedPageNos) == 0 {
			continue
		}
		pages := uniqueSortedUint64s(operation.AffectedPageNos)
		key := transitionPageWindowKey(pages)
		if key == "" {
			continue
		}
		for _, extentID := range operation.AffectedExtentIDs {
			extentSeen := seen[extentID]
			if extentSeen == nil {
				extentSeen = make(map[string]struct{})
				seen[extentID] = extentSeen
			}
			if _, ok := extentSeen[key]; ok {
				continue
			}
			extentSeen[key] = struct{}{}
			out[extentID] = append(out[extentID], append([]uint64(nil), pages...))
		}
	}
	for extentID := range out {
		sort.Slice(out[extentID], func(i, j int) bool {
			left := out[extentID][i]
			right := out[extentID][j]
			if len(left) == 0 || len(right) == 0 {
				return len(left) < len(right)
			}
			return left[0] < right[0]
		})
	}
	return out, nil
}

func (s *Service) loadRetryableTransitionBatchWindows(ctx context.Context, volumeID, parentOperationID string) (map[uint64][][]uint64, error) {
	operations, err := s.store.ListMutationOperations(ctx, volumeID)
	if err != nil {
		return nil, err
	}
	out := make(map[uint64][][]uint64)
	seen := make(map[uint64]map[string]struct{})
	for _, operation := range operations {
		if operation.Kind != "transition_batch" || operation.IdempotencyKey != parentOperationID || len(operation.AffectedPageNos) == 0 {
			continue
		}
		switch operation.State {
		case metadata.MutationOperationPending, metadata.MutationOperationRunning, metadata.MutationOperationFailed:
		default:
			continue
		}
		pages := uniqueSortedUint64s(operation.AffectedPageNos)
		key := transitionPageWindowKey(pages)
		if key == "" {
			continue
		}
		for _, extentID := range operation.AffectedExtentIDs {
			extentSeen := seen[extentID]
			if extentSeen == nil {
				extentSeen = make(map[string]struct{})
				seen[extentID] = extentSeen
			}
			if _, ok := extentSeen[key]; ok {
				continue
			}
			extentSeen[key] = struct{}{}
			out[extentID] = append(out[extentID], append([]uint64(nil), pages...))
		}
	}
	for extentID := range out {
		sort.Slice(out[extentID], func(i, j int) bool {
			left := out[extentID][i]
			right := out[extentID][j]
			if len(left) == 0 || len(right) == 0 {
				return len(left) < len(right)
			}
			return left[0] < right[0]
		})
	}
	return out, nil
}

func (s *Service) loadFailedTransitionBatchPages(ctx context.Context, volumeID, parentOperationID string) (map[uint64]map[uint64]struct{}, error) {
	operations, err := s.store.ListMutationOperations(ctx, volumeID)
	if err != nil {
		return nil, err
	}
	out := make(map[uint64]map[uint64]struct{})
	for _, operation := range operations {
		if operation.Kind != "transition_batch" || operation.State != metadata.MutationOperationFailed || operation.IdempotencyKey != parentOperationID {
			continue
		}
		pages := subtractPageNos(operation.AffectedPageNos, operation.CompletedPageNos)
		if len(pages) == 0 {
			continue
		}
		for _, extentID := range operation.AffectedExtentIDs {
			pageSet := out[extentID]
			if pageSet == nil {
				pageSet = make(map[uint64]struct{})
				out[extentID] = pageSet
			}
			for _, pageNo := range pages {
				pageSet[pageNo] = struct{}{}
			}
		}
	}
	return out, nil
}

func segmentTouchesRecentPage(segment transitionCopySegment, recentPages map[uint64]struct{}, pageBytes uint32) bool {
	if segment.LengthBytes == 0 || pageBytes == 0 {
		return false
	}
	pageBytes64 := uint64(pageBytes)
	startPage := segment.OffsetBytes / pageBytes64
	endPage := (segment.OffsetBytes + segment.LengthBytes - 1) / pageBytes64
	for pageNo := startPage; pageNo <= endPage; pageNo++ {
		if _, ok := recentPages[pageNo]; ok {
			return true
		}
	}
	return false
}

func coalesceTransitionSegments(in []transitionCopySegment) []transitionCopySegment {
	if len(in) <= 1 {
		return in
	}
	out := make([]transitionCopySegment, 0, len(in))
	current := in[0]
	for _, segment := range in[1:] {
		if current.Zero == segment.Zero && current.OffsetBytes+current.LengthBytes == segment.OffsetBytes {
			current.LengthBytes += segment.LengthBytes
			continue
		}
		out = append(out, current)
		current = segment
	}
	out = append(out, current)
	return out
}

func zeroStateForLogicalChunk(pages []metadata.ResolvedAllocationPage, logicalChunk uint64) (bool, bool) {
	for _, page := range pages {
		if logicalChunk < page.RangeStartChunk || logicalChunk >= page.RangeEndChunk {
			continue
		}
		for _, extent := range page.Page.Extents {
			start := extent.LogicalChunkStart
			end := start + uint64(extent.ChunkCount)
			if logicalChunk < start || logicalChunk >= end {
				continue
			}
			return extent.Kind == metadata.AllocationKindZero, true
		}
		return false, false
	}
	return false, false
}

func zeroTransitionAllocationPage(volumeID string, pageNo uint64, pageBytes, chunkSizeBytes uint32) metadata.AllocationPageRecord {
	chunksPerPage := uint32(pageBytes / chunkSizeBytes)
	return metadata.AllocationPageRecord{
		VolumeID:       volumeID,
		PageNo:         pageNo,
		PageBytes:      pageBytes,
		ChunkSizeBytes: chunkSizeBytes,
		Extents: []metadata.AllocationExtentRecord{
			{
				LogicalChunkStart:  pageNo * uint64(chunksPerPage),
				ChunkCount:         chunksPerPage,
				Kind:               metadata.AllocationKindZero,
				PhysicalChunkStart: 0,
			},
		},
	}
}

func segmentsContainData(segments []transitionCopySegment) bool {
	for _, segment := range segments {
		if !segment.Zero && segment.LengthBytes > 0 {
			return true
		}
	}
	return false
}

func buildReplicaWriteTargets(replicaSet metadata.ReplicaSetState) (replication.ReplicaTarget, []replication.ReplicaTarget, error) {
	if len(replicaSet.Replicas) == 0 {
		return replication.ReplicaTarget{}, nil, fmt.Errorf("replica set %q has no replicas", replicaSet.ReplicaSetID)
	}
	var primary replication.ReplicaTarget
	foundPrimary := false
	targets := make([]replication.ReplicaTarget, 0, len(replicaSet.Replicas))
	for _, replica := range replicaSet.Replicas {
		target := replication.ReplicaTarget{
			NodeID:        replica.NodeID,
			ReplicaID:     replica.ReplicaID,
			Role:          replica.Role,
			FailureDomain: replica.FailureDomain,
		}
		if replica.ReplicaID == replicaSet.PrimaryReplicaID || replica.Role == metadata.ReplicaRolePrimary {
			primary = target
			foundPrimary = true
		}
		targets = append(targets, target)
	}
	if !foundPrimary {
		return replication.ReplicaTarget{}, nil, fmt.Errorf("replica set %q has no primary", replicaSet.ReplicaSetID)
	}
	return primary, targets, nil
}

func buildReplicaReadTargets(replicaSet metadata.ReplicaSetState) (replication.ReplicaTarget, []replication.ReplicaTarget, error) {
	primary, targets, err := buildReplicaWriteTargets(replicaSet)
	if err != nil {
		return replication.ReplicaTarget{}, nil, err
	}
	fallbacks := make([]replication.ReplicaTarget, 0, len(targets))
	for _, target := range targets {
		if target.ReplicaID == primary.ReplicaID {
			continue
		}
		fallbacks = append(fallbacks, target)
	}
	return primary, fallbacks, nil
}

func (s *Service) selectRepairTarget(ctx context.Context, evaluated *EvaluatedExtent, replicaSets []metadata.ReplicaSetState) (string, bool, error) {
	return s.selectRepairTargetWithContext(ctx, nil, evaluated, replicaSets)
}

func (s *Service) selectRepairTargetWithContext(ctx context.Context, vc *volumeEvaluationContext, evaluated *EvaluatedExtent, replicaSets []metadata.ReplicaSetState) (string, bool, error) {
	for _, candidate := range replicaSets {
		if candidate.ReplicaSetID == evaluated.ReplicaSet.ReplicaSetID {
			continue
		}
		if candidate.PlacementRef == evaluated.Extent.PlacementRef {
			continue
		}
		var healthy int
		var err error
		if vc != nil {
			healthy, err = s.countHealthyReplicasWithContext(ctx, vc, candidate)
		} else {
			healthy, err = s.countHealthyReplicas(ctx, candidate)
		}
		if err != nil {
			return "", false, err
		}
		if healthy < int(candidate.WriteQuorum) {
			continue
		}
		return candidate.ReplicaSetID, true, nil
	}
	return "", false, nil
}

func (s *Service) planRepairTargetReplicaSetWithContext(ctx context.Context, vc *volumeEvaluationContext, evaluated *EvaluatedExtent) (metadata.ReplicaSetState, bool, error) {
	if evaluated.State != ExtentHealthDegradedWritable {
		return metadata.ReplicaSetState{}, false, nil
	}

	replaceNodeID := ""
	for _, replica := range evaluated.ReplicaSet.Replicas {
		node, err := s.nodeMembershipWithContext(ctx, vc, replica.NodeID)
		if err != nil {
			return metadata.ReplicaSetState{}, false, err
		}
		healthy := node.LifecycleState != metadata.NodeLifecycleRemoved && (node.HealthState == metadata.NodeHealthHealthy || node.HealthState == metadata.NodeHealthSuspect) && s.nodeStoreEligibleForReplica(ctx, vc, replica.NodeID)
		if healthy {
			continue
		}
		if replaceNodeID == "" {
			replaceNodeID = replica.NodeID
		}
	}
	if replaceNodeID == "" {
		return metadata.ReplicaSetState{}, false, nil
	}
	return s.planReplacementReplicaSetWithContext(ctx, vc, evaluated.Extent.PlacementRef, evaluated.ReplicaSet, replaceNodeID, "repair", evaluated)
}

func (s *Service) PlanReplacementReplicaSet(ctx context.Context, volumeID, placementRef string, current metadata.ReplicaSetState, replaceNodeID, reason string, evaluated *EvaluatedExtent) (metadata.ReplicaSetState, bool, error) {
	vc, err := s.loadVolumeEvaluationContext(ctx, volumeID)
	if err != nil {
		return metadata.ReplicaSetState{}, false, err
	}
	return s.planReplacementReplicaSetWithContext(ctx, vc, placementRef, current, replaceNodeID, reason, evaluated)
}

func (s *Service) planReplacementReplicaSetWithContext(ctx context.Context, vc *volumeEvaluationContext, placementRef string, current metadata.ReplicaSetState, replaceNodeID, reason string, evaluated *EvaluatedExtent) (metadata.ReplicaSetState, bool, error) {
	replaceIndex := -1
	currentNodes := make(map[string]struct{}, len(current.Replicas))
	usedZones := make(map[string]struct{}, len(current.Replicas))
	preferredZones := make([]string, 0, len(current.Replicas))
	preferredHosts := make([]string, 0, len(current.Replicas))
	for i, replica := range current.Replicas {
		currentNodes[replica.NodeID] = struct{}{}
		node, err := s.nodeMembershipWithContext(ctx, vc, replica.NodeID)
		if err != nil {
			return metadata.ReplicaSetState{}, false, err
		}
		healthy := node.LifecycleState != metadata.NodeLifecycleRemoved &&
			(node.HealthState == metadata.NodeHealthHealthy || node.HealthState == metadata.NodeHealthSuspect) &&
			s.nodeStoreEligibleForReplica(ctx, vc, replica.NodeID)
		if healthy {
			if node.Zone != "" {
				usedZones[node.Zone] = struct{}{}
			}
			if replica.ReplicaID == current.PrimaryReplicaID || replica.Role == metadata.ReplicaRolePrimary {
				if node.Zone != "" {
					preferredZones = append(preferredZones, node.Zone)
				}
				if node.Host != "" {
					preferredHosts = append(preferredHosts, node.Host)
				}
			}
		}
		if replica.NodeID == replaceNodeID {
			replaceIndex = i
		}
	}
	if replaceIndex == -1 {
		return metadata.ReplicaSetState{}, false, nil
	}
	for _, replica := range current.Replicas {
		node, err := s.nodeMembershipWithContext(ctx, vc, replica.NodeID)
		if err != nil {
			return metadata.ReplicaSetState{}, false, err
		}
		healthy := node.LifecycleState != metadata.NodeLifecycleRemoved &&
			(node.HealthState == metadata.NodeHealthHealthy || node.HealthState == metadata.NodeHealthSuspect) &&
			s.nodeStoreEligibleForReplica(ctx, vc, replica.NodeID)
		if !healthy || replica.ReplicaID == current.PrimaryReplicaID || replica.Role == metadata.ReplicaRolePrimary {
			continue
		}
		if node.Zone != "" {
			preferredZones = append(preferredZones, node.Zone)
		}
		if node.Host != "" {
			preferredHosts = append(preferredHosts, node.Host)
		}
	}
	candidates, err := s.store.ListNodeMemberships(ctx)
	if err != nil {
		return metadata.ReplicaSetState{}, false, err
	}
	sort.Slice(candidates, func(i, j int) bool { return candidates[i].NodeID < candidates[j].NodeID })
	requireNewZoneFirst := true
	if shouldPreferSourceLocalRepairTarget(evaluated) && (reason == "rebalance" || reason == "drain") {
		requireNewZoneFirst = false
	}
	replacement, ok := s.selectReplacementNode(ctx, vc, candidates, currentNodes, usedZones, preferredZones, preferredHosts, requireNewZoneFirst, evaluated)
	if !ok && !strictTopologyMode(vc.volume.TopologyMode) {
		replacement, ok = s.selectReplacementNode(ctx, vc, candidates, currentNodes, usedZones, preferredZones, preferredHosts, !requireNewZoneFirst, evaluated)
	}
	if !ok {
		return metadata.ReplicaSetState{}, false, nil
	}
	target := current
	target.ReplicaSetID, target.PlacementRef = buildTransitionDerivedIDs(current.ReplicaSetID, placementRef, reason, replaceNodeID)
	target.Epoch++
	target.Replicas = append([]metadata.ReplicaDescriptor(nil), current.Replicas...)
	wasPrimary := target.Replicas[replaceIndex].ReplicaID == target.PrimaryReplicaID
	target.Replicas[replaceIndex].NodeID = replacement.NodeID
	target.Replicas[replaceIndex].ReplicaID = fmt.Sprintf("%s-rep-%02d", target.ReplicaSetID, replaceIndex+1)
	target.Replicas[replaceIndex].FailureDomain = replacement.Zone
	if target.Replicas[replaceIndex].FailureDomain == "" {
		target.Replicas[replaceIndex].FailureDomain = replacement.Host
	}
	if target.Replicas[replaceIndex].FailureDomain == "" {
		target.Replicas[replaceIndex].FailureDomain = replacement.NodeID
	}
	if wasPrimary {
		target.PrimaryReplicaID = target.Replicas[replaceIndex].ReplicaID
	}
	if err := s.validateTransitionTargetReplicaSetEligibility(ctx, vc, current, target); err != nil {
		if isTransitionPreconditionError(err) {
			return metadata.ReplicaSetState{}, false, nil
		}
		return metadata.ReplicaSetState{}, false, err
	}
	return target, true, nil
}

func replacedNodeID(current, target metadata.ReplicaSetState) (string, bool) {
	currentCounts := make(map[string]int, len(current.Replicas))
	targetCounts := make(map[string]int, len(target.Replicas))
	for _, replica := range current.Replicas {
		currentCounts[replica.NodeID]++
	}
	for _, replica := range target.Replicas {
		targetCounts[replica.NodeID]++
	}
	replaceNodeID := ""
	for nodeID, count := range currentCounts {
		if targetCounts[nodeID] < count {
			if replaceNodeID != "" {
				return "", false
			}
			replaceNodeID = nodeID
		}
	}
	if replaceNodeID == "" {
		return "", false
	}
	return replaceNodeID, true
}

func (s *Service) nodeMembershipWithContext(ctx context.Context, vc *volumeEvaluationContext, nodeID string) (metadata.NodeMembershipRecord, error) {
	if vc != nil {
		if node, ok := vc.nodeByID[nodeID]; ok {
			return node, nil
		}
	}
	node, err := s.store.GetNodeMembership(ctx, nodeID)
	if err != nil {
		return metadata.NodeMembershipRecord{}, err
	}
	if vc != nil {
		vc.nodeByID[nodeID] = node
	}
	return node, nil
}

func (s *Service) nodeHealthDetailWithContext(ctx context.Context, vc *volumeEvaluationContext, nodeID string) (metadata.NodeHealthDetailRecord, error) {
	if vc != nil {
		if detail, ok := vc.nodeHealthDetailByID[nodeID]; ok {
			return detail, nil
		}
	}
	detail, err := s.store.GetNodeHealthDetail(ctx, nodeID)
	if err != nil {
		return metadata.NodeHealthDetailRecord{}, err
	}
	if vc != nil {
		vc.nodeHealthDetailByID[nodeID] = detail
	}
	return detail, nil
}

func (s *Service) nodeEligibleForNewPlacement(ctx context.Context, vc *volumeEvaluationContext, node metadata.NodeMembershipRecord) bool {
	if node.LifecycleState != metadata.NodeLifecycleActive {
		return false
	}
	if node.HealthState != metadata.NodeHealthHealthy && node.HealthState != metadata.NodeHealthSuspect {
		return false
	}
	detail, err := s.nodeHealthDetailWithContext(ctx, vc, node.NodeID)
	if err != nil {
		return err == metadata.ErrNotFound
	}
	if !detail.StorePlacementEligible() {
		return false
	}
	return detail.RecoveryEligibleAtUnix == 0 || detail.RecoveryEligibleAtUnix <= s.now().Unix()
}

func (s *Service) nodeStoreEligibleForReplica(ctx context.Context, vc *volumeEvaluationContext, nodeID string) bool {
	detail, err := s.nodeHealthDetailWithContext(ctx, vc, nodeID)
	if err != nil {
		return err == metadata.ErrNotFound
	}
	return detail.StorePlacementEligible()
}

func (s *Service) describeReplicaEligibility(ctx context.Context, vc *volumeEvaluationContext, currentNodes map[string]struct{}, replica metadata.ReplicaDescriptor) string {
	node, err := s.nodeMembershipWithContext(ctx, vc, replica.NodeID)
	if err != nil {
		return fmt.Sprintf("node=%s replica=%s membership_err=%q", replica.NodeID, replica.ReplicaID, err)
	}
	detail, err := s.nodeHealthDetailWithContext(ctx, vc, replica.NodeID)
	if err != nil && err != metadata.ErrNotFound {
		return fmt.Sprintf("node=%s replica=%s detail_err=%q lifecycle=%s health=%s", replica.NodeID, replica.ReplicaID, err, node.LifecycleState, node.HealthState)
	}
	_, existingNode := currentNodes[replica.NodeID]
	storeEligible := true
	recoveryEligibleAt := int64(0)
	storeCount := 0
	healthyStoreCount := 0
	writableStoreCount := 0
	allocatableStoreCount := 0
	allocationWeightTotal := 0
	allocationWeightObserved := false
	if err == nil {
		storeEligible = detail.StorePlacementEligible()
		recoveryEligibleAt = detail.RecoveryEligibleAtUnix
		storeCount = detail.StoreCount
		healthyStoreCount = detail.HealthyStoreCount
		writableStoreCount = detail.WritableStoreCount
		allocatableStoreCount = detail.AllocatableStoreCount
		allocationWeightTotal = detail.StoreAllocationWeightTotal
		allocationWeightObserved = detail.StoreAllocationWeightObserved
	}
	nodeEligible := node.LifecycleState == metadata.NodeLifecycleActive &&
		(node.HealthState == metadata.NodeHealthHealthy || node.HealthState == metadata.NodeHealthSuspect) &&
		storeEligible
	return fmt.Sprintf(
		"node=%s replica=%s existing=%t lifecycle=%s health=%s store_eligible=%t stores=%d healthy_stores=%d writable_stores=%d allocatable_stores=%d allocation_weight_total=%d allocation_weight_observed=%t recovery_eligible_at=%d node_eligible=%t",
		replica.NodeID,
		replica.ReplicaID,
		existingNode,
		node.LifecycleState,
		node.HealthState,
		storeEligible,
		storeCount,
		healthyStoreCount,
		writableStoreCount,
		allocatableStoreCount,
		allocationWeightTotal,
		allocationWeightObserved,
		recoveryEligibleAt,
		nodeEligible,
	)
}

func (s *Service) describeReplicaSetEligibility(ctx context.Context, vc *volumeEvaluationContext, currentNodes map[string]struct{}, replicaSet metadata.ReplicaSetState) string {
	parts := make([]string, 0, len(replicaSet.Replicas))
	for _, replica := range replicaSet.Replicas {
		parts = append(parts, s.describeReplicaEligibility(ctx, vc, currentNodes, replica))
	}
	return strings.Join(parts, "; ")
}

func (s *Service) validateTransitionTargetReplicaSetEligibility(ctx context.Context, vc *volumeEvaluationContext, currentReplicaSet, targetReplicaSet metadata.ReplicaSetState) error {
	currentNodes := make(map[string]struct{}, len(currentReplicaSet.Replicas))
	for _, replica := range currentReplicaSet.Replicas {
		currentNodes[replica.NodeID] = struct{}{}
	}
	required := int(targetReplicaSet.WriteQuorum)
	if required <= 0 {
		required = len(targetReplicaSet.Replicas)
	}
	eligible := 0
	for _, replica := range targetReplicaSet.Replicas {
		node, err := s.nodeMembershipWithContext(ctx, vc, replica.NodeID)
		if err != nil {
			return err
		}
		_, existingNode := currentNodes[replica.NodeID]
		nodeEligible := node.LifecycleState == metadata.NodeLifecycleActive &&
			(node.HealthState == metadata.NodeHealthHealthy || node.HealthState == metadata.NodeHealthSuspect) &&
			s.nodeStoreEligibleForReplica(ctx, vc, node.NodeID)
		if nodeEligible {
			eligible++
		}
		if existingNode {
			continue
		}
		if node.LifecycleState != metadata.NodeLifecycleActive {
			return transitionPreconditionf("target", "transition target node %s is not active", node.NodeID)
		}
		if node.HealthState != metadata.NodeHealthHealthy && node.HealthState != metadata.NodeHealthSuspect {
			return transitionPreconditionf("target", "transition target node %s health=%s", node.NodeID, node.HealthState)
		}
		if !s.nodeStoreEligibleForReplica(ctx, vc, node.NodeID) {
			return transitionPreconditionf("target", "transition target node %s has no writable store capacity", node.NodeID)
		}
	}
	if eligible < required {
		return transitionPreconditionf(
			"target",
			"transition target replica set %s has %d store-eligible replicas; need write quorum %d; replicas=[%s]",
			targetReplicaSet.ReplicaSetID,
			eligible,
			required,
			s.describeReplicaSetEligibility(ctx, vc, currentNodes, targetReplicaSet),
		)
	}
	return nil
}

func (s *Service) validateTransitionSourceReplicaSetEligibility(ctx context.Context, vc *volumeEvaluationContext, replicaSet metadata.ReplicaSetState) error {
	required := int(replicaSet.ReadQuorum)
	if required <= 0 {
		required = 1
	}
	eligible := 0
	for _, replica := range replicaSet.Replicas {
		node, err := s.nodeMembershipWithContext(ctx, vc, replica.NodeID)
		if err != nil {
			return err
		}
		if node.LifecycleState == metadata.NodeLifecycleRemoved {
			continue
		}
		if node.HealthState != metadata.NodeHealthHealthy && node.HealthState != metadata.NodeHealthSuspect {
			continue
		}
		if !s.nodeStoreEligibleForReplica(ctx, vc, node.NodeID) {
			continue
		}
		eligible++
	}
	if eligible < required {
		return transitionPreconditionf("source", "transition source replica set %s has %d store-eligible replicas; need read quorum %d", replicaSet.ReplicaSetID, eligible, required)
	}
	return nil
}

func (s *Service) selectReplacementNode(ctx context.Context, vc *volumeEvaluationContext, candidates []metadata.NodeMembershipRecord, currentNodes map[string]struct{}, usedZones map[string]struct{}, preferredZones, preferredHosts []string, requireNewZone bool, evaluated *EvaluatedExtent) (metadata.NodeMembershipRecord, bool) {
	eligible := make([]metadata.NodeMembershipRecord, 0, len(candidates))
	for _, node := range candidates {
		if !s.nodeEligibleForNewPlacement(ctx, vc, node) {
			continue
		}
		if _, exists := currentNodes[node.NodeID]; exists {
			continue
		}
		if requireNewZone && node.Zone != "" {
			if _, used := usedZones[node.Zone]; used {
				continue
			}
		}
		eligible = append(eligible, node)
	}
	if len(eligible) == 0 {
		return metadata.NodeMembershipRecord{}, false
	}
	if !shouldPreferSourceLocalRepairTarget(evaluated) {
		preferredZones = nil
		preferredHosts = nil
	}
	sort.SliceStable(eligible, func(i, j int) bool {
		leftScore := replacementNodeScore(eligible[i], preferredZones, preferredHosts, evaluated)
		rightScore := replacementNodeScore(eligible[j], preferredZones, preferredHosts, evaluated)
		if leftScore != rightScore {
			return leftScore > rightScore
		}
		return eligible[i].NodeID < eligible[j].NodeID
	})
	return eligible[0], true
}

func shouldPreferSourceLocalRepairTarget(evaluated *EvaluatedExtent) bool {
	if evaluated == nil {
		return false
	}
	if evaluated.RecentMutation {
		return true
	}
	if !evaluated.IncompleteTransition {
		return false
	}
	if evaluated.RetryWindowCount > 0 {
		return evaluated.RetryWindowCount <= 1 && evaluated.RetryWindowBytes > 0 && evaluated.RetryWindowBytes <= 16*1024
	}
	return evaluated.IncompleteBatches <= 1 && evaluated.IncompleteBytes > 0
}

func ShouldPreferSourceLocalRepairTarget(evaluated *EvaluatedExtent) bool {
	return shouldPreferSourceLocalRepairTarget(evaluated)
}

func strictTopologyMode(raw string) bool {
	return strings.EqualFold(strings.TrimSpace(raw), "strict")
}

func SelectReplacementNodeForEvaluatedExtent(candidates []metadata.NodeMembershipRecord, currentNodes map[string]struct{}, usedZones map[string]struct{}, preferredZones, preferredHosts []string, requireNewZone bool, evaluated *EvaluatedExtent) (metadata.NodeMembershipRecord, bool) {
	return selectReplacementNodeWithoutHealthDetails(candidates, currentNodes, usedZones, preferredZones, preferredHosts, requireNewZone, evaluated)
}

func selectReplacementNodeWithoutHealthDetails(candidates []metadata.NodeMembershipRecord, currentNodes map[string]struct{}, usedZones map[string]struct{}, preferredZones, preferredHosts []string, requireNewZone bool, evaluated *EvaluatedExtent) (metadata.NodeMembershipRecord, bool) {
	eligible := make([]metadata.NodeMembershipRecord, 0, len(candidates))
	for _, node := range candidates {
		if node.LifecycleState != metadata.NodeLifecycleActive {
			continue
		}
		if node.HealthState != metadata.NodeHealthHealthy && node.HealthState != metadata.NodeHealthSuspect {
			continue
		}
		if _, exists := currentNodes[node.NodeID]; exists {
			continue
		}
		if requireNewZone && node.Zone != "" {
			if _, used := usedZones[node.Zone]; used {
				continue
			}
		}
		eligible = append(eligible, node)
	}
	if len(eligible) == 0 {
		return metadata.NodeMembershipRecord{}, false
	}
	if !shouldPreferSourceLocalRepairTarget(evaluated) {
		preferredZones = nil
		preferredHosts = nil
	}
	sort.SliceStable(eligible, func(i, j int) bool {
		leftScore := replacementNodeScore(eligible[i], preferredZones, preferredHosts, evaluated)
		rightScore := replacementNodeScore(eligible[j], preferredZones, preferredHosts, evaluated)
		if leftScore != rightScore {
			return leftScore > rightScore
		}
		return eligible[i].NodeID < eligible[j].NodeID
	})
	return eligible[0], true
}

func replacementNodeScore(node metadata.NodeMembershipRecord, preferredZones, preferredHosts []string, evaluated *EvaluatedExtent) int64 {
	score := int64(0)
	localityWeight := repairTargetLocalityWeight(evaluated)
	if evaluated != nil && evaluated.RetryWindowCount > 0 {
		score += maxInt64(0, 96-int64(evaluated.RetryWindowBytes/4096))
		score += maxInt64(0, 48-int64(evaluated.RetryWindowChunks))
		score += maxInt64(0, 24-int64(evaluated.RetryWindowCount))
	}
	score += preferredLocalityScore(node.Zone, preferredZones, localityWeight)
	score += preferredLocalityScore(node.Host, preferredHosts, localityWeight*2)
	return score
}

func preferredLocalityScore(value string, preferred []string, unit int64) int64 {
	if value == "" || unit == 0 {
		return 0
	}
	for idx, candidate := range preferred {
		if candidate == "" || candidate != value {
			continue
		}
		return maxInt64(0, int64(len(preferred)-idx)) * unit
	}
	return 0
}

func repairTargetLocalityWeight(evaluated *EvaluatedExtent) int64 {
	if evaluated == nil {
		return 0
	}
	score := int64(0)
	if evaluated.RecentMutation {
		score += 1000
	}
	if evaluated.IncompleteTransition {
		if evaluated.IncompleteBatches > 1 {
			return score
		}
		score += 200
		if evaluated.IncompleteBatches > 0 {
			score += maxInt64(0, 64-int64(evaluated.IncompleteBatches))
		}
		if evaluated.IncompleteBytes > 0 {
			score += maxInt64(0, 128-int64(evaluated.IncompleteBytes/4096))
		}
		if evaluated.IncompleteChunks > 0 {
			score += maxInt64(0, 64-int64(evaluated.IncompleteChunks))
		}
	}
	return score
}

func maxInt64(left, right int64) int64 {
	if left > right {
		return left
	}
	return right
}

func (s *Service) countHealthyReplicas(ctx context.Context, replicaSet metadata.ReplicaSetState) (int, error) {
	healthy := 0
	for _, replica := range replicaSet.Replicas {
		node, err := s.store.GetNodeMembership(ctx, replica.NodeID)
		if err != nil {
			return 0, err
		}
		if node.LifecycleState == metadata.NodeLifecycleRemoved {
			continue
		}
		if node.HealthState == metadata.NodeHealthHealthy || node.HealthState == metadata.NodeHealthSuspect {
			healthy++
		}
	}
	return healthy, nil
}

func (s *Service) evaluatePrimaryFailover(ctx context.Context, replicaSet metadata.ReplicaSetState) (bool, int, string, error) {
	healthyReplicas := 0
	primaryHealthy := false
	nextPrimary := ""
	for _, replica := range replicaSet.Replicas {
		node, err := s.store.GetNodeMembership(ctx, replica.NodeID)
		if err != nil {
			return false, 0, "", err
		}
		if node.LifecycleState == metadata.NodeLifecycleRemoved {
			continue
		}
		if node.HealthState != metadata.NodeHealthHealthy && node.HealthState != metadata.NodeHealthSuspect {
			continue
		}
		healthyReplicas++
		if replica.ReplicaID == replicaSet.PrimaryReplicaID || replica.Role == metadata.ReplicaRolePrimary {
			primaryHealthy = true
			continue
		}
		if nextPrimary == "" {
			nextPrimary = replica.ReplicaID
		}
	}
	return primaryHealthy, healthyReplicas, nextPrimary, nil
}
