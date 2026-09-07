package metadata

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
)

const (
	SummaryAuthorityPageMaximum  = 512
	SummaryAuthorityBatchMaximum = 128
)

var (
	ErrSummaryAuthoritySnapshotRequired = errors.New("summary authority snapshot requires consistent metadata reads")
	ErrSummaryAuthorityNotQuiescent     = errors.New("summary authority is not quiescent for enforced bootstrap")
)

// SummaryAuthorityCaptureEvidence records the explicit full-completion work
// performed by read-only inspection or while all sbs-service writers are
// stopped for bootstrap. Normal summary reads and maintenance ticks never call
// this path.
type SummaryAuthorityCaptureEvidence struct {
	SourceRevision             uint64                                `json:"source_revision"`
	NodeCount                  int                                   `json:"node_count"`
	NodeIDs                    []string                              `json:"node_ids,omitempty"`
	VolumeCount                int                                   `json:"volume_count"`
	VolumeSpecCount            int                                   `json:"volume_spec_count"`
	VolumeStateCount           int                                   `json:"volume_state_count"`
	MissingVolumeStateCount    int                                   `json:"missing_volume_state_count"`
	MissingVolumeStateIDs      []string                              `json:"missing_volume_state_ids,omitempty"`
	OrphanVolumeStateCount     int                                   `json:"orphan_volume_state_count"`
	OrphanVolumeStateIDs       []string                              `json:"orphan_volume_state_ids,omitempty"`
	OrphanVolumeStates         []VolumeState                         `json:"orphan_volume_states,omitempty"`
	OrphanVolumeResiduals      []SummaryOrphanVolumeResidualEvidence `json:"orphan_volume_residuals,omitempty"`
	TransitionCount            int                                   `json:"transition_count"`
	AdminOperationCount        int                                   `json:"admin_operation_count"`
	MutationOperationCount     int                                   `json:"mutation_operation_count"`
	ContributionCount          int                                   `json:"contribution_count"`
	RangePageCount             int                                   `json:"range_page_count"`
	BatchGetCount              int                                   `json:"batch_get_count"`
	BatchGetKeyCount           int                                   `json:"batch_get_key_count"`
	MaximumBatchGetKeyCount    int                                   `json:"maximum_batch_get_key_count"`
	BackendFullScanCount       int                                   `json:"backend_full_scan_count"`
	FullCompletionCount        int                                   `json:"full_completion_count"`
	NestedCompletionCount      int                                   `json:"nested_completion_count"`
	Quiescent                  bool                                  `json:"quiescent"`
	UnrepresentedCount         int                                   `json:"unrepresented_count"`
	FirstUnrepresented         string                                `json:"first_unrepresented,omitempty"`
	UnrepresentedClasses       []string                              `json:"unrepresented_classes,omitempty"`
	UnrepresentedClassCounts   map[string]int                        `json:"unrepresented_class_counts,omitempty"`
	UnrepresentedDetails       []string                              `json:"unrepresented_details,omitempty"`
	RejectedMutationOperations []SummaryMutationOperationEvidence    `json:"rejected_mutation_operations,omitempty"`
}

// SummaryOrphanVolumeResidualEvidence classifies every metadata key beneath
// an orphan volume state. It deliberately reports counts instead of values or
// raw keys so read-only lab inspection can prove the cleanup boundary without
// leaking payload or idempotency material.
type SummaryOrphanVolumeResidualEvidence struct {
	VolumeID                          string         `json:"volume_id"`
	TotalKeyCount                     int            `json:"total_key_count"`
	ResidualKeyCount                  int            `json:"residual_key_count"`
	AuthorityStateKeyCount            int            `json:"authority_state_key_count"`
	ChunkSequenceKeyCount             int            `json:"chunk_sequence_key_count"`
	ExtentMappingKeyCount             int            `json:"extent_mapping_key_count"`
	AllocationPageKeyCount            int            `json:"allocation_page_key_count"`
	WriteStateKeyCount                int            `json:"write_state_key_count"`
	ReplicaSetKeyCount                int            `json:"replica_set_key_count"`
	IdempotencyKeyCount               int            `json:"idempotency_key_count"`
	SnapshotIndexKeyCount             int            `json:"snapshot_index_key_count"`
	CloneIndexKeyCount                int            `json:"clone_index_key_count"`
	PlacementRecordKeyCount           int            `json:"placement_record_key_count"`
	MutationOperationKeyCount         int            `json:"mutation_operation_key_count"`
	MutationOperationStateCounts      map[string]int `json:"mutation_operation_state_counts,omitempty"`
	NonterminalMutationOperationCount int            `json:"nonterminal_mutation_operation_count"`
	NonterminalMutationOperationIDs   []string       `json:"nonterminal_mutation_operation_ids,omitempty"`
	NondeletedSnapshotRecordCount     int            `json:"nondeleted_snapshot_record_count"`
	NondeletedSnapshotRecordIDs       []string       `json:"nondeleted_snapshot_record_ids,omitempty"`
	NondeletedCloneRecordCount        int            `json:"nondeleted_clone_record_count"`
	NondeletedCloneRecordIDs          []string       `json:"nondeleted_clone_record_ids,omitempty"`
	ExactKeyValueCount                int            `json:"exact_key_value_count"`
	ExactKeyValueDigestSHA256         string         `json:"exact_key_value_digest_sha256"`
	PhysicalObjectKeyCount            int            `json:"physical_object_key_count"`
	ECStripeKeyCount                  int            `json:"ec_stripe_key_count"`
	OtherKeyCount                     int            `json:"other_key_count"`
}

type SummaryMutationOperationEvidence struct {
	OperationID                          string                                `json:"operation_id"`
	VolumeID                             string                                `json:"volume_id"`
	Kind                                 string                                `json:"kind"`
	State                                MutationOperationState                `json:"state"`
	PlacementRevision                    uint64                                `json:"placement_revision,omitempty"`
	AllocationRevision                   uint64                                `json:"allocation_revision,omitempty"`
	WriterFencingEpoch                   uint64                                `json:"writer_fencing_epoch,omitempty"`
	AffectedExtentCount                  int                                   `json:"affected_extent_count"`
	AffectedPageCount                    int                                   `json:"affected_page_count"`
	RetiredPhysicalChunkCount            int                                   `json:"retired_physical_chunk_count"`
	RetiredPhysicalChunkIDs              []uint64                              `json:"retired_physical_chunk_ids,omitempty"`
	ProtectedRetiredPhysicalChunkCount   int                                   `json:"protected_retired_physical_chunk_count"`
	ProtectedRetiredPhysicalChunkIDs     []uint64                              `json:"protected_retired_physical_chunk_ids,omitempty"`
	UnprotectedRetiredPhysicalChunkCount int                                   `json:"unprotected_retired_physical_chunk_count"`
	UnprotectedRetiredPhysicalChunkIDs   []uint64                              `json:"unprotected_retired_physical_chunk_ids,omitempty"`
	ProtectingReadViews                  []SummaryProtectingReadViewEvidence   `json:"protecting_read_views,omitempty"`
	CandidateOrigin                      SummaryRetiredCandidateOriginEvidence `json:"candidate_origin"`
	StaleReplicaInspection               SummaryStaleReplicaInspectionEvidence `json:"stale_replica_inspection"`
	StartedAtUnix                        int64                                 `json:"started_at_unix,omitempty"`
	LastUpdatedAtUnix                    int64                                 `json:"last_updated_at_unix,omitempty"`
	ErrorMessage                         string                                `json:"error_message,omitempty"`
}

type SummaryProtectingReadViewEvidence struct {
	Kind                      string   `json:"kind"`
	ID                        string   `json:"id"`
	AllocationPageCount       int      `json:"allocation_page_count"`
	MinimumPageRevision       uint64   `json:"minimum_page_revision,omitempty"`
	MaximumPageRevision       uint64   `json:"maximum_page_revision,omitempty"`
	RetiredPhysicalChunkCount int      `json:"retired_physical_chunk_count"`
	RetiredPhysicalChunkIDs   []uint64 `json:"retired_physical_chunk_ids"`
}

type SummaryRetiredCandidateOriginEvidence struct {
	OperationCount            int            `json:"operation_count"`
	KindCounts                map[string]int `json:"kind_counts,omitempty"`
	MinimumAllocationRevision uint64         `json:"minimum_allocation_revision,omitempty"`
	MaximumAllocationRevision uint64         `json:"maximum_allocation_revision,omitempty"`
	EarliestUpdatedAtUnix     int64          `json:"earliest_updated_at_unix,omitempty"`
	LatestUpdatedAtUnix       int64          `json:"latest_updated_at_unix,omitempty"`
}

type SummaryStaleReplicaInspectionEvidence struct {
	ResolvedCandidateCount               int                                           `json:"resolved_candidate_count"`
	UnresolvedCandidateCount             int                                           `json:"unresolved_candidate_count"`
	UnresolvedCandidateIDs               []uint64                                      `json:"unresolved_candidate_ids,omitempty"`
	PersistedTargetCandidateCount        int                                           `json:"persisted_target_candidate_count"`
	LiveTransitionTargetCandidateCount   int                                           `json:"live_transition_target_candidate_count"`
	ClusterExclusionTargetCandidateCount int                                           `json:"cluster_exclusion_target_candidate_count"`
	TargetCount                          int                                           `json:"target_count"`
	Targets                              []SummaryStaleReplicaInspectionTargetEvidence `json:"targets,omitempty"`
}

type SummaryStaleReplicaInspectionTargetEvidence struct {
	NodeID                    string   `json:"node_id"`
	SourceReplicaIDs          []string `json:"source_replica_ids,omitempty"`
	AuthorityKinds            []string `json:"authority_kinds"`
	RetiredPhysicalChunkCount int      `json:"retired_physical_chunk_count"`
	RetiredPhysicalChunkIDs   []uint64 `json:"retired_physical_chunk_ids"`
	OriginOperationIDs        []string `json:"origin_operation_ids"`
}

// SummaryAuthoritySnapshot is an immutable, revision-fenced source for the
// existing bounded summary rebuild engine.
type SummaryAuthoritySnapshot struct {
	sourceRevision uint64
	contributions  []SummaryContribution
}

func (s *SummaryAuthoritySnapshot) ListSummaryContributions(_ context.Context, kind, cursor string, limit int) (SummaryRebuildSourcePage, error) {
	if s == nil || kind != SummaryKindCluster || s.sourceRevision == 0 || limit < 1 || limit > SummaryRebuildMaxPageSize {
		return SummaryRebuildSourcePage{}, fmt.Errorf("invalid authority summary page request")
	}
	cursor = strings.TrimSpace(cursor)
	start := sort.Search(len(s.contributions), func(i int) bool { return s.contributions[i].SubjectID > cursor })
	end := min(start+limit, len(s.contributions))
	page := SummaryRebuildSourcePage{SourceRevision: s.sourceRevision}
	page.Contributions = append(page.Contributions, s.contributions[start:end]...)
	if end < len(s.contributions) {
		page.NextCursor = page.Contributions[len(page.Contributions)-1].SubjectID
	}
	return page, nil
}

// CaptureSummaryAuthoritySnapshot reads the authority prefixes in one backend
// snapshot. It rejects states whose dynamic counters are not yet maintained by
// the live transactional summary hooks. This makes the initial enforced
// cutover fail closed instead of silently publishing a partial baseline.
func (r *Repository) CaptureSummaryAuthoritySnapshot(ctx context.Context, pageLimit, batchLimit int) (*SummaryAuthoritySnapshot, SummaryAuthorityCaptureEvidence, error) {
	evidence := SummaryAuthorityCaptureEvidence{FullCompletionCount: 1}
	if r == nil || pageLimit < 1 || pageLimit > SummaryAuthorityPageMaximum || batchLimit < 1 || batchLimit > SummaryAuthorityBatchMaximum {
		return nil, evidence, fmt.Errorf("invalid authority summary capture bounds")
	}
	snapshotter, ok := r.kv.(consistentSnapshotKV)
	if !ok {
		return nil, evidence, ErrSummaryAuthoritySnapshotRequired
	}
	capture := &summaryAuthorityCapture{root: r.root, pageLimit: pageLimit, batchLimit: batchLimit, evidence: &evidence}
	err := snapshotter.RunInReadSnapshot(ctx, func(reader kvReadSnapshot) error {
		if err := capture.captureNodes(ctx, reader); err != nil {
			return err
		}
		volumeIDs, err := capture.captureVolumes(ctx, reader)
		if err != nil {
			return err
		}
		if err := capture.captureTransitionsAndMutationOperations(ctx, reader, volumeIDs); err != nil {
			return err
		}
		return capture.captureAdminOperations(ctx, reader)
	})
	if err != nil {
		return nil, evidence, err
	}
	evidence.SourceRevision = summaryMutationRevision(r.now())
	sort.Slice(capture.contributions, func(i, j int) bool {
		return capture.contributions[i].SubjectID < capture.contributions[j].SubjectID
	})
	for i := 1; i < len(capture.contributions); i++ {
		if capture.contributions[i-1].SubjectID == capture.contributions[i].SubjectID {
			return nil, evidence, fmt.Errorf("duplicate summary subject %s", capture.contributions[i].SubjectID)
		}
	}
	evidence.ContributionCount = len(capture.contributions)
	evidence.UnrepresentedClasses = sortedStringSet(capture.unrepresentedClasses)
	evidence.UnrepresentedCount = len(capture.unrepresented)
	evidence.UnrepresentedClassCounts = capture.unrepresentedClassCounts
	evidence.UnrepresentedDetails = append([]string(nil), capture.unrepresented...)
	if len(capture.unrepresented) > 0 {
		evidence.FirstUnrepresented = capture.unrepresented[0]
		return nil, evidence, fmt.Errorf("%w: %s", ErrSummaryAuthorityNotQuiescent, evidence.FirstUnrepresented)
	}
	evidence.Quiescent = true
	return &SummaryAuthoritySnapshot{sourceRevision: evidence.SourceRevision, contributions: capture.contributions}, evidence, nil
}

type summaryAuthorityCapture struct {
	root                        string
	pageLimit                   int
	batchLimit                  int
	evidence                    *SummaryAuthorityCaptureEvidence
	contributions               []SummaryContribution
	unrepresented               []string
	unrepresentedClasses        map[string]struct{}
	unrepresentedClassCounts    map[string]int
	readViewRecordsLoaded       bool
	snapshotIDsByVolume         map[string][]string
	cloneIDsByVolume            map[string][]string
	cloneReferenceIDsByVolume   map[string][]string
	orphanResidualIndexByVolume map[string]int
	protectedChunksByVolume     map[string]map[uint64]struct{}
	protectingViewsByVolume     map[string][]summaryProtectingReadView
	placementAuthorityByVolume  map[string]summaryVolumePlacementAuthority
	inspectionNodeIDs           []string
	inspectionNodeIDSet         map[string]struct{}
}

type summaryProtectingReadView struct {
	kind                string
	id                  string
	allocationPageCount int
	minimumPageRevision uint64
	maximumPageRevision uint64
	chunks              map[uint64]struct{}
}

type summaryVolumePlacementAuthority struct {
	extentsByID            map[uint64]ExtentMappingRecord
	replicaSetsByID        map[string]ReplicaSetState
	replicaSetsByPlacement map[string]ReplicaSetState
}

type summaryAdminOperation struct {
	OperationID string `json:"operation_id"`
	State       string `json:"state"`
}

type summaryRetiredChunk struct {
	volumeID string
	chunkID  uint64
}

func (c *summaryAuthorityCapture) captureNodes(ctx context.Context, reader kvReadSnapshot) error {
	keys, err := c.listFiltered(ctx, reader, c.root+"/nodes/", func(key string) bool { return strings.HasSuffix(key, "/membership") })
	if err != nil {
		return err
	}
	values, err := c.batchGet(ctx, reader, keys)
	if err != nil {
		return err
	}
	for _, key := range keys {
		var record NodeMembershipRecord
		if err := decodeRequiredAuthorityJSON(values, key, &record); err != nil {
			return err
		}
		if nodeMembershipKey(c.root, record.NodeID) != key {
			return fmt.Errorf("node membership identity differs from key %q", key)
		}
		c.inspectionNodeIDs = append(c.inspectionNodeIDs, record.NodeID)
		if err := c.addContribution(summarySubject("membership", record.NodeID), summaryMembershipContribution(record, true)); err != nil {
			return err
		}
		c.evidence.NodeCount++
	}
	sort.Strings(c.inspectionNodeIDs)
	c.evidence.NodeIDs = append([]string(nil), c.inspectionNodeIDs...)
	c.inspectionNodeIDSet = make(map[string]struct{}, len(c.inspectionNodeIDs))
	for _, nodeID := range c.inspectionNodeIDs {
		c.inspectionNodeIDSet[nodeID] = struct{}{}
	}
	return nil
}

func (c *summaryAuthorityCapture) captureVolumes(ctx context.Context, reader kvReadSnapshot) ([]string, error) {
	specKeys, err := c.listFiltered(ctx, reader, volumeCatalogSourcePrefix(c.root), func(key string) bool { return strings.HasSuffix(key, "/spec") })
	if err != nil {
		return nil, err
	}
	stateKeys, err := c.listFiltered(ctx, reader, c.root+"/volumes/", func(key string) bool { return strings.HasSuffix(key, "/meta/state") })
	if err != nil {
		return nil, err
	}
	c.evidence.VolumeSpecCount = len(specKeys)
	c.evidence.VolumeStateCount = len(stateKeys)
	volumeIDs := make([]string, 0, len(specKeys)+len(stateKeys))
	specKeyByVolume := make(map[string]string, len(specKeys))
	stateKeyByVolume := make(map[string]string, len(stateKeys))
	for _, key := range stateKeys {
		volumeID, err := volumeIDFromSummaryStateKey(c.root, key)
		if err != nil {
			return nil, err
		}
		stateKeyByVolume[volumeID] = key
	}
	keys := make([]string, 0, len(specKeys)+len(stateKeys))
	for _, key := range specKeys {
		volumeID, err := volumeIDFromCatalogSpecKey(c.root, key)
		if err != nil {
			return nil, err
		}
		specKeyByVolume[volumeID] = key
		volumeIDs = append(volumeIDs, volumeID)
		if _, found := stateKeyByVolume[volumeID]; !found {
			c.evidence.MissingVolumeStateIDs = append(c.evidence.MissingVolumeStateIDs, volumeID)
		}
		keys = append(keys, key)
	}
	for volumeID := range stateKeyByVolume {
		if _, found := specKeyByVolume[volumeID]; !found {
			c.evidence.OrphanVolumeStateIDs = append(c.evidence.OrphanVolumeStateIDs, volumeID)
			volumeIDs = append(volumeIDs, volumeID)
		}
	}
	sort.Strings(c.evidence.MissingVolumeStateIDs)
	sort.Strings(c.evidence.OrphanVolumeStateIDs)
	c.evidence.MissingVolumeStateCount = len(c.evidence.MissingVolumeStateIDs)
	c.evidence.OrphanVolumeStateCount = len(c.evidence.OrphanVolumeStateIDs)
	if err := c.captureOrphanVolumeResiduals(ctx, reader); err != nil {
		return nil, err
	}
	for _, volumeID := range c.evidence.MissingVolumeStateIDs {
		c.reject("incomplete_volume_authority", "volume "+volumeID+" spec has no state")
	}
	for _, volumeID := range c.evidence.OrphanVolumeStateIDs {
		c.reject("incomplete_volume_authority", "volume "+volumeID+" state has no spec")
	}
	sort.Strings(volumeIDs)
	keys = append(keys, stateKeys...)
	values, err := c.batchGet(ctx, reader, keys)
	if err != nil {
		return nil, err
	}
	for _, specKey := range specKeys {
		volumeID, err := volumeIDFromCatalogSpecKey(c.root, specKey)
		if err != nil {
			return nil, err
		}
		var spec VolumeSpecRecord
		if err := decodeRequiredAuthorityJSON(values, specKey, &spec); err != nil {
			return nil, err
		}
		if spec.VolumeID != volumeID {
			return nil, fmt.Errorf("volume spec authority identity differs from key for %q", volumeID)
		}
		stateKey, stateFound := stateKeyByVolume[volumeID]
		if !stateFound {
			continue
		}
		var state VolumeState
		if err := decodeRequiredAuthorityJSON(values, stateKey, &state); err != nil {
			return nil, err
		}
		if state.VolumeID != volumeID {
			return nil, fmt.Errorf("volume state authority identity differs from key for %q", volumeID)
		}
		if state.Status != VolumeStatusHealthy {
			c.reject("nonhealthy_volume", "volume "+volumeID+" status="+string(state.Status))
		}
		if err := c.addContribution(summarySubject("volume", volumeID), summaryVolumeContribution(state, true, spec, true)); err != nil {
			return nil, err
		}
		c.evidence.VolumeCount++
	}
	for _, volumeID := range c.evidence.OrphanVolumeStateIDs {
		var state VolumeState
		if err := decodeRequiredAuthorityJSON(values, stateKeyByVolume[volumeID], &state); err != nil {
			return nil, err
		}
		if state.VolumeID != volumeID {
			return nil, fmt.Errorf("orphan volume state authority identity differs from key for %q", volumeID)
		}
		c.evidence.OrphanVolumeStates = append(c.evidence.OrphanVolumeStates, state)
	}
	return volumeIDs, nil
}

func (c *summaryAuthorityCapture) captureOrphanVolumeResiduals(ctx context.Context, reader kvReadSnapshot) error {
	if len(c.evidence.OrphanVolumeStateIDs) == 0 {
		return nil
	}
	if err := c.loadReadViewRecords(ctx, reader); err != nil {
		return err
	}
	c.orphanResidualIndexByVolume = make(map[string]int, len(c.evidence.OrphanVolumeStateIDs))
	for _, volumeID := range c.evidence.OrphanVolumeStateIDs {
		prefix := c.root + "/volumes/" + volumeID + "/"
		keys, err := c.listFiltered(ctx, reader, prefix, func(string) bool { return true })
		if err != nil {
			return err
		}
		sort.Strings(keys)
		exactDigest, err := c.digestAuthorityKeyValueSet(ctx, reader, keys)
		if err != nil {
			return err
		}
		item := SummaryOrphanVolumeResidualEvidence{
			VolumeID:                    volumeID,
			TotalKeyCount:               len(keys),
			NondeletedSnapshotRecordIDs: append([]string(nil), c.snapshotIDsByVolume[volumeID]...),
			NondeletedCloneRecordIDs:    append([]string(nil), c.cloneReferenceIDsByVolume[volumeID]...),
			ExactKeyValueCount:          len(keys),
			ExactKeyValueDigestSHA256:   exactDigest,
		}
		item.NondeletedSnapshotRecordCount = len(item.NondeletedSnapshotRecordIDs)
		item.NondeletedCloneRecordCount = len(item.NondeletedCloneRecordIDs)
		for _, key := range keys {
			relative := strings.TrimPrefix(key, prefix)
			switch {
			case relative == "meta/state":
				item.AuthorityStateKeyCount++
			case relative == "meta/next_chunk_id":
				item.ChunkSequenceKeyCount++
			case strings.HasPrefix(relative, "extents/"):
				item.ExtentMappingKeyCount++
			case strings.HasPrefix(relative, "allocation/pages/"):
				item.AllocationPageKeyCount++
			case strings.HasPrefix(relative, "write_state/pages/"):
				item.WriteStateKeyCount++
			case strings.HasPrefix(relative, "replicasets/"):
				item.ReplicaSetKeyCount++
			case strings.HasPrefix(relative, "idem/"), strings.HasPrefix(relative, "idempotency/"):
				item.IdempotencyKeyCount++
			case strings.HasPrefix(relative, "snapshots/"), strings.HasPrefix(relative, "snapshot_idem/"):
				item.SnapshotIndexKeyCount++
			case strings.HasPrefix(relative, "clones/"):
				item.CloneIndexKeyCount++
			case strings.HasPrefix(relative, "placements/"):
				item.PlacementRecordKeyCount++
			case strings.HasPrefix(relative, "operations/"):
				item.MutationOperationKeyCount++
			case strings.HasPrefix(relative, "physical_objects/"):
				item.PhysicalObjectKeyCount++
			case strings.HasPrefix(relative, "ec/stripes/"):
				item.ECStripeKeyCount++
			default:
				item.OtherKeyCount++
			}
		}
		item.ResidualKeyCount = item.TotalKeyCount - item.AuthorityStateKeyCount
		c.orphanResidualIndexByVolume[volumeID] = len(c.evidence.OrphanVolumeResiduals)
		c.evidence.OrphanVolumeResiduals = append(c.evidence.OrphanVolumeResiduals, item)
	}
	return nil
}

func (c *summaryAuthorityCapture) digestAuthorityKeyValueSet(ctx context.Context, reader kvReadSnapshot, keys []string) (string, error) {
	hash := sha256.New()
	_, _ = hash.Write([]byte("namrbd/phase-ad/orphan-key-value-set/v1\x00"))
	var length [8]byte
	for start := 0; start < len(keys); start += c.batchLimit {
		end := min(start+c.batchLimit, len(keys))
		batch, err := reader.BatchGet(ctx, keys[start:end])
		c.evidence.BatchGetCount++
		c.evidence.BatchGetKeyCount += end - start
		c.evidence.MaximumBatchGetKeyCount = max(c.evidence.MaximumBatchGetKeyCount, end-start)
		if err != nil {
			return "", err
		}
		for _, key := range keys[start:end] {
			raw, found := batch[key]
			if !found {
				return "", fmt.Errorf("authority key disappeared from consistent snapshot: %s", key)
			}
			binary.BigEndian.PutUint64(length[:], uint64(len(key)))
			_, _ = hash.Write(length[:])
			_, _ = hash.Write([]byte(key))
			binary.BigEndian.PutUint64(length[:], uint64(len(raw)))
			_, _ = hash.Write(length[:])
			_, _ = hash.Write(raw)
		}
	}
	return hex.EncodeToString(hash.Sum(nil)), nil
}

func (c *summaryAuthorityCapture) recordOrphanMutationOperation(record MutationOperationRecord) {
	index, found := c.orphanResidualIndexByVolume[record.VolumeID]
	if !found {
		return
	}
	item := &c.evidence.OrphanVolumeResiduals[index]
	if item.MutationOperationStateCounts == nil {
		item.MutationOperationStateCounts = make(map[string]int)
	}
	item.MutationOperationStateCounts[string(record.State)]++
	if record.State != MutationOperationCommitted && record.State != MutationOperationRolledBack {
		item.NonterminalMutationOperationCount++
		item.NonterminalMutationOperationIDs = append(item.NonterminalMutationOperationIDs, record.OperationID)
	}
}

func volumeIDFromSummaryStateKey(root, key string) (string, error) {
	prefix := root + "/volumes/"
	suffix := "/meta/state"
	if !strings.HasPrefix(key, prefix) || !strings.HasSuffix(key, suffix) {
		return "", fmt.Errorf("invalid volume state key %q", key)
	}
	volumeID := strings.TrimSuffix(strings.TrimPrefix(key, prefix), suffix)
	if volumeID == "" || strings.Contains(volumeID, "/") {
		return "", fmt.Errorf("invalid volume state key %q", key)
	}
	canonical, err := CanonicalVolumeID(volumeID)
	if err != nil || canonical != volumeID {
		return "", fmt.Errorf("invalid volume id in state key %q", key)
	}
	return volumeID, nil
}

func (c *summaryAuthorityCapture) captureTransitionsAndMutationOperations(ctx context.Context, reader kvReadSnapshot, volumeIDs []string) error {
	pendingRetired := make(map[summaryRetiredChunk]struct{})
	collectedRetired := make(map[summaryRetiredChunk]struct{})
	for _, volumeID := range volumeIDs {
		transitionsByPlacement := make(map[string]PlacementTransitionRecord)
		transitionKeys, err := c.listFiltered(ctx, reader, placementTransitionsPrefix(c.root, volumeID), func(key string) bool { return strings.HasSuffix(key, "/transition") })
		if err != nil {
			return err
		}
		transitionValues, err := c.batchGet(ctx, reader, transitionKeys)
		if err != nil {
			return err
		}
		for _, key := range transitionKeys {
			var record PlacementTransitionRecord
			if err := decodeRequiredAuthorityJSON(transitionValues, key, &record); err != nil {
				return err
			}
			if placementTransitionKey(c.root, record.VolumeID, record.PlacementRef) != key || record.VolumeID != volumeID {
				return fmt.Errorf("placement transition identity differs from key %q", key)
			}
			transitionsByPlacement[record.PlacementRef] = record
			if record.State == PlacementTransitionQueued || record.State == PlacementTransitionRunning || record.State == PlacementTransitionPaused || record.State == PlacementTransitionFailed {
				c.reject("active_or_failed_transition", "transition "+record.VolumeID+"/"+record.PlacementRef+" state="+string(record.State))
			}
			if counters := summaryTransitionContribution(record, true); counters != (SummaryCounters{}) {
				if err := c.addContribution(summarySubject("placement-transition", record.VolumeID, record.PlacementRef), counters); err != nil {
					return err
				}
			}
			c.evidence.TransitionCount++
		}

		operationKeys, err := c.listFiltered(ctx, reader, mutationOperationsPrefix(c.root, volumeID), func(string) bool { return true })
		if err != nil {
			return err
		}
		operationValues, err := c.batchGet(ctx, reader, operationKeys)
		if err != nil {
			return err
		}
		operations := make([]MutationOperationRecord, 0, len(operationKeys))
		for _, key := range operationKeys {
			var record MutationOperationRecord
			if err := decodeRequiredAuthorityJSON(operationValues, key, &record); err != nil {
				return err
			}
			if mutationOperationKey(c.root, record.VolumeID, record.OperationID) != key || record.VolumeID != volumeID {
				return fmt.Errorf("mutation operation identity differs from key %q", key)
			}
			c.recordOrphanMutationOperation(record)
			operations = append(operations, record)
			c.evidence.MutationOperationCount++
		}
		for _, record := range operations {
			if (record.Kind == "transition" || record.Kind == "transition_batch" || record.Kind == "payload_gc" || record.Kind == "payload_gc_batch") && record.State != MutationOperationCommitted && record.State != MutationOperationRolledBack {
				c.reject("active_or_failed_dynamic_operation", "mutation operation "+record.OperationID+" kind="+record.Kind+" state="+string(record.State))
				retiredChunkIDs := sortedNonzeroUint64s(record.RetiredPhysicalChunkIDs)
				protectedRetiredChunkIDs, unprotectedRetiredChunkIDs, protectingReadViews, err := c.classifyRetiredPhysicalChunkIDs(ctx, reader, record.VolumeID, retiredChunkIDs)
				if err != nil {
					return err
				}
				staleReplicaInspection, err := c.staleReplicaInspectionEvidence(ctx, reader, record.VolumeID, retiredChunkIDs, operations, transitionsByPlacement)
				if err != nil {
					return err
				}
				c.evidence.RejectedMutationOperations = append(c.evidence.RejectedMutationOperations, SummaryMutationOperationEvidence{
					OperationID: record.OperationID, VolumeID: record.VolumeID, Kind: record.Kind, State: record.State,
					PlacementRevision: record.PlacementRevision, AllocationRevision: record.AllocationRevision, WriterFencingEpoch: record.WriterFencingEpoch,
					AffectedExtentCount: len(record.AffectedExtentIDs), AffectedPageCount: len(record.AffectedPageNos),
					RetiredPhysicalChunkCount: len(retiredChunkIDs), RetiredPhysicalChunkIDs: retiredChunkIDs,
					ProtectedRetiredPhysicalChunkCount: len(protectedRetiredChunkIDs), ProtectedRetiredPhysicalChunkIDs: protectedRetiredChunkIDs,
					UnprotectedRetiredPhysicalChunkCount: len(unprotectedRetiredChunkIDs), UnprotectedRetiredPhysicalChunkIDs: unprotectedRetiredChunkIDs,
					ProtectingReadViews: protectingReadViews, CandidateOrigin: summarizeRetiredCandidateOrigins(operations, retiredChunkIDs),
					StaleReplicaInspection: staleReplicaInspection,
					StartedAtUnix:          record.StartedAtUnix,
					LastUpdatedAtUnix:      record.LastUpdatedAtUnix, ErrorMessage: record.ErrorMessage,
				})
			}
			if record.State != MutationOperationCommitted {
				continue
			}
			switch record.Kind {
			case "write", "transition":
				for _, chunkID := range record.RetiredPhysicalChunkIDs {
					if chunkID != 0 {
						pendingRetired[summaryRetiredChunk{volumeID: volumeID, chunkID: chunkID}] = struct{}{}
					}
				}
			case "payload_gc", "payload_gc_batch":
				for _, chunkID := range record.RetiredPhysicalChunkIDs {
					if chunkID != 0 {
						collectedRetired[summaryRetiredChunk{volumeID: volumeID, chunkID: chunkID}] = struct{}{}
					}
				}
			}
		}
	}
	for chunkID := range collectedRetired {
		delete(pendingRetired, chunkID)
	}
	for index := range c.evidence.OrphanVolumeResiduals {
		sort.Strings(c.evidence.OrphanVolumeResiduals[index].NonterminalMutationOperationIDs)
	}
	if len(pendingRetired) > 0 {
		c.reject("retired_payload_backlog", fmt.Sprintf("retired payload backlog has %d uncollected chunks", len(pendingRetired)))
	}
	return nil
}

func (c *summaryAuthorityCapture) classifyRetiredPhysicalChunkIDs(ctx context.Context, reader kvReadSnapshot, volumeID string, retiredChunkIDs []uint64) ([]uint64, []uint64, []SummaryProtectingReadViewEvidence, error) {
	if len(retiredChunkIDs) == 0 {
		return nil, nil, nil, nil
	}
	protected, err := c.protectedPhysicalChunkIDs(ctx, reader, volumeID)
	if err != nil {
		return nil, nil, nil, err
	}
	protectedIntersection := make([]uint64, 0)
	unprotected := make([]uint64, 0)
	for _, chunkID := range retiredChunkIDs {
		if _, found := protected[chunkID]; found {
			protectedIntersection = append(protectedIntersection, chunkID)
		} else {
			unprotected = append(unprotected, chunkID)
		}
	}
	views := make([]SummaryProtectingReadViewEvidence, 0)
	for _, view := range c.protectingViewsByVolume[volumeID] {
		intersection := make([]uint64, 0)
		for _, chunkID := range retiredChunkIDs {
			if _, found := view.chunks[chunkID]; found {
				intersection = append(intersection, chunkID)
			}
		}
		if len(intersection) == 0 {
			continue
		}
		views = append(views, SummaryProtectingReadViewEvidence{
			Kind: view.kind, ID: view.id,
			AllocationPageCount: view.allocationPageCount, MinimumPageRevision: view.minimumPageRevision, MaximumPageRevision: view.maximumPageRevision,
			RetiredPhysicalChunkCount: len(intersection), RetiredPhysicalChunkIDs: intersection,
		})
	}
	return protectedIntersection, unprotected, views, nil
}

func (c *summaryAuthorityCapture) protectedPhysicalChunkIDs(ctx context.Context, reader kvReadSnapshot, volumeID string) (map[uint64]struct{}, error) {
	if c.protectedChunksByVolume == nil {
		c.protectedChunksByVolume = make(map[string]map[uint64]struct{})
	}
	if cached, found := c.protectedChunksByVolume[volumeID]; found {
		return cached, nil
	}
	if err := c.loadReadViewRecords(ctx, reader); err != nil {
		return nil, err
	}
	pagePrefixes := []struct {
		kind   string
		id     string
		prefix string
	}{{kind: "live_volume", id: volumeID, prefix: allocationPagesPrefix(c.root, volumeID)}}
	for _, snapshotID := range c.snapshotIDsByVolume[volumeID] {
		pagePrefixes = append(pagePrefixes, struct {
			kind   string
			id     string
			prefix string
		}{kind: "snapshot", id: snapshotID, prefix: snapshotAllocationPagesPrefix(c.root, snapshotID)})
	}
	for _, cloneID := range c.cloneIDsByVolume[volumeID] {
		pagePrefixes = append(pagePrefixes, struct {
			kind   string
			id     string
			prefix string
		}{kind: "clone_delta", id: cloneID, prefix: cloneDeltaAllocationPagesPrefix(c.root, cloneID)})
	}
	protected := make(map[uint64]struct{})
	views := make([]summaryProtectingReadView, 0, len(pagePrefixes))
	for _, source := range pagePrefixes {
		keys, err := c.listFiltered(ctx, reader, source.prefix, func(string) bool { return true })
		if err != nil {
			return nil, err
		}
		values, err := c.batchGet(ctx, reader, keys)
		if err != nil {
			return nil, err
		}
		viewChunks := make(map[uint64]struct{})
		minimumPageRevision := uint64(0)
		maximumPageRevision := uint64(0)
		for _, key := range keys {
			var page AllocationPageRecord
			if err := decodeRequiredAuthorityJSON(values, key, &page); err != nil {
				return nil, err
			}
			if err := addProtectedPhysicalChunks(viewChunks, page); err != nil {
				return nil, fmt.Errorf("protected physical chunks from %s: %w", key, err)
			}
			if minimumPageRevision == 0 || page.Revision < minimumPageRevision {
				minimumPageRevision = page.Revision
			}
			if page.Revision > maximumPageRevision {
				maximumPageRevision = page.Revision
			}
		}
		for chunkID := range viewChunks {
			protected[chunkID] = struct{}{}
		}
		if len(viewChunks) > 0 {
			views = append(views, summaryProtectingReadView{
				kind: source.kind, id: source.id, allocationPageCount: len(keys),
				minimumPageRevision: minimumPageRevision, maximumPageRevision: maximumPageRevision, chunks: viewChunks,
			})
		}
	}
	c.protectedChunksByVolume[volumeID] = protected
	if c.protectingViewsByVolume == nil {
		c.protectingViewsByVolume = make(map[string][]summaryProtectingReadView)
	}
	c.protectingViewsByVolume[volumeID] = views
	return protected, nil
}

func summarizeRetiredCandidateOrigins(operations []MutationOperationRecord, retiredChunkIDs []uint64) SummaryRetiredCandidateOriginEvidence {
	evidence := SummaryRetiredCandidateOriginEvidence{}
	if len(retiredChunkIDs) == 0 {
		return evidence
	}
	candidates := make(map[uint64]struct{}, len(retiredChunkIDs))
	for _, chunkID := range retiredChunkIDs {
		candidates[chunkID] = struct{}{}
	}
	for _, operation := range operations {
		if operation.State != MutationOperationCommitted || (operation.Kind != "write" && operation.Kind != "transition") {
			continue
		}
		intersects := false
		for _, chunkID := range operation.RetiredPhysicalChunkIDs {
			if _, found := candidates[chunkID]; found {
				intersects = true
				break
			}
		}
		if !intersects {
			continue
		}
		evidence.OperationCount++
		if evidence.KindCounts == nil {
			evidence.KindCounts = make(map[string]int)
		}
		evidence.KindCounts[operation.Kind]++
		if operation.AllocationRevision > 0 && (evidence.MinimumAllocationRevision == 0 || operation.AllocationRevision < evidence.MinimumAllocationRevision) {
			evidence.MinimumAllocationRevision = operation.AllocationRevision
		}
		if operation.AllocationRevision > evidence.MaximumAllocationRevision {
			evidence.MaximumAllocationRevision = operation.AllocationRevision
		}
		updatedAt := operation.LastUpdatedAtUnix
		if updatedAt == 0 {
			updatedAt = operation.StartedAtUnix
		}
		if updatedAt > 0 && (evidence.EarliestUpdatedAtUnix == 0 || updatedAt < evidence.EarliestUpdatedAtUnix) {
			evidence.EarliestUpdatedAtUnix = updatedAt
		}
		if updatedAt > evidence.LatestUpdatedAtUnix {
			evidence.LatestUpdatedAtUnix = updatedAt
		}
	}
	return evidence
}

func (c *summaryAuthorityCapture) staleReplicaInspectionEvidence(ctx context.Context, reader kvReadSnapshot, volumeID string, retiredChunkIDs []uint64, operations []MutationOperationRecord, transitionsByPlacement map[string]PlacementTransitionRecord) (SummaryStaleReplicaInspectionEvidence, error) {
	evidence := SummaryStaleReplicaInspectionEvidence{}
	if len(retiredChunkIDs) == 0 {
		return evidence, nil
	}
	candidates := make(map[uint64]struct{}, len(retiredChunkIDs))
	for _, chunkID := range retiredChunkIDs {
		candidates[chunkID] = struct{}{}
	}
	originsByChunk := make(map[uint64][]MutationOperationRecord)
	for _, operation := range operations {
		if operation.State != MutationOperationCommitted || (operation.Kind != "write" && operation.Kind != "transition") {
			continue
		}
		for _, chunkID := range sortedNonzeroUint64s(operation.RetiredPhysicalChunkIDs) {
			if _, found := candidates[chunkID]; found {
				originsByChunk[chunkID] = append(originsByChunk[chunkID], operation)
			}
		}
	}
	type targetAccumulator struct {
		nodeID         string
		replicaIDs     map[string]struct{}
		chunkIDs       map[uint64]struct{}
		operationIDs   map[string]struct{}
		authorityKinds map[string]struct{}
	}
	type candidateTarget struct {
		nodeID           string
		sourceReplicaIDs []string
		operationID      string
		authorityKind    string
	}
	targets := make(map[string]*targetAccumulator)
	for _, chunkID := range retiredChunkIDs {
		origins := originsByChunk[chunkID]
		resolved := len(origins) > 0
		candidateTargets := make([]candidateTarget, 0)
		currentNodeIDs := make(map[string]struct{})
		usedPersistedTarget := false
		usedLiveTransitionTarget := false
		usedClusterExclusionTarget := false
		for _, origin := range origins {
			if origin.Kind != "transition" || len(origin.AffectedExtentIDs) == 0 {
				resolved = false
				break
			}
			authority, err := c.loadVolumePlacementAuthority(ctx, reader, volumeID)
			if err != nil {
				return evidence, err
			}
			for _, extentID := range origin.AffectedExtentIDs {
				mapping, found := authority.extentsByID[extentID]
				if !found {
					resolved = false
					break
				}
				current, found := authority.replicaSetsByPlacement[mapping.PlacementRef]
				if !found || len(current.Replicas) == 0 {
					resolved = false
					break
				}
				for _, replica := range current.Replicas {
					if replica.NodeID == "" || replica.ReplicaID == "" {
						resolved = false
						break
					}
					if _, member := c.inspectionNodeIDSet[replica.NodeID]; !member {
						resolved = false
						break
					}
					currentNodeIDs[replica.NodeID] = struct{}{}
				}
				if !resolved {
					break
				}
			}
			if !resolved {
				break
			}
			if origin.RetiredReplicaTargetsResolved {
				if err := validateMutationRetiredReplicaTargets(origin); err != nil {
					return evidence, err
				}
				usedPersistedTarget = true
				for _, target := range origin.RetiredReplicaTargets {
					candidateTargets = append(candidateTargets, candidateTarget{
						nodeID: target.NodeID, sourceReplicaIDs: target.SourceReplicaIDs,
						operationID: origin.OperationID, authorityKind: "persisted_transition_source",
					})
				}
				continue
			}
			transition, found := transitionsByPlacement[origin.IdempotencyKey]
			if found && transition.State == PlacementTransitionCompleted && transition.CurrentReplicaSetID != "" {
				source, sourceFound := authority.replicaSetsByID[transition.CurrentReplicaSetID]
				if sourceFound && source.PlacementRef == transition.PlacementRef {
					usedLiveTransitionTarget = true
					for _, replica := range source.Replicas {
						if replica.NodeID == "" || replica.ReplicaID == "" {
							resolved = false
							break
						}
						candidateTargets = append(candidateTargets, candidateTarget{
							nodeID: replica.NodeID, sourceReplicaIDs: []string{replica.ReplicaID},
							operationID: origin.OperationID, authorityKind: "live_transition_source",
						})
					}
					if !resolved {
						break
					}
					continue
				}
			}
			usedClusterExclusionTarget = true
			if len(c.inspectionNodeIDs) == 0 {
				resolved = false
				break
			}
			for _, nodeID := range c.inspectionNodeIDs {
				candidateTargets = append(candidateTargets, candidateTarget{
					nodeID: nodeID, operationID: origin.OperationID, authorityKind: "noncurrent_cluster_node",
				})
			}
		}
		filteredTargets := candidateTargets[:0]
		for _, target := range candidateTargets {
			// Physical chunks are node-local volume objects rather than
			// replica-qualified objects. A node retained by any current
			// affected placement must remain protected even when its replica
			// identity changed or it was retired by an older transition.
			if _, current := currentNodeIDs[target.nodeID]; current {
				continue
			}
			if _, member := c.inspectionNodeIDSet[target.nodeID]; !member {
				resolved = false
				break
			}
			filteredTargets = append(filteredTargets, target)
		}
		if !resolved {
			evidence.UnresolvedCandidateIDs = append(evidence.UnresolvedCandidateIDs, chunkID)
			continue
		}
		evidence.ResolvedCandidateCount++
		if usedPersistedTarget {
			evidence.PersistedTargetCandidateCount++
		}
		if usedLiveTransitionTarget {
			evidence.LiveTransitionTargetCandidateCount++
		}
		if usedClusterExclusionTarget {
			evidence.ClusterExclusionTargetCandidateCount++
		}
		for _, candidateTarget := range filteredTargets {
			key := candidateTarget.nodeID
			target := targets[key]
			if target == nil {
				target = &targetAccumulator{
					nodeID: candidateTarget.nodeID, replicaIDs: make(map[string]struct{}), chunkIDs: make(map[uint64]struct{}),
					operationIDs: make(map[string]struct{}), authorityKinds: make(map[string]struct{}),
				}
				targets[key] = target
			}
			for _, replicaID := range candidateTarget.sourceReplicaIDs {
				target.replicaIDs[replicaID] = struct{}{}
			}
			target.chunkIDs[chunkID] = struct{}{}
			target.operationIDs[candidateTarget.operationID] = struct{}{}
			target.authorityKinds[candidateTarget.authorityKind] = struct{}{}
		}
	}
	evidence.UnresolvedCandidateCount = len(evidence.UnresolvedCandidateIDs)
	targetKeys := make([]string, 0, len(targets))
	for key := range targets {
		targetKeys = append(targetKeys, key)
	}
	sort.Strings(targetKeys)
	for _, key := range targetKeys {
		target := targets[key]
		chunkIDs := sortedUint64Set(target.chunkIDs)
		evidence.Targets = append(evidence.Targets, SummaryStaleReplicaInspectionTargetEvidence{
			NodeID: target.nodeID, SourceReplicaIDs: sortedStringSet(target.replicaIDs), AuthorityKinds: sortedStringSet(target.authorityKinds),
			RetiredPhysicalChunkCount: len(chunkIDs), RetiredPhysicalChunkIDs: chunkIDs,
			OriginOperationIDs: sortedStringSet(target.operationIDs),
		})
	}
	evidence.TargetCount = len(evidence.Targets)
	return evidence, nil
}

func (c *summaryAuthorityCapture) loadVolumePlacementAuthority(ctx context.Context, reader kvReadSnapshot, volumeID string) (summaryVolumePlacementAuthority, error) {
	if c.placementAuthorityByVolume == nil {
		c.placementAuthorityByVolume = make(map[string]summaryVolumePlacementAuthority)
	}
	if cached, found := c.placementAuthorityByVolume[volumeID]; found {
		return cached, nil
	}
	authority := summaryVolumePlacementAuthority{
		extentsByID:            make(map[uint64]ExtentMappingRecord),
		replicaSetsByID:        make(map[string]ReplicaSetState),
		replicaSetsByPlacement: make(map[string]ReplicaSetState),
	}
	extentKeys, err := c.listFiltered(ctx, reader, extentMappingsPrefix(c.root, volumeID), func(string) bool { return true })
	if err != nil {
		return authority, err
	}
	extentValues, err := c.batchGet(ctx, reader, extentKeys)
	if err != nil {
		return authority, err
	}
	for _, key := range extentKeys {
		var record ExtentMappingRecord
		if err := decodeRequiredAuthorityJSON(extentValues, key, &record); err != nil {
			return authority, err
		}
		if record.VolumeID != volumeID || extentMappingKey(c.root, volumeID, record.ExtentID) != key {
			return authority, fmt.Errorf("extent mapping identity differs from key %q", key)
		}
		authority.extentsByID[record.ExtentID] = record
	}
	replicaSetKeys, err := c.listFiltered(ctx, reader, replicaSetsPrefix(c.root, volumeID), func(string) bool { return true })
	if err != nil {
		return authority, err
	}
	replicaSetValues, err := c.batchGet(ctx, reader, replicaSetKeys)
	if err != nil {
		return authority, err
	}
	for _, key := range replicaSetKeys {
		var record ReplicaSetState
		if err := decodeRequiredAuthorityJSON(replicaSetValues, key, &record); err != nil {
			return authority, err
		}
		if record.VolumeID != volumeID || replicaSetKey(c.root, volumeID, record.ReplicaSetID) != key {
			return authority, fmt.Errorf("replica set identity differs from key %q", key)
		}
		if _, duplicate := authority.replicaSetsByPlacement[record.PlacementRef]; duplicate {
			return authority, fmt.Errorf("duplicate replica set placement_ref %q for volume %s", record.PlacementRef, volumeID)
		}
		authority.replicaSetsByID[record.ReplicaSetID] = record
		authority.replicaSetsByPlacement[record.PlacementRef] = record
	}
	c.placementAuthorityByVolume[volumeID] = authority
	return authority, nil
}

func sortedUint64Set(values map[uint64]struct{}) []uint64 {
	out := make([]uint64, 0, len(values))
	for value := range values {
		out = append(out, value)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

func (c *summaryAuthorityCapture) loadReadViewRecords(ctx context.Context, reader kvReadSnapshot) error {
	if c.readViewRecordsLoaded {
		return nil
	}
	c.snapshotIDsByVolume = make(map[string][]string)
	c.cloneIDsByVolume = make(map[string][]string)
	c.cloneReferenceIDsByVolume = make(map[string][]string)
	snapshotKeys, err := c.listFiltered(ctx, reader, snapshotsPrefix(c.root), func(key string) bool { return strings.HasSuffix(key, "/record") })
	if err != nil {
		return err
	}
	snapshotValues, err := c.batchGet(ctx, reader, snapshotKeys)
	if err != nil {
		return err
	}
	for _, key := range snapshotKeys {
		var record SnapshotRecord
		if err := decodeRequiredAuthorityJSON(snapshotValues, key, &record); err != nil {
			return err
		}
		if snapshotRecordKey(c.root, record.SnapshotID) != key {
			return fmt.Errorf("snapshot read-view identity differs from key %q", key)
		}
		if canonical, err := CanonicalVolumeID(record.SourceVolumeID); err != nil || canonical != record.SourceVolumeID {
			return fmt.Errorf("snapshot read-view has invalid source volume in key %q", key)
		}
		if record.State != SnapshotStateDeleted {
			c.snapshotIDsByVolume[record.SourceVolumeID] = append(c.snapshotIDsByVolume[record.SourceVolumeID], record.SnapshotID)
		}
	}
	cloneKeys, err := c.listFiltered(ctx, reader, clonesPrefix(c.root), func(key string) bool { return strings.HasSuffix(key, "/record") })
	if err != nil {
		return err
	}
	cloneValues, err := c.batchGet(ctx, reader, cloneKeys)
	if err != nil {
		return err
	}
	for _, key := range cloneKeys {
		var record CloneRecord
		if err := decodeRequiredAuthorityJSON(cloneValues, key, &record); err != nil {
			return err
		}
		if cloneRecordKey(c.root, record.CloneID) != key {
			return fmt.Errorf("clone read-view identity differs from key %q", key)
		}
		if canonical, err := CanonicalVolumeID(record.SourceVolumeID); err != nil || canonical != record.SourceVolumeID {
			return fmt.Errorf("clone read-view has invalid source volume in key %q", key)
		}
		if record.State != CloneStateDeleted {
			c.cloneReferenceIDsByVolume[record.SourceVolumeID] = append(c.cloneReferenceIDsByVolume[record.SourceVolumeID], record.CloneID)
		}
		if record.State == CloneStateAvailable || record.State == CloneStateMaterializing {
			c.cloneIDsByVolume[record.SourceVolumeID] = append(c.cloneIDsByVolume[record.SourceVolumeID], record.CloneID)
		}
	}
	for volumeID := range c.snapshotIDsByVolume {
		sort.Strings(c.snapshotIDsByVolume[volumeID])
	}
	for volumeID := range c.cloneIDsByVolume {
		sort.Strings(c.cloneIDsByVolume[volumeID])
	}
	for volumeID := range c.cloneReferenceIDsByVolume {
		sort.Strings(c.cloneReferenceIDsByVolume[volumeID])
	}
	c.readViewRecordsLoaded = true
	return nil
}

func addProtectedPhysicalChunks(target map[uint64]struct{}, page AllocationPageRecord) error {
	for _, extent := range page.Extents {
		if (extent.Kind != AllocationKindData && extent.Kind != AllocationKindShared) || extent.PhysicalChunkStart == 0 || extent.ChunkCount == 0 {
			continue
		}
		last := extent.PhysicalChunkStart + uint64(extent.ChunkCount) - 1
		if last < extent.PhysicalChunkStart {
			return fmt.Errorf("physical chunk range overflows uint64")
		}
		for chunkID := extent.PhysicalChunkStart; ; chunkID++ {
			target[chunkID] = struct{}{}
			if chunkID == last {
				break
			}
		}
	}
	return nil
}

func sortedNonzeroUint64s(values []uint64) []uint64 {
	set := make(map[uint64]struct{}, len(values))
	for _, value := range values {
		if value != 0 {
			set[value] = struct{}{}
		}
	}
	out := make([]uint64, 0, len(set))
	for value := range set {
		out = append(out, value)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

func (c *summaryAuthorityCapture) captureAdminOperations(ctx context.Context, reader kvReadSnapshot) error {
	keys, err := c.listFiltered(ctx, reader, c.root+"/admin/operations/", func(string) bool { return true })
	if err != nil {
		return err
	}
	values, err := c.batchGet(ctx, reader, keys)
	if err != nil {
		return err
	}
	for _, key := range keys {
		var record summaryAdminOperation
		if err := decodeRequiredAuthorityJSON(values, key, &record); err != nil {
			return err
		}
		if adminOperationListKey(c.root, record.OperationID) != key {
			return fmt.Errorf("admin operation identity differs from key %q", key)
		}
		subjectID, counters := AdminOperationSummaryContribution(record.OperationID, record.State, true)
		if err := c.addContribution(subjectID, counters); err != nil {
			return err
		}
		c.evidence.AdminOperationCount++
	}
	return nil
}

func (c *summaryAuthorityCapture) listFiltered(ctx context.Context, reader kvReadSnapshot, prefix string, keep func(string) bool) ([]string, error) {
	var out []string
	cursor := ""
	for {
		keys, next, err := reader.List(ctx, prefix, cursor, c.pageLimit)
		c.evidence.RangePageCount++
		if err != nil {
			return nil, err
		}
		if len(keys) > c.pageLimit || (next != "" && (len(keys) == 0 || next == cursor)) {
			return nil, fmt.Errorf("non-progressing authority page for prefix %q", prefix)
		}
		for _, key := range keys {
			if keep(key) {
				out = append(out, key)
			}
		}
		if next == "" {
			return out, nil
		}
		cursor = next
	}
}

func (c *summaryAuthorityCapture) batchGet(ctx context.Context, reader kvReadSnapshot, keys []string) (map[string][]byte, error) {
	values := make(map[string][]byte, len(keys))
	for start := 0; start < len(keys); start += c.batchLimit {
		end := min(start+c.batchLimit, len(keys))
		batch, err := reader.BatchGet(ctx, keys[start:end])
		c.evidence.BatchGetCount++
		c.evidence.BatchGetKeyCount += end - start
		c.evidence.MaximumBatchGetKeyCount = max(c.evidence.MaximumBatchGetKeyCount, end-start)
		if err != nil {
			return nil, err
		}
		for key, raw := range batch {
			values[key] = raw
		}
	}
	return values, nil
}

func (c *summaryAuthorityCapture) addContribution(subject string, counters SummaryCounters) error {
	contribution, err := NewSummaryContribution(subject, counters)
	if err != nil {
		return err
	}
	c.contributions = append(c.contributions, contribution)
	return nil
}

func (c *summaryAuthorityCapture) reject(class, detail string) {
	if c.unrepresentedClasses == nil {
		c.unrepresentedClasses = make(map[string]struct{})
	}
	c.unrepresentedClasses[class] = struct{}{}
	if c.unrepresentedClassCounts == nil {
		c.unrepresentedClassCounts = make(map[string]int)
	}
	c.unrepresentedClassCounts[class]++
	c.unrepresented = append(c.unrepresented, detail)
}

func decodeRequiredAuthorityJSON(values map[string][]byte, key string, out any) error {
	raw, found := values[key]
	if !found {
		return fmt.Errorf("authority key disappeared from consistent snapshot: %s", key)
	}
	if err := json.Unmarshal(raw, out); err != nil {
		return fmt.Errorf("decode authority key %s: %w", key, err)
	}
	return nil
}

func sortedStringSet(values map[string]struct{}) []string {
	out := make([]string, 0, len(values))
	for value := range values {
		out = append(out, value)
	}
	sort.Strings(out)
	return out
}
