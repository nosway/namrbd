package metadata

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strings"
)

const SummaryRebuildMaxPageSize = 512

var (
	ErrSummaryRebuildSourceChanged    = errors.New("summary rebuild source revision changed")
	ErrSummaryRebuildDuplicateSubject = errors.New("summary rebuild source repeated a subject")
	ErrSummaryRebuildIncomplete       = errors.New("summary rebuild is not complete")
)

type SummaryContribution struct {
	SchemaVersion      int             `json:"schema_version"`
	Kind               string          `json:"kind"`
	SubjectID          string          `json:"subject_id"`
	VirtualShard       int             `json:"virtual_shard"`
	Counters           SummaryCounters `json:"counters"`
	ContributionDigest string          `json:"contribution_digest"`
}

type SummaryRebuildSourcePage struct {
	Contributions  []SummaryContribution `json:"contributions"`
	NextCursor     string                `json:"next_cursor"`
	SourceRevision uint64                `json:"source_revision"`
}

type SummaryRebuildSource interface {
	ListSummaryContributions(ctx context.Context, kind, cursor string, limit int) (SummaryRebuildSourcePage, error)
}

type SummaryRebuildCheckpoint struct {
	SchemaVersion    int    `json:"schema_version"`
	Kind             string `json:"kind"`
	Epoch            string `json:"epoch"`
	SourceRevision   uint64 `json:"source_revision"`
	SourceCursor     string `json:"source_cursor"`
	ProcessedCount   uint64 `json:"processed_count"`
	PageCount        uint64 `json:"page_count"`
	Completed        bool   `json:"completed"`
	Promoted         bool   `json:"promoted"`
	StartedAtUnix    int64  `json:"started_at_unix"`
	UpdatedAtUnix    int64  `json:"updated_at_unix"`
	CheckpointDigest string `json:"checkpoint_digest"`
}

type SummaryRebuildPageResult struct {
	Kind                 string `json:"kind"`
	Epoch                string `json:"epoch"`
	RequestedLimit       int    `json:"requested_limit"`
	InputCount           int    `json:"input_count"`
	TouchedShardCount    int    `json:"touched_shard_count"`
	SourceRevision       uint64 `json:"source_revision"`
	ProcessedCount       uint64 `json:"processed_count"`
	PageCount            uint64 `json:"page_count"`
	NextCursor           string `json:"next_cursor"`
	Completed            bool   `json:"completed"`
	Promoted             bool   `json:"promoted"`
	BackendFullScanCount int    `json:"backend_full_scan_count"`
	RangePageCount       int    `json:"range_page_count"`
	FullCompletionCount  int    `json:"full_completion_count"`
}

func NewSummaryContribution(subjectID string, counters SummaryCounters) (SummaryContribution, error) {
	contribution := SummaryContribution{
		SchemaVersion: SummarySchemaVersion, Kind: SummaryKindCluster,
		SubjectID: strings.TrimSpace(subjectID), Counters: counters,
	}
	contribution.VirtualShard = SummaryVirtualShard(contribution.SubjectID)
	contribution.ContributionDigest = digestSummaryContribution(contribution)
	if err := ValidateSummaryContribution(contribution); err != nil {
		return SummaryContribution{}, err
	}
	return contribution, nil
}

func ValidateSummaryContribution(contribution SummaryContribution) error {
	if contribution.SchemaVersion != SummarySchemaVersion || contribution.Kind != SummaryKindCluster || !validSummaryIdentifier(contribution.SubjectID) || contribution.VirtualShard != SummaryVirtualShard(contribution.SubjectID) {
		return fmt.Errorf("invalid summary contribution identity")
	}
	if err := validateSummaryContribution(contribution.Counters); err != nil {
		return err
	}
	if contribution.ContributionDigest != digestSummaryContribution(contribution) {
		return fmt.Errorf("summary contribution digest mismatch")
	}
	return nil
}

func (r *Repository) RunSummaryRebuildPage(ctx context.Context, source SummaryRebuildSource, kind, epoch string, limit int) (SummaryRebuildPageResult, error) {
	result := SummaryRebuildPageResult{Kind: kind, Epoch: epoch, RequestedLimit: limit}
	if r == nil || source == nil || kind != SummaryKindCluster || !validSummaryIdentifier(epoch) || limit <= 0 || limit > SummaryRebuildMaxPageSize {
		return result, fmt.Errorf("invalid summary rebuild request")
	}
	runner, ok := r.kv.(transactionalKV)
	if !ok {
		return result, ErrSummaryTransactionRequired
	}
	checkpointKey := summaryRebuildCheckpointKey(r.root, kind, epoch)
	var before SummaryRebuildCheckpoint
	found, err := getOptionalSummaryJSON(ctx, r.kv, checkpointKey, &before)
	if err != nil {
		return result, err
	}
	if found {
		if err := validateSummaryRebuildCheckpoint(before); err != nil {
			return result, err
		}
		if before.Completed {
			return summaryRebuildResult(before, limit, 0, 0), nil
		}
	}
	page, err := source.ListSummaryContributions(ctx, kind, before.SourceCursor, limit)
	if err != nil {
		return result, err
	}
	result.RangePageCount = 1
	if page.SourceRevision == 0 || (before.SourceRevision != 0 && page.SourceRevision != before.SourceRevision) {
		return result, ErrSummaryRebuildSourceChanged
	}
	if len(page.Contributions) > limit || (len(page.Contributions) == 0 && page.NextCursor != "") || (page.NextCursor != "" && page.NextCursor == before.SourceCursor) {
		return result, fmt.Errorf("invalid or non-progressing summary rebuild source page")
	}
	previousSubject := ""
	for _, contribution := range page.Contributions {
		if err := ValidateSummaryContribution(contribution); err != nil {
			return result, err
		}
		if previousSubject != "" && contribution.SubjectID <= previousSubject {
			return result, fmt.Errorf("summary rebuild page subjects are not strictly ordered")
		}
		previousSubject = contribution.SubjectID
	}
	now := r.now().UTC().Unix()
	touched := map[int]SummaryCounters{}
	err = runner.RunInTransaction(ctx, func(tx kvReadWriter) error {
		var current SummaryRebuildCheckpoint
		currentFound, err := getOptionalSummaryJSON(ctx, tx, checkpointKey, &current)
		if err != nil {
			return err
		}
		if currentFound != found || (found && current.CheckpointDigest != before.CheckpointDigest) {
			return ErrCASConflict
		}
		if !currentFound {
			current = SummaryRebuildCheckpoint{
				SchemaVersion: SummarySchemaVersion, Kind: kind, Epoch: epoch,
				SourceRevision: page.SourceRevision, StartedAtUnix: now,
			}
		}
		if current.SourceRevision != page.SourceRevision {
			return ErrSummaryRebuildSourceChanged
		}
		for _, contribution := range page.Contributions {
			markerKey := summaryRebuildSubjectKey(r.root, kind, epoch, contribution.VirtualShard, contribution.SubjectID)
			var marker SummaryContribution
			markerFound, err := getOptionalSummaryJSON(ctx, tx, markerKey, &marker)
			if err != nil {
				return err
			}
			if markerFound {
				return fmt.Errorf("%w: %s", ErrSummaryRebuildDuplicateSubject, contribution.SubjectID)
			}
			if err := putSummaryJSON(ctx, tx, markerKey, contribution); err != nil {
				return err
			}
			counters, err := addSummaryCounters(touched[contribution.VirtualShard], contribution.Counters)
			if err != nil {
				return err
			}
			touched[contribution.VirtualShard] = counters
		}
		for _, shardID := range sortedSummaryKeys(touched) {
			key := summaryRebuildAggregateKey(r.root, kind, epoch, shardID)
			shard := newSummaryAggregateShard(kind, shardID, epoch)
			shardFound, err := getOptionalSummaryJSON(ctx, tx, key, &shard)
			if err != nil {
				return err
			}
			if shardFound {
				if err := validateSummaryAggregateShard(shard); err != nil {
					return err
				}
				if shard.RebuildEpoch != epoch {
					return fmt.Errorf("summary rebuild shard epoch mismatch")
				}
			}
			shard.Counters, err = addSummaryCounters(shard.Counters, touched[shardID])
			if err != nil {
				return err
			}
			shard.SourceRevision = page.SourceRevision
			shard.UpdatedAtUnix = now
			shard.Checksum = digestSummaryAggregateShard(shard)
			if err := putSummaryJSON(ctx, tx, key, shard); err != nil {
				return err
			}
		}
		current.SourceCursor = page.NextCursor
		current.ProcessedCount += uint64(len(page.Contributions))
		current.PageCount++
		current.Completed = page.NextCursor == ""
		current.UpdatedAtUnix = now
		current.CheckpointDigest = digestSummaryRebuildCheckpoint(current)
		return putSummaryJSON(ctx, tx, checkpointKey, current)
	})
	if err != nil {
		return result, err
	}
	checkpoint, err := r.GetSummaryRebuildCheckpoint(ctx, kind, epoch)
	if err != nil {
		return result, err
	}
	return summaryRebuildResult(checkpoint, limit, len(page.Contributions), len(touched)), nil
}

func (r *Repository) PromoteSummaryRebuild(ctx context.Context, kind, epoch string, currentSourceRevision uint64) (SummaryAggregateState, error) {
	if r == nil || kind != SummaryKindCluster || !validSummaryIdentifier(epoch) || currentSourceRevision == 0 {
		return SummaryAggregateState{}, fmt.Errorf("invalid summary rebuild promotion request")
	}
	runner, ok := r.kv.(transactionalKV)
	if !ok {
		return SummaryAggregateState{}, ErrSummaryTransactionRequired
	}
	checkpoint, err := r.GetSummaryRebuildCheckpoint(ctx, kind, epoch)
	if err != nil {
		return SummaryAggregateState{}, err
	}
	if !checkpoint.Completed {
		return SummaryAggregateState{}, ErrSummaryRebuildIncomplete
	}
	if checkpoint.SourceRevision != currentSourceRevision {
		return SummaryAggregateState{}, ErrSummaryRebuildSourceChanged
	}
	if checkpoint.Promoted {
		return r.GetSummaryAggregateState(ctx, kind)
	}
	now := r.now().UTC().Unix()
	state := SummaryAggregateState{}
	err = runner.RunInTransaction(ctx, func(tx kvReadWriter) error {
		var current SummaryRebuildCheckpoint
		if err := getSummaryJSON(ctx, tx, summaryRebuildCheckpointKey(r.root, kind, epoch), &current); err != nil {
			return err
		}
		if current.CheckpointDigest != checkpoint.CheckpointDigest || !current.Completed || current.Promoted {
			return ErrCASConflict
		}
		if current.SourceRevision != currentSourceRevision {
			return ErrSummaryRebuildSourceChanged
		}
		for shardID := 0; shardID < SummaryVirtualShardCount; shardID++ {
			var live SummaryAggregateShard
			liveFound, err := getOptionalSummaryJSON(ctx, tx, summaryAggregateKey(r.root, kind, shardID), &live)
			if err != nil {
				return err
			}
			if liveFound {
				if err := validateSummaryAggregateShard(live); err != nil {
					return err
				}
				if live.PendingOutboxCount != 0 {
					return fmt.Errorf("%w: virtual shard %d has %d events", ErrSummaryOutboxPending, shardID, live.PendingOutboxCount)
				}
				if live.SourceRevision > currentSourceRevision {
					return fmt.Errorf("%w: virtual shard %d advanced to revision %d", ErrSummaryRebuildSourceChanged, shardID, live.SourceRevision)
				}
			}
			shadowKey := summaryRebuildAggregateKey(r.root, kind, epoch, shardID)
			shard := newSummaryAggregateShard(kind, shardID, epoch)
			found, err := getOptionalSummaryJSON(ctx, tx, shadowKey, &shard)
			if err != nil {
				return err
			}
			if found {
				if err := validateSummaryAggregateShard(shard); err != nil {
					return err
				}
			}
			shard.SourceRevision = currentSourceRevision
			shard.UpdatedAtUnix = now
			shard.RebuildEpoch = epoch
			shard.Checksum = digestSummaryAggregateShard(shard)
			if err := putSummaryJSON(ctx, tx, summaryAggregateKey(r.root, kind, shardID), shard); err != nil {
				return err
			}
		}
		state = SummaryAggregateState{
			SchemaVersion: SummarySchemaVersion, Kind: kind, State: SummaryAggregateStateReady,
			ActiveEpoch: epoch, SourceRevision: currentSourceRevision, UpdatedAtUnix: now,
		}
		state.StateDigest = digestSummaryAggregateState(state)
		if err := putSummaryJSON(ctx, tx, summaryAggregateStateKey(r.root, kind), state); err != nil {
			return err
		}
		current.Promoted = true
		current.UpdatedAtUnix = now
		current.CheckpointDigest = digestSummaryRebuildCheckpoint(current)
		return putSummaryJSON(ctx, tx, summaryRebuildCheckpointKey(r.root, kind, epoch), current)
	})
	if err != nil {
		return SummaryAggregateState{}, err
	}
	return state, validateSummaryAggregateState(state)
}

func (r *Repository) GetSummaryRebuildCheckpoint(ctx context.Context, kind, epoch string) (SummaryRebuildCheckpoint, error) {
	if r == nil || kind != SummaryKindCluster || !validSummaryIdentifier(epoch) {
		return SummaryRebuildCheckpoint{}, fmt.Errorf("invalid summary rebuild checkpoint request")
	}
	var checkpoint SummaryRebuildCheckpoint
	if err := getSummaryJSON(ctx, r.kv, summaryRebuildCheckpointKey(r.root, kind, epoch), &checkpoint); err != nil {
		return SummaryRebuildCheckpoint{}, err
	}
	if err := validateSummaryRebuildCheckpoint(checkpoint); err != nil {
		return SummaryRebuildCheckpoint{}, err
	}
	return checkpoint, nil
}

func validateSummaryRebuildCheckpoint(checkpoint SummaryRebuildCheckpoint) error {
	if checkpoint.SchemaVersion != SummarySchemaVersion || checkpoint.Kind != SummaryKindCluster || !validSummaryIdentifier(checkpoint.Epoch) || checkpoint.SourceRevision == 0 || checkpoint.PageCount == 0 || checkpoint.StartedAtUnix <= 0 || checkpoint.UpdatedAtUnix < checkpoint.StartedAtUnix {
		return fmt.Errorf("invalid summary rebuild checkpoint")
	}
	if checkpoint.Promoted && !checkpoint.Completed {
		return fmt.Errorf("promoted summary rebuild is not complete")
	}
	if checkpoint.CheckpointDigest != digestSummaryRebuildCheckpoint(checkpoint) {
		return fmt.Errorf("summary rebuild checkpoint digest mismatch")
	}
	return nil
}

func summaryRebuildResult(checkpoint SummaryRebuildCheckpoint, limit, inputCount, touchedShards int) SummaryRebuildPageResult {
	return SummaryRebuildPageResult{
		Kind: checkpoint.Kind, Epoch: checkpoint.Epoch, RequestedLimit: limit,
		InputCount: inputCount, TouchedShardCount: touchedShards,
		SourceRevision: checkpoint.SourceRevision, ProcessedCount: checkpoint.ProcessedCount,
		PageCount: checkpoint.PageCount, NextCursor: checkpoint.SourceCursor,
		Completed: checkpoint.Completed, Promoted: checkpoint.Promoted,
		RangePageCount: map[bool]int{true: 0, false: 1}[checkpoint.Completed && inputCount == 0],
	}
}

func addSummaryCounters(left, right SummaryCounters) (SummaryCounters, error) {
	leftValues := summaryCounterValues(left)
	rightValues := summaryCounterValues(right)
	for index := range leftValues {
		if math.MaxUint64-leftValues[index] < rightValues[index] {
			return SummaryCounters{}, fmt.Errorf("%w at field %d", ErrSummaryCounterOverflow, index)
		}
		leftValues[index] += rightValues[index]
	}
	return summaryCountersFromValues(leftValues), nil
}

func digestSummaryContribution(contribution SummaryContribution) string {
	contribution.ContributionDigest = ""
	return digestSummaryValue(contribution)
}

func digestSummaryRebuildCheckpoint(checkpoint SummaryRebuildCheckpoint) string {
	checkpoint.CheckpointDigest = ""
	return digestSummaryValue(checkpoint)
}

func summaryRebuildCheckpointKey(root, kind, epoch string) string {
	return fmt.Sprintf("%s/derived/ad/v1/rebuild/%s/%s/checkpoint", root, kind, epoch)
}

func summaryRebuildAggregateKey(root, kind, epoch string, shard int) string {
	return fmt.Sprintf("%s/derived/ad/v1/rebuild/%s/%s/aggregate/%02d", root, kind, epoch, shard)
}

func summaryRebuildSubjectKey(root, kind, epoch string, shard int, subjectID string) string {
	return fmt.Sprintf("%s/derived/ad/v1/rebuild/%s/%s/subject/%02d/%s", root, kind, epoch, shard, subjectID)
}
