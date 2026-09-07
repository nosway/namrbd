package main

import (
	"context"
	"errors"
	"maps"
	"sync"
	"time"

	"github.com/nosway/namrbd/internal/structuredlog"
	clustermeta "github.com/nosway/namrbd/sbs/cluster/metadata"
)

// phaseADCurrentObservability is deliberately in-memory and observation-only.
// It records work that the current path already performed; it never adds a
// repository call, retry, timeout, or mutation.
type phaseADCurrentObservability struct {
	mu sync.Mutex

	operationListRequestsByFilter map[string]uint64
	operationListFailuresByStage  map[string]uint64
	operationListDurationNanos    map[string]int64
	operationListStoredReturned   uint64
	operationListMutationScanned  uint64
	operationListMutationReturned uint64
	legacyExpensiveBySurface      map[string]map[string]uint64

	drainErrorsByStageAndClass map[string]uint64
	lastDrainError             phaseADDrainErrorObservation
}

type phaseADDrainErrorObservation struct {
	OperationID  string `json:"operation_id,omitempty"`
	NodeID       string `json:"node_id,omitempty"`
	Stage        string `json:"stage,omitempty"`
	VolumeID     string `json:"volume_id,omitempty"`
	ExtentID     uint64 `json:"extent_id,omitempty"`
	ErrorClass   string `json:"error_class,omitempty"`
	ErrorMessage string `json:"error_message,omitempty"`
}

type phaseADCurrentObservabilitySnapshot struct {
	OperationListRequestsByFilter map[string]uint64            `json:"operation_list_requests_by_filter"`
	OperationListFailuresByStage  map[string]uint64            `json:"operation_list_failures_by_stage"`
	OperationListDurationNanos    map[string]int64             `json:"operation_list_duration_nanos"`
	OperationListStoredReturned   uint64                       `json:"operation_list_stored_returned_total"`
	OperationListMutationScanned  uint64                       `json:"operation_list_mutation_scanned_total"`
	OperationListMutationReturned uint64                       `json:"operation_list_mutation_returned_total"`
	LegacyExpensiveBySurface      map[string]map[string]uint64 `json:"legacy_expensive_by_surface"`
	DrainErrorsByStageAndClass    map[string]uint64            `json:"drain_errors_by_stage_and_class"`
	LastDrainError                phaseADDrainErrorObservation `json:"last_drain_error"`
}

func (o *phaseADCurrentObservability) recordLegacyExpensive(surface, outcome string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.legacyExpensiveBySurface == nil {
		o.legacyExpensiveBySurface = make(map[string]map[string]uint64)
	}
	if o.legacyExpensiveBySurface[surface] == nil {
		o.legacyExpensiveBySurface[surface] = make(map[string]uint64)
	}
	o.legacyExpensiveBySurface[surface][outcome]++
}

func operationListFilterClass(kind string, state int32) string {
	switch {
	case kind != "" && state != 0:
		return "kind_state"
	case kind != "":
		return "kind"
	case state != 0:
		return "state"
	default:
		return "all"
	}
}

func (o *phaseADCurrentObservability) recordOperationList(filter, failureStage string, duration time.Duration, storedReturned, mutationScanned, mutationReturned int) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.operationListRequestsByFilter == nil {
		o.operationListRequestsByFilter = make(map[string]uint64)
		o.operationListFailuresByStage = make(map[string]uint64)
		o.operationListDurationNanos = make(map[string]int64)
	}
	o.operationListRequestsByFilter[filter]++
	o.operationListDurationNanos[filter] += duration.Nanoseconds()
	if failureStage != "" {
		o.operationListFailuresByStage[failureStage]++
	}
	if storedReturned > 0 {
		o.operationListStoredReturned += uint64(storedReturned)
	}
	if mutationScanned > 0 {
		o.operationListMutationScanned += uint64(mutationScanned)
	}
	if mutationReturned > 0 {
		o.operationListMutationReturned += uint64(mutationReturned)
	}
}

func (o *phaseADCurrentObservability) recordDrainError(observation phaseADDrainErrorObservation) bool {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.drainErrorsByStageAndClass == nil {
		o.drainErrorsByStageAndClass = make(map[string]uint64)
	}
	key := observation.Stage + ":" + observation.ErrorClass
	o.drainErrorsByStageAndClass[key]++
	changed := o.lastDrainError != observation
	o.lastDrainError = observation
	return changed
}

func (o *phaseADCurrentObservability) snapshot() phaseADCurrentObservabilitySnapshot {
	o.mu.Lock()
	defer o.mu.Unlock()
	legacy := make(map[string]map[string]uint64, len(o.legacyExpensiveBySurface))
	for surface, outcomes := range o.legacyExpensiveBySurface {
		legacy[surface] = maps.Clone(outcomes)
	}
	return phaseADCurrentObservabilitySnapshot{
		OperationListRequestsByFilter: maps.Clone(o.operationListRequestsByFilter),
		OperationListFailuresByStage:  maps.Clone(o.operationListFailuresByStage),
		OperationListDurationNanos:    maps.Clone(o.operationListDurationNanos),
		OperationListStoredReturned:   o.operationListStoredReturned,
		OperationListMutationScanned:  o.operationListMutationScanned,
		OperationListMutationReturned: o.operationListMutationReturned,
		LegacyExpensiveBySurface:      legacy,
		DrainErrorsByStageAndClass:    maps.Clone(o.drainErrorsByStageAndClass),
		LastDrainError:                o.lastDrainError,
	}
}

type phaseADDrainObservationContextKey struct{}

type phaseADDrainObservationScope struct {
	OperationID string
	NodeID      string
}

func withPhaseADDrainObservation(ctx context.Context, operationID, nodeID string) context.Context {
	return context.WithValue(ctx, phaseADDrainObservationContextKey{}, phaseADDrainObservationScope{
		OperationID: operationID,
		NodeID:      nodeID,
	})
}

func phaseADDrainErrorClass(err error) string {
	if errors.Is(err, clustermeta.ErrNotFound) {
		return "metadata_not_found"
	}
	return "other"
}

func (s *server) observePhaseADDrainError(ctx context.Context, stage, nodeID, volumeID string, extentID uint64, err error) {
	if err == nil {
		return
	}
	scope, _ := ctx.Value(phaseADDrainObservationContextKey{}).(phaseADDrainObservationScope)
	if scope.NodeID != "" {
		nodeID = scope.NodeID
	}
	observation := phaseADDrainErrorObservation{
		OperationID:  scope.OperationID,
		NodeID:       nodeID,
		Stage:        stage,
		VolumeID:     volumeID,
		ExtentID:     extentID,
		ErrorClass:   phaseADDrainErrorClass(err),
		ErrorMessage: err.Error(),
	}
	if s.phaseADCurrentObservability.recordDrainError(observation) {
		structuredlog.Error("sbs.service", "phase_ad_drain_observation_error", err,
			structuredlog.F("operation_id", observation.OperationID),
			structuredlog.F("node_id", observation.NodeID),
			structuredlog.F("stage", observation.Stage),
			structuredlog.F("volume_id", observation.VolumeID),
			structuredlog.F("extent_id", observation.ExtentID),
			structuredlog.F("error_class", observation.ErrorClass),
		)
	}
}
