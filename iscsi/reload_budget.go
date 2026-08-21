package iscsi

import (
	"fmt"
	"sync/atomic"
)

// ReloadOutcome is what a gateway should do with a registry response.
type ReloadOutcome string

const (
	// ReloadApply means the response is newer and should be applied.
	ReloadApply ReloadOutcome = "apply"
	// ReloadSkip means the response matches what is already serving, so
	// nothing is parsed or re-applied.
	ReloadSkip ReloadOutcome = "skip"
	// ReloadRejectStale means the response is older than what is serving.
	ReloadRejectStale ReloadOutcome = "reject_stale"
)

// ReloadDecision carries the outcome and why.
type ReloadDecision struct {
	Outcome ReloadOutcome
	// Reason is written for an operator reading a log line, not for a
	// developer reading a stack trace.
	Reason string
	// FromRevision and ToRevision bracket the change.
	FromRevision uint64
	ToRevision   uint64
}

// ReloadState tracks what a gateway is currently serving.
//
// The registry response carries every portal, target, LUN, ACL, session, and
// failover record, so at t2_large a poll returns a thousand exports' worth of
// data whether or not anything changed. Comparing revisions before parsing or
// re-applying is what keeps a quiet cluster cheap; making the fetch itself
// incremental needs a changed-set RPC and is AA-IMPL-011.
type ReloadState struct {
	appliedRevision   atomic.Uint64
	appliedGeneration atomic.Uint64

	reloads         atomic.Int64
	skipped         atomic.Int64
	rejectedStale   atomic.Int64
	registryFetches atomic.Int64
}

// Decide compares a fetched registry against what is being served.
//
// A response older than what is serving is rejected rather than applied. A
// gateway reading from a lagging replica would otherwise move its serving
// mappings backwards, which looks like a spontaneous failover to an initiator
// and undoes a change an operator just made.
func (s *ReloadState) Decide(revision, generation uint64) ReloadDecision {
	s.registryFetches.Add(1)
	applied := s.appliedRevision.Load()
	appliedGen := s.appliedGeneration.Load()

	switch {
	case revision < applied:
		s.rejectedStale.Add(1)
		return ReloadDecision{
			Outcome:      ReloadRejectStale,
			Reason:       fmt.Sprintf("registry revision %d is older than the serving revision %d; the response came from a lagging replica", revision, applied),
			FromRevision: applied,
			ToRevision:   revision,
		}
	case revision == applied && generation == appliedGen:
		s.skipped.Add(1)
		return ReloadDecision{
			Outcome:      ReloadSkip,
			Reason:       "registry revision and config generation are unchanged",
			FromRevision: applied,
			ToRevision:   revision,
		}
	case revision == applied && generation < appliedGen:
		s.rejectedStale.Add(1)
		return ReloadDecision{
			Outcome:      ReloadRejectStale,
			Reason:       fmt.Sprintf("config generation %d is older than the serving generation %d at the same revision %d", generation, appliedGen, revision),
			FromRevision: applied,
			ToRevision:   revision,
		}
	default:
		return ReloadDecision{
			Outcome:      ReloadApply,
			Reason:       fmt.Sprintf("registry advanced from revision %d to %d", applied, revision),
			FromRevision: applied,
			ToRevision:   revision,
		}
	}
}

// Applied records a reload that was actually applied.
//
// It is separate from Decide so a failed apply does not advance the serving
// revision. A gateway that recorded the new revision and then failed to apply
// it would skip every later poll and serve the old mappings forever while
// reporting itself current.
func (s *ReloadState) Applied(revision, generation uint64) {
	s.appliedRevision.Store(revision)
	s.appliedGeneration.Store(generation)
	s.reloads.Add(1)
}

// ReloadSnapshot is the observable form.
type ReloadSnapshot struct {
	RegistryReloadCount      int64  `json:"registry_reload_count"`
	RegistryReloadRevision   uint64 `json:"registry_reload_revision"`
	RegistrySkippedCount     int64  `json:"registry_skipped_generation_count"`
	RegistryStaleRejectCount int64  `json:"registry_stale_reject_count"`
	// RegistryFetchCount includes bounded manifest and changed-set requests.
	// It is not an unbounded TiKV scan and must not consume the scan budget.
	RegistryFetchCount       int64  `json:"registry_fetch_count"`
	TiKVRegistryReloadScans  int64  `json:"tikv_registry_reload_scan_count"`
	RegistryConfigGeneration uint64 `json:"registry_config_generation"`
}

// Snapshot returns the current counts.
func (s *ReloadState) Snapshot() ReloadSnapshot {
	if s == nil {
		return ReloadSnapshot{}
	}
	return ReloadSnapshot{
		RegistryReloadCount:      s.reloads.Load(),
		RegistryReloadRevision:   s.appliedRevision.Load(),
		RegistrySkippedCount:     s.skipped.Load(),
		RegistryStaleRejectCount: s.rejectedStale.Load(),
		RegistryFetchCount:       s.registryFetches.Load(),
		RegistryConfigGeneration: s.appliedGeneration.Load(),
	}
}
