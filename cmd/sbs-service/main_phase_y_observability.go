package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	adminv1 "github.com/nosway/namrbd/sbs/admin/v1"
	clustermeta "github.com/nosway/namrbd/sbs/cluster/metadata"
	"github.com/nosway/namrbd/sbs/observability"
)

func (s *server) registerPhaseYOperationsAPI(mux *http.ServeMux) {
	mux.HandleFunc("/api/v1/sbs/cluster", func(w http.ResponseWriter, r *http.Request) {
		s.handlePhaseADFleetSummary(w, r)
	})
	mux.HandleFunc("/api/v1/sbs/nodes", func(w http.ResponseWriter, r *http.Request) {
		s.handlePhaseADNodePage(w, r)
	})
	mux.HandleFunc("/api/v1/sbs/node", func(w http.ResponseWriter, r *http.Request) {
		s.handlePhaseADNodePoint(w, r)
	})
	mux.HandleFunc("/api/v1/sbs/volumes", func(w http.ResponseWriter, r *http.Request) {
		s.handlePhaseADVolumePage(w, r)
	})
	mux.HandleFunc("/api/v1/sbs/volume", func(w http.ResponseWriter, r *http.Request) {
		s.handlePhaseADVolumePoint(w, r)
	})
	mux.HandleFunc("/api/v1/sbs/maintenance", func(w http.ResponseWriter, r *http.Request) {
		s.handlePhaseYView(w, r, "sbs.maintenance", func(snapshot observability.Snapshot) any {
			return map[string]any{"maintenance": snapshot.Maintenance, "operations": snapshot.Operations}
		})
	})
	mux.HandleFunc("/api/v1/sbs/capacity", func(w http.ResponseWriter, r *http.Request) {
		s.handlePhaseYView(w, r, "sbs.capacity", func(snapshot observability.Snapshot) any {
			return snapshot.Capacity
		})
	})
	mux.HandleFunc("/api/v1/sbs/reclaim", func(w http.ResponseWriter, r *http.Request) {
		s.handlePhaseYView(w, r, "sbs.reclaim", func(snapshot observability.Snapshot) any {
			return snapshot.Reclaim
		})
	})
	mux.HandleFunc("/api/v1/membership/status", func(w http.ResponseWriter, r *http.Request) {
		s.handlePhaseYView(w, r, "membership.status", func(snapshot observability.Snapshot) any {
			return snapshot.Membership
		})
	})
	mux.HandleFunc("/api/v1/operations/summary", func(w http.ResponseWriter, r *http.Request) {
		s.handlePhaseYView(w, r, "operations.summary", func(snapshot observability.Snapshot) any {
			return snapshot.Operations
		})
	})
	mux.HandleFunc("/api/v1/operations/warnings", func(w http.ResponseWriter, r *http.Request) {
		s.handlePhaseYView(w, r, "operations.warnings", func(snapshot observability.Snapshot) any {
			return map[string]any{
				"collection_status": snapshot.CollectionStatus,
				"warnings":          snapshot.Warnings,
				"warning_count":     snapshot.WarningCount,
				"first_error":       snapshot.FirstError,
				"last_error":        snapshot.LastError,
				"limitations":       snapshot.Limitations,
			}
		})
	})
	mux.HandleFunc("/api/v1/query/views", func(w http.ResponseWriter, r *http.Request) {
		s.handlePhaseYView(w, r, "query.views", func(snapshot observability.Snapshot) any {
			return snapshot.Query
		})
	})
	mux.HandleFunc("/api/v1/mcp/tools", func(w http.ResponseWriter, r *http.Request) {
		s.handlePhaseYView(w, r, "mcp.tools", func(snapshot observability.Snapshot) any {
			return snapshot.MCP
		})
	})
	mux.HandleFunc("/api/v1/gui/summary", func(w http.ResponseWriter, r *http.Request) {
		s.handlePhaseYView(w, r, "gui.summary", func(snapshot observability.Snapshot) any {
			return map[string]any{
				"gui":        snapshot.GUI,
				"cluster":    map[string]any{"cluster_id": snapshot.ClusterID, "sbs_cluster_id": snapshot.SBSClusterID, "ready": snapshot.Ready},
				"capacity":   snapshot.Capacity,
				"reclaim":    snapshot.Reclaim,
				"membership": snapshot.Membership,
				"operations": snapshot.Operations,
			}
		})
	})
	mux.HandleFunc("/api/v1/workflow/hardening", func(w http.ResponseWriter, r *http.Request) {
		s.handlePhaseYView(w, r, "workflow.hardening", func(snapshot observability.Snapshot) any {
			return snapshot.Workflow
		})
	})
}

// handlePhaseADFleetSummary is the normal dashboard polling path. It is kept
// separate from the Phase Y compatibility views because those views still
// assemble operator-requested node, store, and volume detail. Missing or stale
// aggregate data is returned as typed health; it never triggers legacy
// completion as a fallback.
func (s *server) handlePhaseADFleetSummary(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", http.MethodGet)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	writePhaseYJSON(w, http.StatusOK, s.phaseADFleetSummarySnapshot(r.Context()))
}

func (s *server) phaseADFleetSummarySnapshot(ctx context.Context) observability.Snapshot {
	started := time.Now()
	generatedAt := s.currentTime()
	assessment, err := s.repo.AssessClusterSummary(ctx, clustermeta.SummaryKindCluster, generatedAt, s.clusterSummaryPolicy())
	if err != nil {
		assessment = clustermeta.ClusterSummaryAssessment{
			Health: clustermeta.SummaryHealthRebuildRequired, Reason: "aggregate_unavailable",
			Partial: true, RebuildRequired: true, AssessedAtUnix: generatedAt.Unix(),
		}
	}
	base := observabilitySnapshotFromAssessment(assessment)
	fleetObservation, fleetObservationErr := s.repo.GetFleetObservation(ctx)
	base.LocalIsLeader = true
	base.LeaderState = "leader"
	leaderNodeID := s.nodeID
	requestPointGets := assessment.Read.PointGetCount + 1
	if s.leader != nil {
		base.LocalIsLeader = s.leader.IsLeader()
		base.LeaderState = s.leader.State()
		requestPointGets++
		if record, leaderErr := s.leader.CurrentLeader(ctx); leaderErr == nil && record.NodeID != "" {
			leaderNodeID = record.NodeID
			base.LeaseExpiresAtUnix = record.ExpiresAtUnix
		}
	}

	projectionReady := assessment.Health == clustermeta.SummaryHealthReady && !assessment.Partial && !assessment.Stale
	warnings := []string(nil)
	limitations := []string(nil)
	fleetHealth := []observability.FleetHealth(nil)
	if !projectionReady {
		warnings = append(warnings, fmt.Sprintf("cluster summary is %s: %s", assessment.Health, assessment.Reason))
		fleetHealth = append(fleetHealth, observability.FleetHealth{
			Code: "SBS_FLEET_CHECK_STALE", Severity: fleetHealthSeverity(assessment), Count: 1,
			SourceRevision: assessment.Read.MaximumSourceRevision, Freshness: fleetHealthFreshness(assessment),
		})
	}
	if assessment.Partial {
		limitations = append(limitations, "cluster aggregate is partial; counts are withheld until the projection is rebuilt")
	}
	fleetObservationAge := time.Duration(0)
	capacityObservationAge := time.Duration(0)
	fleetObservationFresh := false
	capacityObservationFresh := false
	if fleetObservationErr == nil {
		fleetObservationAge = generatedAt.Sub(time.Unix(fleetObservation.ObservedAtUnix, 0).UTC())
		capacityObservationAge = generatedAt.Sub(time.Unix(fleetObservation.CapacityObservedAtUnix, 0).UTC())
		if fleetObservationAge < 0 {
			fleetObservationAge = 0
		}
		if capacityObservationAge < 0 {
			capacityObservationAge = 0
		}
		fleetObservationFresh = fleetObservationAge < s.clusterSummaryPolicy().DegradedAfter
		capacityObservationFresh = capacityObservationAge < s.clusterSummaryPolicy().DegradedAfter
	}
	if fleetObservationErr != nil || !fleetObservationFresh || !capacityObservationFresh {
		warnings = append(warnings, "fleet/capacity observation is missing or stale")
		fleetHealth = appendOrMergeFleetHealth(fleetHealth, observability.FleetHealth{
			Code: "SBS_FLEET_CHECK_STALE", Severity: "warning", Count: 1,
			SourceRevision: fleetObservation.SourceRevision, Freshness: "stale",
		})
	}
	observationHealth := fleetObservationHealth(fleetObservation)
	if len(observationHealth) > 0 {
		warnings = append(warnings, "fleet observation reports blocked or drifted state")
	}
	for _, health := range observationHealth {
		fleetHealth = appendOrMergeFleetHealth(fleetHealth, health)
	}

	revisionLag := uint64(0)
	if assessment.Read.MaximumSourceRevision >= assessment.Read.BaselineSourceRevision {
		revisionLag = assessment.Read.MaximumSourceRevision - assessment.Read.BaselineSourceRevision
	}
	mismatchCount := uint64(0)
	if assessment.Reason == "aggregate_changed" || assessment.Reason == "aggregate_invalid" || assessment.Reason == "outbox_pending" {
		mismatchCount = 1
	}
	reclaim := observability.Reclaim{
		Source: "sharded cluster aggregate", PendingChunks: base.RetiredPayloadBacklogChunks,
		PendingBytes: base.RetiredPayloadBacklogBytes, FailedBatches: base.RetiredPayloadFailedBatches,
		ProtectedReferenceCheckPassed: base.RetiredPayloadFailedBatches == 0, CompletedClaimed: false,
	}
	if base.RetiredPayloadFailedBatches > 0 {
		reclaim.BlockedReason = "retired payload gc has failed batches; before/after free-byte evidence is required"
	}
	membership := observability.Membership{
		SourceAuthority: "sharded cluster aggregate", SBSMembershipSourceAuthority: "cluster summary projection",
		NAMRBDGatewayMembershipReady: projectionReady, ISCSIGatewayMembershipReady: projectionReady,
		SBSMembershipSyncCompleted: projectionReady, GatewaySBSViewFresh: projectionReady,
		AdminGuideMembershipHandoffReady: true, ActiveNodes: base.ActiveNodes,
		DrainingNodes: base.DrainingNodes, RemovedNodes: base.RemovedNodes,
		HealthyNodes: base.HealthyNodes, SuspectNodes: base.SuspectNodes, DownNodes: base.DownNodes,
	}
	capacityFreshness := "missing"
	if fleetObservationErr == nil {
		capacityFreshness = "stale"
		if capacityObservationFresh {
			capacityFreshness = "fresh"
		}
	}
	tikvPressure := clustermeta.TiKVPressureSnapshotNow()
	phaseADStats := s.phaseADCurrentObservability.snapshot()
	fullCompletionCount := int64(0)
	for _, surface := range []string{"list_volumes", "list_operations", "list_repairs", "list_rebalances"} {
		fullCompletionCount += int64(phaseADStats.LegacyExpensiveBySurface[surface]["completed"])
	}
	snapshot := observability.NewSnapshot(observability.BuildInput{
		GeneratedAt: generatedAt, ClusterID: s.clusterID, SBSClusterID: s.sbsClusterID,
		NodeID: s.nodeID, LeaderNodeID: leaderNodeID, Ready: s.ready.Load(),
		LocalIsLeader: base.LocalIsLeader, LeaderState: base.LeaderState,
		MetadataBackend: s.effectiveMetadataBackendName(), RuntimeMode: s.effectiveMetadataRuntimeMode(),
		SourceAuthority: "sharded cluster summary projection", CollectorFreshnessSeconds: time.Since(started).Seconds(),
		Limitations: limitations, Warnings: warnings,
		Capacity: observability.Capacity{
			Source: "signed fleet observation aggregate", LogicalBytes: assessment.Read.Counters.TotalBytes,
			PhysicalFreeBytes: fleetObservation.FreeBytes, TotalBytes: fleetObservation.UsableBytes,
			StoreCount: int(fleetObservation.StoreCount), NodeCount: base.KnownNodes,
			UsableBytes: fleetObservation.UsableBytes, ReservedBytes: fleetObservation.ReservedBytes,
			MissingNodeCount: fleetObservation.MissingNodeCount, StaleNodeCount: fleetObservation.StaleNodeCount,
			ObservationAgeSeconds: uint64(capacityObservationAge / time.Second), SourceRevision: fleetObservation.SourceRevision,
			Freshness: capacityFreshness,
		},
		Maintenance: observability.Maintenance{
			RepairBacklog: base.RepairBacklog, RepairBacklogBytes: base.RepairBacklogBytes, RepairBacklogChunks: base.RepairBacklogChunks,
			RebalanceBacklog: base.RebalanceBacklog, RebalanceBacklogBytes: base.RebalanceBacklogBytes, RebalanceBacklogChunks: base.RebalanceBacklogChunks,
			DrainBacklog: base.DrainBacklog, DrainBacklogBytes: base.DrainBacklogBytes, DrainBacklogChunks: base.DrainBacklogChunks,
			TransitionFailedBatches: base.TransitionFailedBatches, TransitionRecentBatches: base.TransitionRecentBatches,
			TransitionSmallBatches: base.TransitionSmallBatches, TransitionRequeued: base.TransitionRequeued,
			TransitionRetryPages: base.TransitionRetryPages, TransitionRetryWindows: base.TransitionRetryWindows,
			TransitionRetryWindowBytes: base.TransitionRetryWindowBytes, TransitionRetryWindowChunks: base.TransitionRetryWindowChunks,
			MaintenanceCooldownVolumes:  base.MaintenanceCooldownVolumes,
			RepairOldestAgeSeconds:      fleetObservation.RepairOldestAgeSeconds,
			RebalanceOldestAgeSeconds:   fleetObservation.RebalanceOldestAgeSeconds,
			DrainOldestAgeSeconds:       fleetObservation.DrainOldestAgeSeconds,
			RepairClaimLatencyMillis:    fleetObservation.RepairClaimLatencyMillis,
			RebalanceClaimLatencyMillis: fleetObservation.RebalanceClaimLatencyMillis,
			DrainClaimLatencyMillis:     fleetObservation.DrainClaimLatencyMillis,
		},
		Reclaim: reclaim, Membership: membership,
		Operations: observability.Operations{Total: base.OperationsTotal, Running: base.OperationsRunning, Failed: base.OperationsFailed, Completed: base.OperationsCompleted, Canceled: base.OperationsCanceled},
		Fleet: observability.FleetSummary{
			KnownNodes: base.KnownNodes, ActiveNodes: base.ActiveNodes, DrainingNodes: base.DrainingNodes,
			RemovedNodes: base.RemovedNodes, HealthyNodes: base.HealthyNodes, SuspectNodes: base.SuspectNodes,
			DownNodes: base.DownNodes, VolumeCount: base.Volumes, HealthyVolumes: base.VolumeHealthy,
			DegradedVolumes: base.VolumeDegraded, BlockedVolumes: base.VolumeBlocked,
			Zones: phaseADFleetZones(fleetObservation.Zones),
		},
		Projection: observability.Projection{
			Health: string(assessment.Health), Reason: assessment.Reason, Partial: assessment.Partial,
			Stale: assessment.Stale, RebuildRequired: assessment.RebuildRequired,
			SourceRevision: assessment.Read.MaximumSourceRevision, BaselineSourceRevision: assessment.Read.BaselineSourceRevision,
			RevisionLag: revisionLag, RebuildEpoch: assessment.Read.State.ActiveEpoch,
			UpdatedAtUnix: assessment.FreshnessUpdatedUnix, FreshnessAgeMillis: assessment.FreshnessAgeMillis,
			MismatchCount: mismatchCount,
		},
		RequestClass: observability.RequestClass{
			PointGetCount: requestPointGets, BatchGetCount: assessment.Read.BatchGetCount,
			BatchGetKeyCount: assessment.Read.BatchGetKeyCount, BackendFullScanCount: assessment.Read.BackendFullScanCount,
			FullCompletionCount: assessment.Read.FullCompletionCount, NestedCompletionCount: assessment.Read.NestedCompletionCount,
		},
		Detail: observability.DetailScope{DefaultPoll: true}, FleetHealth: fleetHealth,
		FleetControl: observability.FleetControl{
			ManifestRevision: fleetObservation.ManifestRevision, ManifestDigest: fleetObservation.ManifestDigest,
			BinaryDigest: fleetObservation.BinaryDigest, ConfigDigest: fleetObservation.ConfigDigest,
			StoreDigest: fleetObservation.StoreDigest, ApplyOperationID: fleetObservation.ApplyOperationID,
			ApplyState: fleetObservation.ApplyState, SourceRevision: fleetObservation.SourceRevision,
			ObservationAgeSeconds: uint64(fleetObservationAge / time.Second),
		},
		MetadataPressure: observability.MetadataPressure{
			PointGetCount: tikvPressure.PointGetCount, BatchGetCount: tikvPressure.BatchGetCount,
			BatchGetKeyCount: tikvPressure.BatchGetKeyCount, BatchGetChunkCount: tikvPressure.BatchGetChunkCount,
			RangePageCount: tikvPressure.RangePageCount, BackendFullScanCount: tikvPressure.FullScanCount,
			FullCompletionCount: fullCompletionCount, NestedCompletionCount: 0, TxnRetryCount: tikvPressure.TxnRetryCount,
			PointGetDurationSeconds:  float64(tikvPressure.PointGetNanos) / float64(time.Second),
			BatchGetDurationSeconds:  float64(tikvPressure.BatchGetNanos) / float64(time.Second),
			RangePageDurationSeconds: float64(tikvPressure.RangePageNanos) / float64(time.Second),
			HotRegionCandidateCount:  tikvPressure.HotCandidateCount,
		},
	})
	// NewSnapshot's compatibility default assumes a successful membership view.
	// The fleet projection owns the stronger freshness contract.
	snapshot.Membership.SBSMembershipSyncCompleted = projectionReady
	snapshot.Membership.GatewaySBSViewFresh = projectionReady
	return snapshot
}

func phaseADFleetZones(zones []clustermeta.FleetZoneObservation) []observability.FleetZoneSummary {
	out := make([]observability.FleetZoneSummary, 0, len(zones))
	for _, zone := range zones {
		out = append(out, observability.FleetZoneSummary{
			Zone: zone.Zone, ActiveNodes: zone.ActiveNodes, DrainingNodes: zone.DrainingNodes,
			SuspectNodes: zone.SuspectNodes, DownNodes: zone.DownNodes,
		})
	}
	return out
}

func fleetObservationHealth(observation clustermeta.FleetObservation) []observability.FleetHealth {
	items := []struct {
		code     string
		count    uint64
		severity string
	}{
		{code: "SBS_APPLY_PAUSED", count: map[bool]uint64{true: 1}[observation.ApplyPaused], severity: "warning"},
		{code: "SBS_HOST_CHECK_FAILED", count: observation.HostCheckFailedCount, severity: "error"},
		{code: "SBS_CONFIG_DRIFT", count: observation.ConfigDriftCount, severity: "warning"},
		{code: "SBS_STRAY_NODE", count: observation.StrayNodeCount, severity: "warning"},
		{code: "SBS_STORAGE_CLAIM_MISMATCH", count: observation.StorageClaimMismatchCount, severity: "error"},
	}
	out := make([]observability.FleetHealth, 0, len(items))
	for _, item := range items {
		if item.count == 0 {
			continue
		}
		out = append(out, observability.FleetHealth{
			Code: item.code, Severity: item.severity, Count: item.count,
			SourceRevision: observation.SourceRevision, Freshness: "fresh",
		})
	}
	return out
}

func appendOrMergeFleetHealth(items []observability.FleetHealth, candidate observability.FleetHealth) []observability.FleetHealth {
	for index := range items {
		if items[index].Code != candidate.Code {
			continue
		}
		if candidate.Count > items[index].Count {
			items[index].Count = candidate.Count
		}
		if candidate.Severity == "error" {
			items[index].Severity = candidate.Severity
		}
		if candidate.SourceRevision > items[index].SourceRevision {
			items[index].SourceRevision = candidate.SourceRevision
		}
		if candidate.Freshness == "stale" || candidate.Freshness == "partial" {
			items[index].Freshness = candidate.Freshness
		}
		return items
	}
	return append(items, candidate)
}

func fleetHealthSeverity(assessment clustermeta.ClusterSummaryAssessment) string {
	if assessment.Health == clustermeta.SummaryHealthRebuildRequired {
		return "error"
	}
	return "warning"
}

func fleetHealthFreshness(assessment clustermeta.ClusterSummaryAssessment) string {
	if assessment.Partial {
		return "partial"
	}
	if assessment.Stale {
		return "stale"
	}
	return "degraded"
}

func (s *server) handlePhaseADNodePage(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", http.MethodGet)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	pageSize, err := phaseADPageSize(r, clustermeta.MembershipProjectionPageDefault, clustermeta.MembershipProjectionPageMaximum)
	if err != nil {
		writePhaseADViewError(w, "sbs.nodes", http.StatusBadRequest, err)
		return
	}
	includeTombstones := r.URL.Query().Get("include_tombstones") == "true"
	cursor := ""
	pinnedRevision := uint64(0)
	pageToken := strings.TrimSpace(r.URL.Query().Get("page_token"))
	if pageToken != "" {
		token, tokenErr := decodeMembershipPageToken(pageToken)
		if tokenErr != nil {
			writePhaseADViewError(w, "sbs.nodes", http.StatusBadRequest, fmt.Errorf("invalid page_token: %w", tokenErr))
			return
		}
		if token.IncludeTombstones != includeTombstones {
			writePhaseADViewError(w, "sbs.nodes", http.StatusBadRequest, fmt.Errorf("page_token include_tombstones filter mismatch"))
			return
		}
		cursor = token.Cursor
		pinnedRevision = token.ProjectionRevision
	}
	page, err := s.repo.ListMembershipProjectionPageReadOnly(r.Context(), cursor, pageSize, includeTombstones)
	if err != nil {
		statusCode := http.StatusServiceUnavailable
		if errors.Is(err, clustermeta.ErrMembershipProjectionChanged) {
			statusCode = http.StatusConflict
		}
		writePhaseADViewError(w, "sbs.nodes", statusCode, fmt.Errorf("read membership projection page: %w", err))
		return
	}
	if pinnedRevision != 0 && pinnedRevision != page.Status.MembershipProjectionRevision {
		writePhaseADViewError(w, "sbs.nodes", http.StatusConflict, fmt.Errorf("page_token revision mismatch: token=%d current=%d", pinnedRevision, page.Status.MembershipProjectionRevision))
		return
	}
	nextPageToken := ""
	if page.NextCursor != "" {
		nextPageToken, err = encodeMembershipPageToken(page.NextCursor, page.Status.MembershipProjectionRevision, includeTombstones)
		if err != nil {
			writePhaseADViewError(w, "sbs.nodes", http.StatusInternalServerError, err)
			return
		}
	}
	nodes := make([]observability.Node, 0, len(page.Records))
	for _, record := range page.Records {
		nodes = append(nodes, phaseADNodeFromMembership(record))
	}
	collectionStatus := observability.StatusOK
	warnings := []string(nil)
	if page.Status.Stale {
		collectionStatus = observability.StatusDegraded
		warnings = append(warnings, "membership projection is stale")
	}
	writePhaseYJSON(w, http.StatusOK, phaseADReadOnlyView("sbs.nodes", "membership projection page", collectionStatus, warnings, map[string]any{
		"nodes": nodes, "next_page_token": nextPageToken, "automatic_page_completion": false,
		"page_size": pageSize, "record_count": len(nodes), "membership_revision": page.Status.MembershipRevision,
		"projection_revision": page.Status.MembershipProjectionRevision, "projection_lag_ms": page.Status.ProjectionLagMS,
		"projection_health": page.Status.ProjectionHealth, "projection_stale": page.Status.Stale,
		"detail":        observability.DetailScope{NodeDetailIncluded: true},
		"request_class": observability.RequestClass{RangePageCount: 1, BatchGetCount: 1, BatchGetKeyCount: len(page.Records)},
	}))
}

func (s *server) handlePhaseADNodePoint(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", http.MethodGet)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	nodeID := strings.TrimSpace(r.URL.Query().Get("id"))
	if nodeID == "" {
		writePhaseADViewError(w, "sbs.node", http.StatusBadRequest, fmt.Errorf("id is required"))
		return
	}
	record, err := s.repo.GetNodeMembership(r.Context(), nodeID)
	if err != nil {
		statusCode := http.StatusInternalServerError
		if errors.Is(err, clustermeta.ErrNotFound) {
			statusCode = http.StatusNotFound
		}
		writePhaseADViewError(w, "sbs.node", statusCode, err)
		return
	}
	node := phaseADNodeFromMembership(record)
	stores := []observability.Store(nil)
	pointGets := 2
	if detail, detailErr := s.repo.GetNodeHealthDetail(r.Context(), nodeID); detailErr == nil {
		node.ConsecutiveProbeFailures = detail.ConsecutiveProbeFailures
		node.StoreCount = detail.StoreCount
		node.HealthyStoreCount = detail.HealthyStoreCount
		node.WritableStoreCount = detail.WritableStoreCount
		node.AllocatableStoreCount = detail.AllocatableStoreCount
		node.StoreAllocationWeightTotal = detail.StoreAllocationWeightTotal
		node.StoreAllocationWeightObserved = detail.StoreAllocationWeightObserved
		stores = append(stores, observability.Store{
			NodeID: nodeID, StoreCount: detail.StoreCount, HealthyStoreCount: detail.HealthyStoreCount,
			WritableStoreCount: detail.WritableStoreCount, AllocatableStoreCount: detail.AllocatableStoreCount,
			CapacityBytes: detail.StoreCapacityBytes, AvailableBytes: detail.StoreAvailableBytes,
			UsedBytes: detail.StoreUsedBytes, CompactionPendingBytes: detail.StoreCompactionPendingBytes,
			CompactionInProgressBytes: detail.StoreCompactionInProgressBytes,
			AllocationWeightTotal:     detail.StoreAllocationWeightTotal, AllocationWeightObserved: detail.StoreAllocationWeightObserved,
			PlacementEligible: detail.StorePlacementEligible(),
		})
	} else if errors.Is(detailErr, clustermeta.ErrNotFound) {
		pointGets = 2
	} else {
		writePhaseADViewError(w, "sbs.node", http.StatusInternalServerError, detailErr)
		return
	}
	writePhaseYJSON(w, http.StatusOK, phaseADReadOnlyView("sbs.node", "node membership and health point records", observability.StatusOK, nil, map[string]any{
		"node": node, "stores": stores, "detail": observability.DetailScope{NodeDetailIncluded: true, StoreDetailIncluded: true},
		"request_class": observability.RequestClass{PointGetCount: pointGets},
	}))
}

func (s *server) handlePhaseADVolumePage(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", http.MethodGet)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	pageSize, err := phaseADPageSize(r, clustermeta.VolumeCatalogPageDefault, clustermeta.VolumeCatalogPageMaximum)
	if err != nil {
		writePhaseADViewError(w, "sbs.volumes", http.StatusBadRequest, err)
		return
	}
	request := &adminv1.ListVolumesPageRequest{
		PageSize: uint32(pageSize), PageToken: strings.TrimSpace(r.URL.Query().Get("page_token")),
		RedundancyBackend: strings.TrimSpace(r.URL.Query().Get("redundancy_backend")), TopologyMode: strings.TrimSpace(r.URL.Query().Get("topology_mode")),
	}
	request.Health, err = phaseADVolumeHealth(r.URL.Query().Get("health"))
	if err != nil {
		writePhaseADViewError(w, "sbs.volumes", http.StatusBadRequest, err)
		return
	}
	cursor := ""
	expectedRevision := uint64(0)
	if request.PageToken != "" {
		token, tokenErr := decodeVolumePageToken(request.PageToken, request)
		if tokenErr != nil {
			writePhaseADViewError(w, "sbs.volumes", http.StatusBadRequest, fmt.Errorf("invalid page_token: %w", tokenErr))
			return
		}
		cursor = token.Cursor
		expectedRevision = token.CatalogRevision
	}
	filterStatus, err := volumeStatusFromProto(request.Health)
	if err != nil {
		writePhaseADViewError(w, "sbs.volumes", http.StatusBadRequest, err)
		return
	}
	page, err := s.repo.ListVolumeCatalogPage(r.Context(), cursor, pageSize, expectedRevision, clustermeta.VolumeCatalogFilter{
		Status: filterStatus, RedundancyBackend: request.RedundancyBackend, TopologyMode: request.TopologyMode,
	})
	if err != nil {
		statusCode := http.StatusInternalServerError
		if errors.Is(err, clustermeta.ErrVolumeCatalogRebuildRequired) || errors.Is(err, clustermeta.ErrVolumeCatalogRevision) {
			statusCode = http.StatusConflict
		} else if errors.Is(err, clustermeta.ErrVolumeCatalogInvalid) {
			statusCode = http.StatusBadRequest
		}
		writePhaseADViewError(w, "sbs.volumes", statusCode, err)
		return
	}
	nextPageToken := ""
	if page.NextCursor != "" {
		nextPageToken, err = encodeVolumePageToken(page.NextCursor, page.State.Revision, request)
		if err != nil {
			writePhaseADViewError(w, "sbs.volumes", http.StatusInternalServerError, err)
			return
		}
	}
	volumes := make([]observability.Volume, 0, len(page.Entries))
	for _, entry := range page.Entries {
		volumes = append(volumes, phaseADVolumeFromCatalog(entry))
	}
	age := s.currentTime().UTC().Sub(time.Unix(page.State.UpdatedAtUnix, 0).UTC())
	if age < 0 {
		age = 0
	}
	projectionHealth := "ready"
	collectionStatus := observability.StatusOK
	warnings := []string(nil)
	if age >= s.clusterSummaryPolicy().DegradedAfter {
		projectionHealth = "stale"
		collectionStatus = observability.StatusDegraded
		warnings = append(warnings, "volume catalog projection is stale")
	}
	writePhaseYJSON(w, http.StatusOK, phaseADReadOnlyView("sbs.volumes", "volume catalog projection page", collectionStatus, warnings, map[string]any{
		"volumes": volumes, "next_page_token": nextPageToken, "automatic_page_completion": false,
		"page_size": pageSize, "record_count": len(volumes), "scanned_records": page.ScannedCount,
		"catalog_revision": page.State.Revision, "projection_health": projectionHealth, "freshness_age_millis": age.Milliseconds(),
		"detail":        observability.DetailScope{VolumeDetailIncluded: true},
		"request_class": observability.RequestClass{PointGetCount: 1, RangePageCount: 1, BatchGetCount: 1, BatchGetKeyCount: page.ScannedCount * 2},
	}))
}

func (s *server) handlePhaseADVolumePoint(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", http.MethodGet)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	volumeID := strings.TrimSpace(r.URL.Query().Get("id"))
	if volumeID == "" {
		writePhaseADViewError(w, "sbs.volume", http.StatusBadRequest, fmt.Errorf("id is required"))
		return
	}
	state, err := s.repo.GetVolumeState(r.Context(), volumeID)
	if err != nil {
		statusCode := http.StatusInternalServerError
		if errors.Is(err, clustermeta.ErrNotFound) {
			statusCode = http.StatusNotFound
		}
		writePhaseADViewError(w, "sbs.volume", statusCode, err)
		return
	}
	spec, err := s.repo.GetVolumeSpec(r.Context(), volumeID)
	if err != nil {
		writePhaseADViewError(w, "sbs.volume", http.StatusInternalServerError, err)
		return
	}
	writePhaseYJSON(w, http.StatusOK, phaseADReadOnlyView("sbs.volume", "volume state and spec point records", observability.StatusOK, nil, map[string]any{
		"volume":        phaseADVolumeFromCatalog(clustermeta.VolumeCatalogEntry{State: state, Spec: spec}),
		"detail":        observability.DetailScope{VolumeDetailIncluded: true},
		"request_class": observability.RequestClass{PointGetCount: 2},
	}))
}

func phaseADPageSize(r *http.Request, defaultValue, maximum int) (int, error) {
	raw := strings.TrimSpace(r.URL.Query().Get("page_size"))
	if raw == "" {
		return defaultValue, nil
	}
	value, err := strconv.Atoi(raw)
	if err != nil || value < 1 || value > maximum {
		return 0, fmt.Errorf("page_size must be between 1 and %d", maximum)
	}
	return value, nil
}

func phaseADVolumeHealth(raw string) (adminv1.VolumeHealth, error) {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "":
		return adminv1.VolumeHealth_VOLUME_HEALTH_UNSPECIFIED, nil
	case "healthy":
		return adminv1.VolumeHealth_VOLUME_HEALTH_HEALTHY, nil
	case "degraded":
		return adminv1.VolumeHealth_VOLUME_HEALTH_DEGRADED, nil
	case "repairing":
		return adminv1.VolumeHealth_VOLUME_HEALTH_REPAIRING, nil
	case "rebalancing":
		return adminv1.VolumeHealth_VOLUME_HEALTH_REBALANCING, nil
	case "blocked":
		return adminv1.VolumeHealth_VOLUME_HEALTH_BLOCKED, nil
	default:
		return adminv1.VolumeHealth_VOLUME_HEALTH_UNSPECIFIED, fmt.Errorf("unsupported health filter %q", raw)
	}
}

func phaseADNodeFromMembership(record clustermeta.NodeMembershipRecord) observability.Node {
	return observability.Node{
		NodeID: record.NodeID, Lifecycle: string(record.LifecycleState), Health: string(record.HealthState),
		Zone: record.Zone, Host: record.Host, Version: record.Version,
		Capabilities: append([]string(nil), record.Capabilities...), LastHeartbeatUnix: record.LastHeartbeatUnix,
		CapacityBytes: record.CapacityBytes, UsedBytes: record.UsedBytes,
		AdminHTTPEndpointConfigured: record.AdminHTTPEndpoint != "", SBSEndpointCount: len(record.SBSEndpoints),
	}
}

func phaseADVolumeFromCatalog(entry clustermeta.VolumeCatalogEntry) observability.Volume {
	return observability.Volume{
		VolumeID: entry.State.VolumeID, Status: string(entry.State.Status), Revision: entry.State.Revision,
		Epoch: entry.State.Epoch, SizeBytes: entry.Spec.SizeBytes, ChunkSizeBytes: entry.Spec.ChunkSizeBytes,
		ExtentSizeBytes: entry.Spec.ExtentSizeBytes, BlockSizeBytes: entry.Spec.BlockSize,
		ReplicationFactor: entry.Spec.ReplicationFactor, ECProfileID: entry.Spec.ECProfileID,
		RedundancyBackend: effectiveVolumeRedundancyBackend(entry.State, volumeSpecRecordFromMetadata(entry.Spec)),
		TopologyMode:      effectiveVolumeTopologyMode(entry.State.TopologyMode, entry.Spec.TopologyMode),
	}
}

func phaseADReadOnlyView(viewID, sourceAuthority, collectionStatus string, warnings []string, data any) observability.View {
	return observability.View{
		SchemaVersion: observability.SchemaVersion, ViewID: viewID, GeneratedAt: time.Now().UTC().Format(time.RFC3339),
		SourceAuthority: sourceAuthority, CollectionStatus: collectionStatus,
		Warnings: append([]string(nil), warnings...), WarningCount: len(warnings),
		RBACChecked: true, TenantScopeChecked: true, RedactionApplied: true,
		ReadOnlyModeEnforced: true, UnsupportedClaimVisible: true, Data: data,
	}
}

func writePhaseADViewError(w http.ResponseWriter, viewID string, statusCode int, err error) {
	writePhaseYJSON(w, statusCode, phaseADReadOnlyView(viewID, "bounded metadata projection", observability.StatusError, nil, map[string]any{
		"error": err.Error(), "automatic_page_completion": false,
	}))
}

func (s *server) handlePhaseYView(w http.ResponseWriter, r *http.Request, viewID string, selectData func(observability.Snapshot) any) {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", http.MethodGet)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	snapshot := s.phaseADFleetSummarySnapshot(r.Context())
	payload := snapshot.View(viewID, selectData(snapshot))
	if viewID == "sbs.cluster" {
		writePhaseYJSON(w, http.StatusOK, selectData(snapshot))
		return
	}
	writePhaseYJSON(w, http.StatusOK, payload)
}

func (s *server) phaseYOperationsSnapshot(ctx context.Context) observability.Snapshot {
	started := time.Now()
	generatedAt := s.currentTime()
	base, leaderNodeID := s.boundedObservabilitySnapshot()
	nodes, stores, capacity, nodeWarnings, nodeErr := s.phaseYNodeStoreCapacity(ctx, generatedAt)
	volumes, volumeLogicalBytes, volumeWarnings, volumeErr := s.phaseYVolumes(ctx)
	warnings := append([]string{}, nodeWarnings...)
	warnings = append(warnings, volumeWarnings...)
	firstErr, lastErr := phaseYFirstLastError(nodeErr, volumeErr)
	if capacity.LogicalBytes == 0 {
		capacity.LogicalBytes = volumeLogicalBytes
	}
	if capacity.NodeCount == 0 {
		capacity.NodeCount = len(nodes)
	}
	if capacity.StoreCount == 0 {
		capacity.StoreCount = len(stores)
	}
	capacity.ReclaimableBytes = base.RetiredPayloadBacklogBytes
	reclaim := observability.Reclaim{
		PendingChunks:                 base.RetiredPayloadBacklogChunks,
		PendingBytes:                  base.RetiredPayloadBacklogBytes,
		FailedBatches:                 base.RetiredPayloadFailedBatches,
		OldestFailedBatchAgeSeconds:   base.RetiredPayloadFailedAgeSec,
		ProtectedReferenceCheckPassed: base.RetiredPayloadFailedBatches == 0,
		CompletedClaimed:              false,
	}
	if base.RetiredPayloadFailedBatches > 0 {
		reclaim.BlockedReason = "retired payload gc has failed batches; before/after free-byte evidence is required"
	}
	membership := phaseYMembershipFromNodes(nodes, nodeErr)
	return observability.NewSnapshot(observability.BuildInput{
		GeneratedAt:               generatedAt,
		ClusterID:                 s.clusterID,
		SBSClusterID:              s.sbsClusterID,
		NodeID:                    s.nodeID,
		LeaderNodeID:              leaderNodeID,
		Ready:                     s.ready.Load(),
		LocalIsLeader:             base.LocalIsLeader,
		LeaderState:               base.LeaderState,
		MetadataBackend:           s.effectiveMetadataBackendName(),
		RuntimeMode:               s.effectiveMetadataRuntimeMode(),
		SourceAuthority:           "sbs-service AdminService, cluster metadata, and sbs-data health detail",
		CollectorFreshnessSeconds: time.Since(started).Seconds(),
		Warnings:                  warnings,
		FirstError:                firstErr,
		LastError:                 lastErr,
		Nodes:                     nodes,
		Stores:                    stores,
		Volumes:                   volumes,
		Capacity:                  capacity,
		Maintenance: observability.Maintenance{
			RepairBacklog:                          base.RepairBacklog,
			RepairBacklogBytes:                     base.RepairBacklogBytes,
			RepairBacklogChunks:                    base.RepairBacklogChunks,
			RebalanceBacklog:                       base.RebalanceBacklog,
			RebalanceBacklogBytes:                  base.RebalanceBacklogBytes,
			RebalanceBacklogChunks:                 base.RebalanceBacklogChunks,
			DrainBacklog:                           base.DrainBacklog,
			DrainBacklogBytes:                      base.DrainBacklogBytes,
			DrainBacklogChunks:                     base.DrainBacklogChunks,
			TransitionFailedBatches:                base.TransitionFailedBatches,
			TransitionRecentBatches:                base.TransitionRecentBatches,
			TransitionSmallBatches:                 base.TransitionSmallBatches,
			TransitionRequeued:                     base.TransitionRequeued,
			TransitionRetryPages:                   base.TransitionRetryPages,
			TransitionRetryWindows:                 base.TransitionRetryWindows,
			TransitionRetryWindowBytes:             base.TransitionRetryWindowBytes,
			TransitionRetryWindowChunks:            base.TransitionRetryWindowChunks,
			TransitionOldestFailedAgeSeconds:       base.TransitionFailedAgeSec,
			MaintenanceCooldownVolumes:             base.MaintenanceCooldownVolumes,
			MaintenanceCooldownMaxRemainingSeconds: base.MaintenanceCooldownMaxSec,
			NodesWithProbeFailures:                 base.NodesWithProbeFailures,
			MaxConsecutiveProbeFailures:            base.MaxProbeFailures,
			NodesInRecoveryCooldown:                base.NodesInRecoveryCooldown,
			MaxRecoveryCooldownRemainingSeconds:    base.MaxRecoveryCooldownSec,
		},
		Reclaim:    reclaim,
		Membership: membership,
		Operations: observability.Operations{
			Total:     base.OperationsTotal,
			Running:   base.OperationsRunning,
			Failed:    base.OperationsFailed,
			Completed: base.OperationsCompleted,
			Canceled:  base.OperationsCanceled,
		},
	})
}

func phaseYMembershipFromNodes(nodes []observability.Node, nodeErr error) observability.Membership {
	membership := observability.Membership{
		NAMRBDGatewayMembershipReady:     true,
		ISCSIGatewayMembershipReady:      true,
		SBSMembershipSyncCompleted:       true,
		GatewaySBSViewFresh:              nodeErr == nil,
		AdminGuideMembershipHandoffReady: true,
	}
	for _, node := range nodes {
		switch node.Lifecycle {
		case "active":
			membership.ActiveNodes++
		case "draining":
			membership.DrainingNodes++
		case "removed":
			membership.RemovedNodes++
		}
		switch node.Health {
		case "healthy":
			membership.HealthyNodes++
		case "suspect":
			membership.SuspectNodes++
		case "down":
			membership.DownNodes++
		}
	}
	if nodeErr != nil {
		membership.SBSMembershipSyncCompleted = false
		membership.GatewaySBSViewFresh = false
	}
	return membership
}

func (s *server) phaseYNodeStoreCapacity(ctx context.Context, now time.Time) ([]observability.Node, []observability.Store, observability.Capacity, []string, error) {
	records, err := s.repo.ListNodeMemberships(ctx)
	if err != nil {
		return nil, nil, observability.Capacity{}, nil, fmt.Errorf("list node memberships: %w", err)
	}
	sort.Slice(records, func(i, j int) bool { return records[i].NodeID < records[j].NodeID })
	nodes := make([]observability.Node, 0, len(records))
	stores := make([]observability.Store, 0, len(records))
	var capacity observability.Capacity
	var warnings []string
	var nodeCapacityTotal uint64
	var nodeUsedTotal uint64
	var storeCapacityTotal uint64
	var storeUsedTotal uint64
	var storeAvailableTotal uint64
	for _, rec := range records {
		node := observability.Node{
			NodeID:                      rec.NodeID,
			Lifecycle:                   string(rec.LifecycleState),
			Health:                      string(rec.HealthState),
			Zone:                        rec.Zone,
			Host:                        rec.Host,
			Version:                     rec.Version,
			Capabilities:                append([]string(nil), rec.Capabilities...),
			LastHeartbeatUnix:           rec.LastHeartbeatUnix,
			CapacityBytes:               rec.CapacityBytes,
			UsedBytes:                   rec.UsedBytes,
			AdminHTTPEndpointConfigured: rec.AdminHTTPEndpoint != "",
			SBSEndpointCount:            len(rec.SBSEndpoints),
		}
		if rec.CapacityBytes > 0 {
			nodeCapacityTotal += rec.CapacityBytes
			if rec.UsedBytes <= rec.CapacityBytes {
				nodeUsedTotal += rec.UsedBytes
			} else {
				capacity.UnknownBytes += rec.UsedBytes - rec.CapacityBytes
			}
		}
		if detail, detailErr := s.repo.GetNodeHealthDetail(ctx, rec.NodeID); detailErr == nil {
			node.ConsecutiveProbeFailures = detail.ConsecutiveProbeFailures
			if detail.RecoveryEligibleAtUnix > now.Unix() {
				node.RecoveryCooldownSeconds = uint64(detail.RecoveryEligibleAtUnix - now.Unix())
			}
			node.StoreCount = detail.StoreCount
			node.HealthyStoreCount = detail.HealthyStoreCount
			node.WritableStoreCount = detail.WritableStoreCount
			node.AllocatableStoreCount = detail.AllocatableStoreCount
			node.StoreAllocationWeightTotal = detail.StoreAllocationWeightTotal
			node.StoreAllocationWeightObserved = detail.StoreAllocationWeightObserved
			if detail.StoreCount > 0 || detail.StoreCapacityBytes > 0 || detail.StoreAvailableBytes > 0 || detail.StoreUsedBytes > 0 {
				stores = append(stores, observability.Store{
					NodeID:                    rec.NodeID,
					StoreCount:                detail.StoreCount,
					HealthyStoreCount:         detail.HealthyStoreCount,
					WritableStoreCount:        detail.WritableStoreCount,
					AllocatableStoreCount:     detail.AllocatableStoreCount,
					CapacityBytes:             detail.StoreCapacityBytes,
					AvailableBytes:            detail.StoreAvailableBytes,
					UsedBytes:                 detail.StoreUsedBytes,
					CompactionPendingBytes:    detail.StoreCompactionPendingBytes,
					CompactionInProgressBytes: detail.StoreCompactionInProgressBytes,
					AllocationWeightTotal:     detail.StoreAllocationWeightTotal,
					AllocationWeightObserved:  detail.StoreAllocationWeightObserved,
					PlacementEligible:         detail.StorePlacementEligible(),
				})
				storeCapacityTotal += detail.StoreCapacityBytes
				storeUsedTotal += detail.StoreUsedBytes
				storeAvailableTotal += detail.StoreAvailableBytes
			}
		} else {
			warnings = append(warnings, fmt.Sprintf("node %s has no sbs-data health detail", rec.NodeID))
		}
		nodes = append(nodes, node)
	}
	capacity.Source = "sbs-service node membership and sbs-data health detail"
	capacity.NodeCount = len(nodes)
	capacity.StoreCount = len(stores)
	if storeCapacityTotal > 0 || storeAvailableTotal > 0 || storeUsedTotal > 0 {
		capacity.TotalBytes = storeCapacityTotal
		capacity.PhysicalUsedBytes = storeUsedTotal
		capacity.PhysicalFreeBytes = storeAvailableTotal
		if storeCapacityTotal > 0 && storeUsedTotal+storeAvailableTotal < storeCapacityTotal {
			capacity.UnknownBytes += storeCapacityTotal - storeUsedTotal - storeAvailableTotal
		}
		return nodes, stores, capacity, warnings, nil
	}
	capacity.TotalBytes = nodeCapacityTotal
	capacity.PhysicalUsedBytes = nodeUsedTotal
	if nodeCapacityTotal >= nodeUsedTotal {
		capacity.PhysicalFreeBytes = nodeCapacityTotal - nodeUsedTotal
	}
	return nodes, stores, capacity, warnings, nil
}

func (s *server) phaseYVolumes(ctx context.Context) ([]observability.Volume, uint64, []string, error) {
	records, err := s.repo.ListVolumeStates(ctx)
	if err != nil {
		return nil, 0, nil, fmt.Errorf("list volumes: %w", err)
	}
	sort.Slice(records, func(i, j int) bool { return records[i].VolumeID < records[j].VolumeID })
	volumes := make([]observability.Volume, 0, len(records))
	var logicalBytes uint64
	var warnings []string
	for _, rec := range records {
		vol := observability.Volume{
			VolumeID:          rec.VolumeID,
			Status:            string(rec.Status),
			RedundancyBackend: rec.RedundancyBackend,
			TopologyMode:      rec.TopologyMode,
			ProtectionPolicy:  rec.ProtectionPolicy,
			Revision:          rec.Revision,
			Epoch:             rec.Epoch,
		}
		if spec, specErr := s.getVolumeSpec(ctx, rec.VolumeID); specErr == nil {
			vol.SizeBytes = spec.SizeBytes
			vol.ChunkSizeBytes = spec.ChunkSizeBytes
			vol.ExtentSizeBytes = spec.ExtentSizeBytes
			vol.BlockSizeBytes = spec.BlockSize
			vol.ReplicationFactor = spec.ReplicationFactor
			vol.ECProfileID = spec.ECProfileID
			if vol.RedundancyBackend == "" {
				vol.RedundancyBackend = spec.RedundancyBackend
			}
			if vol.TopologyMode == "" {
				vol.TopologyMode = spec.TopologyMode
			}
			logicalBytes += spec.SizeBytes
		} else {
			warnings = append(warnings, fmt.Sprintf("volume %s has no volume spec record", rec.VolumeID))
		}
		volumes = append(volumes, vol)
	}
	return volumes, logicalBytes, warnings, nil
}

func phaseYFirstLastError(errs ...error) (string, string) {
	first := ""
	last := ""
	for _, err := range errs {
		if err == nil {
			continue
		}
		if first == "" {
			first = err.Error()
		}
		last = err.Error()
	}
	return first, last
}

func writePhaseYJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}
