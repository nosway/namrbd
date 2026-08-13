package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"time"

	"github.com/nosway/namrbd/sbs/observability"
)

func (s *server) registerPhaseYOperationsAPI(mux *http.ServeMux) {
	mux.HandleFunc("/api/v1/sbs/cluster", func(w http.ResponseWriter, r *http.Request) {
		s.handlePhaseYView(w, r, "sbs.cluster", func(snapshot observability.Snapshot) any {
			return snapshot
		})
	})
	mux.HandleFunc("/api/v1/sbs/nodes", func(w http.ResponseWriter, r *http.Request) {
		s.handlePhaseYView(w, r, "sbs.nodes", func(snapshot observability.Snapshot) any {
			return map[string]any{"nodes": snapshot.Nodes, "stores": snapshot.Stores}
		})
	})
	mux.HandleFunc("/api/v1/sbs/volumes", func(w http.ResponseWriter, r *http.Request) {
		s.handlePhaseYView(w, r, "sbs.volumes", func(snapshot observability.Snapshot) any {
			return map[string]any{"volumes": snapshot.Volumes}
		})
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

func (s *server) handlePhaseYView(w http.ResponseWriter, r *http.Request, viewID string, selectData func(observability.Snapshot) any) {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", http.MethodGet)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	snapshot := s.phaseYOperationsSnapshot(r.Context())
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
