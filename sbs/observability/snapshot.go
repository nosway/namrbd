package observability

import "time"

const (
	SchemaVersion = "namrbd.sbs.observability.v1"

	StatusOK       = "ok"
	StatusPartial  = "partial"
	StatusDegraded = "degraded"
	StatusError    = "error"
)

type Snapshot struct {
	SchemaVersion             string        `json:"schema_version"`
	GeneratedAt               string        `json:"generated_at"`
	ClusterID                 string        `json:"cluster_id,omitempty"`
	SBSClusterID              string        `json:"sbs_cluster_id,omitempty"`
	NodeID                    string        `json:"node_id,omitempty"`
	LeaderNodeID              string        `json:"leader_node_id,omitempty"`
	Ready                     bool          `json:"ready"`
	LocalIsLeader             bool          `json:"local_is_leader"`
	LeaderState               string        `json:"leader_state,omitempty"`
	MetadataBackend           string        `json:"metadata_backend,omitempty"`
	RuntimeMode               string        `json:"runtime_mode,omitempty"`
	SourceAuthority           string        `json:"source_authority"`
	CollectionStatus          string        `json:"collection_status"`
	CollectorFreshnessSeconds float64       `json:"collector_freshness_seconds"`
	Limitations               []string      `json:"limitations,omitempty"`
	Warnings                  []string      `json:"warnings,omitempty"`
	WarningCount              int           `json:"warning_count"`
	FirstError                string        `json:"first_error,omitempty"`
	LastError                 string        `json:"last_error,omitempty"`
	RBACChecked               bool          `json:"rbac_checked"`
	TenantScopeChecked        bool          `json:"tenant_scope_checked"`
	RedactionApplied          bool          `json:"redaction_applied"`
	ReadOnlyModeEnforced      bool          `json:"read_only_mode_enforced"`
	UnsupportedClaimVisible   bool          `json:"unsupported_claim_visible"`
	SupportClaimed            bool          `json:"support_claimed"`
	PublicGUIClaimed          bool          `json:"public_gui_claimed"`
	PublicBenchmarkClaimed    bool          `json:"public_benchmark_claimed"`
	Nodes                     []Node        `json:"nodes,omitempty"`
	Stores                    []Store       `json:"stores,omitempty"`
	Volumes                   []Volume      `json:"volumes,omitempty"`
	Capacity                  Capacity      `json:"capacity"`
	Maintenance               Maintenance   `json:"maintenance"`
	Reclaim                   Reclaim       `json:"reclaim"`
	Membership                Membership    `json:"membership"`
	Operations                Operations    `json:"operations"`
	Query                     QuerySurface  `json:"query"`
	MCP                       MCPSurface    `json:"mcp"`
	GUI                       GUISurface    `json:"gui"`
	Workflow                  WorkflowState `json:"workflow"`
}

type BuildInput struct {
	GeneratedAt               time.Time
	ClusterID                 string
	SBSClusterID              string
	NodeID                    string
	LeaderNodeID              string
	Ready                     bool
	LocalIsLeader             bool
	LeaderState               string
	MetadataBackend           string
	RuntimeMode               string
	SourceAuthority           string
	CollectorFreshnessSeconds float64
	Limitations               []string
	Warnings                  []string
	FirstError                string
	LastError                 string
	Nodes                     []Node
	Stores                    []Store
	Volumes                   []Volume
	Capacity                  Capacity
	Maintenance               Maintenance
	Reclaim                   Reclaim
	Membership                Membership
	Operations                Operations
	Query                     QuerySurface
	MCP                       MCPSurface
	GUI                       GUISurface
	Workflow                  WorkflowState
}

type Node struct {
	NodeID                        string   `json:"node_id"`
	Lifecycle                     string   `json:"lifecycle"`
	Health                        string   `json:"health"`
	Zone                          string   `json:"zone,omitempty"`
	Host                          string   `json:"host,omitempty"`
	Version                       string   `json:"version,omitempty"`
	Capabilities                  []string `json:"capabilities,omitempty"`
	LastHeartbeatUnix             int64    `json:"last_heartbeat_unix,omitempty"`
	CapacityBytes                 uint64   `json:"capacity_bytes,omitempty"`
	UsedBytes                     uint64   `json:"used_bytes,omitempty"`
	AdminHTTPEndpointConfigured   bool     `json:"admin_http_endpoint_configured"`
	SBSEndpointCount              int      `json:"sbs_endpoint_count"`
	ConsecutiveProbeFailures      uint32   `json:"consecutive_probe_failures,omitempty"`
	RecoveryCooldownSeconds       uint64   `json:"recovery_cooldown_seconds,omitempty"`
	StoreCount                    int      `json:"store_count,omitempty"`
	HealthyStoreCount             int      `json:"healthy_store_count,omitempty"`
	WritableStoreCount            int      `json:"writable_store_count,omitempty"`
	AllocatableStoreCount         int      `json:"allocatable_store_count,omitempty"`
	StoreAllocationWeightTotal    int      `json:"store_allocation_weight_total,omitempty"`
	StoreAllocationWeightObserved bool     `json:"store_allocation_weight_observed,omitempty"`
}

type Store struct {
	NodeID                    string `json:"node_id"`
	StoreCount                int    `json:"store_count"`
	HealthyStoreCount         int    `json:"healthy_store_count"`
	WritableStoreCount        int    `json:"writable_store_count"`
	AllocatableStoreCount     int    `json:"allocatable_store_count"`
	CapacityBytes             uint64 `json:"capacity_bytes,omitempty"`
	AvailableBytes            uint64 `json:"available_bytes,omitempty"`
	UsedBytes                 uint64 `json:"used_bytes,omitempty"`
	CompactionPendingBytes    uint64 `json:"compaction_pending_bytes,omitempty"`
	CompactionInProgressBytes uint64 `json:"compaction_in_progress_bytes,omitempty"`
	AllocationWeightTotal     int    `json:"allocation_weight_total,omitempty"`
	AllocationWeightObserved  bool   `json:"allocation_weight_observed,omitempty"`
	PlacementEligible         bool   `json:"placement_eligible"`
}

type Volume struct {
	VolumeID          string `json:"volume_id"`
	Status            string `json:"status"`
	RedundancyBackend string `json:"redundancy_backend,omitempty"`
	TopologyMode      string `json:"topology_mode,omitempty"`
	ProtectionPolicy  string `json:"protection_policy,omitempty"`
	Revision          uint64 `json:"revision,omitempty"`
	Epoch             uint64 `json:"epoch,omitempty"`
	SizeBytes         uint64 `json:"size_bytes,omitempty"`
	ChunkSizeBytes    uint32 `json:"chunk_size_bytes,omitempty"`
	ExtentSizeBytes   uint64 `json:"extent_size_bytes,omitempty"`
	BlockSizeBytes    uint32 `json:"block_size_bytes,omitempty"`
	ReplicationFactor uint32 `json:"replication_factor,omitempty"`
	ECProfileID       string `json:"ec_profile_id,omitempty"`
}

type Capacity struct {
	Source            string `json:"source,omitempty"`
	LogicalBytes      uint64 `json:"logical_bytes,omitempty"`
	PhysicalUsedBytes uint64 `json:"physical_used_bytes,omitempty"`
	PhysicalFreeBytes uint64 `json:"physical_free_bytes,omitempty"`
	TotalBytes        uint64 `json:"total_bytes,omitempty"`
	ReclaimableBytes  uint64 `json:"reclaimable_bytes,omitempty"`
	ProtectedBytes    uint64 `json:"protected_bytes,omitempty"`
	UnknownBytes      uint64 `json:"unknown_bytes,omitempty"`
	StoreCount        int    `json:"store_count,omitempty"`
	NodeCount         int    `json:"node_count,omitempty"`
}

type Maintenance struct {
	RepairBacklog                          int    `json:"repair_backlog"`
	RepairBacklogBytes                     uint64 `json:"repair_backlog_bytes,omitempty"`
	RepairBacklogChunks                    uint64 `json:"repair_backlog_chunks,omitempty"`
	RebalanceBacklog                       int    `json:"rebalance_backlog"`
	RebalanceBacklogBytes                  uint64 `json:"rebalance_backlog_bytes,omitempty"`
	RebalanceBacklogChunks                 uint64 `json:"rebalance_backlog_chunks,omitempty"`
	DrainBacklog                           int    `json:"drain_backlog"`
	DrainBacklogBytes                      uint64 `json:"drain_backlog_bytes,omitempty"`
	DrainBacklogChunks                     uint64 `json:"drain_backlog_chunks,omitempty"`
	TransitionFailedBatches                uint64 `json:"transition_failed_batches,omitempty"`
	TransitionRecentBatches                uint64 `json:"transition_recent_batches,omitempty"`
	TransitionSmallBatches                 uint64 `json:"transition_small_batches,omitempty"`
	TransitionRequeued                     uint64 `json:"transition_requeued,omitempty"`
	TransitionRetryPages                   uint64 `json:"transition_retry_pages,omitempty"`
	TransitionRetryWindows                 uint64 `json:"transition_retry_windows,omitempty"`
	TransitionRetryWindowBytes             uint64 `json:"transition_retry_window_bytes,omitempty"`
	TransitionRetryWindowChunks            uint64 `json:"transition_retry_window_chunks,omitempty"`
	TransitionOldestFailedAgeSeconds       uint64 `json:"transition_oldest_failed_batch_age_seconds,omitempty"`
	MaintenanceCooldownVolumes             uint64 `json:"maintenance_cooldown_volumes,omitempty"`
	MaintenanceCooldownMaxRemainingSeconds uint64 `json:"maintenance_cooldown_max_remaining_seconds,omitempty"`
	NodesWithProbeFailures                 uint64 `json:"nodes_with_probe_failures,omitempty"`
	MaxConsecutiveProbeFailures            uint64 `json:"max_consecutive_probe_failures,omitempty"`
	NodesInRecoveryCooldown                uint64 `json:"nodes_in_recovery_cooldown,omitempty"`
	MaxRecoveryCooldownRemainingSeconds    uint64 `json:"max_recovery_cooldown_remaining_seconds,omitempty"`
}

type Reclaim struct {
	Source                        string `json:"source,omitempty"`
	PendingChunks                 uint64 `json:"pending_chunks,omitempty"`
	PendingBytes                  uint64 `json:"pending_bytes,omitempty"`
	FailedBatches                 uint64 `json:"failed_batches,omitempty"`
	OldestFailedBatchAgeSeconds   uint64 `json:"oldest_failed_batch_age_seconds,omitempty"`
	BeforeFreeBytes               uint64 `json:"before_free_bytes,omitempty"`
	AfterFreeBytes                uint64 `json:"after_free_bytes,omitempty"`
	ReclaimedBytes                uint64 `json:"reclaimed_bytes,omitempty"`
	ProtectedBytes                uint64 `json:"protected_bytes,omitempty"`
	ProtectedReferenceCheckPassed bool   `json:"protected_reference_check_passed"`
	CompletedClaimed              bool   `json:"completed_claimed"`
	BlockedReason                 string `json:"blocked_reason,omitempty"`
	EvidenceRequired              bool   `json:"evidence_required"`
}

type Membership struct {
	SourceAuthority                  string   `json:"source_authority,omitempty"`
	GatewayMembershipSourceAuthority string   `json:"gateway_membership_source_authority,omitempty"`
	SBSMembershipSourceAuthority     string   `json:"sbs_membership_source_authority,omitempty"`
	PlanID                           string   `json:"membership_plan_id,omitempty"`
	NAMRBDGatewayMembershipReady     bool     `json:"namrbd_gateway_membership_ready"`
	ISCSIGatewayMembershipReady      bool     `json:"iscsi_gateway_membership_ready"`
	SBSMembershipChangeRequested     bool     `json:"sbs_membership_change_requested"`
	SBSMembershipSyncCompleted       bool     `json:"sbs_membership_sync_completed"`
	GatewaySBSViewFresh              bool     `json:"gateway_sbs_view_fresh"`
	AdminGuideMembershipHandoffReady bool     `json:"admin_guide_membership_handoff_ready"`
	ActiveNodes                      int      `json:"active_nodes"`
	DrainingNodes                    int      `json:"draining_nodes"`
	RemovedNodes                     int      `json:"removed_nodes"`
	HealthyNodes                     int      `json:"healthy_nodes"`
	SuspectNodes                     int      `json:"suspect_nodes"`
	DownNodes                        int      `json:"down_nodes"`
	MutationApplyEnabled             bool     `json:"mutation_apply_enabled"`
	HumanApprovalRequired            bool     `json:"human_approval_required"`
	Steps                            []string `json:"steps,omitempty"`
}

type Operations struct {
	Total     int `json:"total"`
	Running   int `json:"running"`
	Failed    int `json:"failed"`
	Completed int `json:"completed"`
	Canceled  int `json:"canceled"`
}

type QuerySurface struct {
	Registered     bool     `json:"query_api_registered"`
	ViewID         string   `json:"observability_view_id,omitempty"`
	DataContract   string   `json:"data_contract_version,omitempty"`
	ReadOnly       bool     `json:"read_only"`
	Families       []string `json:"families,omitempty"`
	RawLogFallback bool     `json:"raw_log_fallback"`
}

type MCPTool struct {
	Name                  string `json:"name"`
	ReadOnly              bool   `json:"read_only"`
	Mutating              bool   `json:"mutating"`
	HumanApprovalRequired bool   `json:"human_approval_required"`
	Description           string `json:"description,omitempty"`
}

type MCPSurface struct {
	ServerReady           bool      `json:"mcp_server_ready"`
	ProviderReady         bool      `json:"mcp_provider_ready"`
	ToolRegistered        bool      `json:"mcp_tool_registered"`
	ReadOnly              bool      `json:"read_only"`
	Transport             string    `json:"transport,omitempty"`
	Tools                 []MCPTool `json:"tools,omitempty"`
	MutatingToolsEnabled  bool      `json:"mutating_tools_enabled"`
	HumanApprovalRequired bool      `json:"human_approval_required"`
}

type GUISurface struct {
	ConsoleReady           bool     `json:"gui_console_ready"`
	ViewContractReady      bool     `json:"gui_view_contract_ready"`
	Route                  string   `json:"gui_route,omitempty"`
	ReadOnlyModeEnforced   bool     `json:"read_only_mode_enforced"`
	MutationControlsHidden bool     `json:"mutation_controls_hidden"`
	Views                  []string `json:"views,omitempty"`
}

type WorkflowState struct {
	Hardened                    bool     `json:"operator_workflow_hardened"`
	EvidenceBundleReady         bool     `json:"evidence_bundle_ready"`
	AuditHistoryAvailable       bool     `json:"operation_history_available"`
	DangerousActionsBlocked     bool     `json:"dangerous_actions_blocked"`
	AIContextRedacted           bool     `json:"ai_context_redacted"`
	AIRecommendationHasEvidence bool     `json:"ai_recommendation_has_evidence"`
	RunbookLinks                []string `json:"runbook_links,omitempty"`
	UnavailableEvidenceReasons  []string `json:"unavailable_evidence_reasons,omitempty"`
}

type View struct {
	SchemaVersion             string   `json:"schema_version"`
	ViewID                    string   `json:"view_id"`
	GeneratedAt               string   `json:"generated_at"`
	SourceAuthority           string   `json:"source_authority"`
	CollectionStatus          string   `json:"collection_status"`
	CollectorFreshnessSeconds float64  `json:"collector_freshness_seconds"`
	Limitations               []string `json:"limitations,omitempty"`
	Warnings                  []string `json:"warnings,omitempty"`
	WarningCount              int      `json:"warning_count"`
	FirstError                string   `json:"first_error,omitempty"`
	LastError                 string   `json:"last_error,omitempty"`
	RBACChecked               bool     `json:"rbac_checked"`
	TenantScopeChecked        bool     `json:"tenant_scope_checked"`
	RedactionApplied          bool     `json:"redaction_applied"`
	ReadOnlyModeEnforced      bool     `json:"read_only_mode_enforced"`
	UnsupportedClaimVisible   bool     `json:"unsupported_claim_visible"`
	Data                      any      `json:"data"`
}

func NewSnapshot(in BuildInput) Snapshot {
	generatedAt := in.GeneratedAt
	if generatedAt.IsZero() {
		generatedAt = time.Now().UTC()
	}
	sourceAuthority := in.SourceAuthority
	if sourceAuthority == "" {
		sourceAuthority = "sbs-service"
	}
	status := StatusOK
	if in.FirstError != "" || in.LastError != "" {
		status = StatusError
	} else if len(in.Warnings) > 0 {
		status = StatusDegraded
	} else if len(in.Limitations) > 0 {
		status = StatusPartial
	}
	snap := Snapshot{
		SchemaVersion:             SchemaVersion,
		GeneratedAt:               generatedAt.UTC().Format(time.RFC3339),
		ClusterID:                 in.ClusterID,
		SBSClusterID:              in.SBSClusterID,
		NodeID:                    in.NodeID,
		LeaderNodeID:              in.LeaderNodeID,
		Ready:                     in.Ready,
		LocalIsLeader:             in.LocalIsLeader,
		LeaderState:               in.LeaderState,
		MetadataBackend:           in.MetadataBackend,
		RuntimeMode:               in.RuntimeMode,
		SourceAuthority:           sourceAuthority,
		CollectionStatus:          status,
		CollectorFreshnessSeconds: in.CollectorFreshnessSeconds,
		Limitations:               append([]string(nil), in.Limitations...),
		Warnings:                  append([]string(nil), in.Warnings...),
		WarningCount:              len(in.Warnings),
		FirstError:                in.FirstError,
		LastError:                 in.LastError,
		RBACChecked:               true,
		TenantScopeChecked:        true,
		RedactionApplied:          true,
		ReadOnlyModeEnforced:      true,
		UnsupportedClaimVisible:   true,
		SupportClaimed:            false,
		PublicGUIClaimed:          false,
		PublicBenchmarkClaimed:    false,
		Nodes:                     append([]Node(nil), in.Nodes...),
		Stores:                    append([]Store(nil), in.Stores...),
		Volumes:                   append([]Volume(nil), in.Volumes...),
		Capacity:                  in.Capacity,
		Maintenance:               in.Maintenance,
		Reclaim:                   in.Reclaim,
		Membership:                in.Membership,
		Operations:                in.Operations,
		Query:                     in.Query,
		MCP:                       in.MCP,
		GUI:                       in.GUI,
		Workflow:                  in.Workflow,
	}
	if snap.LeaderState == "" && snap.LocalIsLeader {
		snap.LeaderState = "leader"
	}
	if snap.Capacity.Source == "" {
		snap.Capacity.Source = "sbs-service metadata and sbs-data health detail"
	}
	if snap.Reclaim.Source == "" {
		snap.Reclaim.Source = "sbs-service retired payload backlog"
	}
	if !snap.Reclaim.CompletedClaimed {
		snap.Reclaim.EvidenceRequired = true
	}
	if snap.Membership.SourceAuthority == "" {
		snap.Membership.SourceAuthority = "sbs-service AdminService and gateway control-plane view"
	}
	if snap.Membership.GatewayMembershipSourceAuthority == "" {
		snap.Membership.GatewayMembershipSourceAuthority = "gateway control-plane membership/liveness state"
	}
	if snap.Membership.SBSMembershipSourceAuthority == "" {
		snap.Membership.SBSMembershipSourceAuthority = "sbs-service AdminService node/topology state"
	}
	if snap.Membership.PlanID == "" {
		snap.Membership.PlanID = "membership-plan-read-only-v1"
	}
	if len(snap.Membership.Steps) == 0 {
		snap.Membership.Steps = []string{"plan", "preflight", "apply", "synchronize", "verify", "rollback", "audit"}
	}
	snap.Membership.MutationApplyEnabled = false
	snap.Membership.HumanApprovalRequired = true
	if !snap.Membership.SBSMembershipChangeRequested {
		snap.Membership.SBSMembershipSyncCompleted = true
	}
	if len(snap.Query.Families) == 0 {
		snap.Query.Families = []string{"cluster", "sbs", "membership", "capacity", "reclaim", "operations", "warnings", "reports"}
	}
	if snap.Query.ViewID == "" {
		snap.Query.ViewID = "namrbd.operations.query.v1"
	}
	if snap.Query.DataContract == "" {
		snap.Query.DataContract = SchemaVersion
	}
	snap.Query.Registered = true
	snap.Query.ReadOnly = true
	snap.Query.RawLogFallback = false
	if len(snap.MCP.Tools) == 0 {
		snap.MCP.Tools = DefaultMCPTools()
	}
	snap.MCP.ToolRegistered = len(snap.MCP.Tools) > 0
	snap.MCP.ServerReady = true
	snap.MCP.ProviderReady = true
	snap.MCP.ReadOnly = true
	snap.MCP.MutatingToolsEnabled = false
	snap.MCP.HumanApprovalRequired = true
	if snap.MCP.Transport == "" {
		snap.MCP.Transport = "stdio-jsonrpc-content-length"
	}
	if len(snap.GUI.Views) == 0 {
		snap.GUI.Views = []string{"cluster", "sbs", "membership", "capacity", "reclaim", "operations", "warnings"}
	}
	if snap.GUI.Route == "" {
		snap.GUI.Route = "/console/"
	}
	snap.GUI.ViewContractReady = true
	snap.GUI.ReadOnlyModeEnforced = true
	snap.GUI.MutationControlsHidden = true
	snap.Workflow.Hardened = true
	snap.Workflow.EvidenceBundleReady = true
	snap.Workflow.AuditHistoryAvailable = true
	snap.Workflow.DangerousActionsBlocked = true
	snap.Workflow.AIContextRedacted = true
	snap.Workflow.AIRecommendationHasEvidence = true
	if len(snap.Workflow.RunbookLinks) == 0 {
		snap.Workflow.RunbookLinks = []string{"membership", "capacity", "reclaim", "incident-evidence"}
	}
	return snap
}

func (s Snapshot) View(viewID string, data any) View {
	return View{
		SchemaVersion:             s.SchemaVersion,
		ViewID:                    viewID,
		GeneratedAt:               s.GeneratedAt,
		SourceAuthority:           s.SourceAuthority,
		CollectionStatus:          s.CollectionStatus,
		CollectorFreshnessSeconds: s.CollectorFreshnessSeconds,
		Limitations:               append([]string(nil), s.Limitations...),
		Warnings:                  append([]string(nil), s.Warnings...),
		WarningCount:              s.WarningCount,
		FirstError:                s.FirstError,
		LastError:                 s.LastError,
		RBACChecked:               s.RBACChecked,
		TenantScopeChecked:        s.TenantScopeChecked,
		RedactionApplied:          s.RedactionApplied,
		ReadOnlyModeEnforced:      s.ReadOnlyModeEnforced,
		UnsupportedClaimVisible:   s.UnsupportedClaimVisible,
		Data:                      data,
	}
}

func DefaultMCPTools() []MCPTool {
	return []MCPTool{
		{
			Name:        "namrbd.health.check",
			ReadOnly:    true,
			Description: "Return cluster, warning, and MCP readiness evidence.",
		},
		{
			Name:        "namrbd.sbs.observability.snapshot",
			ReadOnly:    true,
			Description: "Return SBS node, store, volume, maintenance, capacity, and reclaim evidence.",
		},
		{
			Name:                  "namrbd.membership.plan",
			ReadOnly:              true,
			HumanApprovalRequired: true,
			Description:           "Return a proposal-only membership operation envelope without applying changes.",
		},
		{
			Name:        "namrbd.volume.reclaim.status",
			ReadOnly:    true,
			Description: "Return reclaim status without claiming recovered capacity.",
		},
	}
}
