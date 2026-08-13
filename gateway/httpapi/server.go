package httpapi

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/nosway/namrbd/control/bridge"
	"github.com/nosway/namrbd/gateway/auth"
	"github.com/nosway/namrbd/gateway/service"
	"github.com/nosway/namrbd/internal/structuredlog"
	clustercontrol "github.com/nosway/namrbd/sbs/cluster/control"
	clustermeta "github.com/nosway/namrbd/sbs/cluster/metadata"
	"github.com/nosway/namrbd/volumeid"
)

type Server struct {
	svc                             *service.Service
	cfg                             Config
	performanceAdmission            *gatewayPerformanceAdmission
	performanceAdmissionConfigError error
}

type AttachAdmissionRequest struct {
	VolumeID  uint64
	HostID    string
	DeviceID  uint32
	GatewayID string
}

type AttachAdmissionFunc func(context.Context, AttachAdmissionRequest) error

type Config struct {
	ControlAddress                 string
	ControlPort                    uint16
	ControlUseTLS                  bool
	ControlServerName              string
	DataAddress                    string
	DataPort                       uint16
	GatewayID                      string
	RuntimeMode                    string
	BackendDescription             string
	AdminEndpointConfigured        bool
	StaticReplicaTargetsConfigured bool
	LegacyRawFallbackAllowed       bool
	MaxInflightRequests            uint32
	MaxInflightBytes               uint64
	MaxIOSize                      uint32
	MaxZeroLikeIOSize              uint32
	// Phase C3: optional dataplane token issuer; when set, attach response includes dataplane_auth
	TokenIssuer         auth.TokenIssuer
	DataplaneSessionKey string
	DataplaneTokenTTL   time.Duration
	// OnDetachSuccess is called after successful detach (e.g. to revoke dataplane v2 sessions).
	OnDetachSuccess  func(volumeID uint64)
	ClusterNodeDebug *clustercontrol.Controller
	MetadataRepo     service.MetadataRepository
	AttachAdmission  AttachAdmissionFunc

	PerformanceAdmission         PerformanceAdmissionConfig
	PerformanceBudgetLeaseClient PerformanceBudgetLeaseClient

	HTTPZeroBase64WriteFastPath bool
	HTTPZeroBase64ReadFastPath  bool
	InitialZeroMapEvidence      bool
	ReadPathAttribution         bool
}

func New(svc *service.Service, cfg Config) *Server {
	if cfg.PerformanceAdmission.GatewayID == "" {
		cfg.PerformanceAdmission.GatewayID = cfg.GatewayID
	}
	if cfg.MaxZeroLikeIOSize == 0 {
		cfg.MaxZeroLikeIOSize = cfg.MaxIOSize
	}
	admission, err := newGatewayPerformanceAdmission(cfg.PerformanceAdmission, cfg.PerformanceBudgetLeaseClient)
	return &Server{svc: svc, cfg: cfg, performanceAdmission: admission, performanceAdmissionConfigError: err}
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", s.handleHealthz)
	mux.HandleFunc("/readyz", s.handleHealthz)
	mux.HandleFunc("/metrics", s.handlePrometheusMetrics)
	mux.HandleFunc("/api/v1/volumes/", s.handleVolumeRoutes)
	mux.HandleFunc("/api/v1/discovery/gateways", s.handleDiscoveryGateways)
	mux.HandleFunc("/api/v1/discovery/volumes/", s.handleDiscoveryVolumeRoutes)
	mux.HandleFunc("/api/v1/debug/discovery/volumes/", s.handleDiscoveryDebugRoutes)
	mux.HandleFunc("/api/v1/debug/gateway/metrics", s.handleGatewayMetrics)
	mux.HandleFunc("/api/v1/debug/sbs-cluster/nodes/", s.handleClusterNodeRoutes)
	mux.HandleFunc("/api/v1/debug/sbs-cluster/volumes/", s.handleClusterVolumeRoutes)
	mux.HandleFunc("/api/v1/debug/sbs-cluster/metrics", s.handleClusterMetrics)
	return mux
}

func (s *Server) handleHealthz(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	_, _ = fmt.Fprintln(w, "ok")
}

func (s *Server) handlePrometheusMetrics(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	snapshot := s.svc.MetricsSnapshot()
	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	_, _ = fmt.Fprintln(w, "# HELP namrbd_gateway_ready Whether the namrbd-gateway HTTP control endpoint is ready.")
	_, _ = fmt.Fprintln(w, "# TYPE namrbd_gateway_ready gauge")
	_, _ = fmt.Fprintln(w, "namrbd_gateway_ready 1")
	_, _ = fmt.Fprintln(w, "# HELP namrbd_gateway_runtime_info Static namrbd-gateway runtime metadata.")
	_, _ = fmt.Fprintln(w, "# TYPE namrbd_gateway_runtime_info gauge")
	_, _ = fmt.Fprintf(w, "namrbd_gateway_runtime_info{gateway_id=\"%s\",runtime_mode=\"%s\",admin_endpoint_configured=\"%s\"} 1\n",
		prometheusLabelValue(s.cfg.GatewayID),
		prometheusLabelValue(s.cfg.RuntimeMode),
		strconv.FormatBool(s.cfg.AdminEndpointConfigured),
	)
	_, _ = fmt.Fprintln(w, "# HELP namrbd_gateway_io_requests_total Gateway I/O requests by operation and result class.")
	_, _ = fmt.Fprintln(w, "# TYPE namrbd_gateway_io_requests_total counter")
	_, _ = fmt.Fprintln(w, "# HELP namrbd_gateway_io_bytes_total Gateway I/O bytes by operation.")
	_, _ = fmt.Fprintln(w, "# TYPE namrbd_gateway_io_bytes_total counter")
	_, _ = fmt.Fprintln(w, "# HELP namrbd_gateway_io_latency_milliseconds_total Gateway I/O latency by operation.")
	_, _ = fmt.Fprintln(w, "# TYPE namrbd_gateway_io_latency_milliseconds_total counter")
	for _, op := range sortedGatewayOperations(snapshot.ByOperation) {
		metrics := snapshot.ByOperation[op]
		escapedOp := prometheusLabelValue(op)
		_, _ = fmt.Fprintf(w, "namrbd_gateway_io_requests_total{operation=\"%s\",class=\"total\"} %d\n", escapedOp, metrics.Count)
		_, _ = fmt.Fprintf(w, "namrbd_gateway_io_requests_total{operation=\"%s\",class=\"error\"} %d\n", escapedOp, metrics.Errors)
		_, _ = fmt.Fprintf(w, "namrbd_gateway_io_bytes_total{operation=\"%s\"} %d\n", escapedOp, metrics.Bytes)
		_, _ = fmt.Fprintf(w, "namrbd_gateway_io_latency_milliseconds_total{operation=\"%s\"} %d\n", escapedOp, metrics.TotalLatencyMS)
	}
	_, _ = fmt.Fprintln(w, "# HELP namrbd_gateway_sbs_retries_total Gateway SBS retry events summarized by kind.")
	_, _ = fmt.Fprintln(w, "# TYPE namrbd_gateway_sbs_retries_total counter")
	_, _ = fmt.Fprintf(w, "namrbd_gateway_sbs_retries_total{kind=\"total\"} %d\n", snapshot.RetrySummary.TotalRetries)
	_, _ = fmt.Fprintf(w, "namrbd_gateway_sbs_retries_total{kind=\"open_unavailable\"} %d\n", snapshot.RetrySummary.OpenUnavailableRetries)
	_, _ = fmt.Fprintf(w, "namrbd_gateway_sbs_retries_total{kind=\"reopen\"} %d\n", snapshot.RetrySummary.ReopenRetries)
	_, _ = fmt.Fprintln(w, "# HELP namrbd_gateway_sbs_retry_events_total Gateway SBS retry events by source metric key.")
	_, _ = fmt.Fprintln(w, "# TYPE namrbd_gateway_sbs_retry_events_total counter")
	for _, key := range sortedUint64MetricKeys(snapshot.Retry) {
		_, _ = fmt.Fprintf(w, "namrbd_gateway_sbs_retry_events_total{kind=\"%s\"} %d\n", prometheusLabelValue(key), snapshot.Retry[key])
	}
	if snapshot.IOIdentity != nil {
		identity := snapshot.IOIdentity
		_, _ = fmt.Fprintln(w, "# HELP namrbd_gateway_discard_bytes_total Gateway DISCARD bytes observed.")
		_, _ = fmt.Fprintln(w, "# TYPE namrbd_gateway_discard_bytes_total counter")
		_, _ = fmt.Fprintf(w, "namrbd_gateway_discard_bytes_total %d\n", identity.DiscardBytes)
		_, _ = fmt.Fprintln(w, "# HELP namrbd_gateway_zero_bytes_total Gateway logical zero bytes observed.")
		_, _ = fmt.Fprintln(w, "# TYPE namrbd_gateway_zero_bytes_total counter")
		_, _ = fmt.Fprintf(w, "namrbd_gateway_zero_bytes_total %d\n", identity.LogicalZeroBytes)
		_, _ = fmt.Fprintln(w, "# HELP namrbd_gateway_discard_policy_total Gateway DISCARD observations by policy.")
		_, _ = fmt.Fprintln(w, "# TYPE namrbd_gateway_discard_policy_total counter")
		for _, policy := range sortedUint64MetricKeys(identity.ByDiscardPolicy) {
			_, _ = fmt.Fprintf(w, "namrbd_gateway_discard_policy_total{policy=\"%s\"} %d\n", prometheusLabelValue(policy), identity.ByDiscardPolicy[policy])
		}
		_, _ = fmt.Fprintln(w, "# HELP namrbd_gateway_discard_alignment_fallbacks_total Gateway DISCARD requests that fell back because reclaim geometry was unaligned.")
		_, _ = fmt.Fprintln(w, "# TYPE namrbd_gateway_discard_alignment_fallbacks_total counter")
		_, _ = fmt.Fprintf(w, "namrbd_gateway_discard_alignment_fallbacks_total %d\n", identity.DiscardAlignmentFallbacks)
	}
}

func sortedGatewayOperations(metrics map[string]service.OperationMetrics) []string {
	ops := make([]string, 0, len(metrics))
	for op := range metrics {
		ops = append(ops, op)
	}
	sort.Strings(ops)
	return ops
}

func sortedUint64MetricKeys(metrics map[string]uint64) []string {
	keys := make([]string, 0, len(metrics))
	for key := range metrics {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func prometheusLabelValue(value string) string {
	value = strings.ReplaceAll(value, "\\", "\\\\")
	value = strings.ReplaceAll(value, "\n", "\\n")
	value = strings.ReplaceAll(value, "\"", "\\\"")
	return value
}

type ioRequest struct {
	OffsetBytes  uint64 `json:"offset_bytes"`
	LengthBytes  uint64 `json:"length_bytes"`
	DataBase64   string `json:"data_base64,omitempty"`
	HostID       string `json:"host_id,omitempty"`
	AttachmentID string `json:"attachment_id,omitempty"`
	DeviceID     uint32 `json:"device_id,omitempty"`
}

func isCanonicalZeroBase64ForLength(encoded string, decodedLen uint64) bool {
	if decodedLen > uint64(maxInt) {
		return false
	}
	encodedLen := base64.StdEncoding.EncodedLen(int(decodedLen))
	if len(encoded) != encodedLen {
		return false
	}
	padding := 0
	switch decodedLen % 3 {
	case 1:
		padding = 2
	case 2:
		padding = 1
	}
	payloadEnd := encodedLen - padding
	for i := 0; i < payloadEnd; i++ {
		if encoded[i] != 'A' {
			return false
		}
	}
	for i := payloadEnd; i < encodedLen; i++ {
		if encoded[i] != '=' {
			return false
		}
	}
	return true
}

func isAllZeroBytes(data []byte) bool {
	for _, b := range data {
		if b != 0 {
			return false
		}
	}
	return true
}

var zeroBase64ByDecodedLength sync.Map

func canonicalZeroBase64ForLength(decodedLen uint64) (string, bool) {
	if decodedLen > uint64(maxInt) {
		return "", false
	}
	if encoded, ok := zeroBase64ByDecodedLength.Load(decodedLen); ok {
		return encoded.(string), true
	}
	encodedLen := base64.StdEncoding.EncodedLen(int(decodedLen))
	padding := 0
	switch decodedLen % 3 {
	case 1:
		padding = 2
	case 2:
		padding = 1
	}
	encoded := strings.Repeat("A", encodedLen-padding) + strings.Repeat("=", padding)
	actual, _ := zeroBase64ByDecodedLength.LoadOrStore(decodedLen, encoded)
	return actual.(string), true
}

type clusterNodeHealthRequest struct {
	HealthState string `json:"health_state"`
}

type pathPlanRequest struct {
	MaxActive  int               `json:"max_active"`
	PathHealth map[string]string `json:"path_health"`
}

type runtimePathFeedbackRequest struct {
	NeedsAttention          bool                   `json:"needs_attention"`
	AttentionReasons        []string               `json:"attention_reasons"`
	RecommendedActions      []string               `json:"recommended_actions"`
	AppliedPathPlanRevision uint64                 `json:"applied_path_plan_revision"`
	SourceHost              string                 `json:"source_host,omitempty"`
	NoPath                  *noPathFeedbackRequest `json:"no_path,omitempty"`
}

type noPathFeedbackRequest struct {
	State         string `json:"state"`
	RetryMode     string `json:"retry_mode"`
	RetrySeconds  uint32 `json:"retry_seconds"`
	QueuedReqs    uint64 `json:"queued_reqs"`
	RequeuedReqs  uint64 `json:"requeued_reqs"`
	FailedReqs    uint64 `json:"failed_reqs"`
	RecoveredReqs uint64 `json:"recovered_reqs"`
	EnterCount    uint64 `json:"enter_count"`
	LastReason    string `json:"last_reason"`
}

const (
	runtimePathReductionHoldThreshold = 2
	runtimePathReductionHoldDuration  = 30 * time.Second
	gatewayPathPlanMetricsTimeout     = 500 * time.Millisecond
)

var maxInt = int(^uint(0) >> 1)

type clusterPrioritySnapshot struct {
	TopClass string
	TopCount int
	Match    bool
	OK       bool
}

func containsString(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}

func storageTerminologyMap() map[string]any {
	return map[string]any{
		"placement_extent_term":             "Placement Extent",
		"placement_extent_abbrev":           "PE",
		"allocation_chunk_term":             "Allocation Chunk",
		"allocation_chunk_abbrev":           "AC",
		"allocation_page_term":              "allocation page",
		"legacy_chunk_size_bytes_field":     "chunk_size_bytes",
		"legacy_extent_page_bytes_field":    "extent_page_bytes",
		"allocation_chunk_size_bytes_field": "allocation_chunk_size_bytes",
		"allocation_page_bytes_field":       "allocation_page_bytes",
	}
}

func addStorageGeometryAliases(m map[string]any, chunkSizeBytes, pageBytes uint32) {
	m["storage_terminology"] = storageTerminologyMap()
	m["allocation_chunk_size_bytes"] = chunkSizeBytes
	m["allocation_page_bytes"] = pageBytes
}

func effectivePathPlanMaxActive(status service.VolumeStatusRecord, requested int, manifestPathCount int) (int, string) {
	if requested > 0 {
		return requested, "request"
	}
	if manifestPathCount <= 0 {
		return requested, "default"
	}
	actions := service.ControllerPathPlanRecommendedActions(status)
	if containsString(actions, "complete_gateway_handoff") || containsString(actions, "prefer_fewer_active_paths") {
		return 1, "recommended_action"
	}
	return requested, "default"
}

func controllerPriorityClass(status service.VolumeStatusRecord) string {
	return service.OperatorPathPlanPriorityClass(status)
}

func (s *Server) operatorRecommendedActions(ctx context.Context, status service.VolumeStatusRecord) []string {
	return operatorRecommendedActionsFromCluster(status, s.clusterPrioritySnapshot(ctx, status))
}

func (s *Server) clusterPriorityContext(ctx context.Context, status service.VolumeStatusRecord) (string, int, bool, bool) {
	snapshot := s.clusterPrioritySnapshot(ctx, status)
	return snapshot.TopClass, snapshot.TopCount, snapshot.Match, snapshot.OK
}

func (s *Server) clusterPrioritySnapshot(ctx context.Context, status service.VolumeStatusRecord) clusterPrioritySnapshot {
	if s.cfg.MetadataRepo == nil {
		return clusterPrioritySnapshot{}
	}
	metricsCtx, cancel := context.WithTimeout(ctx, gatewayPathPlanMetricsTimeout)
	defer cancel()
	topClass, topCount := s.topGatewayPathPlanPriority(s.gatewayPathPlanMetrics(metricsCtx))
	if topClass == "" {
		return clusterPrioritySnapshot{}
	}
	return clusterPrioritySnapshot{
		TopClass: topClass,
		TopCount: topCount,
		Match:    topClass == controllerPriorityClass(status),
		OK:       true,
	}
}

func (s *Server) clusterPriorityMismatchActions(ctx context.Context, status service.VolumeStatusRecord) []string {
	return clusterPriorityMismatchActionsFromCluster(status, s.clusterPrioritySnapshot(ctx, status))
}

func operatorRecommendedActionsFromCluster(status service.VolumeStatusRecord, snapshot clusterPrioritySnapshot) []string {
	if !snapshot.OK {
		return service.OperatorPathPlanRecommendedActions(status)
	}
	return service.OperatorPathPlanRecommendedActionsWithCluster(status, snapshot.TopClass)
}

func clusterPriorityMismatchActionsFromCluster(status service.VolumeStatusRecord, snapshot clusterPrioritySnapshot) []string {
	if !snapshot.OK {
		return []string{}
	}
	return service.ClusterPriorityRecommendedActions(service.OperatorPathPlanPriorityClass(status), snapshot.TopClass)
}

func pathPlanObservabilitySummary(status service.VolumeStatusRecord, usableGatewayCount int) map[string]any {
	appliedRevision := status.RuntimeAppliedPathPlanRevision
	var revisionLag uint64
	if status.PathPlanRevision > appliedRevision {
		revisionLag = status.PathPlanRevision - appliedRevision
	}
	runtimeFeedbackAgeMs := int64(0)
	if status.LastRuntimePathFeedbackUnix > 0 {
		runtimeFeedbackAgeMs = (time.Now().Unix() - status.LastRuntimePathFeedbackUnix) * 1000
	}
	return map[string]any{
		"desired_revision":             status.PathPlanRevision,
		"applied_revision":             appliedRevision,
		"revision_lag":                 revisionLag,
		"desired_gateway_count":        len(status.DesiredActiveGatewaySet),
		"observed_gateway_count":       len(status.ObservedActiveGatewaySet),
		"usable_gateway_count":         usableGatewayCount,
		"last_reconcile_unix":          status.ControllerReconcileRequestedAtUnix,
		"next_reconcile_unix":          status.ControllerReconcileScheduledAtUnix,
		"last_reconcile_reason":        status.ControllerReconcileReason,
		"controller_priority_class":    controllerPriorityClass(status),
		"runtime_feedback_age_ms":      runtimeFeedbackAgeMs,
		"runtime_feedback_source_host": status.RuntimePathFeedbackSourceHost,
	}
}

func handoffFencingObservabilitySummary(status service.VolumeStatusRecord) map[string]any {
	return map[string]any{
		"attachment_generation":                status.AttachmentGeneration,
		"writer_fencing_epoch":                 status.WriterFencingEpoch,
		"handoff_required":                     status.HandoffRequired,
		"handoff_stage":                        status.HandoffStage,
		"handoff_reason":                       status.HandoffReason,
		"handoff_requested_at_unix":            status.HandoffRequestedAtUnix,
		"handoff_acked_at_unix":                status.HandoffAckedAtUnix,
		"handoff_acked_generation":             status.HandoffAckedGeneration,
		"handoff_completion_eligible_at_unix":  status.HandoffCompletionEligibleAtUnix,
		"handoff_escalation_count":             status.HandoffEscalationCount,
		"handoff_next_escalation_at_unix":      status.HandoffNextEscalationAtUnix,
		"handoff_target_gateway_set":           append([]string(nil), status.HandoffTargetGatewaySet...),
		"handoff_controller_state":             controllerHandoffState(status),
		"handoff_backoff_state":                controllerHandoffBackoffState(status.HandoffNextEscalationAtUnix),
		"last_stale_writer_reject_unix":        int64(0),
		"stale_writer_reject_total":            uint64(0),
		"stale_writer_reject_counters_present": false,
	}
}

func controllerHandoffState(status service.VolumeStatusRecord) string {
	if !status.HandoffRequired {
		return "not_required"
	}
	if status.HandoffStage != "" {
		return status.HandoffStage
	}
	if status.HandoffRequestedAtUnix > 0 {
		return "requested"
	}
	return "required"
}

func controllerHandoffBackoffState(nextEscalationAtUnix int64) string {
	if nextEscalationAtUnix == 0 {
		return "not_scheduled"
	}
	if time.Now().Unix() >= nextEscalationAtUnix {
		return "eligible"
	}
	return "waiting"
}

func (s *Server) handleVolumeRoutes(w http.ResponseWriter, r *http.Request) {
	trim := strings.TrimPrefix(r.URL.Path, "/api/v1/volumes/")
	parts := strings.Split(trim, "/")
	if len(parts) != 2 {
		http.NotFound(w, r)
		return
	}
	volumeID, err := volumeid.ParseLowercase(parts[0])
	if err != nil {
		http.Error(w, "invalid volume id", http.StatusBadRequest)
		return
	}

	switch parts[1] {
	case "info":
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		s.handleInfo(w, r, volumeID)
	case "attach":
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		s.handleAttach(w, r, volumeID)
	case "reload-size":
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		s.handleReloadSize(w, r, volumeID)
	case "detach":
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		s.handleDetach(w, r, volumeID)
	case "read":
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		s.handleRead(w, r, volumeID)
	case "write":
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		s.handleWrite(w, r, volumeID)
	case "flush":
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		s.handleFlush(w, r, volumeID)
	case "discard":
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		s.handleDiscard(w, r, volumeID)
	case "zero":
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		s.handleZero(w, r, volumeID)
	default:
		http.NotFound(w, r)
	}
}

func (s *Server) handleClusterNodeRoutes(w http.ResponseWriter, r *http.Request) {
	if s.cfg.ClusterNodeDebug == nil {
		http.NotFound(w, r)
		return
	}
	trim := strings.TrimPrefix(r.URL.Path, "/api/v1/debug/sbs-cluster/nodes/")
	nodeID := strings.Trim(trim, "/")
	if nodeID == "" || strings.Contains(nodeID, "/") {
		http.NotFound(w, r)
		return
	}

	switch r.Method {
	case http.MethodGet:
		snapshot, err := s.cfg.ClusterNodeDebug.GetNodeSnapshot(r.Context(), nodeID)
		if err != nil {
			writeClusterError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, s.clusterNodeResponse(snapshot, 0, 0))
	case http.MethodPost:
		var req clusterNodeHealthRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "invalid json", http.StatusBadRequest)
			return
		}
		healthState, ok := parseNodeHealthState(req.HealthState)
		if !ok {
			http.Error(w, "invalid health_state", http.StatusBadRequest)
			return
		}
		rec, failovers, enqueued, err := s.cfg.ClusterNodeDebug.SetNodeHealth(r.Context(), nodeID, healthState)
		if err != nil {
			writeClusterError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, s.clusterNodeResponse(clustercontrol.NodeSnapshot{Node: rec}, failovers, enqueued))
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (s *Server) handleDiscoveryDebugRoutes(w http.ResponseWriter, r *http.Request) {
	if s.cfg.MetadataRepo == nil {
		http.NotFound(w, r)
		return
	}
	trim := strings.TrimPrefix(r.URL.Path, "/api/v1/debug/discovery/volumes/")
	parts := strings.Split(strings.Trim(trim, "/"), "/")
	if len(parts) != 2 || parts[0] == "" || (parts[1] != "path-plan" && parts[1] != "runtime-feedback") {
		http.NotFound(w, r)
		return
	}
	volumeID, err := volumeid.ParseLowercase(parts[0])
	if err != nil {
		http.Error(w, "invalid volume id", http.StatusBadRequest)
		return
	}
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if parts[1] == "runtime-feedback" {
		var req runtimePathFeedbackRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil && !errors.Is(err, io.EOF) {
			http.Error(w, "invalid json", http.StatusBadRequest)
			return
		}
		resp, err := s.discoveryRuntimeFeedbackResponse(r.Context(), volumeID, req)
		if err != nil {
			writeServiceError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, resp)
		return
	}

	var req pathPlanRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil && !errors.Is(err, io.EOF) {
		http.Error(w, "invalid json", http.StatusBadRequest)
		return
	}
	planResp, err := s.discoveryPathPlanResponse(r.Context(), volumeID, req)
	if err != nil {
		writeServiceError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, planResp)
}

func (s *Server) handleClusterVolumeRoutes(w http.ResponseWriter, r *http.Request) {
	trim := strings.TrimPrefix(r.URL.Path, "/api/v1/debug/sbs-cluster/volumes/")
	parts := strings.Split(strings.Trim(trim, "/"), "/")
	if len(parts) == 4 && parts[1] == "clones" {
		volumeID, err := volumeid.ParseLowercase(parts[0])
		if err != nil {
			http.Error(w, "invalid volume id", http.StatusBadRequest)
			return
		}
		cloneID := strings.TrimSpace(parts[2])
		if cloneID == "" {
			http.Error(w, "clone_id is required", http.StatusBadRequest)
			return
		}
		switch parts[3] {
		case "read":
			if r.Method != http.MethodPost {
				http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
				return
			}
			s.handleCloneRead(w, r, volumeID, cloneID)
		case "write":
			if r.Method != http.MethodPost {
				http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
				return
			}
			s.handleCloneWrite(w, r, volumeID, cloneID)
		default:
			http.NotFound(w, r)
		}
		return
	}
	if len(parts) == 4 && parts[1] == "snapshots" {
		volumeID, err := volumeid.ParseLowercase(parts[0])
		if err != nil {
			http.Error(w, "invalid volume id", http.StatusBadRequest)
			return
		}
		snapshotID := strings.TrimSpace(parts[2])
		if snapshotID == "" {
			http.Error(w, "snapshot_id is required", http.StatusBadRequest)
			return
		}
		switch parts[3] {
		case "read":
			if r.Method != http.MethodPost {
				http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
				return
			}
			s.handleSnapshotRead(w, r, volumeID, snapshotID)
		default:
			http.NotFound(w, r)
		}
		return
	}
	if len(parts) != 1 || parts[0] == "" {
		http.NotFound(w, r)
		return
	}
	if s.cfg.ClusterNodeDebug == nil {
		http.NotFound(w, r)
		return
	}
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	volumeID := parts[0]
	if strings.Contains(volumeID, "/") {
		http.NotFound(w, r)
		return
	}
	snapshot, err := s.cfg.ClusterNodeDebug.GetVolume(r.Context(), volumeID)
	if err != nil {
		writeClusterError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"volume":       snapshot.Volume,
		"extents":      snapshot.Extents,
		"replica_sets": snapshot.ReplicaSets,
	})
}

func (s *Server) handleClusterMetrics(w http.ResponseWriter, r *http.Request) {
	if s.cfg.ClusterNodeDebug == nil {
		http.NotFound(w, r)
		return
	}
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	metrics, err := s.cfg.ClusterNodeDebug.GetMetrics(r.Context())
	if err != nil {
		writeClusterError(w, err)
		return
	}
	resp := map[string]any{
		"volumes": metrics.Volumes,
		"nodes":   metrics.Nodes,
		"backlog": metrics.Backlog,
	}
	if s.cfg.MetadataRepo != nil {
		pathPlan := s.gatewayPathPlanMetrics(r.Context())
		resp["path_plan"] = pathPlan
		if topClass, topCount := s.topGatewayPathPlanPriority(pathPlan); topClass != "" {
			resp["top_priority_class"] = topClass
			resp["top_priority_count"] = topCount
		}
	}
	if s.cfg.ClusterNodeDebug != nil {
		healthProbe, recovery, err := s.cfg.ClusterNodeDebug.GetHealthDetailMetrics(r.Context())
		if err == nil {
			resp["health_probe"] = healthProbe
			resp["recovery"] = recovery
		}
	}
	writeJSON(w, http.StatusOK, resp)
}

func (s *Server) gatewayPathPlanMetrics(ctx context.Context) map[string]int {
	out := map[string]int{
		"total":                             0,
		"aggressive_handoff":                0,
		"handoff":                           0,
		"expansion_ready":                   0,
		"refresh":                           0,
		"attention":                         0,
		"normal":                            0,
		"revision_lag_total":                0,
		"revision_lag_max":                  0,
		"runtime_feedback_age_seconds_max":  0,
		"runtime_no_path_queueing":          0,
		"runtime_no_path_failing":           0,
		"replacement_recommendations_total": 0,
		"operator_action_active_restore_gateway_path":      0,
		"operator_action_active_start_replacement_gateway": 0,
		"operator_action_active_check_gateway_registry":    0,
		"operator_action_active_disable_no_path_queueing":  0,
	}
	volumes, err := s.cfg.MetadataRepo.ListVolumes(ctx)
	if err != nil {
		return out
	}
	nowUnix := time.Now().Unix()
	for _, volume := range volumes {
		status, err := s.cfg.MetadataRepo.GetVolumeStatus(ctx, uint64(volume.ID))
		if err != nil {
			continue
		}
		out["total"]++
		out[service.OperatorPathPlanPriorityClass(status)]++
		if status.PathPlanRevision > status.RuntimeAppliedPathPlanRevision {
			lag := int(status.PathPlanRevision - status.RuntimeAppliedPathPlanRevision)
			out["revision_lag_total"] += lag
			if lag > out["revision_lag_max"] {
				out["revision_lag_max"] = lag
			}
		}
		if status.LastRuntimePathFeedbackUnix > 0 {
			age := int(nowUnix - status.LastRuntimePathFeedbackUnix)
			if age > out["runtime_feedback_age_seconds_max"] {
				out["runtime_feedback_age_seconds_max"] = age
			}
		}
		switch status.RuntimeNoPathState {
		case "queueing":
			out["runtime_no_path_queueing"]++
		case "failing":
			out["runtime_no_path_failing"]++
		}
		actions := service.OperatorPathPlanRecommendedActions(status)
		if containsString(actions, "start_replacement_gateway") {
			out["replacement_recommendations_total"]++
			out["operator_action_active_start_replacement_gateway"]++
		}
		if containsString(actions, "restore_gateway_path") {
			out["operator_action_active_restore_gateway_path"]++
		}
		if containsString(actions, "check_gateway_registry") {
			out["operator_action_active_check_gateway_registry"]++
		}
		if containsString(actions, "disable_no_path_queueing_if_unwanted") {
			out["operator_action_active_disable_no_path_queueing"]++
		}
	}
	return out
}

func (s *Server) topGatewayPathPlanPriority(metrics map[string]int) (string, int) {
	for _, name := range []string{"aggressive_handoff", "handoff", "expansion_ready", "refresh", "attention", "normal"} {
		if metrics[name] > 0 {
			return name, metrics[name]
		}
	}
	return "", 0
}

func (s *Server) handleGatewayMetrics(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	resp := map[string]any{
		"runtime": map[string]any{
			"gateway_id":                        s.cfg.GatewayID,
			"runtime_mode":                      s.cfg.RuntimeMode,
			"backend_description":               s.cfg.BackendDescription,
			"admin_endpoint_configured":         s.cfg.AdminEndpointConfigured,
			"static_replica_targets_configured": s.cfg.StaticReplicaTargetsConfigured,
			"legacy_raw_fallback_allowed":       s.cfg.LegacyRawFallbackAllowed,
			"control_plane_owner":               "etcd",
			"cluster_metadata_owner":            "sbs-service",
			"local_payload_owner":               "sbs-data-local-pebble",
		},
		"metrics": s.svc.MetricsSnapshot(),
	}
	writeJSON(w, http.StatusOK, resp)
}

func (s *Server) handleDiscoveryGateways(w http.ResponseWriter, r *http.Request) {
	if s.cfg.MetadataRepo == nil {
		http.NotFound(w, r)
		return
	}
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	gateways, err := s.cfg.MetadataRepo.ListGateways(r.Context())
	if err != nil {
		writeServiceError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"gateways": gateways,
	})
}

func (s *Server) handleDiscoveryVolumeRoutes(w http.ResponseWriter, r *http.Request) {
	if s.cfg.MetadataRepo == nil {
		http.NotFound(w, r)
		return
	}
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	trim := strings.TrimPrefix(r.URL.Path, "/api/v1/discovery/volumes/")
	parts := strings.Split(strings.Trim(trim, "/"), "/")
	if len(parts) != 1 || parts[0] == "" {
		http.NotFound(w, r)
		return
	}
	volumeID, err := volumeid.ParseLowercase(parts[0])
	if err != nil {
		http.Error(w, "invalid volume id", http.StatusBadRequest)
		return
	}
	resp, err := s.discoveryVolumeResponse(r.Context(), volumeID)
	if err != nil {
		writeServiceError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, resp)
}

func (s *Server) handleInfo(w http.ResponseWriter, _ *http.Request, volumeID uint64) {
	v, err := s.svc.VolumeState(volumeID)
	if err != nil {
		writeServiceError(w, err)
		return
	}
	manifest := s.volumeManifest(context.Background(), v, s.lookupVolumeStatus(context.Background(), volumeID), nil)
	if manifest, err = s.expandManifestWithDiscovery(context.Background(), volumeID, manifest); err != nil {
		writeServiceError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, manifest)
}

func (s *Server) handleReloadSize(w http.ResponseWriter, r *http.Request, volumeID uint64) {
	v, err := s.svc.ReloadVolumeDataPath(r.Context(), volumeID)
	if err != nil {
		writeServiceError(w, err)
		return
	}
	manifest := s.volumeManifest(r.Context(), v, s.lookupVolumeStatus(r.Context(), volumeID), nil)
	if manifest, err = s.expandManifestWithDiscovery(r.Context(), volumeID, manifest); err != nil {
		writeServiceError(w, err)
		return
	}
	manifest["reload_result"] = "ok"
	writeJSON(w, http.StatusOK, manifest)
}

func (s *Server) handleAttach(w http.ResponseWriter, r *http.Request, volumeID uint64) {
	started := time.Now()
	phase := "decode"
	var err error
	var serviceAttachDuration time.Duration
	var attachAdmissionDuration time.Duration
	var statusLookupDuration time.Duration
	var manifestBuildDuration time.Duration
	var discoveryDuration time.Duration
	var tokenDuration time.Duration
	defer func() {
		fields := []structuredlog.Field{
			structuredlog.F("volume_id", service.CanonicalVolumeID(volumeID)),
			structuredlog.F("phase", phase),
			structuredlog.F("duration_ms", time.Since(started).Milliseconds()),
			structuredlog.F("attach_admission_duration_ms", attachAdmissionDuration.Milliseconds()),
			structuredlog.F("service_attach_duration_ms", serviceAttachDuration.Milliseconds()),
			structuredlog.F("status_lookup_duration_ms", statusLookupDuration.Milliseconds()),
			structuredlog.F("manifest_build_duration_ms", manifestBuildDuration.Milliseconds()),
			structuredlog.F("discovery_duration_ms", discoveryDuration.Milliseconds()),
			structuredlog.F("token_duration_ms", tokenDuration.Milliseconds()),
		}
		if err != nil {
			structuredlog.Error("gateway.httpapi", "attach_request_failed", err, fields...)
			return
		}
		structuredlog.Info("gateway.httpapi", "attach_request_completed", fields...)
	}()
	var req ioRequest
	_ = json.NewDecoder(r.Body).Decode(&req)
	if s.cfg.AttachAdmission != nil {
		phase = "attach_admission"
		phaseStarted := time.Now()
		err = s.cfg.AttachAdmission(r.Context(), AttachAdmissionRequest{
			VolumeID:  volumeID,
			HostID:    strings.TrimSpace(req.HostID),
			DeviceID:  req.DeviceID,
			GatewayID: s.cfg.GatewayID,
		})
		attachAdmissionDuration = time.Since(phaseStarted)
		if err != nil {
			writeServiceError(w, err)
			return
		}
	}
	phase = "service_attach"
	phaseStarted := time.Now()
	v, err := s.svc.AttachContext(r.Context(), volumeID, req.HostID, req.DeviceID)
	serviceAttachDuration = time.Since(phaseStarted)
	if err != nil {
		writeServiceError(w, err)
		return
	}
	phase = "status_lookup"
	phaseStarted = time.Now()
	status := s.lookupVolumeStatus(r.Context(), volumeID)
	statusLookupDuration = time.Since(phaseStarted)
	phase = "manifest_build"
	phaseStarted = time.Now()
	manifest := s.volumeManifest(r.Context(), v, status, nil)
	manifestBuildDuration = time.Since(phaseStarted)
	phase = "discovery"
	phaseStarted = time.Now()
	manifest, err = s.expandManifestWithDiscovery(r.Context(), volumeID, manifest)
	discoveryDuration = time.Since(phaseStarted)
	if err != nil {
		writeServiceError(w, err)
		return
	}
	allowedPathIDs := manifestPathIDs(manifest)
	if len(allowedPathIDs) == 0 {
		allowedPathIDs = []uint32{0}
	}
	if s.cfg.TokenIssuer != nil && s.cfg.GatewayID != "" && s.cfg.DataplaneSessionKey != "" {
		phase = "token"
		phaseStarted = time.Now()
		ttl := s.cfg.DataplaneTokenTTL
		if ttl <= 0 {
			ttl = 5 * time.Minute
		}
		_, dataplaneAuth, err := s.cfg.TokenIssuer.IssueDataplaneToken(auth.IssueTokenRequest{
			VolumeID:       v.VolumeID,
			AttachmentID:   v.AttachmentID,
			DeviceID:       v.AttachedDeviceID,
			HostID:         v.AttachedHostID,
			GatewayID:      s.cfg.GatewayID,
			Generation:     v.Generation,
			TTL:            ttl,
			AllowedPathIDs: allowedPathIDs,
		})
		if err == nil {
			dataplaneAuth.SessionKey = s.cfg.DataplaneSessionKey
			manifest["dataplane_auth"] = map[string]any{
				"mode":        dataplaneAuth.Mode,
				"token":       dataplaneAuth.Token,
				"session_key": dataplaneAuth.SessionKey,
				"expires_at":  dataplaneAuth.ExpiresAt,
			}
		}
		tokenDuration = time.Since(phaseStarted)
	}
	phase = "write_response"
	writeJSON(w, http.StatusOK, manifest)
	phase = "completed"
}

func (s *Server) handleDetach(w http.ResponseWriter, r *http.Request, volumeID uint64) {
	var req ioRequest
	_ = json.NewDecoder(r.Body).Decode(&req)
	v, err := s.svc.DetachContext(r.Context(), volumeID, req.HostID, req.AttachmentID)
	if err != nil {
		writeServiceError(w, err)
		return
	}
	if s.cfg.OnDetachSuccess != nil {
		s.cfg.OnDetachSuccess(volumeID)
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"status":             "detached",
		"volume_id":          service.CanonicalVolumeID(v.VolumeID),
		"generation":         v.Generation,
		"attached_host_id":   v.AttachedHostID,
		"attachment_id":      v.AttachmentID,
		"attached_device_id": v.AttachedDeviceID,
	})
}

func (s *Server) handleRead(w http.ResponseWriter, r *http.Request, volumeID uint64) {
	started := time.Now()
	loadIndex := strings.TrimSpace(r.Header.Get("X-NAMRBD-Load-Index"))
	loadPhase := strings.TrimSpace(r.Header.Get("X-NAMRBD-Load-Phase"))
	var req ioRequest
	decodeStarted := time.Now()
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		if s.cfg.ReadPathAttribution {
			structuredlog.Error("gateway.httpapi", "read_request_failed", err,
				structuredlog.F("volume_id", service.CanonicalVolumeID(volumeID)),
				structuredlog.F("load_index", loadIndex),
				structuredlog.F("load_phase", loadPhase),
				structuredlog.F("phase", "json_decode"),
				structuredlog.F("duration_ms", time.Since(started).Milliseconds()),
				structuredlog.F("json_decode_duration_ms", time.Since(decodeStarted).Milliseconds()),
			)
		}
		http.Error(w, "invalid json", http.StatusBadRequest)
		return
	}
	jsonDecodeDuration := time.Since(decodeStarted)
	admission, handled := s.admitPerformanceIO(w, r, volumeID, "read", req.LengthBytes, loadIndex, loadPhase)
	if handled {
		return
	}
	readResult := service.ReadResult{}
	data := []byte(nil)
	var err error
	serviceStarted := time.Now()
	readCtx := service.WithReadPathAttribution(service.WithLoadMetadata(r.Context(), loadIndex, loadPhase), s.cfg.ReadPathAttribution)
	if s.cfg.HTTPZeroBase64ReadFastPath {
		readResult, err = s.svc.ReadResult(readCtx, volumeID, req.OffsetBytes, req.LengthBytes)
		data = readResult.Data
	} else {
		data, err = s.svc.Read(readCtx, volumeID, req.OffsetBytes, req.LengthBytes)
	}
	if err != nil {
		fields := []structuredlog.Field{
			structuredlog.F("volume_id", service.CanonicalVolumeID(volumeID)),
			structuredlog.F("offset_bytes", req.OffsetBytes),
			structuredlog.F("length_bytes", req.LengthBytes),
			structuredlog.F("load_index", loadIndex),
			structuredlog.F("load_phase", loadPhase),
			structuredlog.F("phase", "service_read"),
			structuredlog.F("duration_ms", time.Since(started).Milliseconds()),
			structuredlog.F("json_decode_duration_ms", jsonDecodeDuration.Milliseconds()),
			structuredlog.F("service_read_duration_ms", time.Since(serviceStarted).Milliseconds()),
		}
		fields = appendGatewayPerformanceFields(fields, admission)
		if s.cfg.ReadPathAttribution {
			structuredlog.Error("gateway.httpapi", "read_request_failed", err, fields...)
		}
		writeServiceError(w, err)
		return
	}
	serviceReadDuration := time.Since(serviceStarted)
	encodeStarted := time.Now()
	dataBase64 := ""
	base64EncodeSkipped := false
	if s.cfg.HTTPZeroBase64ReadFastPath && readResult.ZeroData {
		if encoded, ok := canonicalZeroBase64ForLength(req.LengthBytes); ok {
			dataBase64 = encoded
			base64EncodeSkipped = true
		}
	}
	if dataBase64 == "" && s.cfg.HTTPZeroBase64ReadFastPath && isAllZeroBytes(data) {
		if encoded, ok := canonicalZeroBase64ForLength(uint64(len(data))); ok {
			dataBase64 = encoded
			base64EncodeSkipped = true
		}
	}
	if dataBase64 == "" {
		dataBase64 = base64.StdEncoding.EncodeToString(data)
	}
	responseEncodeDuration := time.Since(encodeStarted)
	resp := map[string]any{
		"volume_id":    service.CanonicalVolumeID(volumeID),
		"offset_bytes": req.OffsetBytes,
		"length_bytes": req.LengthBytes,
		"data_base64":  dataBase64,
	}
	if payload := admission.responsePayload(); payload != nil {
		resp["phase_o_throttle"] = payload
	}
	responseStarted := time.Now()
	writeJSON(w, http.StatusOK, resp)
	responseWriteDuration := time.Since(responseStarted)
	fields := []structuredlog.Field{
		structuredlog.F("volume_id", service.CanonicalVolumeID(volumeID)),
		structuredlog.F("offset_bytes", req.OffsetBytes),
		structuredlog.F("length_bytes", req.LengthBytes),
		structuredlog.F("load_index", loadIndex),
		structuredlog.F("load_phase", loadPhase),
		structuredlog.F("duration_ms", time.Since(started).Milliseconds()),
		structuredlog.F("json_decode_duration_ms", jsonDecodeDuration.Milliseconds()),
		structuredlog.F("service_read_duration_ms", serviceReadDuration.Milliseconds()),
		structuredlog.F("response_encode_duration_ms", responseEncodeDuration.Milliseconds()),
		structuredlog.F("response_write_duration_ms", responseWriteDuration.Milliseconds()),
		structuredlog.F("zero_base64_fast_path", s.cfg.HTTPZeroBase64ReadFastPath),
		structuredlog.F("base64_encode_skipped", base64EncodeSkipped),
		structuredlog.F("zero_data", readResult.ZeroData),
		structuredlog.F("data_bytes", len(data)),
		structuredlog.F("data_base64_bytes", len(dataBase64)),
	}
	if s.cfg.ReadPathAttribution {
		fields = appendGatewayPerformanceFields(fields, admission)
		structuredlog.Info("gateway.httpapi", "read_request_completed", fields...)
	}
}

func (s *Server) handleWrite(w http.ResponseWriter, r *http.Request, volumeID uint64) {
	started := time.Now()
	loadIndex := strings.TrimSpace(r.Header.Get("X-NAMRBD-Load-Index"))
	loadPhase := strings.TrimSpace(r.Header.Get("X-NAMRBD-Load-Phase"))
	var req ioRequest
	decodeStarted := time.Now()
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		structuredlog.Error("gateway.httpapi", "write_request_failed", err,
			structuredlog.F("volume_id", service.CanonicalVolumeID(volumeID)),
			structuredlog.F("load_index", loadIndex),
			structuredlog.F("load_phase", loadPhase),
			structuredlog.F("phase", "json_decode"),
			structuredlog.F("duration_ms", time.Since(started).Milliseconds()),
			structuredlog.F("json_decode_duration_ms", time.Since(decodeStarted).Milliseconds()),
		)
		http.Error(w, "invalid json", http.StatusBadRequest)
		return
	}
	jsonDecodeDuration := time.Since(decodeStarted)
	base64Started := time.Now()
	var data []byte
	zeroBase64FastPath := false
	base64DecodeSkipped := false
	if s.cfg.HTTPZeroBase64WriteFastPath && isCanonicalZeroBase64ForLength(req.DataBase64, req.LengthBytes) {
		data = make([]byte, int(req.LengthBytes))
		zeroBase64FastPath = true
		base64DecodeSkipped = true
	} else {
		var err error
		data, err = base64.StdEncoding.DecodeString(req.DataBase64)
		if err != nil {
			structuredlog.Error("gateway.httpapi", "write_request_failed", err,
				structuredlog.F("volume_id", service.CanonicalVolumeID(volumeID)),
				structuredlog.F("offset_bytes", req.OffsetBytes),
				structuredlog.F("length_bytes", req.LengthBytes),
				structuredlog.F("load_index", loadIndex),
				structuredlog.F("load_phase", loadPhase),
				structuredlog.F("phase", "base64_decode"),
				structuredlog.F("duration_ms", time.Since(started).Milliseconds()),
				structuredlog.F("json_decode_duration_ms", jsonDecodeDuration.Milliseconds()),
				structuredlog.F("base64_decode_duration_ms", time.Since(base64Started).Milliseconds()),
				structuredlog.F("zero_base64_fast_path", zeroBase64FastPath),
				structuredlog.F("base64_decode_skipped", base64DecodeSkipped),
				structuredlog.F("data_base64_bytes", len(req.DataBase64)),
			)
			http.Error(w, "invalid data_base64", http.StatusBadRequest)
			return
		}
	}
	base64DecodeDuration := time.Since(base64Started)
	admission, handled := s.admitPerformanceIO(w, r, volumeID, "write", uint64(len(data)), loadIndex, loadPhase)
	if handled {
		return
	}
	serviceStarted := time.Now()
	writeCtx := service.WithLoadMetadata(r.Context(), loadIndex, loadPhase)
	if err := s.svc.Write(writeCtx, volumeID, req.OffsetBytes, req.LengthBytes, data); err != nil {
		fields := []structuredlog.Field{
			structuredlog.F("volume_id", service.CanonicalVolumeID(volumeID)),
			structuredlog.F("offset_bytes", req.OffsetBytes),
			structuredlog.F("length_bytes", req.LengthBytes),
			structuredlog.F("load_index", loadIndex),
			structuredlog.F("load_phase", loadPhase),
			structuredlog.F("phase", "service_write"),
			structuredlog.F("duration_ms", time.Since(started).Milliseconds()),
			structuredlog.F("json_decode_duration_ms", jsonDecodeDuration.Milliseconds()),
			structuredlog.F("base64_decode_duration_ms", base64DecodeDuration.Milliseconds()),
			structuredlog.F("service_write_duration_ms", time.Since(serviceStarted).Milliseconds()),
			structuredlog.F("zero_base64_fast_path", zeroBase64FastPath),
			structuredlog.F("base64_decode_skipped", base64DecodeSkipped),
			structuredlog.F("data_bytes", len(data)),
			structuredlog.F("data_base64_bytes", len(req.DataBase64)),
		}
		fields = appendProtectedWriteRejectionFields(fields, err)
		fields = appendGatewayPerformanceFields(fields, admission)
		structuredlog.Error("gateway.httpapi", "write_request_failed", err, fields...)
		writeServiceError(w, err)
		return
	}
	serviceWriteDuration := time.Since(serviceStarted)
	responseStarted := time.Now()
	resp := map[string]any{
		"status":       "ok",
		"volume_id":    service.CanonicalVolumeID(volumeID),
		"offset_bytes": req.OffsetBytes,
		"length_bytes": req.LengthBytes,
	}
	if payload := admission.responsePayload(); payload != nil {
		resp["phase_o_throttle"] = payload
	}
	writeJSON(w, http.StatusOK, resp)
	responseWriteDuration := time.Since(responseStarted)
	fields := []structuredlog.Field{
		structuredlog.F("volume_id", service.CanonicalVolumeID(volumeID)),
		structuredlog.F("offset_bytes", req.OffsetBytes),
		structuredlog.F("length_bytes", req.LengthBytes),
		structuredlog.F("load_index", loadIndex),
		structuredlog.F("load_phase", loadPhase),
		structuredlog.F("duration_ms", time.Since(started).Milliseconds()),
		structuredlog.F("json_decode_duration_ms", jsonDecodeDuration.Milliseconds()),
		structuredlog.F("base64_decode_duration_ms", base64DecodeDuration.Milliseconds()),
		structuredlog.F("service_write_duration_ms", serviceWriteDuration.Milliseconds()),
		structuredlog.F("response_write_duration_ms", responseWriteDuration.Milliseconds()),
		structuredlog.F("zero_base64_fast_path", zeroBase64FastPath),
		structuredlog.F("base64_decode_skipped", base64DecodeSkipped),
		structuredlog.F("data_bytes", len(data)),
		structuredlog.F("data_base64_bytes", len(req.DataBase64)),
	}
	fields = appendGatewayPerformanceFields(fields, admission)
	structuredlog.Info("gateway.httpapi", "write_request_completed", fields...)
}

func (s *Server) admitPerformanceIO(w http.ResponseWriter, r *http.Request, volumeID uint64, operation string, bytes uint64, loadIndex, loadPhase string) (gatewayPerformanceAdmissionDecision, bool) {
	if s.performanceAdmissionConfigError != nil {
		structuredlog.Error("gateway.httpapi", "performance_admission_config_failed", s.performanceAdmissionConfigError,
			structuredlog.F("volume_id", service.CanonicalVolumeID(volumeID)),
			structuredlog.F("operation", operation),
			structuredlog.F("load_index", loadIndex),
			structuredlog.F("load_phase", loadPhase),
		)
		writeJSON(w, http.StatusInternalServerError, map[string]any{
			"error":   phaseOThrottleReasonConfig,
			"message": s.performanceAdmissionConfigError.Error(),
		})
		return gatewayPerformanceAdmissionDecision{}, true
	}
	decision, err := s.performanceAdmission.admit(r.Context(), service.CanonicalVolumeID(volumeID), operation, bytes)
	if err == nil {
		return decision, false
	}
	fields := []structuredlog.Field{
		structuredlog.F("volume_id", service.CanonicalVolumeID(volumeID)),
		structuredlog.F("operation", operation),
		structuredlog.F("load_index", loadIndex),
		structuredlog.F("load_phase", loadPhase),
	}
	fields = appendGatewayPerformanceFields(fields, decision)
	if errors.Is(err, errPerformanceAdmissionRejected) {
		structuredlog.Info("gateway.httpapi", "performance_admission_rejected", fields...)
		writeJSON(w, http.StatusTooManyRequests, map[string]any{
			"error":              phaseOThrottleError,
			"rejection_reason":   decision.RejectionReason,
			"phase_o_throttle":   decision.responsePayload(),
			"before_dispatch":    true,
			"cluster_wide_scope": decision.ClusterWideCapSupport,
		})
		return decision, true
	}
	structuredlog.Error("gateway.httpapi", "performance_admission_failed", err, fields...)
	writeJSON(w, http.StatusServiceUnavailable, map[string]any{
		"error":            "phase_o_throttle_wait_failed",
		"message":          err.Error(),
		"phase_o_throttle": decision.responsePayload(),
		"before_dispatch":  true,
	})
	return decision, true
}

func appendGatewayPerformanceFields(fields []structuredlog.Field, decision gatewayPerformanceAdmissionDecision) []structuredlog.Field {
	for _, field := range decision.structuredFields() {
		fields = append(fields, structuredlog.F(field.key, field.value))
	}
	return fields
}

func (s *Server) handleCloneRead(w http.ResponseWriter, r *http.Request, volumeID uint64, cloneID string) {
	var req ioRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid json", http.StatusBadRequest)
		return
	}
	data, err := s.svc.ReadClone(r.Context(), volumeID, cloneID, req.OffsetBytes, req.LengthBytes)
	if err != nil {
		writeServiceError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"volume_id":    service.CanonicalVolumeID(volumeID),
		"clone_id":     cloneID,
		"offset_bytes": req.OffsetBytes,
		"length_bytes": req.LengthBytes,
		"data_base64":  base64.StdEncoding.EncodeToString(data),
	})
}

func (s *Server) handleSnapshotRead(w http.ResponseWriter, r *http.Request, volumeID uint64, snapshotID string) {
	var req ioRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid json", http.StatusBadRequest)
		return
	}
	data, err := s.svc.ReadSnapshot(r.Context(), volumeID, snapshotID, req.OffsetBytes, req.LengthBytes)
	if err != nil {
		writeServiceError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"volume_id":    service.CanonicalVolumeID(volumeID),
		"snapshot_id":  snapshotID,
		"offset_bytes": req.OffsetBytes,
		"length_bytes": req.LengthBytes,
		"data_base64":  base64.StdEncoding.EncodeToString(data),
	})
}

func (s *Server) handleCloneWrite(w http.ResponseWriter, r *http.Request, volumeID uint64, cloneID string) {
	started := time.Now()
	var req ioRequest
	decodeStarted := time.Now()
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		structuredlog.Error("gateway.httpapi", "clone_write_request_failed", err,
			structuredlog.F("volume_id", service.CanonicalVolumeID(volumeID)),
			structuredlog.F("clone_id", cloneID),
			structuredlog.F("phase", "json_decode"),
			structuredlog.F("duration_ms", time.Since(started).Milliseconds()),
			structuredlog.F("json_decode_duration_ms", time.Since(decodeStarted).Milliseconds()),
		)
		http.Error(w, "invalid json", http.StatusBadRequest)
		return
	}
	jsonDecodeDuration := time.Since(decodeStarted)
	base64Started := time.Now()
	data, err := base64.StdEncoding.DecodeString(req.DataBase64)
	if err != nil {
		structuredlog.Error("gateway.httpapi", "clone_write_request_failed", err,
			structuredlog.F("volume_id", service.CanonicalVolumeID(volumeID)),
			structuredlog.F("clone_id", cloneID),
			structuredlog.F("offset_bytes", req.OffsetBytes),
			structuredlog.F("length_bytes", req.LengthBytes),
			structuredlog.F("phase", "base64_decode"),
			structuredlog.F("duration_ms", time.Since(started).Milliseconds()),
			structuredlog.F("json_decode_duration_ms", jsonDecodeDuration.Milliseconds()),
			structuredlog.F("base64_decode_duration_ms", time.Since(base64Started).Milliseconds()),
			structuredlog.F("data_base64_bytes", len(req.DataBase64)),
		)
		http.Error(w, "invalid data_base64", http.StatusBadRequest)
		return
	}
	base64DecodeDuration := time.Since(base64Started)
	serviceStarted := time.Now()
	if err := s.svc.WriteClone(r.Context(), volumeID, cloneID, req.OffsetBytes, req.LengthBytes, data); err != nil {
		structuredlog.Error("gateway.httpapi", "clone_write_request_failed", err,
			structuredlog.F("volume_id", service.CanonicalVolumeID(volumeID)),
			structuredlog.F("clone_id", cloneID),
			structuredlog.F("offset_bytes", req.OffsetBytes),
			structuredlog.F("length_bytes", req.LengthBytes),
			structuredlog.F("phase", "service_write"),
			structuredlog.F("duration_ms", time.Since(started).Milliseconds()),
			structuredlog.F("json_decode_duration_ms", jsonDecodeDuration.Milliseconds()),
			structuredlog.F("base64_decode_duration_ms", base64DecodeDuration.Milliseconds()),
			structuredlog.F("service_write_duration_ms", time.Since(serviceStarted).Milliseconds()),
			structuredlog.F("data_bytes", len(data)),
			structuredlog.F("data_base64_bytes", len(req.DataBase64)),
		)
		writeServiceError(w, err)
		return
	}
	serviceWriteDuration := time.Since(serviceStarted)
	writeJSON(w, http.StatusOK, map[string]any{
		"status":       "ok",
		"volume_id":    service.CanonicalVolumeID(volumeID),
		"clone_id":     cloneID,
		"offset_bytes": req.OffsetBytes,
		"length_bytes": req.LengthBytes,
	})
	structuredlog.Info("gateway.httpapi", "clone_write_request_completed",
		structuredlog.F("volume_id", service.CanonicalVolumeID(volumeID)),
		structuredlog.F("clone_id", cloneID),
		structuredlog.F("offset_bytes", req.OffsetBytes),
		structuredlog.F("length_bytes", req.LengthBytes),
		structuredlog.F("duration_ms", time.Since(started).Milliseconds()),
		structuredlog.F("json_decode_duration_ms", jsonDecodeDuration.Milliseconds()),
		structuredlog.F("base64_decode_duration_ms", base64DecodeDuration.Milliseconds()),
		structuredlog.F("service_write_duration_ms", serviceWriteDuration.Milliseconds()),
		structuredlog.F("data_bytes", len(data)),
		structuredlog.F("data_base64_bytes", len(req.DataBase64)),
	)
}

func (s *Server) handleFlush(w http.ResponseWriter, r *http.Request, volumeID uint64) {
	if err := s.svc.Flush(r.Context(), volumeID); err != nil {
		writeServiceError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"status":    "ok",
		"volume_id": service.CanonicalVolumeID(volumeID),
	})
}

func (s *Server) handleDiscard(w http.ResponseWriter, r *http.Request, volumeID uint64) {
	var req ioRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid json", http.StatusBadRequest)
		return
	}
	if err := s.svc.Discard(r.Context(), volumeID, req.OffsetBytes, req.LengthBytes); err != nil {
		writeServiceError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"status":       "ok",
		"volume_id":    service.CanonicalVolumeID(volumeID),
		"offset_bytes": req.OffsetBytes,
		"length_bytes": req.LengthBytes,
	})
}

func (s *Server) handleZero(w http.ResponseWriter, r *http.Request, volumeID uint64) {
	var req ioRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid json", http.StatusBadRequest)
		return
	}
	if err := s.svc.Zero(r.Context(), volumeID, req.OffsetBytes, req.LengthBytes); err != nil {
		writeServiceError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"status":       "ok",
		"volume_id":    service.CanonicalVolumeID(volumeID),
		"offset_bytes": req.OffsetBytes,
		"length_bytes": req.LengthBytes,
	})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeServiceError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, service.ErrVolumeNotFound):
		http.Error(w, err.Error(), http.StatusNotFound)
	case errors.Is(err, service.ErrBadAlignment), errors.Is(err, service.ErrDiscardAlignment), errors.Is(err, service.ErrBadDataLength):
		http.Error(w, err.Error(), http.StatusBadRequest)
	case errors.Is(err, service.ErrHostIDRequired), errors.Is(err, service.ErrAttachmentIDRequired):
		http.Error(w, err.Error(), http.StatusBadRequest)
	case errors.Is(err, service.ErrVolumeDisabled):
		http.Error(w, err.Error(), http.StatusConflict)
	case errors.Is(err, service.ErrKeyAdmissionRejected):
		http.Error(w, err.Error(), http.StatusConflict)
	case errors.Is(err, service.ErrProtectedWriteRejected):
		http.Error(w, err.Error(), http.StatusConflict)
	case errors.Is(err, service.ErrWriterFenced):
		http.Error(w, err.Error(), http.StatusConflict)
	case errors.Is(err, service.ErrAttachConflict), errors.Is(err, service.ErrDetachConflict):
		http.Error(w, err.Error(), http.StatusConflict)
	case errors.Is(err, service.ErrOutOfRange):
		http.Error(w, err.Error(), http.StatusRequestedRangeNotSatisfiable)
	default:
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func appendProtectedWriteRejectionFields(fields []structuredlog.Field, err error) []structuredlog.Field {
	rejection, ok := service.ProtectedWriteRejectionFromError(err)
	if !ok {
		return fields
	}
	return append(fields,
		structuredlog.F("protected_state_observed", true),
		structuredlog.F("protected_state", rejection.ProtectedState),
		structuredlog.F("rejection_code", rejection.ReasonCode),
		structuredlog.F("sealed_object_id", rejection.SealedObjectID),
		structuredlog.F("seal_operation_id", rejection.SealOperationID),
		structuredlog.F("lifecycle_state", rejection.LifecycleState),
	)
}

func writeClusterError(w http.ResponseWriter, err error) {
	if errors.Is(err, clustermeta.ErrNotFound) {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	http.Error(w, err.Error(), http.StatusInternalServerError)
}

func (s *Server) clusterNodeResponse(snapshot clustercontrol.NodeSnapshot, failovers, enqueued int) map[string]any {
	rec := snapshot.Node
	resp := map[string]any{
		"node_id":             rec.NodeID,
		"replica_id":          rec.ReplicaID,
		"lifecycle_state":     rec.LifecycleState,
		"health_state":        rec.HealthState,
		"zone":                rec.Zone,
		"host":                rec.Host,
		"last_heartbeat_unix": rec.LastHeartbeatUnix,
		"capabilities":        rec.Capabilities,
		"sbs_endpoints":       rec.SBSEndpoints,
		"primary_failovers":   failovers,
		"repair_enqueued":     enqueued,
	}
	if snapshot.Detail == nil {
		return resp
	}
	detail := snapshot.Detail
	resp["last_probe_unix"] = detail.LastProbeUnix
	resp["last_probe_error"] = detail.LastProbeError
	resp["consecutive_probe_failures"] = detail.ConsecutiveProbeFailures
	resp["consecutive_probe_successes"] = detail.ConsecutiveProbeSuccesses
	resp["health_reason"] = detail.HealthReason
	resp["health_updated_by"] = detail.HealthUpdatedBy
	resp["recovery_eligible_at_unix"] = detail.RecoveryEligibleAtUnix
	return resp
}

func parseNodeHealthState(raw string) (clustermeta.NodeHealthState, bool) {
	switch clustermeta.NodeHealthState(strings.TrimSpace(raw)) {
	case clustermeta.NodeHealthHealthy:
		return clustermeta.NodeHealthHealthy, true
	case clustermeta.NodeHealthSuspect:
		return clustermeta.NodeHealthSuspect, true
	case clustermeta.NodeHealthDown:
		return clustermeta.NodeHealthDown, true
	default:
		return "", false
	}
}

func (s *Server) volumeManifest(ctx context.Context, v service.VolumeState, status *service.VolumeStatusRecord, dataplaneAuth *auth.DataplaneAuth) map[string]any {
	retrySummary := s.svc.MetricsSnapshot().RetrySummary
	m := map[string]any{
		"volume_id":          service.CanonicalVolumeID(v.VolumeID),
		"generation":         v.Generation,
		"size_bytes":         v.SizeBytes,
		"block_size":         v.BlockSize,
		"chunk_size_bytes":   v.ChunkSizeBytes,
		"extent_page_bytes":  v.ExtentPageBytes,
		"attachment_id":      v.AttachmentID,
		"attached_host_id":   v.AttachedHostID,
		"attached_device_id": v.AttachedDeviceID,
		"control_endpoints": []map[string]any{
			{
				"address":     s.cfg.ControlAddress,
				"port":        s.cfg.ControlPort,
				"use_tls":     s.cfg.ControlUseTLS,
				"server_name": s.cfg.ControlServerName,
				"auth_mode":   "bearer",
			},
		},
		"dataplane_endpoints": []map[string]any{
			{
				"path_id":    0,
				"gateway_id": s.cfg.GatewayID,
				"address":    s.cfg.DataAddress,
				"port":       s.cfg.DataPort,
				"use_tls":    false,
				"auth_mode":  "bearer",
				"priority":   100,
			},
		},
		"max_inflight_requests": s.cfg.MaxInflightRequests,
		"max_inflight_bytes":    s.cfg.MaxInflightBytes,
		"max_io_size":           s.cfg.MaxIOSize,
		"max_zero_like_io_size": s.cfg.MaxZeroLikeIOSize,
		"gateway_retry_summary": map[string]uint64{
			"total_retries":            retrySummary.TotalRetries,
			"open_unavailable_retries": retrySummary.OpenUnavailableRetries,
			"reopen_retries":           retrySummary.ReopenRetries,
		},
	}
	addStorageGeometryAliases(m, v.ChunkSizeBytes, v.ExtentPageBytes)
	if evidence, err := s.initialZeroMapEvidence(ctx, v); err == nil && evidence.Trusted {
		m["initial_zero_map_trusted"] = true
		m["initial_zero_map_all_zero"] = evidence.AllZero
		if evidence.GranuleBytes != 0 {
			m["initial_zero_map_granule_bytes"] = evidence.GranuleBytes
		}
		m["initial_zero_map_checked_pages"] = evidence.CheckedPageCount
		m["initial_zero_map_checked_chunks"] = evidence.CheckedChunkCount
	} else if err != nil {
		structuredlog.Info("gateway.httpapi", "initial_zero_map_evidence_unavailable",
			structuredlog.F("volume_id", service.CanonicalVolumeID(v.VolumeID)),
			structuredlog.F("error", err.Error()),
		)
	}
	if status != nil {
		clusterPriority := s.clusterPrioritySnapshot(ctx, *status)
		m["path_plan_revision"] = status.PathPlanRevision
		m["path_plan_reapply_requested"] = status.PathPlanReapplyRequested
		m["path_plan_reapply_reason"] = status.PathPlanReapplyReason
		m["path_plan_reapply_requested_at_unix"] = status.PathPlanReapplyRequestedAtUnix
		m["runtime_path_expansion_backoff_level"] = status.RuntimePathExpansionBackoffLevel
		m["runtime_path_expansion_eligible_at_unix"] = status.RuntimePathExpansionEligibleAtUnix
		m["path_plan"] = pathPlanObservabilitySummary(*status, len(status.ObservedActiveGatewaySet))
		m["runtime_no_path"] = service.RuntimeNoPathSummary(*status)
		m["handoff_fencing"] = handoffFencingObservabilitySummary(*status)
		m["attachment_generation"] = status.AttachmentGeneration
		m["writer_fencing_epoch"] = status.WriterFencingEpoch
		m["controller_reconcile_requested_at_unix"] = status.ControllerReconcileRequestedAtUnix
		m["controller_reconcile_reason"] = status.ControllerReconcileReason
		m["controller_reconcile_scheduled_at_unix"] = status.ControllerReconcileScheduledAtUnix
		m["controller_reconcile_scheduled_reason"] = status.ControllerReconcileScheduledReason
		m["handoff_required"] = status.HandoffRequired
		m["handoff_requested_at_unix"] = status.HandoffRequestedAtUnix
		m["handoff_acked_at_unix"] = status.HandoffAckedAtUnix
		m["handoff_acked_generation"] = status.HandoffAckedGeneration
		m["handoff_completion_eligible_at_unix"] = status.HandoffCompletionEligibleAtUnix
		m["handoff_escalation_count"] = status.HandoffEscalationCount
		m["handoff_next_escalation_at_unix"] = status.HandoffNextEscalationAtUnix
		m["handoff_stage"] = status.HandoffStage
		m["handoff_reason"] = status.HandoffReason
		m["handoff_target_gateway_set"] = append([]string(nil), status.HandoffTargetGatewaySet...)
		m["operator_recommended_actions"] = operatorRecommendedActionsFromCluster(*status, clusterPriority)
		m["cluster_priority_mismatch_actions"] = clusterPriorityMismatchActionsFromCluster(*status, clusterPriority)
		if clusterPriority.OK {
			m["cluster_top_priority_class"] = clusterPriority.TopClass
			m["cluster_top_priority_count"] = clusterPriority.TopCount
			m["cluster_priority_matches_controller"] = clusterPriority.Match
		}
	}
	if dataplaneAuth != nil && dataplaneAuth.Mode != "" {
		m["dataplane_auth"] = map[string]any{
			"mode":        dataplaneAuth.Mode,
			"token":       dataplaneAuth.Token,
			"session_key": dataplaneAuth.SessionKey,
			"expires_at":  dataplaneAuth.ExpiresAt,
		}
	}
	return m
}

func (s *Server) initialZeroMapEvidence(ctx context.Context, v service.VolumeState) (service.InitialZeroMapEvidence, error) {
	if !s.cfg.InitialZeroMapEvidence {
		return service.InitialZeroMapEvidence{}, nil
	}
	return s.svc.InitialZeroMapEvidenceForState(ctx, v)
}

func (s *Server) lookupVolumeStatus(ctx context.Context, volumeID uint64) *service.VolumeStatusRecord {
	if s.cfg.MetadataRepo == nil {
		return nil
	}
	status, err := s.cfg.MetadataRepo.GetVolumeStatus(ctx, volumeID)
	if err != nil {
		return nil
	}
	return &status
}

func (s *Server) expandManifestWithDiscovery(ctx context.Context, volumeID uint64, manifest map[string]any) (map[string]any, error) {
	if s.cfg.MetadataRepo == nil {
		return manifest, nil
	}
	discovery, err := s.discoveryVolumeResponse(ctx, volumeID)
	if err != nil {
		return nil, err
	}

	if gateways, ok := discovery["gateways"].([]service.GatewayRecord); ok {
		controlEndpoints := make([]map[string]any, 0)
		for _, gateway := range gateways {
			for _, endpoint := range gateway.ControlEndpoints {
				controlEndpoints = append(controlEndpoints, map[string]any{
					"address":      endpoint.Address,
					"port":         endpoint.Port,
					"use_tls":      endpoint.UseTLS,
					"server_name":  endpoint.ServerName,
					"auth_mode":    endpoint.AuthMode,
					"gateway_id":   gateway.GatewayID,
					"path_id":      endpoint.PathID,
					"priority":     endpoint.Priority,
					"api_prefix":   "/api/v1",
					"bearer_token": "",
				})
			}
		}
		if len(controlEndpoints) > 0 {
			manifest["control_endpoints"] = controlEndpoints
		}
	}

	if paths, ok := discovery["dataplane_paths"].([]map[string]any); ok {
		dataplaneEndpoints := make([]map[string]any, 0, len(paths))
		for _, path := range paths {
			if suppressed, ok := path["handoff_suppressed"].(bool); ok && suppressed {
				continue
			}
			dataplaneEndpoints = append(dataplaneEndpoints, map[string]any{
				"path_id":     path["path_id"],
				"gateway_id":  path["gateway_id"],
				"address":     path["address"],
				"port":        path["port"],
				"use_tls":     path["use_tls"],
				"server_name": path["server_name"],
				"auth_mode":   path["auth_mode"],
				"priority":    path["discovery_priority"],
			})
		}
		if len(dataplaneEndpoints) > 0 {
			manifest["dataplane_endpoints"] = dataplaneEndpoints
		}
	}

	return manifest, nil
}

func manifestPathIDs(manifest map[string]any) []uint32 {
	raw, ok := manifest["dataplane_endpoints"].([]map[string]any)
	if ok {
		out := make([]uint32, 0, len(raw))
		for _, endpoint := range raw {
			if pathID, ok := endpoint["path_id"].(uint32); ok {
				out = append(out, pathID)
			}
		}
		return out
	}

	rawAny, ok := manifest["dataplane_endpoints"].([]any)
	if !ok {
		return nil
	}
	out := make([]uint32, 0, len(rawAny))
	for _, entry := range rawAny {
		endpoint, ok := entry.(map[string]any)
		if !ok {
			continue
		}
		switch pathID := endpoint["path_id"].(type) {
		case uint32:
			out = append(out, pathID)
		case float64:
			out = append(out, uint32(pathID))
		}
	}
	return out
}

func (s *Server) discoveryVolumeResponse(ctx context.Context, volumeID uint64) (map[string]any, error) {
	spec, err := s.cfg.MetadataRepo.GetVolume(ctx, volumeID)
	if err != nil {
		return nil, err
	}
	status, err := s.cfg.MetadataRepo.GetVolumeStatus(ctx, volumeID)
	if err != nil {
		return nil, err
	}
	attachment, err := s.cfg.MetadataRepo.GetAttachment(ctx, volumeID)
	if err != nil {
		return nil, err
	}
	generation := attachment.Generation
	gateways, err := s.cfg.MetadataRepo.ListGateways(ctx)
	if err != nil {
		return nil, err
	}
	desiredGateways := make(map[string]struct{}, len(status.DesiredActiveGatewaySet))
	for _, gatewayID := range status.DesiredActiveGatewaySet {
		desiredGateways[gatewayID] = struct{}{}
	}
	observedGateways := make(map[string]struct{}, len(status.ObservedActiveGatewaySet))
	for _, gatewayID := range status.ObservedActiveGatewaySet {
		observedGateways[gatewayID] = struct{}{}
	}

	sort.Slice(gateways, func(i, j int) bool {
		_, iObserved := observedGateways[gateways[i].GatewayID]
		_, jObserved := observedGateways[gateways[j].GatewayID]
		if iObserved != jObserved {
			return iObserved
		}
		_, iDesired := desiredGateways[gateways[i].GatewayID]
		_, jDesired := desiredGateways[gateways[j].GatewayID]
		if iDesired != jDesired {
			return iDesired
		}
		iCurrent := gateways[i].GatewayID == status.CurrentGatewayID
		jCurrent := gateways[j].GatewayID == status.CurrentGatewayID
		if iCurrent != jCurrent {
			return iCurrent
		}
		return gateways[i].GatewayID < gateways[j].GatewayID
	})

	filtered := make([]service.GatewayRecord, 0, len(gateways))
	dataplanePaths := make([]map[string]any, 0)
	pathID := uint32(0)
	for _, gateway := range gateways {
		if !gatewayActiveForDiscovery(gateway.ConnectionState) {
			continue
		}
		filtered = append(filtered, gateway)
		_, isDesiredGateway := desiredGateways[gateway.GatewayID]
		_, isObservedGateway := observedGateways[gateway.GatewayID]
		handoffSuppressed := status.HandoffRequired && gateway.GatewayID == status.CurrentGatewayID && !containsString(status.HandoffTargetGatewaySet, gateway.GatewayID)
		suppressionReason := ""
		if handoffSuppressed {
			suppressionReason = "handoff_in_progress"
		}
		for idx, endpoint := range gateway.DataplaneEndpoints {
			priority := endpoint.Priority
			if priority == 0 {
				priority = 100
			}
			dataplanePaths = append(dataplanePaths, map[string]any{
				"path_id":             pathID,
				"gateway_path_id":     endpoint.PathID,
				"gateway_id":          gateway.GatewayID,
				"connection_state":    gateway.ConnectionState,
				"path_state":          string(gateway.ConnectionState),
				"is_desired_gateway":  isDesiredGateway,
				"is_observed_gateway": isObservedGateway,
				"is_owner_gateway":    gateway.GatewayID == status.CurrentGatewayID,
				"handoff_suppressed":  handoffSuppressed,
				"suppression_reason":  suppressionReason,
				"address":             endpoint.Address,
				"port":                endpoint.Port,
				"use_tls":             endpoint.UseTLS,
				"server_name":         endpoint.ServerName,
				"auth_mode":           endpoint.AuthMode,
				"priority":            priority,
				"discovery_priority":  discoveryPriority(gateway, idx),
			})
			pathID++
		}
	}
	// Discovery is on the attach/reconfigure critical path. Keep it scoped to
	// this volume and the gateway registry; cluster-wide priority scans are
	// exposed through debug/info endpoints instead.
	clusterPriority := clusterPrioritySnapshot{}

	return map[string]any{
		"volume": map[string]any{
			"volume_id":                               service.CanonicalVolumeID(volumeID),
			"volume_name":                             spec.Name,
			"size_bytes":                              spec.SizeBytes,
			"block_size":                              spec.BlockSize,
			"chunk_size_bytes":                        spec.ChunkSizeBytes,
			"extent_page_bytes":                       spec.ExtentPageBytes,
			"allocation_chunk_size_bytes":             spec.ChunkSizeBytes,
			"allocation_page_bytes":                   spec.ExtentPageBytes,
			"generation":                              generation,
			"attachment_id":                           attachment.AttachmentID,
			"attached_host_id":                        attachment.HostID,
			"attached_device_id":                      attachment.DeviceID,
			"desired_active_gateway_set":              append([]string(nil), status.DesiredActiveGatewaySet...),
			"observed_active_gateway_set":             append([]string(nil), status.ObservedActiveGatewaySet...),
			"path_plan_revision":                      status.PathPlanRevision,
			"path_plan":                               pathPlanObservabilitySummary(status, len(filtered)),
			"handoff_fencing":                         handoffFencingObservabilitySummary(status),
			"path_plan_reapply_requested":             status.PathPlanReapplyRequested,
			"path_plan_reapply_reason":                status.PathPlanReapplyReason,
			"path_plan_reapply_requested_at_unix":     status.PathPlanReapplyRequestedAtUnix,
			"runtime_path_expansion_backoff_level":    status.RuntimePathExpansionBackoffLevel,
			"runtime_path_expansion_eligible_at_unix": status.RuntimePathExpansionEligibleAtUnix,
			"attachment_generation":                   status.AttachmentGeneration,
			"writer_fencing_epoch":                    status.WriterFencingEpoch,
			"controller_reconcile_requested_at_unix":  status.ControllerReconcileRequestedAtUnix,
			"controller_reconcile_reason":             status.ControllerReconcileReason,
			"controller_reconcile_scheduled_at_unix":  status.ControllerReconcileScheduledAtUnix,
			"controller_reconcile_scheduled_reason":   status.ControllerReconcileScheduledReason,
			"handoff_required":                        status.HandoffRequired,
			"handoff_requested_at_unix":               status.HandoffRequestedAtUnix,
			"handoff_acked_at_unix":                   status.HandoffAckedAtUnix,
			"handoff_acked_generation":                status.HandoffAckedGeneration,
			"handoff_completion_eligible_at_unix":     status.HandoffCompletionEligibleAtUnix,
			"handoff_escalation_count":                status.HandoffEscalationCount,
			"handoff_next_escalation_at_unix":         status.HandoffNextEscalationAtUnix,
			"handoff_stage":                           status.HandoffStage,
			"handoff_reason":                          status.HandoffReason,
			"handoff_target_gateway_set":              append([]string(nil), status.HandoffTargetGatewaySet...),
			"path_plan_needs_attention":               status.PathPlanNeedsAttention,
			"path_plan_attention_reasons":             append([]string(nil), status.PathPlanAttentionReasons...),
			"path_plan_recommended_actions":           append([]string(nil), status.PathPlanRecommendedActions...),
			"runtime_path_needs_attention":            status.RuntimePathNeedsAttention,
			"runtime_path_attention_reasons":          append([]string(nil), status.RuntimePathAttentionReasons...),
			"runtime_path_recommended_actions":        append([]string(nil), status.RuntimePathRecommendedActions...),
			"runtime_applied_path_plan_revision":      status.RuntimeAppliedPathPlanRevision,
			"runtime_applied_path_reported_at_unix":   status.RuntimeAppliedPathReportedAtUnix,
			"runtime_no_path":                         service.RuntimeNoPathSummary(status),
			"controller_needs_attention":              service.ControllerPathPlanNeedsAttention(status),
			"controller_attention_reasons":            service.ControllerPathPlanAttentionReasons(status),
			"controller_recommended_actions":          service.ControllerPathPlanRecommendedActions(status),
			"controller_priority_class":               controllerPriorityClass(status),
			"operator_recommended_actions":            operatorRecommendedActionsFromCluster(status, clusterPriority),
			"cluster_priority_mismatch_actions":       clusterPriorityMismatchActionsFromCluster(status, clusterPriority),
			"cluster_top_priority_class":              clusterPriority.TopClass,
			"cluster_top_priority_count":              clusterPriority.TopCount,
			"cluster_priority_matches_controller":     clusterPriority.Match,
			"current_gateway_id":                      status.CurrentGatewayID,
			"gateway_state":                           status.GatewayConnectionState,
			"active_gateway_count":                    len(filtered),
			"storage_terminology":                     storageTerminologyMap(),
		},
		"gateways":        filtered,
		"dataplane_paths": dataplanePaths,
	}, nil
}

func (s *Server) discoveryPathPlanResponse(ctx context.Context, volumeID uint64, req pathPlanRequest) (map[string]any, error) {
	status, err := s.cfg.MetadataRepo.GetVolumeStatus(ctx, volumeID)
	if err != nil {
		return nil, err
	}
	v, err := s.svc.VolumeState(volumeID)
	if err != nil {
		return nil, err
	}
	manifest, err := s.expandManifestWithDiscovery(ctx, volumeID, s.volumeManifest(ctx, v, s.lookupVolumeStatus(ctx, volumeID), nil))
	if err != nil {
		return nil, err
	}
	raw, err := json.Marshal(manifest)
	if err != nil {
		return nil, err
	}

	health := make(map[uint32]bridge.PathHealthState, len(req.PathHealth))
	for rawPathID, rawState := range req.PathHealth {
		pathID, err := parseUint32(rawPathID)
		if err != nil {
			return nil, fmt.Errorf("invalid path_health path id %q", rawPathID)
		}
		health[pathID] = bridge.PathHealthState(strings.TrimSpace(rawState))
	}

	manifestIDs := manifestPathIDs(manifest)
	effectiveMaxActive, maxActiveSource := effectivePathPlanMaxActive(status, req.MaxActive, len(manifestIDs))
	plan, err := bridge.BuildPathSelectionPlan(string(raw), health, effectiveMaxActive)
	if err != nil {
		return nil, err
	}
	clusterPriority := s.clusterPrioritySnapshot(ctx, status)
	return map[string]any{
		"volume_id":                               service.CanonicalVolumeID(volumeID),
		"requested_max_active":                    req.MaxActive,
		"effective_max_active":                    effectiveMaxActive,
		"max_active_source":                       maxActiveSource,
		"max_active":                              effectiveMaxActive,
		"allowed_path_ids":                        bridge.AllowedPathIDs(plan),
		"active":                                  nonNilDataplaneEndpoints(plan.Active),
		"standby":                                 nonNilDataplaneEndpoints(plan.Standby),
		"suppressed":                              nonNilDataplaneEndpoints(plan.Suppressed),
		"health_overrides":                        req.PathHealth,
		"manifest_path_ids":                       manifestIDs,
		"desired_active_gateway_set":              append([]string(nil), status.DesiredActiveGatewaySet...),
		"observed_active_gateway_set":             append([]string(nil), status.ObservedActiveGatewaySet...),
		"path_plan_revision":                      status.PathPlanRevision,
		"path_plan":                               pathPlanObservabilitySummary(status, len(status.ObservedActiveGatewaySet)),
		"handoff_fencing":                         handoffFencingObservabilitySummary(status),
		"path_plan_reapply_requested":             status.PathPlanReapplyRequested,
		"path_plan_reapply_reason":                status.PathPlanReapplyReason,
		"path_plan_reapply_requested_at_unix":     status.PathPlanReapplyRequestedAtUnix,
		"runtime_path_expansion_backoff_level":    status.RuntimePathExpansionBackoffLevel,
		"writer_fencing_epoch":                    status.WriterFencingEpoch,
		"handoff_required":                        status.HandoffRequired,
		"handoff_requested_at_unix":               status.HandoffRequestedAtUnix,
		"handoff_acked_at_unix":                   status.HandoffAckedAtUnix,
		"handoff_acked_generation":                status.HandoffAckedGeneration,
		"handoff_completion_eligible_at_unix":     status.HandoffCompletionEligibleAtUnix,
		"handoff_escalation_count":                status.HandoffEscalationCount,
		"handoff_next_escalation_at_unix":         status.HandoffNextEscalationAtUnix,
		"handoff_stage":                           status.HandoffStage,
		"handoff_reason":                          status.HandoffReason,
		"handoff_target_gateway_set":              append([]string(nil), status.HandoffTargetGatewaySet...),
		"path_plan_needs_attention":               status.PathPlanNeedsAttention,
		"path_plan_attention_reasons":             append([]string(nil), status.PathPlanAttentionReasons...),
		"path_plan_recommended_actions":           append([]string(nil), status.PathPlanRecommendedActions...),
		"runtime_path_needs_attention":            status.RuntimePathNeedsAttention,
		"runtime_path_attention_reasons":          append([]string(nil), status.RuntimePathAttentionReasons...),
		"runtime_path_recommended_actions":        append([]string(nil), status.RuntimePathRecommendedActions...),
		"runtime_path_feedback_count":             status.RuntimePathFeedbackCount,
		"last_runtime_path_feedback_unix":         status.LastRuntimePathFeedbackUnix,
		"runtime_path_reduction_hold_until_unix":  status.RuntimePathReductionHoldUntilUnix,
		"runtime_path_expansion_eligible_at_unix": status.RuntimePathExpansionEligibleAtUnix,
		"runtime_applied_path_plan_revision":      status.RuntimeAppliedPathPlanRevision,
		"runtime_applied_path_reported_at_unix":   status.RuntimeAppliedPathReportedAtUnix,
		"runtime_no_path":                         service.RuntimeNoPathSummary(status),
		"controller_needs_attention":              service.ControllerPathPlanNeedsAttention(status),
		"controller_attention_reasons":            service.ControllerPathPlanAttentionReasons(status),
		"controller_recommended_actions":          service.ControllerPathPlanRecommendedActions(status),
		"controller_priority_class":               controllerPriorityClass(status),
		"operator_recommended_actions":            operatorRecommendedActionsFromCluster(status, clusterPriority),
		"cluster_priority_mismatch_actions":       clusterPriorityMismatchActionsFromCluster(status, clusterPriority),
		"cluster_top_priority_class":              clusterPriority.TopClass,
		"cluster_top_priority_count":              clusterPriority.TopCount,
		"cluster_priority_matches_controller":     clusterPriority.Match,
	}, nil
}

func (s *Server) discoveryRuntimeFeedbackResponse(ctx context.Context, volumeID uint64, req runtimePathFeedbackRequest) (map[string]any, error) {
	status, err := s.cfg.MetadataRepo.GetVolumeStatus(ctx, volumeID)
	if err != nil {
		return nil, err
	}
	nowUnix := time.Now().Unix()
	status.RuntimePathNeedsAttention = req.NeedsAttention
	status.RuntimePathAttentionReasons = service.ControllerPathPlanAttentionReasons(service.VolumeStatusRecord{
		RuntimePathAttentionReasons: req.AttentionReasons,
	})
	status.RuntimePathRecommendedActions = service.ControllerPathPlanRecommendedActions(service.VolumeStatusRecord{
		RuntimePathRecommendedActions: req.RecommendedActions,
	})
	status.RuntimeAppliedPathPlanRevision = req.AppliedPathPlanRevision
	status.RuntimeAppliedPathReportedAtUnix = nowUnix
	if sourceHost := strings.TrimSpace(req.SourceHost); sourceHost != "" {
		status.RuntimePathFeedbackSourceHost = sourceHost
	}
	if req.NoPath != nil {
		status.RuntimeNoPathState = req.NoPath.State
		status.RuntimeNoPathRetryMode = req.NoPath.RetryMode
		status.RuntimeNoPathRetrySeconds = req.NoPath.RetrySeconds
		status.RuntimeNoPathQueuedReqs = req.NoPath.QueuedReqs
		status.RuntimeNoPathRequeuedReqs = req.NoPath.RequeuedReqs
		status.RuntimeNoPathFailedReqs = req.NoPath.FailedReqs
		status.RuntimeNoPathRecoveredReqs = req.NoPath.RecoveredReqs
		status.RuntimeNoPathEnterCount = req.NoPath.EnterCount
		status.RuntimeNoPathLastReason = req.NoPath.LastReason
		status.RuntimeNoPathLastFeedbackUnix = nowUnix
	}
	status.ControllerReconcileRequestedAtUnix = nowUnix
	status.ControllerReconcileReason = "runtime_feedback"
	if req.NeedsAttention {
		status.RuntimePathFeedbackCount++
		status.LastRuntimePathFeedbackUnix = nowUnix
		if containsString(status.RuntimePathRecommendedActions, "prefer_fewer_active_paths") && status.RuntimePathFeedbackCount >= runtimePathReductionHoldThreshold {
			holdUntil := nowUnix + int64(runtimePathReductionHoldDuration/time.Second)
			if holdUntil > status.RuntimePathReductionHoldUntilUnix {
				status.RuntimePathReductionHoldUntilUnix = holdUntil
			}
		}
	}
	if err := s.cfg.MetadataRepo.PutVolumeStatus(ctx, status); err != nil {
		return nil, err
	}
	clusterPriority := s.clusterPrioritySnapshot(ctx, status)
	return map[string]any{
		"volume_id":                              service.CanonicalVolumeID(volumeID),
		"runtime_path_needs_attention":           status.RuntimePathNeedsAttention,
		"runtime_path_attention_reasons":         append([]string(nil), status.RuntimePathAttentionReasons...),
		"runtime_path_recommended_actions":       append([]string(nil), status.RuntimePathRecommendedActions...),
		"runtime_path_feedback_count":            status.RuntimePathFeedbackCount,
		"last_runtime_path_feedback_unix":        status.LastRuntimePathFeedbackUnix,
		"runtime_path_feedback_source_host":      status.RuntimePathFeedbackSourceHost,
		"runtime_path_reduction_hold_until_unix": status.RuntimePathReductionHoldUntilUnix,
		"runtime_path_expansion_backoff_level":   status.RuntimePathExpansionBackoffLevel,
		"runtime_applied_path_plan_revision":     status.RuntimeAppliedPathPlanRevision,
		"runtime_applied_path_reported_at_unix":  status.RuntimeAppliedPathReportedAtUnix,
		"runtime_no_path":                        service.RuntimeNoPathSummary(status),
		"path_plan":                              pathPlanObservabilitySummary(status, len(status.ObservedActiveGatewaySet)),
		"controller_reconcile_requested_at_unix": status.ControllerReconcileRequestedAtUnix,
		"controller_reconcile_reason":            status.ControllerReconcileReason,
		"controller_reconcile_scheduled_at_unix": status.ControllerReconcileScheduledAtUnix,
		"controller_reconcile_scheduled_reason":  status.ControllerReconcileScheduledReason,
		"controller_needs_attention":             service.ControllerPathPlanNeedsAttention(status),
		"controller_attention_reasons":           service.ControllerPathPlanAttentionReasons(status),
		"controller_recommended_actions":         service.ControllerPathPlanRecommendedActions(status),
		"controller_priority_class":              controllerPriorityClass(status),
		"operator_recommended_actions":           operatorRecommendedActionsFromCluster(status, clusterPriority),
		"cluster_priority_mismatch_actions":      clusterPriorityMismatchActionsFromCluster(status, clusterPriority),
		"cluster_top_priority_class":             clusterPriority.TopClass,
		"cluster_top_priority_count":             clusterPriority.TopCount,
		"cluster_priority_matches_controller":    clusterPriority.Match,
	}, nil
}

func nonNilDataplaneEndpoints(paths []bridge.DataplaneEndpointSpec) []bridge.DataplaneEndpointSpec {
	if paths == nil {
		return []bridge.DataplaneEndpointSpec{}
	}
	return paths
}

func parseUint32(raw string) (uint32, error) {
	value, err := strconv.ParseUint(strings.TrimSpace(raw), 10, 32)
	if err != nil {
		return 0, err
	}
	return uint32(value), nil
}

func gatewayActiveForDiscovery(state service.GatewayConnectionState) bool {
	return service.GatewayObservedForPathPlan(state)
}

func discoveryPriority(gateway service.GatewayRecord, endpointIndex int) uint32 {
	base := uint32(100)
	if gateway.ConnectionState == service.GatewayStateDegraded {
		base = 50
	}
	if endpointIndex > 0 {
		return base - uint32(endpointIndex)
	}
	return base
}
