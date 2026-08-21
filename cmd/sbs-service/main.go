package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"hash/crc32"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/nosway/namrbd/gateway/sbsgrpc"
	"github.com/nosway/namrbd/gateway/service"
	"github.com/nosway/namrbd/internal/cliux"
	"github.com/nosway/namrbd/internal/depavail"
	"github.com/nosway/namrbd/internal/envcompat"
	"github.com/nosway/namrbd/internal/structuredlog"
	"github.com/nosway/namrbd/internal/tikvopts"
	adminv1 "github.com/nosway/namrbd/sbs/admin/v1"
	sbscluster "github.com/nosway/namrbd/sbs/cluster"
	clustercontrol "github.com/nosway/namrbd/sbs/cluster/control"
	clusterec "github.com/nosway/namrbd/sbs/cluster/ec"
	clustermaintenance "github.com/nosway/namrbd/sbs/cluster/maintenance"
	clustermeta "github.com/nosway/namrbd/sbs/cluster/metadata"
	clusterpayload "github.com/nosway/namrbd/sbs/cluster/payload"
	"github.com/nosway/namrbd/sbs/cluster/placement"
	clusterreplication "github.com/nosway/namrbd/sbs/cluster/replication"
	internalv1 "github.com/nosway/namrbd/sbs/internalapi/v1"
	"github.com/nosway/namrbd/sbs/local"
	sbsv1 "github.com/nosway/namrbd/sbs/v1"
	namrbdversion "github.com/nosway/namrbd/version"
	opsdashboard "github.com/nosway/namrbd/web/operations-dashboard"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	defaultMetadataRoot                                = "sbs/cluster"
	defaultPlacementApplyTimeout                       = 5 * time.Second
	defaultPublishedViewCacheTTL                       = 2 * time.Second
	defaultECMaintenanceScanTTL                        = 5 * time.Minute
	defaultAutoRebalanceMinVolumeAge                   = time.Minute
	defaultAutoRebalanceForegroundWriteSettleAge       = 15 * time.Minute
	defaultServiceOwnedWriteEffects                    = true
	defaultNativeAllocationFastPath                    = true
	defaultServiceRuntimeWriteEffectsBatchCoalesceWait = time.Millisecond

	adminVolumeSummaryModeMetadataKey = "namrbd-volume-summary-mode"
	adminVolumeSummaryModeSpecOnly    = "spec-only"
	ecMaintenanceScanOperationKind    = "ec_maintenance_scan"
)

var buildVersion = namrbdversion.ProductVersion()

type bootstrapRecord struct {
	ClusterID     string `json:"cluster_id"`
	SBSClusterID  string `json:"sbs_cluster_id"`
	CreatedBy     string `json:"created_by,omitempty"`
	CreatedReason string `json:"created_reason,omitempty"`
	CreatedAtUnix int64  `json:"created_at_unix"`
	SchemaVersion uint64 `json:"schema_version"`
	MetadataRoot  string `json:"metadata_root"`
}

type metadataBackend struct {
	name  string
	kv    clustermeta.KV
	repo  *clustermeta.Repository
	close func() error
}

type volumeSpecRecord struct {
	VolumeID                       string                                  `json:"volume_id"`
	SizeBytes                      uint64                                  `json:"size_bytes"`
	BlockSize                      uint32                                  `json:"block_size"`
	ChunkSizeBytes                 uint32                                  `json:"chunk_size_bytes,omitempty"`
	ExtentPageBytes                uint32                                  `json:"extent_page_bytes,omitempty"`
	ExtentSizeBytes                uint64                                  `json:"extent_size_bytes,omitempty"`
	ReplicationFactor              uint32                                  `json:"replication_factor"`
	PolicyName                     string                                  `json:"policy_name,omitempty"`
	TopologyMode                   string                                  `json:"topology_mode,omitempty"`
	RedundancyBackend              string                                  `json:"redundancy_backend,omitempty"`
	ECProfileID                    string                                  `json:"ec_profile_id,omitempty"`
	ECCodecID                      string                                  `json:"ec_codec_id,omitempty"`
	ECDataShards                   uint32                                  `json:"ec_data_shards,omitempty"`
	ECParityShards                 uint32                                  `json:"ec_parity_shards,omitempty"`
	ECStripeUnitBytes              uint32                                  `json:"ec_stripe_unit_bytes,omitempty"`
	ECFailureDomain                string                                  `json:"ec_failure_domain,omitempty"`
	ECMaxUnavailableFailureDomains uint32                                  `json:"ec_max_unavailable_failure_domains,omitempty"`
	ECMaxShardsPerFailureDomain    uint32                                  `json:"ec_max_shards_per_failure_domain,omitempty"`
	WeakPlacementAllowed           bool                                    `json:"weak_placement_allowed,omitempty"`
	CreatedBy                      string                                  `json:"created_by,omitempty"`
	CreatedReason                  string                                  `json:"created_reason,omitempty"`
	CreatedAtUnix                  int64                                   `json:"created_at_unix"`
	ProtectedState                 *clustermeta.VolumeProtectedStateRecord `json:"protected_state,omitempty"`
}

type storedOperation struct {
	OperationID            string `json:"operation_id"`
	Kind                   string `json:"kind"`
	State                  string `json:"state"`
	TargetNodeID           string `json:"target_node_id,omitempty"`
	TargetVolumeID         string `json:"target_volume_id,omitempty"`
	ExtentsRemaining       uint64 `json:"extents_remaining,omitempty"`
	BytesRemaining         uint64 `json:"bytes_remaining,omitempty"`
	Phase                  string `json:"phase,omitempty"`
	BlockingReason         string `json:"blocking_reason,omitempty"`
	StartedAtUnix          int64  `json:"started_at_unix,omitempty"`
	LastProgressUnix       int64  `json:"last_progress_unix,omitempty"`
	ErrorMessage           string `json:"error_message,omitempty"`
	Actor                  string `json:"actor,omitempty"`
	Reason                 string `json:"reason,omitempty"`
	ApprovalID             string `json:"approval_id,omitempty"`
	RiskAcknowledged       bool   `json:"risk_acknowledged,omitempty"`
	FollowOnRepairRequired bool   `json:"follow_on_repair_required,omitempty"`
}

type operationAudit struct {
	Actor                  string
	Reason                 string
	ApprovalID             string
	RiskAcknowledged       bool
	FollowOnRepairRequired bool
}

func operationAuditFromMeta(meta *adminv1.RequestMeta, defaultReason string) operationAudit {
	actor := strings.TrimSpace(meta.GetActor())
	if actor == "" {
		actor = "unknown"
	}
	reason := strings.TrimSpace(meta.GetReason())
	if reason == "" {
		reason = defaultReason
	}
	return operationAudit{Actor: actor, Reason: reason}
}

type operationStore struct {
	mu   sync.RWMutex
	kv   clustermeta.KV
	root string
}

const (
	nodeHealthShardSize        = 25
	nodeHealthShardConcurrency = 16
)

type nodeHealthReconcilerStatus struct {
	ShardCount           int
	QueueDepth           int
	PeakQueueDepth       int
	InFlight             int
	MaxInFlight          int
	ProbeCount           int
	TransitionCount      int
	VolumeReconcileCount int
	FirstError           string
	LastError            string
	LastRunUnix          int64
}

func newReplicaClientCache() *replicaClientCache {
	return &replicaClientCache{clients: make(map[string]cachedReplicaClient)}
}

func newMaintenanceSettings() *maintenanceSettings {
	return &maintenanceSettings{
		generation:              1,
		maxConcurrentRepairs:    1,
		maxConcurrentRebalances: 1,
		maxConcurrentDrains:     1,
		maxConcurrentPayloadGCs: maxInt(getenvInt("NAMRBD_SBS_MAX_CONCURRENT_PAYLOAD_GCS", 1), 1),
		pausePayloadGCs:         getenvBool("NAMRBD_SBS_PAUSE_PAYLOAD_GCS", false),
	}
}

func newOperationStore(kv clustermeta.KV, root string) *operationStore {
	return &operationStore{kv: kv, root: root}
}

func (s *operationStore) create(kind, nodeID, volumeID, phase string, state adminv1.OperationState) (*adminv1.OperationStatus, error) {
	return s.createAudited(kind, nodeID, volumeID, phase, state, operationAudit{})
}

func (s *operationStore) createAudited(kind, nodeID, volumeID, phase string, state adminv1.OperationState, audit operationAudit) (*adminv1.OperationStatus, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	nextSeq := s.nextSequenceLocked(context.Background())
	opID := fmt.Sprintf("op-%06d", nextSeq)
	now := time.Now().UTC().Unix()
	record := storedOperation{
		OperationID:            opID,
		Kind:                   kind,
		State:                  state.String(),
		TargetNodeID:           nodeID,
		TargetVolumeID:         volumeID,
		Phase:                  phase,
		StartedAtUnix:          now,
		LastProgressUnix:       now,
		Actor:                  strings.TrimSpace(audit.Actor),
		Reason:                 strings.TrimSpace(audit.Reason),
		ApprovalID:             strings.TrimSpace(audit.ApprovalID),
		RiskAcknowledged:       audit.RiskAcknowledged,
		FollowOnRepairRequired: audit.FollowOnRepairRequired,
	}
	if err := s.putLocked(context.Background(), record); err != nil {
		return nil, fmt.Errorf("persist operation %s: %w", opID, err)
	}
	return record.toProto(), nil
}

func (s *operationStore) update(opID string, mutate func(*adminv1.OperationStatus)) (*adminv1.OperationStatus, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	record, err := s.getLocked(context.Background(), opID)
	if err != nil {
		return nil, clustermeta.ErrNotFound
	}
	op := record.toProto()
	mutate(op)
	record = storedOperationFromProto(op)
	record.LastProgressUnix = time.Now().UTC().Unix()
	if err := s.putLocked(context.Background(), record); err != nil {
		return nil, err
	}
	return record.toProto(), nil
}

func (s *operationStore) get(opID string) (*adminv1.OperationStatus, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	record, err := s.getLocked(context.Background(), opID)
	if err != nil {
		return nil, err
	}
	return record.toProto(), nil
}

func (s *operationStore) list(kind string, state adminv1.OperationState) []*adminv1.OperationStatus {
	s.mu.RLock()
	defer s.mu.RUnlock()
	keys, err := listKeys(context.Background(), s.kv, operationsPrefix(s.root))
	if err != nil {
		return nil
	}
	out := make([]*adminv1.OperationStatus, 0, len(keys))
	for _, key := range keys {
		record, err := s.getByKeyLocked(context.Background(), key)
		if err != nil {
			continue
		}
		op := record.toProto()
		if kind != "" && op.GetKind() != kind {
			continue
		}
		if state != adminv1.OperationState_OPERATION_STATE_UNSPECIFIED && op.GetState() != state {
			continue
		}
		out = append(out, op)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].GetOperationId() < out[j].GetOperationId() })
	return out
}

func (s *operationStore) nextSequenceLocked(ctx context.Context) uint64 {
	key := operationsSeqKey(s.root)
	raw, found, err := s.kv.Get(ctx, key)
	if err != nil || !found {
		_ = s.kv.Set(ctx, key, []byte("1"))
		return 1
	}
	current, err := strconv.ParseUint(string(raw), 10, 64)
	if err != nil {
		current = 0
	}
	current++
	_ = s.kv.Set(ctx, key, []byte(strconv.FormatUint(current, 10)))
	return current
}

func (s *operationStore) putLocked(ctx context.Context, record storedOperation) error {
	raw, err := json.Marshal(record)
	if err != nil {
		return err
	}
	return s.kv.Set(ctx, operationKey(s.root, record.OperationID), raw)
}

func (s *operationStore) getLocked(ctx context.Context, opID string) (storedOperation, error) {
	return s.getByKeyLocked(ctx, operationKey(s.root, opID))
}

func (s *operationStore) getByKeyLocked(ctx context.Context, key string) (storedOperation, error) {
	raw, found, err := s.kv.Get(ctx, key)
	if err != nil {
		return storedOperation{}, err
	}
	if !found {
		return storedOperation{}, clustermeta.ErrNotFound
	}
	var record storedOperation
	if err := json.Unmarshal(raw, &record); err != nil {
		return storedOperation{}, err
	}
	return record, nil
}

func storedOperationFromProto(op *adminv1.OperationStatus) storedOperation {
	record := storedOperation{
		OperationID:            op.GetOperationId(),
		Kind:                   op.GetKind(),
		State:                  op.GetState().String(),
		TargetNodeID:           op.GetTargetNodeId(),
		TargetVolumeID:         op.GetTargetVolumeId(),
		ExtentsRemaining:       op.GetExtentsRemaining(),
		BytesRemaining:         op.GetBytesRemaining(),
		Phase:                  op.GetPhase(),
		BlockingReason:         op.GetBlockingReason(),
		ErrorMessage:           op.GetErrorMessage(),
		Actor:                  op.GetActor(),
		Reason:                 op.GetReason(),
		ApprovalID:             op.GetApprovalId(),
		RiskAcknowledged:       op.GetRiskAcknowledged(),
		FollowOnRepairRequired: op.GetFollowOnRepairRequired(),
	}
	if ts := op.GetStartedAt(); ts != nil {
		record.StartedAtUnix = ts.AsTime().Unix()
	}
	if ts := op.GetLastProgressAt(); ts != nil {
		record.LastProgressUnix = ts.AsTime().Unix()
	}
	return record
}

func (o storedOperation) toProto() *adminv1.OperationStatus {
	return &adminv1.OperationStatus{
		OperationId:            o.OperationID,
		Kind:                   o.Kind,
		State:                  operationStateFromString(o.State),
		TargetNodeId:           o.TargetNodeID,
		TargetVolumeId:         o.TargetVolumeID,
		ExtentsRemaining:       o.ExtentsRemaining,
		BytesRemaining:         o.BytesRemaining,
		Phase:                  o.Phase,
		BlockingReason:         o.BlockingReason,
		StartedAt:              unixTimestamp(o.StartedAtUnix),
		LastProgressAt:         unixTimestamp(o.LastProgressUnix),
		ErrorMessage:           o.ErrorMessage,
		Actor:                  o.Actor,
		Reason:                 o.Reason,
		ApprovalId:             o.ApprovalID,
		RiskAcknowledged:       o.RiskAcknowledged,
		FollowOnRepairRequired: o.FollowOnRepairRequired,
	}
}

type server struct {
	adminv1.UnimplementedAdminServiceServer
	adminv1.UnimplementedOperationsServiceServer
	internalv1.UnimplementedPlacementApplyServiceServer
	internalv1.UnimplementedWriteSessionServiceServer
	internalv1.UnimplementedECMetadataServiceServer
	internalv1.UnimplementedChunkIDAllocatorServiceServer
	internalv1.UnimplementedPlacementResolverServiceServer

	clusterID                 string
	sbsClusterID              string
	nodeID                    string
	metadataBackendName       string
	metadataRuntimeMode       string
	tikvPDEndpointsConfigured bool
	metadataPathConfigured    bool
	root                      string
	payloadRoot               string
	startedAt                 time.Time

	kv         clustermeta.KV
	repo       *clustermeta.Repository
	ops        *operationStore
	cache      *replicaClientCache
	viewCache  *publishedViewCache
	maint      *maintenanceSettings
	leader     *leaderLeaseManager
	httpClient *http.Client
	ready      atomic.Bool

	placementApplyInternalService         clustercontrol.PlacementApplyInternalService
	writeSessionInternalService           clustercontrol.WriteSessionInternalService
	ecMetadataInternalService             clustercontrol.ECMetadataInternalService
	serviceOwnedWriteEffects              bool
	writeEffectsQueue                     *serviceWriteEffectsQueue
	writeIntentQueue                      *serviceWriteIntentQueue
	writeSessionCommitLocksMu             sync.Mutex
	writeSessionCommitLocks               map[string]*sync.Mutex
	chunkIDAllocatorService               clustercontrol.ChunkIDAllocatorInternalService
	placementResolverService              clustercontrol.PlacementResolverInternalService
	placementApplyTimeout                 time.Duration
	placementApplyObservability           placementApplyObservability
	writeSessionObservability             writeSessionObservability
	chunkIDAllocatorObservability         chunkIDAllocatorObservability
	placementResolverObservability        placementResolverObservability
	now                                   func() time.Time
	maintenanceMu                         sync.Mutex
	ecMaintenanceMu                       sync.Mutex
	budgetLeaseMu                         sync.Mutex
	securityAuditMu                       sync.Mutex
	iscsiMu                               sync.Mutex
	iscsiWriterFenceProjector             func(context.Context, service.ISCSIWriterFence) error
	lastMaintenanceRunByVolume            map[string]int64
	maintenanceVolumeCooldown             time.Duration
	autoRebalanceMinVolumeAge             time.Duration
	autoRebalanceForegroundWriteSettleAge time.Duration
	healthCheckInterval                   time.Duration
	healthCheckTimeout                    time.Duration
	healthMinimumShardCount               int
	healthConcurrencyPerShard             int
	healthSuspectAfter                    uint32
	healthDownAfter                       uint32
	healthRecoverAfter                    uint32
	healthRecoveryCooldown                time.Duration
	healthStatusMu                        sync.RWMutex
	healthStatus                          nodeHealthReconcilerStatus
	probeNodeHealth                       func(ctx context.Context, node clustermeta.NodeMembershipRecord) error
	beforeMaintenanceVolume               func(ctx context.Context, volumeID string)
}

type publishedViewCache struct {
	ttl time.Duration
	mu  sync.Mutex

	volumes    map[string]cachedPublishedVolume
	placements map[string]cachedPublishedPlacement
	targets    map[string]cachedPublishedReplicaTargets
}

type cachedPublishedVolume struct {
	volume    *adminv1.VolumeSummary
	expiresAt time.Time
}

type cachedPublishedPlacement struct {
	response  *adminv1.GetVolumePlacementViewResponse
	expiresAt time.Time
}

type cachedPublishedReplicaTargets struct {
	response  *adminv1.GetReplicaTargetsViewResponse
	expiresAt time.Time
}

func newPublishedViewCache(ttl time.Duration) *publishedViewCache {
	if ttl <= 0 {
		return nil
	}
	return &publishedViewCache{
		ttl:        ttl,
		volumes:    make(map[string]cachedPublishedVolume),
		placements: make(map[string]cachedPublishedPlacement),
		targets:    make(map[string]cachedPublishedReplicaTargets),
	}
}

func cloneVolumeSummary(in *adminv1.VolumeSummary) *adminv1.VolumeSummary {
	if in == nil {
		return nil
	}
	return proto.Clone(in).(*adminv1.VolumeSummary)
}

func cloneVolumePlacementView(in *adminv1.GetVolumePlacementViewResponse) *adminv1.GetVolumePlacementViewResponse {
	if in == nil {
		return nil
	}
	return proto.Clone(in).(*adminv1.GetVolumePlacementViewResponse)
}

func cloneReplicaTargetsView(in *adminv1.GetReplicaTargetsViewResponse) *adminv1.GetReplicaTargetsViewResponse {
	if in == nil {
		return nil
	}
	return proto.Clone(in).(*adminv1.GetReplicaTargetsViewResponse)
}

func (c *publishedViewCache) getVolume(volumeID string) (*adminv1.VolumeSummary, bool) {
	if c == nil {
		return nil, false
	}
	now := time.Now()
	c.mu.Lock()
	defer c.mu.Unlock()
	entry, ok := c.volumes[volumeID]
	if !ok || now.After(entry.expiresAt) {
		delete(c.volumes, volumeID)
		return nil, false
	}
	return cloneVolumeSummary(entry.volume), true
}

func (c *publishedViewCache) storeVolume(volumeID string, volume *adminv1.VolumeSummary) {
	if c == nil || volume == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.volumes[volumeID] = cachedPublishedVolume{
		volume:    cloneVolumeSummary(volume),
		expiresAt: time.Now().Add(c.ttl),
	}
}

func (c *publishedViewCache) getPlacement(volumeID string) (*adminv1.GetVolumePlacementViewResponse, bool) {
	if c == nil {
		return nil, false
	}
	now := time.Now()
	c.mu.Lock()
	defer c.mu.Unlock()
	entry, ok := c.placements[volumeID]
	if !ok || now.After(entry.expiresAt) {
		delete(c.placements, volumeID)
		return nil, false
	}
	return cloneVolumePlacementView(entry.response), true
}

func (c *publishedViewCache) storePlacement(volumeID string, response *adminv1.GetVolumePlacementViewResponse) {
	if c == nil || response == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.placements[volumeID] = cachedPublishedPlacement{
		response:  cloneVolumePlacementView(response),
		expiresAt: time.Now().Add(c.ttl),
	}
}

func (c *publishedViewCache) getReplicaTargets(volumeID string) (*adminv1.GetReplicaTargetsViewResponse, bool) {
	if c == nil {
		return nil, false
	}
	now := time.Now()
	c.mu.Lock()
	defer c.mu.Unlock()
	entry, ok := c.targets[volumeID]
	if !ok || now.After(entry.expiresAt) {
		delete(c.targets, volumeID)
		return nil, false
	}
	return cloneReplicaTargetsView(entry.response), true
}

func (c *publishedViewCache) storeReplicaTargets(volumeID string, response *adminv1.GetReplicaTargetsViewResponse) {
	if c == nil || response == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.targets[volumeID] = cachedPublishedReplicaTargets{
		response:  cloneReplicaTargetsView(response),
		expiresAt: time.Now().Add(c.ttl),
	}
}

func (c *publishedViewCache) invalidateVolume(volumeID string) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.volumes, volumeID)
	delete(c.placements, volumeID)
	delete(c.targets, volumeID)
}

type placementApplyObservability struct {
	mu              sync.Mutex
	requestsTotal   uint64
	durationNanos   int64
	requestsByClass map[string]uint64
}

type placementApplyObservabilitySnapshot struct {
	RequestsTotal        uint64
	FailuresTotal        uint64
	DurationTotalSeconds float64
	RequestsByClass      map[string]uint64
}

func (o *placementApplyObservability) record(class string, duration time.Duration) {
	if class == "" {
		class = "unknown"
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.requestsByClass == nil {
		o.requestsByClass = make(map[string]uint64)
	}
	o.requestsTotal++
	o.durationNanos += duration.Nanoseconds()
	o.requestsByClass[class]++
}

func (o *placementApplyObservability) snapshot() placementApplyObservabilitySnapshot {
	o.mu.Lock()
	defer o.mu.Unlock()
	byClass := make(map[string]uint64, len(o.requestsByClass))
	failures := uint64(0)
	for class, count := range o.requestsByClass {
		byClass[class] = count
		if class != placementApplyOutcomeOK {
			failures += count
		}
	}
	return placementApplyObservabilitySnapshot{
		RequestsTotal:        o.requestsTotal,
		FailuresTotal:        failures,
		DurationTotalSeconds: float64(o.durationNanos) / float64(time.Second),
		RequestsByClass:      byClass,
	}
}

const placementApplyOutcomeOK = "ok"

type writeSessionObservability = placementApplyObservability
type writeSessionObservabilitySnapshot = placementApplyObservabilitySnapshot

const writeSessionOutcomeOK = "ok"

type chunkIDAllocatorObservability = placementApplyObservability
type chunkIDAllocatorObservabilitySnapshot = placementApplyObservabilitySnapshot

const chunkIDAllocatorOutcomeOK = "ok"

type placementResolverObservability = placementApplyObservability
type placementResolverObservabilitySnapshot = placementApplyObservabilitySnapshot

const placementResolverOutcomeOK = "ok"

type replicaClientCache struct {
	mu      sync.Mutex
	clients map[string]cachedReplicaClient
}

type cachedReplicaClient struct {
	conn   *grpc.ClientConn
	client service.SBSClient
}

type nodeStoreHealthSummary struct {
	StoreCount                int
	HealthyStoreCount         int
	WritableStoreCount        int
	AllocatableStoreCount     int
	AllocationWeightTotal     int
	AllocationWeightObserved  bool
	CapacityBytes             uint64
	AvailableBytes            uint64
	UsedBytes                 uint64
	CompactionPendingBytes    uint64
	CompactionInProgressBytes uint64
}

type sbsDataDebugSummary struct {
	Stores []sbsDataStoreSummary `json:"stores"`
}

type sbsDataStoreHealthSummary struct {
	Stores []sbsDataStoreSummary `json:"stores"`
}

type sbsDataStoreSummary struct {
	State                     string `json:"state"`
	Weight                    *int   `json:"weight"`
	AllocationWeight          *int   `json:"allocation_weight"`
	CapacityBytes             uint64 `json:"capacity_bytes"`
	AvailableBytes            uint64 `json:"available_bytes"`
	UsedBytes                 uint64 `json:"used_bytes"`
	CompactionPendingBytes    uint64 `json:"compaction_pending_bytes"`
	CompactionInProgressBytes uint64 `json:"compaction_in_progress_bytes"`
}

type maintenanceSettings struct {
	mu sync.RWMutex

	generation              uint64
	maxConcurrentRepairs    int
	maxConcurrentRebalances int
	maxConcurrentDrains     int
	maxConcurrentPayloadGCs int

	pauseRepairs    bool
	pauseRebalances bool
	pauseDrains     bool
	pausePayloadGCs bool
}

type maintenanceSnapshot struct {
	generation              uint64
	maxConcurrentRepairs    int
	maxConcurrentRebalances int
	maxConcurrentDrains     int
	maxConcurrentPayloadGCs int
	pauseRepairs            bool
	pauseRebalances         bool
	pauseDrains             bool
	pausePayloadGCs         bool
}

type observabilitySnapshot struct {
	KnownNodes                  int
	ActiveNodes                 int
	DrainingNodes               int
	RemovedNodes                int
	HealthyNodes                int
	SuspectNodes                int
	DownNodes                   int
	Volumes                     int
	VolumeHealthy               int
	VolumeDegraded              int
	VolumeBlocked               int
	RepairBacklog               int
	RepairBacklogBytes          uint64
	RepairBacklogChunks         uint64
	RebalanceBacklog            int
	RebalanceBacklogBytes       uint64
	RebalanceBacklogChunks      uint64
	DrainBacklog                int
	DrainBacklogBytes           uint64
	DrainBacklogChunks          uint64
	RetiredPayloadBacklogBytes  uint64
	RetiredPayloadBacklogChunks uint64
	RetiredPayloadFailedBatches uint64
	RetiredPayloadFailedAgeSec  uint64
	TransitionFailedBatches     uint64
	TransitionRecentBatches     uint64
	TransitionSmallBatches      uint64
	TransitionRequeued          uint64
	TransitionRetryPages        uint64
	TransitionRetryWindows      uint64
	TransitionRetryWindowBytes  uint64
	TransitionRetryWindowChunks uint64
	TransitionFailedAgeSec      uint64
	MaintenanceCooldownVolumes  uint64
	MaintenanceCooldownMaxSec   uint64
	NodesWithProbeFailures      uint64
	MaxProbeFailures            uint64
	NodesInRecoveryCooldown     uint64
	MaxRecoveryCooldownSec      uint64
	OperationsTotal             int
	OperationsRunning           int
	OperationsFailed            int
	OperationsCompleted         int
	OperationsCanceled          int
	LocalIsLeader               bool
	LeaderState                 string
	LeaseExpiresAtUnix          int64
}

func metadataRuntimeMode(backendName string) string {
	switch strings.TrimSpace(backendName) {
	case "tikv":
		return "primary-tikv"
	case "", "pebble":
		return "legacy-dev-pebble"
	default:
		return "custom-" + strings.TrimSpace(backendName)
	}
}

func (s *server) effectiveMetadataBackendName() string {
	if strings.TrimSpace(s.metadataBackendName) == "" {
		return "pebble"
	}
	return strings.TrimSpace(s.metadataBackendName)
}

func (s *server) effectiveMetadataRuntimeMode() string {
	if strings.TrimSpace(s.metadataRuntimeMode) != "" {
		return strings.TrimSpace(s.metadataRuntimeMode)
	}
	return metadataRuntimeMode(s.effectiveMetadataBackendName())
}

type volumeTransitionBacklog struct {
	RepairCount        uint64
	RepairBytes        uint64
	RepairChunks       uint64
	RebalanceCount     uint64
	RebalanceBytes     uint64
	RebalanceChunks    uint64
	DrainCount         uint64
	DrainBytes         uint64
	DrainChunks        uint64
	FailedBatches      uint64
	RecentBatches      uint64
	SmallBatches       uint64
	RequeuedCount      uint64
	RetryPages         uint64
	RetryWindows       uint64
	RetryWindowBytes   uint64
	RetryWindowChunks  uint64
	OldestFailedAgeSec uint64
}

type retiredPayloadBacklog struct {
	Bytes              uint64
	Chunks             uint64
	FailedBatches      uint64
	OldestFailedAgeSec uint64
}

type failedTransitionBatchBacklog struct {
	FailedBatches      uint64
	OldestFailedAgeSec uint64
}

func (s *server) GetClusterStatus(ctx context.Context, req *adminv1.GetClusterStatusRequest) (*adminv1.GetClusterStatusResponse, error) {
	cluster, _ := s.clusterRef(req.GetCluster())
	nodes, err := s.repo.ListNodeMemberships(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "list node memberships: %v", err)
	}
	activeNodes := uint32(0)
	drainingNodes := uint32(0)
	for _, node := range nodes {
		switch node.LifecycleState {
		case clustermeta.NodeLifecycleActive:
			activeNodes++
		case clustermeta.NodeLifecycleDraining:
			drainingNodes++
		}
	}

	var volumes []clustermeta.VolumeState
	var snapshot observabilitySnapshot
	var degradedExtents uint64
	detailTimeout := s.clusterStatusDetailTimeout()
	if detailTimeout > 0 {
		detailCtx, cancelDetails := context.WithTimeout(ctx, detailTimeout)
		defer cancelDetails()

		volumes, err = s.repo.ListVolumeStates(detailCtx)
		if err != nil {
			log.Printf("sbs-service cluster status volume scan skipped: %v", err)
			volumes = nil
		}
		snapshot, _ = s.observabilitySnapshot(detailCtx)

		for _, volume := range volumes {
			if err := detailCtx.Err(); err != nil {
				log.Printf("sbs-service cluster status degraded extent scan truncated: %v", err)
				break
			}
			if volume.Status == clustermeta.VolumeStatusDegraded || volume.Status == clustermeta.VolumeStatusRepairing || volume.Status == clustermeta.VolumeStatusRebalancing || volume.Status == clustermeta.VolumeStatusBlocked {
				mappings, err := s.repo.ListExtentMappings(detailCtx, volume.VolumeID)
				if err == nil {
					degradedExtents += uint64(len(mappings))
				}
			}
		}
	}

	leaderNodeID := s.nodeID
	if s.leader != nil {
		if record, err := s.leader.CurrentLeader(ctx); err == nil && record.NodeID != "" {
			leaderNodeID = record.NodeID
		}
	}
	healthStatus := s.nodeHealthStatusSnapshot()

	return &adminv1.GetClusterStatusResponse{
		Cluster:                     cluster,
		LeaderNodeId:                leaderNodeID,
		QuorumHealth:                adminv1.QuorumHealth_QUORUM_HEALTH_HEALTHY,
		ActiveNodes:                 activeNodes,
		DrainingNodes:               drainingNodes,
		DegradedExtents:             degradedExtents,
		RepairBacklog:               uint64(snapshot.RepairBacklog),
		RebalanceBacklog:            uint64(snapshot.RebalanceBacklog),
		RepairBacklogBytes:          snapshot.RepairBacklogBytes,
		RepairBacklogChunks:         snapshot.RepairBacklogChunks,
		RebalanceBacklogBytes:       snapshot.RebalanceBacklogBytes,
		RebalanceBacklogChunks:      snapshot.RebalanceBacklogChunks,
		DrainBacklog:                uint64(snapshot.DrainBacklog),
		DrainBacklogBytes:           snapshot.DrainBacklogBytes,
		DrainBacklogChunks:          snapshot.DrainBacklogChunks,
		RetiredPayloadBacklogBytes:  snapshot.RetiredPayloadBacklogBytes,
		RetiredPayloadBacklogChunks: snapshot.RetiredPayloadBacklogChunks,
		RetiredPayloadFailedBatches: snapshot.RetiredPayloadFailedBatches,
		RetiredPayloadOldestFailedBatchAgeSeconds: snapshot.RetiredPayloadFailedAgeSec,
		TransitionFailedBatches:                   snapshot.TransitionFailedBatches,
		TransitionOldestFailedBatchAgeSeconds:     snapshot.TransitionFailedAgeSec,
		TransitionRecentBatches:                   snapshot.TransitionRecentBatches,
		TransitionSmallBatches:                    snapshot.TransitionSmallBatches,
		TransitionRequeued:                        snapshot.TransitionRequeued,
		TransitionRetryPages:                      snapshot.TransitionRetryPages,
		TransitionRetryWindows:                    snapshot.TransitionRetryWindows,
		TransitionRetryWindowBytes:                snapshot.TransitionRetryWindowBytes,
		TransitionRetryWindowChunks:               snapshot.TransitionRetryWindowChunks,
		MaintenanceCooldownVolumes:                snapshot.MaintenanceCooldownVolumes,
		MaintenanceCooldownMaxRemainingSeconds:    snapshot.MaintenanceCooldownMaxSec,
		HealthProbeSharded:                        true,
		HealthProbeShardCount:                     uint32(healthStatus.ShardCount),
		HealthProbeQueueDepth:                     uint32(healthStatus.QueueDepth),
		HealthProbePeakQueueDepth:                 uint32(healthStatus.PeakQueueDepth),
		HealthProbeMaxConcurrency:                 uint32(healthStatus.MaxInFlight),
		HealthProbeIntervalSeconds:                uint32(s.nodeHealthCheckInterval().Seconds()),
		HealthProbeTimeoutSeconds:                 uint32(s.nodeHealthCheckTimeout().Seconds()),
		HealthProbeSuspectAfter:                   s.nodeHealthSuspectAfter(),
		HealthProbeDownAfter:                      s.nodeHealthDownAfter(),
		HealthProbeRecoveryCooldownSeconds:        uint32(s.nodeHealthRecoveryCooldown().Seconds()),
		HealthProbeFirstError:                     healthStatus.FirstError,
		HealthProbeLastError:                      healthStatus.LastError,
		HealthProbeCount:                          uint64(healthStatus.ProbeCount),
		HealthTransitionCount:                     uint64(healthStatus.TransitionCount),
		HealthVolumeReconcileCount:                uint64(healthStatus.VolumeReconcileCount),
	}, nil
}

func (s *server) clusterStatusDetailTimeout() time.Duration {
	return getenvDuration("NAMRBD_CLUSTER_STATUS_DETAIL_TIMEOUT", 5*time.Second)
}

func (s *server) observabilitySnapshotTimeout() time.Duration {
	return getenvDuration("NAMRBD_OBSERVABILITY_SNAPSHOT_TIMEOUT", s.clusterStatusDetailTimeout())
}

func (s *server) boundedObservabilitySnapshot() (observabilitySnapshot, string) {
	timeout := s.observabilitySnapshotTimeout()
	if timeout <= 0 {
		return observabilitySnapshot{}, ""
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return s.observabilitySnapshot(ctx)
}

func (s *server) GetLeader(ctx context.Context, req *adminv1.GetLeaderRequest) (*adminv1.GetLeaderResponse, error) {
	cluster, _ := s.clusterRef(req.GetCluster())
	leaderNodeID := s.nodeID
	if s.leader != nil {
		if record, err := s.leader.CurrentLeader(ctx); err == nil && record.NodeID != "" {
			leaderNodeID = record.NodeID
		}
	}
	return &adminv1.GetLeaderResponse{
		Cluster:       cluster,
		LeaderNodeId:  leaderNodeID,
		LocalIsLeader: s.leader == nil || s.leader.IsLeader(),
	}, nil
}

func (s *server) ListNodes(ctx context.Context, req *adminv1.ListNodesRequest) (*adminv1.ListNodesResponse, error) {
	cluster, err := s.clusterRef(req.GetCluster())
	if err != nil {
		return nil, err
	}
	page, err := s.repo.ListMembershipProjectionPage(ctx, req.GetPageToken(), int(req.GetPageSize()), req.GetIncludeTombstones())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "list node membership projection: %v", err)
	}
	s.observeMembershipProjection(page.Status)
	if page.Status.Stale {
		return nil, status.Errorf(codes.FailedPrecondition, "SBS membership projection is %s: authority revision=%d projection revision=%d lag=%dms", page.Status.ProjectionHealth, page.Status.MembershipRevision, page.Status.MembershipProjectionRevision, page.Status.ProjectionLagMS)
	}
	resp := &adminv1.ListNodesResponse{
		Cluster:                      cluster,
		MembershipRevision:           page.Status.MembershipRevision,
		MembershipProjectionRevision: page.Status.MembershipProjectionRevision,
		ProjectionLagMs:              page.Status.ProjectionLagMS,
		ProjectionHealth:             page.Status.ProjectionHealth,
		ProjectionStale:              page.Status.Stale,
		NextPageToken:                page.NextCursor,
		ProjectionRebuildCount:       page.Status.ProjectionRebuildCount,
		ProjectionResyncCount:        page.Status.ProjectionResyncCount,
	}
	for _, node := range page.Records {
		resp.Nodes = append(resp.Nodes, s.nodeToProto(ctx, node))
	}
	return resp, nil
}

func (s *server) GetMembershipProjectionStatus(ctx context.Context, req *adminv1.GetMembershipProjectionStatusRequest) (*adminv1.GetMembershipProjectionStatusResponse, error) {
	cluster, err := s.clusterRef(req.GetCluster())
	if err != nil {
		return nil, err
	}
	projection, err := s.repo.GetMembershipProjectionStatus(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "get node membership projection status: %v", err)
	}
	s.observeMembershipProjection(projection)
	return &adminv1.GetMembershipProjectionStatusResponse{
		Cluster: cluster,
		Status:  membershipProjectionStatusToProto(projection),
	}, nil
}

func (s *server) RebuildMembershipProjection(ctx context.Context, req *adminv1.RebuildMembershipProjectionRequest) (*adminv1.RebuildMembershipProjectionResponse, error) {
	if err := s.requireLeader(ctx); err != nil {
		return nil, status.Error(codes.Unavailable, err.Error())
	}
	cluster, err := s.clusterRef(req.GetCluster())
	if err != nil {
		return nil, err
	}
	if err := enforceDependencyMembershipChange(); err != nil {
		return nil, err
	}
	projection, err := s.repo.RebuildMembershipProjection(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "rebuild node membership projection: %v", err)
	}
	s.observeMembershipProjection(projection)
	return &adminv1.RebuildMembershipProjectionResponse{
		Cluster: cluster,
		Status:  membershipProjectionStatusToProto(projection),
	}, nil
}

func (s *server) observeMembershipProjection(projection clustermeta.MembershipProjectionStatus) {
	if dependencyTracker == nil {
		return
	}
	dependencyTracker.SetProjectionLag(time.Duration(projection.ProjectionLagMS) * time.Millisecond)
	dependencyTracker.Refresh()
}

func (s *server) GetNode(ctx context.Context, req *adminv1.GetNodeRequest) (*adminv1.GetNodeResponse, error) {
	cluster, _ := s.clusterRef(req.GetCluster())
	rec, err := s.repo.GetNodeMembership(ctx, req.GetNodeId())
	if err != nil {
		if errors.Is(err, clustermeta.ErrNotFound) {
			return nil, status.Errorf(codes.NotFound, "node %q not found", req.GetNodeId())
		}
		return nil, status.Errorf(codes.Internal, "get node membership: %v", err)
	}
	return &adminv1.GetNodeResponse{
		Cluster: cluster,
		Node:    s.nodeToProto(ctx, rec),
	}, nil
}

func (s *server) JoinNode(ctx context.Context, req *adminv1.JoinNodeRequest) (*adminv1.JoinNodeResponse, error) {
	if err := s.requireLeader(ctx); err != nil {
		return nil, status.Error(codes.Unavailable, err.Error())
	}
	cluster, err := s.clusterRef(req.GetCluster())
	if err != nil {
		return nil, err
	}
	if req.GetNodeId() == "" || req.GetGrpcEndpoint() == "" {
		return nil, status.Error(codes.InvalidArgument, "node_id and grpc_endpoint are required")
	}
	zoneID := strings.TrimSpace(req.GetZone())
	if err := enforceDependencyMembershipChange(); err != nil {
		return nil, err
	}
	if err := s.ensureJoinZoneAllowed(ctx, zoneID, req.GetAutoCreateZone()); err != nil {
		return nil, err
	}

	audit := operationAuditFromMeta(req.GetMeta(), "join")
	op, err := s.ops.createAudited("node.join", req.GetNodeId(), "", "validating", adminv1.OperationState_OPERATION_STATE_RUNNING, audit)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "create operation: %v", err)
	}

	lifecycle := clustermeta.NodeLifecycleActive
	expectedGeneration := uint64(0)
	rec := clustermeta.NodeMembershipRecord{}
	if existing, err := s.repo.GetNodeMembership(ctx, req.GetNodeId()); err == nil {
		rec = existing
		expectedGeneration = existing.Generation
		if existing.LifecycleState == clustermeta.NodeLifecycleDraining {
			lifecycle = clustermeta.NodeLifecycleDraining
		}
	} else if !errors.Is(err, clustermeta.ErrNotFound) {
		s.failOperation(op.GetOperationId(), err)
		return nil, status.Errorf(codes.Internal, "get existing node membership: %v", err)
	}

	rec.ClusterID = cluster.GetClusterId()
	rec.SBSClusterID = cluster.GetSbsClusterId()
	rec.NodeID = req.GetNodeId()
	rec.LifecycleState = lifecycle
	rec.HealthState = clustermeta.NodeHealthHealthy
	rec.DesiredState = string(lifecycle)
	rec.ObservedState = string(clustermeta.NodeHealthHealthy)
	rec.Zone = zoneID
	rec.Roles = []string{"sbs-data"}
	rec.Capabilities = []string{"sbs-grpc", "admin-http"}
	rec.LastHeartbeatUnix = time.Now().Unix()
	rec.AdminHTTPEndpoint = req.GetAdminHttpEndpoint()
	rec.SBSEndpoints = []clustermeta.SBSEndpoint{parseEndpoint(req.GetGrpcEndpoint())}
	rec.Tombstone = false
	rec.UpdatedBy = audit.Actor
	rec.UpdateReason = audit.Reason
	if _, _, err := s.repo.CompareAndSetNodeMembership(ctx, rec, expectedGeneration); err != nil {
		s.failOperation(op.GetOperationId(), err)
		if errors.Is(err, clustermeta.ErrCASConflict) {
			return nil, status.Errorf(codes.Aborted, "node membership changed concurrently: %v", err)
		}
		return nil, status.Errorf(codes.Internal, "put node membership: %v", err)
	}
	op, _ = s.ops.update(op.GetOperationId(), func(op *adminv1.OperationStatus) {
		op.State = adminv1.OperationState_OPERATION_STATE_COMPLETED
		op.Phase = string(lifecycle)
	})
	message := "node joined and activated"
	if lifecycle == clustermeta.NodeLifecycleDraining {
		message = "node joined and preserved draining lifecycle"
	}
	return &adminv1.JoinNodeResponse{
		Cluster: cluster,
		Operation: &adminv1.OperationHandle{
			Accepted:    true,
			OperationId: op.GetOperationId(),
			Message:     message,
		},
	}, nil
}

func (s *server) UpdateNodeTopology(ctx context.Context, req *adminv1.UpdateNodeTopologyRequest) (*adminv1.UpdateNodeTopologyResponse, error) {
	if err := s.requireLeader(ctx); err != nil {
		return nil, status.Error(codes.Unavailable, err.Error())
	}
	cluster, err := s.clusterRef(req.GetCluster())
	if err != nil {
		return nil, err
	}
	nodeID := strings.TrimSpace(req.GetNodeId())
	zoneID := strings.TrimSpace(req.GetZone())
	if nodeID == "" || zoneID == "" {
		return nil, status.Error(codes.InvalidArgument, "node_id and zone are required")
	}
	if err := enforceDependencyMembershipChange(); err != nil {
		return nil, err
	}
	if err := s.ensureJoinZoneAllowed(ctx, zoneID, req.GetAutoCreateZone()); err != nil {
		return nil, err
	}
	node, err := s.repo.GetNodeMembership(ctx, nodeID)
	if err != nil {
		if errors.Is(err, clustermeta.ErrNotFound) {
			return nil, status.Errorf(codes.NotFound, "node %q not found", nodeID)
		}
		return nil, status.Errorf(codes.Internal, "get node membership: %v", err)
	}
	if strings.TrimSpace(node.Zone) != zoneID {
		if active, err := s.nodeHasActivePlacements(ctx, nodeID); err != nil {
			return nil, status.Errorf(codes.Internal, "check node placements: %v", err)
		} else if active {
			return nil, status.Errorf(codes.FailedPrecondition, "node %q has active placements; drain or migrate placements before changing zone", nodeID)
		}
	}
	audit := operationAuditFromMeta(req.GetMeta(), "update topology")
	op, err := s.ops.createAudited("node.update-topology", nodeID, "", "updating", adminv1.OperationState_OPERATION_STATE_RUNNING, audit)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "create operation: %v", err)
	}
	node.Zone = zoneID
	node.UpdatedBy = audit.Actor
	node.UpdateReason = audit.Reason
	updated, _, err := s.repo.CompareAndSetNodeMembership(ctx, node, node.Generation)
	if err != nil {
		s.failOperation(op.GetOperationId(), err)
		if errors.Is(err, clustermeta.ErrCASConflict) {
			return nil, status.Errorf(codes.Aborted, "node membership changed concurrently: %v", err)
		}
		return nil, status.Errorf(codes.Internal, "put node membership: %v", err)
	}
	node = updated
	op, _ = s.ops.update(op.GetOperationId(), func(op *adminv1.OperationStatus) {
		op.State = adminv1.OperationState_OPERATION_STATE_COMPLETED
		op.Phase = "updated"
	})
	return &adminv1.UpdateNodeTopologyResponse{
		Cluster:   cluster,
		Operation: acceptedOperation(op, "node topology updated"),
		Node:      s.nodeToProto(ctx, node),
	}, nil
}

func (s *server) UpdateNodeRegistration(ctx context.Context, req *adminv1.UpdateNodeRegistrationRequest) (*adminv1.UpdateNodeRegistrationResponse, error) {
	if err := s.requireLeader(ctx); err != nil {
		return nil, status.Error(codes.Unavailable, err.Error())
	}
	cluster, err := s.clusterRef(req.GetCluster())
	if err != nil {
		return nil, err
	}
	nodeID := strings.TrimSpace(req.GetNodeId())
	if nodeID == "" {
		return nil, status.Error(codes.InvalidArgument, "node_id is required")
	}
	if req.GrpcEndpoint == nil && req.AdminHttpEndpoint == nil && len(req.GetStoreIds()) == 0 && len(req.GetRoles()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "at least one endpoint, store_id, or role update is required")
	}
	if req.GrpcEndpoint != nil && strings.TrimSpace(req.GetGrpcEndpoint()) == "" {
		return nil, status.Error(codes.InvalidArgument, "grpc_endpoint cannot be empty")
	}
	if err := enforceDependencyMembershipChange(); err != nil {
		return nil, err
	}
	rec, err := s.repo.GetNodeMembership(ctx, nodeID)
	if err != nil {
		if errors.Is(err, clustermeta.ErrNotFound) {
			return nil, status.Errorf(codes.NotFound, "node %q not found", nodeID)
		}
		return nil, status.Errorf(codes.Internal, "get node membership: %v", err)
	}
	audit := operationAuditFromMeta(req.GetMeta(), "update node registration")
	op, err := s.ops.createAudited("node.update-registration", nodeID, "", "updating", adminv1.OperationState_OPERATION_STATE_RUNNING, audit)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "create operation: %v", err)
	}
	if req.GrpcEndpoint != nil {
		endpoint := strings.TrimSpace(req.GetGrpcEndpoint())
		rec.SBSEndpoints = []clustermeta.SBSEndpoint{parseEndpoint(endpoint)}
	}
	if req.AdminHttpEndpoint != nil {
		rec.AdminHTTPEndpoint = strings.TrimSpace(req.GetAdminHttpEndpoint())
	}
	if len(req.GetStoreIds()) > 0 {
		rec.StoreIDs = normalizeMembershipStrings(req.GetStoreIds())
	}
	if len(req.GetRoles()) > 0 {
		rec.Roles = normalizeMembershipStrings(req.GetRoles())
	}
	rec.UpdatedBy = audit.Actor
	rec.UpdateReason = audit.Reason
	updated, _, err := s.repo.CompareAndSetNodeMembership(ctx, rec, rec.Generation)
	if err != nil {
		s.failOperation(op.GetOperationId(), err)
		if errors.Is(err, clustermeta.ErrCASConflict) {
			return nil, status.Errorf(codes.Aborted, "node membership changed concurrently: %v", err)
		}
		return nil, status.Errorf(codes.Internal, "put node membership: %v", err)
	}
	op, _ = s.ops.update(op.GetOperationId(), func(op *adminv1.OperationStatus) {
		op.State = adminv1.OperationState_OPERATION_STATE_COMPLETED
		op.Phase = "updated"
	})
	return &adminv1.UpdateNodeRegistrationResponse{
		Cluster: cluster, Operation: acceptedOperation(op, "node registration updated"), Node: s.nodeToProto(ctx, updated),
	}, nil
}

func normalizeMembershipStrings(values []string) []string {
	seen := make(map[string]struct{}, len(values))
	out := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	sort.Strings(out)
	return out
}

func (s *server) CreateTopologyZone(ctx context.Context, req *adminv1.CreateTopologyZoneRequest) (*adminv1.CreateTopologyZoneResponse, error) {
	if err := s.requireLeader(ctx); err != nil {
		return nil, status.Error(codes.Unavailable, err.Error())
	}
	cluster, err := s.clusterRef(req.GetCluster())
	if err != nil {
		return nil, err
	}
	zoneID := strings.TrimSpace(req.GetZoneId())
	if zoneID == "" {
		return nil, status.Error(codes.InvalidArgument, "zone_id is required")
	}
	if err := enforceDependencyMembershipChange(); err != nil {
		return nil, err
	}
	op, err := s.ops.create("topology.zone.create", zoneID, "", "creating", adminv1.OperationState_OPERATION_STATE_RUNNING)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "create operation: %v", err)
	}
	now := s.currentTime().Unix()
	rec := clustermeta.TopologyZoneRecord{
		ZoneID:        zoneID,
		DisplayName:   strings.TrimSpace(req.GetDisplayName()),
		Lifecycle:     clustermeta.TopologyZoneLifecycleActive,
		Labels:        copyStringMap(req.GetLabels()),
		CreatedAtUnix: now,
		UpdatedAtUnix: now,
	}
	if err := s.repo.PutTopologyZone(ctx, rec); err != nil {
		s.failOperation(op.GetOperationId(), err)
		return nil, status.Errorf(codes.Internal, "put topology zone: %v", err)
	}
	op, _ = s.ops.update(op.GetOperationId(), func(op *adminv1.OperationStatus) {
		op.State = adminv1.OperationState_OPERATION_STATE_COMPLETED
		op.Phase = "active"
	})
	return &adminv1.CreateTopologyZoneResponse{
		Cluster:   cluster,
		Operation: acceptedOperation(op, "topology zone created"),
		Zone:      topologyZoneToProto(rec),
	}, nil
}

func (s *server) ListTopologyZones(ctx context.Context, req *adminv1.ListTopologyZonesRequest) (*adminv1.ListTopologyZonesResponse, error) {
	cluster, _ := s.clusterRef(req.GetCluster())
	zones, err := s.repo.ListTopologyZones(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "list topology zones: %v", err)
	}
	resp := &adminv1.ListTopologyZonesResponse{Cluster: cluster}
	for _, zone := range zones {
		resp.Zones = append(resp.Zones, topologyZoneToProto(zone))
	}
	return resp, nil
}

func (s *server) GetTopologyZone(ctx context.Context, req *adminv1.GetTopologyZoneRequest) (*adminv1.GetTopologyZoneResponse, error) {
	cluster, _ := s.clusterRef(req.GetCluster())
	zoneID := strings.TrimSpace(req.GetZoneId())
	if zoneID == "" {
		return nil, status.Error(codes.InvalidArgument, "zone_id is required")
	}
	rec, err := s.repo.GetTopologyZone(ctx, zoneID)
	if err != nil {
		if errors.Is(err, clustermeta.ErrNotFound) {
			return nil, status.Errorf(codes.NotFound, "topology zone %q not found", zoneID)
		}
		return nil, status.Errorf(codes.Internal, "get topology zone: %v", err)
	}
	return &adminv1.GetTopologyZoneResponse{Cluster: cluster, Zone: topologyZoneToProto(rec)}, nil
}

func (s *server) UpdateTopologyZone(ctx context.Context, req *adminv1.UpdateTopologyZoneRequest) (*adminv1.UpdateTopologyZoneResponse, error) {
	if err := s.requireLeader(ctx); err != nil {
		return nil, status.Error(codes.Unavailable, err.Error())
	}
	cluster, err := s.clusterRef(req.GetCluster())
	if err != nil {
		return nil, err
	}
	zoneID := strings.TrimSpace(req.GetZoneId())
	if zoneID == "" {
		return nil, status.Error(codes.InvalidArgument, "zone_id is required")
	}
	if err := enforceDependencyMembershipChange(); err != nil {
		return nil, err
	}
	rec, err := s.repo.GetTopologyZone(ctx, zoneID)
	if err != nil {
		if errors.Is(err, clustermeta.ErrNotFound) {
			return nil, status.Errorf(codes.NotFound, "topology zone %q not found", zoneID)
		}
		return nil, status.Errorf(codes.Internal, "get topology zone: %v", err)
	}
	op, err := s.ops.create("topology.zone.update", zoneID, "", "updating", adminv1.OperationState_OPERATION_STATE_RUNNING)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "create operation: %v", err)
	}
	if req.DisplayName != nil {
		rec.DisplayName = strings.TrimSpace(req.GetDisplayName())
	}
	if req.Lifecycle != nil {
		lifecycle, err := topologyZoneLifecycleFromProto(req.GetLifecycle())
		if err != nil {
			s.failOperation(op.GetOperationId(), err)
			return nil, status.Error(codes.InvalidArgument, err.Error())
		}
		rec.Lifecycle = lifecycle
	}
	if labels := req.GetLabels(); len(labels) > 0 {
		if rec.Labels == nil {
			rec.Labels = map[string]string{}
		}
		for k, v := range labels {
			rec.Labels[strings.TrimSpace(k)] = strings.TrimSpace(v)
		}
	}
	rec.UpdatedAtUnix = s.currentTime().Unix()
	if err := s.repo.PutTopologyZone(ctx, rec); err != nil {
		s.failOperation(op.GetOperationId(), err)
		return nil, status.Errorf(codes.Internal, "put topology zone: %v", err)
	}
	op, _ = s.ops.update(op.GetOperationId(), func(op *adminv1.OperationStatus) {
		op.State = adminv1.OperationState_OPERATION_STATE_COMPLETED
		op.Phase = string(rec.Lifecycle)
	})
	return &adminv1.UpdateTopologyZoneResponse{
		Cluster:   cluster,
		Operation: acceptedOperation(op, "topology zone updated"),
		Zone:      topologyZoneToProto(rec),
	}, nil
}

func (s *server) DeleteTopologyZone(ctx context.Context, req *adminv1.DeleteTopologyZoneRequest) (*adminv1.DeleteTopologyZoneResponse, error) {
	if err := s.requireLeader(ctx); err != nil {
		return nil, status.Error(codes.Unavailable, err.Error())
	}
	cluster, err := s.clusterRef(req.GetCluster())
	if err != nil {
		return nil, err
	}
	zoneID := strings.TrimSpace(req.GetZoneId())
	if zoneID == "" {
		return nil, status.Error(codes.InvalidArgument, "zone_id is required")
	}
	if err := enforceDependencyMembershipChange(); err != nil {
		return nil, err
	}
	if _, err := s.repo.GetTopologyZone(ctx, zoneID); err != nil {
		if errors.Is(err, clustermeta.ErrNotFound) {
			return nil, status.Errorf(codes.NotFound, "topology zone %q not found", zoneID)
		}
		return nil, status.Errorf(codes.Internal, "get topology zone: %v", err)
	}
	nodes, err := s.repo.ListNodeMemberships(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "list node memberships: %v", err)
	}
	for _, node := range nodes {
		if strings.TrimSpace(node.Zone) == zoneID && node.LifecycleState != clustermeta.NodeLifecycleRemoved {
			return nil, status.Errorf(codes.FailedPrecondition, "topology zone %q is still referenced by node %q", zoneID, node.NodeID)
		}
	}
	op, err := s.ops.create("topology.zone.delete", zoneID, "", "deleting", adminv1.OperationState_OPERATION_STATE_RUNNING)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "create operation: %v", err)
	}
	if err := s.repo.DeleteTopologyZone(ctx, zoneID); err != nil {
		s.failOperation(op.GetOperationId(), err)
		return nil, status.Errorf(codes.Internal, "delete topology zone: %v", err)
	}
	op, _ = s.ops.update(op.GetOperationId(), func(op *adminv1.OperationStatus) {
		op.State = adminv1.OperationState_OPERATION_STATE_COMPLETED
		op.Phase = "deleted"
	})
	return &adminv1.DeleteTopologyZoneResponse{
		Cluster:   cluster,
		Operation: acceptedOperation(op, "topology zone deleted"),
	}, nil
}

func (s *server) ensureJoinZoneAllowed(ctx context.Context, zoneID string, autoCreate bool) error {
	if zoneID == "" {
		return nil
	}
	zone, err := s.repo.GetTopologyZone(ctx, zoneID)
	if err == nil {
		if zone.Lifecycle == clustermeta.TopologyZoneLifecycleRetiring {
			return status.Errorf(codes.FailedPrecondition, "topology zone %q is retiring", zoneID)
		}
		return nil
	}
	if !errors.Is(err, clustermeta.ErrNotFound) {
		return status.Errorf(codes.Internal, "get topology zone: %v", err)
	}
	zones, listErr := s.repo.ListTopologyZones(ctx)
	if listErr != nil {
		return status.Errorf(codes.Internal, "list topology zones: %v", listErr)
	}
	if autoCreate {
		now := s.currentTime().Unix()
		return s.repo.PutTopologyZone(ctx, clustermeta.TopologyZoneRecord{
			ZoneID:        zoneID,
			Lifecycle:     clustermeta.TopologyZoneLifecycleActive,
			CreatedAtUnix: now,
			UpdatedAtUnix: now,
		})
	}
	if len(zones) == 0 {
		return nil
	}
	return status.Errorf(codes.FailedPrecondition, "topology zone %q is not declared", zoneID)
}

func (s *server) nodeHasActivePlacements(ctx context.Context, nodeID string) (bool, error) {
	volumes, err := s.repo.ListVolumeStates(ctx)
	if err != nil {
		return false, err
	}
	for _, volume := range volumes {
		mappings, err := s.repo.ListExtentMappings(ctx, volume.VolumeID)
		if err != nil {
			return false, err
		}
		replicaSets, err := s.repo.ListReplicaSets(ctx, volume.VolumeID)
		if err != nil {
			return false, err
		}
		byPlacement := make(map[string]clustermeta.ReplicaSetState, len(replicaSets))
		for _, rs := range replicaSets {
			byPlacement[rs.PlacementRef] = rs
		}
		for _, mapping := range mappings {
			rs, ok := byPlacement[mapping.PlacementRef]
			if !ok {
				continue
			}
			if replicaSetContainsNode(rs, nodeID) {
				return true, nil
			}
		}
	}
	return false, nil
}

func (s *server) DrainNode(ctx context.Context, req *adminv1.DrainNodeRequest) (*adminv1.DrainNodeResponse, error) {
	if err := s.requireLeader(ctx); err != nil {
		return nil, status.Error(codes.Unavailable, err.Error())
	}
	cluster, err := s.clusterRef(req.GetCluster())
	if err != nil {
		return nil, err
	}
	op, err := s.beginNodeDrain(ctx, req.GetNodeId(), "node.drain", operationAuditFromMeta(req.GetMeta(), "drain"))
	if err != nil {
		return nil, err
	}

	return &adminv1.DrainNodeResponse{
		Cluster: cluster,
		Operation: &adminv1.OperationHandle{
			Accepted:    true,
			OperationId: op.GetOperationId(),
			Message:     "node marked draining",
		},
	}, nil
}

func (s *server) LeaveNode(ctx context.Context, req *adminv1.LeaveNodeRequest) (*adminv1.LeaveNodeResponse, error) {
	if err := s.requireLeader(ctx); err != nil {
		return nil, status.Error(codes.Unavailable, err.Error())
	}
	cluster, err := s.clusterRef(req.GetCluster())
	if err != nil {
		return nil, err
	}
	op, err := s.beginNodeDrain(ctx, req.GetNodeId(), "node.leave", operationAuditFromMeta(req.GetMeta(), "leave"))
	if err != nil {
		return nil, err
	}
	return &adminv1.LeaveNodeResponse{
		Cluster: cluster,
		Operation: &adminv1.OperationHandle{
			Accepted: true, OperationId: op.GetOperationId(), Message: "node leave accepted; drain required before remove",
		},
	}, nil
}

func (s *server) beginNodeDrain(ctx context.Context, nodeID, operationKind string, audit operationAudit) (*adminv1.OperationStatus, error) {
	nodeID = strings.TrimSpace(nodeID)
	if nodeID == "" {
		return nil, status.Error(codes.InvalidArgument, "node_id is required")
	}
	if err := enforceDependencyMembershipChange(); err != nil {
		return nil, err
	}
	rec, err := s.repo.GetNodeMembership(ctx, nodeID)
	if err != nil {
		if errors.Is(err, clustermeta.ErrNotFound) {
			return nil, status.Errorf(codes.NotFound, "node %q not found", nodeID)
		}
		return nil, status.Errorf(codes.Internal, "get node membership: %v", err)
	}
	op, err := s.ops.createAudited(operationKind, nodeID, "", "evacuation_pending", adminv1.OperationState_OPERATION_STATE_RUNNING, audit)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "create operation: %v", err)
	}
	rec.LifecycleState = clustermeta.NodeLifecycleDraining
	rec.DesiredState = string(clustermeta.NodeLifecycleDraining)
	rec.LastHeartbeatUnix = s.currentTime().Unix()
	rec.UpdatedBy = audit.Actor
	rec.UpdateReason = audit.Reason
	if _, _, err := s.repo.CompareAndSetNodeMembership(ctx, rec, rec.Generation); err != nil {
		s.failOperation(op.GetOperationId(), err)
		if errors.Is(err, clustermeta.ErrCASConflict) {
			return nil, status.Errorf(codes.Aborted, "node membership changed concurrently: %v", err)
		}
		return nil, status.Errorf(codes.Internal, "put node membership: %v", err)
	}
	if err := s.enqueueDrainTransitions(ctx, nodeID); err != nil {
		s.failOperation(op.GetOperationId(), err)
		return nil, status.Errorf(codes.Internal, "enqueue drain transitions: %v", err)
	}
	return s.refreshDrainOperation(ctx, op), nil
}

func (s *server) RemoveNode(ctx context.Context, req *adminv1.RemoveNodeRequest) (*adminv1.RemoveNodeResponse, error) {
	if err := s.requireLeader(ctx); err != nil {
		return nil, status.Error(codes.Unavailable, err.Error())
	}
	cluster, err := s.clusterRef(req.GetCluster())
	if err != nil {
		return nil, err
	}
	if req.GetNodeId() == "" {
		return nil, status.Error(codes.InvalidArgument, "node_id is required")
	}
	op, err := s.removeNode(ctx, req.GetNodeId(), false, operationAuditFromMeta(req.GetMeta(), "remove"))
	if err != nil {
		return nil, err
	}
	return &adminv1.RemoveNodeResponse{
		Cluster: cluster,
		Operation: &adminv1.OperationHandle{
			Accepted:    true,
			OperationId: op.GetOperationId(),
			Message:     "node marked removed",
		},
	}, nil
}

func (s *server) ForceRemoveNode(ctx context.Context, req *adminv1.ForceRemoveNodeRequest) (*adminv1.ForceRemoveNodeResponse, error) {
	if err := s.requireLeader(ctx); err != nil {
		return nil, status.Error(codes.Unavailable, err.Error())
	}
	cluster, err := s.clusterRef(req.GetCluster())
	if err != nil {
		return nil, err
	}
	if req.GetNodeId() == "" {
		return nil, status.Error(codes.InvalidArgument, "node_id is required")
	}
	meta := req.GetMeta()
	if strings.TrimSpace(meta.GetActor()) == "" || strings.TrimSpace(meta.GetReason()) == "" {
		return nil, status.Error(codes.InvalidArgument, "force remove requires actor and reason")
	}
	if strings.TrimSpace(req.GetApprovalId()) == "" || !req.GetAcknowledgeDataLossRisk() {
		return nil, status.Error(codes.FailedPrecondition, "force remove requires approval_id and acknowledge_data_loss_risk=true")
	}
	audit := operationAuditFromMeta(meta, "force remove")
	audit.ApprovalID = req.GetApprovalId()
	audit.RiskAcknowledged = true
	audit.FollowOnRepairRequired = true
	op, err := s.removeNode(ctx, req.GetNodeId(), true, audit)
	if err != nil {
		return nil, err
	}
	return &adminv1.ForceRemoveNodeResponse{
		Cluster: cluster,
		Operation: &adminv1.OperationHandle{
			Accepted:    true,
			OperationId: op.GetOperationId(),
			Message:     "node force removed",
		},
	}, nil
}

func (s *server) ListVolumes(ctx context.Context, req *adminv1.ListVolumesRequest) (*adminv1.ListVolumesResponse, error) {
	cluster, _ := s.clusterRef(req.GetCluster())
	volumes, err := s.repo.ListVolumeStates(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "list volumes: %v", err)
	}
	resp := &adminv1.ListVolumesResponse{Cluster: cluster}
	for _, vol := range volumes {
		resp.Volumes = append(resp.Volumes, s.volumeToProtoCached(ctx, vol))
	}
	return resp, nil
}

func (s *server) GetVolume(ctx context.Context, req *adminv1.GetVolumeRequest) (*adminv1.GetVolumeResponse, error) {
	started := time.Now()
	phase := "validate"
	cacheHit := false
	summaryMode := "full"
	var repoGetDuration time.Duration
	var buildProtoDuration time.Duration
	var err error
	volumeID := strings.TrimSpace(req.GetVolumeId())
	defer func() {
		fields := []structuredlog.Field{
			structuredlog.F("volume_id", volumeID),
			structuredlog.F("phase", phase),
			structuredlog.F("duration_ms", time.Since(started).Milliseconds()),
			structuredlog.F("repo_get_duration_ms", repoGetDuration.Milliseconds()),
			structuredlog.F("build_proto_duration_ms", buildProtoDuration.Milliseconds()),
			structuredlog.F("cache_hit", cacheHit),
			structuredlog.F("summary_mode", summaryMode),
		}
		if err != nil {
			if status.Code(err) == codes.NotFound {
				return
			}
			structuredlog.Error("sbs.service", "admin_get_volume_failed", err, fields...)
			return
		}
		structuredlog.Info("sbs.service", "admin_get_volume_completed", fields...)
	}()
	cluster, _ := s.clusterRef(req.GetCluster())
	if volumeID == "" {
		err = status.Error(codes.InvalidArgument, "volume_id is required")
		return nil, err
	}
	specOnly := adminVolumeSpecOnlySummaryRequested(ctx)
	if specOnly {
		summaryMode = "spec_only"
	}
	if !specOnly {
		if vol, ok := s.viewCache.getVolume(volumeID); ok {
			cacheHit = true
			phase = "completed"
			return &adminv1.GetVolumeResponse{Cluster: cluster, Volume: vol}, nil
		}
	}
	phase = "repo_get"
	phaseStarted := time.Now()
	vol, err := s.repo.GetVolumeState(ctx, req.GetVolumeId())
	repoGetDuration = time.Since(phaseStarted)
	if err != nil {
		if errors.Is(err, clustermeta.ErrNotFound) {
			err = status.Errorf(codes.NotFound, "volume %q not found", req.GetVolumeId())
			return nil, err
		}
		err = status.Errorf(codes.Internal, "get volume: %v", err)
		return nil, err
	}
	phase = "build_proto"
	phaseStarted = time.Now()
	var protoVolume *adminv1.VolumeSummary
	if specOnly {
		phase = "build_proto_spec_only"
		protoVolume = s.volumeToSpecOnlyProto(ctx, vol)
	} else {
		protoVolume = s.volumeToProtoCached(ctx, vol)
	}
	buildProtoDuration = time.Since(phaseStarted)
	phase = "completed"
	return &adminv1.GetVolumeResponse{Cluster: cluster, Volume: protoVolume}, nil
}

func adminVolumeSpecOnlySummaryRequested(ctx context.Context) bool {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return false
	}
	for _, value := range md.Get(adminVolumeSummaryModeMetadataKey) {
		if strings.EqualFold(strings.TrimSpace(value), adminVolumeSummaryModeSpecOnly) {
			return true
		}
	}
	return false
}

func (s *server) GetVolumePlacementView(ctx context.Context, req *adminv1.GetVolumePlacementViewRequest) (*adminv1.GetVolumePlacementViewResponse, error) {
	cluster, _ := s.clusterRef(req.GetCluster())
	volumeID := strings.TrimSpace(req.GetVolumeId())
	if volumeID == "" {
		return nil, status.Error(codes.InvalidArgument, "volume_id is required")
	}
	if cached, ok := s.viewCache.getPlacement(volumeID); ok {
		cached.Cluster = cluster
		return cached, nil
	}
	vol, err := s.repo.GetVolumeState(ctx, volumeID)
	if err != nil {
		if errors.Is(err, clustermeta.ErrNotFound) {
			return nil, status.Errorf(codes.NotFound, "volume %q not found", volumeID)
		}
		return nil, status.Errorf(codes.Internal, "get volume: %v", err)
	}
	mappings, err := s.repo.ListExtentMappings(ctx, volumeID)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "list extent mappings: %v", err)
	}
	replicaSets, err := s.repo.ListReplicaSets(ctx, volumeID)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "list replica sets: %v", err)
	}
	resp := &adminv1.GetVolumePlacementViewResponse{
		Cluster:     cluster,
		VolumeId:    volumeID,
		Revision:    vol.Revision,
		GeneratedAt: timestamppb.New(s.currentTime().UTC()),
	}
	for _, mapping := range mappings {
		resp.ExtentMappings = append(resp.ExtentMappings, &adminv1.ExtentPlacementSummary{
			VolumeId:      mapping.VolumeID,
			ExtentId:      mapping.ExtentID,
			LogicalOffset: mapping.LogicalOffset,
			LengthBytes:   mapping.LengthBytes,
			ChunkId:       mapping.ChunkID,
			PlacementRef:  mapping.PlacementRef,
			Revision:      mapping.Revision,
		})
	}
	for _, replicaSet := range replicaSets {
		resp.ReplicaSets = append(resp.ReplicaSets, replicaSetToProto(replicaSet))
	}
	s.viewCache.storePlacement(volumeID, resp)
	return resp, nil
}

func (s *server) GetVolumeAllocationPageView(ctx context.Context, req *adminv1.GetVolumeAllocationPageViewRequest) (*adminv1.GetVolumeAllocationPageViewResponse, error) {
	cluster, _ := s.clusterRef(req.GetCluster())
	volumeID := strings.TrimSpace(req.GetVolumeId())
	if volumeID == "" {
		return nil, status.Error(codes.InvalidArgument, "volume_id is required")
	}
	pageBytes := req.GetPageBytes()
	chunkSizeBytes := req.GetChunkSizeBytes()
	if pageBytes == 0 || chunkSizeBytes == 0 || pageBytes%chunkSizeBytes != 0 {
		return nil, status.Errorf(codes.InvalidArgument, "invalid allocation geometry: allocation_page_bytes=%d allocation_chunk_size_bytes=%d", pageBytes, chunkSizeBytes)
	}
	revision, page, err := sbscluster.BuildVolumeAllocationPageView(ctx, s.repo, s.repo, volumeID, req.GetPageNo(), pageBytes, chunkSizeBytes)
	if err != nil {
		if errors.Is(err, clustermeta.ErrNotFound) {
			return nil, status.Errorf(codes.NotFound, "volume %q not found", volumeID)
		}
		return nil, status.Errorf(codes.Internal, "get allocation page: %v", err)
	}
	return &adminv1.GetVolumeAllocationPageViewResponse{
		Cluster:        cluster,
		VolumeId:       volumeID,
		Revision:       revision,
		GeneratedAt:    timestamppb.New(s.currentTime().UTC()),
		AllocationPage: page,
	}, nil
}

func (s *server) ApplyPlacementChanges(ctx context.Context, req *internalv1.ApplyPlacementChangesRequest) (*internalv1.ApplyPlacementChangesResponse, error) {
	return clustercontrol.ServeApplyPlacementChanges(ctx, req, s.newPlacementApplyInternalService(), s.placementApplyObservability.record)
}

func (s *server) CommitWriteState(ctx context.Context, req *internalv1.CommitWriteStateRequest) (*internalv1.CommitWriteStateResponse, error) {
	return clustercontrol.ServeCommitWriteState(ctx, req, s.newWriteSessionInternalService(), s.lockWriteSessionCommitVolume, s.writeSessionObservability.record)
}

func (s *server) CommitPageScopedWriteMetadata(ctx context.Context, req *internalv1.CommitPageScopedWriteMetadataRequest) (*internalv1.CommitPageScopedWriteMetadataResponse, error) {
	return clustercontrol.ServeCommitPageScopedWriteMetadata(ctx, req, s.newWriteSessionInternalService(), s.writeSessionObservability.record)
}

func (s *server) CommitRangeLocalWriteState(ctx context.Context, req *internalv1.CommitRangeLocalWriteStateRequest) (*internalv1.CommitRangeLocalWriteStateResponse, error) {
	return clustercontrol.ServeCommitRangeLocalWriteState(ctx, req, s.newWriteSessionInternalService(), s.writeSessionObservability.record)
}

func (s *server) CommitAppendOnlyWriteStateAndQueueEffects(ctx context.Context, req *internalv1.CommitAppendOnlyWriteStateAndQueueEffectsRequest) (*internalv1.CommitAppendOnlyWriteStateAndQueueEffectsResponse, error) {
	var effectsQueue clustercontrol.WriteSessionEffectsQueue
	nativeAllocationFastPath := false
	if s.serviceOwnedWriteEffects && s.writeEffectsQueue != nil {
		effectsQueue = s.writeEffectsQueue
		nativeAllocationFastPath = s.writeEffectsQueue.nativeAllocationFastPath
	}
	return clustercontrol.ServeCommitAppendOnlyWriteStateAndQueueEffects(ctx, req, s.newWriteSessionInternalService(), effectsQueue, nativeAllocationFastPath, s.writeSessionObservability.record)
}

func (s *server) CommitCloneDeltaAllocationPages(ctx context.Context, req *internalv1.CommitCloneDeltaAllocationPagesRequest) (*internalv1.CommitCloneDeltaAllocationPagesResponse, error) {
	cloneService, _ := s.newWriteSessionInternalService().(clustercontrol.WriteSessionCloneDeltaInternalService)
	return clustercontrol.ServeCommitCloneDeltaAllocationPages(ctx, req, cloneService, s.writeSessionObservability.record)
}

func (s *server) lockWriteSessionCommitVolume(volumeID string) func() {
	volumeID = strings.TrimSpace(volumeID)
	if volumeID == "" {
		return func() {}
	}
	s.writeSessionCommitLocksMu.Lock()
	if s.writeSessionCommitLocks == nil {
		s.writeSessionCommitLocks = make(map[string]*sync.Mutex)
	}
	mu := s.writeSessionCommitLocks[volumeID]
	if mu == nil {
		mu = &sync.Mutex{}
		s.writeSessionCommitLocks[volumeID] = mu
	}
	s.writeSessionCommitLocksMu.Unlock()

	mu.Lock()
	return mu.Unlock
}

func (s *server) GetVolumeState(ctx context.Context, req *internalv1.GetVolumeStateRequest) (*internalv1.GetVolumeStateResponse, error) {
	return clustercontrol.ServeGetVolumeState(ctx, req, s.newWriteSessionInternalService())
}

func (s *server) PutVolumeState(ctx context.Context, req *internalv1.PutVolumeStateRequest) (*internalv1.PutVolumeStateResponse, error) {
	return clustercontrol.ServePutVolumeState(ctx, req, s.newWriteSessionInternalService())
}

func (s *server) GetIdempotencyRecord(ctx context.Context, req *internalv1.GetIdempotencyRecordRequest) (*internalv1.GetIdempotencyRecordResponse, error) {
	return clustercontrol.ServeGetIdempotencyRecord(ctx, req, s.newWriteSessionInternalService())
}

func (s *server) PutIdempotencyRecord(ctx context.Context, req *internalv1.PutIdempotencyRecordRequest) (*internalv1.PutIdempotencyRecordResponse, error) {
	return clustercontrol.ServePutIdempotencyRecord(ctx, req, s.newWriteSessionInternalService())
}

func (s *server) GetMutationOperation(ctx context.Context, req *internalv1.GetMutationOperationRequest) (*internalv1.GetMutationOperationResponse, error) {
	return clustercontrol.ServeGetMutationOperation(ctx, req, s.newWriteSessionInternalService())
}

func (s *server) PutMutationOperation(ctx context.Context, req *internalv1.PutMutationOperationRequest) (*internalv1.PutMutationOperationResponse, error) {
	return clustercontrol.ServePutMutationOperation(ctx, req, s.newWriteSessionInternalService())
}

func (s *server) PutWriteIntent(ctx context.Context, req *internalv1.PutWriteIntentRequest) (*internalv1.PutWriteIntentResponse, error) {
	if s.writeIntentQueue != nil {
		record, err := clustercontrol.IdempotencyRecordFromProto(req.GetIdempotencyRecord())
		if err != nil {
			return nil, clustercontrol.WriteSessionErrorToGRPCStatus(err)
		}
		operation, err := clustercontrol.MutationOperationRecordFromProto(req.GetMutationOperation())
		if err != nil {
			return nil, clustercontrol.WriteSessionErrorToGRPCStatus(err)
		}
		if err := s.writeIntentQueue.EnqueueAndWait(ctx, record, operation); err != nil {
			return nil, clustercontrol.WriteSessionErrorToGRPCStatus(err)
		}
		return &internalv1.PutWriteIntentResponse{}, nil
	}
	return clustercontrol.ServePutWriteIntent(ctx, req, s.newWriteSessionInternalService())
}

func (s *server) GetPhysicalObject(ctx context.Context, req *internalv1.GetPhysicalObjectRequest) (*internalv1.GetPhysicalObjectResponse, error) {
	return clustercontrol.ServeGetPhysicalObject(ctx, req, s.newECMetadataInternalService())
}

func (s *server) PutPhysicalObject(ctx context.Context, req *internalv1.PutPhysicalObjectRequest) (*internalv1.PutPhysicalObjectResponse, error) {
	return clustercontrol.ServePutPhysicalObject(ctx, req, s.newECMetadataInternalService())
}

func (s *server) GetECStripe(ctx context.Context, req *internalv1.GetECStripeRequest) (*internalv1.GetECStripeResponse, error) {
	return clustercontrol.ServeGetECStripe(ctx, req, s.newECMetadataInternalService())
}

func (s *server) PutECStripe(ctx context.Context, req *internalv1.PutECStripeRequest) (*internalv1.PutECStripeResponse, error) {
	return clustercontrol.ServePutECStripe(ctx, req, s.newECMetadataInternalService())
}

func (s *server) CommitECFullStripeWrite(ctx context.Context, req *internalv1.CommitECFullStripeWriteRequest) (*internalv1.CommitECFullStripeWriteResponse, error) {
	return clustercontrol.ServeCommitECFullStripeWrite(ctx, req, s.newECMetadataInternalService(), s.lockWriteSessionCommitVolume, s.writeSessionObservability.record)
}

func (s *server) CommitECDiscard(ctx context.Context, req *internalv1.CommitECDiscardRequest) (*internalv1.CommitECDiscardResponse, error) {
	return clustercontrol.ServeCommitECDiscard(ctx, req, s.newECMetadataInternalService(), s.lockWriteSessionCommitVolume, s.writeSessionObservability.record)
}

func (s *server) AllocateChunkIDs(ctx context.Context, req *internalv1.AllocateChunkIDsRequest) (*internalv1.AllocateChunkIDsResponse, error) {
	return clustercontrol.ServeAllocateChunkIDs(ctx, req, s.newChunkIDAllocatorInternalService(), s.chunkIDAllocatorObservability.record)
}

func (s *server) ResolveExtentPlacements(ctx context.Context, req *internalv1.ResolveExtentPlacementsRequest) (*internalv1.ResolveExtentPlacementsResponse, error) {
	return clustercontrol.ServeResolveExtentPlacements(ctx, req, s.newPlacementResolverInternalService(), s.placementResolverObservability.record)
}

func (s *server) ResolveAllocationPages(ctx context.Context, req *internalv1.ResolveAllocationPagesRequest) (*internalv1.ResolveAllocationPagesResponse, error) {
	return clustercontrol.ServeResolveAllocationPages(ctx, req, s.newPlacementResolverInternalService(), s.placementResolverObservability.record)
}

func (s *server) ResolveSnapshotAllocationPages(ctx context.Context, req *internalv1.ResolveSnapshotAllocationPagesRequest) (*internalv1.ResolveSnapshotAllocationPagesResponse, error) {
	return clustercontrol.ServeResolveSnapshotAllocationPages(ctx, req, s.newPlacementResolverInternalService(), s.placementResolverObservability.record)
}

func (s *server) ResolveCloneAllocationPages(ctx context.Context, req *internalv1.ResolveCloneAllocationPagesRequest) (*internalv1.ResolveCloneAllocationPagesResponse, error) {
	return clustercontrol.ServeResolveCloneAllocationPages(ctx, req, s.newPlacementResolverInternalService(), s.placementResolverObservability.record)
}

func (s *server) GetReplicaTargetsView(ctx context.Context, req *adminv1.GetReplicaTargetsViewRequest) (*adminv1.GetReplicaTargetsViewResponse, error) {
	cluster, err := s.clusterRef(req.GetCluster())
	if err != nil {
		return nil, err
	}
	volumeID := strings.TrimSpace(req.GetVolumeId())
	revision := uint64(0)
	if volumeID != "" {
		vol, err := s.repo.GetVolumeState(ctx, volumeID)
		if err != nil {
			if errors.Is(err, clustermeta.ErrNotFound) {
				return nil, status.Errorf(codes.NotFound, "volume %q not found", volumeID)
			}
			return nil, status.Errorf(codes.Internal, "get replica targets view volume: %v", err)
		}
		revision = vol.Revision
		spec, err := s.getVolumeSpec(ctx, volumeID)
		if err == nil && effectiveVolumeRedundancyBackend(vol, spec) == clustermeta.RedundancyBackendEC {
			return nil, status.Errorf(codes.FailedPrecondition, "replica targets view is unsupported for ec volume %q", volumeID)
		}
		if err != nil && !errors.Is(err, clustermeta.ErrNotFound) {
			return nil, status.Errorf(codes.Internal, "get replica targets view volume spec: %v", err)
		}
	}
	if cached, ok := s.viewCache.getReplicaTargets(volumeID); ok {
		cached.Cluster = cluster
		return cached, nil
	}
	targets, err := s.replicaTargetsView(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "build replica targets view: %v", err)
	}
	resp := &adminv1.GetReplicaTargetsViewResponse{
		Cluster:         cluster,
		VolumeId:        volumeID,
		Revision:        revision,
		GeneratedAt:     timestamppb.New(s.currentTime().UTC()),
		CacheTtlSeconds: uint64(maxInt64(int64(s.healthCheckInterval.Seconds()), 1)),
		Targets:         targets,
	}
	s.viewCache.storeReplicaTargets(volumeID, resp)
	return resp, nil
}

func (s *server) CreateVolume(ctx context.Context, req *adminv1.CreateVolumeRequest) (*adminv1.CreateVolumeResponse, error) {
	if err := s.requireLeader(ctx); err != nil {
		return nil, status.Error(codes.Unavailable, err.Error())
	}
	cluster, err := s.clusterRef(req.GetCluster())
	if err != nil {
		return nil, err
	}
	if req.GetVolumeId() == "" || req.GetSizeBytes() == 0 || req.GetBlockSize() == 0 {
		return nil, status.Error(codes.InvalidArgument, "volume_id, size_bytes, and block_size are required")
	}
	if req.GetSizeBytes()%uint64(req.GetBlockSize()) != 0 {
		return nil, status.Error(codes.InvalidArgument, "size_bytes must be aligned to block_size")
	}
	volumeID, err := clustermeta.CanonicalVolumeID(req.GetVolumeId())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "canonical volume id: %v", err)
	}
	if _, err := s.repo.GetVolumeState(ctx, volumeID); err == nil {
		return nil, status.Errorf(codes.AlreadyExists, "volume %q already exists", volumeID)
	} else if !errors.Is(err, clustermeta.ErrNotFound) {
		return nil, status.Errorf(codes.Internal, "get volume: %v", err)
	}

	redundancyBackend, err := normalizeRequestedRedundancyBackend(req.GetRedundancyBackend())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	replicationFactor := maxUint32(req.GetReplicationFactor(), 1)
	topologyMode := ""
	if redundancyBackend == clustermeta.RedundancyBackendEC {
		topologyMode, err = normalizeECVolumeTopologyMode(req.GetTopologyMode(), req.GetWeakPlacementAllowed())
		replicationFactor = 0
	} else {
		topologyMode, err = normalizeRequestedTopologyMode(req.GetTopologyMode())
	}
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	extentSizeBytes, err := configuredExtentSizeBytes(req.GetExtentSizeBytes(), req.GetBlockSize())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "extent size: %v", err)
	}
	chunkSizeBytes, extentPageBytes, err := configuredAllocationGeometry(req.GetAllocationChunkSizeBytes(), req.GetAllocationPageSizeBytes())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "allocation geometry: %v", err)
	}
	normalizedSpec := service.NormalizeVolumeSpec(service.VolumeSpec{
		Name:            volumeID,
		Prefix:          "vol-" + volumeID,
		SizeBytes:       req.GetSizeBytes(),
		BlockSize:       req.GetBlockSize(),
		ChunkSizeBytes:  chunkSizeBytes,
		ExtentPageBytes: extentPageBytes,
	})
	if err := clustercontrol.ValidatePlacementResolverGeometry(normalizedSpec.ExtentPageBytes, normalizedSpec.ChunkSizeBytes); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "allocation geometry: %v", err)
	}
	var ecProfile clustermeta.ECProfileRecord
	if redundancyBackend == clustermeta.RedundancyBackendEC {
		ecProfileID := strings.TrimSpace(req.GetEcProfileId())
		if ecProfileID == "" {
			return nil, status.Error(codes.InvalidArgument, "ec_profile_id is required for ec volumes")
		}
		ecProfile, err = s.repo.GetECProfile(ctx, ecProfileID)
		if err != nil {
			if errors.Is(err, clustermeta.ErrNotFound) {
				return nil, status.Errorf(codes.NotFound, "ec profile %q not found", ecProfileID)
			}
			return nil, status.Errorf(codes.Internal, "get ec profile: %v", err)
		}
		if ecProfile.Lifecycle != clustermeta.ECProfileLifecycleActive {
			return nil, status.Errorf(codes.FailedPrecondition, "ec profile %q is not active", ecProfile.ProfileID)
		}
		if err := clustermeta.ValidateECProfile(ecProfile, clustermeta.ECProfileValidationOptions{
			BlockSizeBytes:           req.GetBlockSize(),
			AllocationChunkSizeBytes: normalizedSpec.ChunkSizeBytes,
		}); err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "validate ec profile for volume geometry: %v", err)
		}
	}
	var selectedNodes []clustermeta.NodeMembershipRecord
	if redundancyBackend == clustermeta.RedundancyBackendReplicated {
		selectedNodes, err = s.selectPlacementNodes(ctx, replicationFactor)
		if err != nil {
			return nil, err
		}
	}

	op, err := s.ops.create("volume.create", "", volumeID, "creating", adminv1.OperationState_OPERATION_STATE_RUNNING)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "create operation: %v", err)
	}
	cleanupVolume := true
	defer func() {
		if cleanupVolume {
			_ = s.deleteVolumeArtifacts(context.Background(), volumeID)
		}
	}()
	volume := clustermeta.VolumeState{
		VolumeID:          volumeID,
		Epoch:             1,
		Revision:          1,
		PlacementPolicyID: req.GetPolicyName(),
		TopologyMode:      topologyMode,
		ProtectionPolicy:  fmt.Sprintf("rf%d", replicationFactor),
		RedundancyBackend: redundancyBackend,
		Status:            clustermeta.VolumeStatusHealthy,
	}
	if redundancyBackend == clustermeta.RedundancyBackendEC {
		volume.ProtectionPolicy = fmt.Sprintf("ec:%s", ecProfile.ProfileID)
	}
	if err := s.repo.PutVolumeState(ctx, volume); err != nil {
		s.failOperation(op.GetOperationId(), err)
		return nil, status.Errorf(codes.Internal, "put volume state: %v", err)
	}
	specRecord := volumeSpecRecord{
		VolumeID:          volumeID,
		SizeBytes:         req.GetSizeBytes(),
		BlockSize:         req.GetBlockSize(),
		ChunkSizeBytes:    normalizedSpec.ChunkSizeBytes,
		ExtentPageBytes:   normalizedSpec.ExtentPageBytes,
		ExtentSizeBytes:   extentSizeBytes,
		ReplicationFactor: replicationFactor,
		PolicyName:        req.GetPolicyName(),
		TopologyMode:      topologyMode,
		RedundancyBackend: redundancyBackend,
		CreatedBy:         req.GetMeta().GetActor(),
		CreatedReason:     req.GetMeta().GetReason(),
		CreatedAtUnix:     time.Now().Unix(),
	}
	if redundancyBackend == clustermeta.RedundancyBackendEC {
		specRecord.ECProfileID = ecProfile.ProfileID
		specRecord.ECCodecID = ecProfile.CodecID
		specRecord.ECDataShards = ecProfile.DataShards
		specRecord.ECParityShards = ecProfile.ParityShards
		specRecord.ECStripeUnitBytes = ecProfile.StripeUnitBytes
		specRecord.ECFailureDomain = ecProfile.FailureDomain
		specRecord.ECMaxUnavailableFailureDomains = ecProfile.MaxUnavailableFailureDomains
		specRecord.ECMaxShardsPerFailureDomain = ecProfile.MaxShardsPerFailureDomain
		specRecord.WeakPlacementAllowed = req.GetWeakPlacementAllowed()
	}
	if err := s.putVolumeSpec(ctx, specRecord); err != nil {
		s.failOperation(op.GetOperationId(), err)
		return nil, status.Errorf(codes.Internal, "put volume spec: %v", err)
	}
	if redundancyBackend == clustermeta.RedundancyBackendReplicated {
		if err := s.createInitialPlacement(ctx, volumeID, req.GetSizeBytes(), extentSizeBytes, replicationFactor, req.GetPolicyName(), topologyMode, selectedNodes); err != nil {
			s.failOperation(op.GetOperationId(), err)
			return nil, status.Errorf(codes.Internal, "create initial placement: %v", err)
		}
		if err := s.initializeVolumeAllocationPages(ctx, volumeSpecRecord{
			VolumeID:        volumeID,
			SizeBytes:       req.GetSizeBytes(),
			ChunkSizeBytes:  normalizedSpec.ChunkSizeBytes,
			ExtentPageBytes: normalizedSpec.ExtentPageBytes,
		}); err != nil {
			s.failOperation(op.GetOperationId(), err)
			return nil, status.Errorf(codes.Internal, "initialize allocation pages: %v", err)
		}
	}
	op, _ = s.ops.update(op.GetOperationId(), func(op *adminv1.OperationStatus) {
		op.State = adminv1.OperationState_OPERATION_STATE_COMPLETED
		if redundancyBackend == clustermeta.RedundancyBackendEC {
			op.Phase = "ec-profile-bound"
		} else {
			op.Phase = "placed"
		}
	})
	cleanupVolume = false
	return &adminv1.CreateVolumeResponse{
		Cluster: cluster,
		Operation: &adminv1.OperationHandle{
			Accepted:    true,
			OperationId: op.GetOperationId(),
			Message:     "volume created",
		},
	}, nil
}

func (s *server) CreateVolumeFromSnapshot(ctx context.Context, req *adminv1.CreateVolumeFromSnapshotRequest) (*adminv1.CreateVolumeFromSnapshotResponse, error) {
	if err := s.requireLeader(ctx); err != nil {
		return nil, status.Error(codes.Unavailable, err.Error())
	}
	cluster, err := s.clusterRef(req.GetCluster())
	if err != nil {
		return nil, err
	}
	sourceSnapshotID := strings.TrimSpace(req.GetSourceSnapshotId())
	if sourceSnapshotID == "" {
		return nil, status.Error(codes.InvalidArgument, "source_snapshot_id is required")
	}
	targetVolumeID := strings.TrimSpace(req.GetVolumeId())
	if targetVolumeID != "" {
		targetVolumeID, err = clustermeta.CanonicalVolumeID(targetVolumeID)
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "canonical volume id: %v", err)
		}
	}
	idempotencyKey := strings.TrimSpace(req.GetIdempotencyKey())
	if idempotencyKey == "" {
		if targetVolumeID == "" {
			return nil, status.Error(codes.InvalidArgument, "idempotency_key is required when volume_id is omitted")
		}
		idempotencyKey = createVolumeFromSnapshotIdempotencyKey(sourceSnapshotID, targetVolumeID)
	}
	sourceSnapshot, err := s.repo.GetSnapshotRecord(ctx, sourceSnapshotID)
	if err != nil {
		if errors.Is(err, clustermeta.ErrNotFound) {
			return nil, status.Errorf(codes.NotFound, "source snapshot %q not found", sourceSnapshotID)
		}
		return nil, status.Errorf(codes.Internal, "get source snapshot: %v", err)
	}
	if sourceSnapshot.State != clustermeta.SnapshotStateAvailable {
		return nil, status.Errorf(codes.FailedPrecondition, "source snapshot %q is not available: state=%s", sourceSnapshotID, sourceSnapshot.State)
	}
	if req.GetSizeBytes() != 0 && req.GetSizeBytes() < sourceSnapshot.SourceSizeBytes {
		return nil, status.Errorf(codes.InvalidArgument, "size_bytes %d is smaller than source snapshot size_bytes %d", req.GetSizeBytes(), sourceSnapshot.SourceSizeBytes)
	}

	createCloneResp, err := s.CreateClone(ctx, &adminv1.CreateCloneRequest{
		Cluster:          req.GetCluster(),
		Meta:             req.GetMeta(),
		SourceSnapshotId: sourceSnapshotID,
		IdempotencyKey:   idempotencyKey,
		SizeBytes:        req.GetSizeBytes(),
	})
	if err != nil {
		return nil, err
	}
	cloneID := createCloneResp.GetCloneId()
	if cloneID == "" {
		return nil, status.Error(codes.Internal, "create clone did not return clone_id")
	}
	materializeResp, err := s.MaterializeClone(ctx, &adminv1.MaterializeCloneRequest{
		Cluster:        req.GetCluster(),
		Meta:           req.GetMeta(),
		CloneId:        cloneID,
		TargetVolumeId: targetVolumeID,
	})
	if err != nil {
		return nil, err
	}
	materializedVolumeID := materializeResp.GetMaterializedVolumeId()
	if materializedVolumeID == "" {
		return nil, status.Error(codes.Internal, "materialize clone did not return materialized_volume_id")
	}
	if targetVolumeID != "" && materializedVolumeID != targetVolumeID {
		return nil, status.Errorf(codes.FailedPrecondition, "idempotency key already restored snapshot %q as volume %q, not requested volume %q",
			sourceSnapshotID, materializedVolumeID, targetVolumeID)
	}
	targetSpec, err := s.getVolumeSpec(ctx, materializedVolumeID)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "get restored volume spec: %v", err)
	}
	if req.GetSizeBytes() != 0 && targetSpec.SizeBytes != req.GetSizeBytes() {
		return nil, status.Errorf(codes.FailedPrecondition, "idempotency key already restored snapshot %q as volume %q with size_bytes %d, not requested size_bytes %d",
			sourceSnapshotID, materializedVolumeID, targetSpec.SizeBytes, req.GetSizeBytes())
	}
	return &adminv1.CreateVolumeFromSnapshotResponse{
		Cluster:          cluster,
		Operation:        materializeResp.GetOperation(),
		VolumeId:         materializedVolumeID,
		SourceSnapshotId: sourceSnapshotID,
		CloneId:          cloneID,
		SizeBytes:        targetSpec.SizeBytes,
	}, nil
}

func (s *server) ExpandVolume(ctx context.Context, req *adminv1.ExpandVolumeRequest) (*adminv1.ExpandVolumeResponse, error) {
	if err := s.requireLeader(ctx); err != nil {
		return nil, status.Error(codes.Unavailable, err.Error())
	}
	cluster, err := s.clusterRef(req.GetCluster())
	if err != nil {
		return nil, err
	}
	if req.GetVolumeId() == "" || req.GetTargetSizeBytes() == 0 {
		return nil, status.Error(codes.InvalidArgument, "volume_id and target_size_bytes are required")
	}
	volumeID, err := clustermeta.CanonicalVolumeID(req.GetVolumeId())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "canonical volume id: %v", err)
	}
	if _, err := s.repo.GetVolumeState(ctx, volumeID); err != nil {
		if errors.Is(err, clustermeta.ErrNotFound) {
			return nil, status.Errorf(codes.NotFound, "volume %q not found", volumeID)
		}
		return nil, status.Errorf(codes.Internal, "get volume: %v", err)
	}
	op, err := s.ops.create("volume.expand", "", volumeID, "expanding", adminv1.OperationState_OPERATION_STATE_RUNNING)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "create operation: %v", err)
	}
	oldSpec, newSpec, newState, err := s.repo.ExpandVolume(ctx, volumeID, req.GetTargetSizeBytes())
	if err != nil {
		s.failOperation(op.GetOperationId(), err)
		switch {
		case errors.Is(err, clustermeta.ErrNotFound):
			return nil, status.Errorf(codes.NotFound, "volume %q not found", volumeID)
		case strings.Contains(err.Error(), "aligned"), strings.Contains(err.Error(), "greater"), strings.Contains(err.Error(), "required"), strings.Contains(err.Error(), "block_size"):
			return nil, status.Error(codes.InvalidArgument, err.Error())
		default:
			return nil, status.Errorf(codes.Internal, "expand volume: %v", err)
		}
	}
	if newSpec.SizeBytes > oldSpec.SizeBytes && effectiveMetadataVolumeRedundancyBackend(newSpec) != clustermeta.RedundancyBackendEC {
		selectedNodes, err := s.selectPlacementNodes(ctx, newSpec.ReplicationFactor)
		if err != nil {
			s.failOperation(op.GetOperationId(), err)
			return nil, err
		}
		if err := s.ensureExpansionPlacement(ctx, volumeID, oldSpec, newSpec, selectedNodes); err != nil {
			s.failOperation(op.GetOperationId(), err)
			return nil, status.Errorf(codes.Internal, "ensure expansion placement: %v", err)
		}
	}
	s.viewCache.invalidateVolume(volumeID)
	op, _ = s.ops.update(op.GetOperationId(), func(op *adminv1.OperationStatus) {
		op.State = adminv1.OperationState_OPERATION_STATE_COMPLETED
		op.Phase = "expanded"
	})
	return &adminv1.ExpandVolumeResponse{
		Cluster:        cluster,
		Operation:      &adminv1.OperationHandle{Accepted: true, OperationId: op.GetOperationId(), Message: "volume expanded"},
		VolumeId:       volumeID,
		OldSizeBytes:   oldSpec.SizeBytes,
		SizeBytes:      newSpec.SizeBytes,
		VolumeRevision: newState.Revision,
	}, nil
}

func (s *server) DeleteVolume(ctx context.Context, req *adminv1.DeleteVolumeRequest) (*adminv1.DeleteVolumeResponse, error) {
	if err := s.requireLeader(ctx); err != nil {
		return nil, status.Error(codes.Unavailable, err.Error())
	}
	cluster, err := s.clusterRef(req.GetCluster())
	if err != nil {
		return nil, err
	}
	if req.GetVolumeId() == "" {
		return nil, status.Error(codes.InvalidArgument, "volume_id is required")
	}
	volumeID, err := clustermeta.CanonicalVolumeID(req.GetVolumeId())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "canonical volume id: %v", err)
	}
	if _, err := s.repo.GetVolumeState(ctx, volumeID); err != nil {
		if errors.Is(err, clustermeta.ErrNotFound) {
			return nil, status.Errorf(codes.NotFound, "volume %q not found", volumeID)
		}
		return nil, status.Errorf(codes.Internal, "get volume: %v", err)
	}

	transitions, err := s.repo.ListPlacementTransitions(ctx, volumeID)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "list transitions: %v", err)
	}
	activeTransitions := 0
	for _, tr := range transitions {
		if isActiveTransitionState(tr.State) {
			activeTransitions++
		}
	}
	if activeTransitions > 0 {
		return nil, status.Errorf(codes.FailedPrecondition, "volume %q still has %d active placement transitions", volumeID, activeTransitions)
	}

	op, err := s.ops.create("volume.delete", "", volumeID, "deleting", adminv1.OperationState_OPERATION_STATE_RUNNING)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "create operation: %v", err)
	}
	if err := s.deleteVolumeArtifacts(ctx, volumeID); err != nil {
		s.failOperation(op.GetOperationId(), err)
		return nil, status.Errorf(codes.Internal, "delete volume artifacts: %v", err)
	}
	op, _ = s.ops.update(op.GetOperationId(), func(op *adminv1.OperationStatus) {
		op.State = adminv1.OperationState_OPERATION_STATE_COMPLETED
		op.Phase = "deleted"
	})
	return &adminv1.DeleteVolumeResponse{
		Cluster: cluster,
		Operation: &adminv1.OperationHandle{
			Accepted:    true,
			OperationId: op.GetOperationId(),
			Message:     "volume deleted",
		},
	}, nil
}

func (s *server) PurgeVolume(ctx context.Context, req *adminv1.PurgeVolumeRequest) (*adminv1.PurgeVolumeResponse, error) {
	if err := s.requireLeader(ctx); err != nil {
		return nil, status.Error(codes.Unavailable, err.Error())
	}
	cluster, err := s.clusterRef(req.GetCluster())
	if err != nil {
		return nil, err
	}
	if req.GetVolumeId() == "" {
		return nil, status.Error(codes.InvalidArgument, "volume_id is required")
	}
	if !req.GetConfirmedDeletion() {
		return nil, status.Error(codes.InvalidArgument, "confirmed_deletion is required for purge")
	}
	volumeID, err := clustermeta.CanonicalVolumeID(req.GetVolumeId())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "canonical volume id: %v", err)
	}
	if _, err := s.repo.GetVolumeState(ctx, volumeID); err != nil {
		if errors.Is(err, clustermeta.ErrNotFound) {
			return nil, status.Errorf(codes.NotFound, "volume %q not found", volumeID)
		}
		return nil, status.Errorf(codes.Internal, "get volume: %v", err)
	}

	op, err := s.ops.create("volume.purge", "", volumeID, "purging", adminv1.OperationState_OPERATION_STATE_RUNNING)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "create operation: %v", err)
	}
	if err := s.deleteVolumeArtifacts(ctx, volumeID); err != nil {
		s.failOperation(op.GetOperationId(), err)
		return nil, status.Errorf(codes.Internal, "purge volume artifacts: %v", err)
	}
	op, _ = s.ops.update(op.GetOperationId(), func(op *adminv1.OperationStatus) {
		op.State = adminv1.OperationState_OPERATION_STATE_COMPLETED
		op.Phase = "purged"
	})
	return &adminv1.PurgeVolumeResponse{
		Cluster: cluster,
		Operation: &adminv1.OperationHandle{
			Accepted:    true,
			OperationId: op.GetOperationId(),
			Message:     "volume purged",
		},
	}, nil
}

func (s *server) CreateECProfile(ctx context.Context, req *adminv1.CreateECProfileRequest) (*adminv1.CreateECProfileResponse, error) {
	if err := s.requireLeader(ctx); err != nil {
		return nil, status.Error(codes.Unavailable, err.Error())
	}
	cluster, err := s.clusterRef(req.GetCluster())
	if err != nil {
		return nil, err
	}
	now := s.currentTime().Unix()
	profile := clustermeta.NormalizeECProfile(clustermeta.ECProfileRecord{
		ProfileID:       req.GetProfileId(),
		CodecID:         req.GetCodecId(),
		DataShards:      req.GetDataShards(),
		ParityShards:    req.GetParityShards(),
		StripeUnitBytes: req.GetStripeUnitBytes(),
		FailureDomain:   req.GetFailureDomain(),
		LabOverride:     req.GetLabOverride(),
		CreatedBy:       req.GetMeta().GetActor(),
		CreatedReason:   req.GetMeta().GetReason(),
		CreatedAtUnix:   now,
	})
	if err := clustermeta.ValidateECProfile(profile, clustermeta.ECProfileValidationOptions{}); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "validate ec profile: %v", err)
	}
	if _, err := s.repo.GetECProfile(ctx, profile.ProfileID); err == nil {
		return nil, status.Errorf(codes.AlreadyExists, "ec profile %q already exists", profile.ProfileID)
	} else if !errors.Is(err, clustermeta.ErrNotFound) {
		return nil, status.Errorf(codes.Internal, "get ec profile: %v", err)
	}
	op, err := s.ops.create("ec.profile.create", "", "", "creating", adminv1.OperationState_OPERATION_STATE_RUNNING)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "create operation: %v", err)
	}
	if err := s.repo.PutECProfile(ctx, profile); err != nil {
		s.failOperation(op.GetOperationId(), err)
		return nil, status.Errorf(codes.Internal, "put ec profile: %v", err)
	}
	op, _ = s.ops.update(op.GetOperationId(), func(op *adminv1.OperationStatus) {
		op.State = adminv1.OperationState_OPERATION_STATE_COMPLETED
		op.Phase = "created"
	})
	return &adminv1.CreateECProfileResponse{
		Cluster: cluster,
		Operation: &adminv1.OperationHandle{
			Accepted:    true,
			OperationId: op.GetOperationId(),
			Message:     "ec profile created",
		},
		Profile: ecProfileToProto(profile),
	}, nil
}

func (s *server) ListECProfiles(ctx context.Context, req *adminv1.ListECProfilesRequest) (*adminv1.ListECProfilesResponse, error) {
	cluster, err := s.clusterRef(req.GetCluster())
	if err != nil {
		return nil, err
	}
	profiles, err := s.repo.ListECProfiles(ctx, req.GetIncludeDisabled())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "list ec profiles: %v", err)
	}
	out := make([]*adminv1.ECProfileSummary, 0, len(profiles))
	for _, profile := range profiles {
		out = append(out, ecProfileToProto(profile))
	}
	return &adminv1.ListECProfilesResponse{Cluster: cluster, Profiles: out}, nil
}

func (s *server) GetECProfile(ctx context.Context, req *adminv1.GetECProfileRequest) (*adminv1.GetECProfileResponse, error) {
	cluster, err := s.clusterRef(req.GetCluster())
	if err != nil {
		return nil, err
	}
	profile, err := s.repo.GetECProfile(ctx, req.GetProfileId())
	if err != nil {
		if errors.Is(err, clustermeta.ErrNotFound) {
			return nil, status.Errorf(codes.NotFound, "ec profile %q not found", req.GetProfileId())
		}
		return nil, status.Errorf(codes.Internal, "get ec profile: %v", err)
	}
	return &adminv1.GetECProfileResponse{Cluster: cluster, Profile: ecProfileToProto(profile)}, nil
}

func (s *server) DisableECProfile(ctx context.Context, req *adminv1.DisableECProfileRequest) (*adminv1.DisableECProfileResponse, error) {
	if err := s.requireLeader(ctx); err != nil {
		return nil, status.Error(codes.Unavailable, err.Error())
	}
	cluster, err := s.clusterRef(req.GetCluster())
	if err != nil {
		return nil, err
	}
	profile, err := s.repo.GetECProfile(ctx, req.GetProfileId())
	if err != nil {
		if errors.Is(err, clustermeta.ErrNotFound) {
			return nil, status.Errorf(codes.NotFound, "ec profile %q not found", req.GetProfileId())
		}
		return nil, status.Errorf(codes.Internal, "get ec profile: %v", err)
	}
	op, err := s.ops.create("ec.profile.disable", "", "", "disabling", adminv1.OperationState_OPERATION_STATE_RUNNING)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "create operation: %v", err)
	}
	profile.Lifecycle = clustermeta.ECProfileLifecycleDisabled
	profile.UpdatedAtUnix = s.currentTime().Unix()
	if err := s.repo.PutECProfile(ctx, profile); err != nil {
		s.failOperation(op.GetOperationId(), err)
		return nil, status.Errorf(codes.Internal, "put ec profile: %v", err)
	}
	op, _ = s.ops.update(op.GetOperationId(), func(op *adminv1.OperationStatus) {
		op.State = adminv1.OperationState_OPERATION_STATE_COMPLETED
		op.Phase = "disabled"
	})
	return &adminv1.DisableECProfileResponse{
		Cluster: cluster,
		Operation: &adminv1.OperationHandle{
			Accepted:    true,
			OperationId: op.GetOperationId(),
			Message:     "ec profile disabled",
		},
		Profile: ecProfileToProto(profile),
	}, nil
}

func (s *server) CreateSnapshot(ctx context.Context, req *adminv1.CreateSnapshotRequest) (*adminv1.CreateSnapshotResponse, error) {
	if err := s.requireLeader(ctx); err != nil {
		return nil, status.Error(codes.Unavailable, err.Error())
	}
	cluster, err := s.clusterRef(req.GetCluster())
	if err != nil {
		return nil, err
	}
	if req.GetSourceVolumeId() == "" {
		return nil, status.Error(codes.InvalidArgument, "source_volume_id is required")
	}
	volumeID, err := clustermeta.CanonicalVolumeID(req.GetSourceVolumeId())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "canonical source volume id: %v", err)
	}
	volume, err := s.repo.GetVolumeState(ctx, volumeID)
	if err != nil {
		if errors.Is(err, clustermeta.ErrNotFound) {
			return nil, status.Errorf(codes.NotFound, "source volume %q not found", volumeID)
		}
		return nil, status.Errorf(codes.Internal, "get source volume: %v", err)
	}
	spec, err := s.getVolumeSpec(ctx, volumeID)
	if err != nil {
		if errors.Is(err, clustermeta.ErrNotFound) {
			return nil, status.Errorf(codes.NotFound, "source volume spec %q not found", volumeID)
		}
		return nil, status.Errorf(codes.Internal, "get source volume spec: %v", err)
	}
	now := s.currentTime().UTC()
	snapshot, replay, err := s.createSnapshotRecord(ctx, volumeID, volume, spec, now, req)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "create snapshot record: %v", err)
	}
	if replay {
		return &adminv1.CreateSnapshotResponse{
			Cluster:        cluster,
			Operation:      &adminv1.OperationHandle{Accepted: false, Message: "snapshot already created for idempotency key"},
			SnapshotId:     snapshot.SnapshotID,
			SnapshotRootId: snapshot.SnapshotRootID,
		}, nil
	}
	pages, err := s.repo.ListCompatibleAllocationPages(ctx, volumeID, spec.ExtentPageBytes, spec.ChunkSizeBytes)
	if err != nil {
		if _, markErr := s.repo.MarkSnapshotState(ctx, snapshot.SnapshotID, clustermeta.SnapshotStateFailed, err.Error()); markErr != nil {
			return nil, status.Errorf(codes.Internal, "capture snapshot read view: %v; mark failed: %v", err, markErr)
		}
		return nil, status.Errorf(codes.Internal, "list source allocation pages: %v", err)
	}
	if err := s.repo.CaptureSnapshotAllocationPages(ctx, snapshot.SnapshotRootID, pages); err != nil {
		if _, markErr := s.repo.MarkSnapshotState(ctx, snapshot.SnapshotID, clustermeta.SnapshotStateFailed, err.Error()); markErr != nil {
			return nil, status.Errorf(codes.Internal, "capture snapshot read view: %v; mark failed: %v", err, markErr)
		}
		return nil, status.Errorf(codes.Internal, "capture snapshot read view: %v", err)
	}
	if snapshot, err = s.repo.MarkSnapshotState(ctx, snapshot.SnapshotID, clustermeta.SnapshotStateAvailable, ""); err != nil {
		return nil, status.Errorf(codes.Internal, "mark snapshot available: %v", err)
	}
	op, err := s.ops.create("snapshot.create", "", volumeID, "read-view-captured", adminv1.OperationState_OPERATION_STATE_COMPLETED)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "create operation: %v", err)
	}
	return &adminv1.CreateSnapshotResponse{
		Cluster: cluster,
		Operation: &adminv1.OperationHandle{
			Accepted:    true,
			OperationId: op.GetOperationId(),
			Message:     "snapshot read view captured",
		},
		SnapshotId:     snapshot.SnapshotID,
		SnapshotRootId: snapshot.SnapshotRootID,
	}, nil
}

func (s *server) createSnapshotRecord(ctx context.Context, volumeID string, volume clustermeta.VolumeState, spec volumeSpecRecord, now time.Time, req *adminv1.CreateSnapshotRequest) (clustermeta.SnapshotRecord, bool, error) {
	var lastErr error
	idempotencyKey := strings.TrimSpace(req.GetIdempotencyKey())
	for attempt := 0; attempt < 4; attempt++ {
		snapshotIDTime := now.Add(time.Duration(attempt) * time.Nanosecond)
		snapshotID := generateSnapshotID(volumeID, snapshotIDTime)
		snapshot, replay, err := s.repo.CreateSnapshotRecord(ctx, clustermeta.SnapshotRecord{
			SnapshotID:               snapshotID,
			SourceVolumeID:           volumeID,
			SnapshotRootID:           snapshotID,
			State:                    clustermeta.SnapshotStateCreating,
			CreatedBy:                req.GetMeta().GetActor(),
			CreatedReason:            req.GetMeta().GetReason(),
			CreatedAtUnix:            now.Unix(),
			UpdatedAtUnix:            now.Unix(),
			CutVolumeRevision:        volume.Revision,
			AllocationChunkSizeBytes: spec.ChunkSizeBytes,
			AllocationPageSizeBytes:  spec.ExtentPageBytes,
			SourceSizeBytes:          spec.SizeBytes,
			IdempotencyKey:           idempotencyKey,
		})
		if err == nil {
			return snapshot, replay, nil
		}
		if !errors.Is(err, clustermeta.ErrCASConflict) {
			return clustermeta.SnapshotRecord{}, false, err
		}
		lastErr = err
	}
	return clustermeta.SnapshotRecord{}, false, lastErr
}

func (s *server) GetSnapshot(ctx context.Context, req *adminv1.GetSnapshotRequest) (*adminv1.GetSnapshotResponse, error) {
	cluster, err := s.clusterRef(req.GetCluster())
	if err != nil {
		return nil, err
	}
	if strings.TrimSpace(req.GetSnapshotId()) == "" {
		return nil, status.Error(codes.InvalidArgument, "snapshot_id is required")
	}
	snapshot, err := s.repo.GetSnapshotRecord(ctx, strings.TrimSpace(req.GetSnapshotId()))
	if err != nil {
		if errors.Is(err, clustermeta.ErrNotFound) {
			return nil, status.Errorf(codes.NotFound, "snapshot %q not found", strings.TrimSpace(req.GetSnapshotId()))
		}
		return nil, status.Errorf(codes.Internal, "get snapshot: %v", err)
	}
	return &adminv1.GetSnapshotResponse{
		Cluster:  cluster,
		Snapshot: snapshotRecordToProto(snapshot),
	}, nil
}

func (s *server) ListSnapshots(ctx context.Context, req *adminv1.ListSnapshotsRequest) (*adminv1.ListSnapshotsResponse, error) {
	cluster, err := s.clusterRef(req.GetCluster())
	if err != nil {
		return nil, err
	}
	sourceVolumeID := ""
	if req.GetSourceVolumeId() != "" {
		volumeID, err := clustermeta.CanonicalVolumeID(req.GetSourceVolumeId())
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "canonical source volume id: %v", err)
		}
		sourceVolumeID = volumeID
	}
	snapshots, err := s.repo.ListSnapshotRecords(ctx, sourceVolumeID, req.GetIncludeDeleted())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "list snapshots: %v", err)
	}
	out := make([]*adminv1.SnapshotSummary, 0, len(snapshots))
	for _, snapshot := range snapshots {
		out = append(out, snapshotRecordToProto(snapshot))
	}
	return &adminv1.ListSnapshotsResponse{
		Cluster:   cluster,
		Snapshots: out,
	}, nil
}

func (s *server) DeleteSnapshot(ctx context.Context, req *adminv1.DeleteSnapshotRequest) (*adminv1.DeleteSnapshotResponse, error) {
	if err := s.requireLeader(ctx); err != nil {
		return nil, status.Error(codes.Unavailable, err.Error())
	}
	cluster, err := s.clusterRef(req.GetCluster())
	if err != nil {
		return nil, err
	}
	if strings.TrimSpace(req.GetSnapshotId()) == "" {
		return nil, status.Error(codes.InvalidArgument, "snapshot_id is required")
	}
	snapshotID := strings.TrimSpace(req.GetSnapshotId())
	snapshot, err := s.repo.GetSnapshotRecord(ctx, snapshotID)
	if err != nil {
		if errors.Is(err, clustermeta.ErrNotFound) {
			return nil, status.Errorf(codes.NotFound, "snapshot %q not found", snapshotID)
		}
		return nil, status.Errorf(codes.Internal, "get snapshot: %v", err)
	}
	if snapshot.CloneReferenceCount > 0 {
		return nil, status.Errorf(codes.FailedPrecondition, "snapshot %q is referenced by %d clone(s)", snapshotID, snapshot.CloneReferenceCount)
	}
	if snapshot.State != clustermeta.SnapshotStateDeleted {
		if _, err := s.repo.MarkSnapshotState(ctx, snapshotID, clustermeta.SnapshotStateDeleted, ""); err != nil {
			return nil, status.Errorf(codes.Internal, "mark snapshot deleted: %v", err)
		}
	}
	op, err := s.ops.create("snapshot.delete", "", snapshot.SourceVolumeID, "deleted", adminv1.OperationState_OPERATION_STATE_COMPLETED)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "create operation: %v", err)
	}
	return &adminv1.DeleteSnapshotResponse{
		Cluster: cluster,
		Operation: &adminv1.OperationHandle{
			Accepted:    true,
			OperationId: op.GetOperationId(),
			Message:     "snapshot deleted",
		},
	}, nil
}

func (s *server) CreateClone(ctx context.Context, req *adminv1.CreateCloneRequest) (*adminv1.CreateCloneResponse, error) {
	if err := s.requireLeader(ctx); err != nil {
		return nil, status.Error(codes.Unavailable, err.Error())
	}
	cluster, err := s.clusterRef(req.GetCluster())
	if err != nil {
		return nil, err
	}
	sourceSnapshotID := strings.TrimSpace(req.GetSourceSnapshotId())
	if sourceSnapshotID == "" {
		return nil, status.Error(codes.InvalidArgument, "source_snapshot_id is required")
	}
	sourceSnapshot, err := s.repo.GetSnapshotRecord(ctx, sourceSnapshotID)
	if err != nil {
		if errors.Is(err, clustermeta.ErrNotFound) {
			return nil, status.Errorf(codes.NotFound, "source snapshot %q not found", sourceSnapshotID)
		}
		return nil, status.Errorf(codes.Internal, "get source snapshot: %v", err)
	}
	if _, err := s.getVolumeSpec(ctx, sourceSnapshot.SourceVolumeID); err != nil && !errors.Is(err, clustermeta.ErrNotFound) {
		return nil, status.Errorf(codes.Internal, "get source volume spec: %v", err)
	}
	now := s.currentTime().UTC()
	cloneID := strings.TrimSpace(req.GetCloneId())
	if cloneID == "" {
		cloneID = generateCloneID(sourceSnapshotID, now)
	}
	clone, replay, err := s.repo.CreateCloneRecord(ctx, clustermeta.CloneRecord{
		CloneID:          cloneID,
		SourceSnapshotID: sourceSnapshotID,
		State:            clustermeta.CloneStateAvailable,
		CreatedBy:        req.GetMeta().GetActor(),
		CreatedReason:    req.GetMeta().GetReason(),
		CreatedAtUnix:    now.Unix(),
		UpdatedAtUnix:    now.Unix(),
		SizeBytes:        req.GetSizeBytes(),
		IdempotencyKey:   strings.TrimSpace(req.GetIdempotencyKey()),
	})
	if err != nil {
		if errors.Is(err, clustermeta.ErrNotFound) {
			return nil, status.Errorf(codes.NotFound, "source snapshot %q not found", sourceSnapshotID)
		}
		return nil, status.Errorf(codes.Internal, "create clone record: %v", err)
	}
	if replay {
		return &adminv1.CreateCloneResponse{
			Cluster:         cluster,
			Operation:       &adminv1.OperationHandle{Accepted: false, Message: "clone already created for idempotency key"},
			CloneId:         clone.CloneID,
			CloneBaseRootId: clone.CloneBaseRootID,
		}, nil
	}
	op, err := s.ops.create("clone.create", "", clone.SourceVolumeID, "clone-created", adminv1.OperationState_OPERATION_STATE_COMPLETED)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "create operation: %v", err)
	}
	return &adminv1.CreateCloneResponse{
		Cluster: cluster,
		Operation: &adminv1.OperationHandle{
			Accepted:    true,
			OperationId: op.GetOperationId(),
			Message:     "clone created",
		},
		CloneId:         clone.CloneID,
		CloneBaseRootId: clone.CloneBaseRootID,
	}, nil
}

func (s *server) GetClone(ctx context.Context, req *adminv1.GetCloneRequest) (*adminv1.GetCloneResponse, error) {
	cluster, err := s.clusterRef(req.GetCluster())
	if err != nil {
		return nil, err
	}
	cloneID := strings.TrimSpace(req.GetCloneId())
	if cloneID == "" {
		return nil, status.Error(codes.InvalidArgument, "clone_id is required")
	}
	clone, err := s.repo.GetCloneRecord(ctx, cloneID)
	if err != nil {
		if errors.Is(err, clustermeta.ErrNotFound) {
			return nil, status.Errorf(codes.NotFound, "clone %q not found", cloneID)
		}
		return nil, status.Errorf(codes.Internal, "get clone: %v", err)
	}
	return &adminv1.GetCloneResponse{
		Cluster: cluster,
		Clone:   cloneRecordToProto(clone),
	}, nil
}

func (s *server) ListClones(ctx context.Context, req *adminv1.ListClonesRequest) (*adminv1.ListClonesResponse, error) {
	cluster, err := s.clusterRef(req.GetCluster())
	if err != nil {
		return nil, err
	}
	sourceVolumeID := strings.TrimSpace(req.GetSourceVolumeId())
	if sourceVolumeID != "" {
		sourceVolumeID, err = clustermeta.CanonicalVolumeID(sourceVolumeID)
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "canonical source volume id: %v", err)
		}
	}
	clones, err := s.repo.ListCloneRecords(ctx, strings.TrimSpace(req.GetSourceSnapshotId()), sourceVolumeID, req.GetIncludeDeleted())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "list clones: %v", err)
	}
	out := make([]*adminv1.CloneSummary, 0, len(clones))
	for _, clone := range clones {
		out = append(out, cloneRecordToProto(clone))
	}
	return &adminv1.ListClonesResponse{
		Cluster: cluster,
		Clones:  out,
	}, nil
}

func (s *server) MaterializeClone(ctx context.Context, req *adminv1.MaterializeCloneRequest) (*adminv1.MaterializeCloneResponse, error) {
	if err := s.requireLeader(ctx); err != nil {
		return nil, status.Error(codes.Unavailable, err.Error())
	}
	cluster, err := s.clusterRef(req.GetCluster())
	if err != nil {
		return nil, err
	}
	cloneID := strings.TrimSpace(req.GetCloneId())
	if cloneID == "" {
		return nil, status.Error(codes.InvalidArgument, "clone_id is required")
	}
	targetVolumeID := strings.TrimSpace(req.GetTargetVolumeId())
	if targetVolumeID != "" {
		targetVolumeID, err = clustermeta.CanonicalVolumeID(targetVolumeID)
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "canonical target volume id: %v", err)
		}
	}
	clone, err := s.repo.GetCloneRecord(ctx, cloneID)
	if err != nil {
		if errors.Is(err, clustermeta.ErrNotFound) {
			return nil, status.Errorf(codes.NotFound, "clone %q not found", cloneID)
		}
		return nil, status.Errorf(codes.Internal, "get clone: %v", err)
	}
	if clone.State == clustermeta.CloneStateDeleted {
		return nil, status.Errorf(codes.FailedPrecondition, "clone %q is deleted", cloneID)
	}
	if clone.State == clustermeta.CloneStateFailed {
		return nil, status.Errorf(codes.FailedPrecondition, "clone %q is failed: %s", cloneID, clone.ErrorMessage)
	}
	if clone.State == clustermeta.CloneStateMaterializing {
		return nil, status.Errorf(codes.FailedPrecondition, "clone %q is already materializing", cloneID)
	}
	if clone.State == clustermeta.CloneStateMaterialized {
		return &adminv1.MaterializeCloneResponse{
			Cluster: cluster,
			Operation: &adminv1.OperationHandle{
				Accepted: false,
				Message:  "clone already materialized",
			},
			CloneId:              clone.CloneID,
			MaterializedVolumeId: clone.MaterializedVolumeID,
		}, nil
	}
	sourceSpec, err := s.getVolumeSpec(ctx, clone.SourceVolumeID)
	if err != nil {
		if errors.Is(err, clustermeta.ErrNotFound) {
			return nil, status.Errorf(codes.FailedPrecondition, "source volume %q spec is missing", clone.SourceVolumeID)
		}
		return nil, status.Errorf(codes.Internal, "get source volume spec: %v", err)
	}
	if targetVolumeID == "" {
		targetVolumeID, err = s.generateMaterializedVolumeID(ctx, clone.CloneID, clone.SourceVolumeID, s.currentTime().UTC())
		if err != nil {
			return nil, status.Errorf(codes.Internal, "generate target volume id: %v", err)
		}
	} else if _, err := s.repo.GetVolumeState(ctx, targetVolumeID); err == nil {
		return nil, status.Errorf(codes.AlreadyExists, "target volume %q already exists", targetVolumeID)
	} else if !errors.Is(err, clustermeta.ErrNotFound) {
		return nil, status.Errorf(codes.Internal, "get target volume: %v", err)
	}

	op, err := s.ops.create("clone.materialize", "", clone.SourceVolumeID, "materializing", adminv1.OperationState_OPERATION_STATE_RUNNING)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "create operation: %v", err)
	}
	failMaterialize := func(format string, args ...any) (*adminv1.MaterializeCloneResponse, error) {
		err := fmt.Errorf(format, args...)
		s.failOperation(op.GetOperationId(), err)
		_ = s.deleteVolumeArtifacts(context.Background(), targetVolumeID)
		if ctxErr := ctx.Err(); ctxErr != nil {
			_, _ = s.repo.MarkCloneState(context.Background(), clone.CloneID, clustermeta.CloneStateAvailable, "")
			return nil, status.Error(materializeContextStatusCode(ctxErr), err.Error())
		}
		_, _ = s.repo.MarkCloneState(context.Background(), clone.CloneID, clustermeta.CloneStateFailed, err.Error())
		return nil, status.Error(codes.Internal, err.Error())
	}
	if _, err := s.repo.MarkCloneState(ctx, clone.CloneID, clustermeta.CloneStateMaterializing, ""); err != nil {
		s.failOperation(op.GetOperationId(), err)
		return nil, status.Errorf(codes.Internal, "mark clone materializing: %v", err)
	}

	createResp, err := s.CreateVolume(ctx, materializedCloneCreateVolumeRequest(req, clone, sourceSpec, targetVolumeID))
	if err != nil {
		return failMaterialize("create materialized target volume: %v", err)
	}
	if !createResp.GetOperation().GetAccepted() {
		return failMaterialize("create materialized target volume was not accepted")
	}
	targetSpec, err := s.getVolumeSpec(ctx, targetVolumeID)
	if err != nil {
		return failMaterialize("get target volume spec: %v", err)
	}
	if err := s.copyCloneIntoMaterializedVolume(ctx, clone, sourceSpec, targetSpec); err != nil {
		return failMaterialize("copy clone data into target volume: %v", err)
	}
	materialized, err := s.repo.MarkCloneMaterialized(ctx, clone.CloneID, targetVolumeID)
	if err != nil {
		return failMaterialize("mark clone materialized: %v", err)
	}
	op, _ = s.ops.update(op.GetOperationId(), func(op *adminv1.OperationStatus) {
		op.State = adminv1.OperationState_OPERATION_STATE_COMPLETED
		op.Phase = "materialized"
		op.TargetVolumeId = targetVolumeID
	})
	return &adminv1.MaterializeCloneResponse{
		Cluster: cluster,
		Operation: &adminv1.OperationHandle{
			Accepted:    true,
			OperationId: op.GetOperationId(),
			Message:     "clone materialized",
		},
		CloneId:              materialized.CloneID,
		MaterializedVolumeId: materialized.MaterializedVolumeID,
	}, nil
}

func (s *server) generateMaterializedVolumeID(ctx context.Context, cloneID, sourceVolumeID string, now time.Time) (string, error) {
	for attempt := 0; attempt < 32; attempt++ {
		input := fmt.Sprintf("%s-%d-%d", cloneID, now.UnixNano(), attempt)
		candidate := fmt.Sprintf("%08x", crc32.ChecksumIEEE([]byte(input)))
		if candidate == sourceVolumeID {
			continue
		}
		if _, err := s.repo.GetVolumeState(ctx, candidate); errors.Is(err, clustermeta.ErrNotFound) {
			return candidate, nil
		} else if err != nil {
			return "", err
		}
	}
	return "", fmt.Errorf("could not allocate unique materialized volume id")
}

func createVolumeFromSnapshotIdempotencyKey(snapshotID, volumeID string) string {
	return fmt.Sprintf("restore-volume:%s:%s", strings.TrimSpace(snapshotID), strings.TrimSpace(volumeID))
}

func materializedCloneCreateVolumeRequest(req *adminv1.MaterializeCloneRequest, clone clustermeta.CloneRecord, sourceSpec volumeSpecRecord, targetVolumeID string) *adminv1.CreateVolumeRequest {
	return &adminv1.CreateVolumeRequest{
		Cluster:                  req.GetCluster(),
		Meta:                     req.GetMeta(),
		VolumeId:                 targetVolumeID,
		SizeBytes:                clone.SizeBytes,
		BlockSize:                sourceSpec.BlockSize,
		ReplicationFactor:        sourceSpec.ReplicationFactor,
		PolicyName:               sourceSpec.PolicyName,
		ExtentSizeBytes:          sourceSpec.ExtentSizeBytes,
		AllocationChunkSizeBytes: clone.AllocationChunkSizeBytes,
		AllocationPageSizeBytes:  clone.AllocationPageSizeBytes,
		TopologyMode:             sourceSpec.TopologyMode,
		RedundancyBackend:        sourceSpec.RedundancyBackend,
		EcProfileId:              sourceSpec.ECProfileID,
		WeakPlacementAllowed:     sourceSpec.WeakPlacementAllowed,
	}
}

func (s *server) copyCloneIntoMaterializedVolume(ctx context.Context, clone clustermeta.CloneRecord, sourceSpec, targetSpec volumeSpecRecord) error {
	sourceReader, err := s.newMaterializeReadViewService(ctx, sourceSpec, "materialize-src-"+targetSpec.VolumeID)
	if err != nil {
		return fmt.Errorf("source read-view: %w", err)
	}
	targetClient, err := s.newMaterializeClusterClient(ctx, targetSpec, "materialize-tgt-"+targetSpec.VolumeID)
	if err != nil {
		return fmt.Errorf("target client: %w", err)
	}
	targetSession := "materialize-tgt-" + targetSpec.VolumeID
	targetAttachment := "att-" + targetSpec.VolumeID + "-materialize"
	targetOpen, err := targetClient.OpenVolume(ctx, &service.OpenVolumeRequest{
		VolumeID:   targetSpec.VolumeID,
		AccessMode: service.SBSAccessModeExclusiveWriter,
		Context: materializeRequestContext(
			"open-target",
			clone.CloneID,
			targetSession,
			targetAttachment,
			1,
			"",
		),
	})
	if err != nil {
		return fmt.Errorf("open target volume %s: %w", targetSpec.VolumeID, err)
	}
	defer func() {
		_, _ = targetClient.CloseVolume(context.Background(), &service.CloseVolumeRequest{
			VolumeID:     targetSpec.VolumeID,
			VolumeHandle: targetOpen.VolumeHandle,
			Context: materializeRequestContext(
				"close-target",
				clone.CloneID,
				targetSession,
				targetAttachment,
				1,
				"",
			),
		})
	}()

	copyRanges, err := s.cloneMaterializeCopyRanges(ctx, clone)
	if err != nil {
		return err
	}
	structuredlog.Info("sbs.restore", "materialize_copy_plan",
		structuredlog.F("clone_id", clone.CloneID),
		structuredlog.F("target_volume_id", targetSpec.VolumeID),
		structuredlog.F("copy_range_count", len(copyRanges)),
		structuredlog.F("source_size_bytes", clone.SourceSizeBytes),
		structuredlog.F("target_size_bytes", clone.SizeBytes),
	)
	const maxCopyWindowBytes uint64 = 4 * 1024 * 1024
	for _, copyRange := range copyRanges {
		for offset := copyRange.OffsetBytes; offset < copyRange.OffsetBytes+copyRange.LengthBytes; offset += maxCopyWindowBytes {
			length := minUint64(maxCopyWindowBytes, copyRange.OffsetBytes+copyRange.LengthBytes-offset)
			readResp, err := sourceReader.ReadClone(ctx, clone.CloneID, clusterreplication.ReadRequest{
				RequestID:      fmt.Sprintf("materialize-read-%s-%020d", clone.CloneID, offset),
				VolumeID:       clone.SourceVolumeID,
				OffsetBytes:    offset,
				LengthBytes:    length,
				PageBytes:      sourceSpec.ExtentPageBytes,
				ChunkSizeBytes: sourceSpec.ChunkSizeBytes,
			})
			if err != nil {
				return fmt.Errorf("read clone offset=%d length=%d: %w", offset, length, err)
			}
			if allZeroBytes(readResp.Data) {
				continue
			}
			if _, err := targetClient.Write(ctx, &service.WriteRequest{
				VolumeID:     targetSpec.VolumeID,
				VolumeHandle: targetOpen.VolumeHandle,
				OffsetBytes:  offset,
				LengthBytes:  uint64(len(readResp.Data)),
				Data:         readResp.Data,
				Context: materializeRequestContext(
					fmt.Sprintf("write-%020d", offset),
					clone.CloneID,
					targetSession,
					targetAttachment,
					1,
					fmt.Sprintf("materialize-%s-%020d", clone.CloneID, offset),
				),
			}); err != nil {
				return fmt.Errorf("write target offset=%d length=%d: %w", offset, len(readResp.Data), err)
			}
		}
	}
	return nil
}

type materializeCopyRange struct {
	OffsetBytes uint64
	LengthBytes uint64
}

func (s *server) cloneMaterializeCopyRanges(ctx context.Context, clone clustermeta.CloneRecord) ([]materializeCopyRange, error) {
	basePages, err := s.repo.ListSnapshotAllocationPages(ctx, clone.CloneBaseRootID)
	if err != nil {
		return nil, fmt.Errorf("list snapshot allocation pages: %w", err)
	}
	deltaPages, err := s.repo.ListCloneDeltaAllocationPages(ctx, clone.CloneID)
	if err != nil {
		return nil, fmt.Errorf("list clone delta allocation pages: %w", err)
	}
	return cloneMaterializeCopyRangesFromPages(clone, basePages, deltaPages)
}

func cloneMaterializeCopyRangesFromPages(clone clustermeta.CloneRecord, basePages, deltaPages []clustermeta.AllocationPageRecord) ([]materializeCopyRange, error) {
	if clone.AllocationChunkSizeBytes == 0 || clone.AllocationPageSizeBytes == 0 {
		return nil, fmt.Errorf("clone allocation geometry is missing: clone_id=%s page_bytes=%d chunk_size_bytes=%d",
			clone.CloneID, clone.AllocationPageSizeBytes, clone.AllocationChunkSizeBytes)
	}
	effectivePages := make(map[uint64]clustermeta.AllocationPageRecord, len(basePages)+len(deltaPages))
	for _, page := range basePages {
		if err := validateMaterializeAllocationPage(clone, page); err != nil {
			return nil, err
		}
		effectivePages[page.PageNo] = page
	}
	for _, page := range deltaPages {
		if err := validateMaterializeAllocationPage(clone, page); err != nil {
			return nil, err
		}
		effectivePages[page.PageNo] = page
	}
	if len(effectivePages) == 0 {
		return nil, nil
	}
	pageNos := make([]uint64, 0, len(effectivePages))
	for pageNo := range effectivePages {
		pageNos = append(pageNos, pageNo)
	}
	sort.Slice(pageNos, func(i, j int) bool { return pageNos[i] < pageNos[j] })

	sourceLimit := minUint64(clone.SourceSizeBytes, clone.SizeBytes)
	chunkSize := uint64(clone.AllocationChunkSizeBytes)
	ranges := make([]materializeCopyRange, 0)
	for _, pageNo := range pageNos {
		page := effectivePages[pageNo]
		extents := append([]clustermeta.AllocationExtentRecord(nil), page.Extents...)
		sort.Slice(extents, func(i, j int) bool { return extents[i].LogicalChunkStart < extents[j].LogicalChunkStart })
		for _, extent := range extents {
			if extent.Kind != clustermeta.AllocationKindData && extent.Kind != clustermeta.AllocationKindShared {
				continue
			}
			offset := extent.LogicalChunkStart * chunkSize
			if offset >= sourceLimit {
				continue
			}
			length := uint64(extent.ChunkCount) * chunkSize
			if offset+length > sourceLimit {
				length = sourceLimit - offset
			}
			ranges = appendMaterializeCopyRange(ranges, materializeCopyRange{
				OffsetBytes: offset,
				LengthBytes: length,
			})
		}
	}
	return ranges, nil
}

func validateMaterializeAllocationPage(clone clustermeta.CloneRecord, page clustermeta.AllocationPageRecord) error {
	if page.PageBytes != clone.AllocationPageSizeBytes || page.ChunkSizeBytes != clone.AllocationChunkSizeBytes {
		return fmt.Errorf("clone %q materialize page geometry mismatch: page_no=%d page_bytes=%d chunk_size_bytes=%d expected_page_bytes=%d expected_chunk_size_bytes=%d",
			clone.CloneID, page.PageNo, page.PageBytes, page.ChunkSizeBytes, clone.AllocationPageSizeBytes, clone.AllocationChunkSizeBytes)
	}
	if page.ChunkSizeBytes == 0 || page.PageBytes%page.ChunkSizeBytes != 0 {
		return fmt.Errorf("clone %q materialize page invalid geometry: page_no=%d page_bytes=%d chunk_size_bytes=%d",
			clone.CloneID, page.PageNo, page.PageBytes, page.ChunkSizeBytes)
	}
	chunksPerPage := uint64(page.PageBytes / page.ChunkSizeBytes)
	pageStartChunk := page.PageNo * chunksPerPage
	pageEndChunk := pageStartChunk + chunksPerPage
	for _, extent := range page.Extents {
		if extent.ChunkCount == 0 {
			return fmt.Errorf("clone %q materialize page %d has zero chunk_count", clone.CloneID, page.PageNo)
		}
		extentEndChunk := extent.LogicalChunkStart + uint64(extent.ChunkCount)
		if extentEndChunk < extent.LogicalChunkStart || extent.LogicalChunkStart < pageStartChunk || extentEndChunk > pageEndChunk {
			return fmt.Errorf("clone %q materialize page %d extent out of page bounds: start=%d end=%d page_start=%d page_end=%d",
				clone.CloneID, page.PageNo, extent.LogicalChunkStart, extentEndChunk, pageStartChunk, pageEndChunk)
		}
		switch extent.Kind {
		case clustermeta.AllocationKindZero, clustermeta.AllocationKindData, clustermeta.AllocationKindShared:
		default:
			return fmt.Errorf("clone %q materialize page %d has unsupported allocation kind %q", clone.CloneID, page.PageNo, extent.Kind)
		}
	}
	return nil
}

func appendMaterializeCopyRange(ranges []materializeCopyRange, next materializeCopyRange) []materializeCopyRange {
	if next.LengthBytes == 0 {
		return ranges
	}
	if len(ranges) == 0 {
		return append(ranges, next)
	}
	prev := &ranges[len(ranges)-1]
	if prev.OffsetBytes+prev.LengthBytes == next.OffsetBytes {
		prev.LengthBytes += next.LengthBytes
		return ranges
	}
	return append(ranges, next)
}

func materializeContextStatusCode(err error) codes.Code {
	if errors.Is(err, context.DeadlineExceeded) {
		return codes.DeadlineExceeded
	}
	if errors.Is(err, context.Canceled) {
		return codes.Canceled
	}
	return codes.Internal
}

type materializeReadViewReader interface {
	Read(ctx context.Context, req clusterreplication.ReadRequest) (*clusterreplication.ReadResponse, error)
	ReadClone(ctx context.Context, cloneID string, req clusterreplication.ReadRequest) (*clusterreplication.ReadResponse, error)
}

type materializeECReadViewService struct {
	volume        service.VolumeSpec
	reader        *clusterec.Service
	resolver      *clustermeta.Service
	sessionPrefix string
	hostID        string
}

func (s *server) newMaterializeReadViewService(ctx context.Context, spec volumeSpecRecord, sessionPrefix string) (materializeReadViewReader, error) {
	if strings.TrimSpace(spec.RedundancyBackend) == clustermeta.RedundancyBackendEC {
		return s.newMaterializeECReadViewService(ctx, spec, sessionPrefix)
	}
	replicaClients, err := s.buildReplicaClientMap(ctx, spec.VolumeID)
	if err != nil {
		return nil, err
	}
	replicas := make(map[string]clusterreplication.RemoteReplica, len(replicaClients))
	for replicaID, client := range replicaClients {
		replicas[replicaID] = clusterreplication.RemoteReplica{
			ReplicaID: replicaID,
			Client:    client,
			VolumeID:  spec.VolumeID,
			GatewayID: "sbs-service",
			HostID:    s.nodeID,
			SessionID: fmt.Sprintf("%s-%s", sessionPrefix, replicaID),
		}
	}
	resolver := clustermeta.NewService(s.repo)
	coordinator := clusterreplication.NewCoordinator(resolver, resolver)
	return clusterreplication.NewReadService(coordinator, clusterreplication.NewRemoteReplicaReader(replicas)), nil
}

func (s *server) newMaterializeECReadViewService(ctx context.Context, spec volumeSpecRecord, sessionPrefix string) (materializeReadViewReader, error) {
	replicaClients, err := s.buildReplicaClientMap(ctx, spec.VolumeID)
	if err != nil {
		return nil, err
	}
	sessions := make(map[string]clusterec.ShardSession, len(replicaClients))
	for nodeID, client := range replicaClients {
		nodeID = strings.TrimSpace(nodeID)
		if nodeID == "" {
			continue
		}
		sessions[nodeID] = clusterec.ShardSession{
			NodeID:    nodeID,
			Client:    client,
			GatewayID: "sbs-service",
			HostID:    s.nodeID,
			SessionID: fmt.Sprintf("%s-%s", sessionPrefix, nodeID),
		}
	}
	if len(sessions) == 0 {
		return nil, fmt.Errorf("no EC shard clients available for materialize read-view volume=%s", spec.VolumeID)
	}
	return &materializeECReadViewService{
		volume:        serviceSpecFromVolumeSpecRecord(spec),
		reader:        clusterec.NewService(s.repo, sessions),
		resolver:      clustermeta.NewService(s.repo),
		sessionPrefix: sessionPrefix,
		hostID:        s.nodeID,
	}, nil
}

func (r *materializeECReadViewService) Read(ctx context.Context, req clusterreplication.ReadRequest) (*clusterreplication.ReadResponse, error) {
	pageBytes := req.PageBytes
	if pageBytes == 0 {
		pageBytes = r.volume.ExtentPageBytes
	}
	chunkSizeBytes := req.ChunkSizeBytes
	if chunkSizeBytes == 0 {
		chunkSizeBytes = r.volume.ChunkSizeBytes
	}
	allocationPages, err := r.resolver.ResolveAllocationPages(ctx, req.VolumeID, req.OffsetBytes, req.LengthBytes, pageBytes, chunkSizeBytes)
	if err != nil {
		return nil, err
	}
	requestID := strings.TrimSpace(req.RequestID)
	if requestID == "" {
		requestID = fmt.Sprintf("materialize-ec-read-%s-%020d", req.VolumeID, req.OffsetBytes)
	}
	sessionID := strings.TrimSpace(r.sessionPrefix)
	if sessionID == "" {
		sessionID = "materialize-ec-read"
	}
	resp, err := r.reader.ReadFromAllocationPages(ctx, clusterec.ReadRequest{
		Volume: r.volume,
		Context: service.SBSRequestContext{
			RequestID: requestID,
			GatewayID: "sbs-service",
			HostID:    r.hostID,
			SessionID: sessionID,
		},
		Offset: req.OffsetBytes,
		Length: req.LengthBytes,
	}, allocationPages)
	if err != nil {
		return nil, err
	}
	return &clusterreplication.ReadResponse{
		VolumeID: req.VolumeID,
		Data:     resp.Data,
	}, nil
}

func (r *materializeECReadViewService) ReadClone(ctx context.Context, cloneID string, req clusterreplication.ReadRequest) (*clusterreplication.ReadResponse, error) {
	cloneID = strings.TrimSpace(cloneID)
	if cloneID == "" {
		return nil, fmt.Errorf("clone_id is required")
	}
	pageBytes := req.PageBytes
	if pageBytes == 0 {
		pageBytes = r.volume.ExtentPageBytes
	}
	chunkSizeBytes := req.ChunkSizeBytes
	if chunkSizeBytes == 0 {
		chunkSizeBytes = r.volume.ChunkSizeBytes
	}
	allocationPages, err := r.resolver.ResolveCloneAllocationPages(ctx, cloneID, req.OffsetBytes, req.LengthBytes, pageBytes, chunkSizeBytes)
	if err != nil {
		return nil, err
	}
	requestID := strings.TrimSpace(req.RequestID)
	if requestID == "" {
		requestID = fmt.Sprintf("materialize-ec-read-%s-%020d", cloneID, req.OffsetBytes)
	}
	sessionID := strings.TrimSpace(r.sessionPrefix)
	if sessionID == "" {
		sessionID = "materialize-ec-read"
	}
	resp, err := r.reader.ReadFromAllocationPages(ctx, clusterec.ReadRequest{
		Volume: r.volume,
		Context: service.SBSRequestContext{
			RequestID: requestID,
			GatewayID: "sbs-service",
			HostID:    r.hostID,
			SessionID: sessionID,
		},
		Offset: req.OffsetBytes,
		Length: req.LengthBytes,
	}, allocationPages)
	if err != nil {
		return nil, err
	}
	return &clusterreplication.ReadResponse{
		VolumeID: req.VolumeID,
		CloneID:  cloneID,
		Data:     resp.Data,
	}, nil
}

func (s *server) newMaterializeClusterClient(ctx context.Context, spec volumeSpecRecord, sessionPrefix string) (*sbscluster.Client, error) {
	replicaClients, err := s.buildReplicaClientMap(ctx, spec.VolumeID)
	if err != nil {
		return nil, err
	}
	resolver := clustermeta.NewService(s.repo)
	return sbscluster.NewClient(sbscluster.Config{
		MetadataWriteSessionStore:           s.repo,
		MetadataChunkIDSequenceStore:        s.repo,
		MetadataAllocationPersistStore:      s.repo,
		MetadataExtentMappingNormalizeStore: s.repo,
		MetadataExtentMappingResolver:       s.repo,
		MetadataReplicaSetResolver:          s.repo,
		MetadataNodeMembershipResolver:      s.repo,
		MetadataAllocationPageReader:        s.repo,
		MetadataAllocationPageLister:        s.repo,
		MetadataResolvedAllocationResolver:  resolver,
		MetadataCloneDeltaCommitter:         resolver,
		VolumeSpecs:                         []service.VolumeSpec{serviceSpecFromVolumeSpecRecord(spec)},
		ReplicaClients:                      replicaClients,
		GatewayID:                           "sbs-service",
		HostID:                              s.nodeID,
		SessionPrefix:                       sessionPrefix,
	})
}

type clusterVolumeServiceProxy struct {
	srv *server

	mu      sync.Mutex
	clients map[string]*sbscluster.Client
}

func newClusterVolumeServiceProxy(srv *server) *clusterVolumeServiceProxy {
	return &clusterVolumeServiceProxy{
		srv:     srv,
		clients: make(map[string]*sbscluster.Client),
	}
}

func (p *clusterVolumeServiceProxy) OpenVolume(ctx context.Context, req *service.OpenVolumeRequest) (*service.OpenVolumeResponse, error) {
	if req == nil {
		return nil, sbsVolumeServiceBadRequest("nil open volume request")
	}
	client, err := p.newClient(ctx, req.VolumeID)
	if err != nil {
		return nil, err
	}
	key := clusterVolumeServiceSessionKey(req.VolumeID, req.Context)
	p.mu.Lock()
	p.clients[key] = client
	p.mu.Unlock()
	resp, err := client.OpenVolume(ctx, req)
	if err != nil {
		p.mu.Lock()
		delete(p.clients, key)
		p.mu.Unlock()
	}
	return resp, err
}

func (p *clusterVolumeServiceProxy) CloseVolume(ctx context.Context, req *service.CloseVolumeRequest) (*service.CloseVolumeResponse, error) {
	if req == nil {
		return nil, sbsVolumeServiceBadRequest("nil close volume request")
	}
	key := clusterVolumeServiceSessionKey(req.VolumeID, req.Context)
	client, err := p.existingClient(key)
	if err != nil {
		return nil, err
	}
	resp, err := client.CloseVolume(ctx, req)
	if err == nil {
		p.mu.Lock()
		delete(p.clients, key)
		p.mu.Unlock()
	}
	return resp, err
}

func (p *clusterVolumeServiceProxy) GetVolumeProfile(ctx context.Context, req *service.GetVolumeProfileRequest) (*service.GetVolumeProfileResponse, error) {
	if req == nil {
		return nil, sbsVolumeServiceBadRequest("nil get volume profile request")
	}
	client, err := p.newClient(ctx, req.VolumeID)
	if err != nil {
		return nil, err
	}
	return client.GetVolumeProfile(ctx, req)
}

func (p *clusterVolumeServiceProxy) GetVolumeStatus(ctx context.Context, req *service.GetVolumeStatusRequest) (*service.GetVolumeStatusResponse, error) {
	if req == nil {
		return nil, sbsVolumeServiceBadRequest("nil get volume status request")
	}
	client, err := p.newClient(ctx, req.VolumeID)
	if err != nil {
		return nil, err
	}
	return client.GetVolumeStatus(ctx, req)
}

func (p *clusterVolumeServiceProxy) Read(ctx context.Context, req *service.ReadRequest) (*service.ReadResponse, error) {
	if req == nil {
		return nil, sbsVolumeServiceBadRequest("nil read request")
	}
	client, err := p.existingClient(clusterVolumeServiceSessionKey(req.VolumeID, req.Context))
	if err != nil {
		return nil, err
	}
	resp, err := client.Read(ctx, req)
	if err != nil {
		structuredlog.Error("sbs.service", "volume_service_read_failed", err,
			structuredlog.F("request_id", req.Context.RequestID),
			structuredlog.F("trace_id", req.Context.TraceID),
			structuredlog.F("volume_id", req.VolumeID),
			structuredlog.F("attachment_id", req.Context.AttachmentID),
			structuredlog.F("generation", req.Context.Generation),
			structuredlog.F("offset_bytes", req.OffsetBytes),
			structuredlog.F("length_bytes", req.LengthBytes),
		)
		return nil, err
	}
	return resp, nil
}

func (p *clusterVolumeServiceProxy) Write(ctx context.Context, req *service.WriteRequest) (*service.WriteResponse, error) {
	if req == nil {
		return nil, sbsVolumeServiceBadRequest("nil write request")
	}
	client, err := p.existingClient(clusterVolumeServiceSessionKey(req.VolumeID, req.Context))
	if err != nil {
		return nil, err
	}
	return client.Write(ctx, req)
}

func (p *clusterVolumeServiceProxy) Flush(ctx context.Context, req *service.FlushRequest) (*service.FlushResponse, error) {
	if req == nil {
		return nil, sbsVolumeServiceBadRequest("nil flush request")
	}
	client, err := p.existingClient(clusterVolumeServiceSessionKey(req.VolumeID, req.Context))
	if err != nil {
		return nil, err
	}
	return client.Flush(ctx, req)
}

func (p *clusterVolumeServiceProxy) Discard(ctx context.Context, req *service.DiscardRequest) (*service.DiscardResponse, error) {
	if req == nil {
		return nil, sbsVolumeServiceBadRequest("nil discard request")
	}
	client, err := p.existingClient(clusterVolumeServiceSessionKey(req.VolumeID, req.Context))
	if err != nil {
		return nil, err
	}
	return client.Discard(ctx, req)
}

func (p *clusterVolumeServiceProxy) Zero(ctx context.Context, req *service.ZeroRequest) (*service.ZeroResponse, error) {
	if req == nil {
		return nil, sbsVolumeServiceBadRequest("nil zero request")
	}
	client, err := p.existingClient(clusterVolumeServiceSessionKey(req.VolumeID, req.Context))
	if err != nil {
		return nil, err
	}
	return client.Zero(ctx, req)
}

func (p *clusterVolumeServiceProxy) newClient(ctx context.Context, volumeID string) (*sbscluster.Client, error) {
	if volumeID == "" {
		return nil, sbsVolumeServiceBadRequest(service.ErrSBSVolumeIDRequired.Error())
	}
	if _, err := service.ParseVolumeID(volumeID); err != nil {
		return nil, sbsVolumeServiceBadRequest(service.ErrSBSVolumeIDInvalid.Error())
	}
	spec, err := p.srv.getVolumeSpec(ctx, volumeID)
	if err != nil {
		if errors.Is(err, clustermeta.ErrNotFound) {
			return nil, &service.SBSError{Code: service.SBSErrorCodeNotFound, Message: "volume not found"}
		}
		return nil, err
	}
	return p.srv.newMaterializeClusterClient(ctx, spec, "sbs-service-volume")
}

func (p *clusterVolumeServiceProxy) existingClient(key string) (*sbscluster.Client, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	client := p.clients[key]
	if client == nil {
		return nil, &service.SBSError{Code: service.SBSErrorCodeAttachmentMismatch, Message: "volume is not opened"}
	}
	return client, nil
}

func clusterVolumeServiceSessionKey(volumeID string, reqCtx service.SBSRequestContext) string {
	return strings.Join([]string{
		volumeID,
		reqCtx.GatewayID,
		reqCtx.AttachmentID,
		strconv.FormatUint(reqCtx.Generation, 10),
	}, "\x00")
}

func sbsVolumeServiceBadRequest(message string) error {
	return &service.SBSError{Code: service.SBSErrorCodeBadRequest, Message: message}
}

func serviceSpecFromVolumeSpecRecord(rec volumeSpecRecord) service.VolumeSpec {
	parsed, _ := service.ParseVolumeID(rec.VolumeID)
	return service.NormalizeVolumeSpec(service.VolumeSpec{
		ID:                             service.HexVolumeID(parsed),
		Name:                           rec.VolumeID,
		Prefix:                         "vol-" + rec.VolumeID,
		SizeBytes:                      rec.SizeBytes,
		BlockSize:                      rec.BlockSize,
		ChunkSizeBytes:                 rec.ChunkSizeBytes,
		ExtentPageBytes:                rec.ExtentPageBytes,
		RedundancyBackend:              rec.RedundancyBackend,
		TopologyMode:                   rec.TopologyMode,
		ECProfileID:                    rec.ECProfileID,
		ECCodecID:                      rec.ECCodecID,
		ECDataShards:                   rec.ECDataShards,
		ECParityShards:                 rec.ECParityShards,
		ECStripeUnitBytes:              rec.ECStripeUnitBytes,
		ECFailureDomain:                rec.ECFailureDomain,
		ECMaxUnavailableFailureDomains: rec.ECMaxUnavailableFailureDomains,
		ECMaxShardsPerFailureDomain:    rec.ECMaxShardsPerFailureDomain,
		WeakPlacementAllowed:           rec.WeakPlacementAllowed,
		ProtectedState:                 serviceProtectedStateFromVolumeSpecRecord(rec.ProtectedState),
	})
}

func serviceProtectedStateFromVolumeSpecRecord(rec *clustermeta.VolumeProtectedStateRecord) *service.VolumeProtectedState {
	if rec == nil {
		return nil
	}
	protectedState := service.VolumeProtectedState{
		State:            service.VolumeProtectedStateKind(strings.TrimSpace(rec.State)),
		ReasonCode:       strings.TrimSpace(rec.ReasonCode),
		SealedObjectID:   strings.TrimSpace(rec.SealedObjectID),
		SealOperationID:  strings.TrimSpace(rec.SealOperationID),
		PolicySnapshotID: strings.TrimSpace(rec.PolicySnapshotID),
		LifecycleState:   strings.TrimSpace(rec.LifecycleState),
		SourceVolumeID:   strings.TrimSpace(rec.SourceVolumeID),
	}.Normalize()
	if protectedState.IsZero() {
		return nil
	}
	return &protectedState
}

func protectedStateToProto(rec *clustermeta.VolumeProtectedStateRecord) *adminv1.VolumeProtectedState {
	protectedState := serviceProtectedStateFromVolumeSpecRecord(rec)
	if protectedState == nil {
		return nil
	}
	return &adminv1.VolumeProtectedState{
		State:            string(protectedState.State),
		ReasonCode:       protectedState.ReasonCode,
		SealedObjectId:   protectedState.SealedObjectID,
		SealOperationId:  protectedState.SealOperationID,
		PolicySnapshotId: protectedState.PolicySnapshotID,
		LifecycleState:   protectedState.LifecycleState,
		SourceVolumeId:   protectedState.SourceVolumeID,
	}
}

func materializeRequestContext(action, cloneID, sessionID, attachmentID string, generation uint64, idempotencyKey string) service.SBSRequestContext {
	requestID := fmt.Sprintf("%s-%s", action, cloneID)
	if len(requestID) > 120 {
		requestID = requestID[:120]
	}
	return service.SBSRequestContext{
		RequestID:      requestID,
		GatewayID:      "sbs-service",
		HostID:         "sbs-service",
		SessionID:      sessionID,
		AttachmentID:   attachmentID,
		Generation:     generation,
		IdempotencyKey: idempotencyKey,
		TraceID:        requestID,
	}
}

func allZeroBytes(data []byte) bool {
	for _, b := range data {
		if b != 0 {
			return false
		}
	}
	return true
}

func (s *server) DeleteClone(ctx context.Context, req *adminv1.DeleteCloneRequest) (*adminv1.DeleteCloneResponse, error) {
	if err := s.requireLeader(ctx); err != nil {
		return nil, status.Error(codes.Unavailable, err.Error())
	}
	cluster, err := s.clusterRef(req.GetCluster())
	if err != nil {
		return nil, err
	}
	cloneID := strings.TrimSpace(req.GetCloneId())
	if cloneID == "" {
		return nil, status.Error(codes.InvalidArgument, "clone_id is required")
	}
	clone, err := s.repo.DeleteCloneRecord(ctx, cloneID)
	if err != nil {
		if errors.Is(err, clustermeta.ErrNotFound) {
			return nil, status.Errorf(codes.NotFound, "clone %q not found", cloneID)
		}
		return nil, status.Errorf(codes.Internal, "delete clone: %v", err)
	}
	op, err := s.ops.create("clone.delete", "", clone.SourceVolumeID, "deleted", adminv1.OperationState_OPERATION_STATE_COMPLETED)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "create operation: %v", err)
	}
	return &adminv1.DeleteCloneResponse{
		Cluster: cluster,
		Operation: &adminv1.OperationHandle{
			Accepted:    true,
			OperationId: op.GetOperationId(),
			Message:     "clone deleted",
		},
	}, nil
}

func (s *server) UpdateNodeStoreWeights(ctx context.Context, req *adminv1.UpdateNodeStoreWeightsRequest) (*adminv1.UpdateNodeStoreWeightsResponse, error) {
	if err := s.requireLeader(ctx); err != nil {
		return nil, status.Error(codes.Unavailable, err.Error())
	}
	cluster, err := s.clusterRef(req.GetCluster())
	if err != nil {
		return nil, err
	}
	nodeID := strings.TrimSpace(req.GetNodeId())
	if nodeID == "" {
		return nil, status.Error(codes.InvalidArgument, "node_id is required")
	}
	if len(req.GetStores()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "stores is required")
	}
	for _, store := range req.GetStores() {
		if strings.TrimSpace(store.GetStoreId()) == "" {
			return nil, status.Error(codes.InvalidArgument, "store_id is required")
		}
		if store.GetWeight() < 0 {
			return nil, status.Error(codes.InvalidArgument, "weight must be zero or greater")
		}
	}
	if err := enforceDependencyMembershipChange(); err != nil {
		return nil, err
	}
	node, err := s.repo.GetNodeMembership(ctx, nodeID)
	if err != nil {
		if errors.Is(err, clustermeta.ErrNotFound) {
			return nil, status.Errorf(codes.NotFound, "node %q not found", nodeID)
		}
		return nil, status.Errorf(codes.Internal, "get node membership: %v", err)
	}
	adminEndpoint := strings.TrimSpace(node.AdminHTTPEndpoint)
	if adminEndpoint == "" {
		return nil, status.Errorf(codes.FailedPrecondition, "node %q has no admin_http_endpoint", nodeID)
	}
	persisted, err := s.updateNodeStoreWeightsViaAdminHTTP(ctx, adminEndpoint, req.GetStores())
	if err != nil {
		return nil, status.Errorf(codes.Unavailable, "update node %q store weights via admin_http_endpoint: %v", nodeID, err)
	}
	message := "deprecated compatibility path: node-local store weights updated (runtime only); prefer UpdateNodeStoreTuning"
	if persisted {
		message = "deprecated compatibility path: node-local store weights updated and persisted to store config; prefer UpdateNodeStoreTuning"
	}
	return &adminv1.UpdateNodeStoreWeightsResponse{
		Cluster: cluster,
		Operation: &adminv1.OperationHandle{
			Accepted: true,
			Message:  message,
		},
	}, nil
}

func (s *server) UpdateNodeStoreTuning(ctx context.Context, req *adminv1.UpdateNodeStoreTuningRequest) (*adminv1.UpdateNodeStoreTuningResponse, error) {
	if err := s.requireLeader(ctx); err != nil {
		return nil, status.Error(codes.Unavailable, err.Error())
	}
	cluster, err := s.clusterRef(req.GetCluster())
	if err != nil {
		return nil, err
	}
	nodeID := strings.TrimSpace(req.GetNodeId())
	if nodeID == "" {
		return nil, status.Error(codes.InvalidArgument, "node_id is required")
	}
	if len(req.GetStores()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "stores is required")
	}
	for _, store := range req.GetStores() {
		if strings.TrimSpace(store.GetStoreId()) == "" {
			return nil, status.Error(codes.InvalidArgument, "store_id is required")
		}
		if store.GetWeight() < 0 {
			return nil, status.Error(codes.InvalidArgument, "weight must be zero or greater")
		}
		if strings.TrimSpace(store.GetAllocationPolicy()) != "" {
			return nil, status.Error(codes.InvalidArgument, "allocation_policy is deprecated; use weight=0 to stop new allocations")
		}
	}
	if err := enforceDependencyMembershipChange(); err != nil {
		return nil, err
	}
	node, err := s.repo.GetNodeMembership(ctx, nodeID)
	if err != nil {
		if errors.Is(err, clustermeta.ErrNotFound) {
			return nil, status.Errorf(codes.NotFound, "node %q not found", nodeID)
		}
		return nil, status.Errorf(codes.Internal, "get node membership: %v", err)
	}
	adminEndpoint := strings.TrimSpace(node.AdminHTTPEndpoint)
	if adminEndpoint == "" {
		return nil, status.Errorf(codes.FailedPrecondition, "node %q has no admin_http_endpoint", nodeID)
	}
	persisted, err := s.updateNodeStoreTuningViaAdminHTTP(ctx, adminEndpoint, req.GetStores())
	if err != nil {
		return nil, status.Errorf(codes.Unavailable, "update node %q store tuning via admin_http_endpoint: %v", nodeID, err)
	}
	message := "node-local store tuning updated (runtime only)"
	if persisted {
		message = "node-local store tuning updated and persisted to store config"
	}
	return &adminv1.UpdateNodeStoreTuningResponse{
		Cluster: cluster,
		Operation: &adminv1.OperationHandle{
			Accepted: true,
			Message:  message,
		},
	}, nil
}

func (s *server) SetMaintenanceThrottle(ctx context.Context, req *adminv1.SetMaintenanceThrottleRequest) (*adminv1.SetMaintenanceThrottleResponse, error) {
	if err := s.requireLeader(ctx); err != nil {
		return nil, status.Error(codes.Unavailable, err.Error())
	}
	cluster, err := s.clusterRef(req.GetCluster())
	if err != nil {
		return nil, err
	}
	if _, err := s.updateMaintenanceThrottleRecord(ctx, req.GetMeta(), func(rec *maintenanceThrottleRecord) bool {
		changed := false
		if req.GetMaxConcurrentRepairs() > 0 {
			rec.MaxConcurrentRepairs = int(req.GetMaxConcurrentRepairs())
			changed = true
		}
		if req.GetMaxConcurrentRebalances() > 0 {
			rec.MaxConcurrentRebalances = int(req.GetMaxConcurrentRebalances())
			changed = true
		}
		if req.GetMaxConcurrentDrains() > 0 {
			rec.MaxConcurrentDrains = int(req.GetMaxConcurrentDrains())
			changed = true
		}
		return changed
	}); err != nil {
		return nil, status.Errorf(codes.Internal, "persist maintenance throttle: %v", err)
	}
	op, err := s.ops.create("maintenance.throttle", "", "", "updated", adminv1.OperationState_OPERATION_STATE_COMPLETED)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "create operation: %v", err)
	}
	return &adminv1.SetMaintenanceThrottleResponse{
		Cluster: cluster,
		Operation: &adminv1.OperationHandle{
			Accepted:    true,
			OperationId: op.GetOperationId(),
			Message:     "maintenance throttle updated",
		},
	}, nil
}

func (s *server) GetMaintenanceStatus(ctx context.Context, req *adminv1.GetMaintenanceStatusRequest) (*adminv1.GetMaintenanceStatusResponse, error) {
	if err := s.requireLeader(ctx); err != nil {
		return nil, status.Error(codes.Unavailable, err.Error())
	}
	cluster, err := s.clusterRef(req.GetCluster())
	if err != nil {
		return nil, err
	}
	settings, err := s.loadMaintenanceSettingsSnapshot(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "load maintenance throttle: %v", err)
	}
	return &adminv1.GetMaintenanceStatusResponse{
		Cluster: cluster,
		Throttle: &adminv1.MaintenanceThrottleSummary{
			Authority:               maintenanceThrottleAuthority,
			Generation:              settings.generation,
			MaxConcurrentRepairs:    uint32(settings.maxConcurrentRepairs),
			MaxConcurrentRebalances: uint32(settings.maxConcurrentRebalances),
			MaxConcurrentDrains:     uint32(settings.maxConcurrentDrains),
			MaxConcurrentPayloadGcs: uint32(settings.maxConcurrentPayloadGCs),
			PauseRepairs:            settings.pauseRepairs,
			PauseRebalances:         settings.pauseRebalances,
			PauseDrains:             settings.pauseDrains,
			PausePayloadGcs:         settings.pausePayloadGCs,
		},
		Budgets: s.backgroundBudgetSummaries(ctx, settings),
	}, nil
}

func (s *server) PauseMaintenance(ctx context.Context, req *adminv1.PauseMaintenanceRequest) (*adminv1.PauseMaintenanceResponse, error) {
	if err := s.requireLeader(ctx); err != nil {
		return nil, status.Error(codes.Unavailable, err.Error())
	}
	cluster, err := s.clusterRef(req.GetCluster())
	if err != nil {
		return nil, err
	}
	if _, err := s.updateMaintenanceThrottleRecord(ctx, req.GetMeta(), func(rec *maintenanceThrottleRecord) bool {
		changed := false
		if req.GetPauseRepairs() {
			rec.PauseRepairs = true
			changed = true
		}
		if req.GetPauseRebalances() {
			rec.PauseRebalances = true
			changed = true
		}
		if req.GetPauseDrains() {
			rec.PauseDrains = true
			changed = true
		}
		return changed
	}); err != nil {
		return nil, status.Errorf(codes.Internal, "persist maintenance pause: %v", err)
	}
	op, err := s.ops.create("maintenance.pause", "", "", "paused", adminv1.OperationState_OPERATION_STATE_COMPLETED)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "create operation: %v", err)
	}
	return &adminv1.PauseMaintenanceResponse{
		Cluster: cluster,
		Operation: &adminv1.OperationHandle{
			Accepted:    true,
			OperationId: op.GetOperationId(),
			Message:     "maintenance pause flags updated",
		},
	}, nil
}

func (s *server) ResumeMaintenance(ctx context.Context, req *adminv1.ResumeMaintenanceRequest) (*adminv1.ResumeMaintenanceResponse, error) {
	if err := s.requireLeader(ctx); err != nil {
		return nil, status.Error(codes.Unavailable, err.Error())
	}
	cluster, err := s.clusterRef(req.GetCluster())
	if err != nil {
		return nil, err
	}
	if _, err := s.updateMaintenanceThrottleRecord(ctx, req.GetMeta(), func(rec *maintenanceThrottleRecord) bool {
		changed := false
		if req.GetResumeRepairs() {
			rec.PauseRepairs = false
			changed = true
		}
		if req.GetResumeRebalances() {
			rec.PauseRebalances = false
			changed = true
		}
		if req.GetResumeDrains() {
			rec.PauseDrains = false
			changed = true
		}
		return changed
	}); err != nil {
		return nil, status.Errorf(codes.Internal, "persist maintenance resume: %v", err)
	}
	op, err := s.ops.create("maintenance.resume", "", "", "resumed", adminv1.OperationState_OPERATION_STATE_COMPLETED)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "create operation: %v", err)
	}
	return &adminv1.ResumeMaintenanceResponse{
		Cluster: cluster,
		Operation: &adminv1.OperationHandle{
			Accepted:    true,
			OperationId: op.GetOperationId(),
			Message:     "maintenance pause flags cleared",
		},
	}, nil
}

func (s *server) ListRepairs(context.Context, *adminv1.ListRepairsRequest) (*adminv1.ListRepairsResponse, error) {
	repairs, err := s.listTransitionsByReason(context.Background(), "repair")
	if err != nil {
		return nil, status.Errorf(codes.Internal, "list repair transitions: %v", err)
	}
	return &adminv1.ListRepairsResponse{Repairs: repairs}, nil
}

func (s *server) ListRebalances(context.Context, *adminv1.ListRebalancesRequest) (*adminv1.ListRebalancesResponse, error) {
	rebalances, err := s.listTransitionsByReason(context.Background(), "rebalance")
	if err != nil {
		return nil, status.Errorf(codes.Internal, "list rebalance transitions: %v", err)
	}
	return &adminv1.ListRebalancesResponse{Rebalances: toRebalanceSummaries(rebalances)}, nil
}

func (s *server) GetOperation(ctx context.Context, req *adminv1.GetOperationRequest) (*adminv1.GetOperationResponse, error) {
	cluster, _ := s.clusterRef(req.GetCluster())
	if req.GetOperationId() == "" {
		return nil, status.Error(codes.InvalidArgument, "operation_id is required")
	}
	op, err := s.ops.get(req.GetOperationId())
	if err != nil {
		if errors.Is(err, clustermeta.ErrNotFound) {
			mutation, mutationErr := s.repo.FindMutationOperationByID(ctx, req.GetOperationId())
			if mutationErr == nil {
				related, _ := s.repo.ListMutationOperations(ctx, mutation.VolumeID)
				return &adminv1.GetOperationResponse{
					Cluster:   cluster,
					Operation: mutationOperationToAdminStatus(mutation, related),
				}, nil
			}
			if errors.Is(mutationErr, clustermeta.ErrNotFound) {
				return nil, status.Errorf(codes.NotFound, "operation %q not found", req.GetOperationId())
			}
			return nil, status.Errorf(codes.Internal, "get mutation operation: %v", mutationErr)
		}
		return nil, status.Errorf(codes.Internal, "get operation: %v", err)
	}
	op = s.refreshOperation(ctx, op)
	return &adminv1.GetOperationResponse{Cluster: cluster, Operation: op}, nil
}

func (s *server) ListOperations(ctx context.Context, req *adminv1.ListOperationsRequest) (*adminv1.ListOperationsResponse, error) {
	cluster, _ := s.clusterRef(req.GetCluster())
	ops := s.ops.list(req.GetKind(), req.GetState())
	for i := range ops {
		ops[i] = s.refreshOperation(ctx, ops[i])
	}
	mutationOps, err := s.repo.ListAllMutationOperations(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "list mutation operations: %v", err)
	}
	for _, mutation := range mutationOps {
		op := mutationOperationToAdminStatus(mutation, mutationOps)
		if req.GetKind() != "" && op.GetKind() != req.GetKind() {
			continue
		}
		if req.GetState() != adminv1.OperationState_OPERATION_STATE_UNSPECIFIED && op.GetState() != req.GetState() {
			continue
		}
		ops = append(ops, op)
	}
	sort.Slice(ops, func(i, j int) bool {
		left := int64(0)
		right := int64(0)
		if ts := ops[i].GetLastProgressAt(); ts != nil {
			left = ts.AsTime().Unix()
		}
		if ts := ops[j].GetLastProgressAt(); ts != nil {
			right = ts.AsTime().Unix()
		}
		if left == right {
			return ops[i].GetOperationId() < ops[j].GetOperationId()
		}
		return left < right
	})
	return &adminv1.ListOperationsResponse{
		Cluster:    cluster,
		Operations: ops,
	}, nil
}

func (s *server) ClusterInit(ctx context.Context, req *adminv1.ClusterInitRequest) (*adminv1.ClusterInitResponse, error) {
	if err := s.requireLeader(ctx); err != nil {
		return nil, status.Error(codes.Unavailable, err.Error())
	}
	cluster, err := s.clusterRef(req.GetCluster())
	if err != nil {
		return nil, err
	}
	record := bootstrapRecord{
		ClusterID:     cluster.GetClusterId(),
		SBSClusterID:  cluster.GetSbsClusterId(),
		CreatedBy:     req.GetMeta().GetActor(),
		CreatedReason: req.GetMeta().GetReason(),
		CreatedAtUnix: time.Now().Unix(),
		SchemaVersion: 1,
		MetadataRoot:  s.root,
	}
	raw, err := json.Marshal(record)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "marshal bootstrap record: %v", err)
	}
	existing, found, err := s.kv.Get(ctx, bootstrapKey(s.root))
	if err != nil {
		return nil, status.Errorf(codes.Internal, "read bootstrap record: %v", err)
	}
	if !found {
		if err := s.kv.Set(ctx, bootstrapKey(s.root), raw); err != nil {
			return nil, status.Errorf(codes.Internal, "write bootstrap record: %v", err)
		}
		return &adminv1.ClusterInitResponse{
			Cluster: cluster,
			Operation: &adminv1.OperationHandle{
				Accepted:    true,
				OperationId: "bootstrap",
				Message:     "cluster initialized",
			},
		}, nil
	}
	// ClusterInit should be safely re-runnable. We only require stable identity fields
	// to match; CreatedBy/Reason/AtUnix may differ across invocations.
	var prev bootstrapRecord
	if err := json.Unmarshal(existing, &prev); err != nil {
		return nil, status.Errorf(codes.Internal, "unmarshal bootstrap record: %v", err)
	}
	if prev.ClusterID != record.ClusterID || prev.SBSClusterID != record.SBSClusterID || prev.SchemaVersion != record.SchemaVersion || prev.MetadataRoot != record.MetadataRoot {
		return nil, status.Errorf(codes.FailedPrecondition,
			"bootstrap metadata mismatch: existing cluster_id=%q sbs_cluster_id=%q schema_version=%d metadata_root=%q; requested cluster_id=%q sbs_cluster_id=%q schema_version=%d metadata_root=%q",
			prev.ClusterID, prev.SBSClusterID, prev.SchemaVersion, prev.MetadataRoot,
			record.ClusterID, record.SBSClusterID, record.SchemaVersion, record.MetadataRoot)
	}
	return &adminv1.ClusterInitResponse{
		Cluster: cluster,
		Operation: &adminv1.OperationHandle{
			Accepted:    true,
			OperationId: "bootstrap",
			Message:     "cluster already initialized with matching bootstrap metadata",
		},
	}, nil
}

func (s *server) clusterRef(req *adminv1.ClusterRef) (*adminv1.ClusterRef, error) {
	clusterID := s.clusterID
	sbsClusterID := s.sbsClusterID
	if req != nil {
		if req.GetClusterId() != "" {
			clusterID = req.GetClusterId()
		}
		if req.GetSbsClusterId() != "" {
			sbsClusterID = req.GetSbsClusterId()
		}
	}
	if clusterID == "" || sbsClusterID == "" {
		return nil, status.Error(codes.InvalidArgument, "cluster_id and sbs_cluster_id are required")
	}
	return &adminv1.ClusterRef{ClusterId: clusterID, SbsClusterId: sbsClusterID}, nil
}

func (s *server) failOperation(opID string, err error) {
	_, _ = s.ops.update(opID, func(op *adminv1.OperationStatus) {
		op.State = adminv1.OperationState_OPERATION_STATE_FAILED
		op.ErrorMessage = err.Error()
	})
}

func openMetadataBackend(ctx context.Context, backendName, metadataPath, metadataRoot string, tikvOpts tikvMetadataOptions) (*metadataBackend, error) {
	switch backendName {
	case "", "pebble":
		kv, err := clustermeta.OpenPebbleKV(metadataPath)
		if err != nil {
			return nil, fmt.Errorf("open metadata pebble %s: %w", metadataPath, err)
		}
		return &metadataBackend{
			name:  "pebble",
			kv:    kv,
			repo:  clustermeta.NewRepository(kv, metadataRoot),
			close: kv.Close,
		}, nil
	case "tikv":
		kv, closeFn, err := clustermeta.OpenTiKVKV(ctx, clustermeta.TiKVConfig{
			Options:              tikvOpts.toStoreOptions(),
			Root:                 metadataRoot,
			TraceOperations:      tikvOpts.traceOperations,
			EnableAsyncCommit:    tikvOpts.enableAsyncCommit,
			EnableOnePhaseCommit: tikvOpts.enableOnePhaseCommit,
		})
		if err != nil {
			return nil, fmt.Errorf("open metadata tikv: %w", err)
		}
		return &metadataBackend{
			name:  "tikv",
			kv:    kv,
			repo:  clustermeta.NewRepository(kv, metadataRoot),
			close: closeFn,
		}, nil
	default:
		return nil, fmt.Errorf("unsupported metadata backend %q", backendName)
	}
}

type tikvMetadataOptions struct {
	pdEndpoints          []string
	timeout              time.Duration
	apiVersion           string
	keyspace             string
	tlsEnabled           bool
	caFile               string
	certFile             string
	keyFile              string
	traceOperations      bool
	enableAsyncCommit    bool
	enableOnePhaseCommit bool
}

type metadataRuntimeConfig struct {
	backendName     string
	metadataPath    string
	tikvPDEndpoints []string
}

func (o tikvMetadataOptions) toStoreOptions() tikvopts.Options {
	return tikvopts.Options{
		PDEndpoints: o.pdEndpoints,
		Timeout:     o.timeout,
		APIVersion:  tikvopts.APIVersion(o.apiVersion),
		Keyspace:    o.keyspace,
		TLS: tikvopts.TLSSecurity{
			Enabled:  o.tlsEnabled,
			CAPath:   o.caFile,
			CertPath: o.certFile,
			KeyPath:  o.keyFile,
		},
	}
}

func parseCSV(value string) []string {
	parts := strings.Split(value, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		out = append(out, part)
	}
	return out
}

func validateMetadataRuntimeConfig(cfg metadataRuntimeConfig) error {
	switch strings.TrimSpace(cfg.backendName) {
	case "", "pebble":
		if strings.TrimSpace(cfg.metadataPath) == "" {
			return fmt.Errorf("pebble metadata backend requires --metadata-path")
		}
		return nil
	case "tikv":
		if len(cfg.tikvPDEndpoints) == 0 {
			return fmt.Errorf("tikv metadata backend requires --tikv-pd-endpoints")
		}
		return nil
	default:
		return nil
	}
}

func main() {
	for _, arg := range os.Args[1:] {
		if arg == "--version" || arg == "version" {
			fmt.Println(namrbdversion.BuildSummary())
			return
		}
	}
	os.Args = append(os.Args[:1], cliux.RewriteDeprecatedFlags(os.Args[1:], []cliux.Alias{
		{Legacy: "grpc-listen", Canonical: "sbs-service-listen", DeprecatedIn: "post-1.0"},
		{Legacy: "http-listen", Canonical: "sbs-service-http-listen", DeprecatedIn: "post-1.0"},
	}, os.Stderr)...)
	os.Args = append(os.Args[:1], cliux.RewriteCommandArgs(os.Args[1:], false, false)...)
	fs := flag.NewFlagSet("sbs-service", flag.ExitOnError)
	configPath := fs.String("config", "", "service config file path (AA-IMPL-001F); when set it supplies stable settings, while environment variables and explicitly typed flags still win")
	clusterID := fs.String("cluster-id", getenvOrDefault("NAMRBD_CLUSTER_ID", "namrbd-dev"), "cluster id")
	sbsClusterID := fs.String("sbs-cluster-id", getenvOrDefault("NAMRBD_SBS_CLUSTER_ID", ""), "SBS cluster id; defaults to --cluster-id when omitted")
	nodeID := fs.String("node-id", getenvCompatOrDefault(envcompat.SBSServiceNodeID, "sbs-svc-1"), "local sbs-service node id")
	metadataBackendName := fs.String("metadata-backend", getenvOrDefault("NAMRBD_SBS_METADATA_BACKEND", "pebble"), "metadata backend: pebble or tikv")
	metadataPath := fs.String("metadata-path", getenvOrDefault("NAMRBD_SBS_STATE_DIR", "./var/sbs-metadata"), "local metadata path for bootstrap development")
	leaderLeaseDuration := fs.Duration("leader-lease-duration", getenvDuration("NAMRBD_SBS_LEADER_LEASE_DURATION", 10*time.Second), "leader lease duration")
	leaderRenewInterval := fs.Duration("leader-renew-interval", getenvDuration("NAMRBD_SBS_LEADER_RENEW_INTERVAL", 3*time.Second), "leader lease renew interval")
	tikvPDEndpoints := fs.String("tikv-pd-endpoints", getenvOrDefault("NAMRBD_TIKV_PD_ENDPOINTS", ""), "comma-separated TiKV PD endpoints")
	tikvTimeout := fs.Duration("tikv-timeout", getenvDuration("NAMRBD_TIKV_TIMEOUT", 3*time.Second), "TiKV metadata request timeout")
	tikvAPIVersion := fs.String("tikv-api-version", getenvOrDefault("NAMRBD_TIKV_API_VERSION", "v1"), "TiKV API version for metadata backend")
	tikvKeyspace := fs.String("tikv-keyspace", getenvOrDefault("NAMRBD_TIKV_KEYSPACE", ""), "TiKV keyspace for metadata backend")
	tikvTLSEnabled := fs.Bool("tikv-tls-enabled", getenvBool("NAMRBD_TIKV_TLS_ENABLED", false), "enable TLS for TiKV metadata backend")
	tikvCAFile := fs.String("tikv-ca-file", getenvOrDefault("NAMRBD_CA_FILE", ""), "CA file for TiKV metadata backend")
	tikvCertFile := fs.String("tikv-cert-file", getenvOrDefault("NAMRBD_CERT_FILE", ""), "client cert file for TiKV metadata backend")
	tikvKeyFile := fs.String("tikv-key-file", getenvOrDefault("NAMRBD_KEY_FILE", ""), "client key file for TiKV metadata backend")
	tikvOperationTrace := fs.Bool("tikv-operation-trace", getenvBool("NAMRBD_TIKV_OPERATION_TRACE", false), "emit structured TiKV metadata operation latency trace events")
	tikvAsyncCommit := fs.Bool("tikv-async-commit", getenvBool("NAMRBD_TIKV_ASYNC_COMMIT", false), "enable TiKV async commit for metadata transactions")
	tikvOnePhaseCommit := fs.Bool("tikv-one-phase-commit", getenvBool("NAMRBD_TIKV_ONE_PHASE_COMMIT", false), "enable TiKV one-phase commit for eligible metadata transactions")
	grpcListen := fs.String("sbs-service-listen", getenvCompatOrDefault(envcompat.SBSServiceGRPCListen, "0.0.0.0:9443"), "listen address for sbs-service gRPC")
	httpListen := fs.String("sbs-service-http-listen", getenvCompatOrDefault(envcompat.SBSServiceHTTPListen, "0.0.0.0:9081"), "listen address for sbs-service HTTP health and observability")
	payloadRoot := fs.String("payload-root", getenvOrDefault("NAMRBD_SBS_PAYLOAD_ROOT", ""), "local replica payload root for automatic payload GC")
	serviceOwnedWriteEffects := fs.Bool("service-owned-write-effects", getenvBool("NAMRBD_SBS_SERVICE_OWNED_WRITE_EFFECTS", defaultServiceOwnedWriteEffects), "own the ordered service-side write-effects queue for append-only write metadata mode")
	nativeAllocationFastPath := fs.Bool("native-allocation-fast-path", getenvBool("NAMRBD_SBS_NATIVE_ALLOCATION_FAST_PATH", defaultNativeAllocationFastPath), "enable native-allocation fast path for already-normalized allocation-backed write effects")
	writeEffectsBatchMax := fs.Int("write-effects-batch-max", maxInt(getenvInt("NAMRBD_SBS_WRITE_EFFECTS_BATCH_MAX", defaultServiceWriteEffectsBatchMax), 1), "maximum service-owned write-effects items to commit in one metadata batch")
	writeEffectsLaneBucketCount := fs.Int("write-effects-lane-bucket-count", maxInt(getenvInt("NAMRBD_SBS_WRITE_EFFECTS_LANE_BUCKET_COUNT", 0), 0), "lab only: group native allocation write-effects page lanes into this many buckets; 0 keeps one lane per page")
	asyncWriteMutationFinalize := fs.Bool("async-write-mutation-finalize", getenvBool("NAMRBD_SBS_ASYNC_WRITE_MUTATION_FINALIZE", false), "lab only: finalize write mutation operation markers outside the client-visible effects transaction")
	writeIntentBatchCoalesceWait := fs.Duration("write-intent-batch-coalesce-wait", getenvDuration("NAMRBD_SBS_WRITE_INTENT_BATCH_COALESCE_WAIT", defaultServiceWriteIntentBatchCoalesceWait), "lab only: coalesce write intent records into service-side metadata batches")
	healthShardCount := maxInt(getenvInt("NAMRBD_SBS_DATA_HEALTH_SHARD_COUNT", 1), 1)
	healthConcurrency := maxInt(getenvInt("NAMRBD_SBS_DATA_HEALTH_CONCURRENCY", nodeHealthShardConcurrency), 1)
	healthInterval := getenvDuration("NAMRBD_SBS_DATA_HEALTH_CHECK_INTERVAL", 10*time.Second)
	healthTimeout := getenvDuration("NAMRBD_SBS_DATA_HEALTH_TIMEOUT", 2*time.Second)
	healthSuspectAfter := maxInt(getenvInt("NAMRBD_SBS_DATA_SUSPECT_AFTER", 3), 1)
	healthDownAfter := maxInt(getenvInt("NAMRBD_SBS_DATA_DOWN_AFTER", 6), 1)
	healthRecoveryCooldown := getenvDuration("NAMRBD_SBS_DATA_RECOVER_COOLDOWN", 30*time.Second)
	cliux.InstallStructuredUsage(fs, "sbs-service", func(name string) bool {
		f := fs.Lookup(name)
		labOnly := f != nil && strings.Contains(strings.ToLower(f.Usage), "lab only")
		return labOnly || strings.Contains(name, "lab-") || name == "async-write-mutation-finalize" ||
			name == "write-effects-lane-bucket-count" || name == "write-intent-batch-coalesce-wait"
	})
	fs.Parse(os.Args[1:])

	// Without --config sbs-service behaves exactly as before. Adoption is
	// additive so existing deployments and lab fixtures are unaffected.
	if strings.TrimSpace(*configPath) != "" {
		summary, err := applySBSServiceConfig(*configPath, sbsServiceConfigBinding{
			ClusterID:       clusterID,
			SBSClusterID:    sbsClusterID,
			NodeID:          nodeID,
			MetadataBackend: metadataBackendName,

			GRPCListen:  grpcListen,
			HTTPListen:  httpListen,
			PayloadRoot: payloadRoot,

			TiKVPDEndpoints:    tikvPDEndpoints,
			TiKVKeyspace:       tikvKeyspace,
			TiKVAPIVersion:     tikvAPIVersion,
			TiKVTimeout:        tikvTimeout,
			TiKVTLSEnabled:     tikvTLSEnabled,
			TiKVCAFile:         tikvCAFile,
			TiKVCertFile:       tikvCertFile,
			TiKVKeyFile:        tikvKeyFile,
			TiKVOperationTrace: tikvOperationTrace,

			LeaderLeaseDuration:    leaderLeaseDuration,
			LeaderRenewInterval:    leaderRenewInterval,
			HealthShardCount:       &healthShardCount,
			HealthConcurrency:      &healthConcurrency,
			HealthInterval:         &healthInterval,
			HealthTimeout:          &healthTimeout,
			HealthSuspectAfter:     &healthSuspectAfter,
			HealthDownAfter:        &healthDownAfter,
			HealthRecoveryCooldown: &healthRecoveryCooldown,

			ServiceOwnedWriteEffects:   serviceOwnedWriteEffects,
			NativeAllocationFastPath:   nativeAllocationFastPath,
			WriteEffectsBatchMax:       writeEffectsBatchMax,
			WriteEffectsLaneBuckets:    writeEffectsLaneBucketCount,
			AsyncWriteMutationFinalize: asyncWriteMutationFinalize,
		}, explicitlySetFlags(fs), osEnvLookup)
		// The summary is emitted either way. On the failure path it is the only
		// record of which config the process tried to start from.
		if blob, mErr := json.Marshal(summary); mErr == nil {
			log.Printf("service config summary: %s", blob)
		}
		if err != nil {
			log.Fatalf("service config: %v", err)
		}
	}
	if strings.TrimSpace(*sbsClusterID) == "" {
		*sbsClusterID = strings.TrimSpace(*clusterID)
	}

	nativeAllocationFastPathEnabled := *nativeAllocationFastPath
	pdEndpoints := parseCSV(*tikvPDEndpoints)
	if err := validateMetadataRuntimeConfig(metadataRuntimeConfig{
		backendName:     *metadataBackendName,
		metadataPath:    *metadataPath,
		tikvPDEndpoints: pdEndpoints,
	}); err != nil {
		log.Fatalf("validate metadata runtime config: %v", err)
	}

	backend, err := openMetadataBackend(context.Background(), *metadataBackendName, *metadataPath, defaultMetadataRoot, tikvMetadataOptions{
		pdEndpoints:          pdEndpoints,
		timeout:              *tikvTimeout,
		apiVersion:           *tikvAPIVersion,
		keyspace:             *tikvKeyspace,
		tlsEnabled:           *tikvTLSEnabled,
		caFile:               *tikvCAFile,
		certFile:             *tikvCertFile,
		keyFile:              *tikvKeyFile,
		traceOperations:      *tikvOperationTrace,
		enableAsyncCommit:    *tikvAsyncCommit,
		enableOnePhaseCommit: *tikvOnePhaseCommit,
	})
	if err != nil {
		log.Fatalf("open metadata backend %s: %v", *metadataBackendName, err)
	}
	defer backend.close()
	backend.repo.SetNativeAllocationFastPath(nativeAllocationFastPathEnabled)
	backend.repo.SetAsyncWriteMutationFinalize(*asyncWriteMutationFinalize)

	srv := &server{
		clusterID:                 *clusterID,
		sbsClusterID:              *sbsClusterID,
		nodeID:                    *nodeID,
		metadataBackendName:       backend.name,
		metadataRuntimeMode:       metadataRuntimeMode(backend.name),
		tikvPDEndpointsConfigured: len(pdEndpoints) > 0,
		metadataPathConfigured:    strings.TrimSpace(*metadataPath) != "",
		root:                      defaultMetadataRoot,
		payloadRoot:               strings.TrimSpace(*payloadRoot),
		startedAt:                 time.Now(),
		kv:                        backend.kv,
		repo:                      backend.repo,
		ops:                       newOperationStore(backend.kv, defaultMetadataRoot),
		cache:                     newReplicaClientCache(),
		viewCache:                 newPublishedViewCache(getenvDuration("NAMRBD_SBS_PUBLISHED_VIEW_CACHE_TTL", defaultPublishedViewCacheTTL)),
		maint:                     newMaintenanceSettings(),
		leader:                    newLeaderLeaseManager(backend.kv, defaultMetadataRoot, *nodeID),
		placementApplyInternalService: clustercontrol.NewRepositoryBackedPlacementApplyInternalService(
			backend.repo,
		),
		writeSessionInternalService: clustercontrol.NewRepositoryBackedWriteSessionInternalService(
			backend.repo,
		),
		serviceOwnedWriteEffects: *serviceOwnedWriteEffects,
		writeEffectsQueue: func() *serviceWriteEffectsQueue {
			if *serviceOwnedWriteEffects {
				return newServiceWriteEffectsQueue(
					backend.repo,
					serviceWriteEffectsQueueNativeAllocationFastPath(nativeAllocationFastPathEnabled),
					serviceWriteEffectsQueueBatchCoalesceWait(getenvDuration("NAMRBD_SBS_WRITE_EFFECTS_BATCH_COALESCE_WAIT", defaultServiceRuntimeWriteEffectsBatchCoalesceWait)),
					serviceWriteEffectsQueueBatchMax(*writeEffectsBatchMax),
					serviceWriteEffectsQueueLaneBucketCount(*writeEffectsLaneBucketCount),
				)
			}
			return nil
		}(),
		writeIntentQueue: func() *serviceWriteIntentQueue {
			if *writeIntentBatchCoalesceWait > 0 {
				return newServiceWriteIntentQueue(
					clustercontrol.NewRepositoryBackedWriteSessionInternalService(backend.repo),
					serviceWriteIntentQueueBatchCoalesceWait(*writeIntentBatchCoalesceWait),
				)
			}
			return nil
		}(),
		chunkIDAllocatorService: clustercontrol.NewRepositoryBackedChunkIDAllocatorInternalService(backend.repo),
		placementResolverService: clustercontrol.NewRepositoryBackedPlacementResolverInternalServiceWithCacheTTL(
			backend.repo,
			getenvDuration("NAMRBD_SBS_PLACEMENT_RESOLVER_CACHE_TTL", clustercontrol.DefaultPlacementResolverCacheTTL),
		),
		placementApplyTimeout:                 getenvDuration("NAMRBD_SBS_PLACEMENT_APPLY_TIMEOUT", defaultPlacementApplyTimeout),
		now:                                   time.Now,
		maintenanceVolumeCooldown:             5 * time.Second,
		autoRebalanceMinVolumeAge:             getenvDuration("NAMRBD_SBS_AUTO_REBALANCE_MIN_VOLUME_AGE", defaultAutoRebalanceMinVolumeAge),
		autoRebalanceForegroundWriteSettleAge: getenvDuration("NAMRBD_SBS_AUTO_REBALANCE_FOREGROUND_WRITE_SETTLE_AGE", defaultAutoRebalanceForegroundWriteSettleAge),
		lastMaintenanceRunByVolume:            make(map[string]int64),
		healthCheckInterval:                   healthInterval,
		healthCheckTimeout:                    healthTimeout,
		healthMinimumShardCount:               healthShardCount,
		healthConcurrencyPerShard:             min(healthConcurrency, nodeHealthShardConcurrency),
		healthSuspectAfter:                    uint32(healthSuspectAfter),
		healthDownAfter:                       uint32(healthDownAfter),
		healthRecoverAfter:                    uint32(maxInt(getenvInt("NAMRBD_SBS_DATA_RECOVER_AFTER", 2), 1)),
		healthRecoveryCooldown:                healthRecoveryCooldown,
	}
	srv.leader.leaseDuration = *leaderLeaseDuration
	srv.leader.renewInterval = *leaderRenewInterval
	srv.ready.Store(true)
	defer srv.cache.Close()

	grpcLn, err := net.Listen("tcp", *grpcListen)
	if err != nil {
		log.Fatalf("listen gRPC %s: %v", *grpcListen, err)
	}
	grpcSrv := grpc.NewServer()
	adminv1.RegisterAdminServiceServer(grpcSrv, srv)
	adminv1.RegisterOperationsServiceServer(grpcSrv, srv)
	sbsv1.RegisterVolumeServiceServer(grpcSrv, sbsgrpc.NewServer(newClusterVolumeServiceProxy(srv)))
	internalv1.RegisterPlacementApplyServiceServer(grpcSrv, srv)
	internalv1.RegisterWriteSessionServiceServer(grpcSrv, srv)
	internalv1.RegisterECMetadataServiceServer(grpcSrv, srv)
	internalv1.RegisterChunkIDAllocatorServiceServer(grpcSrv, srv)
	internalv1.RegisterPlacementResolverServiceServer(grpcSrv, srv)

	httpSrv := &http.Server{
		Addr:    *httpListen,
		Handler: observabilityMux(srv),
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	go func() {
		log.Printf("sbs-service gRPC listening on %s (metadata_backend=%s runtime_mode=%s tikv_pd_endpoints_configured=%t metadata_path_configured=%t)", *grpcListen, backend.name, srv.metadataRuntimeMode, srv.tikvPDEndpointsConfigured, srv.metadataPathConfigured)
		if err := grpcSrv.Serve(grpcLn); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			log.Fatalf("serve gRPC: %v", err)
		}
	}()
	go func() {
		log.Printf("sbs-service HTTP observability listening on %s", *httpListen)
		if err := httpSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("serve HTTP: %v", err)
		}
	}()
	go srv.leader.Run(ctx)
	go srv.runBackgroundMaintenance(ctx)
	go srv.runBackgroundECMaintenance(ctx)
	// AA-IMPL-004B. TiKV reachability is learned from the reads this service
	// already performs rather than from a probe of its own.
	clustermeta.SetTiKVOutcomeObserver(func(err error) {
		dependencyTracker.Report(depavail.DependencyTiKV, err)
	})
	go srv.runBackgroundNodeHealthReconciler(ctx)
	<-ctx.Done()

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = httpSrv.Shutdown(shutdownCtx)
	grpcSrv.GracefulStop()
}

func observabilityMux(s *server) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok\n"))
	})
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) {
		// These two 503s stay. They are about this process's own role, not
		// about a dependency: a service still starting up, or one that is not
		// the leader, genuinely cannot accept a mutation, and an orchestrator
		// routing around it is correct.
		//
		// Dependency state deliberately does not join them. AA-IMPL-004B
		// reports it on /dependency with a 200 in every state, because a
		// dependency outage must not evict a process that is still serving.
		if !s.ready.Load() {
			http.Error(w, "not ready", http.StatusServiceUnavailable)
			return
		}
		if s.leader != nil && !s.leader.MutationReady() {
			http.Error(w, "not leader-ready", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok\n"))
	})
	mux.Handle("/dependency", depavail.ReadinessHandler(dependencyTracker))
	s.registerPhaseYOperationsAPI(mux)
	dashboard := opsdashboard.Handler()
	mux.Handle("/console", dashboard)
	mux.Handle("/console/", dashboard)
	mux.HandleFunc("/debug/summary", func(w http.ResponseWriter, _ *http.Request) {
		snapshot, leaderNodeID := s.boundedObservabilitySnapshot()
		leaseExpiresAt := ""
		if snapshot.LeaseExpiresAtUnix > 0 {
			leaseExpiresAt = time.Unix(snapshot.LeaseExpiresAtUnix, 0).UTC().Format(time.RFC3339)
		}
		placementApplyTimeout := s.effectivePlacementApplyTimeout()
		placementApplyStats := s.placementApplyObservability.snapshot()
		writeSessionStats := s.writeSessionObservability.snapshot()
		chunkIDAllocatorStats := s.chunkIDAllocatorObservability.snapshot()
		placementResolverStats := s.placementResolverObservability.snapshot()
		_ = json.NewEncoder(w).Encode(map[string]any{
			"storage_terminology": map[string]any{
				"placement_extent_term":                     "Placement Extent",
				"placement_extent_abbrev":                   "PE",
				"allocation_chunk_term":                     "Allocation Chunk",
				"allocation_chunk_abbrev":                   "AC",
				"degraded_placement_extents_field":          "degraded_extents",
				"repair_backlog_allocation_chunks_field":    "repair_backlog_allocation_chunks",
				"rebalance_backlog_allocation_chunks_field": "rebalance_backlog_allocation_chunks",
				"drain_backlog_allocation_chunks_field":     "drain_backlog_allocation_chunks",
				"retired_payload_allocation_chunks_field":   "retired_payload_backlog_allocation_chunks",
				"transition_retry_window_ac_field":          "transition_retry_window_allocation_chunks",
			},
			"build_version":                                   buildVersion,
			"cluster_id":                                      s.clusterID,
			"sbs_cluster_id":                                  s.sbsClusterID,
			"node_id":                                         s.nodeID,
			"metadata_backend":                                s.effectiveMetadataBackendName(),
			"runtime_mode":                                    s.effectiveMetadataRuntimeMode(),
			"tikv_pd_endpoints_configured":                    s.tikvPDEndpointsConfigured,
			"metadata_path_configured":                        s.metadataPathConfigured,
			"placement_apply_timeout_seconds":                 placementApplyTimeout.Seconds(),
			"placement_apply_timeout_enabled":                 placementApplyTimeout > 0,
			"placement_apply_requests_total":                  placementApplyStats.RequestsTotal,
			"placement_apply_failures_total":                  placementApplyStats.FailuresTotal,
			"placement_apply_duration_total_seconds":          placementApplyStats.DurationTotalSeconds,
			"placement_apply_requests_by_class":               placementApplyStats.RequestsByClass,
			"write_session_requests_total":                    writeSessionStats.RequestsTotal,
			"write_session_failures_total":                    writeSessionStats.FailuresTotal,
			"write_session_duration_total_seconds":            writeSessionStats.DurationTotalSeconds,
			"write_session_requests_by_class":                 writeSessionStats.RequestsByClass,
			"chunk_id_allocator_requests_total":               chunkIDAllocatorStats.RequestsTotal,
			"chunk_id_allocator_failures_total":               chunkIDAllocatorStats.FailuresTotal,
			"chunk_id_allocator_duration_total_seconds":       chunkIDAllocatorStats.DurationTotalSeconds,
			"chunk_id_allocator_requests_by_class":            chunkIDAllocatorStats.RequestsByClass,
			"placement_resolver_requests_total":               placementResolverStats.RequestsTotal,
			"placement_resolver_failures_total":               placementResolverStats.FailuresTotal,
			"placement_resolver_duration_total_seconds":       placementResolverStats.DurationTotalSeconds,
			"placement_resolver_requests_by_class":            placementResolverStats.RequestsByClass,
			"control_plane_owner":                             "sbs-service",
			"cluster_metadata_owner":                          "tikv",
			"dev_metadata_owner":                              "local-pebble",
			"started_at":                                      s.startedAt.UTC().Format(time.RFC3339),
			"ready":                                           s.ready.Load(),
			"leader_node_id":                                  leaderNodeID,
			"local_is_leader":                                 snapshot.LocalIsLeader,
			"leader_state":                                    snapshot.LeaderState,
			"lease_expires_at":                                leaseExpiresAt,
			"known_nodes":                                     snapshot.KnownNodes,
			"operations_count":                                snapshot.OperationsTotal,
			"repair_backlog":                                  snapshot.RepairBacklog,
			"repair_backlog_current":                          snapshot.RepairBacklog,
			"repair_backlog_bytes":                            snapshot.RepairBacklogBytes,
			"repair_backlog_chunks":                           snapshot.RepairBacklogChunks,
			"repair_backlog_allocation_chunks":                snapshot.RepairBacklogChunks,
			"repair_backlog_oldest_age_seconds":               snapshot.TransitionFailedAgeSec,
			"repair_backlog_max_age_seconds":                  snapshot.TransitionFailedAgeSec,
			"repair_backlog_completed_total":                  snapshot.OperationsCompleted,
			"repair_backlog_failed_total":                     snapshot.TransitionFailedBatches,
			"rebalance_backlog":                               snapshot.RebalanceBacklog,
			"rebalance_backlog_current":                       snapshot.RebalanceBacklog,
			"rebalance_backlog_bytes":                         snapshot.RebalanceBacklogBytes,
			"rebalance_backlog_chunks":                        snapshot.RebalanceBacklogChunks,
			"rebalance_backlog_allocation_chunks":             snapshot.RebalanceBacklogChunks,
			"rebalance_backlog_oldest_age_seconds":            snapshot.TransitionFailedAgeSec,
			"rebalance_backlog_max_age_seconds":               snapshot.TransitionFailedAgeSec,
			"rebalance_backlog_oscillation_window_seconds":    snapshot.MaintenanceCooldownMaxSec,
			"drain_backlog":                                   snapshot.DrainBacklog,
			"drain_backlog_bytes":                             snapshot.DrainBacklogBytes,
			"drain_backlog_chunks":                            snapshot.DrainBacklogChunks,
			"drain_backlog_allocation_chunks":                 snapshot.DrainBacklogChunks,
			"retired_payload_backlog_bytes":                   snapshot.RetiredPayloadBacklogBytes,
			"retired_payload_backlog_chunks":                  snapshot.RetiredPayloadBacklogChunks,
			"retired_payload_backlog_allocation_chunks":       snapshot.RetiredPayloadBacklogChunks,
			"retired_payload_failed_batches":                  snapshot.RetiredPayloadFailedBatches,
			"retired_payload_oldest_failed_batch_age_seconds": snapshot.RetiredPayloadFailedAgeSec,
			"transition_failed_batches":                       snapshot.TransitionFailedBatches,
			"transition_recent_batches":                       snapshot.TransitionRecentBatches,
			"transition_small_batches":                        snapshot.TransitionSmallBatches,
			"transition_requeued":                             snapshot.TransitionRequeued,
			"transition_retry_pages":                          snapshot.TransitionRetryPages,
			"transition_retry_windows":                        snapshot.TransitionRetryWindows,
			"transition_retry_window_bytes":                   snapshot.TransitionRetryWindowBytes,
			"transition_retry_window_chunks":                  snapshot.TransitionRetryWindowChunks,
			"transition_retry_window_allocation_chunks":       snapshot.TransitionRetryWindowChunks,
			"maintenance_cooldown_volumes":                    snapshot.MaintenanceCooldownVolumes,
			"maintenance_cooldown_max_remaining_seconds":      snapshot.MaintenanceCooldownMaxSec,
			"nodes_with_probe_failures":                       snapshot.NodesWithProbeFailures,
			"max_consecutive_probe_failures":                  snapshot.MaxProbeFailures,
			"nodes_in_recovery_cooldown":                      snapshot.NodesInRecoveryCooldown,
			"max_recovery_cooldown_remaining_seconds":         snapshot.MaxRecoveryCooldownSec,
			"transition_oldest_failed_batch_age_seconds":      snapshot.TransitionFailedAgeSec,
			"volumes": snapshot.Volumes,
		})
	})
	mux.HandleFunc("/debug/volume", func(w http.ResponseWriter, r *http.Request) {
		s.handleDebugVolume(w, r)
	})
	mux.HandleFunc("/debug/transitions", func(w http.ResponseWriter, r *http.Request) {
		s.handleDebugTransitions(w, r)
	})
	mux.HandleFunc("/debug/enqueue-repair", func(w http.ResponseWriter, r *http.Request) {
		s.handleDebugEnqueueTransition(w, r, "repair")
	})
	mux.HandleFunc("/debug/enqueue-rebalance", func(w http.ResponseWriter, r *http.Request) {
		s.handleDebugEnqueueTransition(w, r, "rebalance")
	})
	mux.HandleFunc("/debug/set-node-health", func(w http.ResponseWriter, r *http.Request) {
		s.handleDebugSetNodeHealth(w, r)
	})
	mux.HandleFunc("/debug/clear-transitions", func(w http.ResponseWriter, r *http.Request) {
		s.handleDebugClearTransitions(w, r)
	})
	mux.HandleFunc("/debug/payload-gc", func(w http.ResponseWriter, r *http.Request) {
		s.handleDebugPayloadGC(w, r)
	})
	mux.HandleFunc("/debug/ec/inspect", func(w http.ResponseWriter, r *http.Request) {
		s.handleDebugECInspect(w, r)
	})
	mux.HandleFunc("/debug/ec/scrub", func(w http.ResponseWriter, r *http.Request) {
		s.handleDebugECScrub(w, r)
	})
	mux.HandleFunc("/debug/ec/repair", func(w http.ResponseWriter, r *http.Request) {
		s.handleDebugECRepair(w, r)
	})
	mux.HandleFunc("/debug/ec/maintenance-scan", func(w http.ResponseWriter, r *http.Request) {
		s.handleDebugECMaintenanceScan(w, r)
	})
	mux.HandleFunc("/debug/ec/fault-delete-shard", func(w http.ResponseWriter, r *http.Request) {
		s.handleDebugECFaultDeleteShard(w, r)
	})
	mux.HandleFunc("/debug/ec/rebalance", func(w http.ResponseWriter, r *http.Request) {
		s.handleDebugECRebalance(w, r)
	})
	mux.HandleFunc("/debug/ec/drain-preflight", func(w http.ResponseWriter, r *http.Request) {
		s.handleDebugECDrainPreflight(w, r)
	})
	mux.HandleFunc("/debug/ec/drain", func(w http.ResponseWriter, r *http.Request) {
		s.handleDebugECDrain(w, r)
	})
	mux.HandleFunc("/debug/ec/drain-volume", func(w http.ResponseWriter, r *http.Request) {
		s.handleDebugECDrainVolume(w, r)
	})
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		snapshot, _ := s.boundedObservabilitySnapshot()
		tikvPressure := clustermeta.TiKVPressureSnapshotNow()
		settings := s.effectiveMaintenanceSettingsSnapshot(r.Context())
		placementApplyStats := s.placementApplyObservability.snapshot()
		writeSessionStats := s.writeSessionObservability.snapshot()
		chunkIDAllocatorStats := s.chunkIDAllocatorObservability.snapshot()
		placementResolverStats := s.placementResolverObservability.snapshot()
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		_, _ = fmt.Fprintln(w, "# HELP sbs_service_ready Whether the sbs-service process is locally ready.")
		_, _ = fmt.Fprintln(w, "# TYPE sbs_service_ready gauge")
		_, _ = fmt.Fprintf(w, "sbs_service_ready %d\n", boolToMetric(s.ready.Load()))
		_, _ = fmt.Fprintln(w, "# HELP sbs_service_leader Whether this instance currently owns the leader lease.")
		_, _ = fmt.Fprintln(w, "# TYPE sbs_service_leader gauge")
		_, _ = fmt.Fprintf(w, "sbs_service_leader %d\n", boolToMetric(snapshot.LocalIsLeader))
		_, _ = fmt.Fprintln(w, "# HELP sbs_service_tikv_operations_total SBS metadata requests to TiKV by operation class.")
		_, _ = fmt.Fprintln(w, "# TYPE sbs_service_tikv_operations_total counter")
		_, _ = fmt.Fprintf(w, "sbs_service_tikv_operations_total{operation=\"batch_get\"} %d\n", tikvPressure.BatchGetCount)
		_, _ = fmt.Fprintf(w, "sbs_service_tikv_operations_total{operation=\"batch_get_key\"} %d\n", tikvPressure.BatchGetKeyCount)
		_, _ = fmt.Fprintf(w, "sbs_service_tikv_operations_total{operation=\"batch_get_chunk\"} %d\n", tikvPressure.BatchGetChunkCount)
		_, _ = fmt.Fprintf(w, "sbs_service_tikv_operations_total{operation=\"point_get\"} %d\n", tikvPressure.PointGetCount)
		_, _ = fmt.Fprintf(w, "sbs_service_tikv_operations_total{operation=\"full_scan\"} %d\n", tikvPressure.FullScanCount)
		_, _ = fmt.Fprintf(w, "sbs_service_tikv_operations_total{operation=\"txn_retry\"} %d\n", tikvPressure.TxnRetryCount)
		_, _ = fmt.Fprintln(w, "# HELP sbs_service_nodes Number of known nodes by lifecycle and health.")
		_, _ = fmt.Fprintln(w, "# TYPE sbs_service_nodes gauge")
		_, _ = fmt.Fprintf(w, "sbs_service_nodes{state=\"known\"} %d\n", snapshot.KnownNodes)
		_, _ = fmt.Fprintf(w, "sbs_service_nodes{state=\"active\"} %d\n", snapshot.ActiveNodes)
		_, _ = fmt.Fprintf(w, "sbs_service_nodes{state=\"draining\"} %d\n", snapshot.DrainingNodes)
		_, _ = fmt.Fprintf(w, "sbs_service_nodes{state=\"removed\"} %d\n", snapshot.RemovedNodes)
		_, _ = fmt.Fprintf(w, "sbs_service_nodes{state=\"healthy\"} %d\n", snapshot.HealthyNodes)
		_, _ = fmt.Fprintf(w, "sbs_service_nodes{state=\"suspect\"} %d\n", snapshot.SuspectNodes)
		_, _ = fmt.Fprintf(w, "sbs_service_nodes{state=\"down\"} %d\n", snapshot.DownNodes)
		_, _ = fmt.Fprintln(w, "# HELP sbs_service_volumes Number of volumes by health.")
		_, _ = fmt.Fprintln(w, "# TYPE sbs_service_volumes gauge")
		_, _ = fmt.Fprintf(w, "sbs_service_volumes{state=\"total\"} %d\n", snapshot.Volumes)
		_, _ = fmt.Fprintf(w, "sbs_service_volumes{state=\"healthy\"} %d\n", snapshot.VolumeHealthy)
		_, _ = fmt.Fprintf(w, "sbs_service_volumes{state=\"degraded\"} %d\n", snapshot.VolumeDegraded)
		_, _ = fmt.Fprintf(w, "sbs_service_volumes{state=\"blocked\"} %d\n", snapshot.VolumeBlocked)
		_, _ = fmt.Fprintln(w, "# HELP sbs_service_transition_backlog Number of active placement transitions by reason.")
		_, _ = fmt.Fprintln(w, "# TYPE sbs_service_transition_backlog gauge")
		_, _ = fmt.Fprintf(w, "sbs_service_transition_backlog{reason=\"repair\"} %d\n", snapshot.RepairBacklog)
		_, _ = fmt.Fprintf(w, "sbs_service_transition_backlog{reason=\"rebalance\"} %d\n", snapshot.RebalanceBacklog)
		_, _ = fmt.Fprintf(w, "sbs_service_transition_backlog{reason=\"drain\"} %d\n", snapshot.DrainBacklog)
		_, _ = fmt.Fprintln(w, "# HELP sbs_service_repair_backlog_current Active repair placement transitions.")
		_, _ = fmt.Fprintln(w, "# TYPE sbs_service_repair_backlog_current gauge")
		_, _ = fmt.Fprintf(w, "sbs_service_repair_backlog_current %d\n", snapshot.RepairBacklog)
		_, _ = fmt.Fprintln(w, "# HELP sbs_service_rebalance_backlog_current Active rebalance placement transitions.")
		_, _ = fmt.Fprintln(w, "# TYPE sbs_service_rebalance_backlog_current gauge")
		_, _ = fmt.Fprintf(w, "sbs_service_rebalance_backlog_current %d\n", snapshot.RebalanceBacklog)
		_, _ = fmt.Fprintln(w, "# HELP sbs_service_transition_backlog_bytes Active placement transition backlog bytes by reason.")
		_, _ = fmt.Fprintln(w, "# TYPE sbs_service_transition_backlog_bytes gauge")
		_, _ = fmt.Fprintf(w, "sbs_service_transition_backlog_bytes{reason=\"repair\"} %d\n", snapshot.RepairBacklogBytes)
		_, _ = fmt.Fprintf(w, "sbs_service_transition_backlog_bytes{reason=\"rebalance\"} %d\n", snapshot.RebalanceBacklogBytes)
		_, _ = fmt.Fprintf(w, "sbs_service_transition_backlog_bytes{reason=\"drain\"} %d\n", snapshot.DrainBacklogBytes)
		_, _ = fmt.Fprintln(w, "# HELP sbs_service_transition_backlog_chunks Active placement transition backlog allocation chunks (AC) by reason.")
		_, _ = fmt.Fprintln(w, "# TYPE sbs_service_transition_backlog_chunks gauge")
		_, _ = fmt.Fprintf(w, "sbs_service_transition_backlog_chunks{reason=\"repair\"} %d\n", snapshot.RepairBacklogChunks)
		_, _ = fmt.Fprintf(w, "sbs_service_transition_backlog_chunks{reason=\"rebalance\"} %d\n", snapshot.RebalanceBacklogChunks)
		_, _ = fmt.Fprintf(w, "sbs_service_transition_backlog_chunks{reason=\"drain\"} %d\n", snapshot.DrainBacklogChunks)
		_, _ = fmt.Fprintln(w, "# HELP sbs_service_retired_payload_backlog_bytes Retired payload backlog bytes awaiting cleanup.")
		_, _ = fmt.Fprintln(w, "# TYPE sbs_service_retired_payload_backlog_bytes gauge")
		_, _ = fmt.Fprintf(w, "sbs_service_retired_payload_backlog_bytes %d\n", snapshot.RetiredPayloadBacklogBytes)
		_, _ = fmt.Fprintln(w, "# HELP sbs_service_retired_payload_backlog_chunks Retired payload backlog allocation chunks (AC) awaiting cleanup.")
		_, _ = fmt.Fprintln(w, "# TYPE sbs_service_retired_payload_backlog_chunks gauge")
		_, _ = fmt.Fprintf(w, "sbs_service_retired_payload_backlog_chunks %d\n", snapshot.RetiredPayloadBacklogChunks)
		_, _ = fmt.Fprintln(w, "# HELP sbs_service_retired_payload_failed_batches Failed payload-gc child batches awaiting retry.")
		_, _ = fmt.Fprintln(w, "# TYPE sbs_service_retired_payload_failed_batches gauge")
		_, _ = fmt.Fprintf(w, "sbs_service_retired_payload_failed_batches %d\n", snapshot.RetiredPayloadFailedBatches)
		_, _ = fmt.Fprintln(w, "# HELP sbs_service_retired_payload_oldest_failed_batch_age_seconds Age in seconds of the oldest failed payload-gc child batch.")
		_, _ = fmt.Fprintln(w, "# TYPE sbs_service_retired_payload_oldest_failed_batch_age_seconds gauge")
		_, _ = fmt.Fprintf(w, "sbs_service_retired_payload_oldest_failed_batch_age_seconds %d\n", snapshot.RetiredPayloadFailedAgeSec)
		_, _ = fmt.Fprintln(w, "# HELP sbs_service_transition_failed_batches Failed transition child batches awaiting retry.")
		_, _ = fmt.Fprintln(w, "# TYPE sbs_service_transition_failed_batches gauge")
		_, _ = fmt.Fprintf(w, "sbs_service_transition_failed_batches %d\n", snapshot.TransitionFailedBatches)
		_, _ = fmt.Fprintln(w, "# HELP sbs_service_repair_backlog_failed_total Failed repair/rebalance transition child batches awaiting retry.")
		_, _ = fmt.Fprintln(w, "# TYPE sbs_service_repair_backlog_failed_total gauge")
		_, _ = fmt.Fprintf(w, "sbs_service_repair_backlog_failed_total %d\n", snapshot.TransitionFailedBatches)
		_, _ = fmt.Fprintln(w, "# HELP sbs_service_transition_recent_batches Active transition child batches touching recently mutated pages.")
		_, _ = fmt.Fprintln(w, "# TYPE sbs_service_transition_recent_batches gauge")
		_, _ = fmt.Fprintf(w, "sbs_service_transition_recent_batches %d\n", snapshot.TransitionRecentBatches)
		_, _ = fmt.Fprintln(w, "# HELP sbs_service_transition_small_batches Active transition child batches with single-page backlog.")
		_, _ = fmt.Fprintln(w, "# TYPE sbs_service_transition_small_batches gauge")
		_, _ = fmt.Fprintf(w, "sbs_service_transition_small_batches %d\n", snapshot.TransitionSmallBatches)
		_, _ = fmt.Fprintln(w, "# HELP sbs_service_transition_requeued Parent transitions requeued for retry.")
		_, _ = fmt.Fprintln(w, "# TYPE sbs_service_transition_requeued gauge")
		_, _ = fmt.Fprintf(w, "sbs_service_transition_requeued %d\n", snapshot.TransitionRequeued)
		_, _ = fmt.Fprintln(w, "# HELP sbs_service_transition_retry_pages Remaining retry pages across requeued transitions.")
		_, _ = fmt.Fprintln(w, "# TYPE sbs_service_transition_retry_pages gauge")
		_, _ = fmt.Fprintf(w, "sbs_service_transition_retry_pages %d\n", snapshot.TransitionRetryPages)
		_, _ = fmt.Fprintln(w, "# HELP sbs_service_transition_retry_windows Remaining retry windows across requeued transitions.")
		_, _ = fmt.Fprintln(w, "# TYPE sbs_service_transition_retry_windows gauge")
		_, _ = fmt.Fprintf(w, "sbs_service_transition_retry_windows %d\n", snapshot.TransitionRetryWindows)
		_, _ = fmt.Fprintln(w, "# HELP sbs_service_transition_retry_window_bytes Remaining retry window bytes across requeued transitions.")
		_, _ = fmt.Fprintln(w, "# TYPE sbs_service_transition_retry_window_bytes gauge")
		_, _ = fmt.Fprintf(w, "sbs_service_transition_retry_window_bytes %d\n", snapshot.TransitionRetryWindowBytes)
		_, _ = fmt.Fprintln(w, "# HELP sbs_service_transition_retry_window_chunks Remaining retry window allocation chunks (AC) across requeued transitions.")
		_, _ = fmt.Fprintln(w, "# TYPE sbs_service_transition_retry_window_chunks gauge")
		_, _ = fmt.Fprintf(w, "sbs_service_transition_retry_window_chunks %d\n", snapshot.TransitionRetryWindowChunks)
		_, _ = fmt.Fprintln(w, "# HELP sbs_service_maintenance_cooldown_volumes Volumes temporarily deprioritized by maintenance fairness cooldown.")
		_, _ = fmt.Fprintln(w, "# TYPE sbs_service_maintenance_cooldown_volumes gauge")
		_, _ = fmt.Fprintf(w, "sbs_service_maintenance_cooldown_volumes %d\n", snapshot.MaintenanceCooldownVolumes)
		_, _ = fmt.Fprintln(w, "# HELP sbs_service_maintenance_cooldown_max_remaining_seconds Longest remaining fairness cooldown across volumes.")
		_, _ = fmt.Fprintln(w, "# TYPE sbs_service_maintenance_cooldown_max_remaining_seconds gauge")
		_, _ = fmt.Fprintf(w, "sbs_service_maintenance_cooldown_max_remaining_seconds %d\n", snapshot.MaintenanceCooldownMaxSec)
		_, _ = fmt.Fprintln(w, "# HELP sbs_service_nodes_with_probe_failures Nodes with non-zero reconciler probe failure streak.")
		_, _ = fmt.Fprintln(w, "# TYPE sbs_service_nodes_with_probe_failures gauge")
		_, _ = fmt.Fprintf(w, "sbs_service_nodes_with_probe_failures %d\n", snapshot.NodesWithProbeFailures)
		_, _ = fmt.Fprintln(w, "# HELP sbs_service_max_consecutive_probe_failures Maximum reconciler probe failure streak across nodes.")
		_, _ = fmt.Fprintln(w, "# TYPE sbs_service_max_consecutive_probe_failures gauge")
		_, _ = fmt.Fprintf(w, "sbs_service_max_consecutive_probe_failures %d\n", snapshot.MaxProbeFailures)
		_, _ = fmt.Fprintln(w, "# HELP sbs_service_nodes_in_recovery_cooldown Nodes excluded from new placement because they recently recovered.")
		_, _ = fmt.Fprintln(w, "# TYPE sbs_service_nodes_in_recovery_cooldown gauge")
		_, _ = fmt.Fprintf(w, "sbs_service_nodes_in_recovery_cooldown %d\n", snapshot.NodesInRecoveryCooldown)
		_, _ = fmt.Fprintln(w, "# HELP sbs_service_max_recovery_cooldown_remaining_seconds Longest remaining recovered-node cooldown before reuse.")
		_, _ = fmt.Fprintln(w, "# TYPE sbs_service_max_recovery_cooldown_remaining_seconds gauge")
		_, _ = fmt.Fprintf(w, "sbs_service_max_recovery_cooldown_remaining_seconds %d\n", snapshot.MaxRecoveryCooldownSec)
		_, _ = fmt.Fprintln(w, "# HELP sbs_service_transition_oldest_failed_batch_age_seconds Age in seconds of the oldest failed transition child batch.")
		_, _ = fmt.Fprintln(w, "# TYPE sbs_service_transition_oldest_failed_batch_age_seconds gauge")
		_, _ = fmt.Fprintf(w, "sbs_service_transition_oldest_failed_batch_age_seconds %d\n", snapshot.TransitionFailedAgeSec)
		_, _ = fmt.Fprintln(w, "# HELP sbs_service_repair_backlog_oldest_age_seconds Age in seconds of the oldest failed repair/rebalance transition child batch.")
		_, _ = fmt.Fprintln(w, "# TYPE sbs_service_repair_backlog_oldest_age_seconds gauge")
		_, _ = fmt.Fprintf(w, "sbs_service_repair_backlog_oldest_age_seconds %d\n", snapshot.TransitionFailedAgeSec)
		_, _ = fmt.Fprintln(w, "# HELP sbs_service_rebalance_backlog_oscillation_window_seconds Maintenance fairness cooldown window that explains normal 0/1 rebalance oscillation.")
		_, _ = fmt.Fprintln(w, "# TYPE sbs_service_rebalance_backlog_oscillation_window_seconds gauge")
		_, _ = fmt.Fprintf(w, "sbs_service_rebalance_backlog_oscillation_window_seconds %d\n", snapshot.MaintenanceCooldownMaxSec)
		_, _ = fmt.Fprintln(w, "# HELP sbs_service_operations Number of admin operations by state.")
		_, _ = fmt.Fprintln(w, "# TYPE sbs_service_operations gauge")
		_, _ = fmt.Fprintf(w, "sbs_service_operations{state=\"total\"} %d\n", snapshot.OperationsTotal)
		_, _ = fmt.Fprintf(w, "sbs_service_operations{state=\"running\"} %d\n", snapshot.OperationsRunning)
		_, _ = fmt.Fprintf(w, "sbs_service_operations{state=\"failed\"} %d\n", snapshot.OperationsFailed)
		_, _ = fmt.Fprintf(w, "sbs_service_operations{state=\"completed\"} %d\n", snapshot.OperationsCompleted)
		_, _ = fmt.Fprintf(w, "sbs_service_operations{state=\"canceled\"} %d\n", snapshot.OperationsCanceled)
		_, _ = fmt.Fprintln(w, "# HELP sbs_service_throttle_config Current maintenance concurrency configuration.")
		_, _ = fmt.Fprintln(w, "# TYPE sbs_service_throttle_config gauge")
		_, _ = fmt.Fprintf(w, "sbs_service_throttle_config{kind=\"repairs\"} %d\n", settings.maxConcurrentRepairs)
		_, _ = fmt.Fprintf(w, "sbs_service_throttle_config{kind=\"rebalances\"} %d\n", settings.maxConcurrentRebalances)
		_, _ = fmt.Fprintf(w, "sbs_service_throttle_config{kind=\"drains\"} %d\n", settings.maxConcurrentDrains)
		_, _ = fmt.Fprintf(w, "sbs_service_throttle_config{kind=\"payload_gc\"} %d\n", settings.maxConcurrentPayloadGCs)
		_, _ = fmt.Fprintln(w, "# HELP sbs_service_pause_state Whether a maintenance category is paused.")
		_, _ = fmt.Fprintln(w, "# TYPE sbs_service_pause_state gauge")
		_, _ = fmt.Fprintf(w, "sbs_service_pause_state{kind=\"repairs\"} %d\n", boolToMetric(settings.pauseRepairs))
		_, _ = fmt.Fprintf(w, "sbs_service_pause_state{kind=\"rebalances\"} %d\n", boolToMetric(settings.pauseRebalances))
		_, _ = fmt.Fprintf(w, "sbs_service_pause_state{kind=\"drains\"} %d\n", boolToMetric(settings.pauseDrains))
		_, _ = fmt.Fprintf(w, "sbs_service_pause_state{kind=\"payload_gc\"} %d\n", boolToMetric(settings.pausePayloadGCs))
		_, _ = fmt.Fprintln(w, "# HELP sbs_service_placement_apply_requests_total Internal placement apply RPC requests by outcome class.")
		_, _ = fmt.Fprintln(w, "# TYPE sbs_service_placement_apply_requests_total counter")
		for _, class := range sortedPlacementApplyClasses(placementApplyStats.RequestsByClass) {
			_, _ = fmt.Fprintf(w, "sbs_service_placement_apply_requests_total{class=%q} %d\n", class, placementApplyStats.RequestsByClass[class])
		}
		_, _ = fmt.Fprintln(w, "# HELP sbs_service_placement_apply_duration_seconds_total Total internal placement apply RPC handling duration.")
		_, _ = fmt.Fprintln(w, "# TYPE sbs_service_placement_apply_duration_seconds_total counter")
		_, _ = fmt.Fprintf(w, "sbs_service_placement_apply_duration_seconds_total %.9f\n", placementApplyStats.DurationTotalSeconds)
		_, _ = fmt.Fprintln(w, "# HELP sbs_service_write_session_requests_total Internal write session RPC requests by outcome class.")
		_, _ = fmt.Fprintln(w, "# TYPE sbs_service_write_session_requests_total counter")
		for _, class := range sortedPlacementApplyClasses(writeSessionStats.RequestsByClass) {
			_, _ = fmt.Fprintf(w, "sbs_service_write_session_requests_total{class=%q} %d\n", class, writeSessionStats.RequestsByClass[class])
		}
		_, _ = fmt.Fprintln(w, "# HELP sbs_service_write_session_duration_seconds_total Total internal write session RPC handling duration.")
		_, _ = fmt.Fprintln(w, "# TYPE sbs_service_write_session_duration_seconds_total counter")
		_, _ = fmt.Fprintf(w, "sbs_service_write_session_duration_seconds_total %.9f\n", writeSessionStats.DurationTotalSeconds)
		_, _ = fmt.Fprintln(w, "# HELP sbs_service_chunk_id_allocator_requests_total Internal chunk ID allocator RPC requests by outcome class.")
		_, _ = fmt.Fprintln(w, "# TYPE sbs_service_chunk_id_allocator_requests_total counter")
		for _, class := range sortedPlacementApplyClasses(chunkIDAllocatorStats.RequestsByClass) {
			_, _ = fmt.Fprintf(w, "sbs_service_chunk_id_allocator_requests_total{class=%q} %d\n", class, chunkIDAllocatorStats.RequestsByClass[class])
		}
		_, _ = fmt.Fprintln(w, "# HELP sbs_service_chunk_id_allocator_duration_seconds_total Total internal chunk ID allocator RPC handling duration.")
		_, _ = fmt.Fprintln(w, "# TYPE sbs_service_chunk_id_allocator_duration_seconds_total counter")
		_, _ = fmt.Fprintf(w, "sbs_service_chunk_id_allocator_duration_seconds_total %.9f\n", chunkIDAllocatorStats.DurationTotalSeconds)
		_, _ = fmt.Fprintln(w, "# HELP sbs_service_placement_resolver_requests_total Internal placement resolver RPC requests by outcome class.")
		_, _ = fmt.Fprintln(w, "# TYPE sbs_service_placement_resolver_requests_total counter")
		for _, class := range sortedPlacementApplyClasses(placementResolverStats.RequestsByClass) {
			_, _ = fmt.Fprintf(w, "sbs_service_placement_resolver_requests_total{class=%q} %d\n", class, placementResolverStats.RequestsByClass[class])
		}
		_, _ = fmt.Fprintln(w, "# HELP sbs_service_placement_resolver_duration_seconds_total Total internal placement resolver RPC handling duration.")
		_, _ = fmt.Fprintln(w, "# TYPE sbs_service_placement_resolver_duration_seconds_total counter")
		_, _ = fmt.Fprintf(w, "sbs_service_placement_resolver_duration_seconds_total %.9f\n", placementResolverStats.DurationTotalSeconds)
	})
	return mux
}

func bootstrapKey(root string) string {
	return fmt.Sprintf("%s/bootstrap", root)
}

func operationsSeqKey(root string) string {
	return fmt.Sprintf("%s/admin/operations-seq", root)
}

func operationsPrefix(root string) string {
	return fmt.Sprintf("%s/admin/operations/", root)
}

func operationKey(root, opID string) string {
	return fmt.Sprintf("%s%s", operationsPrefix(root), opID)
}

func volumeSpecKey(root, volumeID string) string {
	return fmt.Sprintf("%s/admin/volumes/%s/spec", root, volumeID)
}

func volumePrefix(root, volumeID string) string {
	return fmt.Sprintf("%s/volumes/%s/", root, volumeID)
}

func cloneOperation(op *adminv1.OperationStatus) *adminv1.OperationStatus {
	if op == nil {
		return nil
	}
	return proto.Clone(op).(*adminv1.OperationStatus)
}

func parseEndpoint(addr string) clustermeta.SBSEndpoint {
	host, port := splitHostPortLoose(addr)
	return clustermeta.SBSEndpoint{Address: host, Port: uint16(port)}
}

func splitHostPortLoose(addr string) (string, int) {
	host, portStr, err := net.SplitHostPort(addr)
	if err != nil {
		return addr, 0
	}
	var port int
	fmt.Sscanf(portStr, "%d", &port)
	return host, port
}

func (s *server) nodeToProto(ctx context.Context, rec clustermeta.NodeMembershipRecord) *adminv1.NodeSummary {
	out := &adminv1.NodeSummary{
		NodeId:             rec.NodeID,
		ReplicaId:          rec.ReplicaID,
		Lifecycle:          lifecycleToProto(rec.LifecycleState),
		Health:             healthToProto(rec.HealthState),
		GrpcEndpoint:       endpointString(rec.SBSEndpoints),
		AdminHttpEndpoint:  rec.AdminHTTPEndpoint,
		Zone:               rec.Zone,
		LastHeartbeatTime:  timestamppb.New(time.Unix(rec.LastHeartbeatUnix, 0).UTC()),
		ClusterId:          rec.ClusterID,
		SbsClusterId:       rec.SBSClusterID,
		StoreIds:           slices.Clone(rec.StoreIDs),
		Roles:              slices.Clone(rec.Roles),
		DesiredState:       rec.DesiredState,
		ObservedState:      rec.ObservedState,
		Generation:         rec.Generation,
		MembershipRevision: rec.MembershipRevision,
		Tombstone:          rec.Tombstone,
		UpdatedBy:          rec.UpdatedBy,
		UpdateReason:       rec.UpdateReason,
	}
	if rec.CreatedAtUnix > 0 {
		out.CreatedTime = timestamppb.New(time.Unix(rec.CreatedAtUnix, 0).UTC())
	}
	if rec.UpdatedAtUnix > 0 {
		out.UpdatedTime = timestamppb.New(time.Unix(rec.UpdatedAtUnix, 0).UTC())
	}
	detail, err := s.repo.GetNodeHealthDetail(ctx, rec.NodeID)
	if err != nil {
		return out
	}
	if detail.LastProbeUnix > 0 {
		probeTime := timestamppb.New(time.Unix(detail.LastProbeUnix, 0).UTC())
		out.LastProbeTime = probeTime
		out.LastHeartbeatTime = probeTime
	}
	if detail.RecoveryEligibleAtUnix > 0 {
		out.RecoveryEligibleTime = timestamppb.New(time.Unix(detail.RecoveryEligibleAtUnix, 0).UTC())
	}
	out.LastProbeError = detail.LastProbeError
	out.ConsecutiveProbeFailures = detail.ConsecutiveProbeFailures
	out.ConsecutiveProbeSuccesses = detail.ConsecutiveProbeSuccesses
	out.HealthReason = detail.HealthReason
	out.HealthUpdatedBy = string(detail.HealthUpdatedBy)
	return out
}

func membershipProjectionStatusToProto(in clustermeta.MembershipProjectionStatus) *adminv1.MembershipProjectionStatus {
	return &adminv1.MembershipProjectionStatus{
		MembershipRevision:           in.MembershipRevision,
		MembershipProjectionRevision: in.MembershipProjectionRevision,
		ProjectionLagMs:              in.ProjectionLagMS,
		ProjectionHealth:             in.ProjectionHealth,
		ProjectionStale:              in.Stale,
		ProjectionRebuildCount:       in.ProjectionRebuildCount,
		ProjectionResyncCount:        in.ProjectionResyncCount,
		FirstError:                   in.FirstError,
		LastError:                    in.LastError,
	}
}

func topologyZoneToProto(rec clustermeta.TopologyZoneRecord) *adminv1.TopologyZoneSummary {
	out := &adminv1.TopologyZoneSummary{
		ZoneId:      rec.ZoneID,
		DisplayName: rec.DisplayName,
		Lifecycle:   topologyZoneLifecycleToProto(rec.Lifecycle),
		Labels:      copyStringMap(rec.Labels),
	}
	if rec.CreatedAtUnix > 0 {
		out.CreatedTime = unixTimestamp(rec.CreatedAtUnix)
	}
	if rec.UpdatedAtUnix > 0 {
		out.UpdatedTime = unixTimestamp(rec.UpdatedAtUnix)
	}
	return out
}

func topologyZoneLifecycleToProto(v clustermeta.TopologyZoneLifecycle) adminv1.TopologyZoneLifecycle {
	switch v {
	case clustermeta.TopologyZoneLifecycleDisabled:
		return adminv1.TopologyZoneLifecycle_TOPOLOGY_ZONE_LIFECYCLE_DISABLED
	case clustermeta.TopologyZoneLifecycleRetiring:
		return adminv1.TopologyZoneLifecycle_TOPOLOGY_ZONE_LIFECYCLE_RETIRING
	case clustermeta.TopologyZoneLifecycleActive:
		return adminv1.TopologyZoneLifecycle_TOPOLOGY_ZONE_LIFECYCLE_ACTIVE
	default:
		return adminv1.TopologyZoneLifecycle_TOPOLOGY_ZONE_LIFECYCLE_UNSPECIFIED
	}
}

func ecProfileToProto(rec clustermeta.ECProfileRecord) *adminv1.ECProfileSummary {
	rec = clustermeta.NormalizeECProfile(rec)
	out := &adminv1.ECProfileSummary{
		ProfileId:                    rec.ProfileID,
		CodecId:                      rec.CodecID,
		DataShards:                   rec.DataShards,
		ParityShards:                 rec.ParityShards,
		StripeUnitBytes:              rec.StripeUnitBytes,
		FailureDomain:                rec.FailureDomain,
		MaxUnavailableFailureDomains: rec.MaxUnavailableFailureDomains,
		MaxShardsPerFailureDomain:    rec.MaxShardsPerFailureDomain,
		Lifecycle:                    ecProfileLifecycleToProto(rec.Lifecycle),
		LabOverride:                  rec.LabOverride,
	}
	if rec.CreatedAtUnix > 0 {
		out.CreatedTime = unixTimestamp(rec.CreatedAtUnix)
	}
	if rec.UpdatedAtUnix > 0 {
		out.UpdatedTime = unixTimestamp(rec.UpdatedAtUnix)
	}
	return out
}

func ecProfileLifecycleToProto(v clustermeta.ECProfileLifecycle) adminv1.ECProfileLifecycle {
	switch v {
	case clustermeta.ECProfileLifecycleDisabled:
		return adminv1.ECProfileLifecycle_EC_PROFILE_LIFECYCLE_DISABLED
	case clustermeta.ECProfileLifecycleActive:
		return adminv1.ECProfileLifecycle_EC_PROFILE_LIFECYCLE_ACTIVE
	default:
		return adminv1.ECProfileLifecycle_EC_PROFILE_LIFECYCLE_UNSPECIFIED
	}
}

func topologyZoneLifecycleFromProto(v adminv1.TopologyZoneLifecycle) (clustermeta.TopologyZoneLifecycle, error) {
	switch v {
	case adminv1.TopologyZoneLifecycle_TOPOLOGY_ZONE_LIFECYCLE_ACTIVE:
		return clustermeta.TopologyZoneLifecycleActive, nil
	case adminv1.TopologyZoneLifecycle_TOPOLOGY_ZONE_LIFECYCLE_DISABLED:
		return clustermeta.TopologyZoneLifecycleDisabled, nil
	case adminv1.TopologyZoneLifecycle_TOPOLOGY_ZONE_LIFECYCLE_RETIRING:
		return clustermeta.TopologyZoneLifecycleRetiring, nil
	default:
		return "", fmt.Errorf("unsupported topology zone lifecycle %s", v.String())
	}
}

func normalizeRequestedTopologyMode(raw string) (string, error) {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "", placement.TopologyModeLegacy:
		return placement.TopologyModeLegacy, nil
	case placement.TopologyModePrefer:
		return placement.TopologyModePrefer, nil
	case placement.TopologyModeStrict:
		return placement.TopologyModeStrict, nil
	default:
		return "", fmt.Errorf("unsupported topology_mode %q", raw)
	}
}

func normalizeRequestedRedundancyBackend(raw string) (string, error) {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "", clustermeta.RedundancyBackendReplicated:
		return clustermeta.RedundancyBackendReplicated, nil
	case clustermeta.RedundancyBackendEC:
		return clustermeta.RedundancyBackendEC, nil
	default:
		return "", fmt.Errorf("unsupported redundancy_backend %q", raw)
	}
}

func normalizeECVolumeTopologyMode(raw string, weakPlacementAllowed bool) (string, error) {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "", placement.TopologyModeStrict:
		return placement.TopologyModeStrict, nil
	case placement.ECTopologyModeWeak:
		if !weakPlacementAllowed {
			return "", fmt.Errorf("topology_mode weak requires weak_placement_allowed")
		}
		return placement.ECTopologyModeWeak, nil
	default:
		return "", fmt.Errorf("ec volume topology_mode must be strict or weak")
	}
}

func effectiveVolumeTopologyMode(volumeMode, specMode string) string {
	if mode := strings.TrimSpace(volumeMode); mode != "" {
		return mode
	}
	if mode := strings.TrimSpace(specMode); mode != "" {
		return mode
	}
	return placement.TopologyModeLegacy
}

func effectiveVolumeRedundancyBackend(volume clustermeta.VolumeState, spec volumeSpecRecord) string {
	if backend := strings.TrimSpace(volume.RedundancyBackend); backend != "" {
		return backend
	}
	if backend := strings.TrimSpace(spec.RedundancyBackend); backend != "" {
		return backend
	}
	return clustermeta.RedundancyBackendReplicated
}

func effectiveMetadataVolumeRedundancyBackend(spec clustermeta.VolumeSpecRecord) string {
	if backend := strings.TrimSpace(spec.RedundancyBackend); backend != "" {
		return backend
	}
	return clustermeta.RedundancyBackendReplicated
}

func copyStringMap(in map[string]string) map[string]string {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]string, len(in))
	for k, v := range in {
		key := strings.TrimSpace(k)
		if key == "" {
			continue
		}
		out[key] = strings.TrimSpace(v)
	}
	return out
}

func acceptedOperation(op *adminv1.OperationStatus, message string) *adminv1.OperationHandle {
	if op == nil {
		return &adminv1.OperationHandle{Accepted: true, Message: message}
	}
	return &adminv1.OperationHandle{Accepted: true, OperationId: op.GetOperationId(), Message: message}
}

func (s *server) replicaTargetsView(ctx context.Context) ([]*adminv1.ReplicaTargetView, error) {
	return sbscluster.BuildReplicaTargetViews(ctx, s.repo, s.currentTime(), nodeAdminHTTPEndpoint)
}

func (s *server) volumeToProto(ctx context.Context, rec clustermeta.VolumeState) *adminv1.VolumeSummary {
	spec, _ := s.getVolumeSpec(ctx, rec.VolumeID)
	backlog := s.summarizeVolumeTransitions(ctx, rec.VolumeID)
	retiredBacklog := s.summarizeRetiredPayloadBacklog(ctx, rec.VolumeID, spec.ChunkSizeBytes)
	cooldownActive, cooldownRemaining := s.maintenanceCooldownState(rec.VolumeID, s.currentTime())
	return &adminv1.VolumeSummary{
		VolumeId:                    rec.VolumeID,
		SizeBytes:                   spec.SizeBytes,
		BlockSize:                   spec.BlockSize,
		ChunkSizeBytes:              spec.ChunkSizeBytes,
		ExtentPageBytes:             spec.ExtentPageBytes,
		TopologyMode:                effectiveVolumeTopologyMode(rec.TopologyMode, spec.TopologyMode),
		RedundancyBackend:           effectiveVolumeRedundancyBackend(rec, spec),
		EcProfileId:                 spec.ECProfileID,
		EcCodecId:                   spec.ECCodecID,
		EcDataShards:                spec.ECDataShards,
		EcParityShards:              spec.ECParityShards,
		EcStripeUnitBytes:           spec.ECStripeUnitBytes,
		EcFailureDomain:             spec.ECFailureDomain,
		WeakPlacementAllowed:        spec.WeakPlacementAllowed,
		ProtectedState:              protectedStateToProto(spec.ProtectedState),
		Health:                      volumeHealthToProto(rec.Status),
		VolumeRevision:              rec.Revision,
		RepairBacklog:               backlog.RepairCount,
		RepairBacklogBytes:          backlog.RepairBytes,
		RepairBacklogChunks:         backlog.RepairChunks,
		RebalanceBacklog:            backlog.RebalanceCount,
		RebalanceBacklogBytes:       backlog.RebalanceBytes,
		RebalanceBacklogChunks:      backlog.RebalanceChunks,
		DrainBacklog:                backlog.DrainCount,
		DrainBacklogBytes:           backlog.DrainBytes,
		DrainBacklogChunks:          backlog.DrainChunks,
		RetiredPayloadBacklogBytes:  retiredBacklog.Bytes,
		RetiredPayloadBacklogChunks: retiredBacklog.Chunks,
		RetiredPayloadFailedBatches: retiredBacklog.FailedBatches,
		RetiredPayloadOldestFailedBatchAgeSeconds: retiredBacklog.OldestFailedAgeSec,
		TransitionFailedBatches:                   backlog.FailedBatches,
		TransitionOldestFailedBatchAgeSeconds:     backlog.OldestFailedAgeSec,
		TransitionRecentBatches:                   backlog.RecentBatches,
		TransitionSmallBatches:                    backlog.SmallBatches,
		TransitionRequeued:                        backlog.RequeuedCount,
		TransitionRetryPages:                      backlog.RetryPages,
		TransitionRetryWindows:                    backlog.RetryWindows,
		TransitionRetryWindowBytes:                backlog.RetryWindowBytes,
		TransitionRetryWindowChunks:               backlog.RetryWindowChunks,
		MaintenanceCooldownActive:                 cooldownActive,
		MaintenanceCooldownRemainingSeconds:       uint64(maxInt64(cooldownRemaining, 0)),
	}
}

func (s *server) volumeToSpecOnlyProto(ctx context.Context, rec clustermeta.VolumeState) *adminv1.VolumeSummary {
	spec, _ := s.getVolumeSpec(ctx, rec.VolumeID)
	return &adminv1.VolumeSummary{
		VolumeId:             rec.VolumeID,
		SizeBytes:            spec.SizeBytes,
		BlockSize:            spec.BlockSize,
		ChunkSizeBytes:       spec.ChunkSizeBytes,
		ExtentPageBytes:      spec.ExtentPageBytes,
		TopologyMode:         effectiveVolumeTopologyMode(rec.TopologyMode, spec.TopologyMode),
		RedundancyBackend:    effectiveVolumeRedundancyBackend(rec, spec),
		EcProfileId:          spec.ECProfileID,
		EcCodecId:            spec.ECCodecID,
		EcDataShards:         spec.ECDataShards,
		EcParityShards:       spec.ECParityShards,
		EcStripeUnitBytes:    spec.ECStripeUnitBytes,
		EcFailureDomain:      spec.ECFailureDomain,
		WeakPlacementAllowed: spec.WeakPlacementAllowed,
		ProtectedState:       protectedStateToProto(spec.ProtectedState),
		Health:               volumeHealthToProto(rec.Status),
		VolumeRevision:       rec.Revision,
	}
}

func (s *server) volumeToProtoCached(ctx context.Context, rec clustermeta.VolumeState) *adminv1.VolumeSummary {
	if cached, ok := s.viewCache.getVolume(rec.VolumeID); ok {
		return cached
	}
	out := s.volumeToProto(ctx, rec)
	s.viewCache.storeVolume(rec.VolumeID, out)
	return out
}

func generateSnapshotID(volumeID string, t time.Time) string {
	return fmt.Sprintf("snap-%s-%s", volumeID, t.UTC().Format("20060102T150405.000000000Z"))
}

func generateCloneID(sourceSnapshotID string, t time.Time) string {
	return fmt.Sprintf("clone-%s-%s", sourceSnapshotID, t.UTC().Format("20060102T150405.000000000Z"))
}

func snapshotRecordToProto(rec clustermeta.SnapshotRecord) *adminv1.SnapshotSummary {
	return &adminv1.SnapshotSummary{
		SnapshotId:               rec.SnapshotID,
		SourceVolumeId:           rec.SourceVolumeID,
		SnapshotRootId:           rec.SnapshotRootID,
		State:                    snapshotStateToProto(rec.State),
		CreatedAt:                unixTimestamp(rec.CreatedAtUnix),
		CutVolumeRevision:        rec.CutVolumeRevision,
		AllocationChunkSizeBytes: rec.AllocationChunkSizeBytes,
		AllocationPageSizeBytes:  rec.AllocationPageSizeBytes,
		SourceSizeBytes:          rec.SourceSizeBytes,
		CloneReferenceCount:      rec.CloneReferenceCount,
		ErrorMessage:             rec.ErrorMessage,
	}
}

func cloneRecordToProto(rec clustermeta.CloneRecord) *adminv1.CloneSummary {
	return &adminv1.CloneSummary{
		CloneId:                  rec.CloneID,
		SourceSnapshotId:         rec.SourceSnapshotID,
		SourceVolumeId:           rec.SourceVolumeID,
		CloneBaseRootId:          rec.CloneBaseRootID,
		State:                    cloneStateToProto(rec.State),
		CreatedAt:                unixTimestamp(rec.CreatedAtUnix),
		MaterializedVolumeId:     rec.MaterializedVolumeID,
		AllocationChunkSizeBytes: rec.AllocationChunkSizeBytes,
		AllocationPageSizeBytes:  rec.AllocationPageSizeBytes,
		SizeBytes:                rec.SizeBytes,
		SourceSizeBytes:          rec.SourceSizeBytes,
		DeltaPageCount:           rec.DeltaPageCount,
		DeltaObjectCount:         rec.DeltaObjectCount,
		ErrorMessage:             rec.ErrorMessage,
	}
}

func snapshotStateToProto(state clustermeta.SnapshotState) adminv1.SnapshotState {
	switch state {
	case clustermeta.SnapshotStateCreating:
		return adminv1.SnapshotState_SNAPSHOT_STATE_CREATING
	case clustermeta.SnapshotStateAvailable:
		return adminv1.SnapshotState_SNAPSHOT_STATE_AVAILABLE
	case clustermeta.SnapshotStateDeleting:
		return adminv1.SnapshotState_SNAPSHOT_STATE_DELETING
	case clustermeta.SnapshotStateDeleted:
		return adminv1.SnapshotState_SNAPSHOT_STATE_DELETED
	case clustermeta.SnapshotStateFailed:
		return adminv1.SnapshotState_SNAPSHOT_STATE_FAILED
	default:
		return adminv1.SnapshotState_SNAPSHOT_STATE_UNSPECIFIED
	}
}

func cloneStateToProto(state clustermeta.CloneState) adminv1.CloneState {
	switch state {
	case clustermeta.CloneStateCreating:
		return adminv1.CloneState_CLONE_STATE_CREATING
	case clustermeta.CloneStateAvailable:
		return adminv1.CloneState_CLONE_STATE_AVAILABLE
	case clustermeta.CloneStateMaterializing:
		return adminv1.CloneState_CLONE_STATE_MATERIALIZING
	case clustermeta.CloneStateMaterialized:
		return adminv1.CloneState_CLONE_STATE_MATERIALIZED
	case clustermeta.CloneStateDeleting:
		return adminv1.CloneState_CLONE_STATE_DELETING
	case clustermeta.CloneStateDeleted:
		return adminv1.CloneState_CLONE_STATE_DELETED
	case clustermeta.CloneStateFailed:
		return adminv1.CloneState_CLONE_STATE_FAILED
	default:
		return adminv1.CloneState_CLONE_STATE_UNSPECIFIED
	}
}

func (s *server) putVolumeSpec(ctx context.Context, rec volumeSpecRecord) error {
	raw, err := json.Marshal(rec)
	if err != nil {
		return err
	}
	return s.kv.Set(ctx, volumeSpecKey(s.root, rec.VolumeID), raw)
}

func (s *server) getVolumeSpec(ctx context.Context, volumeID string) (volumeSpecRecord, error) {
	raw, found, err := s.kv.Get(ctx, volumeSpecKey(s.root, volumeID))
	if err != nil {
		return volumeSpecRecord{}, err
	}
	if !found {
		return volumeSpecRecord{}, clustermeta.ErrNotFound
	}
	var rec volumeSpecRecord
	if err := json.Unmarshal(raw, &rec); err != nil {
		return volumeSpecRecord{}, err
	}
	return rec, nil
}

func (s *server) initializeVolumeAllocationPages(ctx context.Context, spec volumeSpecRecord) error {
	return clustermeta.PersistZeroAllocationPages(ctx, s.repo, clustermeta.ZeroAllocationPersistSpec{
		VolumeID:        spec.VolumeID,
		SizeBytes:       spec.SizeBytes,
		ChunkSizeBytes:  spec.ChunkSizeBytes,
		ExtentPageBytes: spec.ExtentPageBytes,
		Revision:        1,
	})
}

func (s *server) removeNode(ctx context.Context, nodeID string, force bool, audit operationAudit) (*adminv1.OperationStatus, error) {
	if err := enforceDependencyMembershipChange(); err != nil {
		return nil, err
	}
	rec, err := s.repo.GetNodeMembership(ctx, nodeID)
	if err != nil {
		if errors.Is(err, clustermeta.ErrNotFound) {
			return nil, status.Errorf(codes.NotFound, "node %q not found", nodeID)
		}
		return nil, status.Errorf(codes.Internal, "get node membership: %v", err)
	}
	if !force && rec.LifecycleState != clustermeta.NodeLifecycleDraining {
		return nil, status.Errorf(codes.FailedPrecondition, "node %q must be draining before remove", nodeID)
	}
	if !force {
		if op := s.ensureLatestDrainOperation(ctx, nodeID); op != nil && op.GetState() != adminv1.OperationState_OPERATION_STATE_COMPLETED {
			return nil, status.Errorf(codes.FailedPrecondition, "node %q drain operation %q is not completed", nodeID, op.GetOperationId())
		}
		if ref, ok, err := s.nodeReplicaReference(ctx, nodeID); err != nil {
			return nil, status.Errorf(codes.Internal, "check node references: %v", err)
		} else if ok {
			return nil, status.Errorf(codes.FailedPrecondition, "node %q still referenced by %s", nodeID, ref)
		}
	}
	opKind := "node.remove"
	if force {
		opKind = "node.force_remove"
	}
	op, err := s.ops.createAudited(opKind, nodeID, "", "removing", adminv1.OperationState_OPERATION_STATE_RUNNING, audit)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "create operation: %v", err)
	}
	rec.LifecycleState = clustermeta.NodeLifecycleRemoved
	rec.HealthState = clustermeta.NodeHealthDown
	rec.DesiredState = string(clustermeta.NodeLifecycleRemoved)
	rec.ObservedState = string(clustermeta.NodeHealthDown)
	rec.Tombstone = true
	rec.LastHeartbeatUnix = s.currentTime().Unix()
	rec.UpdatedBy = audit.Actor
	rec.UpdateReason = audit.Reason
	if _, _, err := s.repo.CompareAndSetNodeMembership(ctx, rec, rec.Generation); err != nil {
		s.failOperation(op.GetOperationId(), err)
		if errors.Is(err, clustermeta.ErrCASConflict) {
			return nil, status.Errorf(codes.Aborted, "node membership changed concurrently: %v", err)
		}
		return nil, status.Errorf(codes.Internal, "put node membership: %v", err)
	}
	if force {
		for _, kind := range []string{"node.drain", "node.leave"} {
			for _, drain := range s.ops.list(kind, adminv1.OperationState_OPERATION_STATE_RUNNING) {
				if drain.GetTargetNodeId() != nodeID {
					continue
				}
				_, _ = s.ops.update(drain.GetOperationId(), func(op *adminv1.OperationStatus) {
					op.State = adminv1.OperationState_OPERATION_STATE_CANCELED
					op.Phase = "force_removed"
					op.BlockingReason = ""
				})
			}
		}
	}
	op, _ = s.ops.update(op.GetOperationId(), func(op *adminv1.OperationStatus) {
		op.State = adminv1.OperationState_OPERATION_STATE_COMPLETED
		op.Phase = "removed"
		op.BlockingReason = ""
		op.ExtentsRemaining = 0
		op.BytesRemaining = 0
	})
	return op, nil
}

func (s *server) nodeReplicaReference(ctx context.Context, nodeID string) (string, bool, error) {
	progress, err := s.computeDrainProgress(ctx, nodeID)
	if err != nil {
		return "", false, err
	}
	if progress.extentsRemaining == 0 {
		return "", false, nil
	}
	return progress.sampleRef, true, nil
}

type drainProgress struct {
	extentsRemaining uint64
	bytesRemaining   uint64
	sampleRef        string
}

func (s *server) computeDrainProgress(ctx context.Context, nodeID string) (drainProgress, error) {
	volumes, err := s.repo.ListVolumeStates(ctx)
	if err != nil {
		return drainProgress{}, err
	}
	maintSvc := s.newMaintenanceService()
	progress := drainProgress{}
	for _, volume := range volumes {
		mappings, err := s.ensureDrainPlacementMappings(ctx, volume, nodeID)
		if err != nil {
			return drainProgress{}, err
		}
		replicaSets, err := s.repo.ListReplicaSets(ctx, volume.VolumeID)
		if err != nil {
			return drainProgress{}, err
		}
		nodePlacements := make(map[string]string)
		for _, rs := range replicaSets {
			for _, replica := range rs.Replicas {
				if replica.NodeID == nodeID {
					nodePlacements[rs.PlacementRef] = rs.ReplicaSetID
				}
			}
		}
		if len(nodePlacements) == 0 {
			continue
		}
		for _, mapping := range mappings {
			replicaSetID, ok := nodePlacements[mapping.PlacementRef]
			if !ok {
				continue
			}
			evaluated, err := maintSvc.EvaluateExtentHealth(ctx, volume.VolumeID, mapping.ExtentID)
			if err != nil {
				return drainProgress{}, err
			}
			progress.extentsRemaining++
			progress.bytesRemaining += evaluated.DataBytes
			if progress.sampleRef == "" {
				progress.sampleRef = fmt.Sprintf("replica_set=%s volume=%s extent=%d", replicaSetID, volume.VolumeID, mapping.ExtentID)
			}
		}
	}
	return progress, nil
}

func (s *server) deleteVolumeArtifacts(ctx context.Context, volumeID string) error {
	mappings, err := s.repo.ListExtentMappings(ctx, volumeID)
	if err != nil && !errors.Is(err, clustermeta.ErrNotFound) {
		return err
	}
	for _, mapping := range mappings {
		if err := s.repo.DeleteExtentMapping(ctx, volumeID, mapping.ExtentID); err != nil {
			return err
		}
	}
	replicaSets, err := s.repo.ListReplicaSets(ctx, volumeID)
	if err != nil && !errors.Is(err, clustermeta.ErrNotFound) {
		return err
	}
	for _, replicaSet := range replicaSets {
		if err := s.repo.DeleteReplicaSet(ctx, volumeID, replicaSet.ReplicaSetID); err != nil {
			return err
		}
	}
	allocationPages, err := s.repo.ListAllocationPages(ctx, volumeID)
	if err != nil && !errors.Is(err, clustermeta.ErrNotFound) {
		return err
	}
	for _, page := range allocationPages {
		if err := s.repo.DeleteAllocationPage(ctx, volumeID, page.PageNo); err != nil {
			return err
		}
	}
	if err := deleteByPrefix(ctx, s.kv, fmt.Sprintf("%s/volumes/%s/idempotency/", s.root, volumeID)); err != nil {
		return err
	}
	if err := deleteByPrefix(ctx, s.kv, placementTransitionsPrefix(s.root, volumeID)); err != nil {
		return err
	}
	if err := s.repo.DeleteVolumeState(ctx, volumeID); err != nil {
		return err
	}
	return s.kv.Delete(ctx, volumeSpecKey(s.root, volumeID))
}

func latestDrainForNode(ops []*adminv1.OperationStatus, nodeID string) *adminv1.OperationStatus {
	var latest *adminv1.OperationStatus
	for _, op := range ops {
		if op.GetTargetNodeId() != nodeID {
			continue
		}
		if latest == nil || op.GetOperationId() > latest.GetOperationId() {
			latest = op
		}
	}
	return latest
}

func (s *server) ensureLatestDrainOperation(ctx context.Context, nodeID string) *adminv1.OperationStatus {
	ops := append(s.ops.list("node.drain", adminv1.OperationState_OPERATION_STATE_UNSPECIFIED), s.ops.list("node.leave", adminv1.OperationState_OPERATION_STATE_UNSPECIFIED)...)
	op := latestDrainForNode(ops, nodeID)
	if op == nil {
		return nil
	}
	return s.refreshDrainOperation(ctx, op)
}

func (s *server) refreshOperation(ctx context.Context, op *adminv1.OperationStatus) *adminv1.OperationStatus {
	if op == nil {
		return nil
	}
	switch op.GetKind() {
	case "node.drain", "node.leave":
		return s.refreshDrainOperation(ctx, op)
	default:
		return op
	}
}

func (s *server) refreshDrainOperation(ctx context.Context, op *adminv1.OperationStatus) *adminv1.OperationStatus {
	if op == nil || op.GetTargetNodeId() == "" {
		return op
	}
	if op.GetState() == adminv1.OperationState_OPERATION_STATE_FAILED || op.GetState() == adminv1.OperationState_OPERATION_STATE_CANCELED {
		return op
	}
	node, err := s.repo.GetNodeMembership(ctx, op.GetTargetNodeId())
	if err != nil && !errors.Is(err, clustermeta.ErrNotFound) {
		_, _ = s.ops.update(op.GetOperationId(), func(cur *adminv1.OperationStatus) {
			cur.State = adminv1.OperationState_OPERATION_STATE_FAILED
			cur.ErrorMessage = err.Error()
		})
		failed, getErr := s.ops.get(op.GetOperationId())
		if getErr == nil {
			return failed
		}
		return op
	}
	if errors.Is(err, clustermeta.ErrNotFound) || node.LifecycleState != clustermeta.NodeLifecycleDraining {
		updated, updateErr := s.ops.update(op.GetOperationId(), func(cur *adminv1.OperationStatus) {
			if cur.State == adminv1.OperationState_OPERATION_STATE_COMPLETED {
				return
			}
			cur.State = adminv1.OperationState_OPERATION_STATE_CANCELED
			cur.Phase = "not_draining"
			cur.BlockingReason = ""
			cur.ExtentsRemaining = 0
			cur.BytesRemaining = 0
		})
		if updateErr == nil {
			return updated
		}
		return op
	}
	progress, err := s.computeDrainProgress(ctx, op.GetTargetNodeId())
	if err != nil {
		_, _ = s.ops.update(op.GetOperationId(), func(cur *adminv1.OperationStatus) {
			cur.State = adminv1.OperationState_OPERATION_STATE_FAILED
			cur.ErrorMessage = err.Error()
		})
		failed, getErr := s.ops.get(op.GetOperationId())
		if getErr == nil {
			return failed
		}
		return op
	}
	updated, err := s.ops.update(op.GetOperationId(), func(cur *adminv1.OperationStatus) {
		cur.ExtentsRemaining = progress.extentsRemaining
		cur.BytesRemaining = progress.bytesRemaining
		if progress.extentsRemaining == 0 {
			cur.State = adminv1.OperationState_OPERATION_STATE_COMPLETED
			cur.Phase = "evacuated"
			cur.BlockingReason = ""
			return
		}
		cur.State = adminv1.OperationState_OPERATION_STATE_RUNNING
		cur.Phase = "evacuating"
		cur.BlockingReason = "awaiting replica evacuation"
	})
	if err != nil {
		return op
	}
	return updated
}

func (s *server) enqueueDrainTransitions(ctx context.Context, nodeID string) error {
	volumes, err := s.repo.ListVolumeStates(ctx)
	if err != nil {
		return err
	}
	maintSvc := s.newMaintenanceService()
	for _, volume := range volumes {
		mappings, err := s.ensureDrainPlacementMappings(ctx, volume, nodeID)
		if err != nil {
			return err
		}
		replicaSets, err := s.repo.ListReplicaSets(ctx, volume.VolumeID)
		if err != nil {
			return err
		}
		byPlacement := make(map[string]clustermeta.ReplicaSetState, len(replicaSets))
		for _, replicaSet := range replicaSets {
			byPlacement[replicaSet.PlacementRef] = replicaSet
		}
		type drainCandidate struct {
			mapping   clustermeta.ExtentMappingRecord
			evaluated *clustermaintenance.EvaluatedExtent
		}
		candidates := make([]drainCandidate, 0, len(mappings))
		for _, mapping := range mappings {
			evaluated, evalErr := maintSvc.EvaluateExtentHealth(ctx, volume.VolumeID, mapping.ExtentID)
			if evalErr != nil {
				continue
			}
			candidates = append(candidates, drainCandidate{mapping: mapping, evaluated: evaluated})
		}
		sort.SliceStable(candidates, func(i, j int) bool {
			return clustermaintenance.CompareEvaluatedExtentPriority(candidates[i].evaluated, candidates[j].evaluated) < 0
		})
		for _, candidate := range candidates {
			mapping := candidate.mapping
			currentReplicaSet, ok := byPlacement[mapping.PlacementRef]
			if !ok || !replicaSetContainsNode(currentReplicaSet, nodeID) {
				continue
			}
			if _, err := s.repo.GetPlacementTransition(ctx, volume.VolumeID, mapping.PlacementRef); err == nil {
				continue
			} else if !errors.Is(err, clustermeta.ErrNotFound) {
				return err
			}
			targetReplicaSet, ok, err := s.planReplacementReplicaSet(ctx, volume.VolumeID, mapping, currentReplicaSet, nodeID, "drain", candidate.evaluated)
			if err != nil {
				return err
			}
			if !ok {
				continue
			}
			if err := s.repo.PutReplicaSet(ctx, targetReplicaSet); err != nil {
				return err
			}
			if err := s.repo.PutPlacementTransition(ctx, clustermeta.PlacementTransitionRecord{
				VolumeID:            volume.VolumeID,
				PlacementRef:        mapping.PlacementRef,
				State:               clustermeta.PlacementTransitionQueued,
				Reason:              "drain",
				CurrentReplicaSetID: currentReplicaSet.ReplicaSetID,
				TargetReplicaSetID:  targetReplicaSet.ReplicaSetID,
				StartedAtUnix:       time.Now().Unix(),
				LastProgressAtUnix:  time.Now().Unix(),
				Attempt:             1,
			}); err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *server) ensureDrainPlacementMappings(ctx context.Context, volume clustermeta.VolumeState, nodeID string) ([]clustermeta.ExtentMappingRecord, error) {
	mappings, err := s.repo.ListExtentMappings(ctx, volume.VolumeID)
	if err != nil {
		return nil, err
	}
	if len(mappings) > 0 {
		return mappings, nil
	}
	if err := s.ensureDrainPlacementCompatibility(ctx, volume, nodeID); err != nil {
		return nil, err
	}
	mappings, err = s.repo.ListExtentMappings(ctx, volume.VolumeID)
	if err != nil {
		return nil, err
	}
	return mappings, nil
}

func (s *server) ensureDrainPlacementCompatibility(ctx context.Context, volume clustermeta.VolumeState, nodeID string) error {
	if strings.TrimSpace(volume.RedundancyBackend) == clustermeta.RedundancyBackendEC {
		return nil
	}
	pages, err := s.repo.ListAllocationPages(ctx, volume.VolumeID)
	if err != nil {
		if errors.Is(err, clustermeta.ErrNotFound) {
			return nil
		}
		return err
	}
	if len(pages) == 0 {
		return nil
	}
	spec, err := s.getVolumeSpec(ctx, volume.VolumeID)
	if err != nil {
		return fmt.Errorf("get volume spec for drain placement compatibility volume=%s: %w", volume.VolumeID, err)
	}
	if effectiveVolumeRedundancyBackend(volume, spec) != clustermeta.RedundancyBackendReplicated {
		return nil
	}
	if spec.SizeBytes == 0 || spec.ReplicationFactor == 0 {
		return nil
	}
	nodes, err := s.selectDrainPlacementCompatibilityNodes(ctx, spec.ReplicationFactor, nodeID)
	if err != nil {
		return err
	}
	layout, err := planVolumeLayout(volume.VolumeID, spec.SizeBytes, spec.ExtentSizeBytes, spec.ReplicationFactor, spec.TopologyMode, nodes)
	if err != nil {
		return err
	}
	for _, extent := range layout.Extents {
		if err := s.putPlannedExtent(ctx, volume.VolumeID, extent); err != nil {
			return err
		}
	}
	structuredlog.Info("sbs.service", "drain_placement_compatibility_materialized",
		structuredlog.F("volume_id", volume.VolumeID),
		structuredlog.F("node_id", nodeID),
		structuredlog.F("allocation_page_count", len(pages)),
		structuredlog.F("extent_count", len(layout.Extents)),
		structuredlog.F("replication_factor", spec.ReplicationFactor),
	)
	return nil
}

func (s *server) selectDrainPlacementCompatibilityNodes(ctx context.Context, replicationFactor uint32, nodeID string) ([]clustermeta.NodeMembershipRecord, error) {
	nodes, err := s.repo.ListNodeMemberships(ctx)
	if err != nil {
		return nil, fmt.Errorf("list node memberships: %w", err)
	}
	var target clustermeta.NodeMembershipRecord
	targetFound := false
	eligible := make([]clustermeta.NodeMembershipRecord, 0, len(nodes))
	for _, node := range nodes {
		if node.NodeID == nodeID {
			if node.LifecycleState != clustermeta.NodeLifecycleRemoved {
				target = node
				targetFound = true
			}
			continue
		}
		if !s.nodeEligibleForNewPlacement(ctx, node) {
			continue
		}
		eligible = append(eligible, node)
	}
	if !targetFound {
		return nil, fmt.Errorf("drain placement compatibility target node %q is not available", nodeID)
	}
	sort.Slice(eligible, func(i, j int) bool {
		if eligible[i].Zone != eligible[j].Zone {
			return eligible[i].Zone < eligible[j].Zone
		}
		return eligible[i].NodeID < eligible[j].NodeID
	})
	selected := []clustermeta.NodeMembershipRecord{target}
	for _, node := range eligible {
		if len(selected) == int(replicationFactor) {
			break
		}
		selected = append(selected, node)
	}
	if len(selected) < int(replicationFactor) {
		return nil, fmt.Errorf("found %d drain placement compatibility nodes including target; need at least %d for replication factor %d", len(selected), replicationFactor, replicationFactor)
	}
	return selected, nil
}

func (s *server) enqueueDrainTransitionsForDrainingNodes(ctx context.Context) error {
	nodes, err := s.repo.ListNodeMemberships(ctx)
	if err != nil {
		return err
	}
	for _, node := range nodes {
		if node.LifecycleState != clustermeta.NodeLifecycleDraining {
			continue
		}
		if err := s.enqueueDrainTransitions(ctx, node.NodeID); err != nil {
			return fmt.Errorf("node %s: %w", node.NodeID, err)
		}
	}
	return nil
}

func (s *server) planReplacementReplicaSet(ctx context.Context, volumeID string, mapping clustermeta.ExtentMappingRecord, current clustermeta.ReplicaSetState, replaceNodeID, reason string, evaluated *clustermaintenance.EvaluatedExtent) (clustermeta.ReplicaSetState, bool, error) {
	maintSvc := s.newMaintenanceService()
	configureMaintenanceService(maintSvc)
	return maintSvc.PlanReplacementReplicaSet(ctx, volumeID, mapping.PlacementRef, current, replaceNodeID, reason, evaluated)
}

func (s *server) selectPlacementNodes(ctx context.Context, replicationFactor uint32) ([]clustermeta.NodeMembershipRecord, error) {
	nodes, err := s.repo.ListNodeMemberships(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "list node memberships: %v", err)
	}
	candidates := make([]clustermeta.NodeMembershipRecord, 0, len(nodes))
	for _, node := range nodes {
		if !s.nodeEligibleForNewPlacement(ctx, node) {
			continue
		}
		candidates = append(candidates, node)
	}
	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].Zone != candidates[j].Zone {
			return candidates[i].Zone < candidates[j].Zone
		}
		return candidates[i].NodeID < candidates[j].NodeID
	})
	if len(candidates) < int(replicationFactor) {
		return nil, status.Errorf(codes.FailedPrecondition, "found %d active nodes; need at least %d for replication factor %d", len(candidates), replicationFactor, replicationFactor)
	}
	selected := make([]clustermeta.NodeMembershipRecord, 0, replicationFactor)
	usedZones := make(map[string]struct{})
	for _, candidate := range candidates {
		if len(selected) == int(replicationFactor) {
			break
		}
		if candidate.Zone != "" {
			if _, ok := usedZones[candidate.Zone]; ok {
				continue
			}
			usedZones[candidate.Zone] = struct{}{}
		}
		selected = append(selected, candidate)
	}
	for _, candidate := range candidates {
		if len(selected) == int(replicationFactor) {
			break
		}
		var already bool
		for _, picked := range selected {
			if picked.NodeID == candidate.NodeID {
				already = true
				break
			}
		}
		if !already {
			selected = append(selected, candidate)
		}
	}
	if len(selected) < int(replicationFactor) {
		return nil, status.Errorf(codes.FailedPrecondition, "insufficient nodes selected for replication factor %d", replicationFactor)
	}
	return selected, nil
}

func replicaSetContainsNode(replicaSet clustermeta.ReplicaSetState, nodeID string) bool {
	for _, replica := range replicaSet.Replicas {
		if replica.NodeID == nodeID {
			return true
		}
	}
	return false
}

func placementTransitionsPrefix(root, volumeID string) string {
	return fmt.Sprintf("%s/volumes/%s/placements/", root, volumeID)
}

func isActiveTransitionState(state clustermeta.PlacementTransitionState) bool {
	switch state {
	case clustermeta.PlacementTransitionQueued,
		clustermeta.PlacementTransitionRunning,
		clustermeta.PlacementTransitionPaused:
		return true
	default:
		return false
	}
}

func isVisibleTransitionState(state clustermeta.PlacementTransitionState) bool {
	if isActiveTransitionState(state) {
		return true
	}
	return state == clustermeta.PlacementTransitionFailed
}

func boolToMetric(v bool) int {
	if v {
		return 1
	}
	return 0
}

func sortedPlacementApplyClasses(classes map[string]uint64) []string {
	out := make([]string, 0, len(classes))
	for class := range classes {
		out = append(out, class)
	}
	sort.Strings(out)
	return out
}

func (s *server) observabilitySnapshot(ctx context.Context) (observabilitySnapshot, string) {
	snapshot := observabilitySnapshot{
		LocalIsLeader: true,
		LeaderState:   "leader",
	}
	if err := ctx.Err(); err != nil {
		return snapshot, s.nodeID
	}
	maintSvc := s.newMaintenanceService()
	nodes, _ := s.repo.ListNodeMemberships(ctx)
	if err := ctx.Err(); err != nil {
		log.Printf("sbs-service observability snapshot truncated after node scan: %v", err)
		return snapshot, s.nodeID
	}
	volumes, _ := s.repo.ListVolumeStates(ctx)
	if err := ctx.Err(); err != nil {
		log.Printf("sbs-service observability snapshot truncated after volume scan: %v", err)
		return snapshot, s.nodeID
	}
	ops := s.ops.list("", adminv1.OperationState_OPERATION_STATE_UNSPECIFIED)
	snapshot.KnownNodes = len(nodes)
	for _, node := range nodes {
		switch node.LifecycleState {
		case clustermeta.NodeLifecycleActive:
			snapshot.ActiveNodes++
		case clustermeta.NodeLifecycleDraining:
			snapshot.DrainingNodes++
		case clustermeta.NodeLifecycleRemoved:
			snapshot.RemovedNodes++
		}
		switch node.HealthState {
		case clustermeta.NodeHealthHealthy:
			snapshot.HealthyNodes++
		case clustermeta.NodeHealthSuspect:
			snapshot.SuspectNodes++
		case clustermeta.NodeHealthDown:
			snapshot.DownNodes++
		}
	}
	snapshot.Volumes = len(volumes)
	for _, volume := range volumes {
		if err := ctx.Err(); err != nil {
			log.Printf("sbs-service observability snapshot truncated during volume backlog scan: %v", err)
			return snapshot, s.nodeID
		}
		switch volume.Status {
		case clustermeta.VolumeStatusHealthy:
			snapshot.VolumeHealthy++
		case clustermeta.VolumeStatusDegraded, clustermeta.VolumeStatusRepairing, clustermeta.VolumeStatusRebalancing:
			snapshot.VolumeDegraded++
		case clustermeta.VolumeStatusBlocked:
			snapshot.VolumeBlocked++
		}
		operations, err := s.repo.ListMutationOperations(ctx, volume.VolumeID)
		if err != nil {
			operations = nil
		}
		backlog := s.summarizeVolumeTransitionsWithOperations(ctx, maintSvc, volume.VolumeID, operations)
		spec, _ := s.getVolumeSpec(ctx, volume.VolumeID)
		retiredBacklog := summarizeRetiredPayloadBacklogFromOperations(operations, spec.ChunkSizeBytes, time.Now().Unix())
		cooldownActive, cooldownRemaining := s.maintenanceCooldownState(volume.VolumeID, s.currentTime())
		snapshot.RepairBacklog += int(backlog.RepairCount)
		snapshot.RepairBacklogBytes += backlog.RepairBytes
		snapshot.RepairBacklogChunks += backlog.RepairChunks
		snapshot.RebalanceBacklog += int(backlog.RebalanceCount)
		snapshot.RebalanceBacklogBytes += backlog.RebalanceBytes
		snapshot.RebalanceBacklogChunks += backlog.RebalanceChunks
		snapshot.DrainBacklog += int(backlog.DrainCount)
		snapshot.DrainBacklogBytes += backlog.DrainBytes
		snapshot.DrainBacklogChunks += backlog.DrainChunks
		snapshot.RetiredPayloadBacklogBytes += retiredBacklog.Bytes
		snapshot.RetiredPayloadBacklogChunks += retiredBacklog.Chunks
		snapshot.RetiredPayloadFailedBatches += retiredBacklog.FailedBatches
		if retiredBacklog.OldestFailedAgeSec > snapshot.RetiredPayloadFailedAgeSec {
			snapshot.RetiredPayloadFailedAgeSec = retiredBacklog.OldestFailedAgeSec
		}
		snapshot.TransitionFailedBatches += backlog.FailedBatches
		snapshot.TransitionRecentBatches += backlog.RecentBatches
		snapshot.TransitionSmallBatches += backlog.SmallBatches
		snapshot.TransitionRequeued += backlog.RequeuedCount
		snapshot.TransitionRetryPages += backlog.RetryPages
		snapshot.TransitionRetryWindows += backlog.RetryWindows
		snapshot.TransitionRetryWindowBytes += backlog.RetryWindowBytes
		snapshot.TransitionRetryWindowChunks += backlog.RetryWindowChunks
		if cooldownActive {
			snapshot.MaintenanceCooldownVolumes++
			if uint64(cooldownRemaining) > snapshot.MaintenanceCooldownMaxSec {
				snapshot.MaintenanceCooldownMaxSec = uint64(cooldownRemaining)
			}
		}
		if backlog.OldestFailedAgeSec > snapshot.TransitionFailedAgeSec {
			snapshot.TransitionFailedAgeSec = backlog.OldestFailedAgeSec
		}
	}
	for _, node := range nodes {
		if err := ctx.Err(); err != nil {
			log.Printf("sbs-service observability snapshot truncated during node health detail scan: %v", err)
			return snapshot, s.nodeID
		}
		detail, err := s.repo.GetNodeHealthDetail(ctx, node.NodeID)
		if err != nil {
			continue
		}
		if detail.ConsecutiveProbeFailures > 0 {
			snapshot.NodesWithProbeFailures++
		}
		if uint64(detail.ConsecutiveProbeFailures) > snapshot.MaxProbeFailures {
			snapshot.MaxProbeFailures = uint64(detail.ConsecutiveProbeFailures)
		}
		if detail.RecoveryEligibleAtUnix > 0 && detail.RecoveryEligibleAtUnix > s.currentTime().Unix() {
			snapshot.NodesInRecoveryCooldown++
			remaining := uint64(detail.RecoveryEligibleAtUnix - s.currentTime().Unix())
			if remaining > snapshot.MaxRecoveryCooldownSec {
				snapshot.MaxRecoveryCooldownSec = remaining
			}
		}
	}
	snapshot.OperationsTotal = len(ops)
	for _, op := range ops {
		switch op.GetState() {
		case adminv1.OperationState_OPERATION_STATE_RUNNING:
			snapshot.OperationsRunning++
		case adminv1.OperationState_OPERATION_STATE_FAILED:
			snapshot.OperationsFailed++
		case adminv1.OperationState_OPERATION_STATE_COMPLETED:
			snapshot.OperationsCompleted++
		case adminv1.OperationState_OPERATION_STATE_CANCELED:
			snapshot.OperationsCanceled++
		}
	}
	leaderNodeID := s.nodeID
	if s.leader != nil {
		snapshot.LocalIsLeader = s.leader.IsLeader()
		snapshot.LeaderState = s.leader.State()
		if record, err := s.leader.CurrentLeader(ctx); err == nil && record.NodeID != "" {
			leaderNodeID = record.NodeID
			snapshot.LeaseExpiresAtUnix = record.ExpiresAtUnix
		}
	}
	return snapshot, leaderNodeID
}

func (s *server) transitionBacklogData(ctx context.Context, maintSvc *clustermaintenance.Service, volumeID, placementRef string, mappingByPlacementRef map[string]clustermeta.ExtentMappingRecord) (uint64, uint64) {
	mapping, ok := mappingByPlacementRef[placementRef]
	if !ok {
		return 0, 0
	}
	evaluated, err := maintSvc.EvaluateExtentHealth(ctx, volumeID, mapping.ExtentID)
	if err != nil {
		return 0, 0
	}
	if evaluated.IncompleteTransition {
		return evaluated.IncompleteBytes, evaluated.IncompleteChunks
	}
	return evaluated.DataBytes, evaluated.DataChunks
}

func (s *server) summarizeVolumeTransitions(ctx context.Context, volumeID string) volumeTransitionBacklog {
	return s.summarizeVolumeTransitionsWithService(ctx, s.newMaintenanceService(), volumeID)
}

func (s *server) summarizeRetiredPayloadBacklog(ctx context.Context, volumeID string, chunkSizeBytes uint32) retiredPayloadBacklog {
	operations, err := s.repo.ListMutationOperations(ctx, volumeID)
	if err != nil {
		return retiredPayloadBacklog{}
	}
	return summarizeRetiredPayloadBacklogFromOperations(operations, chunkSizeBytes, time.Now().Unix())
}

func summarizeRetiredPayloadBacklogFromOperations(operations []clustermeta.MutationOperationRecord, chunkSizeBytes uint32, nowUnix int64) retiredPayloadBacklog {
	pending := make(map[uint64]struct{})
	collected := make(map[uint64]struct{})
	var failedBatches uint64
	var oldestFailedAgeSec uint64
	for _, operation := range operations {
		switch operation.State {
		case clustermeta.MutationOperationCommitted:
			switch operation.Kind {
			case "write", "transition":
				for _, chunkID := range operation.RetiredPhysicalChunkIDs {
					if chunkID != 0 {
						pending[chunkID] = struct{}{}
					}
				}
			case "payload_gc", "payload_gc_batch":
				for _, chunkID := range operation.RetiredPhysicalChunkIDs {
					if chunkID != 0 {
						collected[chunkID] = struct{}{}
					}
				}
			}
		case clustermeta.MutationOperationFailed:
			if operation.Kind != "payload_gc_batch" {
				continue
			}
			failedBatches++
			failedAtUnix := operation.LastUpdatedAtUnix
			if failedAtUnix == 0 {
				failedAtUnix = operation.StartedAtUnix
			}
			if failedAtUnix > 0 && failedAtUnix <= nowUnix {
				ageSec := uint64(nowUnix - failedAtUnix)
				if ageSec > oldestFailedAgeSec {
					oldestFailedAgeSec = ageSec
				}
			}
		}
	}
	for chunkID := range collected {
		delete(pending, chunkID)
	}
	chunks := uint64(len(pending))
	bytes := uint64(0)
	if chunkSizeBytes > 0 {
		bytes = chunks * uint64(chunkSizeBytes)
	}
	return retiredPayloadBacklog{
		Bytes:              bytes,
		Chunks:             chunks,
		FailedBatches:      failedBatches,
		OldestFailedAgeSec: oldestFailedAgeSec,
	}
}

func (s *server) summarizeVolumeTransitionsWithService(ctx context.Context, maintSvc *clustermaintenance.Service, volumeID string) volumeTransitionBacklog {
	if err := ctx.Err(); err != nil {
		return volumeTransitionBacklog{}
	}
	operations, err := s.repo.ListMutationOperations(ctx, volumeID)
	if err != nil {
		operations = nil
	}
	return s.summarizeVolumeTransitionsWithOperations(ctx, maintSvc, volumeID, operations)
}

func (s *server) summarizeVolumeTransitionsWithOperations(ctx context.Context, maintSvc *clustermaintenance.Service, volumeID string, operations []clustermeta.MutationOperationRecord) volumeTransitionBacklog {
	if err := ctx.Err(); err != nil {
		return volumeTransitionBacklog{}
	}
	transitions, err := s.repo.ListPlacementTransitions(ctx, volumeID)
	if err != nil {
		return volumeTransitionBacklog{}
	}
	if err := ctx.Err(); err != nil {
		return volumeTransitionBacklog{}
	}
	mappings, err := s.repo.ListExtentMappings(ctx, volumeID)
	if err != nil {
		mappings = nil
	}
	mappingByPlacementRef := make(map[string]clustermeta.ExtentMappingRecord, len(mappings))
	for _, mapping := range mappings {
		mappingByPlacementRef[mapping.PlacementRef] = mapping
	}
	var backlog volumeTransitionBacklog
	backlog.FailedBatches, backlog.RecentBatches, backlog.SmallBatches, backlog.RequeuedCount, backlog.RetryPages, backlog.RetryWindows, backlog.RetryWindowBytes, backlog.RetryWindowChunks, backlog.OldestFailedAgeSec = summarizeTransitionBatchBacklog(operations)
	for _, tr := range transitions {
		if err := ctx.Err(); err != nil {
			return backlog
		}
		if !isActiveTransitionState(tr.State) {
			continue
		}
		dataBytes, dataChunks := s.transitionBacklogData(ctx, maintSvc, volumeID, tr.PlacementRef, mappingByPlacementRef)
		switch tr.Reason {
		case "rebalance":
			backlog.RebalanceCount++
			backlog.RebalanceBytes += dataBytes
			backlog.RebalanceChunks += dataChunks
		case "drain":
			backlog.DrainCount++
			backlog.DrainBytes += dataBytes
			backlog.DrainChunks += dataChunks
		default:
			backlog.RepairCount++
			backlog.RepairBytes += dataBytes
			backlog.RepairChunks += dataChunks
		}
	}
	return backlog
}

func summarizeTransitionBatchBacklog(operations []clustermeta.MutationOperationRecord) (uint64, uint64, uint64, uint64, uint64, uint64, uint64, uint64, uint64) {
	nowUnix := time.Now().Unix()
	var failedBatches uint64
	var recentBatches uint64
	var smallBatches uint64
	var requeuedCount uint64
	var retryPages uint64
	var retryWindows uint64
	var retryWindowBytes uint64
	var retryWindowChunks uint64
	var oldestFailedAgeSec uint64
	recentPagesByExtent := make(map[uint64]map[uint64]struct{})
	for _, operation := range operations {
		if operation.Kind != "write" || operation.State == clustermeta.MutationOperationRolledBack {
			continue
		}
		for _, extentID := range operation.AffectedExtentIDs {
			pageSet := recentPagesByExtent[extentID]
			if pageSet == nil {
				pageSet = make(map[uint64]struct{})
				recentPagesByExtent[extentID] = pageSet
			}
			for _, pageNo := range operation.AffectedPageNos {
				pageSet[pageNo] = struct{}{}
			}
		}
	}
	for _, operation := range operations {
		if operation.Kind != "transition" {
			continue
		}
		remainingPages := subtractMutationCompletedPages(operation.AffectedPageNos, operation.CompletedPageNos)
		if operation.State == clustermeta.MutationOperationPending && len(remainingPages) > 0 {
			requeuedCount++
			retryPages += uint64(len(remainingPages))
			retryWindows += uint64(len(operation.RetryPageWindows))
			for _, window := range operation.RetryPageWindows {
				retryWindowBytes += window.DataBytes
				retryWindowChunks += window.DataChunks
			}
		}
	}
	for _, operation := range operations {
		if operation.Kind != "transition_batch" {
			continue
		}
		remainingPages := subtractMutationCompletedPages(operation.AffectedPageNos, operation.CompletedPageNos)
		if operation.State == clustermeta.MutationOperationRunning || operation.State == clustermeta.MutationOperationPending || operation.State == clustermeta.MutationOperationFailed {
			if len(remainingPages) <= 1 {
				smallBatches++
			}
			if mutationPagesTouchRecentSet(operation.AffectedExtentIDs, remainingPages, recentPagesByExtent) {
				recentBatches++
			}
		}
		if operation.State != clustermeta.MutationOperationFailed {
			continue
		}
		failedBatches++
		failedAtUnix := operation.LastUpdatedAtUnix
		if failedAtUnix == 0 {
			failedAtUnix = operation.StartedAtUnix
		}
		if failedAtUnix > 0 && failedAtUnix <= nowUnix {
			ageSec := uint64(nowUnix - failedAtUnix)
			if ageSec > oldestFailedAgeSec {
				oldestFailedAgeSec = ageSec
			}
		}
	}
	return failedBatches, recentBatches, smallBatches, requeuedCount, retryPages, retryWindows, retryWindowBytes, retryWindowChunks, oldestFailedAgeSec
}

func (s *server) listTransitionsByReason(ctx context.Context, reason string) ([]*adminv1.RepairSummary, error) {
	volumes, err := s.repo.ListVolumeStates(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]*adminv1.RepairSummary, 0)
	for _, volume := range volumes {
		transitions, err := s.repo.ListPlacementTransitions(ctx, volume.VolumeID)
		if err != nil {
			return nil, err
		}
		for _, tr := range transitions {
			if tr.Reason != reason || !isVisibleTransitionState(tr.State) {
				continue
			}
			out = append(out, &adminv1.RepairSummary{
				VolumeId:            tr.VolumeID,
				PlacementRef:        tr.PlacementRef,
				CurrentReplicaSetId: tr.CurrentReplicaSetID,
				TargetReplicaSetId:  tr.TargetReplicaSetID,
				State:               string(tr.State),
			})
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].GetVolumeId() == out[j].GetVolumeId() {
			return out[i].GetPlacementRef() < out[j].GetPlacementRef()
		}
		return out[i].GetVolumeId() < out[j].GetVolumeId()
	})
	return out, nil
}

func toRebalanceSummaries(repairs []*adminv1.RepairSummary) []*adminv1.RebalanceSummary {
	out := make([]*adminv1.RebalanceSummary, 0, len(repairs))
	for _, repair := range repairs {
		out = append(out, &adminv1.RebalanceSummary{
			VolumeId:            repair.GetVolumeId(),
			PlacementRef:        repair.GetPlacementRef(),
			CurrentReplicaSetId: repair.GetCurrentReplicaSetId(),
			TargetReplicaSetId:  repair.GetTargetReplicaSetId(),
			State:               repair.GetState(),
		})
	}
	return out
}

func (s *server) handleDebugEnqueueTransition(w http.ResponseWriter, r *http.Request, reason string) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if err := s.requireLeader(r.Context()); err != nil {
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}
	volumeID := r.URL.Query().Get("volume_id")
	extentID, err := strconv.ParseUint(r.URL.Query().Get("extent_id"), 10, 64)
	if volumeID == "" || err != nil || extentID == 0 {
		http.Error(w, "volume_id and extent_id are required", http.StatusBadRequest)
		return
	}
	target, transition, err := s.debugEnqueueTransition(r.Context(), reason, volumeID, extentID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	_ = json.NewEncoder(w).Encode(map[string]any{
		"ok":                     true,
		"reason":                 reason,
		"volume_id":              volumeID,
		"extent_id":              extentID,
		"placement_ref":          transition.PlacementRef,
		"current_replica_set_id": transition.CurrentReplicaSetID,
		"target_replica_set_id":  target.ReplicaSetID,
		"target_placement_ref":   target.PlacementRef,
		"state":                  transition.State,
	})
}

func (s *server) handleDebugClearTransitions(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if err := s.requireLeader(r.Context()); err != nil {
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}
	volumeID := r.URL.Query().Get("volume_id")
	if volumeID == "" {
		http.Error(w, "volume_id is required", http.StatusBadRequest)
		return
	}
	transitions, err := s.repo.ListPlacementTransitions(r.Context(), volumeID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	deleted := 0
	for _, tr := range transitions {
		if err := s.repo.DeletePlacementTransition(r.Context(), volumeID, tr.PlacementRef); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		deleted++
	}
	_ = json.NewEncoder(w).Encode(map[string]any{
		"ok":                  true,
		"volume_id":           volumeID,
		"deleted_count":       deleted,
		"cleared_transitions": deleted,
	})
}

func (s *server) handleDebugSetNodeHealth(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if err := s.requireLeader(r.Context()); err != nil {
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}
	nodeID := r.URL.Query().Get("node_id")
	nextHealth := clustermeta.NodeHealthState(strings.ToLower(strings.TrimSpace(r.URL.Query().Get("health"))))
	if nodeID == "" {
		http.Error(w, "node_id is required", http.StatusBadRequest)
		return
	}
	switch nextHealth {
	case clustermeta.NodeHealthHealthy, clustermeta.NodeHealthSuspect, clustermeta.NodeHealthDown:
	default:
		http.Error(w, "health must be healthy|suspect|down", http.StatusBadRequest)
		return
	}
	controller := clustercontrol.NewFromRepository(s.repo)
	rec, failovers, enqueued, err := controller.SetNodeHealth(r.Context(), nodeID, nextHealth)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	_ = json.NewEncoder(w).Encode(map[string]any{
		"ok":              true,
		"node_id":         rec.NodeID,
		"health":          rec.HealthState,
		"failovers":       failovers,
		"repair_enqueued": enqueued,
		"lifecycle_state": rec.LifecycleState,
		"last_heartbeat":  rec.LastHeartbeatUnix,
	})
}

func (s *server) handleDebugTransitions(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if err := s.requireLeader(r.Context()); err != nil {
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}
	volumeID := r.URL.Query().Get("volume_id")
	if volumeID == "" {
		http.Error(w, "volume_id is required", http.StatusBadRequest)
		return
	}
	transitions, err := s.repo.ListPlacementTransitions(r.Context(), volumeID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	type transitionRow struct {
		VolumeID            string `json:"volume_id"`
		PlacementRef        string `json:"placement_ref"`
		State               string `json:"state"`
		Reason              string `json:"reason,omitempty"`
		CurrentReplicaSetID string `json:"current_replica_set_id,omitempty"`
		TargetReplicaSetID  string `json:"target_replica_set_id,omitempty"`
		StartedAtUnix       int64  `json:"started_at_unix,omitempty"`
		LastProgressAtUnix  int64  `json:"last_progress_at_unix,omitempty"`
		Attempt             uint32 `json:"attempt,omitempty"`
		Active              bool   `json:"active"`
		Visible             bool   `json:"visible"`
	}
	rows := make([]transitionRow, 0, len(transitions))
	for _, tr := range transitions {
		rows = append(rows, transitionRow{
			VolumeID:            tr.VolumeID,
			PlacementRef:        tr.PlacementRef,
			State:               string(tr.State),
			Reason:              tr.Reason,
			CurrentReplicaSetID: tr.CurrentReplicaSetID,
			TargetReplicaSetID:  tr.TargetReplicaSetID,
			StartedAtUnix:       tr.StartedAtUnix,
			LastProgressAtUnix:  tr.LastProgressAtUnix,
			Attempt:             tr.Attempt,
			Active:              isActiveTransitionState(tr.State),
			Visible:             isVisibleTransitionState(tr.State),
		})
	}
	_ = json.NewEncoder(w).Encode(map[string]any{
		"volume_id":   volumeID,
		"transitions": rows,
	})
}

func (s *server) handleDebugVolume(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if err := s.requireLeader(r.Context()); err != nil {
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}
	volumeID := r.URL.Query().Get("volume_id")
	if volumeID == "" {
		http.Error(w, "volume_id is required", http.StatusBadRequest)
		return
	}
	controller := clustercontrol.NewFromRepository(s.repo)
	snapshot, err := controller.GetVolume(r.Context(), volumeID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	mutationOps, err := s.repo.ListMutationOperations(r.Context(), volumeID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	encodedOps := make([]*adminv1.OperationStatus, 0, len(mutationOps))
	for _, rec := range mutationOps {
		encodedOps = append(encodedOps, mutationOperationToAdminStatus(rec, mutationOps))
	}
	_ = json.NewEncoder(w).Encode(map[string]any{
		"volume":                     snapshot,
		"mutation_operations":        encodedOps,
		"mutation_operation_records": mutationOps,
	})
}

func (s *server) handleDebugPayloadGC(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if !s.ready.Load() {
		http.Error(w, "not ready", http.StatusServiceUnavailable)
		return
	}
	payloadRoot := strings.TrimSpace(r.URL.Query().Get("payload_root"))
	if payloadRoot == "" {
		http.Error(w, "payload_root is required", http.StatusBadRequest)
		return
	}
	volumeID := strings.TrimSpace(r.URL.Query().Get("volume_id"))
	results, err := s.debugPayloadGCSweep(r.Context(), payloadRoot, volumeID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(results)
}

func (s *server) handleDebugECInspect(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if err := s.requireLeader(r.Context()); err != nil {
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}
	params, err := parseDebugECParams(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	out, err := s.debugECInspect(r.Context(), params)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(out)
}

func (s *server) handleDebugECScrub(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if err := s.requireLeader(r.Context()); err != nil {
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}
	params, err := parseDebugECParams(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if err := s.resolveDebugECStripeParams(r.Context(), &params, true); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	volume, ecSvc, closeSessions, err := s.newECMaintenanceService(r.Context(), params.VolumeID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	defer closeSessions()
	reqCtx := s.debugECRequestContext("ec-scrub", params)
	resp, err := ecSvc.ScrubStripe(r.Context(), clusterec.ScrubRequest{
		Volume:           volume,
		Context:          reqCtx,
		ObjectID:         params.ObjectID,
		StripeID:         params.StripeID,
		StripeGeneration: params.StripeGeneration,
	})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(debugECScrubResponseJSON(params.VolumeID, resp))
}

func (s *server) handleDebugECRepair(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if err := s.requireLeader(r.Context()); err != nil {
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}
	params, err := parseDebugECParams(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if !params.ShardIDSet {
		http.Error(w, "shard_id is required", http.StatusBadRequest)
		return
	}
	if err := s.resolveDebugECStripeParams(r.Context(), &params, true); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	volume, ecSvc, closeSessions, err := s.newECMaintenanceService(r.Context(), params.VolumeID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	defer closeSessions()
	reqCtx := s.debugECRequestContext("ec-repair", params)
	resp, err := ecSvc.RepairShard(r.Context(), clusterec.RepairRequest{
		Volume:           volume,
		Context:          reqCtx,
		ObjectID:         params.ObjectID,
		StripeID:         params.StripeID,
		StripeGeneration: params.StripeGeneration,
		ShardID:          params.ShardID,
	})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(debugECRepairResponseJSON(params.VolumeID, resp))
}

func (s *server) handleDebugECMaintenanceScan(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if err := s.requireLeader(r.Context()); err != nil {
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}
	volumeID := strings.TrimSpace(r.URL.Query().Get("volume_id"))
	if volumeID == "" {
		http.Error(w, "volume_id is required", http.StatusBadRequest)
		return
	}
	state, err := s.repo.GetVolumeState(r.Context(), volumeID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	spec, err := s.getVolumeSpec(r.Context(), volumeID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	settings, err := s.loadMaintenanceSettingsSnapshot(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	result, err := s.runECMaintenanceScanOnce(r.Context(), state, spec, settings)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(debugECMaintenanceScanResponseJSON(result))
}

func (s *server) handleDebugECFaultDeleteShard(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if err := s.requireLeader(r.Context()); err != nil {
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}
	params, err := parseDebugECParams(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if !params.ShardIDSet {
		http.Error(w, "shard_id is required", http.StatusBadRequest)
		return
	}
	if err := s.resolveDebugECStripeParams(r.Context(), &params, true); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	stripe, err := s.debugECStripe(r.Context(), params.VolumeID, params.StripeID, params.StripeGeneration)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	shard, ok := debugECShardByID(stripe, params.ShardID)
	if !ok {
		http.Error(w, fmt.Sprintf("shard_id=%d not found", params.ShardID), http.StatusBadRequest)
		return
	}
	_, _, sessions, closeSessions, err := s.newECMaintenanceServiceWithSessions(r.Context(), params.VolumeID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	defer closeSessions()
	session, ok := sessions[shard.NodeID]
	if !ok {
		http.Error(w, fmt.Sprintf("no maintenance session for node_id=%s", shard.NodeID), http.StatusBadRequest)
		return
	}
	client, ok := session.Client.(service.ECShardSBSClient)
	if !ok {
		http.Error(w, service.ErrNotSupported.Error(), http.StatusNotImplemented)
		return
	}
	reqCtx := service.SBSRequestContext{
		RequestID:      fmt.Sprintf("ec-fault-delete-shard-%s-%s-%d", params.VolumeID, params.StripeID, params.ShardID),
		GatewayID:      "sbs-service",
		HostID:         s.nodeID,
		SessionID:      session.SessionID,
		AttachmentID:   session.AttachmentID,
		Generation:     session.Generation,
		IdempotencyKey: fmt.Sprintf("ec-fault-delete-shard-%s-%s-%d-%d", params.VolumeID, params.StripeID, params.StripeGeneration, params.ShardID),
		TraceID:        fmt.Sprintf("ec-fault-delete-shard-%s-%s-%d", params.VolumeID, params.StripeID, params.ShardID),
	}
	resp, err := client.DeleteECShard(r.Context(), &service.DeleteECShardRequest{
		VolumeID:         params.VolumeID,
		VolumeHandle:     session.VolumeHandle,
		ObjectID:         params.ObjectID,
		StripeID:         params.StripeID,
		StripeGeneration: params.StripeGeneration,
		ShardID:          params.ShardID,
		StoreID:          shard.StoreID,
		Context:          reqCtx,
	})
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	_ = json.NewEncoder(w).Encode(map[string]any{
		"ok":                true,
		"fault_injected":    true,
		"volume_id":         params.VolumeID,
		"object_id":         params.ObjectID,
		"stripe_id":         params.StripeID,
		"stripe_generation": params.StripeGeneration,
		"shard_id":          params.ShardID,
		"node_id":           shard.NodeID,
		"zone":              shard.Zone,
		"store_id":          shard.StoreID,
		"delete_status":     resp.Status,
	})
}

func (s *server) handleDebugECRebalance(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if err := s.requireLeader(r.Context()); err != nil {
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}
	params, err := parseDebugECParams(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if !params.ShardIDSet {
		http.Error(w, "shard_id is required", http.StatusBadRequest)
		return
	}
	if params.TargetNodeID == "" {
		http.Error(w, "target_node_id is required", http.StatusBadRequest)
		return
	}
	if err := s.resolveDebugECStripeParams(r.Context(), &params, true); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	volume, ecSvc, closeSessions, err := s.newECMaintenanceService(r.Context(), params.VolumeID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	defer closeSessions()
	reqCtx := s.debugECRequestContext("ec-rebalance", params)
	resp, err := ecSvc.RebalanceShard(r.Context(), clusterec.RebalanceShardRequest{
		Volume:           volume,
		Context:          reqCtx,
		ObjectID:         params.ObjectID,
		StripeID:         params.StripeID,
		StripeGeneration: params.StripeGeneration,
		ShardID:          params.ShardID,
		TargetNodeID:     params.TargetNodeID,
	})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(debugECRebalanceResponseJSON(params.VolumeID, resp))
}

func (s *server) handleDebugECDrainPreflight(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if err := s.requireLeader(r.Context()); err != nil {
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}
	params, err := parseDebugECParams(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if params.NodeID == "" && params.Zone == "" {
		http.Error(w, "node_id or zone is required", http.StatusBadRequest)
		return
	}
	if err := s.resolveDebugECStripeParams(r.Context(), &params, false); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	volume, ecSvc, closeSessions, err := s.newECMaintenanceService(r.Context(), params.VolumeID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	defer closeSessions()
	resp, err := ecSvc.PreflightDrain(r.Context(), clusterec.DrainPreflightRequest{
		Volume:           volume,
		StripeID:         params.StripeID,
		StripeGeneration: params.StripeGeneration,
		NodeID:           params.NodeID,
		Zone:             params.Zone,
		AllowWeak:        params.AllowWeak,
	})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(debugECDrainPreflightResponseJSON(params.VolumeID, resp))
}

func (s *server) handleDebugECDrain(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if err := s.requireLeader(r.Context()); err != nil {
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}
	params, err := parseDebugECParams(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if params.NodeID == "" && params.Zone == "" {
		http.Error(w, "node_id or zone is required", http.StatusBadRequest)
		return
	}
	if err := s.resolveDebugECStripeParams(r.Context(), &params, false); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	volume, ecSvc, closeSessions, err := s.newECMaintenanceService(r.Context(), params.VolumeID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	defer closeSessions()
	reqCtx := s.debugECRequestContext("ec-drain", params)
	resp, err := ecSvc.Drain(r.Context(), clusterec.DrainRequest{
		Volume:           volume,
		Context:          reqCtx,
		StripeID:         params.StripeID,
		StripeGeneration: params.StripeGeneration,
		NodeID:           params.NodeID,
		Zone:             params.Zone,
		AllowWeak:        params.AllowWeak,
	})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(debugECDrainResponseJSON(params.VolumeID, resp))
}

func (s *server) handleDebugECDrainVolume(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if err := s.requireLeader(r.Context()); err != nil {
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}
	params, err := parseDebugECDrainVolumeParams(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	resp, err := s.debugECDrainVolume(r.Context(), params)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}

type debugECParams struct {
	VolumeID            string
	ObjectID            string
	StripeID            string
	StripeGeneration    uint64
	ShardID             uint32
	ShardIDSet          bool
	TargetNodeID        string
	NodeID              string
	Zone                string
	AllowWeak           bool
	IncludeReachability bool
	IdempotencyKey      string
}

type debugECDrainVolumeParams struct {
	VolumeID       string
	NodeID         string
	Zone           string
	AllowWeak      bool
	IdempotencyKey string
	MaxStripes     int
}

func parseDebugECParams(r *http.Request) (debugECParams, error) {
	q := r.URL.Query()
	out := debugECParams{
		VolumeID:            strings.TrimSpace(q.Get("volume_id")),
		ObjectID:            strings.TrimSpace(q.Get("object_id")),
		StripeID:            strings.TrimSpace(q.Get("stripe_id")),
		TargetNodeID:        strings.TrimSpace(q.Get("target_node_id")),
		NodeID:              strings.TrimSpace(q.Get("node_id")),
		Zone:                strings.TrimSpace(q.Get("zone")),
		AllowWeak:           q.Get("allow_weak") == "true" || q.Get("allow_weak") == "1",
		IncludeReachability: q.Get("include_reachability") == "true" || q.Get("include_reachability") == "1",
		IdempotencyKey:      strings.TrimSpace(q.Get("idempotency_key")),
	}
	if out.VolumeID == "" {
		return out, fmt.Errorf("volume_id is required")
	}
	if out.StripeID == "" {
		return out, fmt.Errorf("stripe_id is required")
	}
	if raw := strings.TrimSpace(q.Get("stripe_generation")); raw != "" {
		parsed, err := strconv.ParseUint(raw, 10, 64)
		if err != nil || parsed == 0 {
			return out, fmt.Errorf("stripe_generation must be a positive integer")
		}
		out.StripeGeneration = parsed
	}
	if raw := strings.TrimSpace(q.Get("shard_id")); raw != "" {
		parsed, err := strconv.ParseUint(raw, 10, 32)
		if err != nil {
			return out, fmt.Errorf("shard_id must be a uint32")
		}
		out.ShardID = uint32(parsed)
		out.ShardIDSet = true
	}
	return out, nil
}

func parseDebugECDrainVolumeParams(r *http.Request) (debugECDrainVolumeParams, error) {
	q := r.URL.Query()
	out := debugECDrainVolumeParams{
		VolumeID:       strings.TrimSpace(q.Get("volume_id")),
		NodeID:         strings.TrimSpace(q.Get("node_id")),
		Zone:           strings.TrimSpace(q.Get("zone")),
		AllowWeak:      q.Get("allow_weak") == "true" || q.Get("allow_weak") == "1",
		IdempotencyKey: strings.TrimSpace(q.Get("idempotency_key")),
		MaxStripes:     16,
	}
	if out.VolumeID == "" {
		return out, fmt.Errorf("volume_id is required")
	}
	if out.NodeID == "" && out.Zone == "" {
		return out, fmt.Errorf("node_id or zone is required")
	}
	if raw := strings.TrimSpace(q.Get("max_stripes")); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil || parsed <= 0 {
			return out, fmt.Errorf("max_stripes must be a positive integer")
		}
		if parsed > 256 {
			parsed = 256
		}
		out.MaxStripes = parsed
	}
	return out, nil
}

func (s *server) debugECInspect(ctx context.Context, params debugECParams) (map[string]any, error) {
	state, err := s.repo.GetVolumeState(ctx, params.VolumeID)
	if err != nil {
		return nil, err
	}
	specRec, err := s.getVolumeSpec(ctx, params.VolumeID)
	if err != nil {
		return nil, err
	}
	if effectiveVolumeRedundancyBackend(state, specRec) != clustermeta.RedundancyBackendEC {
		return nil, fmt.Errorf("volume %q is not ec-backed", params.VolumeID)
	}
	stripe, err := s.debugECStripe(ctx, params.VolumeID, params.StripeID, params.StripeGeneration)
	if err != nil {
		return nil, err
	}
	objectID := params.ObjectID
	if objectID == "" {
		objectID = stripe.ObjectID
	}
	object, err := s.repo.GetPhysicalObject(ctx, params.VolumeID, objectID)
	if err != nil {
		return nil, err
	}
	topology := debugECStripeTopology(stripe, specRec)
	out := map[string]any{
		"ok":                            true,
		"volume_id":                     params.VolumeID,
		"backend_type":                  clustermeta.RedundancyBackendEC,
		"ec_profile_id":                 specRec.ECProfileID,
		"codec_id":                      specRec.ECCodecID,
		"k":                             specRec.ECDataShards,
		"m":                             specRec.ECParityShards,
		"data_shards":                   specRec.ECDataShards,
		"parity_shards":                 specRec.ECParityShards,
		"stripe_unit_bytes":             specRec.ECStripeUnitBytes,
		"failure_domain_policy":         specRec.ECFailureDomain,
		"object_id":                     object.ObjectID,
		"stripe_id":                     stripe.StripeID,
		"stripe_generation":             stripe.StripeGeneration,
		"topology_revision":             stripe.TopologyRevision,
		"zone_shard_counts":             topology["zone_shard_counts"],
		"data_zone_counts":              topology["data_zone_counts"],
		"coding_zone_counts":            topology["coding_zone_counts"],
		"node_shard_counts":             topology["node_shard_counts"],
		"data_node_counts":              topology["data_node_counts"],
		"coding_node_counts":            topology["coding_node_counts"],
		"store_shard_counts":            topology["store_shard_counts"],
		"max_shards_per_node":           topology["max_shards_per_node"],
		"zone_tolerance_ok":             topology["zone_tolerance_ok"],
		"node_spread_ok":                topology["node_spread_ok"],
		"store_spread_ok":               topology["store_spread_ok"],
		"degraded_shard_count":          topology["degraded_shard_count"],
		"placement_skew":                topology["placement_skew"],
		"rebuild_state":                 "",
		"scrub_state":                   "",
		"operation_id":                  "",
		"blocked_reason":                "",
		"volume":                        state,
		"physical_object":               object,
		"stripe":                        stripe,
		"topology":                      topology,
		"weak_placement_allowed":        specRec.WeakPlacementAllowed,
		"max_shards_per_failure_domain": specRec.ECMaxShardsPerFailureDomain,
	}
	if params.IncludeReachability {
		report, err := clusterec.CollectReachability(ctx, s.repo, params.VolumeID)
		if err != nil {
			return nil, fmt.Errorf("collect ec reachability: %w", err)
		}
		out["reachability"] = report
		out["reachable_object_count"] = report.ReachableObjectCount
		out["retired_protected_count"] = report.RetiredProtectedCount
		out["retired_reclaimable_count"] = report.RetiredReclaimableCount
		out["retired_protected"] = report.RetiredProtected
		out["retired_reclaimable"] = report.RetiredReclaimable
	}
	return out, nil
}

func (s *server) debugECDrainVolume(ctx context.Context, params debugECDrainVolumeParams) (map[string]any, error) {
	state, err := s.repo.GetVolumeState(ctx, params.VolumeID)
	if err != nil {
		return nil, err
	}
	specRec, err := s.getVolumeSpec(ctx, params.VolumeID)
	if err != nil {
		return nil, err
	}
	if effectiveVolumeRedundancyBackend(state, specRec) != clustermeta.RedundancyBackendEC {
		return nil, fmt.Errorf("volume %q is not ec-backed", params.VolumeID)
	}
	stripes, err := s.currentECDrainStripes(ctx, params.VolumeID)
	if err != nil {
		return nil, err
	}
	volume, ecSvc, closeSessions, err := s.newECMaintenanceService(ctx, params.VolumeID)
	if err != nil {
		return nil, err
	}
	defer closeSessions()

	resultCap := len(stripes)
	if resultCap > params.MaxStripes {
		resultCap = params.MaxStripes
	}
	results := make([]map[string]any, 0, resultCap)
	scanned := 0
	drained := 0
	blocked := 0
	errorCount := 0
	movedTotal := 0
	for _, stripe := range stripes {
		if scanned >= params.MaxStripes {
			break
		}
		if !ecStripeMatchesDrainScope(stripe, params.NodeID, params.Zone) {
			continue
		}
		scanned++
		preflight, err := ecSvc.PreflightDrain(ctx, clusterec.DrainPreflightRequest{
			Volume:           volume,
			StripeID:         stripe.StripeID,
			StripeGeneration: stripe.StripeGeneration,
			NodeID:           params.NodeID,
			Zone:             params.Zone,
			AllowWeak:        params.AllowWeak,
		})
		if err != nil {
			errorCount++
			results = append(results, debugECDrainVolumeErrorResult(stripe, err))
			continue
		}
		if len(preflight.AffectedShards) == 0 {
			results = append(results, map[string]any{
				"stripe_id":         stripe.StripeID,
				"stripe_generation": stripe.StripeGeneration,
				"status":            "skipped",
				"reason":            "no_matching_shards",
			})
			continue
		}
		if preflight.Blocked {
			blocked++
			results = append(results, map[string]any{
				"stripe_id":         stripe.StripeID,
				"stripe_generation": stripe.StripeGeneration,
				"status":            "blocked",
				"blocked_reason":    preflight.BlockedReason,
				"affected_shards":   preflight.AffectedShards,
				"weak":              preflight.Weak,
			})
			continue
		}
		stripeParams := debugECParams{
			VolumeID:         params.VolumeID,
			StripeID:         stripe.StripeID,
			StripeGeneration: stripe.StripeGeneration,
			NodeID:           params.NodeID,
			Zone:             params.Zone,
			AllowWeak:        params.AllowWeak,
			IdempotencyKey:   debugECDrainVolumeStripeKey(params, stripe),
		}
		resp, err := ecSvc.Drain(ctx, clusterec.DrainRequest{
			Volume:           volume,
			Context:          s.debugECRequestContext("ec-drain-volume", stripeParams),
			StripeID:         stripe.StripeID,
			StripeGeneration: stripe.StripeGeneration,
			NodeID:           params.NodeID,
			Zone:             params.Zone,
			AllowWeak:        params.AllowWeak,
		})
		if err != nil {
			errorCount++
			results = append(results, debugECDrainVolumeErrorResult(stripe, err))
			continue
		}
		drained++
		movedTotal += len(resp.MovedShards)
		results = append(results, map[string]any{
			"stripe_id":         resp.StripeID,
			"stripe_generation": resp.StripeGeneration,
			"status":            "drained",
			"operation_id":      resp.OperationID,
			"weak":              resp.Weak,
			"moved_shards":      resp.MovedShards,
			"topology_revision": resp.TopologyRevision,
		})
	}
	return map[string]any{
		"ok":              errorCount == 0 && blocked == 0,
		"volume_id":       params.VolumeID,
		"node_id":         params.NodeID,
		"zone":            params.Zone,
		"allow_weak":      params.AllowWeak,
		"max_stripes":     params.MaxStripes,
		"scanned_stripes": scanned,
		"drained_stripes": drained,
		"blocked_stripes": blocked,
		"error_count":     errorCount,
		"moved_shards":    movedTotal,
		"results":         results,
	}, nil
}

func (s *server) currentECDrainStripes(ctx context.Context, volumeID string) ([]clustermeta.ECStripeRecord, error) {
	stripes, err := s.repo.ListECStripes(ctx, volumeID)
	if err != nil {
		return nil, err
	}
	latest := make(map[string]clustermeta.ECStripeRecord)
	for _, stripe := range stripes {
		if stripe.State != clustermeta.ECStripeStateCommitted {
			continue
		}
		prev, ok := latest[stripe.StripeID]
		if !ok || stripe.StripeGeneration > prev.StripeGeneration {
			latest[stripe.StripeID] = stripe
		}
	}
	out := make([]clustermeta.ECStripeRecord, 0, len(latest))
	for _, stripe := range latest {
		out = append(out, stripe)
	}
	sort.Slice(out, func(i, j int) bool {
		left, leftErr := strconv.ParseUint(out[i].StripeID, 10, 64)
		right, rightErr := strconv.ParseUint(out[j].StripeID, 10, 64)
		if leftErr == nil && rightErr == nil && left != right {
			return left < right
		}
		if out[i].StripeID != out[j].StripeID {
			return out[i].StripeID < out[j].StripeID
		}
		return out[i].StripeGeneration < out[j].StripeGeneration
	})
	return out, nil
}

func ecStripeMatchesDrainScope(stripe clustermeta.ECStripeRecord, nodeID, zone string) bool {
	for _, shard := range stripe.Shards {
		if nodeID != "" && shard.NodeID == nodeID {
			return true
		}
		if zone != "" && shard.Zone == zone {
			return true
		}
	}
	return false
}

func debugECDrainVolumeStripeKey(params debugECDrainVolumeParams, stripe clustermeta.ECStripeRecord) string {
	if params.IdempotencyKey != "" {
		return fmt.Sprintf("%s-%s-%d", params.IdempotencyKey, stripe.StripeID, stripe.StripeGeneration)
	}
	scope := params.NodeID
	if scope == "" {
		scope = "zone-" + params.Zone
	}
	return fmt.Sprintf("ec-drain-volume-%s-%s-%d-%s", params.VolumeID, stripe.StripeID, stripe.StripeGeneration, scope)
}

func debugECDrainVolumeErrorResult(stripe clustermeta.ECStripeRecord, err error) map[string]any {
	return map[string]any{
		"stripe_id":         stripe.StripeID,
		"stripe_generation": stripe.StripeGeneration,
		"status":            "error",
		"error":             err.Error(),
	}
}

func (s *server) debugECStripe(ctx context.Context, volumeID, stripeID string, stripeGeneration uint64) (clustermeta.ECStripeRecord, error) {
	if stripeGeneration != 0 {
		return s.repo.GetECStripe(ctx, volumeID, stripeID, stripeGeneration)
	}
	stripes, err := s.repo.ListECStripes(ctx, volumeID)
	if err != nil {
		return clustermeta.ECStripeRecord{}, err
	}
	var found clustermeta.ECStripeRecord
	for _, stripe := range stripes {
		if stripe.StripeID != stripeID {
			continue
		}
		if found.StripeID == "" || stripe.StripeGeneration > found.StripeGeneration {
			found = stripe
		}
	}
	if found.StripeID == "" {
		return clustermeta.ECStripeRecord{}, fmt.Errorf("ec stripe_id=%s not found", stripeID)
	}
	return found, nil
}

func debugECShardByID(stripe clustermeta.ECStripeRecord, shardID uint32) (clustermeta.ECShardRecord, bool) {
	for _, shard := range stripe.Shards {
		if shard.ShardID == shardID {
			return shard, true
		}
	}
	return clustermeta.ECShardRecord{}, false
}

func (s *server) resolveDebugECStripeParams(ctx context.Context, params *debugECParams, needObject bool) error {
	stripe, err := s.debugECStripe(ctx, params.VolumeID, params.StripeID, params.StripeGeneration)
	if err != nil {
		return err
	}
	params.StripeGeneration = stripe.StripeGeneration
	if needObject && params.ObjectID == "" {
		params.ObjectID = stripe.ObjectID
	}
	return nil
}

func debugECStripeTopology(stripe clustermeta.ECStripeRecord, spec volumeSpecRecord) map[string]any {
	zoneCounts := make(map[string]uint32)
	dataZoneCounts := make(map[string]uint32)
	codingZoneCounts := make(map[string]uint32)
	nodeCounts := make(map[string]uint32)
	dataNodeCounts := make(map[string]uint32)
	codingNodeCounts := make(map[string]uint32)
	storeCounts := make(map[string]uint32)
	degraded := uint32(0)
	for _, shard := range stripe.Shards {
		shardDegraded := false
		if shard.Zone != "" {
			zoneCounts[shard.Zone]++
		}
		if shard.NodeID != "" {
			nodeCounts[shard.NodeID]++
		} else {
			shardDegraded = true
		}
		if shard.StoreID != "" {
			storeCounts[shard.StoreID]++
		} else {
			shardDegraded = true
		}
		if shardDegraded {
			degraded++
		}
		switch shard.Role {
		case clustermeta.ECShardRoleData:
			if shard.Zone != "" {
				dataZoneCounts[shard.Zone]++
			}
			if shard.NodeID != "" {
				dataNodeCounts[shard.NodeID]++
			}
		case clustermeta.ECShardRoleCoding:
			if shard.Zone != "" {
				codingZoneCounts[shard.Zone]++
			}
			if shard.NodeID != "" {
				codingNodeCounts[shard.NodeID]++
			}
		}
	}
	maxZone := maxUint32Value(zoneCounts)
	maxNode := maxUint32Value(nodeCounts)
	maxStore := maxUint32Value(storeCounts)
	maxFailureDomain := spec.ECMaxShardsPerFailureDomain
	if maxFailureDomain == 0 {
		maxFailureDomain = spec.ECParityShards
	}
	return map[string]any{
		"zone_shard_counts":    zoneCounts,
		"data_zone_counts":     dataZoneCounts,
		"coding_zone_counts":   codingZoneCounts,
		"node_shard_counts":    nodeCounts,
		"data_node_counts":     dataNodeCounts,
		"coding_node_counts":   codingNodeCounts,
		"store_shard_counts":   storeCounts,
		"zone_tolerance_ok":    maxFailureDomain == 0 || maxZone <= maxFailureDomain,
		"node_spread_ok":       maxNode <= 1,
		"store_spread_ok":      maxStore <= 1,
		"max_shards_per_node":  maxNode,
		"max_shards_per_store": maxStore,
		"degraded_shard_count": degraded,
		"placement_skew":       maxZone - minNonZeroUint32Value(zoneCounts),
	}
}

func maxUint32Value(values map[string]uint32) uint32 {
	var out uint32
	for _, value := range values {
		if value > out {
			out = value
		}
	}
	return out
}

func minNonZeroUint32Value(values map[string]uint32) uint32 {
	var out uint32
	for _, value := range values {
		if value == 0 {
			continue
		}
		if out == 0 || value < out {
			out = value
		}
	}
	return out
}

func (s *server) newECMaintenanceService(ctx context.Context, volumeID string) (service.VolumeSpec, *clusterec.Service, func(), error) {
	volume, svc, _, closeSessions, err := s.newECMaintenanceServiceWithSessions(ctx, volumeID)
	return volume, svc, closeSessions, err
}

func (s *server) newECMaintenanceServiceWithSessions(ctx context.Context, volumeID string) (service.VolumeSpec, *clusterec.Service, map[string]clusterec.ShardSession, func(), error) {
	state, err := s.repo.GetVolumeState(ctx, volumeID)
	if err != nil {
		return service.VolumeSpec{}, nil, nil, nil, err
	}
	specRec, err := s.getVolumeSpec(ctx, volumeID)
	if err != nil {
		return service.VolumeSpec{}, nil, nil, nil, err
	}
	if effectiveVolumeRedundancyBackend(state, specRec) != clustermeta.RedundancyBackendEC {
		return service.VolumeSpec{}, nil, nil, nil, fmt.Errorf("volume %q is not ec-backed", volumeID)
	}
	clients, err := s.ecMaintenanceNodeClients(ctx, specRec)
	if err != nil {
		return service.VolumeSpec{}, nil, nil, nil, err
	}
	generation := state.Revision
	if generation == 0 {
		generation = 1
	}
	attachmentID := fmt.Sprintf("ec-maint-%s-%d", volumeID, time.Now().UnixNano())
	replicas, err := clusterreplication.OpenReplicaSessions(ctx, clients, clusterreplication.OpenReplicaSessionsRequest{
		VolumeID:      volumeID,
		GatewayID:     "sbs-service",
		HostID:        s.nodeID,
		ClientVersion: buildVersion,
		AttachmentID:  attachmentID,
		Generation:    generation,
		SessionPrefix: "ec-maint",
		AccessMode:    service.SBSAccessModeExclusiveWriter,
	})
	if err != nil {
		return service.VolumeSpec{}, nil, nil, nil, err
	}
	sessions := make(map[string]clusterec.ShardSession, len(replicas))
	for nodeID, replica := range replicas {
		session := clusterec.ShardSession{
			NodeID:       nodeID,
			Client:       replica.Client,
			VolumeHandle: replica.VolumeHandle,
			GatewayID:    replica.GatewayID,
			HostID:       replica.HostID,
			SessionID:    replica.SessionID,
			AttachmentID: replica.AttachmentID,
			Generation:   replica.Generation,
		}
		sessions[nodeID] = session
		if replica.ReplicaID != "" {
			sessions[replica.ReplicaID] = session
		}
	}
	closeSessions := func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		for _, replica := range replicas {
			_, _ = replica.Client.CloseVolume(closeCtx, &service.CloseVolumeRequest{
				VolumeID:     volumeID,
				VolumeHandle: replica.VolumeHandle,
				Context: service.SBSRequestContext{
					RequestID:    fmt.Sprintf("ec-maint-close-%s-%s", volumeID, replica.ReplicaID),
					GatewayID:    replica.GatewayID,
					HostID:       replica.HostID,
					SessionID:    replica.SessionID,
					AttachmentID: replica.AttachmentID,
					Generation:   replica.Generation,
				},
			})
		}
	}
	return serviceSpecFromVolumeSpecRecord(specRec), clusterec.NewService(s.repo, sessions), sessions, closeSessions, nil
}

func (s *server) ecMaintenanceNodeClients(ctx context.Context, spec volumeSpecRecord) (map[string]service.SBSClient, error) {
	nodes, err := s.repo.ListNodeMemberships(ctx)
	if err != nil {
		return nil, err
	}
	clients := make(map[string]service.SBSClient)
	for _, node := range nodes {
		if node.LifecycleState != clustermeta.NodeLifecycleActive {
			continue
		}
		if node.HealthState != clustermeta.NodeHealthHealthy && node.HealthState != clustermeta.NodeHealthSuspect {
			continue
		}
		endpoint := endpointString(node.SBSEndpoints)
		if endpoint == "" {
			continue
		}
		client, err := s.cache.Get(endpoint)
		if err != nil {
			return nil, err
		}
		client = newMaterializingSBSClient(client, nodeAdminHTTPEndpoint(node), spec)
		clients[node.NodeID] = client
	}
	if len(clients) == 0 {
		return nil, fmt.Errorf("no active EC maintenance node clients")
	}
	return clients, nil
}

func (s *server) debugECRequestContext(action string, params debugECParams) service.SBSRequestContext {
	idem := params.IdempotencyKey
	if idem == "" {
		idem = fmt.Sprintf("%s-%s-%s-%d-%d", action, params.VolumeID, params.StripeID, params.StripeGeneration, time.Now().UnixNano())
	}
	reqID := fmt.Sprintf("%s-%s-%s", action, params.VolumeID, params.StripeID)
	if len(reqID) > 120 {
		reqID = reqID[:120]
	}
	return service.SBSRequestContext{
		RequestID:      reqID,
		GatewayID:      "sbs-service",
		HostID:         s.nodeID,
		SessionID:      "ec-admin",
		AttachmentID:   "ec-admin-" + params.VolumeID,
		Generation:     1,
		IdempotencyKey: idem,
		TraceID:        reqID,
	}
}

func (s *server) ecMaintenanceMaxStripesPerRun() int {
	return maxInt(getenvInt("NAMRBD_SBS_EC_MAINTENANCE_MAX_STRIPES_PER_RUN", 1), 1)
}

func (s *server) ecMaintenanceScanInterval() time.Duration {
	interval := getenvDuration("NAMRBD_SBS_EC_MAINTENANCE_SCAN_INTERVAL", defaultECMaintenanceScanTTL)
	if interval <= 0 {
		return defaultECMaintenanceScanTTL
	}
	return interval
}

func ecMaintenanceScanBucket(now time.Time, interval time.Duration) int64 {
	if interval <= 0 {
		interval = defaultECMaintenanceScanTTL
	}
	return now.UnixNano() / int64(interval)
}

func ecMaintenanceScanOperationID(volumeID string, stripe clustermeta.ECStripeRecord, bucket int64) string {
	return fmt.Sprintf("ec-maint-scan-%s-%s-%d-%d", volumeID, stripe.StripeID, stripe.StripeGeneration, bucket)
}

func (s *server) logECMaintenanceScanResult(result ecMaintenanceRunResult) {
	if !result.worked() {
		return
	}
	log.Printf("sbs-service ec maintenance scan volume=%s scanned=%d skipped=%d scrubbed=%d repaired=%d repair_paused=%d blocked=%d errors=%d first_error=%q last_error=%q",
		result.VolumeID, result.ScannedStripes, result.SkippedStripes, result.ScrubbedStripes, result.RepairedShards,
		result.RepairPausedCount, result.BlockedCount, result.ErrorCount, result.FirstError, result.LastError)
	s.markVolumeMaintenanceRun(result.VolumeID, s.currentTime())
}

func ecMaintenanceStripePageNo(stripeID string) []uint64 {
	parsed, err := strconv.ParseUint(stripeID, 10, 64)
	if err != nil {
		return nil
	}
	return []uint64{parsed}
}

type ecMaintenanceScanDisposition int

const (
	ecMaintenanceScanStart ecMaintenanceScanDisposition = iota
	ecMaintenanceScanSkip
	ecMaintenanceScanResume
)

func (s *server) ecMaintenanceScanOperationDisposition(ctx context.Context, volumeID, operationID string) (ecMaintenanceScanDisposition, error) {
	operation, err := s.repo.GetMutationOperation(ctx, volumeID, operationID)
	if err != nil {
		if errors.Is(err, clustermeta.ErrNotFound) {
			return ecMaintenanceScanStart, nil
		}
		return ecMaintenanceScanSkip, err
	}
	switch operation.State {
	case clustermeta.MutationOperationPending, clustermeta.MutationOperationRunning:
		return ecMaintenanceScanResume, nil
	case clustermeta.MutationOperationFailed:
		if ecMaintenanceScanFailureRetryable(operation.ErrorMessage) {
			return ecMaintenanceScanResume, nil
		}
		return ecMaintenanceScanSkip, nil
	case clustermeta.MutationOperationCommitted, clustermeta.MutationOperationRolledBack:
		return ecMaintenanceScanSkip, nil
	default:
		return ecMaintenanceScanSkip, nil
	}
}

func ecMaintenanceScanFailureRetryable(message string) bool {
	msg := strings.ToLower(strings.TrimSpace(message))
	return msg == context.Canceled.Error() || msg == context.DeadlineExceeded.Error() ||
		strings.Contains(msg, "context canceled") ||
		strings.Contains(msg, "context deadline exceeded")
}

func (s *server) beginECMaintenanceScanOperation(ctx context.Context, volume clustermeta.VolumeState, stripe clustermeta.ECStripeRecord, operationID string, now time.Time) error {
	return s.repo.PutMutationOperation(ctx, clustermeta.MutationOperationRecord{
		OperationID:        operationID,
		VolumeID:           volume.VolumeID,
		Kind:               ecMaintenanceScanOperationKind,
		State:              clustermeta.MutationOperationRunning,
		PlacementRevision:  stripe.TopologyRevision,
		WriterFencingEpoch: volume.Epoch,
		IdempotencyKey:     operationID,
		AffectedPageNos:    ecMaintenanceStripePageNo(stripe.StripeID),
		StartedAtUnix:      now.Unix(),
		LastUpdatedAtUnix:  now.Unix(),
	})
}

func (s *server) resumeECMaintenanceScanOperation(ctx context.Context, volume clustermeta.VolumeState, stripe clustermeta.ECStripeRecord, operationID string, now time.Time) error {
	operation, err := s.repo.GetMutationOperation(ctx, volume.VolumeID, operationID)
	if err != nil {
		if errors.Is(err, clustermeta.ErrNotFound) {
			return s.beginECMaintenanceScanOperation(ctx, volume, stripe, operationID, now)
		}
		return err
	}
	operation.State = clustermeta.MutationOperationRunning
	operation.Kind = ecMaintenanceScanOperationKind
	operation.PlacementRevision = stripe.TopologyRevision
	operation.WriterFencingEpoch = volume.Epoch
	operation.IdempotencyKey = operationID
	operation.AffectedPageNos = ecMaintenanceStripePageNo(stripe.StripeID)
	if operation.StartedAtUnix == 0 {
		operation.StartedAtUnix = now.Unix()
	}
	operation.LastUpdatedAtUnix = now.Unix()
	operation.ErrorMessage = ""
	return s.repo.PutMutationOperation(ctx, operation)
}

func (s *server) markECMaintenanceScanCommitted(ctx context.Context, volumeID, operationID string, placementRevision uint64, note string) error {
	operation, err := s.repo.GetMutationOperation(ctx, volumeID, operationID)
	if err != nil {
		return err
	}
	operation.State = clustermeta.MutationOperationCommitted
	operation.PlacementRevision = placementRevision
	operation.LastUpdatedAtUnix = s.currentTime().Unix()
	operation.ErrorMessage = note
	return s.repo.PutMutationOperation(ctx, operation)
}

func (s *server) markECMaintenanceScanFailed(ctx context.Context, volumeID, operationID string, runErr error) error {
	operation, err := s.repo.GetMutationOperation(ctx, volumeID, operationID)
	if err != nil {
		return err
	}
	operation.State = clustermeta.MutationOperationFailed
	operation.LastUpdatedAtUnix = s.currentTime().Unix()
	if runErr != nil {
		operation.ErrorMessage = runErr.Error()
	}
	return s.repo.PutMutationOperation(ctx, operation)
}

func (s *server) runECMaintenanceScanOnce(ctx context.Context, volume clustermeta.VolumeState, spec volumeSpecRecord, settings maintenanceSnapshot) (ecMaintenanceRunResult, error) {
	s.ecMaintenanceMu.Lock()
	defer s.ecMaintenanceMu.Unlock()

	result := ecMaintenanceRunResult{VolumeID: volume.VolumeID}
	if effectiveVolumeRedundancyBackend(volume, spec) != clustermeta.RedundancyBackendEC {
		return result, nil
	}
	stripes, err := s.repo.ListECStripes(ctx, volume.VolumeID)
	if err != nil {
		if errors.Is(err, clustermeta.ErrNotFound) {
			return result, nil
		}
		return result, err
	}
	sort.Slice(stripes, func(i, j int) bool {
		left, leftErr := strconv.ParseUint(stripes[i].StripeID, 10, 64)
		right, rightErr := strconv.ParseUint(stripes[j].StripeID, 10, 64)
		if leftErr == nil && rightErr == nil && left != right {
			return left < right
		}
		if stripes[i].StripeID != stripes[j].StripeID {
			return stripes[i].StripeID < stripes[j].StripeID
		}
		return stripes[i].StripeGeneration < stripes[j].StripeGeneration
	})

	now := s.currentTime()
	interval := s.ecMaintenanceScanInterval()
	bucket := ecMaintenanceScanBucket(now, interval)
	maxStripes := s.ecMaintenanceMaxStripesPerRun()
	for _, stripe := range stripes {
		if result.ScannedStripes >= maxStripes {
			break
		}
		if stripe.State != clustermeta.ECStripeStateCommitted {
			result.SkippedStripes++
			continue
		}
		operationID := ecMaintenanceScanOperationID(volume.VolumeID, stripe, bucket)
		disposition, err := s.ecMaintenanceScanOperationDisposition(ctx, volume.VolumeID, operationID)
		if err != nil {
			return result, err
		}
		if disposition == ecMaintenanceScanSkip {
			result.SkippedStripes++
			continue
		}
		if disposition == ecMaintenanceScanResume {
			if err := s.resumeECMaintenanceScanOperation(ctx, volume, stripe, operationID, now); err != nil {
				return result, err
			}
		} else {
			if err := s.beginECMaintenanceScanOperation(ctx, volume, stripe, operationID, now); err != nil {
				return result, err
			}
		}
		result.ScannedStripes++
		if err := s.runECMaintenanceStripeScan(ctx, volume, stripe, operationID, settings, &result); err != nil {
			result.recordError(err)
			_ = s.markECMaintenanceScanFailed(ctx, volume.VolumeID, operationID, err)
			log.Printf("sbs-service ec maintenance stripe scan failed volume=%s stripe_id=%s stripe_generation=%d operation_id=%s err=%v",
				volume.VolumeID, stripe.StripeID, stripe.StripeGeneration, operationID, err)
		}
	}
	return result, nil
}

func (s *server) runECMaintenanceStripeScan(ctx context.Context, volume clustermeta.VolumeState, stripe clustermeta.ECStripeRecord, operationID string, settings maintenanceSnapshot, result *ecMaintenanceRunResult) error {
	params := debugECParams{
		VolumeID:         volume.VolumeID,
		ObjectID:         stripe.ObjectID,
		StripeID:         stripe.StripeID,
		StripeGeneration: stripe.StripeGeneration,
		IdempotencyKey:   fmt.Sprintf("ec-maint-scrub-%s-%s-%d-%s", volume.VolumeID, stripe.StripeID, stripe.StripeGeneration, operationID),
	}
	volumeSpec, ecSvc, closeSessions, err := s.newECMaintenanceService(ctx, volume.VolumeID)
	if err != nil {
		return err
	}
	defer closeSessions()
	scrubResp, err := ecSvc.ScrubStripe(ctx, clusterec.ScrubRequest{
		Volume:           volumeSpec,
		Context:          s.debugECRequestContext("ec-maint-scrub", params),
		ObjectID:         params.ObjectID,
		StripeID:         params.StripeID,
		StripeGeneration: params.StripeGeneration,
	})
	if err != nil {
		return err
	}
	result.ScrubbedStripes++
	placementRevision := stripe.TopologyRevision
	note := ""
	shardID, repairable := firstECRepairCandidate(scrubResp.Findings)
	if repairable {
		if settings.pauseRepairs {
			result.RepairPausedCount++
			note = "repair_paused"
		} else {
			params.ShardID = shardID
			params.ShardIDSet = true
			params.IdempotencyKey = fmt.Sprintf("ec-maint-repair-%s-%s-%d-%d-%s", volume.VolumeID, stripe.StripeID, stripe.StripeGeneration, shardID, operationID)
			repairResp, err := ecSvc.RepairShard(ctx, clusterec.RepairRequest{
				Volume:           volumeSpec,
				Context:          s.debugECRequestContext("ec-maint-repair", params),
				ObjectID:         stripe.ObjectID,
				StripeID:         stripe.StripeID,
				StripeGeneration: stripe.StripeGeneration,
				ShardID:          shardID,
			})
			if err != nil {
				return err
			}
			result.RepairedShards++
			placementRevision = repairResp.TopologyRevision
		}
	} else if len(scrubResp.Findings) > 0 {
		result.BlockedCount++
		note = "scrub_findings_not_repairable"
	}
	return s.markECMaintenanceScanCommitted(ctx, volume.VolumeID, operationID, placementRevision, note)
}

func firstECRepairCandidate(findings []clusterec.ScrubFinding) (uint32, bool) {
	for _, finding := range findings {
		if finding.RepairCandidate {
			return finding.ShardID, true
		}
	}
	return 0, false
}

func debugECScrubResponseJSON(volumeID string, resp *clusterec.ScrubResponse) map[string]any {
	return map[string]any{
		"ok":                true,
		"volume_id":         volumeID,
		"operation_id":      resp.OperationID,
		"object_id":         resp.ObjectID,
		"stripe_id":         resp.StripeID,
		"stripe_generation": resp.StripeGeneration,
		"scrub_state":       "committed",
		"healthy":           resp.Healthy,
		"parity_verified":   resp.ParityVerified,
		"checked_shards":    resp.CheckedShards,
		"missing_shards":    resp.MissingShards,
		"corrupt_shards":    resp.CorruptShards,
		"findings":          resp.Findings,
		"blocked_reason":    "",
	}
}

func debugECRepairResponseJSON(volumeID string, resp *clusterec.RepairResponse) map[string]any {
	return map[string]any{
		"ok":                true,
		"volume_id":         volumeID,
		"operation_id":      resp.OperationID,
		"object_id":         resp.ObjectID,
		"stripe_id":         resp.StripeID,
		"stripe_generation": resp.StripeGeneration,
		"shard_id":          resp.ShardID,
		"node_id":           resp.NodeID,
		"zone":              resp.Zone,
		"store_id":          resp.StoreID,
		"checksum":          resp.Checksum,
		"topology_revision": resp.TopologyRevision,
		"rebuild_state":     "committed",
		"blocked_reason":    "",
	}
}

func debugECMaintenanceScanResponseJSON(result ecMaintenanceRunResult) map[string]any {
	return map[string]any{
		"ok":                  result.ErrorCount == 0,
		"volume_id":           result.VolumeID,
		"scanned_stripes":     result.ScannedStripes,
		"skipped_stripes":     result.SkippedStripes,
		"scrubbed_stripes":    result.ScrubbedStripes,
		"repaired_shards":     result.RepairedShards,
		"repair_paused_count": result.RepairPausedCount,
		"blocked_count":       result.BlockedCount,
		"error_count":         result.ErrorCount,
		"first_error":         result.FirstError,
		"last_error":          result.LastError,
	}
}

func debugECRebalanceResponseJSON(volumeID string, resp *clusterec.RebalanceShardResponse) map[string]any {
	return map[string]any{
		"ok":                true,
		"volume_id":         volumeID,
		"operation_id":      resp.OperationID,
		"object_id":         resp.ObjectID,
		"stripe_id":         resp.StripeID,
		"stripe_generation": resp.StripeGeneration,
		"shard_id":          resp.ShardID,
		"source_node_id":    resp.SourceNodeID,
		"source_zone":       resp.SourceZone,
		"source_store_id":   resp.SourceStoreID,
		"target_node_id":    resp.TargetNodeID,
		"target_zone":       resp.TargetZone,
		"target_store_id":   resp.TargetStoreID,
		"checksum":          resp.Checksum,
		"topology_revision": resp.TopologyRevision,
		"rebuild_state":     "",
		"blocked_reason":    "",
	}
}

func debugECDrainPreflightResponseJSON(volumeID string, resp *clusterec.DrainPreflightResponse) map[string]any {
	return map[string]any{
		"ok":                true,
		"volume_id":         volumeID,
		"stripe_id":         resp.StripeID,
		"stripe_generation": resp.StripeGeneration,
		"blocked":           resp.Blocked,
		"blocked_reason":    resp.BlockedReason,
		"weak":              resp.Weak,
		"affected_shards":   resp.AffectedShards,
		"plans":             resp.Plans,
		"zone_shard_counts": resp.ZoneShardCounts,
	}
}

func debugECDrainResponseJSON(volumeID string, resp *clusterec.DrainResponse) map[string]any {
	return map[string]any{
		"ok":                true,
		"volume_id":         volumeID,
		"operation_id":      resp.OperationID,
		"object_id":         resp.ObjectID,
		"stripe_id":         resp.StripeID,
		"stripe_generation": resp.StripeGeneration,
		"weak":              resp.Weak,
		"moved_shards":      resp.MovedShards,
		"plans":             resp.Plans,
		"zone_shard_counts": resp.ZoneShardCounts,
		"topology_revision": resp.TopologyRevision,
	}
}

func mutationOperationToAdminStatus(rec clustermeta.MutationOperationRecord, related []clustermeta.MutationOperationRecord) *adminv1.OperationStatus {
	return &adminv1.OperationStatus{
		OperationId:      rec.OperationID,
		Kind:             rec.Kind,
		State:            mutationOperationStateToAdminState(rec.State),
		TargetVolumeId:   rec.VolumeID,
		Phase:            mutationOperationPhase(rec, related),
		BlockingReason:   rec.IdempotencyKey,
		StartedAt:        unixTimestamp(rec.StartedAtUnix),
		LastProgressAt:   unixTimestamp(rec.LastUpdatedAtUnix),
		ErrorMessage:     rec.ErrorMessage,
		ExtentsRemaining: rec.PlacementRevision,
		BytesRemaining:   rec.AllocationRevision,
	}
}

func mutationOperationStateToAdminState(state clustermeta.MutationOperationState) adminv1.OperationState {
	switch state {
	case clustermeta.MutationOperationPending:
		return adminv1.OperationState_OPERATION_STATE_QUEUED
	case clustermeta.MutationOperationRunning:
		return adminv1.OperationState_OPERATION_STATE_RUNNING
	case clustermeta.MutationOperationCommitted:
		return adminv1.OperationState_OPERATION_STATE_COMPLETED
	case clustermeta.MutationOperationFailed:
		return adminv1.OperationState_OPERATION_STATE_FAILED
	case clustermeta.MutationOperationRolledBack:
		return adminv1.OperationState_OPERATION_STATE_CANCELED
	default:
		return adminv1.OperationState_OPERATION_STATE_UNSPECIFIED
	}
}

func mutationOperationPhase(rec clustermeta.MutationOperationRecord, related []clustermeta.MutationOperationRecord) string {
	base := ""
	switch {
	case rec.AllocationRevision > 0 && rec.PlacementRevision > 0:
		base = fmt.Sprintf("placement_rev=%d allocation_rev=%d", rec.PlacementRevision, rec.AllocationRevision)
	case rec.AllocationRevision > 0:
		base = fmt.Sprintf("allocation_rev=%d", rec.AllocationRevision)
	case rec.PlacementRevision > 0:
		base = fmt.Sprintf("placement_rev=%d", rec.PlacementRevision)
	default:
		base = ""
	}
	switch rec.Kind {
	case "payload_gc":
		summary := payloadGCBatchPhaseSummary(rec, related)
		return appendPhase(base, summary)
	case "payload_gc_batch":
		parent := rec.IdempotencyKey
		if parent == "" {
			parent = rec.VolumeID
		}
		summary := fmt.Sprintf("parent=%s chunks=%d", parent, len(rec.RetiredPhysicalChunkIDs))
		return appendPhase(base, summary)
	case "transition":
		summary := transitionBatchPhaseSummary(rec, related)
		if summary != "" {
			return appendPhase(base, summary)
		}
		if len(rec.AffectedPageNos) > 0 || len(rec.CompletedPageNos) > 0 {
			remainingPages := subtractMutationCompletedPages(rec.AffectedPageNos, rec.CompletedPageNos)
			summary := fmt.Sprintf("pages=%d completed_pages=%d extents=%d chunks=%d remaining_pages=%d", len(rec.AffectedPageNos), len(rec.CompletedPageNos), len(rec.AffectedExtentIDs), len(rec.RetiredPhysicalChunkIDs), len(remainingPages))
			if rec.State == clustermeta.MutationOperationPending && len(remainingPages) > 0 {
				summary += fmt.Sprintf(" retry=requeued remaining_retry_pages=%d", len(remainingPages))
				if retrySummary := retryPageWindowPhaseSummary(rec.RetryPageWindows); retrySummary != "" {
					summary += " " + retrySummary
				}
			}
			return appendPhase(base, summary)
		}
		return base
	case "transition_batch":
		parent := rec.IdempotencyKey
		if parent == "" {
			parent = rec.VolumeID
		}
		remainingPages := subtractMutationCompletedPages(rec.AffectedPageNos, rec.CompletedPageNos)
		summary := fmt.Sprintf("parent=%s pages=%d completed_pages=%d remaining_pages=%d", parent, len(rec.AffectedPageNos), len(rec.CompletedPageNos), len(remainingPages))
		if parentRec, ok := findRelatedMutationOperationByID(related, rec.IdempotencyKey, rec.VolumeID); ok && parentRec.Kind == "transition" && parentRec.State == clustermeta.MutationOperationPending && len(remainingPages) > 0 {
			summary += " parent_retry=requeued"
		}
		return appendPhase(base, summary)
	default:
		if len(rec.AffectedPageNos) > 0 || len(rec.AffectedExtentIDs) > 0 || len(rec.RetiredPhysicalChunkIDs) > 0 {
			summary := fmt.Sprintf("extents=%d pages=%d chunks=%d", len(rec.AffectedExtentIDs), len(rec.AffectedPageNos), len(rec.RetiredPhysicalChunkIDs))
			return appendPhase(base, summary)
		}
		return base
	}
}

func payloadGCBatchPhaseSummary(rec clustermeta.MutationOperationRecord, related []clustermeta.MutationOperationRecord) string {
	total := 0
	running := 0
	failed := 0
	completed := 0
	for _, candidate := range related {
		if candidate.Kind != "payload_gc_batch" || candidate.VolumeID != rec.VolumeID || candidate.IdempotencyKey != rec.OperationID {
			continue
		}
		total++
		switch candidate.State {
		case clustermeta.MutationOperationRunning, clustermeta.MutationOperationPending:
			running++
		case clustermeta.MutationOperationFailed:
			failed++
		case clustermeta.MutationOperationCommitted:
			completed++
		}
	}
	return fmt.Sprintf("batches=%d completed=%d running=%d failed=%d chunks=%d", total, completed, running, failed, len(rec.RetiredPhysicalChunkIDs))
}

func transitionBatchPhaseSummary(rec clustermeta.MutationOperationRecord, related []clustermeta.MutationOperationRecord) string {
	total := 0
	running := 0
	failed := 0
	completed := 0
	recent := 0
	small := 0
	recentPagesByExtent := make(map[uint64]map[uint64]struct{})
	for _, candidate := range related {
		if candidate.VolumeID != rec.VolumeID || candidate.Kind != "write" || candidate.State == clustermeta.MutationOperationRolledBack {
			continue
		}
		for _, extentID := range candidate.AffectedExtentIDs {
			pageSet := recentPagesByExtent[extentID]
			if pageSet == nil {
				pageSet = make(map[uint64]struct{})
				recentPagesByExtent[extentID] = pageSet
			}
			for _, pageNo := range candidate.AffectedPageNos {
				pageSet[pageNo] = struct{}{}
			}
		}
	}
	for _, candidate := range related {
		if candidate.Kind != "transition_batch" || candidate.VolumeID != rec.VolumeID || candidate.IdempotencyKey != rec.OperationID {
			continue
		}
		total++
		remainingPages := subtractMutationCompletedPages(candidate.AffectedPageNos, candidate.CompletedPageNos)
		if len(remainingPages) <= 1 {
			small++
		}
		if mutationPagesTouchRecentSet(candidate.AffectedExtentIDs, remainingPages, recentPagesByExtent) {
			recent++
		}
		switch candidate.State {
		case clustermeta.MutationOperationRunning, clustermeta.MutationOperationPending:
			running++
		case clustermeta.MutationOperationFailed:
			failed++
		case clustermeta.MutationOperationCommitted:
			completed++
		}
	}
	if total == 0 {
		return ""
	}
	remainingPages := subtractMutationCompletedPages(rec.AffectedPageNos, rec.CompletedPageNos)
	summary := fmt.Sprintf("batches=%d completed=%d running=%d failed=%d recent=%d small=%d pages=%d completed_pages=%d remaining_retry_pages=%d remaining_retry_batches=%d", total, completed, running, failed, recent, small, len(rec.AffectedPageNos), len(rec.CompletedPageNos), len(remainingPages), running+failed)
	if rec.State == clustermeta.MutationOperationPending && len(remainingPages) > 0 {
		summary += " retry=requeued"
		if retrySummary := retryPageWindowPhaseSummary(rec.RetryPageWindows); retrySummary != "" {
			summary += " " + retrySummary
		}
	}
	return summary
}

func retryPageWindowPhaseSummary(windows []clustermeta.MutationPageWindowRecord) string {
	if len(windows) == 0 {
		return ""
	}
	next := windows[0]
	var totalBytes uint64
	var totalChunks uint64
	for _, window := range windows {
		totalBytes += window.DataBytes
		totalChunks += window.DataChunks
	}
	return fmt.Sprintf(
		"retry_windows=%d retry_window_bytes=%d retry_window_chunks=%d next_retry_window=extent:%d pages:%d-%d bytes:%d chunks:%d",
		len(windows),
		totalBytes,
		totalChunks,
		next.ExtentID,
		next.StartPageNo,
		next.EndPageNo,
		next.DataBytes,
		next.DataChunks,
	)
}

func appendPhase(base, extra string) string {
	switch {
	case base == "":
		return extra
	case extra == "":
		return base
	default:
		return base + " " + extra
	}
}

func subtractMutationCompletedPages(affected, completed []uint64) []uint64 {
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
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

func uniqueSortedMutationPageNos(values []uint64) []uint64 {
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

func mutationPagesTouchRecentSet(extentIDs, pageNos []uint64, recentPagesByExtent map[uint64]map[uint64]struct{}) bool {
	if len(pageNos) == 0 || len(extentIDs) == 0 {
		return false
	}
	for _, extentID := range extentIDs {
		pageSet := recentPagesByExtent[extentID]
		if len(pageSet) == 0 {
			continue
		}
		for _, pageNo := range pageNos {
			if _, ok := pageSet[pageNo]; ok {
				return true
			}
		}
	}
	return false
}

func findRelatedMutationOperationByID(related []clustermeta.MutationOperationRecord, operationID, volumeID string) (clustermeta.MutationOperationRecord, bool) {
	for _, candidate := range related {
		if candidate.VolumeID != volumeID || candidate.OperationID != operationID {
			continue
		}
		return candidate, true
	}
	return clustermeta.MutationOperationRecord{}, false
}

func (s *server) debugPayloadGCSweep(ctx context.Context, payloadRoot, volumeID string) ([]clusterreplication.LocalPayloadSweepResult, error) {
	collector, closeStores, err := s.openLocalPayloadGarbageCollector(ctx, payloadRoot)
	if err != nil {
		return nil, err
	}
	defer closeStores()
	if volumeID == "" {
		return collector.SweepAll(ctx)
	}
	return collector.SweepVolume(ctx, volumeID)
}

func (s *server) openLocalPayloadGarbageCollector(ctx context.Context, payloadRoot string) (*clusterreplication.LocalPayloadGarbageCollector, func(), error) {
	nodes, err := s.repo.ListNodeMemberships(ctx)
	if err != nil {
		return nil, func() {}, fmt.Errorf("list node memberships: %w", err)
	}
	replicaIDs := make([]string, 0, len(nodes))
	seen := make(map[string]struct{}, len(nodes))
	for _, node := range nodes {
		replicaID := strings.TrimSpace(node.ReplicaID)
		if replicaID == "" {
			continue
		}
		if _, ok := seen[replicaID]; ok {
			continue
		}
		seen[replicaID] = struct{}{}
		replicaIDs = append(replicaIDs, replicaID)
	}
	if len(replicaIDs) == 0 {
		return nil, func() {}, fmt.Errorf("no replica ids found in node membership records")
	}
	replicaStores, err := clusterpayload.OpenReplicaStores(payloadRoot, replicaIDs)
	if err != nil {
		return nil, func() {}, fmt.Errorf("open replica payload stores: %w", err)
	}
	collector := clusterreplication.NewLocalPayloadGarbageCollector(s.repo, replicaStores.ObjectStores())
	return collector, func() {
		_ = replicaStores.Close()
	}, nil
}

func (s *server) debugEnqueueTransition(ctx context.Context, reason, volumeID string, extentID uint64) (clustermeta.ReplicaSetState, clustermeta.PlacementTransitionRecord, error) {
	mapping, currentReplicaSet, err := s.extentPlacement(ctx, volumeID, extentID)
	if err != nil {
		return clustermeta.ReplicaSetState{}, clustermeta.PlacementTransitionRecord{}, err
	}
	if existing, err := s.repo.GetPlacementTransition(ctx, volumeID, mapping.PlacementRef); err == nil && isActiveTransitionState(existing.State) {
		return clustermeta.ReplicaSetState{}, clustermeta.PlacementTransitionRecord{}, fmt.Errorf("active transition already exists for %s", mapping.PlacementRef)
	} else if err != nil && !errors.Is(err, clustermeta.ErrNotFound) {
		return clustermeta.ReplicaSetState{}, clustermeta.PlacementTransitionRecord{}, err
	}
	replaceNodeID := currentReplicaSet.Replicas[len(currentReplicaSet.Replicas)-1].NodeID
	targetReplicaSet, ok, err := s.planReplacementReplicaSet(ctx, volumeID, mapping, currentReplicaSet, replaceNodeID, reason, nil)
	if err != nil {
		return clustermeta.ReplicaSetState{}, clustermeta.PlacementTransitionRecord{}, err
	}
	if !ok {
		return clustermeta.ReplicaSetState{}, clustermeta.PlacementTransitionRecord{}, fmt.Errorf("no replacement candidate available for extent %d", extentID)
	}
	if err := s.repo.PutReplicaSet(ctx, targetReplicaSet); err != nil {
		return clustermeta.ReplicaSetState{}, clustermeta.PlacementTransitionRecord{}, err
	}
	svc := s.newMaintenanceService()
	switch reason {
	case "repair":
		tr, err := svc.EnqueueRepair(ctx, volumeID, extentID, targetReplicaSet.ReplicaSetID)
		return targetReplicaSet, tr, err
	case "rebalance":
		tr, err := svc.EnqueueRebalance(ctx, volumeID, extentID, targetReplicaSet.ReplicaSetID)
		return targetReplicaSet, tr, err
	default:
		return clustermeta.ReplicaSetState{}, clustermeta.PlacementTransitionRecord{}, fmt.Errorf("unsupported reason %q", reason)
	}
}

func (s *server) extentPlacement(ctx context.Context, volumeID string, extentID uint64) (clustermeta.ExtentMappingRecord, clustermeta.ReplicaSetState, error) {
	mappings, err := s.repo.ListExtentMappings(ctx, volumeID)
	if err != nil {
		return clustermeta.ExtentMappingRecord{}, clustermeta.ReplicaSetState{}, err
	}
	var mapping clustermeta.ExtentMappingRecord
	found := false
	for _, candidate := range mappings {
		if candidate.ExtentID != extentID {
			continue
		}
		mapping = candidate
		found = true
		break
	}
	if !found {
		return clustermeta.ExtentMappingRecord{}, clustermeta.ReplicaSetState{}, fmt.Errorf("extent %d not found", extentID)
	}
	replicaSets, err := s.repo.ListReplicaSets(ctx, volumeID)
	if err != nil {
		return clustermeta.ExtentMappingRecord{}, clustermeta.ReplicaSetState{}, err
	}
	for _, replicaSet := range replicaSets {
		if replicaSet.PlacementRef == mapping.PlacementRef {
			return mapping, replicaSet, nil
		}
	}
	return clustermeta.ExtentMappingRecord{}, clustermeta.ReplicaSetState{}, fmt.Errorf("replica set for placement %s not found", mapping.PlacementRef)
}

func (s *server) createInitialPlacement(ctx context.Context, volumeID string, sizeBytes, extentSizeBytes uint64, replicationFactor uint32, policyName, topologyMode string, nodes []clustermeta.NodeMembershipRecord) error {
	layout, err := planVolumeLayout(volumeID, sizeBytes, extentSizeBytes, replicationFactor, topologyMode, nodes)
	if err != nil {
		return err
	}
	for _, extent := range layout.Extents {
		if err := s.putPlannedExtent(ctx, volumeID, extent); err != nil {
			return err
		}
	}
	_ = policyName
	return nil
}

func planVolumeLayout(volumeID string, sizeBytes, extentSizeBytes uint64, replicationFactor uint32, topologyMode string, nodes []clustermeta.NodeMembershipRecord) (placement.InitialLayout, error) {
	candidates := make([]placement.CandidateNode, 0, len(nodes))
	for _, node := range nodes {
		candidates = append(candidates, placement.CandidateNode{
			NodeID: node.NodeID,
			Zone:   node.Zone,
		})
	}
	policy := placement.NewRFSpreadPolicy()
	return policy.PlanInitialLayout(placement.InitialLayoutRequest{
		VolumeID:          volumeID,
		SizeBytes:         sizeBytes,
		ExtentSizeBytes:   extentSizeBytes,
		ReplicationFactor: replicationFactor,
		Candidates:        candidates,
		TopologyMode:      topologyMode,
	})
}

func (s *server) ensureExpansionPlacement(ctx context.Context, volumeID string, oldSpec, newSpec clustermeta.VolumeSpecRecord, nodes []clustermeta.NodeMembershipRecord) error {
	if newSpec.SizeBytes <= oldSpec.SizeBytes {
		return nil
	}
	layout, err := planVolumeLayout(volumeID, newSpec.SizeBytes, newSpec.ExtentSizeBytes, newSpec.ReplicationFactor, newSpec.TopologyMode, nodes)
	if err != nil {
		return err
	}
	existingMappings, err := s.repo.ListExtentMappings(ctx, volumeID)
	if err != nil {
		return err
	}
	existingByExtentID := make(map[uint64]clustermeta.ExtentMappingRecord, len(existingMappings))
	for _, mapping := range existingMappings {
		existingByExtentID[mapping.ExtentID] = mapping
	}
	for _, extent := range layout.Extents {
		extentEnd := extent.LogicalOffset + extent.LengthBytes
		if extentEnd <= oldSpec.SizeBytes {
			continue
		}
		if existing, ok := existingByExtentID[extent.ExtentID]; ok {
			if extent.LengthBytes > existing.LengthBytes {
				existing.LengthBytes = extent.LengthBytes
				existing.Revision++
				if existing.Revision == 0 {
					existing.Revision = 1
				}
				if err := s.repo.PutExtentMapping(ctx, existing); err != nil {
					return err
				}
			}
			continue
		}
		if err := s.putPlannedExtent(ctx, volumeID, extent); err != nil {
			return err
		}
	}
	return nil
}

func (s *server) putPlannedExtent(ctx context.Context, volumeID string, extent placement.ExtentPlan) error {
	replicas := make([]clustermeta.ReplicaDescriptor, 0, len(extent.ReplicaSet.Replicas))
	for _, replica := range extent.ReplicaSet.Replicas {
		role := clustermeta.ReplicaRoleSecondary
		if replica.Primary {
			role = clustermeta.ReplicaRolePrimary
		}
		replicas = append(replicas, clustermeta.ReplicaDescriptor{
			NodeID:        replica.NodeID,
			ReplicaID:     replica.ReplicaID,
			Role:          role,
			FailureDomain: replica.FailureDomain,
		})
	}
	if err := s.repo.PutReplicaSet(ctx, clustermeta.ReplicaSetState{
		ReplicaSetID:     extent.ReplicaSet.ReplicaSetID,
		VolumeID:         volumeID,
		PlacementRef:     extent.ReplicaSet.PlacementRef,
		Epoch:            1,
		Replicas:         replicas,
		PrimaryReplicaID: extent.ReplicaSet.PrimaryReplicaID,
		WriteQuorum:      extent.ReplicaSet.WriteQuorum,
		ReadQuorum:       extent.ReplicaSet.ReadQuorum,
	}); err != nil {
		return err
	}
	return s.repo.PutExtentMapping(ctx, clustermeta.ExtentMappingRecord{
		VolumeID:      volumeID,
		ExtentID:      extent.ExtentID,
		LogicalOffset: extent.LogicalOffset,
		LengthBytes:   extent.LengthBytes,
		// Phase G+ volumes start from authoritative zero allocation pages.
		// Keep legacy chunk ownership unassigned until a real allocation commit occurs.
		ChunkID:      0,
		PlacementRef: extent.ReplicaSet.PlacementRef,
		Revision:     1,
	})
}

func listKeys(ctx context.Context, kv clustermeta.KV, prefix string) ([]string, error) {
	cursor := ""
	var out []string
	for {
		keys, next, err := kv.List(ctx, prefix, cursor, 128)
		if err != nil {
			return nil, err
		}
		out = append(out, keys...)
		if next == "" {
			return out, nil
		}
		cursor = next
	}
}

func deleteByPrefix(ctx context.Context, kv clustermeta.KV, prefix string) error {
	keys, err := listKeys(ctx, kv, prefix)
	if err != nil {
		return err
	}
	for _, key := range keys {
		if err := kv.Delete(ctx, key); err != nil {
			return err
		}
	}
	return nil
}

func endpointString(endpoints []clustermeta.SBSEndpoint) string {
	if len(endpoints) == 0 {
		return ""
	}
	return fmt.Sprintf("%s:%d", endpoints[0].Address, endpoints[0].Port)
}

func replicaSetToProto(replicaSet clustermeta.ReplicaSetState) *adminv1.ReplicaSetSummary {
	out := &adminv1.ReplicaSetSummary{
		ReplicaSetId:     replicaSet.ReplicaSetID,
		VolumeId:         replicaSet.VolumeID,
		PlacementRef:     replicaSet.PlacementRef,
		Epoch:            replicaSet.Epoch,
		PrimaryReplicaId: replicaSet.PrimaryReplicaID,
		WriteQuorum:      replicaSet.WriteQuorum,
		ReadQuorum:       replicaSet.ReadQuorum,
		FailureDomains:   append([]string(nil), replicaSet.FailureDomains...),
	}
	for _, replica := range replicaSet.Replicas {
		out.Replicas = append(out.Replicas, &adminv1.ReplicaMemberSummary{
			NodeId:        replica.NodeID,
			ReplicaId:     replica.ReplicaID,
			Role:          string(replica.Role),
			FailureDomain: replica.FailureDomain,
		})
	}
	return out
}

func lifecycleToProto(v clustermeta.NodeLifecycleState) adminv1.NodeLifecycle {
	switch v {
	case clustermeta.NodeLifecycleJoining:
		return adminv1.NodeLifecycle_NODE_LIFECYCLE_JOINING
	case clustermeta.NodeLifecycleActive:
		return adminv1.NodeLifecycle_NODE_LIFECYCLE_ACTIVE
	case clustermeta.NodeLifecycleDraining:
		return adminv1.NodeLifecycle_NODE_LIFECYCLE_DRAINING
	case clustermeta.NodeLifecycleRemoved:
		return adminv1.NodeLifecycle_NODE_LIFECYCLE_REMOVED
	default:
		return adminv1.NodeLifecycle_NODE_LIFECYCLE_UNSPECIFIED
	}
}

func healthToProto(v clustermeta.NodeHealthState) adminv1.NodeHealth {
	switch v {
	case clustermeta.NodeHealthHealthy:
		return adminv1.NodeHealth_NODE_HEALTH_HEALTHY
	case clustermeta.NodeHealthSuspect:
		return adminv1.NodeHealth_NODE_HEALTH_SUSPECT
	case clustermeta.NodeHealthDown:
		return adminv1.NodeHealth_NODE_HEALTH_DOWN
	default:
		return adminv1.NodeHealth_NODE_HEALTH_UNSPECIFIED
	}
}

func volumeHealthToProto(v clustermeta.VolumeStatus) adminv1.VolumeHealth {
	switch v {
	case clustermeta.VolumeStatusHealthy:
		return adminv1.VolumeHealth_VOLUME_HEALTH_HEALTHY
	case clustermeta.VolumeStatusDegraded:
		return adminv1.VolumeHealth_VOLUME_HEALTH_DEGRADED
	case clustermeta.VolumeStatusRepairing:
		return adminv1.VolumeHealth_VOLUME_HEALTH_REPAIRING
	case clustermeta.VolumeStatusRebalancing:
		return adminv1.VolumeHealth_VOLUME_HEALTH_REBALANCING
	case clustermeta.VolumeStatusBlocked:
		return adminv1.VolumeHealth_VOLUME_HEALTH_BLOCKED
	default:
		return adminv1.VolumeHealth_VOLUME_HEALTH_UNSPECIFIED
	}
}

func operationStateFromString(v string) adminv1.OperationState {
	switch v {
	case adminv1.OperationState_OPERATION_STATE_QUEUED.String():
		return adminv1.OperationState_OPERATION_STATE_QUEUED
	case adminv1.OperationState_OPERATION_STATE_RUNNING.String():
		return adminv1.OperationState_OPERATION_STATE_RUNNING
	case adminv1.OperationState_OPERATION_STATE_COMPLETED.String():
		return adminv1.OperationState_OPERATION_STATE_COMPLETED
	case adminv1.OperationState_OPERATION_STATE_FAILED.String():
		return adminv1.OperationState_OPERATION_STATE_FAILED
	case adminv1.OperationState_OPERATION_STATE_CANCELED.String():
		return adminv1.OperationState_OPERATION_STATE_CANCELED
	default:
		return adminv1.OperationState_OPERATION_STATE_UNSPECIFIED
	}
}

func unixTimestamp(v int64) *timestamppb.Timestamp {
	if v <= 0 {
		return nil
	}
	return timestamppb.New(time.Unix(v, 0).UTC())
}

func maxUint32(v, fallback uint32) uint32 {
	if v == 0 {
		return fallback
	}
	return v
}

func minUint64(a, b uint64) uint64 {
	if a < b {
		return a
	}
	return b
}

func maxInt(v, fallback int) int {
	if v <= 0 {
		return fallback
	}
	return v
}

func configuredExtentSizeBytes(requestedExtentSizeBytes uint64, blockSize uint32) (uint64, error) {
	extentSizeBytes := requestedExtentSizeBytes
	if extentSizeBytes == 0 {
		extentSizeBytes = getenvUint64("NAMRBD_SBS_EXTENT_SIZE", placement.DefaultExtentSizeBytes)
	}
	if extentSizeBytes == 0 {
		return 0, fmt.Errorf("must be greater than zero")
	}
	if blockSize > 0 && extentSizeBytes%uint64(blockSize) != 0 {
		return 0, fmt.Errorf("must be aligned to block_size")
	}
	return extentSizeBytes, nil
}

func configuredAllocationGeometry(requestedChunkSizeBytes, requestedPageSizeBytes uint32) (uint32, uint32, error) {
	chunkSizeBytes := uint64(requestedChunkSizeBytes)
	if chunkSizeBytes == 0 {
		chunkSizeBytes = getenvUint64("NAMRBD_SBS_ALLOCATION_CHUNK_SIZE", service.DefaultAllocationChunkSize)
	}
	extentPageBytes := uint64(requestedPageSizeBytes)
	if extentPageBytes == 0 {
		extentPageBytes = getenvUint64("NAMRBD_SBS_ALLOCATION_PAGE_SIZE", service.DefaultAllocationPageSize)
	}
	if chunkSizeBytes == 0 || chunkSizeBytes > uint64(^uint32(0)) {
		return 0, 0, fmt.Errorf("allocation chunk size must fit uint32 and be greater than zero")
	}
	if extentPageBytes == 0 || extentPageBytes > uint64(^uint32(0)) {
		return 0, 0, fmt.Errorf("allocation page size must fit uint32 and be greater than zero")
	}
	return uint32(chunkSizeBytes), uint32(extentPageBytes), nil
}

func configureMaintenanceService(svc *clustermaintenance.Service) {
	svc.SetTransitionCopyChunkBytes(getenvUint64("NAMRBD_SBS_TRANSITION_COPY_CHUNK_SIZE", clustermaintenance.DefaultTransitionCopyChunkBytes))
}

func (s *server) newPlacementApplyInternalService() clustercontrol.PlacementApplyInternalService {
	if s.placementApplyInternalService != nil {
		return s.placementApplyInternalService
	}
	return clustercontrol.NewRepositoryBackedPlacementApplyInternalService(s.repo)
}

func (s *server) newWriteSessionInternalService() clustercontrol.WriteSessionInternalService {
	if s.writeSessionInternalService != nil {
		return s.writeSessionInternalService
	}
	return clustercontrol.NewRepositoryBackedWriteSessionInternalService(s.repo)
}

func (s *server) newECMetadataInternalService() clustercontrol.ECMetadataInternalService {
	if s.ecMetadataInternalService != nil {
		return s.ecMetadataInternalService
	}
	return clustercontrol.NewRepositoryBackedECMetadataInternalService(s.repo)
}

func (s *server) newChunkIDAllocatorInternalService() clustercontrol.ChunkIDAllocatorInternalService {
	if s.chunkIDAllocatorService != nil {
		return s.chunkIDAllocatorService
	}
	return clustercontrol.NewRepositoryBackedChunkIDAllocatorInternalService(s.repo)
}

func (s *server) newPlacementResolverInternalService() clustercontrol.PlacementResolverInternalService {
	if s.placementResolverService != nil {
		return s.placementResolverService
	}
	return clustercontrol.NewRepositoryBackedPlacementResolverInternalService(s.repo)
}

func (s *server) effectivePlacementApplyTimeout() time.Duration {
	if s.placementApplyTimeout == 0 {
		return defaultPlacementApplyTimeout
	}
	return s.placementApplyTimeout
}

func (s *server) newMaintenanceService() *clustermaintenance.Service {
	var placementApply clustercontrol.PlacementApplyAdapter = clustercontrol.NewServiceBackedPlacementApplyAdapter(s.newPlacementApplyInternalService())
	placementApplyTimeout := s.effectivePlacementApplyTimeout()
	if placementApplyTimeout > 0 {
		placementApply = clustercontrol.NewTimeoutPlacementApplyAdapter(placementApply, placementApplyTimeout)
	}
	return clustermaintenance.NewServiceWithPlacementApply(s.repo, placementApply)
}

func (m *maintenanceSettings) snapshot() maintenanceSnapshot {
	m.mu.RLock()
	defer m.mu.RUnlock()
	generation := m.generation
	if generation == 0 {
		generation = 1
	}
	return maintenanceSnapshot{
		generation:              generation,
		maxConcurrentRepairs:    maxInt(m.maxConcurrentRepairs, 1),
		maxConcurrentRebalances: maxInt(m.maxConcurrentRebalances, 1),
		maxConcurrentDrains:     maxInt(m.maxConcurrentDrains, 1),
		maxConcurrentPayloadGCs: maxInt(m.maxConcurrentPayloadGCs, 1),
		pauseRepairs:            m.pauseRepairs,
		pauseRebalances:         m.pauseRebalances,
		pauseDrains:             m.pauseDrains,
		pausePayloadGCs:         m.pausePayloadGCs,
	}
}

func (s *server) currentTime() time.Time {
	if s != nil && s.now != nil {
		return s.now()
	}
	return time.Now()
}

func (s *server) maintenanceCooldown() time.Duration {
	if s == nil || s.maintenanceVolumeCooldown <= 0 {
		return 5 * time.Second
	}
	return s.maintenanceVolumeCooldown
}

func (s *server) autoRebalanceMinVolumeLifetime() time.Duration {
	if s == nil || s.autoRebalanceMinVolumeAge < 0 {
		return defaultAutoRebalanceMinVolumeAge
	}
	return s.autoRebalanceMinVolumeAge
}

func (s *server) autoRebalanceForegroundWriteSettleLifetime() time.Duration {
	if s == nil || s.autoRebalanceForegroundWriteSettleAge < 0 {
		return defaultAutoRebalanceForegroundWriteSettleAge
	}
	return s.autoRebalanceForegroundWriteSettleAge
}

func (s *server) nodeHealthCheckInterval() time.Duration {
	if s == nil || s.healthCheckInterval <= 0 {
		return 10 * time.Second
	}
	return s.healthCheckInterval
}

func (s *server) nodeHealthCheckTimeout() time.Duration {
	if s == nil || s.healthCheckTimeout <= 0 {
		return 2 * time.Second
	}
	return s.healthCheckTimeout
}

func (s *server) nodeHealthMinimumShardCount() int {
	if s == nil || s.healthMinimumShardCount <= 0 {
		return 1
	}
	return s.healthMinimumShardCount
}

func (s *server) nodeHealthConcurrencyPerShard() int {
	if s == nil || s.healthConcurrencyPerShard <= 0 {
		return nodeHealthShardConcurrency
	}
	return min(s.healthConcurrencyPerShard, nodeHealthShardConcurrency)
}

func (s *server) nodeHealthSuspectAfter() uint32 {
	if s == nil || s.healthSuspectAfter == 0 {
		return 3
	}
	return s.healthSuspectAfter
}

func (s *server) nodeHealthDownAfter() uint32 {
	if s == nil || s.healthDownAfter == 0 {
		return 6
	}
	return s.healthDownAfter
}

func (s *server) nodeHealthRecoverAfter() uint32 {
	if s == nil || s.healthRecoverAfter == 0 {
		return 2
	}
	return s.healthRecoverAfter
}

func (s *server) nodeHealthRecoveryCooldown() time.Duration {
	if s == nil || s.healthRecoveryCooldown <= 0 {
		return 30 * time.Second
	}
	return s.healthRecoveryCooldown
}

func (s *server) maintenanceCooldownState(volumeID string, now time.Time) (bool, int64) {
	if s == nil {
		return false, 0
	}
	s.maintenanceMu.Lock()
	defer s.maintenanceMu.Unlock()
	if s.lastMaintenanceRunByVolume == nil {
		return false, 0
	}
	lastUnix, ok := s.lastMaintenanceRunByVolume[volumeID]
	if !ok || lastUnix == 0 {
		return false, 0
	}
	remaining := lastUnix + int64(s.maintenanceCooldown().Seconds()) - now.Unix()
	if remaining <= 0 {
		return false, 0
	}
	return true, remaining
}

func (s *server) markVolumeMaintenanceRun(volumeID string, now time.Time) {
	if s == nil {
		return
	}
	s.maintenanceMu.Lock()
	defer s.maintenanceMu.Unlock()
	if s.lastMaintenanceRunByVolume == nil {
		s.lastMaintenanceRunByVolume = make(map[string]int64)
	}
	s.lastMaintenanceRunByVolume[volumeID] = now.Unix()
}

func (s *server) runBackgroundMaintenance(ctx context.Context) {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	for {
		if err := s.runMaintenanceOnce(ctx); err != nil && ctx.Err() == nil {
			log.Printf("sbs-service background loop error: %v", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (s *server) runBackgroundECMaintenance(ctx context.Context) {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	for {
		if err := s.runECMaintenanceScanPass(ctx); err != nil && ctx.Err() == nil {
			log.Printf("sbs-service ec maintenance loop error: %v", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (s *server) runECMaintenanceScanPass(ctx context.Context) error {
	if !s.ready.Load() {
		return nil
	}
	if s.leader != nil && !s.leader.IsLeader() {
		return nil
	}
	settings, err := s.loadMaintenanceSettingsSnapshot(ctx)
	if err != nil {
		return err
	}
	volumes, err := s.repo.ListVolumeStates(ctx)
	if err != nil {
		return err
	}
	for _, volume := range volumes {
		if err := ctx.Err(); err != nil {
			return err
		}
		spec, err := s.getVolumeSpec(ctx, volume.VolumeID)
		if err != nil {
			if volume.RedundancyBackend == clustermeta.RedundancyBackendEC {
				log.Printf("sbs-service ec maintenance volume=%s stage=get_volume_spec error: %v", volume.VolumeID, err)
			}
			continue
		}
		if effectiveVolumeRedundancyBackend(volume, spec) != clustermeta.RedundancyBackendEC {
			continue
		}
		if err := s.reconcileMutationScopes(ctx, volume.VolumeID, spec); err != nil {
			log.Printf("sbs-service ec maintenance volume=%s stage=reconcile_mutation_scopes error: %v", volume.VolumeID, err)
			continue
		}
		result, err := s.runECMaintenanceScanOnce(ctx, volume, spec, settings)
		if err != nil {
			log.Printf("sbs-service ec maintenance volume=%s stage=scan error: %v", volume.VolumeID, err)
			continue
		}
		s.logECMaintenanceScanResult(result)
	}
	return nil
}

func (s *server) runBackgroundNodeHealthReconciler(ctx context.Context) {
	ticker := time.NewTicker(s.nodeHealthCheckInterval())
	defer ticker.Stop()
	for {
		// Classify on a loop this service already runs. Refresh does no I/O.
		// It is here rather than inside runNodeHealthReconcilerOnce because
		// that returns early on a follower, and a follower's dependency state
		// is exactly what an operator needs during a leader-side outage.
		dependencyTracker.Refresh()

		if err := s.runNodeHealthReconcilerOnce(ctx); err != nil && ctx.Err() == nil {
			log.Printf("sbs-service node health reconciler error: %v", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (s *server) beginNodeHealthRun(queueDepth, shardCount int) {
	s.healthStatusMu.Lock()
	s.healthStatus = nodeHealthReconcilerStatus{
		ShardCount:     shardCount,
		QueueDepth:     queueDepth,
		PeakQueueDepth: queueDepth,
		LastRunUnix:    s.currentTime().Unix(),
	}
	s.healthStatusMu.Unlock()
}

func (s *server) noteNodeHealthProbeStarted() {
	s.healthStatusMu.Lock()
	if s.healthStatus.QueueDepth > 0 {
		s.healthStatus.QueueDepth--
	}
	s.healthStatus.InFlight++
	if s.healthStatus.InFlight > s.healthStatus.MaxInFlight {
		s.healthStatus.MaxInFlight = s.healthStatus.InFlight
	}
	s.healthStatusMu.Unlock()
}

func (s *server) noteNodeHealthProbeCompleted(err error) {
	s.healthStatusMu.Lock()
	s.healthStatus.ProbeCount++
	if s.healthStatus.InFlight > 0 {
		s.healthStatus.InFlight--
	}
	if err != nil {
		message := err.Error()
		if s.healthStatus.FirstError == "" {
			s.healthStatus.FirstError = message
		}
		s.healthStatus.LastError = message
	}
	s.healthStatusMu.Unlock()
}

func (s *server) noteNodeHealthTransition() {
	s.healthStatusMu.Lock()
	s.healthStatus.TransitionCount++
	s.healthStatusMu.Unlock()
}

func (s *server) noteNodeHealthVolumeReconcile() {
	s.healthStatusMu.Lock()
	s.healthStatus.VolumeReconcileCount++
	s.healthStatusMu.Unlock()
}

func (s *server) noteNodeHealthError(err error) {
	if err == nil {
		return
	}
	s.healthStatusMu.Lock()
	message := err.Error()
	if s.healthStatus.FirstError == "" {
		s.healthStatus.FirstError = message
	}
	s.healthStatus.LastError = message
	s.healthStatusMu.Unlock()
}

func (s *server) finishNodeHealthRun() {
	s.healthStatusMu.Lock()
	s.healthStatus.QueueDepth = 0
	s.healthStatusMu.Unlock()
}

func (s *server) nodeHealthStatusSnapshot() nodeHealthReconcilerStatus {
	s.healthStatusMu.RLock()
	defer s.healthStatusMu.RUnlock()
	return s.healthStatus
}

func (s *server) runNodeHealthReconcilerOnce(ctx context.Context) error {
	if !s.ready.Load() {
		return nil
	}
	if s.leader != nil && !s.leader.IsLeader() {
		return nil
	}
	nodes, err := s.repo.ListNodeMemberships(ctx)
	if err != nil {
		return err
	}
	eligible := make([]clustermeta.NodeMembershipRecord, 0, len(nodes))
	for _, node := range nodes {
		if node.LifecycleState == clustermeta.NodeLifecycleActive || node.LifecycleState == clustermeta.NodeLifecycleDraining {
			eligible = append(eligible, node)
		}
	}
	shardCount := (len(eligible) + nodeHealthShardSize - 1) / nodeHealthShardSize
	if len(eligible) > 0 {
		shardCount = max(shardCount, s.nodeHealthMinimumShardCount())
		shardCount = min(shardCount, len(eligible))
	}
	s.beginNodeHealthRun(len(eligible), shardCount)
	controller := clustercontrol.NewFromRepository(s.repo)
	transitioned := false
	var runErrors []error
	shardSize := nodeHealthShardSize
	if shardCount > 0 {
		shardSize = (len(eligible) + shardCount - 1) / shardCount
	}
	for start := 0; start < len(eligible); start += shardSize {
		end := min(start+shardSize, len(eligible))
		results := s.probeNodeHealthShard(ctx, eligible[start:end])
		sort.Slice(results, func(i, j int) bool { return results[i].node.NodeID < results[j].node.NodeID })
		for _, result := range results {
			changed, err := s.reconcileNodeHealthResult(ctx, controller, result)
			if changed {
				transitioned = true
				s.noteNodeHealthTransition()
			}
			if err != nil {
				s.noteNodeHealthError(err)
				runErrors = append(runErrors, fmt.Errorf("node %s: %w", result.node.NodeID, err))
			}
		}
	}
	if transitioned {
		if _, _, err := controller.ReconcileNodeHealthTransitions(ctx); err != nil {
			s.noteNodeHealthError(err)
			runErrors = append(runErrors, fmt.Errorf("reconcile node health transitions: %w", err))
		} else {
			s.noteNodeHealthVolumeReconcile()
		}
	}
	s.finishNodeHealthRun()
	return errors.Join(runErrors...)
}

func (s *server) reconcileNodeHealth(ctx context.Context, controller *clustercontrol.Controller, node clustermeta.NodeMembershipRecord) error {
	storeSummary, probeErr := s.probeSBSDataNode(ctx, node)
	_, err := s.reconcileNodeHealthResult(ctx, controller, nodeHealthProbeResult{
		node: node, summary: storeSummary, err: probeErr,
	})
	return err
}

type nodeHealthProbeResult struct {
	node    clustermeta.NodeMembershipRecord
	summary nodeStoreHealthSummary
	err     error
}

func (s *server) probeNodeHealthShard(ctx context.Context, nodes []clustermeta.NodeMembershipRecord) []nodeHealthProbeResult {
	results := make(chan nodeHealthProbeResult, len(nodes))
	semaphore := make(chan struct{}, s.nodeHealthConcurrencyPerShard())
	var wg sync.WaitGroup
	for _, node := range nodes {
		node := node
		wg.Add(1)
		go func() {
			defer wg.Done()
			select {
			case semaphore <- struct{}{}:
			case <-ctx.Done():
				results <- nodeHealthProbeResult{node: node, err: ctx.Err()}
				return
			}
			s.noteNodeHealthProbeStarted()
			summary, err := s.probeSBSDataNode(ctx, node)
			s.noteNodeHealthProbeCompleted(err)
			<-semaphore
			results <- nodeHealthProbeResult{node: node, summary: summary, err: err}
		}()
	}
	wg.Wait()
	close(results)
	out := make([]nodeHealthProbeResult, 0, len(nodes))
	for result := range results {
		out = append(out, result)
	}
	return out
}

func (s *server) reconcileNodeHealthResult(ctx context.Context, controller *clustercontrol.Controller, result nodeHealthProbeResult) (bool, error) {
	node := result.node
	storeSummary := result.summary
	probeErr := result.err
	now := s.currentTime().Unix()
	detail, err := s.repo.GetNodeHealthDetail(ctx, node.NodeID)
	if err != nil && !errors.Is(err, clustermeta.ErrNotFound) {
		return false, err
	}
	if detail.NodeID == "" {
		detail.NodeID = node.NodeID
	}
	if detail.OverrideExpiresAtUnix > now {
		return false, nil
	}
	transitioned := false

	detail.LastProbeUnix = now
	detail.HealthUpdatedBy = clustermeta.HealthUpdatedByReconciler
	detail.StoreCount = storeSummary.StoreCount
	detail.HealthyStoreCount = storeSummary.HealthyStoreCount
	detail.WritableStoreCount = storeSummary.WritableStoreCount
	detail.AllocatableStoreCount = storeSummary.AllocatableStoreCount
	detail.StoreAllocationWeightTotal = storeSummary.AllocationWeightTotal
	detail.StoreAllocationWeightObserved = storeSummary.AllocationWeightObserved
	detail.StoreCapacityBytes = storeSummary.CapacityBytes
	detail.StoreAvailableBytes = storeSummary.AvailableBytes
	detail.StoreUsedBytes = storeSummary.UsedBytes
	detail.StoreCompactionPendingBytes = storeSummary.CompactionPendingBytes
	detail.StoreCompactionInProgressBytes = storeSummary.CompactionInProgressBytes

	if probeErr == nil {
		detail.LastProbeError = ""
		detail.HealthReason = "probe_ok"
		detail.ConsecutiveProbeFailures = 0
		detail.ConsecutiveProbeSuccesses++
		if node.HealthState != clustermeta.NodeHealthHealthy && detail.ConsecutiveProbeSuccesses >= s.nodeHealthRecoverAfter() {
			rec, err := controller.SetNodeHealthOnly(ctx, node.NodeID, clustermeta.NodeHealthHealthy)
			if err != nil {
				return false, err
			}
			node = rec
			transitioned = true
			detail.RecoveryEligibleAtUnix = now + int64(s.nodeHealthRecoveryCooldown().Seconds())
		}
	} else {
		detail.LastProbeError = probeErr.Error()
		detail.HealthReason = probeErr.Error()
		detail.ConsecutiveProbeSuccesses = 0
		detail.ConsecutiveProbeFailures++
		detail.RecoveryEligibleAtUnix = 0
		switch {
		case detail.ConsecutiveProbeFailures >= s.nodeHealthDownAfter() && node.HealthState != clustermeta.NodeHealthDown:
			rec, err := controller.SetNodeHealthOnly(ctx, node.NodeID, clustermeta.NodeHealthDown)
			if err != nil {
				return false, err
			}
			node = rec
			transitioned = true
		case detail.ConsecutiveProbeFailures >= s.nodeHealthSuspectAfter() && node.HealthState == clustermeta.NodeHealthHealthy:
			rec, err := controller.SetNodeHealthOnly(ctx, node.NodeID, clustermeta.NodeHealthSuspect)
			if err != nil {
				return false, err
			}
			node = rec
			transitioned = true
		}
	}

	switch node.HealthState {
	case clustermeta.NodeHealthHealthy:
		detail.HealthReason = "healthy"
	case clustermeta.NodeHealthSuspect:
		if detail.HealthReason == "" {
			detail.HealthReason = "probe_suspect"
		}
	case clustermeta.NodeHealthDown:
		if detail.HealthReason == "" {
			detail.HealthReason = "probe_down"
		}
	}
	return transitioned, s.repo.PutNodeHealthDetail(ctx, detail)
}

func (s *server) probeSBSDataNode(ctx context.Context, node clustermeta.NodeMembershipRecord) (nodeStoreHealthSummary, error) {
	if s != nil && s.probeNodeHealth != nil {
		return nodeStoreHealthSummary{}, s.probeNodeHealth(ctx, node)
	}
	adminEndpoint := strings.TrimSpace(nodeAdminHTTPEndpoint(node))
	if adminEndpoint == "" {
		return nodeStoreHealthSummary{}, fmt.Errorf("missing admin endpoint")
	}
	if err := s.probeSBSDataHealthz(ctx, adminEndpoint); err != nil {
		return nodeStoreHealthSummary{}, err
	}
	storeSummary, err := s.probeSBSDataStoreHealthWithTimeout(ctx, adminEndpoint)
	if err != nil {
		return nodeStoreHealthSummary{}, err
	}
	endpoint := endpointString(node.SBSEndpoints)
	if endpoint == "" {
		return nodeStoreHealthSummary{}, fmt.Errorf("missing sbs endpoint")
	}
	if err := s.probeSBSDataGRPCDial(ctx, endpoint); err != nil {
		return nodeStoreHealthSummary{}, err
	}
	return storeSummary, nil
}

func (s *server) probeSBSDataHealthz(ctx context.Context, adminEndpoint string) error {
	probeCtx, cancel := context.WithTimeout(ctx, s.nodeHealthCheckTimeout())
	defer cancel()
	req, err := http.NewRequestWithContext(probeCtx, http.MethodGet, strings.TrimRight(adminEndpoint, "/")+"/healthz", nil)
	if err != nil {
		return err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("healthz probe failed: %w", err)
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("healthz status=%d", resp.StatusCode)
	}
	return nil
}

func (s *server) probeSBSDataGRPCDial(ctx context.Context, endpoint string) error {
	probeCtx, cancel := context.WithTimeout(ctx, s.nodeHealthCheckTimeout())
	defer cancel()
	var dialer net.Dialer
	conn, err := dialer.DialContext(probeCtx, "tcp", endpoint)
	if err != nil {
		return fmt.Errorf("grpc dial failed: %w", err)
	}
	_ = conn.Close()
	return nil
}

func (s *server) probeSBSDataStoreHealthWithTimeout(ctx context.Context, adminEndpoint string) (nodeStoreHealthSummary, error) {
	probeCtx, cancel := context.WithTimeout(ctx, s.nodeHealthCheckTimeout())
	defer cancel()
	return s.probeSBSDataStoreHealth(probeCtx, adminEndpoint)
}

func (s *server) probeSBSDataStoreHealth(ctx context.Context, adminEndpoint string) (nodeStoreHealthSummary, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimRight(adminEndpoint, "/")+"/debug/store-health", nil)
	if err != nil {
		return nodeStoreHealthSummary{}, err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nodeStoreHealthSummary{}, fmt.Errorf("store health probe failed: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		_, _ = io.Copy(io.Discard, resp.Body)
		return s.probeSBSDataDebugSummary(ctx, adminEndpoint)
	}
	if resp.StatusCode != http.StatusOK {
		_, _ = io.Copy(io.Discard, resp.Body)
		return nodeStoreHealthSummary{}, fmt.Errorf("store health status=%d", resp.StatusCode)
	}
	var summary sbsDataStoreHealthSummary
	if err := json.NewDecoder(resp.Body).Decode(&summary); err != nil {
		return nodeStoreHealthSummary{}, fmt.Errorf("decode store health: %w", err)
	}
	return summarizeSBSDataStores(summary.Stores), nil
}

func (s *server) updateNodeStoreWeightsViaAdminHTTP(ctx context.Context, adminEndpoint string, stores []*adminv1.StoreWeightSummary) (bool, error) {
	payload := struct {
		Stores []local.StoreWeightUpdate `json:"stores"`
	}{
		Stores: make([]local.StoreWeightUpdate, 0, len(stores)),
	}
	for _, store := range stores {
		payload.Stores = append(payload.Stores, local.StoreWeightUpdate{
			StoreID: store.GetStoreId(),
			Weight:  int(store.GetWeight()),
		})
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return false, fmt.Errorf("marshal request: %w", err)
	}
	return s.postNodeAdminJSON(ctx, adminEndpoint, []string{"/admin/store-weights", "/debug/store-weights"}, body)
}

func (s *server) updateNodeStoreTuningViaAdminHTTP(ctx context.Context, adminEndpoint string, stores []*adminv1.StoreTuningSummary) (bool, error) {
	payload := struct {
		Stores []local.StoreTuningUpdate `json:"stores"`
	}{
		Stores: make([]local.StoreTuningUpdate, 0, len(stores)),
	}
	for _, store := range stores {
		payload.Stores = append(payload.Stores, local.StoreTuningUpdate{
			StoreID: store.GetStoreId(),
			Weight:  int(store.GetWeight()),
		})
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return false, fmt.Errorf("marshal request: %w", err)
	}
	return s.postNodeAdminJSON(ctx, adminEndpoint, []string{"/admin/store-tuning", "/debug/store-tuning"}, body)
}

func (s *server) postNodeAdminJSON(ctx context.Context, adminEndpoint string, paths []string, body []byte) (bool, error) {
	base := strings.TrimRight(adminEndpoint, "/")
	var lastErr error
	for i, suffix := range paths {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, base+suffix, bytes.NewReader(body))
		if err != nil {
			return false, err
		}
		req.Header.Set("Content-Type", "application/json")
		resp, err := s.httpClientOrDefault().Do(req)
		if err != nil {
			lastErr = err
			continue
		}
		if resp.StatusCode == http.StatusNotFound && i < len(paths)-1 {
			_, _ = io.Copy(io.Discard, resp.Body)
			_ = resp.Body.Close()
			lastErr = fmt.Errorf("status=%d", resp.StatusCode)
			continue
		}
		if resp.StatusCode != http.StatusOK {
			payload, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
			_ = resp.Body.Close()
			return false, fmt.Errorf("status=%d body=%s", resp.StatusCode, strings.TrimSpace(string(payload)))
		}
		var response struct {
			Persisted bool `json:"persisted"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&response); err != nil {
			_ = resp.Body.Close()
			return false, fmt.Errorf("decode response: %w", err)
		}
		_ = resp.Body.Close()
		return response.Persisted, nil
	}
	if lastErr != nil {
		return false, lastErr
	}
	return false, fmt.Errorf("no admin endpoint path succeeded")
}

func (s *server) httpClientOrDefault() *http.Client {
	if s != nil && s.httpClient != nil {
		return s.httpClient
	}
	return http.DefaultClient
}

func (s *server) probeSBSDataDebugSummary(ctx context.Context, adminEndpoint string) (nodeStoreHealthSummary, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimRight(adminEndpoint, "/")+"/debug/summary", nil)
	if err != nil {
		return nodeStoreHealthSummary{}, err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nodeStoreHealthSummary{}, fmt.Errorf("debug summary probe failed: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		_, _ = io.Copy(io.Discard, resp.Body)
		return nodeStoreHealthSummary{}, nil
	}
	if resp.StatusCode != http.StatusOK {
		_, _ = io.Copy(io.Discard, resp.Body)
		return nodeStoreHealthSummary{}, fmt.Errorf("debug summary status=%d", resp.StatusCode)
	}
	var summary sbsDataDebugSummary
	if err := json.NewDecoder(resp.Body).Decode(&summary); err != nil {
		return nodeStoreHealthSummary{}, fmt.Errorf("decode debug summary: %w", err)
	}
	return summarizeSBSDataStores(summary.Stores), nil
}

func summarizeSBSDataStores(stores []sbsDataStoreSummary) nodeStoreHealthSummary {
	var summary nodeStoreHealthSummary
	for _, store := range stores {
		summary.StoreCount++
		weight := storeAllocationWeight(store)
		if weight != nil {
			summary.AllocationWeightObserved = true
			if *weight > 0 {
				summary.AllocationWeightTotal += *weight
			}
		}
		switch strings.TrimSpace(store.State) {
		case "", "healthy":
			summary.HealthyStoreCount++
		}
		writable := sbsDataStoreWritable(store.State)
		if writable {
			summary.WritableStoreCount++
		}
		if writable && weight != nil && *weight > 0 {
			summary.AllocatableStoreCount++
		}
		summary.CapacityBytes += store.CapacityBytes
		summary.AvailableBytes += store.AvailableBytes
		summary.UsedBytes += store.UsedBytes
		summary.CompactionPendingBytes += store.CompactionPendingBytes
		summary.CompactionInProgressBytes += store.CompactionInProgressBytes
	}
	return summary
}

func storeAllocationWeight(store sbsDataStoreSummary) *int {
	if store.AllocationWeight != nil {
		return store.AllocationWeight
	}
	return store.Weight
}

func sbsDataStoreWritable(state string) bool {
	switch strings.TrimSpace(state) {
	case "failed", "read_only", "draining":
		return false
	default:
		return true
	}
}

func (s *server) nodeEligibleForNewPlacement(ctx context.Context, node clustermeta.NodeMembershipRecord) bool {
	if node.LifecycleState != clustermeta.NodeLifecycleActive {
		return false
	}
	if node.HealthState != clustermeta.NodeHealthHealthy && node.HealthState != clustermeta.NodeHealthSuspect {
		return false
	}
	detail, err := s.repo.GetNodeHealthDetail(ctx, node.NodeID)
	if err != nil {
		return errors.Is(err, clustermeta.ErrNotFound)
	}
	if !detail.StorePlacementEligible() {
		return false
	}
	return detail.RecoveryEligibleAtUnix == 0 || detail.RecoveryEligibleAtUnix <= s.currentTime().Unix()
}

type maintenanceJob struct {
	volumeID           string
	reason             string
	reasonCount        uint64
	reasonBytes        uint64
	reasonChunks       uint64
	failedBatches      uint64
	retryWindows       uint64
	retryWindowBytes   uint64
	retryWindowChunks  uint64
	recentBatches      uint64
	smallBatches       uint64
	oldestFailedAgeSec uint64
	cooldownActive     bool
	cooldownRemaining  int64
}

type ecMaintenanceRunResult struct {
	VolumeID          string
	ScannedStripes    int
	SkippedStripes    int
	ScrubbedStripes   int
	RepairedShards    int
	RepairPausedCount int
	BlockedCount      int
	ErrorCount        int
	FirstError        string
	LastError         string
}

func (r *ecMaintenanceRunResult) recordError(err error) {
	if r == nil || err == nil {
		return
	}
	msg := err.Error()
	r.ErrorCount++
	if r.FirstError == "" {
		r.FirstError = msg
	}
	r.LastError = msg
}

func (r ecMaintenanceRunResult) worked() bool {
	return r.ScannedStripes > 0 || r.ScrubbedStripes > 0 || r.RepairedShards > 0
}

func (s *server) runMaintenanceOnce(ctx context.Context) error {
	if !s.ready.Load() {
		return nil
	}
	if s.leader != nil && !s.leader.IsLeader() {
		return nil
	}
	now := s.currentTime()
	settings, err := s.loadMaintenanceSettingsSnapshot(ctx)
	if err != nil {
		return err
	}
	volumes, err := s.repo.ListVolumeStates(ctx)
	if err != nil {
		return err
	}
	if err := s.enqueueDrainTransitionsForDrainingNodes(ctx); err != nil {
		log.Printf("sbs-service maintenance stage=enqueue_drain_transitions error: %v", err)
	}
	svc := s.newMaintenanceService()
	configureMaintenanceService(svc)
	jobs := make([]maintenanceJob, 0, len(volumes))
	for _, volume := range volumes {
		if s.beforeMaintenanceVolume != nil {
			s.beforeMaintenanceVolume(ctx, volume.VolumeID)
		}
		settings, err = s.loadMaintenanceSettingsSnapshot(ctx)
		if err != nil {
			return err
		}
		if spec, err := s.getVolumeSpec(ctx, volume.VolumeID); err == nil {
			if err := s.reconcileMutationScopes(ctx, volume.VolumeID, spec); err != nil {
				log.Printf("sbs-service maintenance volume=%s stage=reconcile_mutation_scopes error: %v", volume.VolumeID, err)
				continue
			}
			if effectiveVolumeRedundancyBackend(volume, spec) == clustermeta.RedundancyBackendEC {
				result, err := s.runECMaintenanceScanOnce(ctx, volume, spec, settings)
				if err != nil {
					log.Printf("sbs-service maintenance volume=%s stage=ec_maintenance_scan error: %v", volume.VolumeID, err)
					continue
				}
				s.logECMaintenanceScanResult(result)
				continue
			}
		} else if volume.RedundancyBackend == clustermeta.RedundancyBackendEC {
			log.Printf("sbs-service maintenance volume=%s stage=get_ec_volume_spec error: %v", volume.VolumeID, err)
			continue
		}
		if err := s.cleanupCompletedTransitions(ctx, volume.VolumeID); err != nil {
			log.Printf("sbs-service maintenance volume=%s stage=cleanup_completed_transitions error: %v", volume.VolumeID, err)
			continue
		}
		if err := s.requeueRetryableFailedTransitions(ctx, volume.VolumeID); err != nil {
			log.Printf("sbs-service maintenance volume=%s stage=requeue_retryable_failed_transitions error: %v", volume.VolumeID, err)
			continue
		}
		transitions, err := s.repo.ListPlacementTransitions(ctx, volume.VolumeID)
		if err != nil {
			log.Printf("sbs-service maintenance volume=%s stage=list_placement_transitions error: %v", volume.VolumeID, err)
			continue
		}
		if len(transitions) == 0 && !settings.pauseRebalances {
			if _, err := s.enqueueVolumeRebalance(ctx, volume.VolumeID); err != nil {
				log.Printf("sbs-service maintenance volume=%s stage=enqueue_rebalance error: %v", volume.VolumeID, err)
				continue
			}
			transitions, err = s.repo.ListPlacementTransitions(ctx, volume.VolumeID)
			if err != nil {
				log.Printf("sbs-service maintenance volume=%s stage=list_placement_transitions_after_enqueue error: %v", volume.VolumeID, err)
				continue
			}
		}
		reason := dominantTransitionReason(transitions)
		if reason == "" {
			reason = "repair"
		}
		backlog := s.summarizeVolumeTransitionsWithService(ctx, svc, volume.VolumeID)
		switch reason {
		case "drain":
			if settings.pauseDrains {
				continue
			}
		case "rebalance":
			if settings.pauseRebalances {
				continue
			}
		default:
			if settings.pauseRepairs {
				continue
			}
		}
		job := maintenanceJob{
			volumeID:           volume.VolumeID,
			reason:             reason,
			failedBatches:      backlog.FailedBatches,
			retryWindows:       backlog.RetryWindows,
			retryWindowBytes:   backlog.RetryWindowBytes,
			retryWindowChunks:  backlog.RetryWindowChunks,
			recentBatches:      backlog.RecentBatches,
			smallBatches:       backlog.SmallBatches,
			oldestFailedAgeSec: backlog.OldestFailedAgeSec,
		}
		job.cooldownActive, job.cooldownRemaining = s.maintenanceCooldownState(volume.VolumeID, now)
		switch reason {
		case "drain":
			job.reasonCount = backlog.DrainCount
			job.reasonBytes = backlog.DrainBytes
			job.reasonChunks = backlog.DrainChunks
		case "rebalance":
			job.reasonCount = backlog.RebalanceCount
			job.reasonBytes = backlog.RebalanceBytes
			job.reasonChunks = backlog.RebalanceChunks
		default:
			job.reasonCount = backlog.RepairCount
			job.reasonBytes = backlog.RepairBytes
			job.reasonChunks = backlog.RepairChunks
		}
		if job.reasonCount > 0 {
			log.Printf("sbs-service maintenance job volume=%s reason=%s count=%d bytes=%d chunks=%d cooldown_active=%t cooldown_remaining=%d pause_repairs=%t pause_rebalances=%t pause_drains=%t",
				job.volumeID, job.reason, job.reasonCount, job.reasonBytes, job.reasonChunks,
				job.cooldownActive, job.cooldownRemaining,
				settings.pauseRepairs, settings.pauseRebalances, settings.pauseDrains)
		}
		jobs = append(jobs, job)
	}
	settings, err = s.loadMaintenanceSettingsSnapshot(ctx)
	if err != nil {
		return err
	}
	filteredJobs := jobs[:0]
	for _, job := range jobs {
		switch job.reason {
		case "drain":
			if settings.pauseDrains {
				continue
			}
		case "rebalance":
			if settings.pauseRebalances {
				continue
			}
		default:
			if settings.pauseRepairs {
				continue
			}
		}
		filteredJobs = append(filteredJobs, job)
	}
	jobs = filteredJobs
	sortMaintenanceJobs(jobs)
	repairSem := make(chan struct{}, settings.maxConcurrentRepairs)
	rebalanceSem := make(chan struct{}, settings.maxConcurrentRebalances)
	drainSem := make(chan struct{}, settings.maxConcurrentDrains)
	var wg sync.WaitGroup
	errCh := make(chan error, len(jobs))
	for _, job := range jobs {
		var sem chan struct{}
		switch job.reason {
		case "drain":
			sem = drainSem
		case "rebalance":
			sem = rebalanceSem
		default:
			sem = repairSem
		}
		wg.Add(1)
		go func(current maintenanceJob, sem chan struct{}) {
			defer wg.Done()
			select {
			case <-ctx.Done():
				return
			case sem <- struct{}{}:
			}
			defer func() { <-sem }()
			replicaClients, err := s.buildReplicaClientMap(ctx, current.volumeID)
			if err != nil {
				log.Printf("sbs-service build replica clients volume=%s: %v", current.volumeID, err)
				errCh <- err
				return
			}
			worker := clustermaintenance.NewWorker(svc, clustermaintenance.WorkerConfig{
				VolumeID:       current.volumeID,
				ReplicaClients: replicaClients,
				GatewayID:      "sbs-service",
				HostID:         s.nodeID,
				RetryBackoff:   2 * time.Second,
				PollInterval:   1 * time.Second,
			})
			if current.reasonCount > 0 {
				log.Printf("sbs-service maintenance worker start volume=%s reason=%s count=%d", current.volumeID, current.reason, current.reasonCount)
			}
			worked, err := worker.RunOnce(ctx)
			if current.reasonCount > 0 {
				log.Printf("sbs-service maintenance worker done volume=%s reason=%s worked=%t err=%v", current.volumeID, current.reason, worked, err)
			}
			if worked {
				s.markVolumeMaintenanceRun(current.volumeID, s.currentTime())
			}
			if err != nil && ctx.Err() == nil {
				log.Printf("sbs-service worker volume=%s: %v", current.volumeID, err)
				errCh <- err
			}
		}(job, sem)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			continue
		}
	}
	s.runRetiredPayloadBacklogSweep(ctx, settings, volumes)
	return nil
}

func (s *server) requeueRetryableFailedTransitions(ctx context.Context, volumeID string) error {
	transitions, err := s.repo.ListPlacementTransitions(ctx, volumeID)
	if err != nil {
		if errors.Is(err, clustermeta.ErrNotFound) {
			return nil
		}
		return err
	}
	operations, err := s.repo.ListMutationOperations(ctx, volumeID)
	if err != nil {
		if errors.Is(err, clustermeta.ErrNotFound) {
			return nil
		}
		return err
	}
	parentByID := make(map[string]clustermeta.MutationOperationRecord, len(operations))
	childCountsByParent := make(map[string]int)
	retryWindowsByParent := make(map[string][]clustermeta.MutationPageWindowRecord)
	spec, specErr := s.getVolumeSpec(ctx, volumeID)
	pageBytes := uint32(0)
	chunkSizeBytes := uint32(0)
	if specErr == nil {
		pageBytes = spec.ExtentPageBytes
		chunkSizeBytes = spec.ChunkSizeBytes
	}
	for _, operation := range operations {
		if operation.Kind == "transition" {
			parentByID[operation.OperationID] = operation
			continue
		}
		if operation.Kind != "transition_batch" || operation.IdempotencyKey == "" {
			continue
		}
		remainingPages := subtractMutationCompletedPages(operation.AffectedPageNos, operation.CompletedPageNos)
		if len(remainingPages) == 0 {
			continue
		}
		switch operation.State {
		case clustermeta.MutationOperationPending, clustermeta.MutationOperationRunning, clustermeta.MutationOperationFailed:
			childCountsByParent[operation.IdempotencyKey]++
			retryWindowsByParent[operation.IdempotencyKey] = appendRetryPageWindows(retryWindowsByParent[operation.IdempotencyKey], operation.AffectedExtentIDs, remainingPages, pageBytes, chunkSizeBytes)
		}
	}
	nowUnix := time.Now().Unix()
	for _, transition := range transitions {
		if transition.State != clustermeta.PlacementTransitionFailed {
			continue
		}
		operationID := fmt.Sprintf("transition-%s", transition.PlacementRef)
		operation, ok := parentByID[operationID]
		if !ok || operation.Kind != "transition" {
			continue
		}
		incompletePages := subtractMutationCompletedPages(operation.AffectedPageNos, operation.CompletedPageNos)
		if len(incompletePages) == 0 {
			continue
		}
		if childCountsByParent[operationID] == 0 && operation.State != clustermeta.MutationOperationFailed {
			continue
		}
		transition.State = clustermeta.PlacementTransitionQueued
		transition.LastProgressAtUnix = nowUnix
		if err := s.repo.PutPlacementTransition(ctx, transition); err != nil {
			return err
		}
		operation.State = clustermeta.MutationOperationPending
		operation.LastUpdatedAtUnix = nowUnix
		operation.ErrorMessage = ""
		operation.RetryPageWindows = retryWindowsByParent[operationID]
		if err := s.repo.PutMutationOperation(ctx, operation); err != nil {
			return err
		}
	}
	return nil
}

func appendRetryPageWindows(in []clustermeta.MutationPageWindowRecord, extentIDs, pageNos []uint64, pageBytes, chunkSizeBytes uint32) []clustermeta.MutationPageWindowRecord {
	if len(extentIDs) == 0 || len(pageNos) == 0 {
		return in
	}
	pageNos = uniqueSortedMutationPageNos(pageNos)
	windowData := func(start, end uint64) (uint64, uint64) {
		pageCount := end - start + 1
		dataBytes := pageCount * uint64(pageBytes)
		var dataChunks uint64
		if pageBytes > 0 && chunkSizeBytes > 0 {
			chunksPerPage := (uint64(pageBytes) + uint64(chunkSizeBytes) - 1) / uint64(chunkSizeBytes)
			dataChunks = pageCount * chunksPerPage
		}
		return dataBytes, dataChunks
	}
	for _, extentID := range extentIDs {
		start := pageNos[0]
		prev := pageNos[0]
		for _, pageNo := range pageNos[1:] {
			if pageNo == prev+1 {
				prev = pageNo
				continue
			}
			dataBytes, dataChunks := windowData(start, prev)
			in = append(in, clustermeta.MutationPageWindowRecord{
				ExtentID:    extentID,
				StartPageNo: start,
				EndPageNo:   prev,
				DataBytes:   dataBytes,
				DataChunks:  dataChunks,
			})
			start = pageNo
			prev = pageNo
		}
		dataBytes, dataChunks := windowData(start, prev)
		in = append(in, clustermeta.MutationPageWindowRecord{
			ExtentID:    extentID,
			StartPageNo: start,
			EndPageNo:   prev,
			DataBytes:   dataBytes,
			DataChunks:  dataChunks,
		})
	}
	return sortUniqueRetryPageWindows(in)
}

func sortUniqueRetryPageWindows(in []clustermeta.MutationPageWindowRecord) []clustermeta.MutationPageWindowRecord {
	if len(in) <= 1 {
		return in
	}
	sort.Slice(in, func(i, j int) bool {
		if in[i].ExtentID != in[j].ExtentID {
			return in[i].ExtentID < in[j].ExtentID
		}
		if in[i].StartPageNo != in[j].StartPageNo {
			return in[i].StartPageNo < in[j].StartPageNo
		}
		return in[i].EndPageNo < in[j].EndPageNo
	})
	out := in[:0]
	for _, window := range in {
		if len(out) > 0 {
			last := out[len(out)-1]
			if last.ExtentID == window.ExtentID && last.StartPageNo == window.StartPageNo && last.EndPageNo == window.EndPageNo {
				continue
			}
		}
		out = append(out, window)
	}
	return out
}

func sortMaintenanceJobs(jobs []maintenanceJob) {
	sort.Slice(jobs, func(i, j int) bool {
		left := jobs[i]
		right := jobs[j]
		if left.reason != right.reason {
			return left.reason < right.reason
		}
		if left.failedBatches != right.failedBatches {
			return left.failedBatches > right.failedBatches
		}
		if left.cooldownActive != right.cooldownActive {
			return !left.cooldownActive
		}
		if left.cooldownActive && right.cooldownActive && left.cooldownRemaining != right.cooldownRemaining {
			return left.cooldownRemaining < right.cooldownRemaining
		}
		leftRetryBucket := retryFairnessBucket(left)
		rightRetryBucket := retryFairnessBucket(right)
		if leftRetryBucket != rightRetryBucket {
			return leftRetryBucket < rightRetryBucket
		}
		if left.retryWindowBytes != right.retryWindowBytes {
			return left.retryWindowBytes < right.retryWindowBytes
		}
		if left.retryWindowChunks != right.retryWindowChunks {
			return left.retryWindowChunks < right.retryWindowChunks
		}
		if left.retryWindows != right.retryWindows {
			return left.retryWindows < right.retryWindows
		}
		if left.recentBatches != right.recentBatches {
			return left.recentBatches > right.recentBatches
		}
		if left.smallBatches != right.smallBatches {
			return left.smallBatches > right.smallBatches
		}
		if left.oldestFailedAgeSec != right.oldestFailedAgeSec {
			return left.oldestFailedAgeSec > right.oldestFailedAgeSec
		}
		if left.reasonCount != right.reasonCount {
			return left.reasonCount > right.reasonCount
		}
		if left.reasonBytes != right.reasonBytes {
			return left.reasonBytes < right.reasonBytes
		}
		if left.reasonChunks != right.reasonChunks {
			return left.reasonChunks < right.reasonChunks
		}
		return left.volumeID < right.volumeID
	})
}

func maxInt64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

func retryFairnessBucket(job maintenanceJob) int {
	if job.retryWindows == 0 {
		return 1
	}
	switch {
	case job.retryWindowBytes <= 64*1024 && job.retryWindowChunks <= 32:
		return 0
	case job.retryWindowBytes <= 1024*1024 && job.retryWindowChunks <= 256:
		return 2
	default:
		return 3
	}
}

func (s *server) reconcileMutationScopes(ctx context.Context, volumeID string, spec volumeSpecRecord) error {
	if spec.ExtentPageBytes == 0 || spec.ChunkSizeBytes == 0 {
		return nil
	}
	mutations, err := s.repo.ListMutationOperations(ctx, volumeID)
	if err != nil {
		return err
	}
	resolve := clustermeta.NewService(s.repo)
	for _, mutation := range mutations {
		switch mutation.Kind {
		case "write", "transition", "payload_gc", "payload_gc_batch":
		default:
			continue
		}
		if len(mutation.AffectedExtentIDs) > 0 && len(mutation.AffectedPageNos) > 0 {
			continue
		}
		reconciled, err := resolve.ReconcileMutationOperationScope(ctx, volumeID, mutation, spec.ExtentPageBytes, spec.ChunkSizeBytes)
		if err != nil {
			return err
		}
		if !reconciled.Changed {
			continue
		}
		if err := s.repo.PutMutationOperation(ctx, reconciled.Operation); err != nil {
			return err
		}
	}
	return nil
}

func (s *server) runRetiredPayloadBacklogSweep(ctx context.Context, settings maintenanceSnapshot, volumes []clustermeta.VolumeState) {
	payloadRoot := strings.TrimSpace(s.payloadRoot)
	if payloadRoot == "" || len(volumes) == 0 || settings.pausePayloadGCs {
		return
	}
	candidates := s.buildRetiredPayloadSweepCandidates(ctx, volumes)
	if len(candidates) == 0 {
		return
	}
	collector, closeStores, err := s.openLocalPayloadGarbageCollector(ctx, payloadRoot)
	if err != nil {
		log.Printf("sbs-service payload-gc collector: %v", err)
		return
	}
	defer closeStores()
	sem := make(chan struct{}, settings.maxConcurrentPayloadGCs)
	var wg sync.WaitGroup
	for _, candidate := range candidates {
		candidate := candidate
		wg.Add(1)
		go func() {
			defer wg.Done()
			select {
			case <-ctx.Done():
				return
			case sem <- struct{}{}:
			}
			defer func() { <-sem }()
			if _, err := collector.SweepVolume(ctx, candidate.VolumeID); err != nil && ctx.Err() == nil {
				log.Printf("sbs-service payload-gc volume=%s chunks=%d bytes=%d failed_batches=%d: %v", candidate.VolumeID, candidate.Backlog.Chunks, candidate.Backlog.Bytes, candidate.Backlog.FailedBatches, err)
			}
		}()
	}
	wg.Wait()
}

type retiredPayloadSweepCandidate struct {
	VolumeID string
	Backlog  retiredPayloadBacklog
}

func (s *server) buildRetiredPayloadSweepCandidates(ctx context.Context, volumes []clustermeta.VolumeState) []retiredPayloadSweepCandidate {
	candidates := make([]retiredPayloadSweepCandidate, 0, len(volumes))
	for _, volume := range volumes {
		spec, _ := s.getVolumeSpec(ctx, volume.VolumeID)
		backlog := s.summarizeRetiredPayloadBacklog(ctx, volume.VolumeID, spec.ChunkSizeBytes)
		if backlog.Chunks == 0 && backlog.FailedBatches == 0 {
			continue
		}
		candidates = append(candidates, retiredPayloadSweepCandidate{
			VolumeID: volume.VolumeID,
			Backlog:  backlog,
		})
	}
	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].Backlog.FailedBatches != candidates[j].Backlog.FailedBatches {
			return candidates[i].Backlog.FailedBatches > candidates[j].Backlog.FailedBatches
		}
		if candidates[i].Backlog.OldestFailedAgeSec != candidates[j].Backlog.OldestFailedAgeSec {
			return candidates[i].Backlog.OldestFailedAgeSec > candidates[j].Backlog.OldestFailedAgeSec
		}
		if candidates[i].Backlog.Chunks != candidates[j].Backlog.Chunks {
			return candidates[i].Backlog.Chunks > candidates[j].Backlog.Chunks
		}
		if candidates[i].Backlog.Bytes != candidates[j].Backlog.Bytes {
			return candidates[i].Backlog.Bytes > candidates[j].Backlog.Bytes
		}
		return candidates[i].VolumeID < candidates[j].VolumeID
	})
	return candidates
}

func (s *server) cleanupCompletedTransitions(ctx context.Context, volumeID string) error {
	transitions, err := s.repo.ListPlacementTransitions(ctx, volumeID)
	if err != nil {
		return err
	}
	mappings, err := s.repo.ListExtentMappings(ctx, volumeID)
	if err != nil && !errors.Is(err, clustermeta.ErrNotFound) {
		return err
	}
	activePlacements := make(map[string]struct{}, len(mappings))
	for _, mapping := range mappings {
		activePlacements[mapping.PlacementRef] = struct{}{}
	}
	for _, tr := range transitions {
		if tr.State != clustermeta.PlacementTransitionCompleted {
			continue
		}
		if currentReplicaSet, err := s.repo.GetReplicaSet(ctx, volumeID, tr.CurrentReplicaSetID); err == nil {
			if _, stillReferenced := activePlacements[currentReplicaSet.PlacementRef]; !stillReferenced {
				_ = s.repo.DeleteReplicaSet(ctx, volumeID, currentReplicaSet.ReplicaSetID)
			}
		}
		if err := s.repo.DeletePlacementTransition(ctx, volumeID, tr.PlacementRef); err != nil {
			return err
		}
	}
	return nil
}

func (s *server) enqueueVolumeRebalance(ctx context.Context, volumeID string) (bool, error) {
	if skip, err := s.skipFreshVolumeAutoRebalance(ctx, volumeID); err != nil {
		return false, err
	} else if skip {
		return false, nil
	}
	if blocked, err := s.volumeHasForegroundWriteBlockingAutoRebalance(ctx, volumeID); err != nil {
		return false, err
	} else if blocked {
		return false, nil
	}
	mappings, err := s.repo.ListExtentMappings(ctx, volumeID)
	if err != nil {
		if errors.Is(err, clustermeta.ErrNotFound) {
			return false, nil
		}
		return false, err
	}
	if len(mappings) == 0 {
		return false, nil
	}
	replicaSets, err := s.repo.ListReplicaSets(ctx, volumeID)
	if err != nil {
		return false, err
	}
	nodes, err := s.repo.ListNodeMemberships(ctx)
	if err != nil {
		return false, err
	}
	byPlacement := make(map[string]clustermeta.ReplicaSetState, len(replicaSets))
	for _, rs := range replicaSets {
		byPlacement[rs.PlacementRef] = rs
	}
	nodeLoad := make(map[string]int)
	for _, mapping := range mappings {
		rs, ok := byPlacement[mapping.PlacementRef]
		if !ok {
			continue
		}
		for _, replica := range rs.Replicas {
			nodeLoad[replica.NodeID]++
		}
	}
	unusedCandidates := make([]clustermeta.NodeMembershipRecord, 0)
	for _, node := range nodes {
		if !s.nodeEligibleForNewPlacement(ctx, node) {
			continue
		}
		if nodeLoad[node.NodeID] == 0 {
			unusedCandidates = append(unusedCandidates, node)
		}
	}
	if len(unusedCandidates) == 0 {
		return false, nil
	}
	maintSvc := s.newMaintenanceService()
	type rebalanceCandidate struct {
		mapping   clustermeta.ExtentMappingRecord
		evaluated *clustermaintenance.EvaluatedExtent
	}
	candidates := make([]rebalanceCandidate, 0, len(mappings))
	for _, mapping := range mappings {
		evaluated, evalErr := maintSvc.EvaluateExtentHealth(ctx, volumeID, mapping.ExtentID)
		if evalErr != nil {
			continue
		}
		candidates = append(candidates, rebalanceCandidate{mapping: mapping, evaluated: evaluated})
	}
	sort.SliceStable(candidates, func(i, j int) bool {
		return clustermaintenance.CompareEvaluatedExtentPriority(candidates[i].evaluated, candidates[j].evaluated) < 0
	})
	for _, candidate := range candidates {
		mapping := candidate.mapping
		if _, err := s.repo.GetPlacementTransition(ctx, volumeID, mapping.PlacementRef); err == nil {
			continue
		} else if !errors.Is(err, clustermeta.ErrNotFound) {
			return false, err
		}
		currentReplicaSet, ok := byPlacement[mapping.PlacementRef]
		if !ok {
			continue
		}
		sourceNodeID := rebalanceSourceNode(currentReplicaSet, nodeLoad)
		// Auto rebalance smooths volume-local hot spots. A single placement
		// whose replica nodes are each used once is already balanced within
		// that volume; moving it only because the cluster has an idle node
		// creates maintenance churn for short-lived baseline volumes.
		if sourceNodeID == "" || nodeLoad[sourceNodeID] < 2 {
			continue
		}
		targetReplicaSet, ok, err := s.planReplacementReplicaSet(ctx, volumeID, mapping, currentReplicaSet, sourceNodeID, "rebalance", candidate.evaluated)
		if err != nil {
			return false, err
		}
		if !ok {
			continue
		}
		if err := s.repo.PutReplicaSet(ctx, targetReplicaSet); err != nil {
			return false, err
		}
		if _, err := maintSvc.EnqueueRebalance(ctx, volumeID, mapping.ExtentID, targetReplicaSet.ReplicaSetID); err != nil {
			return false, err
		}
		return true, nil
	}
	return false, nil
}

func (s *server) skipFreshVolumeAutoRebalance(ctx context.Context, volumeID string) (bool, error) {
	minAge := s.autoRebalanceMinVolumeLifetime()
	if minAge <= 0 {
		return false, nil
	}
	spec, err := s.getVolumeSpec(ctx, volumeID)
	if err != nil {
		if errors.Is(err, clustermeta.ErrNotFound) {
			return false, nil
		}
		return false, err
	}
	if spec.CreatedAtUnix <= 0 {
		return false, nil
	}
	age := s.currentTime().Sub(time.Unix(spec.CreatedAtUnix, 0))
	return age >= 0 && age < minAge, nil
}

func (s *server) volumeHasForegroundWriteBlockingAutoRebalance(ctx context.Context, volumeID string) (bool, error) {
	operations, err := s.repo.ListMutationOperations(ctx, volumeID)
	if err != nil {
		if errors.Is(err, clustermeta.ErrNotFound) {
			return false, nil
		}
		return false, err
	}
	for _, operation := range operations {
		if operation.Kind != "write" {
			continue
		}
		switch operation.State {
		case clustermeta.MutationOperationPending, clustermeta.MutationOperationRunning:
			return true, nil
		case clustermeta.MutationOperationCommitted:
			settle := s.autoRebalanceForegroundWriteSettleLifetime()
			if settle <= 0 || operation.LastUpdatedAtUnix <= 0 {
				continue
			}
			age := s.currentTime().Sub(time.Unix(operation.LastUpdatedAtUnix, 0))
			if age < settle {
				return true, nil
			}
		}
	}
	return false, nil
}

func rebalanceSourceNode(replicaSet clustermeta.ReplicaSetState, nodeLoad map[string]int) string {
	bestNodeID := ""
	bestLoad := -1
	for _, replica := range replicaSet.Replicas {
		load := nodeLoad[replica.NodeID]
		if load > bestLoad {
			bestLoad = load
			bestNodeID = replica.NodeID
		}
	}
	return bestNodeID
}

func dominantTransitionReason(transitions []clustermeta.PlacementTransitionRecord) string {
	for _, transition := range transitions {
		if transition.State != clustermeta.PlacementTransitionQueued && transition.State != clustermeta.PlacementTransitionRunning {
			continue
		}
		switch transition.Reason {
		case "drain":
			return "drain"
		case "rebalance":
			return "rebalance"
		case "repair":
			return "repair"
		}
	}
	return ""
}

func (s *server) buildReplicaClientMap(ctx context.Context, volumeID string) (map[string]service.SBSClient, error) {
	replicaSets, err := s.repo.ListReplicaSets(ctx, volumeID)
	if err != nil {
		return nil, err
	}
	volumeSpec, specErr := s.getVolumeSpec(ctx, volumeID)
	if specErr != nil && !errors.Is(specErr, clustermeta.ErrNotFound) {
		return nil, specErr
	}
	clients := make(map[string]service.SBSClient)
	nodes, err := s.repo.ListNodeMemberships(ctx)
	if err != nil {
		return nil, err
	}
	for _, node := range nodes {
		if node.LifecycleState != clustermeta.NodeLifecycleActive {
			continue
		}
		if node.HealthState != clustermeta.NodeHealthHealthy && node.HealthState != clustermeta.NodeHealthSuspect {
			continue
		}
		endpoint := endpointString(node.SBSEndpoints)
		if endpoint == "" {
			continue
		}
		client, err := s.cache.Get(endpoint)
		if err != nil {
			return nil, err
		}
		if specErr == nil {
			client = newMaterializingSBSClient(client, nodeAdminHTTPEndpoint(node), volumeSpec)
		}
		clients[node.NodeID] = client
	}
	for _, replicaSet := range replicaSets {
		for _, replica := range replicaSet.Replicas {
			node, err := s.repo.GetNodeMembership(ctx, replica.NodeID)
			if err != nil {
				return nil, err
			}
			endpoint := endpointString(node.SBSEndpoints)
			if endpoint == "" {
				return nil, fmt.Errorf("node %s has no sbs endpoint", node.NodeID)
			}
			client, err := s.cache.Get(endpoint)
			if err != nil {
				return nil, err
			}
			if specErr == nil {
				client = newMaterializingSBSClient(client, nodeAdminHTTPEndpoint(node), volumeSpec)
			}
			clients[replica.ReplicaID] = client
		}
	}
	return clients, nil
}

type materializingSBSClient struct {
	next           service.SBSClient
	adminHTTP      string
	volumeSpec     volumeSpecRecord
	materializedMu sync.Mutex
	materialized   bool
}

func newMaterializingSBSClient(next service.SBSClient, adminHTTP string, volumeSpec volumeSpecRecord) service.SBSClient {
	if next == nil || adminHTTP == "" || volumeSpec.VolumeID == "" {
		return next
	}
	return &materializingSBSClient{next: next, adminHTTP: strings.TrimRight(adminHTTP, "/"), volumeSpec: volumeSpec}
}

func (c *materializingSBSClient) OpenVolume(ctx context.Context, req *service.OpenVolumeRequest) (*service.OpenVolumeResponse, error) {
	resp, err := c.next.OpenVolume(ctx, req)
	if err == nil || !isSBSNotFoundError(err) {
		return resp, err
	}
	if materializeErr := c.materialize(ctx); materializeErr != nil {
		return nil, fmt.Errorf("%w; materialize target volume: %v", err, materializeErr)
	}
	return c.next.OpenVolume(ctx, req)
}

func (c *materializingSBSClient) materialize(ctx context.Context) error {
	c.materializedMu.Lock()
	defer c.materializedMu.Unlock()
	if c.materialized {
		return nil
	}
	q := url.Values{}
	q.Set("volume_id", c.volumeSpec.VolumeID)
	q.Set("size_bytes", strconv.FormatUint(c.volumeSpec.SizeBytes, 10))
	q.Set("block_size", strconv.FormatUint(uint64(c.volumeSpec.BlockSize), 10))
	q.Set("prefix", "sbs-"+c.volumeSpec.VolumeID)
	if c.volumeSpec.ChunkSizeBytes != 0 {
		q.Set("allocation_chunk_size_bytes", strconv.FormatUint(uint64(c.volumeSpec.ChunkSizeBytes), 10))
	}
	if c.volumeSpec.ExtentPageBytes != 0 {
		q.Set("allocation_page_bytes", strconv.FormatUint(uint64(c.volumeSpec.ExtentPageBytes), 10))
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.adminHTTP+"/debug/materialize-volume?"+q.Encode(), nil)
	if err != nil {
		return err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("unexpected status %s", resp.Status)
	}
	c.materialized = true
	return nil
}

func (c *materializingSBSClient) CloseVolume(ctx context.Context, req *service.CloseVolumeRequest) (*service.CloseVolumeResponse, error) {
	return c.next.CloseVolume(ctx, req)
}

func (c *materializingSBSClient) GetVolumeProfile(ctx context.Context, req *service.GetVolumeProfileRequest) (*service.GetVolumeProfileResponse, error) {
	return c.next.GetVolumeProfile(ctx, req)
}

func (c *materializingSBSClient) GetVolumeStatus(ctx context.Context, req *service.GetVolumeStatusRequest) (*service.GetVolumeStatusResponse, error) {
	return c.next.GetVolumeStatus(ctx, req)
}

func (c *materializingSBSClient) Read(ctx context.Context, req *service.ReadRequest) (*service.ReadResponse, error) {
	return c.next.Read(ctx, req)
}

func (c *materializingSBSClient) Write(ctx context.Context, req *service.WriteRequest) (*service.WriteResponse, error) {
	return c.next.Write(ctx, req)
}

func (c *materializingSBSClient) ReadPhysicalChunk(ctx context.Context, req *service.ReadPhysicalChunkRequest) (*service.ReadPhysicalChunkResponse, error) {
	next, ok := c.next.(service.PhysicalChunkSBSClient)
	if !ok {
		return nil, service.ErrNotSupported
	}
	resp, err := next.ReadPhysicalChunk(ctx, req)
	if err == nil || !isSBSNotFoundError(err) {
		return resp, err
	}
	if materializeErr := c.materialize(ctx); materializeErr != nil {
		return nil, fmt.Errorf("%w; materialize target volume: %v", err, materializeErr)
	}
	return next.ReadPhysicalChunk(ctx, req)
}

func (c *materializingSBSClient) WritePhysicalChunk(ctx context.Context, req *service.WritePhysicalChunkRequest) (*service.WritePhysicalChunkResponse, error) {
	next, ok := c.next.(service.PhysicalChunkSBSClient)
	if !ok {
		return nil, service.ErrNotSupported
	}
	return next.WritePhysicalChunk(ctx, req)
}

func (c *materializingSBSClient) WriteECShard(ctx context.Context, req *service.WriteECShardRequest) (*service.WriteECShardResponse, error) {
	next, ok := c.next.(service.ECShardSBSClient)
	if !ok {
		return nil, service.ErrNotSupported
	}
	resp, err := next.WriteECShard(ctx, req)
	if err == nil || !isSBSNotFoundError(err) {
		return resp, err
	}
	if materializeErr := c.materialize(ctx); materializeErr != nil {
		return nil, fmt.Errorf("%w; materialize target volume: %v", err, materializeErr)
	}
	return next.WriteECShard(ctx, req)
}

func (c *materializingSBSClient) ReadECShard(ctx context.Context, req *service.ReadECShardRequest) (*service.ReadECShardResponse, error) {
	next, ok := c.next.(service.ECShardSBSClient)
	if !ok {
		return nil, service.ErrNotSupported
	}
	resp, err := next.ReadECShard(ctx, req)
	if err == nil || !isSBSNotFoundError(err) {
		return resp, err
	}
	if materializeErr := c.materialize(ctx); materializeErr != nil {
		return nil, fmt.Errorf("%w; materialize target volume: %v", err, materializeErr)
	}
	return next.ReadECShard(ctx, req)
}

func (c *materializingSBSClient) DeleteECShard(ctx context.Context, req *service.DeleteECShardRequest) (*service.DeleteECShardResponse, error) {
	next, ok := c.next.(service.ECShardSBSClient)
	if !ok {
		return nil, service.ErrNotSupported
	}
	return next.DeleteECShard(ctx, req)
}

func (c *materializingSBSClient) Flush(ctx context.Context, req *service.FlushRequest) (*service.FlushResponse, error) {
	return c.next.Flush(ctx, req)
}

func (c *materializingSBSClient) Discard(ctx context.Context, req *service.DiscardRequest) (*service.DiscardResponse, error) {
	return c.next.Discard(ctx, req)
}

func (c *materializingSBSClient) Zero(ctx context.Context, req *service.ZeroRequest) (*service.ZeroResponse, error) {
	return c.next.Zero(ctx, req)
}

func isSBSNotFoundError(err error) bool {
	var sbsErr *service.SBSError
	return errors.As(err, &sbsErr) && sbsErr.Code == service.SBSErrorCodeNotFound
}

func nodeAdminHTTPEndpoint(node clustermeta.NodeMembershipRecord) string {
	if node.AdminHTTPEndpoint != "" {
		return node.AdminHTTPEndpoint
	}
	if len(node.SBSEndpoints) == 0 || node.SBSEndpoints[0].Address == "" {
		return ""
	}
	// Phase F lab default: sbs-data HTTP observability/materialization port.
	return fmt.Sprintf("http://%s:%d", node.SBSEndpoints[0].Address, 9082)
}

func (c *replicaClientCache) Get(endpoint string) (service.SBSClient, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if cached, ok := c.clients[endpoint]; ok {
		return cached.client, nil
	}
	conn, err := grpc.NewClient(endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	client := sbsgrpc.NewClient(sbsv1.NewVolumeServiceClient(conn))
	c.clients[endpoint] = cachedReplicaClient{conn: conn, client: client}
	return client, nil
}

func (c *replicaClientCache) GetISCSIWriterFenceClient(endpoint string) (service.ISCSIWriterFenceClient, error) {
	client, err := c.Get(endpoint)
	if err != nil {
		return nil, err
	}
	fenceClient, ok := client.(service.ISCSIWriterFenceClient)
	if !ok {
		return nil, fmt.Errorf("sbs-data endpoint %s does not implement iSCSI writer fencing", endpoint)
	}
	return fenceClient, nil
}

func (c *replicaClientCache) Close() {
	c.mu.Lock()
	defer c.mu.Unlock()
	for endpoint, cached := range c.clients {
		if cached.conn == nil {
			continue
		}
		if err := cached.conn.Close(); err != nil {
			log.Printf("close replica client %s: %v", endpoint, err)
		}
	}
	c.clients = make(map[string]cachedReplicaClient)
}

func getenvOrDefault(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func getenvCompatOrDefault(spec envcompat.Spec, fallback string) string {
	resolved, err := envcompat.ResolveCurrent(spec, os.LookupEnv)
	if err != nil {
		log.Fatalf("environment configuration: %v", err)
	}
	envcompat.WriteWarnings(os.Stderr, resolved.Warnings)
	if resolved.Present {
		return resolved.Value
	}
	return fallback
}

func getenvDuration(key string, fallback time.Duration) time.Duration {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	parsed, err := time.ParseDuration(value)
	if err != nil {
		return fallback
	}
	return parsed
}

func getenvUint64(key string, fallback uint64) uint64 {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	parsed, err := parseSizeEnvUint64(value)
	if err != nil {
		return fallback
	}
	return parsed
}

func parseSizeEnvUint64(raw string) (uint64, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return 0, fmt.Errorf("size is empty")
	}
	last := raw[len(raw)-1]
	var multiplier uint64 = 1
	switch last {
	case 'K', 'k':
		multiplier = 1 << 10
		raw = strings.TrimSpace(raw[:len(raw)-1])
	case 'M', 'm':
		multiplier = 1 << 20
		raw = strings.TrimSpace(raw[:len(raw)-1])
	case 'G', 'g':
		multiplier = 1 << 30
		raw = strings.TrimSpace(raw[:len(raw)-1])
	case 'T', 't':
		multiplier = 1 << 40
		raw = strings.TrimSpace(raw[:len(raw)-1])
	}
	if raw == "" {
		return 0, fmt.Errorf("size value is empty")
	}
	value, err := strconv.ParseUint(raw, 10, 64)
	if err != nil {
		return 0, err
	}
	if value > ^uint64(0)/multiplier {
		return 0, fmt.Errorf("size overflows uint64")
	}
	return value * multiplier, nil
}

func getenvInt(key string, fallback int) int {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return fallback
	}
	return parsed
}

func getenvBool(key string, fallback bool) bool {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	parsed, err := strconv.ParseBool(value)
	if err != nil {
		return fallback
	}
	return parsed
}
