package replication

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/nosway/namrbd/gateway/service"
	"github.com/nosway/namrbd/internal/structuredlog"
	"github.com/nosway/namrbd/sbs/cluster/metadata"
	namrbdversion "github.com/nosway/namrbd/version"
)

type RemoteReplica struct {
	ReplicaID    string
	Client       service.SBSClient
	VolumeID     string
	VolumeHandle string
	GatewayID    string
	HostID       string
	SessionID    string
	AttachmentID string
	Generation   uint64
}

type OpenReplicaSessionsRequest struct {
	VolumeID      string
	GatewayID     string
	HostID        string
	ClientVersion string
	AttachmentID  string
	Generation    uint64
	SessionPrefix string
	AccessMode    service.SBSAccessMode
	AllowNotFound bool
}

func OpenReplicaSessions(ctx context.Context, clients map[string]service.SBSClient, req OpenReplicaSessionsRequest) (map[string]RemoteReplica, error) {
	if req.VolumeID == "" {
		return nil, fmt.Errorf("volume id is required")
	}
	if req.GatewayID == "" {
		return nil, fmt.Errorf("gateway id is required")
	}
	if req.AttachmentID == "" {
		return nil, fmt.Errorf("attachment id is required")
	}
	if req.Generation == 0 {
		return nil, fmt.Errorf("generation must be >= 1")
	}
	if req.AccessMode == "" {
		req.AccessMode = service.SBSAccessModeExclusiveWriter
	}
	if req.SessionPrefix == "" {
		req.SessionPrefix = "cluster"
	}
	if strings.TrimSpace(req.ClientVersion) == "" {
		req.ClientVersion = namrbdversion.ProductVersion()
	}

	replicaIDs := make([]string, 0, len(clients))
	for replicaID := range clients {
		replicaIDs = append(replicaIDs, replicaID)
	}
	sort.Strings(replicaIDs)

	out := make(map[string]RemoteReplica, len(replicaIDs))
	for _, replicaID := range replicaIDs {
		client := clients[replicaID]
		if client == nil {
			return nil, fmt.Errorf("replica %q has nil client", replicaID)
		}
		openResp, err := client.OpenVolume(ctx, &service.OpenVolumeRequest{
			VolumeID:   req.VolumeID,
			AccessMode: req.AccessMode,
			Context: service.SBSRequestContext{
				RequestID:    fmt.Sprintf("cluster-open-%s-%s", req.VolumeID, replicaID),
				GatewayID:    req.GatewayID,
				HostID:       req.HostID,
				SessionID:    fmt.Sprintf("%s-%s", req.SessionPrefix, replicaID),
				AttachmentID: req.AttachmentID,
				Generation:   req.Generation,
			},
		})
		if err != nil {
			if req.AllowNotFound && isSBSNotFound(err) {
				continue
			}
			return nil, fmt.Errorf("open replica %q: %w", replicaID, err)
		}
		if err := service.CheckSBSVersionCompatibility(req.ClientVersion, openResp.ServerVersion); err != nil {
			return nil, fmt.Errorf("open replica %q: %w", replicaID, err)
		}
		out[replicaID] = RemoteReplica{
			ReplicaID:    replicaID,
			Client:       client,
			VolumeID:     req.VolumeID,
			VolumeHandle: openResp.VolumeHandle,
			GatewayID:    req.GatewayID,
			HostID:       req.HostID,
			SessionID:    fmt.Sprintf("%s-%s", req.SessionPrefix, replicaID),
			AttachmentID: req.AttachmentID,
			Generation:   req.Generation,
		}
	}
	return out, nil
}

func isSBSNotFound(err error) bool {
	var sbsErr *service.SBSError
	return errors.As(err, &sbsErr) && sbsErr.Code == service.SBSErrorCodeNotFound
}

type RemoteReplicaWriter struct {
	replicas                         map[string]RemoteReplica
	requireAllWrites                 bool
	quorumEarlyReturn                bool
	quorumEarlyStagedFanoutDelay     time.Duration
	quorumEarlyBackgroundFanoutLimit int
	maxParallelChunkWrites           int
	encryption                       *localReplicaPayloadEncryptor
}

var quorumEarlyBackgroundFanoutLimiters sync.Map

func NewRemoteReplicaWriter(replicas map[string]RemoteReplica) *RemoteReplicaWriter {
	return newRemoteReplicaWriter(replicas, false)
}

func NewStrictRemoteReplicaWriter(replicas map[string]RemoteReplica) *RemoteReplicaWriter {
	return newRemoteReplicaWriter(replicas, true)
}

func NewEncryptedRemoteReplicaWriterForPhaseP(replicas map[string]RemoteReplica, cfg PhasePReplicaEncryptionConfig) *RemoteReplicaWriter {
	writer := NewRemoteReplicaWriter(replicas)
	writer.encryption = newLocalReplicaPayloadEncryptor(cfg)
	return writer
}

func newRemoteReplicaWriter(replicas map[string]RemoteReplica, requireAllWrites bool) *RemoteReplicaWriter {
	cloned := make(map[string]RemoteReplica, len(replicas))
	for replicaID, replica := range replicas {
		cloned[replicaID] = replica
	}
	return &RemoteReplicaWriter{replicas: cloned, requireAllWrites: requireAllWrites}
}

func (w *RemoteReplicaWriter) WithQuorumEarlyReturn(enabled bool) *RemoteReplicaWriter {
	w.quorumEarlyReturn = enabled
	return w
}

func (w *RemoteReplicaWriter) WithQuorumEarlyStagedFanoutDelay(delay time.Duration) *RemoteReplicaWriter {
	if delay < 0 {
		delay = 0
	}
	w.quorumEarlyStagedFanoutDelay = delay
	return w
}

func (w *RemoteReplicaWriter) WithQuorumEarlyBackgroundFanoutLimit(limit int) *RemoteReplicaWriter {
	if limit < 0 {
		limit = 0
	}
	w.quorumEarlyBackgroundFanoutLimit = limit
	return w
}

func (w *RemoteReplicaWriter) WithMaxParallelChunkWrites(max int) *RemoteReplicaWriter {
	if max < 0 {
		max = 0
	}
	w.maxParallelChunkWrites = max
	return w
}

func (w *RemoteReplicaWriter) WriteExtent(ctx context.Context, plan ExtentWritePlan, req ReplicaWriteRequest) (*ReplicaWriteResult, error) {
	start := time.Now()
	payload, err := payloadForExtent(plan, req)
	if err != nil {
		return nil, err
	}
	writeOffset, _, err := overlapRange(plan.Extent.LogicalOffset, plan.Extent.LengthBytes, req.OffsetBytes, req.LengthBytes)
	if err != nil {
		return nil, err
	}
	writeChunks, useChunkAware := resolvedWriteChunkMappings(plan, req)
	if plan.CopyOnWrite && !useChunkAware {
		return nil, fmt.Errorf("remote copy-on-write requires allocation chunk mappings")
	}
	if w.encryption != nil && !useChunkAware {
		return nil, fmt.Errorf("phase p encrypted remote replica writer requires allocation chunk mappings")
	}
	writeDataKeys := newPhasePReplicaDataKeyRequestCache(w.encryption)
	if w.quorumEarlyReturn && !w.requireAllWrites && plan.RequiredAcks > 0 && len(plan.WriteTargets) > int(plan.RequiredAcks) {
		return w.writeExtentQuorumEarly(ctx, plan, req, writeOffset, payload, writeChunks, useChunkAware, writeDataKeys)
	}
	results := make([]remoteReplicaWriteAttempt, len(plan.WriteTargets))
	var wg sync.WaitGroup
	wg.Add(len(plan.WriteTargets))
	for i, target := range plan.WriteTargets {
		go func(i int, target ReplicaTarget) {
			defer wg.Done()
			results[i] = w.writeTarget(ctx, target, plan, req, writeOffset, payload, writeChunks, useChunkAware, writeDataKeys)
		}(i, target)
	}
	wg.Wait()

	acked := make([]string, 0, len(plan.WriteTargets))
	var failures []string
	chunkEncryptionHeaders := make(map[uint64]ReplicaChunkEncryptionHeader)
	for _, result := range results {
		if result.acked {
			acked = append(acked, result.replicaID)
			for _, header := range result.chunkEncryptionHeaders {
				if err := recordReplicaChunkEncryptionHeader(chunkEncryptionHeaders, header.LogicalChunk, header.PhysicalChunkID, header.Header); err != nil {
					return nil, err
				}
			}
		}
		if result.failure != "" {
			failures = append(failures, result.failure)
		}
	}
	if len(failures) > 0 && (w.requireAllWrites || len(acked) == 0) {
		return nil, fmt.Errorf("replica writes failed acked=%d failures=%d: %s", len(acked), len(failures), strings.Join(failures, "; "))
	}
	stats := replicaWriteStatsFromAttempts(results, plan.RequiredAcks, primaryReplicaIDForPlan(plan), time.Since(start), false, 0, true)
	return &ReplicaWriteResult{AckedReplicaIDs: acked, FailureMessages: failures, ChunkEncryptionHeaders: sortedReplicaChunkEncryptionHeaders(chunkEncryptionHeaders), Stats: stats}, nil
}

func (w *RemoteReplicaWriter) writeExtentQuorumEarly(ctx context.Context, plan ExtentWritePlan, req ReplicaWriteRequest, writeOffset uint64, payload []byte, writeChunks []resolvedChunkWrite, useChunkAware bool, writeDataKeys phasePReplicaDataKeySource) (*ReplicaWriteResult, error) {
	start := time.Now()
	resultCh := make(chan remoteReplicaWriteAttempt, len(plan.WriteTargets))
	initialTargets, delayedTargets := quorumEarlyStagedFanoutTargets(plan, w.quorumEarlyStagedFanoutDelay)
	delayedStarted := len(delayedTargets) == 0
	pending := 0
	startTarget := func(target ReplicaTarget, backgroundLimited bool) {
		pending++
		go func(target ReplicaTarget) {
			writeCtx, cancel := detachedWriteContext(ctx)
			defer cancel()
			if backgroundLimited {
				release, ok := acquireQuorumEarlyBackgroundFanoutSlot(writeCtx, w.quorumEarlyBackgroundFanoutLimit)
				if !ok {
					resultCh <- remoteReplicaWriteAttempt{replicaID: target.ReplicaID, failure: writeCtx.Err().Error()}
					return
				}
				defer release()
			}
			resultCh <- w.writeTarget(writeCtx, target, plan, req, writeOffset, payload, writeChunks, useChunkAware, writeDataKeys)
		}(target)
	}
	for _, target := range initialTargets {
		startTarget(target, false)
	}

	primaryReplicaID := primaryReplicaIDForPlan(plan)
	acked := make([]string, 0, len(plan.WriteTargets))
	ackedSet := make(map[string]struct{}, len(plan.WriteTargets))
	chunkEncryptionHeaders := make(map[uint64]ReplicaChunkEncryptionHeader)
	var failures []string
	var results []remoteReplicaWriteAttempt
	var stagedTimer *time.Timer
	if !delayedStarted && w.quorumEarlyStagedFanoutDelay > 0 {
		stagedTimer = time.NewTimer(w.quorumEarlyStagedFanoutDelay)
		defer stagedTimer.Stop()
	}
	startDelayed := func(reason string, backgroundLimited bool) {
		if delayedStarted {
			return
		}
		delayedStarted = true
		if stagedTimer != nil {
			stagedTimer.Stop()
		}
		for _, target := range delayedTargets {
			startTarget(target, backgroundLimited)
		}
		structuredlog.Info("sbs.replication", "replica_write_quorum_early_staged_fanout_released",
			structuredlog.F("request_id", req.RequestID),
			structuredlog.F("volume_id", req.VolumeID),
			structuredlog.F("extent_id", plan.Extent.ExtentID),
			structuredlog.F("reason", reason),
			structuredlog.F("staged_fanout_delay_ms", w.quorumEarlyStagedFanoutDelay.Milliseconds()),
			structuredlog.F("background_fanout_limit", w.quorumEarlyBackgroundFanoutLimit),
			structuredlog.F("background_limited", backgroundLimited),
			structuredlog.F("initial_replicas", replicaTargetIDs(initialTargets)),
			structuredlog.F("delayed_replicas", replicaTargetIDs(delayedTargets)),
		)
	}
	for pending > 0 || !delayedStarted {
		var timerC <-chan time.Time
		if stagedTimer != nil && !delayedStarted {
			timerC = stagedTimer.C
		}
		select {
		case result := <-resultCh:
			pending--
			results = append(results, result)
			if result.acked {
				if _, exists := ackedSet[result.replicaID]; !exists {
					ackedSet[result.replicaID] = struct{}{}
					acked = append(acked, result.replicaID)
				}
				for _, header := range result.chunkEncryptionHeaders {
					if err := recordReplicaChunkEncryptionHeader(chunkEncryptionHeaders, header.LogicalChunk, header.PhysicalChunkID, header.Header); err != nil {
						return nil, err
					}
				}
			}
			if result.failure != "" {
				failures = append(failures, result.failure)
				if uint32(len(acked))+uint32(pending) < plan.RequiredAcks {
					startDelayed("failure_before_quorum", false)
				}
			}
			primaryAckedAtQuorum := primaryAcked(ackedSet, primaryReplicaID)
			if uint32(len(acked)) >= plan.RequiredAcks {
				pendingAfterQuorum := pending
				if !delayedStarted {
					pendingAfterQuorum += len(delayedTargets)
				}
				structuredlog.Info("sbs.replication", "replica_write_quorum_early_return",
					structuredlog.F("request_id", req.RequestID),
					structuredlog.F("volume_id", req.VolumeID),
					structuredlog.F("extent_id", plan.Extent.ExtentID),
					structuredlog.F("acked_replicas", len(acked)),
					structuredlog.F("acked_replica_ids", acked),
					structuredlog.F("required_acks", plan.RequiredAcks),
					structuredlog.F("primary_replica_id", primaryReplicaID),
					structuredlog.F("primary_ack_required", false),
					structuredlog.F("primary_acked", primaryAckedAtQuorum),
					structuredlog.F("pending_replicas", pendingAfterQuorum),
					structuredlog.F("staged_fanout_delay_ms", w.quorumEarlyStagedFanoutDelay.Milliseconds()),
					structuredlog.F("background_fanout_limit", w.quorumEarlyBackgroundFanoutLimit),
					structuredlog.F("initial_replicas", replicaTargetIDs(initialTargets)),
					structuredlog.F("delayed_replicas", replicaTargetIDs(delayedTargets)),
					structuredlog.F("delayed_replicas_started", delayedStarted),
					structuredlog.F("quorum_ack_duration_ms", time.Since(start).Milliseconds()),
				)
				if !delayedStarted {
					startDelayed("after_quorum", true)
				}
				stats := replicaWriteStatsFromAttempts(results, plan.RequiredAcks, primaryReplicaIDForPlan(plan), 0, true, pendingAfterQuorum, false)
				return &ReplicaWriteResult{AckedReplicaIDs: acked, FailureMessages: failures, ChunkEncryptionHeaders: sortedReplicaChunkEncryptionHeaders(chunkEncryptionHeaders), Stats: stats, AllowNonPrimaryQuorum: true}, nil
			}
		case <-timerC:
			startDelayed("fallback_delay", false)
		}
	}
	if len(failures) > 0 && len(acked) == 0 {
		return nil, fmt.Errorf("replica writes failed acked=%d failures=%d: %s", len(acked), len(failures), strings.Join(failures, "; "))
	}
	stats := replicaWriteStatsFromAttempts(results, plan.RequiredAcks, primaryReplicaIDForPlan(plan), time.Since(start), false, 0, true)
	return &ReplicaWriteResult{AckedReplicaIDs: acked, FailureMessages: failures, ChunkEncryptionHeaders: sortedReplicaChunkEncryptionHeaders(chunkEncryptionHeaders), Stats: stats}, nil
}

func acquireQuorumEarlyBackgroundFanoutSlot(ctx context.Context, limit int) (func(), bool) {
	if limit <= 0 {
		return func() {}, true
	}
	value, _ := quorumEarlyBackgroundFanoutLimiters.LoadOrStore(limit, make(chan struct{}, limit))
	slots := value.(chan struct{})
	select {
	case slots <- struct{}{}:
		return func() { <-slots }, true
	case <-ctx.Done():
		return func() {}, false
	}
}

func detachedWriteContext(ctx context.Context) (context.Context, context.CancelFunc) {
	detached := context.WithoutCancel(ctx)
	if deadline, ok := ctx.Deadline(); ok {
		return context.WithDeadline(detached, deadline)
	}
	return context.WithCancel(detached)
}

func primaryReplicaIDForPlan(plan ExtentWritePlan) string {
	if plan.Primary.ReplicaID != "" {
		return plan.Primary.ReplicaID
	}
	if len(plan.WriteTargets) > 0 {
		return plan.WriteTargets[0].ReplicaID
	}
	return ""
}

func primaryAcked(acked map[string]struct{}, primaryReplicaID string) bool {
	if primaryReplicaID == "" {
		return len(acked) > 0
	}
	_, ok := acked[primaryReplicaID]
	return ok
}

func quorumEarlyStagedFanoutTargets(plan ExtentWritePlan, delay time.Duration) ([]ReplicaTarget, []ReplicaTarget) {
	if delay <= 0 || plan.RequiredAcks == 0 || len(plan.WriteTargets) <= int(plan.RequiredAcks) {
		return append([]ReplicaTarget(nil), plan.WriteTargets...), nil
	}
	required := int(plan.RequiredAcks)
	primaryID := primaryReplicaIDForPlan(plan)
	initialIndexes := make(map[int]struct{}, required)
	if primaryID != "" {
		for i, target := range plan.WriteTargets {
			if len(initialIndexes) >= required {
				break
			}
			if target.ReplicaID == primaryID {
				continue
			}
			initialIndexes[i] = struct{}{}
		}
	}
	for i := range plan.WriteTargets {
		if len(initialIndexes) >= required {
			break
		}
		initialIndexes[i] = struct{}{}
	}
	initial := make([]ReplicaTarget, 0, required)
	delayed := make([]ReplicaTarget, 0, len(plan.WriteTargets)-required)
	for i, target := range plan.WriteTargets {
		if _, ok := initialIndexes[i]; ok {
			initial = append(initial, target)
			continue
		}
		delayed = append(delayed, target)
	}
	return initial, delayed
}

func replicaTargetIDs(targets []ReplicaTarget) []string {
	out := make([]string, 0, len(targets))
	for _, target := range targets {
		if target.ReplicaID != "" {
			out = append(out, target.ReplicaID)
			continue
		}
		out = append(out, target.NodeID)
	}
	return out
}

type remoteReplicaWriteAttempt struct {
	replicaID              string
	acked                  bool
	failure                string
	duration               time.Duration
	chunkEncryptionHeaders []ReplicaChunkEncryptionHeader
}

func (w *RemoteReplicaWriter) writeTarget(ctx context.Context, target ReplicaTarget, plan ExtentWritePlan, req ReplicaWriteRequest, writeOffset uint64, payload []byte, writeChunks []resolvedChunkWrite, useChunkAware bool, writeDataKeys phasePReplicaDataKeySource) (result remoteReplicaWriteAttempt) {
	start := time.Now()
	result = remoteReplicaWriteAttempt{replicaID: target.ReplicaID}
	remoteReplicaID := ""
	replicaSessionID := ""
	defer func() {
		result.duration = time.Since(start)
		structuredlog.Info("sbs.replication", "replica_write_attempt_completed",
			structuredlog.F("request_id", req.RequestID),
			structuredlog.F("volume_id", req.VolumeID),
			structuredlog.F("replica_id", target.ReplicaID),
			structuredlog.F("target_replica_id", target.ReplicaID),
			structuredlog.F("node_id", target.NodeID),
			structuredlog.F("remote_replica_id", remoteReplicaID),
			structuredlog.F("replica_session_id", replicaSessionID),
			structuredlog.F("extent_id", plan.Extent.ExtentID),
			structuredlog.F("offset_bytes", writeOffset),
			structuredlog.F("length_bytes", uint64(len(payload))),
			structuredlog.F("logical_length_bytes", req.LengthBytes),
			structuredlog.F("payload_bytes", uint64(len(payload))),
			structuredlog.F("zero_semantic", req.ZeroSemantic),
			structuredlog.F("zero_semantic_payload_omitted", req.ZeroSemantic && req.LengthBytes > 0 && len(payload) == 0),
			structuredlog.F("chunk_aware", useChunkAware),
			structuredlog.F("acked", result.acked),
			structuredlog.F("failure", result.failure),
			structuredlog.F("duration_ms", result.duration.Milliseconds()),
		)
		if result.failure != "" {
			structuredlog.Error("sbs.replication", "replica_write_attempt_failed", errors.New(result.failure),
				structuredlog.F("request_id", req.RequestID),
				structuredlog.F("volume_id", req.VolumeID),
				structuredlog.F("replica_id", target.ReplicaID),
				structuredlog.F("node_id", target.NodeID),
				structuredlog.F("extent_id", plan.Extent.ExtentID),
				structuredlog.F("offset_bytes", writeOffset),
				structuredlog.F("length_bytes", uint64(len(payload))),
				structuredlog.F("logical_length_bytes", req.LengthBytes),
				structuredlog.F("payload_bytes", uint64(len(payload))),
				structuredlog.F("zero_semantic", req.ZeroSemantic),
				structuredlog.F("zero_semantic_payload_omitted", req.ZeroSemantic && req.LengthBytes > 0 && len(payload) == 0),
				structuredlog.F("chunk_aware", useChunkAware),
				structuredlog.F("duration_ms", result.duration.Milliseconds()),
			)
		}
	}()
	replica, ok := remoteReplicaForTarget(w.replicas, target)
	if !ok {
		result.failure = fmt.Sprintf("replica %q on node %q has no remote client", target.ReplicaID, target.NodeID)
		return result
	}
	remoteReplicaID = replica.ReplicaID
	replicaSessionID = replica.SessionID
	if ok, chunkEncryptionHeaders, err := w.writeViaChunkAwareRPC(ctx, replica, target, plan, req, writeOffset, payload, writeChunks, useChunkAware, writeDataKeys); ok {
		if err != nil {
			result.failure = fmt.Sprintf("replica %q on node %q: %v", target.ReplicaID, target.NodeID, err)
			return result
		}
		result.chunkEncryptionHeaders = chunkEncryptionHeaders
		result.acked = true
		return result
	}
	if w.encryption != nil {
		result.failure = fmt.Sprintf("replica %q on node %q: phase p encrypted remote replica writer requires physical chunk write RPC", target.ReplicaID, target.NodeID)
		return result
	}
	rpcStart := time.Now()
	_, err := replica.Client.Write(ctx, &service.WriteRequest{
		VolumeID:     replica.VolumeID,
		VolumeHandle: replica.VolumeHandle,
		OffsetBytes:  writeOffset,
		LengthBytes:  uint64(len(payload)),
		Data:         payload,
		Context: service.SBSRequestContext{
			RequestID:      fmt.Sprintf("%s-ext-%020d-%s", req.RequestID, plan.Extent.ExtentID, target.ReplicaID),
			GatewayID:      replica.GatewayID,
			HostID:         replica.HostID,
			SessionID:      replica.SessionID,
			AttachmentID:   replica.AttachmentID,
			Generation:     replica.Generation,
			IdempotencyKey: fmt.Sprintf("%s-ext-%020d", req.IdempotencyKey, plan.Extent.ExtentID),
		},
	})
	rpcDuration := time.Since(rpcStart)
	if err != nil {
		structuredlog.Error("sbs.replication", "replica_write_failed", err,
			structuredlog.F("request_id", req.RequestID),
			structuredlog.F("volume_id", req.VolumeID),
			structuredlog.F("replica_id", target.ReplicaID),
			structuredlog.F("node_id", target.NodeID),
			structuredlog.F("extent_id", plan.Extent.ExtentID),
			structuredlog.F("offset_bytes", writeOffset),
			structuredlog.F("length_bytes", uint64(len(payload))),
			structuredlog.F("duration_ms", rpcDuration.Milliseconds()),
		)
		result.failure = fmt.Sprintf("replica %q on node %q: %v", target.ReplicaID, target.NodeID, err)
		return result
	}
	structuredlog.Info("sbs.replication", "replica_write_completed",
		structuredlog.F("request_id", req.RequestID),
		structuredlog.F("volume_id", req.VolumeID),
		structuredlog.F("replica_id", target.ReplicaID),
		structuredlog.F("node_id", target.NodeID),
		structuredlog.F("extent_id", plan.Extent.ExtentID),
		structuredlog.F("offset_bytes", writeOffset),
		structuredlog.F("length_bytes", uint64(len(payload))),
		structuredlog.F("duration_ms", rpcDuration.Milliseconds()),
	)
	result.acked = true
	return result
}

func replicaWriteStatsFromAttempts(attempts []remoteReplicaWriteAttempt, requiredAcks uint32, primaryReplicaID string, allDuration time.Duration, quorumEarlyReturn bool, pendingReplicas int, requirePrimaryAck bool) ReplicaWriteStats {
	stats := ReplicaWriteStats{
		AllAckDuration:     allDuration,
		PerReplicaDuration: make(map[string]time.Duration, len(attempts)),
		QuorumEarlyReturn:  quorumEarlyReturn,
		PendingReplicas:    pendingReplicas,
		PrimaryAckRequired: requirePrimaryAck,
	}
	ackedDurations := make([]time.Duration, 0, len(attempts))
	var primaryAckDuration time.Duration
	for _, attempt := range attempts {
		if attempt.replicaID != "" && attempt.duration > 0 {
			stats.PerReplicaDuration[attempt.replicaID] = attempt.duration
		}
		if attempt.duration > stats.SlowestDuration {
			stats.SlowestDuration = attempt.duration
			stats.SlowestReplicaID = attempt.replicaID
		}
		if attempt.acked {
			ackedDurations = append(ackedDurations, attempt.duration)
			if attempt.replicaID == primaryReplicaID {
				primaryAckDuration = attempt.duration
				stats.PrimaryAcked = true
			}
		}
	}
	if len(stats.PerReplicaDuration) == 0 {
		stats.PerReplicaDuration = nil
	}
	sort.Slice(ackedDurations, func(i, j int) bool {
		return ackedDurations[i] < ackedDurations[j]
	})
	if len(ackedDurations) > 0 {
		stats.FirstAckDuration = ackedDurations[0]
		required := int(requiredAcks)
		if required <= 0 {
			required = 1
		}
		if len(ackedDurations) >= required {
			stats.QuorumAckDuration = ackedDurations[required-1]
		} else {
			stats.QuorumAckDuration = ackedDurations[len(ackedDurations)-1]
		}
		if requirePrimaryAck && primaryAckDuration > stats.QuorumAckDuration {
			stats.QuorumAckDuration = primaryAckDuration
		}
	}
	return stats
}

func (w *RemoteReplicaWriter) writeViaChunkAwareRPC(ctx context.Context, replica RemoteReplica, target ReplicaTarget, plan ExtentWritePlan, req ReplicaWriteRequest, writeOffset uint64, payload []byte, chunks []resolvedChunkWrite, useChunkAware bool, writeDataKeys phasePReplicaDataKeySource) (bool, []ReplicaChunkEncryptionHeader, error) {
	if !useChunkAware {
		return false, nil, nil
	}
	if len(chunks) == 0 {
		return true, nil, nil
	}
	physicalClient, ok := replica.Client.(service.PhysicalChunkSBSClient)
	if !ok {
		return true, nil, fmt.Errorf("replica %q does not support physical chunk write RPC", replica.ReplicaID)
	}
	chunkSize := uint64(plan.ChunkSizeBytes)
	if chunkSize == 0 {
		return true, nil, fmt.Errorf("chunk-aware write requires non-zero chunk size")
	}
	if w.encryption == nil && !plan.CopyOnWrite {
		if handled, err := w.writeFullChunkAwareRPCsInParallel(ctx, physicalClient, replica, target, plan, req, writeOffset, payload, chunks, chunkSize); handled {
			return true, nil, err
		}
	}
	writeEnd := writeOffset + uint64(len(payload))
	chunkEncryptionHeaders := make(map[uint64]ReplicaChunkEncryptionHeader)
	for _, chunk := range chunks {
		chunkStart := chunk.LogicalChunk * chunkSize
		chunkEnd := chunkStart + chunkSize
		copyStart := maxUint64(writeOffset, chunkStart)
		copyEnd := minUint64(writeEnd, chunkEnd)
		if copyStart >= copyEnd {
			continue
		}
		chunkOffset := copyStart - chunkStart
		payloadOffset := copyStart - writeOffset
		copyLen := copyEnd - copyStart
		chunkPayload := payload[payloadOffset : payloadOffset+copyLen]
		physicalWriteOffset := chunkOffset
		physicalWriteLength := copyLen
		physicalWriteData := chunkPayload
		basePhysicalChunkID := chunk.PhysicalChunkID
		baseEncryption := chunk.Encryption
		if chunk.BasePhysicalChunkID != 0 && chunk.BasePhysicalChunkID != chunk.PhysicalChunkID {
			basePhysicalChunkID = chunk.BasePhysicalChunkID
			baseEncryption = chunk.BaseEncryption
		}
		if w.encryption != nil || chunkOffset != 0 || copyLen != chunkSize {
			merged, err := w.loadRemoteChunkForMerge(ctx, physicalClient, replica, plan, req, chunk.LogicalChunk, basePhysicalChunkID, chunkSize, baseEncryption, copyStart == chunkStart && copyEnd == chunkEnd)
			if err != nil {
				return true, nil, err
			}
			copy(merged[chunkOffset:chunkOffset+copyLen], chunkPayload)
			physicalWriteOffset = 0
			physicalWriteLength = chunkSize
			physicalWriteData = merged
		}
		if w.encryption != nil {
			stored, encryptionHeader, err := w.encryption.encryptFixedChunkWithKeySource(ctx, req.VolumeID, chunk.LogicalChunk, chunk.PhysicalChunkID, chunkSize, physicalWriteData, writeDataKeys)
			if err != nil {
				return true, nil, err
			}
			if err := recordReplicaChunkEncryptionHeader(chunkEncryptionHeaders, chunk.LogicalChunk, chunk.PhysicalChunkID, encryptionHeader); err != nil {
				return true, nil, err
			}
			physicalWriteOffset = 0
			physicalWriteLength = chunkSize
			physicalWriteData = stored
		}
		physicalRequestID := physicalChunkWriteRequestID(req.RequestID, plan.Extent.ExtentID, replica.ReplicaID, chunk.LogicalChunk)
		rpcStart := time.Now()
		_, err := physicalClient.WritePhysicalChunk(ctx, &service.WritePhysicalChunkRequest{
			VolumeID:         replica.VolumeID,
			VolumeHandle:     replica.VolumeHandle,
			PhysicalChunkID:  chunk.PhysicalChunkID,
			ChunkOffsetBytes: physicalWriteOffset,
			LengthBytes:      physicalWriteLength,
			Data:             physicalWriteData,
			Context: service.SBSRequestContext{
				RequestID:      physicalRequestID,
				GatewayID:      replica.GatewayID,
				HostID:         replica.HostID,
				SessionID:      replica.SessionID,
				AttachmentID:   replica.AttachmentID,
				Generation:     replica.Generation,
				IdempotencyKey: fmt.Sprintf("%s-ext-%020d-chunk-%020d-phys-%020d", req.IdempotencyKey, plan.Extent.ExtentID, chunk.LogicalChunk, chunk.PhysicalChunkID),
			},
		})
		rpcDuration := time.Since(rpcStart)
		if err != nil {
			structuredlog.Error("sbs.replication", "replica_chunk_write_failed", err,
				structuredlog.F("request_id", req.RequestID),
				structuredlog.F("physical_request_id", physicalRequestID),
				structuredlog.F("volume_id", req.VolumeID),
				structuredlog.F("replica_id", replica.ReplicaID),
				structuredlog.F("target_replica_id", target.ReplicaID),
				structuredlog.F("node_id", target.NodeID),
				structuredlog.F("replica_session_id", replica.SessionID),
				structuredlog.F("extent_id", plan.Extent.ExtentID),
				structuredlog.F("logical_chunk", chunk.LogicalChunk),
				structuredlog.F("logical_chunk_count", 1),
				structuredlog.F("physical_chunk_id", chunk.PhysicalChunkID),
				structuredlog.F("physical_chunk_count", 1),
				structuredlog.F("coalesced_chunks", 1),
				structuredlog.F("offset_bytes", copyStart),
				structuredlog.F("physical_chunk_offset_bytes", physicalWriteOffset),
				structuredlog.F("length_bytes", copyLen),
				structuredlog.F("duration_ms", rpcDuration.Milliseconds()),
			)
			return true, nil, err
		}
		structuredlog.Info("sbs.replication", "replica_chunk_write_completed",
			structuredlog.F("request_id", req.RequestID),
			structuredlog.F("physical_request_id", physicalRequestID),
			structuredlog.F("volume_id", req.VolumeID),
			structuredlog.F("replica_id", replica.ReplicaID),
			structuredlog.F("target_replica_id", target.ReplicaID),
			structuredlog.F("node_id", target.NodeID),
			structuredlog.F("replica_session_id", replica.SessionID),
			structuredlog.F("extent_id", plan.Extent.ExtentID),
			structuredlog.F("logical_chunk", chunk.LogicalChunk),
			structuredlog.F("logical_chunk_count", 1),
			structuredlog.F("physical_chunk_id", chunk.PhysicalChunkID),
			structuredlog.F("physical_chunk_count", 1),
			structuredlog.F("coalesced_chunks", 1),
			structuredlog.F("offset_bytes", copyStart),
			structuredlog.F("physical_chunk_offset_bytes", physicalWriteOffset),
			structuredlog.F("length_bytes", copyLen),
			structuredlog.F("duration_ms", rpcDuration.Milliseconds()),
		)
	}
	return true, sortedReplicaChunkEncryptionHeaders(chunkEncryptionHeaders), nil
}

type physicalChunkWriteTask struct {
	LogicalChunk     uint64
	PhysicalChunkID  uint64
	CopyStart        uint64
	LengthBytes      uint64
	Data             []byte
	ParallelChunkCnt int
}

type physicalChunkWriteTaskResult struct {
	Task     physicalChunkWriteTask
	Duration time.Duration
	Err      error
}

func (w *RemoteReplicaWriter) writeFullChunkAwareRPCsInParallel(ctx context.Context, physicalClient service.PhysicalChunkSBSClient, replica RemoteReplica, target ReplicaTarget, plan ExtentWritePlan, req ReplicaWriteRequest, writeOffset uint64, payload []byte, chunks []resolvedChunkWrite, chunkSize uint64) (bool, error) {
	tasks, ok, err := fullPhysicalChunkWriteTasks(plan, req, writeOffset, payload, chunks, chunkSize)
	if err != nil {
		return true, err
	}
	if !ok {
		return false, nil
	}
	if len(tasks) <= 1 {
		return false, nil
	}

	parallelism := len(tasks)
	if w.maxParallelChunkWrites > 0 && w.maxParallelChunkWrites < parallelism {
		parallelism = w.maxParallelChunkWrites
	}
	resultCh := make(chan physicalChunkWriteTaskResult, len(tasks))
	taskCh := make(chan physicalChunkWriteTask)
	var wg sync.WaitGroup
	for worker := 0; worker < parallelism; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for task := range taskCh {
				rpcStart := time.Now()
				err := writePhysicalChunkTask(ctx, physicalClient, replica, plan, req, task)
				resultCh <- physicalChunkWriteTaskResult{Task: task, Duration: time.Since(rpcStart), Err: err}
			}
		}()
	}
	for _, task := range tasks {
		task := task
		task.ParallelChunkCnt = parallelism
		taskCh <- task
	}
	close(taskCh)
	wg.Wait()
	close(resultCh)

	var failures []string
	for result := range resultCh {
		if result.Err == nil {
			logReplicaChunkWriteCompleted(replica, target, plan, req, result.Task, result.Duration)
			continue
		}
		logReplicaChunkWriteFailed(replica, target, plan, req, result.Task, result.Duration, result.Err)
		failures = append(failures, result.Err.Error())
	}
	if len(failures) > 0 {
		return true, errors.New(strings.Join(failures, "; "))
	}
	return true, nil
}

func fullPhysicalChunkWriteTasks(plan ExtentWritePlan, req ReplicaWriteRequest, writeOffset uint64, payload []byte, chunks []resolvedChunkWrite, chunkSize uint64) ([]physicalChunkWriteTask, bool, error) {
	writeEnd := writeOffset + uint64(len(payload))
	tasks := make([]physicalChunkWriteTask, 0, len(chunks))
	for _, chunk := range chunks {
		if chunk.BasePhysicalChunkID != 0 && chunk.BasePhysicalChunkID != chunk.PhysicalChunkID {
			return nil, false, nil
		}
		chunkStart := chunk.LogicalChunk * chunkSize
		chunkEnd := chunkStart + chunkSize
		copyStart := maxUint64(writeOffset, chunkStart)
		copyEnd := minUint64(writeEnd, chunkEnd)
		if copyStart >= copyEnd {
			continue
		}
		chunkOffset := copyStart - chunkStart
		payloadOffset := copyStart - writeOffset
		copyLen := copyEnd - copyStart
		if chunkOffset != 0 || copyLen != chunkSize {
			return nil, false, nil
		}
		if payloadOffset+copyLen > uint64(len(payload)) {
			return nil, true, fmt.Errorf("chunk-aware write payload bounds exceeded")
		}
		task := physicalChunkWriteTask{
			LogicalChunk:     chunk.LogicalChunk,
			PhysicalChunkID:  chunk.PhysicalChunkID,
			CopyStart:        copyStart,
			LengthBytes:      copyLen,
			Data:             payload[payloadOffset : payloadOffset+copyLen],
			ParallelChunkCnt: 1,
		}
		tasks = append(tasks, task)
	}
	return tasks, true, nil
}

func physicalChunkWriteRequestID(rootRequestID string, extentID uint64, replicaID string, logicalChunk uint64) string {
	if replicaID == "" {
		return fmt.Sprintf("%s-ext-%020d-chunk-%020d", rootRequestID, extentID, logicalChunk)
	}
	return fmt.Sprintf("%s-ext-%020d-replica-%s-chunk-%020d", rootRequestID, extentID, replicaID, logicalChunk)
}

func writePhysicalChunkTask(ctx context.Context, physicalClient service.PhysicalChunkSBSClient, replica RemoteReplica, plan ExtentWritePlan, req ReplicaWriteRequest, task physicalChunkWriteTask) error {
	physicalRequestID := physicalChunkWriteRequestID(req.RequestID, plan.Extent.ExtentID, replica.ReplicaID, task.LogicalChunk)
	_, err := physicalClient.WritePhysicalChunk(ctx, &service.WritePhysicalChunkRequest{
		VolumeID:         replica.VolumeID,
		VolumeHandle:     replica.VolumeHandle,
		PhysicalChunkID:  task.PhysicalChunkID,
		ChunkOffsetBytes: 0,
		LengthBytes:      task.LengthBytes,
		Data:             task.Data,
		Context: service.SBSRequestContext{
			RequestID:      physicalRequestID,
			GatewayID:      replica.GatewayID,
			HostID:         replica.HostID,
			SessionID:      replica.SessionID,
			AttachmentID:   replica.AttachmentID,
			Generation:     replica.Generation,
			IdempotencyKey: fmt.Sprintf("%s-ext-%020d-chunk-%020d-phys-%020d", req.IdempotencyKey, plan.Extent.ExtentID, task.LogicalChunk, task.PhysicalChunkID),
		},
	})
	return err
}

func logReplicaChunkWriteFailed(replica RemoteReplica, target ReplicaTarget, plan ExtentWritePlan, req ReplicaWriteRequest, task physicalChunkWriteTask, duration time.Duration, err error) {
	physicalRequestID := physicalChunkWriteRequestID(req.RequestID, plan.Extent.ExtentID, replica.ReplicaID, task.LogicalChunk)
	structuredlog.Error("sbs.replication", "replica_chunk_write_failed", err,
		structuredlog.F("request_id", req.RequestID),
		structuredlog.F("physical_request_id", physicalRequestID),
		structuredlog.F("volume_id", req.VolumeID),
		structuredlog.F("replica_id", replica.ReplicaID),
		structuredlog.F("target_replica_id", target.ReplicaID),
		structuredlog.F("node_id", target.NodeID),
		structuredlog.F("replica_session_id", replica.SessionID),
		structuredlog.F("extent_id", plan.Extent.ExtentID),
		structuredlog.F("logical_chunk", task.LogicalChunk),
		structuredlog.F("logical_chunk_count", 1),
		structuredlog.F("physical_chunk_id", task.PhysicalChunkID),
		structuredlog.F("physical_chunk_count", 1),
		structuredlog.F("coalesced_chunks", 1),
		structuredlog.F("parallel_chunks", task.ParallelChunkCnt),
		structuredlog.F("chunk_write_mode", "parallel_full_chunks"),
		structuredlog.F("offset_bytes", task.CopyStart),
		structuredlog.F("physical_chunk_offset_bytes", uint64(0)),
		structuredlog.F("length_bytes", task.LengthBytes),
		structuredlog.F("duration_ms", duration.Milliseconds()),
	)
}

func logReplicaChunkWriteCompleted(replica RemoteReplica, target ReplicaTarget, plan ExtentWritePlan, req ReplicaWriteRequest, task physicalChunkWriteTask, duration time.Duration) {
	physicalRequestID := physicalChunkWriteRequestID(req.RequestID, plan.Extent.ExtentID, replica.ReplicaID, task.LogicalChunk)
	structuredlog.Info("sbs.replication", "replica_chunk_write_completed",
		structuredlog.F("request_id", req.RequestID),
		structuredlog.F("physical_request_id", physicalRequestID),
		structuredlog.F("volume_id", req.VolumeID),
		structuredlog.F("replica_id", replica.ReplicaID),
		structuredlog.F("target_replica_id", target.ReplicaID),
		structuredlog.F("node_id", target.NodeID),
		structuredlog.F("replica_session_id", replica.SessionID),
		structuredlog.F("extent_id", plan.Extent.ExtentID),
		structuredlog.F("logical_chunk", task.LogicalChunk),
		structuredlog.F("logical_chunk_count", 1),
		structuredlog.F("physical_chunk_id", task.PhysicalChunkID),
		structuredlog.F("physical_chunk_count", 1),
		structuredlog.F("coalesced_chunks", 1),
		structuredlog.F("parallel_chunks", task.ParallelChunkCnt),
		structuredlog.F("chunk_write_mode", "parallel_full_chunks"),
		structuredlog.F("offset_bytes", task.CopyStart),
		structuredlog.F("physical_chunk_offset_bytes", uint64(0)),
		structuredlog.F("length_bytes", task.LengthBytes),
		structuredlog.F("duration_ms", duration.Milliseconds()),
	)
}

func (w *RemoteReplicaWriter) loadRemoteChunkForMerge(ctx context.Context, physicalClient service.PhysicalChunkSBSClient, replica RemoteReplica, plan ExtentWritePlan, req ReplicaWriteRequest, logicalChunk, physicalChunkID, chunkSize uint64, encryptionHeader *metadata.PayloadEncryptionHeader, fullChunkWrite bool) ([]byte, error) {
	if fullChunkWrite {
		return make([]byte, chunkSize), nil
	}
	resp, err := physicalClient.ReadPhysicalChunk(ctx, &service.ReadPhysicalChunkRequest{
		VolumeID:         replica.VolumeID,
		VolumeHandle:     replica.VolumeHandle,
		PhysicalChunkID:  physicalChunkID,
		ChunkOffsetBytes: 0,
		LengthBytes:      chunkSize,
		Context: service.SBSRequestContext{
			RequestID:    fmt.Sprintf("%s-ext-%020d-base-chunk-%020d", req.RequestID, plan.Extent.ExtentID, logicalChunk),
			GatewayID:    replica.GatewayID,
			HostID:       replica.HostID,
			SessionID:    replica.SessionID,
			AttachmentID: replica.AttachmentID,
			Generation:   replica.Generation,
		},
	})
	if err != nil {
		if isSBSNotFound(err) && encryptionHeader == nil {
			return make([]byte, chunkSize), nil
		}
		return nil, fmt.Errorf("read base physical chunk %d for partial write: %w", physicalChunkID, err)
	}
	if uint64(len(resp.Data)) < chunkSize {
		return nil, fmt.Errorf("remote replica returned short base chunk payload")
	}
	chunkPayload := append([]byte(nil), resp.Data[:chunkSize]...)
	if encryptionHeader != nil {
		plaintext, err := w.encryption.decryptFixedChunk(ctx, req.VolumeID, logicalChunk, physicalChunkID, chunkSize, encryptionHeader, chunkPayload)
		if err != nil {
			return nil, err
		}
		chunkPayload = plaintext
	}
	return chunkPayload, nil
}

type chunkWriteRange struct {
	LogicalChunkStart  uint64
	PhysicalChunkStart uint64
	ChunkCount         uint64
	OffsetBytes        uint64
	LengthBytes        uint64
	PayloadOffset      uint64
}

func coalescedChunkWriteRanges(plan ExtentWritePlan, writeOffset, payloadLength uint64, chunks []resolvedChunkWrite) ([]chunkWriteRange, error) {
	chunkSize := uint64(plan.ChunkSizeBytes)
	if chunkSize == 0 {
		return nil, fmt.Errorf("chunk-aware write requires non-zero chunk size")
	}
	writeEnd := writeOffset + payloadLength
	ranges := make([]chunkWriteRange, 0, len(chunks))
	for _, chunk := range chunks {
		chunkStart := chunk.LogicalChunk * chunkSize
		chunkEnd := chunkStart + chunkSize
		copyStart := maxUint64(writeOffset, chunkStart)
		copyEnd := minUint64(writeEnd, chunkEnd)
		if copyStart >= copyEnd {
			continue
		}
		payloadOffset := copyStart - writeOffset
		copyLen := copyEnd - copyStart
		if payloadOffset+copyLen > payloadLength {
			return nil, fmt.Errorf("chunk-aware write payload bounds exceeded")
		}
		next := chunkWriteRange{
			LogicalChunkStart:  chunk.LogicalChunk,
			PhysicalChunkStart: chunk.PhysicalChunkID,
			ChunkCount:         1,
			OffsetBytes:        chunkAwarePayloadOffset(plan, chunk, copyStart, chunkStart),
			LengthBytes:        copyLen,
			PayloadOffset:      payloadOffset,
		}
		if len(ranges) == 0 {
			ranges = append(ranges, next)
			continue
		}
		prev := &ranges[len(ranges)-1]
		if canCoalesceChunkWriteRange(*prev, next) {
			prev.ChunkCount++
			prev.LengthBytes += next.LengthBytes
			continue
		}
		ranges = append(ranges, next)
	}
	return ranges, nil
}

func chunkAwarePayloadOffset(plan ExtentWritePlan, chunk resolvedChunkWrite, logicalOffset, logicalChunkStart uint64) uint64 {
	chunkSize := uint64(plan.ChunkSizeBytes)
	return chunk.PhysicalChunkID*chunkSize + (logicalOffset - logicalChunkStart)
}

func canCoalesceChunkWriteRange(prev, next chunkWriteRange) bool {
	return prev.LogicalChunkStart+prev.ChunkCount == next.LogicalChunkStart &&
		prev.PhysicalChunkStart+prev.ChunkCount == next.PhysicalChunkStart &&
		prev.OffsetBytes+prev.LengthBytes == next.OffsetBytes &&
		prev.PayloadOffset+prev.LengthBytes == next.PayloadOffset
}

type RemoteReplicaReader struct {
	replicas   map[string]RemoteReplica
	encryption *localReplicaPayloadEncryptor
}

func NewRemoteReplicaReader(replicas map[string]RemoteReplica) *RemoteReplicaReader {
	cloned := make(map[string]RemoteReplica, len(replicas))
	for replicaID, replica := range replicas {
		cloned[replicaID] = replica
	}
	return &RemoteReplicaReader{replicas: cloned}
}

func NewEncryptedRemoteReplicaReaderForPhaseP(replicas map[string]RemoteReplica, cfg PhasePReplicaEncryptionConfig) *RemoteReplicaReader {
	reader := NewRemoteReplicaReader(replicas)
	reader.encryption = newLocalReplicaPayloadEncryptor(cfg)
	return reader
}

func (r *RemoteReplicaReader) ReadExtent(ctx context.Context, plan ExtentReadPlan, req ReplicaReadRequest) ([]byte, string, error) {
	started := time.Now()
	readOffset, readLength, err := overlapRange(plan.Extent.LogicalOffset, plan.Extent.LengthBytes, req.OffsetBytes, req.LengthBytes)
	if err != nil {
		return nil, "", err
	}
	if readRangeSatisfiedByZeroAllocation(plan, req) {
		structuredlog.Info("sbs.replication", "replica_read_zero_allocation",
			structuredlog.F("request_id", req.RequestID),
			structuredlog.F("volume_id", req.VolumeID),
			structuredlog.F("extent_id", plan.Extent.ExtentID),
			structuredlog.F("offset_bytes", readOffset),
			structuredlog.F("length_bytes", readLength),
			structuredlog.F("duration_ms", time.Since(started).Milliseconds()),
		)
		return make([]byte, readLength), "", nil
	}
	targets := append([]ReplicaTarget{plan.Preferred}, plan.Fallbacks...)
	var lastErr error
	sawMissingPayload := false
	for _, target := range targets {
		replica, ok := remoteReplicaForTarget(r.replicas, target)
		if !ok {
			lastErr = fmt.Errorf("replica %q on node %q has no remote client", target.ReplicaID, target.NodeID)
			continue
		}
		attemptStarted := time.Now()
		if data, ok, sawMissing, err := r.readViaChunkAwareRPC(ctx, replica, plan, req, readOffset, readLength); ok {
			attemptDuration := time.Since(attemptStarted)
			if err != nil {
				if sawMissing {
					sawMissingPayload = true
				}
				if req.Attribution {
					structuredlog.Error("sbs.replication", "replica_read_failed", err,
						structuredlog.F("request_id", req.RequestID),
						structuredlog.F("volume_id", req.VolumeID),
						structuredlog.F("replica_id", replica.ReplicaID),
						structuredlog.F("target_replica_id", target.ReplicaID),
						structuredlog.F("node_id", target.NodeID),
						structuredlog.F("extent_id", plan.Extent.ExtentID),
						structuredlog.F("offset_bytes", readOffset),
						structuredlog.F("length_bytes", readLength),
						structuredlog.F("duration_ms", attemptDuration.Milliseconds()),
						structuredlog.F("chunk_aware", true),
						structuredlog.F("missing_payload", sawMissing),
					)
				}
				lastErr = err
				continue
			}
			if req.Attribution {
				structuredlog.Info("sbs.replication", "replica_read_completed",
					structuredlog.F("request_id", req.RequestID),
					structuredlog.F("volume_id", req.VolumeID),
					structuredlog.F("replica_id", replica.ReplicaID),
					structuredlog.F("target_replica_id", target.ReplicaID),
					structuredlog.F("node_id", target.NodeID),
					structuredlog.F("extent_id", plan.Extent.ExtentID),
					structuredlog.F("offset_bytes", readOffset),
					structuredlog.F("length_bytes", readLength),
					structuredlog.F("response_bytes", len(data)),
					structuredlog.F("duration_ms", attemptDuration.Milliseconds()),
					structuredlog.F("chunk_aware", true),
				)
			}
			return data, target.ReplicaID, nil
		}
		if r.encryption != nil {
			lastErr = fmt.Errorf("phase p encrypted remote replica reader requires allocation chunk mappings")
			continue
		}
		resp, err := replica.Client.Read(ctx, &service.ReadRequest{
			VolumeID:     replica.VolumeID,
			VolumeHandle: replica.VolumeHandle,
			OffsetBytes:  readOffset,
			LengthBytes:  readLength,
			Context: service.SBSRequestContext{
				RequestID:    fmt.Sprintf("cluster-read-%s-ext-%020d-%s", replica.VolumeID, plan.Extent.ExtentID, target.ReplicaID),
				GatewayID:    replica.GatewayID,
				HostID:       replica.HostID,
				SessionID:    replica.SessionID,
				AttachmentID: replica.AttachmentID,
				Generation:   replica.Generation,
			},
		})
		if err != nil {
			if isSBSNotFound(err) {
				sawMissingPayload = true
			}
			structuredlog.Error("sbs.replication", "replica_read_failed", err,
				structuredlog.F("request_id", req.RequestID),
				structuredlog.F("volume_id", req.VolumeID),
				structuredlog.F("replica_id", replica.ReplicaID),
				structuredlog.F("extent_id", plan.Extent.ExtentID),
				structuredlog.F("offset_bytes", readOffset),
				structuredlog.F("length_bytes", readLength),
				structuredlog.F("duration_ms", time.Since(attemptStarted).Milliseconds()),
				structuredlog.F("chunk_aware", false),
				structuredlog.F("missing_payload", isSBSNotFound(err)),
			)
			lastErr = err
			continue
		}
		structuredlog.Info("sbs.replication", "replica_read_completed",
			structuredlog.F("request_id", req.RequestID),
			structuredlog.F("volume_id", req.VolumeID),
			structuredlog.F("replica_id", replica.ReplicaID),
			structuredlog.F("extent_id", plan.Extent.ExtentID),
			structuredlog.F("offset_bytes", readOffset),
			structuredlog.F("length_bytes", readLength),
			structuredlog.F("response_bytes", len(resp.Data)),
			structuredlog.F("duration_ms", time.Since(attemptStarted).Milliseconds()),
			structuredlog.F("chunk_aware", false),
		)
		return append([]byte(nil), resp.Data...), target.ReplicaID, nil
	}
	if sawMissingPayload {
		structuredlog.Info("sbs.replication", "replica_read_missing_zero_fallback",
			structuredlog.F("request_id", req.RequestID),
			structuredlog.F("volume_id", req.VolumeID),
			structuredlog.F("extent_id", plan.Extent.ExtentID),
			structuredlog.F("offset_bytes", readOffset),
			structuredlog.F("length_bytes", readLength),
		)
		return make([]byte, readLength), "", nil
	}
	if lastErr == nil {
		lastErr = fmt.Errorf("no readable remote replica found")
	}
	return nil, "", lastErr
}

func (r *RemoteReplicaReader) readViaChunkAwareRPC(ctx context.Context, replica RemoteReplica, plan ExtentReadPlan, req ReplicaReadRequest, readOffset, readLength uint64) ([]byte, bool, bool, error) {
	chunks, ok := resolvedReadChunkMappings(plan, req)
	if !ok || len(chunks) == 0 {
		return nil, false, false, nil
	}
	physicalClient, ok := replica.Client.(service.PhysicalChunkSBSClient)
	if !ok {
		return nil, true, false, fmt.Errorf("replica %q does not support physical chunk read RPC", replica.ReplicaID)
	}
	out := make([]byte, readLength)
	chunkSize := uint64(plan.ChunkSizeBytes)
	if chunkSize == 0 {
		return nil, true, false, fmt.Errorf("chunk-aware read requires non-zero chunk size")
	}
	readEnd := readOffset + readLength
	for _, chunk := range chunks {
		chunkStart := chunk.LogicalChunk * chunkSize
		chunkEnd := chunkStart + chunkSize
		copyStart := maxUint64(readOffset, chunkStart)
		copyEnd := minUint64(readEnd, chunkEnd)
		if copyStart >= copyEnd {
			continue
		}
		outOffset := copyStart - readOffset
		copyLen := copyEnd - copyStart
		if chunk.Zero {
			structuredlog.Info("sbs.replication", "replica_chunk_read_zero_allocation",
				structuredlog.F("request_id", req.RequestID),
				structuredlog.F("volume_id", req.VolumeID),
				structuredlog.F("replica_id", replica.ReplicaID),
				structuredlog.F("extent_id", plan.Extent.ExtentID),
				structuredlog.F("logical_chunk", chunk.LogicalChunk),
				structuredlog.F("physical_chunk_id", chunk.PhysicalChunkID),
				structuredlog.F("offset_bytes", copyStart),
				structuredlog.F("length_bytes", copyLen),
			)
			continue
		}
		rpcStart := time.Now()
		chunkOffset := copyStart - chunkStart
		physicalReadOffset := chunkOffset
		physicalReadLength := copyLen
		chunkValueOffset := uint64(0)
		if chunk.Encryption != nil {
			physicalReadOffset = 0
			physicalReadLength = chunkSize
			chunkValueOffset = chunkOffset
		}
		resp, err := physicalClient.ReadPhysicalChunk(ctx, &service.ReadPhysicalChunkRequest{
			VolumeID:         replica.VolumeID,
			VolumeHandle:     replica.VolumeHandle,
			PhysicalChunkID:  chunk.PhysicalChunkID,
			ChunkOffsetBytes: physicalReadOffset,
			LengthBytes:      physicalReadLength,
			Context: service.SBSRequestContext{
				RequestID:    fmt.Sprintf("cluster-read-%s-ext-%020d-chunk-%020d", replica.VolumeID, plan.Extent.ExtentID, chunk.LogicalChunk),
				GatewayID:    replica.GatewayID,
				HostID:       replica.HostID,
				SessionID:    replica.SessionID,
				AttachmentID: replica.AttachmentID,
				Generation:   replica.Generation,
			},
		})
		rpcDuration := time.Since(rpcStart)
		if err != nil {
			structuredlog.Error("sbs.replication", "replica_chunk_read_failed", err,
				structuredlog.F("request_id", req.RequestID),
				structuredlog.F("volume_id", req.VolumeID),
				structuredlog.F("replica_id", replica.ReplicaID),
				structuredlog.F("extent_id", plan.Extent.ExtentID),
				structuredlog.F("logical_chunk", chunk.LogicalChunk),
				structuredlog.F("physical_chunk_id", chunk.PhysicalChunkID),
				structuredlog.F("physical_chunk_offset_bytes", physicalReadOffset),
				structuredlog.F("logical_offset_bytes", copyStart),
				structuredlog.F("length_bytes", copyLen),
				structuredlog.F("duration_ms", rpcDuration.Milliseconds()),
				structuredlog.F("missing_payload", isSBSNotFound(err)),
			)
			return nil, true, isSBSNotFound(err) && chunk.Encryption == nil, err
		}
		if uint64(len(resp.Data)) < physicalReadLength {
			err := fmt.Errorf("remote replica returned short chunk payload")
			structuredlog.Error("sbs.replication", "replica_chunk_read_failed", err,
				structuredlog.F("request_id", req.RequestID),
				structuredlog.F("volume_id", req.VolumeID),
				structuredlog.F("replica_id", replica.ReplicaID),
				structuredlog.F("extent_id", plan.Extent.ExtentID),
				structuredlog.F("logical_chunk", chunk.LogicalChunk),
				structuredlog.F("physical_chunk_id", chunk.PhysicalChunkID),
				structuredlog.F("physical_chunk_offset_bytes", physicalReadOffset),
				structuredlog.F("logical_offset_bytes", copyStart),
				structuredlog.F("length_bytes", copyLen),
				structuredlog.F("response_bytes", len(resp.Data)),
				structuredlog.F("duration_ms", rpcDuration.Milliseconds()),
			)
			return nil, true, false, err
		}
		chunkValue := resp.Data[:physicalReadLength]
		if chunk.Encryption != nil {
			var err error
			chunkValue, err = r.encryption.decryptFixedChunk(ctx, req.VolumeID, chunk.LogicalChunk, chunk.PhysicalChunkID, chunkSize, chunk.Encryption, chunkValue)
			if err != nil {
				return nil, true, false, err
			}
		}
		if uint64(len(chunkValue)) < chunkValueOffset+copyLen {
			return nil, true, false, fmt.Errorf("remote replica payload shorter than requested chunk window")
		}
		copy(out[outOffset:outOffset+copyLen], chunkValue[chunkValueOffset:chunkValueOffset+copyLen])
		structuredlog.Info("sbs.replication", "replica_chunk_read_completed",
			structuredlog.F("request_id", req.RequestID),
			structuredlog.F("volume_id", req.VolumeID),
			structuredlog.F("replica_id", replica.ReplicaID),
			structuredlog.F("extent_id", plan.Extent.ExtentID),
			structuredlog.F("logical_chunk", chunk.LogicalChunk),
			structuredlog.F("physical_chunk_id", chunk.PhysicalChunkID),
			structuredlog.F("physical_chunk_offset_bytes", physicalReadOffset),
			structuredlog.F("logical_offset_bytes", copyStart),
			structuredlog.F("length_bytes", copyLen),
			structuredlog.F("response_bytes", len(resp.Data)),
			structuredlog.F("duration_ms", rpcDuration.Milliseconds()),
		)
	}
	return out, true, false, nil
}

func remoteReplicaForTarget(replicas map[string]RemoteReplica, target ReplicaTarget) (RemoteReplica, bool) {
	if replica, ok := replicas[target.ReplicaID]; ok {
		return replica, true
	}
	if target.NodeID != "" {
		if replica, ok := replicas[target.NodeID]; ok {
			return replica, true
		}
	}
	return RemoteReplica{}, false
}

func overlapRange(extentOffset, extentLength, reqOffset, reqLength uint64) (uint64, uint64, error) {
	extentEnd := extentOffset + extentLength
	reqEnd := reqOffset + reqLength
	if extentEnd <= reqOffset || extentOffset >= reqEnd {
		return 0, 0, fmt.Errorf("request does not overlap extent")
	}
	start := maxUint64(reqOffset, extentOffset)
	end := minUint64(reqEnd, extentEnd)
	return start, end - start, nil
}
