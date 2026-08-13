package maintenance

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nosway/namrbd/gateway/service"
	"github.com/nosway/namrbd/sbs/cluster/metadata"
	"github.com/nosway/namrbd/sbs/cluster/replication"
)

type failReadSBSClient struct {
	next service.SBSClient
}

type trackingSBSClient struct {
	next service.SBSClient

	mu         sync.Mutex
	readCalls  []service.ReadRequest
	writeCalls []service.WriteRequest
}

type failAfterNWritesSBSClient struct {
	next        *trackingSBSClient
	failAfter   int
	failMessage string

	mu         sync.Mutex
	writeCount int
}

type failWriteOffsetOnceSBSClient struct {
	next        *trackingSBSClient
	failOffset  uint64
	failMessage string

	mu     sync.Mutex
	failed bool
}

func newTrackingSBSClient(next service.SBSClient) *trackingSBSClient {
	return &trackingSBSClient{next: next}
}

func (c *trackingSBSClient) snapshotReads() []service.ReadRequest {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]service.ReadRequest(nil), c.readCalls...)
}

func (c *trackingSBSClient) snapshotWrites() []service.WriteRequest {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]service.WriteRequest(nil), c.writeCalls...)
}

func (c failReadSBSClient) OpenVolume(ctx context.Context, req *service.OpenVolumeRequest) (*service.OpenVolumeResponse, error) {
	return c.next.OpenVolume(ctx, req)
}

func (c failReadSBSClient) CloseVolume(ctx context.Context, req *service.CloseVolumeRequest) (*service.CloseVolumeResponse, error) {
	return c.next.CloseVolume(ctx, req)
}

func (c failReadSBSClient) GetVolumeProfile(ctx context.Context, req *service.GetVolumeProfileRequest) (*service.GetVolumeProfileResponse, error) {
	return c.next.GetVolumeProfile(ctx, req)
}

func (c failReadSBSClient) GetVolumeStatus(ctx context.Context, req *service.GetVolumeStatusRequest) (*service.GetVolumeStatusResponse, error) {
	return c.next.GetVolumeStatus(ctx, req)
}

func (c failReadSBSClient) Read(context.Context, *service.ReadRequest) (*service.ReadResponse, error) {
	return nil, fmt.Errorf("unexpected source read for metadata-only zero transition")
}

func (c failReadSBSClient) ReadPhysicalChunk(context.Context, *service.ReadPhysicalChunkRequest) (*service.ReadPhysicalChunkResponse, error) {
	return nil, fmt.Errorf("unexpected source physical chunk read for metadata-only zero transition")
}

func (c failReadSBSClient) WritePhysicalChunk(ctx context.Context, req *service.WritePhysicalChunkRequest) (*service.WritePhysicalChunkResponse, error) {
	physical, ok := c.next.(service.PhysicalChunkSBSClient)
	if !ok {
		return nil, fmt.Errorf("wrapped client does not support physical chunk write RPC")
	}
	return physical.WritePhysicalChunk(ctx, req)
}

func (c *trackingSBSClient) OpenVolume(ctx context.Context, req *service.OpenVolumeRequest) (*service.OpenVolumeResponse, error) {
	return c.next.OpenVolume(ctx, req)
}

func (c *trackingSBSClient) CloseVolume(ctx context.Context, req *service.CloseVolumeRequest) (*service.CloseVolumeResponse, error) {
	return c.next.CloseVolume(ctx, req)
}

func (c *trackingSBSClient) GetVolumeProfile(ctx context.Context, req *service.GetVolumeProfileRequest) (*service.GetVolumeProfileResponse, error) {
	return c.next.GetVolumeProfile(ctx, req)
}

func (c *trackingSBSClient) GetVolumeStatus(ctx context.Context, req *service.GetVolumeStatusRequest) (*service.GetVolumeStatusResponse, error) {
	return c.next.GetVolumeStatus(ctx, req)
}

func (c *trackingSBSClient) Read(ctx context.Context, req *service.ReadRequest) (*service.ReadResponse, error) {
	c.mu.Lock()
	c.readCalls = append(c.readCalls, *req)
	c.mu.Unlock()
	return c.next.Read(ctx, req)
}

func (c *trackingSBSClient) Write(ctx context.Context, req *service.WriteRequest) (*service.WriteResponse, error) {
	c.mu.Lock()
	c.writeCalls = append(c.writeCalls, *req)
	c.mu.Unlock()
	return c.next.Write(ctx, req)
}

func (c *trackingSBSClient) ReadPhysicalChunk(ctx context.Context, req *service.ReadPhysicalChunkRequest) (*service.ReadPhysicalChunkResponse, error) {
	offsetBytes := trackedPhysicalReadOffset(req.Context.RequestID, req.ChunkOffsetBytes, req.LengthBytes)
	readReq := &service.ReadRequest{
		VolumeID:     req.VolumeID,
		VolumeHandle: req.VolumeHandle,
		OffsetBytes:  offsetBytes,
		LengthBytes:  req.LengthBytes,
		Context:      req.Context,
	}
	c.mu.Lock()
	c.readCalls = append(c.readCalls, *readReq)
	c.mu.Unlock()
	resp, err := c.next.Read(ctx, readReq)
	if err != nil {
		return nil, err
	}
	return &service.ReadPhysicalChunkResponse{Data: resp.Data}, nil
}

func (c *trackingSBSClient) WritePhysicalChunk(ctx context.Context, req *service.WritePhysicalChunkRequest) (*service.WritePhysicalChunkResponse, error) {
	offsetBytes := trackedPhysicalWriteOffset(req.Context.RequestID, req.ChunkOffsetBytes)
	writeReq := &service.WriteRequest{
		VolumeID:     req.VolumeID,
		VolumeHandle: req.VolumeHandle,
		OffsetBytes:  offsetBytes,
		LengthBytes:  req.LengthBytes,
		Data:         append([]byte(nil), req.Data...),
		Context:      req.Context,
	}
	c.mu.Lock()
	c.writeCalls = append(c.writeCalls, *writeReq)
	c.mu.Unlock()
	if _, err := c.next.Write(ctx, writeReq); err != nil {
		return nil, err
	}
	return &service.WritePhysicalChunkResponse{}, nil
}

func trackedPhysicalReadOffset(requestID string, chunkOffsetBytes, lengthBytes uint64) uint64 {
	marker := "-chunk-"
	idx := strings.LastIndex(requestID, marker)
	if idx < 0 {
		return chunkOffsetBytes
	}
	var logicalChunk uint64
	if _, err := fmt.Sscanf(requestID[idx+len(marker):], "%d", &logicalChunk); err != nil {
		return chunkOffsetBytes
	}
	return logicalChunk*lengthBytes + chunkOffsetBytes
}

func trackedPhysicalWriteOffset(requestID string, chunkOffsetBytes uint64) uint64 {
	if strings.Contains(requestID, "-chunk-") {
		return trackedPhysicalReadOffset(requestID, chunkOffsetBytes, 4)
	}
	idx := strings.LastIndex(requestID, "-")
	if idx < 0 {
		return chunkOffsetBytes
	}
	var offset uint64
	if _, err := fmt.Sscanf(requestID[idx+1:], "%d", &offset); err != nil {
		return chunkOffsetBytes
	}
	return offset + chunkOffsetBytes
}

func (c *trackingSBSClient) Flush(ctx context.Context, req *service.FlushRequest) (*service.FlushResponse, error) {
	return c.next.Flush(ctx, req)
}

func (c *trackingSBSClient) Discard(ctx context.Context, req *service.DiscardRequest) (*service.DiscardResponse, error) {
	return c.next.Discard(ctx, req)
}

func (c *trackingSBSClient) Zero(ctx context.Context, req *service.ZeroRequest) (*service.ZeroResponse, error) {
	return c.next.Zero(ctx, req)
}

func (c failReadSBSClient) Write(ctx context.Context, req *service.WriteRequest) (*service.WriteResponse, error) {
	return c.next.Write(ctx, req)
}

func (c failReadSBSClient) Flush(ctx context.Context, req *service.FlushRequest) (*service.FlushResponse, error) {
	return c.next.Flush(ctx, req)
}

func (c failReadSBSClient) Discard(ctx context.Context, req *service.DiscardRequest) (*service.DiscardResponse, error) {
	return c.next.Discard(ctx, req)
}

func (c failReadSBSClient) Zero(ctx context.Context, req *service.ZeroRequest) (*service.ZeroResponse, error) {
	return c.next.Zero(ctx, req)
}

func (c *failAfterNWritesSBSClient) OpenVolume(ctx context.Context, req *service.OpenVolumeRequest) (*service.OpenVolumeResponse, error) {
	return c.next.OpenVolume(ctx, req)
}

func (c *failAfterNWritesSBSClient) CloseVolume(ctx context.Context, req *service.CloseVolumeRequest) (*service.CloseVolumeResponse, error) {
	return c.next.CloseVolume(ctx, req)
}

func (c *failAfterNWritesSBSClient) GetVolumeProfile(ctx context.Context, req *service.GetVolumeProfileRequest) (*service.GetVolumeProfileResponse, error) {
	return c.next.GetVolumeProfile(ctx, req)
}

func (c *failAfterNWritesSBSClient) GetVolumeStatus(ctx context.Context, req *service.GetVolumeStatusRequest) (*service.GetVolumeStatusResponse, error) {
	return c.next.GetVolumeStatus(ctx, req)
}

func (c *failAfterNWritesSBSClient) Read(ctx context.Context, req *service.ReadRequest) (*service.ReadResponse, error) {
	return c.next.Read(ctx, req)
}

func (c *failAfterNWritesSBSClient) Write(ctx context.Context, req *service.WriteRequest) (*service.WriteResponse, error) {
	c.mu.Lock()
	c.writeCount++
	writeCount := c.writeCount
	c.mu.Unlock()
	if c.failAfter > 0 && writeCount > c.failAfter {
		return nil, fmt.Errorf("%s", c.failMessage)
	}
	return c.next.Write(ctx, req)
}

func (c *failAfterNWritesSBSClient) ReadPhysicalChunk(ctx context.Context, req *service.ReadPhysicalChunkRequest) (*service.ReadPhysicalChunkResponse, error) {
	return c.next.ReadPhysicalChunk(ctx, req)
}

func (c *failAfterNWritesSBSClient) WritePhysicalChunk(ctx context.Context, req *service.WritePhysicalChunkRequest) (*service.WritePhysicalChunkResponse, error) {
	c.mu.Lock()
	c.writeCount++
	writeCount := c.writeCount
	c.mu.Unlock()
	if c.failAfter > 0 && writeCount > c.failAfter {
		return nil, fmt.Errorf("%s", c.failMessage)
	}
	return c.next.WritePhysicalChunk(ctx, req)
}

func (c *failAfterNWritesSBSClient) Flush(ctx context.Context, req *service.FlushRequest) (*service.FlushResponse, error) {
	return c.next.Flush(ctx, req)
}

func (c *failAfterNWritesSBSClient) Discard(ctx context.Context, req *service.DiscardRequest) (*service.DiscardResponse, error) {
	return c.next.Discard(ctx, req)
}

func (c *failAfterNWritesSBSClient) Zero(ctx context.Context, req *service.ZeroRequest) (*service.ZeroResponse, error) {
	return c.next.Zero(ctx, req)
}

func (c *failWriteOffsetOnceSBSClient) OpenVolume(ctx context.Context, req *service.OpenVolumeRequest) (*service.OpenVolumeResponse, error) {
	return c.next.OpenVolume(ctx, req)
}

func (c *failWriteOffsetOnceSBSClient) CloseVolume(ctx context.Context, req *service.CloseVolumeRequest) (*service.CloseVolumeResponse, error) {
	return c.next.CloseVolume(ctx, req)
}

func (c *failWriteOffsetOnceSBSClient) GetVolumeProfile(ctx context.Context, req *service.GetVolumeProfileRequest) (*service.GetVolumeProfileResponse, error) {
	return c.next.GetVolumeProfile(ctx, req)
}

func (c *failWriteOffsetOnceSBSClient) GetVolumeStatus(ctx context.Context, req *service.GetVolumeStatusRequest) (*service.GetVolumeStatusResponse, error) {
	return c.next.GetVolumeStatus(ctx, req)
}

func (c *failWriteOffsetOnceSBSClient) Read(ctx context.Context, req *service.ReadRequest) (*service.ReadResponse, error) {
	return c.next.Read(ctx, req)
}

func (c *failWriteOffsetOnceSBSClient) Write(ctx context.Context, req *service.WriteRequest) (*service.WriteResponse, error) {
	c.mu.Lock()
	if !c.failed && req.OffsetBytes == c.failOffset {
		c.failed = true
		c.mu.Unlock()
		return nil, fmt.Errorf("%s", c.failMessage)
	}
	c.mu.Unlock()
	return c.next.Write(ctx, req)
}

func (c *failWriteOffsetOnceSBSClient) ReadPhysicalChunk(ctx context.Context, req *service.ReadPhysicalChunkRequest) (*service.ReadPhysicalChunkResponse, error) {
	return c.next.ReadPhysicalChunk(ctx, req)
}

func (c *failWriteOffsetOnceSBSClient) WritePhysicalChunk(ctx context.Context, req *service.WritePhysicalChunkRequest) (*service.WritePhysicalChunkResponse, error) {
	c.mu.Lock()
	if !c.failed && req.ChunkOffsetBytes == c.failOffset {
		c.failed = true
		c.mu.Unlock()
		return nil, fmt.Errorf("%s", c.failMessage)
	}
	c.mu.Unlock()
	return c.next.WritePhysicalChunk(ctx, req)
}

func (c *failWriteOffsetOnceSBSClient) Flush(ctx context.Context, req *service.FlushRequest) (*service.FlushResponse, error) {
	return c.next.Flush(ctx, req)
}

func (c *failWriteOffsetOnceSBSClient) Discard(ctx context.Context, req *service.DiscardRequest) (*service.DiscardResponse, error) {
	return c.next.Discard(ctx, req)
}

func (c *failWriteOffsetOnceSBSClient) Zero(ctx context.Context, req *service.ZeroRequest) (*service.ZeroResponse, error) {
	return c.next.Zero(ctx, req)
}

type fakeStore struct {
	volume                      metadata.VolumeState
	volumeSpec                  metadata.VolumeSpecRecord
	mappings                    []metadata.ExtentMappingRecord
	allocationPages             []metadata.AllocationPageRecord
	snapshotRecords             []metadata.SnapshotRecord
	snapshotAllocationPages     map[string][]metadata.AllocationPageRecord
	cloneRecords                []metadata.CloneRecord
	cloneDeltaAllocationPages   map[string][]metadata.AllocationPageRecord
	replicaSets                 []metadata.ReplicaSetState
	nodes                       map[string]metadata.NodeMembershipRecord
	nodeHealthDetails           map[string]metadata.NodeHealthDetailRecord
	transitions                 map[string]metadata.PlacementTransitionRecord
	transitionListOverride      []metadata.PlacementTransitionRecord
	transitionListOverrideCount int
	transitionPutOrder          []string
	mutationOps                 map[string]metadata.MutationOperationRecord
	listExtentMappingsCalls     int
	listCompatibleCalls         int
	listReplicaSetsCalls        int
	getNodeMembershipCalls      int
}

type recordingPlacementApplyRunner struct {
	calls int
	req   metadata.PlacementApplyRequest
	err   error
}

func (r *recordingPlacementApplyRunner) ApplyPlacementChanges(_ context.Context, req metadata.PlacementApplyRequest) error {
	r.calls++
	r.req = req
	return r.err
}

func newFakeStore() *fakeStore {
	return &fakeStore{
		volume: metadata.VolumeState{
			VolumeID: "00a1b2c3",
			Epoch:    5,
			Revision: 11,
			Status:   metadata.VolumeStatusHealthy,
		},
		mappings: []metadata.ExtentMappingRecord{
			{VolumeID: "00a1b2c3", ExtentID: 1, LogicalOffset: 0, LengthBytes: 8, ChunkID: 101, PlacementRef: "pl-1", Revision: 11},
		},
		replicaSets: []metadata.ReplicaSetState{
			{
				ReplicaSetID:     "rs-1",
				VolumeID:         "00a1b2c3",
				PlacementRef:     "pl-1",
				Epoch:            5,
				PrimaryReplicaID: "rep-a",
				WriteQuorum:      2,
				ReadQuorum:       1,
				Replicas: []metadata.ReplicaDescriptor{
					{NodeID: "node-a", ReplicaID: "rep-a", Role: metadata.ReplicaRolePrimary},
					{NodeID: "node-b", ReplicaID: "rep-b", Role: metadata.ReplicaRoleSecondary},
					{NodeID: "node-c", ReplicaID: "rep-c", Role: metadata.ReplicaRoleSecondary},
				},
			},
		},
		nodes: map[string]metadata.NodeMembershipRecord{
			"node-a": {NodeID: "node-a", LifecycleState: metadata.NodeLifecycleActive, HealthState: metadata.NodeHealthHealthy},
			"node-b": {NodeID: "node-b", LifecycleState: metadata.NodeLifecycleActive, HealthState: metadata.NodeHealthHealthy},
			"node-c": {NodeID: "node-c", LifecycleState: metadata.NodeLifecycleActive, HealthState: metadata.NodeHealthHealthy},
		},
		nodeHealthDetails:         make(map[string]metadata.NodeHealthDetailRecord),
		transitions:               make(map[string]metadata.PlacementTransitionRecord),
		mutationOps:               make(map[string]metadata.MutationOperationRecord),
		snapshotAllocationPages:   make(map[string][]metadata.AllocationPageRecord),
		cloneDeltaAllocationPages: make(map[string][]metadata.AllocationPageRecord),
	}
}

func (f *fakeStore) GetVolumeState(_ context.Context, _ string) (metadata.VolumeState, error) {
	return f.volume, nil
}

func (f *fakeStore) GetVolumeSpec(_ context.Context, _ string) (metadata.VolumeSpecRecord, error) {
	if f.volumeSpec.VolumeID == "" {
		return metadata.VolumeSpecRecord{}, metadata.ErrNotFound
	}
	return f.volumeSpec, nil
}

func (f *fakeStore) PutVolumeState(_ context.Context, rec metadata.VolumeState) error {
	f.volume = rec
	return nil
}

func (f *fakeStore) PutExtentMapping(_ context.Context, rec metadata.ExtentMappingRecord) error {
	for i := range f.mappings {
		if f.mappings[i].ExtentID == rec.ExtentID {
			f.mappings[i] = rec
			return nil
		}
	}
	f.mappings = append(f.mappings, rec)
	return nil
}

func (f *fakeStore) GetExtentMapping(_ context.Context, volumeID string, extentID uint64) (metadata.ExtentMappingRecord, error) {
	for _, rec := range f.mappings {
		if rec.VolumeID == volumeID && rec.ExtentID == extentID {
			return rec, nil
		}
	}
	return metadata.ExtentMappingRecord{}, metadata.ErrNotFound
}

func (f *fakeStore) PutAllocationPage(_ context.Context, rec metadata.AllocationPageRecord) error {
	for i := range f.allocationPages {
		if f.allocationPages[i].PageNo == rec.PageNo {
			f.allocationPages[i] = rec
			return nil
		}
	}
	f.allocationPages = append(f.allocationPages, rec)
	sort.Slice(f.allocationPages, func(i, j int) bool { return f.allocationPages[i].PageNo < f.allocationPages[j].PageNo })
	return nil
}

func (f *fakeStore) ListExtentMappings(_ context.Context, _ string) ([]metadata.ExtentMappingRecord, error) {
	f.listExtentMappingsCalls++
	return append([]metadata.ExtentMappingRecord(nil), f.mappings...), nil
}

func (f *fakeStore) ListAllocationPages(_ context.Context, _ string) ([]metadata.AllocationPageRecord, error) {
	return append([]metadata.AllocationPageRecord(nil), f.allocationPages...), nil
}

func (f *fakeStore) ListCompatibleAllocationPages(_ context.Context, _ string, pageBytes, chunkSizeBytes uint32) ([]metadata.AllocationPageRecord, error) {
	f.listCompatibleCalls++
	if len(f.allocationPages) > 0 {
		return append([]metadata.AllocationPageRecord(nil), f.allocationPages...), nil
	}
	if pageBytes == 0 || chunkSizeBytes == 0 || pageBytes%chunkSizeBytes != 0 {
		return nil, fmt.Errorf("invalid geometry")
	}
	chunksPerPage := uint64(pageBytes / chunkSizeBytes)
	pageNos := make(map[uint64]struct{})
	for _, mapping := range f.mappings {
		if mapping.LengthBytes == 0 {
			continue
		}
		startChunk := mapping.LogicalOffset / uint64(chunkSizeBytes)
		endChunk := (mapping.LogicalOffset + mapping.LengthBytes - 1) / uint64(chunkSizeBytes)
		for pageNo := startChunk / chunksPerPage; pageNo <= endChunk/chunksPerPage; pageNo++ {
			pageNos[pageNo] = struct{}{}
		}
	}
	ordered := make([]uint64, 0, len(pageNos))
	for pageNo := range pageNos {
		ordered = append(ordered, pageNo)
	}
	sort.Slice(ordered, func(i, j int) bool { return ordered[i] < ordered[j] })
	out := make([]metadata.AllocationPageRecord, 0, len(ordered))
	for _, pageNo := range ordered {
		pageStartChunk := pageNo * chunksPerPage
		physicalChunkIDs := make([]uint64, int(chunksPerPage))
		for _, mapping := range f.mappings {
			if mapping.LengthBytes == 0 {
				continue
			}
			extentStartChunk := mapping.LogicalOffset / uint64(chunkSizeBytes)
			extentEndChunk := (mapping.LogicalOffset + mapping.LengthBytes + uint64(chunkSizeBytes) - 1) / uint64(chunkSizeBytes)
			pageStart := pageNo * chunksPerPage
			pageEnd := pageStart + chunksPerPage
			if extentEndChunk <= pageStart || extentStartChunk >= pageEnd {
				continue
			}
			overlapStart := maxUint64(extentStartChunk, pageStart)
			overlapEnd := minUint64(extentEndChunk, pageEnd)
			for logicalChunk := overlapStart; logicalChunk < overlapEnd; logicalChunk++ {
				if mapping.ChunkID == 0 {
					continue
				}
				physicalChunkIDs[logicalChunk-pageStartChunk] = mapping.ChunkID + (logicalChunk - extentStartChunk)
			}
		}
		extents := make([]metadata.AllocationExtentRecord, 0, len(physicalChunkIDs))
		for i := 0; i < len(physicalChunkIDs); {
			start := pageStartChunk + uint64(i)
			if physicalChunkIDs[i] == 0 {
				j := i + 1
				for j < len(physicalChunkIDs) && physicalChunkIDs[j] == 0 {
					j++
				}
				extents = append(extents, metadata.AllocationExtentRecord{
					LogicalChunkStart: start,
					ChunkCount:        uint32(j - i),
					Kind:              metadata.AllocationKindZero,
				})
				i = j
				continue
			}
			j := i + 1
			for j < len(physicalChunkIDs) && physicalChunkIDs[j] == physicalChunkIDs[j-1]+1 {
				j++
			}
			extents = append(extents, metadata.AllocationExtentRecord{
				LogicalChunkStart:  start,
				ChunkCount:         uint32(j - i),
				Kind:               metadata.AllocationKindData,
				PhysicalChunkStart: physicalChunkIDs[i],
			})
			i = j
		}
		out = append(out, metadata.AllocationPageRecord{
			VolumeID:       f.volume.VolumeID,
			PageNo:         pageNo,
			PageBytes:      pageBytes,
			ChunkSizeBytes: chunkSizeBytes,
			Extents:        extents,
		})
	}
	return out, nil
}

func (f *fakeStore) ListSnapshotRecords(_ context.Context, sourceVolumeID string, includeDeleted bool) ([]metadata.SnapshotRecord, error) {
	out := make([]metadata.SnapshotRecord, 0, len(f.snapshotRecords))
	for _, rec := range f.snapshotRecords {
		if rec.SourceVolumeID != sourceVolumeID {
			continue
		}
		if !includeDeleted && rec.State == metadata.SnapshotStateDeleted {
			continue
		}
		out = append(out, rec)
	}
	return out, nil
}

func (f *fakeStore) ListSnapshotAllocationPages(_ context.Context, snapshotID string) ([]metadata.AllocationPageRecord, error) {
	return append([]metadata.AllocationPageRecord(nil), f.snapshotAllocationPages[snapshotID]...), nil
}

func (f *fakeStore) ListCloneRecords(_ context.Context, sourceSnapshotID, sourceVolumeID string, includeDeleted bool) ([]metadata.CloneRecord, error) {
	out := make([]metadata.CloneRecord, 0, len(f.cloneRecords))
	for _, rec := range f.cloneRecords {
		if sourceSnapshotID != "" && rec.SourceSnapshotID != sourceSnapshotID {
			continue
		}
		if sourceVolumeID != "" && rec.SourceVolumeID != sourceVolumeID {
			continue
		}
		if !includeDeleted && rec.State == metadata.CloneStateDeleted {
			continue
		}
		out = append(out, rec)
	}
	return out, nil
}

func (f *fakeStore) ListCloneDeltaAllocationPages(_ context.Context, cloneID string) ([]metadata.AllocationPageRecord, error) {
	return append([]metadata.AllocationPageRecord(nil), f.cloneDeltaAllocationPages[cloneID]...), nil
}

func (f *fakeStore) ListReplicaSets(_ context.Context, _ string) ([]metadata.ReplicaSetState, error) {
	f.listReplicaSetsCalls++
	return append([]metadata.ReplicaSetState(nil), f.replicaSets...), nil
}

func (f *fakeStore) GetReplicaSet(_ context.Context, _, replicaSetID string) (metadata.ReplicaSetState, error) {
	for _, rec := range f.replicaSets {
		if rec.ReplicaSetID == replicaSetID {
			return rec, nil
		}
	}
	return metadata.ReplicaSetState{}, metadata.ErrNotFound
}

func (f *fakeStore) PutReplicaSet(_ context.Context, rec metadata.ReplicaSetState) error {
	for i := range f.replicaSets {
		if f.replicaSets[i].ReplicaSetID == rec.ReplicaSetID {
			f.replicaSets[i] = rec
			return nil
		}
	}
	f.replicaSets = append(f.replicaSets, rec)
	return nil
}

func (f *fakeStore) GetNodeMembership(_ context.Context, nodeID string) (metadata.NodeMembershipRecord, error) {
	f.getNodeMembershipCalls++
	rec, ok := f.nodes[nodeID]
	if !ok {
		return metadata.NodeMembershipRecord{}, metadata.ErrNotFound
	}
	return rec, nil
}

func (f *fakeStore) ListNodeMemberships(_ context.Context) ([]metadata.NodeMembershipRecord, error) {
	out := make([]metadata.NodeMembershipRecord, 0, len(f.nodes))
	for _, rec := range f.nodes {
		out = append(out, rec)
	}
	return out, nil
}

func (f *fakeStore) GetNodeHealthDetail(_ context.Context, nodeID string) (metadata.NodeHealthDetailRecord, error) {
	rec, ok := f.nodeHealthDetails[nodeID]
	if !ok {
		return metadata.NodeHealthDetailRecord{}, metadata.ErrNotFound
	}
	return rec, nil
}

func (f *fakeStore) CommitPrimaryFailover(_ context.Context, req metadata.CommitPrimaryFailoverRequest) (metadata.VolumeState, metadata.ReplicaSetState, error) {
	if f.volume.Epoch != req.ExpectedVolumeEpoch {
		return metadata.VolumeState{}, metadata.ReplicaSetState{}, metadata.ErrCASConflict
	}
	for i := range f.replicaSets {
		if f.replicaSets[i].ReplicaSetID != req.ReplicaSetID {
			continue
		}
		if f.replicaSets[i].Epoch != req.ExpectedReplicaSetEpoch || f.replicaSets[i].PrimaryReplicaID != req.ExpectedPrimaryReplicaID {
			return metadata.VolumeState{}, metadata.ReplicaSetState{}, metadata.ErrCASConflict
		}
		f.volume.Epoch++
		f.replicaSets[i].Epoch++
		f.replicaSets[i].PrimaryReplicaID = req.NewPrimaryReplicaID
		for j := range f.replicaSets[i].Replicas {
			switch f.replicaSets[i].Replicas[j].ReplicaID {
			case req.NewPrimaryReplicaID:
				f.replicaSets[i].Replicas[j].Role = metadata.ReplicaRolePrimary
			case req.ExpectedPrimaryReplicaID:
				f.replicaSets[i].Replicas[j].Role = metadata.ReplicaRoleSecondary
			}
		}
		return f.volume, f.replicaSets[i], nil
	}
	return metadata.VolumeState{}, metadata.ReplicaSetState{}, metadata.ErrNotFound
}

func (f *fakeStore) PutPlacementTransition(_ context.Context, rec metadata.PlacementTransitionRecord) error {
	if _, exists := f.transitions[rec.PlacementRef]; !exists {
		f.transitionPutOrder = append(f.transitionPutOrder, rec.PlacementRef)
	}
	f.transitions[rec.PlacementRef] = rec
	return nil
}

func (f *fakeStore) GetPlacementTransition(_ context.Context, _, placementRef string) (metadata.PlacementTransitionRecord, error) {
	rec, ok := f.transitions[placementRef]
	if !ok {
		return metadata.PlacementTransitionRecord{}, metadata.ErrNotFound
	}
	return rec, nil
}

func (f *fakeStore) ListPlacementTransitions(_ context.Context, _ string) ([]metadata.PlacementTransitionRecord, error) {
	if f.transitionListOverride != nil && f.transitionListOverrideCount != 0 {
		out := append([]metadata.PlacementTransitionRecord(nil), f.transitionListOverride...)
		if f.transitionListOverrideCount > 0 {
			f.transitionListOverrideCount--
		}
		return out, nil
	}
	out := make([]metadata.PlacementTransitionRecord, 0, len(f.transitions))
	for _, rec := range f.transitions {
		out = append(out, rec)
	}
	return out, nil
}

func (f *fakeStore) ListMutationOperations(_ context.Context, _ string) ([]metadata.MutationOperationRecord, error) {
	out := make([]metadata.MutationOperationRecord, 0, len(f.mutationOps))
	for _, rec := range f.mutationOps {
		out = append(out, rec)
	}
	return out, nil
}

func (f *fakeStore) PutMutationOperation(_ context.Context, rec metadata.MutationOperationRecord) error {
	f.mutationOps[rec.OperationID] = rec
	return nil
}

func (f *fakeStore) GetMutationOperation(_ context.Context, _, operationID string) (metadata.MutationOperationRecord, error) {
	rec, ok := f.mutationOps[operationID]
	if !ok {
		return metadata.MutationOperationRecord{}, metadata.ErrNotFound
	}
	return rec, nil
}

func TestLoadTransitionAllocationViewUsesCompatiblePagesFromVolumeSpec(t *testing.T) {
	ctx := context.Background()
	store := newFakeStore()
	store.allocationPages = nil
	store.volumeSpec = metadata.VolumeSpecRecord{
		VolumeID:        "00a1b2c3",
		SizeBytes:       8,
		BlockSize:       4,
		ChunkSizeBytes:  4,
		ExtentPageBytes: 8,
	}
	store.mappings = []metadata.ExtentMappingRecord{
		{VolumeID: "00a1b2c3", ExtentID: 1, LogicalOffset: 0, LengthBytes: 4, ChunkID: 101, PlacementRef: "pl-1", Revision: 11},
		{VolumeID: "00a1b2c3", ExtentID: 2, LogicalOffset: 4, LengthBytes: 4, ChunkID: 0, PlacementRef: "pl-2", Revision: 11},
	}

	svc := NewService(store)
	view, err := svc.loadTransitionAllocationView(ctx, "00a1b2c3")
	if err != nil {
		t.Fatalf("loadTransitionAllocationView: %v", err)
	}
	if !view.enabled {
		t.Fatalf("expected allocation view to be enabled")
	}
	if store.listCompatibleCalls != 1 {
		t.Fatalf("listCompatibleCalls=%d want=1", store.listCompatibleCalls)
	}
	pages := view.allocationPagesForMapping(store.mappings[0])
	if len(pages) != 1 {
		t.Fatalf("allocation pages=%d want=1", len(pages))
	}
	if len(pages[0].Page.Extents) != 2 {
		t.Fatalf("extents=%+v", pages[0].Page.Extents)
	}
	if pages[0].Page.Extents[0].Kind != metadata.AllocationKindData || pages[0].Page.Extents[0].PhysicalChunkStart != 101 {
		t.Fatalf("first extent=%+v", pages[0].Page.Extents[0])
	}
	if pages[0].Page.Extents[1].Kind != metadata.AllocationKindZero {
		t.Fatalf("second extent=%+v", pages[0].Page.Extents[1])
	}
}

func TestMaterializeLegacyAllocationViewUsesInjectedPlacementApply(t *testing.T) {
	ctx := context.Background()
	store := newFakeStore()
	placementApply := &recordingPlacementApplyRunner{}
	svc := NewServiceWithPlacementApply(store, placementApply)

	err := svc.materializeLegacyAllocationView(ctx, transitionAllocationView{
		enabled:        true,
		volumeID:       "00a1b2c3",
		pageBytes:      4096,
		chunkSizeBytes: 1024,
		pagesByNo: map[uint64]metadata.AllocationPageRecord{
			2: {PageNo: 2, PageBytes: 4096, ChunkSizeBytes: 1024},
			1: {PageNo: 1, PageBytes: 4096, ChunkSizeBytes: 1024},
		},
	}, map[uint64]metadata.ExtentMappingRecord{
		9: {VolumeID: "00a1b2c3", ExtentID: 9, ChunkID: 0, Revision: 20},
		7: {VolumeID: "00a1b2c3", ExtentID: 7, ChunkID: 707, Revision: 10},
		8: {VolumeID: "00a1b2c3", ExtentID: 8, ChunkID: 808, Revision: 10},
	}, 20)
	if err != nil {
		t.Fatalf("materializeLegacyAllocationView: %v", err)
	}
	if placementApply.calls != 1 {
		t.Fatalf("placement apply calls=%d want=1", placementApply.calls)
	}
	if placementApply.req.VolumeID != "00a1b2c3" || placementApply.req.CommittedRevision != 20 {
		t.Fatalf("unexpected request: %+v", placementApply.req)
	}
	if len(placementApply.req.AllocationPages) != 2 ||
		placementApply.req.AllocationPages[0].PageNo != 1 ||
		placementApply.req.AllocationPages[1].PageNo != 2 {
		t.Fatalf("allocation pages not sorted: %+v", placementApply.req.AllocationPages)
	}
	if len(placementApply.req.NormalizeExtentIDs) != 2 ||
		placementApply.req.NormalizeExtentIDs[0] != 7 ||
		placementApply.req.NormalizeExtentIDs[1] != 8 {
		t.Fatalf("normalize extent IDs=%+v want [7 8]", placementApply.req.NormalizeExtentIDs)
	}
}

func TestEvaluateExtentHealthHealthy(t *testing.T) {
	store := newFakeStore()
	svc := NewService(store)

	evaluated, err := svc.EvaluateExtentHealth(context.Background(), "00a1b2c3", 1)
	if err != nil {
		t.Fatalf("EvaluateExtentHealth: %v", err)
	}
	if evaluated.State != ExtentHealthHealthy {
		t.Fatalf("state=%q want=%q", evaluated.State, ExtentHealthHealthy)
	}
	if !evaluated.DataPresent || evaluated.ZeroOnly || evaluated.DataBytes != 8 {
		t.Fatalf("evaluated=%+v", evaluated)
	}
}

func TestEvaluateExtentHealthDegradedWritable(t *testing.T) {
	store := newFakeStore()
	store.nodes["node-c"] = metadata.NodeMembershipRecord{NodeID: "node-c", LifecycleState: metadata.NodeLifecycleActive, HealthState: metadata.NodeHealthDown}
	svc := NewService(store)

	evaluated, err := svc.EvaluateExtentHealth(context.Background(), "00a1b2c3", 1)
	if err != nil {
		t.Fatalf("EvaluateExtentHealth: %v", err)
	}
	if evaluated.State != ExtentHealthDegradedWritable {
		t.Fatalf("state=%q want=%q", evaluated.State, ExtentHealthDegradedWritable)
	}
}

func TestEvaluateExtentHealthBlocked(t *testing.T) {
	store := newFakeStore()
	store.nodes["node-b"] = metadata.NodeMembershipRecord{NodeID: "node-b", LifecycleState: metadata.NodeLifecycleActive, HealthState: metadata.NodeHealthDown}
	store.nodes["node-c"] = metadata.NodeMembershipRecord{NodeID: "node-c", LifecycleState: metadata.NodeLifecycleActive, HealthState: metadata.NodeHealthDown}
	svc := NewService(store)

	evaluated, err := svc.EvaluateExtentHealth(context.Background(), "00a1b2c3", 1)
	if err != nil {
		t.Fatalf("EvaluateExtentHealth: %v", err)
	}
	if evaluated.State != ExtentHealthBlocked {
		t.Fatalf("state=%q want=%q", evaluated.State, ExtentHealthBlocked)
	}
}

func TestEvaluateExtentHealthZeroOnlyTreatsExtentAsRecoverable(t *testing.T) {
	store := newFakeStore()
	store.volumeSpec = metadata.VolumeSpecRecord{
		VolumeID:        "00a1b2c3",
		SizeBytes:       8,
		BlockSize:       4,
		ChunkSizeBytes:  4,
		ExtentPageBytes: 8,
	}
	store.mappings = []metadata.ExtentMappingRecord{
		{VolumeID: "00a1b2c3", ExtentID: 1, LogicalOffset: 0, LengthBytes: 8, ChunkID: 0, PlacementRef: "pl-1", Revision: 11},
	}
	store.nodes["node-a"] = metadata.NodeMembershipRecord{NodeID: "node-a", LifecycleState: metadata.NodeLifecycleActive, HealthState: metadata.NodeHealthDown}
	store.nodes["node-b"] = metadata.NodeMembershipRecord{NodeID: "node-b", LifecycleState: metadata.NodeLifecycleActive, HealthState: metadata.NodeHealthDown}
	store.nodes["node-c"] = metadata.NodeMembershipRecord{NodeID: "node-c", LifecycleState: metadata.NodeLifecycleActive, HealthState: metadata.NodeHealthDown}
	svc := NewService(store)

	evaluated, err := svc.EvaluateExtentHealth(context.Background(), "00a1b2c3", 1)
	if err != nil {
		t.Fatalf("EvaluateExtentHealth: %v", err)
	}
	if evaluated.State != ExtentHealthDegradedWritable {
		t.Fatalf("state=%q want=%q", evaluated.State, ExtentHealthDegradedWritable)
	}
	if !evaluated.ZeroOnly || evaluated.DataPresent {
		t.Fatalf("evaluated=%+v", evaluated)
	}
	if evaluated.DataBytes != 0 || evaluated.DataChunks != 0 {
		t.Fatalf("evaluated=%+v", evaluated)
	}
}

func TestEvaluateExtentHealthMixedAllocationTracksDataBytesAndChunks(t *testing.T) {
	store := newFakeStore()
	store.volumeSpec = metadata.VolumeSpecRecord{
		VolumeID:        "00a1b2c3",
		SizeBytes:       16,
		BlockSize:       4,
		ChunkSizeBytes:  4,
		ExtentPageBytes: 16,
	}
	store.mappings = []metadata.ExtentMappingRecord{
		{VolumeID: "00a1b2c3", ExtentID: 1, LogicalOffset: 0, LengthBytes: 16, ChunkID: 101, PlacementRef: "pl-1", Revision: 11},
	}
	store.allocationPages = []metadata.AllocationPageRecord{
		{
			VolumeID:       "00a1b2c3",
			PageNo:         0,
			PageBytes:      16,
			ChunkSizeBytes: 4,
			Extents: []metadata.AllocationExtentRecord{
				{LogicalChunkStart: 0, ChunkCount: 1, Kind: metadata.AllocationKindData, PhysicalChunkStart: 101},
				{LogicalChunkStart: 1, ChunkCount: 2, Kind: metadata.AllocationKindZero},
				{LogicalChunkStart: 3, ChunkCount: 1, Kind: metadata.AllocationKindData, PhysicalChunkStart: 104},
			},
		},
	}
	svc := NewService(store)

	evaluated, err := svc.EvaluateExtentHealth(context.Background(), "00a1b2c3", 1)
	if err != nil {
		t.Fatalf("EvaluateExtentHealth: %v", err)
	}
	if evaluated.ZeroOnly || !evaluated.DataPresent {
		t.Fatalf("evaluated=%+v", evaluated)
	}
	if evaluated.DataBytes != 8 || evaluated.DataChunks != 2 {
		t.Fatalf("evaluated=%+v", evaluated)
	}
}

func TestReconcileVolumeStatus(t *testing.T) {
	store := newFakeStore()
	store.nodes["node-c"] = metadata.NodeMembershipRecord{NodeID: "node-c", LifecycleState: metadata.NodeLifecycleActive, HealthState: metadata.NodeHealthDown}
	svc := NewService(store)

	volume, err := svc.ReconcileVolumeStatus(context.Background(), "00a1b2c3")
	if err != nil {
		t.Fatalf("ReconcileVolumeStatus: %v", err)
	}
	if volume.Status != metadata.VolumeStatusDegraded {
		t.Fatalf("volume status=%q want=%q", volume.Status, metadata.VolumeStatusDegraded)
	}
}

func TestReconcileVolumeStatusZeroOnlyExtentAvoidsBlocked(t *testing.T) {
	store := newFakeStore()
	store.volumeSpec = metadata.VolumeSpecRecord{
		VolumeID:        "00a1b2c3",
		SizeBytes:       8,
		BlockSize:       4,
		ChunkSizeBytes:  4,
		ExtentPageBytes: 8,
	}
	store.mappings = []metadata.ExtentMappingRecord{
		{VolumeID: "00a1b2c3", ExtentID: 1, LogicalOffset: 0, LengthBytes: 8, ChunkID: 0, PlacementRef: "pl-1", Revision: 11},
	}
	store.nodes["node-a"] = metadata.NodeMembershipRecord{NodeID: "node-a", LifecycleState: metadata.NodeLifecycleActive, HealthState: metadata.NodeHealthDown}
	store.nodes["node-b"] = metadata.NodeMembershipRecord{NodeID: "node-b", LifecycleState: metadata.NodeLifecycleActive, HealthState: metadata.NodeHealthDown}
	store.nodes["node-c"] = metadata.NodeMembershipRecord{NodeID: "node-c", LifecycleState: metadata.NodeLifecycleActive, HealthState: metadata.NodeHealthDown}
	svc := NewService(store)

	volume, err := svc.ReconcileVolumeStatus(context.Background(), "00a1b2c3")
	if err != nil {
		t.Fatalf("ReconcileVolumeStatus: %v", err)
	}
	if volume.Status != metadata.VolumeStatusDegraded {
		t.Fatalf("volume status=%q want=%q", volume.Status, metadata.VolumeStatusDegraded)
	}
}

func TestEnqueueRepairAndRebalance(t *testing.T) {
	store := newFakeStore()
	svc := NewService(store)
	svc.now = func() time.Time { return time.Unix(1000, 0) }

	repair, err := svc.EnqueueRepair(context.Background(), "00a1b2c3", 1, "rs-repair")
	if err != nil {
		t.Fatalf("EnqueueRepair: %v", err)
	}
	if repair.Reason != "repair" || repair.State != metadata.PlacementTransitionQueued {
		t.Fatalf("repair=%+v", repair)
	}

	rebalance, err := svc.EnqueueRebalance(context.Background(), "00a1b2c3", 1, "rs-rebalance")
	if err != nil {
		t.Fatalf("EnqueueRebalance: %v", err)
	}
	if rebalance.Reason != "rebalance" || rebalance.State != metadata.PlacementTransitionQueued {
		t.Fatalf("rebalance=%+v", rebalance)
	}
}

func TestScanAndEnqueueRepairsOnDegradedExtent(t *testing.T) {
	store := newFakeStore()
	store.nodes["node-c"] = metadata.NodeMembershipRecord{NodeID: "node-c", LifecycleState: metadata.NodeLifecycleActive, HealthState: metadata.NodeHealthDown}
	store.replicaSets = append(store.replicaSets, metadata.ReplicaSetState{
		ReplicaSetID:     "rs-2",
		VolumeID:         "00a1b2c3",
		PlacementRef:     "pl-2",
		Epoch:            5,
		PrimaryReplicaID: "rep-d",
		WriteQuorum:      2,
		ReadQuorum:       1,
		Replicas: []metadata.ReplicaDescriptor{
			{NodeID: "node-d", ReplicaID: "rep-d", Role: metadata.ReplicaRolePrimary},
			{NodeID: "node-e", ReplicaID: "rep-e", Role: metadata.ReplicaRoleSecondary},
			{NodeID: "node-f", ReplicaID: "rep-f", Role: metadata.ReplicaRoleSecondary},
		},
	})
	store.nodes["node-d"] = metadata.NodeMembershipRecord{NodeID: "node-d", LifecycleState: metadata.NodeLifecycleActive, HealthState: metadata.NodeHealthHealthy}
	store.nodes["node-e"] = metadata.NodeMembershipRecord{NodeID: "node-e", LifecycleState: metadata.NodeLifecycleActive, HealthState: metadata.NodeHealthHealthy}
	store.nodes["node-f"] = metadata.NodeMembershipRecord{NodeID: "node-f", LifecycleState: metadata.NodeLifecycleActive, HealthState: metadata.NodeHealthHealthy}

	svc := NewService(store)
	svc.now = func() time.Time { return time.Unix(1000, 0) }

	enqueued, err := svc.ScanAndEnqueueRepairs(context.Background(), "00a1b2c3")
	if err != nil {
		t.Fatalf("ScanAndEnqueueRepairs: %v", err)
	}
	if enqueued != len(store.mappings) {
		t.Fatalf("enqueued=%d want=%d", enqueued, len(store.mappings))
	}
	transition := store.transitions["pl-1"]
	if transition.State != metadata.PlacementTransitionQueued || transition.TargetReplicaSetID != "rs-1-repair-node-c" {
		t.Fatalf("transition=%+v", transition)
	}
	target, err := store.GetReplicaSet(context.Background(), "00a1b2c3", "rs-1-repair-node-c")
	if err != nil {
		t.Fatalf("repair target replica set: %v", err)
	}
	if target.PlacementRef != "pl-1-repair-node-c" || target.Replicas[2].NodeID != "node-d" {
		t.Fatalf("target replica set=%+v", target)
	}
}

func TestScanAndEnqueueRepairsReusesLoadedMetadata(t *testing.T) {
	store := newFakeStore()
	store.nodes["node-c"] = metadata.NodeMembershipRecord{NodeID: "node-c", LifecycleState: metadata.NodeLifecycleActive, HealthState: metadata.NodeHealthDown}
	store.replicaSets = append(store.replicaSets, metadata.ReplicaSetState{
		ReplicaSetID:     "rs-2",
		VolumeID:         "00a1b2c3",
		PlacementRef:     "pl-2",
		Epoch:            5,
		PrimaryReplicaID: "rep-d",
		WriteQuorum:      2,
		ReadQuorum:       1,
		Replicas: []metadata.ReplicaDescriptor{
			{NodeID: "node-d", ReplicaID: "rep-d", Role: metadata.ReplicaRolePrimary},
			{NodeID: "node-e", ReplicaID: "rep-e", Role: metadata.ReplicaRoleSecondary},
			{NodeID: "node-f", ReplicaID: "rep-f", Role: metadata.ReplicaRoleSecondary},
		},
	})
	store.nodes["node-d"] = metadata.NodeMembershipRecord{NodeID: "node-d", LifecycleState: metadata.NodeLifecycleActive, HealthState: metadata.NodeHealthHealthy}
	store.nodes["node-e"] = metadata.NodeMembershipRecord{NodeID: "node-e", LifecycleState: metadata.NodeLifecycleActive, HealthState: metadata.NodeHealthHealthy}
	store.nodes["node-f"] = metadata.NodeMembershipRecord{NodeID: "node-f", LifecycleState: metadata.NodeLifecycleActive, HealthState: metadata.NodeHealthHealthy}
	store.mappings = nil
	for i := 0; i < 128; i++ {
		store.mappings = append(store.mappings, metadata.ExtentMappingRecord{
			VolumeID:      "00a1b2c3",
			ExtentID:      uint64(i + 1),
			LogicalOffset: uint64(i) * 4096,
			LengthBytes:   4096,
			ChunkID:       uint64(i + 1),
			PlacementRef:  "pl-1",
			Revision:      11,
		})
	}

	svc := NewService(store)
	svc.now = func() time.Time { return time.Unix(1000, 0) }

	enqueued, err := svc.ScanAndEnqueueRepairs(context.Background(), "00a1b2c3")
	if err != nil {
		t.Fatalf("ScanAndEnqueueRepairs: %v", err)
	}
	if enqueued != len(store.mappings) {
		t.Fatalf("enqueued=%d want=%d", enqueued, len(store.mappings))
	}
	if store.listExtentMappingsCalls > 2 {
		t.Fatalf("ListExtentMappings calls=%d want<=2", store.listExtentMappingsCalls)
	}
	if store.listReplicaSetsCalls > 1 {
		t.Fatalf("ListReplicaSets calls=%d want<=1", store.listReplicaSetsCalls)
	}
	if store.getNodeMembershipCalls > 6 {
		t.Fatalf("GetNodeMembership calls=%d want<=6", store.getNodeMembershipCalls)
	}
}

func TestScanAndEnqueueRepairsPrioritizesZeroOnlyExtent(t *testing.T) {
	store := newFakeStore()
	store.volumeSpec = metadata.VolumeSpecRecord{
		VolumeID:        "00a1b2c3",
		ChunkSizeBytes:  4,
		ExtentPageBytes: 8,
	}
	store.nodes["node-c"] = metadata.NodeMembershipRecord{NodeID: "node-c", LifecycleState: metadata.NodeLifecycleActive, HealthState: metadata.NodeHealthDown}
	store.nodes["node-d"] = metadata.NodeMembershipRecord{NodeID: "node-d", LifecycleState: metadata.NodeLifecycleActive, HealthState: metadata.NodeHealthHealthy}
	store.nodes["node-e"] = metadata.NodeMembershipRecord{NodeID: "node-e", LifecycleState: metadata.NodeLifecycleActive, HealthState: metadata.NodeHealthHealthy}
	store.nodes["node-f"] = metadata.NodeMembershipRecord{NodeID: "node-f", LifecycleState: metadata.NodeLifecycleActive, HealthState: metadata.NodeHealthHealthy}
	store.mappings = []metadata.ExtentMappingRecord{
		{VolumeID: "00a1b2c3", ExtentID: 1, LogicalOffset: 0, LengthBytes: 8, ChunkID: 101, PlacementRef: "pl-1", Revision: 11},
		{VolumeID: "00a1b2c3", ExtentID: 2, LogicalOffset: 8, LengthBytes: 8, ChunkID: 102, PlacementRef: "pl-2", Revision: 11},
	}
	store.replicaSets = []metadata.ReplicaSetState{
		{
			ReplicaSetID:     "rs-1",
			VolumeID:         "00a1b2c3",
			PlacementRef:     "pl-1",
			Epoch:            5,
			PrimaryReplicaID: "rep-a",
			WriteQuorum:      2,
			ReadQuorum:       1,
			Replicas: []metadata.ReplicaDescriptor{
				{NodeID: "node-a", ReplicaID: "rep-a", Role: metadata.ReplicaRolePrimary},
				{NodeID: "node-b", ReplicaID: "rep-b", Role: metadata.ReplicaRoleSecondary},
				{NodeID: "node-c", ReplicaID: "rep-c", Role: metadata.ReplicaRoleSecondary},
			},
		},
		{
			ReplicaSetID:     "rs-2",
			VolumeID:         "00a1b2c3",
			PlacementRef:     "pl-2",
			Epoch:            5,
			PrimaryReplicaID: "rep-d",
			WriteQuorum:      2,
			ReadQuorum:       1,
			Replicas: []metadata.ReplicaDescriptor{
				{NodeID: "node-a", ReplicaID: "rep-d", Role: metadata.ReplicaRolePrimary},
				{NodeID: "node-b", ReplicaID: "rep-e", Role: metadata.ReplicaRoleSecondary},
				{NodeID: "node-c", ReplicaID: "rep-f", Role: metadata.ReplicaRoleSecondary},
			},
		},
	}
	store.allocationPages = []metadata.AllocationPageRecord{
		{
			VolumeID:       "00a1b2c3",
			PageNo:         0,
			PageBytes:      8,
			ChunkSizeBytes: 4,
			Revision:       11,
			Extents: []metadata.AllocationExtentRecord{
				{LogicalChunkStart: 0, ChunkCount: 2, Kind: metadata.AllocationKindZero},
			},
		},
		{
			VolumeID:       "00a1b2c3",
			PageNo:         1,
			PageBytes:      8,
			ChunkSizeBytes: 4,
			Revision:       11,
			Extents: []metadata.AllocationExtentRecord{
				{LogicalChunkStart: 2, PhysicalChunkStart: 600, ChunkCount: 2, Kind: metadata.AllocationKindData},
			},
		},
	}

	svc := NewService(store)
	svc.now = func() time.Time { return time.Unix(1000, 0) }

	if _, err := svc.ScanAndEnqueueRepairs(context.Background(), "00a1b2c3"); err != nil {
		t.Fatalf("ScanAndEnqueueRepairs: %v", err)
	}
	if len(store.transitionPutOrder) < 2 {
		t.Fatalf("transitionPutOrder=%v want at least 2 entries", store.transitionPutOrder)
	}
	if store.transitionPutOrder[0] != "pl-1" || store.transitionPutOrder[1] != "pl-2" {
		t.Fatalf("transitionPutOrder=%v want [pl-1 pl-2 ...]", store.transitionPutOrder)
	}
}

func TestScanAndEnqueueRepairsPrioritizesRecentlyMutatedExtent(t *testing.T) {
	store := newFakeStore()
	store.volumeSpec = metadata.VolumeSpecRecord{
		VolumeID:        "00a1b2c3",
		ChunkSizeBytes:  4,
		ExtentPageBytes: 8,
	}
	store.nodes["node-c"] = metadata.NodeMembershipRecord{NodeID: "node-c", LifecycleState: metadata.NodeLifecycleActive, HealthState: metadata.NodeHealthDown}
	store.nodes["node-f"] = metadata.NodeMembershipRecord{NodeID: "node-f", LifecycleState: metadata.NodeLifecycleActive, HealthState: metadata.NodeHealthDown}
	store.nodes["node-d"] = metadata.NodeMembershipRecord{NodeID: "node-d", LifecycleState: metadata.NodeLifecycleActive, HealthState: metadata.NodeHealthHealthy}
	store.nodes["node-e"] = metadata.NodeMembershipRecord{NodeID: "node-e", LifecycleState: metadata.NodeLifecycleActive, HealthState: metadata.NodeHealthHealthy}
	store.nodes["node-g"] = metadata.NodeMembershipRecord{NodeID: "node-g", LifecycleState: metadata.NodeLifecycleActive, HealthState: metadata.NodeHealthHealthy}
	store.nodes["node-h"] = metadata.NodeMembershipRecord{NodeID: "node-h", LifecycleState: metadata.NodeLifecycleActive, HealthState: metadata.NodeHealthHealthy}
	store.mappings = []metadata.ExtentMappingRecord{
		{VolumeID: "00a1b2c3", ExtentID: 1, LogicalOffset: 0, LengthBytes: 8, ChunkID: 101, PlacementRef: "pl-1", Revision: 11},
		{VolumeID: "00a1b2c3", ExtentID: 2, LogicalOffset: 8, LengthBytes: 8, ChunkID: 102, PlacementRef: "pl-2", Revision: 11},
	}
	store.replicaSets = []metadata.ReplicaSetState{
		{
			ReplicaSetID:     "rs-1",
			VolumeID:         "00a1b2c3",
			PlacementRef:     "pl-1",
			Epoch:            5,
			PrimaryReplicaID: "rep-a",
			WriteQuorum:      2,
			ReadQuorum:       1,
			Replicas: []metadata.ReplicaDescriptor{
				{NodeID: "node-a", ReplicaID: "rep-a", Role: metadata.ReplicaRolePrimary},
				{NodeID: "node-b", ReplicaID: "rep-b", Role: metadata.ReplicaRoleSecondary},
				{NodeID: "node-c", ReplicaID: "rep-c", Role: metadata.ReplicaRoleSecondary},
			},
		},
		{
			ReplicaSetID:     "rs-2",
			VolumeID:         "00a1b2c3",
			PlacementRef:     "pl-2",
			Epoch:            5,
			PrimaryReplicaID: "rep-d",
			WriteQuorum:      2,
			ReadQuorum:       1,
			Replicas: []metadata.ReplicaDescriptor{
				{NodeID: "node-d", ReplicaID: "rep-d", Role: metadata.ReplicaRolePrimary},
				{NodeID: "node-e", ReplicaID: "rep-e", Role: metadata.ReplicaRoleSecondary},
				{NodeID: "node-f", ReplicaID: "rep-f", Role: metadata.ReplicaRoleSecondary},
			},
		},
	}
	store.mutationOps["write-recent"] = metadata.MutationOperationRecord{
		OperationID:       "write-recent",
		VolumeID:          "00a1b2c3",
		Kind:              "write",
		State:             metadata.MutationOperationCommitted,
		AffectedExtentIDs: []uint64{2},
		LastUpdatedAtUnix: 2000,
	}

	svc := NewService(store)
	svc.now = func() time.Time { return time.Unix(1000, 0) }

	if _, err := svc.ScanAndEnqueueRepairs(context.Background(), "00a1b2c3"); err != nil {
		t.Fatalf("ScanAndEnqueueRepairs: %v", err)
	}
	if len(store.transitionPutOrder) < 2 {
		t.Fatalf("transitionPutOrder=%v want at least 2 entries", store.transitionPutOrder)
	}
	if store.transitionPutOrder[0] != "pl-2" || store.transitionPutOrder[1] != "pl-1" {
		t.Fatalf("transitionPutOrder=%v want [pl-2 pl-1 ...]", store.transitionPutOrder)
	}
}

func TestScanAndEnqueueRepairsPrioritizesIncompleteTransitionExtent(t *testing.T) {
	store := newFakeStore()
	store.volumeSpec = metadata.VolumeSpecRecord{
		VolumeID:        "00a1b2c3",
		ChunkSizeBytes:  4,
		ExtentPageBytes: 8,
	}
	store.nodes["node-c"] = metadata.NodeMembershipRecord{NodeID: "node-c", LifecycleState: metadata.NodeLifecycleActive, HealthState: metadata.NodeHealthDown}
	store.nodes["node-f"] = metadata.NodeMembershipRecord{NodeID: "node-f", LifecycleState: metadata.NodeLifecycleActive, HealthState: metadata.NodeHealthDown}
	store.nodes["node-d"] = metadata.NodeMembershipRecord{NodeID: "node-d", LifecycleState: metadata.NodeLifecycleActive, HealthState: metadata.NodeHealthHealthy}
	store.nodes["node-e"] = metadata.NodeMembershipRecord{NodeID: "node-e", LifecycleState: metadata.NodeLifecycleActive, HealthState: metadata.NodeHealthHealthy}
	store.nodes["node-g"] = metadata.NodeMembershipRecord{NodeID: "node-g", LifecycleState: metadata.NodeLifecycleActive, HealthState: metadata.NodeHealthHealthy}
	store.nodes["node-h"] = metadata.NodeMembershipRecord{NodeID: "node-h", LifecycleState: metadata.NodeLifecycleActive, HealthState: metadata.NodeHealthHealthy}
	store.mappings = []metadata.ExtentMappingRecord{
		{VolumeID: "00a1b2c3", ExtentID: 1, LogicalOffset: 0, LengthBytes: 8, ChunkID: 101, PlacementRef: "pl-1", Revision: 11},
		{VolumeID: "00a1b2c3", ExtentID: 2, LogicalOffset: 8, LengthBytes: 8, ChunkID: 102, PlacementRef: "pl-2", Revision: 11},
	}
	store.replicaSets = []metadata.ReplicaSetState{
		{
			ReplicaSetID:     "rs-1",
			VolumeID:         "00a1b2c3",
			PlacementRef:     "pl-1",
			Epoch:            5,
			PrimaryReplicaID: "rep-a",
			WriteQuorum:      2,
			ReadQuorum:       1,
			Replicas: []metadata.ReplicaDescriptor{
				{NodeID: "node-a", ReplicaID: "rep-a", Role: metadata.ReplicaRolePrimary},
				{NodeID: "node-b", ReplicaID: "rep-b", Role: metadata.ReplicaRoleSecondary},
				{NodeID: "node-c", ReplicaID: "rep-c", Role: metadata.ReplicaRoleSecondary},
			},
		},
		{
			ReplicaSetID:     "rs-2",
			VolumeID:         "00a1b2c3",
			PlacementRef:     "pl-2",
			Epoch:            5,
			PrimaryReplicaID: "rep-d",
			WriteQuorum:      2,
			ReadQuorum:       1,
			Replicas: []metadata.ReplicaDescriptor{
				{NodeID: "node-d", ReplicaID: "rep-d", Role: metadata.ReplicaRolePrimary},
				{NodeID: "node-e", ReplicaID: "rep-e", Role: metadata.ReplicaRoleSecondary},
				{NodeID: "node-f", ReplicaID: "rep-f", Role: metadata.ReplicaRoleSecondary},
			},
		},
	}
	store.mutationOps["write-recent"] = metadata.MutationOperationRecord{
		OperationID:       "write-recent",
		VolumeID:          "00a1b2c3",
		Kind:              "write",
		State:             metadata.MutationOperationCommitted,
		AffectedExtentIDs: []uint64{2},
		AffectedPageNos:   []uint64{1},
		LastUpdatedAtUnix: 3000,
	}
	store.mutationOps["transition-incomplete"] = metadata.MutationOperationRecord{
		OperationID:       "transition-incomplete",
		VolumeID:          "00a1b2c3",
		Kind:              "transition",
		State:             metadata.MutationOperationFailed,
		AffectedExtentIDs: []uint64{1},
		AffectedPageNos:   []uint64{0},
		CompletedPageNos:  []uint64{},
		LastUpdatedAtUnix: 2000,
	}

	svc := NewService(store)
	svc.now = func() time.Time { return time.Unix(4000, 0) }

	if _, err := svc.ScanAndEnqueueRepairs(context.Background(), "00a1b2c3"); err != nil {
		t.Fatalf("ScanAndEnqueueRepairs: %v", err)
	}
	if len(store.transitionPutOrder) < 2 {
		t.Fatalf("transitionPutOrder=%v want at least 2 entries", store.transitionPutOrder)
	}
	if store.transitionPutOrder[0] != "pl-1" || store.transitionPutOrder[1] != "pl-2" {
		t.Fatalf("transitionPutOrder=%v want incomplete transition extent first", store.transitionPutOrder)
	}
}

func TestEvaluateExtentHealthTracksIncompleteTransitionBatchBacklog(t *testing.T) {
	store := newFakeStore()
	store.volumeSpec = metadata.VolumeSpecRecord{
		VolumeID:        "00a1b2c3",
		ChunkSizeBytes:  4,
		ExtentPageBytes: 8,
	}
	store.mappings = []metadata.ExtentMappingRecord{
		{VolumeID: "00a1b2c3", ExtentID: 1, LogicalOffset: 0, LengthBytes: 16, ChunkID: 0, PlacementRef: "pl-1", Revision: 11},
	}
	store.replicaSets = []metadata.ReplicaSetState{
		{
			ReplicaSetID:     "rs-1",
			VolumeID:         "00a1b2c3",
			PlacementRef:     "pl-1",
			Epoch:            5,
			PrimaryReplicaID: "rep-a",
			WriteQuorum:      2,
			ReadQuorum:       1,
			Replicas: []metadata.ReplicaDescriptor{
				{NodeID: "node-a", ReplicaID: "rep-a", Role: metadata.ReplicaRolePrimary},
				{NodeID: "node-b", ReplicaID: "rep-b", Role: metadata.ReplicaRoleSecondary},
				{NodeID: "node-c", ReplicaID: "rep-c", Role: metadata.ReplicaRoleSecondary},
			},
		},
	}
	store.nodes = map[string]metadata.NodeMembershipRecord{
		"node-a": {NodeID: "node-a", LifecycleState: metadata.NodeLifecycleActive, HealthState: metadata.NodeHealthDown},
		"node-b": {NodeID: "node-b", LifecycleState: metadata.NodeLifecycleActive, HealthState: metadata.NodeHealthDown},
		"node-c": {NodeID: "node-c", LifecycleState: metadata.NodeLifecycleActive, HealthState: metadata.NodeHealthDown},
	}
	store.allocationPages = []metadata.AllocationPageRecord{
		{
			VolumeID:       "00a1b2c3",
			PageNo:         0,
			PageBytes:      8,
			ChunkSizeBytes: 4,
			Extents: []metadata.AllocationExtentRecord{
				{LogicalChunkStart: 0, ChunkCount: 2, Kind: metadata.AllocationKindData, PhysicalChunkStart: 500},
			},
		},
		{
			VolumeID:       "00a1b2c3",
			PageNo:         1,
			PageBytes:      8,
			ChunkSizeBytes: 4,
			Extents: []metadata.AllocationExtentRecord{
				{LogicalChunkStart: 2, ChunkCount: 2, Kind: metadata.AllocationKindData, PhysicalChunkStart: 600},
			},
		},
	}
	store.mutationOps["transition-pl-1"] = metadata.MutationOperationRecord{
		OperationID:       "transition-pl-1",
		VolumeID:          "00a1b2c3",
		Kind:              "transition",
		State:             metadata.MutationOperationFailed,
		IdempotencyKey:    "pl-1",
		AffectedExtentIDs: []uint64{1},
		AffectedPageNos:   []uint64{0, 1},
		LastUpdatedAtUnix: 2000,
	}
	store.mutationOps["transition-pl-1-pages-00000000000000000000-00000000000000000000"] = metadata.MutationOperationRecord{
		OperationID:       "transition-pl-1-pages-00000000000000000000-00000000000000000000",
		VolumeID:          "00a1b2c3",
		Kind:              "transition_batch",
		State:             metadata.MutationOperationFailed,
		IdempotencyKey:    "transition-pl-1",
		AffectedExtentIDs: []uint64{1},
		AffectedPageNos:   []uint64{0},
		LastUpdatedAtUnix: 2001,
	}
	store.mutationOps["transition-pl-1-pages-00000000000000000001-00000000000000000001"] = metadata.MutationOperationRecord{
		OperationID:       "transition-pl-1-pages-00000000000000000001-00000000000000000001",
		VolumeID:          "00a1b2c3",
		Kind:              "transition_batch",
		State:             metadata.MutationOperationFailed,
		IdempotencyKey:    "transition-pl-1",
		AffectedExtentIDs: []uint64{1},
		AffectedPageNos:   []uint64{1},
		LastUpdatedAtUnix: 2002,
	}

	svc := NewService(store)
	evaluated, err := svc.EvaluateExtentHealth(context.Background(), "00a1b2c3", 1)
	if err != nil {
		t.Fatalf("EvaluateExtentHealth: %v", err)
	}
	if !evaluated.IncompleteTransition || evaluated.IncompleteBatches != 2 {
		t.Fatalf("incomplete transition=%v batches=%d want=true/2", evaluated.IncompleteTransition, evaluated.IncompleteBatches)
	}
	if evaluated.IncompletePages != 2 {
		t.Fatalf("incomplete pages=%d want=2", evaluated.IncompletePages)
	}
	if evaluated.IncompleteBytes != 16 || evaluated.IncompleteChunks != 4 {
		t.Fatalf("incomplete bytes=%d chunks=%d want=16/4", evaluated.IncompleteBytes, evaluated.IncompleteChunks)
	}
}

func TestCompareEvaluatedExtentPriorityPrefersSmallerIncompleteBatchBacklog(t *testing.T) {
	left := &EvaluatedExtent{
		Extent:               metadata.ExtentMappingRecord{ExtentID: 1},
		IncompleteTransition: true,
		IncompleteUpdatedAt:  2000,
		RetryWindowCount:     2,
		RetryWindowBytes:     16,
		RetryWindowChunks:    4,
		IncompleteBatches:    2,
		IncompleteBytes:      16,
		IncompleteChunks:     4,
		IncompletePages:      2,
	}
	right := &EvaluatedExtent{
		Extent:               metadata.ExtentMappingRecord{ExtentID: 2},
		IncompleteTransition: true,
		IncompleteUpdatedAt:  2000,
		RetryWindowCount:     1,
		RetryWindowBytes:     8,
		RetryWindowChunks:    2,
		IncompleteBatches:    1,
		IncompleteBytes:      16,
		IncompleteChunks:     4,
		IncompletePages:      2,
	}
	if got := compareEvaluatedExtentPriority(left, right); got <= 0 {
		t.Fatalf("compare left vs right=%d want >0 because smaller retry window cost should win", got)
	}
	if got := compareEvaluatedExtentPriority(right, left); got >= 0 {
		t.Fatalf("compare right vs left=%d want <0 because smaller retry window cost should win", got)
	}
}

func TestScanAndEnqueueRepairsPrioritizesSmallerRetryWindowCost(t *testing.T) {
	store := newFakeStore()
	store.volumeSpec = metadata.VolumeSpecRecord{
		VolumeID:        "00a1b2c3",
		ChunkSizeBytes:  4,
		ExtentPageBytes: 8,
	}
	store.nodes["node-c"] = metadata.NodeMembershipRecord{NodeID: "node-c", LifecycleState: metadata.NodeLifecycleActive, HealthState: metadata.NodeHealthDown}
	store.nodes["node-f"] = metadata.NodeMembershipRecord{NodeID: "node-f", LifecycleState: metadata.NodeLifecycleActive, HealthState: metadata.NodeHealthDown}
	store.nodes["node-d"] = metadata.NodeMembershipRecord{NodeID: "node-d", LifecycleState: metadata.NodeLifecycleActive, HealthState: metadata.NodeHealthHealthy}
	store.nodes["node-e"] = metadata.NodeMembershipRecord{NodeID: "node-e", LifecycleState: metadata.NodeLifecycleActive, HealthState: metadata.NodeHealthHealthy}
	store.nodes["node-g"] = metadata.NodeMembershipRecord{NodeID: "node-g", LifecycleState: metadata.NodeLifecycleActive, HealthState: metadata.NodeHealthHealthy}
	store.nodes["node-h"] = metadata.NodeMembershipRecord{NodeID: "node-h", LifecycleState: metadata.NodeLifecycleActive, HealthState: metadata.NodeHealthHealthy}
	store.mappings = []metadata.ExtentMappingRecord{
		{VolumeID: "00a1b2c3", ExtentID: 1, LogicalOffset: 0, LengthBytes: 8, ChunkID: 101, PlacementRef: "pl-1", Revision: 11},
		{VolumeID: "00a1b2c3", ExtentID: 2, LogicalOffset: 8, LengthBytes: 8, ChunkID: 102, PlacementRef: "pl-2", Revision: 11},
	}
	store.replicaSets = []metadata.ReplicaSetState{
		{
			ReplicaSetID:     "rs-1",
			VolumeID:         "00a1b2c3",
			PlacementRef:     "pl-1",
			Epoch:            5,
			PrimaryReplicaID: "rep-a",
			WriteQuorum:      2,
			ReadQuorum:       1,
			Replicas: []metadata.ReplicaDescriptor{
				{NodeID: "node-a", ReplicaID: "rep-a", Role: metadata.ReplicaRolePrimary},
				{NodeID: "node-b", ReplicaID: "rep-b", Role: metadata.ReplicaRoleSecondary},
				{NodeID: "node-c", ReplicaID: "rep-c", Role: metadata.ReplicaRoleSecondary},
			},
		},
		{
			ReplicaSetID:     "rs-2",
			VolumeID:         "00a1b2c3",
			PlacementRef:     "pl-2",
			Epoch:            5,
			PrimaryReplicaID: "rep-d",
			WriteQuorum:      2,
			ReadQuorum:       1,
			Replicas: []metadata.ReplicaDescriptor{
				{NodeID: "node-d", ReplicaID: "rep-d", Role: metadata.ReplicaRolePrimary},
				{NodeID: "node-e", ReplicaID: "rep-e", Role: metadata.ReplicaRoleSecondary},
				{NodeID: "node-f", ReplicaID: "rep-f", Role: metadata.ReplicaRoleSecondary},
			},
		},
	}
	store.mutationOps["transition-cost-a"] = metadata.MutationOperationRecord{
		OperationID:       "transition-cost-a",
		VolumeID:          "00a1b2c3",
		Kind:              "transition",
		State:             metadata.MutationOperationPending,
		AffectedExtentIDs: []uint64{1},
		AffectedPageNos:   []uint64{0},
		RetryPageWindows: []metadata.MutationPageWindowRecord{
			{ExtentID: 1, StartPageNo: 0, EndPageNo: 0, DataBytes: 16, DataChunks: 4},
		},
		LastUpdatedAtUnix: 2000,
	}
	store.mutationOps["transition-cost-b"] = metadata.MutationOperationRecord{
		OperationID:       "transition-cost-b",
		VolumeID:          "00a1b2c3",
		Kind:              "transition",
		State:             metadata.MutationOperationPending,
		AffectedExtentIDs: []uint64{2},
		AffectedPageNos:   []uint64{1},
		RetryPageWindows: []metadata.MutationPageWindowRecord{
			{ExtentID: 2, StartPageNo: 1, EndPageNo: 1, DataBytes: 8, DataChunks: 2},
		},
		LastUpdatedAtUnix: 2000,
	}

	svc := NewService(store)
	svc.now = func() time.Time { return time.Unix(4000, 0) }

	if _, err := svc.ScanAndEnqueueRepairs(context.Background(), "00a1b2c3"); err != nil {
		t.Fatalf("ScanAndEnqueueRepairs: %v", err)
	}
	if len(store.transitionPutOrder) < 2 {
		t.Fatalf("transitionPutOrder=%v want at least 2 entries", store.transitionPutOrder)
	}
	if store.transitionPutOrder[0] != "pl-2" || store.transitionPutOrder[1] != "pl-1" {
		t.Fatalf("transitionPutOrder=%v want smaller retry window cost extent first", store.transitionPutOrder)
	}
}

func TestScanAndEnqueueRepairsPrefersSourceLocalTargetForRecentMutation(t *testing.T) {
	store := newFakeStore()
	store.volumeSpec = metadata.VolumeSpecRecord{
		VolumeID:        "00a1b2c3",
		ChunkSizeBytes:  4,
		ExtentPageBytes: 8,
	}
	store.nodes = map[string]metadata.NodeMembershipRecord{
		"node-a": {NodeID: "node-a", LifecycleState: metadata.NodeLifecycleActive, HealthState: metadata.NodeHealthHealthy, Zone: "zone-a", Host: "host-a"},
		"node-b": {NodeID: "node-b", LifecycleState: metadata.NodeLifecycleActive, HealthState: metadata.NodeHealthHealthy, Zone: "zone-b", Host: "host-b"},
		"node-c": {NodeID: "node-c", LifecycleState: metadata.NodeLifecycleActive, HealthState: metadata.NodeHealthDown, Zone: "zone-c", Host: "host-c"},
		"node-d": {NodeID: "node-d", LifecycleState: metadata.NodeLifecycleActive, HealthState: metadata.NodeHealthHealthy, Zone: "zone-b", Host: "host-b"},
		"node-e": {NodeID: "node-e", LifecycleState: metadata.NodeLifecycleActive, HealthState: metadata.NodeHealthHealthy, Zone: "zone-a", Host: "host-a"},
	}
	store.mappings = []metadata.ExtentMappingRecord{
		{VolumeID: "00a1b2c3", ExtentID: 1, LogicalOffset: 0, LengthBytes: 8, ChunkID: 101, PlacementRef: "pl-1", Revision: 11},
	}
	store.replicaSets = []metadata.ReplicaSetState{
		{
			ReplicaSetID:     "rs-1",
			VolumeID:         "00a1b2c3",
			PlacementRef:     "pl-1",
			Epoch:            5,
			PrimaryReplicaID: "rep-a",
			WriteQuorum:      2,
			ReadQuorum:       1,
			Replicas: []metadata.ReplicaDescriptor{
				{NodeID: "node-a", ReplicaID: "rep-a", Role: metadata.ReplicaRolePrimary},
				{NodeID: "node-b", ReplicaID: "rep-b", Role: metadata.ReplicaRoleSecondary},
				{NodeID: "node-c", ReplicaID: "rep-c", Role: metadata.ReplicaRoleSecondary},
			},
		},
	}
	store.mutationOps["write-recent"] = metadata.MutationOperationRecord{
		OperationID:       "write-recent",
		VolumeID:          "00a1b2c3",
		Kind:              "write",
		State:             metadata.MutationOperationCommitted,
		AffectedExtentIDs: []uint64{1},
		LastUpdatedAtUnix: 2000,
	}

	svc := NewService(store)
	svc.now = func() time.Time { return time.Unix(1000, 0) }

	enqueued, err := svc.ScanAndEnqueueRepairs(context.Background(), "00a1b2c3")
	if err != nil {
		t.Fatalf("ScanAndEnqueueRepairs: %v", err)
	}
	if enqueued != 1 {
		t.Fatalf("enqueued=%d want=1", enqueued)
	}
	target, err := store.GetReplicaSet(context.Background(), "00a1b2c3", "rs-1-repair-node-c")
	if err != nil {
		t.Fatalf("repair target replica set: %v", err)
	}
	if target.Replicas[2].NodeID != "node-e" {
		t.Fatalf("replacement node=%q want=node-e", target.Replicas[2].NodeID)
	}
}

func TestScanAndEnqueueRepairsPrefersSourceLocalTargetForSmallIncompleteBatchBacklog(t *testing.T) {
	store := newFakeStore()
	store.volumeSpec = metadata.VolumeSpecRecord{
		VolumeID:        "00a1b2c3",
		ChunkSizeBytes:  4,
		ExtentPageBytes: 8,
	}
	store.nodes = map[string]metadata.NodeMembershipRecord{
		"node-a": {NodeID: "node-a", LifecycleState: metadata.NodeLifecycleActive, HealthState: metadata.NodeHealthHealthy, Zone: "zone-a", Host: "host-a"},
		"node-b": {NodeID: "node-b", LifecycleState: metadata.NodeLifecycleActive, HealthState: metadata.NodeHealthHealthy, Zone: "zone-b", Host: "host-b"},
		"node-c": {NodeID: "node-c", LifecycleState: metadata.NodeLifecycleActive, HealthState: metadata.NodeHealthDown, Zone: "zone-c", Host: "host-c"},
		"node-d": {NodeID: "node-d", LifecycleState: metadata.NodeLifecycleActive, HealthState: metadata.NodeHealthHealthy, Zone: "zone-b", Host: "host-b"},
		"node-e": {NodeID: "node-e", LifecycleState: metadata.NodeLifecycleActive, HealthState: metadata.NodeHealthHealthy, Zone: "zone-a", Host: "host-a"},
	}
	store.mappings = []metadata.ExtentMappingRecord{
		{VolumeID: "00a1b2c3", ExtentID: 1, LogicalOffset: 0, LengthBytes: 8, ChunkID: 0, PlacementRef: "pl-1", Revision: 11},
	}
	store.replicaSets = []metadata.ReplicaSetState{
		{
			ReplicaSetID:     "rs-1",
			VolumeID:         "00a1b2c3",
			PlacementRef:     "pl-1",
			Epoch:            5,
			PrimaryReplicaID: "rep-a",
			WriteQuorum:      2,
			ReadQuorum:       1,
			Replicas: []metadata.ReplicaDescriptor{
				{NodeID: "node-a", ReplicaID: "rep-a", Role: metadata.ReplicaRolePrimary},
				{NodeID: "node-b", ReplicaID: "rep-b", Role: metadata.ReplicaRoleSecondary},
				{NodeID: "node-c", ReplicaID: "rep-c", Role: metadata.ReplicaRoleSecondary},
			},
		},
	}
	store.allocationPages = []metadata.AllocationPageRecord{
		{
			VolumeID:       "00a1b2c3",
			PageNo:         0,
			PageBytes:      8,
			ChunkSizeBytes: 4,
			Extents: []metadata.AllocationExtentRecord{
				{LogicalChunkStart: 0, ChunkCount: 2, Kind: metadata.AllocationKindData, PhysicalChunkStart: 500},
			},
		},
	}
	store.mutationOps["transition-pl-1"] = metadata.MutationOperationRecord{
		OperationID:       "transition-pl-1",
		VolumeID:          "00a1b2c3",
		Kind:              "transition",
		State:             metadata.MutationOperationFailed,
		IdempotencyKey:    "pl-1",
		AffectedExtentIDs: []uint64{1},
		AffectedPageNos:   []uint64{0},
		LastUpdatedAtUnix: 2000,
	}
	store.mutationOps["transition-pl-1-pages-00000000000000000000-00000000000000000000"] = metadata.MutationOperationRecord{
		OperationID:       "transition-pl-1-pages-00000000000000000000-00000000000000000000",
		VolumeID:          "00a1b2c3",
		Kind:              "transition_batch",
		State:             metadata.MutationOperationFailed,
		IdempotencyKey:    "transition-pl-1",
		AffectedExtentIDs: []uint64{1},
		AffectedPageNos:   []uint64{0},
		LastUpdatedAtUnix: 2001,
	}

	svc := NewService(store)
	svc.now = func() time.Time { return time.Unix(1000, 0) }

	enqueued, err := svc.ScanAndEnqueueRepairs(context.Background(), "00a1b2c3")
	if err != nil {
		t.Fatalf("ScanAndEnqueueRepairs: %v", err)
	}
	if enqueued != 1 {
		t.Fatalf("enqueued=%d want=1", enqueued)
	}
	target, err := store.GetReplicaSet(context.Background(), "00a1b2c3", "rs-1-repair-node-c")
	if err != nil {
		t.Fatalf("repair target replica set: %v", err)
	}
	if target.Replicas[2].NodeID != "node-e" {
		t.Fatalf("replacement node=%q want=node-e for small incomplete backlog locality preference", target.Replicas[2].NodeID)
	}
}

func TestScanAndEnqueueRepairsPrefersSourceLocalTargetForSmallRetryWindowCost(t *testing.T) {
	store := newFakeStore()
	store.volumeSpec = metadata.VolumeSpecRecord{
		VolumeID:        "00a1b2c3",
		ChunkSizeBytes:  4,
		ExtentPageBytes: 8,
	}
	store.nodes = map[string]metadata.NodeMembershipRecord{
		"node-a": {NodeID: "node-a", LifecycleState: metadata.NodeLifecycleActive, HealthState: metadata.NodeHealthHealthy, Zone: "zone-a", Host: "host-a"},
		"node-b": {NodeID: "node-b", LifecycleState: metadata.NodeLifecycleActive, HealthState: metadata.NodeHealthHealthy, Zone: "zone-b", Host: "host-b"},
		"node-c": {NodeID: "node-c", LifecycleState: metadata.NodeLifecycleActive, HealthState: metadata.NodeHealthDown, Zone: "zone-c", Host: "host-c"},
		"node-d": {NodeID: "node-d", LifecycleState: metadata.NodeLifecycleActive, HealthState: metadata.NodeHealthHealthy, Zone: "zone-b", Host: "host-b"},
		"node-e": {NodeID: "node-e", LifecycleState: metadata.NodeLifecycleActive, HealthState: metadata.NodeHealthHealthy, Zone: "zone-a", Host: "host-a"},
	}
	store.mappings = []metadata.ExtentMappingRecord{
		{VolumeID: "00a1b2c3", ExtentID: 1, LogicalOffset: 0, LengthBytes: 8, ChunkID: 0, PlacementRef: "pl-1", Revision: 11},
	}
	store.replicaSets = []metadata.ReplicaSetState{
		{
			ReplicaSetID:     "rs-1",
			VolumeID:         "00a1b2c3",
			PlacementRef:     "pl-1",
			Epoch:            5,
			PrimaryReplicaID: "rep-a",
			WriteQuorum:      2,
			ReadQuorum:       1,
			Replicas: []metadata.ReplicaDescriptor{
				{NodeID: "node-a", ReplicaID: "rep-a", Role: metadata.ReplicaRolePrimary},
				{NodeID: "node-b", ReplicaID: "rep-b", Role: metadata.ReplicaRoleSecondary},
				{NodeID: "node-c", ReplicaID: "rep-c", Role: metadata.ReplicaRoleSecondary},
			},
		},
	}
	store.mutationOps["transition-pl-1"] = metadata.MutationOperationRecord{
		OperationID:       "transition-pl-1",
		VolumeID:          "00a1b2c3",
		Kind:              "transition",
		State:             metadata.MutationOperationPending,
		IdempotencyKey:    "pl-1",
		AffectedExtentIDs: []uint64{1},
		AffectedPageNos:   []uint64{0},
		RetryPageWindows: []metadata.MutationPageWindowRecord{
			{ExtentID: 1, StartPageNo: 0, EndPageNo: 0, DataBytes: 8, DataChunks: 2},
		},
		LastUpdatedAtUnix: 2000,
	}

	svc := NewService(store)
	svc.now = func() time.Time { return time.Unix(1000, 0) }

	enqueued, err := svc.ScanAndEnqueueRepairs(context.Background(), "00a1b2c3")
	if err != nil {
		t.Fatalf("ScanAndEnqueueRepairs: %v", err)
	}
	if enqueued != 1 {
		t.Fatalf("enqueued=%d want=1", enqueued)
	}
	target, err := store.GetReplicaSet(context.Background(), "00a1b2c3", "rs-1-repair-node-c")
	if err != nil {
		t.Fatalf("repair target replica set: %v", err)
	}
	if target.Replicas[2].NodeID != "node-e" {
		t.Fatalf("replacement node=%q want=node-e for small retry window locality preference", target.Replicas[2].NodeID)
	}
}

func TestScanAndEnqueueRepairsSkipsRecoveredCooldownNode(t *testing.T) {
	store := newFakeStore()
	store.volumeSpec = metadata.VolumeSpecRecord{
		VolumeID:        "00a1b2c3",
		ChunkSizeBytes:  4,
		ExtentPageBytes: 8,
	}
	store.nodes = map[string]metadata.NodeMembershipRecord{
		"node-a": {NodeID: "node-a", LifecycleState: metadata.NodeLifecycleActive, HealthState: metadata.NodeHealthHealthy, Zone: "zone-a", Host: "host-a"},
		"node-b": {NodeID: "node-b", LifecycleState: metadata.NodeLifecycleActive, HealthState: metadata.NodeHealthHealthy, Zone: "zone-b", Host: "host-b"},
		"node-c": {NodeID: "node-c", LifecycleState: metadata.NodeLifecycleActive, HealthState: metadata.NodeHealthDown, Zone: "zone-c", Host: "host-c"},
		"node-d": {NodeID: "node-d", LifecycleState: metadata.NodeLifecycleActive, HealthState: metadata.NodeHealthHealthy, Zone: "zone-b", Host: "host-b"},
		"node-e": {NodeID: "node-e", LifecycleState: metadata.NodeLifecycleActive, HealthState: metadata.NodeHealthHealthy, Zone: "zone-a", Host: "host-a"},
	}
	store.nodeHealthDetails["node-d"] = metadata.NodeHealthDetailRecord{
		NodeID:                 "node-d",
		RecoveryEligibleAtUnix: 2000,
	}
	store.mappings = []metadata.ExtentMappingRecord{
		{VolumeID: "00a1b2c3", ExtentID: 1, LogicalOffset: 0, LengthBytes: 8, ChunkID: 101, PlacementRef: "pl-1", Revision: 11},
	}
	store.replicaSets = []metadata.ReplicaSetState{
		{
			ReplicaSetID:     "rs-1",
			VolumeID:         "00a1b2c3",
			PlacementRef:     "pl-1",
			Epoch:            5,
			PrimaryReplicaID: "rep-a",
			WriteQuorum:      2,
			ReadQuorum:       1,
			Replicas: []metadata.ReplicaDescriptor{
				{NodeID: "node-a", ReplicaID: "rep-a", Role: metadata.ReplicaRolePrimary},
				{NodeID: "node-b", ReplicaID: "rep-b", Role: metadata.ReplicaRoleSecondary},
				{NodeID: "node-c", ReplicaID: "rep-c", Role: metadata.ReplicaRoleSecondary},
			},
		},
	}

	svc := NewService(store)
	svc.now = func() time.Time { return time.Unix(1000, 0) }

	enqueued, err := svc.ScanAndEnqueueRepairs(context.Background(), "00a1b2c3")
	if err != nil {
		t.Fatalf("ScanAndEnqueueRepairs: %v", err)
	}
	if enqueued != 1 {
		t.Fatalf("enqueued=%d want=1", enqueued)
	}
	target, err := store.GetReplicaSet(context.Background(), "00a1b2c3", "rs-1-repair-node-c")
	if err != nil {
		t.Fatalf("repair target replica set: %v", err)
	}
	if target.Replicas[2].NodeID != "node-e" {
		t.Fatalf("replacement node=%q want=node-e because node-d is still in recovery cooldown", target.Replicas[2].NodeID)
	}
}

func TestScanAndEnqueueRepairsSkipsNodeWithoutWritableStores(t *testing.T) {
	store := newFakeStore()
	store.volumeSpec = metadata.VolumeSpecRecord{
		VolumeID:        "00a1b2c3",
		ChunkSizeBytes:  4,
		ExtentPageBytes: 8,
	}
	store.nodes = map[string]metadata.NodeMembershipRecord{
		"node-a": {NodeID: "node-a", LifecycleState: metadata.NodeLifecycleActive, HealthState: metadata.NodeHealthHealthy, Zone: "zone-a", Host: "host-a"},
		"node-b": {NodeID: "node-b", LifecycleState: metadata.NodeLifecycleActive, HealthState: metadata.NodeHealthHealthy, Zone: "zone-b", Host: "host-b"},
		"node-c": {NodeID: "node-c", LifecycleState: metadata.NodeLifecycleActive, HealthState: metadata.NodeHealthDown, Zone: "zone-c", Host: "host-c"},
		"node-d": {NodeID: "node-d", LifecycleState: metadata.NodeLifecycleActive, HealthState: metadata.NodeHealthHealthy, Zone: "zone-b", Host: "host-b"},
		"node-e": {NodeID: "node-e", LifecycleState: metadata.NodeLifecycleActive, HealthState: metadata.NodeHealthHealthy, Zone: "zone-a", Host: "host-a"},
	}
	store.nodeHealthDetails["node-d"] = metadata.NodeHealthDetailRecord{
		NodeID:             "node-d",
		StoreCount:         1,
		WritableStoreCount: 0,
	}
	store.mappings = []metadata.ExtentMappingRecord{
		{VolumeID: "00a1b2c3", ExtentID: 1, LogicalOffset: 0, LengthBytes: 8, ChunkID: 101, PlacementRef: "pl-1", Revision: 11},
	}
	store.replicaSets = []metadata.ReplicaSetState{
		{
			ReplicaSetID:     "rs-1",
			VolumeID:         "00a1b2c3",
			PlacementRef:     "pl-1",
			Epoch:            5,
			PrimaryReplicaID: "rep-a",
			WriteQuorum:      2,
			ReadQuorum:       1,
			Replicas: []metadata.ReplicaDescriptor{
				{NodeID: "node-a", ReplicaID: "rep-a", Role: metadata.ReplicaRolePrimary},
				{NodeID: "node-b", ReplicaID: "rep-b", Role: metadata.ReplicaRoleSecondary},
				{NodeID: "node-c", ReplicaID: "rep-c", Role: metadata.ReplicaRoleSecondary},
			},
		},
	}

	svc := NewService(store)
	svc.now = func() time.Time { return time.Unix(1000, 0) }

	enqueued, err := svc.ScanAndEnqueueRepairs(context.Background(), "00a1b2c3")
	if err != nil {
		t.Fatalf("ScanAndEnqueueRepairs: %v", err)
	}
	if enqueued != 1 {
		t.Fatalf("enqueued=%d want=1", enqueued)
	}
	target, err := store.GetReplicaSet(context.Background(), "00a1b2c3", "rs-1-repair-node-c")
	if err != nil {
		t.Fatalf("repair target replica set: %v", err)
	}
	if target.Replicas[2].NodeID != "node-e" {
		t.Fatalf("replacement node=%q want=node-e because node-d has no writable stores", target.Replicas[2].NodeID)
	}
}

func TestScanAndEnqueueRepairsSkipsNodeWithoutPositiveAllocationWeight(t *testing.T) {
	store := newFakeStore()
	store.volumeSpec = metadata.VolumeSpecRecord{
		VolumeID:        "00a1b2c3",
		ChunkSizeBytes:  4,
		ExtentPageBytes: 8,
	}
	store.nodes = map[string]metadata.NodeMembershipRecord{
		"node-a": {NodeID: "node-a", LifecycleState: metadata.NodeLifecycleActive, HealthState: metadata.NodeHealthHealthy, Zone: "zone-a", Host: "host-a"},
		"node-b": {NodeID: "node-b", LifecycleState: metadata.NodeLifecycleActive, HealthState: metadata.NodeHealthHealthy, Zone: "zone-b", Host: "host-b"},
		"node-c": {NodeID: "node-c", LifecycleState: metadata.NodeLifecycleActive, HealthState: metadata.NodeHealthDown, Zone: "zone-c", Host: "host-c"},
		"node-d": {NodeID: "node-d", LifecycleState: metadata.NodeLifecycleActive, HealthState: metadata.NodeHealthHealthy, Zone: "zone-b", Host: "host-b"},
		"node-e": {NodeID: "node-e", LifecycleState: metadata.NodeLifecycleActive, HealthState: metadata.NodeHealthHealthy, Zone: "zone-a", Host: "host-a"},
	}
	store.nodeHealthDetails["node-d"] = metadata.NodeHealthDetailRecord{
		NodeID:                        "node-d",
		StoreCount:                    2,
		WritableStoreCount:            2,
		AllocatableStoreCount:         0,
		StoreAllocationWeightTotal:    0,
		StoreAllocationWeightObserved: true,
	}
	store.mappings = []metadata.ExtentMappingRecord{
		{VolumeID: "00a1b2c3", ExtentID: 1, LogicalOffset: 0, LengthBytes: 8, ChunkID: 101, PlacementRef: "pl-1", Revision: 11},
	}
	store.replicaSets = []metadata.ReplicaSetState{
		{
			ReplicaSetID:     "rs-1",
			VolumeID:         "00a1b2c3",
			PlacementRef:     "pl-1",
			Epoch:            5,
			PrimaryReplicaID: "rep-a",
			WriteQuorum:      2,
			ReadQuorum:       1,
			Replicas: []metadata.ReplicaDescriptor{
				{NodeID: "node-a", ReplicaID: "rep-a", Role: metadata.ReplicaRolePrimary},
				{NodeID: "node-b", ReplicaID: "rep-b", Role: metadata.ReplicaRoleSecondary},
				{NodeID: "node-c", ReplicaID: "rep-c", Role: metadata.ReplicaRoleSecondary},
			},
		},
	}

	svc := NewService(store)
	svc.now = func() time.Time { return time.Unix(1000, 0) }

	enqueued, err := svc.ScanAndEnqueueRepairs(context.Background(), "00a1b2c3")
	if err != nil {
		t.Fatalf("ScanAndEnqueueRepairs: %v", err)
	}
	if enqueued != 1 {
		t.Fatalf("enqueued=%d want=1", enqueued)
	}
	target, err := store.GetReplicaSet(context.Background(), "00a1b2c3", "rs-1-repair-node-c")
	if err != nil {
		t.Fatalf("repair target replica set: %v", err)
	}
	if target.Replicas[2].NodeID != "node-e" {
		t.Fatalf("replacement node=%q want=node-e because node-d has no positive allocation weight", target.Replicas[2].NodeID)
	}
}

func TestScanAndEnqueueRepairsReplacesReplicaWithoutWritableStores(t *testing.T) {
	store := newFakeStore()
	store.nodes["node-d"] = metadata.NodeMembershipRecord{NodeID: "node-d", LifecycleState: metadata.NodeLifecycleActive, HealthState: metadata.NodeHealthHealthy, Zone: "zone-d", Host: "host-d"}
	store.nodeHealthDetails["node-c"] = metadata.NodeHealthDetailRecord{
		NodeID:             "node-c",
		StoreCount:         1,
		WritableStoreCount: 0,
	}

	svc := NewService(store)
	svc.now = func() time.Time { return time.Unix(1000, 0) }

	enqueued, err := svc.ScanAndEnqueueRepairs(context.Background(), "00a1b2c3")
	if err != nil {
		t.Fatalf("ScanAndEnqueueRepairs: %v", err)
	}
	if enqueued != 1 {
		t.Fatalf("enqueued=%d want=1", enqueued)
	}
	target, err := store.GetReplicaSet(context.Background(), "00a1b2c3", "rs-1-repair-node-c")
	if err != nil {
		t.Fatalf("repair target replica set: %v", err)
	}
	if target.Replicas[2].NodeID != "node-d" {
		t.Fatalf("replacement node=%q want=node-d", target.Replicas[2].NodeID)
	}
}

func TestShouldPreferSourceLocalRepairTarget(t *testing.T) {
	if !shouldPreferSourceLocalRepairTarget(&EvaluatedExtent{RecentMutation: true}) {
		t.Fatalf("recent mutation should enable source locality preference")
	}
	if !shouldPreferSourceLocalRepairTarget(&EvaluatedExtent{
		IncompleteTransition: true,
		IncompleteBatches:    1,
		IncompleteBytes:      8,
	}) {
		t.Fatalf("single-batch incomplete backlog should enable source locality preference")
	}
	if !shouldPreferSourceLocalRepairTarget(&EvaluatedExtent{
		IncompleteTransition: true,
		RetryWindowCount:     1,
		RetryWindowBytes:     8,
		RetryWindowChunks:    2,
	}) {
		t.Fatalf("small retry window cost should enable source locality preference")
	}
	if shouldPreferSourceLocalRepairTarget(&EvaluatedExtent{
		IncompleteTransition: true,
		IncompleteBatches:    2,
		IncompleteBytes:      16,
	}) {
		t.Fatalf("multi-batch incomplete backlog should not enable source locality preference by itself")
	}
}

func TestScanAndFailoverPrimariesWhenPrimaryNodeDown(t *testing.T) {
	store := newFakeStore()
	store.nodes["node-a"] = metadata.NodeMembershipRecord{NodeID: "node-a", LifecycleState: metadata.NodeLifecycleActive, HealthState: metadata.NodeHealthDown}
	svc := NewService(store)

	failovers, err := svc.ScanAndFailoverPrimaries(context.Background(), "00a1b2c3")
	if err != nil {
		t.Fatalf("ScanAndFailoverPrimaries: %v", err)
	}
	if failovers != 1 {
		t.Fatalf("failovers=%d want=1", failovers)
	}
	if store.volume.Epoch != 6 {
		t.Fatalf("volume epoch=%d want=6", store.volume.Epoch)
	}
	if store.replicaSets[0].PrimaryReplicaID != "rep-b" {
		t.Fatalf("primary=%q want=rep-b", store.replicaSets[0].PrimaryReplicaID)
	}
	if store.replicaSets[0].Replicas[0].Role != metadata.ReplicaRoleSecondary {
		t.Fatalf("old primary role=%q want=secondary", store.replicaSets[0].Replicas[0].Role)
	}
	if store.replicaSets[0].Replicas[1].Role != metadata.ReplicaRolePrimary {
		t.Fatalf("new primary role=%q want=primary", store.replicaSets[0].Replicas[1].Role)
	}
}

func TestApplyTransitionRejectsTargetWithoutWritableStores(t *testing.T) {
	store := newFakeStore()
	store.replicaSets = append(store.replicaSets, metadata.ReplicaSetState{
		ReplicaSetID:     "rs-2",
		VolumeID:         "00a1b2c3",
		PlacementRef:     "pl-2",
		Epoch:            5,
		PrimaryReplicaID: "rep-d",
		WriteQuorum:      2,
		ReadQuorum:       1,
		Replicas: []metadata.ReplicaDescriptor{
			{NodeID: "node-d", ReplicaID: "rep-d", Role: metadata.ReplicaRolePrimary},
			{NodeID: "node-e", ReplicaID: "rep-e", Role: metadata.ReplicaRoleSecondary},
			{NodeID: "node-f", ReplicaID: "rep-f", Role: metadata.ReplicaRoleSecondary},
		},
	})
	store.nodes["node-d"] = metadata.NodeMembershipRecord{NodeID: "node-d", LifecycleState: metadata.NodeLifecycleActive, HealthState: metadata.NodeHealthHealthy}
	store.nodes["node-e"] = metadata.NodeMembershipRecord{NodeID: "node-e", LifecycleState: metadata.NodeLifecycleActive, HealthState: metadata.NodeHealthHealthy}
	store.nodes["node-f"] = metadata.NodeMembershipRecord{NodeID: "node-f", LifecycleState: metadata.NodeLifecycleActive, HealthState: metadata.NodeHealthHealthy}
	store.nodeHealthDetails["node-d"] = metadata.NodeHealthDetailRecord{
		NodeID:             "node-d",
		StoreCount:         1,
		WritableStoreCount: 0,
	}

	svc := NewService(store)
	if _, err := svc.EnqueueRepair(context.Background(), "00a1b2c3", 1, "rs-2"); err != nil {
		t.Fatalf("EnqueueRepair: %v", err)
	}
	if _, err := svc.ApplyTransition(context.Background(), "00a1b2c3", "pl-1", nil, "gw-a", "host-a"); err == nil || !strings.Contains(err.Error(), "no writable store capacity") {
		t.Fatalf("ApplyTransition error=%v want no writable store capacity", err)
	}
}

func TestValidateTransitionTargetAllowsExistingUnhealthyReplicaWhenWriteQuorumEligible(t *testing.T) {
	store := newFakeStore()
	currentReplicaSet := metadata.ReplicaSetState{
		ReplicaSetID:     "rs-1",
		VolumeID:         "00a1b2c3",
		PlacementRef:     "pl-1",
		PrimaryReplicaID: "rep-a",
		WriteQuorum:      2,
		ReadQuorum:       1,
		Replicas: []metadata.ReplicaDescriptor{
			{NodeID: "node-a", ReplicaID: "rep-a", Role: metadata.ReplicaRolePrimary},
			{NodeID: "node-b", ReplicaID: "rep-b", Role: metadata.ReplicaRoleSecondary},
			{NodeID: "node-c", ReplicaID: "rep-c", Role: metadata.ReplicaRoleSecondary},
		},
	}
	targetReplicaSet := metadata.ReplicaSetState{
		ReplicaSetID:     "rs-1-repair-node-c",
		VolumeID:         "00a1b2c3",
		PlacementRef:     "pl-1-repair-node-c",
		PrimaryReplicaID: "rep-a",
		WriteQuorum:      2,
		ReadQuorum:       1,
		Replicas: []metadata.ReplicaDescriptor{
			{NodeID: "node-a", ReplicaID: "rep-a", Role: metadata.ReplicaRolePrimary},
			{NodeID: "node-b", ReplicaID: "rep-b", Role: metadata.ReplicaRoleSecondary},
			{NodeID: "node-d", ReplicaID: "rep-d", Role: metadata.ReplicaRoleSecondary},
		},
	}
	store.nodes["node-a"] = metadata.NodeMembershipRecord{NodeID: "node-a", LifecycleState: metadata.NodeLifecycleActive, HealthState: metadata.NodeHealthDown}
	store.nodes["node-d"] = metadata.NodeMembershipRecord{NodeID: "node-d", LifecycleState: metadata.NodeLifecycleActive, HealthState: metadata.NodeHealthHealthy}

	svc := NewService(store)
	vc, err := svc.loadVolumeEvaluationContext(context.Background(), "00a1b2c3")
	if err != nil {
		t.Fatalf("loadVolumeEvaluationContext: %v", err)
	}
	if err := svc.validateTransitionTargetReplicaSetEligibility(context.Background(), vc, currentReplicaSet, targetReplicaSet); err != nil {
		t.Fatalf("validateTransitionTargetReplicaSetEligibility: %v", err)
	}
}

func TestApplyTransitionRejectsSourceWithoutReadQuorumStoreEligibility(t *testing.T) {
	store := newFakeStore()
	store.replicaSets = append(store.replicaSets, metadata.ReplicaSetState{
		ReplicaSetID:     "rs-2",
		VolumeID:         "00a1b2c3",
		PlacementRef:     "pl-2",
		Epoch:            5,
		PrimaryReplicaID: "rep-d",
		WriteQuorum:      2,
		ReadQuorum:       1,
		Replicas: []metadata.ReplicaDescriptor{
			{NodeID: "node-d", ReplicaID: "rep-d", Role: metadata.ReplicaRolePrimary},
			{NodeID: "node-e", ReplicaID: "rep-e", Role: metadata.ReplicaRoleSecondary},
			{NodeID: "node-f", ReplicaID: "rep-f", Role: metadata.ReplicaRoleSecondary},
		},
	})
	store.nodes["node-d"] = metadata.NodeMembershipRecord{NodeID: "node-d", LifecycleState: metadata.NodeLifecycleActive, HealthState: metadata.NodeHealthHealthy}
	store.nodes["node-e"] = metadata.NodeMembershipRecord{NodeID: "node-e", LifecycleState: metadata.NodeLifecycleActive, HealthState: metadata.NodeHealthHealthy}
	store.nodes["node-f"] = metadata.NodeMembershipRecord{NodeID: "node-f", LifecycleState: metadata.NodeLifecycleActive, HealthState: metadata.NodeHealthHealthy}
	for _, nodeID := range []string{"node-a", "node-b", "node-c"} {
		store.nodeHealthDetails[nodeID] = metadata.NodeHealthDetailRecord{
			NodeID:             nodeID,
			StoreCount:         1,
			WritableStoreCount: 0,
		}
	}

	svc := NewService(store)
	if _, err := svc.EnqueueRepair(context.Background(), "00a1b2c3", 1, "rs-2"); err != nil {
		t.Fatalf("EnqueueRepair: %v", err)
	}
	if _, err := svc.ApplyTransition(context.Background(), "00a1b2c3", "pl-1", nil, "gw-a", "host-a"); err == nil || !strings.Contains(err.Error(), "store-eligible replicas") {
		t.Fatalf("ApplyTransition error=%v want source store eligibility error", err)
	}
}

func TestApplyTransitionCopiesExtentAndSwitchesPlacement(t *testing.T) {
	store := newFakeStore()
	store.replicaSets = append(store.replicaSets, metadata.ReplicaSetState{
		ReplicaSetID:     "rs-2",
		VolumeID:         "00a1b2c3",
		PlacementRef:     "pl-2",
		Epoch:            5,
		PrimaryReplicaID: "rep-d",
		WriteQuorum:      2,
		ReadQuorum:       1,
		Replicas: []metadata.ReplicaDescriptor{
			{NodeID: "node-d", ReplicaID: "rep-d", Role: metadata.ReplicaRolePrimary},
			{NodeID: "node-e", ReplicaID: "rep-e", Role: metadata.ReplicaRoleSecondary},
			{NodeID: "node-f", ReplicaID: "rep-f", Role: metadata.ReplicaRoleSecondary},
		},
	})
	store.nodes["node-d"] = metadata.NodeMembershipRecord{NodeID: "node-d", LifecycleState: metadata.NodeLifecycleActive, HealthState: metadata.NodeHealthHealthy}
	store.nodes["node-e"] = metadata.NodeMembershipRecord{NodeID: "node-e", LifecycleState: metadata.NodeLifecycleActive, HealthState: metadata.NodeHealthHealthy}
	store.nodes["node-f"] = metadata.NodeMembershipRecord{NodeID: "node-f", LifecycleState: metadata.NodeLifecycleActive, HealthState: metadata.NodeHealthHealthy}

	spec := service.NormalizeVolumeSpec(service.VolumeSpec{
		ID:        service.HexVolumeID(0x00a1b2c3),
		Name:      "vol-a",
		Prefix:    "vol-a-00a1b2c3",
		SizeBytes: 4096 * 4,
		BlockSize: 8,
	})
	replicaClients := map[string]service.SBSClient{
		"rep-a": service.NewInMemorySBSClient([]service.VolumeSpec{spec}),
		"rep-b": service.NewInMemorySBSClient([]service.VolumeSpec{spec}),
		"rep-c": service.NewInMemorySBSClient([]service.VolumeSpec{spec}),
		"rep-d": service.NewInMemorySBSClient([]service.VolumeSpec{spec}),
		"rep-e": service.NewInMemorySBSClient([]service.VolumeSpec{spec}),
		"rep-f": service.NewInMemorySBSClient([]service.VolumeSpec{spec}),
	}

	svc := NewService(store)
	svc.now = func() time.Time { return time.Unix(1000, 0) }

	if _, err := svc.EnqueueRepair(context.Background(), "00a1b2c3", 1, "rs-2"); err != nil {
		t.Fatalf("EnqueueRepair: %v", err)
	}

	sourceReplicas, err := replication.OpenReplicaSessions(context.Background(), map[string]service.SBSClient{
		"rep-a": replicaClients["rep-a"],
		"rep-b": replicaClients["rep-b"],
		"rep-c": replicaClients["rep-c"],
	}, replication.OpenReplicaSessionsRequest{
		VolumeID:      "00a1b2c3",
		GatewayID:     "gw-a",
		HostID:        "host-a",
		AttachmentID:  "seed",
		Generation:    1,
		SessionPrefix: "seed",
	})
	if err != nil {
		t.Fatalf("OpenReplicaSessions seed: %v", err)
	}
	writer := replication.NewRemoteReplicaWriter(sourceReplicas)
	payload := []byte("payload1")
	if _, err := writer.WriteExtent(context.Background(), replication.ExtentWritePlan{
		Extent:       store.mappings[0],
		WriteTargets: []replication.ReplicaTarget{{ReplicaID: "rep-a"}, {ReplicaID: "rep-b"}, {ReplicaID: "rep-c"}},
	}, replication.ReplicaWriteRequest{
		RequestID:      "seed-1",
		VolumeID:       "00a1b2c3",
		AttachmentID:   "seed",
		Generation:     1,
		IdempotencyKey: "seed-1",
		OffsetBytes:    0,
		LengthBytes:    8,
		Data:           payload,
	}); err != nil {
		t.Fatalf("seed write: %v", err)
	}

	transition, err := svc.ApplyTransition(context.Background(), "00a1b2c3", "pl-1", replicaClients, "gw-a", "host-a")
	if err != nil {
		t.Fatalf("ApplyTransition: %v", err)
	}
	if transition.State != metadata.PlacementTransitionCompleted {
		t.Fatalf("transition state=%q want=%q", transition.State, metadata.PlacementTransitionCompleted)
	}
	if store.mappings[0].PlacementRef != "pl-2" {
		t.Fatalf("placement_ref=%q want=pl-2", store.mappings[0].PlacementRef)
	}
	if store.volume.Revision != 12 {
		t.Fatalf("volume revision=%d want=12", store.volume.Revision)
	}
	op := store.mutationOps[transitionMutationOperationID(transition)]
	if op.State != metadata.MutationOperationCommitted || op.PlacementRevision != 12 {
		t.Fatalf("mutation operation=%+v", op)
	}

	targetReplicas, err := replication.OpenReplicaSessions(context.Background(), map[string]service.SBSClient{
		"rep-d": replicaClients["rep-d"],
		"rep-e": replicaClients["rep-e"],
		"rep-f": replicaClients["rep-f"],
	}, replication.OpenReplicaSessionsRequest{
		VolumeID:      "00a1b2c3",
		GatewayID:     "gw-a",
		HostID:        "host-a",
		AttachmentID:  "verify",
		Generation:    1,
		SessionPrefix: "verify",
	})
	if err != nil {
		t.Fatalf("OpenReplicaSessions verify: %v", err)
	}
	reader := replication.NewRemoteReplicaReader(targetReplicas)
	data, replicaID, err := reader.ReadExtent(context.Background(), replication.ExtentReadPlan{
		Extent:    store.mappings[0],
		Preferred: replication.ReplicaTarget{ReplicaID: "rep-d"},
		Fallbacks: []replication.ReplicaTarget{{ReplicaID: "rep-e"}, {ReplicaID: "rep-f"}},
	}, replication.ReplicaReadRequest{
		VolumeID:    "00a1b2c3",
		OffsetBytes: 0,
		LengthBytes: 8,
	})
	if err != nil {
		t.Fatalf("ReadExtent target: %v", err)
	}
	if replicaID != "rep-d" {
		t.Fatalf("replicaID=%q want rep-d", replicaID)
	}
	if string(data[:len("payload1")]) != "payload1" {
		t.Fatalf("payload=%q", data[:len("payload1")])
	}
}

func TestApplyTransitionMaterializesLegacyCompatibleAllocationPages(t *testing.T) {
	store := newFakeStore()
	store.volumeSpec = metadata.VolumeSpecRecord{
		VolumeID:        "00a1b2c3",
		SizeBytes:       8,
		BlockSize:       8,
		ChunkSizeBytes:  8,
		ExtentPageBytes: 8,
	}
	store.allocationPages = nil
	store.replicaSets = append(store.replicaSets, metadata.ReplicaSetState{
		ReplicaSetID:     "rs-2",
		VolumeID:         "00a1b2c3",
		PlacementRef:     "pl-2",
		Epoch:            5,
		PrimaryReplicaID: "rep-d",
		WriteQuorum:      2,
		ReadQuorum:       1,
		Replicas: []metadata.ReplicaDescriptor{
			{NodeID: "node-d", ReplicaID: "rep-d", Role: metadata.ReplicaRolePrimary},
			{NodeID: "node-e", ReplicaID: "rep-e", Role: metadata.ReplicaRoleSecondary},
			{NodeID: "node-f", ReplicaID: "rep-f", Role: metadata.ReplicaRoleSecondary},
		},
	})
	store.nodes["node-d"] = metadata.NodeMembershipRecord{NodeID: "node-d", LifecycleState: metadata.NodeLifecycleActive, HealthState: metadata.NodeHealthHealthy}
	store.nodes["node-e"] = metadata.NodeMembershipRecord{NodeID: "node-e", LifecycleState: metadata.NodeLifecycleActive, HealthState: metadata.NodeHealthHealthy}
	store.nodes["node-f"] = metadata.NodeMembershipRecord{NodeID: "node-f", LifecycleState: metadata.NodeLifecycleActive, HealthState: metadata.NodeHealthHealthy}

	spec := service.NormalizeVolumeSpec(service.VolumeSpec{
		ID:        service.HexVolumeID(0x00a1b2c3),
		Name:      "vol-a",
		Prefix:    "vol-a-00a1b2c3",
		SizeBytes: 4096 * 4,
		BlockSize: 8,
	})
	sourceA := service.NewInMemorySBSClient([]service.VolumeSpec{spec})
	sourceB := service.NewInMemorySBSClient([]service.VolumeSpec{spec})
	sourceC := service.NewInMemorySBSClient([]service.VolumeSpec{spec})
	targetD := service.NewInMemorySBSClient([]service.VolumeSpec{spec})
	targetE := service.NewInMemorySBSClient([]service.VolumeSpec{spec})
	targetF := service.NewInMemorySBSClient([]service.VolumeSpec{spec})
	replicaClients := map[string]service.SBSClient{
		"rep-a": sourceA,
		"rep-b": sourceB,
		"rep-c": sourceC,
		"rep-d": targetD,
		"rep-e": targetE,
		"rep-f": targetF,
	}

	svc := NewService(store)
	svc.now = func() time.Time { return time.Unix(1000, 0) }

	if _, err := svc.EnqueueRepair(context.Background(), "00a1b2c3", 1, "rs-2"); err != nil {
		t.Fatalf("EnqueueRepair: %v", err)
	}

	sourceReplicas, err := replication.OpenReplicaSessions(context.Background(), map[string]service.SBSClient{
		"rep-a": replicaClients["rep-a"],
		"rep-b": replicaClients["rep-b"],
		"rep-c": replicaClients["rep-c"],
	}, replication.OpenReplicaSessionsRequest{
		VolumeID:      "00a1b2c3",
		GatewayID:     "gw-a",
		HostID:        "host-a",
		AttachmentID:  "seed",
		Generation:    1,
		SessionPrefix: "seed",
	})
	if err != nil {
		t.Fatalf("OpenReplicaSessions seed: %v", err)
	}
	writer := replication.NewRemoteReplicaWriter(sourceReplicas)
	if _, err := writer.WriteExtent(context.Background(), replication.ExtentWritePlan{
		Extent:       store.mappings[0],
		WriteTargets: []replication.ReplicaTarget{{ReplicaID: "rep-a"}, {ReplicaID: "rep-b"}, {ReplicaID: "rep-c"}},
	}, replication.ReplicaWriteRequest{
		RequestID:      "seed-1",
		VolumeID:       "00a1b2c3",
		AttachmentID:   "seed",
		Generation:     1,
		IdempotencyKey: "seed-1",
		OffsetBytes:    0,
		LengthBytes:    8,
		Data:           []byte("payload1"),
	}); err != nil {
		t.Fatalf("seed write: %v", err)
	}

	if _, err := svc.ApplyTransition(context.Background(), "00a1b2c3", "pl-1", replicaClients, "gw-a", "host-a"); err != nil {
		t.Fatalf("ApplyTransition: %v", err)
	}
	if len(store.allocationPages) != 1 {
		t.Fatalf("allocation pages=%d want=1", len(store.allocationPages))
	}
	if store.allocationPages[0].Revision != store.volume.Revision {
		t.Fatalf("allocation page revision=%d volume revision=%d", store.allocationPages[0].Revision, store.volume.Revision)
	}
	if len(store.allocationPages[0].Extents) != 1 || store.allocationPages[0].Extents[0].Kind != metadata.AllocationKindData {
		t.Fatalf("allocation page extents=%+v", store.allocationPages[0].Extents)
	}
	if store.allocationPages[0].Extents[0].PhysicalChunkStart != 101 {
		t.Fatalf("allocation page=%+v", store.allocationPages[0].Extents[0])
	}
	if store.mappings[0].ChunkID != 0 || store.mappings[0].Revision != store.volume.Revision {
		t.Fatalf("mapping=%+v volume revision=%d", store.mappings[0], store.volume.Revision)
	}
	operation := store.mutationOps["transition-pl-1"]
	if len(operation.RetiredPhysicalChunkIDs) != 1 || operation.RetiredPhysicalChunkIDs[0] != 101 {
		t.Fatalf("retired physical chunk ids=%v want=[101]", operation.RetiredPhysicalChunkIDs)
	}
	payloadGCOp := store.mutationOps[metadata.PayloadGCMutationOperationID("00a1b2c3")]
	if payloadGCOp.State != metadata.MutationOperationPending {
		t.Fatalf("payload-gc state=%q want=%q", payloadGCOp.State, metadata.MutationOperationPending)
	}
	if len(payloadGCOp.AffectedExtentIDs) != 1 || payloadGCOp.AffectedExtentIDs[0] != 1 {
		t.Fatalf("payload-gc affected extents=%v want=[1]", payloadGCOp.AffectedExtentIDs)
	}
	if len(payloadGCOp.AffectedPageNos) != 1 || payloadGCOp.AffectedPageNos[0] != 0 {
		t.Fatalf("payload-gc affected pages=%v want=[0]", payloadGCOp.AffectedPageNos)
	}
	if len(payloadGCOp.RetiredPhysicalChunkIDs) != 1 || payloadGCOp.RetiredPhysicalChunkIDs[0] != 101 {
		t.Fatalf("payload-gc retired chunks=%v want=[101]", payloadGCOp.RetiredPhysicalChunkIDs)
	}
}

func TestApplyTransitionMarksMutationOperationFailedOnTargetOpenError(t *testing.T) {
	store := newFakeStore()
	store.replicaSets = append(store.replicaSets, metadata.ReplicaSetState{
		ReplicaSetID:     "rs-2",
		VolumeID:         "00a1b2c3",
		PlacementRef:     "pl-2",
		Epoch:            5,
		PrimaryReplicaID: "rep-d",
		WriteQuorum:      2,
		ReadQuorum:       1,
		Replicas: []metadata.ReplicaDescriptor{
			{NodeID: "node-d", ReplicaID: "rep-d", Role: metadata.ReplicaRolePrimary},
			{NodeID: "node-e", ReplicaID: "rep-e", Role: metadata.ReplicaRoleSecondary},
			{NodeID: "node-f", ReplicaID: "rep-f", Role: metadata.ReplicaRoleSecondary},
		},
	})
	store.nodes["node-d"] = metadata.NodeMembershipRecord{NodeID: "node-d", LifecycleState: metadata.NodeLifecycleActive, HealthState: metadata.NodeHealthHealthy}
	store.nodes["node-e"] = metadata.NodeMembershipRecord{NodeID: "node-e", LifecycleState: metadata.NodeLifecycleActive, HealthState: metadata.NodeHealthHealthy}
	store.nodes["node-f"] = metadata.NodeMembershipRecord{NodeID: "node-f", LifecycleState: metadata.NodeLifecycleActive, HealthState: metadata.NodeHealthHealthy}

	spec := service.NormalizeVolumeSpec(service.VolumeSpec{
		ID:        service.HexVolumeID(0x00a1b2c3),
		Name:      "vol-a",
		Prefix:    "vol-a-00a1b2c3",
		SizeBytes: 4096 * 4,
		BlockSize: 8,
	})
	sourceA := service.NewInMemorySBSClient([]service.VolumeSpec{spec})
	sourceB := service.NewInMemorySBSClient([]service.VolumeSpec{spec})
	sourceC := service.NewInMemorySBSClient([]service.VolumeSpec{spec})
	replicaClients := map[string]service.SBSClient{
		"rep-a": sourceA,
		"rep-b": sourceB,
		"rep-c": sourceC,
	}

	svc := NewService(store)
	svc.now = func() time.Time { return time.Unix(1000, 0) }

	if _, err := svc.EnqueueRepair(context.Background(), "00a1b2c3", 1, "rs-2"); err != nil {
		t.Fatalf("EnqueueRepair: %v", err)
	}

	sourceReplicas, err := replication.OpenReplicaSessions(context.Background(), map[string]service.SBSClient{
		"rep-a": sourceA,
		"rep-b": sourceB,
		"rep-c": sourceC,
	}, replication.OpenReplicaSessionsRequest{
		VolumeID:      "00a1b2c3",
		GatewayID:     "gw-a",
		HostID:        "host-a",
		AttachmentID:  "seed",
		Generation:    1,
		SessionPrefix: "seed",
	})
	if err != nil {
		t.Fatalf("OpenReplicaSessions seed: %v", err)
	}
	writer := replication.NewRemoteReplicaWriter(sourceReplicas)
	if _, err := writer.WriteExtent(context.Background(), replication.ExtentWritePlan{
		Extent:       store.mappings[0],
		WriteTargets: []replication.ReplicaTarget{{ReplicaID: "rep-a"}, {ReplicaID: "rep-b"}, {ReplicaID: "rep-c"}},
	}, replication.ReplicaWriteRequest{
		RequestID:      "seed-1",
		VolumeID:       "00a1b2c3",
		AttachmentID:   "seed",
		Generation:     1,
		IdempotencyKey: "seed-1",
		OffsetBytes:    0,
		LengthBytes:    8,
		Data:           []byte("payload1"),
	}); err != nil {
		t.Fatalf("seed write: %v", err)
	}

	transition, err := svc.ApplyTransition(context.Background(), "00a1b2c3", "pl-1", replicaClients, "gw-a", "host-a")
	if err == nil {
		t.Fatalf("ApplyTransition unexpectedly succeeded: %+v", transition)
	}
	op := store.mutationOps["transition-pl-1"]
	if op.State != metadata.MutationOperationFailed || op.ErrorMessage == "" {
		t.Fatalf("mutation operation=%+v", op)
	}
}

func TestApplyTransitionSparseZeroExtentSkipsTargetPayloadWrite(t *testing.T) {
	store := newFakeStore()
	store.replicaSets = append(store.replicaSets, metadata.ReplicaSetState{
		ReplicaSetID:     "rs-2",
		VolumeID:         "00a1b2c3",
		PlacementRef:     "pl-2",
		Epoch:            5,
		PrimaryReplicaID: "rep-d",
		WriteQuorum:      2,
		ReadQuorum:       1,
		Replicas: []metadata.ReplicaDescriptor{
			{NodeID: "node-d", ReplicaID: "rep-d", Role: metadata.ReplicaRolePrimary},
			{NodeID: "node-e", ReplicaID: "rep-e", Role: metadata.ReplicaRoleSecondary},
			{NodeID: "node-f", ReplicaID: "rep-f", Role: metadata.ReplicaRoleSecondary},
		},
	})
	store.nodes["node-d"] = metadata.NodeMembershipRecord{NodeID: "node-d", LifecycleState: metadata.NodeLifecycleActive, HealthState: metadata.NodeHealthHealthy}
	store.nodes["node-e"] = metadata.NodeMembershipRecord{NodeID: "node-e", LifecycleState: metadata.NodeLifecycleActive, HealthState: metadata.NodeHealthHealthy}
	store.nodes["node-f"] = metadata.NodeMembershipRecord{NodeID: "node-f", LifecycleState: metadata.NodeLifecycleActive, HealthState: metadata.NodeHealthHealthy}

	spec := service.NormalizeVolumeSpec(service.VolumeSpec{
		ID:        service.HexVolumeID(0x00a1b2c3),
		Name:      "vol-a",
		Prefix:    "vol-a-00a1b2c3",
		SizeBytes: 4096 * 4,
		BlockSize: 8,
	})
	replicaClients := map[string]service.SBSClient{
		"rep-a": service.NewInMemorySBSClient([]service.VolumeSpec{spec}),
		"rep-b": service.NewInMemorySBSClient([]service.VolumeSpec{spec}),
		"rep-c": service.NewInMemorySBSClient([]service.VolumeSpec{spec}),
	}

	svc := NewService(store)
	svc.now = func() time.Time { return time.Unix(1000, 0) }

	if _, err := svc.EnqueueRebalance(context.Background(), "00a1b2c3", 1, "rs-2"); err != nil {
		t.Fatalf("EnqueueRebalance: %v", err)
	}

	transition, err := svc.ApplyTransition(context.Background(), "00a1b2c3", "pl-1", replicaClients, "gw-a", "host-a")
	if err != nil {
		t.Fatalf("ApplyTransition sparse zero extent: %v", err)
	}
	if transition.State != metadata.PlacementTransitionCompleted {
		t.Fatalf("transition state=%q want=%q", transition.State, metadata.PlacementTransitionCompleted)
	}
	if store.mappings[0].PlacementRef != "pl-2" {
		t.Fatalf("placement_ref=%q want=pl-2", store.mappings[0].PlacementRef)
	}
}

func TestApplyTransitionSparseMissingSourceVolumeSwitchesPlacementOnly(t *testing.T) {
	store := newFakeStore()
	store.replicaSets = append(store.replicaSets, metadata.ReplicaSetState{
		ReplicaSetID:     "rs-2",
		VolumeID:         "00a1b2c3",
		PlacementRef:     "pl-2",
		Epoch:            5,
		PrimaryReplicaID: "rep-d",
		WriteQuorum:      2,
		ReadQuorum:       1,
		Replicas: []metadata.ReplicaDescriptor{
			{NodeID: "node-d", ReplicaID: "rep-d", Role: metadata.ReplicaRolePrimary},
			{NodeID: "node-e", ReplicaID: "rep-e", Role: metadata.ReplicaRoleSecondary},
			{NodeID: "node-f", ReplicaID: "rep-f", Role: metadata.ReplicaRoleSecondary},
		},
	})
	store.nodes["node-d"] = metadata.NodeMembershipRecord{NodeID: "node-d", LifecycleState: metadata.NodeLifecycleActive, HealthState: metadata.NodeHealthHealthy}
	store.nodes["node-e"] = metadata.NodeMembershipRecord{NodeID: "node-e", LifecycleState: metadata.NodeLifecycleActive, HealthState: metadata.NodeHealthHealthy}
	store.nodes["node-f"] = metadata.NodeMembershipRecord{NodeID: "node-f", LifecycleState: metadata.NodeLifecycleActive, HealthState: metadata.NodeHealthHealthy}

	replicaClients := map[string]service.SBSClient{
		"rep-a": service.NewInMemorySBSClient(nil),
		"rep-b": service.NewInMemorySBSClient(nil),
		"rep-c": service.NewInMemorySBSClient(nil),
	}

	svc := NewService(store)
	svc.now = func() time.Time { return time.Unix(1000, 0) }

	if _, err := svc.EnqueueRebalance(context.Background(), "00a1b2c3", 1, "rs-2"); err != nil {
		t.Fatalf("EnqueueRebalance: %v", err)
	}

	transition, err := svc.ApplyTransition(context.Background(), "00a1b2c3", "pl-1", replicaClients, "gw-a", "host-a")
	if err != nil {
		t.Fatalf("ApplyTransition missing source volume: %v", err)
	}
	if transition.State != metadata.PlacementTransitionCompleted {
		t.Fatalf("transition state=%q want=%q", transition.State, metadata.PlacementTransitionCompleted)
	}
	if store.mappings[0].PlacementRef != "pl-2" {
		t.Fatalf("placement_ref=%q want=pl-2", store.mappings[0].PlacementRef)
	}
}

func TestApplyTransitionZeroChunkMetadataOnlySkipsSourceRead(t *testing.T) {
	store := newFakeStore()
	store.mappings[0].ChunkID = 0
	store.replicaSets = append(store.replicaSets, metadata.ReplicaSetState{
		ReplicaSetID:     "rs-2",
		VolumeID:         "00a1b2c3",
		PlacementRef:     "pl-2",
		Epoch:            5,
		PrimaryReplicaID: "rep-d",
		WriteQuorum:      2,
		ReadQuorum:       1,
		Replicas: []metadata.ReplicaDescriptor{
			{NodeID: "node-d", ReplicaID: "rep-d", Role: metadata.ReplicaRolePrimary},
			{NodeID: "node-e", ReplicaID: "rep-e", Role: metadata.ReplicaRoleSecondary},
			{NodeID: "node-f", ReplicaID: "rep-f", Role: metadata.ReplicaRoleSecondary},
		},
	})
	store.nodes["node-d"] = metadata.NodeMembershipRecord{NodeID: "node-d", LifecycleState: metadata.NodeLifecycleActive, HealthState: metadata.NodeHealthHealthy}
	store.nodes["node-e"] = metadata.NodeMembershipRecord{NodeID: "node-e", LifecycleState: metadata.NodeLifecycleActive, HealthState: metadata.NodeHealthHealthy}
	store.nodes["node-f"] = metadata.NodeMembershipRecord{NodeID: "node-f", LifecycleState: metadata.NodeLifecycleActive, HealthState: metadata.NodeHealthHealthy}

	spec := service.NormalizeVolumeSpec(service.VolumeSpec{
		ID:        service.HexVolumeID(0x00a1b2c3),
		Name:      "vol-a",
		Prefix:    "vol-a-00a1b2c3",
		SizeBytes: 4096 * 4,
		BlockSize: 8,
	})
	base := service.NewInMemorySBSClient([]service.VolumeSpec{spec})
	replicaClients := map[string]service.SBSClient{
		"rep-a": failReadSBSClient{next: base},
		"rep-b": failReadSBSClient{next: base},
		"rep-c": failReadSBSClient{next: base},
	}

	svc := NewService(store)
	svc.now = func() time.Time { return time.Unix(1000, 0) }

	if _, err := svc.EnqueueRebalance(context.Background(), "00a1b2c3", 1, "rs-2"); err != nil {
		t.Fatalf("EnqueueRebalance: %v", err)
	}

	transition, err := svc.ApplyTransition(context.Background(), "00a1b2c3", "pl-1", replicaClients, "gw-a", "host-a")
	if err != nil {
		t.Fatalf("ApplyTransition zero metadata-only: %v", err)
	}
	if transition.State != metadata.PlacementTransitionCompleted {
		t.Fatalf("transition state=%q want=%q", transition.State, metadata.PlacementTransitionCompleted)
	}
	if store.mappings[0].PlacementRef != "pl-2" {
		t.Fatalf("placement_ref=%q want=pl-2", store.mappings[0].PlacementRef)
	}
}

func TestApplyTransitionMixedAllocationCopiesOnlyDataChunks(t *testing.T) {
	store := newFakeStore()
	store.replicaSets = append(store.replicaSets, metadata.ReplicaSetState{
		ReplicaSetID:     "rs-2",
		VolumeID:         "00a1b2c3",
		PlacementRef:     "pl-2",
		Epoch:            5,
		PrimaryReplicaID: "rep-d",
		WriteQuorum:      2,
		ReadQuorum:       1,
		Replicas: []metadata.ReplicaDescriptor{
			{NodeID: "node-d", ReplicaID: "rep-d", Role: metadata.ReplicaRolePrimary},
			{NodeID: "node-e", ReplicaID: "rep-e", Role: metadata.ReplicaRoleSecondary},
			{NodeID: "node-f", ReplicaID: "rep-f", Role: metadata.ReplicaRoleSecondary},
		},
	})
	store.nodes["node-d"] = metadata.NodeMembershipRecord{NodeID: "node-d", LifecycleState: metadata.NodeLifecycleActive, HealthState: metadata.NodeHealthHealthy}
	store.nodes["node-e"] = metadata.NodeMembershipRecord{NodeID: "node-e", LifecycleState: metadata.NodeLifecycleActive, HealthState: metadata.NodeHealthHealthy}
	store.nodes["node-f"] = metadata.NodeMembershipRecord{NodeID: "node-f", LifecycleState: metadata.NodeLifecycleActive, HealthState: metadata.NodeHealthHealthy}
	store.allocationPages = []metadata.AllocationPageRecord{
		{
			VolumeID:       "00a1b2c3",
			PageNo:         0,
			PageBytes:      8,
			ChunkSizeBytes: 4,
			Extents: []metadata.AllocationExtentRecord{
				{LogicalChunkStart: 0, ChunkCount: 1, Kind: metadata.AllocationKindData, PhysicalChunkStart: 500},
				{LogicalChunkStart: 1, ChunkCount: 1, Kind: metadata.AllocationKindZero},
			},
		},
	}

	spec := service.NormalizeVolumeSpec(service.VolumeSpec{
		ID:              service.HexVolumeID(0x00a1b2c3),
		Name:            "vol-a",
		Prefix:          "vol-a-00a1b2c3",
		SizeBytes:       8,
		BlockSize:       1,
		ChunkSizeBytes:  4,
		ExtentPageBytes: 8,
	})

	sourceA := newTrackingSBSClient(service.NewInMemorySBSClient([]service.VolumeSpec{spec}))
	sourceB := newTrackingSBSClient(service.NewInMemorySBSClient([]service.VolumeSpec{spec}))
	sourceC := newTrackingSBSClient(service.NewInMemorySBSClient([]service.VolumeSpec{spec}))
	targetD := newTrackingSBSClient(service.NewInMemorySBSClient([]service.VolumeSpec{spec}))
	targetE := newTrackingSBSClient(service.NewInMemorySBSClient([]service.VolumeSpec{spec}))
	targetF := newTrackingSBSClient(service.NewInMemorySBSClient([]service.VolumeSpec{spec}))
	replicaClients := map[string]service.SBSClient{
		"rep-a": sourceA,
		"rep-b": sourceB,
		"rep-c": sourceC,
		"rep-d": targetD,
		"rep-e": targetE,
		"rep-f": targetF,
	}

	sourceReplicas, err := replication.OpenReplicaSessions(context.Background(), map[string]service.SBSClient{
		"rep-a": sourceA,
		"rep-b": sourceB,
		"rep-c": sourceC,
	}, replication.OpenReplicaSessionsRequest{
		VolumeID:      "00a1b2c3",
		GatewayID:     "gw-a",
		HostID:        "host-a",
		AttachmentID:  "seed-source",
		Generation:    1,
		SessionPrefix: "seed-source",
	})
	if err != nil {
		t.Fatalf("OpenReplicaSessions seed: %v", err)
	}
	for _, replica := range sourceReplicas {
		if _, err := replica.Client.Write(context.Background(), &service.WriteRequest{
			VolumeID:     replica.VolumeID,
			VolumeHandle: replica.VolumeHandle,
			OffsetBytes:  0,
			LengthBytes:  4,
			Data:         []byte("ABCD"),
			Context: service.SBSRequestContext{
				RequestID:      "seed-source-data",
				GatewayID:      replica.GatewayID,
				HostID:         replica.HostID,
				SessionID:      replica.SessionID,
				AttachmentID:   replica.AttachmentID,
				Generation:     replica.Generation,
				IdempotencyKey: "seed-source-data",
			},
		}); err != nil {
			t.Fatalf("seed source write: %v", err)
		}
	}
	for _, replica := range sourceReplicas {
		_, _ = replica.Client.CloseVolume(context.Background(), &service.CloseVolumeRequest{
			VolumeID:     replica.VolumeID,
			VolumeHandle: replica.VolumeHandle,
			Context: service.SBSRequestContext{
				RequestID:    "seed-source-close",
				GatewayID:    replica.GatewayID,
				HostID:       replica.HostID,
				SessionID:    replica.SessionID,
				AttachmentID: replica.AttachmentID,
				Generation:   replica.Generation,
			},
		})
	}

	svc := NewService(store)
	svc.now = func() time.Time { return time.Unix(1000, 0) }

	if _, err := svc.EnqueueRebalance(context.Background(), "00a1b2c3", 1, "rs-2"); err != nil {
		t.Fatalf("EnqueueRebalance: %v", err)
	}

	transition, err := svc.ApplyTransition(context.Background(), "00a1b2c3", "pl-1", replicaClients, "gw-a", "host-a")
	if err != nil {
		t.Fatalf("ApplyTransition mixed allocation: %v", err)
	}
	if transition.State != metadata.PlacementTransitionCompleted {
		t.Fatalf("transition state=%q want=%q", transition.State, metadata.PlacementTransitionCompleted)
	}
	if store.mappings[0].PlacementRef != "pl-2" {
		t.Fatalf("placement_ref=%q want=pl-2", store.mappings[0].PlacementRef)
	}

	sourceReads := sourceA.snapshotReads()
	if len(sourceReads) != 1 || sourceReads[0].OffsetBytes != 0 || sourceReads[0].LengthBytes != 4 {
		t.Fatalf("source reads=%+v want one data-chunk read at offset 0 length 4", sourceReads)
	}
	if reads := sourceB.snapshotReads(); len(reads) != 0 {
		t.Fatalf("sourceB reads=%+v want none", reads)
	}
	if reads := sourceC.snapshotReads(); len(reads) != 0 {
		t.Fatalf("sourceC reads=%+v want none", reads)
	}

	for name, client := range map[string]*trackingSBSClient{
		"rep-d": targetD,
		"rep-e": targetE,
		"rep-f": targetF,
	} {
		writes := client.snapshotWrites()
		if len(writes) != 1 || writes[0].OffsetBytes != 0 || writes[0].LengthBytes != 4 {
			t.Fatalf("%s writes=%+v want one data-chunk write at offset 0 length 4", name, writes)
		}
	}
}

func TestApplyTransitionPrioritizesRecentMutationPages(t *testing.T) {
	store := newFakeStore()
	store.mappings = []metadata.ExtentMappingRecord{
		{VolumeID: "00a1b2c3", ExtentID: 1, LogicalOffset: 0, LengthBytes: 16, ChunkID: 0, PlacementRef: "pl-1", Revision: 11},
	}
	store.replicaSets = append(store.replicaSets, metadata.ReplicaSetState{
		ReplicaSetID:     "rs-2",
		VolumeID:         "00a1b2c3",
		PlacementRef:     "pl-2",
		Epoch:            5,
		PrimaryReplicaID: "rep-d",
		WriteQuorum:      2,
		ReadQuorum:       1,
		Replicas: []metadata.ReplicaDescriptor{
			{NodeID: "node-d", ReplicaID: "rep-d", Role: metadata.ReplicaRolePrimary},
			{NodeID: "node-e", ReplicaID: "rep-e", Role: metadata.ReplicaRoleSecondary},
			{NodeID: "node-f", ReplicaID: "rep-f", Role: metadata.ReplicaRoleSecondary},
		},
	})
	store.nodes["node-d"] = metadata.NodeMembershipRecord{NodeID: "node-d", LifecycleState: metadata.NodeLifecycleActive, HealthState: metadata.NodeHealthHealthy}
	store.nodes["node-e"] = metadata.NodeMembershipRecord{NodeID: "node-e", LifecycleState: metadata.NodeLifecycleActive, HealthState: metadata.NodeHealthHealthy}
	store.nodes["node-f"] = metadata.NodeMembershipRecord{NodeID: "node-f", LifecycleState: metadata.NodeLifecycleActive, HealthState: metadata.NodeHealthHealthy}
	store.allocationPages = []metadata.AllocationPageRecord{
		{
			VolumeID:       "00a1b2c3",
			PageNo:         0,
			PageBytes:      8,
			ChunkSizeBytes: 4,
			Extents: []metadata.AllocationExtentRecord{
				{LogicalChunkStart: 0, ChunkCount: 2, Kind: metadata.AllocationKindData, PhysicalChunkStart: 500},
			},
		},
		{
			VolumeID:       "00a1b2c3",
			PageNo:         1,
			PageBytes:      8,
			ChunkSizeBytes: 4,
			Extents: []metadata.AllocationExtentRecord{
				{LogicalChunkStart: 2, ChunkCount: 2, Kind: metadata.AllocationKindData, PhysicalChunkStart: 600},
			},
		},
	}
	store.mutationOps["write-recent-page-1"] = metadata.MutationOperationRecord{
		OperationID:       "write-recent-page-1",
		VolumeID:          "00a1b2c3",
		Kind:              "write",
		State:             metadata.MutationOperationCommitted,
		AffectedExtentIDs: []uint64{1},
		AffectedPageNos:   []uint64{1},
		LastUpdatedAtUnix: 2000,
	}

	spec := service.NormalizeVolumeSpec(service.VolumeSpec{
		ID:              service.HexVolumeID(0x00a1b2c3),
		Name:            "vol-a",
		Prefix:          "vol-a-00a1b2c3",
		SizeBytes:       16,
		BlockSize:       1,
		ChunkSizeBytes:  4,
		ExtentPageBytes: 8,
	})

	sourceA := newTrackingSBSClient(service.NewInMemorySBSClient([]service.VolumeSpec{spec}))
	sourceB := newTrackingSBSClient(service.NewInMemorySBSClient([]service.VolumeSpec{spec}))
	sourceC := newTrackingSBSClient(service.NewInMemorySBSClient([]service.VolumeSpec{spec}))
	targetD := newTrackingSBSClient(service.NewInMemorySBSClient([]service.VolumeSpec{spec}))
	targetE := newTrackingSBSClient(service.NewInMemorySBSClient([]service.VolumeSpec{spec}))
	targetF := newTrackingSBSClient(service.NewInMemorySBSClient([]service.VolumeSpec{spec}))
	replicaClients := map[string]service.SBSClient{
		"rep-a": sourceA,
		"rep-b": sourceB,
		"rep-c": sourceC,
		"rep-d": targetD,
		"rep-e": targetE,
		"rep-f": targetF,
	}

	sourceReplicas, err := replication.OpenReplicaSessions(context.Background(), map[string]service.SBSClient{
		"rep-a": sourceA,
		"rep-b": sourceB,
		"rep-c": sourceC,
	}, replication.OpenReplicaSessionsRequest{
		VolumeID:      "00a1b2c3",
		GatewayID:     "gw-a",
		HostID:        "host-a",
		AttachmentID:  "seed-source",
		Generation:    1,
		SessionPrefix: "seed-source",
	})
	if err != nil {
		t.Fatalf("OpenReplicaSessions seed: %v", err)
	}
	for _, replica := range sourceReplicas {
		if _, err := replica.Client.Write(context.Background(), &service.WriteRequest{
			VolumeID:     replica.VolumeID,
			VolumeHandle: replica.VolumeHandle,
			OffsetBytes:  0,
			LengthBytes:  16,
			Data:         []byte("ABCDEFGHIJKLMNOP"),
			Context: service.SBSRequestContext{
				RequestID:      "seed-source-data",
				GatewayID:      replica.GatewayID,
				HostID:         replica.HostID,
				SessionID:      replica.SessionID,
				AttachmentID:   replica.AttachmentID,
				Generation:     replica.Generation,
				IdempotencyKey: "seed-source-data",
			},
		}); err != nil {
			t.Fatalf("seed source write: %v", err)
		}
	}
	for _, replica := range sourceReplicas {
		_, _ = replica.Client.CloseVolume(context.Background(), &service.CloseVolumeRequest{
			VolumeID:     replica.VolumeID,
			VolumeHandle: replica.VolumeHandle,
			Context: service.SBSRequestContext{
				RequestID:    "seed-source-close",
				GatewayID:    replica.GatewayID,
				HostID:       replica.HostID,
				SessionID:    replica.SessionID,
				AttachmentID: replica.AttachmentID,
				Generation:   replica.Generation,
			},
		})
	}

	svc := NewService(store)
	svc.now = func() time.Time { return time.Unix(1000, 0) }

	if _, err := svc.EnqueueRebalance(context.Background(), "00a1b2c3", 1, "rs-2"); err != nil {
		t.Fatalf("EnqueueRebalance: %v", err)
	}

	transition, err := svc.ApplyTransition(context.Background(), "00a1b2c3", "pl-1", replicaClients, "gw-a", "host-a")
	if err != nil {
		t.Fatalf("ApplyTransition recent pages: %v", err)
	}
	if transition.State != metadata.PlacementTransitionCompleted {
		t.Fatalf("transition state=%q want=%q", transition.State, metadata.PlacementTransitionCompleted)
	}

	sourceReads := sourceA.snapshotReads()
	if len(sourceReads) != 4 {
		t.Fatalf("source reads=%+v want four chunk reads", sourceReads)
	}
	if sourceReads[0].OffsetBytes != 8 || sourceReads[0].LengthBytes != 4 {
		t.Fatalf("first source read=%+v want offset 8 length 4", sourceReads[0])
	}
	if sourceReads[1].OffsetBytes != 12 || sourceReads[1].LengthBytes != 4 {
		t.Fatalf("second source read=%+v want offset 12 length 4", sourceReads[1])
	}
	if sourceReads[2].OffsetBytes != 0 || sourceReads[2].LengthBytes != 4 {
		t.Fatalf("third source read=%+v want offset 0 length 4", sourceReads[2])
	}
	if sourceReads[3].OffsetBytes != 4 || sourceReads[3].LengthBytes != 4 {
		t.Fatalf("fourth source read=%+v want offset 4 length 4", sourceReads[3])
	}
	for name, client := range map[string]*trackingSBSClient{
		"rep-d": targetD,
		"rep-e": targetE,
		"rep-f": targetF,
	} {
		writes := client.snapshotWrites()
		if len(writes) != 4 {
			t.Fatalf("%s writes=%+v want four physical chunk writes", name, writes)
		}
		firstPageOffsets := []uint64{writes[0].OffsetBytes, writes[1].OffsetBytes}
		secondPageOffsets := []uint64{writes[2].OffsetBytes, writes[3].OffsetBytes}
		sort.Slice(firstPageOffsets, func(i, j int) bool { return firstPageOffsets[i] < firstPageOffsets[j] })
		sort.Slice(secondPageOffsets, func(i, j int) bool { return secondPageOffsets[i] < secondPageOffsets[j] })
		if writes[0].LengthBytes != 4 || writes[1].LengthBytes != 4 ||
			writes[2].LengthBytes != 4 || writes[3].LengthBytes != 4 ||
			firstPageOffsets[0] != 8 || firstPageOffsets[1] != 12 ||
			secondPageOffsets[0] != 0 || secondPageOffsets[1] != 4 {
			t.Fatalf("%s writes=%+v want recent page physical chunk writes first", name, writes)
		}
	}
}

func TestApplyTransitionCopiesCloneDeltaAllocationPages(t *testing.T) {
	store := newFakeStore()
	store.replicaSets = append(store.replicaSets, metadata.ReplicaSetState{
		ReplicaSetID:     "rs-2",
		VolumeID:         "00a1b2c3",
		PlacementRef:     "pl-2",
		Epoch:            5,
		PrimaryReplicaID: "rep-d",
		WriteQuorum:      2,
		ReadQuorum:       1,
		Replicas: []metadata.ReplicaDescriptor{
			{NodeID: "node-d", ReplicaID: "rep-d", Role: metadata.ReplicaRolePrimary},
			{NodeID: "node-e", ReplicaID: "rep-e", Role: metadata.ReplicaRoleSecondary},
			{NodeID: "node-f", ReplicaID: "rep-f", Role: metadata.ReplicaRoleSecondary},
		},
	})
	store.nodes["node-d"] = metadata.NodeMembershipRecord{NodeID: "node-d", LifecycleState: metadata.NodeLifecycleActive, HealthState: metadata.NodeHealthHealthy}
	store.nodes["node-e"] = metadata.NodeMembershipRecord{NodeID: "node-e", LifecycleState: metadata.NodeLifecycleActive, HealthState: metadata.NodeHealthHealthy}
	store.nodes["node-f"] = metadata.NodeMembershipRecord{NodeID: "node-f", LifecycleState: metadata.NodeLifecycleActive, HealthState: metadata.NodeHealthHealthy}
	store.allocationPages = []metadata.AllocationPageRecord{
		{
			VolumeID:       "00a1b2c3",
			PageNo:         0,
			PageBytes:      8,
			ChunkSizeBytes: 4,
			Extents: []metadata.AllocationExtentRecord{
				{LogicalChunkStart: 0, ChunkCount: 1, Kind: metadata.AllocationKindData, PhysicalChunkStart: 500},
				{LogicalChunkStart: 1, ChunkCount: 1, Kind: metadata.AllocationKindZero},
			},
		},
	}
	store.snapshotRecords = []metadata.SnapshotRecord{
		{
			SnapshotID:               "snap-1",
			SourceVolumeID:           "00a1b2c3",
			SnapshotRootID:           "snap-1",
			State:                    metadata.SnapshotStateAvailable,
			AllocationChunkSizeBytes: 4,
			AllocationPageSizeBytes:  8,
		},
	}
	store.cloneRecords = []metadata.CloneRecord{
		{
			CloneID:                  "clone-1",
			SourceSnapshotID:         "snap-1",
			SourceVolumeID:           "00a1b2c3",
			CloneBaseRootID:          "snap-1",
			State:                    metadata.CloneStateAvailable,
			AllocationChunkSizeBytes: 4,
			AllocationPageSizeBytes:  8,
		},
	}
	store.cloneDeltaAllocationPages["clone-1"] = []metadata.AllocationPageRecord{
		{
			VolumeID:       "00a1b2c3",
			PageNo:         0,
			PageBytes:      8,
			ChunkSizeBytes: 4,
			Extents: []metadata.AllocationExtentRecord{
				{LogicalChunkStart: 0, ChunkCount: 1, Kind: metadata.AllocationKindData, PhysicalChunkStart: 900},
				{LogicalChunkStart: 1, ChunkCount: 1, Kind: metadata.AllocationKindZero},
			},
		},
	}

	spec := service.NormalizeVolumeSpec(service.VolumeSpec{
		ID:              service.HexVolumeID(0x00a1b2c3),
		Name:            "vol-a",
		Prefix:          "vol-a-00a1b2c3",
		SizeBytes:       4096,
		BlockSize:       1,
		ChunkSizeBytes:  4,
		ExtentPageBytes: 8,
	})

	sourceA := service.NewInMemorySBSClient([]service.VolumeSpec{spec})
	sourceB := service.NewInMemorySBSClient([]service.VolumeSpec{spec})
	sourceC := service.NewInMemorySBSClient([]service.VolumeSpec{spec})
	targetD := service.NewInMemorySBSClient([]service.VolumeSpec{spec})
	targetE := service.NewInMemorySBSClient([]service.VolumeSpec{spec})
	targetF := service.NewInMemorySBSClient([]service.VolumeSpec{spec})
	replicaClients := map[string]service.SBSClient{
		"rep-a": sourceA,
		"rep-b": sourceB,
		"rep-c": sourceC,
		"rep-d": targetD,
		"rep-e": targetE,
		"rep-f": targetF,
	}
	for name, client := range map[string]*service.InMemorySBSClient{"rep-a": sourceA, "rep-b": sourceB, "rep-c": sourceC} {
		seedPhysicalChunk(t, client, "00a1b2c3", name, 500, []byte("LIVE"))
		seedPhysicalChunk(t, client, "00a1b2c3", name, 900, []byte("CLON"))
	}

	svc := NewService(store)
	svc.now = func() time.Time { return time.Unix(1000, 0) }

	if _, err := svc.EnqueueRebalance(context.Background(), "00a1b2c3", 1, "rs-2"); err != nil {
		t.Fatalf("EnqueueRebalance: %v", err)
	}
	transition, err := svc.ApplyTransition(context.Background(), "00a1b2c3", "pl-1", replicaClients, "gw-a", "host-a")
	if err != nil {
		t.Fatalf("ApplyTransition clone delta: %v", err)
	}
	if transition.State != metadata.PlacementTransitionCompleted {
		t.Fatalf("transition state=%q want=%q", transition.State, metadata.PlacementTransitionCompleted)
	}
	for name, client := range map[string]*service.InMemorySBSClient{"rep-d": targetD, "rep-e": targetE, "rep-f": targetF} {
		gotLive := readPhysicalChunk(t, client, "00a1b2c3", name, 500, 4)
		if string(gotLive) != "LIVE" {
			t.Fatalf("%s live chunk=%q want LIVE", name, string(gotLive))
		}
		gotClone := readPhysicalChunk(t, client, "00a1b2c3", name, 900, 4)
		if string(gotClone) != "CLON" {
			t.Fatalf("%s clone delta chunk=%q want CLON", name, string(gotClone))
		}
	}
}

func seedPhysicalChunk(t *testing.T, client *service.InMemorySBSClient, volumeID, replicaID string, physicalChunkID uint64, data []byte) {
	t.Helper()
	handle, ctx := openTestPhysicalChunkVolume(t, client, volumeID, replicaID, "seed")
	if _, err := client.WritePhysicalChunk(context.Background(), &service.WritePhysicalChunkRequest{
		VolumeID:         volumeID,
		VolumeHandle:     handle,
		PhysicalChunkID:  physicalChunkID,
		ChunkOffsetBytes: 0,
		LengthBytes:      uint64(len(data)),
		Data:             append([]byte(nil), data...),
		Context:          ctx,
	}); err != nil {
		t.Fatalf("seed physical chunk %d on %s: %v", physicalChunkID, replicaID, err)
	}
	_, _ = client.CloseVolume(context.Background(), &service.CloseVolumeRequest{
		VolumeID:     volumeID,
		VolumeHandle: handle,
		Context:      ctx,
	})
}

func readPhysicalChunk(t *testing.T, client *service.InMemorySBSClient, volumeID, replicaID string, physicalChunkID, lengthBytes uint64) []byte {
	t.Helper()
	handle, ctx := openTestPhysicalChunkVolume(t, client, volumeID, replicaID, "verify")
	resp, err := client.ReadPhysicalChunk(context.Background(), &service.ReadPhysicalChunkRequest{
		VolumeID:         volumeID,
		VolumeHandle:     handle,
		PhysicalChunkID:  physicalChunkID,
		ChunkOffsetBytes: 0,
		LengthBytes:      lengthBytes,
		Context:          ctx,
	})
	if err != nil {
		t.Fatalf("read physical chunk %d on %s: %v", physicalChunkID, replicaID, err)
	}
	_, _ = client.CloseVolume(context.Background(), &service.CloseVolumeRequest{
		VolumeID:     volumeID,
		VolumeHandle: handle,
		Context:      ctx,
	})
	return resp.Data
}

func openTestPhysicalChunkVolume(t *testing.T, client *service.InMemorySBSClient, volumeID, replicaID, prefix string) (string, service.SBSRequestContext) {
	t.Helper()
	attachmentID := prefix + "-" + replicaID
	reqCtx := service.SBSRequestContext{
		RequestID:    prefix + "-" + replicaID + "-open",
		GatewayID:    "gw-a",
		HostID:       "host-a",
		SessionID:    prefix + "-" + replicaID,
		AttachmentID: attachmentID,
		Generation:   1,
	}
	open, err := client.OpenVolume(context.Background(), &service.OpenVolumeRequest{
		VolumeID:   volumeID,
		AccessMode: service.SBSAccessModeExclusiveWriter,
		Context:    reqCtx,
	})
	if err != nil {
		t.Fatalf("open %s on %s: %v", prefix, replicaID, err)
	}
	return open.VolumeHandle, service.SBSRequestContext{
		RequestID:      prefix + "-" + replicaID,
		GatewayID:      "gw-a",
		HostID:         "host-a",
		SessionID:      reqCtx.SessionID,
		AttachmentID:   attachmentID,
		Generation:     1,
		IdempotencyKey: prefix + "-" + replicaID,
	}
}

func TestApplyTransitionResumesFromCompletedRecentPages(t *testing.T) {
	store := newFakeStore()
	store.mappings = []metadata.ExtentMappingRecord{
		{VolumeID: "00a1b2c3", ExtentID: 1, LogicalOffset: 0, LengthBytes: 16, ChunkID: 0, PlacementRef: "pl-1", Revision: 11},
	}
	store.replicaSets = append(store.replicaSets, metadata.ReplicaSetState{
		ReplicaSetID:     "rs-2",
		VolumeID:         "00a1b2c3",
		PlacementRef:     "pl-2",
		Epoch:            5,
		PrimaryReplicaID: "rep-d",
		WriteQuorum:      2,
		ReadQuorum:       1,
		Replicas: []metadata.ReplicaDescriptor{
			{NodeID: "node-d", ReplicaID: "rep-d", Role: metadata.ReplicaRolePrimary},
			{NodeID: "node-e", ReplicaID: "rep-e", Role: metadata.ReplicaRoleSecondary},
			{NodeID: "node-f", ReplicaID: "rep-f", Role: metadata.ReplicaRoleSecondary},
		},
	})
	store.nodes["node-d"] = metadata.NodeMembershipRecord{NodeID: "node-d", LifecycleState: metadata.NodeLifecycleActive, HealthState: metadata.NodeHealthHealthy}
	store.nodes["node-e"] = metadata.NodeMembershipRecord{NodeID: "node-e", LifecycleState: metadata.NodeLifecycleActive, HealthState: metadata.NodeHealthHealthy}
	store.nodes["node-f"] = metadata.NodeMembershipRecord{NodeID: "node-f", LifecycleState: metadata.NodeLifecycleActive, HealthState: metadata.NodeHealthHealthy}
	store.allocationPages = []metadata.AllocationPageRecord{
		{
			VolumeID:       "00a1b2c3",
			PageNo:         0,
			PageBytes:      8,
			ChunkSizeBytes: 4,
			Extents: []metadata.AllocationExtentRecord{
				{LogicalChunkStart: 0, ChunkCount: 2, Kind: metadata.AllocationKindData, PhysicalChunkStart: 500},
			},
		},
		{
			VolumeID:       "00a1b2c3",
			PageNo:         1,
			PageBytes:      8,
			ChunkSizeBytes: 4,
			Extents: []metadata.AllocationExtentRecord{
				{LogicalChunkStart: 2, ChunkCount: 2, Kind: metadata.AllocationKindData, PhysicalChunkStart: 600},
			},
		},
	}
	store.mutationOps["write-recent-page-1"] = metadata.MutationOperationRecord{
		OperationID:       "write-recent-page-1",
		VolumeID:          "00a1b2c3",
		Kind:              "write",
		State:             metadata.MutationOperationCommitted,
		AffectedExtentIDs: []uint64{1},
		AffectedPageNos:   []uint64{1},
		LastUpdatedAtUnix: 2000,
	}

	spec := service.NormalizeVolumeSpec(service.VolumeSpec{
		ID:              service.HexVolumeID(0x00a1b2c3),
		Name:            "vol-a",
		Prefix:          "vol-a-00a1b2c3",
		SizeBytes:       16,
		BlockSize:       1,
		ChunkSizeBytes:  4,
		ExtentPageBytes: 8,
	})

	sourceA := newTrackingSBSClient(service.NewInMemorySBSClient([]service.VolumeSpec{spec}))
	sourceB := newTrackingSBSClient(service.NewInMemorySBSClient([]service.VolumeSpec{spec}))
	sourceC := newTrackingSBSClient(service.NewInMemorySBSClient([]service.VolumeSpec{spec}))
	targetD := newTrackingSBSClient(service.NewInMemorySBSClient([]service.VolumeSpec{spec}))
	targetE := newTrackingSBSClient(service.NewInMemorySBSClient([]service.VolumeSpec{spec}))
	targetF := newTrackingSBSClient(service.NewInMemorySBSClient([]service.VolumeSpec{spec}))

	replicaClients := map[string]service.SBSClient{
		"rep-a": sourceA,
		"rep-b": sourceB,
		"rep-c": sourceC,
		"rep-d": targetD,
		"rep-e": targetE,
		"rep-f": targetF,
	}

	sourceReplicas, err := replication.OpenReplicaSessions(context.Background(), map[string]service.SBSClient{
		"rep-a": sourceA,
		"rep-b": sourceB,
		"rep-c": sourceC,
	}, replication.OpenReplicaSessionsRequest{
		VolumeID:      "00a1b2c3",
		GatewayID:     "gw-a",
		HostID:        "host-a",
		AttachmentID:  "seed-source",
		Generation:    1,
		SessionPrefix: "seed-source",
	})
	if err != nil {
		t.Fatalf("OpenReplicaSessions seed: %v", err)
	}
	for _, replica := range sourceReplicas {
		if _, err := replica.Client.Write(context.Background(), &service.WriteRequest{
			VolumeID:     replica.VolumeID,
			VolumeHandle: replica.VolumeHandle,
			OffsetBytes:  0,
			LengthBytes:  16,
			Data:         []byte("ABCDEFGHIJKLMNOP"),
			Context: service.SBSRequestContext{
				RequestID:      "seed-source-data",
				GatewayID:      replica.GatewayID,
				HostID:         replica.HostID,
				SessionID:      replica.SessionID,
				AttachmentID:   replica.AttachmentID,
				Generation:     replica.Generation,
				IdempotencyKey: "seed-source-data",
			},
		}); err != nil {
			t.Fatalf("seed source write: %v", err)
		}
	}
	for _, replica := range sourceReplicas {
		_, _ = replica.Client.CloseVolume(context.Background(), &service.CloseVolumeRequest{
			VolumeID:     replica.VolumeID,
			VolumeHandle: replica.VolumeHandle,
			Context: service.SBSRequestContext{
				RequestID:    "seed-source-close",
				GatewayID:    replica.GatewayID,
				HostID:       replica.HostID,
				SessionID:    replica.SessionID,
				AttachmentID: replica.AttachmentID,
				Generation:   replica.Generation,
			},
		})
	}

	svc := NewService(store)
	svc.now = func() time.Time { return time.Unix(1000, 0) }

	if _, err := svc.EnqueueRebalance(context.Background(), "00a1b2c3", 1, "rs-2"); err != nil {
		t.Fatalf("EnqueueRebalance: %v", err)
	}
	store.mutationOps[transitionMutationOperationID(store.transitions["pl-1"])] = metadata.MutationOperationRecord{
		OperationID:       transitionMutationOperationID(store.transitions["pl-1"]),
		VolumeID:          "00a1b2c3",
		Kind:              "transition",
		State:             metadata.MutationOperationFailed,
		IdempotencyKey:    "pl-1",
		AffectedExtentIDs: []uint64{1},
		AffectedPageNos:   []uint64{0, 1},
		CompletedPageNos:  []uint64{1},
		LastUpdatedAtUnix: 1000,
	}
	if _, err := svc.ApplyTransition(context.Background(), "00a1b2c3", "pl-1", replicaClients, "gw-a", "host-a"); err != nil {
		t.Fatalf("ApplyTransition retry: %v", err)
	}

	allReads := sourceA.snapshotReads()
	if len(allReads) != 2 {
		t.Fatalf("all reads=%+v want two remaining-page reads", allReads)
	}
	if allReads[0].OffsetBytes != 0 || allReads[1].OffsetBytes != 4 {
		t.Fatalf("retry reads=%+v want only remaining page reads", allReads)
	}
	operation := store.mutationOps[transitionMutationOperationID(store.transitions["pl-1"])]
	if operation.State != metadata.MutationOperationCommitted {
		t.Fatalf("operation state after retry=%q want=%q", operation.State, metadata.MutationOperationCommitted)
	}
	if len(operation.CompletedPageNos) != 2 || operation.CompletedPageNos[0] != 0 || operation.CompletedPageNos[1] != 1 {
		t.Fatalf("completed pages after retry=%v want=[0 1]", operation.CompletedPageNos)
	}
	page0 := store.mutationOps[transitionPageBatchMutationOperationID(store.transitions["pl-1"], 0)]
	if page0.Kind != "transition_batch" || page0.State != metadata.MutationOperationCommitted || len(page0.CompletedPageNos) != 1 || page0.CompletedPageNos[0] != 0 {
		t.Fatalf("page0 batch=%+v", page0)
	}
	if _, ok := store.mutationOps[transitionPageBatchMutationOperationID(store.transitions["pl-1"], 1)]; ok {
		t.Fatalf("page1 batch should not be recreated when parent progress already marks it complete")
	}
}

func TestApplyTransitionPrioritizesFailedTransitionBatchPages(t *testing.T) {
	store := newFakeStore()
	store.mappings = []metadata.ExtentMappingRecord{
		{VolumeID: "00a1b2c3", ExtentID: 1, LogicalOffset: 0, LengthBytes: 16, ChunkID: 0, PlacementRef: "pl-1", Revision: 11},
	}
	store.replicaSets = append(store.replicaSets, metadata.ReplicaSetState{
		ReplicaSetID:     "rs-2",
		VolumeID:         "00a1b2c3",
		PlacementRef:     "pl-2",
		Epoch:            5,
		PrimaryReplicaID: "rep-d",
		WriteQuorum:      2,
		ReadQuorum:       1,
		Replicas: []metadata.ReplicaDescriptor{
			{NodeID: "node-d", ReplicaID: "rep-d", Role: metadata.ReplicaRolePrimary},
			{NodeID: "node-e", ReplicaID: "rep-e", Role: metadata.ReplicaRoleSecondary},
			{NodeID: "node-f", ReplicaID: "rep-f", Role: metadata.ReplicaRoleSecondary},
		},
	})
	store.nodes["node-d"] = metadata.NodeMembershipRecord{NodeID: "node-d", LifecycleState: metadata.NodeLifecycleActive, HealthState: metadata.NodeHealthHealthy}
	store.nodes["node-e"] = metadata.NodeMembershipRecord{NodeID: "node-e", LifecycleState: metadata.NodeLifecycleActive, HealthState: metadata.NodeHealthHealthy}
	store.nodes["node-f"] = metadata.NodeMembershipRecord{NodeID: "node-f", LifecycleState: metadata.NodeLifecycleActive, HealthState: metadata.NodeHealthHealthy}
	store.allocationPages = []metadata.AllocationPageRecord{
		{
			VolumeID:       "00a1b2c3",
			PageNo:         0,
			PageBytes:      8,
			ChunkSizeBytes: 4,
			Extents: []metadata.AllocationExtentRecord{
				{LogicalChunkStart: 0, ChunkCount: 2, Kind: metadata.AllocationKindData, PhysicalChunkStart: 500},
			},
		},
		{
			VolumeID:       "00a1b2c3",
			PageNo:         1,
			PageBytes:      8,
			ChunkSizeBytes: 4,
			Extents: []metadata.AllocationExtentRecord{
				{LogicalChunkStart: 2, ChunkCount: 2, Kind: metadata.AllocationKindData, PhysicalChunkStart: 600},
			},
		},
	}
	store.mutationOps["write-recent-page-1"] = metadata.MutationOperationRecord{
		OperationID:       "write-recent-page-1",
		VolumeID:          "00a1b2c3",
		Kind:              "write",
		State:             metadata.MutationOperationCommitted,
		AffectedExtentIDs: []uint64{1},
		AffectedPageNos:   []uint64{1},
		LastUpdatedAtUnix: 2000,
	}

	spec := service.NormalizeVolumeSpec(service.VolumeSpec{
		ID:              service.HexVolumeID(0x00a1b2c3),
		Name:            "vol-a",
		Prefix:          "vol-a-00a1b2c3",
		SizeBytes:       16,
		BlockSize:       1,
		ChunkSizeBytes:  4,
		ExtentPageBytes: 8,
	})

	sourceA := newTrackingSBSClient(service.NewInMemorySBSClient([]service.VolumeSpec{spec}))
	sourceB := newTrackingSBSClient(service.NewInMemorySBSClient([]service.VolumeSpec{spec}))
	sourceC := newTrackingSBSClient(service.NewInMemorySBSClient([]service.VolumeSpec{spec}))
	targetD := newTrackingSBSClient(service.NewInMemorySBSClient([]service.VolumeSpec{spec}))
	targetE := newTrackingSBSClient(service.NewInMemorySBSClient([]service.VolumeSpec{spec}))
	targetF := newTrackingSBSClient(service.NewInMemorySBSClient([]service.VolumeSpec{spec}))
	replicaClients := map[string]service.SBSClient{
		"rep-a": sourceA,
		"rep-b": sourceB,
		"rep-c": sourceC,
		"rep-d": targetD,
		"rep-e": targetE,
		"rep-f": targetF,
	}

	sourceReplicas, err := replication.OpenReplicaSessions(context.Background(), map[string]service.SBSClient{
		"rep-a": sourceA,
		"rep-b": sourceB,
		"rep-c": sourceC,
	}, replication.OpenReplicaSessionsRequest{
		VolumeID:      "00a1b2c3",
		GatewayID:     "gw-a",
		HostID:        "host-a",
		AttachmentID:  "seed-source",
		Generation:    1,
		SessionPrefix: "seed-source",
	})
	if err != nil {
		t.Fatalf("OpenReplicaSessions seed: %v", err)
	}
	for _, replica := range sourceReplicas {
		if _, err := replica.Client.Write(context.Background(), &service.WriteRequest{
			VolumeID:     replica.VolumeID,
			VolumeHandle: replica.VolumeHandle,
			OffsetBytes:  0,
			LengthBytes:  16,
			Data:         []byte("ABCDEFGHIJKLMNOP"),
			Context: service.SBSRequestContext{
				RequestID:      "seed-source-data",
				GatewayID:      replica.GatewayID,
				HostID:         replica.HostID,
				SessionID:      replica.SessionID,
				AttachmentID:   replica.AttachmentID,
				Generation:     replica.Generation,
				IdempotencyKey: "seed-source-data",
			},
		}); err != nil {
			t.Fatalf("seed source write: %v", err)
		}
	}
	for _, replica := range sourceReplicas {
		_, _ = replica.Client.CloseVolume(context.Background(), &service.CloseVolumeRequest{
			VolumeID:     replica.VolumeID,
			VolumeHandle: replica.VolumeHandle,
			Context: service.SBSRequestContext{
				RequestID:    "seed-source-close",
				GatewayID:    replica.GatewayID,
				HostID:       replica.HostID,
				SessionID:    replica.SessionID,
				AttachmentID: replica.AttachmentID,
				Generation:   replica.Generation,
			},
		})
	}

	svc := NewService(store)
	svc.now = func() time.Time { return time.Unix(1000, 0) }

	if _, err := svc.EnqueueRebalance(context.Background(), "00a1b2c3", 1, "rs-2"); err != nil {
		t.Fatalf("EnqueueRebalance: %v", err)
	}
	store.mutationOps[transitionMutationOperationID(store.transitions["pl-1"])] = metadata.MutationOperationRecord{
		OperationID:       transitionMutationOperationID(store.transitions["pl-1"]),
		VolumeID:          "00a1b2c3",
		Kind:              "transition",
		State:             metadata.MutationOperationFailed,
		IdempotencyKey:    "pl-1",
		AffectedExtentIDs: []uint64{1},
		AffectedPageNos:   []uint64{0, 1},
		LastUpdatedAtUnix: 1000,
	}
	store.mutationOps[transitionPageBatchMutationOperationID(store.transitions["pl-1"], 0)] = metadata.MutationOperationRecord{
		OperationID:       transitionPageBatchMutationOperationID(store.transitions["pl-1"], 0),
		VolumeID:          "00a1b2c3",
		Kind:              "transition_batch",
		State:             metadata.MutationOperationFailed,
		IdempotencyKey:    transitionMutationOperationID(store.transitions["pl-1"]),
		AffectedExtentIDs: []uint64{1},
		AffectedPageNos:   []uint64{0},
		LastUpdatedAtUnix: 1001,
	}

	transition, err := svc.ApplyTransition(context.Background(), "00a1b2c3", "pl-1", replicaClients, "gw-a", "host-a")
	if err != nil {
		t.Fatalf("ApplyTransition failed-batch priority: %v", err)
	}
	if transition.State != metadata.PlacementTransitionCompleted {
		t.Fatalf("transition state=%q want=%q", transition.State, metadata.PlacementTransitionCompleted)
	}

	sourceReads := sourceA.snapshotReads()
	if len(sourceReads) != 4 {
		t.Fatalf("source reads=%+v want four chunk reads", sourceReads)
	}
	if sourceReads[0].OffsetBytes != 0 || sourceReads[1].OffsetBytes != 4 || sourceReads[2].OffsetBytes != 8 || sourceReads[3].OffsetBytes != 12 {
		t.Fatalf("source reads=%+v want failed-batch page first then recent page", sourceReads)
	}
}

func TestPrioritizeTransitionPageBatchesPrefersFailedRetryRecentThenSmallerBatches(t *testing.T) {
	batches := []transitionPageBatch{
		{
			PageNos:     []uint64{0, 1},
			ActivePages: []uint64{0, 1},
			Segments: []transitionCopySegment{
				{OffsetBytes: 0, LengthBytes: 8},
				{OffsetBytes: 8, LengthBytes: 8},
			},
		},
		{
			PageNos:     []uint64{2},
			ActivePages: []uint64{2},
			Segments: []transitionCopySegment{
				{OffsetBytes: 16, LengthBytes: 8},
			},
		},
		{
			PageNos:     []uint64{3},
			ActivePages: []uint64{3},
			Segments: []transitionCopySegment{
				{OffsetBytes: 24, LengthBytes: 8},
			},
		},
		{
			PageNos:     []uint64{4},
			ActivePages: []uint64{4},
			Segments: []transitionCopySegment{
				{OffsetBytes: 32, LengthBytes: 8},
			},
		},
	}
	failedPages := map[uint64]struct{}{3: {}}
	retryWindows := [][]uint64{{4}, {0, 1}}
	recentPages := map[uint64]struct{}{2: {}}

	prioritizeTransitionPageBatches(batches, failedPages, retryWindows, recentPages)

	if batches[0].ActivePages[0] != 3 {
		t.Fatalf("batch[0]=%v want failed page 3 first", batches[0].ActivePages)
	}
	if batches[1].ActivePages[0] != 4 {
		t.Fatalf("batch[1]=%v want smaller retry window page 4 second", batches[1].ActivePages)
	}
	if len(batches[2].ActivePages) != 2 || batches[2].ActivePages[0] != 0 || batches[2].ActivePages[1] != 1 {
		t.Fatalf("batch[2]=%v want larger retry batch [0 1] third", batches[2].ActivePages)
	}
	if batches[3].ActivePages[0] != 2 {
		t.Fatalf("batch[3]=%v want recent page 2 last", batches[3].ActivePages)
	}
}

func TestApplyTransitionReusesExistingTransitionBatchWindowOnRetry(t *testing.T) {
	store := newFakeStore()
	store.mappings = []metadata.ExtentMappingRecord{
		{VolumeID: "00a1b2c3", ExtentID: 1, LogicalOffset: 0, LengthBytes: 8, ChunkID: 0, PlacementRef: "pl-1", Revision: 11},
	}
	store.replicaSets = append(store.replicaSets, metadata.ReplicaSetState{
		ReplicaSetID:     "rs-2",
		VolumeID:         "00a1b2c3",
		PlacementRef:     "pl-2",
		Epoch:            5,
		PrimaryReplicaID: "rep-d",
		WriteQuorum:      2,
		ReadQuorum:       1,
		Replicas: []metadata.ReplicaDescriptor{
			{NodeID: "node-d", ReplicaID: "rep-d", Role: metadata.ReplicaRolePrimary},
			{NodeID: "node-e", ReplicaID: "rep-e", Role: metadata.ReplicaRoleSecondary},
			{NodeID: "node-f", ReplicaID: "rep-f", Role: metadata.ReplicaRoleSecondary},
		},
	})
	store.nodes["node-d"] = metadata.NodeMembershipRecord{NodeID: "node-d", LifecycleState: metadata.NodeLifecycleActive, HealthState: metadata.NodeHealthHealthy}
	store.nodes["node-e"] = metadata.NodeMembershipRecord{NodeID: "node-e", LifecycleState: metadata.NodeLifecycleActive, HealthState: metadata.NodeHealthHealthy}
	store.nodes["node-f"] = metadata.NodeMembershipRecord{NodeID: "node-f", LifecycleState: metadata.NodeLifecycleActive, HealthState: metadata.NodeHealthHealthy}
	store.allocationPages = []metadata.AllocationPageRecord{
		{
			VolumeID:       "00a1b2c3",
			PageNo:         0,
			PageBytes:      8,
			ChunkSizeBytes: 4,
			Extents: []metadata.AllocationExtentRecord{
				{LogicalChunkStart: 0, ChunkCount: 2, Kind: metadata.AllocationKindData, PhysicalChunkStart: 500},
			},
		},
	}

	spec := service.NormalizeVolumeSpec(service.VolumeSpec{
		ID:              service.HexVolumeID(0x00a1b2c3),
		Name:            "vol-a",
		Prefix:          "vol-a-00a1b2c3",
		SizeBytes:       8,
		BlockSize:       1,
		ChunkSizeBytes:  4,
		ExtentPageBytes: 8,
	})

	sourceA := newTrackingSBSClient(service.NewInMemorySBSClient([]service.VolumeSpec{spec}))
	sourceB := newTrackingSBSClient(service.NewInMemorySBSClient([]service.VolumeSpec{spec}))
	sourceC := newTrackingSBSClient(service.NewInMemorySBSClient([]service.VolumeSpec{spec}))
	targetD := newTrackingSBSClient(service.NewInMemorySBSClient([]service.VolumeSpec{spec}))
	targetE := newTrackingSBSClient(service.NewInMemorySBSClient([]service.VolumeSpec{spec}))
	targetF := newTrackingSBSClient(service.NewInMemorySBSClient([]service.VolumeSpec{spec}))
	replicaClients := map[string]service.SBSClient{
		"rep-a": sourceA,
		"rep-b": sourceB,
		"rep-c": sourceC,
		"rep-d": targetD,
		"rep-e": targetE,
		"rep-f": targetF,
	}

	sourceReplicas, err := replication.OpenReplicaSessions(context.Background(), map[string]service.SBSClient{
		"rep-a": sourceA,
		"rep-b": sourceB,
		"rep-c": sourceC,
	}, replication.OpenReplicaSessionsRequest{
		VolumeID:      "00a1b2c3",
		GatewayID:     "gw-a",
		HostID:        "host-a",
		AttachmentID:  "seed-source",
		Generation:    1,
		SessionPrefix: "seed-source",
	})
	if err != nil {
		t.Fatalf("OpenReplicaSessions seed: %v", err)
	}
	for _, replica := range sourceReplicas {
		if _, err := replica.Client.Write(context.Background(), &service.WriteRequest{
			VolumeID:     replica.VolumeID,
			VolumeHandle: replica.VolumeHandle,
			OffsetBytes:  0,
			LengthBytes:  8,
			Data:         []byte("ABCDEFGH"),
			Context: service.SBSRequestContext{
				RequestID:      "seed-source-data",
				GatewayID:      replica.GatewayID,
				HostID:         replica.HostID,
				SessionID:      replica.SessionID,
				AttachmentID:   replica.AttachmentID,
				Generation:     replica.Generation,
				IdempotencyKey: "seed-source-data",
			},
		}); err != nil {
			t.Fatalf("seed source write: %v", err)
		}
	}
	for _, replica := range sourceReplicas {
		_, _ = replica.Client.CloseVolume(context.Background(), &service.CloseVolumeRequest{
			VolumeID:     replica.VolumeID,
			VolumeHandle: replica.VolumeHandle,
			Context: service.SBSRequestContext{
				RequestID:    "seed-source-close",
				GatewayID:    replica.GatewayID,
				HostID:       replica.HostID,
				SessionID:    replica.SessionID,
				AttachmentID: replica.AttachmentID,
				Generation:   replica.Generation,
			},
		})
	}

	svc := NewService(store)
	svc.now = func() time.Time { return time.Unix(1000, 0) }

	if _, err := svc.EnqueueRebalance(context.Background(), "00a1b2c3", 1, "rs-2"); err != nil {
		t.Fatalf("EnqueueRebalance: %v", err)
	}
	parentID := transitionMutationOperationID(store.transitions["pl-1"])
	rangeID := transitionPageRangeBatchMutationOperationID(store.transitions["pl-1"], 0, 1)
	store.mutationOps[parentID] = metadata.MutationOperationRecord{
		OperationID:       parentID,
		VolumeID:          "00a1b2c3",
		Kind:              "transition",
		State:             metadata.MutationOperationFailed,
		IdempotencyKey:    "pl-1",
		AffectedExtentIDs: []uint64{1},
		AffectedPageNos:   []uint64{0, 1},
		CompletedPageNos:  []uint64{1},
		LastUpdatedAtUnix: 1000,
	}
	store.mutationOps[rangeID] = metadata.MutationOperationRecord{
		OperationID:       rangeID,
		VolumeID:          "00a1b2c3",
		Kind:              "transition_batch",
		State:             metadata.MutationOperationFailed,
		IdempotencyKey:    parentID,
		AffectedExtentIDs: []uint64{1},
		AffectedPageNos:   []uint64{0, 1},
		CompletedPageNos:  []uint64{1},
		LastUpdatedAtUnix: 1001,
	}

	if _, err := svc.ApplyTransition(context.Background(), "00a1b2c3", "pl-1", replicaClients, "gw-a", "host-a"); err != nil {
		t.Fatalf("ApplyTransition retry: %v", err)
	}

	rangeBatch := store.mutationOps[rangeID]
	if rangeBatch.State != metadata.MutationOperationCommitted {
		t.Fatalf("range batch state=%q want=%q", rangeBatch.State, metadata.MutationOperationCommitted)
	}
	if len(rangeBatch.CompletedPageNos) != 2 || rangeBatch.CompletedPageNos[0] != 0 || rangeBatch.CompletedPageNos[1] != 1 {
		t.Fatalf("range batch completed pages=%v want=[0 1]", rangeBatch.CompletedPageNos)
	}
	if _, ok := store.mutationOps[transitionPageBatchMutationOperationID(store.transitions["pl-1"], 0)]; ok {
		t.Fatalf("single-page batch should not be recreated when an existing range batch window can be reused")
	}
}

func TestBuildTransitionPageBatchesPrefersRetryableWindowOverHistoricalWindow(t *testing.T) {
	mapping := metadata.ExtentMappingRecord{
		VolumeID:      "00a1b2c3",
		ExtentID:      1,
		LogicalOffset: 0,
		LengthBytes:   4,
		PlacementRef:  "pl-1",
	}
	segments := []transitionCopySegment{
		{OffsetBytes: 0, LengthBytes: 2},
		{OffsetBytes: 2, LengthBytes: 2},
	}

	batches := buildTransitionPageBatches(
		mapping,
		segments,
		2,
		1,
		[][]uint64{{0, 1}},
		[][]uint64{{0}, {1}},
	)

	if len(batches) != 1 {
		t.Fatalf("batches=%d want=1", len(batches))
	}
	if len(batches[0].PageNos) != 2 || batches[0].PageNos[0] != 0 || batches[0].PageNos[1] != 1 {
		t.Fatalf("page window=%v want=[0 1]", batches[0].PageNos)
	}
	if len(batches[0].ActivePages) != 2 || batches[0].ActivePages[0] != 0 || batches[0].ActivePages[1] != 1 {
		t.Fatalf("active pages=%v want=[0 1]", batches[0].ActivePages)
	}
}

func TestApplyTransitionCoalescesContiguousPagesIntoTransitionBatch(t *testing.T) {
	store := newFakeStore()
	store.mappings = []metadata.ExtentMappingRecord{
		{VolumeID: "00a1b2c3", ExtentID: 1, LogicalOffset: 0, LengthBytes: 16, ChunkID: 0, PlacementRef: "pl-1", Revision: 11},
	}
	store.replicaSets = append(store.replicaSets, metadata.ReplicaSetState{
		ReplicaSetID:     "rs-2",
		VolumeID:         "00a1b2c3",
		PlacementRef:     "pl-2",
		Epoch:            5,
		PrimaryReplicaID: "rep-d",
		WriteQuorum:      2,
		ReadQuorum:       1,
		Replicas: []metadata.ReplicaDescriptor{
			{NodeID: "node-d", ReplicaID: "rep-d", Role: metadata.ReplicaRolePrimary},
			{NodeID: "node-e", ReplicaID: "rep-e", Role: metadata.ReplicaRoleSecondary},
			{NodeID: "node-f", ReplicaID: "rep-f", Role: metadata.ReplicaRoleSecondary},
		},
	})
	store.nodes["node-d"] = metadata.NodeMembershipRecord{NodeID: "node-d", LifecycleState: metadata.NodeLifecycleActive, HealthState: metadata.NodeHealthHealthy}
	store.nodes["node-e"] = metadata.NodeMembershipRecord{NodeID: "node-e", LifecycleState: metadata.NodeLifecycleActive, HealthState: metadata.NodeHealthHealthy}
	store.nodes["node-f"] = metadata.NodeMembershipRecord{NodeID: "node-f", LifecycleState: metadata.NodeLifecycleActive, HealthState: metadata.NodeHealthHealthy}
	store.allocationPages = []metadata.AllocationPageRecord{
		{
			VolumeID:       "00a1b2c3",
			PageNo:         0,
			PageBytes:      8,
			ChunkSizeBytes: 4,
			Extents: []metadata.AllocationExtentRecord{
				{LogicalChunkStart: 0, ChunkCount: 2, Kind: metadata.AllocationKindData, PhysicalChunkStart: 500},
			},
		},
		{
			VolumeID:       "00a1b2c3",
			PageNo:         1,
			PageBytes:      8,
			ChunkSizeBytes: 4,
			Extents: []metadata.AllocationExtentRecord{
				{LogicalChunkStart: 2, ChunkCount: 2, Kind: metadata.AllocationKindData, PhysicalChunkStart: 600},
			},
		},
	}

	spec := service.NormalizeVolumeSpec(service.VolumeSpec{
		ID:              service.HexVolumeID(0x00a1b2c3),
		Name:            "vol-a",
		Prefix:          "vol-a-00a1b2c3",
		SizeBytes:       16,
		BlockSize:       1,
		ChunkSizeBytes:  4,
		ExtentPageBytes: 8,
	})

	sourceA := newTrackingSBSClient(service.NewInMemorySBSClient([]service.VolumeSpec{spec}))
	sourceB := newTrackingSBSClient(service.NewInMemorySBSClient([]service.VolumeSpec{spec}))
	sourceC := newTrackingSBSClient(service.NewInMemorySBSClient([]service.VolumeSpec{spec}))
	targetD := newTrackingSBSClient(service.NewInMemorySBSClient([]service.VolumeSpec{spec}))
	targetE := newTrackingSBSClient(service.NewInMemorySBSClient([]service.VolumeSpec{spec}))
	targetF := newTrackingSBSClient(service.NewInMemorySBSClient([]service.VolumeSpec{spec}))
	replicaClients := map[string]service.SBSClient{
		"rep-a": sourceA,
		"rep-b": sourceB,
		"rep-c": sourceC,
		"rep-d": targetD,
		"rep-e": targetE,
		"rep-f": targetF,
	}

	sourceReplicas, err := replication.OpenReplicaSessions(context.Background(), map[string]service.SBSClient{
		"rep-a": sourceA,
		"rep-b": sourceB,
		"rep-c": sourceC,
	}, replication.OpenReplicaSessionsRequest{
		VolumeID:      "00a1b2c3",
		GatewayID:     "gw-a",
		HostID:        "host-a",
		AttachmentID:  "seed-source",
		Generation:    1,
		SessionPrefix: "seed-source",
	})
	if err != nil {
		t.Fatalf("OpenReplicaSessions seed: %v", err)
	}
	for _, replica := range sourceReplicas {
		if _, err := replica.Client.Write(context.Background(), &service.WriteRequest{
			VolumeID:     replica.VolumeID,
			VolumeHandle: replica.VolumeHandle,
			OffsetBytes:  0,
			LengthBytes:  16,
			Data:         []byte("ABCDEFGHIJKLMNOP"),
			Context: service.SBSRequestContext{
				RequestID:      "seed-source-data",
				GatewayID:      replica.GatewayID,
				HostID:         replica.HostID,
				SessionID:      replica.SessionID,
				AttachmentID:   replica.AttachmentID,
				Generation:     replica.Generation,
				IdempotencyKey: "seed-source-data",
			},
		}); err != nil {
			t.Fatalf("seed source write: %v", err)
		}
	}
	for _, replica := range sourceReplicas {
		_, _ = replica.Client.CloseVolume(context.Background(), &service.CloseVolumeRequest{
			VolumeID:     replica.VolumeID,
			VolumeHandle: replica.VolumeHandle,
			Context: service.SBSRequestContext{
				RequestID:    "seed-source-close",
				GatewayID:    replica.GatewayID,
				HostID:       replica.HostID,
				SessionID:    replica.SessionID,
				AttachmentID: replica.AttachmentID,
				Generation:   replica.Generation,
			},
		})
	}

	svc := NewService(store)
	svc.now = func() time.Time { return time.Unix(1000, 0) }

	if _, err := svc.EnqueueRebalance(context.Background(), "00a1b2c3", 1, "rs-2"); err != nil {
		t.Fatalf("EnqueueRebalance: %v", err)
	}
	if _, err := svc.ApplyTransition(context.Background(), "00a1b2c3", "pl-1", replicaClients, "gw-a", "host-a"); err != nil {
		t.Fatalf("ApplyTransition: %v", err)
	}

	rangeBatch := store.mutationOps[transitionPageRangeBatchMutationOperationID(store.transitions["pl-1"], 0, 1)]
	if rangeBatch.Kind != "transition_batch" || rangeBatch.State != metadata.MutationOperationCommitted {
		t.Fatalf("range batch=%+v", rangeBatch)
	}
	if len(rangeBatch.AffectedPageNos) != 2 || rangeBatch.AffectedPageNos[0] != 0 || rangeBatch.AffectedPageNos[1] != 1 {
		t.Fatalf("range batch affected pages=%v want=[0 1]", rangeBatch.AffectedPageNos)
	}
	if len(rangeBatch.CompletedPageNos) != 2 || rangeBatch.CompletedPageNos[0] != 0 || rangeBatch.CompletedPageNos[1] != 1 {
		t.Fatalf("range batch completed pages=%v want=[0 1]", rangeBatch.CompletedPageNos)
	}
}

func TestApplyTransitionSplitsContiguousPagesIntoSizedTransitionBatches(t *testing.T) {
	store := newFakeStore()
	store.mappings = []metadata.ExtentMappingRecord{
		{VolumeID: "00a1b2c3", ExtentID: 1, LogicalOffset: 0, LengthBytes: 16, ChunkID: 0, PlacementRef: "pl-1", Revision: 11},
	}
	store.replicaSets = append(store.replicaSets, metadata.ReplicaSetState{
		ReplicaSetID:     "rs-2",
		VolumeID:         "00a1b2c3",
		PlacementRef:     "pl-2",
		Epoch:            5,
		PrimaryReplicaID: "rep-d",
		WriteQuorum:      2,
		ReadQuorum:       1,
		Replicas: []metadata.ReplicaDescriptor{
			{NodeID: "node-d", ReplicaID: "rep-d", Role: metadata.ReplicaRolePrimary},
			{NodeID: "node-e", ReplicaID: "rep-e", Role: metadata.ReplicaRoleSecondary},
			{NodeID: "node-f", ReplicaID: "rep-f", Role: metadata.ReplicaRoleSecondary},
		},
	})
	store.nodes["node-d"] = metadata.NodeMembershipRecord{NodeID: "node-d", LifecycleState: metadata.NodeLifecycleActive, HealthState: metadata.NodeHealthHealthy}
	store.nodes["node-e"] = metadata.NodeMembershipRecord{NodeID: "node-e", LifecycleState: metadata.NodeLifecycleActive, HealthState: metadata.NodeHealthHealthy}
	store.nodes["node-f"] = metadata.NodeMembershipRecord{NodeID: "node-f", LifecycleState: metadata.NodeLifecycleActive, HealthState: metadata.NodeHealthHealthy}
	store.allocationPages = []metadata.AllocationPageRecord{
		{
			VolumeID:       "00a1b2c3",
			PageNo:         0,
			PageBytes:      8,
			ChunkSizeBytes: 4,
			Extents: []metadata.AllocationExtentRecord{
				{LogicalChunkStart: 0, ChunkCount: 2, Kind: metadata.AllocationKindData, PhysicalChunkStart: 500},
			},
		},
		{
			VolumeID:       "00a1b2c3",
			PageNo:         1,
			PageBytes:      8,
			ChunkSizeBytes: 4,
			Extents: []metadata.AllocationExtentRecord{
				{LogicalChunkStart: 2, ChunkCount: 2, Kind: metadata.AllocationKindData, PhysicalChunkStart: 600},
			},
		},
	}

	spec := service.NormalizeVolumeSpec(service.VolumeSpec{
		ID:              service.HexVolumeID(0x00a1b2c3),
		Name:            "vol-a",
		Prefix:          "vol-a-00a1b2c3",
		SizeBytes:       16,
		BlockSize:       1,
		ChunkSizeBytes:  4,
		ExtentPageBytes: 8,
	})

	sourceA := newTrackingSBSClient(service.NewInMemorySBSClient([]service.VolumeSpec{spec}))
	sourceB := newTrackingSBSClient(service.NewInMemorySBSClient([]service.VolumeSpec{spec}))
	sourceC := newTrackingSBSClient(service.NewInMemorySBSClient([]service.VolumeSpec{spec}))
	targetD := newTrackingSBSClient(service.NewInMemorySBSClient([]service.VolumeSpec{spec}))
	targetE := newTrackingSBSClient(service.NewInMemorySBSClient([]service.VolumeSpec{spec}))
	targetF := newTrackingSBSClient(service.NewInMemorySBSClient([]service.VolumeSpec{spec}))
	replicaClients := map[string]service.SBSClient{
		"rep-a": sourceA,
		"rep-b": sourceB,
		"rep-c": sourceC,
		"rep-d": targetD,
		"rep-e": targetE,
		"rep-f": targetF,
	}

	sourceReplicas, err := replication.OpenReplicaSessions(context.Background(), map[string]service.SBSClient{
		"rep-a": sourceA,
		"rep-b": sourceB,
		"rep-c": sourceC,
	}, replication.OpenReplicaSessionsRequest{
		VolumeID:      "00a1b2c3",
		GatewayID:     "gw-a",
		HostID:        "host-a",
		AttachmentID:  "seed-source",
		Generation:    1,
		SessionPrefix: "seed-source",
	})
	if err != nil {
		t.Fatalf("OpenReplicaSessions seed: %v", err)
	}
	for _, replica := range sourceReplicas {
		if _, err := replica.Client.Write(context.Background(), &service.WriteRequest{
			VolumeID:     replica.VolumeID,
			VolumeHandle: replica.VolumeHandle,
			OffsetBytes:  0,
			LengthBytes:  16,
			Data:         []byte("ABCDEFGHIJKLMNOP"),
			Context: service.SBSRequestContext{
				RequestID:      "seed-source-data",
				GatewayID:      replica.GatewayID,
				HostID:         replica.HostID,
				SessionID:      replica.SessionID,
				AttachmentID:   replica.AttachmentID,
				Generation:     replica.Generation,
				IdempotencyKey: "seed-source-data",
			},
		}); err != nil {
			t.Fatalf("seed source write: %v", err)
		}
	}
	for _, replica := range sourceReplicas {
		_, _ = replica.Client.CloseVolume(context.Background(), &service.CloseVolumeRequest{
			VolumeID:     replica.VolumeID,
			VolumeHandle: replica.VolumeHandle,
			Context: service.SBSRequestContext{
				RequestID:    "seed-source-close",
				GatewayID:    replica.GatewayID,
				HostID:       replica.HostID,
				SessionID:    replica.SessionID,
				AttachmentID: replica.AttachmentID,
				Generation:   replica.Generation,
			},
		})
	}

	svc := NewService(store)
	svc.now = func() time.Time { return time.Unix(1000, 0) }
	svc.SetTransitionBatchMaxPages(1)

	if _, err := svc.EnqueueRebalance(context.Background(), "00a1b2c3", 1, "rs-2"); err != nil {
		t.Fatalf("EnqueueRebalance: %v", err)
	}
	if _, err := svc.ApplyTransition(context.Background(), "00a1b2c3", "pl-1", replicaClients, "gw-a", "host-a"); err != nil {
		t.Fatalf("ApplyTransition: %v", err)
	}

	page0 := store.mutationOps[transitionPageBatchMutationOperationID(store.transitions["pl-1"], 0)]
	page1 := store.mutationOps[transitionPageBatchMutationOperationID(store.transitions["pl-1"], 1)]
	if page0.State != metadata.MutationOperationCommitted || page1.State != metadata.MutationOperationCommitted {
		t.Fatalf("sized page batches=%+v %+v", page0, page1)
	}
	if _, ok := store.mutationOps[transitionPageRangeBatchMutationOperationID(store.transitions["pl-1"], 0, 1)]; ok {
		t.Fatalf("range batch should not be created when max pages per batch is 1")
	}
}
