package metadata

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/nosway/namrbd/internal/structuredlog"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"
)

type fakeKV struct {
	values map[string][]byte
}

type stalePlacementTransitionListKV struct {
	*fakeKV
	staleKey string
}

func (f *stalePlacementTransitionListKV) List(_ context.Context, prefix, _ string, _ int) ([]string, string, error) {
	if f.staleKey != "" && len(f.staleKey) >= len(prefix) && f.staleKey[:len(prefix)] == prefix {
		return []string{f.staleKey}, "", nil
	}
	return nil, "", nil
}

func newFakeKV() *fakeKV {
	return &fakeKV{values: make(map[string][]byte)}
}

func TestListPlacementTransitionsSkipsConcurrentlyDeletedRecord(t *testing.T) {
	volumeID := "00a1b2c3"
	kv := &stalePlacementTransitionListKV{
		fakeKV:   newFakeKV(),
		staleKey: placementTransitionKey("sbs/cluster", volumeID, "pl-deleted"),
	}
	repo := NewRepository(kv, "sbs/cluster")

	got, err := repo.ListPlacementTransitions(context.Background(), volumeID)
	if err != nil {
		t.Fatalf("ListPlacementTransitions: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("ListPlacementTransitions=%+v want empty", got)
	}
}

func (f *fakeKV) Get(_ context.Context, key string) ([]byte, bool, error) {
	value, ok := f.values[key]
	if !ok {
		return nil, false, nil
	}
	out := append([]byte(nil), value...)
	return out, true, nil
}

func (f *fakeKV) Set(_ context.Context, key string, value []byte) error {
	f.values[key] = append([]byte(nil), value...)
	return nil
}

func (f *fakeKV) Delete(_ context.Context, key string) error {
	delete(f.values, key)
	return nil
}

func (f *fakeKV) List(_ context.Context, prefix, cursor string, limit int) ([]string, string, error) {
	keys := make([]string, 0)
	for key := range f.values {
		if len(prefix) > 0 && len(key) >= len(prefix) && key[:len(prefix)] == prefix {
			keys = append(keys, key)
		}
	}
	slices.Sort(keys)
	start := 0
	if cursor != "" {
		for i, key := range keys {
			if key > cursor {
				start = i
				break
			}
			start = len(keys)
		}
	}
	if limit <= 0 || start+limit >= len(keys) {
		return keys[start:], "", nil
	}
	out := keys[start : start+limit]
	return out, out[len(out)-1], nil
}

func TestMembershipProjectionCASPagingAndTombstone(t *testing.T) {
	ctx := context.Background()
	repo := NewRepository(newFakeTransactionalKV(), "sbs/cluster")
	repo.now = func() time.Time { return time.Unix(100, 0) }

	created, status, err := repo.CompareAndSetNodeMembership(ctx, NodeMembershipRecord{
		ClusterID:      "cluster-a",
		SBSClusterID:   "sbs-a",
		NodeID:         "node-a",
		LifecycleState: NodeLifecycleActive,
		HealthState:    NodeHealthHealthy,
		UpdatedBy:      "operator-a",
		UpdateReason:   "join",
	}, 0)
	if err != nil {
		t.Fatalf("CompareAndSetNodeMembership(create): %v", err)
	}
	if created.Generation != 1 || created.MembershipRevision != 1 {
		t.Fatalf("created generation/revision=%d/%d want 1/1", created.Generation, created.MembershipRevision)
	}
	if status.MembershipRevision != 1 || status.MembershipProjectionRevision != 1 || status.Stale {
		t.Fatalf("create projection status=%+v", status)
	}
	if _, _, err := repo.CompareAndSetNodeMembership(ctx, created, 0); !errors.Is(err, ErrCASConflict) {
		t.Fatalf("stale create error=%v want ErrCASConflict", err)
	}

	created.Zone = "zone-b"
	updated, status, err := repo.CompareAndSetNodeMembership(ctx, created, created.Generation)
	if err != nil {
		t.Fatalf("CompareAndSetNodeMembership(update): %v", err)
	}
	if updated.Generation != 2 || updated.MembershipRevision != 2 || status.MembershipProjectionRevision != 2 {
		t.Fatalf("updated=%+v status=%+v", updated, status)
	}

	for _, nodeID := range []string{"node-b", "node-c"} {
		if err := repo.PutNodeMembership(ctx, NodeMembershipRecord{
			NodeID: nodeID, LifecycleState: NodeLifecycleActive, HealthState: NodeHealthHealthy,
		}); err != nil {
			t.Fatalf("PutNodeMembership(%s): %v", nodeID, err)
		}
	}
	page, err := repo.ListMembershipProjectionPage(ctx, "", 2, false)
	if err != nil {
		t.Fatalf("ListMembershipProjectionPage(first): %v", err)
	}
	if len(page.Records) != 2 || page.NextCursor == "" {
		t.Fatalf("first page=%+v", page)
	}
	page2, err := repo.ListMembershipProjectionPage(ctx, page.NextCursor, 2, false)
	if err != nil {
		t.Fatalf("ListMembershipProjectionPage(second): %v", err)
	}
	if len(page2.Records) != 1 || page2.NextCursor != "" {
		t.Fatalf("second page=%+v", page2)
	}

	if err := repo.DeleteNodeMembership(ctx, "node-a"); err != nil {
		t.Fatalf("DeleteNodeMembership: %v", err)
	}
	visible, err := repo.ListNodeMemberships(ctx)
	if err != nil {
		t.Fatalf("ListNodeMemberships: %v", err)
	}
	if len(visible) != 2 {
		t.Fatalf("visible nodes=%+v want two non-tombstones", visible)
	}
	all, err := repo.ListMembershipProjectionPage(ctx, "", 16, true)
	if err != nil {
		t.Fatalf("ListMembershipProjectionPage(tombstones): %v", err)
	}
	if len(all.Records) != 3 || !all.Records[0].Tombstone || all.Records[0].MembershipRevision != 5 {
		t.Fatalf("all records=%+v", all.Records)
	}
}

func TestMembershipMutationRetriesTransactionConflictButNotGenerationConflict(t *testing.T) {
	ResetTiKVPressureForTest()
	defer ResetTiKVPressureForTest()
	ctx := context.Background()
	kv := &conflictInjectingMembershipKV{
		fakeTransactionalKV: newFakeTransactionalKV(),
		conflictsRemaining:  2,
	}
	repo := NewRepository(kv, "sbs/cluster")
	repo.now = func() time.Time { return time.Unix(100, 0) }

	created, status, err := repo.CompareAndSetNodeMembership(ctx, NodeMembershipRecord{
		NodeID: "node-a", LifecycleState: NodeLifecycleActive, HealthState: NodeHealthHealthy,
	}, 0)
	if err != nil {
		t.Fatalf("CompareAndSetNodeMembership: %v", err)
	}
	if created.Generation != 1 || status.MembershipProjectionRevision != 1 {
		t.Fatalf("created=%+v status=%+v", created, status)
	}
	if kv.runTxCalls != 3 {
		t.Fatalf("transaction attempts=%d want 3", kv.runTxCalls)
	}
	if retries := TiKVPressureSnapshotNow().TxnRetryCount; retries != 2 {
		t.Fatalf("transaction retries=%d want 2", retries)
	}

	before := kv.runTxCalls
	if _, _, err := repo.CompareAndSetNodeMembership(ctx, created, 0); !errors.Is(err, ErrCASConflict) {
		t.Fatalf("stale generation error=%v want ErrCASConflict", err)
	}
	if kv.runTxCalls != before+1 {
		t.Fatalf("stale generation transaction attempts=%d want 1", kv.runTxCalls-before)
	}
	if retries := TiKVPressureSnapshotNow().TxnRetryCount; retries != 2 {
		t.Fatalf("stale generation changed retry count to %d", retries)
	}
}

func TestListNodeMembershipsBatchReadsProjectionDuringHealthChurn(t *testing.T) {
	ctx := context.Background()
	const root = "sbs/cluster"
	kv := &churningMembershipProjectionKV{
		fakeTransactionalKV: newFakeTransactionalKV(),
		root:                root,
	}
	repo := NewRepository(kv, root)
	for _, nodeID := range []string{"node-a", "node-b"} {
		if err := repo.PutNodeMembership(ctx, NodeMembershipRecord{
			NodeID: nodeID, LifecycleState: NodeLifecycleActive, HealthState: NodeHealthHealthy,
		}); err != nil {
			t.Fatalf("PutNodeMembership(%s): %v", nodeID, err)
		}
	}

	nodes, err := repo.ListNodeMemberships(ctx)
	if err != nil {
		t.Fatalf("ListNodeMemberships: %v", err)
	}
	if len(nodes) != 2 {
		t.Fatalf("ListNodeMemberships nodes=%+v want two", nodes)
	}
	if kv.projectionPointGetCalls != 0 {
		t.Fatalf("projection point reads=%d want 0", kv.projectionPointGetCalls)
	}
	if kv.readSnapshotCalls != 1 {
		t.Fatalf("projection read snapshots=%d want 1", kv.readSnapshotCalls)
	}
	if kv.snapshotBatchGetCalls != 1 || kv.batchGetCalls != 0 {
		t.Fatalf("snapshot batch reads=%d standalone batch reads=%d want 1/0", kv.snapshotBatchGetCalls, kv.batchGetCalls)
	}
}

func TestListNodeMembershipsKeepsAllPagesInOneSnapshot(t *testing.T) {
	ctx := context.Background()
	const root = "sbs/cluster"
	kv := &churningMembershipProjectionKV{
		fakeTransactionalKV: newFakeTransactionalKV(),
		root:                root,
	}
	repo := NewRepository(kv, root)
	for i := 0; i < MembershipProjectionPageMaximum+1; i++ {
		if err := repo.PutNodeMembership(ctx, NodeMembershipRecord{
			NodeID: fmt.Sprintf("node-%04d", i), LifecycleState: NodeLifecycleActive, HealthState: NodeHealthHealthy,
		}); err != nil {
			t.Fatalf("PutNodeMembership(%d): %v", i, err)
		}
	}

	nodes, err := repo.ListNodeMemberships(ctx)
	if err != nil {
		t.Fatalf("ListNodeMemberships: %v", err)
	}
	if len(nodes) != MembershipProjectionPageMaximum+1 {
		t.Fatalf("ListNodeMemberships count=%d want %d", len(nodes), MembershipProjectionPageMaximum+1)
	}
	if kv.readSnapshotCalls != 1 || kv.snapshotBatchGetCalls != 2 {
		t.Fatalf("read snapshots=%d batch pages=%d want 1/2", kv.readSnapshotCalls, kv.snapshotBatchGetCalls)
	}
}

func TestMembershipProjectionStaleBlocksAndRebuilds(t *testing.T) {
	ctx := context.Background()
	kv := newFakeTransactionalKV()
	repo := NewRepository(kv, "sbs/cluster")
	now := time.Unix(200, 0)
	repo.now = func() time.Time { return now }
	if err := repo.PutNodeMembership(ctx, NodeMembershipRecord{
		NodeID: "node-a", LifecycleState: NodeLifecycleActive, HealthState: NodeHealthHealthy,
	}); err != nil {
		t.Fatalf("PutNodeMembership: %v", err)
	}

	err := kv.RunInTransaction(ctx, func(tx kvReadWriter) error {
		var state MembershipProjectionState
		if err := getJSONStore(ctx, tx, membershipProjectionStateKey(repo.root), &state); err != nil {
			return err
		}
		state.MembershipRevision++
		state.MembershipUpdatedAtUnixNano = now.Add(-6 * time.Second).UnixNano()
		return putJSONStore(ctx, tx, membershipProjectionStateKey(repo.root), state)
	})
	if err != nil {
		t.Fatalf("inject projection lag: %v", err)
	}
	repo.invalidateMembershipCache()
	status, err := repo.GetMembershipProjectionStatus(ctx)
	if err != nil {
		t.Fatalf("GetMembershipProjectionStatus: %v", err)
	}
	if !status.Stale || status.ProjectionHealth != "degraded" || status.ProjectionLagMS != 6000 {
		t.Fatalf("degraded status=%+v", status)
	}
	if _, err := repo.ListNodeMemberships(ctx); !errors.Is(err, ErrMembershipProjectionStale) {
		t.Fatalf("ListNodeMemberships error=%v want ErrMembershipProjectionStale", err)
	}

	now = now.Add(10 * time.Second)
	status, err = repo.GetMembershipProjectionStatus(ctx)
	if err != nil {
		t.Fatalf("GetMembershipProjectionStatus(blocked): %v", err)
	}
	if status.ProjectionHealth != "blocked" {
		t.Fatalf("blocked status=%+v", status)
	}
	status, err = repo.RebuildMembershipProjection(ctx)
	if err != nil {
		t.Fatalf("RebuildMembershipProjection: %v", err)
	}
	if status.Stale || status.MembershipRevision != status.MembershipProjectionRevision || status.ProjectionResyncCount != 1 {
		t.Fatalf("rebuilt status=%+v", status)
	}
	if _, err := repo.ListNodeMemberships(ctx); err != nil {
		t.Fatalf("ListNodeMemberships after rebuild: %v", err)
	}
}

type countingCompatibleAllocationStore struct {
	pages     []AllocationPageRecord
	getCalls  int
	listCalls int
}

func (s *countingCompatibleAllocationStore) GetCompatibleAllocationPage(_ context.Context, volumeID string, pageNo uint64, pageBytes, chunkSizeBytes uint32) (AllocationPageRecord, error) {
	s.getCalls++
	for _, page := range s.pages {
		if page.VolumeID == volumeID && page.PageNo == pageNo {
			return cloneAllocationPageRecord(page), nil
		}
	}
	return zeroAllocationPage(volumeID, pageNo, pageBytes, chunkSizeBytes), nil
}

func (s *countingCompatibleAllocationStore) ListCompatibleAllocationPages(_ context.Context, volumeID string, _, _ uint32) ([]AllocationPageRecord, error) {
	s.listCalls++
	out := make([]AllocationPageRecord, 0, len(s.pages))
	for _, page := range s.pages {
		if page.VolumeID == volumeID {
			out = append(out, cloneAllocationPageRecord(page))
		}
	}
	return out, nil
}

func cloneAllocationPageRecord(page AllocationPageRecord) AllocationPageRecord {
	page.Extents = slices.Clone(page.Extents)
	return page
}

type fakeTransactionalKV struct {
	mu            sync.Mutex
	values        map[string][]byte
	getCalls      map[string]int
	setCalls      map[string]int
	batchGetCalls int
	batchGetKeys  [][]string
	runTxCalls    int
}

type churningMembershipProjectionKV struct {
	*fakeTransactionalKV
	root                    string
	projectionPointGetCalls int
	readSnapshotCalls       int
	snapshotBatchGetCalls   int
}

type conflictInjectingMembershipKV struct {
	*fakeTransactionalKV
	conflictsRemaining int
}

type mapMembershipReadSnapshot struct {
	values        map[string][]byte
	batchGetCalls *int
}

func newFakeTransactionalKV() *fakeTransactionalKV {
	return &fakeTransactionalKV{
		values:   make(map[string][]byte),
		getCalls: make(map[string]int),
		setCalls: make(map[string]int),
	}
}

func (f *fakeTransactionalKV) Get(_ context.Context, key string) ([]byte, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.getCalls[key]++
	value, ok := f.values[key]
	if !ok {
		return nil, false, nil
	}
	return append([]byte(nil), value...), true, nil
}

func (f *churningMembershipProjectionKV) Get(ctx context.Context, key string) ([]byte, bool, error) {
	if strings.HasPrefix(key, membershipProjectionNodesPrefix(f.root)) {
		f.mu.Lock()
		f.projectionPointGetCalls++
		stateKey := membershipProjectionStateKey(f.root)
		var state MembershipProjectionState
		if err := json.Unmarshal(f.values[stateKey], &state); err != nil {
			f.mu.Unlock()
			return nil, false, err
		}
		state.MembershipRevision++
		state.MembershipProjectionRevision++
		raw, err := json.Marshal(state)
		if err != nil {
			f.mu.Unlock()
			return nil, false, err
		}
		f.values[stateKey] = raw
		f.mu.Unlock()
	}
	return f.fakeTransactionalKV.Get(ctx, key)
}

func (f *churningMembershipProjectionKV) BatchGet(_ context.Context, keys []string) (map[string][]byte, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.batchGetCalls++
	f.batchGetKeys = append(f.batchGetKeys, slices.Clone(keys))
	out := make(map[string][]byte, len(keys))
	for _, key := range keys {
		if value, ok := f.values[key]; ok {
			out[key] = append([]byte(nil), value...)
		}
	}
	return out, nil
}

func (f *churningMembershipProjectionKV) RunInReadSnapshot(ctx context.Context, fn func(snapshot kvReadSnapshot) error) error {
	f.mu.Lock()
	f.readSnapshotCalls++
	values := make(map[string][]byte, len(f.values))
	for key, value := range f.values {
		values[key] = append([]byte(nil), value...)
	}
	stateKey := membershipProjectionStateKey(f.root)
	var state MembershipProjectionState
	if err := json.Unmarshal(f.values[stateKey], &state); err != nil {
		f.mu.Unlock()
		return err
	}
	state.MembershipRevision++
	state.MembershipProjectionRevision++
	raw, err := json.Marshal(state)
	if err != nil {
		f.mu.Unlock()
		return err
	}
	f.values[stateKey] = raw
	f.mu.Unlock()
	return fn(&mapMembershipReadSnapshot{values: values, batchGetCalls: &f.snapshotBatchGetCalls})
}

func (s *mapMembershipReadSnapshot) Get(_ context.Context, key string) ([]byte, bool, error) {
	value, ok := s.values[key]
	if !ok {
		return nil, false, nil
	}
	return append([]byte(nil), value...), true, nil
}

func (s *mapMembershipReadSnapshot) BatchGet(_ context.Context, keys []string) (map[string][]byte, error) {
	*s.batchGetCalls++
	out := make(map[string][]byte, len(keys))
	for _, key := range keys {
		if value, ok := s.values[key]; ok {
			out[key] = append([]byte(nil), value...)
		}
	}
	return out, nil
}

func (s *mapMembershipReadSnapshot) List(ctx context.Context, prefix, cursor string, limit int) ([]string, string, error) {
	return (&fakeKV{values: s.values}).List(ctx, prefix, cursor, limit)
}

func (f *fakeTransactionalKV) Set(_ context.Context, key string, value []byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.setCalls[key]++
	f.values[key] = append([]byte(nil), value...)
	return nil
}

func (f *fakeTransactionalKV) Delete(_ context.Context, key string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.values, key)
	return nil
}

func (f *fakeTransactionalKV) List(_ context.Context, prefix, cursor string, limit int) ([]string, string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	keys := make([]string, 0)
	for key := range f.values {
		if len(prefix) > 0 && len(key) >= len(prefix) && key[:len(prefix)] == prefix {
			keys = append(keys, key)
		}
	}
	slices.Sort(keys)
	start := 0
	if cursor != "" {
		for i, key := range keys {
			if key > cursor {
				start = i
				break
			}
			start = len(keys)
		}
	}
	if limit <= 0 || start+limit >= len(keys) {
		return keys[start:], "", nil
	}
	out := keys[start : start+limit]
	return out, out[len(out)-1], nil
}

func (f *fakeTransactionalKV) RunInTransaction(ctx context.Context, fn func(tx kvReadWriter) error) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.runTxCalls++
	return fn(ctxLockedStore{
		values:        f.values,
		getCalls:      f.getCalls,
		setCalls:      f.setCalls,
		batchGetCalls: &f.batchGetCalls,
		batchGetKeys:  &f.batchGetKeys,
	})
}

func (f *conflictInjectingMembershipKV) RunInTransaction(ctx context.Context, fn func(tx kvReadWriter) error) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.runTxCalls++
	working := make(map[string][]byte, len(f.values))
	for key, value := range f.values {
		working[key] = append([]byte(nil), value...)
	}
	if err := fn(ctxLockedStore{
		values:        working,
		getCalls:      f.getCalls,
		setCalls:      f.setCalls,
		batchGetCalls: &f.batchGetCalls,
		batchGetKeys:  &f.batchGetKeys,
	}); err != nil {
		return err
	}
	if f.conflictsRemaining > 0 {
		f.conflictsRemaining--
		return ErrCASConflict
	}
	f.values = working
	return nil
}

func (f *fakeTransactionalKV) resetGetCalls() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.getCalls = make(map[string]int)
	f.batchGetCalls = 0
	f.batchGetKeys = nil
}

func (f *fakeTransactionalKV) getCallCount(key string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.getCalls[key]
}

func (f *fakeTransactionalKV) resetSetCalls() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.setCalls = make(map[string]int)
}

func (f *fakeTransactionalKV) setCallCount(key string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.setCalls[key]
}

func (f *fakeTransactionalKV) batchGetCallCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.batchGetCalls
}

func (f *fakeTransactionalKV) lastBatchGetKeys() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.batchGetKeys) == 0 {
		return nil
	}
	return append([]string(nil), f.batchGetKeys[len(f.batchGetKeys)-1]...)
}

type ctxLockedStore struct {
	values        map[string][]byte
	getCalls      map[string]int
	setCalls      map[string]int
	batchGetCalls *int
	batchGetKeys  *[][]string
}

func (s ctxLockedStore) Get(_ context.Context, key string) ([]byte, bool, error) {
	if s.getCalls != nil {
		s.getCalls[key]++
	}
	value, ok := s.values[key]
	if !ok {
		return nil, false, nil
	}
	return append([]byte(nil), value...), true, nil
}

func (s ctxLockedStore) Set(_ context.Context, key string, value []byte) error {
	if s.setCalls != nil {
		s.setCalls[key]++
	}
	s.values[key] = append([]byte(nil), value...)
	return nil
}

func (s ctxLockedStore) Delete(_ context.Context, key string) error {
	delete(s.values, key)
	return nil
}

func (s ctxLockedStore) BatchGet(_ context.Context, keys []string) (map[string][]byte, error) {
	if s.batchGetCalls != nil {
		*s.batchGetCalls++
	}
	if s.batchGetKeys != nil {
		*s.batchGetKeys = append(*s.batchGetKeys, append([]string(nil), keys...))
	}
	out := make(map[string][]byte, len(keys))
	for _, key := range keys {
		if s.getCalls != nil {
			s.getCalls[key]++
		}
		value, ok := s.values[key]
		if !ok {
			continue
		}
		out[key] = append([]byte(nil), value...)
	}
	return out, nil
}

type countingBatchReadWriter struct {
	values        map[string][]byte
	getCalls      int
	batchGetCalls int
}

func newCountingBatchReadWriter() *countingBatchReadWriter {
	return &countingBatchReadWriter{values: make(map[string][]byte)}
}

func (s *countingBatchReadWriter) Get(_ context.Context, key string) ([]byte, bool, error) {
	s.getCalls++
	value, ok := s.values[key]
	if !ok {
		return nil, false, nil
	}
	return append([]byte(nil), value...), true, nil
}

func (s *countingBatchReadWriter) BatchGet(_ context.Context, keys []string) (map[string][]byte, error) {
	s.batchGetCalls++
	out := make(map[string][]byte, len(keys))
	for _, key := range keys {
		if value, ok := s.values[key]; ok {
			out[key] = append([]byte(nil), value...)
		}
	}
	return out, nil
}

func (s *countingBatchReadWriter) Set(_ context.Context, key string, value []byte) error {
	s.values[key] = append([]byte(nil), value...)
	return nil
}

func (s *countingBatchReadWriter) Delete(_ context.Context, key string) error {
	delete(s.values, key)
	return nil
}

func TestRepositoryRoundTripCoreRecords(t *testing.T) {
	ctx := context.Background()
	repo := NewRepository(newFakeKV(), "phase-e-test")

	volume := VolumeState{
		VolumeID:          "00a1b2c3",
		Epoch:             3,
		Revision:          7,
		PlacementPolicyID: "extent-placement-v1",
		ProtectionPolicy:  "rf3-primary",
		Status:            VolumeStatusHealthy,
	}
	if err := repo.PutVolumeState(ctx, volume); err != nil {
		t.Fatalf("PutVolumeState: %v", err)
	}

	mapping1 := ExtentMappingRecord{
		VolumeID:      "00a1b2c3",
		ExtentID:      1,
		LogicalOffset: 0,
		LengthBytes:   4 << 20,
		ChunkID:       101,
		PlacementRef:  "pl-0001",
		Revision:      7,
	}
	mapping2 := ExtentMappingRecord{
		VolumeID:      "00a1b2c3",
		ExtentID:      2,
		LogicalOffset: 4 << 20,
		LengthBytes:   4 << 20,
		ChunkID:       102,
		PlacementRef:  "pl-0002",
		Revision:      7,
	}
	if err := repo.PutExtentMapping(ctx, mapping2); err != nil {
		t.Fatalf("PutExtentMapping(2): %v", err)
	}
	if err := repo.PutExtentMapping(ctx, mapping1); err != nil {
		t.Fatalf("PutExtentMapping(1): %v", err)
	}

	replicaSet := ReplicaSetState{
		ReplicaSetID:     "rs-0001",
		VolumeID:         "00a1b2c3",
		PlacementRef:     "pl-0001",
		Epoch:            3,
		PrimaryReplicaID: "rep-a",
		WriteQuorum:      2,
		ReadQuorum:       1,
		FailureDomains:   []string{"zone-a", "zone-b", "zone-c"},
		Replicas: []ReplicaDescriptor{
			{NodeID: "node-a", ReplicaID: "rep-a", Role: ReplicaRolePrimary, FailureDomain: "zone-a"},
			{NodeID: "node-b", ReplicaID: "rep-b", Role: ReplicaRoleSecondary, FailureDomain: "zone-b"},
			{NodeID: "node-c", ReplicaID: "rep-c", Role: ReplicaRoleSecondary, FailureDomain: "zone-c"},
		},
	}
	if err := repo.PutReplicaSet(ctx, replicaSet); err != nil {
		t.Fatalf("PutReplicaSet: %v", err)
	}

	idem := IdempotencyRecord{
		IdempotencyKey: "idem-1",
		VolumeID:       "00a1b2c3",
		AttachmentID:   "att-00a1b2c3-0001",
		Generation:     4,
		Epoch:          3,
		Revision:       7,
		Operation:      "write",
		ResultState:    IdempotencyCommitted,
	}
	if err := repo.PutIdempotencyRecord(ctx, idem); err != nil {
		t.Fatalf("PutIdempotencyRecord: %v", err)
	}

	zone := TopologyZoneRecord{
		ZoneID:        "zone-a",
		DisplayName:   "Zone A",
		Lifecycle:     TopologyZoneLifecycleActive,
		Labels:        map[string]string{"purpose": "test"},
		CreatedAtUnix: 1230,
		UpdatedAtUnix: 1231,
	}
	if err := repo.PutTopologyZone(ctx, zone); err != nil {
		t.Fatalf("PutTopologyZone: %v", err)
	}

	node := NodeMembershipRecord{
		NodeID:            "node-a",
		LifecycleState:    NodeLifecycleActive,
		HealthState:       NodeHealthHealthy,
		Zone:              "zone-a",
		Host:              "host-a",
		CapacityBytes:     1 << 30,
		UsedBytes:         128 << 20,
		LastHeartbeatUnix: 1234,
		Version:           "phase-e-dev",
		Capabilities:      []string{"rf3", "active-active"},
	}
	if err := repo.PutNodeMembership(ctx, node); err != nil {
		t.Fatalf("PutNodeMembership: %v", err)
	}

	transition := PlacementTransitionRecord{
		VolumeID:            "00a1b2c3",
		PlacementRef:        "pl-0001",
		State:               PlacementTransitionRunning,
		Reason:              "rebalance",
		CurrentReplicaSetID: "rs-0001",
		TargetReplicaSetID:  "rs-0002",
		StartedAtUnix:       1000,
		LastProgressAtUnix:  1001,
		Attempt:             1,
	}
	if err := repo.PutPlacementTransition(ctx, transition); err != nil {
		t.Fatalf("PutPlacementTransition: %v", err)
	}

	allocationPage2 := AllocationPageRecord{
		VolumeID:       "00a1b2c3",
		PageNo:         2,
		PageBytes:      4 << 20,
		ChunkSizeBytes: 64 << 10,
		Revision:       9,
		Extents: []AllocationExtentRecord{
			{
				LogicalChunkStart:  128,
				ChunkCount:         8,
				Kind:               AllocationKindData,
				PhysicalChunkStart: 1000,
				Generation:         3,
				Checksum:           "sha256:abc",
			},
		},
	}
	allocationPage1 := AllocationPageRecord{
		VolumeID:       "00a1b2c3",
		PageNo:         1,
		PageBytes:      4 << 20,
		ChunkSizeBytes: 64 << 10,
		Revision:       8,
		Extents: []AllocationExtentRecord{
			{
				LogicalChunkStart: 0,
				ChunkCount:        64,
				Kind:              AllocationKindZero,
			},
		},
	}
	if err := repo.PutAllocationPage(ctx, allocationPage2); err != nil {
		t.Fatalf("PutAllocationPage(2): %v", err)
	}
	if err := repo.PutAllocationPage(ctx, allocationPage1); err != nil {
		t.Fatalf("PutAllocationPage(1): %v", err)
	}

	operation2 := MutationOperationRecord{
		OperationID:        "op-0002",
		VolumeID:           "00a1b2c3",
		Kind:               "rebalance",
		State:              MutationOperationRunning,
		PlacementRevision:  7,
		AllocationRevision: 9,
		AffectedExtentIDs:  []uint64{2, 3},
		AffectedPageNos:    []uint64{1},
		CompletedPageNos:   []uint64{1},
		RetryPageWindows: []MutationPageWindowRecord{
			{ExtentID: 2, StartPageNo: 4, EndPageNo: 5, DataBytes: 16, DataChunks: 4},
		},
		RetiredPhysicalChunkIDs: []uint64{1000, 1001},
		LastUpdatedAtUnix:       2002,
	}
	operation1 := MutationOperationRecord{
		OperationID:        "op-0001",
		VolumeID:           "00a1b2c3",
		Kind:               "write",
		State:              MutationOperationPending,
		IdempotencyKey:     "idem-1",
		AllocationRevision: 8,
		LastUpdatedAtUnix:  2001,
	}
	if err := repo.PutMutationOperation(ctx, operation2); err != nil {
		t.Fatalf("PutMutationOperation(2): %v", err)
	}
	if err := repo.PutMutationOperation(ctx, operation1); err != nil {
		t.Fatalf("PutMutationOperation(1): %v", err)
	}

	healthDetail := NodeHealthDetailRecord{
		NodeID:                    "node-a",
		LastProbeUnix:             2222,
		ConsecutiveProbeFailures:  1,
		ConsecutiveProbeSuccesses: 3,
		HealthReason:              "http healthz timeout",
		HealthUpdatedBy:           HealthUpdatedByReconciler,
	}
	if err := repo.PutNodeHealthDetail(ctx, healthDetail); err != nil {
		t.Fatalf("PutNodeHealthDetail: %v", err)
	}

	gotVolume, err := repo.GetVolumeState(ctx, "00a1b2c3")
	if err != nil || gotVolume.Revision != 7 {
		t.Fatalf("GetVolumeState: got=%+v err=%v", gotVolume, err)
	}
	gotVolumes, err := repo.ListVolumeStates(ctx)
	if err != nil {
		t.Fatalf("ListVolumeStates: %v", err)
	}
	if len(gotVolumes) != 1 || gotVolumes[0].VolumeID != "00a1b2c3" {
		t.Fatalf("ListVolumeStates=%+v", gotVolumes)
	}
	gotMappings, err := repo.ListExtentMappings(ctx, "00a1b2c3")
	if err != nil {
		t.Fatalf("ListExtentMappings: %v", err)
	}
	if len(gotMappings) != 2 || gotMappings[0].ExtentID != 1 || gotMappings[1].ExtentID != 2 {
		t.Fatalf("ListExtentMappings order mismatch: %+v", gotMappings)
	}
	gotReplicaSet, err := repo.GetReplicaSet(ctx, "00a1b2c3", "rs-0001")
	if err != nil || gotReplicaSet.PrimaryReplicaID != "rep-a" {
		t.Fatalf("GetReplicaSet: got=%+v err=%v", gotReplicaSet, err)
	}
	gotIdem, err := repo.GetIdempotencyRecord(ctx, "00a1b2c3", "idem-1")
	if err != nil || gotIdem.ResultState != IdempotencyCommitted {
		t.Fatalf("GetIdempotencyRecord: got=%+v err=%v", gotIdem, err)
	}
	gotNode, err := repo.GetNodeMembership(ctx, "node-a")
	if err != nil || gotNode.HealthState != NodeHealthHealthy {
		t.Fatalf("GetNodeMembership: got=%+v err=%v", gotNode, err)
	}
	gotNodes, err := repo.ListNodeMemberships(ctx)
	if err != nil {
		t.Fatalf("ListNodeMemberships: %v", err)
	}
	if len(gotNodes) != 1 || gotNodes[0].NodeID != "node-a" {
		t.Fatalf("ListNodeMemberships=%+v", gotNodes)
	}
	gotZone, err := repo.GetTopologyZone(ctx, "zone-a")
	if err != nil || gotZone.DisplayName != "Zone A" {
		t.Fatalf("GetTopologyZone: got=%+v err=%v", gotZone, err)
	}
	gotZones, err := repo.ListTopologyZones(ctx)
	if err != nil {
		t.Fatalf("ListTopologyZones: %v", err)
	}
	if len(gotZones) != 1 || gotZones[0].ZoneID != "zone-a" {
		t.Fatalf("ListTopologyZones=%+v", gotZones)
	}
	gotTransition, err := repo.GetPlacementTransition(ctx, "00a1b2c3", "pl-0001")
	if err != nil || gotTransition.TargetReplicaSetID != "rs-0002" {
		t.Fatalf("GetPlacementTransition: got=%+v err=%v", gotTransition, err)
	}
	gotTransitions, err := repo.ListPlacementTransitions(ctx, "00a1b2c3")
	if err != nil {
		t.Fatalf("ListPlacementTransitions: %v", err)
	}
	if len(gotTransitions) != 1 || gotTransitions[0].PlacementRef != "pl-0001" {
		t.Fatalf("ListPlacementTransitions=%+v", gotTransitions)
	}
	gotAllocationPage, err := repo.GetAllocationPage(ctx, "00a1b2c3", 1)
	if err != nil || gotAllocationPage.Revision != 8 {
		t.Fatalf("GetAllocationPage: got=%+v err=%v", gotAllocationPage, err)
	}
	gotAllocationPages, err := repo.ListAllocationPages(ctx, "00a1b2c3")
	if err != nil {
		t.Fatalf("ListAllocationPages: %v", err)
	}
	if len(gotAllocationPages) != 2 || gotAllocationPages[0].PageNo != 1 || gotAllocationPages[1].PageNo != 2 {
		t.Fatalf("ListAllocationPages=%+v", gotAllocationPages)
	}
	gotOperation, err := repo.GetMutationOperation(ctx, "00a1b2c3", "op-0001")
	if err != nil || gotOperation.Kind != "write" {
		t.Fatalf("GetMutationOperation: got=%+v err=%v", gotOperation, err)
	}
	gotOperations, err := repo.ListMutationOperations(ctx, "00a1b2c3")
	if err != nil {
		t.Fatalf("ListMutationOperations: %v", err)
	}
	if len(gotOperations) != 2 || gotOperations[0].OperationID != "op-0001" || gotOperations[1].OperationID != "op-0002" {
		t.Fatalf("ListMutationOperations=%+v", gotOperations)
	}
	if len(gotOperations[1].RetiredPhysicalChunkIDs) != 2 || gotOperations[1].RetiredPhysicalChunkIDs[0] != 1000 || gotOperations[1].RetiredPhysicalChunkIDs[1] != 1001 {
		t.Fatalf("retired physical chunk ids=%v", gotOperations[1].RetiredPhysicalChunkIDs)
	}
	if len(gotOperations[1].AffectedExtentIDs) != 2 || gotOperations[1].AffectedExtentIDs[0] != 2 || gotOperations[1].AffectedExtentIDs[1] != 3 {
		t.Fatalf("affected extent ids=%v", gotOperations[1].AffectedExtentIDs)
	}
	if len(gotOperations[1].AffectedPageNos) != 1 || gotOperations[1].AffectedPageNos[0] != 1 {
		t.Fatalf("affected page nos=%v", gotOperations[1].AffectedPageNos)
	}
	if len(gotOperations[1].CompletedPageNos) != 1 || gotOperations[1].CompletedPageNos[0] != 1 {
		t.Fatalf("completed page nos=%v", gotOperations[1].CompletedPageNos)
	}
	if len(gotOperations[1].RetryPageWindows) != 1 || gotOperations[1].RetryPageWindows[0].ExtentID != 2 || gotOperations[1].RetryPageWindows[0].StartPageNo != 4 || gotOperations[1].RetryPageWindows[0].EndPageNo != 5 || gotOperations[1].RetryPageWindows[0].DataBytes != 16 || gotOperations[1].RetryPageWindows[0].DataChunks != 4 {
		t.Fatalf("retry page windows=%v", gotOperations[1].RetryPageWindows)
	}
	gotHealthDetail, err := repo.GetNodeHealthDetail(ctx, "node-a")
	if err != nil || gotHealthDetail.HealthUpdatedBy != HealthUpdatedByReconciler {
		t.Fatalf("GetNodeHealthDetail: got=%+v err=%v", gotHealthDetail, err)
	}
}

func TestRepositorySnapshotRecordsAndIdempotency(t *testing.T) {
	ctx := context.Background()
	repo := NewRepository(newFakeKV(), "phase-j-test")

	rec := SnapshotRecord{
		SnapshotID:               "snap-00a1b2c3-20260521T120000.000000000Z",
		SourceVolumeID:           "00a1b2c3",
		State:                    SnapshotStateAvailable,
		CreatedAtUnix:            100,
		UpdatedAtUnix:            100,
		CutVolumeRevision:        7,
		AllocationChunkSizeBytes: 65536,
		AllocationPageSizeBytes:  4194304,
		SourceSizeBytes:          1 << 30,
		IdempotencyKey:           "idem-1",
	}
	created, replay, err := repo.CreateSnapshotRecord(ctx, rec)
	if err != nil {
		t.Fatalf("CreateSnapshotRecord: %v", err)
	}
	if replay {
		t.Fatalf("first CreateSnapshotRecord replay=true")
	}
	if created.SnapshotRootID != rec.SnapshotID {
		t.Fatalf("snapshot_root_id=%q want snapshot_id", created.SnapshotRootID)
	}

	replayed, replay, err := repo.CreateSnapshotRecord(ctx, SnapshotRecord{
		SnapshotID:      "snap-00a1b2c3-20260521T120001.000000000Z",
		SourceVolumeID:  "00a1b2c3",
		State:           SnapshotStateAvailable,
		CreatedAtUnix:   101,
		UpdatedAtUnix:   101,
		IdempotencyKey:  "idem-1",
		SourceSizeBytes: 2 << 30,
	})
	if err != nil {
		t.Fatalf("CreateSnapshotRecord replay: %v", err)
	}
	if !replay {
		t.Fatalf("second CreateSnapshotRecord replay=false")
	}
	if replayed.SnapshotID != rec.SnapshotID || replayed.SourceSizeBytes != rec.SourceSizeBytes {
		t.Fatalf("replayed snapshot=%+v want original %+v", replayed, rec)
	}

	list, err := repo.ListSnapshotRecords(ctx, "00a1b2c3", false)
	if err != nil {
		t.Fatalf("ListSnapshotRecords: %v", err)
	}
	if len(list) != 1 || list[0].SnapshotID != rec.SnapshotID {
		t.Fatalf("snapshots=%+v", list)
	}

	deleted, err := repo.MarkSnapshotState(ctx, rec.SnapshotID, SnapshotStateDeleted, "")
	if err != nil {
		t.Fatalf("MarkSnapshotState: %v", err)
	}
	if deleted.State != SnapshotStateDeleted {
		t.Fatalf("state=%q want deleted", deleted.State)
	}
	list, err = repo.ListSnapshotRecords(ctx, "00a1b2c3", false)
	if err != nil {
		t.Fatalf("ListSnapshotRecords after delete: %v", err)
	}
	if len(list) != 0 {
		t.Fatalf("deleted snapshot should be hidden by default: %+v", list)
	}
	list, err = repo.ListSnapshotRecords(ctx, "00a1b2c3", true)
	if err != nil {
		t.Fatalf("ListSnapshotRecords include deleted: %v", err)
	}
	if len(list) != 1 || list[0].State != SnapshotStateDeleted {
		t.Fatalf("include deleted snapshots=%+v", list)
	}
}

func TestRepositoryCloneRecordsProtectSnapshotReferences(t *testing.T) {
	ctx := context.Background()
	repo := NewRepository(newFakeKV(), "phase-j-test")

	snapshotID := "snap-00a1b2c3-20260521T120000.000000000Z"
	if _, _, err := repo.CreateSnapshotRecord(ctx, SnapshotRecord{
		SnapshotID:               snapshotID,
		SourceVolumeID:           "00a1b2c3",
		SnapshotRootID:           snapshotID,
		State:                    SnapshotStateAvailable,
		CreatedAtUnix:            100,
		UpdatedAtUnix:            100,
		CutVolumeRevision:        7,
		AllocationChunkSizeBytes: 65536,
		AllocationPageSizeBytes:  4194304,
		SourceSizeBytes:          1 << 30,
	}); err != nil {
		t.Fatalf("CreateSnapshotRecord: %v", err)
	}

	created, replay, err := repo.CreateCloneRecord(ctx, CloneRecord{
		CloneID:          "clone-1",
		SourceSnapshotID: snapshotID,
		CreatedAtUnix:    101,
		UpdatedAtUnix:    101,
		IdempotencyKey:   "clone-idem-1",
	})
	if err != nil {
		t.Fatalf("CreateCloneRecord: %v", err)
	}
	if replay {
		t.Fatalf("first CreateCloneRecord replay=true")
	}
	if created.SourceVolumeID != "00a1b2c3" || created.CloneBaseRootID != snapshotID || created.SizeBytes != 1<<30 {
		t.Fatalf("clone inherited fields=%+v", created)
	}
	snapshot, err := repo.GetSnapshotRecord(ctx, snapshotID)
	if err != nil {
		t.Fatalf("GetSnapshotRecord: %v", err)
	}
	if snapshot.CloneReferenceCount != 1 {
		t.Fatalf("clone_reference_count=%d want=1", snapshot.CloneReferenceCount)
	}

	replayed, replay, err := repo.CreateCloneRecord(ctx, CloneRecord{
		CloneID:          "clone-2",
		SourceSnapshotID: snapshotID,
		CreatedAtUnix:    102,
		UpdatedAtUnix:    102,
		IdempotencyKey:   "clone-idem-1",
		SizeBytes:        2 << 30,
	})
	if err != nil {
		t.Fatalf("CreateCloneRecord replay: %v", err)
	}
	if !replay || replayed.CloneID != created.CloneID {
		t.Fatalf("replay=%v clone=%+v want original %+v", replay, replayed, created)
	}
	snapshot, err = repo.GetSnapshotRecord(ctx, snapshotID)
	if err != nil {
		t.Fatalf("GetSnapshotRecord after replay: %v", err)
	}
	if snapshot.CloneReferenceCount != 1 {
		t.Fatalf("clone_reference_count after replay=%d want=1", snapshot.CloneReferenceCount)
	}

	list, err := repo.ListCloneRecords(ctx, snapshotID, "", false)
	if err != nil {
		t.Fatalf("ListCloneRecords by snapshot: %v", err)
	}
	if len(list) != 1 || list[0].CloneID != created.CloneID {
		t.Fatalf("clones by snapshot=%+v", list)
	}
	list, err = repo.ListCloneRecords(ctx, "", "00a1b2c3", false)
	if err != nil {
		t.Fatalf("ListCloneRecords by volume: %v", err)
	}
	if len(list) != 1 || list[0].SourceSnapshotID != snapshotID {
		t.Fatalf("clones by volume=%+v", list)
	}

	deleted, err := repo.DeleteCloneRecord(ctx, created.CloneID)
	if err != nil {
		t.Fatalf("DeleteCloneRecord: %v", err)
	}
	if deleted.State != CloneStateDeleted {
		t.Fatalf("deleted clone state=%q want deleted", deleted.State)
	}
	snapshot, err = repo.GetSnapshotRecord(ctx, snapshotID)
	if err != nil {
		t.Fatalf("GetSnapshotRecord after delete: %v", err)
	}
	if snapshot.CloneReferenceCount != 0 {
		t.Fatalf("clone_reference_count after delete=%d want=0", snapshot.CloneReferenceCount)
	}
	list, err = repo.ListCloneRecords(ctx, snapshotID, "", false)
	if err != nil {
		t.Fatalf("ListCloneRecords after delete: %v", err)
	}
	if len(list) != 0 {
		t.Fatalf("deleted clone should be hidden by default: %+v", list)
	}
}

func TestRepositoryMarkCloneMaterializedReleasesSnapshotReference(t *testing.T) {
	ctx := context.Background()
	repo := NewRepository(newFakeKV(), "phase-j-test")

	snapshotID := "snap-00a1b2c3-20260521T120000.000000000Z"
	if _, _, err := repo.CreateSnapshotRecord(ctx, SnapshotRecord{
		SnapshotID:               snapshotID,
		SourceVolumeID:           "00a1b2c3",
		SnapshotRootID:           snapshotID,
		State:                    SnapshotStateAvailable,
		CreatedAtUnix:            100,
		UpdatedAtUnix:            100,
		CutVolumeRevision:        7,
		AllocationChunkSizeBytes: 65536,
		AllocationPageSizeBytes:  4194304,
		SourceSizeBytes:          1 << 30,
	}); err != nil {
		t.Fatalf("CreateSnapshotRecord: %v", err)
	}
	clone, _, err := repo.CreateCloneRecord(ctx, CloneRecord{
		CloneID:          "clone-1",
		SourceSnapshotID: snapshotID,
		CreatedAtUnix:    101,
		UpdatedAtUnix:    101,
	})
	if err != nil {
		t.Fatalf("CreateCloneRecord: %v", err)
	}
	snapshot, err := repo.GetSnapshotRecord(ctx, snapshotID)
	if err != nil {
		t.Fatalf("GetSnapshotRecord: %v", err)
	}
	if snapshot.CloneReferenceCount != 1 {
		t.Fatalf("clone_reference_count=%d want=1", snapshot.CloneReferenceCount)
	}

	materialized, err := repo.MarkCloneMaterialized(ctx, clone.CloneID, "00a1b2c4")
	if err != nil {
		t.Fatalf("MarkCloneMaterialized: %v", err)
	}
	if materialized.State != CloneStateMaterialized || materialized.MaterializedVolumeID != "00a1b2c4" {
		t.Fatalf("materialized clone=%+v", materialized)
	}
	snapshot, err = repo.GetSnapshotRecord(ctx, snapshotID)
	if err != nil {
		t.Fatalf("GetSnapshotRecord after materialize: %v", err)
	}
	if snapshot.CloneReferenceCount != 0 {
		t.Fatalf("clone_reference_count after materialize=%d want=0", snapshot.CloneReferenceCount)
	}
	replayed, err := repo.MarkCloneMaterialized(ctx, clone.CloneID, "00a1b2c4")
	if err != nil {
		t.Fatalf("MarkCloneMaterialized replay: %v", err)
	}
	if replayed.State != CloneStateMaterialized {
		t.Fatalf("replayed clone=%+v", replayed)
	}
	if _, err := repo.MarkCloneMaterialized(ctx, clone.CloneID, "00a1b2c5"); !errors.Is(err, ErrCASConflict) {
		t.Fatalf("MarkCloneMaterialized conflicting target err=%v want ErrCASConflict", err)
	}

	if _, err := repo.DeleteCloneRecord(ctx, clone.CloneID); err != nil {
		t.Fatalf("DeleteCloneRecord materialized clone: %v", err)
	}
	snapshot, err = repo.GetSnapshotRecord(ctx, snapshotID)
	if err != nil {
		t.Fatalf("GetSnapshotRecord after materialized delete: %v", err)
	}
	if snapshot.CloneReferenceCount != 0 {
		t.Fatalf("clone_reference_count after materialized delete=%d want=0", snapshot.CloneReferenceCount)
	}
}

func TestRepositoryCreateCloneRejectsUnavailableSnapshotAndTooSmallSize(t *testing.T) {
	ctx := context.Background()
	repo := NewRepository(newFakeKV(), "phase-j-test")

	failedSnapshotID := "snap-00a1b2c3-20260521T120000.000000000Z"
	if _, _, err := repo.CreateSnapshotRecord(ctx, SnapshotRecord{
		SnapshotID:               failedSnapshotID,
		SourceVolumeID:           "00a1b2c3",
		State:                    SnapshotStateFailed,
		CreatedAtUnix:            100,
		UpdatedAtUnix:            100,
		AllocationChunkSizeBytes: 65536,
		AllocationPageSizeBytes:  4194304,
		SourceSizeBytes:          1 << 30,
	}); err != nil {
		t.Fatalf("CreateSnapshotRecord failed snapshot: %v", err)
	}
	if _, _, err := repo.CreateCloneRecord(ctx, CloneRecord{
		CloneID:          "clone-failed",
		SourceSnapshotID: failedSnapshotID,
	}); err == nil {
		t.Fatalf("CreateCloneRecord should reject failed source snapshot")
	}

	availableSnapshotID := "snap-00a1b2c3-20260521T120001.000000000Z"
	if _, _, err := repo.CreateSnapshotRecord(ctx, SnapshotRecord{
		SnapshotID:               availableSnapshotID,
		SourceVolumeID:           "00a1b2c3",
		State:                    SnapshotStateAvailable,
		CreatedAtUnix:            101,
		UpdatedAtUnix:            101,
		AllocationChunkSizeBytes: 65536,
		AllocationPageSizeBytes:  4194304,
		SourceSizeBytes:          1 << 30,
	}); err != nil {
		t.Fatalf("CreateSnapshotRecord available snapshot: %v", err)
	}
	if _, _, err := repo.CreateCloneRecord(ctx, CloneRecord{
		CloneID:          "clone-too-small",
		SourceSnapshotID: availableSnapshotID,
		SizeBytes:        (1 << 30) - 1,
	}); err == nil {
		t.Fatalf("CreateCloneRecord should reject clone smaller than source snapshot")
	}
}

func TestRepositoryCloneDeltaAllocationPagesUpdateCloneSummaryCounters(t *testing.T) {
	ctx := context.Background()
	repo := NewRepository(newFakeKV(), "phase-j-test")

	snapshotID := "snap-00a1b2c3-20260521T120000.000000000Z"
	if _, _, err := repo.CreateSnapshotRecord(ctx, SnapshotRecord{
		SnapshotID:               snapshotID,
		SourceVolumeID:           "00a1b2c3",
		State:                    SnapshotStateAvailable,
		CreatedAtUnix:            100,
		UpdatedAtUnix:            100,
		AllocationChunkSizeBytes: 4096,
		AllocationPageSizeBytes:  4 * 4096,
		SourceSizeBytes:          8 * 4096,
	}); err != nil {
		t.Fatalf("CreateSnapshotRecord: %v", err)
	}
	clone, _, err := repo.CreateCloneRecord(ctx, CloneRecord{
		CloneID:          "clone-1",
		SourceSnapshotID: snapshotID,
		CreatedAtUnix:    101,
		UpdatedAtUnix:    101,
	})
	if err != nil {
		t.Fatalf("CreateCloneRecord: %v", err)
	}

	if err := repo.PutCloneDeltaAllocationPage(ctx, clone.CloneID, AllocationPageRecord{
		PageNo:         1,
		PageBytes:      4 * 4096,
		ChunkSizeBytes: 4096,
		Revision:       1,
		Extents: []AllocationExtentRecord{
			{LogicalChunkStart: 4, ChunkCount: 2, Kind: AllocationKindData, PhysicalChunkStart: 200},
			{LogicalChunkStart: 6, ChunkCount: 2, Kind: AllocationKindZero},
		},
	}); err != nil {
		t.Fatalf("PutCloneDeltaAllocationPage: %v", err)
	}
	got, err := repo.GetCloneDeltaAllocationPage(ctx, clone.CloneID, 1)
	if err != nil {
		t.Fatalf("GetCloneDeltaAllocationPage: %v", err)
	}
	if got.VolumeID != clone.CloneID || got.PageNo != 1 || got.Extents[0].PhysicalChunkStart != 200 {
		t.Fatalf("clone delta page=%+v", got)
	}
	clone, err = repo.GetCloneRecord(ctx, clone.CloneID)
	if err != nil {
		t.Fatalf("GetCloneRecord: %v", err)
	}
	if clone.DeltaPageCount != 1 || clone.DeltaObjectCount != 1 {
		t.Fatalf("clone delta counters=%d/%d want=1/1", clone.DeltaPageCount, clone.DeltaObjectCount)
	}
	list, err := repo.ListCloneDeltaAllocationPages(ctx, clone.CloneID)
	if err != nil {
		t.Fatalf("ListCloneDeltaAllocationPages: %v", err)
	}
	if len(list) != 1 || list[0].PageNo != 1 {
		t.Fatalf("clone delta pages=%+v", list)
	}

	if err := repo.PutCloneDeltaAllocationPages(ctx, clone.CloneID, []AllocationPageRecord{{
		PageNo:         1,
		PageBytes:      4 * 4096,
		ChunkSizeBytes: 4096,
		Revision:       2,
		Extents: []AllocationExtentRecord{
			{LogicalChunkStart: 4, ChunkCount: 4, Kind: AllocationKindData, PhysicalChunkStart: 300},
		},
	}, {
		PageNo:         0,
		PageBytes:      4 * 4096,
		ChunkSizeBytes: 4096,
		Revision:       2,
		Extents: []AllocationExtentRecord{
			{LogicalChunkStart: 0, ChunkCount: 4, Kind: AllocationKindData, PhysicalChunkStart: 400},
		},
	}}); err != nil {
		t.Fatalf("PutCloneDeltaAllocationPages: %v", err)
	}
	clone, err = repo.GetCloneRecord(ctx, clone.CloneID)
	if err != nil {
		t.Fatalf("GetCloneRecord after batch: %v", err)
	}
	if clone.DeltaPageCount != 2 || clone.DeltaObjectCount != 2 {
		t.Fatalf("clone delta counters after batch=%d/%d want=2/2", clone.DeltaPageCount, clone.DeltaObjectCount)
	}
}

func TestRepositoryCapturesSnapshotAllocationPages(t *testing.T) {
	ctx := context.Background()
	repo := NewRepository(newFakeKV(), "phase-j-test")
	snapshotID := "snap-00a1b2c3-20260521T120000.000000000Z"

	pages := []AllocationPageRecord{
		{
			VolumeID:       "00a1b2c3",
			PageNo:         1,
			PageBytes:      4096,
			ChunkSizeBytes: 1024,
			Revision:       7,
			Extents: []AllocationExtentRecord{
				{LogicalChunkStart: 4, ChunkCount: 4, Kind: AllocationKindData, PhysicalChunkStart: 100},
			},
		},
		{
			VolumeID:       "00a1b2c3",
			PageNo:         0,
			PageBytes:      4096,
			ChunkSizeBytes: 1024,
			Revision:       7,
			Extents: []AllocationExtentRecord{
				{LogicalChunkStart: 0, ChunkCount: 4, Kind: AllocationKindZero},
			},
		},
	}
	if err := repo.CaptureSnapshotAllocationPages(ctx, snapshotID, pages); err != nil {
		t.Fatalf("CaptureSnapshotAllocationPages: %v", err)
	}

	list, err := repo.ListSnapshotAllocationPages(ctx, snapshotID)
	if err != nil {
		t.Fatalf("ListSnapshotAllocationPages: %v", err)
	}
	if len(list) != 2 {
		t.Fatalf("snapshot allocation pages=%d want=2", len(list))
	}
	if list[0].PageNo != 0 || list[1].PageNo != 1 {
		t.Fatalf("snapshot allocation pages should be sorted by page_no: %+v", list)
	}
	page, err := repo.GetSnapshotAllocationPage(ctx, snapshotID, 1)
	if err != nil {
		t.Fatalf("GetSnapshotAllocationPage: %v", err)
	}
	if page.Extents[0].PhysicalChunkStart != 100 {
		t.Fatalf("physical chunk start=%d want=100", page.Extents[0].PhysicalChunkStart)
	}
}

func TestRepositoryUsesCanonicalVolumeIDInKeys(t *testing.T) {
	repo := NewRepository(newFakeKV(), "phase-e-test")
	got := extentMappingKey(repo.root, "00A1B2C3", 9)
	want := fmt.Sprintf("%s/volumes/%s/extents/%020d", repo.root, "00a1b2c3", 9)
	if got != want {
		t.Fatalf("extentMappingKey mismatch: got=%q want=%q", got, want)
	}
	got = allocationPageKey(repo.root, "00A1B2C3", 7)
	want = fmt.Sprintf("%s/volumes/%s/allocation/pages/%020d", repo.root, "00a1b2c3", 7)
	if got != want {
		t.Fatalf("allocationPageKey mismatch: got=%q want=%q", got, want)
	}
}

func TestRepositoryFindMutationOperationByID(t *testing.T) {
	ctx := context.Background()
	repo := NewRepository(newFakeKV(), "phase-g-test")
	if err := repo.PutMutationOperation(ctx, MutationOperationRecord{
		OperationID:        "payload-gc-00a1b2c3",
		VolumeID:           "00a1b2c3",
		Kind:               "payload_gc",
		State:              MutationOperationCommitted,
		AllocationRevision: 12,
	}); err != nil {
		t.Fatalf("PutMutationOperation: %v", err)
	}

	rec, err := repo.FindMutationOperationByID(ctx, "payload-gc-00a1b2c3")
	if err != nil {
		t.Fatalf("FindMutationOperationByID: %v", err)
	}
	if rec.VolumeID != "00a1b2c3" || rec.Kind != "payload_gc" {
		t.Fatalf("record=%+v", rec)
	}
}

func TestServiceResolveExtentPlacements(t *testing.T) {
	ctx := context.Background()
	repo := NewRepository(newFakeKV(), "phase-e-test")
	svc := NewService(repo)

	for _, rec := range []ExtentMappingRecord{
		{
			VolumeID:      "00a1b2c3",
			ExtentID:      1,
			LogicalOffset: 0,
			LengthBytes:   4 << 20,
			ChunkID:       100,
			PlacementRef:  "pl-0001",
			Revision:      1,
		},
		{
			VolumeID:      "00a1b2c3",
			ExtentID:      2,
			LogicalOffset: 4 << 20,
			LengthBytes:   4 << 20,
			ChunkID:       200,
			PlacementRef:  "pl-0002",
			Revision:      1,
		},
	} {
		if err := repo.PutExtentMapping(ctx, rec); err != nil {
			t.Fatalf("PutExtentMapping(%d): %v", rec.ExtentID, err)
		}
	}
	for _, rec := range []ReplicaSetState{
		{
			ReplicaSetID:     "rs-1",
			VolumeID:         "00a1b2c3",
			PlacementRef:     "pl-0001",
			Epoch:            1,
			PrimaryReplicaID: "rep-a",
			WriteQuorum:      2,
			ReadQuorum:       1,
		},
		{
			ReplicaSetID:     "rs-2",
			VolumeID:         "00a1b2c3",
			PlacementRef:     "pl-0002",
			Epoch:            1,
			PrimaryReplicaID: "rep-b",
			WriteQuorum:      2,
			ReadQuorum:       1,
		},
	} {
		if err := repo.PutReplicaSet(ctx, rec); err != nil {
			t.Fatalf("PutReplicaSet(%s): %v", rec.ReplicaSetID, err)
		}
	}

	resolved, err := svc.ResolveExtentPlacements(ctx, "00a1b2c3", 2<<20, 5<<20)
	if err != nil {
		t.Fatalf("ResolveExtentPlacements: %v", err)
	}
	if len(resolved) != 2 {
		t.Fatalf("ResolveExtentPlacements count=%d want=2", len(resolved))
	}
	if resolved[0].ExtentMapping.ExtentID != 1 || resolved[0].ReplicaSet.ReplicaSetID != "rs-1" {
		t.Fatalf("first resolved mismatch: %+v", resolved[0])
	}
	if resolved[1].ExtentMapping.ExtentID != 2 || resolved[1].ReplicaSet.ReplicaSetID != "rs-2" {
		t.Fatalf("second resolved mismatch: %+v", resolved[1])
	}

	resolvedWithStats, stats, err := svc.ResolveExtentPlacementsDetailed(ctx, "00a1b2c3", 2<<20, 5<<20)
	if err != nil {
		t.Fatalf("ResolveExtentPlacementsDetailed: %v", err)
	}
	if len(resolvedWithStats) != 2 {
		t.Fatalf("ResolveExtentPlacementsDetailed count=%d want=2", len(resolvedWithStats))
	}
	if stats.MappingCountTotal != 2 || stats.MappingCountSelected != 2 {
		t.Fatalf("mapping stats=%+v, want total=2 selected=2", stats)
	}
	if stats.ReplicaSetCount != 2 {
		t.Fatalf("ReplicaSetCount=%d want=2", stats.ReplicaSetCount)
	}
}

func TestRepositoryCommitWriteMetadataCAS(t *testing.T) {
	ctx := context.Background()
	repo := NewRepository(newFakeKV(), "phase-e-test")

	if err := repo.PutVolumeState(ctx, VolumeState{
		VolumeID: "00a1b2c3",
		Epoch:    5,
		Revision: 11,
		Status:   VolumeStatusHealthy,
	}); err != nil {
		t.Fatalf("PutVolumeState: %v", err)
	}
	if err := repo.PutIdempotencyRecord(ctx, IdempotencyRecord{
		IdempotencyKey: "idem-1",
		VolumeID:       "00a1b2c3",
		AttachmentID:   "att-1",
		Generation:     8,
		Epoch:          5,
		Revision:       11,
		Operation:      "write",
		ResultState:    IdempotencyPending,
	}); err != nil {
		t.Fatalf("PutIdempotencyRecord: %v", err)
	}

	state, record, err := repo.CommitWriteMetadata(ctx, CommitWriteMetadataRequest{
		VolumeID:                 "00a1b2c3",
		ExpectedEpoch:            5,
		ExpectedRevision:         11,
		IdempotencyKey:           "idem-1",
		ExpectedIdempotencyState: IdempotencyPending,
		CommittedRevision:        12,
	})
	if err != nil {
		t.Fatalf("CommitWriteMetadata: %v", err)
	}
	if state.Revision != 12 {
		t.Fatalf("state revision=%d want=12", state.Revision)
	}
	if record.ResultState != IdempotencyCommitted || record.Revision != 12 {
		t.Fatalf("record=%+v", record)
	}

	if _, _, err := repo.CommitWriteMetadata(ctx, CommitWriteMetadataRequest{
		VolumeID:                 "00a1b2c3",
		ExpectedEpoch:            5,
		ExpectedRevision:         11,
		IdempotencyKey:           "idem-1",
		ExpectedIdempotencyState: IdempotencyPending,
		CommittedRevision:        13,
	}); err != ErrCASConflict {
		t.Fatalf("second CommitWriteMetadata err=%v want=%v", err, ErrCASConflict)
	}
}

func TestRepositoryCommitWriteMetadataUsesTransactionalKV(t *testing.T) {
	ctx := context.Background()
	kv := newFakeTransactionalKV()
	repo := NewRepository(kv, "phase-e-test")
	if err := repo.PutVolumeState(ctx, VolumeState{
		VolumeID: "00a1b2c3",
		Epoch:    4,
		Revision: 11,
	}); err != nil {
		t.Fatalf("PutVolumeState: %v", err)
	}
	if err := repo.PutIdempotencyRecord(ctx, IdempotencyRecord{
		VolumeID:       "00a1b2c3",
		IdempotencyKey: "idem-1",
		ResultState:    IdempotencyPending,
		Revision:       11,
	}); err != nil {
		t.Fatalf("PutIdempotencyRecord: %v", err)
	}

	state, record, err := repo.CommitWriteMetadata(ctx, CommitWriteMetadataRequest{
		VolumeID:                 "00a1b2c3",
		ExpectedEpoch:            4,
		ExpectedRevision:         11,
		IdempotencyKey:           "idem-1",
		ExpectedIdempotencyState: IdempotencyPending,
		CommittedRevision:        12,
	})
	if err != nil {
		t.Fatalf("CommitWriteMetadata: %v", err)
	}
	if kv.runTxCalls == 0 {
		t.Fatalf("expected RunInTransaction to be used")
	}
	if state.Revision != 12 || record.Revision != 12 || record.ResultState != IdempotencyCommitted {
		t.Fatalf("unexpected commit result: state=%+v record=%+v", state, record)
	}
}

func TestRepositoryCommitWriteMetadataPersistsAllocationPages(t *testing.T) {
	ctx := context.Background()
	repo := NewRepository(newFakeKV(), "phase-g-test")
	if err := repo.PutVolumeState(ctx, VolumeState{
		VolumeID: "00a1b2c3",
		Epoch:    5,
		Revision: 11,
		Status:   VolumeStatusHealthy,
	}); err != nil {
		t.Fatalf("PutVolumeState: %v", err)
	}
	if err := repo.PutIdempotencyRecord(ctx, IdempotencyRecord{
		IdempotencyKey: "idem-alloc-commit-1",
		VolumeID:       "00a1b2c3",
		AttachmentID:   "att-1",
		Generation:     7,
		Epoch:          5,
		Revision:       11,
		Operation:      "write",
		ResultState:    IdempotencyPending,
	}); err != nil {
		t.Fatalf("PutIdempotencyRecord: %v", err)
	}
	if err := repo.PutMutationOperation(ctx, MutationOperationRecord{
		OperationID:        "write-6964656d2d616c6c6f632d636f6d6d69742d31",
		VolumeID:           "00a1b2c3",
		Kind:               "write",
		State:              MutationOperationRunning,
		AllocationRevision: 11,
		WriterFencingEpoch: 5,
		IdempotencyKey:     "idem-alloc-commit-1",
		StartedAtUnix:      100,
		LastUpdatedAtUnix:  100,
	}); err != nil {
		t.Fatalf("PutMutationOperation: %v", err)
	}
	if err := repo.PutExtentMapping(ctx, ExtentMappingRecord{
		VolumeID:      "00a1b2c3",
		ExtentID:      1,
		LogicalOffset: 0,
		LengthBytes:   8,
		ChunkID:       101,
		PlacementRef:  "pl-1",
		Revision:      11,
	}); err != nil {
		t.Fatalf("PutExtentMapping: %v", err)
	}
	header := testPayloadEncryptionHeader("replicated:00a1b2c3:101", PhysicalObjectBackendReplicated, 4)

	_, _, err := repo.CommitWriteMetadata(ctx, CommitWriteMetadataRequest{
		VolumeID:                 "00a1b2c3",
		ExpectedEpoch:            5,
		ExpectedRevision:         11,
		IdempotencyKey:           "idem-alloc-commit-1",
		ExpectedIdempotencyState: IdempotencyPending,
		CommittedRevision:        12,
		MutationOperationID:      "write-6964656d2d616c6c6f632d636f6d6d69742d31",
		ExpectedMutationState:    MutationOperationRunning,
		AffectedExtentIDs:        []uint64{1},
		AffectedPageNos:          []uint64{0},
		RetiredPhysicalChunkIDs:  []uint64{77, 88},
		NormalizeExtentMappings:  []uint64{1},
		AllocationPages: []AllocationPageRecord{
			{
				VolumeID:       "00a1b2c3",
				PageNo:         0,
				PageBytes:      8,
				ChunkSizeBytes: 4,
				Extents: []AllocationExtentRecord{
					{LogicalChunkStart: 0, ChunkCount: 1, Kind: AllocationKindData, PhysicalChunkStart: 101, Encryption: header},
					{LogicalChunkStart: 1, ChunkCount: 1, Kind: AllocationKindData, PhysicalChunkStart: 102},
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("CommitWriteMetadata: %v", err)
	}

	page, err := repo.GetAllocationPage(ctx, "00a1b2c3", 0)
	if err != nil {
		t.Fatalf("GetAllocationPage: %v", err)
	}
	if page.Revision != 12 {
		t.Fatalf("allocation page revision=%d want=12", page.Revision)
	}
	if len(page.Extents) != 2 || page.Extents[0].Kind != AllocationKindData || page.Extents[0].PhysicalChunkStart != 101 || page.Extents[1].PhysicalChunkStart != 102 {
		t.Fatalf("allocation page extents=%+v", page.Extents)
	}
	if page.Extents[0].Encryption == nil || page.Extents[0].Encryption.ObjectID != header.ObjectID || page.Extents[1].Encryption != nil {
		t.Fatalf("allocation page encryption headers=%+v", page.Extents)
	}
	operation, err := repo.GetMutationOperation(ctx, "00a1b2c3", "write-6964656d2d616c6c6f632d636f6d6d69742d31")
	if err != nil {
		t.Fatalf("GetMutationOperation: %v", err)
	}
	if operation.State != MutationOperationCommitted || operation.AllocationRevision != 12 {
		t.Fatalf("mutation operation=%+v", operation)
	}
	if len(operation.RetiredPhysicalChunkIDs) != 2 || operation.RetiredPhysicalChunkIDs[0] != 77 || operation.RetiredPhysicalChunkIDs[1] != 88 {
		t.Fatalf("retired physical chunk ids=%v", operation.RetiredPhysicalChunkIDs)
	}
	if len(operation.AffectedExtentIDs) != 1 || operation.AffectedExtentIDs[0] != 1 {
		t.Fatalf("affected extent ids=%v", operation.AffectedExtentIDs)
	}
	if len(operation.AffectedPageNos) != 1 || operation.AffectedPageNos[0] != 0 {
		t.Fatalf("affected page nos=%v", operation.AffectedPageNos)
	}
	mapping, err := repo.GetExtentMapping(ctx, "00a1b2c3", 1)
	if err != nil {
		t.Fatalf("GetExtentMapping: %v", err)
	}
	if mapping.ChunkID != 0 || mapping.Revision != 12 {
		t.Fatalf("extent mapping=%+v", mapping)
	}
}

func TestRepositoryCommitPageScopedWriteMetadataAllowsDisjointPageCommits(t *testing.T) {
	ctx := context.Background()
	repo := NewRepository(newFakeKV(), "phase-g-test")
	if err := repo.PutVolumeState(ctx, VolumeState{
		VolumeID: "00a1b2c3",
		Epoch:    5,
		Revision: 11,
		Status:   VolumeStatusHealthy,
	}); err != nil {
		t.Fatalf("PutVolumeState: %v", err)
	}
	for _, page := range []AllocationPageRecord{
		{
			VolumeID:       "00a1b2c3",
			PageNo:         0,
			PageBytes:      8,
			ChunkSizeBytes: 4,
			Revision:       3,
			Extents: []AllocationExtentRecord{
				{LogicalChunkStart: 0, ChunkCount: 2, Kind: AllocationKindZero},
			},
		},
		{
			VolumeID:       "00a1b2c3",
			PageNo:         1,
			PageBytes:      8,
			ChunkSizeBytes: 4,
			Revision:       7,
			Extents: []AllocationExtentRecord{
				{LogicalChunkStart: 2, ChunkCount: 2, Kind: AllocationKindZero},
			},
		},
	} {
		if err := repo.PutAllocationPage(ctx, page); err != nil {
			t.Fatalf("PutAllocationPage(%d): %v", page.PageNo, err)
		}
	}
	for _, key := range []string{"idem-page-0", "idem-page-1"} {
		if err := repo.PutIdempotencyRecord(ctx, IdempotencyRecord{
			IdempotencyKey: key,
			VolumeID:       "00a1b2c3",
			AttachmentID:   "att-1",
			Generation:     7,
			Epoch:          5,
			Revision:       11,
			Operation:      "write",
			ResultState:    IdempotencyPending,
		}); err != nil {
			t.Fatalf("PutIdempotencyRecord(%s): %v", key, err)
		}
	}

	state0, record0, err := repo.CommitPageScopedWriteMetadata(ctx, CommitWriteMetadataRequest{
		VolumeID:                 "00a1b2c3",
		ExpectedEpoch:            5,
		ExpectedRevision:         11,
		IdempotencyKey:           "idem-page-0",
		ExpectedIdempotencyState: IdempotencyPending,
		CommittedRevision:        12,
		AllocationPages: []AllocationPageRecord{
			{
				VolumeID:       "00a1b2c3",
				PageNo:         0,
				PageBytes:      8,
				ChunkSizeBytes: 4,
				Revision:       3,
				Extents: []AllocationExtentRecord{
					{LogicalChunkStart: 0, ChunkCount: 2, Kind: AllocationKindData, PhysicalChunkStart: 101},
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("CommitPageScopedWriteMetadata page0: %v", err)
	}
	if state0.Revision != 11 || record0.Revision != 4 || record0.ResultState != IdempotencyCommitted {
		t.Fatalf("page0 commit state=%+v record=%+v", state0, record0)
	}

	state1, record1, err := repo.CommitPageScopedWriteMetadata(ctx, CommitWriteMetadataRequest{
		VolumeID:                 "00a1b2c3",
		ExpectedEpoch:            5,
		ExpectedRevision:         11,
		IdempotencyKey:           "idem-page-1",
		ExpectedIdempotencyState: IdempotencyPending,
		CommittedRevision:        12,
		AllocationPages: []AllocationPageRecord{
			{
				VolumeID:       "00a1b2c3",
				PageNo:         1,
				PageBytes:      8,
				ChunkSizeBytes: 4,
				Revision:       7,
				Extents: []AllocationExtentRecord{
					{LogicalChunkStart: 2, ChunkCount: 2, Kind: AllocationKindData, PhysicalChunkStart: 201},
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("CommitPageScopedWriteMetadata page1: %v", err)
	}
	if state1.Revision != 11 || record1.Revision != 8 || record1.ResultState != IdempotencyCommitted {
		t.Fatalf("page1 commit state=%+v record=%+v", state1, record1)
	}

	state, err := repo.GetVolumeState(ctx, "00a1b2c3")
	if err != nil {
		t.Fatalf("GetVolumeState: %v", err)
	}
	if state.Revision != 11 {
		t.Fatalf("volume revision=%d want=11", state.Revision)
	}
	page0, err := repo.GetAllocationPage(ctx, "00a1b2c3", 0)
	if err != nil {
		t.Fatalf("GetAllocationPage page0: %v", err)
	}
	page1, err := repo.GetAllocationPage(ctx, "00a1b2c3", 1)
	if err != nil {
		t.Fatalf("GetAllocationPage page1: %v", err)
	}
	if page0.Revision != 4 || page1.Revision != 8 {
		t.Fatalf("page revisions=(%d,%d) want=(4,8)", page0.Revision, page1.Revision)
	}
}

func TestRepositoryCommitRangeLocalWriteStateLeavesVolumeAndEffectsUntouched(t *testing.T) {
	ctx := context.Background()
	kv := newFakeKV()
	repo := NewRepository(kv, "phase-g-test")
	if err := repo.PutVolumeState(ctx, VolumeState{
		VolumeID: "00a1b2c3",
		Epoch:    5,
		Revision: 11,
		Status:   VolumeStatusHealthy,
	}); err != nil {
		t.Fatalf("PutVolumeState: %v", err)
	}
	if err := repo.PutAllocationPage(ctx, AllocationPageRecord{
		VolumeID:       "00a1b2c3",
		PageNo:         0,
		PageBytes:      8,
		ChunkSizeBytes: 4,
		Revision:       4,
		Extents: []AllocationExtentRecord{
			{LogicalChunkStart: 0, ChunkCount: 2, Kind: AllocationKindZero},
		},
	}); err != nil {
		t.Fatalf("PutAllocationPage: %v", err)
	}
	for _, key := range []string{"idem-range-local-1", "idem-range-local-2"} {
		if err := repo.PutIdempotencyRecord(ctx, IdempotencyRecord{
			IdempotencyKey: key,
			VolumeID:       "00a1b2c3",
			AttachmentID:   "att-1",
			Generation:     7,
			Epoch:          5,
			Revision:       11,
			Operation:      "write",
			ResultState:    IdempotencyPending,
		}); err != nil {
			t.Fatalf("PutIdempotencyRecord(%s): %v", key, err)
		}
	}

	state, record, err := repo.CommitRangeLocalWriteState(ctx, CommitWriteMetadataRequest{
		VolumeID:                 "00a1b2c3",
		ExpectedEpoch:            5,
		ExpectedRevision:         11,
		IdempotencyKey:           "idem-range-local-1",
		ExpectedIdempotencyState: IdempotencyPending,
		CommittedRevision:        12,
		AllocationPages: []AllocationPageRecord{
			{
				VolumeID:       "00a1b2c3",
				PageNo:         0,
				PageBytes:      8,
				ChunkSizeBytes: 4,
				Revision:       4,
				Extents: []AllocationExtentRecord{
					{LogicalChunkStart: 0, ChunkCount: 2, Kind: AllocationKindData, PhysicalChunkStart: 101},
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("CommitRangeLocalWriteState first: %v", err)
	}
	if state.Revision != 11 || record.Revision != 5 || record.ResultState != IdempotencyCommitted {
		t.Fatalf("first range-local commit state=%+v record=%+v", state, record)
	}

	state, record, err = repo.CommitRangeLocalWriteState(ctx, CommitWriteMetadataRequest{
		VolumeID:                 "00a1b2c3",
		ExpectedEpoch:            5,
		ExpectedRevision:         11,
		IdempotencyKey:           "idem-range-local-2",
		ExpectedIdempotencyState: IdempotencyPending,
		CommittedRevision:        12,
		AllocationPages: []AllocationPageRecord{
			{
				VolumeID:       "00a1b2c3",
				PageNo:         0,
				PageBytes:      8,
				ChunkSizeBytes: 4,
				Revision:       4,
				Extents: []AllocationExtentRecord{
					{LogicalChunkStart: 0, ChunkCount: 2, Kind: AllocationKindData, PhysicalChunkStart: 201},
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("CommitRangeLocalWriteState second: %v", err)
	}
	if state.Revision != 11 || record.Revision != 6 || record.ResultState != IdempotencyCommitted {
		t.Fatalf("second range-local commit state=%+v record=%+v", state, record)
	}

	volume, err := repo.GetVolumeState(ctx, "00a1b2c3")
	if err != nil {
		t.Fatalf("GetVolumeState: %v", err)
	}
	if volume.Revision != 11 {
		t.Fatalf("volume revision=%d want=11", volume.Revision)
	}
	page, err := repo.GetAllocationPage(ctx, "00a1b2c3", 0)
	if err != nil {
		t.Fatalf("GetAllocationPage: %v", err)
	}
	if page.Revision != 4 || page.Extents[0].Kind != AllocationKindZero {
		t.Fatalf("allocation page changed before effects apply: %+v", page)
	}
	rangeState, err := readRangeLocalWriteState(ctx, kv, repo.root, "00a1b2c3", 0)
	if err != nil {
		t.Fatalf("readRangeLocalWriteState: %v", err)
	}
	if rangeState.Revision != 6 || rangeState.IdempotencyKey != "idem-range-local-2" {
		t.Fatalf("range-local state=%+v want revision=6 idempotency=idem-range-local-2", rangeState)
	}
}

func TestRepositoryPutWriteIntentStoresRecordsInTransaction(t *testing.T) {
	kv := newFakeTransactionalKV()
	repo := NewRepository(kv, "phase-g-test")
	ctx := context.Background()
	record := IdempotencyRecord{
		IdempotencyKey: "idem-combined",
		VolumeID:       "00a1b2c3",
		AttachmentID:   "att-1",
		Generation:     7,
		Operation:      "write",
		ResultState:    IdempotencyPending,
	}
	operation := MutationOperationRecord{
		OperationID:        "write-combined",
		VolumeID:           "00a1b2c3",
		Kind:               "write",
		State:              MutationOperationRunning,
		WriterFencingEpoch: 3,
		IdempotencyKey:     "idem-combined",
	}
	if err := repo.PutWriteIntent(ctx, record, operation); err != nil {
		t.Fatalf("PutWriteIntent: %v", err)
	}
	if kv.runTxCalls != 1 {
		t.Fatalf("RunInTransaction calls=%d want 1", kv.runTxCalls)
	}
	gotRecord, err := repo.GetIdempotencyRecord(ctx, "00a1b2c3", "idem-combined")
	if err != nil {
		t.Fatalf("GetIdempotencyRecord: %v", err)
	}
	if gotRecord.ResultState != IdempotencyPending || gotRecord.AttachmentID != "att-1" {
		t.Fatalf("idempotency record=%+v", gotRecord)
	}
	gotOperation, err := repo.GetMutationOperation(ctx, "00a1b2c3", "write-combined")
	if err != nil {
		t.Fatalf("GetMutationOperation: %v", err)
	}
	if gotOperation.State != MutationOperationRunning || gotOperation.IdempotencyKey != "idem-combined" {
		t.Fatalf("mutation operation=%+v", gotOperation)
	}
}

func TestRepositoryPutWriteIntentBatchStoresRecordsInOneTransaction(t *testing.T) {
	kv := newFakeTransactionalKV()
	repo := NewRepository(kv, "phase-g-test")
	ctx := context.Background()
	intents := []WriteIntentRecord{
		{
			IdempotencyRecord: IdempotencyRecord{
				IdempotencyKey: "idem-batch-1",
				VolumeID:       "00a1b2c3",
				AttachmentID:   "att-1",
				Operation:      "write",
				ResultState:    IdempotencyPending,
			},
			MutationOperation: MutationOperationRecord{
				OperationID:    "write-batch-1",
				VolumeID:       "00a1b2c3",
				Kind:           "write",
				State:          MutationOperationRunning,
				IdempotencyKey: "idem-batch-1",
			},
		},
		{
			IdempotencyRecord: IdempotencyRecord{
				IdempotencyKey: "idem-batch-2",
				VolumeID:       "00a1b2c3",
				AttachmentID:   "att-2",
				Operation:      "write",
				ResultState:    IdempotencyPending,
			},
			MutationOperation: MutationOperationRecord{
				OperationID:    "write-batch-2",
				VolumeID:       "00a1b2c3",
				Kind:           "write",
				State:          MutationOperationRunning,
				IdempotencyKey: "idem-batch-2",
			},
		},
	}
	if err := repo.PutWriteIntentBatch(ctx, intents); err != nil {
		t.Fatalf("PutWriteIntentBatch: %v", err)
	}
	if kv.runTxCalls != 1 {
		t.Fatalf("RunInTransaction calls=%d want 1", kv.runTxCalls)
	}
	for _, intent := range intents {
		record, err := repo.GetIdempotencyRecord(ctx, intent.IdempotencyRecord.VolumeID, intent.IdempotencyRecord.IdempotencyKey)
		if err != nil {
			t.Fatalf("GetIdempotencyRecord %s: %v", intent.IdempotencyRecord.IdempotencyKey, err)
		}
		if record.ResultState != IdempotencyPending || record.AttachmentID != intent.IdempotencyRecord.AttachmentID {
			t.Fatalf("idempotency record=%+v want attachment=%s", record, intent.IdempotencyRecord.AttachmentID)
		}
		operation, err := repo.GetMutationOperation(ctx, intent.MutationOperation.VolumeID, intent.MutationOperation.OperationID)
		if err != nil {
			t.Fatalf("GetMutationOperation %s: %v", intent.MutationOperation.OperationID, err)
		}
		if operation.State != MutationOperationRunning || operation.IdempotencyKey != intent.IdempotencyRecord.IdempotencyKey {
			t.Fatalf("mutation operation=%+v", operation)
		}
	}
}

func TestRepositoryCommitAppendOnlyWriteStateLeavesVolumeAndEffectsUntouched(t *testing.T) {
	ctx := context.Background()
	repo := NewRepository(newFakeKV(), "phase-g-test")
	if err := repo.PutVolumeState(ctx, VolumeState{
		VolumeID: "00a1b2c3",
		Epoch:    5,
		Revision: 11,
		Status:   VolumeStatusHealthy,
	}); err != nil {
		t.Fatalf("PutVolumeState: %v", err)
	}
	if err := repo.PutAllocationPage(ctx, AllocationPageRecord{
		VolumeID:       "00a1b2c3",
		PageNo:         0,
		PageBytes:      8,
		ChunkSizeBytes: 4,
		Revision:       4,
		Extents: []AllocationExtentRecord{
			{LogicalChunkStart: 0, ChunkCount: 2, Kind: AllocationKindZero},
		},
	}); err != nil {
		t.Fatalf("PutAllocationPage: %v", err)
	}
	if err := repo.PutIdempotencyRecord(ctx, IdempotencyRecord{
		IdempotencyKey: "idem-append-only",
		VolumeID:       "00a1b2c3",
		AttachmentID:   "att-1",
		Generation:     7,
		Epoch:          5,
		Revision:       11,
		Operation:      "write",
		ResultState:    IdempotencyPending,
	}); err != nil {
		t.Fatalf("PutIdempotencyRecord: %v", err)
	}

	state, record, err := repo.CommitAppendOnlyWriteState(ctx, CommitWriteStateRequest{
		VolumeID:                 "00a1b2c3",
		ExpectedEpoch:            5,
		ExpectedRevision:         11,
		IdempotencyKey:           "idem-append-only",
		ExpectedIdempotencyState: IdempotencyPending,
		CommittedRevision:        12,
	})
	if err != nil {
		t.Fatalf("CommitAppendOnlyWriteState: %v", err)
	}
	if state.Revision <= 12 || record.Revision != state.Revision || record.ResultState != IdempotencyCommitted {
		t.Fatalf("append-only commit state=%+v record=%+v", state, record)
	}

	volume, err := repo.GetVolumeState(ctx, "00a1b2c3")
	if err != nil {
		t.Fatalf("GetVolumeState: %v", err)
	}
	if volume.Revision != 11 {
		t.Fatalf("volume revision=%d want=11", volume.Revision)
	}
	page, err := repo.GetAllocationPage(ctx, "00a1b2c3", 0)
	if err != nil {
		t.Fatalf("GetAllocationPage: %v", err)
	}
	if page.Revision != 4 || page.Extents[0].Kind != AllocationKindZero {
		t.Fatalf("allocation page changed before effects apply: %+v", page)
	}
}

func TestRepositoryCommitAppendOnlyWriteMetadataBatchCommitsStateAndEffectsInOneTransaction(t *testing.T) {
	ctx := context.Background()
	kv := newFakeTransactionalKV()
	repo := NewRepository(kv, "phase-g-test")
	repo.rememberNativeAllocationVolume("00a1b2c3")
	if err := repo.PutVolumeState(ctx, VolumeState{
		VolumeID: "00a1b2c3",
		Epoch:    5,
		Revision: 11,
		Status:   VolumeStatusHealthy,
	}); err != nil {
		t.Fatalf("PutVolumeState: %v", err)
	}
	reqs := []CommitWriteMetadataRequest{
		{
			VolumeID:                 "00a1b2c3",
			ExpectedEpoch:            5,
			ExpectedRevision:         11,
			IdempotencyKey:           "idem-append-batch-1",
			ExpectedIdempotencyState: IdempotencyPending,
			CommittedRevision:        12,
			MutationOperationID:      "write-append-batch-1",
			ExpectedMutationState:    MutationOperationRunning,
			AffectedPageNos:          []uint64{0},
			AllocationPages: []AllocationPageRecord{{
				VolumeID:       "00a1b2c3",
				PageNo:         0,
				PageBytes:      8,
				ChunkSizeBytes: 4,
				Extents: []AllocationExtentRecord{
					{LogicalChunkStart: 0, ChunkCount: 1, Kind: AllocationKindData, PhysicalChunkStart: 101},
				},
			}},
		},
		{
			VolumeID:                 "00a1b2c3",
			ExpectedEpoch:            5,
			ExpectedRevision:         11,
			IdempotencyKey:           "idem-append-batch-2",
			ExpectedIdempotencyState: IdempotencyPending,
			CommittedRevision:        13,
			MutationOperationID:      "write-append-batch-2",
			ExpectedMutationState:    MutationOperationRunning,
			AffectedPageNos:          []uint64{0},
			AllocationPages: []AllocationPageRecord{{
				VolumeID:       "00a1b2c3",
				PageNo:         0,
				PageBytes:      8,
				ChunkSizeBytes: 4,
				Extents: []AllocationExtentRecord{
					{LogicalChunkStart: 1, ChunkCount: 1, Kind: AllocationKindData, PhysicalChunkStart: 201},
				},
			}},
		},
	}
	for _, req := range reqs {
		if err := repo.PutIdempotencyRecord(ctx, IdempotencyRecord{
			IdempotencyKey: req.IdempotencyKey,
			VolumeID:       req.VolumeID,
			Epoch:          req.ExpectedEpoch,
			Revision:       req.ExpectedRevision,
			Operation:      "write",
			ResultState:    IdempotencyPending,
		}); err != nil {
			t.Fatalf("PutIdempotencyRecord %s: %v", req.IdempotencyKey, err)
		}
		if err := repo.PutMutationOperation(ctx, MutationOperationRecord{
			OperationID:        req.MutationOperationID,
			VolumeID:           req.VolumeID,
			Kind:               "write",
			State:              MutationOperationRunning,
			AllocationRevision: req.CommittedRevision - 1,
			IdempotencyKey:     req.IdempotencyKey,
			AffectedPageNos:    append([]uint64(nil), req.AffectedPageNos...),
			StartedAtUnix:      100,
			LastUpdatedAtUnix:  100,
		}); err != nil {
			t.Fatalf("PutMutationOperation %s: %v", req.MutationOperationID, err)
		}
	}

	kv.resetGetCalls()
	kv.resetSetCalls()
	states, records, err := repo.CommitAppendOnlyWriteMetadataBatch(ctx, reqs)
	if err != nil {
		t.Fatalf("CommitAppendOnlyWriteMetadataBatch: %v", err)
	}
	if kv.runTxCalls != 1 {
		t.Fatalf("RunInTransaction calls=%d want 1", kv.runTxCalls)
	}
	if len(states) != 2 || len(records) != 2 {
		t.Fatalf("states=%d records=%d want 2 each", len(states), len(records))
	}
	if records[0].Revision == 0 || records[1].Revision <= records[0].Revision {
		t.Fatalf("records not assigned monotonic append-only revisions: %+v", records)
	}
	if got := kv.getCallCount(volumeStateKey("phase-g-test", "00a1b2c3")); got != 1 {
		t.Fatalf("volume state get calls=%d want 1", got)
	}
	if got := kv.setCallCount(allocationPageKey("phase-g-test", "00a1b2c3", 0)); got != 1 {
		t.Fatalf("allocation page set calls=%d want 1", got)
	}
	page, err := repo.GetAllocationPage(ctx, "00a1b2c3", 0)
	if err != nil {
		t.Fatalf("GetAllocationPage: %v", err)
	}
	chunks, err := expandAllocationChunkMappings(page)
	if err != nil {
		t.Fatalf("expandAllocationChunkMappings: %v", err)
	}
	if !slices.Equal(chunks, []uint64{101, 201}) || page.Revision != records[1].Revision {
		t.Fatalf("page revision=%d chunks=%v, want revision=%d chunks [101 201]", page.Revision, chunks, records[1].Revision)
	}
	for i, req := range reqs {
		record, err := repo.GetIdempotencyRecord(ctx, req.VolumeID, req.IdempotencyKey)
		if err != nil {
			t.Fatalf("GetIdempotencyRecord %s: %v", req.IdempotencyKey, err)
		}
		if record.ResultState != IdempotencyCommitted || record.Revision != records[i].Revision {
			t.Fatalf("idempotency %s=%+v want committed revision %d", req.IdempotencyKey, record, records[i].Revision)
		}
		operation, err := repo.GetMutationOperation(ctx, req.VolumeID, req.MutationOperationID)
		if err != nil {
			t.Fatalf("GetMutationOperation %s: %v", req.MutationOperationID, err)
		}
		if operation.State != MutationOperationCommitted || operation.AllocationRevision != records[i].Revision {
			t.Fatalf("operation %s=%+v want committed revision %d", req.MutationOperationID, operation, records[i].Revision)
		}
	}
}

func TestRepositoryCommitAppendOnlyWriteMetadataBatchReadsAppendOnlyStateInOneBatch(t *testing.T) {
	ctx := context.Background()
	kv := newFakeTransactionalKV()
	repo := NewRepository(kv, "phase-g-test")
	if err := repo.PutVolumeState(ctx, VolumeState{
		VolumeID: "00a1b2c3",
		Epoch:    5,
		Revision: 11,
		Status:   VolumeStatusHealthy,
	}); err != nil {
		t.Fatalf("PutVolumeState: %v", err)
	}

	reqs := make([]CommitWriteMetadataRequest, 0, 3)
	for i := 0; i < 3; i++ {
		idemKey := fmt.Sprintf("idem-append-batch-read-%d", i)
		req := CommitWriteMetadataRequest{
			VolumeID:                 "00a1b2c3",
			ExpectedEpoch:            5,
			ExpectedRevision:         11,
			IdempotencyKey:           idemKey,
			ExpectedIdempotencyState: IdempotencyPending,
			CommittedRevision:        uint64(12 + i),
		}
		reqs = append(reqs, req)
		if err := repo.PutIdempotencyRecord(ctx, IdempotencyRecord{
			IdempotencyKey: idemKey,
			VolumeID:       req.VolumeID,
			Epoch:          req.ExpectedEpoch,
			Revision:       req.ExpectedRevision,
			Operation:      "write",
			ResultState:    IdempotencyPending,
		}); err != nil {
			t.Fatalf("PutIdempotencyRecord %s: %v", idemKey, err)
		}
	}

	kv.resetGetCalls()
	states, records, err := repo.CommitAppendOnlyWriteMetadataBatch(ctx, reqs)
	if err != nil {
		t.Fatalf("CommitAppendOnlyWriteMetadataBatch: %v", err)
	}
	if len(states) != len(reqs) || len(records) != len(reqs) {
		t.Fatalf("states=%d records=%d want %d each", len(states), len(records), len(reqs))
	}
	if got := kv.batchGetCallCount(); got != 1 {
		t.Fatalf("backend BatchGet calls=%d want 1", got)
	}
	stateKey := volumeStateKey("phase-g-test", "00a1b2c3")
	keyCounts := make(map[string]int)
	for _, key := range kv.lastBatchGetKeys() {
		keyCounts[key]++
	}
	if keyCounts[stateKey] != 1 {
		t.Fatalf("volume state key batch count=%d want 1 keys=%v", keyCounts[stateKey], kv.lastBatchGetKeys())
	}
	if got := kv.getCallCount(stateKey); got != 1 {
		t.Fatalf("volume state backend read calls=%d want 1", got)
	}
	for i, req := range reqs {
		recordKey := idempotencyKey("phase-g-test", req.VolumeID, req.IdempotencyKey)
		if keyCounts[recordKey] != 1 {
			t.Fatalf("idempotency key %s batch count=%d want 1 keys=%v", req.IdempotencyKey, keyCounts[recordKey], kv.lastBatchGetKeys())
		}
		if got := kv.getCallCount(recordKey); got != 1 {
			t.Fatalf("idempotency %s backend read calls=%d want 1", req.IdempotencyKey, got)
		}
		if records[i].ResultState != IdempotencyCommitted || records[i].Revision == 0 {
			t.Fatalf("record[%d]=%+v want committed append-only revision", i, records[i])
		}
		if i > 0 && records[i].Revision <= records[i-1].Revision {
			t.Fatalf("records not monotonic: %+v", records)
		}
	}
}

func TestRepositoryCommitAppendOnlyWriteMetadataBatchDuplicateIdempotencyUsesSequentialSemantics(t *testing.T) {
	ctx := context.Background()
	kv := newFakeTransactionalKV()
	repo := NewRepository(kv, "phase-g-test")
	if err := repo.PutVolumeState(ctx, VolumeState{
		VolumeID: "00a1b2c3",
		Epoch:    5,
		Revision: 11,
		Status:   VolumeStatusHealthy,
	}); err != nil {
		t.Fatalf("PutVolumeState: %v", err)
	}
	if err := repo.PutIdempotencyRecord(ctx, IdempotencyRecord{
		IdempotencyKey: "idem-duplicate",
		VolumeID:       "00a1b2c3",
		Epoch:          5,
		Revision:       11,
		Operation:      "write",
		ResultState:    IdempotencyPending,
	}); err != nil {
		t.Fatalf("PutIdempotencyRecord: %v", err)
	}

	reqs := []CommitWriteMetadataRequest{
		{
			VolumeID:                 "00a1b2c3",
			ExpectedEpoch:            5,
			ExpectedRevision:         11,
			IdempotencyKey:           "idem-duplicate",
			ExpectedIdempotencyState: IdempotencyPending,
			CommittedRevision:        12,
		},
		{
			VolumeID:                 "00a1b2c3",
			ExpectedEpoch:            5,
			ExpectedRevision:         11,
			IdempotencyKey:           "idem-duplicate",
			ExpectedIdempotencyState: IdempotencyPending,
			CommittedRevision:        13,
		},
	}

	kv.resetGetCalls()
	kv.resetSetCalls()
	_, _, err := repo.CommitAppendOnlyWriteMetadataBatch(ctx, reqs)
	if !errors.Is(err, ErrCASConflict) {
		t.Fatalf("CommitAppendOnlyWriteMetadataBatch err=%v want ErrCASConflict", err)
	}
	if got := kv.batchGetCallCount(); got != 1 {
		t.Fatalf("backend BatchGet calls=%d want 1; duplicate idempotency should use cached sequential reads", got)
	}
	record, err := repo.GetIdempotencyRecord(ctx, "00a1b2c3", "idem-duplicate")
	if err != nil {
		t.Fatalf("GetIdempotencyRecord: %v", err)
	}
	if record.ResultState != IdempotencyPending || record.Revision != 11 {
		t.Fatalf("idempotency changed despite failed transaction: %+v", record)
	}
	if got := kv.setCallCount(idempotencyKey("phase-g-test", "00a1b2c3", "idem-duplicate")); got != 0 {
		t.Fatalf("idempotency backend set calls=%d want 0 when duplicate transaction aborts", got)
	}
}

func TestRepositoryCommitAppendOnlyWriteMetadataBatchUsesMutationSnapshot(t *testing.T) {
	ctx := context.Background()
	kv := newFakeTransactionalKV()
	repo := NewRepository(kv, "phase-g-test")
	repo.rememberNativeAllocationVolume("00a1b2c3")
	if err := repo.PutVolumeState(ctx, VolumeState{
		VolumeID: "00a1b2c3",
		Epoch:    5,
		Revision: 11,
		Status:   VolumeStatusHealthy,
	}); err != nil {
		t.Fatalf("PutVolumeState: %v", err)
	}
	record := IdempotencyRecord{
		IdempotencyKey: "idem-append-snapshot",
		VolumeID:       "00a1b2c3",
		AttachmentID:   "att-1",
		Generation:     9,
		Epoch:          5,
		Revision:       11,
		Operation:      "write",
		ResultState:    IdempotencyPending,
	}
	if err := repo.PutIdempotencyRecord(ctx, record); err != nil {
		t.Fatalf("PutIdempotencyRecord: %v", err)
	}
	operation := MutationOperationRecord{
		OperationID:        "write-append-snapshot",
		VolumeID:           "00a1b2c3",
		Kind:               "write",
		State:              MutationOperationRunning,
		AllocationRevision: 11,
		WriterFencingEpoch: 5,
		IdempotencyKey:     record.IdempotencyKey,
		StartedAtUnix:      100,
		LastUpdatedAtUnix:  100,
	}
	if err := repo.PutMutationOperation(ctx, operation); err != nil {
		t.Fatalf("PutMutationOperation: %v", err)
	}
	req := CommitWriteMetadataRequest{
		VolumeID:                 "00a1b2c3",
		ExpectedEpoch:            5,
		ExpectedRevision:         11,
		IdempotencyKey:           record.IdempotencyKey,
		ExpectedIdempotencyState: IdempotencyPending,
		CommittedRevision:        12,
		MutationOperationID:      operation.OperationID,
		ExpectedMutationState:    MutationOperationRunning,
		AffectedPageNos:          []uint64{0},
		MutationOperation:        operation,
		AllocationPages: []AllocationPageRecord{{
			VolumeID:       "00a1b2c3",
			PageNo:         0,
			PageBytes:      8,
			ChunkSizeBytes: 4,
			Extents: []AllocationExtentRecord{
				{LogicalChunkStart: 0, ChunkCount: 1, Kind: AllocationKindData, PhysicalChunkStart: 101},
			},
		}},
	}

	kv.resetGetCalls()
	states, records, err := repo.CommitAppendOnlyWriteMetadataBatch(ctx, []CommitWriteMetadataRequest{req})
	if err != nil {
		t.Fatalf("CommitAppendOnlyWriteMetadataBatch: %v", err)
	}
	if len(states) != 1 || len(records) != 1 {
		t.Fatalf("states=%d records=%d want 1 each", len(states), len(records))
	}
	if got := kv.getCallCount(mutationOperationKey("phase-g-test", req.VolumeID, req.MutationOperationID)); got != 0 {
		t.Fatalf("mutation operation get calls=%d want 0 when request snapshot is valid", got)
	}
	committed, err := repo.GetMutationOperation(ctx, req.VolumeID, req.MutationOperationID)
	if err != nil {
		t.Fatalf("GetMutationOperation: %v", err)
	}
	if committed.State != MutationOperationCommitted || committed.StartedAtUnix != operation.StartedAtUnix || committed.IdempotencyKey != operation.IdempotencyKey {
		t.Fatalf("committed operation=%+v want committed snapshot preserving identity fields", committed)
	}
}

func TestRepositoryCommitAppendOnlyWriteMetadataBatchAllowsMissingWriteIntentFromRequestSnapshot(t *testing.T) {
	ctx := context.Background()
	kv := newFakeTransactionalKV()
	repo := NewRepository(kv, "phase-g-test")
	repo.rememberNativeAllocationVolume("00a1b2c3")
	if err := repo.PutVolumeState(ctx, VolumeState{
		VolumeID: "00a1b2c3",
		Epoch:    5,
		Revision: 11,
		Status:   VolumeStatusHealthy,
	}); err != nil {
		t.Fatalf("PutVolumeState: %v", err)
	}
	operation := MutationOperationRecord{
		OperationID:        "write-intentless",
		VolumeID:           "00a1b2c3",
		Kind:               "write",
		State:              MutationOperationRunning,
		AllocationRevision: 11,
		WriterFencingEpoch: 5,
		IdempotencyKey:     "idem-intentless",
		StartedAtUnix:      100,
		LastUpdatedAtUnix:  100,
	}
	req := CommitWriteMetadataRequest{
		VolumeID:                 "00a1b2c3",
		ExpectedEpoch:            5,
		ExpectedRevision:         11,
		IdempotencyKey:           "idem-intentless",
		ExpectedIdempotencyState: IdempotencyPending,
		CommittedRevision:        12,
		AttachmentID:             "att-1",
		Generation:               9,
		AllowMissingWriteIntent:  true,
		MutationOperationID:      operation.OperationID,
		ExpectedMutationState:    MutationOperationRunning,
		AffectedPageNos:          []uint64{0},
		MutationOperation:        operation,
		AllocationPages: []AllocationPageRecord{{
			VolumeID:       "00a1b2c3",
			PageNo:         0,
			PageBytes:      8,
			ChunkSizeBytes: 4,
			Extents: []AllocationExtentRecord{
				{LogicalChunkStart: 0, ChunkCount: 1, Kind: AllocationKindData, PhysicalChunkStart: 101},
			},
		}},
	}

	states, records, err := repo.CommitAppendOnlyWriteMetadataBatch(ctx, []CommitWriteMetadataRequest{req})
	if err != nil {
		t.Fatalf("CommitAppendOnlyWriteMetadataBatch: %v", err)
	}
	if len(states) != 1 || len(records) != 1 {
		t.Fatalf("states=%d records=%d want 1 each", len(states), len(records))
	}
	record, err := repo.GetIdempotencyRecord(ctx, req.VolumeID, req.IdempotencyKey)
	if err != nil {
		t.Fatalf("GetIdempotencyRecord: %v", err)
	}
	if record.ResultState != IdempotencyCommitted || record.AttachmentID != "att-1" || record.Generation != 9 || record.Revision != records[0].Revision {
		t.Fatalf("record=%+v want committed synthesized idempotency revision=%d", record, records[0].Revision)
	}
	committed, err := repo.GetMutationOperation(ctx, req.VolumeID, req.MutationOperationID)
	if err != nil {
		t.Fatalf("GetMutationOperation: %v", err)
	}
	if committed.State != MutationOperationCommitted || committed.IdempotencyKey != req.IdempotencyKey || committed.AllocationRevision != records[0].Revision {
		t.Fatalf("committed operation=%+v want committed synthesized operation revision=%d", committed, records[0].Revision)
	}
}

func TestRepositoryAppendOnlyWriteStateUsesBatchGetForFencingReads(t *testing.T) {
	ctx := context.Background()
	repo := NewRepository(newFakeKV(), "phase-g-test")
	store := newCountingBatchReadWriter()
	if err := putJSONStore(ctx, store, volumeStateKey(repo.root, "00a1b2c3"), VolumeState{
		VolumeID: "00a1b2c3",
		Epoch:    5,
		Revision: 11,
		Status:   VolumeStatusHealthy,
	}); err != nil {
		t.Fatalf("put volume state: %v", err)
	}
	if err := putJSONStore(ctx, store, idempotencyKey(repo.root, "00a1b2c3", "idem-append-batch-read"), IdempotencyRecord{
		IdempotencyKey: "idem-append-batch-read",
		VolumeID:       "00a1b2c3",
		Epoch:          5,
		Revision:       11,
		Operation:      "write",
		ResultState:    IdempotencyPending,
	}); err != nil {
		t.Fatalf("put idempotency record: %v", err)
	}

	var timings appendOnlyWriteStateTimings
	state, record, err := repo.readAndValidateAppendOnlyWriteCommitState(ctx, store, CommitWriteStateRequest{
		VolumeID:                 "00a1b2c3",
		ExpectedEpoch:            5,
		IdempotencyKey:           "idem-append-batch-read",
		ExpectedIdempotencyState: IdempotencyPending,
	}, &timings)
	if err != nil {
		t.Fatalf("readAndValidateAppendOnlyWriteCommitState: %v", err)
	}
	if store.batchGetCalls != 1 || store.getCalls != 0 {
		t.Fatalf("batchGetCalls=%d getCalls=%d, want batch=1 get=0", store.batchGetCalls, store.getCalls)
	}
	if timings.batchRead < 0 || timings.volumeStateRead != 0 || timings.idempotencyRead != 0 {
		t.Fatalf("unexpected timings: %+v", timings)
	}
	if state.Epoch != 5 || record.ResultState != IdempotencyPending {
		t.Fatalf("state=%+v record=%+v", state, record)
	}

	_, _, err = repo.readAndValidateAppendOnlyWriteCommitState(ctx, store, CommitWriteStateRequest{
		VolumeID:                 "00a1b2c3",
		ExpectedEpoch:            6,
		IdempotencyKey:           "idem-append-batch-read",
		ExpectedIdempotencyState: IdempotencyPending,
	}, &timings)
	if !errors.Is(err, ErrCASConflict) {
		t.Fatalf("stale epoch err=%v, want ErrCASConflict", err)
	}
}

func TestRepositoryCommitPageScopedWriteMetadataConflictsOnStalePage(t *testing.T) {
	ctx := context.Background()
	repo := NewRepository(newFakeKV(), "phase-g-test")
	if err := repo.PutVolumeState(ctx, VolumeState{
		VolumeID: "00a1b2c3",
		Epoch:    5,
		Revision: 11,
		Status:   VolumeStatusHealthy,
	}); err != nil {
		t.Fatalf("PutVolumeState: %v", err)
	}
	if err := repo.PutAllocationPage(ctx, AllocationPageRecord{
		VolumeID:       "00a1b2c3",
		PageNo:         0,
		PageBytes:      8,
		ChunkSizeBytes: 4,
		Revision:       4,
		Extents: []AllocationExtentRecord{
			{LogicalChunkStart: 0, ChunkCount: 2, Kind: AllocationKindData, PhysicalChunkStart: 101},
		},
	}); err != nil {
		t.Fatalf("PutAllocationPage: %v", err)
	}
	if err := repo.PutIdempotencyRecord(ctx, IdempotencyRecord{
		IdempotencyKey: "idem-stale-page",
		VolumeID:       "00a1b2c3",
		Epoch:          5,
		Revision:       11,
		Operation:      "write",
		ResultState:    IdempotencyPending,
	}); err != nil {
		t.Fatalf("PutIdempotencyRecord: %v", err)
	}

	_, _, err := repo.CommitPageScopedWriteMetadata(ctx, CommitWriteMetadataRequest{
		VolumeID:                 "00a1b2c3",
		ExpectedEpoch:            5,
		ExpectedRevision:         11,
		IdempotencyKey:           "idem-stale-page",
		ExpectedIdempotencyState: IdempotencyPending,
		CommittedRevision:        12,
		AllocationPages: []AllocationPageRecord{
			{
				VolumeID:       "00a1b2c3",
				PageNo:         0,
				PageBytes:      8,
				ChunkSizeBytes: 4,
				Revision:       3,
				Extents: []AllocationExtentRecord{
					{LogicalChunkStart: 0, ChunkCount: 2, Kind: AllocationKindData, PhysicalChunkStart: 201},
				},
			},
		},
	})
	if err != ErrCASConflict {
		t.Fatalf("CommitPageScopedWriteMetadata err=%v want=%v", err, ErrCASConflict)
	}
}

func TestRepositoryApplyCommittedWriteEffectsLeavesMutationRunningWhenPlacementApplyFails(t *testing.T) {
	ctx := context.Background()
	repo := NewRepository(newFakeKV(), "phase-g-test")
	if err := repo.PutMutationOperation(ctx, MutationOperationRecord{
		OperationID:        "write-effects-fail",
		VolumeID:           "00a1b2c3",
		Kind:               "write",
		State:              MutationOperationRunning,
		AllocationRevision: 11,
		IdempotencyKey:     "idem-effects-fail",
		StartedAtUnix:      100,
		LastUpdatedAtUnix:  100,
	}); err != nil {
		t.Fatalf("PutMutationOperation: %v", err)
	}

	err := repo.ApplyCommittedWriteEffects(ctx, ApplyCommittedWriteEffectsRequest{
		VolumeID:              "00a1b2c3",
		CommittedRevision:     12,
		MutationOperationID:   "write-effects-fail",
		ExpectedMutationState: MutationOperationRunning,
		AffectedExtentIDs:     []uint64{99},
		AffectedPageNos:       []uint64{0},
		NormalizeExtentMappings: []uint64{
			99,
		},
		AllocationPages: []AllocationPageRecord{
			{
				VolumeID:       "00a1b2c3",
				PageNo:         0,
				PageBytes:      8,
				ChunkSizeBytes: 4,
				Extents: []AllocationExtentRecord{
					{LogicalChunkStart: 0, ChunkCount: 2, Kind: AllocationKindData, PhysicalChunkStart: 101},
				},
			},
		},
	})
	if err == nil {
		t.Fatalf("expected placement apply failure")
	}

	operation, err := repo.GetMutationOperation(ctx, "00a1b2c3", "write-effects-fail")
	if err != nil {
		t.Fatalf("GetMutationOperation: %v", err)
	}
	if operation.State != MutationOperationRunning {
		t.Fatalf("mutation state=%q want=%q", operation.State, MutationOperationRunning)
	}
}

func TestRepositoryApplyCommittedWriteEffectsTreatsCommittedMutationAsIdempotentReplay(t *testing.T) {
	ctx := context.Background()
	repo := NewRepository(newFakeKV(), "phase-g-test")
	if err := repo.PutMutationOperation(ctx, MutationOperationRecord{
		OperationID:             "write-effects-replay",
		VolumeID:                "00a1b2c3",
		Kind:                    "write",
		State:                   MutationOperationCommitted,
		AllocationRevision:      12,
		IdempotencyKey:          "idem-effects-replay",
		AffectedExtentIDs:       []uint64{99},
		AffectedPageNos:         []uint64{0},
		RetiredPhysicalChunkIDs: []uint64{77},
		StartedAtUnix:           100,
		LastUpdatedAtUnix:       100,
	}); err != nil {
		t.Fatalf("PutMutationOperation: %v", err)
	}

	err := repo.ApplyCommittedWriteEffects(ctx, ApplyCommittedWriteEffectsRequest{
		VolumeID:                "00a1b2c3",
		CommittedRevision:       12,
		MutationOperationID:     "write-effects-replay",
		ExpectedMutationState:   MutationOperationRunning,
		AffectedExtentIDs:       []uint64{99},
		AffectedPageNos:         []uint64{0},
		RetiredPhysicalChunkIDs: []uint64{77},
		NormalizeExtentMappings: []uint64{99},
		AllocationPages:         []AllocationPageRecord{{VolumeID: "00a1b2c3", PageNo: 0, PageBytes: 8, ChunkSizeBytes: 4}},
	})
	if err != nil {
		t.Fatalf("ApplyCommittedWriteEffects: %v", err)
	}
}

func TestRepositoryApplyCommittedWriteEffectsBatchAppliesRequestsInOrder(t *testing.T) {
	ctx := context.Background()
	kv := newFakeTransactionalKV()
	repo := NewRepository(kv, "phase-g-test")
	for _, operation := range []MutationOperationRecord{
		{
			OperationID:        "write-effects-batch-1",
			VolumeID:           "00a1b2c3",
			Kind:               "write",
			State:              MutationOperationRunning,
			AllocationRevision: 12,
			IdempotencyKey:     "idem-effects-batch-1",
			AffectedPageNos:    []uint64{0},
			StartedAtUnix:      100,
			LastUpdatedAtUnix:  100,
		},
		{
			OperationID:        "write-effects-batch-2",
			VolumeID:           "00a1b2c3",
			Kind:               "write",
			State:              MutationOperationRunning,
			AllocationRevision: 13,
			IdempotencyKey:     "idem-effects-batch-2",
			AffectedPageNos:    []uint64{1},
			StartedAtUnix:      100,
			LastUpdatedAtUnix:  100,
		},
	} {
		if err := repo.PutMutationOperation(ctx, operation); err != nil {
			t.Fatalf("PutMutationOperation %s: %v", operation.OperationID, err)
		}
	}

	err := repo.ApplyCommittedWriteEffectsBatch(ctx, []ApplyCommittedWriteEffectsRequest{
		{
			VolumeID:              "00a1b2c3",
			CommittedRevision:     12,
			MutationOperationID:   "write-effects-batch-1",
			ExpectedMutationState: MutationOperationRunning,
			AffectedPageNos:       []uint64{0},
			AllocationPages: []AllocationPageRecord{{
				VolumeID:       "00a1b2c3",
				PageNo:         0,
				PageBytes:      8,
				ChunkSizeBytes: 4,
				Revision:       12,
				Extents: []AllocationExtentRecord{
					{LogicalChunkStart: 0, ChunkCount: 2, Kind: AllocationKindData, PhysicalChunkStart: 101},
				},
			}},
		},
		{
			VolumeID:              "00a1b2c3",
			CommittedRevision:     13,
			MutationOperationID:   "write-effects-batch-2",
			ExpectedMutationState: MutationOperationRunning,
			AffectedPageNos:       []uint64{1},
			AllocationPages: []AllocationPageRecord{{
				VolumeID:       "00a1b2c3",
				PageNo:         1,
				PageBytes:      8,
				ChunkSizeBytes: 4,
				Revision:       13,
				Extents: []AllocationExtentRecord{
					{LogicalChunkStart: 2, ChunkCount: 2, Kind: AllocationKindData, PhysicalChunkStart: 201},
				},
			}},
		},
	})
	if err != nil {
		t.Fatalf("ApplyCommittedWriteEffectsBatch: %v", err)
	}
	if kv.runTxCalls == 0 {
		t.Fatalf("expected batch apply to use RunInTransaction")
	}

	for _, tc := range []struct {
		pageNo              uint64
		operationID         string
		committedRevision   uint64
		physicalChunkStart  uint64
		expectedPageExtents int
	}{
		{pageNo: 0, operationID: "write-effects-batch-1", committedRevision: 12, physicalChunkStart: 101, expectedPageExtents: 1},
		{pageNo: 1, operationID: "write-effects-batch-2", committedRevision: 13, physicalChunkStart: 201, expectedPageExtents: 1},
	} {
		page, err := repo.GetAllocationPage(ctx, "00a1b2c3", tc.pageNo)
		if err != nil {
			t.Fatalf("GetAllocationPage page=%d: %v", tc.pageNo, err)
		}
		if page.Revision != tc.committedRevision || len(page.Extents) != tc.expectedPageExtents || page.Extents[0].PhysicalChunkStart != tc.physicalChunkStart {
			t.Fatalf("page %d after batch=%+v", tc.pageNo, page)
		}
		operation, err := repo.GetMutationOperation(ctx, "00a1b2c3", tc.operationID)
		if err != nil {
			t.Fatalf("GetMutationOperation %s: %v", tc.operationID, err)
		}
		if operation.State != MutationOperationCommitted || operation.AllocationRevision != tc.committedRevision {
			t.Fatalf("operation %s after batch=%+v", tc.operationID, operation)
		}
	}
}

func TestRepositoryApplyCommittedWriteEffectsBatchSingleRequestUsesBatchEnvelope(t *testing.T) {
	ctx := context.Background()
	kv := newFakeTransactionalKV()
	repo := NewRepository(kv, "phase-g-test")
	operation := MutationOperationRecord{
		OperationID:        "write-effects-batch-single",
		VolumeID:           "00a1b2c3",
		Kind:               "write",
		State:              MutationOperationRunning,
		AllocationRevision: 41,
		IdempotencyKey:     "idem-effects-batch-single",
		AffectedPageNos:    []uint64{0},
		StartedAtUnix:      100,
		LastUpdatedAtUnix:  100,
	}
	if err := repo.PutMutationOperation(ctx, operation); err != nil {
		t.Fatalf("PutMutationOperation: %v", err)
	}

	var logs bytes.Buffer
	restore := structuredlog.SetOutput(&logs)
	defer restore()
	err := repo.ApplyCommittedWriteEffectsBatch(ctx, []ApplyCommittedWriteEffectsRequest{
		{
			VolumeID:              "00a1b2c3",
			CommittedRevision:     42,
			MutationOperationID:   operation.OperationID,
			ExpectedMutationState: MutationOperationRunning,
			AffectedPageNos:       []uint64{0},
			AllocationPages: []AllocationPageRecord{{
				VolumeID:       "00a1b2c3",
				PageNo:         0,
				PageBytes:      8,
				ChunkSizeBytes: 4,
				Revision:       42,
				Extents: []AllocationExtentRecord{
					{LogicalChunkStart: 0, ChunkCount: 1, Kind: AllocationKindData, PhysicalChunkStart: 901},
				},
			}},
		},
	})
	if err != nil {
		t.Fatalf("ApplyCommittedWriteEffectsBatch: %v", err)
	}
	if !bytes.Contains(logs.Bytes(), []byte(`"event":"write_session_effects_apply_batch"`)) {
		t.Fatalf("expected batch apply log, got %s", logs.String())
	}
	if bytes.Contains(logs.Bytes(), []byte(`"event":"write_session_effects_apply_phases"`)) {
		t.Fatalf("single-request batch apply used single-request log: %s", logs.String())
	}

	page, err := repo.GetAllocationPage(ctx, "00a1b2c3", 0)
	if err != nil {
		t.Fatalf("GetAllocationPage: %v", err)
	}
	if page.Revision != 42 || len(page.Extents) == 0 || page.Extents[0].PhysicalChunkStart != 901 {
		t.Fatalf("page after single-request batch=%+v", page)
	}
	got, err := repo.GetMutationOperation(ctx, "00a1b2c3", operation.OperationID)
	if err != nil {
		t.Fatalf("GetMutationOperation: %v", err)
	}
	if got.State != MutationOperationCommitted || got.AllocationRevision != 42 {
		t.Fatalf("operation after single-request batch=%+v", got)
	}
}

func TestRepositoryApplyCommittedWriteEffectsBatchReadsMutationOperationOncePerRequest(t *testing.T) {
	ctx := context.Background()
	kv := newFakeTransactionalKV()
	repo := NewRepository(kv, "phase-g-test")
	reqs := []ApplyCommittedWriteEffectsRequest{
		{
			VolumeID:              "00a1b2c3",
			CommittedRevision:     20,
			MutationOperationID:   "write-effects-batch-read-1",
			ExpectedMutationState: MutationOperationRunning,
			AffectedPageNos:       []uint64{0},
			AllocationPages: []AllocationPageRecord{{
				VolumeID:       "00a1b2c3",
				PageNo:         0,
				PageBytes:      8,
				ChunkSizeBytes: 4,
				Revision:       20,
				Extents: []AllocationExtentRecord{
					{LogicalChunkStart: 0, ChunkCount: 1, Kind: AllocationKindData, PhysicalChunkStart: 301},
				},
			}},
		},
		{
			VolumeID:              "00a1b2c3",
			CommittedRevision:     21,
			MutationOperationID:   "write-effects-batch-read-2",
			ExpectedMutationState: MutationOperationRunning,
			AffectedPageNos:       []uint64{1},
			AllocationPages: []AllocationPageRecord{{
				VolumeID:       "00a1b2c3",
				PageNo:         1,
				PageBytes:      8,
				ChunkSizeBytes: 4,
				Revision:       21,
				Extents: []AllocationExtentRecord{
					{LogicalChunkStart: 2, ChunkCount: 1, Kind: AllocationKindData, PhysicalChunkStart: 401},
				},
			}},
		},
	}
	for _, req := range reqs {
		if err := repo.PutMutationOperation(ctx, MutationOperationRecord{
			OperationID:        req.MutationOperationID,
			VolumeID:           req.VolumeID,
			Kind:               "write",
			State:              MutationOperationRunning,
			AllocationRevision: req.CommittedRevision - 1,
			IdempotencyKey:     req.MutationOperationID + "-idem",
			AffectedPageNos:    append([]uint64(nil), req.AffectedPageNos...),
			StartedAtUnix:      100,
			LastUpdatedAtUnix:  100,
		}); err != nil {
			t.Fatalf("PutMutationOperation %s: %v", req.MutationOperationID, err)
		}
	}

	kv.resetGetCalls()
	if err := repo.ApplyCommittedWriteEffectsBatch(ctx, reqs); err != nil {
		t.Fatalf("ApplyCommittedWriteEffectsBatch: %v", err)
	}
	for _, req := range reqs {
		key := mutationOperationKey("phase-g-test", req.VolumeID, req.MutationOperationID)
		if got := kv.getCallCount(key); got != 1 {
			t.Fatalf("mutation operation %s get calls=%d, want 1", req.MutationOperationID, got)
		}
	}
}

func TestRepositoryApplyCommittedWriteEffectsBatchCachesRepeatedPageAndExtentReads(t *testing.T) {
	ctx := context.Background()
	kv := newFakeTransactionalKV()
	repo := NewRepository(kv, "phase-g-test")
	repo.rememberNativeAllocationVolume("00a1b2c3")
	if err := repo.PutExtentMapping(ctx, ExtentMappingRecord{
		VolumeID:      "00a1b2c3",
		ExtentID:      99,
		LogicalOffset: 0,
		LengthBytes:   8,
		ChunkID:       123,
		PlacementRef:  "pl-99",
		Revision:      7,
	}); err != nil {
		t.Fatalf("PutExtentMapping: %v", err)
	}
	reqs := []ApplyCommittedWriteEffectsRequest{
		{
			VolumeID:                "00a1b2c3",
			CommittedRevision:       30,
			MutationOperationID:     "write-effects-batch-cache-1",
			ExpectedMutationState:   MutationOperationRunning,
			AffectedExtentIDs:       []uint64{99},
			AffectedPageNos:         []uint64{0},
			NormalizeExtentMappings: []uint64{99},
			AllocationPages: []AllocationPageRecord{{
				VolumeID:       "00a1b2c3",
				PageNo:         0,
				PageBytes:      8,
				ChunkSizeBytes: 4,
				Revision:       30,
				Extents: []AllocationExtentRecord{
					{LogicalChunkStart: 0, ChunkCount: 1, Kind: AllocationKindData, PhysicalChunkStart: 501},
				},
			}},
		},
		{
			VolumeID:                "00a1b2c3",
			CommittedRevision:       31,
			MutationOperationID:     "write-effects-batch-cache-2",
			ExpectedMutationState:   MutationOperationRunning,
			AffectedExtentIDs:       []uint64{99},
			AffectedPageNos:         []uint64{0},
			NormalizeExtentMappings: []uint64{99},
			AllocationPages: []AllocationPageRecord{{
				VolumeID:       "00a1b2c3",
				PageNo:         0,
				PageBytes:      8,
				ChunkSizeBytes: 4,
				Revision:       31,
				Extents: []AllocationExtentRecord{
					{LogicalChunkStart: 1, ChunkCount: 1, Kind: AllocationKindData, PhysicalChunkStart: 601},
				},
			}},
		},
	}
	for _, req := range reqs {
		if err := repo.PutMutationOperation(ctx, MutationOperationRecord{
			OperationID:        req.MutationOperationID,
			VolumeID:           req.VolumeID,
			Kind:               "write",
			State:              MutationOperationRunning,
			AllocationRevision: req.CommittedRevision - 1,
			IdempotencyKey:     req.MutationOperationID + "-idem",
			AffectedPageNos:    append([]uint64(nil), req.AffectedPageNos...),
			StartedAtUnix:      100,
			LastUpdatedAtUnix:  100,
		}); err != nil {
			t.Fatalf("PutMutationOperation %s: %v", req.MutationOperationID, err)
		}
	}

	kv.resetGetCalls()
	kv.resetSetCalls()
	if err := repo.ApplyCommittedWriteEffectsBatch(ctx, reqs); err != nil {
		t.Fatalf("ApplyCommittedWriteEffectsBatch: %v", err)
	}
	pageKey := allocationPageKey("phase-g-test", "00a1b2c3", 0)
	if got := kv.getCallCount(pageKey); got != 1 {
		t.Fatalf("allocation page get calls=%d, want 1", got)
	}
	if got := kv.setCallCount(pageKey); got != 1 {
		t.Fatalf("allocation page set calls=%d, want 1", got)
	}
	extentKey := extentMappingKey("phase-g-test", "00a1b2c3", 99)
	if got := kv.getCallCount(extentKey); got != 1 {
		t.Fatalf("extent mapping get calls=%d, want 1", got)
	}
	if got := kv.setCallCount(extentKey); got != 1 {
		t.Fatalf("extent mapping set calls=%d, want 1", got)
	}
	page, err := repo.GetAllocationPage(ctx, "00a1b2c3", 0)
	if err != nil {
		t.Fatalf("GetAllocationPage: %v", err)
	}
	chunks, err := expandAllocationChunkMappings(page)
	if err != nil {
		t.Fatalf("expandAllocationChunkMappings: %v", err)
	}
	if want := []uint64{501, 601}; !slices.Equal(chunks, want) {
		t.Fatalf("allocation chunks=%v, want %v", chunks, want)
	}
	if page.Revision != 31 {
		t.Fatalf("page revision=%d, want 31", page.Revision)
	}
	mapping, err := repo.GetExtentMapping(ctx, "00a1b2c3", 99)
	if err != nil {
		t.Fatalf("GetExtentMapping: %v", err)
	}
	if mapping.ChunkID != 0 || mapping.Revision != 31 {
		t.Fatalf("mapping chunk_id=%d revision=%d, want chunk_id=0 revision=31", mapping.ChunkID, mapping.Revision)
	}
}

func TestRepositoryApplyCommittedWriteEffectsBatchUsesAffectedPageChunkRangesWithoutExtentRead(t *testing.T) {
	ctx := context.Background()
	kv := newFakeTransactionalKV()
	repo := NewRepository(kv, "phase-g-test")
	repo.rememberNativeAllocationVolume("00a1b2c3")
	if err := repo.PutAllocationPage(ctx, AllocationPageRecord{
		VolumeID:       "00a1b2c3",
		PageNo:         0,
		PageBytes:      8,
		ChunkSizeBytes: 4,
		Revision:       20,
		Extents: []AllocationExtentRecord{
			{LogicalChunkStart: 0, ChunkCount: 1, Kind: AllocationKindData, PhysicalChunkStart: 100},
			{LogicalChunkStart: 1, ChunkCount: 1, Kind: AllocationKindData, PhysicalChunkStart: 200},
		},
	}); err != nil {
		t.Fatalf("PutAllocationPage: %v", err)
	}
	req := ApplyCommittedWriteEffectsRequest{
		VolumeID:                "00a1b2c3",
		CommittedRevision:       30,
		MutationOperationID:     "write-effects-range",
		ExpectedMutationState:   MutationOperationRunning,
		AffectedExtentIDs:       []uint64{99},
		AffectedPageNos:         []uint64{0},
		AffectedPageChunkRanges: []AllocationPageChunkRangeRecord{{PageNo: 0, StartChunk: 0, EndChunk: 1}},
		MutationOperation: MutationOperationRecord{
			OperationID:        "write-effects-range",
			VolumeID:           "00a1b2c3",
			Kind:               "write",
			State:              MutationOperationRunning,
			AllocationRevision: 20,
			IdempotencyKey:     "idem-effects-range",
			AffectedExtentIDs:  []uint64{99},
			AffectedPageNos:    []uint64{0},
			StartedAtUnix:      100,
			LastUpdatedAtUnix:  100,
		},
		AllocationPages: []AllocationPageRecord{{
			VolumeID:       "00a1b2c3",
			PageNo:         0,
			PageBytes:      8,
			ChunkSizeBytes: 4,
			Revision:       30,
			Extents: []AllocationExtentRecord{
				{LogicalChunkStart: 0, ChunkCount: 1, Kind: AllocationKindData, PhysicalChunkStart: 501},
				{LogicalChunkStart: 1, ChunkCount: 1, Kind: AllocationKindData, PhysicalChunkStart: 999},
			},
		}},
	}

	kv.resetGetCalls()
	if err := repo.ApplyCommittedWriteEffectsBatch(ctx, []ApplyCommittedWriteEffectsRequest{req}); err != nil {
		t.Fatalf("ApplyCommittedWriteEffectsBatch: %v", err)
	}
	if got := kv.getCallCount(extentMappingKey("phase-g-test", "00a1b2c3", 99)); got != 0 {
		t.Fatalf("extent mapping get calls=%d, want 0 when affected page chunk ranges are supplied", got)
	}
	page, err := repo.GetAllocationPage(ctx, "00a1b2c3", 0)
	if err != nil {
		t.Fatalf("GetAllocationPage: %v", err)
	}
	chunks, err := expandAllocationChunkMappings(page)
	if err != nil {
		t.Fatalf("expandAllocationChunkMappings: %v", err)
	}
	if want := []uint64{501, 200}; !slices.Equal(chunks, want) {
		t.Fatalf("allocation chunks=%v, want %v", chunks, want)
	}
}

func TestRepositoryApplyCommittedWriteEffectsBatchAsyncMutationFinalize(t *testing.T) {
	ctx := context.Background()
	kv := newFakeTransactionalKV()
	repo := NewRepository(kv, "phase-g-test")
	repo.SetAsyncWriteMutationFinalize(true)

	operation := MutationOperationRecord{
		OperationID:        "write-effects-batch-async-finalize",
		VolumeID:           "00a1b2c3",
		Kind:               "write",
		State:              MutationOperationRunning,
		AllocationRevision: 40,
		IdempotencyKey:     "idem-effects-batch-async-finalize",
		AffectedPageNos:    []uint64{0},
		StartedAtUnix:      100,
		LastUpdatedAtUnix:  100,
	}
	if err := repo.PutMutationOperation(ctx, operation); err != nil {
		t.Fatalf("PutMutationOperation: %v", err)
	}
	req := ApplyCommittedWriteEffectsRequest{
		VolumeID:              "00a1b2c3",
		CommittedRevision:     41,
		MutationOperationID:   operation.OperationID,
		ExpectedMutationState: MutationOperationRunning,
		AffectedPageNos:       []uint64{0},
		AllocationPages: []AllocationPageRecord{{
			VolumeID:       "00a1b2c3",
			PageNo:         0,
			PageBytes:      8,
			ChunkSizeBytes: 4,
			Revision:       41,
			Extents: []AllocationExtentRecord{
				{LogicalChunkStart: 0, ChunkCount: 1, Kind: AllocationKindData, PhysicalChunkStart: 901},
			},
		}},
	}

	if err := repo.ApplyCommittedWriteEffectsBatch(ctx, []ApplyCommittedWriteEffectsRequest{req}); err != nil {
		t.Fatalf("ApplyCommittedWriteEffectsBatch: %v", err)
	}
	page, err := repo.GetAllocationPage(ctx, "00a1b2c3", 0)
	if err != nil {
		t.Fatalf("GetAllocationPage: %v", err)
	}
	chunks, err := expandAllocationChunkMappings(page)
	if err != nil {
		t.Fatalf("expandAllocationChunkMappings: %v", err)
	}
	if page.Revision != 41 || !slices.Equal(chunks, []uint64{901, 0}) {
		t.Fatalf("page revision=%d chunks=%v, want revision=41 chunks=[901 0]", page.Revision, chunks)
	}

	deadline := time.Now().Add(time.Second)
	for {
		got, err := repo.GetMutationOperation(ctx, "00a1b2c3", operation.OperationID)
		if err != nil {
			t.Fatalf("GetMutationOperation: %v", err)
		}
		if got.State == MutationOperationCommitted && got.AllocationRevision == 41 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("operation was not finalized asynchronously: %+v", got)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestRepositoryApplyCommittedWriteEffectsBatchDuplicateMutationIDUsesSequentialSemantics(t *testing.T) {
	ctx := context.Background()
	kv := newFakeTransactionalKV()
	repo := NewRepository(kv, "phase-g-test")
	operation := MutationOperationRecord{
		OperationID:        "write-effects-batch-dup",
		VolumeID:           "00a1b2c3",
		Kind:               "write",
		State:              MutationOperationRunning,
		AllocationRevision: 29,
		IdempotencyKey:     "idem-effects-batch-dup",
		AffectedPageNos:    []uint64{0},
		StartedAtUnix:      100,
		LastUpdatedAtUnix:  100,
	}
	if err := repo.PutMutationOperation(ctx, operation); err != nil {
		t.Fatalf("PutMutationOperation: %v", err)
	}
	reqs := []ApplyCommittedWriteEffectsRequest{
		{
			VolumeID:              "00a1b2c3",
			CommittedRevision:     30,
			MutationOperationID:   operation.OperationID,
			ExpectedMutationState: MutationOperationRunning,
			AffectedPageNos:       []uint64{0},
			AllocationPages: []AllocationPageRecord{{
				VolumeID:       "00a1b2c3",
				PageNo:         0,
				PageBytes:      8,
				ChunkSizeBytes: 4,
				Revision:       30,
				Extents: []AllocationExtentRecord{
					{LogicalChunkStart: 0, ChunkCount: 1, Kind: AllocationKindData, PhysicalChunkStart: 701},
				},
			}},
		},
		{
			VolumeID:              "00a1b2c3",
			CommittedRevision:     31,
			MutationOperationID:   operation.OperationID,
			ExpectedMutationState: MutationOperationRunning,
			AffectedPageNos:       []uint64{0},
			AllocationPages: []AllocationPageRecord{{
				VolumeID:       "00a1b2c3",
				PageNo:         0,
				PageBytes:      8,
				ChunkSizeBytes: 4,
				Revision:       31,
				Extents: []AllocationExtentRecord{
					{LogicalChunkStart: 1, ChunkCount: 1, Kind: AllocationKindData, PhysicalChunkStart: 801},
				},
			}},
		},
	}
	err := repo.ApplyCommittedWriteEffectsBatch(ctx, reqs)
	if !errors.Is(err, ErrCASConflict) {
		t.Fatalf("ApplyCommittedWriteEffectsBatch err=%v, want ErrCASConflict", err)
	}
	got, err := repo.GetMutationOperation(ctx, operation.VolumeID, operation.OperationID)
	if err != nil {
		t.Fatalf("GetMutationOperation: %v", err)
	}
	if got.State != MutationOperationRunning || got.AllocationRevision != 29 {
		t.Fatalf("operation state=%q revision=%d, want running revision 29", got.State, got.AllocationRevision)
	}
	if _, err := repo.GetAllocationPage(ctx, operation.VolumeID, 0); !errors.Is(err, ErrNotFound) {
		t.Fatalf("GetAllocationPage err=%v, want ErrNotFound", err)
	}
}

func TestRepositoryNativeAllocationFastPathCanSkipNormalizedRevisionOnlyWrite(t *testing.T) {
	ctx := context.Background()
	repo := NewRepository(newFakeKV(), "phase-g-test")
	repo.SetNativeAllocationFastPath(true)
	if err := repo.PutAllocationPage(ctx, AllocationPageRecord{
		VolumeID:       "00a1b2c3",
		PageNo:         0,
		PageBytes:      4096,
		ChunkSizeBytes: 4096,
		Revision:       1,
		Extents: []AllocationExtentRecord{
			{LogicalChunkStart: 0, ChunkCount: 1, Kind: AllocationKindZero},
		},
	}); err != nil {
		t.Fatalf("PutAllocationPage: %v", err)
	}
	if err := repo.PutExtentMapping(ctx, ExtentMappingRecord{
		VolumeID:      "00a1b2c3",
		ExtentID:      99,
		LogicalOffset: 0,
		LengthBytes:   4096,
		ChunkID:       0,
		PlacementRef:  "pl-1",
		Revision:      7,
	}); err != nil {
		t.Fatalf("PutExtentMapping: %v", err)
	}
	stats, err := repo.normalizeCommittedExtentMappings(ctx, repo.kv, ApplyCommittedWriteEffectsRequest{
		VolumeID:                "00a1b2c3",
		CommittedRevision:       12,
		NormalizeExtentMappings: []uint64{99},
	})
	if err != nil {
		t.Fatalf("normalizeCommittedExtentMappings: %v", err)
	}
	if stats.requested != 1 || stats.read != 1 || stats.skipped != 1 || stats.written != 0 || stats.alreadyNormalized != 1 {
		t.Fatalf("unexpected stats: %+v", stats)
	}
	got, err := repo.GetExtentMapping(ctx, "00a1b2c3", 99)
	if err != nil {
		t.Fatalf("GetExtentMapping: %v", err)
	}
	if got.Revision != 7 {
		t.Fatalf("revision=%d want preserved 7", got.Revision)
	}
}

func TestRepositoryNormalizeCommittedExtentMappingsDoesNotSkipWithoutNativeAllocationObserved(t *testing.T) {
	ctx := context.Background()
	repo := NewRepository(newFakeKV(), "phase-g-test")
	repo.SetNativeAllocationFastPath(true)
	if err := repo.PutExtentMapping(ctx, ExtentMappingRecord{
		VolumeID:      "00a1b2c3",
		ExtentID:      99,
		LogicalOffset: 0,
		LengthBytes:   4096,
		ChunkID:       0,
		PlacementRef:  "pl-1",
		Revision:      7,
	}); err != nil {
		t.Fatalf("PutExtentMapping: %v", err)
	}
	stats, err := repo.normalizeCommittedExtentMappings(ctx, repo.kv, ApplyCommittedWriteEffectsRequest{
		VolumeID:                "00a1b2c3",
		CommittedRevision:       12,
		NormalizeExtentMappings: []uint64{99},
	})
	if err != nil {
		t.Fatalf("normalizeCommittedExtentMappings: %v", err)
	}
	if stats.requested != 1 || stats.read != 1 || stats.skipped != 0 || stats.written != 1 || stats.alreadyNormalized != 1 || stats.revisionAdvanced != 1 {
		t.Fatalf("unexpected stats: %+v", stats)
	}
	got, err := repo.GetExtentMapping(ctx, "00a1b2c3", 99)
	if err != nil {
		t.Fatalf("GetExtentMapping: %v", err)
	}
	if got.Revision != 12 {
		t.Fatalf("revision=%d want advanced 12", got.Revision)
	}
}

func TestRepositoryNormalizeCommittedExtentMappingsStillNormalizesLegacyMappingWithFastPathEnabled(t *testing.T) {
	ctx := context.Background()
	repo := NewRepository(newFakeKV(), "phase-g-test")
	repo.SetNativeAllocationFastPath(true)
	if err := repo.PutExtentMapping(ctx, ExtentMappingRecord{
		VolumeID:      "00a1b2c3",
		ExtentID:      99,
		LogicalOffset: 0,
		LengthBytes:   4096,
		ChunkID:       1234,
		PlacementRef:  "pl-1",
		Revision:      7,
	}); err != nil {
		t.Fatalf("PutExtentMapping: %v", err)
	}

	stats, err := repo.normalizeCommittedExtentMappings(ctx, repo.kv, ApplyCommittedWriteEffectsRequest{
		VolumeID:                "00a1b2c3",
		CommittedRevision:       12,
		NormalizeExtentMappings: []uint64{99},
	})
	if err != nil {
		t.Fatalf("normalizeCommittedExtentMappings: %v", err)
	}
	if stats.requested != 1 || stats.read != 1 || stats.skipped != 0 || stats.written != 1 || stats.revisionAdvanced != 1 {
		t.Fatalf("unexpected stats: %+v", stats)
	}
	got, err := repo.GetExtentMapping(ctx, "00a1b2c3", 99)
	if err != nil {
		t.Fatalf("GetExtentMapping: %v", err)
	}
	if got.ChunkID != 0 || got.Revision != 12 {
		t.Fatalf("mapping chunk_id=%d revision=%d want chunk_id=0 revision=12", got.ChunkID, got.Revision)
	}
}

func TestRepositoryApplyCommittedWriteEffectsNativeAllocationFastPathKeepsReadAfterWriteVisibleAcrossRepositoryRestart(t *testing.T) {
	ctx := context.Background()
	kv := newFakeKV()
	repo := NewRepository(kv, "phase-g-test")
	repo.SetNativeAllocationFastPath(true)
	if err := repo.PutAllocationPage(ctx, AllocationPageRecord{
		VolumeID:       "00a1b2c3",
		PageNo:         0,
		PageBytes:      8,
		ChunkSizeBytes: 4,
		Revision:       1,
		Extents: []AllocationExtentRecord{
			{LogicalChunkStart: 0, ChunkCount: 2, Kind: AllocationKindZero},
		},
	}); err != nil {
		t.Fatalf("PutAllocationPage: %v", err)
	}
	if err := repo.PutExtentMapping(ctx, ExtentMappingRecord{
		VolumeID:      "00a1b2c3",
		ExtentID:      99,
		LogicalOffset: 0,
		LengthBytes:   8,
		ChunkID:       0,
		PlacementRef:  "pl-1",
		Revision:      7,
	}); err != nil {
		t.Fatalf("PutExtentMapping: %v", err)
	}
	if err := repo.PutMutationOperation(ctx, MutationOperationRecord{
		OperationID: "write-99",
		VolumeID:    "00a1b2c3",
		Kind:        "write",
		State:       MutationOperationRunning,
	}); err != nil {
		t.Fatalf("PutMutationOperation: %v", err)
	}

	if err := repo.ApplyCommittedWriteEffects(ctx, ApplyCommittedWriteEffectsRequest{
		VolumeID:                "00a1b2c3",
		CommittedRevision:       12,
		MutationOperationID:     "write-99",
		ExpectedMutationState:   MutationOperationRunning,
		AffectedExtentIDs:       []uint64{99},
		AffectedPageNos:         []uint64{0},
		NormalizeExtentMappings: []uint64{99},
		AllocationPages: []AllocationPageRecord{{
			VolumeID:       "00a1b2c3",
			PageNo:         0,
			PageBytes:      8,
			ChunkSizeBytes: 4,
			Extents: []AllocationExtentRecord{
				{LogicalChunkStart: 0, ChunkCount: 1, Kind: AllocationKindData, PhysicalChunkStart: 5001},
				{LogicalChunkStart: 1, ChunkCount: 1, Kind: AllocationKindZero},
			},
		}},
	}); err != nil {
		t.Fatalf("ApplyCommittedWriteEffects: %v", err)
	}

	restarted := NewRepository(kv, "phase-g-test")
	page, err := restarted.GetAllocationPage(ctx, "00a1b2c3", 0)
	if err != nil {
		t.Fatalf("GetAllocationPage after restart: %v", err)
	}
	chunks, err := expandAllocationChunkMappings(page)
	if err != nil {
		t.Fatalf("expandAllocationChunkMappings: %v", err)
	}
	if len(chunks) != 2 || chunks[0] != 5001 || chunks[1] != 0 {
		t.Fatalf("allocation chunks after restart=%v want [5001 0], page=%+v", chunks, page)
	}
	mapping, err := restarted.GetExtentMapping(ctx, "00a1b2c3", 99)
	if err != nil {
		t.Fatalf("GetExtentMapping after restart: %v", err)
	}
	if mapping.ChunkID != 0 || mapping.Revision != 7 {
		t.Fatalf("mapping after restart chunk_id=%d revision=%d want chunk_id=0 revision=7", mapping.ChunkID, mapping.Revision)
	}
	operation, err := restarted.GetMutationOperation(ctx, "00a1b2c3", "write-99")
	if err != nil {
		t.Fatalf("GetMutationOperation after restart: %v", err)
	}
	if operation.State != MutationOperationCommitted || operation.AllocationRevision != 12 {
		t.Fatalf("operation state=%s allocation_revision=%d want committed/12", operation.State, operation.AllocationRevision)
	}
}

func TestRepositoryApplyCommittedWriteEffectsMergesStaleSamePageWrites(t *testing.T) {
	ctx := context.Background()
	repo := NewRepository(newFakeKV(), "phase-g-test")
	if err := repo.PutAllocationPage(ctx, AllocationPageRecord{
		VolumeID:       "00a1b2c3",
		PageNo:         0,
		PageBytes:      8,
		ChunkSizeBytes: 4,
		Revision:       1,
		Extents: []AllocationExtentRecord{
			{LogicalChunkStart: 0, ChunkCount: 2, Kind: AllocationKindZero},
		},
	}); err != nil {
		t.Fatalf("PutAllocationPage: %v", err)
	}
	for _, rec := range []ExtentMappingRecord{
		{VolumeID: "00a1b2c3", ExtentID: 101, LogicalOffset: 0, LengthBytes: 4, ChunkID: 1001, PlacementRef: "pl-1", Revision: 2},
		{VolumeID: "00a1b2c3", ExtentID: 102, LogicalOffset: 4, LengthBytes: 4, ChunkID: 2001, PlacementRef: "pl-1", Revision: 2},
	} {
		if err := repo.PutExtentMapping(ctx, rec); err != nil {
			t.Fatalf("PutExtentMapping(%d): %v", rec.ExtentID, err)
		}
	}
	for _, rec := range []MutationOperationRecord{
		{OperationID: "write-101", VolumeID: "00a1b2c3", Kind: "write", State: MutationOperationRunning},
		{OperationID: "write-102", VolumeID: "00a1b2c3", Kind: "write", State: MutationOperationRunning},
	} {
		if err := repo.PutMutationOperation(ctx, rec); err != nil {
			t.Fatalf("PutMutationOperation(%s): %v", rec.OperationID, err)
		}
	}

	if err := repo.ApplyCommittedWriteEffects(ctx, ApplyCommittedWriteEffectsRequest{
		VolumeID:                "00a1b2c3",
		CommittedRevision:       10,
		MutationOperationID:     "write-101",
		ExpectedMutationState:   MutationOperationRunning,
		AffectedExtentIDs:       []uint64{101},
		AffectedPageNos:         []uint64{0},
		NormalizeExtentMappings: []uint64{101},
		AllocationPages: []AllocationPageRecord{{
			VolumeID:       "00a1b2c3",
			PageNo:         0,
			PageBytes:      8,
			ChunkSizeBytes: 4,
			Extents: []AllocationExtentRecord{
				{LogicalChunkStart: 0, ChunkCount: 1, Kind: AllocationKindData, PhysicalChunkStart: 1001},
				{LogicalChunkStart: 1, ChunkCount: 1, Kind: AllocationKindZero},
			},
		}},
	}); err != nil {
		t.Fatalf("ApplyCommittedWriteEffects first write: %v", err)
	}
	if err := repo.ApplyCommittedWriteEffects(ctx, ApplyCommittedWriteEffectsRequest{
		VolumeID:                "00a1b2c3",
		CommittedRevision:       11,
		MutationOperationID:     "write-102",
		ExpectedMutationState:   MutationOperationRunning,
		AffectedExtentIDs:       []uint64{102},
		AffectedPageNos:         []uint64{0},
		NormalizeExtentMappings: []uint64{102},
		AllocationPages: []AllocationPageRecord{{
			VolumeID:       "00a1b2c3",
			PageNo:         0,
			PageBytes:      8,
			ChunkSizeBytes: 4,
			Extents: []AllocationExtentRecord{
				{LogicalChunkStart: 0, ChunkCount: 1, Kind: AllocationKindZero},
				{LogicalChunkStart: 1, ChunkCount: 1, Kind: AllocationKindData, PhysicalChunkStart: 2001},
			},
		}},
	}); err != nil {
		t.Fatalf("ApplyCommittedWriteEffects second write: %v", err)
	}

	page, err := repo.GetAllocationPage(ctx, "00a1b2c3", 0)
	if err != nil {
		t.Fatalf("GetAllocationPage: %v", err)
	}
	chunks, err := expandAllocationChunkMappings(page)
	if err != nil {
		t.Fatalf("expandAllocationChunkMappings: %v", err)
	}
	if len(chunks) != 2 || chunks[0] != 1001 || chunks[1] != 2001 {
		t.Fatalf("allocation chunks=%v want [1001 2001], page=%+v", chunks, page)
	}
}

func TestRepositoryApplyCommittedWriteEffectsMergesZeroOverwriteOnlyTouchedChunk(t *testing.T) {
	ctx := context.Background()
	repo := NewRepository(newFakeKV(), "phase-g-test")
	if err := repo.PutAllocationPage(ctx, AllocationPageRecord{
		VolumeID:       "00a1b2c3",
		PageNo:         0,
		PageBytes:      8,
		ChunkSizeBytes: 4,
		Revision:       1,
		Extents: []AllocationExtentRecord{
			{LogicalChunkStart: 0, ChunkCount: 2, Kind: AllocationKindData, PhysicalChunkStart: 1001},
		},
	}); err != nil {
		t.Fatalf("PutAllocationPage: %v", err)
	}
	if err := repo.PutExtentMapping(ctx, ExtentMappingRecord{
		VolumeID:      "00a1b2c3",
		ExtentID:      103,
		LogicalOffset: 0,
		LengthBytes:   4,
		ChunkID:       0,
		PlacementRef:  "pl-1",
		Revision:      2,
	}); err != nil {
		t.Fatalf("PutExtentMapping: %v", err)
	}
	if err := repo.PutMutationOperation(ctx, MutationOperationRecord{
		OperationID: "write-103",
		VolumeID:    "00a1b2c3",
		Kind:        "write",
		State:       MutationOperationRunning,
	}); err != nil {
		t.Fatalf("PutMutationOperation: %v", err)
	}

	if err := repo.ApplyCommittedWriteEffects(ctx, ApplyCommittedWriteEffectsRequest{
		VolumeID:                "00a1b2c3",
		CommittedRevision:       12,
		MutationOperationID:     "write-103",
		ExpectedMutationState:   MutationOperationRunning,
		AffectedExtentIDs:       []uint64{103},
		AffectedPageNos:         []uint64{0},
		RetiredPhysicalChunkIDs: []uint64{1001},
		NormalizeExtentMappings: []uint64{103},
		AllocationPages: []AllocationPageRecord{{
			VolumeID:       "00a1b2c3",
			PageNo:         0,
			PageBytes:      8,
			ChunkSizeBytes: 4,
			Extents: []AllocationExtentRecord{
				{LogicalChunkStart: 0, ChunkCount: 1, Kind: AllocationKindZero},
				{LogicalChunkStart: 1, ChunkCount: 1, Kind: AllocationKindData, PhysicalChunkStart: 1002},
			},
		}},
	}); err != nil {
		t.Fatalf("ApplyCommittedWriteEffects: %v", err)
	}

	page, err := repo.GetAllocationPage(ctx, "00a1b2c3", 0)
	if err != nil {
		t.Fatalf("GetAllocationPage: %v", err)
	}
	chunks, err := expandAllocationChunkMappings(page)
	if err != nil {
		t.Fatalf("expandAllocationChunkMappings: %v", err)
	}
	if len(chunks) != 2 || chunks[0] != 0 || chunks[1] != 1002 {
		t.Fatalf("allocation chunks=%v want [0 1002], page=%+v", chunks, page)
	}
}

func TestApplyCommittedWriteEffectsRequestMatchesCommittedMutationOperation(t *testing.T) {
	req := ApplyCommittedWriteEffectsRequest{
		CommittedRevision:       12,
		AffectedExtentIDs:       []uint64{1, 2},
		AffectedPageNos:         []uint64{3, 4},
		RetiredPhysicalChunkIDs: []uint64{77, 88},
	}
	operation := MutationOperationRecord{
		State:                   MutationOperationCommitted,
		AllocationRevision:      12,
		AffectedExtentIDs:       []uint64{1, 2},
		AffectedPageNos:         []uint64{3, 4},
		RetiredPhysicalChunkIDs: []uint64{77, 88},
	}
	if !req.MatchesCommittedMutationOperation(operation) {
		t.Fatalf("expected matching committed mutation operation")
	}

	operation.AffectedPageNos = []uint64{4, 3}
	if req.MatchesCommittedMutationOperation(operation) {
		t.Fatalf("expected page order mismatch to fail")
	}
}

func TestRepositoryCommitWriteMetadataBootstrapsLegacyAllocationPages(t *testing.T) {
	ctx := context.Background()
	repo := NewRepository(newFakeKV(), "phase-g-test")
	if err := repo.PutVolumeState(ctx, VolumeState{
		VolumeID: "00a1b2c3",
		Epoch:    5,
		Revision: 11,
		Status:   VolumeStatusHealthy,
	}); err != nil {
		t.Fatalf("PutVolumeState: %v", err)
	}
	if err := repo.PutIdempotencyRecord(ctx, IdempotencyRecord{
		IdempotencyKey: "idem-bootstrap-1",
		VolumeID:       "00a1b2c3",
		AttachmentID:   "att-1",
		Generation:     7,
		Epoch:          5,
		Revision:       11,
		Operation:      "write",
		ResultState:    IdempotencyPending,
	}); err != nil {
		t.Fatalf("PutIdempotencyRecord: %v", err)
	}
	if err := repo.PutMutationOperation(ctx, MutationOperationRecord{
		OperationID:        "write-6964656d2d626f6f7473747261702d31",
		VolumeID:           "00a1b2c3",
		Kind:               "write",
		State:              MutationOperationRunning,
		AllocationRevision: 11,
		WriterFencingEpoch: 5,
		IdempotencyKey:     "idem-bootstrap-1",
		StartedAtUnix:      100,
		LastUpdatedAtUnix:  100,
	}); err != nil {
		t.Fatalf("PutMutationOperation: %v", err)
	}
	for _, mapping := range []ExtentMappingRecord{
		{VolumeID: "00a1b2c3", ExtentID: 1, LogicalOffset: 0, LengthBytes: 4, ChunkID: 101, PlacementRef: "pl-1", Revision: 11},
		{VolumeID: "00a1b2c3", ExtentID: 2, LogicalOffset: 4, LengthBytes: 4, ChunkID: 201, PlacementRef: "pl-2", Revision: 11},
	} {
		if err := repo.PutExtentMapping(ctx, mapping); err != nil {
			t.Fatalf("PutExtentMapping(%d): %v", mapping.ExtentID, err)
		}
	}

	_, _, err := repo.CommitWriteMetadata(ctx, CommitWriteMetadataRequest{
		VolumeID:                 "00a1b2c3",
		ExpectedEpoch:            5,
		ExpectedRevision:         11,
		IdempotencyKey:           "idem-bootstrap-1",
		ExpectedIdempotencyState: IdempotencyPending,
		CommittedRevision:        12,
		MutationOperationID:      "write-6964656d2d626f6f7473747261702d31",
		ExpectedMutationState:    MutationOperationRunning,
		NormalizeExtentMappings:  []uint64{1},
		AllocationPages: []AllocationPageRecord{
			{
				VolumeID:       "00a1b2c3",
				PageNo:         0,
				PageBytes:      4,
				ChunkSizeBytes: 4,
				Extents: []AllocationExtentRecord{
					{LogicalChunkStart: 0, ChunkCount: 1, Kind: AllocationKindData, PhysicalChunkStart: 555},
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("CommitWriteMetadata: %v", err)
	}

	pages, err := repo.ListAllocationPages(ctx, "00a1b2c3")
	if err != nil {
		t.Fatalf("ListAllocationPages: %v", err)
	}
	if len(pages) != 2 {
		t.Fatalf("allocation pages=%d want=2", len(pages))
	}
	if pages[0].Extents[0].Kind != AllocationKindData || pages[0].Extents[0].PhysicalChunkStart != 555 {
		t.Fatalf("page0 extents=%+v", pages[0].Extents)
	}
	if pages[1].Extents[0].Kind != AllocationKindData || pages[1].Extents[0].PhysicalChunkStart != 201 {
		t.Fatalf("page1 extents=%+v", pages[1].Extents)
	}
	for _, extentID := range []uint64{1, 2} {
		mapping, err := repo.GetExtentMapping(ctx, "00a1b2c3", extentID)
		if err != nil {
			t.Fatalf("GetExtentMapping(%d): %v", extentID, err)
		}
		if mapping.ChunkID != 0 || mapping.Revision != 12 {
			t.Fatalf("extent mapping %d=%+v", extentID, mapping)
		}
	}
}

func TestRepositoryCommitECFullStripeWriteUsesPageScopedAllocationCAS(t *testing.T) {
	ctx := context.Background()
	repo := NewRepository(newFakeKV(), "phase-y-test")
	seedECWriteIntent(t, ctx, repo, "idem-ec-page-0", "ec-op-page-0")
	if err := repo.PutAllocationPage(ctx, AllocationPageRecord{
		VolumeID:       "00a1b2c3",
		PageNo:         0,
		PageBytes:      8,
		ChunkSizeBytes: 4,
		Revision:       7,
		Extents: []AllocationExtentRecord{
			{LogicalChunkStart: 0, ChunkCount: 2, Kind: AllocationKindZero},
		},
	}); err != nil {
		t.Fatalf("PutAllocationPage: %v", err)
	}

	object, stripe := testECCommitRecords("ec-object-000010", "stripe-000010", 1)
	state, record, err := repo.CommitECFullStripeWrite(ctx, CommitECFullStripeWriteRequest{
		VolumeID:          "00a1b2c3",
		ExpectedEpoch:     5,
		ExpectedRevision:  11,
		IdempotencyKey:    "idem-ec-page-0",
		CommittedRevision: 12,
		PhysicalObject:    object,
		ECStripe:          stripe,
		AllocationPages: []AllocationPageRecord{{
			VolumeID:       "00a1b2c3",
			PageNo:         0,
			PageBytes:      8,
			ChunkSizeBytes: 4,
			Revision:       7,
			Extents: []AllocationExtentRecord{
				{LogicalChunkStart: 0, ChunkCount: 2, Kind: AllocationKindData, BackingRef: object.ObjectID, Generation: 1},
			},
		}},
		MutationOperationID:   "ec-op-page-0",
		ExpectedMutationState: MutationOperationRunning,
		AffectedPageNos:       []uint64{0},
	})
	if err != nil {
		t.Fatalf("CommitECFullStripeWrite: %v", err)
	}
	if state.Revision != 11 || record.Revision != 12 || record.ResultState != IdempotencyCommitted {
		t.Fatalf("state=%+v record=%+v want state revision unchanged and record revision 12 committed", state, record)
	}
	root, err := repo.GetVolumeState(ctx, "00a1b2c3")
	if err != nil {
		t.Fatalf("GetVolumeState: %v", err)
	}
	if root.Revision != 11 {
		t.Fatalf("volume revision=%d want unchanged 11", root.Revision)
	}
	page, err := repo.GetAllocationPage(ctx, "00a1b2c3", 0)
	if err != nil {
		t.Fatalf("GetAllocationPage: %v", err)
	}
	if page.Revision != 12 || len(page.Extents) != 1 || page.Extents[0].BackingRef != object.ObjectID {
		t.Fatalf("page=%+v want revision 12 backing %q", page, object.ObjectID)
	}
	gotObject, err := repo.GetPhysicalObject(ctx, "00a1b2c3", object.ObjectID)
	if err != nil {
		t.Fatalf("GetPhysicalObject: %v", err)
	}
	if gotObject.State != PhysicalObjectStateCommitted {
		t.Fatalf("object state=%q want committed", gotObject.State)
	}
	gotStripe, err := repo.GetECStripe(ctx, "00a1b2c3", stripe.StripeID, stripe.StripeGeneration)
	if err != nil {
		t.Fatalf("GetECStripe: %v", err)
	}
	if gotStripe.State != ECStripeStateCommitted {
		t.Fatalf("stripe state=%q want committed", gotStripe.State)
	}
	op, err := repo.GetMutationOperation(ctx, "00a1b2c3", "ec-op-page-0")
	if err != nil {
		t.Fatalf("GetMutationOperation: %v", err)
	}
	if op.State != MutationOperationCommitted || op.AllocationRevision != 12 {
		t.Fatalf("operation=%+v want committed allocation_revision=12", op)
	}
}

func TestRepositoryCommitECFullStripeWriteUsesMutationSnapshot(t *testing.T) {
	ctx := context.Background()
	kv := newFakeTransactionalKV()
	repo := NewRepository(kv, "phase-y-test")
	seedECWriteIntent(t, ctx, repo, "idem-ec-snapshot", "ec-op-snapshot")
	if err := repo.PutAllocationPage(ctx, AllocationPageRecord{
		VolumeID:       "00a1b2c3",
		PageNo:         0,
		PageBytes:      8,
		ChunkSizeBytes: 4,
		Revision:       7,
		Extents: []AllocationExtentRecord{
			{LogicalChunkStart: 0, ChunkCount: 2, Kind: AllocationKindZero},
		},
	}); err != nil {
		t.Fatalf("PutAllocationPage: %v", err)
	}

	operation := MutationOperationRecord{
		OperationID:        "ec-op-snapshot",
		VolumeID:           "00a1b2c3",
		Kind:               "ec-write",
		State:              MutationOperationRunning,
		AllocationRevision: 11,
		WriterFencingEpoch: 5,
		IdempotencyKey:     "idem-ec-snapshot",
		StartedAtUnix:      100,
		LastUpdatedAtUnix:  100,
	}
	object, stripe := testECCommitRecords("ec-object-snapshot", "stripe-snapshot", 1)
	kv.resetGetCalls()
	_, _, err := repo.CommitECFullStripeWrite(ctx, CommitECFullStripeWriteRequest{
		VolumeID:          "00a1b2c3",
		ExpectedEpoch:     5,
		ExpectedRevision:  11,
		IdempotencyKey:    "idem-ec-snapshot",
		CommittedRevision: 12,
		PhysicalObject:    object,
		ECStripe:          stripe,
		AllocationPages: []AllocationPageRecord{{
			VolumeID:       "00a1b2c3",
			PageNo:         0,
			PageBytes:      8,
			ChunkSizeBytes: 4,
			Revision:       7,
			Extents: []AllocationExtentRecord{
				{LogicalChunkStart: 0, ChunkCount: 2, Kind: AllocationKindData, BackingRef: object.ObjectID, Generation: 1},
			},
		}},
		MutationOperationID:   operation.OperationID,
		ExpectedMutationState: MutationOperationRunning,
		AffectedPageNos:       []uint64{0},
		MutationOperation:     operation,
	})
	if err != nil {
		t.Fatalf("CommitECFullStripeWrite: %v", err)
	}
	stateKey := volumeStateKey("phase-y-test", "00a1b2c3")
	recordKey := idempotencyKey("phase-y-test", "00a1b2c3", operation.IdempotencyKey)
	pageKey := allocationPageKey("phase-y-test", "00a1b2c3", 0)
	mutationKey := mutationOperationKey("phase-y-test", "00a1b2c3", operation.OperationID)
	if got := kv.batchGetCallCount(); got != 1 {
		t.Fatalf("backend BatchGet calls=%d want 1", got)
	}
	batchKeys := kv.lastBatchGetKeys()
	for _, want := range []string{stateKey, recordKey, pageKey} {
		if !slices.Contains(batchKeys, want) {
			t.Fatalf("batch keys %v missing %s", batchKeys, want)
		}
		if got := kv.getCallCount(want); got != 1 {
			t.Fatalf("backend read calls for prefetched key %s = %d want 1 batch read and no extra point get", want, got)
		}
	}
	if slices.Contains(batchKeys, mutationKey) {
		t.Fatalf("batch keys %v should not include mutation key when request snapshot is valid", batchKeys)
	}
	if got := kv.getCallCount(mutationKey); got != 0 {
		t.Fatalf("mutation operation get calls=%d want 0 when request snapshot is valid", got)
	}
	committed, err := repo.GetMutationOperation(ctx, "00a1b2c3", operation.OperationID)
	if err != nil {
		t.Fatalf("GetMutationOperation: %v", err)
	}
	if committed.State != MutationOperationCommitted || committed.StartedAtUnix != operation.StartedAtUnix || committed.IdempotencyKey != operation.IdempotencyKey {
		t.Fatalf("committed operation=%+v want committed snapshot preserving identity fields", committed)
	}
}

func TestRepositoryCommitECFullStripeWriteAllowsDifferentPageWithoutVolumeRootCAS(t *testing.T) {
	ctx := context.Background()
	repo := NewRepository(newFakeKV(), "phase-y-test")
	seedECWriteIntent(t, ctx, repo, "idem-ec-page-0", "ec-op-page-0")
	firstObject, firstStripe := testECCommitRecords("ec-object-000011", "stripe-000011", 1)
	if _, _, err := repo.CommitECFullStripeWrite(ctx, CommitECFullStripeWriteRequest{
		VolumeID:          "00a1b2c3",
		ExpectedEpoch:     5,
		ExpectedRevision:  11,
		IdempotencyKey:    "idem-ec-page-0",
		CommittedRevision: 12,
		PhysicalObject:    firstObject,
		ECStripe:          firstStripe,
		AllocationPages: []AllocationPageRecord{{
			VolumeID:       "00a1b2c3",
			PageNo:         0,
			PageBytes:      8,
			ChunkSizeBytes: 4,
			Revision:       0,
			Extents: []AllocationExtentRecord{
				{LogicalChunkStart: 0, ChunkCount: 2, Kind: AllocationKindData, BackingRef: firstObject.ObjectID, Generation: 1},
			},
		}},
		MutationOperationID:   "ec-op-page-0",
		ExpectedMutationState: MutationOperationRunning,
		AffectedPageNos:       []uint64{0},
	}); err != nil {
		t.Fatalf("first CommitECFullStripeWrite: %v", err)
	}

	seedECWriteIntent(t, ctx, repo, "idem-ec-page-1", "ec-op-page-1")
	secondObject, secondStripe := testECCommitRecords("ec-object-000012", "stripe-000012", 1)
	state, record, err := repo.CommitECFullStripeWrite(ctx, CommitECFullStripeWriteRequest{
		VolumeID:          "00a1b2c3",
		ExpectedEpoch:     5,
		ExpectedRevision:  11,
		IdempotencyKey:    "idem-ec-page-1",
		CommittedRevision: 12,
		PhysicalObject:    secondObject,
		ECStripe:          secondStripe,
		AllocationPages: []AllocationPageRecord{{
			VolumeID:       "00a1b2c3",
			PageNo:         1,
			PageBytes:      8,
			ChunkSizeBytes: 4,
			Revision:       0,
			Extents: []AllocationExtentRecord{
				{LogicalChunkStart: 2, ChunkCount: 2, Kind: AllocationKindData, BackingRef: secondObject.ObjectID, Generation: 1},
			},
		}},
		MutationOperationID:   "ec-op-page-1",
		ExpectedMutationState: MutationOperationRunning,
		AffectedPageNos:       []uint64{1},
	})
	if err != nil {
		t.Fatalf("second CommitECFullStripeWrite with unchanged expected root revision: %v", err)
	}
	if state.Revision != 11 || record.Revision != 12 {
		t.Fatalf("second state=%+v record=%+v want root revision 11 record revision 12", state, record)
	}
	root, err := repo.GetVolumeState(ctx, "00a1b2c3")
	if err != nil {
		t.Fatalf("GetVolumeState: %v", err)
	}
	if root.Revision != 11 {
		t.Fatalf("volume revision=%d want unchanged 11", root.Revision)
	}
}

func TestRepositoryCommitECFullStripeWriteRejectsStaleAllocationPage(t *testing.T) {
	ctx := context.Background()
	repo := NewRepository(newFakeKV(), "phase-y-test")
	seedECWriteIntent(t, ctx, repo, "idem-ec-stale-page", "ec-op-stale-page")
	if err := repo.PutAllocationPage(ctx, AllocationPageRecord{
		VolumeID:       "00a1b2c3",
		PageNo:         0,
		PageBytes:      8,
		ChunkSizeBytes: 4,
		Revision:       8,
		Extents: []AllocationExtentRecord{
			{LogicalChunkStart: 0, ChunkCount: 2, Kind: AllocationKindZero},
		},
	}); err != nil {
		t.Fatalf("PutAllocationPage: %v", err)
	}
	object, stripe := testECCommitRecords("ec-object-000013", "stripe-000013", 1)
	_, _, err := repo.CommitECFullStripeWrite(ctx, CommitECFullStripeWriteRequest{
		VolumeID:          "00a1b2c3",
		ExpectedEpoch:     5,
		ExpectedRevision:  11,
		IdempotencyKey:    "idem-ec-stale-page",
		CommittedRevision: 12,
		PhysicalObject:    object,
		ECStripe:          stripe,
		AllocationPages: []AllocationPageRecord{{
			VolumeID:       "00a1b2c3",
			PageNo:         0,
			PageBytes:      8,
			ChunkSizeBytes: 4,
			Revision:       7,
			Extents: []AllocationExtentRecord{
				{LogicalChunkStart: 0, ChunkCount: 2, Kind: AllocationKindData, BackingRef: object.ObjectID, Generation: 1},
			},
		}},
		MutationOperationID:   "ec-op-stale-page",
		ExpectedMutationState: MutationOperationRunning,
		AffectedPageNos:       []uint64{0},
	})
	if !errors.Is(err, ErrCASConflict) {
		t.Fatalf("CommitECFullStripeWrite err=%v want ErrCASConflict", err)
	}
	if _, err := repo.GetPhysicalObject(ctx, "00a1b2c3", object.ObjectID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("GetPhysicalObject after failed commit err=%v want ErrNotFound", err)
	}
	record, err := repo.GetIdempotencyRecord(ctx, "00a1b2c3", "idem-ec-stale-page")
	if err != nil {
		t.Fatalf("GetIdempotencyRecord: %v", err)
	}
	if record.ResultState != IdempotencyPending {
		t.Fatalf("idempotency state=%q want pending", record.ResultState)
	}
}

func seedECWriteIntent(t *testing.T, ctx context.Context, repo *Repository, idemKey, operationID string) {
	t.Helper()
	if _, err := repo.GetVolumeState(ctx, "00a1b2c3"); errors.Is(err, ErrNotFound) {
		if putErr := repo.PutVolumeState(ctx, VolumeState{
			VolumeID:          "00a1b2c3",
			Epoch:             5,
			Revision:          11,
			RedundancyBackend: "ec",
			Status:            VolumeStatusHealthy,
		}); putErr != nil {
			t.Fatalf("PutVolumeState: %v", putErr)
		}
	} else if err != nil {
		t.Fatalf("GetVolumeState: %v", err)
	}
	if err := repo.PutIdempotencyRecord(ctx, IdempotencyRecord{
		IdempotencyKey: idemKey,
		VolumeID:       "00a1b2c3",
		AttachmentID:   "att-ec",
		Generation:     1,
		Epoch:          5,
		Revision:       11,
		Operation:      "ec-write",
		ResultState:    IdempotencyPending,
	}); err != nil {
		t.Fatalf("PutIdempotencyRecord: %v", err)
	}
	if err := repo.PutMutationOperation(ctx, MutationOperationRecord{
		OperationID:        operationID,
		VolumeID:           "00a1b2c3",
		Kind:               "ec-write",
		State:              MutationOperationRunning,
		AllocationRevision: 11,
		WriterFencingEpoch: 5,
		IdempotencyKey:     idemKey,
	}); err != nil {
		t.Fatalf("PutMutationOperation: %v", err)
	}
}

func testECCommitRecords(objectID, stripeID string, generation uint64) (PhysicalObjectRecord, ECStripeRecord) {
	object := testECPhysicalObjectRecord()
	object.ObjectID = objectID
	object.Generation = generation
	object.State = PhysicalObjectStatePreparing
	object.EC.StripeID = stripeID
	object.EC.StripeGeneration = generation
	stripe := testECStripeRecord()
	stripe.ObjectID = objectID
	stripe.StripeID = stripeID
	stripe.StripeGeneration = generation
	stripe.State = ECStripeStatePreparing
	for i := range stripe.Shards {
		stripe.Shards[i].ShardObjectID = fmt.Sprintf("%s-shard-%d", objectID, i)
	}
	return object, stripe
}

func TestRepositoryGetCompatibleAllocationPageFallsBackToExtentMappings(t *testing.T) {
	ctx := context.Background()
	repo := NewRepository(newFakeKV(), "phase-g-test")

	if err := repo.PutExtentMapping(ctx, ExtentMappingRecord{
		VolumeID:      "00a1b2c3",
		ExtentID:      1,
		LogicalOffset: 0,
		LengthBytes:   2 * 4096,
		ChunkID:       100,
		PlacementRef:  "pl-1",
		Revision:      1,
	}); err != nil {
		t.Fatalf("PutExtentMapping: %v", err)
	}

	page, err := repo.GetCompatibleAllocationPage(ctx, "00a1b2c3", 0, 4*4096, 4096)
	if err != nil {
		t.Fatalf("GetCompatibleAllocationPage: %v", err)
	}
	if len(page.Extents) != 2 {
		t.Fatalf("unexpected extents: %+v", page.Extents)
	}
	if page.Extents[0].Kind != AllocationKindData || page.Extents[0].LogicalChunkStart != 0 || page.Extents[0].ChunkCount != 2 || page.Extents[0].PhysicalChunkStart != 100 {
		t.Fatalf("unexpected first extent: %+v", page.Extents[0])
	}
	if page.Extents[1].Kind != AllocationKindZero || page.Extents[1].LogicalChunkStart != 2 || page.Extents[1].ChunkCount != 2 {
		t.Fatalf("unexpected second extent: %+v", page.Extents[1])
	}
}

func TestRepositoryGetCompatibleAllocationPagePrefersNativeAllocationMap(t *testing.T) {
	ctx := context.Background()
	repo := NewRepository(newFakeKV(), "phase-g-test")

	if err := repo.PutExtentMapping(ctx, ExtentMappingRecord{
		VolumeID:      "00a1b2c3",
		ExtentID:      1,
		LogicalOffset: 0,
		LengthBytes:   4 * 4096,
		ChunkID:       100,
		PlacementRef:  "pl-1",
		Revision:      1,
	}); err != nil {
		t.Fatalf("PutExtentMapping: %v", err)
	}
	if err := repo.PutAllocationPage(ctx, AllocationPageRecord{
		VolumeID:       "00a1b2c3",
		PageNo:         0,
		PageBytes:      4 * 4096,
		ChunkSizeBytes: 4096,
		Revision:       9,
		Extents: []AllocationExtentRecord{
			{LogicalChunkStart: 0, ChunkCount: 4, Kind: AllocationKindZero},
		},
	}); err != nil {
		t.Fatalf("PutAllocationPage: %v", err)
	}

	page, err := repo.GetCompatibleAllocationPage(ctx, "00a1b2c3", 0, 4*4096, 4096)
	if err != nil {
		t.Fatalf("GetCompatibleAllocationPage: %v", err)
	}
	if page.Revision != 9 || len(page.Extents) != 1 || page.Extents[0].Kind != AllocationKindZero {
		t.Fatalf("expected native allocation page to win: %+v", page)
	}
}

func TestRepositoryGetCompatibleAllocationPageReturnsZeroForMissingNativePage(t *testing.T) {
	ctx := context.Background()
	repo := NewRepository(newFakeKV(), "phase-g-test")

	if err := repo.PutExtentMapping(ctx, ExtentMappingRecord{
		VolumeID:      "00a1b2c3",
		ExtentID:      1,
		LogicalOffset: 4 * 4096,
		LengthBytes:   2 * 4096,
		ChunkID:       200,
		PlacementRef:  "pl-1",
		Revision:      1,
	}); err != nil {
		t.Fatalf("PutExtentMapping: %v", err)
	}
	if err := repo.PutAllocationPage(ctx, AllocationPageRecord{
		VolumeID:       "00a1b2c3",
		PageNo:         1,
		PageBytes:      4 * 4096,
		ChunkSizeBytes: 4096,
		Revision:       3,
		Extents: []AllocationExtentRecord{
			{LogicalChunkStart: 4, ChunkCount: 2, Kind: AllocationKindData, PhysicalChunkStart: 200},
			{LogicalChunkStart: 6, ChunkCount: 2, Kind: AllocationKindZero},
		},
	}); err != nil {
		t.Fatalf("PutAllocationPage: %v", err)
	}

	page, err := repo.GetCompatibleAllocationPage(ctx, "00a1b2c3", 0, 4*4096, 4096)
	if err != nil {
		t.Fatalf("GetCompatibleAllocationPage: %v", err)
	}
	if len(page.Extents) != 1 || page.Extents[0].Kind != AllocationKindZero || page.Extents[0].ChunkCount != 4 {
		t.Fatalf("expected zero page when native page is absent: %+v", page)
	}
}

func TestRepositoryListCompatibleAllocationPagesFallsBackToExtentMappings(t *testing.T) {
	ctx := context.Background()
	repo := NewRepository(newFakeKV(), "phase-g-test")

	if err := repo.PutExtentMapping(ctx, ExtentMappingRecord{
		VolumeID:      "00a1b2c3",
		ExtentID:      1,
		LogicalOffset: 0,
		LengthBytes:   4 * 4096,
		ChunkID:       10,
		PlacementRef:  "pl-1",
		Revision:      1,
	}); err != nil {
		t.Fatalf("PutExtentMapping(1): %v", err)
	}
	if err := repo.PutExtentMapping(ctx, ExtentMappingRecord{
		VolumeID:      "00a1b2c3",
		ExtentID:      2,
		LogicalOffset: 4 * 4096,
		LengthBytes:   2 * 4096,
		ChunkID:       20,
		PlacementRef:  "pl-2",
		Revision:      1,
	}); err != nil {
		t.Fatalf("PutExtentMapping(2): %v", err)
	}

	pages, err := repo.ListCompatibleAllocationPages(ctx, "00a1b2c3", 4*4096, 4096)
	if err != nil {
		t.Fatalf("ListCompatibleAllocationPages: %v", err)
	}
	if len(pages) != 2 {
		t.Fatalf("unexpected pages: %+v", pages)
	}
	if pages[0].PageNo != 0 || pages[1].PageNo != 1 {
		t.Fatalf("unexpected page order: %+v", pages)
	}
	if len(pages[1].Extents) != 2 || pages[1].Extents[0].Kind != AllocationKindData || pages[1].Extents[1].Kind != AllocationKindZero {
		t.Fatalf("unexpected synthesized page 1: %+v", pages[1])
	}
}

func TestServiceResolveAllocationPagesUsesCompatibleReader(t *testing.T) {
	ctx := context.Background()
	repo := NewRepository(newFakeKV(), "phase-g-test")
	svc := NewService(repo)

	if err := repo.PutExtentMapping(ctx, ExtentMappingRecord{
		VolumeID:      "00a1b2c3",
		ExtentID:      1,
		LogicalOffset: 0,
		LengthBytes:   4 * 4096,
		ChunkID:       10,
		PlacementRef:  "pl-1",
		Revision:      1,
	}); err != nil {
		t.Fatalf("PutExtentMapping(1): %v", err)
	}
	if err := repo.PutExtentMapping(ctx, ExtentMappingRecord{
		VolumeID:      "00a1b2c3",
		ExtentID:      2,
		LogicalOffset: 4 * 4096,
		LengthBytes:   4 * 4096,
		ChunkID:       20,
		PlacementRef:  "pl-2",
		Revision:      1,
	}); err != nil {
		t.Fatalf("PutExtentMapping(2): %v", err)
	}

	pages, err := svc.ResolveAllocationPages(ctx, "00a1b2c3", 2*4096, 4*4096, 4*4096, 4096)
	if err != nil {
		t.Fatalf("ResolveAllocationPages: %v", err)
	}
	if len(pages) != 2 {
		t.Fatalf("unexpected page count: %+v", pages)
	}
	if pages[0].Page.PageNo != 0 || pages[0].RangeStartChunk != 2 || pages[0].RangeEndChunk != 4 || pages[0].CoversWholePage {
		t.Fatalf("unexpected first resolved page: %+v", pages[0])
	}
	if pages[1].Page.PageNo != 1 || pages[1].RangeStartChunk != 4 || pages[1].RangeEndChunk != 6 || pages[1].CoversWholePage {
		t.Fatalf("unexpected second resolved page: %+v", pages[1])
	}
	if pages[0].Page.Extents[0].Kind != AllocationKindData {
		t.Fatalf("expected synthesized allocation extents, got: %+v", pages[0].Page.Extents)
	}
}

func TestServiceResolveAllocationPagesPrefersNativeAllocationMap(t *testing.T) {
	ctx := context.Background()
	repo := NewRepository(newFakeKV(), "phase-g-test")
	svc := NewService(repo)

	if err := repo.PutExtentMapping(ctx, ExtentMappingRecord{
		VolumeID:      "00a1b2c3",
		ExtentID:      1,
		LogicalOffset: 0,
		LengthBytes:   4 * 4096,
		ChunkID:       10,
		PlacementRef:  "pl-1",
		Revision:      1,
	}); err != nil {
		t.Fatalf("PutExtentMapping: %v", err)
	}
	if err := repo.PutAllocationPage(ctx, AllocationPageRecord{
		VolumeID:       "00a1b2c3",
		PageNo:         0,
		PageBytes:      4 * 4096,
		ChunkSizeBytes: 4096,
		Revision:       5,
		Extents: []AllocationExtentRecord{
			{LogicalChunkStart: 0, ChunkCount: 4, Kind: AllocationKindZero},
		},
	}); err != nil {
		t.Fatalf("PutAllocationPage: %v", err)
	}

	pages, err := svc.ResolveAllocationPages(ctx, "00a1b2c3", 0, 4*4096, 4*4096, 4096)
	if err != nil {
		t.Fatalf("ResolveAllocationPages: %v", err)
	}
	if len(pages) != 1 {
		t.Fatalf("unexpected page count: %+v", pages)
	}
	if !pages[0].CoversWholePage || pages[0].Page.Revision != 5 || pages[0].Page.Extents[0].Kind != AllocationKindZero {
		t.Fatalf("expected native allocation page to win: %+v", pages[0])
	}
}

func TestServiceResolveAllocationPagesUsesListerForWideRange(t *testing.T) {
	ctx := context.Background()
	store := &countingCompatibleAllocationStore{
		pages: []AllocationPageRecord{
			{
				VolumeID:       "00a1b2c3",
				PageNo:         0,
				PageBytes:      4096,
				ChunkSizeBytes: 4096,
				Revision:       5,
				Extents: []AllocationExtentRecord{
					{LogicalChunkStart: 0, ChunkCount: 1, Kind: AllocationKindZero},
				},
			},
			{
				VolumeID:       "00a1b2c3",
				PageNo:         9,
				PageBytes:      4096,
				ChunkSizeBytes: 4096,
				Revision:       6,
				Extents: []AllocationExtentRecord{
					{LogicalChunkStart: 9, ChunkCount: 1, Kind: AllocationKindData, PhysicalChunkStart: 900},
				},
			},
		},
	}
	svc := NewServiceWithDependencies(nil, store, store)

	pages, err := svc.ResolveAllocationPages(ctx, "00a1b2c3", 0, 10*4096, 4096, 4096)
	if err != nil {
		t.Fatalf("ResolveAllocationPages: %v", err)
	}
	if store.listCalls != 1 || store.getCalls != 0 {
		t.Fatalf("listCalls=%d getCalls=%d, want list once without per-page get", store.listCalls, store.getCalls)
	}
	if len(pages) != 10 {
		t.Fatalf("resolved pages=%d want 10", len(pages))
	}
	if pages[1].Page.PageNo != 1 || pages[1].Page.Extents[0].Kind != AllocationKindZero {
		t.Fatalf("missing native page should resolve as zero page: %+v", pages[1])
	}
	if pages[9].Page.Extents[0].Kind != AllocationKindData || pages[9].Page.Extents[0].PhysicalChunkStart != 900 {
		t.Fatalf("listed native page not preserved: %+v", pages[9])
	}
}

func TestServiceResolveAllocationPagesKeepsReaderForNarrowRange(t *testing.T) {
	ctx := context.Background()
	store := &countingCompatibleAllocationStore{
		pages: []AllocationPageRecord{
			{
				VolumeID:       "00a1b2c3",
				PageNo:         0,
				PageBytes:      4096,
				ChunkSizeBytes: 4096,
				Revision:       5,
				Extents: []AllocationExtentRecord{
					{LogicalChunkStart: 0, ChunkCount: 1, Kind: AllocationKindZero},
				},
			},
		},
	}
	svc := NewServiceWithDependencies(nil, store, store)

	pages, err := svc.ResolveAllocationPages(ctx, "00a1b2c3", 0, 2*4096, 4096, 4096)
	if err != nil {
		t.Fatalf("ResolveAllocationPages: %v", err)
	}
	if store.listCalls != 0 || store.getCalls != 2 {
		t.Fatalf("listCalls=%d getCalls=%d, want per-page get for narrow range", store.listCalls, store.getCalls)
	}
	if len(pages) != 2 {
		t.Fatalf("resolved pages=%d want 2", len(pages))
	}
}

func TestServiceResolveSnapshotAllocationPagesUsesCapturedReadView(t *testing.T) {
	ctx := context.Background()
	repo := NewRepository(newFakeKV(), "phase-j-test")
	svc := NewService(repo)
	snapshotID := "snap-00a1b2c3-20260521T120000.000000000Z"

	if err := repo.CaptureSnapshotAllocationPages(ctx, snapshotID, []AllocationPageRecord{
		{
			VolumeID:       "00a1b2c3",
			PageNo:         0,
			PageBytes:      4 * 4096,
			ChunkSizeBytes: 4096,
			Revision:       7,
			Extents: []AllocationExtentRecord{
				{LogicalChunkStart: 0, ChunkCount: 4, Kind: AllocationKindData, PhysicalChunkStart: 100},
			},
		},
		{
			VolumeID:       "00a1b2c3",
			PageNo:         1,
			PageBytes:      4 * 4096,
			ChunkSizeBytes: 4096,
			Revision:       7,
			Extents: []AllocationExtentRecord{
				{LogicalChunkStart: 4, ChunkCount: 4, Kind: AllocationKindZero},
			},
		},
	}); err != nil {
		t.Fatalf("CaptureSnapshotAllocationPages: %v", err)
	}

	pages, err := svc.ResolveSnapshotAllocationPages(ctx, snapshotID, 2*4096, 4*4096, 4*4096, 4096)
	if err != nil {
		t.Fatalf("ResolveSnapshotAllocationPages: %v", err)
	}
	if len(pages) != 2 {
		t.Fatalf("unexpected page count: %+v", pages)
	}
	if pages[0].Page.PageNo != 0 || pages[0].RangeStartChunk != 2 || pages[0].RangeEndChunk != 4 || pages[0].CoversWholePage {
		t.Fatalf("unexpected first resolved page: %+v", pages[0])
	}
	if pages[1].Page.PageNo != 1 || pages[1].RangeStartChunk != 4 || pages[1].RangeEndChunk != 6 || pages[1].CoversWholePage {
		t.Fatalf("unexpected second resolved page: %+v", pages[1])
	}
	if pages[0].Page.Extents[0].PhysicalChunkStart != 100 || pages[1].Page.Extents[0].Kind != AllocationKindZero {
		t.Fatalf("unexpected snapshot read view pages: %+v", pages)
	}
}

func TestServiceResolveSnapshotAllocationPagesRejectsGeometryMismatch(t *testing.T) {
	ctx := context.Background()
	repo := NewRepository(newFakeKV(), "phase-j-test")
	svc := NewService(repo)
	snapshotID := "snap-00a1b2c3-20260521T120000.000000000Z"

	if err := repo.CaptureSnapshotAllocationPages(ctx, snapshotID, []AllocationPageRecord{{
		VolumeID:       "00a1b2c3",
		PageNo:         0,
		PageBytes:      8 * 4096,
		ChunkSizeBytes: 4096,
		Revision:       7,
		Extents: []AllocationExtentRecord{
			{LogicalChunkStart: 0, ChunkCount: 8, Kind: AllocationKindZero},
		},
	}}); err != nil {
		t.Fatalf("CaptureSnapshotAllocationPages: %v", err)
	}

	if _, err := svc.ResolveSnapshotAllocationPages(ctx, snapshotID, 0, 4*4096, 4*4096, 4096); err == nil {
		t.Fatalf("ResolveSnapshotAllocationPages should reject mismatched geometry")
	}
}

func TestServiceResolveSnapshotAllocationPagesSynthesizesMissingPageAsZero(t *testing.T) {
	ctx := context.Background()
	repo := NewRepository(newFakeKV(), "phase-j-test")
	svc := NewService(repo)
	snapshotID := "snap-00a1b2c3-20260521T120000.000000000Z"

	if _, _, err := repo.CreateSnapshotRecord(ctx, SnapshotRecord{
		SnapshotID:               snapshotID,
		SourceVolumeID:           "00a1b2c3",
		SnapshotRootID:           snapshotID,
		State:                    SnapshotStateAvailable,
		CreatedAtUnix:            100,
		UpdatedAtUnix:            100,
		CutVolumeRevision:        7,
		AllocationChunkSizeBytes: 4096,
		AllocationPageSizeBytes:  4 * 4096,
		SourceSizeBytes:          8 * 4096,
	}); err != nil {
		t.Fatalf("CreateSnapshotRecord: %v", err)
	}

	pages, err := svc.ResolveSnapshotAllocationPages(ctx, snapshotID, 4*4096, 4096, 4*4096, 4096)
	if err != nil {
		t.Fatalf("ResolveSnapshotAllocationPages: %v", err)
	}
	if len(pages) != 1 {
		t.Fatalf("page count=%d want 1", len(pages))
	}
	page := pages[0].Page
	if page.VolumeID != "00a1b2c3" || page.PageNo != 1 || page.PageBytes != 4*4096 || page.ChunkSizeBytes != 4096 {
		t.Fatalf("zero page metadata=%+v", page)
	}
	if len(page.Extents) != 1 || page.Extents[0].Kind != AllocationKindZero || page.Extents[0].LogicalChunkStart != 4 || page.Extents[0].ChunkCount != 4 {
		t.Fatalf("zero page extents=%+v", page.Extents)
	}
}

func TestServiceResolveCloneAllocationPagesUsesBaseSnapshotReadView(t *testing.T) {
	ctx := context.Background()
	repo := NewRepository(newFakeKV(), "phase-j-test")
	svc := NewService(repo)
	snapshotID := "snap-00a1b2c3-20260521T120000.000000000Z"

	if _, _, err := repo.CreateSnapshotRecord(ctx, SnapshotRecord{
		SnapshotID:               snapshotID,
		SourceVolumeID:           "00a1b2c3",
		SnapshotRootID:           snapshotID,
		State:                    SnapshotStateAvailable,
		CreatedAtUnix:            100,
		UpdatedAtUnix:            100,
		CutVolumeRevision:        7,
		AllocationChunkSizeBytes: 4096,
		AllocationPageSizeBytes:  4 * 4096,
		SourceSizeBytes:          8 * 4096,
	}); err != nil {
		t.Fatalf("CreateSnapshotRecord: %v", err)
	}
	if err := repo.CaptureSnapshotAllocationPages(ctx, snapshotID, []AllocationPageRecord{{
		VolumeID:       "00a1b2c3",
		PageNo:         0,
		PageBytes:      4 * 4096,
		ChunkSizeBytes: 4096,
		Revision:       7,
		Extents: []AllocationExtentRecord{
			{LogicalChunkStart: 0, ChunkCount: 4, Kind: AllocationKindData, PhysicalChunkStart: 100},
		},
	}}); err != nil {
		t.Fatalf("CaptureSnapshotAllocationPages: %v", err)
	}
	clone, _, err := repo.CreateCloneRecord(ctx, CloneRecord{
		CloneID:          "clone-1",
		SourceSnapshotID: snapshotID,
		CreatedAtUnix:    101,
		UpdatedAtUnix:    101,
	})
	if err != nil {
		t.Fatalf("CreateCloneRecord: %v", err)
	}

	pages, err := svc.ResolveCloneAllocationPages(ctx, clone.CloneID, 2*4096, 4*4096, 4*4096, 4096)
	if err != nil {
		t.Fatalf("ResolveCloneAllocationPages: %v", err)
	}
	if len(pages) != 2 {
		t.Fatalf("page count=%d want 2", len(pages))
	}
	if pages[0].Page.PageNo != 0 || pages[0].Page.Extents[0].PhysicalChunkStart != 100 {
		t.Fatalf("first clone read page=%+v", pages[0])
	}
	if pages[1].Page.PageNo != 1 || len(pages[1].Page.Extents) != 1 || pages[1].Page.Extents[0].Kind != AllocationKindZero {
		t.Fatalf("missing clone read page should synthesize zero: %+v", pages[1])
	}
}

func TestServiceResolveCloneAllocationPagesOverlaysDelta(t *testing.T) {
	ctx := context.Background()
	repo := NewRepository(newFakeKV(), "phase-j-test")
	svc := NewService(repo)
	snapshotID := "snap-00a1b2c3-20260521T120000.000000000Z"

	if _, _, err := repo.CreateSnapshotRecord(ctx, SnapshotRecord{
		SnapshotID:               snapshotID,
		SourceVolumeID:           "00a1b2c3",
		SnapshotRootID:           snapshotID,
		State:                    SnapshotStateAvailable,
		CreatedAtUnix:            100,
		UpdatedAtUnix:            100,
		AllocationChunkSizeBytes: 4096,
		AllocationPageSizeBytes:  4 * 4096,
		SourceSizeBytes:          8 * 4096,
	}); err != nil {
		t.Fatalf("CreateSnapshotRecord: %v", err)
	}
	if err := repo.CaptureSnapshotAllocationPages(ctx, snapshotID, []AllocationPageRecord{{
		VolumeID:       "00a1b2c3",
		PageNo:         0,
		PageBytes:      4 * 4096,
		ChunkSizeBytes: 4096,
		Revision:       7,
		Extents: []AllocationExtentRecord{
			{LogicalChunkStart: 0, ChunkCount: 4, Kind: AllocationKindData, PhysicalChunkStart: 100},
		},
	}}); err != nil {
		t.Fatalf("CaptureSnapshotAllocationPages: %v", err)
	}
	clone, _, err := repo.CreateCloneRecord(ctx, CloneRecord{
		CloneID:          "clone-1",
		SourceSnapshotID: snapshotID,
		CreatedAtUnix:    101,
		UpdatedAtUnix:    101,
	})
	if err != nil {
		t.Fatalf("CreateCloneRecord: %v", err)
	}
	if err := svc.CommitCloneDeltaAllocationPages(ctx, clone.CloneID, []AllocationPageRecord{{
		PageNo:         0,
		PageBytes:      4 * 4096,
		ChunkSizeBytes: 4096,
		Revision:       8,
		Extents: []AllocationExtentRecord{
			{LogicalChunkStart: 0, ChunkCount: 4, Kind: AllocationKindData, PhysicalChunkStart: 200},
		},
	}}); err != nil {
		t.Fatalf("CommitCloneDeltaAllocationPages: %v", err)
	}

	pages, err := svc.ResolveCloneAllocationPages(ctx, clone.CloneID, 0, 4*4096, 4*4096, 4096)
	if err != nil {
		t.Fatalf("ResolveCloneAllocationPages: %v", err)
	}
	if len(pages) != 1 {
		t.Fatalf("page count=%d want 1", len(pages))
	}
	if pages[0].Page.Revision != 8 || pages[0].Page.Extents[0].PhysicalChunkStart != 200 {
		t.Fatalf("clone delta should override base snapshot page: %+v", pages[0])
	}
	basePages, err := svc.ResolveSnapshotAllocationPages(ctx, snapshotID, 0, 4*4096, 4*4096, 4096)
	if err != nil {
		t.Fatalf("ResolveSnapshotAllocationPages: %v", err)
	}
	if len(basePages) != 1 || basePages[0].Page.Extents[0].PhysicalChunkStart != 100 {
		t.Fatalf("snapshot base page should remain unchanged: %+v", basePages)
	}
}

func TestServiceResolveCloneAllocationPagesRejectsDeletedClone(t *testing.T) {
	ctx := context.Background()
	repo := NewRepository(newFakeKV(), "phase-j-test")
	svc := NewService(repo)
	snapshotID := "snap-00a1b2c3-20260521T120000.000000000Z"

	if _, _, err := repo.CreateSnapshotRecord(ctx, SnapshotRecord{
		SnapshotID:               snapshotID,
		SourceVolumeID:           "00a1b2c3",
		SnapshotRootID:           snapshotID,
		State:                    SnapshotStateAvailable,
		CreatedAtUnix:            100,
		UpdatedAtUnix:            100,
		AllocationChunkSizeBytes: 4096,
		AllocationPageSizeBytes:  4 * 4096,
		SourceSizeBytes:          8 * 4096,
	}); err != nil {
		t.Fatalf("CreateSnapshotRecord: %v", err)
	}
	clone, _, err := repo.CreateCloneRecord(ctx, CloneRecord{
		CloneID:          "clone-1",
		SourceSnapshotID: snapshotID,
		CreatedAtUnix:    101,
		UpdatedAtUnix:    101,
	})
	if err != nil {
		t.Fatalf("CreateCloneRecord: %v", err)
	}
	if _, err := repo.DeleteCloneRecord(ctx, clone.CloneID); err != nil {
		t.Fatalf("DeleteCloneRecord: %v", err)
	}
	if _, err := svc.ResolveCloneAllocationPages(ctx, clone.CloneID, 0, 4096, 4*4096, 4096); err == nil {
		t.Fatalf("ResolveCloneAllocationPages should reject deleted clone")
	}
}

func TestServiceResolveCloneAllocationPagesAllowsMaterializingClone(t *testing.T) {
	ctx := context.Background()
	repo := NewRepository(newFakeKV(), "phase-j-test")
	svc := NewService(repo)
	snapshotID := "snap-00a1b2c3-20260521T120000.000000000Z"

	if _, _, err := repo.CreateSnapshotRecord(ctx, SnapshotRecord{
		SnapshotID:               snapshotID,
		SourceVolumeID:           "00a1b2c3",
		SnapshotRootID:           snapshotID,
		State:                    SnapshotStateAvailable,
		CreatedAtUnix:            100,
		UpdatedAtUnix:            100,
		AllocationChunkSizeBytes: 4096,
		AllocationPageSizeBytes:  4 * 4096,
		SourceSizeBytes:          8 * 4096,
	}); err != nil {
		t.Fatalf("CreateSnapshotRecord: %v", err)
	}
	if err := repo.CaptureSnapshotAllocationPages(ctx, snapshotID, []AllocationPageRecord{{
		VolumeID:       "00a1b2c3",
		PageNo:         0,
		PageBytes:      4 * 4096,
		ChunkSizeBytes: 4096,
		Revision:       7,
		Extents: []AllocationExtentRecord{
			{LogicalChunkStart: 0, ChunkCount: 4, Kind: AllocationKindData, PhysicalChunkStart: 100},
		},
	}}); err != nil {
		t.Fatalf("CaptureSnapshotAllocationPages: %v", err)
	}
	clone, _, err := repo.CreateCloneRecord(ctx, CloneRecord{
		CloneID:          "clone-1",
		SourceSnapshotID: snapshotID,
		CreatedAtUnix:    101,
		UpdatedAtUnix:    101,
	})
	if err != nil {
		t.Fatalf("CreateCloneRecord: %v", err)
	}
	if _, err := repo.MarkCloneState(ctx, clone.CloneID, CloneStateMaterializing, ""); err != nil {
		t.Fatalf("MarkCloneState materializing: %v", err)
	}
	pages, err := svc.ResolveCloneAllocationPages(ctx, clone.CloneID, 0, 4096, 4*4096, 4096)
	if err != nil {
		t.Fatalf("ResolveCloneAllocationPages materializing: %v", err)
	}
	if len(pages) != 1 || pages[0].Page.Extents[0].PhysicalChunkStart != 100 {
		t.Fatalf("materializing clone pages=%+v", pages)
	}
	if err := svc.CommitCloneDeltaAllocationPages(ctx, clone.CloneID, []AllocationPageRecord{{
		PageNo:         0,
		PageBytes:      4 * 4096,
		ChunkSizeBytes: 4096,
		Revision:       8,
		Extents: []AllocationExtentRecord{
			{LogicalChunkStart: 0, ChunkCount: 4, Kind: AllocationKindData, PhysicalChunkStart: 200},
		},
	}}); err == nil {
		t.Fatalf("CommitCloneDeltaAllocationPages should reject materializing clone")
	}
}

func TestServiceReconcileMutationOperationScopeFromRetiredChunks(t *testing.T) {
	ctx := context.Background()
	repo := NewRepository(newFakeKV(), "phase-g-test")
	svc := NewService(repo)

	if err := repo.PutExtentMapping(ctx, ExtentMappingRecord{
		VolumeID:      "00a1b2c3",
		ExtentID:      1,
		LogicalOffset: 0,
		LengthBytes:   8,
		ChunkID:       0,
		PlacementRef:  "pl-1",
		Revision:      12,
	}); err != nil {
		t.Fatalf("PutExtentMapping(1): %v", err)
	}
	if err := repo.PutExtentMapping(ctx, ExtentMappingRecord{
		VolumeID:      "00a1b2c3",
		ExtentID:      2,
		LogicalOffset: 8,
		LengthBytes:   8,
		ChunkID:       0,
		PlacementRef:  "pl-2",
		Revision:      12,
	}); err != nil {
		t.Fatalf("PutExtentMapping(2): %v", err)
	}
	for _, page := range []AllocationPageRecord{
		{
			VolumeID:       "00a1b2c3",
			PageNo:         0,
			PageBytes:      8,
			ChunkSizeBytes: 4,
			Revision:       12,
			Extents: []AllocationExtentRecord{
				{LogicalChunkStart: 0, ChunkCount: 2, Kind: AllocationKindData, PhysicalChunkStart: 500},
			},
		},
		{
			VolumeID:       "00a1b2c3",
			PageNo:         1,
			PageBytes:      8,
			ChunkSizeBytes: 4,
			Revision:       12,
			Extents: []AllocationExtentRecord{
				{LogicalChunkStart: 2, ChunkCount: 2, Kind: AllocationKindData, PhysicalChunkStart: 700},
			},
		},
	} {
		if err := repo.PutAllocationPage(ctx, page); err != nil {
			t.Fatalf("PutAllocationPage(%d): %v", page.PageNo, err)
		}
	}

	reconciled, err := svc.ReconcileMutationOperationScope(ctx, "00a1b2c3", MutationOperationRecord{
		OperationID:             "write-retired-only",
		VolumeID:                "00a1b2c3",
		Kind:                    "write",
		State:                   MutationOperationCommitted,
		RetiredPhysicalChunkIDs: []uint64{500},
	}, 8, 4)
	if err != nil {
		t.Fatalf("ReconcileMutationOperationScope: %v", err)
	}
	if !reconciled.Changed {
		t.Fatalf("expected scope reconciliation to change operation")
	}
	if len(reconciled.Operation.AffectedPageNos) != 1 || reconciled.Operation.AffectedPageNos[0] != 0 {
		t.Fatalf("affected pages=%v want=[0]", reconciled.Operation.AffectedPageNos)
	}
	if len(reconciled.Operation.AffectedExtentIDs) != 1 || reconciled.Operation.AffectedExtentIDs[0] != 1 {
		t.Fatalf("affected extents=%v want=[1]", reconciled.Operation.AffectedExtentIDs)
	}
}
