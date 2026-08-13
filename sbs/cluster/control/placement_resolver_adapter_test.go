package control

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/nosway/namrbd/sbs/cluster/metadata"
)

type recordingPlacementResolverRepository struct {
	extentMappingCalls  int
	replicaSetCalls     int
	nodeMembershipCalls int
}

func (r *recordingPlacementResolverRepository) ListExtentMappings(context.Context, string) ([]metadata.ExtentMappingRecord, error) {
	r.extentMappingCalls++
	return []metadata.ExtentMappingRecord{{
		VolumeID:      "00a1b2c3",
		ExtentID:      1,
		LogicalOffset: 0,
		LengthBytes:   4 << 20,
		PlacementRef:  "pl-1",
	}}, nil
}

func (r *recordingPlacementResolverRepository) ListReplicaSets(context.Context, string) ([]metadata.ReplicaSetState, error) {
	r.replicaSetCalls++
	return []metadata.ReplicaSetState{{
		VolumeID:     "00a1b2c3",
		ReplicaSetID: "rs-1",
		PlacementRef: "pl-1",
		WriteQuorum:  1,
		ReadQuorum:   1,
	}}, nil
}

func (r *recordingPlacementResolverRepository) ListNodeMemberships(context.Context) ([]metadata.NodeMembershipRecord, error) {
	r.nodeMembershipCalls++
	return []metadata.NodeMembershipRecord{{
		NodeID:         "node-a",
		LifecycleState: metadata.NodeLifecycleActive,
		HealthState:    metadata.NodeHealthHealthy,
	}}, nil
}

func (r *recordingPlacementResolverRepository) GetCompatibleAllocationPage(context.Context, string, uint64, uint32, uint32) (metadata.AllocationPageRecord, error) {
	return metadata.AllocationPageRecord{}, nil
}

func (r *recordingPlacementResolverRepository) ListCompatibleAllocationPages(context.Context, string, uint32, uint32) ([]metadata.AllocationPageRecord, error) {
	return nil, nil
}

func (r *recordingPlacementResolverRepository) GetSnapshotAllocationPage(context.Context, string, uint64) (metadata.AllocationPageRecord, error) {
	return metadata.AllocationPageRecord{}, nil
}

func (r *recordingPlacementResolverRepository) GetSnapshotRecord(context.Context, string) (metadata.SnapshotRecord, error) {
	return metadata.SnapshotRecord{}, nil
}

func (r *recordingPlacementResolverRepository) GetCloneRecord(context.Context, string) (metadata.CloneRecord, error) {
	return metadata.CloneRecord{}, nil
}

func (r *recordingPlacementResolverRepository) GetCloneDeltaAllocationPage(context.Context, string, uint64) (metadata.AllocationPageRecord, error) {
	return metadata.AllocationPageRecord{}, nil
}

type blockingNodeMembershipRepository struct {
	recordingPlacementResolverRepository

	mu          sync.Mutex
	calls       int
	blockOnCall int
	started     chan struct{}
	release     chan struct{}
}

func (r *blockingNodeMembershipRepository) ListNodeMemberships(context.Context) ([]metadata.NodeMembershipRecord, error) {
	r.mu.Lock()
	r.calls++
	call := r.calls
	r.mu.Unlock()

	if call == r.blockOnCall {
		close(r.started)
		<-r.release
	}
	return []metadata.NodeMembershipRecord{{
		NodeID:         "node-a",
		LifecycleState: metadata.NodeLifecycleActive,
		HealthState:    metadata.NodeHealthHealthy,
	}}, nil
}

type blockingPlacementMetadataRepository struct {
	recordingPlacementResolverRepository

	mu          sync.Mutex
	blockMethod string
	blockOnCall int
	calls       int
	started     chan struct{}
	release     chan struct{}
	done        chan struct{}
}

func (r *blockingPlacementMetadataRepository) blockIfNeeded(method string) {
	if method != r.blockMethod {
		return
	}
	r.mu.Lock()
	r.calls++
	call := r.calls
	r.mu.Unlock()

	if call == r.blockOnCall {
		close(r.started)
		<-r.release
		close(r.done)
	}
}

func (r *blockingPlacementMetadataRepository) ListExtentMappings(ctx context.Context, volumeID string) ([]metadata.ExtentMappingRecord, error) {
	r.blockIfNeeded("mappings")
	return r.recordingPlacementResolverRepository.ListExtentMappings(ctx, volumeID)
}

func (r *blockingPlacementMetadataRepository) ListReplicaSets(ctx context.Context, volumeID string) ([]metadata.ReplicaSetState, error) {
	r.blockIfNeeded("replica_sets")
	return r.recordingPlacementResolverRepository.ListReplicaSets(ctx, volumeID)
}

func (r *blockingPlacementMetadataRepository) ListNodeMemberships(ctx context.Context) ([]metadata.NodeMembershipRecord, error) {
	r.blockIfNeeded("nodes")
	return r.recordingPlacementResolverRepository.ListNodeMemberships(ctx)
}

func TestCachedPlacementResolverRepositoryCachesPlacementMetadata(t *testing.T) {
	ctx := context.Background()
	repo := &recordingPlacementResolverRepository{}
	now := time.Unix(100, 0)
	cached := newCachedPlacementResolverRepository(repo, time.Second, func() time.Time { return now })

	if _, err := cached.ListExtentMappings(ctx, "00a1b2c3"); err != nil {
		t.Fatalf("ListExtentMappings first: %v", err)
	}
	if _, err := cached.ListReplicaSets(ctx, "00a1b2c3"); err != nil {
		t.Fatalf("ListReplicaSets first: %v", err)
	}
	if _, err := cached.ListNodeMemberships(ctx); err != nil {
		t.Fatalf("ListNodeMemberships first: %v", err)
	}
	if _, err := cached.ListExtentMappings(ctx, "00a1b2c3"); err != nil {
		t.Fatalf("ListExtentMappings second: %v", err)
	}
	if _, err := cached.ListReplicaSets(ctx, "00a1b2c3"); err != nil {
		t.Fatalf("ListReplicaSets second: %v", err)
	}
	if _, err := cached.ListNodeMemberships(ctx); err != nil {
		t.Fatalf("ListNodeMemberships second: %v", err)
	}

	if repo.extentMappingCalls != 1 {
		t.Fatalf("extentMappingCalls=%d want=1", repo.extentMappingCalls)
	}
	if repo.replicaSetCalls != 1 {
		t.Fatalf("replicaSetCalls=%d want=1", repo.replicaSetCalls)
	}
	if repo.nodeMembershipCalls != 1 {
		t.Fatalf("nodeMembershipCalls=%d want=1", repo.nodeMembershipCalls)
	}

	now = now.Add(2 * time.Second)
	if _, err := cached.ListExtentMappings(ctx, "00a1b2c3"); err != nil {
		t.Fatalf("ListExtentMappings after expiry: %v", err)
	}
	if _, err := cached.ListReplicaSets(ctx, "00a1b2c3"); err != nil {
		t.Fatalf("ListReplicaSets after expiry: %v", err)
	}
	if _, err := cached.ListNodeMemberships(ctx); err != nil {
		t.Fatalf("ListNodeMemberships after expiry: %v", err)
	}
	if repo.extentMappingCalls != 2 {
		t.Fatalf("extentMappingCalls after expiry=%d want=2", repo.extentMappingCalls)
	}
	if repo.replicaSetCalls != 2 {
		t.Fatalf("replicaSetCalls after expiry=%d want=2", repo.replicaSetCalls)
	}
	if repo.nodeMembershipCalls != 2 {
		t.Fatalf("nodeMembershipCalls after expiry=%d want=2", repo.nodeMembershipCalls)
	}
}

func TestCachedPlacementResolverRepositoryRefreshesExpiredPlacementMetadataInBackground(t *testing.T) {
	ctx := context.Background()
	volumeID := "00a1b2c3"

	t.Run("extent mappings", func(t *testing.T) {
		repo := &blockingPlacementMetadataRepository{
			blockMethod: "mappings",
			blockOnCall: 2,
			started:     make(chan struct{}),
			release:     make(chan struct{}),
			done:        make(chan struct{}),
		}
		now := time.Unix(100, 0)
		cached := newCachedPlacementResolverRepository(repo, time.Second, func() time.Time { return now })

		if _, err := cached.ListExtentMappings(ctx, volumeID); err != nil {
			t.Fatalf("ListExtentMappings first: %v", err)
		}
		now = now.Add(1500 * time.Millisecond)
		mappings, err := cached.ListExtentMappings(ctx, volumeID)
		if err != nil {
			t.Fatalf("ListExtentMappings stale: %v", err)
		}
		if len(mappings) != 1 || mappings[0].PlacementRef != "pl-1" {
			t.Fatalf("stale mappings=%+v", mappings)
		}
		waitForTestChannel(t, repo.started, "mapping background refresh")
		if _, err := cached.ListExtentMappings(ctx, volumeID); err != nil {
			t.Fatalf("ListExtentMappings while refreshing: %v", err)
		}
		close(repo.release)
		waitForTestChannel(t, repo.done, "mapping refresh done")
		if repo.calls != 2 {
			t.Fatalf("mapping backend calls=%d want=2", repo.calls)
		}
	})

	t.Run("replica sets", func(t *testing.T) {
		repo := &blockingPlacementMetadataRepository{
			blockMethod: "replica_sets",
			blockOnCall: 2,
			started:     make(chan struct{}),
			release:     make(chan struct{}),
			done:        make(chan struct{}),
		}
		now := time.Unix(100, 0)
		cached := newCachedPlacementResolverRepository(repo, time.Second, func() time.Time { return now })

		if _, err := cached.ListReplicaSets(ctx, volumeID); err != nil {
			t.Fatalf("ListReplicaSets first: %v", err)
		}
		now = now.Add(1500 * time.Millisecond)
		replicaSets, err := cached.ListReplicaSets(ctx, volumeID)
		if err != nil {
			t.Fatalf("ListReplicaSets stale: %v", err)
		}
		if len(replicaSets) != 1 || replicaSets[0].PlacementRef != "pl-1" {
			t.Fatalf("stale replica sets=%+v", replicaSets)
		}
		waitForTestChannel(t, repo.started, "replica-set background refresh")
		if _, err := cached.ListReplicaSets(ctx, volumeID); err != nil {
			t.Fatalf("ListReplicaSets while refreshing: %v", err)
		}
		close(repo.release)
		waitForTestChannel(t, repo.done, "replica-set refresh done")
		if repo.calls != 2 {
			t.Fatalf("replica-set backend calls=%d want=2", repo.calls)
		}
	})

	t.Run("nodes", func(t *testing.T) {
		repo := &blockingPlacementMetadataRepository{
			blockMethod: "nodes",
			blockOnCall: 2,
			started:     make(chan struct{}),
			release:     make(chan struct{}),
			done:        make(chan struct{}),
		}
		now := time.Unix(100, 0)
		cached := newCachedPlacementResolverRepository(repo, time.Second, func() time.Time { return now })

		if _, err := cached.ListNodeMemberships(ctx); err != nil {
			t.Fatalf("ListNodeMemberships first: %v", err)
		}
		now = now.Add(1500 * time.Millisecond)
		nodes, err := cached.ListNodeMemberships(ctx)
		if err != nil {
			t.Fatalf("ListNodeMemberships stale: %v", err)
		}
		if len(nodes) != 1 || nodes[0].NodeID != "node-a" {
			t.Fatalf("stale nodes=%+v", nodes)
		}
		waitForTestChannel(t, repo.started, "node background refresh")
		if _, err := cached.ListNodeMemberships(ctx); err != nil {
			t.Fatalf("ListNodeMemberships while refreshing: %v", err)
		}
		close(repo.release)
		waitForTestChannel(t, repo.done, "node refresh done")
		if repo.calls != 2 {
			t.Fatalf("node backend calls=%d want=2", repo.calls)
		}
	})
}

func TestCachedPlacementResolverRepositoryReturnsStaleNodesDuringRefresh(t *testing.T) {
	ctx := context.Background()
	repo := &blockingNodeMembershipRepository{
		blockOnCall: 2,
		started:     make(chan struct{}),
		release:     make(chan struct{}),
	}
	now := time.Unix(100, 0)
	cached := newCachedPlacementResolverRepository(repo, time.Second, func() time.Time { return now })

	if _, err := cached.ListNodeMemberships(ctx); err != nil {
		t.Fatalf("ListNodeMemberships first: %v", err)
	}
	now = now.Add(2 * time.Second)

	refreshErr := make(chan error, 1)
	go func() {
		_, err := cached.ListNodeMemberships(ctx)
		refreshErr <- err
	}()
	<-repo.started

	nodes, err := cached.ListNodeMemberships(ctx)
	if err != nil {
		t.Fatalf("ListNodeMemberships stale during refresh: %v", err)
	}
	if len(nodes) != 1 || nodes[0].NodeID != "node-a" {
		t.Fatalf("stale nodes=%+v", nodes)
	}
	select {
	case err := <-refreshErr:
		t.Fatalf("refresh completed before release: %v", err)
	default:
	}

	close(repo.release)
	if err := <-refreshErr; err != nil {
		t.Fatalf("refresh: %v", err)
	}
	if repo.calls != 2 {
		t.Fatalf("node membership backend calls=%d want=2", repo.calls)
	}
}

func waitForTestChannel(t *testing.T, ch <-chan struct{}, label string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(time.Second):
		t.Fatalf("timeout waiting for %s", label)
	}
}
