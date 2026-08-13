package control

import (
	"context"
	"testing"
	"time"

	"github.com/nosway/namrbd/sbs/cluster/metadata"
	internalv1 "github.com/nosway/namrbd/sbs/internalapi/v1"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type fakePlacementResolverInternalService struct {
	placements []metadata.ResolvedExtentPlacement
	pages      []metadata.ResolvedAllocationPage
	stats      metadata.ResolveExtentPlacementsStats
	err        error

	extentCalls     int
	allocationCalls int
	snapshotCalls   int
	cloneCalls      int
}

func (s *fakePlacementResolverInternalService) ResolveExtentPlacements(context.Context, string, uint64, uint64) ([]metadata.ResolvedExtentPlacement, error) {
	s.extentCalls++
	return s.placements, s.err
}

func (s *fakePlacementResolverInternalService) ResolveExtentPlacementsWithStats(context.Context, string, uint64, uint64) ([]metadata.ResolvedExtentPlacement, metadata.ResolveExtentPlacementsStats, error) {
	s.extentCalls++
	return s.placements, s.stats, s.err
}

func (s *fakePlacementResolverInternalService) ResolveAllocationPages(context.Context, string, uint64, uint64, uint32, uint32) ([]metadata.ResolvedAllocationPage, error) {
	s.allocationCalls++
	return s.pages, s.err
}

func (s *fakePlacementResolverInternalService) ResolveSnapshotAllocationPages(context.Context, string, uint64, uint64, uint32, uint32) ([]metadata.ResolvedAllocationPage, error) {
	s.snapshotCalls++
	return s.pages, s.err
}

func (s *fakePlacementResolverInternalService) ResolveCloneAllocationPages(context.Context, string, uint64, uint64, uint32, uint32) ([]metadata.ResolvedAllocationPage, error) {
	s.cloneCalls++
	return s.pages, s.err
}

func TestServeResolveExtentPlacementsDelegatesToInternalService(t *testing.T) {
	resolver := &fakePlacementResolverInternalService{
		placements: []metadata.ResolvedExtentPlacement{
			{
				ExtentMapping: metadata.ExtentMappingRecord{VolumeID: "00a1b2c3", ExtentID: 1, LogicalOffset: 0, LengthBytes: 4096, PlacementRef: "pl-1"},
				ReplicaSet:    metadata.ReplicaSetState{ReplicaSetID: "rs-1", PlacementRef: "pl-1"},
				Nodes:         map[string]metadata.NodeMembershipRecord{"node-a": {NodeID: "node-a", HealthState: metadata.NodeHealthHealthy}},
			},
		},
	}
	var records []string
	resp, err := ServeResolveExtentPlacements(context.Background(), &internalv1.ResolveExtentPlacementsRequest{
		VolumeId:    "00a1b2c3",
		OffsetBytes: 0,
		LengthBytes: 4096,
	}, resolver, func(class string, _ time.Duration) {
		records = append(records, class)
	})
	if err != nil {
		t.Fatalf("ServeResolveExtentPlacements: %v", err)
	}
	if resolver.extentCalls != 1 {
		t.Fatalf("extent calls=%d want 1", resolver.extentCalls)
	}
	if len(resp.GetPlacements()) != 1 {
		t.Fatalf("placements=%d want 1", len(resp.GetPlacements()))
	}
	if len(records) != 1 || records[0] != "ok" {
		t.Fatalf("records=%v want [ok]", records)
	}
}

func TestServeResolveAllocationPagesDelegatesToInternalService(t *testing.T) {
	resolver := &fakePlacementResolverInternalService{
		pages: []metadata.ResolvedAllocationPage{
			{
				Page:            metadata.AllocationPageRecord{VolumeID: "00a1b2c3", PageNo: 1, PageBytes: 4096, ChunkSizeBytes: 1024},
				RangeStartChunk: 4,
				RangeEndChunk:   8,
				CoversWholePage: true,
			},
		},
	}
	resp, err := ServeResolveAllocationPages(context.Background(), &internalv1.ResolveAllocationPagesRequest{
		VolumeId:       "00a1b2c3",
		OffsetBytes:    4096,
		LengthBytes:    4096,
		PageBytes:      4096,
		ChunkSizeBytes: 1024,
	}, resolver, nil)
	if err != nil {
		t.Fatalf("ServeResolveAllocationPages: %v", err)
	}
	if resolver.allocationCalls != 1 {
		t.Fatalf("allocation calls=%d want 1", resolver.allocationCalls)
	}
	if len(resp.GetAllocationPages()) != 1 {
		t.Fatalf("allocation pages=%d want 1", len(resp.GetAllocationPages()))
	}
}

func TestServeResolveAllocationPagesMapsInvalidGeometry(t *testing.T) {
	resolver := &fakePlacementResolverInternalService{}
	var records []string
	_, err := ServeResolveAllocationPages(context.Background(), &internalv1.ResolveAllocationPagesRequest{
		VolumeId:       "00a1b2c3",
		OffsetBytes:    0,
		LengthBytes:    4096,
		PageBytes:      4096,
		ChunkSizeBytes: 3000,
	}, resolver, func(class string, _ time.Duration) {
		records = append(records, class)
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("status=%v err=%v want invalid argument", status.Code(err), err)
	}
	if resolver.allocationCalls != 0 {
		t.Fatalf("allocation calls=%d want 0", resolver.allocationCalls)
	}
	if len(records) != 1 || records[0] != string(PlacementResolverErrorInvalidArgument) {
		t.Fatalf("records=%v want invalid_argument", records)
	}
}

func TestServeResolveSnapshotAllocationPagesDelegatesToInternalService(t *testing.T) {
	resolver := &fakePlacementResolverInternalService{
		pages: []metadata.ResolvedAllocationPage{
			{
				Page:            metadata.AllocationPageRecord{PageNo: 1, PageBytes: 4096, ChunkSizeBytes: 1024},
				RangeStartChunk: 4,
				RangeEndChunk:   8,
				CoversWholePage: true,
			},
		},
	}
	resp, err := ServeResolveSnapshotAllocationPages(context.Background(), &internalv1.ResolveSnapshotAllocationPagesRequest{
		SnapshotId:     "snap-00a1b2c3-20260521T120000.000000000Z",
		OffsetBytes:    4096,
		LengthBytes:    4096,
		PageBytes:      4096,
		ChunkSizeBytes: 1024,
	}, resolver, nil)
	if err != nil {
		t.Fatalf("ServeResolveSnapshotAllocationPages: %v", err)
	}
	if resolver.snapshotCalls != 1 {
		t.Fatalf("snapshot calls=%d want 1", resolver.snapshotCalls)
	}
	if len(resp.GetAllocationPages()) != 1 {
		t.Fatalf("allocation pages=%d want 1", len(resp.GetAllocationPages()))
	}
}

func TestServeResolveSnapshotAllocationPagesMapsMissingSnapshotID(t *testing.T) {
	resolver := &fakePlacementResolverInternalService{}
	var records []string
	_, err := ServeResolveSnapshotAllocationPages(context.Background(), &internalv1.ResolveSnapshotAllocationPagesRequest{
		OffsetBytes:    0,
		LengthBytes:    4096,
		PageBytes:      4096,
		ChunkSizeBytes: 1024,
	}, resolver, func(class string, _ time.Duration) {
		records = append(records, class)
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("status=%v err=%v want invalid argument", status.Code(err), err)
	}
	if resolver.snapshotCalls != 0 {
		t.Fatalf("snapshot calls=%d want 0", resolver.snapshotCalls)
	}
	if len(records) != 1 || records[0] != string(PlacementResolverErrorInvalidArgument) {
		t.Fatalf("records=%v want invalid_argument", records)
	}
}

func TestServeResolveCloneAllocationPagesDelegatesToInternalService(t *testing.T) {
	resolver := &fakePlacementResolverInternalService{
		pages: []metadata.ResolvedAllocationPage{
			{
				Page:            metadata.AllocationPageRecord{PageNo: 1, PageBytes: 4096, ChunkSizeBytes: 1024},
				RangeStartChunk: 4,
				RangeEndChunk:   8,
				CoversWholePage: true,
			},
		},
	}
	resp, err := ServeResolveCloneAllocationPages(context.Background(), &internalv1.ResolveCloneAllocationPagesRequest{
		CloneId:        "clone-1",
		OffsetBytes:    4096,
		LengthBytes:    4096,
		PageBytes:      4096,
		ChunkSizeBytes: 1024,
	}, resolver, nil)
	if err != nil {
		t.Fatalf("ServeResolveCloneAllocationPages: %v", err)
	}
	if resolver.cloneCalls != 1 {
		t.Fatalf("clone calls=%d want 1", resolver.cloneCalls)
	}
	if len(resp.GetAllocationPages()) != 1 {
		t.Fatalf("allocation pages=%d want 1", len(resp.GetAllocationPages()))
	}
}

func TestServeResolveCloneAllocationPagesMapsMissingCloneID(t *testing.T) {
	resolver := &fakePlacementResolverInternalService{}
	var records []string
	_, err := ServeResolveCloneAllocationPages(context.Background(), &internalv1.ResolveCloneAllocationPagesRequest{
		OffsetBytes:    0,
		LengthBytes:    4096,
		PageBytes:      4096,
		ChunkSizeBytes: 1024,
	}, resolver, func(class string, _ time.Duration) {
		records = append(records, class)
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("status=%v err=%v want invalid argument", status.Code(err), err)
	}
	if resolver.cloneCalls != 0 {
		t.Fatalf("clone calls=%d want 0", resolver.cloneCalls)
	}
	if len(records) != 1 || records[0] != string(PlacementResolverErrorInvalidArgument) {
		t.Fatalf("records=%v want invalid_argument", records)
	}
}
