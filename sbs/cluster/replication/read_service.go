package replication

import (
	"context"
	"fmt"
	"time"

	"github.com/nosway/namrbd/internal/structuredlog"
)

type ReadService struct {
	coordinator *Coordinator
	reader      replicaReader
}

func NewReadService(coordinator *Coordinator, reader replicaReader) *ReadService {
	return &ReadService{
		coordinator: coordinator,
		reader:      reader,
	}
}

type ReadRequest struct {
	RequestID      string
	VolumeID       string
	CloneID        string
	SnapshotID     string
	OffsetBytes    uint64
	LengthBytes    uint64
	PageBytes      uint32
	ChunkSizeBytes uint32
	Attribution    bool
}

type ReadResponse struct {
	VolumeID     string
	CloneID      string
	SnapshotID   string
	Data         []byte
	ReplicaReads []string
}

func (s *ReadService) ReadClone(ctx context.Context, cloneID string, req ReadRequest) (*ReadResponse, error) {
	req.CloneID = cloneID
	return s.Read(ctx, req)
}

func (s *ReadService) ReadSnapshot(ctx context.Context, snapshotID string, req ReadRequest) (*ReadResponse, error) {
	req.SnapshotID = snapshotID
	return s.Read(ctx, req)
}

func (s *ReadService) Read(ctx context.Context, req ReadRequest) (*ReadResponse, error) {
	started := time.Now()
	var plan *ReadPlan
	var err error
	if req.CloneID != "" && req.SnapshotID != "" {
		return nil, fmt.Errorf("clone_id and snapshot_id are mutually exclusive")
	}
	planStarted := time.Now()
	if req.SnapshotID != "" {
		plan, err = s.coordinator.PlanSnapshotRead(ctx, req.SnapshotID, req.VolumeID, req.OffsetBytes, req.LengthBytes, req.PageBytes, req.ChunkSizeBytes)
	} else if req.CloneID != "" {
		plan, err = s.coordinator.PlanCloneRead(ctx, req.CloneID, req.VolumeID, req.OffsetBytes, req.LengthBytes, req.PageBytes, req.ChunkSizeBytes)
	} else {
		plan, err = s.coordinator.PlanRead(ctx, req.VolumeID, req.OffsetBytes, req.LengthBytes, req.PageBytes, req.ChunkSizeBytes)
	}
	planDuration := time.Since(planStarted)
	if err != nil {
		if req.Attribution {
			structuredlog.Error("sbs.replication", "read_failed", err,
				structuredlog.F("request_id", req.RequestID),
				structuredlog.F("volume_id", req.VolumeID),
				structuredlog.F("clone_id", req.CloneID),
				structuredlog.F("snapshot_id", req.SnapshotID),
				structuredlog.F("offset_bytes", req.OffsetBytes),
				structuredlog.F("length_bytes", req.LengthBytes),
				structuredlog.F("phase", "plan_read"),
				structuredlog.F("duration_ms", time.Since(started).Milliseconds()),
				structuredlog.F("plan_duration_ms", planDuration.Milliseconds()),
			)
		}
		return nil, err
	}
	resp := &ReadResponse{
		VolumeID:     req.VolumeID,
		CloneID:      req.CloneID,
		SnapshotID:   req.SnapshotID,
		Data:         make([]byte, 0),
		ReplicaReads: make([]string, 0, len(plan.Extents)),
	}
	var extentReadDuration time.Duration
	for _, extent := range plan.Extents {
		extentStarted := time.Now()
		payload, replicaID, err := s.reader.ReadExtent(ctx, extent, ReplicaReadRequest{
			RequestID:   req.RequestID,
			VolumeID:    req.VolumeID,
			CloneID:     req.CloneID,
			SnapshotID:  req.SnapshotID,
			OffsetBytes: req.OffsetBytes,
			LengthBytes: req.LengthBytes,
			Attribution: req.Attribution,
		})
		extentReadDuration += time.Since(extentStarted)
		if err != nil {
			if req.Attribution {
				structuredlog.Error("sbs.replication", "read_failed", err,
					structuredlog.F("request_id", req.RequestID),
					structuredlog.F("volume_id", req.VolumeID),
					structuredlog.F("clone_id", req.CloneID),
					structuredlog.F("snapshot_id", req.SnapshotID),
					structuredlog.F("offset_bytes", req.OffsetBytes),
					structuredlog.F("length_bytes", req.LengthBytes),
					structuredlog.F("phase", "replica_read"),
					structuredlog.F("duration_ms", time.Since(started).Milliseconds()),
					structuredlog.F("plan_duration_ms", planDuration.Milliseconds()),
					structuredlog.F("extent_read_duration_ms", extentReadDuration.Milliseconds()),
					structuredlog.F("extent_count", len(plan.Extents)),
				)
			}
			return nil, err
		}
		resp.Data = append(resp.Data, payload...)
		resp.ReplicaReads = append(resp.ReplicaReads, replicaID)
	}
	if req.Attribution {
		structuredlog.Info("sbs.replication", "read_completed",
			structuredlog.F("request_id", req.RequestID),
			structuredlog.F("volume_id", req.VolumeID),
			structuredlog.F("clone_id", req.CloneID),
			structuredlog.F("snapshot_id", req.SnapshotID),
			structuredlog.F("offset_bytes", req.OffsetBytes),
			structuredlog.F("length_bytes", req.LengthBytes),
			structuredlog.F("duration_ms", time.Since(started).Milliseconds()),
			structuredlog.F("plan_duration_ms", planDuration.Milliseconds()),
			structuredlog.F("extent_read_duration_ms", extentReadDuration.Milliseconds()),
			structuredlog.F("extent_count", len(plan.Extents)),
			structuredlog.F("replica_reads", resp.ReplicaReads),
			structuredlog.F("response_bytes", len(resp.Data)),
		)
	}
	return resp, nil
}
