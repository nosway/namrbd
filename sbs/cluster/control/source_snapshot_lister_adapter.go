package control

import (
	"context"
	"fmt"
	"strings"

	adminv1 "github.com/nosway/namrbd/sbs/admin/v1"
	"github.com/nosway/namrbd/sbs/cluster/metadata"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type AdminEndpointSourceSnapshotLister struct {
	client adminv1.AdminServiceClient
}

func NewAdminEndpointSourceSnapshotLister(endpoint string) (*AdminEndpointSourceSnapshotLister, func(), error) {
	endpoint = strings.TrimSpace(endpoint)
	if endpoint == "" {
		return nil, nil, fmt.Errorf("source snapshot lister requires reachable --sbs-admin-endpoint")
	}
	conn, err := grpc.NewClient(endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, nil, fmt.Errorf("dial source snapshot lister endpoint %q: %w", endpoint, err)
	}
	return &AdminEndpointSourceSnapshotLister{client: adminv1.NewAdminServiceClient(conn)}, func() { _ = conn.Close() }, nil
}

func (l *AdminEndpointSourceSnapshotLister) ListSnapshotRecords(ctx context.Context, sourceVolumeID string, includeDeleted bool) ([]metadata.SnapshotRecord, error) {
	if l == nil || l.client == nil {
		return nil, fmt.Errorf("source snapshot lister admin client is required")
	}
	resp, err := l.client.ListSnapshots(ctx, &adminv1.ListSnapshotsRequest{
		SourceVolumeId: strings.TrimSpace(sourceVolumeID),
		IncludeDeleted: includeDeleted,
	})
	if err != nil {
		return nil, err
	}
	out := make([]metadata.SnapshotRecord, 0, len(resp.GetSnapshots()))
	for _, snap := range resp.GetSnapshots() {
		out = append(out, metadata.SnapshotRecord{
			SnapshotID:               snap.GetSnapshotId(),
			SourceVolumeID:           snap.GetSourceVolumeId(),
			SnapshotRootID:           snap.GetSnapshotRootId(),
			State:                    snapshotStateFromAdminProto(snap.GetState()),
			CutVolumeRevision:        snap.GetCutVolumeRevision(),
			AllocationChunkSizeBytes: snap.GetAllocationChunkSizeBytes(),
			AllocationPageSizeBytes:  snap.GetAllocationPageSizeBytes(),
			SourceSizeBytes:          snap.GetSourceSizeBytes(),
			CloneReferenceCount:      snap.GetCloneReferenceCount(),
			ErrorMessage:             snap.GetErrorMessage(),
		})
	}
	return out, nil
}

func snapshotStateFromAdminProto(state adminv1.SnapshotState) metadata.SnapshotState {
	switch state {
	case adminv1.SnapshotState_SNAPSHOT_STATE_CREATING:
		return metadata.SnapshotStateCreating
	case adminv1.SnapshotState_SNAPSHOT_STATE_AVAILABLE:
		return metadata.SnapshotStateAvailable
	case adminv1.SnapshotState_SNAPSHOT_STATE_DELETING:
		return metadata.SnapshotStateDeleting
	case adminv1.SnapshotState_SNAPSHOT_STATE_DELETED:
		return metadata.SnapshotStateDeleted
	case adminv1.SnapshotState_SNAPSHOT_STATE_FAILED:
		return metadata.SnapshotStateFailed
	default:
		return ""
	}
}
