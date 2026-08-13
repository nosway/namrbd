package replication

import (
	"context"

	"github.com/nosway/namrbd/sbs/cluster/metadata"
)

type failoverStore interface {
	GetVolumeState(ctx context.Context, volumeID string) (metadata.VolumeState, error)
	GetReplicaSet(ctx context.Context, volumeID, replicaSetID string) (metadata.ReplicaSetState, error)
	CommitPrimaryFailover(ctx context.Context, req metadata.CommitPrimaryFailoverRequest) (metadata.VolumeState, metadata.ReplicaSetState, error)
}

type FailoverService struct {
	store failoverStore
}

func NewFailoverService(store failoverStore) *FailoverService {
	return &FailoverService{store: store}
}

func (s *FailoverService) FailoverExtent(ctx context.Context, volumeID, replicaSetID, newPrimaryReplicaID string) (metadata.VolumeState, metadata.ReplicaSetState, error) {
	volumeState, err := s.store.GetVolumeState(ctx, volumeID)
	if err != nil {
		return metadata.VolumeState{}, metadata.ReplicaSetState{}, err
	}
	replicaSet, err := s.store.GetReplicaSet(ctx, volumeID, replicaSetID)
	if err != nil {
		return metadata.VolumeState{}, metadata.ReplicaSetState{}, err
	}
	return s.store.CommitPrimaryFailover(ctx, metadata.CommitPrimaryFailoverRequest{
		VolumeID:                 volumeID,
		ReplicaSetID:             replicaSetID,
		ExpectedVolumeEpoch:      volumeState.Epoch,
		ExpectedReplicaSetEpoch:  replicaSet.Epoch,
		ExpectedPrimaryReplicaID: replicaSet.PrimaryReplicaID,
		NewPrimaryReplicaID:      newPrimaryReplicaID,
	})
}
