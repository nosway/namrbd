package control

import (
	"fmt"

	"github.com/nosway/namrbd/sbs/cluster/metadata"
	internalv1 "github.com/nosway/namrbd/sbs/internalapi/v1"
)

func ResolvedExtentPlacementToProto(rec metadata.ResolvedExtentPlacement) *internalv1.ResolvedExtentPlacement {
	nodes := make([]*internalv1.NodeMembership, 0, len(rec.Nodes))
	for _, node := range rec.Nodes {
		nodes = append(nodes, NodeMembershipToProto(node))
	}
	return &internalv1.ResolvedExtentPlacement{
		ExtentMapping: ExtentMappingToProto(rec.ExtentMapping),
		ReplicaSet:    ReplicaSetToProto(rec.ReplicaSet),
		Nodes:         nodes,
	}
}

func ResolvedExtentPlacementFromProto(rec *internalv1.ResolvedExtentPlacement) (metadata.ResolvedExtentPlacement, error) {
	if rec == nil {
		return metadata.ResolvedExtentPlacement{}, fmt.Errorf("resolved extent placement is required")
	}
	nodes := make(map[string]metadata.NodeMembershipRecord, len(rec.GetNodes()))
	for _, node := range rec.GetNodes() {
		out, err := NodeMembershipFromProto(node)
		if err != nil {
			return metadata.ResolvedExtentPlacement{}, err
		}
		nodes[out.NodeID] = out
	}
	return metadata.ResolvedExtentPlacement{
		ExtentMapping: ExtentMappingFromProto(rec.GetExtentMapping()),
		ReplicaSet:    ReplicaSetFromProto(rec.GetReplicaSet()),
		Nodes:         nodes,
	}, nil
}

func ResolvedAllocationPageToProto(rec metadata.ResolvedAllocationPage) *internalv1.ResolvedAllocationPage {
	return &internalv1.ResolvedAllocationPage{
		Page:            AllocationPageRecordToProto(rec.Page),
		RangeStartChunk: rec.RangeStartChunk,
		RangeEndChunk:   rec.RangeEndChunk,
		CoversWholePage: rec.CoversWholePage,
	}
}

func ResolvedAllocationPageFromProto(rec *internalv1.ResolvedAllocationPage) (metadata.ResolvedAllocationPage, error) {
	if rec == nil {
		return metadata.ResolvedAllocationPage{}, fmt.Errorf("resolved allocation page is required")
	}
	page, err := AllocationPageRecordFromProto(rec.GetPage())
	if err != nil {
		return metadata.ResolvedAllocationPage{}, err
	}
	return metadata.ResolvedAllocationPage{
		Page:            page,
		RangeStartChunk: rec.GetRangeStartChunk(),
		RangeEndChunk:   rec.GetRangeEndChunk(),
		CoversWholePage: rec.GetCoversWholePage(),
	}, nil
}

func AllocationPageRecordToProto(page metadata.AllocationPageRecord) *internalv1.AllocationPage {
	extents := make([]*internalv1.AllocationExtent, 0, len(page.Extents))
	for _, extent := range page.Extents {
		extents = append(extents, &internalv1.AllocationExtent{
			LogicalChunkStart:  extent.LogicalChunkStart,
			ChunkCount:         extent.ChunkCount,
			Kind:               placementApplyKindToProto(extent.Kind),
			PhysicalChunkStart: extent.PhysicalChunkStart,
			BackingRef:         extent.BackingRef,
			Generation:         extent.Generation,
			Checksum:           extent.Checksum,
			Encryption:         payloadEncryptionHeaderToProto(extent.Encryption),
		})
	}
	return &internalv1.AllocationPage{
		VolumeId:       page.VolumeID,
		PageNo:         page.PageNo,
		PageBytes:      page.PageBytes,
		ChunkSizeBytes: page.ChunkSizeBytes,
		Revision:       page.Revision,
		Extents:        extents,
	}
}

func AllocationPageRecordFromProto(page *internalv1.AllocationPage) (metadata.AllocationPageRecord, error) {
	if page == nil {
		return metadata.AllocationPageRecord{}, fmt.Errorf("allocation page is required")
	}
	extents := make([]metadata.AllocationExtentRecord, 0, len(page.GetExtents()))
	for _, extent := range page.GetExtents() {
		if extent == nil {
			return metadata.AllocationPageRecord{}, fmt.Errorf("allocation extent is required")
		}
		kind, err := placementApplyKindFromProto(extent.GetKind())
		if err != nil {
			return metadata.AllocationPageRecord{}, err
		}
		extents = append(extents, metadata.AllocationExtentRecord{
			LogicalChunkStart:  extent.GetLogicalChunkStart(),
			ChunkCount:         extent.GetChunkCount(),
			Kind:               kind,
			PhysicalChunkStart: extent.GetPhysicalChunkStart(),
			BackingRef:         extent.GetBackingRef(),
			Generation:         extent.GetGeneration(),
			Checksum:           extent.GetChecksum(),
			Encryption:         payloadEncryptionHeaderFromProto(extent.GetEncryption()),
		})
	}
	return metadata.AllocationPageRecord{
		VolumeID:       page.GetVolumeId(),
		PageNo:         page.GetPageNo(),
		PageBytes:      page.GetPageBytes(),
		ChunkSizeBytes: page.GetChunkSizeBytes(),
		Revision:       page.GetRevision(),
		Extents:        extents,
	}, nil
}

func ExtentMappingToProto(rec metadata.ExtentMappingRecord) *internalv1.ExtentMapping {
	return &internalv1.ExtentMapping{
		VolumeId:      rec.VolumeID,
		ExtentId:      rec.ExtentID,
		LogicalOffset: rec.LogicalOffset,
		LengthBytes:   rec.LengthBytes,
		ChunkId:       rec.ChunkID,
		PlacementRef:  rec.PlacementRef,
		Revision:      rec.Revision,
	}
}

func ExtentMappingFromProto(rec *internalv1.ExtentMapping) metadata.ExtentMappingRecord {
	if rec == nil {
		return metadata.ExtentMappingRecord{}
	}
	return metadata.ExtentMappingRecord{
		VolumeID:      rec.GetVolumeId(),
		ExtentID:      rec.GetExtentId(),
		LogicalOffset: rec.GetLogicalOffset(),
		LengthBytes:   rec.GetLengthBytes(),
		ChunkID:       rec.GetChunkId(),
		PlacementRef:  rec.GetPlacementRef(),
		Revision:      rec.GetRevision(),
	}
}

func ReplicaSetToProto(rec metadata.ReplicaSetState) *internalv1.ReplicaSet {
	replicas := make([]*internalv1.ReplicaDescriptor, 0, len(rec.Replicas))
	for _, replica := range rec.Replicas {
		replicas = append(replicas, &internalv1.ReplicaDescriptor{
			NodeId:        replica.NodeID,
			ReplicaId:     replica.ReplicaID,
			Role:          replicaRoleToProto(replica.Role),
			FailureDomain: replica.FailureDomain,
		})
	}
	return &internalv1.ReplicaSet{
		ReplicaSetId:     rec.ReplicaSetID,
		VolumeId:         rec.VolumeID,
		PlacementRef:     rec.PlacementRef,
		Epoch:            rec.Epoch,
		Replicas:         replicas,
		PrimaryReplicaId: rec.PrimaryReplicaID,
		WriteQuorum:      rec.WriteQuorum,
		ReadQuorum:       rec.ReadQuorum,
		FailureDomains:   append([]string(nil), rec.FailureDomains...),
	}
}

func ReplicaSetFromProto(rec *internalv1.ReplicaSet) metadata.ReplicaSetState {
	if rec == nil {
		return metadata.ReplicaSetState{}
	}
	replicas := make([]metadata.ReplicaDescriptor, 0, len(rec.GetReplicas()))
	for _, replica := range rec.GetReplicas() {
		replicas = append(replicas, metadata.ReplicaDescriptor{
			NodeID:        replica.GetNodeId(),
			ReplicaID:     replica.GetReplicaId(),
			Role:          replicaRoleFromProto(replica.GetRole()),
			FailureDomain: replica.GetFailureDomain(),
		})
	}
	return metadata.ReplicaSetState{
		ReplicaSetID:     rec.GetReplicaSetId(),
		VolumeID:         rec.GetVolumeId(),
		PlacementRef:     rec.GetPlacementRef(),
		Epoch:            rec.GetEpoch(),
		Replicas:         replicas,
		PrimaryReplicaID: rec.GetPrimaryReplicaId(),
		WriteQuorum:      rec.GetWriteQuorum(),
		ReadQuorum:       rec.GetReadQuorum(),
		FailureDomains:   append([]string(nil), rec.GetFailureDomains()...),
	}
}

func NodeMembershipToProto(rec metadata.NodeMembershipRecord) *internalv1.NodeMembership {
	endpoints := make([]*internalv1.SBSEndpoint, 0, len(rec.SBSEndpoints))
	for _, endpoint := range rec.SBSEndpoints {
		endpoints = append(endpoints, &internalv1.SBSEndpoint{
			Address:    endpoint.Address,
			Port:       uint32(endpoint.Port),
			UseTls:     endpoint.UseTLS,
			ServerName: endpoint.ServerName,
		})
	}
	return &internalv1.NodeMembership{
		NodeId:            rec.NodeID,
		ReplicaId:         rec.ReplicaID,
		LifecycleState:    nodeLifecycleStateToProto(rec.LifecycleState),
		HealthState:       nodeHealthStateToProto(rec.HealthState),
		Zone:              rec.Zone,
		Host:              rec.Host,
		CapacityBytes:     rec.CapacityBytes,
		UsedBytes:         rec.UsedBytes,
		LastHeartbeatUnix: rec.LastHeartbeatUnix,
		Version:           rec.Version,
		Capabilities:      append([]string(nil), rec.Capabilities...),
		AdminHttpEndpoint: rec.AdminHTTPEndpoint,
		SbsEndpoints:      endpoints,
	}
}

func NodeMembershipFromProto(rec *internalv1.NodeMembership) (metadata.NodeMembershipRecord, error) {
	if rec == nil {
		return metadata.NodeMembershipRecord{}, fmt.Errorf("node membership is required")
	}
	endpoints := make([]metadata.SBSEndpoint, 0, len(rec.GetSbsEndpoints()))
	for _, endpoint := range rec.GetSbsEndpoints() {
		endpoints = append(endpoints, metadata.SBSEndpoint{
			Address:    endpoint.GetAddress(),
			Port:       uint16(endpoint.GetPort()),
			UseTLS:     endpoint.GetUseTls(),
			ServerName: endpoint.GetServerName(),
		})
	}
	return metadata.NodeMembershipRecord{
		NodeID:            rec.GetNodeId(),
		ReplicaID:         rec.GetReplicaId(),
		LifecycleState:    nodeLifecycleStateFromProto(rec.GetLifecycleState()),
		HealthState:       nodeHealthStateFromProto(rec.GetHealthState()),
		Zone:              rec.GetZone(),
		Host:              rec.GetHost(),
		CapacityBytes:     rec.GetCapacityBytes(),
		UsedBytes:         rec.GetUsedBytes(),
		LastHeartbeatUnix: rec.GetLastHeartbeatUnix(),
		Version:           rec.GetVersion(),
		Capabilities:      append([]string(nil), rec.GetCapabilities()...),
		AdminHTTPEndpoint: rec.GetAdminHttpEndpoint(),
		SBSEndpoints:      endpoints,
	}, nil
}

func replicaRoleToProto(role metadata.ReplicaRole) internalv1.ReplicaRole {
	switch role {
	case metadata.ReplicaRolePrimary:
		return internalv1.ReplicaRole_REPLICA_ROLE_PRIMARY
	case metadata.ReplicaRoleSecondary:
		return internalv1.ReplicaRole_REPLICA_ROLE_SECONDARY
	default:
		return internalv1.ReplicaRole_REPLICA_ROLE_UNSPECIFIED
	}
}

func replicaRoleFromProto(role internalv1.ReplicaRole) metadata.ReplicaRole {
	switch role {
	case internalv1.ReplicaRole_REPLICA_ROLE_PRIMARY:
		return metadata.ReplicaRolePrimary
	case internalv1.ReplicaRole_REPLICA_ROLE_SECONDARY:
		return metadata.ReplicaRoleSecondary
	default:
		return ""
	}
}

func nodeLifecycleStateToProto(state metadata.NodeLifecycleState) internalv1.NodeLifecycleState {
	switch state {
	case metadata.NodeLifecycleJoining:
		return internalv1.NodeLifecycleState_NODE_LIFECYCLE_STATE_JOINING
	case metadata.NodeLifecycleActive:
		return internalv1.NodeLifecycleState_NODE_LIFECYCLE_STATE_ACTIVE
	case metadata.NodeLifecycleDraining:
		return internalv1.NodeLifecycleState_NODE_LIFECYCLE_STATE_DRAINING
	case metadata.NodeLifecycleRemoved:
		return internalv1.NodeLifecycleState_NODE_LIFECYCLE_STATE_REMOVED
	default:
		return internalv1.NodeLifecycleState_NODE_LIFECYCLE_STATE_UNSPECIFIED
	}
}

func nodeLifecycleStateFromProto(state internalv1.NodeLifecycleState) metadata.NodeLifecycleState {
	switch state {
	case internalv1.NodeLifecycleState_NODE_LIFECYCLE_STATE_JOINING:
		return metadata.NodeLifecycleJoining
	case internalv1.NodeLifecycleState_NODE_LIFECYCLE_STATE_ACTIVE:
		return metadata.NodeLifecycleActive
	case internalv1.NodeLifecycleState_NODE_LIFECYCLE_STATE_DRAINING:
		return metadata.NodeLifecycleDraining
	case internalv1.NodeLifecycleState_NODE_LIFECYCLE_STATE_REMOVED:
		return metadata.NodeLifecycleRemoved
	default:
		return ""
	}
}

func nodeHealthStateToProto(state metadata.NodeHealthState) internalv1.NodeHealthState {
	switch state {
	case metadata.NodeHealthHealthy:
		return internalv1.NodeHealthState_NODE_HEALTH_STATE_HEALTHY
	case metadata.NodeHealthSuspect:
		return internalv1.NodeHealthState_NODE_HEALTH_STATE_SUSPECT
	case metadata.NodeHealthDown:
		return internalv1.NodeHealthState_NODE_HEALTH_STATE_DOWN
	default:
		return internalv1.NodeHealthState_NODE_HEALTH_STATE_UNSPECIFIED
	}
}

func nodeHealthStateFromProto(state internalv1.NodeHealthState) metadata.NodeHealthState {
	switch state {
	case internalv1.NodeHealthState_NODE_HEALTH_STATE_HEALTHY:
		return metadata.NodeHealthHealthy
	case internalv1.NodeHealthState_NODE_HEALTH_STATE_SUSPECT:
		return metadata.NodeHealthSuspect
	case internalv1.NodeHealthState_NODE_HEALTH_STATE_DOWN:
		return metadata.NodeHealthDown
	default:
		return ""
	}
}
