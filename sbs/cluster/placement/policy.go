package placement

import (
	"fmt"
	"sort"
)

const DefaultExtentSizeBytes uint64 = 64 << 20

const (
	TopologyModeLegacy = "legacy"
	TopologyModePrefer = "prefer"
	TopologyModeStrict = "strict"
)

type CandidateNode struct {
	NodeID string
	Zone   string
}

type ReplicaPlan struct {
	NodeID        string
	ReplicaID     string
	FailureDomain string
	Primary       bool
}

type ReplicaSetPlan struct {
	PlacementRef     string
	ReplicaSetID     string
	PrimaryReplicaID string
	Replicas         []ReplicaPlan
	WriteQuorum      uint32
	ReadQuorum       uint32
}

type ExtentPlan struct {
	ExtentID      uint64
	LogicalOffset uint64
	LengthBytes   uint64
	ChunkID       uint64
	ReplicaSet    ReplicaSetPlan
}

type InitialLayout struct {
	ExtentSizeBytes uint64
	Extents         []ExtentPlan
}

type InitialLayoutRequest struct {
	VolumeID          string
	SizeBytes         uint64
	ExtentSizeBytes   uint64
	ReplicationFactor uint32
	Candidates        []CandidateNode
	TopologyMode      string
}

type Policy interface {
	Name() string
	PlanInitialLayout(req InitialLayoutRequest) (InitialLayout, error)
}

type RFSpreadPolicy struct{}

func NewRFSpreadPolicy() RFSpreadPolicy {
	return RFSpreadPolicy{}
}

func (RFSpreadPolicy) Name() string {
	return "rf-spread-v1"
}

func (RFSpreadPolicy) PlanInitialLayout(req InitialLayoutRequest) (InitialLayout, error) {
	if req.VolumeID == "" {
		return InitialLayout{}, fmt.Errorf("volume_id is required")
	}
	if req.SizeBytes == 0 {
		return InitialLayout{}, fmt.Errorf("size_bytes is required")
	}
	if req.ReplicationFactor == 0 {
		return InitialLayout{}, fmt.Errorf("replication_factor is required")
	}
	if len(req.Candidates) < int(req.ReplicationFactor) {
		return InitialLayout{}, fmt.Errorf("need at least %d placement candidates", req.ReplicationFactor)
	}
	extentSize := req.ExtentSizeBytes
	if extentSize == 0 {
		extentSize = DefaultExtentSizeBytes
	}
	candidates := append([]CandidateNode(nil), req.Candidates...)
	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].Zone != candidates[j].Zone {
			return candidates[i].Zone < candidates[j].Zone
		}
		return candidates[i].NodeID < candidates[j].NodeID
	})

	layout := InitialLayout{ExtentSizeBytes: extentSize}
	var offset uint64
	var extentID uint64 = 1
	for offset < req.SizeBytes {
		length := extentSize
		if remaining := req.SizeBytes - offset; remaining < length {
			length = remaining
		}
		selected, err := selectReplicas(candidates, int(req.ReplicationFactor), int(extentID-1), req.TopologyMode)
		if err != nil {
			return InitialLayout{}, err
		}
		replicas := make([]ReplicaPlan, 0, len(selected))
		primaryReplicaID := ""
		for i, node := range selected {
			replicaID := fmt.Sprintf("%s-e%06d-r%02d", req.VolumeID, extentID, i+1)
			primary := i == 0
			if primary {
				primaryReplicaID = replicaID
			}
			replicas = append(replicas, ReplicaPlan{
				NodeID:        node.NodeID,
				ReplicaID:     replicaID,
				FailureDomain: failureDomain(node),
				Primary:       primary,
			})
		}
		layout.Extents = append(layout.Extents, ExtentPlan{
			ExtentID:      extentID,
			LogicalOffset: offset,
			LengthBytes:   length,
			ChunkID:       extentID,
			ReplicaSet: ReplicaSetPlan{
				PlacementRef:     fmt.Sprintf("pl-%06d", extentID),
				ReplicaSetID:     fmt.Sprintf("rs-%06d", extentID),
				PrimaryReplicaID: primaryReplicaID,
				Replicas:         replicas,
				WriteQuorum:      uint32(len(replicas)/2) + 1,
				ReadQuorum:       1,
			},
		})
		offset += length
		extentID++
	}
	return layout, nil
}

func selectReplicas(candidates []CandidateNode, want, rotation int, topologyMode string) ([]CandidateNode, error) {
	if len(candidates) < want {
		return nil, fmt.Errorf("insufficient candidates: have=%d want=%d", len(candidates), want)
	}
	rotated := make([]CandidateNode, 0, len(candidates))
	for i := 0; i < len(candidates); i++ {
		rotated = append(rotated, candidates[(rotation+i)%len(candidates)])
	}
	selected := make([]CandidateNode, 0, want)
	usedZones := make(map[string]struct{})
	if normalizeTopologyMode(topologyMode) != TopologyModeLegacy {
		for _, candidate := range rotated {
			if len(selected) == want {
				break
			}
			if candidate.Zone != "" {
				if _, exists := usedZones[candidate.Zone]; exists {
					continue
				}
				usedZones[candidate.Zone] = struct{}{}
			}
			selected = append(selected, candidate)
		}
		if normalizeTopologyMode(topologyMode) == TopologyModeStrict && len(selected) != want {
			return nil, fmt.Errorf("strict topology requires %d distinct zones; selected=%d", want, len(selected))
		}
	}
	for _, candidate := range rotated {
		if len(selected) == want {
			break
		}
		duplicate := false
		for _, existing := range selected {
			if existing.NodeID == candidate.NodeID {
				duplicate = true
				break
			}
		}
		if !duplicate {
			selected = append(selected, candidate)
		}
	}
	if len(selected) != want {
		return nil, fmt.Errorf("could not select %d replicas", want)
	}
	return selected, nil
}

func normalizeTopologyMode(raw string) string {
	switch raw {
	case TopologyModeStrict:
		return TopologyModeStrict
	case TopologyModePrefer:
		return TopologyModePrefer
	default:
		return TopologyModeLegacy
	}
}

func failureDomain(node CandidateNode) string {
	if node.Zone != "" {
		return node.Zone
	}
	return node.NodeID
}
