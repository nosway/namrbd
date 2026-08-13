//go:build !enterprise

package ec

import (
	"context"
	"fmt"

	"github.com/nosway/namrbd/gateway/service"
	"github.com/nosway/namrbd/sbs/cluster/metadata"
)

const (
	OperationECWriteFullStripe = "ec_write_full_stripe"
	OperationECWriteRMW        = "ec_write_rmw"
	OperationECCloneWriteRMW   = "ec_clone_write_rmw"
	OperationECDiscard         = "ec_discard"
	OperationECRebuild         = "ec_rebuild"
	OperationECScrub           = "ec_scrub"
	OperationECRebalance       = "ec_rebalance"
	OperationECDrain           = "ec_drain"

	ShardRoleData   = "data"
	ShardRoleCoding = "coding"

	ECReadReasonUnavailableShard  = "unavailable_shard"
	ECReadReasonChecksumMismatch  = "checksum_mismatch"
	ECReadReasonTopologyViolation = "topology_violation"
	ECReadReasonTooManyErasures   = "too_many_erasures"
	ECReadReasonBackendTimeout    = "backend_timeout"

	ECScrubMismatchMissing        = "missing"
	ECScrubMismatchChecksum       = "checksum_mismatch"
	ECScrubMismatchParity         = "parity_mismatch"
	ECScrubMismatchStripeChecksum = "stripe_checksum_mismatch"
	ECScrubMismatchTopology       = "topology_violation"
	ECScrubMismatchBackendTimeout = "backend_timeout"
	ECScrubMismatchMetadata       = "metadata_missing"
)

type MetadataStore interface {
	ListNodeMemberships(context.Context) ([]metadata.NodeMembershipRecord, error)
}

type ShardSession struct {
	NodeID       string
	Client       service.SBSClient
	VolumeHandle string
	GatewayID    string
	HostID       string
	SessionID    string
	AttachmentID string
	Generation   uint64
}

type Service struct{}

type WriteRequest struct {
	Volume       service.VolumeSpec
	Context      service.SBSRequestContext
	Offset       uint64
	Length       uint64
	Data         []byte
	ZeroSemantic bool
}

type WriteResponse struct {
	Revision    uint64
	OperationID string
	ObjectID    string
	StripeID    string
}

type DiscardRequest struct {
	Volume  service.VolumeSpec
	Context service.SBSRequestContext
	Offset  uint64
	Length  uint64
}

type DiscardResponse struct {
	Revision         uint64
	OperationID      string
	StripeID         string
	RetiredECObjects []metadata.RetiredECObjectRef
}

type CloneDeltaCommitter interface {
	CommitCloneDeltaAllocationPages(ctx context.Context, cloneID string, pages []metadata.AllocationPageRecord) error
}

type ReadRequest struct {
	Volume  service.VolumeSpec
	Context service.SBSRequestContext
	Offset  uint64
	Length  uint64
}

type ReadResponse struct {
	Data          []byte
	Degraded      bool
	Reason        string
	MissingShards []uint32
	CorruptShards []uint32
}

type RepairRequest struct {
	Volume           service.VolumeSpec
	Context          service.SBSRequestContext
	ObjectID         string
	StripeID         string
	StripeGeneration uint64
	ShardID          uint32
}

type RepairResponse struct {
	OperationID      string
	ObjectID         string
	StripeID         string
	StripeGeneration uint64
	ShardID          uint32
	NodeID           string
	Zone             string
	StoreID          string
	Checksum         string
	TopologyRevision uint64
}

type ScrubRequest struct {
	Volume           service.VolumeSpec
	Context          service.SBSRequestContext
	ObjectID         string
	StripeID         string
	StripeGeneration uint64
}

type ScrubResponse struct {
	OperationID      string
	ObjectID         string
	StripeID         string
	StripeGeneration uint64
	Healthy          bool
	ParityVerified   bool
	CheckedShards    int
	MissingShards    []uint32
	CorruptShards    []uint32
	Findings         []ScrubFinding
}

type ScrubFinding struct {
	StripeID         string
	ShardID          uint32
	Role             string
	NodeID           string
	StoreID          string
	MismatchKind     string
	ChecksumExpected string
	ChecksumObserved string
	RepairCandidate  bool
}

type RebalanceShardRequest struct {
	Volume           service.VolumeSpec
	Context          service.SBSRequestContext
	ObjectID         string
	StripeID         string
	StripeGeneration uint64
	ShardID          uint32
	TargetNodeID     string
}

type RebalanceShardResponse struct {
	OperationID      string
	ObjectID         string
	StripeID         string
	StripeGeneration uint64
	ShardID          uint32
	SourceNodeID     string
	SourceZone       string
	SourceStoreID    string
	TargetNodeID     string
	TargetZone       string
	TargetStoreID    string
	Checksum         string
	TopologyRevision uint64
}

type DrainPreflightRequest struct {
	Volume           service.VolumeSpec
	StripeID         string
	StripeGeneration uint64
	NodeID           string
	Zone             string
	AllowWeak        bool
}

type DrainPreflightResponse struct {
	StripeID         string
	StripeGeneration uint64
	Blocked          bool
	BlockedReason    string
	Weak             bool
	AffectedShards   []uint32
	Plans            []DrainShardPlan
	ZoneShardCounts  map[string]uint32
}

type DrainRequest struct {
	Volume           service.VolumeSpec
	Context          service.SBSRequestContext
	StripeID         string
	StripeGeneration uint64
	NodeID           string
	Zone             string
	AllowWeak        bool
}

type DrainResponse struct {
	OperationID      string
	ObjectID         string
	StripeID         string
	StripeGeneration uint64
	Weak             bool
	MovedShards      []uint32
	Plans            []DrainShardPlan
	ZoneShardCounts  map[string]uint32
	TopologyRevision uint64
}

type DrainShardPlan struct {
	ShardID        uint32
	Role           string
	SourceNodeID   string
	SourceZone     string
	SourceStoreID  string
	TargetNodeID   string
	TargetZone     string
	TargetStoreID  string
	BlockingReason string
}

type ReachabilityRoot struct {
	Kind  string `json:"kind"`
	ID    string `json:"id"`
	State string `json:"state,omitempty"`
}

type ReachableECObject struct {
	ObjectID         string             `json:"object_id"`
	StripeID         string             `json:"stripe_id,omitempty"`
	StripeGeneration uint64             `json:"stripe_generation,omitempty"`
	Roots            []ReachabilityRoot `json:"roots"`
}

type RetiredECObjectStatus struct {
	ObjectID         string                       `json:"object_id"`
	StripeID         string                       `json:"stripe_id,omitempty"`
	StripeGeneration uint64                       `json:"stripe_generation,omitempty"`
	PhysicalState    metadata.PhysicalObjectState `json:"physical_state"`
	StripeState      metadata.ECStripeState       `json:"stripe_state,omitempty"`
	Protected        bool                         `json:"protected"`
	ProtectingRoots  []ReachabilityRoot           `json:"protecting_roots,omitempty"`
}

type ReachabilityReport struct {
	VolumeID                string                  `json:"volume_id"`
	ReachableObjects        []ReachableECObject     `json:"reachable_objects"`
	RetiredProtected        []RetiredECObjectStatus `json:"retired_protected"`
	RetiredReclaimable      []RetiredECObjectStatus `json:"retired_reclaimable"`
	ReachableObjectCount    int                     `json:"reachable_object_count"`
	RetiredProtectedCount   int                     `json:"retired_protected_count"`
	RetiredReclaimableCount int                     `json:"retired_reclaimable_count"`
}

func NewService(MetadataStore, map[string]ShardSession) *Service {
	return &Service{}
}

func IsECVolume(spec service.VolumeSpec) bool {
	return spec.RedundancyBackend == service.RedundancyBackendEC || spec.ECProfileID != ""
}

func CollectReachability(context.Context, MetadataStore, string) (ReachabilityReport, error) {
	return ReachabilityReport{}, errEnterpriseOnly()
}

func (s *Service) Write(context.Context, WriteRequest) (*WriteResponse, error) {
	return nil, errEnterpriseOnly()
}

func (s *Service) WriteFullStripe(context.Context, WriteRequest) (*WriteResponse, error) {
	return nil, errEnterpriseOnly()
}

func (s *Service) WriteCloneDelta(context.Context, string, WriteRequest, []metadata.ResolvedAllocationPage, CloneDeltaCommitter) (*WriteResponse, error) {
	return nil, errEnterpriseOnly()
}

func (s *Service) Discard(context.Context, DiscardRequest) (*DiscardResponse, error) {
	return nil, errEnterpriseOnly()
}

func (s *Service) Read(context.Context, ReadRequest) (*ReadResponse, error) {
	return nil, errEnterpriseOnly()
}

func (s *Service) ReadFullStripe(context.Context, ReadRequest) (*ReadResponse, error) {
	return nil, errEnterpriseOnly()
}

func (s *Service) ReadFromAllocationPages(context.Context, ReadRequest, []metadata.ResolvedAllocationPage) (*ReadResponse, error) {
	return nil, errEnterpriseOnly()
}

func (s *Service) RepairShard(context.Context, RepairRequest) (*RepairResponse, error) {
	return nil, errEnterpriseOnly()
}

func (s *Service) ScrubStripe(context.Context, ScrubRequest) (*ScrubResponse, error) {
	return nil, errEnterpriseOnly()
}

func (s *Service) RebalanceShard(context.Context, RebalanceShardRequest) (*RebalanceShardResponse, error) {
	return nil, errEnterpriseOnly()
}

func (s *Service) PreflightDrain(context.Context, DrainPreflightRequest) (*DrainPreflightResponse, error) {
	return nil, errEnterpriseOnly()
}

func (s *Service) Drain(context.Context, DrainRequest) (*DrainResponse, error) {
	return nil, errEnterpriseOnly()
}

func errEnterpriseOnly() error {
	return fmt.Errorf("EC backend is Enterprise edition only")
}
