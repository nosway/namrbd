package cluster

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/nosway/namrbd/gateway/service"
	"github.com/nosway/namrbd/internal/structuredlog"
	"github.com/nosway/namrbd/sbs/cluster/control"
	clusterec "github.com/nosway/namrbd/sbs/cluster/ec"
	"github.com/nosway/namrbd/sbs/cluster/metadata"
	"github.com/nosway/namrbd/sbs/cluster/replication"
	phasesecurity "github.com/nosway/namrbd/sbs/cluster/security"
	namrbdversion "github.com/nosway/namrbd/version"
	"github.com/nosway/namrbd/volumeid"
)

const (
	DefaultVolumeCacheTTL        = 30 * time.Second
	DefaultPlacementApplyTimeout = 5 * time.Second
)

type VolumeLookup interface {
	GetVolume(ctx context.Context, volumeID uint64) (service.VolumeSpec, error)
}

type ReplicaTargetAvailabilityProvider interface {
	AvailableReplicaTargetIDs(ctx context.Context, volumeID string) (map[string]struct{}, error)
}

type ReplicaTargetAvailabilityFunc func(ctx context.Context, volumeID string) (map[string]struct{}, error)

func (f ReplicaTargetAvailabilityFunc) AvailableReplicaTargetIDs(ctx context.Context, volumeID string) (map[string]struct{}, error) {
	return f(ctx, volumeID)
}

type volumeStateReader interface {
	GetVolumeState(ctx context.Context, volumeID string) (metadata.VolumeState, error)
}

type volumeStateCommitStore interface {
	GetVolumeState(ctx context.Context, volumeID string) (metadata.VolumeState, error)
	PutVolumeState(ctx context.Context, state metadata.VolumeState) error
}

type metadataIntentStore interface {
	GetVolumeState(ctx context.Context, volumeID string) (metadata.VolumeState, error)
	GetIdempotencyRecord(ctx context.Context, volumeID, idempotencyKey string) (metadata.IdempotencyRecord, error)
	PutIdempotencyRecord(ctx context.Context, rec metadata.IdempotencyRecord) error
	GetMutationOperation(ctx context.Context, volumeID, operationID string) (metadata.MutationOperationRecord, error)
	PutMutationOperation(ctx context.Context, rec metadata.MutationOperationRecord) error
}

type metadataIdempotencyStore interface {
	GetIdempotencyRecord(ctx context.Context, volumeID, idempotencyKey string) (metadata.IdempotencyRecord, error)
	PutIdempotencyRecord(ctx context.Context, rec metadata.IdempotencyRecord) error
}

type metadataMutationOperationStore interface {
	GetMutationOperation(ctx context.Context, volumeID, operationID string) (metadata.MutationOperationRecord, error)
	PutMutationOperation(ctx context.Context, rec metadata.MutationOperationRecord) error
}

type metadataControlRecordStore interface {
	metadataIdempotencyStore
	metadataMutationOperationStore
}

type metadataWriteSessionStore interface {
	volumeStateCommitStore
	metadataControlRecordStore
}

type metadataChunkIDAllocator interface {
	AllocateChunkIDs(ctx context.Context, volumeID string, count uint32) (uint64, error)
}

type metadataChunkIDSequenceStore = metadata.ChunkIDSequenceStore

type metadataWriteMetadataCommitter interface {
	CommitWriteMetadata(ctx context.Context, req metadata.CommitWriteMetadataRequest) (metadata.VolumeState, metadata.IdempotencyRecord, error)
}

type metadataCloneDeltaCommitter interface {
	CommitCloneDeltaAllocationPages(ctx context.Context, cloneID string, pages []metadata.AllocationPageRecord) error
}

type metadataWriteStateCommitter interface {
	CommitWriteState(ctx context.Context, req metadata.CommitWriteStateRequest) (metadata.VolumeState, metadata.IdempotencyRecord, error)
}

type metadataCommittedWriteEffectsApplier interface {
	ApplyCommittedWriteEffects(ctx context.Context, req metadata.ApplyCommittedWriteEffectsRequest) error
}

type metadataPlacementApplyStore = metadata.PlacementApplyAuthority

type metadataPlacementApplyAdapter = control.PlacementApplyAdapter

type metadataCommittedPlacementStore interface {
	metadataPlacementApplyStore
}

type metadataWritePlanningStore = metadata.WritePlanningAuthority

type metadataWriteMetadataStore interface {
	metadataChunkIDAllocator
	metadataWriteMetadataCommitter
}

type metadataExtentMappingResolver interface {
	ListExtentMappings(ctx context.Context, volumeID string) ([]metadata.ExtentMappingRecord, error)
}

type metadataReplicaSetResolver interface {
	ListReplicaSets(ctx context.Context, volumeID string) ([]metadata.ReplicaSetState, error)
}

type metadataNodeMembershipResolver interface {
	ListNodeMemberships(ctx context.Context) ([]metadata.NodeMembershipRecord, error)
}

type metadataAllocationPageReader interface {
	GetCompatibleAllocationPage(ctx context.Context, volumeID string, pageNo uint64, pageBytes, chunkSizeBytes uint32) (metadata.AllocationPageRecord, error)
}

type metadataAllocationPageLister interface {
	ListCompatibleAllocationPages(ctx context.Context, volumeID string, pageBytes, chunkSizeBytes uint32) ([]metadata.AllocationPageRecord, error)
}

type metadataAllocationResolver interface {
	metadataAllocationPageReader
	metadataAllocationPageLister
}

type replicationPlacementResolver interface {
	ResolveExtentPlacements(ctx context.Context, volumeID string, offsetBytes, lengthBytes uint64) ([]metadata.ResolvedExtentPlacement, error)
}

type replicationAllocationResolver interface {
	ResolveAllocationPages(ctx context.Context, volumeID string, offsetBytes, lengthBytes uint64, pageBytes, chunkSizeBytes uint32) ([]metadata.ResolvedAllocationPage, error)
}

type snapshotAllocationResolver interface {
	ResolveSnapshotAllocationPages(ctx context.Context, snapshotID string, offsetBytes, lengthBytes uint64, pageBytes, chunkSizeBytes uint32) ([]metadata.ResolvedAllocationPage, error)
}

type cloneAllocationResolver interface {
	ResolveCloneAllocationPages(ctx context.Context, cloneID string, offsetBytes, lengthBytes uint64, pageBytes, chunkSizeBytes uint32) ([]metadata.ResolvedAllocationPage, error)
}

type metadataSourceSnapshotLister interface {
	ListSnapshotRecords(ctx context.Context, sourceVolumeID string, includeDeleted bool) ([]metadata.SnapshotRecord, error)
}

type Config struct {
	MetadataStateStore                      volumeStateReader
	MetadataWriteSessionStore               metadataWriteSessionStore
	MetadataIntentStore                     metadataIntentStore
	MetadataControlRecordStore              metadataControlRecordStore
	MetadataIdempotencyStore                metadataIdempotencyStore
	MetadataMutationOperationStore          metadataMutationOperationStore
	MetadataChunkIDAllocator                metadataChunkIDAllocator
	MetadataWritePlanningStore              metadataWritePlanningStore
	MetadataChunkIDSequenceStore            metadataChunkIDSequenceStore
	MetadataWriteStateCommitter             metadataWriteStateCommitter
	MetadataCommittedWriteEffectsApplier    metadataCommittedWriteEffectsApplier
	MetadataAllocationPersistStore          metadata.AllocationPersistStore
	MetadataExtentMappingNormalizeStore     metadata.ExtentMappingNormalizeStore
	MetadataPlacementApplyStore             metadataPlacementApplyStore
	MetadataPlacementApplyAdapter           metadataPlacementApplyAdapter
	MetadataPlacementApplyTimeout           time.Duration
	MetadataCommittedPlacementStore         metadataCommittedPlacementStore
	MetadataWriteMetadataCommitter          metadataWriteMetadataCommitter
	MetadataCloneDeltaCommitter             metadataCloneDeltaCommitter
	MetadataWriteMetadataStore              metadataWriteMetadataStore
	MetadataExtentMappingResolver           metadataExtentMappingResolver
	MetadataReplicaSetResolver              metadataReplicaSetResolver
	MetadataNodeMembershipResolver          metadataNodeMembershipResolver
	MetadataPlacementResolver               replicationPlacementResolver
	MetadataAllocationPageReader            metadataAllocationPageReader
	MetadataAllocationPageLister            metadataAllocationPageLister
	MetadataAllocationResolver              metadataAllocationResolver
	MetadataResolvedAllocationResolver      replicationAllocationResolver
	MetadataSourceSnapshotLister            metadataSourceSnapshotLister
	MetadataECStore                         clusterec.MetadataStore
	PreferPageScopedWriteMetadata           bool
	PreferRangeLocalWriteState              bool
	PreferAsyncWriteEffects                 bool
	PreferUnsafeAppendOnlyWriteState        bool
	PreferAppendOnlyServiceWriteEffects     bool
	UnsafeAppendOnlyIntentlessCommit        bool
	PreferPayloadOnlyWrites                 bool
	PromoteZeroPayloadWrites                bool
	ZeroAllocationReadFastPath              bool
	UnsafeZeroNoopSkipIdempotency           bool
	UnsafeZeroReplayFastPath                bool
	ZeroEvidenceCacheTTL                    time.Duration
	PreferQuorumEarlyReplicaWrites          bool
	ReplicaFullChunkWriteParallelism        int
	QuorumEarlyStagedFanoutDelay            time.Duration
	QuorumEarlyBackgroundFanoutLimit        int
	ParallelBeginPlan                       bool
	PhasePReplicatedPayloadEncryption       bool
	PhasePDataKeyID                         string
	PhasePKeyVersion                        uint64
	PhasePKeyAccessLeaseIssuer              phasesecurity.KeyAccessLeaseIssuer
	PhasePDataKeyUnwrapper                  phasesecurity.DataKeyUnwrapper
	PhasePKeyAccessLeaseTTLSeconds          uint64
	ChunkIDAllocationCacheSize              uint32
	WritePlanCacheTTL                       time.Duration
	BeginWriteVolumeStateCacheTTL           time.Duration
	VolumeSpecs                             []service.VolumeSpec
	VolumeLookup                            VolumeLookup
	VolumeCacheTTL                          time.Duration
	ReplicaClients                          map[string]service.SBSClient
	ReplicaTargetAvailabilitySource         ReplicaTargetAvailabilityProvider
	FallbackReplicaTargetAvailabilitySource ReplicaTargetAvailabilityProvider
	GatewayID                               string
	HostID                                  string
	ClientVersion                           string
	SessionPrefix                           string
}

type Client struct {
	stateStore                        volumeStateReader
	coordinator                       *replication.Coordinator
	executor                          *replication.Executor
	volumeLookup                      VolumeLookup
	volumeCacheTTL                    time.Duration
	volumeCache                       map[string]cachedVolumeSpec
	allocationResolver                replicationAllocationResolver
	replicaClients                    map[string]service.SBSClient
	availability                      ReplicaTargetAvailabilityProvider
	fallbackAvailability              ReplicaTargetAvailabilityProvider
	gatewayID                         string
	hostID                            string
	clientVersion                     string
	sessionPrefix                     string
	preferPayloadOnlyWrites           bool
	promoteZeroPayloadWrites          bool
	zeroAllocationReadFastPath        bool
	unsafeZeroNoopSkipIdempotency     bool
	unsafeZeroReplayFastPath          bool
	zeroEvidenceCacheTTL              time.Duration
	preferQuorumEarlyReplicaWrites    bool
	replicaFullChunkWriteParallelism  int
	quorumEarlyStagedFanoutDelay      time.Duration
	quorumEarlyBackgroundFanoutLimit  int
	parallelBeginPlan                 bool
	phasePReplicatedPayloadEncryption bool
	phasePDataKeyID                   string
	phasePKeyVersion                  uint64
	phasePKeyAccessLeaseIssuer        phasesecurity.KeyAccessLeaseIssuer
	phasePDataKeyUnwrapper            phasesecurity.DataKeyUnwrapper
	phasePKeyAccessLeaseTTLSeconds    uint64
	ecStore                           clusterec.MetadataStore
	snapshotAllocationResolver        snapshotAllocationResolver
	cloneAllocationResolver           cloneAllocationResolver
	cloneDeltaCommitter               metadataCloneDeltaCommitter

	mu   sync.RWMutex
	open map[string]openSession

	observedVolumeRevisions map[string]uint64
	zeroEvidenceCache       map[zeroEvidenceCacheKey]cachedZeroEvidencePage
}

type metadataResolverAdapter struct {
	mappingResolver    metadataExtentMappingResolver
	replicaSetResolver metadataReplicaSetResolver
	nodeResolver       metadataNodeMembershipResolver
	allocationResolver metadataAllocationPageReader
}

type derivedWriteStateCommitter struct {
	stateStore  volumeStateCommitStore
	intentStore metadataIdempotencyStore
}

type unsafeAppendOnlyWriteStateCommitter struct {
	stateStore  volumeStateReader
	intentStore metadataIdempotencyStore

	mu           sync.Mutex
	lastRevision uint64
}

type derivedCommittedWriteEffectsApplier struct {
	mutationStore    metadataMutationOperationStore
	placement        metadataPlacementApplyAdapter
	allocationReader metadataAllocationPageReader
}

type resolvedAllocationPageReader struct {
	resolver replicationAllocationResolver
}

type composedPlacementApplyStore struct {
	allocationStore    metadata.AllocationPersistStore
	extentMappingStore metadata.ExtentMappingNormalizeStore
}

type composedWritePlanningStore struct {
	sequenceStore       metadataChunkIDSequenceStore
	placementApplyStore metadataPlacementApplyStore
}

type cachedVolumeSpec struct {
	spec      service.VolumeSpec
	expiresAt time.Time
}

type zeroEvidenceCacheKey struct {
	volumeID       string
	pageNo         uint64
	pageBytes      uint32
	chunkSizeBytes uint32
}

type cachedZeroEvidencePage struct {
	page      metadata.AllocationPageRecord
	expiresAt time.Time
}

type openSession struct {
	handle       string
	attachmentID string
	generation   uint64
	replicas     map[string]replication.RemoteReplica
}

type derivedIntentStore struct {
	stateStore    volumeStateReader
	idempotency   metadataIdempotencyStore
	mutationStore metadataMutationOperationStore
}

func NewClient(cfg Config) (*Client, error) {
	if cfg.MetadataWriteSessionStore != nil {
		if cfg.MetadataStateStore == nil {
			cfg.MetadataStateStore = cfg.MetadataWriteSessionStore
		}
		if cfg.MetadataControlRecordStore == nil {
			cfg.MetadataControlRecordStore = cfg.MetadataWriteSessionStore
		}
		if cfg.MetadataIntentStore == nil {
			if inferred, ok := any(cfg.MetadataWriteSessionStore).(metadataIntentStore); ok {
				cfg.MetadataIntentStore = inferred
			}
		}
	}
	if cfg.MetadataStateStore == nil {
		return nil, fmt.Errorf("metadata state store is required")
	}
	if cfg.MetadataWritePlanningStore != nil {
		if cfg.MetadataChunkIDSequenceStore == nil {
			cfg.MetadataChunkIDSequenceStore = cfg.MetadataWritePlanningStore
		}
		if cfg.MetadataPlacementApplyStore == nil {
			cfg.MetadataPlacementApplyStore = cfg.MetadataWritePlanningStore
		}
		if cfg.MetadataCommittedPlacementStore == nil {
			cfg.MetadataCommittedPlacementStore = cfg.MetadataWritePlanningStore
		}
	}
	if cfg.MetadataPlacementApplyStore != nil && cfg.MetadataAllocationPersistStore == nil {
		cfg.MetadataAllocationPersistStore = cfg.MetadataPlacementApplyStore
	}
	if cfg.MetadataPlacementApplyStore != nil && cfg.MetadataExtentMappingNormalizeStore == nil {
		cfg.MetadataExtentMappingNormalizeStore = cfg.MetadataPlacementApplyStore
	}
	if cfg.MetadataPlacementApplyStore != nil && cfg.MetadataCommittedPlacementStore == nil {
		cfg.MetadataCommittedPlacementStore = cfg.MetadataPlacementApplyStore
	}
	if cfg.MetadataControlRecordStore != nil {
		if cfg.MetadataIdempotencyStore == nil {
			cfg.MetadataIdempotencyStore = cfg.MetadataControlRecordStore
		}
		if cfg.MetadataMutationOperationStore == nil {
			cfg.MetadataMutationOperationStore = cfg.MetadataControlRecordStore
		}
		if cfg.MetadataIntentStore == nil {
			if inferred, ok := any(cfg.MetadataControlRecordStore).(metadataIntentStore); ok {
				cfg.MetadataIntentStore = inferred
			}
		}
	}
	idempotencyStore := cfg.MetadataIdempotencyStore
	mutationStore := cfg.MetadataMutationOperationStore
	if cfg.MetadataIntentStore != nil {
		if idempotencyStore == nil {
			idempotencyStore = cfg.MetadataIntentStore
		}
		if mutationStore == nil {
			mutationStore = cfg.MetadataIntentStore
		}
	}
	if idempotencyStore == nil {
		return nil, fmt.Errorf("metadata idempotency store is required")
	}
	if mutationStore == nil {
		return nil, fmt.Errorf("metadata mutation lifecycle store is required")
	}
	intentStore := cfg.MetadataIntentStore
	if intentStore == nil {
		intentStore = derivedIntentStore{
			stateStore:    cfg.MetadataStateStore,
			idempotency:   idempotencyStore,
			mutationStore: mutationStore,
		}
	}
	chunkAllocator := cfg.MetadataChunkIDAllocator
	chunkIDSequenceStore := cfg.MetadataChunkIDSequenceStore
	stateCommitter := cfg.MetadataWriteStateCommitter
	effectsApplier := cfg.MetadataCommittedWriteEffectsApplier
	writeCommitter := cfg.MetadataWriteMetadataCommitter
	cloneDeltaCommitter := cfg.MetadataCloneDeltaCommitter
	if chunkIDSequenceStore == nil {
		if inferred, ok := cfg.MetadataStateStore.(metadataChunkIDSequenceStore); ok {
			chunkIDSequenceStore = inferred
		}
	}
	if chunkIDSequenceStore == nil {
		if inferred, ok := intentStore.(metadataChunkIDSequenceStore); ok {
			chunkIDSequenceStore = inferred
		}
	}
	if stateCommitter == nil {
		if inferred, ok := cfg.MetadataStateStore.(volumeStateCommitStore); ok {
			stateCommitter = derivedWriteStateCommitter{
				stateStore:  inferred,
				intentStore: idempotencyStore,
			}
		}
	}
	if cfg.MetadataWriteMetadataStore != nil {
		if chunkIDSequenceStore == nil {
			if inferred, ok := cfg.MetadataWriteMetadataStore.(metadataChunkIDSequenceStore); ok {
				chunkIDSequenceStore = inferred
			}
		}
		if chunkAllocator == nil {
			chunkAllocator = cfg.MetadataWriteMetadataStore
		}
		if stateCommitter == nil {
			if inferred, ok := cfg.MetadataWriteMetadataStore.(metadataWriteStateCommitter); ok {
				stateCommitter = inferred
			}
		}
		if effectsApplier == nil {
			if inferred, ok := cfg.MetadataWriteMetadataStore.(metadataCommittedWriteEffectsApplier); ok {
				effectsApplier = inferred
			}
		}
		if writeCommitter == nil {
			writeCommitter = cfg.MetadataWriteMetadataStore
		}
		if cloneDeltaCommitter == nil {
			if inferred, ok := cfg.MetadataWriteMetadataStore.(metadataCloneDeltaCommitter); ok {
				cloneDeltaCommitter = inferred
			}
		}
	}
	if chunkAllocator == nil && chunkIDSequenceStore != nil {
		chunkAllocator = control.NewRepositoryBackedChunkIDAllocatorAdapter(chunkIDSequenceStore)
	}
	if chunkAllocator == nil {
		return nil, fmt.Errorf("metadata chunk id allocator is required")
	}
	if cfg.ChunkIDAllocationCacheSize > 0 {
		chunkAllocator = newCachedChunkIDAllocator(chunkAllocator, cfg.ChunkIDAllocationCacheSize)
	}
	if cfg.MetadataPlacementResolver == nil && cfg.MetadataExtentMappingResolver == nil {
		return nil, fmt.Errorf("metadata extent mapping resolver is required")
	}
	if cfg.MetadataPlacementResolver == nil && cfg.MetadataReplicaSetResolver == nil {
		return nil, fmt.Errorf("metadata replica set resolver is required")
	}
	if cfg.MetadataPlacementResolver == nil && cfg.MetadataNodeMembershipResolver == nil {
		return nil, fmt.Errorf("metadata node membership resolver is required")
	}
	allocationPageReader := cfg.MetadataAllocationPageReader
	allocationPageLister := cfg.MetadataAllocationPageLister
	if cfg.MetadataAllocationResolver != nil {
		if allocationPageReader == nil {
			allocationPageReader = cfg.MetadataAllocationResolver
		}
		if allocationPageLister == nil {
			allocationPageLister = cfg.MetadataAllocationResolver
		}
	}
	if allocationPageReader == nil && cfg.MetadataResolvedAllocationResolver != nil {
		allocationPageReader = resolvedAllocationPageReader{resolver: cfg.MetadataResolvedAllocationResolver}
	}
	allocationPersistStore := cfg.MetadataAllocationPersistStore
	if cfg.MetadataCommittedPlacementStore != nil {
		allocationPersistStore = cfg.MetadataCommittedPlacementStore
	}
	if allocationPersistStore == nil && allocationPageReader != nil {
		if inferred, ok := allocationPageReader.(metadata.AllocationPersistStore); ok {
			allocationPersistStore = inferred
		}
	}
	extentMappingNormalizeStore := cfg.MetadataExtentMappingNormalizeStore
	if cfg.MetadataCommittedPlacementStore != nil {
		extentMappingNormalizeStore = cfg.MetadataCommittedPlacementStore
	}
	if extentMappingNormalizeStore == nil && cfg.MetadataExtentMappingResolver != nil {
		if inferred, ok := cfg.MetadataExtentMappingResolver.(metadata.ExtentMappingNormalizeStore); ok {
			extentMappingNormalizeStore = inferred
		}
	}
	placementApplyStore := cfg.MetadataPlacementApplyStore
	if placementApplyStore == nil && allocationPersistStore != nil && extentMappingNormalizeStore != nil {
		placementApplyStore = composedPlacementApplyStore{
			allocationStore:    allocationPersistStore,
			extentMappingStore: extentMappingNormalizeStore,
		}
	}
	if cfg.MetadataWritePlanningStore == nil && chunkIDSequenceStore != nil && placementApplyStore != nil {
		cfg.MetadataWritePlanningStore = composedWritePlanningStore{
			sequenceStore:       chunkIDSequenceStore,
			placementApplyStore: placementApplyStore,
		}
	}
	if cfg.MetadataCommittedPlacementStore == nil && placementApplyStore != nil {
		cfg.MetadataCommittedPlacementStore = placementApplyStore
	}
	placementApplyAdapter := cfg.MetadataPlacementApplyAdapter
	if placementApplyAdapter == nil && placementApplyStore != nil {
		placementApplyAdapter = control.NewRepositoryBackedPlacementApplyAdapter(placementApplyStore)
	}
	placementApplyTimeout := cfg.MetadataPlacementApplyTimeout
	if placementApplyTimeout == 0 {
		placementApplyTimeout = DefaultPlacementApplyTimeout
	}
	if placementApplyAdapter != nil && placementApplyTimeout > 0 {
		placementApplyAdapter = control.NewTimeoutPlacementApplyAdapter(placementApplyAdapter, placementApplyTimeout)
	}
	if effectsApplier == nil {
		if placementApplyAdapter != nil {
			effectsApplier = derivedCommittedWriteEffectsApplier{
				mutationStore:    mutationStore,
				placement:        placementApplyAdapter,
				allocationReader: allocationPageReader,
			}
		}
	}
	if cfg.PreferUnsafeAppendOnlyWriteState {
		if stateCommitter == nil {
			return nil, fmt.Errorf("unsafe append-only write state requires a metadata write state committer")
		}
		stateCommitter = &unsafeAppendOnlyWriteStateCommitter{
			stateStore:  cfg.MetadataStateStore,
			intentStore: idempotencyStore,
		}
	}
	if writeCommitter == nil && (stateCommitter == nil || effectsApplier == nil) {
		return nil, fmt.Errorf("metadata write metadata committer or split state/effects commit capabilities are required")
	}
	if cfg.GatewayID == "" {
		return nil, fmt.Errorf("gateway id is required")
	}
	if len(cfg.ReplicaClients) == 0 {
		return nil, fmt.Errorf("replica clients are required")
	}
	if cfg.SessionPrefix == "" {
		cfg.SessionPrefix = "cluster"
	}
	if cfg.VolumeCacheTTL == 0 {
		cfg.VolumeCacheTTL = DefaultVolumeCacheTTL
	}

	specs := make(map[string]cachedVolumeSpec, len(cfg.VolumeSpecs))
	var expiresAt time.Time
	if cfg.VolumeLookup != nil {
		expiresAt = time.Now().Add(cfg.VolumeCacheTTL)
	}
	for _, spec := range cfg.VolumeSpecs {
		spec = service.NormalizeVolumeSpec(spec)
		volumeID := service.CanonicalVolumeID(uint64(spec.ID))
		specs[volumeID] = cachedVolumeSpec{spec: spec, expiresAt: expiresAt}
	}
	extentMappingResolver := cfg.MetadataExtentMappingResolver
	replicaSetResolver := cfg.MetadataReplicaSetResolver
	nodeMembershipResolver := cfg.MetadataNodeMembershipResolver
	sourceSnapshotLister := cfg.MetadataSourceSnapshotLister
	if cfg.WritePlanCacheTTL > 0 && extentMappingResolver != nil && replicaSetResolver != nil && nodeMembershipResolver != nil {
		cachedPlanInputs := newCachedPlanMetadataResolver(extentMappingResolver, replicaSetResolver, nodeMembershipResolver, sourceSnapshotLister, cfg.WritePlanCacheTTL)
		extentMappingResolver = cachedPlanInputs
		replicaSetResolver = cachedPlanInputs
		nodeMembershipResolver = cachedPlanInputs
		if sourceSnapshotLister != nil {
			sourceSnapshotLister = cachedPlanInputs
		}
		cfg.MetadataPlacementResolver = nil
	}
	var placementResolver replicationPlacementResolver = cfg.MetadataPlacementResolver
	var allocationResolver replicationAllocationResolver = cfg.MetadataResolvedAllocationResolver
	var snapshotResolver snapshotAllocationResolver
	var cloneResolver cloneAllocationResolver
	if inferred, ok := any(cfg.MetadataResolvedAllocationResolver).(snapshotAllocationResolver); ok {
		snapshotResolver = inferred
	}
	if inferred, ok := any(cfg.MetadataResolvedAllocationResolver).(cloneAllocationResolver); ok {
		cloneResolver = inferred
	}
	var fallbackResolver *metadata.Service
	if placementResolver == nil || allocationResolver == nil {
		fallbackResolver = metadata.NewServiceWithDependencies(metadataResolverAdapter{
			mappingResolver:    extentMappingResolver,
			replicaSetResolver: replicaSetResolver,
			nodeResolver:       nodeMembershipResolver,
		}, metadataResolverAdapter{allocationResolver: allocationPageReader}, allocationPageLister)
		if placementResolver == nil {
			placementResolver = fallbackResolver
		}
		if allocationResolver == nil && allocationPageReader != nil {
			allocationResolver = fallbackResolver
		}
	}
	for _, candidate := range []any{
		snapshotResolver,
		cloneResolver,
		allocationResolver,
		cfg.MetadataPlacementResolver,
		placementResolver,
		fallbackResolver,
	} {
		if snapshotResolver != nil {
			break
		}
		if inferred, ok := candidate.(snapshotAllocationResolver); ok {
			snapshotResolver = inferred
		}
	}
	for _, candidate := range []any{
		cloneResolver,
		snapshotResolver,
		allocationResolver,
		cfg.MetadataPlacementResolver,
		placementResolver,
		fallbackResolver,
	} {
		if cloneResolver != nil {
			break
		}
		if inferred, ok := candidate.(cloneAllocationResolver); ok {
			cloneResolver = inferred
		}
	}
	if snapshotResolver == nil && allocationPageReader != nil {
		snapshotResolver = metadata.NewServiceWithDependencies(metadataResolverAdapter{
			mappingResolver:    extentMappingResolver,
			replicaSetResolver: replicaSetResolver,
			nodeResolver:       nodeMembershipResolver,
		}, metadataResolverAdapter{allocationResolver: allocationPageReader}, allocationPageLister)
	}
	if cloneDeltaCommitter == nil {
		if inferred, ok := allocationResolver.(metadataCloneDeltaCommitter); ok {
			cloneDeltaCommitter = inferred
		}
	}
	if cfg.PhasePReplicatedPayloadEncryption && cfg.PreferPayloadOnlyWrites {
		return nil, fmt.Errorf("phase p replicated payload encryption requires metadata-backed writes")
	}
	coordinator := replication.NewCoordinator(placementResolver, allocationResolver).WithWritePlanCacheTTL(cfg.WritePlanCacheTTL)
	if sourceSnapshotLister != nil {
		coordinator = coordinator.WithSourceSnapshotLister(sourceSnapshotLister)
	}
	ecStore := cfg.MetadataECStore
	if ecStore == nil {
		for _, candidate := range []any{
			cfg.MetadataStateStore,
			cfg.MetadataWriteSessionStore,
			cfg.MetadataWriteMetadataStore,
			cfg.MetadataCommittedPlacementStore,
			cfg.MetadataAllocationPageReader,
		} {
			if inferred, ok := candidate.(clusterec.MetadataStore); ok {
				ecStore = inferred
				break
			}
		}
	}
	if cfg.BeginWriteVolumeStateCacheTTL > 0 {
		if !cfg.PreferAppendOnlyServiceWriteEffects {
			return nil, fmt.Errorf("begin write volume-state cache requires append-only service write effects")
		}
		intentStore = newCachedBeginWriteVolumeStateIntentStore(intentStore, cfg.BeginWriteVolumeStateCacheTTL)
	}
	if cfg.UnsafeAppendOnlyIntentlessCommit && !cfg.PreferAppendOnlyServiceWriteEffects {
		return nil, fmt.Errorf("unsafe append-only intentless commit requires append-only service write effects")
	}

	executor := replication.NewExecutorWithStores(intentStore, coordinator, chunkAllocator, stateCommitter, effectsApplier, writeCommitter)
	if cloneDeltaCommitter != nil {
		executor = executor.WithCloneDeltaMetadataCommitter(cloneDeltaCommitter)
	}
	executor = executor.
		WithPageScopedWriteMetadata(cfg.PreferPageScopedWriteMetadata).
		WithRangeLocalWriteState(cfg.PreferRangeLocalWriteState).
		WithAppendOnlyServiceWriteEffects(cfg.PreferAppendOnlyServiceWriteEffects).
		WithAsyncWriteEffects(cfg.PreferAsyncWriteEffects).
		WithParallelBeginPlan(cfg.ParallelBeginPlan).
		WithAppendOnlyMissingWriteIntent(cfg.UnsafeAppendOnlyIntentlessCommit)

	clientVersion := strings.TrimSpace(cfg.ClientVersion)
	if clientVersion == "" {
		clientVersion = namrbdversion.ProductVersion()
	}

	return &Client{
		stateStore:                        cfg.MetadataStateStore,
		coordinator:                       coordinator,
		executor:                          executor,
		volumeLookup:                      cfg.VolumeLookup,
		volumeCacheTTL:                    cfg.VolumeCacheTTL,
		volumeCache:                       specs,
		allocationResolver:                allocationResolver,
		replicaClients:                    cloneReplicaClients(cfg.ReplicaClients),
		availability:                      cfg.ReplicaTargetAvailabilitySource,
		fallbackAvailability:              cfg.FallbackReplicaTargetAvailabilitySource,
		gatewayID:                         cfg.GatewayID,
		hostID:                            cfg.HostID,
		clientVersion:                     clientVersion,
		sessionPrefix:                     cfg.SessionPrefix,
		preferPayloadOnlyWrites:           cfg.PreferPayloadOnlyWrites,
		promoteZeroPayloadWrites:          cfg.PromoteZeroPayloadWrites,
		zeroAllocationReadFastPath:        cfg.ZeroAllocationReadFastPath,
		unsafeZeroNoopSkipIdempotency:     cfg.UnsafeZeroNoopSkipIdempotency,
		unsafeZeroReplayFastPath:          cfg.UnsafeZeroReplayFastPath,
		zeroEvidenceCacheTTL:              cfg.ZeroEvidenceCacheTTL,
		preferQuorumEarlyReplicaWrites:    cfg.PreferQuorumEarlyReplicaWrites,
		replicaFullChunkWriteParallelism:  cfg.ReplicaFullChunkWriteParallelism,
		quorumEarlyStagedFanoutDelay:      cfg.QuorumEarlyStagedFanoutDelay,
		quorumEarlyBackgroundFanoutLimit:  cfg.QuorumEarlyBackgroundFanoutLimit,
		parallelBeginPlan:                 cfg.ParallelBeginPlan,
		phasePReplicatedPayloadEncryption: cfg.PhasePReplicatedPayloadEncryption,
		phasePDataKeyID:                   strings.TrimSpace(cfg.PhasePDataKeyID),
		phasePKeyVersion:                  cfg.PhasePKeyVersion,
		phasePKeyAccessLeaseIssuer:        cfg.PhasePKeyAccessLeaseIssuer,
		phasePDataKeyUnwrapper:            cfg.PhasePDataKeyUnwrapper,
		phasePKeyAccessLeaseTTLSeconds:    cfg.PhasePKeyAccessLeaseTTLSeconds,
		ecStore:                           ecStore,
		snapshotAllocationResolver:        snapshotResolver,
		cloneAllocationResolver:           cloneResolver,
		cloneDeltaCommitter:               cloneDeltaCommitter,
		open:                              make(map[string]openSession),
		observedVolumeRevisions:           make(map[string]uint64),
		zeroEvidenceCache:                 make(map[zeroEvidenceCacheKey]cachedZeroEvidencePage),
	}, nil
}

func (c *Client) rememberVolumeRevision(volumeID string, revision uint64) {
	if volumeID == "" || revision == 0 {
		return
	}
	c.mu.Lock()
	if revision > c.observedVolumeRevisions[volumeID] {
		c.observedVolumeRevisions[volumeID] = revision
	}
	c.mu.Unlock()
}

func (c *Client) observedVolumeRevision(volumeID string) uint64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.observedVolumeRevisions[volumeID]
}

func (c derivedWriteStateCommitter) CommitWriteState(ctx context.Context, req metadata.CommitWriteStateRequest) (metadata.VolumeState, metadata.IdempotencyRecord, error) {
	state, err := c.stateStore.GetVolumeState(ctx, req.VolumeID)
	if err != nil {
		return metadata.VolumeState{}, metadata.IdempotencyRecord{}, err
	}
	if state.Epoch != req.ExpectedEpoch || state.Revision != req.ExpectedRevision {
		return metadata.VolumeState{}, metadata.IdempotencyRecord{}, metadata.ErrCASConflict
	}
	record, err := c.intentStore.GetIdempotencyRecord(ctx, req.VolumeID, req.IdempotencyKey)
	if err != nil {
		return metadata.VolumeState{}, metadata.IdempotencyRecord{}, err
	}
	if record.ResultState != req.ExpectedIdempotencyState {
		return metadata.VolumeState{}, metadata.IdempotencyRecord{}, metadata.ErrCASConflict
	}
	state.Revision = req.CommittedRevision
	record.Revision = req.CommittedRevision
	record.ResultState = metadata.IdempotencyCommitted
	if err := c.stateStore.PutVolumeState(ctx, state); err != nil {
		return metadata.VolumeState{}, metadata.IdempotencyRecord{}, err
	}
	if err := c.intentStore.PutIdempotencyRecord(ctx, record); err != nil {
		return metadata.VolumeState{}, metadata.IdempotencyRecord{}, err
	}
	return state, record, nil
}

func (c *unsafeAppendOnlyWriteStateCommitter) CommitWriteState(ctx context.Context, req metadata.CommitWriteStateRequest) (metadata.VolumeState, metadata.IdempotencyRecord, error) {
	state, err := c.stateStore.GetVolumeState(ctx, req.VolumeID)
	if err != nil {
		return metadata.VolumeState{}, metadata.IdempotencyRecord{}, err
	}
	if state.Epoch != req.ExpectedEpoch {
		return metadata.VolumeState{}, metadata.IdempotencyRecord{}, metadata.ErrCASConflict
	}
	record, err := c.intentStore.GetIdempotencyRecord(ctx, req.VolumeID, req.IdempotencyKey)
	if err != nil {
		return metadata.VolumeState{}, metadata.IdempotencyRecord{}, err
	}
	if record.ResultState != req.ExpectedIdempotencyState {
		return metadata.VolumeState{}, metadata.IdempotencyRecord{}, metadata.ErrCASConflict
	}
	revision := c.nextRevision(req.CommittedRevision)
	state.Revision = revision
	record.Revision = revision
	record.ResultState = metadata.IdempotencyCommitted
	if err := c.intentStore.PutIdempotencyRecord(ctx, record); err != nil {
		return metadata.VolumeState{}, metadata.IdempotencyRecord{}, err
	}
	structuredlog.Info("sbs.cluster.client", "write_metadata_state_append_only_committed",
		structuredlog.F("volume_id", req.VolumeID),
		structuredlog.F("expected_revision", req.ExpectedRevision),
		structuredlog.F("committed_revision", revision),
		structuredlog.F("idempotency_key", req.IdempotencyKey),
	)
	return state, record, nil
}

func (c *unsafeAppendOnlyWriteStateCommitter) nextRevision(floor uint64) uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	next := uint64(time.Now().UnixNano())
	if next < floor {
		next = floor
	}
	if next <= c.lastRevision {
		next = c.lastRevision + 1
	}
	c.lastRevision = next
	return next
}

func (s composedPlacementApplyStore) PutAllocationPage(ctx context.Context, rec metadata.AllocationPageRecord) error {
	return s.allocationStore.PutAllocationPage(ctx, rec)
}

func (s composedPlacementApplyStore) GetExtentMapping(ctx context.Context, volumeID string, extentID uint64) (metadata.ExtentMappingRecord, error) {
	return s.extentMappingStore.GetExtentMapping(ctx, volumeID, extentID)
}

func (s composedPlacementApplyStore) PutExtentMapping(ctx context.Context, rec metadata.ExtentMappingRecord) error {
	return s.extentMappingStore.PutExtentMapping(ctx, rec)
}

func (s composedWritePlanningStore) GetNextChunkID(ctx context.Context, volumeID string) (uint64, error) {
	return s.sequenceStore.GetNextChunkID(ctx, volumeID)
}

func (s composedWritePlanningStore) PutNextChunkID(ctx context.Context, volumeID string, nextID uint64) error {
	return s.sequenceStore.PutNextChunkID(ctx, volumeID, nextID)
}

func (s composedWritePlanningStore) PutAllocationPage(ctx context.Context, rec metadata.AllocationPageRecord) error {
	return s.placementApplyStore.PutAllocationPage(ctx, rec)
}

func (s composedWritePlanningStore) GetExtentMapping(ctx context.Context, volumeID string, extentID uint64) (metadata.ExtentMappingRecord, error) {
	return s.placementApplyStore.GetExtentMapping(ctx, volumeID, extentID)
}

func (s composedWritePlanningStore) PutExtentMapping(ctx context.Context, rec metadata.ExtentMappingRecord) error {
	return s.placementApplyStore.PutExtentMapping(ctx, rec)
}

func (a derivedIntentStore) GetVolumeState(ctx context.Context, volumeID string) (metadata.VolumeState, error) {
	return a.stateStore.GetVolumeState(ctx, volumeID)
}

func (a derivedIntentStore) GetIdempotencyRecord(ctx context.Context, volumeID, idempotencyKey string) (metadata.IdempotencyRecord, error) {
	return a.idempotency.GetIdempotencyRecord(ctx, volumeID, idempotencyKey)
}

func (a derivedIntentStore) PutIdempotencyRecord(ctx context.Context, rec metadata.IdempotencyRecord) error {
	return a.idempotency.PutIdempotencyRecord(ctx, rec)
}

func (a derivedIntentStore) GetMutationOperation(ctx context.Context, volumeID, operationID string) (metadata.MutationOperationRecord, error) {
	return a.mutationStore.GetMutationOperation(ctx, volumeID, operationID)
}

func (a derivedIntentStore) PutMutationOperation(ctx context.Context, rec metadata.MutationOperationRecord) error {
	return a.mutationStore.PutMutationOperation(ctx, rec)
}

func (c derivedCommittedWriteEffectsApplier) ApplyCommittedWriteEffects(ctx context.Context, req metadata.ApplyCommittedWriteEffectsRequest) error {
	if req.MutationOperationID != "" {
		operation, err := c.mutationStore.GetMutationOperation(ctx, req.VolumeID, req.MutationOperationID)
		if err != nil {
			return err
		}
		if req.MatchesCommittedMutationOperation(operation) {
			logPlacementApplyReplay("pre_apply", req)
			return nil
		}
		if operation.State != req.ExpectedMutationState {
			return metadata.ErrCASConflict
		}
	}
	pages, err := c.mergedAllocationPagesForPlacementApply(ctx, req)
	if err != nil {
		return err
	}
	if err := c.placement.ApplyPlacementChanges(ctx, metadata.PlacementApplyRequest{
		VolumeID:                req.VolumeID,
		CommittedRevision:       req.CommittedRevision,
		AllocationPages:         pages,
		NormalizeExtentIDs:      req.NormalizeExtentMappings,
		RetiredPhysicalChunkIDs: req.RetiredPhysicalChunkIDs,
	}); err != nil {
		return err
	}
	if req.MutationOperationID != "" {
		operation, err := c.mutationStore.GetMutationOperation(ctx, req.VolumeID, req.MutationOperationID)
		if err != nil {
			return err
		}
		if req.MatchesCommittedMutationOperation(operation) {
			logPlacementApplyReplay("post_apply", req)
			return nil
		}
		if operation.State != req.ExpectedMutationState {
			return metadata.ErrCASConflict
		}
		operation.State = metadata.MutationOperationCommitted
		operation.AllocationRevision = req.CommittedRevision
		operation.AffectedExtentIDs = append([]uint64(nil), req.AffectedExtentIDs...)
		operation.AffectedPageNos = append([]uint64(nil), req.AffectedPageNos...)
		operation.RetiredPhysicalChunkIDs = append([]uint64(nil), req.RetiredPhysicalChunkIDs...)
		operation.LastUpdatedAtUnix = time.Now().Unix()
		if err := c.mutationStore.PutMutationOperation(ctx, operation); err != nil {
			return err
		}
	}
	return nil
}

func (c derivedCommittedWriteEffectsApplier) mergedAllocationPagesForPlacementApply(ctx context.Context, req metadata.ApplyCommittedWriteEffectsRequest) ([]metadata.AllocationPageRecord, error) {
	pages := cloneMetadataAllocationPages(req.AllocationPages)
	if c.allocationReader == nil || len(pages) == 0 {
		return pages, nil
	}
	retired := make(map[uint64]struct{}, len(req.RetiredPhysicalChunkIDs))
	for _, chunkID := range req.RetiredPhysicalChunkIDs {
		if chunkID != 0 {
			retired[chunkID] = struct{}{}
		}
	}
	for i := range pages {
		page := pages[i]
		if page.PageBytes == 0 || page.ChunkSizeBytes == 0 || page.PageBytes%page.ChunkSizeBytes != 0 {
			return nil, fmt.Errorf("invalid allocation page geometry: page_no=%d page_bytes=%d chunk_size_bytes=%d", page.PageNo, page.PageBytes, page.ChunkSizeBytes)
		}
		current, err := c.allocationReader.GetCompatibleAllocationPage(ctx, req.VolumeID, page.PageNo, page.PageBytes, page.ChunkSizeBytes)
		if err != nil {
			if errors.Is(err, metadata.ErrNotFound) {
				continue
			}
			return nil, err
		}
		merged, err := mergeAllocationPageEffects(current, page, retired)
		if err != nil {
			return nil, err
		}
		pages[i] = merged
	}
	return pages, nil
}

func cloneMetadataAllocationPages(pages []metadata.AllocationPageRecord) []metadata.AllocationPageRecord {
	if len(pages) == 0 {
		return nil
	}
	out := make([]metadata.AllocationPageRecord, len(pages))
	for i, page := range pages {
		out[i] = page
		out[i].Extents = append([]metadata.AllocationExtentRecord(nil), page.Extents...)
	}
	return out
}

func (r resolvedAllocationPageReader) GetCompatibleAllocationPage(ctx context.Context, volumeID string, pageNo uint64, pageBytes, chunkSizeBytes uint32) (metadata.AllocationPageRecord, error) {
	if r.resolver == nil {
		return metadata.AllocationPageRecord{}, metadata.ErrNotFound
	}
	if pageBytes == 0 || chunkSizeBytes == 0 || pageBytes%chunkSizeBytes != 0 {
		return metadata.AllocationPageRecord{}, fmt.Errorf("invalid allocation page geometry: page_no=%d page_bytes=%d chunk_size_bytes=%d", pageNo, pageBytes, chunkSizeBytes)
	}
	if pageNo > ^uint64(0)/uint64(pageBytes) {
		return metadata.AllocationPageRecord{}, fmt.Errorf("allocation page offset overflows: page_no=%d page_bytes=%d", pageNo, pageBytes)
	}
	offsetBytes := pageNo * uint64(pageBytes)
	pages, err := r.resolver.ResolveAllocationPages(ctx, volumeID, offsetBytes, uint64(pageBytes), pageBytes, chunkSizeBytes)
	if err != nil {
		return metadata.AllocationPageRecord{}, err
	}
	for _, page := range pages {
		if page.Page.PageNo == pageNo && page.Page.PageBytes == pageBytes && page.Page.ChunkSizeBytes == chunkSizeBytes {
			return page.Page, nil
		}
	}
	return metadata.AllocationPageRecord{}, metadata.ErrNotFound
}

func mergeAllocationPageEffects(current, incoming metadata.AllocationPageRecord, retired map[uint64]struct{}) (metadata.AllocationPageRecord, error) {
	if current.PageBytes != incoming.PageBytes || current.ChunkSizeBytes != incoming.ChunkSizeBytes {
		return metadata.AllocationPageRecord{}, fmt.Errorf("allocation page geometry mismatch: volume_id=%s page_no=%d current_page_bytes=%d current_chunk_size_bytes=%d page_bytes=%d chunk_size_bytes=%d",
			incoming.VolumeID, incoming.PageNo, current.PageBytes, current.ChunkSizeBytes, incoming.PageBytes, incoming.ChunkSizeBytes)
	}
	currentIDs, currentHeaders, err := expandMetadataAllocationPageWithHeaders(current)
	if err != nil {
		return metadata.AllocationPageRecord{}, err
	}
	incomingIDs, incomingHeaders, err := expandMetadataAllocationPageWithHeaders(incoming)
	if err != nil {
		return metadata.AllocationPageRecord{}, err
	}
	for i := 0; i < len(currentIDs) && i < len(incomingIDs); i++ {
		if incomingIDs[i] != 0 {
			if currentIDs[i] == 0 || currentIDs[i] == incomingIDs[i] || current.Revision <= incoming.Revision {
				currentIDs[i] = incomingIDs[i]
				currentHeaders[i] = cloneMetadataPayloadEncryptionHeader(incomingHeaders[i])
				continue
			}
			if _, ok := retired[currentIDs[i]]; ok {
				currentIDs[i] = incomingIDs[i]
				currentHeaders[i] = cloneMetadataPayloadEncryptionHeader(incomingHeaders[i])
			}
			continue
		}
		if _, ok := retired[currentIDs[i]]; ok {
			currentIDs[i] = 0
			currentHeaders[i] = nil
		}
	}
	pageStartChunk := incoming.PageNo * uint64(incoming.PageBytes/incoming.ChunkSizeBytes)
	current.VolumeID = incoming.VolumeID
	current.PageNo = incoming.PageNo
	current.PageBytes = incoming.PageBytes
	current.ChunkSizeBytes = incoming.ChunkSizeBytes
	current.Extents = compressMetadataAllocationPageWithHeaders(pageStartChunk, currentIDs, currentHeaders)
	return current, nil
}

func expandMetadataAllocationPage(page metadata.AllocationPageRecord) ([]uint64, error) {
	ids, _, err := expandMetadataAllocationPageWithHeaders(page)
	return ids, err
}

func expandMetadataAllocationPageWithHeaders(page metadata.AllocationPageRecord) ([]uint64, []*metadata.PayloadEncryptionHeader, error) {
	if page.PageBytes == 0 || page.ChunkSizeBytes == 0 || page.PageBytes%page.ChunkSizeBytes != 0 {
		return nil, nil, fmt.Errorf("invalid allocation page geometry: page_no=%d page_bytes=%d chunk_size_bytes=%d", page.PageNo, page.PageBytes, page.ChunkSizeBytes)
	}
	chunksPerPage := uint64(page.PageBytes / page.ChunkSizeBytes)
	pageStartChunk := page.PageNo * chunksPerPage
	pageEndChunk := pageStartChunk + chunksPerPage
	out := make([]uint64, int(chunksPerPage))
	headers := make([]*metadata.PayloadEncryptionHeader, int(chunksPerPage))
	for _, extent := range page.Extents {
		start := extent.LogicalChunkStart
		end := start + uint64(extent.ChunkCount)
		if extent.ChunkCount == 0 {
			return nil, nil, fmt.Errorf("allocation extent has zero chunks: page_no=%d logical_chunk_start=%d", page.PageNo, start)
		}
		if start < pageStartChunk || end > pageEndChunk {
			return nil, nil, fmt.Errorf("allocation extent out of page bounds: page_no=%d start=%d end=%d", page.PageNo, start, end)
		}
		for logicalChunk := start; logicalChunk < end; logicalChunk++ {
			index := logicalChunk - pageStartChunk
			if extent.Kind == metadata.AllocationKindData {
				out[index] = extent.PhysicalChunkStart + (logicalChunk - start)
				headers[index] = cloneMetadataPayloadEncryptionHeader(extent.Encryption)
				continue
			}
			out[index] = 0
			headers[index] = nil
		}
	}
	return out, headers, nil
}

func compressMetadataAllocationPage(pageStartChunk uint64, physicalChunkIDs []uint64) []metadata.AllocationExtentRecord {
	return compressMetadataAllocationPageWithHeaders(pageStartChunk, physicalChunkIDs, nil)
}

func compressMetadataAllocationPageWithHeaders(pageStartChunk uint64, physicalChunkIDs []uint64, encryptionHeaders []*metadata.PayloadEncryptionHeader) []metadata.AllocationExtentRecord {
	if len(physicalChunkIDs) == 0 {
		return nil
	}
	out := make([]metadata.AllocationExtentRecord, 0, len(physicalChunkIDs))
	for i := 0; i < len(physicalChunkIDs); {
		logicalStart := pageStartChunk + uint64(i)
		physicalStart := physicalChunkIDs[i]
		if physicalStart == 0 {
			j := i + 1
			for j < len(physicalChunkIDs) && physicalChunkIDs[j] == 0 {
				j++
			}
			out = append(out, metadata.AllocationExtentRecord{
				LogicalChunkStart: logicalStart,
				ChunkCount:        uint32(j - i),
				Kind:              metadata.AllocationKindZero,
			})
			i = j
			continue
		}
		j := i + 1
		header := metadataPayloadEncryptionHeaderAt(encryptionHeaders, i)
		for j < len(physicalChunkIDs) &&
			physicalChunkIDs[j] == physicalStart+uint64(j-i) &&
			sameMetadataPayloadEncryptionHeader(header, metadataPayloadEncryptionHeaderAt(encryptionHeaders, j)) {
			j++
		}
		out = append(out, metadata.AllocationExtentRecord{
			LogicalChunkStart:  logicalStart,
			ChunkCount:         uint32(j - i),
			Kind:               metadata.AllocationKindData,
			PhysicalChunkStart: physicalStart,
			Encryption:         cloneMetadataPayloadEncryptionHeader(header),
		})
		i = j
	}
	return out
}

func metadataPayloadEncryptionHeaderAt(headers []*metadata.PayloadEncryptionHeader, index int) *metadata.PayloadEncryptionHeader {
	if index < 0 || index >= len(headers) {
		return nil
	}
	return headers[index]
}

func cloneMetadataPayloadEncryptionHeader(header *metadata.PayloadEncryptionHeader) *metadata.PayloadEncryptionHeader {
	if header == nil {
		return nil
	}
	cloned := *header
	return &cloned
}

func sameMetadataPayloadEncryptionHeader(a, b *metadata.PayloadEncryptionHeader) bool {
	if a == nil || b == nil {
		return a == b
	}
	return *a == *b
}

func logPlacementApplyReplay(outcome string, req metadata.ApplyCommittedWriteEffectsRequest) {
	structuredlog.Info("sbs.cluster.client", "placement_apply_replayed",
		structuredlog.F("outcome", outcome),
		structuredlog.F("volume_id", req.VolumeID),
		structuredlog.F("committed_revision", req.CommittedRevision),
		structuredlog.F("mutation_operation_id", req.MutationOperationID),
		structuredlog.F("affected_extent_count", len(req.AffectedExtentIDs)),
		structuredlog.F("affected_page_count", len(req.AffectedPageNos)),
		structuredlog.F("retired_chunk_count", len(req.RetiredPhysicalChunkIDs)),
	)
}

func (a metadataResolverAdapter) ListExtentMappings(ctx context.Context, volumeID string) ([]metadata.ExtentMappingRecord, error) {
	return a.mappingResolver.ListExtentMappings(ctx, volumeID)
}

func (a metadataResolverAdapter) ListReplicaSets(ctx context.Context, volumeID string) ([]metadata.ReplicaSetState, error) {
	return a.replicaSetResolver.ListReplicaSets(ctx, volumeID)
}

func (a metadataResolverAdapter) ListNodeMemberships(ctx context.Context) ([]metadata.NodeMembershipRecord, error) {
	return a.nodeResolver.ListNodeMemberships(ctx)
}

func (a metadataResolverAdapter) GetCompatibleAllocationPage(ctx context.Context, volumeID string, pageNo uint64, pageBytes, chunkSizeBytes uint32) (metadata.AllocationPageRecord, error) {
	if a.allocationResolver == nil {
		return metadata.AllocationPageRecord{}, fmt.Errorf("allocation resolver is not configured")
	}
	return a.allocationResolver.GetCompatibleAllocationPage(ctx, volumeID, pageNo, pageBytes, chunkSizeBytes)
}

func (c *Client) OpenVolume(ctx context.Context, req *service.OpenVolumeRequest) (*service.OpenVolumeResponse, error) {
	if req == nil {
		return nil, badRequest("nil request")
	}
	if err := req.Validate(); err != nil {
		return nil, badRequest(err.Error())
	}
	spec, err := c.lookupVolume(ctx, req.VolumeID)
	if err != nil {
		structuredlog.Error("sbs.cluster.client", "open_volume_failed", err,
			structuredlog.F("volume_id", req.VolumeID),
			structuredlog.F("attachment_id", req.Context.AttachmentID),
			structuredlog.F("generation", req.Context.Generation),
			structuredlog.F("gateway_id", c.gatewayID),
			structuredlog.F("host_id", c.hostID),
		)
		return nil, err
	}

	replicaClients, err := c.availableReplicaClients(ctx, req.VolumeID)
	if err != nil {
		return nil, err
	}
	replicas, err := replication.OpenReplicaSessions(ctx, replicaClients, replication.OpenReplicaSessionsRequest{
		VolumeID:      req.VolumeID,
		GatewayID:     c.gatewayID,
		HostID:        c.hostID,
		ClientVersion: c.clientVersion,
		AttachmentID:  req.Context.AttachmentID,
		Generation:    req.Context.Generation,
		SessionPrefix: c.sessionPrefix,
		AccessMode:    req.AccessMode,
	})
	if err != nil {
		return nil, err
	}

	handle := volumeHandle(req.VolumeID, req.Context.AttachmentID, req.Context.Generation)
	c.mu.Lock()
	c.open[req.VolumeID] = openSession{
		handle:       handle,
		attachmentID: req.Context.AttachmentID,
		generation:   req.Context.Generation,
		replicas:     replicas,
	}
	c.mu.Unlock()
	structuredlog.Info("sbs.cluster.client", "open_volume_completed",
		structuredlog.F("volume_id", req.VolumeID),
		structuredlog.F("volume_handle", handle),
		structuredlog.F("attachment_id", req.Context.AttachmentID),
		structuredlog.F("generation", req.Context.Generation),
		structuredlog.F("gateway_id", c.gatewayID),
		structuredlog.F("host_id", c.hostID),
		structuredlog.F("replica_count", len(replicas)),
	)

	state, err := c.stateStore.GetVolumeState(ctx, req.VolumeID)
	if err != nil {
		return nil, err
	}
	c.rememberVolumeRevision(req.VolumeID, state.Revision)
	return &service.OpenVolumeResponse{
		Status:         "ok",
		VolumeHandle:   handle,
		VolumeID:       req.VolumeID,
		VolumeRevision: state.Revision,
		Profile:        profileFromSpec(spec),
		ServerVersion:  c.clientVersion,
	}, nil
}

func (c *Client) CloseVolume(ctx context.Context, req *service.CloseVolumeRequest) (*service.CloseVolumeResponse, error) {
	if req == nil {
		return nil, badRequest("nil request")
	}
	if err := req.Validate(); err != nil {
		return nil, badRequest(err.Error())
	}

	current, err := c.requireOpen(req.VolumeID, req.VolumeHandle, req.Context)
	if err != nil {
		return nil, err
	}
	for _, replica := range current.replicas {
		_, closeErr := replica.Client.CloseVolume(ctx, &service.CloseVolumeRequest{
			VolumeID:     replica.VolumeID,
			VolumeHandle: replica.VolumeHandle,
			Context: service.SBSRequestContext{
				RequestID:    fmt.Sprintf("cluster-close-%s-%s", replica.VolumeID, replica.ReplicaID),
				GatewayID:    replica.GatewayID,
				HostID:       replica.HostID,
				SessionID:    replica.SessionID,
				AttachmentID: replica.AttachmentID,
				Generation:   replica.Generation,
			},
		})
		if closeErr != nil {
			return nil, closeErr
		}
	}

	c.mu.Lock()
	delete(c.open, req.VolumeID)
	c.mu.Unlock()
	return &service.CloseVolumeResponse{Status: "ok"}, nil
}

func (c *Client) GetVolumeProfile(ctx context.Context, req *service.GetVolumeProfileRequest) (*service.GetVolumeProfileResponse, error) {
	if req == nil {
		return nil, badRequest("nil request")
	}
	if err := req.Validate(); err != nil {
		return nil, badRequest(err.Error())
	}
	spec, err := c.lookupVolume(ctx, req.VolumeID)
	if err != nil {
		return nil, err
	}
	return &service.GetVolumeProfileResponse{
		VolumeID: req.VolumeID,
		Profile:  profileFromSpec(spec),
	}, nil
}

func (c *Client) GetVolumeStatus(ctx context.Context, req *service.GetVolumeStatusRequest) (*service.GetVolumeStatusResponse, error) {
	if req == nil {
		return nil, badRequest("nil request")
	}
	if err := req.Validate(); err != nil {
		return nil, badRequest(err.Error())
	}
	if _, err := c.lookupVolume(ctx, req.VolumeID); err != nil {
		return nil, err
	}
	state, err := c.stateStore.GetVolumeState(ctx, req.VolumeID)
	if err != nil {
		return nil, err
	}
	c.rememberVolumeRevision(req.VolumeID, state.Revision)
	sbsState, readable, writable := toSBSVolumeStatus(state.Status)
	return &service.GetVolumeStatusResponse{
		VolumeID:       req.VolumeID,
		State:          sbsState,
		Readable:       readable,
		Writable:       writable,
		VolumeRevision: state.Revision,
	}, nil
}

func (c *Client) phasePReplicaEncryptionConfig() replication.PhasePReplicaEncryptionConfig {
	return replication.PhasePReplicaEncryptionConfig{
		Enabled:                  c.phasePReplicatedPayloadEncryption,
		DataKeyID:                c.phasePDataKeyID,
		KeyVersion:               c.phasePKeyVersion,
		KeyAccessLeaseRequired:   c.phasePReplicatedPayloadEncryption,
		KeyAccessLeaseIssuer:     c.phasePKeyAccessLeaseIssuer,
		DataKeyUnwrapper:         c.phasePDataKeyUnwrapper,
		KeyAccessLeaseTTLSeconds: c.phasePKeyAccessLeaseTTLSeconds,
		KeyAccessLeaseIssuedTo:   c.gatewayID,
	}
}

func (c *Client) newRemoteReplicaWriter(replicas map[string]replication.RemoteReplica) *replication.RemoteReplicaWriter {
	configure := func(writer *replication.RemoteReplicaWriter) *replication.RemoteReplicaWriter {
		return writer.
			WithQuorumEarlyReturn(c.preferQuorumEarlyReplicaWrites).
			WithQuorumEarlyStagedFanoutDelay(c.quorumEarlyStagedFanoutDelay).
			WithQuorumEarlyBackgroundFanoutLimit(c.quorumEarlyBackgroundFanoutLimit).
			WithMaxParallelChunkWrites(c.replicaFullChunkWriteParallelism)
	}
	if c.phasePReplicatedPayloadEncryption {
		return configure(replication.NewEncryptedRemoteReplicaWriterForPhaseP(replicas, c.phasePReplicaEncryptionConfig()))
	}
	return configure(replication.NewRemoteReplicaWriter(replicas))
}

func (c *Client) newRemoteReplicaReader(replicas map[string]replication.RemoteReplica) *replication.RemoteReplicaReader {
	if c.phasePReplicatedPayloadEncryption {
		return replication.NewEncryptedRemoteReplicaReaderForPhaseP(replicas, c.phasePReplicaEncryptionConfig())
	}
	return replication.NewRemoteReplicaReader(replicas)
}

func (c *Client) Read(ctx context.Context, req *service.ReadRequest) (*service.ReadResponse, error) {
	return c.readWithReadView(ctx, "", "", req)
}

func (c *Client) ReadClone(ctx context.Context, cloneID string, req *service.ReadRequest) (*service.ReadResponse, error) {
	cloneID = strings.TrimSpace(cloneID)
	if cloneID == "" {
		return nil, badRequest("clone_id is required")
	}
	return c.readWithReadView(ctx, "", cloneID, req)
}

func (c *Client) ReadSnapshot(ctx context.Context, snapshotID string, req *service.ReadRequest) (*service.ReadResponse, error) {
	snapshotID = strings.TrimSpace(snapshotID)
	if snapshotID == "" {
		return nil, badRequest("snapshot_id is required")
	}
	return c.readWithReadView(ctx, snapshotID, "", req)
}

func (c *Client) readWithReadView(ctx context.Context, snapshotID, cloneID string, req *service.ReadRequest) (*service.ReadResponse, error) {
	if req == nil {
		return nil, badRequest("nil request")
	}
	if snapshotID != "" && cloneID != "" {
		return nil, badRequest("snapshot_id and clone_id are mutually exclusive")
	}
	if err := req.Validate(); err != nil {
		return nil, badRequest(err.Error())
	}
	started := time.Now()
	requireOpenStarted := time.Now()
	current, err := c.requireOpen(req.VolumeID, req.VolumeHandle, req.Context)
	requireOpenDuration := time.Since(requireOpenStarted)
	if err != nil {
		return nil, err
	}
	lookupVolumeStarted := time.Now()
	spec, err := c.lookupVolume(ctx, req.VolumeID)
	lookupVolumeDuration := time.Since(lookupVolumeStarted)
	if err != nil {
		return nil, err
	}
	if clusterec.IsECVolume(spec) {
		ecSvc, err := c.ecService(ctx, current)
		if err != nil {
			return nil, err
		}
		ecReq := clusterec.ReadRequest{
			Volume:  spec,
			Context: req.Context,
			Offset:  req.OffsetBytes,
			Length:  req.LengthBytes,
		}
		if snapshotID != "" {
			if c.snapshotAllocationResolver == nil {
				return nil, fmt.Errorf("ec snapshot allocation resolver is not configured")
			}
			allocationPages, err := c.snapshotAllocationResolver.ResolveSnapshotAllocationPages(ctx, snapshotID, req.OffsetBytes, req.LengthBytes, spec.ExtentPageBytes, spec.ChunkSizeBytes)
			if err != nil {
				structuredlog.Error("sbs.cluster.client", "ec_snapshot_allocation_resolve_failed", err,
					structuredlog.F("request_id", req.Context.RequestID),
					structuredlog.F("trace_id", req.Context.TraceID),
					structuredlog.F("volume_id", req.VolumeID),
					structuredlog.F("snapshot_id", snapshotID),
					structuredlog.F("attachment_id", req.Context.AttachmentID),
					structuredlog.F("generation", req.Context.Generation),
					structuredlog.F("offset_bytes", req.OffsetBytes),
					structuredlog.F("length_bytes", req.LengthBytes),
				)
				return nil, err
			}
			resp, err := ecSvc.ReadFromAllocationPages(ctx, ecReq, allocationPages)
			if err != nil {
				structuredlog.Error("sbs.cluster.client", "ec_snapshot_read_failed", err,
					structuredlog.F("request_id", req.Context.RequestID),
					structuredlog.F("trace_id", req.Context.TraceID),
					structuredlog.F("volume_id", req.VolumeID),
					structuredlog.F("snapshot_id", snapshotID),
					structuredlog.F("attachment_id", req.Context.AttachmentID),
					structuredlog.F("generation", req.Context.Generation),
					structuredlog.F("offset_bytes", req.OffsetBytes),
					structuredlog.F("length_bytes", req.LengthBytes),
				)
				return nil, err
			}
			structuredlog.Info("sbs.cluster.client", "ec_snapshot_read_completed",
				structuredlog.F("request_id", req.Context.RequestID),
				structuredlog.F("trace_id", req.Context.TraceID),
				structuredlog.F("volume_id", req.VolumeID),
				structuredlog.F("snapshot_id", snapshotID),
				structuredlog.F("attachment_id", req.Context.AttachmentID),
				structuredlog.F("generation", req.Context.Generation),
				structuredlog.F("offset_bytes", req.OffsetBytes),
				structuredlog.F("length_bytes", req.LengthBytes),
				structuredlog.F("degraded", resp.Degraded),
				structuredlog.F("reason", resp.Reason),
			)
			return &service.ReadResponse{
				VolumeID:       req.VolumeID,
				OffsetBytes:    req.OffsetBytes,
				LengthBytes:    req.LengthBytes,
				Data:           resp.Data,
				VolumeRevision: c.observedVolumeRevision(req.VolumeID),
			}, nil
		}
		if cloneID != "" {
			if c.cloneAllocationResolver == nil {
				return nil, fmt.Errorf("ec clone allocation resolver is not configured")
			}
			allocationPages, err := c.cloneAllocationResolver.ResolveCloneAllocationPages(ctx, cloneID, req.OffsetBytes, req.LengthBytes, spec.ExtentPageBytes, spec.ChunkSizeBytes)
			if err != nil {
				structuredlog.Error("sbs.cluster.client", "ec_clone_allocation_resolve_failed", err,
					structuredlog.F("request_id", req.Context.RequestID),
					structuredlog.F("trace_id", req.Context.TraceID),
					structuredlog.F("volume_id", req.VolumeID),
					structuredlog.F("clone_id", cloneID),
					structuredlog.F("attachment_id", req.Context.AttachmentID),
					structuredlog.F("generation", req.Context.Generation),
					structuredlog.F("offset_bytes", req.OffsetBytes),
					structuredlog.F("length_bytes", req.LengthBytes),
				)
				return nil, err
			}
			resp, err := ecSvc.ReadFromAllocationPages(ctx, ecReq, allocationPages)
			if err != nil {
				structuredlog.Error("sbs.cluster.client", "ec_clone_read_failed", err,
					structuredlog.F("request_id", req.Context.RequestID),
					structuredlog.F("trace_id", req.Context.TraceID),
					structuredlog.F("volume_id", req.VolumeID),
					structuredlog.F("clone_id", cloneID),
					structuredlog.F("attachment_id", req.Context.AttachmentID),
					structuredlog.F("generation", req.Context.Generation),
					structuredlog.F("offset_bytes", req.OffsetBytes),
					structuredlog.F("length_bytes", req.LengthBytes),
				)
				return nil, err
			}
			structuredlog.Info("sbs.cluster.client", "ec_clone_read_completed",
				structuredlog.F("request_id", req.Context.RequestID),
				structuredlog.F("trace_id", req.Context.TraceID),
				structuredlog.F("volume_id", req.VolumeID),
				structuredlog.F("clone_id", cloneID),
				structuredlog.F("attachment_id", req.Context.AttachmentID),
				structuredlog.F("generation", req.Context.Generation),
				structuredlog.F("offset_bytes", req.OffsetBytes),
				structuredlog.F("length_bytes", req.LengthBytes),
				structuredlog.F("degraded", resp.Degraded),
				structuredlog.F("reason", resp.Reason),
			)
			return &service.ReadResponse{
				VolumeID:       req.VolumeID,
				OffsetBytes:    req.OffsetBytes,
				LengthBytes:    req.LengthBytes,
				Data:           resp.Data,
				VolumeRevision: c.observedVolumeRevision(req.VolumeID),
			}, nil
		}
		resp, err := ecSvc.Read(ctx, clusterec.ReadRequest{
			Volume:  ecReq.Volume,
			Context: ecReq.Context,
			Offset:  ecReq.Offset,
			Length:  ecReq.Length,
		})
		if err != nil {
			structuredlog.Error("sbs.cluster.client", "ec_read_failed", err,
				structuredlog.F("request_id", req.Context.RequestID),
				structuredlog.F("trace_id", req.Context.TraceID),
				structuredlog.F("volume_id", req.VolumeID),
				structuredlog.F("attachment_id", req.Context.AttachmentID),
				structuredlog.F("generation", req.Context.Generation),
				structuredlog.F("offset_bytes", req.OffsetBytes),
				structuredlog.F("length_bytes", req.LengthBytes),
			)
			return nil, err
		}
		structuredlog.Info("sbs.cluster.client", "ec_read_completed",
			structuredlog.F("request_id", req.Context.RequestID),
			structuredlog.F("trace_id", req.Context.TraceID),
			structuredlog.F("volume_id", req.VolumeID),
			structuredlog.F("attachment_id", req.Context.AttachmentID),
			structuredlog.F("generation", req.Context.Generation),
			structuredlog.F("offset_bytes", req.OffsetBytes),
			structuredlog.F("length_bytes", req.LengthBytes),
			structuredlog.F("duration_ms", time.Since(started).Milliseconds()),
			structuredlog.F("require_open_duration_ms", requireOpenDuration.Milliseconds()),
			structuredlog.F("lookup_volume_duration_ms", lookupVolumeDuration.Milliseconds()),
			structuredlog.F("degraded", resp.Degraded),
			structuredlog.F("reason", resp.Reason),
			structuredlog.F("missing_shards", resp.MissingShards),
			structuredlog.F("corrupt_shards", resp.CorruptShards),
		)
		return &service.ReadResponse{
			VolumeID:       req.VolumeID,
			OffsetBytes:    req.OffsetBytes,
			LengthBytes:    req.LengthBytes,
			Data:           resp.Data,
			VolumeRevision: c.observedVolumeRevision(req.VolumeID),
		}, nil
	}
	zeroEvidenceStarted := time.Now()
	if resp, ok, err := c.tryZeroAllocationReadFastPath(ctx, snapshotID, cloneID, req, spec); ok || err != nil {
		zeroEvidenceDuration := time.Since(zeroEvidenceStarted)
		if err != nil {
			structuredlog.Error("sbs.cluster.client", "read_failed", err,
				structuredlog.F("request_id", req.Context.RequestID),
				structuredlog.F("trace_id", req.Context.TraceID),
				structuredlog.F("volume_id", req.VolumeID),
				structuredlog.F("clone_id", cloneID),
				structuredlog.F("snapshot_id", snapshotID),
				structuredlog.F("attachment_id", req.Context.AttachmentID),
				structuredlog.F("generation", req.Context.Generation),
				structuredlog.F("offset_bytes", req.OffsetBytes),
				structuredlog.F("length_bytes", req.LengthBytes),
				structuredlog.F("phase", "zero_allocation_evidence"),
				structuredlog.F("duration_ms", time.Since(started).Milliseconds()),
				structuredlog.F("require_open_duration_ms", requireOpenDuration.Milliseconds()),
				structuredlog.F("lookup_volume_duration_ms", lookupVolumeDuration.Milliseconds()),
				structuredlog.F("zero_evidence_duration_ms", zeroEvidenceDuration.Milliseconds()),
			)
			return nil, err
		}
		structuredlog.Info("sbs.cluster.client", "read_completed",
			structuredlog.F("request_id", req.Context.RequestID),
			structuredlog.F("trace_id", req.Context.TraceID),
			structuredlog.F("volume_id", req.VolumeID),
			structuredlog.F("clone_id", cloneID),
			structuredlog.F("snapshot_id", snapshotID),
			structuredlog.F("attachment_id", req.Context.AttachmentID),
			structuredlog.F("generation", req.Context.Generation),
			structuredlog.F("offset_bytes", req.OffsetBytes),
			structuredlog.F("length_bytes", req.LengthBytes),
			structuredlog.F("duration_ms", time.Since(started).Milliseconds()),
			structuredlog.F("require_open_duration_ms", requireOpenDuration.Milliseconds()),
			structuredlog.F("lookup_volume_duration_ms", lookupVolumeDuration.Milliseconds()),
			structuredlog.F("zero_evidence_duration_ms", zeroEvidenceDuration.Milliseconds()),
			structuredlog.F("replica_read_duration_ms", int64(0)),
			structuredlog.F("zero_allocation_fast_path", true),
			structuredlog.F("zero_data", resp != nil && resp.ZeroData),
		)
		return resp, err
	}
	zeroEvidenceDuration := time.Since(zeroEvidenceStarted)
	readAttribution := service.ReadPathAttributionFromContext(ctx)
	readSvc := replication.NewReadService(c.coordinator, c.newRemoteReplicaReader(current.replicas))
	readReq := replication.ReadRequest{
		RequestID:      req.Context.RequestID,
		VolumeID:       req.VolumeID,
		CloneID:        cloneID,
		SnapshotID:     snapshotID,
		OffsetBytes:    req.OffsetBytes,
		LengthBytes:    req.LengthBytes,
		PageBytes:      spec.ExtentPageBytes,
		ChunkSizeBytes: spec.ChunkSizeBytes,
		Attribution:    readAttribution,
	}
	replicaReadStarted := time.Now()
	resp, err := readSvc.Read(ctx, readReq)
	replicaReadDuration := time.Since(replicaReadStarted)
	if err != nil && isRecoverableReplicaSessionError(err) {
		structuredlog.Error("sbs.cluster.client", "read_retryable_session_error", err,
			structuredlog.F("request_id", req.Context.RequestID),
			structuredlog.F("trace_id", req.Context.TraceID),
			structuredlog.F("volume_id", req.VolumeID),
			structuredlog.F("clone_id", cloneID),
			structuredlog.F("snapshot_id", snapshotID),
			structuredlog.F("attachment_id", req.Context.AttachmentID),
			structuredlog.F("generation", req.Context.Generation),
		)
		current, err = c.refreshOpenSession(ctx, req.VolumeID)
		if err != nil {
			return nil, err
		}
		readSvc = replication.NewReadService(c.coordinator, c.newRemoteReplicaReader(current.replicas))
		replicaReadStarted = time.Now()
		resp, err = readSvc.Read(ctx, readReq)
		replicaReadDuration += time.Since(replicaReadStarted)
	}
	if err != nil {
		structuredlog.Error("sbs.cluster.client", "read_failed", err,
			structuredlog.F("request_id", req.Context.RequestID),
			structuredlog.F("trace_id", req.Context.TraceID),
			structuredlog.F("volume_id", req.VolumeID),
			structuredlog.F("clone_id", cloneID),
			structuredlog.F("snapshot_id", snapshotID),
			structuredlog.F("attachment_id", req.Context.AttachmentID),
			structuredlog.F("generation", req.Context.Generation),
			structuredlog.F("offset_bytes", req.OffsetBytes),
			structuredlog.F("length_bytes", req.LengthBytes),
			structuredlog.F("phase", "replica_read"),
			structuredlog.F("duration_ms", time.Since(started).Milliseconds()),
			structuredlog.F("require_open_duration_ms", requireOpenDuration.Milliseconds()),
			structuredlog.F("lookup_volume_duration_ms", lookupVolumeDuration.Milliseconds()),
			structuredlog.F("zero_evidence_duration_ms", zeroEvidenceDuration.Milliseconds()),
			structuredlog.F("replica_read_duration_ms", replicaReadDuration.Milliseconds()),
		)
		return nil, err
	}
	structuredlog.Info("sbs.cluster.client", "read_completed",
		structuredlog.F("request_id", req.Context.RequestID),
		structuredlog.F("trace_id", req.Context.TraceID),
		structuredlog.F("volume_id", req.VolumeID),
		structuredlog.F("clone_id", cloneID),
		structuredlog.F("snapshot_id", snapshotID),
		structuredlog.F("attachment_id", req.Context.AttachmentID),
		structuredlog.F("generation", req.Context.Generation),
		structuredlog.F("offset_bytes", req.OffsetBytes),
		structuredlog.F("length_bytes", req.LengthBytes),
		structuredlog.F("duration_ms", time.Since(started).Milliseconds()),
		structuredlog.F("require_open_duration_ms", requireOpenDuration.Milliseconds()),
		structuredlog.F("lookup_volume_duration_ms", lookupVolumeDuration.Milliseconds()),
		structuredlog.F("zero_evidence_duration_ms", zeroEvidenceDuration.Milliseconds()),
		structuredlog.F("replica_read_duration_ms", replicaReadDuration.Milliseconds()),
		structuredlog.F("zero_allocation_fast_path", false),
		structuredlog.F("zero_data", false),
		structuredlog.F("replica_reads", resp.ReplicaReads),
	)
	return &service.ReadResponse{
		VolumeID:       req.VolumeID,
		OffsetBytes:    req.OffsetBytes,
		LengthBytes:    req.LengthBytes,
		Data:           resp.Data,
		VolumeRevision: c.observedVolumeRevision(req.VolumeID),
	}, nil
}

func (c *Client) tryZeroAllocationReadFastPath(ctx context.Context, snapshotID, cloneID string, req *service.ReadRequest, spec service.VolumeSpec) (*service.ReadResponse, bool, error) {
	if !c.zeroAllocationReadFastPath || snapshotID != "" || cloneID != "" {
		return nil, false, nil
	}
	if spec.ExtentPageBytes == 0 || spec.ChunkSizeBytes == 0 || spec.ExtentPageBytes%spec.ChunkSizeBytes != 0 || req.LengthBytes == 0 {
		return nil, false, nil
	}
	started := time.Now()
	readAttribution := service.ReadPathAttributionFromContext(ctx)
	if c.unsafeZeroReplayFastPathActive() {
		structuredlog.Info("sbs.cluster.client", "zero_allocation_read_fast_path",
			structuredlog.F("request_id", req.Context.RequestID),
			structuredlog.F("trace_id", req.Context.TraceID),
			structuredlog.F("volume_id", req.VolumeID),
			structuredlog.F("attachment_id", req.Context.AttachmentID),
			structuredlog.F("generation", req.Context.Generation),
			structuredlog.F("offset_bytes", req.OffsetBytes),
			structuredlog.F("length_bytes", req.LengthBytes),
			structuredlog.F("allocation_page_count", 0),
			structuredlog.F("unsafe_zero_replay_fast_path", true),
			structuredlog.F("allocation_evidence_skipped", true),
			structuredlog.F("duration_ms", time.Since(started).Milliseconds()),
		)
		return &service.ReadResponse{
			VolumeID:       req.VolumeID,
			OffsetBytes:    req.OffsetBytes,
			LengthBytes:    req.LengthBytes,
			Data:           make([]byte, req.LengthBytes),
			ZeroData:       true,
			VolumeRevision: c.observedVolumeRevision(req.VolumeID),
		}, true, nil
	}
	cacheLookupStarted := time.Now()
	if pages, ok := c.lookupZeroEvidencePages(req.VolumeID, req.OffsetBytes, req.LengthBytes, spec.ExtentPageBytes, spec.ChunkSizeBytes); ok {
		cacheLookupDuration := time.Since(cacheLookupStarted)
		structuredlog.Info("sbs.cluster.client", "zero_allocation_read_fast_path",
			structuredlog.F("request_id", req.Context.RequestID),
			structuredlog.F("trace_id", req.Context.TraceID),
			structuredlog.F("volume_id", req.VolumeID),
			structuredlog.F("attachment_id", req.Context.AttachmentID),
			structuredlog.F("generation", req.Context.Generation),
			structuredlog.F("offset_bytes", req.OffsetBytes),
			structuredlog.F("length_bytes", req.LengthBytes),
			structuredlog.F("allocation_page_count", len(pages)),
			structuredlog.F("zero_evidence_cache_hit", true),
			structuredlog.F("duration_ms", time.Since(started).Milliseconds()),
			structuredlog.F("zero_evidence_cache_lookup_duration_ms", cacheLookupDuration.Milliseconds()),
		)
		return &service.ReadResponse{
			VolumeID:       req.VolumeID,
			OffsetBytes:    req.OffsetBytes,
			LengthBytes:    req.LengthBytes,
			Data:           make([]byte, req.LengthBytes),
			ZeroData:       true,
			VolumeRevision: c.observedVolumeRevision(req.VolumeID),
		}, true, nil
	}
	cacheLookupDuration := time.Since(cacheLookupStarted)
	if c.allocationResolver == nil {
		return nil, false, nil
	}
	resolverStarted := time.Now()
	pages, err := c.allocationResolver.ResolveAllocationPages(ctx, req.VolumeID, req.OffsetBytes, req.LengthBytes, spec.ExtentPageBytes, spec.ChunkSizeBytes)
	resolverDuration := time.Since(resolverStarted)
	if err != nil {
		if readAttribution {
			structuredlog.Error("sbs.cluster.client", "zero_allocation_read_evidence_failed", err,
				structuredlog.F("request_id", req.Context.RequestID),
				structuredlog.F("trace_id", req.Context.TraceID),
				structuredlog.F("volume_id", req.VolumeID),
				structuredlog.F("attachment_id", req.Context.AttachmentID),
				structuredlog.F("generation", req.Context.Generation),
				structuredlog.F("offset_bytes", req.OffsetBytes),
				structuredlog.F("length_bytes", req.LengthBytes),
				structuredlog.F("zero_evidence_cache_hit", false),
				structuredlog.F("duration_ms", time.Since(started).Milliseconds()),
				structuredlog.F("zero_evidence_cache_lookup_duration_ms", cacheLookupDuration.Milliseconds()),
				structuredlog.F("allocation_resolver_duration_ms", resolverDuration.Milliseconds()),
			)
		}
		return nil, false, err
	}
	if !resolvedAllocationPagesRangeIsZero(pages, req.OffsetBytes, req.LengthBytes, spec.ChunkSizeBytes) {
		if readAttribution {
			structuredlog.Info("sbs.cluster.client", "zero_allocation_read_evidence_checked",
				structuredlog.F("request_id", req.Context.RequestID),
				structuredlog.F("trace_id", req.Context.TraceID),
				structuredlog.F("volume_id", req.VolumeID),
				structuredlog.F("attachment_id", req.Context.AttachmentID),
				structuredlog.F("generation", req.Context.Generation),
				structuredlog.F("offset_bytes", req.OffsetBytes),
				structuredlog.F("length_bytes", req.LengthBytes),
				structuredlog.F("allocation_page_count", len(pages)),
				structuredlog.F("zero_evidence_cache_hit", false),
				structuredlog.F("zero_allocation", false),
				structuredlog.F("duration_ms", time.Since(started).Milliseconds()),
				structuredlog.F("zero_evidence_cache_lookup_duration_ms", cacheLookupDuration.Milliseconds()),
				structuredlog.F("allocation_resolver_duration_ms", resolverDuration.Milliseconds()),
			)
		}
		return nil, false, nil
	}
	c.rememberZeroEvidencePages(req.VolumeID, pages)
	structuredlog.Info("sbs.cluster.client", "zero_allocation_read_fast_path",
		structuredlog.F("request_id", req.Context.RequestID),
		structuredlog.F("trace_id", req.Context.TraceID),
		structuredlog.F("volume_id", req.VolumeID),
		structuredlog.F("attachment_id", req.Context.AttachmentID),
		structuredlog.F("generation", req.Context.Generation),
		structuredlog.F("offset_bytes", req.OffsetBytes),
		structuredlog.F("length_bytes", req.LengthBytes),
		structuredlog.F("allocation_page_count", len(pages)),
		structuredlog.F("zero_evidence_cache_hit", false),
		structuredlog.F("zero_allocation", true),
		structuredlog.F("duration_ms", time.Since(started).Milliseconds()),
		structuredlog.F("zero_evidence_cache_lookup_duration_ms", cacheLookupDuration.Milliseconds()),
		structuredlog.F("allocation_resolver_duration_ms", resolverDuration.Milliseconds()),
	)
	return &service.ReadResponse{
		VolumeID:       req.VolumeID,
		OffsetBytes:    req.OffsetBytes,
		LengthBytes:    req.LengthBytes,
		Data:           make([]byte, req.LengthBytes),
		ZeroData:       true,
		VolumeRevision: c.observedVolumeRevision(req.VolumeID),
	}, true, nil
}

func (c *Client) unsafeZeroReplayFastPathActive() bool {
	return c.unsafeZeroReplayFastPath && c.zeroAllocationReadFastPath && c.unsafeZeroNoopSkipIdempotency
}

func (c *Client) lookupZeroEvidencePages(volumeID string, offsetBytes, lengthBytes uint64, pageBytes, chunkSizeBytes uint32) ([]metadata.ResolvedAllocationPage, bool) {
	if c.zeroEvidenceCacheTTL <= 0 || volumeID == "" || lengthBytes == 0 || pageBytes == 0 || chunkSizeBytes == 0 || pageBytes%chunkSizeBytes != 0 {
		return nil, false
	}
	endBytes := offsetBytes + lengthBytes
	if endBytes <= offsetBytes {
		return nil, false
	}
	startPage := offsetBytes / uint64(pageBytes)
	endPage := (endBytes - 1) / uint64(pageBytes)
	chunksPerPage := uint64(pageBytes / chunkSizeBytes)
	now := time.Now()
	pages := make([]metadata.ResolvedAllocationPage, 0, endPage-startPage+1)
	c.mu.RLock()
	for pageNo := startPage; ; pageNo++ {
		key := zeroEvidenceCacheKey{
			volumeID:       volumeID,
			pageNo:         pageNo,
			pageBytes:      pageBytes,
			chunkSizeBytes: chunkSizeBytes,
		}
		cached, ok := c.zeroEvidenceCache[key]
		if !ok {
			c.mu.RUnlock()
			return nil, false
		}
		if now.After(cached.expiresAt) {
			c.mu.RUnlock()
			c.mu.Lock()
			if current, ok := c.zeroEvidenceCache[key]; ok && current.expiresAt.Equal(cached.expiresAt) {
				delete(c.zeroEvidenceCache, key)
			}
			c.mu.Unlock()
			return nil, false
		}
		rangeStart := pageNo * chunksPerPage
		pages = append(pages, metadata.ResolvedAllocationPage{
			Page:            cached.page,
			RangeStartChunk: rangeStart,
			RangeEndChunk:   rangeStart + chunksPerPage,
			CoversWholePage: true,
		})
		if pageNo == endPage {
			break
		}
	}
	c.mu.RUnlock()
	if !resolvedAllocationPagesRangeIsZero(pages, offsetBytes, lengthBytes, chunkSizeBytes) {
		return nil, false
	}
	return pages, true
}

func (c *Client) rememberZeroEvidencePages(volumeID string, pages []metadata.ResolvedAllocationPage) {
	if c.zeroEvidenceCacheTTL <= 0 || volumeID == "" || len(pages) == 0 {
		return
	}
	expiresAt := time.Now().Add(c.zeroEvidenceCacheTTL)
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.zeroEvidenceCache == nil {
		c.zeroEvidenceCache = make(map[zeroEvidenceCacheKey]cachedZeroEvidencePage)
	}
	for _, resolved := range pages {
		page := resolved.Page
		if page.PageBytes == 0 || page.ChunkSizeBytes == 0 || page.PageBytes%page.ChunkSizeBytes != 0 {
			continue
		}
		if page.VolumeID == "" {
			page.VolumeID = volumeID
		}
		if page.VolumeID != volumeID {
			continue
		}
		key := zeroEvidenceCacheKey{
			volumeID:       volumeID,
			pageNo:         page.PageNo,
			pageBytes:      page.PageBytes,
			chunkSizeBytes: page.ChunkSizeBytes,
		}
		c.zeroEvidenceCache[key] = cachedZeroEvidencePage{page: page, expiresAt: expiresAt}
	}
}

func (c *Client) invalidateZeroEvidenceRange(volumeID string, offsetBytes, lengthBytes uint64, pageBytes, chunkSizeBytes uint32) {
	if c.zeroEvidenceCacheTTL <= 0 || volumeID == "" || lengthBytes == 0 || pageBytes == 0 || chunkSizeBytes == 0 {
		return
	}
	endBytes := offsetBytes + lengthBytes
	if endBytes <= offsetBytes {
		return
	}
	startPage := offsetBytes / uint64(pageBytes)
	endPage := (endBytes - 1) / uint64(pageBytes)
	c.mu.Lock()
	defer c.mu.Unlock()
	for pageNo := startPage; ; pageNo++ {
		delete(c.zeroEvidenceCache, zeroEvidenceCacheKey{
			volumeID:       volumeID,
			pageNo:         pageNo,
			pageBytes:      pageBytes,
			chunkSizeBytes: chunkSizeBytes,
		})
		if pageNo == endPage {
			break
		}
	}
}

func resolvedAllocationPagesRangeIsZero(pages []metadata.ResolvedAllocationPage, offsetBytes, lengthBytes uint64, chunkSizeBytes uint32) bool {
	if len(pages) == 0 || lengthBytes == 0 || chunkSizeBytes == 0 {
		return false
	}
	endBytes := offsetBytes + lengthBytes
	if endBytes <= offsetBytes {
		return false
	}
	chunkSize := uint64(chunkSizeBytes)
	startChunk := offsetBytes / chunkSize
	endChunk := (endBytes - 1) / chunkSize
	for logicalChunk := startChunk; ; logicalChunk++ {
		covered, zero := resolvedAllocationPagesChunkZeroState(pages, logicalChunk)
		if !covered || !zero {
			return false
		}
		if logicalChunk == endChunk {
			break
		}
	}
	return true
}

func resolvedAllocationPagesChunkZeroState(pages []metadata.ResolvedAllocationPage, logicalChunk uint64) (bool, bool) {
	for _, page := range pages {
		if logicalChunk < page.RangeStartChunk || logicalChunk >= page.RangeEndChunk {
			continue
		}
		for _, extent := range page.Page.Extents {
			start := extent.LogicalChunkStart
			end := start + uint64(extent.ChunkCount)
			if logicalChunk < start || logicalChunk >= end {
				continue
			}
			return true, extent.Kind == metadata.AllocationKindZero
		}
		return false, false
	}
	return false, false
}

func (c *Client) Write(ctx context.Context, req *service.WriteRequest) (*service.WriteResponse, error) {
	return c.writeWithCloneAndMode(ctx, "", req, false)
}

func (c *Client) WriteClone(ctx context.Context, cloneID string, req *service.WriteRequest) (*service.WriteResponse, error) {
	cloneID = strings.TrimSpace(cloneID)
	if cloneID == "" {
		return nil, badRequest("clone_id is required")
	}
	return c.writeWithCloneAndMode(ctx, cloneID, req, false)
}

func (c *Client) writeWithMode(ctx context.Context, req *service.WriteRequest, zeroSemantic bool) (*service.WriteResponse, error) {
	return c.writeWithCloneAndMode(ctx, "", req, zeroSemantic)
}

func (c *Client) writeWithCloneAndMode(ctx context.Context, cloneID string, req *service.WriteRequest, zeroSemantic bool) (*service.WriteResponse, error) {
	start := time.Now()
	if req == nil {
		return nil, badRequest("nil request")
	}
	if err := validateClusterWriteRequest(req, zeroSemantic); err != nil {
		return nil, badRequest(err.Error())
	}
	current, err := c.requireOpen(req.VolumeID, req.VolumeHandle, req.Context)
	if err != nil {
		return nil, err
	}
	lookupStart := time.Now()
	spec, err := c.lookupVolume(ctx, req.VolumeID)
	lookupDuration := time.Since(lookupStart)
	if err != nil {
		return nil, err
	}
	zeroPayloadPromoted := false
	if !zeroSemantic && cloneID == "" && c.promoteZeroPayloadWrites && !c.preferPayloadOnlyWrites && isAllZeroPayload(req.Data) {
		zeroSemantic = true
		zeroPayloadPromoted = true
	}
	if clusterec.IsECVolume(spec) {
		if cloneID != "" {
			return c.writeECClone(ctx, cloneID, req, current, spec, start, lookupDuration, zeroSemantic)
		}
		ecSvc, err := c.ecService(ctx, current)
		if err != nil {
			return nil, err
		}
		data := append([]byte(nil), req.Data...)
		if zeroSemantic && len(data) == 0 && !service.DiscardRangeAligned(spec, req.OffsetBytes, req.LengthBytes) {
			data = make([]byte, req.LengthBytes)
		}
		writeStart := time.Now()
		resp, err := ecSvc.Write(ctx, clusterec.WriteRequest{
			Volume:       spec,
			Context:      req.Context,
			Offset:       req.OffsetBytes,
			Length:       req.LengthBytes,
			Data:         data,
			ZeroSemantic: zeroSemantic,
		})
		writeDuration := time.Since(writeStart)
		if err != nil {
			structuredlog.Error("sbs.cluster.client", "ec_write_failed", err,
				structuredlog.F("request_id", req.Context.RequestID),
				structuredlog.F("trace_id", req.Context.TraceID),
				structuredlog.F("volume_id", req.VolumeID),
				structuredlog.F("attachment_id", req.Context.AttachmentID),
				structuredlog.F("generation", req.Context.Generation),
				structuredlog.F("idempotency_key", req.Context.IdempotencyKey),
				structuredlog.F("offset_bytes", req.OffsetBytes),
				structuredlog.F("length_bytes", req.LengthBytes),
				structuredlog.F("zero_semantic", zeroSemantic),
				structuredlog.F("zero_payload_promoted", zeroPayloadPromoted),
				structuredlog.F("duration_ms", time.Since(start).Milliseconds()),
				structuredlog.F("lookup_volume_duration_ms", lookupDuration.Milliseconds()),
				structuredlog.F("ec_write_duration_ms", writeDuration.Milliseconds()),
			)
			return nil, err
		}
		c.rememberVolumeRevision(req.VolumeID, resp.Revision)
		structuredlog.Info("sbs.cluster.client", "ec_write_committed",
			structuredlog.F("request_id", req.Context.RequestID),
			structuredlog.F("trace_id", req.Context.TraceID),
			structuredlog.F("volume_id", req.VolumeID),
			structuredlog.F("attachment_id", req.Context.AttachmentID),
			structuredlog.F("generation", req.Context.Generation),
			structuredlog.F("idempotency_key", req.Context.IdempotencyKey),
			structuredlog.F("operation_id", resp.OperationID),
			structuredlog.F("object_id", resp.ObjectID),
			structuredlog.F("stripe_id", resp.StripeID),
			structuredlog.F("offset_bytes", req.OffsetBytes),
			structuredlog.F("length_bytes", req.LengthBytes),
			structuredlog.F("revision", resp.Revision),
			structuredlog.F("zero_semantic", zeroSemantic),
			structuredlog.F("zero_payload_promoted", zeroPayloadPromoted),
			structuredlog.F("duration_ms", time.Since(start).Milliseconds()),
			structuredlog.F("lookup_volume_duration_ms", lookupDuration.Milliseconds()),
			structuredlog.F("ec_write_duration_ms", writeDuration.Milliseconds()),
		)
		return &service.WriteResponse{
			Status:         "ok",
			VolumeID:       req.VolumeID,
			OffsetBytes:    req.OffsetBytes,
			LengthBytes:    req.LengthBytes,
			CommitID:       fmt.Sprintf("ec-commit-%s-%d", req.VolumeID, resp.Revision),
			VolumeRevision: resp.Revision,
		}, nil
	}
	if resp, ok, err := c.tryUnsafeZeroNoopWriteFastPath(ctx, cloneID, req, spec, zeroPayloadPromoted, start, lookupDuration); ok || err != nil {
		return resp, err
	}
	if c.preferPayloadOnlyWrites && cloneID == "" {
		return c.writePayloadOnly(ctx, req, current, start, lookupDuration, zeroSemantic)
	}
	allowZeroNoop := zeroPayloadPromoted || (zeroSemantic && cloneID == "" && len(req.Data) == 0)
	writeStart := time.Now()
	writeSvc := replication.NewWriteService(c.executor, c.newRemoteReplicaWriter(current.replicas))
	writeReq := replication.WriteRequest{
		VolumeID:                      req.VolumeID,
		CloneID:                       cloneID,
		RequestID:                     req.Context.RequestID,
		AttachmentID:                  req.Context.AttachmentID,
		Generation:                    req.Context.Generation,
		IdempotencyKey:                req.Context.IdempotencyKey,
		OffsetBytes:                   req.OffsetBytes,
		LengthBytes:                   req.LengthBytes,
		Data:                          append([]byte(nil), req.Data...),
		PageBytes:                     spec.ExtentPageBytes,
		ChunkSizeBytes:                spec.ChunkSizeBytes,
		ZeroSemantic:                  zeroSemantic,
		AllowZeroNoop:                 allowZeroNoop,
		UnsafeZeroNoopSkipIdempotency: zeroPayloadPromoted && c.unsafeZeroNoopSkipIdempotency,
	}
	resp, err := writeSvc.Write(ctx, writeReq)
	writeDuration := time.Since(writeStart)
	if err != nil && isRecoverableReplicaSessionError(err) {
		structuredlog.Error("sbs.cluster.client", "write_retryable_session_error", err,
			structuredlog.F("request_id", req.Context.RequestID),
			structuredlog.F("trace_id", req.Context.TraceID),
			structuredlog.F("volume_id", req.VolumeID),
			structuredlog.F("clone_id", cloneID),
			structuredlog.F("attachment_id", req.Context.AttachmentID),
			structuredlog.F("generation", req.Context.Generation),
			structuredlog.F("idempotency_key", req.Context.IdempotencyKey),
		)
		current, err = c.refreshOpenSession(ctx, req.VolumeID)
		if err != nil {
			return nil, err
		}
		writeSvc = replication.NewWriteService(c.executor, c.newRemoteReplicaWriter(current.replicas))
		writeStart = time.Now()
		resp, err = writeSvc.Write(ctx, writeReq)
		writeDuration += time.Since(writeStart)
	}
	if err != nil {
		structuredlog.Error("sbs.cluster.client", "write_failed", err,
			structuredlog.F("request_id", req.Context.RequestID),
			structuredlog.F("trace_id", req.Context.TraceID),
			structuredlog.F("volume_id", req.VolumeID),
			structuredlog.F("clone_id", cloneID),
			structuredlog.F("attachment_id", req.Context.AttachmentID),
			structuredlog.F("generation", req.Context.Generation),
			structuredlog.F("idempotency_key", req.Context.IdempotencyKey),
			structuredlog.F("offset_bytes", req.OffsetBytes),
			structuredlog.F("length_bytes", req.LengthBytes),
			structuredlog.F("duration_ms", time.Since(start).Milliseconds()),
			structuredlog.F("lookup_volume_duration_ms", lookupDuration.Milliseconds()),
			structuredlog.F("replication_write_duration_ms", writeDuration.Milliseconds()),
		)
		return nil, err
	}
	c.invalidateZeroEvidenceRange(req.VolumeID, req.OffsetBytes, req.LengthBytes, spec.ExtentPageBytes, spec.ChunkSizeBytes)
	structuredlog.Info("sbs.cluster.client", "write_committed",
		structuredlog.F("request_id", req.Context.RequestID),
		structuredlog.F("trace_id", req.Context.TraceID),
		structuredlog.F("volume_id", req.VolumeID),
		structuredlog.F("clone_id", cloneID),
		structuredlog.F("attachment_id", req.Context.AttachmentID),
		structuredlog.F("generation", req.Context.Generation),
		structuredlog.F("idempotency_key", req.Context.IdempotencyKey),
		structuredlog.F("offset_bytes", req.OffsetBytes),
		structuredlog.F("length_bytes", req.LengthBytes),
		structuredlog.F("revision", resp.Revision),
		structuredlog.F("zero_payload_promoted", zeroPayloadPromoted),
		structuredlog.F("zero_noop_allowed", allowZeroNoop),
		structuredlog.F("duration_ms", time.Since(start).Milliseconds()),
		structuredlog.F("lookup_volume_duration_ms", lookupDuration.Milliseconds()),
		structuredlog.F("replication_write_duration_ms", writeDuration.Milliseconds()),
	)
	c.rememberVolumeRevision(req.VolumeID, resp.Revision)
	return &service.WriteResponse{
		Status:         "ok",
		VolumeID:       req.VolumeID,
		OffsetBytes:    req.OffsetBytes,
		LengthBytes:    req.LengthBytes,
		CommitID:       fmt.Sprintf("commit-%s-%d", req.VolumeID, resp.Revision),
		VolumeRevision: resp.Revision,
	}, nil
}

func (c *Client) tryUnsafeZeroNoopWriteFastPath(ctx context.Context, cloneID string, req *service.WriteRequest, spec service.VolumeSpec, zeroPayloadPromoted bool, start time.Time, lookupDuration time.Duration) (*service.WriteResponse, bool, error) {
	if !zeroPayloadPromoted || !c.unsafeZeroNoopSkipIdempotency || cloneID != "" {
		return nil, false, nil
	}
	if spec.ExtentPageBytes == 0 || spec.ChunkSizeBytes == 0 || spec.ExtentPageBytes%spec.ChunkSizeBytes != 0 || req.LengthBytes == 0 {
		return nil, false, nil
	}
	if c.unsafeZeroReplayFastPathActive() {
		return c.completeUnsafeZeroNoopWriteFastPath(req, cloneID, start, lookupDuration, 0, true, false), true, nil
	}
	if pages, ok := c.lookupZeroEvidencePages(req.VolumeID, req.OffsetBytes, req.LengthBytes, spec.ExtentPageBytes, spec.ChunkSizeBytes); ok {
		return c.completeUnsafeZeroNoopWriteFastPath(req, cloneID, start, lookupDuration, len(pages), false, true), true, nil
	}
	if c.allocationResolver == nil {
		return nil, false, nil
	}
	pages, err := c.allocationResolver.ResolveAllocationPages(ctx, req.VolumeID, req.OffsetBytes, req.LengthBytes, spec.ExtentPageBytes, spec.ChunkSizeBytes)
	if err != nil {
		return nil, false, err
	}
	if !resolvedAllocationPagesRangeIsZero(pages, req.OffsetBytes, req.LengthBytes, spec.ChunkSizeBytes) {
		return nil, false, nil
	}
	c.rememberZeroEvidencePages(req.VolumeID, pages)
	return c.completeUnsafeZeroNoopWriteFastPath(req, cloneID, start, lookupDuration, len(pages), false, false), true, nil
}

func (c *Client) completeUnsafeZeroNoopWriteFastPath(req *service.WriteRequest, cloneID string, start time.Time, lookupDuration time.Duration, allocationPageCount int, unsafeZeroReplayFastPath, zeroEvidenceCacheHit bool) *service.WriteResponse {
	revision := c.observedVolumeRevision(req.VolumeID)
	structuredlog.Info("sbs.cluster.client", "write_committed",
		structuredlog.F("request_id", req.Context.RequestID),
		structuredlog.F("trace_id", req.Context.TraceID),
		structuredlog.F("volume_id", req.VolumeID),
		structuredlog.F("clone_id", cloneID),
		structuredlog.F("attachment_id", req.Context.AttachmentID),
		structuredlog.F("generation", req.Context.Generation),
		structuredlog.F("idempotency_key", req.Context.IdempotencyKey),
		structuredlog.F("offset_bytes", req.OffsetBytes),
		structuredlog.F("length_bytes", req.LengthBytes),
		structuredlog.F("revision", revision),
		structuredlog.F("zero_payload_promoted", true),
		structuredlog.F("zero_noop_client_fast_path", true),
		structuredlog.F("zero_noop_idempotency_skipped", true),
		structuredlog.F("allocation_page_count", allocationPageCount),
		structuredlog.F("unsafe_zero_replay_fast_path", unsafeZeroReplayFastPath),
		structuredlog.F("allocation_evidence_skipped", unsafeZeroReplayFastPath),
		structuredlog.F("zero_evidence_cache_hit", zeroEvidenceCacheHit),
		structuredlog.F("duration_ms", time.Since(start).Milliseconds()),
		structuredlog.F("lookup_volume_duration_ms", lookupDuration.Milliseconds()),
		structuredlog.F("replication_write_duration_ms", int64(0)),
	)
	return &service.WriteResponse{
		Status:         "ok",
		VolumeID:       req.VolumeID,
		OffsetBytes:    req.OffsetBytes,
		LengthBytes:    req.LengthBytes,
		CommitID:       fmt.Sprintf("commit-%s-%d", req.VolumeID, revision),
		VolumeRevision: revision,
	}
}

func isAllZeroPayload(data []byte) bool {
	if len(data) == 0 {
		return false
	}
	for _, b := range data {
		if b != 0 {
			return false
		}
	}
	return true
}

func (c *Client) writeECClone(ctx context.Context, cloneID string, req *service.WriteRequest, current openSession, spec service.VolumeSpec, start time.Time, lookupDuration time.Duration, zeroSemantic bool) (*service.WriteResponse, error) {
	if zeroSemantic {
		return nil, badRequest("ec clone zero write is unsupported")
	}
	if c.cloneAllocationResolver == nil {
		return nil, fmt.Errorf("ec clone allocation resolver is not configured")
	}
	if c.cloneDeltaCommitter == nil {
		return nil, fmt.Errorf("ec clone delta committer is not configured")
	}
	ecSvc, err := c.ecService(ctx, current)
	if err != nil {
		return nil, err
	}
	stripeBytes := uint64(spec.ECDataShards) * uint64(spec.ECStripeUnitBytes)
	end := req.OffsetBytes + req.LengthBytes
	writeStart := time.Now()
	var lastResp *clusterec.WriteResponse
	for pos := req.OffsetBytes; pos < end; {
		stripeID := pos / stripeBytes
		stripeStart := stripeID * stripeBytes
		next := minUint64(end, stripeStart+stripeBytes)
		dataStart := pos - req.OffsetBytes
		dataEnd := dataStart + (next - pos)
		allocationPages, err := c.cloneAllocationResolver.ResolveCloneAllocationPages(ctx, cloneID, stripeStart, stripeBytes, spec.ExtentPageBytes, spec.ChunkSizeBytes)
		if err != nil {
			structuredlog.Error("sbs.cluster.client", "ec_clone_write_allocation_resolve_failed", err,
				structuredlog.F("request_id", req.Context.RequestID),
				structuredlog.F("trace_id", req.Context.TraceID),
				structuredlog.F("volume_id", req.VolumeID),
				structuredlog.F("clone_id", cloneID),
				structuredlog.F("attachment_id", req.Context.AttachmentID),
				structuredlog.F("generation", req.Context.Generation),
				structuredlog.F("offset_bytes", pos),
				structuredlog.F("length_bytes", next-pos),
			)
			return nil, err
		}
		stripeReq := clusterec.WriteRequest{
			Volume:  spec,
			Context: req.Context,
			Offset:  pos,
			Length:  next - pos,
			Data:    append([]byte(nil), req.Data[int(dataStart):int(dataEnd)]...),
		}
		stripeReq.Context.IdempotencyKey = fmt.Sprintf("%s:ec-clone-stripe:%d:%d:%d", req.Context.IdempotencyKey, stripeID, pos-stripeStart, stripeReq.Length)
		resp, err := ecSvc.WriteCloneDelta(ctx, cloneID, stripeReq, allocationPages, c.cloneDeltaCommitter)
		if err != nil {
			writeDuration := time.Since(writeStart)
			structuredlog.Error("sbs.cluster.client", "ec_clone_write_failed", err,
				structuredlog.F("request_id", req.Context.RequestID),
				structuredlog.F("trace_id", req.Context.TraceID),
				structuredlog.F("volume_id", req.VolumeID),
				structuredlog.F("clone_id", cloneID),
				structuredlog.F("attachment_id", req.Context.AttachmentID),
				structuredlog.F("generation", req.Context.Generation),
				structuredlog.F("idempotency_key", req.Context.IdempotencyKey),
				structuredlog.F("offset_bytes", req.OffsetBytes),
				structuredlog.F("length_bytes", req.LengthBytes),
				structuredlog.F("duration_ms", time.Since(start).Milliseconds()),
				structuredlog.F("lookup_volume_duration_ms", lookupDuration.Milliseconds()),
				structuredlog.F("ec_clone_write_duration_ms", writeDuration.Milliseconds()),
			)
			return nil, err
		}
		lastResp = resp
		pos = next
	}
	writeDuration := time.Since(writeStart)
	objectID := ""
	stripeID := ""
	operationID := ""
	if lastResp != nil {
		objectID = lastResp.ObjectID
		stripeID = lastResp.StripeID
		operationID = lastResp.OperationID
	}
	structuredlog.Info("sbs.cluster.client", "ec_clone_write_committed",
		structuredlog.F("request_id", req.Context.RequestID),
		structuredlog.F("trace_id", req.Context.TraceID),
		structuredlog.F("volume_id", req.VolumeID),
		structuredlog.F("clone_id", cloneID),
		structuredlog.F("attachment_id", req.Context.AttachmentID),
		structuredlog.F("generation", req.Context.Generation),
		structuredlog.F("idempotency_key", req.Context.IdempotencyKey),
		structuredlog.F("operation_id", operationID),
		structuredlog.F("object_id", objectID),
		structuredlog.F("stripe_id", stripeID),
		structuredlog.F("offset_bytes", req.OffsetBytes),
		structuredlog.F("length_bytes", req.LengthBytes),
		structuredlog.F("duration_ms", time.Since(start).Milliseconds()),
		structuredlog.F("lookup_volume_duration_ms", lookupDuration.Milliseconds()),
		structuredlog.F("ec_clone_write_duration_ms", writeDuration.Milliseconds()),
	)
	return &service.WriteResponse{
		Status:         "ok",
		VolumeID:       req.VolumeID,
		OffsetBytes:    req.OffsetBytes,
		LengthBytes:    req.LengthBytes,
		CommitID:       fmt.Sprintf("ec-clone-commit-%s-%s", req.VolumeID, cloneID),
		VolumeRevision: c.observedVolumeRevision(req.VolumeID),
	}, nil
}

type payloadOnlyWriteResult struct {
	revision            uint64
	planDuration        time.Duration
	extentWriteDuration time.Duration
	extentCount         int
}

func (c *Client) writePayloadOnly(ctx context.Context, req *service.WriteRequest, current openSession, start time.Time, lookupDuration time.Duration, zeroSemantic bool) (*service.WriteResponse, error) {
	writeStart := time.Now()
	result, err := c.writePayloadOnlyOnce(ctx, req, current, zeroSemantic)
	writeDuration := time.Since(writeStart)
	if err != nil && isRecoverableReplicaSessionError(err) {
		structuredlog.Error("sbs.cluster.client", "write_payload_only_retryable_session_error", err,
			structuredlog.F("request_id", req.Context.RequestID),
			structuredlog.F("trace_id", req.Context.TraceID),
			structuredlog.F("volume_id", req.VolumeID),
			structuredlog.F("attachment_id", req.Context.AttachmentID),
			structuredlog.F("generation", req.Context.Generation),
			structuredlog.F("idempotency_key", req.Context.IdempotencyKey),
		)
		current, err = c.refreshOpenSession(ctx, req.VolumeID)
		if err != nil {
			return nil, err
		}
		writeStart = time.Now()
		result, err = c.writePayloadOnlyOnce(ctx, req, current, zeroSemantic)
		writeDuration += time.Since(writeStart)
	}
	if err != nil {
		structuredlog.Error("sbs.cluster.client", "write_payload_only_failed", err,
			structuredlog.F("request_id", req.Context.RequestID),
			structuredlog.F("trace_id", req.Context.TraceID),
			structuredlog.F("volume_id", req.VolumeID),
			structuredlog.F("attachment_id", req.Context.AttachmentID),
			structuredlog.F("generation", req.Context.Generation),
			structuredlog.F("idempotency_key", req.Context.IdempotencyKey),
			structuredlog.F("offset_bytes", req.OffsetBytes),
			structuredlog.F("length_bytes", req.LengthBytes),
			structuredlog.F("duration_ms", time.Since(start).Milliseconds()),
			structuredlog.F("lookup_volume_duration_ms", lookupDuration.Milliseconds()),
			structuredlog.F("replication_write_duration_ms", writeDuration.Milliseconds()),
		)
		return nil, err
	}
	structuredlog.Info("sbs.cluster.client", "write_payload_only_committed",
		structuredlog.F("request_id", req.Context.RequestID),
		structuredlog.F("trace_id", req.Context.TraceID),
		structuredlog.F("volume_id", req.VolumeID),
		structuredlog.F("attachment_id", req.Context.AttachmentID),
		structuredlog.F("generation", req.Context.Generation),
		structuredlog.F("idempotency_key", req.Context.IdempotencyKey),
		structuredlog.F("offset_bytes", req.OffsetBytes),
		structuredlog.F("length_bytes", req.LengthBytes),
		structuredlog.F("revision", result.revision),
		structuredlog.F("extent_count", result.extentCount),
		structuredlog.F("duration_ms", time.Since(start).Milliseconds()),
		structuredlog.F("lookup_volume_duration_ms", lookupDuration.Milliseconds()),
		structuredlog.F("replication_write_duration_ms", writeDuration.Milliseconds()),
		structuredlog.F("plan_duration_ms", result.planDuration.Milliseconds()),
		structuredlog.F("extent_write_duration_ms", result.extentWriteDuration.Milliseconds()),
		structuredlog.F("metadata_commit_mode", "payload_only"),
	)
	c.rememberVolumeRevision(req.VolumeID, result.revision)
	return &service.WriteResponse{
		Status:         "ok",
		VolumeID:       req.VolumeID,
		OffsetBytes:    req.OffsetBytes,
		LengthBytes:    req.LengthBytes,
		CommitID:       fmt.Sprintf("payload-only-%s-%d", req.VolumeID, result.revision),
		VolumeRevision: result.revision,
	}, nil
}

func (c *Client) writePayloadOnlyOnce(ctx context.Context, req *service.WriteRequest, current openSession, zeroSemantic bool) (payloadOnlyWriteResult, error) {
	planStart := time.Now()
	plan, err := c.coordinator.PlanWrite(ctx, req.VolumeID, req.OffsetBytes, req.LengthBytes, 0, 0)
	planDuration := time.Since(planStart)
	if err != nil {
		return payloadOnlyWriteResult{planDuration: planDuration}, err
	}
	if len(plan.Extents) == 0 {
		return payloadOnlyWriteResult{planDuration: planDuration}, fmt.Errorf("payload-only write resolved no extents")
	}
	writer := c.newRemoteReplicaWriter(current.replicas)
	var extentWriteDuration time.Duration
	for _, extent := range plan.Extents {
		extentStart := time.Now()
		result, err := writer.WriteExtent(ctx, extent, replication.ReplicaWriteRequest{
			RequestID:      req.Context.RequestID,
			VolumeID:       req.VolumeID,
			AttachmentID:   req.Context.AttachmentID,
			Generation:     req.Context.Generation,
			IdempotencyKey: req.Context.IdempotencyKey,
			OffsetBytes:    req.OffsetBytes,
			LengthBytes:    req.LengthBytes,
			Data:           append([]byte(nil), req.Data...),
			ZeroSemantic:   zeroSemantic,
		})
		extentDuration := time.Since(extentStart)
		extentWriteDuration += extentDuration
		if err != nil {
			return payloadOnlyWriteResult{planDuration: planDuration, extentWriteDuration: extentWriteDuration, extentCount: len(plan.Extents)}, err
		}
		ackedReplicas := 0
		if result != nil {
			ackedReplicas = len(result.AckedReplicaIDs)
		}
		structuredlog.Info("sbs.cluster.client", "write_payload_only_extent_acked",
			structuredlog.F("request_id", req.Context.RequestID),
			structuredlog.F("trace_id", req.Context.TraceID),
			structuredlog.F("volume_id", req.VolumeID),
			structuredlog.F("placement_ref", extent.PlacementRef),
			structuredlog.F("extent_id", extent.Extent.ExtentID),
			structuredlog.F("write_targets", len(extent.WriteTargets)),
			structuredlog.F("required_acks", extent.RequiredAcks),
			structuredlog.F("acked_replicas", ackedReplicas),
			structuredlog.F("extent_write_duration_ms", extentDuration.Milliseconds()),
			structuredlog.F("metadata_commit_mode", "payload_only"),
		)
		if uint32(ackedReplicas) < extent.RequiredAcks {
			return payloadOnlyWriteResult{planDuration: planDuration, extentWriteDuration: extentWriteDuration, extentCount: len(plan.Extents)}, fmt.Errorf("payload-only extent %d did not reach quorum acked=%d required=%d", extent.Extent.ExtentID, ackedReplicas, extent.RequiredAcks)
		}
	}
	return payloadOnlyWriteResult{
		revision:            uint64(time.Now().UnixNano()),
		planDuration:        planDuration,
		extentWriteDuration: extentWriteDuration,
		extentCount:         len(plan.Extents),
	}, nil
}

func (c *Client) Flush(ctx context.Context, req *service.FlushRequest) (*service.FlushResponse, error) {
	if req == nil {
		return nil, badRequest("nil request")
	}
	if err := req.Validate(); err != nil {
		return nil, badRequest(err.Error())
	}
	if _, err := c.requireOpen(req.VolumeID, req.VolumeHandle, req.Context); err != nil {
		return nil, err
	}
	state, err := c.stateStore.GetVolumeState(ctx, req.VolumeID)
	if err != nil {
		return nil, err
	}
	c.rememberVolumeRevision(req.VolumeID, state.Revision)
	return &service.FlushResponse{
		Status:         "ok",
		VolumeRevision: state.Revision,
	}, nil
}

func (c *Client) Discard(ctx context.Context, req *service.DiscardRequest) (*service.DiscardResponse, error) {
	if req == nil {
		return nil, badRequest("nil request")
	}
	if err := req.Validate(); err != nil {
		return nil, badRequest(err.Error())
	}
	start := time.Now()
	lookupStart := time.Now()
	spec, err := c.lookupVolume(ctx, req.VolumeID)
	lookupDuration := time.Since(lookupStart)
	if err != nil {
		return nil, err
	}
	if !service.DiscardRangeAligned(spec, req.OffsetBytes, req.LengthBytes) {
		if clusterec.IsECVolume(spec) {
			return c.discardECZeroFallback(ctx, req, spec, start, lookupDuration)
		}
		resp, err := c.zeroLike(ctx, req.VolumeID, req.OffsetBytes, req.LengthBytes, req.Context, service.IOOperationDiscard)
		if err != nil {
			return nil, err
		}
		return &service.DiscardResponse{Status: "ok", VolumeRevision: resp.VolumeRevision}, nil
	}
	if clusterec.IsECVolume(spec) {
		return c.discardECTrueReclaim(ctx, req, spec, start, lookupDuration)
	}
	resp, err := c.zeroLike(ctx, req.VolumeID, req.OffsetBytes, req.LengthBytes, req.Context, service.IOOperationDiscard)
	if err != nil {
		return nil, err
	}
	return &service.DiscardResponse{Status: "ok", VolumeRevision: resp.VolumeRevision}, nil
}

func (c *Client) DiscardObservationFor(volume service.VolumeSpec, offsetBytes, lengthBytes uint64) service.DiscardObservation {
	volume = service.NormalizeVolumeSpec(volume)
	if !service.DiscardRangeAligned(volume, offsetBytes, lengthBytes) {
		return service.NewDiscardAlignmentZeroFallbackObservation(volume, offsetBytes, lengthBytes)
	}
	if volume.RedundancyBackend == service.RedundancyBackendEC {
		return service.NewDiscardTrueReclaimObservation(volume, offsetBytes, lengthBytes)
	}
	if volume.RedundancyBackend == service.RedundancyBackendReplicated && !c.preferPayloadOnlyWrites {
		return service.NewDiscardTrueReclaimObservation(volume, offsetBytes, lengthBytes)
	}
	return service.NewDiscardZeroFallbackObservation(volume, offsetBytes, lengthBytes)
}

func (c *Client) discardECZeroFallback(ctx context.Context, req *service.DiscardRequest, spec service.VolumeSpec, start time.Time, lookupDuration time.Duration) (*service.DiscardResponse, error) {
	current, err := c.requireOpen(req.VolumeID, "", req.Context)
	if err != nil {
		return nil, err
	}
	ecSvc, err := c.ecService(ctx, current)
	if err != nil {
		return nil, err
	}
	payload := make([]byte, req.LengthBytes)
	hash := zeroLikeZeroPayloadHash(service.IOOperationDiscard, req.OffsetBytes, req.LengthBytes)
	fallbackCtx := req.Context
	fallbackCtx.IdempotencyKey = fmt.Sprintf("%s-%s-%x", req.Context.IdempotencyKey, service.IOOperationDiscard, hash[:8])
	writeStart := time.Now()
	resp, err := ecSvc.Write(ctx, clusterec.WriteRequest{
		Volume:  spec,
		Context: fallbackCtx,
		Offset:  req.OffsetBytes,
		Length:  req.LengthBytes,
		Data:    payload,
	})
	writeDuration := time.Since(writeStart)
	if err != nil {
		structuredlog.Error("sbs.cluster.client", "ec_discard_zero_fallback_failed", err,
			structuredlog.F("request_id", req.Context.RequestID),
			structuredlog.F("trace_id", req.Context.TraceID),
			structuredlog.F("volume_id", req.VolumeID),
			structuredlog.F("attachment_id", req.Context.AttachmentID),
			structuredlog.F("generation", req.Context.Generation),
			structuredlog.F("idempotency_key", fallbackCtx.IdempotencyKey),
			structuredlog.F("offset_bytes", req.OffsetBytes),
			structuredlog.F("length_bytes", req.LengthBytes),
			structuredlog.F("duration_ms", time.Since(start).Milliseconds()),
			structuredlog.F("lookup_volume_duration_ms", lookupDuration.Milliseconds()),
			structuredlog.F("ec_zero_fallback_duration_ms", writeDuration.Milliseconds()),
		)
		return nil, err
	}
	c.rememberVolumeRevision(req.VolumeID, resp.Revision)
	structuredlog.Info("sbs.cluster.client", "ec_discard_zero_fallback_committed",
		structuredlog.F("request_id", req.Context.RequestID),
		structuredlog.F("trace_id", req.Context.TraceID),
		structuredlog.F("volume_id", req.VolumeID),
		structuredlog.F("attachment_id", req.Context.AttachmentID),
		structuredlog.F("generation", req.Context.Generation),
		structuredlog.F("idempotency_key", fallbackCtx.IdempotencyKey),
		structuredlog.F("offset_bytes", req.OffsetBytes),
		structuredlog.F("length_bytes", req.LengthBytes),
		structuredlog.F("revision", resp.Revision),
		structuredlog.F("duration_ms", time.Since(start).Milliseconds()),
		structuredlog.F("lookup_volume_duration_ms", lookupDuration.Milliseconds()),
		structuredlog.F("ec_zero_fallback_duration_ms", writeDuration.Milliseconds()),
	)
	return &service.DiscardResponse{Status: "ok", VolumeRevision: resp.Revision}, nil
}

func (c *Client) discardECTrueReclaim(ctx context.Context, req *service.DiscardRequest, spec service.VolumeSpec, start time.Time, lookupDuration time.Duration) (*service.DiscardResponse, error) {
	current, err := c.requireOpen(req.VolumeID, "", req.Context)
	if err != nil {
		return nil, err
	}
	ecSvc, err := c.ecService(ctx, current)
	if err != nil {
		return nil, err
	}
	discardStart := time.Now()
	resp, err := ecSvc.Discard(ctx, clusterec.DiscardRequest{
		Volume:  spec,
		Context: req.Context,
		Offset:  req.OffsetBytes,
		Length:  req.LengthBytes,
	})
	discardDuration := time.Since(discardStart)
	if err != nil {
		structuredlog.Error("sbs.cluster.client", "ec_discard_failed", err,
			structuredlog.F("request_id", req.Context.RequestID),
			structuredlog.F("trace_id", req.Context.TraceID),
			structuredlog.F("volume_id", req.VolumeID),
			structuredlog.F("attachment_id", req.Context.AttachmentID),
			structuredlog.F("generation", req.Context.Generation),
			structuredlog.F("idempotency_key", req.Context.IdempotencyKey),
			structuredlog.F("offset_bytes", req.OffsetBytes),
			structuredlog.F("length_bytes", req.LengthBytes),
			structuredlog.F("duration_ms", time.Since(start).Milliseconds()),
			structuredlog.F("lookup_volume_duration_ms", lookupDuration.Milliseconds()),
			structuredlog.F("ec_discard_duration_ms", discardDuration.Milliseconds()),
		)
		return nil, err
	}
	c.rememberVolumeRevision(req.VolumeID, resp.Revision)
	structuredlog.Info("sbs.cluster.client", "ec_discard_committed",
		structuredlog.F("request_id", req.Context.RequestID),
		structuredlog.F("trace_id", req.Context.TraceID),
		structuredlog.F("volume_id", req.VolumeID),
		structuredlog.F("attachment_id", req.Context.AttachmentID),
		structuredlog.F("generation", req.Context.Generation),
		structuredlog.F("idempotency_key", req.Context.IdempotencyKey),
		structuredlog.F("operation_id", resp.OperationID),
		structuredlog.F("stripe_id", resp.StripeID),
		structuredlog.F("offset_bytes", req.OffsetBytes),
		structuredlog.F("length_bytes", req.LengthBytes),
		structuredlog.F("revision", resp.Revision),
		structuredlog.F("retired_ec_object_count", len(resp.RetiredECObjects)),
		structuredlog.F("duration_ms", time.Since(start).Milliseconds()),
		structuredlog.F("lookup_volume_duration_ms", lookupDuration.Milliseconds()),
		structuredlog.F("ec_discard_duration_ms", discardDuration.Milliseconds()),
	)
	return &service.DiscardResponse{Status: "ok", VolumeRevision: resp.Revision}, nil
}

func (c *Client) Zero(ctx context.Context, req *service.ZeroRequest) (*service.ZeroResponse, error) {
	if req == nil {
		return nil, badRequest("nil request")
	}
	if err := req.Validate(); err != nil {
		return nil, badRequest(err.Error())
	}
	resp, err := c.zeroLike(ctx, req.VolumeID, req.OffsetBytes, req.LengthBytes, req.Context, service.IOOperationZero)
	if err != nil {
		return nil, err
	}
	return &service.ZeroResponse{Status: "ok", VolumeRevision: resp.VolumeRevision}, nil
}

func (c *Client) zeroLike(ctx context.Context, volumeID string, offsetBytes, lengthBytes uint64, reqCtx service.SBSRequestContext, op string) (*service.WriteResponse, error) {
	hash := zeroLikeZeroPayloadHash(op, offsetBytes, lengthBytes)
	return c.writeWithMode(ctx, &service.WriteRequest{
		VolumeID:     volumeID,
		VolumeHandle: "",
		OffsetBytes:  offsetBytes,
		LengthBytes:  lengthBytes,
		Context: service.SBSRequestContext{
			RequestID:      reqCtx.RequestID,
			GatewayID:      reqCtx.GatewayID,
			HostID:         reqCtx.HostID,
			SessionID:      reqCtx.SessionID,
			AttachmentID:   reqCtx.AttachmentID,
			Generation:     reqCtx.Generation,
			IdempotencyKey: fmt.Sprintf("%s-%s-%x", reqCtx.IdempotencyKey, op, hash[:8]),
		},
	}, true)
}

func validateClusterWriteRequest(req *service.WriteRequest, zeroSemantic bool) error {
	if !zeroSemantic || len(req.Data) > 0 {
		return req.Validate()
	}
	if _, err := volumeid.ParseLowercase(req.VolumeID); err != nil {
		return service.ErrSBSVolumeIDInvalid
	}
	if req.LengthBytes == 0 {
		return service.ErrSBSLengthRequired
	}
	return req.Context.Validate(true, true)
}

func (c *Client) lookupVolume(ctx context.Context, volumeID string) (service.VolumeSpec, error) {
	parsedID, err := volumeid.Parse(volumeID)
	if err != nil {
		return service.VolumeSpec{}, badRequest("invalid volume id")
	}
	cacheKey := service.CanonicalVolumeID(parsedID)
	now := time.Now()
	c.mu.RLock()
	if entry, ok := c.volumeCache[cacheKey]; ok {
		if entry.expiresAt.IsZero() || now.Before(entry.expiresAt) {
			spec := entry.spec
			c.mu.RUnlock()
			return spec, nil
		}
	}
	c.mu.RUnlock()

	if c.volumeLookup == nil {
		return service.VolumeSpec{}, notFound("volume not found")
	}
	spec, err := c.volumeLookup.GetVolume(ctx, parsedID)
	if err != nil {
		return service.VolumeSpec{}, err
	}
	spec = service.NormalizeVolumeSpec(spec)
	c.mu.Lock()
	c.volumeCache[cacheKey] = cachedVolumeSpec{
		spec:      spec,
		expiresAt: time.Now().Add(c.volumeCacheTTL),
	}
	c.mu.Unlock()
	return spec, nil
}

func (c *Client) requireOpen(volumeID, handle string, reqCtx service.SBSRequestContext) (openSession, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	current, ok := c.open[volumeID]
	if !ok {
		return openSession{}, attachmentMismatch("volume is not opened")
	}
	if current.attachmentID != reqCtx.AttachmentID {
		return openSession{}, attachmentMismatch("attachment mismatch")
	}
	if current.generation != reqCtx.Generation {
		return openSession{}, staleGeneration("generation mismatch")
	}
	if handle != "" && current.handle != handle {
		return openSession{}, attachmentMismatch("volume handle mismatch")
	}
	return current, nil
}

func (c *Client) refreshOpenSession(ctx context.Context, volumeID string) (openSession, error) {
	c.mu.RLock()
	current, ok := c.open[volumeID]
	c.mu.RUnlock()
	if !ok {
		return openSession{}, attachmentMismatch("volume is not opened")
	}
	structuredlog.Info("sbs.cluster.client", "refresh_open_session_started",
		structuredlog.F("volume_id", volumeID),
		structuredlog.F("attachment_id", current.attachmentID),
		structuredlog.F("generation", current.generation),
	)

	replicaClients, err := c.availableReplicaClients(ctx, volumeID)
	if err != nil {
		return openSession{}, err
	}
	replicas, err := replication.OpenReplicaSessions(ctx, replicaClients, replication.OpenReplicaSessionsRequest{
		VolumeID:      volumeID,
		GatewayID:     c.gatewayID,
		HostID:        c.hostID,
		ClientVersion: c.clientVersion,
		AttachmentID:  current.attachmentID,
		Generation:    current.generation,
		SessionPrefix: c.sessionPrefix,
		AccessMode:    service.SBSAccessModeExclusiveWriter,
	})
	if err != nil {
		structuredlog.Error("sbs.cluster.client", "refresh_open_session_failed", err,
			structuredlog.F("volume_id", volumeID),
			structuredlog.F("attachment_id", current.attachmentID),
			structuredlog.F("generation", current.generation),
		)
		return openSession{}, err
	}

	c.mu.Lock()
	previousReplicas := current.replicas
	current.replicas = replicas
	c.open[volumeID] = current
	c.mu.Unlock()
	c.closeRemovedReplicaSessions(ctx, volumeID, previousReplicas, replicas)
	structuredlog.Info("sbs.cluster.client", "refresh_open_session_completed",
		structuredlog.F("volume_id", volumeID),
		structuredlog.F("attachment_id", current.attachmentID),
		structuredlog.F("generation", current.generation),
		structuredlog.F("replica_count", len(replicas)),
	)
	return current, nil
}

func (c *Client) ecService(ctx context.Context, current openSession) (*clusterec.Service, error) {
	if c.ecStore == nil {
		return nil, service.ErrNotSupported
	}
	sessions, nodes, err := c.ecShardSessions(ctx, current)
	if err != nil {
		return nil, err
	}
	store := c.ecStore
	if nodes != nil {
		store = ecNodeMembershipSnapshotStore{MetadataStore: c.ecStore, nodes: nodes}
	}
	return clusterec.NewService(store, sessions), nil
}

func (c *Client) ecShardSessions(ctx context.Context, current openSession) (map[string]clusterec.ShardSession, []metadata.NodeMembershipRecord, error) {
	sessions := make(map[string]clusterec.ShardSession, len(current.replicas))
	for targetID, replica := range current.replicas {
		nodeID := replica.ReplicaID
		if nodeID == "" {
			nodeID = targetID
		}
		session := clusterec.ShardSession{
			NodeID:       nodeID,
			Client:       replica.Client,
			VolumeHandle: replica.VolumeHandle,
			GatewayID:    replica.GatewayID,
			HostID:       replica.HostID,
			SessionID:    replica.SessionID,
			AttachmentID: replica.AttachmentID,
			Generation:   replica.Generation,
		}
		if strings.TrimSpace(targetID) != "" {
			sessions[targetID] = session
		}
		if strings.TrimSpace(replica.ReplicaID) != "" {
			sessions[replica.ReplicaID] = session
		}
	}
	nodes, err := c.ecStore.ListNodeMemberships(ctx)
	if err != nil {
		return nil, nil, err
	}
	for _, node := range nodes {
		nodeID := strings.TrimSpace(node.NodeID)
		if nodeID == "" {
			continue
		}
		if _, ok := sessions[nodeID]; ok {
			continue
		}
		replicaID := strings.TrimSpace(node.ReplicaID)
		if replicaID == "" {
			continue
		}
		session, ok := sessions[replicaID]
		if !ok {
			continue
		}
		session.NodeID = nodeID
		sessions[nodeID] = session
	}
	return sessions, nodes, nil
}

type ecNodeMembershipSnapshotStore struct {
	clusterec.MetadataStore
	nodes []metadata.NodeMembershipRecord
}

func (s ecNodeMembershipSnapshotStore) ListNodeMemberships(context.Context) ([]metadata.NodeMembershipRecord, error) {
	return cloneNodeMembershipRecords(s.nodes), nil
}

func (c *Client) closeRemovedReplicaSessions(ctx context.Context, volumeID string, previous, current map[string]replication.RemoteReplica) {
	for replicaID, replica := range previous {
		if _, ok := current[replicaID]; ok {
			continue
		}
		_, err := replica.Client.CloseVolume(ctx, &service.CloseVolumeRequest{
			VolumeID:     replica.VolumeID,
			VolumeHandle: replica.VolumeHandle,
			Context: service.SBSRequestContext{
				RequestID:    fmt.Sprintf("cluster-refresh-close-%s-%s", volumeID, replicaID),
				GatewayID:    replica.GatewayID,
				HostID:       replica.HostID,
				SessionID:    replica.SessionID,
				AttachmentID: replica.AttachmentID,
				Generation:   replica.Generation,
			},
		})
		if err != nil {
			structuredlog.Error("sbs.cluster.client", "refresh_open_session_stale_close_failed", err,
				structuredlog.F("volume_id", volumeID),
				structuredlog.F("replica_id", replicaID),
			)
			continue
		}
		structuredlog.Info("sbs.cluster.client", "refresh_open_session_stale_closed",
			structuredlog.F("volume_id", volumeID),
			structuredlog.F("replica_id", replicaID),
		)
	}
}

func isRecoverableReplicaSessionError(err error) bool {
	var sbsErr *service.SBSError
	if !errors.As(err, &sbsErr) {
		return false
	}
	return sbsErr.Code == service.SBSErrorCodeAttachmentMismatch
}

func (c *Client) availableReplicaClients(ctx context.Context, volumeID string) (map[string]service.SBSClient, error) {
	if availableIDs, err := c.availableReplicaTargetIDsFromPublishedView(ctx, volumeID); err == nil && len(availableIDs) > 0 {
		out := filterReplicaClientsByTargetID(c.replicaClients, availableIDs)
		if len(out) > 0 {
			return out, nil
		}
		structuredlog.Info("sbs.cluster.client", "published_target_availability_empty_after_filter_fallback",
			structuredlog.F("volume_id", volumeID),
			structuredlog.F("published_target_count", len(availableIDs)),
			structuredlog.F("configured_replica_client_count", len(c.replicaClients)),
		)
	} else if err != nil {
		structuredlog.Info("sbs.cluster.client", "published_target_availability_unavailable_fallback",
			structuredlog.F("volume_id", volumeID),
			structuredlog.F("gateway_id", c.gatewayID),
			structuredlog.F("host_id", c.hostID),
			structuredlog.F("error", err.Error()),
		)
	}
	if availableIDs, err := c.availableReplicaTargetIDsFromFallbackSource(ctx, volumeID); err == nil && len(availableIDs) > 0 {
		out := filterReplicaClientsByTargetID(c.replicaClients, availableIDs)
		if len(out) > 0 {
			return out, nil
		}
		return nil, &service.SBSError{Code: service.SBSErrorCodeUnavailable, Message: "no available replica clients", Retryable: true}
	} else if err != nil {
		structuredlog.Info("sbs.cluster.client", "fallback_target_availability_unavailable",
			structuredlog.F("volume_id", volumeID),
			structuredlog.F("gateway_id", c.gatewayID),
			structuredlog.F("host_id", c.hostID),
			structuredlog.F("error", err.Error()),
		)
	}
	return cloneReplicaClients(c.replicaClients), nil
}

func (c *Client) availableReplicaTargetIDsFromPublishedView(ctx context.Context, volumeID string) (map[string]struct{}, error) {
	if c == nil || c.availability == nil || strings.TrimSpace(volumeID) == "" {
		return nil, nil
	}
	return c.availability.AvailableReplicaTargetIDs(ctx, volumeID)
}

func (c *Client) availableReplicaTargetIDsFromFallbackSource(ctx context.Context, volumeID string) (map[string]struct{}, error) {
	if c == nil || c.fallbackAvailability == nil || strings.TrimSpace(volumeID) == "" {
		return nil, nil
	}
	return c.fallbackAvailability.AvailableReplicaTargetIDs(ctx, volumeID)
}

func filterReplicaClientsByTargetID(clients map[string]service.SBSClient, availableIDs map[string]struct{}) map[string]service.SBSClient {
	out := make(map[string]service.SBSClient, len(clients))
	for replicaID, client := range clients {
		if _, ok := availableIDs[replicaID]; ok {
			out[replicaID] = client
		}
	}
	return out
}

func cloneReplicaClients(in map[string]service.SBSClient) map[string]service.SBSClient {
	out := make(map[string]service.SBSClient, len(in))
	for replicaID, client := range in {
		out[replicaID] = client
	}
	return out
}

func volumeHandle(volumeID, attachmentID string, generation uint64) string {
	return fmt.Sprintf("cvh-%s-%s-%d", volumeID, attachmentID, generation)
}

func profileFromSpec(spec service.VolumeSpec) service.SBSVolumeProfile {
	return service.SBSVolumeProfile{
		SizeBytes:       spec.SizeBytes,
		BlockSize:       spec.BlockSize,
		MaxIOSize:       spec.ExtentPageBytes,
		SupportsFlush:   true,
		SupportsDiscard: true,
		SupportsZero:    true,
		ConsistencyMode: "cluster-quorum-committed",
	}
}

func toSBSVolumeStatus(status metadata.VolumeStatus) (service.SBSVolumeState, bool, bool) {
	switch status {
	case metadata.VolumeStatusHealthy:
		return service.SBSVolumeStateReady, true, true
	case metadata.VolumeStatusDegraded:
		return service.SBSVolumeStateDegraded, true, true
	case metadata.VolumeStatusRepairing, metadata.VolumeStatusRebalancing:
		return service.SBSVolumeStateRecovering, true, true
	case metadata.VolumeStatusBlocked:
		return service.SBSVolumeStateUnavailable, false, false
	default:
		return service.SBSVolumeStateUnavailable, false, false
	}
}

func zeroLikeHash(op string, offsetBytes, lengthBytes uint64, data []byte) [32]byte {
	return sha256.Sum256(append([]byte(fmt.Sprintf("%s:%d:%d:", op, offsetBytes, lengthBytes)), data...))
}

func zeroLikeZeroPayloadHash(op string, offsetBytes, lengthBytes uint64) [32]byte {
	h := sha256.New()
	_, _ = h.Write([]byte(fmt.Sprintf("%s:%d:%d:", op, offsetBytes, lengthBytes)))
	var zero [32 * 1024]byte
	remaining := lengthBytes
	for remaining > 0 {
		n := minUint64(remaining, uint64(len(zero)))
		_, _ = h.Write(zero[:n])
		remaining -= n
	}
	var out [32]byte
	copy(out[:], h.Sum(nil))
	return out
}

func minUint64(a, b uint64) uint64 {
	if a < b {
		return a
	}
	return b
}

func badRequest(msg string) error {
	return &service.SBSError{Code: service.SBSErrorCodeBadRequest, Message: msg}
}

func attachmentMismatch(msg string) error {
	return &service.SBSError{Code: service.SBSErrorCodeAttachmentMismatch, Message: msg}
}

func staleGeneration(msg string) error {
	return &service.SBSError{Code: service.SBSErrorCodeStaleGeneration, Message: msg}
}

func notFound(msg string) error {
	return &service.SBSError{Code: service.SBSErrorCodeNotFound, Message: msg}
}

var _ service.SBSClient = (*Client)(nil)
