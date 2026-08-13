package service

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/nosway/namrbd/gateway/store"
	"github.com/nosway/namrbd/internal/structuredlog"
)

const defaultDetachCloseTimeout = 3 * time.Second

var (
	ErrVolumeNotFound         = errors.New("volume not found")
	ErrVolumeDisabled         = errors.New("volume is disabled for new attachments")
	ErrBadAlignment           = errors.New("offset/length must be 4KB aligned")
	ErrDiscardAlignment       = errors.New("discard offset/length must align to reclaim geometry")
	ErrOutOfRange             = errors.New("request out of volume range")
	ErrBadDataLength          = errors.New("data length does not match length_bytes")
	ErrHostIDRequired         = errors.New("host_id is required")
	ErrAttachmentIDRequired   = errors.New("attachment_id is required")
	ErrAttachConflict         = errors.New("volume already attached to another host")
	ErrDetachConflict         = errors.New("volume is attached to another host")
	ErrGatewayNotFound        = errors.New("gateway not found")
	ErrVolumeNameRequired     = errors.New("volume name is required")
	ErrVolumeNameConflict     = errors.New("volume name already exists")
	ErrVolumeNotDetached      = errors.New("volume must be detached for this operation")
	ErrVolumeHasObjects       = errors.New("volume has object metadata and cannot be deleted safely")
	ErrVolumeGeometryChange   = errors.New("volume geometry is immutable")
	ErrMetadataCASConflict    = errors.New("metadata compare-and-swap conflict")
	ErrWriterFenced           = errors.New("writer fenced by handoff")
	ErrNotSupported           = errors.New("operation not supported")
	ErrKeyAdmissionRejected   = errors.New("key admission rejected for attachment")
	ErrProtectedWriteRejected = errors.New("write rejected by protected state")
)

type ProtectedWriteRejection struct {
	VolumeID        string
	ProtectedState  string
	ReasonCode      string
	SealedObjectID  string
	SealOperationID string
	LifecycleState  string
}

func (e *ProtectedWriteRejection) Error() string {
	if e == nil {
		return ErrProtectedWriteRejected.Error()
	}
	parts := []string{ErrProtectedWriteRejected.Error()}
	if e.VolumeID != "" {
		parts = append(parts, "volume="+e.VolumeID)
	}
	if e.ProtectedState != "" {
		parts = append(parts, "protected_state="+e.ProtectedState)
	}
	if e.ReasonCode != "" {
		parts = append(parts, "reason_code="+e.ReasonCode)
	}
	if e.SealedObjectID != "" {
		parts = append(parts, "sealed_object_id="+e.SealedObjectID)
	}
	if e.SealOperationID != "" {
		parts = append(parts, "seal_operation_id="+e.SealOperationID)
	}
	if e.LifecycleState != "" {
		parts = append(parts, "lifecycle_state="+e.LifecycleState)
	}
	return strings.Join(parts, " ")
}

func (e *ProtectedWriteRejection) Unwrap() error {
	return ErrProtectedWriteRejected
}

func ProtectedWriteRejectionFromError(err error) (*ProtectedWriteRejection, bool) {
	var rejection *ProtectedWriteRejection
	if errors.As(err, &rejection) {
		return rejection, true
	}
	return nil, false
}

type VolumeState struct {
	VolumeID         uint64
	SizeBytes        uint64
	BlockSize        uint32
	ChunkSizeBytes   uint32
	ExtentPageBytes  uint32
	Generation       uint64
	AttachedHostID   string
	AttachmentID     string
	AttachedDeviceID uint32
}

type InitialZeroMapEvidence struct {
	Trusted           bool
	AllZero           bool
	GranuleBytes      uint32
	CheckedPageCount  int
	CheckedChunkCount uint64
}

type InitialZeroMapEvidenceProvider interface {
	InitialZeroMapEvidence(ctx context.Context, volume VolumeSpec) (InitialZeroMapEvidence, error)
}

type Service struct {
	metadata  MetadataRepository
	data      DataRepository
	gatewayID string
	metrics   *MetricsCollector
}

type unsupportedDataRepository struct{}

func New(kv store.KV, volumes []store.Volume) *Service {
	specs := make([]VolumeSpec, 0, len(volumes))
	for _, v := range volumes {
		specs = append(specs, VolumeSpec{
			ID:        HexVolumeID(v.ID),
			Prefix:    v.Prefix,
			SizeBytes: v.SizeBytes,
			BlockSize: DefaultBlockSize,
		})
	}
	if objects, ok := kv.(store.ObjectStore); ok {
		meta := NewInMemoryMetadataRepository(specs)
		return NewWithRepositoryOptions(meta, NewChunkExtentDataRepository(meta, objects), "")
	}
	return NewWithRepositoryOptions(NewInMemoryMetadataRepository(specs), unsupportedDataRepository{}, "")
}

func (unsupportedDataRepository) ReadAt(context.Context, VolumeSpec, uint64, uint64) ([]byte, error) {
	return nil, ErrNotSupported
}

func (unsupportedDataRepository) WriteAt(context.Context, VolumeSpec, uint64, uint64, []byte) error {
	return ErrNotSupported
}

func (unsupportedDataRepository) CloseAttachment(context.Context, uint64, AttachmentRecord) error {
	return nil
}

func (unsupportedDataRepository) ReloadAttachment(context.Context, VolumeSpec) error {
	return nil
}

func NewWithRepositories(metadata MetadataRepository, data DataRepository) *Service {
	return NewWithRepositoryOptions(metadata, data, "")
}

func NewWithRepositoryOptions(metadata MetadataRepository, data DataRepository, gatewayID string) *Service {
	return &Service{
		metadata:  metadata,
		data:      data,
		gatewayID: gatewayID,
		metrics:   NewMetricsCollector(),
	}
}

func (s *Service) MetricsSnapshot() MetricsSnapshot {
	if s == nil || s.metrics == nil {
		return MetricsSnapshot{ByOperation: map[string]OperationMetrics{}, Retry: map[string]uint64{}, RetrySummary: RetrySummary{}}
	}
	snapshot := s.metrics.Snapshot()
	if snapshot.Retry == nil {
		snapshot.Retry = map[string]uint64{}
	}
	if provider, ok := s.data.(interface{ RetryMetricsSnapshot() map[string]uint64 }); ok {
		for key, value := range provider.RetryMetricsSnapshot() {
			snapshot.Retry[key] = value
		}
	}
	snapshot.RetrySummary = summarizeRetryMetrics(snapshot.Retry)
	return snapshot
}

func summarizeRetryMetrics(retry map[string]uint64) RetrySummary {
	summary := RetrySummary{}
	for key, value := range retry {
		summary.TotalRetries += value
		switch key {
		case "open_unavailable_retry":
			summary.OpenUnavailableRetries += value
		default:
			summary.ReopenRetries += value
		}
	}
	return summary
}

func (s *Service) VolumeInfo(volumeID uint64) (VolumeSpec, error) {
	return s.volumeInfo(context.Background(), volumeID)
}

func (s *Service) VolumeState(volumeID uint64) (VolumeState, error) {
	return s.volumeState(context.Background(), volumeID)
}

func (s *Service) ReloadVolumeDataPath(ctx context.Context, volumeID uint64) (VolumeState, error) {
	v, err := s.volumeInfoFresh(ctx, volumeID)
	if err != nil {
		return VolumeState{}, err
	}
	if reloader, ok := s.data.(ReloadCapableDataRepository); ok {
		if err := reloader.ReloadAttachment(ctx, v); err != nil {
			return VolumeState{}, err
		}
	}
	return s.volumeStateFromSpec(ctx, v)
}

func (s *Service) volumeInfo(ctx context.Context, volumeID uint64) (VolumeSpec, error) {
	return s.metadata.GetVolume(ctx, volumeID)
}

func (s *Service) volumeInfoFresh(ctx context.Context, volumeID uint64) (VolumeSpec, error) {
	if repo, ok := s.metadata.(FreshVolumeMetadataRepository); ok {
		return repo.RefreshVolume(ctx, volumeID)
	}
	return s.metadata.GetVolume(ctx, volumeID)
}

func (s *Service) volumeState(ctx context.Context, volumeID uint64) (VolumeState, error) {
	v, err := s.metadata.GetVolume(ctx, volumeID)
	if err != nil {
		return VolumeState{}, err
	}
	return s.volumeStateFromSpec(ctx, v)
}

func (s *Service) volumeStateFromSpec(ctx context.Context, v VolumeSpec) (VolumeState, error) {
	volumeID := uint64(v.ID)
	st, err := s.metadata.GetAttachment(ctx, volumeID)
	if err != nil {
		return VolumeState{}, err
	}

	return VolumeState{
		VolumeID:         uint64(v.ID),
		SizeBytes:        v.SizeBytes,
		BlockSize:        v.BlockSize,
		ChunkSizeBytes:   v.ChunkSizeBytes,
		ExtentPageBytes:  v.ExtentPageBytes,
		Generation:       st.Generation,
		AttachedHostID:   st.HostID,
		AttachmentID:     st.AttachmentID,
		AttachedDeviceID: st.DeviceID,
	}, nil
}

func (s *Service) InitialZeroMapEvidenceForState(ctx context.Context, st VolumeState) (InitialZeroMapEvidence, error) {
	if s == nil {
		return InitialZeroMapEvidence{}, nil
	}
	provider, ok := s.data.(InitialZeroMapEvidenceProvider)
	if !ok {
		return InitialZeroMapEvidence{}, nil
	}
	spec := NormalizeVolumeSpec(VolumeSpec{
		ID:              HexVolumeID(st.VolumeID),
		SizeBytes:       st.SizeBytes,
		BlockSize:       st.BlockSize,
		ChunkSizeBytes:  st.ChunkSizeBytes,
		ExtentPageBytes: st.ExtentPageBytes,
	})
	return provider.InitialZeroMapEvidence(ctx, spec)
}

func (s *Service) Attach(volumeID uint64, hostID string, deviceID uint32) (VolumeState, error) {
	return s.AttachContext(context.Background(), volumeID, hostID, deviceID)
}

func (s *Service) AttachContext(ctx context.Context, volumeID uint64, hostID string, deviceID uint32) (VolumeState, error) {
	started := time.Now()
	var err error
	phase := "validate"
	var volumeInfoDuration time.Duration
	var statusDuration time.Duration
	var metadataAttachDuration time.Duration
	var handoffStatusGetDuration time.Duration
	var handoffStatusPutDuration time.Duration
	var volumeStateDuration time.Duration
	consumesHandoff := false
	defer func() {
		s.metrics.Record("attach", 0, started, err)
		fields := []structuredlog.Field{
			structuredlog.F("volume_id", CanonicalVolumeID(volumeID)),
			structuredlog.F("host_id", hostID),
			structuredlog.F("device_id", deviceID),
			structuredlog.F("gateway_id", s.gatewayID),
			structuredlog.F("phase", phase),
			structuredlog.F("duration_ms", time.Since(started).Milliseconds()),
			structuredlog.F("volume_info_duration_ms", volumeInfoDuration.Milliseconds()),
			structuredlog.F("get_status_duration_ms", statusDuration.Milliseconds()),
			structuredlog.F("metadata_attach_duration_ms", metadataAttachDuration.Milliseconds()),
			structuredlog.F("handoff_status_get_duration_ms", handoffStatusGetDuration.Milliseconds()),
			structuredlog.F("handoff_status_put_duration_ms", handoffStatusPutDuration.Milliseconds()),
			structuredlog.F("volume_state_duration_ms", volumeStateDuration.Milliseconds()),
			structuredlog.F("consumes_handoff", consumesHandoff),
		}
		if err != nil {
			structuredlog.Error("gateway.service", "attach_failed", err, fields...)
			return
		}
		structuredlog.Info("gateway.service", "attach_completed", fields...)
	}()
	if hostID == "" {
		err = ErrHostIDRequired
		return VolumeState{}, err
	}
	phase = "volume_info"
	phaseStarted := time.Now()
	v, err := s.volumeInfo(ctx, volumeID)
	volumeInfoDuration = time.Since(phaseStarted)
	if err != nil {
		return VolumeState{}, err
	}
	if v.State == VolumeStateDisabled {
		err = ErrVolumeDisabled
		return VolumeState{}, err
	}
	phase = "get_status"
	phaseStarted = time.Now()
	status, err := s.metadata.GetVolumeStatus(ctx, volumeID)
	statusDuration = time.Since(phaseStarted)
	if err != nil {
		return VolumeState{}, err
	}
	consumesHandoff = status.HandoffRequired && gatewayAllowedDuringHandoff(status, s.gatewayID)
	if status.HandoffRequired && !gatewayAllowedDuringHandoff(status, s.gatewayID) {
		err = fmt.Errorf("%w: volume=%s fencing_epoch=%d handoff_stage=%s handoff_reason=%s", ErrWriterFenced, CanonicalVolumeID(volumeID), status.WriterFencingEpoch, status.HandoffStage, status.HandoffReason)
		return VolumeState{}, err
	}

	phase = "metadata_attach"
	phaseStarted = time.Now()
	if _, err = s.metadata.Attach(ctx, AttachRequest{
		VolumeID:  volumeID,
		HostID:    hostID,
		DeviceID:  deviceID,
		GatewayID: s.gatewayID,
	}); err != nil {
		metadataAttachDuration = time.Since(phaseStarted)
		return VolumeState{}, err
	}
	metadataAttachDuration = time.Since(phaseStarted)
	if consumesHandoff {
		phase = "handoff_status_get"
		phaseStarted = time.Now()
		updatedStatus, getErr := s.metadata.GetVolumeStatus(ctx, volumeID)
		handoffStatusGetDuration = time.Since(phaseStarted)
		if getErr != nil {
			err = getErr
			return VolumeState{}, err
		}
		nowUnix := time.Now().Unix()
		if updatedStatus.CurrentGatewayID == s.gatewayID && updatedStatus.AttachmentGeneration > 0 {
			updatedStatus.HandoffAckedAtUnix = nowUnix
			updatedStatus.HandoffAckedGeneration = updatedStatus.AttachmentGeneration
		}
		if handoffCompletionSatisfied(updatedStatus) {
			updatedStatus.HandoffRequired = false
			updatedStatus.HandoffRequestedAtUnix = 0
			updatedStatus.HandoffAckedAtUnix = 0
			updatedStatus.HandoffAckedGeneration = 0
			updatedStatus.HandoffCompletionEligibleAtUnix = 0
			updatedStatus.HandoffStage = ""
			updatedStatus.HandoffReason = ""
			updatedStatus.HandoffTargetGatewaySet = nil
		} else {
			updatedStatus.HandoffStage = handoffStage(updatedStatus)
			updateHandoffCompletionEligibility(&updatedStatus, nowUnix)
			if updatedStatus.HandoffStage == "ready_to_complete" {
				updatedStatus.ControllerReconcileRequestedAtUnix = 0
				updatedStatus.ControllerReconcileReason = ""
				updatedStatus.ControllerReconcileScheduledAtUnix = updatedStatus.HandoffCompletionEligibleAtUnix
				updatedStatus.ControllerReconcileScheduledReason = "handoff_completion_ready"
			} else {
				updatedStatus.ControllerReconcileRequestedAtUnix = nowUnix
				updatedStatus.ControllerReconcileReason = "handoff_ack"
				updatedStatus.ControllerReconcileScheduledAtUnix = 0
				updatedStatus.ControllerReconcileScheduledReason = ""
			}
		}
		phase = "handoff_status_put"
		phaseStarted = time.Now()
		if putErr := s.metadata.PutVolumeStatus(ctx, updatedStatus); putErr != nil {
			handoffStatusPutDuration = time.Since(phaseStarted)
			err = putErr
			return VolumeState{}, err
		}
		handoffStatusPutDuration = time.Since(phaseStarted)
	}
	phase = "volume_state"
	phaseStarted = time.Now()
	state, stateErr := s.volumeState(ctx, volumeID)
	volumeStateDuration = time.Since(phaseStarted)
	if stateErr != nil {
		err = stateErr
		return VolumeState{}, err
	}
	phase = "completed"
	return state, nil
}

func (s *Service) Detach(volumeID uint64, hostID, attachmentID string) (VolumeState, error) {
	return s.DetachContext(context.Background(), volumeID, hostID, attachmentID)
}

func (s *Service) DetachContext(ctx context.Context, volumeID uint64, hostID, attachmentID string) (VolumeState, error) {
	started := time.Now()
	var err error
	defer func() { s.metrics.Record("detach", 0, started, err) }()
	if hostID == "" {
		err = ErrHostIDRequired
		return VolumeState{}, err
	}
	if attachmentID == "" {
		err = ErrAttachmentIDRequired
		return VolumeState{}, err
	}
	if _, err = s.volumeInfo(ctx, volumeID); err != nil {
		return VolumeState{}, err
	}
	attachment, err := s.metadata.GetAttachment(ctx, volumeID)
	if err != nil {
		return VolumeState{}, err
	}
	if attachment.HostID != "" && (attachment.HostID != hostID || attachment.AttachmentID != attachmentID) {
		err = ErrDetachConflict
		return VolumeState{}, err
	}
	if attachment.HostID != "" {
		if closer, ok := s.data.(DetachCapableDataRepository); ok {
			closeCtx, cancel := detachCloseContext(ctx)
			err = closer.CloseAttachment(closeCtx, volumeID, attachment)
			cancel()
			if err != nil {
				if !isDeferredDetachCloseError(err) {
					return VolumeState{}, err
				}
				structuredlog.Info("gateway.service", "detach_close_deferred",
					structuredlog.F("volume_id", CanonicalVolumeID(volumeID)),
					structuredlog.F("attachment_id", attachment.AttachmentID),
					structuredlog.F("generation", attachment.Generation),
					structuredlog.F("error", err.Error()),
				)
				err = nil
			}
		}
	} else if localCloser, ok := s.data.(LocalAttachmentDataRepository); ok {
		closeCtx, cancel := detachCloseContext(ctx)
		err = localCloser.CloseLocalAttachment(closeCtx, volumeID, hostID, attachmentID)
		cancel()
		if err != nil {
			if !isDeferredDetachCloseError(err) {
				return VolumeState{}, err
			}
			structuredlog.Info("gateway.service", "detach_local_close_deferred",
				structuredlog.F("volume_id", CanonicalVolumeID(volumeID)),
				structuredlog.F("attachment_id", attachmentID),
				structuredlog.F("error", err.Error()),
			)
			err = nil
		}
	}

	if _, err = s.metadata.Detach(ctx, DetachRequest{
		VolumeID:     volumeID,
		HostID:       hostID,
		AttachmentID: attachmentID,
	}); err != nil {
		return VolumeState{}, err
	}

	return s.volumeState(ctx, volumeID)
}

func detachCloseContext(parent context.Context) (context.Context, context.CancelFunc) {
	if parent == nil {
		parent = context.Background()
	}
	if deadline, ok := parent.Deadline(); ok && time.Until(deadline) <= defaultDetachCloseTimeout {
		return parent, func() {}
	}
	return context.WithTimeout(parent, defaultDetachCloseTimeout)
}

func isDeferredDetachCloseError(err error) bool {
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		return true
	}
	return isRetryableUnavailable(err)
}

func (s *Service) Read(ctx context.Context, volumeID, offsetBytes, lengthBytes uint64) ([]byte, error) {
	result, err := s.ReadResult(ctx, volumeID, offsetBytes, lengthBytes)
	if err != nil {
		return nil, err
	}
	if result.ZeroData && result.Data == nil {
		return make([]byte, lengthBytes), nil
	}
	return result.Data, nil
}

func (s *Service) ReadResult(ctx context.Context, volumeID, offsetBytes, lengthBytes uint64) (ReadResult, error) {
	started := time.Now()
	var err error
	defer func() { s.metrics.Record("read", lengthBytes, started, err) }()
	readAttribution := ReadPathAttributionFromContext(ctx)
	volumeInfoStarted := time.Now()
	v, err := s.VolumeInfo(volumeID)
	volumeInfoDuration := time.Since(volumeInfoStarted)
	if err != nil {
		if readAttribution {
			structuredlog.Error("gateway.service", "service_read_failed", err,
				structuredlog.F("volume_id", CanonicalVolumeID(volumeID)),
				structuredlog.F("offset_bytes", offsetBytes),
				structuredlog.F("length_bytes", lengthBytes),
				structuredlog.F("phase", "volume_info"),
				structuredlog.F("duration_ms", time.Since(started).Milliseconds()),
				structuredlog.F("volume_info_duration_ms", volumeInfoDuration.Milliseconds()),
			)
		}
		return ReadResult{}, err
	}
	validateStarted := time.Now()
	err = validateRange(v.BlockSize, v.SizeBytes, offsetBytes, lengthBytes)
	validateDuration := time.Since(validateStarted)
	if err != nil {
		if readAttribution {
			structuredlog.Error("gateway.service", "service_read_failed", err,
				structuredlog.F("volume_id", CanonicalVolumeID(volumeID)),
				structuredlog.F("offset_bytes", offsetBytes),
				structuredlog.F("length_bytes", lengthBytes),
				structuredlog.F("phase", "range_validation"),
				structuredlog.F("duration_ms", time.Since(started).Milliseconds()),
				structuredlog.F("volume_info_duration_ms", volumeInfoDuration.Milliseconds()),
				structuredlog.F("range_validation_duration_ms", validateDuration.Milliseconds()),
			)
		}
		return ReadResult{}, err
	}
	dataReadStarted := time.Now()
	if repo, ok := s.data.(ReadResultDataRepository); ok {
		var result ReadResult
		result, err = repo.ReadAtResult(ctx, v, offsetBytes, lengthBytes)
		dataReadDuration := time.Since(dataReadStarted)
		if err != nil {
			if readAttribution {
				structuredlog.Error("gateway.service", "service_read_failed", err,
					structuredlog.F("volume_id", CanonicalVolumeID(volumeID)),
					structuredlog.F("offset_bytes", offsetBytes),
					structuredlog.F("length_bytes", lengthBytes),
					structuredlog.F("phase", "data_repository_read_result"),
					structuredlog.F("duration_ms", time.Since(started).Milliseconds()),
					structuredlog.F("volume_info_duration_ms", volumeInfoDuration.Milliseconds()),
					structuredlog.F("range_validation_duration_ms", validateDuration.Milliseconds()),
					structuredlog.F("data_read_duration_ms", dataReadDuration.Milliseconds()),
					structuredlog.F("read_result_repository", true),
				)
			}
			return ReadResult{}, err
		}
		if readAttribution {
			structuredlog.Info("gateway.service", "service_read_completed",
				structuredlog.F("volume_id", CanonicalVolumeID(volumeID)),
				structuredlog.F("offset_bytes", offsetBytes),
				structuredlog.F("length_bytes", lengthBytes),
				structuredlog.F("duration_ms", time.Since(started).Milliseconds()),
				structuredlog.F("volume_info_duration_ms", volumeInfoDuration.Milliseconds()),
				structuredlog.F("range_validation_duration_ms", validateDuration.Milliseconds()),
				structuredlog.F("data_read_duration_ms", dataReadDuration.Milliseconds()),
				structuredlog.F("read_result_repository", true),
				structuredlog.F("zero_data", result.ZeroData),
				structuredlog.F("data_bytes", len(result.Data)),
			)
		}
		return result, nil
	}
	data, err := s.data.ReadAt(ctx, v, offsetBytes, lengthBytes)
	dataReadDuration := time.Since(dataReadStarted)
	if err != nil {
		if readAttribution {
			structuredlog.Error("gateway.service", "service_read_failed", err,
				structuredlog.F("volume_id", CanonicalVolumeID(volumeID)),
				structuredlog.F("offset_bytes", offsetBytes),
				structuredlog.F("length_bytes", lengthBytes),
				structuredlog.F("phase", "data_repository_read"),
				structuredlog.F("duration_ms", time.Since(started).Milliseconds()),
				structuredlog.F("volume_info_duration_ms", volumeInfoDuration.Milliseconds()),
				structuredlog.F("range_validation_duration_ms", validateDuration.Milliseconds()),
				structuredlog.F("data_read_duration_ms", dataReadDuration.Milliseconds()),
				structuredlog.F("read_result_repository", false),
			)
		}
		return ReadResult{}, err
	}
	if readAttribution {
		structuredlog.Info("gateway.service", "service_read_completed",
			structuredlog.F("volume_id", CanonicalVolumeID(volumeID)),
			structuredlog.F("offset_bytes", offsetBytes),
			structuredlog.F("length_bytes", lengthBytes),
			structuredlog.F("duration_ms", time.Since(started).Milliseconds()),
			structuredlog.F("volume_info_duration_ms", volumeInfoDuration.Milliseconds()),
			structuredlog.F("range_validation_duration_ms", validateDuration.Milliseconds()),
			structuredlog.F("data_read_duration_ms", dataReadDuration.Milliseconds()),
			structuredlog.F("read_result_repository", false),
			structuredlog.F("zero_data", false),
			structuredlog.F("data_bytes", len(data)),
		)
	}
	return ReadResult{Data: data}, nil
}

func (s *Service) ReadClone(ctx context.Context, volumeID uint64, cloneID string, offsetBytes, lengthBytes uint64) ([]byte, error) {
	started := time.Now()
	var err error
	defer func() { s.metrics.Record("clone_read", lengthBytes, started, err) }()
	cloneID = strings.TrimSpace(cloneID)
	if cloneID == "" {
		return nil, fmt.Errorf("clone_id is required")
	}
	v, err := s.VolumeInfo(volumeID)
	if err != nil {
		return nil, err
	}
	err = validateRange(v.BlockSize, v.SizeBytes, offsetBytes, lengthBytes)
	if err != nil {
		return nil, err
	}
	cloneData, ok := s.data.(CloneDataRepository)
	if !ok {
		return nil, ErrNotSupported
	}
	data, err := cloneData.ReadCloneAt(ctx, v, cloneID, offsetBytes, lengthBytes)
	if err != nil {
		return nil, err
	}
	return data, nil
}

func (s *Service) ReadSnapshot(ctx context.Context, volumeID uint64, snapshotID string, offsetBytes, lengthBytes uint64) ([]byte, error) {
	started := time.Now()
	var err error
	defer func() { s.metrics.Record("snapshot_read", lengthBytes, started, err) }()
	snapshotID = strings.TrimSpace(snapshotID)
	if snapshotID == "" {
		return nil, fmt.Errorf("snapshot_id is required")
	}
	v, err := s.VolumeInfo(volumeID)
	if err != nil {
		return nil, err
	}
	err = validateRange(v.BlockSize, v.SizeBytes, offsetBytes, lengthBytes)
	if err != nil {
		return nil, err
	}
	snapshotData, ok := s.data.(SnapshotDataRepository)
	if !ok {
		return nil, ErrNotSupported
	}
	data, err := snapshotData.ReadSnapshotAt(ctx, v, snapshotID, offsetBytes, lengthBytes)
	if err != nil {
		return nil, err
	}
	return data, nil
}

func (s *Service) Write(ctx context.Context, volumeID, offsetBytes, lengthBytes uint64, data []byte) error {
	started := time.Now()
	var err error
	defer func() { s.metrics.Record("write", lengthBytes, started, err) }()
	v, err := s.VolumeInfo(volumeID)
	if err != nil {
		return err
	}
	err = validateRange(v.BlockSize, v.SizeBytes, offsetBytes, lengthBytes)
	if err != nil {
		return err
	}
	if uint64(len(data)) != lengthBytes {
		return ErrBadDataLength
	}
	if rejection, rejected := protectedWriteRejectionForVolume(v); rejected {
		return rejection
	}
	err = s.data.WriteAt(ctx, v, offsetBytes, lengthBytes, data)
	return err
}

func protectedWriteRejectionForVolume(volume VolumeSpec) (*ProtectedWriteRejection, bool) {
	if volume.ProtectedState == nil {
		return nil, false
	}
	protectedState := volume.ProtectedState.Normalize()
	reasonCode, rejected := protectedState.WriteRejectionReason()
	if !rejected {
		return nil, false
	}
	return &ProtectedWriteRejection{
		VolumeID:        CanonicalVolumeID(uint64(volume.ID)),
		ProtectedState:  string(protectedState.State),
		ReasonCode:      reasonCode,
		SealedObjectID:  protectedState.SealedObjectID,
		SealOperationID: protectedState.SealOperationID,
		LifecycleState:  protectedState.LifecycleState,
	}, true
}

func (s *Service) WriteClone(ctx context.Context, volumeID uint64, cloneID string, offsetBytes, lengthBytes uint64, data []byte) error {
	started := time.Now()
	var err error
	defer func() { s.metrics.Record("clone_write", lengthBytes, started, err) }()
	cloneID = strings.TrimSpace(cloneID)
	if cloneID == "" {
		return fmt.Errorf("clone_id is required")
	}
	v, err := s.VolumeInfo(volumeID)
	if err != nil {
		return err
	}
	err = validateRange(v.BlockSize, v.SizeBytes, offsetBytes, lengthBytes)
	if err != nil {
		return err
	}
	if uint64(len(data)) != lengthBytes {
		return ErrBadDataLength
	}
	cloneData, ok := s.data.(CloneDataRepository)
	if !ok {
		return ErrNotSupported
	}
	err = cloneData.WriteCloneAt(ctx, v, cloneID, offsetBytes, lengthBytes, data)
	return err
}

func (s *Service) Flush(ctx context.Context, volumeID uint64) error {
	started := time.Now()
	var err error
	defer func() { s.metrics.Record("flush", 0, started, err) }()
	v, err := s.VolumeInfo(volumeID)
	if err != nil {
		return err
	}
	if flusher, ok := s.data.(FlushCapableDataRepository); ok {
		err = flusher.FlushVolume(ctx, v)
		return err
	}
	return nil
}

func (s *Service) Discard(ctx context.Context, volumeID, offsetBytes, lengthBytes uint64) error {
	started := time.Now()
	var err error
	defer func() { s.metrics.Record("discard", lengthBytes, started, err) }()
	v, err := s.VolumeInfo(volumeID)
	if err != nil {
		return err
	}
	err = validateRange(v.BlockSize, v.SizeBytes, offsetBytes, lengthBytes)
	if err != nil {
		return err
	}
	discardObservation := NewDiscardZeroFallbackObservation(v, offsetBytes, lengthBytes)
	if provider, ok := s.data.(DiscardObservationProvider); ok {
		discardObservation = provider.DiscardObservationFor(v, offsetBytes, lengthBytes)
	}
	discarder, hasDiscarder := s.data.(DiscardCapableDataRepository)
	if discardObservation.Policy == DiscardPolicyTrueReclaim && !hasDiscarder {
		discardObservation = NewDiscardZeroFallbackObservation(v, offsetBytes, lengthBytes)
	}
	s.metrics.RecordDiscardObservation(discardObservation)
	if discardObservation.Policy == DiscardPolicyPartialReject {
		err = NewDiscardAlignmentError(v, offsetBytes, lengthBytes)
		return err
	}
	if discardObservation.Policy == DiscardPolicyTrueReclaim {
		err = discarder.DiscardAt(ctx, v, offsetBytes, lengthBytes)
		return err
	}
	if zeroer, ok := s.data.(ZeroCapableDataRepository); ok {
		err = zeroer.ZeroAt(ctx, v, offsetBytes, lengthBytes)
		return err
	}
	zeroes := make([]byte, lengthBytes)
	err = s.data.WriteAt(ctx, v, offsetBytes, lengthBytes, zeroes)
	return err
}

func (s *Service) Zero(ctx context.Context, volumeID, offsetBytes, lengthBytes uint64) error {
	started := time.Now()
	var err error
	defer func() { s.metrics.Record("zero", lengthBytes, started, err) }()
	v, err := s.VolumeInfo(volumeID)
	if err != nil {
		return err
	}
	err = validateRange(v.BlockSize, v.SizeBytes, offsetBytes, lengthBytes)
	if err != nil {
		return err
	}
	s.metrics.RecordDiscardObservation(NewZeroObservation(v, offsetBytes, lengthBytes))
	if zeroer, ok := s.data.(ZeroCapableDataRepository); ok {
		err = zeroer.ZeroAt(ctx, v, offsetBytes, lengthBytes)
		return err
	}
	zeroes := make([]byte, lengthBytes)
	err = s.data.WriteAt(ctx, v, offsetBytes, lengthBytes, zeroes)
	return err
}

func validateRange(blockSize uint32, volumeSize, offsetBytes, lengthBytes uint64) error {
	if blockSize == 0 {
		blockSize = DefaultBlockSize
	}
	if lengthBytes == 0 {
		return ErrBadAlignment
	}
	if offsetBytes%uint64(blockSize) != 0 || lengthBytes%uint64(blockSize) != 0 {
		return ErrBadAlignment
	}
	if offsetBytes >= volumeSize || offsetBytes+lengthBytes > volumeSize {
		return ErrOutOfRange
	}
	return nil
}

func FormatAttachmentID(volumeID, generation uint64) string {
	return fmt.Sprintf("att-%s-%04d", CanonicalVolumeID(volumeID), generation)
}
