package iscsi

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/nosway/namrbd/gateway/service"
)

const (
	SBSBackendMode          = "sbs"
	SBSBackendAdapterName   = "sbs_client"
	SBSAlignmentOK          = "ok"
	SBSAlignmentRoundedDown = "capacity_rounded_down"
	SBSAlignmentRejected    = "rejected"

	SBSDefaultWireSafeMaxIOSize uint32 = 4*1024*1024 - 64*1024
)

type SBSAdapterConfig struct {
	ExportID                 string
	VolumeID                 string
	TargetIQN                string
	LUNID                    uint64
	LUNWWN                   string
	ISCSIGatewayID           string
	ActiveISCSIGatewayID     string
	ExportLeaseID            string
	ExportEpoch              uint64
	AttachmentID             string
	Generation               uint64
	SBSHostID                string
	SBSDeviceID              uint32
	SessionID                string
	LogicalBlockSize         uint64
	MaxTransferBytes         uint32
	OperationJSONLPath       string
	RegistryLoaded           bool
	RegistryAdminEndpoint    string
	RegistryRevision         uint64
	RegistryConfigGeneration uint64
	RegistryPortalID         string
	RegistryTargetFound      bool
	RegistryLUNFound         bool
	RegistryFailoverFound    bool
	ALUAMode                 string
	ALUAImplicitSupported    bool
	ALUAExplicitSupported    bool
	ALUATargetPortGroupID    uint16
	ALUAAccessState          string
	ALUAPreferred            bool
}

type SBSAdapterSummary struct {
	Result                              string   `json:"result"`
	Path                                string   `json:"path"`
	Entrypoint                          string   `json:"entrypoint,omitempty"`
	PortalAddress                       string   `json:"portal_address,omitempty"`
	BackendMode                         string   `json:"backend_mode"`
	BackendAdapter                      string   `json:"backend_adapter"`
	TargetIQN                           string   `json:"target_iqn"`
	LUNID                               uint64   `json:"lun_id"`
	LUNWWN                              string   `json:"lun_wwn"`
	ExportID                            string   `json:"export_id"`
	VolumeID                            string   `json:"volume_id"`
	ISCSIEdition                        string   `json:"iscsi_edition"`
	ExportVolumeLimit                   int      `json:"export_volume_limit"`
	BackendVolumeHandle                 string   `json:"backend_volume_handle,omitempty"`
	SBSHostID                           string   `json:"sbs_host_id"`
	SBSDeviceID                         uint32   `json:"sbs_device_id"`
	ISCSIGatewayID                      string   `json:"iscsi_gateway_id"`
	ActiveISCSIGatewayID                string   `json:"active_iscsi_gateway_id"`
	ExportLeaseID                       string   `json:"export_lease_id"`
	ExportEpoch                         uint64   `json:"export_epoch"`
	ISCSIRegistryLoaded                 bool     `json:"iscsi_registry_loaded"`
	ISCSIRegistryAdminEndpoint          string   `json:"iscsi_registry_admin_endpoint,omitempty"`
	ISCSIRegistryRevision               uint64   `json:"iscsi_registry_revision,omitempty"`
	ISCSIRegistryConfigGeneration       uint64   `json:"iscsi_registry_config_generation,omitempty"`
	ISCSIRegistryPortalID               string   `json:"iscsi_registry_portal_id,omitempty"`
	ISCSIRegistryTargetFound            bool     `json:"iscsi_registry_target_found,omitempty"`
	ISCSIRegistryLUNFound               bool     `json:"iscsi_registry_lun_found,omitempty"`
	ISCSIRegistryFailoverFound          bool     `json:"iscsi_registry_failover_found,omitempty"`
	ALUAMode                            string   `json:"alua_mode,omitempty"`
	ALUAImplicitSupported               bool     `json:"alua_implicit_supported"`
	ALUAExplicitSupported               bool     `json:"alua_explicit_supported"`
	ALUATargetPortGroupID               uint16   `json:"alua_target_port_group_id,omitempty"`
	ALUAAccessState                     string   `json:"alua_access_state,omitempty"`
	ALUAPreferred                       bool     `json:"alua_preferred"`
	WriterPolicy                        string   `json:"writer_policy"`
	HAFailoverMode                      string   `json:"ha_failover_mode"`
	ActivePathIOAllowed                 bool     `json:"active_path_io_allowed"`
	ActivePathWriteAllowed              bool     `json:"active_path_write_allowed"`
	StandbyPathIOAllowed                bool     `json:"standby_path_io_allowed"`
	StandbyPathWriteAllowed             bool     `json:"standby_path_write_allowed"`
	AttachmentID                        string   `json:"attachment_id"`
	Generation                          uint64   `json:"generation"`
	BackendProfileSizeBytes             uint64   `json:"backend_profile_size_bytes"`
	BackendProfileBlockSize             uint32   `json:"backend_profile_block_size"`
	BackendMaxIOSize                    uint32   `json:"backend_max_io_size"`
	BackendEffectiveMaxIOSize           uint32   `json:"backend_effective_max_io_size"`
	BackendSupportsFlush                bool     `json:"backend_supports_flush"`
	BackendSupportsDiscard              bool     `json:"backend_supports_discard"`
	BackendConsistencyMode              string   `json:"backend_consistency_mode"`
	BackendAlignmentResult              string   `json:"backend_alignment_result"`
	AdvertisedLUNBytes                  uint64   `json:"advertised_lun_bytes"`
	SBSVolumeRevision                   uint64   `json:"sbs_volume_revision"`
	BytesRead                           uint64   `json:"bytes_read"`
	BytesWritten                        uint64   `json:"bytes_written"`
	ZeroBytes                           uint64   `json:"zero_bytes"`
	FlushCount                          uint64   `json:"flush_count"`
	UnmapBytes                          uint64   `json:"unmap_bytes"`
	ReadbackMatched                     bool     `json:"readback_matched"`
	ZeroReadbackMatched                 bool     `json:"zero_readback_matched"`
	UnmapReadbackMatched                bool     `json:"unmap_readback_matched"`
	CloseRecorded                       bool     `json:"close_recorded"`
	StaleGatewayRejected                bool     `json:"stale_gateway_rejected"`
	StandbyWriteRejected                bool     `json:"standby_write_rejected"`
	SecurityRejected                    bool     `json:"security_rejected"`
	SBSErrorCode                        string   `json:"sbs_error_code,omitempty"`
	SBSErrorRetryable                   bool     `json:"sbs_error_retryable,omitempty"`
	SCSIStatus                          string   `json:"scsi_status"`
	SenseKey                            string   `json:"sense_key,omitempty"`
	ASC                                 string   `json:"asc,omitempty"`
	ASCQ                                string   `json:"ascq,omitempty"`
	FUAClaim                            string   `json:"fua_claim"`
	AuthPolicy                          string   `json:"auth_policy"`
	AuthMode                            string   `json:"auth_mode"`
	RuntimeCHAPSupported                bool     `json:"runtime_chap_supported"`
	AuthRuntimeClaim                    string   `json:"auth_runtime_claim"`
	RuntimeInitiatorAllowlistSupported  bool     `json:"runtime_initiator_allowlist_supported"`
	InitiatorAllowlistRuntimeClaim      string   `json:"initiator_allowlist_runtime_claim"`
	CHAPSecretRef                       string   `json:"chap_secret_ref,omitempty"`
	AllowedInitiatorIQNs                []string `json:"allowed_initiator_iqns"`
	TargetStack                         string   `json:"target_stack,omitempty"`
	TargetStackVersion                  string   `json:"target_stack_version,omitempty"`
	TargetStackAccepted                 bool     `json:"target_stack_accepted"`
	CompatibilityClaim                  string   `json:"compatibility_claim,omitempty"`
	SummaryJSONPath                     string   `json:"summary_json_path,omitempty"`
	OperationJSONLPath                  string   `json:"operation_jsonl_path,omitempty"`
	GotgtWildcardListenRequiresOverride bool     `json:"gotgt_wildcard_listen_requires_override,omitempty"`
	FirstError                          string   `json:"first_error,omitempty"`
	LastError                           string   `json:"last_error,omitempty"`
	OKCount                             int      `json:"ok_count"`
	ErrorCount                          int      `json:"error_count"`
	RemoteLabUsed                       bool     `json:"remote_lab_used"`
	ISCSIGatewayRestarted               bool     `json:"iscsi_gateway_restarted"`
	SBSServiceRestarted                 bool     `json:"sbs_service_restarted"`
	SBSDataRestarted                    bool     `json:"sbs_data_restarted"`
	KernelModuleReloaded                bool     `json:"kernel_module_reloaded"`
}

type SBSAdapterOperationRecord struct {
	Operation            string  `json:"operation"`
	Result               string  `json:"result"`
	BackendMode          string  `json:"backend_mode"`
	BackendAdapter       string  `json:"backend_adapter"`
	TargetIQN            string  `json:"target_iqn"`
	VolumeID             string  `json:"volume_id"`
	ExportID             string  `json:"export_id"`
	ISCSIGatewayID       string  `json:"iscsi_gateway_id"`
	ActiveISCSIGatewayID string  `json:"active_iscsi_gateway_id"`
	AttachmentID         string  `json:"attachment_id"`
	Generation           uint64  `json:"generation"`
	OffsetBytes          uint64  `json:"offset_bytes,omitempty"`
	LengthBytes          uint64  `json:"length_bytes,omitempty"`
	ChunkCount           int     `json:"chunk_count,omitempty"`
	MaxChunkBytes        uint64  `json:"max_chunk_bytes,omitempty"`
	EffectiveMaxIOSize   uint32  `json:"backend_effective_max_io_size,omitempty"`
	Bytes                uint64  `json:"bytes,omitempty"`
	PayloadBytes         *uint64 `json:"payload_bytes,omitempty"`
	ZeroSemantic         *bool   `json:"zero_semantic,omitempty"`
	VolumeRevision       uint64  `json:"sbs_volume_revision,omitempty"`
	SCSIStatus           string  `json:"scsi_status"`
	SenseKey             string  `json:"sense_key,omitempty"`
	ASC                  string  `json:"asc,omitempty"`
	ASCQ                 string  `json:"ascq,omitempty"`
	SBSErrorCode         string  `json:"sbs_error_code,omitempty"`
	SBSErrorMessage      string  `json:"sbs_error_message,omitempty"`
	SBSErrorRetryable    bool    `json:"sbs_error_retryable,omitempty"`
	StaleGatewayRejected bool    `json:"stale_gateway_rejected,omitempty"`
	StandbyWriteRejected bool    `json:"standby_write_rejected,omitempty"`
	SecurityRejected     bool    `json:"security_rejected,omitempty"`
	Error                string  `json:"error,omitempty"`
}

type SBSBackendAdapter struct {
	mu             sync.Mutex
	mutateMu       sync.Mutex
	client         service.SBSClient
	cfg            SBSAdapterConfig
	profile        service.SBSVolumeProfile
	volumeHandle   string
	volumeRevision uint64
	writerOpened   bool
	advertisedSize uint64
	alignment      string
	seq            uint64
	bytesRead      uint64
	bytesWritten   uint64
	zeroBytes      uint64
	flushCount     uint64
	unmapBytes     uint64
	closeRecorded  bool
	lastErr        error
	operations     []SBSAdapterOperationRecord
}

type SBSAdapterSelfTestOptions struct {
	VolumeID                string
	SizeBytes               uint64
	OffsetBytes             uint64
	LengthBytes             uint64
	ExerciseZero            bool
	ExerciseUNMAP           bool
	SecurityRejectOperation string
	SummaryJSONPath         string
	OperationJSONLPath      string
}

func OpenSBSBackendAdapter(ctx context.Context, client service.SBSClient, cfg SBSAdapterConfig) (*SBSBackendAdapter, SBSAdapterSummary, error) {
	if client == nil {
		return nil, SBSAdapterSummary{}, fmt.Errorf("sbs client is required")
	}
	cfg = normalizeSBSAdapterConfig(cfg)
	if err := validateSBSAdapterConfig(cfg); err != nil {
		return nil, SBSAdapterSummary{}, err
	}
	adapter := &SBSBackendAdapter{client: client, cfg: cfg}
	if !adapter.activePathAllowed() {
		profileResp, err := client.GetVolumeProfile(ctx, &service.GetVolumeProfileRequest{
			VolumeID: cfg.VolumeID,
			Context:  adapter.contextFor("profile", 0, 0, false),
		})
		if err != nil {
			summary := adapter.summary()
			adapter.recordError(&summary, err)
			return nil, summary, err
		}
		adapter.profile = profileResp.Profile
		adapter.advertisedSize, adapter.alignment = advertisedCapacity(profileResp.Profile.SizeBytes, cfg.LogicalBlockSize, uint64(profileResp.Profile.BlockSize))
		adapter.recordOperation(SBSAdapterOperationRecord{
			Operation: "profile",
			Result:    "ok",
		})
		summary := adapter.summary()
		summary.OKCount = 1
		return adapter, summary, nil
	}
	openResp, err := client.OpenVolume(ctx, &service.OpenVolumeRequest{
		VolumeID:   cfg.VolumeID,
		AccessMode: service.SBSAccessModeExclusiveWriter,
		Context:    adapter.contextFor("open", 0, 0, false),
	})
	if err != nil {
		summary := adapter.summary()
		adapter.recordError(&summary, err)
		return nil, summary, err
	}
	adapter.profile = openResp.Profile
	adapter.volumeHandle = openResp.VolumeHandle
	adapter.volumeRevision = openResp.VolumeRevision
	adapter.writerOpened = true
	adapter.advertisedSize, adapter.alignment = advertisedCapacity(openResp.Profile.SizeBytes, cfg.LogicalBlockSize, uint64(openResp.Profile.BlockSize))
	adapter.recordOperation(SBSAdapterOperationRecord{
		Operation:      "open",
		Result:         "ok",
		VolumeRevision: openResp.VolumeRevision,
	})
	summary := adapter.summary()
	summary.OKCount = 1
	return adapter, summary, nil
}

func (a *SBSBackendAdapter) Size() uint64 {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.advertisedSize
}

func (a *SBSBackendAdapter) ReadAt(p []byte, off int64) (int, error) {
	if off < 0 {
		err := fmt.Errorf("negative read offset")
		a.setLastError(err)
		return 0, err
	}
	if len(p) == 0 {
		return 0, nil
	}
	offset := uint64(off)
	length := uint64(len(p))
	if err := validateSBSAdapterLogicalRange(a.advertisedSize, a.cfg.LogicalBlockSize, offset, length); err != nil {
		a.setLastError(err)
		a.recordOperationError("read", offset, length, 0, err)
		return 0, err
	}
	if err := a.rejectInactivePath("read", offset, length); err != nil {
		a.setLastError(err)
		a.recordOperationError("read", offset, length, 0, err)
		return 0, err
	}
	backendOffset, backendLength := a.backendRangeFor(offset, length)
	data, chunks, err := a.readBackendRange(backendOffset, backendLength)
	if err != nil {
		a.setLastError(err)
		a.recordOperationError("read", offset, length, len(chunks), err)
		return 0, err
	}
	start := offset - backendOffset
	copy(p, data[start:start+length])
	a.mu.Lock()
	a.bytesRead += length
	a.mu.Unlock()
	a.recordOperation(SBSAdapterOperationRecord{
		Operation:      "read",
		Result:         "ok",
		OffsetBytes:    offset,
		LengthBytes:    length,
		ChunkCount:     len(chunks),
		MaxChunkBytes:  maxTransferChunkLength(chunks),
		Bytes:          length,
		VolumeRevision: a.currentVolumeRevision(),
	})
	return len(p), nil
}

func (a *SBSBackendAdapter) WriteAt(p []byte, off int64) (int, error) {
	if off < 0 {
		err := fmt.Errorf("negative write offset")
		a.setLastError(err)
		return 0, err
	}
	if len(p) == 0 {
		return 0, nil
	}
	offset := uint64(off)
	length := uint64(len(p))
	if err := validateSBSAdapterLogicalRange(a.advertisedSize, a.cfg.LogicalBlockSize, offset, length); err != nil {
		a.setLastError(err)
		a.recordOperationError("write", offset, length, 0, err)
		return 0, err
	}
	if err := a.rejectInactivePath("write", offset, length); err != nil {
		a.setLastError(err)
		a.recordOperationError("write", offset, length, 0, err)
		return 0, err
	}
	a.mutateMu.Lock()
	defer a.mutateMu.Unlock()

	chunks, err := a.writeLogicalRange(offset, p)
	if err != nil {
		a.setLastError(err)
		a.recordOperationError("write", offset, length, len(chunks), err)
		return 0, err
	}
	a.mu.Lock()
	a.bytesWritten += length
	a.mu.Unlock()
	a.recordOperation(SBSAdapterOperationRecord{
		Operation:      "write",
		Result:         "ok",
		OffsetBytes:    offset,
		LengthBytes:    length,
		ChunkCount:     len(chunks),
		MaxChunkBytes:  maxTransferChunkLength(chunks),
		Bytes:          length,
		VolumeRevision: a.currentVolumeRevision(),
	})
	return len(p), nil
}

func (a *SBSBackendAdapter) Zero(offset, length int64) (int, error) {
	if offset < 0 || length < 0 {
		err := IllegalRequestError("negative zero range")
		a.setLastError(err)
		return 0, err
	}
	if length == 0 {
		return 0, nil
	}
	logicalOffset := uint64(offset)
	logicalLength := uint64(length)
	if err := validateSBSAdapterLogicalRange(a.advertisedSize, a.cfg.LogicalBlockSize, logicalOffset, logicalLength); err != nil {
		a.setLastError(err)
		a.recordOperationError("zero", logicalOffset, logicalLength, 0, err)
		return 0, err
	}
	if err := a.rejectInactivePath("zero", logicalOffset, logicalLength); err != nil {
		a.setLastError(err)
		a.recordOperationError("zero", logicalOffset, logicalLength, 0, err)
		return 0, err
	}
	a.mutateMu.Lock()
	defer a.mutateMu.Unlock()

	zeroSemantic := a.backendAligned(logicalOffset, logicalLength)
	var chunks []sbsTransferChunk
	var err error
	if zeroSemantic {
		chunks, err = a.zeroBackendRange(logicalOffset, logicalLength)
	} else {
		chunks, err = a.zeroLogicalRangeWithWrite(logicalOffset, logicalLength)
	}
	if err != nil {
		a.setLastError(err)
		a.recordOperationError("zero", logicalOffset, logicalLength, len(chunks), err)
		return 0, err
	}
	payloadBytes := uint64(0)
	if !zeroSemantic {
		payloadBytes = transferChunkBytes(chunks)
	}
	zeroSemanticValue := zeroSemantic
	a.mu.Lock()
	a.zeroBytes += logicalLength
	a.mu.Unlock()
	a.recordOperation(SBSAdapterOperationRecord{
		Operation:      "zero",
		Result:         "ok",
		OffsetBytes:    logicalOffset,
		LengthBytes:    logicalLength,
		ChunkCount:     len(chunks),
		MaxChunkBytes:  maxTransferChunkLength(chunks),
		Bytes:          logicalLength,
		PayloadBytes:   &payloadBytes,
		ZeroSemantic:   &zeroSemanticValue,
		VolumeRevision: a.currentVolumeRevision(),
	})
	return 0, nil
}

func (a *SBSBackendAdapter) Sync() (int, error) {
	if err := a.rejectInactivePath("flush", 0, 0); err != nil {
		a.setLastError(err)
		a.recordOperationError("flush", 0, 0, 0, err)
		return 0, err
	}
	a.mutateMu.Lock()
	defer a.mutateMu.Unlock()

	resp, err := a.client.Flush(context.Background(), &service.FlushRequest{
		VolumeID:     a.cfg.VolumeID,
		VolumeHandle: a.volumeHandle,
		Context:      a.contextFor("flush", 0, 0, true),
	})
	if err != nil {
		a.setLastError(err)
		a.recordOperationError("flush", 0, 0, 0, err)
		return 0, err
	}
	a.mu.Lock()
	a.flushCount++
	a.volumeRevision = resp.VolumeRevision
	a.mu.Unlock()
	a.recordOperation(SBSAdapterOperationRecord{
		Operation:      "flush",
		Result:         "ok",
		VolumeRevision: resp.VolumeRevision,
	})
	return 0, nil
}

func (a *SBSBackendAdapter) Unmap(offset, length int64) (int, error) {
	if offset < 0 || length < 0 {
		err := IllegalRequestError("negative UNMAP range")
		a.setLastError(err)
		return 0, err
	}
	if length == 0 {
		return 0, nil
	}
	logicalOffset := uint64(offset)
	logicalLength := uint64(length)
	if err := validateSBSAdapterLogicalRange(a.advertisedSize, a.cfg.LogicalBlockSize, logicalOffset, logicalLength); err != nil {
		a.setLastError(err)
		a.recordOperationError("unmap", logicalOffset, logicalLength, 0, err)
		return 0, err
	}
	if err := a.rejectInactivePath("unmap", logicalOffset, logicalLength); err != nil {
		a.setLastError(err)
		a.recordOperationError("unmap", logicalOffset, logicalLength, 0, err)
		return 0, err
	}
	a.mutateMu.Lock()
	defer a.mutateMu.Unlock()

	chunks, err := a.unmapLogicalRange(logicalOffset, logicalLength)
	if err != nil {
		a.setLastError(err)
		a.recordOperationError("unmap", logicalOffset, logicalLength, len(chunks), err)
		return 0, err
	}
	a.mu.Lock()
	a.unmapBytes += logicalLength
	a.mu.Unlock()
	a.recordOperation(SBSAdapterOperationRecord{
		Operation:      "unmap",
		Result:         "ok",
		OffsetBytes:    logicalOffset,
		LengthBytes:    logicalLength,
		ChunkCount:     len(chunks),
		MaxChunkBytes:  maxTransferChunkLength(chunks),
		Bytes:          logicalLength,
		VolumeRevision: a.currentVolumeRevision(),
	})
	return 0, nil
}

func (a *SBSBackendAdapter) Close(ctx context.Context) (SBSAdapterSummary, error) {
	if !a.writerOpened {
		a.mu.Lock()
		a.closeRecorded = true
		a.mu.Unlock()
		a.recordOperation(SBSAdapterOperationRecord{
			Operation: "close",
			Result:    "ok",
		})
		summary := a.summary()
		summary.OKCount++
		return summary, nil
	}
	_, err := a.client.CloseVolume(ctx, &service.CloseVolumeRequest{
		VolumeID:     a.cfg.VolumeID,
		VolumeHandle: a.volumeHandle,
		Context:      a.contextFor("close", 0, 0, false),
	})
	a.mu.Lock()
	a.closeRecorded = err == nil
	a.mu.Unlock()
	summary := a.summary()
	if err != nil {
		a.recordOperationError("close", 0, 0, 0, err)
		a.recordError(&summary, err)
		return summary, err
	}
	a.recordOperation(SBSAdapterOperationRecord{
		Operation: "close",
		Result:    "ok",
	})
	summary.OKCount++
	return summary, nil
}

func (a *SBSBackendAdapter) Summary() SBSAdapterSummary {
	return a.summary()
}

func (a *SBSBackendAdapter) Operations() []SBSAdapterOperationRecord {
	a.mu.Lock()
	defer a.mu.Unlock()
	out := make([]SBSAdapterOperationRecord, len(a.operations))
	copy(out, a.operations)
	return out
}

func RunSBSAdapterSelfTest(opts SBSAdapterSelfTestOptions) (SBSAdapterSummary, error) {
	if opts.VolumeID == "" {
		opts.VolumeID = "00a1b2c3"
	}
	if opts.SizeBytes == 0 {
		opts.SizeBytes = 8 * 1024 * 1024
	}
	if opts.OffsetBytes == 0 {
		opts.OffsetBytes = 4096
	}
	if opts.LengthBytes == 0 {
		opts.LengthBytes = 8192
	}
	var client service.SBSClient = service.NewInMemorySBSClient([]service.VolumeSpec{{
		ID:              service.HexVolumeID(0x00a1b2c3),
		Name:            "phase-q-sbs-adapter-fixture",
		Prefix:          "phase-q",
		SizeBytes:       opts.SizeBytes,
		BlockSize:       service.DefaultBlockSize,
		ChunkSizeBytes:  service.DefaultAllocationChunkSize,
		ExtentPageBytes: service.DefaultAllocationPageSize,
		AccessMode:      service.VolumeAccessModeExclusive,
		State:           service.VolumeStateAvailable,
	}})
	if opts.SecurityRejectOperation != "" {
		client = &securityRejectSBSClient{
			SBSClient: client,
			operation: opts.SecurityRejectOperation,
		}
	}
	cfg := SBSAdapterConfig{
		ExportID:             "fixture",
		VolumeID:             opts.VolumeID,
		TargetIQN:            DefaultTargetIQN("fixture"),
		LUNID:                DefaultLUNID,
		LUNWWN:               LUNWWN("fixture"),
		ISCSIGatewayID:       "gw-fixture",
		ActiveISCSIGatewayID: "gw-fixture",
		ExportLeaseID:        "lease-fixture",
		ExportEpoch:          1,
		AttachmentID:         "att-00a1b2c3-0001",
		Generation:           1,
		SBSHostID:            "iscsi-export:fixture",
		SBSDeviceID:          1,
		SessionID:            "session-fixture",
		LogicalBlockSize:     DefaultLogicalBlock,
		OperationJSONLPath:   opts.OperationJSONLPath,
	}
	adapter, summary, err := OpenSBSBackendAdapter(context.Background(), client, cfg)
	if err != nil {
		summary.SummaryJSONPath = opts.SummaryJSONPath
		summary.OperationJSONLPath = opts.OperationJSONLPath
		_ = WriteSBSAdapterOperationsFile(opts.OperationJSONLPath, nil)
		_ = WriteSBSAdapterSummaryFile(opts.SummaryJSONPath, summary)
		return summary, err
	}
	finish := func(summary SBSAdapterSummary, err error) (SBSAdapterSummary, error) {
		summary.SummaryJSONPath = opts.SummaryJSONPath
		summary.OperationJSONLPath = opts.OperationJSONLPath
		if artifactErr := WriteSBSAdapterOperationsFile(opts.OperationJSONLPath, adapter.Operations()); artifactErr != nil && err == nil {
			summary.Result = "error"
			summary.ErrorCount = 1
			summary.FirstError = artifactErr.Error()
			summary.LastError = artifactErr.Error()
			return summary, artifactErr
		}
		if artifactErr := WriteSBSAdapterSummaryFile(opts.SummaryJSONPath, summary); artifactErr != nil && err == nil {
			summary.Result = "error"
			summary.ErrorCount = 1
			summary.FirstError = artifactErr.Error()
			summary.LastError = artifactErr.Error()
			return summary, artifactErr
		}
		return summary, err
	}
	pattern := []byte("namrbd-phase-q-sbs-adapter:")
	payload := bytes.Repeat(pattern, int((opts.LengthBytes/uint64(len(pattern)))+1))[:opts.LengthBytes]
	if _, err := adapter.WriteAt(payload, int64(opts.OffsetBytes)); err != nil {
		return finish(adapter.Summary(), err)
	}
	readback := make([]byte, opts.LengthBytes)
	if _, err := adapter.ReadAt(readback, int64(opts.OffsetBytes)); err != nil {
		return finish(adapter.Summary(), err)
	}
	matched := bytes.Equal(payload, readback)
	if !matched {
		err := fmt.Errorf("SBS adapter readback mismatch")
		adapter.setLastError(err)
		summary = adapter.Summary()
		summary.ReadbackMatched = false
		return finish(summary, err)
	}
	if _, err := adapter.Sync(); err != nil {
		summary = adapter.Summary()
		summary.ReadbackMatched = matched
		return finish(summary, err)
	}
	zeroMatched := false
	if opts.ExerciseZero {
		if _, err := adapter.Zero(int64(opts.OffsetBytes), int64(opts.LengthBytes)); err != nil {
			summary = adapter.Summary()
			summary.ReadbackMatched = matched
			return finish(summary, err)
		}
		afterZero := make([]byte, opts.LengthBytes)
		if _, err := adapter.ReadAt(afterZero, int64(opts.OffsetBytes)); err != nil {
			summary = adapter.Summary()
			summary.ReadbackMatched = matched
			return finish(summary, err)
		}
		zeroMatched = bytes.Equal(afterZero, make([]byte, opts.LengthBytes))
		if !zeroMatched {
			err := fmt.Errorf("SBS adapter zero readback mismatch")
			adapter.setLastError(err)
			summary = adapter.Summary()
			summary.ReadbackMatched = matched
			summary.ZeroReadbackMatched = false
			return finish(summary, err)
		}
	}
	unmapMatched := false
	if opts.ExerciseUNMAP {
		if opts.ExerciseZero {
			if _, err := adapter.WriteAt(payload, int64(opts.OffsetBytes)); err != nil {
				summary = adapter.Summary()
				summary.ReadbackMatched = matched
				summary.ZeroReadbackMatched = zeroMatched
				return finish(summary, err)
			}
		}
		if _, err := adapter.Unmap(int64(opts.OffsetBytes), int64(opts.LengthBytes)); err != nil {
			summary = adapter.Summary()
			summary.ReadbackMatched = matched
			summary.ZeroReadbackMatched = zeroMatched
			return finish(summary, err)
		}
		afterUnmap := make([]byte, opts.LengthBytes)
		if _, err := adapter.ReadAt(afterUnmap, int64(opts.OffsetBytes)); err != nil {
			summary = adapter.Summary()
			summary.ReadbackMatched = matched
			summary.ZeroReadbackMatched = zeroMatched
			return finish(summary, err)
		}
		unmapMatched = bytes.Equal(afterUnmap, make([]byte, opts.LengthBytes))
		if !unmapMatched {
			err := fmt.Errorf("SBS adapter UNMAP readback mismatch")
			adapter.setLastError(err)
			summary = adapter.Summary()
			summary.ReadbackMatched = matched
			summary.ZeroReadbackMatched = zeroMatched
			summary.UnmapReadbackMatched = false
			return finish(summary, err)
		}
	}
	summary, err = adapter.Close(context.Background())
	summary.ReadbackMatched = matched
	summary.ZeroReadbackMatched = zeroMatched
	summary.UnmapReadbackMatched = unmapMatched
	if err != nil {
		return finish(summary, err)
	}
	summary.OKCount = 4
	if opts.ExerciseZero {
		summary.OKCount += 2
	}
	if opts.ExerciseUNMAP {
		summary.OKCount += 2
		if opts.ExerciseZero {
			summary.OKCount++
		}
	}
	return finish(summary, nil)
}

func WriteSBSAdapterSummary(w io.Writer, summary SBSAdapterSummary) error {
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(summary)
}

func WriteSBSAdapterSummaryFile(path string, summary SBSAdapterSummary) error {
	if path == "" {
		return nil
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	if err := WriteSBSAdapterSummary(f, summary); err != nil {
		_ = f.Close()
		return err
	}
	return f.Close()
}

func WriteSBSAdapterOperationsFile(path string, operations []SBSAdapterOperationRecord) error {
	if path == "" {
		return nil
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	enc := json.NewEncoder(f)
	for _, op := range operations {
		if err := enc.Encode(op); err != nil {
			_ = f.Close()
			return err
		}
	}
	return f.Close()
}

type sbsTransferChunk struct {
	Offset uint64
	Length uint64
}

func (a *SBSBackendAdapter) transferChunks(offset, length uint64) ([]sbsTransferChunk, error) {
	if err := validateSBSAdapterBackendRange(a.profile, offset, length); err != nil {
		return nil, err
	}
	maxIO := uint64(a.effectiveMaxIOSize())
	if maxIO == 0 || maxIO > length {
		maxIO = length
	}
	if maxIO == 0 {
		return nil, fmt.Errorf("backend max_io_size is zero")
	}
	out := make([]sbsTransferChunk, 0, (length+maxIO-1)/maxIO)
	for remaining, at := length, offset; remaining > 0; {
		n := maxIO
		if n > remaining {
			n = remaining
		}
		out = append(out, sbsTransferChunk{Offset: at, Length: n})
		at += n
		remaining -= n
	}
	return out, nil
}

func (a *SBSBackendAdapter) backendBlockSize() uint64 {
	return normalizedBackendBlockSize(a.profile, a.cfg.LogicalBlockSize)
}

func (a *SBSBackendAdapter) backendRangeFor(offset, length uint64) (uint64, uint64) {
	blockSize := a.backendBlockSize()
	start := offset - offset%blockSize
	end := offset + length
	if rem := end % blockSize; rem != 0 {
		end += blockSize - rem
	}
	return start, end - start
}

func (a *SBSBackendAdapter) backendAligned(offset, length uint64) bool {
	blockSize := a.backendBlockSize()
	return offset%blockSize == 0 && length%blockSize == 0
}

func (a *SBSBackendAdapter) readBackendRange(offset, length uint64) ([]byte, []sbsTransferChunk, error) {
	chunks, err := a.transferChunks(offset, length)
	if err != nil {
		return nil, chunks, err
	}
	out := make([]byte, length)
	written := 0
	for _, chunk := range chunks {
		resp, err := a.client.Read(context.Background(), &service.ReadRequest{
			VolumeID:     a.cfg.VolumeID,
			VolumeHandle: a.volumeHandle,
			OffsetBytes:  chunk.Offset,
			LengthBytes:  chunk.Length,
			Context:      a.contextFor("read", chunk.Offset, chunk.Length, false),
		})
		if err != nil {
			return nil, chunks, err
		}
		copy(out[written:written+int(chunk.Length)], resp.Data)
		written += int(chunk.Length)
		a.mu.Lock()
		a.volumeRevision = resp.VolumeRevision
		a.mu.Unlock()
	}
	return out, chunks, nil
}

func (a *SBSBackendAdapter) writeBackendRange(offset uint64, p []byte) ([]sbsTransferChunk, error) {
	chunks, err := a.transferChunks(offset, uint64(len(p)))
	if err != nil {
		return chunks, err
	}
	written := 0
	for _, chunk := range chunks {
		data := append([]byte(nil), p[written:written+int(chunk.Length)]...)
		resp, err := a.client.Write(context.Background(), &service.WriteRequest{
			VolumeID:     a.cfg.VolumeID,
			VolumeHandle: a.volumeHandle,
			OffsetBytes:  chunk.Offset,
			LengthBytes:  chunk.Length,
			Data:         data,
			Context:      a.contextFor("write", chunk.Offset, chunk.Length, true),
		})
		if err != nil {
			return chunks, err
		}
		written += int(chunk.Length)
		a.mu.Lock()
		a.volumeRevision = resp.VolumeRevision
		a.mu.Unlock()
	}
	return chunks, nil
}

func (a *SBSBackendAdapter) writeLogicalRange(offset uint64, p []byte) ([]sbsTransferChunk, error) {
	length := uint64(len(p))
	if a.backendAligned(offset, length) {
		return a.writeBackendRange(offset, p)
	}
	backendOffset, backendLength := a.backendRangeFor(offset, length)
	block, readChunks, err := a.readBackendRange(backendOffset, backendLength)
	if err != nil {
		return readChunks, err
	}
	copy(block[offset-backendOffset:offset-backendOffset+length], p)
	writeChunks, err := a.writeBackendRange(backendOffset, block)
	if err != nil {
		return append(readChunks, writeChunks...), err
	}
	return writeChunks, nil
}

func (a *SBSBackendAdapter) discardBackendRange(offset, length uint64) ([]sbsTransferChunk, error) {
	chunks, err := a.transferChunks(offset, length)
	if err != nil {
		return chunks, err
	}
	for _, chunk := range chunks {
		resp, err := a.client.Discard(context.Background(), &service.DiscardRequest{
			VolumeID:    a.cfg.VolumeID,
			OffsetBytes: chunk.Offset,
			LengthBytes: chunk.Length,
			Context:     a.contextFor("discard", chunk.Offset, chunk.Length, true),
		})
		if err != nil {
			return chunks, err
		}
		a.mu.Lock()
		a.volumeRevision = resp.VolumeRevision
		a.mu.Unlock()
	}
	return chunks, nil
}

func (a *SBSBackendAdapter) zeroBackendRange(offset, length uint64) ([]sbsTransferChunk, error) {
	chunks, err := a.transferChunks(offset, length)
	if err != nil {
		return chunks, err
	}
	for _, chunk := range chunks {
		resp, err := a.client.Zero(context.Background(), &service.ZeroRequest{
			VolumeID:    a.cfg.VolumeID,
			OffsetBytes: chunk.Offset,
			LengthBytes: chunk.Length,
			Context:     a.contextFor("zero", chunk.Offset, chunk.Length, true),
		})
		if err != nil {
			return chunks, err
		}
		a.mu.Lock()
		a.volumeRevision = resp.VolumeRevision
		a.mu.Unlock()
	}
	return chunks, nil
}

func (a *SBSBackendAdapter) zeroLogicalRangeWithWrite(offset, length uint64) ([]sbsTransferChunk, error) {
	zeros := make([]byte, length)
	return a.writeLogicalRange(offset, zeros)
}

func (a *SBSBackendAdapter) unmapLogicalRange(offset, length uint64) ([]sbsTransferChunk, error) {
	if a.backendAligned(offset, length) {
		return a.discardBackendRange(offset, length)
	}

	blockSize := a.backendBlockSize()
	end := offset + length
	middleStart := offset
	var chunks []sbsTransferChunk

	if rem := offset % blockSize; rem != 0 {
		headLength := blockSize - rem
		if headLength > length {
			headLength = length
		}
		headChunks, err := a.zeroLogicalRangeWithWrite(offset, headLength)
		chunks = append(chunks, headChunks...)
		if err != nil {
			return chunks, err
		}
		middleStart += headLength
	}

	middleEnd := end - end%blockSize
	if middleEnd > middleStart {
		middleChunks, err := a.discardBackendRange(middleStart, middleEnd-middleStart)
		chunks = append(chunks, middleChunks...)
		if err != nil {
			return chunks, err
		}
	}

	if tailLength := end - middleEnd; tailLength > 0 && middleEnd >= middleStart {
		tailChunks, err := a.zeroLogicalRangeWithWrite(middleEnd, tailLength)
		chunks = append(chunks, tailChunks...)
		if err != nil {
			return chunks, err
		}
	}

	return chunks, nil
}

func maxTransferChunkLength(chunks []sbsTransferChunk) uint64 {
	var max uint64
	for _, chunk := range chunks {
		if chunk.Length > max {
			max = chunk.Length
		}
	}
	return max
}

func transferChunkBytes(chunks []sbsTransferChunk) uint64 {
	var total uint64
	for _, chunk := range chunks {
		total += chunk.Length
	}
	return total
}

func (a *SBSBackendAdapter) effectiveMaxIOSize() uint32 {
	return effectiveSBSMaxIOSize(a.profile, a.cfg.MaxTransferBytes)
}

func (a *SBSBackendAdapter) activePathAllowed() bool {
	local := strings.TrimSpace(a.cfg.ISCSIGatewayID)
	active := strings.TrimSpace(a.cfg.ActiveISCSIGatewayID)
	return local != "" && active != "" && local == active
}

func (a *SBSBackendAdapter) rejectInactivePath(operation string, offset, length uint64) error {
	if a.activePathAllowed() {
		return nil
	}
	local := strings.TrimSpace(a.cfg.ISCSIGatewayID)
	active := strings.TrimSpace(a.cfg.ActiveISCSIGatewayID)
	if local == "" {
		local = "<unset>"
	}
	if active == "" {
		active = "<unset>"
	}
	return StandbyGatewayError(fmt.Sprintf("iSCSI gateway %q is standby for active gateway %q during %s offset=%d length=%d", local, active, operation, offset, length))
}

func effectiveSBSMaxIOSize(profile service.SBSVolumeProfile, cfgLimit uint32) uint32 {
	profileMax := uint64(profile.MaxIOSize)
	if profileMax == 0 {
		return 0
	}
	maxIO := profileMax
	wireSafe := uint64(cfgLimit)
	if wireSafe == 0 {
		wireSafe = uint64(SBSDefaultWireSafeMaxIOSize)
	}
	if wireSafe > 0 && wireSafe < maxIO {
		maxIO = wireSafe
	}
	blockSize := uint64(profile.BlockSize)
	if blockSize == 0 {
		blockSize = DefaultLogicalBlock
	}
	if blockSize > 0 {
		maxIO -= maxIO % blockSize
	}
	if maxIO == 0 || maxIO > uint64(^uint32(0)) {
		return 0
	}
	return uint32(maxIO)
}

func (a *SBSBackendAdapter) contextFor(op string, offset, length uint64, idempotent bool) service.SBSRequestContext {
	a.mu.Lock()
	a.seq++
	seq := a.seq
	a.mu.Unlock()
	ctx := service.SBSRequestContext{
		RequestID:    fmt.Sprintf("iscsi-%s-%06d", op, seq),
		GatewayID:    a.cfg.ISCSIGatewayID,
		HostID:       a.cfg.SBSHostID,
		SessionID:    a.cfg.SessionID,
		AttachmentID: a.cfg.AttachmentID,
		Generation:   a.cfg.Generation,
		TraceID:      fmt.Sprintf("trace-iscsi-%s-%06d", op, seq),
	}
	if idempotent {
		ctx.IdempotencyKey = fmt.Sprintf("iscsi:%s:%s:%s:%d:%d:%d", a.cfg.ExportID, a.cfg.SessionID, op, offset, length, seq)
	}
	return ctx
}

func (a *SBSBackendAdapter) currentVolumeRevision() uint64 {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.volumeRevision
}

func (a *SBSBackendAdapter) recordOperation(record SBSAdapterOperationRecord) {
	record.BackendMode = SBSBackendMode
	record.BackendAdapter = SBSBackendAdapterName
	record.TargetIQN = a.cfg.TargetIQN
	record.VolumeID = a.cfg.VolumeID
	record.ExportID = a.cfg.ExportID
	record.ISCSIGatewayID = a.cfg.ISCSIGatewayID
	record.ActiveISCSIGatewayID = a.cfg.ActiveISCSIGatewayID
	record.AttachmentID = a.cfg.AttachmentID
	record.Generation = a.cfg.Generation
	if record.SCSIStatus == "" {
		record.SCSIStatus = "good"
	}
	if record.EffectiveMaxIOSize == 0 {
		record.EffectiveMaxIOSize = a.effectiveMaxIOSize()
	}
	a.mu.Lock()
	a.operations = append(a.operations, record)
	a.mu.Unlock()
}

func (a *SBSBackendAdapter) recordOperationError(operation string, offset, length uint64, chunkCount int, err error) {
	record := SBSAdapterOperationRecord{
		Operation:   operation,
		Result:      "error",
		OffsetBytes: offset,
		LengthBytes: length,
		ChunkCount:  chunkCount,
		Error:       sanitizedSBSError(err),
	}
	outcome := MapErrorToSCSI(err)
	record.SCSIStatus = outcome.Status
	record.SenseKey = outcome.SenseKey
	record.ASC = outcome.ASC
	record.ASCQ = outcome.ASCQ
	record.SBSErrorCode = outcome.SBSErrorCode
	record.SBSErrorRetryable = outcome.SBSErrorRetryable
	var sbsErr *service.SBSError
	if errors.As(err, &sbsErr) {
		record.SBSErrorMessage = sbsErr.Message
	}
	record.StaleGatewayRejected = outcome.StaleGatewayRejected
	record.StandbyWriteRejected = outcome.StandbyWriteRejected
	record.SecurityRejected = outcome.SecurityRejected
	a.recordOperation(record)
}

func (a *SBSBackendAdapter) summary() SBSAdapterSummary {
	a.mu.Lock()
	defer a.mu.Unlock()
	s := SBSAdapterSummary{
		Result:                             "ok",
		Path:                               "q-slice-003-sbs-backend-adapter",
		BackendMode:                        SBSBackendMode,
		BackendAdapter:                     SBSBackendAdapterName,
		TargetIQN:                          a.cfg.TargetIQN,
		LUNID:                              a.cfg.LUNID,
		LUNWWN:                             a.cfg.LUNWWN,
		ExportID:                           a.cfg.ExportID,
		VolumeID:                           a.cfg.VolumeID,
		ISCSIEdition:                       ISCSIEdition,
		ExportVolumeLimit:                  ISCSIExportVolumeLimit,
		BackendVolumeHandle:                a.volumeHandle,
		SBSHostID:                          a.cfg.SBSHostID,
		SBSDeviceID:                        a.cfg.SBSDeviceID,
		ISCSIGatewayID:                     a.cfg.ISCSIGatewayID,
		ActiveISCSIGatewayID:               a.cfg.ActiveISCSIGatewayID,
		ExportLeaseID:                      a.cfg.ExportLeaseID,
		ExportEpoch:                        a.cfg.ExportEpoch,
		ISCSIRegistryLoaded:                a.cfg.RegistryLoaded,
		ISCSIRegistryAdminEndpoint:         a.cfg.RegistryAdminEndpoint,
		ISCSIRegistryRevision:              a.cfg.RegistryRevision,
		ISCSIRegistryConfigGeneration:      a.cfg.RegistryConfigGeneration,
		ISCSIRegistryPortalID:              a.cfg.RegistryPortalID,
		ISCSIRegistryTargetFound:           a.cfg.RegistryTargetFound,
		ISCSIRegistryLUNFound:              a.cfg.RegistryLUNFound,
		ISCSIRegistryFailoverFound:         a.cfg.RegistryFailoverFound,
		ALUAMode:                           a.cfg.ALUAMode,
		ALUAImplicitSupported:              a.cfg.ALUAImplicitSupported,
		ALUAExplicitSupported:              a.cfg.ALUAExplicitSupported,
		ALUATargetPortGroupID:              a.cfg.ALUATargetPortGroupID,
		ALUAAccessState:                    a.cfg.ALUAAccessState,
		ALUAPreferred:                      a.cfg.ALUAPreferred,
		WriterPolicy:                       "single_active_writer_session",
		HAFailoverMode:                     "manual_promote_demote_first",
		ActivePathIOAllowed:                a.activePathAllowed(),
		ActivePathWriteAllowed:             a.activePathAllowed(),
		StandbyPathIOAllowed:               false,
		StandbyPathWriteAllowed:            false,
		AttachmentID:                       a.cfg.AttachmentID,
		Generation:                         a.cfg.Generation,
		BackendProfileSizeBytes:            a.profile.SizeBytes,
		BackendProfileBlockSize:            a.profile.BlockSize,
		BackendMaxIOSize:                   a.profile.MaxIOSize,
		BackendEffectiveMaxIOSize:          a.effectiveMaxIOSize(),
		BackendSupportsFlush:               a.profile.SupportsFlush,
		BackendSupportsDiscard:             a.profile.SupportsDiscard,
		BackendConsistencyMode:             a.profile.ConsistencyMode,
		BackendAlignmentResult:             a.alignment,
		AdvertisedLUNBytes:                 a.advertisedSize,
		SBSVolumeRevision:                  a.volumeRevision,
		BytesRead:                          a.bytesRead,
		BytesWritten:                       a.bytesWritten,
		ZeroBytes:                          a.zeroBytes,
		FlushCount:                         a.flushCount,
		UnmapBytes:                         a.unmapBytes,
		CloseRecorded:                      a.closeRecorded,
		OperationJSONLPath:                 a.cfg.OperationJSONLPath,
		SCSIStatus:                         "good",
		FUAClaim:                           "backend_write_ack",
		AuthPolicy:                         AuthPolicyNoAuthAllowlistFirst,
		AuthMode:                           AuthModeNone,
		RuntimeCHAPSupported:               false,
		AuthRuntimeClaim:                   AuthRuntimeClaimGotgtNoneOnly,
		RuntimeInitiatorAllowlistSupported: false,
		InitiatorAllowlistRuntimeClaim:     InitiatorAllowlistRuntimeClaimGotgtNoHook,
		AllowedInitiatorIQNs:               []string{},
		OKCount:                            1,
		RemoteLabUsed:                      false,
		ISCSIGatewayRestarted:              false,
		SBSServiceRestarted:                false,
		SBSDataRestarted:                   false,
		KernelModuleReloaded:               false,
	}
	if a.lastErr != nil {
		a.recordError(&s, a.lastErr)
	}
	return s
}

func (a *SBSBackendAdapter) setLastError(err error) {
	a.mu.Lock()
	a.lastErr = err
	a.mu.Unlock()
}

func (a *SBSBackendAdapter) recordError(summary *SBSAdapterSummary, err error) {
	if summary == nil || err == nil {
		return
	}
	summary.Result = "error"
	summary.SCSIStatus = "check_condition"
	summary.ErrorCount = 1
	summary.FirstError = sanitizedSBSError(err)
	summary.LastError = summary.FirstError
	outcome := MapErrorToSCSI(err)
	summary.SCSIStatus = outcome.Status
	summary.SenseKey = outcome.SenseKey
	summary.ASC = outcome.ASC
	summary.ASCQ = outcome.ASCQ
	summary.SBSErrorCode = outcome.SBSErrorCode
	summary.SBSErrorRetryable = outcome.SBSErrorRetryable
	summary.StaleGatewayRejected = outcome.StaleGatewayRejected
	summary.StandbyWriteRejected = outcome.StandbyWriteRejected
	summary.SecurityRejected = outcome.SecurityRejected
}

type securityRejectSBSClient struct {
	service.SBSClient
	operation string
}

func (c *securityRejectSBSClient) reject(operation string) error {
	if c == nil || c.operation != operation {
		return nil
	}
	return &service.SBSError{
		Code:    service.SBSErrorCodeSecurityRejected,
		Message: "security policy rejected " + operation,
	}
}

func (c *securityRejectSBSClient) Read(ctx context.Context, req *service.ReadRequest) (*service.ReadResponse, error) {
	if err := c.reject("read"); err != nil {
		return nil, err
	}
	return c.SBSClient.Read(ctx, req)
}

func (c *securityRejectSBSClient) Write(ctx context.Context, req *service.WriteRequest) (*service.WriteResponse, error) {
	if err := c.reject("write"); err != nil {
		return nil, err
	}
	return c.SBSClient.Write(ctx, req)
}

func (c *securityRejectSBSClient) Flush(ctx context.Context, req *service.FlushRequest) (*service.FlushResponse, error) {
	if err := c.reject("flush"); err != nil {
		return nil, err
	}
	return c.SBSClient.Flush(ctx, req)
}

func (c *securityRejectSBSClient) Discard(ctx context.Context, req *service.DiscardRequest) (*service.DiscardResponse, error) {
	if err := c.reject("unmap"); err != nil {
		return nil, err
	}
	return c.SBSClient.Discard(ctx, req)
}

func (c *securityRejectSBSClient) Zero(ctx context.Context, req *service.ZeroRequest) (*service.ZeroResponse, error) {
	if err := c.reject("zero"); err != nil {
		return nil, err
	}
	return c.SBSClient.Zero(ctx, req)
}

func normalizeSBSAdapterConfig(cfg SBSAdapterConfig) SBSAdapterConfig {
	if cfg.ExportID == "" {
		cfg.ExportID = DefaultExportID
	}
	if cfg.TargetIQN == "" {
		cfg.TargetIQN = DefaultTargetIQN(cfg.ExportID)
	}
	if cfg.LUNWWN == "" {
		cfg.LUNWWN = LUNWWN(cfg.ExportID)
	}
	if cfg.ISCSIGatewayID == "" {
		cfg.ISCSIGatewayID = cfg.ActiveISCSIGatewayID
	}
	if cfg.SBSHostID == "" {
		cfg.SBSHostID = "iscsi-export:" + cfg.ExportID
	}
	if cfg.SBSDeviceID == 0 {
		cfg.SBSDeviceID = StableSCSIDeviceID(cfg.LUNWWN)
	}
	if cfg.SessionID == "" {
		cfg.SessionID = newSBSAdapterSessionID(cfg.ExportID)
	}
	if cfg.LogicalBlockSize == 0 {
		cfg.LogicalBlockSize = DefaultLogicalBlock
	}
	if cfg.ALUAMode == "" {
		cfg.ALUAMode = ALUAModeImplicit
	}
	if !cfg.ALUAImplicitSupported && cfg.ALUAMode == ALUAModeImplicit {
		cfg.ALUAImplicitSupported = true
	}
	if cfg.ALUATargetPortGroupID == 0 {
		cfg.ALUATargetPortGroupID = 1
	}
	if cfg.ALUAAccessState == "" {
		if strings.TrimSpace(cfg.ISCSIGatewayID) != "" && strings.TrimSpace(cfg.ISCSIGatewayID) == strings.TrimSpace(cfg.ActiveISCSIGatewayID) {
			cfg.ALUAAccessState = ALUAAccessStateActiveOptimized
			cfg.ALUAPreferred = true
		} else {
			cfg.ALUAAccessState = ALUAAccessStateStandby
		}
	}
	return cfg
}

func newSBSAdapterSessionID(exportID string) string {
	var nonce [16]byte
	if _, err := rand.Read(nonce[:]); err == nil {
		return fmt.Sprintf("iscsi-session:%s:%s", exportID, hex.EncodeToString(nonce[:]))
	}
	return fmt.Sprintf("iscsi-session:%s:%d", exportID, time.Now().UnixNano())
}

func validateSBSAdapterConfig(cfg SBSAdapterConfig) error {
	switch {
	case cfg.VolumeID == "":
		return fmt.Errorf("volume_id is required")
	case cfg.ISCSIGatewayID == "":
		return fmt.Errorf("iscsi_gateway_id is required")
	case cfg.ActiveISCSIGatewayID == "":
		return fmt.Errorf("active_iscsi_gateway_id is required")
	case cfg.AttachmentID == "":
		return fmt.Errorf("attachment_id is required")
	case cfg.Generation == 0:
		return fmt.Errorf("generation must be >= 1")
	case cfg.LogicalBlockSize == 0:
		return fmt.Errorf("logical block size is required")
	}
	return nil
}

func advertisedCapacity(sizeBytes, logicalBlockSize, backendBlockSize uint64) (uint64, string) {
	alignmentUnit := capacityAlignmentUnit(logicalBlockSize, backendBlockSize)
	if alignmentUnit == 0 || sizeBytes%alignmentUnit == 0 {
		return sizeBytes, SBSAlignmentOK
	}
	return sizeBytes - (sizeBytes % alignmentUnit), SBSAlignmentRoundedDown
}

func capacityAlignmentUnit(logicalBlockSize, backendBlockSize uint64) uint64 {
	if logicalBlockSize == 0 {
		logicalBlockSize = DefaultLogicalBlock
	}
	if backendBlockSize == 0 {
		backendBlockSize = logicalBlockSize
	}
	divisor := gcd(logicalBlockSize, backendBlockSize)
	if divisor == 0 {
		return 0
	}
	return (logicalBlockSize / divisor) * backendBlockSize
}

func gcd(a, b uint64) uint64 {
	for b != 0 {
		a, b = b, a%b
	}
	return a
}

func normalizedBackendBlockSize(profile service.SBSVolumeProfile, logicalBlockSize uint64) uint64 {
	blockSize := uint64(profile.BlockSize)
	if blockSize == 0 {
		blockSize = logicalBlockSize
	}
	if blockSize == 0 {
		blockSize = DefaultLogicalBlock
	}
	return blockSize
}

func validateSBSAdapterLogicalRange(advertisedSize, logicalBlockSize, offset, length uint64) error {
	if length == 0 {
		return io.EOF
	}
	if offset > advertisedSize || length > advertisedSize-offset {
		return IllegalRequestError("iSCSI range outside advertised LUN capacity")
	}
	if logicalBlockSize == 0 {
		logicalBlockSize = DefaultLogicalBlock
	}
	if offset%logicalBlockSize != 0 || length%logicalBlockSize != 0 {
		return IllegalRequestError(fmt.Sprintf("iSCSI range is not aligned to logical block size %d", logicalBlockSize))
	}
	return nil
}

func validateSBSAdapterBackendRange(profile service.SBSVolumeProfile, offset, length uint64) error {
	if length == 0 {
		return io.EOF
	}
	if offset > profile.SizeBytes || length > profile.SizeBytes-offset {
		return IllegalRequestError("iSCSI range outside backend LUN capacity")
	}
	blockSize := normalizedBackendBlockSize(profile, DefaultLogicalBlock)
	if offset%blockSize != 0 || length%blockSize != 0 {
		return IllegalRequestError(fmt.Sprintf("iSCSI range is not aligned to SBS backend block size %d", blockSize))
	}
	return nil
}

func sanitizedSBSError(err error) string {
	var cond *SCSIConditionError
	if errors.As(err, &cond) {
		if cond.SenseKey == "" {
			return "scsi_condition"
		}
		return "scsi_condition:" + cond.SenseKey
	}
	var sbsErr *service.SBSError
	if errors.As(err, &sbsErr) {
		if sbsErr.Code == "" {
			return "sbs_error"
		}
		return "sbs_error:" + string(sbsErr.Code)
	}
	return err.Error()
}
