package iscsi

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
)

const (
	DefaultMemoryLUNBytes uint64 = 512 * 1024 * 1024
	DefaultLogicalBlock   uint64 = 512
	DefaultExportID              = "memory"
	DefaultLUNID          uint64 = 0
	TargetStack                  = "gotgt"
	TargetStackVersion           = "v0.2.2"
)

type MemoryOptions struct {
	Portal             string
	MemoryLUNBytes     uint64
	ExportID           string
	TargetIQN          string
	OperationJSONLPath string
	SummaryJSONPath    string
	InitiatorOS        string
}

type Summary struct {
	Result                              string   `json:"result"`
	Path                                string   `json:"path"`
	Entrypoint                          string   `json:"entrypoint"`
	TargetIQN                           string   `json:"target_iqn"`
	PortalAddress                       string   `json:"portal_address"`
	LUNID                               uint64   `json:"lun_id"`
	LUNWWN                              string   `json:"lun_wwn"`
	ExportID                            string   `json:"export_id"`
	VolumeID                            string   `json:"volume_id"`
	BackendMode                         string   `json:"backend_mode"`
	MemoryLUNBytes                      uint64   `json:"memory_lun_bytes"`
	MemoryBackendSeed                   string   `json:"memory_backend_seed"`
	MemoryBackendPersistence            string   `json:"memory_backend_persistence"`
	InitiatorOS                         string   `json:"initiator_os"`
	TargetStack                         string   `json:"target_stack"`
	TargetStackVersion                  string   `json:"target_stack_version"`
	TargetStackAccepted                 bool     `json:"target_stack_accepted"`
	TargetStackUpstreamEvidence         string   `json:"target_stack_upstream_evidence"`
	TargetLUNModel                      string   `json:"target_lun_model"`
	TargetIQNSource                     string   `json:"target_iqn_source"`
	PortalListenRequired                bool     `json:"portal_listen_required"`
	MacOSSupportClaimed                 bool     `json:"macos_support_claimed"`
	SCSIAllowlistProfile                string   `json:"scsi_allowlist_profile"`
	LogicalBlockSizeBytes               uint64   `json:"logical_block_size_bytes"`
	FUAClaim                            string   `json:"fua_claim"`
	UNMAPPolicy                         string   `json:"unmap_policy"`
	PRPolicy                            string   `json:"pr_policy"`
	MPIOALUASupported                   bool     `json:"mpio_alua_supported"`
	ISCSIERL                            int      `json:"iscsi_erl"`
	SessionConnectionPolicy             string   `json:"session_connection_policy"`
	HeaderDigestEnabled                 bool     `json:"header_digest_enabled"`
	DataDigestEnabled                   bool     `json:"data_digest_enabled"`
	ISCSIEdition                        string   `json:"iscsi_edition"`
	ExportVolumeLimit                   int      `json:"export_volume_limit"`
	MetadataAuthority                   string   `json:"metadata_authority"`
	AuthPolicy                          string   `json:"auth_policy"`
	AuthMode                            string   `json:"auth_mode"`
	RuntimeCHAPSupported                bool     `json:"runtime_chap_supported"`
	AuthRuntimeClaim                    string   `json:"auth_runtime_claim"`
	RuntimeInitiatorAllowlistSupported  bool     `json:"runtime_initiator_allowlist_supported"`
	InitiatorAllowlistRuntimeClaim      string   `json:"initiator_allowlist_runtime_claim"`
	CHAPSecretRef                       string   `json:"chap_secret_ref,omitempty"`
	AllowedInitiatorIQNs                []string `json:"allowed_initiator_iqns"`
	WriterPolicy                        string   `json:"writer_policy"`
	HAFailoverMode                      string   `json:"ha_failover_mode"`
	SummaryJSONPath                     string   `json:"summary_json_path,omitempty"`
	OperationJSONLPath                  string   `json:"operation_jsonl_path,omitempty"`
	AppendixCRequired                   bool     `json:"appendix_c_required"`
	SCSIOpcode                          string   `json:"scsi_opcode,omitempty"`
	SCSIStatus                          string   `json:"scsi_status"`
	SenseKey                            string   `json:"sense_key,omitempty"`
	ASC                                 string   `json:"asc,omitempty"`
	ASCQ                                string   `json:"ascq,omitempty"`
	BytesRead                           uint64   `json:"bytes_read"`
	BytesWritten                        uint64   `json:"bytes_written"`
	FlushCount                          uint64   `json:"flush_count"`
	UnmapBytes                          uint64   `json:"unmap_bytes"`
	ReadbackMatched                     bool     `json:"readback_matched"`
	RemoteLabUsed                       bool     `json:"remote_lab_used"`
	ISCSIGatewayRestarted               bool     `json:"iscsi_gateway_restarted"`
	SBSServiceRestarted                 bool     `json:"sbs_service_restarted"`
	SBSDataRestarted                    bool     `json:"sbs_data_restarted"`
	KernelModuleReloaded                bool     `json:"kernel_module_reloaded"`
	GotgtWildcardListenRequiresOverride bool     `json:"gotgt_wildcard_listen_requires_override"`
	CompatibilityClaim                  string   `json:"compatibility_claim"`
	OKCount                             int      `json:"ok_count"`
	ErrorCount                          int      `json:"error_count"`
	FirstError                          string   `json:"first_error,omitempty"`
	LastError                           string   `json:"last_error,omitempty"`
}

type OperationRecord struct {
	Operation string `json:"operation"`
	Result    string `json:"result"`
	Offset    uint64 `json:"offset_bytes,omitempty"`
	Length    uint64 `json:"length_bytes,omitempty"`
	Error     string `json:"error,omitempty"`
}

type MemoryLUN struct {
	mu        sync.RWMutex
	data      []byte
	syncCount uint64
}

func NewMemoryLUN(size uint64) (*MemoryLUN, error) {
	if size == 0 {
		size = DefaultMemoryLUNBytes
	}
	if size > uint64(int(^uint(0)>>1)) {
		return nil, fmt.Errorf("memory LUN size exceeds process addressable limit: %d", size)
	}
	return &MemoryLUN{data: make([]byte, int(size))}, nil
}

func (m *MemoryLUN) Size() uint64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return uint64(len(m.data))
}

func (m *MemoryLUN) ReadAt(p []byte, off int64) (int, error) {
	if off < 0 {
		return 0, fmt.Errorf("negative read offset")
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	if uint64(off) >= uint64(len(m.data)) {
		return 0, io.EOF
	}
	n := copy(p, m.data[off:])
	if n < len(p) {
		return n, io.EOF
	}
	return n, nil
}

func (m *MemoryLUN) WriteAt(p []byte, off int64) (int, error) {
	if off < 0 {
		return 0, fmt.Errorf("negative write offset")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if uint64(off) > uint64(len(m.data)) || uint64(len(p)) > uint64(len(m.data))-uint64(off) {
		return 0, fmt.Errorf("write outside memory LUN range")
	}
	return copy(m.data[off:], p), nil
}

func (m *MemoryLUN) Sync() (int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.syncCount++
	return 0, nil
}

func (m *MemoryLUN) Unmap(offset, length int64) (int, error) {
	return 0, fmt.Errorf("memory backend rejects UNMAP")
}

func (m *MemoryLUN) Zero(offset, length int64) (int, error) {
	if offset < 0 || length < 0 {
		return 0, fmt.Errorf("negative zero range")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if uint64(offset) > uint64(len(m.data)) || uint64(length) > uint64(len(m.data))-uint64(offset) {
		return 0, fmt.Errorf("zero outside memory LUN range")
	}
	clear(m.data[offset : offset+length])
	return 0, nil
}

func (m *MemoryLUN) SyncCount() uint64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.syncCount
}

func NormalizeMemoryOptions(opts MemoryOptions) (MemoryOptions, error) {
	opts.Portal = strings.TrimSpace(opts.Portal)
	if opts.Portal == "" {
		return opts, fmt.Errorf("portal is required")
	}
	if _, _, err := net.SplitHostPort(opts.Portal); err != nil {
		return opts, fmt.Errorf("portal must be host:port: %w", err)
	}
	if opts.MemoryLUNBytes == 0 {
		opts.MemoryLUNBytes = DefaultMemoryLUNBytes
	}
	if opts.MemoryLUNBytes%DefaultLogicalBlock != 0 {
		return opts, fmt.Errorf("memory LUN size must be %d-byte aligned", DefaultLogicalBlock)
	}
	if opts.ExportID == "" {
		opts.ExportID = DefaultExportID
	}
	if opts.TargetIQN == "" {
		opts.TargetIQN = DefaultTargetIQN(opts.ExportID)
	}
	if opts.InitiatorOS == "" {
		opts.InitiatorOS = "fixture"
	}
	return opts, nil
}

func DefaultTargetIQN(exportID string) string {
	return "iqn.2026-06.io.namrbd:iscsi." + sanitizeIQNPart(exportID)
}

func LUNWWN(exportID string) string {
	return "namrbd-phase-q-" + sanitizeIQNPart(exportID)
}

func ParseSizeBytes(raw string) (uint64, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return 0, fmt.Errorf("empty size")
	}
	units := []struct {
		suffix string
		mult   uint64
	}{
		{"GiB", 1024 * 1024 * 1024},
		{"MiB", 1024 * 1024},
		{"KiB", 1024},
		{"G", 1024 * 1024 * 1024},
		{"M", 1024 * 1024},
		{"K", 1024},
	}
	for _, unit := range units {
		if strings.HasSuffix(raw, unit.suffix) {
			n, err := strconv.ParseUint(strings.TrimSpace(strings.TrimSuffix(raw, unit.suffix)), 10, 64)
			if err != nil {
				return 0, err
			}
			return n * unit.mult, nil
		}
	}
	return strconv.ParseUint(raw, 10, 64)
}

func RunMemorySelfTest(opts MemoryOptions) (Summary, error) {
	opts, err := NormalizeMemoryOptions(opts)
	if err != nil {
		return Summary{}, err
	}
	lun, err := NewMemoryLUN(opts.MemoryLUNBytes)
	if err != nil {
		return Summary{}, err
	}
	var operations []OperationRecord
	record := func(op OperationRecord) {
		operations = append(operations, op)
	}
	testOffset := uint64(DefaultLogicalBlock * 8)
	testLength := uint64(DefaultLogicalBlock * 8)
	if opts.MemoryLUNBytes < testOffset+testLength {
		testOffset = 0
		testLength = opts.MemoryLUNBytes
	}
	pattern := []byte("namrbd-phase-q-memory:")
	payload := bytes.Repeat(pattern, int((testLength/uint64(len(pattern)))+1))[:testLength]
	n, err := lun.WriteAt(payload, int64(testOffset))
	if err != nil {
		record(OperationRecord{Operation: "write", Result: "error", Offset: testOffset, Length: testLength, Error: err.Error()})
		summary := summaryFrom(opts, lun, false, uint64(n), 0, 1, operations, err)
		if artifactErr := writeArtifacts(opts, operations, &summary); artifactErr != nil {
			return summary, artifactErr
		}
		return summary, err
	}
	record(OperationRecord{Operation: "write", Result: "ok", Offset: testOffset, Length: testLength})
	readBuf := make([]byte, testLength)
	n, readErr := lun.ReadAt(readBuf, int64(testOffset))
	if readErr != nil && !errors.Is(readErr, io.EOF) {
		record(OperationRecord{Operation: "read", Result: "error", Offset: testOffset, Length: testLength, Error: readErr.Error()})
		summary := summaryFrom(opts, lun, false, testLength, uint64(n), 1, operations, readErr)
		if artifactErr := writeArtifacts(opts, operations, &summary); artifactErr != nil {
			return summary, artifactErr
		}
		return summary, readErr
	}
	matched := bytes.Equal(payload, readBuf)
	if !matched {
		err = fmt.Errorf("memory readback mismatch")
		record(OperationRecord{Operation: "read", Result: "error", Offset: testOffset, Length: testLength, Error: err.Error()})
		summary := summaryFrom(opts, lun, false, testLength, uint64(n), 1, operations, err)
		if artifactErr := writeArtifacts(opts, operations, &summary); artifactErr != nil {
			return summary, artifactErr
		}
		return summary, err
	}
	record(OperationRecord{Operation: "read", Result: "ok", Offset: testOffset, Length: testLength})
	if _, err = lun.Sync(); err != nil {
		record(OperationRecord{Operation: "sync", Result: "error", Error: err.Error()})
		summary := summaryFrom(opts, lun, matched, testLength, testLength, 1, operations, err)
		if artifactErr := writeArtifacts(opts, operations, &summary); artifactErr != nil {
			return summary, artifactErr
		}
		return summary, err
	}
	record(OperationRecord{Operation: "sync", Result: "ok"})
	if _, err = lun.Unmap(int64(testOffset), int64(testLength)); err == nil {
		err = fmt.Errorf("memory UNMAP unexpectedly succeeded")
		record(OperationRecord{Operation: "unmap", Result: "error", Offset: testOffset, Length: testLength, Error: err.Error()})
		summary := summaryFrom(opts, lun, matched, testLength, testLength, 1, operations, err)
		if artifactErr := writeArtifacts(opts, operations, &summary); artifactErr != nil {
			return summary, artifactErr
		}
		return summary, err
	}
	record(OperationRecord{Operation: "unmap_reject", Result: "ok", Offset: testOffset, Length: testLength})
	summary := summaryFrom(opts, lun, matched, testLength, testLength, 0, operations, nil)
	return summary, writeArtifacts(opts, operations, &summary)
}

func WriteSummary(w io.Writer, summary Summary) error {
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(summary)
}

func writeArtifacts(opts MemoryOptions, operations []OperationRecord, summary *Summary) error {
	if opts.OperationJSONLPath != "" {
		if err := os.MkdirAll(filepath.Dir(opts.OperationJSONLPath), 0o755); err != nil {
			return err
		}
		f, err := os.Create(opts.OperationJSONLPath)
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
		if err := f.Close(); err != nil {
			return err
		}
	}
	if opts.SummaryJSONPath != "" && summary != nil {
		if err := os.MkdirAll(filepath.Dir(opts.SummaryJSONPath), 0o755); err != nil {
			return err
		}
		f, err := os.Create(opts.SummaryJSONPath)
		if err != nil {
			return err
		}
		if err := WriteSummary(f, *summary); err != nil {
			_ = f.Close()
			return err
		}
		return f.Close()
	}
	return nil
}

func summaryFrom(opts MemoryOptions, lun *MemoryLUN, matched bool, bytesWritten, bytesRead uint64, errors int, operations []OperationRecord, err error) Summary {
	okCount := len(operations)
	if errors > 0 && okCount > 0 {
		okCount--
	}
	s := Summary{
		Result:                              "ok",
		Path:                                "q-slice-001-memory-backend",
		Entrypoint:                          "namrbd-iscsictl smoke memory",
		TargetIQN:                           opts.TargetIQN,
		PortalAddress:                       opts.Portal,
		LUNID:                               DefaultLUNID,
		LUNWWN:                              LUNWWN(opts.ExportID),
		ExportID:                            opts.ExportID,
		VolumeID:                            "memory:" + opts.ExportID,
		BackendMode:                         "memory",
		MemoryLUNBytes:                      lun.Size(),
		MemoryBackendSeed:                   "deterministic-self-test-pattern",
		MemoryBackendPersistence:            "volatile",
		InitiatorOS:                         opts.InitiatorOS,
		TargetStack:                         TargetStack,
		TargetStackVersion:                  TargetStackVersion,
		TargetStackAccepted:                 false,
		TargetStackUpstreamEvidence:         "github.com/gostor/gotgt@v0.2.2",
		TargetLUNModel:                      "one_target_one_lun",
		TargetIQNSource:                     "export_id",
		PortalListenRequired:                true,
		MacOSSupportClaimed:                 false,
		SCSIAllowlistProfile:                "phase_q_initial",
		LogicalBlockSizeBytes:               DefaultLogicalBlock,
		FUAClaim:                            "backend_write_ack",
		UNMAPPolicy:                         "reject_memory_backend",
		PRPolicy:                            "reject_phase_q",
		MPIOALUASupported:                   false,
		ISCSIERL:                            0,
		SessionConnectionPolicy:             "single_connection",
		HeaderDigestEnabled:                 false,
		DataDigestEnabled:                   false,
		ISCSIEdition:                        ISCSIEdition,
		ExportVolumeLimit:                   ISCSIExportVolumeLimit,
		MetadataAuthority:                   "local_fixture",
		AuthPolicy:                          AuthPolicyNoAuthAllowlistFirst,
		AuthMode:                            AuthModeNone,
		RuntimeCHAPSupported:                false,
		AuthRuntimeClaim:                    AuthRuntimeClaimGotgtNoneOnly,
		RuntimeInitiatorAllowlistSupported:  false,
		InitiatorAllowlistRuntimeClaim:      InitiatorAllowlistRuntimeClaimGotgtNoHook,
		AllowedInitiatorIQNs:                []string{},
		WriterPolicy:                        "single_active_writer_session",
		HAFailoverMode:                      "manual_promote_demote_first",
		SummaryJSONPath:                     opts.SummaryJSONPath,
		OperationJSONLPath:                  opts.OperationJSONLPath,
		AppendixCRequired:                   false,
		SCSIStatus:                          "good",
		BytesRead:                           bytesRead,
		BytesWritten:                        bytesWritten,
		FlushCount:                          lun.SyncCount(),
		UnmapBytes:                          0,
		ReadbackMatched:                     matched,
		GotgtWildcardListenRequiresOverride: true,
		CompatibilityClaim:                  "local_memory_backend_only",
		OKCount:                             okCount,
		ErrorCount:                          errors,
	}
	if err != nil {
		s.Result = "error"
		s.SCSIStatus = "check_condition"
		s.FirstError = err.Error()
		s.LastError = err.Error()
	}
	return s
}

func sanitizeIQNPart(raw string) string {
	raw = strings.ToLower(strings.TrimSpace(raw))
	var b strings.Builder
	for _, r := range raw {
		switch {
		case r >= 'a' && r <= 'z':
			b.WriteRune(r)
		case r >= '0' && r <= '9':
			b.WriteRune(r)
		case r == '-' || r == '.' || r == ':':
			b.WriteRune(r)
		default:
			b.WriteByte('-')
		}
	}
	out := strings.Trim(b.String(), "-.:")
	if out == "" {
		return DefaultExportID
	}
	return out
}
