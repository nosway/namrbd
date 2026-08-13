package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/nosway/namrbd/gateway/metadata"
	"github.com/nosway/namrbd/gateway/service"
	"github.com/nosway/namrbd/gateway/store"
	"github.com/nosway/namrbd/volumeid"
)

type commandConfig struct {
	etcdEndpoints string
	etcdRoot      string
	storeBackend  string
	redisAddr     string
	jsonOutput    bool
}

const gatewayPostResponseLimitBytes = 128 << 20

type validationIssue struct {
	Severity string `json:"severity"`
	Code     string `json:"code"`
	Message  string `json:"message"`
}

type volumeSpecOutput struct {
	service.VolumeSpec
	AllocationChunkSizeBytes uint32 `json:"allocation_chunk_size_bytes,omitempty"`
	AllocationPageBytes      uint32 `json:"allocation_page_bytes,omitempty"`
}

type volumeLayoutReport struct {
	Volume               volumeSpecOutput               `json:"volume"`
	PageCount            int                            `json:"page_count"`
	AllocationPageCount  int                            `json:"allocation_page_count"`
	ExtentCount          int                            `json:"extent_count"`
	AllocationChunkCount int                            `json:"allocation_chunk_count"`
	DataExtentCount      int                            `json:"data_extent_count"`
	DataAllocationChunks int                            `json:"data_allocation_chunks"`
	ZeroExtentCount      int                            `json:"zero_extent_count"`
	ZeroAllocationChunks int                            `json:"zero_allocation_chunks"`
	Pages                []service.AllocationPageRecord `json:"pages"`
	AllocationPages      []service.AllocationPageRecord `json:"allocation_pages"`
}

type extentValidationReport struct {
	VolumeID        service.HexVolumeID            `json:"volume_id"`
	OK              bool                           `json:"ok"`
	PageCount       int                            `json:"page_count"`
	IssueCount      int                            `json:"issue_count"`
	Issues          []validationIssue              `json:"issues"`
	Volume          volumeSpecOutput               `json:"volume"`
	ExtentPages     []service.AllocationPageRecord `json:"extent_pages"`
	AllocationPages []service.AllocationPageRecord `json:"allocation_pages"`
}

type chunkGarbageSweepReport struct {
	VolumeID       service.HexVolumeID `json:"volume_id"`
	CandidateCount int                 `json:"candidate_count"`
	DeletedCount   int                 `json:"deleted_count"`
	RetainedCount  int                 `json:"retained_count"`
}

type gatewayWriteLoadReport struct {
	Result               string                           `json:"result"`
	GatewayURL           string                           `json:"gateway_url"`
	GatewayURLs          []string                         `json:"gateway_urls,omitempty"`
	GatewayPolicy        string                           `json:"gateway_policy,omitempty"`
	ActiveLaneCount      int                              `json:"active_lane_count,omitempty"`
	GatewayLanes         []gatewayLaneReport              `json:"gateway_lanes,omitempty"`
	GatewayRequestCounts map[string]int                   `json:"gateway_request_counts,omitempty"`
	GatewayOKCounts      map[string]int                   `json:"gateway_ok_counts,omitempty"`
	GatewayErrorCounts   map[string]int                   `json:"gateway_error_counts,omitempty"`
	VolumeID             string                           `json:"volume_id"`
	ClientNetworkProfile string                           `json:"client_network_profile,omitempty"`
	HostID               string                           `json:"host_id"`
	DeviceID             uint32                           `json:"device_id"`
	AttachmentID         string                           `json:"attachment_id,omitempty"`
	Generation           uint64                           `json:"generation,omitempty"`
	Detached             bool                             `json:"detached,omitempty"`
	DetachError          string                           `json:"detach_error,omitempty"`
	DetachWarning        string                           `json:"detach_warning,omitempty"`
	DetachAttemptCount   int                              `json:"detach_attempt_count,omitempty"`
	DetachOKCount        int                              `json:"detach_ok_count,omitempty"`
	DetachConflictCount  int                              `json:"detach_ignored_conflict_count,omitempty"`
	RW                   string                           `json:"rw"`
	RWMixRead            int                              `json:"rwmixread,omitempty"`
	Prefill              bool                             `json:"prefill,omitempty"`
	SizeBytes            uint64                           `json:"size_bytes"`
	BlockSizeBytes       uint64                           `json:"block_size_bytes"`
	IODepth              int                              `json:"iodepth"`
	NumJobs              int                              `json:"numjobs"`
	Concurrency          int                              `json:"concurrency"`
	WarmupRequests       int                              `json:"warmup_requests,omitempty"`
	SteadyStateSkip      int                              `json:"steady_state_skip,omitempty"`
	PayloadPattern       string                           `json:"payload_pattern"`
	PayloadVerifyVolume  string                           `json:"payload_verify_volume,omitempty"`
	Verify               bool                             `json:"verify"`
	RequestCount         int                              `json:"request_count"`
	OKCount              int                              `json:"ok_count"`
	ErrorCount           int                              `json:"error_count"`
	WriteCount           int                              `json:"write_count,omitempty"`
	WriteOKCount         int                              `json:"write_ok_count,omitempty"`
	WriteErrorCount      int                              `json:"write_error_count,omitempty"`
	ReadCount            int                              `json:"read_count,omitempty"`
	ReadOKCount          int                              `json:"read_ok_count,omitempty"`
	ReadErrorCount       int                              `json:"read_error_count,omitempty"`
	PrefillCount         int                              `json:"prefill_count,omitempty"`
	PrefillErrorCount    int                              `json:"prefill_error_count,omitempty"`
	WarmupOKCount        int                              `json:"warmup_ok_count,omitempty"`
	WarmupErrorCount     int                              `json:"warmup_error_count,omitempty"`
	WarmupLatency        *gatewayLoadLatencySummary       `json:"warmup_latency,omitempty"`
	VerifyCount          int                              `json:"verify_count,omitempty"`
	VerifyOKCount        int                              `json:"verify_ok_count,omitempty"`
	VerifyErrorCount     int                              `json:"verify_error_count,omitempty"`
	ElapsedMS            int64                            `json:"elapsed_ms"`
	WriteIOPS            float64                          `json:"write_iops"`
	WriteBWKiB           float64                          `json:"write_bw_kib"`
	ReadIOPS             float64                          `json:"read_iops,omitempty"`
	ReadBWKiB            float64                          `json:"read_bw_kib,omitempty"`
	TotalIOPS            float64                          `json:"total_iops"`
	TotalBWKiB           float64                          `json:"total_bw_kib"`
	LatencyAvgMS         float64                          `json:"latency_avg_ms"`
	LatencyMinMS         float64                          `json:"latency_min_ms"`
	LatencyP50MS         float64                          `json:"latency_p50_ms"`
	LatencyP90MS         float64                          `json:"latency_p90_ms"`
	LatencyP95MS         float64                          `json:"latency_p95_ms"`
	LatencyP99MS         float64                          `json:"latency_p99_ms"`
	LatencyP999MS        float64                          `json:"latency_p999_ms"`
	LatencyMaxMS         float64                          `json:"latency_max_ms"`
	ColdStartLatency     *gatewayLoadLatencySummary       `json:"cold_start_latency,omitempty"`
	SteadyStateLatency   *gatewayLoadLatencySummary       `json:"steady_state_latency,omitempty"`
	SlowOperations       []gatewayLoadSlowOperation       `json:"slow_operations,omitempty"`
	PhaseOThrottle       *gatewayPhaseOThrottleReport     `json:"phase_o_throttle,omitempty"`
	FirstError           string                           `json:"first_error,omitempty"`
	LastError            string                           `json:"last_error,omitempty"`
	VerifyFirstError     string                           `json:"verify_first_error,omitempty"`
	VerifyLastError      string                           `json:"verify_last_error,omitempty"`
	IntegrityScenarios   string                           `json:"integrity_scenarios,omitempty"`
	ScenarioCount        int                              `json:"scenario_count,omitempty"`
	ScenarioOKCount      int                              `json:"scenario_ok_count,omitempty"`
	ScenarioErrorCount   int                              `json:"scenario_error_count,omitempty"`
	ScenarioFirstError   string                           `json:"scenario_first_error,omitempty"`
	Scenarios            []gatewayIntegrityScenarioReport `json:"scenarios,omitempty"`
}

type gatewayReplayLoadReport struct {
	Result                 string                     `json:"result"`
	GatewayURL             string                     `json:"gateway_url"`
	GatewayURLs            []string                   `json:"gateway_urls,omitempty"`
	GatewayPolicy          string                     `json:"gateway_policy,omitempty"`
	ActiveLaneCount        int                        `json:"active_lane_count,omitempty"`
	GatewayLanes           []gatewayLaneReport        `json:"gateway_lanes,omitempty"`
	ReplayLaneCounts       []gatewayLaneCountReport   `json:"replay_lane_counts,omitempty"`
	ReplaySelectionCounts  map[string]int             `json:"replay_selection_counts,omitempty"`
	GatewayRequestCounts   map[string]int             `json:"gateway_request_counts,omitempty"`
	GatewayOKCounts        map[string]int             `json:"gateway_ok_counts,omitempty"`
	GatewayErrorCounts     map[string]int             `json:"gateway_error_counts,omitempty"`
	VolumeID               string                     `json:"volume_id"`
	ClientNetworkProfile   string                     `json:"client_network_profile,omitempty"`
	HostID                 string                     `json:"host_id"`
	DeviceID               uint32                     `json:"device_id"`
	AttachmentID           string                     `json:"attachment_id,omitempty"`
	Generation             uint64                     `json:"generation,omitempty"`
	TraceJSONL             string                     `json:"trace_jsonl"`
	ReplayMode             string                     `json:"replay_mode"`
	TraceOperationCount    int                        `json:"trace_operation_count"`
	RequestCount           int                        `json:"request_count"`
	SkippedOperationCount  int                        `json:"skipped_operation_count,omitempty"`
	Concurrency            int                        `json:"concurrency"`
	PayloadPattern         string                     `json:"payload_pattern"`
	Verify                 bool                       `json:"verify"`
	PathSurface            string                     `json:"path_surface"`
	ClaimClassification    string                     `json:"claim_classification"`
	KernelPayloadReplayed  bool                       `json:"kernel_payload_replayed"`
	SupportClaimed         bool                       `json:"support_claimed"`
	PublicBenchmarkClaimed bool                       `json:"public_benchmark_claimed"`
	OKCount                int                        `json:"ok_count"`
	ErrorCount             int                        `json:"error_count"`
	WriteCount             int                        `json:"write_count,omitempty"`
	WriteOKCount           int                        `json:"write_ok_count,omitempty"`
	WriteErrorCount        int                        `json:"write_error_count,omitempty"`
	ReadCount              int                        `json:"read_count,omitempty"`
	ReadOKCount            int                        `json:"read_ok_count,omitempty"`
	ReadErrorCount         int                        `json:"read_error_count,omitempty"`
	ZeroCount              int                        `json:"zero_count,omitempty"`
	ZeroOKCount            int                        `json:"zero_ok_count,omitempty"`
	ZeroErrorCount         int                        `json:"zero_error_count,omitempty"`
	DiscardCount           int                        `json:"discard_count,omitempty"`
	DiscardOKCount         int                        `json:"discard_ok_count,omitempty"`
	DiscardErrorCount      int                        `json:"discard_error_count,omitempty"`
	FlushCount             int                        `json:"flush_count,omitempty"`
	FlushOKCount           int                        `json:"flush_ok_count,omitempty"`
	FlushErrorCount        int                        `json:"flush_error_count,omitempty"`
	VerifyCount            int                        `json:"verify_count,omitempty"`
	VerifyOKCount          int                        `json:"verify_ok_count,omitempty"`
	VerifyErrorCount       int                        `json:"verify_error_count,omitempty"`
	VerifySkippedCount     int                        `json:"verify_skipped_count,omitempty"`
	ElapsedMS              int64                      `json:"elapsed_ms"`
	WriteIOPS              float64                    `json:"write_iops"`
	WriteBWKiB             float64                    `json:"write_bw_kib"`
	ReadIOPS               float64                    `json:"read_iops,omitempty"`
	ReadBWKiB              float64                    `json:"read_bw_kib,omitempty"`
	TotalIOPS              float64                    `json:"total_iops"`
	TotalBWKiB             float64                    `json:"total_bw_kib"`
	LatencyAvgMS           float64                    `json:"latency_avg_ms"`
	LatencyMinMS           float64                    `json:"latency_min_ms"`
	LatencyP50MS           float64                    `json:"latency_p50_ms"`
	LatencyP90MS           float64                    `json:"latency_p90_ms"`
	LatencyP95MS           float64                    `json:"latency_p95_ms"`
	LatencyP99MS           float64                    `json:"latency_p99_ms"`
	LatencyP999MS          float64                    `json:"latency_p999_ms"`
	LatencyMaxMS           float64                    `json:"latency_max_ms"`
	SlowOperations         []gatewayLoadSlowOperation `json:"slow_operations,omitempty"`
	FirstError             string                     `json:"first_error,omitempty"`
	LastError              string                     `json:"last_error,omitempty"`
	VerifyFirstError       string                     `json:"verify_first_error,omitempty"`
	VerifyLastError        string                     `json:"verify_last_error,omitempty"`
}

type gatewayLoadSlowOperation struct {
	Index                  int     `json:"index"`
	Phase                  string  `json:"phase,omitempty"`
	GatewayURL             string  `json:"gateway_url,omitempty"`
	LaneID                 int     `json:"lane_id,omitempty"`
	ReplaySelection        string  `json:"replay_selection,omitempty"`
	TraceGatewayID         string  `json:"trace_gateway_id,omitempty"`
	TracePathID            *uint32 `json:"trace_path_id,omitempty"`
	Op                     string  `json:"op"`
	OffsetBytes            uint64  `json:"offset_bytes"`
	LengthBytes            uint64  `json:"length_bytes"`
	LatencyMS              float64 `json:"latency_ms"`
	Error                  string  `json:"error,omitempty"`
	PhaseOThrottleWaitMS   uint64  `json:"phase_o_throttle_wait_ms,omitempty"`
	PhaseOThrottleRejected bool    `json:"phase_o_throttle_rejected,omitempty"`
	PhaseORejectionReason  string  `json:"phase_o_rejection_reason,omitempty"`
}

type gatewayPhaseOThrottleReport struct {
	Observed                 bool              `json:"observed"`
	PolicyID                 string            `json:"policy_id,omitempty"`
	PolicyGeneration         uint64            `json:"policy_generation,omitempty"`
	CapScope                 string            `json:"cap_scope,omitempty"`
	ThrottleMode             string            `json:"throttle_mode,omitempty"`
	IOPSCap                  uint64            `json:"iops_cap,omitempty"`
	BandwidthCapBytesPerSec  uint64            `json:"bandwidth_cap_bytes_per_sec,omitempty"`
	BurstIOPS                uint64            `json:"burst_iops,omitempty"`
	BurstBytes               uint64            `json:"burst_bytes,omitempty"`
	RequestedTokens          uint64            `json:"requested_tokens"`
	GrantedTokens            uint64            `json:"granted_tokens"`
	RequestedBytes           uint64            `json:"requested_bytes"`
	GrantedBytes             uint64            `json:"granted_bytes"`
	DeniedTokens             uint64            `json:"denied_tokens"`
	DeniedBytes              uint64            `json:"denied_bytes"`
	ThrottledOps             uint64            `json:"throttled_ops"`
	ThrottledBytes           uint64            `json:"throttled_bytes"`
	ThrottleWaitCount        uint64            `json:"throttle_wait_count"`
	ThrottleWaitTotalMS      uint64            `json:"throttle_wait_total_ms"`
	ThrottleWaitMaxMS        uint64            `json:"throttle_wait_max_ms"`
	RejectedOps              uint64            `json:"rejected_ops"`
	RejectionReasons         map[string]uint64 `json:"rejection_reasons,omitempty"`
	LeaseCount               uint64            `json:"lease_count,omitempty"`
	FirstLeaseID             string            `json:"first_lease_id,omitempty"`
	LastLeaseID              string            `json:"last_lease_id,omitempty"`
	MaxLeaseGeneration       uint64            `json:"max_lease_generation,omitempty"`
	SharedBudgetAuthority    bool              `json:"shared_budget_authority"`
	GatewayConsumesLease     bool              `json:"gateway_consumes_lease"`
	EnforcedBeforeDispatch   bool              `json:"enforced_before_dispatch"`
	ClusterWideCapSupport    bool              `json:"cluster_wide_cap_support"`
	GatewayRestartRequired   bool              `json:"gateway_restart_required"`
	RemoteLabValidationState string            `json:"remote_lab_validation_state,omitempty"`
}

type gatewayPhaseOThrottleObservation struct {
	PolicyID                 string `json:"policy_id"`
	PolicyGeneration         uint64 `json:"policy_generation"`
	CapScope                 string `json:"cap_scope"`
	ThrottleMode             string `json:"throttle_mode"`
	RequestedTokens          uint64 `json:"requested_tokens"`
	GrantedTokens            uint64 `json:"granted_tokens"`
	RequestedBytes           uint64 `json:"requested_bytes"`
	GrantedBytes             uint64 `json:"granted_bytes"`
	DeniedTokens             uint64 `json:"denied_tokens"`
	DeniedBytes              uint64 `json:"denied_bytes"`
	ThrottledOps             uint64 `json:"throttled_ops"`
	ThrottledBytes           uint64 `json:"throttled_bytes"`
	ThrottleWaitMs           uint64 `json:"throttle_wait_ms"`
	RejectedOps              uint64 `json:"rejected_ops"`
	RejectionReason          string `json:"rejection_reason"`
	LeaseID                  string `json:"lease_id"`
	LeaseGeneration          uint64 `json:"lease_generation"`
	SharedBudgetAuthority    bool   `json:"shared_budget_authority"`
	GatewayConsumesLease     bool   `json:"gateway_consumes_lease"`
	IOPSCap                  uint64 `json:"iops_cap"`
	BandwidthCapBytesPerSec  uint64 `json:"bandwidth_cap_bytes_per_sec"`
	BurstIOPS                uint64 `json:"burst_iops"`
	BurstBytes               uint64 `json:"burst_bytes"`
	EnforcedBeforeDispatch   bool   `json:"enforced_before_dispatch"`
	ClusterWideCapSupport    bool   `json:"cluster_wide_cap_support"`
	GatewayRestartRequired   bool   `json:"gateway_restart_required"`
	RemoteLabValidationState string `json:"remote_lab_validation_state"`
}

type gatewayOperationResponse struct {
	PhaseOThrottle *gatewayPhaseOThrottleObservation `json:"phase_o_throttle,omitempty"`
}

type gatewayLaneReport struct {
	LaneID     int    `json:"lane_id"`
	GatewayURL string `json:"gateway_url"`
	GatewayID  string `json:"gateway_id,omitempty"`
}

type gatewayLaneCountReport struct {
	LaneID       int    `json:"lane_id"`
	GatewayURL   string `json:"gateway_url"`
	GatewayID    string `json:"gateway_id,omitempty"`
	RequestCount int    `json:"request_count,omitempty"`
	OKCount      int    `json:"ok_count,omitempty"`
	ErrorCount   int    `json:"error_count,omitempty"`
}

type gatewayLoadLatencySummary struct {
	Count  int     `json:"count"`
	AvgMS  float64 `json:"avg_ms"`
	MinMS  float64 `json:"min_ms"`
	P50MS  float64 `json:"p50_ms"`
	P90MS  float64 `json:"p90_ms"`
	P95MS  float64 `json:"p95_ms"`
	P99MS  float64 `json:"p99_ms"`
	P999MS float64 `json:"p999_ms"`
	MaxMS  float64 `json:"max_ms"`
}

type fileLoadReport struct {
	Result            string  `json:"result"`
	Path              string  `json:"path"`
	StorageProfile    string  `json:"storage_profile,omitempty"`
	RW                string  `json:"rw"`
	RWMixRead         int     `json:"rwmixread,omitempty"`
	Prefill           bool    `json:"prefill,omitempty"`
	Reset             bool    `json:"reset"`
	Fsync             bool    `json:"fsync,omitempty"`
	SizeBytes         uint64  `json:"size_bytes"`
	BlockSizeBytes    uint64  `json:"block_size_bytes"`
	IODepth           int     `json:"iodepth"`
	NumJobs           int     `json:"numjobs"`
	Concurrency       int     `json:"concurrency"`
	PayloadPattern    string  `json:"payload_pattern"`
	Verify            bool    `json:"verify"`
	RequestCount      int     `json:"request_count"`
	OKCount           int     `json:"ok_count"`
	ErrorCount        int     `json:"error_count"`
	WriteCount        int     `json:"write_count,omitempty"`
	WriteOKCount      int     `json:"write_ok_count,omitempty"`
	WriteErrorCount   int     `json:"write_error_count,omitempty"`
	ReadCount         int     `json:"read_count,omitempty"`
	ReadOKCount       int     `json:"read_ok_count,omitempty"`
	ReadErrorCount    int     `json:"read_error_count,omitempty"`
	PrefillCount      int     `json:"prefill_count,omitempty"`
	PrefillErrorCount int     `json:"prefill_error_count,omitempty"`
	VerifyCount       int     `json:"verify_count,omitempty"`
	VerifyOKCount     int     `json:"verify_ok_count,omitempty"`
	VerifyErrorCount  int     `json:"verify_error_count,omitempty"`
	ElapsedMS         int64   `json:"elapsed_ms"`
	WriteIOPS         float64 `json:"write_iops"`
	WriteBWKiB        float64 `json:"write_bw_kib"`
	ReadIOPS          float64 `json:"read_iops,omitempty"`
	ReadBWKiB         float64 `json:"read_bw_kib,omitempty"`
	TotalIOPS         float64 `json:"total_iops"`
	TotalBWKiB        float64 `json:"total_bw_kib"`
	LatencyAvgMS      float64 `json:"latency_avg_ms"`
	LatencyMinMS      float64 `json:"latency_min_ms"`
	LatencyP50MS      float64 `json:"latency_p50_ms"`
	LatencyP90MS      float64 `json:"latency_p90_ms"`
	LatencyP95MS      float64 `json:"latency_p95_ms"`
	LatencyP99MS      float64 `json:"latency_p99_ms"`
	LatencyP999MS     float64 `json:"latency_p999_ms"`
	LatencyMaxMS      float64 `json:"latency_max_ms"`
	FirstError        string  `json:"first_error,omitempty"`
	VerifyFirstError  string  `json:"verify_first_error,omitempty"`
}

type gatewayIntegrityScenarioReport struct {
	Name             string                            `json:"name"`
	Result           string                            `json:"result"`
	WriteCount       int                               `json:"write_count,omitempty"`
	ZeroCount        int                               `json:"zero_count,omitempty"`
	ReadCount        int                               `json:"read_count,omitempty"`
	VerifyCount      int                               `json:"verify_count,omitempty"`
	VerifyOKCount    int                               `json:"verify_ok_count,omitempty"`
	VerifyErrorCount int                               `json:"verify_error_count,omitempty"`
	FirstError       string                            `json:"first_error,omitempty"`
	Mismatch         *gatewayPayloadMismatchReport     `json:"mismatch,omitempty"`
	Operations       []gatewayIntegrityScenarioOpTrace `json:"operations,omitempty"`
}

type gatewayIntegrityScenarioOpTrace struct {
	Op          string `json:"op"`
	OffsetBytes uint64 `json:"offset_bytes"`
	LengthBytes uint64 `json:"length_bytes"`
	Seed        string `json:"seed,omitempty"`
	SHA256      string `json:"sha256,omitempty"`
}

type gatewayPayloadMismatchReport struct {
	OffsetBytes               uint64                          `json:"offset_bytes"`
	LengthBytes               uint64                          `json:"length_bytes"`
	FirstMismatchOffsetBytes  uint64                          `json:"first_mismatch_offset_bytes"`
	FirstMismatchInReadBytes  uint64                          `json:"first_mismatch_in_read_bytes"`
	FirstMismatchExpectedByte uint8                           `json:"first_mismatch_expected_byte"`
	FirstMismatchActualByte   uint8                           `json:"first_mismatch_actual_byte"`
	ExpectedSHA256            string                          `json:"expected_sha256"`
	ActualSHA256              string                          `json:"actual_sha256"`
	ActualAllZero             bool                            `json:"actual_all_zero"`
	WindowOffsetBytes         uint64                          `json:"window_offset_bytes"`
	WindowLengthBytes         uint64                          `json:"window_length_bytes"`
	ExpectedWindowHex         string                          `json:"expected_window_hex"`
	ActualWindowHex           string                          `json:"actual_window_hex"`
	SegmentSizeBytes          uint64                          `json:"segment_size_bytes"`
	DifferingSegments         []gatewayPayloadSegmentMismatch `json:"differing_segments,omitempty"`
}

type gatewayPayloadSegmentMismatch struct {
	Index          int    `json:"index"`
	OffsetBytes    uint64 `json:"offset_bytes"`
	LengthBytes    uint64 `json:"length_bytes"`
	ExpectedSHA256 string `json:"expected_sha256"`
	ActualSHA256   string `json:"actual_sha256"`
	ActualAllZero  bool   `json:"actual_all_zero"`
}

type gatewayCloneIOCheckReport struct {
	Result         string                        `json:"result"`
	GatewayURL     string                        `json:"gateway_url"`
	VolumeID       string                        `json:"volume_id"`
	CloneID        string                        `json:"clone_id"`
	OffsetBytes    uint64                        `json:"offset_bytes"`
	LengthBytes    uint64                        `json:"length_bytes"`
	Attached       bool                          `json:"attached"`
	Detached       bool                          `json:"detached"`
	AttachmentID   string                        `json:"attachment_id,omitempty"`
	Generation     uint64                        `json:"generation,omitempty"`
	ExpectedSHA256 string                        `json:"expected_sha256"`
	ActualSHA256   string                        `json:"actual_sha256,omitempty"`
	Mismatch       *gatewayPayloadMismatchReport `json:"mismatch,omitempty"`
	Error          string                        `json:"error,omitempty"`
}

func main() {
	if len(os.Args) < 3 {
		usage()
		os.Exit(2)
	}

	resource := os.Args[1]
	verb := os.Args[2]
	args := os.Args[3:]

	switch resource {
	case "volume":
		runVolume(verb, args)
	case "validate":
		runValidate(verb, args)
	case "unsafe":
		runUnsafe(verb, args)
	case "gateway":
		runGateway(verb, args)
	case "file":
		runFile(verb, args)
	default:
		usage()
		os.Exit(2)
	}
}

func runVolume(verb string, args []string) {
	switch verb {
	case "inspect-layout":
		fs, cfg := newCommandFlagSet("volume inspect-layout")
		rawVolumeID := fs.String("volume", "", "volume id (8 lowercase hex digits)")
		fs.Parse(args)
		volumeID, err := parseRequiredVolumeID(*rawVolumeID)
		if err != nil {
			fatalf("--volume is required")
		}
		repo, closeFn := mustOpenRepository(cfg)
		defer closeFn()
		report, err := inspectVolumeLayout(context.Background(), repo, volumeID)
		if err != nil {
			fatalf("volume inspect-layout failed: %v", err)
		}
		if cfg.jsonOutput {
			writeJSON(report)
			return
		}
		printVolumeLayout(report)
	default:
		usage()
		os.Exit(2)
	}
}

func runValidate(verb string, args []string) {
	switch verb {
	case "extents":
		fs, cfg := newCommandFlagSet("validate extents")
		rawVolumeID := fs.String("volume", "", "volume id (8 lowercase hex digits)")
		fs.Parse(args)
		volumeID, err := parseRequiredVolumeID(*rawVolumeID)
		if err != nil {
			fatalf("--volume is required")
		}
		repo, closeFn := mustOpenRepository(cfg)
		defer closeFn()
		report, err := validateExtents(context.Background(), repo, volumeID)
		if err != nil {
			fatalf("validate extents failed: %v", err)
		}
		if cfg.jsonOutput {
			writeJSON(report)
			return
		}
		printExtentValidation(report)
		if !report.OK {
			os.Exit(1)
		}
	default:
		usage()
		os.Exit(2)
	}
}

func runGateway(verb string, args []string) {
	switch verb {
	case "write-load":
		fs := flag.NewFlagSet("gateway write-load", flag.ExitOnError)
		gatewayURL := fs.String("gateway", "", "gateway base URL, e.g. http://u40:9899")
		gatewayURLs := fs.String("gateways", "", "comma-separated gateway base URLs; requests are distributed using the kernel-like lane policy")
		activeLanes := fs.Int("active-lanes", 0, "override active gateway lane count; 0 uses the kernel-like default cap")
		rawVolumeID := fs.String("volume", "", "volume id (8 lowercase hex digits)")
		hostID := fs.String("host-id", "phase-f-http-load", "gateway attachment host id")
		deviceID := fs.Uint("device-id", 9901, "gateway attachment device id")
		sizeRaw := fs.String("size", "16m", "per-job write span, e.g. 16m")
		bsRaw := fs.String("bs", "128k", "write block size, e.g. 128k")
		rw := fs.String("rw", "write", "fio-like workload: write, read, randwrite, randread, randrw")
		rwmixRead := fs.Int("rwmixread", 70, "read percentage for --rw=randrw")
		prefill := fs.Bool("prefill", true, "prefill the tested range before read/randread/randrw workloads")
		iodepth := fs.Int("iodepth", 16, "fio-like iodepth per job")
		numjobs := fs.Int("numjobs", 2, "fio-like job count")
		concurrency := fs.Int("concurrency", 0, "override total concurrent HTTP writes; default iodepth*numjobs")
		warmupRequests := fs.Int("warmup-requests", 0, "requests to run before measurement; excluded from latency/IOPS summary")
		steadyStateSkip := fs.Int("steady-state-skip", 0, "initial request count excluded from steady-state latency summary; default total concurrency")
		timeout := fs.Duration("timeout", 240*time.Second, "overall HTTP client timeout")
		clientNetworkProfile := fs.String("client-network-profile", "", "label for the client network path")
		payloadPattern := fs.String("payload-pattern", "zero", "write payload pattern: zero or deterministic")
		payloadVerifyVolume := fs.String("verify-volume", "", "optional volume id used as deterministic verification seed")
		verify := fs.Bool("verify", false, "read back each unique written block and verify payload bytes")
		integrityScenarios := fs.String("integrity-scenarios", "", "comma-separated integrity scenarios: sequential-full,overwrite,zero-hole,unaligned-boundary,read-after-write")
		attach := fs.Bool("attach", true, "attach the volume before the load")
		detach := fs.Bool("detach", true, "detach the volume after the load")
		fs.Parse(args)
		if *deviceID > uint(^uint32(0)) {
			fatalf("--device-id is too large")
		}
		volumeID, err := parseRequiredVolumeID(*rawVolumeID)
		if err != nil {
			fatalf("--volume is required")
		}
		report, err := runGatewayWriteLoad(gatewayWriteLoadOptions{
			GatewayURL:           *gatewayURL,
			GatewayURLs:          *gatewayURLs,
			ActiveLanes:          *activeLanes,
			VolumeID:             volumeID,
			HostID:               *hostID,
			DeviceID:             uint32(*deviceID),
			SizeRaw:              *sizeRaw,
			BSRaw:                *bsRaw,
			RW:                   *rw,
			RWMixRead:            *rwmixRead,
			Prefill:              *prefill,
			IODepth:              *iodepth,
			NumJobs:              *numjobs,
			Concurrency:          *concurrency,
			WarmupRequests:       *warmupRequests,
			SteadyStateSkip:      *steadyStateSkip,
			Timeout:              *timeout,
			ClientNetworkProfile: *clientNetworkProfile,
			PayloadPattern:       *payloadPattern,
			PayloadVerifyVolume:  *payloadVerifyVolume,
			Verify:               *verify,
			IntegrityScenarios:   *integrityScenarios,
			Attach:               *attach,
			Detach:               *detach,
		})
		if err != nil {
			if report.Result == "" {
				fatalf("gateway write-load failed: %v", err)
			}
			writeJSON(report)
			os.Exit(1)
		}
		writeJSON(report)
	case "replay-load":
		fs := flag.NewFlagSet("gateway replay-load", flag.ExitOnError)
		gatewayURL := fs.String("gateway", "", "gateway base URL, e.g. http://u40:9899")
		gatewayURLs := fs.String("gateways", "", "comma-separated gateway base URLs; requests are distributed using the kernel-like lane policy")
		activeLanes := fs.Int("active-lanes", 0, "override active gateway lane count; 0 uses the kernel-like default cap")
		traceJSONL := fs.String("trace-jsonl", "", "normalized kernel-origin trace JSONL file, or - for stdin")
		rawVolumeID := fs.String("volume", "", "volume id (8 lowercase hex digits)")
		hostID := fs.String("host-id", "phase-x-kernel-origin-replay", "gateway attachment host id")
		deviceID := fs.Uint("device-id", 9902, "gateway attachment device id")
		mode := fs.String("mode", "saturating", "replay mode: saturating or paced")
		concurrency := fs.Int("concurrency", 0, "maximum concurrent replay requests; default min(trace_ops, 16)")
		timeout := fs.Duration("timeout", 240*time.Second, "overall HTTP client timeout")
		clientNetworkProfile := fs.String("client-network-profile", "", "label for the client network path")
		payloadPattern := fs.String("payload-pattern", "zero", "write payload pattern for replayed writes: zero or deterministic")
		verify := fs.Bool("verify", false, "read back final exact write/write-zeroes ranges and verify payload bytes")
		attach := fs.Bool("attach", true, "attach the volume before the replay")
		detach := fs.Bool("detach", true, "detach the volume after the replay")
		fs.Parse(args)
		if *deviceID > uint(^uint32(0)) {
			fatalf("--device-id is too large")
		}
		volumeID, err := parseRequiredVolumeID(*rawVolumeID)
		if err != nil {
			fatalf("--volume is required")
		}
		report, err := runGatewayReplayLoad(gatewayReplayLoadOptions{
			GatewayURL:           *gatewayURL,
			GatewayURLs:          *gatewayURLs,
			ActiveLanes:          *activeLanes,
			TraceJSONL:           *traceJSONL,
			VolumeID:             volumeID,
			HostID:               *hostID,
			DeviceID:             uint32(*deviceID),
			Mode:                 *mode,
			Concurrency:          *concurrency,
			Timeout:              *timeout,
			ClientNetworkProfile: *clientNetworkProfile,
			PayloadPattern:       *payloadPattern,
			Verify:               *verify,
			Attach:               *attach,
			Detach:               *detach,
		})
		if err != nil {
			if report.Result == "" {
				fatalf("gateway replay-load failed: %v", err)
			}
			writeJSON(report)
			os.Exit(1)
		}
		writeJSON(report)
	case "clone-io-check":
		fs := flag.NewFlagSet("gateway clone-io-check", flag.ExitOnError)
		gatewayURL := fs.String("gateway", "", "gateway base URL, e.g. http://u40:9899")
		rawVolumeID := fs.String("volume", "", "source volume id (8 lowercase hex digits)")
		cloneID := fs.String("clone-id", "", "clone id")
		hostID := fs.String("host-id", "phase-j-clone-debug", "gateway attachment host id")
		deviceID := fs.Uint("device-id", 9902, "gateway attachment device id")
		offsetRaw := fs.String("offset", "0", "I/O offset, e.g. 0 or 4m")
		lengthRaw := fs.String("length", "4k", "I/O length, e.g. 4k")
		attach := fs.Bool("attach", true, "attach the source volume before clone I/O")
		detach := fs.Bool("detach", true, "detach the source volume after clone I/O")
		timeout := fs.Duration("timeout", 60*time.Second, "overall HTTP client timeout")
		fs.Parse(args)
		if *gatewayURL == "" {
			fatalf("--gateway is required")
		}
		volumeID, err := parseRequiredVolumeID(*rawVolumeID)
		if err != nil {
			fatalf("--volume is required")
		}
		if strings.TrimSpace(*cloneID) == "" {
			fatalf("--clone-id is required")
		}
		if *deviceID > uint(^uint32(0)) {
			fatalf("--device-id is too large")
		}
		offset, err := parseByteOffset(*offsetRaw)
		if err != nil {
			fatalf("--offset: %v", err)
		}
		length, err := parseByteSize(*lengthRaw)
		if err != nil {
			fatalf("--length: %v", err)
		}
		client := &http.Client{Timeout: *timeout}
		report, err := runGatewayCloneIOCheck(client, strings.TrimRight(*gatewayURL, "/"), service.CanonicalVolumeID(volumeID), strings.TrimSpace(*cloneID), *hostID, uint32(*deviceID), offset, length, *attach, *detach)
		if err != nil {
			writeJSON(report)
			os.Exit(1)
		}
		writeJSON(report)
	default:
		usage()
		os.Exit(2)
	}
}

func runFile(verb string, args []string) {
	switch verb {
	case "load":
		fs := flag.NewFlagSet("file load", flag.ExitOnError)
		path := fs.String("path", "", "local file path used as the emulated block device")
		sizeRaw := fs.String("size", "16m", "per-job I/O span, e.g. 16m")
		bsRaw := fs.String("bs", "128k", "I/O block size, e.g. 128k")
		rw := fs.String("rw", "write", "fio-like workload: write, read, randwrite, randread, randrw")
		rwmixRead := fs.Int("rwmixread", 70, "read percentage for --rw=randrw")
		prefill := fs.Bool("prefill", true, "prefill the tested range before read/randread/randrw workloads")
		reset := fs.Bool("reset", true, "truncate and recreate the file before the load")
		fsync := fs.Bool("fsync", false, "fsync the file after the measured load")
		iodepth := fs.Int("iodepth", 16, "fio-like iodepth per job")
		numjobs := fs.Int("numjobs", 2, "fio-like job count")
		concurrency := fs.Int("concurrency", 0, "override total concurrent file I/O; default iodepth*numjobs")
		storageProfile := fs.String("storage-profile", "", "label for the local storage path")
		payloadPattern := fs.String("payload-pattern", "zero", "write payload pattern: zero or deterministic")
		verify := fs.Bool("verify", false, "read back each unique written block and verify payload bytes")
		fs.Parse(args)
		report, err := runFileLoad(fileLoadOptions{
			Path:           *path,
			SizeRaw:        *sizeRaw,
			BSRaw:          *bsRaw,
			RW:             *rw,
			RWMixRead:      *rwmixRead,
			Prefill:        *prefill,
			Reset:          *reset,
			Fsync:          *fsync,
			IODepth:        *iodepth,
			NumJobs:        *numjobs,
			Concurrency:    *concurrency,
			StorageProfile: *storageProfile,
			PayloadPattern: *payloadPattern,
			Verify:         *verify,
		})
		if err != nil {
			if report.Result == "" {
				fatalf("file load failed: %v", err)
			}
			writeJSON(report)
			os.Exit(1)
		}
		writeJSON(report)
	default:
		usage()
		os.Exit(2)
	}
}

func runUnsafe(verb string, args []string) {
	switch verb {
	case "attachment-clear":
		fs, cfg := newCommandFlagSet("unsafe attachment-clear")
		rawVolumeID := fs.String("volume", "", "volume id (8 lowercase hex digits)")
		yes := fs.Bool("yes", false, "confirm unsafe metadata mutation")
		fs.Parse(args)
		requireYes(*yes)
		volumeID, err := parseRequiredVolumeID(*rawVolumeID)
		if err != nil {
			fatalf("--volume is required")
		}
		repo, closeFn := mustOpenRepository(cfg)
		defer closeFn()
		rec, err := repo.UnsafeClearAttachment(context.Background(), volumeID)
		if err != nil {
			fatalf("unsafe attachment-clear failed: %v", err)
		}
		if cfg.jsonOutput {
			writeJSON(rec)
			return
		}
		fmt.Printf("unsafe_ok volume_id=%s generation=%d\n", service.CanonicalVolumeID(volumeID), rec.Generation)
	case "generation-set":
		fs, cfg := newCommandFlagSet("unsafe generation-set")
		rawVolumeID := fs.String("volume", "", "volume id (8 lowercase hex digits)")
		generation := fs.Uint64("generation", 0, "new generation")
		yes := fs.Bool("yes", false, "confirm unsafe metadata mutation")
		fs.Parse(args)
		requireYes(*yes)
		volumeID, err := parseRequiredVolumeID(*rawVolumeID)
		if err != nil || *generation == 0 {
			fatalf("--volume and --generation are required")
		}
		repo, closeFn := mustOpenRepository(cfg)
		defer closeFn()
		next, err := repo.UnsafeSetGeneration(context.Background(), volumeID, *generation)
		if err != nil {
			fatalf("unsafe generation-set failed: %v", err)
		}
		if cfg.jsonOutput {
			writeJSON(map[string]any{"volume_id": service.CanonicalVolumeID(volumeID), "generation": next})
			return
		}
		fmt.Printf("unsafe_ok volume_id=%s generation=%d\n", service.CanonicalVolumeID(volumeID), next)
	case "gc-sweep":
		fs, cfg := newCommandFlagSet("unsafe gc-sweep")
		rawVolumeID := fs.String("volume", "", "volume id (8 lowercase hex digits)")
		limit := fs.Int("limit", 256, "maximum chunk garbage candidates to process")
		yes := fs.Bool("yes", false, "confirm unsafe metadata mutation")
		fs.Parse(args)
		requireYes(*yes)
		volumeID, err := parseRequiredVolumeID(*rawVolumeID)
		if err != nil {
			fatalf("--volume is required")
		}
		repo, closeFn := mustOpenRepository(cfg)
		defer closeFn()
		objects, closeObjects, err := newObjectStore(context.Background(), cfg)
		if err != nil {
			fatalf("open object store failed: %v", err)
		}
		defer closeObjects()
		collector := service.NewChunkGarbageCollector(repo, objects)
		result, err := collector.SweepVolume(context.Background(), volumeID, *limit)
		if err != nil {
			fatalf("unsafe gc-sweep failed: %v", err)
		}
		report := chunkGarbageSweepReport{
			VolumeID:       result.VolumeID,
			CandidateCount: result.CandidateCount,
			DeletedCount:   result.DeletedCount,
			RetainedCount:  result.RetainedCount,
		}
		if cfg.jsonOutput {
			writeJSON(report)
			return
		}
		fmt.Printf("unsafe_ok volume_id=%s candidates=%d deleted=%d retained=%d\n",
			service.CanonicalVolumeID(uint64(report.VolumeID)), report.CandidateCount, report.DeletedCount, report.RetainedCount)
	default:
		usage()
		os.Exit(2)
	}
}

func newCommandFlagSet(name string) (*flag.FlagSet, *commandConfig) {
	fs := flag.NewFlagSet(name, flag.ExitOnError)
	cfg := &commandConfig{}
	fs.StringVar(&cfg.etcdEndpoints, "etcd-endpoints", "127.0.0.1:2379", "comma-separated etcd endpoints")
	fs.StringVar(&cfg.etcdRoot, "etcd-root", "/namrbd", "etcd metadata root prefix")
	fs.StringVar(&cfg.storeBackend, "store-backend", "memory", "object store backend: memory (redis requires -tags legacy_redis)")
	fs.StringVar(&cfg.redisAddr, "redis-addr", "127.0.0.1:6379", "redis object store address (requires -tags legacy_redis)")
	fs.BoolVar(&cfg.jsonOutput, "json", false, "emit JSON output")
	return fs, cfg
}

func mustOpenRepository(cfg *commandConfig) (service.MetadataRepository, func()) {
	endpoints := splitCommaList(cfg.etcdEndpoints)
	client, err := metadata.NewEtcdClient(endpoints, 5*time.Second)
	if err != nil {
		fatalf("create etcd client failed: %v", err)
	}
	repo := metadata.NewEtcdRepository(client, cfg.etcdRoot)
	return repo, func() { _ = client.Close() }
}

func splitCommaList(raw string) []string {
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part != "" {
			out = append(out, part)
		}
	}
	return out
}

func writeJSON(v any) {
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	if err := enc.Encode(v); err != nil {
		fatalf("write JSON failed: %v", err)
	}
}

func volumeSpecOutputFrom(volume service.VolumeSpec) volumeSpecOutput {
	return volumeSpecOutput{
		VolumeSpec:               volume,
		AllocationChunkSizeBytes: volume.ChunkSizeBytes,
		AllocationPageBytes:      volume.ExtentPageBytes,
	}
}

func parseRequiredVolumeID(raw string) (uint64, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return 0, fmt.Errorf("volume is required")
	}
	return volumeid.Parse(raw)
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage: namrbd-debug <resource> <verb> [flags]")
	fmt.Fprintln(os.Stderr, "")
	fmt.Fprintln(os.Stderr, "namrbd-debug is the low-level debug and break-glass metadata CLI.")
	fmt.Fprintln(os.Stderr, "Use namrbdctl and sbsctl as the primary operational surfaces. Historical namrbd-meta source is archived under stale-codes and is not an active CLI.")
	fmt.Fprintln(os.Stderr, "")
	fmt.Fprintln(os.Stderr, "resources:")
	fmt.Fprintln(os.Stderr, "  volume inspect-layout")
	fmt.Fprintln(os.Stderr, "  validate extents")
	fmt.Fprintln(os.Stderr, "  gateway write-load|replay-load|clone-io-check")
	fmt.Fprintln(os.Stderr, "  file load")
	fmt.Fprintln(os.Stderr, "  unsafe attachment-clear|generation-set|gc-sweep")
	fmt.Fprintln(os.Stderr, "")
	fmt.Fprintln(os.Stderr, "global flags per command:")
	fmt.Fprintln(os.Stderr, "  --etcd-endpoints 127.0.0.1:2379")
	fmt.Fprintln(os.Stderr, "  --etcd-root /namrbd")
	fmt.Fprintln(os.Stderr, "  --store-backend memory")
	fmt.Fprintln(os.Stderr, "  --redis-addr 127.0.0.1:6379   (legacy Redis object store; requires -tags legacy_redis)")
	fmt.Fprintln(os.Stderr, "  --json")
}

type gatewayWriteLoadOptions struct {
	GatewayURL           string
	GatewayURLs          string
	ActiveLanes          int
	VolumeID             uint64
	HostID               string
	DeviceID             uint32
	SizeRaw              string
	BSRaw                string
	RW                   string
	RWMixRead            int
	Prefill              bool
	IODepth              int
	NumJobs              int
	Concurrency          int
	WarmupRequests       int
	SteadyStateSkip      int
	Timeout              time.Duration
	ClientNetworkProfile string
	PayloadPattern       string
	PayloadVerifyVolume  string
	IntegrityScenarios   string
	Verify               bool
	Attach               bool
	Detach               bool
	HTTPClient           *http.Client
}

type gatewayReplayLoadOptions struct {
	GatewayURL           string
	GatewayURLs          string
	ActiveLanes          int
	TraceJSONL           string
	VolumeID             uint64
	HostID               string
	DeviceID             uint32
	Mode                 string
	Concurrency          int
	Timeout              time.Duration
	ClientNetworkProfile string
	PayloadPattern       string
	Verify               bool
	Attach               bool
	Detach               bool
	HTTPClient           *http.Client
}

type gatewayReplayTrace struct {
	Operations []gatewayReplayTraceOp
	TotalCount int
	Skipped    int
}

type gatewayReplayTraceOp struct {
	Seq            int     `json:"seq"`
	Source         string  `json:"source,omitempty"`
	TraceSource    string  `json:"trace_source,omitempty"`
	Op             string  `json:"op"`
	OffsetBytes    uint64  `json:"offset_bytes"`
	LengthBytes    uint64  `json:"length_bytes"`
	PayloadBytes   uint64  `json:"payload_bytes,omitempty"`
	SubmitDeltaUS  int64   `json:"submit_delta_us,omitempty"`
	TS             string  `json:"ts,omitempty"`
	GatewayID      string  `json:"gateway_id,omitempty"`
	PathID         *uint32 `json:"path_id,omitempty"`
	StatusCode     int32   `json:"status_code,omitempty"`
	ReplayEligible *bool   `json:"replay_eligible,omitempty"`
}

type gatewayReplayExpectedRange struct {
	OffsetBytes uint64
	LengthBytes uint64
	Kind        string
}

type gatewayTargetSet struct {
	urls            []string
	lanes           []gatewayLaneReport
	gatewayIDToLane map[string]int
	activeLaneCount int
}

func newGatewayTargetSet(urls []string, activeLaneOverride, numjobs, concurrency int) gatewayTargetSet {
	active := len(urls)
	const defaultActiveLanes = 2
	if activeLaneOverride > 0 && active > activeLaneOverride {
		active = activeLaneOverride
	} else if activeLaneOverride <= 0 && defaultActiveLanes > 0 && active > defaultActiveLanes {
		active = defaultActiveLanes
	}
	if numjobs > 0 && active > numjobs {
		active = numjobs
	}
	if concurrency > 0 && active > concurrency {
		active = concurrency
	}
	if active < 1 {
		active = 1
	}
	lanes := make([]gatewayLaneReport, active)
	gatewayIDToLane := make(map[string]int, active*4)
	for laneID := 0; laneID < active; laneID++ {
		gatewayID := defaultGatewayIDForURL(urls[laneID])
		lanes[laneID] = gatewayLaneReport{
			LaneID:     laneID,
			GatewayURL: urls[laneID],
			GatewayID:  gatewayID,
		}
		for _, alias := range gatewayIDAliasesForURL(urls[laneID]) {
			if alias == "" {
				continue
			}
			if _, exists := gatewayIDToLane[alias]; !exists {
				gatewayIDToLane[alias] = laneID
			}
		}
	}
	return gatewayTargetSet{
		urls:            urls,
		lanes:           lanes,
		gatewayIDToLane: gatewayIDToLane,
		activeLaneCount: active,
	}
}

func defaultGatewayIDForURL(rawURL string) string {
	aliases := gatewayIDAliasesForURL(rawURL)
	for _, alias := range aliases {
		if strings.HasPrefix(alias, "gw-") {
			return alias
		}
	}
	if len(aliases) > 0 {
		return aliases[0]
	}
	return ""
}

func gatewayIDAliasesForURL(rawURL string) []string {
	parsed, err := url.Parse(strings.TrimSpace(rawURL))
	if err != nil {
		return nil
	}
	host := strings.ToLower(strings.TrimSpace(parsed.Hostname()))
	if host == "" {
		return nil
	}
	seen := make(map[string]struct{}, 6)
	var aliases []string
	add := func(value string) {
		value = strings.ToLower(strings.TrimSpace(value))
		if value == "" {
			return
		}
		if _, exists := seen[value]; exists {
			return
		}
		seen[value] = struct{}{}
		aliases = append(aliases, value)
	}
	add(host)
	if shortHost := strings.Split(host, ".")[0]; shortHost != host {
		add(shortHost)
	}
	if !strings.HasPrefix(host, "gw-") {
		add("gw-" + host)
		if shortHost := strings.Split(host, ".")[0]; shortHost != "" {
			add("gw-" + shortHost)
		}
	} else {
		add(strings.TrimPrefix(host, "gw-"))
	}
	return aliases
}

func (s gatewayTargetSet) primaryURL() string {
	if len(s.urls) == 0 {
		return ""
	}
	return s.urls[0]
}

func (s gatewayTargetSet) targetForLoadIndex(idx, blocksPerJob int) (string, int) {
	if s.activeLaneCount <= 1 {
		return s.primaryURL(), 0
	}
	jobID := idx
	if blocksPerJob > 0 {
		jobID = idx / blocksPerJob
	}
	laneID := jobID % s.activeLaneCount
	return s.lanes[laneID].GatewayURL, laneID
}

func (s gatewayTargetSet) targetForWriteLoadIndex(idx, blocksPerJob int) (string, int) {
	block := idx
	if blocksPerJob >= s.activeLaneCount {
		block = idx % blocksPerJob
	}
	return s.targetForBlock(block)
}

func (s gatewayTargetSet) targetForBlock(block int) (string, int) {
	if s.activeLaneCount <= 1 {
		return s.primaryURL(), 0
	}
	laneID := block % s.activeLaneCount
	return s.lanes[laneID].GatewayURL, laneID
}

func parseGatewayTargetURLs(primaryRaw, listRaw string) ([]string, error) {
	rawParts := []string{primaryRaw}
	if strings.TrimSpace(listRaw) != "" {
		rawParts = strings.Split(listRaw, ",")
	}
	seen := make(map[string]struct{})
	urls := make([]string, 0, len(rawParts))
	for _, raw := range rawParts {
		url := strings.TrimRight(strings.TrimSpace(raw), "/")
		if url == "" {
			continue
		}
		if _, ok := seen[url]; ok {
			continue
		}
		seen[url] = struct{}{}
		urls = append(urls, url)
	}
	if len(urls) == 0 {
		if strings.TrimSpace(listRaw) != "" {
			return nil, fmt.Errorf("--gateways must include at least one gateway URL")
		}
		return nil, fmt.Errorf("--gateway is required")
	}
	return urls, nil
}

type fileLoadOptions struct {
	Path           string
	SizeRaw        string
	BSRaw          string
	RW             string
	RWMixRead      int
	Prefill        bool
	Reset          bool
	Fsync          bool
	IODepth        int
	NumJobs        int
	Concurrency    int
	StorageProfile string
	PayloadPattern string
	Verify         bool
}

func runFileLoad(opts fileLoadOptions) (fileLoadReport, error) {
	path := strings.TrimSpace(opts.Path)
	if path == "" {
		return fileLoadReport{}, fmt.Errorf("--path is required")
	}
	sizeBytes, blockBytes, rw, concurrency, blocksPerJob, requestCount, payloadPattern, err := validateWorkloadShape(opts.SizeRaw, opts.BSRaw, opts.RW, opts.RWMixRead, opts.IODepth, opts.NumJobs, opts.Concurrency, opts.PayloadPattern)
	if err != nil {
		return fileLoadReport{}, err
	}
	prefillActive := opts.Prefill && gatewayWorkloadNeedsPrefill(rw)
	report := fileLoadReport{
		Result:         "ok",
		Path:           path,
		StorageProfile: opts.StorageProfile,
		RW:             rw,
		RWMixRead:      opts.RWMixRead,
		Prefill:        prefillActive,
		Reset:          opts.Reset,
		Fsync:          opts.Fsync,
		SizeBytes:      sizeBytes,
		BlockSizeBytes: blockBytes,
		IODepth:        opts.IODepth,
		NumJobs:        opts.NumJobs,
		Concurrency:    concurrency,
		PayloadPattern: payloadPattern,
		Verify:         opts.Verify,
		RequestCount:   requestCount,
	}

	if dir := filepath.Dir(path); dir != "." && dir != "" {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return report, fmt.Errorf("create file directory: %w", err)
		}
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return report, fmt.Errorf("open file: %w", err)
	}
	defer file.Close()
	if opts.Reset {
		if err := file.Truncate(0); err != nil {
			return report, fmt.Errorf("truncate file: %w", err)
		}
	}
	if err := file.Truncate(int64(sizeBytes)); err != nil {
		return report, fmt.Errorf("size file: %w", err)
	}

	seedID := "file:" + path
	if prefillActive {
		prefillCount, prefillErrors, err := filePrefillLoad(file, seedID, blocksPerJob, blockBytes, payloadPattern)
		report.PrefillCount = prefillCount
		report.PrefillErrorCount = prefillErrors
		if err != nil {
			report.Result = "error"
			report.FirstError = err.Error()
			return report, err
		}
	}

	jobs := make(chan int)
	latencies := make([]float64, 0, requestCount)
	var mu sync.Mutex
	var firstErr string
	started := time.Now()
	var wg sync.WaitGroup
	for worker := 0; worker < concurrency; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			readBuf := make([]byte, blockBytes)
			zeroPayload := make([]byte, blockBytes)
			for idx := range jobs {
				offset := gatewayWorkloadOffset(idx, blocksPerJob, blockBytes, rw)
				op := gatewayWorkloadOperation(idx, rw, opts.RWMixRead)
				reqStarted := time.Now()
				var err error
				switch op {
				case "read":
					err = fileReadAt(file, readBuf, offset)
				default:
					payload := zeroPayload
					if payloadPattern == "deterministic" {
						payload = gatewayDeterministicPayload(seedID, offset, blockBytes)
					}
					_, err = file.WriteAt(payload, int64(offset))
				}
				latencyMS := float64(time.Since(reqStarted).Microseconds()) / 1000.0
				mu.Lock()
				latencies = append(latencies, latencyMS)
				if op == "read" {
					report.ReadCount++
				} else {
					report.WriteCount++
				}
				if err != nil {
					report.ErrorCount++
					if op == "read" {
						report.ReadErrorCount++
					} else {
						report.WriteErrorCount++
					}
					if firstErr == "" {
						firstErr = err.Error()
					}
				} else {
					report.OKCount++
					if op == "read" {
						report.ReadOKCount++
					} else {
						report.WriteOKCount++
					}
				}
				mu.Unlock()
			}
		}()
	}
	for idx := 0; idx < requestCount; idx++ {
		jobs <- idx
	}
	close(jobs)
	wg.Wait()
	if opts.Fsync {
		if err := file.Sync(); err != nil {
			report.ErrorCount++
			if firstErr == "" {
				firstErr = fmt.Sprintf("fsync failed: %v", err)
			}
		}
	}
	elapsed := time.Since(started)
	report.ElapsedMS = elapsed.Milliseconds()
	report.FirstError = firstErr
	if report.ErrorCount > 0 {
		report.Result = "error"
	}
	if elapsed > 0 {
		report.WriteIOPS = float64(report.WriteOKCount) / elapsed.Seconds()
		report.WriteBWKiB = float64(uint64(report.WriteOKCount)*blockBytes) / 1024.0 / elapsed.Seconds()
		report.ReadIOPS = float64(report.ReadOKCount) / elapsed.Seconds()
		report.ReadBWKiB = float64(uint64(report.ReadOKCount)*blockBytes) / 1024.0 / elapsed.Seconds()
		report.TotalIOPS = float64(report.OKCount) / elapsed.Seconds()
		report.TotalBWKiB = float64(uint64(report.OKCount)*blockBytes) / 1024.0 / elapsed.Seconds()
	}
	fillFileLatencySummary(&report, latencies)
	if report.ErrorCount > 0 {
		return report, fmt.Errorf("file load completed with %d errors", report.ErrorCount)
	}
	if opts.Verify {
		if err := verifyFileWriteLoad(file, seedID, blocksPerJob, blockBytes, payloadPattern, &report); err != nil {
			report.Result = "error"
			return report, err
		}
	}
	return report, nil
}

func validateWorkloadShape(sizeRaw, bsRaw, rwRaw string, rwmixRead, iodepth, numjobs, concurrency int, payloadPatternRaw string) (uint64, uint64, string, int, int, int, string, error) {
	sizeBytes, err := parseByteSize(sizeRaw)
	if err != nil {
		return 0, 0, "", 0, 0, 0, "", fmt.Errorf("invalid --size: %w", err)
	}
	blockBytes, err := parseByteSize(bsRaw)
	if err != nil {
		return 0, 0, "", 0, 0, 0, "", fmt.Errorf("invalid --bs: %w", err)
	}
	if blockBytes == 0 || blockBytes > sizeBytes {
		return 0, 0, "", 0, 0, 0, "", fmt.Errorf("--bs must be greater than zero and no larger than --size")
	}
	if sizeBytes > uint64(1<<63-1) || blockBytes > uint64(1<<63-1) {
		return 0, 0, "", 0, 0, 0, "", fmt.Errorf("--size and --bs must fit in signed 64-bit file offsets")
	}
	if iodepth <= 0 {
		return 0, 0, "", 0, 0, 0, "", fmt.Errorf("--iodepth must be greater than zero")
	}
	if numjobs <= 0 {
		return 0, 0, "", 0, 0, 0, "", fmt.Errorf("--numjobs must be greater than zero")
	}
	rw := strings.TrimSpace(rwRaw)
	if rw == "" {
		rw = "write"
	}
	switch rw {
	case "write", "read", "randwrite", "randread", "randrw":
	default:
		return 0, 0, "", 0, 0, 0, "", fmt.Errorf("--rw must be write, read, randwrite, randread, or randrw")
	}
	if rwmixRead < 0 || rwmixRead > 100 {
		return 0, 0, "", 0, 0, 0, "", fmt.Errorf("--rwmixread must be between 0 and 100")
	}
	if concurrency <= 0 {
		concurrency = iodepth * numjobs
	}
	if concurrency <= 0 {
		return 0, 0, "", 0, 0, 0, "", fmt.Errorf("--concurrency must be greater than zero")
	}
	blocksPerJob := int((sizeBytes + blockBytes - 1) / blockBytes)
	requestCount := blocksPerJob * numjobs
	if requestCount <= 0 {
		return 0, 0, "", 0, 0, 0, "", fmt.Errorf("computed request count is zero")
	}
	payloadPattern := strings.TrimSpace(payloadPatternRaw)
	if payloadPattern == "" {
		payloadPattern = "zero"
	}
	switch payloadPattern {
	case "zero", "deterministic":
	default:
		return 0, 0, "", 0, 0, 0, "", fmt.Errorf("--payload-pattern must be zero or deterministic")
	}
	return sizeBytes, blockBytes, rw, concurrency, blocksPerJob, requestCount, payloadPattern, nil
}

func runGatewayWriteLoad(opts gatewayWriteLoadOptions) (report gatewayWriteLoadReport, err error) {
	if opts.HostID == "" {
		return gatewayWriteLoadReport{}, fmt.Errorf("--host-id is required")
	}
	if opts.DeviceID == 0 {
		return gatewayWriteLoadReport{}, fmt.Errorf("--device-id must be greater than zero")
	}
	sizeBytes, blockBytes, rw, concurrency, blocksPerJob, requestCount, payloadPattern, err := validateWorkloadShape(opts.SizeRaw, opts.BSRaw, opts.RW, opts.RWMixRead, opts.IODepth, opts.NumJobs, opts.Concurrency, opts.PayloadPattern)
	if err != nil {
		return gatewayWriteLoadReport{}, err
	}
	payloadVerifyVolume := strings.TrimSpace(opts.PayloadVerifyVolume)
	if payloadVerifyVolume != "" {
		if _, err := parseRequiredVolumeID(payloadVerifyVolume); err != nil {
			return gatewayWriteLoadReport{}, fmt.Errorf("invalid --verify-volume: %w", err)
		}
	}
	gatewayURLs, err := parseGatewayTargetURLs(opts.GatewayURL, opts.GatewayURLs)
	if err != nil {
		return gatewayWriteLoadReport{}, err
	}
	if opts.ActiveLanes < 0 {
		return gatewayWriteLoadReport{}, fmt.Errorf("--active-lanes must be greater than or equal to zero")
	}
	targets := newGatewayTargetSet(gatewayURLs, opts.ActiveLanes, opts.NumJobs, concurrency)
	gatewayURL := targets.primaryURL()
	if opts.Timeout <= 0 {
		opts.Timeout = 240 * time.Second
	}
	if opts.WarmupRequests < 0 {
		return gatewayWriteLoadReport{}, fmt.Errorf("--warmup-requests must be greater than or equal to zero")
	}
	steadyStateSkip := opts.SteadyStateSkip
	if steadyStateSkip <= 0 {
		steadyStateSkip = concurrency
	}
	if steadyStateSkip > requestCount {
		steadyStateSkip = requestCount
	}

	volumeID := service.CanonicalVolumeID(opts.VolumeID)
	prefillActive := opts.Prefill && gatewayWorkloadNeedsPrefill(rw)
	report = gatewayWriteLoadReport{
		Result:               "ok",
		GatewayURL:           gatewayURL,
		GatewayURLs:          gatewayURLs,
		GatewayPolicy:        "kernel-lane",
		ActiveLaneCount:      targets.activeLaneCount,
		GatewayLanes:         targets.lanes,
		GatewayRequestCounts: make(map[string]int, len(gatewayURLs)),
		GatewayOKCounts:      make(map[string]int, len(gatewayURLs)),
		GatewayErrorCounts:   make(map[string]int, len(gatewayURLs)),
		VolumeID:             volumeID,
		ClientNetworkProfile: opts.ClientNetworkProfile,
		HostID:               opts.HostID,
		DeviceID:             opts.DeviceID,
		RW:                   rw,
		RWMixRead:            opts.RWMixRead,
		Prefill:              prefillActive,
		SizeBytes:            sizeBytes,
		BlockSizeBytes:       blockBytes,
		IODepth:              opts.IODepth,
		NumJobs:              opts.NumJobs,
		Concurrency:          concurrency,
		WarmupRequests:       opts.WarmupRequests,
		SteadyStateSkip:      steadyStateSkip,
		PayloadPattern:       payloadPattern,
		PayloadVerifyVolume:  payloadVerifyVolume,
		Verify:               opts.Verify,
		RequestCount:         requestCount,
	}
	client := opts.HTTPClient
	if client == nil {
		client = &http.Client{
			Timeout: opts.Timeout,
			Transport: &http.Transport{
				MaxIdleConns:        concurrency * len(gatewayURLs) * 2,
				MaxIdleConnsPerHost: concurrency * 2,
				IdleConnTimeout:     90 * time.Second,
			},
		}
	}

	if opts.Attach {
		attachmentID, generation, err := gatewayAttach(client, gatewayURL, volumeID, opts.HostID, opts.DeviceID)
		if err != nil {
			report.Result = "error"
			report.ErrorCount = 1
			report.FirstError = "attach failed: " + err.Error()
			report.LastError = report.FirstError
			return report, err
		}
		report.AttachmentID = attachmentID
		report.Generation = generation
	}
	if opts.Detach {
		defer func() {
			if report.AttachmentID != "" {
				detachSummary, detachErr := gatewayDetachAll(client, gatewayURLs, volumeID, opts.HostID, report.AttachmentID)
				recordGatewayDetachSummary(&report, detachSummary)
				if detachErr == nil {
					report.Detached = detachSummary.OKCount > 0
					return
				}
				report.DetachError = detachErr.Error()
				if report.FirstError == "" {
					report.FirstError = "detach failed: " + detachErr.Error()
				}
				report.LastError = "detach failed: " + detachErr.Error()
				if report.Result == "ok" {
					report.Result = "error"
					report.ErrorCount++
				}
				err = errors.Join(err, detachErr)
			}
		}()
	}

	scenarios, err := parseGatewayIntegrityScenarios(opts.IntegrityScenarios)
	if err != nil {
		return report, err
	}
	if len(scenarios) > 0 {
		report.IntegrityScenarios = strings.Join(scenarios, ",")
		report.RequestCount = 0
		started := time.Now()
		err := runGatewayIntegrityScenarios(client, gatewayURL, volumeID, sizeBytes, blockBytes, scenarios, &report)
		elapsed := time.Since(started)
		report.ElapsedMS = elapsed.Milliseconds()
		if elapsed > 0 {
			report.WriteIOPS = float64(report.OKCount) / elapsed.Seconds()
			report.WriteBWKiB = float64(uint64(report.OKCount)*blockBytes) / 1024.0 / elapsed.Seconds()
			report.TotalIOPS = report.WriteIOPS
			report.TotalBWKiB = report.WriteBWKiB
		}
		if err != nil {
			report.Result = "error"
			return report, err
		}
		return report, nil
	}

	if prefillActive {
		prefillCount, prefillErrors, err := gatewayPrefillWriteLoad(client, targets, volumeID, blocksPerJob, blockBytes, payloadPattern)
		report.PrefillCount = prefillCount
		report.PrefillErrorCount = prefillErrors
		if err != nil {
			report.Result = "error"
			report.FirstError = err.Error()
			report.LastError = err.Error()
			return report, err
		}
	}

	zeroPayloadBase64 := base64.StdEncoding.EncodeToString(make([]byte, blockBytes))
	if opts.WarmupRequests > 0 {
		warmupOK, warmupErrors, warmupLatencies, err := runGatewayLoadWarmupRequests(client, targets, volumeID, blocksPerJob, blockBytes, rw, opts.RWMixRead, payloadPattern, zeroPayloadBase64, concurrency, opts.WarmupRequests)
		report.WarmupOKCount = warmupOK
		report.WarmupErrorCount = warmupErrors
		report.WarmupLatency = newGatewayLoadLatencySummary(warmupLatencies)
		if err != nil {
			report.Result = "error"
			report.FirstError = err.Error()
			report.LastError = err.Error()
			return report, err
		}
	}

	jobs := make(chan int)
	latencies := make([]float64, 0, requestCount)
	coldStartLatencies := make([]float64, 0, steadyStateSkip)
	steadyStateLatencies := make([]float64, 0, requestCount-steadyStateSkip)
	slowOperations := make([]gatewayLoadSlowOperation, 0, requestCount)
	var mu sync.Mutex
	var firstErr string
	var lastErr string
	started := time.Now()
	var wg sync.WaitGroup
	for worker := 0; worker < concurrency; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for idx := range jobs {
				jobOffset := gatewayWorkloadOffset(idx, blocksPerJob, blockBytes, rw)
				op := gatewayWorkloadOperation(idx, rw, opts.RWMixRead)
				phase := gatewayLoadPhase(idx, steadyStateSkip)
				targetURL, laneID := targets.targetForWriteLoadIndex(idx, blocksPerJob)
				reqStarted := time.Now()
				var err error
				var opResp gatewayOperationResponse
				switch op {
				case "read":
					opResp, err = gatewayReadOperationWithLoadMetadata(client, targetURL, volumeID, jobOffset, blockBytes, idx, phase)
				default:
					dataBase64 := zeroPayloadBase64
					if payloadPattern == "deterministic" {
						dataBase64 = base64.StdEncoding.EncodeToString(gatewayDeterministicPayload(volumeID, jobOffset, blockBytes))
					}
					opResp, err = gatewayWriteWithLoadMetadata(client, targetURL, volumeID, jobOffset, blockBytes, dataBase64, idx, phase)
				}
				latencyMS := float64(time.Since(reqStarted).Microseconds()) / 1000.0
				throttle := opResp.PhaseOThrottle
				if throttle == nil && err != nil {
					throttle = gatewayPhaseOThrottleObservationFromError(err)
				}
				mu.Lock()
				report.GatewayRequestCounts[targetURL]++
				latencies = append(latencies, latencyMS)
				if idx < steadyStateSkip {
					coldStartLatencies = append(coldStartLatencies, latencyMS)
				} else {
					steadyStateLatencies = append(steadyStateLatencies, latencyMS)
				}
				sample := gatewayLoadSlowOperation{
					Index:       idx,
					Phase:       phase,
					GatewayURL:  targetURL,
					LaneID:      laneID,
					Op:          op,
					OffsetBytes: jobOffset,
					LengthBytes: blockBytes,
					LatencyMS:   latencyMS,
				}
				if err != nil {
					sample.Error = err.Error()
				}
				if throttle != nil {
					sample.PhaseOThrottleWaitMS = throttle.ThrottleWaitMs
					sample.PhaseOThrottleRejected = throttle.RejectedOps > 0
					sample.PhaseORejectionReason = throttle.RejectionReason
					recordGatewayPhaseOThrottle(&report, throttle)
				}
				slowOperations = append(slowOperations, sample)
				if op == "read" {
					report.ReadCount++
				} else {
					report.WriteCount++
				}
				if err != nil {
					report.ErrorCount++
					report.GatewayErrorCounts[targetURL]++
					lastErr = err.Error()
					if op == "read" {
						report.ReadErrorCount++
					} else {
						report.WriteErrorCount++
					}
					if firstErr == "" {
						firstErr = err.Error()
					}
				} else {
					report.OKCount++
					report.GatewayOKCounts[targetURL]++
					if op == "read" {
						report.ReadOKCount++
					} else {
						report.WriteOKCount++
					}
				}
				mu.Unlock()
			}
		}()
	}
	for idx := 0; idx < requestCount; idx++ {
		jobs <- idx
	}
	close(jobs)
	wg.Wait()
	elapsed := time.Since(started)
	report.ElapsedMS = elapsed.Milliseconds()
	report.FirstError = firstErr
	report.LastError = lastErr
	if report.ErrorCount > 0 {
		report.Result = "error"
	}
	if elapsed > 0 {
		report.WriteIOPS = float64(report.WriteOKCount) / elapsed.Seconds()
		report.WriteBWKiB = float64(uint64(report.WriteOKCount)*blockBytes) / 1024.0 / elapsed.Seconds()
		report.ReadIOPS = float64(report.ReadOKCount) / elapsed.Seconds()
		report.ReadBWKiB = float64(uint64(report.ReadOKCount)*blockBytes) / 1024.0 / elapsed.Seconds()
		report.TotalIOPS = float64(report.OKCount) / elapsed.Seconds()
		report.TotalBWKiB = float64(uint64(report.OKCount)*blockBytes) / 1024.0 / elapsed.Seconds()
	}
	fillLatencySummary(&report, latencies)
	report.ColdStartLatency = newGatewayLoadLatencySummary(coldStartLatencies)
	report.SteadyStateLatency = newGatewayLoadLatencySummary(steadyStateLatencies)
	report.SlowOperations = topGatewayLoadSlowOperations(slowOperations, 16)
	if report.ErrorCount > 0 {
		return report, fmt.Errorf("gateway write-load completed with %d errors", report.ErrorCount)
	}
	if opts.Verify {
		verifyVolumeID := volumeID
		if payloadVerifyVolume != "" {
			verifyVolumeID = payloadVerifyVolume
		}
		if err := verifyGatewayWriteLoad(client, targets, volumeID, verifyVolumeID, blocksPerJob, blockBytes, payloadPattern, &report); err != nil {
			report.Result = "error"
			if report.FirstError == "" {
				report.FirstError = report.VerifyFirstError
			}
			if report.LastError == "" {
				report.LastError = report.VerifyLastError
			}
			return report, err
		}
	}
	return report, nil
}

func runGatewayReplayLoad(opts gatewayReplayLoadOptions) (gatewayReplayLoadReport, error) {
	if opts.HostID == "" {
		return gatewayReplayLoadReport{}, fmt.Errorf("--host-id is required")
	}
	if opts.DeviceID == 0 {
		return gatewayReplayLoadReport{}, fmt.Errorf("--device-id must be greater than zero")
	}
	tracePath := strings.TrimSpace(opts.TraceJSONL)
	if tracePath == "" {
		return gatewayReplayLoadReport{}, fmt.Errorf("--trace-jsonl is required")
	}
	mode := strings.TrimSpace(opts.Mode)
	if mode == "" {
		mode = "saturating"
	}
	switch mode {
	case "saturating", "paced":
	default:
		return gatewayReplayLoadReport{}, fmt.Errorf("--mode must be saturating or paced")
	}
	payloadPattern := strings.TrimSpace(opts.PayloadPattern)
	if payloadPattern == "" {
		payloadPattern = "zero"
	}
	switch payloadPattern {
	case "zero", "deterministic":
	default:
		return gatewayReplayLoadReport{}, fmt.Errorf("--payload-pattern must be zero or deterministic")
	}
	if opts.ActiveLanes < 0 {
		return gatewayReplayLoadReport{}, fmt.Errorf("--active-lanes must be greater than or equal to zero")
	}
	gatewayURLs, err := parseGatewayTargetURLs(opts.GatewayURL, opts.GatewayURLs)
	if err != nil {
		return gatewayReplayLoadReport{}, err
	}
	trace, err := loadGatewayReplayTrace(tracePath)
	if err != nil {
		return gatewayReplayLoadReport{}, err
	}
	if len(trace.Operations) == 0 {
		return gatewayReplayLoadReport{}, fmt.Errorf("trace contains no replay-eligible operations")
	}
	concurrency := opts.Concurrency
	if concurrency <= 0 {
		concurrency = minInt(len(trace.Operations), 16)
	}
	if concurrency <= 0 {
		return gatewayReplayLoadReport{}, fmt.Errorf("--concurrency must be greater than zero")
	}
	targets := newGatewayTargetSet(gatewayURLs, opts.ActiveLanes, len(gatewayURLs), len(gatewayURLs))
	gatewayURL := targets.primaryURL()
	if opts.Timeout <= 0 {
		opts.Timeout = 240 * time.Second
	}

	volumeID := service.CanonicalVolumeID(opts.VolumeID)
	report := gatewayReplayLoadReport{
		Result:                 "ok",
		GatewayURL:             gatewayURL,
		GatewayURLs:            gatewayURLs,
		GatewayPolicy:          "kernel-lane",
		ActiveLaneCount:        targets.activeLaneCount,
		GatewayLanes:           targets.lanes,
		ReplayLaneCounts:       make([]gatewayLaneCountReport, len(targets.lanes)),
		ReplaySelectionCounts:  make(map[string]int, 4),
		GatewayRequestCounts:   make(map[string]int, len(gatewayURLs)),
		GatewayOKCounts:        make(map[string]int, len(gatewayURLs)),
		GatewayErrorCounts:     make(map[string]int, len(gatewayURLs)),
		VolumeID:               volumeID,
		ClientNetworkProfile:   opts.ClientNetworkProfile,
		HostID:                 opts.HostID,
		DeviceID:               opts.DeviceID,
		TraceJSONL:             tracePath,
		ReplayMode:             mode,
		TraceOperationCount:    trace.TotalCount,
		RequestCount:           len(trace.Operations),
		SkippedOperationCount:  trace.Skipped,
		Concurrency:            concurrency,
		PayloadPattern:         payloadPattern,
		Verify:                 opts.Verify,
		PathSurface:            "kernel_origin_replay",
		ClaimClassification:    "kernel_origin_shape_only",
		KernelPayloadReplayed:  false,
		SupportClaimed:         false,
		PublicBenchmarkClaimed: false,
	}
	for laneID, lane := range targets.lanes {
		report.ReplayLaneCounts[laneID] = gatewayLaneCountReport{
			LaneID:     lane.LaneID,
			GatewayURL: lane.GatewayURL,
			GatewayID:  lane.GatewayID,
		}
	}
	client := opts.HTTPClient
	if client == nil {
		client = &http.Client{
			Timeout: opts.Timeout,
			Transport: &http.Transport{
				MaxIdleConns:        concurrency * len(gatewayURLs) * 2,
				MaxIdleConnsPerHost: concurrency * 2,
				IdleConnTimeout:     90 * time.Second,
			},
		}
	}

	if opts.Attach {
		attachmentID, generation, err := gatewayAttach(client, gatewayURL, volumeID, opts.HostID, opts.DeviceID)
		if err != nil {
			report.Result = "error"
			report.ErrorCount = 1
			report.FirstError = "attach failed: " + err.Error()
			report.LastError = report.FirstError
			return report, err
		}
		report.AttachmentID = attachmentID
		report.Generation = generation
	}
	if opts.Detach {
		defer func() {
			if report.AttachmentID != "" {
				_, _ = gatewayDetachAll(client, gatewayURLs, volumeID, opts.HostID, report.AttachmentID)
			}
		}()
	}

	latencies := make([]float64, 0, len(trace.Operations))
	slowOperations := make([]gatewayLoadSlowOperation, 0, len(trace.Operations))
	var writeOKBytes uint64
	var readOKBytes uint64
	var totalOKBytes uint64
	var firstErr string
	var lastErr string
	var mu sync.Mutex

	runOne := func(idx int, traceOp gatewayReplayTraceOp) {
		targetURL, laneID, replaySelection := targets.targetForReplayOp(idx, traceOp)
		reqStarted := time.Now()
		err := executeGatewayReplayOperation(client, targetURL, volumeID, payloadPattern, idx, traceOp)
		latencyMS := float64(time.Since(reqStarted).Microseconds()) / 1000.0

		mu.Lock()
		defer mu.Unlock()
		report.GatewayRequestCounts[targetURL]++
		report.ReplaySelectionCounts[replaySelection]++
		if laneID >= 0 && laneID < len(report.ReplayLaneCounts) {
			report.ReplayLaneCounts[laneID].RequestCount++
		}
		latencies = append(latencies, latencyMS)
		sample := gatewayLoadSlowOperation{
			Index:           idx,
			GatewayURL:      targetURL,
			LaneID:          laneID,
			ReplaySelection: replaySelection,
			TraceGatewayID:  traceOp.GatewayID,
			TracePathID:     traceOp.PathID,
			Op:              traceOp.Op,
			OffsetBytes:     traceOp.OffsetBytes,
			LengthBytes:     traceOp.LengthBytes,
			LatencyMS:       latencyMS,
		}
		if err != nil {
			sample.Error = err.Error()
		}
		slowOperations = append(slowOperations, sample)
		recordGatewayReplayOpCount(&report, traceOp.Op, err)
		if err != nil {
			report.ErrorCount++
			report.GatewayErrorCounts[targetURL]++
			if laneID >= 0 && laneID < len(report.ReplayLaneCounts) {
				report.ReplayLaneCounts[laneID].ErrorCount++
			}
			lastErr = err.Error()
			if firstErr == "" {
				firstErr = err.Error()
			}
			return
		}
		report.OKCount++
		report.GatewayOKCounts[targetURL]++
		if laneID >= 0 && laneID < len(report.ReplayLaneCounts) {
			report.ReplayLaneCounts[laneID].OKCount++
		}
		totalOKBytes += traceOp.LengthBytes
		switch traceOp.Op {
		case "write":
			writeOKBytes += traceOp.LengthBytes
		case "read":
			readOKBytes += traceOp.LengthBytes
		}
	}

	started := time.Now()
	switch mode {
	case "paced":
		var wg sync.WaitGroup
		sem := make(chan struct{}, concurrency)
		pacedStart := time.Now()
		var cumulativeDelay time.Duration
		for idx, traceOp := range trace.Operations {
			if traceOp.SubmitDeltaUS > 0 {
				cumulativeDelay += time.Duration(traceOp.SubmitDeltaUS) * time.Microsecond
			}
			if sleepFor := time.Until(pacedStart.Add(cumulativeDelay)); sleepFor > 0 {
				time.Sleep(sleepFor)
			}
			sem <- struct{}{}
			wg.Add(1)
			go func(idx int, traceOp gatewayReplayTraceOp) {
				defer wg.Done()
				defer func() { <-sem }()
				runOne(idx, traceOp)
			}(idx, traceOp)
		}
		wg.Wait()
	default:
		jobs := make(chan int)
		var wg sync.WaitGroup
		for worker := 0; worker < concurrency; worker++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for idx := range jobs {
					runOne(idx, trace.Operations[idx])
				}
			}()
		}
		for idx := range trace.Operations {
			jobs <- idx
		}
		close(jobs)
		wg.Wait()
	}
	elapsed := time.Since(started)
	report.ElapsedMS = elapsed.Milliseconds()
	report.FirstError = firstErr
	report.LastError = lastErr
	if report.ErrorCount > 0 {
		report.Result = "error"
	}
	if elapsed > 0 {
		report.WriteIOPS = float64(report.WriteOKCount) / elapsed.Seconds()
		report.WriteBWKiB = float64(writeOKBytes) / 1024.0 / elapsed.Seconds()
		report.ReadIOPS = float64(report.ReadOKCount) / elapsed.Seconds()
		report.ReadBWKiB = float64(readOKBytes) / 1024.0 / elapsed.Seconds()
		report.TotalIOPS = float64(report.OKCount) / elapsed.Seconds()
		report.TotalBWKiB = float64(totalOKBytes) / 1024.0 / elapsed.Seconds()
	}
	if summary := newGatewayLoadLatencySummary(latencies); summary != nil {
		report.LatencyAvgMS = summary.AvgMS
		report.LatencyMinMS = summary.MinMS
		report.LatencyP50MS = summary.P50MS
		report.LatencyP90MS = summary.P90MS
		report.LatencyP95MS = summary.P95MS
		report.LatencyP99MS = summary.P99MS
		report.LatencyP999MS = summary.P999MS
		report.LatencyMaxMS = summary.MaxMS
	}
	report.SlowOperations = topGatewayLoadSlowOperations(slowOperations, 16)
	if report.ErrorCount > 0 {
		return report, fmt.Errorf("gateway replay-load completed with %d errors", report.ErrorCount)
	}
	if opts.Verify {
		expected := buildGatewayReplayExpectedRanges(trace.Operations)
		report.VerifySkippedCount = countGatewayReplayVerifiableOps(trace.Operations) - len(expected)
		if err := verifyGatewayReplayLoad(client, targets, volumeID, payloadPattern, expected, &report); err != nil {
			report.Result = "error"
			if report.FirstError == "" {
				report.FirstError = report.VerifyFirstError
			}
			if report.LastError == "" {
				report.LastError = report.VerifyLastError
			}
			return report, err
		}
	}
	return report, nil
}

func loadGatewayReplayTrace(path string) (gatewayReplayTrace, error) {
	var reader io.Reader
	var closeFn func() error
	if path == "-" {
		reader = os.Stdin
	} else {
		file, err := os.Open(path)
		if err != nil {
			return gatewayReplayTrace{}, err
		}
		reader = file
		closeFn = file.Close
	}
	if closeFn != nil {
		defer closeFn()
	}
	scanner := bufio.NewScanner(reader)
	scanner.Buffer(make([]byte, 0, 64*1024), 16*1024*1024)
	var trace gatewayReplayTrace
	lineNo := 0
	for scanner.Scan() {
		lineNo++
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		var op gatewayReplayTraceOp
		if err := json.Unmarshal([]byte(line), &op); err != nil {
			return gatewayReplayTrace{}, fmt.Errorf("decode trace line %d: %w", lineNo, err)
		}
		trace.TotalCount++
		op.Op = normalizeGatewayReplayOp(op.Op)
		if op.Seq == 0 {
			op.Seq = lineNo
		}
		if !gatewayReplayOpSupported(op.Op) || (op.Op != "flush" && op.LengthBytes == 0) {
			trace.Skipped++
			continue
		}
		if op.ReplayEligible != nil && !*op.ReplayEligible {
			trace.Skipped++
			continue
		}
		if op.StatusCode != 0 {
			trace.Skipped++
			continue
		}
		trace.Operations = append(trace.Operations, op)
	}
	if err := scanner.Err(); err != nil {
		return gatewayReplayTrace{}, err
	}
	fillGatewayReplaySubmitDeltas(trace.Operations)
	return trace, nil
}

func fillGatewayReplaySubmitDeltas(ops []gatewayReplayTraceOp) {
	var previous time.Time
	for i := range ops {
		ts := strings.TrimSpace(ops[i].TS)
		if ts == "" {
			continue
		}
		current, err := time.Parse(time.RFC3339Nano, ts)
		if err != nil {
			continue
		}
		if ops[i].SubmitDeltaUS <= 0 && !previous.IsZero() {
			delta := current.Sub(previous).Microseconds()
			if delta > 0 {
				ops[i].SubmitDeltaUS = delta
			}
		}
		previous = current
	}
}

func normalizeGatewayReplayOp(op string) string {
	switch strings.ToLower(strings.ReplaceAll(strings.TrimSpace(op), "-", "_")) {
	case "read":
		return "read"
	case "write":
		return "write"
	case "flush":
		return "flush"
	case "discard":
		return "discard"
	case "zero", "zeroes", "write_zero", "write_zeroes":
		return "write_zeroes"
	default:
		return ""
	}
}

func gatewayReplayOpSupported(op string) bool {
	switch op {
	case "read", "write", "flush", "discard", "write_zeroes":
		return true
	default:
		return false
	}
}

func (s gatewayTargetSet) targetForReplayOp(idx int, op gatewayReplayTraceOp) (string, int, string) {
	if s.activeLaneCount <= 1 {
		return s.primaryURL(), 0, "single_lane"
	}
	if op.GatewayID != "" {
		if laneID, ok := s.gatewayIDToLane[strings.ToLower(strings.TrimSpace(op.GatewayID))]; ok && laneID < len(s.lanes) {
			return s.lanes[laneID].GatewayURL, laneID, "gateway_id"
		}
	}
	if op.PathID != nil {
		laneID := int(*op.PathID) % s.activeLaneCount
		return s.lanes[laneID].GatewayURL, laneID, "path_id"
	}
	targetURL, laneID := s.targetForLoadIndex(idx, 1)
	return targetURL, laneID, "synthetic_index"
}

func executeGatewayReplayOperation(client *http.Client, gatewayURL, volumeID, payloadPattern string, idx int, traceOp gatewayReplayTraceOp) error {
	phase := "replay"
	switch traceOp.Op {
	case "read":
		_, err := gatewayReadOperationWithLoadMetadata(client, gatewayURL, volumeID, traceOp.OffsetBytes, traceOp.LengthBytes, idx, phase)
		return err
	case "write":
		dataBase64, ok := gatewayCanonicalZeroBase64ForLength(traceOp.LengthBytes)
		if !ok {
			return fmt.Errorf("zero replay payload length=%d is too large to encode", traceOp.LengthBytes)
		}
		if payloadPattern == "deterministic" {
			dataBase64 = base64.StdEncoding.EncodeToString(gatewayDeterministicPayload(volumeID, traceOp.OffsetBytes, traceOp.LengthBytes))
		}
		_, err := gatewayWriteWithLoadMetadata(client, gatewayURL, volumeID, traceOp.OffsetBytes, traceOp.LengthBytes, dataBase64, idx, phase)
		return err
	case "write_zeroes":
		_, err := gatewayZeroWithLoadMetadata(client, gatewayURL, volumeID, traceOp.OffsetBytes, traceOp.LengthBytes, idx, phase)
		return err
	case "discard":
		_, err := gatewayDiscardWithLoadMetadata(client, gatewayURL, volumeID, traceOp.OffsetBytes, traceOp.LengthBytes, idx, phase)
		return err
	case "flush":
		_, err := gatewayFlushWithLoadMetadata(client, gatewayURL, volumeID, idx, phase)
		return err
	default:
		return fmt.Errorf("unsupported replay op %q", traceOp.Op)
	}
}

func recordGatewayReplayOpCount(report *gatewayReplayLoadReport, op string, err error) {
	ok := err == nil
	switch op {
	case "read":
		report.ReadCount++
		if ok {
			report.ReadOKCount++
		} else {
			report.ReadErrorCount++
		}
	case "write":
		report.WriteCount++
		if ok {
			report.WriteOKCount++
		} else {
			report.WriteErrorCount++
		}
	case "write_zeroes":
		report.ZeroCount++
		if ok {
			report.ZeroOKCount++
		} else {
			report.ZeroErrorCount++
		}
	case "discard":
		report.DiscardCount++
		if ok {
			report.DiscardOKCount++
		} else {
			report.DiscardErrorCount++
		}
	case "flush":
		report.FlushCount++
		if ok {
			report.FlushOKCount++
		} else {
			report.FlushErrorCount++
		}
	}
}

func buildGatewayReplayExpectedRanges(ops []gatewayReplayTraceOp) []gatewayReplayExpectedRange {
	type key struct {
		offset uint64
		length uint64
	}
	order := make([]key, 0)
	expected := make(map[key]gatewayReplayExpectedRange)
	for _, op := range ops {
		if op.LengthBytes == 0 {
			continue
		}
		kind := ""
		switch op.Op {
		case "write":
			kind = "write"
		case "write_zeroes":
			kind = "zero"
		default:
			continue
		}
		k := key{offset: op.OffsetBytes, length: op.LengthBytes}
		if _, ok := expected[k]; !ok {
			order = append(order, k)
		}
		expected[k] = gatewayReplayExpectedRange{
			OffsetBytes: op.OffsetBytes,
			LengthBytes: op.LengthBytes,
			Kind:        kind,
		}
	}
	out := make([]gatewayReplayExpectedRange, 0, len(expected))
	for _, k := range order {
		out = append(out, expected[k])
	}
	return out
}

func countGatewayReplayVerifiableOps(ops []gatewayReplayTraceOp) int {
	count := 0
	for _, op := range ops {
		if op.Op == "write" || op.Op == "write_zeroes" {
			count++
		}
	}
	return count
}

func verifyGatewayReplayLoad(client *http.Client, targets gatewayTargetSet, volumeID, payloadPattern string, expected []gatewayReplayExpectedRange, report *gatewayReplayLoadReport) error {
	var firstErr string
	var lastErr string
	for idx, item := range expected {
		gatewayURL, _ := targets.targetForBlock(idx)
		want := make([]byte, item.LengthBytes)
		if item.Kind == "write" && payloadPattern == "deterministic" {
			want = gatewayDeterministicPayload(volumeID, item.OffsetBytes, item.LengthBytes)
		}
		got, err := gatewayRead(client, gatewayURL, volumeID, item.OffsetBytes, item.LengthBytes)
		report.VerifyCount++
		if err != nil {
			report.VerifyErrorCount++
			lastErr = err.Error()
			if firstErr == "" {
				firstErr = err.Error()
			}
			continue
		}
		if !bytes.Equal(got, want) {
			report.VerifyErrorCount++
			lastErr = fmt.Sprintf("payload mismatch at offset=%d length=%d actual_all_zero=%t expected_sha256=%s actual_sha256=%s",
				item.OffsetBytes, item.LengthBytes, allZero(got), shortSHA256(want), shortSHA256(got))
			if firstErr == "" {
				firstErr = lastErr
			}
			continue
		}
		report.VerifyOKCount++
	}
	report.VerifyFirstError = firstErr
	report.VerifyLastError = lastErr
	if report.VerifyErrorCount > 0 {
		return fmt.Errorf("gateway replay-load verification completed with %d errors", report.VerifyErrorCount)
	}
	return nil
}

func topGatewayLoadSlowOperations(samples []gatewayLoadSlowOperation, limit int) []gatewayLoadSlowOperation {
	if limit <= 0 || len(samples) == 0 {
		return nil
	}
	sort.Slice(samples, func(i, j int) bool {
		if samples[i].LatencyMS == samples[j].LatencyMS {
			return samples[i].Index < samples[j].Index
		}
		return samples[i].LatencyMS > samples[j].LatencyMS
	})
	if len(samples) > limit {
		samples = samples[:limit]
	}
	out := make([]gatewayLoadSlowOperation, len(samples))
	copy(out, samples)
	return out
}

func runGatewayLoadWarmupRequests(client *http.Client, targets gatewayTargetSet, volumeID string, blocksPerJob int, blockBytes uint64, rw string, rwmixRead int, payloadPattern string, zeroPayloadBase64 string, concurrency int, requestCount int) (int, int, []float64, error) {
	jobs := make(chan int)
	latencies := make([]float64, 0, requestCount)
	okCount := 0
	errorCount := 0
	var firstErr string
	var mu sync.Mutex
	var wg sync.WaitGroup
	for worker := 0; worker < concurrency; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for idx := range jobs {
				jobOffset := gatewayWorkloadOffset(idx, blocksPerJob, blockBytes, rw)
				op := gatewayWorkloadOperation(idx, rw, rwmixRead)
				block := int(jobOffset / blockBytes)
				gatewayURL, _ := targets.targetForBlock(block)
				reqStarted := time.Now()
				var err error
				switch op {
				case "read":
					_, err = gatewayReadOperationWithLoadMetadata(client, gatewayURL, volumeID, jobOffset, blockBytes, idx, "warmup")
				default:
					dataBase64 := zeroPayloadBase64
					if payloadPattern == "deterministic" {
						dataBase64 = base64.StdEncoding.EncodeToString(gatewayDeterministicPayload(volumeID, jobOffset, blockBytes))
					}
					_, err = gatewayWriteWithLoadMetadata(client, gatewayURL, volumeID, jobOffset, blockBytes, dataBase64, idx, "warmup")
				}
				latencyMS := float64(time.Since(reqStarted).Microseconds()) / 1000.0
				mu.Lock()
				latencies = append(latencies, latencyMS)
				if err != nil {
					errorCount++
					if firstErr == "" {
						firstErr = err.Error()
					}
				} else {
					okCount++
				}
				mu.Unlock()
			}
		}()
	}
	for idx := 0; idx < requestCount; idx++ {
		jobs <- idx
	}
	close(jobs)
	wg.Wait()
	if errorCount > 0 {
		return okCount, errorCount, latencies, fmt.Errorf("gateway write-load warmup completed with %d errors: %s", errorCount, firstErr)
	}
	return okCount, errorCount, latencies, nil
}

func gatewayLoadPhase(idx, steadyStateSkip int) string {
	if idx < steadyStateSkip {
		return "cold_start"
	}
	return "steady_state"
}

func gatewayWorkloadNeedsPrefill(rw string) bool {
	switch rw {
	case "read", "randread", "randrw":
		return true
	default:
		return false
	}
}

func gatewayWorkloadOperation(idx int, rw string, rwmixRead int) string {
	switch rw {
	case "read", "randread":
		return "read"
	case "randrw":
		if rwmixRead <= 0 {
			return "write"
		}
		if rwmixRead >= 100 {
			return "read"
		}
		if gatewayDeterministicPercent(idx) < rwmixRead {
			return "read"
		}
		return "write"
	default:
		return "write"
	}
}

func gatewayWorkloadOffset(idx, blocksPerJob int, blockBytes uint64, rw string) uint64 {
	block := idx % blocksPerJob
	switch rw {
	case "randwrite", "randread", "randrw":
		block = gatewayDeterministicBlock(idx, blocksPerJob)
	}
	return uint64(block) * blockBytes
}

func gatewayDeterministicBlock(idx, blocksPerJob int) int {
	if blocksPerJob <= 1 {
		return 0
	}
	x := uint64(idx + 1)
	x ^= x >> 33
	x *= 0xff51afd7ed558ccd
	x ^= x >> 33
	x *= 0xc4ceb9fe1a85ec53
	x ^= x >> 33
	return int(x % uint64(blocksPerJob))
}

func gatewayDeterministicPercent(idx int) int {
	return gatewayDeterministicBlock(idx, 100)
}

func gatewayPrefillWriteLoad(client *http.Client, targets gatewayTargetSet, volumeID string, blocksPerJob int, blockBytes uint64, payloadPattern string) (int, int, error) {
	zeroPayloadBase64 := base64.StdEncoding.EncodeToString(make([]byte, blockBytes))
	var firstErr error
	var prefillCount, errorCount int
	for block := 0; block < blocksPerJob; block++ {
		offset := uint64(block) * blockBytes
		gatewayURL, _ := targets.targetForBlock(block)
		dataBase64 := zeroPayloadBase64
		if payloadPattern == "deterministic" {
			dataBase64 = base64.StdEncoding.EncodeToString(gatewayDeterministicPayload(volumeID, offset, blockBytes))
		}
		prefillCount++
		if err := gatewayWrite(client, gatewayURL, volumeID, offset, blockBytes, dataBase64); err != nil {
			errorCount++
			if firstErr == nil {
				firstErr = err
			}
		}
	}
	if firstErr != nil {
		return prefillCount, errorCount, fmt.Errorf("gateway prefill failed with %d errors: %w", errorCount, firstErr)
	}
	return prefillCount, 0, nil
}

func filePrefillLoad(file *os.File, seedID string, blocksPerJob int, blockBytes uint64, payloadPattern string) (int, int, error) {
	zeroPayload := make([]byte, blockBytes)
	var firstErr error
	var prefillCount, errorCount int
	for block := 0; block < blocksPerJob; block++ {
		offset := uint64(block) * blockBytes
		payload := zeroPayload
		if payloadPattern == "deterministic" {
			payload = gatewayDeterministicPayload(seedID, offset, blockBytes)
		}
		prefillCount++
		if _, err := file.WriteAt(payload, int64(offset)); err != nil {
			errorCount++
			if firstErr == nil {
				firstErr = err
			}
		}
	}
	if firstErr != nil {
		return prefillCount, errorCount, fmt.Errorf("file prefill failed with %d errors: %w", errorCount, firstErr)
	}
	return prefillCount, 0, nil
}

func fileReadAt(file *os.File, buf []byte, offset uint64) error {
	n, err := file.ReadAt(buf, int64(offset))
	if err == io.EOF && n == len(buf) {
		return nil
	}
	if err != nil {
		return err
	}
	if n != len(buf) {
		return fmt.Errorf("short read at offset=%d got=%d want=%d", offset, n, len(buf))
	}
	return nil
}

func verifyFileWriteLoad(file *os.File, seedID string, blocksPerJob int, blockBytes uint64, payloadPattern string, report *fileLoadReport) error {
	got := make([]byte, blockBytes)
	var firstErr string
	for block := 0; block < blocksPerJob; block++ {
		offset := uint64(block) * blockBytes
		want := make([]byte, blockBytes)
		if payloadPattern == "deterministic" {
			want = gatewayDeterministicPayload(seedID, offset, blockBytes)
		}
		report.VerifyCount++
		if err := fileReadAt(file, got, offset); err != nil {
			report.VerifyErrorCount++
			if firstErr == "" {
				firstErr = err.Error()
			}
			continue
		}
		if !bytes.Equal(got, want) {
			report.VerifyErrorCount++
			if firstErr == "" {
				firstErr = fmt.Sprintf("payload mismatch at offset=%d length=%d actual_all_zero=%t expected_sha256=%s actual_sha256=%s",
					offset, blockBytes, allZero(got), shortSHA256(want), shortSHA256(got))
			}
			continue
		}
		report.VerifyOKCount++
	}
	report.VerifyFirstError = firstErr
	if report.VerifyErrorCount > 0 {
		return fmt.Errorf("file load verification completed with %d errors", report.VerifyErrorCount)
	}
	return nil
}

func verifyGatewayWriteLoad(client *http.Client, targets gatewayTargetSet, volumeID, verifyVolumeID string, blocksPerJob int, blockBytes uint64, payloadPattern string, report *gatewayWriteLoadReport) error {
	var firstErr string
	var lastErr string
	for block := 0; block < blocksPerJob; block++ {
		offset := uint64(block) * blockBytes
		gatewayURL, _ := targets.targetForBlock(block)
		want := make([]byte, blockBytes)
		if payloadPattern == "deterministic" {
			want = gatewayDeterministicPayload(verifyVolumeID, offset, blockBytes)
		}
		got, err := gatewayRead(client, gatewayURL, volumeID, offset, blockBytes)
		report.VerifyCount++
		if err != nil {
			report.VerifyErrorCount++
			lastErr = err.Error()
			if firstErr == "" {
				firstErr = err.Error()
			}
			continue
		}
		if !bytes.Equal(got, want) {
			report.VerifyErrorCount++
			lastErr = fmt.Sprintf("payload mismatch at offset=%d length=%d actual_all_zero=%t expected_sha256=%s actual_sha256=%s",
				offset, blockBytes, allZero(got), shortSHA256(want), shortSHA256(got))
			if firstErr == "" {
				firstErr = lastErr
			}
			continue
		}
		report.VerifyOKCount++
	}
	report.VerifyFirstError = firstErr
	report.VerifyLastError = lastErr
	if report.VerifyErrorCount > 0 {
		return fmt.Errorf("gateway write-load verification completed with %d errors", report.VerifyErrorCount)
	}
	return nil
}

func parseGatewayIntegrityScenarios(raw string) ([]string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, nil
	}
	valid := map[string]struct{}{
		"sequential-full":    {},
		"overwrite":          {},
		"zero-hole":          {},
		"unaligned-boundary": {},
		"read-after-write":   {},
	}
	seen := make(map[string]struct{})
	out := make([]string, 0)
	for _, part := range strings.Split(raw, ",") {
		name := strings.TrimSpace(part)
		if name == "" {
			continue
		}
		if _, ok := valid[name]; !ok {
			return nil, fmt.Errorf("unknown integrity scenario %q", name)
		}
		if _, ok := seen[name]; ok {
			continue
		}
		seen[name] = struct{}{}
		out = append(out, name)
	}
	return out, nil
}

func runGatewayIntegrityScenarios(client *http.Client, gatewayURL, volumeID string, sizeBytes, blockBytes uint64, scenarios []string, report *gatewayWriteLoadReport) error {
	expected := make([]byte, sizeBytes)
	var firstErr string
	for _, name := range scenarios {
		scenario := gatewayIntegrityScenarioReport{Name: name, Result: "ok"}
		var err error
		switch name {
		case "sequential-full":
			err = runGatewaySequentialFullScenario(client, gatewayURL, volumeID, blockBytes, expected, &scenario)
		case "overwrite":
			err = runGatewayOverwriteScenario(client, gatewayURL, volumeID, blockBytes, expected, &scenario)
		case "zero-hole":
			err = runGatewayZeroHoleScenario(client, gatewayURL, volumeID, blockBytes, expected, &scenario)
		case "unaligned-boundary":
			err = runGatewayUnalignedBoundaryScenario(client, gatewayURL, volumeID, blockBytes, expected, &scenario)
		case "read-after-write":
			err = runGatewayReadAfterWriteScenario(client, gatewayURL, volumeID, blockBytes, expected, &scenario)
		default:
			err = fmt.Errorf("unknown integrity scenario %q", name)
		}
		report.ScenarioCount++
		report.OKCount += scenario.WriteCount
		report.RequestCount += scenario.WriteCount + scenario.ZeroCount
		report.VerifyCount += scenario.VerifyCount
		report.VerifyOKCount += scenario.VerifyOKCount
		report.VerifyErrorCount += scenario.VerifyErrorCount
		if err != nil {
			scenario.Result = "error"
			report.ScenarioErrorCount++
			report.ErrorCount++
			if scenario.FirstError == "" {
				scenario.FirstError = err.Error()
			}
			if firstErr == "" {
				firstErr = fmt.Sprintf("%s: %s", name, scenario.FirstError)
			}
			report.Scenarios = append(report.Scenarios, scenario)
			break
		} else {
			report.ScenarioOKCount++
		}
		report.Scenarios = append(report.Scenarios, scenario)
	}
	report.ScenarioFirstError = firstErr
	report.VerifyFirstError = firstErr
	if firstErr != "" {
		return fmt.Errorf("gateway integrity scenarios failed: %s", firstErr)
	}
	return nil
}

func runGatewaySequentialFullScenario(client *http.Client, gatewayURL, volumeID string, blockBytes uint64, expected []byte, report *gatewayIntegrityScenarioReport) error {
	for offset := uint64(0); offset < uint64(len(expected)); offset += blockBytes {
		length := minUint64(blockBytes, uint64(len(expected))-offset)
		payload := gatewayDeterministicPayloadForSeed(volumeID, offset, length, "sequential-full")
		if err := gatewayWriteBytes(client, gatewayURL, volumeID, offset, payload); err != nil {
			return gatewayScenarioError(report, "write", offset, length, err)
		}
		report.WriteCount++
		copy(expected[offset:offset+length], payload)
	}
	return verifyGatewayExpectedRange(client, gatewayURL, volumeID, expected, 0, uint64(len(expected)), blockBytes, report)
}

func runGatewayOverwriteScenario(client *http.Client, gatewayURL, volumeID string, blockBytes uint64, expected []byte, report *gatewayIntegrityScenarioReport) error {
	stride := blockBytes * 3
	if stride == 0 {
		stride = blockBytes
	}
	for offset := blockBytes; offset < uint64(len(expected)); offset += stride {
		length := minUint64(blockBytes, uint64(len(expected))-offset)
		payload := gatewayDeterministicPayloadForSeed(volumeID, offset, length, "overwrite")
		if err := gatewayWriteBytes(client, gatewayURL, volumeID, offset, payload); err != nil {
			return gatewayScenarioError(report, "overwrite", offset, length, err)
		}
		report.WriteCount++
		copy(expected[offset:offset+length], payload)
	}
	return verifyGatewayExpectedRange(client, gatewayURL, volumeID, expected, 0, uint64(len(expected)), blockBytes, report)
}

func runGatewayZeroHoleScenario(client *http.Client, gatewayURL, volumeID string, blockBytes uint64, expected []byte, report *gatewayIntegrityScenarioReport) error {
	ranges := [][2]uint64{
		{blockBytes, blockBytes},
		{blockBytes*3 + blockBytes/2, blockBytes / 2},
	}
	for _, rng := range ranges {
		offset, length := clampGatewayRange(rng[0], rng[1], uint64(len(expected)))
		if length == 0 {
			continue
		}
		if err := gatewayZero(client, gatewayURL, volumeID, offset, length); err != nil {
			return gatewayScenarioError(report, "zero", offset, length, err)
		}
		report.ZeroCount++
		clear(expected[offset : offset+length])
	}
	return verifyGatewayExpectedRange(client, gatewayURL, volumeID, expected, 0, uint64(len(expected)), blockBytes, report)
}

func runGatewayUnalignedBoundaryScenario(client *http.Client, gatewayURL, volumeID string, blockBytes uint64, expected []byte, report *gatewayIntegrityScenarioReport) error {
	candidates := [][2]uint64{
		{4 * 1024, 8 * 1024},
		{64*1024 - 4*1024, 16 * 1024},
		{blockBytes - 4*1024, 12 * 1024},
		{2*blockBytes - 4*1024, 12 * 1024},
	}
	for i, candidate := range candidates {
		offset, length := clampGatewayRange(candidate[0], candidate[1], uint64(len(expected)))
		if length == 0 {
			continue
		}
		payload := gatewayDeterministicPayloadForSeed(volumeID, offset, length, fmt.Sprintf("unaligned-boundary-%d", i))
		if err := gatewayWriteBytes(client, gatewayURL, volumeID, offset, payload); err != nil {
			return gatewayScenarioError(report, "unaligned write", offset, length, err)
		}
		report.WriteCount++
		report.Operations = append(report.Operations, gatewayIntegrityScenarioOpTrace{
			Op:          "write",
			OffsetBytes: offset,
			LengthBytes: length,
			Seed:        fmt.Sprintf("unaligned-boundary-%d", i),
			SHA256:      shortSHA256(payload),
		})
		copy(expected[offset:offset+length], payload)
	}
	return verifyGatewayExpectedRange(client, gatewayURL, volumeID, expected, 0, uint64(len(expected)), blockBytes, report)
}

func runGatewayReadAfterWriteScenario(client *http.Client, gatewayURL, volumeID string, blockBytes uint64, expected []byte, report *gatewayIntegrityScenarioReport) error {
	limit := minUint64(16, (uint64(len(expected))+blockBytes-1)/blockBytes)
	for block := uint64(0); block < limit; block++ {
		offset := block * blockBytes
		length := minUint64(blockBytes, uint64(len(expected))-offset)
		payload := gatewayDeterministicPayloadForSeed(volumeID, offset, length, "read-after-write")
		if err := gatewayWriteBytes(client, gatewayURL, volumeID, offset, payload); err != nil {
			return gatewayScenarioError(report, "read-after-write write", offset, length, err)
		}
		report.WriteCount++
		copy(expected[offset:offset+length], payload)
		if err := verifyGatewayExpectedBytes(client, gatewayURL, volumeID, expected, offset, length, report); err != nil {
			return err
		}
	}
	return verifyGatewayExpectedRange(client, gatewayURL, volumeID, expected, 0, uint64(len(expected)), blockBytes, report)
}

func verifyGatewayExpectedRange(client *http.Client, gatewayURL, volumeID string, expected []byte, offset, length, readSize uint64, report *gatewayIntegrityScenarioReport) error {
	end := minUint64(offset+length, uint64(len(expected)))
	for cursor := offset; cursor < end; cursor += readSize {
		n := minUint64(readSize, end-cursor)
		if err := verifyGatewayExpectedBytes(client, gatewayURL, volumeID, expected, cursor, n, report); err != nil {
			return err
		}
	}
	return nil
}

func verifyGatewayExpectedBytes(client *http.Client, gatewayURL, volumeID string, expected []byte, offset, length uint64, report *gatewayIntegrityScenarioReport) error {
	got, err := gatewayRead(client, gatewayURL, volumeID, offset, length)
	report.ReadCount++
	report.VerifyCount++
	if err != nil {
		return gatewayScenarioError(report, "read", offset, length, err)
	}
	want := expected[offset : offset+length]
	if !bytes.Equal(got, want) {
		report.VerifyErrorCount++
		mismatch := gatewayBuildPayloadMismatch(offset, want, got)
		report.Mismatch = mismatch
		if report.FirstError == "" {
			report.FirstError = fmt.Sprintf("payload mismatch at offset=%d length=%d actual_all_zero=%t expected_sha256=%s actual_sha256=%s",
				offset, length, mismatch.ActualAllZero, mismatch.ExpectedSHA256, mismatch.ActualSHA256)
		}
		return fmt.Errorf("%s", report.FirstError)
	}
	report.VerifyOKCount++
	return nil
}

func gatewayScenarioError(report *gatewayIntegrityScenarioReport, op string, offset, length uint64, err error) error {
	report.VerifyErrorCount++
	if report.FirstError == "" {
		report.FirstError = fmt.Sprintf("%s failed at offset=%d length=%d: %v", op, offset, length, err)
	}
	return err
}

func clampGatewayRange(offset, length, size uint64) (uint64, uint64) {
	if offset >= size || length == 0 {
		return offset, 0
	}
	return offset, minUint64(length, size-offset)
}

func allZero(data []byte) bool {
	for _, b := range data {
		if b != 0 {
			return false
		}
	}
	return true
}

var gatewayMaxIntUint64 = uint64(^uint(0) >> 1)

var gatewayZeroBase64ByDecodedLength sync.Map

func gatewayCanonicalZeroBase64ForLength(decodedLen uint64) (string, bool) {
	if decodedLen > gatewayMaxIntUint64 {
		return "", false
	}
	// base64.EncodedLen returns int, so keep enough headroom to avoid overflow.
	if decodedLen > (gatewayMaxIntUint64/4)*3 {
		return "", false
	}
	if encoded, ok := gatewayZeroBase64ByDecodedLength.Load(decodedLen); ok {
		return encoded.(string), true
	}
	encodedLen := base64.StdEncoding.EncodedLen(int(decodedLen))
	padding := 0
	switch decodedLen % 3 {
	case 1:
		padding = 2
	case 2:
		padding = 1
	}
	encoded := strings.Repeat("A", encodedLen-padding) + strings.Repeat("=", padding)
	actual, _ := gatewayZeroBase64ByDecodedLength.LoadOrStore(decodedLen, encoded)
	return actual.(string), true
}

func gatewayBuildPayloadMismatch(offset uint64, want, got []byte) *gatewayPayloadMismatchReport {
	first := 0
	maxLen := minInt(len(want), len(got))
	for first < maxLen && want[first] == got[first] {
		first++
	}
	if first == maxLen && len(want) != len(got) {
		first = maxLen
	}

	windowStart := first - minInt(first, 16)
	windowEnd := minInt(maxLen, first+16)
	if windowEnd < windowStart {
		windowEnd = windowStart
	}

	report := &gatewayPayloadMismatchReport{
		OffsetBytes:              offset,
		LengthBytes:              uint64(len(want)),
		FirstMismatchOffsetBytes: offset + uint64(first),
		FirstMismatchInReadBytes: uint64(first),
		ExpectedSHA256:           shortSHA256(want),
		ActualSHA256:             shortSHA256(got),
		ActualAllZero:            allZero(got),
		WindowOffsetBytes:        offset + uint64(windowStart),
		WindowLengthBytes:        uint64(windowEnd - windowStart),
		ExpectedWindowHex:        hex.EncodeToString(want[windowStart:windowEnd]),
		ActualWindowHex:          hex.EncodeToString(got[windowStart:windowEnd]),
		SegmentSizeBytes:         4 * 1024,
	}
	if first < len(want) {
		report.FirstMismatchExpectedByte = want[first]
	}
	if first < len(got) {
		report.FirstMismatchActualByte = got[first]
	}

	segmentSize := int(report.SegmentSizeBytes)
	for segmentStart := 0; segmentStart < maxLen; segmentStart += segmentSize {
		segmentEnd := minInt(maxLen, segmentStart+segmentSize)
		wantSegment := want[segmentStart:segmentEnd]
		gotSegment := got[segmentStart:segmentEnd]
		if bytes.Equal(wantSegment, gotSegment) {
			continue
		}
		report.DifferingSegments = append(report.DifferingSegments, gatewayPayloadSegmentMismatch{
			Index:          segmentStart / segmentSize,
			OffsetBytes:    offset + uint64(segmentStart),
			LengthBytes:    uint64(segmentEnd - segmentStart),
			ExpectedSHA256: shortSHA256(wantSegment),
			ActualSHA256:   shortSHA256(gotSegment),
			ActualAllZero:  allZero(gotSegment),
		})
		if len(report.DifferingSegments) >= 16 {
			break
		}
	}
	return report
}

func shortSHA256(data []byte) string {
	sum := sha256.Sum256(data)
	return fmt.Sprintf("%x", sum[:8])
}

func gatewayDeterministicPayload(volumeID string, offsetBytes, lengthBytes uint64) []byte {
	return gatewayDeterministicPayloadForSeed(volumeID, offsetBytes, lengthBytes, "default")
}

func gatewayDeterministicPayloadForSeed(volumeID string, offsetBytes, lengthBytes uint64, seed string) []byte {
	payload := make([]byte, lengthBytes)
	var cursor uint64
	var counter uint64
	for cursor < lengthBytes {
		sum := sha256.Sum256([]byte(fmt.Sprintf("namrbd-debug-gateway-write-load|%s|%s|%d|%d|%d", seed, volumeID, offsetBytes, lengthBytes, counter)))
		n := copy(payload[cursor:], sum[:])
		cursor += uint64(n)
		counter++
	}
	return payload
}

func runGatewayCloneIOCheck(client *http.Client, gatewayURL, volumeID, cloneID, hostID string, deviceID uint32, offsetBytes, lengthBytes uint64, attach, detach bool) (gatewayCloneIOCheckReport, error) {
	report := gatewayCloneIOCheckReport{
		Result:         "failed",
		GatewayURL:     gatewayURL,
		VolumeID:       volumeID,
		CloneID:        cloneID,
		OffsetBytes:    offsetBytes,
		LengthBytes:    lengthBytes,
		ExpectedSHA256: shortSHA256(gatewayDeterministicPayloadForSeed(volumeID, offsetBytes, lengthBytes, "clone-io-check")),
	}
	if attach {
		attachmentID, generation, err := gatewayAttach(client, gatewayURL, volumeID, hostID, deviceID)
		if err != nil {
			report.Error = fmt.Sprintf("attach: %v", err)
			return report, err
		}
		report.Attached = true
		report.AttachmentID = attachmentID
		report.Generation = generation
	}
	payload := gatewayDeterministicPayloadForSeed(volumeID, offsetBytes, lengthBytes, "clone-io-check")
	if err := gatewayCloneWriteBytes(client, gatewayURL, volumeID, cloneID, offsetBytes, payload); err != nil {
		report.Error = fmt.Sprintf("clone write: %v", err)
		return report, err
	}
	got, err := gatewayCloneRead(client, gatewayURL, volumeID, cloneID, offsetBytes, lengthBytes)
	if err != nil {
		report.Error = fmt.Sprintf("clone read: %v", err)
		return report, err
	}
	report.ActualSHA256 = shortSHA256(got)
	if !bytes.Equal(payload, got) {
		report.Mismatch = gatewayBuildPayloadMismatch(offsetBytes, payload, got)
		err := fmt.Errorf("clone payload mismatch offset=%d length=%d", offsetBytes, lengthBytes)
		report.Error = err.Error()
		return report, err
	}
	if detach {
		if err := gatewayDetach(client, gatewayURL, volumeID, hostID, report.AttachmentID); err != nil {
			report.Error = fmt.Sprintf("detach: %v", err)
			return report, err
		}
		report.Detached = true
	}
	report.Result = "ok"
	report.Error = ""
	return report, nil
}

func gatewayAttach(client *http.Client, gatewayURL, volumeID, hostID string, deviceID uint32) (string, uint64, error) {
	body := fmt.Sprintf(`{"host_id":%q,"device_id":%d}`, hostID, deviceID)
	respBody, err := gatewayPost(client, gatewayURL+"/api/v1/volumes/"+volumeID+"/attach", []byte(body))
	if err != nil {
		return "", 0, err
	}
	var resp struct {
		AttachmentID string `json:"attachment_id"`
		Generation   uint64 `json:"generation"`
	}
	if err := json.Unmarshal(respBody, &resp); err != nil {
		return "", 0, fmt.Errorf("decode attach response: %w", err)
	}
	if resp.AttachmentID == "" {
		return "", 0, fmt.Errorf("attach response did not include attachment_id")
	}
	return resp.AttachmentID, resp.Generation, nil
}

func gatewayDetach(client *http.Client, gatewayURL, volumeID, hostID, attachmentID string) error {
	body := fmt.Sprintf(`{"host_id":%q,"attachment_id":%q}`, hostID, attachmentID)
	_, err := gatewayPost(client, gatewayURL+"/api/v1/volumes/"+volumeID+"/detach", []byte(body))
	return err
}

type gatewayDetachSummary struct {
	AttemptCount         int
	OKCount              int
	IgnoredConflictCount int
	IgnoredConflicts     []string
	Errors               []string
}

func (s gatewayDetachSummary) ignoredConflictWarning() string {
	if len(s.IgnoredConflicts) == 0 {
		return ""
	}
	return strings.Join(s.IgnoredConflicts, "; ")
}

func recordGatewayDetachSummary(report *gatewayWriteLoadReport, summary gatewayDetachSummary) {
	report.DetachAttemptCount = summary.AttemptCount
	report.DetachOKCount = summary.OKCount
	report.DetachConflictCount = summary.IgnoredConflictCount
	report.DetachWarning = summary.ignoredConflictWarning()
}

func isGatewayDetachConflict(err error) bool {
	var statusErr *gatewayHTTPStatusError
	return errors.As(err, &statusErr) &&
		statusErr.StatusCode == http.StatusConflict &&
		strings.Contains(statusErr.Body, service.ErrDetachConflict.Error())
}

func gatewayDetachAll(client *http.Client, gatewayURLs []string, volumeID, hostID, attachmentID string) (gatewayDetachSummary, error) {
	var wg sync.WaitGroup
	type result struct {
		gatewayURL string
		err        error
	}
	resultCh := make(chan result, len(gatewayURLs))
	for _, gatewayURL := range gatewayURLs {
		gatewayURL := gatewayURL
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := gatewayDetach(client, gatewayURL, volumeID, hostID, attachmentID); err != nil {
				resultCh <- result{gatewayURL: gatewayURL, err: err}
				return
			}
			resultCh <- result{gatewayURL: gatewayURL}
		}()
	}
	wg.Wait()
	close(resultCh)

	var summary gatewayDetachSummary
	for res := range resultCh {
		summary.AttemptCount++
		if res.err == nil {
			summary.OKCount++
			continue
		}
		formatted := fmt.Sprintf("%s detach: %s", res.gatewayURL, res.err)
		if isGatewayDetachConflict(res.err) {
			summary.IgnoredConflictCount++
			summary.IgnoredConflicts = append(summary.IgnoredConflicts, formatted)
			continue
		}
		summary.Errors = append(summary.Errors, formatted)
	}
	sort.Strings(summary.IgnoredConflicts)
	sort.Strings(summary.Errors)
	if len(summary.Errors) > 0 {
		errs := make([]error, 0, len(summary.Errors))
		for _, msg := range summary.Errors {
			errs = append(errs, errors.New(msg))
		}
		return summary, errors.Join(errs...)
	}
	if summary.OKCount == 0 && summary.IgnoredConflictCount > 0 {
		return summary, fmt.Errorf("detach conflicts without successful cleanup: %s", summary.ignoredConflictWarning())
	}
	return summary, nil
}

func gatewayWrite(client *http.Client, gatewayURL, volumeID string, offsetBytes, lengthBytes uint64, dataBase64 string) error {
	body := fmt.Sprintf(`{"offset_bytes":%d,"length_bytes":%d,"data_base64":%q}`, offsetBytes, lengthBytes, dataBase64)
	_, err := decodeGatewayOperationResponse(gatewayPost(client, gatewayURL+"/api/v1/volumes/"+volumeID+"/write", []byte(body)))
	return err
}

func gatewayWriteWithLoadMetadata(client *http.Client, gatewayURL, volumeID string, offsetBytes, lengthBytes uint64, dataBase64 string, index int, phase string) (gatewayOperationResponse, error) {
	body := fmt.Sprintf(`{"offset_bytes":%d,"length_bytes":%d,"data_base64":%q}`, offsetBytes, lengthBytes, dataBase64)
	return decodeGatewayOperationResponse(gatewayPostWithHeaders(client, gatewayURL+"/api/v1/volumes/"+volumeID+"/write", []byte(body), gatewayLoadHeaders(index, phase)))
}

func gatewayWriteBytes(client *http.Client, gatewayURL, volumeID string, offsetBytes uint64, data []byte) error {
	return gatewayWrite(client, gatewayURL, volumeID, offsetBytes, uint64(len(data)), base64.StdEncoding.EncodeToString(data))
}

func gatewayCloneWriteBytes(client *http.Client, gatewayURL, volumeID, cloneID string, offsetBytes uint64, data []byte) error {
	body := fmt.Sprintf(`{"offset_bytes":%d,"length_bytes":%d,"data_base64":%q}`, offsetBytes, len(data), base64.StdEncoding.EncodeToString(data))
	_, err := gatewayPost(client, gatewayURL+"/api/v1/debug/sbs-cluster/volumes/"+volumeID+"/clones/"+cloneID+"/write", []byte(body))
	return err
}

func gatewayZero(client *http.Client, gatewayURL, volumeID string, offsetBytes, lengthBytes uint64) error {
	body := fmt.Sprintf(`{"offset_bytes":%d,"length_bytes":%d}`, offsetBytes, lengthBytes)
	_, err := gatewayPost(client, gatewayURL+"/api/v1/volumes/"+volumeID+"/zero", []byte(body))
	return err
}

func gatewayZeroWithLoadMetadata(client *http.Client, gatewayURL, volumeID string, offsetBytes, lengthBytes uint64, index int, phase string) (gatewayOperationResponse, error) {
	body := fmt.Sprintf(`{"offset_bytes":%d,"length_bytes":%d}`, offsetBytes, lengthBytes)
	return decodeGatewayOperationResponse(gatewayPostWithHeaders(client, gatewayURL+"/api/v1/volumes/"+volumeID+"/zero", []byte(body), gatewayLoadHeaders(index, phase)))
}

func gatewayDiscardWithLoadMetadata(client *http.Client, gatewayURL, volumeID string, offsetBytes, lengthBytes uint64, index int, phase string) (gatewayOperationResponse, error) {
	body := fmt.Sprintf(`{"offset_bytes":%d,"length_bytes":%d}`, offsetBytes, lengthBytes)
	return decodeGatewayOperationResponse(gatewayPostWithHeaders(client, gatewayURL+"/api/v1/volumes/"+volumeID+"/discard", []byte(body), gatewayLoadHeaders(index, phase)))
}

func gatewayFlushWithLoadMetadata(client *http.Client, gatewayURL, volumeID string, index int, phase string) (gatewayOperationResponse, error) {
	return decodeGatewayOperationResponse(gatewayPostWithHeaders(client, gatewayURL+"/api/v1/volumes/"+volumeID+"/flush", []byte(`{}`), gatewayLoadHeaders(index, phase)))
}

func gatewayRead(client *http.Client, gatewayURL, volumeID string, offsetBytes, lengthBytes uint64) ([]byte, error) {
	body := fmt.Sprintf(`{"offset_bytes":%d,"length_bytes":%d}`, offsetBytes, lengthBytes)
	respBody, err := gatewayPost(client, gatewayURL+"/api/v1/volumes/"+volumeID+"/read", []byte(body))
	return decodeGatewayReadResponse(respBody, err)
}

func gatewayCloneRead(client *http.Client, gatewayURL, volumeID, cloneID string, offsetBytes, lengthBytes uint64) ([]byte, error) {
	body := fmt.Sprintf(`{"offset_bytes":%d,"length_bytes":%d}`, offsetBytes, lengthBytes)
	respBody, err := gatewayPost(client, gatewayURL+"/api/v1/debug/sbs-cluster/volumes/"+volumeID+"/clones/"+cloneID+"/read", []byte(body))
	return decodeGatewayReadResponse(respBody, err)
}

func gatewayReadWithLoadMetadata(client *http.Client, gatewayURL, volumeID string, offsetBytes, lengthBytes uint64, index int, phase string) ([]byte, gatewayOperationResponse, error) {
	body := fmt.Sprintf(`{"offset_bytes":%d,"length_bytes":%d}`, offsetBytes, lengthBytes)
	respBody, err := gatewayPostWithHeaders(client, gatewayURL+"/api/v1/volumes/"+volumeID+"/read", []byte(body), gatewayLoadHeaders(index, phase))
	return decodeGatewayReadResponseWithOperation(respBody, err)
}

func gatewayReadOperationWithLoadMetadata(client *http.Client, gatewayURL, volumeID string, offsetBytes, lengthBytes uint64, index int, phase string) (gatewayOperationResponse, error) {
	body := fmt.Sprintf(`{"offset_bytes":%d,"length_bytes":%d}`, offsetBytes, lengthBytes)
	respBody, err := gatewayPostWithHeaders(client, gatewayURL+"/api/v1/volumes/"+volumeID+"/read", []byte(body), gatewayLoadHeaders(index, phase))
	return decodeGatewayReadOperationResponse(respBody, err, lengthBytes)
}

func decodeGatewayReadResponse(respBody []byte, err error) ([]byte, error) {
	data, _, err := decodeGatewayReadResponseWithOperation(respBody, err)
	return data, err
}

func decodeGatewayReadResponseWithOperation(respBody []byte, err error) ([]byte, gatewayOperationResponse, error) {
	if err != nil {
		return nil, gatewayOperationResponse{PhaseOThrottle: gatewayPhaseOThrottleObservationFromError(err)}, err
	}
	var resp struct {
		DataBase64     string                            `json:"data_base64"`
		PhaseOThrottle *gatewayPhaseOThrottleObservation `json:"phase_o_throttle,omitempty"`
	}
	if err := json.Unmarshal(respBody, &resp); err != nil {
		return nil, gatewayOperationResponse{}, fmt.Errorf("decode read response: %w", err)
	}
	data, err := base64.StdEncoding.DecodeString(resp.DataBase64)
	if err != nil {
		return nil, gatewayOperationResponse{PhaseOThrottle: resp.PhaseOThrottle}, fmt.Errorf("decode read data: %w", err)
	}
	return data, gatewayOperationResponse{PhaseOThrottle: resp.PhaseOThrottle}, nil
}

func decodeGatewayReadOperationResponse(respBody []byte, err error, expectedLengthBytes uint64) (gatewayOperationResponse, error) {
	if err != nil {
		return gatewayOperationResponse{PhaseOThrottle: gatewayPhaseOThrottleObservationFromError(err)}, err
	}
	var resp struct {
		DataBase64     string                            `json:"data_base64"`
		PhaseOThrottle *gatewayPhaseOThrottleObservation `json:"phase_o_throttle,omitempty"`
	}
	if err := json.Unmarshal(respBody, &resp); err != nil {
		return gatewayOperationResponse{}, fmt.Errorf("decode read response: %w", err)
	}
	opResp := gatewayOperationResponse{PhaseOThrottle: resp.PhaseOThrottle}
	if encoded, ok := gatewayCanonicalZeroBase64ForLength(expectedLengthBytes); ok && resp.DataBase64 == encoded {
		return opResp, nil
	}
	data, err := base64.StdEncoding.DecodeString(resp.DataBase64)
	if err != nil {
		return opResp, fmt.Errorf("decode read data: %w", err)
	}
	if expectedLengthBytes > gatewayMaxIntUint64 {
		return opResp, fmt.Errorf("read response length %d exceeds platform int capacity", expectedLengthBytes)
	}
	if len(data) != int(expectedLengthBytes) {
		return opResp, fmt.Errorf("read response length mismatch: expected=%d actual=%d", expectedLengthBytes, len(data))
	}
	return opResp, nil
}

func decodeGatewayOperationResponse(respBody []byte, err error) (gatewayOperationResponse, error) {
	if err != nil {
		return gatewayOperationResponse{PhaseOThrottle: gatewayPhaseOThrottleObservationFromError(err)}, err
	}
	var resp gatewayOperationResponse
	if len(respBody) == 0 {
		return resp, nil
	}
	if err := json.Unmarshal(respBody, &resp); err != nil {
		return gatewayOperationResponse{}, fmt.Errorf("decode gateway response: %w", err)
	}
	return resp, nil
}

func gatewayPost(client *http.Client, url string, body []byte) ([]byte, error) {
	return gatewayPostWithHeaders(client, url, body, nil)
}

type gatewayHTTPStatusError struct {
	StatusCode int
	Body       string
}

func (e *gatewayHTTPStatusError) Error() string {
	return fmt.Sprintf("http status=%d body=%s", e.StatusCode, strings.TrimSpace(e.Body))
}

func gatewayLoadHeaders(index int, phase string) map[string]string {
	return map[string]string{
		"X-NAMRBD-Load-Index": strconv.Itoa(index),
		"X-NAMRBD-Load-Phase": phase,
	}
}

func gatewayPostWithHeaders(client *http.Client, url string, body []byte, headers map[string]string) ([]byte, error) {
	req, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	for key, value := range headers {
		req.Header.Set(key, value)
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	respBody, readErr := io.ReadAll(io.LimitReader(resp.Body, gatewayPostResponseLimitBytes+1))
	if readErr != nil {
		return nil, readErr
	}
	if len(respBody) > gatewayPostResponseLimitBytes {
		return nil, fmt.Errorf("gateway response body exceeds limit: have>%d", gatewayPostResponseLimitBytes)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, &gatewayHTTPStatusError{StatusCode: resp.StatusCode, Body: string(respBody)}
	}
	return respBody, nil
}

func gatewayPhaseOThrottleObservationFromError(err error) *gatewayPhaseOThrottleObservation {
	var statusErr *gatewayHTTPStatusError
	if !errors.As(err, &statusErr) {
		return nil
	}
	var resp gatewayOperationResponse
	if json.Unmarshal([]byte(statusErr.Body), &resp) != nil {
		return nil
	}
	return resp.PhaseOThrottle
}

func recordGatewayPhaseOThrottle(report *gatewayWriteLoadReport, obs *gatewayPhaseOThrottleObservation) {
	if obs == nil {
		return
	}
	if report.PhaseOThrottle == nil {
		report.PhaseOThrottle = &gatewayPhaseOThrottleReport{
			Observed:                 true,
			PolicyID:                 obs.PolicyID,
			PolicyGeneration:         obs.PolicyGeneration,
			CapScope:                 obs.CapScope,
			ThrottleMode:             obs.ThrottleMode,
			IOPSCap:                  obs.IOPSCap,
			BandwidthCapBytesPerSec:  obs.BandwidthCapBytesPerSec,
			BurstIOPS:                obs.BurstIOPS,
			BurstBytes:               obs.BurstBytes,
			EnforcedBeforeDispatch:   obs.EnforcedBeforeDispatch,
			ClusterWideCapSupport:    obs.ClusterWideCapSupport,
			GatewayRestartRequired:   obs.GatewayRestartRequired,
			RemoteLabValidationState: obs.RemoteLabValidationState,
		}
	}
	throttle := report.PhaseOThrottle
	throttle.Observed = true
	throttle.RequestedTokens += obs.RequestedTokens
	throttle.GrantedTokens += obs.GrantedTokens
	throttle.RequestedBytes += obs.RequestedBytes
	throttle.GrantedBytes += obs.GrantedBytes
	throttle.DeniedTokens += obs.DeniedTokens
	throttle.DeniedBytes += obs.DeniedBytes
	if obs.LeaseID != "" {
		if throttle.FirstLeaseID == "" {
			throttle.FirstLeaseID = obs.LeaseID
		}
		throttle.LastLeaseID = obs.LeaseID
		throttle.LeaseCount++
		if obs.LeaseGeneration > throttle.MaxLeaseGeneration {
			throttle.MaxLeaseGeneration = obs.LeaseGeneration
		}
	}
	throttle.SharedBudgetAuthority = throttle.SharedBudgetAuthority || obs.SharedBudgetAuthority
	throttle.GatewayConsumesLease = throttle.GatewayConsumesLease || obs.GatewayConsumesLease
	throttle.EnforcedBeforeDispatch = throttle.EnforcedBeforeDispatch || obs.EnforcedBeforeDispatch
	throttle.ClusterWideCapSupport = throttle.ClusterWideCapSupport || obs.ClusterWideCapSupport
	throttle.ThrottledOps += obs.ThrottledOps
	throttle.ThrottledBytes += obs.ThrottledBytes
	if obs.ThrottleWaitMs > 0 {
		throttle.ThrottleWaitCount++
		throttle.ThrottleWaitTotalMS += obs.ThrottleWaitMs
		if obs.ThrottleWaitMs > throttle.ThrottleWaitMaxMS {
			throttle.ThrottleWaitMaxMS = obs.ThrottleWaitMs
		}
	}
	throttle.RejectedOps += obs.RejectedOps
	if obs.RejectedOps > 0 {
		reason := obs.RejectionReason
		if reason == "" {
			reason = "unknown"
		}
		if throttle.RejectionReasons == nil {
			throttle.RejectionReasons = make(map[string]uint64)
		}
		throttle.RejectionReasons[reason] += obs.RejectedOps
	}
}

func fillLatencySummary(report *gatewayWriteLoadReport, latencies []float64) {
	summary := newGatewayLoadLatencySummary(latencies)
	if summary == nil {
		return
	}
	report.LatencyAvgMS = summary.AvgMS
	report.LatencyMinMS = summary.MinMS
	report.LatencyP50MS = summary.P50MS
	report.LatencyP90MS = summary.P90MS
	report.LatencyP95MS = summary.P95MS
	report.LatencyP99MS = summary.P99MS
	report.LatencyP999MS = summary.P999MS
	report.LatencyMaxMS = summary.MaxMS
}

func newGatewayLoadLatencySummary(latencies []float64) *gatewayLoadLatencySummary {
	if len(latencies) == 0 {
		return nil
	}
	values := append([]float64(nil), latencies...)
	sort.Float64s(values)
	var sum float64
	for _, latency := range values {
		sum += latency
	}
	return &gatewayLoadLatencySummary{
		Count:  len(values),
		AvgMS:  sum / float64(len(values)),
		MinMS:  values[0],
		P50MS:  percentile(values, 50),
		P90MS:  percentile(values, 90),
		P95MS:  percentile(values, 95),
		P99MS:  percentile(values, 99),
		P999MS: percentile(values, 99.9),
		MaxMS:  values[len(values)-1],
	}
}

func fillFileLatencySummary(report *fileLoadReport, latencies []float64) {
	if len(latencies) == 0 {
		return
	}
	sort.Float64s(latencies)
	var sum float64
	for _, latency := range latencies {
		sum += latency
	}
	report.LatencyAvgMS = sum / float64(len(latencies))
	report.LatencyMinMS = latencies[0]
	report.LatencyP50MS = percentile(latencies, 50)
	report.LatencyP90MS = percentile(latencies, 90)
	report.LatencyP95MS = percentile(latencies, 95)
	report.LatencyP99MS = percentile(latencies, 99)
	report.LatencyP999MS = percentile(latencies, 99.9)
	report.LatencyMaxMS = latencies[len(latencies)-1]
}

func percentile(values []float64, pct float64) float64 {
	if len(values) == 0 {
		return 0
	}
	if pct <= 0 {
		return values[0]
	}
	if pct >= 100 {
		return values[len(values)-1]
	}
	pos := (pct / 100.0) * float64(len(values)-1)
	lower := int(math.Floor(pos))
	upper := int(math.Ceil(pos))
	if lower == upper {
		return values[lower]
	}
	weight := pos - float64(lower)
	return values[lower]*(1-weight) + values[upper]*weight
}

func parseByteSize(raw string) (uint64, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return 0, fmt.Errorf("size is empty")
	}
	unitMultiplier := uint64(1)
	last := raw[len(raw)-1]
	if last < '0' || last > '9' {
		switch last {
		case 'k', 'K':
			unitMultiplier = 1024
		case 'm', 'M':
			unitMultiplier = 1024 * 1024
		case 'g', 'G':
			unitMultiplier = 1024 * 1024 * 1024
		default:
			return 0, fmt.Errorf("unsupported size unit %q", string(last))
		}
		raw = strings.TrimSpace(raw[:len(raw)-1])
	}
	value, err := strconv.ParseUint(raw, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("size value must be an integer")
	}
	if value == 0 {
		return 0, fmt.Errorf("size must be greater than zero")
	}
	if value > ^uint64(0)/unitMultiplier {
		return 0, fmt.Errorf("size is too large")
	}
	return value * unitMultiplier, nil
}

func parseByteOffset(raw string) (uint64, error) {
	raw = strings.TrimSpace(raw)
	if raw == "0" {
		return 0, nil
	}
	return parseByteSize(raw)
}

func minUint64(a, b uint64) uint64 {
	if a < b {
		return a
	}
	return b
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func inspectVolumeLayout(ctx context.Context, repo service.MetadataRepository, volumeID uint64) (volumeLayoutReport, error) {
	volume, err := repo.GetVolume(ctx, volumeID)
	if err != nil {
		return volumeLayoutReport{}, err
	}
	pages, err := repo.ListExtentPages(ctx, volumeID)
	if err != nil {
		return volumeLayoutReport{}, err
	}
	sort.Slice(pages, func(i, j int) bool { return pages[i].PageNo < pages[j].PageNo })
	report := volumeLayoutReport{
		Volume:              volumeSpecOutputFrom(volume),
		PageCount:           len(pages),
		AllocationPageCount: len(pages),
		Pages:               pages,
		AllocationPages:     pages,
	}
	for _, page := range pages {
		report.ExtentCount += len(page.Extents)
		report.AllocationChunkCount += len(page.Extents)
		for _, extent := range page.Extents {
			if extent.Kind == service.AllocationChunkKindData {
				report.DataExtentCount++
				report.DataAllocationChunks++
			}
			if extent.Kind == service.AllocationChunkKindZero {
				report.ZeroExtentCount++
				report.ZeroAllocationChunks++
			}
		}
	}
	return report, nil
}

func validateExtents(ctx context.Context, repo service.MetadataRepository, volumeID uint64) (extentValidationReport, error) {
	volume, err := repo.GetVolume(ctx, volumeID)
	if err != nil {
		return extentValidationReport{}, err
	}
	pages, err := repo.ListExtentPages(ctx, volumeID)
	if err != nil {
		return extentValidationReport{}, err
	}
	sort.Slice(pages, func(i, j int) bool { return pages[i].PageNo < pages[j].PageNo })
	issues := make([]validationIssue, 0)
	addIssue := func(code, message string) {
		issues = append(issues, validationIssue{Severity: "error", Code: code, Message: message})
	}

	if volume.ChunkSizeBytes == 0 {
		addIssue("chunk_size_zero", "volume chunk_size_bytes is zero")
	}
	if volume.ExtentPageBytes == 0 {
		addIssue("extent_page_size_zero", "volume extent_page_bytes is zero")
	}
	if volume.ChunkSizeBytes > 0 && volume.ExtentPageBytes > 0 && volume.ExtentPageBytes%volume.ChunkSizeBytes != 0 {
		addIssue("extent_page_alignment", fmt.Sprintf("extent_page_bytes=%d is not a multiple of chunk_size_bytes=%d", volume.ExtentPageBytes, volume.ChunkSizeBytes))
	}
	maxPageCount := uint64(0)
	if volume.ExtentPageBytes > 0 && volume.SizeBytes > 0 {
		maxPageCount = (volume.SizeBytes + uint64(volume.ExtentPageBytes) - 1) / uint64(volume.ExtentPageBytes)
	}

	for _, page := range pages {
		validateExtentPage(volume, page, maxPageCount, addIssue)
	}

	return extentValidationReport{
		VolumeID:        service.HexVolumeID(volumeID),
		OK:              len(issues) == 0,
		PageCount:       len(pages),
		IssueCount:      len(issues),
		Issues:          issues,
		Volume:          volumeSpecOutputFrom(volume),
		ExtentPages:     pages,
		AllocationPages: pages,
	}, nil
}

func validateExtentPage(volume service.VolumeSpec, page service.AllocationPageRecord, maxPageCount uint64, addIssue func(code, message string)) {
	pageLabel := fmt.Sprintf("page %d", page.PageNo)
	if page.VolumeID != volume.ID {
		addIssue("extent_page_volume_mismatch", fmt.Sprintf("%s has volume_id=%s expected=%s", pageLabel, service.CanonicalVolumeID(uint64(page.VolumeID)), service.CanonicalVolumeID(uint64(volume.ID))))
	}
	if maxPageCount > 0 && page.PageNo >= maxPageCount {
		addIssue("extent_page_out_of_range", fmt.Sprintf("%s is outside volume address space", pageLabel))
	}
	if page.PageBytes != volume.ExtentPageBytes {
		addIssue("extent_page_size_mismatch", fmt.Sprintf("%s page_bytes=%d expected=%d", pageLabel, page.PageBytes, volume.ExtentPageBytes))
	}
	if page.ChunkSizeBytes != volume.ChunkSizeBytes {
		addIssue("extent_chunk_size_mismatch", fmt.Sprintf("%s chunk_size_bytes=%d expected=%d", pageLabel, page.ChunkSizeBytes, volume.ChunkSizeBytes))
	}
	if page.PageBytes == 0 || page.ChunkSizeBytes == 0 {
		return
	}
	if page.PageBytes%page.ChunkSizeBytes != 0 {
		addIssue("extent_page_unaligned", fmt.Sprintf("%s page_bytes=%d is not a multiple of chunk_size_bytes=%d", pageLabel, page.PageBytes, page.ChunkSizeBytes))
		return
	}
	chunksPerPage := uint64(page.PageBytes / page.ChunkSizeBytes)
	pageStartChunk := page.PageNo * chunksPerPage
	if len(page.Extents) == 0 {
		addIssue("extent_page_empty", fmt.Sprintf("%s has no extents", pageLabel))
		return
	}
	expectedLocalStart := uint64(0)
	for idx, extent := range page.Extents {
		if extent.ChunkCount == 0 {
			addIssue("extent_chunk_count_zero", fmt.Sprintf("%s extent[%d] chunk_count is zero", pageLabel, idx))
			continue
		}
		if extent.LogicalChunkStart < pageStartChunk {
			addIssue("extent_page_chunk_underflow", fmt.Sprintf("%s extent[%d] starts at global chunk %d before page start %d", pageLabel, idx, extent.LogicalChunkStart, pageStartChunk))
			continue
		}
		localStart := extent.LogicalChunkStart - pageStartChunk
		if localStart != expectedLocalStart {
			addIssue("extent_not_contiguous", fmt.Sprintf("%s extent[%d] starts at local chunk %d expected %d", pageLabel, idx, localStart, expectedLocalStart))
		}
		endLocal := localStart + uint64(extent.ChunkCount)
		if endLocal > chunksPerPage {
			addIssue("extent_page_overflow", fmt.Sprintf("%s extent[%d] ends at local chunk %d beyond page chunk limit %d", pageLabel, idx, endLocal, chunksPerPage))
		}
		switch extent.Kind {
		case service.AllocationChunkKindData:
			if extent.PhysicalChunkStart == 0 {
				addIssue("extent_data_physical_zero", fmt.Sprintf("%s extent[%d] is data but physical_chunk_start is zero", pageLabel, idx))
			}
		case service.AllocationChunkKindZero:
			if extent.PhysicalChunkStart != 0 {
				addIssue("extent_zero_physical_nonzero", fmt.Sprintf("%s extent[%d] is zero but physical_chunk_start=%d", pageLabel, idx, extent.PhysicalChunkStart))
			}
		default:
			addIssue("extent_kind_invalid", fmt.Sprintf("%s extent[%d] has unknown kind=%q", pageLabel, idx, extent.Kind))
		}
		expectedLocalStart = endLocal
	}
	if expectedLocalStart != chunksPerPage {
		addIssue("extent_page_incomplete_coverage", fmt.Sprintf("%s covers %d local chunks expected %d", pageLabel, expectedLocalStart, chunksPerPage))
	}
}

func printVolumeSpec(volume service.VolumeSpec) {
	fmt.Printf("volume_id=%s\nname=%s\nprefix=%s\nsize_bytes=%d\nblock_size=%d\nchunk_size_bytes=%d\nallocation_chunk_size_bytes=%d\nextent_page_bytes=%d\nallocation_page_bytes=%d\naccess_mode=%s\nstate=%s\n",
		service.CanonicalVolumeID(uint64(volume.ID)), volume.Name, volume.Prefix, volume.SizeBytes, volume.BlockSize, volume.ChunkSizeBytes, volume.ChunkSizeBytes, volume.ExtentPageBytes, volume.ExtentPageBytes, volume.AccessMode, volume.State)
}

func printVolumeLayout(report volumeLayoutReport) {
	printVolumeSpec(report.Volume.VolumeSpec)
	fmt.Printf("page_count=%d\nallocation_page_count=%d\nextent_count=%d\nallocation_chunk_count=%d\ndata_extent_count=%d\ndata_allocation_chunks=%d\nzero_extent_count=%d\nzero_allocation_chunks=%d\n",
		report.PageCount, report.AllocationPageCount, report.ExtentCount, report.AllocationChunkCount, report.DataExtentCount, report.DataAllocationChunks, report.ZeroExtentCount, report.ZeroAllocationChunks)
	for _, page := range report.Pages {
		fmt.Printf("page_no=%d page_bytes=%d allocation_page_bytes=%d chunk_size_bytes=%d allocation_chunk_size_bytes=%d revision=%d extent_count=%d allocation_chunk_count=%d\n",
			page.PageNo, page.PageBytes, page.PageBytes, page.ChunkSizeBytes, page.ChunkSizeBytes, page.Revision, len(page.Extents), len(page.Extents))
		for idx, extent := range page.Extents {
			fmt.Printf("  allocation_chunk[%d] logical_chunk_start=%d logical_allocation_chunk_start=%d chunk_count=%d allocation_chunk_count=%d kind=%s physical_chunk_start=%d physical_allocation_chunk_start=%d\n",
				idx, extent.LogicalChunkStart, extent.LogicalChunkStart, extent.ChunkCount, extent.ChunkCount, extent.Kind, extent.PhysicalChunkStart, extent.PhysicalChunkStart)
		}
	}
}

func printExtentValidation(report extentValidationReport) {
	fmt.Printf("volume_id=%s ok=%t page_count=%d issue_count=%d\n",
		service.CanonicalVolumeID(uint64(report.VolumeID)), report.OK, report.PageCount, report.IssueCount)
	for _, issue := range report.Issues {
		fmt.Printf("%s %s: %s\n", strings.ToUpper(issue.Severity), issue.Code, issue.Message)
	}
}

func fatalf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}

func requireYes(yes bool) {
	if !yes {
		fatalf("unsafe command requires --yes")
	}
}

func newObjectStore(ctx context.Context, cfg *commandConfig) (store.ObjectStore, func(), error) {
	switch cfg.storeBackend {
	case "memory":
		return store.NewMemoryStore(), func() {}, nil
	case "redis":
		return store.NewRedisStore(cfg.redisAddr, 3*time.Second), func() {}, nil
	default:
		return nil, nil, fmt.Errorf("invalid --store-backend %q: must be redis or memory", cfg.storeBackend)
	}
}

// parseBlockSizeK accepts only <n>K (binary KiB). Example: 4K -> 4096 bytes.
func parseBlockSizeK(raw string) (uint64, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return 0, fmt.Errorf("block size is empty")
	}
	if len(raw) < 2 {
		return 0, fmt.Errorf("block size must be <n>K (e.g. 4K)")
	}
	unit := raw[len(raw)-1]
	if unit != 'K' && unit != 'k' {
		return 0, fmt.Errorf("block size unit must be K only (e.g. 4K)")
	}
	number := strings.TrimSpace(raw[:len(raw)-1])
	if number == "" {
		return 0, fmt.Errorf("block size value is required")
	}
	value, err := strconv.ParseUint(number, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("block size value must be an integer")
	}
	if value == 0 {
		return 0, fmt.Errorf("block size must be greater than zero")
	}
	if value > ^uint64(0)/1024 {
		return 0, fmt.Errorf("block size is too large")
	}
	bytes := value * 1024
	if bytes > uint64(^uint32(0)) {
		return 0, fmt.Errorf("block size is too large for uint32")
	}
	return bytes, nil
}
