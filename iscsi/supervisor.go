package iscsi

import (
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/gostor/gotgt/pkg/api"
)

const DefaultMaxExportsPerProcess = 64

const (
	ExportRuntimeStarting = "starting"
	ExportRuntimeServing  = "serving"
	ExportRuntimeFailed   = "failed"
	ExportRuntimeStopped  = "stopped"
)

// ExportRuntimeSpec is the complete registry-selected state needed to install
// one target/LUN. The backing store is opened before Install is called, so a
// partially valid snapshot never changes the set being served.
type ExportRuntimeSpec struct {
	ExportID            string
	VolumeID            string
	TargetIQN           string
	LUNID               uint64
	LUNWWN              string
	DeviceID            uint64
	SizeBytes           uint64
	ActiveGatewayID     string
	ExportLeaseID       string
	ExportEpoch         uint64
	WriteAdmissionState string
	InitialMemoryBytes  uint64
	BackingStore        api.RemoteBackingStore
}

type ExportRuntimeSnapshot struct {
	ExportID            string `json:"export_id"`
	VolumeID            string `json:"volume_id"`
	TargetIQN           string `json:"target_iqn"`
	LUNID               uint64 `json:"lun_id"`
	LUNWWN              string `json:"lun_wwn"`
	ActiveGatewayID     string `json:"active_iscsi_gateway_id"`
	ExportLeaseID       string `json:"export_lease_id"`
	ExportEpoch         uint64 `json:"export_epoch"`
	WriteAdmissionState string `json:"write_admission_state"`
	State               string `json:"state"`
	SessionCount        int64  `json:"session_count"`
	InFlightIO          int64  `json:"in_flight_io"`
	MemoryBytes         uint64 `json:"memory_bytes"`
	PeakMemoryBytes     uint64 `json:"peak_memory_bytes"`
	ReadCount           uint64 `json:"read_count"`
	WriteCount          uint64 `json:"write_count"`
	FlushCount          uint64 `json:"flush_count"`
	UnmapCount          uint64 `json:"unmap_count"`
	ErrorCount          uint64 `json:"error_count"`
	FirstError          string `json:"first_error,omitempty"`
	LastError           string `json:"last_error,omitempty"`
	LastReloadError     string `json:"last_reload_error,omitempty"`
}

type MultiExportSummary struct {
	ISCSIGatewayMultiExportReady bool                    `json:"iscsi_gateway_multi_export_ready"`
	ServedExportCount            int                     `json:"served_export_count"`
	ServedVolumeCount            int                     `json:"served_volume_count"`
	MaxExportsPerProcess         int                     `json:"max_exports_per_process"`
	MultiExportFirstError        string                  `json:"multi_export_first_error,omitempty"`
	MultiExportLastError         string                  `json:"multi_export_last_error,omitempty"`
	Exports                      []ExportRuntimeSnapshot `json:"exports"`
}

// MultiExportSupervisor owns independently accounted export runtimes. Install
// validates the whole input before publishing any runtime.
type MultiExportSupervisor struct {
	mu         sync.RWMutex
	maxExports int
	runtimes   map[string]*ExportRuntime
	firstErr   string
	lastErr    string
}

func NewMultiExportSupervisor(maxExports int) (*MultiExportSupervisor, error) {
	if maxExports == 0 {
		maxExports = DefaultMaxExportsPerProcess
	}
	if maxExports < 1 || maxExports > DefaultMaxExportsPerProcess {
		return nil, fmt.Errorf("max_exports_per_process must be 1..%d, got %d", DefaultMaxExportsPerProcess, maxExports)
	}
	return &MultiExportSupervisor{
		maxExports: maxExports,
		runtimes:   make(map[string]*ExportRuntime, maxExports),
	}, nil
}

func (s *MultiExportSupervisor) Install(specs []ExportRuntimeSpec) error {
	if s == nil {
		return fmt.Errorf("multi-export supervisor is nil")
	}
	if len(specs) > s.maxExports {
		err := fmt.Errorf("registry selected %d exports, exceeding max_exports_per_process=%d", len(specs), s.maxExports)
		s.recordError(err)
		return err
	}

	next := make(map[string]*ExportRuntime, len(specs))
	targets := make(map[string]struct{}, len(specs))
	devices := make(map[uint64]struct{}, len(specs))
	for i := range specs {
		spec := normalizeExportRuntimeSpec(specs[i])
		if err := validateExportRuntimeSpec(spec); err != nil {
			err = fmt.Errorf("export[%d]: %w", i, err)
			s.recordError(err)
			return err
		}
		if _, ok := next[spec.ExportID]; ok {
			return s.reject(fmt.Errorf("duplicate export_id %q", spec.ExportID))
		}
		if _, ok := targets[spec.TargetIQN]; ok {
			return s.reject(fmt.Errorf("duplicate target_iqn %q", spec.TargetIQN))
		}
		if _, ok := devices[spec.DeviceID]; ok {
			return s.reject(fmt.Errorf("duplicate SCSI device_id %d", spec.DeviceID))
		}
		targets[spec.TargetIQN] = struct{}{}
		devices[spec.DeviceID] = struct{}{}
		next[spec.ExportID] = newExportRuntime(spec)
	}

	s.mu.Lock()
	if len(s.runtimes) != 0 {
		s.mu.Unlock()
		return s.reject(fmt.Errorf("multi-export supervisor already has %d installed exports", len(s.runtimes)))
	}
	s.runtimes = next
	s.mu.Unlock()
	return nil
}

func (s *MultiExportSupervisor) reject(err error) error {
	s.recordError(err)
	return err
}

func (s *MultiExportSupervisor) recordError(err error) {
	if s == nil || err == nil {
		return
	}
	msg := strings.TrimSpace(err.Error())
	s.mu.Lock()
	if s.firstErr == "" {
		s.firstErr = msg
	}
	s.lastErr = msg
	s.mu.Unlock()
}

func (s *MultiExportSupervisor) Runtime(exportID string) (*ExportRuntime, bool) {
	if s == nil {
		return nil, false
	}
	s.mu.RLock()
	runtime, ok := s.runtimes[strings.TrimSpace(exportID)]
	s.mu.RUnlock()
	return runtime, ok
}

func (s *MultiExportSupervisor) Specs() []ExportRuntimeSpec {
	if s == nil {
		return nil
	}
	s.mu.RLock()
	specs := make([]ExportRuntimeSpec, 0, len(s.runtimes))
	for _, runtime := range s.runtimes {
		specs = append(specs, runtime.spec)
	}
	s.mu.RUnlock()
	sort.Slice(specs, func(i, j int) bool { return specs[i].ExportID < specs[j].ExportID })
	return specs
}

func (s *MultiExportSupervisor) MarkServing() {
	if s == nil {
		return
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, runtime := range s.runtimes {
		runtime.setState(ExportRuntimeServing)
	}
}

func (s *MultiExportSupervisor) MarkStopped() {
	if s == nil {
		return
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, runtime := range s.runtimes {
		runtime.setState(ExportRuntimeStopped)
	}
}

func (s *MultiExportSupervisor) Summary() MultiExportSummary {
	if s == nil {
		return MultiExportSummary{}
	}
	s.mu.RLock()
	summary := MultiExportSummary{
		ISCSIGatewayMultiExportReady: len(s.runtimes) > 0,
		ServedExportCount:            len(s.runtimes),
		MaxExportsPerProcess:         s.maxExports,
		MultiExportFirstError:        s.firstErr,
		MultiExportLastError:         s.lastErr,
		Exports:                      make([]ExportRuntimeSnapshot, 0, len(s.runtimes)),
	}
	volumes := make(map[string]struct{}, len(s.runtimes))
	for _, runtime := range s.runtimes {
		snapshot := runtime.Snapshot()
		summary.Exports = append(summary.Exports, snapshot)
		volumes[snapshot.VolumeID] = struct{}{}
		if snapshot.State == ExportRuntimeFailed {
			summary.ISCSIGatewayMultiExportReady = false
		}
	}
	s.mu.RUnlock()
	summary.ServedVolumeCount = len(volumes)
	sort.Slice(summary.Exports, func(i, j int) bool { return summary.Exports[i].ExportID < summary.Exports[j].ExportID })
	return summary
}

// ExportRuntime wraps one backing store so I/O pressure and failure evidence
// never share counters with another export.
type ExportRuntime struct {
	spec ExportRuntimeSpec

	stateMu         sync.RWMutex
	state           string
	firstErr        string
	lastErr         string
	lastReloadError string

	sessions    atomic.Int64
	inFlight    atomic.Int64
	memoryBytes atomic.Uint64
	peakMemory  atomic.Uint64
	readCount   atomic.Uint64
	writeCount  atomic.Uint64
	flushCount  atomic.Uint64
	unmapCount  atomic.Uint64
	errorCount  atomic.Uint64
}

func newExportRuntime(spec ExportRuntimeSpec) *ExportRuntime {
	r := &ExportRuntime{spec: spec, state: ExportRuntimeStarting}
	r.memoryBytes.Store(spec.InitialMemoryBytes)
	r.peakMemory.Store(spec.InitialMemoryBytes)
	return r
}

func (r *ExportRuntime) BackingStore() api.RemoteBackingStore { return r }

func (r *ExportRuntime) ReadAt(p []byte, off int64) (int, error) {
	r.beginIO(uint64(len(p)))
	defer r.endIO(uint64(len(p)))
	n, err := r.spec.BackingStore.ReadAt(p, off)
	r.readCount.Add(1)
	r.observeError(err)
	return n, err
}

func (r *ExportRuntime) WriteAt(p []byte, off int64) (int, error) {
	r.beginIO(uint64(len(p)))
	defer r.endIO(uint64(len(p)))
	n, err := r.spec.BackingStore.WriteAt(p, off)
	r.writeCount.Add(1)
	r.observeError(err)
	return n, err
}

func (r *ExportRuntime) Sync() (int, error) {
	r.beginIO(0)
	defer r.endIO(0)
	n, err := r.spec.BackingStore.Sync()
	r.flushCount.Add(1)
	r.observeError(err)
	return n, err
}

func (r *ExportRuntime) Unmap(offset, length int64) (int, error) {
	bytes := uint64(0)
	if length > 0 {
		bytes = uint64(length)
	}
	r.beginIO(bytes)
	defer r.endIO(bytes)
	n, err := r.spec.BackingStore.Unmap(offset, length)
	r.unmapCount.Add(1)
	r.observeError(err)
	return n, err
}

func (r *ExportRuntime) SessionOpened() { r.sessions.Add(1) }

func (r *ExportRuntime) SessionClosed() {
	for {
		current := r.sessions.Load()
		if current == 0 || r.sessions.CompareAndSwap(current, current-1) {
			return
		}
	}
}

func (r *ExportRuntime) SetLastReloadError(err error) {
	r.stateMu.Lock()
	if err == nil {
		r.lastReloadError = ""
	} else {
		r.lastReloadError = strings.TrimSpace(err.Error())
	}
	r.stateMu.Unlock()
}

func (r *ExportRuntime) Snapshot() ExportRuntimeSnapshot {
	r.stateMu.RLock()
	snapshot := ExportRuntimeSnapshot{
		ExportID:            r.spec.ExportID,
		VolumeID:            r.spec.VolumeID,
		TargetIQN:           r.spec.TargetIQN,
		LUNID:               r.spec.LUNID,
		LUNWWN:              r.spec.LUNWWN,
		ActiveGatewayID:     r.spec.ActiveGatewayID,
		ExportLeaseID:       r.spec.ExportLeaseID,
		ExportEpoch:         r.spec.ExportEpoch,
		WriteAdmissionState: r.spec.WriteAdmissionState,
		State:               r.state,
		FirstError:          r.firstErr,
		LastError:           r.lastErr,
		LastReloadError:     r.lastReloadError,
	}
	r.stateMu.RUnlock()
	snapshot.SessionCount = r.sessions.Load()
	snapshot.InFlightIO = r.inFlight.Load()
	snapshot.MemoryBytes = r.memoryBytes.Load()
	snapshot.PeakMemoryBytes = r.peakMemory.Load()
	snapshot.ReadCount = r.readCount.Load()
	snapshot.WriteCount = r.writeCount.Load()
	snapshot.FlushCount = r.flushCount.Load()
	snapshot.UnmapCount = r.unmapCount.Load()
	snapshot.ErrorCount = r.errorCount.Load()
	return snapshot
}

func (r *ExportRuntime) setState(state string) {
	r.stateMu.Lock()
	r.state = state
	r.stateMu.Unlock()
}

func (r *ExportRuntime) beginIO(bytes uint64) {
	r.inFlight.Add(1)
	current := r.memoryBytes.Add(bytes)
	for {
		peak := r.peakMemory.Load()
		if current <= peak || r.peakMemory.CompareAndSwap(peak, current) {
			break
		}
	}
}

func (r *ExportRuntime) endIO(bytes uint64) {
	if bytes > 0 {
		r.memoryBytes.Add(^uint64(bytes - 1))
	}
	r.inFlight.Add(-1)
}

func (r *ExportRuntime) observeError(err error) {
	if err == nil || err == io.EOF {
		return
	}
	msg := strings.TrimSpace(err.Error())
	r.errorCount.Add(1)
	r.stateMu.Lock()
	if r.firstErr == "" {
		r.firstErr = msg
	}
	r.lastErr = msg
	r.state = ExportRuntimeFailed
	r.stateMu.Unlock()
}

func normalizeExportRuntimeSpec(spec ExportRuntimeSpec) ExportRuntimeSpec {
	spec.ExportID = strings.TrimSpace(spec.ExportID)
	spec.VolumeID = strings.TrimSpace(spec.VolumeID)
	spec.TargetIQN = strings.TrimSpace(spec.TargetIQN)
	spec.LUNWWN = strings.TrimSpace(spec.LUNWWN)
	spec.ActiveGatewayID = strings.TrimSpace(spec.ActiveGatewayID)
	spec.WriteAdmissionState = strings.TrimSpace(spec.WriteAdmissionState)
	return spec
}

func validateExportRuntimeSpec(spec ExportRuntimeSpec) error {
	for name, value := range map[string]string{
		"export_id":  spec.ExportID,
		"volume_id":  spec.VolumeID,
		"target_iqn": spec.TargetIQN,
		"lun_wwn":    spec.LUNWWN,
	} {
		if value == "" {
			return fmt.Errorf("%s is required", name)
		}
	}
	if spec.DeviceID == 0 {
		return fmt.Errorf("SCSI device_id is required")
	}
	if spec.SizeBytes == 0 {
		return fmt.Errorf("LUN size is required")
	}
	if spec.BackingStore == nil {
		return fmt.Errorf("backing store is required")
	}
	return nil
}
