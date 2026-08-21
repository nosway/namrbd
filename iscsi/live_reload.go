package iscsi

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const RegistryReloadPageSize = 128

type RegistryExportState struct {
	ExportID            string
	VolumeID            string
	TargetIQN           string
	LUNID               uint64
	LUNWWN              string
	Enabled             bool
	ActiveGatewayID     string
	StandbyGatewayIDs   []string
	ExportLeaseID       string
	ExportEpoch         uint64
	ReadWriteAllowed    bool
	WriteAdmissionState string
}

func (e RegistryExportState) AssignedTo(gatewayID string) bool {
	gatewayID = strings.TrimSpace(gatewayID)
	if gatewayID == "" || !e.Enabled {
		return false
	}
	if strings.TrimSpace(e.ActiveGatewayID) == gatewayID {
		return true
	}
	for _, standby := range e.StandbyGatewayIDs {
		if strings.TrimSpace(standby) == gatewayID {
			return true
		}
	}
	return false
}

type RegistryExportChange struct {
	RegistryRevision uint64
	ConfigGeneration uint64
	Operation        string
	ExportID         string
	Export           *RegistryExportState
}

type RegistryChangePage struct {
	FromRevision        uint64
	ToRevision          uint64
	ConfigGeneration    uint64
	Changes             []RegistryExportChange
	NextPageToken       string
	CheckpointRevision  uint64
	ResyncRequired      bool
	ResyncReason        string
	ChangeFloorRevision uint64
}

type RegistrySnapshotPage struct {
	RegistryRevision uint64
	ConfigGeneration uint64
	Exports          []RegistryExportState
	NextPageToken    string
}

type RegistryReloadSource interface {
	GetChanges(context.Context, uint64, int, string) (RegistryChangePage, error)
	ListSnapshot(context.Context, uint64, int, string) (RegistrySnapshotPage, error)
}

type PreparedExportRuntime struct {
	State RegistryExportState
	Spec  ExportRuntimeSpec
	Close func() error
}

type ExportRuntimePreparer interface {
	Prepare(context.Context, RegistryExportState) (PreparedExportRuntime, error)
}

type ExportGenerationApplier interface {
	Apply(context.Context, map[string]PreparedExportRuntime) error
}

type ExportRuntimePrepareFunc func(context.Context, RegistryExportState) (PreparedExportRuntime, error)

func (f ExportRuntimePrepareFunc) Prepare(ctx context.Context, state RegistryExportState) (PreparedExportRuntime, error) {
	return f(ctx, state)
}

type ExportGenerationApplyFunc func(context.Context, map[string]PreparedExportRuntime) error

func (f ExportGenerationApplyFunc) Apply(ctx context.Context, exports map[string]PreparedExportRuntime) error {
	return f(ctx, exports)
}

type LiveReloadResult struct {
	Outcome          ReloadOutcome
	RegistryRevision uint64
	ConfigGeneration uint64
	ChangedExports   int
	ResyncApplied    bool
	Reason           string
}

type LiveReloadSummary struct {
	ReloadSnapshot
	ISCSIRegistryLiveReloadReady bool   `json:"iscsi_registry_live_reload_ready"`
	RegistryReloadBounded        bool   `json:"iscsi_registry_reload_bounded"`
	RegistryReloadPageSize       int    `json:"iscsi_registry_reload_page_size"`
	RegistryReloadPageCount      uint64 `json:"iscsi_registry_reload_page_count"`
	RegistryChangedExportCount   uint64 `json:"iscsi_registry_changed_export_count"`
	RegistryResyncCount          uint64 `json:"registry_resync_count"`
	RegistryApplyFailureCount    uint64 `json:"registry_apply_failure_count"`
	RegistryReloadFirstError     string `json:"registry_reload_first_error,omitempty"`
	RegistryReloadLastError      string `json:"registry_reload_last_error,omitempty"`
	ServedExportCount            int    `json:"served_export_count"`
	MaxExportsPerProcess         int    `json:"max_exports_per_process"`
}

// LiveReloadController serializes fetch/prepare/apply so one serving
// checkpoint always names one complete supervisor generation.
type LiveReloadController struct {
	mu sync.Mutex

	gatewayID  string
	maxExports int
	source     RegistryReloadSource
	preparer   ExportRuntimePreparer
	applier    ExportGenerationApplier
	reload     ReloadState
	current    map[string]PreparedExportRuntime

	pageCount     atomic.Uint64
	changedCount  atomic.Uint64
	resyncCount   atomic.Uint64
	applyFailures atomic.Uint64
	initialized   atomic.Bool
	errorMu       sync.Mutex
	firstError    string
	lastError     string
}

func NewLiveReloadController(gatewayID string, maxExports int, source RegistryReloadSource, preparer ExportRuntimePreparer, applier ExportGenerationApplier) (*LiveReloadController, error) {
	gatewayID = strings.TrimSpace(gatewayID)
	if gatewayID == "" {
		return nil, fmt.Errorf("iSCSI gateway id is required")
	}
	if maxExports == 0 {
		maxExports = DefaultMaxExportsPerProcess
	}
	if maxExports < 1 || maxExports > DefaultMaxExportsPerProcess {
		return nil, fmt.Errorf("max_exports_per_process must be 1..%d, got %d", DefaultMaxExportsPerProcess, maxExports)
	}
	if source == nil || preparer == nil || applier == nil {
		return nil, fmt.Errorf("reload source, runtime preparer, and generation applier are required")
	}
	return &LiveReloadController{
		gatewayID: gatewayID, maxExports: maxExports,
		source: source, preparer: preparer, applier: applier,
		current: make(map[string]PreparedExportRuntime, maxExports),
	}, nil
}

func (c *LiveReloadController) ReloadOnce(ctx context.Context) (LiveReloadResult, error) {
	if c == nil {
		return LiveReloadResult{}, fmt.Errorf("live reload controller is nil")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	afterRevision := c.reload.Snapshot().RegistryReloadRevision
	changes, checkpoint, generation, pages, resync, reason, err := c.fetchChanges(ctx, afterRevision)
	c.pageCount.Add(uint64(pages))
	if err != nil {
		return LiveReloadResult{}, c.fail(err)
	}
	if resync {
		result, err := c.applyResync(ctx, reason)
		if err != nil {
			return result, c.fail(err)
		}
		return result, nil
	}

	decision := c.reload.Decide(checkpoint, generation)
	if !c.initialized.Load() && decision.Outcome == ReloadSkip {
		decision.Outcome = ReloadApply
		decision.Reason = "publish the initial registry generation"
	}
	if decision.Outcome != ReloadApply {
		return LiveReloadResult{
			Outcome: decision.Outcome, RegistryRevision: checkpoint,
			ConfigGeneration: generation, Reason: decision.Reason,
		}, nil
	}
	finalChanges, err := collapseRegistryChanges(changes)
	if err != nil {
		return LiveReloadResult{}, c.fail(err)
	}
	result, err := c.applyChanges(ctx, finalChanges, checkpoint, generation)
	if err != nil {
		return result, c.fail(err)
	}
	c.changedCount.Add(uint64(len(finalChanges)))
	return result, nil
}

// Run polls the bounded changed-set immediately and then at the configured
// cadence. Startup fails if no valid initial generation can be loaded. Later
// errors are observed while the last valid generation continues serving.
func (c *LiveReloadController) Run(ctx context.Context, interval time.Duration, observe func(error)) error {
	if interval <= 0 {
		return fmt.Errorf("registry reload interval must be positive")
	}
	if _, err := c.ReloadOnce(ctx); err != nil {
		return err
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			if _, err := c.ReloadOnce(ctx); err != nil && observe != nil {
				observe(err)
			}
		}
	}
}

func (c *LiveReloadController) Close() {
	if c == nil {
		return
	}
	c.mu.Lock()
	closePreparedRuntimes(c.current)
	c.current = make(map[string]PreparedExportRuntime)
	c.mu.Unlock()
}

func (c *LiveReloadController) fetchChanges(ctx context.Context, afterRevision uint64) ([]RegistryExportChange, uint64, uint64, int, bool, string, error) {
	var changes []RegistryExportChange
	pageToken := ""
	lastToRevision := afterRevision
	generation := uint64(0)
	pages := 0
	for {
		page, err := c.source.GetChanges(ctx, afterRevision, RegistryReloadPageSize, pageToken)
		if err != nil {
			return nil, 0, 0, pages, false, "", err
		}
		pages++
		if len(page.Changes) > RegistryReloadPageSize {
			return nil, 0, 0, pages, false, "", fmt.Errorf("changed-set page has %d exports, exceeding %d", len(page.Changes), RegistryReloadPageSize)
		}
		if page.ToRevision < afterRevision {
			c.reload.Decide(page.ToRevision, page.ConfigGeneration)
			return nil, 0, 0, pages, false, "", fmt.Errorf("changed-set revision %d is older than serving revision %d", page.ToRevision, afterRevision)
		}
		if page.FromRevision != afterRevision {
			return nil, 0, 0, pages, false, "", fmt.Errorf("changed-set page starts at revision %d, want %d", page.FromRevision, afterRevision)
		}
		if pages == 1 {
			generation = page.ConfigGeneration
		} else if page.ConfigGeneration != generation {
			return nil, 0, 0, pages, false, "", fmt.Errorf("changed-set config generation changed between pages: %d, want %d", page.ConfigGeneration, generation)
		}
		if page.ToRevision < lastToRevision {
			return nil, 0, 0, pages, false, "", fmt.Errorf("changed-set to_revision moved backwards from %d to %d", lastToRevision, page.ToRevision)
		}
		for _, change := range page.Changes {
			if change.RegistryRevision <= afterRevision || change.RegistryRevision > page.ToRevision {
				return nil, 0, 0, pages, false, "", fmt.Errorf("export %q change revision %d is outside (%d,%d]", change.ExportID, change.RegistryRevision, afterRevision, page.ToRevision)
			}
			if change.ConfigGeneration != 0 && change.ConfigGeneration > generation {
				return nil, 0, 0, pages, false, "", fmt.Errorf("export %q change generation %d exceeds page generation %d", change.ExportID, change.ConfigGeneration, generation)
			}
		}
		lastToRevision = page.ToRevision
		if page.ResyncRequired {
			if page.NextPageToken != "" || len(page.Changes) != 0 {
				return nil, 0, 0, pages, false, "", fmt.Errorf("resync-required response also contained changes or a next page")
			}
			return nil, 0, generation, pages, true, page.ResyncReason, nil
		}
		changes = append(changes, page.Changes...)
		if page.NextPageToken == "" {
			if page.CheckpointRevision != page.ToRevision {
				return nil, 0, 0, pages, false, "", fmt.Errorf("final changed-set checkpoint %d does not match to_revision %d", page.CheckpointRevision, page.ToRevision)
			}
			return changes, page.CheckpointRevision, generation, pages, false, "", nil
		}
		if page.CheckpointRevision != 0 {
			return nil, 0, 0, pages, false, "", fmt.Errorf("changed-set advanced checkpoint before the final page")
		}
		pageToken = page.NextPageToken
	}
}

func (c *LiveReloadController) applyChanges(ctx context.Context, changes map[string]RegistryExportChange, revision, generation uint64) (LiveReloadResult, error) {
	next := clonePreparedRuntimes(c.current)
	newlyPrepared := make(map[string]PreparedExportRuntime)
	for exportID, change := range changes {
		switch change.Operation {
		case "delete":
			delete(next, exportID)
		case "upsert":
			if change.Export == nil {
				closePreparedRuntimes(newlyPrepared)
				return LiveReloadResult{}, fmt.Errorf("upsert for export %q has no export view", exportID)
			}
			state := normalizeRegistryExportState(*change.Export)
			if !state.AssignedTo(c.gatewayID) {
				delete(next, exportID)
				continue
			}
			prepared, err := c.preparer.Prepare(ctx, state)
			if err != nil {
				closePreparedRuntimes(newlyPrepared)
				return LiveReloadResult{}, fmt.Errorf("prepare export %q: %w", exportID, err)
			}
			if err := validatePreparedRuntime(state, prepared); err != nil {
				closePreparedRuntime(prepared)
				closePreparedRuntimes(newlyPrepared)
				return LiveReloadResult{}, fmt.Errorf("prepare export %q: %w", exportID, err)
			}
			prepared.State = state
			newlyPrepared[exportID] = prepared
			next[exportID] = prepared
		default:
			closePreparedRuntimes(newlyPrepared)
			return LiveReloadResult{}, fmt.Errorf("unsupported registry export operation %q", change.Operation)
		}
	}
	if len(next) > c.maxExports {
		closePreparedRuntimes(newlyPrepared)
		return LiveReloadResult{}, fmt.Errorf("registry selected %d exports, exceeding max_exports_per_process=%d", len(next), c.maxExports)
	}
	if err := c.applier.Apply(ctx, next); err != nil {
		c.applyFailures.Add(1)
		closePreparedRuntimes(newlyPrepared)
		return LiveReloadResult{}, fmt.Errorf("apply registry generation %d: %w", revision, err)
	}
	closeReplacedRuntimes(c.current, next)
	c.current = next
	c.reload.Applied(revision, generation)
	c.initialized.Store(true)
	return LiveReloadResult{
		Outcome: ReloadApply, RegistryRevision: revision,
		ConfigGeneration: generation, ChangedExports: len(changes),
		Reason: fmt.Sprintf("atomically applied %d changed exports", len(changes)),
	}, nil
}

func (c *LiveReloadController) applyResync(ctx context.Context, reason string) (LiveReloadResult, error) {
	exports, revision, generation, pages, err := c.fetchSnapshot(ctx)
	c.pageCount.Add(uint64(pages))
	if err != nil {
		return LiveReloadResult{}, err
	}
	decision := c.reload.Decide(revision, generation)
	if decision.Outcome != ReloadApply {
		return LiveReloadResult{Outcome: decision.Outcome, RegistryRevision: revision, ConfigGeneration: generation, Reason: decision.Reason}, nil
	}
	prepared := make(map[string]PreparedExportRuntime, len(exports))
	for exportID, state := range exports {
		runtime, err := c.preparer.Prepare(ctx, state)
		if err != nil {
			closePreparedRuntimes(prepared)
			return LiveReloadResult{}, fmt.Errorf("prepare resync export %q: %w", exportID, err)
		}
		if err := validatePreparedRuntime(state, runtime); err != nil {
			closePreparedRuntime(runtime)
			closePreparedRuntimes(prepared)
			return LiveReloadResult{}, fmt.Errorf("prepare resync export %q: %w", exportID, err)
		}
		runtime.State = state
		prepared[exportID] = runtime
	}
	if err := c.applier.Apply(ctx, prepared); err != nil {
		c.applyFailures.Add(1)
		closePreparedRuntimes(prepared)
		return LiveReloadResult{}, fmt.Errorf("apply resync generation %d: %w", revision, err)
	}
	closePreparedRuntimes(c.current)
	c.current = prepared
	c.reload.Applied(revision, generation)
	c.initialized.Store(true)
	c.resyncCount.Add(1)
	return LiveReloadResult{
		Outcome: ReloadApply, RegistryRevision: revision,
		ConfigGeneration: generation, ChangedExports: len(exports), ResyncApplied: true,
		Reason: fmt.Sprintf("bounded resync applied after changed-set gap: %s", strings.TrimSpace(reason)),
	}, nil
}

func (c *LiveReloadController) fetchSnapshot(ctx context.Context) (map[string]RegistryExportState, uint64, uint64, int, error) {
	exports := make(map[string]RegistryExportState, c.maxExports)
	pageToken := ""
	revision := uint64(0)
	generation := uint64(0)
	pages := 0
	for {
		page, err := c.source.ListSnapshot(ctx, revision, RegistryReloadPageSize, pageToken)
		if err != nil {
			return nil, 0, 0, pages, err
		}
		pages++
		if len(page.Exports) > RegistryReloadPageSize {
			return nil, 0, 0, pages, fmt.Errorf("snapshot page has %d exports, exceeding %d", len(page.Exports), RegistryReloadPageSize)
		}
		if pages == 1 {
			revision = page.RegistryRevision
			generation = page.ConfigGeneration
		} else if page.RegistryRevision != revision || page.ConfigGeneration != generation {
			return nil, 0, 0, pages, fmt.Errorf("snapshot generation changed between pages: %d/%d, want %d/%d", page.RegistryRevision, page.ConfigGeneration, revision, generation)
		}
		for _, raw := range page.Exports {
			state := normalizeRegistryExportState(raw)
			if !state.AssignedTo(c.gatewayID) {
				continue
			}
			if state.ExportID == "" {
				return nil, 0, 0, pages, fmt.Errorf("assigned snapshot export has no export_id")
			}
			if _, exists := exports[state.ExportID]; exists {
				return nil, 0, 0, pages, fmt.Errorf("snapshot contains duplicate export_id %q", state.ExportID)
			}
			exports[state.ExportID] = state
			if len(exports) > c.maxExports {
				return nil, 0, 0, pages, fmt.Errorf("registry selected %d exports, exceeding max_exports_per_process=%d", len(exports), c.maxExports)
			}
		}
		pageToken = page.NextPageToken
		if pageToken == "" {
			return exports, revision, generation, pages, nil
		}
	}
}

func (c *LiveReloadController) Summary() LiveReloadSummary {
	if c == nil {
		return LiveReloadSummary{}
	}
	c.errorMu.Lock()
	firstErr, lastErr := c.firstError, c.lastError
	c.errorMu.Unlock()
	c.mu.Lock()
	servedExportCount := len(c.current)
	c.mu.Unlock()
	return LiveReloadSummary{
		ReloadSnapshot:               c.reload.Snapshot(),
		ISCSIRegistryLiveReloadReady: c.initialized.Load(),
		RegistryReloadBounded:        true,
		RegistryReloadPageSize:       RegistryReloadPageSize,
		RegistryReloadPageCount:      c.pageCount.Load(),
		RegistryChangedExportCount:   c.changedCount.Load(),
		RegistryResyncCount:          c.resyncCount.Load(),
		RegistryApplyFailureCount:    c.applyFailures.Load(),
		RegistryReloadFirstError:     firstErr,
		RegistryReloadLastError:      lastErr,
		ServedExportCount:            servedExportCount,
		MaxExportsPerProcess:         c.maxExports,
	}
}

func (c *LiveReloadController) fail(err error) error {
	if err == nil {
		return nil
	}
	msg := strings.TrimSpace(err.Error())
	c.errorMu.Lock()
	if c.firstError == "" {
		c.firstError = msg
	}
	c.lastError = msg
	c.errorMu.Unlock()
	return err
}

func collapseRegistryChanges(changes []RegistryExportChange) (map[string]RegistryExportChange, error) {
	final := make(map[string]RegistryExportChange)
	for _, change := range changes {
		exportID := strings.TrimSpace(change.ExportID)
		if exportID == "" && change.Export != nil {
			exportID = strings.TrimSpace(change.Export.ExportID)
		}
		if exportID == "" {
			return nil, fmt.Errorf("registry change has no export_id")
		}
		change.ExportID = exportID
		if previous, exists := final[exportID]; exists && change.RegistryRevision < previous.RegistryRevision {
			return nil, fmt.Errorf("changes for export %q are out of revision order", exportID)
		}
		final[exportID] = change
	}
	return final, nil
}

func normalizeRegistryExportState(state RegistryExportState) RegistryExportState {
	state.ExportID = strings.TrimSpace(state.ExportID)
	state.VolumeID = strings.TrimSpace(state.VolumeID)
	state.TargetIQN = strings.TrimSpace(state.TargetIQN)
	state.LUNWWN = strings.TrimSpace(state.LUNWWN)
	state.ActiveGatewayID = strings.TrimSpace(state.ActiveGatewayID)
	state.ExportLeaseID = strings.TrimSpace(state.ExportLeaseID)
	state.WriteAdmissionState = strings.TrimSpace(state.WriteAdmissionState)
	state.StandbyGatewayIDs = append([]string(nil), state.StandbyGatewayIDs...)
	sort.Strings(state.StandbyGatewayIDs)
	return state
}

func validatePreparedRuntime(state RegistryExportState, runtime PreparedExportRuntime) error {
	if runtime.Spec.ExportID != state.ExportID {
		return fmt.Errorf("prepared export_id %q does not match registry export_id %q", runtime.Spec.ExportID, state.ExportID)
	}
	if runtime.Spec.VolumeID != state.VolumeID {
		return fmt.Errorf("prepared volume_id %q does not match registry volume_id %q", runtime.Spec.VolumeID, state.VolumeID)
	}
	if runtime.Spec.TargetIQN != state.TargetIQN || runtime.Spec.LUNID != state.LUNID || runtime.Spec.LUNWWN != state.LUNWWN {
		return fmt.Errorf("prepared target/LUN identity does not match registry export %q", state.ExportID)
	}
	if runtime.Spec.ExportLeaseID != state.ExportLeaseID || runtime.Spec.ExportEpoch != state.ExportEpoch {
		return fmt.Errorf("prepared lease/epoch does not match registry export %q", state.ExportID)
	}
	return nil
}

func clonePreparedRuntimes(src map[string]PreparedExportRuntime) map[string]PreparedExportRuntime {
	dst := make(map[string]PreparedExportRuntime, len(src))
	for exportID, runtime := range src {
		dst[exportID] = runtime
	}
	return dst
}

func closePreparedRuntimes(runtimes map[string]PreparedExportRuntime) {
	for _, runtime := range runtimes {
		closePreparedRuntime(runtime)
	}
}

func closePreparedRuntime(runtime PreparedExportRuntime) {
	if runtime.Close != nil {
		_ = runtime.Close()
	}
}

func closeReplacedRuntimes(previous, next map[string]PreparedExportRuntime) {
	for exportID, old := range previous {
		current, retained := next[exportID]
		if retained && current.Spec.BackingStore == old.Spec.BackingStore {
			continue
		}
		if old.Close != nil {
			_ = old.Close()
		}
	}
}

// AtomicSupervisorGeneration publishes one fully validated supervisor pointer.
// Readers see either the previous pointer or the next pointer.
type AtomicSupervisorGeneration struct {
	maxExports int
	current    atomic.Pointer[MultiExportSupervisor]
}

func NewAtomicSupervisorGeneration(maxExports int) (*AtomicSupervisorGeneration, error) {
	probe, err := NewMultiExportSupervisor(maxExports)
	if err != nil {
		return nil, err
	}
	return &AtomicSupervisorGeneration{maxExports: probe.maxExports}, nil
}

func (g *AtomicSupervisorGeneration) Apply(_ context.Context, exports map[string]PreparedExportRuntime) error {
	if g == nil {
		return fmt.Errorf("atomic supervisor generation is nil")
	}
	supervisor, err := NewMultiExportSupervisor(g.maxExports)
	if err != nil {
		return err
	}
	specs := make([]ExportRuntimeSpec, 0, len(exports))
	for _, runtime := range exports {
		specs = append(specs, runtime.Spec)
	}
	sort.Slice(specs, func(i, j int) bool { return specs[i].ExportID < specs[j].ExportID })
	if err := supervisor.Install(specs); err != nil {
		return err
	}
	supervisor.MarkServing()
	previous := g.current.Swap(supervisor)
	if previous != nil {
		previous.MarkStopped()
	}
	return nil
}

func (g *AtomicSupervisorGeneration) Current() *MultiExportSupervisor {
	if g == nil {
		return nil
	}
	return g.current.Load()
}
