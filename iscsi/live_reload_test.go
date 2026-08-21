package iscsi

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"testing"
)

func TestLiveReloadAtomicallyReplacesChangedExportAndSkipsUnchanged(t *testing.T) {
	var snapshotCalls int
	source := reloadSourceFuncs{
		changes: func(_ context.Context, after uint64, size int, token string) (RegistryChangePage, error) {
			if size != RegistryReloadPageSize || token != "" {
				t.Fatalf("changed-set request size=%d token=%q", size, token)
			}
			switch after {
			case 0:
				return singleUpsertPage(0, 1, 1, testRegistryExport("export-a", "volume-a", "iscsi-gw-a")), nil
			case 1:
				return singleUpsertPage(1, 2, 2, testRegistryExport("export-a", "volume-b", "iscsi-gw-a")), nil
			case 2:
				return RegistryChangePage{FromRevision: 2, ToRevision: 2, ConfigGeneration: 2, CheckpointRevision: 2}, nil
			default:
				return RegistryChangePage{}, fmt.Errorf("unexpected after revision %d", after)
			}
		},
		snapshot: func(context.Context, uint64, int, string) (RegistrySnapshotPage, error) {
			snapshotCalls++
			return RegistrySnapshotPage{}, errors.New("snapshot must not be called for retained changes")
		},
	}
	generation, err := NewAtomicSupervisorGeneration(64)
	if err != nil {
		t.Fatal(err)
	}
	controller, err := NewLiveReloadController("iscsi-gw-a", 64, source, memoryRuntimePreparer(nil), generation)
	if err != nil {
		t.Fatal(err)
	}

	first, err := controller.ReloadOnce(context.Background())
	if err != nil || first.Outcome != ReloadApply || first.RegistryRevision != 1 {
		t.Fatalf("initial reload = %#v, %v", first, err)
	}
	oldGeneration := generation.Current()
	oldRuntime, ok := oldGeneration.Runtime("export-a")
	if !ok {
		t.Fatal("initial export was not published")
	}
	payload := []byte("old-generation")
	if _, err := oldRuntime.WriteAt(payload, 128); err != nil {
		t.Fatal(err)
	}

	second, err := controller.ReloadOnce(context.Background())
	if err != nil || second.Outcome != ReloadApply || second.RegistryRevision != 2 {
		t.Fatalf("changed reload = %#v, %v", second, err)
	}
	newGeneration := generation.Current()
	if newGeneration == oldGeneration {
		t.Fatal("atomic supervisor pointer did not change")
	}
	if oldGeneration.Summary().Exports[0].State != ExportRuntimeStopped {
		t.Fatalf("old generation was not retired: %#v", oldGeneration.Summary())
	}
	newRuntime, ok := newGeneration.Runtime("export-a")
	if !ok || newRuntime.Snapshot().VolumeID != "volume-b" {
		t.Fatalf("new mapping not published: %#v", newGeneration.Summary())
	}
	got := make([]byte, len(payload))
	if _, err := newRuntime.ReadAt(got, 128); err != nil {
		t.Fatal(err)
	}
	if bytes.Equal(got, payload) {
		t.Fatal("new volume exposed the old volume's payload")
	}

	unchanged, err := controller.ReloadOnce(context.Background())
	if err != nil || unchanged.Outcome != ReloadSkip || generation.Current() != newGeneration {
		t.Fatalf("unchanged reload = %#v, %v", unchanged, err)
	}
	if snapshotCalls != 0 {
		t.Fatalf("changed-set path performed %d snapshot scans", snapshotCalls)
	}
	summary := controller.Summary()
	if summary.RegistryReloadCount != 2 || summary.RegistrySkippedCount != 1 || summary.RegistryReloadRevision != 2 || summary.RegistryChangedExportCount != 2 {
		t.Fatalf("reload summary = %#v", summary)
	}
	if summary.MaxExportsPerProcess != 64 {
		t.Fatalf("max exports per process = %d, want 64", summary.MaxExportsPerProcess)
	}
}

func TestLiveReloadFailureKeepsOldGenerationAndCheckpoint(t *testing.T) {
	failApply := false
	source := reloadSourceFuncs{
		changes: func(_ context.Context, after uint64, _ int, _ string) (RegistryChangePage, error) {
			if after == 0 {
				return singleUpsertPage(0, 1, 1, testRegistryExport("export-a", "volume-a", "iscsi-gw-a")), nil
			}
			return singleUpsertPage(1, 2, 2, testRegistryExport("export-a", "volume-b", "iscsi-gw-a")), nil
		},
		snapshot: func(context.Context, uint64, int, string) (RegistrySnapshotPage, error) {
			return RegistrySnapshotPage{}, errors.New("unexpected snapshot")
		},
	}
	generation, err := NewAtomicSupervisorGeneration(64)
	if err != nil {
		t.Fatal(err)
	}
	applier := ExportGenerationApplyFunc(func(ctx context.Context, exports map[string]PreparedExportRuntime) error {
		if failApply {
			return errors.New("injected generation apply failure")
		}
		return generation.Apply(ctx, exports)
	})
	controller, err := NewLiveReloadController("iscsi-gw-a", 64, source, memoryRuntimePreparer(nil), applier)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := controller.ReloadOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	oldGeneration := generation.Current()
	failApply = true
	if _, err := controller.ReloadOnce(context.Background()); err == nil || !strings.Contains(err.Error(), "injected generation apply failure") {
		t.Fatalf("apply error = %v", err)
	}
	if generation.Current() != oldGeneration {
		t.Fatal("failed apply replaced the serving generation")
	}
	summary := controller.Summary()
	if summary.RegistryReloadRevision != 1 || summary.RegistryReloadCount != 1 || summary.RegistryApplyFailureCount != 1 {
		t.Fatalf("failed apply advanced checkpoint: %#v", summary)
	}
	if summary.RegistryReloadFirstError == "" || summary.RegistryReloadLastError == "" {
		t.Fatalf("failed apply did not record first/last error: %#v", summary)
	}

	failApply = false
	if result, err := controller.ReloadOnce(context.Background()); err != nil || result.RegistryRevision != 2 {
		t.Fatalf("retry = %#v, %v", result, err)
	}
	if generation.Current() == oldGeneration {
		t.Fatal("successful retry did not replace the generation")
	}
}

func TestLiveReloadGapUsesRevisionPinnedBoundedResync(t *testing.T) {
	requests := make([]uint64, 0, 8)
	all := make([]RegistryExportState, 0, 1000)
	for i := 0; i < 1000; i++ {
		gatewayID := "iscsi-gw-other"
		if i < 64 {
			gatewayID = "iscsi-gw-a"
		}
		all = append(all, testRegistryExport(fmt.Sprintf("export-%04d", i), fmt.Sprintf("volume-%04d", i), gatewayID))
	}
	source := reloadSourceFuncs{
		changes: func(_ context.Context, after uint64, _ int, _ string) (RegistryChangePage, error) {
			return RegistryChangePage{
				FromRevision: after, ToRevision: 40, ConfigGeneration: 8,
				ResyncRequired: true, ResyncReason: "checkpoint below retained floor", ChangeFloorRevision: 12,
			}, nil
		},
		snapshot: func(_ context.Context, revision uint64, size int, token string) (RegistrySnapshotPage, error) {
			requests = append(requests, revision)
			if size != RegistryReloadPageSize {
				return RegistrySnapshotPage{}, fmt.Errorf("page size %d", size)
			}
			start := 0
			if token != "" {
				var err error
				start, err = strconv.Atoi(token)
				if err != nil {
					return RegistrySnapshotPage{}, err
				}
			}
			end := start + size
			if end > len(all) {
				end = len(all)
			}
			next := ""
			if end < len(all) {
				next = strconv.Itoa(end)
			}
			return RegistrySnapshotPage{RegistryRevision: 40, ConfigGeneration: 8, Exports: all[start:end], NextPageToken: next}, nil
		},
	}
	generation, err := NewAtomicSupervisorGeneration(64)
	if err != nil {
		t.Fatal(err)
	}
	controller, err := NewLiveReloadController("iscsi-gw-a", 64, source, memoryRuntimePreparer(nil), generation)
	if err != nil {
		t.Fatal(err)
	}
	result, err := controller.ReloadOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !result.ResyncApplied || result.RegistryRevision != 40 || result.ChangedExports != 64 {
		t.Fatalf("resync result = %#v", result)
	}
	if len(requests) != 8 || requests[0] != 0 {
		t.Fatalf("snapshot requests = %v", requests)
	}
	for i, revision := range requests[1:] {
		if revision != 40 {
			t.Fatalf("snapshot page %d was not revision-pinned: %d", i+2, revision)
		}
	}
	summary := controller.Summary()
	if summary.RegistryResyncCount != 1 || summary.RegistryReloadPageCount != 9 || summary.RegistryReloadPageSize != 128 || summary.RegistryReloadRevision != 40 {
		t.Fatalf("resync summary = %#v", summary)
	}
	if got := generation.Current().Summary(); got.ServedExportCount != 64 || got.ServedVolumeCount != 64 {
		t.Fatalf("resynced generation = %#v", got)
	}
}

func TestLiveReloadAcceptsMonotonicMultiPageChangedSet(t *testing.T) {
	source := reloadSourceFuncs{
		changes: func(_ context.Context, after uint64, _ int, token string) (RegistryChangePage, error) {
			switch token {
			case "":
				export := testRegistryExport("export-a", "volume-a", "iscsi-gw-a")
				return RegistryChangePage{
					FromRevision: after, ToRevision: 2, ConfigGeneration: 5, NextPageToken: "page-2",
					Changes: []RegistryExportChange{{RegistryRevision: 2, ConfigGeneration: 5, Operation: "upsert", ExportID: export.ExportID, Export: &export}},
				}, nil
			case "page-2":
				export := testRegistryExport("export-b", "volume-b", "iscsi-gw-a")
				return RegistryChangePage{
					FromRevision: after, ToRevision: 3, ConfigGeneration: 5, CheckpointRevision: 3,
					Changes: []RegistryExportChange{{RegistryRevision: 3, ConfigGeneration: 5, Operation: "upsert", ExportID: export.ExportID, Export: &export}},
				}, nil
			default:
				return RegistryChangePage{}, fmt.Errorf("unexpected page token %q", token)
			}
		},
		snapshot: func(context.Context, uint64, int, string) (RegistrySnapshotPage, error) {
			return RegistrySnapshotPage{}, errors.New("unexpected snapshot")
		},
	}
	generation, _ := NewAtomicSupervisorGeneration(64)
	controller, err := NewLiveReloadController("iscsi-gw-a", 64, source, memoryRuntimePreparer(nil), generation)
	if err != nil {
		t.Fatal(err)
	}
	result, err := controller.ReloadOnce(context.Background())
	if err != nil || result.RegistryRevision != 3 || result.ChangedExports != 2 {
		t.Fatalf("multi-page reload = %#v, %v", result, err)
	}
	if got := generation.Current().Summary().ServedExportCount; got != 2 {
		t.Fatalf("served exports = %d, want 2", got)
	}
	if got := controller.Summary().RegistryReloadPageCount; got != 2 {
		t.Fatalf("reload page count = %d, want 2", got)
	}
}

func TestLiveReloadPublishesInitialEmptyRevisionZero(t *testing.T) {
	source := reloadSourceFuncs{
		changes: func(_ context.Context, after uint64, _ int, _ string) (RegistryChangePage, error) {
			return RegistryChangePage{FromRevision: after, ToRevision: 0, ConfigGeneration: 0, CheckpointRevision: 0}, nil
		},
		snapshot: func(context.Context, uint64, int, string) (RegistrySnapshotPage, error) {
			return RegistrySnapshotPage{}, errors.New("unexpected snapshot")
		},
	}
	generation, _ := NewAtomicSupervisorGeneration(64)
	controller, err := NewLiveReloadController("iscsi-gw-a", 64, source, memoryRuntimePreparer(nil), generation)
	if err != nil {
		t.Fatal(err)
	}
	result, err := controller.ReloadOnce(context.Background())
	if err != nil || result.Outcome != ReloadApply {
		t.Fatalf("initial empty reload = %#v, %v", result, err)
	}
	if generation.Current() == nil || generation.Current().Summary().ServedExportCount != 0 {
		t.Fatalf("empty generation was not published: %#v", generation.Current())
	}
	if !controller.Summary().ISCSIRegistryLiveReloadReady {
		t.Fatal("valid empty registry was not reported ready")
	}
}

func TestLiveReloadSerializesConcurrentReloads(t *testing.T) {
	var mu sync.Mutex
	maxActive := 0
	active := 0
	source := reloadSourceFuncs{
		changes: func(_ context.Context, after uint64, _ int, _ string) (RegistryChangePage, error) {
			mu.Lock()
			active++
			if active > maxActive {
				maxActive = active
			}
			active--
			mu.Unlock()
			return RegistryChangePage{FromRevision: after, ToRevision: after, ConfigGeneration: 0, CheckpointRevision: after}, nil
		},
		snapshot: func(context.Context, uint64, int, string) (RegistrySnapshotPage, error) {
			return RegistrySnapshotPage{}, errors.New("unexpected snapshot")
		},
	}
	generation, _ := NewAtomicSupervisorGeneration(64)
	controller, err := NewLiveReloadController("iscsi-gw-a", 64, source, memoryRuntimePreparer(nil), generation)
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = controller.ReloadOnce(context.Background())
		}()
	}
	wg.Wait()
	if maxActive != 1 {
		t.Fatalf("concurrent reload fetches = %d", maxActive)
	}
}

type reloadSourceFuncs struct {
	changes  func(context.Context, uint64, int, string) (RegistryChangePage, error)
	snapshot func(context.Context, uint64, int, string) (RegistrySnapshotPage, error)
}

func (f reloadSourceFuncs) GetChanges(ctx context.Context, after uint64, size int, token string) (RegistryChangePage, error) {
	return f.changes(ctx, after, size, token)
}

func (f reloadSourceFuncs) ListSnapshot(ctx context.Context, revision uint64, size int, token string) (RegistrySnapshotPage, error) {
	return f.snapshot(ctx, revision, size, token)
}

func singleUpsertPage(from, to, generation uint64, export RegistryExportState) RegistryChangePage {
	return RegistryChangePage{
		FromRevision: from, ToRevision: to, ConfigGeneration: generation, CheckpointRevision: to,
		Changes: []RegistryExportChange{{RegistryRevision: to, ConfigGeneration: generation, Operation: "upsert", ExportID: export.ExportID, Export: &export}},
	}
}

func testRegistryExport(exportID, volumeID, gatewayID string) RegistryExportState {
	return RegistryExportState{
		ExportID: exportID, VolumeID: volumeID,
		TargetIQN: "iqn.2026-08.io.namrbd:" + exportID,
		LUNWWN:    LUNWWN(exportID), Enabled: true,
		ActiveGatewayID: gatewayID, ExportEpoch: 1,
		ReadWriteAllowed: true, WriteAdmissionState: "read_write",
	}
}

func memoryRuntimePreparer(fail map[string]error) ExportRuntimePrepareFunc {
	var deviceID uint64
	return func(_ context.Context, state RegistryExportState) (PreparedExportRuntime, error) {
		if err := fail[state.ExportID]; err != nil {
			return PreparedExportRuntime{}, err
		}
		deviceID++
		lun, err := NewMemoryLUN(4096)
		if err != nil {
			return PreparedExportRuntime{}, err
		}
		return PreparedExportRuntime{
			State: state,
			Spec: ExportRuntimeSpec{
				ExportID: state.ExportID, VolumeID: state.VolumeID,
				TargetIQN: state.TargetIQN, LUNID: state.LUNID, LUNWWN: state.LUNWWN,
				DeviceID: deviceID, SizeBytes: 4096, ActiveGatewayID: state.ActiveGatewayID,
				ExportEpoch: state.ExportEpoch, WriteAdmissionState: state.WriteAdmissionState,
				BackingStore: lun,
			},
		}, nil
	}
}
