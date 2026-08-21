package iscsi

import (
	"bytes"
	"errors"
	"fmt"
	"testing"
)

func TestMultiExportSupervisorServesSixtyFourWithPerExportIsolation(t *testing.T) {
	const exportCount = 64
	supervisor, err := NewMultiExportSupervisor(0)
	if err != nil {
		t.Fatal(err)
	}
	specs := make([]ExportRuntimeSpec, 0, exportCount)
	for i := 0; i < exportCount; i++ {
		lun, err := NewMemoryLUN(4096)
		if err != nil {
			t.Fatal(err)
		}
		store := remoteStore(lun)
		if i == 17 {
			store = &writeFailingStore{RemoteBackingStore: store, err: errors.New("injected export-17 write failure")}
		}
		exportID := fmt.Sprintf("export-%02d", i)
		specs = append(specs, ExportRuntimeSpec{
			ExportID:            exportID,
			VolumeID:            fmt.Sprintf("volume-%02d", i),
			TargetIQN:           "iqn.2026-08.io.namrbd:" + exportID,
			LUNID:               0,
			LUNWWN:              LUNWWN(exportID),
			DeviceID:            uint64(i + 1),
			SizeBytes:           4096,
			ActiveGatewayID:     "iscsi-gw-a",
			ExportEpoch:         9,
			WriteAdmissionState: "read_write",
			InitialMemoryBytes:  256,
			BackingStore:        store,
		})
	}
	if err := supervisor.Install(specs); err != nil {
		t.Fatal(err)
	}
	supervisor.MarkServing()

	for i := 0; i < exportCount; i++ {
		exportID := fmt.Sprintf("export-%02d", i)
		runtime, ok := supervisor.Runtime(exportID)
		if !ok {
			t.Fatalf("runtime %s not installed", exportID)
		}
		payload := []byte(fmt.Sprintf("payload-%02d", i))
		runtime.SessionOpened()
		_, writeErr := runtime.WriteAt(payload, 512)
		if i == 17 {
			if writeErr == nil || writeErr.Error() != "injected export-17 write failure" {
				t.Fatalf("faulted export write error = %v", writeErr)
			}
			runtime.SessionClosed()
			continue
		}
		if writeErr != nil {
			t.Fatalf("write %s: %v", exportID, writeErr)
		}
		if _, err := runtime.Sync(); err != nil {
			t.Fatalf("flush %s: %v", exportID, err)
		}
		got := make([]byte, len(payload))
		if _, err := runtime.ReadAt(got, 512); err != nil {
			t.Fatalf("read %s: %v", exportID, err)
		}
		if !bytes.Equal(got, payload) {
			t.Fatalf("readback %s = %q, want %q", exportID, got, payload)
		}
		runtime.SessionClosed()
	}

	failed, _ := supervisor.Runtime("export-17")
	healthy, _ := supervisor.Runtime("export-18")
	failedSnapshot := failed.Snapshot()
	healthySnapshot := healthy.Snapshot()
	if failedSnapshot.State != ExportRuntimeFailed || failedSnapshot.ErrorCount != 1 {
		t.Fatalf("faulted runtime not isolated as failed: %#v", failedSnapshot)
	}
	if healthySnapshot.State != ExportRuntimeServing || healthySnapshot.ErrorCount != 0 || healthySnapshot.WriteCount != 1 || healthySnapshot.ReadCount != 1 || healthySnapshot.FlushCount != 1 {
		t.Fatalf("healthy neighbor was damaged: %#v", healthySnapshot)
	}
	if healthySnapshot.SessionCount != 0 || healthySnapshot.InFlightIO != 0 || healthySnapshot.MemoryBytes != 256 || healthySnapshot.PeakMemoryBytes <= 256 {
		t.Fatalf("per-export resource accounting is wrong: %#v", healthySnapshot)
	}

	summary := supervisor.Summary()
	if summary.ServedExportCount != exportCount || summary.ServedVolumeCount != exportCount || summary.MaxExportsPerProcess != exportCount {
		t.Fatalf("multi-export summary = %#v", summary)
	}
	if summary.MultiExportFirstError != "" || summary.MultiExportLastError != "" {
		t.Fatalf("an isolated I/O error became a process install error: %#v", summary)
	}
}

func TestMultiExportSupervisorRejectsCapBeforePublishing(t *testing.T) {
	supervisor, err := NewMultiExportSupervisor(64)
	if err != nil {
		t.Fatal(err)
	}
	specs := make([]ExportRuntimeSpec, 65)
	for i := range specs {
		lun, err := NewMemoryLUN(512)
		if err != nil {
			t.Fatal(err)
		}
		exportID := fmt.Sprintf("export-%02d", i)
		specs[i] = ExportRuntimeSpec{
			ExportID:     exportID,
			VolumeID:     fmt.Sprintf("volume-%02d", i),
			TargetIQN:    "iqn.2026-08.io.namrbd:" + exportID,
			LUNWWN:       LUNWWN(exportID),
			DeviceID:     uint64(i + 1),
			SizeBytes:    512,
			BackingStore: lun,
		}
	}
	err = supervisor.Install(specs)
	if err == nil || err.Error() != "registry selected 65 exports, exceeding max_exports_per_process=64" {
		t.Fatalf("cap error = %v", err)
	}
	summary := supervisor.Summary()
	if summary.ServedExportCount != 0 || summary.ISCSIGatewayMultiExportReady {
		t.Fatalf("over-cap snapshot partially published: %#v", summary)
	}
	if summary.MultiExportFirstError != err.Error() || summary.MultiExportLastError != err.Error() {
		t.Fatalf("cap evidence missing: %#v", summary)
	}
}

type remoteStore interface {
	ReadAt([]byte, int64) (int, error)
	WriteAt([]byte, int64) (int, error)
	Sync() (int, error)
	Unmap(int64, int64) (int, error)
}

type writeFailingStore struct {
	RemoteBackingStore remoteStore
	err                error
}

func (s *writeFailingStore) ReadAt(p []byte, off int64) (int, error) {
	return s.RemoteBackingStore.ReadAt(p, off)
}

func (s *writeFailingStore) WriteAt([]byte, int64) (int, error) { return 0, s.err }

func (s *writeFailingStore) Sync() (int, error) { return s.RemoteBackingStore.Sync() }

func (s *writeFailingStore) Unmap(off, length int64) (int, error) {
	return s.RemoteBackingStore.Unmap(off, length)
}
