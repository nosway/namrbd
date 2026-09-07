package metadata

import (
	"context"
	"errors"
	"fmt"
	"testing"
)

type volumeCatalogBatchGuard struct {
	base       *fakeTransactionalKV
	callCount  int
	maximumKey int
}

func (g *volumeCatalogBatchGuard) Get(ctx context.Context, key string) ([]byte, bool, error) {
	return g.base.Get(ctx, key)
}

func (g *volumeCatalogBatchGuard) BatchGet(ctx context.Context, keys []string) (map[string][]byte, error) {
	g.callCount++
	g.maximumKey = max(g.maximumKey, len(keys))
	values := make(map[string][]byte, len(keys))
	for _, key := range keys {
		raw, found, err := g.base.Get(ctx, key)
		if err != nil {
			return nil, err
		}
		if found {
			values[key] = raw
		}
	}
	return values, nil
}

func TestVolumeCatalogRebuildPagingFiltersAndRevisionFence(t *testing.T) {
	ctx := context.Background()
	repo := NewRepository(newFakeTransactionalKV(), "phase-ad-volume-catalog")
	fixtures := []struct {
		id       string
		status   VolumeStatus
		backend  string
		topology string
	}{
		{id: "00a1b2c1", status: VolumeStatusHealthy, backend: RedundancyBackendReplicated, topology: "rack"},
		{id: "00a1b2c2", status: VolumeStatusDegraded, backend: RedundancyBackendEC, topology: "zone"},
		{id: "00a1b2c3", status: VolumeStatusHealthy, backend: RedundancyBackendEC, topology: "zone"},
	}
	for _, fixture := range fixtures {
		if err := repo.PutVolumeSpec(ctx, VolumeSpecRecord{
			VolumeID: fixture.id, SizeBytes: 1 << 20, BlockSize: 4096,
			RedundancyBackend: fixture.backend, TopologyMode: fixture.topology,
		}); err != nil {
			t.Fatalf("PutVolumeSpec(%s): %v", fixture.id, err)
		}
		if err := repo.PutVolumeState(ctx, VolumeState{
			VolumeID: fixture.id, Epoch: 1, Revision: 1, Status: fixture.status,
			RedundancyBackend: fixture.backend, TopologyMode: fixture.topology,
		}); err != nil {
			t.Fatalf("PutVolumeState(%s): %v", fixture.id, err)
		}
	}
	if _, err := repo.ListVolumeCatalogPage(ctx, "", 2, 0, VolumeCatalogFilter{}); !errors.Is(err, ErrVolumeCatalogRebuildRequired) {
		t.Fatalf("pre-rebuild list error=%v", err)
	}
	firstRebuild, err := repo.RunVolumeCatalogRebuildPage(ctx, 2)
	if err != nil {
		t.Fatal(err)
	}
	if firstRebuild.Ready || firstRebuild.InputCount != 2 || firstRebuild.NextCursor == "" || firstRebuild.RangePageCount != 1 {
		t.Fatalf("first rebuild=%+v", firstRebuild)
	}
	secondRebuild, err := repo.RunVolumeCatalogRebuildPage(ctx, 2)
	if err != nil {
		t.Fatal(err)
	}
	if !secondRebuild.Ready || secondRebuild.InputCount != 1 || secondRebuild.ScannedSpecCount != 3 {
		t.Fatalf("second rebuild=%+v", secondRebuild)
	}
	page, err := repo.ListVolumeCatalogPage(ctx, "", 2, secondRebuild.Revision, VolumeCatalogFilter{
		Status: VolumeStatusDegraded, RedundancyBackend: RedundancyBackendEC, TopologyMode: "zone",
	})
	if err != nil {
		t.Fatal(err)
	}
	if page.ScannedCount != 2 || len(page.Entries) != 1 || page.Entries[0].State.VolumeID != "00a1b2c2" || page.NextCursor == "" {
		t.Fatalf("filtered page=%+v", page)
	}
	state := fixtures[1]
	if err := repo.PutVolumeState(ctx, VolumeState{
		VolumeID: state.id, Epoch: 1, Revision: 2, Status: VolumeStatusHealthy,
		RedundancyBackend: state.backend, TopologyMode: state.topology,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.ListVolumeCatalogPage(ctx, page.NextCursor, 2, secondRebuild.Revision, VolumeCatalogFilter{}); !errors.Is(err, ErrVolumeCatalogRevision) {
		t.Fatalf("stale revision error=%v", err)
	}
	catalog, err := repo.GetVolumeCatalogState(ctx)
	if err != nil {
		t.Fatal(err)
	}
	stableRevision := catalog.Revision
	if err := repo.PutVolumeState(ctx, VolumeState{
		VolumeID: state.id, Epoch: 1, Revision: 3, Status: VolumeStatusHealthy,
		RedundancyBackend: state.backend, TopologyMode: state.topology,
	}); err != nil {
		t.Fatal(err)
	}
	catalog, err = repo.GetVolumeCatalogState(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if catalog.Revision != stableRevision {
		t.Fatalf("ordinary volume revision advanced catalog %d -> %d", stableRevision, catalog.Revision)
	}
}

func TestVolumeCatalogRejectsOversizedPage(t *testing.T) {
	repo := NewRepository(newFakeTransactionalKV(), "phase-ad-volume-catalog-limit")
	if _, err := repo.RunVolumeCatalogRebuildPage(context.Background(), VolumeCatalogPageMaximum+1); !errors.Is(err, ErrVolumeCatalogInvalid) {
		t.Fatalf("oversized rebuild error=%v", err)
	}
	if _, err := repo.ListVolumeCatalogPage(context.Background(), "", VolumeCatalogPageMaximum+1, 0, VolumeCatalogFilter{}); !errors.Is(err, ErrVolumeCatalogInvalid) {
		t.Fatalf("oversized list error=%v", err)
	}
}

func TestVolumeCatalogEntryBatchGetIsBounded(t *testing.T) {
	ctx := context.Background()
	root := "phase-ad-volume-catalog-batch"
	base := newFakeTransactionalKV()
	repo := NewRepository(base, root)
	keys := make([]string, 0, 65)
	for i := 1; i <= 65; i++ {
		volumeID := fmt.Sprintf("%08x", i)
		if err := repo.PutVolumeSpec(ctx, VolumeSpecRecord{VolumeID: volumeID, SizeBytes: 4096, BlockSize: 4096}); err != nil {
			t.Fatal(err)
		}
		if err := repo.PutVolumeState(ctx, VolumeState{VolumeID: volumeID, Epoch: 1, Revision: 1, Status: VolumeStatusHealthy}); err != nil {
			t.Fatal(err)
		}
		keys = append(keys, volumeSpecKey(root, volumeID))
	}
	guard := &volumeCatalogBatchGuard{base: base}
	entries, err := readVolumeCatalogEntries(ctx, guard, root, keys, VolumeCatalogFilter{})
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 65 || guard.callCount != 2 || guard.maximumKey != VolumeCatalogBatchMaximum {
		t.Fatalf("entries=%d calls=%d maximum_keys=%d", len(entries), guard.callCount, guard.maximumKey)
	}
}
