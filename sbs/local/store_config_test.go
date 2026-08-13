package local

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/nosway/namrbd/gateway/service"
)

func TestNormalizeStoreSpecsLegacyPath(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "legacy")
	stores, err := normalizeStoreSpecs(dir, nil)
	if err != nil {
		t.Fatalf("normalizeStoreSpecs: %v", err)
	}
	if len(stores) != 1 {
		t.Fatalf("stores=%d want=1", len(stores))
	}
	if stores[0].ID != DefaultStoreID || stores[0].Path != dir || stores[0].Shards != 1 || stores[0].Weight != 100 {
		t.Fatalf("unexpected legacy store: %+v", stores[0])
	}
}

func TestParseStoreSpec(t *testing.T) {
	spec, err := ParseStoreSpec("/data/nvme0:shards=4,weight=200,id=fast")
	if err != nil {
		t.Fatalf("ParseStoreSpec: %v", err)
	}
	if spec.ID != "fast" || spec.Path != "/data/nvme0" || spec.Shards != 4 || spec.Weight != 200 {
		t.Fatalf("unexpected store spec: %+v", spec)
	}
}

func TestLoadStoreConfigFile(t *testing.T) {
	dir := t.TempDir()
	configPath := filepath.Join(dir, "stores.yaml")
	payload := []byte(`
stores:
  - id: fast
    path: ` + filepath.Join(dir, "fast") + `
    shards: 4
    weight: 200
  - id: bulk
    path: ` + filepath.Join(dir, "bulk") + `
    shards: 2
    weight: 50
`)
	if err := os.WriteFile(configPath, payload, 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	stores, err := LoadStoreConfigFile(configPath)
	if err != nil {
		t.Fatalf("LoadStoreConfigFile: %v", err)
	}
	if len(stores) != 2 {
		t.Fatalf("stores=%d want=2", len(stores))
	}
	if stores[0].ID != "fast" || stores[0].Shards != 4 || stores[0].Weight != 200 {
		t.Fatalf("unexpected first store: %+v", stores[0])
	}
	if stores[1].ID != "bulk" || stores[1].Shards != 2 || stores[1].Weight != 50 {
		t.Fatalf("unexpected second store: %+v", stores[1])
	}
}

func TestLoadStoreConfigFileRejectsInvalidConfig(t *testing.T) {
	dir := t.TempDir()
	tests := []struct {
		name    string
		payload string
	}{
		{
			name: "empty stores",
			payload: `
stores: []
`,
		},
		{
			name: "duplicate store id",
			payload: `
stores:
  - id: dup
    path: ` + filepath.Join(dir, "a") + `
    shards: 1
    weight: 100
  - id: dup
    path: ` + filepath.Join(dir, "b") + `
    shards: 1
    weight: 100
`,
		},
		{
			name: "unknown field",
			payload: `
stores:
  - id: fast
    path: ` + filepath.Join(dir, "a") + `
    shards: 1
    weight: 100
    bogus: true
`,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			configPath := filepath.Join(dir, tc.name+".yaml")
			if err := os.WriteFile(configPath, []byte(tc.payload), 0o644); err != nil {
				t.Fatalf("WriteFile: %v", err)
			}
			if _, err := LoadStoreConfigFile(configPath); err == nil {
				t.Fatal("expected config validation error")
			}
		})
	}
}

func TestNormalizeStoreSpecsRejectsDuplicatesAndInvalidValues(t *testing.T) {
	dir := t.TempDir()
	tests := []struct {
		name   string
		stores []StoreSpec
	}{
		{
			name: "duplicate id",
			stores: []StoreSpec{
				{ID: "a", Path: filepath.Join(dir, "a"), Shards: 1, Weight: 100},
				{ID: "a", Path: filepath.Join(dir, "b"), Shards: 1, Weight: 100},
			},
		},
		{
			name: "duplicate path",
			stores: []StoreSpec{
				{ID: "a", Path: filepath.Join(dir, "same"), Shards: 1, Weight: 100},
				{ID: "b", Path: filepath.Join(dir, "same"), Shards: 1, Weight: 100},
			},
		},
		{
			name:   "zero shards",
			stores: []StoreSpec{{ID: "a", Path: filepath.Join(dir, "a"), Shards: 0, Weight: 100}},
		},
		{
			name:   "negative weight",
			stores: []StoreSpec{{ID: "a", Path: filepath.Join(dir, "a"), Shards: 1, Weight: -1}},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := normalizeStoreSpecs(filepath.Join(dir, "meta"), tc.stores); err == nil {
				t.Fatalf("expected validation error")
			}
		})
	}
}

func TestClientSnapshotIncludesStoreInventory(t *testing.T) {
	dir := t.TempDir()
	client, err := Open(Config{
		Path: filepath.Join(dir, "meta"),
		Stores: []StoreSpec{
			{ID: "fast", Path: filepath.Join(dir, "fast"), Shards: 2, Weight: 100},
			{ID: "bulk", Path: filepath.Join(dir, "bulk"), Shards: 3, Weight: 50},
		},
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer client.Close()

	snapshot, err := client.ObservabilitySnapshot()
	if err != nil {
		t.Fatalf("ObservabilitySnapshot: %v", err)
	}
	if len(snapshot.Stores) != 2 {
		t.Fatalf("stores=%d want=2", len(snapshot.Stores))
	}
	if snapshot.Stores[0].ID != "fast" || snapshot.Stores[0].Shards != 2 || snapshot.Stores[0].State != "healthy" {
		t.Fatalf("unexpected first store: %+v", snapshot.Stores[0])
	}
	if snapshot.Stores[0].CapacityBytes == 0 || snapshot.Stores[0].AvailableBytes == 0 {
		t.Fatalf("expected capacity fields on first store: %+v", snapshot.Stores[0])
	}
	if snapshot.Stores[1].ID != "bulk" || snapshot.Stores[1].Weight != 50 {
		t.Fatalf("unexpected second store: %+v", snapshot.Stores[1])
	}
}

func TestClientSetStoreStateUpdatesSnapshot(t *testing.T) {
	dir := t.TempDir()
	client, err := Open(Config{
		Path: filepath.Join(dir, "meta"),
		Stores: []StoreSpec{
			{ID: "fast", Path: filepath.Join(dir, "fast"), Shards: 2, Weight: 100},
			{ID: "bulk", Path: filepath.Join(dir, "bulk"), Shards: 1, Weight: 50},
		},
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer client.Close()

	if err := client.SetStoreState("fast", StoreStateFailed); err != nil {
		t.Fatalf("SetStoreState: %v", err)
	}
	snapshot, err := client.ObservabilitySnapshot()
	if err != nil {
		t.Fatalf("ObservabilitySnapshot: %v", err)
	}
	if snapshot.Stores[0].State != StoreStateFailed {
		t.Fatalf("store[0].State=%q want=%q", snapshot.Stores[0].State, StoreStateFailed)
	}
	if snapshot.Stores[1].State != StoreStateHealthy {
		t.Fatalf("store[1].State=%q want=%q", snapshot.Stores[1].State, StoreStateHealthy)
	}
}

func TestLegacyStoreShardSnapshotUsesMetadataPath(t *testing.T) {
	dir := t.TempDir()
	client, err := Open(Config{Path: filepath.Join(dir, "meta")})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer client.Close()

	snapshots, err := client.ShardSnapshots()
	if err != nil {
		t.Fatalf("ShardSnapshots: %v", err)
	}
	if len(snapshots) != 1 {
		t.Fatalf("snapshots=%d want=1", len(snapshots))
	}
	if snapshots[0].StoreID != DefaultStoreID || snapshots[0].ShardID != 0 {
		t.Fatalf("unexpected legacy shard snapshot: %+v", snapshots[0])
	}
	if snapshots[0].Path != filepath.Join(dir, "meta") {
		t.Fatalf("legacy shard path=%q want metadata path", snapshots[0].Path)
	}
}

func TestSetStoreStateRejectsUnknownStoreAndInvalidState(t *testing.T) {
	dir := t.TempDir()
	client, err := Open(Config{
		Path: filepath.Join(dir, "meta"),
		Stores: []StoreSpec{
			{ID: "fast", Path: filepath.Join(dir, "fast"), Shards: 1, Weight: 100},
		},
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer client.Close()

	if err := client.SetStoreState("missing", StoreStateFailed); err == nil {
		t.Fatal("expected unknown store error")
	}
	if err := client.SetStoreState("fast", "bogus"); err == nil {
		t.Fatal("expected invalid state error")
	}
}

func TestClientReloadStoreConfigUpdatesWeightsAndPreservesState(t *testing.T) {
	dir := t.TempDir()
	client, err := Open(Config{
		Path: filepath.Join(dir, "meta"),
		Stores: []StoreSpec{
			{ID: "fast", Path: filepath.Join(dir, "fast"), Shards: 2, Weight: 100},
			{ID: "bulk", Path: filepath.Join(dir, "bulk"), Shards: 1, Weight: 50},
		},
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer client.Close()

	if err := client.SetStoreState("fast", StoreStateFailed); err != nil {
		t.Fatalf("SetStoreState: %v", err)
	}
	if err := client.ReloadStoreConfig([]StoreSpec{
		{ID: "bulk", Path: filepath.Join(dir, "bulk"), Shards: 1, Weight: 80},
		{ID: "fast", Path: filepath.Join(dir, "fast"), Shards: 2, Weight: 0},
	}); err != nil {
		t.Fatalf("ReloadStoreConfig: %v", err)
	}

	snapshot, err := client.ObservabilitySnapshot()
	if err != nil {
		t.Fatalf("ObservabilitySnapshot: %v", err)
	}
	if snapshot.Stores[0].ID != "fast" || snapshot.Stores[0].Weight != 0 {
		t.Fatalf("unexpected fast store after reload: %+v", snapshot.Stores[0])
	}
	if snapshot.Stores[0].State != StoreStateFailed {
		t.Fatalf("fast state=%q want=%q", snapshot.Stores[0].State, StoreStateFailed)
	}
	if snapshot.Stores[1].ID != "bulk" || snapshot.Stores[1].Weight != 80 {
		t.Fatalf("unexpected bulk store after reload: %+v", snapshot.Stores[1])
	}
}

func TestClientReloadStoreConfigRejectsIdentityChanges(t *testing.T) {
	dir := t.TempDir()
	tests := []struct {
		name   string
		stores []StoreSpec
	}{
		{
			name: "changed path",
			stores: []StoreSpec{
				{ID: "fast", Path: filepath.Join(dir, "other-fast"), Shards: 1, Weight: 100},
				{ID: "bulk", Path: filepath.Join(dir, "bulk"), Shards: 1, Weight: 50},
			},
		},
		{
			name: "changed shards",
			stores: []StoreSpec{
				{ID: "fast", Path: filepath.Join(dir, "fast"), Shards: 2, Weight: 100},
				{ID: "bulk", Path: filepath.Join(dir, "bulk"), Shards: 1, Weight: 50},
			},
		},
		{
			name: "removed store",
			stores: []StoreSpec{
				{ID: "fast", Path: filepath.Join(dir, "fast"), Shards: 1, Weight: 100},
			},
		},
		{
			name: "added store",
			stores: []StoreSpec{
				{ID: "fast", Path: filepath.Join(dir, "fast"), Shards: 1, Weight: 100},
				{ID: "bulk", Path: filepath.Join(dir, "bulk"), Shards: 1, Weight: 50},
				{ID: "extra", Path: filepath.Join(dir, "extra"), Shards: 1, Weight: 25},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			client, err := Open(Config{
				Path: filepath.Join(dir, "meta-"+strings.ReplaceAll(tc.name, " ", "-")),
				Stores: []StoreSpec{
					{ID: "fast", Path: filepath.Join(dir, "fast"), Shards: 1, Weight: 100},
					{ID: "bulk", Path: filepath.Join(dir, "bulk"), Shards: 1, Weight: 50},
				},
			})
			if err != nil {
				t.Fatalf("Open: %v", err)
			}
			defer client.Close()

			if err := client.ReloadStoreConfig(tc.stores); err == nil {
				t.Fatal("expected reload validation error")
			}
		})
	}
}

func TestClientReloadStoreConfigAffectsNewAllocationDecisions(t *testing.T) {
	dir := t.TempDir()
	client, err := Open(Config{
		Path: filepath.Join(dir, "meta"),
		Stores: []StoreSpec{
			{ID: "fast", Path: filepath.Join(dir, "fast"), Shards: 1, Weight: 100},
			{ID: "bulk", Path: filepath.Join(dir, "bulk"), Shards: 1, Weight: 100},
		},
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer client.Close()

	if err := client.ReloadStoreConfig([]StoreSpec{
		{ID: "fast", Path: filepath.Join(dir, "fast"), Shards: 1, Weight: 0},
		{ID: "bulk", Path: filepath.Join(dir, "bulk"), Shards: 1, Weight: 100},
	}); err != nil {
		t.Fatalf("ReloadStoreConfig: %v", err)
	}

	spec, err := client.CreateVolume(context.Background(), service.VolumeSpec{
		ID:              0x111,
		Name:            "vol-reload",
		Prefix:          "vol-reload",
		SizeBytes:       128 * 1024,
		BlockSize:       4096,
		ChunkSizeBytes:  64 * 1024,
		ExtentPageBytes: 4 * 1024 * 1024,
	})
	if err != nil {
		t.Fatalf("CreateVolume: %v", err)
	}

	payload := make([]byte, spec.ChunkSizeBytes)
	payload[0] = 0x44
	if err := client.data.WriteAt(context.Background(), spec, 0, uint64(len(payload)), payload); err != nil {
		t.Fatalf("WriteAt: %v", err)
	}

	pages, err := client.meta.ListExtentPages(context.Background(), uint64(spec.ID))
	if err != nil {
		t.Fatalf("ListExtentPages: %v", err)
	}
	if len(pages) != 1 || len(pages[0].Extents) == 0 {
		t.Fatalf("unexpected pages after write: %+v", pages)
	}
	if got := pages[0].Extents[0].StoreID; got != "bulk" {
		t.Fatalf("extent store=%q want=bulk", got)
	}
}

func TestClientReloadStoreConfigZeroWeightStopsNewAllocations(t *testing.T) {
	dir := t.TempDir()
	client, err := Open(Config{
		Path: filepath.Join(dir, "meta"),
		Stores: []StoreSpec{
			{ID: "fast", Path: filepath.Join(dir, "fast"), Shards: 1, Weight: 100},
			{ID: "bulk", Path: filepath.Join(dir, "bulk"), Shards: 1, Weight: 100},
		},
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer client.Close()

	specBefore, err := client.CreateVolume(context.Background(), service.VolumeSpec{
		ID:              0x112,
		Name:            "vol-policy-before",
		Prefix:          "vol-policy-before",
		SizeBytes:       128 * 1024,
		BlockSize:       4096,
		ChunkSizeBytes:  64 * 1024,
		ExtentPageBytes: 4 * 1024 * 1024,
	})
	if err != nil {
		t.Fatalf("CreateVolume before reload: %v", err)
	}
	payloadBefore := make([]byte, specBefore.ChunkSizeBytes)
	payloadBefore[0] = 0x11
	if err := client.data.WriteAt(context.Background(), specBefore, 0, uint64(len(payloadBefore)), payloadBefore); err != nil {
		t.Fatalf("WriteAt before reload: %v", err)
	}
	pagesBefore, err := client.meta.ListExtentPages(context.Background(), uint64(specBefore.ID))
	if err != nil {
		t.Fatalf("ListExtentPages before reload: %v", err)
	}
	if len(pagesBefore) != 1 || len(pagesBefore[0].Extents) == 0 {
		t.Fatalf("unexpected pages before reload: %+v", pagesBefore)
	}
	if got := pagesBefore[0].Extents[0].StoreID; got != "fast" {
		t.Fatalf("expected stable baseline on fast store, got %q", got)
	}

	if err := client.ReloadStoreConfig([]StoreSpec{
		{ID: "fast", Path: filepath.Join(dir, "fast"), Shards: 1, Weight: 0},
		{ID: "bulk", Path: filepath.Join(dir, "bulk"), Shards: 1, Weight: 100},
	}); err != nil {
		t.Fatalf("ReloadStoreConfig: %v", err)
	}

	specAfter, err := client.CreateVolume(context.Background(), service.VolumeSpec{
		ID:              0x113,
		Name:            "vol-policy-after",
		Prefix:          "vol-policy-after",
		SizeBytes:       128 * 1024,
		BlockSize:       4096,
		ChunkSizeBytes:  64 * 1024,
		ExtentPageBytes: 4 * 1024 * 1024,
	})
	if err != nil {
		t.Fatalf("CreateVolume after reload: %v", err)
	}
	payloadAfter := make([]byte, specAfter.ChunkSizeBytes)
	payloadAfter[0] = 0x22
	if err := client.data.WriteAt(context.Background(), specAfter, 0, uint64(len(payloadAfter)), payloadAfter); err != nil {
		t.Fatalf("WriteAt after reload: %v", err)
	}
	pagesAfter, err := client.meta.ListExtentPages(context.Background(), uint64(specAfter.ID))
	if err != nil {
		t.Fatalf("ListExtentPages after reload: %v", err)
	}
	if len(pagesAfter) != 1 || len(pagesAfter[0].Extents) == 0 {
		t.Fatalf("unexpected pages after reload: %+v", pagesAfter)
	}
	if got := pagesAfter[0].Extents[0].StoreID; got != "bulk" {
		t.Fatalf("expected zero-weight fast store to be excluded, got %q", got)
	}
}

func TestClientReloadStoreWeightsRejectsInvalidStoreSet(t *testing.T) {
	dir := t.TempDir()
	client, err := Open(Config{
		Path: filepath.Join(dir, "meta"),
		Stores: []StoreSpec{
			{ID: "fast", Path: filepath.Join(dir, "fast"), Shards: 1, Weight: 100},
			{ID: "bulk", Path: filepath.Join(dir, "bulk"), Shards: 1, Weight: 50},
		},
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer client.Close()

	if err := client.ReloadStoreWeights([]StoreWeightUpdate{
		{StoreID: "fast", Weight: 120},
	}); err == nil {
		t.Fatal("expected incomplete store set validation error")
	}
	if err := client.ReloadStoreWeights([]StoreWeightUpdate{
		{StoreID: "fast", Weight: 120},
		{StoreID: "bulk", Weight: 80},
		{StoreID: "extra", Weight: 10},
	}); err == nil {
		t.Fatal("expected unknown store validation error")
	}
}

func TestUpdateStoreWeightsInConfigFilePersistsWeights(t *testing.T) {
	dir := t.TempDir()
	configPath := filepath.Join(dir, "stores.yaml")
	if err := WriteStoreConfigFile(configPath, []StoreSpec{
		{ID: "fast", Path: filepath.Join(dir, "fast"), Shards: 2, Weight: 100},
		{ID: "bulk", Path: filepath.Join(dir, "bulk"), Shards: 1, Weight: 50},
	}); err != nil {
		t.Fatalf("WriteStoreConfigFile: %v", err)
	}

	updated, err := UpdateStoreWeightsInConfigFile(configPath, []StoreWeightUpdate{
		{StoreID: "fast", Weight: 0},
		{StoreID: "bulk", Weight: 80},
	})
	if err != nil {
		t.Fatalf("UpdateStoreWeightsInConfigFile: %v", err)
	}
	if updated[0].Weight != 0 || updated[1].Weight != 80 {
		t.Fatalf("updated=%+v", updated)
	}
	reloaded, err := LoadStoreConfigFile(configPath)
	if err != nil {
		t.Fatalf("LoadStoreConfigFile: %v", err)
	}
	if reloaded[0].Weight != 0 || reloaded[1].Weight != 80 {
		t.Fatalf("reloaded=%+v", reloaded)
	}
}

func TestUpdateStoreTuningInConfigFilePersistsWeights(t *testing.T) {
	dir := t.TempDir()
	configPath := filepath.Join(dir, "stores.yaml")
	if err := WriteStoreConfigFile(configPath, []StoreSpec{
		{ID: "fast", Path: filepath.Join(dir, "fast"), Shards: 2, Weight: 100},
		{ID: "bulk", Path: filepath.Join(dir, "bulk"), Shards: 1, Weight: 50},
	}); err != nil {
		t.Fatalf("WriteStoreConfigFile: %v", err)
	}

	updated, err := UpdateStoreTuningInConfigFile(configPath, []StoreTuningUpdate{
		{StoreID: "fast", Weight: 0},
		{StoreID: "bulk", Weight: 80},
	})
	if err != nil {
		t.Fatalf("UpdateStoreTuningInConfigFile: %v", err)
	}
	if updated[0].Weight != 0 || updated[1].Weight != 80 {
		t.Fatalf("updated=%+v", updated)
	}
	reloaded, err := LoadStoreConfigFile(configPath)
	if err != nil {
		t.Fatalf("LoadStoreConfigFile: %v", err)
	}
	if reloaded[0].Weight != 0 || reloaded[1].Weight != 80 {
		t.Fatalf("reloaded=%+v", reloaded)
	}
}

func TestClientWriteUsesEligibleStoreMetadata(t *testing.T) {
	dir := t.TempDir()
	client, err := Open(Config{
		Path: filepath.Join(dir, "meta"),
		Stores: []StoreSpec{
			{ID: "fast", Path: filepath.Join(dir, "fast"), Shards: 2, Weight: 100},
			{ID: "bulk", Path: filepath.Join(dir, "bulk"), Shards: 2, Weight: 50},
		},
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer client.Close()

	if err := client.SetStoreState("fast", StoreStateFailed); err != nil {
		t.Fatalf("SetStoreState fast failed: %v", err)
	}

	spec, err := client.CreateVolume(context.Background(), service.VolumeSpec{
		ID:              0x101,
		Name:            "vol-a",
		Prefix:          "vol-a",
		SizeBytes:       128 * 1024,
		BlockSize:       4096,
		ChunkSizeBytes:  64 * 1024,
		ExtentPageBytes: 4 * 1024 * 1024,
	})
	if err != nil {
		t.Fatalf("CreateVolume: %v", err)
	}

	payload := make([]byte, spec.ChunkSizeBytes)
	payload[0] = 0x7f
	if err := client.data.WriteAt(context.Background(), spec, 0, uint64(len(payload)), payload); err != nil {
		t.Fatalf("WriteAt: %v", err)
	}

	pages, err := client.meta.ListExtentPages(context.Background(), uint64(spec.ID))
	if err != nil {
		t.Fatalf("ListExtentPages: %v", err)
	}
	if len(pages) != 1 {
		t.Fatalf("pages=%d want=1", len(pages))
	}
	if len(pages[0].Extents) == 0 {
		t.Fatal("expected extent records")
	}
	dataExtent := pages[0].Extents[0]
	if dataExtent.Kind != service.AllocationChunkKindData {
		t.Fatalf("unexpected extent kind: %+v", dataExtent)
	}
	if dataExtent.StoreID != "bulk" {
		t.Fatalf("StoreID=%q want=bulk", dataExtent.StoreID)
	}
	if dataExtent.ShardID >= 2 {
		t.Fatalf("ShardID=%d out of range", dataExtent.ShardID)
	}
}

func TestClientWriteFailsWithoutWritableStores(t *testing.T) {
	dir := t.TempDir()
	client, err := Open(Config{
		Path: filepath.Join(dir, "meta"),
		Stores: []StoreSpec{
			{ID: "fast", Path: filepath.Join(dir, "fast"), Shards: 1, Weight: 100},
			{ID: "bulk", Path: filepath.Join(dir, "bulk"), Shards: 1, Weight: 100},
		},
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer client.Close()

	if err := client.SetStoreState("fast", StoreStateFailed); err != nil {
		t.Fatalf("SetStoreState fast failed: %v", err)
	}
	if err := client.SetStoreState("bulk", StoreStateDraining); err != nil {
		t.Fatalf("SetStoreState bulk failed: %v", err)
	}

	spec, err := client.CreateVolume(context.Background(), service.VolumeSpec{
		ID:              0x102,
		Name:            "vol-b",
		Prefix:          "vol-b",
		SizeBytes:       128 * 1024,
		BlockSize:       4096,
		ChunkSizeBytes:  64 * 1024,
		ExtentPageBytes: 4 * 1024 * 1024,
	})
	if err != nil {
		t.Fatalf("CreateVolume: %v", err)
	}

	payload := make([]byte, spec.ChunkSizeBytes)
	payload[0] = 0x1
	err = client.data.WriteAt(context.Background(), spec, 0, uint64(len(payload)), payload)
	if err == nil {
		t.Fatal("expected write to fail without writable stores")
	}
	if !strings.Contains(err.Error(), "no writable store available") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestStoreEligibleForAllocationExcludesDraining(t *testing.T) {
	for _, tc := range []struct {
		state string
		want  bool
	}{
		{StoreStateHealthy, true},
		{StoreStateDraining, false},
		{StoreStateReadOnly, false},
		{StoreStateFailed, false},
	} {
		if got := storeEligibleForAllocation(tc.state); got != tc.want {
			t.Fatalf("storeEligibleForAllocation(%q)=%t want=%t", tc.state, got, tc.want)
		}
	}
}

func TestClientWritesPayloadIntoSeparateStorePebbles(t *testing.T) {
	dir := t.TempDir()
	metaPath := filepath.Join(dir, "meta")
	fastPath := filepath.Join(dir, "fast")
	bulkPath := filepath.Join(dir, "bulk")
	client, err := Open(Config{
		Path: metaPath,
		Stores: []StoreSpec{
			{ID: "fast", Path: fastPath, Shards: 2, Weight: 100},
			{ID: "bulk", Path: bulkPath, Shards: 2, Weight: 50},
		},
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer client.Close()

	spec, err := client.CreateVolume(context.Background(), service.VolumeSpec{
		ID:              0x103,
		Name:            "vol-c",
		Prefix:          "vol-c",
		SizeBytes:       8 * 1024 * 1024,
		BlockSize:       4096,
		ChunkSizeBytes:  64 * 1024,
		ExtentPageBytes: 4 * 1024 * 1024,
	})
	if err != nil {
		t.Fatalf("CreateVolume: %v", err)
	}

	payload := make([]byte, 1024*1024)
	for i := range payload {
		payload[i] = 0x5a
	}
	if err := client.data.WriteAt(context.Background(), spec, 0, uint64(len(payload)), payload); err != nil {
		t.Fatalf("WriteAt: %v", err)
	}

	snapshots, err := client.ShardSnapshots()
	if err != nil {
		t.Fatalf("ShardSnapshots: %v", err)
	}
	storeCounts := make(map[string]int)
	for _, snapshot := range snapshots {
		storeCounts[snapshot.StoreID] += snapshot.ChunkKeys
		if snapshot.StoreID == "fast" || snapshot.StoreID == "bulk" {
			if _, err := os.Stat(snapshot.Path); err != nil {
				t.Fatalf("expected shard path %s to exist: %v", snapshot.Path, err)
			}
		}
	}
	if storeCounts["fast"] == 0 || storeCounts["bulk"] == 0 {
		t.Fatalf("expected chunk keys on both stores, got %+v", storeCounts)
	}
	entries, err := os.ReadDir(metaPath)
	if err != nil {
		t.Fatalf("ReadDir metaPath: %v", err)
	}
	if len(entries) == 0 {
		t.Fatalf("expected metadata pebble files under %s", metaPath)
	}
}
