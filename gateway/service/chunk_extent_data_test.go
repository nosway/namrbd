package service

import (
	"bytes"
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/nosway/namrbd/gateway/store"
)

func TestChunkExtentDataRepositoryReadSingleChunk(t *testing.T) {
	objects := store.NewMemoryStore()
	meta := NewInMemoryMetadataRepository(nil)
	repo := NewChunkExtentDataRepository(meta, objects)
	ctx := context.Background()

	volume, err := meta.CreateVolume(ctx, VolumeCreateRequest{
		Name:            "vol-read-single",
		SizeBytes:       64,
		BlockSize:       4,
		ChunkSizeBytes:  16,
		ExtentPageBytes: 32,
	})
	if err != nil {
		t.Fatalf("CreateVolume failed: %v", err)
	}

	startChunkID, err := meta.AllocateChunkIDs(ctx, uint64(volume.ID), 1)
	if err != nil {
		t.Fatalf("AllocateChunkIDs failed: %v", err)
	}
	payload := []byte("abcdefghijklmnop")
	if err := newChunkPayloadRepository(objects).WriteChunk(ctx, volume, startChunkID, payload); err != nil {
		t.Fatalf("WriteChunk failed: %v", err)
	}
	if _, err := meta.PutExtentPage(ctx, AllocationPageRecord{
		VolumeID:       volume.ID,
		PageNo:         0,
		PageBytes:      volume.ExtentPageBytes,
		ChunkSizeBytes: volume.ChunkSizeBytes,
		Extents: []AllocationChunkRecord{{
			LogicalChunkStart:  0,
			ChunkCount:         1,
			Kind:               AllocationChunkKindData,
			PhysicalChunkStart: startChunkID,
		}},
	}, 0); err != nil {
		t.Fatalf("PutExtentPage failed: %v", err)
	}

	got, err := repo.ReadAt(ctx, volume, 0, 16)
	if err != nil {
		t.Fatalf("ReadAt failed: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("unexpected payload: got=%q want=%q", got, payload)
	}
}

func TestChunkExtentDataRepositoryConcurrentSamePageWrites(t *testing.T) {
	objects := store.NewMemoryStore()
	meta := NewInMemoryMetadataRepository(nil)
	repo := NewChunkExtentDataRepository(meta, objects)
	ctx := context.Background()

	volume, err := meta.CreateVolume(ctx, VolumeCreateRequest{
		Name:            "vol-concurrent-same-page",
		SizeBytes:       1024,
		BlockSize:       4,
		ChunkSizeBytes:  16,
		ExtentPageBytes: 1024,
	})
	if err != nil {
		t.Fatalf("CreateVolume failed: %v", err)
	}

	const writes = 64
	var wg sync.WaitGroup
	errCh := make(chan error, writes)
	for i := 0; i < writes; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			payload := bytes.Repeat([]byte{byte(i + 1)}, 16)
			if err := repo.WriteAt(ctx, volume, uint64(i*16), 16, payload); err != nil {
				errCh <- fmt.Errorf("write %d failed: %w", i, err)
			}
		}()
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatal(err)
	}

	got, err := repo.ReadAt(ctx, volume, 0, volume.SizeBytes)
	if err != nil {
		t.Fatalf("ReadAt failed: %v", err)
	}
	for i := 0; i < writes; i++ {
		want := bytes.Repeat([]byte{byte(i + 1)}, 16)
		start := i * 16
		if !bytes.Equal(got[start:start+16], want) {
			t.Fatalf("chunk %d mismatch: got=%v want=%v", i, got[start:start+16], want)
		}
	}
}

func TestChunkExtentDataRepositoryReadSparseZeroRange(t *testing.T) {
	objects := store.NewMemoryStore()
	meta := NewInMemoryMetadataRepository(nil)
	repo := NewChunkExtentDataRepository(meta, objects)
	ctx := context.Background()

	volume, err := meta.CreateVolume(ctx, VolumeCreateRequest{
		Name:            "vol-read-zero",
		SizeBytes:       64,
		BlockSize:       4,
		ChunkSizeBytes:  16,
		ExtentPageBytes: 32,
	})
	if err != nil {
		t.Fatalf("CreateVolume failed: %v", err)
	}

	if _, err := meta.PutExtentPage(ctx, AllocationPageRecord{
		VolumeID:       volume.ID,
		PageNo:         0,
		PageBytes:      volume.ExtentPageBytes,
		ChunkSizeBytes: volume.ChunkSizeBytes,
		Extents: []AllocationChunkRecord{{
			LogicalChunkStart: 0,
			ChunkCount:        2,
			Kind:              AllocationChunkKindZero,
		}},
	}, 0); err != nil {
		t.Fatalf("PutExtentPage failed: %v", err)
	}

	got, err := repo.ReadAt(ctx, volume, 0, 32)
	if err != nil {
		t.Fatalf("ReadAt failed: %v", err)
	}
	if !bytes.Equal(got, make([]byte, 32)) {
		t.Fatalf("expected zero-filled read, got %v", got)
	}
}

func TestChunkExtentDataRepositoryReadAcrossPages(t *testing.T) {
	objects := store.NewMemoryStore()
	meta := NewInMemoryMetadataRepository(nil)
	repo := NewChunkExtentDataRepository(meta, objects)
	ctx := context.Background()

	volume, err := meta.CreateVolume(ctx, VolumeCreateRequest{
		Name:            "vol-read-pages",
		SizeBytes:       128,
		BlockSize:       4,
		ChunkSizeBytes:  16,
		ExtentPageBytes: 32,
	})
	if err != nil {
		t.Fatalf("CreateVolume failed: %v", err)
	}

	startChunkID, err := meta.AllocateChunkIDs(ctx, uint64(volume.ID), 2)
	if err != nil {
		t.Fatalf("AllocateChunkIDs failed: %v", err)
	}
	chunks := newChunkPayloadRepository(objects)
	if err := chunks.WriteChunk(ctx, volume, startChunkID, []byte("abcdefghijklmnop")); err != nil {
		t.Fatalf("WriteChunk page0 failed: %v", err)
	}
	if err := chunks.WriteChunk(ctx, volume, startChunkID+1, []byte("QRSTUVWXYZ012345")); err != nil {
		t.Fatalf("WriteChunk page1 failed: %v", err)
	}

	if _, err := meta.PutExtentPage(ctx, AllocationPageRecord{
		VolumeID:       volume.ID,
		PageNo:         0,
		PageBytes:      volume.ExtentPageBytes,
		ChunkSizeBytes: volume.ChunkSizeBytes,
		Extents: []AllocationChunkRecord{{
			LogicalChunkStart:  1,
			ChunkCount:         1,
			Kind:               AllocationChunkKindData,
			PhysicalChunkStart: startChunkID,
		}},
	}, 0); err != nil {
		t.Fatalf("PutExtentPage page0 failed: %v", err)
	}
	if _, err := meta.PutExtentPage(ctx, AllocationPageRecord{
		VolumeID:       volume.ID,
		PageNo:         1,
		PageBytes:      volume.ExtentPageBytes,
		ChunkSizeBytes: volume.ChunkSizeBytes,
		Extents: []AllocationChunkRecord{{
			LogicalChunkStart:  2,
			ChunkCount:         1,
			Kind:               AllocationChunkKindData,
			PhysicalChunkStart: startChunkID + 1,
		}},
	}, 0); err != nil {
		t.Fatalf("PutExtentPage page1 failed: %v", err)
	}

	got, err := repo.ReadAt(ctx, volume, 16, 32)
	if err != nil {
		t.Fatalf("ReadAt failed: %v", err)
	}
	want := []byte("abcdefghijklmnopQRSTUVWXYZ012345")
	if !bytes.Equal(got, want) {
		t.Fatalf("unexpected read across pages: got=%q want=%q", got, want)
	}
}

func TestChunkExtentDataRepositoryWriteSingleChunk(t *testing.T) {
	objects := store.NewMemoryStore()
	meta := NewInMemoryMetadataRepository(nil)
	repo := NewChunkExtentDataRepository(meta, objects)
	ctx := context.Background()

	volume, err := meta.CreateVolume(ctx, VolumeCreateRequest{
		Name:            "vol-write-single",
		SizeBytes:       64,
		BlockSize:       4,
		ChunkSizeBytes:  16,
		ExtentPageBytes: 32,
	})
	if err != nil {
		t.Fatalf("CreateVolume failed: %v", err)
	}

	payload := []byte("abcdefghijklmnop")
	if err := repo.WriteAt(ctx, volume, 0, 16, payload); err != nil {
		t.Fatalf("WriteAt failed: %v", err)
	}

	got, err := repo.ReadAt(ctx, volume, 0, 16)
	if err != nil {
		t.Fatalf("ReadAt failed: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("unexpected read after write: got=%q want=%q", got, payload)
	}
	pages, err := meta.ListExtentPages(ctx, uint64(volume.ID))
	if err != nil {
		t.Fatalf("ListExtentPages failed: %v", err)
	}
	if len(pages) != 1 || len(pages[0].Extents) != 2 || pages[0].Extents[0].Kind != AllocationChunkKindData || pages[0].Extents[1].Kind != AllocationChunkKindZero {
		t.Fatalf("unexpected extent page after write: %+v", pages)
	}
}

func TestChunkExtentDataRepositoryWritePartialOverwrite(t *testing.T) {
	objects := store.NewMemoryStore()
	meta := NewInMemoryMetadataRepository(nil)
	repo := NewChunkExtentDataRepository(meta, objects)
	ctx := context.Background()

	volume, err := meta.CreateVolume(ctx, VolumeCreateRequest{
		Name:            "vol-write-partial",
		SizeBytes:       64,
		BlockSize:       4,
		ChunkSizeBytes:  16,
		ExtentPageBytes: 32,
	})
	if err != nil {
		t.Fatalf("CreateVolume failed: %v", err)
	}

	if err := repo.WriteAt(ctx, volume, 0, 16, []byte("abcdefghijklmnop")); err != nil {
		t.Fatalf("initial WriteAt failed: %v", err)
	}
	if err := repo.WriteAt(ctx, volume, 4, 4, []byte("WXYZ")); err != nil {
		t.Fatalf("partial WriteAt failed: %v", err)
	}

	got, err := repo.ReadAt(ctx, volume, 0, 16)
	if err != nil {
		t.Fatalf("ReadAt failed: %v", err)
	}
	want := []byte("abcdWXYZijklmnop")
	if !bytes.Equal(got, want) {
		t.Fatalf("unexpected partial overwrite result: got=%q want=%q", got, want)
	}
}

func TestChunkExtentDataRepositoryFullChunkOverwriteSkipsRead(t *testing.T) {
	objects := store.NewMemoryStore()
	meta := NewInMemoryMetadataRepository(nil)
	repo := NewChunkExtentDataRepository(meta, objects)
	instrumented, ok := repo.(InstrumentedDataRepository)
	if !ok {
		t.Fatal("repository does not expose instrumentation")
	}
	ctx := context.Background()

	volume, err := meta.CreateVolume(ctx, VolumeCreateRequest{
		Name:            "vol-write-full-overwrite",
		SizeBytes:       64,
		BlockSize:       4,
		ChunkSizeBytes:  16,
		ExtentPageBytes: 32,
	})
	if err != nil {
		t.Fatalf("CreateVolume failed: %v", err)
	}

	if err := repo.WriteAt(ctx, volume, 0, 16, []byte("abcdefghijklmnop")); err != nil {
		t.Fatalf("initial WriteAt failed: %v", err)
	}
	stats, err := instrumented.WriteAtWithStats(ctx, volume, 0, 16, []byte("ABCDEFGHIJKLMNOP"))
	if err != nil {
		t.Fatalf("overwrite WriteAtWithStats failed: %v", err)
	}
	if stats.ChunksRead != 0 {
		t.Fatalf("full chunk overwrite read old chunks: got=%d want=0", stats.ChunksRead)
	}
	if stats.FullChunkOverwrites != 1 {
		t.Fatalf("unexpected full chunk overwrite count: got=%d want=1", stats.FullChunkOverwrites)
	}
	if stats.ChunksWritten != 1 {
		t.Fatalf("unexpected chunks written: got=%d want=1", stats.ChunksWritten)
	}

	got, err := repo.ReadAt(ctx, volume, 0, 16)
	if err != nil {
		t.Fatalf("ReadAt failed: %v", err)
	}
	want := []byte("ABCDEFGHIJKLMNOP")
	if !bytes.Equal(got, want) {
		t.Fatalf("unexpected full overwrite result: got=%q want=%q", got, want)
	}
}

func TestChunkExtentDataRepositoryPhysicalChunkWriteStats(t *testing.T) {
	objects := store.NewMemoryStore()
	meta := NewInMemoryMetadataRepository(nil)
	repo := NewChunkExtentDataRepository(meta, objects)
	instrumented, ok := repo.(interface {
		WritePhysicalChunkAtWithStats(context.Context, VolumeSpec, uint64, uint64, uint64, []byte) (PhysicalChunkWriteStats, error)
	})
	if !ok {
		t.Fatal("repository does not expose physical chunk instrumentation")
	}
	physicalReader, ok := repo.(interface {
		ReadPhysicalChunkAt(context.Context, VolumeSpec, uint64, uint64, uint64) ([]byte, error)
	})
	if !ok {
		t.Fatal("repository does not expose physical chunk reads")
	}
	ctx := context.Background()

	volume, err := meta.CreateVolume(ctx, VolumeCreateRequest{
		Name:            "vol-physical-write-stats",
		SizeBytes:       64,
		BlockSize:       4,
		ChunkSizeBytes:  16,
		ExtentPageBytes: 32,
	})
	if err != nil {
		t.Fatalf("CreateVolume failed: %v", err)
	}

	stats, err := instrumented.WritePhysicalChunkAtWithStats(ctx, volume, 7, 4, 4, []byte("DATA"))
	if err != nil {
		t.Fatalf("WritePhysicalChunkAtWithStats failed: %v", err)
	}
	if stats.ChunksRead != 1 {
		t.Fatalf("unexpected chunks read: got=%d want=1", stats.ChunksRead)
	}
	if stats.ChunksWritten != 1 {
		t.Fatalf("unexpected chunks written: got=%d want=1", stats.ChunksWritten)
	}
	if stats.FullChunkOverwrites != 0 {
		t.Fatalf("unexpected full chunk overwrite count: got=%d want=0", stats.FullChunkOverwrites)
	}

	got, err := physicalReader.ReadPhysicalChunkAt(ctx, volume, 7, 0, 16)
	if err != nil {
		t.Fatalf("ReadPhysicalChunkAt failed: %v", err)
	}
	want := []byte{0, 0, 0, 0, 'D', 'A', 'T', 'A', 0, 0, 0, 0, 0, 0, 0, 0}
	if !bytes.Equal(got, want) {
		t.Fatalf("unexpected physical chunk payload: got=%q want=%q", got, want)
	}
}

func TestChunkExtentDataRepositoryFullPhysicalChunkWriteSkipsRead(t *testing.T) {
	objects := store.NewMemoryStore()
	meta := NewInMemoryMetadataRepository(nil)
	repo := NewChunkExtentDataRepository(meta, objects)
	instrumented, ok := repo.(interface {
		WritePhysicalChunkAtWithStats(context.Context, VolumeSpec, uint64, uint64, uint64, []byte) (PhysicalChunkWriteStats, error)
	})
	if !ok {
		t.Fatal("repository does not expose physical chunk instrumentation")
	}
	physicalReader, ok := repo.(interface {
		ReadPhysicalChunkAt(context.Context, VolumeSpec, uint64, uint64, uint64) ([]byte, error)
	})
	if !ok {
		t.Fatal("repository does not expose physical chunk reads")
	}
	ctx := context.Background()

	volume, err := meta.CreateVolume(ctx, VolumeCreateRequest{
		Name:            "vol-physical-full-overwrite",
		SizeBytes:       64,
		BlockSize:       4,
		ChunkSizeBytes:  16,
		ExtentPageBytes: 32,
	})
	if err != nil {
		t.Fatalf("CreateVolume failed: %v", err)
	}

	payload := []byte("ABCDEFGHIJKLMNOP")
	stats, err := instrumented.WritePhysicalChunkAtWithStats(ctx, volume, 11, 0, 16, payload)
	if err != nil {
		t.Fatalf("WritePhysicalChunkAtWithStats failed: %v", err)
	}
	if stats.ChunksRead != 0 {
		t.Fatalf("unexpected chunks read: got=%d want=0", stats.ChunksRead)
	}
	if stats.ChunksWritten != 1 {
		t.Fatalf("unexpected chunks written: got=%d want=1", stats.ChunksWritten)
	}
	if stats.FullChunkOverwrites != 1 {
		t.Fatalf("unexpected full chunk overwrite count: got=%d want=1", stats.FullChunkOverwrites)
	}

	got, err := physicalReader.ReadPhysicalChunkAt(ctx, volume, 11, 0, 16)
	if err != nil {
		t.Fatalf("ReadPhysicalChunkAt failed: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("unexpected physical chunk payload: got=%q want=%q", got, payload)
	}
}

func TestChunkExtentDataRepositoryWriteAcrossPages(t *testing.T) {
	objects := store.NewMemoryStore()
	meta := NewInMemoryMetadataRepository(nil)
	repo := NewChunkExtentDataRepository(meta, objects)
	ctx := context.Background()

	volume, err := meta.CreateVolume(ctx, VolumeCreateRequest{
		Name:            "vol-write-pages",
		SizeBytes:       128,
		BlockSize:       4,
		ChunkSizeBytes:  16,
		ExtentPageBytes: 32,
	})
	if err != nil {
		t.Fatalf("CreateVolume failed: %v", err)
	}

	payload := []byte("abcdefghijklmnopQRSTUVWXYZ012345")
	if err := repo.WriteAt(ctx, volume, 16, 32, payload); err != nil {
		t.Fatalf("WriteAt failed: %v", err)
	}

	got, err := repo.ReadAt(ctx, volume, 16, 32)
	if err != nil {
		t.Fatalf("ReadAt failed: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("unexpected cross-page read after write: got=%q want=%q", got, payload)
	}
	pages, err := meta.ListExtentPages(ctx, uint64(volume.ID))
	if err != nil {
		t.Fatalf("ListExtentPages failed: %v", err)
	}
	if len(pages) != 2 {
		t.Fatalf("expected 2 extent pages, got %d", len(pages))
	}
}

func TestChunkExtentDataRepositoryWriteZeroCreatesZeroExtent(t *testing.T) {
	objects := store.NewMemoryStore()
	meta := NewInMemoryMetadataRepository(nil)
	repo := NewChunkExtentDataRepository(meta, objects)
	ctx := context.Background()

	volume, err := meta.CreateVolume(ctx, VolumeCreateRequest{
		Name:            "vol-write-zero",
		SizeBytes:       64,
		BlockSize:       4,
		ChunkSizeBytes:  16,
		ExtentPageBytes: 32,
	})
	if err != nil {
		t.Fatalf("CreateVolume failed: %v", err)
	}

	if err := repo.WriteAt(ctx, volume, 0, 16, make([]byte, 16)); err != nil {
		t.Fatalf("zero WriteAt failed: %v", err)
	}
	pages, err := meta.ListExtentPages(ctx, uint64(volume.ID))
	if err != nil {
		t.Fatalf("ListExtentPages failed: %v", err)
	}
	if len(pages) != 1 || pages[0].Extents[0].Kind != AllocationChunkKindZero {
		t.Fatalf("expected zero extent after zero write, got %+v", pages)
	}
}
