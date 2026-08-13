package service

import (
	"bytes"
	"context"
	"testing"

	"github.com/nosway/namrbd/gateway/store"
)

func TestChunkPayloadRepositoryReadMissingReturnsZeroFilledChunk(t *testing.T) {
	objects := store.NewMemoryStore()
	repo := newChunkPayloadRepository(objects)
	volume := NormalizeVolumeSpec(VolumeSpec{
		ID:              101,
		Prefix:          "devA",
		SizeBytes:       1 << 20,
		BlockSize:       DefaultBlockSize,
		ChunkSizeBytes:  64,
		ExtentPageBytes: DefaultAllocationPageSize,
	})

	got, err := repo.ReadChunk(context.Background(), volume, 7)
	if err != nil {
		t.Fatalf("ReadChunk failed: %v", err)
	}
	if len(got) != 64 {
		t.Fatalf("unexpected chunk length: %d", len(got))
	}
	if !bytes.Equal(got, make([]byte, 64)) {
		t.Fatalf("expected zero-filled chunk, got %v", got)
	}
}

func TestChunkPayloadRepositoryWriteAndReadChunk(t *testing.T) {
	objects := store.NewMemoryStore()
	repo := newChunkPayloadRepository(objects)
	volume := NormalizeVolumeSpec(VolumeSpec{
		ID:              101,
		Prefix:          "devA",
		SizeBytes:       1 << 20,
		BlockSize:       DefaultBlockSize,
		ChunkSizeBytes:  16,
		ExtentPageBytes: DefaultAllocationPageSize,
	})
	payload := []byte("abcdefghijklmnop")

	if err := repo.WriteChunk(context.Background(), volume, 3, payload); err != nil {
		t.Fatalf("WriteChunk failed: %v", err)
	}

	got, err := repo.ReadChunk(context.Background(), volume, 3)
	if err != nil {
		t.Fatalf("ReadChunk failed: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("unexpected chunk contents: got=%q want=%q", got, payload)
	}

	keys, _, err := objects.List(context.Background(), "devA:chk:", "", 10)
	if err != nil {
		t.Fatalf("List failed: %v", err)
	}
	if len(keys) != 1 || keys[0] != "devA:chk:3" {
		t.Fatalf("unexpected chunk keys: %v", keys)
	}
}

func TestChunkPayloadRepositoryWriteRejectsWrongSize(t *testing.T) {
	objects := store.NewMemoryStore()
	repo := newChunkPayloadRepository(objects)
	volume := NormalizeVolumeSpec(VolumeSpec{
		ID:              101,
		Prefix:          "devA",
		SizeBytes:       1 << 20,
		BlockSize:       DefaultBlockSize,
		ChunkSizeBytes:  8,
		ExtentPageBytes: DefaultAllocationPageSize,
	})

	if err := repo.WriteChunk(context.Background(), volume, 1, []byte("short")); err == nil {
		t.Fatal("expected WriteChunk size validation error")
	}
}
