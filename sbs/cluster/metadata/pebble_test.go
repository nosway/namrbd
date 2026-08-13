package metadata

import (
	"context"
	"path/filepath"
	"testing"
)

func TestPebbleKVRoundTripAndList(t *testing.T) {
	kv, err := OpenPebbleKV(filepath.Join(t.TempDir(), "cluster-meta"))
	if err != nil {
		t.Fatalf("OpenPebbleKV: %v", err)
	}
	defer kv.Close()

	ctx := context.Background()
	if err := kv.Set(ctx, "sbs/cluster/volumes/00a1/meta/state", []byte("v1")); err != nil {
		t.Fatalf("Set(state): %v", err)
	}
	if err := kv.Set(ctx, "sbs/cluster/volumes/00a1/extents/0001", []byte("e1")); err != nil {
		t.Fatalf("Set(extent): %v", err)
	}

	value, found, err := kv.Get(ctx, "sbs/cluster/volumes/00a1/meta/state")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !found || string(value) != "v1" {
		t.Fatalf("Get returned found=%t value=%q", found, value)
	}

	keys, next, err := kv.List(ctx, "sbs/cluster/volumes/00a1/", "", 1)
	if err != nil {
		t.Fatalf("List first: %v", err)
	}
	if len(keys) != 1 || next == "" {
		t.Fatalf("List first returned keys=%v next=%q", keys, next)
	}

	keys, next, err = kv.List(ctx, "sbs/cluster/volumes/00a1/", next, 10)
	if err != nil {
		t.Fatalf("List second: %v", err)
	}
	if len(keys) != 1 || next != "" {
		t.Fatalf("List second returned keys=%v next=%q", keys, next)
	}
}

func TestPebbleKVPersistsAcrossReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cluster-meta")
	ctx := context.Background()

	kv, err := OpenPebbleKV(path)
	if err != nil {
		t.Fatalf("OpenPebbleKV: %v", err)
	}
	if err := kv.Set(ctx, "k1", []byte("v1")); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if err := kv.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	kv, err = OpenPebbleKV(path)
	if err != nil {
		t.Fatalf("Reopen PebbleKV: %v", err)
	}
	defer kv.Close()

	value, found, err := kv.Get(ctx, "k1")
	if err != nil {
		t.Fatalf("Get after reopen: %v", err)
	}
	if !found || string(value) != "v1" {
		t.Fatalf("Get after reopen returned found=%t value=%q", found, value)
	}
}
