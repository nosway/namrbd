package payload

import (
	"context"
	"path/filepath"
	"reflect"
	"testing"
)

func TestPebbleStoreRoundTripAndList(t *testing.T) {
	payloadStore, err := OpenPebbleStore(filepath.Join(t.TempDir(), "payload"))
	if err != nil {
		t.Fatalf("OpenPebbleStore: %v", err)
	}
	defer payloadStore.Close()

	ctx := context.Background()
	if err := payloadStore.Put(ctx, "volumes/00a1b2c3/extents/1", []byte("one")); err != nil {
		t.Fatalf("Put one: %v", err)
	}
	if err := payloadStore.Put(ctx, "volumes/00a1b2c3/extents/2", []byte("two")); err != nil {
		t.Fatalf("Put two: %v", err)
	}

	value, found, err := payloadStore.Get(ctx, "volumes/00a1b2c3/extents/1")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !found || string(value) != "one" {
		t.Fatalf("Get returned found=%v value=%q", found, value)
	}

	keys, next, err := payloadStore.List(ctx, "volumes/00a1b2c3/extents/", "", 1)
	if err != nil {
		t.Fatalf("List first page: %v", err)
	}
	if !reflect.DeepEqual(keys, []string{"volumes/00a1b2c3/extents/1"}) {
		t.Fatalf("keys=%v", keys)
	}
	if next != "volumes/00a1b2c3/extents/1" {
		t.Fatalf("next=%q", next)
	}

	keys, next, err = payloadStore.List(ctx, "volumes/00a1b2c3/extents/", next, 10)
	if err != nil {
		t.Fatalf("List second page: %v", err)
	}
	if !reflect.DeepEqual(keys, []string{"volumes/00a1b2c3/extents/2"}) {
		t.Fatalf("keys=%v", keys)
	}
	if next != "" {
		t.Fatalf("next=%q want empty", next)
	}
}

func TestReplicaStoresExposeObjectStores(t *testing.T) {
	replicas, err := OpenReplicaStores(t.TempDir(), []string{"rep-b", "rep-a"})
	if err != nil {
		t.Fatalf("OpenReplicaStores: %v", err)
	}
	defer replicas.Close()

	stores := replicas.ObjectStores()
	if len(stores) != 2 {
		t.Fatalf("stores=%d want=2", len(stores))
	}

	ctx := context.Background()
	if err := stores["rep-a"].Put(ctx, "k1", []byte("v1")); err != nil {
		t.Fatalf("rep-a Put: %v", err)
	}
	if err := stores["rep-b"].Put(ctx, "k1", []byte("v2")); err != nil {
		t.Fatalf("rep-b Put: %v", err)
	}

	valueA, found, err := stores["rep-a"].Get(ctx, "k1")
	if err != nil || !found {
		t.Fatalf("rep-a Get found=%v err=%v", found, err)
	}
	valueB, found, err := stores["rep-b"].Get(ctx, "k1")
	if err != nil || !found {
		t.Fatalf("rep-b Get found=%v err=%v", found, err)
	}
	if string(valueA) != "v1" || string(valueB) != "v2" {
		t.Fatalf("replica values=(%q,%q)", valueA, valueB)
	}
}
