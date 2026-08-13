package main

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	clustermeta "github.com/nosway/namrbd/sbs/cluster/metadata"
)

func TestLeaderLeaseAcquireAndFailover(t *testing.T) {
	kv, err := clustermeta.OpenPebbleKV(filepath.Join(t.TempDir(), "leader-meta"))
	if err != nil {
		t.Fatalf("OpenPebbleKV: %v", err)
	}
	defer kv.Close()

	now := time.Unix(1_700_000_000, 0)
	a := newLeaderLeaseManager(kv, defaultMetadataRoot, "svc-a")
	a.leaseDuration = 5 * time.Second
	a.now = func() time.Time { return now }

	record, leader, err := a.acquireOrRenew(context.Background())
	if err != nil {
		t.Fatalf("a.acquireOrRenew: %v", err)
	}
	if !leader || record.NodeID != "svc-a" {
		t.Fatalf("leader=%v record=%+v", leader, record)
	}

	b := newLeaderLeaseManager(kv, defaultMetadataRoot, "svc-b")
	b.leaseDuration = 5 * time.Second
	b.now = func() time.Time { return now.Add(2 * time.Second) }

	record, leader, err = b.acquireOrRenew(context.Background())
	if err != nil {
		t.Fatalf("b.acquireOrRenew: %v", err)
	}
	if leader {
		t.Fatalf("expected svc-b to stay standby, record=%+v", record)
	}
	if record.NodeID != "svc-a" {
		t.Fatalf("leader node=%q want=svc-a", record.NodeID)
	}

	now = now.Add(6 * time.Second)
	b.now = func() time.Time { return now }
	record, leader, err = b.acquireOrRenew(context.Background())
	if err != nil {
		t.Fatalf("b.acquireOrRenew after expiry: %v", err)
	}
	if !leader || record.NodeID != "svc-b" {
		t.Fatalf("leader=%v record=%+v", leader, record)
	}
}

func TestLeaderLeaseRelease(t *testing.T) {
	kv, err := clustermeta.OpenPebbleKV(filepath.Join(t.TempDir(), "leader-release"))
	if err != nil {
		t.Fatalf("OpenPebbleKV: %v", err)
	}
	defer kv.Close()

	mgr := newLeaderLeaseManager(kv, defaultMetadataRoot, "svc-a")
	mgr.now = func() time.Time { return time.Unix(1_700_000_000, 0) }
	record, leader, err := mgr.acquireOrRenew(context.Background())
	if err != nil {
		t.Fatalf("acquireOrRenew: %v", err)
	}
	mgr.mu.Lock()
	mgr.current = record
	mgr.lastErr = nil
	mgr.mu.Unlock()
	mgr.isLeader.Store(leader)

	if err := mgr.Release(context.Background()); err != nil {
		t.Fatalf("Release: %v", err)
	}
	if mgr.IsLeader() {
		t.Fatalf("expected leader flag cleared after release")
	}
	if _, err := mgr.CurrentLeader(context.Background()); err == nil {
		t.Fatalf("expected no current leader after release")
	}
}

func TestLeaderLeaseMutationReadyExpires(t *testing.T) {
	kv, err := clustermeta.OpenPebbleKV(filepath.Join(t.TempDir(), "leader-expire"))
	if err != nil {
		t.Fatalf("OpenPebbleKV: %v", err)
	}
	defer kv.Close()

	now := time.Unix(1_700_000_000, 0)
	mgr := newLeaderLeaseManager(kv, defaultMetadataRoot, "svc-a")
	mgr.leaseDuration = 5 * time.Second
	mgr.now = func() time.Time { return now }
	record, leader, err := mgr.acquireOrRenew(context.Background())
	if err != nil {
		t.Fatalf("acquireOrRenew: %v", err)
	}
	mgr.mu.Lock()
	mgr.current = record
	mgr.lastErr = nil
	mgr.mu.Unlock()
	mgr.isLeader.Store(leader)
	if !mgr.MutationReady() {
		t.Fatalf("expected mutation ready while lease valid")
	}

	now = now.Add(6 * time.Second)
	if mgr.MutationReady() {
		t.Fatalf("expected mutation not ready after lease expiry")
	}
	if state := mgr.State(); state != "election" {
		t.Fatalf("state=%q want=election", state)
	}
}
