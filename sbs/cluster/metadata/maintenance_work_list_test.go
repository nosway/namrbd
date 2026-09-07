package metadata

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"
)

func TestMaintenanceWorkDetailPageAndProjectionAreBounded(t *testing.T) {
	ctx := context.Background()
	kv, err := OpenPebbleKV(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer kv.Close()
	repo := NewRepository(kv, "phase-ad-work-list")
	seedMaintenanceWorkTransition(t, ctx, repo, "00a1b2d1", "pl-page-a", "repair")
	seedMaintenanceWorkTransition(t, ctx, repo, "00a1b2d2", "pl-page-b", "repair")
	if _, err := repo.GetMaintenanceWorkListProjection(ctx, "repair"); !errors.Is(err, ErrMaintenanceWorkListRebuildRequired) {
		t.Fatalf("projection before promotion error=%v", err)
	}
	putReadyMaintenanceIndexState(t, kv, repo.root, "epoch-work-list")
	before, err := repo.GetMaintenanceWorkListProjection(ctx, "repair")
	if err != nil || before.RevisionDigest == "" || before.MaintenanceEpoch != "epoch-work-list" {
		t.Fatalf("projection=%+v err=%v", before, err)
	}
	page, err := repo.ListMaintenanceWorkDetailPage(ctx, "repair", MaintenanceWorkStateReady, "", 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Work) != 1 || page.NextCursor == "" || page.ScannedCount != 1 || page.RangePageCount != 1 || page.BatchGetCount != 2 || page.BatchGetKeyCount != 2 || page.BackendFullScanCount != 0 || page.FullCompletionCount != 0 || page.NestedCompletionCount != 0 {
		t.Fatalf("page=%+v", page)
	}
	indexPage, err := repo.ListMaintenanceWorkPage(ctx, "repair", MaintenanceWorkStateReady, "", 1)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := repo.ClaimMaintenanceWork(ctx, indexPage.Records[0], "worker-page", time.Minute); err != nil {
		t.Fatal(err)
	}
	after, err := repo.GetMaintenanceWorkListProjection(ctx, "repair")
	if err != nil {
		t.Fatal(err)
	}
	if after.RevisionDigest == before.RevisionDigest {
		t.Fatalf("claim did not advance ready/leased revision: before=%+v after=%+v", before, after)
	}
	leasedPage, err := repo.ListMaintenanceWorkPage(ctx, "repair", MaintenanceWorkStateLeased, "", 1)
	if err != nil || len(leasedPage.Records) != 1 {
		t.Fatalf("leased page=%+v err=%v", leasedPage, err)
	}
	if err := kv.Delete(ctx, maintenanceWorkIndexKey(repo.root, leasedPage.Records[0])); err != nil {
		t.Fatal(err)
	}
	for attempt := 0; attempt < 100; attempt++ {
		rebuilt, err := repo.RunMaintenanceIndexRebuildPage(ctx, "epoch-work-list-repair", 16, 64)
		if err != nil {
			t.Fatal(err)
		}
		if rebuilt.Completed {
			break
		}
		if attempt == 99 {
			t.Fatal("maintenance index repair did not complete")
		}
	}
	afterRepair, err := repo.GetMaintenanceWorkListProjection(ctx, "repair")
	if err != nil {
		t.Fatal(err)
	}
	if afterRepair.RevisionDigest == after.RevisionDigest {
		t.Fatal("rebuild index repair did not advance maintenance work list revision")
	}
	if _, err := repo.ListMaintenanceWorkDetailPage(ctx, "repair", MaintenanceWorkStateReady, "", MaintenanceWorkPageMaximum+1); !errors.Is(err, ErrMaintenanceIndexInvalid) {
		t.Fatalf("oversized page error=%v", err)
	}
}

func TestMaintenanceWorkDetailPageChunksBatchGetAt128(t *testing.T) {
	ctx := context.Background()
	kv, err := OpenPebbleKV(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer kv.Close()
	repo := NewRepository(kv, "phase-ad-work-list-batch")
	for i := 0; i < 129; i++ {
		seedMaintenanceWorkTransition(t, ctx, repo, fmt.Sprintf("%08x", i+1000), fmt.Sprintf("pl-batch-%03d", i), "repair")
	}
	putReadyMaintenanceIndexState(t, kv, repo.root, "epoch-work-list-batch")
	page, err := repo.ListMaintenanceWorkDetailPage(ctx, "repair", MaintenanceWorkStateReady, "", 129)
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Work) != 129 || page.ScannedCount != 129 || page.BatchGetCount != 4 || page.BatchGetKeyCount != 258 || page.RangePageCount != 1 {
		t.Fatalf("chunked page=%+v", page)
	}
}
