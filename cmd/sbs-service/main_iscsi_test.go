package main

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/nosway/namrbd/gateway/service"
	"github.com/nosway/namrbd/iscsi"
	adminv1 "github.com/nosway/namrbd/sbs/admin/v1"
	clustermeta "github.com/nosway/namrbd/sbs/cluster/metadata"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestISCSIRegistryReadAPIsReturnDurableState(t *testing.T) {
	ctx := context.Background()
	srv := newTestISCSIRegistryServer(t)

	err := srv.saveISCSIRegistryState(ctx, &iscsiRegistryState{
		RegistryRevision: 7,
		ConfigGeneration: 3,
		Portals: map[string]iscsi.Portal{
			"portal-a": {PortalID: "portal-a", Address: "10.0.0.11:3260", GatewayID: "gw-a", Enabled: true},
		},
		Targets: map[string]iscsi.Target{
			"iqn.2026-06.io.namrbd:cluster": {
				TargetIQN: "iqn.2026-06.io.namrbd:cluster",
				PortalID:  "portal-a",
				ExportID:  "export-a",
				Enabled:   true,
			},
		},
		LUNs: map[string]iscsi.LUN{
			"iqn.2026-06.io.namrbd:cluster#0": {
				TargetIQN:             "iqn.2026-06.io.namrbd:cluster",
				LUNID:                 0,
				ExportID:              "export-a",
				VolumeID:              "00a1b2c3",
				ExportMode:            "read_write",
				LogicalBlockSizeBytes: 4096,
				Enabled:               true,
			},
		},
		ACLs: map[string]iscsi.InitiatorACL{
			"iqn.1994-05.com.redhat:node-a@iqn.2026-06.io.namrbd:cluster": {
				InitiatorIQN:  "iqn.1994-05.com.redhat:node-a",
				TargetIQN:     "iqn.2026-06.io.namrbd:cluster",
				AllowedLUNs:   []uint64{0},
				AuthMode:      "chap",
				CHAPSecretSet: true,
				CHAPSecretRef: "vault:iscsi/node-a",
				Enabled:       true,
			},
		},
		Sessions: map[string]iscsi.Session{
			"sess-a": {
				SessionID:        "sess-a",
				TargetIQN:        "iqn.2026-06.io.namrbd:cluster",
				InitiatorIQN:     "iqn.1994-05.com.redhat:node-a",
				LUNID:            0,
				ISCSIGatewayID:   "gw-a",
				Connected:        true,
				ReadWriteAllowed: true,
				BytesRead:        11,
				BytesWritten:     22,
				FlushCount:       2,
			},
		},
		Failovers: map[string]iscsi.FailoverRuntime{
			"export-a": {
				ExportID:             "export-a",
				ActiveISCSIGatewayID: "gw-a",
				ExportEpoch:          2,
				State:                "active",
			},
		},
	})
	if err != nil {
		t.Fatalf("saveISCSIRegistryState: %v", err)
	}

	resp, err := srv.GetISCSIRegistry(ctx, &adminv1.GetISCSIRegistryRequest{Cluster: testISCSIClusterRef()})
	if err != nil {
		t.Fatalf("GetISCSIRegistry: %v", err)
	}
	if resp.GetRegistryRevision() != 7 || resp.GetConfigGeneration() != 3 {
		t.Fatalf("registry revision/generation=(%d,%d) want=(7,3)", resp.GetRegistryRevision(), resp.GetConfigGeneration())
	}
	if len(resp.GetPortals()) != 1 || len(resp.GetTargets()) != 1 || len(resp.GetLuns()) != 1 || len(resp.GetInitiatorAcls()) != 1 || len(resp.GetSessions()) != 1 || len(resp.GetFailovers()) != 1 {
		t.Fatalf("unexpected registry cardinality portals=%d targets=%d luns=%d acls=%d sessions=%d failovers=%d",
			len(resp.GetPortals()), len(resp.GetTargets()), len(resp.GetLuns()), len(resp.GetInitiatorAcls()), len(resp.GetSessions()), len(resp.GetFailovers()))
	}
	if resp.GetLuns()[0].GetLunWwn() == "" {
		t.Fatalf("LUN WWN should be normalized from export id: %#v", resp.GetLuns()[0])
	}
	if counters := resp.GetObservabilityCounters(); counters.GetSessionCount() != 1 || counters.GetConnectedSessions() != 1 || counters.GetBytesWritten() != 22 {
		t.Fatalf("unexpected counters: %#v", counters)
	}

	listResp, err := srv.ListISCSILUNs(ctx, &adminv1.ListISCSILUNsRequest{Cluster: testISCSIClusterRef(), TargetIqn: "iqn.2026-06.io.namrbd:cluster"})
	if err != nil {
		t.Fatalf("ListISCSILUNs: %v", err)
	}
	if len(listResp.GetLuns()) != 1 || listResp.GetLuns()[0].GetVolumeId() != "00a1b2c3" {
		t.Fatalf("unexpected filtered LUNs: %#v", listResp.GetLuns())
	}

	getResp, err := srv.GetISCSILUN(ctx, &adminv1.GetISCSILUNRequest{Cluster: testISCSIClusterRef(), TargetIqn: "iqn.2026-06.io.namrbd:cluster", LunId: 0})
	if err != nil {
		t.Fatalf("GetISCSILUN: %v", err)
	}
	if getResp.GetLun().GetExportMode() != "read_write" || getResp.GetLun().GetLogicalBlockSizeBytes() != 4096 {
		t.Fatalf("unexpected LUN: %#v", getResp.GetLun())
	}

	_, err = srv.GetISCSILUN(ctx, &adminv1.GetISCSILUNRequest{Cluster: testISCSIClusterRef(), TargetIqn: "iqn.2026-06.io.namrbd:cluster", LunId: 9})
	if status.Code(err) != codes.NotFound {
		t.Fatalf("missing LUN code=%v err=%v want NotFound", status.Code(err), err)
	}
}

func TestISCSIRegistryReadAPIsTreatMissingRegistryAsEmpty(t *testing.T) {
	ctx := context.Background()
	srv := newTestISCSIRegistryServer(t)

	resp, err := srv.GetISCSIRegistry(ctx, &adminv1.GetISCSIRegistryRequest{Cluster: testISCSIClusterRef()})
	if err != nil {
		t.Fatalf("GetISCSIRegistry empty: %v", err)
	}
	if resp.GetRegistryRevision() != 0 || len(resp.GetLuns()) != 0 || resp.GetObservabilityCounters().GetSessionCount() != 0 {
		t.Fatalf("unexpected empty registry response: %#v", resp)
	}
}

func TestISCSIRegistrySplitStorePagesOneThousandExports(t *testing.T) {
	ctx := context.Background()
	srv := newTestISCSIRegistryServer(t)
	state := newISCSIRegistryState()
	state.RegistryRevision = 77
	state.ConfigGeneration = 11
	for i := 0; i < 32; i++ {
		portalID := fmt.Sprintf("portal-%02d", i)
		state.Portals[portalID] = iscsi.Portal{
			PortalID: portalID, Address: fmt.Sprintf("10.20.0.%d:3260", i+1),
			GatewayID: fmt.Sprintf("iscsi-gw-%02d", i), Enabled: true,
		}
	}
	for i := 0; i < 1000; i++ {
		targetIQN := fmt.Sprintf("iqn.2026-08.io.namrbd:target-%04d", i)
		exportID := fmt.Sprintf("export-%04d", i)
		portalID := fmt.Sprintf("portal-%02d", i%32)
		state.Targets[targetIQN] = iscsi.Target{
			TargetIQN: targetIQN, PortalID: portalID, PortalIDs: []string{portalID},
			ExportID: exportID, Enabled: true,
		}
		state.LUNs[iscsi.LUNKey(targetIQN, 0)] = iscsi.LUN{
			TargetIQN: targetIQN, LUNID: 0, LUNWWN: iscsi.LUNWWN(exportID),
			ExportID: exportID, VolumeID: fmt.Sprintf("%08x", i+1), ExportMode: "read_write",
			LogicalBlockSizeBytes: 4096, Enabled: true,
		}
		state.Failovers[exportID] = iscsi.FailoverRuntime{
			ExportID: exportID, ActiveISCSIGatewayID: fmt.Sprintf("iscsi-gw-%02d", i%32),
			StandbyISCSIGatewayIDs: []string{fmt.Sprintf("iscsi-gw-%02d", (i+1)%32)},
			ExportLeaseID:          fmt.Sprintf("lease-%04d", i), ExportEpoch: uint64(i + 1),
			State: "active", WriterPolicy: "active_only", HAFailoverMode: "automatic",
		}
	}
	if err := srv.saveISCSIRegistryState(ctx, state); err != nil {
		t.Fatalf("save split registry: %v", err)
	}

	summary, err := srv.GetISCSIRegistry(ctx, &adminv1.GetISCSIRegistryRequest{
		Cluster: testISCSIClusterRef(), SummaryOnly: true,
	})
	if err != nil {
		t.Fatalf("GetISCSIRegistry summary: %v", err)
	}
	if summary.GetServingRegistryAuthority() != iscsiServingRegistryAuthority ||
		summary.GetStorageLayout() != iscsiRegistryLayoutSplit || summary.GetRegistryEmpty() ||
		summary.GetExportCount() != 1000 || summary.GetTargetCount() != 1000 || summary.GetLunCount() != 1000 {
		t.Fatalf("registry summary = %+v", summary)
	}
	if len(summary.GetTargets()) != 0 || len(summary.GetLuns()) != 0 {
		t.Fatal("summary-only registry response returned object collections")
	}

	seen := map[string]bool{}
	token := ""
	pages := 0
	for {
		page, err := srv.ListISCSIExports(ctx, &adminv1.ListISCSIExportsRequest{
			Cluster: testISCSIClusterRef(), PageSize: 128, PageToken: token, RegistryRevision: 77,
		})
		if err != nil {
			t.Fatalf("ListISCSIExports page %d: %v", pages, err)
		}
		pages++
		if len(page.GetExports()) == 0 || len(page.GetExports()) > 128 {
			t.Fatalf("page %d size=%d", pages, len(page.GetExports()))
		}
		for _, export := range page.GetExports() {
			if seen[export.GetExportId()] {
				t.Fatalf("duplicate export %s", export.GetExportId())
			}
			seen[export.GetExportId()] = true
			if export.GetVolumeId() == "" || export.GetActiveIscsiGatewayId() == "" ||
				export.GetExportLeaseId() == "" || export.GetExportEpoch() == 0 {
				t.Fatalf("export lacks serving/fencing evidence: %+v", export)
			}
		}
		token = page.GetNextPageToken()
		if token == "" {
			break
		}
	}
	if len(seen) != 1000 || pages != 8 {
		t.Fatalf("paged exports=%d pages=%d", len(seen), pages)
	}

	point, err := srv.GetISCSIExport(ctx, &adminv1.GetISCSIExportRequest{
		Cluster: testISCSIClusterRef(), ExportId: "export-0999",
	})
	if err != nil {
		t.Fatalf("GetISCSIExport: %v", err)
	}
	if point.GetExport().GetVolumeId() != "000003e8" || point.GetRegistryRevision() != 77 ||
		point.GetServingRegistryAuthority() != iscsiServingRegistryAuthority {
		t.Fatalf("point export = %+v", point)
	}
	if _, err := srv.GetISCSIExport(ctx, &adminv1.GetISCSIExportRequest{
		Cluster: testISCSIClusterRef(), ExportId: "missing",
	}); status.Code(err) != codes.NotFound {
		t.Fatalf("missing export code=%v err=%v", status.Code(err), err)
	}
	if _, err := srv.ListISCSIExports(ctx, &adminv1.ListISCSIExportsRequest{
		Cluster: testISCSIClusterRef(), PageSize: 128, RegistryRevision: 76,
	}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("stale page revision code=%v err=%v", status.Code(err), err)
	}
}

func TestISCSIRegistryLegacyStateMigratesAtomicallyToSplitV2(t *testing.T) {
	ctx := context.Background()
	srv := newTestISCSIRegistryServer(t)
	legacy := newISCSIRegistryState()
	legacy.RegistryRevision = 4
	legacy.ConfigGeneration = 2
	legacy.Targets["iqn.2026-08.io.namrbd:legacy"] = iscsi.Target{
		TargetIQN: "iqn.2026-08.io.namrbd:legacy", ExportID: "legacy-export", Enabled: true,
	}
	legacy.LUNs[iscsi.LUNKey("iqn.2026-08.io.namrbd:legacy", 0)] = iscsi.LUN{
		TargetIQN: "iqn.2026-08.io.namrbd:legacy", LUNID: 0, ExportID: "legacy-export",
		VolumeID: "00000001", ExportMode: "read_write", Enabled: true,
	}
	raw, err := json.Marshal(legacy)
	if err != nil {
		t.Fatal(err)
	}
	if err := srv.kv.Set(ctx, srv.iscsiRegistryKey(), raw); err != nil {
		t.Fatalf("seed legacy registry: %v", err)
	}
	loaded, err := srv.loadISCSIRegistryState(ctx)
	if err != nil {
		t.Fatalf("load legacy registry: %v", err)
	}
	if loaded.storageLayout != iscsiRegistryLayoutLegacy {
		t.Fatalf("legacy layout=%q", loaded.storageLayout)
	}
	before, err := cloneISCSIRegistryState(loaded)
	if err != nil {
		t.Fatal(err)
	}
	loaded.RegistryRevision++
	loaded.ConfigGeneration++
	if err := srv.saveISCSIRegistryStateDelta(ctx, before, loaded); err != nil {
		t.Fatalf("migrate registry: %v", err)
	}
	manifest, found, err := srv.loadISCSIRegistryManifest(ctx)
	if err != nil || !found || manifest.StorageLayout != iscsiRegistryLayoutSplit {
		t.Fatalf("split manifest found=%t manifest=%+v err=%v", found, manifest, err)
	}
	if _, found, err := srv.kv.Get(ctx, srv.iscsiRegistryKey()); err != nil || found {
		t.Fatalf("legacy authority remained found=%t err=%v", found, err)
	}
	point, err := srv.GetISCSIExport(ctx, &adminv1.GetISCSIExportRequest{
		Cluster: testISCSIClusterRef(), ExportId: "legacy-export",
	})
	if err != nil || point.GetExport().GetVolumeId() != "00000001" {
		t.Fatalf("migrated export=%+v err=%v", point, err)
	}
}

func TestISCSIRegistryChangedSetIsBoundedCheckpointedAndResyncsAfterGap(t *testing.T) {
	ctx := context.Background()
	srv := newTestISCSIRegistryServer(t)
	before := newISCSIRegistryState()
	before.storageLayout = iscsiRegistryLayoutSplit
	after, err := cloneISCSIRegistryState(before)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 300; i++ {
		targetIQN := fmt.Sprintf("iqn.2026-08.io.namrbd:change-%03d", i)
		exportID := fmt.Sprintf("change-export-%03d", i)
		after.Targets[targetIQN] = iscsi.Target{
			TargetIQN: targetIQN, PortalID: "portal-a", PortalIDs: []string{"portal-a"},
			ExportID: exportID, Enabled: true,
		}
		after.LUNs[iscsi.LUNKey(targetIQN, 0)] = iscsi.LUN{
			TargetIQN: targetIQN, LUNID: 0, ExportID: exportID,
			VolumeID: fmt.Sprintf("%08x", i+1), ExportMode: "read_write", Enabled: true,
		}
	}
	after.RegistryRevision = 1
	after.ConfigGeneration = 1
	if err := srv.saveISCSIRegistryStateDelta(ctx, before, after); err != nil {
		t.Fatalf("save initial changed set: %v", err)
	}

	token := ""
	changeCount := 0
	pageCount := 0
	for {
		page, err := srv.GetISCSIRegistryChanges(ctx, &adminv1.GetISCSIRegistryChangesRequest{
			Cluster: testISCSIClusterRef(), AfterRevision: 0, PageSize: 128, PageToken: token,
		})
		if err != nil {
			t.Fatalf("GetISCSIRegistryChanges page %d: %v", pageCount, err)
		}
		pageCount++
		if len(page.GetChanges()) == 0 || len(page.GetChanges()) > 128 || page.GetResyncRequired() {
			t.Fatalf("changed page %d = %+v", pageCount, page)
		}
		changeCount += len(page.GetChanges())
		token = page.GetNextPageToken()
		if token == "" {
			if page.GetCheckpointRevision() != 1 || page.GetToRevision() != 1 {
				t.Fatalf("final checkpoint = %+v", page)
			}
			break
		}
		if page.GetCheckpointRevision() != 0 {
			t.Fatalf("intermediate page advanced checkpoint: %+v", page)
		}
	}
	if changeCount != 300 || pageCount != 3 {
		t.Fatalf("changed exports=%d pages=%d", changeCount, pageCount)
	}

	current, err := srv.loadISCSIRegistryState(ctx)
	if err != nil {
		t.Fatal(err)
	}
	beforeUpdate, err := cloneISCSIRegistryState(current)
	if err != nil {
		t.Fatal(err)
	}
	current.RegistryRevision = 2
	current.ConfigGeneration = 2
	current.Failovers["change-export-299"] = iscsi.FailoverRuntime{
		ExportID: "change-export-299", ActiveISCSIGatewayID: "iscsi-gw-01",
		ExportLeaseID: "lease-299", ExportEpoch: 9, State: "active",
	}
	if err := srv.saveISCSIRegistryStateDelta(ctx, beforeUpdate, current); err != nil {
		t.Fatalf("save failover changed set: %v", err)
	}
	page, err := srv.GetISCSIRegistryChanges(ctx, &adminv1.GetISCSIRegistryChangesRequest{
		Cluster: testISCSIClusterRef(), AfterRevision: 1, PageSize: 128,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.GetChanges()) != 1 || page.GetChanges()[0].GetOperation() != "upsert" ||
		page.GetChanges()[0].GetExport().GetExportEpoch() != 9 || page.GetCheckpointRevision() != 2 {
		t.Fatalf("failover change = %+v", page)
	}
	baseKV := srv.kv
	countedKV := &countingISCSIRegistryKV{KV: baseKV}
	srv.kv = countedKV
	unchanged, err := srv.GetISCSIRegistryChanges(ctx, &adminv1.GetISCSIRegistryChangesRequest{
		Cluster: testISCSIClusterRef(), AfterRevision: 2,
	})
	if err != nil || len(unchanged.GetChanges()) != 0 || unchanged.GetCheckpointRevision() != 2 {
		t.Fatalf("unchanged checkpoint = %+v err=%v", unchanged, err)
	}
	if countedKV.listCalls != 0 || countedKV.getCalls != 1 {
		t.Fatalf("unchanged checkpoint reads: get=%d list=%d, want one manifest get and no scan", countedKV.getCalls, countedKV.listCalls)
	}
	srv.kv = baseKV

	beforeDelete, err := cloneISCSIRegistryState(current)
	if err != nil {
		t.Fatal(err)
	}
	delete(current.LUNs, iscsi.LUNKey("iqn.2026-08.io.namrbd:change-000", 0))
	current.RegistryRevision = 3
	current.ConfigGeneration = 3
	if err := srv.saveISCSIRegistryStateDelta(ctx, beforeDelete, current); err != nil {
		t.Fatalf("save delete changed set: %v", err)
	}
	deleted, err := srv.GetISCSIRegistryChanges(ctx, &adminv1.GetISCSIRegistryChangesRequest{
		Cluster: testISCSIClusterRef(), AfterRevision: 2,
	})
	if err != nil || len(deleted.GetChanges()) != 1 || deleted.GetChanges()[0].GetOperation() != "delete" ||
		deleted.GetChanges()[0].GetExport() != nil {
		t.Fatalf("delete change = %+v err=%v", deleted, err)
	}

	manifest, found, err := srv.loadISCSIRegistryManifest(ctx)
	if err != nil || !found {
		t.Fatalf("load manifest found=%t err=%v", found, err)
	}
	manifest.ChangeFloorRevision = 2
	raw, err := json.Marshal(manifest)
	if err != nil {
		t.Fatal(err)
	}
	if err := srv.kv.Set(ctx, srv.iscsiRegistryManifestKey(), raw); err != nil {
		t.Fatal(err)
	}
	resync, err := srv.GetISCSIRegistryChanges(ctx, &adminv1.GetISCSIRegistryChangesRequest{
		Cluster: testISCSIClusterRef(), AfterRevision: 1,
	})
	if err != nil || !resync.GetResyncRequired() || resync.GetChangeFloorRevision() != 2 ||
		resync.GetCheckpointRevision() != 0 {
		t.Fatalf("resync response = %+v err=%v", resync, err)
	}
}

type countingISCSIRegistryKV struct {
	clustermeta.KV
	getCalls  int
	listCalls int
}

func (k *countingISCSIRegistryKV) Get(ctx context.Context, key string) ([]byte, bool, error) {
	k.getCalls++
	return k.KV.Get(ctx, key)
}

func (k *countingISCSIRegistryKV) List(ctx context.Context, prefix, cursor string, limit int) ([]string, string, error) {
	k.listCalls++
	return k.KV.List(ctx, prefix, cursor, limit)
}

func TestDependencyEnforcementBlocksISCSIAdmissionAndSuppressesFailover(t *testing.T) {
	ctx := context.Background()
	tr := installBlockedDependencyTracker(t)
	srv := newTestISCSIRegistryServer(t)

	_, _, err := srv.mutateISCSIRegistry(ctx, testISCSIMutationMeta(), "blocked-export", 0, "iscsi.lun.export", "iqn.test:0", "vol-a", "blocked", func(state *iscsiRegistryState) error {
		t.Fatal("export mutation callback ran despite blocked dependency view")
		return nil
	})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("iscsi.lun.export code=%v err=%v want FailedPrecondition", status.Code(err), err)
	}
	if got := tr.Status().AdmissionBlockedCount; got != 1 {
		t.Fatalf("export_admission_blocked_count=%d want 1", got)
	}

	_, _, err = srv.mutateISCSIRegistry(ctx, testISCSIMutationMeta(), "blocked-failover", 0, "iscsi.failover.promote", "iqn.test:0:gw-b", "vol-a", "blocked", func(state *iscsiRegistryState) error {
		t.Fatal("failover mutation callback ran despite blocked dependency view")
		return nil
	})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("iscsi.failover.promote code=%v err=%v want FailedPrecondition", status.Code(err), err)
	}
	if got := tr.Status().FailoverSuppressCount; got != 1 {
		t.Fatalf("failover_suppressed_count=%d want 1", got)
	}
}

func TestISCSIRegistryMutationsPersistRevisionAndValidateReferences(t *testing.T) {
	ctx := context.Background()
	srv := newTestISCSIRegistryServer(t)
	if err := srv.repo.PutVolumeState(ctx, clustermeta.VolumeState{
		VolumeID: "00a1b2c3",
		Epoch:    1,
		Revision: 1,
		Status:   clustermeta.VolumeStatusHealthy,
	}); err != nil {
		t.Fatalf("PutVolumeState: %v", err)
	}

	portalResp, err := srv.CreateISCSIPortal(ctx, &adminv1.CreateISCSIPortalRequest{
		Cluster:        testISCSIClusterRef(),
		Meta:           testISCSIMutationMeta(),
		IdempotencyKey: "idem-portal-create",
		PortalId:       "portal-a",
		Address:        "10.0.0.11:3260",
		GatewayId:      "gw-a",
		Enabled:        true,
	})
	if err != nil {
		t.Fatalf("CreateISCSIPortal: %v", err)
	}
	if portalResp.GetRegistryRevision() != 1 || !portalResp.GetOperation().GetAccepted() || portalResp.GetPortal().GetGatewayId() != "gw-a" {
		t.Fatalf("unexpected portal response: %#v", portalResp)
	}

	portalReplay, err := srv.CreateISCSIPortal(ctx, &adminv1.CreateISCSIPortalRequest{
		Cluster:        testISCSIClusterRef(),
		Meta:           testISCSIMutationMeta(),
		IdempotencyKey: "idem-portal-create",
		PortalId:       "portal-a",
		Address:        "10.0.0.11:3260",
		GatewayId:      "gw-a",
		Enabled:        true,
	})
	if err != nil {
		t.Fatalf("CreateISCSIPortal replay: %v", err)
	}
	if portalReplay.GetRegistryRevision() != 1 || portalReplay.GetOperation().GetAccepted() {
		t.Fatalf("unexpected portal replay response: %#v", portalReplay)
	}

	targetResp, err := srv.CreateISCSITarget(ctx, &adminv1.CreateISCSITargetRequest{
		Cluster:                  testISCSIClusterRef(),
		Meta:                     testISCSIMutationMeta(),
		IdempotencyKey:           "idem-target-create",
		ExpectedRegistryRevision: 1,
		TargetIqn:                "iqn.2026-06.io.namrbd:cluster",
		PortalIds:                []string{"portal-a"},
		ExportId:                 "export-a",
		Enabled:                  true,
	})
	if err != nil {
		t.Fatalf("CreateISCSITarget: %v", err)
	}
	if targetResp.GetRegistryRevision() != 2 || targetResp.GetTarget().GetPortalId() != "portal-a" {
		t.Fatalf("unexpected target response: %#v", targetResp)
	}

	lunResp, err := srv.ExportISCSILUN(ctx, &adminv1.ExportISCSILUNRequest{
		Cluster:                  testISCSIClusterRef(),
		Meta:                     testISCSIMutationMeta(),
		IdempotencyKey:           "idem-lun-export",
		ExpectedRegistryRevision: 2,
		TargetIqn:                "iqn.2026-06.io.namrbd:cluster",
		LunId:                    0,
		ExportId:                 "export-a",
		VolumeId:                 "00a1b2c3",
		ExportMode:               "read_write",
		LogicalBlockSizeBytes:    4096,
		Enabled:                  true,
	})
	if err != nil {
		t.Fatalf("ExportISCSILUN: %v", err)
	}
	if lunResp.GetRegistryRevision() != 3 || lunResp.GetLun().GetVolumeId() != "00a1b2c3" || lunResp.GetLun().GetLunWwn() == "" {
		t.Fatalf("unexpected lun response: %#v", lunResp)
	}

	allowResp, err := srv.AllowISCSIInitiator(ctx, &adminv1.AllowISCSIInitiatorRequest{
		Cluster:                  testISCSIClusterRef(),
		Meta:                     testISCSIMutationMeta(),
		IdempotencyKey:           "idem-initiator-allow",
		ExpectedRegistryRevision: 3,
		InitiatorIqn:             "iqn.1994-05.com.redhat:node-a",
		TargetIqn:                "iqn.2026-06.io.namrbd:cluster",
		AllowedLunIds:            []uint64{0},
		AuthMode:                 "chap",
		ChapSecretRef:            "vault:iscsi/node-a",
		Enabled:                  true,
	})
	if err != nil {
		t.Fatalf("AllowISCSIInitiator: %v", err)
	}
	if allowResp.GetRegistryRevision() != 4 || !allowResp.GetInitiatorAcl().GetChapSecretSet() {
		t.Fatalf("unexpected allow response: %#v", allowResp)
	}

	modeResp, err := srv.SetISCSILUNMode(ctx, &adminv1.SetISCSILUNModeRequest{
		Cluster:                  testISCSIClusterRef(),
		Meta:                     testISCSIMutationMeta(),
		IdempotencyKey:           "idem-lun-mode",
		ExpectedRegistryRevision: 4,
		TargetIqn:                "iqn.2026-06.io.namrbd:cluster",
		LunId:                    0,
		ExportMode:               "read_only",
	})
	if err != nil {
		t.Fatalf("SetISCSILUNMode: %v", err)
	}
	if modeResp.GetRegistryRevision() != 5 || modeResp.GetLun().GetExportMode() != "read_only" {
		t.Fatalf("unexpected mode response: %#v", modeResp)
	}

	recordResp, err := srv.RecordISCSISession(ctx, &adminv1.RecordISCSISessionRequest{
		Cluster:                  testISCSIClusterRef(),
		Meta:                     testISCSIMutationMeta(),
		IdempotencyKey:           "idem-session-record",
		ExpectedRegistryRevision: 5,
		Session: &adminv1.ISCSISessionSummary{
			SessionId:            "sess-a",
			TargetIqn:            "iqn.2026-06.io.namrbd:cluster",
			InitiatorIqn:         "iqn.1994-05.com.redhat:node-a",
			LunId:                0,
			IscsiGatewayId:       "gw-a",
			Connected:            true,
			ConnectionCount:      1,
			ActiveIscsiGatewayId: "gw-a",
			ExportEpoch:          1,
			ReadWriteAllowed:     true,
			BytesWritten:         33,
		},
	})
	if err != nil {
		t.Fatalf("RecordISCSISession: %v", err)
	}
	if recordResp.GetRegistryRevision() != 6 || !recordResp.GetSession().GetConnected() || recordResp.GetSession().GetState() != "connected" {
		t.Fatalf("unexpected record session response: %#v", recordResp)
	}

	disconnectResp, err := srv.DisconnectISCSISession(ctx, &adminv1.DisconnectISCSISessionRequest{
		Cluster:                  testISCSIClusterRef(),
		Meta:                     testISCSIMutationMeta(),
		IdempotencyKey:           "idem-session-disconnect",
		ExpectedRegistryRevision: 6,
		SessionId:                "sess-a",
	})
	if err != nil {
		t.Fatalf("DisconnectISCSISession: %v", err)
	}
	if disconnectResp.GetRegistryRevision() != 7 || !disconnectResp.GetDisconnectRequested() || disconnectResp.GetSession().GetConnected() || disconnectResp.GetSession().GetState() != "disconnect_requested" {
		t.Fatalf("unexpected disconnect response: %#v", disconnectResp)
	}

	sessionListResp, err := srv.ListISCSISessions(ctx, &adminv1.ListISCSISessionsRequest{
		Cluster:       testISCSIClusterRef(),
		TargetIqn:     "iqn.2026-06.io.namrbd:cluster",
		ConnectedOnly: true,
	})
	if err != nil {
		t.Fatalf("ListISCSISessions connected-only: %v", err)
	}
	if len(sessionListResp.GetSessions()) != 0 || sessionListResp.GetObservabilityCounters().GetSessionCount() != 1 || sessionListResp.GetObservabilityCounters().GetConnectedSessions() != 0 {
		t.Fatalf("unexpected session counters after disconnect: %#v", sessionListResp)
	}

	standbyResp, err := srv.StandbyISCSIFailover(ctx, &adminv1.StandbyISCSIFailoverRequest{
		Cluster:                  testISCSIClusterRef(),
		Meta:                     testISCSIMutationMeta(),
		IdempotencyKey:           "idem-failover-standby",
		ExpectedRegistryRevision: 7,
		ExportId:                 "export-a",
		GatewayId:                "gw-b",
	})
	if err != nil {
		t.Fatalf("StandbyISCSIFailover: %v", err)
	}
	if standbyResp.GetRegistryRevision() != 8 || standbyResp.GetFailover().GetExportEpoch() != 1 || !testISCSIStringSliceContains(standbyResp.GetFailover().GetStandbyIscsiGatewayIds(), "gw-b") {
		t.Fatalf("unexpected standby failover response: %#v", standbyResp)
	}
	if standbyResp.GetFailover().GetAluaMode() != iscsi.ALUAModeImplicit ||
		!standbyResp.GetFailover().GetAluaImplicitSupported() ||
		standbyResp.GetFailover().GetAluaExplicitSupported() ||
		standbyResp.GetFailover().GetActiveAluaAccessState() != iscsi.ALUAAccessStateActiveOptimized ||
		standbyResp.GetFailover().GetStandbyAluaAccessState() != iscsi.ALUAAccessStateStandby {
		t.Fatalf("standby failover response missing implicit ALUA summary: %#v", standbyResp.GetFailover())
	}

	srv.iscsiWriterFenceProjector = func(context.Context, service.ISCSIWriterFence) error {
		return fmt.Errorf("receiver unavailable")
	}
	_, err = srv.PromoteISCSIFailover(ctx, &adminv1.PromoteISCSIFailoverRequest{
		Cluster: testISCSIClusterRef(), Meta: testISCSIMutationMeta(),
		IdempotencyKey: "idem-failover-promote-blocked", ExpectedRegistryRevision: 8,
		ExportId: "export-a", GatewayId: "gw-b", ExportLeaseId: "lease-b", Trigger: "etcd_lease_expired",
	})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("PromoteISCSIFailover projection failure code=%v err=%v", status.Code(err), err)
	}
	blockedState, err := srv.loadISCSIRegistryState(ctx)
	if err != nil {
		t.Fatalf("load registry after blocked projection: %v", err)
	}
	if blockedState.RegistryRevision != 8 || blockedState.Failovers["export-a"].ActiveISCSIGatewayID == "gw-b" {
		t.Fatalf("blocked projection published failover: revision=%d runtime=%+v", blockedState.RegistryRevision, blockedState.Failovers["export-a"])
	}

	var projectedFence service.ISCSIWriterFence
	srv.iscsiWriterFenceProjector = func(_ context.Context, fence service.ISCSIWriterFence) error {
		projectedFence = fence
		return nil
	}
	promoteResp, err := srv.PromoteISCSIFailover(ctx, &adminv1.PromoteISCSIFailoverRequest{
		Cluster:                  testISCSIClusterRef(),
		Meta:                     testISCSIMutationMeta(),
		IdempotencyKey:           "idem-failover-promote",
		ExpectedRegistryRevision: 8,
		ExportId:                 "export-a",
		GatewayId:                "gw-b",
		ExportLeaseId:            "lease-b",
		Trigger:                  "manual_test",
	})
	if err != nil {
		t.Fatalf("PromoteISCSIFailover: %v", err)
	}
	if promoteResp.GetRegistryRevision() != 9 || promoteResp.GetFailover().GetActiveIscsiGatewayId() != "gw-b" || promoteResp.GetFailover().GetExportLeaseId() != "lease-b" || promoteResp.GetFailover().GetExportEpoch() != 2 || testISCSIStringSliceContains(promoteResp.GetFailover().GetStandbyIscsiGatewayIds(), "gw-b") {
		t.Fatalf("unexpected promote failover response: %#v", promoteResp)
	}
	if projectedFence != (service.ISCSIWriterFence{
		VolumeID: "00a1b2c3", ExportID: "export-a", ExportLeaseID: "lease-b",
		ExportEpoch: 2, ActiveGatewayID: "gw-b", RegistryRevision: 9,
	}) {
		t.Fatalf("projected writer fence=%+v", projectedFence)
	}
	if promoteResp.GetFailover().GetAluaMode() != iscsi.ALUAModeImplicit ||
		promoteResp.GetFailover().GetActiveAluaAccessState() != iscsi.ALUAAccessStateActiveOptimized ||
		promoteResp.GetFailover().GetStandbyAluaAccessState() != iscsi.ALUAAccessStateStandby {
		t.Fatalf("promote failover response missing implicit ALUA summary: %#v", promoteResp.GetFailover())
	}

	demoteResp, err := srv.DemoteISCSIFailover(ctx, &adminv1.DemoteISCSIFailoverRequest{
		Cluster:                  testISCSIClusterRef(),
		Meta:                     testISCSIMutationMeta(),
		IdempotencyKey:           "idem-failover-demote",
		ExpectedRegistryRevision: 9,
		ExportId:                 "export-a",
		GatewayId:                "gw-b",
	})
	if err != nil {
		t.Fatalf("DemoteISCSIFailover: %v", err)
	}
	if demoteResp.GetRegistryRevision() != 10 || demoteResp.GetFailover().GetActiveIscsiGatewayId() != "" || demoteResp.GetFailover().GetExportEpoch() != 3 || !testISCSIStringSliceContains(demoteResp.GetFailover().GetStandbyIscsiGatewayIds(), "gw-b") {
		t.Fatalf("unexpected demote failover response: %#v", demoteResp)
	}

	revokeResp, err := srv.RevokeStaleISCSIFailover(ctx, &adminv1.RevokeStaleISCSIFailoverRequest{
		Cluster:                  testISCSIClusterRef(),
		Meta:                     testISCSIMutationMeta(),
		IdempotencyKey:           "idem-failover-revoke",
		ExpectedRegistryRevision: 10,
		ExportId:                 "export-a",
		GatewayId:                "gw-b",
	})
	if err != nil {
		t.Fatalf("RevokeStaleISCSIFailover: %v", err)
	}
	if revokeResp.GetRegistryRevision() != 11 || revokeResp.GetFailover().GetStaleGatewayRevokedId() != "gw-b" || !revokeResp.GetFailover().GetStaleGatewayRejected() || revokeResp.GetFailover().GetExportEpoch() != 4 {
		t.Fatalf("unexpected revoke failover response: %#v", revokeResp)
	}
	_, decision := iscsi.EvaluateFailoverWriteAdmission(iscsi.FailoverRuntime{
		ExportID:              "export-a",
		ActiveISCSIGatewayID:  revokeResp.GetFailover().GetActiveIscsiGatewayId(),
		StaleGatewayRevokedID: revokeResp.GetFailover().GetStaleGatewayRevokedId(),
		ExportEpoch:           revokeResp.GetFailover().GetExportEpoch(),
		WriterPolicy:          revokeResp.GetFailover().GetWriterPolicy(),
		HAFailoverMode:        revokeResp.GetFailover().GetHaFailoverMode(),
	}, "gw-b", revokeResp.GetFailover().GetExportEpoch())
	if decision.WriteAdmitted || decision.RejectionReason != "revoked_stale_gateway" || decision.SenseKey != "data_protect" {
		t.Fatalf("revoked gateway write decision=%#v want data-protect rejection", decision)
	}

	_, err = srv.DeleteISCSITarget(ctx, &adminv1.DeleteISCSITargetRequest{
		Cluster:        testISCSIClusterRef(),
		Meta:           testISCSIMutationMeta(),
		IdempotencyKey: "idem-target-delete-blocked",
		TargetIqn:      "iqn.2026-06.io.namrbd:cluster",
	})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("DeleteISCSITarget code=%v err=%v want FailedPrecondition", status.Code(err), err)
	}

	_, err = srv.SetISCSILUNMode(ctx, &adminv1.SetISCSILUNModeRequest{
		Cluster:                  testISCSIClusterRef(),
		Meta:                     testISCSIMutationMeta(),
		IdempotencyKey:           "idem-lun-mode-stale",
		ExpectedRegistryRevision: 99,
		TargetIqn:                "iqn.2026-06.io.namrbd:cluster",
		LunId:                    0,
		ExportMode:               "read_write",
	})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("SetISCSILUNMode stale code=%v err=%v want FailedPrecondition", status.Code(err), err)
	}
}

func newTestISCSIRegistryServer(t *testing.T) *server {
	t.Helper()
	kv, err := clustermeta.OpenPebbleKV(t.TempDir())
	if err != nil {
		t.Fatalf("OpenPebbleKV: %v", err)
	}
	t.Cleanup(func() { _ = kv.Close() })
	return &server{
		clusterID:                 "test-cluster",
		sbsClusterID:              "test-sbs",
		nodeID:                    "svc-1",
		root:                      defaultMetadataRoot,
		startedAt:                 time.Now(),
		kv:                        kv,
		repo:                      clustermeta.NewRepository(kv, defaultMetadataRoot),
		ops:                       newOperationStore(kv, defaultMetadataRoot),
		cache:                     newReplicaClientCache(),
		maint:                     newMaintenanceSettings(),
		iscsiWriterFenceProjector: func(context.Context, service.ISCSIWriterFence) error { return nil },
	}
}

func testISCSIClusterRef() *adminv1.ClusterRef {
	return &adminv1.ClusterRef{ClusterId: "test-cluster", SbsClusterId: "test-sbs"}
}

func testISCSIMutationMeta() *adminv1.RequestMeta {
	return &adminv1.RequestMeta{Actor: "test-admin", Reason: "unit test"}
}

func testISCSIStringSliceContains(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}
