package main

import (
	"context"
	"encoding/json"
	"testing"

	adminv1 "github.com/nosway/namrbd/sbs/admin/v1"
)

type pagedMaintenanceWorkCLIServer struct {
	adminv1.UnimplementedAdminServiceServer
	legacyRepairCalls    int
	legacyRebalanceCalls int
	repairPageCalls      int
	rebalancePageCalls   int
	lastRepairRequest    *adminv1.ListRepairsPageRequest
	lastRebalanceRequest *adminv1.ListRebalancesPageRequest
}

func (s *pagedMaintenanceWorkCLIServer) ListRepairs(context.Context, *adminv1.ListRepairsRequest) (*adminv1.ListRepairsResponse, error) {
	s.legacyRepairCalls++
	return &adminv1.ListRepairsResponse{}, nil
}

func (s *pagedMaintenanceWorkCLIServer) ListRebalances(context.Context, *adminv1.ListRebalancesRequest) (*adminv1.ListRebalancesResponse, error) {
	s.legacyRebalanceCalls++
	return &adminv1.ListRebalancesResponse{}, nil
}

func (s *pagedMaintenanceWorkCLIServer) ListRepairsPage(_ context.Context, req *adminv1.ListRepairsPageRequest) (*adminv1.ListRepairsPageResponse, error) {
	s.repairPageCalls++
	s.lastRepairRequest = req
	return &adminv1.ListRepairsPageResponse{
		ProjectionRevision: "repair-revision", ProjectionHealth: "healthy", NextPageToken: "next-repair-page", ScannedRecords: 128,
		Repairs: []*adminv1.RepairSummary{{VolumeId: "volume-a", PlacementRef: "placement-a", State: "queued"}},
	}, nil
}

func (s *pagedMaintenanceWorkCLIServer) ListRebalancesPage(_ context.Context, req *adminv1.ListRebalancesPageRequest) (*adminv1.ListRebalancesPageResponse, error) {
	s.rebalancePageCalls++
	s.lastRebalanceRequest = req
	return &adminv1.ListRebalancesPageResponse{
		ProjectionRevision: "rebalance-revision", ProjectionHealth: "healthy", NextPageToken: "next-rebalance-page", ScannedRecords: 128,
		Rebalances: []*adminv1.RebalanceSummary{{VolumeId: "volume-b", PlacementRef: "placement-b", State: "running"}},
	}, nil
}

func TestMaintenanceWorkListsDefaultToOnePagedRPC(t *testing.T) {
	server := &pagedMaintenanceWorkCLIServer{}
	installBufconnSBSCTLAdminDialer(t, server)
	repairOutput := captureSBSCTLStdout(t, func() {
		runRepairList([]string{"--sbs-service-endpoint", "bufnet", "--cluster-id", "cluster-a", "--sbs-cluster-id", "sbs-a", "--output", "json"})
	})
	rebalanceOutput := captureSBSCTLStdout(t, func() {
		runRebalanceList([]string{"--sbs-service-endpoint", "bufnet", "--cluster-id", "cluster-a", "--sbs-cluster-id", "sbs-a", "--output", "json"})
	})
	for name, output := range map[string]string{"repair": repairOutput, "rebalance": rebalanceOutput} {
		var result map[string]any
		if err := json.Unmarshal([]byte(output), &result); err != nil {
			t.Fatalf("decode %s JSON: %v\n%s", name, err, output)
		}
		if result["pages_read"] != float64(1) || result["automatic_page_completion"] != false {
			t.Fatalf("%s page output=%+v", name, result)
		}
	}
	if server.legacyRepairCalls != 0 || server.legacyRebalanceCalls != 0 || server.repairPageCalls != 1 || server.rebalancePageCalls != 1 {
		t.Fatalf("legacy repair/rebalance=%d/%d page=%d/%d", server.legacyRepairCalls, server.legacyRebalanceCalls, server.repairPageCalls, server.rebalancePageCalls)
	}
	if server.lastRepairRequest.GetPageSize() != 128 || server.lastRebalanceRequest.GetPageSize() != 128 {
		t.Fatalf("default page sizes repair/rebalance=%d/%d", server.lastRepairRequest.GetPageSize(), server.lastRebalanceRequest.GetPageSize())
	}
}
