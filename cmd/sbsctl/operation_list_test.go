package main

import (
	"context"
	"encoding/json"
	"net"
	"testing"
	"time"

	"github.com/nosway/namrbd/internal/adminclient"
	adminv1 "github.com/nosway/namrbd/sbs/admin/v1"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

func TestParseOperationListTime(t *testing.T) {
	parsed, err := parseOperationListTime("2026-09-03T01:02:03Z", "--updated-after")
	if err != nil || !parsed.AsTime().Equal(time.Date(2026, 9, 3, 1, 2, 3, 0, time.UTC)) {
		t.Fatalf("parsed=%v err=%v", parsed, err)
	}
	if _, err := parseOperationListTime("yesterday", "--updated-after"); err == nil {
		t.Fatal("invalid operation list time accepted")
	}
}

type pagedOperationsCLIServer struct {
	adminv1.UnimplementedOperationsServiceServer
	legacyListCalls int
	pageCalls       int
	getCalls        int
	lastPageRequest *adminv1.ListOperationsPageRequest
	lastGetRequest  *adminv1.GetOperationRequest
}

func (s *pagedOperationsCLIServer) ListOperations(context.Context, *adminv1.ListOperationsRequest) (*adminv1.ListOperationsResponse, error) {
	s.legacyListCalls++
	return &adminv1.ListOperationsResponse{}, nil
}

func (s *pagedOperationsCLIServer) ListOperationsPage(_ context.Context, req *adminv1.ListOperationsPageRequest) (*adminv1.ListOperationsPageResponse, error) {
	s.pageCalls++
	s.lastPageRequest = req
	return &adminv1.ListOperationsPageResponse{
		ProjectionRevision: "revision-a", ProjectionHealth: "healthy", NextPageToken: "next-operation-page", ScannedRecords: 128,
		Operations: []*adminv1.OperationStatus{{OperationId: "op-1", Kind: "node.drain", State: adminv1.OperationState_OPERATION_STATE_RUNNING, TargetNodeId: "node1"}},
	}, nil
}

func (s *pagedOperationsCLIServer) GetOperation(_ context.Context, req *adminv1.GetOperationRequest) (*adminv1.GetOperationResponse, error) {
	s.getCalls++
	s.lastGetRequest = req
	return &adminv1.GetOperationResponse{Operation: &adminv1.OperationStatus{
		OperationId: req.GetOperationId(), Kind: "node.drain", State: adminv1.OperationState_OPERATION_STATE_RUNNING, TargetNodeId: "node1",
	}}, nil
}

func TestOperationListDefaultUsesOnePagedRPC(t *testing.T) {
	server := &pagedOperationsCLIServer{}
	installBufconnSBSCTLOperationsDialer(t, server)
	output := captureSBSCTLStdout(t, func() {
		runOperationList([]string{"--sbs-service-endpoint", "bufnet", "--cluster-id", "cluster-a", "--sbs-cluster-id", "sbs-a", "--output", "json"})
	})
	var result map[string]any
	if err := json.Unmarshal([]byte(output), &result); err != nil {
		t.Fatalf("decode operations list JSON: %v\n%s", err, output)
	}
	if server.legacyListCalls != 0 || server.pageCalls != 1 || server.lastPageRequest.GetPageSize() != operationListDefaultPageSize {
		t.Fatalf("legacy/page/default-size=%d/%d/%d", server.legacyListCalls, server.pageCalls, server.lastPageRequest.GetPageSize())
	}
	if result["pages_read"] != float64(1) || result["automatic_page_completion"] != false || result["next_page_token"] != "next-operation-page" {
		t.Fatalf("paged operation output=%+v", result)
	}
}

func TestNodeDrainStatusUsesOperationPointRPC(t *testing.T) {
	server := &pagedOperationsCLIServer{}
	installBufconnSBSCTLOperationsDialer(t, server)
	output := captureSBSCTLStdout(t, func() {
		runNodeDrainStatus([]string{
			"--sbs-service-endpoint", "bufnet", "--cluster-id", "cluster-a", "--sbs-cluster-id", "sbs-a",
			"--operation-id", "op-drain-1", "--node-id", "node1", "--output", "json",
		})
	})
	var result map[string]any
	if err := json.Unmarshal([]byte(output), &result); err != nil {
		t.Fatalf("decode drain status JSON: %v\n%s", err, output)
	}
	if server.getCalls != 1 || server.legacyListCalls != 0 || server.pageCalls != 0 || server.lastGetRequest.GetOperationId() != "op-drain-1" {
		t.Fatalf("get/legacy/page/id=%d/%d/%d/%q", server.getCalls, server.legacyListCalls, server.pageCalls, server.lastGetRequest.GetOperationId())
	}
	if result["operation_id"] != "op-drain-1" || result["target_node_id"] != "node1" {
		t.Fatalf("drain point output=%+v", result)
	}
}

func installBufconnSBSCTLOperationsDialer(t *testing.T, server adminv1.OperationsServiceServer) {
	t.Helper()
	listener := bufconn.Listen(1024 * 1024)
	grpcServer := grpc.NewServer()
	adminv1.RegisterOperationsServiceServer(grpcServer, server)
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(func() {
		grpcServer.Stop()
		_ = listener.Close()
	})
	var conn *grpc.ClientConn
	oldDial := dialAdminClient
	dialAdminClient = func(ctx context.Context, _ string) (*adminclient.Client, error) {
		var err error
		conn, err = grpc.DialContext(ctx, "bufnet",
			grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }),
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		)
		if err != nil {
			return nil, err
		}
		return &adminclient.Client{Operations: adminv1.NewOperationsServiceClient(conn)}, nil
	}
	t.Cleanup(func() {
		dialAdminClient = oldDial
		if conn != nil {
			_ = conn.Close()
		}
	})
}
