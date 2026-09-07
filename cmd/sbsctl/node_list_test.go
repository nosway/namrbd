package main

import (
	"context"
	"strings"
	"testing"

	adminv1 "github.com/nosway/namrbd/sbs/admin/v1"

	"google.golang.org/grpc"
)

type pagedNodeListClient struct {
	adminv1.AdminServiceClient
	pages    []*adminv1.ListNodesResponse
	requests []*adminv1.ListNodesRequest
}

func (c *pagedNodeListClient) ListNodes(_ context.Context, req *adminv1.ListNodesRequest, _ ...grpc.CallOption) (*adminv1.ListNodesResponse, error) {
	c.requests = append(c.requests, req)
	return c.pages[len(c.requests)-1], nil
}

func TestListNodePagesDefaultDoesNotCompleteNextPage(t *testing.T) {
	client := &pagedNodeListClient{pages: []*adminv1.ListNodesResponse{
		{
			Nodes:              []*adminv1.NodeSummary{{NodeId: "node1"}, {NodeId: "node2"}},
			MembershipRevision: 160, MembershipProjectionRevision: 160,
			ProjectionHealth: "healthy", NextPageToken: "opaque-page-2",
		},
		{Nodes: []*adminv1.NodeSummary{{NodeId: "node3"}}, MembershipRevision: 160, MembershipProjectionRevision: 160, ProjectionHealth: "healthy"},
	}}
	options := nodeListOptions{PageSize: 128}
	result, err := listNodePages(context.Background(), client, &adminv1.ClusterRef{}, options)
	if err != nil {
		t.Fatal(err)
	}
	if len(client.requests) != 1 || result.PagesRead != 1 || result.AutomaticCompletion || len(result.Response.GetNodes()) != 2 || result.Response.GetNextPageToken() != "opaque-page-2" {
		t.Fatalf("requests=%d result=%+v", len(client.requests), result)
	}
	output := nodeListJSON(result, options)
	if output["automatic_page_completion"] != false || output["expensive_all_requested"] != false || output["next_page_token"] != "opaque-page-2" {
		t.Fatalf("output=%+v", output)
	}
}

func TestListNodePagesExplicitAllHonorsReasonAndBudget(t *testing.T) {
	client := &pagedNodeListClient{pages: []*adminv1.ListNodesResponse{
		{
			Nodes:              []*adminv1.NodeSummary{{NodeId: "node1"}, {NodeId: "node2"}},
			MembershipRevision: 160, MembershipProjectionRevision: 160,
			ProjectionHealth: "healthy", NextPageToken: "opaque-page-2",
		},
		{
			Nodes:              []*adminv1.NodeSummary{{NodeId: "node3"}},
			MembershipRevision: 160, MembershipProjectionRevision: 160,
			ProjectionHealth: "healthy", NextPageToken: "opaque-page-3",
		},
	}}
	options := nodeListOptions{PageSize: 2, All: true, Reason: "incident export", Budget: 3}
	result, err := listNodePages(context.Background(), client, &adminv1.ClusterRef{}, options)
	if err != nil {
		t.Fatal(err)
	}
	if len(client.requests) != 2 || client.requests[1].GetPageSize() != 1 || len(result.Response.GetNodes()) != 3 || !result.AutomaticCompletion || result.Response.GetNextPageToken() != "opaque-page-3" {
		t.Fatalf("requests=%+v result=%+v", client.requests, result)
	}
	output := nodeListJSON(result, options)
	if output["expensive_reason"] != "incident export" || output["expensive_node_budget"] != uint64(3) || output["result_truncated_by_budget"] != true {
		t.Fatalf("output=%+v", output)
	}
}

func TestNodeListOptionsRejectsImplicitCompletion(t *testing.T) {
	for _, tc := range []struct {
		name    string
		options nodeListOptions
		want    string
	}{
		{name: "oversized page", options: nodeListOptions{PageSize: 513}, want: "between 1 and 512"},
		{name: "reason without all", options: nodeListOptions{PageSize: 128, Reason: "hidden completion"}, want: "require --all"},
		{name: "all without reason", options: nodeListOptions{PageSize: 128, All: true, Budget: 160}, want: "requires --reason"},
		{name: "all without budget", options: nodeListOptions{PageSize: 128, All: true, Reason: "export"}, want: "positive --budget"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.options.validate(); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error=%v want substring %q", err, tc.want)
			}
		})
	}
}
