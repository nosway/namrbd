package adminclient

import (
	"context"
	"strings"
	"testing"

	adminv1 "github.com/nosway/namrbd/sbs/admin/v1"
	"google.golang.org/grpc"
)

type pagedMembershipAdminClient struct {
	adminv1.AdminServiceClient
	pages []*adminv1.ListNodesResponse
	calls int
}

func (c *pagedMembershipAdminClient) ListNodes(_ context.Context, req *adminv1.ListNodesRequest, _ ...grpc.CallOption) (*adminv1.ListNodesResponse, error) {
	if req.GetPageSize() != membershipProjectionPageSize {
		return nil, context.Canceled
	}
	page := c.pages[c.calls]
	c.calls++
	return page, nil
}

func TestListAllNodesPinsProjectionRevision(t *testing.T) {
	client := &pagedMembershipAdminClient{pages: []*adminv1.ListNodesResponse{
		{
			Nodes:                        []*adminv1.NodeSummary{{NodeId: "node-a"}},
			MembershipRevision:           7,
			MembershipProjectionRevision: 7,
			ProjectionHealth:             "healthy",
			NextPageToken:                "node-a",
		},
		{
			Nodes:                        []*adminv1.NodeSummary{{NodeId: "node-b"}},
			MembershipRevision:           7,
			MembershipProjectionRevision: 7,
			ProjectionHealth:             "healthy",
		},
	}}
	resp, err := ListAllNodes(context.Background(), client, &adminv1.ClusterRef{}, false)
	if err != nil {
		t.Fatalf("ListAllNodes: %v", err)
	}
	if client.calls != 2 || len(resp.GetNodes()) != 2 || resp.GetMembershipProjectionRevision() != 7 {
		t.Fatalf("calls=%d response=%+v", client.calls, resp)
	}
}

func TestListAllNodesRejectsMixedOrStaleProjection(t *testing.T) {
	for _, tc := range []struct {
		name  string
		pages []*adminv1.ListNodesResponse
		want  string
	}{
		{
			name: "mixed revision",
			pages: []*adminv1.ListNodesResponse{
				{MembershipProjectionRevision: 7, ProjectionHealth: "healthy", NextPageToken: "node-a"},
				{MembershipProjectionRevision: 8, ProjectionHealth: "healthy"},
			},
			want: "changed during page read",
		},
		{
			name: "stale projection",
			pages: []*adminv1.ListNodesResponse{
				{MembershipRevision: 8, MembershipProjectionRevision: 7, ProjectionHealth: "degraded", ProjectionStale: true},
			},
			want: "projection is degraded",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := ListAllNodes(context.Background(), &pagedMembershipAdminClient{pages: tc.pages}, &adminv1.ClusterRef{}, false)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error=%v want substring %q", err, tc.want)
			}
		})
	}
}
