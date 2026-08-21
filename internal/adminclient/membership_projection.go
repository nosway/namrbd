package adminclient

import (
	"context"
	"fmt"

	adminv1 "github.com/nosway/namrbd/sbs/admin/v1"
)

const membershipProjectionPageSize = 512

// ListAllNodes follows bounded projection pages while pinning every page to
// the same projection revision. A caller never receives a mixed or stale SBS
// membership view.
func ListAllNodes(ctx context.Context, client adminv1.AdminServiceClient, cluster *adminv1.ClusterRef, includeTombstones bool) (*adminv1.ListNodesResponse, error) {
	if client == nil {
		return nil, fmt.Errorf("admin client is required")
	}
	out := &adminv1.ListNodesResponse{Cluster: cluster}
	token := ""
	var pinnedRevision uint64
	pinned := false
	for {
		page, err := client.ListNodes(ctx, &adminv1.ListNodesRequest{
			Cluster:           cluster,
			PageSize:          membershipProjectionPageSize,
			PageToken:         token,
			IncludeTombstones: includeTombstones,
		})
		if err != nil {
			return nil, err
		}
		if page.GetProjectionStale() || page.GetProjectionHealth() == "degraded" || page.GetProjectionHealth() == "blocked" {
			return nil, fmt.Errorf("SBS membership projection is %s: authority revision=%d projection revision=%d lag=%dms", page.GetProjectionHealth(), page.GetMembershipRevision(), page.GetMembershipProjectionRevision(), page.GetProjectionLagMs())
		}
		if !pinned {
			pinnedRevision = page.GetMembershipProjectionRevision()
			pinned = true
		} else if page.GetMembershipProjectionRevision() != pinnedRevision {
			return nil, fmt.Errorf("SBS membership projection changed during page read: first revision=%d current revision=%d", pinnedRevision, page.GetMembershipProjectionRevision())
		}
		out.Nodes = append(out.Nodes, page.GetNodes()...)
		out.MembershipRevision = page.GetMembershipRevision()
		out.MembershipProjectionRevision = page.GetMembershipProjectionRevision()
		out.ProjectionLagMs = page.GetProjectionLagMs()
		out.ProjectionHealth = page.GetProjectionHealth()
		out.ProjectionStale = page.GetProjectionStale()
		out.ProjectionRebuildCount = page.GetProjectionRebuildCount()
		out.ProjectionResyncCount = page.GetProjectionResyncCount()
		token = page.GetNextPageToken()
		if token == "" {
			return out, nil
		}
	}
}
