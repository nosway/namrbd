package main

import (
	"context"
	"fmt"
	"strings"

	adminv1 "github.com/nosway/namrbd/sbs/admin/v1"
)

const (
	nodeListDefaultPageSize = 128
	nodeListMaximumPageSize = 512
)

type nodeListOptions struct {
	PageSize          uint32
	PageToken         string
	IncludeTombstones bool
	All               bool
	Reason            string
	Budget            uint64
}

type nodeListResult struct {
	Response            *adminv1.ListNodesResponse
	PagesRead           uint32
	AutomaticCompletion bool
	Reason              string
	Budget              uint64
}

func (o nodeListOptions) validate() error {
	if o.PageSize == 0 || o.PageSize > nodeListMaximumPageSize {
		return fmt.Errorf("--page-size must be between 1 and %d", nodeListMaximumPageSize)
	}
	if !o.All {
		if strings.TrimSpace(o.Reason) != "" || o.Budget != 0 {
			return fmt.Errorf("--reason and --budget require --all")
		}
		return nil
	}
	if strings.TrimSpace(o.Reason) == "" {
		return fmt.Errorf("--all requires --reason")
	}
	if o.Budget == 0 {
		return fmt.Errorf("--all requires a positive --budget")
	}
	return nil
}

func listNodePages(ctx context.Context, client adminv1.AdminServiceClient, cluster *adminv1.ClusterRef, options nodeListOptions) (nodeListResult, error) {
	if client == nil {
		return nodeListResult{}, fmt.Errorf("admin client is required")
	}
	if err := options.validate(); err != nil {
		return nodeListResult{}, err
	}
	result := nodeListResult{Response: &adminv1.ListNodesResponse{Cluster: cluster}, Reason: strings.TrimSpace(options.Reason), Budget: options.Budget}
	token := strings.TrimSpace(options.PageToken)
	var pinnedRevision uint64
	for {
		pageSize := options.PageSize
		if options.All {
			remaining := options.Budget - uint64(len(result.Response.GetNodes()))
			if remaining < uint64(pageSize) {
				pageSize = uint32(remaining)
			}
		}
		page, err := client.ListNodes(ctx, &adminv1.ListNodesRequest{
			Cluster:           cluster,
			PageSize:          pageSize,
			PageToken:         token,
			IncludeTombstones: options.IncludeTombstones,
		})
		if err != nil {
			return nodeListResult{}, err
		}
		if len(page.GetNodes()) > int(pageSize) {
			return nodeListResult{}, fmt.Errorf("node page returned %d records above requested page_size %d", len(page.GetNodes()), pageSize)
		}
		result.PagesRead++
		if page.GetProjectionStale() || page.GetProjectionHealth() == "degraded" || page.GetProjectionHealth() == "blocked" {
			return nodeListResult{}, fmt.Errorf("SBS membership projection is %s: authority revision=%d projection revision=%d lag=%dms", page.GetProjectionHealth(), page.GetMembershipRevision(), page.GetMembershipProjectionRevision(), page.GetProjectionLagMs())
		}
		if pinnedRevision == 0 {
			pinnedRevision = page.GetMembershipProjectionRevision()
		} else if page.GetMembershipProjectionRevision() != pinnedRevision {
			return nodeListResult{}, fmt.Errorf("SBS membership projection changed during page read: first revision=%d current revision=%d", pinnedRevision, page.GetMembershipProjectionRevision())
		}
		result.Response.Nodes = append(result.Response.Nodes, page.GetNodes()...)
		result.Response.Cluster = page.GetCluster()
		result.Response.MembershipRevision = page.GetMembershipRevision()
		result.Response.MembershipProjectionRevision = page.GetMembershipProjectionRevision()
		result.Response.ProjectionLagMs = page.GetProjectionLagMs()
		result.Response.ProjectionHealth = page.GetProjectionHealth()
		result.Response.ProjectionStale = page.GetProjectionStale()
		result.Response.NextPageToken = page.GetNextPageToken()
		result.Response.ProjectionRebuildCount = page.GetProjectionRebuildCount()
		result.Response.ProjectionResyncCount = page.GetProjectionResyncCount()
		if !options.All || page.GetNextPageToken() == "" || uint64(len(result.Response.GetNodes())) >= options.Budget {
			result.AutomaticCompletion = options.All && result.PagesRead > 1
			return result, nil
		}
		token = page.GetNextPageToken()
	}
}

func nodeListJSON(result nodeListResult, options nodeListOptions) map[string]any {
	resp := result.Response
	return map[string]any{
		"cluster":                        resp.GetCluster(),
		"nodes":                          resp.GetNodes(),
		"membership_revision":            resp.GetMembershipRevision(),
		"membership_projection_revision": resp.GetMembershipProjectionRevision(),
		"projection_lag_ms":              resp.GetProjectionLagMs(),
		"projection_health":              resp.GetProjectionHealth(),
		"projection_stale":               resp.GetProjectionStale(),
		"next_page_token":                resp.GetNextPageToken(),
		"projection_rebuild_count":       resp.GetProjectionRebuildCount(),
		"projection_resync_count":        resp.GetProjectionResyncCount(),
		"page_size":                      options.PageSize,
		"pages_read":                     result.PagesRead,
		"automatic_page_completion":      result.AutomaticCompletion,
		"expensive_all_requested":        options.All,
		"expensive_reason":               result.Reason,
		"expensive_node_budget":          result.Budget,
		"result_truncated_by_budget":     options.All && resp.GetNextPageToken() != "",
	}
}
