package authz

import (
	"net/http"
	"sort"
	"strings"
)

const (
	GatewayHTTPRouteReloadVolumeSize = "gateway.volume.reload-size"
	GatewayHTTPRouteApplyPathPlan    = "gateway.discovery.path-plan"
	GatewayHTTPRouteUpdateNodeHealth = "gateway.cluster.node-health"
)

type GatewayHTTPPermission struct {
	RouteID    string   `json:"route_id"`
	HTTPMethod string   `json:"http_method"`
	Resource   string   `json:"resource"`
	Action     string   `json:"action"`
	Permission string   `json:"permission"`
	Roles      []string `json:"roles"`
	QACaseID   string   `json:"qa_case_id"`
	Mutation   bool     `json:"mutation"`
}

var gatewayHTTPPermissions = []GatewayHTTPPermission{
	{
		RouteID: GatewayHTTPRouteUpdateNodeHealth, HTTPMethod: http.MethodPost,
		Resource: "maintenance", Action: "update_node_health", Permission: "maintenance:update_node_health",
		Roles: []string{RoleStorageOperator, RolePlatformAdmin}, QACaseID: "AC-RBAC-HTTP-UPDATE-NODE-HEALTH", Mutation: true,
	},
	{
		RouteID: GatewayHTTPRouteApplyPathPlan, HTTPMethod: http.MethodPost,
		Resource: "platform", Action: "apply_volume_path_plan", Permission: "platform:apply_volume_path_plan",
		Roles: []string{RolePlatformAdmin}, QACaseID: "AC-RBAC-HTTP-APPLY-VOLUME-PATH-PLAN", Mutation: true,
	},
	{
		RouteID: GatewayHTTPRouteReloadVolumeSize, HTTPMethod: http.MethodPost,
		Resource: "storage", Action: "reload_volume_size", Permission: "storage:reload_volume_size",
		Roles: []string{RoleStorageOperator}, QACaseID: "AC-RBAC-HTTP-RELOAD-VOLUME-SIZE", Mutation: true,
	},
}

func GatewayHTTPInventory() []GatewayHTTPPermission {
	out := make([]GatewayHTTPPermission, len(gatewayHTTPPermissions))
	for index, item := range gatewayHTTPPermissions {
		out[index] = item
		out[index].Roles = append([]string(nil), item.Roles...)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].RouteID < out[j].RouteID })
	return out
}

func GatewayHTTPPermissionForRoute(routeID, method string) (GatewayHTTPPermission, bool) {
	routeID = strings.TrimSpace(routeID)
	method = strings.ToUpper(strings.TrimSpace(method))
	for _, item := range gatewayHTTPPermissions {
		if item.RouteID == routeID && item.HTTPMethod == method {
			item.Roles = append([]string(nil), item.Roles...)
			return item, true
		}
	}
	return GatewayHTTPPermission{}, false
}

func GatewayHTTPPermissionForRequest(method, path string) (GatewayHTTPPermission, bool) {
	if strings.ToUpper(strings.TrimSpace(method)) != http.MethodPost {
		return GatewayHTTPPermission{}, false
	}
	parts := strings.Split(strings.Trim(strings.TrimSpace(path), "/"), "/")
	var routeID string
	switch {
	case len(parts) == 5 && parts[0] == "api" && parts[1] == "v1" && parts[2] == "volumes" && parts[3] != "" && parts[4] == "reload-size":
		routeID = GatewayHTTPRouteReloadVolumeSize
	case len(parts) == 7 && parts[0] == "api" && parts[1] == "v1" && parts[2] == "debug" && parts[3] == "discovery" && parts[4] == "volumes" && parts[5] != "" && parts[6] == "path-plan":
		routeID = GatewayHTTPRouteApplyPathPlan
	case len(parts) == 6 && parts[0] == "api" && parts[1] == "v1" && parts[2] == "debug" && parts[3] == "sbs-cluster" && parts[4] == "nodes" && parts[5] != "":
		routeID = GatewayHTTPRouteUpdateNodeHealth
	default:
		return GatewayHTTPPermission{}, false
	}
	return GatewayHTTPPermissionForRoute(routeID, http.MethodPost)
}

func RoleAllowsGatewayHTTP(role string, permission GatewayHTTPPermission) bool {
	return permission.Mutation && contains(permission.Roles, strings.TrimSpace(role))
}
