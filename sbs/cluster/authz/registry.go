package authz

import (
	"fmt"
	"sort"
	"strings"
	"unicode"

	adminv1 "github.com/nosway/namrbd/sbs/admin/v1"
	internalv1 "github.com/nosway/namrbd/sbs/internalapi/v1"

	"google.golang.org/protobuf/reflect/protoreflect"
)

const (
	RoleObserver         = "observer"
	RoleStorageOperator  = "storage-operator"
	RoleBackupDROperator = "backup-dr-operator"
	RoleSecurityAdmin    = "security-admin"
	RolePlatformAdmin    = "platform-admin"
)

type MethodPermission struct {
	FullMethod string   `json:"full_method"`
	Method     string   `json:"method"`
	Resource   string   `json:"resource"`
	Action     string   `json:"action"`
	Permission string   `json:"permission"`
	Mutation   bool     `json:"mutation"`
	Roles      []string `json:"roles"`
	QACaseID   string   `json:"qa_case_id"`
	Covered    bool     `json:"covered"`
}

type resourceRule struct {
	resource string
	contains []string
	roles    []string
}

var resourceRules = []resourceRule{
	{resource: "rbac", contains: []string{"RBAC", "RoleBinding", "Permission"}, roles: []string{RolePlatformAdmin}},
	{resource: "security", contains: []string{"Security", "KeyAccessLease"}, roles: []string{RoleSecurityAdmin}},
	{resource: "governance", contains: []string{"WORM"}, roles: []string{RoleSecurityAdmin}},
	{resource: "backup", contains: []string{"Backup"}, roles: []string{RoleBackupDROperator}},
	{resource: "dr", contains: []string{"DR"}, roles: []string{RoleBackupDROperator}},
	{resource: "protocol", contains: []string{"ISCSI", "NVMe", "PersistentReservation"}, roles: []string{RoleStorageOperator, RolePlatformAdmin}},
	{resource: "storage", contains: []string{"Volume", "Snapshot", "Clone", "Allocation", "EC", "Dedupe", "DiffIndex", "Compression", "Tiering", "RestoreWarmup", "ReplicaTargets"}, roles: []string{RoleStorageOperator}},
	{resource: "maintenance", contains: []string{"Repair", "Rebalance", "Maintenance", "Drain", "Store", "Node", "Topology", "Placement"}, roles: []string{RoleStorageOperator, RolePlatformAdmin}},
	{resource: "platform", contains: []string{"Cluster", "Leader", "Operation", "Performance", "Service", "BackgroundBudget", "BudgetLease", "MembershipProjection"}, roles: []string{RolePlatformAdmin}},
}

func BuiltinRoles() []string {
	return []string{RoleObserver, RoleStorageOperator, RoleBackupDROperator, RoleSecurityAdmin, RolePlatformAdmin}
}

func AdminServiceInventory() ([]MethodPermission, error) {
	service := adminv1.File_sbs_admin_v1_admin_proto.Services().ByName("AdminService")
	if service == nil {
		return nil, fmt.Errorf("sbs.admin.v1.AdminService descriptor is unavailable")
	}
	return serviceInventory(service), nil
}

func AdmittedGRPCInventory() ([]MethodPermission, error) {
	services := []protoreflect.ServiceDescriptor{
		adminv1.File_sbs_admin_v1_admin_proto.Services().ByName("AdminService"),
		adminv1.File_sbs_admin_v1_operations_proto.Services().ByName("OperationsService"),
		internalv1.File_sbs_internalapi_v1_placement_resolver_proto.Services().ByName("PlacementResolverService"),
	}
	var out []MethodPermission
	for _, service := range services {
		if service == nil {
			return nil, fmt.Errorf("admitted RBAC gRPC service descriptor is unavailable")
		}
		out = append(out, serviceInventory(service)...)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].FullMethod < out[j].FullMethod })
	return out, nil
}

func serviceInventory(service protoreflect.ServiceDescriptor) []MethodPermission {
	out := make([]MethodPermission, 0, service.Methods().Len())
	for i := 0; i < service.Methods().Len(); i++ {
		method := service.Methods().Get(i)
		out = append(out, classifyMethod(service, method))
	}
	sort.Slice(out, func(i, j int) bool { return out[i].FullMethod < out[j].FullMethod })
	return out
}

func PermissionForFullMethod(fullMethod string) (MethodPermission, bool) {
	inventory, err := AdmittedGRPCInventory()
	if err != nil {
		return MethodPermission{}, false
	}
	for _, item := range inventory {
		if item.FullMethod == fullMethod {
			return item, item.Covered
		}
	}
	return MethodPermission{}, false
}

func RoleAllows(role string, permission MethodPermission) bool {
	role = strings.TrimSpace(role)
	if !permission.Covered {
		return false
	}
	if !permission.Mutation {
		return role == RoleObserver || contains(permission.Roles, role) || role == RolePlatformAdmin
	}
	return contains(permission.Roles, role)
}

func PermissionsForRole(role string) ([]string, error) {
	role = strings.TrimSpace(role)
	if !contains(BuiltinRoles(), role) {
		return nil, fmt.Errorf("unknown built-in role %q", role)
	}
	inventory, err := AdmittedGRPCInventory()
	if err != nil {
		return nil, err
	}
	seen := map[string]struct{}{}
	for _, item := range inventory {
		if RoleAllows(role, item) {
			seen[item.Permission] = struct{}{}
		}
	}
	out := make([]string, 0, len(seen))
	for permission := range seen {
		out = append(out, permission)
	}
	sort.Strings(out)
	return out, nil
}

func classifyMethod(service protoreflect.ServiceDescriptor, method protoreflect.MethodDescriptor) MethodPermission {
	name := string(method.Name())
	item := MethodPermission{
		FullMethod: "/" + string(service.FullName()) + "/" + name,
		Method:     name,
		Mutation:   method.Input().Fields().ByName("meta") != nil,
	}
	// The secure listener currently admits unary RPCs only. Keep a future
	// streaming method uncovered until it has an explicit stream interceptor;
	// otherwise the unary admission boundary could be bypassed silently.
	if method.IsStreamingClient() || method.IsStreamingServer() {
		return item
	}
	for _, rule := range resourceRules {
		for _, token := range rule.contains {
			if strings.Contains(name, token) {
				item.Resource = rule.resource
				item.Roles = append([]string(nil), rule.roles...)
				break
			}
		}
		if item.Resource != "" {
			break
		}
	}
	if item.Resource == "" {
		return item
	}
	item.Action = snakeCase(name)
	if !item.Mutation {
		item.Action = "read"
		item.Roles = appendUnique(item.Roles, RoleObserver)
	}
	item.Permission = item.Resource + ":" + item.Action
	item.QACaseID = "AC-RBAC-ADMIN-" + strings.ToUpper(strings.ReplaceAll(snakeCase(name), "_", "-"))
	item.Covered = true
	return item
}

func snakeCase(value string) string {
	var out []rune
	for i, r := range value {
		if unicode.IsUpper(r) && i > 0 {
			prev := rune(value[i-1])
			if unicode.IsLower(prev) || unicode.IsDigit(prev) {
				out = append(out, '_')
			}
		}
		out = append(out, unicode.ToLower(r))
	}
	return string(out)
}

func appendUnique(values []string, value string) []string {
	if contains(values, value) {
		return values
	}
	return append(values, value)
}

func contains(values []string, value string) bool {
	for _, candidate := range values {
		if candidate == value {
			return true
		}
	}
	return false
}
