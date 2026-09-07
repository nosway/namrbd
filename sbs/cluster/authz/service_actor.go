package authz

import (
	"sort"

	adminv1 "github.com/nosway/namrbd/sbs/admin/v1"
)

const (
	ServiceActorBackupScheduler       = "sbs-service.backup-scheduler"
	ServiceActorMaintenanceController = "sbs-service.maintenance-controller"
	ServiceActorNodeHealthReconciler  = "sbs-service.node-health-reconciler"
	ServiceActorTieringReconciler     = "sbs-service.tiering-reconciler"
	ServiceActorGatewayInternalRPC    = "gateway.internal-product-rpc"
)

const (
	ServiceActorClassAdminWorker        = "admin_worker"
	ServiceActorClassProductReconciler  = "product_owned_reconciler"
	ServiceActorClassProductInternalRPC = "product_internal_rpc"
)

type ServiceActorEntry struct {
	ActorID         string   `json:"actor_id"`
	Classification  string   `json:"classification"`
	FullMethod      string   `json:"full_method,omitempty"`
	Permission      string   `json:"permission,omitempty"`
	Roles           []string `json:"roles,omitempty"`
	RBACRequired    bool     `json:"rbac_required"`
	AuditRequired   bool     `json:"audit_required"`
	ExclusionReason string   `json:"exclusion_reason,omitempty"`
	QACaseID        string   `json:"qa_case_id"`
}

var serviceActorEntries = []ServiceActorEntry{
	{
		ActorID: ServiceActorBackupScheduler, Classification: ServiceActorClassAdminWorker,
		FullMethod: adminv1.AdminService_TickBackupScheduler_FullMethodName,
		Permission: "backup:tick_backup_scheduler", Roles: []string{RoleBackupDROperator},
		RBACRequired: true, AuditRequired: true, QACaseID: "AC-RBAC-SERVICE-BACKUP-SCHEDULER",
	},
	{
		ActorID: ServiceActorMaintenanceController, Classification: ServiceActorClassProductReconciler,
		RBACRequired: false, AuditRequired: false,
		ExclusionReason: "leader-owned product reconciliation has no external admin request and remains governed by maintenance policy and fencing",
		QACaseID:        "AC-RBAC-EXCLUDE-MAINTENANCE-RECONCILER",
	},
	{
		ActorID: ServiceActorNodeHealthReconciler, Classification: ServiceActorClassProductReconciler,
		RBACRequired: false, AuditRequired: false,
		ExclusionReason: "health observations are product state transitions rather than delegated operator mutations",
		QACaseID:        "AC-RBAC-EXCLUDE-NODE-HEALTH-RECONCILER",
	},
	{
		ActorID: ServiceActorTieringReconciler, Classification: ServiceActorClassProductReconciler,
		RBACRequired: false, AuditRequired: false,
		ExclusionReason: "reconciliation executes an already authorized durable tiering operation and cannot create a new operator intent",
		QACaseID:        "AC-RBAC-EXCLUDE-TIERING-RECONCILER",
	},
	{
		ActorID: ServiceActorGatewayInternalRPC, Classification: ServiceActorClassProductInternalRPC,
		RBACRequired: false, AuditRequired: false,
		ExclusionReason: "placement, write-session, allocation, EC, and VolumeService calls are product/data authority and not AdminService",
		QACaseID:        "AC-RBAC-EXCLUDE-GATEWAY-PRODUCT-RPC",
	},
}

func ServiceActorInventory() []ServiceActorEntry {
	out := make([]ServiceActorEntry, len(serviceActorEntries))
	for index, item := range serviceActorEntries {
		out[index] = item
		out[index].Roles = append([]string(nil), item.Roles...)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ActorID < out[j].ActorID })
	return out
}

func ServiceActorForFullMethod(fullMethod string) (ServiceActorEntry, bool) {
	for _, item := range serviceActorEntries {
		if item.RBACRequired && item.FullMethod == fullMethod {
			item.Roles = append([]string(nil), item.Roles...)
			return item, true
		}
	}
	return ServiceActorEntry{}, false
}
