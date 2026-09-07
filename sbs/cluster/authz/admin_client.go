package authz

import "sort"

const (
	AdminClientClassOperator = "operator_cli"
	AdminClientClassService  = "service_actor"
	AdminClientClassHarness  = "release_harness"
)

type AdminClientConsumerEntry struct {
	ConsumerID     string `json:"consumer_id"`
	Classification string `json:"classification"`
	SourcePath     string `json:"source_path"`
	Transport      string `json:"transport"`
	SplitRequired  bool   `json:"split_required"`
	RBACRequired   bool   `json:"rbac_required"`
	AuditRequired  bool   `json:"audit_required"`
	QACaseID       string `json:"qa_case_id"`
}

var adminClientConsumers = []AdminClientConsumerEntry{
	{ConsumerID: "sbsctl", Classification: AdminClientClassOperator, SourcePath: "cmd/sbsctl", Transport: "internal/adminclient", RBACRequired: true, AuditRequired: true, QACaseID: "AC-RBAC-CLIENT-SBSCTL"},
	{ConsumerID: "namrbd-gateway", Classification: AdminClientClassService, SourcePath: "cmd/namrbd-gateway", Transport: "internal/adminclient", SplitRequired: true, RBACRequired: true, AuditRequired: true, QACaseID: "AC-RBAC-CLIENT-GATEWAY"},
	{ConsumerID: "namrbd-csi-driver", Classification: AdminClientClassService, SourcePath: "cmd/namrbd-csi-driver", Transport: "internal/adminclient", RBACRequired: true, AuditRequired: true, QACaseID: "AC-RBAC-CLIENT-CSI"},
	{ConsumerID: "namrbd-iscsi-gateway", Classification: AdminClientClassService, SourcePath: "cmd/namrbd-iscsi-gateway", Transport: "internal/adminclient", RBACRequired: true, AuditRequired: true, QACaseID: "AC-RBAC-CLIENT-ISCSI"},
	{ConsumerID: "namrbd-nvme-tcp-gateway", Classification: AdminClientClassService, SourcePath: "cmd/namrbd-nvme-tcp-gateway", Transport: "internal/adminclient", SplitRequired: true, RBACRequired: true, AuditRequired: true, QACaseID: "AC-RBAC-CLIENT-NVME"},
	{ConsumerID: "cluster-published-views", Classification: AdminClientClassService, SourcePath: "sbs/cluster", Transport: "internal/adminclient", SplitRequired: true, RBACRequired: true, AuditRequired: true, QACaseID: "AC-RBAC-CLIENT-PUBLISHED-VIEWS"},
	{ConsumerID: "phase-aa-live-scale", Classification: AdminClientClassHarness, SourcePath: "tools/phase-aa-live-scale", Transport: "internal/adminclient", RBACRequired: true, AuditRequired: true, QACaseID: "AC-RBAC-CLIENT-LIVE-HARNESS"},
}

func AdminClientConsumerInventory() []AdminClientConsumerEntry {
	out := append([]AdminClientConsumerEntry(nil), adminClientConsumers...)
	sort.Slice(out, func(i, j int) bool { return out[i].ConsumerID < out[j].ConsumerID })
	return out
}
