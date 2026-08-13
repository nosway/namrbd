//go:build !enterprise

package performance

const (
	CapScopeLabOnly       = "lab_only"
	CapScopePerGateway    = "per_gateway"
	CapScopeClusterVolume = "cluster_volume"

	ThrottleModeWait   = "wait"
	ThrottleModeReject = "reject"

	BudgetClassForeground = "foreground"
)
