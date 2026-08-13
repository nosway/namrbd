package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/nosway/namrbd/internal/adminclient"
	"github.com/nosway/namrbd/iscsi"
	adminv1 "github.com/nosway/namrbd/sbs/admin/v1"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const iscsiRegistryReadyStatus = "cluster_iscsi_registry_ready"

type sbsctlISCSIConfig struct {
	ClusterID     string
	SBSClusterID  string
	ContextFile   string
	ContextName   string
	AdminEndpoint string
	DataEndpoint  string
	Timeout       time.Duration
}

type sbsctlISCSIAdminClient = adminv1.AdminServiceClient

var newSBSCTLISCSIAdminClient = func(ctx context.Context, endpoint string) (sbsctlISCSIAdminClient, func() error, error) {
	client, err := adminclient.Dial(ctx, endpoint)
	if err != nil {
		return nil, nil, err
	}
	return client.Admin, client.Close, nil
}

func runISCSI(args []string) {
	if len(args) < 1 {
		iscsiUsage()
		os.Exit(2)
	}
	switch args[0] {
	case "status":
		runISCSIStatus(args[1:])
	case "portal":
		runISCSIPortal(args[1:])
	case "target":
		runISCSITarget(args[1:])
	case "lun":
		runISCSILUN(args[1:])
	case "initiator":
		runISCSIInitiator(args[1:])
	case "session":
		runISCSISession(args[1:])
	case "failover":
		runISCSIFailover(args[1:])
	default:
		iscsiUsage()
		os.Exit(2)
	}
}

func runISCSIStatus(args []string) {
	if len(args) < 1 {
		iscsiStatusUsage()
		os.Exit(2)
	}
	switch args[0] {
	case "gateway":
		runISCSIStatusGateway(args[1:])
	case "target":
		runISCSIStatusTarget(args[1:])
	default:
		iscsiStatusUsage()
		os.Exit(2)
	}
}

func runISCSILUN(args []string) {
	if len(args) < 1 {
		iscsiLUNUsage()
		os.Exit(2)
	}
	switch args[0] {
	case "list":
		runISCSILUNList(args[1:])
	case "get":
		runISCSILUNGet(args[1:])
	case "export":
		runISCSILUNExport(args[1:])
	case "unexport":
		runISCSILUNUnexport(args[1:])
	case "set-mode":
		runISCSILUNSetMode(args[1:])
	default:
		iscsiLUNUsage()
		os.Exit(2)
	}
}

func runISCSIPortal(args []string) {
	if len(args) < 1 {
		iscsiPortalUsage()
		os.Exit(2)
	}
	switch args[0] {
	case "list":
		runISCSIPortalList(args[1:])
	case "get":
		runISCSIPortalGet(args[1:])
	case "create":
		runISCSIPortalCreate(args[1:])
	case "delete":
		runISCSIPortalDelete(args[1:])
	case "enable":
		runISCSIPortalSetEnabled(args[1:], true)
	case "disable":
		runISCSIPortalSetEnabled(args[1:], false)
	default:
		iscsiPortalUsage()
		os.Exit(2)
	}
}

func runISCSITarget(args []string) {
	if len(args) < 1 {
		iscsiTargetUsage()
		os.Exit(2)
	}
	switch args[0] {
	case "list":
		runISCSITargetList(args[1:])
	case "get":
		runISCSITargetGet(args[1:])
	case "create":
		runISCSITargetCreate(args[1:])
	case "delete":
		runISCSITargetDelete(args[1:])
	case "enable":
		runISCSITargetSetEnabled(args[1:], true)
	case "disable":
		runISCSITargetSetEnabled(args[1:], false)
	default:
		iscsiTargetUsage()
		os.Exit(2)
	}
}

func runISCSIInitiator(args []string) {
	if len(args) < 1 {
		iscsiInitiatorUsage()
		os.Exit(2)
	}
	switch args[0] {
	case "list":
		runISCSIInitiatorList(args[1:])
	case "get":
		runISCSIInitiatorGet(args[1:])
	case "allow":
		runISCSIInitiatorAllow(args[1:])
	case "deny":
		runISCSIInitiatorDeny(args[1:])
	case "set-auth":
		runISCSIInitiatorSetAuth(args[1:])
	default:
		iscsiInitiatorUsage()
		os.Exit(2)
	}
}

func runISCSISession(args []string) {
	if len(args) < 1 {
		iscsiSessionUsage()
		os.Exit(2)
	}
	switch args[0] {
	case "list":
		runISCSISessionList(args[1:])
	case "get":
		runISCSISessionGet(args[1:])
	case "disconnect":
		runISCSISessionDisconnect(args[1:])
	default:
		iscsiSessionUsage()
		os.Exit(2)
	}
}

func runISCSIFailover(args []string) {
	if len(args) < 1 {
		iscsiFailoverUsage()
		os.Exit(2)
	}
	switch args[0] {
	case "status":
		runISCSIFailoverStatus(args[1:])
	case "promote":
		runISCSIFailoverPromote(args[1:])
	case "demote":
		runISCSIFailoverDemote(args[1:])
	case "standby":
		runISCSIFailoverStandby(args[1:])
	case "revoke-stale":
		runISCSIFailoverRevokeStale(args[1:])
	default:
		iscsiFailoverUsage()
		os.Exit(2)
	}
}

func runISCSIStatusGateway(args []string) {
	defaults := mustResolveCLIDefaults(args)
	fs := flag.NewFlagSet("iscsi status gateway", flag.ExitOnError)
	flags := addISCSIFlags(fs, defaults)
	parseISCSIFlags(fs, args, flags)

	out, code := withISCSIAdmin(flags.config(), "status", "gateway", func(ctx context.Context, client sbsctlISCSIAdminClient, cfg sbsctlISCSIConfig) map[string]any {
		statusResp, err := client.GetClusterStatus(ctx, &adminv1.GetClusterStatusRequest{Cluster: cfg.clusterRef()})
		if err != nil {
			return sbsctlISCSIAdminRPCErrorResult(cfg, "status", "gateway", err)
		}
		registryResp, err := client.GetISCSIRegistry(ctx, &adminv1.GetISCSIRegistryRequest{Cluster: cfg.clusterRef()})
		if err != nil {
			return sbsctlISCSIAdminRPCErrorResult(cfg, "status", "gateway", err)
		}
		counters := registryResp.GetObservabilityCounters()
		fields := map[string]any{
			"backend_mode":                     "sbs_cluster",
			"gateway_runtime":                  "cluster_admin_reachable",
			"target_stack":                     iscsi.TargetStack,
			"target_stack_version":             iscsi.TargetStackVersion,
			"leader_node_id":                   statusResp.GetLeaderNodeId(),
			"quorum_health":                    sbsctlISCSIEnumLabel(statusResp.GetQuorumHealth().String(), "QUORUM_HEALTH_"),
			"active_nodes":                     statusResp.GetActiveNodes(),
			"draining_nodes":                   statusResp.GetDrainingNodes(),
			"degraded_extents":                 statusResp.GetDegradedExtents(),
			"repair_backlog":                   statusResp.GetRepairBacklog(),
			"rebalance_backlog":                statusResp.GetRebalanceBacklog(),
			"drain_backlog":                    statusResp.GetDrainBacklog(),
			"target_count":                     len(registryResp.GetTargets()),
			"lun_count":                        len(registryResp.GetLuns()),
			"session_count":                    counters.GetSessionCount(),
			"connected_sessions":               counters.GetConnectedSessions(),
			"portal_count":                     len(registryResp.GetPortals()),
			"initiator_acl_count":              len(registryResp.GetInitiatorAcls()),
			"failover_count":                   len(registryResp.GetFailovers()),
			"iscsi_registry_available":         true,
			"registry_status":                  iscsiRegistryReadyStatus,
			"metrics_runtime_claim":            "sbs_admin_iscsi_registry",
			"troubleshooting_fields_available": true,
			"log_json_channel":                 "stdout_json_only",
			"remote_lab_used":                  false,
			"iscsi_gateway_restarted":          false,
			"sbs_service_restarted":            false,
			"sbs_data_restarted":               false,
			"kernel_module_reloaded":           false,
			"observability_counters": map[string]any{
				"active_nodes":       statusResp.GetActiveNodes(),
				"draining_nodes":     statusResp.GetDrainingNodes(),
				"degraded_extents":   statusResp.GetDegradedExtents(),
				"repair_backlog":     statusResp.GetRepairBacklog(),
				"rebalance_backlog":  statusResp.GetRebalanceBacklog(),
				"drain_backlog":      statusResp.GetDrainBacklog(),
				"session_count":      counters.GetSessionCount(),
				"connected_sessions": counters.GetConnectedSessions(),
				"backend_errors":     counters.GetBackendErrors(),
				"fencing_errors":     counters.GetFencingErrors(),
				"flush_count":        counters.GetFlushCount(),
				"unmap_bytes":        counters.GetUnmapBytes(),
				"registry_runtime":   "sbs_admin_iscsi_registry",
			},
			"ok_count": 1,
		}
		applySBSCTLISCSIClusterRefFields(fields, statusResp.GetCluster())
		applySBSCTLISCSIClusterRefFields(fields, registryResp.GetCluster())
		applySBSCTLISCSIRegistryFields(fields, registryResp.GetRegistryRevision(), registryResp.GetConfigGeneration())
		return sbsctlISCSIResult(cfg, "status", "gateway", fields)
	})
	writeJSON(out)
	if code != 0 {
		os.Exit(code)
	}
}

func runISCSIStatusTarget(args []string) {
	defaults := mustResolveCLIDefaults(args)
	fs := flag.NewFlagSet("iscsi status target", flag.ExitOnError)
	targetIQN := fs.String("target-iqn", "", "target IQN")
	flags := addISCSIFlags(fs, defaults)
	parseISCSIFlags(fs, args, flags)
	if *targetIQN == "" {
		fatalf("iscsi status target requires --target-iqn")
	}

	out, code := withISCSIAdmin(flags.config(), "status", "target", func(ctx context.Context, client sbsctlISCSIAdminClient, cfg sbsctlISCSIConfig) map[string]any {
		statusResp, err := client.GetClusterStatus(ctx, &adminv1.GetClusterStatusRequest{Cluster: cfg.clusterRef()})
		if err != nil {
			return sbsctlISCSIAdminRPCErrorResult(cfg, "status", "target", err)
		}
		registryResp, err := client.GetISCSIRegistry(ctx, &adminv1.GetISCSIRegistryRequest{Cluster: cfg.clusterRef()})
		if err != nil {
			return sbsctlISCSIAdminRPCErrorResult(cfg, "status", "target", err)
		}
		target := sbsctlISCSIFindTarget(registryResp.GetTargets(), *targetIQN)
		luns := sbsctlISCSILUNRows(sbsctlISCSIFilterLUNs(registryResp.GetLuns(), *targetIQN))
		sessions := sbsctlISCSISessionRows(sbsctlISCSIFilterSessions(registryResp.GetSessions(), *targetIQN, ""))
		fields := map[string]any{
			"target_iqn":               *targetIQN,
			"backend_mode":             "sbs_cluster",
			"target_runtime":           "sbs_admin_iscsi_registry",
			"target_found":             target != nil,
			"target":                   sbsctlISCSITargetRow(target),
			"luns":                     luns,
			"sessions":                 sessions,
			"lun_count":                len(luns),
			"session_count":            len(sessions),
			"target_stack":             iscsi.TargetStack,
			"target_stack_version":     iscsi.TargetStackVersion,
			"iscsi_registry_available": true,
			"registry_status":          iscsiRegistryReadyStatus,
			"leader_node_id":           statusResp.GetLeaderNodeId(),
			"quorum_health":            sbsctlISCSIEnumLabel(statusResp.GetQuorumHealth().String(), "QUORUM_HEALTH_"),
			"log_json_channel":         "stdout_json_only",
			"ok_count":                 1,
		}
		if target == nil {
			fields["target_found_claim"] = "not_found_in_cluster_iscsi_registry"
		}
		applySBSCTLISCSIClusterRefFields(fields, statusResp.GetCluster())
		applySBSCTLISCSIClusterRefFields(fields, registryResp.GetCluster())
		applySBSCTLISCSIRegistryFields(fields, registryResp.GetRegistryRevision(), registryResp.GetConfigGeneration())
		return sbsctlISCSIResult(cfg, "status", "target", fields)
	})
	writeJSON(out)
	if code != 0 {
		os.Exit(code)
	}
}

func runISCSIPortalList(args []string) {
	defaults := mustResolveCLIDefaults(args)
	fs := flag.NewFlagSet("iscsi portal list", flag.ExitOnError)
	flags := addISCSIFlags(fs, defaults)
	parseISCSIFlags(fs, args, flags)

	out, code := withISCSIAdmin(flags.config(), "portal", "list", func(ctx context.Context, client sbsctlISCSIAdminClient, cfg sbsctlISCSIConfig) map[string]any {
		resp, err := client.ListISCSIPortals(ctx, &adminv1.ListISCSIPortalsRequest{Cluster: cfg.clusterRef()})
		if err != nil {
			return sbsctlISCSIAdminRPCErrorResult(cfg, "portal", "list", err)
		}
		portals := sbsctlISCSIPortalRows(resp.GetPortals())
		fields := map[string]any{
			"portals":                  portals,
			"count":                    len(portals),
			"iscsi_registry_available": true,
			"registry_status":          iscsiRegistryReadyStatus,
		}
		applySBSCTLISCSIClusterRefFields(fields, resp.GetCluster())
		applySBSCTLISCSIRegistryFields(fields, resp.GetRegistryRevision(), resp.GetConfigGeneration())
		return sbsctlISCSIResult(cfg, "portal", "list", fields)
	})
	writeJSON(out)
	if code != 0 {
		os.Exit(code)
	}
}

func runISCSIPortalGet(args []string) {
	defaults := mustResolveCLIDefaults(args)
	fs := flag.NewFlagSet("iscsi portal get", flag.ExitOnError)
	portalID := fs.String("portal-id", "", "portal id")
	flags := addISCSIFlags(fs, defaults)
	parseISCSIFlags(fs, args, flags)
	if strings.TrimSpace(*portalID) == "" {
		fatalf("iscsi portal get requires --portal-id")
	}

	out, code := withISCSIAdmin(flags.config(), "portal", "get", func(ctx context.Context, client sbsctlISCSIAdminClient, cfg sbsctlISCSIConfig) map[string]any {
		resp, err := client.GetISCSIPortal(ctx, &adminv1.GetISCSIPortalRequest{Cluster: cfg.clusterRef(), PortalId: strings.TrimSpace(*portalID)})
		if err != nil {
			return sbsctlISCSIAdminRPCErrorResult(cfg, "portal", "get", err)
		}
		fields := map[string]any{
			"portal_id":                strings.TrimSpace(*portalID),
			"portal":                   sbsctlISCSIPortalRow(resp.GetPortal()),
			"iscsi_registry_available": true,
			"registry_status":          iscsiRegistryReadyStatus,
		}
		applySBSCTLISCSIClusterRefFields(fields, resp.GetCluster())
		applySBSCTLISCSIRegistryFields(fields, resp.GetRegistryRevision(), resp.GetConfigGeneration())
		return sbsctlISCSIResult(cfg, "portal", "get", fields)
	})
	writeJSON(out)
	if code != 0 {
		os.Exit(code)
	}
}

func runISCSIPortalCreate(args []string) {
	defaults := mustResolveCLIDefaults(args)
	fs := flag.NewFlagSet("iscsi portal create", flag.ExitOnError)
	portalID := fs.String("portal-id", "", "portal id")
	address := fs.String("address", "", "portal listen address, host:port")
	gatewayID := fs.String("gateway-id", "", "owning iSCSI gateway id")
	enabled := fs.Bool("enabled", true, "create portal enabled")
	flags, mutation := addISCSIMutationFlags(fs, defaults)
	parseISCSIFlags(fs, args, flags)
	requireISCSIMutationKey(mutation)
	if strings.TrimSpace(*portalID) == "" || strings.TrimSpace(*address) == "" {
		fatalf("iscsi portal create requires --portal-id and --address")
	}

	out, code := withISCSIAdmin(flags.config(), "portal", "create", func(ctx context.Context, client sbsctlISCSIAdminClient, cfg sbsctlISCSIConfig) map[string]any {
		resp, err := client.CreateISCSIPortal(ctx, &adminv1.CreateISCSIPortalRequest{
			Cluster:                  cfg.clusterRef(),
			Meta:                     mutation.requestMeta(),
			IdempotencyKey:           mutation.idempotencyKey(),
			ExpectedRegistryRevision: mutation.expectedRegistryRevision(),
			PortalId:                 strings.TrimSpace(*portalID),
			Address:                  strings.TrimSpace(*address),
			GatewayId:                strings.TrimSpace(*gatewayID),
			Enabled:                  *enabled,
		})
		if err != nil {
			return sbsctlISCSIAdminRPCErrorResult(cfg, "portal", "create", err)
		}
		fields := map[string]any{
			"portal_id":                resp.GetPortal().GetPortalId(),
			"portal":                   sbsctlISCSIPortalRow(resp.GetPortal()),
			"operation_handle":         sbsctlISCSIOperationHandleRow(resp.GetOperation()),
			"iscsi_registry_available": true,
			"registry_status":          iscsiRegistryReadyStatus,
		}
		applySBSCTLISCSIClusterRefFields(fields, resp.GetCluster())
		applySBSCTLISCSIRegistryFields(fields, resp.GetRegistryRevision(), resp.GetConfigGeneration())
		return sbsctlISCSIResult(cfg, "portal", "create", fields)
	})
	writeJSON(out)
	if code != 0 {
		os.Exit(code)
	}
}

func runISCSIPortalDelete(args []string) {
	defaults := mustResolveCLIDefaults(args)
	fs := flag.NewFlagSet("iscsi portal delete", flag.ExitOnError)
	portalID := fs.String("portal-id", "", "portal id")
	force := fs.Bool("force", false, "remove target portal references when possible")
	yes := fs.Bool("yes", false, "confirm destructive registry mutation")
	flags, mutation := addISCSIMutationFlags(fs, defaults)
	parseISCSIFlags(fs, args, flags)
	requireISCSIMutationKey(mutation)
	requireISCSIYes(*yes, "iscsi portal delete")
	if strings.TrimSpace(*portalID) == "" {
		fatalf("iscsi portal delete requires --portal-id")
	}

	out, code := withISCSIAdmin(flags.config(), "portal", "delete", func(ctx context.Context, client sbsctlISCSIAdminClient, cfg sbsctlISCSIConfig) map[string]any {
		resp, err := client.DeleteISCSIPortal(ctx, &adminv1.DeleteISCSIPortalRequest{
			Cluster:                  cfg.clusterRef(),
			Meta:                     mutation.requestMeta(),
			IdempotencyKey:           mutation.idempotencyKey(),
			ExpectedRegistryRevision: mutation.expectedRegistryRevision(),
			PortalId:                 strings.TrimSpace(*portalID),
			Force:                    *force,
		})
		if err != nil {
			return sbsctlISCSIAdminRPCErrorResult(cfg, "portal", "delete", err)
		}
		fields := map[string]any{
			"portal_id":                resp.GetPortalId(),
			"operation_handle":         sbsctlISCSIOperationHandleRow(resp.GetOperation()),
			"iscsi_registry_available": true,
			"registry_status":          iscsiRegistryReadyStatus,
		}
		applySBSCTLISCSIClusterRefFields(fields, resp.GetCluster())
		applySBSCTLISCSIRegistryFields(fields, resp.GetRegistryRevision(), resp.GetConfigGeneration())
		return sbsctlISCSIResult(cfg, "portal", "delete", fields)
	})
	writeJSON(out)
	if code != 0 {
		os.Exit(code)
	}
}

func runISCSIPortalSetEnabled(args []string, enabled bool) {
	defaults := mustResolveCLIDefaults(args)
	operation := "disable"
	if enabled {
		operation = "enable"
	}
	fs := flag.NewFlagSet("iscsi portal "+operation, flag.ExitOnError)
	portalID := fs.String("portal-id", "", "portal id")
	flags, mutation := addISCSIMutationFlags(fs, defaults)
	parseISCSIFlags(fs, args, flags)
	requireISCSIMutationKey(mutation)
	if strings.TrimSpace(*portalID) == "" {
		fatalf("iscsi portal %s requires --portal-id", operation)
	}

	out, code := withISCSIAdmin(flags.config(), "portal", operation, func(ctx context.Context, client sbsctlISCSIAdminClient, cfg sbsctlISCSIConfig) map[string]any {
		resp, err := client.SetISCSIPortalEnabled(ctx, &adminv1.SetISCSIPortalEnabledRequest{
			Cluster:                  cfg.clusterRef(),
			Meta:                     mutation.requestMeta(),
			IdempotencyKey:           mutation.idempotencyKey(),
			ExpectedRegistryRevision: mutation.expectedRegistryRevision(),
			PortalId:                 strings.TrimSpace(*portalID),
			Enabled:                  enabled,
		})
		if err != nil {
			return sbsctlISCSIAdminRPCErrorResult(cfg, "portal", operation, err)
		}
		fields := map[string]any{
			"portal_id":                resp.GetPortal().GetPortalId(),
			"portal":                   sbsctlISCSIPortalRow(resp.GetPortal()),
			"operation_handle":         sbsctlISCSIOperationHandleRow(resp.GetOperation()),
			"iscsi_registry_available": true,
			"registry_status":          iscsiRegistryReadyStatus,
		}
		applySBSCTLISCSIClusterRefFields(fields, resp.GetCluster())
		applySBSCTLISCSIRegistryFields(fields, resp.GetRegistryRevision(), resp.GetConfigGeneration())
		return sbsctlISCSIResult(cfg, "portal", operation, fields)
	})
	writeJSON(out)
	if code != 0 {
		os.Exit(code)
	}
}

func runISCSITargetList(args []string) {
	defaults := mustResolveCLIDefaults(args)
	fs := flag.NewFlagSet("iscsi target list", flag.ExitOnError)
	flags := addISCSIFlags(fs, defaults)
	parseISCSIFlags(fs, args, flags)

	out, code := withISCSIAdmin(flags.config(), "target", "list", func(ctx context.Context, client sbsctlISCSIAdminClient, cfg sbsctlISCSIConfig) map[string]any {
		resp, err := client.ListISCSITargets(ctx, &adminv1.ListISCSITargetsRequest{Cluster: cfg.clusterRef()})
		if err != nil {
			return sbsctlISCSIAdminRPCErrorResult(cfg, "target", "list", err)
		}
		targets := sbsctlISCSITargetRows(resp.GetTargets())
		fields := map[string]any{
			"targets":                  targets,
			"count":                    len(targets),
			"iscsi_registry_available": true,
			"registry_status":          iscsiRegistryReadyStatus,
		}
		applySBSCTLISCSIClusterRefFields(fields, resp.GetCluster())
		applySBSCTLISCSIRegistryFields(fields, resp.GetRegistryRevision(), resp.GetConfigGeneration())
		return sbsctlISCSIResult(cfg, "target", "list", fields)
	})
	writeJSON(out)
	if code != 0 {
		os.Exit(code)
	}
}

func runISCSITargetGet(args []string) {
	defaults := mustResolveCLIDefaults(args)
	fs := flag.NewFlagSet("iscsi target get", flag.ExitOnError)
	targetIQN := fs.String("target-iqn", "", "target IQN")
	flags := addISCSIFlags(fs, defaults)
	parseISCSIFlags(fs, args, flags)
	if strings.TrimSpace(*targetIQN) == "" {
		fatalf("iscsi target get requires --target-iqn")
	}

	out, code := withISCSIAdmin(flags.config(), "target", "get", func(ctx context.Context, client sbsctlISCSIAdminClient, cfg sbsctlISCSIConfig) map[string]any {
		resp, err := client.GetISCSITarget(ctx, &adminv1.GetISCSITargetRequest{Cluster: cfg.clusterRef(), TargetIqn: strings.TrimSpace(*targetIQN)})
		if err != nil {
			return sbsctlISCSIAdminRPCErrorResult(cfg, "target", "get", err)
		}
		luns := sbsctlISCSILUNRows(resp.GetLuns())
		sessions := sbsctlISCSISessionRows(resp.GetSessions())
		acls := sbsctlISCSIACLRows(resp.GetInitiatorAcls())
		fields := map[string]any{
			"target_iqn":               strings.TrimSpace(*targetIQN),
			"target":                   sbsctlISCSITargetRow(resp.GetTarget()),
			"luns":                     luns,
			"lun_count":                len(luns),
			"initiator_acls":           acls,
			"initiator_acl_count":      len(acls),
			"sessions":                 sessions,
			"session_count":            len(sessions),
			"iscsi_registry_available": true,
			"registry_status":          iscsiRegistryReadyStatus,
		}
		applySBSCTLISCSIClusterRefFields(fields, resp.GetCluster())
		applySBSCTLISCSIRegistryFields(fields, resp.GetRegistryRevision(), resp.GetConfigGeneration())
		return sbsctlISCSIResult(cfg, "target", "get", fields)
	})
	writeJSON(out)
	if code != 0 {
		os.Exit(code)
	}
}

func runISCSITargetCreate(args []string) {
	defaults := mustResolveCLIDefaults(args)
	fs := flag.NewFlagSet("iscsi target create", flag.ExitOnError)
	targetIQN := fs.String("target-iqn", "", "target IQN")
	portalID := fs.String("portal-id", "", "portal id")
	portalIDs := fs.String("portal-ids", "", "comma-separated portal ids")
	exportID := fs.String("export-id", "", "export id")
	enabled := fs.Bool("enabled", true, "create target enabled")
	flags, mutation := addISCSIMutationFlags(fs, defaults)
	parseISCSIFlags(fs, args, flags)
	requireISCSIMutationKey(mutation)
	if strings.TrimSpace(*targetIQN) == "" {
		fatalf("iscsi target create requires --target-iqn")
	}
	resolvedPortalIDs := mergeISCSIStringList(*portalID, *portalIDs)
	if len(resolvedPortalIDs) == 0 {
		fatalf("iscsi target create requires --portal-id or --portal-ids")
	}

	out, code := withISCSIAdmin(flags.config(), "target", "create", func(ctx context.Context, client sbsctlISCSIAdminClient, cfg sbsctlISCSIConfig) map[string]any {
		resp, err := client.CreateISCSITarget(ctx, &adminv1.CreateISCSITargetRequest{
			Cluster:                  cfg.clusterRef(),
			Meta:                     mutation.requestMeta(),
			IdempotencyKey:           mutation.idempotencyKey(),
			ExpectedRegistryRevision: mutation.expectedRegistryRevision(),
			TargetIqn:                strings.TrimSpace(*targetIQN),
			PortalIds:                resolvedPortalIDs,
			ExportId:                 strings.TrimSpace(*exportID),
			Enabled:                  *enabled,
		})
		if err != nil {
			return sbsctlISCSIAdminRPCErrorResult(cfg, "target", "create", err)
		}
		fields := map[string]any{
			"target_iqn":               resp.GetTarget().GetTargetIqn(),
			"target":                   sbsctlISCSITargetRow(resp.GetTarget()),
			"operation_handle":         sbsctlISCSIOperationHandleRow(resp.GetOperation()),
			"iscsi_registry_available": true,
			"registry_status":          iscsiRegistryReadyStatus,
		}
		applySBSCTLISCSIClusterRefFields(fields, resp.GetCluster())
		applySBSCTLISCSIRegistryFields(fields, resp.GetRegistryRevision(), resp.GetConfigGeneration())
		return sbsctlISCSIResult(cfg, "target", "create", fields)
	})
	writeJSON(out)
	if code != 0 {
		os.Exit(code)
	}
}

func runISCSITargetDelete(args []string) {
	defaults := mustResolveCLIDefaults(args)
	fs := flag.NewFlagSet("iscsi target delete", flag.ExitOnError)
	targetIQN := fs.String("target-iqn", "", "target IQN")
	force := fs.Bool("force", false, "remove child LUN/ACL/session registry entries")
	yes := fs.Bool("yes", false, "confirm destructive registry mutation")
	flags, mutation := addISCSIMutationFlags(fs, defaults)
	parseISCSIFlags(fs, args, flags)
	requireISCSIMutationKey(mutation)
	requireISCSIYes(*yes, "iscsi target delete")
	if strings.TrimSpace(*targetIQN) == "" {
		fatalf("iscsi target delete requires --target-iqn")
	}

	out, code := withISCSIAdmin(flags.config(), "target", "delete", func(ctx context.Context, client sbsctlISCSIAdminClient, cfg sbsctlISCSIConfig) map[string]any {
		resp, err := client.DeleteISCSITarget(ctx, &adminv1.DeleteISCSITargetRequest{
			Cluster:                  cfg.clusterRef(),
			Meta:                     mutation.requestMeta(),
			IdempotencyKey:           mutation.idempotencyKey(),
			ExpectedRegistryRevision: mutation.expectedRegistryRevision(),
			TargetIqn:                strings.TrimSpace(*targetIQN),
			Force:                    *force,
		})
		if err != nil {
			return sbsctlISCSIAdminRPCErrorResult(cfg, "target", "delete", err)
		}
		fields := map[string]any{
			"target_iqn":               resp.GetTargetIqn(),
			"operation_handle":         sbsctlISCSIOperationHandleRow(resp.GetOperation()),
			"iscsi_registry_available": true,
			"registry_status":          iscsiRegistryReadyStatus,
		}
		applySBSCTLISCSIClusterRefFields(fields, resp.GetCluster())
		applySBSCTLISCSIRegistryFields(fields, resp.GetRegistryRevision(), resp.GetConfigGeneration())
		return sbsctlISCSIResult(cfg, "target", "delete", fields)
	})
	writeJSON(out)
	if code != 0 {
		os.Exit(code)
	}
}

func runISCSITargetSetEnabled(args []string, enabled bool) {
	defaults := mustResolveCLIDefaults(args)
	operation := "disable"
	if enabled {
		operation = "enable"
	}
	fs := flag.NewFlagSet("iscsi target "+operation, flag.ExitOnError)
	targetIQN := fs.String("target-iqn", "", "target IQN")
	flags, mutation := addISCSIMutationFlags(fs, defaults)
	parseISCSIFlags(fs, args, flags)
	requireISCSIMutationKey(mutation)
	if strings.TrimSpace(*targetIQN) == "" {
		fatalf("iscsi target %s requires --target-iqn", operation)
	}

	out, code := withISCSIAdmin(flags.config(), "target", operation, func(ctx context.Context, client sbsctlISCSIAdminClient, cfg sbsctlISCSIConfig) map[string]any {
		resp, err := client.SetISCSITargetEnabled(ctx, &adminv1.SetISCSITargetEnabledRequest{
			Cluster:                  cfg.clusterRef(),
			Meta:                     mutation.requestMeta(),
			IdempotencyKey:           mutation.idempotencyKey(),
			ExpectedRegistryRevision: mutation.expectedRegistryRevision(),
			TargetIqn:                strings.TrimSpace(*targetIQN),
			Enabled:                  enabled,
		})
		if err != nil {
			return sbsctlISCSIAdminRPCErrorResult(cfg, "target", operation, err)
		}
		fields := map[string]any{
			"target_iqn":               resp.GetTarget().GetTargetIqn(),
			"target":                   sbsctlISCSITargetRow(resp.GetTarget()),
			"operation_handle":         sbsctlISCSIOperationHandleRow(resp.GetOperation()),
			"iscsi_registry_available": true,
			"registry_status":          iscsiRegistryReadyStatus,
		}
		applySBSCTLISCSIClusterRefFields(fields, resp.GetCluster())
		applySBSCTLISCSIRegistryFields(fields, resp.GetRegistryRevision(), resp.GetConfigGeneration())
		return sbsctlISCSIResult(cfg, "target", operation, fields)
	})
	writeJSON(out)
	if code != 0 {
		os.Exit(code)
	}
}

func runISCSILUNList(args []string) {
	defaults := mustResolveCLIDefaults(args)
	fs := flag.NewFlagSet("iscsi lun list", flag.ExitOnError)
	targetIQN := fs.String("target-iqn", "", "optional target IQN")
	flags := addISCSIFlags(fs, defaults)
	parseISCSIFlags(fs, args, flags)

	out, code := withISCSIAdmin(flags.config(), "lun", "list", func(ctx context.Context, client sbsctlISCSIAdminClient, cfg sbsctlISCSIConfig) map[string]any {
		resp, err := client.ListISCSILUNs(ctx, &adminv1.ListISCSILUNsRequest{Cluster: cfg.clusterRef(), TargetIqn: strings.TrimSpace(*targetIQN)})
		if err != nil {
			return sbsctlISCSIAdminRPCErrorResult(cfg, "lun", "list", err)
		}
		luns := sbsctlISCSILUNRows(resp.GetLuns())
		fields := map[string]any{
			"target_iqn":               strings.TrimSpace(*targetIQN),
			"luns":                     luns,
			"count":                    len(luns),
			"sbs_volume_projection":    false,
			"iscsi_registry_available": true,
			"registry_status":          iscsiRegistryReadyStatus,
			"ok_count":                 1,
		}
		applySBSCTLISCSIClusterRefFields(fields, resp.GetCluster())
		applySBSCTLISCSIRegistryFields(fields, resp.GetRegistryRevision(), resp.GetConfigGeneration())
		return sbsctlISCSIResult(cfg, "lun", "list", fields)
	})
	writeJSON(out)
	if code != 0 {
		os.Exit(code)
	}
}

func runISCSILUNGet(args []string) {
	defaults := mustResolveCLIDefaults(args)
	fs := flag.NewFlagSet("iscsi lun get", flag.ExitOnError)
	targetIQN := fs.String("target-iqn", "", "target IQN")
	lunID := fs.Uint64("lun-id", 0, "LUN id")
	volumeID := fs.String("volume-id", "", "optional expected SBS volume id")
	flags := addISCSIFlags(fs, defaults)
	parseISCSIFlags(fs, args, flags)
	if *targetIQN == "" {
		fatalf("iscsi lun get requires --target-iqn")
	}
	if _, ok := visitedFlags(fs)["lun-id"]; !ok {
		fatalf("iscsi lun get requires --lun-id")
	}

	out, code := withISCSIAdmin(flags.config(), "lun", "get", func(ctx context.Context, client sbsctlISCSIAdminClient, cfg sbsctlISCSIConfig) map[string]any {
		resp, err := client.GetISCSILUN(ctx, &adminv1.GetISCSILUNRequest{Cluster: cfg.clusterRef(), TargetIqn: strings.TrimSpace(*targetIQN), LunId: *lunID})
		if err != nil {
			return sbsctlISCSIAdminRPCErrorResult(cfg, "lun", "get", err)
		}
		lun := resp.GetLun()
		expectedVolumeID := strings.TrimSpace(*volumeID)
		if expectedVolumeID != "" && lun.GetVolumeId() != expectedVolumeID {
			return sbsctlISCSIErrorResult(cfg, "lun", "get", "volume_id_mismatch", fmt.Sprintf("registry LUN maps to volume %q, not %q", lun.GetVolumeId(), expectedVolumeID))
		}
		fields := map[string]any{
			"target_iqn":               strings.TrimSpace(*targetIQN),
			"lun_id":                   *lunID,
			"volume_id":                lun.GetVolumeId(),
			"lun_found":                true,
			"lun":                      sbsctlISCSILUNRow(lun),
			"sbs_volume_projection":    false,
			"iscsi_registry_available": true,
			"registry_status":          iscsiRegistryReadyStatus,
			"ok_count":                 1,
		}
		applySBSCTLISCSIClusterRefFields(fields, resp.GetCluster())
		applySBSCTLISCSIRegistryFields(fields, resp.GetRegistryRevision(), resp.GetConfigGeneration())
		return sbsctlISCSIResult(cfg, "lun", "get", fields)
	})
	writeJSON(out)
	if code != 0 {
		os.Exit(code)
	}
}

func runISCSILUNExport(args []string) {
	defaults := mustResolveCLIDefaults(args)
	fs := flag.NewFlagSet("iscsi lun export", flag.ExitOnError)
	targetIQN := fs.String("target-iqn", "", "target IQN")
	lunID := fs.Uint64("lun-id", 0, "LUN id")
	exportID := fs.String("export-id", "", "export id")
	volumeID := fs.String("volume-id", "", "SBS volume id")
	exportMode := fs.String("export-mode", "read_write", "export mode: read_write or read_only")
	blockSize := fs.Uint64("logical-block-size-bytes", 4096, "logical block size bytes")
	enabled := fs.Bool("enabled", true, "export LUN enabled")
	flags, mutation := addISCSIMutationFlags(fs, defaults)
	parseISCSIFlags(fs, args, flags)
	requireISCSIMutationKey(mutation)
	if strings.TrimSpace(*targetIQN) == "" || strings.TrimSpace(*volumeID) == "" {
		fatalf("iscsi lun export requires --target-iqn and --volume-id")
	}
	if _, ok := visitedFlags(fs)["lun-id"]; !ok {
		fatalf("iscsi lun export requires --lun-id")
	}

	out, code := withISCSIAdmin(flags.config(), "lun", "export", func(ctx context.Context, client sbsctlISCSIAdminClient, cfg sbsctlISCSIConfig) map[string]any {
		resp, err := client.ExportISCSILUN(ctx, &adminv1.ExportISCSILUNRequest{
			Cluster:                  cfg.clusterRef(),
			Meta:                     mutation.requestMeta(),
			IdempotencyKey:           mutation.idempotencyKey(),
			ExpectedRegistryRevision: mutation.expectedRegistryRevision(),
			TargetIqn:                strings.TrimSpace(*targetIQN),
			LunId:                    *lunID,
			ExportId:                 strings.TrimSpace(*exportID),
			VolumeId:                 strings.TrimSpace(*volumeID),
			ExportMode:               strings.TrimSpace(*exportMode),
			LogicalBlockSizeBytes:    *blockSize,
			Enabled:                  *enabled,
		})
		if err != nil {
			return sbsctlISCSIAdminRPCErrorResult(cfg, "lun", "export", err)
		}
		fields := map[string]any{
			"target_iqn":               resp.GetLun().GetTargetIqn(),
			"lun_id":                   resp.GetLun().GetLunId(),
			"volume_id":                resp.GetLun().GetVolumeId(),
			"lun":                      sbsctlISCSILUNRow(resp.GetLun()),
			"operation_handle":         sbsctlISCSIOperationHandleRow(resp.GetOperation()),
			"iscsi_registry_available": true,
			"registry_status":          iscsiRegistryReadyStatus,
		}
		applySBSCTLISCSIClusterRefFields(fields, resp.GetCluster())
		applySBSCTLISCSIRegistryFields(fields, resp.GetRegistryRevision(), resp.GetConfigGeneration())
		return sbsctlISCSIResult(cfg, "lun", "export", fields)
	})
	writeJSON(out)
	if code != 0 {
		os.Exit(code)
	}
}

func runISCSILUNUnexport(args []string) {
	defaults := mustResolveCLIDefaults(args)
	fs := flag.NewFlagSet("iscsi lun unexport", flag.ExitOnError)
	targetIQN := fs.String("target-iqn", "", "target IQN")
	lunID := fs.Uint64("lun-id", 0, "LUN id")
	force := fs.Bool("force", false, "remove connected session records from registry")
	yes := fs.Bool("yes", false, "confirm destructive registry mutation")
	flags, mutation := addISCSIMutationFlags(fs, defaults)
	parseISCSIFlags(fs, args, flags)
	requireISCSIMutationKey(mutation)
	requireISCSIYes(*yes, "iscsi lun unexport")
	if strings.TrimSpace(*targetIQN) == "" {
		fatalf("iscsi lun unexport requires --target-iqn")
	}
	if _, ok := visitedFlags(fs)["lun-id"]; !ok {
		fatalf("iscsi lun unexport requires --lun-id")
	}

	out, code := withISCSIAdmin(flags.config(), "lun", "unexport", func(ctx context.Context, client sbsctlISCSIAdminClient, cfg sbsctlISCSIConfig) map[string]any {
		resp, err := client.UnexportISCSILUN(ctx, &adminv1.UnexportISCSILUNRequest{
			Cluster:                  cfg.clusterRef(),
			Meta:                     mutation.requestMeta(),
			IdempotencyKey:           mutation.idempotencyKey(),
			ExpectedRegistryRevision: mutation.expectedRegistryRevision(),
			TargetIqn:                strings.TrimSpace(*targetIQN),
			LunId:                    *lunID,
			Force:                    *force,
		})
		if err != nil {
			return sbsctlISCSIAdminRPCErrorResult(cfg, "lun", "unexport", err)
		}
		fields := map[string]any{
			"target_iqn":               resp.GetTargetIqn(),
			"lun_id":                   resp.GetLunId(),
			"operation_handle":         sbsctlISCSIOperationHandleRow(resp.GetOperation()),
			"iscsi_registry_available": true,
			"registry_status":          iscsiRegistryReadyStatus,
		}
		applySBSCTLISCSIClusterRefFields(fields, resp.GetCluster())
		applySBSCTLISCSIRegistryFields(fields, resp.GetRegistryRevision(), resp.GetConfigGeneration())
		return sbsctlISCSIResult(cfg, "lun", "unexport", fields)
	})
	writeJSON(out)
	if code != 0 {
		os.Exit(code)
	}
}

func runISCSILUNSetMode(args []string) {
	defaults := mustResolveCLIDefaults(args)
	fs := flag.NewFlagSet("iscsi lun set-mode", flag.ExitOnError)
	targetIQN := fs.String("target-iqn", "", "target IQN")
	lunID := fs.Uint64("lun-id", 0, "LUN id")
	exportMode := fs.String("export-mode", "", "export mode: read_write or read_only")
	flags, mutation := addISCSIMutationFlags(fs, defaults)
	parseISCSIFlags(fs, args, flags)
	requireISCSIMutationKey(mutation)
	if strings.TrimSpace(*targetIQN) == "" || strings.TrimSpace(*exportMode) == "" {
		fatalf("iscsi lun set-mode requires --target-iqn and --export-mode")
	}
	if _, ok := visitedFlags(fs)["lun-id"]; !ok {
		fatalf("iscsi lun set-mode requires --lun-id")
	}

	out, code := withISCSIAdmin(flags.config(), "lun", "set-mode", func(ctx context.Context, client sbsctlISCSIAdminClient, cfg sbsctlISCSIConfig) map[string]any {
		resp, err := client.SetISCSILUNMode(ctx, &adminv1.SetISCSILUNModeRequest{
			Cluster:                  cfg.clusterRef(),
			Meta:                     mutation.requestMeta(),
			IdempotencyKey:           mutation.idempotencyKey(),
			ExpectedRegistryRevision: mutation.expectedRegistryRevision(),
			TargetIqn:                strings.TrimSpace(*targetIQN),
			LunId:                    *lunID,
			ExportMode:               strings.TrimSpace(*exportMode),
		})
		if err != nil {
			return sbsctlISCSIAdminRPCErrorResult(cfg, "lun", "set-mode", err)
		}
		fields := map[string]any{
			"target_iqn":               resp.GetLun().GetTargetIqn(),
			"lun_id":                   resp.GetLun().GetLunId(),
			"volume_id":                resp.GetLun().GetVolumeId(),
			"lun":                      sbsctlISCSILUNRow(resp.GetLun()),
			"operation_handle":         sbsctlISCSIOperationHandleRow(resp.GetOperation()),
			"iscsi_registry_available": true,
			"registry_status":          iscsiRegistryReadyStatus,
		}
		applySBSCTLISCSIClusterRefFields(fields, resp.GetCluster())
		applySBSCTLISCSIRegistryFields(fields, resp.GetRegistryRevision(), resp.GetConfigGeneration())
		return sbsctlISCSIResult(cfg, "lun", "set-mode", fields)
	})
	writeJSON(out)
	if code != 0 {
		os.Exit(code)
	}
}

func runISCSIInitiatorList(args []string) {
	defaults := mustResolveCLIDefaults(args)
	fs := flag.NewFlagSet("iscsi initiator list", flag.ExitOnError)
	targetIQN := fs.String("target-iqn", "", "optional target IQN")
	flags := addISCSIFlags(fs, defaults)
	parseISCSIFlags(fs, args, flags)

	out, code := withISCSIAdmin(flags.config(), "initiator", "list", func(ctx context.Context, client sbsctlISCSIAdminClient, cfg sbsctlISCSIConfig) map[string]any {
		resp, err := client.ListISCSIInitiatorACLs(ctx, &adminv1.ListISCSIInitiatorACLsRequest{Cluster: cfg.clusterRef(), TargetIqn: strings.TrimSpace(*targetIQN)})
		if err != nil {
			return sbsctlISCSIAdminRPCErrorResult(cfg, "initiator", "list", err)
		}
		acls := sbsctlISCSIACLRows(resp.GetInitiatorAcls())
		fields := map[string]any{
			"target_iqn":               strings.TrimSpace(*targetIQN),
			"initiator_acls":           acls,
			"count":                    len(acls),
			"iscsi_registry_available": true,
			"registry_status":          iscsiRegistryReadyStatus,
		}
		applySBSCTLISCSIClusterRefFields(fields, resp.GetCluster())
		applySBSCTLISCSIRegistryFields(fields, resp.GetRegistryRevision(), resp.GetConfigGeneration())
		return sbsctlISCSIResult(cfg, "initiator", "list", fields)
	})
	writeJSON(out)
	if code != 0 {
		os.Exit(code)
	}
}

func runISCSIInitiatorGet(args []string) {
	defaults := mustResolveCLIDefaults(args)
	fs := flag.NewFlagSet("iscsi initiator get", flag.ExitOnError)
	initiatorIQN := fs.String("initiator-iqn", "", "initiator IQN")
	targetIQN := fs.String("target-iqn", "", "target IQN")
	flags := addISCSIFlags(fs, defaults)
	parseISCSIFlags(fs, args, flags)
	if strings.TrimSpace(*initiatorIQN) == "" || strings.TrimSpace(*targetIQN) == "" {
		fatalf("iscsi initiator get requires --initiator-iqn and --target-iqn")
	}

	out, code := withISCSIAdmin(flags.config(), "initiator", "get", func(ctx context.Context, client sbsctlISCSIAdminClient, cfg sbsctlISCSIConfig) map[string]any {
		resp, err := client.GetISCSIInitiatorACL(ctx, &adminv1.GetISCSIInitiatorACLRequest{
			Cluster:      cfg.clusterRef(),
			InitiatorIqn: strings.TrimSpace(*initiatorIQN),
			TargetIqn:    strings.TrimSpace(*targetIQN),
		})
		if err != nil {
			return sbsctlISCSIAdminRPCErrorResult(cfg, "initiator", "get", err)
		}
		fields := map[string]any{
			"initiator_iqn":            strings.TrimSpace(*initiatorIQN),
			"target_iqn":               strings.TrimSpace(*targetIQN),
			"initiator_acl":            sbsctlISCSIACLRow(resp.GetInitiatorAcl()),
			"iscsi_registry_available": true,
			"registry_status":          iscsiRegistryReadyStatus,
		}
		applySBSCTLISCSIClusterRefFields(fields, resp.GetCluster())
		applySBSCTLISCSIRegistryFields(fields, resp.GetRegistryRevision(), resp.GetConfigGeneration())
		return sbsctlISCSIResult(cfg, "initiator", "get", fields)
	})
	writeJSON(out)
	if code != 0 {
		os.Exit(code)
	}
}

func runISCSIInitiatorAllow(args []string) {
	defaults := mustResolveCLIDefaults(args)
	fs := flag.NewFlagSet("iscsi initiator allow", flag.ExitOnError)
	initiatorIQN := fs.String("initiator-iqn", "", "initiator IQN")
	targetIQN := fs.String("target-iqn", "", "target IQN")
	lunID := fs.Uint64("lun-id", 0, "single allowed LUN id")
	lunIDs := fs.String("lun-ids", "", "comma-separated allowed LUN ids")
	authMode := fs.String("auth-mode", "none", "auth mode: none or chap")
	chapSecretRef := fs.String("chap-secret-ref", "", "CHAP secret reference")
	enabled := fs.Bool("enabled", true, "create ACL enabled")
	flags, mutation := addISCSIMutationFlags(fs, defaults)
	parseISCSIFlags(fs, args, flags)
	requireISCSIMutationKey(mutation)
	if strings.TrimSpace(*initiatorIQN) == "" || strings.TrimSpace(*targetIQN) == "" {
		fatalf("iscsi initiator allow requires --initiator-iqn and --target-iqn")
	}
	_, lunIDSet := visitedFlags(fs)["lun-id"]
	allowedLUNs, err := parseISCSIAllowedLUNs(*lunIDs, *lunID, lunIDSet)
	if err != nil {
		fatalf("parse allowed LUNs: %v", err)
	}

	out, code := withISCSIAdmin(flags.config(), "initiator", "allow", func(ctx context.Context, client sbsctlISCSIAdminClient, cfg sbsctlISCSIConfig) map[string]any {
		resp, err := client.AllowISCSIInitiator(ctx, &adminv1.AllowISCSIInitiatorRequest{
			Cluster:                  cfg.clusterRef(),
			Meta:                     mutation.requestMeta(),
			IdempotencyKey:           mutation.idempotencyKey(),
			ExpectedRegistryRevision: mutation.expectedRegistryRevision(),
			InitiatorIqn:             strings.TrimSpace(*initiatorIQN),
			TargetIqn:                strings.TrimSpace(*targetIQN),
			AllowedLunIds:            allowedLUNs,
			AuthMode:                 strings.TrimSpace(*authMode),
			ChapSecretRef:            strings.TrimSpace(*chapSecretRef),
			Enabled:                  *enabled,
		})
		if err != nil {
			return sbsctlISCSIAdminRPCErrorResult(cfg, "initiator", "allow", err)
		}
		fields := map[string]any{
			"initiator_iqn":            resp.GetInitiatorAcl().GetInitiatorIqn(),
			"target_iqn":               resp.GetInitiatorAcl().GetTargetIqn(),
			"initiator_acl":            sbsctlISCSIACLRow(resp.GetInitiatorAcl()),
			"operation_handle":         sbsctlISCSIOperationHandleRow(resp.GetOperation()),
			"iscsi_registry_available": true,
			"registry_status":          iscsiRegistryReadyStatus,
		}
		applySBSCTLISCSIClusterRefFields(fields, resp.GetCluster())
		applySBSCTLISCSIRegistryFields(fields, resp.GetRegistryRevision(), resp.GetConfigGeneration())
		return sbsctlISCSIResult(cfg, "initiator", "allow", fields)
	})
	writeJSON(out)
	if code != 0 {
		os.Exit(code)
	}
}

func runISCSIInitiatorDeny(args []string) {
	defaults := mustResolveCLIDefaults(args)
	fs := flag.NewFlagSet("iscsi initiator deny", flag.ExitOnError)
	initiatorIQN := fs.String("initiator-iqn", "", "initiator IQN")
	targetIQN := fs.String("target-iqn", "", "target IQN")
	yes := fs.Bool("yes", false, "confirm destructive registry mutation")
	flags, mutation := addISCSIMutationFlags(fs, defaults)
	parseISCSIFlags(fs, args, flags)
	requireISCSIMutationKey(mutation)
	requireISCSIYes(*yes, "iscsi initiator deny")
	if strings.TrimSpace(*initiatorIQN) == "" || strings.TrimSpace(*targetIQN) == "" {
		fatalf("iscsi initiator deny requires --initiator-iqn and --target-iqn")
	}

	out, code := withISCSIAdmin(flags.config(), "initiator", "deny", func(ctx context.Context, client sbsctlISCSIAdminClient, cfg sbsctlISCSIConfig) map[string]any {
		resp, err := client.DenyISCSIInitiator(ctx, &adminv1.DenyISCSIInitiatorRequest{
			Cluster:                  cfg.clusterRef(),
			Meta:                     mutation.requestMeta(),
			IdempotencyKey:           mutation.idempotencyKey(),
			ExpectedRegistryRevision: mutation.expectedRegistryRevision(),
			InitiatorIqn:             strings.TrimSpace(*initiatorIQN),
			TargetIqn:                strings.TrimSpace(*targetIQN),
		})
		if err != nil {
			return sbsctlISCSIAdminRPCErrorResult(cfg, "initiator", "deny", err)
		}
		fields := map[string]any{
			"initiator_iqn":            resp.GetInitiatorIqn(),
			"target_iqn":               resp.GetTargetIqn(),
			"operation_handle":         sbsctlISCSIOperationHandleRow(resp.GetOperation()),
			"iscsi_registry_available": true,
			"registry_status":          iscsiRegistryReadyStatus,
		}
		applySBSCTLISCSIClusterRefFields(fields, resp.GetCluster())
		applySBSCTLISCSIRegistryFields(fields, resp.GetRegistryRevision(), resp.GetConfigGeneration())
		return sbsctlISCSIResult(cfg, "initiator", "deny", fields)
	})
	writeJSON(out)
	if code != 0 {
		os.Exit(code)
	}
}

func runISCSIInitiatorSetAuth(args []string) {
	defaults := mustResolveCLIDefaults(args)
	fs := flag.NewFlagSet("iscsi initiator set-auth", flag.ExitOnError)
	initiatorIQN := fs.String("initiator-iqn", "", "initiator IQN")
	targetIQN := fs.String("target-iqn", "", "target IQN")
	authMode := fs.String("auth-mode", "", "auth mode: none or chap")
	chapSecretRef := fs.String("chap-secret-ref", "", "CHAP secret reference")
	flags, mutation := addISCSIMutationFlags(fs, defaults)
	parseISCSIFlags(fs, args, flags)
	requireISCSIMutationKey(mutation)
	if strings.TrimSpace(*initiatorIQN) == "" || strings.TrimSpace(*targetIQN) == "" || strings.TrimSpace(*authMode) == "" {
		fatalf("iscsi initiator set-auth requires --initiator-iqn, --target-iqn, and --auth-mode")
	}

	out, code := withISCSIAdmin(flags.config(), "initiator", "set-auth", func(ctx context.Context, client sbsctlISCSIAdminClient, cfg sbsctlISCSIConfig) map[string]any {
		resp, err := client.SetISCSIInitiatorAuth(ctx, &adminv1.SetISCSIInitiatorAuthRequest{
			Cluster:                  cfg.clusterRef(),
			Meta:                     mutation.requestMeta(),
			IdempotencyKey:           mutation.idempotencyKey(),
			ExpectedRegistryRevision: mutation.expectedRegistryRevision(),
			InitiatorIqn:             strings.TrimSpace(*initiatorIQN),
			TargetIqn:                strings.TrimSpace(*targetIQN),
			AuthMode:                 strings.TrimSpace(*authMode),
			ChapSecretRef:            strings.TrimSpace(*chapSecretRef),
		})
		if err != nil {
			return sbsctlISCSIAdminRPCErrorResult(cfg, "initiator", "set-auth", err)
		}
		fields := map[string]any{
			"initiator_iqn":            resp.GetInitiatorAcl().GetInitiatorIqn(),
			"target_iqn":               resp.GetInitiatorAcl().GetTargetIqn(),
			"initiator_acl":            sbsctlISCSIACLRow(resp.GetInitiatorAcl()),
			"operation_handle":         sbsctlISCSIOperationHandleRow(resp.GetOperation()),
			"iscsi_registry_available": true,
			"registry_status":          iscsiRegistryReadyStatus,
		}
		applySBSCTLISCSIClusterRefFields(fields, resp.GetCluster())
		applySBSCTLISCSIRegistryFields(fields, resp.GetRegistryRevision(), resp.GetConfigGeneration())
		return sbsctlISCSIResult(cfg, "initiator", "set-auth", fields)
	})
	writeJSON(out)
	if code != 0 {
		os.Exit(code)
	}
}

func runISCSISessionList(args []string) {
	defaults := mustResolveCLIDefaults(args)
	fs := flag.NewFlagSet("iscsi session list", flag.ExitOnError)
	targetIQN := fs.String("target-iqn", "", "optional target IQN")
	initiatorIQN := fs.String("initiator-iqn", "", "optional initiator IQN")
	connectedOnly := fs.Bool("connected-only", false, "show connected sessions only")
	flags := addISCSIFlags(fs, defaults)
	parseISCSIFlags(fs, args, flags)

	out, code := withISCSIAdmin(flags.config(), "session", "list", func(ctx context.Context, client sbsctlISCSIAdminClient, cfg sbsctlISCSIConfig) map[string]any {
		resp, err := client.ListISCSISessions(ctx, &adminv1.ListISCSISessionsRequest{
			Cluster:       cfg.clusterRef(),
			TargetIqn:     strings.TrimSpace(*targetIQN),
			InitiatorIqn:  strings.TrimSpace(*initiatorIQN),
			ConnectedOnly: *connectedOnly,
		})
		if err != nil {
			return sbsctlISCSIAdminRPCErrorResult(cfg, "session", "list", err)
		}
		sessions := sbsctlISCSISessionRows(resp.GetSessions())
		fields := map[string]any{
			"target_iqn":               strings.TrimSpace(*targetIQN),
			"initiator_iqn":            strings.TrimSpace(*initiatorIQN),
			"connected_only":           *connectedOnly,
			"sessions":                 sessions,
			"count":                    len(sessions),
			"observability_counters":   sbsctlISCSIObservabilityCountersRow(resp.GetObservabilityCounters()),
			"iscsi_registry_available": true,
			"registry_status":          iscsiRegistryReadyStatus,
		}
		applySBSCTLISCSIClusterRefFields(fields, resp.GetCluster())
		applySBSCTLISCSIRegistryFields(fields, resp.GetRegistryRevision(), resp.GetConfigGeneration())
		return sbsctlISCSIResult(cfg, "session", "list", fields)
	})
	writeJSON(out)
	if code != 0 {
		os.Exit(code)
	}
}

func runISCSISessionGet(args []string) {
	defaults := mustResolveCLIDefaults(args)
	fs := flag.NewFlagSet("iscsi session get", flag.ExitOnError)
	sessionID := fs.String("session-id", "", "session id")
	flags := addISCSIFlags(fs, defaults)
	parseISCSIFlags(fs, args, flags)
	if strings.TrimSpace(*sessionID) == "" {
		fatalf("iscsi session get requires --session-id")
	}

	out, code := withISCSIAdmin(flags.config(), "session", "get", func(ctx context.Context, client sbsctlISCSIAdminClient, cfg sbsctlISCSIConfig) map[string]any {
		resp, err := client.GetISCSISession(ctx, &adminv1.GetISCSISessionRequest{Cluster: cfg.clusterRef(), SessionId: strings.TrimSpace(*sessionID)})
		if err != nil {
			return sbsctlISCSIAdminRPCErrorResult(cfg, "session", "get", err)
		}
		fields := map[string]any{
			"session_id":               strings.TrimSpace(*sessionID),
			"session":                  sbsctlISCSISessionRow(resp.GetSession()),
			"iscsi_registry_available": true,
			"registry_status":          iscsiRegistryReadyStatus,
		}
		applySBSCTLISCSIClusterRefFields(fields, resp.GetCluster())
		applySBSCTLISCSIRegistryFields(fields, resp.GetRegistryRevision(), resp.GetConfigGeneration())
		return sbsctlISCSIResult(cfg, "session", "get", fields)
	})
	writeJSON(out)
	if code != 0 {
		os.Exit(code)
	}
}

func runISCSISessionDisconnect(args []string) {
	defaults := mustResolveCLIDefaults(args)
	fs := flag.NewFlagSet("iscsi session disconnect", flag.ExitOnError)
	sessionID := fs.String("session-id", "", "session id")
	force := fs.Bool("force", false, "force registry disconnect request")
	yes := fs.Bool("yes", false, "confirm disruptive session mutation")
	flags, mutation := addISCSIMutationFlags(fs, defaults)
	parseISCSIFlags(fs, args, flags)
	requireISCSIMutationKey(mutation)
	requireISCSIYes(*yes, "iscsi session disconnect")
	if strings.TrimSpace(*sessionID) == "" {
		fatalf("iscsi session disconnect requires --session-id")
	}

	out, code := withISCSIAdmin(flags.config(), "session", "disconnect", func(ctx context.Context, client sbsctlISCSIAdminClient, cfg sbsctlISCSIConfig) map[string]any {
		resp, err := client.DisconnectISCSISession(ctx, &adminv1.DisconnectISCSISessionRequest{
			Cluster:                  cfg.clusterRef(),
			Meta:                     mutation.requestMeta(),
			IdempotencyKey:           mutation.idempotencyKey(),
			ExpectedRegistryRevision: mutation.expectedRegistryRevision(),
			SessionId:                strings.TrimSpace(*sessionID),
			Force:                    *force,
		})
		if err != nil {
			return sbsctlISCSIAdminRPCErrorResult(cfg, "session", "disconnect", err)
		}
		fields := map[string]any{
			"session_id":               resp.GetSession().GetSessionId(),
			"session":                  sbsctlISCSISessionRow(resp.GetSession()),
			"disconnect_requested":     resp.GetDisconnectRequested(),
			"operation_handle":         sbsctlISCSIOperationHandleRow(resp.GetOperation()),
			"iscsi_registry_available": true,
			"registry_status":          iscsiRegistryReadyStatus,
		}
		applySBSCTLISCSIClusterRefFields(fields, resp.GetCluster())
		applySBSCTLISCSIRegistryFields(fields, resp.GetRegistryRevision(), resp.GetConfigGeneration())
		return sbsctlISCSIResult(cfg, "session", "disconnect", fields)
	})
	writeJSON(out)
	if code != 0 {
		os.Exit(code)
	}
}

func runISCSIFailoverStatus(args []string) {
	defaults := mustResolveCLIDefaults(args)
	fs := flag.NewFlagSet("iscsi failover status", flag.ExitOnError)
	exportID := fs.String("export-id", "", "export id")
	flags := addISCSIFlags(fs, defaults)
	parseISCSIFlags(fs, args, flags)
	if strings.TrimSpace(*exportID) == "" {
		fatalf("iscsi failover status requires --export-id")
	}

	out, code := withISCSIAdmin(flags.config(), "failover", "status", func(ctx context.Context, client sbsctlISCSIAdminClient, cfg sbsctlISCSIConfig) map[string]any {
		resp, err := client.GetISCSIFailover(ctx, &adminv1.GetISCSIFailoverRequest{Cluster: cfg.clusterRef(), ExportId: strings.TrimSpace(*exportID)})
		if err != nil {
			return sbsctlISCSIAdminRPCErrorResult(cfg, "failover", "status", err)
		}
		fields := map[string]any{
			"export_id":                strings.TrimSpace(*exportID),
			"failover":                 sbsctlISCSIFailoverRow(resp.GetFailover()),
			"iscsi_registry_available": true,
			"registry_status":          iscsiRegistryReadyStatus,
		}
		applySBSCTLISCSIClusterRefFields(fields, resp.GetCluster())
		applySBSCTLISCSIRegistryFields(fields, resp.GetRegistryRevision(), resp.GetConfigGeneration())
		return sbsctlISCSIResult(cfg, "failover", "status", fields)
	})
	writeJSON(out)
	if code != 0 {
		os.Exit(code)
	}
}

func runISCSIFailoverPromote(args []string) {
	defaults := mustResolveCLIDefaults(args)
	fs := flag.NewFlagSet("iscsi failover promote", flag.ExitOnError)
	exportID := fs.String("export-id", "", "export id")
	gatewayID := fs.String("gateway-id", "", "gateway id to promote")
	exportLeaseID := fs.String("export-lease-id", "", "new export lease id")
	trigger := fs.String("trigger", "manual_promote", "failover trigger")
	yes := fs.Bool("yes", false, "confirm writer authority mutation")
	flags, mutation := addISCSIMutationFlags(fs, defaults)
	parseISCSIFlags(fs, args, flags)
	requireISCSIMutationKey(mutation)
	requireISCSIYes(*yes, "iscsi failover promote")
	if strings.TrimSpace(*exportID) == "" || strings.TrimSpace(*gatewayID) == "" {
		fatalf("iscsi failover promote requires --export-id and --gateway-id")
	}

	out, code := withISCSIAdmin(flags.config(), "failover", "promote", func(ctx context.Context, client sbsctlISCSIAdminClient, cfg sbsctlISCSIConfig) map[string]any {
		resp, err := client.PromoteISCSIFailover(ctx, &adminv1.PromoteISCSIFailoverRequest{
			Cluster:                  cfg.clusterRef(),
			Meta:                     mutation.requestMeta(),
			IdempotencyKey:           mutation.idempotencyKey(),
			ExpectedRegistryRevision: mutation.expectedRegistryRevision(),
			ExportId:                 strings.TrimSpace(*exportID),
			GatewayId:                strings.TrimSpace(*gatewayID),
			ExportLeaseId:            strings.TrimSpace(*exportLeaseID),
			Trigger:                  strings.TrimSpace(*trigger),
		})
		if err != nil {
			return sbsctlISCSIAdminRPCErrorResult(cfg, "failover", "promote", err)
		}
		fields := sbsctlISCSIFailoverMutationFields(resp.GetCluster(), resp.GetRegistryRevision(), resp.GetConfigGeneration(), resp.GetOperation(), resp.GetFailover())
		return sbsctlISCSIResult(cfg, "failover", "promote", fields)
	})
	writeJSON(out)
	if code != 0 {
		os.Exit(code)
	}
}

func runISCSIFailoverDemote(args []string) {
	defaults := mustResolveCLIDefaults(args)
	fs := flag.NewFlagSet("iscsi failover demote", flag.ExitOnError)
	exportID := fs.String("export-id", "", "export id")
	gatewayID := fs.String("gateway-id", "", "active gateway id to demote; defaults to current active gateway")
	trigger := fs.String("trigger", "manual_demote", "failover trigger")
	yes := fs.Bool("yes", false, "confirm writer authority mutation")
	flags, mutation := addISCSIMutationFlags(fs, defaults)
	parseISCSIFlags(fs, args, flags)
	requireISCSIMutationKey(mutation)
	requireISCSIYes(*yes, "iscsi failover demote")
	if strings.TrimSpace(*exportID) == "" {
		fatalf("iscsi failover demote requires --export-id")
	}

	out, code := withISCSIAdmin(flags.config(), "failover", "demote", func(ctx context.Context, client sbsctlISCSIAdminClient, cfg sbsctlISCSIConfig) map[string]any {
		resp, err := client.DemoteISCSIFailover(ctx, &adminv1.DemoteISCSIFailoverRequest{
			Cluster:                  cfg.clusterRef(),
			Meta:                     mutation.requestMeta(),
			IdempotencyKey:           mutation.idempotencyKey(),
			ExpectedRegistryRevision: mutation.expectedRegistryRevision(),
			ExportId:                 strings.TrimSpace(*exportID),
			GatewayId:                strings.TrimSpace(*gatewayID),
			Trigger:                  strings.TrimSpace(*trigger),
		})
		if err != nil {
			return sbsctlISCSIAdminRPCErrorResult(cfg, "failover", "demote", err)
		}
		fields := sbsctlISCSIFailoverMutationFields(resp.GetCluster(), resp.GetRegistryRevision(), resp.GetConfigGeneration(), resp.GetOperation(), resp.GetFailover())
		return sbsctlISCSIResult(cfg, "failover", "demote", fields)
	})
	writeJSON(out)
	if code != 0 {
		os.Exit(code)
	}
}

func runISCSIFailoverStandby(args []string) {
	defaults := mustResolveCLIDefaults(args)
	fs := flag.NewFlagSet("iscsi failover standby", flag.ExitOnError)
	exportID := fs.String("export-id", "", "export id")
	gatewayID := fs.String("gateway-id", "", "standby gateway id")
	flags, mutation := addISCSIMutationFlags(fs, defaults)
	parseISCSIFlags(fs, args, flags)
	requireISCSIMutationKey(mutation)
	if strings.TrimSpace(*exportID) == "" || strings.TrimSpace(*gatewayID) == "" {
		fatalf("iscsi failover standby requires --export-id and --gateway-id")
	}

	out, code := withISCSIAdmin(flags.config(), "failover", "standby", func(ctx context.Context, client sbsctlISCSIAdminClient, cfg sbsctlISCSIConfig) map[string]any {
		resp, err := client.StandbyISCSIFailover(ctx, &adminv1.StandbyISCSIFailoverRequest{
			Cluster:                  cfg.clusterRef(),
			Meta:                     mutation.requestMeta(),
			IdempotencyKey:           mutation.idempotencyKey(),
			ExpectedRegistryRevision: mutation.expectedRegistryRevision(),
			ExportId:                 strings.TrimSpace(*exportID),
			GatewayId:                strings.TrimSpace(*gatewayID),
		})
		if err != nil {
			return sbsctlISCSIAdminRPCErrorResult(cfg, "failover", "standby", err)
		}
		fields := sbsctlISCSIFailoverMutationFields(resp.GetCluster(), resp.GetRegistryRevision(), resp.GetConfigGeneration(), resp.GetOperation(), resp.GetFailover())
		return sbsctlISCSIResult(cfg, "failover", "standby", fields)
	})
	writeJSON(out)
	if code != 0 {
		os.Exit(code)
	}
}

func runISCSIFailoverRevokeStale(args []string) {
	defaults := mustResolveCLIDefaults(args)
	fs := flag.NewFlagSet("iscsi failover revoke-stale", flag.ExitOnError)
	exportID := fs.String("export-id", "", "export id")
	gatewayID := fs.String("gateway-id", "", "stale gateway id to revoke")
	trigger := fs.String("trigger", "manual_revoke_stale", "failover trigger")
	yes := fs.Bool("yes", false, "confirm stale writer fencing mutation")
	flags, mutation := addISCSIMutationFlags(fs, defaults)
	parseISCSIFlags(fs, args, flags)
	requireISCSIMutationKey(mutation)
	requireISCSIYes(*yes, "iscsi failover revoke-stale")
	if strings.TrimSpace(*exportID) == "" || strings.TrimSpace(*gatewayID) == "" {
		fatalf("iscsi failover revoke-stale requires --export-id and --gateway-id")
	}

	out, code := withISCSIAdmin(flags.config(), "failover", "revoke-stale", func(ctx context.Context, client sbsctlISCSIAdminClient, cfg sbsctlISCSIConfig) map[string]any {
		resp, err := client.RevokeStaleISCSIFailover(ctx, &adminv1.RevokeStaleISCSIFailoverRequest{
			Cluster:                  cfg.clusterRef(),
			Meta:                     mutation.requestMeta(),
			IdempotencyKey:           mutation.idempotencyKey(),
			ExpectedRegistryRevision: mutation.expectedRegistryRevision(),
			ExportId:                 strings.TrimSpace(*exportID),
			GatewayId:                strings.TrimSpace(*gatewayID),
			Trigger:                  strings.TrimSpace(*trigger),
		})
		if err != nil {
			return sbsctlISCSIAdminRPCErrorResult(cfg, "failover", "revoke-stale", err)
		}
		fields := sbsctlISCSIFailoverMutationFields(resp.GetCluster(), resp.GetRegistryRevision(), resp.GetConfigGeneration(), resp.GetOperation(), resp.GetFailover())
		return sbsctlISCSIResult(cfg, "failover", "revoke-stale", fields)
	})
	writeJSON(out)
	if code != 0 {
		os.Exit(code)
	}
}

type sbsctlISCSIFlags struct {
	AdminEndpoint *string
	DataEndpoint  *string
	ClusterID     *string
	SBSClusterID  *string
	Output        *string
	JSON          *bool
	Timeout       *time.Duration
	Defaults      cliDefaults
}

type sbsctlISCSIMutationFlags struct {
	Actor                    *string
	Reason                   *string
	IdempotencyKey           *string
	ExpectedRegistryRevision *uint64
}

func addISCSIFlags(fs *flag.FlagSet, defaults cliDefaults) sbsctlISCSIFlags {
	registerContextFlags(fs, defaults)
	return sbsctlISCSIFlags{
		AdminEndpoint: fs.String("admin-endpoint", defaults.adminEndpoint(), "cluster-wide sbs-admin gRPC endpoint"),
		DataEndpoint:  fs.String("data-endpoint", defaults.dataEndpoint(), "sbs-data gRPC endpoint for output authority context"),
		ClusterID:     fs.String("cluster-id", defaults.fieldValue("cluster_id", "NAMRBD_CLUSTER_ID"), "cluster id"),
		SBSClusterID:  fs.String("sbs-cluster-id", defaults.fieldValue("sbs_cluster_id", "SBS_CLUSTER_ID", "NAMRBD_SBS_CLUSTER_ID"), "sbs cluster id"),
		Output:        fs.String("output", defaults.fieldValue("output", "SBS_OUTPUT", "NAMRBD_OUTPUT"), "output format: json"),
		JSON:          fs.Bool("json", false, "emit JSON"),
		Timeout:       fs.Duration("timeout", defaults.timeout(10*time.Second), "request timeout"),
		Defaults:      defaults,
	}
}

func addISCSIMutationFlags(fs *flag.FlagSet, defaults cliDefaults) (sbsctlISCSIFlags, sbsctlISCSIMutationFlags) {
	flags := addISCSIFlags(fs, defaults)
	actorDefault := strings.TrimSpace(os.Getenv("USER"))
	if actorDefault == "" {
		actorDefault = "sbsctl"
	}
	return flags, sbsctlISCSIMutationFlags{
		Actor:                    fs.String("actor", actorDefault, "mutation actor"),
		Reason:                   fs.String("reason", "", "mutation reason"),
		IdempotencyKey:           fs.String("idempotency-key", "", "mutation idempotency key"),
		ExpectedRegistryRevision: fs.Uint64("expected-registry-revision", 0, "expected registry revision; 0 disables the precondition"),
	}
}

func parseISCSIFlags(fs *flag.FlagSet, args []string, flags sbsctlISCSIFlags) {
	fs.Parse(args)
	if *flags.JSON {
		*flags.Output = "json"
	}
	if strings.TrimSpace(*flags.Output) == "" {
		*flags.Output = "json"
	}
	if *flags.Output != "json" {
		fatalf("sbsctl iscsi supports --output json only until registry table views are defined")
	}
	printResolvedSettings(fs,
		flags.Defaults.adminEndpointSetting(),
		flags.Defaults.dataEndpointSetting(),
		flags.Defaults.fieldSetting("cluster_id", "cluster-id", "", "NAMRBD_CLUSTER_ID"),
		flags.Defaults.fieldSetting("sbs_cluster_id", "sbs-cluster-id", "", "SBS_CLUSTER_ID", "NAMRBD_SBS_CLUSTER_ID"),
		flags.Defaults.fieldSetting("output", "output", "json", "SBS_OUTPUT", "NAMRBD_OUTPUT"),
		flags.Defaults.timeoutSetting(10*time.Second),
	)
}

func (f sbsctlISCSIMutationFlags) requestMeta() *adminv1.RequestMeta {
	return &adminv1.RequestMeta{
		Actor:  strings.TrimSpace(*f.Actor),
		Reason: strings.TrimSpace(*f.Reason),
	}
}

func (f sbsctlISCSIMutationFlags) idempotencyKey() string {
	return strings.TrimSpace(*f.IdempotencyKey)
}

func (f sbsctlISCSIMutationFlags) expectedRegistryRevision() uint64 {
	return *f.ExpectedRegistryRevision
}

func requireISCSIMutationKey(flags sbsctlISCSIMutationFlags) {
	if flags.idempotencyKey() == "" {
		fatalf("sbsctl iscsi mutation requires --idempotency-key")
	}
	if strings.TrimSpace(*flags.Actor) == "" {
		fatalf("sbsctl iscsi mutation requires --actor")
	}
}

func requireISCSIYes(yes bool, operation string) {
	if !yes {
		fatalf("%s requires --yes", operation)
	}
}

func (f sbsctlISCSIFlags) config() sbsctlISCSIConfig {
	return sbsctlISCSIConfig{
		ClusterID:     strings.TrimSpace(*f.ClusterID),
		SBSClusterID:  strings.TrimSpace(*f.SBSClusterID),
		ContextFile:   f.Defaults.contextFile,
		ContextName:   f.Defaults.contextName,
		AdminEndpoint: strings.TrimSpace(*f.AdminEndpoint),
		DataEndpoint:  strings.TrimSpace(*f.DataEndpoint),
		Timeout:       *f.Timeout,
	}
}

func withISCSIAdmin(cfg sbsctlISCSIConfig, objectType, operation string, read func(context.Context, sbsctlISCSIAdminClient, sbsctlISCSIConfig) map[string]any) (map[string]any, int) {
	if cfg.AdminEndpoint == "" {
		return sbsctlISCSIErrorResult(cfg, objectType, operation, "cluster_admin_endpoint_required", "admin endpoint is required (use --admin-endpoint or SBS_ADMIN_ENDPOINTS/NAMRBD_SBS_ADMIN_ENDPOINTS)"), 1
	}
	ctx, cancel := context.WithTimeout(context.Background(), cfg.Timeout)
	defer cancel()
	client, closeClient, err := newSBSCTLISCSIAdminClient(ctx, cfg.AdminEndpoint)
	if err != nil {
		return sbsctlISCSIErrorResult(cfg, objectType, operation, "cluster_admin_client_error", err.Error()), 1
	}
	if closeClient != nil {
		defer func() {
			if err := closeClient(); err != nil {
				fmt.Fprintf(os.Stderr, "close cluster admin client: %v\n", err)
			}
		}()
	}
	out := read(ctx, client, cfg)
	if result, _ := out["result"].(string); result == "error" {
		return out, 1
	}
	return out, 0
}

func (cfg sbsctlISCSIConfig) clusterRef() *adminv1.ClusterRef {
	if cfg.ClusterID == "" && cfg.SBSClusterID == "" {
		return nil
	}
	return clusterRef(cfg.ClusterID, cfg.SBSClusterID)
}

func sbsctlISCSIResult(cfg sbsctlISCSIConfig, objectType, operation string, fields map[string]any) map[string]any {
	out := map[string]any{
		"result":              "ok",
		"entrypoint":          "sbsctl iscsi " + objectType + " " + operation,
		"control_plane_mode":  "sbs_cluster",
		"metadata_authority":  "cluster_iscsi_control_plane",
		"storage_authority":   "sbs_service",
		"cluster_id":          cfg.ClusterID,
		"sbs_cluster_id":      cfg.SBSClusterID,
		"context_name":        cfg.ContextName,
		"admin_endpoint":      cfg.AdminEndpoint,
		"data_endpoint":       cfg.DataEndpoint,
		"registry_revision":   0,
		"config_generation":   0,
		"iscsi_edition":       iscsi.ISCSIEdition,
		"export_volume_limit": iscsi.ISCSIExportVolumeLimit,
		"object_type":         objectType,
		"operation":           operation,
		"error_count":         0,
	}
	if cfg.ContextFile != "" {
		out["context_file"] = cfg.ContextFile
	}
	for k, v := range fields {
		out[k] = v
	}
	if _, ok := out["ok_count"]; !ok {
		out["ok_count"] = 1
	}
	return out
}

func sbsctlISCSIErrorResult(cfg sbsctlISCSIConfig, objectType, operation, reason, message string) map[string]any {
	return sbsctlISCSIResult(cfg, objectType, operation, map[string]any{
		"result":           "error",
		"ok_count":         0,
		"error_count":      1,
		"rejection_reason": reason,
		"error":            message,
	})
}

func sbsctlISCSIAdminRPCErrorResult(cfg sbsctlISCSIConfig, objectType, operation string, err error) map[string]any {
	reason := "cluster_admin_rpc_error"
	switch status.Code(err) {
	case codes.NotFound:
		reason = "cluster_iscsi_registry_not_found"
	case codes.InvalidArgument:
		reason = "cluster_iscsi_invalid_request"
	case codes.FailedPrecondition:
		reason = "cluster_iscsi_precondition_failed"
	case codes.AlreadyExists:
		reason = "cluster_iscsi_registry_already_exists"
	}
	return sbsctlISCSIErrorResult(cfg, objectType, operation, reason, err.Error())
}

func applySBSCTLISCSIClusterRefFields(fields map[string]any, cluster *adminv1.ClusterRef) {
	if cluster == nil {
		return
	}
	if value := strings.TrimSpace(cluster.GetClusterId()); value != "" {
		fields["cluster_id"] = value
	}
	if value := strings.TrimSpace(cluster.GetSbsClusterId()); value != "" {
		fields["sbs_cluster_id"] = value
	}
}

func applySBSCTLISCSIRegistryFields(fields map[string]any, registryRevision, configGeneration uint64) {
	fields["registry_revision"] = registryRevision
	fields["config_generation"] = configGeneration
}

func sbsctlISCSIPortalRows(portals []*adminv1.ISCSIPortalSummary) []map[string]any {
	rows := make([]map[string]any, 0, len(portals))
	for _, portal := range portals {
		rows = append(rows, sbsctlISCSIPortalRow(portal))
	}
	return rows
}

func sbsctlISCSIPortalRow(portal *adminv1.ISCSIPortalSummary) map[string]any {
	if portal == nil {
		return nil
	}
	return map[string]any{
		"portal_id":  portal.GetPortalId(),
		"address":    portal.GetAddress(),
		"gateway_id": portal.GetGatewayId(),
		"enabled":    portal.GetEnabled(),
	}
}

func sbsctlISCSIFindTarget(targets []*adminv1.ISCSITargetSummary, targetIQN string) *adminv1.ISCSITargetSummary {
	targetIQN = strings.TrimSpace(targetIQN)
	for _, target := range targets {
		if target.GetTargetIqn() == targetIQN {
			return target
		}
	}
	return nil
}

func sbsctlISCSITargetRows(targets []*adminv1.ISCSITargetSummary) []map[string]any {
	rows := make([]map[string]any, 0, len(targets))
	for _, target := range targets {
		rows = append(rows, sbsctlISCSITargetRow(target))
	}
	return rows
}

func sbsctlISCSIFilterLUNs(luns []*adminv1.ISCSILUNSummary, targetIQN string) []*adminv1.ISCSILUNSummary {
	targetIQN = strings.TrimSpace(targetIQN)
	out := make([]*adminv1.ISCSILUNSummary, 0, len(luns))
	for _, lun := range luns {
		if targetIQN != "" && lun.GetTargetIqn() != targetIQN {
			continue
		}
		out = append(out, lun)
	}
	return out
}

func sbsctlISCSIFilterSessions(sessions []*adminv1.ISCSISessionSummary, targetIQN, initiatorIQN string) []*adminv1.ISCSISessionSummary {
	targetIQN = strings.TrimSpace(targetIQN)
	initiatorIQN = strings.TrimSpace(initiatorIQN)
	out := make([]*adminv1.ISCSISessionSummary, 0, len(sessions))
	for _, session := range sessions {
		if targetIQN != "" && session.GetTargetIqn() != targetIQN {
			continue
		}
		if initiatorIQN != "" && session.GetInitiatorIqn() != initiatorIQN {
			continue
		}
		out = append(out, session)
	}
	return out
}

func sbsctlISCSITargetRow(target *adminv1.ISCSITargetSummary) map[string]any {
	if target == nil {
		return nil
	}
	return map[string]any{
		"target_iqn": target.GetTargetIqn(),
		"portal_id":  target.GetPortalId(),
		"portal_ids": append([]string{}, target.GetPortalIds()...),
		"export_id":  target.GetExportId(),
		"enabled":    target.GetEnabled(),
	}
}

func sbsctlISCSILUNRows(luns []*adminv1.ISCSILUNSummary) []map[string]any {
	rows := make([]map[string]any, 0, len(luns))
	for _, lun := range luns {
		rows = append(rows, sbsctlISCSILUNRow(lun))
	}
	return rows
}

func sbsctlISCSILUNRow(lun *adminv1.ISCSILUNSummary) map[string]any {
	if lun == nil {
		return nil
	}
	return map[string]any{
		"target_iqn":               lun.GetTargetIqn(),
		"lun_id":                   lun.GetLunId(),
		"lun_wwn":                  lun.GetLunWwn(),
		"export_id":                lun.GetExportId(),
		"volume_id":                lun.GetVolumeId(),
		"export_mode":              lun.GetExportMode(),
		"logical_block_size_bytes": lun.GetLogicalBlockSizeBytes(),
		"enabled":                  lun.GetEnabled(),
	}
}

func sbsctlISCSIACLRows(acls []*adminv1.ISCSIInitiatorACLSummary) []map[string]any {
	rows := make([]map[string]any, 0, len(acls))
	for _, acl := range acls {
		rows = append(rows, sbsctlISCSIACLRow(acl))
	}
	return rows
}

func sbsctlISCSIACLRow(acl *adminv1.ISCSIInitiatorACLSummary) map[string]any {
	if acl == nil {
		return nil
	}
	return map[string]any{
		"initiator_iqn":   acl.GetInitiatorIqn(),
		"target_iqn":      acl.GetTargetIqn(),
		"allowed_lun_ids": append([]uint64{}, acl.GetAllowedLunIds()...),
		"auth_mode":       acl.GetAuthMode(),
		"chap_secret_set": acl.GetChapSecretSet(),
		"chap_secret_ref": acl.GetChapSecretRef(),
		"enabled":         acl.GetEnabled(),
	}
}

func sbsctlISCSIOperationHandleRow(op *adminv1.OperationHandle) map[string]any {
	if op == nil {
		return nil
	}
	return map[string]any{
		"accepted":     op.GetAccepted(),
		"operation_id": op.GetOperationId(),
		"message":      op.GetMessage(),
	}
}

func sbsctlISCSIFailoverMutationFields(cluster *adminv1.ClusterRef, registryRevision, configGeneration uint64, op *adminv1.OperationHandle, failover *adminv1.ISCSIFailoverRuntimeSummary) map[string]any {
	fields := map[string]any{
		"export_id":                "",
		"active_iscsi_gateway_id":  "",
		"export_epoch":             uint64(0),
		"failover":                 sbsctlISCSIFailoverRow(failover),
		"operation_handle":         sbsctlISCSIOperationHandleRow(op),
		"iscsi_registry_available": true,
		"registry_status":          iscsiRegistryReadyStatus,
	}
	if failover != nil {
		fields["export_id"] = failover.GetExportId()
		fields["active_iscsi_gateway_id"] = failover.GetActiveIscsiGatewayId()
		fields["export_epoch"] = failover.GetExportEpoch()
	}
	applySBSCTLISCSIClusterRefFields(fields, cluster)
	applySBSCTLISCSIRegistryFields(fields, registryRevision, configGeneration)
	return fields
}

func sbsctlISCSIFailoverRow(failover *adminv1.ISCSIFailoverRuntimeSummary) map[string]any {
	if failover == nil {
		return nil
	}
	return map[string]any{
		"export_id":                        failover.GetExportId(),
		"active_iscsi_gateway_id":          failover.GetActiveIscsiGatewayId(),
		"standby_iscsi_gateway_ids":        append([]string{}, failover.GetStandbyIscsiGatewayIds()...),
		"previous_active_iscsi_gateway_id": failover.GetPreviousActiveIscsiGatewayId(),
		"export_lease_id":                  failover.GetExportLeaseId(),
		"export_epoch":                     failover.GetExportEpoch(),
		"state":                            failover.GetState(),
		"writer_policy":                    failover.GetWriterPolicy(),
		"ha_failover_mode":                 failover.GetHaFailoverMode(),
		"alua_mode":                        failover.GetAluaMode(),
		"alua_implicit_supported":          failover.GetAluaImplicitSupported(),
		"alua_explicit_supported":          failover.GetAluaExplicitSupported(),
		"active_alua_access_state":         failover.GetActiveAluaAccessState(),
		"standby_alua_access_state":        failover.GetStandbyAluaAccessState(),
		"failover_trigger":                 failover.GetFailoverTrigger(),
		"failover_completed":               failover.GetFailoverCompleted(),
		"stale_gateway_revoked_id":         failover.GetStaleGatewayRevokedId(),
		"stale_gateway_rejected":           failover.GetStaleGatewayRejected(),
		"standby_write_rejected":           failover.GetStandbyWriteRejected(),
		"last_write_gateway_id":            failover.GetLastWriteGatewayId(),
		"last_write_observed_epoch":        failover.GetLastWriteObservedEpoch(),
		"last_write_admitted":              failover.GetLastWriteAdmitted(),
		"last_write_rejection_reason":      failover.GetLastWriteRejectionReason(),
		"last_write_scsi_status":           failover.GetLastWriteScsiStatus(),
		"last_write_sense_key":             failover.GetLastWriteSenseKey(),
		"last_rejected_iscsi_gateway_id":   failover.GetLastRejectedIscsiGatewayId(),
	}
}

func sbsctlISCSIObservabilityCountersRow(counters *adminv1.ISCSIObservabilityCounters) map[string]any {
	if counters == nil {
		return map[string]any{}
	}
	return map[string]any{
		"session_count":                  counters.GetSessionCount(),
		"connected_sessions":             counters.GetConnectedSessions(),
		"active_sessions":                counters.GetActiveSessions(),
		"protocol_errors":                counters.GetProtocolErrors(),
		"backend_errors":                 counters.GetBackendErrors(),
		"auth_errors":                    counters.GetAuthErrors(),
		"fencing_errors":                 counters.GetFencingErrors(),
		"stale_rejects":                  counters.GetStaleRejects(),
		"standby_rejects":                counters.GetStandbyRejects(),
		"flush_count":                    counters.GetFlushCount(),
		"unmap_bytes":                    counters.GetUnmapBytes(),
		"bytes_read":                     counters.GetBytesRead(),
		"bytes_written":                  counters.GetBytesWritten(),
		"last_rejected_iscsi_gateway_id": counters.GetLastRejectedIscsiGatewayId(),
		"last_error_class":               counters.GetLastErrorClass(),
		"last_error":                     counters.GetLastError(),
	}
}

func sbsctlISCSISessionRows(sessions []*adminv1.ISCSISessionSummary) []map[string]any {
	rows := make([]map[string]any, 0, len(sessions))
	for _, session := range sessions {
		rows = append(rows, sbsctlISCSISessionRow(session))
	}
	return rows
}

func sbsctlISCSISessionRow(session *adminv1.ISCSISessionSummary) map[string]any {
	if session == nil {
		return nil
	}
	return map[string]any{
		"session_id":              session.GetSessionId(),
		"target_iqn":              session.GetTargetIqn(),
		"initiator_iqn":           session.GetInitiatorIqn(),
		"lun_id":                  session.GetLunId(),
		"lun_wwn":                 session.GetLunWwn(),
		"iscsi_gateway_id":        session.GetIscsiGatewayId(),
		"state":                   session.GetState(),
		"connected":               session.GetConnected(),
		"iscsi_erl":               session.GetIscsiErl(),
		"connection_count":        session.GetConnectionCount(),
		"header_digest":           session.GetHeaderDigest(),
		"data_digest":             session.GetDataDigest(),
		"writer_policy":           session.GetWriterPolicy(),
		"ha_failover_mode":        session.GetHaFailoverMode(),
		"active_iscsi_gateway_id": session.GetActiveIscsiGatewayId(),
		"export_epoch":            session.GetExportEpoch(),
		"read_write_allowed":      session.GetReadWriteAllowed(),
		"scsi_status":             session.GetScsiStatus(),
		"sense_key":               session.GetSenseKey(),
		"last_error_class":        session.GetLastErrorClass(),
		"last_error":              session.GetLastError(),
		"bytes_read":              session.GetBytesRead(),
		"bytes_written":           session.GetBytesWritten(),
		"flush_count":             session.GetFlushCount(),
		"unmap_bytes":             session.GetUnmapBytes(),
	}
}

func mergeISCSIStringList(single, csv string) []string {
	values := []string{single}
	values = append(values, splitISCSICSV(csv)...)
	seen := map[string]bool{}
	out := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" || seen[value] {
			continue
		}
		seen[value] = true
		out = append(out, value)
	}
	return out
}

func splitISCSICSV(raw string) []string {
	if strings.TrimSpace(raw) == "" {
		return nil
	}
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part != "" {
			out = append(out, part)
		}
	}
	return out
}

func parseISCSIAllowedLUNs(raw string, single uint64, singleSet bool) ([]uint64, error) {
	out := make([]uint64, 0, 1)
	if singleSet {
		out = append(out, single)
	}
	for _, part := range splitISCSICSV(raw) {
		value, err := strconv.ParseUint(part, 10, 64)
		if err != nil {
			return nil, err
		}
		out = append(out, value)
	}
	seen := map[uint64]bool{}
	deduped := out[:0]
	for _, value := range out {
		if seen[value] {
			continue
		}
		seen[value] = true
		deduped = append(deduped, value)
	}
	if len(deduped) == 0 {
		return nil, fmt.Errorf("at least one of --lun-id or --lun-ids is required")
	}
	return deduped, nil
}

func sbsctlISCSIVolumeSummaryRows(volumes []*adminv1.VolumeSummary) []map[string]any {
	rows := make([]map[string]any, 0, len(volumes))
	for _, volume := range volumes {
		rows = append(rows, sbsctlISCSIVolumeSummaryRow(volume))
	}
	return rows
}

func sbsctlISCSIVolumeSummaryRow(volume *adminv1.VolumeSummary) map[string]any {
	if volume == nil {
		return nil
	}
	return map[string]any{
		"volume_id":              volume.GetVolumeId(),
		"size_bytes":             volume.GetSizeBytes(),
		"block_size":             volume.GetBlockSize(),
		"health":                 sbsctlISCSIEnumLabel(volume.GetHealth().String(), "VOLUME_HEALTH_"),
		"volume_revision":        volume.GetVolumeRevision(),
		"repair_backlog":         volume.GetRepairBacklog(),
		"rebalance_backlog":      volume.GetRebalanceBacklog(),
		"drain_backlog":          volume.GetDrainBacklog(),
		"chunk_size_bytes":       volume.GetChunkSizeBytes(),
		"extent_page_bytes":      volume.GetExtentPageBytes(),
		"topology_mode":          volume.GetTopologyMode(),
		"redundancy_backend":     volume.GetRedundancyBackend(),
		"ec_profile_id":          volume.GetEcProfileId(),
		"weak_placement_allowed": volume.GetWeakPlacementAllowed(),
	}
}

func sbsctlISCSIMaxVolumeRevision(volumes []*adminv1.VolumeSummary) uint64 {
	var max uint64
	for _, volume := range volumes {
		if revision := volume.GetVolumeRevision(); revision > max {
			max = revision
		}
	}
	return max
}

func sbsctlISCSIEnumLabel(value, prefix string) string {
	value = strings.TrimPrefix(value, prefix)
	value = strings.TrimSpace(value)
	if value == "" {
		return "unspecified"
	}
	return strings.ToLower(value)
}

func iscsiUsage() {
	fmt.Fprintln(os.Stderr, "usage: sbsctl iscsi status|portal|target|lun|initiator|session|failover ...")
}

func iscsiStatusUsage() {
	fmt.Fprintln(os.Stderr, "usage: sbsctl iscsi status gateway|target ...")
}

func iscsiPortalUsage() {
	fmt.Fprintln(os.Stderr, "usage: sbsctl iscsi portal list|get|create|delete|enable|disable ...")
}

func iscsiTargetUsage() {
	fmt.Fprintln(os.Stderr, "usage: sbsctl iscsi target list|get|create|delete|enable|disable ...")
}

func iscsiLUNUsage() {
	fmt.Fprintln(os.Stderr, "usage: sbsctl iscsi lun list|get|export|unexport|set-mode ...")
}

func iscsiInitiatorUsage() {
	fmt.Fprintln(os.Stderr, "usage: sbsctl iscsi initiator list|get|allow|deny|set-auth ...")
}

func iscsiSessionUsage() {
	fmt.Fprintln(os.Stderr, "usage: sbsctl iscsi session list|get|disconnect ...")
}

func iscsiFailoverUsage() {
	fmt.Fprintln(os.Stderr, "usage: sbsctl iscsi failover status|promote|demote|standby|revoke-stale ...")
}
