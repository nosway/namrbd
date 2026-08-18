package main

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/nosway/namrbd/internal/adminclient"
	"github.com/nosway/namrbd/internal/sbsdataclient"
	adminv1 "github.com/nosway/namrbd/sbs/admin/v1"
	clustermeta "github.com/nosway/namrbd/sbs/cluster/metadata"
	"github.com/nosway/namrbd/sbs/cluster/payload"
	clusterreplication "github.com/nosway/namrbd/sbs/cluster/replication"
	sbsv1 "github.com/nosway/namrbd/sbs/v1"
	namrbdversion "github.com/nosway/namrbd/version"

	grpcmetadata "google.golang.org/grpc/metadata"
)

var (
	globalContextFile string
	globalContextName string
)

const (
	adminVolumeSummaryModeMetadataKey = "namrbd-volume-summary-mode"
	adminVolumeSummaryModeSpecOnly    = "spec-only"
)

func main() {
	if len(os.Args) >= 2 && (os.Args[1] == "--version" || os.Args[1] == "version") {
		fmt.Println(namrbdversion.BuildSummary())
		return
	}
	// Parse global flags before the command so scripts can call:
	//   sbsctl --context-file ... cluster init
	global := flag.NewFlagSet("sbsctl", flag.ContinueOnError)
	global.SetOutput(io.Discard)
	global.StringVar(&globalContextFile, "context-file", "", "context file path")
	global.StringVar(&globalContextName, "context", "", "context name/profile")
	_ = global.Parse(os.Args[1:])
	args := global.Args()

	if len(args) >= 1 && args[0] == "version" {
		fmt.Println(namrbdversion.BuildSummary())
		return
	}
	if len(args) < 1 {
		usage()
		os.Exit(2)
	}

	switch args[0] {
	case "cluster":
		runCluster(args[1:])
	case "node":
		runNode(args[1:])
	case "topology":
		runTopology(args[1:])
	case "store":
		runStore(args[1:])
	case "volume":
		runVolume(args[1:])
	case "snapshot":
		runSnapshot(args[1:])
	case "iscsi":
		runISCSI(args[1:])
	case "repair":
		runRepair(args[1:])
	case "rebalance":
		runRebalance(args[1:])
	case "maintenance":
		runMaintenance(args[1:])
	case "operations":
		runOperations(args[1:])
	case "testio":
		runTestIO(args[1:])
	default:
		if runEnterpriseTopLevel(args) {
			return
		}
		usage()
		os.Exit(2)
	}
}

func runCluster(args []string) {
	if len(args) < 1 {
		clusterUsage()
		os.Exit(2)
	}
	switch args[0] {
	case "status":
		runClusterStatus(args[1:])
	case "init":
		runClusterInit(args[1:])
	default:
		clusterUsage()
		os.Exit(2)
	}
}

func runNode(args []string) {
	if len(args) < 1 {
		nodeUsage()
		os.Exit(2)
	}
	switch args[0] {
	case "join":
		runNodeJoin(args[1:])
	case "update-topology":
		runNodeUpdateTopology(args[1:])
	case "status":
		runNodeStatus(args[1:])
	case "drain":
		if len(args) >= 2 && args[1] == "status" {
			runNodeDrainStatus(args[2:])
			return
		}
		runNodeDrain(args[1:])
	case "remove":
		runNodeRemove(args[1:])
	default:
		nodeUsage()
		os.Exit(2)
	}
}

func runStore(args []string) {
	if len(args) < 1 {
		storeUsage()
		os.Exit(2)
	}
	switch args[0] {
	case "status":
		runStoreStatus(args[1:])
	case "tuning":
		runStoreTuning(args[1:])
	default:
		storeUsage()
		os.Exit(2)
	}
}

func runTopology(args []string) {
	if len(args) < 1 {
		topologyUsage()
		os.Exit(2)
	}
	switch args[0] {
	case "zone":
		runTopologyZone(args[1:])
	default:
		topologyUsage()
		os.Exit(2)
	}
}

func runTopologyZone(args []string) {
	if len(args) < 1 {
		topologyUsage()
		os.Exit(2)
	}
	switch args[0] {
	case "create":
		runTopologyZoneCreate(args[1:])
	case "list":
		runTopologyZoneList(args[1:])
	case "get":
		runTopologyZoneGet(args[1:])
	case "update":
		runTopologyZoneUpdate(args[1:])
	case "delete":
		runTopologyZoneDelete(args[1:])
	default:
		topologyUsage()
		os.Exit(2)
	}
}

func runVolume(args []string) {
	if len(args) < 1 {
		volumeUsage()
		os.Exit(2)
	}
	switch args[0] {
	case "create":
		runVolumeCreate(args[1:])
	case "restore-from-snapshot":
		runVolumeRestoreFromSnapshot(args[1:])
	case "expand":
		runVolumeExpand(args[1:])
	case "delete":
		runVolumeDelete(args[1:])
	case "purge":
		runVolumePurge(args[1:])
	case "status":
		runVolumeStatus(args[1:])
	case "health":
		runVolumeHealth(args[1:])
	case "replica-targets":
		runVolumeReplicaTargets(args[1:])
	case "allocation-page":
		runVolumeAllocationPage(args[1:])
	case "placement":
		runVolumePlacement(args[1:])
	case "transitions":
		runVolumeTransitions(args[1:])
	case "list":
		runVolumeList(args[1:])
	default:
		volumeUsage()
		os.Exit(2)
	}
}

func runOperations(args []string) {
	if len(args) < 1 {
		operationsUsage()
		os.Exit(2)
	}
	switch args[0] {
	case "list":
		runOperationList(args[1:])
	case "show":
		runOperationShow(args[1:])
	default:
		operationsUsage()
		os.Exit(2)
	}
}

func runRepair(args []string) {
	if len(args) < 1 {
		repairUsage()
		os.Exit(2)
	}
	switch args[0] {
	case "list":
		runRepairList(args[1:])
	case "show":
		runRepairShow(args[1:])
	default:
		repairUsage()
		os.Exit(2)
	}
}

func runRebalance(args []string) {
	if len(args) < 1 {
		rebalanceUsage()
		os.Exit(2)
	}
	switch args[0] {
	case "list":
		runRebalanceList(args[1:])
	default:
		rebalanceUsage()
		os.Exit(2)
	}
}

func runMaintenance(args []string) {
	if len(args) < 1 {
		maintenanceUsage()
		os.Exit(2)
	}
	switch args[0] {
	case "throttle":
		runMaintenanceThrottle(args[1:])
	case "pause":
		runMaintenancePause(args[1:])
	case "resume":
		runMaintenanceResume(args[1:])
	case "payload-gc":
		runMaintenancePayloadGC(args[1:])
	default:
		maintenanceUsage()
		os.Exit(2)
	}
}

func runTestIO(args []string) {
	if len(args) < 1 {
		testIOUsage()
		os.Exit(2)
	}
	switch args[0] {
	case "open":
		runTestIOOpen(args[1:])
	case "read":
		runTestIORead(args[1:])
	case "write":
		runTestIOWrite(args[1:])
	case "flush":
		runTestIOFlush(args[1:])
	default:
		testIOUsage()
		os.Exit(2)
	}
}

func runClusterStatus(args []string) {
	defaults := mustResolveCLIDefaults(args)
	fs := flag.NewFlagSet("cluster status", flag.ExitOnError)
	registerContextFlags(fs, defaults)
	adminEndpoint := fs.String("admin-endpoint", defaults.adminEndpoint(), "cluster-wide sbs-admin gRPC endpoint")
	clusterID := fs.String("cluster-id", defaults.fieldValue("cluster_id", "NAMRBD_CLUSTER_ID"), "cluster id")
	sbsClusterID := fs.String("sbs-cluster-id", defaults.fieldValue("sbs_cluster_id", "SBS_CLUSTER_ID", "NAMRBD_SBS_CLUSTER_ID"), "sbs cluster id")
	output := fs.String("output", defaults.fieldValue("output", "SBS_OUTPUT", "NAMRBD_OUTPUT"), "output format: table|json")
	timeout := fs.Duration("timeout", defaults.timeout(10*time.Second), "request timeout")
	fs.Parse(args)
	if *output == "" {
		*output = "table"
	}
	printResolvedSettings(fs,
		defaults.adminEndpointSetting(),
		defaults.fieldSetting("cluster_id", "cluster-id", "", "NAMRBD_CLUSTER_ID"),
		defaults.fieldSetting("sbs_cluster_id", "sbs-cluster-id", "", "SBS_CLUSTER_ID", "NAMRBD_SBS_CLUSTER_ID"),
		defaults.fieldSetting("output", "output", "table", "SBS_OUTPUT", "NAMRBD_OUTPUT"),
		defaults.timeoutSetting(10*time.Second),
	)

	client, ctx, cancel := dialAdmin(*adminEndpoint, *timeout)
	defer cancel()
	defer client.Close()

	resp, err := client.Admin.GetClusterStatus(ctx, &adminv1.GetClusterStatusRequest{
		Cluster: clusterRef(*clusterID, *sbsClusterID),
	})
	if err != nil {
		fatalf("cluster status failed: %v", err)
	}
	nodesResp, err := client.Admin.ListNodes(ctx, &adminv1.ListNodesRequest{
		Cluster: clusterRef(*clusterID, *sbsClusterID),
	})
	if err != nil {
		fatalf("list nodes for cluster status failed: %v", err)
	}
	summary := summarizeNodeHealth(nodesResp.GetNodes())

	switch *output {
	case "json":
		writeJSON(clusterStatusJSON(resp, nodesResp.GetNodes(), summary))
	default:
		fmt.Printf("cluster_id: %s\n", resp.GetCluster().GetClusterId())
		fmt.Printf("sbs_cluster_id: %s\n", resp.GetCluster().GetSbsClusterId())
		fmt.Printf("leader_node_id: %s\n", resp.GetLeaderNodeId())
		fmt.Printf("quorum_health: %s\n", resp.GetQuorumHealth().String())
		fmt.Printf("active_nodes: %d\n", resp.GetActiveNodes())
		fmt.Printf("active_healthy_nodes: %d\n", summary.ActiveHealthyNodes)
		fmt.Printf("active_suspect_nodes: %d\n", summary.ActiveSuspectNodes)
		fmt.Printf("active_down_nodes: %d\n", summary.ActiveDownNodes)
		fmt.Printf("draining_nodes: %d\n", resp.GetDrainingNodes())
		fmt.Printf("repair_backlog: %d\n", resp.GetRepairBacklog())
		fmt.Printf("rebalance_backlog: %d\n", resp.GetRebalanceBacklog())
		fmt.Printf("drain_backlog: %d\n", resp.GetDrainBacklog())
		fmt.Printf("degraded_extents: %d\n", resp.GetDegradedExtents())
		if resp.GetMaintenanceCooldownVolumes() > 0 {
			fmt.Printf("maintenance_cooldown_volumes: %d\n", resp.GetMaintenanceCooldownVolumes())
			fmt.Printf("maintenance_cooldown_max_remaining_seconds: %d\n", resp.GetMaintenanceCooldownMaxRemainingSeconds())
		}
		fmt.Printf("nodes:\n")
		for _, n := range nodesResp.GetNodes() {
			fmt.Printf("  %s: %s\n", n.GetNodeId(), compactNodeHealthName(n.GetHealth()))
		}
	}
}

func runClusterInit(args []string) {
	defaults := mustResolveCLIDefaults(args)
	fs := flag.NewFlagSet("cluster init", flag.ExitOnError)
	registerContextFlags(fs, defaults)
	adminEndpoint := fs.String("admin-endpoint", defaults.adminEndpoint(), "cluster-wide sbs-admin gRPC endpoint")
	clusterID := fs.String("cluster-id", defaults.fieldValue("cluster_id", "NAMRBD_CLUSTER_ID"), "cluster id")
	sbsClusterID := fs.String("sbs-cluster-id", defaults.fieldValue("sbs_cluster_id", "SBS_CLUSTER_ID", "NAMRBD_SBS_CLUSTER_ID"), "sbs cluster id")
	actor := fs.String("actor", getenvOrDefault("USER", "unknown"), "actor")
	reason := fs.String("reason", "bootstrap", "reason")
	timeout := fs.Duration("timeout", defaults.timeout(10*time.Second), "request timeout")
	fs.Parse(args)
	printResolvedSettings(fs,
		defaults.adminEndpointSetting(),
		defaults.fieldSetting("cluster_id", "cluster-id", "", "NAMRBD_CLUSTER_ID"),
		defaults.fieldSetting("sbs_cluster_id", "sbs-cluster-id", "", "SBS_CLUSTER_ID", "NAMRBD_SBS_CLUSTER_ID"),
		defaults.timeoutSetting(10*time.Second),
	)

	client, ctx, cancel := dialAdmin(*adminEndpoint, *timeout)
	defer cancel()
	defer client.Close()
	resp, err := client.Admin.ClusterInit(ctx, &adminv1.ClusterInitRequest{
		Cluster: clusterRef(*clusterID, *sbsClusterID),
		Meta:    &adminv1.RequestMeta{Actor: *actor, Reason: *reason},
	})
	if err != nil {
		fatalf("cluster init failed: %v", err)
	}
	writeJSON(resp)
}

func runNodeStatus(args []string) {
	defaults := mustResolveCLIDefaults(args)
	fs := flag.NewFlagSet("node status", flag.ExitOnError)
	registerContextFlags(fs, defaults)
	adminEndpoint := fs.String("admin-endpoint", defaults.adminEndpoint(), "cluster-wide sbs-admin gRPC endpoint")
	clusterID := fs.String("cluster-id", defaults.fieldValue("cluster_id", "NAMRBD_CLUSTER_ID"), "cluster id")
	sbsClusterID := fs.String("sbs-cluster-id", defaults.fieldValue("sbs_cluster_id", "SBS_CLUSTER_ID", "NAMRBD_SBS_CLUSTER_ID"), "sbs cluster id")
	nodeID := fs.String("node-id", defaults.fieldValue("node_id", "SBS_NODE_ID", "NAMRBD_NODE_ID"), "node id")
	output := fs.String("output", defaults.fieldValue("output", "SBS_OUTPUT", "NAMRBD_OUTPUT"), "output format: table|json")
	timeout := fs.Duration("timeout", defaults.timeout(10*time.Second), "request timeout")
	fs.Parse(args)
	if *output == "" {
		*output = "table"
	}
	printResolvedSettings(fs,
		defaults.adminEndpointSetting(),
		defaults.fieldSetting("cluster_id", "cluster-id", "", "NAMRBD_CLUSTER_ID"),
		defaults.fieldSetting("sbs_cluster_id", "sbs-cluster-id", "", "SBS_CLUSTER_ID", "NAMRBD_SBS_CLUSTER_ID"),
		defaults.fieldSetting("node_id", "node-id", "", "SBS_NODE_ID", "NAMRBD_NODE_ID"),
		defaults.fieldSetting("output", "output", "table", "SBS_OUTPUT", "NAMRBD_OUTPUT"),
		defaults.timeoutSetting(10*time.Second),
	)
	if *nodeID == "" {
		fatalf("--node-id is required")
	}

	client, ctx, cancel := dialAdmin(*adminEndpoint, *timeout)
	defer cancel()
	defer client.Close()
	resp, err := client.Admin.GetNode(ctx, &adminv1.GetNodeRequest{
		Cluster: clusterRef(*clusterID, *sbsClusterID),
		NodeId:  *nodeID,
	})
	if err != nil {
		fatalf("node status failed: %v", err)
	}
	switch *output {
	case "json":
		writeJSON(nodeStatusJSON(resp))
	default:
		n := resp.GetNode()
		if n == nil {
			fatalf("empty node response")
		}
		fmt.Printf("node_id: %s\n", n.GetNodeId())
		fmt.Printf("lifecycle: %s\n", n.GetLifecycle().String())
		fmt.Printf("health: %s\n", n.GetHealth().String())
		fmt.Printf("grpc_endpoint: %s\n", n.GetGrpcEndpoint())
		fmt.Printf("admin_http_endpoint: %s\n", n.GetAdminHttpEndpoint())
		fmt.Printf("zone: %s\n", n.GetZone())
		if n.GetLastHeartbeatTime() != nil {
			fmt.Printf("last_heartbeat: %s\n", n.GetLastHeartbeatTime().AsTime().UTC().Format(time.RFC3339))
		}
		if n.GetLastProbeTime() != nil {
			fmt.Printf("last_probe: %s\n", n.GetLastProbeTime().AsTime().UTC().Format(time.RFC3339))
		}
		if n.GetRecoveryEligibleTime() != nil {
			fmt.Printf("recovery_eligible: %s\n", n.GetRecoveryEligibleTime().AsTime().UTC().Format(time.RFC3339))
		}
		if n.GetLastProbeError() != "" {
			fmt.Printf("last_probe_error: %s\n", n.GetLastProbeError())
		}
		if n.GetHealthReason() != "" {
			fmt.Printf("health_reason: %s\n", n.GetHealthReason())
		}
		if n.GetHealthUpdatedBy() != "" {
			fmt.Printf("health_updated_by: %s\n", n.GetHealthUpdatedBy())
		}
		fmt.Printf("consecutive_probe_failures: %d\n", n.GetConsecutiveProbeFailures())
		fmt.Printf("consecutive_probe_successes: %d\n", n.GetConsecutiveProbeSuccesses())
		for k, v := range n.GetLabels() {
			fmt.Printf("label_%s: %s\n", k, v)
		}
	}
}

func nodeStatusJSON(resp *adminv1.GetNodeResponse) map[string]any {
	clusterMap := map[string]any{}
	if resp.GetCluster() != nil {
		clusterMap["cluster_id"] = resp.GetCluster().GetClusterId()
		clusterMap["sbs_cluster_id"] = resp.GetCluster().GetSbsClusterId()
	}
	nodeMap := map[string]any{}
	if n := resp.GetNode(); n != nil {
		nodeMap["node_id"] = n.GetNodeId()
		nodeMap["lifecycle"] = int32(n.GetLifecycle())
		nodeMap["lifecycle_name"] = n.GetLifecycle().String()
		nodeMap["health"] = int32(n.GetHealth())
		nodeMap["health_name"] = n.GetHealth().String()
		nodeMap["grpc_endpoint"] = n.GetGrpcEndpoint()
		nodeMap["admin_http_endpoint"] = n.GetAdminHttpEndpoint()
		nodeMap["zone"] = n.GetZone()
		if ts := n.GetLastHeartbeatTime(); ts != nil {
			nodeMap["last_heartbeat_time"] = ts
		}
		if ts := n.GetLastProbeTime(); ts != nil {
			nodeMap["last_probe_time"] = ts
		}
		if ts := n.GetRecoveryEligibleTime(); ts != nil {
			nodeMap["recovery_eligible_time"] = ts
		}
		if err := n.GetLastProbeError(); err != "" {
			nodeMap["last_probe_error"] = err
		}
		if reason := n.GetHealthReason(); reason != "" {
			nodeMap["health_reason"] = reason
		}
		if updatedBy := n.GetHealthUpdatedBy(); updatedBy != "" {
			nodeMap["health_updated_by"] = updatedBy
		}
		nodeMap["consecutive_probe_failures"] = n.GetConsecutiveProbeFailures()
		nodeMap["consecutive_probe_successes"] = n.GetConsecutiveProbeSuccesses()
		if labels := n.GetLabels(); len(labels) > 0 {
			nodeMap["labels"] = labels
		}
	}
	return map[string]any{
		"cluster": clusterMap,
		"node":    nodeMap,
	}
}

type clusterNodeHealthSummary struct {
	ActiveHealthyNodes uint32
	ActiveSuspectNodes uint32
	ActiveDownNodes    uint32
	DrainingNodes      uint32
	InactiveNodes      uint32
	UnhealthyNodes     []string
}

func clusterStatusJSON(resp *adminv1.GetClusterStatusResponse, nodes []*adminv1.NodeSummary, summary clusterNodeHealthSummary) map[string]any {
	clusterMap := map[string]any{}
	if resp.GetCluster() != nil {
		clusterMap["cluster_id"] = resp.GetCluster().GetClusterId()
		clusterMap["sbs_cluster_id"] = resp.GetCluster().GetSbsClusterId()
	}
	nodeMaps := make([]map[string]any, 0, len(nodes))
	for _, n := range nodes {
		nodeMaps = append(nodeMaps, clusterNodeSummaryJSON(n))
	}
	return map[string]any{
		"cluster":                      clusterMap,
		"leader_node_id":               resp.GetLeaderNodeId(),
		"quorum_health":                int32(resp.GetQuorumHealth()),
		"quorum_health_name":           resp.GetQuorumHealth().String(),
		"active_nodes":                 resp.GetActiveNodes(),
		"draining_nodes":               resp.GetDrainingNodes(),
		"degraded_extents":             resp.GetDegradedExtents(),
		"repair_backlog":               resp.GetRepairBacklog(),
		"rebalance_backlog":            resp.GetRebalanceBacklog(),
		"drain_backlog":                resp.GetDrainBacklog(),
		"maintenance_cooldown_volumes": resp.GetMaintenanceCooldownVolumes(),
		"maintenance_cooldown_max_remaining_seconds": resp.GetMaintenanceCooldownMaxRemainingSeconds(),
		"node_health_summary": map[string]any{
			"active_healthy_nodes": summary.ActiveHealthyNodes,
			"active_suspect_nodes": summary.ActiveSuspectNodes,
			"active_down_nodes":    summary.ActiveDownNodes,
			"draining_nodes":       summary.DrainingNodes,
			"inactive_nodes":       summary.InactiveNodes,
			"unhealthy_nodes":      summary.UnhealthyNodes,
		},
		"nodes": nodeMaps,
	}
}

func clusterNodeSummaryJSON(n *adminv1.NodeSummary) map[string]any {
	nodeMap := map[string]any{
		"node_id":        n.GetNodeId(),
		"lifecycle":      int32(n.GetLifecycle()),
		"lifecycle_name": n.GetLifecycle().String(),
		"health":         int32(n.GetHealth()),
		"health_name":    compactNodeHealthName(n.GetHealth()),
		"zone":           n.GetZone(),
	}
	return nodeMap
}

func replicaTargetsViewJSON(resp *adminv1.GetReplicaTargetsViewResponse) map[string]any {
	clusterMap := map[string]any{}
	if resp.GetCluster() != nil {
		clusterMap["cluster_id"] = resp.GetCluster().GetClusterId()
		clusterMap["sbs_cluster_id"] = resp.GetCluster().GetSbsClusterId()
	}
	targets := make([]map[string]any, 0, len(resp.GetTargets()))
	for _, t := range resp.GetTargets() {
		entry := map[string]any{
			"target_id":           t.GetTargetId(),
			"usable":              t.GetUsable(),
			"priority":            t.GetPriority(),
			"reason_code":         int32(t.GetReasonCode()),
			"reason_code_name":    t.GetReasonCode().String(),
			"admin_http_endpoint": t.GetAdminHttpEndpoint(),
		}
		if ep := t.GetEndpoint(); ep != nil {
			entry["endpoint"] = map[string]any{
				"address":     ep.GetAddress(),
				"port":        ep.GetPort(),
				"use_tls":     ep.GetUseTls(),
				"server_name": ep.GetServerName(),
			}
		}
		targets = append(targets, entry)
	}
	out := map[string]any{
		"cluster":           clusterMap,
		"volume_id":         resp.GetVolumeId(),
		"revision":          resp.GetRevision(),
		"cache_ttl_seconds": resp.GetCacheTtlSeconds(),
		"targets":           targets,
	}
	if ts := resp.GetGeneratedAt(); ts != nil {
		out["generated_at"] = ts
	}
	return out
}

func compactNodeHealthName(health adminv1.NodeHealth) string {
	switch health {
	case adminv1.NodeHealth_NODE_HEALTH_HEALTHY:
		return "healthy"
	case adminv1.NodeHealth_NODE_HEALTH_SUSPECT:
		return "suspect"
	case adminv1.NodeHealth_NODE_HEALTH_DOWN:
		return "down"
	default:
		return strings.ToLower(strings.TrimPrefix(health.String(), "NODE_HEALTH_"))
	}
}

func compactReplicaTargetReason(reason adminv1.ReplicaTargetReasonCode) string {
	return strings.ToLower(strings.TrimPrefix(reason.String(), "REPLICA_TARGET_REASON_CODE_"))
}

func summarizeNodeHealth(nodes []*adminv1.NodeSummary) clusterNodeHealthSummary {
	summary := clusterNodeHealthSummary{}
	for _, n := range nodes {
		switch n.GetLifecycle() {
		case adminv1.NodeLifecycle_NODE_LIFECYCLE_ACTIVE:
			switch n.GetHealth() {
			case adminv1.NodeHealth_NODE_HEALTH_HEALTHY:
				summary.ActiveHealthyNodes++
			case adminv1.NodeHealth_NODE_HEALTH_SUSPECT:
				summary.ActiveSuspectNodes++
				summary.UnhealthyNodes = append(summary.UnhealthyNodes, n.GetNodeId())
			default:
				summary.ActiveDownNodes++
				summary.UnhealthyNodes = append(summary.UnhealthyNodes, n.GetNodeId())
			}
		case adminv1.NodeLifecycle_NODE_LIFECYCLE_DRAINING:
			summary.DrainingNodes++
			if n.GetHealth() != adminv1.NodeHealth_NODE_HEALTH_HEALTHY {
				summary.UnhealthyNodes = append(summary.UnhealthyNodes, n.GetNodeId())
			}
		default:
			summary.InactiveNodes++
			if n.GetHealth() != adminv1.NodeHealth_NODE_HEALTH_HEALTHY {
				summary.UnhealthyNodes = append(summary.UnhealthyNodes, n.GetNodeId())
			}
		}
	}
	sort.Strings(summary.UnhealthyNodes)
	return summary
}

func runStoreStatus(args []string) {
	defaults := mustResolveCLIDefaults(args)
	fs := flag.NewFlagSet("store status", flag.ExitOnError)
	registerContextFlags(fs, defaults)
	adminHTTP := fs.String("admin-http-endpoint", defaults.fieldValue("sbs_node_admin_http", "SBS_NODE_ADMIN_HTTP", "NAMRBD_SBS_ADMIN_ADDR"), "node-local admin/debug HTTP endpoint")
	output := fs.String("output", defaults.fieldValue("output", "SBS_OUTPUT", "NAMRBD_OUTPUT"), "output format: table|json")
	timeout := fs.Duration("timeout", defaults.timeout(10*time.Second), "request timeout")
	fs.Parse(args)
	if *output == "" {
		*output = "table"
	}
	printResolvedSettings(fs,
		defaults.fieldSetting("sbs_node_admin_http", "admin-http-endpoint", "", "SBS_NODE_ADMIN_HTTP", "NAMRBD_SBS_ADMIN_ADDR"),
		defaults.fieldSetting("output", "output", "table", "SBS_OUTPUT", "NAMRBD_OUTPUT"),
		defaults.timeoutSetting(10*time.Second),
	)
	if strings.TrimSpace(*adminHTTP) == "" {
		fatalf("--admin-http-endpoint is required")
	}

	summary, err := runStoreStatusRemote(*adminHTTP, *timeout)
	if err != nil {
		fatalf("store status failed: %v", err)
	}

	switch strings.ToLower(strings.TrimSpace(*output)) {
	case "json":
		writeJSON(summary)
	case "", "table":
		fmt.Printf("process_state: %s\n", transitionAnyString(summary["process_state"]))
		fmt.Printf("path: %s\n", transitionAnyString(summary["path"]))
		fmt.Printf("build_version: %s\n", transitionAnyString(summary["build_version"]))
		fmt.Printf("open_sessions: %d\n", transitionAnyUint64(summary["open_sessions"]))
		fmt.Printf("volumes: %d\n", transitionAnyUint64(summary["volumes"]))
		stores, _ := summary["stores"].([]any)
		if len(stores) == 0 {
			fmt.Println("stores: none")
			return
		}
		fmt.Println("stores:")
		for _, raw := range stores {
			store, _ := raw.(map[string]any)
			if store == nil {
				continue
			}
			fmt.Printf("  store_id=%s state=%s path=%s shards=%d allocation_weight=%d capacity_bytes=%d available_bytes=%d pebble_disk_usage_bytes=%d compaction_pending_bytes=%d compaction_in_progress_bytes=%d\n",
				transitionAnyString(store["id"]),
				transitionAnyString(store["state"]),
				transitionAnyString(store["path"]),
				transitionAnyUint64(store["shards"]),
				storeAllocationWeightForOutput(store),
				transitionAnyUint64(store["capacity_bytes"]),
				transitionAnyUint64(store["available_bytes"]),
				transitionAnyUint64(store["pebble_disk_usage_bytes"]),
				transitionAnyUint64(store["compaction_pending_bytes"]),
				transitionAnyUint64(store["compaction_in_progress_bytes"]),
			)
		}
	default:
		fatalf("unsupported output format %q", *output)
	}
}

type storeTuningFlag []*adminv1.StoreTuningSummary

type labelFlag map[string]string

func (f *labelFlag) String() string {
	if f == nil || len(*f) == 0 {
		return ""
	}
	keys := make([]string, 0, len(*f))
	for k := range *f {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		parts = append(parts, k+"="+(*f)[k])
	}
	return strings.Join(parts, ",")
}

func (f *labelFlag) Set(raw string) error {
	key, value, ok := strings.Cut(strings.TrimSpace(raw), "=")
	if !ok || strings.TrimSpace(key) == "" {
		return fmt.Errorf("label must be k=v")
	}
	if *f == nil {
		*f = map[string]string{}
	}
	(*f)[strings.TrimSpace(key)] = strings.TrimSpace(value)
	return nil
}

func (f *storeTuningFlag) String() string {
	if f == nil || len(*f) == 0 {
		return ""
	}
	parts := make([]string, 0, len(*f))
	for _, spec := range *f {
		parts = append(parts, fmt.Sprintf("store_id=%s,allocation_weight=%d", spec.GetStoreId(), spec.GetWeight()))
	}
	return strings.Join(parts, ";")
}

func (f *storeTuningFlag) Set(raw string) error {
	spec, err := parseStoreTuningSpec(raw)
	if err != nil {
		return err
	}
	*f = append(*f, spec)
	return nil
}

func parseStoreTuningSpec(raw string) (*adminv1.StoreTuningSummary, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, fmt.Errorf("store tuning spec is empty")
	}
	fields := strings.Split(raw, ",")
	spec := &adminv1.StoreTuningSummary{}
	for _, field := range fields {
		field = strings.TrimSpace(field)
		if field == "" {
			continue
		}
		key, value, ok := strings.Cut(field, "=")
		if !ok {
			return nil, fmt.Errorf("invalid store tuning option %q", field)
		}
		key = strings.TrimSpace(key)
		value = strings.TrimSpace(value)
		switch key {
		case "store_id", "id":
			spec.StoreId = value
		case "weight", "allocation_weight":
			var parsed int
			if _, err := fmt.Sscanf(value, "%d", &parsed); err != nil {
				return nil, fmt.Errorf("invalid weight %q", value)
			}
			spec.Weight = int32(parsed)
		default:
			return nil, fmt.Errorf("unknown store tuning option %q", key)
		}
	}
	if strings.TrimSpace(spec.GetStoreId()) == "" {
		return nil, fmt.Errorf("store_id is required")
	}
	if spec.GetWeight() < 0 {
		return nil, fmt.Errorf("weight must be zero or greater")
	}
	return spec, nil
}

func runStoreTuning(args []string) {
	defaults := mustResolveCLIDefaults(args)
	fs := flag.NewFlagSet("store tuning", flag.ExitOnError)
	registerContextFlags(fs, defaults)
	adminEndpoint := fs.String("admin-endpoint", defaults.adminEndpoint(), "cluster-wide sbs-admin gRPC endpoint")
	clusterID := fs.String("cluster-id", defaults.fieldValue("cluster_id", "NAMRBD_CLUSTER_ID"), "cluster id")
	sbsClusterID := fs.String("sbs-cluster-id", defaults.fieldValue("sbs_cluster_id", "SBS_CLUSTER_ID", "NAMRBD_SBS_CLUSTER_ID"), "sbs cluster id")
	nodeID := fs.String("node-id", defaults.fieldValue("node_id", "SBS_NODE_ID", "NAMRBD_NODE_ID"), "node id")
	actor := fs.String("actor", getenvOrDefault("USER", "unknown"), "actor")
	reason := fs.String("reason", "store-tuning", "reason")
	output := fs.String("output", defaults.fieldValue("output", "SBS_OUTPUT", "NAMRBD_OUTPUT"), "output format: table|json")
	timeout := fs.Duration("timeout", defaults.timeout(10*time.Second), "request timeout")
	var tuning storeTuningFlag
	fs.Var(&tuning, "store-tuning", "store tuning spec store_id=<id>,allocation_weight=<n> (weight=<n> is accepted as compatibility alias; repeatable)")
	fs.Parse(args)
	if *output == "" {
		*output = "table"
	}
	printResolvedSettings(fs,
		defaults.adminEndpointSetting(),
		defaults.fieldSetting("cluster_id", "cluster-id", "", "NAMRBD_CLUSTER_ID"),
		defaults.fieldSetting("sbs_cluster_id", "sbs-cluster-id", "", "SBS_CLUSTER_ID", "NAMRBD_SBS_CLUSTER_ID"),
		defaults.fieldSetting("node_id", "node-id", "", "SBS_NODE_ID", "NAMRBD_NODE_ID"),
		defaults.fieldSetting("output", "output", "table", "SBS_OUTPUT", "NAMRBD_OUTPUT"),
		defaults.timeoutSetting(10*time.Second),
	)
	if *nodeID == "" {
		fatalf("--node-id is required")
	}
	if len(tuning) == 0 {
		fatalf("at least one --store-tuning is required")
	}

	client, ctx, cancel := dialAdmin(*adminEndpoint, *timeout)
	defer cancel()
	defer client.Close()
	resp, err := client.Admin.UpdateNodeStoreTuning(ctx, &adminv1.UpdateNodeStoreTuningRequest{
		Cluster: clusterRef(*clusterID, *sbsClusterID),
		Meta:    &adminv1.RequestMeta{Actor: *actor, Reason: *reason},
		NodeId:  *nodeID,
		Stores:  []*adminv1.StoreTuningSummary(tuning),
	})
	if err != nil {
		fatalf("store tuning failed: %v", err)
	}
	switch strings.ToLower(strings.TrimSpace(*output)) {
	case "json":
		writeJSON(resp)
	default:
		fmt.Printf("cluster_id: %s\n", resp.GetCluster().GetClusterId())
		fmt.Printf("sbs_cluster_id: %s\n", resp.GetCluster().GetSbsClusterId())
		if op := resp.GetOperation(); op != nil {
			fmt.Printf("accepted: %t\n", op.GetAccepted())
			fmt.Printf("message: %s\n", op.GetMessage())
		}
		fmt.Printf("node_id: %s\n", *nodeID)
		fmt.Printf("stores:\n")
		for _, spec := range tuning {
			fmt.Printf("  store_id=%s allocation_weight=%d\n", spec.GetStoreId(), spec.GetWeight())
		}
	}
}

func runTopologyZoneCreate(args []string) {
	defaults := mustResolveCLIDefaults(args)
	fs := flag.NewFlagSet("topology zone create", flag.ExitOnError)
	registerContextFlags(fs, defaults)
	adminEndpoint := fs.String("admin-endpoint", defaults.adminEndpoint(), "cluster-wide sbs-admin gRPC endpoint")
	clusterID := fs.String("cluster-id", defaults.fieldValue("cluster_id", "NAMRBD_CLUSTER_ID"), "cluster id")
	sbsClusterID := fs.String("sbs-cluster-id", defaults.fieldValue("sbs_cluster_id", "SBS_CLUSTER_ID", "NAMRBD_SBS_CLUSTER_ID"), "sbs cluster id")
	zoneID := fs.String("zone", "", "zone id")
	displayName := fs.String("display-name", "", "display name")
	actor := fs.String("actor", getenvOrDefault("USER", "unknown"), "actor")
	reason := fs.String("reason", "topology-zone-create", "reason")
	timeout := fs.Duration("timeout", defaults.timeout(10*time.Second), "request timeout")
	var labels labelFlag
	fs.Var(&labels, "label", "label k=v (repeatable)")
	fs.Parse(args)
	printResolvedSettings(fs,
		defaults.adminEndpointSetting(),
		defaults.fieldSetting("cluster_id", "cluster-id", "", "NAMRBD_CLUSTER_ID"),
		defaults.fieldSetting("sbs_cluster_id", "sbs-cluster-id", "", "SBS_CLUSTER_ID", "NAMRBD_SBS_CLUSTER_ID"),
		defaults.timeoutSetting(10*time.Second),
	)
	if strings.TrimSpace(*zoneID) == "" {
		fatalf("--zone is required")
	}
	client, ctx, cancel := dialAdmin(*adminEndpoint, *timeout)
	defer cancel()
	defer client.Close()
	resp, err := client.Admin.CreateTopologyZone(ctx, &adminv1.CreateTopologyZoneRequest{
		Cluster:     clusterRef(*clusterID, *sbsClusterID),
		Meta:        &adminv1.RequestMeta{Actor: *actor, Reason: *reason},
		ZoneId:      strings.TrimSpace(*zoneID),
		DisplayName: strings.TrimSpace(*displayName),
		Labels:      map[string]string(labels),
	})
	if err != nil {
		fatalf("topology zone create failed: %v", err)
	}
	writeJSON(resp)
}

func runTopologyZoneList(args []string) {
	defaults := mustResolveCLIDefaults(args)
	fs := flag.NewFlagSet("topology zone list", flag.ExitOnError)
	registerContextFlags(fs, defaults)
	adminEndpoint := fs.String("admin-endpoint", defaults.adminEndpoint(), "cluster-wide sbs-admin gRPC endpoint")
	clusterID := fs.String("cluster-id", defaults.fieldValue("cluster_id", "NAMRBD_CLUSTER_ID"), "cluster id")
	sbsClusterID := fs.String("sbs-cluster-id", defaults.fieldValue("sbs_cluster_id", "SBS_CLUSTER_ID", "NAMRBD_SBS_CLUSTER_ID"), "sbs cluster id")
	output := fs.String("output", defaults.fieldValue("output", "SBS_OUTPUT", "NAMRBD_OUTPUT"), "output format: table|json")
	timeout := fs.Duration("timeout", defaults.timeout(10*time.Second), "request timeout")
	fs.Parse(args)
	if *output == "" {
		*output = "table"
	}
	printResolvedSettings(fs,
		defaults.adminEndpointSetting(),
		defaults.fieldSetting("cluster_id", "cluster-id", "", "NAMRBD_CLUSTER_ID"),
		defaults.fieldSetting("sbs_cluster_id", "sbs-cluster-id", "", "SBS_CLUSTER_ID", "NAMRBD_SBS_CLUSTER_ID"),
		defaults.fieldSetting("output", "output", "table", "SBS_OUTPUT", "NAMRBD_OUTPUT"),
		defaults.timeoutSetting(10*time.Second),
	)
	client, ctx, cancel := dialAdmin(*adminEndpoint, *timeout)
	defer cancel()
	defer client.Close()
	resp, err := client.Admin.ListTopologyZones(ctx, &adminv1.ListTopologyZonesRequest{Cluster: clusterRef(*clusterID, *sbsClusterID)})
	if err != nil {
		fatalf("topology zone list failed: %v", err)
	}
	switch strings.ToLower(strings.TrimSpace(*output)) {
	case "json":
		writeJSON(resp)
	default:
		if len(resp.GetZones()) == 0 {
			fmt.Println("no topology zones")
			return
		}
		for _, zone := range resp.GetZones() {
			fmt.Printf("zone: %s lifecycle: %s display_name: %s labels: %d\n", zone.GetZoneId(), topologyZoneLifecycleName(zone.GetLifecycle()), zone.GetDisplayName(), len(zone.GetLabels()))
		}
	}
}

func runTopologyZoneGet(args []string) {
	defaults := mustResolveCLIDefaults(args)
	fs := flag.NewFlagSet("topology zone get", flag.ExitOnError)
	registerContextFlags(fs, defaults)
	adminEndpoint := fs.String("admin-endpoint", defaults.adminEndpoint(), "cluster-wide sbs-admin gRPC endpoint")
	clusterID := fs.String("cluster-id", defaults.fieldValue("cluster_id", "NAMRBD_CLUSTER_ID"), "cluster id")
	sbsClusterID := fs.String("sbs-cluster-id", defaults.fieldValue("sbs_cluster_id", "SBS_CLUSTER_ID", "NAMRBD_SBS_CLUSTER_ID"), "sbs cluster id")
	zoneID := fs.String("zone", "", "zone id")
	output := fs.String("output", defaults.fieldValue("output", "SBS_OUTPUT", "NAMRBD_OUTPUT"), "output format: table|json")
	timeout := fs.Duration("timeout", defaults.timeout(10*time.Second), "request timeout")
	fs.Parse(args)
	if *output == "" {
		*output = "table"
	}
	if strings.TrimSpace(*zoneID) == "" {
		fatalf("--zone is required")
	}
	client, ctx, cancel := dialAdmin(*adminEndpoint, *timeout)
	defer cancel()
	defer client.Close()
	resp, err := client.Admin.GetTopologyZone(ctx, &adminv1.GetTopologyZoneRequest{Cluster: clusterRef(*clusterID, *sbsClusterID), ZoneId: strings.TrimSpace(*zoneID)})
	if err != nil {
		fatalf("topology zone get failed: %v", err)
	}
	if strings.EqualFold(strings.TrimSpace(*output), "json") {
		writeJSON(resp)
		return
	}
	printTopologyZone(resp.GetZone())
}

func runTopologyZoneUpdate(args []string) {
	defaults := mustResolveCLIDefaults(args)
	fs := flag.NewFlagSet("topology zone update", flag.ExitOnError)
	registerContextFlags(fs, defaults)
	adminEndpoint := fs.String("admin-endpoint", defaults.adminEndpoint(), "cluster-wide sbs-admin gRPC endpoint")
	clusterID := fs.String("cluster-id", defaults.fieldValue("cluster_id", "NAMRBD_CLUSTER_ID"), "cluster id")
	sbsClusterID := fs.String("sbs-cluster-id", defaults.fieldValue("sbs_cluster_id", "SBS_CLUSTER_ID", "NAMRBD_SBS_CLUSTER_ID"), "sbs cluster id")
	zoneID := fs.String("zone", "", "zone id")
	displayName := fs.String("display-name", "", "display name")
	enable := fs.Bool("enable", false, "set lifecycle active")
	disable := fs.Bool("disable", false, "set lifecycle disabled")
	retire := fs.Bool("retire", false, "set lifecycle retiring")
	actor := fs.String("actor", getenvOrDefault("USER", "unknown"), "actor")
	reason := fs.String("reason", "topology-zone-update", "reason")
	timeout := fs.Duration("timeout", defaults.timeout(10*time.Second), "request timeout")
	var labels labelFlag
	fs.Var(&labels, "label", "label k=v (repeatable)")
	fs.Parse(args)
	if strings.TrimSpace(*zoneID) == "" {
		fatalf("--zone is required")
	}
	selected := 0
	var lifecycle adminv1.TopologyZoneLifecycle
	for _, item := range []struct {
		on    bool
		value adminv1.TopologyZoneLifecycle
	}{
		{*enable, adminv1.TopologyZoneLifecycle_TOPOLOGY_ZONE_LIFECYCLE_ACTIVE},
		{*disable, adminv1.TopologyZoneLifecycle_TOPOLOGY_ZONE_LIFECYCLE_DISABLED},
		{*retire, adminv1.TopologyZoneLifecycle_TOPOLOGY_ZONE_LIFECYCLE_RETIRING},
	} {
		if item.on {
			selected++
			lifecycle = item.value
		}
	}
	if selected > 1 {
		fatalf("only one of --enable, --disable, --retire may be set")
	}
	req := &adminv1.UpdateTopologyZoneRequest{
		Cluster: clusterRef(*clusterID, *sbsClusterID),
		Meta:    &adminv1.RequestMeta{Actor: *actor, Reason: *reason},
		ZoneId:  strings.TrimSpace(*zoneID),
		Labels:  map[string]string(labels),
	}
	if fs.Lookup("display-name") != nil && flagWasProvided(fs, "display-name") {
		value := strings.TrimSpace(*displayName)
		req.DisplayName = &value
	}
	if selected == 1 {
		req.Lifecycle = &lifecycle
	}
	client, ctx, cancel := dialAdmin(*adminEndpoint, *timeout)
	defer cancel()
	defer client.Close()
	resp, err := client.Admin.UpdateTopologyZone(ctx, req)
	if err != nil {
		fatalf("topology zone update failed: %v", err)
	}
	writeJSON(resp)
}

func runTopologyZoneDelete(args []string) {
	defaults := mustResolveCLIDefaults(args)
	fs := flag.NewFlagSet("topology zone delete", flag.ExitOnError)
	registerContextFlags(fs, defaults)
	adminEndpoint := fs.String("admin-endpoint", defaults.adminEndpoint(), "cluster-wide sbs-admin gRPC endpoint")
	clusterID := fs.String("cluster-id", defaults.fieldValue("cluster_id", "NAMRBD_CLUSTER_ID"), "cluster id")
	sbsClusterID := fs.String("sbs-cluster-id", defaults.fieldValue("sbs_cluster_id", "SBS_CLUSTER_ID", "NAMRBD_SBS_CLUSTER_ID"), "sbs cluster id")
	zoneID := fs.String("zone", "", "zone id")
	actor := fs.String("actor", getenvOrDefault("USER", "unknown"), "actor")
	reason := fs.String("reason", "topology-zone-delete", "reason")
	yes := fs.Bool("yes", false, "confirm delete")
	timeout := fs.Duration("timeout", defaults.timeout(10*time.Second), "request timeout")
	fs.Parse(args)
	if !*yes {
		fatalf("--yes is required for topology zone delete")
	}
	if strings.TrimSpace(*zoneID) == "" {
		fatalf("--zone is required")
	}
	client, ctx, cancel := dialAdmin(*adminEndpoint, *timeout)
	defer cancel()
	defer client.Close()
	resp, err := client.Admin.DeleteTopologyZone(ctx, &adminv1.DeleteTopologyZoneRequest{
		Cluster: clusterRef(*clusterID, *sbsClusterID),
		Meta:    &adminv1.RequestMeta{Actor: *actor, Reason: *reason},
		ZoneId:  strings.TrimSpace(*zoneID),
	})
	if err != nil {
		fatalf("topology zone delete failed: %v", err)
	}
	writeJSON(resp)
}

func flagWasProvided(fs *flag.FlagSet, name string) bool {
	found := false
	fs.Visit(func(f *flag.Flag) {
		if f.Name == name {
			found = true
		}
	})
	return found
}

func topologyZoneLifecycleName(v adminv1.TopologyZoneLifecycle) string {
	switch v {
	case adminv1.TopologyZoneLifecycle_TOPOLOGY_ZONE_LIFECYCLE_ACTIVE:
		return "active"
	case adminv1.TopologyZoneLifecycle_TOPOLOGY_ZONE_LIFECYCLE_DISABLED:
		return "disabled"
	case adminv1.TopologyZoneLifecycle_TOPOLOGY_ZONE_LIFECYCLE_RETIRING:
		return "retiring"
	default:
		return "unspecified"
	}
}

func printTopologyZone(zone *adminv1.TopologyZoneSummary) {
	if zone == nil {
		return
	}
	fmt.Printf("zone: %s\n", zone.GetZoneId())
	fmt.Printf("display_name: %s\n", zone.GetDisplayName())
	fmt.Printf("lifecycle: %s\n", topologyZoneLifecycleName(zone.GetLifecycle()))
	if labels := zone.GetLabels(); len(labels) > 0 {
		keys := make([]string, 0, len(labels))
		for k := range labels {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			fmt.Printf("label_%s: %s\n", k, labels[k])
		}
	}
}

func runNodeJoin(args []string) {
	defaults := mustResolveCLIDefaults(args)
	fs := flag.NewFlagSet("node join", flag.ExitOnError)
	registerContextFlags(fs, defaults)
	adminEndpoint := fs.String("admin-endpoint", defaults.adminEndpoint(), "cluster-wide sbs-admin gRPC endpoint")
	clusterID := fs.String("cluster-id", defaults.fieldValue("cluster_id", "NAMRBD_CLUSTER_ID"), "cluster id")
	sbsClusterID := fs.String("sbs-cluster-id", defaults.fieldValue("sbs_cluster_id", "SBS_CLUSTER_ID", "NAMRBD_SBS_CLUSTER_ID"), "sbs cluster id")
	nodeID := fs.String("node-id", defaults.fieldValue("node_id", "SBS_NODE_ID", "NAMRBD_NODE_ID"), "node id")
	grpcEndpoint := fs.String("grpc-endpoint", defaults.dataEndpoint(), "sbs-data gRPC endpoint")
	adminHTTP := fs.String("admin-http-endpoint", defaults.fieldValue("sbs_node_admin_http", "SBS_NODE_ADMIN_HTTP", "NAMRBD_SBS_ADMIN_ADDR"), "node-local admin/debug HTTP endpoint")
	zone := fs.String("zone", defaults.fieldValue("zone", "SBS_ZONE", "NAMRBD_ZONE"), "zone")
	autoCreateZone := fs.Bool("auto-create-zone", false, "create the zone if missing")
	actor := fs.String("actor", getenvOrDefault("USER", "unknown"), "actor")
	reason := fs.String("reason", "join", "reason")
	timeout := fs.Duration("timeout", defaults.timeout(10*time.Second), "request timeout")
	fs.Parse(args)
	printResolvedSettings(fs,
		defaults.adminEndpointSetting(),
		defaults.fieldSetting("cluster_id", "cluster-id", "", "NAMRBD_CLUSTER_ID"),
		defaults.fieldSetting("sbs_cluster_id", "sbs-cluster-id", "", "SBS_CLUSTER_ID", "NAMRBD_SBS_CLUSTER_ID"),
		defaults.fieldSetting("node_id", "node-id", "", "SBS_NODE_ID", "NAMRBD_NODE_ID"),
		defaults.dataEndpointSetting(),
		defaults.fieldSetting("sbs_node_admin_http", "admin-http-endpoint", "", "SBS_NODE_ADMIN_HTTP", "NAMRBD_SBS_ADMIN_ADDR"),
		defaults.fieldSetting("zone", "zone", "", "SBS_ZONE", "NAMRBD_ZONE"),
		defaults.timeoutSetting(10*time.Second),
	)
	if *nodeID == "" || *grpcEndpoint == "" {
		fatalf("--node-id and --grpc-endpoint are required")
	}

	client, ctx, cancel := dialAdmin(*adminEndpoint, *timeout)
	defer cancel()
	defer client.Close()
	resp, err := client.Admin.JoinNode(ctx, &adminv1.JoinNodeRequest{
		Cluster:           clusterRef(*clusterID, *sbsClusterID),
		Meta:              &adminv1.RequestMeta{Actor: *actor, Reason: *reason},
		NodeId:            *nodeID,
		GrpcEndpoint:      *grpcEndpoint,
		AdminHttpEndpoint: *adminHTTP,
		Zone:              *zone,
		AutoCreateZone:    *autoCreateZone,
	})
	if err != nil {
		fatalf("node join failed: %v", err)
	}
	writeJSON(resp)
}

func runNodeUpdateTopology(args []string) {
	defaults := mustResolveCLIDefaults(args)
	fs := flag.NewFlagSet("node update-topology", flag.ExitOnError)
	registerContextFlags(fs, defaults)
	adminEndpoint := fs.String("admin-endpoint", defaults.adminEndpoint(), "cluster-wide sbs-admin gRPC endpoint")
	clusterID := fs.String("cluster-id", defaults.fieldValue("cluster_id", "NAMRBD_CLUSTER_ID"), "cluster id")
	sbsClusterID := fs.String("sbs-cluster-id", defaults.fieldValue("sbs_cluster_id", "SBS_CLUSTER_ID", "NAMRBD_SBS_CLUSTER_ID"), "sbs cluster id")
	nodeID := fs.String("node-id", defaults.fieldValue("node_id", "SBS_NODE_ID", "NAMRBD_NODE_ID"), "node id")
	zone := fs.String("zone", defaults.fieldValue("zone", "SBS_ZONE", "NAMRBD_ZONE"), "zone")
	autoCreateZone := fs.Bool("auto-create-zone", false, "create the zone if missing")
	actor := fs.String("actor", getenvOrDefault("USER", "unknown"), "actor")
	reason := fs.String("reason", "node-update-topology", "reason")
	timeout := fs.Duration("timeout", defaults.timeout(10*time.Second), "request timeout")
	fs.Parse(args)
	printResolvedSettings(fs,
		defaults.adminEndpointSetting(),
		defaults.fieldSetting("cluster_id", "cluster-id", "", "NAMRBD_CLUSTER_ID"),
		defaults.fieldSetting("sbs_cluster_id", "sbs-cluster-id", "", "SBS_CLUSTER_ID", "NAMRBD_SBS_CLUSTER_ID"),
		defaults.fieldSetting("node_id", "node-id", "", "SBS_NODE_ID", "NAMRBD_NODE_ID"),
		defaults.fieldSetting("zone", "zone", "", "SBS_ZONE", "NAMRBD_ZONE"),
		defaults.timeoutSetting(10*time.Second),
	)
	if strings.TrimSpace(*nodeID) == "" || strings.TrimSpace(*zone) == "" {
		fatalf("--node-id and --zone are required")
	}
	client, ctx, cancel := dialAdmin(*adminEndpoint, *timeout)
	defer cancel()
	defer client.Close()
	resp, err := client.Admin.UpdateNodeTopology(ctx, &adminv1.UpdateNodeTopologyRequest{
		Cluster:        clusterRef(*clusterID, *sbsClusterID),
		Meta:           &adminv1.RequestMeta{Actor: *actor, Reason: *reason},
		NodeId:         strings.TrimSpace(*nodeID),
		Zone:           strings.TrimSpace(*zone),
		AutoCreateZone: *autoCreateZone,
	})
	if err != nil {
		fatalf("node update-topology failed: %v", err)
	}
	writeJSON(resp)
}

func runNodeDrain(args []string) {
	defaults := mustResolveCLIDefaults(args)
	fs := flag.NewFlagSet("node drain", flag.ExitOnError)
	registerContextFlags(fs, defaults)
	adminEndpoint := fs.String("admin-endpoint", defaults.adminEndpoint(), "cluster-wide sbs-admin gRPC endpoint")
	clusterID := fs.String("cluster-id", defaults.fieldValue("cluster_id", "NAMRBD_CLUSTER_ID"), "cluster id")
	sbsClusterID := fs.String("sbs-cluster-id", defaults.fieldValue("sbs_cluster_id", "SBS_CLUSTER_ID", "NAMRBD_SBS_CLUSTER_ID"), "sbs cluster id")
	nodeID := fs.String("node-id", defaults.fieldValue("node_id", "SBS_NODE_ID", "NAMRBD_NODE_ID"), "node id")
	actor := fs.String("actor", getenvOrDefault("USER", "unknown"), "actor")
	reason := fs.String("reason", "drain", "reason")
	yes := fs.Bool("yes", false, "confirm drain")
	timeout := fs.Duration("timeout", defaults.timeout(10*time.Second), "request timeout")
	fs.Parse(args)
	printResolvedSettings(fs,
		defaults.adminEndpointSetting(),
		defaults.fieldSetting("cluster_id", "cluster-id", "", "NAMRBD_CLUSTER_ID"),
		defaults.fieldSetting("sbs_cluster_id", "sbs-cluster-id", "", "SBS_CLUSTER_ID", "NAMRBD_SBS_CLUSTER_ID"),
		defaults.fieldSetting("node_id", "node-id", "", "SBS_NODE_ID", "NAMRBD_NODE_ID"),
		defaults.timeoutSetting(10*time.Second),
	)
	if !*yes {
		fatalf("--yes is required for node drain")
	}
	if *nodeID == "" {
		fatalf("--node-id is required")
	}

	client, ctx, cancel := dialAdmin(*adminEndpoint, *timeout)
	defer cancel()
	defer client.Close()
	resp, err := client.Admin.DrainNode(ctx, &adminv1.DrainNodeRequest{
		Cluster: clusterRef(*clusterID, *sbsClusterID),
		Meta:    &adminv1.RequestMeta{Actor: *actor, Reason: *reason},
		NodeId:  *nodeID,
	})
	if err != nil {
		fatalf("node drain failed: %v", err)
	}
	writeJSON(resp)
}

func runNodeDrainStatus(args []string) {
	defaults := mustResolveCLIDefaults(args)
	fs := flag.NewFlagSet("node drain status", flag.ExitOnError)
	registerContextFlags(fs, defaults)
	adminEndpoint := fs.String("admin-endpoint", defaults.adminEndpoint(), "cluster-wide sbs-admin gRPC endpoint")
	clusterID := fs.String("cluster-id", defaults.fieldValue("cluster_id", "NAMRBD_CLUSTER_ID"), "cluster id")
	sbsClusterID := fs.String("sbs-cluster-id", defaults.fieldValue("sbs_cluster_id", "SBS_CLUSTER_ID", "NAMRBD_SBS_CLUSTER_ID"), "sbs cluster id")
	nodeID := fs.String("node-id", defaults.fieldValue("node_id", "SBS_NODE_ID", "NAMRBD_NODE_ID"), "node id")
	output := fs.String("output", defaults.fieldValue("output", "SBS_OUTPUT", "NAMRBD_OUTPUT"), "output format: table|json")
	timeout := fs.Duration("timeout", defaults.timeout(10*time.Second), "request timeout")
	fs.Parse(args)
	if *output == "" {
		*output = "table"
	}
	printResolvedSettings(fs,
		defaults.adminEndpointSetting(),
		defaults.fieldSetting("cluster_id", "cluster-id", "", "NAMRBD_CLUSTER_ID"),
		defaults.fieldSetting("sbs_cluster_id", "sbs-cluster-id", "", "SBS_CLUSTER_ID", "NAMRBD_SBS_CLUSTER_ID"),
		defaults.fieldSetting("node_id", "node-id", "", "SBS_NODE_ID", "NAMRBD_NODE_ID"),
		defaults.fieldSetting("output", "output", "table", "SBS_OUTPUT", "NAMRBD_OUTPUT"),
		defaults.timeoutSetting(10*time.Second),
	)
	if *nodeID == "" {
		fatalf("--node-id is required")
	}

	client, ctx, cancel := dialAdmin(*adminEndpoint, *timeout)
	defer cancel()
	defer client.Close()
	resp, err := client.Operations.ListOperations(ctx, &adminv1.ListOperationsRequest{
		Cluster: clusterRef(*clusterID, *sbsClusterID),
		Kind:    "node.drain",
	})
	if err != nil {
		fatalf("node drain status failed: %v", err)
	}
	var selected *adminv1.OperationStatus
	for _, op := range resp.GetOperations() {
		if op.GetTargetNodeId() == *nodeID {
			selected = op
		}
	}
	if selected == nil {
		fatalf("no drain operation found for node %s", *nodeID)
	}
	switch *output {
	case "json":
		writeJSON(selected)
	default:
		fmt.Printf("operation_id: %s\n", selected.GetOperationId())
		fmt.Printf("node_id: %s\n", selected.GetTargetNodeId())
		fmt.Printf("state: %s\n", selected.GetState().String())
		fmt.Printf("phase: %s\n", selected.GetPhase())
		fmt.Printf("extents_remaining: %d\n", selected.GetExtentsRemaining())
		fmt.Printf("bytes_remaining: %d\n", selected.GetBytesRemaining())
		fmt.Printf("blocking_reason: %s\n", selected.GetBlockingReason())
	}
}

func runNodeRemove(args []string) {
	defaults := mustResolveCLIDefaults(args)
	fs := flag.NewFlagSet("node remove", flag.ExitOnError)
	registerContextFlags(fs, defaults)
	adminEndpoint := fs.String("admin-endpoint", defaults.adminEndpoint(), "cluster-wide sbs-admin gRPC endpoint")
	clusterID := fs.String("cluster-id", defaults.fieldValue("cluster_id", "NAMRBD_CLUSTER_ID"), "cluster id")
	sbsClusterID := fs.String("sbs-cluster-id", defaults.fieldValue("sbs_cluster_id", "SBS_CLUSTER_ID", "NAMRBD_SBS_CLUSTER_ID"), "sbs cluster id")
	nodeID := fs.String("node-id", defaults.fieldValue("node_id", "SBS_NODE_ID", "NAMRBD_NODE_ID"), "node id")
	actor := fs.String("actor", getenvOrDefault("USER", "unknown"), "actor")
	reason := fs.String("reason", "remove", "reason")
	force := fs.Bool("force", false, "force remove")
	yes := fs.Bool("yes", false, "confirm remove")
	timeout := fs.Duration("timeout", defaults.timeout(10*time.Second), "request timeout")
	fs.Parse(args)
	printResolvedSettings(fs,
		defaults.adminEndpointSetting(),
		defaults.fieldSetting("cluster_id", "cluster-id", "", "NAMRBD_CLUSTER_ID"),
		defaults.fieldSetting("sbs_cluster_id", "sbs-cluster-id", "", "SBS_CLUSTER_ID", "NAMRBD_SBS_CLUSTER_ID"),
		defaults.fieldSetting("node_id", "node-id", "", "SBS_NODE_ID", "NAMRBD_NODE_ID"),
		defaults.timeoutSetting(10*time.Second),
	)
	if !*yes {
		fatalf("--yes is required for node remove")
	}
	if *nodeID == "" {
		fatalf("--node-id is required")
	}

	client, ctx, cancel := dialAdmin(*adminEndpoint, *timeout)
	defer cancel()
	defer client.Close()

	if *force {
		resp, err := client.Admin.ForceRemoveNode(ctx, &adminv1.ForceRemoveNodeRequest{
			Cluster: clusterRef(*clusterID, *sbsClusterID),
			Meta:    &adminv1.RequestMeta{Actor: *actor, Reason: *reason},
			NodeId:  *nodeID,
		})
		if err != nil {
			fatalf("node remove --force failed: %v", err)
		}
		writeJSON(resp)
		return
	}

	resp, err := client.Admin.RemoveNode(ctx, &adminv1.RemoveNodeRequest{
		Cluster: clusterRef(*clusterID, *sbsClusterID),
		Meta:    &adminv1.RequestMeta{Actor: *actor, Reason: *reason},
		NodeId:  *nodeID,
	})
	if err != nil {
		fatalf("node remove failed: %v", err)
	}
	writeJSON(resp)
}

func runVolumeCreate(args []string) {
	defaults := mustResolveCLIDefaults(args)
	fs := flag.NewFlagSet("volume create", flag.ExitOnError)
	registerContextFlags(fs, defaults)
	adminEndpoint := fs.String("admin-endpoint", defaults.adminEndpoint(), "cluster-wide sbs-admin gRPC endpoint")
	clusterID := fs.String("cluster-id", defaults.fieldValue("cluster_id", "NAMRBD_CLUSTER_ID"), "cluster id")
	sbsClusterID := fs.String("sbs-cluster-id", defaults.fieldValue("sbs_cluster_id", "SBS_CLUSTER_ID", "NAMRBD_SBS_CLUSTER_ID"), "sbs cluster id")
	volumeID := fs.String("volume-id", "", "volume id")
	size := fs.String("size", "", "volume size, e.g. 10G or 100T")
	blockSize := fs.String("block-size", "4K", "block size, e.g. 4K")
	extentSize := fs.String("extent-size", "", "logical extent size, e.g. 4M; empty uses server default")
	allocationChunkSize := fs.String("allocation-chunk-size", "", "allocation chunk size, e.g. 64K; empty uses server default")
	allocationPageSize := fs.String("allocation-page-size", "", "allocation page size, e.g. 256K; empty uses server default")
	replicationFactor := fs.Uint("replication-factor", 1, "replication factor")
	redundancyBackend := fs.String("redundancy-backend", "replicated", "redundancy backend: replicated|ec")
	ecProfileID := fs.String("ec-profile", "", "EC profile id for ec volumes")
	weakPlacementAllowed := fs.Bool("weak-placement", false, "allow explicit weak EC placement")
	policyName := fs.String("policy-name", "", "placement policy")
	topologyMode := fs.String("topology-mode", "", "topology mode; empty uses backend default")
	actor := fs.String("actor", getenvOrDefault("USER", "unknown"), "actor")
	reason := fs.String("reason", "create-volume", "reason")
	timeout := fs.Duration("timeout", defaults.timeout(10*time.Second), "request timeout")
	fs.Parse(args)
	printResolvedSettings(fs,
		defaults.adminEndpointSetting(),
		defaults.fieldSetting("cluster_id", "cluster-id", "", "NAMRBD_CLUSTER_ID"),
		defaults.fieldSetting("sbs_cluster_id", "sbs-cluster-id", "", "SBS_CLUSTER_ID", "NAMRBD_SBS_CLUSTER_ID"),
		defaults.timeoutSetting(10*time.Second),
	)
	if *volumeID == "" {
		fatalf("--volume-id is required")
	}
	sizeBytes, err := parseBinarySize(*size, "size")
	if err != nil {
		fatalf("invalid --size: %v", err)
	}
	blockSizeBytes, err := parseUint32BinarySize(*blockSize, "block size")
	if err != nil {
		fatalf("invalid --block-size: %v", err)
	}
	var extentSizeBytes uint64
	if flagWasSet(fs, "extent-size") {
		extentSizeBytes, err = parseBinarySize(*extentSize, "extent-size")
		if err != nil {
			fatalf("invalid --extent-size: %v", err)
		}
	}
	allocationChunkSizeBytes, err := resolveOptionalUint32BinarySizeFlag(fs, "allocation-chunk-size", *allocationChunkSize)
	if err != nil {
		fatalf("invalid --allocation-chunk-size: %v", err)
	}
	allocationPageSizeBytes, err := resolveOptionalUint32BinarySizeFlag(fs, "allocation-page-size", *allocationPageSize)
	if err != nil {
		fatalf("invalid --allocation-page-size: %v", err)
	}

	client, ctx, cancel := dialAdmin(*adminEndpoint, *timeout)
	defer cancel()
	defer client.Close()
	resp, err := client.Admin.CreateVolume(ctx, &adminv1.CreateVolumeRequest{
		Cluster:                  clusterRef(*clusterID, *sbsClusterID),
		Meta:                     &adminv1.RequestMeta{Actor: *actor, Reason: *reason},
		VolumeId:                 *volumeID,
		SizeBytes:                sizeBytes,
		BlockSize:                blockSizeBytes,
		ExtentSizeBytes:          extentSizeBytes,
		AllocationChunkSizeBytes: allocationChunkSizeBytes,
		AllocationPageSizeBytes:  allocationPageSizeBytes,
		ReplicationFactor:        uint32(*replicationFactor),
		RedundancyBackend:        *redundancyBackend,
		EcProfileId:              *ecProfileID,
		WeakPlacementAllowed:     *weakPlacementAllowed,
		PolicyName:               *policyName,
		TopologyMode:             *topologyMode,
	})
	if err != nil {
		fatalf("volume create failed: %v", err)
	}
	writeJSON(resp)
}

func runVolumeRestoreFromSnapshot(args []string) {
	defaults := mustResolveCLIDefaults(args)
	fs := flag.NewFlagSet("volume restore-from-snapshot", flag.ExitOnError)
	registerContextFlags(fs, defaults)
	adminEndpoint := fs.String("admin-endpoint", defaults.adminEndpoint(), "cluster-wide sbs-admin gRPC endpoint")
	clusterID := fs.String("cluster-id", defaults.fieldValue("cluster_id", "NAMRBD_CLUSTER_ID"), "cluster id")
	sbsClusterID := fs.String("sbs-cluster-id", defaults.fieldValue("sbs_cluster_id", "SBS_CLUSTER_ID", "NAMRBD_SBS_CLUSTER_ID"), "sbs cluster id")
	sourceSnapshotID := fs.String("source-snapshot-id", "", "source snapshot id")
	volumeID := fs.String("volume-id", "", "restored target volume id")
	size := fs.String("size", "", "restored target size, e.g. 10G; empty uses source snapshot size")
	idempotencyKey := fs.String("idempotency-key", "", "idempotency key; empty derives from source snapshot and target volume id")
	actor := fs.String("actor", getenvOrDefault("USER", "unknown"), "actor")
	reason := fs.String("reason", "restore-volume-from-snapshot", "reason")
	timeout := fs.Duration("timeout", defaults.timeout(10*time.Second), "request timeout")
	fs.Parse(args)
	printResolvedSettings(fs,
		defaults.adminEndpointSetting(),
		defaults.fieldSetting("cluster_id", "cluster-id", "", "NAMRBD_CLUSTER_ID"),
		defaults.fieldSetting("sbs_cluster_id", "sbs-cluster-id", "", "SBS_CLUSTER_ID", "NAMRBD_SBS_CLUSTER_ID"),
		defaults.timeoutSetting(10*time.Second),
	)
	if *sourceSnapshotID == "" {
		fatalf("--source-snapshot-id is required")
	}
	var sizeBytes uint64
	if flagWasSet(fs, "size") {
		parsed, err := parseBinarySize(*size, "size")
		if err != nil {
			fatalf("invalid --size: %v", err)
		}
		sizeBytes = parsed
	}

	client, ctx, cancel := dialAdmin(*adminEndpoint, *timeout)
	defer cancel()
	defer client.Close()
	resp, err := client.Admin.CreateVolumeFromSnapshot(ctx, &adminv1.CreateVolumeFromSnapshotRequest{
		Cluster:          clusterRef(*clusterID, *sbsClusterID),
		Meta:             &adminv1.RequestMeta{Actor: *actor, Reason: *reason},
		SourceSnapshotId: *sourceSnapshotID,
		VolumeId:         *volumeID,
		SizeBytes:        sizeBytes,
		IdempotencyKey:   *idempotencyKey,
	})
	if err != nil {
		fatalf("volume restore-from-snapshot failed: %v", err)
	}
	writeJSON(resp)
}

func runVolumeExpand(args []string) {
	defaults := mustResolveCLIDefaults(args)
	fs := flag.NewFlagSet("volume expand", flag.ExitOnError)
	registerContextFlags(fs, defaults)
	adminEndpoint := fs.String("admin-endpoint", defaults.adminEndpoint(), "cluster-wide sbs-admin gRPC endpoint")
	clusterID := fs.String("cluster-id", defaults.fieldValue("cluster_id", "NAMRBD_CLUSTER_ID"), "cluster id")
	sbsClusterID := fs.String("sbs-cluster-id", defaults.fieldValue("sbs_cluster_id", "SBS_CLUSTER_ID", "NAMRBD_SBS_CLUSTER_ID"), "sbs cluster id")
	volumeID := fs.String("volume-id", "", "volume id")
	targetSize := fs.String("target-size", "", "target volume size, e.g. 100G")
	addSize := fs.String("add-size", "", "size to add, e.g. 10G")
	idempotencyKey := fs.String("idempotency-key", "", "idempotency key")
	actor := fs.String("actor", getenvOrDefault("USER", "unknown"), "actor")
	reason := fs.String("reason", "expand-volume", "reason")
	timeout := fs.Duration("timeout", defaults.timeout(10*time.Second), "request timeout")
	fs.Parse(args)
	printResolvedSettings(fs,
		defaults.adminEndpointSetting(),
		defaults.fieldSetting("cluster_id", "cluster-id", "", "NAMRBD_CLUSTER_ID"),
		defaults.fieldSetting("sbs_cluster_id", "sbs-cluster-id", "", "SBS_CLUSTER_ID", "NAMRBD_SBS_CLUSTER_ID"),
		defaults.timeoutSetting(10*time.Second),
	)
	if *volumeID == "" {
		fatalf("--volume-id is required")
	}
	if (*targetSize == "") == (*addSize == "") {
		fatalf("exactly one of --target-size or --add-size is required")
	}
	client, ctx, cancel := dialAdmin(*adminEndpoint, *timeout)
	defer cancel()
	defer client.Close()
	var targetSizeBytes uint64
	if *targetSize != "" {
		var err error
		targetSizeBytes, err = parseBinarySize(*targetSize, "target-size")
		if err != nil {
			fatalf("invalid --target-size: %v", err)
		}
	} else {
		addSizeBytes, err := parseBinarySize(*addSize, "add-size")
		if err != nil {
			fatalf("invalid --add-size: %v", err)
		}
		current, err := client.Admin.GetVolume(ctx, &adminv1.GetVolumeRequest{
			Cluster:  clusterRef(*clusterID, *sbsClusterID),
			VolumeId: *volumeID,
		})
		if err != nil {
			fatalf("volume expand failed: get current volume: %v", err)
		}
		currentSize := current.GetVolume().GetSizeBytes()
		if currentSize > ^uint64(0)-addSizeBytes {
			fatalf("volume expand failed: size overflow")
		}
		targetSizeBytes = currentSize + addSizeBytes
	}
	resp, err := client.Admin.ExpandVolume(ctx, &adminv1.ExpandVolumeRequest{
		Cluster:         clusterRef(*clusterID, *sbsClusterID),
		Meta:            &adminv1.RequestMeta{Actor: *actor, Reason: *reason},
		VolumeId:        *volumeID,
		TargetSizeBytes: targetSizeBytes,
		IdempotencyKey:  *idempotencyKey,
	})
	if err != nil {
		fatalf("volume expand failed: %v", err)
	}
	writeJSON(resp)
}

func flagWasSet(fs *flag.FlagSet, name string) bool {
	wasSet := false
	fs.Visit(func(f *flag.Flag) {
		if f.Name == name {
			wasSet = true
		}
	})
	return wasSet
}

func resolveOptionalUint32BinarySizeFlag(fs *flag.FlagSet, name, value string) (uint32, error) {
	if !flagWasSet(fs, name) {
		return 0, nil
	}
	return parseUint32BinarySize(value, name)
}

func parseUint32BinarySize(raw, label string) (uint32, error) {
	value, err := parseBinarySize(raw, label)
	if err != nil {
		return 0, err
	}
	if value > uint64(^uint32(0)) {
		return 0, fmt.Errorf("must fit uint32")
	}
	return uint32(value), nil
}

func parseBinarySize(raw, label string) (uint64, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return 0, fmt.Errorf("%s is required", label)
	}
	if len(raw) < 2 {
		return 0, fmt.Errorf("must include a numeric value and unit K, M, G, or T")
	}
	unit := raw[len(raw)-1]
	var multiplier uint64
	switch unit {
	case 'K', 'k':
		multiplier = 1 << 10
	case 'M', 'm':
		multiplier = 1 << 20
	case 'G', 'g':
		multiplier = 1 << 30
	case 'T', 't':
		multiplier = 1 << 40
	default:
		return 0, fmt.Errorf("unit must be one of K, M, G, or T")
	}
	number := strings.TrimSpace(raw[:len(raw)-1])
	if number == "" {
		return 0, fmt.Errorf("numeric value is required")
	}
	value, err := strconv.ParseUint(number, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid numeric value %q", number)
	}
	if value == 0 {
		return 0, fmt.Errorf("must be greater than zero")
	}
	if value > ^uint64(0)/multiplier {
		return 0, fmt.Errorf("value overflows uint64")
	}
	return value * multiplier, nil
}

func runVolumeDelete(args []string) {
	defaults := mustResolveCLIDefaults(args)
	fs := flag.NewFlagSet("volume delete", flag.ExitOnError)
	registerContextFlags(fs, defaults)
	adminEndpoint := fs.String("admin-endpoint", defaults.adminEndpoint(), "cluster-wide sbs-admin gRPC endpoint")
	clusterID := fs.String("cluster-id", defaults.fieldValue("cluster_id", "NAMRBD_CLUSTER_ID"), "cluster id")
	sbsClusterID := fs.String("sbs-cluster-id", defaults.fieldValue("sbs_cluster_id", "SBS_CLUSTER_ID", "NAMRBD_SBS_CLUSTER_ID"), "sbs cluster id")
	volumeID := fs.String("volume-id", "", "volume id")
	actor := fs.String("actor", getenvOrDefault("USER", "unknown"), "actor")
	reason := fs.String("reason", "delete-volume", "reason")
	yes := fs.Bool("yes", false, "confirm delete")
	timeout := fs.Duration("timeout", defaults.timeout(10*time.Second), "request timeout")
	fs.Parse(args)
	printResolvedSettings(fs,
		defaults.adminEndpointSetting(),
		defaults.fieldSetting("cluster_id", "cluster-id", "", "NAMRBD_CLUSTER_ID"),
		defaults.fieldSetting("sbs_cluster_id", "sbs-cluster-id", "", "SBS_CLUSTER_ID", "NAMRBD_SBS_CLUSTER_ID"),
		defaults.timeoutSetting(10*time.Second),
	)
	if !*yes {
		fatalf("--yes is required for volume delete")
	}
	if *volumeID == "" {
		fatalf("--volume-id is required")
	}

	client, ctx, cancel := dialAdmin(*adminEndpoint, *timeout)
	defer cancel()
	defer client.Close()
	resp, err := client.Admin.DeleteVolume(ctx, &adminv1.DeleteVolumeRequest{
		Cluster:  clusterRef(*clusterID, *sbsClusterID),
		Meta:     &adminv1.RequestMeta{Actor: *actor, Reason: *reason},
		VolumeId: *volumeID,
	})
	if err != nil {
		fatalf("volume delete failed: %v", err)
	}
	writeJSON(resp)
}

func validateVolumePurgeArgs(volumeID string, yes, confirmedDeletion bool) error {
	if !yes {
		return fmt.Errorf("--yes is required for volume purge")
	}
	if !confirmedDeletion {
		return fmt.Errorf("--i-confirmed-deletion is required for volume purge")
	}
	if volumeID == "" {
		return fmt.Errorf("--volume-id is required")
	}
	return nil
}

func runVolumePurge(args []string) {
	defaults := mustResolveCLIDefaults(args)
	fs := flag.NewFlagSet("volume purge", flag.ExitOnError)
	registerContextFlags(fs, defaults)
	adminEndpoint := fs.String("admin-endpoint", defaults.adminEndpoint(), "cluster-wide sbs-admin gRPC endpoint")
	clusterID := fs.String("cluster-id", defaults.fieldValue("cluster_id", "NAMRBD_CLUSTER_ID"), "cluster id")
	sbsClusterID := fs.String("sbs-cluster-id", defaults.fieldValue("sbs_cluster_id", "SBS_CLUSTER_ID", "NAMRBD_SBS_CLUSTER_ID"), "sbs cluster id")
	volumeID := fs.String("volume-id", "", "volume id")
	actor := fs.String("actor", getenvOrDefault("USER", "unknown"), "actor")
	reason := fs.String("reason", "purge-volume", "reason")
	yes := fs.Bool("yes", false, "confirm purge")
	confirmedDeletion := fs.Bool("i-confirmed-deletion", false, "explicitly acknowledge destructive purge semantics")
	timeout := fs.Duration("timeout", defaults.timeout(10*time.Second), "request timeout")
	fs.Parse(args)
	printResolvedSettings(fs,
		defaults.adminEndpointSetting(),
		defaults.fieldSetting("cluster_id", "cluster-id", "", "NAMRBD_CLUSTER_ID"),
		defaults.fieldSetting("sbs_cluster_id", "sbs-cluster-id", "", "SBS_CLUSTER_ID", "NAMRBD_SBS_CLUSTER_ID"),
		defaults.timeoutSetting(10*time.Second),
	)
	if err := validateVolumePurgeArgs(*volumeID, *yes, *confirmedDeletion); err != nil {
		fatalf("%v", err)
	}

	client, ctx, cancel := dialAdmin(*adminEndpoint, *timeout)
	defer cancel()
	defer client.Close()
	resp, err := client.Admin.PurgeVolume(ctx, &adminv1.PurgeVolumeRequest{
		Cluster:           clusterRef(*clusterID, *sbsClusterID),
		Meta:              &adminv1.RequestMeta{Actor: *actor, Reason: *reason},
		VolumeId:          *volumeID,
		ConfirmedDeletion: true,
	})
	if err != nil {
		fatalf("volume purge failed: %v", err)
	}
	writeJSON(resp)
}

func runVolumeStatus(args []string) {
	defaults := mustResolveCLIDefaults(args)
	fs := flag.NewFlagSet("volume status", flag.ExitOnError)
	registerContextFlags(fs, defaults)
	adminEndpoint := fs.String("admin-endpoint", defaults.adminEndpoint(), "cluster-wide sbs-admin gRPC endpoint")
	clusterID := fs.String("cluster-id", defaults.fieldValue("cluster_id", "NAMRBD_CLUSTER_ID"), "cluster id")
	sbsClusterID := fs.String("sbs-cluster-id", defaults.fieldValue("sbs_cluster_id", "SBS_CLUSTER_ID", "NAMRBD_SBS_CLUSTER_ID"), "sbs cluster id")
	volumeID := fs.String("volume-id", "", "volume id")
	output := fs.String("output", defaults.fieldValue("output", "SBS_OUTPUT", "NAMRBD_OUTPUT"), "output format: table|json")
	summaryMode := fs.String("summary-mode", "", "summary mode: full|spec-only")
	timeout := fs.Duration("timeout", defaults.timeout(10*time.Second), "request timeout")
	fs.Parse(args)
	if *output == "" {
		*output = "table"
	}
	*summaryMode = strings.TrimSpace(*summaryMode)
	switch {
	case *summaryMode == "", strings.EqualFold(*summaryMode, "full"):
		*summaryMode = ""
	case strings.EqualFold(*summaryMode, adminVolumeSummaryModeSpecOnly):
		*summaryMode = adminVolumeSummaryModeSpecOnly
	default:
		fatalf("unsupported --summary-mode %q", *summaryMode)
	}
	printResolvedSettings(fs,
		defaults.adminEndpointSetting(),
		defaults.fieldSetting("cluster_id", "cluster-id", "", "NAMRBD_CLUSTER_ID"),
		defaults.fieldSetting("sbs_cluster_id", "sbs-cluster-id", "", "SBS_CLUSTER_ID", "NAMRBD_SBS_CLUSTER_ID"),
		defaults.fieldSetting("output", "output", "table", "SBS_OUTPUT", "NAMRBD_OUTPUT"),
		defaults.timeoutSetting(10*time.Second),
	)
	if *volumeID == "" {
		fatalf("--volume-id is required")
	}

	client, ctx, cancel := dialAdmin(*adminEndpoint, *timeout)
	defer cancel()
	defer client.Close()
	if *summaryMode == adminVolumeSummaryModeSpecOnly {
		ctx = grpcmetadata.AppendToOutgoingContext(ctx, adminVolumeSummaryModeMetadataKey, adminVolumeSummaryModeSpecOnly)
	}
	resp, err := client.Admin.GetVolume(ctx, &adminv1.GetVolumeRequest{
		Cluster:  clusterRef(*clusterID, *sbsClusterID),
		VolumeId: *volumeID,
	})
	if err != nil {
		fatalf("volume status failed: %v", err)
	}
	switch *output {
	case "json":
		writeJSON(resp)
	default:
		vol := resp.GetVolume()
		fmt.Printf("volume_id: %s\n", vol.GetVolumeId())
		fmt.Printf("size_bytes: %d\n", vol.GetSizeBytes())
		fmt.Printf("block_size: %d\n", vol.GetBlockSize())
		fmt.Printf("topology_mode: %s\n", vol.GetTopologyMode())
		fmt.Printf("redundancy_backend: %s\n", vol.GetRedundancyBackend())
		if vol.GetRedundancyBackend() == "ec" {
			fmt.Printf("ec_profile_id: %s\n", vol.GetEcProfileId())
			fmt.Printf("ec_codec_id: %s\n", vol.GetEcCodecId())
			fmt.Printf("ec_data_shards: %d\n", vol.GetEcDataShards())
			fmt.Printf("ec_parity_shards: %d\n", vol.GetEcParityShards())
			fmt.Printf("ec_stripe_unit_bytes: %d\n", vol.GetEcStripeUnitBytes())
			fmt.Printf("ec_failure_domain: %s\n", vol.GetEcFailureDomain())
			fmt.Printf("weak_placement_allowed: %t\n", vol.GetWeakPlacementAllowed())
		}
		fmt.Printf("health: %s\n", vol.GetHealth().String())
		fmt.Printf("volume_revision: %d\n", vol.GetVolumeRevision())
		fmt.Printf("repair_backlog: %d\n", vol.GetRepairBacklog())
		fmt.Printf("repair_backlog_current: %d\n", vol.GetRepairBacklog())
		fmt.Printf("repair_backlog_bytes: %d\n", vol.GetRepairBacklogBytes())
		fmt.Printf("repair_backlog_chunks: %d\n", vol.GetRepairBacklogChunks())
		fmt.Printf("repair_backlog_allocation_chunks: %d\n", vol.GetRepairBacklogChunks())
		fmt.Printf("repair_backlog_oldest_age_seconds: %d\n", vol.GetTransitionOldestFailedBatchAgeSeconds())
		fmt.Printf("repair_backlog_max_age_seconds: %d\n", vol.GetTransitionOldestFailedBatchAgeSeconds())
		fmt.Printf("repair_backlog_failed_total: %d\n", vol.GetTransitionFailedBatches())
		fmt.Printf("rebalance_backlog: %d\n", vol.GetRebalanceBacklog())
		fmt.Printf("rebalance_backlog_current: %d\n", vol.GetRebalanceBacklog())
		fmt.Printf("rebalance_backlog_bytes: %d\n", vol.GetRebalanceBacklogBytes())
		fmt.Printf("rebalance_backlog_chunks: %d\n", vol.GetRebalanceBacklogChunks())
		fmt.Printf("rebalance_backlog_allocation_chunks: %d\n", vol.GetRebalanceBacklogChunks())
		fmt.Printf("rebalance_backlog_oldest_age_seconds: %d\n", vol.GetTransitionOldestFailedBatchAgeSeconds())
		fmt.Printf("rebalance_backlog_max_age_seconds: %d\n", vol.GetTransitionOldestFailedBatchAgeSeconds())
		fmt.Printf("rebalance_backlog_oscillation_window_seconds: %d\n", vol.GetMaintenanceCooldownRemainingSeconds())
		fmt.Printf("drain_backlog: %d\n", vol.GetDrainBacklog())
		fmt.Printf("drain_backlog_bytes: %d\n", vol.GetDrainBacklogBytes())
		fmt.Printf("drain_backlog_chunks: %d\n", vol.GetDrainBacklogChunks())
		fmt.Printf("drain_backlog_allocation_chunks: %d\n", vol.GetDrainBacklogChunks())
		fmt.Printf("retired_payload_backlog_bytes: %d\n", vol.GetRetiredPayloadBacklogBytes())
		fmt.Printf("retired_payload_backlog_chunks: %d\n", vol.GetRetiredPayloadBacklogChunks())
		fmt.Printf("retired_payload_backlog_allocation_chunks: %d\n", vol.GetRetiredPayloadBacklogChunks())
		fmt.Printf("transition_failed_batches: %d\n", vol.GetTransitionFailedBatches())
		fmt.Printf("transition_oldest_failed_batch_age_seconds: %d\n", vol.GetTransitionOldestFailedBatchAgeSeconds())
		fmt.Printf("transition_recent_batches: %d\n", vol.GetTransitionRecentBatches())
		fmt.Printf("transition_small_batches: %d\n", vol.GetTransitionSmallBatches())
		fmt.Printf("transition_requeued: %d\n", vol.GetTransitionRequeued())
		fmt.Printf("transition_retry_pages: %d\n", vol.GetTransitionRetryPages())
		fmt.Printf("transition_retry_windows: %d\n", vol.GetTransitionRetryWindows())
		fmt.Printf("transition_retry_window_bytes: %d\n", vol.GetTransitionRetryWindowBytes())
		fmt.Printf("transition_retry_window_chunks: %d\n", vol.GetTransitionRetryWindowChunks())
		fmt.Printf("transition_retry_window_allocation_chunks: %d\n", vol.GetTransitionRetryWindowChunks())
		fmt.Printf("maintenance_cooldown_active: %t\n", vol.GetMaintenanceCooldownActive())
		fmt.Printf("maintenance_cooldown_remaining_seconds: %d\n", vol.GetMaintenanceCooldownRemainingSeconds())
	}
}

func runVolumeReplicaTargets(args []string) {
	defaults := mustResolveCLIDefaults(args)
	fs := flag.NewFlagSet("volume replica-targets", flag.ExitOnError)
	registerContextFlags(fs, defaults)
	adminEndpoint := fs.String("admin-endpoint", defaults.adminEndpoint(), "cluster-wide sbs-admin gRPC endpoint")
	clusterID := fs.String("cluster-id", defaults.fieldValue("cluster_id", "NAMRBD_CLUSTER_ID"), "cluster id")
	sbsClusterID := fs.String("sbs-cluster-id", defaults.fieldValue("sbs_cluster_id", "SBS_CLUSTER_ID", "NAMRBD_SBS_CLUSTER_ID"), "sbs cluster id")
	volumeID := fs.String("volume-id", "", "volume id")
	output := fs.String("output", defaults.fieldValue("output", "SBS_OUTPUT", "NAMRBD_OUTPUT"), "output format: table|json")
	timeout := fs.Duration("timeout", defaults.timeout(10*time.Second), "request timeout")
	fs.Parse(args)
	if *output == "" {
		*output = "table"
	}
	printResolvedSettings(fs,
		defaults.adminEndpointSetting(),
		defaults.fieldSetting("cluster_id", "cluster-id", "", "NAMRBD_CLUSTER_ID"),
		defaults.fieldSetting("sbs_cluster_id", "sbs-cluster-id", "", "SBS_CLUSTER_ID", "NAMRBD_SBS_CLUSTER_ID"),
		defaults.fieldSetting("output", "output", "table", "SBS_OUTPUT", "NAMRBD_OUTPUT"),
		defaults.timeoutSetting(10*time.Second),
	)
	if strings.TrimSpace(*volumeID) == "" {
		fatalf("--volume-id is required")
	}
	client, ctx, cancel := dialAdmin(*adminEndpoint, *timeout)
	defer cancel()
	defer client.Close()
	resp, err := client.Admin.GetReplicaTargetsView(ctx, &adminv1.GetReplicaTargetsViewRequest{
		Cluster:  clusterRef(*clusterID, *sbsClusterID),
		VolumeId: strings.TrimSpace(*volumeID),
	})
	if err != nil {
		fatalf("volume replica-targets failed: %v", err)
	}
	switch *output {
	case "json":
		writeJSON(replicaTargetsViewJSON(resp))
	default:
		fmt.Printf("volume_id: %s\n", resp.GetVolumeId())
		fmt.Printf("revision: %d\n", resp.GetRevision())
		fmt.Printf("cache_ttl_seconds: %d\n", resp.GetCacheTtlSeconds())
		if resp.GetGeneratedAt() != nil {
			fmt.Printf("generated_at: %s\n", resp.GetGeneratedAt().AsTime().UTC().Format(time.RFC3339))
		}
		if len(resp.GetTargets()) == 0 {
			fmt.Println("targets: none")
			return
		}
		fmt.Println("targets:")
		for _, target := range resp.GetTargets() {
			endpoint := ""
			if ep := target.GetEndpoint(); ep != nil {
				endpoint = fmt.Sprintf("%s:%d", ep.GetAddress(), ep.GetPort())
			}
			fmt.Printf("  %s usable=%t priority=%d reason=%s endpoint=%s admin_http=%s\n",
				target.GetTargetId(),
				target.GetUsable(),
				target.GetPriority(),
				compactReplicaTargetReason(target.GetReasonCode()),
				endpoint,
				target.GetAdminHttpEndpoint(),
			)
		}
	}
}

func runVolumeAllocationPage(args []string) {
	defaults := mustResolveCLIDefaults(args)
	fs := flag.NewFlagSet("volume allocation-page", flag.ExitOnError)
	registerContextFlags(fs, defaults)
	adminEndpoint := fs.String("admin-endpoint", defaults.adminEndpoint(), "cluster-wide sbs-admin gRPC endpoint")
	clusterID := fs.String("cluster-id", defaults.fieldValue("cluster_id", "NAMRBD_CLUSTER_ID"), "cluster id")
	sbsClusterID := fs.String("sbs-cluster-id", defaults.fieldValue("sbs_cluster_id", "SBS_CLUSTER_ID", "NAMRBD_SBS_CLUSTER_ID"), "sbs cluster id")
	volumeID := fs.String("volume-id", "", "volume id")
	pageNo := fs.Uint64("page-no", 0, "allocation page number")
	allocationPageSize := fs.String("allocation-page-size", "", "allocation page size, e.g. 4M")
	allocationChunkSize := fs.String("allocation-chunk-size", "", "allocation chunk size, e.g. 64K")
	output := fs.String("output", defaults.fieldValue("output", "SBS_OUTPUT", "NAMRBD_OUTPUT"), "output format: table|json")
	timeout := fs.Duration("timeout", defaults.timeout(10*time.Second), "request timeout")
	fs.Parse(args)
	if *output == "" {
		*output = "table"
	}
	printResolvedSettings(fs,
		defaults.adminEndpointSetting(),
		defaults.fieldSetting("cluster_id", "cluster-id", "", "NAMRBD_CLUSTER_ID"),
		defaults.fieldSetting("sbs_cluster_id", "sbs-cluster-id", "", "SBS_CLUSTER_ID", "NAMRBD_SBS_CLUSTER_ID"),
		defaults.fieldSetting("output", "output", "table", "SBS_OUTPUT", "NAMRBD_OUTPUT"),
		defaults.timeoutSetting(10*time.Second),
	)
	if strings.TrimSpace(*volumeID) == "" {
		fatalf("--volume-id is required")
	}
	pageBytes, err := parseBinarySize(*allocationPageSize, "--allocation-page-size")
	if err != nil {
		fatalf("invalid --allocation-page-size: %v", err)
	}
	chunkSizeBytes, err := parseBinarySize(*allocationChunkSize, "--allocation-chunk-size")
	if err != nil {
		fatalf("invalid --allocation-chunk-size: %v", err)
	}
	if pageBytes > uint64(^uint32(0)) {
		fatalf("--allocation-page-size must fit uint32")
	}
	if chunkSizeBytes > uint64(^uint32(0)) {
		fatalf("--allocation-chunk-size must fit uint32")
	}

	client, ctx, cancel := dialAdmin(*adminEndpoint, *timeout)
	defer cancel()
	defer client.Close()
	resp, err := client.Admin.GetVolumeAllocationPageView(ctx, &adminv1.GetVolumeAllocationPageViewRequest{
		Cluster:        clusterRef(*clusterID, *sbsClusterID),
		VolumeId:       strings.TrimSpace(*volumeID),
		PageNo:         *pageNo,
		PageBytes:      uint32(pageBytes),
		ChunkSizeBytes: uint32(chunkSizeBytes),
	})
	if err != nil {
		fatalf("volume allocation-page failed: %v", err)
	}
	switch *output {
	case "json":
		writeJSON(resp)
	default:
		page := resp.GetAllocationPage()
		fmt.Printf("volume_id: %s\n", resp.GetVolumeId())
		fmt.Printf("revision: %d\n", resp.GetRevision())
		fmt.Printf("page_no: %d\n", page.GetPageNo())
		fmt.Printf("allocation_page_bytes: %d\n", page.GetPageBytes())
		fmt.Printf("allocation_chunk_size_bytes: %d\n", page.GetChunkSizeBytes())
		fmt.Println("extents:")
		for _, extent := range page.GetExtents() {
			fmt.Printf("  logical_chunk_start=%d chunk_count=%d kind=%s physical_chunk_start=%d backing_ref=%s generation=%d\n",
				extent.GetLogicalChunkStart(),
				extent.GetChunkCount(),
				extent.GetKind(),
				extent.GetPhysicalChunkStart(),
				extent.GetBackingRef(),
				extent.GetGeneration(),
			)
		}
	}
}

func runVolumeHealth(args []string) {
	defaults := mustResolveCLIDefaults(args)
	fs := flag.NewFlagSet("volume health", flag.ExitOnError)
	registerContextFlags(fs, defaults)
	adminEndpoint := fs.String("admin-endpoint", defaults.adminEndpoint(), "cluster-wide sbs-admin gRPC endpoint")
	clusterID := fs.String("cluster-id", defaults.fieldValue("cluster_id", "NAMRBD_CLUSTER_ID"), "cluster id")
	sbsClusterID := fs.String("sbs-cluster-id", defaults.fieldValue("sbs_cluster_id", "SBS_CLUSTER_ID", "NAMRBD_SBS_CLUSTER_ID"), "sbs cluster id")
	volumeID := fs.String("volume-id", "", "volume id")
	output := fs.String("output", defaults.fieldValue("output", "SBS_OUTPUT", "NAMRBD_OUTPUT"), "output format: table|json")
	timeout := fs.Duration("timeout", defaults.timeout(10*time.Second), "request timeout")
	fs.Parse(args)
	if *output == "" {
		*output = "table"
	}
	printResolvedSettings(fs,
		defaults.adminEndpointSetting(),
		defaults.fieldSetting("cluster_id", "cluster-id", "", "NAMRBD_CLUSTER_ID"),
		defaults.fieldSetting("sbs_cluster_id", "sbs-cluster-id", "", "SBS_CLUSTER_ID", "NAMRBD_SBS_CLUSTER_ID"),
		defaults.fieldSetting("output", "output", "table", "SBS_OUTPUT", "NAMRBD_OUTPUT"),
		defaults.timeoutSetting(10*time.Second),
	)
	if *volumeID == "" {
		fatalf("--volume-id is required")
	}

	client, ctx, cancel := dialAdmin(*adminEndpoint, *timeout)
	defer cancel()
	defer client.Close()
	resp, err := client.Admin.GetVolume(ctx, &adminv1.GetVolumeRequest{
		Cluster:  clusterRef(*clusterID, *sbsClusterID),
		VolumeId: *volumeID,
	})
	if err != nil {
		fatalf("volume health failed: %v", err)
	}
	switch *output {
	case "json":
		writeJSON(map[string]any{
			"volume_id":       resp.GetVolume().GetVolumeId(),
			"health":          resp.GetVolume().GetHealth().String(),
			"volume_revision": resp.GetVolume().GetVolumeRevision(),
		})
	default:
		vol := resp.GetVolume()
		fmt.Printf("volume_id: %s\n", vol.GetVolumeId())
		fmt.Printf("health: %s\n", vol.GetHealth().String())
		fmt.Printf("volume_revision: %d\n", vol.GetVolumeRevision())
	}
}

func runVolumePlacement(args []string) {
	defaults := mustResolveCLIDefaults(args)
	fs := flag.NewFlagSet("volume placement", flag.ExitOnError)
	registerContextFlags(fs, defaults)
	adminEndpoint := fs.String("admin-endpoint", defaults.adminEndpoint(), "cluster-wide sbs-admin gRPC endpoint")
	adminHTTP := fs.String("admin-http-endpoint", defaults.fieldValue("sbs_node_admin_http", "SBS_NODE_ADMIN_HTTP", "NAMRBD_SBS_ADMIN_ADDR"), "optional node-local admin/debug HTTP endpoint for current placement rows")
	clusterID := fs.String("cluster-id", defaults.fieldValue("cluster_id", "NAMRBD_CLUSTER_ID"), "cluster id")
	sbsClusterID := fs.String("sbs-cluster-id", defaults.fieldValue("sbs_cluster_id", "SBS_CLUSTER_ID", "NAMRBD_SBS_CLUSTER_ID"), "sbs cluster id")
	volumeID := fs.String("volume-id", "", "volume id")
	output := fs.String("output", defaults.fieldValue("output", "SBS_OUTPUT", "NAMRBD_OUTPUT"), "output format: table|json")
	timeout := fs.Duration("timeout", defaults.timeout(10*time.Second), "request timeout")
	fs.Parse(args)
	if *output == "" {
		*output = "table"
	}
	printResolvedSettings(fs,
		defaults.adminEndpointSetting(),
		defaults.fieldSetting("sbs_node_admin_http", "admin-http-endpoint", "", "SBS_NODE_ADMIN_HTTP", "NAMRBD_SBS_ADMIN_ADDR"),
		defaults.fieldSetting("cluster_id", "cluster-id", "", "NAMRBD_CLUSTER_ID"),
		defaults.fieldSetting("sbs_cluster_id", "sbs-cluster-id", "", "SBS_CLUSTER_ID", "NAMRBD_SBS_CLUSTER_ID"),
		defaults.fieldSetting("output", "output", "table", "SBS_OUTPUT", "NAMRBD_OUTPUT"),
		defaults.timeoutSetting(10*time.Second),
	)
	if *volumeID == "" {
		fatalf("--volume-id is required")
	}

	client, ctx, cancel := dialAdmin(*adminEndpoint, *timeout)
	defer cancel()
	defer client.Close()

	repairs, err := client.Admin.ListRepairs(ctx, &adminv1.ListRepairsRequest{
		Cluster: clusterRef(*clusterID, *sbsClusterID),
	})
	if err != nil {
		fatalf("list repairs: %v", err)
	}
	rebalances, err := client.Admin.ListRebalances(ctx, &adminv1.ListRebalancesRequest{
		Cluster: clusterRef(*clusterID, *sbsClusterID),
	})
	if err != nil {
		fatalf("list rebalances: %v", err)
	}

	type row struct {
		Kind                string `json:"kind"`
		VolumeID            string `json:"volume_id"`
		PlacementRef        string `json:"placement_ref"`
		State               string `json:"state"`
		CurrentReplicaSetID string `json:"current_replica_set_id"`
		TargetReplicaSetID  string `json:"target_replica_set_id"`
	}
	var rows []row
	for _, item := range repairs.GetRepairs() {
		if item.GetVolumeId() != *volumeID {
			continue
		}
		rows = append(rows, row{
			Kind:                "repair",
			VolumeID:            item.GetVolumeId(),
			PlacementRef:        item.GetPlacementRef(),
			State:               item.GetState(),
			CurrentReplicaSetID: item.GetCurrentReplicaSetId(),
			TargetReplicaSetID:  item.GetTargetReplicaSetId(),
		})
	}
	for _, item := range rebalances.GetRebalances() {
		if item.GetVolumeId() != *volumeID {
			continue
		}
		rows = append(rows, row{
			Kind:                "rebalance",
			VolumeID:            item.GetVolumeId(),
			PlacementRef:        item.GetPlacementRef(),
			State:               item.GetState(),
			CurrentReplicaSetID: item.GetCurrentReplicaSetId(),
			TargetReplicaSetID:  item.GetTargetReplicaSetId(),
		})
	}
	currentPlacements := []map[string]any{}
	if strings.TrimSpace(*adminHTTP) != "" {
		placements, err := runVolumeCurrentPlacementRemote(strings.TrimSpace(*adminHTTP), *volumeID, *timeout)
		if err != nil {
			fatalf("current placement: %v", err)
		}
		currentPlacements = placements
	}

	switch *output {
	case "json":
		writeJSON(map[string]any{
			"volume_id":          *volumeID,
			"current_placements": currentPlacements,
			"transition_rows":    rows,
			"rows":               rows,
		})
	default:
		if len(currentPlacements) > 0 {
			fmt.Println("CURRENT_PLACEMENT\tEXTENT_ID\tREPLICA_SET\tREPLICA_NODES")
			for _, r := range currentPlacements {
				fmt.Printf("%s\t%d\t%s\t%s\n",
					transitionAnyString(r["placement_ref"]),
					transitionAnyUint64(r["extent_id"]),
					transitionAnyString(r["replica_set_id"]),
					transitionAnyString(r["replica_nodes"]),
				)
			}
		}
		if len(rows) == 0 {
			fmt.Printf("no placement transitions for volume_id=%s (no active repair/rebalance rows)\n", *volumeID)
			return
		}
		for _, r := range rows {
			fmt.Printf("kind: %s volume_id: %s placement_ref: %s state: %s current: %s target: %s\n",
				r.Kind, r.VolumeID, r.PlacementRef, r.State, r.CurrentReplicaSetID, r.TargetReplicaSetID)
		}
	}
}

func runVolumeTransitions(args []string) {
	defaults := mustResolveCLIDefaults(args)
	fs := flag.NewFlagSet("volume transitions", flag.ExitOnError)
	registerContextFlags(fs, defaults)
	adminHTTP := fs.String("admin-http-endpoint", defaults.fieldValue("sbs_node_admin_http", "SBS_NODE_ADMIN_HTTP", "NAMRBD_SBS_ADMIN_ADDR"), "node-local admin/debug HTTP endpoint")
	volumeID := fs.String("volume-id", "", "volume id")
	output := fs.String("output", defaults.fieldValue("output", "SBS_OUTPUT", "NAMRBD_OUTPUT"), "output format: table|json")
	timeout := fs.Duration("timeout", defaults.timeout(10*time.Second), "request timeout")
	fs.Parse(args)
	if *output == "" {
		*output = "table"
	}
	printResolvedSettings(fs,
		defaults.fieldSetting("sbs_node_admin_http", "admin-http-endpoint", "", "SBS_NODE_ADMIN_HTTP", "NAMRBD_SBS_ADMIN_ADDR"),
		defaults.fieldSetting("output", "output", "table", "SBS_OUTPUT", "NAMRBD_OUTPUT"),
		defaults.timeoutSetting(10*time.Second),
	)
	if strings.TrimSpace(*adminHTTP) == "" {
		fatalf("--admin-http-endpoint is required")
	}
	if *volumeID == "" {
		fatalf("--volume-id is required")
	}

	transitions, err := runVolumeTransitionsRemote(strings.TrimSpace(*adminHTTP), strings.TrimSpace(*volumeID), *timeout)
	if err != nil {
		fatalf("volume transitions failed: %v", err)
	}

	switch strings.ToLower(strings.TrimSpace(*output)) {
	case "json":
		writeJSON(transitions)
	case "", "table":
		rows, _ := transitions["transitions"].([]any)
		if len(rows) == 0 {
			fmt.Printf("no raw placement transitions for volume_id=%s\n", *volumeID)
			return
		}
		fmt.Println("PLACEMENT_REF\tSTATE\tACTIVE\tVISIBLE\tREASON\tCURRENT\tTARGET\tATTEMPT")
		for _, raw := range rows {
			row, _ := raw.(map[string]any)
			fmt.Printf("%s\t%s\t%t\t%t\t%s\t%s\t%s\t%d\n",
				transitionAnyString(row["placement_ref"]),
				transitionAnyString(row["state"]),
				transitionAnyBool(row["active"]),
				transitionAnyBool(row["visible"]),
				transitionAnyString(row["reason"]),
				transitionAnyString(row["current_replica_set_id"]),
				transitionAnyString(row["target_replica_set_id"]),
				transitionAnyUint64(row["attempt"]),
			)
		}
	default:
		fatalf("unsupported output format %q", *output)
	}
}

func transitionAnyString(v any) string {
	s, _ := v.(string)
	return s
}

func transitionAnyBool(v any) bool {
	b, _ := v.(bool)
	return b
}

func transitionAnyUint64(v any) uint64 {
	switch n := v.(type) {
	case uint64:
		return n
	case uint32:
		return uint64(n)
	case int:
		if n < 0 {
			return 0
		}
		return uint64(n)
	case float64:
		if n < 0 {
			return 0
		}
		return uint64(n)
	default:
		return 0
	}
}

func storeAllocationWeightForOutput(store map[string]any) uint64 {
	if _, ok := store["allocation_weight"]; ok {
		return transitionAnyUint64(store["allocation_weight"])
	}
	return transitionAnyUint64(store["weight"])
}

func runVolumeTransitionsRemote(adminHTTP, volumeID string, timeout time.Duration) (map[string]any, error) {
	client := &http.Client{Timeout: timeout}
	return runVolumeTransitionsRemoteWithClient(context.Background(), client, adminHTTP, volumeID)
}

func runStoreStatusRemote(adminHTTP string, timeout time.Duration) (map[string]any, error) {
	client := &http.Client{Timeout: timeout}
	return runStoreStatusRemoteWithClient(context.Background(), client, adminHTTP)
}

func runVolumeCurrentPlacementRemote(adminHTTP, volumeID string, timeout time.Duration) ([]map[string]any, error) {
	client := &http.Client{Timeout: timeout}
	return runVolumeCurrentPlacementRemoteWithClient(context.Background(), client, adminHTTP, volumeID)
}

func runStoreStatusRemoteWithClient(ctx context.Context, client *http.Client, adminHTTP string) (map[string]any, error) {
	if client == nil {
		client = http.DefaultClient
	}
	reqURL := strings.TrimRight(adminHTTP, "/") + "/debug/summary"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL, nil)
	if err != nil {
		return nil, err
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		raw, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		msg := strings.TrimSpace(string(raw))
		if msg != "" {
			return nil, fmt.Errorf("admin http %s: %s", resp.Status, msg)
		}
		return nil, fmt.Errorf("admin http %s", resp.Status)
	}
	var out map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, fmt.Errorf("decode summary response: %w", err)
	}
	out["process_state"] = "healthy"
	normalizeStoreStatusAllocationWeights(out)
	return out, nil
}

func normalizeStoreStatusAllocationWeights(summary map[string]any) {
	stores, _ := summary["stores"].([]any)
	for _, raw := range stores {
		store, _ := raw.(map[string]any)
		if store == nil {
			continue
		}
		if _, ok := store["allocation_weight"]; ok {
			continue
		}
		if weight, ok := store["weight"]; ok {
			store["allocation_weight"] = weight
		}
	}
}

func runVolumeCurrentPlacementRemoteWithClient(ctx context.Context, client *http.Client, adminHTTP, volumeID string) ([]map[string]any, error) {
	if client == nil {
		client = http.DefaultClient
	}
	reqURL := strings.TrimRight(adminHTTP, "/") + "/debug/volume?volume_id=" + url.QueryEscape(volumeID)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL, nil)
	if err != nil {
		return nil, err
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		raw, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		msg := strings.TrimSpace(string(raw))
		if msg != "" {
			return nil, fmt.Errorf("admin http %s: %s", resp.Status, msg)
		}
		return nil, fmt.Errorf("admin http %s", resp.Status)
	}
	var payload map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return nil, fmt.Errorf("decode debug volume response: %w", err)
	}
	volume, _ := payload["volume"].(map[string]any)
	if volume == nil {
		return nil, fmt.Errorf("debug volume response missing volume object")
	}
	extentRows, _ := volume["extents"].([]any)
	replicaSetRows, _ := volume["replica_sets"].([]any)
	replicaSets := make(map[string]map[string]any, len(replicaSetRows))
	for _, raw := range replicaSetRows {
		row, _ := raw.(map[string]any)
		if row == nil {
			continue
		}
		placementRef := transitionAnyString(row["placement_ref"])
		if placementRef == "" {
			continue
		}
		replicaSets[placementRef] = row
	}
	out := make([]map[string]any, 0, len(extentRows))
	for _, raw := range extentRows {
		extent, _ := raw.(map[string]any)
		if extent == nil {
			continue
		}
		placementRef := transitionAnyString(extent["placement_ref"])
		replicaSet := replicaSets[placementRef]
		replicas, _ := replicaSet["replicas"].([]any)
		nodes := make([]string, 0, len(replicas))
		for _, rawReplica := range replicas {
			replica, _ := rawReplica.(map[string]any)
			if replica == nil {
				continue
			}
			nodeID := transitionAnyString(replica["node_id"])
			if nodeID != "" {
				nodes = append(nodes, nodeID)
			}
		}
		out = append(out, map[string]any{
			"volume_id":      transitionAnyString(extent["volume_id"]),
			"extent_id":      transitionAnyUint64(extent["extent_id"]),
			"placement_ref":  placementRef,
			"replica_set_id": transitionAnyString(replicaSet["replica_set_id"]),
			"replicas":       replicas,
			"replica_nodes":  strings.Join(nodes, ","),
		})
	}
	return out, nil
}

func runVolumeTransitionsRemoteWithClient(ctx context.Context, client *http.Client, adminHTTP, volumeID string) (map[string]any, error) {
	if client == nil {
		client = http.DefaultClient
	}
	reqURL := strings.TrimRight(adminHTTP, "/") + "/debug/transitions?volume_id=" + url.QueryEscape(volumeID)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL, nil)
	if err != nil {
		return nil, err
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		raw, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		msg := strings.TrimSpace(string(raw))
		if msg != "" {
			return nil, fmt.Errorf("admin http %s: %s", resp.Status, msg)
		}
		return nil, fmt.Errorf("admin http %s", resp.Status)
	}
	var out map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, fmt.Errorf("decode transitions response: %w", err)
	}
	return out, nil
}

func runVolumeList(args []string) {
	defaults := mustResolveCLIDefaults(args)
	fs := flag.NewFlagSet("volume list", flag.ExitOnError)
	registerContextFlags(fs, defaults)
	adminEndpoint := fs.String("admin-endpoint", defaults.adminEndpoint(), "cluster-wide sbs-admin gRPC endpoint")
	clusterID := fs.String("cluster-id", defaults.fieldValue("cluster_id", "NAMRBD_CLUSTER_ID"), "cluster id")
	sbsClusterID := fs.String("sbs-cluster-id", defaults.fieldValue("sbs_cluster_id", "SBS_CLUSTER_ID", "NAMRBD_SBS_CLUSTER_ID"), "sbs cluster id")
	output := fs.String("output", defaults.fieldValue("output", "SBS_OUTPUT", "NAMRBD_OUTPUT"), "output format: table|json")
	timeout := fs.Duration("timeout", defaults.timeout(10*time.Second), "request timeout")
	fs.Parse(args)
	if *output == "" {
		*output = "table"
	}
	printResolvedSettings(fs,
		defaults.adminEndpointSetting(),
		defaults.fieldSetting("cluster_id", "cluster-id", "", "NAMRBD_CLUSTER_ID"),
		defaults.fieldSetting("sbs_cluster_id", "sbs-cluster-id", "", "SBS_CLUSTER_ID", "NAMRBD_SBS_CLUSTER_ID"),
		defaults.fieldSetting("output", "output", "table", "SBS_OUTPUT", "NAMRBD_OUTPUT"),
		defaults.timeoutSetting(10*time.Second),
	)

	client, ctx, cancel := dialAdmin(*adminEndpoint, *timeout)
	defer cancel()
	defer client.Close()
	resp, err := client.Admin.ListVolumes(ctx, &adminv1.ListVolumesRequest{
		Cluster: clusterRef(*clusterID, *sbsClusterID),
	})
	if err != nil {
		fatalf("volume list failed: %v", err)
	}
	switch *output {
	case "json":
		writeJSON(resp)
	default:
		for _, vol := range resp.GetVolumes() {
			fmt.Printf("volume_id: %s size_bytes: %d block_size: %d health: %s revision: %d\n",
				vol.GetVolumeId(), vol.GetSizeBytes(), vol.GetBlockSize(), vol.GetHealth().String(), vol.GetVolumeRevision())
		}
		if len(resp.GetVolumes()) == 0 {
			fmt.Println("no volumes")
		}
	}
}

func runOperationShow(args []string) {
	defaults := mustResolveCLIDefaults(args)
	fs := flag.NewFlagSet("operations show", flag.ExitOnError)
	registerContextFlags(fs, defaults)
	adminEndpoint := fs.String("admin-endpoint", defaults.adminEndpoint(), "cluster-wide sbs-admin gRPC endpoint")
	clusterID := fs.String("cluster-id", defaults.fieldValue("cluster_id", "NAMRBD_CLUSTER_ID"), "cluster id")
	sbsClusterID := fs.String("sbs-cluster-id", defaults.fieldValue("sbs_cluster_id", "SBS_CLUSTER_ID", "NAMRBD_SBS_CLUSTER_ID"), "sbs cluster id")
	operationID := fs.String("operation-id", "", "operation id")
	output := fs.String("output", defaults.fieldValue("output", "SBS_OUTPUT", "NAMRBD_OUTPUT"), "output format: table|json")
	timeout := fs.Duration("timeout", defaults.timeout(10*time.Second), "request timeout")
	fs.Parse(args)
	if *output == "" {
		*output = "table"
	}
	printResolvedSettings(fs,
		defaults.adminEndpointSetting(),
		defaults.fieldSetting("cluster_id", "cluster-id", "", "NAMRBD_CLUSTER_ID"),
		defaults.fieldSetting("sbs_cluster_id", "sbs-cluster-id", "", "SBS_CLUSTER_ID", "NAMRBD_SBS_CLUSTER_ID"),
		defaults.fieldSetting("output", "output", "table", "SBS_OUTPUT", "NAMRBD_OUTPUT"),
		defaults.timeoutSetting(10*time.Second),
	)
	if *operationID == "" {
		fatalf("--operation-id is required")
	}

	client, ctx, cancel := dialAdmin(*adminEndpoint, *timeout)
	defer cancel()
	defer client.Close()

	resp, err := client.Operations.GetOperation(ctx, &adminv1.GetOperationRequest{
		Cluster:     clusterRef(*clusterID, *sbsClusterID),
		OperationId: *operationID,
	})
	if err != nil {
		fatalf("operations show failed: %v", err)
	}

	switch *output {
	case "json":
		writeJSON(resp)
	default:
		op := resp.GetOperation()
		fmt.Printf("operation_id: %s\n", op.GetOperationId())
		fmt.Printf("kind: %s\n", op.GetKind())
		fmt.Printf("state: %s\n", op.GetState().String())
		fmt.Printf("target_node_id: %s\n", op.GetTargetNodeId())
		fmt.Printf("target_volume_id: %s\n", op.GetTargetVolumeId())
		fmt.Printf("phase: %s\n", op.GetPhase())
		fmt.Printf("blocking_reason: %s\n", op.GetBlockingReason())
		fmt.Printf("error_message: %s\n", op.GetErrorMessage())
		if ts := op.GetStartedAt(); ts != nil {
			fmt.Printf("started_at: %s\n", ts.AsTime().UTC().Format(time.RFC3339))
		}
		if ts := op.GetLastProgressAt(); ts != nil {
			fmt.Printf("last_progress_at: %s\n", ts.AsTime().UTC().Format(time.RFC3339))
		}
	}
}

func runOperationList(args []string) {
	defaults := mustResolveCLIDefaults(args)
	fs := flag.NewFlagSet("operations list", flag.ExitOnError)
	registerContextFlags(fs, defaults)
	adminEndpoint := fs.String("admin-endpoint", defaults.adminEndpoint(), "cluster-wide sbs-admin gRPC endpoint")
	clusterID := fs.String("cluster-id", defaults.fieldValue("cluster_id", "NAMRBD_CLUSTER_ID"), "cluster id")
	sbsClusterID := fs.String("sbs-cluster-id", defaults.fieldValue("sbs_cluster_id", "SBS_CLUSTER_ID", "NAMRBD_SBS_CLUSTER_ID"), "sbs cluster id")
	kind := fs.String("kind", "", "optional operation kind filter")
	state := fs.String("state", "", "optional state filter: queued|running|completed|failed|canceled")
	output := fs.String("output", defaults.fieldValue("output", "SBS_OUTPUT", "NAMRBD_OUTPUT"), "output format: table|json")
	timeout := fs.Duration("timeout", defaults.timeout(10*time.Second), "request timeout")
	fs.Parse(args)
	if *output == "" {
		*output = "table"
	}
	printResolvedSettings(fs,
		defaults.adminEndpointSetting(),
		defaults.fieldSetting("cluster_id", "cluster-id", "", "NAMRBD_CLUSTER_ID"),
		defaults.fieldSetting("sbs_cluster_id", "sbs-cluster-id", "", "SBS_CLUSTER_ID", "NAMRBD_SBS_CLUSTER_ID"),
		defaults.fieldSetting("output", "output", "table", "SBS_OUTPUT", "NAMRBD_OUTPUT"),
		defaults.timeoutSetting(10*time.Second),
	)

	parsedState, err := parseOperationStateFilter(*state)
	if err != nil {
		fatalf("operations list failed: %v", err)
	}

	client, ctx, cancel := dialAdmin(*adminEndpoint, *timeout)
	defer cancel()
	defer client.Close()

	resp, err := client.Operations.ListOperations(ctx, &adminv1.ListOperationsRequest{
		Cluster: clusterRef(*clusterID, *sbsClusterID),
		Kind:    strings.TrimSpace(*kind),
		State:   parsedState,
	})
	if err != nil {
		fatalf("operations list failed: %v", err)
	}

	switch strings.ToLower(strings.TrimSpace(*output)) {
	case "json":
		writeJSON(resp)
	case "", "table":
		fmt.Println("OPERATION\tKIND\tSTATE\tVOLUME\tNODE\tPHASE")
		for _, op := range resp.GetOperations() {
			fmt.Printf("%s\t%s\t%s\t%s\t%s\t%s\n",
				op.GetOperationId(),
				op.GetKind(),
				op.GetState().String(),
				op.GetTargetVolumeId(),
				op.GetTargetNodeId(),
				op.GetPhase(),
			)
		}
		if len(resp.GetOperations()) == 0 {
			fmt.Println("no operations")
		}
	default:
		fatalf("unsupported output format %q", *output)
	}
}

func parseOperationStateFilter(raw string) (adminv1.OperationState, error) {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "", "all":
		return adminv1.OperationState_OPERATION_STATE_UNSPECIFIED, nil
	case "queued":
		return adminv1.OperationState_OPERATION_STATE_QUEUED, nil
	case "running":
		return adminv1.OperationState_OPERATION_STATE_RUNNING, nil
	case "completed":
		return adminv1.OperationState_OPERATION_STATE_COMPLETED, nil
	case "failed":
		return adminv1.OperationState_OPERATION_STATE_FAILED, nil
	case "canceled", "cancelled":
		return adminv1.OperationState_OPERATION_STATE_CANCELED, nil
	default:
		return adminv1.OperationState_OPERATION_STATE_UNSPECIFIED, fmt.Errorf("unsupported operation state %q", raw)
	}
}

func runRepairList(args []string) {
	defaults := mustResolveCLIDefaults(args)
	fs := flag.NewFlagSet("repair list", flag.ExitOnError)
	registerContextFlags(fs, defaults)
	adminEndpoint := fs.String("admin-endpoint", defaults.adminEndpoint(), "cluster-wide sbs-admin gRPC endpoint")
	clusterID := fs.String("cluster-id", defaults.fieldValue("cluster_id", "NAMRBD_CLUSTER_ID"), "cluster id")
	sbsClusterID := fs.String("sbs-cluster-id", defaults.fieldValue("sbs_cluster_id", "SBS_CLUSTER_ID", "NAMRBD_SBS_CLUSTER_ID"), "sbs cluster id")
	output := fs.String("output", defaults.fieldValue("output", "SBS_OUTPUT", "NAMRBD_OUTPUT"), "output format: table|json")
	timeout := fs.Duration("timeout", defaults.timeout(10*time.Second), "request timeout")
	fs.Parse(args)
	if *output == "" {
		*output = "table"
	}
	printResolvedSettings(fs,
		defaults.adminEndpointSetting(),
		defaults.fieldSetting("cluster_id", "cluster-id", "", "NAMRBD_CLUSTER_ID"),
		defaults.fieldSetting("sbs_cluster_id", "sbs-cluster-id", "", "SBS_CLUSTER_ID", "NAMRBD_SBS_CLUSTER_ID"),
		defaults.fieldSetting("output", "output", "table", "SBS_OUTPUT", "NAMRBD_OUTPUT"),
		defaults.timeoutSetting(10*time.Second),
	)

	client, ctx, cancel := dialAdmin(*adminEndpoint, *timeout)
	defer cancel()
	defer client.Close()
	resp, err := client.Admin.ListRepairs(ctx, &adminv1.ListRepairsRequest{
		Cluster: clusterRef(*clusterID, *sbsClusterID),
	})
	if err != nil {
		fatalf("repair list failed: %v", err)
	}
	switch *output {
	case "json":
		writeJSON(resp)
	default:
		for _, item := range resp.GetRepairs() {
			fmt.Printf("volume_id: %s placement_ref: %s state: %s current: %s target: %s\n",
				item.GetVolumeId(), item.GetPlacementRef(), item.GetState(), item.GetCurrentReplicaSetId(), item.GetTargetReplicaSetId())
		}
		if len(resp.GetRepairs()) == 0 {
			fmt.Println("no repairs")
		}
	}
}

func runRepairShow(args []string) {
	defaults := mustResolveCLIDefaults(args)
	fs := flag.NewFlagSet("repair show", flag.ExitOnError)
	registerContextFlags(fs, defaults)
	adminEndpoint := fs.String("admin-endpoint", defaults.adminEndpoint(), "cluster-wide sbs-admin gRPC endpoint")
	clusterID := fs.String("cluster-id", defaults.fieldValue("cluster_id", "NAMRBD_CLUSTER_ID"), "cluster id")
	sbsClusterID := fs.String("sbs-cluster-id", defaults.fieldValue("sbs_cluster_id", "SBS_CLUSTER_ID", "NAMRBD_SBS_CLUSTER_ID"), "sbs cluster id")
	volumeID := fs.String("volume-id", "", "volume id")
	placementRef := fs.String("placement-ref", "", "optional placement ref filter")
	output := fs.String("output", defaults.fieldValue("output", "SBS_OUTPUT", "NAMRBD_OUTPUT"), "output format: table|json")
	timeout := fs.Duration("timeout", defaults.timeout(10*time.Second), "request timeout")
	fs.Parse(args)
	if *output == "" {
		*output = "table"
	}
	printResolvedSettings(fs,
		defaults.adminEndpointSetting(),
		defaults.fieldSetting("cluster_id", "cluster-id", "", "NAMRBD_CLUSTER_ID"),
		defaults.fieldSetting("sbs_cluster_id", "sbs-cluster-id", "", "SBS_CLUSTER_ID", "NAMRBD_SBS_CLUSTER_ID"),
		defaults.fieldSetting("output", "output", "table", "SBS_OUTPUT", "NAMRBD_OUTPUT"),
		defaults.timeoutSetting(10*time.Second),
	)
	if *volumeID == "" {
		fatalf("--volume-id is required")
	}

	client, ctx, cancel := dialAdmin(*adminEndpoint, *timeout)
	defer cancel()
	defer client.Close()
	resp, err := client.Admin.ListRepairs(ctx, &adminv1.ListRepairsRequest{
		Cluster: clusterRef(*clusterID, *sbsClusterID),
	})
	if err != nil {
		fatalf("repair show failed: %v", err)
	}
	var matched []*adminv1.RepairSummary
	for _, item := range resp.GetRepairs() {
		if item.GetVolumeId() != *volumeID {
			continue
		}
		if *placementRef != "" && item.GetPlacementRef() != *placementRef {
			continue
		}
		matched = append(matched, item)
	}
	switch *output {
	case "json":
		writeJSON(map[string]any{"repairs": matched})
	default:
		if len(matched) == 0 {
			fmt.Println("no matching repairs")
			return
		}
		for _, item := range matched {
			fmt.Printf("volume_id: %s placement_ref: %s state: %s current: %s target: %s\n",
				item.GetVolumeId(), item.GetPlacementRef(), item.GetState(), item.GetCurrentReplicaSetId(), item.GetTargetReplicaSetId())
		}
	}
}

func runRebalanceList(args []string) {
	defaults := mustResolveCLIDefaults(args)
	fs := flag.NewFlagSet("rebalance list", flag.ExitOnError)
	registerContextFlags(fs, defaults)
	adminEndpoint := fs.String("admin-endpoint", defaults.adminEndpoint(), "cluster-wide sbs-admin gRPC endpoint")
	clusterID := fs.String("cluster-id", defaults.fieldValue("cluster_id", "NAMRBD_CLUSTER_ID"), "cluster id")
	sbsClusterID := fs.String("sbs-cluster-id", defaults.fieldValue("sbs_cluster_id", "SBS_CLUSTER_ID", "NAMRBD_SBS_CLUSTER_ID"), "sbs cluster id")
	output := fs.String("output", defaults.fieldValue("output", "SBS_OUTPUT", "NAMRBD_OUTPUT"), "output format: table|json")
	timeout := fs.Duration("timeout", defaults.timeout(10*time.Second), "request timeout")
	fs.Parse(args)
	if *output == "" {
		*output = "table"
	}
	printResolvedSettings(fs,
		defaults.adminEndpointSetting(),
		defaults.fieldSetting("cluster_id", "cluster-id", "", "NAMRBD_CLUSTER_ID"),
		defaults.fieldSetting("sbs_cluster_id", "sbs-cluster-id", "", "SBS_CLUSTER_ID", "NAMRBD_SBS_CLUSTER_ID"),
		defaults.fieldSetting("output", "output", "table", "SBS_OUTPUT", "NAMRBD_OUTPUT"),
		defaults.timeoutSetting(10*time.Second),
	)

	client, ctx, cancel := dialAdmin(*adminEndpoint, *timeout)
	defer cancel()
	defer client.Close()
	resp, err := client.Admin.ListRebalances(ctx, &adminv1.ListRebalancesRequest{
		Cluster: clusterRef(*clusterID, *sbsClusterID),
	})
	if err != nil {
		fatalf("rebalance list failed: %v", err)
	}
	switch *output {
	case "json":
		writeJSON(resp)
	default:
		for _, item := range resp.GetRebalances() {
			fmt.Printf("volume_id: %s placement_ref: %s state: %s current: %s target: %s\n",
				item.GetVolumeId(), item.GetPlacementRef(), item.GetState(), item.GetCurrentReplicaSetId(), item.GetTargetReplicaSetId())
		}
		if len(resp.GetRebalances()) == 0 {
			fmt.Println("no rebalances")
		}
	}
}

func runMaintenanceThrottle(args []string) {
	defaults := mustResolveCLIDefaults(args)
	fs := flag.NewFlagSet("maintenance throttle", flag.ExitOnError)
	registerContextFlags(fs, defaults)
	adminEndpoint := fs.String("admin-endpoint", defaults.adminEndpoint(), "cluster-wide sbs-admin gRPC endpoint")
	clusterID := fs.String("cluster-id", defaults.fieldValue("cluster_id", "NAMRBD_CLUSTER_ID"), "cluster id")
	sbsClusterID := fs.String("sbs-cluster-id", defaults.fieldValue("sbs_cluster_id", "SBS_CLUSTER_ID", "NAMRBD_SBS_CLUSTER_ID"), "sbs cluster id")
	repairs := fs.Uint("repairs", 0, "max concurrent repairs")
	rebalances := fs.Uint("rebalances", 0, "max concurrent rebalances")
	drains := fs.Uint("drains", 0, "max concurrent drains")
	actor := fs.String("actor", getenvOrDefault("USER", "unknown"), "actor")
	reason := fs.String("reason", "maintenance-throttle", "reason")
	timeout := fs.Duration("timeout", defaults.timeout(10*time.Second), "request timeout")
	fs.Parse(args)
	printResolvedSettings(fs,
		defaults.adminEndpointSetting(),
		defaults.fieldSetting("cluster_id", "cluster-id", "", "NAMRBD_CLUSTER_ID"),
		defaults.fieldSetting("sbs_cluster_id", "sbs-cluster-id", "", "SBS_CLUSTER_ID", "NAMRBD_SBS_CLUSTER_ID"),
		defaults.timeoutSetting(10*time.Second),
	)

	client, ctx, cancel := dialAdmin(*adminEndpoint, *timeout)
	defer cancel()
	defer client.Close()
	resp, err := client.Admin.SetMaintenanceThrottle(ctx, &adminv1.SetMaintenanceThrottleRequest{
		Cluster:                 clusterRef(*clusterID, *sbsClusterID),
		Meta:                    &adminv1.RequestMeta{Actor: *actor, Reason: *reason},
		MaxConcurrentRepairs:    uint32(*repairs),
		MaxConcurrentRebalances: uint32(*rebalances),
		MaxConcurrentDrains:     uint32(*drains),
	})
	if err != nil {
		fatalf("maintenance throttle failed: %v", err)
	}
	writeJSON(resp)
}

func runMaintenancePause(args []string) {
	defaults := mustResolveCLIDefaults(args)
	fs := flag.NewFlagSet("maintenance pause", flag.ExitOnError)
	registerContextFlags(fs, defaults)
	adminEndpoint := fs.String("admin-endpoint", defaults.adminEndpoint(), "cluster-wide sbs-admin gRPC endpoint")
	clusterID := fs.String("cluster-id", defaults.fieldValue("cluster_id", "NAMRBD_CLUSTER_ID"), "cluster id")
	sbsClusterID := fs.String("sbs-cluster-id", defaults.fieldValue("sbs_cluster_id", "SBS_CLUSTER_ID", "NAMRBD_SBS_CLUSTER_ID"), "sbs cluster id")
	repairs := fs.Bool("repairs", false, "pause repairs")
	rebalances := fs.Bool("rebalances", false, "pause rebalances")
	drains := fs.Bool("drains", false, "pause drains")
	actor := fs.String("actor", getenvOrDefault("USER", "unknown"), "actor")
	reason := fs.String("reason", "maintenance-pause", "reason")
	timeout := fs.Duration("timeout", defaults.timeout(10*time.Second), "request timeout")
	fs.Parse(args)
	printResolvedSettings(fs,
		defaults.adminEndpointSetting(),
		defaults.fieldSetting("cluster_id", "cluster-id", "", "NAMRBD_CLUSTER_ID"),
		defaults.fieldSetting("sbs_cluster_id", "sbs-cluster-id", "", "SBS_CLUSTER_ID", "NAMRBD_SBS_CLUSTER_ID"),
		defaults.timeoutSetting(10*time.Second),
	)

	client, ctx, cancel := dialAdmin(*adminEndpoint, *timeout)
	defer cancel()
	defer client.Close()
	resp, err := client.Admin.PauseMaintenance(ctx, &adminv1.PauseMaintenanceRequest{
		Cluster:         clusterRef(*clusterID, *sbsClusterID),
		Meta:            &adminv1.RequestMeta{Actor: *actor, Reason: *reason},
		PauseRepairs:    *repairs,
		PauseRebalances: *rebalances,
		PauseDrains:     *drains,
	})
	if err != nil {
		fatalf("maintenance pause failed: %v", err)
	}
	writeJSON(resp)
}

func runMaintenanceResume(args []string) {
	defaults := mustResolveCLIDefaults(args)
	fs := flag.NewFlagSet("maintenance resume", flag.ExitOnError)
	registerContextFlags(fs, defaults)
	adminEndpoint := fs.String("admin-endpoint", defaults.adminEndpoint(), "cluster-wide sbs-admin gRPC endpoint")
	clusterID := fs.String("cluster-id", defaults.fieldValue("cluster_id", "NAMRBD_CLUSTER_ID"), "cluster id")
	sbsClusterID := fs.String("sbs-cluster-id", defaults.fieldValue("sbs_cluster_id", "SBS_CLUSTER_ID", "NAMRBD_SBS_CLUSTER_ID"), "sbs cluster id")
	repairs := fs.Bool("repairs", false, "resume repairs")
	rebalances := fs.Bool("rebalances", false, "resume rebalances")
	drains := fs.Bool("drains", false, "resume drains")
	actor := fs.String("actor", getenvOrDefault("USER", "unknown"), "actor")
	reason := fs.String("reason", "maintenance-resume", "reason")
	timeout := fs.Duration("timeout", defaults.timeout(10*time.Second), "request timeout")
	fs.Parse(args)
	printResolvedSettings(fs,
		defaults.adminEndpointSetting(),
		defaults.fieldSetting("cluster_id", "cluster-id", "", "NAMRBD_CLUSTER_ID"),
		defaults.fieldSetting("sbs_cluster_id", "sbs-cluster-id", "", "SBS_CLUSTER_ID", "NAMRBD_SBS_CLUSTER_ID"),
		defaults.timeoutSetting(10*time.Second),
	)

	client, ctx, cancel := dialAdmin(*adminEndpoint, *timeout)
	defer cancel()
	defer client.Close()
	resp, err := client.Admin.ResumeMaintenance(ctx, &adminv1.ResumeMaintenanceRequest{
		Cluster:          clusterRef(*clusterID, *sbsClusterID),
		Meta:             &adminv1.RequestMeta{Actor: *actor, Reason: *reason},
		ResumeRepairs:    *repairs,
		ResumeRebalances: *rebalances,
		ResumeDrains:     *drains,
	})
	if err != nil {
		fatalf("maintenance resume failed: %v", err)
	}
	writeJSON(resp)
}

func runTestIOOpen(args []string) {
	defaults := mustResolveCLIDefaults(args)
	fs := flag.NewFlagSet("testio open", flag.ExitOnError)
	registerContextFlags(fs, defaults)
	dataEndpoint := fs.String("data-endpoint", defaults.dataEndpoint(), "sbs-data gRPC endpoint")
	volumeID := fs.String("volume-id", "", "volume id")
	gatewayID := fs.String("gateway-id", defaults.fieldValue("gateway_id", "NAMRBD_GATEWAY_ID"), "gateway id")
	attachmentID := fs.String("attachment-id", getenvOrDefault("NAMRBD_ATTACHMENT_ID", "sbsctl-test-attachment"), "attachment id")
	attachmentGeneration := fs.Uint64("attachment-generation", 1, "attachment generation")
	timeout := fs.Duration("timeout", defaults.timeout(10*time.Second), "request timeout")
	fs.Parse(args)
	if *gatewayID == "" {
		*gatewayID = "sbsctl-test"
	}
	printResolvedSettings(fs,
		defaults.dataEndpointSetting(),
		defaults.fieldSetting("gateway_id", "gateway-id", "sbsctl-test", "NAMRBD_GATEWAY_ID"),
		defaults.timeoutSetting(10*time.Second),
	)
	if *volumeID == "" {
		fatalf("--volume-id is required")
	}

	client, ctx, cancel := dialData(*dataEndpoint, *timeout)
	defer cancel()
	defer client.Close()
	resp, err := client.Volume.OpenVolume(ctx, &sbsv1.OpenVolumeRequest{
		VolumeId:   *volumeID,
		AccessMode: sbsv1.AccessMode_ACCESS_MODE_EXCLUSIVE_WRITER,
		Context:    requestContext(*gatewayID, *attachmentID, *attachmentGeneration),
	})
	if err != nil {
		fatalf("testio open failed: %v", err)
	}
	writeJSON(resp)
}

func runTestIORead(args []string) {
	defaults := mustResolveCLIDefaults(args)
	fs := flag.NewFlagSet("testio read", flag.ExitOnError)
	registerContextFlags(fs, defaults)
	dataEndpoint := fs.String("data-endpoint", defaults.dataEndpoint(), "sbs-data gRPC endpoint")
	volumeID := fs.String("volume-id", "", "volume id")
	handle := fs.String("handle", "", "volume handle")
	offset := fs.Uint64("offset", 0, "offset bytes")
	length := fs.Uint64("length", 0, "length bytes")
	gatewayID := fs.String("gateway-id", defaults.fieldValue("gateway_id", "NAMRBD_GATEWAY_ID"), "gateway id")
	attachmentID := fs.String("attachment-id", getenvOrDefault("NAMRBD_ATTACHMENT_ID", "sbsctl-test-attachment"), "attachment id")
	attachmentGeneration := fs.Uint64("attachment-generation", 1, "attachment generation")
	timeout := fs.Duration("timeout", defaults.timeout(10*time.Second), "request timeout")
	fs.Parse(args)
	if *gatewayID == "" {
		*gatewayID = "sbsctl-test"
	}
	printResolvedSettings(fs,
		defaults.dataEndpointSetting(),
		defaults.fieldSetting("gateway_id", "gateway-id", "sbsctl-test", "NAMRBD_GATEWAY_ID"),
		defaults.timeoutSetting(10*time.Second),
	)
	if *volumeID == "" || *length == 0 {
		fatalf("--volume-id and --length are required")
	}

	client, ctx, cancel := dialData(*dataEndpoint, *timeout)
	defer cancel()
	defer client.Close()
	resp, err := client.Volume.Read(ctx, &sbsv1.ReadRequest{
		VolumeId:     *volumeID,
		VolumeHandle: *handle,
		OffsetBytes:  *offset,
		LengthBytes:  *length,
		Context:      requestContext(*gatewayID, *attachmentID, *attachmentGeneration),
	})
	if err != nil {
		fatalf("testio read failed: %v", err)
	}
	fmt.Printf("volume_id: %s\n", resp.GetVolumeId())
	fmt.Printf("offset_bytes: %d\n", resp.GetOffsetBytes())
	fmt.Printf("length_bytes: %d\n", resp.GetLengthBytes())
	fmt.Printf("volume_revision: %d\n", resp.GetVolumeRevision())
	fmt.Printf("data_hex: %s\n", hex.EncodeToString(resp.GetData()))
}

func runTestIOWrite(args []string) {
	defaults := mustResolveCLIDefaults(args)
	fs := flag.NewFlagSet("testio write", flag.ExitOnError)
	registerContextFlags(fs, defaults)
	dataEndpoint := fs.String("data-endpoint", defaults.dataEndpoint(), "sbs-data gRPC endpoint")
	volumeID := fs.String("volume-id", "", "volume id")
	handle := fs.String("handle", "", "volume handle")
	offset := fs.Uint64("offset", 0, "offset bytes")
	data := fs.String("data", "", "plain text payload")
	dataHex := fs.String("data-hex", "", "hex encoded payload")
	gatewayID := fs.String("gateway-id", defaults.fieldValue("gateway_id", "NAMRBD_GATEWAY_ID"), "gateway id")
	attachmentID := fs.String("attachment-id", getenvOrDefault("NAMRBD_ATTACHMENT_ID", "sbsctl-test-attachment"), "attachment id")
	attachmentGeneration := fs.Uint64("attachment-generation", 1, "attachment generation")
	timeout := fs.Duration("timeout", defaults.timeout(10*time.Second), "request timeout")
	fs.Parse(args)
	if *gatewayID == "" {
		*gatewayID = "sbsctl-test"
	}
	printResolvedSettings(fs,
		defaults.dataEndpointSetting(),
		defaults.fieldSetting("gateway_id", "gateway-id", "sbsctl-test", "NAMRBD_GATEWAY_ID"),
		defaults.timeoutSetting(10*time.Second),
	)
	if *volumeID == "" {
		fatalf("--volume-id is required")
	}
	payload := []byte(*data)
	if *dataHex != "" {
		buf, err := hex.DecodeString(*dataHex)
		if err != nil {
			fatalf("--data-hex decode failed: %v", err)
		}
		payload = buf
	}
	if len(payload) == 0 {
		fatalf("--data or --data-hex is required")
	}

	client, ctx, cancel := dialData(*dataEndpoint, *timeout)
	defer cancel()
	defer client.Close()
	resp, err := client.Volume.Write(ctx, &sbsv1.WriteRequest{
		VolumeId:     *volumeID,
		VolumeHandle: *handle,
		OffsetBytes:  *offset,
		LengthBytes:  uint64(len(payload)),
		Data:         payload,
		Context:      requestContext(*gatewayID, *attachmentID, *attachmentGeneration),
	})
	if err != nil {
		fatalf("testio write failed: %v", err)
	}
	writeJSON(resp)
}

func runTestIOFlush(args []string) {
	defaults := mustResolveCLIDefaults(args)
	fs := flag.NewFlagSet("testio flush", flag.ExitOnError)
	registerContextFlags(fs, defaults)
	dataEndpoint := fs.String("data-endpoint", defaults.dataEndpoint(), "sbs-data gRPC endpoint")
	volumeID := fs.String("volume-id", "", "volume id")
	handle := fs.String("handle", "", "volume handle")
	gatewayID := fs.String("gateway-id", defaults.fieldValue("gateway_id", "NAMRBD_GATEWAY_ID"), "gateway id")
	attachmentID := fs.String("attachment-id", getenvOrDefault("NAMRBD_ATTACHMENT_ID", "sbsctl-test-attachment"), "attachment id")
	attachmentGeneration := fs.Uint64("attachment-generation", 1, "attachment generation")
	timeout := fs.Duration("timeout", defaults.timeout(10*time.Second), "request timeout")
	fs.Parse(args)
	if *gatewayID == "" {
		*gatewayID = "sbsctl-test"
	}
	printResolvedSettings(fs,
		defaults.dataEndpointSetting(),
		defaults.fieldSetting("gateway_id", "gateway-id", "sbsctl-test", "NAMRBD_GATEWAY_ID"),
		defaults.timeoutSetting(10*time.Second),
	)
	if *volumeID == "" {
		fatalf("--volume-id is required")
	}

	client, ctx, cancel := dialData(*dataEndpoint, *timeout)
	defer cancel()
	defer client.Close()
	resp, err := client.Volume.Flush(ctx, &sbsv1.FlushRequest{
		VolumeId:     *volumeID,
		VolumeHandle: *handle,
		Context:      requestContext(*gatewayID, *attachmentID, *attachmentGeneration),
	})
	if err != nil {
		fatalf("testio flush failed: %v", err)
	}
	writeJSON(resp)
}

func dialAdmin(endpoint string, timeout time.Duration) (*adminclient.Client, context.Context, context.CancelFunc) {
	if endpoint == "" {
		fatalf("admin endpoint is required (use --admin-endpoint or SBS_ADMIN_ENDPOINTS/NAMRBD_SBS_ADMIN_ENDPOINTS)")
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	client, err := adminclient.Dial(ctx, endpoint)
	if err != nil {
		cancel()
		fatalf("%v", err)
	}
	return client, ctx, cancel
}

func dialData(endpoint string, timeout time.Duration) (*sbsdataclient.Client, context.Context, context.CancelFunc) {
	if endpoint == "" {
		fatalf("data endpoint is required (use --data-endpoint or SBS_DATA_ENDPOINTS/SBS_GRPC_ADDR/NAMRBD_SBS_GRPC_ADDR)")
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	client, err := sbsdataclient.Dial(ctx, endpoint)
	if err != nil {
		cancel()
		fatalf("%v", err)
	}
	return client, ctx, cancel
}

func clusterRef(clusterID, sbsClusterID string) *adminv1.ClusterRef {
	return &adminv1.ClusterRef{ClusterId: clusterID, SbsClusterId: sbsClusterID}
}

func defaultAdminEndpoint() string {
	for _, key := range []string{"SBS_ADMIN_ENDPOINTS", "NAMRBD_SBS_ADMIN_ENDPOINTS"} {
		if raw := os.Getenv(key); raw != "" {
			parts := strings.Split(raw, ",")
			if len(parts) > 0 && strings.TrimSpace(parts[0]) != "" {
				return strings.TrimSpace(parts[0])
			}
		}
	}
	return ""
}

func defaultDataEndpoint() string {
	for _, key := range []string{"SBS_DATA_ENDPOINTS", "SBS_GRPC_ADDR", "NAMRBD_SBS_GRPC_ADDR"} {
		if raw := os.Getenv(key); raw != "" {
			parts := strings.Split(raw, ",")
			if len(parts) > 0 && strings.TrimSpace(parts[0]) != "" {
				return strings.TrimSpace(parts[0])
			}
		}
	}
	return ""
}

func defaultTimeout() time.Duration {
	raw := firstEnvOrDefault("SBS_TIMEOUT", "NAMRBD_TIMEOUT", "10s")
	d, err := time.ParseDuration(raw)
	if err != nil {
		return 10 * time.Second
	}
	return d
}

func firstEnv(keys ...string) string {
	for _, key := range keys {
		if v := strings.TrimSpace(os.Getenv(key)); v != "" {
			return v
		}
	}
	return ""
}

func firstEnvOrDefault(primary, secondary, fallback string) string {
	if v := firstEnv(primary, secondary); v != "" {
		return v
	}
	return fallback
}

func getenvOrDefault(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func writeJSON(v any) {
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	_ = enc.Encode(v)
}

func fatalf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}

func usage() {
	fmt.Fprintf(os.Stderr, "usage: %s <command> [args]\n", os.Args[0])
	fmt.Fprintln(os.Stderr, "commands:")
	fmt.Fprintln(os.Stderr, "  cluster init|status")
	fmt.Fprintln(os.Stderr, "  node join|update-topology|status|drain|drain status|remove")
	fmt.Fprintln(os.Stderr, "  topology zone create|list|get|update|delete")
	fmt.Fprintln(os.Stderr, "  store status|tuning")
	fmt.Fprintln(os.Stderr, "  volume create|restore-from-snapshot|expand|delete|purge|status|health|placement|transitions|list")
	fmt.Fprintln(os.Stderr, "  snapshot create|get|list|delete")
	fmt.Fprintln(os.Stderr, "  iscsi status|lun")
	fmt.Fprintln(os.Stderr, "  repair list|show")
	fmt.Fprintln(os.Stderr, "  rebalance list")
	fmt.Fprintln(os.Stderr, "  maintenance throttle|pause|resume")
	fmt.Fprintln(os.Stderr, "  operations list|show")
	fmt.Fprintln(os.Stderr, "  testio open|read|write|flush")
	for _, line := range enterpriseUsageLines() {
		fmt.Fprintln(os.Stderr, line)
	}
	fmt.Fprintln(os.Stderr, "  version")
}

func clusterUsage() {
	fmt.Fprintln(os.Stderr, "usage: sbsctl cluster init|status ...")
}

func nodeUsage() {
	fmt.Fprintln(os.Stderr, "usage: sbsctl node join|update-topology|status|drain|drain status|remove ...")
}

func storeUsage() {
	fmt.Fprintln(os.Stderr, "usage: sbsctl store status|tuning ...")
}

func topologyUsage() {
	fmt.Fprintln(os.Stderr, "usage: sbsctl topology zone create|list|get|update|delete ...")
}

func volumeUsage() {
	fmt.Fprintln(os.Stderr, "usage: sbsctl volume create|restore-from-snapshot|expand|delete|purge|status|health|replica-targets|allocation-page|placement|transitions|list ...")
}

func operationsUsage() {
	fmt.Fprintln(os.Stderr, "usage: sbsctl operations list|show ...")
}

func repairUsage() {
	fmt.Fprintln(os.Stderr, "usage: sbsctl repair list|show ...")
}

func rebalanceUsage() {
	fmt.Fprintln(os.Stderr, "usage: sbsctl rebalance list")
}

func maintenanceUsage() {
	fmt.Fprintln(os.Stderr, "usage: sbsctl maintenance throttle|pause|resume|payload-gc ...")
}

func runMaintenancePayloadGC(args []string) {
	defaults := mustResolveCLIDefaults(args)
	fs := flag.NewFlagSet("maintenance payload-gc", flag.ExitOnError)
	registerContextFlags(fs, defaults)
	adminHTTP := fs.String("admin-http-endpoint", defaults.fieldValue("sbs_node_admin_http", "SBS_NODE_ADMIN_HTTP", "NAMRBD_SBS_ADMIN_ADDR"), "node-local admin/debug HTTP endpoint")
	metadataPath := fs.String("metadata-path", getenvOrDefault("NAMRBD_SBS_METADATA_PATH", ""), "path to SBS cluster metadata pebble directory")
	metadataRoot := fs.String("metadata-root", getenvOrDefault("NAMRBD_SBS_METADATA_ROOT", "sbs/cluster"), "metadata root prefix")
	payloadRoot := fs.String("payload-root", getenvOrDefault("NAMRBD_SBS_PAYLOAD_ROOT", ""), "path to local replica payload root directory")
	volumeID := fs.String("volume-id", "", "optional canonical volume id to sweep; when empty sweeps all volumes")
	output := fs.String("output", defaults.fieldValue("output", "SBS_OUTPUT", "NAMRBD_OUTPUT"), "output format: table|json")
	fs.Parse(args)
	if *output == "" {
		*output = "table"
	}
	if strings.TrimSpace(*payloadRoot) == "" {
		fatalf("--payload-root is required")
	}
	printResolvedSettings(fs,
		defaults.fieldSetting("sbs_node_admin_http", "admin-http-endpoint", "", "SBS_NODE_ADMIN_HTTP", "NAMRBD_SBS_ADMIN_ADDR"),
		defaults.fieldSetting("output", "output", "table", "SBS_OUTPUT", "NAMRBD_OUTPUT"),
	)

	var (
		results []clusterreplication.LocalPayloadSweepResult
		err     error
	)
	switch adminHTTP := strings.TrimRight(strings.TrimSpace(*adminHTTP), "/"); {
	case adminHTTP != "":
		results, err = runMaintenancePayloadGCRemote(context.Background(), adminHTTP, *payloadRoot, *volumeID)
	default:
		if strings.TrimSpace(*metadataPath) == "" {
			fatalf("--metadata-path is required when --admin-http-endpoint is not set")
		}
		results, err = runMaintenancePayloadGCSweep(context.Background(), *metadataPath, *metadataRoot, *payloadRoot, *volumeID)
	}
	if err != nil {
		fatalf("payload gc sweep failed: %v", err)
	}

	switch strings.ToLower(strings.TrimSpace(*output)) {
	case "json":
		writeJSON(results)
	case "", "table":
		fmt.Println("VOLUME\tREPLICA\tCANDIDATES\tDELETED\tRETAINED")
		for _, result := range results {
			fmt.Printf("%s\t%s\t%d\t%d\t%d\n",
				result.VolumeID,
				result.ReplicaID,
				result.CandidateCount,
				result.DeletedCount,
				result.RetainedCount,
			)
		}
	default:
		fatalf("unsupported output format %q", *output)
	}
}

func runMaintenancePayloadGCRemote(ctx context.Context, adminHTTP, payloadRoot, volumeID string) ([]clusterreplication.LocalPayloadSweepResult, error) {
	return runMaintenancePayloadGCRemoteWithClient(ctx, http.DefaultClient, adminHTTP, payloadRoot, volumeID)
}

func runMaintenancePayloadGCRemoteWithClient(ctx context.Context, client *http.Client, adminHTTP, payloadRoot, volumeID string) ([]clusterreplication.LocalPayloadSweepResult, error) {
	if client == nil {
		client = http.DefaultClient
	}
	q := url.Values{}
	q.Set("payload_root", payloadRoot)
	if strings.TrimSpace(volumeID) != "" {
		q.Set("volume_id", strings.TrimSpace(volumeID))
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(adminHTTP, "/")+"/debug/payload-gc?"+q.Encode(), nil)
	if err != nil {
		return nil, err
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		var body struct {
			Error string `json:"error"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&body); err == nil && strings.TrimSpace(body.Error) != "" {
			return nil, fmt.Errorf("admin http %s: %s", resp.Status, body.Error)
		}
		raw, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		if msg := strings.TrimSpace(string(raw)); msg != "" {
			return nil, fmt.Errorf("admin http %s: %s", resp.Status, msg)
		}
		return nil, fmt.Errorf("admin http %s", resp.Status)
	}
	var results []clusterreplication.LocalPayloadSweepResult
	if err := json.NewDecoder(resp.Body).Decode(&results); err != nil {
		return nil, fmt.Errorf("decode payload gc response: %w", err)
	}
	return results, nil
}

func runMaintenancePayloadGCSweep(ctx context.Context, metadataPath, metadataRoot, payloadRoot, volumeID string) ([]clusterreplication.LocalPayloadSweepResult, error) {
	kv, err := clustermeta.OpenPebbleKV(metadataPath)
	if err != nil {
		return nil, fmt.Errorf("open metadata pebble: %w", err)
	}
	defer kv.Close()

	repo := clustermeta.NewRepository(kv, metadataRoot)
	nodes, err := repo.ListNodeMemberships(ctx)
	if err != nil {
		return nil, fmt.Errorf("list node memberships: %w", err)
	}
	replicaIDs := make([]string, 0, len(nodes))
	seen := make(map[string]struct{}, len(nodes))
	for _, node := range nodes {
		if node.ReplicaID == "" {
			continue
		}
		if _, ok := seen[node.ReplicaID]; ok {
			continue
		}
		seen[node.ReplicaID] = struct{}{}
		replicaIDs = append(replicaIDs, node.ReplicaID)
	}
	if len(replicaIDs) == 0 {
		return nil, fmt.Errorf("no replica ids found in node membership records")
	}

	replicaStores, err := payload.OpenReplicaStores(payloadRoot, replicaIDs)
	if err != nil {
		return nil, fmt.Errorf("open replica payload stores: %w", err)
	}
	defer replicaStores.Close()

	collector := clusterreplication.NewLocalPayloadGarbageCollector(repo, replicaStores.ObjectStores())
	if strings.TrimSpace(volumeID) == "" {
		return collector.SweepAll(ctx)
	}
	return collector.SweepVolume(ctx, volumeID)
}

func testIOUsage() {
	fmt.Fprintln(os.Stderr, "usage: sbsctl testio open|read|write|flush ...")
}

func requestContext(gatewayID, attachmentID string, attachmentGeneration uint64) *sbsv1.RequestContext {
	return &sbsv1.RequestContext{
		RequestId:      fmt.Sprintf("sbsctl-%d", time.Now().UnixNano()),
		GatewayId:      gatewayID,
		TraceId:        fmt.Sprintf("trace-%d", time.Now().UnixNano()),
		AttachmentId:   attachmentID,
		Generation:     attachmentGeneration,
		IdempotencyKey: fmt.Sprintf("idem-%d", time.Now().UnixNano()),
	}
}
