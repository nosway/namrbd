package main

import (
	"flag"
	"os"
	"time"

	adminv1 "github.com/nosway/namrbd/sbs/admin/v1"
)

func runSnapshot(args []string) {
	if len(args) < 1 {
		snapshotUsage()
		os.Exit(2)
	}
	switch args[0] {
	case "create":
		runSnapshotCreate(args[1:])
	case "get":
		runSnapshotGet(args[1:])
	case "list":
		runSnapshotList(args[1:])
	case "delete":
		runSnapshotDelete(args[1:])
	default:
		snapshotUsage()
		os.Exit(2)
	}
}

func runSnapshotCreate(args []string) {
	defaults := mustResolveCLIDefaults(args)
	fs := flag.NewFlagSet("snapshot create", flag.ExitOnError)
	registerContextFlags(fs, defaults)
	adminEndpoint := fs.String("admin-endpoint", defaults.adminEndpoint(), "cluster-wide sbs-admin gRPC endpoint")
	clusterID := fs.String("cluster-id", defaults.fieldValue("cluster_id", "NAMRBD_CLUSTER_ID"), "cluster id")
	sbsClusterID := fs.String("sbs-cluster-id", defaults.fieldValue("sbs_cluster_id", "SBS_CLUSTER_ID", "NAMRBD_SBS_CLUSTER_ID"), "sbs cluster id")
	sourceVolumeID := fs.String("source-volume-id", "", "source volume id")
	idempotencyKey := fs.String("idempotency-key", "", "idempotency key; empty lets the service derive one when implemented")
	actor := fs.String("actor", getenvOrDefault("USER", "unknown"), "actor")
	reason := fs.String("reason", "create-snapshot", "reason")
	timeout := fs.Duration("timeout", defaults.timeout(10*time.Second), "request timeout")
	fs.Parse(args)
	printResolvedSettings(fs,
		defaults.adminEndpointSetting(),
		defaults.fieldSetting("cluster_id", "cluster-id", "", "NAMRBD_CLUSTER_ID"),
		defaults.fieldSetting("sbs_cluster_id", "sbs-cluster-id", "", "SBS_CLUSTER_ID", "NAMRBD_SBS_CLUSTER_ID"),
		defaults.timeoutSetting(10*time.Second),
	)
	if *sourceVolumeID == "" {
		fatalf("--source-volume-id is required")
	}

	client, ctx, cancel := dialAdmin(*adminEndpoint, *timeout)
	defer cancel()
	defer client.Close()
	resp, err := client.Admin.CreateSnapshot(ctx, &adminv1.CreateSnapshotRequest{
		Cluster:        clusterRef(*clusterID, *sbsClusterID),
		Meta:           &adminv1.RequestMeta{Actor: *actor, Reason: *reason},
		SourceVolumeId: *sourceVolumeID,
		IdempotencyKey: *idempotencyKey,
	})
	if err != nil {
		fatalf("snapshot create failed: %v", err)
	}
	writeJSON(resp)
}

func runSnapshotGet(args []string) {
	defaults := mustResolveCLIDefaults(args)
	fs := flag.NewFlagSet("snapshot get", flag.ExitOnError)
	registerContextFlags(fs, defaults)
	adminEndpoint := fs.String("admin-endpoint", defaults.adminEndpoint(), "cluster-wide sbs-admin gRPC endpoint")
	clusterID := fs.String("cluster-id", defaults.fieldValue("cluster_id", "NAMRBD_CLUSTER_ID"), "cluster id")
	sbsClusterID := fs.String("sbs-cluster-id", defaults.fieldValue("sbs_cluster_id", "SBS_CLUSTER_ID", "NAMRBD_SBS_CLUSTER_ID"), "sbs cluster id")
	snapshotID := fs.String("snapshot-id", "", "snapshot id")
	timeout := fs.Duration("timeout", defaults.timeout(10*time.Second), "request timeout")
	fs.Parse(args)
	printResolvedSettings(fs,
		defaults.adminEndpointSetting(),
		defaults.fieldSetting("cluster_id", "cluster-id", "", "NAMRBD_CLUSTER_ID"),
		defaults.fieldSetting("sbs_cluster_id", "sbs-cluster-id", "", "SBS_CLUSTER_ID", "NAMRBD_SBS_CLUSTER_ID"),
		defaults.timeoutSetting(10*time.Second),
	)
	if *snapshotID == "" {
		fatalf("--snapshot-id is required")
	}

	client, ctx, cancel := dialAdmin(*adminEndpoint, *timeout)
	defer cancel()
	defer client.Close()
	resp, err := client.Admin.GetSnapshot(ctx, &adminv1.GetSnapshotRequest{
		Cluster:    clusterRef(*clusterID, *sbsClusterID),
		SnapshotId: *snapshotID,
	})
	if err != nil {
		fatalf("snapshot get failed: %v", err)
	}
	writeJSON(resp)
}

func runSnapshotList(args []string) {
	defaults := mustResolveCLIDefaults(args)
	fs := flag.NewFlagSet("snapshot list", flag.ExitOnError)
	registerContextFlags(fs, defaults)
	adminEndpoint := fs.String("admin-endpoint", defaults.adminEndpoint(), "cluster-wide sbs-admin gRPC endpoint")
	clusterID := fs.String("cluster-id", defaults.fieldValue("cluster_id", "NAMRBD_CLUSTER_ID"), "cluster id")
	sbsClusterID := fs.String("sbs-cluster-id", defaults.fieldValue("sbs_cluster_id", "SBS_CLUSTER_ID", "NAMRBD_SBS_CLUSTER_ID"), "sbs cluster id")
	sourceVolumeID := fs.String("source-volume-id", "", "source volume id filter")
	includeDeleted := fs.Bool("include-deleted", false, "include deleted snapshots")
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
	resp, err := client.Admin.ListSnapshots(ctx, &adminv1.ListSnapshotsRequest{
		Cluster:        clusterRef(*clusterID, *sbsClusterID),
		SourceVolumeId: *sourceVolumeID,
		IncludeDeleted: *includeDeleted,
	})
	if err != nil {
		fatalf("snapshot list failed: %v", err)
	}
	writeJSON(resp)
}

func runSnapshotDelete(args []string) {
	defaults := mustResolveCLIDefaults(args)
	fs := flag.NewFlagSet("snapshot delete", flag.ExitOnError)
	registerContextFlags(fs, defaults)
	adminEndpoint := fs.String("admin-endpoint", defaults.adminEndpoint(), "cluster-wide sbs-admin gRPC endpoint")
	clusterID := fs.String("cluster-id", defaults.fieldValue("cluster_id", "NAMRBD_CLUSTER_ID"), "cluster id")
	sbsClusterID := fs.String("sbs-cluster-id", defaults.fieldValue("sbs_cluster_id", "SBS_CLUSTER_ID", "NAMRBD_SBS_CLUSTER_ID"), "sbs cluster id")
	snapshotID := fs.String("snapshot-id", "", "snapshot id")
	actor := fs.String("actor", getenvOrDefault("USER", "unknown"), "actor")
	reason := fs.String("reason", "delete-snapshot", "reason")
	yes := fs.Bool("yes", false, "confirm snapshot delete")
	timeout := fs.Duration("timeout", defaults.timeout(10*time.Second), "request timeout")
	fs.Parse(args)
	printResolvedSettings(fs,
		defaults.adminEndpointSetting(),
		defaults.fieldSetting("cluster_id", "cluster-id", "", "NAMRBD_CLUSTER_ID"),
		defaults.fieldSetting("sbs_cluster_id", "sbs-cluster-id", "", "SBS_CLUSTER_ID", "NAMRBD_SBS_CLUSTER_ID"),
		defaults.timeoutSetting(10*time.Second),
	)
	if !*yes {
		fatalf("--yes is required for snapshot delete")
	}
	if *snapshotID == "" {
		fatalf("--snapshot-id is required")
	}

	client, ctx, cancel := dialAdmin(*adminEndpoint, *timeout)
	defer cancel()
	defer client.Close()
	resp, err := client.Admin.DeleteSnapshot(ctx, &adminv1.DeleteSnapshotRequest{
		Cluster:    clusterRef(*clusterID, *sbsClusterID),
		Meta:       &adminv1.RequestMeta{Actor: *actor, Reason: *reason},
		SnapshotId: *snapshotID,
	})
	if err != nil {
		fatalf("snapshot delete failed: %v", err)
	}
	writeJSON(resp)
}

func snapshotUsage() {
	fatalf("usage: sbsctl snapshot create|get|list|delete ...")
}
