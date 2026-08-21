package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math/bits"
	"net"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/nosway/namrbd/control/bridge"
	"github.com/nosway/namrbd/control/netlinkclient"
	"github.com/nosway/namrbd/control/netlinktlv"
	"github.com/nosway/namrbd/gateway/metadata"
	"github.com/nosway/namrbd/gateway/service"
	"github.com/nosway/namrbd/internal/cliux"
	namrbdversion "github.com/nosway/namrbd/version"
	"github.com/nosway/namrbd/volumeid"
)

func main() {
	os.Args, globalJSONOutput = normalizeRootArgs(os.Args)
	if len(os.Args) >= 2 && (os.Args[1] == "--version" || os.Args[1] == "version") {
		writeCommandResult(map[string]any{"version": namrbdversion.BuildSummary()}, namrbdversion.BuildSummary())
		return
	}
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	if os.Args[1] == "--help" || os.Args[1] == "-h" || os.Args[1] == "help" && len(os.Args) == 2 {
		usage()
		return
	}
	if os.Args[1] == "help" {
		os.Args = append(append([]string{os.Args[0]}, os.Args[2:]...), "--help")
	}
	commandHelpRequested = len(os.Args) > 2 && os.Args[len(os.Args)-1] == "--help"

	switch os.Args[1] {
	case "create-device":
		withNetlinkClient(func(client netlinkclient.Client) {
			runCreateDevice(client, os.Args[2:])
		})
	case "destroy-device":
		withNetlinkClient(func(client netlinkclient.Client) {
			runDestroyDevice(client, os.Args[2:])
		})
	case "config-rest":
		withNetlinkClient(func(client netlinkclient.Client) {
			runConfigREST(client, os.Args[2:])
		})
	case "attach":
		withNetlinkClient(func(client netlinkclient.Client) {
			runAttach(client, os.Args[2:])
		})
	case "reconfigure-data-paths":
		withNetlinkClient(func(client netlinkclient.Client) {
			runReconfigureDataPaths(client, os.Args[2:])
		})
	case "volume-reload-size":
		withNetlinkClient(func(client netlinkclient.Client) {
			runVolumeReloadSize(client, os.Args[2:])
		})
	case "detach":
		withNetlinkClient(func(client netlinkclient.Client) {
			runDetach(client, os.Args[2:])
		})
	case "status":
		withNetlinkClient(func(client netlinkclient.Client) {
			runStatus(client, os.Args[2:])
		})
	case "list-devices":
		withNetlinkClient(func(client netlinkclient.Client) {
			runListDevices(client, os.Args[2:])
		})
	case "info":
		runInfo(os.Args[2:])
	case "discover-gateways":
		runDiscoverGateways(os.Args[2:])
	case "discover-volume":
		runDiscoverVolume(os.Args[2:])
	case "plan-volume-paths":
		runPlanVolumePaths(os.Args[2:])
	case "cluster-metrics":
		runClusterMetrics(os.Args[2:])
	case "gateway-list":
		runGatewayList(os.Args[2:])
	case "gateway-get":
		runGatewayGet(os.Args[2:])
	case "gateway-put":
		runGatewayPut(os.Args[2:])
	case "attachment-get":
		runAttachmentGet(os.Args[2:])
	case "volume-list":
		runVolumeList(os.Args[2:])
	case "volume-create":
		runVolumeCreate(os.Args[2:])
	case "volume-get":
		runVolumeGet(os.Args[2:])
	case "volume-update":
		runVolumeUpdate(os.Args[2:])
	case "volume-set-state":
		runVolumeSetState(os.Args[2:])
	case "volume-delete":
		runVolumeDelete(os.Args[2:])
	case "volume-status":
		runVolumeStatus(os.Args[2:])
	case "validate-volume":
		runValidateVolume(os.Args[2:])
	case "validate-all":
		runValidateAll(os.Args[2:])
	case "apply-volume-path-plan":
		withNetlinkClient(func(client netlinkclient.Client) {
			runApplyVolumePathPlan(client, os.Args[2:])
		})
	case "read":
		runRead(os.Args[2:])
	case "write":
		runWrite(os.Args[2:])
	default:
		usage()
		os.Exit(2)
	}
}

var (
	globalJSONOutput     bool
	commandHelpRequested bool
)

func normalizeRootArgs(args []string) ([]string, bool) {
	if len(args) == 0 {
		return nil, false
	}
	out := []string{args[0]}
	jsonOutput := false
	for _, arg := range args[1:] {
		if arg == "--json" || arg == "-json" {
			jsonOutput = true
			continue
		}
		out = append(out, arg)
	}
	if len(out) > 2 && out[len(out)-1] == "help" {
		out[len(out)-1] = "--help"
	}
	return out, jsonOutput
}

func writeCommandResult(v any, textOutput string) {
	if globalJSONOutput {
		_ = json.NewEncoder(os.Stdout).Encode(v)
		return
	}
	fmt.Println(textOutput)
}

func withNetlinkClient(run func(netlinkclient.Client)) {
	if commandHelpRequested {
		run(nil)
		return
	}
	client, err := netlinkclient.Dial()
	if err != nil {
		fatalf("dial netlink: %v", err)
	}
	defer client.Close()
	run(client)
}

func newCommandFlagSet(name string, errorHandling flag.ErrorHandling) *flag.FlagSet {
	fs := flag.NewFlagSet(name, errorHandling)
	cliux.InstallStructuredUsage(fs, "namrbdctl "+name, nil)
	return fs
}

func runCreateDevice(client netlinkclient.Client, args []string) {
	fs := newCommandFlagSet("create-device", flag.ExitOnError)
	fs.Parse(args)
	resp, err := client.CreateDevice()
	if err != nil {
		fatalf("create-device failed: %v", err)
	}
	enc := json.NewEncoder(os.Stdout)
	_ = enc.Encode(map[string]any{
		"device_id": resp.DeviceID,
		"disk_name": resp.DiskName,
	})
}

func runDestroyDevice(client netlinkclient.Client, args []string) {
	fs := newCommandFlagSet("destroy-device", flag.ExitOnError)
	deviceID := fs.Uint("device", 0, "device id")
	fs.Parse(args)
	if err := client.DestroyDevice(uint32(*deviceID)); err != nil {
		fatalf("destroy-device failed: %v", err)
	}
	writeCommandResult(map[string]any{"result": "ok", "device_id": *deviceID}, "ok")
}

func runConfigREST(client netlinkclient.Client, args []string) {
	fs := newCommandFlagSet("config-rest", flag.ExitOnError)
	deviceID := fs.Uint("device", 0, "device id")
	var serverSpecs multiString
	fs.Var(&serverSpecs, "server", "server spec: id,address,port,tls,api_prefix[,bearer_token]")
	fs.Parse(args)
	if *deviceID == 0 && !hasUintFlag(args, "device") {
		fatalf("--device is required")
	}
	if len(serverSpecs) == 0 {
		fatalf("at least one --server is required")
	}

	var servers []netlinktlv.RESTServer
	for _, spec := range serverSpecs {
		s, err := parseServerSpec(spec)
		if err != nil {
			fatalf("invalid --server %q: %v", spec, err)
		}
		servers = append(servers, s)
	}

	if err := client.ConfigREST(netlinktlv.ConfigRESTRequest{
		DeviceID: uint32(*deviceID),
		Servers:  servers,
	}); err != nil {
		fatalf("config-rest failed: %v", err)
	}
	writeCommandResult(map[string]any{"result": "ok", "device_id": *deviceID, "server_count": len(servers)}, "ok")
}

func runAttach(client netlinkclient.Client, args []string) {
	defaults := mustResolveCLIDefaults(args)
	fs := newCommandFlagSet("attach", flag.ExitOnError)
	deviceID := fs.Uint("device", 0, "device id")
	registerContextFlags(fs, defaults)
	hostID := fs.String("host", defaults.hostID(), "host id")
	rawVolumeID := fs.String("volume", "", "volume id (8 lowercase hex digits)")
	gateway := fs.String("gateway", defaults.gatewayEndpoint(), "gateway base URL for userspace-mediated attach")
	gatewayCAFile := fs.String("gateway-ca-file", defaults.gatewayCAFile(), "PEM CA bundle for mediated gateway TLS")
	discoveryMaxPaths := fs.Int("discovery-max-paths", 0, "limit active dataplane paths from gateway discovery (0 = all)")
	discoveryOwnerOnly := fs.Bool("discovery-owner-only", false, "use only owner-gateway dataplane paths from gateway discovery")
	discoveryPreferGateway := fs.String("discovery-prefer-gateway", "", "prefer this gateway id when selecting discovery dataplane paths")
	timeout := fs.Duration("timeout", defaults.timeout(10*time.Second), "gateway request timeout")
	fs.Parse(args)
	printResolvedSettings(fs, defaults.hostSetting(), defaults.gatewayEndpointSetting(), defaults.gatewayCASetting(), defaults.timeoutSetting(10*time.Second))
	volumeID, err := parseVolumeID(*rawVolumeID)
	if (*deviceID == 0 && !hasUintFlag(args, "device")) || *hostID == "" || err != nil || volumeID == 0 {
		fatalf("--device, --host and --volume are required")
	}
	if *gateway != "" {
		cli := newGatewayClientFunc(*gateway, gatewayClientOptions{CAFile: *gatewayCAFile, Timeout: *timeout})
		manifest, err := cli.attach(volumeID, *hostID, uint32(*deviceID))
		if err != nil {
			fatalf("gateway attach failed: %v", err)
		}
		if discovery, err := cli.discoveryVolume(volumeID); err == nil {
			if merged, err := mergeDiscoveryIntoManifest(manifest, discovery, discoveryMergeOptions{
				MaxPaths:         *discoveryMaxPaths,
				OwnerOnly:        *discoveryOwnerOnly,
				PreferredGateway: *discoveryPreferGateway,
			}); err == nil {
				manifest = merged
			} else {
				fatalf("merge discovery manifest failed: %v", err)
			}
		}
		manifest, servers, err := prepareGatewayAttachManifest(manifest)
		if err != nil {
			fatalf("prepare gateway attach manifest failed: %v", err)
		}
		if err := client.ConfigREST(netlinktlv.ConfigRESTRequest{
			DeviceID: uint32(*deviceID),
			Servers:  servers,
		}); err != nil {
			fatalf("config-rest from attach manifest failed: %v", err)
		}
		if err := client.AttachManifest(netlinktlv.AttachManifestRequest{
			DeviceID:     uint32(*deviceID),
			HostID:       *hostID,
			VolumeID:     volumeID,
			ManifestJSON: manifest,
		}); err != nil {
			fatalf("attach-manifest failed: %v", err)
		}
		summary, err := summarizeGatewayAttachManifest(manifest)
		if err != nil {
			fatalf("summarize attach manifest failed: %v", err)
		}
		if clusterMetrics, metricsErr := cli.clusterMetrics(); metricsErr == nil {
			if topClass := anyString(clusterMetrics["top_priority_class"]); topClass != "" {
				summary["cluster_top_priority_class"] = topClass
				summary["cluster_top_priority_count"] = anyInt64(clusterMetrics["top_priority_count"])
				if currentClass := anyString(summary["controller_priority_class"]); currentClass != "" {
					match := currentClass == topClass
					summary["cluster_priority_matches_controller"] = match
					if !match {
						mismatchActions := clusterPriorityRecommendedActions(currentClass, topClass)
						summary["cluster_priority_mismatch_actions"] = mismatchActions
						actions := anyStrings(summary["operator_recommended_actions"])
						summary["operator_recommended_actions"] = dedupeStrings(append(actions, mismatchActions...))
					}
				}
			}
		}
		out, err := json.Marshal(summary)
		if err != nil {
			fatalf("marshal attach manifest summary failed: %v", err)
		}
		fmt.Println(string(out))
		return
	}

	if err := client.AttachVolume(netlinktlv.AttachRequest{
		DeviceID: uint32(*deviceID),
		HostID:   *hostID,
		VolumeID: volumeID,
	}); err != nil {
		fatalf("attach failed: %v", err)
	}
	writeCommandResult(map[string]any{"result": "ok", "device_id": *deviceID, "volume_id": service.CanonicalVolumeID(volumeID)}, "ok")
}

func runReconfigureDataPaths(client netlinkclient.Client, args []string) {
	defaults := mustResolveCLIDefaults(args)
	fs := newCommandFlagSet("reconfigure-data-paths", flag.ExitOnError)
	deviceID := fs.Uint("device", 0, "device id")
	registerContextFlags(fs, defaults)
	hostID := fs.String("host", defaults.hostID(), "host id")
	rawVolumeID := fs.String("volume", "", "volume id (8 lowercase hex digits)")
	gateway := fs.String("gateway", defaults.gatewayEndpoint(), "gateway base URL for discovery-expanded manifest")
	gatewayCAFile := fs.String("gateway-ca-file", defaults.gatewayCAFile(), "PEM CA bundle for gateway TLS")
	discoveryMaxPaths := fs.Int("discovery-max-paths", 0, "limit active dataplane paths from gateway discovery (0 = all)")
	discoveryOwnerOnly := fs.Bool("discovery-owner-only", false, "use only owner-gateway dataplane paths from gateway discovery")
	discoveryPreferGateway := fs.String("discovery-prefer-gateway", "", "prefer this gateway id when selecting discovery dataplane paths")
	timeout := fs.Duration("timeout", defaults.timeout(10*time.Second), "gateway request timeout")
	fs.Parse(args)
	printResolvedSettings(fs, defaults.hostSetting(), defaults.gatewayEndpointSetting(), defaults.gatewayCASetting(), defaults.timeoutSetting(10*time.Second))
	volumeID, err := parseVolumeID(*rawVolumeID)
	if (*deviceID == 0 && !hasUintFlag(args, "device")) || *hostID == "" || err != nil || volumeID == 0 || *gateway == "" {
		fatalf("--device, --host, --volume and --gateway are required")
	}

	cli := newGatewayClientFunc(*gateway, gatewayClientOptions{CAFile: *gatewayCAFile, Timeout: *timeout})
	info, err := cli.info(volumeID)
	if err != nil {
		fatalf("gateway info failed: %v", err)
	}
	infoJSON, err := json.Marshal(info)
	if err != nil {
		fatalf("marshal gateway info manifest failed: %v", err)
	}
	manifest := string(infoJSON)
	if discovery, err := cli.discoveryVolume(volumeID); err == nil {
		if merged, err := mergeDiscoveryIntoManifest(manifest, discovery, discoveryMergeOptions{
			MaxPaths:         *discoveryMaxPaths,
			OwnerOnly:        *discoveryOwnerOnly,
			PreferredGateway: *discoveryPreferGateway,
		}); err == nil {
			manifest = merged
		} else {
			fatalf("merge discovery manifest failed: %v", err)
		}
	}
	manifest, servers, err := prepareGatewayAttachManifest(manifest)
	if err != nil {
		fatalf("prepare gateway reconfigure manifest failed: %v", err)
	}
	if err := client.ConfigREST(netlinktlv.ConfigRESTRequest{
		DeviceID: uint32(*deviceID),
		Servers:  servers,
	}); err != nil {
		fatalf("config-rest from reconfigure manifest failed: %v", err)
	}
	if err := client.ReconfigureDataPaths(netlinktlv.AttachManifestRequest{
		DeviceID:     uint32(*deviceID),
		HostID:       *hostID,
		VolumeID:     volumeID,
		ManifestJSON: manifest,
	}); err != nil {
		fatalf("reconfigure-data-paths failed: %v", err)
	}
	status, err := client.GetStatus(uint32(*deviceID))
	if err != nil {
		fatalf("reconfigure-data-paths status failed: %v", err)
	}
	summary, err := summarizeGatewayAttachManifest(manifest)
	if err != nil {
		fatalf("summarize reconfigure manifest failed: %v", err)
	}
	summary["result"] = "ok"
	summary["device_id"] = *deviceID
	summary["path_count"] = status.PathCount
	summary["down_mask"] = fmt.Sprintf("0x%x", status.DownMask)
	summary["degraded_mask"] = fmt.Sprintf("0x%x", status.DegradedMask)
	summary["draining_mask"] = fmt.Sprintf("0x%x", status.DrainingMask)
	summary["runtime_path_plan"] = summarizeRuntimePathPlan(status)
	summary["no_path"] = summarizeNoPathStatus(status)
	summary["paths"] = encodePathStatuses(status.Paths)
	summary["lanes"] = encodeLaneStatuses(status.Lanes)
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	if err := enc.Encode(summary); err != nil {
		fatalf("marshal reconfigure summary failed: %v", err)
	}
}

func runVolumeReloadSize(client netlinkclient.Client, args []string) {
	defaults := mustResolveCLIDefaults(args)
	fs := newCommandFlagSet("volume-reload-size", flag.ExitOnError)
	deviceID := fs.Uint("device", 0, "device id")
	registerContextFlags(fs, defaults)
	hostID := fs.String("host", defaults.hostID(), "host id")
	rawVolumeID := fs.String("volume", "", "volume id (8 lowercase hex digits)")
	gateway := fs.String("gateway", defaults.gatewayEndpoint(), "gateway base URL")
	gatewayCAFile := fs.String("gateway-ca-file", defaults.gatewayCAFile(), "PEM CA bundle for gateway TLS")
	timeout := fs.Duration("timeout", defaults.timeout(10*time.Second), "gateway request timeout")
	fs.Parse(args)
	printResolvedSettings(fs, defaults.hostSetting(), defaults.gatewayEndpointSetting(), defaults.gatewayCASetting(), defaults.timeoutSetting(10*time.Second))
	volumeID, err := parseVolumeID(*rawVolumeID)
	if (*deviceID == 0 && !hasUintFlag(args, "device")) || *hostID == "" || err != nil || volumeID == 0 || *gateway == "" {
		fatalf("--device, --host, --volume and --gateway are required")
	}

	status, err := client.GetStatus(uint32(*deviceID))
	if err != nil {
		fatalf("get device status failed: %v", err)
	}
	if !status.Attached || status.VolumeID != volumeID {
		fatalf("device %d is not attached to volume %s", *deviceID, service.CanonicalVolumeID(volumeID))
	}

	opts := gatewayClientOptions{CAFile: *gatewayCAFile, Timeout: *timeout}
	cli := newGatewayClientFunc(*gateway, opts)
	info, err := cli.info(volumeID)
	if err != nil {
		fatalf("gateway info failed: %v", err)
	}
	controlURLs := reloadControlEndpointURLs(info, *gateway)
	var manifest map[string]any
	reloaded := make([]string, 0, len(controlURLs))
	reloadDetails := make([]map[string]any, 0, len(controlURLs))
	var expectedSizeBytes uint64
	var expectedGeneration uint64
	for _, url := range controlURLs {
		gw := newGatewayClientFunc(url, opts)
		manifest, err = gw.reloadSize(volumeID)
		if err != nil {
			fatalf("gateway reload-size failed url=%s: %v", url, err)
		}
		sizeBytes := anyUint64(manifest["size_bytes"])
		generation := anyUint64(manifest["generation"])
		reloadDetails = append(reloadDetails, map[string]any{
			"gateway":    url,
			"size_bytes": sizeBytes,
			"generation": generation,
		})
		if sizeBytes == 0 || generation == 0 {
			fatalf("gateway reload-size response missing size_bytes or generation url=%s", url)
		}
		if expectedSizeBytes == 0 {
			expectedSizeBytes = sizeBytes
			expectedGeneration = generation
		} else if sizeBytes != expectedSizeBytes || generation != expectedGeneration {
			fatalf("gateway reload-size responses disagree first_size=%d first_generation=%d url=%s size=%d generation=%d details=%v",
				expectedSizeBytes, expectedGeneration, url, sizeBytes, generation, reloadDetails)
		}
		reloaded = append(reloaded, url)
	}
	if manifest == nil {
		fatalf("no gateway reload endpoints selected")
	}
	sizeBytes := expectedSizeBytes
	generation := expectedGeneration
	if generation != status.Generation {
		fatalf("attachment generation mismatch kernel=%d gateway=%d", status.Generation, generation)
	}
	if err := client.ResizeDevice(netlinktlv.ResizeDeviceRequest{
		DeviceID:   uint32(*deviceID),
		VolumeID:   volumeID,
		Generation: generation,
		SizeBytes:  sizeBytes,
	}); err != nil {
		fatalf("kernel resize failed: %v", err)
	}
	out := map[string]any{
		"result":            "ok",
		"device_id":         *deviceID,
		"volume_id":         service.CanonicalVolumeID(volumeID),
		"generation":        generation,
		"size_bytes":        sizeBytes,
		"reloaded_gateways": reloaded,
		"reload_details":    reloadDetails,
	}
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	if err := enc.Encode(out); err != nil {
		fatalf("marshal reload-size summary failed: %v", err)
	}
}

func runDetach(client netlinkclient.Client, args []string) {
	defaults := mustResolveCLIDefaults(args)
	fs := newCommandFlagSet("detach", flag.ExitOnError)
	deviceID := fs.Uint("device", 0, "device id")
	registerContextFlags(fs, defaults)
	hostID := fs.String("host", defaults.hostID(), "host id")
	rawVolumeID := fs.String("volume", "", "volume id (8 lowercase hex digits)")
	gateway := fs.String("gateway", defaults.gatewayEndpoint(), "gateway base URL for userspace-mediated detach")
	gatewayCAFile := fs.String("gateway-ca-file", defaults.gatewayCAFile(), "PEM CA bundle for mediated gateway TLS")
	localOnly := fs.Bool("local-only", false, "detach only local kernel state without contacting the gateway")
	timeout := fs.Duration("timeout", defaults.timeout(10*time.Second), "gateway request timeout")
	fs.Parse(args)
	printResolvedSettings(fs, defaults.hostSetting(), defaults.gatewayEndpointSetting(), defaults.gatewayCASetting(), defaults.timeoutSetting(10*time.Second))
	volumeID, err := parseVolumeID(*rawVolumeID)
	if (*deviceID == 0 && !hasUintFlag(args, "device")) || err != nil || volumeID == 0 || (!*localOnly && *hostID == "") {
		if *localOnly {
			fatalf("--device and --volume are required")
		}
		fatalf("--device, --host and --volume are required")
	}
	if *localOnly {
		if err := client.DetachLocal(netlinktlv.DetachLocalRequest{
			DeviceID: uint32(*deviceID),
			VolumeID: volumeID,
		}); err != nil {
			fatalf("detach-local failed: %v", err)
		}
		writeCommandResult(map[string]any{"result": "ok", "device_id": *deviceID, "volume_id": service.CanonicalVolumeID(volumeID), "local_only": true}, "ok")
		return
	}
	if *gateway != "" {
		cli := newGatewayClientFunc(*gateway, gatewayClientOptions{CAFile: *gatewayCAFile, Timeout: *timeout})
		info, err := cli.info(volumeID)
		if err != nil {
			fatalf("gateway info failed: %v", err)
		}
		attachmentID, _ := info["attachment_id"].(string)
		if attachmentID == "" {
			fatalf("gateway info missing attachment_id")
		}
		if err := cli.detach(volumeID, *hostID, attachmentID); err != nil {
			fatalf("gateway detach failed: %v", err)
		}
		if err := client.DetachLocal(netlinktlv.DetachLocalRequest{
			DeviceID: uint32(*deviceID),
			VolumeID: volumeID,
		}); err != nil {
			fatalf("detach-local failed: %v", err)
		}
		writeCommandResult(map[string]any{"result": "ok", "device_id": *deviceID, "volume_id": service.CanonicalVolumeID(volumeID)}, "ok")
		return
	}

	if err := client.DetachVolume(netlinktlv.DetachRequest{
		DeviceID: uint32(*deviceID),
		HostID:   *hostID,
		VolumeID: volumeID,
	}); err != nil {
		fatalf("detach failed: %v", err)
	}
	writeCommandResult(map[string]any{"result": "ok", "device_id": *deviceID, "volume_id": service.CanonicalVolumeID(volumeID)}, "ok")
}

func runStatus(client netlinkclient.Client, args []string) {
	defaults := mustResolveCLIDefaults(args)
	fs := newCommandFlagSet("status", flag.ExitOnError)
	deviceID := fs.Uint("device", 0, "device id")
	registerContextFlags(fs, defaults)
	gateway := fs.String("gateway", defaults.gatewayEndpoint(), "gateway base URL")
	gatewayCAFile := fs.String("gateway-ca-file", defaults.gatewayCAFile(), "PEM CA bundle for gateway TLS")
	rawVolumeID := fs.String("volume", "", "volume id (8 lowercase hex digits); defaults to attached runtime volume")
	expectedRevision := fs.Uint64("expected-path-plan-revision", 0, "expected path plan revision for convergence check")
	reportRuntimeFeedback := fs.Bool("report-runtime-feedback", false, "report runtime lane attention/recommended actions back to gateway control-plane")
	feedbackSourceHost := fs.String("feedback-source-host", defaults.hostID(), "source host id for runtime feedback")
	timeout := fs.Duration("timeout", defaults.timeout(10*time.Second), "gateway request timeout")
	fs.Parse(args)
	if *deviceID == 0 && !hasUintFlag(args, "device") {
		fatalf("--device is required")
	}
	status, err := client.GetStatus(uint32(*deviceID))
	if err != nil {
		fatalf("status failed: %v", err)
	}
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	out := map[string]any{
		"device_id":                  status.DeviceID,
		"disk_name":                  status.DiskName,
		"attached":                   status.Attached,
		"volume_id":                  service.CanonicalVolumeID(status.VolumeID),
		"generation":                 status.Generation,
		"path_count":                 status.PathCount,
		"nr_hw_queues":               status.NrHwQueues,
		"target_nr_hw_queues":        status.TargetNrHwQueues,
		"queue_topology_generation":  status.QueueTopologyGeneration,
		"queue_topology_state":       status.QueueTopologyState,
		"down_mask":                  fmt.Sprintf("0x%x", status.DownMask),
		"degraded_mask":              fmt.Sprintf("0x%x", status.DegradedMask),
		"draining_mask":              fmt.Sprintf("0x%x", status.DrainingMask),
		"applied_path_plan_revision": status.AppliedPathPlanRevision,
		"runtime_path_plan":          summarizeRuntimePathPlan(status),
		"no_path":                    summarizeNoPathStatus(status),
		"lanes":                      encodeLaneStatuses(status.Lanes),
		"paths":                      encodePathStatuses(status.Paths),
	}
	runtimeActions := recommendedRuntimePathPlanActions(out["runtime_path_plan"].(map[string]any))
	out["recommended_actions"] = runtimeActions
	out["operator_recommended_actions"] = dedupeStrings(append([]string(nil), runtimeActions...))
	if *expectedRevision != 0 {
		out["expected_path_plan_revision"] = *expectedRevision
		out["path_plan_revision_state"] = summarizePathPlanRevisionState(*expectedRevision, status.AppliedPathPlanRevision)
	}
	if *gateway != "" {
		volumeID := status.VolumeID
		if *rawVolumeID != "" {
			parsedVolumeID, parseErr := parseVolumeID(*rawVolumeID)
			if parseErr != nil || parsedVolumeID == 0 {
				fatalf("--volume must be a valid 8 lowercase hex digits")
			}
			volumeID = parsedVolumeID
		}
		cli := newGatewayClientFunc(*gateway, gatewayClientOptions{CAFile: *gatewayCAFile, Timeout: *timeout})
		info, infoErr := cli.info(volumeID)
		if infoErr == nil {
			gatewayManifest := summarizeGatewayManifestInfo(info)
			out["gateway_manifest"] = gatewayManifest
			comparison := summarizeManifestRuntimeComparison(info, status)
			out["manifest_runtime_comparison"] = comparison
			out["handoff_fencing"] = summarizeHandoffFencingStatus(gatewayManifest, comparison)
			if expansionState := anyString(comparison["runtime_expansion_state"]); expansionState != "" {
				out["controller_runtime_expansion_state"] = expansionState
			}
			if raw, ok := comparison["runtime_expansion_backoff_level"]; ok {
				out["controller_runtime_expansion_backoff_level"] = raw
			}
			if backoffState := anyString(comparison["handoff_backoff_state"]); backoffState != "" && backoffState != "not_scheduled" {
				out["controller_handoff_backoff_state"] = backoffState
			}
			manifestActions := recommendedManifestRuntimeActions(comparison)
			out["manifest_recommended_actions"] = manifestActions
			if priorityClass := anyString(gatewayManifest["controller_priority_class"]); priorityClass != "" {
				out["controller_priority_class"] = priorityClass
			}
			controllerActions := dedupeStrings(append(anyStrings(gatewayManifest["controller_recommended_actions"]), manifestActions...))
			out["controller_recommended_actions"] = controllerActions
			out["operator_recommended_actions"] = dedupeStrings(append(append([]string(nil), runtimeActions...), controllerActions...))
		}
		if clusterMetrics, metricsErr := cli.clusterMetrics(); metricsErr == nil {
			if topClass := anyString(clusterMetrics["top_priority_class"]); topClass != "" {
				out["cluster_top_priority_class"] = topClass
				out["cluster_top_priority_count"] = anyInt64(clusterMetrics["top_priority_count"])
				if currentClass := anyString(out["controller_priority_class"]); currentClass != "" {
					match := currentClass == topClass
					out["cluster_priority_matches_controller"] = match
					if !match {
						mismatchActions := clusterPriorityRecommendedActions(currentClass, topClass)
						out["cluster_priority_mismatch_actions"] = mismatchActions
						out["operator_recommended_actions"] = dedupeStrings(append(anyStrings(out["operator_recommended_actions"]), mismatchActions...))
					}
				}
			}
		}
		if *reportRuntimeFeedback {
			runtimeSummary := out["runtime_path_plan"].(map[string]any)
			noPathSummary := out["no_path"].(map[string]any)
			reportResp, reportErr := cli.reportRuntimeFeedback(volumeID, runtimeFeedbackPayload(runtimeSummary, noPathSummary, runtimeActions, *feedbackSourceHost))
			if reportErr != nil {
				out["runtime_feedback_report_error"] = reportErr.Error()
			} else {
				out["runtime_feedback_report"] = reportResp
			}
		}
	}
	_ = enc.Encode(out)
}

func runListDevices(client netlinkclient.Client, args []string) {
	fs := newCommandFlagSet("list-devices", flag.ExitOnError)
	fs.Parse(args)
	devices, err := client.ListDevices()
	if err != nil {
		fatalf("list-devices failed: %v", err)
	}
	out := make([]map[string]any, len(devices))
	for i, d := range devices {
		out[i] = map[string]any{
			"device_id":                  d.DeviceID,
			"disk_name":                  d.DiskName,
			"attached":                   d.Attached,
			"volume_id":                  service.CanonicalVolumeID(d.VolumeID),
			"generation":                 d.Generation,
			"path_count":                 d.PathCount,
			"nr_hw_queues":               d.NrHwQueues,
			"target_nr_hw_queues":        d.TargetNrHwQueues,
			"queue_topology_generation":  d.QueueTopologyGeneration,
			"queue_topology_state":       d.QueueTopologyState,
			"down_mask":                  fmt.Sprintf("0x%x", d.DownMask),
			"degraded_mask":              fmt.Sprintf("0x%x", d.DegradedMask),
			"draining_mask":              fmt.Sprintf("0x%x", d.DrainingMask),
			"applied_path_plan_revision": d.AppliedPathPlanRevision,
			"runtime_path_plan":          summarizeRuntimePathPlan(d),
			"no_path":                    summarizeNoPathStatus(d),
			"lanes":                      encodeLaneStatuses(d.Lanes),
			"paths":                      encodePathStatuses(d.Paths),
		}
		out[i]["recommended_actions"] = recommendedRuntimePathPlanActions(out[i]["runtime_path_plan"].(map[string]any))
	}
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	_ = enc.Encode(out)
}

func parseServerSpec(s string) (netlinktlv.RESTServer, error) {
	parts := strings.Split(s, ",")
	if len(parts) < 5 || len(parts) > 6 {
		return netlinktlv.RESTServer{}, fmt.Errorf("format must be id,address,port,tls,api_prefix[,bearer_token]")
	}
	id, err := strconv.ParseUint(parts[0], 10, 32)
	if err != nil {
		return netlinktlv.RESTServer{}, err
	}
	port, err := strconv.ParseUint(parts[2], 10, 16)
	if err != nil {
		return netlinktlv.RESTServer{}, err
	}
	useTLS, err := strconv.ParseBool(parts[3])
	if err != nil {
		return netlinktlv.RESTServer{}, err
	}
	out := netlinktlv.RESTServer{
		ID:        uint32(id),
		Address:   parts[1],
		Port:      uint16(port),
		UseTLS:    useTLS,
		APIPrefix: parts[4],
	}
	if len(parts) == 6 {
		out.BearerToken = parts[5]
	}
	if out.Address == "" {
		return netlinktlv.RESTServer{}, fmt.Errorf("address is required")
	}
	return out, nil
}

func encodePathStatuses(paths []netlinktlv.PathStatus) []map[string]any {
	out := make([]map[string]any, 0, len(paths))
	for _, path := range paths {
		entry := map[string]any{
			"path_id":            path.PathID,
			"state":              pathStateString(path.State),
			"state_code":         path.State,
			"consecutive_errors": path.ConsecutiveErrors,
			"last_errno":         path.LastErrno,
			"last_wire_status":   path.LastWireStatus,
			"connected":          path.Connected,
			"inflight":           path.Inflight,
			"pending":            path.Pending,
			"pending_high_water": path.PendingHighWater,
			"outstanding_limit":  path.OutstandingLimit,
			"submitted":          path.Submitted,
			"completed":          path.Completed,
			"retries":            path.Retries,
			"conn_opens":         path.ConnOpens,
			"conn_resets":        path.ConnResets,
		}
		if path.GatewayID != "" {
			entry["gateway_id"] = path.GatewayID
		}
		if path.Address != "" {
			entry["address"] = path.Address
			entry["port"] = path.Port
			entry["use_tls"] = path.UseTLS
		}
		if path.ServerName != "" {
			entry["server_name"] = path.ServerName
		}
		if path.Priority != 0 {
			entry["priority"] = path.Priority
		}
		out = append(out, entry)
	}
	return out
}

func pathStateString(state uint32) string {
	switch state {
	case 0:
		return "up"
	case 1:
		return "degraded"
	case 2:
		return "down"
	case 3:
		return "draining"
	default:
		return "unknown"
	}
}

type multiString []string

func (m *multiString) String() string { return strings.Join(*m, ",") }
func (m *multiString) Set(v string) error {
	*m = append(*m, v)
	return nil
}

func usage() {
	fmt.Fprintf(os.Stderr, "usage:\n")
	fmt.Fprintf(os.Stderr, "  namrbdctl [--json] help COMMAND\n")
	fmt.Fprintf(os.Stderr, "  namrbdctl create-device\n")
	fmt.Fprintf(os.Stderr, "  namrbdctl destroy-device --device DEVICE_ID\n")
	fmt.Fprintf(os.Stderr, "  namrbdctl config-rest --device DEVICE_ID --server id,address,port,tls,api_prefix[,token]\n")
	fmt.Fprintf(os.Stderr, "  namrbdctl attach --device DEVICE_ID --host HOST --volume VOLUME_ID [--gateway URL | --etcd-endpoints HOST:PORT[,HOST:PORT...]] [--discovery-max-paths N --discovery-prefer-gateway GW]\n")
	fmt.Fprintf(os.Stderr, "  namrbdctl reconfigure-data-paths --device DEVICE_ID --host HOST --volume VOLUME_ID [--gateway https://HOST:PORT --gateway-ca-file CA.pem --discovery-max-paths N --discovery-prefer-gateway GW]\n")
	fmt.Fprintf(os.Stderr, "  namrbdctl volume-reload-size --device DEVICE_ID --host HOST --volume VOLUME_ID --gateway https://HOST:PORT [--gateway-ca-file CA.pem]\n")
	fmt.Fprintf(os.Stderr, "  namrbdctl detach --device DEVICE_ID --host HOST --volume VOLUME_ID [--gateway https://HOST:PORT --gateway-ca-file CA.pem]\n")
	fmt.Fprintf(os.Stderr, "  namrbdctl status --device DEVICE_ID [--gateway URL --volume VOLUME_ID --report-runtime-feedback]\n")
	fmt.Fprintf(os.Stderr, "  namrbdctl list-devices\n")
	fmt.Fprintf(os.Stderr, "direct-etcd metadata read commands:\n")
	fmt.Fprintf(os.Stderr, "  namrbdctl gateway-list [--etcd-endpoints HOST:PORT[,HOST:PORT...] --etcd-root /namrbd --json]\n")
	fmt.Fprintf(os.Stderr, "  namrbdctl gateway-get --gateway GATEWAY_ID [--etcd-endpoints HOST:PORT[,HOST:PORT...] --etcd-root /namrbd --json]\n")
	fmt.Fprintf(os.Stderr, "  namrbdctl attachment-get --volume VOLUME_ID [--etcd-endpoints HOST:PORT[,HOST:PORT...] --etcd-root /namrbd --json]\n")
	fmt.Fprintf(os.Stderr, "  namrbdctl volume-list [--etcd-endpoints HOST:PORT[,HOST:PORT...] --etcd-root /namrbd --json]\n")
	fmt.Fprintf(os.Stderr, "  namrbdctl volume-get --volume VOLUME_ID [--etcd-endpoints HOST:PORT[,HOST:PORT...] --etcd-root /namrbd --json]\n")
	fmt.Fprintf(os.Stderr, "  namrbdctl volume-status --volume VOLUME_ID [--etcd-endpoints HOST:PORT[,HOST:PORT...] --etcd-root /namrbd --json]\n")
	fmt.Fprintf(os.Stderr, "  namrbdctl validate-volume --volume VOLUME_ID [--etcd-endpoints HOST:PORT[,HOST:PORT...] --etcd-root /namrbd --json]\n")
	fmt.Fprintf(os.Stderr, "  namrbdctl validate-all [--etcd-endpoints HOST:PORT[,HOST:PORT...] --etcd-root /namrbd --json]\n")
	fmt.Fprintf(os.Stderr, "direct-etcd metadata mutation commands:\n")
	fmt.Fprintf(os.Stderr, "  namrbdctl gateway-put --from-file PATH [--gateway GATEWAY_ID] [--etcd-endpoints HOST:PORT[,HOST:PORT...] --etcd-root /namrbd --json]\n")
	fmt.Fprintf(os.Stderr, "  namrbdctl volume-create [--name NAME] --size <n>M|<n>G|<n>T [--block-size <n>K] [--allocation-chunk-size <n>K|<n>M] [--allocation-page-size <n>M|<n>G] [--access-mode exclusive|shared] [--state available|disabled] [--etcd-endpoints HOST:PORT[,HOST:PORT...] --etcd-root /namrbd --json]\n")
	fmt.Fprintf(os.Stderr, "  namrbdctl volume-update --volume VOLUME_ID [--name NAME] [--size <n>M|<n>G|<n>T] [--access-mode exclusive|shared] [--state available|in_use|disabled] [--etcd-endpoints HOST:PORT[,HOST:PORT...] --etcd-root /namrbd --json]\n")
	fmt.Fprintf(os.Stderr, "  namrbdctl volume-set-state --volume VOLUME_ID --state available|in_use|disabled [--etcd-endpoints HOST:PORT[,HOST:PORT...] --etcd-root /namrbd --json]\n")
	fmt.Fprintf(os.Stderr, "  namrbdctl volume-delete --volume VOLUME_ID [--etcd-endpoints HOST:PORT[,HOST:PORT...] --etcd-root /namrbd --json]\n")
	fmt.Fprintf(os.Stderr, "gateway-direct commands:\n")
	fmt.Fprintf(os.Stderr, "  namrbdctl info --gateway http://127.0.0.1:9701 --volume VOLUME_ID [--gateway-ca-file CA.pem]\n")
	fmt.Fprintf(os.Stderr, "  namrbdctl discover-gateways --gateway http://127.0.0.1:9701 [--gateway-ca-file CA.pem]\n")
	fmt.Fprintf(os.Stderr, "  namrbdctl discover-volume --gateway http://127.0.0.1:9701 --volume VOLUME_ID [--gateway-ca-file CA.pem]\n")
	fmt.Fprintf(os.Stderr, "  namrbdctl plan-volume-paths --gateway http://127.0.0.1:9701 --volume VOLUME_ID [--path-health PATH_ID=healthy|suspect|down --max-active N --gateway-ca-file CA.pem]\n")
	fmt.Fprintf(os.Stderr, "  namrbdctl cluster-metrics --gateway http://127.0.0.1:9701 [--gateway-ca-file CA.pem]\n")
	fmt.Fprintf(os.Stderr, "  namrbdctl apply-volume-path-plan --device DEVICE_ID --gateway http://127.0.0.1:9701 --volume VOLUME_ID [--path-health PATH_ID=healthy|suspect|down --max-active N --gateway-ca-file CA.pem]\n")
	fmt.Fprintf(os.Stderr, "  namrbdctl read --gateway http://127.0.0.1:9701 --volume VOLUME_ID --offset BYTES --length BYTES [--out FILE --gateway-ca-file CA.pem]\n")
	fmt.Fprintf(os.Stderr, "  namrbdctl write --gateway http://127.0.0.1:9701 --volume VOLUME_ID --offset BYTES --data-file FILE [--gateway-ca-file CA.pem]\n")
	fmt.Fprintf(os.Stderr, "  namrbdctl version\n")
}

func hasUintFlag(args []string, name string) bool {
	prefix := "--" + name
	for _, arg := range args {
		if arg == prefix || strings.HasPrefix(arg, prefix+"=") {
			return true
		}
	}
	return false
}

func fatalf(format string, args ...any) {
	message := fmt.Sprintf(format, args...)
	if globalJSONOutput {
		_ = json.NewEncoder(os.Stdout).Encode(map[string]any{"result": "error", "error": message})
	} else {
		fmt.Fprintln(os.Stderr, message)
	}
	os.Exit(1)
}

func formatUnix(ts int64) string {
	if ts == 0 {
		return ""
	}
	return time.Unix(ts, 0).UTC().Format(time.RFC3339)
}

func loadGatewayRecord(path string) (service.GatewayRecord, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return service.GatewayRecord{}, err
	}
	var rec service.GatewayRecord
	if err := json.Unmarshal(data, &rec); err != nil {
		return service.GatewayRecord{}, err
	}
	return rec, nil
}

func parseVolumeState(raw string) (service.VolumeLifecycleState, error) {
	switch service.VolumeLifecycleState(strings.TrimSpace(raw)) {
	case service.VolumeStateAvailable:
		return service.VolumeStateAvailable, nil
	case service.VolumeStateInUse:
		return service.VolumeStateInUse, nil
	case service.VolumeStateDisabled:
		return service.VolumeStateDisabled, nil
	default:
		return "", fmt.Errorf("state must be one of available, in_use, disabled")
	}
}

func parseSizeWithUnit(raw string) (uint64, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return 0, fmt.Errorf("size is empty")
	}
	if len(raw) < 2 {
		return 0, fmt.Errorf("size must be in the form <n>M|<n>G|<n>T")
	}
	unit := raw[len(raw)-1]
	number := strings.TrimSpace(raw[:len(raw)-1])
	value, err := strconv.ParseUint(number, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("size value must be an integer")
	}
	var multiplier uint64
	switch unit {
	case 'M', 'm':
		multiplier = 1024 * 1024
	case 'G', 'g':
		multiplier = 1024 * 1024 * 1024
	case 'T', 't':
		multiplier = 1024 * 1024 * 1024 * 1024
	default:
		return 0, fmt.Errorf("size unit must be one of M, G, T")
	}
	if value > ^uint64(0)/multiplier {
		return 0, fmt.Errorf("size is too large")
	}
	return value * multiplier, nil
}

func parseBlockSizeK(raw string) (uint64, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return 0, fmt.Errorf("block size is empty")
	}
	if len(raw) < 2 {
		return 0, fmt.Errorf("block size must be <n>K (e.g. 4K)")
	}
	unit := raw[len(raw)-1]
	if unit != 'K' && unit != 'k' {
		return 0, fmt.Errorf("block size unit must be K only (e.g. 4K)")
	}
	number := strings.TrimSpace(raw[:len(raw)-1])
	if number == "" {
		return 0, fmt.Errorf("block size value is required")
	}
	value, err := strconv.ParseUint(number, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("block size value must be an integer")
	}
	if value == 0 {
		return 0, fmt.Errorf("block size must be greater than zero")
	}
	if value > ^uint64(0)/1024 {
		return 0, fmt.Errorf("block size is too large")
	}
	bytes := value * 1024
	if bytes > uint64(^uint32(0)) {
		return 0, fmt.Errorf("block size is too large for uint32")
	}
	return bytes, nil
}

func parseChunkSizeWithUnit(raw string) (uint64, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return 0, fmt.Errorf("chunk size is empty")
	}
	if len(raw) < 2 {
		return 0, fmt.Errorf("chunk size must be <n>K|<n>M")
	}
	unit := raw[len(raw)-1]
	number := strings.TrimSpace(raw[:len(raw)-1])
	if number == "" {
		return 0, fmt.Errorf("chunk size value is required")
	}
	value, err := strconv.ParseUint(number, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("chunk size value must be an integer")
	}
	if value == 0 {
		return 0, fmt.Errorf("chunk size must be greater than zero")
	}
	var multiplier uint64
	switch unit {
	case 'K', 'k':
		multiplier = 1024
	case 'M', 'm':
		multiplier = 1024 * 1024
	default:
		return 0, fmt.Errorf("chunk size unit must be K or M")
	}
	if value > ^uint64(0)/multiplier {
		return 0, fmt.Errorf("chunk size is too large")
	}
	bytes := value * multiplier
	if bytes > uint64(^uint32(0)) {
		return 0, fmt.Errorf("chunk size is too large for uint32")
	}
	return bytes, nil
}

func parseAllocationChunkSizeWithUnit(raw string) (uint64, error) {
	return parseChunkSizeWithUnit(raw)
}

func parseExtentPageSizeWithUnit(raw string) (uint64, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return 0, fmt.Errorf("extent page size is empty")
	}
	if len(raw) < 2 {
		return 0, fmt.Errorf("extent page size must be <n>M|<n>G")
	}
	unit := raw[len(raw)-1]
	number := strings.TrimSpace(raw[:len(raw)-1])
	if number == "" {
		return 0, fmt.Errorf("extent page size value is required")
	}
	value, err := strconv.ParseUint(number, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("extent page size value must be an integer")
	}
	if value == 0 {
		return 0, fmt.Errorf("extent page size must be greater than zero")
	}
	var multiplier uint64
	switch unit {
	case 'M', 'm':
		multiplier = 1024 * 1024
	case 'G', 'g':
		multiplier = 1024 * 1024 * 1024
	default:
		return 0, fmt.Errorf("extent page size unit must be M or G")
	}
	if value > ^uint64(0)/multiplier {
		return 0, fmt.Errorf("extent page size is too large")
	}
	bytes := value * multiplier
	if bytes > uint64(^uint32(0)) {
		return 0, fmt.Errorf("extent page size is too large for uint32")
	}
	return bytes, nil
}

func parseAllocationPageSizeWithUnit(raw string) (uint64, error) {
	return parseExtentPageSizeWithUnit(raw)
}

func parseVolumeAccessMode(raw string) (service.VolumeAccessMode, error) {
	switch service.VolumeAccessMode(strings.TrimSpace(raw)) {
	case service.VolumeAccessModeExclusive:
		return service.VolumeAccessModeExclusive, nil
	case service.VolumeAccessModeShared:
		return service.VolumeAccessModeShared, nil
	default:
		return "", fmt.Errorf("access mode must be one of exclusive, shared")
	}
}

type gatewayClient struct {
	baseURL    string
	httpClient *http.Client
}

type validationIssue struct {
	Severity string `json:"severity"`
	Code     string `json:"code"`
	Message  string `json:"message"`
}

type volumeSpecOutput struct {
	service.VolumeSpec
	AllocationChunkSizeBytes uint32 `json:"allocation_chunk_size_bytes,omitempty"`
	AllocationPageBytes      uint32 `json:"allocation_page_bytes,omitempty"`
}

type volumeValidationReport struct {
	VolumeID   service.HexVolumeID        `json:"volume_id"`
	OK         bool                       `json:"ok"`
	Generation uint64                     `json:"generation"`
	IssueCount int                        `json:"issue_count"`
	Issues     []validationIssue          `json:"issues"`
	Volume     volumeSpecOutput           `json:"volume"`
	Status     service.VolumeStatusRecord `json:"status"`
	Attachment service.AttachmentRecord   `json:"attachment"`
}

type allValidationReport struct {
	OK           bool                     `json:"ok"`
	VolumeCount  int                      `json:"volume_count"`
	GatewayCount int                      `json:"gateway_count"`
	IssueCount   int                      `json:"issue_count"`
	Volumes      []volumeValidationReport `json:"volumes"`
	Issues       []validationIssue        `json:"issues"`
}

type gatewayClientOptions struct {
	CAFile  string
	Timeout time.Duration
}

var newGatewayClientFunc = newGatewayClient
var openEtcdMetadataRepositoryFunc = openEtcdMetadataRepository
var discoverGatewayControlEndpointFunc = discoverGatewayControlEndpoint

type gatewayFleetPageLister interface {
	ListGatewayFleetPage(context.Context, metadata.GatewayFleetListOptions) (metadata.GatewayFleetPage, error)
}

func resolveDefaultGatewayFlag(fs *flag.FlagSet, settings []resolvedSetting) {
	gatewayFlag := fs.Lookup("gateway")
	if gatewayFlag == nil {
		return
	}
	for _, setting := range settings {
		if setting.Key != "gateway" || sourceForFlag(fs, setting, "gateway").Source != "default" {
			continue
		}
		endpointsFlag := fs.Lookup("etcd-endpoints")
		rootFlag := fs.Lookup("etcd-root")
		limitFlag := fs.Lookup("gateway-discovery-limit")
		if endpointsFlag == nil || rootFlag == nil || limitFlag == nil {
			return
		}
		limit, err := strconv.ParseInt(limitFlag.Value.String(), 10, 64)
		if err != nil || limit < 1 || limit > metadata.MaxGatewayFleetPageSize {
			fatalf("--gateway-discovery-limit must be between 1 and %d", metadata.MaxGatewayFleetPageSize)
		}
		timeout := 10 * time.Second
		if timeoutFlag := fs.Lookup("timeout"); timeoutFlag != nil {
			if parsed, err := time.ParseDuration(timeoutFlag.Value.String()); err == nil && parsed > 0 {
				timeout = parsed
			}
		}
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		endpoint, err := discoverGatewayControlEndpointFunc(ctx, &etcdMetadataCLIConfig{
			etcdEndpoints: endpointsFlag.Value.String(),
			etcdRoot:      rootFlag.Value.String(),
		}, limit)
		cancel()
		if err != nil {
			fatalf("discover gateway control endpoint from etcd: %v", err)
		}
		if err := gatewayFlag.Value.Set(endpoint); err != nil {
			fatalf("apply discovered gateway endpoint: %v", err)
		}
		fmt.Fprintf(os.Stderr, "gateway=%s (source=etcd-fleet)\n", endpoint)
		return
	}
}

func discoverGatewayControlEndpoint(ctx context.Context, cfg *etcdMetadataCLIConfig, limit int64) (string, error) {
	repo, closeFn, err := openEtcdMetadataRepositoryFunc(cfg)
	if err != nil {
		return "", err
	}
	defer closeFn()
	lister, ok := repo.(gatewayFleetPageLister)
	if !ok {
		return "", fmt.Errorf("metadata repository does not support bounded gateway fleet pages")
	}
	page, err := lister.ListGatewayFleetPage(ctx, metadata.GatewayFleetListOptions{Limit: limit})
	if err != nil {
		return "", err
	}
	records := append([]service.GatewayRecord(nil), page.Records...)
	sort.Slice(records, func(i, j int) bool { return records[i].GatewayID < records[j].GatewayID })
	now := time.Now().Unix()
	for _, rec := range records {
		rec = service.NormalizeGatewayFleetRecord(rec)
		if rec.Product != service.GatewayProductNAMRBD || rec.Role != service.GatewayRoleBlock ||
			rec.ConnectionState != service.GatewayStateUp || rec.Readiness != service.GatewayReadinessReady ||
			rec.DrainState != service.GatewayDrainActive || (rec.LeaseExpiresAtUnix > 0 && rec.LeaseExpiresAtUnix <= now) {
			continue
		}
		for _, endpoint := range rec.ControlEndpoints {
			address := strings.TrimSpace(endpoint.Address)
			if address == "" || endpoint.Port == 0 {
				continue
			}
			scheme := "http"
			if endpoint.UseTLS {
				scheme = "https"
			}
			return scheme + "://" + net.JoinHostPort(address, strconv.Itoa(int(endpoint.Port))), nil
		}
	}
	return "", fmt.Errorf("no ready active NAMRBD gateway with a control endpoint in the first %d fleet records", limit)
}

func newGatewayClient(baseURL string, opts ...gatewayClientOptions) *gatewayClient {
	httpClient := &http.Client{}
	if len(opts) > 0 && opts[0].Timeout > 0 {
		httpClient.Timeout = opts[0].Timeout
	}
	if len(opts) > 0 && opts[0].CAFile != "" {
		pemData, err := os.ReadFile(opts[0].CAFile)
		if err != nil {
			fatalf("read gateway-ca-file: %v", err)
		}
		pool, err := x509.SystemCertPool()
		if err != nil || pool == nil {
			pool = x509.NewCertPool()
		}
		if !pool.AppendCertsFromPEM(pemData) {
			fatalf("parse gateway-ca-file: no certificates found")
		}
		httpClient.Transport = &http.Transport{
			TLSClientConfig: &tls.Config{
				MinVersion: tls.VersionTLS12,
				RootCAs:    pool,
			},
		}
	}
	return &gatewayClient{
		baseURL:    strings.TrimRight(baseURL, "/"),
		httpClient: httpClient,
	}
}

func runInfo(args []string) {
	defaults := mustResolveCLIDefaults(args)
	fs := newCommandFlagSet("info", flag.ExitOnError)
	registerContextFlags(fs, defaults)
	gateway := fs.String("gateway", defaults.gatewayEndpoint(), "gateway base URL")
	gatewayCAFile := fs.String("gateway-ca-file", defaults.gatewayCAFile(), "PEM CA bundle for gateway TLS")
	rawVolumeID := fs.String("volume", "", "volume id (8 lowercase hex digits)")
	timeout := fs.Duration("timeout", defaults.timeout(10*time.Second), "gateway request timeout")
	fs.Parse(args)
	printResolvedSettings(fs, defaults.gatewayEndpointSetting(), defaults.gatewayCASetting(), defaults.timeoutSetting(10*time.Second))
	volumeID, err := parseVolumeID(*rawVolumeID)
	if err != nil || volumeID == 0 {
		fatalf("--volume is required")
	}
	cli := newGatewayClientFunc(*gateway, gatewayClientOptions{CAFile: *gatewayCAFile, Timeout: *timeout})
	info, err := cli.info(volumeID)
	if err != nil {
		fatalf("info failed: %v", err)
	}
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	_ = enc.Encode(info)
}

func runDiscoverGateways(args []string) {
	defaults := mustResolveCLIDefaults(args)
	fs := newCommandFlagSet("discover-gateways", flag.ExitOnError)
	registerContextFlags(fs, defaults)
	gateway := fs.String("gateway", defaults.gatewayEndpoint(), "gateway base URL")
	gatewayCAFile := fs.String("gateway-ca-file", defaults.gatewayCAFile(), "PEM CA bundle for gateway TLS")
	timeout := fs.Duration("timeout", defaults.timeout(10*time.Second), "gateway request timeout")
	fs.Parse(args)
	printResolvedSettings(fs, defaults.gatewayEndpointSetting(), defaults.gatewayCASetting(), defaults.timeoutSetting(10*time.Second))

	cli := newGatewayClientFunc(*gateway, gatewayClientOptions{CAFile: *gatewayCAFile, Timeout: *timeout})
	discovery, err := cli.discoveryGateways()
	if err != nil {
		fatalf("discover-gateways failed: %v", err)
	}
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	_ = enc.Encode(discovery)
}

func runDiscoverVolume(args []string) {
	defaults := mustResolveCLIDefaults(args)
	fs := newCommandFlagSet("discover-volume", flag.ExitOnError)
	registerContextFlags(fs, defaults)
	gateway := fs.String("gateway", defaults.gatewayEndpoint(), "gateway base URL")
	gatewayCAFile := fs.String("gateway-ca-file", defaults.gatewayCAFile(), "PEM CA bundle for gateway TLS")
	rawVolumeID := fs.String("volume", "", "volume id (8 lowercase hex digits)")
	maxPaths := fs.Int("max-paths", 0, "limit active dataplane paths in output (0 = all)")
	ownerOnly := fs.Bool("owner-only", false, "show only owner-gateway dataplane paths")
	timeout := fs.Duration("timeout", defaults.timeout(10*time.Second), "gateway request timeout")
	fs.Parse(args)
	printResolvedSettings(fs, defaults.gatewayEndpointSetting(), defaults.gatewayCASetting(), defaults.timeoutSetting(10*time.Second))
	volumeID, err := parseVolumeID(*rawVolumeID)
	if err != nil || volumeID == 0 {
		fatalf("--volume is required")
	}

	cli := newGatewayClientFunc(*gateway, gatewayClientOptions{CAFile: *gatewayCAFile, Timeout: *timeout})
	discovery, err := cli.discoveryVolume(volumeID)
	if err != nil {
		fatalf("discover-volume failed: %v", err)
	}
	discovery = selectDiscoveryPaths(discovery, discoveryMergeOptions{
		MaxPaths:  *maxPaths,
		OwnerOnly: *ownerOnly,
	})
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	_ = enc.Encode(discovery)
}

func runPlanVolumePaths(args []string) {
	defaults := mustResolveCLIDefaults(args)
	fs := newCommandFlagSet("plan-volume-paths", flag.ExitOnError)
	registerContextFlags(fs, defaults)
	gateway := fs.String("gateway", defaults.gatewayEndpoint(), "gateway base URL")
	gatewayCAFile := fs.String("gateway-ca-file", defaults.gatewayCAFile(), "PEM CA bundle for gateway TLS")
	rawVolumeID := fs.String("volume", "", "volume id (8 lowercase hex digits)")
	maxActive := fs.Int("max-active", 0, "maximum active dataplane paths (0 = all non-down)")
	var pathHealthSpecs multiString
	fs.Var(&pathHealthSpecs, "path-health", "path health override: PATH_ID=healthy|suspect|down")
	timeout := fs.Duration("timeout", defaults.timeout(10*time.Second), "gateway request timeout")
	fs.Parse(args)
	printResolvedSettings(fs, defaults.gatewayEndpointSetting(), defaults.gatewayCASetting(), defaults.timeoutSetting(10*time.Second))
	volumeID, err := parseVolumeID(*rawVolumeID)
	if err != nil || volumeID == 0 {
		fatalf("--volume is required")
	}

	pathHealth := make(map[string]string, len(pathHealthSpecs))
	for _, spec := range pathHealthSpecs {
		pathID, state, err := parsePathHealthSpec(spec)
		if err != nil {
			fatalf("invalid --path-health %q: %v", spec, err)
		}
		pathHealth[pathID] = state
	}

	cli := newGatewayClientFunc(*gateway, gatewayClientOptions{CAFile: *gatewayCAFile, Timeout: *timeout})
	plan, err := cli.discoveryPathPlan(volumeID, *maxActive, pathHealth)
	if err != nil {
		fatalf("plan-volume-paths failed: %v", err)
	}
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	_ = enc.Encode(plan)
}

func runClusterMetrics(args []string) {
	defaults := mustResolveCLIDefaults(args)
	fs := newCommandFlagSet("cluster-metrics", flag.ExitOnError)
	registerContextFlags(fs, defaults)
	gateway := fs.String("gateway", defaults.gatewayEndpoint(), "gateway base URL")
	gatewayCAFile := fs.String("gateway-ca-file", defaults.gatewayCAFile(), "PEM CA bundle for gateway TLS")
	timeout := fs.Duration("timeout", defaults.timeout(10*time.Second), "gateway request timeout")
	fs.Parse(args)
	printResolvedSettings(fs, defaults.gatewayEndpointSetting(), defaults.gatewayCASetting(), defaults.timeoutSetting(10*time.Second))
	if *gateway == "" {
		fatalf("--gateway is required")
	}
	cli := newGatewayClientFunc(*gateway, gatewayClientOptions{CAFile: *gatewayCAFile, Timeout: *timeout})
	metrics, err := cli.clusterMetrics()
	if err != nil {
		fatalf("cluster-metrics failed: %v", err)
	}
	if raw, ok := metrics["path_plan"].(map[string]any); ok {
		metrics["path_plan_priority_counts"] = summarizePathPlanPriorityCounts(raw)
		if topClass, topCount := summarizeTopPathPlanPriority(raw); topClass != "" {
			metrics["top_priority_class"] = topClass
			metrics["top_priority_count"] = topCount
		}
	}
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	_ = enc.Encode(metrics)
}

type etcdMetadataCLIConfig struct {
	etcdEndpoints string
	etcdRoot      string
	jsonOutput    bool
}

type namrbdctlDirectEtcdMetadataRepository interface {
	CreateVolume(ctx context.Context, req service.VolumeCreateRequest) (service.VolumeSpec, error)
	UpdateVolume(ctx context.Context, volumeID uint64, req service.VolumeUpdateRequest) (service.VolumeSpec, error)
	DeleteVolume(ctx context.Context, volumeID uint64) error
	GetVolume(ctx context.Context, volumeID uint64) (service.VolumeSpec, error)
	GetVolumeStatus(ctx context.Context, volumeID uint64) (service.VolumeStatusRecord, error)
	ListVolumes(ctx context.Context) ([]service.VolumeSpec, error)
	SetVolumeState(ctx context.Context, volumeID uint64, state service.VolumeLifecycleState) (service.VolumeSpec, error)
	GetAttachment(ctx context.Context, volumeID uint64) (service.AttachmentRecord, error)
	GetGeneration(ctx context.Context, volumeID uint64) (uint64, error)
	GetGateway(ctx context.Context, gatewayID string) (service.GatewayRecord, error)
	ListGateways(ctx context.Context) ([]service.GatewayRecord, error)
	PutGateway(ctx context.Context, rec service.GatewayRecord) error
}

func newEtcdMetadataFlagSet(name string) (*flag.FlagSet, *etcdMetadataCLIConfig) {
	fs := newCommandFlagSet(name, flag.ExitOnError)
	cfg := &etcdMetadataCLIConfig{}
	fs.StringVar(&cfg.etcdEndpoints, "etcd-endpoints", "127.0.0.1:2379", "comma-separated etcd endpoints")
	fs.StringVar(&cfg.etcdRoot, "etcd-root", "/namrbd", "etcd metadata root prefix")
	fs.BoolVar(&cfg.jsonOutput, "json", globalJSONOutput, "emit JSON output")
	return fs, cfg
}

func runGatewayList(args []string) {
	fs, cfg := newEtcdMetadataFlagSet("gateway-list")
	fs.Parse(args)
	repo, closeFn := mustOpenEtcdMetadataRepository(cfg)
	defer closeFn()
	records, err := repo.ListGateways(context.Background())
	if err != nil {
		fatalf("gateway-list failed: %v", err)
	}
	if cfg.jsonOutput {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		_ = enc.Encode(records)
		return
	}
	for _, rec := range records {
		fmt.Printf("gateway_id=%s state=%s cluster_id=%s metadata_root=%s last_seen_unix=%d control_endpoints=%d dataplane_endpoints=%d\n",
			rec.GatewayID, rec.ConnectionState, rec.ClusterID, rec.MetadataRoot, rec.LastSeenUnix, len(rec.ControlEndpoints), len(rec.DataplaneEndpoints))
	}
}

func runGatewayGet(args []string) {
	fs, cfg := newEtcdMetadataFlagSet("gateway-get")
	gatewayID := fs.String("gateway", "", "gateway id")
	fs.Parse(args)
	if strings.TrimSpace(*gatewayID) == "" {
		fatalf("--gateway is required")
	}
	repo, closeFn := mustOpenEtcdMetadataRepository(cfg)
	defer closeFn()
	rec, err := repo.GetGateway(context.Background(), *gatewayID)
	if err != nil {
		fatalf("gateway-get failed: %v", err)
	}
	if cfg.jsonOutput {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		_ = enc.Encode(rec)
		return
	}
	fmt.Printf("gateway_id=%s\nconnection_state=%s\ncluster_id=%s\nmetadata_backend=%s\nmetadata_root=%s\nfailure_domain=%s\nlast_seen_unix=%d\nlast_seen=%s\nlease_id=%s\nlease_expires_at_unix=%d\nlease_expires_at=%s\n",
		rec.GatewayID, rec.ConnectionState, rec.ClusterID, rec.MetadataBackend, rec.MetadataRoot, rec.FailureDomain, rec.LastSeenUnix, formatUnix(rec.LastSeenUnix), rec.LeaseID, rec.LeaseExpiresAtUnix, formatUnix(rec.LeaseExpiresAtUnix))
	printGatewayEndpoints("control_endpoints", rec.ControlEndpoints)
	printGatewayEndpoints("dataplane_endpoints", rec.DataplaneEndpoints)
}

func runGatewayPut(args []string) {
	fs, cfg := newEtcdMetadataFlagSet("gateway-put")
	gatewayID := fs.String("gateway", "", "gateway id override")
	fromFile := fs.String("from-file", "", "JSON file containing gateway record")
	fs.Parse(args)
	if strings.TrimSpace(*fromFile) == "" {
		fatalf("--from-file is required")
	}
	rec, err := loadGatewayRecord(*fromFile)
	if err != nil {
		fatalf("load gateway file failed: %v", err)
	}
	if strings.TrimSpace(*gatewayID) != "" {
		rec.GatewayID = *gatewayID
	}
	if strings.TrimSpace(rec.GatewayID) == "" {
		fatalf("gateway record missing gateway_id")
	}
	repo, closeFn := mustOpenEtcdMetadataRepository(cfg)
	defer closeFn()
	if err := repo.PutGateway(context.Background(), rec); err != nil {
		fatalf("gateway-put failed: %v", err)
	}
	if cfg.jsonOutput {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		_ = enc.Encode(rec)
		return
	}
	fmt.Printf("ok gateway_id=%s state=%s\n", rec.GatewayID, rec.ConnectionState)
}

func runAttachmentGet(args []string) {
	fs, cfg := newEtcdMetadataFlagSet("attachment-get")
	rawVolumeID := fs.String("volume", "", "volume id (8 lowercase hex digits)")
	fs.Parse(args)
	volumeID, err := parseVolumeID(*rawVolumeID)
	if err != nil || volumeID == 0 {
		fatalf("--volume is required")
	}
	repo, closeFn := mustOpenEtcdMetadataRepository(cfg)
	defer closeFn()
	rec, err := repo.GetAttachment(context.Background(), volumeID)
	if err != nil {
		fatalf("attachment-get failed: %v", err)
	}
	if cfg.jsonOutput {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		_ = enc.Encode(rec)
		return
	}
	fmt.Printf("generation=%d\nattachment_id=%s\nhost_id=%s\ndevice_id=%d\nowner_gateway_id=%s\nlease_id=%s\nattached_at_unix=%d\nattached_at=%s\n",
		rec.Generation, rec.AttachmentID, rec.HostID, rec.DeviceID, rec.OwnerGatewayID, rec.LeaseID, rec.AttachedAtUnix, formatUnix(rec.AttachedAtUnix))
}

func runVolumeList(args []string) {
	fs, cfg := newEtcdMetadataFlagSet("volume-list")
	fs.Parse(args)
	repo, closeFn := mustOpenEtcdMetadataRepository(cfg)
	defer closeFn()
	volumes, err := repo.ListVolumes(context.Background())
	if err != nil {
		fatalf("volume-list failed: %v", err)
	}
	if cfg.jsonOutput {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		_ = enc.Encode(volumeSpecOutputs(volumes))
		return
	}
	for _, volume := range volumes {
		fmt.Printf("volume_id=%s name=%s prefix=%s size_bytes=%d block_size=%d allocation_chunk_size_bytes=%d allocation_page_bytes=%d chunk_size_bytes=%d extent_page_bytes=%d access_mode=%s state=%s\n",
			service.CanonicalVolumeID(uint64(volume.ID)), volume.Name, volume.Prefix, volume.SizeBytes, volume.BlockSize, volume.ChunkSizeBytes, volume.ExtentPageBytes, volume.ChunkSizeBytes, volume.ExtentPageBytes, volume.AccessMode, volume.State)
	}
}

func runVolumeCreate(args []string) {
	fs, cfg := newEtcdMetadataFlagSet("volume-create")
	name := fs.String("name", "", "globally unique volume name")
	size := fs.String("size", "", "volume size with unit: <n>M|<n>G|<n>T")
	blockSizeStr := fs.String("block-size", "4K", "block size as <n>K only (binary KiB, e.g. 4K)")
	allocationChunkSizeStr := fs.String("allocation-chunk-size", "64K", "allocation chunk size as <n>K|<n>M (binary KiB/MiB)")
	allocationPageSizeStr := fs.String("allocation-page-size", "4M", "allocation page size as <n>M|<n>G (binary MiB/GiB)")
	accessMode := fs.String("access-mode", string(service.VolumeAccessModeExclusive), "access mode: exclusive|shared")
	rawState := fs.String("state", string(service.VolumeStateAvailable), "initial state: available|disabled")
	fs.Parse(args)
	if strings.TrimSpace(*size) == "" {
		fatalf("--size is required")
	}
	resolvedSizeBytes, err := parseSizeWithUnit(*size)
	if err != nil {
		fatalf("invalid --size: %v", err)
	}
	blockBytes, err := parseBlockSizeK(*blockSizeStr)
	if err != nil {
		fatalf("invalid --block-size: %v", err)
	}
	chunkBytes, err := parseAllocationChunkSizeWithUnit(*allocationChunkSizeStr)
	if err != nil {
		fatalf("invalid --allocation-chunk-size: %v", err)
	}
	extentBytes, err := parseAllocationPageSizeWithUnit(*allocationPageSizeStr)
	if err != nil {
		fatalf("invalid --allocation-page-size: %v", err)
	}
	state, err := parseVolumeState(*rawState)
	if err != nil {
		fatalf("invalid --state: %v", err)
	}
	mode, err := parseVolumeAccessMode(*accessMode)
	if err != nil {
		fatalf("invalid --access-mode: %v", err)
	}
	repo, closeFn := mustOpenEtcdMetadataRepository(cfg)
	defer closeFn()
	volume, err := repo.CreateVolume(context.Background(), service.VolumeCreateRequest{
		Name:            *name,
		SizeBytes:       resolvedSizeBytes,
		BlockSize:       uint32(blockBytes),
		ChunkSizeBytes:  uint32(chunkBytes),
		ExtentPageBytes: uint32(extentBytes),
		AccessMode:      mode,
		State:           state,
	})
	if err != nil {
		fatalf("volume-create failed: %v", err)
	}
	if cfg.jsonOutput {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		_ = enc.Encode(volumeSpecOutputFrom(volume))
		return
	}
	fmt.Printf("ok volume_id=%s name=%s prefix=%s allocation_chunk_size_bytes=%d allocation_page_bytes=%d chunk_size_bytes=%d extent_page_bytes=%d\n",
		service.CanonicalVolumeID(uint64(volume.ID)), volume.Name, volume.Prefix, volume.ChunkSizeBytes, volume.ExtentPageBytes, volume.ChunkSizeBytes, volume.ExtentPageBytes)
}

func runVolumeGet(args []string) {
	fs, cfg := newEtcdMetadataFlagSet("volume-get")
	rawVolumeID := fs.String("volume", "", "volume id (8 lowercase hex digits)")
	fs.Parse(args)
	volumeID, err := parseVolumeID(*rawVolumeID)
	if err != nil || volumeID == 0 {
		fatalf("--volume is required")
	}
	repo, closeFn := mustOpenEtcdMetadataRepository(cfg)
	defer closeFn()
	volume, err := repo.GetVolume(context.Background(), volumeID)
	if err != nil {
		fatalf("volume-get failed: %v", err)
	}
	if cfg.jsonOutput {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		_ = enc.Encode(volumeSpecOutputFrom(volume))
		return
	}
	printVolumeSpec(volume)
}

func runVolumeUpdate(args []string) {
	fs, cfg := newEtcdMetadataFlagSet("volume-update")
	rawVolumeID := fs.String("volume", "", "volume id (8 lowercase hex digits)")
	name := fs.String("name", "", "globally unique volume name")
	size := fs.String("size", "", "target volume size with unit: <n>M|<n>G|<n>T")
	accessMode := fs.String("access-mode", "", "access mode: exclusive|shared")
	rawState := fs.String("state", "", "target state: available|in_use|disabled")
	fs.Parse(args)
	volumeID, err := parseVolumeID(*rawVolumeID)
	if err != nil || volumeID == 0 {
		fatalf("--volume is required")
	}
	req := service.VolumeUpdateRequest{}
	if strings.TrimSpace(*name) != "" {
		req.Name = name
	}
	if strings.TrimSpace(*size) != "" {
		parsed, err := parseSizeWithUnit(*size)
		if err != nil {
			fatalf("invalid --size: %v", err)
		}
		req.SizeBytes = &parsed
	}
	if strings.TrimSpace(*accessMode) != "" {
		mode, err := parseVolumeAccessMode(*accessMode)
		if err != nil {
			fatalf("invalid --access-mode: %v", err)
		}
		req.AccessMode = &mode
	}
	if strings.TrimSpace(*rawState) != "" {
		state, err := parseVolumeState(*rawState)
		if err != nil {
			fatalf("invalid --state: %v", err)
		}
		req.State = &state
	}
	repo, closeFn := mustOpenEtcdMetadataRepository(cfg)
	defer closeFn()
	volume, err := repo.UpdateVolume(context.Background(), volumeID, req)
	if err != nil {
		fatalf("volume-update failed: %v", err)
	}
	if cfg.jsonOutput {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		_ = enc.Encode(volumeSpecOutputFrom(volume))
		return
	}
	fmt.Printf("ok volume_id=%s name=%s prefix=%s allocation_chunk_size_bytes=%d allocation_page_bytes=%d chunk_size_bytes=%d extent_page_bytes=%d state=%s\n",
		service.CanonicalVolumeID(uint64(volume.ID)), volume.Name, volume.Prefix, volume.ChunkSizeBytes, volume.ExtentPageBytes, volume.ChunkSizeBytes, volume.ExtentPageBytes, volume.State)
}

func runVolumeSetState(args []string) {
	fs, cfg := newEtcdMetadataFlagSet("volume-set-state")
	rawVolumeID := fs.String("volume", "", "volume id (8 lowercase hex digits)")
	rawState := fs.String("state", "", "target state: available|in_use|disabled")
	fs.Parse(args)
	volumeID, err := parseVolumeID(*rawVolumeID)
	if err != nil || volumeID == 0 {
		fatalf("--volume is required")
	}
	state, err := parseVolumeState(*rawState)
	if err != nil {
		fatalf("invalid --state: %v", err)
	}
	repo, closeFn := mustOpenEtcdMetadataRepository(cfg)
	defer closeFn()
	volume, err := repo.SetVolumeState(context.Background(), volumeID, state)
	if err != nil {
		fatalf("volume-set-state failed: %v", err)
	}
	if cfg.jsonOutput {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		_ = enc.Encode(volumeSpecOutputFrom(volume))
		return
	}
	fmt.Printf("ok volume_id=%s state=%s\n", service.CanonicalVolumeID(uint64(volume.ID)), volume.State)
}

func runVolumeDelete(args []string) {
	fs, cfg := newEtcdMetadataFlagSet("volume-delete")
	rawVolumeID := fs.String("volume", "", "volume id (8 lowercase hex digits)")
	fs.Parse(args)
	volumeID, err := parseVolumeID(*rawVolumeID)
	if err != nil || volumeID == 0 {
		fatalf("--volume is required")
	}
	repo, closeFn := mustOpenEtcdMetadataRepository(cfg)
	defer closeFn()
	if err := repo.DeleteVolume(context.Background(), volumeID); err != nil {
		fatalf("volume-delete failed: %v", err)
	}
	if cfg.jsonOutput {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		_ = enc.Encode(map[string]any{"volume_id": service.CanonicalVolumeID(volumeID), "deleted": true})
		return
	}
	fmt.Printf("ok volume_id=%s deleted=true\n", service.CanonicalVolumeID(volumeID))
}

func runVolumeStatus(args []string) {
	fs, cfg := newEtcdMetadataFlagSet("volume-status")
	rawVolumeID := fs.String("volume", "", "volume id (8 lowercase hex digits)")
	fs.Parse(args)
	volumeID, err := parseVolumeID(*rawVolumeID)
	if err != nil || volumeID == 0 {
		fatalf("--volume is required")
	}
	repo, closeFn := mustOpenEtcdMetadataRepository(cfg)
	defer closeFn()
	status, err := repo.GetVolumeStatus(context.Background(), volumeID)
	if err != nil {
		fatalf("volume-status failed: %v", err)
	}
	if cfg.jsonOutput {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		_ = enc.Encode(status)
		return
	}
	fmt.Printf("volume_id=%s\nin_use=%t\ncurrent_attachment_id=%s\ncurrent_host_id=%s\ncurrent_gateway_id=%s\ncurrent_gateway_id_note=%s\ngateway_connection_state=%s\ndesired_active_gateway_set=%s\nobserved_active_gateway_set=%s\npath_plan_revision=%d\nattachment_generation=%d\nwriter_fencing_epoch=%d\n",
		service.CanonicalVolumeID(uint64(status.VolumeID)),
		status.InUse,
		status.CurrentAttachmentID,
		status.CurrentHostID,
		status.CurrentGatewayID,
		"compatibility field; use desired_active_gateway_set/observed_active_gateway_set and path_plan_revision for active-active path-plan state",
		status.GatewayConnectionState,
		strings.Join(status.DesiredActiveGatewaySet, ","),
		strings.Join(status.ObservedActiveGatewaySet, ","),
		status.PathPlanRevision,
		status.AttachmentGeneration,
		status.WriterFencingEpoch)
}

func runValidateVolume(args []string) {
	fs, cfg := newEtcdMetadataFlagSet("validate-volume")
	rawVolumeID := fs.String("volume", "", "volume id (8 lowercase hex digits)")
	fs.Parse(args)
	volumeID, err := parseVolumeID(*rawVolumeID)
	if err != nil || volumeID == 0 {
		fatalf("--volume is required")
	}
	repo, closeFn := mustOpenEtcdMetadataRepository(cfg)
	defer closeFn()
	report, err := validateVolumeMetadata(context.Background(), repo, volumeID)
	if err != nil {
		fatalf("validate-volume failed: %v", err)
	}
	if cfg.jsonOutput {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		_ = enc.Encode(report)
		return
	}
	printVolumeValidation(report)
	if !report.OK {
		os.Exit(1)
	}
}

func runValidateAll(args []string) {
	fs, cfg := newEtcdMetadataFlagSet("validate-all")
	fs.Parse(args)
	repo, closeFn := mustOpenEtcdMetadataRepository(cfg)
	defer closeFn()
	report, err := validateAllMetadata(context.Background(), repo)
	if err != nil {
		fatalf("validate-all failed: %v", err)
	}
	if cfg.jsonOutput {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		_ = enc.Encode(report)
		return
	}
	fmt.Printf("ok=%t volume_count=%d gateway_count=%d issue_count=%d\n",
		report.OK, report.VolumeCount, report.GatewayCount, report.IssueCount)
	for _, volumeReport := range report.Volumes {
		printVolumeValidation(volumeReport)
	}
	for _, issue := range report.Issues {
		fmt.Printf("%s %s: %s\n", strings.ToUpper(issue.Severity), issue.Code, issue.Message)
	}
	if !report.OK {
		os.Exit(1)
	}
}

func runApplyVolumePathPlan(client netlinkclient.Client, args []string) {
	defaults := mustResolveCLIDefaults(args)
	fs := newCommandFlagSet("apply-volume-path-plan", flag.ExitOnError)
	deviceID := fs.Uint("device", 0, "device id")
	registerContextFlags(fs, defaults)
	gateway := fs.String("gateway", defaults.gatewayEndpoint(), "gateway base URL")
	gatewayCAFile := fs.String("gateway-ca-file", defaults.gatewayCAFile(), "PEM CA bundle for gateway TLS")
	rawVolumeID := fs.String("volume", "", "volume id (8 lowercase hex digits)")
	maxActive := fs.Int("max-active", 0, "maximum active dataplane paths (0 = all non-down)")
	var pathHealthSpecs multiString
	fs.Var(&pathHealthSpecs, "path-health", "path health override: PATH_ID=healthy|suspect|down")
	timeout := fs.Duration("timeout", defaults.timeout(10*time.Second), "gateway request timeout")
	fs.Parse(args)
	printResolvedSettings(fs, defaults.gatewayEndpointSetting(), defaults.gatewayCASetting(), defaults.timeoutSetting(10*time.Second))
	volumeID, err := parseVolumeID(*rawVolumeID)
	if (*deviceID == 0 && !hasUintFlag(args, "device")) || err != nil || volumeID == 0 {
		fatalf("--device and --volume are required")
	}

	pathHealth := make(map[string]string, len(pathHealthSpecs))
	for _, spec := range pathHealthSpecs {
		pathID, state, err := parsePathHealthSpec(spec)
		if err != nil {
			fatalf("invalid --path-health %q: %v", spec, err)
		}
		pathHealth[pathID] = state
	}

	cli := newGatewayClientFunc(*gateway, gatewayClientOptions{CAFile: *gatewayCAFile, Timeout: *timeout})
	plan, err := cli.discoveryPathPlan(volumeID, *maxActive, pathHealth)
	if err != nil {
		fatalf("apply-volume-path-plan plan fetch failed: %v", err)
	}
	req, err := pathPlanToNetlinkRequest(uint32(*deviceID), plan)
	if err != nil {
		fatalf("apply-volume-path-plan conversion failed: %v", err)
	}
	statusBefore, err := client.GetStatus(uint32(*deviceID))
	if err != nil {
		fatalf("apply-volume-path-plan status failed: %v", err)
	}
	sourcePathPlanRevision := req.PathPlanRevision
	req = adjustRuntimePathPlanRevision(req, statusBefore)
	if err := client.UpdatePathPlan(req); err != nil {
		fatalf("apply-volume-path-plan netlink failed: %v", err)
	}
	status, err := client.GetStatus(uint32(*deviceID))
	if err != nil {
		fatalf("apply-volume-path-plan status failed: %v", err)
	}
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	out := map[string]any{
		"result":                       "ok",
		"device_id":                    *deviceID,
		"source_path_plan_revision":    sourcePathPlanRevision,
		"requested_path_plan_revision": req.PathPlanRevision,
		"applied_path_plan_revision":   status.AppliedPathPlanRevision,
		"path_plan_revision_state":     summarizePathPlanRevisionState(req.PathPlanRevision, status.AppliedPathPlanRevision),
		"path_count":                   status.PathCount,
		"down_mask":                    fmt.Sprintf("0x%x", status.DownMask),
		"degraded_mask":                fmt.Sprintf("0x%x", status.DegradedMask),
		"draining_mask":                fmt.Sprintf("0x%x", status.DrainingMask),
		"runtime_path_plan":            summarizeRuntimePathPlan(status),
		"no_path":                      summarizeNoPathStatus(status),
		"lanes":                        encodeLaneStatuses(status.Lanes),
	}
	out["recommended_actions"] = recommendedRuntimePathPlanActions(out["runtime_path_plan"].(map[string]any))
	_ = enc.Encode(out)
}

func mustOpenEtcdMetadataRepository(cfg *etcdMetadataCLIConfig) (namrbdctlDirectEtcdMetadataRepository, func()) {
	repo, closeFn, err := openEtcdMetadataRepositoryFunc(cfg)
	if err != nil {
		fatalf("open direct-etcd metadata repository failed: %v", err)
	}
	return repo, closeFn
}

func openEtcdMetadataRepository(cfg *etcdMetadataCLIConfig) (namrbdctlDirectEtcdMetadataRepository, func(), error) {
	client, err := metadata.NewEtcdClient(splitCommaList(cfg.etcdEndpoints), 5*time.Second)
	if err != nil {
		return nil, nil, err
	}
	repo := metadata.NewEtcdRepository(client, cfg.etcdRoot)
	return repo, func() { _ = client.Close() }, nil
}

func runRead(args []string) {
	defaults := mustResolveCLIDefaults(args)
	fs := newCommandFlagSet("read", flag.ExitOnError)
	registerContextFlags(fs, defaults)
	gateway := fs.String("gateway", defaults.gatewayEndpoint(), "gateway base URL")
	gatewayCAFile := fs.String("gateway-ca-file", defaults.gatewayCAFile(), "PEM CA bundle for gateway TLS")
	rawVolumeID := fs.String("volume", "", "volume id (8 lowercase hex digits)")
	offset := fs.Uint64("offset", 0, "offset bytes")
	length := fs.Uint64("length", 0, "length bytes")
	outPath := fs.String("out", "", "output file path (optional)")
	timeout := fs.Duration("timeout", defaults.timeout(10*time.Second), "gateway request timeout")
	fs.Parse(args)
	printResolvedSettings(fs, defaults.gatewayEndpointSetting(), defaults.gatewayCASetting(), defaults.timeoutSetting(10*time.Second))
	volumeID, err := parseVolumeID(*rawVolumeID)
	if err != nil || volumeID == 0 || *length == 0 {
		fatalf("--volume and --length are required")
	}

	cli := newGatewayClientFunc(*gateway, gatewayClientOptions{CAFile: *gatewayCAFile, Timeout: *timeout})
	data, err := cli.read(volumeID, *offset, *length)
	if err != nil {
		fatalf("read failed: %v", err)
	}
	if *outPath != "" {
		if err := os.WriteFile(*outPath, data, 0o644); err != nil {
			fatalf("write output file: %v", err)
		}
		writeCommandResult(map[string]any{"result": "ok", "bytes": len(data), "output_file": *outPath}, fmt.Sprintf("ok (%d bytes written to %s)", len(data), *outPath))
		return
	}
	if globalJSONOutput {
		writeCommandResult(map[string]any{"result": "ok", "bytes": len(data), "data_base64": base64.StdEncoding.EncodeToString(data)}, "")
		return
	}
	fmt.Println(base64.StdEncoding.EncodeToString(data))
}

func runWrite(args []string) {
	defaults := mustResolveCLIDefaults(args)
	fs := newCommandFlagSet("write", flag.ExitOnError)
	registerContextFlags(fs, defaults)
	gateway := fs.String("gateway", defaults.gatewayEndpoint(), "gateway base URL")
	gatewayCAFile := fs.String("gateway-ca-file", defaults.gatewayCAFile(), "PEM CA bundle for gateway TLS")
	rawVolumeID := fs.String("volume", "", "volume id (8 lowercase hex digits)")
	offset := fs.Uint64("offset", 0, "offset bytes")
	dataFile := fs.String("data-file", "", "input data file path")
	timeout := fs.Duration("timeout", defaults.timeout(10*time.Second), "gateway request timeout")
	fs.Parse(args)
	printResolvedSettings(fs, defaults.gatewayEndpointSetting(), defaults.gatewayCASetting(), defaults.timeoutSetting(10*time.Second))
	volumeID, err := parseVolumeID(*rawVolumeID)
	if err != nil || volumeID == 0 || *dataFile == "" {
		fatalf("--volume and --data-file are required")
	}
	data, err := os.ReadFile(*dataFile)
	if err != nil {
		fatalf("read data-file: %v", err)
	}
	cli := newGatewayClientFunc(*gateway, gatewayClientOptions{CAFile: *gatewayCAFile, Timeout: *timeout})
	if err := cli.write(volumeID, *offset, data); err != nil {
		fatalf("write failed: %v", err)
	}
	writeCommandResult(map[string]any{"result": "ok", "bytes": len(data)}, fmt.Sprintf("ok (%d bytes)", len(data)))
}

func parseVolumeID(raw string) (uint64, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return 0, fmt.Errorf("volume is required")
	}
	return volumeid.Parse(raw)
}

func splitCommaList(raw string) []string {
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

func printGatewayEndpoints(label string, endpoints []service.EndpointSpec) {
	if len(endpoints) == 0 {
		fmt.Printf("%s=[]\n", label)
		return
	}
	for i, ep := range endpoints {
		fmt.Printf("%s[%d]=%s:%d tls=%t server_name=%s auth_mode=%s path_id=%d priority=%d\n",
			label, i, ep.Address, ep.Port, ep.UseTLS, ep.ServerName, ep.AuthMode, ep.PathID, ep.Priority)
	}
}

func volumeSpecOutputFrom(volume service.VolumeSpec) volumeSpecOutput {
	return volumeSpecOutput{
		VolumeSpec:               volume,
		AllocationChunkSizeBytes: volume.ChunkSizeBytes,
		AllocationPageBytes:      volume.ExtentPageBytes,
	}
}

func volumeSpecOutputs(volumes []service.VolumeSpec) []volumeSpecOutput {
	out := make([]volumeSpecOutput, 0, len(volumes))
	for _, volume := range volumes {
		out = append(out, volumeSpecOutputFrom(volume))
	}
	return out
}

func printVolumeSpec(volume service.VolumeSpec) {
	fmt.Printf("volume_id=%s\nname=%s\nprefix=%s\nsize_bytes=%d\nblock_size=%d\nallocation_chunk_size_bytes=%d\nallocation_page_bytes=%d\nchunk_size_bytes=%d\nextent_page_bytes=%d\naccess_mode=%s\nstate=%s\n",
		service.CanonicalVolumeID(uint64(volume.ID)), volume.Name, volume.Prefix, volume.SizeBytes, volume.BlockSize, volume.ChunkSizeBytes, volume.ExtentPageBytes, volume.ChunkSizeBytes, volume.ExtentPageBytes, volume.AccessMode, volume.State)
}

func validateVolumeMetadata(ctx context.Context, repo namrbdctlDirectEtcdMetadataRepository, volumeID uint64) (volumeValidationReport, error) {
	volume, err := repo.GetVolume(ctx, volumeID)
	if err != nil {
		return volumeValidationReport{}, err
	}
	status, err := repo.GetVolumeStatus(ctx, volumeID)
	if err != nil {
		return volumeValidationReport{}, err
	}
	attachment, err := repo.GetAttachment(ctx, volumeID)
	if err != nil {
		return volumeValidationReport{}, err
	}
	generation, err := repo.GetGeneration(ctx, volumeID)
	if err != nil {
		return volumeValidationReport{}, err
	}
	issues := make([]validationIssue, 0)
	addIssue := func(code, message string) {
		issues = append(issues, validationIssue{Severity: "error", Code: code, Message: message})
	}
	addWarning := func(code, message string) {
		issues = append(issues, validationIssue{Severity: "warning", Code: code, Message: message})
	}

	if volume.Name == "" {
		addIssue("volume_name_empty", "volume_name is empty")
	}
	if volume.Prefix == "" {
		addIssue("volume_prefix_empty", "data_key_prefix is empty")
	}
	if volume.SizeBytes == 0 {
		addIssue("volume_size_zero", "size_bytes is zero")
	}
	if volume.BlockSize == 0 {
		addIssue("block_size_zero", "block_size is zero")
	}
	if generation == 0 {
		addIssue("generation_zero", "generation is zero")
	}
	if uint64(status.VolumeID) != volumeID {
		addIssue("status_volume_mismatch", fmt.Sprintf("status volume_id=%s does not match requested volume_id=%s", service.CanonicalVolumeID(uint64(status.VolumeID)), service.CanonicalVolumeID(volumeID)))
	}

	hasAttachment := attachment.AttachmentID != ""
	if status.InUse && !hasAttachment {
		addIssue("in_use_without_attachment", "status reports in_use=true but current attachment is empty")
	}
	if !status.InUse && hasAttachment {
		addIssue("attachment_without_in_use", "attachment exists but status reports in_use=false")
	}
	if hasAttachment {
		if attachment.Generation == 0 {
			addIssue("attachment_generation_zero", "attachment generation is zero")
		}
		if status.CurrentAttachmentID != "" && status.CurrentAttachmentID != attachment.AttachmentID {
			addIssue("attachment_id_mismatch", "status current_attachment_id does not match attachment record")
		}
		if status.CurrentHostID != "" && status.CurrentHostID != attachment.HostID {
			addIssue("attachment_host_mismatch", "status current_host_id does not match attachment record")
		}
		if status.CurrentGatewayID != "" && status.CurrentGatewayID != attachment.OwnerGatewayID {
			addWarning("attachment_gateway_mismatch_compat", "status current_gateway_id does not match attachment owner_gateway_id; current_gateway_id is a compatibility field in the active-active path-plan model")
		}
	} else {
		if status.CurrentAttachmentID != "" {
			addIssue("status_attachment_stale", "status current_attachment_id is set but attachment record is empty")
		}
		if status.CurrentHostID != "" {
			addIssue("status_host_stale", "status current_host_id is set but attachment record is empty")
		}
		if status.CurrentGatewayID != "" {
			addWarning("status_gateway_stale_compat", "status current_gateway_id is set but attachment record is empty; current_gateway_id is a compatibility field in the active-active path-plan model")
		}
	}
	if volume.State == service.VolumeStateInUse && !status.InUse {
		addIssue("volume_state_inuse_mismatch", "volume state is in_use but status reports in_use=false")
	}
	if volume.State == service.VolumeStateAvailable && status.InUse {
		addIssue("volume_state_available_mismatch", "volume state is available but status reports in_use=true")
	}
	return volumeValidationReport{
		VolumeID:   service.HexVolumeID(volumeID),
		OK:         !hasValidationErrors(issues),
		Generation: generation,
		IssueCount: len(issues),
		Issues:     issues,
		Volume:     volumeSpecOutputFrom(volume),
		Status:     status,
		Attachment: attachment,
	}, nil
}

func validateAllMetadata(ctx context.Context, repo namrbdctlDirectEtcdMetadataRepository) (allValidationReport, error) {
	volumes, err := repo.ListVolumes(ctx)
	if err != nil {
		return allValidationReport{}, err
	}
	gateways, err := repo.ListGateways(ctx)
	if err != nil {
		return allValidationReport{}, err
	}

	volumeReports := make([]volumeValidationReport, 0, len(volumes))
	issues := make([]validationIssue, 0)
	gatewayIDs := make(map[string]struct{}, len(gateways))
	var (
		refClusterID       string
		refMetadataBackend string
		refMetadataRoot    string
	)
	for _, gateway := range gateways {
		gatewayIDs[gateway.GatewayID] = struct{}{}
		if gateway.GatewayID == "" {
			issues = append(issues, validationIssue{Severity: "error", Code: "gateway_id_empty", Message: "gateway record has empty gateway_id"})
		}
		if refClusterID == "" && gateway.ClusterID != "" {
			refClusterID = gateway.ClusterID
		}
		if refMetadataBackend == "" && gateway.MetadataBackend != "" {
			refMetadataBackend = gateway.MetadataBackend
		}
		if refMetadataRoot == "" && gateway.MetadataRoot != "" {
			refMetadataRoot = gateway.MetadataRoot
		}
		if refClusterID != "" && gateway.ClusterID != "" && gateway.ClusterID != refClusterID {
			issues = append(issues, validationIssue{Severity: "error", Code: "gateway_cluster_id_mismatch", Message: fmt.Sprintf("gateway %s cluster_id=%s does not match registry cluster_id=%s", gateway.GatewayID, gateway.ClusterID, refClusterID)})
		}
		if refMetadataBackend != "" && gateway.MetadataBackend != "" && gateway.MetadataBackend != refMetadataBackend {
			issues = append(issues, validationIssue{Severity: "error", Code: "gateway_metadata_backend_mismatch", Message: fmt.Sprintf("gateway %s metadata_backend=%s does not match registry metadata_backend=%s", gateway.GatewayID, gateway.MetadataBackend, refMetadataBackend)})
		}
		if refMetadataRoot != "" && gateway.MetadataRoot != "" && gateway.MetadataRoot != refMetadataRoot {
			issues = append(issues, validationIssue{Severity: "error", Code: "gateway_metadata_root_mismatch", Message: fmt.Sprintf("gateway %s metadata_root=%s does not match registry metadata_root=%s", gateway.GatewayID, gateway.MetadataRoot, refMetadataRoot)})
		}
	}
	for _, volume := range volumes {
		report, err := validateVolumeMetadata(ctx, repo, uint64(volume.ID))
		if err != nil {
			return allValidationReport{}, err
		}
		if report.Status.CurrentGatewayID != "" {
			if _, ok := gatewayIDs[report.Status.CurrentGatewayID]; !ok {
				report.IssueCount++
				report.Issues = append(report.Issues, validationIssue{
					Severity: "warning",
					Code:     "current_gateway_missing_compat",
					Message:  fmt.Sprintf("status current_gateway_id=%s has no gateway record; current_gateway_id is a compatibility field in the active-active path-plan model", report.Status.CurrentGatewayID),
				})
			}
		}
		for _, gatewayID := range report.Status.DesiredActiveGatewaySet {
			if _, ok := gatewayIDs[gatewayID]; !ok {
				report.IssueCount++
				report.Issues = append(report.Issues, validationIssue{
					Severity: "error",
					Code:     "desired_gateway_missing",
					Message:  fmt.Sprintf("desired_active_gateway_set gateway_id=%s has no gateway record", gatewayID),
				})
			}
		}
		for _, gatewayID := range report.Status.ObservedActiveGatewaySet {
			if _, ok := gatewayIDs[gatewayID]; !ok {
				report.IssueCount++
				report.Issues = append(report.Issues, validationIssue{
					Severity: "error",
					Code:     "observed_gateway_missing",
					Message:  fmt.Sprintf("observed_active_gateway_set gateway_id=%s has no gateway record", gatewayID),
				})
			}
		}
		report.OK = !hasValidationErrors(report.Issues)
		issues = append(issues, report.Issues...)
		volumeReports = append(volumeReports, report)
	}

	return allValidationReport{
		OK:           !hasValidationErrors(issues),
		VolumeCount:  len(volumes),
		GatewayCount: len(gateways),
		IssueCount:   len(issues),
		Volumes:      volumeReports,
		Issues:       issues,
	}, nil
}

func hasValidationErrors(issues []validationIssue) bool {
	for _, issue := range issues {
		if issue.Severity == "error" {
			return true
		}
	}
	return false
}

func printVolumeValidation(report volumeValidationReport) {
	fmt.Printf("volume_id=%s ok=%t generation=%d issue_count=%d\n",
		service.CanonicalVolumeID(uint64(report.VolumeID)), report.OK, report.Generation, report.IssueCount)
	for _, issue := range report.Issues {
		fmt.Printf("%s %s: %s\n", strings.ToUpper(issue.Severity), issue.Code, issue.Message)
	}
}

func (c *gatewayClient) info(volumeID uint64) (map[string]any, error) {
	url := fmt.Sprintf("%s/api/v1/volumes/%s/info", c.baseURL, service.CanonicalVolumeID(volumeID))
	resp, err := c.httpClient.Get(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("status=%d body=%s", resp.StatusCode, string(b))
	}
	var out map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, err
	}
	return out, nil
}

func (c *gatewayClient) reloadSize(volumeID uint64) (map[string]any, error) {
	url := fmt.Sprintf("%s/api/v1/volumes/%s/reload-size", c.baseURL, service.CanonicalVolumeID(volumeID))
	return c.postJSON(url, map[string]any{})
}

func (c *gatewayClient) discoveryPathPlan(volumeID uint64, maxActive int, pathHealth map[string]string) (map[string]any, error) {
	url := fmt.Sprintf("%s/api/v1/debug/discovery/volumes/%s/path-plan", c.baseURL, service.CanonicalVolumeID(volumeID))
	reqBody := map[string]any{
		"max_active":  maxActive,
		"path_health": pathHealth,
	}
	return c.postJSON(url, reqBody)
}

func (c *gatewayClient) reportRuntimeFeedback(volumeID uint64, payload map[string]any) (map[string]any, error) {
	url := fmt.Sprintf("%s/api/v1/debug/discovery/volumes/%s/runtime-feedback", c.baseURL, service.CanonicalVolumeID(volumeID))
	return c.postJSON(url, payload)
}

func (c *gatewayClient) attach(volumeID uint64, hostID string, deviceID uint32) (string, error) {
	url := fmt.Sprintf("%s/api/v1/volumes/%s/attach", c.baseURL, service.CanonicalVolumeID(volumeID))
	reqBody := map[string]any{
		"host_id":   hostID,
		"device_id": deviceID,
	}
	b, _ := json.Marshal(reqBody)
	resp, err := c.httpClient.Post(url, "application/json", bytes.NewReader(b))
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return "", fmt.Errorf("status=%d body=%s", resp.StatusCode, string(body))
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	return string(body), nil
}

func (c *gatewayClient) discoveryGateways() (map[string]any, error) {
	url := fmt.Sprintf("%s/api/v1/discovery/gateways", c.baseURL)
	return c.getJSON(url)
}

func (c *gatewayClient) discoveryVolume(volumeID uint64) (map[string]any, error) {
	url := fmt.Sprintf("%s/api/v1/discovery/volumes/%s", c.baseURL, service.CanonicalVolumeID(volumeID))
	return c.getJSON(url)
}

func (c *gatewayClient) clusterMetrics() (map[string]any, error) {
	url := fmt.Sprintf("%s/api/v1/debug/sbs-cluster/metrics", c.baseURL)
	return c.getJSON(url)
}

func (c *gatewayClient) postJSON(url string, reqBody any) (map[string]any, error) {
	b, _ := json.Marshal(reqBody)
	resp, err := c.httpClient.Post(url, "application/json", bytes.NewReader(b))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("status=%d body=%s", resp.StatusCode, string(body))
	}
	var out map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, err
	}
	return out, nil
}

func (c *gatewayClient) detach(volumeID uint64, hostID, attachmentID string) error {
	url := fmt.Sprintf("%s/api/v1/volumes/%s/detach", c.baseURL, service.CanonicalVolumeID(volumeID))
	reqBody := map[string]any{
		"host_id":       hostID,
		"attachment_id": attachmentID,
	}
	b, _ := json.Marshal(reqBody)
	resp, err := c.httpClient.Post(url, "application/json", bytes.NewReader(b))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("status=%d body=%s", resp.StatusCode, string(body))
	}
	return nil
}

func (c *gatewayClient) read(volumeID, offset, length uint64) ([]byte, error) {
	url := fmt.Sprintf("%s/api/v1/volumes/%s/read", c.baseURL, service.CanonicalVolumeID(volumeID))
	reqBody := map[string]any{
		"offset_bytes": offset,
		"length_bytes": length,
	}
	b, _ := json.Marshal(reqBody)
	resp, err := c.httpClient.Post(url, "application/json", bytes.NewReader(b))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("status=%d body=%s", resp.StatusCode, string(body))
	}
	var out struct {
		DataBase64 string `json:"data_base64"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, err
	}
	return base64.StdEncoding.DecodeString(out.DataBase64)
}

func (c *gatewayClient) write(volumeID, offset uint64, data []byte) error {
	url := fmt.Sprintf("%s/api/v1/volumes/%s/write", c.baseURL, service.CanonicalVolumeID(volumeID))
	reqBody := map[string]any{
		"offset_bytes": offset,
		"length_bytes": len(data),
		"data_base64":  base64.StdEncoding.EncodeToString(data),
	}
	b, _ := json.Marshal(reqBody)
	resp, err := c.httpClient.Post(url, "application/json", bytes.NewReader(b))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("status=%d body=%s", resp.StatusCode, string(body))
	}
	return nil
}

func (c *gatewayClient) getJSON(url string) (map[string]any, error) {
	resp, err := c.httpClient.Get(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("status=%d body=%s", resp.StatusCode, string(b))
	}
	var out map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, err
	}
	return out, nil
}

type discoveryMergeOptions struct {
	MaxPaths         int
	OwnerOnly        bool
	PreferredGateway string
}

func mergeDiscoveryIntoManifest(manifestJSON string, discovery map[string]any, opts discoveryMergeOptions) (string, error) {
	var manifest map[string]any
	if err := json.Unmarshal([]byte(manifestJSON), &manifest); err != nil {
		return "", err
	}
	discovery = selectDiscoveryPaths(discovery, opts)

	if gateways, ok := discovery["gateways"].([]any); ok {
		controlEndpoints := make([]any, 0)
		for _, gatewayRaw := range gateways {
			gatewayMap, ok := gatewayRaw.(map[string]any)
			if !ok {
				continue
			}
			if endpoints, ok := gatewayMap["control_endpoints"].([]any); ok {
				controlEndpoints = append(controlEndpoints, endpoints...)
			}
		}
		if len(controlEndpoints) > 0 {
			manifest["control_endpoints"] = controlEndpoints
		}
	}

	if dataplanePaths, ok := discovery["dataplane_paths"].([]any); ok && len(dataplanePaths) > 0 {
		endpoints := make([]any, 0, len(dataplanePaths))
		for _, raw := range dataplanePaths {
			path, ok := raw.(map[string]any)
			if !ok {
				continue
			}
			endpoints = append(endpoints, map[string]any{
				"path_id":     path["path_id"],
				"gateway_id":  path["gateway_id"],
				"address":     path["address"],
				"port":        path["port"],
				"use_tls":     path["use_tls"],
				"server_name": path["server_name"],
				"auth_mode":   path["auth_mode"],
				"priority":    path["discovery_priority"],
			})
		}
		if len(endpoints) > 0 {
			manifest["dataplane_endpoints"] = endpoints
		}
	}

	merged, err := json.Marshal(manifest)
	if err != nil {
		return "", err
	}
	return string(merged), nil
}

func selectDiscoveryPaths(discovery map[string]any, opts discoveryMergeOptions) map[string]any {
	if discovery == nil {
		return nil
	}
	gatewaysRaw, _ := discovery["gateways"].([]any)
	pathsRaw, _ := discovery["dataplane_paths"].([]any)
	if len(pathsRaw) == 0 {
		return discovery
	}

	type pathEntry struct {
		raw               map[string]any
		pathID            uint32
		gatewayID         string
		isDesiredGateway  bool
		isObservedGateway bool
		isOwnerGateway    bool
		discoveryPriority uint32
	}
	paths := make([]pathEntry, 0, len(pathsRaw))
	for _, raw := range pathsRaw {
		pathMap, ok := raw.(map[string]any)
		if !ok {
			continue
		}
		entry := pathEntry{
			raw:               cloneMap(pathMap),
			pathID:            anyUint32(pathMap["path_id"]),
			gatewayID:         anyString(pathMap["gateway_id"]),
			isDesiredGateway:  anyBool(pathMap["is_desired_gateway"]),
			isObservedGateway: anyBool(pathMap["is_observed_gateway"]),
			isOwnerGateway:    anyBool(pathMap["is_owner_gateway"]),
			discoveryPriority: anyUint32(pathMap["discovery_priority"]),
		}
		if opts.OwnerOnly && !entry.isOwnerGateway {
			continue
		}
		paths = append(paths, entry)
	}

	sort.Slice(paths, func(i, j int) bool {
		if opts.PreferredGateway != "" && (paths[i].gatewayID == opts.PreferredGateway) != (paths[j].gatewayID == opts.PreferredGateway) {
			return paths[i].gatewayID == opts.PreferredGateway
		}
		if paths[i].isObservedGateway != paths[j].isObservedGateway {
			return paths[i].isObservedGateway
		}
		if paths[i].isDesiredGateway != paths[j].isDesiredGateway {
			return paths[i].isDesiredGateway
		}
		if paths[i].isOwnerGateway != paths[j].isOwnerGateway {
			return paths[i].isOwnerGateway
		}
		if paths[i].discoveryPriority != paths[j].discoveryPriority {
			return paths[i].discoveryPriority > paths[j].discoveryPriority
		}
		if paths[i].gatewayID != paths[j].gatewayID {
			return paths[i].gatewayID < paths[j].gatewayID
		}
		return paths[i].pathID < paths[j].pathID
	})

	if opts.MaxPaths > 0 && len(paths) > opts.MaxPaths {
		paths = paths[:opts.MaxPaths]
	}

	selectedGatewayIDs := make(map[string]struct{}, len(paths))
	hasGatewayIDs := false
	selectedPaths := make([]any, 0, len(paths))
	for i, entry := range paths {
		entry.raw["path_id"] = uint32(i)
		selectedPaths = append(selectedPaths, entry.raw)
		if entry.gatewayID != "" {
			hasGatewayIDs = true
			selectedGatewayIDs[entry.gatewayID] = struct{}{}
		}
	}

	selectedGateways := make([]any, 0, len(gatewaysRaw))
	for _, raw := range gatewaysRaw {
		gatewayMap, ok := raw.(map[string]any)
		if !ok {
			continue
		}
		if hasGatewayIDs {
			if _, ok := selectedGatewayIDs[anyString(gatewayMap["gateway_id"])]; !ok {
				continue
			}
		}
		selectedGateways = append(selectedGateways, cloneMap(gatewayMap))
	}

	out := cloneMap(discovery)
	out["gateways"] = selectedGateways
	out["dataplane_paths"] = selectedPaths
	if volumeRaw, ok := out["volume"].(map[string]any); ok {
		volume := cloneMap(volumeRaw)
		volume["active_gateway_count"] = len(selectedGateways)
		out["volume"] = volume
	}
	return out
}

func cloneMap(in map[string]any) map[string]any {
	out := make(map[string]any, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func prepareGatewayAttachManifest(raw string) (string, []netlinktlv.RESTServer, error) {
	manifest, servers, err := bridge.PrepareAttachManifest(raw)
	if err != nil {
		return "", nil, err
	}
	out := make([]netlinktlv.RESTServer, 0, len(servers))
	for _, server := range servers {
		out = append(out, netlinktlv.RESTServer{
			ID:          server.ID,
			Address:     server.Address,
			Port:        server.Port,
			UseTLS:      server.UseTLS,
			APIPrefix:   server.APIPrefix,
			BearerToken: server.BearerToken,
		})
	}
	return manifest, out, nil
}

func summarizeGatewayAttachManifest(raw string) (map[string]any, error) {
	var manifest map[string]any
	if err := json.Unmarshal([]byte(raw), &manifest); err != nil {
		return nil, err
	}
	out := map[string]any{
		"status": "ok",
	}
	for _, key := range []string{
		"volume_id",
		"generation",
		"attachment_id",
		"attachment_generation",
		"path_plan_revision",
		"path_plan_reapply_requested",
		"path_plan_reapply_reason",
		"path_plan_reapply_requested_at_unix",
		"runtime_path_expansion_backoff_level",
		"runtime_path_expansion_eligible_at_unix",
		"writer_fencing_epoch",
		"handoff_required",
		"handoff_requested_at_unix",
		"handoff_escalation_count",
		"handoff_next_escalation_at_unix",
		"handoff_stage",
		"handoff_reason",
		"handoff_target_gateway_set",
		"controller_priority_class",
		"controller_recommended_actions",
		"cluster_priority_mismatch_actions",
		"path_plan",
		"runtime_no_path",
	} {
		if value, ok := manifest[key]; ok {
			out[key] = value
		}
	}
	if raw, ok := manifest["runtime_path_expansion_eligible_at_unix"]; ok {
		out["controller_runtime_expansion_state"] = summarizeRuntimeExpansionState(anyInt64(raw))
	}
	if raw, ok := manifest["runtime_path_expansion_backoff_level"]; ok {
		out["runtime_path_expansion_backoff_level"] = anyUint64(raw)
		out["controller_runtime_expansion_backoff_level"] = anyUint64(raw)
	}
	if anyString(out["handoff_reason"]) == "handoff_generation_rotation_stalled_current_gateway" {
		out["controller_handoff_backoff_state"] = summarizeHandoffBackoffState(anyInt64(out["handoff_next_escalation_at_unix"]))
	}
	actions := anyStrings(manifest["controller_recommended_actions"])
	if anyString(out["controller_runtime_expansion_state"]) == "eligible" {
		actions = append(actions, "refresh_gateway_path_plan")
	}
	out["operator_recommended_actions"] = dedupeStrings(actions)
	return out, nil
}

func summarizeGatewayManifestInfo(info map[string]any) map[string]any {
	out := map[string]any{}
	for _, key := range []string{
		"volume_id",
		"generation",
		"attachment_id",
		"attachment_generation",
		"path_plan_revision",
		"path_plan_reapply_requested",
		"path_plan_reapply_reason",
		"path_plan_reapply_requested_at_unix",
		"runtime_path_expansion_eligible_at_unix",
		"writer_fencing_epoch",
		"handoff_required",
		"handoff_requested_at_unix",
		"handoff_escalation_count",
		"handoff_next_escalation_at_unix",
		"handoff_stage",
		"handoff_reason",
		"handoff_target_gateway_set",
		"controller_priority_class",
		"controller_recommended_actions",
		"cluster_priority_mismatch_actions",
		"path_plan",
		"runtime_no_path",
		"handoff_fencing",
	} {
		if value, ok := info[key]; ok {
			out[key] = value
		}
	}
	return out
}

func summarizeHandoffFencingStatus(manifest map[string]any, comparison map[string]any) map[string]any {
	out := map[string]any{
		"attachment_generation":                anyUint64(manifest["attachment_generation"]),
		"writer_fencing_epoch":                 anyUint64(manifest["writer_fencing_epoch"]),
		"handoff_required":                     anyBool(manifest["handoff_required"]),
		"handoff_stage":                        anyString(manifest["handoff_stage"]),
		"handoff_reason":                       anyString(manifest["handoff_reason"]),
		"handoff_requested_at_unix":            anyInt64(manifest["handoff_requested_at_unix"]),
		"handoff_escalation_count":             anyUint64(manifest["handoff_escalation_count"]),
		"handoff_next_escalation_at_unix":      anyInt64(manifest["handoff_next_escalation_at_unix"]),
		"handoff_target_gateway_set":           anyStrings(manifest["handoff_target_gateway_set"]),
		"attachment_generation_state":          anyString(comparison["attachment_generation_state"]),
		"handoff_convergence_state":            anyString(comparison["handoff_convergence_state"]),
		"handoff_backoff_state":                anyString(comparison["handoff_backoff_state"]),
		"last_stale_writer_reject_unix":        int64(0),
		"stale_writer_reject_total":            uint64(0),
		"stale_writer_reject_counters_present": false,
	}
	if nested, ok := manifest["handoff_fencing"].(map[string]any); ok {
		if raw, exists := nested["handoff_acked_at_unix"]; exists {
			out["handoff_acked_at_unix"] = anyInt64(raw)
		}
		if raw, exists := nested["handoff_acked_generation"]; exists {
			out["handoff_acked_generation"] = anyUint64(raw)
		}
		if raw, exists := nested["handoff_completion_eligible_at_unix"]; exists {
			out["handoff_completion_eligible_at_unix"] = anyInt64(raw)
		}
		if raw, exists := nested["last_stale_writer_reject_unix"]; exists {
			out["last_stale_writer_reject_unix"] = anyInt64(raw)
		}
		if raw, exists := nested["stale_writer_reject_total"]; exists {
			out["stale_writer_reject_total"] = anyUint64(raw)
		}
		if raw, exists := nested["stale_writer_reject_counters_present"]; exists {
			out["stale_writer_reject_counters_present"] = anyBool(raw)
		}
	}
	return out
}

func summarizeManifestRuntimeComparison(info map[string]any, status netlinktlv.DeviceStatus) map[string]any {
	out := map[string]any{
		"runtime_volume_id":                        service.CanonicalVolumeID(status.VolumeID),
		"runtime_generation":                       status.Generation,
		"runtime_applied_path_revision":            status.AppliedPathPlanRevision,
		"runtime_applied_path_reported_revision":   uint64(0),
		"runtime_expansion_eligible_at_unix":       int64(0),
		"manifest_attachment_generation":           uint64(0),
		"manifest_path_plan_revision":              uint64(0),
		"manifest_writer_fencing_epoch":            uint64(0),
		"manifest_handoff_requested_at_unix":       int64(0),
		"manifest_handoff_escalation_count":        uint64(0),
		"manifest_handoff_next_escalation_at_unix": int64(0),
		"attachment_generation_state":              "unavailable",
		"handoff_convergence_state":                "not_required",
		"handoff_backoff_state":                    "not_scheduled",
		"path_plan_revision_state":                 "unavailable",
		"reapply_convergence_state":                "unavailable",
		"runtime_expansion_state":                  "not_requested",
		"volume_identity_state":                    "unknown",
	}
	if raw, ok := info["attachment_generation"]; ok {
		value := anyUint64(raw)
		out["manifest_attachment_generation"] = value
		out["attachment_generation_state"] = summarizePathPlanRevisionState(value, status.Generation)
	}
	if raw, ok := info["path_plan_revision"]; ok {
		value := anyUint64(raw)
		out["manifest_path_plan_revision"] = value
		out["path_plan_revision_state"] = summarizePathPlanRevisionState(value, status.AppliedPathPlanRevision)
	}
	if raw, ok := info["runtime_applied_path_plan_revision"]; ok {
		value := anyUint64(raw)
		out["runtime_applied_path_reported_revision"] = value
		if manifestRevision, ok := out["manifest_path_plan_revision"].(uint64); ok && manifestRevision > 0 {
			out["reapply_convergence_state"] = summarizePathPlanRevisionState(manifestRevision, value)
		}
	}
	if raw, ok := info["runtime_path_expansion_eligible_at_unix"]; ok {
		value := anyInt64(raw)
		out["runtime_expansion_eligible_at_unix"] = value
		out["runtime_expansion_state"] = summarizeRuntimeExpansionState(value)
	}
	if raw, ok := info["runtime_path_expansion_backoff_level"]; ok {
		out["runtime_expansion_backoff_level"] = anyUint64(raw)
	}
	if raw, ok := info["writer_fencing_epoch"]; ok {
		out["manifest_writer_fencing_epoch"] = anyUint64(raw)
	}
	if raw, ok := info["handoff_required"]; ok {
		out["manifest_handoff_required"] = anyBool(raw)
	}
	if raw, ok := info["handoff_requested_at_unix"]; ok {
		out["manifest_handoff_requested_at_unix"] = anyInt64(raw)
	}
	if raw, ok := info["handoff_escalation_count"]; ok {
		out["manifest_handoff_escalation_count"] = anyUint64(raw)
	}
	if raw, ok := info["handoff_next_escalation_at_unix"]; ok {
		out["manifest_handoff_next_escalation_at_unix"] = anyInt64(raw)
		out["handoff_backoff_state"] = summarizeHandoffBackoffState(anyInt64(raw))
	}
	if raw, ok := info["handoff_stage"]; ok {
		out["manifest_handoff_stage"] = anyString(raw)
	}
	if raw, ok := info["handoff_reason"]; ok {
		out["manifest_handoff_reason"] = anyString(raw)
	}
	if raw, ok := info["volume_id"]; ok {
		manifestVolumeID := anyString(raw)
		out["manifest_volume_id"] = manifestVolumeID
		if manifestVolumeID == service.CanonicalVolumeID(status.VolumeID) {
			out["volume_identity_state"] = "converged"
		} else {
			out["volume_identity_state"] = "mismatch"
		}
	}
	out["handoff_convergence_state"] = summarizeHandoffConvergenceState(
		anyBool(out["manifest_handoff_required"]),
		anyString(out["manifest_handoff_stage"]),
		anyInt64(out["manifest_handoff_requested_at_unix"]),
		anyString(out["attachment_generation_state"]),
		anyString(out["path_plan_revision_state"]),
		anyString(out["reapply_convergence_state"]),
	)
	return out
}

func summarizeHandoffBackoffState(nextEscalationAtUnix int64) string {
	if nextEscalationAtUnix == 0 {
		return "not_scheduled"
	}
	if nextEscalationAtUnix > time.Now().Unix() {
		return "waiting"
	}
	return "eligible"
}

func summarizeHandoffConvergenceState(handoffRequired bool, handoffStage string, handoffRequestedAtUnix int64, attachmentGenerationState string, pathPlanRevisionState string, reapplyConvergenceState string) string {
	if !handoffRequired {
		return "not_required"
	}
	if handoffStage == "pending_generation_rotation" && handoffRequestedAtUnix > 0 && time.Now().Unix()-handoffRequestedAtUnix >= 15 {
		return "pending_generation_rotation_stalled"
	}
	switch attachmentGenerationState {
	case "unavailable":
		return "unavailable"
	case "stale":
		return "pending_generation_rotation"
	case "ahead":
		return "runtime_generation_ahead"
	}
	switch reapplyConvergenceState {
	case "converged":
		return "ready_to_complete"
	case "stale":
		return "target_attached_pending_path_convergence"
	case "ahead":
		return "runtime_path_revision_ahead"
	}
	switch pathPlanRevisionState {
	case "converged":
		return "pending_runtime_report"
	case "stale":
		return "target_attached_pending_path_convergence"
	case "ahead":
		return "runtime_path_revision_ahead"
	default:
		return "pending_runtime_convergence"
	}
}

func summarizeRuntimeExpansionState(eligibleAtUnix int64) string {
	if eligibleAtUnix == 0 {
		return "not_requested"
	}
	if eligibleAtUnix > time.Now().Unix() {
		return "waiting"
	}
	return "eligible"
}

func summarizePathPlanPriorityCounts(metrics map[string]any) []map[string]any {
	order := []string{"aggressive_handoff", "handoff", "expansion_ready", "refresh", "attention", "normal"}
	out := make([]map[string]any, 0, len(order))
	for _, name := range order {
		out = append(out, map[string]any{
			"priority_class": name,
			"count":          anyInt64(metrics[name]),
		})
	}
	return out
}

func summarizeTopPathPlanPriority(metrics map[string]any) (string, int64) {
	for _, name := range []string{"aggressive_handoff", "handoff", "expansion_ready", "refresh", "attention", "normal"} {
		count := anyInt64(metrics[name])
		if count > 0 {
			return name, count
		}
	}
	return "", 0
}

func clusterPriorityRecommendedActions(currentClass, topClass string) []string {
	switch topClass {
	case "aggressive_handoff":
		return []string{"complete_gateway_handoff_aggressively"}
	case "handoff":
		return []string{"complete_gateway_handoff"}
	case "expansion_ready", "refresh":
		return []string{"refresh_gateway_path_plan"}
	case "attention":
		return []string{"refresh_gateway_path_plan"}
	default:
		return nil
	}
}

func dedupeStrings(values []string) []string {
	if len(values) == 0 {
		return []string{}
	}
	seen := make(map[string]struct{}, len(values))
	out := make([]string, 0, len(values))
	for _, value := range values {
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	return out
}

func recommendedManifestRuntimeActions(comparison map[string]any) []string {
	if comparison == nil {
		return []string{}
	}
	actions := make([]string, 0, 4)
	switch anyString(comparison["volume_identity_state"]) {
	case "mismatch":
		actions = append(actions, "detach_and_reattach")
	}
	switch anyString(comparison["attachment_generation_state"]) {
	case "stale":
		actions = append(actions, "reattach_via_gateway")
	case "ahead":
		actions = append(actions, "reopen_or_wait_for_runtime_catchup")
	}
	switch anyString(comparison["path_plan_revision_state"]) {
	case "stale":
		actions = append(actions, "reapply_latest_path_plan")
	case "ahead":
		actions = append(actions, "refresh_gateway_path_plan")
	}
	if anyString(comparison["runtime_expansion_state"]) == "eligible" {
		actions = append(actions, "refresh_gateway_path_plan")
	}
	if handoffRequired, ok := comparison["manifest_handoff_required"].(bool); ok && handoffRequired {
		actions = append(actions, "complete_gateway_handoff")
		if anyString(comparison["manifest_handoff_reason"]) == "runtime_hold_borderline_current_gateway" {
			actions = append(actions, "complete_gateway_handoff_aggressively")
		}
		if anyString(comparison["handoff_convergence_state"]) == "pending_generation_rotation_stalled" &&
			anyString(comparison["handoff_backoff_state"]) == "eligible" {
			actions = append(actions, "complete_gateway_handoff_aggressively")
		}
	}
	return dedupeStrings(actions)
}

func anyString(v any) string {
	s, _ := v.(string)
	return s
}

func anyBool(v any) bool {
	b, _ := v.(bool)
	return b
}

func anyStrings(v any) []string {
	switch t := v.(type) {
	case []string:
		return append([]string(nil), t...)
	case []any:
		out := make([]string, 0, len(t))
		for _, item := range t {
			if s := anyString(item); s != "" {
				out = append(out, s)
			}
		}
		return out
	default:
		return nil
	}
}

func anyUint32(v any) uint32 {
	switch t := v.(type) {
	case uint32:
		return t
	case uint64:
		return uint32(t)
	case int:
		return uint32(t)
	case float64:
		return uint32(t)
	default:
		return 0
	}
}

func anyUint64(v any) uint64 {
	switch t := v.(type) {
	case uint64:
		return t
	case uint32:
		return uint64(t)
	case int:
		return uint64(t)
	case float64:
		return uint64(t)
	default:
		return 0
	}
}

func anyInt64(v any) int64 {
	switch t := v.(type) {
	case int64:
		return t
	case uint64:
		return int64(t)
	case uint32:
		return int64(t)
	case int:
		return int64(t)
	case float64:
		return int64(t)
	default:
		return 0
	}
}

func reloadControlEndpointURLs(manifest map[string]any, fallback string) []string {
	selectedGatewayIDs := map[string]bool{}
	if endpoints, ok := manifest["dataplane_endpoints"].([]any); ok {
		for _, raw := range endpoints {
			ep, ok := raw.(map[string]any)
			if !ok {
				continue
			}
			if gatewayID := anyString(ep["gateway_id"]); gatewayID != "" {
				selectedGatewayIDs[gatewayID] = true
			}
		}
	}

	out := []string{}
	seen := map[string]bool{}
	if endpoints, ok := manifest["control_endpoints"].([]any); ok {
		for _, raw := range endpoints {
			ep, ok := raw.(map[string]any)
			if !ok {
				continue
			}
			gatewayID := anyString(ep["gateway_id"])
			if len(selectedGatewayIDs) > 0 && gatewayID != "" && !selectedGatewayIDs[gatewayID] {
				continue
			}
			address := anyString(ep["address"])
			port := anyUint32(ep["port"])
			if address == "" || port == 0 {
				continue
			}
			scheme := "http"
			if anyBool(ep["use_tls"]) {
				scheme = "https"
			}
			url := fmt.Sprintf("%s://%s:%d", scheme, address, port)
			if !seen[url] {
				out = append(out, url)
				seen[url] = true
			}
		}
	}
	if len(out) == 0 && fallback != "" {
		out = append(out, strings.TrimRight(fallback, "/"))
	}
	return out
}

func parsePathHealthSpec(spec string) (pathID string, state string, err error) {
	parts := strings.SplitN(strings.TrimSpace(spec), "=", 2)
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return "", "", fmt.Errorf("format must be PATH_ID=healthy|suspect|down")
	}
	state = strings.TrimSpace(parts[1])
	switch state {
	case "healthy", "suspect", "down":
	default:
		return "", "", fmt.Errorf("invalid state %q", state)
	}
	return strings.TrimSpace(parts[0]), state, nil
}

func pathPlanToNetlinkRequest(deviceID uint32, plan map[string]any) (netlinktlv.UpdatePathPlanRequest, error) {
	req := netlinktlv.UpdatePathPlanRequest{DeviceID: deviceID}
	req.PathPlanRevision = anyUint64(plan["path_plan_revision"])

	if suppressed, ok := plan["suppressed"].([]any); ok {
		mask, err := pathMaskFromEntries(suppressed)
		if err != nil {
			return netlinktlv.UpdatePathPlanRequest{}, fmt.Errorf("suppressed: %w", err)
		}
		req.DownMask = mask
	}
	if standby, ok := plan["standby"].([]any); ok {
		mask, err := pathMaskFromEntries(standby)
		if err != nil {
			return netlinktlv.UpdatePathPlanRequest{}, fmt.Errorf("standby: %w", err)
		}
		req.DegradedMask = mask
	}
	return req, nil
}

func adjustRuntimePathPlanRevision(req netlinktlv.UpdatePathPlanRequest, status netlinktlv.DeviceStatus) netlinktlv.UpdatePathPlanRequest {
	applied := status.AppliedPathPlanRevision
	sameMasks := req.DownMask == status.DownMask &&
		req.DegradedMask == status.DegradedMask &&
		req.DrainingMask == status.DrainingMask
	if applied == 0 {
		return req
	}
	if req.PathPlanRevision > applied {
		return req
	}
	req.PathPlanRevision = applied
	if !sameMasks && applied < ^uint64(0) {
		req.PathPlanRevision = applied + 1
	}
	return req
}

func summarizePathPlanRevisionState(requested, applied uint64) string {
	switch {
	case requested == 0 && applied == 0:
		return "unversioned"
	case requested == 0 && applied != 0:
		return "applied-versioned"
	case applied == requested:
		return "converged"
	case applied < requested:
		return "stale"
	default:
		return "ahead"
	}
}

func summarizeRuntimePathPlan(status netlinktlv.DeviceStatus) map[string]any {
	degradedPreferredLanes := 0
	downPreferredLanes := 0
	lanesWithUpFallback := 0
	lanesWithoutFallback := 0
	stableLanes := 0
	degradedWithUpFallbackLanes := 0
	degradedWithoutUpFallbackLanes := 0
	unavailableLanes := 0
	attentionReasons := make([]string, 0, 3)
	for _, lane := range status.Lanes {
		switch lane.Readiness {
		case 1:
			stableLanes++
		case 2:
			degradedWithUpFallbackLanes++
		case 3:
			degradedWithoutUpFallbackLanes++
		case 4:
			unavailableLanes++
		}
		switch pathStateForRuntimeLane(status, lane.PreferredPathID) {
		case "degraded":
			degradedPreferredLanes++
		case "down":
			downPreferredLanes++
		}
		switch pathStateForRuntimeLane(status, lane.FallbackPathID) {
		case "up":
			lanesWithUpFallback++
		case "none":
			lanesWithoutFallback++
		}
	}
	upPaths := countUnsetPathMask(status.PathCount, status.DownMask|status.DegradedMask|status.DrainingMask)
	if degradedWithoutUpFallbackLanes > 0 {
		attentionReasons = append(attentionReasons, "lane_degraded_without_up_fallback")
	}
	if unavailableLanes > 0 {
		attentionReasons = append(attentionReasons, "lane_unavailable")
	}
	if downPreferredLanes > 0 {
		attentionReasons = append(attentionReasons, "lane_down_preferred")
	}
	return map[string]any{
		"applied_revision":             status.AppliedPathPlanRevision,
		"path_count":                   status.PathCount,
		"active_lane_count":            status.ActiveLaneCount,
		"nr_hw_queues":                 status.NrHwQueues,
		"target_nr_hw_queues":          status.TargetNrHwQueues,
		"queue_topology_generation":    status.QueueTopologyGeneration,
		"queue_topology_state":         status.QueueTopologyState,
		"lane_remap_count":             status.LaneRemapCount,
		"last_lane_remapped_lanes":     status.LastLaneRemappedLanes,
		"last_lane_remap_jiffies":      status.LastLaneRemapJiffies,
		"last_lane_remap_reason":       status.LastLaneRemapReason,
		"up_paths":                     upPaths,
		"degraded_paths":               bits.OnesCount64(status.DegradedMask),
		"down_paths":                   bits.OnesCount64(status.DownMask),
		"draining_paths":               bits.OnesCount64(status.DrainingMask),
		"degraded_preferred_lanes":     degradedPreferredLanes,
		"down_preferred_lanes":         downPreferredLanes,
		"lanes_with_up_fallback":       lanesWithUpFallback,
		"lanes_without_fallback":       lanesWithoutFallback,
		"stable_lanes":                 stableLanes,
		"degraded_with_up_fallback":    degradedWithUpFallbackLanes,
		"degraded_without_up_fallback": degradedWithoutUpFallbackLanes,
		"unavailable_lanes":            unavailableLanes,
		"needs_attention":              len(attentionReasons) > 0,
		"attention_reasons":            attentionReasons,
	}
}

func summarizeNoPathStatus(status netlinktlv.DeviceStatus) map[string]any {
	return map[string]any{
		"retry_mode":               noPathRetryModeString(status.NoPathRetryMode),
		"retry_seconds":            status.NoPathRetrySeconds,
		"state":                    noPathStateString(status.NoPathState),
		"since_jiffies":            status.NoPathSinceJiffies,
		"retry_deadline_jiffies":   status.NoPathRetryDeadline,
		"last_wakeup_jiffies":      status.LastNoPathWakeupJiffies,
		"queued_reqs":              status.NoPathQueuedReqs,
		"requeued_reqs":            status.NoPathRequeuedReqs,
		"failed_reqs":              status.NoPathFailedReqs,
		"recovered_reqs":           status.NoPathRecoveredReqs,
		"enter_count":              status.NoPathEnterCount,
		"last_reason":              noPathReasonString(status.LastNoPathReason),
		"last_op":                  status.LastNoPathOp,
		"last_eligible_path_count": status.LastNoPathEligiblePaths,
		"last_tried_mask":          fmt.Sprintf("0x%x", status.LastNoPathTriedMask),
		"last_jiffies":             status.LastNoPathJiffies,
	}
}

func noPathRetryModeString(mode uint32) string {
	switch mode {
	case 1:
		return "queue"
	case 2:
		return "timed"
	default:
		return "fail"
	}
}

func noPathStateString(state uint32) string {
	switch state {
	case 1:
		return "queueing"
	case 2:
		return "recovering"
	case 3:
		return "failing"
	default:
		return "inactive"
	}
}

func noPathReasonString(reason uint32) string {
	switch reason {
	case 1:
		return "detached"
	case 2:
		return "path_plan_empty"
	case 3:
		return "all_paths_down"
	case 4:
		return "all_paths_draining"
	case 5:
		return "no_eligible_path"
	case 6:
		return "exhausted_after_retry"
	default:
		return "none"
	}
}

func recommendedRuntimePathPlanActions(summary map[string]any) []string {
	needsAttention, _ := summary["needs_attention"].(bool)
	if !needsAttention {
		return []string{}
	}
	rawReasons, _ := summary["attention_reasons"].([]string)
	actions := make([]string, 0, len(rawReasons)+1)
	for _, reason := range rawReasons {
		switch reason {
		case "lane_degraded_without_up_fallback":
			actions = append(actions, "refresh_gateway_path_plan", "prefer_fewer_active_paths")
		case "lane_unavailable":
			actions = append(actions, "refresh_gateway_path_plan", "reopen_or_reapply_path_plan")
		case "lane_down_preferred":
			actions = append(actions, "reapply_latest_path_plan")
		}
	}
	return dedupeStrings(actions)
}

func runtimeFeedbackPayload(summary map[string]any, noPath map[string]any, actions []string, sourceHost string) map[string]any {
	reasons := []string{}
	if rawReasons, ok := summary["attention_reasons"].([]string); ok {
		reasons = append(reasons, rawReasons...)
	}
	needsAttention, _ := summary["needs_attention"].(bool)
	return map[string]any{
		"needs_attention":            needsAttention,
		"attention_reasons":          reasons,
		"recommended_actions":        dedupeStrings(actions),
		"applied_path_plan_revision": anyUint64(summary["applied_revision"]),
		"source_host":                strings.TrimSpace(sourceHost),
		"no_path": map[string]any{
			"state":          anyString(noPath["state"]),
			"retry_mode":     anyString(noPath["retry_mode"]),
			"retry_seconds":  uint32(anyUint64(noPath["retry_seconds"])),
			"queued_reqs":    anyUint64(noPath["queued_reqs"]),
			"requeued_reqs":  anyUint64(noPath["requeued_reqs"]),
			"failed_reqs":    anyUint64(noPath["failed_reqs"]),
			"recovered_reqs": anyUint64(noPath["recovered_reqs"]),
			"enter_count":    anyUint64(noPath["enter_count"]),
			"last_reason":    anyString(noPath["last_reason"]),
		},
	}
}

func pathStateForRuntimeLane(status netlinktlv.DeviceStatus, pathID uint32) string {
	if pathID == ^uint32(0) {
		return "none"
	}
	for _, path := range status.Paths {
		if path.PathID != pathID {
			continue
		}
		switch {
		case status.DrainingMask&(1<<path.PathID) != 0:
			return "draining"
		case status.DownMask&(1<<path.PathID) != 0:
			return "down"
		case status.DegradedMask&(1<<path.PathID) != 0:
			return "degraded"
		default:
			return "up"
		}
	}
	switch {
	case status.DrainingMask&(1<<pathID) != 0:
		return "draining"
	case status.DownMask&(1<<pathID) != 0:
		return "down"
	case status.DegradedMask&(1<<pathID) != 0:
		return "degraded"
	default:
		return "up"
	}
}

func countUnsetPathMask(pathCount uint32, mask uint64) int {
	total := int(pathCount)
	if total <= 0 {
		return 0
	}
	setBits := 0
	for i := 0; i < total; i++ {
		if mask&(1<<i) != 0 {
			setBits++
		}
	}
	if setBits > total {
		return 0
	}
	return total - setBits
}

func encodeLaneStatuses(lanes []netlinktlv.LaneStatus) []map[string]any {
	out := make([]map[string]any, 0, len(lanes))
	for _, lane := range lanes {
		entry := map[string]any{
			"lane_id": lane.LaneID,
		}
		if lane.PreferredPathID == ^uint32(0) {
			entry["preferred_path_id"] = nil
		} else {
			entry["preferred_path_id"] = lane.PreferredPathID
		}
		if lane.FallbackPathID == ^uint32(0) {
			entry["fallback_path_id"] = nil
		} else {
			entry["fallback_path_id"] = lane.FallbackPathID
		}
		entry["readiness"] = laneReadinessString(lane.Readiness)
		entry["dispatch_reqs"] = lane.DispatchReqs
		out = append(out, entry)
	}
	return out
}

func laneReadinessString(readiness uint32) string {
	switch readiness {
	case 1:
		return "stable"
	case 2:
		return "degraded_with_up_fallback"
	case 3:
		return "degraded_without_up_fallback"
	case 4:
		return "unavailable"
	default:
		return "unknown"
	}
}

func pathMaskFromEntries(entries []any) (uint64, error) {
	var mask uint64
	for _, entry := range entries {
		item, ok := entry.(map[string]any)
		if !ok {
			return 0, fmt.Errorf("invalid path entry")
		}
		pathID := anyUint32(item["path_id"])
		if pathID >= 64 {
			return 0, fmt.Errorf("path_id %d exceeds 63", pathID)
		}
		mask |= uint64(1) << pathID
	}
	return mask, nil
}
