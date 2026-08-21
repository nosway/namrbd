package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"strings"
	"testing"

	"github.com/nosway/namrbd/control/netlinktlv"
	"github.com/nosway/namrbd/gateway/metadata"
	"github.com/nosway/namrbd/gateway/service"
)

func TestParseServerSpec(t *testing.T) {
	s, err := parseServerSpec("1,10.0.0.10,9701,true,/api/v1,token")
	if err != nil {
		t.Fatalf("parseServerSpec failed: %v", err)
	}
	if s.ID != 1 || s.Address != "10.0.0.10" || s.Port != 9701 || !s.UseTLS || s.APIPrefix != "/api/v1" || s.BearerToken != "token" {
		t.Fatalf("unexpected parse result: %+v", s)
	}
}

func TestParseServerSpec_BadFormat(t *testing.T) {
	if _, err := parseServerSpec("1,2,3"); err == nil {
		t.Fatalf("expected parse error")
	}
}

func TestHasUintFlag(t *testing.T) {
	if !hasUintFlag([]string{"--device", "3"}, "device") {
		t.Fatalf("expected --device to be detected")
	}
	if !hasUintFlag([]string{"--device=3"}, "device") {
		t.Fatalf("expected --device=3 to be detected")
	}
	if hasUintFlag([]string{"--volume=3"}, "device") {
		t.Fatalf("unexpected detection")
	}
}

func TestNormalizeRootArgsSupportsJSONAndCommandHelp(t *testing.T) {
	got, jsonOutput := normalizeRootArgs([]string{"namrbdctl", "attach", "--json", "help"})
	if !jsonOutput || strings.Join(got, " ") != "namrbdctl attach --help" {
		t.Fatalf("args=%v json=%t", got, jsonOutput)
	}
}

func TestDiscoverGatewayControlEndpointUsesBoundedHealthyFleetPage(t *testing.T) {
	orig := openEtcdMetadataRepositoryFunc
	openEtcdMetadataRepositoryFunc = func(*etcdMetadataCLIConfig) (namrbdctlDirectEtcdMetadataRepository, func(), error) {
		return namrbdctlFakeMetadataRepository{gateways: []service.GatewayRecord{
			{GatewayID: "gw-b", Product: service.GatewayProductNAMRBD, Role: service.GatewayRoleBlock, ConnectionState: service.GatewayStateDown, ControlEndpoints: []service.EndpointSpec{{Address: "10.0.0.2", Port: 9701}}},
			{GatewayID: "gw-a", Product: service.GatewayProductNAMRBD, Role: service.GatewayRoleBlock, ConnectionState: service.GatewayStateUp, Readiness: service.GatewayReadinessReady, DrainState: service.GatewayDrainActive, ControlEndpoints: []service.EndpointSpec{{Address: "10.0.0.1", Port: 9701, UseTLS: true}}},
		}}, func() {}, nil
	}
	defer func() { openEtcdMetadataRepositoryFunc = orig }()

	endpoint, err := discoverGatewayControlEndpoint(context.Background(), &etcdMetadataCLIConfig{}, 128)
	if err != nil {
		t.Fatalf("discoverGatewayControlEndpoint: %v", err)
	}
	if endpoint != "https://10.0.0.1:9701" {
		t.Fatalf("endpoint=%q", endpoint)
	}
}

func TestUsageSeparatesDirectEtcdReadAndMutationCommands(t *testing.T) {
	output := captureStderr(t, usage)
	if strings.Contains(output, "metadata-read commands:") {
		t.Fatalf("usage still uses ambiguous metadata-read heading:\n%s", output)
	}
	readHeading := strings.Index(output, "direct-etcd metadata read commands:")
	mutationHeading := strings.Index(output, "direct-etcd metadata mutation commands:")
	gatewayList := strings.Index(output, "namrbdctl gateway-list")
	gatewayPut := strings.Index(output, "namrbdctl gateway-put")
	volumeDelete := strings.Index(output, "namrbdctl volume-delete")
	if readHeading < 0 || mutationHeading < 0 {
		t.Fatalf("usage missing direct-etcd read/mutation headings:\n%s", output)
	}
	if !(readHeading < gatewayList && gatewayList < mutationHeading) {
		t.Fatalf("gateway-list should be under direct-etcd read commands:\n%s", output)
	}
	if !(mutationHeading < gatewayPut && mutationHeading < volumeDelete) {
		t.Fatalf("mutation commands should be under direct-etcd mutation commands:\n%s", output)
	}
}

func TestParseVolumeID(t *testing.T) {
	if got, err := parseVolumeID("00a1b2c3"); err != nil || got != 0x00a1b2c3 {
		t.Fatalf("expected canonical hex parse, got=%d err=%v", got, err)
	}
	if _, err := parseVolumeID("0x12"); err == nil {
		t.Fatalf("expected 0x form to be rejected")
	}
	if _, err := parseVolumeID("123"); err == nil {
		t.Fatalf("expected short id to be rejected")
	}
}

func TestRunGatewayListUsesMetadataRepository(t *testing.T) {
	orig := openEtcdMetadataRepositoryFunc
	openEtcdMetadataRepositoryFunc = func(cfg *etcdMetadataCLIConfig) (namrbdctlDirectEtcdMetadataRepository, func(), error) {
		return namrbdctlFakeMetadataRepository{
			gateways: []service.GatewayRecord{
				{
					GatewayID:        "gw-a",
					ConnectionState:  service.GatewayStateUp,
					ClusterID:        "namrbd:etcd:/namrbd",
					MetadataRoot:     "/namrbd",
					LastSeenUnix:     1700000000,
					ControlEndpoints: []service.EndpointSpec{{Address: "10.0.0.1", Port: 9701}},
				},
			},
		}, func() {}, nil
	}
	defer func() { openEtcdMetadataRepositoryFunc = orig }()

	output := captureStdout(t, func() {
		runGatewayList([]string{"--etcd-endpoints", "127.0.0.1:2379"})
	})
	if !strings.Contains(output, "gateway_id=gw-a") || !strings.Contains(output, "control_endpoints=1") {
		t.Fatalf("unexpected gateway-list output: %s", output)
	}
	if strings.Contains(output, "sbs_cluster_root") {
		t.Fatalf("gateway-list should not print SBS internal metadata fields: %s", output)
	}
}

func TestRunGatewayGetUsesReducedPrimaryTextOutput(t *testing.T) {
	orig := openEtcdMetadataRepositoryFunc
	openEtcdMetadataRepositoryFunc = func(cfg *etcdMetadataCLIConfig) (namrbdctlDirectEtcdMetadataRepository, func(), error) {
		return namrbdctlFakeMetadataRepository{
			gateway: service.GatewayRecord{
				GatewayID:                 "gw-a",
				ConnectionState:           service.GatewayStateUp,
				ClusterID:                 "namrbd:etcd:/namrbd",
				MetadataBackend:           "etcd",
				MetadataRoot:              "/namrbd",
				SBSClusterID:              "sbs-cluster-a",
				SBSClusterMetadataBackend: "tikv",
				SBSClusterMetadataRoot:    "/sbs-a",
				FailureDomain:             "zone-a",
				LastSeenUnix:              1700000000,
				LeaseID:                   "lease-1",
				ControlEndpoints:          []service.EndpointSpec{{Address: "10.0.0.1", Port: 9701}},
				DataplaneEndpoints:        []service.EndpointSpec{{Address: "10.0.0.1", Port: 9700}},
			},
		}, func() {}, nil
	}
	defer func() { openEtcdMetadataRepositoryFunc = orig }()

	output := captureStdout(t, func() {
		runGatewayGet([]string{"--gateway", "gw-a"})
	})
	if !strings.Contains(output, "gateway_id=gw-a") || !strings.Contains(output, "metadata_root=/namrbd") {
		t.Fatalf("unexpected gateway-get output: %s", output)
	}
	if strings.Contains(output, "sbs_cluster_id=") || strings.Contains(output, "sbs_cluster_metadata_root=") {
		t.Fatalf("gateway-get primary text output should omit SBS internal metadata fields: %s", output)
	}
}

func TestRunGatewayPutUsesMetadataRepository(t *testing.T) {
	orig := openEtcdMetadataRepositoryFunc
	openEtcdMetadataRepositoryFunc = func(cfg *etcdMetadataCLIConfig) (namrbdctlDirectEtcdMetadataRepository, func(), error) {
		return namrbdctlFakeMetadataRepository{}, func() {}, nil
	}
	defer func() { openEtcdMetadataRepositoryFunc = orig }()

	file := writeTempFile(t, `{
  "gateway_id": "gw-a",
  "connection_state": "up",
  "cluster_id": "namrbd:etcd:/namrbd",
  "metadata_backend": "etcd",
  "metadata_root": "/namrbd"
}`)

	output := captureStdout(t, func() {
		runGatewayPut([]string{"--from-file", file})
	})
	if !strings.Contains(output, "ok gateway_id=gw-a state=up") {
		t.Fatalf("unexpected gateway-put output: %s", output)
	}
}

func TestRunAttachmentGetUsesMetadataRepository(t *testing.T) {
	orig := openEtcdMetadataRepositoryFunc
	openEtcdMetadataRepositoryFunc = func(cfg *etcdMetadataCLIConfig) (namrbdctlDirectEtcdMetadataRepository, func(), error) {
		return namrbdctlFakeMetadataRepository{
			attachment: service.AttachmentRecord{
				Generation:     7,
				AttachmentID:   "att-00000065-0007",
				HostID:         "host-a",
				DeviceID:       3,
				OwnerGatewayID: "gw-a",
				LeaseID:        "lease-1",
				AttachedAtUnix: 1700000000,
			},
		}, func() {}, nil
	}
	defer func() { openEtcdMetadataRepositoryFunc = orig }()

	output := captureStdout(t, func() {
		runAttachmentGet([]string{"--volume", "00000065"})
	})
	if !strings.Contains(output, "attachment_id=att-00000065-0007") || !strings.Contains(output, "owner_gateway_id=gw-a") {
		t.Fatalf("unexpected attachment-get output: %s", output)
	}
}

func TestRunVolumeListUsesMetadataRepository(t *testing.T) {
	orig := openEtcdMetadataRepositoryFunc
	openEtcdMetadataRepositoryFunc = func(cfg *etcdMetadataCLIConfig) (namrbdctlDirectEtcdMetadataRepository, func(), error) {
		return namrbdctlFakeMetadataRepository{
			volumes: []service.VolumeSpec{
				{
					ID:              service.HexVolumeID(101),
					Name:            "devA",
					Prefix:          "devA",
					SizeBytes:       1 << 20,
					BlockSize:       service.DefaultBlockSize,
					ChunkSizeBytes:  service.DefaultAllocationChunkSize,
					ExtentPageBytes: service.DefaultAllocationPageSize,
					AccessMode:      service.VolumeAccessModeExclusive,
					State:           service.VolumeStateAvailable,
				},
			},
		}, func() {}, nil
	}
	defer func() { openEtcdMetadataRepositoryFunc = orig }()

	output := captureStdout(t, func() {
		runVolumeList(nil)
	})
	if !strings.Contains(output, "volume_id=00000065") || !strings.Contains(output, "name=devA") {
		t.Fatalf("unexpected volume-list output: %s", output)
	}
}

func TestRunVolumeCreateUsesMetadataRepository(t *testing.T) {
	orig := openEtcdMetadataRepositoryFunc
	openEtcdMetadataRepositoryFunc = func(cfg *etcdMetadataCLIConfig) (namrbdctlDirectEtcdMetadataRepository, func(), error) {
		return namrbdctlFakeMetadataRepository{}, func() {}, nil
	}
	defer func() { openEtcdMetadataRepositoryFunc = orig }()

	output := captureStdout(t, func() {
		runVolumeCreate([]string{"--name", "devA", "--size", "1M", "--allocation-chunk-size", "128K", "--allocation-page-size", "8M"})
	})
	if !strings.Contains(output, "ok volume_id=00000065 name=devA prefix=devA") ||
		!strings.Contains(output, "allocation_chunk_size_bytes=131072") ||
		!strings.Contains(output, "allocation_page_bytes=8388608") {
		t.Fatalf("unexpected volume-create output: %s", output)
	}
}

func TestRunVolumeGetUsesMetadataRepository(t *testing.T) {
	orig := openEtcdMetadataRepositoryFunc
	openEtcdMetadataRepositoryFunc = func(cfg *etcdMetadataCLIConfig) (namrbdctlDirectEtcdMetadataRepository, func(), error) {
		return namrbdctlFakeMetadataRepository{
			volume: service.VolumeSpec{
				ID:              service.HexVolumeID(101),
				Name:            "devA",
				Prefix:          "devA",
				SizeBytes:       1 << 20,
				BlockSize:       service.DefaultBlockSize,
				ChunkSizeBytes:  service.DefaultAllocationChunkSize,
				ExtentPageBytes: service.DefaultAllocationPageSize,
				AccessMode:      service.VolumeAccessModeExclusive,
				State:           service.VolumeStateAvailable,
			},
		}, func() {}, nil
	}
	defer func() { openEtcdMetadataRepositoryFunc = orig }()

	output := captureStdout(t, func() {
		runVolumeGet([]string{"--volume", "00000065"})
	})
	if !strings.Contains(output, "volume_id=00000065") || !strings.Contains(output, "name=devA") || !strings.Contains(output, "allocation_page_bytes=") {
		t.Fatalf("unexpected volume-get output: %s", output)
	}
}

func TestRunVolumeUpdateUsesMetadataRepository(t *testing.T) {
	orig := openEtcdMetadataRepositoryFunc
	openEtcdMetadataRepositoryFunc = func(cfg *etcdMetadataCLIConfig) (namrbdctlDirectEtcdMetadataRepository, func(), error) {
		return namrbdctlFakeMetadataRepository{
			volume: service.VolumeSpec{
				ID:              service.HexVolumeID(101),
				Name:            "devA",
				Prefix:          "devA",
				SizeBytes:       1 << 20,
				BlockSize:       service.DefaultBlockSize,
				ChunkSizeBytes:  service.DefaultAllocationChunkSize,
				ExtentPageBytes: service.DefaultAllocationPageSize,
				AccessMode:      service.VolumeAccessModeExclusive,
				State:           service.VolumeStateAvailable,
			},
		}, func() {}, nil
	}
	defer func() { openEtcdMetadataRepositoryFunc = orig }()

	output := captureStdout(t, func() {
		runVolumeUpdate([]string{"--volume", "00000065", "--name", "devB", "--size", "2M", "--state", "disabled"})
	})
	if !strings.Contains(output, "ok volume_id=00000065 name=devB") || !strings.Contains(output, "state=disabled") {
		t.Fatalf("unexpected volume-update output: %s", output)
	}
}

func TestRunVolumeSetStateUsesMetadataRepository(t *testing.T) {
	orig := openEtcdMetadataRepositoryFunc
	openEtcdMetadataRepositoryFunc = func(cfg *etcdMetadataCLIConfig) (namrbdctlDirectEtcdMetadataRepository, func(), error) {
		return namrbdctlFakeMetadataRepository{
			volume: service.VolumeSpec{
				ID:              service.HexVolumeID(101),
				Name:            "devA",
				Prefix:          "devA",
				SizeBytes:       1 << 20,
				BlockSize:       service.DefaultBlockSize,
				ChunkSizeBytes:  service.DefaultAllocationChunkSize,
				ExtentPageBytes: service.DefaultAllocationPageSize,
				AccessMode:      service.VolumeAccessModeExclusive,
				State:           service.VolumeStateAvailable,
			},
		}, func() {}, nil
	}
	defer func() { openEtcdMetadataRepositoryFunc = orig }()

	output := captureStdout(t, func() {
		runVolumeSetState([]string{"--volume", "00000065", "--state", "disabled"})
	})
	if !strings.Contains(output, "ok volume_id=00000065 state=disabled") {
		t.Fatalf("unexpected volume-set-state output: %s", output)
	}
}

func TestRunVolumeDeleteUsesMetadataRepository(t *testing.T) {
	orig := openEtcdMetadataRepositoryFunc
	openEtcdMetadataRepositoryFunc = func(cfg *etcdMetadataCLIConfig) (namrbdctlDirectEtcdMetadataRepository, func(), error) {
		return namrbdctlFakeMetadataRepository{
			volume: service.VolumeSpec{
				ID: service.HexVolumeID(101),
			},
		}, func() {}, nil
	}
	defer func() { openEtcdMetadataRepositoryFunc = orig }()

	output := captureStdout(t, func() {
		runVolumeDelete([]string{"--volume", "00000065"})
	})
	if !strings.Contains(output, "ok volume_id=00000065 deleted=true") {
		t.Fatalf("unexpected volume-delete output: %s", output)
	}
}

func TestRunVolumeStatusUsesMetadataRepository(t *testing.T) {
	orig := openEtcdMetadataRepositoryFunc
	openEtcdMetadataRepositoryFunc = func(cfg *etcdMetadataCLIConfig) (namrbdctlDirectEtcdMetadataRepository, func(), error) {
		return namrbdctlFakeMetadataRepository{
			status: service.VolumeStatusRecord{
				VolumeID:                 service.HexVolumeID(101),
				InUse:                    true,
				CurrentAttachmentID:      "att-00000065-0007",
				CurrentHostID:            "host-a",
				CurrentGatewayID:         "gw-a",
				GatewayConnectionState:   "up",
				DesiredActiveGatewaySet:  []string{"gw-a", "gw-b"},
				ObservedActiveGatewaySet: []string{"gw-a"},
				PathPlanRevision:         17,
				AttachmentGeneration:     7,
				WriterFencingEpoch:       3,
			},
		}, func() {}, nil
	}
	defer func() { openEtcdMetadataRepositoryFunc = orig }()

	output := captureStdout(t, func() {
		runVolumeStatus([]string{"--volume", "00000065"})
	})
	if !strings.Contains(output, "volume_id=00000065") || !strings.Contains(output, "desired_active_gateway_set=gw-a,gw-b") {
		t.Fatalf("unexpected volume-status output: %s", output)
	}
	if !strings.Contains(output, "current_gateway_id_note=compatibility field; use desired_active_gateway_set/observed_active_gateway_set and path_plan_revision for active-active path-plan state") {
		t.Fatalf("expected compatibility note in volume-status output: %s", output)
	}
}

func TestRunValidateVolumeUsesMetadataRepository(t *testing.T) {
	orig := openEtcdMetadataRepositoryFunc
	openEtcdMetadataRepositoryFunc = func(cfg *etcdMetadataCLIConfig) (namrbdctlDirectEtcdMetadataRepository, func(), error) {
		return namrbdctlFakeMetadataRepository{
			volume: service.VolumeSpec{
				ID:        service.HexVolumeID(101),
				Name:      "devA",
				Prefix:    "devA",
				SizeBytes: 1 << 20,
				BlockSize: service.DefaultBlockSize,
				State:     service.VolumeStateInUse,
			},
			status: service.VolumeStatusRecord{
				VolumeID:            service.HexVolumeID(101),
				InUse:               true,
				CurrentAttachmentID: "att-00000065-0007",
				CurrentHostID:       "host-a",
				CurrentGatewayID:    "gw-a",
			},
			attachment: service.AttachmentRecord{
				Generation:     7,
				AttachmentID:   "att-00000065-0007",
				HostID:         "host-a",
				OwnerGatewayID: "gw-a",
			},
			generation: 7,
		}, func() {}, nil
	}
	defer func() { openEtcdMetadataRepositoryFunc = orig }()

	output := captureStdout(t, func() {
		runValidateVolume([]string{"--volume", "00000065"})
	})
	if !strings.Contains(output, "volume_id=00000065 ok=true generation=7 issue_count=0") {
		t.Fatalf("unexpected validate-volume output: %s", output)
	}
}

func TestRunValidateAllUsesMetadataRepository(t *testing.T) {
	orig := openEtcdMetadataRepositoryFunc
	openEtcdMetadataRepositoryFunc = func(cfg *etcdMetadataCLIConfig) (namrbdctlDirectEtcdMetadataRepository, func(), error) {
		return namrbdctlFakeMetadataRepository{
			volumes: []service.VolumeSpec{
				{
					ID:        service.HexVolumeID(101),
					Name:      "devA",
					Prefix:    "devA",
					SizeBytes: 1 << 20,
					BlockSize: service.DefaultBlockSize,
					State:     service.VolumeStateAvailable,
				},
			},
			volume: service.VolumeSpec{
				ID:        service.HexVolumeID(101),
				Name:      "devA",
				Prefix:    "devA",
				SizeBytes: 1 << 20,
				BlockSize: service.DefaultBlockSize,
				State:     service.VolumeStateAvailable,
			},
			status:     service.VolumeStatusRecord{VolumeID: service.HexVolumeID(101)},
			generation: 1,
			gateways: []service.GatewayRecord{
				{GatewayID: "gw-a", ClusterID: "namrbd:etcd:/namrbd", MetadataBackend: "etcd", MetadataRoot: "/namrbd"},
			},
		}, func() {}, nil
	}
	defer func() { openEtcdMetadataRepositoryFunc = orig }()

	output := captureStdout(t, func() {
		runValidateAll(nil)
	})
	if !strings.Contains(output, "ok=true volume_count=1 gateway_count=1 issue_count=0") {
		t.Fatalf("unexpected validate-all output: %s", output)
	}
}

func TestGatewayClientInfoReadWrite(t *testing.T) {
	var writeCalled bool
	var attachCalled bool
	var detachCalled bool
	c := newGatewayClient("http://gateway.test")
	c.httpClient = &http.Client{
		Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			rec := newResponseRecorder()
			switch req.URL.Path {
			case "/api/v1/volumes/00000065/info":
				_ = json.NewEncoder(rec).Encode(map[string]any{
					"volume_id":  "00000065",
					"size_bytes": 8192,
					"block_size": 4096,
				})
			case "/api/v1/discovery/gateways":
				_ = json.NewEncoder(rec).Encode(map[string]any{
					"gateways": []map[string]any{
						{"gateway_id": "gw-a"},
						{"gateway_id": "gw-b"},
					},
				})
			case "/api/v1/discovery/volumes/00000065":
				_ = json.NewEncoder(rec).Encode(map[string]any{
					"gateways": []map[string]any{
						{
							"gateway_id": "gw-a",
							"control_endpoints": []map[string]any{
								{"address": "10.0.0.1", "port": 9701, "use_tls": true, "server_name": "gw-a", "auth_mode": "bearer"},
							},
						},
						{
							"gateway_id": "gw-b",
							"control_endpoints": []map[string]any{
								{"address": "10.0.0.2", "port": 9801, "use_tls": true, "server_name": "gw-b", "auth_mode": "bearer"},
							},
						},
					},
					"dataplane_paths": []map[string]any{
						{"path_id": 0, "address": "10.0.0.1", "port": 9700, "use_tls": true, "server_name": "gw-a", "auth_mode": "bearer", "discovery_priority": 100},
						{"path_id": 1, "address": "10.0.0.2", "port": 9800, "use_tls": true, "server_name": "gw-b", "auth_mode": "bearer", "discovery_priority": 100},
					},
				})
			case "/api/v1/debug/discovery/volumes/00000065/path-plan":
				_ = json.NewEncoder(rec).Encode(map[string]any{
					"allowed_path_ids": []uint32{0},
					"active": []map[string]any{
						{"path_id": 0},
					},
					"standby": []map[string]any{},
					"suppressed": []map[string]any{
						{"path_id": 1},
					},
				})
			case "/api/v1/debug/discovery/volumes/00000065/runtime-feedback":
				_ = json.NewDecoder(req.Body).Decode(&map[string]any{})
				_ = json.NewEncoder(rec).Encode(map[string]any{
					"volume_id":                      "00000065",
					"runtime_path_needs_attention":   true,
					"controller_needs_attention":     true,
					"controller_recommended_actions": []string{"refresh_gateway_path_plan", "prefer_fewer_active_paths"},
				})
			case "/api/v1/debug/sbs-cluster/metrics":
				_ = json.NewEncoder(rec).Encode(map[string]any{
					"volumes": map[string]any{"total": 1},
					"path_plan": map[string]any{
						"total":              3,
						"aggressive_handoff": 1,
						"handoff":            0,
						"expansion_ready":    1,
						"refresh":            1,
						"attention":          0,
						"normal":             0,
					},
				})
			case "/api/v1/volumes/00000065/read":
				_ = json.NewEncoder(rec).Encode(map[string]any{
					"data_base64": base64.StdEncoding.EncodeToString([]byte("abcd")),
				})
			case "/api/v1/volumes/00000065/write":
				writeCalled = true
				rec.WriteHeader(http.StatusOK)
			case "/api/v1/volumes/00000065/attach":
				attachCalled = true
				_ = json.NewEncoder(rec).Encode(map[string]any{
					"volume_id":          "00000065",
					"attachment_id":      "att-00000065-0001",
					"attached_host_id":   "host-a",
					"attached_device_id": 3,
				})
			case "/api/v1/volumes/00000065/detach":
				detachCalled = true
				rec.WriteHeader(http.StatusOK)
			default:
				rec.WriteHeader(http.StatusNotFound)
			}
			return rec.Result(req), nil
		}),
	}

	info, err := c.info(101)
	if err != nil {
		t.Fatalf("info failed: %v", err)
	}
	if info["volume_id"].(string) != "00000065" {
		t.Fatalf("unexpected info: %+v", info)
	}
	discoveryGateways, err := c.discoveryGateways()
	if err != nil {
		t.Fatalf("discoveryGateways failed: %v", err)
	}
	if len(discoveryGateways["gateways"].([]any)) != 2 {
		t.Fatalf("unexpected discovery gateways: %+v", discoveryGateways)
	}
	discoveryVolume, err := c.discoveryVolume(101)
	if err != nil {
		t.Fatalf("discoveryVolume failed: %v", err)
	}
	if len(discoveryVolume["dataplane_paths"].([]any)) != 2 {
		t.Fatalf("unexpected discovery volume: %+v", discoveryVolume)
	}
	pathPlan, err := c.discoveryPathPlan(101, 1, map[string]string{"1": "down"})
	if err != nil {
		t.Fatalf("discoveryPathPlan failed: %v", err)
	}
	if len(pathPlan["allowed_path_ids"].([]any)) != 1 {
		t.Fatalf("unexpected path plan: %+v", pathPlan)
	}
	readData, err := c.read(101, 0, 4)
	if err != nil {
		t.Fatalf("read failed: %v", err)
	}
	if string(readData) != "abcd" {
		t.Fatalf("unexpected read data: %q", readData)
	}
	if err := c.write(101, 0, []byte("zzzz")); err != nil {
		t.Fatalf("write failed: %v", err)
	}
	manifest, err := c.attach(101, "host-a", 3)
	if err != nil {
		t.Fatalf("attach failed: %v", err)
	}
	if !strings.Contains(manifest, `"attachment_id":"att-00000065-0001"`) {
		t.Fatalf("unexpected manifest: %s", manifest)
	}
	if err := c.detach(101, "host-a", "att-00000065-0001"); err != nil {
		t.Fatalf("detach failed: %v", err)
	}
	if !writeCalled {
		t.Fatalf("write endpoint was not called")
	}
	if !attachCalled || !detachCalled {
		t.Fatalf("attach/detach endpoints were not both called")
	}
}

func TestRunClusterMetricsIncludesPrioritySummary(t *testing.T) {
	origNewGatewayClientFunc := newGatewayClientFunc
	newGatewayClientFunc = func(baseURL string, opts ...gatewayClientOptions) *gatewayClient {
		var resolved gatewayClientOptions
		if len(opts) > 0 {
			resolved = opts[0]
		}
		httpClient := &http.Client{
			Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
				rec := newResponseRecorder()
				switch {
				case req.Method == http.MethodGet && req.URL.Path == "/api/v1/debug/sbs-cluster/metrics":
					rec.Header().Set("Content-Type", "application/json")
					_, _ = rec.Write([]byte(`{
						"volumes":{"total":1},
						"path_plan":{
							"total":3,
							"aggressive_handoff":1,
							"handoff":0,
							"expansion_ready":1,
							"refresh":1,
							"attention":0,
							"normal":0
						}
					}`))
					return rec.Result(req), nil
				default:
					t.Fatalf("unexpected request: %s %s", req.Method, req.URL.Path)
					return nil, nil
				}
			}),
		}
		c := newGatewayClient(baseURL, resolved)
		c.httpClient = httpClient
		return c
	}
	defer func() {
		newGatewayClientFunc = origNewGatewayClientFunc
	}()

	output := captureStdout(t, func() {
		runClusterMetrics([]string{"--gateway", "http://gateway.test"})
	})
	var out map[string]any
	if err := json.Unmarshal([]byte(output), &out); err != nil {
		t.Fatalf("unmarshal cluster metrics output: %v", err)
	}
	if out["volumes"].(map[string]any)["total"].(float64) != 1 {
		t.Fatalf("unexpected volumes metrics: %+v", out)
	}
	ordered := out["path_plan_priority_counts"].([]any)
	if len(ordered) != 6 {
		t.Fatalf("unexpected priority counts: %+v", ordered)
	}
	first := ordered[0].(map[string]any)
	if first["priority_class"].(string) != "aggressive_handoff" || first["count"].(float64) != 1 {
		t.Fatalf("unexpected first priority count: %+v", first)
	}
	third := ordered[2].(map[string]any)
	if third["priority_class"].(string) != "expansion_ready" || third["count"].(float64) != 1 {
		t.Fatalf("unexpected expansion_ready priority count: %+v", third)
	}
	if out["top_priority_class"].(string) != "aggressive_handoff" || out["top_priority_count"].(float64) != 1 {
		t.Fatalf("unexpected top priority summary: %+v", out)
	}
}

func TestMergeDiscoveryIntoManifest(t *testing.T) {
	manifest := `{"volume_id":"00000065","control_endpoints":[{"address":"127.0.0.1","port":9701}],"dataplane_endpoints":[{"path_id":0,"address":"127.0.0.1","port":9700,"priority":100}]}`
	discovery := map[string]any{
		"gateways": []any{
			map[string]any{
				"gateway_id": "gw-a",
				"control_endpoints": []any{
					map[string]any{"address": "10.0.0.1", "port": float64(9701), "use_tls": true, "server_name": "gw-a", "auth_mode": "bearer"},
				},
			},
			map[string]any{
				"gateway_id": "gw-b",
				"control_endpoints": []any{
					map[string]any{"address": "10.0.0.2", "port": float64(9801), "use_tls": true, "server_name": "gw-b", "auth_mode": "bearer"},
				},
			},
		},
		"dataplane_paths": []any{
			map[string]any{"path_id": float64(0), "gateway_id": "gw-a", "address": "10.0.0.1", "port": float64(9700), "use_tls": true, "server_name": "gw-a", "auth_mode": "bearer", "discovery_priority": float64(100)},
			map[string]any{"path_id": float64(1), "gateway_id": "gw-b", "address": "10.0.0.2", "port": float64(9800), "use_tls": true, "server_name": "gw-b", "auth_mode": "bearer", "discovery_priority": float64(90)},
		},
	}

	merged, err := mergeDiscoveryIntoManifest(manifest, discovery, discoveryMergeOptions{})
	if err != nil {
		t.Fatalf("mergeDiscoveryIntoManifest failed: %v", err)
	}
	var out map[string]any
	if err := json.Unmarshal([]byte(merged), &out); err != nil {
		t.Fatalf("unmarshal merged manifest: %v", err)
	}
	if len(out["control_endpoints"].([]any)) != 2 {
		t.Fatalf("unexpected control_endpoints: %+v", out["control_endpoints"])
	}
	dataplaneEndpoints := out["dataplane_endpoints"].([]any)
	if len(dataplaneEndpoints) != 2 {
		t.Fatalf("unexpected dataplane_endpoints: %+v", dataplaneEndpoints)
	}
	first := dataplaneEndpoints[0].(map[string]any)
	second := dataplaneEndpoints[1].(map[string]any)
	if first["path_id"].(float64) != 0 || second["path_id"].(float64) != 1 {
		t.Fatalf("unexpected path ids: first=%+v second=%+v", first, second)
	}
	if first["priority"].(float64) != 100 || second["priority"].(float64) != 90 {
		t.Fatalf("unexpected priorities: first=%+v second=%+v", first, second)
	}
	if first["gateway_id"].(string) != "gw-a" || second["gateway_id"].(string) != "gw-b" {
		t.Fatalf("unexpected gateway ids: first=%+v second=%+v", first, second)
	}
}

func TestSelectDiscoveryPathsPolicy(t *testing.T) {
	discovery := map[string]any{
		"volume": map[string]any{
			"current_gateway_id":          "gw-a",
			"desired_active_gateway_set":  []any{"gw-a", "gw-c"},
			"observed_active_gateway_set": []any{"gw-c"},
			"path_plan_revision":          float64(7),
			"active_gateway_count":        float64(3),
		},
		"gateways": []any{
			map[string]any{"gateway_id": "gw-a"},
			map[string]any{"gateway_id": "gw-b"},
			map[string]any{"gateway_id": "gw-c"},
		},
		"dataplane_paths": []any{
			map[string]any{"path_id": float64(9), "gateway_id": "gw-b", "is_observed_gateway": false, "is_desired_gateway": false, "is_owner_gateway": false, "discovery_priority": float64(80)},
			map[string]any{"path_id": float64(7), "gateway_id": "gw-a", "is_observed_gateway": false, "is_desired_gateway": true, "is_owner_gateway": true, "discovery_priority": float64(100)},
			map[string]any{"path_id": float64(8), "gateway_id": "gw-c", "is_observed_gateway": true, "is_desired_gateway": true, "is_owner_gateway": false, "discovery_priority": float64(90)},
		},
	}

	selected := selectDiscoveryPaths(discovery, discoveryMergeOptions{MaxPaths: 2})
	paths := selected["dataplane_paths"].([]any)
	if len(paths) != 2 {
		t.Fatalf("unexpected path count: %+v", paths)
	}
	first := paths[0].(map[string]any)
	second := paths[1].(map[string]any)
	if first["gateway_id"].(string) != "gw-c" || second["gateway_id"].(string) != "gw-a" {
		t.Fatalf("unexpected ordering: first=%+v second=%+v", first, second)
	}
	if first["path_id"].(uint32) != 0 || second["path_id"].(uint32) != 1 {
		t.Fatalf("unexpected reassigned path ids: first=%+v second=%+v", first, second)
	}

	preferred := selectDiscoveryPaths(discovery, discoveryMergeOptions{MaxPaths: 1, PreferredGateway: "gw-b"})
	preferredPaths := preferred["dataplane_paths"].([]any)
	if len(preferredPaths) != 1 {
		t.Fatalf("unexpected preferred path count: %+v", preferredPaths)
	}
	preferredFirst := preferredPaths[0].(map[string]any)
	if preferredFirst["gateway_id"].(string) != "gw-b" {
		t.Fatalf("unexpected preferred gateway selection: %+v", preferredPaths)
	}
	if preferredFirst["path_id"].(uint32) != 0 {
		t.Fatalf("unexpected preferred path id reassignment: %+v", preferredPaths)
	}

	ownerOnly := selectDiscoveryPaths(discovery, discoveryMergeOptions{OwnerOnly: true})
	ownerPaths := ownerOnly["dataplane_paths"].([]any)
	if len(ownerPaths) != 1 || ownerPaths[0].(map[string]any)["gateway_id"].(string) != "gw-a" {
		t.Fatalf("unexpected owner-only selection: %+v", ownerPaths)
	}
}

func TestPrepareGatewayAttachManifest(t *testing.T) {
	raw := `{
		"volume_id":"00000065",
		"generation":3,
		"attachment_generation":3,
		"size_bytes":8388608,
		"block_size":4096,
		"attachment_id":"att-00000065-0003",
		"attached_host_id":"host-a",
		"attached_device_id":3,
		"writer_fencing_epoch":9,
		"handoff_required":true,
		"handoff_reason":"current_gateway_not_desired",
		"handoff_target_gateway_set":["gw-b"],
		"control_endpoints":[
			{"address":"10.0.0.1","port":9701,"use_tls":true,"bearer_token":"tok-a"},
			{"address":"10.0.0.2","port":9801,"use_tls":true,"bearer_token":"tok-b"}
		],
		"dataplane_endpoints":[
			{"path_id":9,"address":"10.0.0.2","port":9800,"priority":90},
			{"path_id":7,"address":"10.0.0.1","port":9700,"priority":100}
		]
	}`

	manifest, servers, err := prepareGatewayAttachManifest(raw)
	if err != nil {
		t.Fatalf("prepareGatewayAttachManifest failed: %v", err)
	}
	if len(servers) != 2 {
		t.Fatalf("unexpected servers: %+v", servers)
	}
	if servers[0].ID != 1 || servers[0].Address != "10.0.0.1" || servers[0].APIPrefix != "/api/v1" {
		t.Fatalf("unexpected first server: %+v", servers[0])
	}

	var out map[string]any
	if err := json.Unmarshal([]byte(manifest), &out); err != nil {
		t.Fatalf("unmarshal manifest: %v", err)
	}
	if out["attachment_generation"].(float64) != 3 || out["writer_fencing_epoch"].(float64) != 9 || out["handoff_required"].(bool) != true || out["handoff_reason"].(string) != "current_gateway_not_desired" {
		t.Fatalf("unexpected preserved handoff/fencing fields: %+v", out)
	}
	if targets := out["handoff_target_gateway_set"].([]any); len(targets) != 1 || targets[0].(string) != "gw-b" {
		t.Fatalf("unexpected handoff target set: %+v", out)
	}
	dataplane := out["dataplane_endpoints"].([]any)
	if dataplane[0].(map[string]any)["path_id"].(float64) != 0 || dataplane[1].(map[string]any)["path_id"].(float64) != 1 {
		t.Fatalf("unexpected normalized dataplane endpoints: %+v", dataplane)
	}
}

func TestSummarizeGatewayAttachManifest(t *testing.T) {
	summary, err := summarizeGatewayAttachManifest(`{
		"volume_id":"00000065",
		"generation":1,
		"attachment_id":"att-00000065-0001",
		"attachment_generation":3,
		"path_plan_revision":7,
		"writer_fencing_epoch":9,
		"runtime_path_expansion_eligible_at_unix":1,
		"handoff_required":true,
		"handoff_reason":"current_gateway_not_desired",
		"handoff_target_gateway_set":["gw-b"],
		"controller_priority_class":"handoff",
		"controller_recommended_actions":["complete_gateway_handoff","prefer_fewer_active_paths"],
		"cluster_priority_mismatch_actions":["complete_gateway_handoff_aggressively"]
	}`)
	if err != nil {
		t.Fatalf("summarizeGatewayAttachManifest failed: %v", err)
	}
	if summary["status"].(string) != "ok" || summary["volume_id"].(string) != "00000065" || summary["writer_fencing_epoch"].(float64) != 9 || summary["handoff_required"].(bool) != true {
		t.Fatalf("unexpected summary: %+v", summary)
	}
	if summary["controller_priority_class"].(string) != "handoff" {
		t.Fatalf("unexpected controller priority class: %+v", summary)
	}
	if summary["controller_runtime_expansion_state"].(string) != "eligible" {
		t.Fatalf("unexpected controller runtime expansion state: %+v", summary)
	}
	actions := summary["operator_recommended_actions"].([]string)
	if len(actions) != 3 || actions[0] != "complete_gateway_handoff" || actions[1] != "prefer_fewer_active_paths" || actions[2] != "refresh_gateway_path_plan" {
		t.Fatalf("unexpected operator recommended actions: %+v", actions)
	}
	mismatchActions := summary["cluster_priority_mismatch_actions"].([]any)
	if len(mismatchActions) != 1 || mismatchActions[0].(string) != "complete_gateway_handoff_aggressively" {
		t.Fatalf("unexpected cluster mismatch actions in summary: %+v", mismatchActions)
	}
}

func TestParsePathHealthSpec(t *testing.T) {
	pathID, state, err := parsePathHealthSpec("1=down")
	if err != nil {
		t.Fatalf("parsePathHealthSpec failed: %v", err)
	}
	if pathID != "1" || state != "down" {
		t.Fatalf("unexpected parse result: pathID=%s state=%s", pathID, state)
	}
	if _, _, err := parsePathHealthSpec("1=bad"); err == nil {
		t.Fatalf("expected invalid state error")
	}
}

func TestPathPlanToNetlinkRequest(t *testing.T) {
	req, err := pathPlanToNetlinkRequest(7, map[string]any{
		"path_plan_revision": float64(11),
		"active": []any{
			map[string]any{"path_id": float64(0)},
		},
		"standby": []any{
			map[string]any{"path_id": float64(1)},
		},
		"suppressed": []any{
			map[string]any{"path_id": float64(2)},
		},
	})
	if err != nil {
		t.Fatalf("pathPlanToNetlinkRequest failed: %v", err)
	}
	if req.DeviceID != 7 || req.PathPlanRevision != 11 || req.DegradedMask != 0x2 || req.DownMask != 0x4 || req.DrainingMask != 0 {
		t.Fatalf("unexpected request: %+v", req)
	}
}

func TestAdjustRuntimePathPlanRevisionBumpsSameRevisionMaskChange(t *testing.T) {
	req := adjustRuntimePathPlanRevision(
		netlinktlv.UpdatePathPlanRequest{DeviceID: 7, PathPlanRevision: 11, DownMask: 0, DegradedMask: 0},
		netlinktlv.DeviceStatus{DeviceID: 7, AppliedPathPlanRevision: 11, DownMask: 0x4, DegradedMask: 0x2},
	)
	if req.PathPlanRevision != 12 {
		t.Fatalf("expected bumped runtime revision, got %+v", req)
	}
}

func TestAdjustRuntimePathPlanRevisionKeepsIdempotentSameRevision(t *testing.T) {
	req := adjustRuntimePathPlanRevision(
		netlinktlv.UpdatePathPlanRequest{DeviceID: 7, PathPlanRevision: 11, DownMask: 0x4, DegradedMask: 0x2},
		netlinktlv.DeviceStatus{DeviceID: 7, AppliedPathPlanRevision: 11, DownMask: 0x4, DegradedMask: 0x2},
	)
	if req.PathPlanRevision != 11 {
		t.Fatalf("expected idempotent revision to stay unchanged, got %+v", req)
	}
}

func TestAdjustRuntimePathPlanRevisionPromotesLegacyAfterVersionedApply(t *testing.T) {
	req := adjustRuntimePathPlanRevision(
		netlinktlv.UpdatePathPlanRequest{DeviceID: 7, PathPlanRevision: 0, DownMask: 0x4},
		netlinktlv.DeviceStatus{DeviceID: 7, AppliedPathPlanRevision: 11, DownMask: 0},
	)
	if req.PathPlanRevision != 12 {
		t.Fatalf("expected legacy runtime update to be promoted above applied revision, got %+v", req)
	}
}

func TestRunAttachGatewayPrintsFencingAndHandoffSummary(t *testing.T) {
	client := &fakeNetlinkClient{}
	origNewGatewayClientFunc := newGatewayClientFunc
	newGatewayClientFunc = func(baseURL string, opts ...gatewayClientOptions) *gatewayClient {
		var resolved gatewayClientOptions
		if len(opts) > 0 {
			resolved = opts[0]
		}
		httpClient := &http.Client{
			Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
				rec := newResponseRecorder()
				switch {
				case req.Method == http.MethodPost && req.URL.Path == "/api/v1/volumes/00000065/attach":
					rec.Header().Set("Content-Type", "application/json")
					_, _ = rec.Write([]byte(`{
						"volume_id":"00000065",
						"generation":1,
						"size_bytes":8388608,
						"block_size":4096,
						"attachment_id":"att-00000065-0001",
						"attached_host_id":"host-a",
						"attached_device_id":7,
						"attachment_generation":3,
						"path_plan_revision":7,
						"writer_fencing_epoch":9,
						"runtime_path_expansion_eligible_at_unix":1,
						"handoff_required":true,
						"handoff_reason":"current_gateway_not_desired",
						"handoff_target_gateway_set":["gw-b"],
						"controller_priority_class":"handoff",
						"controller_recommended_actions":["complete_gateway_handoff","prefer_fewer_active_paths"],
						"control_endpoints":[{"address":"10.0.0.1","port":9701,"use_tls":true,"bearer_token":"tok-a"}],
						"dataplane_endpoints":[{"path_id":7,"address":"10.0.0.1","port":9700,"priority":100}]
					}`))
					return rec.Result(req), nil
				case req.Method == http.MethodGet && req.URL.Path == "/api/v1/debug/sbs-cluster/metrics":
					rec.Header().Set("Content-Type", "application/json")
					_, _ = rec.Write([]byte(`{
						"top_priority_class":"aggressive_handoff",
						"top_priority_count":2
					}`))
					return rec.Result(req), nil
				case req.Method == http.MethodGet && req.URL.Path == "/api/v1/discovery/volumes/00000065":
					rec.status = http.StatusNotFound
					_, _ = rec.Write([]byte(`not found`))
					return rec.Result(req), nil
				default:
					t.Fatalf("unexpected request: %s %s", req.Method, req.URL.Path)
					return nil, nil
				}
			}),
		}
		c := newGatewayClient(baseURL, resolved)
		c.httpClient = httpClient
		return c
	}
	defer func() {
		newGatewayClientFunc = origNewGatewayClientFunc
	}()

	output := captureStdout(t, func() {
		runAttach(client, []string{"--device", "7", "--host", "host-a", "--volume", "00000065", "--gateway", "http://gateway.test"})
	})
	var out map[string]any
	if err := json.Unmarshal([]byte(output), &out); err != nil {
		t.Fatalf("unmarshal attach output: %v", err)
	}
	if out["status"].(string) != "ok" || out["writer_fencing_epoch"].(float64) != 9 || out["handoff_required"].(bool) != true || out["handoff_reason"].(string) != "current_gateway_not_desired" {
		t.Fatalf("unexpected attach output: %+v", out)
	}
	if out["controller_priority_class"].(string) != "handoff" {
		t.Fatalf("unexpected attach controller priority class: %+v", out)
	}
	if out["cluster_top_priority_class"].(string) != "aggressive_handoff" || out["cluster_top_priority_count"].(float64) != 2 || out["cluster_priority_matches_controller"].(bool) != false {
		t.Fatalf("unexpected attach cluster priority summary: %+v", out)
	}
	mismatchActions := out["cluster_priority_mismatch_actions"].([]any)
	if len(mismatchActions) != 1 || mismatchActions[0].(string) != "complete_gateway_handoff_aggressively" {
		t.Fatalf("unexpected attach cluster mismatch actions: %+v", mismatchActions)
	}
	if out["controller_runtime_expansion_state"].(string) != "eligible" {
		t.Fatalf("unexpected attach controller runtime expansion state: %+v", out)
	}
	attachActions := out["controller_recommended_actions"].([]any)
	if len(attachActions) != 2 || attachActions[0].(string) != "complete_gateway_handoff" || attachActions[1].(string) != "prefer_fewer_active_paths" {
		t.Fatalf("unexpected attach controller actions: %+v", attachActions)
	}
	operatorActions := out["operator_recommended_actions"].([]any)
	if len(operatorActions) != 4 || operatorActions[0].(string) != "complete_gateway_handoff" || operatorActions[1].(string) != "prefer_fewer_active_paths" || operatorActions[2].(string) != "refresh_gateway_path_plan" || operatorActions[3].(string) != "complete_gateway_handoff_aggressively" {
		t.Fatalf("unexpected attach operator actions: %+v", operatorActions)
	}
	if targets := out["handoff_target_gateway_set"].([]any); len(targets) != 1 || targets[0].(string) != "gw-b" {
		t.Fatalf("unexpected attach output targets: %+v", out)
	}
	if client.attachManifestReq.ManifestJSON == "" {
		t.Fatalf("expected attach manifest request to be recorded")
	}
	var manifest map[string]any
	if err := json.Unmarshal([]byte(client.attachManifestReq.ManifestJSON), &manifest); err != nil {
		t.Fatalf("unmarshal attach manifest req: %v", err)
	}
	if manifest["writer_fencing_epoch"].(float64) != 9 || manifest["handoff_required"].(bool) != true {
		t.Fatalf("unexpected attach manifest request: %+v", manifest)
	}
	if manifest["runtime_path_expansion_eligible_at_unix"].(float64) != 1 {
		t.Fatalf("unexpected attach manifest runtime expansion eligibility: %+v", manifest)
	}
	if manifest["controller_priority_class"].(string) != "handoff" {
		t.Fatalf("unexpected attach manifest controller priority class: %+v", manifest)
	}
}

func TestSummarizePathPlanRevisionState(t *testing.T) {
	cases := []struct {
		name      string
		requested uint64
		applied   uint64
		want      string
	}{
		{name: "unversioned", requested: 0, applied: 0, want: "unversioned"},
		{name: "applied-versioned", requested: 0, applied: 3, want: "applied-versioned"},
		{name: "converged", requested: 7, applied: 7, want: "converged"},
		{name: "stale", requested: 7, applied: 6, want: "stale"},
		{name: "ahead", requested: 7, applied: 8, want: "ahead"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := summarizePathPlanRevisionState(tc.requested, tc.applied); got != tc.want {
				t.Fatalf("unexpected state: got=%s want=%s", got, tc.want)
			}
		})
	}
}

func TestSummarizeRuntimePathPlan(t *testing.T) {
	out := summarizeRuntimePathPlan(netlinktlv.DeviceStatus{
		PathCount:               4,
		DownMask:                0x8,
		DegradedMask:            0x2,
		DrainingMask:            0x4,
		AppliedPathPlanRevision: 12,
		ActiveLaneCount:         2,
		LaneRemapCount:          5,
		LastLaneRemappedLanes:   1,
		LastLaneRemapJiffies:    123,
		LastLaneRemapReason:     "path_state_change",
		Paths: []netlinktlv.PathStatus{
			{PathID: 0},
			{PathID: 1},
			{PathID: 2},
			{PathID: 3},
		},
		Lanes: []netlinktlv.LaneStatus{
			{LaneID: 0, PreferredPathID: 1, FallbackPathID: 0, Readiness: 2},
			{LaneID: 1, PreferredPathID: 2, FallbackPathID: ^uint32(0), Readiness: 4},
		},
	})
	if out["applied_revision"].(uint64) != 12 || out["path_count"].(uint32) != 4 || out["active_lane_count"].(uint32) != 2 || out["lane_remap_count"].(uint64) != 5 || out["last_lane_remapped_lanes"].(uint32) != 1 || out["last_lane_remap_jiffies"].(uint64) != 123 || out["last_lane_remap_reason"].(string) != "path_state_change" || out["up_paths"].(int) != 1 || out["degraded_paths"].(int) != 1 || out["down_paths"].(int) != 1 || out["draining_paths"].(int) != 1 || out["degraded_preferred_lanes"].(int) != 1 || out["down_preferred_lanes"].(int) != 0 || out["lanes_with_up_fallback"].(int) != 1 || out["lanes_without_fallback"].(int) != 1 || out["stable_lanes"].(int) != 0 || out["degraded_with_up_fallback"].(int) != 1 || out["degraded_without_up_fallback"].(int) != 0 || out["unavailable_lanes"].(int) != 1 || out["needs_attention"].(bool) != true {
		t.Fatalf("unexpected runtime path plan summary: %+v", out)
	}
	reasons := out["attention_reasons"].([]string)
	if len(reasons) != 1 || reasons[0] != "lane_unavailable" {
		t.Fatalf("unexpected attention reasons: %+v", reasons)
	}
}

func TestRecommendedRuntimePathPlanActions(t *testing.T) {
	got := recommendedRuntimePathPlanActions(map[string]any{
		"needs_attention":   true,
		"attention_reasons": []string{"lane_degraded_without_up_fallback", "lane_unavailable", "lane_down_preferred"},
	})
	want := []string{
		"refresh_gateway_path_plan",
		"prefer_fewer_active_paths",
		"reopen_or_reapply_path_plan",
		"reapply_latest_path_plan",
	}
	if len(got) != len(want) {
		t.Fatalf("unexpected actions length: got=%v want=%v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("unexpected actions: got=%v want=%v", got, want)
		}
	}
}

func TestEncodeLaneStatuses(t *testing.T) {
	out := encodeLaneStatuses([]netlinktlv.LaneStatus{
		{LaneID: 0, PreferredPathID: 2, FallbackPathID: 3, Readiness: 1, DispatchReqs: 99},
		{LaneID: 1, PreferredPathID: ^uint32(0), FallbackPathID: ^uint32(0), Readiness: 4},
	})
	if len(out) != 2 || out[0]["lane_id"].(uint32) != 0 || out[0]["preferred_path_id"].(uint32) != 2 || out[0]["fallback_path_id"].(uint32) != 3 || out[0]["readiness"].(string) != "stable" || out[0]["dispatch_reqs"].(uint64) != 99 || out[1]["preferred_path_id"] != nil || out[1]["fallback_path_id"] != nil || out[1]["readiness"].(string) != "unavailable" {
		t.Fatalf("unexpected lane output: %+v", out)
	}
}

func TestEncodePathStatuses(t *testing.T) {
	out := encodePathStatuses([]netlinktlv.PathStatus{
		{
			PathID:            7,
			State:             0,
			ConsecutiveErrors: 2,
			LastErrno:         11,
			LastWireStatus:    5,
			GatewayID:         "gw-b",
			Address:           "10.0.0.2",
			Port:              9800,
			UseTLS:            true,
			ServerName:        "gw-b",
			Priority:          90,
			Connected:         true,
			Inflight:          3,
			Pending:           2,
			PendingHighWater:  12,
			OutstandingLimit:  16,
			Submitted:         4321,
			Completed:         1234,
			Retries:           5,
			ConnOpens:         2,
			ConnResets:        1,
		},
	})
	if len(out) != 1 {
		t.Fatalf("unexpected path output length: %+v", out)
	}
	entry := out[0]
	if entry["path_id"].(uint32) != 7 || entry["state"].(string) != "up" || entry["consecutive_errors"].(uint32) != 2 || entry["last_errno"].(uint32) != 11 || entry["last_wire_status"].(uint32) != 5 {
		t.Fatalf("unexpected base path output: %+v", entry)
	}
	if entry["gateway_id"].(string) != "gw-b" || entry["address"].(string) != "10.0.0.2" || entry["port"].(uint16) != 9800 || entry["use_tls"].(bool) != true || entry["server_name"].(string) != "gw-b" || entry["priority"].(uint32) != 90 {
		t.Fatalf("unexpected extended path output: %+v", entry)
	}
	if entry["connected"].(bool) != true || entry["inflight"].(uint32) != 3 || entry["pending"].(uint32) != 2 || entry["pending_high_water"].(uint32) != 12 || entry["outstanding_limit"].(uint32) != 16 || entry["submitted"].(uint64) != 4321 || entry["completed"].(uint64) != 1234 || entry["retries"].(uint64) != 5 || entry["conn_opens"].(uint64) != 2 || entry["conn_resets"].(uint64) != 1 {
		t.Fatalf("unexpected async path output: %+v", entry)
	}
}

func TestRunStatusIncludesExpectedPathPlanRevisionState(t *testing.T) {
	client := &fakeNetlinkClient{
		status: netlinktlv.DeviceStatus{
			DeviceID:                3,
			DiskName:                "namrbd3",
			Attached:                true,
			VolumeID:                0x65,
			Generation:              9,
			PathCount:               2,
			AppliedPathPlanRevision: 5,
			ActiveLaneCount:         1,
			LaneRemapCount:          2,
			LastLaneRemappedLanes:   1,
			LastLaneRemapJiffies:    55,
			LastLaneRemapReason:     "path_plan_apply",
			Lanes: []netlinktlv.LaneStatus{
				{LaneID: 0, PreferredPathID: 0, FallbackPathID: 1, Readiness: 1},
			},
		},
	}
	output := captureStdout(t, func() {
		runStatus(client, []string{"--device", "3", "--expected-path-plan-revision", "7"})
	})
	var out map[string]any
	if err := json.Unmarshal([]byte(output), &out); err != nil {
		t.Fatalf("unmarshal status output: %v", err)
	}
	if out["expected_path_plan_revision"].(float64) != 7 || out["path_plan_revision_state"].(string) != "stale" {
		t.Fatalf("unexpected status output: %+v", out)
	}
	actions := out["recommended_actions"].([]any)
	if len(actions) != 0 {
		t.Fatalf("unexpected recommended actions: %+v", actions)
	}
	runtimePlan := out["runtime_path_plan"].(map[string]any)
	if runtimePlan["applied_revision"].(float64) != 5 || runtimePlan["path_count"].(float64) != 2 || runtimePlan["active_lane_count"].(float64) != 1 || runtimePlan["lane_remap_count"].(float64) != 2 || runtimePlan["last_lane_remapped_lanes"].(float64) != 1 || runtimePlan["last_lane_remap_jiffies"].(float64) != 55 || runtimePlan["last_lane_remap_reason"].(string) != "path_plan_apply" || runtimePlan["up_paths"].(float64) != 2 {
		t.Fatalf("unexpected runtime path plan: %+v", runtimePlan)
	}
	lanes := out["lanes"].([]any)
	if len(lanes) != 1 || lanes[0].(map[string]any)["lane_id"].(float64) != 0 || lanes[0].(map[string]any)["preferred_path_id"].(float64) != 0 || lanes[0].(map[string]any)["fallback_path_id"].(float64) != 1 || lanes[0].(map[string]any)["readiness"].(string) != "stable" {
		t.Fatalf("unexpected lanes output: %+v", lanes)
	}
}

func TestRunStatusIncludesGatewayManifestComparison(t *testing.T) {
	client := &fakeNetlinkClient{
		status: netlinktlv.DeviceStatus{
			DeviceID:                3,
			DiskName:                "namrbd3",
			Attached:                true,
			VolumeID:                0x65,
			Generation:              3,
			PathCount:               2,
			AppliedPathPlanRevision: 7,
		},
	}
	origNewGatewayClientFunc := newGatewayClientFunc
	newGatewayClientFunc = func(baseURL string, opts ...gatewayClientOptions) *gatewayClient {
		var resolved gatewayClientOptions
		if len(opts) > 0 {
			resolved = opts[0]
		}
		httpClient := &http.Client{
			Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
				rec := newResponseRecorder()
				switch {
				case req.Method == http.MethodGet && req.URL.Path == "/api/v1/volumes/00000065/info":
					rec.Header().Set("Content-Type", "application/json")
					_, _ = rec.Write([]byte(`{
						"volume_id":"00000065",
						"attachment_generation":3,
						"path_plan_revision":7,
						"runtime_path_expansion_backoff_level":2,
						"runtime_path_expansion_eligible_at_unix":4102444800,
						"runtime_applied_path_plan_revision":7,
						"writer_fencing_epoch":9,
						"handoff_required":false,
						"controller_priority_class":"normal"
					}`))
					return rec.Result(req), nil
				case req.Method == http.MethodGet && req.URL.Path == "/api/v1/debug/sbs-cluster/metrics":
					rec.Header().Set("Content-Type", "application/json")
					_, _ = rec.Write([]byte(`{
						"top_priority_class":"normal",
						"top_priority_count":1
					}`))
					return rec.Result(req), nil
				default:
					t.Fatalf("unexpected request: %s %s", req.Method, req.URL.Path)
					return nil, nil
				}
			}),
		}
		c := newGatewayClient(baseURL, resolved)
		c.httpClient = httpClient
		return c
	}
	defer func() {
		newGatewayClientFunc = origNewGatewayClientFunc
	}()

	output := captureStdout(t, func() {
		runStatus(client, []string{"--device", "3", "--gateway", "http://gateway.test"})
	})
	var out map[string]any
	if err := json.Unmarshal([]byte(output), &out); err != nil {
		t.Fatalf("unmarshal status output: %v", err)
	}
	manifest := out["gateway_manifest"].(map[string]any)
	if manifest["writer_fencing_epoch"].(float64) != 9 || manifest["path_plan_revision"].(float64) != 7 {
		t.Fatalf("unexpected gateway manifest: %+v", manifest)
	}
	if manifest["runtime_path_expansion_eligible_at_unix"].(float64) != 4102444800 {
		t.Fatalf("unexpected runtime expansion eligibility in gateway manifest: %+v", manifest)
	}
	if manifest["controller_priority_class"].(string) != "normal" {
		t.Fatalf("unexpected controller priority class: %+v", manifest)
	}
	if out["controller_priority_class"].(string) != "normal" {
		t.Fatalf("unexpected top-level controller priority class: %+v", out)
	}
	if out["cluster_top_priority_class"].(string) != "normal" || out["cluster_top_priority_count"].(float64) != 1 || out["cluster_priority_matches_controller"].(bool) != true {
		t.Fatalf("unexpected cluster top priority summary: %+v", out)
	}
	if out["controller_runtime_expansion_state"].(string) != "waiting" {
		t.Fatalf("unexpected top-level controller runtime expansion state: %+v", out)
	}
	if out["controller_runtime_expansion_backoff_level"].(float64) != 2 {
		t.Fatalf("unexpected top-level controller runtime expansion backoff level: %+v", out)
	}
	if out["controller_runtime_expansion_backoff_level"].(float64) != 2 {
		t.Fatalf("unexpected top-level controller runtime expansion backoff level: %+v", out)
	}
	comparison := out["manifest_runtime_comparison"].(map[string]any)
	if comparison["attachment_generation_state"].(string) != "converged" || comparison["path_plan_revision_state"].(string) != "converged" || comparison["volume_identity_state"].(string) != "converged" {
		t.Fatalf("unexpected manifest/runtime comparison: %+v", comparison)
	}
	if comparison["reapply_convergence_state"].(string) != "converged" {
		t.Fatalf("unexpected reapply convergence state: %+v", comparison)
	}
	if comparison["handoff_convergence_state"].(string) != "not_required" {
		t.Fatalf("unexpected handoff convergence state: %+v", comparison)
	}
	if comparison["runtime_expansion_state"].(string) != "waiting" {
		t.Fatalf("unexpected runtime expansion state: %+v", comparison)
	}
	if comparison["runtime_expansion_backoff_level"].(float64) != 2 {
		t.Fatalf("unexpected runtime expansion backoff level: %+v", comparison)
	}
	if comparison["runtime_expansion_backoff_level"].(float64) != 2 {
		t.Fatalf("unexpected runtime expansion backoff level: %+v", comparison)
	}
	handoffFencing := out["handoff_fencing"].(map[string]any)
	if handoffFencing["attachment_generation"].(float64) != 3 ||
		handoffFencing["writer_fencing_epoch"].(float64) != 9 ||
		handoffFencing["handoff_required"].(bool) ||
		handoffFencing["attachment_generation_state"].(string) != "converged" ||
		handoffFencing["handoff_convergence_state"].(string) != "not_required" {
		t.Fatalf("unexpected handoff/fencing summary: %+v", handoffFencing)
	}
	manifestActions := out["manifest_recommended_actions"].([]any)
	if len(manifestActions) != 0 {
		t.Fatalf("unexpected manifest recommended actions: %+v", manifestActions)
	}
	controllerActions := out["controller_recommended_actions"].([]any)
	if len(controllerActions) != 0 {
		t.Fatalf("unexpected top-level controller actions: %+v", controllerActions)
	}
	operatorActions := out["operator_recommended_actions"].([]any)
	if len(operatorActions) != 0 {
		t.Fatalf("unexpected operator recommended actions: %+v", operatorActions)
	}
}

func TestRunStatusDistinguishesRotatedWriterPendingHandoffConvergence(t *testing.T) {
	client := &fakeNetlinkClient{
		status: netlinktlv.DeviceStatus{
			DeviceID:                5,
			DiskName:                "namrbd5",
			Attached:                true,
			VolumeID:                0x65,
			Generation:              3,
			PathCount:               2,
			AppliedPathPlanRevision: 5,
		},
	}
	origNewGatewayClientFunc := newGatewayClientFunc
	newGatewayClientFunc = func(baseURL string, opts ...gatewayClientOptions) *gatewayClient {
		var resolved gatewayClientOptions
		if len(opts) > 0 {
			resolved = opts[0]
		}
		httpClient := &http.Client{
			Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
				rec := newResponseRecorder()
				switch {
				case req.Method == http.MethodGet && req.URL.Path == "/api/v1/volumes/00000065/info":
					rec.Header().Set("Content-Type", "application/json")
					_, _ = rec.Write([]byte(`{
						"volume_id":"00000065",
						"attachment_generation":3,
						"path_plan_revision":7,
						"runtime_applied_path_plan_revision":5,
						"writer_fencing_epoch":9,
						"handoff_required":true,
						"handoff_reason":"current_gateway_not_desired",
						"controller_priority_class":"handoff"
					}`))
					return rec.Result(req), nil
				case req.Method == http.MethodGet && req.URL.Path == "/api/v1/debug/sbs-cluster/metrics":
					rec.Header().Set("Content-Type", "application/json")
					_, _ = rec.Write([]byte(`{
						"top_priority_class":"handoff",
						"top_priority_count":1
					}`))
					return rec.Result(req), nil
				default:
					t.Fatalf("unexpected request: %s %s", req.Method, req.URL.Path)
					return nil, nil
				}
			}),
		}
		c := newGatewayClient(baseURL, resolved)
		c.httpClient = httpClient
		return c
	}
	defer func() {
		newGatewayClientFunc = origNewGatewayClientFunc
	}()

	output := captureStdout(t, func() {
		runStatus(client, []string{"--device", "5", "--gateway", "http://gateway.test"})
	})
	var out map[string]any
	if err := json.Unmarshal([]byte(output), &out); err != nil {
		t.Fatalf("unmarshal status output: %v", err)
	}
	comparison := out["manifest_runtime_comparison"].(map[string]any)
	if comparison["attachment_generation_state"].(string) != "converged" {
		t.Fatalf("unexpected attachment generation state: %+v", comparison)
	}
	if comparison["path_plan_revision_state"].(string) != "stale" {
		t.Fatalf("unexpected path plan revision state: %+v", comparison)
	}
	if comparison["reapply_convergence_state"].(string) != "stale" {
		t.Fatalf("unexpected reapply convergence state: %+v", comparison)
	}
	if comparison["handoff_convergence_state"].(string) != "target_attached_pending_path_convergence" {
		t.Fatalf("unexpected handoff convergence state: %+v", comparison)
	}
	actions := out["manifest_recommended_actions"].([]any)
	for _, forbidden := range []string{"reattach_via_gateway"} {
		for _, action := range actions {
			if action.(string) == forbidden {
				t.Fatalf("unexpected action %q in %+v", forbidden, actions)
			}
		}
	}
}

func TestRunStatusDistinguishesStalledGenerationRotationHandoff(t *testing.T) {
	client := &fakeNetlinkClient{
		status: netlinktlv.DeviceStatus{
			DeviceID:                6,
			DiskName:                "namrbd6",
			Attached:                true,
			VolumeID:                0x65,
			Generation:              2,
			PathCount:               2,
			AppliedPathPlanRevision: 5,
		},
	}
	origNewGatewayClientFunc := newGatewayClientFunc
	newGatewayClientFunc = func(baseURL string, opts ...gatewayClientOptions) *gatewayClient {
		var resolved gatewayClientOptions
		if len(opts) > 0 {
			resolved = opts[0]
		}
		httpClient := &http.Client{
			Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
				rec := newResponseRecorder()
				switch {
				case req.Method == http.MethodGet && req.URL.Path == "/api/v1/volumes/00000065/info":
					rec.Header().Set("Content-Type", "application/json")
					_, _ = rec.Write([]byte(`{
						"volume_id":"00000065",
						"attachment_generation":3,
						"path_plan_revision":7,
						"runtime_applied_path_plan_revision":5,
						"writer_fencing_epoch":9,
						"handoff_required":true,
						"handoff_requested_at_unix":1,
						"handoff_escalation_count":2,
						"handoff_next_escalation_at_unix":1,
						"handoff_stage":"pending_generation_rotation",
						"handoff_reason":"handoff_generation_rotation_stalled_current_gateway",
						"controller_priority_class":"aggressive_handoff"
					}`))
					return rec.Result(req), nil
				case req.Method == http.MethodGet && req.URL.Path == "/api/v1/debug/sbs-cluster/metrics":
					rec.Header().Set("Content-Type", "application/json")
					_, _ = rec.Write([]byte(`{
						"top_priority_class":"aggressive_handoff",
						"top_priority_count":1
					}`))
					return rec.Result(req), nil
				default:
					t.Fatalf("unexpected request: %s %s", req.Method, req.URL.Path)
					return nil, nil
				}
			}),
		}
		c := newGatewayClient(baseURL, resolved)
		c.httpClient = httpClient
		return c
	}
	defer func() {
		newGatewayClientFunc = origNewGatewayClientFunc
	}()

	output := captureStdout(t, func() {
		runStatus(client, []string{"--device", "6", "--gateway", "http://gateway.test"})
	})
	var out map[string]any
	if err := json.Unmarshal([]byte(output), &out); err != nil {
		t.Fatalf("unmarshal status output: %v", err)
	}
	comparison := out["manifest_runtime_comparison"].(map[string]any)
	if comparison["handoff_convergence_state"].(string) != "pending_generation_rotation_stalled" {
		t.Fatalf("unexpected handoff convergence state: %+v", comparison)
	}
	if comparison["manifest_handoff_escalation_count"].(float64) != 2 || comparison["handoff_backoff_state"].(string) != "eligible" {
		t.Fatalf("unexpected handoff backoff summary: %+v", comparison)
	}
	if out["controller_handoff_backoff_state"].(string) != "eligible" {
		t.Fatalf("unexpected top-level handoff backoff state: %+v", out)
	}
	actions := out["manifest_recommended_actions"].([]any)
	foundAggressive := false
	for _, action := range actions {
		if action.(string) == "complete_gateway_handoff_aggressively" {
			foundAggressive = true
			break
		}
	}
	if !foundAggressive {
		t.Fatalf("expected aggressive handoff action for stalled generation rotation: %+v", actions)
	}
}

func TestRunStatusWaitsDuringGenerationRotationBackoff(t *testing.T) {
	client := &fakeNetlinkClient{
		status: netlinktlv.DeviceStatus{
			DeviceID:                7,
			DiskName:                "namrbd7",
			Attached:                true,
			VolumeID:                0x65,
			Generation:              2,
			PathCount:               2,
			AppliedPathPlanRevision: 5,
		},
	}
	origNewGatewayClientFunc := newGatewayClientFunc
	newGatewayClientFunc = func(baseURL string, opts ...gatewayClientOptions) *gatewayClient {
		var resolved gatewayClientOptions
		if len(opts) > 0 {
			resolved = opts[0]
		}
		httpClient := &http.Client{
			Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
				rec := newResponseRecorder()
				switch {
				case req.Method == http.MethodGet && req.URL.Path == "/api/v1/volumes/00000065/info":
					rec.Header().Set("Content-Type", "application/json")
					_, _ = rec.Write([]byte(`{
						"volume_id":"00000065",
						"attachment_generation":3,
						"path_plan_revision":7,
						"runtime_applied_path_plan_revision":5,
						"writer_fencing_epoch":9,
						"handoff_required":true,
						"handoff_requested_at_unix":1,
						"handoff_escalation_count":1,
						"handoff_next_escalation_at_unix":4102444800,
						"handoff_stage":"pending_generation_rotation",
						"handoff_reason":"handoff_generation_rotation_stalled_current_gateway",
						"controller_priority_class":"handoff"
					}`))
					return rec.Result(req), nil
				case req.Method == http.MethodGet && req.URL.Path == "/api/v1/debug/sbs-cluster/metrics":
					rec.Header().Set("Content-Type", "application/json")
					_, _ = rec.Write([]byte(`{
						"top_priority_class":"handoff",
						"top_priority_count":1
					}`))
					return rec.Result(req), nil
				default:
					t.Fatalf("unexpected request: %s %s", req.Method, req.URL.Path)
					return nil, nil
				}
			}),
		}
		c := newGatewayClient(baseURL, resolved)
		c.httpClient = httpClient
		return c
	}
	defer func() {
		newGatewayClientFunc = origNewGatewayClientFunc
	}()

	output := captureStdout(t, func() {
		runStatus(client, []string{"--device", "7", "--gateway", "http://gateway.test"})
	})
	var out map[string]any
	if err := json.Unmarshal([]byte(output), &out); err != nil {
		t.Fatalf("unmarshal status output: %v", err)
	}
	comparison := out["manifest_runtime_comparison"].(map[string]any)
	if comparison["handoff_backoff_state"].(string) != "waiting" {
		t.Fatalf("unexpected handoff backoff state: %+v", comparison)
	}
	if out["controller_handoff_backoff_state"].(string) != "waiting" {
		t.Fatalf("unexpected top-level handoff backoff state: %+v", out)
	}
	actions := out["manifest_recommended_actions"].([]any)
	for _, action := range actions {
		if action.(string) == "complete_gateway_handoff_aggressively" {
			t.Fatalf("did not expect aggressive handoff during backoff wait: %+v", actions)
		}
	}
	if out["controller_priority_class"].(string) != "handoff" {
		t.Fatalf("unexpected controller priority class during backoff wait: %+v", out)
	}
}

func TestRunStatusIncludesExpansionReadyControllerPriorityClass(t *testing.T) {
	client := &fakeNetlinkClient{
		status: netlinktlv.DeviceStatus{
			DeviceID:                4,
			DiskName:                "namrbd4",
			Attached:                true,
			VolumeID:                0x65,
			Generation:              3,
			PathCount:               2,
			AppliedPathPlanRevision: 9,
		},
	}
	origNewGatewayClientFunc := newGatewayClientFunc
	newGatewayClientFunc = func(baseURL string, opts ...gatewayClientOptions) *gatewayClient {
		var resolved gatewayClientOptions
		if len(opts) > 0 {
			resolved = opts[0]
		}
		httpClient := &http.Client{
			Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
				rec := newResponseRecorder()
				switch {
				case req.Method == http.MethodGet && req.URL.Path == "/api/v1/volumes/00000065/info":
					rec.Header().Set("Content-Type", "application/json")
					_, _ = rec.Write([]byte(`{
						"volume_id":"00000065",
						"attachment_generation":3,
						"path_plan_revision":9,
						"runtime_applied_path_plan_revision":9,
						"runtime_path_expansion_eligible_at_unix":1,
						"controller_priority_class":"expansion_ready",
						"controller_recommended_actions":["refresh_gateway_path_plan"]
					}`))
					return rec.Result(req), nil
				case req.Method == http.MethodGet && req.URL.Path == "/api/v1/debug/sbs-cluster/metrics":
					rec.Header().Set("Content-Type", "application/json")
					_, _ = rec.Write([]byte(`{
						"top_priority_class":"expansion_ready",
						"top_priority_count":2
					}`))
					return rec.Result(req), nil
				default:
					t.Fatalf("unexpected request: %s %s", req.Method, req.URL.Path)
					return nil, nil
				}
			}),
		}
		c := newGatewayClient(baseURL, resolved)
		c.httpClient = httpClient
		return c
	}
	defer func() {
		newGatewayClientFunc = origNewGatewayClientFunc
	}()

	output := captureStdout(t, func() {
		runStatus(client, []string{"--device", "4", "--gateway", "http://gateway.test"})
	})
	var out map[string]any
	if err := json.Unmarshal([]byte(output), &out); err != nil {
		t.Fatalf("unmarshal status output: %v", err)
	}
	if out["controller_priority_class"].(string) != "expansion_ready" {
		t.Fatalf("unexpected top-level controller priority class: %+v", out)
	}
	if out["cluster_top_priority_class"].(string) != "expansion_ready" || out["cluster_top_priority_count"].(float64) != 2 || out["cluster_priority_matches_controller"].(bool) != true {
		t.Fatalf("unexpected cluster top priority summary: %+v", out)
	}
	if out["controller_runtime_expansion_state"].(string) != "eligible" {
		t.Fatalf("unexpected top-level controller runtime expansion state: %+v", out)
	}
	manifest := out["gateway_manifest"].(map[string]any)
	if manifest["controller_priority_class"].(string) != "expansion_ready" {
		t.Fatalf("unexpected gateway manifest controller priority class: %+v", manifest)
	}
	controllerActions := out["controller_recommended_actions"].([]any)
	if len(controllerActions) != 1 || controllerActions[0].(string) != "refresh_gateway_path_plan" {
		t.Fatalf("unexpected controller actions: %+v", controllerActions)
	}
}

func TestRecommendedManifestRuntimeActions(t *testing.T) {
	actions := recommendedManifestRuntimeActions(map[string]any{
		"volume_identity_state":       "mismatch",
		"attachment_generation_state": "stale",
		"path_plan_revision_state":    "stale",
		"runtime_expansion_state":     "eligible",
		"manifest_handoff_required":   true,
		"manifest_handoff_reason":     "runtime_hold_borderline_current_gateway",
	})
	want := []string{
		"detach_and_reattach",
		"reattach_via_gateway",
		"reapply_latest_path_plan",
		"refresh_gateway_path_plan",
		"complete_gateway_handoff",
		"complete_gateway_handoff_aggressively",
	}
	if len(actions) != len(want) {
		t.Fatalf("unexpected actions len: got=%v want=%v", actions, want)
	}
	for i := range want {
		if actions[i] != want[i] {
			t.Fatalf("unexpected actions: got=%v want=%v", actions, want)
		}
	}
}

func TestRecommendedManifestRuntimeActionsEscalatesStalledGenerationRotation(t *testing.T) {
	actions := recommendedManifestRuntimeActions(map[string]any{
		"attachment_generation_state": "stale",
		"manifest_handoff_required":   true,
		"manifest_handoff_reason":     "handoff_generation_rotation_stalled_current_gateway",
		"handoff_convergence_state":   "pending_generation_rotation_stalled",
		"handoff_backoff_state":       "eligible",
	})
	if len(actions) != 3 {
		t.Fatalf("unexpected actions len: %+v", actions)
	}
	if actions[0] != "reattach_via_gateway" || actions[1] != "complete_gateway_handoff" || actions[2] != "complete_gateway_handoff_aggressively" {
		t.Fatalf("unexpected actions: %+v", actions)
	}
}

func TestRecommendedManifestRuntimeActionsWaitDuringGenerationRotationBackoff(t *testing.T) {
	actions := recommendedManifestRuntimeActions(map[string]any{
		"attachment_generation_state": "stale",
		"manifest_handoff_required":   true,
		"manifest_handoff_reason":     "handoff_generation_rotation_stalled_current_gateway",
		"handoff_convergence_state":   "pending_generation_rotation_stalled",
		"handoff_backoff_state":       "waiting",
	})
	if len(actions) != 2 {
		t.Fatalf("unexpected actions len: %+v", actions)
	}
	if actions[0] != "reattach_via_gateway" || actions[1] != "complete_gateway_handoff" {
		t.Fatalf("unexpected actions: %+v", actions)
	}
}

func TestRuntimeFeedbackPayload(t *testing.T) {
	payload := runtimeFeedbackPayload(map[string]any{
		"needs_attention":   true,
		"attention_reasons": []string{"lane_unavailable", "lane_down_preferred"},
		"applied_revision":  uint64(11),
	}, map[string]any{
		"state":          "queueing",
		"retry_mode":     "queue",
		"retry_seconds":  uint32(0),
		"queued_reqs":    uint64(3),
		"requeued_reqs":  uint64(2),
		"failed_reqs":    uint64(0),
		"recovered_reqs": uint64(0),
		"enter_count":    uint64(1),
		"last_reason":    "all_paths_down",
	}, []string{"refresh_gateway_path_plan", "reapply_latest_path_plan", "refresh_gateway_path_plan"}, "host-a")
	if payload["needs_attention"].(bool) != true {
		t.Fatalf("unexpected needs_attention: %+v", payload)
	}
	reasons := payload["attention_reasons"].([]string)
	if len(reasons) != 2 || reasons[0] != "lane_unavailable" || reasons[1] != "lane_down_preferred" {
		t.Fatalf("unexpected reasons: %+v", reasons)
	}
	actions := payload["recommended_actions"].([]string)
	if len(actions) != 2 || actions[0] != "refresh_gateway_path_plan" || actions[1] != "reapply_latest_path_plan" {
		t.Fatalf("unexpected actions: %+v", actions)
	}
	if payload["applied_path_plan_revision"].(uint64) != 11 {
		t.Fatalf("unexpected applied revision: %+v", payload)
	}
	if payload["source_host"].(string) != "host-a" {
		t.Fatalf("unexpected source host: %+v", payload)
	}
	noPath := payload["no_path"].(map[string]any)
	if noPath["state"].(string) != "queueing" || noPath["retry_mode"].(string) != "queue" || noPath["queued_reqs"].(uint64) != 3 {
		t.Fatalf("unexpected no-path feedback: %+v", noPath)
	}
}

func TestRunStatusIncludesManifestRecommendedActions(t *testing.T) {
	client := &fakeNetlinkClient{
		status: netlinktlv.DeviceStatus{
			DeviceID:                3,
			DiskName:                "namrbd3",
			Attached:                true,
			VolumeID:                0x65,
			Generation:              2,
			PathCount:               2,
			AppliedPathPlanRevision: 5,
		},
	}
	origNewGatewayClientFunc := newGatewayClientFunc
	newGatewayClientFunc = func(baseURL string, opts ...gatewayClientOptions) *gatewayClient {
		var resolved gatewayClientOptions
		if len(opts) > 0 {
			resolved = opts[0]
		}
		httpClient := &http.Client{
			Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
				rec := newResponseRecorder()
				switch {
				case req.Method == http.MethodGet && req.URL.Path == "/api/v1/volumes/00000065/info":
					rec.Header().Set("Content-Type", "application/json")
					_, _ = rec.Write([]byte(`{
						"volume_id":"00000066",
						"attachment_generation":3,
						"path_plan_revision":7,
						"runtime_applied_path_plan_revision":5,
						"runtime_path_expansion_eligible_at_unix":1,
						"writer_fencing_epoch":9,
						"handoff_required":true,
						"handoff_reason":"runtime_hold_borderline_current_gateway",
						"controller_priority_class":"aggressive_handoff"
					}`))
					return rec.Result(req), nil
				case req.Method == http.MethodGet && req.URL.Path == "/api/v1/debug/sbs-cluster/metrics":
					rec.Header().Set("Content-Type", "application/json")
					_, _ = rec.Write([]byte(`{
						"top_priority_class":"refresh",
						"top_priority_count":4
					}`))
					return rec.Result(req), nil
				default:
					t.Fatalf("unexpected request: %s %s", req.Method, req.URL.Path)
					return nil, nil
				}
			}),
		}
		c := newGatewayClient(baseURL, resolved)
		c.httpClient = httpClient
		return c
	}
	defer func() {
		newGatewayClientFunc = origNewGatewayClientFunc
	}()

	output := captureStdout(t, func() {
		runStatus(client, []string{"--device", "3", "--gateway", "http://gateway.test"})
	})
	var out map[string]any
	if err := json.Unmarshal([]byte(output), &out); err != nil {
		t.Fatalf("unmarshal status output: %v", err)
	}
	actions := out["manifest_recommended_actions"].([]any)
	want := []string{
		"detach_and_reattach",
		"reattach_via_gateway",
		"reapply_latest_path_plan",
		"refresh_gateway_path_plan",
		"complete_gateway_handoff",
		"complete_gateway_handoff_aggressively",
	}
	if len(actions) != len(want) {
		t.Fatalf("unexpected actions len: got=%v want=%v", actions, want)
	}
	comparison := out["manifest_runtime_comparison"].(map[string]any)
	if comparison["handoff_convergence_state"].(string) != "pending_generation_rotation" {
		t.Fatalf("unexpected handoff convergence state: %+v", comparison)
	}
	manifest := out["gateway_manifest"].(map[string]any)
	if manifest["controller_priority_class"].(string) != "aggressive_handoff" {
		t.Fatalf("unexpected gateway manifest priority class: %+v", manifest)
	}
	if out["controller_priority_class"].(string) != "aggressive_handoff" {
		t.Fatalf("unexpected top-level controller priority class: %+v", out)
	}
	if out["cluster_top_priority_class"].(string) != "refresh" || out["cluster_top_priority_count"].(float64) != 4 || out["cluster_priority_matches_controller"].(bool) != false {
		t.Fatalf("unexpected cluster top priority summary: %+v", out)
	}
	mismatchActions := out["cluster_priority_mismatch_actions"].([]any)
	if len(mismatchActions) != 1 || mismatchActions[0].(string) != "refresh_gateway_path_plan" {
		t.Fatalf("unexpected cluster mismatch actions: %+v", mismatchActions)
	}
	if out["controller_runtime_expansion_state"].(string) != "eligible" {
		t.Fatalf("unexpected top-level controller runtime expansion state: %+v", out)
	}
	for i := range want {
		if actions[i].(string) != want[i] {
			t.Fatalf("unexpected actions: got=%+v want=%+v", actions, want)
		}
	}
	comparison = out["manifest_runtime_comparison"].(map[string]any)
	if comparison["reapply_convergence_state"].(string) != "stale" {
		t.Fatalf("unexpected reapply convergence state: %+v", comparison)
	}
	if comparison["runtime_expansion_state"].(string) != "eligible" {
		t.Fatalf("unexpected runtime expansion state: %+v", comparison)
	}
	controllerActions := out["controller_recommended_actions"].([]any)
	wantController := []string{
		"detach_and_reattach",
		"reattach_via_gateway",
		"reapply_latest_path_plan",
		"refresh_gateway_path_plan",
		"complete_gateway_handoff",
		"complete_gateway_handoff_aggressively",
	}
	if len(controllerActions) != len(wantController) {
		t.Fatalf("unexpected controller actions len: got=%v want=%v", controllerActions, wantController)
	}
	for i := range wantController {
		if controllerActions[i].(string) != wantController[i] {
			t.Fatalf("unexpected controller actions: got=%+v want=%+v", controllerActions, wantController)
		}
	}
	operatorActions := out["operator_recommended_actions"].([]any)
	if len(operatorActions) != len(wantController) {
		t.Fatalf("unexpected operator actions len: got=%v want=%v", operatorActions, wantController)
	}
	for i := range wantController {
		if operatorActions[i].(string) != wantController[i] {
			t.Fatalf("unexpected operator actions: got=%+v want=%+v", operatorActions, wantController)
		}
	}
}

func TestRunStatusReportsRuntimeFeedbackToGateway(t *testing.T) {
	client := &fakeNetlinkClient{
		status: netlinktlv.DeviceStatus{
			DeviceID:                3,
			DiskName:                "namrbd3",
			Attached:                true,
			VolumeID:                0x65,
			Generation:              2,
			PathCount:               2,
			AppliedPathPlanRevision: 5,
			NoPathRetryMode:         1,
			NoPathState:             1,
			NoPathQueuedReqs:        4,
			NoPathRequeuedReqs:      3,
			NoPathEnterCount:        1,
			LastNoPathReason:        3,
			DownMask:                0x1,
			Lanes: []netlinktlv.LaneStatus{
				{LaneID: 0, PreferredPathID: 0, FallbackPathID: ^uint32(0), Readiness: 4},
			},
		},
	}
	var posted map[string]any
	origNewGatewayClientFunc := newGatewayClientFunc
	newGatewayClientFunc = func(baseURL string, opts ...gatewayClientOptions) *gatewayClient {
		var resolved gatewayClientOptions
		if len(opts) > 0 {
			resolved = opts[0]
		}
		httpClient := &http.Client{
			Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
				rec := newResponseRecorder()
				switch {
				case req.Method == http.MethodGet && req.URL.Path == "/api/v1/volumes/00000065/info":
					rec.Header().Set("Content-Type", "application/json")
					_, _ = rec.Write([]byte(`{"volume_id":"00000065","attachment_generation":2,"path_plan_revision":5}`))
					return rec.Result(req), nil
				case req.Method == http.MethodGet && req.URL.Path == "/api/v1/debug/sbs-cluster/metrics":
					rec.Header().Set("Content-Type", "application/json")
					_, _ = rec.Write([]byte(`{"path_plan":{"total":1,"refresh":1},"top_priority_class":"refresh","top_priority_count":1}`))
					return rec.Result(req), nil
				case req.Method == http.MethodPost && req.URL.Path == "/api/v1/debug/discovery/volumes/00000065/runtime-feedback":
					if err := json.NewDecoder(req.Body).Decode(&posted); err != nil {
						t.Fatalf("decode posted feedback: %v", err)
					}
					rec.Header().Set("Content-Type", "application/json")
					_, _ = rec.Write([]byte(`{"volume_id":"00000065","runtime_path_needs_attention":true,"controller_needs_attention":true,"controller_recommended_actions":["refresh_gateway_path_plan","reopen_or_reapply_path_plan"]}`))
					return rec.Result(req), nil
				default:
					t.Fatalf("unexpected request: %s %s", req.Method, req.URL.Path)
					return nil, nil
				}
			}),
		}
		c := newGatewayClient(baseURL, resolved)
		c.httpClient = httpClient
		return c
	}
	defer func() {
		newGatewayClientFunc = origNewGatewayClientFunc
	}()

	output := captureStdout(t, func() {
		runStatus(client, []string{"--device", "3", "--gateway", "http://gateway.test", "--report-runtime-feedback", "--feedback-source-host", "host-a"})
	})
	if posted == nil {
		t.Fatalf("expected runtime feedback post")
	}
	if posted["needs_attention"].(bool) != true {
		t.Fatalf("unexpected posted payload: %+v", posted)
	}
	if posted["source_host"].(string) != "host-a" {
		t.Fatalf("unexpected posted source host: %+v", posted)
	}
	reasons := posted["attention_reasons"].([]any)
	if len(reasons) != 2 || reasons[0].(string) != "lane_unavailable" || reasons[1].(string) != "lane_down_preferred" {
		t.Fatalf("unexpected posted reasons: %+v", posted)
	}
	actions := posted["recommended_actions"].([]any)
	if len(actions) != 3 || actions[0].(string) != "refresh_gateway_path_plan" || actions[1].(string) != "reopen_or_reapply_path_plan" || actions[2].(string) != "reapply_latest_path_plan" {
		t.Fatalf("unexpected posted actions: %+v", posted)
	}
	noPath := posted["no_path"].(map[string]any)
	if noPath["state"].(string) != "queueing" || noPath["retry_mode"].(string) != "queue" || noPath["queued_reqs"].(float64) != 4 {
		t.Fatalf("unexpected posted no-path feedback: %+v", posted)
	}

	var out map[string]any
	if err := json.Unmarshal([]byte(output), &out); err != nil {
		t.Fatalf("unmarshal status output: %v", err)
	}
	report := out["runtime_feedback_report"].(map[string]any)
	if report["controller_needs_attention"].(bool) != true {
		t.Fatalf("unexpected runtime feedback report: %+v", report)
	}
}

func TestRunApplyVolumePathPlanPrintsRequestedAndAppliedRevision(t *testing.T) {
	client := &fakeNetlinkClient{
		status: netlinktlv.DeviceStatus{
			DeviceID:                7,
			PathCount:               3,
			DownMask:                0x4,
			DegradedMask:            0x2,
			AppliedPathPlanRevision: 11,
			ActiveLaneCount:         2,
			LaneRemapCount:          7,
			LastLaneRemappedLanes:   2,
			LastLaneRemapJiffies:    88,
			LastLaneRemapReason:     "path_state_change",
			Lanes: []netlinktlv.LaneStatus{
				{LaneID: 0, PreferredPathID: 0, FallbackPathID: 1, Readiness: 1},
				{LaneID: 1, PreferredPathID: 1, FallbackPathID: 0, Readiness: 2},
			},
		},
	}
	origNewGatewayClientFunc := newGatewayClientFunc
	newGatewayClientFunc = func(baseURL string, opts ...gatewayClientOptions) *gatewayClient {
		c := newGatewayClient(baseURL, opts...)
		c.httpClient = &http.Client{
			Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
				rec := newResponseRecorder()
				if req.URL.Path != "/api/v1/debug/discovery/volumes/00000065/path-plan" {
					rec.WriteHeader(http.StatusNotFound)
					return rec.Result(req), nil
				}
				_ = json.NewEncoder(rec).Encode(map[string]any{
					"path_plan_revision": 11,
					"active": []map[string]any{
						{"path_id": 0},
					},
					"standby": []map[string]any{
						{"path_id": 1},
					},
					"suppressed": []map[string]any{
						{"path_id": 2},
					},
				})
				return rec.Result(req), nil
			}),
		}
		return c
	}
	defer func() {
		newGatewayClientFunc = origNewGatewayClientFunc
	}()
	output := captureStdout(t, func() {
		runApplyVolumePathPlan(client, []string{"--device", "7", "--gateway", "http://gateway.test", "--volume", "00000065"})
	})
	if client.updatedReq.PathPlanRevision != 11 || client.updatedReq.DownMask != 0x4 || client.updatedReq.DegradedMask != 0x2 {
		t.Fatalf("unexpected update request: %+v", client.updatedReq)
	}
	var out map[string]any
	if err := json.Unmarshal([]byte(output), &out); err != nil {
		t.Fatalf("unmarshal apply output: %v", err)
	}
	if out["requested_path_plan_revision"].(float64) != 11 || out["applied_path_plan_revision"].(float64) != 11 || out["path_plan_revision_state"].(string) != "converged" {
		t.Fatalf("unexpected apply output: %+v", out)
	}
	actions := out["recommended_actions"].([]any)
	if len(actions) != 0 {
		t.Fatalf("unexpected recommended actions: %+v", actions)
	}
	lanes := out["lanes"].([]any)
	if len(lanes) != 2 || lanes[0].(map[string]any)["fallback_path_id"].(float64) != 1 || lanes[0].(map[string]any)["readiness"].(string) != "stable" || lanes[1].(map[string]any)["preferred_path_id"].(float64) != 1 || lanes[1].(map[string]any)["fallback_path_id"].(float64) != 0 || lanes[1].(map[string]any)["readiness"].(string) != "degraded_with_up_fallback" {
		t.Fatalf("unexpected lanes output: %+v", lanes)
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

type responseRecorder struct {
	header http.Header
	body   strings.Builder
	status int
}

type fakeNetlinkClient struct {
	status            netlinktlv.DeviceStatus
	updatedReq        netlinktlv.UpdatePathPlanRequest
	resizeReq         netlinktlv.ResizeDeviceRequest
	attachManifestReq netlinktlv.AttachManifestRequest
}

func (f *fakeNetlinkClient) Close() error { return nil }
func (f *fakeNetlinkClient) CreateDevice() (netlinktlv.CreateDeviceResponse, error) {
	return netlinktlv.CreateDeviceResponse{}, nil
}
func (f *fakeNetlinkClient) DestroyDevice(uint32) error                    { return nil }
func (f *fakeNetlinkClient) ConfigREST(netlinktlv.ConfigRESTRequest) error { return nil }
func (f *fakeNetlinkClient) AttachVolume(netlinktlv.AttachRequest) error   { return nil }
func (f *fakeNetlinkClient) DetachVolume(netlinktlv.DetachRequest) error   { return nil }
func (f *fakeNetlinkClient) AttachManifest(req netlinktlv.AttachManifestRequest) error {
	f.attachManifestReq = req
	return nil
}
func (f *fakeNetlinkClient) ReconfigureDataPaths(req netlinktlv.AttachManifestRequest) error {
	f.attachManifestReq = req
	return nil
}
func (f *fakeNetlinkClient) DetachLocal(netlinktlv.DetachLocalRequest) error { return nil }
func (f *fakeNetlinkClient) UpdatePathPlan(req netlinktlv.UpdatePathPlanRequest) error {
	f.updatedReq = req
	return nil
}
func (f *fakeNetlinkClient) ResizeDevice(req netlinktlv.ResizeDeviceRequest) error {
	f.resizeReq = req
	return nil
}
func (f *fakeNetlinkClient) GetStatus(uint32) (netlinktlv.DeviceStatus, error) { return f.status, nil }
func (f *fakeNetlinkClient) ListDevices() ([]netlinktlv.DeviceStatus, error) {
	return []netlinktlv.DeviceStatus{f.status}, nil
}

func captureStdout(t *testing.T, fn func()) string {
	t.Helper()
	orig := os.Stdout
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("os.Pipe: %v", err)
	}
	os.Stdout = w
	defer func() {
		os.Stdout = orig
	}()
	fn()
	_ = w.Close()
	out, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	return string(out)
}

func captureStderr(t *testing.T, fn func()) string {
	t.Helper()
	orig := os.Stderr
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("os.Pipe: %v", err)
	}
	os.Stderr = w
	defer func() {
		os.Stderr = orig
	}()
	fn()
	_ = w.Close()
	out, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	return string(out)
}

func writeTempFile(t *testing.T, contents string) string {
	t.Helper()
	f, err := os.CreateTemp(t.TempDir(), "namrbdctl-*.json")
	if err != nil {
		t.Fatalf("CreateTemp: %v", err)
	}
	if _, err := f.WriteString(contents); err != nil {
		_ = f.Close()
		t.Fatalf("WriteString: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	return f.Name()
}

func newResponseRecorder() *responseRecorder {
	return &responseRecorder{
		header: make(http.Header),
		status: http.StatusOK,
	}
}

type namrbdctlFakeMetadataRepository struct {
	gateways   []service.GatewayRecord
	gateway    service.GatewayRecord
	volumes    []service.VolumeSpec
	volume     service.VolumeSpec
	attachment service.AttachmentRecord
	status     service.VolumeStatusRecord
	generation uint64
}

func (f namrbdctlFakeMetadataRepository) EnsureVolume(context.Context, service.VolumeSpec) error {
	return nil
}
func (f namrbdctlFakeMetadataRepository) CreateVolume(_ context.Context, req service.VolumeCreateRequest) (service.VolumeSpec, error) {
	return service.VolumeSpec{
		ID:              service.HexVolumeID(101),
		Name:            req.Name,
		Prefix:          req.Name,
		SizeBytes:       req.SizeBytes,
		BlockSize:       req.BlockSize,
		ChunkSizeBytes:  req.ChunkSizeBytes,
		ExtentPageBytes: req.ExtentPageBytes,
		AccessMode:      req.AccessMode,
		State:           req.State,
	}, nil
}
func (f namrbdctlFakeMetadataRepository) UpdateVolume(_ context.Context, _ uint64, req service.VolumeUpdateRequest) (service.VolumeSpec, error) {
	volume := f.volume
	if req.Name != nil {
		volume.Name = *req.Name
	}
	if req.SizeBytes != nil {
		volume.SizeBytes = *req.SizeBytes
	}
	if req.BlockSize != nil {
		volume.BlockSize = *req.BlockSize
	}
	if req.ChunkSizeBytes != nil {
		volume.ChunkSizeBytes = *req.ChunkSizeBytes
	}
	if req.ExtentPageBytes != nil {
		volume.ExtentPageBytes = *req.ExtentPageBytes
	}
	if req.AccessMode != nil {
		volume.AccessMode = *req.AccessMode
	}
	if req.State != nil {
		volume.State = *req.State
	}
	return volume, nil
}
func (f namrbdctlFakeMetadataRepository) DeleteVolume(context.Context, uint64) error { return nil }
func (f namrbdctlFakeMetadataRepository) GetVolume(context.Context, uint64) (service.VolumeSpec, error) {
	return f.volume, nil
}
func (f namrbdctlFakeMetadataRepository) GetVolumeStatus(context.Context, uint64) (service.VolumeStatusRecord, error) {
	return f.status, nil
}
func (f namrbdctlFakeMetadataRepository) ListVolumes(context.Context) ([]service.VolumeSpec, error) {
	if len(f.volumes) > 0 {
		return f.volumes, nil
	}
	if f.volume.ID != 0 {
		return []service.VolumeSpec{f.volume}, nil
	}
	return nil, nil
}
func (f namrbdctlFakeMetadataRepository) SetVolumeState(_ context.Context, _ uint64, state service.VolumeLifecycleState) (service.VolumeSpec, error) {
	volume := f.volume
	volume.State = state
	return volume, nil
}
func (f namrbdctlFakeMetadataRepository) GetAttachment(context.Context, uint64) (service.AttachmentRecord, error) {
	return f.attachment, nil
}
func (f namrbdctlFakeMetadataRepository) GetGeneration(context.Context, uint64) (uint64, error) {
	return f.generation, nil
}
func (f namrbdctlFakeMetadataRepository) GetGateway(_ context.Context, gatewayID string) (service.GatewayRecord, error) {
	if f.gateway.GatewayID == gatewayID {
		return f.gateway, nil
	}
	for _, rec := range f.gateways {
		if rec.GatewayID == gatewayID {
			return rec, nil
		}
	}
	return service.GatewayRecord{}, nil
}
func (f namrbdctlFakeMetadataRepository) ListGateways(context.Context) ([]service.GatewayRecord, error) {
	if len(f.gateways) > 0 {
		return f.gateways, nil
	}
	if f.gateway.GatewayID != "" {
		return []service.GatewayRecord{f.gateway}, nil
	}
	return nil, nil
}
func (f namrbdctlFakeMetadataRepository) ListGatewayFleetPage(_ context.Context, opts metadata.GatewayFleetListOptions) (metadata.GatewayFleetPage, error) {
	records := append([]service.GatewayRecord(nil), f.gateways...)
	if opts.Limit > 0 && int64(len(records)) > opts.Limit {
		records = records[:opts.Limit]
	}
	return metadata.GatewayFleetPage{Records: records, Revision: 1}, nil
}
func (f namrbdctlFakeMetadataRepository) PutGateway(context.Context, service.GatewayRecord) error {
	return nil
}

func (r *responseRecorder) Header() http.Header {
	return r.header
}

func (r *responseRecorder) Write(b []byte) (int, error) {
	return r.body.Write(b)
}

func (r *responseRecorder) WriteHeader(statusCode int) {
	r.status = statusCode
}

func (r *responseRecorder) Result(req *http.Request) *http.Response {
	return &http.Response{
		StatusCode: r.status,
		Header:     r.header,
		Body:       io.NopCloser(strings.NewReader(r.body.String())),
		Request:    req,
	}
}

func TestParseSizeWithUnitAcceptsLowercase(t *testing.T) {
	for _, tc := range []struct {
		raw  string
		want uint64
	}{
		{"7m", 7 << 20},
		{"2g", 2 << 30},
		{"3t", 3 << 40},
	} {
		got, err := parseSizeWithUnit(tc.raw)
		if err != nil {
			t.Fatalf("parseSizeWithUnit(%q): %v", tc.raw, err)
		}
		if got != tc.want {
			t.Fatalf("parseSizeWithUnit(%q)=%d want=%d", tc.raw, got, tc.want)
		}
	}
}

func TestParseGeometrySizeUnitsAcceptLowercase(t *testing.T) {
	blockSize, err := parseBlockSizeK("4k")
	if err != nil || blockSize != 4096 {
		t.Fatalf("parseBlockSizeK(4k)=%d err=%v", blockSize, err)
	}
	chunkSize, err := parseChunkSizeWithUnit("64k")
	if err != nil || chunkSize != 64<<10 {
		t.Fatalf("parseChunkSizeWithUnit(64k)=%d err=%v", chunkSize, err)
	}
	allocationChunkSize, err := parseAllocationChunkSizeWithUnit("64k")
	if err != nil || allocationChunkSize != 64<<10 {
		t.Fatalf("parseAllocationChunkSizeWithUnit(64k)=%d err=%v", allocationChunkSize, err)
	}
	extentPageSize, err := parseExtentPageSizeWithUnit("4m")
	if err != nil || extentPageSize != 4<<20 {
		t.Fatalf("parseExtentPageSizeWithUnit(4m)=%d err=%v", extentPageSize, err)
	}
	allocationPageSize, err := parseAllocationPageSizeWithUnit("4m")
	if err != nil || allocationPageSize != 4<<20 {
		t.Fatalf("parseAllocationPageSizeWithUnit(4m)=%d err=%v", allocationPageSize, err)
	}
}
