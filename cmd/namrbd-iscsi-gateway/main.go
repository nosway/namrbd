package main

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/nosway/namrbd/gateway/sbsgrpc"
	"github.com/nosway/namrbd/gateway/service"
	"github.com/nosway/namrbd/internal/adminclient"
	"github.com/nosway/namrbd/internal/cliux"
	"github.com/nosway/namrbd/internal/depavail"
	"github.com/nosway/namrbd/iscsi"
	iscsifleet "github.com/nosway/namrbd/iscsi/fleet"
	adminv1 "github.com/nosway/namrbd/sbs/admin/v1"
	sbsv1 "github.com/nosway/namrbd/sbs/v1"
	namrbdversion "github.com/nosway/namrbd/version"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

func run(args []string, stdout, stderr io.Writer) int {
	args = cliux.RewriteDeprecatedFlags(args, []cliux.Alias{
		{Legacy: "sbs-endpoint", Canonical: "sbs-data-endpoint", DeprecatedIn: "post-1.0"},
		{Legacy: "sbs-admin-endpoint", Canonical: "sbs-service-endpoint", DeprecatedIn: "post-1.0"},
	}, stderr)
	args = cliux.RewriteCommandArgs(args, false, false)
	if len(args) >= 1 && (args[0] == "--version" || args[0] == "version") {
		fmt.Fprintln(stdout, namrbdversion.BuildSummary())
		return 0
	}
	fs := flag.NewFlagSet("namrbd-iscsi-gateway", flag.ContinueOnError)
	fs.SetOutput(stderr)
	configPath := fs.String("config", "", "service config file path (AA-IMPL-001E); when set, it supplies instance settings while target/LUN/export mappings stay registry-owned")
	backend := fs.String("backend", "memory", "backend mode: memory or sbs")
	portal := fs.String("portal", "", "explicit portal address")
	size := fs.String("memory-lun-size", "512MiB", "memory LUN size in bytes, KiB, MiB, or GiB")
	exportID := fs.String("export-id", iscsi.DefaultExportID, "export id")
	targetIQN := fs.String("target-iqn", "", "optional target IQN override")
	selfTest := fs.Bool("self-test", false, "run backend self-test and exit")
	serve := fs.Bool("serve", false, "start gotgt iSCSI target")
	runFor := fs.Duration("run-for", 0, "serve duration before clean shutdown")
	allowWildcard := fs.Bool("allow-gotgt-wildcard-listen", false, "allow gotgt v0.2.2 wildcard :port listener in isolated fixtures")
	summaryPath := fs.String("summary-json", "", "optional summary JSON artifact path")
	operationPath := fs.String("operation-jsonl", "", "optional operation JSONL artifact path")
	volumeID := fs.String("volume-id", "", "SBS volume id for --backend=sbs")
	sbsEndpoint := fs.String("sbs-data-endpoint", "", "sbs-data VolumeService gRPC endpoint host:port for --backend=sbs")
	sbsAdminEndpoint := fs.String("sbs-service-endpoint", "", "sbs-service AdminService gRPC endpoint host:port for registry-backed iSCSI config")
	registryRequired := fs.Bool("registry-required", false, "fail startup unless the iSCSI registry is loaded")
	lunID := fs.Uint64("lun-id", iscsi.DefaultLUNID, "iSCSI LUN id for registry-backed SBS export")
	sbsEndpointTLS := fs.Bool("sbs-endpoint-tls", false, "use TLS for --sbs-data-endpoint")
	sbsEndpointServerName := fs.String("sbs-endpoint-server-name", "", "TLS server name for --sbs-data-endpoint")
	sbsFixture := fs.Bool("sbs-fixture", false, "use an in-process fixture SBS client for --backend=sbs")
	sbsFixtureSize := fs.String("sbs-fixture-size", "8MiB", "fixture SBS volume size for --backend=sbs")
	iscsiGatewayID := fs.String("iscsi-gateway-id", "", "local iSCSI gateway id for SBS request context")
	activeGatewayID := fs.String("active-iscsi-gateway-id", "", "active iSCSI gateway id for SBS writer authority")
	exportLeaseID := fs.String("export-lease-id", "", "export lease id for SBS summary evidence")
	exportEpoch := fs.Uint64("export-epoch", 0, "export epoch for SBS summary evidence")
	aluaTargetPortGroupID := fs.Uint("alua-target-port-group-id", 0, "ALUA target port group id for the local iSCSI portal; defaults from registry portal order")
	aluaAccessState := fs.String("alua-access-state", "", "ALUA access state: active_optimized, standby, or unavailable")
	aluaPreferred := fs.Bool("alua-preferred", false, "mark the local ALUA target port group as preferred")
	attachmentID := fs.String("attachment-id", "", "SBS attachment id for writer context")
	generation := fs.Uint64("generation", 0, "SBS attachment generation for writer context")
	sbsHostID := fs.String("sbs-host-id", "", "SBS host id override for the iSCSI export")
	sbsDeviceID := fs.Uint64("sbs-device-id", 0, "optional SBS/SCSI device id for the iSCSI LUN; defaults from LUN WWN")
	sessionID := fs.String("session-id", "", "SBS session id override for request context")
	authMode := fs.String("auth-mode", iscsi.AuthModeNone, "iSCSI auth mode: none or chap")
	chapSecretRef := fs.String("chap-secret-ref", "", "CHAP secret reference for --auth-mode=chap; raw secrets are not accepted")
	allowedInitiatorIQNs := fs.String("allowed-initiator-iqns", "", "comma-separated initiator IQN allowlist for summary/admission evidence")
	jsonOut := fs.Bool("json", false, "emit final JSON summary to stdout")
	observabilityListen := fs.String("observability-listen", "", "optional HTTP listen address for /healthz, /readyz, and /metrics")
	var advertisePortals []string
	var fleetEtcdEndpoints []string
	var fleetEtcdRoot string
	var largeScale bool
	var reloadMode string
	var reloadPollInterval int
	maxExportsPerProcess := iscsi.DefaultMaxExportsPerProcess
	cliux.InstallStructuredUsage(fs, "namrbd-iscsi-gateway", func(name string) bool {
		_, hidden := labFlagsRejectedAtScale[name]
		return hidden
	})
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}

	// Without --config the process behaves exactly as before. Adoption is
	// additive so existing fixtures and deployments are unaffected.
	if strings.TrimSpace(*configPath) != "" {
		summary, err := applyISCSIServiceConfig(*configPath, iscsiConfigBinding{
			GatewayID:        iscsiGatewayID,
			Portal:           portal,
			AdvertisePortals: &advertisePortals,
			EtcdEndpoints:    &fleetEtcdEndpoints,
			EtcdRoot:         &fleetEtcdRoot,
			SBSEndpoint:      sbsEndpoint,
			SBSAdminEndpoint: sbsAdminEndpoint,
			SBSEndpointTLS:   sbsEndpointTLS,
			SBSServerName:    sbsEndpointServerName,
			RegistryRequired: registryRequired,
			LargeScale:       &largeScale,

			AuthMode:             authMode,
			CHAPSecretRef:        chapSecretRef,
			Allowlist:            allowedInitiatorIQNs,
			ReloadMode:           &reloadMode,
			ReloadPollInterval:   &reloadPollInterval,
			MaxExportsPerProcess: &maxExportsPerProcess,

			ObservabilityListen: observabilityListen,
		}, explicitlySetFlags(fs))
		// The summary is emitted either way. On the failure path it is the only
		// record of which config the process tried to start from.
		if blob, mErr := json.Marshal(summary); mErr == nil {
			fmt.Fprintf(stderr, "service config summary: %s\n", blob)
		}
		if err != nil {
			fmt.Fprintf(stderr, "service config: %v\n", err)
			return 2
		}
	}
	if len(advertisePortals) == 0 && strings.TrimSpace(*portal) != "" {
		advertisePortals = []string{strings.TrimSpace(*portal)}
	}
	if largeScale {
		*backend = "sbs"
		*serve = true
	}
	fleetArgs := iscsiFleetArgs{
		gatewayID:        *iscsiGatewayID,
		advertisePortals: advertisePortals,
		etcdEndpoints:    fleetEtcdEndpoints,
		etcdRoot:         fleetEtcdRoot,
	}

	switch *backend {
	case "memory":
		return runMemoryBackend(stdout, stderr, memoryGatewayArgs{
			portal:              *portal,
			size:                *size,
			exportID:            *exportID,
			targetIQN:           *targetIQN,
			selfTest:            *selfTest,
			serve:               *serve,
			runFor:              *runFor,
			allowWildcard:       *allowWildcard,
			summaryPath:         *summaryPath,
			operationPath:       *operationPath,
			authMode:            *authMode,
			chapSecretRef:       *chapSecretRef,
			allowlist:           *allowedInitiatorIQNs,
			jsonOut:             *jsonOut,
			observabilityListen: *observabilityListen,
			fleet:               fleetArgs,
		})
	case "sbs":
		return runSBSBackend(stdout, stderr, sbsGatewayArgs{
			portal:                *portal,
			exportID:              *exportID,
			targetIQN:             *targetIQN,
			selfTest:              *selfTest,
			serve:                 *serve,
			runFor:                *runFor,
			allowWildcard:         *allowWildcard,
			summaryPath:           *summaryPath,
			operationPath:         *operationPath,
			jsonOut:               *jsonOut,
			volumeID:              *volumeID,
			sbsEndpoint:           *sbsEndpoint,
			sbsAdminEndpoint:      *sbsAdminEndpoint,
			registryRequired:      *registryRequired,
			lunID:                 *lunID,
			sbsEndpointTLS:        *sbsEndpointTLS,
			sbsEndpointServerName: *sbsEndpointServerName,
			sbsFixture:            *sbsFixture,
			sbsFixtureSize:        *sbsFixtureSize,
			iscsiGatewayID:        *iscsiGatewayID,
			activeISCSIGatewayID:  *activeGatewayID,
			exportLeaseID:         *exportLeaseID,
			exportEpoch:           *exportEpoch,
			aluaTargetPortGroupID: uint64(*aluaTargetPortGroupID),
			aluaAccessState:       *aluaAccessState,
			aluaPreferred:         *aluaPreferred,
			attachmentID:          *attachmentID,
			generation:            *generation,
			sbsHostID:             *sbsHostID,
			sbsDeviceID:           *sbsDeviceID,
			sessionID:             *sessionID,
			authMode:              *authMode,
			chapSecretRef:         *chapSecretRef,
			allowlist:             *allowedInitiatorIQNs,
			observabilityListen:   *observabilityListen,
			fleet:                 fleetArgs,
			largeScale:            largeScale,
			reloadPollInterval:    time.Duration(reloadPollInterval) * time.Second,
			maxExportsPerProcess:  maxExportsPerProcess,
		})
	default:
		fmt.Fprintf(stderr, "backend %q is not supported; expected memory or sbs\n", *backend)
		return 2
	}
}

type memoryGatewayArgs struct {
	portal              string
	size                string
	exportID            string
	targetIQN           string
	selfTest            bool
	serve               bool
	runFor              time.Duration
	allowWildcard       bool
	summaryPath         string
	operationPath       string
	authMode            string
	chapSecretRef       string
	allowlist           string
	jsonOut             bool
	observabilityListen string
	fleet               iscsiFleetArgs
}

type iscsiFleetArgs struct {
	gatewayID        string
	advertisePortals []string
	etcdEndpoints    []string
	etcdRoot         string
}

func startISCSIGatewayFleet(args iscsiFleetArgs) (*iscsifleet.Registry, error) {
	if len(args.etcdEndpoints) == 0 {
		return nil, nil
	}
	return iscsifleet.Start(context.Background(), iscsifleet.Config{
		GatewayID:        args.gatewayID,
		AdvertisePortals: args.advertisePortals,
		EtcdEndpoints:    args.etcdEndpoints,
		EtcdRoot:         args.etcdRoot,
		BuildVersion:     namrbdversion.ProductVersion(),
	}, func(err error) {
		dependencyTracker.Report(depavail.DependencyEtcd, err)
	})
}

func markISCSIGatewayFleetError(registry *iscsifleet.Registry, cause error, stderr io.Writer) {
	if registry == nil || cause == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := registry.SetLifecycle(ctx, service.GatewayReadinessDegraded, service.GatewayDrainActive, cause); err != nil {
		fmt.Fprintf(stderr, "publish iSCSI gateway fleet error: %v\n", err)
	}
}

func stopISCSIGatewayFleet(registry *iscsifleet.Registry, stderr io.Writer) {
	if registry == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	if err := registry.SetLifecycle(ctx, service.GatewayReadinessDegraded, service.GatewayDrainDraining, nil); err != nil {
		fmt.Fprintf(stderr, "publish iSCSI gateway drain: %v\n", err)
	}
	cancel()
	registry.Close()
}

func fleetAuthority(registry *iscsifleet.Registry) string {
	if registry == nil {
		return ""
	}
	return iscsifleet.MembershipAuthority
}

func runMemoryBackend(stdout, stderr io.Writer, args memoryGatewayArgs) int {
	sizeBytes, err := iscsi.ParseSizeBytes(args.size)
	if err != nil {
		fmt.Fprintf(stderr, "invalid --memory-lun-size: %v\n", err)
		return 2
	}
	opts := iscsi.MemoryOptions{
		Portal:             args.portal,
		MemoryLUNBytes:     sizeBytes,
		ExportID:           args.exportID,
		TargetIQN:          args.targetIQN,
		SummaryJSONPath:    args.summaryPath,
		OperationJSONLPath: args.operationPath,
	}
	authCfg, err := gatewayAuthConfig(args.authMode, args.chapSecretRef, args.allowlist)
	if err != nil {
		fmt.Fprintf(stderr, "invalid auth config: %v\n", err)
		return 2
	}
	if args.serve {
		ctx, stop := serveContext()
		defer stop()
		fleetRegistry, err := startISCSIGatewayFleet(args.fleet)
		if err != nil {
			fmt.Fprintf(stderr, "start iSCSI gateway fleet membership: %v\n", err)
			return 2
		}
		defer stopISCSIGatewayFleet(fleetRegistry, stderr)
		stopObservability, err := startISCSIObservabilityServer(ctx, args.observabilityListen, iscsiObservabilityState{
			Backend:                  "memory",
			ExportID:                 args.exportID,
			TargetIQN:                args.targetIQN,
			AuthMode:                 args.authMode,
			RegistryLoaded:           false,
			FleetRegistered:          fleetRegistry != nil,
			FleetMembershipAuthority: fleetAuthority(fleetRegistry),
			FleetHealthAuthority:     fleetAuthority(fleetRegistry),
		}, stderr)
		if err != nil {
			fmt.Fprintf(stderr, "start observability listener: %v\n", err)
			return 2
		}
		defer stopObservability()
		summary, err := iscsi.ServeGotgtMemory(ctx, iscsi.ServeOptions{
			MemoryOptions:            opts,
			AllowGotgtWildcardListen: args.allowWildcard,
			RunFor:                   args.runFor,
			AuthConfig:               authCfg,
		})
		if args.jsonOut {
			if writeErr := iscsi.WriteSummary(stdout, summary); writeErr != nil {
				fmt.Fprintf(stderr, "write summary: %v\n", writeErr)
				return 1
			}
		}
		if err != nil {
			markISCSIGatewayFleetError(fleetRegistry, err, stderr)
			fmt.Fprintf(stderr, "serve failed: %v\n", err)
			return 1
		}
		return 0
	}
	if args.selfTest || !args.serve {
		summary, err := iscsi.RunMemorySelfTest(opts)
		summary.Entrypoint = "namrbd-iscsi-gateway --self-test"
		if args.jsonOut {
			if writeErr := iscsi.WriteSummary(stdout, summary); writeErr != nil {
				fmt.Fprintf(stderr, "write summary: %v\n", writeErr)
				return 1
			}
		}
		if err != nil {
			fmt.Fprintf(stderr, "self-test failed: %v\n", err)
			return 1
		}
		return 0
	}
	return 0
}

type sbsGatewayArgs struct {
	portal                   string
	exportID                 string
	targetIQN                string
	lunID                    uint64
	lunWWN                   string
	selfTest                 bool
	serve                    bool
	runFor                   time.Duration
	allowWildcard            bool
	summaryPath              string
	operationPath            string
	jsonOut                  bool
	volumeID                 string
	sbsEndpoint              string
	sbsAdminEndpoint         string
	registryRequired         bool
	sbsEndpointTLS           bool
	sbsEndpointServerName    string
	sbsFixture               bool
	sbsFixtureSize           string
	iscsiGatewayID           string
	activeISCSIGatewayID     string
	exportLeaseID            string
	exportEpoch              uint64
	aluaTargetPortGroupID    uint64
	aluaAccessState          string
	aluaPreferred            bool
	attachmentID             string
	generation               uint64
	sbsHostID                string
	sbsDeviceID              uint64
	sessionID                string
	authMode                 string
	chapSecretRef            string
	allowlist                string
	observabilityListen      string
	registryLoaded           bool
	registryRevision         uint64
	registryConfigGeneration uint64
	registryPortalID         string
	registryTargetFound      bool
	registryLUNFound         bool
	registryFailoverFound    bool
	fleet                    iscsiFleetArgs
	largeScale               bool
	reloadPollInterval       time.Duration
	maxExportsPerProcess     int
}

type iscsiGatewayAdminClient interface {
	GetISCSIRegistry(context.Context, *adminv1.GetISCSIRegistryRequest, ...grpc.CallOption) (*adminv1.GetISCSIRegistryResponse, error)
}

var newISCSIGatewayAdminClient = func(ctx context.Context, endpoint string) (iscsiGatewayAdminClient, func() error, error) {
	client, err := adminclient.Dial(ctx, endpoint)
	if err != nil {
		return nil, nil, err
	}
	return client.Admin, client.Close, nil
}

func runSBSBackend(stdout, stderr io.Writer, args sbsGatewayArgs) int {
	if args.largeScale {
		return runLargeScaleSBSBackend(stdout, stderr, args)
	}
	if args.selfTest || !args.serve {
		sizeBytes, err := iscsi.ParseSizeBytes(args.sbsFixtureSize)
		if err != nil {
			fmt.Fprintf(stderr, "invalid --sbs-fixture-size: %v\n", err)
			return 2
		}
		summary, err := iscsi.RunSBSAdapterSelfTest(iscsi.SBSAdapterSelfTestOptions{
			SizeBytes: sizeBytes,
		})
		summary.Entrypoint = "namrbd-iscsi-gateway --self-test"
		if args.summaryPath != "" {
			summary.SummaryJSONPath = args.summaryPath
			if writeErr := iscsi.WriteSBSAdapterSummaryFile(args.summaryPath, summary); writeErr != nil {
				fmt.Fprintf(stderr, "write summary artifact: %v\n", writeErr)
				return 1
			}
		}
		if args.jsonOut {
			if writeErr := iscsi.WriteSBSAdapterSummary(stdout, summary); writeErr != nil {
				fmt.Fprintf(stderr, "write summary: %v\n", writeErr)
				return 1
			}
		}
		if err != nil {
			fmt.Fprintf(stderr, "sbs self-test failed: %v\n", err)
			return 1
		}
		return 0
	}
	if args.registryRequired && strings.TrimSpace(args.sbsAdminEndpoint) == "" {
		fmt.Fprintln(stderr, "--registry-required requires --sbs-service-endpoint")
		return 2
	}
	if strings.TrimSpace(args.sbsAdminEndpoint) != "" {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		err := resolveSBSGatewayRegistry(ctx, &args)
		cancel()
		if err != nil {
			fmt.Fprintf(stderr, "load iSCSI registry: %v\n", err)
			return 2
		}
	}
	if strings.TrimSpace(args.volumeID) == "" {
		fmt.Fprintln(stderr, "--backend=sbs --serve requires --volume-id")
		return 2
	}
	if args.sbsDeviceID > uint64(^uint32(0)) {
		fmt.Fprintln(stderr, "--sbs-device-id must be 0..4294967295")
		return 2
	}
	if args.aluaTargetPortGroupID > 0xffff {
		fmt.Fprintln(stderr, "--alua-target-port-group-id must be 0..65535")
		return 2
	}
	if args.generation == 0 {
		fmt.Fprintln(stderr, "--backend=sbs --serve requires --generation >= 1")
		return 2
	}
	if strings.TrimSpace(args.activeISCSIGatewayID) == "" {
		fmt.Fprintln(stderr, "--backend=sbs --serve requires --active-iscsi-gateway-id")
		return 2
	}
	if strings.TrimSpace(args.attachmentID) == "" {
		fmt.Fprintln(stderr, "--backend=sbs --serve requires --attachment-id")
		return 2
	}
	authCfg, err := gatewayAuthConfig(args.authMode, args.chapSecretRef, args.allowlist)
	if err != nil {
		fmt.Fprintf(stderr, "invalid auth config: %v\n", err)
		return 2
	}
	client, closeClient, err := openSBSClient(args)
	if err != nil {
		fmt.Fprintf(stderr, "open SBS client: %v\n", err)
		return 2
	}
	defer closeClient()
	ctx, stop := serveContext()
	defer stop()
	fleetRegistry, err := startISCSIGatewayFleet(args.fleet)
	if err != nil {
		fmt.Fprintf(stderr, "start iSCSI gateway fleet membership: %v\n", err)
		return 2
	}
	defer stopISCSIGatewayFleet(fleetRegistry, stderr)
	stopObservability, err := startISCSIObservabilityServer(ctx, args.observabilityListen, iscsiObservabilityState{
		Backend:                  "sbs",
		ExportID:                 args.exportID,
		TargetIQN:                args.targetIQN,
		AuthMode:                 args.authMode,
		RegistryLoaded:           args.registryLoaded,
		VolumeID:                 args.volumeID,
		FleetRegistered:          fleetRegistry != nil,
		FleetMembershipAuthority: fleetAuthority(fleetRegistry),
		FleetHealthAuthority:     fleetAuthority(fleetRegistry),
	}, stderr)
	if err != nil {
		fmt.Fprintf(stderr, "start observability listener: %v\n", err)
		return 2
	}
	defer stopObservability()
	summary, err := iscsi.ServeGotgtSBS(ctx, client, iscsi.SBSServeOptions{
		Portal: args.portal,
		Config: iscsi.SBSAdapterConfig{
			ExportID:                 args.exportID,
			VolumeID:                 strings.TrimSpace(args.volumeID),
			TargetIQN:                args.targetIQN,
			LUNID:                    args.lunID,
			LUNWWN:                   firstNonEmpty(args.lunWWN, iscsi.LUNWWN(args.exportID)),
			ISCSIGatewayID:           args.iscsiGatewayID,
			ActiveISCSIGatewayID:     args.activeISCSIGatewayID,
			ExportLeaseID:            args.exportLeaseID,
			ExportEpoch:              args.exportEpoch,
			ALUAMode:                 iscsi.ALUAModeImplicit,
			ALUAImplicitSupported:    true,
			ALUAExplicitSupported:    false,
			ALUATargetPortGroupID:    uint16(args.aluaTargetPortGroupID),
			ALUAAccessState:          args.aluaAccessState,
			ALUAPreferred:            args.aluaPreferred,
			AttachmentID:             args.attachmentID,
			Generation:               args.generation,
			SBSHostID:                args.sbsHostID,
			SBSDeviceID:              uint32(args.sbsDeviceID),
			SessionID:                args.sessionID,
			LogicalBlockSize:         iscsi.DefaultLogicalBlock,
			OperationJSONLPath:       args.operationPath,
			RegistryLoaded:           args.registryLoaded,
			RegistryAdminEndpoint:    strings.TrimSpace(args.sbsAdminEndpoint),
			RegistryRevision:         args.registryRevision,
			RegistryConfigGeneration: args.registryConfigGeneration,
			RegistryPortalID:         args.registryPortalID,
			RegistryTargetFound:      args.registryTargetFound,
			RegistryLUNFound:         args.registryLUNFound,
			RegistryFailoverFound:    args.registryFailoverFound,
		},
		SummaryJSONPath:          args.summaryPath,
		OperationJSONLPath:       args.operationPath,
		AllowGotgtWildcardListen: args.allowWildcard,
		RunFor:                   args.runFor,
		AuthConfig:               authCfg,
	})
	if args.jsonOut {
		if writeErr := iscsi.WriteSBSAdapterSummary(stdout, summary); writeErr != nil {
			fmt.Fprintf(stderr, "write summary: %v\n", writeErr)
			return 1
		}
	}
	if err != nil {
		markISCSIGatewayFleetError(fleetRegistry, err, stderr)
		fmt.Fprintf(stderr, "serve failed: %v\n", err)
		return 1
	}
	return 0
}

type iscsiObservabilityState struct {
	Backend                  string
	ExportID                 string
	TargetIQN                string
	AuthMode                 string
	RegistryLoaded           bool
	VolumeID                 string
	FleetRegistered          bool
	FleetMembershipAuthority string
	FleetHealthAuthority     string
	LiveReloadSummary        func() iscsi.LiveReloadSummary
}

func startISCSIObservabilityServer(ctx context.Context, listen string, state iscsiObservabilityState, stderr io.Writer) (func(), error) {
	listen = strings.TrimSpace(listen)
	if listen == "" {
		return func() {}, nil
	}
	ln, err := net.Listen("tcp", listen)
	if err != nil {
		return nil, err
	}
	srv := &http.Server{
		Handler:           newISCSIObservabilityHandler(state),
		ReadHeaderTimeout: 5 * time.Second,
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		if serveErr := srv.Serve(ln); serveErr != nil && serveErr != http.ErrServerClosed {
			fmt.Fprintf(stderr, "observability listener stopped: %v\n", serveErr)
		}
	}()
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
	}()
	return func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
		<-done
	}, nil
}

func newISCSIObservabilityHandler(state iscsiObservabilityState) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		_, _ = fmt.Fprintln(w, "ok")
	})
	// /healthz stays process liveness. /readyz includes outcomes from the etcd
	// lease writes that publish this process's fleet membership.
	mux.Handle("/readyz", depavail.ReadinessHandler(dependencyTracker))
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		_, _ = fmt.Fprintln(w, "# HELP namrbd_iscsi_gateway_ready Whether the namrbd-iscsi-gateway process is ready.")
		_, _ = fmt.Fprintln(w, "# TYPE namrbd_iscsi_gateway_ready gauge")
		_, _ = fmt.Fprintln(w, "namrbd_iscsi_gateway_ready 1")
		_, _ = fmt.Fprintln(w, "# HELP namrbd_iscsi_gateway_runtime_info Static namrbd-iscsi-gateway runtime metadata.")
		_, _ = fmt.Fprintln(w, "# TYPE namrbd_iscsi_gateway_runtime_info gauge")
		_, _ = fmt.Fprintf(w, "namrbd_iscsi_gateway_runtime_info{backend=\"%s\",export_id=\"%s\",target_iqn=\"%s\",auth_mode=\"%s\",volume_id=\"%s\"} 1\n",
			prometheusLabelValue(state.Backend),
			prometheusLabelValue(state.ExportID),
			prometheusLabelValue(state.TargetIQN),
			prometheusLabelValue(state.AuthMode),
			prometheusLabelValue(state.VolumeID),
		)
		_, _ = fmt.Fprintln(w, "# HELP namrbd_iscsi_gateway_registry_loaded Whether SBS registry state was loaded before serving.")
		_, _ = fmt.Fprintln(w, "# TYPE namrbd_iscsi_gateway_registry_loaded gauge")
		_, _ = fmt.Fprintf(w, "namrbd_iscsi_gateway_registry_loaded %d\n", boolToMetric(state.RegistryLoaded))
		_, _ = fmt.Fprintln(w, "# HELP namrbd_iscsi_gateway_fleet_registered Whether this process holds an etcd fleet lease.")
		_, _ = fmt.Fprintln(w, "# TYPE namrbd_iscsi_gateway_fleet_registered gauge")
		_, _ = fmt.Fprintf(w, "namrbd_iscsi_gateway_fleet_registered{membership_authority=\"%s\",health_authority=\"%s\"} %d\n",
			prometheusLabelValue(state.FleetMembershipAuthority),
			prometheusLabelValue(state.FleetHealthAuthority),
			boolToMetric(state.FleetRegistered),
		)
		if state.LiveReloadSummary != nil {
			summary := state.LiveReloadSummary()
			_, _ = fmt.Fprintln(w, "# HELP namrbd_iscsi_gateway_registry_reload_total Successful registry generations applied by this process.")
			_, _ = fmt.Fprintln(w, "# TYPE namrbd_iscsi_gateway_registry_reload_total counter")
			_, _ = fmt.Fprintf(w, "namrbd_iscsi_gateway_registry_reload_total %d\n", summary.RegistryReloadCount)
			_, _ = fmt.Fprintln(w, "# HELP namrbd_iscsi_gateway_served_exports Number of exports in the current atomic generation.")
			_, _ = fmt.Fprintln(w, "# TYPE namrbd_iscsi_gateway_served_exports gauge")
			_, _ = fmt.Fprintf(w, "namrbd_iscsi_gateway_served_exports %d\n", summary.ServedExportCount)
			_, _ = fmt.Fprintln(w, "# HELP namrbd_iscsi_gateway_max_exports Configured per-process export cap.")
			_, _ = fmt.Fprintln(w, "# TYPE namrbd_iscsi_gateway_max_exports gauge")
			_, _ = fmt.Fprintf(w, "namrbd_iscsi_gateway_max_exports %d\n", summary.MaxExportsPerProcess)
			_, _ = fmt.Fprintln(w, "# HELP namrbd_iscsi_gateway_registry_revision Applied TiKV serving-registry revision.")
			_, _ = fmt.Fprintln(w, "# TYPE namrbd_iscsi_gateway_registry_revision gauge")
			_, _ = fmt.Fprintf(w, "namrbd_iscsi_gateway_registry_revision %d\n", summary.RegistryReloadRevision)
		}
	})
	mux.HandleFunc("/debug/registry", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		if state.LiveReloadSummary == nil {
			http.Error(w, "registry reload is not active", http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(state.LiveReloadSummary())
	})
	return mux
}

func boolToMetric(value bool) int {
	if value {
		return 1
	}
	return 0
}

func prometheusLabelValue(value string) string {
	value = strings.ReplaceAll(value, "\\", "\\\\")
	value = strings.ReplaceAll(value, "\n", "\\n")
	value = strings.ReplaceAll(value, "\"", "\\\"")
	return value
}

func resolveSBSGatewayRegistry(ctx context.Context, args *sbsGatewayArgs) error {
	if args == nil {
		return fmt.Errorf("sbs gateway args are nil")
	}
	targetIQN := strings.TrimSpace(args.targetIQN)
	if targetIQN == "" {
		return fmt.Errorf("--sbs-service-endpoint registry lookup requires --target-iqn")
	}
	client, closeClient, err := newISCSIGatewayAdminClient(ctx, args.sbsAdminEndpoint)
	if err != nil {
		return err
	}
	if closeClient != nil {
		defer func() { _ = closeClient() }()
	}
	resp, err := client.GetISCSIRegistry(ctx, &adminv1.GetISCSIRegistryRequest{})
	if err != nil {
		return err
	}
	target := findGatewayRegistryTarget(resp.GetTargets(), targetIQN)
	if target == nil {
		return fmt.Errorf("target %q not found in iSCSI registry", targetIQN)
	}
	lun := findGatewayRegistryLUN(resp.GetLuns(), targetIQN, args.lunID)
	if lun == nil {
		return fmt.Errorf("LUN %s#%d not found in iSCSI registry", targetIQN, args.lunID)
	}
	args.registryLoaded = true
	args.registryRevision = resp.GetRegistryRevision()
	args.registryConfigGeneration = resp.GetConfigGeneration()
	args.registryTargetFound = true
	args.registryLUNFound = true
	args.targetIQN = lun.GetTargetIqn()
	args.lunID = lun.GetLunId()
	args.lunWWN = lun.GetLunWwn()
	args.exportID = lun.GetExportId()
	args.volumeID = lun.GetVolumeId()
	var selectedPortal *adminv1.ISCSIPortalSummary
	if args.portal == "" {
		portal := selectGatewayRegistryPortal(resp.GetPortals(), target.GetPortalIds(), args.iscsiGatewayID)
		if portal != nil {
			selectedPortal = portal
			args.portal = portal.GetAddress()
			args.registryPortalID = portal.GetPortalId()
			if strings.TrimSpace(args.iscsiGatewayID) == "" {
				args.iscsiGatewayID = portal.GetGatewayId()
			}
		}
	} else if portal := selectGatewayRegistryPortal(resp.GetPortals(), target.GetPortalIds(), args.iscsiGatewayID); portal != nil {
		selectedPortal = portal
		args.registryPortalID = portal.GetPortalId()
	}
	if failover := findGatewayRegistryFailover(resp.GetFailovers(), lun.GetExportId()); failover != nil {
		args.registryFailoverFound = true
		if failover.GetActiveIscsiGatewayId() != "" {
			args.activeISCSIGatewayID = failover.GetActiveIscsiGatewayId()
		}
		if failover.GetExportLeaseId() != "" {
			args.exportLeaseID = failover.GetExportLeaseId()
		}
		if failover.GetExportEpoch() != 0 {
			args.exportEpoch = failover.GetExportEpoch()
		}
	}
	applyGatewayALUARegistryState(args, target.GetPortalIds(), selectedPortal)
	return nil
}

func applyGatewayALUARegistryState(args *sbsGatewayArgs, portalIDs []string, selectedPortal *adminv1.ISCSIPortalSummary) {
	if args == nil {
		return
	}
	if args.aluaTargetPortGroupID == 0 && selectedPortal != nil {
		if groupID := iscsi.ALUATargetPortGroupIDForIndex(gatewayRegistryPortalIndex(portalIDs, selectedPortal.GetPortalId())); groupID != 0 {
			args.aluaTargetPortGroupID = uint64(groupID)
		}
	}
	if strings.TrimSpace(args.aluaAccessState) == "" {
		args.aluaAccessState = iscsi.ALUAAccessStateStandby
		args.aluaPreferred = false
		if strings.TrimSpace(args.iscsiGatewayID) != "" && strings.TrimSpace(args.iscsiGatewayID) == strings.TrimSpace(args.activeISCSIGatewayID) {
			args.aluaAccessState = iscsi.ALUAAccessStateActiveOptimized
			args.aluaPreferred = true
		}
	}
}

func gatewayRegistryPortalIndex(portalIDs []string, portalID string) int {
	portalID = strings.TrimSpace(portalID)
	for idx, candidate := range portalIDs {
		if strings.TrimSpace(candidate) == portalID {
			return idx
		}
	}
	return 0
}

func findGatewayRegistryTarget(targets []*adminv1.ISCSITargetSummary, targetIQN string) *adminv1.ISCSITargetSummary {
	targetIQN = strings.TrimSpace(targetIQN)
	for _, target := range targets {
		if target.GetTargetIqn() == targetIQN {
			return target
		}
	}
	return nil
}

func findGatewayRegistryLUN(luns []*adminv1.ISCSILUNSummary, targetIQN string, lunID uint64) *adminv1.ISCSILUNSummary {
	targetIQN = strings.TrimSpace(targetIQN)
	for _, lun := range luns {
		if lun.GetTargetIqn() == targetIQN && lun.GetLunId() == lunID {
			return lun
		}
	}
	return nil
}

func selectGatewayRegistryPortal(portals []*adminv1.ISCSIPortalSummary, portalIDs []string, gatewayID string) *adminv1.ISCSIPortalSummary {
	portalByID := map[string]*adminv1.ISCSIPortalSummary{}
	for _, portal := range portals {
		portalByID[portal.GetPortalId()] = portal
	}
	gatewayID = strings.TrimSpace(gatewayID)
	if gatewayID != "" {
		for _, portalID := range portalIDs {
			portal := portalByID[portalID]
			if portal != nil && portal.GetGatewayId() == gatewayID {
				return portal
			}
		}
	}
	for _, portalID := range portalIDs {
		if portal := portalByID[portalID]; portal != nil {
			return portal
		}
	}
	return nil
}

func findGatewayRegistryFailover(failovers []*adminv1.ISCSIFailoverRuntimeSummary, exportID string) *adminv1.ISCSIFailoverRuntimeSummary {
	exportID = strings.TrimSpace(exportID)
	for _, failover := range failovers {
		if failover.GetExportId() == exportID {
			return failover
		}
	}
	return nil
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value != "" {
			return value
		}
	}
	return ""
}

func openSBSClient(args sbsGatewayArgs) (service.SBSClient, func(), error) {
	endpoint := strings.TrimSpace(args.sbsEndpoint)
	if args.sbsFixture == (endpoint != "") {
		return nil, nil, fmt.Errorf("requires exactly one of --sbs-fixture or --sbs-data-endpoint")
	}
	if args.sbsFixture {
		sizeBytes, err := iscsi.ParseSizeBytes(args.sbsFixtureSize)
		if err != nil {
			return nil, nil, fmt.Errorf("invalid --sbs-fixture-size: %w", err)
		}
		id, err := parseSBSFixtureVolumeID(args.volumeID)
		if err != nil {
			return nil, nil, err
		}
		client := service.NewInMemorySBSClient([]service.VolumeSpec{{
			ID:              service.HexVolumeID(id),
			Name:            "phase-q-sbs-gateway-fixture",
			Prefix:          "phase-q",
			SizeBytes:       sizeBytes,
			BlockSize:       service.DefaultBlockSize,
			ChunkSizeBytes:  service.DefaultAllocationChunkSize,
			ExtentPageBytes: service.DefaultAllocationPageSize,
			AccessMode:      service.VolumeAccessModeExclusive,
			State:           service.VolumeStateAvailable,
		}})
		return client, func() {}, nil
	}
	if _, _, err := net.SplitHostPort(endpoint); err != nil {
		return nil, nil, fmt.Errorf("--sbs-data-endpoint must be host:port: %w", err)
	}
	var dialCreds credentials.TransportCredentials
	if args.sbsEndpointTLS {
		serverName := strings.TrimSpace(args.sbsEndpointServerName)
		dialCreds = credentials.NewTLS(&tls.Config{
			MinVersion: tls.VersionTLS12,
			ServerName: serverName,
		})
	} else {
		dialCreds = insecure.NewCredentials()
	}
	conn, err := grpc.NewClient(endpoint, grpc.WithTransportCredentials(dialCreds))
	if err != nil {
		return nil, nil, err
	}
	return sbsgrpc.NewClient(sbsv1.NewVolumeServiceClient(conn)), func() { _ = conn.Close() }, nil
}

func serveContext() (context.Context, context.CancelFunc) {
	return signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
}

func gatewayAuthConfig(mode, chapSecretRef, allowlist string) (iscsi.AuthConfig, error) {
	return iscsi.NormalizeAuthConfig(iscsi.AuthConfig{
		Mode:                 mode,
		CHAPSecretRef:        chapSecretRef,
		AllowedInitiatorIQNs: splitCommaList(allowlist),
	})
}

func splitCommaList(raw string) []string {
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

func parseSBSFixtureVolumeID(raw string) (uint64, error) {
	raw = strings.TrimSpace(raw)
	if len(raw) != 8 {
		return 0, fmt.Errorf("--volume-id must be 8 lowercase hex chars for --sbs-fixture")
	}
	if raw != strings.ToLower(raw) {
		return 0, fmt.Errorf("--volume-id must be lowercase for --sbs-fixture")
	}
	id, err := strconv.ParseUint(raw, 16, 32)
	if err != nil {
		return 0, fmt.Errorf("invalid --volume-id: %w", err)
	}
	return id, nil
}
