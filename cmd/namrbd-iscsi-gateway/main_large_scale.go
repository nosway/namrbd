package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/nosway/namrbd/gateway/service"
	"github.com/nosway/namrbd/internal/adminclient"
	"github.com/nosway/namrbd/iscsi"
)

type largeScaleSBSRuntimePreparer struct {
	client           service.SBSClient
	gatewayID        string
	adminEndpoint    string
	logicalBlockSize uint64
}

func (p largeScaleSBSRuntimePreparer) Prepare(ctx context.Context, state iscsi.RegistryExportState) (iscsi.PreparedExportRuntime, error) {
	if strings.TrimSpace(state.ExportLeaseID) == "" || state.ExportEpoch == 0 {
		return iscsi.PreparedExportRuntime{}, fmt.Errorf("export %q has no receiver fencing lease/epoch", state.ExportID)
	}
	adapter, _, err := iscsi.OpenSBSBackendAdapter(ctx, p.client, iscsi.SBSAdapterConfig{
		ExportID: state.ExportID, VolumeID: state.VolumeID, TargetIQN: state.TargetIQN,
		LUNID: state.LUNID, LUNWWN: state.LUNWWN, ISCSIGatewayID: p.gatewayID,
		ActiveISCSIGatewayID: state.ActiveGatewayID, ExportLeaseID: state.ExportLeaseID,
		ExportEpoch: state.ExportEpoch, AttachmentID: state.ExportLeaseID, Generation: state.ExportEpoch,
		LogicalBlockSize: p.logicalBlockSize, RegistryLoaded: true,
		RegistryAdminEndpoint: p.adminEndpoint, ALUAMode: iscsi.ALUAModeImplicit,
		ALUAImplicitSupported: true, ALUAAccessState: registryALUAState(state, p.gatewayID),
		ALUAPreferred: state.ActiveGatewayID == p.gatewayID,
	})
	if err != nil {
		return iscsi.PreparedExportRuntime{}, err
	}
	writeState := state.WriteAdmissionState
	if writeState == "" {
		if state.ReadWriteAllowed && state.ActiveGatewayID == p.gatewayID {
			writeState = "read_write"
		} else {
			writeState = "standby"
		}
	}
	return iscsi.PreparedExportRuntime{
		State: state,
		Spec: iscsi.ExportRuntimeSpec{
			ExportID: state.ExportID, VolumeID: state.VolumeID, TargetIQN: state.TargetIQN,
			LUNID: state.LUNID, LUNWWN: state.LUNWWN, DeviceID: uint64(iscsi.StableSCSIDeviceID(state.LUNWWN)),
			SizeBytes: adapter.Size(), ActiveGatewayID: state.ActiveGatewayID,
			ExportLeaseID: state.ExportLeaseID, ExportEpoch: state.ExportEpoch,
			WriteAdmissionState: writeState, BackingStore: adapter,
		},
		Close: func() error {
			_, closeErr := adapter.Close(context.Background())
			return closeErr
		},
	}, nil
}

func registryALUAState(state iscsi.RegistryExportState, gatewayID string) string {
	if state.ActiveGatewayID == gatewayID && state.ReadWriteAllowed {
		return iscsi.ALUAAccessStateActiveOptimized
	}
	return iscsi.ALUAAccessStateStandby
}

type largeScaleServingGeneration struct {
	atomic *iscsi.AtomicSupervisorGeneration
	stop   func()
}

func (g *largeScaleServingGeneration) Apply(ctx context.Context, exports map[string]iscsi.PreparedExportRuntime) error {
	if g.stop != nil {
		g.stop()
	}
	return g.atomic.Apply(ctx, exports)
}

func runLargeScaleSBSBackend(stdout, stderr io.Writer, args sbsGatewayArgs) int {
	if strings.TrimSpace(args.sbsAdminEndpoint) == "" || strings.TrimSpace(args.sbsEndpoint) == "" {
		fmt.Fprintln(stderr, "large_scale iSCSI serving requires sbs-service and sbs-data endpoints")
		return 2
	}
	authCfg, err := gatewayAuthConfig(args.authMode, args.chapSecretRef, args.allowlist)
	if err != nil {
		fmt.Fprintf(stderr, "invalid auth config: %v\n", err)
		return 2
	}
	authCfg, err = iscsi.NormalizeAuthConfig(authCfg)
	if err != nil {
		fmt.Fprintf(stderr, "invalid auth config: %v\n", err)
		return 2
	}
	if err := authCfg.RuntimeAuthError(); err != nil {
		fmt.Fprintf(stderr, "iSCSI auth runtime unavailable: %v\n", err)
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
	if args.runFor > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, args.runFor)
		defer cancel()
	}
	admin, err := adminclient.Dial(ctx, args.sbsAdminEndpoint)
	if err != nil {
		fmt.Fprintf(stderr, "open sbs-service registry client: %v\n", err)
		return 2
	}
	defer admin.Close()
	fleetRegistry, err := startISCSIGatewayFleet(args.fleet)
	if err != nil {
		fmt.Fprintf(stderr, "start iSCSI gateway fleet membership: %v\n", err)
		return 2
	}
	defer stopISCSIGatewayFleet(fleetRegistry, stderr)

	maxExports := args.maxExportsPerProcess
	if maxExports == 0 {
		maxExports = iscsi.DefaultMaxExportsPerProcess
	}
	atomicGeneration, err := iscsi.NewAtomicSupervisorGeneration(maxExports)
	if err != nil {
		fmt.Fprintf(stderr, "create multi-export generation: %v\n", err)
		return 2
	}
	var servingCancel context.CancelFunc
	var servingDone chan struct{}
	stopServing := func() {
		if servingCancel == nil {
			return
		}
		servingCancel()
		<-servingDone
		servingCancel = nil
		servingDone = nil
	}
	defer stopServing()
	applier := &largeScaleServingGeneration{atomic: atomicGeneration, stop: stopServing}
	controller, err := iscsi.NewLiveReloadController(args.iscsiGatewayID, maxExports,
		grpcISCSIRegistryReloadSource{client: admin.Admin},
		largeScaleSBSRuntimePreparer{client: client, gatewayID: args.iscsiGatewayID, adminEndpoint: args.sbsAdminEndpoint, logicalBlockSize: iscsi.DefaultLogicalBlock},
		applier)
	if err != nil {
		fmt.Fprintf(stderr, "create registry live reload controller: %v\n", err)
		return 2
	}
	interval := args.reloadPollInterval
	if interval <= 0 {
		interval = 5 * time.Second
	}
	startServing := func() {
		supervisor := atomicGeneration.Current()
		if supervisor == nil || len(supervisor.Specs()) == 0 {
			return
		}
		serveCtx, cancel := context.WithCancel(ctx)
		servingCancel = cancel
		servingDone = make(chan struct{})
		go func(done chan struct{}) {
			defer close(done)
			err := iscsi.ServeGotgtMultiExport(serveCtx, iscsi.MultiExportServeOptions{
				Portal: args.portal, Supervisor: supervisor,
				AllowGotgtWildcardListen: args.allowWildcard,
			})
			if err != nil && !errors.Is(err, context.Canceled) && serveCtx.Err() == nil {
				fmt.Fprintf(stderr, "multi-export serving generation failed: %v\n", err)
			}
		}(servingDone)
	}
	if _, err := controller.ReloadOnce(ctx); err != nil {
		fmt.Fprintf(stderr, "load initial iSCSI registry generation: %v\n", err)
		return 2
	}
	stopObservability, err := startISCSIObservabilityServer(ctx, args.observabilityListen, iscsiObservabilityState{
		Backend:                  "sbs",
		RegistryLoaded:           true,
		FleetRegistered:          fleetRegistry != nil,
		FleetMembershipAuthority: fleetAuthority(fleetRegistry),
		FleetHealthAuthority:     fleetAuthority(fleetRegistry),
		LiveReloadSummary:        controller.Summary,
	}, stderr)
	if err != nil {
		fmt.Fprintf(stderr, "start observability listener: %v\n", err)
		return 2
	}
	defer stopObservability()
	startServing()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			if args.jsonOut {
				_ = writeJSONLine(stdout, controller.Summary())
			}
			return 0
		case <-ticker.C:
			result, reloadErr := controller.ReloadOnce(ctx)
			if reloadErr != nil {
				fmt.Fprintf(stderr, "reload iSCSI registry: %v\n", reloadErr)
				continue
			}
			if result.Outcome == iscsi.ReloadApply {
				startServing()
			}
		}
	}
}

func writeJSONLine(w io.Writer, value any) error {
	return json.NewEncoder(w).Encode(value)
}
