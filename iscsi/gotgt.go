package iscsi

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/gostor/gotgt/pkg/api"
	"github.com/gostor/gotgt/pkg/config"
	_ "github.com/gostor/gotgt/pkg/port/iscsit"
	"github.com/gostor/gotgt/pkg/scsi"
	_ "github.com/gostor/gotgt/pkg/scsi/backingstore"
	"github.com/gostor/gotgt/pkg/scsi/backingstore/remote"

	"github.com/nosway/namrbd/gateway/service"
)

type ServeOptions struct {
	MemoryOptions
	AllowGotgtWildcardListen bool
	RunFor                   time.Duration
	AuthConfig               AuthConfig
}

type SBSServeOptions struct {
	Portal                   string
	Config                   SBSAdapterConfig
	SummaryJSONPath          string
	OperationJSONLPath       string
	AllowGotgtWildcardListen bool
	RunFor                   time.Duration
	AuthConfig               AuthConfig
}

func ServeGotgtMemory(ctx context.Context, opts ServeOptions) (Summary, error) {
	memOpts, err := NormalizeMemoryOptions(opts.MemoryOptions)
	if err != nil {
		return Summary{}, err
	}
	authCfg, err := NormalizeAuthConfig(opts.AuthConfig)
	if err != nil {
		return Summary{}, err
	}
	if _, err := validateGotgtRemoteTargetConfig(gotgtRemoteTargetOptions{
		Portal:                   memOpts.Portal,
		TargetIQN:                memOpts.TargetIQN,
		SizeBytes:                memOpts.MemoryLUNBytes,
		AllowGotgtWildcardListen: opts.AllowGotgtWildcardListen,
	}); err != nil {
		return Summary{}, err
	}
	lun, err := NewMemoryLUN(memOpts.MemoryLUNBytes)
	if err != nil {
		return Summary{}, err
	}
	summary := summaryFrom(memOpts, lun, false, 0, 0, 0, nil, nil)
	summary.Entrypoint = "namrbd-iscsi-gateway"
	summary.CompatibilityClaim = "gotgt_memory_target_started"
	summary.TargetStackAccepted = true
	ApplyMemoryAuthSummary(&summary, authCfg)
	if authErr := authCfg.RuntimeAuthError(); authErr != nil {
		MarkMemoryAuthRuntimeError(&summary, authErr)
		if artifactErr := writeArtifacts(memOpts, nil, &summary); artifactErr != nil {
			return summary, artifactErr
		}
		return summary, authErr
	}
	err = serveGotgtRemoteTarget(ctx, gotgtRemoteTargetOptions{
		Portal:                   memOpts.Portal,
		TargetIQN:                memOpts.TargetIQN,
		LUNID:                    DefaultLUNID,
		DeviceID:                 uint64(StableSCSIDeviceID(LUNWWN(memOpts.ExportID))),
		SCSIIdentity:             SCSIIdentityForLUN(LUN{LUNID: DefaultLUNID, ExportID: memOpts.ExportID, LUNWWN: LUNWWN(memOpts.ExportID)}),
		SizeBytes:                memOpts.MemoryLUNBytes,
		BackingStore:             lun,
		AllowGotgtWildcardListen: opts.AllowGotgtWildcardListen,
		RunFor:                   opts.RunFor,
	})
	if err != nil {
		summary.Result = "error"
		summary.TargetStackAccepted = false
		summary.ErrorCount = 1
		summary.FirstError = err.Error()
		summary.LastError = err.Error()
	}
	if artifactErr := writeArtifacts(memOpts, nil, &summary); artifactErr != nil && err == nil {
		summary.Result = "error"
		summary.ErrorCount = 1
		summary.FirstError = artifactErr.Error()
		summary.LastError = artifactErr.Error()
		return summary, artifactErr
	}
	return summary, err
}

func ServeGotgtSBS(ctx context.Context, client service.SBSClient, opts SBSServeOptions) (SBSAdapterSummary, error) {
	if client == nil {
		return SBSAdapterSummary{}, fmt.Errorf("sbs client is required")
	}
	authCfg, err := NormalizeAuthConfig(opts.AuthConfig)
	if err != nil {
		return SBSAdapterSummary{}, err
	}
	if authErr := authCfg.RuntimeAuthError(); authErr != nil {
		summary := sbsServeSummaryFromConfig(opts)
		ApplySBSAuthSummary(&summary, authCfg)
		MarkSBSAuthRuntimeError(&summary, authErr)
		if artifactErr := WriteSBSAdapterOperationsFile(opts.OperationJSONLPath, nil); artifactErr != nil {
			return summary, artifactErr
		}
		if artifactErr := WriteSBSAdapterSummaryFile(opts.SummaryJSONPath, summary); artifactErr != nil {
			return summary, artifactErr
		}
		return summary, authErr
	}
	adapter, summary, err := OpenSBSBackendAdapter(ctx, client, opts.Config)
	if err != nil {
		return summary, err
	}
	decorateSBSServeSummary(&summary, opts, authCfg)

	serveErr := serveGotgtRemoteTarget(ctx, gotgtRemoteTargetOptions{
		Portal:                   opts.Portal,
		TargetIQN:                summary.TargetIQN,
		LUNID:                    summary.LUNID,
		DeviceID:                 uint64(summary.SBSDeviceID),
		SCSIIdentity:             SCSIIdentityForLUN(LUN{LUNID: summary.LUNID, ExportID: summary.ExportID, LUNWWN: summary.LUNWWN}),
		ALUATargetPortGroupID:    summary.ALUATargetPortGroupID,
		ALUAAccessState:          summary.ALUAAccessState,
		ALUAPreferred:            summary.ALUAPreferred,
		ALUAImplicitSupported:    summary.ALUAImplicitSupported,
		ALUAExplicitSupported:    summary.ALUAExplicitSupported,
		SizeBytes:                adapter.Size(),
		BackingStore:             adapter,
		AllowGotgtWildcardListen: opts.AllowGotgtWildcardListen,
		RunFor:                   opts.RunFor,
	})
	closeSummary, closeErr := adapter.Close(context.Background())
	summary = closeSummary
	decorateSBSServeSummary(&summary, opts, authCfg)
	if serveErr != nil {
		summary.Result = "error"
		summary.TargetStackAccepted = false
		summary.ErrorCount = 1
		summary.FirstError = serveErr.Error()
		summary.LastError = serveErr.Error()
	}
	if closeErr != nil && serveErr == nil {
		summary.Result = "error"
		summary.ErrorCount = 1
		summary.FirstError = closeErr.Error()
		summary.LastError = closeErr.Error()
	}
	if artifactErr := WriteSBSAdapterOperationsFile(opts.OperationJSONLPath, adapter.Operations()); artifactErr != nil && serveErr == nil && closeErr == nil {
		summary.Result = "error"
		summary.ErrorCount = 1
		summary.FirstError = artifactErr.Error()
		summary.LastError = artifactErr.Error()
		return summary, artifactErr
	}
	if artifactErr := WriteSBSAdapterSummaryFile(opts.SummaryJSONPath, summary); artifactErr != nil && serveErr == nil && closeErr == nil {
		summary.Result = "error"
		summary.ErrorCount = 1
		summary.FirstError = artifactErr.Error()
		summary.LastError = artifactErr.Error()
		return summary, artifactErr
	}
	if serveErr != nil {
		return summary, serveErr
	}
	return summary, closeErr
}

func decorateSBSServeSummary(summary *SBSAdapterSummary, opts SBSServeOptions, authCfg AuthConfig) {
	summary.Path = "q-slice-003-sbs-gotgt-target"
	summary.Entrypoint = "namrbd-iscsi-gateway"
	summary.PortalAddress = strings.TrimSpace(opts.Portal)
	summary.TargetStack = TargetStack
	summary.TargetStackVersion = TargetStackVersion
	summary.TargetStackAccepted = true
	summary.CompatibilityClaim = "gotgt_sbs_target_started"
	summary.GotgtWildcardListenRequiresOverride = true
	summary.SummaryJSONPath = opts.SummaryJSONPath
	summary.OperationJSONLPath = opts.OperationJSONLPath
	ApplySBSAuthSummary(summary, authCfg)
}

func sbsServeSummaryFromConfig(opts SBSServeOptions) SBSAdapterSummary {
	cfg := normalizeSBSAdapterConfig(opts.Config)
	summary := SBSAdapterSummary{
		Result:                              "ok",
		Path:                                "q-slice-003-sbs-gotgt-target",
		Entrypoint:                          "namrbd-iscsi-gateway",
		PortalAddress:                       strings.TrimSpace(opts.Portal),
		BackendMode:                         SBSBackendMode,
		BackendAdapter:                      SBSBackendAdapterName,
		TargetIQN:                           cfg.TargetIQN,
		LUNID:                               cfg.LUNID,
		LUNWWN:                              cfg.LUNWWN,
		ExportID:                            cfg.ExportID,
		VolumeID:                            cfg.VolumeID,
		ISCSIEdition:                        ISCSIEdition,
		ExportVolumeLimit:                   ISCSIExportVolumeLimit,
		SBSHostID:                           cfg.SBSHostID,
		SBSDeviceID:                         cfg.SBSDeviceID,
		ISCSIGatewayID:                      cfg.ISCSIGatewayID,
		ActiveISCSIGatewayID:                cfg.ActiveISCSIGatewayID,
		ExportLeaseID:                       cfg.ExportLeaseID,
		ExportEpoch:                         cfg.ExportEpoch,
		ALUAMode:                            cfg.ALUAMode,
		ALUAImplicitSupported:               cfg.ALUAImplicitSupported,
		ALUAExplicitSupported:               cfg.ALUAExplicitSupported,
		ALUATargetPortGroupID:               cfg.ALUATargetPortGroupID,
		ALUAAccessState:                     cfg.ALUAAccessState,
		ALUAPreferred:                       cfg.ALUAPreferred,
		WriterPolicy:                        "single_active_writer_session",
		HAFailoverMode:                      "manual_promote_demote_first",
		ActivePathIOAllowed:                 cfg.ISCSIGatewayID == cfg.ActiveISCSIGatewayID,
		ActivePathWriteAllowed:              cfg.ISCSIGatewayID == cfg.ActiveISCSIGatewayID,
		StandbyPathIOAllowed:                false,
		StandbyPathWriteAllowed:             false,
		AttachmentID:                        cfg.AttachmentID,
		Generation:                          cfg.Generation,
		SCSIStatus:                          "good",
		FUAClaim:                            "backend_write_ack",
		TargetStack:                         TargetStack,
		TargetStackVersion:                  TargetStackVersion,
		TargetStackAccepted:                 false,
		CompatibilityClaim:                  "gotgt_sbs_target_started",
		SummaryJSONPath:                     opts.SummaryJSONPath,
		OperationJSONLPath:                  opts.OperationJSONLPath,
		GotgtWildcardListenRequiresOverride: true,
	}
	return summary
}

type gotgtRemoteTargetOptions struct {
	Portal                   string
	TargetIQN                string
	LUNID                    uint64
	DeviceID                 uint64
	SCSIIdentity             SCSIIdentity
	ALUATargetPortGroupID    uint16
	ALUAAccessState          string
	ALUAPreferred            bool
	ALUAImplicitSupported    bool
	ALUAExplicitSupported    bool
	SizeBytes                uint64
	BackingStore             api.RemoteBackingStore
	AllowGotgtWildcardListen bool
	RunFor                   time.Duration
}

func serveGotgtRemoteTarget(ctx context.Context, opts gotgtRemoteTargetOptions) error {
	port, err := validateGotgtRemoteTargetConfig(opts)
	if err != nil {
		return err
	}
	if opts.BackingStore == nil {
		return fmt.Errorf("remote backing store is required")
	}
	tpgt := opts.ALUATargetPortGroupID
	if tpgt == 0 {
		tpgt = 1
	}
	tpgtText := fmt.Sprint(tpgt)
	cfg := &config.Config{
		Storages: []config.BackendStorage{},
		ISCSIPortals: []config.ISCSIPortalInfo{{
			ID:     0,
			Portal: opts.Portal,
		}},
		ISCSITargets: map[string]config.ISCSITarget{
			opts.TargetIQN: {
				TPGTs: map[string][]uint64{tpgtText: {0}},
				TPGTALUA: map[string]config.ALUATargetPortGroup{
					tpgtText: {
						AccessState:       ALUAAccessStateCode(opts.ALUAAccessState),
						Preferred:         opts.ALUAPreferred,
						ImplicitSupported: opts.ALUAImplicitSupported,
						ExplicitSupported: opts.ALUAExplicitSupported,
					},
				},
				LUNs: map[string]uint64{"0": opts.DeviceID},
			},
		},
	}
	remote.Size = opts.SizeBytes
	if err := scsi.InitSCSILUMapEx(&config.BackendStorage{
		DeviceID:         opts.DeviceID,
		Path:             "RemBs:" + opts.TargetIQN,
		Online:           true,
		ThinProvisioning: true,
		BlockShift:       9,
		SCSIVendorID:     opts.SCSIIdentity.Vendor,
		SCSIProductID:    opts.SCSIIdentity.Product,
		SCSIProductRev:   "001",
		SCSIID:           opts.SCSIIdentity.LUNWWN,
		SCSISerial:       opts.SCSIIdentity.Serial,
	}, opts.TargetIQN, opts.LUNID, opts.BackingStore); err != nil {
		return err
	}
	scsiTarget := scsi.NewSCSITargetService()
	targetDriver, err := scsi.NewTargetDriver("iscsi", scsiTarget)
	if err != nil {
		return err
	}
	if err := targetDriver.NewTarget(opts.TargetIQN, cfg); err != nil {
		return err
	}
	runErr := make(chan error, 1)
	go func() {
		runErr <- targetDriver.Run(port)
	}()
	waitCtx := ctx
	cancel := func() {}
	if opts.RunFor > 0 {
		waitCtx, cancel = context.WithTimeout(ctx, opts.RunFor)
	}
	defer cancel()
	select {
	case err := <-runErr:
		return err
	case <-waitCtx.Done():
		if err := targetDriver.Close(); err != nil {
			return err
		}
		if opts.RunFor == 0 {
			return ctx.Err()
		}
		return nil
	}
}

func validateGotgtRemoteTargetConfig(opts gotgtRemoteTargetOptions) (int, error) {
	host, portText, err := net.SplitHostPort(strings.TrimSpace(opts.Portal))
	if err != nil {
		return 0, fmt.Errorf("portal must be host:port: %w", err)
	}
	if strings.TrimSpace(host) == "" {
		return 0, fmt.Errorf("portal host is required")
	}
	port, err := strconv.Atoi(portText)
	if err != nil || port <= 0 || port > 65535 {
		return 0, fmt.Errorf("portal port must be 1..65535")
	}
	if strings.TrimSpace(opts.TargetIQN) == "" {
		return 0, fmt.Errorf("target IQN is required")
	}
	if opts.SizeBytes == 0 {
		return 0, fmt.Errorf("LUN size is required")
	}
	if !opts.AllowGotgtWildcardListen {
		return 0, fmt.Errorf("gotgt %s listens on wildcard :port; pass --allow-gotgt-wildcard-listen only in an isolated fixture environment", TargetStackVersion)
	}
	return port, nil
}
