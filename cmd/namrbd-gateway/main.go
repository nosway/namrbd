package main

import (
	"context"
	"crypto/tls"
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/nosway/namrbd/gateway/auth"
	"github.com/nosway/namrbd/gateway/dataplane"
	"github.com/nosway/namrbd/gateway/httpapi"
	"github.com/nosway/namrbd/gateway/metadata"
	"github.com/nosway/namrbd/gateway/sbsgrpc"
	"github.com/nosway/namrbd/gateway/service"
	"github.com/nosway/namrbd/gateway/store"
	"github.com/nosway/namrbd/internal/adminclient"
	adminv1 "github.com/nosway/namrbd/sbs/admin/v1"
	sbscluster "github.com/nosway/namrbd/sbs/cluster"
	clustercontrol "github.com/nosway/namrbd/sbs/cluster/control"
	clustermeta "github.com/nosway/namrbd/sbs/cluster/metadata"
	phaseperformance "github.com/nosway/namrbd/sbs/cluster/performance"
	phasesecurity "github.com/nosway/namrbd/sbs/cluster/security"
	"github.com/nosway/namrbd/sbs/local"
	sbsv1 "github.com/nosway/namrbd/sbs/v1"
	namrbdversion "github.com/nosway/namrbd/version"
	"github.com/nosway/namrbd/volumeid"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

const (
	envDataplaneTokenKey   = "NAMRBD_DP_TOKEN_SIGNING_KEY"
	envDataplaneSessionKey = "NAMRBD_DP_SESSION_KEY"

	defaultSBSAppendOnlyServiceWriteEffects = true
	defaultSBSInitialZeroMapEvidence        = true
	defaultSBSChunkIDAllocationCacheSize    = 256
)

var buildVersion = namrbdversion.Current

var openClusterMetadataPebble = clustermeta.OpenPebbleKV
var newSBSClusterClient = sbscluster.NewClient
var gatewayMaterializeHTTPDo = http.DefaultClient.Do

func main() {
	for _, arg := range os.Args[1:] {
		if arg == "--version" || arg == "version" {
			fmt.Println(buildVersion)
			return
		}
	}
	listenAddr := flag.String("listen", ":9701", "http listen address")
	dataListenAddr := flag.String("data-listen", ":9700", "binary dataplane listen address")
	advertiseControlAddr := flag.String("advertise-control-address", "", "control-plane address advertised in metadata/discovery (defaults to host from --listen, or 127.0.0.1 for wildcard listen)")
	advertiseDataAddr := flag.String("advertise-data-address", "", "dataplane address advertised in metadata/discovery (defaults to host from --data-listen, or 127.0.0.1 for wildcard listen)")
	dataDisable := flag.Bool("data-disable", false, "disable dataplane listener while still advertising dataplane endpoint metadata")
	dataplaneRequestTrace := flag.Bool("dataplane-request-trace", false, "lab only: emit structured dataplane request success trace events for kernel-origin workload capture")
	controlTLSEnable := flag.Bool("tls-enable", false, "enable TLS for control-plane HTTP listener")
	controlTLSCertFile := flag.String("tls-cert-file", "", "TLS certificate file for control-plane HTTP listener")
	controlTLSKeyFile := flag.String("tls-key-file", "", "TLS private key file for control-plane HTTP listener")
	controlTLSServerName := flag.String("tls-server-name", "", "advertised TLS server name for control-plane endpoint")
	metadataBackend := flag.String("metadata-backend", "memory", "metadata backend: memory|etcd")
	etcdEndpoints := flag.String("etcd-endpoints", "127.0.0.1:2379", "comma-separated etcd endpoints")
	etcdRoot := flag.String("etcd-root", "/namrbd", "etcd metadata root path")
	gatewayID := flag.String("gateway-id", defaultGatewayID(), "gateway id advertised in metadata")
	dataBackendMode := flag.String("data-backend-mode", "c6", "data backend mode: c6|sbs|sbs-cluster")
	storeBackend := flag.String("store-backend", "memory", "storage backend: memory (redis requires -tags legacy_redis)")
	sbsLocalPath := flag.String("sbs-local-path", "", "filesystem path for single-node local SBS when --data-backend-mode=sbs")
	sbsClusterReplicas := flag.String("sbs-cluster-replicas", "", "comma-separated replica definitions replica_id=path for legacy/dev sbs-cluster bootstrap; primary uses the admin-published view")
	sbsClusterMetadataBackend := flag.String("sbs-cluster-metadata-backend", "", "legacy/dev raw SBS cluster metadata backend: pebble; primary admin mode does not open raw SBS cluster metadata")
	sbsClusterMetadataPath := flag.String("sbs-cluster-metadata-path", "", "legacy/dev pebble SBS cluster metadata path; primary admin mode must not set this")
	sbsClusterMetadataRoot := flag.String("sbs-cluster-metadata-root", "sbs/cluster", "legacy/dev raw SBS cluster metadata root prefix")
	sbsAdminEndpoint := flag.String("sbs-admin-endpoint", "", "sbs-service admin/internal gRPC endpoint for primary sbs-cluster target, volume, placement, and write authority")
	sbsClusterBootstrapMetadata := flag.Bool("sbs-cluster-bootstrap-metadata", false, "legacy/dev only: bootstrap SBS cluster metadata from gateway metadata and --sbs-cluster-replicas")
	redisAddr := flag.String("redis-addr", "127.0.0.1:6379", "redis address (requires -tags legacy_redis)")
	volumeSpec := flag.String("volumes", "", "volume specs: volume_id,prefix,size_bytes;...")
	volumeCacheTTL := flag.Duration("volume-cache-ttl", sbscluster.DefaultVolumeCacheTTL, "TTL for local volume spec cache populated from admin volume lookup or legacy/dev raw metadata")
	sbsPlacementApplyTimeout := flag.Duration("sbs-placement-apply-timeout", sbscluster.DefaultPlacementApplyTimeout, "timeout for SBS cluster placement apply calls; 0 uses the default and <0 disables the timeout wrapper")
	sbsPageScopedWriteMetadata := flag.Bool("sbs-page-scoped-write-metadata", false, "prefer page-scoped SBS write metadata commits when the metadata authority supports them")
	sbsRangeLocalWriteState := flag.Bool("sbs-range-local-write-state", false, "lab only: commit SBS write state through range-local page state without advancing volume revision")
	sbsAsyncWriteEffects := flag.Bool("sbs-async-write-effects", false, "lab only: return after SBS write state commit and apply allocation/extent effects in the background")
	sbsUnsafeAppendOnlyWriteState := flag.Bool("sbs-unsafe-append-only-write-state", false, "lab only: commit SBS write idempotency with a generated append-only revision without advancing volume state")
	sbsAppendOnlyServiceWriteEffects := flag.Bool("sbs-append-only-service-write-effects", defaultSBSAppendOnlyServiceWriteEffects, "commit SBS write idempotency append-only and wait for service-owned metadata effects")
	sbsUnsafeAppendOnlyIntentlessCommit := flag.Bool("sbs-unsafe-append-only-intentless-commit", false, "lab only: skip append-only SBS write intent creation and synthesize pending idempotency during commit")
	sbsPayloadOnlyWrites := flag.Bool("sbs-payload-only-writes", false, "lab only: resolve placement and write replica payloads without SBS write intents, allocation commit, or metadata effects")
	sbsPromoteZeroPayloadWrites := flag.Bool("sbs-promote-zero-payload-writes", false, "lab only: promote all-zero SBS write payloads to zero-semantic allocation writes")
	sbsZeroAllocationReadFastPath := flag.Bool("sbs-zero-allocation-read-fast-path", false, "lab only: satisfy reads from resolved zero allocation pages without replica read RPCs")
	sbsInitialZeroMapEvidence := flag.Bool("sbs-initial-zero-map-evidence", defaultSBSInitialZeroMapEvidence, "advertise trusted all-zero allocation evidence for kernel local zero-map initialization")
	sbsUnsafeZeroNoopSkipIdempotency := flag.Bool("sbs-unsafe-zero-noop-skip-idempotency", false, "lab only: skip committed idempotency records for promoted zero no-op writes")
	sbsUnsafeZeroReplayFastPath := flag.Bool("sbs-unsafe-zero-replay-fast-path", false, "lab only: assume source-volume reads and promoted zero writes are zero in all-zero replay mode")
	sbsZeroEvidenceCacheTTL := flag.Duration("sbs-zero-evidence-cache-ttl", 0, "lab only: cache resolved zero allocation evidence for this duration; 0 disables")
	sbsOpenReuseTTL := flag.Duration("sbs-open-reuse-ttl", 0, "lab only: reuse a validated SBS open handle without metadata revalidation for this duration; 0 disables")
	httpZeroBase64WriteFastPath := flag.Bool("http-zero-base64-write-fast-path", false, "lab only: materialize canonical all-zero base64 write payloads without base64 decoding")
	httpZeroBase64ReadFastPath := flag.Bool("http-zero-base64-read-fast-path", false, "lab only: encode all-zero HTTP read responses with a cached canonical base64 string")
	readPathAttribution := flag.Bool("read-path-attribution", false, "lab only: emit detailed gateway/SBS read path attribution events")
	sbsQuorumEarlyReplicaWrites := flag.Bool("sbs-quorum-early-replica-writes", false, "return from non-strict SBS replica writes after any write quorum while remaining replica writes, including a slow primary, complete in the background")
	sbsReplicaFullChunkWriteParallelism := flag.Uint("sbs-replica-full-chunk-write-parallelism", 0, "lab only: maximum full-physical-chunk RPCs to issue concurrently per replica write; 0 keeps existing unbounded per-write fanout")
	sbsQuorumEarlyStagedFanoutDelay := flag.Duration("sbs-quorum-early-staged-fanout-delay", 0, "lab only: delay excess quorum-early replica writes by this duration, preferring a non-primary initial quorum when possible; 0 keeps immediate fanout")
	sbsQuorumEarlyBackgroundFanoutLimit := flag.Uint("sbs-quorum-early-background-fanout-limit", 0, "lab only: maximum delayed quorum-early background replica writes per gateway process; 0 leaves delayed background fanout unlimited")
	sbsChunkIDAllocationCacheSize := flag.Uint("sbs-chunk-id-allocation-cache-size", defaultSBSChunkIDAllocationCacheSize, "preallocate this many SBS chunk IDs per gateway volume cache refill; 0 disables")
	sbsParallelBeginPlan := flag.Bool("sbs-parallel-begin-plan", false, "lab only: overlap SBS begin-write volume/idempotency reads with write plan resolution")
	sbsWritePlanCacheTTL := flag.Duration("sbs-write-plan-cache-ttl", 0, "lab only: TTL for gateway-local SBS read-only write plan inputs; 0 disables")
	sbsBeginWriteVolumeStateCacheTTL := flag.Duration("sbs-begin-write-volume-state-cache-ttl", 0, "lab only: cache BeginWrite volume-state reads in append-only service effects mode; 0 disables")
	phaseOPerformanceAdmission := flag.Bool("phase-o-performance-admission", false, "lab only: enable Phase O gateway-local foreground I/O admission before dispatch")
	phaseOPerformancePolicyID := flag.String("phase-o-performance-policy-id", "gateway-local", "Phase O lab admission policy id reported in gateway responses")
	phaseOPerformancePolicyGeneration := flag.Uint64("phase-o-performance-policy-generation", 1, "Phase O lab admission policy generation reported in gateway responses")
	phaseOPerformanceCapScope := flag.String("phase-o-performance-cap-scope", phaseperformance.CapScopeLabOnly, "Phase O admission cap scope: lab_only|per_gateway|cluster_volume (cluster_volume requires --sbs-admin-endpoint)")
	phaseOPerformanceThrottleMode := flag.String("phase-o-performance-throttle-mode", phaseperformance.ThrottleModeWait, "Phase O lab admission throttle mode: wait|reject")
	phaseOPerformanceIOPSCap := flag.Uint64("phase-o-performance-iops-cap", 0, "Phase O lab admission foreground IOPS cap; 0 leaves IOPS uncapped")
	phaseOPerformanceBWCap := flag.Uint64("phase-o-performance-bandwidth-cap", 0, "Phase O lab admission foreground bandwidth cap in bytes/sec; 0 leaves bandwidth uncapped")
	phaseOPerformanceBurstIOPS := flag.Uint64("phase-o-performance-burst-iops", 0, "Phase O lab admission additional IOPS burst tokens")
	phaseOPerformanceBurstBytes := flag.Uint64("phase-o-performance-burst-bytes", 0, "Phase O lab admission additional byte burst tokens")
	phasePRepositoryFlags := registerPhasePRepositoryFlags(flag.CommandLine)
	gatewayLeaseTTL := flag.Duration("gateway-lease-ttl", 30*time.Second, "TTL for gateway liveness lease in etcd")
	pathPlanReconcileInterval := flag.Duration("path-plan-reconcile-interval", 5*time.Second, "background desired/observed gateway path-plan reconcile interval; <=0 disables the worker")
	chunkGCInterval := flag.Duration("chunk-gc-interval", 30*time.Second, "background allocation chunk (AC) garbage collection interval; <=0 disables the worker")
	chunkGCBatchSize := flag.Int("chunk-gc-batch-size", 256, "maximum allocation chunk (AC) garbage candidates to process per volume in one sweep")
	maxInflightRequests := flag.Uint("max-inflight-requests", 128, "dataplane inflight request limit")
	maxInflightBytes := flag.Uint64("max-inflight-bytes", 8*1024*1024, "dataplane inflight byte limit")
	maxIOSize := flag.Uint("max-io-size", uint(dataplane.DefaultMaxIOSize), "dataplane max io size in bytes")
	maxZeroLikeIOSize := flag.Uint("max-zero-like-io-size", uint(dataplane.DefaultMaxZeroLikeIOSize), "dataplane max DISCARD/WRITE_ZEROES logical range size in bytes")
	// Phase C3: dataplane token/session keys (env default; flag overrides env)
	dataplaneTokenKey := flag.String("dataplane-token-key", "", "dataplane token signing key (or "+envDataplaneTokenKey+")")
	dataplaneSessionKey := flag.String("dataplane-session-key", "", "dataplane session derivation key (or "+envDataplaneSessionKey+")")
	dataplaneTokenTTL := flag.Duration("dataplane-token-ttl", 5*time.Minute, "dataplane token TTL")
	dataplaneWireVersion := flag.Int("dataplane-wire-version", 1, "dataplane wire version: 1 or 2")
	flag.Parse()

	volumes, err := parseVolumes(*volumeSpec)
	if err != nil {
		log.Fatalf("invalid --volumes: %v", err)
	}
	if err := validateTLSOptions(*controlTLSEnable, *controlTLSCertFile, *controlTLSKeyFile); err != nil {
		log.Fatalf("invalid control-plane TLS options: %v", err)
	}
	performanceAdmissionCfg, err := httpapi.ValidatePerformanceAdmissionConfig(httpapi.PerformanceAdmissionConfig{
		Enabled:                           *phaseOPerformanceAdmission,
		PolicyID:                          *phaseOPerformancePolicyID,
		PolicyGeneration:                  *phaseOPerformancePolicyGeneration,
		CapScope:                          *phaseOPerformanceCapScope,
		ThrottleMode:                      *phaseOPerformanceThrottleMode,
		IOPSCap:                           *phaseOPerformanceIOPSCap,
		BandwidthCapBytesPerSec:           *phaseOPerformanceBWCap,
		BurstIOPS:                         *phaseOPerformanceBurstIOPS,
		BurstBytes:                        *phaseOPerformanceBurstBytes,
		GatewayID:                         *gatewayID,
		SharedBudgetLeaseClientConfigured: strings.TrimSpace(*sbsAdminEndpoint) != "",
	})
	if err != nil {
		log.Fatalf("invalid Phase O performance admission options: %v", err)
	}
	controlPort, err := parseListenPort(*listenAddr)
	if err != nil {
		log.Fatalf("invalid --listen: %v", err)
	}
	dataPort, err := parseListenPort(*dataListenAddr)
	if err != nil {
		log.Fatalf("invalid --data-listen: %v", err)
	}
	repoCfg := repositoryConfig{
		MetadataBackend:                     *metadataBackend,
		EtcdEndpoints:                       splitCSV(*etcdEndpoints),
		EtcdRoot:                            *etcdRoot,
		GatewayID:                           *gatewayID,
		DataBackendMode:                     *dataBackendMode,
		StoreBackend:                        *storeBackend,
		SBSLocalPath:                        *sbsLocalPath,
		SBSClusterReplicas:                  parseReplicaTargets(*sbsClusterReplicas),
		SBSClusterMetadataBackend:           strings.TrimSpace(*sbsClusterMetadataBackend),
		SBSClusterMetadataPath:              strings.TrimSpace(*sbsClusterMetadataPath),
		SBSClusterMetadataRoot:              strings.TrimSpace(*sbsClusterMetadataRoot),
		SBSAdminEndpoint:                    strings.TrimSpace(*sbsAdminEndpoint),
		SBSClusterBootstrapMetadata:         *sbsClusterBootstrapMetadata,
		RedisAddr:                           *redisAddr,
		VolumeCacheTTL:                      *volumeCacheTTL,
		SBSPlacementApplyTimeout:            *sbsPlacementApplyTimeout,
		SBSPageScopedWriteMetadata:          *sbsPageScopedWriteMetadata,
		SBSRangeLocalWriteState:             *sbsRangeLocalWriteState,
		SBSAsyncWriteEffects:                *sbsAsyncWriteEffects,
		SBSUnsafeAppendOnlyWriteState:       *sbsUnsafeAppendOnlyWriteState,
		SBSAppendOnlyServiceWriteEffects:    *sbsAppendOnlyServiceWriteEffects,
		SBSUnsafeAppendOnlyIntentlessCommit: *sbsUnsafeAppendOnlyIntentlessCommit,
		SBSPayloadOnlyWrites:                *sbsPayloadOnlyWrites,
		SBSPromoteZeroPayloadWrites:         *sbsPromoteZeroPayloadWrites,
		SBSZeroAllocationReadFastPath:       *sbsZeroAllocationReadFastPath,
		SBSUnsafeZeroNoopSkipIdempotency:    *sbsUnsafeZeroNoopSkipIdempotency,
		SBSUnsafeZeroReplayFastPath:         *sbsUnsafeZeroReplayFastPath,
		SBSZeroEvidenceCacheTTL:             *sbsZeroEvidenceCacheTTL,
		SBSOpenReuseTTL:                     *sbsOpenReuseTTL,
		SBSQuorumEarlyReplicaWrites:         *sbsQuorumEarlyReplicaWrites,
		SBSReplicaFullChunkWriteParallelism: int(*sbsReplicaFullChunkWriteParallelism),
		SBSQuorumEarlyStagedFanoutDelay:     *sbsQuorumEarlyStagedFanoutDelay,
		SBSQuorumEarlyBackgroundFanoutLimit: int(*sbsQuorumEarlyBackgroundFanoutLimit),
		SBSChunkIDAllocationCacheSize:       uint32(*sbsChunkIDAllocationCacheSize),
		SBSParallelBeginPlan:                *sbsParallelBeginPlan,
		SBSWritePlanCacheTTL:                *sbsWritePlanCacheTTL,
		SBSBeginWriteVolumeStateCacheTTL:    *sbsBeginWriteVolumeStateCacheTTL,
		GatewayLeaseTTL:                     *gatewayLeaseTTL,
		Volumes:                             toVolumeSpecs(volumes),
		ControlAddress:                      effectiveAdvertisedAddress(*advertiseControlAddr, *listenAddr),
		ControlPort:                         uint16(controlPort),
		ControlUseTLS:                       *controlTLSEnable,
		ControlServerName:                   *controlTLSServerName,
		DataAddress:                         effectiveAdvertisedAddress(*advertiseDataAddr, *dataListenAddr),
		DataPort:                            uint16(dataPort),
		PhaseP:                              phasePRepositoryFlags.config(),
	}
	metadataRepo, dataRepo, gcCollector, clusterDebug, backendDesc, cleanup, err := newRepositories(repoCfg)
	if err != nil {
		log.Fatalf("initialize repositories: %v", err)
	}
	defer func() { cleanup() }()
	svc := service.NewWithRepositoryOptions(metadataRepo, dataRepo, *gatewayID)
	tokenKey := *dataplaneTokenKey
	if tokenKey == "" {
		tokenKey = os.Getenv(envDataplaneTokenKey)
	}
	sessionKey := *dataplaneSessionKey
	if sessionKey == "" {
		sessionKey = os.Getenv(envDataplaneSessionKey)
	}
	var tokenIssuer auth.TokenIssuer
	if tokenKey != "" {
		var err error
		tokenIssuer, err = auth.NewTokenIssuer([]byte(tokenKey))
		if err != nil {
			log.Fatalf("dataplane token issuer: %v", err)
		}
	}
	useWireV2 := *dataplaneWireVersion == 2 && tokenIssuer != nil && sessionKey != ""
	var dpSrv *dataplane.Server
	if !*dataDisable {
		dpCfg := dataplane.Config{
			PathID:                 0,
			GatewayID:              *gatewayID,
			MaxIOSize:              uint32(*maxIOSize),
			MaxZeroLikeIOSize:      uint32(*maxZeroLikeIOSize),
			MaxSegments:            32,
			MaxInflightRequests:    uint32(*maxInflightRequests),
			MaxInflightBytes:       *maxInflightBytes,
			TraceCompletedRequests: *dataplaneRequestTrace,
		}
		if useWireV2 {
			dpCfg.UseWireV2 = true
			dpCfg.TokenVerifier = tokenIssuer
			dpCfg.SessionDerivationKey = []byte(sessionKey)
		}
		dpSrv = dataplane.New(svc, dpCfg)
	}
	var performanceBudgetLeaseClient httpapi.PerformanceBudgetLeaseClient
	if performanceAdmissionCfg.Enabled && performanceAdmissionCfg.CapScope == phaseperformance.CapScopeClusterVolume {
		var leaseCleanup func()
		performanceBudgetLeaseClient, leaseCleanup, err = newAdminEndpointPerformanceBudgetLeaseClient(strings.TrimSpace(*sbsAdminEndpoint))
		if err != nil {
			log.Fatalf("initialize Phase O shared budget lease client: %v", err)
		}
		prevCleanup := cleanup
		cleanup = func() {
			leaseCleanup()
			prevCleanup()
		}
	}
	var attachAdmission httpapi.AttachAdmissionFunc
	if phasePAttachAdmissionEnabled(repoCfg) {
		authority, admissionCleanup, err := newAdminEndpointPhasePKeyAccessLeaseClient(repoCfg)
		if err != nil {
			log.Fatalf("initialize Phase P attach key admission: %v", err)
		}
		attachAdmission = newPhasePAttachAdmission(authority, repoCfg.GatewayID, repoCfg.PhaseP.DataKeyID, repoCfg.PhaseP.KeyVersion)
		prevCleanup := cleanup
		cleanup = func() {
			admissionCleanup()
			prevCleanup()
		}
	}
	httpSrv := httpapi.New(svc, httpapi.Config{
		ControlAddress:    effectiveAdvertisedAddress(*advertiseControlAddr, *listenAddr),
		ControlPort:       uint16(controlPort),
		ControlUseTLS:     *controlTLSEnable,
		ControlServerName: *controlTLSServerName,
		DataAddress:       effectiveAdvertisedAddress(*advertiseDataAddr, *dataListenAddr),
		DataPort:          uint16(dataPort),
		GatewayID:         *gatewayID,
		RuntimeMode: func() string {
			if *dataBackendMode == "sbs-cluster" {
				if *sbsClusterBootstrapMetadata {
					return "legacy-dev-bootstrap"
				}
				return "primary-admin"
			}
			return ""
		}(),
		BackendDescription:             backendDesc,
		AdminEndpointConfigured:        strings.TrimSpace(*sbsAdminEndpoint) != "",
		StaticReplicaTargetsConfigured: len(parseReplicaTargets(*sbsClusterReplicas)) > 0,
		LegacyRawFallbackAllowed:       *sbsClusterBootstrapMetadata,
		MaxInflightRequests:            uint32(*maxInflightRequests),
		MaxInflightBytes:               *maxInflightBytes,
		MaxIOSize:                      uint32(*maxIOSize),
		MaxZeroLikeIOSize:              uint32(*maxZeroLikeIOSize),
		TokenIssuer: func() auth.TokenIssuer {
			if useWireV2 {
				return tokenIssuer
			}
			return nil
		}(),
		DataplaneSessionKey: func() string {
			if useWireV2 {
				return sessionKey
			}
			return ""
		}(),
		DataplaneTokenTTL:            *dataplaneTokenTTL,
		ClusterNodeDebug:             clusterDebug,
		MetadataRepo:                 metadataRepo,
		AttachAdmission:              attachAdmission,
		PerformanceAdmission:         performanceAdmissionCfg,
		PerformanceBudgetLeaseClient: performanceBudgetLeaseClient,
		HTTPZeroBase64WriteFastPath:  *httpZeroBase64WriteFastPath,
		HTTPZeroBase64ReadFastPath:   *httpZeroBase64ReadFastPath,
		InitialZeroMapEvidence:       *sbsInitialZeroMapEvidence,
		ReadPathAttribution:          *readPathAttribution,
		OnDetachSuccess: func(volumeID uint64) {
			if dpSrv != nil {
				dpSrv.RevokeSessionsForVolume(volumeID)
			}
		},
	})
	if gcCollector != nil && *chunkGCInterval > 0 {
		gcCtx, cancelGC := context.WithCancel(context.Background())
		prevCleanup := cleanup
		cleanup = func() {
			cancelGC()
			prevCleanup()
		}
		go func() {
			ticker := time.NewTicker(*chunkGCInterval)
			defer ticker.Stop()
			for {
				results, err := gcCollector.SweepAll(gcCtx, *chunkGCBatchSize)
				if err != nil {
					if gcCtx.Err() != nil {
						return
					}
					log.Printf("allocation chunk (AC) GC sweep failed: %v", err)
				} else {
					for _, result := range results {
						if result.CandidateCount == 0 {
							continue
						}
						log.Printf("allocation chunk (AC) GC volume=%s candidates=%d deleted=%d retained=%d",
							service.CanonicalVolumeID(uint64(result.VolumeID)), result.CandidateCount, result.DeletedCount, result.RetainedCount)
					}
				}
				select {
				case <-gcCtx.Done():
					return
				case <-ticker.C:
				}
			}
		}()
	}
	if *pathPlanReconcileInterval > 0 {
		reconcileCtx, cancelReconcile := context.WithCancel(context.Background())
		prevCleanup := cleanup
		cleanup = func() {
			cancelReconcile()
			prevCleanup()
		}
		go func() {
			ticker := time.NewTicker(*pathPlanReconcileInterval)
			defer ticker.Stop()
			for {
				updated, err := reconcileAllGatewayPathPlanStatuses(reconcileCtx, metadataRepo)
				if err != nil {
					if reconcileCtx.Err() != nil {
						return
					}
					log.Printf("gateway path-plan reconcile failed: %v", err)
				} else if updated > 0 {
					log.Printf("gateway path-plan reconcile updated_volumes=%d", updated)
				}
				select {
				case <-reconcileCtx.Done():
					return
				case <-ticker.C:
				}
			}
		}()
	}
	if !*dataDisable {
		dpLn, err := listenDataPlane(*dataListenAddr)
		if err != nil {
			log.Fatalf("listen dataplane %s: %v", *dataListenAddr, err)
		}
		defer dpLn.Close()
		go func() {
			log.Printf("starting NAMRBD dataplane on %s", *dataListenAddr)
			if err := dpSrv.Serve(dpLn); err != nil {
				log.Printf("dataplane server stopped: %v", err)
			}
		}()
	} else {
		log.Printf("dataplane listener disabled; advertising endpoint %s", *dataListenAddr)
	}

	log.Printf("starting NAMRBD gateway on %s (%s)", *listenAddr, backendDesc)
	if err := serveHTTP(*listenAddr, httpSrv.Handler(), *controlTLSEnable, *controlTLSCertFile, *controlTLSKeyFile); err != nil {
		log.Fatal(err)
	}
}

func parseListenPort(addr string) (int, error) {
	_, portRaw, err := net.SplitHostPort(addr)
	if err != nil {
		return 0, err
	}
	return strconv.Atoi(portRaw)
}

func advertisedAddress(addr string) string {
	host, _, err := net.SplitHostPort(addr)
	if err != nil || host == "" || host == "0.0.0.0" || host == "::" {
		return "127.0.0.1"
	}
	return host
}

func effectiveAdvertisedAddress(override, listenAddr string) string {
	override = strings.TrimSpace(override)
	if override != "" {
		return override
	}
	return advertisedAddress(listenAddr)
}

func validateTLSOptions(enabled bool, certFile, keyFile string) error {
	if !enabled {
		return nil
	}
	if certFile == "" || keyFile == "" {
		return errors.New("cert and key files are required when TLS is enabled")
	}
	return nil
}

func listenDataPlane(addr string) (net.Listener, error) {
	return net.Listen("tcp", addr)
}

func serveHTTP(addr string, handler http.Handler, tlsEnabled bool, certFile, keyFile string) error {
	srv := &http.Server{
		Addr:              addr,
		Handler:           handler,
		ReadHeaderTimeout: 5 * time.Second,
	}
	if !tlsEnabled {
		return srv.ListenAndServe()
	}
	srv.TLSConfig = &tls.Config{MinVersion: tls.VersionTLS12}
	return srv.ListenAndServeTLS(certFile, keyFile)
}

func newObjectStore(ctx context.Context, cfg repositoryConfig) (store.ObjectStore, string, func(), error) {
	closeFn := func() {}
	switch cfg.StoreBackend {
	case "memory", "":
		return store.NewMemoryStore(), "store=memory", closeFn, nil
	case "redis":
		return store.NewRedisStore(cfg.RedisAddr, 3*time.Second), "store=redis addr=" + cfg.RedisAddr, closeFn, nil
	default:
		return nil, "", nil, fmt.Errorf("invalid --store-backend %q: must be redis or memory", cfg.StoreBackend)
	}
}

type replicaTargetKind string

const (
	replicaTargetLocal replicaTargetKind = "local"
	replicaTargetGRPC  replicaTargetKind = "grpc"
)

type replicaTarget struct {
	TargetID  string
	Kind      replicaTargetKind
	Path      string
	Endpoint  clustermeta.SBSEndpoint
	AdminHTTP string
}

type repositoryConfig struct {
	MetadataBackend                     string
	EtcdEndpoints                       []string
	EtcdRoot                            string
	GatewayID                           string
	DataBackendMode                     string
	StoreBackend                        string
	SBSLocalPath                        string
	SBSClusterReplicas                  map[string]replicaTarget
	SBSClusterMetadataBackend           string
	SBSClusterMetadataPath              string
	SBSClusterMetadataRoot              string
	SBSAdminEndpoint                    string
	SBSClusterBootstrapMetadata         bool
	RedisAddr                           string
	VolumeCacheTTL                      time.Duration
	SBSPlacementApplyTimeout            time.Duration
	SBSPageScopedWriteMetadata          bool
	SBSRangeLocalWriteState             bool
	SBSAsyncWriteEffects                bool
	SBSUnsafeAppendOnlyWriteState       bool
	SBSAppendOnlyServiceWriteEffects    bool
	SBSUnsafeAppendOnlyIntentlessCommit bool
	SBSPayloadOnlyWrites                bool
	SBSPromoteZeroPayloadWrites         bool
	SBSZeroAllocationReadFastPath       bool
	SBSUnsafeZeroNoopSkipIdempotency    bool
	SBSUnsafeZeroReplayFastPath         bool
	SBSZeroEvidenceCacheTTL             time.Duration
	SBSOpenReuseTTL                     time.Duration
	SBSQuorumEarlyReplicaWrites         bool
	SBSReplicaFullChunkWriteParallelism int
	SBSQuorumEarlyStagedFanoutDelay     time.Duration
	SBSQuorumEarlyBackgroundFanoutLimit int
	SBSChunkIDAllocationCacheSize       uint32
	SBSParallelBeginPlan                bool
	SBSWritePlanCacheTTL                time.Duration
	SBSBeginWriteVolumeStateCacheTTL    time.Duration
	GatewayLeaseTTL                     time.Duration
	Volumes                             []service.VolumeSpec
	ControlAddress                      string
	ControlPort                         uint16
	ControlUseTLS                       bool
	ControlServerName                   string
	DataAddress                         string
	DataPort                            uint16
	PhaseP                              phasePRepositoryConfig
}

func validateLegacyClusterBootstrapConfig(cfg repositoryConfig) error {
	if !cfg.SBSClusterBootstrapMetadata {
		return nil
	}
	if strings.TrimSpace(cfg.DataBackendMode) != "sbs-cluster" {
		return fmt.Errorf("legacy/dev SBS cluster bootstrap requires --data-backend-mode=sbs-cluster")
	}
	if len(cfg.SBSClusterReplicas) == 0 {
		return fmt.Errorf("legacy/dev SBS cluster bootstrap requires explicit --sbs-cluster-replicas")
	}
	return nil
}

func validatePrimaryClusterRuntimeConfig(cfg repositoryConfig) error {
	if strings.TrimSpace(cfg.DataBackendMode) != "sbs-cluster" {
		return nil
	}
	if cfg.SBSClusterBootstrapMetadata {
		return nil
	}
	if strings.TrimSpace(cfg.SBSAdminEndpoint) == "" {
		return fmt.Errorf("primary sbs-cluster runtime requires --sbs-admin-endpoint; raw metadata fallbacks are legacy/dev only")
	}
	if strings.TrimSpace(cfg.MetadataBackend) != "etcd" {
		return fmt.Errorf("primary sbs-cluster runtime requires --metadata-backend=etcd; gateway/control-plane metadata ownership is not available via %q", cfg.MetadataBackend)
	}
	if strings.TrimSpace(cfg.SBSClusterMetadataPath) != "" {
		return fmt.Errorf("primary sbs-cluster runtime must not set --sbs-cluster-metadata-path; local pebble metadata paths are legacy/dev bootstrap only")
	}
	return nil
}

func newRepositories(cfg repositoryConfig) (service.MetadataRepository, service.DataRepository, *service.ChunkGarbageCollector, *clustercontrol.Controller, string, func(), error) {
	if err := validateLegacyClusterBootstrapConfig(cfg); err != nil {
		return nil, nil, nil, nil, "", nil, err
	}
	if err := validatePhasePRepositoryConfig(cfg); err != nil {
		return nil, nil, nil, nil, "", nil, err
	}
	if err := validatePrimaryClusterRuntimeConfig(cfg); err != nil {
		return nil, nil, nil, nil, "", nil, err
	}
	dataMode := strings.TrimSpace(cfg.DataBackendMode)
	if dataMode == "" {
		dataMode = "c6"
	}
	switch cfg.MetadataBackend {
	case "memory", "":
		repo := service.NewInMemoryMetadataRepository(cfg.Volumes)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := bootstrapMetadata(ctx, repo, cfg.Volumes, gatewayRecordFromConfig(cfg)); err != nil {
			return nil, nil, nil, nil, "", nil, err
		}
		metadataRepo, dataRepo, gcCollector, clusterDebug, dataDesc, cleanup, err := newDataRepository(context.Background(), repo, cfg)
		if err != nil {
			return nil, nil, nil, nil, "", nil, err
		}
		return metadataRepo, dataRepo, gcCollector, clusterDebug, "metadata=memory data=" + dataMode + " " + dataDesc, cleanup, nil
	case "etcd":
		client, err := metadata.NewEtcdClient(cfg.EtcdEndpoints, 5*time.Second)
		if err != nil {
			return nil, nil, nil, nil, "", nil, err
		}
		repo := metadata.NewEtcdRepository(client, cfg.EtcdRoot)
		wrappedRepo := service.NewCachedMetadataRepository(repo, cfg.VolumeCacheTTL)
		cleanup := func() {
			_ = client.Close()
		}
		lease, err := repo.StartGatewayLease(context.Background(), gatewayRecordFromConfig(cfg), cfg.GatewayLeaseTTL)
		if err != nil {
			cleanup()
			return nil, nil, nil, nil, "", nil, err
		}
		prevCleanup := cleanup
		cleanup = func() {
			lease.Close()
			prevCleanup()
		}
		if len(cfg.Volumes) > 0 {
			log.Printf("warning: --volumes is ignored when --metadata-backend=etcd; use namrbdctl or sbsctl volume commands instead")
		}
		metadataRepo, dataRepo, gcCollector, clusterDebug, dataDesc, dataCleanup, err := newDataRepository(context.Background(), wrappedRepo, cfg)
		if err != nil {
			cleanup()
			return nil, nil, nil, nil, "", nil, err
		}
		prevCleanup = cleanup
		cleanup = func() {
			dataCleanup()
			prevCleanup()
		}
		return metadataRepo, dataRepo, gcCollector, clusterDebug, "metadata=etcd endpoints=" + strings.Join(cfg.EtcdEndpoints, ",") + " data=" + dataMode + " " + dataDesc, cleanup, nil
	default:
		return nil, nil, nil, nil, "", nil, fmt.Errorf("invalid --metadata-backend %q: must be memory or etcd", cfg.MetadataBackend)
	}
}

func newDataRepository(ctx context.Context, meta service.MetadataRepository, cfg repositoryConfig) (service.MetadataRepository, service.DataRepository, *service.ChunkGarbageCollector, *clustercontrol.Controller, string, func(), error) {
	switch strings.TrimSpace(cfg.DataBackendMode) {
	case "", "c6":
		objects, dataDesc, closeObjects, err := newObjectStore(ctx, cfg)
		if err != nil {
			return nil, nil, nil, nil, "", nil, err
		}
		dataRepo := service.NewChunkExtentDataRepository(meta, objects)
		dataRepo, dataDesc, err = maybeWrapPhasePC6DataRepository(meta, objects, cfg, dataRepo, dataDesc)
		if err != nil {
			closeObjects()
			return nil, nil, nil, nil, "", nil, err
		}
		gcCollector := service.NewChunkGarbageCollector(meta, objects)
		return meta, dataRepo, gcCollector, nil, dataDesc, func() { closeObjects() }, nil
	case "sbs":
		if strings.TrimSpace(cfg.SBSLocalPath) == "" {
			return nil, nil, nil, nil, "", nil, fmt.Errorf("--sbs-local-path is required when --data-backend-mode=sbs")
		}
		client, err := local.Open(local.Config{Path: cfg.SBSLocalPath, BuildVersion: buildVersion})
		if err != nil {
			return nil, nil, nil, nil, "", nil, err
		}
		if err := bootstrapSBSVolumes(ctx, meta, client); err != nil {
			_ = client.Close()
			return nil, nil, nil, nil, "", nil, err
		}
		dataRepo := service.NewSBSDataRepositoryWithOpenReuseTTL(meta, client, cfg.GatewayID, cfg.SBSOpenReuseTTL, buildVersion)
		return meta, dataRepo, nil, nil, "sbs=local path=" + cfg.SBSLocalPath, func() { _ = client.Close() }, nil
	case "sbs-cluster":
		caps := runtimeClusterCapabilities{}
		clusterMetadataDesc := "admin-endpoint-only"
		cleanupFns := []func(){}
		if !useAdminSBSClusterWriteAuthority(cfg) {
			clusterHandle, desc, clusterCleanup, err := openRuntimeSBSClusterMetadataRepository(ctx, runtimeSBSClusterMetadataConfigFromRepositoryConfig(cfg))
			if err != nil {
				return nil, nil, nil, nil, "", nil, err
			}
			clusterMetadataDesc = desc
			cleanupFns = append(cleanupFns, clusterCleanup)
			caps, err = resolveRuntimeClusterCapabilities(clusterHandle, cfg)
			if err != nil {
				for _, cleanupFn := range cleanupFns {
					cleanupFn()
				}
				return nil, nil, nil, nil, "", nil, err
			}
		}
		if useAdminSBSClusterWriteAuthority(cfg) {
			placementApplyAdapter, cleanupPlacementApply, err := newAdminEndpointPlacementApplyAdapter(cfg)
			if err != nil {
				for _, cleanupFn := range cleanupFns {
					cleanupFn()
				}
				return nil, nil, nil, nil, "", nil, err
			}
			caps.placementApplyAdapter = placementApplyAdapter
			cleanupFns = append(cleanupFns, cleanupPlacementApply)
			writeSessionAdapter, cleanupWriteSession, err := newAdminEndpointWriteSessionCommitter(cfg)
			if err != nil {
				for _, cleanupFn := range cleanupFns {
					cleanupFn()
				}
				return nil, nil, nil, nil, "", nil, err
			}
			caps.writeSessionStore = writeSessionAdapter
			caps.writeStateCommitter = writeSessionAdapter
			cleanupFns = append(cleanupFns, cleanupWriteSession)
			chunkIDAllocator, cleanupChunkIDAllocator, err := newAdminEndpointChunkIDAllocator(cfg)
			if err != nil {
				for _, cleanupFn := range cleanupFns {
					cleanupFn()
				}
				return nil, nil, nil, nil, "", nil, err
			}
			caps.chunkIDAllocator = chunkIDAllocator
			cleanupFns = append(cleanupFns, cleanupChunkIDAllocator)
			placementResolver, cleanupPlacementResolver, err := newAdminEndpointPlacementResolver(cfg)
			if err != nil {
				for _, cleanupFn := range cleanupFns {
					cleanupFn()
				}
				return nil, nil, nil, nil, "", nil, err
			}
			caps.placementResolver = placementResolver
			caps.allocationResolver = placementResolver
			cleanupFns = append(cleanupFns, cleanupPlacementResolver)
			sourceSnapshotLister, cleanupSourceSnapshotLister, err := newAdminEndpointSourceSnapshotLister(cfg)
			if err != nil {
				for _, cleanupFn := range cleanupFns {
					cleanupFn()
				}
				return nil, nil, nil, nil, "", nil, err
			}
			caps.sourceSnapshotLister = sourceSnapshotLister
			cleanupFns = append(cleanupFns, cleanupSourceSnapshotLister)
			ecMetadata, cleanupECMetadata, err := clustercontrol.NewAdminEndpointECMetadataAdapter(cfg.SBSAdminEndpoint)
			if err != nil {
				for _, cleanupFn := range cleanupFns {
					cleanupFn()
				}
				return nil, nil, nil, nil, "", nil, err
			}
			caps.ecMetadata = ecMetadata
			cleanupFns = append(cleanupFns, cleanupECMetadata)
		}
		replicaTargets, replicaTargetSource, err := loadSBSClusterReplicaTargets(ctx, cfg, caps.rawReplicaTargets)
		if err != nil {
			for _, cleanupFn := range cleanupFns {
				cleanupFn()
			}
			return nil, nil, nil, nil, "", nil, err
		}
		replicaClients := make(map[string]service.SBSClient, len(replicaTargets))
		var rawVolumeLookup sbscluster.VolumeLookup
		if cfg.SBSClusterBootstrapMetadata && caps.rawVolumeSpecs != nil {
			rawVolumeLookup = newClusterVolumeLookup(caps.rawVolumeSpecs, cfg.VolumeCacheTTL)
		}
		volumeLookup := newPublishedClusterVolumeLookup(cfg, rawVolumeLookup, cfg.VolumeCacheTTL)
		for replicaID, target := range replicaTargets {
			client, closeFn, err := openReplicaClient(ctx, meta, target)
			if err != nil {
				for _, cleanupFn := range cleanupFns {
					cleanupFn()
				}
				return nil, nil, nil, nil, "", nil, err
			}
			if target.Kind == replicaTargetGRPC {
				client = newGatewayMaterializingSBSClient(client, targetAdminHTTPEndpoint(target), volumeLookup)
			}
			replicaClients[replicaID] = client
			cleanupFns = append(cleanupFns, closeFn)
		}
		if err := maybeBootstrapLegacyClusterMetadata(ctx, meta, cfg, caps.legacyBootstrap, replicaTargets); err != nil {
			for _, cleanupFn := range cleanupFns {
				cleanupFn()
			}
			return nil, nil, nil, nil, "", nil, err
		}
		clusterMeta := &clusterBackedMetadataRepository{
			MetadataRepository: meta,
			lookup:             volumeLookup,
			ensureTTL:          cfg.VolumeCacheTTL,
			ensured:            make(map[uint64]time.Time),
		}
		extentMappings, replicaSets := newPublishedVolumePlacementResolvers(cfg, caps.extentMappings, caps.replicaSets)
		var extentMappingResolver runtimeExtentMappingMetadataResolver = extentMappings
		var replicaSetResolver runtimeReplicaSetMetadataResolver = replicaSets
		var nodeMembershipResolver sbsclusterNodeMembershipResolver = newPublishedNodeMembershipResolver(cfg, caps.rawReplicaTargets)
		var allocationPageReader runtimeAllocationPageReader = newPublishedAllocationPageReader(cfg, caps.allocationPageReader)
		ecNodeMembershipResolver := nodeMembershipResolver
		ecAllocationPageReader := allocationPageReader
		var placementApplyStore runtimePlacementApplyMetadataStore
		var allocationPersistStore runtimeAllocationPersistStore
		var extentMappingNormalizeStore runtimeExtentMappingNormalizeStore
		var writePlanningStore runtimeWritePlanningMetadataStore
		if !useAdminSBSClusterWriteAuthority(cfg) {
			placementApplyStore = caps.placementApply
			allocationPersistStore = caps.allocationPersist
			extentMappingNormalizeStore = caps.extentMappingNormalize
			writePlanningStore = caps.writePlanning
		} else {
			extentMappingResolver = nil
			replicaSetResolver = nil
			nodeMembershipResolver = nil
			allocationPageReader = nil
		}
		var cloneDeltaCommitter sbsclusterCloneDeltaCommitter
		cloneDeltaCommitter, _ = caps.writeSessionStore.(sbsclusterCloneDeltaCommitter)
		var phasePKeyAccessLeaseIssuer phasesecurity.KeyAccessLeaseIssuer
		var phasePDataKeyUnwrapper phasesecurity.DataKeyUnwrapper
		if cfg.PhaseP.SBSClusterReplicatedPayloadEncryption && useAdminSBSClusterWriteAuthority(cfg) {
			authority, closeFn, err := newAdminEndpointPhasePKeyAccessLeaseClient(cfg)
			if err != nil {
				for _, cleanupFn := range cleanupFns {
					cleanupFn()
				}
				return nil, nil, nil, nil, "", nil, err
			}
			phasePKeyAccessLeaseIssuer = authority
			phasePDataKeyUnwrapper = authority
			cleanupFns = append(cleanupFns, closeFn)
		}
		client, err := newSBSClusterClient(sbscluster.Config{
			MetadataWriteSessionStore:           caps.writeSessionStore,
			MetadataCloneDeltaCommitter:         cloneDeltaCommitter,
			MetadataWriteStateCommitter:         caps.writeStateCommitter,
			MetadataChunkIDAllocator:            caps.chunkIDAllocator,
			MetadataWritePlanningStore:          writePlanningStore,
			MetadataPlacementApplyStore:         placementApplyStore,
			MetadataPlacementApplyAdapter:       caps.placementApplyAdapter,
			MetadataAllocationPersistStore:      allocationPersistStore,
			MetadataExtentMappingNormalizeStore: extentMappingNormalizeStore,
			MetadataExtentMappingResolver:       extentMappingResolver,
			MetadataReplicaSetResolver:          replicaSetResolver,
			MetadataPlacementResolver:           caps.placementResolver,
			MetadataNodeMembershipResolver:      nodeMembershipResolver,
			MetadataAllocationPageReader:        allocationPageReader,
			MetadataResolvedAllocationResolver:  caps.allocationResolver,
			MetadataSourceSnapshotLister:        caps.sourceSnapshotLister,
			MetadataECStore:                     newRuntimeECMetadataStore(caps.writeSessionStore, ecNodeMembershipResolver, ecAllocationPageReader, caps.ecMetadata),
			MetadataAllocationPageLister: func() sbsclusterAllocationPageLister {
				if cfg.SBSClusterBootstrapMetadata {
					return caps.allocationPageLister
				}
				return nil
			}(),
			VolumeLookup:                        volumeLookup,
			VolumeCacheTTL:                      cfg.VolumeCacheTTL,
			PreferPageScopedWriteMetadata:       cfg.SBSPageScopedWriteMetadata,
			PreferRangeLocalWriteState:          cfg.SBSRangeLocalWriteState,
			PreferAsyncWriteEffects:             cfg.SBSAsyncWriteEffects,
			PreferUnsafeAppendOnlyWriteState:    cfg.SBSUnsafeAppendOnlyWriteState,
			PreferAppendOnlyServiceWriteEffects: cfg.SBSAppendOnlyServiceWriteEffects,
			UnsafeAppendOnlyIntentlessCommit:    cfg.SBSUnsafeAppendOnlyIntentlessCommit,
			PreferPayloadOnlyWrites:             cfg.SBSPayloadOnlyWrites,
			PromoteZeroPayloadWrites:            cfg.SBSPromoteZeroPayloadWrites,
			ZeroAllocationReadFastPath:          cfg.SBSZeroAllocationReadFastPath,
			UnsafeZeroNoopSkipIdempotency:       cfg.SBSUnsafeZeroNoopSkipIdempotency,
			UnsafeZeroReplayFastPath:            cfg.SBSUnsafeZeroReplayFastPath,
			ZeroEvidenceCacheTTL:                cfg.SBSZeroEvidenceCacheTTL,
			PreferQuorumEarlyReplicaWrites:      cfg.SBSQuorumEarlyReplicaWrites,
			ReplicaFullChunkWriteParallelism:    cfg.SBSReplicaFullChunkWriteParallelism,
			QuorumEarlyStagedFanoutDelay:        cfg.SBSQuorumEarlyStagedFanoutDelay,
			QuorumEarlyBackgroundFanoutLimit:    cfg.SBSQuorumEarlyBackgroundFanoutLimit,
			ParallelBeginPlan:                   cfg.SBSParallelBeginPlan,
			PhasePReplicatedPayloadEncryption:   cfg.PhaseP.SBSClusterReplicatedPayloadEncryption,
			PhasePDataKeyID:                     cfg.PhaseP.DataKeyID,
			PhasePKeyVersion:                    cfg.PhaseP.KeyVersion,
			PhasePKeyAccessLeaseIssuer:          phasePKeyAccessLeaseIssuer,
			PhasePDataKeyUnwrapper:              phasePDataKeyUnwrapper,
			PhasePKeyAccessLeaseTTLSeconds:      300,
			ChunkIDAllocationCacheSize:          cfg.SBSChunkIDAllocationCacheSize,
			WritePlanCacheTTL:                   cfg.SBSWritePlanCacheTTL,
			BeginWriteVolumeStateCacheTTL:       cfg.SBSBeginWriteVolumeStateCacheTTL,
			MetadataPlacementApplyTimeout:       effectiveSBSPlacementApplyTimeout(cfg),
			ReplicaClients:                      replicaClients,
			ReplicaTargetAvailabilitySource:     newPublishedReplicaTargetAvailabilityProvider(cfg),
			FallbackReplicaTargetAvailabilitySource: func() sbscluster.ReplicaTargetAvailabilityProvider {
				if cfg.SBSClusterBootstrapMetadata {
					return newRawReplicaTargetAvailabilityProvider(caps.rawReplicaTargets)
				}
				return nil
			}(),
			GatewayID:     cfg.GatewayID,
			HostID:        cfg.ControlAddress,
			ClientVersion: buildVersion,
			SessionPrefix: "gateway-cluster",
		})
		if err != nil {
			for _, cleanupFn := range cleanupFns {
				cleanupFn()
			}
			return nil, nil, nil, nil, "", nil, err
		}
		dataRepo := service.NewSBSDataRepositoryWithOpenReuseTTLAndAllocationResolver(clusterMeta, client, cfg.GatewayID, cfg.SBSOpenReuseTTL, caps.allocationResolver, buildVersion)
		runtimeMode := "primary-admin"
		if cfg.SBSClusterBootstrapMetadata {
			runtimeMode = "legacy-dev-bootstrap"
		}
		desc := "sbs=cluster runtime_mode=" + runtimeMode + " replicas=" + strings.Join(sortedReplicaIDs(replicaTargets), ",") + " metadata=" + clusterMetadataDesc + " target_source=" + replicaTargetSource
		if cfg.SBSClusterBootstrapMetadata {
			desc += " bootstrap_metadata=legacy-dev"
		} else {
			desc += " admin_endpoint=" + cfg.SBSAdminEndpoint
		}
		if cfg.PhaseP.SBSClusterReplicatedPayloadEncryption {
			desc += " phase_p_sbs_cluster_replicated_payload_encryption=local_fixture"
			if strings.TrimSpace(cfg.PhaseP.DataKeyID) != "" {
				desc += " phase_p_data_key_id=" + strings.TrimSpace(cfg.PhaseP.DataKeyID)
			}
			if cfg.PhaseP.KeyVersion != 0 {
				desc += fmt.Sprintf(" phase_p_key_version=%d", cfg.PhaseP.KeyVersion)
			}
			if phasePKeyAccessLeaseIssuer != nil {
				desc += " phase_p_key_access_lease=admin_endpoint"
			}
			if phasePDataKeyUnwrapper != nil {
				desc += " phase_p_data_key_unwrap=admin_endpoint"
			}
		}
		return clusterMeta, dataRepo, nil, nil, desc, func() {
			for _, cleanupFn := range cleanupFns {
				cleanupFn()
			}
		}, nil
	default:
		return nil, nil, nil, nil, "", nil, fmt.Errorf("invalid --data-backend-mode %q: must be c6, sbs, or sbs-cluster", cfg.DataBackendMode)
	}
}

type clusterVolumeLookup struct {
	repo rawClusterVolumeSpecReader
	ttl  time.Duration

	mu    sync.Mutex
	cache map[uint64]clusterVolumeCacheEntry
}

type rawClusterVolumeSpecReader interface {
	GetVolumeSpec(ctx context.Context, volumeID string) (clustermeta.VolumeSpecRecord, error)
}

type rawReplicaTargetMetadataReader interface {
	ListNodeMemberships(ctx context.Context) ([]clustermeta.NodeMembershipRecord, error)
	GetNodeHealthDetail(ctx context.Context, nodeID string) (clustermeta.NodeHealthDetailRecord, error)
}

type runtimeAllocationPageReader interface {
	GetCompatibleAllocationPage(ctx context.Context, volumeID string, pageNo uint64, pageBytes, chunkSizeBytes uint32) (clustermeta.AllocationPageRecord, error)
}

type runtimeAllocationPageLister interface {
	ListCompatibleAllocationPages(ctx context.Context, volumeID string, pageBytes, chunkSizeBytes uint32) ([]clustermeta.AllocationPageRecord, error)
}

type runtimeAllocationPersistStore = clustermeta.AllocationPersistStore

type sbsclusterAllocationPageReader interface {
	GetCompatibleAllocationPage(ctx context.Context, volumeID string, pageNo uint64, pageBytes, chunkSizeBytes uint32) (clustermeta.AllocationPageRecord, error)
}

type sbsclusterAllocationPageLister interface {
	ListCompatibleAllocationPages(ctx context.Context, volumeID string, pageBytes, chunkSizeBytes uint32) ([]clustermeta.AllocationPageRecord, error)
}

type sbsclusterCloneDeltaCommitter interface {
	CommitCloneDeltaAllocationPages(ctx context.Context, cloneID string, pages []clustermeta.AllocationPageRecord) error
}

type runtimeVolumeStateStore interface {
	GetVolumeState(ctx context.Context, volumeID string) (clustermeta.VolumeState, error)
	PutVolumeState(ctx context.Context, state clustermeta.VolumeState) error
}

type runtimeWriteSessionMetadataStore interface {
	runtimeVolumeStateStore
	runtimeControlRecordMetadataStore
}

type runtimeExtentMappingMetadataResolver interface {
	ListExtentMappings(ctx context.Context, volumeID string) ([]clustermeta.ExtentMappingRecord, error)
}

type runtimeExtentMappingNormalizeStore = clustermeta.ExtentMappingNormalizeStore

type runtimeReplicaSetMetadataResolver interface {
	ListReplicaSets(ctx context.Context, volumeID string) ([]clustermeta.ReplicaSetState, error)
}

type runtimeIdempotencyMetadataStore interface {
	GetIdempotencyRecord(ctx context.Context, volumeID, idempotencyKey string) (clustermeta.IdempotencyRecord, error)
	PutIdempotencyRecord(ctx context.Context, rec clustermeta.IdempotencyRecord) error
}

type runtimeMutationLifecycleMetadataStore interface {
	GetMutationOperation(ctx context.Context, volumeID, operationID string) (clustermeta.MutationOperationRecord, error)
	PutMutationOperation(ctx context.Context, rec clustermeta.MutationOperationRecord) error
	PutWriteIntent(ctx context.Context, record clustermeta.IdempotencyRecord, operation clustermeta.MutationOperationRecord) error
}

type runtimeControlRecordMetadataStore interface {
	runtimeIdempotencyMetadataStore
	runtimeMutationLifecycleMetadataStore
}

type runtimeWriteStateCommitter interface {
	CommitWriteState(ctx context.Context, req clustermeta.CommitWriteStateRequest) (clustermeta.VolumeState, clustermeta.IdempotencyRecord, error)
}

type runtimePageScopedWriteMetadataCommitter interface {
	CommitPageScopedWriteMetadata(ctx context.Context, req clustermeta.CommitWriteMetadataRequest) (clustermeta.VolumeState, clustermeta.IdempotencyRecord, error)
}

type runtimeRangeLocalWriteStateCommitter interface {
	CommitRangeLocalWriteState(ctx context.Context, req clustermeta.CommitWriteMetadataRequest) (clustermeta.VolumeState, clustermeta.IdempotencyRecord, error)
}

type runtimeAppendOnlyServiceWriteEffectsCommitter interface {
	CommitAppendOnlyWriteStateAndQueueEffects(ctx context.Context, req clustermeta.CommitWriteMetadataRequest) (clustermeta.VolumeState, clustermeta.IdempotencyRecord, error)
}

type runtimeCloneDeltaCommitter interface {
	CommitCloneDeltaAllocationPages(ctx context.Context, cloneID string, pages []clustermeta.AllocationPageRecord) error
}

type runtimeAppendOnlyWriteStateCommitter interface {
	CommitAppendOnlyWriteState(ctx context.Context, req clustermeta.CommitWriteStateRequest) (clustermeta.VolumeState, clustermeta.IdempotencyRecord, error)
}

type runtimeWriteSessionAuthority interface {
	runtimeWriteSessionMetadataStore
	runtimeWriteStateCommitter
	runtimePageScopedWriteMetadataCommitter
	runtimeRangeLocalWriteStateCommitter
	runtimeAppendOnlyServiceWriteEffectsCommitter
}

type runtimeWriteSessionRepositoryAuthority interface {
	runtimeWriteSessionAuthority
	runtimeAppendOnlyWriteStateCommitter
	runtimeCloneDeltaCommitter
}

type runtimeChunkIDSequenceStore interface {
	GetNextChunkID(ctx context.Context, volumeID string) (uint64, error)
	PutNextChunkID(ctx context.Context, volumeID string, nextID uint64) error
}

type runtimeChunkIDAllocator interface {
	AllocateChunkIDs(ctx context.Context, volumeID string, count uint32) (uint64, error)
}

type runtimePlacementResolver interface {
	ResolveExtentPlacements(ctx context.Context, volumeID string, offsetBytes, lengthBytes uint64) ([]clustermeta.ResolvedExtentPlacement, error)
}

type runtimeResolvedAllocationResolver interface {
	ResolveAllocationPages(ctx context.Context, volumeID string, offsetBytes, lengthBytes uint64, pageBytes, chunkSizeBytes uint32) ([]clustermeta.ResolvedAllocationPage, error)
}

type runtimeSourceSnapshotLister interface {
	ListSnapshotRecords(ctx context.Context, sourceVolumeID string, includeDeleted bool) ([]clustermeta.SnapshotRecord, error)
}

type runtimePlacementPlanningResolver interface {
	runtimePlacementResolver
	runtimeResolvedAllocationResolver
}

type runtimePlacementApplyMetadataStore = clustermeta.PlacementApplyAuthority
type runtimePlacementApplyAdapter = clustercontrol.PlacementApplyAdapter
type runtimeWritePlanningMetadataStore = clustermeta.WritePlanningAuthority

type runtimeClusterCapabilities struct {
	writeSessionStore      runtimeWriteSessionMetadataStore
	writeStateCommitter    runtimeWriteStateCommitter
	placementApply         runtimePlacementApplyMetadataStore
	placementApplyAdapter  runtimePlacementApplyAdapter
	placementResolver      runtimePlacementResolver
	allocationResolver     runtimeResolvedAllocationResolver
	sourceSnapshotLister   runtimeSourceSnapshotLister
	ecMetadata             clustercontrol.ECMetadataAdapter
	chunkIDAllocator       runtimeChunkIDAllocator
	writePlanning          runtimeWritePlanningMetadataStore
	allocationPersist      runtimeAllocationPersistStore
	extentMappingNormalize runtimeExtentMappingNormalizeStore
	extentMappings         runtimeExtentMappingMetadataResolver
	replicaSets            runtimeReplicaSetMetadataResolver
	allocationPageReader   runtimeAllocationPageReader
	allocationPageLister   runtimeAllocationPageLister
	rawReplicaTargets      rawReplicaTargetMetadataReader
	rawVolumeSpecs         rawClusterVolumeSpecReader
	legacyBootstrap        legacyClusterBootstrapWriter
}

func resolveRuntimeClusterCapabilities(handle runtimeClusterMetadataHandle, cfg repositoryConfig) (runtimeClusterCapabilities, error) {
	caps := runtimeClusterCapabilities{}
	useAdminWriteAuthority := useAdminSBSClusterWriteAuthority(cfg)
	caps.writeSessionStore, _ = handle.(runtimeWriteSessionMetadataStore)
	if caps.writeSessionStore == nil {
		return caps, fmt.Errorf("runtime cluster metadata repository does not support write session store operations")
	}
	if authority, ok := handle.(runtimeWriteSessionRepositoryAuthority); ok {
		caps.writeSessionStore = clustercontrol.NewServiceBackedWriteSessionAdapter(clustercontrol.NewRepositoryBackedWriteSessionInternalServiceWithInlineEffects(authority))
		caps.writeStateCommitter = caps.writeSessionStore.(runtimeWriteStateCommitter)
	}
	caps.rawReplicaTargets, _ = handle.(rawReplicaTargetMetadataReader)
	if caps.rawReplicaTargets == nil && !useAdminWriteAuthority {
		return caps, fmt.Errorf("runtime cluster metadata repository does not support node membership reads")
	}
	caps.extentMappings, _ = handle.(runtimeExtentMappingMetadataResolver)
	if caps.extentMappings == nil {
		return caps, fmt.Errorf("runtime cluster metadata repository does not support extent mapping reads")
	}
	caps.replicaSets, _ = handle.(runtimeReplicaSetMetadataResolver)
	if caps.replicaSets == nil {
		return caps, fmt.Errorf("runtime cluster metadata repository does not support replica set reads")
	}
	caps.allocationPersist, _ = handle.(runtimeAllocationPersistStore)
	if caps.allocationPersist == nil && !useAdminWriteAuthority {
		return caps, fmt.Errorf("runtime cluster metadata repository does not support allocation persist store operations")
	}
	caps.extentMappingNormalize, _ = handle.(runtimeExtentMappingNormalizeStore)
	if caps.extentMappingNormalize == nil && !useAdminWriteAuthority {
		return caps, fmt.Errorf("runtime cluster metadata repository does not support extent mapping normalize store operations")
	}
	if !useAdminWriteAuthority {
		caps.placementApply, _ = handle.(runtimePlacementApplyMetadataStore)
	}
	if caps.placementApply == nil && caps.allocationPersist != nil && caps.extentMappingNormalize != nil {
		caps.placementApply = splitRuntimePlacementApplyStore{
			allocationPersist:      caps.allocationPersist,
			extentMappingNormalize: caps.extentMappingNormalize,
		}
	}
	if caps.placementApply != nil {
		caps.placementApplyAdapter = clustercontrol.NewRepositoryBackedPlacementApplyAdapter(caps.placementApply)
	}
	if !useAdminWriteAuthority {
		caps.writePlanning, _ = handle.(runtimeWritePlanningMetadataStore)
	}
	if caps.writePlanning == nil && !useAdminWriteAuthority {
		if sequenceStore, ok := handle.(runtimeChunkIDSequenceStore); ok {
			caps.chunkIDAllocator = clustercontrol.NewServiceBackedChunkIDAllocatorAdapter(clustercontrol.NewRepositoryBackedChunkIDAllocatorInternalService(sequenceStore))
			caps.writePlanning = splitRuntimeWritePlanningStore{
				sequenceStore:  sequenceStore,
				placementApply: caps.placementApply,
			}
		}
	} else {
		caps.chunkIDAllocator = clustercontrol.NewServiceBackedChunkIDAllocatorAdapter(clustercontrol.NewRepositoryBackedChunkIDAllocatorInternalService(caps.writePlanning))
	}
	if caps.chunkIDAllocator == nil {
		if allocator, ok := handle.(runtimeChunkIDAllocator); ok {
			caps.chunkIDAllocator = allocator
		}
	}
	caps.allocationPageReader, _ = handle.(runtimeAllocationPageReader)
	caps.allocationPageLister, _ = handle.(runtimeAllocationPageLister)
	caps.sourceSnapshotLister, _ = handle.(runtimeSourceSnapshotLister)
	caps.rawVolumeSpecs, _ = handle.(rawClusterVolumeSpecReader)
	caps.legacyBootstrap, _ = handle.(legacyClusterBootstrapWriter)
	if caps.legacyBootstrap == nil && cfg.SBSClusterBootstrapMetadata {
		return caps, fmt.Errorf("runtime cluster metadata repository does not support legacy bootstrap writes")
	}
	return caps, nil
}

func useAdminSBSClusterWriteAuthority(cfg repositoryConfig) bool {
	return !cfg.SBSClusterBootstrapMetadata && strings.TrimSpace(cfg.SBSAdminEndpoint) != ""
}

type legacyClusterBootstrapWriter interface {
	GetNodeMembership(ctx context.Context, nodeID string) (clustermeta.NodeMembershipRecord, error)
	PutNodeMembership(ctx context.Context, rec clustermeta.NodeMembershipRecord) error
	PutVolumeSpec(ctx context.Context, rec clustermeta.VolumeSpecRecord) error
	GetVolumeState(ctx context.Context, volumeID string) (clustermeta.VolumeState, error)
	PutVolumeState(ctx context.Context, state clustermeta.VolumeState) error
	GetReplicaSet(ctx context.Context, volumeID, replicaSetID string) (clustermeta.ReplicaSetState, error)
	PutReplicaSet(ctx context.Context, state clustermeta.ReplicaSetState) error
	GetExtentMapping(ctx context.Context, volumeID string, extentID uint64) (clustermeta.ExtentMappingRecord, error)
	PutExtentMapping(ctx context.Context, rec clustermeta.ExtentMappingRecord) error
}

type splitRuntimePlacementApplyStore struct {
	allocationPersist      runtimeAllocationPersistStore
	extentMappingNormalize runtimeExtentMappingNormalizeStore
}

type splitRuntimeWritePlanningStore struct {
	sequenceStore  runtimeChunkIDSequenceStore
	placementApply runtimePlacementApplyMetadataStore
}

func (s splitRuntimePlacementApplyStore) PutAllocationPage(ctx context.Context, rec clustermeta.AllocationPageRecord) error {
	return s.allocationPersist.PutAllocationPage(ctx, rec)
}

func (s splitRuntimePlacementApplyStore) GetExtentMapping(ctx context.Context, volumeID string, extentID uint64) (clustermeta.ExtentMappingRecord, error) {
	return s.extentMappingNormalize.GetExtentMapping(ctx, volumeID, extentID)
}

func (s splitRuntimePlacementApplyStore) PutExtentMapping(ctx context.Context, rec clustermeta.ExtentMappingRecord) error {
	return s.extentMappingNormalize.PutExtentMapping(ctx, rec)
}

func (s splitRuntimeWritePlanningStore) GetNextChunkID(ctx context.Context, volumeID string) (uint64, error) {
	return s.sequenceStore.GetNextChunkID(ctx, volumeID)
}

func (s splitRuntimeWritePlanningStore) PutNextChunkID(ctx context.Context, volumeID string, nextID uint64) error {
	return s.sequenceStore.PutNextChunkID(ctx, volumeID, nextID)
}

func (s splitRuntimeWritePlanningStore) PutAllocationPage(ctx context.Context, rec clustermeta.AllocationPageRecord) error {
	return s.placementApply.PutAllocationPage(ctx, rec)
}

func (s splitRuntimeWritePlanningStore) GetExtentMapping(ctx context.Context, volumeID string, extentID uint64) (clustermeta.ExtentMappingRecord, error) {
	return s.placementApply.GetExtentMapping(ctx, volumeID, extentID)
}

func (s splitRuntimeWritePlanningStore) PutExtentMapping(ctx context.Context, rec clustermeta.ExtentMappingRecord) error {
	return s.placementApply.PutExtentMapping(ctx, rec)
}

type runtimeClusterMetadataHandle interface{}

type runtimeSBSClusterMetadataConfig struct {
	Backend    string
	Root       string
	PebblePath string
}

type clusterVolumeCacheEntry struct {
	spec      service.VolumeSpec
	expiresAt time.Time
}

func newClusterVolumeLookup(repo rawClusterVolumeSpecReader, ttl time.Duration) *clusterVolumeLookup {
	if ttl == 0 {
		ttl = sbscluster.DefaultVolumeCacheTTL
	}
	return &clusterVolumeLookup{
		repo:  repo,
		ttl:   ttl,
		cache: make(map[uint64]clusterVolumeCacheEntry),
	}
}

func newPublishedClusterVolumeLookup(cfg repositoryConfig, fallback sbscluster.VolumeLookup, ttl time.Duration) sbscluster.VolumeLookup {
	return sbscluster.NewPublishedVolumeLookup(sbscluster.PublishedVolumeLookupOptions{
		Endpoint:         cfg.SBSAdminEndpoint,
		ClusterID:        gatewayClusterIdentityFromConfig(cfg),
		SBSClusterID:     sbsClusterIdentityFromConfig(cfg),
		Fallback:         fallback,
		AllowRawFallback: cfg.SBSClusterBootstrapMetadata,
		TTL:              ttl,
	})
}

func newPublishedVolumePlacementResolvers(cfg repositoryConfig, fallbackMappings runtimeExtentMappingMetadataResolver, fallbackSets runtimeReplicaSetMetadataResolver) (runtimeExtentMappingMetadataResolver, runtimeReplicaSetMetadataResolver) {
	return sbscluster.NewPublishedVolumePlacementResolvers(sbscluster.PublishedVolumePlacementOptions{
		Endpoint:         cfg.SBSAdminEndpoint,
		ClusterID:        gatewayClusterIdentityFromConfig(cfg),
		SBSClusterID:     sbsClusterIdentityFromConfig(cfg),
		TTL:              cfg.VolumeCacheTTL,
		FallbackMappings: fallbackMappings,
		FallbackSets:     fallbackSets,
		AllowRawFallback: cfg.SBSClusterBootstrapMetadata,
	})
}

func (l *clusterVolumeLookup) GetVolume(ctx context.Context, volumeID uint64) (service.VolumeSpec, error) {
	if l == nil || l.repo == nil {
		return service.VolumeSpec{}, service.ErrVolumeNotFound
	}
	now := time.Now()
	l.mu.Lock()
	if entry, ok := l.cache[volumeID]; ok && now.Before(entry.expiresAt) {
		spec := entry.spec
		l.mu.Unlock()
		return spec, nil
	}
	l.mu.Unlock()

	canonical := service.CanonicalVolumeID(volumeID)
	rec, err := l.repo.GetVolumeSpec(ctx, canonical)
	if err != nil {
		if errors.Is(err, clustermeta.ErrNotFound) {
			return service.VolumeSpec{}, service.ErrVolumeNotFound
		}
		return service.VolumeSpec{}, err
	}
	spec, err := clusterVolumeSpecFromRecord(volumeID, rec)
	if err != nil {
		return service.VolumeSpec{}, err
	}
	l.mu.Lock()
	l.cache[volumeID] = clusterVolumeCacheEntry{
		spec:      spec,
		expiresAt: time.Now().Add(l.ttl),
	}
	l.mu.Unlock()
	return spec, nil
}

func (l *clusterVolumeLookup) RefreshVolume(ctx context.Context, volumeID uint64) (service.VolumeSpec, error) {
	if l == nil || l.repo == nil {
		return service.VolumeSpec{}, service.ErrVolumeNotFound
	}
	spec, err := l.fetchVolume(ctx, volumeID)
	if err != nil {
		return service.VolumeSpec{}, err
	}
	l.mu.Lock()
	l.cache[volumeID] = clusterVolumeCacheEntry{
		spec:      spec,
		expiresAt: time.Now().Add(l.ttl),
	}
	l.mu.Unlock()
	return spec, nil
}

func (l *clusterVolumeLookup) fetchVolume(ctx context.Context, volumeID uint64) (service.VolumeSpec, error) {
	if l == nil || l.repo == nil {
		return service.VolumeSpec{}, service.ErrVolumeNotFound
	}
	canonical := service.CanonicalVolumeID(volumeID)
	rec, err := l.repo.GetVolumeSpec(ctx, canonical)
	if err != nil {
		if errors.Is(err, clustermeta.ErrNotFound) {
			return service.VolumeSpec{}, service.ErrVolumeNotFound
		}
		return service.VolumeSpec{}, err
	}
	return clusterVolumeSpecFromRecord(volumeID, rec)
}

func clusterVolumeSpecFromRecord(volumeID uint64, rec clustermeta.VolumeSpecRecord) (service.VolumeSpec, error) {
	canonical := service.CanonicalVolumeID(volumeID)
	blockSize := rec.BlockSize
	if blockSize == 0 {
		return service.VolumeSpec{}, fmt.Errorf("sbs cluster volume %s has empty block size", canonical)
	}
	extentSize := rec.ExtentSizeBytes
	if extentSize == 0 {
		return service.VolumeSpec{}, fmt.Errorf("sbs cluster volume %s has empty extent size", canonical)
	}
	if extentSize%uint64(blockSize) != 0 {
		return service.VolumeSpec{}, fmt.Errorf("sbs cluster volume %s extent size %d is not aligned to block size %d", canonical, extentSize, blockSize)
	}
	defaulted := service.NormalizeVolumeSpec(service.VolumeSpec{BlockSize: blockSize})
	chunkSizeBytes := rec.ChunkSizeBytes
	if chunkSizeBytes == 0 {
		chunkSizeBytes = defaulted.ChunkSizeBytes
	}
	extentPageBytes := rec.ExtentPageBytes
	if extentPageBytes == 0 {
		extentPageBytes = defaulted.ExtentPageBytes
	}
	if extentPageBytes%chunkSizeBytes != 0 {
		return service.VolumeSpec{}, fmt.Errorf("sbs cluster volume %s has invalid allocation geometry: page_bytes=%d chunk_size_bytes=%d", canonical, extentPageBytes, chunkSizeBytes)
	}
	return service.NormalizeVolumeSpec(service.VolumeSpec{
		ID:                             service.HexVolumeID(volumeID),
		Name:                           "sbs-" + canonical,
		Prefix:                         "sbs-" + canonical,
		SizeBytes:                      rec.SizeBytes,
		BlockSize:                      blockSize,
		ChunkSizeBytes:                 chunkSizeBytes,
		ExtentPageBytes:                extentPageBytes,
		State:                          service.VolumeStateAvailable,
		RedundancyBackend:              rec.RedundancyBackend,
		TopologyMode:                   rec.TopologyMode,
		ECProfileID:                    rec.ECProfileID,
		ECCodecID:                      rec.ECCodecID,
		ECDataShards:                   rec.ECDataShards,
		ECParityShards:                 rec.ECParityShards,
		ECStripeUnitBytes:              rec.ECStripeUnitBytes,
		ECFailureDomain:                rec.ECFailureDomain,
		ECMaxUnavailableFailureDomains: rec.ECMaxUnavailableFailureDomains,
		ECMaxShardsPerFailureDomain:    rec.ECMaxShardsPerFailureDomain,
		WeakPlacementAllowed:           rec.WeakPlacementAllowed,
		ProtectedState:                 serviceProtectedStateFromCluster(rec.ProtectedState),
	}), nil
}

func serviceProtectedStateFromCluster(rec *clustermeta.VolumeProtectedStateRecord) *service.VolumeProtectedState {
	if rec == nil {
		return nil
	}
	protectedState := service.VolumeProtectedState{
		State:            service.VolumeProtectedStateKind(strings.TrimSpace(rec.State)),
		ReasonCode:       strings.TrimSpace(rec.ReasonCode),
		SealedObjectID:   strings.TrimSpace(rec.SealedObjectID),
		SealOperationID:  strings.TrimSpace(rec.SealOperationID),
		PolicySnapshotID: strings.TrimSpace(rec.PolicySnapshotID),
		LifecycleState:   strings.TrimSpace(rec.LifecycleState),
		SourceVolumeID:   strings.TrimSpace(rec.SourceVolumeID),
	}.Normalize()
	if protectedState.IsZero() {
		return nil
	}
	return &protectedState
}

func newPublishedAllocationPageReader(cfg repositoryConfig, fallback runtimeAllocationPageReader) sbsclusterAllocationPageReader {
	return sbscluster.NewPublishedAllocationPageReader(sbscluster.PublishedAllocationPageReaderOptions{
		Endpoint:         cfg.SBSAdminEndpoint,
		ClusterID:        gatewayClusterIdentityFromConfig(cfg),
		SBSClusterID:     sbsClusterIdentityFromConfig(cfg),
		Fallback:         fallback,
		AllowRawFallback: cfg.SBSClusterBootstrapMetadata,
	})
}

type runtimeECMetadataStore struct {
	state   runtimeWriteSessionMetadataStore
	nodes   sbsclusterNodeMembershipResolver
	pages   sbsclusterAllocationPageReader
	records clustercontrol.ECMetadataAdapter
}

func newRuntimeECMetadataStore(state runtimeWriteSessionMetadataStore, nodes sbsclusterNodeMembershipResolver, pages sbsclusterAllocationPageReader, records clustercontrol.ECMetadataAdapter) *runtimeECMetadataStore {
	if records == nil {
		return nil
	}
	return &runtimeECMetadataStore{state: state, nodes: nodes, pages: pages, records: records}
}

func (s *runtimeECMetadataStore) GetVolumeState(ctx context.Context, volumeID string) (clustermeta.VolumeState, error) {
	if s == nil || s.state == nil {
		return clustermeta.VolumeState{}, fmt.Errorf("ec metadata volume state store is not configured")
	}
	return s.state.GetVolumeState(ctx, volumeID)
}

func (s *runtimeECMetadataStore) GetIdempotencyRecord(ctx context.Context, volumeID, idempotencyKey string) (clustermeta.IdempotencyRecord, error) {
	if s == nil || s.state == nil {
		return clustermeta.IdempotencyRecord{}, fmt.Errorf("ec metadata idempotency store is not configured")
	}
	return s.state.GetIdempotencyRecord(ctx, volumeID, idempotencyKey)
}

func (s *runtimeECMetadataStore) PutIdempotencyRecord(ctx context.Context, rec clustermeta.IdempotencyRecord) error {
	if s == nil || s.state == nil {
		return fmt.Errorf("ec metadata idempotency store is not configured")
	}
	return s.state.PutIdempotencyRecord(ctx, rec)
}

func (s *runtimeECMetadataStore) GetMutationOperation(ctx context.Context, volumeID, operationID string) (clustermeta.MutationOperationRecord, error) {
	if s == nil || s.state == nil {
		return clustermeta.MutationOperationRecord{}, fmt.Errorf("ec metadata mutation store is not configured")
	}
	return s.state.GetMutationOperation(ctx, volumeID, operationID)
}

func (s *runtimeECMetadataStore) PutMutationOperation(ctx context.Context, rec clustermeta.MutationOperationRecord) error {
	if s == nil || s.state == nil {
		return fmt.Errorf("ec metadata mutation store is not configured")
	}
	return s.state.PutMutationOperation(ctx, rec)
}

func (s *runtimeECMetadataStore) PutWriteIntent(ctx context.Context, record clustermeta.IdempotencyRecord, operation clustermeta.MutationOperationRecord) error {
	if s == nil || s.state == nil {
		return fmt.Errorf("ec metadata write intent store is not configured")
	}
	return s.state.PutWriteIntent(ctx, record, operation)
}

func (s *runtimeECMetadataStore) ListNodeMemberships(ctx context.Context) ([]clustermeta.NodeMembershipRecord, error) {
	if s == nil || s.nodes == nil {
		return nil, fmt.Errorf("ec metadata node membership resolver is not configured")
	}
	return s.nodes.ListNodeMemberships(ctx)
}

func (s *runtimeECMetadataStore) GetCompatibleAllocationPage(ctx context.Context, volumeID string, pageNo uint64, pageBytes, chunkSizeBytes uint32) (clustermeta.AllocationPageRecord, error) {
	if s == nil || s.pages == nil {
		return clustermeta.AllocationPageRecord{}, fmt.Errorf("ec metadata allocation page reader is not configured")
	}
	return s.pages.GetCompatibleAllocationPage(ctx, volumeID, pageNo, pageBytes, chunkSizeBytes)
}

func (s *runtimeECMetadataStore) GetPhysicalObject(ctx context.Context, volumeID, objectID string) (clustermeta.PhysicalObjectRecord, error) {
	if s == nil || s.records == nil {
		return clustermeta.PhysicalObjectRecord{}, fmt.Errorf("ec metadata object store is not configured")
	}
	return s.records.GetPhysicalObject(ctx, volumeID, objectID)
}

func (s *runtimeECMetadataStore) PutPhysicalObject(ctx context.Context, rec clustermeta.PhysicalObjectRecord) error {
	if s == nil || s.records == nil {
		return fmt.Errorf("ec metadata object store is not configured")
	}
	return s.records.PutPhysicalObject(ctx, rec)
}

func (s *runtimeECMetadataStore) GetECStripe(ctx context.Context, volumeID, stripeID string, stripeGeneration uint64) (clustermeta.ECStripeRecord, error) {
	if s == nil || s.records == nil {
		return clustermeta.ECStripeRecord{}, fmt.Errorf("ec metadata stripe store is not configured")
	}
	return s.records.GetECStripe(ctx, volumeID, stripeID, stripeGeneration)
}

func (s *runtimeECMetadataStore) PutECStripe(ctx context.Context, rec clustermeta.ECStripeRecord) error {
	if s == nil || s.records == nil {
		return fmt.Errorf("ec metadata stripe store is not configured")
	}
	return s.records.PutECStripe(ctx, rec)
}

func (s *runtimeECMetadataStore) CommitECFullStripeWrite(ctx context.Context, req clustermeta.CommitECFullStripeWriteRequest) (clustermeta.VolumeState, clustermeta.IdempotencyRecord, error) {
	if s == nil || s.records == nil {
		return clustermeta.VolumeState{}, clustermeta.IdempotencyRecord{}, fmt.Errorf("ec metadata commit store is not configured")
	}
	return s.records.CommitECFullStripeWrite(ctx, req)
}

func (s *runtimeECMetadataStore) CommitECDiscard(ctx context.Context, req clustermeta.CommitECDiscardRequest) (clustermeta.VolumeState, clustermeta.IdempotencyRecord, error) {
	if s == nil || s.records == nil {
		return clustermeta.VolumeState{}, clustermeta.IdempotencyRecord{}, fmt.Errorf("ec metadata commit store is not configured")
	}
	return s.records.CommitECDiscard(ctx, req)
}

type clusterBackedMetadataRepository struct {
	service.MetadataRepository
	lookup    sbscluster.VolumeLookup
	ensureTTL time.Duration

	mu      sync.Mutex
	ensured map[uint64]time.Time
}

func (r *clusterBackedMetadataRepository) GetVolume(ctx context.Context, volumeID uint64) (service.VolumeSpec, error) {
	return r.lookup.GetVolume(ctx, volumeID)
}

func (r *clusterBackedMetadataRepository) RefreshVolume(ctx context.Context, volumeID uint64) (service.VolumeSpec, error) {
	if err := r.ensureVolumeFresh(ctx, volumeID); err != nil {
		return service.VolumeSpec{}, err
	}
	return r.MetadataRepository.GetVolume(ctx, volumeID)
}

func (r *clusterBackedMetadataRepository) GetVolumeStatus(ctx context.Context, volumeID uint64) (service.VolumeStatusRecord, error) {
	if err := r.ensureVolume(ctx, volumeID); err != nil {
		return service.VolumeStatusRecord{}, err
	}
	return r.MetadataRepository.GetVolumeStatus(ctx, volumeID)
}

func (r *clusterBackedMetadataRepository) PutVolumeStatus(ctx context.Context, status service.VolumeStatusRecord) error {
	if err := r.ensureVolumeFresh(ctx, uint64(status.VolumeID)); err != nil {
		return err
	}
	return r.MetadataRepository.PutVolumeStatus(ctx, status)
}

func (r *clusterBackedMetadataRepository) SetVolumeState(ctx context.Context, volumeID uint64, state service.VolumeLifecycleState) (service.VolumeSpec, error) {
	if err := r.ensureVolumeFresh(ctx, volumeID); err != nil {
		return service.VolumeSpec{}, err
	}
	return r.MetadataRepository.SetVolumeState(ctx, volumeID, state)
}

func (r *clusterBackedMetadataRepository) GetAttachment(ctx context.Context, volumeID uint64) (service.AttachmentRecord, error) {
	if err := r.ensureVolume(ctx, volumeID); err != nil {
		return service.AttachmentRecord{}, err
	}
	return r.MetadataRepository.GetAttachment(ctx, volumeID)
}

func (r *clusterBackedMetadataRepository) GetGeneration(ctx context.Context, volumeID uint64) (uint64, error) {
	if err := r.ensureVolume(ctx, volumeID); err != nil {
		return 0, err
	}
	return r.MetadataRepository.GetGeneration(ctx, volumeID)
}

func (r *clusterBackedMetadataRepository) UnsafeClearAttachment(ctx context.Context, volumeID uint64) (service.AttachmentRecord, error) {
	if err := r.ensureVolumeFresh(ctx, volumeID); err != nil {
		return service.AttachmentRecord{}, err
	}
	return r.MetadataRepository.UnsafeClearAttachment(ctx, volumeID)
}

func (r *clusterBackedMetadataRepository) UnsafeSetGeneration(ctx context.Context, volumeID uint64, generation uint64) (uint64, error) {
	if err := r.ensureVolumeFresh(ctx, volumeID); err != nil {
		return 0, err
	}
	return r.MetadataRepository.UnsafeSetGeneration(ctx, volumeID, generation)
}

func (r *clusterBackedMetadataRepository) Attach(ctx context.Context, req service.AttachRequest) (service.AttachmentRecord, error) {
	if err := r.ensureVolumeFresh(ctx, req.VolumeID); err != nil {
		return service.AttachmentRecord{}, err
	}
	return r.MetadataRepository.Attach(ctx, req)
}

func (r *clusterBackedMetadataRepository) ensureVolume(ctx context.Context, volumeID uint64) error {
	return r.ensureVolumeFromLookup(ctx, volumeID, false)
}

func (r *clusterBackedMetadataRepository) ensureVolumeFresh(ctx context.Context, volumeID uint64) error {
	return r.ensureVolumeFromLookup(ctx, volumeID, true)
}

func (r *clusterBackedMetadataRepository) ensureVolumeFromLookup(ctx context.Context, volumeID uint64, force bool) error {
	now := time.Now()
	if !force {
		r.mu.Lock()
		if expiresAt, ok := r.ensured[volumeID]; ok && now.Before(expiresAt) {
			r.mu.Unlock()
			return nil
		}
		r.mu.Unlock()
	}

	spec, err := lookupVolume(ctx, r.lookup, volumeID, force)
	if err != nil {
		return err
	}
	existing, existingErr := r.MetadataRepository.GetVolume(ctx, volumeID)
	if existingErr == nil {
		if existing.Name != "" {
			spec.Name = existing.Name
		}
		if existing.Prefix != "" {
			spec.Prefix = existing.Prefix
		}
		if err := syncVolumeSpec(ctx, r.MetadataRepository, spec); err != nil {
			return err
		}
	} else if errors.Is(existingErr, service.ErrVolumeNotFound) {
		if err := r.MetadataRepository.EnsureVolume(ctx, spec); err != nil {
			return err
		}
	} else {
		return existingErr
	}

	ttl := r.ensureTTL
	if ttl == 0 {
		ttl = sbscluster.DefaultVolumeCacheTTL
	}
	r.mu.Lock()
	if r.ensured == nil {
		r.ensured = make(map[uint64]time.Time)
	}
	r.ensured[volumeID] = time.Now().Add(ttl)
	r.mu.Unlock()
	return nil
}

func syncVolumeSpec(ctx context.Context, repo service.MetadataRepository, spec service.VolumeSpec) error {
	if syncer, ok := repo.(service.VolumeSpecSyncRepository); ok {
		return syncer.SyncVolumeSpec(ctx, spec)
	}
	return repo.EnsureVolume(ctx, spec)
}

func lookupVolume(ctx context.Context, lookup sbscluster.VolumeLookup, volumeID uint64, fresh bool) (service.VolumeSpec, error) {
	if fresh {
		if refresh, ok := lookup.(interface {
			RefreshVolume(context.Context, uint64) (service.VolumeSpec, error)
		}); ok {
			return refresh.RefreshVolume(ctx, volumeID)
		}
	}
	return lookup.GetVolume(ctx, volumeID)
}

type sbsVolumeBootstrapper interface {
	CreateVolume(ctx context.Context, spec service.VolumeSpec) (service.VolumeSpec, error)
}

func bootstrapSBSVolumes(ctx context.Context, meta service.MetadataRepository, bootstrapper sbsVolumeBootstrapper) error {
	volumes, err := meta.ListVolumes(ctx)
	if err != nil {
		return err
	}
	for _, volume := range volumes {
		if _, err := bootstrapper.CreateVolume(ctx, volume); err != nil {
			return err
		}
	}
	return nil
}

func bootstrapMetadata(ctx context.Context, repo service.MetadataRepository, volumes []service.VolumeSpec, gateway service.GatewayRecord) error {
	for _, volume := range volumes {
		if err := repo.EnsureVolume(ctx, volume); err != nil {
			return err
		}
	}
	if gateway.GatewayID != "" {
		if err := repo.PutGateway(ctx, gateway); err != nil {
			return err
		}
	}
	return nil
}

func gatewayRecordFromConfig(cfg repositoryConfig) service.GatewayRecord {
	rec := service.NewGatewayRecord(cfg.GatewayID, buildVersion, []service.EndpointSpec{{
		Address:    cfg.ControlAddress,
		Port:       cfg.ControlPort,
		UseTLS:     cfg.ControlUseTLS,
		ServerName: cfg.ControlServerName,
		AuthMode:   "bearer",
	}},
		[]service.EndpointSpec{{
			PathID:   0,
			Address:  cfg.DataAddress,
			Port:     cfg.DataPort,
			AuthMode: "bearer",
			Priority: 100,
		}})
	rec.ClusterID = gatewayClusterIdentityFromConfig(cfg)
	rec.SBSClusterID = sbsClusterIdentityFromConfig(cfg)
	rec.MetadataBackend = canonicalGatewayMetadataBackend(cfg)
	rec.MetadataRoot = canonicalGatewayMetadataRoot(cfg)
	rec.SBSClusterMetadataBackend = canonicalSBSClusterMetadataBackend(cfg)
	rec.SBSClusterMetadataRoot = canonicalSBSClusterMetadataRoot(cfg)
	rec.FailureDomain = gatewayFailureDomainFromConfig(cfg)
	return rec
}

func gatewayClusterIdentityFromConfig(cfg repositoryConfig) string {
	return fmt.Sprintf("namrbd:%s:%s", canonicalGatewayMetadataBackend(cfg), canonicalGatewayMetadataRoot(cfg))
}

func sbsClusterIdentityFromConfig(cfg repositoryConfig) string {
	backend := canonicalSBSClusterMetadataBackend(cfg)
	root := canonicalSBSClusterMetadataRoot(cfg)
	if backend == "" || root == "" {
		return ""
	}
	return fmt.Sprintf("sbs:%s:%s", backend, root)
}

func effectiveSBSPlacementApplyTimeout(cfg repositoryConfig) time.Duration {
	if cfg.SBSPlacementApplyTimeout == 0 {
		return sbscluster.DefaultPlacementApplyTimeout
	}
	return cfg.SBSPlacementApplyTimeout
}

var newAdminEndpointPlacementApplyAdapter = newAdminEndpointPlacementApplyAdapterDefault

func newAdminEndpointPlacementApplyAdapterDefault(cfg repositoryConfig) (runtimePlacementApplyAdapter, func(), error) {
	return clustercontrol.NewAdminEndpointPlacementApplyAdapter(cfg.SBSAdminEndpoint)
}

var newAdminEndpointWriteSessionCommitter = newAdminEndpointWriteSessionCommitterDefault

func newAdminEndpointWriteSessionCommitterDefault(cfg repositoryConfig) (runtimeWriteSessionAuthority, func(), error) {
	return clustercontrol.NewAdminEndpointWriteSessionAdapter(cfg.SBSAdminEndpoint)
}

var newAdminEndpointChunkIDAllocator = newAdminEndpointChunkIDAllocatorDefault

func newAdminEndpointChunkIDAllocatorDefault(cfg repositoryConfig) (runtimeChunkIDAllocator, func(), error) {
	return clustercontrol.NewAdminEndpointChunkIDAllocator(cfg.SBSAdminEndpoint)
}

var newAdminEndpointPlacementResolver = newAdminEndpointPlacementResolverDefault

func newAdminEndpointPlacementResolverDefault(cfg repositoryConfig) (runtimePlacementPlanningResolver, func(), error) {
	return clustercontrol.NewAdminEndpointPlacementResolver(cfg.SBSAdminEndpoint)
}

var newAdminEndpointSourceSnapshotLister = newAdminEndpointSourceSnapshotListerDefault

func newAdminEndpointSourceSnapshotListerDefault(cfg repositoryConfig) (runtimeSourceSnapshotLister, func(), error) {
	return clustercontrol.NewAdminEndpointSourceSnapshotLister(cfg.SBSAdminEndpoint)
}

func newAdminEndpointPerformanceBudgetLeaseClient(endpoint string) (httpapi.PerformanceBudgetLeaseClient, func(), error) {
	endpoint = strings.TrimSpace(endpoint)
	if endpoint == "" {
		return nil, nil, fmt.Errorf("--sbs-admin-endpoint is required for Phase O cluster_volume performance admission")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	client, err := adminclient.Dial(ctx, endpoint)
	if err != nil {
		return nil, nil, err
	}
	return &adminEndpointPerformanceBudgetLeaseClient{client: client}, func() { _ = client.Close() }, nil
}

type adminEndpointPerformanceBudgetLeaseClient struct {
	client *adminclient.Client
}

func (c *adminEndpointPerformanceBudgetLeaseClient) AcquirePerformanceBudgetLease(ctx context.Context, req httpapi.PerformanceBudgetLeaseRequest) (httpapi.PerformanceBudgetLeaseGrant, error) {
	resp, err := c.client.Admin.AcquireBudgetLease(ctx, &adminv1.AcquireBudgetLeaseRequest{
		Meta:                    &adminv1.RequestMeta{Actor: req.GatewayID, Reason: "phase-o-performance-admission"},
		LeaseId:                 req.LeaseID,
		VolumeId:                req.VolumeID,
		PolicyId:                req.PolicyID,
		PolicyGeneration:        req.PolicyGeneration,
		BudgetClass:             req.BudgetClass,
		CapScope:                req.CapScope,
		ThrottleMode:            req.ThrottleMode,
		RequestedTokens:         req.RequestedTokens,
		RequestedBytes:          req.RequestedBytes,
		IopsCap:                 req.IOPSCap,
		BandwidthCapBytesPerSec: req.BandwidthCapBytesPerSec,
		BurstIops:               req.BurstIOPS,
		BurstBytes:              req.BurstBytes,
		WindowMs:                req.WindowMs,
		TtlMs:                   req.TTLMs,
		GatewayId:               req.GatewayID,
	})
	if err != nil {
		return httpapi.PerformanceBudgetLeaseGrant{}, err
	}
	lease := resp.GetLease()
	if lease == nil {
		return httpapi.PerformanceBudgetLeaseGrant{}, fmt.Errorf("budget lease missing from admin response")
	}
	return httpapi.PerformanceBudgetLeaseGrant{
		LeaseID:                 lease.GetLeaseId(),
		LeaseGeneration:         lease.GetLeaseGeneration(),
		GrantedTokens:           lease.GetGrantedTokens(),
		GrantedBytes:            lease.GetGrantedBytes(),
		DeniedTokens:            lease.GetDeniedTokens(),
		DeniedBytes:             lease.GetDeniedBytes(),
		ThrottleWaitMs:          lease.GetThrottleWaitMs(),
		RejectedOps:             lease.GetRejectedOps(),
		RejectionReason:         lease.GetRejectionReason(),
		OutstandingTokensBefore: lease.GetOutstandingTokensBefore(),
		OutstandingBytesBefore:  lease.GetOutstandingBytesBefore(),
		AvailableTokensBefore:   lease.GetAvailableTokensBefore(),
		AvailableBytesBefore:    lease.GetAvailableBytesBefore(),
		ClusterWideCapSupport:   lease.GetClusterWideCapSupport(),
		SharedBudgetAuthority:   lease.GetSharedBudgetAuthority(),
	}, nil
}

func canonicalGatewayMetadataBackend(cfg repositoryConfig) string {
	backend := strings.TrimSpace(cfg.MetadataBackend)
	if backend == "" {
		return "memory"
	}
	return backend
}

func canonicalGatewayMetadataRoot(cfg repositoryConfig) string {
	switch canonicalGatewayMetadataBackend(cfg) {
	case "etcd":
		root := strings.TrimSpace(cfg.EtcdRoot)
		if root == "" {
			root = "/namrbd"
		}
		return root
	default:
		return "memory"
	}
}

func canonicalSBSClusterMetadataBackend(cfg repositoryConfig) string {
	if strings.TrimSpace(cfg.DataBackendMode) != "sbs-cluster" {
		return ""
	}
	return effectiveSBSClusterMetadataBackend(cfg)
}

func effectiveSBSClusterMetadataBackend(cfg repositoryConfig) string {
	backend := strings.TrimSpace(cfg.SBSClusterMetadataBackend)
	if backend == "" {
		return "pebble"
	}
	return backend
}

func canonicalSBSClusterMetadataRoot(cfg repositoryConfig) string {
	if strings.TrimSpace(cfg.DataBackendMode) != "sbs-cluster" {
		return ""
	}
	root := strings.TrimSpace(cfg.SBSClusterMetadataRoot)
	if root == "" {
		root = "sbs/cluster"
	}
	return root
}

func gatewayFailureDomainFromConfig(cfg repositoryConfig) string {
	controlAddr := strings.TrimSpace(cfg.ControlAddress)
	if controlAddr == "" {
		return ""
	}
	return "host:" + controlAddr
}

func reconcileAllGatewayPathPlanStatuses(ctx context.Context, repo service.MetadataRepository) (int, error) {
	gateways, err := repo.ListGateways(ctx)
	if err != nil {
		return 0, err
	}
	volumes, err := repo.ListVolumes(ctx)
	if err != nil {
		return 0, err
	}
	volumes, err = filterExistingPathPlanReconcileVolumes(ctx, repo, volumes)
	if err != nil {
		return 0, err
	}
	volumes, err = prioritizePathPlanReconcileVolumes(ctx, repo, volumes)
	if err != nil {
		return 0, err
	}
	updated := 0
	topClass, err := gatewayPathPlanTopPriorityClass(ctx, repo, volumes)
	if err != nil {
		return 0, err
	}
	for _, volume := range volumes {
		status, err := repo.GetVolumeStatus(ctx, uint64(volume.ID))
		if err != nil {
			if isVolumeNotFoundError(err) {
				continue
			}
			return updated, err
		}
		priorityClass := service.OperatorPathPlanPriorityClassWithCluster(status, topClass)
		operatorActions := service.OperatorPathPlanRecommendedActionsWithCluster(status, topClass)
		clusterPriorityMatches := topClass == "" || priorityClass == topClass
		if priorityClass != "normal" {
			log.Printf("gateway path-plan reconcile candidate volume=%s priority_class=%s cluster_top_priority_class=%s cluster_priority_matches_controller=%t operator_actions=%v",
				service.CanonicalVolumeID(uint64(volume.ID)), priorityClass, topClass, clusterPriorityMatches, operatorActions)
		}
		next, observedChanged, desiredChanged := service.ReconcileVolumePathPlanStatus(status, gateways)
		if !observedChanged && !desiredChanged && reflect.DeepEqual(next, status) {
			continue
		}
		if err := repo.PutVolumeStatus(ctx, next); err != nil {
			return updated, err
		}
		nextPriorityClass := service.OperatorPathPlanPriorityClassWithCluster(next, topClass)
		nextOperatorActions := service.OperatorPathPlanRecommendedActionsWithCluster(next, topClass)
		nextClusterPriorityMatches := topClass == "" || nextPriorityClass == topClass
		log.Printf("gateway path-plan reconcile applied volume=%s priority_class=%s operator_actions=%v observed_changed=%t desired_changed=%t reapply_requested=%t handoff_required=%t",
			service.CanonicalVolumeID(uint64(volume.ID)),
			nextPriorityClass,
			nextOperatorActions,
			observedChanged,
			desiredChanged,
			next.PathPlanReapplyRequested,
			next.HandoffRequired,
		)
		log.Printf("gateway path-plan reconcile applied-context volume=%s cluster_top_priority_class=%s cluster_priority_matches_controller=%t",
			service.CanonicalVolumeID(uint64(volume.ID)),
			topClass,
			nextClusterPriorityMatches,
		)
		updated++
	}
	return updated, nil
}

func filterExistingPathPlanReconcileVolumes(ctx context.Context, repo service.MetadataRepository, volumes []service.VolumeSpec) ([]service.VolumeSpec, error) {
	if len(volumes) == 0 {
		return nil, nil
	}
	out := make([]service.VolumeSpec, 0, len(volumes))
	for _, volume := range volumes {
		if _, err := repo.GetVolume(ctx, uint64(volume.ID)); err != nil {
			if isVolumeNotFoundError(err) {
				if deleteErr := repo.DeleteVolume(ctx, uint64(volume.ID)); deleteErr != nil && !isVolumeNotFoundError(deleteErr) {
					log.Printf("gateway path-plan reconcile stale volume cleanup failed volume=%s error=%v", service.CanonicalVolumeID(uint64(volume.ID)), deleteErr)
				} else if deleteErr == nil {
					log.Printf("gateway path-plan reconcile removed stale local volume=%s", service.CanonicalVolumeID(uint64(volume.ID)))
				}
				continue
			}
			return nil, err
		}
		out = append(out, volume)
	}
	return out, nil
}

func gatewayPathPlanTopPriorityClass(ctx context.Context, repo service.MetadataRepository, volumes []service.VolumeSpec) (string, error) {
	counts := make(map[string]int, 6)
	for _, volume := range volumes {
		status, err := repo.GetVolumeStatus(ctx, uint64(volume.ID))
		if err != nil {
			if isVolumeNotFoundError(err) {
				continue
			}
			return "", err
		}
		counts[service.OperatorPathPlanPriorityClass(status)]++
	}
	for _, name := range []string{"aggressive_handoff", "handoff", "expansion_ready", "refresh", "attention", "normal"} {
		if counts[name] > 0 {
			return name, nil
		}
	}
	return "", nil
}

func isVolumeNotFoundError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, service.ErrVolumeNotFound) {
		return true
	}
	if status.Code(err) == codes.NotFound {
		return true
	}
	var sbsErr *service.SBSError
	return errors.As(err, &sbsErr) && sbsErr.Code == service.SBSErrorCodeNotFound
}

func prioritizePathPlanReconcileVolumes(ctx context.Context, repo service.MetadataRepository, volumes []service.VolumeSpec) ([]service.VolumeSpec, error) {
	type prioritizedVolume struct {
		spec             service.VolumeSpec
		priority         int
		requestedAt      int64
		scheduledAt      int64
		reconcileNow     bool
		reconcileDueSoon bool
	}
	nowUnix := time.Now().Unix()
	topClass, err := gatewayPathPlanTopPriorityClass(ctx, repo, volumes)
	if err != nil {
		return nil, err
	}
	items := make([]prioritizedVolume, 0, len(volumes))
	for _, volume := range volumes {
		status, err := repo.GetVolumeStatus(ctx, uint64(volume.ID))
		if err != nil {
			if isVolumeNotFoundError(err) {
				continue
			}
			return nil, err
		}
		priorityClass := service.OperatorPathPlanPriorityClassWithCluster(status, topClass)
		priority := 0
		switch priorityClass {
		case "aggressive_handoff":
			priority = 5
		case "handoff":
			priority = 4
		case "expansion_ready":
			priority = 3
		case "refresh":
			priority = 2
		case "attention":
			priority = 1
		}
		items = append(items, prioritizedVolume{
			spec:             volume,
			priority:         priority,
			requestedAt:      status.ControllerReconcileRequestedAtUnix,
			scheduledAt:      status.ControllerReconcileScheduledAtUnix,
			reconcileNow:     status.ControllerReconcileRequestedAtUnix > 0,
			reconcileDueSoon: status.ControllerReconcileRequestedAtUnix == 0 && status.ControllerReconcileScheduledAtUnix > 0 && status.ControllerReconcileScheduledAtUnix <= nowUnix,
		})
	}
	sort.SliceStable(items, func(i, j int) bool {
		if items[i].reconcileNow != items[j].reconcileNow {
			return items[i].reconcileNow
		}
		if items[i].priority != items[j].priority {
			return items[i].priority > items[j].priority
		}
		if items[i].reconcileDueSoon != items[j].reconcileDueSoon {
			return items[i].reconcileDueSoon
		}
		if items[i].reconcileDueSoon && items[i].scheduledAt != items[j].scheduledAt {
			return items[i].scheduledAt < items[j].scheduledAt
		}
		if items[i].requestedAt != items[j].requestedAt {
			return items[i].requestedAt > items[j].requestedAt
		}
		if items[i].scheduledAt != items[j].scheduledAt {
			if items[i].scheduledAt == 0 {
				return false
			}
			if items[j].scheduledAt == 0 {
				return true
			}
			return items[i].scheduledAt < items[j].scheduledAt
		}
		return uint64(items[i].spec.ID) < uint64(items[j].spec.ID)
	})
	out := make([]service.VolumeSpec, 0, len(items))
	for _, item := range items {
		out = append(out, item.spec)
	}
	return out, nil
}

func toVolumeSpecs(volumes []store.Volume) []service.VolumeSpec {
	out := make([]service.VolumeSpec, 0, len(volumes))
	for _, volume := range volumes {
		spec := service.NormalizeVolumeSpec(service.VolumeSpec{
			ID:         service.HexVolumeID(volume.ID),
			Name:       volume.Prefix,
			Prefix:     volume.Prefix,
			SizeBytes:  volume.SizeBytes,
			BlockSize:  service.DefaultBlockSize,
			AccessMode: service.VolumeAccessModeExclusive,
			State:      service.VolumeStateAvailable,
		})
		out = append(out, spec)
	}
	return out
}

func splitCSV(raw string) []string {
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

func parseReplicaTargets(raw string) map[string]replicaTarget {
	out := map[string]replicaTarget{}
	for _, item := range splitCSV(raw) {
		parts := strings.SplitN(item, "=", 2)
		if len(parts) != 2 {
			continue
		}
		replicaID := strings.TrimSpace(parts[0])
		rawTarget := strings.TrimSpace(parts[1])
		if replicaID == "" || rawTarget == "" {
			continue
		}
		target := replicaTarget{TargetID: replicaID, Kind: replicaTargetLocal, Path: rawTarget}
		switch {
		case strings.HasPrefix(rawTarget, "grpc://"):
			host, port, err := splitReplicaEndpoint(strings.TrimPrefix(rawTarget, "grpc://"))
			if err != nil {
				continue
			}
			target.Kind = replicaTargetGRPC
			target.Path = ""
			target.Endpoint = clustermeta.SBSEndpoint{Address: host, Port: port}
		case strings.HasPrefix(rawTarget, "grpcs://"):
			host, port, err := splitReplicaEndpoint(strings.TrimPrefix(rawTarget, "grpcs://"))
			if err != nil {
				continue
			}
			target.Kind = replicaTargetGRPC
			target.Path = ""
			target.Endpoint = clustermeta.SBSEndpoint{Address: host, Port: port, UseTLS: true, ServerName: host}
		case strings.HasPrefix(rawTarget, "local:"):
			target.Path = strings.TrimPrefix(rawTarget, "local:")
		}
		out[replicaID] = target
	}
	return out
}

func splitReplicaEndpoint(raw string) (string, uint16, error) {
	host, portRaw, err := net.SplitHostPort(raw)
	if err != nil {
		return "", 0, err
	}
	port, err := strconv.Atoi(portRaw)
	if err != nil || port < 1 || port > 65535 {
		return "", 0, fmt.Errorf("invalid replica endpoint port %q", portRaw)
	}
	return host, uint16(port), nil
}

func loadSBSClusterReplicaTargets(ctx context.Context, cfg repositoryConfig, clusterRepo rawReplicaTargetMetadataReader) (map[string]replicaTarget, string, error) {
	replicaTargets := cfg.SBSClusterReplicas
	if strings.TrimSpace(cfg.SBSAdminEndpoint) != "" {
		targets, err := resolveReplicaTargetsFromAdmin(ctx, cfg)
		if err == nil && len(targets) > 0 {
			return targets, "published-view", nil
		}
		if err != nil {
			if len(replicaTargets) > 0 {
				log.Printf("gateway published replica targets view unavailable via sbs-admin endpoint %q: %v; activating static replica target fallback", cfg.SBSAdminEndpoint, err)
				return replicaTargets, "static-config-fallback", nil
			}
			if !cfg.SBSClusterBootstrapMetadata {
				return nil, "", fmt.Errorf("published replica targets view unavailable via sbs-admin endpoint %q: %w", cfg.SBSAdminEndpoint, err)
			}
			log.Printf("gateway published replica targets view unavailable via sbs-admin endpoint %q: %v; activating legacy raw cluster metadata bootstrap fallback", cfg.SBSAdminEndpoint, err)
		}
	}
	if len(replicaTargets) > 0 {
		return replicaTargets, "static-config", nil
	}
	if !cfg.SBSClusterBootstrapMetadata {
		return nil, "", fmt.Errorf("sbs-cluster startup requires explicit --sbs-cluster-replicas or reachable --sbs-admin-endpoint")
	}
	if clusterRepo == nil {
		return nil, "", fmt.Errorf("runtime cluster metadata repository does not support legacy target bootstrap fallback")
	}
	targets, err := resolveReplicaTargetsFromMetadata(ctx, clusterRepo)
	if err != nil {
		return nil, "", err
	}
	return targets, "legacy-raw-metadata-fallback", nil
}

func maybeBootstrapLegacyClusterMetadata(ctx context.Context, meta service.MetadataRepository, cfg repositoryConfig, clusterRepo legacyClusterBootstrapWriter, replicaTargets map[string]replicaTarget) error {
	if !cfg.SBSClusterBootstrapMetadata {
		return nil
	}
	log.Printf("gateway legacy/dev bootstrap path active: bootstrapping SBS cluster metadata from gateway metadata and replica targets")
	volumes := cfg.Volumes
	if len(volumes) == 0 {
		var err error
		volumes, err = meta.ListVolumes(ctx)
		if err != nil {
			return err
		}
	}
	return bootstrapLegacyClusterMetadata(ctx, clusterRepo, volumes, replicaTargets)
}

func sortedReplicaIDs(targets map[string]replicaTarget) []string {
	ids := make([]string, 0, len(targets))
	for replicaID := range targets {
		ids = append(ids, replicaID)
	}
	sort.Strings(ids)
	return ids
}

func replicaIDsFromMap(clients map[string]service.SBSClient) []string {
	ids := make([]string, 0, len(clients))
	for replicaID := range clients {
		ids = append(ids, replicaID)
	}
	sort.Strings(ids)
	return ids
}

func defaultClusterMetadataPath(targets map[string]replicaTarget) string {
	replicaIDs := sortedReplicaIDs(targets)
	if len(replicaIDs) == 0 {
		return ""
	}
	for _, replicaID := range replicaIDs {
		firstPath := targets[replicaID].Path
		if firstPath == "" {
			continue
		}
		return filepath.Join(filepath.Dir(firstPath), "_cluster-metadata")
	}
	return ""
}

func openReplicaClient(ctx context.Context, meta service.MetadataRepository, target replicaTarget) (service.SBSClient, func(), error) {
	switch target.Kind {
	case replicaTargetLocal:
		client, err := local.Open(local.Config{Path: target.Path, BuildVersion: buildVersion})
		if err != nil {
			return nil, nil, err
		}
		if err := bootstrapSBSVolumes(ctx, meta, client); err != nil {
			_ = client.Close()
			return nil, nil, err
		}
		return client, func() { _ = client.Close() }, nil
	case replicaTargetGRPC:
		endpoint := net.JoinHostPort(target.Endpoint.Address, strconv.Itoa(int(target.Endpoint.Port)))
		var dialCreds credentials.TransportCredentials
		if target.Endpoint.UseTLS {
			dialCreds = credentials.NewTLS(&tls.Config{
				MinVersion: tls.VersionTLS12,
				ServerName: target.Endpoint.ServerName,
			})
		} else {
			dialCreds = insecure.NewCredentials()
		}
		conn, err := grpc.NewClient(endpoint, grpc.WithTransportCredentials(dialCreds))
		if err != nil {
			return nil, nil, err
		}
		return sbsgrpc.NewClient(sbsv1.NewVolumeServiceClient(conn)), func() { _ = conn.Close() }, nil
	default:
		return nil, nil, fmt.Errorf("unknown replica target kind %q", target.Kind)
	}
}

func resolveReplicaTargetsFromMetadata(ctx context.Context, repo rawReplicaTargetMetadataReader) (map[string]replicaTarget, error) {
	nodes, err := repo.ListNodeMemberships(ctx)
	if err != nil {
		return nil, err
	}
	targets := make(map[string]replicaTarget)
	nowUnix := time.Now().Unix()
	for _, node := range nodes {
		if !nodeEligibleForGatewayReplicaTarget(ctx, repo, node, nowUnix) {
			continue
		}
		targetID := strings.TrimSpace(node.ReplicaID)
		if targetID == "" {
			// For Phase F sbs-service membership, the endpoint is owned by a
			// storage node. Keep logical replica_id in placement metadata, and
			// expose this client under node_id so the replication layer can
			// resolve ReplicaTarget.NodeID without equating node_id to replica_id.
			targetID = strings.TrimSpace(node.NodeID)
		}
		if targetID == "" || len(node.SBSEndpoints) == 0 {
			continue
		}
		endpoint := node.SBSEndpoints[0]
		target := replicaTarget{
			TargetID:  targetID,
			Kind:      replicaTargetGRPC,
			Endpoint:  endpoint,
			AdminHTTP: nodeAdminHTTPEndpoint(node),
		}
		targets[targetID] = target
		nodeID := strings.TrimSpace(node.NodeID)
		if nodeID != "" && nodeID != targetID {
			nodeTarget := target
			nodeTarget.TargetID = nodeID
			targets[nodeID] = nodeTarget
		}
	}
	if len(targets) == 0 {
		return nil, fmt.Errorf("no replica targets configured and no SBS gRPC endpoints found in cluster metadata")
	}
	return targets, nil
}

func resolveReplicaTargetsFromAdmin(ctx context.Context, cfg repositoryConfig) (map[string]replicaTarget, error) {
	adminEndpoint := strings.TrimSpace(cfg.SBSAdminEndpoint)
	if adminEndpoint == "" {
		return nil, fmt.Errorf("admin endpoint is required")
	}
	dialCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	client, err := adminclient.Dial(dialCtx, adminEndpoint)
	if err != nil {
		return nil, err
	}
	defer client.Close()
	cluster := &adminv1.ClusterRef{
		ClusterId:    gatewayClusterIdentityFromConfig(cfg),
		SbsClusterId: sbsClusterIdentityFromConfig(cfg),
	}
	resp, err := client.Admin.GetReplicaTargetsView(ctx, &adminv1.GetReplicaTargetsViewRequest{
		Cluster: cluster,
	})
	if err != nil {
		return nil, err
	}
	targets, err := replicaTargetsFromPublishedView(resp.GetTargets())
	if err != nil {
		return nil, err
	}
	nodesResp, err := client.Admin.ListNodes(ctx, &adminv1.ListNodesRequest{Cluster: cluster})
	if err != nil {
		log.Printf("gateway published node membership view unavailable while adding replica target node aliases: %v", err)
		return targets, nil
	}
	addReplicaTargetNodeAliasesFromAdminNodes(targets, nodesResp.GetNodes())
	return targets, nil
}

func replicaTargetsFromPublishedView(targetViews []*adminv1.ReplicaTargetView) (map[string]replicaTarget, error) {
	targets := make(map[string]replicaTarget)
	for _, view := range targetViews {
		if view == nil || !view.GetUsable() {
			continue
		}
		targetID := strings.TrimSpace(view.GetTargetId())
		ep := view.GetEndpoint()
		if targetID == "" || ep == nil || strings.TrimSpace(ep.GetAddress()) == "" || ep.GetPort() == 0 {
			continue
		}
		target := replicaTarget{
			TargetID: targetID,
			Kind:     replicaTargetGRPC,
			Endpoint: clustermeta.SBSEndpoint{
				Address:    ep.GetAddress(),
				Port:       uint16(ep.GetPort()),
				UseTLS:     ep.GetUseTls(),
				ServerName: ep.GetServerName(),
			},
			AdminHTTP: strings.TrimSpace(view.GetAdminHttpEndpoint()),
		}
		targets[targetID] = target
	}
	if len(targets) == 0 {
		return nil, fmt.Errorf("no usable replica targets returned by published view")
	}
	return targets, nil
}

func addReplicaTargetNodeAliasesFromAdminNodes(targets map[string]replicaTarget, nodes []*adminv1.NodeSummary) {
	if len(targets) == 0 || len(nodes) == 0 {
		return
	}
	byEndpoint := make(map[string]replicaTarget, len(targets))
	byAdminHTTP := make(map[string]replicaTarget, len(targets))
	for _, target := range targets {
		if key := replicaTargetEndpointKey(target.Endpoint); key != "" {
			if _, exists := byEndpoint[key]; !exists {
				byEndpoint[key] = target
			}
		}
		if key := normalizedAdminHTTPEndpoint(target.AdminHTTP); key != "" {
			if _, exists := byAdminHTTP[key]; !exists {
				byAdminHTTP[key] = target
			}
		}
	}
	for _, node := range nodes {
		if node == nil {
			continue
		}
		nodeID := strings.TrimSpace(node.GetNodeId())
		if nodeID == "" {
			continue
		}
		if _, exists := targets[nodeID]; exists {
			continue
		}
		target, ok := byEndpoint[adminGRPCEndpointKey(node.GetGrpcEndpoint())]
		if !ok {
			target, ok = byAdminHTTP[normalizedAdminHTTPEndpoint(node.GetAdminHttpEndpoint())]
		}
		if !ok {
			continue
		}
		target.TargetID = nodeID
		targets[nodeID] = target
	}
}

func replicaTargetEndpointKey(endpoint clustermeta.SBSEndpoint) string {
	address := strings.ToLower(strings.TrimSpace(endpoint.Address))
	if address == "" || endpoint.Port == 0 {
		return ""
	}
	return net.JoinHostPort(address, strconv.Itoa(int(endpoint.Port)))
}

func adminGRPCEndpointKey(raw string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return ""
	}
	host, port, err := net.SplitHostPort(raw)
	if err != nil {
		return strings.ToLower(raw)
	}
	return net.JoinHostPort(strings.ToLower(strings.TrimSpace(host)), strings.TrimSpace(port))
}

func normalizedAdminHTTPEndpoint(raw string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return ""
	}
	return strings.TrimRight(strings.ToLower(raw), "/")
}

func newPublishedReplicaTargetAvailabilityProvider(cfg repositoryConfig) sbscluster.ReplicaTargetAvailabilityProvider {
	return sbscluster.NewPublishedReplicaTargetAvailabilityProvider(sbscluster.PublishedReplicaTargetAvailabilityOptions{
		Endpoint:     cfg.SBSAdminEndpoint,
		ClusterID:    gatewayClusterIdentityFromConfig(cfg),
		SBSClusterID: sbsClusterIdentityFromConfig(cfg),
		TTL:          cfg.VolumeCacheTTL,
	})
}

func newRawReplicaTargetAvailabilityProvider(repo rawReplicaTargetMetadataReader) sbscluster.ReplicaTargetAvailabilityProvider {
	if repo == nil {
		return nil
	}
	return sbscluster.ReplicaTargetAvailabilityFunc(func(ctx context.Context, _ string) (map[string]struct{}, error) {
		nodes, err := repo.ListNodeMemberships(ctx)
		if err != nil {
			return nil, err
		}
		if len(nodes) == 0 {
			return nil, nil
		}
		nowUnix := time.Now().Unix()
		available := make(map[string]struct{}, len(nodes)*2)
		for _, node := range nodes {
			if !nodeEligibleForGatewayReplicaTarget(ctx, repo, node, nowUnix) {
				continue
			}
			if nodeID := strings.TrimSpace(node.NodeID); nodeID != "" {
				available[nodeID] = struct{}{}
			}
			if replicaID := strings.TrimSpace(node.ReplicaID); replicaID != "" {
				available[replicaID] = struct{}{}
			}
		}
		if len(available) == 0 {
			return nil, fmt.Errorf("no usable replica targets available from raw cluster metadata")
		}
		return available, nil
	})
}

func newPublishedNodeMembershipResolver(cfg repositoryConfig, fallback rawReplicaTargetMetadataReader) sbsclusterNodeMembershipResolver {
	return sbscluster.NewPublishedNodeMembershipResolver(sbscluster.PublishedNodeMembershipOptions{
		Endpoint:         cfg.SBSAdminEndpoint,
		ClusterID:        gatewayClusterIdentityFromConfig(cfg),
		SBSClusterID:     sbsClusterIdentityFromConfig(cfg),
		Fallback:         fallback,
		AllowRawFallback: cfg.SBSClusterBootstrapMetadata,
	})
}

type sbsclusterNodeMembershipResolver interface {
	ListNodeMemberships(ctx context.Context) ([]clustermeta.NodeMembershipRecord, error)
}

func nodeEligibleForGatewayReplicaTarget(ctx context.Context, repo rawReplicaTargetMetadataReader, node clustermeta.NodeMembershipRecord, nowUnix int64) bool {
	if node.HealthState != clustermeta.NodeHealthHealthy && node.HealthState != clustermeta.NodeHealthSuspect {
		return false
	}
	detail, err := repo.GetNodeHealthDetail(ctx, strings.TrimSpace(node.NodeID))
	switch {
	case err == nil:
		return detail.RecoveryEligibleAtUnix <= nowUnix
	case errors.Is(err, clustermeta.ErrNotFound):
		return true
	default:
		return false
	}
}

type gatewayMaterializingSBSClient struct {
	next           service.SBSClient
	adminHTTP      string
	lookup         sbscluster.VolumeLookup
	materializedMu sync.Mutex
	materialized   map[string]bool
}

func newGatewayMaterializingSBSClient(next service.SBSClient, adminHTTP string, lookup sbscluster.VolumeLookup) service.SBSClient {
	adminHTTP = strings.TrimRight(strings.TrimSpace(adminHTTP), "/")
	if next == nil || adminHTTP == "" || lookup == nil {
		return next
	}
	return &gatewayMaterializingSBSClient{
		next:         next,
		adminHTTP:    adminHTTP,
		lookup:       lookup,
		materialized: make(map[string]bool),
	}
}

func (c *gatewayMaterializingSBSClient) OpenVolume(ctx context.Context, req *service.OpenVolumeRequest) (*service.OpenVolumeResponse, error) {
	resp, err := c.next.OpenVolume(ctx, req)
	if err == nil || !isSBSNotFoundError(err) || req == nil {
		return resp, err
	}
	if materializeErr := c.materialize(ctx, req.VolumeID); materializeErr != nil {
		return nil, fmt.Errorf("%w; materialize target volume: %v", err, materializeErr)
	}
	return c.next.OpenVolume(ctx, req)
}

func (c *gatewayMaterializingSBSClient) materialize(ctx context.Context, volumeID string) error {
	c.materializedMu.Lock()
	defer c.materializedMu.Unlock()
	if c.materialized[volumeID] {
		return nil
	}
	parsedID, err := volumeid.Parse(volumeID)
	if err != nil {
		return err
	}
	spec, err := c.lookup.GetVolume(ctx, parsedID)
	if err != nil {
		return err
	}
	q := url.Values{}
	q.Set("volume_id", service.CanonicalVolumeID(parsedID))
	q.Set("size_bytes", strconv.FormatUint(spec.SizeBytes, 10))
	q.Set("block_size", strconv.FormatUint(uint64(spec.BlockSize), 10))
	q.Set("prefix", strings.TrimSpace(spec.Prefix))
	if spec.ChunkSizeBytes != 0 {
		q.Set("chunk_size_bytes", strconv.FormatUint(uint64(spec.ChunkSizeBytes), 10))
	}
	if spec.ExtentPageBytes != 0 {
		q.Set("extent_page_bytes", strconv.FormatUint(uint64(spec.ExtentPageBytes), 10))
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, c.adminHTTP+"/debug/materialize-volume?"+q.Encode(), nil)
	if err != nil {
		return err
	}
	resp, err := gatewayMaterializeHTTPDo(httpReq)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		return fmt.Errorf("materialize volume returned status %s", resp.Status)
	}
	c.materialized[volumeID] = true
	return nil
}

func (c *gatewayMaterializingSBSClient) CloseVolume(ctx context.Context, req *service.CloseVolumeRequest) (*service.CloseVolumeResponse, error) {
	return c.next.CloseVolume(ctx, req)
}

func (c *gatewayMaterializingSBSClient) GetVolumeProfile(ctx context.Context, req *service.GetVolumeProfileRequest) (*service.GetVolumeProfileResponse, error) {
	return c.next.GetVolumeProfile(ctx, req)
}

func (c *gatewayMaterializingSBSClient) GetVolumeStatus(ctx context.Context, req *service.GetVolumeStatusRequest) (*service.GetVolumeStatusResponse, error) {
	return c.next.GetVolumeStatus(ctx, req)
}

func (c *gatewayMaterializingSBSClient) Read(ctx context.Context, req *service.ReadRequest) (*service.ReadResponse, error) {
	return c.next.Read(ctx, req)
}

func (c *gatewayMaterializingSBSClient) ReadClone(ctx context.Context, cloneID string, req *service.ReadRequest) (*service.ReadResponse, error) {
	next, ok := c.next.(interface {
		ReadClone(context.Context, string, *service.ReadRequest) (*service.ReadResponse, error)
	})
	if !ok {
		return nil, service.ErrNotSupported
	}
	return next.ReadClone(ctx, cloneID, req)
}

func (c *gatewayMaterializingSBSClient) ReadSnapshot(ctx context.Context, snapshotID string, req *service.ReadRequest) (*service.ReadResponse, error) {
	next, ok := c.next.(interface {
		ReadSnapshot(context.Context, string, *service.ReadRequest) (*service.ReadResponse, error)
	})
	if !ok {
		return nil, service.ErrNotSupported
	}
	return next.ReadSnapshot(ctx, snapshotID, req)
}

func (c *gatewayMaterializingSBSClient) Write(ctx context.Context, req *service.WriteRequest) (*service.WriteResponse, error) {
	return c.next.Write(ctx, req)
}

func (c *gatewayMaterializingSBSClient) ReadPhysicalChunk(ctx context.Context, req *service.ReadPhysicalChunkRequest) (*service.ReadPhysicalChunkResponse, error) {
	next, ok := c.next.(service.PhysicalChunkSBSClient)
	if !ok {
		return nil, service.ErrNotSupported
	}
	return next.ReadPhysicalChunk(ctx, req)
}

func (c *gatewayMaterializingSBSClient) WritePhysicalChunk(ctx context.Context, req *service.WritePhysicalChunkRequest) (*service.WritePhysicalChunkResponse, error) {
	next, ok := c.next.(service.PhysicalChunkSBSClient)
	if !ok {
		return nil, service.ErrNotSupported
	}
	return next.WritePhysicalChunk(ctx, req)
}

func (c *gatewayMaterializingSBSClient) WriteECShard(ctx context.Context, req *service.WriteECShardRequest) (*service.WriteECShardResponse, error) {
	next, ok := c.next.(service.ECShardSBSClient)
	if !ok {
		return nil, service.ErrNotSupported
	}
	return next.WriteECShard(ctx, req)
}

func (c *gatewayMaterializingSBSClient) ReadECShard(ctx context.Context, req *service.ReadECShardRequest) (*service.ReadECShardResponse, error) {
	next, ok := c.next.(service.ECShardSBSClient)
	if !ok {
		return nil, service.ErrNotSupported
	}
	return next.ReadECShard(ctx, req)
}

func (c *gatewayMaterializingSBSClient) DeleteECShard(ctx context.Context, req *service.DeleteECShardRequest) (*service.DeleteECShardResponse, error) {
	next, ok := c.next.(service.ECShardSBSClient)
	if !ok {
		return nil, service.ErrNotSupported
	}
	return next.DeleteECShard(ctx, req)
}

func (c *gatewayMaterializingSBSClient) WriteClone(ctx context.Context, cloneID string, req *service.WriteRequest) (*service.WriteResponse, error) {
	next, ok := c.next.(interface {
		WriteClone(context.Context, string, *service.WriteRequest) (*service.WriteResponse, error)
	})
	if !ok {
		return nil, service.ErrNotSupported
	}
	return next.WriteClone(ctx, cloneID, req)
}

func (c *gatewayMaterializingSBSClient) Flush(ctx context.Context, req *service.FlushRequest) (*service.FlushResponse, error) {
	return c.next.Flush(ctx, req)
}

func (c *gatewayMaterializingSBSClient) Discard(ctx context.Context, req *service.DiscardRequest) (*service.DiscardResponse, error) {
	return c.next.Discard(ctx, req)
}

func (c *gatewayMaterializingSBSClient) Zero(ctx context.Context, req *service.ZeroRequest) (*service.ZeroResponse, error) {
	return c.next.Zero(ctx, req)
}

func isSBSNotFoundError(err error) bool {
	var sbsErr *service.SBSError
	return errors.As(err, &sbsErr) && sbsErr.Code == service.SBSErrorCodeNotFound
}

func targetAdminHTTPEndpoint(target replicaTarget) string {
	if strings.TrimSpace(target.AdminHTTP) != "" {
		return target.AdminHTTP
	}
	if target.Endpoint.Address == "" {
		return ""
	}
	// Phase F lab default: sbs-data HTTP observability/materialization port.
	return fmt.Sprintf("http://%s:%d", target.Endpoint.Address, 9082)
}

func nodeAdminHTTPEndpoint(node clustermeta.NodeMembershipRecord) string {
	if strings.TrimSpace(node.AdminHTTPEndpoint) != "" {
		return node.AdminHTTPEndpoint
	}
	if len(node.SBSEndpoints) == 0 || node.SBSEndpoints[0].Address == "" {
		return ""
	}
	// Phase F lab default: sbs-data HTTP observability/materialization port.
	return fmt.Sprintf("http://%s:%d", node.SBSEndpoints[0].Address, 9082)
}

func runtimeSBSClusterMetadataConfigFromRepositoryConfig(cfg repositoryConfig) runtimeSBSClusterMetadataConfig {
	root := strings.TrimSpace(cfg.SBSClusterMetadataRoot)
	if root == "" {
		root = "sbs/cluster"
	}
	backend := effectiveSBSClusterMetadataBackend(cfg)
	clusterMetadataPath := ""
	if backend == "pebble" {
		clusterMetadataPath = cfg.SBSClusterMetadataPath
		if clusterMetadataPath == "" {
			clusterMetadataPath = defaultClusterMetadataPath(cfg.SBSClusterReplicas)
		}
	}
	return runtimeSBSClusterMetadataConfig{
		Backend:    backend,
		Root:       root,
		PebblePath: clusterMetadataPath,
	}
}

func openRuntimeSBSClusterMetadataRepository(ctx context.Context, cfg runtimeSBSClusterMetadataConfig) (runtimeClusterMetadataHandle, string, func(), error) {
	backend := strings.TrimSpace(cfg.Backend)
	root := strings.TrimSpace(cfg.Root)
	if root == "" {
		root = "sbs/cluster"
	}

	switch backend {
	case "pebble":
		clusterMetadataPath := strings.TrimSpace(cfg.PebblePath)
		if clusterMetadataPath == "" {
			return nil, "", nil, fmt.Errorf("cluster metadata path is required for pebble backend")
		}
		clusterKV, err := openClusterMetadataPebble(clusterMetadataPath)
		if err != nil {
			return nil, "", nil, err
		}
		return clustermeta.NewRepository(clusterKV, root), "pebble path=" + clusterMetadataPath + " root=" + root, func() { _ = clusterKV.Close() }, nil
	default:
		return nil, "", nil, fmt.Errorf("invalid --sbs-cluster-metadata-backend %q: must be pebble", backend)
	}
}

func bootstrapLegacyClusterMetadata(ctx context.Context, repo legacyClusterBootstrapWriter, volumes []service.VolumeSpec, replicaTargets map[string]replicaTarget) error {
	replicaIDs := sortedReplicaIDs(replicaTargets)
	for _, replicaID := range replicaIDs {
		target := replicaTargets[replicaID]
		nodeID := "node-" + replicaID
		rec, err := repo.GetNodeMembership(ctx, nodeID)
		if err != nil && !errors.Is(err, clustermeta.ErrNotFound) {
			return err
		}
		if errors.Is(err, clustermeta.ErrNotFound) {
			rec = clustermeta.NodeMembershipRecord{
				NodeID:         nodeID,
				ReplicaID:      replicaID,
				LifecycleState: clustermeta.NodeLifecycleActive,
				HealthState:    clustermeta.NodeHealthHealthy,
				Host:           "host-" + replicaID,
				Version:        buildVersion,
				Capabilities:   []string{"replication", "sbs-cluster"},
			}
		}
		rec.ReplicaID = replicaID
		if target.Kind == replicaTargetLocal {
			rec.Capabilities = appendUnique(rec.Capabilities, "local-path")
		}
		if target.Kind == replicaTargetGRPC && target.Endpoint.Address != "" && target.Endpoint.Port != 0 {
			rec.Capabilities = appendUnique(rec.Capabilities, "sbs-grpc")
			rec.SBSEndpoints = []clustermeta.SBSEndpoint{target.Endpoint}
		}
		if err := repo.PutNodeMembership(ctx, rec); err != nil {
			return err
		}
	}
	for _, volume := range volumes {
		volumeID := service.CanonicalVolumeID(uint64(volume.ID))
		if err := repo.PutVolumeSpec(ctx, clustermeta.VolumeSpecRecord{
			VolumeID:          volumeID,
			SizeBytes:         volume.SizeBytes,
			BlockSize:         volume.BlockSize,
			ChunkSizeBytes:    volume.ChunkSizeBytes,
			ExtentPageBytes:   volume.ExtentPageBytes,
			ExtentSizeBytes:   uint64(volume.ExtentPageBytes),
			ReplicationFactor: uint32(len(replicaIDs)),
			PolicyName:        "gateway-bootstrap-v1",
			CreatedBy:         "namrbd-gateway",
			CreatedReason:     "legacy-dev-bootstrap",
			CreatedAtUnix:     time.Now().Unix(),
		}); err != nil {
			return err
		}
		if _, err := repo.GetVolumeState(ctx, volumeID); err != nil {
			if !errors.Is(err, clustermeta.ErrNotFound) {
				return err
			}
			if err := repo.PutVolumeState(ctx, clustermeta.VolumeState{
				VolumeID:          volumeID,
				Epoch:             1,
				Revision:          1,
				PlacementPolicyID: "gateway-bootstrap-v1",
				ProtectionPolicy:  fmt.Sprintf("rf%d", len(replicaIDs)),
				Status:            clustermeta.VolumeStatusHealthy,
			}); err != nil {
				return err
			}
		}

		extentLength := uint64(volume.ChunkSizeBytes)
		if extentLength == 0 {
			extentLength = uint64(volume.BlockSize)
		}
		if extentLength == 0 {
			extentLength = service.DefaultAllocationChunkSize
		}
		if extentLength > volume.SizeBytes {
			extentLength = volume.SizeBytes
		}
		replicas := make([]clustermeta.ReplicaDescriptor, 0, len(replicaIDs))
		failureDomains := make([]string, 0, len(replicaIDs))
		for i, replicaID := range replicaIDs {
			role := clustermeta.ReplicaRoleSecondary
			if i == 0 {
				role = clustermeta.ReplicaRolePrimary
			}
			failureDomain := "host-" + replicaID
			replicas = append(replicas, clustermeta.ReplicaDescriptor{
				NodeID:        "node-" + replicaID,
				ReplicaID:     replicaID,
				Role:          role,
				FailureDomain: failureDomain,
			})
			failureDomains = append(failureDomains, failureDomain)
		}

		replicaSetID := "rs-" + volumeID
		if _, err := repo.GetReplicaSet(ctx, volumeID, replicaSetID); err != nil {
			if !errors.Is(err, clustermeta.ErrNotFound) {
				return err
			}
			if err := repo.PutReplicaSet(ctx, clustermeta.ReplicaSetState{
				ReplicaSetID:     replicaSetID,
				VolumeID:         volumeID,
				PlacementRef:     replicaSetID,
				Epoch:            1,
				Replicas:         replicas,
				PrimaryReplicaID: replicaIDs[0],
				WriteQuorum:      2,
				ReadQuorum:       1,
				FailureDomains:   failureDomains,
			}); err != nil {
				return err
			}
		}

		var extentID uint64 = 1
		var chunkID uint64 = 1
		for offset := uint64(0); offset < volume.SizeBytes; offset += extentLength {
			length := extentLength
			if remaining := volume.SizeBytes - offset; remaining < length {
				length = remaining
			}
			if _, err := repo.GetExtentMapping(ctx, volumeID, extentID); err != nil {
				if !errors.Is(err, clustermeta.ErrNotFound) {
					return err
				}
				if err := repo.PutExtentMapping(ctx, clustermeta.ExtentMappingRecord{
					VolumeID:      volumeID,
					ExtentID:      extentID,
					LogicalOffset: offset,
					LengthBytes:   length,
					ChunkID:       chunkID,
					PlacementRef:  replicaSetID,
					Revision:      1,
				}); err != nil {
					return err
				}
			}
			extentID++
			chunkID++
		}
	}
	return nil
}

func appendUnique(values []string, next string) []string {
	for _, value := range values {
		if value == next {
			return values
		}
	}
	return append(values, next)
}

func defaultGatewayID() string {
	host, err := os.Hostname()
	if err != nil || host == "" {
		return "gw-local"
	}
	return host
}

func parseVolumes(spec string) ([]store.Volume, error) {
	spec = strings.TrimSpace(spec)
	if spec == "" {
		return nil, nil
	}
	items := strings.Split(spec, ";")
	out := make([]store.Volume, 0, len(items))
	for _, item := range items {
		parts := strings.Split(item, ",")
		if len(parts) != 3 {
			return nil, strconv.ErrSyntax
		}
		id, err := volumeid.Parse(parts[0])
		if err != nil {
			return nil, err
		}
		sizeBytes, err := strconv.ParseUint(parts[2], 10, 64)
		if err != nil {
			return nil, err
		}
		out = append(out, store.Volume{
			ID:        id,
			Prefix:    parts[1],
			SizeBytes: sizeBytes,
		})
	}
	return out, nil
}
