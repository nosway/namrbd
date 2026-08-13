package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
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
	"github.com/nosway/namrbd/sbs/local"
	sbsv1 "github.com/nosway/namrbd/sbs/v1"
	namrbdversion "github.com/nosway/namrbd/version"

	"google.golang.org/grpc"
)

var buildVersion = namrbdversion.Current

func main() {
	for _, arg := range os.Args[1:] {
		if arg == "--version" || arg == "version" {
			fmt.Println(buildVersion)
			return
		}
	}
	fs := flag.NewFlagSet("sbs-data", flag.ExitOnError)
	path := fs.String("path", getenvOrDefault("NAMRBD_SBS_DATA_DIR", "./var/sbs-data"), "local sbs-data path")
	storeConfigPath := fs.String("store-config", getenvOrDefault("NAMRBD_SBS_STORE_CONFIG", ""), "YAML store config file path")
	var storeSpecs storeFlag
	fs.Var(&storeSpecs, "store", "payload store spec path:shards=N,weight=W[,id=ID] (repeatable)")
	grpcListen := fs.String("grpc-listen", getenvOrDefault("NAMRBD_SBS_GRPC_ADDR", "0.0.0.0:9444"), "listen address for sbs-data gRPC")
	httpListen := fs.String("http-listen", getenvOrDefault("NAMRBD_BIND_ADDR", "0.0.0.0:9082"), "listen address for HTTP health/debug")
	enableLabStoreDebug := fs.Bool("enable-lab-store-debug", getenvBoolOrDefault("NAMRBD_SBS_ENABLE_LAB_STORE_DEBUG", false), "enable lab-only debug store mutation endpoints")
	disableIdempotencySync := fs.Bool("lab-disable-idempotency-sync", getenvBoolOrDefault("NAMRBD_SBS_LAB_DISABLE_IDEMPOTENCY_SYNC", false), "lab-only: write idempotency records without Pebble sync")
	cacheOpenVolumeSpec := fs.Bool("lab-cache-open-volume-spec", getenvBoolOrDefault("NAMRBD_SBS_LAB_CACHE_OPEN_VOLUME_SPEC", false), "lab-only: reuse the opened volume spec on hot data-plane requests")
	disablePhysicalWriteIdempotency := fs.Bool("lab-disable-physical-write-idempotency", getenvBoolOrDefault("NAMRBD_SBS_LAB_DISABLE_PHYSICAL_WRITE_IDEMPOTENCY", false), "lab-only: skip durable idempotency lookup/store for fresh physical chunk writes")
	dataOperationTrace := fs.Bool("data-operation-trace", getenvBoolOrDefault("NAMRBD_SBS_DATA_OPERATION_TRACE", false), "lab-only: emit structured sbs-data read/write success trace events")
	fs.Parse(os.Args[1:])

	if strings.TrimSpace(*storeConfigPath) != "" && len(storeSpecs) > 0 {
		log.Fatalf("--store-config and -store cannot be used together")
	}
	if strings.TrimSpace(*storeConfigPath) != "" {
		loadedStores, err := local.LoadStoreConfigFile(*storeConfigPath)
		if err != nil {
			log.Fatalf("load store config %s: %v", *storeConfigPath, err)
		}
		storeSpecs = storeFlag(loadedStores)
	}

	client, err := local.Open(local.Config{
		Path:                            *path,
		Stores:                          storeSpecs,
		BuildVersion:                    buildVersion,
		DisableIdempotencySync:          *disableIdempotencySync,
		CacheOpenVolumeSpec:             *cacheOpenVolumeSpec,
		DisablePhysicalWriteIdempotency: *disablePhysicalWriteIdempotency,
		TraceDataOperations:             *dataOperationTrace,
	})
	if err != nil {
		log.Fatalf("open local sbs-data path %s: %v", *path, err)
	}
	defer client.Close()

	grpcLn, err := net.Listen("tcp", *grpcListen)
	if err != nil {
		log.Fatalf("listen gRPC %s: %v", *grpcListen, err)
	}
	grpcSrv := grpc.NewServer()
	sbsv1.RegisterVolumeServiceServer(grpcSrv, sbsgrpc.NewServer(client))

	httpSrv := &http.Server{
		Addr:    *httpListen,
		Handler: observabilityMux(*path, *storeConfigPath, client, *enableLabStoreDebug),
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	go func() {
		log.Printf("sbs-data gRPC listening on %s", *grpcListen)
		if err := grpcSrv.Serve(grpcLn); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			log.Fatalf("serve gRPC: %v", err)
		}
	}()
	go func() {
		log.Printf("sbs-data HTTP observability listening on %s", *httpListen)
		if err := httpSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("serve HTTP: %v", err)
		}
	}()

	<-ctx.Done()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = httpSrv.Shutdown(shutdownCtx)
	grpcSrv.GracefulStop()
}

func observabilityMux(path, storeConfigPath string, client *local.Client, enableLabStoreDebug bool) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok\n"))
	})
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok\n"))
	})
	mux.HandleFunc("/debug/summary", func(w http.ResponseWriter, _ *http.Request) {
		snapshot, err := client.ObservabilitySnapshot()
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"path":                path,
			"build_version":       snapshot.BuildVersion,
			"volumes":             snapshot.Volumes,
			"open_sessions":       snapshot.OpenSessions,
			"allocation_pages":    snapshot.ExtentPages,
			"extent_pages":        snapshot.ExtentPages,
			"garbage_chunks":      snapshot.GarbageChunks,
			"idempotency_records": snapshot.IdempotencyRecords,
			"stores":              snapshot.Stores,
			"timings":             snapshot.Timings,
		})
	})
	mux.HandleFunc("/debug/store-health", func(w http.ResponseWriter, _ *http.Request) {
		snapshot, err := client.StoreHealthSnapshot()
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"path":          path,
			"build_version": snapshot.BuildVersion,
			"stores":        snapshot.Stores,
			"timings":       snapshot.Timings,
		})
	})
	mux.HandleFunc("/debug/materialize-volume", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		q := r.URL.Query()
		volumeID := q.Get("volume_id")
		sizeBytes, err := strconv.ParseUint(q.Get("size_bytes"), 10, 64)
		if err != nil || sizeBytes == 0 {
			http.Error(w, "size_bytes must be a positive integer", http.StatusBadRequest)
			return
		}
		blockSize, err := strconv.ParseUint(q.Get("block_size"), 10, 32)
		if err != nil || blockSize == 0 {
			http.Error(w, "block_size must be a positive integer", http.StatusBadRequest)
			return
		}
		chunkSizeBytes, err := optionalPositiveUint32(q, "allocation_chunk_size_bytes", "chunk_size_bytes")
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		extentPageBytes, err := optionalPositiveUint32(q, "allocation_page_bytes", "extent_page_bytes", "allocation_extent_page_bytes")
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		parsedID, err := service.ParseVolumeID(volumeID)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		prefix := q.Get("prefix")
		if prefix == "" {
			prefix = "sbs-" + volumeID
		}
		spec, err := client.CreateVolume(r.Context(), service.VolumeSpec{
			ID:              service.HexVolumeID(parsedID),
			Name:            prefix,
			Prefix:          prefix,
			SizeBytes:       sizeBytes,
			BlockSize:       uint32(blockSize),
			ChunkSizeBytes:  chunkSizeBytes,
			ExtentPageBytes: extentPageBytes,
		})
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"ok":                          true,
			"volume_id":                   volumeID,
			"size_bytes":                  spec.SizeBytes,
			"block_size":                  spec.BlockSize,
			"allocation_chunk_size_bytes": spec.ChunkSizeBytes,
			"allocation_page_bytes":       spec.ExtentPageBytes,
			"chunk_size_bytes":            spec.ChunkSizeBytes,
			"extent_page_bytes":           spec.ExtentPageBytes,
		})
	})
	handleAllocationPagesDebug := func(w http.ResponseWriter, r *http.Request) {
		volumeID := strings.TrimSpace(r.URL.Query().Get("volume_id"))
		if volumeID == "" {
			http.Error(w, "volume_id is required", http.StatusBadRequest)
			return
		}
		pages, err := client.ListExtentPages(r.Context(), volumeID)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"volume_id": volumeID,
			"pages":     pages,
		})
	}
	mux.HandleFunc("/debug/allocation-pages", handleAllocationPagesDebug)
	mux.HandleFunc("/debug/extent-pages", handleAllocationPagesDebug)
	mux.HandleFunc("/debug/store-shards", func(w http.ResponseWriter, r *http.Request) {
		snapshots, err := client.ShardSnapshots()
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"shards": snapshots,
		})
	})
	mux.HandleFunc("/debug/write-pattern", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		q := r.URL.Query()
		volumeID := strings.TrimSpace(q.Get("volume_id"))
		if volumeID == "" {
			http.Error(w, "volume_id is required", http.StatusBadRequest)
			return
		}
		offsetBytes, err := strconv.ParseUint(q.Get("offset_bytes"), 10, 64)
		if err != nil {
			http.Error(w, "offset_bytes must be a non-negative integer", http.StatusBadRequest)
			return
		}
		lengthBytes, err := strconv.ParseUint(q.Get("length_bytes"), 10, 64)
		if err != nil || lengthBytes == 0 {
			http.Error(w, "length_bytes must be a positive integer", http.StatusBadRequest)
			return
		}
		fillByte, err := parseDebugFillByte(q.Get("fill_byte"))
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		handle, err := ensureDebugVolumeOpen(r.Context(), client, volumeID)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		payload := make([]byte, lengthBytes)
		for i := range payload {
			payload[i] = fillByte
		}
		resp, err := client.Write(r.Context(), &service.WriteRequest{
			VolumeID:     volumeID,
			VolumeHandle: handle,
			OffsetBytes:  offsetBytes,
			LengthBytes:  lengthBytes,
			Data:         payload,
			Context:      debugWriterContext(volumeID, offsetBytes, lengthBytes, fillByte),
		})
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"ok":              true,
			"volume_id":       volumeID,
			"volume_handle":   handle,
			"offset_bytes":    offsetBytes,
			"length_bytes":    lengthBytes,
			"fill_byte":       fmt.Sprintf("0x%02x", fillByte),
			"volume_revision": resp.VolumeRevision,
		})
	})
	if enableLabStoreDebug {
		mux.HandleFunc("/debug/chunk-gc", func(w http.ResponseWriter, r *http.Request) {
			if r.Method != http.MethodPost {
				http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
				return
			}
			var payload struct {
				VolumeID      string                     `json:"volume_id"`
				Limit         int                        `json:"limit"`
				ProtectedRefs []service.PhysicalChunkRef `json:"protected_refs"`
			}
			if r.Body != nil {
				if err := json.NewDecoder(r.Body).Decode(&payload); err != nil && !errors.Is(err, io.EOF) {
					http.Error(w, fmt.Sprintf("decode request: %v", err), http.StatusBadRequest)
					return
				}
			}
			if payload.VolumeID == "" {
				payload.VolumeID = strings.TrimSpace(r.URL.Query().Get("volume_id"))
			}
			if payload.Limit == 0 {
				limitText := strings.TrimSpace(r.URL.Query().Get("limit"))
				if limitText != "" {
					limit, err := strconv.Atoi(limitText)
					if err != nil || limit < 0 {
						http.Error(w, "limit must be a non-negative integer", http.StatusBadRequest)
						return
					}
					payload.Limit = limit
				}
			}
			if payload.VolumeID == "" {
				http.Error(w, "volume_id is required", http.StatusBadRequest)
				return
			}
			for _, ref := range payload.ProtectedRefs {
				if ref.ChunkID == 0 {
					http.Error(w, "protected_refs chunk_id must be positive", http.StatusBadRequest)
					return
				}
			}
			result, err := client.SweepChunkGarbage(r.Context(), payload.VolumeID, payload.Limit, payload.ProtectedRefs)
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			_ = json.NewEncoder(w).Encode(map[string]any{
				"ok":             true,
				"volume_id":      payload.VolumeID,
				"protected_refs": payload.ProtectedRefs,
				"result":         result,
			})
		})
		mux.HandleFunc("/debug/store-state", func(w http.ResponseWriter, r *http.Request) {
			if r.Method != http.MethodPost {
				http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
				return
			}
			storeID := strings.TrimSpace(r.URL.Query().Get("store_id"))
			state := strings.TrimSpace(r.URL.Query().Get("state"))
			if storeID == "" {
				http.Error(w, "store_id is required", http.StatusBadRequest)
				return
			}
			if state == "" {
				http.Error(w, "state is required", http.StatusBadRequest)
				return
			}
			if err := client.SetStoreState(storeID, state); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			snapshot, err := client.ObservabilitySnapshot()
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			_ = json.NewEncoder(w).Encode(map[string]any{
				"ok":       true,
				"store_id": storeID,
				"state":    state,
				"stores":   snapshot.Stores,
			})
		})
		mux.HandleFunc("/debug/store-config-reload", func(w http.ResponseWriter, r *http.Request) {
			if r.Method != http.MethodPost {
				http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
				return
			}
			configPath := strings.TrimSpace(r.URL.Query().Get("path"))
			if configPath == "" {
				configPath = strings.TrimSpace(storeConfigPath)
			}
			if configPath == "" {
				http.Error(w, "reload path is required; set path query parameter or start with --store-config", http.StatusBadRequest)
				return
			}
			if err := client.ReloadStoreConfigFile(configPath); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			snapshot, err := client.ObservabilitySnapshot()
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			_ = json.NewEncoder(w).Encode(map[string]any{
				"ok":          true,
				"config_path": configPath,
				"stores":      snapshot.Stores,
			})
		})
	}
	handleStoreWeights := func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var payload struct {
			Stores []local.StoreWeightUpdate `json:"stores"`
		}
		dec := json.NewDecoder(r.Body)
		dec.DisallowUnknownFields()
		if err := dec.Decode(&payload); err != nil {
			http.Error(w, fmt.Sprintf("decode request: %v", err), http.StatusBadRequest)
			return
		}
		configPath := strings.TrimSpace(storeConfigPath)
		persisted := false
		if configPath != "" {
			stores, err := local.UpdateStoreWeightsInConfigFile(configPath, payload.Stores)
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			if err := client.ReloadStoreConfig(stores); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			persisted = true
		} else {
			if err := client.ReloadStoreWeights(payload.Stores); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
		}
		snapshot, err := client.ObservabilitySnapshot()
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"ok":          true,
			"persisted":   persisted,
			"config_path": configPath,
			"stores":      snapshot.Stores,
		})
	}
	mux.HandleFunc("/debug/store-weights", handleStoreWeights)
	mux.HandleFunc("/admin/store-weights", handleStoreWeights)
	handleStoreTuning := func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var payload struct {
			Stores []local.StoreTuningUpdate `json:"stores"`
		}
		dec := json.NewDecoder(r.Body)
		dec.DisallowUnknownFields()
		if err := dec.Decode(&payload); err != nil {
			http.Error(w, fmt.Sprintf("decode request: %v", err), http.StatusBadRequest)
			return
		}
		configPath := strings.TrimSpace(storeConfigPath)
		persisted := false
		if configPath != "" {
			stores, err := local.UpdateStoreTuningInConfigFile(configPath, payload.Stores)
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			if err := client.ReloadStoreConfig(stores); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			persisted = true
		} else {
			if err := client.ReloadStoreTuning(payload.Stores); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
		}
		snapshot, err := client.ObservabilitySnapshot()
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"ok":          true,
			"persisted":   persisted,
			"config_path": configPath,
			"stores":      snapshot.Stores,
		})
	}
	mux.HandleFunc("/debug/store-tuning", handleStoreTuning)
	mux.HandleFunc("/admin/store-tuning", handleStoreTuning)
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, _ *http.Request) {
		snapshot, err := client.ObservabilitySnapshot()
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		_, _ = fmt.Fprintln(w, "# HELP sbs_data_ready Whether the sbs-data process is ready.")
		_, _ = fmt.Fprintln(w, "# TYPE sbs_data_ready gauge")
		_, _ = fmt.Fprintln(w, "sbs_data_ready 1")
		_, _ = fmt.Fprintln(w, "# HELP sbs_data_volumes_total Number of locally known volumes.")
		_, _ = fmt.Fprintln(w, "# TYPE sbs_data_volumes_total gauge")
		_, _ = fmt.Fprintf(w, "sbs_data_volumes_total %d\n", snapshot.Volumes)
		_, _ = fmt.Fprintln(w, "# HELP sbs_data_open_sessions Number of currently open writer sessions.")
		_, _ = fmt.Fprintln(w, "# TYPE sbs_data_open_sessions gauge")
		_, _ = fmt.Fprintf(w, "sbs_data_open_sessions %d\n", snapshot.OpenSessions)
		_, _ = fmt.Fprintln(w, "# HELP sbs_data_allocation_pages_total Number of persisted allocation page metadata records.")
		_, _ = fmt.Fprintln(w, "# TYPE sbs_data_allocation_pages_total gauge")
		_, _ = fmt.Fprintf(w, "sbs_data_allocation_pages_total %d\n", snapshot.ExtentPages)
		_, _ = fmt.Fprintln(w, "# HELP sbs_data_extent_pages_total Number of persisted extent page metadata records. Deprecated alias for sbs_data_allocation_pages_total.")
		_, _ = fmt.Fprintln(w, "# TYPE sbs_data_extent_pages_total gauge")
		_, _ = fmt.Fprintf(w, "sbs_data_extent_pages_total %d\n", snapshot.ExtentPages)
		_, _ = fmt.Fprintln(w, "# HELP sbs_data_garbage_chunks_total Number of garbage chunk records pending cleanup.")
		_, _ = fmt.Fprintln(w, "# TYPE sbs_data_garbage_chunks_total gauge")
		_, _ = fmt.Fprintf(w, "sbs_data_garbage_chunks_total %d\n", snapshot.GarbageChunks)
		_, _ = fmt.Fprintln(w, "# HELP sbs_data_idempotency_records_total Number of stored idempotency records.")
		_, _ = fmt.Fprintln(w, "# TYPE sbs_data_idempotency_records_total gauge")
		_, _ = fmt.Fprintf(w, "sbs_data_idempotency_records_total %d\n", snapshot.IdempotencyRecords)
		_, _ = fmt.Fprintln(w, "# HELP sbs_data_store_state Store health state as a numeric gauge.")
		_, _ = fmt.Fprintln(w, "# TYPE sbs_data_store_state gauge")
		_, _ = fmt.Fprintln(w, "# HELP sbs_data_store_allocation_weight Store allocation weight.")
		_, _ = fmt.Fprintln(w, "# TYPE sbs_data_store_allocation_weight gauge")
		_, _ = fmt.Fprintln(w, "# HELP sbs_data_store_capacity_bytes Store filesystem capacity in bytes.")
		_, _ = fmt.Fprintln(w, "# TYPE sbs_data_store_capacity_bytes gauge")
		_, _ = fmt.Fprintln(w, "# HELP sbs_data_store_available_bytes Store filesystem available bytes.")
		_, _ = fmt.Fprintln(w, "# TYPE sbs_data_store_available_bytes gauge")
		_, _ = fmt.Fprintln(w, "# HELP sbs_data_store_pebble_disk_usage_bytes Store Pebble disk usage in bytes.")
		_, _ = fmt.Fprintln(w, "# TYPE sbs_data_store_pebble_disk_usage_bytes gauge")
		_, _ = fmt.Fprintln(w, "# HELP sbs_data_store_compaction_pending_bytes Estimated bytes that need compaction.")
		_, _ = fmt.Fprintln(w, "# TYPE sbs_data_store_compaction_pending_bytes gauge")
		_, _ = fmt.Fprintln(w, "# HELP sbs_data_store_compaction_in_progress_bytes Bytes in in-progress compactions.")
		_, _ = fmt.Fprintln(w, "# TYPE sbs_data_store_compaction_in_progress_bytes gauge")
		for _, store := range snapshot.Stores {
			for shardID := 0; shardID < store.Shards; shardID++ {
				_, _ = fmt.Fprintf(w, "sbs_data_store_state{store_id=%q,shard_id=%q,state=%q} 1\n", store.ID, fmt.Sprintf("%d", shardID), store.State)
			}
			_, _ = fmt.Fprintf(w, "sbs_data_store_allocation_weight{store_id=%q} %d\n", store.ID, store.Weight)
			_, _ = fmt.Fprintf(w, "sbs_data_store_capacity_bytes{store_id=%q} %d\n", store.ID, store.CapacityBytes)
			_, _ = fmt.Fprintf(w, "sbs_data_store_available_bytes{store_id=%q} %d\n", store.ID, store.AvailableBytes)
			_, _ = fmt.Fprintf(w, "sbs_data_store_pebble_disk_usage_bytes{store_id=%q} %d\n", store.ID, store.PebbleDiskUsageBytes)
			_, _ = fmt.Fprintf(w, "sbs_data_store_compaction_pending_bytes{store_id=%q} %d\n", store.ID, store.CompactionPendingBytes)
			_, _ = fmt.Fprintf(w, "sbs_data_store_compaction_in_progress_bytes{store_id=%q} %d\n", store.ID, store.CompactionInProgressBytes)
		}
	})
	return mux
}

func ensureDebugVolumeOpen(ctx context.Context, client *local.Client, volumeID string) (string, error) {
	resp, err := client.OpenVolume(ctx, &service.OpenVolumeRequest{
		VolumeID:   volumeID,
		AccessMode: service.SBSAccessModeExclusiveWriter,
		Context:    debugWriterContext(volumeID, 0, 0, 0),
	})
	if err != nil {
		return "", err
	}
	return resp.VolumeHandle, nil
}

func debugWriterContext(volumeID string, offsetBytes, lengthBytes uint64, fillByte byte) service.SBSRequestContext {
	return service.SBSRequestContext{
		RequestID:      fmt.Sprintf("debug-write-%s-%d-%d-%02x", volumeID, offsetBytes, lengthBytes, fillByte),
		GatewayID:      "debug",
		HostID:         "debug",
		AttachmentID:   "debug-writer",
		Generation:     1,
		IdempotencyKey: fmt.Sprintf("debug-write-%s-%d-%d-%02x", volumeID, offsetBytes, lengthBytes, fillByte),
		TraceID:        "debug",
	}
}

func parseDebugFillByte(raw string) (byte, error) {
	if strings.TrimSpace(raw) == "" {
		return 0xab, nil
	}
	raw = strings.TrimSpace(raw)
	raw = strings.TrimPrefix(raw, "0x")
	parsed, err := strconv.ParseUint(raw, 16, 8)
	if err != nil {
		return 0, fmt.Errorf("fill_byte must be an 8-bit hex value")
	}
	return byte(parsed), nil
}

func optionalPositiveUint32(q map[string][]string, names ...string) (uint32, error) {
	for _, name := range names {
		raw := strings.TrimSpace(firstQueryValue(q, name))
		if raw == "" {
			continue
		}
		parsed, err := strconv.ParseUint(raw, 10, 32)
		if err != nil || parsed == 0 {
			return 0, fmt.Errorf("%s must be a positive uint32 integer", name)
		}
		return uint32(parsed), nil
	}
	return 0, nil
}

func firstQueryValue(q map[string][]string, name string) string {
	values := q[name]
	if len(values) == 0 {
		return ""
	}
	return values[0]
}

type storeFlag []local.StoreSpec

func (f *storeFlag) String() string {
	if f == nil {
		return ""
	}
	parts := make([]string, 0, len(*f))
	for _, spec := range *f {
		parts = append(parts, fmt.Sprintf("%s:shards=%d,weight=%d,id=%s", spec.Path, spec.Shards, spec.Weight, spec.ID))
	}
	return strings.Join(parts, ",")
}

func (f *storeFlag) Set(raw string) error {
	spec, err := local.ParseStoreSpec(raw)
	if err != nil {
		return err
	}
	*f = append(*f, spec)
	return nil
}

func getenvOrDefault(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func getenvBoolOrDefault(key string, fallback bool) bool {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return fallback
	}
	switch strings.ToLower(v) {
	case "1", "true", "yes", "on":
		return true
	case "0", "false", "no", "off":
		return false
	default:
		return fallback
	}
}
