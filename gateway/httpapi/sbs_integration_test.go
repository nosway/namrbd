package httpapi

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/nosway/namrbd/gateway/service"
	"github.com/nosway/namrbd/gateway/store"
	cluster "github.com/nosway/namrbd/sbs/cluster"
	clustercontrol "github.com/nosway/namrbd/sbs/cluster/control"
	clustermaintenance "github.com/nosway/namrbd/sbs/cluster/maintenance"
	clustermeta "github.com/nosway/namrbd/sbs/cluster/metadata"
)

func TestGatewayHTTPWithInMemorySBSClient(t *testing.T) {
	spec := service.NormalizeVolumeSpec(service.VolumeSpec{
		ID:             service.HexVolumeID(101),
		Name:           "vol-a",
		Prefix:         "vol-a-00000065",
		SizeBytes:      4096 * 8,
		BlockSize:      4096,
		ChunkSizeBytes: 4096,
	})
	meta := service.NewInMemoryMetadataRepository([]service.VolumeSpec{spec})
	sbs := service.NewInMemorySBSClient([]service.VolumeSpec{spec})
	repo := service.NewSBSDataRepository(meta, sbs, "gw-a")
	svc := service.NewWithRepositoryOptions(meta, repo, "gw-a")

	server := New(svc, Config{GatewayID: "gw-a"})
	handler := server.Handler()

	attachReq := httptest.NewRequest(http.MethodPost, "/api/v1/volumes/00000065/attach", strings.NewReader(`{"host_id":"host-a","device_id":1}`))
	attachRec := httptest.NewRecorder()
	handler.ServeHTTP(attachRec, attachReq)
	if attachRec.Code != http.StatusOK {
		t.Fatalf("attach returned status %d body=%s", attachRec.Code, attachRec.Body.String())
	}

	payload := make([]byte, 4096)
	payload[0] = 0x11
	payload[1] = 0x22
	writeBody := `{"offset_bytes":0,"length_bytes":4096,"data_base64":"` + base64.StdEncoding.EncodeToString(payload) + `"}`
	writeReq := httptest.NewRequest(http.MethodPost, "/api/v1/volumes/00000065/write", strings.NewReader(writeBody))
	writeRec := httptest.NewRecorder()
	handler.ServeHTTP(writeRec, writeReq)
	if writeRec.Code != http.StatusOK {
		t.Fatalf("write returned status %d body=%s", writeRec.Code, writeRec.Body.String())
	}

	readReq := httptest.NewRequest(http.MethodPost, "/api/v1/volumes/00000065/read", strings.NewReader(`{"offset_bytes":0,"length_bytes":4096}`))
	readRec := httptest.NewRecorder()
	handler.ServeHTTP(readRec, readReq)
	if readRec.Code != http.StatusOK {
		t.Fatalf("read returned status %d body=%s", readRec.Code, readRec.Body.String())
	}

	var resp struct {
		DataBase64 string `json:"data_base64"`
	}
	if err := json.Unmarshal(readRec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode read response: %v", err)
	}
	got, err := base64.StdEncoding.DecodeString(resp.DataBase64)
	if err != nil {
		t.Fatalf("decode payload: %v", err)
	}
	if len(got) != len(payload) || got[0] != payload[0] || got[1] != payload[1] {
		t.Fatalf("unexpected read payload")
	}

	flushReq := httptest.NewRequest(http.MethodPost, "/api/v1/volumes/00000065/flush", strings.NewReader(`{}`))
	flushRec := httptest.NewRecorder()
	handler.ServeHTTP(flushRec, flushReq)
	if flushRec.Code != http.StatusOK {
		t.Fatalf("flush returned status %d body=%s", flushRec.Code, flushRec.Body.String())
	}

	zeroReq := httptest.NewRequest(http.MethodPost, "/api/v1/volumes/00000065/zero", strings.NewReader(`{"offset_bytes":0,"length_bytes":4096}`))
	zeroRec := httptest.NewRecorder()
	handler.ServeHTTP(zeroRec, zeroReq)
	if zeroRec.Code != http.StatusOK {
		t.Fatalf("zero returned status %d body=%s", zeroRec.Code, zeroRec.Body.String())
	}

	readReq = httptest.NewRequest(http.MethodPost, "/api/v1/volumes/00000065/read", strings.NewReader(`{"offset_bytes":0,"length_bytes":4096}`))
	readRec = httptest.NewRecorder()
	handler.ServeHTTP(readRec, readReq)
	if readRec.Code != http.StatusOK {
		t.Fatalf("read-after-zero returned status %d body=%s", readRec.Code, readRec.Body.String())
	}
	if err := json.Unmarshal(readRec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode zero read response: %v", err)
	}
	got, err = base64.StdEncoding.DecodeString(resp.DataBase64)
	if err != nil {
		t.Fatalf("decode zero payload: %v", err)
	}
	if got[0] != 0 || got[1] != 0 {
		t.Fatalf("expected zeroed payload after zero")
	}

	writeReq = httptest.NewRequest(http.MethodPost, "/api/v1/volumes/00000065/write", strings.NewReader(writeBody))
	writeRec = httptest.NewRecorder()
	handler.ServeHTTP(writeRec, writeReq)
	if writeRec.Code != http.StatusOK {
		t.Fatalf("second write returned status %d body=%s", writeRec.Code, writeRec.Body.String())
	}

	discardReq := httptest.NewRequest(http.MethodPost, "/api/v1/volumes/00000065/discard", strings.NewReader(`{"offset_bytes":0,"length_bytes":4096}`))
	discardRec := httptest.NewRecorder()
	handler.ServeHTTP(discardRec, discardReq)
	if discardRec.Code != http.StatusOK {
		t.Fatalf("discard returned status %d body=%s", discardRec.Code, discardRec.Body.String())
	}

	readReq = httptest.NewRequest(http.MethodPost, "/api/v1/volumes/00000065/read", strings.NewReader(`{"offset_bytes":0,"length_bytes":4096}`))
	readRec = httptest.NewRecorder()
	handler.ServeHTTP(readRec, readReq)
	if readRec.Code != http.StatusOK {
		t.Fatalf("read-after-discard returned status %d body=%s", readRec.Code, readRec.Body.String())
	}
	if err := json.Unmarshal(readRec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode discard read response: %v", err)
	}
	got, err = base64.StdEncoding.DecodeString(resp.DataBase64)
	if err != nil {
		t.Fatalf("decode discard payload: %v", err)
	}
	if got[0] != 0 || got[1] != 0 {
		t.Fatalf("expected zeroed payload after discard")
	}
}

type clusterHTTPTestEnv struct {
	handler http.Handler
	worker  *clustermaintenance.Worker
	repo    *clustermeta.Repository
}

func newClusterHTTPTestEnv(t *testing.T) clusterHTTPTestEnv {
	t.Helper()

	spec := service.NormalizeVolumeSpec(service.VolumeSpec{
		ID:        service.HexVolumeID(101),
		Name:      "vol-a",
		Prefix:    "vol-a-00000065",
		SizeBytes: 4096 * 8,
		BlockSize: 4096,
	})
	meta := service.NewInMemoryMetadataRepository([]service.VolumeSpec{spec})
	repo := clustermeta.NewRepository(store.NewMemoryStore(), "")
	ctx := context.Background()

	nodeReplicaIDs := map[string]string{
		"node-a": "rep-a",
		"node-b": "rep-b",
		"node-c": "rep-c",
		"node-d": "rep-d",
		"node-e": "rep-e",
		"node-f": "rep-f",
	}
	for _, nodeID := range []string{"node-a", "node-b", "node-c", "node-d", "node-e", "node-f"} {
		if err := repo.PutNodeMembership(ctx, clustermeta.NodeMembershipRecord{
			NodeID:         nodeID,
			ReplicaID:      nodeReplicaIDs[nodeID],
			LifecycleState: clustermeta.NodeLifecycleActive,
			HealthState:    clustermeta.NodeHealthHealthy,
		}); err != nil {
			t.Fatalf("PutNodeMembership(%s): %v", nodeID, err)
		}
	}
	if err := repo.PutVolumeState(ctx, clustermeta.VolumeState{
		VolumeID:          "00000065",
		Epoch:             1,
		Revision:          1,
		PlacementPolicyID: "extent-placement-v1",
		ProtectionPolicy:  "rf3",
		Status:            clustermeta.VolumeStatusHealthy,
	}); err != nil {
		t.Fatalf("PutVolumeState: %v", err)
	}
	if err := repo.PutExtentMapping(ctx, clustermeta.ExtentMappingRecord{
		VolumeID:      "00000065",
		ExtentID:      1,
		LogicalOffset: 0,
		LengthBytes:   4096,
		ChunkID:       11,
		PlacementRef:  "pl-1",
		Revision:      1,
	}); err != nil {
		t.Fatalf("PutExtentMapping: %v", err)
	}
	if err := repo.PutReplicaSet(ctx, clustermeta.ReplicaSetState{
		ReplicaSetID:     "rs-1",
		VolumeID:         "00000065",
		PlacementRef:     "pl-1",
		Epoch:            1,
		PrimaryReplicaID: "rep-a",
		WriteQuorum:      2,
		ReadQuorum:       1,
		Replicas: []clustermeta.ReplicaDescriptor{
			{NodeID: "node-a", ReplicaID: "rep-a", Role: clustermeta.ReplicaRolePrimary, FailureDomain: "host-a"},
			{NodeID: "node-b", ReplicaID: "rep-b", Role: clustermeta.ReplicaRoleSecondary, FailureDomain: "host-b"},
			{NodeID: "node-c", ReplicaID: "rep-c", Role: clustermeta.ReplicaRoleSecondary, FailureDomain: "host-c"},
		},
		FailureDomains: []string{"host-a", "host-b", "host-c"},
	}); err != nil {
		t.Fatalf("PutReplicaSet(pl-1): %v", err)
	}
	if err := repo.PutReplicaSet(ctx, clustermeta.ReplicaSetState{
		ReplicaSetID:     "rs-2",
		VolumeID:         "00000065",
		PlacementRef:     "pl-2",
		Epoch:            1,
		PrimaryReplicaID: "rep-d",
		WriteQuorum:      2,
		ReadQuorum:       1,
		Replicas: []clustermeta.ReplicaDescriptor{
			{NodeID: "node-d", ReplicaID: "rep-d", Role: clustermeta.ReplicaRolePrimary, FailureDomain: "host-d"},
			{NodeID: "node-e", ReplicaID: "rep-e", Role: clustermeta.ReplicaRoleSecondary, FailureDomain: "host-e"},
			{NodeID: "node-f", ReplicaID: "rep-f", Role: clustermeta.ReplicaRoleSecondary, FailureDomain: "host-f"},
		},
		FailureDomains: []string{"host-d", "host-e", "host-f"},
	}); err != nil {
		t.Fatalf("PutReplicaSet(pl-2): %v", err)
	}

	replicaClients := map[string]service.SBSClient{
		"rep-a": service.NewInMemorySBSClient([]service.VolumeSpec{spec}),
		"rep-b": service.NewInMemorySBSClient([]service.VolumeSpec{spec}),
		"rep-c": service.NewInMemorySBSClient([]service.VolumeSpec{spec}),
		"rep-d": service.NewInMemorySBSClient([]service.VolumeSpec{spec}),
		"rep-e": service.NewInMemorySBSClient([]service.VolumeSpec{spec}),
		"rep-f": service.NewInMemorySBSClient([]service.VolumeSpec{spec}),
	}
	for nodeID, replicaID := range nodeReplicaIDs {
		replicaClients[nodeID] = replicaClients[replicaID]
	}
	clusterClient, err := cluster.NewClient(cluster.Config{
		MetadataWriteSessionStore:           repo,
		MetadataChunkIDSequenceStore:        repo,
		MetadataAllocationPersistStore:      repo,
		MetadataExtentMappingNormalizeStore: repo,
		MetadataExtentMappingResolver:       repo,
		MetadataReplicaSetResolver:          repo,
		MetadataNodeMembershipResolver:      repo,
		MetadataAllocationPageReader:        repo,
		MetadataAllocationPageLister:        repo,
		VolumeSpecs:                         []service.VolumeSpec{spec},
		ReplicaClients:                      replicaClients,
		GatewayID:                           "gw-a",
		HostID:                              "host-a",
	})
	if err != nil {
		t.Fatalf("cluster.NewClient: %v", err)
	}

	dataRepo := service.NewSBSDataRepository(meta, clusterClient, "gw-a")
	svc := service.NewWithRepositoryOptions(meta, dataRepo, "gw-a")
	maintenanceSvc := clustermaintenance.NewService(repo)
	worker := clustermaintenance.NewWorker(maintenanceSvc, clustermaintenance.WorkerConfig{
		VolumeID:       "00000065",
		ReplicaClients: replicaClients,
		GatewayID:      "gw-a",
		HostID:         "host-a",
	})
	server := New(svc, Config{
		GatewayID:           "gw-a",
		ClusterNodeDebug:    clustercontrol.NewController(repo, maintenanceSvc),
		DataplaneSessionKey: "test-session-key-32-bytes-long!!",
	})
	return clusterHTTPTestEnv{
		handler: server.Handler(),
		worker:  worker,
		repo:    repo,
	}
}

func TestGatewayHTTPClusterNodeFailureTriggersRepairAndPreservesIO(t *testing.T) {
	env := newClusterHTTPTestEnv(t)

	attachReq := httptest.NewRequest(http.MethodPost, "/api/v1/volumes/00000065/attach", strings.NewReader(`{"host_id":"host-a","device_id":1}`))
	attachRec := httptest.NewRecorder()
	env.handler.ServeHTTP(attachRec, attachReq)
	if attachRec.Code != http.StatusOK {
		t.Fatalf("attach returned status %d body=%s", attachRec.Code, attachRec.Body.String())
	}

	initialPayload := make([]byte, 4096)
	copy(initialPayload, []byte("before-repair-payload"))
	writeBody := `{"offset_bytes":0,"length_bytes":4096,"data_base64":"` + base64.StdEncoding.EncodeToString(initialPayload) + `"}`
	writeReq := httptest.NewRequest(http.MethodPost, "/api/v1/volumes/00000065/write", strings.NewReader(writeBody))
	writeRec := httptest.NewRecorder()
	env.handler.ServeHTTP(writeRec, writeReq)
	if writeRec.Code != http.StatusOK {
		t.Fatalf("initial write returned status %d body=%s", writeRec.Code, writeRec.Body.String())
	}

	setHealthReq := httptest.NewRequest(http.MethodPost, "/api/v1/debug/sbs-cluster/nodes/node-c", strings.NewReader(`{"health_state":"down"}`))
	setHealthRec := httptest.NewRecorder()
	env.handler.ServeHTTP(setHealthRec, setHealthReq)
	if setHealthRec.Code != http.StatusOK {
		t.Fatalf("set node health returned status %d body=%s", setHealthRec.Code, setHealthRec.Body.String())
	}
	var setHealthResp struct {
		PrimaryFailovers int `json:"primary_failovers"`
		RepairEnqueued   int `json:"repair_enqueued"`
	}
	if err := json.Unmarshal(setHealthRec.Body.Bytes(), &setHealthResp); err != nil {
		t.Fatalf("decode set node response: %v", err)
	}
	if setHealthResp.PrimaryFailovers != 0 || setHealthResp.RepairEnqueued != 1 {
		t.Fatalf("failovers=%d repairs=%d want failovers=0 repairs=1", setHealthResp.PrimaryFailovers, setHealthResp.RepairEnqueued)
	}

	worked, err := env.worker.RunOnce(context.Background())
	if err != nil {
		t.Fatalf("worker.RunOnce: %v", err)
	}
	if !worked {
		t.Fatal("expected maintenance worker to apply repair transition")
	}

	mappings, err := env.repo.ListExtentMappings(context.Background(), "00000065")
	if err != nil {
		t.Fatalf("ListExtentMappings: %v", err)
	}
	if len(mappings) != 1 || mappings[0].PlacementRef != "pl-1-repair-node-c" {
		t.Fatalf("unexpected placement mappings: %+v", mappings)
	}

	readReq := httptest.NewRequest(http.MethodPost, "/api/v1/volumes/00000065/read", strings.NewReader(`{"offset_bytes":0,"length_bytes":4096}`))
	readRec := httptest.NewRecorder()
	env.handler.ServeHTTP(readRec, readReq)
	if readRec.Code != http.StatusOK {
		t.Fatalf("post-repair read returned status %d body=%s", readRec.Code, readRec.Body.String())
	}
	var readResp struct {
		DataBase64 string `json:"data_base64"`
	}
	if err := json.Unmarshal(readRec.Body.Bytes(), &readResp); err != nil {
		t.Fatalf("decode read response: %v", err)
	}
	got, err := base64.StdEncoding.DecodeString(readResp.DataBase64)
	if err != nil {
		t.Fatalf("decode read payload: %v", err)
	}
	if string(got[:len("before-repair-payload")]) != "before-repair-payload" {
		t.Fatalf("unexpected pre-repair payload after transition: %q", got[:len("before-repair-payload")])
	}

	nextPayload := make([]byte, 4096)
	copy(nextPayload, []byte("after-repair-payload"))
	writeBody = `{"offset_bytes":0,"length_bytes":4096,"data_base64":"` + base64.StdEncoding.EncodeToString(nextPayload) + `"}`
	writeReq = httptest.NewRequest(http.MethodPost, "/api/v1/volumes/00000065/write", strings.NewReader(writeBody))
	writeRec = httptest.NewRecorder()
	env.handler.ServeHTTP(writeRec, writeReq)
	if writeRec.Code != http.StatusOK {
		t.Fatalf("post-repair write returned status %d body=%s", writeRec.Code, writeRec.Body.String())
	}

	readReq = httptest.NewRequest(http.MethodPost, "/api/v1/volumes/00000065/read", strings.NewReader(`{"offset_bytes":0,"length_bytes":4096}`))
	readRec = httptest.NewRecorder()
	env.handler.ServeHTTP(readRec, readReq)
	if readRec.Code != http.StatusOK {
		t.Fatalf("post-repair second read returned status %d body=%s", readRec.Code, readRec.Body.String())
	}
	if err := json.Unmarshal(readRec.Body.Bytes(), &readResp); err != nil {
		t.Fatalf("decode second read response: %v", err)
	}
	got, err = base64.StdEncoding.DecodeString(readResp.DataBase64)
	if err != nil {
		t.Fatalf("decode second read payload: %v", err)
	}
	if string(got[:len("after-repair-payload")]) != "after-repair-payload" {
		t.Fatalf("unexpected payload after repair write: %q", got[:len("after-repair-payload")])
	}
}

func TestGatewayHTTPClusterPrimaryFailureTriggersFailoverAndRepair(t *testing.T) {
	env := newClusterHTTPTestEnv(t)

	attachReq := httptest.NewRequest(http.MethodPost, "/api/v1/volumes/00000065/attach", strings.NewReader(`{"host_id":"host-a","device_id":2}`))
	attachRec := httptest.NewRecorder()
	env.handler.ServeHTTP(attachRec, attachReq)
	if attachRec.Code != http.StatusOK {
		t.Fatalf("attach returned status %d body=%s", attachRec.Code, attachRec.Body.String())
	}

	initialPayload := make([]byte, 4096)
	copy(initialPayload, []byte("before-primary-failover"))
	writeBody := `{"offset_bytes":0,"length_bytes":4096,"data_base64":"` + base64.StdEncoding.EncodeToString(initialPayload) + `"}`
	writeReq := httptest.NewRequest(http.MethodPost, "/api/v1/volumes/00000065/write", strings.NewReader(writeBody))
	writeRec := httptest.NewRecorder()
	env.handler.ServeHTTP(writeRec, writeReq)
	if writeRec.Code != http.StatusOK {
		t.Fatalf("initial write returned status %d body=%s", writeRec.Code, writeRec.Body.String())
	}

	setHealthReq := httptest.NewRequest(http.MethodPost, "/api/v1/debug/sbs-cluster/nodes/node-a", strings.NewReader(`{"health_state":"down"}`))
	setHealthRec := httptest.NewRecorder()
	env.handler.ServeHTTP(setHealthRec, setHealthReq)
	if setHealthRec.Code != http.StatusOK {
		t.Fatalf("set node health returned status %d body=%s", setHealthRec.Code, setHealthRec.Body.String())
	}
	var setHealthResp struct {
		PrimaryFailovers int `json:"primary_failovers"`
		RepairEnqueued   int `json:"repair_enqueued"`
	}
	if err := json.Unmarshal(setHealthRec.Body.Bytes(), &setHealthResp); err != nil {
		t.Fatalf("decode set node response: %v", err)
	}
	if setHealthResp.PrimaryFailovers != 1 || setHealthResp.RepairEnqueued != 1 {
		t.Fatalf("failovers=%d repairs=%d want failovers=1 repairs=1", setHealthResp.PrimaryFailovers, setHealthResp.RepairEnqueued)
	}

	worked, err := env.worker.RunOnce(context.Background())
	if err != nil {
		t.Fatalf("worker.RunOnce: %v", err)
	}
	if !worked {
		t.Fatal("expected maintenance worker to apply repair transition")
	}

	replicaSets, err := env.repo.ListReplicaSets(context.Background(), "00000065")
	if err != nil {
		t.Fatalf("ListReplicaSets: %v", err)
	}
	if len(replicaSets) < 1 || replicaSets[0].PrimaryReplicaID != "rep-b" {
		t.Fatalf("unexpected replica sets after failover: %+v", replicaSets)
	}

	readReq := httptest.NewRequest(http.MethodPost, "/api/v1/volumes/00000065/read", strings.NewReader(`{"offset_bytes":0,"length_bytes":4096}`))
	readRec := httptest.NewRecorder()
	env.handler.ServeHTTP(readRec, readReq)
	if readRec.Code != http.StatusOK {
		t.Fatalf("post-failover read returned status %d body=%s", readRec.Code, readRec.Body.String())
	}
	var readResp struct {
		DataBase64 string `json:"data_base64"`
	}
	if err := json.Unmarshal(readRec.Body.Bytes(), &readResp); err != nil {
		t.Fatalf("decode read response: %v", err)
	}
	got, err := base64.StdEncoding.DecodeString(readResp.DataBase64)
	if err != nil {
		t.Fatalf("decode read payload: %v", err)
	}
	if string(got[:len("before-primary-failover")]) != "before-primary-failover" {
		t.Fatalf("unexpected payload after failover: %q", got[:len("before-primary-failover")])
	}
}

func TestGatewayHTTPClusterDetachReattachBumpsGenerationAndPreservesIO(t *testing.T) {
	env := newClusterHTTPTestEnv(t)

	attachReq := httptest.NewRequest(http.MethodPost, "/api/v1/volumes/00000065/attach", strings.NewReader(`{"host_id":"host-a","device_id":3}`))
	attachRec := httptest.NewRecorder()
	env.handler.ServeHTTP(attachRec, attachReq)
	if attachRec.Code != http.StatusOK {
		t.Fatalf("attach returned status %d body=%s", attachRec.Code, attachRec.Body.String())
	}
	var attachResp struct {
		AttachmentID string `json:"attachment_id"`
		Generation   uint64 `json:"generation"`
	}
	if err := json.Unmarshal(attachRec.Body.Bytes(), &attachResp); err != nil {
		t.Fatalf("decode attach response: %v", err)
	}
	if attachResp.AttachmentID != "att-00000065-0001" || attachResp.Generation != 1 {
		t.Fatalf("unexpected initial attach response: %+v", attachResp)
	}

	initialPayload := make([]byte, 4096)
	copy(initialPayload, []byte("before-reattach-payload"))
	writeBody := `{"offset_bytes":0,"length_bytes":4096,"data_base64":"` + base64.StdEncoding.EncodeToString(initialPayload) + `"}`
	writeReq := httptest.NewRequest(http.MethodPost, "/api/v1/volumes/00000065/write", strings.NewReader(writeBody))
	writeRec := httptest.NewRecorder()
	env.handler.ServeHTTP(writeRec, writeReq)
	if writeRec.Code != http.StatusOK {
		t.Fatalf("initial write returned status %d body=%s", writeRec.Code, writeRec.Body.String())
	}

	detachReq := httptest.NewRequest(http.MethodPost, "/api/v1/volumes/00000065/detach", strings.NewReader(`{"host_id":"host-a","attachment_id":"att-00000065-0001"}`))
	detachRec := httptest.NewRecorder()
	env.handler.ServeHTTP(detachRec, detachReq)
	if detachRec.Code != http.StatusOK {
		t.Fatalf("detach returned status %d body=%s", detachRec.Code, detachRec.Body.String())
	}
	var detachResp struct {
		Generation uint64 `json:"generation"`
	}
	if err := json.Unmarshal(detachRec.Body.Bytes(), &detachResp); err != nil {
		t.Fatalf("decode detach response: %v", err)
	}
	if detachResp.Generation != 2 {
		t.Fatalf("detach generation=%d want=2", detachResp.Generation)
	}

	attachReq = httptest.NewRequest(http.MethodPost, "/api/v1/volumes/00000065/attach", strings.NewReader(`{"host_id":"host-a","device_id":4}`))
	attachRec = httptest.NewRecorder()
	env.handler.ServeHTTP(attachRec, attachReq)
	if attachRec.Code != http.StatusOK {
		t.Fatalf("reattach returned status %d body=%s", attachRec.Code, attachRec.Body.String())
	}
	if err := json.Unmarshal(attachRec.Body.Bytes(), &attachResp); err != nil {
		t.Fatalf("decode reattach response: %v", err)
	}
	if attachResp.AttachmentID != "att-00000065-0002" || attachResp.Generation != 2 {
		t.Fatalf("unexpected reattach response: %+v", attachResp)
	}

	readReq := httptest.NewRequest(http.MethodPost, "/api/v1/volumes/00000065/read", strings.NewReader(`{"offset_bytes":0,"length_bytes":4096}`))
	readRec := httptest.NewRecorder()
	env.handler.ServeHTTP(readRec, readReq)
	if readRec.Code != http.StatusOK {
		t.Fatalf("read after reattach returned status %d body=%s", readRec.Code, readRec.Body.String())
	}
	var readResp struct {
		DataBase64 string `json:"data_base64"`
	}
	if err := json.Unmarshal(readRec.Body.Bytes(), &readResp); err != nil {
		t.Fatalf("decode read response: %v", err)
	}
	got, err := base64.StdEncoding.DecodeString(readResp.DataBase64)
	if err != nil {
		t.Fatalf("decode read payload: %v", err)
	}
	if string(got[:len("before-reattach-payload")]) != "before-reattach-payload" {
		t.Fatalf("unexpected payload after reattach: %q", got[:len("before-reattach-payload")])
	}

	nextPayload := make([]byte, 4096)
	copy(nextPayload, []byte("after-reattach-payload"))
	writeBody = `{"offset_bytes":0,"length_bytes":4096,"data_base64":"` + base64.StdEncoding.EncodeToString(nextPayload) + `"}`
	writeReq = httptest.NewRequest(http.MethodPost, "/api/v1/volumes/00000065/write", strings.NewReader(writeBody))
	writeRec = httptest.NewRecorder()
	env.handler.ServeHTTP(writeRec, writeReq)
	if writeRec.Code != http.StatusOK {
		t.Fatalf("write after reattach returned status %d body=%s", writeRec.Code, writeRec.Body.String())
	}

	readReq = httptest.NewRequest(http.MethodPost, "/api/v1/volumes/00000065/read", strings.NewReader(`{"offset_bytes":0,"length_bytes":4096}`))
	readRec = httptest.NewRecorder()
	env.handler.ServeHTTP(readRec, readReq)
	if readRec.Code != http.StatusOK {
		t.Fatalf("second read after reattach returned status %d body=%s", readRec.Code, readRec.Body.String())
	}
	if err := json.Unmarshal(readRec.Body.Bytes(), &readResp); err != nil {
		t.Fatalf("decode second read response: %v", err)
	}
	got, err = base64.StdEncoding.DecodeString(readResp.DataBase64)
	if err != nil {
		t.Fatalf("decode second read payload: %v", err)
	}
	if string(got[:len("after-reattach-payload")]) != "after-reattach-payload" {
		t.Fatalf("unexpected payload after reattach write: %q", got[:len("after-reattach-payload")])
	}
}
