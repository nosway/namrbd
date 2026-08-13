package driver

import (
	"context"
	"testing"

	csipb "github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type fakeNodeHelper struct {
	attachCalls      []NodeAttachRequest
	lookupCalls      []NodeLookupAttachmentRequest
	detachCalls      []NodeDetachRequest
	formatCalls      []NodeFormatRequest
	mountCalls       []NodeMountRequest
	bindCalls        []NodeBindBlockRequest
	unmountCalls     []string
	reloadCalls      []NodeReloadSizeRequest
	growCalls        []NodeGrowFilesystemRequest
	nextAttach       NodeAttachment
	lookupAttachment NodeAttachment
	lookupErr        error
}

func newFakeNodeHelper() *fakeNodeHelper {
	return &fakeNodeHelper{
		nextAttach: NodeAttachment{
			DevicePath:   "/dev/namrbd0",
			DeviceID:     7,
			AttachmentID: "att-00a1b2c3-0001",
			Generation:   1,
		},
	}
}

func (f *fakeNodeHelper) Attach(_ context.Context, req NodeAttachRequest) (NodeAttachment, error) {
	f.attachCalls = append(f.attachCalls, req)
	return f.nextAttach, nil
}

func (f *fakeNodeHelper) LookupAttachment(_ context.Context, req NodeLookupAttachmentRequest) (NodeAttachment, error) {
	f.lookupCalls = append(f.lookupCalls, req)
	if f.lookupErr != nil {
		return NodeAttachment{}, f.lookupErr
	}
	if f.lookupAttachment.DevicePath == "" {
		return NodeAttachment{}, errNodeAttachmentNotFound
	}
	return f.lookupAttachment, nil
}

func (f *fakeNodeHelper) Detach(_ context.Context, req NodeDetachRequest) error {
	f.detachCalls = append(f.detachCalls, req)
	return nil
}

func (f *fakeNodeHelper) FormatIfNeeded(_ context.Context, req NodeFormatRequest) error {
	f.formatCalls = append(f.formatCalls, req)
	return nil
}

func (f *fakeNodeHelper) Mount(_ context.Context, req NodeMountRequest) error {
	f.mountCalls = append(f.mountCalls, req)
	return nil
}

func (f *fakeNodeHelper) BindBlock(_ context.Context, req NodeBindBlockRequest) error {
	f.bindCalls = append(f.bindCalls, req)
	return nil
}

func (f *fakeNodeHelper) Unmount(_ context.Context, targetPath string) error {
	f.unmountCalls = append(f.unmountCalls, targetPath)
	return nil
}

func (f *fakeNodeHelper) ReloadSize(_ context.Context, req NodeReloadSizeRequest) error {
	f.reloadCalls = append(f.reloadCalls, req)
	return nil
}

func (f *fakeNodeHelper) GrowFilesystem(_ context.Context, req NodeGrowFilesystemRequest) error {
	f.growCalls = append(f.growCalls, req)
	return nil
}

func newNodeTestServer(t *testing.T, helper *fakeNodeHelper) *Server {
	t.Helper()
	srv, err := New(Config{
		ClusterID:    "test-cluster",
		SBSClusterID: "test-sbs",
		Backend:      newFakeBackend(),
		NodeID:       "node-u11",
		NodeHelper:   helper,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return srv
}

func TestNodeCapabilitiesAndInfo(t *testing.T) {
	srv := newNodeTestServer(t, newFakeNodeHelper())
	info, err := srv.NodeGetInfo(context.Background(), &csipb.NodeGetInfoRequest{})
	if err != nil {
		t.Fatalf("NodeGetInfo: %v", err)
	}
	if info.GetNodeId() != "node-u11" {
		t.Fatalf("node id=%q", info.GetNodeId())
	}
	caps, err := srv.NodeGetCapabilities(context.Background(), &csipb.NodeGetCapabilitiesRequest{})
	if err != nil {
		t.Fatalf("NodeGetCapabilities: %v", err)
	}
	got := map[csipb.NodeServiceCapability_RPC_Type]bool{}
	for _, cap := range caps.GetCapabilities() {
		got[cap.GetRpc().GetType()] = true
	}
	if !got[csipb.NodeServiceCapability_RPC_STAGE_UNSTAGE_VOLUME] || !got[csipb.NodeServiceCapability_RPC_EXPAND_VOLUME] {
		t.Fatalf("node capabilities=%+v", got)
	}
}

func TestNodeBlockStagePublishUnpublishUnstageIsIdempotent(t *testing.T) {
	helper := newFakeNodeHelper()
	srv := newNodeTestServer(t, helper)
	stageReq := &csipb.NodeStageVolumeRequest{
		VolumeId:          "00a1b2c3",
		StagingTargetPath: "/var/lib/kubelet/plugins/kubernetes.io/csi/namrbd/stage/00a1b2c3",
		VolumeCapability:  blockRWO(),
	}
	if _, err := srv.NodeStageVolume(context.Background(), stageReq); err != nil {
		t.Fatalf("NodeStageVolume: %v", err)
	}
	if _, err := srv.NodeStageVolume(context.Background(), stageReq); err != nil {
		t.Fatalf("NodeStageVolume replay: %v", err)
	}
	if len(helper.attachCalls) != 1 {
		t.Fatalf("attach calls=%d", len(helper.attachCalls))
	}
	if helper.attachCalls[0].VolumeID != "00a1b2c3" || helper.attachCalls[0].NodeID != "node-u11" {
		t.Fatalf("attach call=%+v", helper.attachCalls[0])
	}

	publishReq := &csipb.NodePublishVolumeRequest{
		VolumeId:          "00a1b2c3",
		StagingTargetPath: stageReq.GetStagingTargetPath(),
		TargetPath:        "/var/lib/kubelet/pods/pod-1/volumes/kubernetes.io~csi/vol/block",
		VolumeCapability:  blockRWO(),
		Readonly:          true,
		PublishContext:    map[string]string{nodeAttachmentGenerationKey: "1"},
	}
	if _, err := srv.NodePublishVolume(context.Background(), publishReq); err != nil {
		t.Fatalf("NodePublishVolume: %v", err)
	}
	if _, err := srv.NodePublishVolume(context.Background(), publishReq); err != nil {
		t.Fatalf("NodePublishVolume replay: %v", err)
	}
	if len(helper.bindCalls) != 1 || !helper.bindCalls[0].Readonly {
		t.Fatalf("bind calls=%+v", helper.bindCalls)
	}
	if helper.bindCalls[0].DevicePath != "/dev/namrbd0" || helper.bindCalls[0].TargetPath != publishReq.GetTargetPath() {
		t.Fatalf("bind call=%+v", helper.bindCalls[0])
	}

	staleReq := proto.Clone(publishReq).(*csipb.NodePublishVolumeRequest)
	staleReq.TargetPath = "/var/lib/kubelet/pods/pod-2/volumes/kubernetes.io~csi/vol/block"
	staleReq.PublishContext = map[string]string{nodeAttachmentGenerationKey: "2"}
	if _, err := srv.NodePublishVolume(context.Background(), staleReq); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("stale publish err=%v want FailedPrecondition", err)
	}

	if _, err := srv.NodeUnpublishVolume(context.Background(), &csipb.NodeUnpublishVolumeRequest{
		VolumeId:   "00a1b2c3",
		TargetPath: publishReq.GetTargetPath(),
	}); err != nil {
		t.Fatalf("NodeUnpublishVolume: %v", err)
	}
	if _, err := srv.NodeUnpublishVolume(context.Background(), &csipb.NodeUnpublishVolumeRequest{
		VolumeId:   "00a1b2c3",
		TargetPath: publishReq.GetTargetPath(),
	}); err != nil {
		t.Fatalf("NodeUnpublishVolume replay: %v", err)
	}
	if len(helper.unmountCalls) != 1 || helper.unmountCalls[0] != publishReq.GetTargetPath() {
		t.Fatalf("unmount calls=%+v", helper.unmountCalls)
	}

	if _, err := srv.NodeUnstageVolume(context.Background(), &csipb.NodeUnstageVolumeRequest{
		VolumeId:          "00a1b2c3",
		StagingTargetPath: stageReq.GetStagingTargetPath(),
	}); err != nil {
		t.Fatalf("NodeUnstageVolume: %v", err)
	}
	if _, err := srv.NodeUnstageVolume(context.Background(), &csipb.NodeUnstageVolumeRequest{
		VolumeId:          "00a1b2c3",
		StagingTargetPath: stageReq.GetStagingTargetPath(),
	}); err != nil {
		t.Fatalf("NodeUnstageVolume replay: %v", err)
	}
	if len(helper.detachCalls) != 1 || helper.detachCalls[0].AttachmentID != "att-00a1b2c3-0001" {
		t.Fatalf("detach calls=%+v", helper.detachCalls)
	}
}

func TestNodeStageTreatsGRPCNotFoundLookupAsMissingAttachment(t *testing.T) {
	helper := newFakeNodeHelper()
	helper.lookupErr = status.Error(codes.NotFound, "volume not found")
	srv := newNodeTestServer(t, helper)
	stageReq := &csipb.NodeStageVolumeRequest{
		VolumeId:          "00a1b2c8",
		StagingTargetPath: "/var/lib/kubelet/plugins/kubernetes.io/csi/namrbd/stage/00a1b2c8",
		VolumeCapability:  blockRWO(),
	}

	if _, err := srv.NodeStageVolume(context.Background(), stageReq); err != nil {
		t.Fatalf("NodeStageVolume: %v", err)
	}
	if len(helper.lookupCalls) != 1 || helper.lookupCalls[0].VolumeID != "00a1b2c8" {
		t.Fatalf("lookup calls=%+v", helper.lookupCalls)
	}
	if len(helper.attachCalls) != 1 || helper.attachCalls[0].VolumeID != "00a1b2c8" || helper.attachCalls[0].NodeID != "node-u11" {
		t.Fatalf("attach calls=%+v", helper.attachCalls)
	}
	if len(helper.bindCalls) != 0 || len(helper.mountCalls) != 0 {
		t.Fatalf("unexpected publish-stage side effects: bind=%+v mount=%+v", helper.bindCalls, helper.mountCalls)
	}
}

func TestNodeFilesystemStagePublishExpandAndCleanup(t *testing.T) {
	helper := newFakeNodeHelper()
	srv := newNodeTestServer(t, helper)
	stageReq := &csipb.NodeStageVolumeRequest{
		VolumeId:          "00a1b2c4",
		StagingTargetPath: "/var/lib/kubelet/plugins/kubernetes.io/csi/namrbd/stage/00a1b2c4",
		VolumeCapability: &csipb.VolumeCapability{
			AccessType: &csipb.VolumeCapability_Mount{Mount: &csipb.VolumeCapability_MountVolume{
				FsType:     "ext4",
				MountFlags: []string{"noatime"},
			}},
			AccessMode: &csipb.VolumeCapability_AccessMode{Mode: csipb.VolumeCapability_AccessMode_SINGLE_NODE_SINGLE_WRITER},
		},
	}
	if _, err := srv.NodeStageVolume(context.Background(), stageReq); err != nil {
		t.Fatalf("NodeStageVolume fs: %v", err)
	}
	if len(helper.formatCalls) != 1 || helper.formatCalls[0].FSType != "ext4" {
		t.Fatalf("format calls=%+v", helper.formatCalls)
	}
	if len(helper.mountCalls) != 1 || helper.mountCalls[0].Target != stageReq.GetStagingTargetPath() || helper.mountCalls[0].FSType != "ext4" {
		t.Fatalf("stage mount calls=%+v", helper.mountCalls)
	}

	publishReq := &csipb.NodePublishVolumeRequest{
		VolumeId:          "00a1b2c4",
		StagingTargetPath: stageReq.GetStagingTargetPath(),
		TargetPath:        "/var/lib/kubelet/pods/pod-1/volumes/kubernetes.io~csi/vol/mount",
		VolumeCapability:  stageReq.GetVolumeCapability(),
	}
	if _, err := srv.NodePublishVolume(context.Background(), publishReq); err != nil {
		t.Fatalf("NodePublishVolume fs: %v", err)
	}
	if len(helper.mountCalls) != 2 || !helper.mountCalls[1].Bind || helper.mountCalls[1].Source != stageReq.GetStagingTargetPath() {
		t.Fatalf("publish mount calls=%+v", helper.mountCalls)
	}
	if _, err := srv.NodeUnstageVolume(context.Background(), &csipb.NodeUnstageVolumeRequest{
		VolumeId:          "00a1b2c4",
		StagingTargetPath: stageReq.GetStagingTargetPath(),
	}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("unstage while published err=%v want FailedPrecondition", err)
	}

	expandResp, err := srv.NodeExpandVolume(context.Background(), &csipb.NodeExpandVolumeRequest{
		VolumeId:          "00a1b2c4",
		VolumePath:        publishReq.GetTargetPath(),
		StagingTargetPath: stageReq.GetStagingTargetPath(),
		CapacityRange:     &csipb.CapacityRange{RequiredBytes: 64 << 20},
		VolumeCapability:  stageReq.GetVolumeCapability(),
	})
	if err != nil {
		t.Fatalf("NodeExpandVolume: %v", err)
	}
	if expandResp.GetCapacityBytes() != 64<<20 {
		t.Fatalf("expand response=%+v", expandResp)
	}
	if len(helper.reloadCalls) != 1 || len(helper.growCalls) != 1 || helper.growCalls[0].VolumePath != publishReq.GetTargetPath() {
		t.Fatalf("reload=%+v grow=%+v", helper.reloadCalls, helper.growCalls)
	}

	if _, err := srv.NodeUnpublishVolume(context.Background(), &csipb.NodeUnpublishVolumeRequest{
		VolumeId:   "00a1b2c4",
		TargetPath: publishReq.GetTargetPath(),
	}); err != nil {
		t.Fatalf("NodeUnpublishVolume fs: %v", err)
	}
	if _, err := srv.NodeUnstageVolume(context.Background(), &csipb.NodeUnstageVolumeRequest{
		VolumeId:          "00a1b2c4",
		StagingTargetPath: stageReq.GetStagingTargetPath(),
	}); err != nil {
		t.Fatalf("NodeUnstageVolume fs: %v", err)
	}
	if len(helper.unmountCalls) != 2 || helper.unmountCalls[1] != stageReq.GetStagingTargetPath() {
		t.Fatalf("unmount calls=%+v", helper.unmountCalls)
	}
}

func TestNodeStageRecoversExistingLocalAttachment(t *testing.T) {
	helper := newFakeNodeHelper()
	helper.lookupAttachment = NodeAttachment{
		DevicePath: "/dev/namrbd5",
		DeviceID:   5,
		Generation: 4,
	}
	srv := newNodeTestServer(t, helper)
	stageReq := &csipb.NodeStageVolumeRequest{
		VolumeId:          "00a1b2c7",
		StagingTargetPath: "/var/lib/kubelet/plugins/kubernetes.io/csi/namrbd/stage/00a1b2c7",
		VolumeCapability:  mountRWOP(),
	}

	if _, err := srv.NodeStageVolume(context.Background(), stageReq); err != nil {
		t.Fatalf("NodeStageVolume recovered: %v", err)
	}
	if len(helper.lookupCalls) != 1 || helper.lookupCalls[0].VolumeID != "00a1b2c7" {
		t.Fatalf("lookup calls=%+v", helper.lookupCalls)
	}
	if len(helper.attachCalls) != 0 {
		t.Fatalf("attach calls=%+v want none", helper.attachCalls)
	}
	if len(helper.mountCalls) != 1 ||
		helper.mountCalls[0].Source != "/dev/namrbd5" ||
		helper.mountCalls[0].Target != stageReq.GetStagingTargetPath() {
		t.Fatalf("mount calls=%+v", helper.mountCalls)
	}

	if _, err := srv.NodeUnstageVolume(context.Background(), &csipb.NodeUnstageVolumeRequest{
		VolumeId:          stageReq.GetVolumeId(),
		StagingTargetPath: stageReq.GetStagingTargetPath(),
	}); err != nil {
		t.Fatalf("NodeUnstageVolume recovered: %v", err)
	}
	if len(helper.detachCalls) != 1 ||
		helper.detachCalls[0].DeviceID != 5 ||
		helper.detachCalls[0].AttachmentID != "recovered-local:00a1b2c7:5" {
		t.Fatalf("detach calls=%+v", helper.detachCalls)
	}
}

func TestNodeExpandRecoversLocalAttachmentAfterNodeRestart(t *testing.T) {
	helper := newFakeNodeHelper()
	helper.lookupAttachment = NodeAttachment{
		DevicePath: "/dev/namrbd3",
		DeviceID:   3,
		Generation: 9,
	}
	srv := newNodeTestServer(t, helper)

	expandResp, err := srv.NodeExpandVolume(context.Background(), &csipb.NodeExpandVolumeRequest{
		VolumeId:          "00a1b2c4",
		VolumePath:        "/var/lib/kubelet/pods/pod-1/volumes/kubernetes.io~csi/vol/mount",
		StagingTargetPath: "/var/lib/kubelet/plugins/kubernetes.io/csi/block.namrbd.io/globalmount",
		CapacityRange:     &csipb.CapacityRange{RequiredBytes: 64 << 20},
		VolumeCapability:  mountRWOP(),
	})
	if err != nil {
		t.Fatalf("NodeExpandVolume after restart: %v", err)
	}
	if expandResp.GetCapacityBytes() != 64<<20 {
		t.Fatalf("expand response=%+v", expandResp)
	}
	if len(helper.lookupCalls) != 1 || helper.lookupCalls[0].VolumeID != "00a1b2c4" {
		t.Fatalf("lookup calls=%+v", helper.lookupCalls)
	}
	if len(helper.reloadCalls) != 1 ||
		helper.reloadCalls[0].VolumeID != "00a1b2c4" ||
		helper.reloadCalls[0].DevicePath != "/dev/namrbd3" ||
		helper.reloadCalls[0].DeviceID != 3 {
		t.Fatalf("reload calls=%+v", helper.reloadCalls)
	}
	if len(helper.growCalls) != 1 ||
		helper.growCalls[0].DevicePath != "/dev/namrbd3" ||
		helper.growCalls[0].VolumePath != "/var/lib/kubelet/pods/pod-1/volumes/kubernetes.io~csi/vol/mount" ||
		helper.growCalls[0].FSType != "ext4" {
		t.Fatalf("grow calls=%+v", helper.growCalls)
	}
}

func TestNodeExpandReportsNotFoundWhenRestartedWithoutLocalAttachment(t *testing.T) {
	helper := newFakeNodeHelper()
	helper.lookupErr = errNodeAttachmentNotFound
	srv := newNodeTestServer(t, helper)

	_, err := srv.NodeExpandVolume(context.Background(), &csipb.NodeExpandVolumeRequest{
		VolumeId:          "00a1b2c4",
		VolumePath:        "/var/lib/kubelet/pods/pod-1/volumes/kubernetes.io~csi/vol/mount",
		StagingTargetPath: "/var/lib/kubelet/plugins/kubernetes.io/csi/block.namrbd.io/globalmount",
		CapacityRange:     &csipb.CapacityRange{RequiredBytes: 64 << 20},
		VolumeCapability:  mountRWOP(),
	})
	if status.Code(err) != codes.NotFound {
		t.Fatalf("NodeExpandVolume err=%v want NotFound", err)
	}
}

func TestNodeStageSnapshotFilesystemGrowsAfterMount(t *testing.T) {
	helper := newFakeNodeHelper()
	srv := newNodeTestServer(t, helper)
	stageReq := &csipb.NodeStageVolumeRequest{
		VolumeId:          "00a1b2c4",
		StagingTargetPath: "/var/lib/kubelet/plugins/kubernetes.io/csi/namrbd/stage/00a1b2c4",
		VolumeCapability: &csipb.VolumeCapability{
			AccessType: &csipb.VolumeCapability_Mount{Mount: &csipb.VolumeCapability_MountVolume{
				FsType: "ext4",
			}},
			AccessMode: &csipb.VolumeCapability_AccessMode{Mode: csipb.VolumeCapability_AccessMode_SINGLE_NODE_SINGLE_WRITER},
		},
		VolumeContext: volumeContext(volumeContextProvisioningSnapshot, "restore-pvc"),
	}

	if _, err := srv.NodeStageVolume(context.Background(), stageReq); err != nil {
		t.Fatalf("NodeStageVolume snapshot fs: %v", err)
	}
	if len(helper.mountCalls) != 1 || helper.mountCalls[0].Target != stageReq.GetStagingTargetPath() {
		t.Fatalf("mount calls=%+v", helper.mountCalls)
	}
	if len(helper.reloadCalls) != 1 || helper.reloadCalls[0].VolumeID != stageReq.GetVolumeId() {
		t.Fatalf("reload calls=%+v", helper.reloadCalls)
	}
	if len(helper.growCalls) != 1 ||
		helper.growCalls[0].DevicePath != helper.nextAttach.DevicePath ||
		helper.growCalls[0].VolumePath != stageReq.GetStagingTargetPath() ||
		helper.growCalls[0].FSType != "ext4" {
		t.Fatalf("grow calls=%+v", helper.growCalls)
	}
}

func TestNodeRejectsConflictingStageAndPublish(t *testing.T) {
	helper := newFakeNodeHelper()
	srv := newNodeTestServer(t, helper)
	stageReq := &csipb.NodeStageVolumeRequest{
		VolumeId:          "00a1b2c5",
		StagingTargetPath: "/stage/shared",
		VolumeCapability:  blockRWO(),
	}
	if _, err := srv.NodeStageVolume(context.Background(), stageReq); err != nil {
		t.Fatalf("NodeStageVolume: %v", err)
	}
	if _, err := srv.NodeStageVolume(context.Background(), &csipb.NodeStageVolumeRequest{
		VolumeId:          "00a1b2c6",
		StagingTargetPath: "/stage/shared",
		VolumeCapability:  blockRWO(),
	}); status.Code(err) != codes.AlreadyExists {
		t.Fatalf("conflicting stage err=%v want AlreadyExists", err)
	}
	if _, err := srv.NodePublishVolume(context.Background(), &csipb.NodePublishVolumeRequest{
		VolumeId:          "00a1b2c5",
		StagingTargetPath: "/stage/shared",
		TargetPath:        "/target/shared",
		VolumeCapability:  blockRWO(),
	}); err != nil {
		t.Fatalf("NodePublishVolume: %v", err)
	}
	if _, err := srv.NodePublishVolume(context.Background(), &csipb.NodePublishVolumeRequest{
		VolumeId:          "00a1b2c5",
		StagingTargetPath: "/stage/shared",
		TargetPath:        "/target/shared",
		VolumeCapability:  mountRWOP(),
	}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("conflicting publish err=%v want FailedPrecondition", err)
	}
}
