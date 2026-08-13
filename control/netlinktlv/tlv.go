package netlinktlv

import (
	"encoding/binary"
	"errors"
	"fmt"
)

const (
	FamilyName    = "NAMRBD_CTRL"
	FamilyVersion = 1
)

const (
	nlaFNested       uint16 = 1 << 15
	nlaFNetByteorder uint16 = 1 << 14
	nlaTypeMask      uint16 = ^(nlaFNested | nlaFNetByteorder)
)

const (
	CmdUnspec               uint8 = 0
	CmdCreateDevice         uint8 = 1
	CmdDestroyDevice        uint8 = 2
	CmdConfigREST           uint8 = 3
	CmdAttach               uint8 = 4
	CmdDetach               uint8 = 5
	CmdGetStatus            uint8 = 6
	CmdListDevices          uint8 = 7
	CmdAttachManifest       uint8 = 8
	CmdDetachLocal          uint8 = 9
	CmdUpdatePathPlan       uint8 = 10
	CmdReconfigureDataPaths uint8 = 11
	CmdResizeDevice         uint8 = 12
)

const (
	AttrUnspec           uint16 = 0
	AttrDeviceID         uint16 = 1
	AttrDiskName         uint16 = 2
	AttrServers          uint16 = 3
	AttrServerEntry      uint16 = 4
	AttrAttachReq        uint16 = 5
	AttrDetachReq        uint16 = 6
	AttrDeviceStatus     uint16 = 7
	AttrDeviceList       uint16 = 8
	AttrStatus           uint16 = 9
	AttrErrMsg           uint16 = 10
	AttrManifestJSON     uint16 = 11
	AttrDownMask         uint16 = 12
	AttrDegradedMask     uint16 = 13
	AttrDrainingMask     uint16 = 14
	AttrPathPlanRevision uint16 = 15
	AttrSizeBytes        uint16 = 16
	AttrVolumeID         uint16 = 17
	AttrGeneration       uint16 = 18
)

const (
	ServerAttrUnspec      uint16 = 0
	ServerAttrID          uint16 = 1
	ServerAttrAddress     uint16 = 2
	ServerAttrPort        uint16 = 3
	ServerAttrUseTLS      uint16 = 4
	ServerAttrAPIPrefix   uint16 = 5
	ServerAttrBearerToken uint16 = 6
)

const (
	ReqAttrUnspec   uint16 = 0
	ReqAttrDeviceID uint16 = 1
	ReqAttrHostID   uint16 = 2
	ReqAttrVolumeID uint16 = 3
)

const (
	StatusAttrUnspec                  uint16 = 0
	StatusAttrDeviceID                uint16 = 1
	StatusAttrDiskName                uint16 = 2
	StatusAttrAttached                uint16 = 3
	StatusAttrVolumeID                uint16 = 4
	StatusAttrGeneration              uint16 = 5
	StatusAttrPathCount               uint16 = 6
	StatusAttrDownMask                uint16 = 7
	StatusAttrDegradedMask            uint16 = 8
	StatusAttrDrainingMask            uint16 = 9
	StatusAttrAppliedPathPlanRevision uint16 = 10
	StatusAttrActiveLaneCount         uint16 = 11
	StatusAttrNrHwQueues              uint16 = 12
	StatusAttrTargetNrHwQueues        uint16 = 13
	StatusAttrQueueTopologyGeneration uint16 = 14
	StatusAttrQueueTopologyState      uint16 = 15
	StatusAttrPaths                   uint16 = 16
	StatusAttrLanes                   uint16 = 17
	StatusAttrLaneRemapCount          uint16 = 18
	StatusAttrLastLaneRemappedLanes   uint16 = 19
	StatusAttrLastLaneRemapJiffies    uint16 = 20
	StatusAttrLastLaneRemapReason     uint16 = 21
	StatusAttrNoPathRetryMode         uint16 = 22
	StatusAttrNoPathRetrySeconds      uint16 = 23
	StatusAttrNoPathState             uint16 = 24
	StatusAttrNoPathSinceJiffies      uint16 = 25
	StatusAttrNoPathRetryDeadline     uint16 = 26
	StatusAttrLastNoPathWakeupJiffies uint16 = 27
	StatusAttrNoPathQueuedReqs        uint16 = 28
	StatusAttrNoPathRequeuedReqs      uint16 = 29
	StatusAttrNoPathFailedReqs        uint16 = 30
	StatusAttrNoPathRecoveredReqs     uint16 = 31
	StatusAttrNoPathEnterCount        uint16 = 32
	StatusAttrLastNoPathReason        uint16 = 33
	StatusAttrLastNoPathOp            uint16 = 34
	StatusAttrLastNoPathEligiblePaths uint16 = 35
	StatusAttrLastNoPathTriedMask     uint16 = 36
	StatusAttrLastNoPathJiffies       uint16 = 37
)

const (
	StatusPathAttrUnspec            uint16 = 0
	StatusPathAttrEntry             uint16 = 1
	StatusPathAttrPathID            uint16 = 2
	StatusPathAttrState             uint16 = 3
	StatusPathAttrConsecutiveErrors uint16 = 4
	StatusPathAttrLastErrno         uint16 = 5
	StatusPathAttrLastWireStatus    uint16 = 6
	StatusPathAttrGatewayID         uint16 = 7
	StatusPathAttrAddress           uint16 = 8
	StatusPathAttrPort              uint16 = 9
	StatusPathAttrUseTLS            uint16 = 10
	StatusPathAttrServerName        uint16 = 11
	StatusPathAttrPriority          uint16 = 12
	StatusPathAttrConnected         uint16 = 13
	StatusPathAttrInflight          uint16 = 14
	StatusPathAttrPending           uint16 = 15
	StatusPathAttrOutstandingLimit  uint16 = 16
	StatusPathAttrCompleted         uint16 = 17
	StatusPathAttrRetries           uint16 = 18
	StatusPathAttrConnOpens         uint16 = 19
	StatusPathAttrConnResets        uint16 = 20
	StatusPathAttrPendingHighWater  uint16 = 21
	StatusPathAttrSubmitted         uint16 = 22
)

const (
	StatusLaneAttrUnspec          uint16 = 0
	StatusLaneAttrEntry           uint16 = 1
	StatusLaneAttrLaneID          uint16 = 2
	StatusLaneAttrPreferredPathID uint16 = 3
	StatusLaneAttrFallbackPathID  uint16 = 4
	StatusLaneAttrReadiness       uint16 = 5
	StatusLaneAttrDispatchReqs    uint16 = 6
)

type RESTServer struct {
	ID          uint32
	Address     string
	Port        uint16
	UseTLS      bool
	APIPrefix   string
	BearerToken string
}

type CreateDeviceResponse struct {
	DeviceID uint32
	DiskName string
}

type DestroyDeviceRequest struct {
	DeviceID uint32
}

type ConfigRESTRequest struct {
	DeviceID uint32
	Servers  []RESTServer
}

type AttachRequest struct {
	DeviceID uint32
	HostID   string
	VolumeID uint64
}

type DetachRequest struct {
	DeviceID uint32
	HostID   string
	VolumeID uint64
}

type AttachManifestRequest struct {
	DeviceID     uint32
	HostID       string
	VolumeID     uint64
	ManifestJSON string
}

type DetachLocalRequest struct {
	DeviceID uint32
	VolumeID uint64
}

type UpdatePathPlanRequest struct {
	DeviceID         uint32
	PathPlanRevision uint64
	DownMask         uint64
	DegradedMask     uint64
	DrainingMask     uint64
}

type ResizeDeviceRequest struct {
	DeviceID   uint32
	VolumeID   uint64
	Generation uint64
	SizeBytes  uint64
}

type DeviceStatus struct {
	DeviceID                uint32
	DiskName                string
	Attached                bool
	VolumeID                uint64
	Generation              uint64
	PathCount               uint32
	DownMask                uint64
	DegradedMask            uint64
	DrainingMask            uint64
	AppliedPathPlanRevision uint64
	ActiveLaneCount         uint32
	NrHwQueues              uint32
	TargetNrHwQueues        uint32
	QueueTopologyGeneration uint64
	QueueTopologyState      string
	LaneRemapCount          uint64
	LastLaneRemappedLanes   uint32
	LastLaneRemapJiffies    uint64
	LastLaneRemapReason     string
	NoPathRetryMode         uint32
	NoPathRetrySeconds      uint32
	NoPathState             uint32
	NoPathSinceJiffies      uint64
	NoPathRetryDeadline     uint64
	LastNoPathWakeupJiffies uint64
	NoPathQueuedReqs        uint64
	NoPathRequeuedReqs      uint64
	NoPathFailedReqs        uint64
	NoPathRecoveredReqs     uint64
	NoPathEnterCount        uint64
	LastNoPathReason        uint32
	LastNoPathOp            uint32
	LastNoPathEligiblePaths uint32
	LastNoPathTriedMask     uint64
	LastNoPathJiffies       uint64
	Paths                   []PathStatus
	Lanes                   []LaneStatus
}

type PathStatus struct {
	PathID            uint32
	State             uint32
	ConsecutiveErrors uint32
	LastErrno         uint32
	LastWireStatus    uint32
	GatewayID         string
	Address           string
	Port              uint16
	UseTLS            bool
	ServerName        string
	Priority          uint32
	Connected         bool
	Inflight          uint32
	Pending           uint32
	PendingHighWater  uint32
	OutstandingLimit  uint32
	Submitted         uint64
	Completed         uint64
	Retries           uint64
	ConnOpens         uint64
	ConnResets        uint64
}

type LaneStatus struct {
	LaneID          uint32
	PreferredPathID uint32
	FallbackPathID  uint32
	Readiness       uint32
	DispatchReqs    uint64
}

type ListDevicesResponse struct {
	Devices []DeviceStatus
}

func EncodeCreateDeviceRequest() ([]byte, error) {
	return nil, nil
}

func DecodeCreateDeviceResponse(payload []byte) (CreateDeviceResponse, error) {
	attrs, err := parseAttrs(payload)
	if err != nil {
		return CreateDeviceResponse{}, err
	}
	deviceIDRaw, ok := attrs[AttrDeviceID]
	if !ok || len(deviceIDRaw) != 4 {
		return CreateDeviceResponse{}, errors.New("missing AttrDeviceID")
	}
	diskName, err := parseCString(attrs[AttrDiskName])
	if err != nil {
		return CreateDeviceResponse{}, err
	}
	return CreateDeviceResponse{
		DeviceID: binary.LittleEndian.Uint32(deviceIDRaw),
		DiskName: diskName,
	}, nil
}

func EncodeDestroyDeviceRequest(req DestroyDeviceRequest) ([]byte, error) {
	if req.DeviceID == 0 && false {
		return nil, nil
	}
	return encodeAttrU32(AttrDeviceID, req.DeviceID), nil
}

func EncodeConfigREST(req ConfigRESTRequest) ([]byte, error) {
	var listPayload []byte

	for _, s := range req.Servers {
		entry, err := encodeServerEntry(s)
		if err != nil {
			return nil, err
		}
		listPayload = append(listPayload, entry...)
	}

	payload := encodeAttrU32(AttrDeviceID, req.DeviceID)
	payload = append(payload, encodeNestedAttr(AttrServers, listPayload)...)
	return payload, nil
}

func DecodeConfigRESTRequest(payload []byte) (ConfigRESTRequest, error) {
	top, err := parseAttrs(payload)
	if err != nil {
		return ConfigRESTRequest{}, err
	}
	deviceIDRaw, ok := top[AttrDeviceID]
	if !ok || len(deviceIDRaw) != 4 {
		return ConfigRESTRequest{}, errors.New("missing AttrDeviceID")
	}
	rawServers, ok := top[AttrServers]
	if !ok {
		return ConfigRESTRequest{}, errors.New("missing AttrServers")
	}
	items, err := parseAttrList(rawServers)
	if err != nil {
		return ConfigRESTRequest{}, err
	}
	out := ConfigRESTRequest{
		DeviceID: binary.LittleEndian.Uint32(deviceIDRaw),
		Servers:  make([]RESTServer, 0, len(items)),
	}
	for _, item := range items {
		if item.typ != AttrServerEntry {
			continue
		}
		s, err := decodeServerEntry(item.payload)
		if err != nil {
			return ConfigRESTRequest{}, err
		}
		out.Servers = append(out.Servers, s)
	}
	return out, nil
}

func EncodeAttachRequest(req AttachRequest) ([]byte, error) {
	if req.HostID == "" {
		return nil, errors.New("host_id is required")
	}
	payload := encodeAttrU32(ReqAttrDeviceID, req.DeviceID)
	payload = append(payload, encodeAttr(ReqAttrHostID, append([]byte(req.HostID), 0))...)
	payload = append(payload, encodeAttrU64(ReqAttrVolumeID, req.VolumeID)...)
	return encodeNestedAttr(AttrAttachReq, payload), nil
}

func EncodeDetachRequest(req DetachRequest) ([]byte, error) {
	if req.HostID == "" {
		return nil, errors.New("host_id is required")
	}
	payload := encodeAttrU32(ReqAttrDeviceID, req.DeviceID)
	payload = append(payload, encodeAttr(ReqAttrHostID, append([]byte(req.HostID), 0))...)
	payload = append(payload, encodeAttrU64(ReqAttrVolumeID, req.VolumeID)...)
	return encodeNestedAttr(AttrDetachReq, payload), nil
}

func EncodeGetStatusRequest(deviceID uint32) ([]byte, error) {
	return encodeAttrU32(AttrDeviceID, deviceID), nil
}

func EncodeAttachManifestRequest(req AttachManifestRequest) ([]byte, error) {
	if req.HostID == "" {
		return nil, errors.New("host_id is required")
	}
	if req.ManifestJSON == "" {
		return nil, errors.New("manifest_json is required")
	}
	payload := encodeAttrU32(ReqAttrDeviceID, req.DeviceID)
	payload = append(payload, encodeAttr(ReqAttrHostID, append([]byte(req.HostID), 0))...)
	payload = append(payload, encodeAttrU64(ReqAttrVolumeID, req.VolumeID)...)
	payload = append(payload, encodeAttr(AttrManifestJSON, append([]byte(req.ManifestJSON), 0))...)
	return encodeNestedAttr(AttrAttachReq, payload), nil
}

func EncodeDetachLocalRequest(req DetachLocalRequest) ([]byte, error) {
	payload := encodeAttrU32(ReqAttrDeviceID, req.DeviceID)
	payload = append(payload, encodeAttrU64(ReqAttrVolumeID, req.VolumeID)...)
	return encodeNestedAttr(AttrDetachReq, payload), nil
}

func EncodeUpdatePathPlanRequest(req UpdatePathPlanRequest) ([]byte, error) {
	payload := encodeAttrU32(AttrDeviceID, req.DeviceID)
	payload = append(payload, encodeAttrU64(AttrPathPlanRevision, req.PathPlanRevision)...)
	payload = append(payload, encodeAttrU64(AttrDownMask, req.DownMask)...)
	payload = append(payload, encodeAttrU64(AttrDegradedMask, req.DegradedMask)...)
	payload = append(payload, encodeAttrU64(AttrDrainingMask, req.DrainingMask)...)
	return payload, nil
}

func EncodeResizeDeviceRequest(req ResizeDeviceRequest) ([]byte, error) {
	if req.VolumeID == 0 {
		return nil, errors.New("volume_id is required")
	}
	if req.Generation == 0 {
		return nil, errors.New("generation is required")
	}
	if req.SizeBytes == 0 {
		return nil, errors.New("size_bytes is required")
	}
	payload := encodeAttrU32(AttrDeviceID, req.DeviceID)
	payload = append(payload, encodeAttrU64(AttrVolumeID, req.VolumeID)...)
	payload = append(payload, encodeAttrU64(AttrGeneration, req.Generation)...)
	payload = append(payload, encodeAttrU64(AttrSizeBytes, req.SizeBytes)...)
	return payload, nil
}

func DecodeDeviceStatus(payload []byte) (DeviceStatus, error) {
	top, err := parseAttrs(payload)
	if err != nil {
		return DeviceStatus{}, err
	}
	raw, ok := top[AttrDeviceStatus]
	if !ok {
		return DeviceStatus{}, errors.New("missing AttrDeviceStatus")
	}
	return decodeDeviceStatus(raw)
}

func DecodeListDevices(payload []byte) (ListDevicesResponse, error) {
	top, err := parseAttrs(payload)
	if err != nil {
		return ListDevicesResponse{}, err
	}
	raw, ok := top[AttrDeviceList]
	if !ok {
		return ListDevicesResponse{}, errors.New("missing AttrDeviceList")
	}
	items, err := parseAttrList(raw)
	if err != nil {
		return ListDevicesResponse{}, err
	}
	out := ListDevicesResponse{Devices: make([]DeviceStatus, 0, len(items))}
	for _, item := range items {
		if item.typ != AttrDeviceStatus {
			continue
		}
		st, err := decodeDeviceStatus(item.payload)
		if err != nil {
			return ListDevicesResponse{}, err
		}
		out.Devices = append(out.Devices, st)
	}
	return out, nil
}

func decodeDeviceStatus(raw []byte) (DeviceStatus, error) {
	attrs, err := parseAttrs(raw)
	if err != nil {
		return DeviceStatus{}, err
	}
	deviceIDRaw, ok := attrs[StatusAttrDeviceID]
	if !ok || len(deviceIDRaw) != 4 {
		return DeviceStatus{}, errors.New("invalid StatusAttrDeviceID")
	}
	diskName, err := parseCString(attrs[StatusAttrDiskName])
	if err != nil {
		return DeviceStatus{}, err
	}
	attachedRaw, ok := attrs[StatusAttrAttached]
	if !ok || len(attachedRaw) != 1 {
		return DeviceStatus{}, errors.New("invalid StatusAttrAttached")
	}
	volumeIDRaw, ok := attrs[StatusAttrVolumeID]
	if !ok || len(volumeIDRaw) != 8 {
		return DeviceStatus{}, errors.New("invalid StatusAttrVolumeID")
	}
	generationRaw, ok := attrs[StatusAttrGeneration]
	if !ok || len(generationRaw) != 8 {
		return DeviceStatus{}, errors.New("invalid StatusAttrGeneration")
	}
	pathCountRaw, ok := attrs[StatusAttrPathCount]
	if !ok || len(pathCountRaw) != 4 {
		return DeviceStatus{}, errors.New("invalid StatusAttrPathCount")
	}
	downMaskRaw, ok := attrs[StatusAttrDownMask]
	if !ok || len(downMaskRaw) != 8 {
		return DeviceStatus{}, errors.New("invalid StatusAttrDownMask")
	}
	degradedMaskRaw, ok := attrs[StatusAttrDegradedMask]
	if !ok || len(degradedMaskRaw) != 8 {
		return DeviceStatus{}, errors.New("invalid StatusAttrDegradedMask")
	}
	drainingMaskRaw, ok := attrs[StatusAttrDrainingMask]
	if !ok || len(drainingMaskRaw) != 8 {
		return DeviceStatus{}, errors.New("invalid StatusAttrDrainingMask")
	}
	appliedRevisionRaw, ok := attrs[StatusAttrAppliedPathPlanRevision]
	if !ok || len(appliedRevisionRaw) != 8 {
		return DeviceStatus{}, errors.New("invalid StatusAttrAppliedPathPlanRevision")
	}
	activeLaneCountRaw, ok := attrs[StatusAttrActiveLaneCount]
	if !ok || len(activeLaneCountRaw) != 4 {
		return DeviceStatus{}, errors.New("invalid StatusAttrActiveLaneCount")
	}
	nrHwQueuesRaw, ok := attrs[StatusAttrNrHwQueues]
	if !ok || len(nrHwQueuesRaw) != 4 {
		return DeviceStatus{}, errors.New("invalid StatusAttrNrHwQueues")
	}
	targetNrHwQueuesRaw, ok := attrs[StatusAttrTargetNrHwQueues]
	if !ok || len(targetNrHwQueuesRaw) != 4 {
		return DeviceStatus{}, errors.New("invalid StatusAttrTargetNrHwQueues")
	}
	queueTopologyGenerationRaw, ok := attrs[StatusAttrQueueTopologyGeneration]
	if !ok || len(queueTopologyGenerationRaw) != 8 {
		return DeviceStatus{}, errors.New("invalid StatusAttrQueueTopologyGeneration")
	}
	queueTopologyState, err := parseCString(attrs[StatusAttrQueueTopologyState])
	if err != nil {
		return DeviceStatus{}, errors.New("invalid StatusAttrQueueTopologyState")
	}
	laneRemapCountRaw, ok := attrs[StatusAttrLaneRemapCount]
	if !ok || len(laneRemapCountRaw) != 8 {
		return DeviceStatus{}, errors.New("invalid StatusAttrLaneRemapCount")
	}
	lastLaneRemappedLanesRaw, ok := attrs[StatusAttrLastLaneRemappedLanes]
	if !ok || len(lastLaneRemappedLanesRaw) != 4 {
		return DeviceStatus{}, errors.New("invalid StatusAttrLastLaneRemappedLanes")
	}
	lastLaneRemapJiffiesRaw, ok := attrs[StatusAttrLastLaneRemapJiffies]
	if !ok || len(lastLaneRemapJiffiesRaw) != 8 {
		return DeviceStatus{}, errors.New("invalid StatusAttrLastLaneRemapJiffies")
	}
	lastLaneRemapReason, err := parseCString(attrs[StatusAttrLastLaneRemapReason])
	if err != nil {
		return DeviceStatus{}, errors.New("invalid StatusAttrLastLaneRemapReason")
	}
	pathStatuses, err := decodePathStatuses(attrs[StatusAttrPaths])
	if err != nil {
		return DeviceStatus{}, err
	}
	laneStatuses, err := decodeLaneStatuses(attrs[StatusAttrLanes])
	if err != nil {
		return DeviceStatus{}, err
	}
	getU32 := func(attr uint16) uint32 {
		raw := attrs[attr]
		if len(raw) != 4 {
			return 0
		}
		return binary.LittleEndian.Uint32(raw)
	}
	getU64 := func(attr uint16) uint64 {
		raw := attrs[attr]
		if len(raw) != 8 {
			return 0
		}
		return binary.LittleEndian.Uint64(raw)
	}
	return DeviceStatus{
		DeviceID:                binary.LittleEndian.Uint32(deviceIDRaw),
		DiskName:                diskName,
		Attached:                attachedRaw[0] != 0,
		VolumeID:                binary.LittleEndian.Uint64(volumeIDRaw),
		Generation:              binary.LittleEndian.Uint64(generationRaw),
		PathCount:               binary.LittleEndian.Uint32(pathCountRaw),
		DownMask:                binary.LittleEndian.Uint64(downMaskRaw),
		DegradedMask:            binary.LittleEndian.Uint64(degradedMaskRaw),
		DrainingMask:            binary.LittleEndian.Uint64(drainingMaskRaw),
		AppliedPathPlanRevision: binary.LittleEndian.Uint64(appliedRevisionRaw),
		ActiveLaneCount:         binary.LittleEndian.Uint32(activeLaneCountRaw),
		NrHwQueues:              binary.LittleEndian.Uint32(nrHwQueuesRaw),
		TargetNrHwQueues:        binary.LittleEndian.Uint32(targetNrHwQueuesRaw),
		QueueTopologyGeneration: binary.LittleEndian.Uint64(queueTopologyGenerationRaw),
		QueueTopologyState:      queueTopologyState,
		LaneRemapCount:          binary.LittleEndian.Uint64(laneRemapCountRaw),
		LastLaneRemappedLanes:   binary.LittleEndian.Uint32(lastLaneRemappedLanesRaw),
		LastLaneRemapJiffies:    binary.LittleEndian.Uint64(lastLaneRemapJiffiesRaw),
		LastLaneRemapReason:     lastLaneRemapReason,
		NoPathRetryMode:         getU32(StatusAttrNoPathRetryMode),
		NoPathRetrySeconds:      getU32(StatusAttrNoPathRetrySeconds),
		NoPathState:             getU32(StatusAttrNoPathState),
		NoPathSinceJiffies:      getU64(StatusAttrNoPathSinceJiffies),
		NoPathRetryDeadline:     getU64(StatusAttrNoPathRetryDeadline),
		LastNoPathWakeupJiffies: getU64(StatusAttrLastNoPathWakeupJiffies),
		NoPathQueuedReqs:        getU64(StatusAttrNoPathQueuedReqs),
		NoPathRequeuedReqs:      getU64(StatusAttrNoPathRequeuedReqs),
		NoPathFailedReqs:        getU64(StatusAttrNoPathFailedReqs),
		NoPathRecoveredReqs:     getU64(StatusAttrNoPathRecoveredReqs),
		NoPathEnterCount:        getU64(StatusAttrNoPathEnterCount),
		LastNoPathReason:        getU32(StatusAttrLastNoPathReason),
		LastNoPathOp:            getU32(StatusAttrLastNoPathOp),
		LastNoPathEligiblePaths: getU32(StatusAttrLastNoPathEligiblePaths),
		LastNoPathTriedMask:     getU64(StatusAttrLastNoPathTriedMask),
		LastNoPathJiffies:       getU64(StatusAttrLastNoPathJiffies),
		Paths:                   pathStatuses,
		Lanes:                   laneStatuses,
	}, nil
}

func decodePathStatuses(raw []byte) ([]PathStatus, error) {
	if len(raw) == 0 {
		return nil, nil
	}
	items, err := parseAttrList(raw)
	if err != nil {
		return nil, err
	}
	out := make([]PathStatus, 0, len(items))
	for _, item := range items {
		if item.typ != StatusPathAttrEntry {
			continue
		}
		attrs, err := parseAttrs(item.payload)
		if err != nil {
			return nil, err
		}
		pathIDRaw, ok := attrs[StatusPathAttrPathID]
		if !ok || len(pathIDRaw) != 4 {
			return nil, errors.New("invalid StatusPathAttrPathID")
		}
		stateRaw, ok := attrs[StatusPathAttrState]
		if !ok || len(stateRaw) != 4 {
			return nil, errors.New("invalid StatusPathAttrState")
		}
		consecutiveRaw, ok := attrs[StatusPathAttrConsecutiveErrors]
		if !ok || len(consecutiveRaw) != 4 {
			return nil, errors.New("invalid StatusPathAttrConsecutiveErrors")
		}
		lastErrnoRaw, ok := attrs[StatusPathAttrLastErrno]
		if !ok || len(lastErrnoRaw) != 4 {
			return nil, errors.New("invalid StatusPathAttrLastErrno")
		}
		lastWireRaw, ok := attrs[StatusPathAttrLastWireStatus]
		if !ok || len(lastWireRaw) != 4 {
			return nil, errors.New("invalid StatusPathAttrLastWireStatus")
		}
		var gatewayID, address, serverName string
		var port uint16
		var useTLS bool
		var priority uint32
		var connected bool
		var inflight, pending, pendingHighWater, outstandingLimit uint32
		var submitted, completed, retries, connOpens, connResets uint64
		if raw, ok := attrs[StatusPathAttrGatewayID]; ok {
			gatewayID, err = parseCString(raw)
			if err != nil {
				return nil, errors.New("invalid StatusPathAttrGatewayID")
			}
		}
		if raw, ok := attrs[StatusPathAttrAddress]; ok {
			address, err = parseCString(raw)
			if err != nil {
				return nil, errors.New("invalid StatusPathAttrAddress")
			}
		}
		if raw, ok := attrs[StatusPathAttrPort]; ok {
			if len(raw) != 2 {
				return nil, errors.New("invalid StatusPathAttrPort")
			}
			port = binary.LittleEndian.Uint16(raw)
		}
		if raw, ok := attrs[StatusPathAttrUseTLS]; ok {
			if len(raw) != 1 {
				return nil, errors.New("invalid StatusPathAttrUseTLS")
			}
			useTLS = raw[0] != 0
		}
		if raw, ok := attrs[StatusPathAttrServerName]; ok {
			serverName, err = parseCString(raw)
			if err != nil {
				return nil, errors.New("invalid StatusPathAttrServerName")
			}
		}
		if raw, ok := attrs[StatusPathAttrPriority]; ok {
			if len(raw) != 4 {
				return nil, errors.New("invalid StatusPathAttrPriority")
			}
			priority = binary.LittleEndian.Uint32(raw)
		}
		if raw, ok := attrs[StatusPathAttrConnected]; ok {
			if len(raw) != 1 {
				return nil, errors.New("invalid StatusPathAttrConnected")
			}
			connected = raw[0] != 0
		}
		if raw, ok := attrs[StatusPathAttrInflight]; ok {
			if len(raw) != 4 {
				return nil, errors.New("invalid StatusPathAttrInflight")
			}
			inflight = binary.LittleEndian.Uint32(raw)
		}
		if raw, ok := attrs[StatusPathAttrPending]; ok {
			if len(raw) != 4 {
				return nil, errors.New("invalid StatusPathAttrPending")
			}
			pending = binary.LittleEndian.Uint32(raw)
		}
		if raw, ok := attrs[StatusPathAttrPendingHighWater]; ok {
			if len(raw) != 4 {
				return nil, errors.New("invalid StatusPathAttrPendingHighWater")
			}
			pendingHighWater = binary.LittleEndian.Uint32(raw)
		}
		if raw, ok := attrs[StatusPathAttrOutstandingLimit]; ok {
			if len(raw) != 4 {
				return nil, errors.New("invalid StatusPathAttrOutstandingLimit")
			}
			outstandingLimit = binary.LittleEndian.Uint32(raw)
		}
		if raw, ok := attrs[StatusPathAttrSubmitted]; ok {
			if len(raw) != 8 {
				return nil, errors.New("invalid StatusPathAttrSubmitted")
			}
			submitted = binary.LittleEndian.Uint64(raw)
		}
		if raw, ok := attrs[StatusPathAttrCompleted]; ok {
			if len(raw) != 8 {
				return nil, errors.New("invalid StatusPathAttrCompleted")
			}
			completed = binary.LittleEndian.Uint64(raw)
		}
		if raw, ok := attrs[StatusPathAttrRetries]; ok {
			if len(raw) != 8 {
				return nil, errors.New("invalid StatusPathAttrRetries")
			}
			retries = binary.LittleEndian.Uint64(raw)
		}
		if raw, ok := attrs[StatusPathAttrConnOpens]; ok {
			if len(raw) != 8 {
				return nil, errors.New("invalid StatusPathAttrConnOpens")
			}
			connOpens = binary.LittleEndian.Uint64(raw)
		}
		if raw, ok := attrs[StatusPathAttrConnResets]; ok {
			if len(raw) != 8 {
				return nil, errors.New("invalid StatusPathAttrConnResets")
			}
			connResets = binary.LittleEndian.Uint64(raw)
		}
		out = append(out, PathStatus{
			PathID:            binary.LittleEndian.Uint32(pathIDRaw),
			State:             binary.LittleEndian.Uint32(stateRaw),
			ConsecutiveErrors: binary.LittleEndian.Uint32(consecutiveRaw),
			LastErrno:         binary.LittleEndian.Uint32(lastErrnoRaw),
			LastWireStatus:    binary.LittleEndian.Uint32(lastWireRaw),
			GatewayID:         gatewayID,
			Address:           address,
			Port:              port,
			UseTLS:            useTLS,
			ServerName:        serverName,
			Priority:          priority,
			Connected:         connected,
			Inflight:          inflight,
			Pending:           pending,
			PendingHighWater:  pendingHighWater,
			OutstandingLimit:  outstandingLimit,
			Submitted:         submitted,
			Completed:         completed,
			Retries:           retries,
			ConnOpens:         connOpens,
			ConnResets:        connResets,
		})
	}
	return out, nil
}

func decodeLaneStatuses(raw []byte) ([]LaneStatus, error) {
	if len(raw) == 0 {
		return nil, nil
	}
	items, err := parseAttrList(raw)
	if err != nil {
		return nil, err
	}
	out := make([]LaneStatus, 0, len(items))
	for _, item := range items {
		if item.typ != StatusLaneAttrEntry {
			continue
		}
		attrs, err := parseAttrs(item.payload)
		if err != nil {
			return nil, err
		}
		laneIDRaw, ok := attrs[StatusLaneAttrLaneID]
		if !ok || len(laneIDRaw) != 4 {
			return nil, errors.New("invalid StatusLaneAttrLaneID")
		}
		preferredPathIDRaw, ok := attrs[StatusLaneAttrPreferredPathID]
		if !ok || len(preferredPathIDRaw) != 4 {
			return nil, errors.New("invalid StatusLaneAttrPreferredPathID")
		}
		fallbackPathIDRaw, ok := attrs[StatusLaneAttrFallbackPathID]
		if !ok || len(fallbackPathIDRaw) != 4 {
			return nil, errors.New("invalid StatusLaneAttrFallbackPathID")
		}
		readinessRaw, ok := attrs[StatusLaneAttrReadiness]
		if !ok || len(readinessRaw) != 4 {
			return nil, errors.New("invalid StatusLaneAttrReadiness")
		}
		var dispatchReqs uint64
		if raw, ok := attrs[StatusLaneAttrDispatchReqs]; ok {
			if len(raw) != 8 {
				return nil, errors.New("invalid StatusLaneAttrDispatchReqs")
			}
			dispatchReqs = binary.LittleEndian.Uint64(raw)
		}
		out = append(out, LaneStatus{
			LaneID:          binary.LittleEndian.Uint32(laneIDRaw),
			PreferredPathID: binary.LittleEndian.Uint32(preferredPathIDRaw),
			FallbackPathID:  binary.LittleEndian.Uint32(fallbackPathIDRaw),
			Readiness:       binary.LittleEndian.Uint32(readinessRaw),
			DispatchReqs:    dispatchReqs,
		})
	}
	return out, nil
}

func encodeServerEntry(s RESTServer) ([]byte, error) {
	if s.Address == "" {
		return nil, errors.New("server address is required")
	}
	var payload []byte
	payload = append(payload, encodeAttrU32(ServerAttrID, s.ID)...)
	payload = append(payload, encodeAttr(ServerAttrAddress, append([]byte(s.Address), 0))...)
	payload = append(payload, encodeAttrU16(ServerAttrPort, s.Port)...)
	if s.UseTLS {
		payload = append(payload, encodeAttrU8(ServerAttrUseTLS, 1)...)
	} else {
		payload = append(payload, encodeAttrU8(ServerAttrUseTLS, 0)...)
	}
	payload = append(payload, encodeAttr(ServerAttrAPIPrefix, append([]byte(s.APIPrefix), 0))...)
	if s.BearerToken != "" {
		payload = append(payload, encodeAttr(ServerAttrBearerToken, append([]byte(s.BearerToken), 0))...)
	}
	return encodeNestedAttr(AttrServerEntry, payload), nil
}

func decodeServerEntry(raw []byte) (RESTServer, error) {
	attrs, err := parseAttrs(raw)
	if err != nil {
		return RESTServer{}, err
	}
	addr, err := parseCString(attrs[ServerAttrAddress])
	if err != nil {
		return RESTServer{}, err
	}
	idRaw, ok := attrs[ServerAttrID]
	if !ok || len(idRaw) != 4 {
		return RESTServer{}, errors.New("invalid ServerAttrID")
	}
	portRaw, ok := attrs[ServerAttrPort]
	if !ok || len(portRaw) != 2 {
		return RESTServer{}, errors.New("invalid ServerAttrPort")
	}
	tlsRaw, ok := attrs[ServerAttrUseTLS]
	if !ok || len(tlsRaw) != 1 {
		return RESTServer{}, errors.New("invalid ServerAttrUseTLS")
	}
	prefix, err := parseCString(attrs[ServerAttrAPIPrefix])
	if err != nil {
		return RESTServer{}, err
	}
	var token string
	if tokRaw, ok := attrs[ServerAttrBearerToken]; ok {
		token, err = parseCString(tokRaw)
		if err != nil {
			return RESTServer{}, err
		}
	}
	return RESTServer{
		ID:          binary.LittleEndian.Uint32(idRaw),
		Address:     addr,
		Port:        binary.LittleEndian.Uint16(portRaw),
		UseTLS:      tlsRaw[0] != 0,
		APIPrefix:   prefix,
		BearerToken: token,
	}, nil
}

func parseCString(raw []byte) (string, error) {
	if len(raw) == 0 {
		return "", errors.New("empty string attr")
	}
	if raw[len(raw)-1] != 0 {
		return "", errors.New("string attr missing trailing NUL")
	}
	return string(raw[:len(raw)-1]), nil
}

func encodeAttrU64(typ uint16, v uint64) []byte {
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, v)
	return encodeAttr(typ, b)
}

func encodeAttrU32(typ uint16, v uint32) []byte {
	b := make([]byte, 4)
	binary.LittleEndian.PutUint32(b, v)
	return encodeAttr(typ, b)
}

func encodeAttrU16(typ uint16, v uint16) []byte {
	b := make([]byte, 2)
	binary.LittleEndian.PutUint16(b, v)
	return encodeAttr(typ, b)
}

func encodeAttrU8(typ uint16, v uint8) []byte {
	return encodeAttr(typ, []byte{v})
}

func encodeAttr(typ uint16, payload []byte) []byte {
	l := 4 + len(payload)
	padded := align4(l)
	b := make([]byte, padded)
	binary.LittleEndian.PutUint16(b[0:2], uint16(l))
	binary.LittleEndian.PutUint16(b[2:4], typ)
	copy(b[4:4+len(payload)], payload)
	return b
}

func encodeNestedAttr(typ uint16, payload []byte) []byte {
	return encodeAttr(typ|nlaFNested, payload)
}

func parseAttrs(data []byte) (map[uint16][]byte, error) {
	out := make(map[uint16][]byte)
	i := 0
	for i < len(data) {
		if len(data)-i < 4 {
			return nil, fmt.Errorf("short attr header at %d", i)
		}
		l := int(binary.LittleEndian.Uint16(data[i : i+2]))
		t := binary.LittleEndian.Uint16(data[i+2:i+4]) & nlaTypeMask
		if l < 4 || i+l > len(data) {
			return nil, fmt.Errorf("invalid attr len=%d at %d", l, i)
		}
		out[t] = append([]byte(nil), data[i+4:i+l]...)
		i += align4(l)
	}
	return out, nil
}

type attrItem struct {
	typ     uint16
	payload []byte
}

func parseAttrList(data []byte) ([]attrItem, error) {
	var out []attrItem
	i := 0
	for i < len(data) {
		if len(data)-i < 4 {
			return nil, fmt.Errorf("short attr header at %d", i)
		}
		l := int(binary.LittleEndian.Uint16(data[i : i+2]))
		t := binary.LittleEndian.Uint16(data[i+2:i+4]) & nlaTypeMask
		if l < 4 || i+l > len(data) {
			return nil, fmt.Errorf("invalid attr len=%d at %d", l, i)
		}
		out = append(out, attrItem{
			typ:     t,
			payload: append([]byte(nil), data[i+4:i+l]...),
		})
		i += align4(l)
	}
	return out, nil
}

func align4(n int) int {
	return (n + 3) &^ 3
}
