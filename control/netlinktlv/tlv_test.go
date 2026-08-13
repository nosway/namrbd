package netlinktlv

import "testing"

func TestConfigRESTRoundTrip(t *testing.T) {
	req := ConfigRESTRequest{
		DeviceID: 7,
		Servers: []RESTServer{
			{
				ID:          1,
				Address:     "10.0.0.10",
				Port:        9701,
				UseTLS:      true,
				APIPrefix:   "/api/v1",
				BearerToken: "tok1",
			},
		},
	}

	raw, err := EncodeConfigREST(req)
	if err != nil {
		t.Fatalf("EncodeConfigREST failed: %v", err)
	}
	attrs, err := parseAttrs(raw)
	if err != nil {
		t.Fatalf("parseAttrs failed: %v", err)
	}
	if len(attrs[AttrDeviceID]) != 4 {
		t.Fatalf("device id attr missing")
	}
	if _, ok := attrs[AttrServers]; !ok {
		t.Fatalf("servers attr missing")
	}
}

func TestAttachDetachRequestRoundTrip(t *testing.T) {
	aRaw, err := EncodeAttachRequest(AttachRequest{DeviceID: 3, HostID: "host-a", VolumeID: 101})
	if err != nil {
		t.Fatalf("EncodeAttachRequest failed: %v", err)
	}
	attrs, err := parseAttrs(aRaw)
	if err != nil {
		t.Fatalf("parseAttrs failed: %v", err)
	}
	if _, ok := attrs[AttrAttachReq]; !ok {
		t.Fatalf("attach req missing")
	}

	dRaw, err := EncodeDetachRequest(DetachRequest{DeviceID: 4, HostID: "host-a", VolumeID: 101})
	if err != nil {
		t.Fatalf("EncodeDetachRequest failed: %v", err)
	}
	attrs, err = parseAttrs(dRaw)
	if err != nil {
		t.Fatalf("parseAttrs failed: %v", err)
	}
	if _, ok := attrs[AttrDetachReq]; !ok {
		t.Fatalf("detach req missing")
	}
}

func TestAttachManifestAndDetachLocalEncoding(t *testing.T) {
	raw, err := EncodeAttachManifestRequest(AttachManifestRequest{
		DeviceID:     5,
		HostID:       "host-a",
		VolumeID:     101,
		ManifestJSON: `{"volume_id":"00000065"}`,
	})
	if err != nil {
		t.Fatalf("EncodeAttachManifestRequest failed: %v", err)
	}
	attrs, err := parseAttrs(raw)
	if err != nil {
		t.Fatalf("parseAttrs failed: %v", err)
	}
	if _, ok := attrs[AttrAttachReq]; !ok {
		t.Fatalf("attach manifest req missing")
	}

	raw, err = EncodeDetachLocalRequest(DetachLocalRequest{DeviceID: 5, VolumeID: 101})
	if err != nil {
		t.Fatalf("EncodeDetachLocalRequest failed: %v", err)
	}
	attrs, err = parseAttrs(raw)
	if err != nil {
		t.Fatalf("parseAttrs failed: %v", err)
	}
	if _, ok := attrs[AttrDetachReq]; !ok {
		t.Fatalf("detach local req missing")
	}
}

func TestUpdatePathPlanEncoding(t *testing.T) {
	raw, err := EncodeUpdatePathPlanRequest(UpdatePathPlanRequest{
		DeviceID:         5,
		PathPlanRevision: 7,
		DownMask:         0x2,
		DegradedMask:     0x4,
		DrainingMask:     0x8,
	})
	if err != nil {
		t.Fatalf("EncodeUpdatePathPlanRequest failed: %v", err)
	}
	attrs, err := parseAttrs(raw)
	if err != nil {
		t.Fatalf("parseAttrs failed: %v", err)
	}
	if len(attrs[AttrDeviceID]) != 4 || len(attrs[AttrPathPlanRevision]) != 8 || len(attrs[AttrDownMask]) != 8 || len(attrs[AttrDegradedMask]) != 8 || len(attrs[AttrDrainingMask]) != 8 {
		t.Fatalf("unexpected update path plan attrs: %+v", attrs)
	}
}

func TestResizeDeviceEncoding(t *testing.T) {
	raw, err := EncodeResizeDeviceRequest(ResizeDeviceRequest{
		DeviceID:   0,
		VolumeID:   101,
		Generation: 3,
		SizeBytes:  2 << 20,
	})
	if err != nil {
		t.Fatalf("EncodeResizeDeviceRequest failed: %v", err)
	}
	attrs, err := parseAttrs(raw)
	if err != nil {
		t.Fatalf("parseAttrs failed: %v", err)
	}
	if len(attrs[AttrDeviceID]) != 4 || len(attrs[AttrVolumeID]) != 8 || len(attrs[AttrGeneration]) != 8 || len(attrs[AttrSizeBytes]) != 8 {
		t.Fatalf("unexpected resize attrs: %+v", attrs)
	}
}

func TestCreateDeviceAndStatusDecode(t *testing.T) {
	payload := append(encodeAttrU32(AttrDeviceID, 9), encodeAttr(AttrDiskName, []byte("namrbd9\x00"))...)
	resp, err := DecodeCreateDeviceResponse(payload)
	if err != nil {
		t.Fatalf("DecodeCreateDeviceResponse failed: %v", err)
	}
	if resp.DeviceID != 9 || resp.DiskName != "namrbd9" {
		t.Fatalf("unexpected response: %+v", resp)
	}

	pathEntries := append(
		encodeNestedAttr(StatusPathAttrEntry,
			append(
				append(
					append(
						append(encodeAttrU32(StatusPathAttrPathID, 0),
							encodeAttrU32(StatusPathAttrState, 0)...),
						encodeAttrU32(StatusPathAttrConsecutiveErrors, 0)...),
					encodeAttrU32(StatusPathAttrLastErrno, 0)...),
				encodeAttrU32(StatusPathAttrLastWireStatus, 0)...)),
		encodeNestedAttr(StatusPathAttrEntry,
			append(
				append(
					append(
						append(encodeAttrU32(StatusPathAttrPathID, 1),
							encodeAttrU32(StatusPathAttrState, 1)...),
						encodeAttrU32(StatusPathAttrConsecutiveErrors, 2)...),
					encodeAttrU32(StatusPathAttrLastErrno, 5)...),
				encodeAttrU32(StatusPathAttrLastWireStatus, 7)...))...,
	)
	statusAttrs := append(encodeAttrU32(StatusAttrDeviceID, 9), encodeAttr(StatusAttrDiskName, []byte("namrbd9\x00"))...)
	statusAttrs = append(statusAttrs, encodeAttrU8(StatusAttrAttached, 1)...)
	statusAttrs = append(statusAttrs, encodeAttrU64(StatusAttrVolumeID, 101)...)
	statusAttrs = append(statusAttrs, encodeAttrU64(StatusAttrGeneration, 7)...)
	statusAttrs = append(statusAttrs, encodeAttrU32(StatusAttrPathCount, 3)...)
	statusAttrs = append(statusAttrs, encodeAttrU64(StatusAttrDownMask, 0x4)...)
	statusAttrs = append(statusAttrs, encodeAttrU64(StatusAttrDegradedMask, 0x2)...)
	statusAttrs = append(statusAttrs, encodeAttrU64(StatusAttrDrainingMask, 0x0)...)
	statusAttrs = append(statusAttrs, encodeAttrU64(StatusAttrAppliedPathPlanRevision, 9)...)
	statusAttrs = append(statusAttrs, encodeAttrU32(StatusAttrActiveLaneCount, 2)...)
	statusAttrs = append(statusAttrs, encodeAttrU32(StatusAttrNrHwQueues, 1)...)
	statusAttrs = append(statusAttrs, encodeAttrU32(StatusAttrTargetNrHwQueues, 2)...)
	statusAttrs = append(statusAttrs, encodeAttrU64(StatusAttrQueueTopologyGeneration, 3)...)
	statusAttrs = append(statusAttrs, encodeAttr(StatusAttrQueueTopologyState, []byte("planned\x00"))...)
	statusAttrs = append(statusAttrs, encodeAttrU64(StatusAttrLaneRemapCount, 4)...)
	statusAttrs = append(statusAttrs, encodeAttrU32(StatusAttrLastLaneRemappedLanes, 1)...)
	statusAttrs = append(statusAttrs, encodeAttrU64(StatusAttrLastLaneRemapJiffies, 99)...)
	statusAttrs = append(statusAttrs, encodeAttr(StatusAttrLastLaneRemapReason, []byte("path_plan_apply\x00"))...)
	statusAttrs = append(statusAttrs, encodeAttrU32(StatusAttrNoPathRetryMode, 2)...)
	statusAttrs = append(statusAttrs, encodeAttrU32(StatusAttrNoPathRetrySeconds, 30)...)
	statusAttrs = append(statusAttrs, encodeAttrU32(StatusAttrNoPathState, 3)...)
	statusAttrs = append(statusAttrs, encodeAttrU64(StatusAttrNoPathSinceJiffies, 1001)...)
	statusAttrs = append(statusAttrs, encodeAttrU64(StatusAttrNoPathRetryDeadline, 2001)...)
	statusAttrs = append(statusAttrs, encodeAttrU64(StatusAttrLastNoPathWakeupJiffies, 1501)...)
	statusAttrs = append(statusAttrs, encodeAttrU64(StatusAttrNoPathQueuedReqs, 11)...)
	statusAttrs = append(statusAttrs, encodeAttrU64(StatusAttrNoPathRequeuedReqs, 12)...)
	statusAttrs = append(statusAttrs, encodeAttrU64(StatusAttrNoPathFailedReqs, 13)...)
	statusAttrs = append(statusAttrs, encodeAttrU64(StatusAttrNoPathRecoveredReqs, 14)...)
	statusAttrs = append(statusAttrs, encodeAttrU64(StatusAttrNoPathEnterCount, 15)...)
	statusAttrs = append(statusAttrs, encodeAttrU32(StatusAttrLastNoPathReason, 3)...)
	statusAttrs = append(statusAttrs, encodeAttrU32(StatusAttrLastNoPathOp, 1)...)
	statusAttrs = append(statusAttrs, encodeAttrU32(StatusAttrLastNoPathEligiblePaths, 0)...)
	statusAttrs = append(statusAttrs, encodeAttrU64(StatusAttrLastNoPathTriedMask, 0x3)...)
	statusAttrs = append(statusAttrs, encodeAttrU64(StatusAttrLastNoPathJiffies, 1002)...)
	statusAttrs = append(statusAttrs, encodeNestedAttr(StatusAttrPaths, pathEntries)...)
	statusAttrs = append(statusAttrs, encodeNestedAttr(StatusAttrLanes,
		append(
			encodeNestedAttr(StatusLaneAttrEntry,
				append(
					append(
						encodeAttrU32(StatusLaneAttrLaneID, 0),
						encodeAttrU32(StatusLaneAttrPreferredPathID, 0)...,
					),
					append(
						encodeAttrU32(StatusLaneAttrFallbackPathID, 1),
						encodeAttrU32(StatusLaneAttrReadiness, 1)...,
					)...,
				),
			),
			encodeNestedAttr(StatusLaneAttrEntry,
				append(
					append(
						encodeAttrU32(StatusLaneAttrLaneID, 1),
						encodeAttrU32(StatusLaneAttrPreferredPathID, 1)...,
					),
					append(
						encodeAttrU32(StatusLaneAttrFallbackPathID, ^uint32(0)),
						encodeAttrU32(StatusLaneAttrReadiness, 3)...,
					)...,
				),
			)...,
		),
	)...)
	statusPayload := encodeNestedAttr(AttrDeviceStatus, statusAttrs)
	st, err := DecodeDeviceStatus(statusPayload)
	if err != nil {
		t.Fatalf("DecodeDeviceStatus failed: %v", err)
	}
	if st.DeviceID != 9 || st.DiskName != "namrbd9" || !st.Attached || st.VolumeID != 101 || st.Generation != 7 || st.PathCount != 3 || st.DownMask != 0x4 || st.DegradedMask != 0x2 || st.DrainingMask != 0 || st.AppliedPathPlanRevision != 9 || st.ActiveLaneCount != 2 || st.NrHwQueues != 1 || st.TargetNrHwQueues != 2 || st.QueueTopologyGeneration != 3 || st.QueueTopologyState != "planned" || st.LaneRemapCount != 4 || st.LastLaneRemappedLanes != 1 || st.LastLaneRemapJiffies != 99 || st.LastLaneRemapReason != "path_plan_apply" || st.NoPathRetryMode != 2 || st.NoPathRetrySeconds != 30 || st.NoPathState != 3 || st.NoPathSinceJiffies != 1001 || st.NoPathRetryDeadline != 2001 || st.LastNoPathWakeupJiffies != 1501 || st.NoPathQueuedReqs != 11 || st.NoPathRequeuedReqs != 12 || st.NoPathFailedReqs != 13 || st.NoPathRecoveredReqs != 14 || st.NoPathEnterCount != 15 || st.LastNoPathReason != 3 || st.LastNoPathOp != 1 || st.LastNoPathEligiblePaths != 0 || st.LastNoPathTriedMask != 0x3 || st.LastNoPathJiffies != 1002 || len(st.Paths) != 2 || len(st.Lanes) != 2 || st.Lanes[0].FallbackPathID != 1 || st.Lanes[0].Readiness != 1 || st.Lanes[1].LaneID != 1 || st.Lanes[1].PreferredPathID != 1 || st.Lanes[1].FallbackPathID != ^uint32(0) || st.Lanes[1].Readiness != 3 || st.Paths[1].PathID != 1 || st.Paths[1].State != 1 || st.Paths[1].ConsecutiveErrors != 2 {
		t.Fatalf("unexpected status: %+v", st)
	}
}

func TestDecodePathStatusAsyncCounters(t *testing.T) {
	entry := encodeAttrU32(StatusPathAttrPathID, 2)
	entry = append(entry, encodeAttrU32(StatusPathAttrState, 0)...)
	entry = append(entry, encodeAttrU32(StatusPathAttrConsecutiveErrors, 1)...)
	entry = append(entry, encodeAttrU32(StatusPathAttrLastErrno, 5)...)
	entry = append(entry, encodeAttrU32(StatusPathAttrLastWireStatus, 14)...)
	entry = append(entry, encodeAttrU8(StatusPathAttrConnected, 1)...)
	entry = append(entry, encodeAttrU32(StatusPathAttrInflight, 3)...)
	entry = append(entry, encodeAttrU32(StatusPathAttrPending, 2)...)
	entry = append(entry, encodeAttrU32(StatusPathAttrPendingHighWater, 12)...)
	entry = append(entry, encodeAttrU32(StatusPathAttrOutstandingLimit, 16)...)
	entry = append(entry, encodeAttrU64(StatusPathAttrSubmitted, 4321)...)
	entry = append(entry, encodeAttrU64(StatusPathAttrCompleted, 1234)...)
	entry = append(entry, encodeAttrU64(StatusPathAttrRetries, 5)...)
	entry = append(entry, encodeAttrU64(StatusPathAttrConnOpens, 2)...)
	entry = append(entry, encodeAttrU64(StatusPathAttrConnResets, 1)...)

	paths, err := decodePathStatuses(encodeNestedAttr(StatusPathAttrEntry, entry))
	if err != nil {
		t.Fatalf("decodePathStatuses failed: %v", err)
	}
	if len(paths) != 1 {
		t.Fatalf("unexpected path count: %+v", paths)
	}
	path := paths[0]
	if path.PathID != 2 || path.State != 0 || path.ConsecutiveErrors != 1 || path.LastErrno != 5 || path.LastWireStatus != 14 || !path.Connected || path.Inflight != 3 || path.Pending != 2 || path.PendingHighWater != 12 || path.OutstandingLimit != 16 || path.Submitted != 4321 || path.Completed != 1234 || path.Retries != 5 || path.ConnOpens != 2 || path.ConnResets != 1 {
		t.Fatalf("unexpected async path status: %+v", path)
	}
}

func TestListDevicesDecode(t *testing.T) {
	entry1Paths := encodeNestedAttr(StatusPathAttrEntry,
		append(
			append(
				append(
					append(encodeAttrU32(StatusPathAttrPathID, 0),
						encodeAttrU32(StatusPathAttrState, 0)...),
					encodeAttrU32(StatusPathAttrConsecutiveErrors, 0)...),
				encodeAttrU32(StatusPathAttrLastErrno, 0)...),
			encodeAttrU32(StatusPathAttrLastWireStatus, 0)...))
	entry1Attrs := append(encodeAttrU32(StatusAttrDeviceID, 0), encodeAttr(StatusAttrDiskName, []byte("namrbd0\x00"))...)
	entry1Attrs = append(entry1Attrs, encodeAttrU8(StatusAttrAttached, 0)...)
	entry1Attrs = append(entry1Attrs, encodeAttrU64(StatusAttrVolumeID, 0)...)
	entry1Attrs = append(entry1Attrs, encodeAttrU64(StatusAttrGeneration, 1)...)
	entry1Attrs = append(entry1Attrs, encodeAttrU32(StatusAttrPathCount, 2)...)
	entry1Attrs = append(entry1Attrs, encodeAttrU64(StatusAttrDownMask, 0x0)...)
	entry1Attrs = append(entry1Attrs, encodeAttrU64(StatusAttrDegradedMask, 0x0)...)
	entry1Attrs = append(entry1Attrs, encodeAttrU64(StatusAttrDrainingMask, 0x0)...)
	entry1Attrs = append(entry1Attrs, encodeAttrU64(StatusAttrAppliedPathPlanRevision, 3)...)
	entry1Attrs = append(entry1Attrs, encodeAttrU32(StatusAttrActiveLaneCount, 1)...)
	entry1Attrs = append(entry1Attrs, encodeAttrU32(StatusAttrNrHwQueues, 1)...)
	entry1Attrs = append(entry1Attrs, encodeAttrU32(StatusAttrTargetNrHwQueues, 1)...)
	entry1Attrs = append(entry1Attrs, encodeAttrU64(StatusAttrQueueTopologyGeneration, 0)...)
	entry1Attrs = append(entry1Attrs, encodeAttr(StatusAttrQueueTopologyState, []byte("stable\x00"))...)
	entry1Attrs = append(entry1Attrs, encodeAttrU64(StatusAttrLaneRemapCount, 0)...)
	entry1Attrs = append(entry1Attrs, encodeAttrU32(StatusAttrLastLaneRemappedLanes, 0)...)
	entry1Attrs = append(entry1Attrs, encodeAttrU64(StatusAttrLastLaneRemapJiffies, 0)...)
	entry1Attrs = append(entry1Attrs, encodeAttr(StatusAttrLastLaneRemapReason, []byte("\x00"))...)
	entry1Attrs = append(entry1Attrs, encodeNestedAttr(StatusAttrPaths, entry1Paths)...)
	entry1Attrs = append(entry1Attrs, encodeNestedAttr(StatusAttrLanes,
		encodeNestedAttr(StatusLaneAttrEntry,
			append(
				append(
					encodeAttrU32(StatusLaneAttrLaneID, 0),
					encodeAttrU32(StatusLaneAttrPreferredPathID, 0)...,
				),
				append(
					encodeAttrU32(StatusLaneAttrFallbackPathID, ^uint32(0)),
					encodeAttrU32(StatusLaneAttrReadiness, 4)...,
				)...,
			),
		),
	)...)
	entry1 := encodeNestedAttr(AttrDeviceStatus, entry1Attrs)

	entry2Paths := encodeNestedAttr(StatusPathAttrEntry,
		append(
			append(
				append(
					append(encodeAttrU32(StatusPathAttrPathID, 0),
						encodeAttrU32(StatusPathAttrState, 3)...),
					encodeAttrU32(StatusPathAttrConsecutiveErrors, 0)...),
				encodeAttrU32(StatusPathAttrLastErrno, 0)...),
			encodeAttrU32(StatusPathAttrLastWireStatus, 0)...))
	entry2Paths = append(entry2Paths, encodeNestedAttr(StatusPathAttrEntry,
		append(
			append(
				append(
					append(encodeAttrU32(StatusPathAttrPathID, 1),
						encodeAttrU32(StatusPathAttrState, 1)...),
					encodeAttrU32(StatusPathAttrConsecutiveErrors, 1)...),
				encodeAttrU32(StatusPathAttrLastErrno, 110)...),
			encodeAttrU32(StatusPathAttrLastWireStatus, 9)...))...)
	entry2Paths = append(entry2Paths, encodeNestedAttr(StatusPathAttrEntry,
		append(
			append(
				append(
					append(encodeAttrU32(StatusPathAttrPathID, 2),
						encodeAttrU32(StatusPathAttrState, 2)...),
					encodeAttrU32(StatusPathAttrConsecutiveErrors, 3)...),
				encodeAttrU32(StatusPathAttrLastErrno, 5)...),
			encodeAttrU32(StatusPathAttrLastWireStatus, 11)...))...)
	entry2Attrs := append(encodeAttrU32(StatusAttrDeviceID, 1), encodeAttr(StatusAttrDiskName, []byte("namrbd1\x00"))...)
	entry2Attrs = append(entry2Attrs, encodeAttrU8(StatusAttrAttached, 1)...)
	entry2Attrs = append(entry2Attrs, encodeAttrU64(StatusAttrVolumeID, 202)...)
	entry2Attrs = append(entry2Attrs, encodeAttrU64(StatusAttrGeneration, 3)...)
	entry2Attrs = append(entry2Attrs, encodeAttrU32(StatusAttrPathCount, 3)...)
	entry2Attrs = append(entry2Attrs, encodeAttrU64(StatusAttrDownMask, 0x4)...)
	entry2Attrs = append(entry2Attrs, encodeAttrU64(StatusAttrDegradedMask, 0x2)...)
	entry2Attrs = append(entry2Attrs, encodeAttrU64(StatusAttrDrainingMask, 0x1)...)
	entry2Attrs = append(entry2Attrs, encodeAttrU64(StatusAttrAppliedPathPlanRevision, 8)...)
	entry2Attrs = append(entry2Attrs, encodeAttrU32(StatusAttrActiveLaneCount, 2)...)
	entry2Attrs = append(entry2Attrs, encodeAttrU32(StatusAttrNrHwQueues, 1)...)
	entry2Attrs = append(entry2Attrs, encodeAttrU32(StatusAttrTargetNrHwQueues, 2)...)
	entry2Attrs = append(entry2Attrs, encodeAttrU64(StatusAttrQueueTopologyGeneration, 4)...)
	entry2Attrs = append(entry2Attrs, encodeAttr(StatusAttrQueueTopologyState, []byte("planned\x00"))...)
	entry2Attrs = append(entry2Attrs, encodeAttrU64(StatusAttrLaneRemapCount, 3)...)
	entry2Attrs = append(entry2Attrs, encodeAttrU32(StatusAttrLastLaneRemappedLanes, 2)...)
	entry2Attrs = append(entry2Attrs, encodeAttrU64(StatusAttrLastLaneRemapJiffies, 44)...)
	entry2Attrs = append(entry2Attrs, encodeAttr(StatusAttrLastLaneRemapReason, []byte("path_state_change\x00"))...)
	entry2Attrs = append(entry2Attrs, encodeNestedAttr(StatusAttrPaths, entry2Paths)...)
	entry2Attrs = append(entry2Attrs, encodeNestedAttr(StatusAttrLanes,
		append(
			encodeNestedAttr(StatusLaneAttrEntry,
				append(
					append(
						encodeAttrU32(StatusLaneAttrLaneID, 0),
						encodeAttrU32(StatusLaneAttrPreferredPathID, 0)...,
					),
					append(
						encodeAttrU32(StatusLaneAttrFallbackPathID, 2),
						encodeAttrU32(StatusLaneAttrReadiness, 2)...,
					)...,
				),
			),
			encodeNestedAttr(StatusLaneAttrEntry,
				append(
					append(
						encodeAttrU32(StatusLaneAttrLaneID, 1),
						encodeAttrU32(StatusLaneAttrPreferredPathID, 1)...,
					),
					append(
						encodeAttrU32(StatusLaneAttrFallbackPathID, 0),
						encodeAttrU32(StatusLaneAttrReadiness, 1)...,
					)...,
				),
			)...,
		),
	)...)
	entry2 := encodeNestedAttr(AttrDeviceStatus, entry2Attrs)
	resp, err := DecodeListDevices(encodeNestedAttr(AttrDeviceList, append(entry1, entry2...)))
	if err != nil {
		t.Fatalf("DecodeListDevices failed: %v", err)
	}
	if len(resp.Devices) != 2 {
		t.Fatalf("unexpected count=%d", len(resp.Devices))
	}
	if resp.Devices[1].DeviceID != 1 || resp.Devices[1].VolumeID != 202 || resp.Devices[1].PathCount != 3 || resp.Devices[1].DownMask != 0x4 || resp.Devices[1].DegradedMask != 0x2 || resp.Devices[1].DrainingMask != 0x1 || resp.Devices[1].AppliedPathPlanRevision != 8 || resp.Devices[1].ActiveLaneCount != 2 || resp.Devices[1].NrHwQueues != 1 || resp.Devices[1].TargetNrHwQueues != 2 || resp.Devices[1].QueueTopologyGeneration != 4 || resp.Devices[1].QueueTopologyState != "planned" || resp.Devices[1].LaneRemapCount != 3 || resp.Devices[1].LastLaneRemappedLanes != 2 || resp.Devices[1].LastLaneRemapJiffies != 44 || resp.Devices[1].LastLaneRemapReason != "path_state_change" || len(resp.Devices[1].Lanes) != 2 || resp.Devices[1].Lanes[0].FallbackPathID != 2 || resp.Devices[1].Lanes[0].Readiness != 2 || resp.Devices[1].Lanes[1].PreferredPathID != 1 || resp.Devices[1].Lanes[1].FallbackPathID != 0 || resp.Devices[1].Lanes[1].Readiness != 1 || len(resp.Devices[1].Paths) != 3 || resp.Devices[1].Paths[2].State != 2 {
		t.Fatalf("unexpected device list: %+v", resp.Devices)
	}
}
