/*
Copyright 2017 The GoStor Authors All rights reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package iscsit

import (
	"bytes"
	"strings"

	"github.com/gostor/gotgt/pkg/util"
)

var (
	iSCSILoginParamTextKV = []util.KeyValue{
		{"HeaderDigest", "None"},
		{"DataDigest", "None"},
		{"ImmediateData", "Yes"},
		{"InitialR2T", "Yes"},
		{"MaxBurstLength", "262144"},
		{"FirstBurstLength", "65536"},
		{"MaxRecvDataSegmentLength", "65536"},
		{"DefaultTime2Wait", "2"},
		{"DefaultTime2Retain", "0"},
		{"MaxOutstandingR2T", "1"},
		{"IFMarker", "No"},
		{"OFMarker", "No"},
		{"DataPDUInOrder", "Yes"},
		{"DataSequenceInOrder", "Yes"}}
)

type iSCSILoginStage int

const (
	SecurityNegotiation         iSCSILoginStage = 0
	LoginOperationalNegotiation                 = 1
	FullFeaturePhase                            = 3
)

const (
	loginStatusSuccess              uint8 = 0x00
	loginStatusInitiatorError       uint8 = 0x02
	loginStatusDetailAuthFailed     uint8 = 0x01
	loginStatusDetailTargetNotFound uint8 = 0x03
)

func (s iSCSILoginStage) String() string {
	switch s {
	case SecurityNegotiation:
		return "Security Negotiation"
	case LoginOperationalNegotiation:
		return "Login Operational Negotiation"
	case FullFeaturePhase:
		return "Full Feature Phase"
	}
	return "Unknown Stage"
}

func loginKVDeclare(conn *iscsiConnection, negoKV []util.KeyValue) []util.KeyValue {
	negoKV = appendLoginKVIfMissing(negoKV, util.KeyValue{"TargetPortalGroupTag",
		numberKeyInConv(uint(conn.loginParam.tpgt))})
	negoKV = appendLoginKVIfMissing(negoKV, util.KeyValue{"MaxRecvDataSegmentLength",
		numberKeyInConv(sessionKeys["MaxRecvDataSegmentLength"].def)})
	return negoKV
}

func appendLoginKVIfMissing(negoKV []util.KeyValue, kv util.KeyValue) []util.KeyValue {
	for _, existing := range negoKV {
		if existing.Key == kv.Key {
			return negoKV
		}
	}
	return append(negoKV, kv)
}

func stringsContains(s []string, p string) bool {
	for _, q := range s {
		if strings.EqualFold(strings.TrimSpace(q), p) {
			return true
		}
	}
	return false
}

func (conn *iscsiConnection) processSecurityData() ([]util.KeyValue, error) {
	var negoKV []util.KeyValue
	securityKV := util.ParseKVText(conn.req.RawData)

	for key, val := range securityKV {
		if key == "AuthMethod" {
			// It can be a list.
			vals := strings.Split(val, ",")
			if !stringsContains(vals, "None") {
				negoKV = append(negoKV, util.KeyValue{key, "Reject"})
				conn.loginParam.statusClass = loginStatusInitiatorError
				conn.loginParam.statusDetail = loginStatusDetailAuthFailed
				conn.loginParam.tgtTrans = false
				continue
			}
			negoKV = append(negoKV, util.KeyValue{key, "None"})
			conn.loginParam.authMethod = AuthNone
		} else if key == "TargetName" {
			conn.loginParam.target = val
		} else if key == "InitiatorName" {
			conn.loginParam.initiator = val
		} else if key == "SessionType" {
			conn.processDeclarativeLoginKey(key, val)
		} else {
			negoKV = append(negoKV, util.KeyValue{key, "NotUnderstood"})
		}
	}

	if conn.loginParam.statusClass == loginStatusSuccess {
		conn.mirrorInitiatorLoginTransition()
	}
	return negoKV, nil
}

func (conn *iscsiConnection) processLoginData() ([]util.KeyValue, error) {
	var negoKV []util.KeyValue
	loginKV := util.ParseKVText(conn.req.RawData)
	if val, ok := loginKV["SessionType"]; ok {
		conn.processDeclarativeLoginKey("SessionType", val)
	}

	for key, val := range loginKV {
		if conn.processDeclarativeLoginKey(key, val) {
			continue
		}

		if conn.loginKeyIrrelevantForDiscovery(key) {
			negoKV = append(negoKV, util.KeyValue{key, "Irrelevant"})
			continue
		}
		if key == "TargetPortalGroupTag" {
			continue
		}

		kv, understood := conn.negotiateLoginKey(key, val)
		negoKV = append(negoKV, kv)
		if !understood {
			continue
		}
	}
	conn.mirrorInitiatorLoginTransition()
	return negoKV, nil
}

func (conn *iscsiConnection) processDeclarativeLoginKey(key, val string) bool {
	switch key {
	case "InitiatorName":
		conn.loginParam.initiator = val
	case "InitiatorAlias":
		conn.loginParam.initiatorAlias = val
	case "TargetName":
		conn.loginParam.target = val
	case "SessionType":
		if strings.EqualFold(val, "Normal") {
			conn.loginParam.sessionType = SESSION_NORMAL
		} else if strings.EqualFold(val, "Discovery") {
			conn.loginParam.sessionType = SESSION_DISCOVERY
		}
	case "TargetAlias", "TargetAddress":
		// Declarative target keys are not negotiated back during normal login.
	default:
		return false
	}
	return true
}

func (conn *iscsiConnection) loginKeyIrrelevantForDiscovery(key string) bool {
	if conn.loginParam.sessionType != SESSION_DISCOVERY {
		return false
	}
	switch key {
	case "MaxConnections", "InitialR2T", "ImmediateData", "MaxBurstLength",
		"FirstBurstLength", "MaxOutstandingR2T", "DataPDUInOrder",
		"DataSequenceInOrder":
		return true
	}
	return false
}

func (conn *iscsiConnection) negotiateLoginKey(key, val string) (util.KeyValue, bool) {
	if key == "MaxRecvDataSegmentLength" {
		return conn.negotiateMaxRecvDataSegmentLength(key, val), true
	}
	defSessKey, ok := sessionKeys[key]
	if !ok {
		return util.KeyValue{key, "NotUnderstood"}, false
	}
	if defSessKey.idx >= ISCSI_PARAM_FIRST_LOCAL {
		return util.KeyValue{key, "NotUnderstood"}, false
	}

	switch key {
	case "HeaderDigest", "DataDigest":
		return conn.negotiateDigestKey(key, val, defSessKey), true
	case "InitialR2T", "DataPDUInOrder", "DataSequenceInOrder":
		return conn.negotiateBooleanORKey(key, val, defSessKey), true
	case "ImmediateData", "IFMarker", "OFMarker", "RDMAExtensions":
		return conn.negotiateBooleanANDKey(key, val, defSessKey), true
	case "DefaultTime2Wait":
		return conn.negotiateNumericalMaxKey(key, val, defSessKey), true
	default:
		return conn.negotiateNumericalMinKey(key, val, defSessKey), true
	}
}

func (conn *iscsiConnection) negotiateMaxRecvDataSegmentLength(key, val string) util.KeyValue {
	initiatorValue, ok := numberKeyConv(val)
	initiatorKey := sessionKeys["MaxXmitDataSegmentLength"]
	targetKey := sessionKeys["MaxRecvDataSegmentLength"]
	if !ok || initiatorValue < initiatorKey.min || initiatorValue > initiatorKey.max {
		return util.KeyValue{key, "Reject"}
	}
	targetValue := targetKey.def
	if initiatorValue < targetValue {
		targetValue = initiatorValue
	}
	conn.loginParam.sessionParam[initiatorKey.idx].Value = targetValue
	return util.KeyValue{key, targetKey.inConv(targetValue)}
}

func (conn *iscsiConnection) negotiateDigestKey(key, val string, defSessKey *iscsiSessionKeys) util.KeyValue {
	uintVal, ok := defSessKey.conv(val)
	if !ok {
		return util.KeyValue{key, "Reject"}
	}
	selected := uint(0)
	if uintVal&DIGEST_NONE != 0 {
		selected = DIGEST_NONE
	} else if uintVal&DIGEST_CRC32C != 0 {
		selected = DIGEST_CRC32C
	} else {
		return util.KeyValue{key, "Reject"}
	}
	conn.loginParam.sessionParam[defSessKey.idx].Value = selected
	return util.KeyValue{key, defSessKey.inConv(selected)}
}

func (conn *iscsiConnection) negotiateBooleanORKey(key, val string, defSessKey *iscsiSessionKeys) util.KeyValue {
	uintVal, ok := defSessKey.conv(val)
	if !ok {
		return util.KeyValue{key, "Reject"}
	}
	if defSessKey.def != 0 || uintVal != 0 {
		uintVal = 1
	}
	conn.loginParam.sessionParam[defSessKey.idx].Value = uintVal
	return util.KeyValue{key, defSessKey.inConv(uintVal)}
}

func (conn *iscsiConnection) negotiateBooleanANDKey(key, val string, defSessKey *iscsiSessionKeys) util.KeyValue {
	uintVal, ok := defSessKey.conv(val)
	if !ok {
		return util.KeyValue{key, "Reject"}
	}
	if defSessKey.def == 0 || uintVal == 0 {
		uintVal = 0
	} else {
		uintVal = 1
	}
	conn.loginParam.sessionParam[defSessKey.idx].Value = uintVal
	return util.KeyValue{key, defSessKey.inConv(uintVal)}
}

func (conn *iscsiConnection) negotiateNumericalMinKey(key, val string, defSessKey *iscsiSessionKeys) util.KeyValue {
	uintVal, ok := defSessKey.conv(val)
	if !ok || uintVal < defSessKey.min || uintVal > defSessKey.max {
		return util.KeyValue{key, "Reject"}
	}
	if uintVal > defSessKey.def {
		uintVal = defSessKey.def
	}
	conn.loginParam.sessionParam[defSessKey.idx].Value = uintVal
	return util.KeyValue{key, defSessKey.inConv(uintVal)}
}

func (conn *iscsiConnection) negotiateNumericalMaxKey(key, val string, defSessKey *iscsiSessionKeys) util.KeyValue {
	uintVal, ok := defSessKey.conv(val)
	if !ok || uintVal < defSessKey.min || uintVal > defSessKey.max {
		return util.KeyValue{key, "Reject"}
	}
	if uintVal < defSessKey.def {
		uintVal = defSessKey.def
	}
	conn.loginParam.sessionParam[defSessKey.idx].Value = uintVal
	return util.KeyValue{key, defSessKey.inConv(uintVal)}
}

func (conn *iscsiConnection) mirrorInitiatorLoginTransition() {
	conn.loginParam.tgtTrans = conn.loginParam.iniTrans
	if conn.loginParam.iniTrans {
		conn.loginParam.tgtNSG = conn.loginParam.iniNSG
	} else {
		conn.loginParam.tgtNSG = conn.loginParam.iniCSG
	}
}

type iscsiLoginParam struct {
	paramInit bool

	iniCSG   iSCSILoginStage
	iniNSG   iSCSILoginStage
	iniTrans bool
	iniCont  bool

	tgtCSG   iSCSILoginStage
	tgtNSG   iSCSILoginStage
	tgtTrans bool
	tgtCont  bool

	sessionType  int
	sessionParam ISCSISessionParamList
	keyDeclared  bool
	respKV       []util.KeyValue
	statusClass  uint8
	statusDetail uint8

	initiator      string
	initiatorAlias string
	target         string
	targetAlias    string

	tpgt uint16
	isid uint64
	tsih uint16

	authMethod AuthMethod
}

func (m *ISCSICommand) loginRespBytes() []byte {
	// rfc7143 11.13
	buf := &bytes.Buffer{}
	// byte 0
	buf.WriteByte(byte(OpLoginResp))
	var b byte
	if m.Transit {
		b |= 0x80
	}
	if m.Cont {
		b |= 0x40
	}
	b |= byte(m.CSG&0xff) << 2
	b |= byte(m.NSG & 0xff)
	// byte 1
	buf.WriteByte(b)

	b = 0
	buf.WriteByte(b)                                          // version-max
	buf.WriteByte(b)                                          // version-active
	buf.WriteByte(b)                                          // ahsLen
	buf.Write(util.MarshalUint64(uint64(len(m.RawData)))[5:]) // data segment length, no padding
	buf.Write(util.MarshalUint64(m.ISID)[2:])
	buf.Write(util.MarshalUint64(uint64(m.TSIH))[6:])
	buf.Write(util.MarshalUint64(uint64(m.TaskTag))[4:])
	buf.WriteByte(b)
	buf.WriteByte(b)
	buf.WriteByte(b)
	buf.WriteByte(b) // "reserved"
	buf.Write(util.MarshalUint64(uint64(m.StatSN))[4:])
	buf.Write(util.MarshalUint64(uint64(m.ExpCmdSN))[4:])
	buf.Write(util.MarshalUint64(uint64(m.MaxCmdSN))[4:])
	buf.WriteByte(byte(m.StatusClass))
	buf.WriteByte(byte(m.StatusDetail))
	buf.WriteByte(b)
	buf.WriteByte(b) // "reserved"
	var bs [8]byte
	buf.Write(bs[:])
	rd := m.RawData
	for len(rd)%4 != 0 {
		rd = append(rd, 0)
	}
	buf.Write(rd)
	return buf.Bytes()
}
