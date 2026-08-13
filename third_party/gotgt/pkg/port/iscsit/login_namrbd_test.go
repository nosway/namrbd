package iscsit

import (
	"errors"
	"testing"

	"github.com/gostor/gotgt/pkg/config"
	scsipkg "github.com/gostor/gotgt/pkg/scsi"
	_ "github.com/gostor/gotgt/pkg/scsi/backingstore"
	"github.com/gostor/gotgt/pkg/util"
)

func newLoginTestConn(csg, nsg iSCSILoginStage, transit bool, kv []util.KeyValue) *iscsiConnection {
	conn := &iscsiConnection{
		loginParam: &iscsiLoginParam{},
		req: &ISCSICommand{
			CSG:       csg,
			NSG:       nsg,
			Transit:   transit,
			RawData:   util.MarshalKVText(kv),
			ISID:      0x112233445566,
			TaskTag:   0xffffffff,
			CmdSN:     7,
			ExpStatSN: 3,
		},
	}
	conn.init()
	conn.loginParam.iniCSG = csg
	conn.loginParam.iniNSG = nsg
	conn.loginParam.iniTrans = transit
	return conn
}

func kvMap(kv []util.KeyValue) map[string]string {
	out := map[string]string{}
	for _, item := range kv {
		out[item.Key] = item.Value
	}
	return out
}

func TestProcessSecurityDataSelectsAuthNone(t *testing.T) {
	conn := newLoginTestConn(SecurityNegotiation, LoginOperationalNegotiation, true, []util.KeyValue{
		{Key: "InitiatorName", Value: "iqn.1991-05.com.microsoft:host"},
		{Key: "AuthMethod", Value: "CHAP,None"},
	})

	kv, err := conn.processSecurityData()
	if err != nil {
		t.Fatalf("processSecurityData returned error: %v", err)
	}
	got := kvMap(kv)
	if got["AuthMethod"] != "None" {
		t.Fatalf("AuthMethod=%q, want None", got["AuthMethod"])
	}
	if !conn.loginParam.tgtTrans || conn.loginParam.tgtNSG != LoginOperationalNegotiation {
		t.Fatalf("target transition trans=%v nsg=%v, want true operational", conn.loginParam.tgtTrans, conn.loginParam.tgtNSG)
	}
}

func TestProcessSecurityDataUnknownKeyIsNotUnderstood(t *testing.T) {
	conn := newLoginTestConn(SecurityNegotiation, LoginOperationalNegotiation, true, []util.KeyValue{
		{Key: "AuthMethod", Value: "None"},
		{Key: "X-Microsoft-LoginProbe", Value: "1"},
	})

	kv, err := conn.processSecurityData()
	if err != nil {
		t.Fatalf("processSecurityData returned error: %v", err)
	}
	got := kvMap(kv)
	if got["X-Microsoft-LoginProbe"] != "NotUnderstood" {
		t.Fatalf("unknown key response=%q, want NotUnderstood", got["X-Microsoft-LoginProbe"])
	}
}

func TestProcessLoginDataNegotiatesIstyleKeys(t *testing.T) {
	conn := newLoginTestConn(LoginOperationalNegotiation, FullFeaturePhase, true, []util.KeyValue{
		{Key: "InitiatorName", Value: "iqn.1991-05.com.microsoft:host"},
		{Key: "TargetName", Value: "iqn.2026-06.io.namrbd:iscsi.windows"},
		{Key: "SessionType", Value: "Normal"},
		{Key: "HeaderDigest", Value: "CRC32C,None"},
		{Key: "InitialR2T", Value: "No"},
		{Key: "ImmediateData", Value: "No"},
		{Key: "DataPDUInOrder", Value: "No"},
		{Key: "DataSequenceInOrder", Value: "No"},
		{Key: "X-Microsoft-LoginProbe", Value: "1"},
	})

	kv, err := conn.processLoginData()
	if err != nil {
		t.Fatalf("processLoginData returned error: %v", err)
	}
	got := kvMap(kv)
	want := map[string]string{
		"HeaderDigest":           "None",
		"InitialR2T":             "Yes",
		"ImmediateData":          "No",
		"DataPDUInOrder":         "Yes",
		"DataSequenceInOrder":    "Yes",
		"X-Microsoft-LoginProbe": "NotUnderstood",
	}
	for key, value := range want {
		if got[key] != value {
			t.Fatalf("%s=%q, want %q (all kv=%v)", key, got[key], value, got)
		}
	}
	if !conn.loginParam.tgtTrans || conn.loginParam.tgtNSG != FullFeaturePhase {
		t.Fatalf("target transition trans=%v nsg=%v, want true full feature", conn.loginParam.tgtTrans, conn.loginParam.tgtNSG)
	}
}

func TestProcessLoginDataDoesNotForceTransit(t *testing.T) {
	conn := newLoginTestConn(LoginOperationalNegotiation, LoginOperationalNegotiation, false, []util.KeyValue{
		{Key: "SessionType", Value: "Normal"},
		{Key: "MaxBurstLength", Value: "131072"},
	})

	kv, err := conn.processLoginData()
	if err != nil {
		t.Fatalf("processLoginData returned error: %v", err)
	}
	got := kvMap(kv)
	if got["MaxBurstLength"] != "131072" {
		t.Fatalf("MaxBurstLength=%q, want 131072", got["MaxBurstLength"])
	}
	if conn.loginParam.tgtTrans || conn.loginParam.tgtNSG != LoginOperationalNegotiation {
		t.Fatalf("target transition trans=%v nsg=%v, want false operational", conn.loginParam.tgtTrans, conn.loginParam.tgtNSG)
	}
}

func TestProcessLoginDataDiscoveryIrrelevantKeys(t *testing.T) {
	conn := newLoginTestConn(LoginOperationalNegotiation, FullFeaturePhase, true, []util.KeyValue{
		{Key: "SessionType", Value: "Discovery"},
		{Key: "MaxConnections", Value: "4"},
		{Key: "InitialR2T", Value: "No"},
	})

	kv, err := conn.processLoginData()
	if err != nil {
		t.Fatalf("processLoginData returned error: %v", err)
	}
	got := kvMap(kv)
	if got["MaxConnections"] != "Irrelevant" || got["InitialR2T"] != "Irrelevant" {
		t.Fatalf("discovery kv=%v, want Irrelevant responses", got)
	}
}

func TestBuildLoginRespIncludesSessionTSIHAndDeclaredKeys(t *testing.T) {
	conn := newLoginTestConn(LoginOperationalNegotiation, FullFeaturePhase, true, []util.KeyValue{
		{Key: "SessionType", Value: "Normal"},
	})
	conn.loginParam.isid = conn.req.ISID
	conn.loginParam.tsih = conn.req.TSIH
	conn.loginParam.tpgt = 1
	conn.loginParam.tgtTrans = true
	conn.loginParam.tgtNSG = FullFeaturePhase
	conn.loginParam.respKV = []util.KeyValue{{Key: "MaxBurstLength", Value: "262144"}}
	conn.session = &ISCSISession{
		TSIH:            0x1234,
		ExpCmdSN:        7,
		MaxQueueCommand: 128,
	}

	if err := conn.buildRespPackage(OpLoginResp, nil); err != nil {
		t.Fatalf("buildRespPackage returned error: %v", err)
	}
	if conn.resp.TSIH != 0x1234 {
		t.Fatalf("TSIH=0x%x, want 0x1234", conn.resp.TSIH)
	}
	got := util.ParseKVText(conn.resp.RawData)
	if got["TargetPortalGroupTag"] != "1" {
		t.Fatalf("TargetPortalGroupTag=%q, want 1 (all kv=%v)", got["TargetPortalGroupTag"], got)
	}
	if got["MaxRecvDataSegmentLength"] == "" {
		t.Fatalf("MaxRecvDataSegmentLength missing from login declaration: %v", got)
	}
}

func TestNewTargetCopiesALUAPortGroupsToSCSIServiceTarget(t *testing.T) {
	scsiSvc := scsipkg.NewSCSITargetService()
	driver, err := NewISCSITargetDriver(scsiSvc)
	if err != nil {
		t.Fatalf("NewISCSITargetDriver: %v", err)
	}
	iscsiDriver := driver.(*ISCSITargetDriver)
	cfg := &config.Config{
		ISCSIPortals: []config.ISCSIPortalInfo{{ID: 0, Portal: "127.0.0.1:3260"}},
		ISCSITargets: map[string]config.ISCSITarget{
			"iqn.test:alua": {
				TPGTs: map[string][]uint64{"2": {0}},
				TPGTALUA: map[string]config.ALUATargetPortGroup{
					"2": {
						AccessState:       0x02,
						ImplicitSupported: true,
					},
				},
			},
		},
	}

	if err := iscsiDriver.NewTarget("iqn.test:alua", cfg); err != nil {
		t.Fatalf("NewTarget: %v", err)
	}
	if len(scsiSvc.Targets) != 1 {
		t.Fatalf("SCSI service target count=%d want 1", len(scsiSvc.Targets))
	}
	serviceTarget := scsiSvc.Targets[0]
	if got := scsipkg.FindTargetGroup(serviceTarget, 2); got != 2 {
		t.Fatalf("service target port group=%d want 2", got)
	}
	if port := scsipkg.FindTargetPort(serviceTarget, 2); port == nil || port.RelativeTargetPortID != 2 {
		t.Fatalf("service target port=%+v want relative target port 2", port)
	}
	if got := scsipkg.FindTargetGroup(&iscsiDriver.iSCSITargets["iqn.test:alua"].SCSITarget, 2); got != 2 {
		t.Fatalf("iSCSI target port group=%d want 2", got)
	}
}

func TestClosedNetworkConnectionErrorIsNormalTeardown(t *testing.T) {
	if !isClosedNetworkConnectionError(errors.New("read tcp 10.105.23.85:3260->10.79.36.209:63486: use of closed network connection")) {
		t.Fatalf("closed network connection was not classified as normal teardown")
	}
	if isClosedNetworkConnectionError(errors.New("connection reset by peer")) {
		t.Fatalf("unexpected connection reset was classified as normal teardown")
	}
}
