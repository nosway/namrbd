package iscsi

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

const (
	DefaultControlStateDir = ".cache/phase-q-iscsictl-state"
	ControlStateFileName   = "state.json"
	ControlStateVersion    = 1

	WriterPolicySingleActiveWriterSession = "single_active_writer_session"
	HAFailoverModeManualPromoteDemote     = "manual_promote_demote_first"

	ALUAModeImplicit               = "implicit"
	ALUAAccessStateActiveOptimized = "active_optimized"
	ALUAAccessStateStandby         = "standby"
	ALUAAccessStateUnavailable     = "unavailable"
)

type ControlState struct {
	Version   int                        `json:"version"`
	Portals   map[string]Portal          `json:"portals"`
	Targets   map[string]Target          `json:"targets"`
	LUNs      map[string]LUN             `json:"luns"`
	ACLs      map[string]InitiatorACL    `json:"initiator_acls"`
	Sessions  map[string]Session         `json:"sessions"`
	Failovers map[string]FailoverRuntime `json:"failovers"`
}

type Portal struct {
	PortalID  string `json:"portal_id"`
	Address   string `json:"address"`
	GatewayID string `json:"gateway_id,omitempty"`
	Enabled   bool   `json:"enabled"`
}

type Target struct {
	TargetIQN string   `json:"target_iqn"`
	PortalID  string   `json:"portal_id"`
	PortalIDs []string `json:"portal_ids,omitempty"`
	ExportID  string   `json:"export_id"`
	Enabled   bool     `json:"enabled"`
}

type LUN struct {
	TargetIQN             string `json:"target_iqn"`
	LUNID                 uint64 `json:"lun_id"`
	LUNWWN                string `json:"lun_wwn"`
	ExportID              string `json:"export_id"`
	VolumeID              string `json:"volume_id"`
	ExportMode            string `json:"export_mode"`
	LogicalBlockSizeBytes uint64 `json:"logical_block_size_bytes"`
	Enabled               bool   `json:"enabled"`
}

type InitiatorACL struct {
	InitiatorIQN  string   `json:"initiator_iqn"`
	TargetIQN     string   `json:"target_iqn"`
	AllowedLUNs   []uint64 `json:"allowed_lun_ids"`
	AuthMode      string   `json:"auth_mode"`
	CHAPSecretSet bool     `json:"chap_secret_set"`
	CHAPSecretRef string   `json:"chap_secret_ref,omitempty"`
	Enabled       bool     `json:"enabled"`
}

type Session struct {
	SessionID        string `json:"session_id"`
	TargetIQN        string `json:"target_iqn"`
	InitiatorIQN     string `json:"initiator_iqn"`
	LUNID            uint64 `json:"lun_id"`
	LUNWWN           string `json:"lun_wwn,omitempty"`
	ISCSIGatewayID   string `json:"iscsi_gateway_id,omitempty"`
	State            string `json:"state"`
	Connected        bool   `json:"connected"`
	ISCSIERL         int    `json:"iscsi_erl"`
	ConnectionCount  int    `json:"connection_count"`
	HeaderDigest     string `json:"header_digest"`
	DataDigest       string `json:"data_digest"`
	WriterPolicy     string `json:"writer_policy"`
	HAFailoverMode   string `json:"ha_failover_mode"`
	ActiveGatewayID  string `json:"active_iscsi_gateway_id,omitempty"`
	ExportEpoch      uint64 `json:"export_epoch,omitempty"`
	ReadWriteAllowed bool   `json:"read_write_allowed"`
	SCSIStatus       string `json:"scsi_status"`
	SenseKey         string `json:"sense_key,omitempty"`
	LastErrorClass   string `json:"last_error_class,omitempty"`
	LastError        string `json:"last_error,omitempty"`
	BytesRead        uint64 `json:"bytes_read"`
	BytesWritten     uint64 `json:"bytes_written"`
	FlushCount       uint64 `json:"flush_count"`
	UnmapBytes       uint64 `json:"unmap_bytes"`
}

type FailoverRuntime struct {
	ExportID                   string   `json:"export_id"`
	ActiveISCSIGatewayID       string   `json:"active_iscsi_gateway_id,omitempty"`
	StandbyISCSIGatewayIDs     []string `json:"standby_iscsi_gateway_ids,omitempty"`
	PreviousActiveGatewayID    string   `json:"previous_active_iscsi_gateway_id,omitempty"`
	ExportLeaseID              string   `json:"export_lease_id,omitempty"`
	ExportEpoch                uint64   `json:"export_epoch"`
	State                      string   `json:"state"`
	WriterPolicy               string   `json:"writer_policy"`
	HAFailoverMode             string   `json:"ha_failover_mode"`
	ALUAMode                   string   `json:"alua_mode,omitempty"`
	ALUAImplicitSupported      bool     `json:"alua_implicit_supported"`
	ALUAExplicitSupported      bool     `json:"alua_explicit_supported"`
	ActiveALUAAccessState      string   `json:"active_alua_access_state,omitempty"`
	StandbyALUAAccessState     string   `json:"standby_alua_access_state,omitempty"`
	FailoverTrigger            string   `json:"failover_trigger,omitempty"`
	FailoverCompleted          bool     `json:"failover_completed"`
	StaleGatewayRevokedID      string   `json:"stale_gateway_revoked_id,omitempty"`
	StaleGatewayRejected       bool     `json:"stale_gateway_rejected"`
	StandbyWriteRejected       bool     `json:"standby_write_rejected"`
	LastWriteGatewayID         string   `json:"last_write_gateway_id,omitempty"`
	LastWriteObservedEpoch     uint64   `json:"last_write_observed_epoch,omitempty"`
	LastWriteAdmitted          bool     `json:"last_write_admitted"`
	LastWriteRejectionReason   string   `json:"last_write_rejection_reason,omitempty"`
	LastWriteSCSIStatus        string   `json:"last_write_scsi_status,omitempty"`
	LastWriteSenseKey          string   `json:"last_write_sense_key,omitempty"`
	LastRejectedISCSIGatewayID string   `json:"last_rejected_iscsi_gateway_id,omitempty"`
}

type FailoverWriteAdmission struct {
	GatewayID            string `json:"gateway_id"`
	ActiveISCSIGatewayID string `json:"active_iscsi_gateway_id,omitempty"`
	ObservedExportEpoch  uint64 `json:"observed_export_epoch"`
	ExportEpoch          uint64 `json:"export_epoch"`
	WriteAdmitted        bool   `json:"write_admitted"`
	StaleGatewayRejected bool   `json:"stale_gateway_rejected"`
	StandbyWriteRejected bool   `json:"standby_write_rejected"`
	RejectionReason      string `json:"rejection_reason,omitempty"`
	SCSIStatus           string `json:"scsi_status"`
	SenseKey             string `json:"sense_key,omitempty"`
	WriterPolicy         string `json:"writer_policy"`
	HAFailoverMode       string `json:"ha_failover_mode"`
}

type SCSIIdentity struct {
	Vendor       string `json:"vendor"`
	Product      string `json:"product"`
	Serial       string `json:"serial"`
	LUNWWN       string `json:"lun_wwn"`
	DeviceID     uint32 `json:"device_id"`
	IdentityHash string `json:"identity_hash"`
}

type MPIOPath struct {
	PathID                string `json:"path_id"`
	PortalID              string `json:"portal_id"`
	Portal                string `json:"portal"`
	GatewayID             string `json:"gateway_id"`
	Role                  string `json:"role"`
	TargetIQN             string `json:"target_iqn"`
	LUNWWN                string `json:"lun_wwn"`
	SCSIIdentityHash      string `json:"scsi_identity_hash"`
	IOAllowed             bool   `json:"io_allowed"`
	WriteAllowed          bool   `json:"write_allowed"`
	ALUATargetPortGroupID uint16 `json:"alua_target_port_group_id,omitempty"`
	ALUAAccessState       string `json:"alua_access_state,omitempty"`
	ALUAPreferred         bool   `json:"alua_preferred"`
}

type MPIOStatus struct {
	Result                      string       `json:"result"`
	SupportClaimed              bool         `json:"support_claimed"`
	ISCSIMPIOClaimed            bool         `json:"iscsi_mpio_claimed"`
	ClaimBlockedReason          string       `json:"claim_blocked_reason"`
	MPIOALUASupported           bool         `json:"mpio_alua_supported"`
	ALUAMode                    string       `json:"alua_mode,omitempty"`
	ALUAImplicitSupported       bool         `json:"alua_implicit_supported"`
	ALUAExplicitSupported       bool         `json:"alua_explicit_supported"`
	ALUAActiveAccessState       string       `json:"alua_active_access_state,omitempty"`
	ALUAStandbyAccessState      string       `json:"alua_standby_access_state,omitempty"`
	InitiatorMultipath          string       `json:"initiator_multipath"`
	TargetIQN                   string       `json:"target_iqn"`
	LUNID                       uint64       `json:"lun_id"`
	LUNWWN                      string       `json:"lun_wwn"`
	SCSIIdentity                SCSIIdentity `json:"scsi_identity"`
	SCSIIdentityHash            string       `json:"scsi_identity_hash"`
	PathCount                   int          `json:"path_count"`
	Paths                       []MPIOPath   `json:"paths"`
	Portals                     []MPIOPath   `json:"portals"`
	ActivePath                  string       `json:"active_path,omitempty"`
	StandbyPath                 string       `json:"standby_path,omitempty"`
	ActiveGatewayID             string       `json:"active_gateway_id,omitempty"`
	StandbyGatewayID            string       `json:"standby_gateway_id,omitempty"`
	ActiveTargetPortGroupID     uint16       `json:"active_target_port_group_id,omitempty"`
	StandbyTargetPortGroupID    uint16       `json:"standby_target_port_group_id,omitempty"`
	ActiveISCSIGatewayID        string       `json:"active_iscsi_gateway_id,omitempty"`
	StandbyISCSIGatewayIDs      []string     `json:"standby_iscsi_gateway_ids,omitempty"`
	ExportEpoch                 uint64       `json:"export_epoch"`
	ExportEpochAfter            uint64       `json:"export_epoch_after"`
	ActivePathIOAllowed         bool         `json:"active_path_io_allowed"`
	ActivePathWriteAllowed      bool         `json:"active_path_write_allowed"`
	StandbyPathIOAllowed        bool         `json:"standby_path_io_allowed"`
	StandbyPathWriteAllowed     bool         `json:"standby_path_write_allowed"`
	StandbyPathWriteSucceeded   bool         `json:"standby_path_write_succeeded"`
	StandbyWriteRejected        bool         `json:"standby_write_rejected"`
	StaleGatewayRejected        bool         `json:"stale_gateway_rejected"`
	StandbyPromoted             bool         `json:"standby_promoted"`
	MultipathFailoverCompleted  bool         `json:"multipath_failover_completed"`
	PostFailoverReadbackMatched bool         `json:"post_failover_readback_matched"`
	PostFailoverReadbackClaim   string       `json:"post_failover_readback_claim"`
	StableTargetIdentity        bool         `json:"stable_target_identity"`
	StableLUNIdentity           bool         `json:"stable_lun_identity"`
	StableSCSIIdentity          bool         `json:"stable_scsi_identity"`
	UnsupportedClaims           []string     `json:"unsupported_claims"`
	FutureWork                  []string     `json:"future_work"`
	OKCount                     int          `json:"ok_count"`
	ErrorCount                  int          `json:"error_count"`
	FirstError                  string       `json:"first_error,omitempty"`
	LastError                   string       `json:"last_error,omitempty"`
}

type ObservabilityCounters struct {
	SessionCount          int    `json:"session_count"`
	ConnectedSessions     int    `json:"connected_sessions"`
	ActiveSessions        int    `json:"active_sessions"`
	ProtocolErrors        uint64 `json:"protocol_errors"`
	BackendErrors         uint64 `json:"backend_errors"`
	AuthErrors            uint64 `json:"auth_errors"`
	FencingErrors         uint64 `json:"fencing_errors"`
	StaleRejects          uint64 `json:"stale_rejects"`
	StandbyRejects        uint64 `json:"standby_rejects"`
	FlushCount            uint64 `json:"flush_count"`
	UnmapBytes            uint64 `json:"unmap_bytes"`
	BytesRead             uint64 `json:"bytes_read"`
	BytesWritten          uint64 `json:"bytes_written"`
	LastRejectedGatewayID string `json:"last_rejected_iscsi_gateway_id,omitempty"`
	LastErrorClass        string `json:"last_error_class,omitempty"`
	LastError             string `json:"last_error,omitempty"`
}

func NewControlState() *ControlState {
	return (&ControlState{Version: ControlStateVersion}).Normalize()
}

func LoadControlState(stateDir string) (*ControlState, string, error) {
	path := ControlStatePath(stateDir)
	raw, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return NewControlState(), path, nil
		}
		return nil, path, err
	}
	var state ControlState
	if err := json.Unmarshal(raw, &state); err != nil {
		return nil, path, err
	}
	return state.Normalize(), path, nil
}

func SaveControlState(stateDir string, state *ControlState) (string, error) {
	path := ControlStatePath(stateDir)
	if state == nil {
		return path, fmt.Errorf("control state is nil")
	}
	state.Normalize()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return path, err
	}
	tmp := fmt.Sprintf("%s.%d.tmp", path, os.Getpid())
	raw, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		return path, err
	}
	raw = append(raw, '\n')
	if err := os.WriteFile(tmp, raw, 0o644); err != nil {
		return path, err
	}
	defer os.Remove(tmp)
	if err := os.Rename(tmp, path); err != nil {
		return path, err
	}
	return path, nil
}

func ControlStatePath(stateDir string) string {
	stateDir = strings.TrimSpace(stateDir)
	if stateDir == "" {
		stateDir = DefaultControlStateDir
	}
	return filepath.Join(stateDir, ControlStateFileName)
}

func (s *ControlState) Normalize() *ControlState {
	if s.Version == 0 {
		s.Version = ControlStateVersion
	}
	if s.Portals == nil {
		s.Portals = map[string]Portal{}
	}
	if s.Targets == nil {
		s.Targets = map[string]Target{}
	}
	if s.LUNs == nil {
		s.LUNs = map[string]LUN{}
	}
	if s.ACLs == nil {
		s.ACLs = map[string]InitiatorACL{}
	}
	if s.Sessions == nil {
		s.Sessions = map[string]Session{}
	}
	for key, session := range s.Sessions {
		s.Sessions[key] = NormalizeSession(session)
	}
	for key, portal := range s.Portals {
		s.Portals[key] = NormalizePortal(portal)
	}
	for key, target := range s.Targets {
		s.Targets[key] = NormalizeTarget(target)
	}
	if s.Failovers == nil {
		s.Failovers = map[string]FailoverRuntime{}
	}
	for key, runtime := range s.Failovers {
		s.Failovers[key] = NormalizeFailoverRuntime(runtime)
	}
	return s
}

func LUNKey(targetIQN string, lunID uint64) string {
	return targetIQN + "#" + fmt.Sprint(lunID)
}

func NormalizePortal(portal Portal) Portal {
	portal.PortalID = strings.TrimSpace(portal.PortalID)
	portal.Address = strings.TrimSpace(portal.Address)
	portal.GatewayID = strings.TrimSpace(portal.GatewayID)
	if portal.GatewayID == "" {
		portal.GatewayID = portal.PortalID
	}
	return portal
}

func NormalizeTarget(target Target) Target {
	target.TargetIQN = strings.TrimSpace(target.TargetIQN)
	target.PortalID = strings.TrimSpace(target.PortalID)
	target.PortalIDs = uniqueNonEmptyStrings(append([]string{target.PortalID}, target.PortalIDs...))
	if len(target.PortalIDs) > 0 {
		target.PortalID = target.PortalIDs[0]
	}
	target.ExportID = strings.TrimSpace(target.ExportID)
	return target
}

func TargetPortalIDs(target Target) []string {
	target = NormalizeTarget(target)
	return append([]string{}, target.PortalIDs...)
}

func ACLKey(initiatorIQN, targetIQN string) string {
	return initiatorIQN + "@" + targetIQN
}

func CHAPSecretRef(secret string) string {
	sum := sha256.Sum256([]byte(secret))
	return "fixture-chap-sha256:" + hex.EncodeToString(sum[:8])
}

func StableSCSIDeviceID(lunWWN string) uint32 {
	lunWWN = strings.TrimSpace(lunWWN)
	if lunWWN == "" {
		lunWWN = "namrbd-default-lun"
	}
	sum := sha256.Sum256([]byte(lunWWN))
	id := binary.BigEndian.Uint32(sum[:4])
	id &= 0x7fffffff
	if id == 0 {
		id = 1
	}
	return id
}

func SCSIIdentityForLUN(lun LUN) SCSIIdentity {
	lun.LUNWWN = strings.TrimSpace(lun.LUNWWN)
	if lun.LUNWWN == "" {
		lun.LUNWWN = LUNWWN(lun.ExportID)
	}
	serial := sanitizeSCSISerial(lun.LUNWWN)
	deviceID := StableSCSIDeviceID(lun.LUNWWN)
	base := fmt.Sprintf("NAMRBD|NAMRBD iSCSI|%s|%s|%d", serial, lun.LUNWWN, deviceID)
	sum := sha256.Sum256([]byte(base))
	return SCSIIdentity{
		Vendor:       "NAMRBD",
		Product:      "NAMRBD iSCSI",
		Serial:       serial,
		LUNWWN:       lun.LUNWWN,
		DeviceID:     deviceID,
		IdentityHash: "sha256:" + hex.EncodeToString(sum[:16]),
	}
}

func BuildMPIOStatus(state *ControlState, targetIQN string, lunID uint64) (MPIOStatus, error) {
	if state == nil {
		return MPIOStatus{}, fmt.Errorf("control state is nil")
	}
	state.Normalize()
	target, ok := state.Targets[targetIQN]
	if !ok {
		return MPIOStatus{}, fmt.Errorf("target %q does not exist", targetIQN)
	}
	target = NormalizeTarget(target)
	lun, ok := state.LUNs[LUNKey(targetIQN, lunID)]
	if !ok {
		return MPIOStatus{}, fmt.Errorf("lun %s does not exist", LUNKey(targetIQN, lunID))
	}
	identity := SCSIIdentityForLUN(lun)
	runtime := NormalizeFailoverRuntime(state.Failovers[lun.ExportID])
	if runtime.ExportID == "" {
		runtime.ExportID = lun.ExportID
	}
	status := MPIOStatus{
		Result:                      "ok",
		SupportClaimed:              false,
		ISCSIMPIOClaimed:            false,
		ClaimBlockedReason:          "local_contract_fixture_no_live_linux_dm_multipath",
		MPIOALUASupported:           true,
		ALUAMode:                    runtime.ALUAMode,
		ALUAImplicitSupported:       runtime.ALUAImplicitSupported,
		ALUAExplicitSupported:       runtime.ALUAExplicitSupported,
		ALUAActiveAccessState:       runtime.ActiveALUAAccessState,
		ALUAStandbyAccessState:      runtime.StandbyALUAAccessState,
		InitiatorMultipath:          "linux_dm_multipath",
		TargetIQN:                   target.TargetIQN,
		LUNID:                       lun.LUNID,
		LUNWWN:                      identity.LUNWWN,
		SCSIIdentity:                identity,
		SCSIIdentityHash:            identity.IdentityHash,
		ExportEpoch:                 runtime.ExportEpoch,
		ExportEpochAfter:            runtime.ExportEpoch,
		ActiveISCSIGatewayID:        runtime.ActiveISCSIGatewayID,
		StandbyISCSIGatewayIDs:      append([]string{}, runtime.StandbyISCSIGatewayIDs...),
		StandbyWriteRejected:        runtime.StandbyWriteRejected,
		StaleGatewayRejected:        runtime.StaleGatewayRejected,
		StandbyPromoted:             runtime.PreviousActiveGatewayID != "" && runtime.ActiveISCSIGatewayID != "" && runtime.PreviousActiveGatewayID != runtime.ActiveISCSIGatewayID,
		PostFailoverReadbackClaim:   "not_observed_by_local_contract",
		StableTargetIdentity:        true,
		StableLUNIdentity:           true,
		StableSCSIIdentity:          true,
		UnsupportedClaims:           []string{"active_active_load_balancing", "explicit_alua_state_transition", "windows_mpio_product_support"},
		FutureWork:                  []string{"active/active load balancing", "explicit ALUA state transition", "Windows MPIO product support"},
		PostFailoverReadbackMatched: false,
	}
	for idx, portalID := range TargetPortalIDs(target) {
		portal, ok := state.Portals[portalID]
		if !ok {
			status.Result = "error"
			status.ErrorCount++
			err := fmt.Sprintf("target portal %q does not exist", portalID)
			if status.FirstError == "" {
				status.FirstError = err
			}
			status.LastError = err
			continue
		}
		portal = NormalizePortal(portal)
		role := "standby"
		ioAllowed := false
		writeAllowed := false
		if portal.GatewayID == runtime.ActiveISCSIGatewayID {
			role = "active"
			ioAllowed = true
			writeAllowed = true
		}
		accessState := runtime.StandbyALUAAccessState
		preferred := false
		if role == "active" {
			accessState = runtime.ActiveALUAAccessState
			preferred = true
		}
		if !portal.Enabled {
			accessState = ALUAAccessStateUnavailable
			ioAllowed = false
			writeAllowed = false
		}
		path := MPIOPath{
			PathID:                portal.PortalID,
			PortalID:              portal.PortalID,
			Portal:                portal.Address,
			GatewayID:             portal.GatewayID,
			Role:                  role,
			TargetIQN:             target.TargetIQN,
			LUNWWN:                identity.LUNWWN,
			SCSIIdentityHash:      identity.IdentityHash,
			IOAllowed:             ioAllowed,
			WriteAllowed:          writeAllowed,
			ALUATargetPortGroupID: ALUATargetPortGroupIDForIndex(idx),
			ALUAAccessState:       accessState,
			ALUAPreferred:         preferred,
		}
		status.Paths = append(status.Paths, path)
		status.Portals = append(status.Portals, path)
		if role == "active" && status.ActivePath == "" {
			status.ActivePath = path.PathID
			status.ActiveGatewayID = path.GatewayID
			status.ActiveTargetPortGroupID = path.ALUATargetPortGroupID
			status.ActivePathIOAllowed = true
			status.ActivePathWriteAllowed = true
		}
		if role == "standby" && status.StandbyPath == "" {
			status.StandbyPath = path.PathID
			status.StandbyGatewayID = path.GatewayID
			status.StandbyTargetPortGroupID = path.ALUATargetPortGroupID
		}
	}
	status.PathCount = len(status.Paths)
	status.StandbyPathWriteAllowed = false
	status.StandbyPathIOAllowed = false
	status.StandbyPathWriteSucceeded = false
	status.MultipathFailoverCompleted = status.StandbyPromoted
	status.OKCount = 1
	if status.PathCount < 2 {
		status.Result = "error"
		status.OKCount = 0
		status.ErrorCount++
		status.FirstError = nonEmpty(status.FirstError, "mpio requires at least two portal paths")
		status.LastError = "mpio requires at least two portal paths"
	}
	if status.ActivePath == "" {
		status.Result = "error"
		status.OKCount = 0
		status.ErrorCount++
		status.FirstError = nonEmpty(status.FirstError, "active path not found")
		status.LastError = "active path not found"
	}
	if status.StandbyPath == "" {
		status.Result = "error"
		status.OKCount = 0
		status.ErrorCount++
		status.FirstError = nonEmpty(status.FirstError, "standby path not found")
		status.LastError = "standby path not found"
	}
	if status.ErrorCount > 0 {
		status.Result = "error"
		status.OKCount = 0
	}
	return status, nil
}

func ALUATargetPortGroupIDForIndex(index int) uint16 {
	if index < 0 {
		return 1
	}
	groupID := index + 1
	if groupID > 0xffff {
		return 0xffff
	}
	return uint16(groupID)
}

func ALUAAccessStateCode(state string) uint8 {
	switch strings.TrimSpace(state) {
	case ALUAAccessStateStandby:
		return 0x02
	case ALUAAccessStateUnavailable:
		return 0x03
	default:
		return 0x00
	}
}

func NormalizeFailoverRuntime(runtime FailoverRuntime) FailoverRuntime {
	if runtime.State == "" {
		runtime.State = "none"
	}
	if runtime.WriterPolicy == "" {
		runtime.WriterPolicy = WriterPolicySingleActiveWriterSession
	}
	if runtime.HAFailoverMode == "" {
		runtime.HAFailoverMode = HAFailoverModeManualPromoteDemote
	}
	if runtime.ALUAMode == "" {
		runtime.ALUAMode = ALUAModeImplicit
	}
	if !runtime.ALUAImplicitSupported && runtime.ALUAMode == ALUAModeImplicit {
		runtime.ALUAImplicitSupported = true
	}
	if runtime.ActiveALUAAccessState == "" {
		runtime.ActiveALUAAccessState = ALUAAccessStateActiveOptimized
	}
	if runtime.StandbyALUAAccessState == "" {
		runtime.StandbyALUAAccessState = ALUAAccessStateStandby
	}
	runtime.StandbyISCSIGatewayIDs = uniqueNonEmptyStrings(runtime.StandbyISCSIGatewayIDs)
	return runtime
}

func NormalizeSession(session Session) Session {
	if session.State == "" {
		if session.Connected {
			session.State = "connected"
		} else {
			session.State = "disconnected"
		}
	}
	if session.ConnectionCount == 0 && session.Connected {
		session.ConnectionCount = 1
	}
	if session.HeaderDigest == "" {
		session.HeaderDigest = "none"
	}
	if session.DataDigest == "" {
		session.DataDigest = "none"
	}
	if session.WriterPolicy == "" {
		session.WriterPolicy = WriterPolicySingleActiveWriterSession
	}
	if session.HAFailoverMode == "" {
		session.HAFailoverMode = HAFailoverModeManualPromoteDemote
	}
	if session.SCSIStatus == "" {
		session.SCSIStatus = "good"
	}
	if session.ActiveGatewayID == "" {
		session.ActiveGatewayID = session.ISCSIGatewayID
	}
	if session.ReadWriteAllowed && !session.Connected {
		session.ReadWriteAllowed = false
	}
	return session
}

func BuildObservabilityCounters(state *ControlState) ObservabilityCounters {
	if state == nil {
		return ObservabilityCounters{}
	}
	state.Normalize()
	out := ObservabilityCounters{}
	for _, session := range state.Sessions {
		session = NormalizeSession(session)
		out.SessionCount++
		if session.Connected {
			out.ConnectedSessions++
		}
		if session.State == "connected" || session.State == "active" {
			out.ActiveSessions++
		}
		switch session.LastErrorClass {
		case "protocol":
			out.ProtocolErrors++
		case "backend":
			out.BackendErrors++
		case "auth":
			out.AuthErrors++
		case "fencing":
			out.FencingErrors++
		}
		out.FlushCount += session.FlushCount
		out.UnmapBytes += session.UnmapBytes
		out.BytesRead += session.BytesRead
		out.BytesWritten += session.BytesWritten
		if session.LastError != "" {
			out.LastErrorClass = session.LastErrorClass
			out.LastError = session.LastError
		}
	}
	for _, runtime := range state.Failovers {
		runtime = NormalizeFailoverRuntime(runtime)
		if runtime.StaleGatewayRejected {
			out.StaleRejects++
			out.FencingErrors++
		}
		if runtime.StandbyWriteRejected {
			out.StandbyRejects++
			out.FencingErrors++
		}
		if runtime.LastRejectedISCSIGatewayID != "" {
			out.LastRejectedGatewayID = runtime.LastRejectedISCSIGatewayID
		}
		if runtime.LastWriteRejectionReason != "" {
			out.LastErrorClass = "fencing"
			out.LastError = runtime.LastWriteRejectionReason
		}
	}
	return out
}

func AddStandbyGatewayID(runtime FailoverRuntime, gatewayID string) FailoverRuntime {
	gatewayID = strings.TrimSpace(gatewayID)
	runtime = NormalizeFailoverRuntime(runtime)
	if gatewayID == "" || gatewayID == runtime.ActiveISCSIGatewayID {
		return runtime
	}
	runtime.StandbyISCSIGatewayIDs = append(runtime.StandbyISCSIGatewayIDs, gatewayID)
	runtime.StandbyISCSIGatewayIDs = uniqueNonEmptyStrings(runtime.StandbyISCSIGatewayIDs)
	if runtime.State == "none" {
		runtime.State = "standby_registered"
	}
	return runtime
}

func RemoveStandbyGatewayID(runtime FailoverRuntime, gatewayID string) FailoverRuntime {
	gatewayID = strings.TrimSpace(gatewayID)
	runtime = NormalizeFailoverRuntime(runtime)
	if gatewayID == "" {
		return runtime
	}
	out := runtime.StandbyISCSIGatewayIDs[:0]
	for _, candidate := range runtime.StandbyISCSIGatewayIDs {
		if candidate != gatewayID {
			out = append(out, candidate)
		}
	}
	runtime.StandbyISCSIGatewayIDs = out
	return runtime
}

func EvaluateFailoverWriteAdmission(runtime FailoverRuntime, gatewayID string, observedEpoch uint64) (FailoverRuntime, FailoverWriteAdmission) {
	gatewayID = strings.TrimSpace(gatewayID)
	runtime = NormalizeFailoverRuntime(runtime)
	decision := FailoverWriteAdmission{
		GatewayID:            gatewayID,
		ActiveISCSIGatewayID: runtime.ActiveISCSIGatewayID,
		ObservedExportEpoch:  observedEpoch,
		ExportEpoch:          runtime.ExportEpoch,
		SCSIStatus:           "good",
		WriterPolicy:         runtime.WriterPolicy,
		HAFailoverMode:       runtime.HAFailoverMode,
	}
	reject := func(reason string, stale, standby bool) {
		decision.WriteAdmitted = false
		decision.StaleGatewayRejected = stale
		decision.StandbyWriteRejected = standby
		decision.RejectionReason = reason
		decision.SCSIStatus = "check_condition"
		decision.SenseKey = "data_protect"
		runtime.StaleGatewayRejected = runtime.StaleGatewayRejected || stale
		runtime.StandbyWriteRejected = runtime.StandbyWriteRejected || standby
		runtime.LastRejectedISCSIGatewayID = gatewayID
		runtime.LastWriteGatewayID = gatewayID
		runtime.LastWriteObservedEpoch = observedEpoch
		runtime.LastWriteAdmitted = false
		runtime.LastWriteRejectionReason = reason
		runtime.LastWriteSCSIStatus = decision.SCSIStatus
		runtime.LastWriteSenseKey = decision.SenseKey
	}
	switch {
	case gatewayID == "":
		reject("missing_gateway_id", false, false)
	case runtime.StaleGatewayRevokedID != "" && runtime.StaleGatewayRevokedID == gatewayID:
		reject("revoked_stale_gateway", true, false)
	case runtime.ActiveISCSIGatewayID == "":
		reject("no_active_gateway", false, false)
	case observedEpoch != runtime.ExportEpoch:
		reject("stale_export_epoch", true, false)
	case gatewayID != runtime.ActiveISCSIGatewayID:
		runtime = AddStandbyGatewayID(runtime, gatewayID)
		reject("standby_gateway", false, true)
	default:
		decision.WriteAdmitted = true
		runtime.LastWriteGatewayID = gatewayID
		runtime.LastWriteObservedEpoch = observedEpoch
		runtime.LastWriteAdmitted = true
		runtime.LastWriteRejectionReason = ""
		runtime.LastWriteSCSIStatus = "good"
		runtime.LastWriteSenseKey = ""
	}
	return NormalizeFailoverRuntime(runtime), decision
}

func uniqueNonEmptyStrings(values []string) []string {
	if len(values) == 0 {
		return nil
	}
	seen := map[string]bool{}
	out := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" || seen[value] {
			continue
		}
		seen[value] = true
		out = append(out, value)
	}
	return out
}

func sanitizeSCSISerial(raw string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		raw = "namrbd-lun"
	}
	var b strings.Builder
	for _, r := range raw {
		switch {
		case r >= 'a' && r <= 'z':
			b.WriteRune(r)
		case r >= 'A' && r <= 'Z':
			b.WriteRune(r)
		case r >= '0' && r <= '9':
			b.WriteRune(r)
		case r == '-' || r == '_' || r == '.' || r == ':':
			b.WriteRune(r)
		default:
			b.WriteRune('-')
		}
		if b.Len() >= 64 {
			break
		}
	}
	out := strings.Trim(b.String(), "-_.:")
	if out == "" {
		return "namrbd-lun"
	}
	return out
}
