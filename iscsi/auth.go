package iscsi

import (
	"fmt"
	"strings"
)

const (
	AuthModeNone = "none"
	AuthModeCHAP = "chap"

	AuthPolicyNoAuthAllowlistFirst             = "no_auth_allowlist_first"
	AuthPolicyNoAuthAllowlistRuntimeFailClosed = "no_auth_allowlist_runtime_fail_closed"
	AuthPolicyCHAPRuntimeFailClosed            = "chap_runtime_fail_closed"

	AuthRuntimeClaimGotgtNoneOnly             = "gotgt_v0.2.2_authmethod_none_only"
	InitiatorAllowlistRuntimeClaimGotgtNoHook = "gotgt_v0.2.2_no_initiator_allowlist_hook"
)

type AuthConfig struct {
	Mode                 string
	CHAPSecretRef        string
	AllowedInitiatorIQNs []string
}

func NormalizeAuthConfig(cfg AuthConfig) (AuthConfig, error) {
	cfg.Mode = strings.TrimSpace(strings.ToLower(cfg.Mode))
	if cfg.Mode == "" {
		cfg.Mode = AuthModeNone
	}
	switch cfg.Mode {
	case AuthModeNone:
		cfg.CHAPSecretRef = ""
	case AuthModeCHAP:
		cfg.CHAPSecretRef = strings.TrimSpace(cfg.CHAPSecretRef)
		if cfg.CHAPSecretRef == "" {
			return cfg, fmt.Errorf("auth mode chap requires a CHAP secret reference")
		}
	default:
		return cfg, fmt.Errorf("auth mode must be none or chap")
	}
	cfg.AllowedInitiatorIQNs = normalizeInitiatorAllowlist(cfg.AllowedInitiatorIQNs)
	return cfg, nil
}

func (cfg AuthConfig) Policy() string {
	if cfg.Mode == AuthModeCHAP {
		return AuthPolicyCHAPRuntimeFailClosed
	}
	if len(cfg.AllowedInitiatorIQNs) > 0 {
		return AuthPolicyNoAuthAllowlistRuntimeFailClosed
	}
	return AuthPolicyNoAuthAllowlistFirst
}

func (cfg AuthConfig) RuntimeCHAPSupported() bool {
	return false
}

func (cfg AuthConfig) RuntimeAuthError() error {
	if cfg.Mode == AuthModeCHAP {
		return fmt.Errorf("CHAP runtime is not supported by gotgt %s; AuthMethod=None only; refusing to start target", TargetStackVersion)
	}
	if len(cfg.AllowedInitiatorIQNs) > 0 {
		return fmt.Errorf("initiator allowlist runtime enforcement is not supported by gotgt %s; refusing to start target", TargetStackVersion)
	}
	return nil
}

func ApplyMemoryAuthSummary(summary *Summary, cfg AuthConfig) {
	if summary == nil {
		return
	}
	summary.AuthPolicy = cfg.Policy()
	summary.AuthMode = cfg.Mode
	summary.RuntimeCHAPSupported = cfg.RuntimeCHAPSupported()
	summary.AuthRuntimeClaim = AuthRuntimeClaimGotgtNoneOnly
	summary.RuntimeInitiatorAllowlistSupported = false
	summary.InitiatorAllowlistRuntimeClaim = InitiatorAllowlistRuntimeClaimGotgtNoHook
	summary.CHAPSecretRef = cfg.CHAPSecretRef
	summary.AllowedInitiatorIQNs = append([]string(nil), cfg.AllowedInitiatorIQNs...)
}

func ApplySBSAuthSummary(summary *SBSAdapterSummary, cfg AuthConfig) {
	if summary == nil {
		return
	}
	summary.AuthPolicy = cfg.Policy()
	summary.AuthMode = cfg.Mode
	summary.RuntimeCHAPSupported = cfg.RuntimeCHAPSupported()
	summary.AuthRuntimeClaim = AuthRuntimeClaimGotgtNoneOnly
	summary.RuntimeInitiatorAllowlistSupported = false
	summary.InitiatorAllowlistRuntimeClaim = InitiatorAllowlistRuntimeClaimGotgtNoHook
	summary.CHAPSecretRef = cfg.CHAPSecretRef
	summary.AllowedInitiatorIQNs = append([]string(nil), cfg.AllowedInitiatorIQNs...)
}

func MarkMemoryAuthRuntimeError(summary *Summary, err error) {
	if summary == nil || err == nil {
		return
	}
	summary.Result = "error"
	summary.TargetStackAccepted = false
	summary.SCSIStatus = "check_condition"
	summary.SenseKey = "data_protect"
	summary.ErrorCount = 1
	summary.FirstError = err.Error()
	summary.LastError = err.Error()
}

func MarkSBSAuthRuntimeError(summary *SBSAdapterSummary, err error) {
	if summary == nil || err == nil {
		return
	}
	summary.Result = "error"
	summary.TargetStackAccepted = false
	summary.SCSIStatus = "check_condition"
	summary.SenseKey = "data_protect"
	summary.ErrorCount = 1
	summary.FirstError = err.Error()
	summary.LastError = err.Error()
}

func normalizeInitiatorAllowlist(values []string) []string {
	seen := map[string]bool{}
	out := []string{}
	for _, raw := range values {
		for _, item := range strings.Split(raw, ",") {
			item = strings.TrimSpace(item)
			if item == "" || seen[item] {
				continue
			}
			seen[item] = true
			out = append(out, item)
		}
	}
	return out
}
