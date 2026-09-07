package clustermanifest

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
)

const (
	ObservedStateAPIVersion = "namrbd.io/v1alpha1"
	ObservedStateKind       = "SBSClusterObservedState"

	ActionCreate  = "create"
	ActionUpdate  = "update"
	ActionRestart = "restart"
	ActionNoOp    = "no-op"
	ActionBlocked = "blocked"
)

type ObservedState struct {
	APIVersion     string         `json:"apiVersion"`
	Kind           string         `json:"kind"`
	ManifestDigest string         `json:"manifestDigest"`
	Nodes          []ObservedNode `json:"nodes"`
}

type ObservedNode struct {
	ID             string   `json:"id"`
	Present        bool     `json:"present"`
	BundleDigest   string   `json:"bundleDigest,omitempty"`
	DaemonState    string   `json:"daemonState,omitempty"`
	BlockedReasons []string `json:"blockedReasons,omitempty"`
}

type PlanAction struct {
	NodeID              string   `json:"node_id"`
	Zone                string   `json:"zone"`
	Action              string   `json:"action"`
	Reason              string   `json:"reason"`
	DesiredBundleDigest string   `json:"desired_bundle_digest"`
	CurrentBundleDigest string   `json:"current_bundle_digest,omitempty"`
	BlockedReasons      []string `json:"blocked_reasons,omitempty"`
}

type Plan struct {
	APIVersion               string         `json:"api_version"`
	Kind                     string         `json:"kind"`
	PlanID                   string         `json:"plan_id"`
	PlanDigest               string         `json:"plan_digest"`
	DesiredManifestDigest    string         `json:"desired_manifest_digest"`
	CurrentManifestDigest    string         `json:"current_manifest_digest,omitempty"`
	Actions                  []PlanAction   `json:"actions"`
	ActionCounts             map[string]int `json:"action_counts"`
	ProjectedDaemonRestarts  int            `json:"projected_daemon_restarts"`
	ExecutedTiKVMutations    int            `json:"executed_tikv_mutations"`
	ExecutedDaemonActions    int            `json:"executed_daemon_actions"`
	ExecutedStorageMutations int            `json:"executed_storage_mutations"`
	NoMutationDryRun         bool           `json:"no_mutation_dry_run"`
	PlanUnblocked            bool           `json:"plan_unblocked"`
}

func ParseObservedState(raw []byte) (*ObservedState, error) {
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	var state ObservedState
	if err := dec.Decode(&state); err != nil {
		return nil, fmt.Errorf("decode observed state: %w", err)
	}
	var extra any
	if err := dec.Decode(&extra); err != io.EOF {
		if err == nil {
			return nil, fmt.Errorf("decode observed state: multiple JSON documents are not allowed")
		}
		return nil, fmt.Errorf("decode observed state trailing data: %w", err)
	}
	if err := ValidateObservedState(&state); err != nil {
		return nil, err
	}
	return &state, nil
}

func LoadObservedState(path string) (*ObservedState, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read observed state %s: %w", path, err)
	}
	return ParseObservedState(raw)
}

func ValidateObservedState(state *ObservedState) error {
	if state == nil {
		return fmt.Errorf("observed state is nil")
	}
	if state.APIVersion != ObservedStateAPIVersion || state.Kind != ObservedStateKind {
		return fmt.Errorf("observed state identity must be apiVersion=%s kind=%s", ObservedStateAPIVersion, ObservedStateKind)
	}
	if state.ManifestDigest != "" && !sha256Pattern.MatchString(state.ManifestDigest) {
		return fmt.Errorf("observed state manifestDigest is not canonical sha256")
	}
	seen := map[string]bool{}
	for i, node := range state.Nodes {
		if seen[node.ID] {
			return fmt.Errorf("observed state node %d duplicates id %q", i, node.ID)
		}
		seen[node.ID] = true
		if nodeIDPattern.FindStringSubmatch(node.ID) == nil {
			return fmt.Errorf("observed state node %d has invalid id %q", i, node.ID)
		}
		if node.BundleDigest != "" && !sha256Pattern.MatchString(node.BundleDigest) {
			return fmt.Errorf("observed state node %s bundleDigest is not canonical sha256", node.ID)
		}
		switch node.DaemonState {
		case "", "running", "stopped":
		default:
			return fmt.Errorf("observed state node %s daemonState %q is not running or stopped", node.ID, node.DaemonState)
		}
		for _, reason := range node.BlockedReasons {
			if strings.TrimSpace(reason) == "" {
				return fmt.Errorf("observed state node %s has an empty blocked reason", node.ID)
			}
		}
	}
	return nil
}

// ObservedStateFromRenderSet constructs an explicit comparison input. It does
// not claim that a daemon or host was observed; callers must choose the daemon
// state and persist/provenance the result separately when using real evidence.
func ObservedStateFromRenderSet(set *RenderSet, daemonState string) (*ObservedState, error) {
	if set == nil {
		return nil, fmt.Errorf("render set is nil")
	}
	if daemonState != "running" && daemonState != "stopped" {
		return nil, fmt.Errorf("daemon state must be running or stopped")
	}
	state := &ObservedState{APIVersion: ObservedStateAPIVersion, Kind: ObservedStateKind, ManifestDigest: set.ManifestDigest}
	for _, bundle := range set.Bundles {
		state.Nodes = append(state.Nodes, ObservedNode{ID: bundle.NodeID, Present: true, BundleDigest: bundle.BundleDigest, DaemonState: daemonState})
	}
	return state, nil
}

// BuildPlan computes a deterministic impact plan. The executed counters are
// invariants of this function, not observations: BuildPlan never applies an
// action or contacts TiKV, a daemon, or a host.
func BuildPlan(desired *Manifest, current *ObservedState, policy ValidationPolicy) (*Plan, error) {
	rendered, err := Render(desired, policy)
	if err != nil {
		return nil, err
	}
	if current == nil {
		current = &ObservedState{APIVersion: ObservedStateAPIVersion, Kind: ObservedStateKind}
	}
	if err := ValidateObservedState(current); err != nil {
		return nil, err
	}
	currentByID := make(map[string]ObservedNode, len(current.Nodes))
	for _, node := range current.Nodes {
		currentByID[node.ID] = node
	}

	plan := &Plan{
		APIVersion: "namrbd.io/v1alpha1", Kind: "SBSClusterPlan",
		DesiredManifestDigest: rendered.ManifestDigest, CurrentManifestDigest: current.ManifestDigest,
		ActionCounts:          map[string]int{ActionCreate: 0, ActionUpdate: 0, ActionRestart: 0, ActionNoOp: 0, ActionBlocked: 0},
		ExecutedTiKVMutations: 0, ExecutedDaemonActions: 0, ExecutedStorageMutations: 0,
		NoMutationDryRun: true,
	}
	desiredIDs := map[string]bool{}
	for _, bundle := range rendered.Bundles {
		desiredIDs[bundle.NodeID] = true
		observed, exists := currentByID[bundle.NodeID]
		action := PlanAction{NodeID: bundle.NodeID, Zone: bundle.Zone, DesiredBundleDigest: bundle.BundleDigest}
		switch {
		case !exists || !observed.Present:
			action.Action = ActionCreate
			action.Reason = "node is absent from current state"
		case len(observed.BlockedReasons) > 0:
			action.Action = ActionBlocked
			action.Reason = "current state has unresolved preflight blockers"
			action.CurrentBundleDigest = observed.BundleDigest
			action.BlockedReasons = append([]string(nil), observed.BlockedReasons...)
			sort.Strings(action.BlockedReasons)
		case observed.BundleDigest == bundle.BundleDigest:
			action.Action = ActionNoOp
			action.Reason = "current bundle digest matches desired bundle digest"
			action.CurrentBundleDigest = observed.BundleDigest
		case observed.DaemonState == "running":
			action.Action = ActionRestart
			action.Reason = "running node bundle differs from desired bundle"
			action.CurrentBundleDigest = observed.BundleDigest
			plan.ProjectedDaemonRestarts++
		default:
			action.Action = ActionUpdate
			action.Reason = "stopped node bundle differs from desired bundle"
			action.CurrentBundleDigest = observed.BundleDigest
		}
		plan.ActionCounts[action.Action]++
		plan.Actions = append(plan.Actions, action)
	}
	for _, observed := range current.Nodes {
		if desiredIDs[observed.ID] {
			continue
		}
		plan.Actions = append(plan.Actions, PlanAction{
			NodeID: observed.ID, Action: ActionBlocked,
			Reason:              "current node is not present in the exact desired manifest; removal is a separate operation",
			CurrentBundleDigest: observed.BundleDigest,
			BlockedReasons:      []string{"unexpected current node"},
		})
		plan.ActionCounts[ActionBlocked]++
	}
	sort.Slice(plan.Actions, func(i, j int) bool { return nodeLess(plan.Actions[i].NodeID, plan.Actions[j].NodeID) })
	plan.PlanUnblocked = plan.ActionCounts[ActionBlocked] == 0
	plan.PlanDigest = digestPlan(plan)
	plan.PlanID = "ad-plan-" + strings.TrimPrefix(plan.PlanDigest, "sha256:")[:16]
	return plan, nil
}

func digestPlan(plan *Plan) string {
	copyPlan := *plan
	copyPlan.PlanID = ""
	copyPlan.PlanDigest = ""
	raw, _ := json.Marshal(copyPlan)
	sum := sha256.Sum256(raw)
	return "sha256:" + hex.EncodeToString(sum[:])
}

func MarshalPlan(plan *Plan) ([]byte, error) {
	if plan == nil {
		return nil, fmt.Errorf("plan is nil")
	}
	raw, err := json.MarshalIndent(plan, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("marshal plan: %w", err)
	}
	return append(raw, '\n'), nil
}
