package main

import (
	"fmt"
	"strings"

	"github.com/nosway/namrbd/internal/depavail"
	"github.com/nosway/namrbd/internal/serviceconfig"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// dependencyTracker holds this process's dependency availability state.
//
// AA-IMPL-004A builds it from the reviewed config so the thresholds are
// validated at startup and republished on the status surface. AA-IMPL-004B
// feeds it from existing dependency outcomes, and AA-IMPL-004C consults it at
// mutation enforcement points.
var dependencyTracker = depavail.NewTracker(depavail.DefaultThresholds())

// installDependencyThresholds applies the config section, replacing the
// defaults the process started with.
func installDependencyThresholds(field string, t *depavail.Thresholds) error {
	tr, err := depavail.NewValidatedTracker(serviceconfig.DependencyThresholds(t))
	if err != nil {
		return fmt.Errorf("%s: %w", field, err)
	}
	dependencyTracker = tr
	return nil
}

func dependencyMutationBehavior() (*depavail.Tracker, depavail.Behavior) {
	tr := dependencyTracker
	if tr == nil {
		return nil, depavail.Resolve(depavail.State{
			Etcd:       depavail.Available,
			TiKV:       depavail.Available,
			Projection: depavail.ProjectionFresh,
		})
	}
	return tr, tr.Refresh()
}

func dependencyMutationDenied(scope string, decision depavail.Decision, b depavail.Behavior) error {
	reason := "dependency view is untrusted"
	if len(b.Reasons) > 0 {
		reason = strings.Join(b.Reasons, "; ")
	}
	return status.Errorf(codes.FailedPrecondition, "%s %s while dependency readiness is %s: %s", scope, decision, b.Readiness, reason)
}

func enforceDependencyMembershipChange() error {
	tr, b := dependencyMutationBehavior()
	if b.MembershipChange == depavail.DecisionAllowed {
		return nil
	}
	if tr != nil {
		tr.CountMembershipRejected()
	}
	return dependencyMutationDenied("membership change", b.MembershipChange, b)
}

func enforceDependencyExportAdmission(kind string) error {
	tr, b := dependencyMutationBehavior()
	if b.ExportAdmission == depavail.DecisionAllowed {
		return nil
	}
	if tr != nil {
		tr.CountAdmissionBlocked()
	}
	return dependencyMutationDenied(strings.TrimSpace(kind), b.ExportAdmission, b)
}

func enforceDependencyExportFailover(kind string) error {
	tr, b := dependencyMutationBehavior()
	if b.ExportFailover == depavail.DecisionAllowed {
		return nil
	}
	if tr != nil {
		tr.CountFailoverSuppressed()
	}
	return dependencyMutationDenied(strings.TrimSpace(kind), b.ExportFailover, b)
}

func enforceDependencyISCSIRegistryMutation(kind string) error {
	switch {
	case strings.HasPrefix(kind, "iscsi.failover."):
		return enforceDependencyExportFailover(kind)
	case kind == "iscsi.lun.export":
		return enforceDependencyExportAdmission(kind)
	default:
		return nil
	}
}
