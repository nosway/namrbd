package main

import (
	"fmt"

	"github.com/nosway/namrbd/internal/depavail"
	"github.com/nosway/namrbd/internal/serviceconfig"
)

// dependencyTracker holds this process's dependency availability state.
//
// AA-IMPL-004A builds it from the reviewed config so the thresholds are
// validated at startup and republished on the status surface. The observation
// loop that drives it and the enforcement points that consult it are
// AA-IMPL-004B and 004C; until those land the tracker reports the healthy state
// it starts in, which is honest, because nothing is observing a failure yet.
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
