// Package envcompat owns environment-variable rename compatibility.
package envcompat

import (
	"fmt"
	"io"
	"sort"
	"strings"

	namrbdversion "github.com/nosway/namrbd/version"
)

const (
	DeprecatedIn = "v1.0.0"
	RemovedIn    = "v1.1.0"
)

type Lookup func(string) (string, bool)

type Legacy struct {
	Name         string
	DeprecatedIn string
	RemovedIn    string
}

type Spec struct {
	Canonical string
	Legacy    []Legacy
}

type Resolution struct {
	Value      string
	Source     string
	Present    bool
	LegacyUsed bool
	Conflict   bool
	Warnings   []string
}

func (s Spec) Matches(name string) bool {
	name = strings.TrimSpace(name)
	if name == strings.TrimSpace(s.Canonical) {
		return true
	}
	for _, legacy := range s.Legacy {
		if name == strings.TrimSpace(legacy.Name) {
			return true
		}
	}
	return false
}

func LegacyName(name string) Legacy {
	return Legacy{Name: name, DeprecatedIn: DeprecatedIn, RemovedIn: RemovedIn}
}

func New(canonical string, legacy ...string) Spec {
	s := Spec{Canonical: canonical, Legacy: make([]Legacy, 0, len(legacy))}
	for _, name := range legacy {
		s.Legacy = append(s.Legacy, LegacyName(name))
	}
	return s
}

// Resolve applies canonical-over-legacy precedence. Legacy variables remain
// accepted by v1.0.x binaries and become startup errors in v1.1.0.
func Resolve(spec Spec, lookup Lookup, productVersion string) (Resolution, error) {
	if lookup == nil {
		return Resolution{}, fmt.Errorf("environment lookup is nil")
	}
	canonical := strings.TrimSpace(spec.Canonical)
	if canonical == "" {
		return Resolution{}, fmt.Errorf("canonical environment variable is empty")
	}

	type value struct {
		name  string
		value string
		meta  Legacy
	}
	legacyValues := make([]value, 0, len(spec.Legacy))
	for _, legacy := range spec.Legacy {
		name := strings.TrimSpace(legacy.Name)
		if name == "" {
			continue
		}
		if v, ok := lookup(name); ok {
			legacyValues = append(legacyValues, value{name: name, value: v, meta: legacy})
		}
	}
	if len(legacyValues) > 0 {
		var removed []string
		for _, legacy := range legacyValues {
			removedIn := strings.TrimSpace(legacy.meta.RemovedIn)
			if removedIn == "" {
				continue
			}
			atLeast, err := namrbdversion.AtLeast(productVersion, removedIn)
			if err == nil && atLeast {
				removed = append(removed, legacy.name)
			}
		}
		if len(removed) > 0 {
			sort.Strings(removed)
			return Resolution{}, fmt.Errorf("removed environment variable(s) %s: use %s (removed in %s)",
				strings.Join(removed, ", "), canonical, RemovedIn)
		}
	}

	resolved := Resolution{}
	if v, ok := lookup(canonical); ok {
		resolved.Value = v
		resolved.Source = canonical
		resolved.Present = true
	}
	for _, legacy := range legacyValues {
		resolved.LegacyUsed = true
		deprecatedIn := legacy.meta.DeprecatedIn
		if deprecatedIn == "" {
			deprecatedIn = DeprecatedIn
		}
		removedIn := legacy.meta.RemovedIn
		if removedIn == "" {
			removedIn = RemovedIn
		}
		resolved.Warnings = append(resolved.Warnings,
			fmt.Sprintf("deprecated environment variable %s: use %s (deprecated in %s; removed in %s)",
				legacy.name, canonical, deprecatedIn, removedIn))
		if !resolved.Present {
			resolved.Value = legacy.value
			resolved.Source = legacy.name
			resolved.Present = true
			continue
		}
		if legacy.value != resolved.Value {
			resolved.Conflict = true
			resolved.Warnings = append(resolved.Warnings,
				fmt.Sprintf("environment variable %s conflicts with %s; %s wins",
					legacy.name, resolved.Source, resolved.Source))
		}
	}
	return resolved, nil
}

func ResolveCurrent(spec Spec, lookup Lookup) (Resolution, error) {
	return Resolve(spec, lookup, namrbdversion.ProductVersion())
}

func WriteWarnings(w io.Writer, warnings []string) {
	if w == nil {
		return
	}
	for _, warning := range warnings {
		_, _ = fmt.Fprintln(w, warning)
	}
}
