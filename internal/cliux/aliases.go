package cliux

import (
	"flag"
	"fmt"
	"io"
	"sort"
	"strings"
)

// Alias maps one accepted legacy flag name to its canonical replacement.
type Alias struct {
	Legacy    string
	Canonical string
	// DeprecatedIn records when compatibility warning started. Empty is
	// accepted by the helper for tests, but product alias tables set it.
	DeprecatedIn string
}

// InstallStructuredUsage gives daemon and control binaries the same stable
// help shape while allowing fixture-only flags to remain accepted but hidden
// from the production-facing help surface.
func InstallStructuredUsage(fs *flag.FlagSet, command string, hidden func(string) bool) {
	fs.Usage = func() {
		w := fs.Output()
		_, _ = fmt.Fprintf(w, "Usage: %s [flags]\n\nFlags:\n", command)
		fs.VisitAll(func(f *flag.Flag) {
			if hidden != nil && hidden(f.Name) {
				return
			}
			name, usage := flag.UnquoteUsage(f)
			if name == "" {
				_, _ = fmt.Fprintf(w, "  --%s\n", f.Name)
			} else {
				_, _ = fmt.Fprintf(w, "  --%s %s\n", f.Name, name)
			}
			_, _ = fmt.Fprintf(w, "      %s (default %s)\n", usage, f.DefValue)
		})
	}
}

// RewriteDeprecatedFlags preserves compatibility while ensuring the standard
// flag package and its help output only need to know canonical names.
func RewriteDeprecatedFlags(args []string, aliases []Alias, stderr io.Writer) []string {
	byLegacy := make(map[string]string, len(aliases))
	for _, alias := range aliases {
		legacy := strings.TrimLeft(strings.TrimSpace(alias.Legacy), "-")
		canonical := strings.TrimLeft(strings.TrimSpace(alias.Canonical), "-")
		if legacy != "" && canonical != "" && legacy != canonical {
			byLegacy[legacy] = canonical
		}
	}
	out := append([]string(nil), args...)
	warned := map[string]bool{}
	for i, arg := range out {
		prefix := ""
		switch {
		case strings.HasPrefix(arg, "--"):
			prefix = "--"
		case strings.HasPrefix(arg, "-"):
			prefix = "-"
		default:
			continue
		}
		body := strings.TrimPrefix(arg, prefix)
		name, value, hasValue := strings.Cut(body, "=")
		canonical, ok := byLegacy[name]
		if !ok {
			continue
		}
		out[i] = prefix + canonical
		if hasValue {
			out[i] += "=" + value
		}
		if !warned[name] && stderr != nil {
			_, _ = fmt.Fprintf(stderr, "deprecated flag --%s: use --%s", name, canonical)
			for _, alias := range aliases {
				if strings.TrimLeft(alias.Legacy, "-") == name && alias.DeprecatedIn != "" {
					_, _ = fmt.Fprintf(stderr, " (deprecated in %s)", alias.DeprecatedIn)
					break
				}
			}
			_, _ = fmt.Fprintln(stderr)
			warned[name] = true
		}
	}
	return out
}

func SortedAliases(aliases []Alias) []Alias {
	out := append([]Alias(nil), aliases...)
	sort.Slice(out, func(i, j int) bool { return out[i].Legacy < out[j].Legacy })
	return out
}

// RewriteCommandArgs translates the two cross-command conveniences supported
// by NAMRBD CLIs. A trailing "help" is equivalent to --help, and --json is a
// shorthand for --output=json when the command has an output selector.
func RewriteCommandArgs(args []string, supportsOutput, hasNativeJSON bool) []string {
	out := append([]string(nil), args...)
	if len(out) > 0 && out[len(out)-1] == "help" {
		out[len(out)-1] = "--help"
	}
	if !supportsOutput || hasNativeJSON {
		return out
	}
	for i, arg := range out {
		if arg == "--json" || arg == "-json" {
			out[i] = "--output=json"
		}
	}
	return out
}
