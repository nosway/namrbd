package serviceconfig

import (
	"fmt"
	"strings"

	"gopkg.in/yaml.v3"
)

// SecretPlaceholder marks a secret a generated config cannot carry.
//
// A flag that holds literal secret material has nowhere safe to go in a config
// file, so the generator emits a reference the operator must point at real
// material. Emitting the value would defeat the reason config files carry
// references, and emitting nothing would produce a file that starts without
// credentials.
const SecretPlaceholder = "REPLACE_ME"

// GenerateResult carries a generated config plus what an operator still has to
// do before it can be used.
type GenerateResult struct {
	YAML string `json:"-"`
	// SecretsToSupply names each field whose material could not be carried
	// over, so a migration cannot silently produce a config that starts
	// without credentials.
	SecretsToSupply []string `json:"config_generate_secrets_to_supply"`
	// DroppedFlags names flags that were set but have no config representation,
	// which is how a migration would otherwise lose a setting quietly.
	DroppedFlags []string `json:"config_generate_dropped_flags"`
	Process      string   `json:"config_generate_process"`
	Profile      string   `json:"config_generate_profile"`
}

// Generate marshals a config file built from a running invocation's flags.
//
// The result is a starting point for review, not a drop-in replacement: it
// carries a revision of 1 and the dev profile unless the caller chose
// otherwise, because a generated file has not been reviewed yet and the strict
// profile refuses settings a flag-started deployment may still rely on.
func Generate(f *File, secretsToSupply, droppedFlags []string) (GenerateResult, error) {
	if f == nil {
		return GenerateResult{}, fmt.Errorf("nothing to generate from")
	}
	blob, err := yaml.Marshal(f)
	if err != nil {
		return GenerateResult{}, fmt.Errorf("marshal generated config: %w", err)
	}
	body := string(blob)

	// A generated file must not carry a secret literal, whatever the caller
	// passed in. This is the last check before the bytes leave the process.
	if hits := ScanForSecretLiterals(body); len(hits) > 0 {
		return GenerateResult{}, fmt.Errorf("generated config would carry secret values: %s",
			strings.Join(hits, "; "))
	}

	header := fmt.Sprintf(`# Generated from a running %s invocation by --print-config.
#
# Review before use. Two things are deliberate:
#
#   * The profile is %q. A generated file has not been reviewed, and the
#     large_scale profile refuses settings a flag-started deployment may still
#     rely on. Switch it once the settings below have been checked.
#   * Any %s below is a secret this file could not carry. Point it at real
#     material before starting the process.
#
# Install this file mode 0600 and owned by the service user.
`, f.Process, f.Profile, SecretPlaceholder)

	return GenerateResult{
		YAML:            header + body,
		SecretsToSupply: secretsToSupply,
		DroppedFlags:    droppedFlags,
		Process:         f.Process,
		Profile:         f.Profile,
	}, nil
}

// SecretRefFor turns a flag value into a reference for a generated config.
//
// A path-valued flag becomes a file reference directly. A flag that held the
// material itself becomes a placeholder, and the field is reported so the
// operator knows what to supply.
func SecretRefFor(field, flagValue string, valueIsPath bool, report *[]string) SecretRef {
	v := strings.TrimSpace(flagValue)
	if v == "" {
		return SecretRef{}
	}
	if valueIsPath {
		return SecretRef{File: v}
	}
	if report != nil {
		*report = append(*report, field)
	}
	return SecretRef{File: SecretPlaceholder}
}
