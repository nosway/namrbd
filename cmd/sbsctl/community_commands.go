//go:build !enterprise

package main

import "fmt"

func runEnterpriseTopLevel(args []string) bool {
	if len(args) == 0 {
		return false
	}
	switch args[0] {
	case "ec", "backup", "dr", "performance", "security", "mobility", "dedupe":
		fatalf("%s", enterpriseCapabilityRequiredMessage(args[0]))
		return true
	}
	return false
}

func enterpriseUsageLines() []string {
	return nil
}

func enterpriseCapabilityRequiredMessage(command string) string {
	return fmt.Sprintf("enterprise_capability_required: %s requires an enterprise build", command)
}
