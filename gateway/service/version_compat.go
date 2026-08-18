package service

import (
	"fmt"
	"strings"

	namrbdversion "github.com/nosway/namrbd/version"
)

func CheckSBSVersionCompatibility(clientVersion, serverVersion string) error {
	rawClientVersion := strings.TrimSpace(clientVersion)
	rawServerVersion := strings.TrimSpace(serverVersion)

	clientVersion, clientErr := namrbdversion.NormalizeProductVersion(rawClientVersion)
	if clientErr != nil {
		return fmt.Errorf("invalid client version %q: %w", rawClientVersion, clientErr)
	}
	serverVersion, serverErr := namrbdversion.NormalizeProductVersion(rawServerVersion)
	if serverErr != nil {
		return fmt.Errorf("invalid server version %q: %w", rawServerVersion, serverErr)
	}
	if clientVersion != serverVersion {
		return fmt.Errorf("sbs version mismatch: client=%q server=%q", clientVersion, serverVersion)
	}
	return nil
}
