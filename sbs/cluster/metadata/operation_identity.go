package metadata

import (
	"fmt"
	"strings"
)

// TransitionMutationOperationID returns the globally unique parent identity
// used by transition and transition-batch mutation records. Placement refs are
// scoped to a volume, so the canonical volume ID must be part of the identity.
func TransitionMutationOperationID(volumeID, placementRef string) string {
	canonicalVolumeID, err := CanonicalVolumeID(volumeID)
	rawPlacementRef := placementRef
	placementRef = strings.TrimSpace(placementRef)
	if err != nil || canonicalVolumeID != volumeID || placementRef == "" || placementRef != rawPlacementRef {
		return ""
	}
	return fmt.Sprintf("transition-%s-%s", canonicalVolumeID, placementRef)
}
