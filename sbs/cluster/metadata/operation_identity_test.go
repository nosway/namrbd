package metadata

import "testing"

func TestTransitionMutationOperationIDIncludesVolumeScope(t *testing.T) {
	first := TransitionMutationOperationID("00a1b2c3", "pl-000001")
	second := TransitionMutationOperationID("00a1b2c4", "pl-000001")
	if first != "transition-00a1b2c3-pl-000001" || second != "transition-00a1b2c4-pl-000001" || first == second {
		t.Fatalf("transition operation ids first=%q second=%q", first, second)
	}
	for _, test := range []struct{ volumeID, placementRef string }{
		{"a1b2c3", "pl-1"},
		{"00A1B2C3", "pl-1"},
		{"00a1b2c3", ""},
		{"00a1b2c3", " pl-1 "},
	} {
		if got := TransitionMutationOperationID(test.volumeID, test.placementRef); got != "" {
			t.Fatalf("invalid identity accepted volume=%q placement=%q got=%q", test.volumeID, test.placementRef, got)
		}
	}
}
