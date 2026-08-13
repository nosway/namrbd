package metadata

// PlacementApplyAuthority groups the write-side placement update contract:
// allocation page persistence plus extent mapping normalization/update.
type PlacementApplyAuthority interface {
	AllocationPersistStore
	ExtentMappingNormalizeStore
}

// WritePlanningAuthority extends placement apply with monotonic chunk ID sequencing.
type WritePlanningAuthority interface {
	ChunkIDSequenceStore
	PlacementApplyAuthority
}
