package metadata

import "context"

type ExtentMappingNormalizeStore interface {
	GetExtentMapping(ctx context.Context, volumeID string, extentID uint64) (ExtentMappingRecord, error)
	PutExtentMapping(ctx context.Context, rec ExtentMappingRecord) error
}

func NormalizeExtentMappings(ctx context.Context, store ExtentMappingNormalizeStore, volumeID string, extentIDs []uint64, revision uint64) error {
	for _, extentID := range extentIDs {
		mapping, err := store.GetExtentMapping(ctx, volumeID, extentID)
		if err != nil {
			return err
		}
		mapping.ChunkID = 0
		if mapping.Revision < revision {
			mapping.Revision = revision
		}
		if err := store.PutExtentMapping(ctx, mapping); err != nil {
			return err
		}
	}
	return nil
}
