package common

import (
	"context"
	"path"

	"github.com/milvus-io/birdwatcher/models"
	"github.com/milvus-io/birdwatcher/states/kv"
)

var snapshotMetaKeySpec = MetaKeySpec{
	Prefix: path.Join(DCPrefix, DCSnapshotPrefix),
	Parts:  []MetaKeyPart{KeyCollectionID, KeySnapshotID},
}

type SnapshotSelector struct {
	CollectionID int64
	SnapshotID   int64
	Filters      []PostFilter[models.Snapshot]
}

func (s SnapshotSelector) MetaKeyHints() MetaKeyHints {
	return NewMetaKeyHints().
		WithInt64(KeyCollectionID, s.CollectionID).
		WithInt64(KeySnapshotID, s.SnapshotID)
}

func (s SnapshotSelector) Match(snapshot *models.Snapshot) bool {
	proto := snapshot.GetProto()
	if s.CollectionID > 0 && proto.GetCollectionId() != s.CollectionID {
		return false
	}
	if s.SnapshotID > 0 && proto.GetId() != s.SnapshotID {
		return false
	}
	for _, filter := range s.Filters {
		if !filter.Match(snapshot) {
			return false
		}
	}
	return true
}

func ListSnapshots(ctx context.Context, cli kv.MetaKV, basePath string, filters ...func(*models.Snapshot) bool) ([]*models.Snapshot, error) {
	return ListSnapshotsBy(ctx, cli, basePath, SnapshotSelector{Filters: wrapPostFilters(filters)})
}

func ListSnapshotsBy(ctx context.Context, cli kv.MetaKV, basePath string, selector SnapshotSelector) ([]*models.Snapshot, error) {
	return ListObj2ModelsBySpec(ctx, cli, basePath, snapshotMetaKeySpec, selector.MetaKeyHints(), models.NewSnapshot, selector)
}
