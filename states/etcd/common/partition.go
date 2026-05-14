package common

import (
	"context"
	"path"

	"github.com/milvus-io/birdwatcher/models"
	"github.com/milvus-io/birdwatcher/states/kv"
)

var partitionMetaKeySpec = MetaKeySpec{
	Prefix: path.Join(RCPrefix, PartitionPrefix),
	Parts:  []MetaKeyPart{KeyCollectionID, KeyPartitionID},
}

type PartitionSelector struct {
	CollectionID  int64
	PartitionID   int64
	PartitionName string
	Filters       []PostFilter[models.Partition]
}

func (s PartitionSelector) MetaKeyHints() MetaKeyHints {
	return NewMetaKeyHints().
		WithInt64(KeyCollectionID, s.CollectionID).
		WithInt64(KeyPartitionID, s.PartitionID)
}

func (s PartitionSelector) Match(partition *models.Partition) bool {
	proto := partition.GetProto()
	if s.CollectionID > 0 && proto.GetCollectionId() != s.CollectionID {
		return false
	}
	if s.PartitionID > 0 && proto.GetPartitionID() != s.PartitionID {
		return false
	}
	if s.PartitionName != "" && proto.GetPartitionName() != s.PartitionName {
		return false
	}
	for _, filter := range s.Filters {
		if !filter.Match(partition) {
			return false
		}
	}
	return true
}

// ListCollectionPartitions returns partition list of collection.
func ListCollectionPartitions(ctx context.Context, cli kv.MetaKV, basePath string, collectionID int64) ([]*models.Partition, error) {
	return ListPartitionsBy(ctx, cli, basePath, PartitionSelector{CollectionID: collectionID})
}

// ListPartitions returns all partition info which meets the filters.
func ListPartitions(ctx context.Context, cli kv.MetaKV, basePath string, filters ...func(*models.Partition) bool) ([]*models.Partition, error) {
	return ListPartitionsBy(ctx, cli, basePath, PartitionSelector{Filters: wrapPostFilters(filters)})
}

func ListPartitionsBy(ctx context.Context, cli kv.MetaKV, basePath string, selector PartitionSelector) ([]*models.Partition, error) {
	return ListObj2ModelsBySpec(ctx, cli, basePath, partitionMetaKeySpec, selector.MetaKeyHints(), models.NewPartition, selector)
}
