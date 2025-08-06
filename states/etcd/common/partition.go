package common

import (
	"context"
	"fmt"
	"path"

	"github.com/milvus-io/birdwatcher/models"
	"github.com/milvus-io/birdwatcher/states/kv"
)

// ListCollectionPartitions returns partition list of collection.
func ListCollectionPartitions(ctx context.Context, cli kv.MetaKV, basePath string, collectionID int64) ([]*models.Partition, error) {
	prefix := path.Join(basePath, RCPrefix, PartitionPrefix, fmt.Sprintf("%d", collectionID)) + "/"

	return ListObj2Models(ctx, cli, prefix, models.NewPartition)
}

// ListPartitions returns all partition info which meets the filters.
func ListPartitions(ctx context.Context, cli kv.MetaKV, basePath string, filters ...func(*models.Partition) bool) ([]*models.Partition, error) {
	prefix := path.Join(basePath, RCPrefix, PartitionPrefix) + "/"
	return ListObj2Models(ctx, cli, prefix, models.NewPartition, filters...)
}
