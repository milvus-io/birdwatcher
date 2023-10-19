package common

import (
	"context"
	"fmt"
	"path"

	"github.com/milvus-io/birdwatcher/models"
	etcdpbv2 "github.com/milvus-io/birdwatcher/proto/v2.2/etcdpb"
	"github.com/milvus-io/birdwatcher/states/kv"
	"github.com/samber/lo"
)

const (
	PartitionPrefix = `root-coord/partitions/`
)

// ListCollectionPartitions returns partition list of collection.
func ListCollectionPartitions(ctx context.Context, cli kv.MetaKV, basePath string, collectionID int64) ([]*models.Partition, error) {
	prefix := path.Join(basePath, PartitionPrefix, fmt.Sprintf("%d", collectionID))

	infos, keys, err := ListProtoObjects[etcdpbv2.PartitionInfo](ctx, cli, prefix)

	if err != nil {
		return nil, err
	}

	return lo.Map(infos, func(info etcdpbv2.PartitionInfo, idx int) *models.Partition {
		return models.NewPartition(&info, keys[idx])
	}), nil
}
