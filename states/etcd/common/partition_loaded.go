package common

import (
	"context"
	"path"

	"github.com/milvus-io/birdwatcher/models"
	"github.com/milvus-io/birdwatcher/states/kv"
)

func ListPartitionLoadedInfo(ctx context.Context, cli kv.MetaKV, basePath string, filters ...func(*models.PartitionLoaded) bool) ([]*models.PartitionLoaded, error) {
	prefix := path.Join(basePath, PartitionLoadedPrefix)
	return ListObj2Models(ctx, cli, prefix, models.NewPartitionLoaded, filters...)
}
