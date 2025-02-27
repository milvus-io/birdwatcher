package common

import (
	"context"
	"path"

	"github.com/milvus-io/birdwatcher/models"
	"github.com/milvus-io/birdwatcher/states/kv"
)

// ListCollectionPartitions returns partition list of collection.
func ListResourceGroups(ctx context.Context, cli kv.MetaKV, basePath string, filters ...func(rg *models.ResourceGroup) bool) ([]*models.ResourceGroup, error) {
	prefix := path.Join(basePath, ResourceGroupPrefix) + "/"

	return ListObj2Models(ctx, cli, prefix, models.NewResourceGroup, filters...)
}
