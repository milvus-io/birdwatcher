package common

import (
	"context"
	"path"

	"github.com/milvus-io/birdwatcher/models"
	"github.com/milvus-io/birdwatcher/states/kv"
)

// ListCollectionLoadedInfo returns collection loaded info with provided version.
func ListCollectionLoadedInfo(ctx context.Context, cli kv.MetaKV, basePath string, filters ...func(cl *models.CollectionLoaded) bool) ([]*models.CollectionLoaded, error) {
	prefix := path.Join(basePath, CollectionLoadPrefixV2)
	return ListObj2Models(ctx, cli, prefix, models.NewCollectionLoaded, filters...)
}
