package common

import (
	"context"
	"errors"
	"path"

	"github.com/milvus-io/birdwatcher/models"
	"github.com/milvus-io/birdwatcher/proto/v2.0/querypb"
	querypbv2 "github.com/milvus-io/birdwatcher/proto/v2.2/querypb"
	"github.com/samber/lo"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// ListCollectionLoadedInfo returns collection loaded info with provided version.
func ListCollectionLoadedInfo(ctx context.Context, cli clientv3.KV, basePath string, version string) ([]*models.CollectionLoaded, error) {
	switch version {
	case models.LTEVersion2_1:
		prefix := path.Join(basePath, CollectionLoadPrefix)
		infos, paths, err := ListProtoObjects[querypb.CollectionInfo](ctx, cli, prefix)
		if err != nil {
			return nil, err
		}
		return lo.Map(infos, func(info querypb.CollectionInfo, idx int) *models.CollectionLoaded {
			return models.NewCollectionLoadedV2_1(&info, paths[idx])
		}), nil
	case models.GTEVersion2_2:
		prefix := path.Join(basePath, CollectionLoadPrefixV2)
		infos, paths, err := ListProtoObjects[querypbv2.CollectionLoadInfo](ctx, cli, prefix)
		if err != nil {
			return nil, err
		}
		return lo.Map(infos, func(info querypbv2.CollectionLoadInfo, idx int) *models.CollectionLoaded {
			return models.NewCollectionLoadedV2_2(&info, paths[idx])
		}), nil
	default:
		return nil, errors.New("version not supported")
	}
}
