package common

import (
	"context"
	"errors"
	"path"

	"github.com/milvus-io/birdwatcher/models"
	"github.com/milvus-io/birdwatcher/proto/v2.0/querypb"
	querypbv2 "github.com/milvus-io/birdwatcher/proto/v2.2/querypb"
	"github.com/milvus-io/birdwatcher/states/kv"
	"github.com/samber/lo"
)

// ListCollectionLoadedInfo returns collection loaded info with provided version.
func ListCollectionLoadedInfo(ctx context.Context, cli kv.MetaKV, basePath string, version string, filters ...func(cl *models.CollectionLoaded) bool) ([]*models.CollectionLoaded, error) {
	switch version {
	case models.LTEVersion2_1:
		prefix := path.Join(basePath, CollectionLoadPrefix)
		infos, paths, err := ListProtoObjects(ctx, cli, prefix, func(info *querypb.CollectionInfo) bool {
			cl := models.NewCollectionLoadedV2_1(info, "")
			for _, filter := range filters {
				if !filter(cl) {
					return false
				}
			}
			return true
		})
		if err != nil {
			return nil, err
		}
		return lo.Map(infos, func(info querypb.CollectionInfo, idx int) *models.CollectionLoaded {
			return models.NewCollectionLoadedV2_1(&info, paths[idx])
		}), nil
	case models.GTEVersion2_2:
		prefix := path.Join(basePath, CollectionLoadPrefixV2)
		infos, paths, err := ListProtoObjects(ctx, cli, prefix, func(info *querypbv2.CollectionLoadInfo) bool {
			cl := models.NewCollectionLoadedV2_2(info, "")
			for _, filter := range filters {
				if !filter(cl) {
					return false
				}
			}
			return true
		})
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
