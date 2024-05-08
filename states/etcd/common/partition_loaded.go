package common

import (
	"context"
	"errors"
	"path"

	"github.com/milvus-io/birdwatcher/models"
	querypbv2 "github.com/milvus-io/birdwatcher/proto/v2.2/querypb"
	"github.com/milvus-io/birdwatcher/states/kv"
	"github.com/samber/lo"
)

func ListPartitionLoadedInfo(ctx context.Context, cli kv.MetaKV, basePath string, version string, filters ...func(*models.PartitionLoaded) bool) ([]*models.PartitionLoaded, error) {
	switch version {
	case models.GTEVersion2_2:
		prefix := path.Join(basePath, PartitionLoadedPrefix)
		infos, paths, err := ListProtoObjects(ctx, cli, prefix, func(info *querypbv2.PartitionLoadInfo) bool {
			pl := models.NewPartitionLoaded(info, "")
			for _, filter := range filters {
				if !filter(pl) {
					return false
				}
			}
			return true
		})
		if err != nil {
			return nil, err
		}

		return lo.Map(infos, func(info querypbv2.PartitionLoadInfo, idx int) *models.PartitionLoaded {
			return models.NewPartitionLoaded(&info, paths[idx])
		}), nil
	default:
		return nil, errors.New("version not supported")
	}
}
