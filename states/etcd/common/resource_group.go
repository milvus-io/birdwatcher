package common

import (
	"context"
	"path"

	"github.com/milvus-io/birdwatcher/models"
	querypbv2 "github.com/milvus-io/birdwatcher/proto/v2.2/querypb"
	"github.com/samber/lo"
	clientv3 "go.etcd.io/etcd/client/v3"
)

const (
	ResourceGroupPrefix = `queryCoord-ResourceGroup/`
)

// ListCollectionPartitions returns partition list of collection.
func ListResourceGroups(ctx context.Context, cli clientv3.KV, basePath string, filters ...func(rg *models.ResourceGroup) bool) ([]*models.ResourceGroup, error) {
	prefix := path.Join(basePath, ResourceGroupPrefix)

	infos, keys, err := ListProtoObjects[querypbv2.ResourceGroup](ctx, cli, prefix)
	if err != nil {
		return nil, err
	}

	rgs := lo.FilterMap(infos, func(info querypbv2.ResourceGroup, idx int) (*models.ResourceGroup, bool) {
		rg := models.NewResourceGroup(info, keys[idx])
		for _, filter := range filters {
			if !filter(rg) {
				return nil, false
			}
		}
		return rg, true
	})

	return rgs, nil
}
