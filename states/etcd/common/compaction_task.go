package common

import (
	"context"
	"path"

	"github.com/samber/lo"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/milvus-io/birdwatcher/models"
	"github.com/milvus-io/birdwatcher/proto/v2.2/datapb"
)

// ListCompactionTask returns compaction task information as provided filters.
func ListCompactionTask(ctx context.Context, cli clientv3.KV, basePath string, filters ...func(task *models.CompactionTask) bool) ([]*models.CompactionTask, error) {
	prefixes := []string{
		path.Join(basePath, CompactionTaskPrefix),
	}
	var result []*models.CompactionTask

	for _, prefix := range prefixes {
		compactions, keys, err := ListProtoObjectsAdv[datapb.CompactionTask](ctx, cli, prefix, func(_ string, value []byte) bool {
			return true
		})
		if err != nil {
			return nil, err
		}
		result = append(result, lo.FilterMap(compactions, func(info datapb.CompactionTask, idx int) (*models.CompactionTask, bool) {
			c := models.NewCompactionTask(&info, keys[idx])
			for _, filter := range filters {
				if !filter(c) {
					return nil, false
				}
			}
			return c, true
		})...)
	}
	return result, nil
}
