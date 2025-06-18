package common

import (
	"context"
	"path"

	"github.com/milvus-io/birdwatcher/models"
	"github.com/milvus-io/birdwatcher/states/kv"
	"github.com/milvus-io/milvus/pkg/v2/proto/indexpb"
)

// ListStatsTask returns stats task information as provided filters.
func ListStatsTask(ctx context.Context, cli kv.MetaKV, basePath string, filters ...func(task *models.StatsTask) bool) ([]*models.StatsTask, error) {
	prefix := path.Join(basePath, DCPrefix, StatsTaskPrefix) + "/"
	return ListObj2Models(ctx, cli, prefix, models.NewProtoWrapper[*indexpb.StatsTask], filters...)
}
