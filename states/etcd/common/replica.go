package common

import (
	"context"
	"path"

	"github.com/milvus-io/birdwatcher/models"
	"github.com/milvus-io/birdwatcher/states/kv"
)

func ListReplicas(ctx context.Context, cli kv.MetaKV, basePath string, filters ...func(*models.Replica) bool) ([]*models.Replica, error) {
	prefix := path.Join(basePath, ReplicaPrefix) + "/"
	return ListObj2Models(ctx, cli, prefix, models.NewReplica, filters...)
}
