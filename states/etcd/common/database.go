package common

import (
	"context"
	"path"

	"github.com/samber/lo"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/milvus-io/birdwatcher/models"
	"github.com/milvus-io/birdwatcher/proto/v2.2/etcdpb"
)

const (
	// DataBaseMetaPrefix is prefix for rootcoord database meta.
	DataBaseMetaPrefix = `root-coord/database/db-info`
)

// ListDatabase returns all database info from etcd meta converted to models.
func ListDatabase(ctx context.Context, cli clientv3.KV, basePath string) ([]*models.Database, error) {
	prefix := path.Join(basePath, DataBaseMetaPrefix)
	dbs, keys, err := ListProtoObjects(ctx, cli, prefix, func(*etcdpb.DatabaseInfo) bool {
		return true
	})
	if err != nil {
		return nil, err
	}

	result := lo.Map(dbs, func(db etcdpb.DatabaseInfo, idx int) *models.Database {
		return models.NewDatabase(&db, keys[idx])
	})
	return result, nil
}
