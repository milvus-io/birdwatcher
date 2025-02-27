package common

import (
	"context"
	"path"

	"github.com/milvus-io/birdwatcher/models"
	"github.com/milvus-io/birdwatcher/states/kv"
)

const (
	// DataBaseMetaPrefix is prefix for rootcoord database meta.
	DataBaseMetaPrefix = `root-coord/database/db-info`
)

// ListDatabase returns all database info from etcd meta converted to models.
func ListDatabase(ctx context.Context, cli kv.MetaKV, basePath string, filters ...func(db *models.Database) bool) ([]*models.Database, error) {
	prefix := path.Join(basePath, RCPrefix, DBPrefix, DBInfoPrefix) + "/"
	return ListObj2Models(ctx, cli, prefix, models.NewDatabase, filters...)
}
