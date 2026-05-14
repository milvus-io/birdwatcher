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

var databaseMetaKeySpec = MetaKeySpec{
	Prefix: path.Join(RCPrefix, DBPrefix, DBInfoPrefix),
	Parts:  []MetaKeyPart{KeyDatabaseID},
}

type DatabaseSelector struct {
	DatabaseID   int64
	DatabaseName string
	Filters      []PostFilter[models.Database]
}

func (s DatabaseSelector) MetaKeyHints() MetaKeyHints {
	return NewMetaKeyHints().WithInt64(KeyDatabaseID, s.DatabaseID)
}

func (s DatabaseSelector) Match(db *models.Database) bool {
	proto := db.GetProto()
	if s.DatabaseID > 0 && proto.GetId() != s.DatabaseID {
		return false
	}
	if s.DatabaseName != "" && proto.GetName() != s.DatabaseName {
		return false
	}
	for _, filter := range s.Filters {
		if !filter.Match(db) {
			return false
		}
	}
	return true
}

// ListDatabase returns all database info from etcd meta converted to models.
func ListDatabase(ctx context.Context, cli kv.MetaKV, basePath string, filters ...func(db *models.Database) bool) ([]*models.Database, error) {
	return ListDatabaseBy(ctx, cli, basePath, DatabaseSelector{Filters: wrapPostFilters(filters)})
}

func ListDatabaseBy(ctx context.Context, cli kv.MetaKV, basePath string, selector DatabaseSelector) ([]*models.Database, error) {
	return ListObj2ModelsBySpec(ctx, cli, basePath, databaseMetaKeySpec, selector.MetaKeyHints(), models.NewDatabase, selector)
}
