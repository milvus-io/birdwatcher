package common

import (
	"context"
	"path"

	"github.com/milvus-io/birdwatcher/models"
	"github.com/milvus-io/birdwatcher/states/kv"
)

const (
	// AliasPrefixBefore210 is the legacy meta prefix for milvus before 2.2.0
	AliasPrefixBefore210 = `root-coord/collection-alias`

	// AliasPrefixWithoutDB is the meta prefix for alias before database.
	AliasPrefixWithoutDB = `root-coord/aliases`

	// AliasPrefixDB iis the meta prefix for alias with database feature
	AliasPrefixDB = `root-coord/database/alias`
)

func ListAlias(ctx context.Context, cli kv.MetaKV, basePath string, version string, filters ...func(*models.Alias) bool) ([]*models.Alias, error) {
	var result []*models.Alias
	prefixes := []string{
		path.Join(basePath, AliasPrefixWithoutDB),
		path.Join(basePath, AliasPrefixDB),
	}
	for _, prefix := range prefixes {
		aliases, err := ListObj2Models(ctx, cli, prefix, models.NewAlias, filters...)
		if err != nil {
			return nil, err
		}
		result = append(result, aliases...)
	}
	return result, nil
}
