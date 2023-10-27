package common

import (
	"context"
	"errors"
	"path"

	"github.com/milvus-io/birdwatcher/models"
	etcdpbv2 "github.com/milvus-io/birdwatcher/proto/v2.2/etcdpb"
	"github.com/milvus-io/birdwatcher/states/kv"
	"github.com/samber/lo"
)

const (
	// AliasPrefixBefore210 is the legacy meta prefix for milvus before 2.2.0
	AliasPrefixBefore210 = `root-coord/collection-alias`

	// AliasPrefixWithoutDB is the meta prefix for alias before database.
	AliasPrefixWithoutDB = `root-coord/aliases`

	// AliasPrefixDB iis the meta prefix for alias with database feature
	AliasPrefixDB = `root-coord/database/alias`
)

func ListAliasVersion(ctx context.Context, cli kv.MetaKV, basePath string, version string, filters ...func(*models.Alias) bool) ([]*models.Alias, error) {
	prefixes := []string{
		path.Join(basePath, AliasPrefixWithoutDB),
		path.Join(basePath, AliasPrefixDB),
	}

	switch version {
	case models.GTEVersion2_2:
		var result []*models.Alias
		for _, prefix := range prefixes {
			infos, keys, err := ListProtoObjects[etcdpbv2.AliasInfo](ctx, cli, prefix)
			if err != nil {
				return nil, err
			}

			result = append(result, lo.FilterMap(infos, func(info etcdpbv2.AliasInfo, idx int) (*models.Alias, bool) {
				value := models.NewAlias(&info, keys[idx])
				for _, filter := range filters {
					if !filter(value) {
						return nil, false
					}
				}
				return value, true
			})...)
		}

		return result, nil
	default:
		return nil, errors.New("not supported version")
	}
}
