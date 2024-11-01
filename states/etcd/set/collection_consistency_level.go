package set

import (
	"context"
	"fmt"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/birdwatcher/framework"
	"github.com/milvus-io/birdwatcher/models"
	commonpbv2 "github.com/milvus-io/birdwatcher/proto/v2.2/commonpb"
	etcdpbv2 "github.com/milvus-io/birdwatcher/proto/v2.2/etcdpb"
	"github.com/milvus-io/birdwatcher/states/etcd/common"
	etcdversion "github.com/milvus-io/birdwatcher/states/etcd/version"
)

type CollectionConsistencyLevelParam struct {
	framework.ParamBase `use:"set collection consistency-level" desc:"set collection default consistency level"`
	CollectionID        int64  `name:"collection" default:"0" desc:"collection id to update"`
	ConsistencyLevel    string `name:"consistency-level" default:"" desc:"Consistency Level to set"`
	Run                 bool   `name:"run" default:"false"`
}

func (c *ComponentSet) CollectionConsistencyLevelCommand(ctx context.Context, p *CollectionConsistencyLevelParam) error {
	levelVal, ok := commonpbv2.ConsistencyLevel_value[p.ConsistencyLevel]
	if !ok {
		return errors.Newf(`consistency level string "%s" is not valid`, p.ConsistencyLevel)
	}

	consistencyLevel := commonpbv2.ConsistencyLevel(levelVal)
	collection, err := common.GetCollectionByIDVersion(ctx, c.client, c.basePath, etcdversion.GetVersion(), p.CollectionID)
	if err != nil {
		return err
	}

	if collection.ConsistencyLevel == models.ConsistencyLevel(consistencyLevel) {
		fmt.Printf("collection consistency level is already %s\n", p.ConsistencyLevel)
		return nil
	}

	return common.UpdateCollection(ctx, c.client, collection.Key(), func(coll *etcdpbv2.CollectionInfo) {
		coll.ConsistencyLevel = consistencyLevel
	}, p.Run)
}
