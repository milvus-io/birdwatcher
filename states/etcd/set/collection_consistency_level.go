package set

import (
	"context"
	"fmt"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/birdwatcher/framework"
	"github.com/milvus-io/birdwatcher/states/etcd/common"
	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/etcdpb"
)

type CollectionConsistencyLevelParam struct {
	framework.ParamBase `use:"set collection consistency-level" desc:"set collection default consistency level"`
	CollectionID        int64  `name:"collection" default:"0" desc:"collection id to update"`
	ConsistencyLevel    string `name:"consistency-level" default:"" desc:"Consistency Level to set"`
	Run                 bool   `name:"run" default:"false"`
}

func (c *ComponentSet) CollectionConsistencyLevelCommand(ctx context.Context, p *CollectionConsistencyLevelParam) error {
	levelVal, ok := commonpb.ConsistencyLevel_value[p.ConsistencyLevel]
	if !ok {
		return errors.Newf(`consistency level string "%s" is not valid`, p.ConsistencyLevel)
	}

	consistencyLevel := commonpb.ConsistencyLevel(levelVal)
	collection, err := common.GetCollectionByIDVersion(ctx, c.client, c.basePath, p.CollectionID)
	if err != nil {
		return err
	}

	if collection.GetProto().ConsistencyLevel == consistencyLevel {
		fmt.Printf("collection consistency level is already %s\n", p.ConsistencyLevel)
		return nil
	}

	return common.UpdateCollection(ctx, c.client, collection.Key(), func(coll *etcdpb.CollectionInfo) {
		coll.ConsistencyLevel = consistencyLevel
	}, p.Run)
}
