package repair

import (
	"context"
	"fmt"

	"github.com/milvus-io/birdwatcher/framework"
	"github.com/milvus-io/birdwatcher/models"
	"github.com/milvus-io/birdwatcher/states/etcd/common"
	etcdversion "github.com/milvus-io/birdwatcher/states/etcd/version"
)

type CollectionLegacyDroppedParams struct {
	framework.ParamBase `use:"repair legacy-collection-remnant"`
	CollectionID        int64 `name:""`
	Run                 bool  `name:"run" default:"false" desc:"whether to remove legacy collection meta, default set to \"false\" to dry run"`
}

func (c *ComponentRepair) CollectionLegacyDroppedCommand(ctx context.Context, p *CollectionLegacyDroppedParams) error {
	collections, err := common.ListCollectionsVersion(ctx, c.client, c.basePath, etcdversion.GetVersion(), func(coll *models.Collection) bool {
		return coll.DBID == 0 && len(coll.Schema.Fields) == 0 && (p.CollectionID == 0 || p.CollectionID == coll.ID)
	})

	if err != nil {
		return err
	}

	var removed int
	for _, collection := range collections {
		fmt.Printf("collection [%d]%s is suspect of legacy collection remnant\n", collection.ID, collection.Schema.Name)
		if p.Run {
			key := collection.Key()
			fmt.Printf("start to remove remnant meta for %s, key:%s\n", collection.Schema.Name, key)
			err := c.client.Remove(ctx, collection.Key())
			if err != nil {
				fmt.Printf("failed to remove %s, error: %s\n", key, err.Error())
				continue
			}
			fmt.Println("Removal done!")
			removed++
		}
	}
	if len(collections) == 0 {
		fmt.Println("no suspect found")
	} else if removed > 0 {
		fmt.Println("Remnant meta removed, please restart rootcoord/mixtcord to check")
	}

	return nil
}
