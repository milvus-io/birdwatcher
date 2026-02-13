package repair

import (
	"context"
	"fmt"

	"github.com/milvus-io/birdwatcher/framework"
	"github.com/milvus-io/birdwatcher/models"
	"github.com/milvus-io/birdwatcher/states/etcd/common"
)

type CollectionLegacyDroppedParams struct {
	framework.ExecutionParam `use:"repair legacy-collection-remnant"`
	CollectionID             int64 `name:"collection" default:"0" desc:"collection id to repair"`
}

func (c *ComponentRepair) CollectionLegacyDroppedCommand(ctx context.Context, p *CollectionLegacyDroppedParams) error {
	collections, err := common.ListCollections(ctx, c.client, c.basePath, func(info *models.Collection) bool {
		coll := info.GetProto()
		return coll.DbId == 0 && len(coll.Schema.Fields) == 0 && (p.CollectionID == 0 || p.CollectionID == coll.ID)
	})
	if err != nil {
		return err
	}

	var removed int
	for _, info := range collections {
		collection := info.GetProto()
		fmt.Printf("collection [%d]%s is suspect of legacy collection remnant\n", collection.ID, collection.Schema.Name)
		if p.Run {
			key := info.Key()
			fmt.Printf("start to remove remnant meta for %s, key:%s\n", collection.Schema.Name, key)
			err := c.client.Remove(ctx, info.Key())
			if err != nil {
				fmt.Printf("failed to remove %s, error: %s\n", key, err.Error())
				continue
			}
			historyCollections, err := common.ListCollectionHistory(ctx, c.client, c.basePath, collection.DbId, collection.ID)
			if err != nil {
				fmt.Println("failed to list collection history", err.Error())
			} else {
				for _, hc := range historyCollections {
					c.client.Remove(ctx, hc.Key())
				}
			}
			fmt.Println("Removal done!")
			removed++
		}
	}
	if len(collections) == 0 {
		// try to delete legacy history when collection removed
		if p.CollectionID != 0 {
			historyCollections, err := common.ListCollectionHistory(ctx, c.client, c.basePath, 0, p.CollectionID)
			if err != nil {
				fmt.Println("failed to list legacy collection history")
				return err
			}
			for _, hc := range historyCollections {
				if p.Run {
					c.client.Remove(ctx, hc.Key())
				} else {
					fmt.Println("legacy collection history found:", hc.Key())
				}
			}
		}
		fmt.Println("no suspect found")
	} else if removed > 0 {
		fmt.Println("Remnant meta removed, please restart rootcoord/mixtcord to check")
	}

	return nil
}
