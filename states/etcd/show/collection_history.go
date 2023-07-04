package show

import (
	"context"
	"errors"
	"fmt"

	"github.com/milvus-io/birdwatcher/framework"
	"github.com/milvus-io/birdwatcher/states/etcd/common"
	etcdversion "github.com/milvus-io/birdwatcher/states/etcd/version"
	"github.com/milvus-io/birdwatcher/utils"
)

type CollectionHistoryParam struct {
	framework.ParamBase `use:"show collection-history" desc:"display collection change history"`
	CollectionID        int64 `name:"id" default:"0" desc:"collection id to display"`
}

// CollectionHistoryCommand returns sub command for showCmd.
// show collection-history [options...]
func (c *ComponentShow) CollectionHistoryCommand(ctx context.Context, p *CollectionHistoryParam) {
	if p.CollectionID == 0 {
		fmt.Println("collection id not provided")
		return
	}

	// fetch current for now
	collection, err := common.GetCollectionByIDVersion(ctx, c.client, c.basePath, etcdversion.GetVersion(), p.CollectionID)
	if err != nil {
		switch {
		case errors.Is(err, common.ErrCollectionDropped):
			fmt.Printf("[Current] collection id %d already marked with Tombstone\n", p.CollectionID)
		case errors.Is(err, common.ErrCollectionNotFound):
			fmt.Printf("[Current] collection id %d not found\n", p.CollectionID)
			return
		default:
			fmt.Println("failed to get current collection state:", err.Error())
			return
		}
	}
	printCollection(collection)
	// fetch history
	items, err := common.ListCollectionHistory(ctx, c.client, c.basePath, etcdversion.GetVersion(), p.CollectionID)
	if err != nil {
		fmt.Println("failed to list history", err.Error())
		return
	}

	for _, item := range items {
		t, _ := utils.ParseTS(item.Ts)
		fmt.Println("Snapshot at", t.Format("2006-01-02 15:04:05"))
		if item.Dropped {
			fmt.Println("Collection Dropped")
			continue
		}
		printCollection(&item.Collection)
	}
}
