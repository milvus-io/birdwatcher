package show

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/milvus-io/birdwatcher/framework"
	"github.com/milvus-io/birdwatcher/models"
	"github.com/milvus-io/birdwatcher/states/etcd/common"
	etcdversion "github.com/milvus-io/birdwatcher/states/etcd/version"
	"github.com/milvus-io/birdwatcher/utils"
)

type CollectionHistoryParam struct {
	framework.ParamBase `use:"show collection-history" desc:"display collection change history"`
	CollectionID        int64 `name:"id" default:"0" desc:"collection id to display" form:"id"`
}

// CollectionHistoryCommand returns sub command for showCmd.
// show collection-history [options...]
func (c *ComponentShow) CollectionHistoryCommand(ctx context.Context, p *CollectionHistoryParam) (*CollectionHistory, error) {
	if p.CollectionID == 0 {
		return nil, errors.New("collection id not provided")
	}

	// fetch current for now
	collection, err := common.GetCollectionByIDVersion(ctx, c.client, c.basePath, etcdversion.GetVersion(), p.CollectionID)
	if err != nil {
		switch {
		case errors.Is(err, common.ErrCollectionDropped):
			return nil, fmt.Errorf("[Current] collection id %d already marked with Tombstone", p.CollectionID)
		case errors.Is(err, common.ErrCollectionNotFound):
			return nil, fmt.Errorf("[Current] collection id %d not found", p.CollectionID)
		default:
			return nil, err
		}
	}

	result := &CollectionHistory{
		Collection: collection,
	}
	// fetch history
	items, err := common.ListCollectionHistoryWithDB(ctx, c.client, c.basePath, etcdversion.GetVersion(), collection.DBID, p.CollectionID)
	if err != nil {
		return nil, err
	}

	result.HistoryItems = items
	return result, nil
}

type CollectionHistory struct {
	Collection   *models.Collection
	HistoryItems []*models.CollectionHistory
}

func (rs *CollectionHistory) PrintAs(format framework.Format) string {
	switch format {
	case framework.FormatDefault, framework.FormatPlain:
		sb := &strings.Builder{}
		printCollection(sb, rs.Collection)
		for _, item := range rs.HistoryItems {
			t, _ := utils.ParseTS(item.Ts)
			fmt.Fprintln(sb, "Snapshot at", t.Format("2006-01-02 15:04:05"))
			if item.Dropped {
				fmt.Fprintln(sb, "Collection Dropped")
				continue
			}
			printCollection(sb, &item.Collection)
		}
	default:
	}
	return ""
}

func (rs *CollectionHistory) Entities() any {
	return rs
}
