package repair

import (
	"context"
	"fmt"
	"path"

	"github.com/samber/lo"

	"github.com/milvus-io/birdwatcher/framework"
	indexpbv2 "github.com/milvus-io/birdwatcher/proto/v2.2/indexpb"
	"github.com/milvus-io/birdwatcher/states/etcd/common"
	etcdversion "github.com/milvus-io/birdwatcher/states/etcd/version"
)

type IndexDuplicateParam struct {
	framework.ParamBase `use:"repair index-dup"`
	CollectionID        int64 `name:"collection" default:"0" desc:"specify collection id to process"`
	RunFix              bool  `name:"runFix" default:"false" desc:"whether to fix the duplicate index"`
}

func (c *ComponentRepair) RepairIndexRepairCommand(ctx context.Context, p *IndexDuplicateParam) error {
	collections, err := common.ListCollectionsVersion(ctx, c.client, c.basePath, etcdversion.GetVersion())
	if err != nil {
		return err
	}

	fieldIndexes, err := c.listIndexMetaV2(ctx)
	collIndexs := lo.GroupBy(fieldIndexes, func(index indexpbv2.FieldIndex) int64 {
		return index.IndexInfo.CollectionID
	})

	for _, collection := range collections {
		if p.CollectionID > 0 && p.CollectionID != collection.ID {
			continue
		}
		indexes := collIndexs[collection.ID]
		fields := make(map[int64]struct{})

		if err != nil {
			fmt.Println(err.Error())
			return err
		}
		for _, fieldIndex := range indexes {
			if fieldIndex.Deleted {
				continue
			}
			_, ok := fields[fieldIndex.IndexInfo.FieldID]
			if ok {
				fmt.Printf("duplicate index found, collectionID = %d, fieldID = %d\n", collection.ID, fieldIndex.IndexInfo.FieldID)
				if p.RunFix {
					markFieldIndexDeleted(ctx, c.client, c.basePath, fieldIndex)
				}
				continue
			}
			fields[fieldIndex.IndexInfo.FieldID] = struct{}{}
		}
	}
	return nil
}

func (c *ComponentRepair) listIndexMetaV2(ctx context.Context) ([]indexpbv2.FieldIndex, error) {
	indexes, _, err := common.ListProtoObjects[indexpbv2.FieldIndex](ctx, c.client, path.Join(c.basePath, "field-index"))
	return indexes, err
}
