package repair

import (
	"context"
	"fmt"
	"path"

	"github.com/milvus-io/birdwatcher/framework"
	indexpbv2 "github.com/milvus-io/birdwatcher/proto/v2.2/indexpb"
	"github.com/milvus-io/birdwatcher/states/etcd/common"
	etcdversion "github.com/milvus-io/birdwatcher/states/etcd/version"
)

type IndexDuplicateParam struct {
	framework.ParamBase `use:"repair index-dup"`
	RunFix              bool `name:"runFix" default:"false" desc:"whether to fix the duplicate index"`
}

func (c *ComponentRepair) RepairIndexRepairCommand(ctx context.Context, p *IndexDuplicateParam) error {
	collections, err := common.ListCollectionsVersion(ctx, c.client, c.basePath, etcdversion.GetVersion())
	if err != nil {
		return err
	}

	for _, collection := range collections {
		fields := make(map[int64]struct{})
		fieldIndexes, err := c.listIndexMetaV2(ctx)
		if err != nil {
			fmt.Println(err.Error())
			return err
		}
		for _, fieldIndex := range fieldIndexes {
			_, ok := fields[fieldIndex.IndexInfo.FieldID]
			if ok {
				fmt.Printf("duplicate index found, collectionID = %d, fieldID = %d\n", collection.ID, fieldIndex.IndexInfo.FieldID)
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
