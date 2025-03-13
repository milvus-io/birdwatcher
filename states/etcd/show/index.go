package show

import (
	"context"
	"fmt"

	"github.com/milvus-io/birdwatcher/framework"
	"github.com/milvus-io/birdwatcher/models"
	"github.com/milvus-io/birdwatcher/states/etcd/common"
	"github.com/milvus-io/birdwatcher/utils"
)

type IndexParam struct {
	framework.ParamBase `use:"show index" desc:"" alias:"indexes"`
	CollectionID        int64 `name:"collection" default:"0" desc:"collection id to list index on"`
}

// IndexCommand returns show index command.
func (c *ComponentShow) IndexCommand(ctx context.Context, p *IndexParam) error {
	fieldIndexes, err := common.ListIndex(ctx, c.client, c.metaPath, func(info *models.FieldIndex) bool {
		return p.CollectionID == 0 || p.CollectionID == info.GetProto().GetIndexInfo().GetCollectionID()
	})
	if err != nil {
		return err
	}

	for _, index := range fieldIndexes {
		printIndex(index)
	}
	return nil
}

func printIndex(info *models.FieldIndex) {
	index := info.GetProto()
	fmt.Println("==================================================================")
	fmt.Printf("Index ID: %d\tIndex Name: %s\tCollectionID: %d\tFieldID: %d\n", index.GetIndexInfo().GetIndexID(), index.GetIndexInfo().GetIndexName(), index.GetIndexInfo().GetCollectionID(), index.GetIndexInfo().GetFieldID())
	createTime, _ := utils.ParseTS(index.GetCreateTime())
	fmt.Printf("Create Time: %s\tDeleted: %t\n", createTime.Format(tsPrintFormat), index.GetDeleted())
	indexParams := index.GetIndexInfo().GetIndexParams()
	fmt.Printf("Index Type: %s\tMetric Type: %s\n",
		common.GetKVPair(indexParams, "index_type"),
		common.GetKVPair(indexParams, "metric_type"),
	)
	fmt.Printf("ParamsJSON : %s\n", common.GetKVPair(index.GetIndexInfo().GetUserIndexParams(), "params"))
	// print detail param in meta
	fmt.Println("Index.IndexParams:")
	for _, kv := range indexParams {
		fmt.Printf("\t%s: %s\n", kv.GetKey(), kv.GetValue())
	}
	fmt.Println("Index.UserParams")
	for _, kv := range index.GetIndexInfo().GetUserIndexParams() {
		fmt.Printf("\t%s: %s\n", kv.GetKey(), kv.GetValue())
	}
	fmt.Println("==================================================================")
}
