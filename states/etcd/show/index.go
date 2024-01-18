package show

import (
	"context"
	"fmt"
	"path"

	"github.com/milvus-io/birdwatcher/framework"
	"github.com/milvus-io/birdwatcher/proto/v2.0/etcdpb"
	indexpbv2 "github.com/milvus-io/birdwatcher/proto/v2.2/indexpb"
	"github.com/milvus-io/birdwatcher/states/etcd/common"
	"github.com/milvus-io/birdwatcher/utils"
)

type IndexParam struct {
	framework.ParamBase `use:"show index" desc:"" alias:"indexes"`
	CollectionID        int64 `name:"collection" default:"0" desc:"collection id to list index on"`
}

// IndexCommand returns show index command.
func (c *ComponentShow) IndexCommand(ctx context.Context, p *IndexParam) {
	fmt.Println("*************2.1.x***************")
	// v2.0+
	meta, err := c.listIndexMeta(ctx)
	if err != nil {
		fmt.Println(err.Error())
		return
	}

	for _, m := range meta {
		printIndex(m)
	}

	fmt.Println("*************2.2.x***************")
	// v2.2+
	fieldIndexes, err := c.listIndexMetaV2(ctx)
	if err != nil {
		fmt.Println(err.Error())
		return
	}

	for _, index := range fieldIndexes {
		if p.CollectionID == 0 || p.CollectionID == index.IndexInfo.GetCollectionID() {
			printIndexV2(index)
		}
	}
}

type IndexInfoV1 struct {
	info         etcdpb.IndexInfo
	collectionID int64
}

func (c *ComponentShow) listIndexMeta(ctx context.Context) ([]IndexInfoV1, error) {
	prefix := path.Join(c.basePath, "root-coord/index")
	indexes, keys, err := common.ListProtoObjects[etcdpb.IndexInfo](ctx, c.client, prefix)
	result := make([]IndexInfoV1, 0, len(indexes))
	for idx, info := range indexes {
		collectionID, err := common.PathPartInt64(keys[idx], -2)
		if err != nil {
			continue
		}
		result = append(result, IndexInfoV1{
			info:         info,
			collectionID: collectionID,
		})
	}

	return result, err
}

func (c *ComponentShow) listIndexMetaV2(ctx context.Context) ([]indexpbv2.FieldIndex, error) {
	indexes, _, err := common.ListProtoObjects[indexpbv2.FieldIndex](ctx, c.client, path.Join(c.basePath, "field-index"))
	return indexes, err
}

func printIndex(index IndexInfoV1) {
	fmt.Println("==================================================================")
	fmt.Printf("Index ID: %d\tIndex Name: %s\tCollectionID: %d\n", index.info.GetIndexID(), index.info.GetIndexName(), index.collectionID)
	indexParams := index.info.GetIndexParams()
	fmt.Printf("Index Type: %s\tMetric Type: %s\n",
		common.GetKVPair(indexParams, "index_type"),
		common.GetKVPair(indexParams, "metric_type"),
	)
	fmt.Printf("Index Params: %s\n", common.GetKVPair(index.info.GetIndexParams(), "params"))
	for _ , v := range indexParams {
		fmt.Printf("%s:%s", v.GetKey(), v.GetValue())
	}
	fmt.Println("==================================================================")
}

func printIndexV2(index indexpbv2.FieldIndex) {
	fmt.Println("==================================================================")
	fmt.Printf("Index ID: %d\tIndex Name: %s\tCollectionID: %d\tFieldID: %d\n", index.GetIndexInfo().GetIndexID(), index.GetIndexInfo().GetIndexName(), index.GetIndexInfo().GetCollectionID(), index.GetIndexInfo().GetFieldID())
	createTime, _ := utils.ParseTS(index.GetCreateTime())
	fmt.Printf("Create Time: %s\tDeleted: %t\n", createTime.Format(tsPrintFormat), index.GetDeleted())
	indexParams := index.GetIndexInfo().GetIndexParams()
	fmt.Printf("Index Type: %s\tMetric Type: %s\n",
		common.GetKVPair(indexParams, "index_type"),
		common.GetKVPair(indexParams, "metric_type"),
	)
	fmt.Printf("Index Params: %s\n", common.GetKVPair(index.GetIndexInfo().GetUserIndexParams(), "params"))
	for _ , v := range indexParams {
		fmt.Printf("%s:%s", v.GetKey(), v.GetValue())
	}
	fmt.Println("==================================================================")
}
