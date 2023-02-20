package show

import (
	"context"
	"fmt"
	"path"
	"time"

	"github.com/milvus-io/birdwatcher/proto/v2.0/etcdpb"
	indexpbv2 "github.com/milvus-io/birdwatcher/proto/v2.2/indexpb"
	"github.com/milvus-io/birdwatcher/states/etcd/common"
	"github.com/milvus-io/birdwatcher/utils"
	"github.com/spf13/cobra"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// IndexCommand returns show index command.
func IndexCommand(cli clientv3.KV, basePath string) *cobra.Command {
	cmd := &cobra.Command{
		Use:     "index",
		Aliases: []string{"indexes"},
		Run: func(cmd *cobra.Command, args []string) {

			fmt.Println("*************2.1.x***************")
			// v2.0+
			meta, err := listIndexMeta(cli, basePath)
			if err != nil {
				fmt.Println(err.Error())
				return
			}

			for _, m := range meta {
				printIndex(m)
			}

			fmt.Println("*************2.2.x***************")
			// v2.2+
			fieldIndexes, err := listIndexMetaV2(cli, basePath)
			if err != nil {
				fmt.Println(err.Error())
				return
			}

			for _, index := range fieldIndexes {
				printIndexV2(index)
			}
		},
	}
	return cmd
}

type IndexInfoV1 struct {
	info         etcdpb.IndexInfo
	collectionID int64
}

func listIndexMeta(cli clientv3.KV, basePath string) ([]IndexInfoV1, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
	defer cancel()

	prefix := path.Join(basePath, "root-coord/index")
	indexes, keys, err := common.ListProtoObjects[etcdpb.IndexInfo](ctx, cli, prefix)
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

func listIndexMetaV2(cli clientv3.KV, basePath string) ([]indexpbv2.FieldIndex, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
	defer cancel()
	indexes, _, err := common.ListProtoObjects[indexpbv2.FieldIndex](ctx, cli, path.Join(basePath, "field-index"))
	return indexes, err
}

func printIndex(index IndexInfoV1) {
	fmt.Println("==================================================================")
	fmt.Printf("Index ID: %d\tIndex Name: %s\tCollectionID:%d\n", index.info.GetIndexID(), index.info.GetIndexName(), index.collectionID)
	indexParams := index.info.GetIndexParams()
	fmt.Printf("Index Type: %s\tMetric Type: %s\n",
		common.GetKVPair(indexParams, "index_type"),
		common.GetKVPair(indexParams, "metric_type"),
	)
	fmt.Printf("Index Params: %s\n", common.GetKVPair(index.info.GetIndexParams(), "params"))
	fmt.Println("==================================================================")
}

func printIndexV2(index indexpbv2.FieldIndex) {
	fmt.Println("==================================================================")
	fmt.Printf("Index ID: %d\tIndex Name: %s\tCollectionID:%d\n", index.GetIndexInfo().GetIndexID(), index.GetIndexInfo().GetIndexName(), index.GetIndexInfo().GetCollectionID())
	createTime, _ := utils.ParseTS(index.GetCreateTime())
	fmt.Printf("Create Time: %s\tDeleted: %t\n", createTime.Format(tsPrintFormat), index.GetDeleted())
	indexParams := index.GetIndexInfo().GetIndexParams()
	fmt.Printf("Index Type: %s\tMetric Type: %s\n",
		common.GetKVPair(indexParams, "index_type"),
		common.GetKVPair(indexParams, "metric_type"),
	)
	fmt.Printf("Index Params: %s\n", common.GetKVPair(index.GetIndexInfo().GetUserIndexParams(), "params"))
	fmt.Println("==================================================================")
}
