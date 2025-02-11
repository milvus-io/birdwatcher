package repair

import (
	"context"
	"fmt"
	"path"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/spf13/cobra"

	commonpbv2 "github.com/milvus-io/birdwatcher/proto/v2.2/commonpb"
	indexpbv2 "github.com/milvus-io/birdwatcher/proto/v2.2/indexpb"
	"github.com/milvus-io/birdwatcher/states/etcd/common"
	"github.com/milvus-io/birdwatcher/states/kv"
	"github.com/milvus-io/birdwatcher/utils"
)

const (
	tsPrintFormat = "2006-01-02 15:04:05.999 -0700"
)

// IndexMetricCommand return repair segment command.
func IndexMetricCommand(cli kv.MetaKV, basePath string) *cobra.Command {
	cmd := &cobra.Command{
		Use:     "index_metric_type",
		Aliases: []string{"indexes_metric_type"},
		Short:   "do index meta of metric_type check and try to repair",
		Run: func(cmd *cobra.Command, args []string) {
			collID, err := cmd.Flags().GetInt64("collection")
			if err != nil {
				fmt.Println(err.Error())
				return
			}
			indexes, err := listIndexMetaV2(cli, basePath)
			if err != nil {
				fmt.Println(err.Error())
				return
			}
			errExist := false
			for _, index := range indexes {
				if index.IndexInfo.CollectionID != collID {
					continue
				}
				exitInIndexParams := false
				newIndex := &indexpbv2.FieldIndex{
					IndexInfo: &indexpbv2.IndexInfo{
						CollectionID:         index.GetIndexInfo().GetCollectionID(),
						FieldID:              index.GetIndexInfo().GetFieldID(),
						IndexName:            index.GetIndexInfo().GetIndexName(),
						IndexID:              index.GetIndexInfo().GetIndexID(),
						TypeParams:           index.GetIndexInfo().GetTypeParams(),
						IndexParams:          make([]*commonpbv2.KeyValuePair, 0),
						IndexedRows:          index.GetIndexInfo().GetIndexedRows(),
						TotalRows:            index.GetIndexInfo().GetTotalRows(),
						State:                index.GetIndexInfo().GetState(),
						IndexStateFailReason: index.GetIndexInfo().GetIndexStateFailReason(),
						IsAutoIndex:          index.GetIndexInfo().GetIsAutoIndex(),
						UserIndexParams:      index.GetIndexInfo().GetUserIndexParams(),
					},
					Deleted:    index.GetDeleted(),
					CreateTime: index.GetCreateTime(),
				}
				for _, pair := range index.IndexInfo.IndexParams {
					if pair.Key == "metric_type" {
						if pair.Value == "" {
							exitInIndexParams = false
							continue
						}
						exitInIndexParams = true
					}
					newIndex.IndexInfo.IndexParams = append(newIndex.IndexInfo.IndexParams, pair)
				}
				if !exitInIndexParams {
					errExist = true
					exitInTypeParams := false
					for _, pair := range index.IndexInfo.TypeParams {
						if pair.Key == "metric_type" && pair.Value != "" {
							exitInTypeParams = true
							newIndex.IndexInfo.IndexParams = append(newIndex.IndexInfo.IndexParams, pair)
							break
						}
					}
					if !exitInTypeParams {
						fmt.Println("no metric_type in IndexParams or TypeParams")
						return
					}
					if err := writeRepairedIndex(cli, basePath, newIndex); err != nil {
						fmt.Println(err.Error())
						return
					}
				}
			}
			if !errExist {
				fmt.Println("no error found")
				return
			}
			newIndexes, err := listIndexMetaV2(cli, basePath)
			if err != nil {
				fmt.Println(err.Error())
				return
			}
			for _, index := range newIndexes {
				printIndexV2(index)
			}
		},
	}

	cmd.Flags().Int64("collection", 0, "collection id to filter with")
	return cmd
}

func listIndexMetaV2(cli kv.MetaKV, basePath string) ([]indexpbv2.FieldIndex, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
	defer cancel()
	indexes, _, err := common.ListProtoObjects[indexpbv2.FieldIndex](ctx, cli, path.Join(basePath, "field-index"))
	return indexes, err
}

func writeRepairedIndex(cli kv.MetaKV, basePath string, index *indexpbv2.FieldIndex) error {
	p := path.Join(basePath, fmt.Sprintf("field-index/%d/%d", index.IndexInfo.CollectionID, index.IndexInfo.IndexID))

	bs, err := proto.Marshal(index)
	if err != nil {
		fmt.Println("failed to marshal segment info", err.Error())
	}
	err = cli.Save(context.Background(), p, string(bs))
	return err
}

func printIndexV2(index indexpbv2.FieldIndex) {
	fmt.Println("==========================after repair index metric========================================")
	fmt.Printf("Index ID: %d\tIndex Name: %s\tCollectionID:%d\n", index.GetIndexInfo().GetIndexID(), index.GetIndexInfo().GetIndexName(), index.GetIndexInfo().GetCollectionID())
	createTime, _ := utils.ParseTS(index.GetCreateTime())
	fmt.Printf("Create Time: %s\tDeleted: %t\n", createTime.Format(tsPrintFormat), index.GetDeleted())
	indexParams := index.GetIndexInfo().GetIndexParams()
	fmt.Printf("Index Params: %s\n", indexParams)
	fmt.Println("===========================================================================================")
}
