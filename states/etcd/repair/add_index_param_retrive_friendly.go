package repair

import (
	"fmt"

	"github.com/spf13/cobra"

	"github.com/golang/protobuf/proto"
	commonpbv2 "github.com/milvus-io/birdwatcher/proto/v2.2/commonpb"
	indexpbv2 "github.com/milvus-io/birdwatcher/proto/v2.2/indexpb"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// AddIndexParamsCommand return repair segment command.
func AddIndexParamsCommand(cli clientv3.KV, basePath string) *cobra.Command {
	cmd := &cobra.Command{
		Use:     "add_index_params",
		Aliases: []string{"add_index_params"},
		Short:   "check index param and try to add param",
		Run: func(cmd *cobra.Command, args []string) {
			collID, err := cmd.Flags().GetInt64("collection")
			if err != nil {
				fmt.Println(err.Error())
				return
			}
			run, err := cmd.Flags().GetBool("run")
			if err != nil {
				fmt.Println(err.Error())
				return
			}
			key, err := cmd.Flags().GetString("key")
			if err != nil {
				fmt.Println(err.Error())
				return
			}
			value, err := cmd.Flags().GetString("value")
			if err != nil {
				fmt.Println(err.Error())
				return
			}
			indexes, err := listIndexMetaV2(cli, basePath)
			if err != nil {
				fmt.Println(err.Error())
				return
			}
			newIndexes := make([]*indexpbv2.FieldIndex, 0)
			for i, index := range indexes {
				if collID != 0 && index.IndexInfo.CollectionID != collID {
					continue
				}
				newIndex := proto.Clone(&indexes[i]).(*indexpbv2.FieldIndex)
				indexType := ""
				for _, pair := range index.IndexInfo.IndexParams {
					if pair.Key == "index_type" {
						indexType = pair.Value
					}
				}
				if indexType != "DISKANN" && indexType != "HNSW" {
					continue
				}
				exist := false
				for _, pair := range index.IndexInfo.IndexParams {
					if pair.Key == key {
						exist = true
						break
					}
				}
				if !exist {
					newIndex.IndexInfo.IndexParams = append(newIndex.IndexInfo.IndexParams, &commonpbv2.KeyValuePair{
						Key:   key,
						Value: value,
					})
					newIndexes = append(newIndexes, newIndex)
				}
			}
			if !run {
				fmt.Println("after repair index:")
				for _, index := range newIndexes {
					printIndexV2(*index)
				}
				return
			}
			for _, index := range newIndexes {
				if err := writeRepairedIndex(cli, basePath, index); err != nil {
					fmt.Println(err.Error())
					return
				}
			}
			afterRepairIndexes, err := listIndexMetaV2(cli, basePath)
			if err != nil {
				fmt.Println(err.Error())
				return
			}
			for _, index := range afterRepairIndexes {
				printIndexV2(index)
			}
		},
	}

	cmd.Flags().Int64("collection", 0, "collection id to filter with")
	cmd.Flags().Bool("run", false, "actual do repair")
	cmd.Flags().String("key", "retrieve_friendly", "add params key")
	cmd.Flags().String("value", "true", "add params value")
	return cmd
}
