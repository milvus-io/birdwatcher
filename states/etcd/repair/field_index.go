package repair

import (
	"context"
	"fmt"

	"github.com/spf13/cobra"
	clientv3 "go.etcd.io/etcd/client/v3"

	indexpbv2 "github.com/milvus-io/birdwatcher/proto/v2.2/indexpb"
)

// FieldIndexParamsCommand return repair segment command.
func FieldIndexParamsCommand(cli clientv3.KV, basePath string) *cobra.Command {
	cmd := &cobra.Command{
		Use:     "delete_field_index",
		Aliases: []string{"delete_field_index"},
		Short:   "mark field index as deleted",
		Run: func(cmd *cobra.Command, args []string) {
			collID, err := cmd.Flags().GetInt64("collectionID")
			if err != nil {
				fmt.Println(err.Error())
				return
			}
			indexID, err := cmd.Flags().GetInt64("indexID")
			if err != nil {
				fmt.Println(err.Error())
				return
			}
			run, err := cmd.Flags().GetBool("run")
			if err != nil {
				fmt.Println(err.Error())
				return
			}

			indexes, err := listIndexMetaV2(cli, basePath)
			if err != nil {
				fmt.Println(err.Error())
				return
			}

			matchIndexes := make([]indexpbv2.FieldIndex, 0)
			for _, index := range indexes {
				if index.GetIndexInfo().GetCollectionID() == collID && index.GetIndexInfo().GetIndexID() == indexID {
					matchIndexes = append(matchIndexes, index)
				}
			}

			fmt.Println("==========================before repair index metric========================================")
			for _, index := range matchIndexes {
				printIndexV2(index)
				if !run {
					continue
				}
				err := markFieldIndexDeleted(context.Background(), cli, basePath, index)
				if err != nil {
					fmt.Println("write repaired index failed, ", err.Error())
				}
			}
			fmt.Println("==========================after repair index metric========================================")
			newIndexes, err := listIndexMetaV2(cli, basePath)
			if err != nil {
				fmt.Println(err.Error())
				return
			}
			for _, index := range newIndexes {
				if index.GetIndexInfo().GetCollectionID() == collID && index.GetIndexInfo().GetIndexID() == indexID {
					printIndexV2(index)
				}
			}
		},
	}

	cmd.Flags().Int64("collectionID", 0, "collection id to filter with")
	cmd.Flags().Int64("indexID", 0, "index id to filter with")
	cmd.Flags().Bool("run", false, "actual do repair")
	return cmd
}

func markFieldIndexDeleted(ctx context.Context, cli clientv3.KV, basePath string, index indexpbv2.FieldIndex) error {
	newIndex := &indexpbv2.FieldIndex{
		IndexInfo:  index.GetIndexInfo(),
		Deleted:    true,
		CreateTime: index.GetCreateTime(),
	}
	if err := writeRepairedIndex(cli, basePath, newIndex); err != nil {
		return err
	}
	return nil
}
