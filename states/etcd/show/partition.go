package show

import (
	"context"
	"fmt"

	"github.com/milvus-io/birdwatcher/states/etcd/common"
	"github.com/spf13/cobra"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func PartitionCommand(cli clientv3.KV, basePath string) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "partition",
		Short: "list partitions of provided collection",
		Run: func(cmd *cobra.Command, args []string) {
			collectionID, err := cmd.Flags().GetInt64("collection")
			if err != nil {
				fmt.Println(err.Error())
				return
			}

			if collectionID == 0 {
				fmt.Println("please provided collection id")
				return
			}

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			partitions, err := common.ListCollectionPartitions(ctx, cli, basePath, collectionID)
			if err != nil {
				fmt.Println("failed to list partition info", err.Error())
			}

			if len(partitions) == 0 {
				fmt.Printf("no partition found for collection %d\n", collectionID)
			}

			for _, partition := range partitions {
				fmt.Printf("Parition ID: %d\tName: %s\tState: %s\n", partition.ID, partition.Name, partition.State.String())
			}
		},
	}

	cmd.Flags().Int64("collection", 0, "collection id to list")
	return cmd
}
