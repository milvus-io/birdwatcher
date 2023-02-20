package show

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/milvus-io/birdwatcher/states/etcd/common"
	etcdversion "github.com/milvus-io/birdwatcher/states/etcd/version"
	"github.com/milvus-io/birdwatcher/utils"
	"github.com/spf13/cobra"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// CollectionHistoryCommand returns sub command for showCmd.
// show collection-history [options...]
func CollectionHistoryCommand(cli clientv3.KV, basePath string) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "collection-history",
		Short: "display collection change history",
		Run: func(cmd *cobra.Command, args []string) {
			collectionID, err := cmd.Flags().GetInt64("id")
			if err != nil {
				fmt.Println(err.Error())
				return
			}
			if collectionID == 0 {
				fmt.Println("collection id not provided")
				return
			}

			// fetch current for now
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
			defer cancel()

			collection, err := common.GetCollectionByIDVersion(ctx, cli, basePath, etcdversion.GetVersion(), collectionID)
			if err != nil {
				switch {
				case errors.Is(err, common.ErrCollectionDropped):
					fmt.Printf("[Current] collection id %d already marked with Tombstone\n", collectionID)
				case errors.Is(err, common.ErrCollectionNotFound):
					fmt.Printf("[Current] collection id %d not found\n", collectionID)
					return
				default:
					fmt.Println("failed to get current collection state:", err.Error())
					return
				}
			}
			printCollection(collection)
			// fetch history
			items, err := common.ListCollectionHistory(ctx, cli, basePath, etcdversion.GetVersion(), collectionID)
			if err != nil {
				fmt.Println("failed to list history", err.Error())
				return
			}

			for _, item := range items {
				t, _ := utils.ParseTS(item.Ts)
				fmt.Println("Snapshot at", t.Format("2006-01-02 15:04:05"))
				if item.Dropped {
					fmt.Println("Collection Dropped")
					continue
				}
				printCollection(&item.Collection)
			}
		},
	}

	cmd.Flags().Int64("id", 0, "collection id to display")
	return cmd
}
