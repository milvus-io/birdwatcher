package show

import (
	"bytes"
	"context"
	"fmt"
	"path"
	"strconv"
	"time"

	"github.com/milvus-io/birdwatcher/states/etcd/common"
	"github.com/milvus-io/birdwatcher/utils"
	"github.com/spf13/cobra"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// CollectionHistoryCommand returns sub command for showCmd.
// show collection-history [options...]
func CollectionHistoryCommand(cli *clientv3.Client, basePath string) *cobra.Command {
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

			resp, err := cli.Get(ctx, path.Join(basePath, "root-coord/collection/", strconv.FormatInt(collectionID, 10)))
			if err != nil {
				fmt.Println("failed to get current collection status", err.Error())
				return
			}
			if len(resp.Kvs) == 0 {
				fmt.Printf("collection id %d does not exist\n", collectionID)
			}

			for _, kv := range resp.Kvs {
				if bytes.Equal(kv.Value, common.CollectionTombstone) {
					fmt.Println("[Current] Collection Already Dropped")
					continue
				}
				fmt.Println("[Current] Collection still healthy")
			}

			// fetch history
			items, err := common.ListCollectionHistory(cli, basePath, collectionID)
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
				common.FillFieldSchemaIfEmpty(cli, basePath, &item.Info)
				printCollection(&item.Info)
			}

		},
	}

	cmd.Flags().Int64("id", 0, "collection id to display")
	return cmd
}
