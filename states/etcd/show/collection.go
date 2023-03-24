package show

import (
	"context"
	"fmt"
	"sort"

	"github.com/spf13/cobra"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/milvus-io/birdwatcher/models"
	"github.com/milvus-io/birdwatcher/states/etcd/common"
	etcdversion "github.com/milvus-io/birdwatcher/states/etcd/version"
)

// CollectionCommand returns sub command for showCmd.
// show collection [options...]
func CollectionCommand(cli clientv3.KV, basePath string) *cobra.Command {
	cmd := &cobra.Command{
		Use:     "collections",
		Short:   "list current available collection from RootCoord",
		Aliases: []string{"collection"},
		Run: func(cmd *cobra.Command, args []string) {
			collectionID, err := cmd.Flags().GetInt64("id")
			if err != nil {
				fmt.Println(err.Error())
				return
			}

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			var collections []*models.Collection
			var total int64
			// perform get by id to accelerate
			if collectionID > 0 {
				var collection *models.Collection
				collection, err = common.GetCollectionByIDVersion(ctx, cli, basePath, etcdversion.GetVersion(), collectionID)
				if err == nil {
					collections = append(collections, collection)
				}
			} else {
				collections, err = common.ListCollectionsVersion(ctx, cli, basePath, etcdversion.GetVersion(), func(_ *models.Collection) bool {
					total++
					return true
				})
			}

			if err != nil {
				fmt.Println(err.Error())
				return
			}
			for _, collection := range collections {
				printCollection(collection)
			}
			fmt.Println("================================================================================")
			fmt.Printf("--- Total collections:  %d\t Matched collections:  %d\n", total, len(collections))
		},
	}

	cmd.Flags().Int64("id", 0, "collection id to display")
	return cmd
}

func printCollection(collection *models.Collection) {
	fmt.Println("================================================================================")
	fmt.Printf("Collection ID: %d\tCollection Name: %s\n", collection.ID, collection.Schema.Name)
	fmt.Printf("Collection State: %s\n", collection.State.String())
	/*
		fmt.Printf("Partitions:\n")
		for idx, partID := range collection.GetPartitionIDs() {
			fmt.Printf(" - Partition ID: %d\tPartition Name: %s\n", partID, collection.GetPartitionNames()[idx])
		}*/
	fmt.Printf("Fields:\n")
	fields := collection.Schema.Fields
	sort.Slice(fields, func(i, j int) bool {
		return fields[i].FieldID < fields[j].FieldID
	})
	for _, field := range fields {
		fmt.Printf(" - Field ID: %d \t Field Name: %s \t Field Type: %s\n", field.FieldID, field.Name, field.DataType.String())
		if field.IsPrimaryKey {
			fmt.Printf("\t - Primary Key, AutoID: %v\n", field.AutoID)
		}
		for key, value := range field.Properties {
			fmt.Printf("\t - Type Param %s: %s\n", key, value)
		}
	}

	fmt.Printf("Consistency Level: %s\n", collection.ConsistencyLevel.String())
	for _, channel := range collection.Channels {
		fmt.Printf("Start position for channel %s(%s): %v\n", channel.PhysicalName, channel.VirtualName, channel.StartPosition.MsgID)
	}
}
