package remove

import (
	"context"
	"fmt"
	"path"

	"github.com/milvus-io/birdwatcher/models"
	"github.com/milvus-io/birdwatcher/states/etcd/common"
	etcdversion "github.com/milvus-io/birdwatcher/states/etcd/version"
	"github.com/spf13/cobra"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// CollectionDropCommand returns `remove collection-drop` command.
func CollectionDropCommand(cli clientv3.KV, basePath string) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "collection-drop",
		Short: "Remove collection & channel meta for collection in dropping/dropped state",
		Run: func(cmd *cobra.Command, args []string) {
			collectionID, err := cmd.Flags().GetInt64("collectionID")
			if err != nil {
				fmt.Println(err.Error())
				return
			}
			run, err := cmd.Flags().GetBool("run")
			if err != nil {
				fmt.Println(err.Error())
				return
			}

			var collections []*models.Collection
			if collectionID > 0 {
				collection, err := common.GetCollectionByIDVersion(context.Background(), cli, basePath, etcdversion.GetVersion(), collectionID)
				if err != nil {
					fmt.Printf("failed to get collection by id(%d): %s\n", collectionID, err.Error())
				}
				// skip healthy collection
				if collection.State != models.CollectionStateCollectionDropping && collection.State != models.CollectionStateCollectionDropped {
					fmt.Printf("Collection State is [%s]\n", collection.State.String())
					return
				}
				collections = append(collections, collection)
			} else {
				collections, err = common.ListCollectionsVersion(context.Background(), cli, basePath, etcdversion.GetVersion(), func(coll *models.Collection) bool {
					return coll.State == models.CollectionStateCollectionDropping || coll.State == models.CollectionStateCollectionDropped
				})
			}

			for _, collection := range collections {
				fmt.Printf("Found Dropping Collection ID: %s[%d]\n", collection.Schema.Name, collection.ID)
				cleanCollectionDropMeta(cli, basePath, collection, run)
			}
		},
	}

	cmd.Flags().Bool("run", false, "flags indicating whether to execute removed command")
	cmd.Flags().Int64("collectionID", 0, "collection id to remove")
	return cmd
}

func cleanCollectionDropMeta(cli clientv3.KV, basePath string, collection *models.Collection, run bool) {

	fmt.Println("Clean collection(drop) meta:")
	var ops []clientv3.Op
	if collection.Key() == "" {
		fmt.Printf("Collection %s[%d] key is empty string, cannot perform cleanup", collection.Schema.Name, collection.ID)
		return
	}
	// remove collection meta
	ops = append(ops, clientv3.OpDelete(collection.Key()))
	fmt.Printf("Collection Meta: %s\n", collection.Key())
	// remove collection field meta
	fieldsPrefix := path.Join(basePath, fmt.Sprintf("root-coord/fields/%d", collection.ID)) + "/"
	fmt.Printf("Collection Field Meta(Prefix): %s\n", collection.Key())
	ops = append(ops, clientv3.OpDelete(fieldsPrefix, clientv3.WithPrefix()))
	// remove collection partition meta
	partitionsPrefix := path.Join(basePath, fmt.Sprintf("root-coord/partitions/%d", collection.ID)) + "/"
	fmt.Printf("Collection Partition Meta(Prefix): %s\n", partitionsPrefix)
	ops = append(ops, clientv3.OpDelete(partitionsPrefix, clientv3.WithPrefix()))

	channelWatchInfos, err := common.ListChannelWatch(context.Background(), cli, basePath, etcdversion.GetVersion(), func(cw *models.ChannelWatch) bool {
		return cw.Vchan.CollectionID == collection.ID
	})
	if err != nil {
		fmt.Printf("failed to list channel watch info for collection[%d], err: %s\n", collection.ID, err.Error())
		return
	}
	for _, info := range channelWatchInfos {
		// remove channel watch meta
		if info.Key() == "" {
			fmt.Printf("channel[%s] watch info key is empty\n", info.Vchan.ChannelName)
			return
		}
		fmt.Println("channel watch info:", info.Key())
		ops = append(ops, clientv3.OpDelete(info.Key()))
	}

	// channel checkpoint and removal
	for _, channel := range collection.Channels {
		cpKey := path.Join(basePath, "datacoord-meta/channel-cp", channel.VirtualName)
		fmt.Println("channel checkpoint:", cpKey)
		ops = append(ops, clientv3.OpDelete(cpKey))
		removalKey := path.Join(basePath, "datacoord-meta/channel-removal", channel.VirtualName)
		fmt.Println("channel removal", removalKey)
		ops = append(ops, clientv3.OpDelete(removalKey))
	}

	// dry run
	if !run {
		fmt.Println("Dry run complete")
		return
	}

	// remove all keys with transaction
	resp, err := cli.Txn(context.TODO()).If().Then(ops...).Commit()
	if err != nil {
		fmt.Printf("failed to remove meta for collection %s[%d], err: %s\n", collection.Schema.Name, collection.ID, err.Error())
		return
	}
	fmt.Printf("Batch remove collection %s[%d] meta, transaction succeed: %v\n", collection.Schema.Name, collection.ID, resp.Succeeded)
}
