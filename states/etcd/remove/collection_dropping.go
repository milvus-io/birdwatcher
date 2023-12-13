package remove

import (
	"context"
	"fmt"
	"path"

	"github.com/milvus-io/birdwatcher/models"
	"github.com/milvus-io/birdwatcher/states/etcd/common"
	etcdversion "github.com/milvus-io/birdwatcher/states/etcd/version"
	"github.com/milvus-io/birdwatcher/states/kv"
	"github.com/spf13/cobra"
)

// CollectionDropCommand returns `remove collection-drop` command.
func CollectionDropCommand(cli kv.MetaKV, basePath string) *cobra.Command {
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

func cleanCollectionDropMeta(cli kv.MetaKV, basePath string, collection *models.Collection, run bool) {
	fmt.Println("Clean collection(drop) meta:")
	if collection.Key() == "" {
		fmt.Printf("Collection %s[%d] key is empty string, cannot perform cleanup", collection.Schema.Name, collection.ID)
		return
	}

	// better to remove collection meta finally for better atomicity.
	// TODO: alias meta can't be cleaned conveniently.
	prefixes := []string{
		// remove collection field meta
		path.Join(basePath, fmt.Sprintf("root-coord/fields/%d", collection.ID)) + "/",
		path.Join(basePath, common.SnapshotPrefix, fmt.Sprintf("root-coord/fields/%d", collection.ID)) + "/",

		// remove collection partition meta
		path.Join(basePath, fmt.Sprintf("root-coord/partitions/%d", collection.ID)) + "/",
		path.Join(basePath, common.SnapshotPrefix, fmt.Sprintf("root-coord/partitions/%d", collection.ID)) + "/",
	}

	var collectionKey string
	if collection.DBID != 0 {
		collectionKey = fmt.Sprintf("root-coord/database/collection-info/%d/%d", collection.DBID, collection.ID)
	} else {
		collectionKey = fmt.Sprintf("root-coord/collection/%d", collection.ID)
	}

	// collection will have timestamp suffix, which should be also removed by prefix.
	prefixes = append(prefixes, path.Join(basePath, collectionKey))
	prefixes = append(prefixes, path.Join(basePath, common.SnapshotPrefix, collectionKey))

	for _, prefix := range prefixes {
		if err := cli.RemoveWithPrefix(context.TODO(), prefix); err != nil {
			fmt.Printf("failed to clean prefix: %s, error: %s\n", prefix, err.Error())
		} else {
			fmt.Printf("clean prefix: %s\n", prefix)
		}
	}

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
		cli.Remove(context.TODO(), info.Key())
	}

	// channel checkpoint and removal
	for _, channel := range collection.Channels {
		cpKey := path.Join(basePath, "datacoord-meta/channel-cp", channel.VirtualName)
		fmt.Println("channel checkpoint:", cpKey)
		cli.Remove(context.TODO(), cpKey)
		removalKey := path.Join(basePath, "datacoord-meta/channel-removal", channel.VirtualName)
		fmt.Println("channel removal", removalKey)
		cli.Remove(context.TODO(), removalKey)
	}

	// dry run
	if !run {
		fmt.Println("Dry run complete")
		return
	}

	// TODO: yi
	// remove all keys with transaction
	/*
		resp, err := cli.Txn(context.TODO()).If().Then(ops...).Commit()
		if err != nil {
			fmt.Printf("failed to remove meta for collection %s[%d], err: %s\n", collection.Schema.Name, collection.ID, err.Error())
			return
		}
		fmt.Printf("Batch remove collection %s[%d] meta, transaction succeed: %v\n", collection.Schema.Name, collection.ID, resp.Succeeded)
	*/
}
