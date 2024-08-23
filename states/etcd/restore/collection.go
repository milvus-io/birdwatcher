package restore

import (
	"context"
	"fmt"

	"github.com/spf13/cobra"

	"github.com/milvus-io/birdwatcher/models"
	commonpbv2 "github.com/milvus-io/birdwatcher/proto/v2.2/commonpb"
	datapbv2 "github.com/milvus-io/birdwatcher/proto/v2.2/datapb"
	etcdpbv2 "github.com/milvus-io/birdwatcher/proto/v2.2/etcdpb"
	"github.com/milvus-io/birdwatcher/states/etcd/common"
	etcdversion "github.com/milvus-io/birdwatcher/states/etcd/version"
	"github.com/milvus-io/birdwatcher/states/kv"
)

// CollectionCommand returns sub command for restore command.
// restore collection [options...]
func CollectionCommand(cli kv.MetaKV, basePath string) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "collection",
		Short: "restore dropping/dropped collection meta",
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

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			var collection *models.Collection
			// TODO check history as well for dropped collection
			collection, err = common.GetCollectionByIDVersion(ctx, cli, basePath, etcdversion.GetVersion(), collectionID)
			if err != nil {
				fmt.Println("faile to get collection by id", err.Error())
				return
			}

			updateCollState := func(collection *etcdpbv2.CollectionInfo) {
				collection.State = etcdpbv2.CollectionState_CollectionCreated
			}

			if collection.DBID > 0 {
				// err = common.UpdateCollectionWithDB(ctx, cli, basePath, collectionID, collection.DBID, updateCollState)
			} else {
				err = common.UpdateCollection(ctx, cli, basePath, collectionID, updateCollState, false)
			}

			if err != nil {
				fmt.Println(err.Error())
				return
			}
			err = common.RemoveCollectionHistory(ctx, cli, basePath, etcdversion.GetVersion(), collectionID)
			if err != nil {
				fmt.Println(err.Error())
				return
			}

			err = common.UpdateSegments(ctx, cli, basePath, collectionID, func(segment *datapbv2.SegmentInfo) {
				segment.State = commonpbv2.SegmentState_Flushed
			})
			if err != nil {
				fmt.Println(err.Error())
				return
			}

			for _, channel := range collection.Channels {
				err = common.SetChannelWatch(ctx, cli, basePath, channel.VirtualName, collection)
				if err != nil {
					fmt.Println(err.Error())
				}
			}
		},
	}

	cmd.Flags().Int64("id", 0, "collection id to restore")
	return cmd
}
