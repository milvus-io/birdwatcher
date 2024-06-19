package remove

import (
	"context"
	"fmt"

	"github.com/spf13/cobra"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/milvus-io/birdwatcher/models"
	"github.com/milvus-io/birdwatcher/proto/v2.0/datapb"
	"github.com/milvus-io/birdwatcher/states/etcd/common"
	etcdversion "github.com/milvus-io/birdwatcher/states/etcd/version"
)

// SegmentCollectionDroppedCommand returns `remove collection-drop` command.
func SegmentCollectionDroppedCommand(cli clientv3.KV, basePath string) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "segments-collection-dropped",
		Short: "Remove segments & binlogs meta for collection that has been dropped",
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
			collections, _ = common.ListCollectionsVersion(context.Background(), cli, basePath, etcdversion.GetVersion(), func(coll *models.Collection) bool {
				return coll.ID == collectionID
			})
			if len(collections) != 0 {
				fmt.Printf("collection %d is still exist.", collectionID)
				return
			}
			fmt.Printf("Drop segments meta with dropped collection: %d\n", collectionID)

			segments, err := common.ListSegments(cli, basePath, func(segmentInfo *datapb.SegmentInfo) bool {
				return segmentInfo.GetCollectionID() == collectionID
			})
			if err != nil {
				fmt.Println("failed to list segments", err.Error())
				return
			}

			// dry run, display segment first
			if !run {
				// show.PrintSegmentInfo(segments[0], false)
				for _, info := range segments {
					fmt.Printf("segment %d with collection %d will be removed.\n", info.GetID(), info.GetCollectionID())
				}
				return
			}

			for _, info := range segments {
				fmt.Printf("[WARNING] about to remove segment %d from etcd\n", info.GetID())
				err = common.RemoveSegment(cli, basePath, info)
				if err != nil {
					fmt.Printf("Remove segment %d from Etcd failed, err: %s\n", info.ID, err.Error())
					return
				}
				fmt.Printf("Remove segment %d from etcd succeeds.\n", info.GetID())
			}
		},
	}

	cmd.Flags().Bool("run", false, "flags indicating whether to execute removed command")
	cmd.Flags().Int64("collectionID", 0, "collection id to remove")
	return cmd
}
