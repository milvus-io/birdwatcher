package show

import (
	"fmt"

	"github.com/milvus-io/birdwatcher/proto/v2.0/querypb"
	"github.com/milvus-io/birdwatcher/states/etcd/common"
	"github.com/spf13/cobra"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// SegmentLoadedCommand returns show segment-loaded command.
func SegmentLoadedCommand(cli clientv3.KV, basePath string) *cobra.Command {
	cmd := &cobra.Command{
		Use:     "segment-loaded",
		Short:   "display segment information from querycoord",
		Aliases: []string{"segments-loaded"},
		RunE: func(cmd *cobra.Command, args []string) error {

			collID, err := cmd.Flags().GetInt64("collection")
			if err != nil {
				return err
			}
			segmentID, err := cmd.Flags().GetInt64("segment")
			if err != nil {
				return err
			}

			segments, err := common.ListLoadedSegments(cli, basePath, func(info *querypb.SegmentInfo) bool {
				return (collID == 0 || info.CollectionID == collID) &&
					(segmentID == 0 || info.SegmentID == segmentID)
			})
			if err != nil {
				fmt.Println("failed to list segments", err.Error())
				return nil
			}

			for _, info := range segments {
				fmt.Printf("Segment ID: %d LegacyNodeID: %d NodeIds: %v,DmlChannel: %s\n", info.SegmentID, info.NodeID, info.NodeIds, info.DmChannel)
				fmt.Printf("%#v\n", info.GetIndexInfos())
			}

			return nil
		},
	}
	cmd.Flags().Int64("collection", 0, "collection id to filter with")
	cmd.Flags().String("format", "line", "segment display format")
	cmd.Flags().Bool("detail", false, "flags indicating whether pring detail binlog info")
	cmd.Flags().Int64("segment", 0, "segment id to filter with")
	return cmd
}
