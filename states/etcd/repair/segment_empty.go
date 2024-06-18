package repair

import (
	"fmt"

	"github.com/spf13/cobra"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/milvus-io/birdwatcher/proto/v2.0/commonpb"
	"github.com/milvus-io/birdwatcher/proto/v2.0/datapb"
	"github.com/milvus-io/birdwatcher/states/etcd/common"
)

// EmptySegmentCommand returns repair empty-segment command.
func EmptySegmentCommand(cli clientv3.KV, basePath string) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "empty-segment",
		Short: "Remove empty segment from meta",
		RunE: func(cmd *cobra.Command, args []string) error {
			run, err := cmd.Flags().GetBool("run")
			if err != nil {
				return err
			}
			segments, err := common.ListSegments(cli, basePath, func(info *datapb.SegmentInfo) bool {
				return info.GetState() == commonpb.SegmentState_Flushed || info.GetState() == commonpb.SegmentState_Flushing || info.GetState() == commonpb.SegmentState_Sealed
			})
			if err != nil {
				fmt.Println("failed to list segments", err.Error())
				return nil
			}

			for _, info := range segments {
				common.FillFieldsIfV2(cli, basePath, info)
				if isEmptySegment(info) {
					fmt.Printf("suspect segment %d found:\n", info.GetID())
					fmt.Printf("SegmentID: %d State: %s, Row Count:%d\n", info.ID, info.State.String(), info.NumOfRows)
					if run {
						err := common.RemoveSegment(cli, basePath, info)
						if err == nil {
							fmt.Printf("remove segment %d from meta succeed\n", info.GetID())
						} else {
							fmt.Printf("remove segment %d failed, err: %s\n", info.GetID(), err.Error())
						}
					}
				}
			}

			return nil
		},
	}

	cmd.Flags().Bool("run", false, "flags indicating whether to remove segments from meta")
	return cmd
}

// returns whether all binlog/statslog/deltalog is empty
func isEmptySegment(info *datapb.SegmentInfo) bool {
	for _, log := range info.GetBinlogs() {
		if len(log.Binlogs) > 0 {
			return false
		}
	}
	for _, log := range info.GetStatslogs() {
		if len(log.Binlogs) > 0 {
			return false
		}
	}
	for _, log := range info.GetDeltalogs() {
		if len(log.Binlogs) > 0 {
			return false
		}
	}
	return true
}
