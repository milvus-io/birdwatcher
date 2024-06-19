package remove

import (
	"fmt"
	"os"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/spf13/cobra"

	"github.com/milvus-io/birdwatcher/proto/v2.0/datapb"
	"github.com/milvus-io/birdwatcher/states/etcd/common"
	"github.com/milvus-io/birdwatcher/states/kv"
)

// SegmentCommand returns remove segment command.
func SegmentCommand(cli kv.MetaKV, basePath string) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "segment",
		Short: "Remove segment from meta with specified segment id",
		Run: func(cmd *cobra.Command, args []string) {
			targetSegmentID, err := cmd.Flags().GetInt64("segment")
			if err != nil {
				fmt.Println(err.Error())
				return
			}
			run, err := cmd.Flags().GetBool("run")
			if err != nil {
				fmt.Println(err.Error())
				return
			}

			segments, err := common.ListSegments(cli, basePath, func(segmentInfo *datapb.SegmentInfo) bool {
				return segmentInfo.GetID() == targetSegmentID
			})
			if err != nil {
				fmt.Println("failed to list segments", err.Error())
				return
			}

			if len(segments) != 1 {
				fmt.Printf("failed to get segment with id %d, get %d result(s)\n", targetSegmentID, len(segments))
				return
			}

			// dry run, display segment first
			if !run {
				// show.PrintSegmentInfo(segments[0], false)
				fmt.Printf("segment info %v", segments[0])
				return
			}

			// TODO put audit log
			info := segments[0]
			backupSegmentInfo(info)
			fmt.Println("[WARNING] about to remove segment from etcd")
			err = common.RemoveSegment(cli, basePath, info)
			if err != nil {
				fmt.Printf("Remove segment %d from Etcd failed, err: %s\n", info.ID, err.Error())
				return
			}
			fmt.Printf("Remove segment %d from etcd succeeds.\n", info.GetID())
		},
	}

	cmd.Flags().Bool("run", false, "flags indicating whether to remove segment from meta")
	cmd.Flags().Int64("segment", 0, "segment id to remove")
	return cmd
}

func backupSegmentInfo(info *datapb.SegmentInfo) {
	now := time.Now()
	filePath := fmt.Sprintf("bw_etcd_segment_%d.%s.bak", info.GetID(), now.Format("060102-150405"))
	f, err := os.OpenFile(filePath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		fmt.Println("failed to open backup segment file", err.Error())
		return
	}

	defer f.Close()

	bs, err := proto.Marshal(info)
	if err != nil {
		fmt.Println("failed to marshal backup segment", err.Error())
		return
	}

	f.Write(bs)
}
