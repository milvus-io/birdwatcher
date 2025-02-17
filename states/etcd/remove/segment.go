package remove

import (
	"fmt"
	"math"
	"os"
	"strings"
	"time"

	"github.com/cockroachdb/errors"
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
		Short: "Remove segment from meta with specified filters",
		Run: func(cmd *cobra.Command, args []string) {
			targetSegmentID, err := cmd.Flags().GetInt64("segmentID")
			if err != nil {
				fmt.Println(err.Error())
				return
			}
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

			maxNum, err := cmd.Flags().GetInt64("maxNum")
			if err != nil {
				fmt.Println(err.Error())
				return
			}

			state, err := cmd.Flags().GetString("state")
			if err != nil {
				fmt.Println(err.Error())
				return
			}

			backupDir := fmt.Sprintf("segments-backup_%d", time.Now().UnixMilli())

			filterFunc := func(segmentInfo *datapb.SegmentInfo) bool {
				return (collectionID == 0 || segmentInfo.CollectionID == collectionID) &&
					(targetSegmentID == 0 || segmentInfo.GetID() == targetSegmentID) &&
					(state == "" || strings.EqualFold(segmentInfo.State.String(), state))
			}

			removedCnt := 0
			dryRunCount := 0
			opFunc := func(info *datapb.SegmentInfo) error {
				// dry run, display segment first
				if !run {
					dryRunCount++
					fmt.Printf("dry run segment:%d collectionID:%d state:%s\n", info.ID, info.CollectionID, info.State.String())
					return nil
				}

				if err = backupSegmentInfo(info, backupDir); err != nil {
					return err
				}

				if err = common.RemoveSegment(cli, basePath, info); err != nil {
					fmt.Printf("Remove segment %d from Etcd failed, err: %s\n", info.ID, err.Error())
					return err
				}

				removedCnt++
				fmt.Printf("Remove segment %d from etcd succeeds.\n", info.GetID())
				return nil
			}

			err = common.WalkAllSegments(cli, basePath, filterFunc, opFunc, maxNum)
			if err != nil && !errors.Is(err, common.ErrReachMaxNumOfWalkSegment) {
				fmt.Printf("WalkAllSegmentsfailed, err: %s\n", err.Error())
			}

			if !run {
				fmt.Println("dry run segments, total count:", dryRunCount)
				return
			}
			fmt.Println("Remove segments succeeds, total count:", removedCnt)
		},
	}

	cmd.Flags().Bool("run", false, "flags indicating whether to remove segment from meta")
	cmd.Flags().Int64("segmentID", 0, "segment id")
	cmd.Flags().Int64("collectionID", 0, "collection id")
	cmd.Flags().String("state", "", "segment state")
	cmd.Flags().Int64("maxNum", math.MaxInt64, "max number of segment to remove")
	return cmd
}

func backupSegmentInfo(info *datapb.SegmentInfo, backupDir string) error {
	if _, err := os.Stat(backupDir); errors.Is(err, os.ErrNotExist) {
		err := os.MkdirAll(backupDir, os.ModePerm)
		if err != nil {
			fmt.Println("Failed to create folder,", err.Error())
			return err
		}
	}

	now := time.Now()
	filePath := fmt.Sprintf("%s/bw_etcd_segment_%d.%s.bak", backupDir, info.GetID(), now.Format("060102-150405"))
	f, err := os.OpenFile(filePath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		fmt.Println("failed to open backup segment file", err.Error())
		return err
	}

	defer f.Close()

	bs, err := proto.Marshal(info)
	if err != nil {
		fmt.Println("failed to marshal backup segment", err.Error())
		return err
	}

	_, err = f.Write(bs)
	return err
}
