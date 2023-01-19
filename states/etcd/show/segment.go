package show

import (
	"fmt"
	"sort"
	"time"

	"github.com/milvus-io/birdwatcher/proto/v2.0/commonpb"
	"github.com/milvus-io/birdwatcher/proto/v2.0/datapb"
	"github.com/milvus-io/birdwatcher/states/etcd/common"
	"github.com/milvus-io/birdwatcher/utils"

	"github.com/spf13/cobra"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// SegmentCommand returns show segments command.
func SegmentCommand(cli *clientv3.Client, basePath string) *cobra.Command {
	cmd := &cobra.Command{
		Use:     "segment",
		Short:   "display segment information from data coord meta store",
		Aliases: []string{"segments"},
		RunE: func(cmd *cobra.Command, args []string) error {

			collID, err := cmd.Flags().GetInt64("collection")
			if err != nil {
				return err
			}
			segmentID, err := cmd.Flags().GetInt64("segment")
			if err != nil {
				return err
			}
			format, err := cmd.Flags().GetString("format")
			if err != nil {
				return err
			}
			detail, err := cmd.Flags().GetBool("detail")
			if err != nil {
				return err
			}
			state, err := cmd.Flags().GetString("state")
			if err != nil {
				return err
			}

			segments, err := common.ListSegments(cli, basePath, func(info *datapb.SegmentInfo) bool {
				return (collID == 0 || info.CollectionID == collID) &&
					(segmentID == 0 || info.ID == segmentID) &&
					(state == "" || info.State.String() == state)
			})
			if err != nil {
				fmt.Println("failed to list segments", err.Error())
				return nil
			}

			totalRC := int64(0)
			healthy := 0
			var statslogSize int64
			var growing, sealed, flushed int
			fieldSize := make(map[int64]int64)
			for _, info := range segments {

				if info.State != commonpb.SegmentState_Dropped {
					totalRC += info.NumOfRows
					healthy++
				}
				switch info.State {
				case commonpb.SegmentState_Growing:
					growing++
				case commonpb.SegmentState_Sealed:
					sealed++
				case commonpb.SegmentState_Flushing, commonpb.SegmentState_Flushed:
					flushed++
				}

				switch format {
				case "table":
					common.FillFieldsIfV2(cli, basePath, info)
					PrintSegmentInfo(info, detail)
				case "line":
					fmt.Printf("SegmentID: %d State: %s, Row Count:%d\n", info.ID, info.State.String(), info.NumOfRows)
				case "statistics":
					if info.GetState() != commonpb.SegmentState_Dropped {
						common.FillFieldsIfV2(cli, basePath, info)
						for _, binlog := range info.GetBinlogs() {
							for _, log := range binlog.GetBinlogs() {
								fieldSize[binlog.FieldID] += log.GetLogSize()
							}
						}
						for _, statslog := range info.GetStatslogs() {
							for _, binlog := range statslog.GetBinlogs() {
								statslogSize += binlog.LogSize
							}
						}
					}

				}

			}
			if format == "statistics" {
				var totalBinlogSize int64
				for fieldID, size := range fieldSize {
					fmt.Printf("\t field binlog size[%d]: %s\n", fieldID, hrSize(size))
					totalBinlogSize += size
				}
				fmt.Printf("--- Total binlog size: %s\n", hrSize(totalBinlogSize))
				fmt.Printf("--- Total statslog size: %s\n", hrSize(statslogSize))
			}

			fmt.Printf("--- Growing: %d, Sealed: %d, Flushed: %d\n", growing, sealed, flushed)
			fmt.Printf("--- Total Segments: %d, row count: %d\n", healthy, totalRC)
			return nil
		},
	}
	cmd.Flags().Int64("collection", 0, "collection id to filter with")
	cmd.Flags().Int64("partition", 0, "partition id to filter with")
	cmd.Flags().String("format", "line", "segment display format")
	cmd.Flags().Bool("detail", false, "flags indicating whether pring detail binlog info")
	cmd.Flags().Int64("segment", 0, "segment id to filter with")
	cmd.Flags().String("state", "", "target segment state")
	return cmd
}

func hrSize(size int64) string {
	sf := float64(size)
	units := []string{"Bytes", "KB", "MB", "GB"}
	idx := 0
	for sf > 1024.0 && idx < 3 {
		sf /= 1024.0
		idx++
	}
	return fmt.Sprintf("%f %s", sf, units[idx])
}

const (
	tsPrintFormat = "2006-01-02 15:04:05.999 -0700"
)

// PrintSegmentInfo prints segments info
func PrintSegmentInfo(info *datapb.SegmentInfo, detailBinlog bool) {
	fmt.Println("================================================================================")
	fmt.Printf("Segment ID: %d\n", info.ID)
	fmt.Printf("Segment State:%v", info.State)
	if info.State == commonpb.SegmentState_Dropped {
		dropTime := time.Unix(0, int64(info.DroppedAt))
		fmt.Printf("\tDropped Time: %s", dropTime.Format(tsPrintFormat))
	}
	fmt.Println()
	fmt.Printf("Collection ID: %d\t\tPartitionID: %d\n", info.CollectionID, info.PartitionID)
	fmt.Printf("Insert Channel:%s\n", info.InsertChannel)
	fmt.Printf("Num of Rows: %d\t\tMax Row Num: %d\n", info.NumOfRows, info.MaxRowNum)
	lastExpireTime, _ := utils.ParseTS(info.LastExpireTime)
	fmt.Printf("Last Expire Time: %s\n", lastExpireTime.Format(tsPrintFormat))
	fmt.Printf("Compact from %v \n", info.CompactionFrom)
	if info.StartPosition != nil {
		startTime, _ := utils.ParseTS(info.GetStartPosition().GetTimestamp())
		fmt.Printf("Start Position ID: %v, time: %s\n", info.StartPosition.MsgID, startTime.Format(tsPrintFormat))
	} else {
		fmt.Println("Start Position: nil")
	}
	if info.DmlPosition != nil {
		dmlTime, _ := utils.ParseTS(info.DmlPosition.Timestamp)
		fmt.Printf("Dml Position ID: %v, time: %s, channel name: %s\n", info.DmlPosition.MsgID, dmlTime.Format(tsPrintFormat), info.GetDmlPosition().GetChannelName())
	} else {
		fmt.Println("Dml Position: nil")
	}
	fmt.Printf("Binlog Nums %d\tStatsLog Nums: %d\tDeltaLog Nums:%d\n",
		countBinlogNum(info.Binlogs), countBinlogNum(info.Statslogs), countBinlogNum(info.Deltalogs))

	if detailBinlog {
		var binlogSize int64
		fmt.Println("**************************************")
		fmt.Println("Binlogs:")
		sort.Slice(info.Binlogs, func(i, j int) bool {
			return info.Binlogs[i].FieldID < info.Binlogs[j].FieldID
		})
		for _, log := range info.Binlogs {
			for _, binlog := range log.Binlogs {
				fmt.Printf("Path: %s\n", binlog.LogPath)
				tf, _ := utils.ParseTS(binlog.TimestampFrom)
				tt, _ := utils.ParseTS(binlog.TimestampTo)
				fmt.Printf("Log Size: %d \t Entry Num: %d\t TimeRange:%s-%s\n",
					binlog.LogSize, binlog.EntriesNum,
					tf.Format(tsPrintFormat), tt.Format(tsPrintFormat))
				binlogSize += binlog.LogSize
			}
		}

		fmt.Println("**************************************")
		fmt.Println("Statslogs:")
		sort.Slice(info.Statslogs, func(i, j int) bool {
			return info.Statslogs[i].FieldID < info.Statslogs[j].FieldID
		})
		for _, log := range info.Statslogs {
			fmt.Printf("Field %d: %v\n", log.FieldID, log.Binlogs)
		}

		fmt.Println("**************************************")
		fmt.Println("Delta Logs:")
		for _, log := range info.Deltalogs {
			for _, l := range log.Binlogs {
				fmt.Printf("Entries: %d From: %v - To: %v\n", l.EntriesNum, l.TimestampFrom, l.TimestampTo)
				fmt.Printf("Path: %v\n", l.LogPath)
			}
		}
	}

	fmt.Println("================================================================================")
}

func countBinlogNum(fbl []*datapb.FieldBinlog) int {
	result := 0
	for _, f := range fbl {
		result += len(f.Binlogs)
	}
	return result
}
