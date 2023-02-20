package show

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/milvus-io/birdwatcher/models"
	"github.com/milvus-io/birdwatcher/states/etcd/common"
	etcdversion "github.com/milvus-io/birdwatcher/states/etcd/version"
	"github.com/milvus-io/birdwatcher/utils"

	"github.com/spf13/cobra"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// SegmentCommand returns show segments command.
func SegmentCommand(cli clientv3.KV, basePath string) *cobra.Command {
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

			segments, err := common.ListSegmentsVersion(context.Background(), cli, basePath, etcdversion.GetVersion(), func(segment *models.Segment) bool {
				return (collID == 0 || segment.CollectionID == collID) &&
					(segmentID == 0 || segment.ID == segmentID) &&
					(state == "" || segment.State.String() == state)
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

				if info.State != models.SegmentStateDropped {
					totalRC += info.NumOfRows
					healthy++
				}
				switch info.State {
				case models.SegmentStateGrowing:
					growing++
				case models.SegmentStateSealed:
					sealed++
				case models.SegmentStateFlushing, models.SegmentStateFlushed:
					flushed++
				}

				switch format {
				case "table":
					PrintSegmentInfo(info, detail)
				case "line":
					fmt.Printf("SegmentID: %d State: %s, Row Count:%d\n", info.ID, info.State.String(), info.NumOfRows)
				case "statistics":
					if info.State != models.SegmentStateDropped {
						for _, binlog := range info.GetBinlogs() {
							for _, log := range binlog.Binlogs {
								fieldSize[binlog.FieldID] += log.LogSize
							}
						}
						for _, statslog := range info.GetStatslogs() {
							for _, binlog := range statslog.Binlogs {
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
func PrintSegmentInfo(info *models.Segment, detailBinlog bool) {
	fmt.Println("================================================================================")
	fmt.Printf("Segment ID: %d\n", info.ID)
	fmt.Printf("Segment State:%v", info.State)
	if info.State == models.SegmentStateDropped {
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
		fmt.Printf("Start Position ID: %v, time: %s, channel name %s\n", info.GetStartPosition().MsgID, startTime.Format(tsPrintFormat), info.GetStartPosition().GetChannelName())
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
		countBinlogNum(info.GetBinlogs()), countBinlogNum(info.GetStatslogs()), countBinlogNum(info.GetDeltalogs()))

	if detailBinlog {
		var binlogSize int64
		fmt.Println("**************************************")
		fmt.Println("Binlogs:")
		sort.Slice(info.GetBinlogs(), func(i, j int) bool {
			return info.GetBinlogs()[i].FieldID < info.GetBinlogs()[j].FieldID
		})
		for _, log := range info.GetBinlogs() {
			fmt.Printf("Field %d:\n", log.FieldID)
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
		sort.Slice(info.GetStatslogs(), func(i, j int) bool {
			return info.GetStatslogs()[i].FieldID < info.GetStatslogs()[j].FieldID
		})
		for _, log := range info.GetStatslogs() {
			fmt.Printf("Field %d:\n", log.FieldID)
			for _, binlog := range log.Binlogs {
				fmt.Printf("Path: %s\n", binlog.LogPath)
				tf, _ := utils.ParseTS(binlog.TimestampFrom)
				tt, _ := utils.ParseTS(binlog.TimestampTo)
				fmt.Printf("Log Size: %d \t Entry Num: %d\t TimeRange:%s-%s\n",
					binlog.LogSize, binlog.EntriesNum,
					tf.Format(tsPrintFormat), tt.Format(tsPrintFormat))
			}
		}

		fmt.Println("**************************************")
		fmt.Println("Delta Logs:")
		for _, log := range info.GetDeltalogs() {
			for _, l := range log.Binlogs {
				fmt.Printf("Entries: %d From: %v - To: %v\n", l.EntriesNum, l.TimestampFrom, l.TimestampTo)
				fmt.Printf("Path: %v\n", l.LogPath)
			}
		}
	}

	fmt.Println("================================================================================")
}

func countBinlogNum(fbl []*models.FieldBinlog) int {
	result := 0
	for _, f := range fbl {
		result += len(f.Binlogs)
	}
	return result
}
