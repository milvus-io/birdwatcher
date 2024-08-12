package show

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/samber/lo"

	"github.com/milvus-io/birdwatcher/framework"
	"github.com/milvus-io/birdwatcher/models"
	"github.com/milvus-io/birdwatcher/states/etcd/common"
	etcdversion "github.com/milvus-io/birdwatcher/states/etcd/version"
	"github.com/milvus-io/birdwatcher/utils"
)

type SegmentParam struct {
	framework.ParamBase `use:"show segment" desc:"display segment information from data coord meta store" alias:"segments"`
	CollectionID        int64  `name:"collection" default:"0" desc:"collection id to filter with"`
	PartitionID         int64  `name:"partition" default:"0" desc:"partition id to filter with"`
	SegmentID           int64  `name:"segment" default:"0" desc:"segment id to display"`
	Format              string `name:"format" default:"line" desc:"segment display format"`
	Detail              bool   `name:"detail" default:"false" desc:"flags indicating whether printing detail binlog info"`
	State               string `name:"state" default:"" desc:"target segment state"`
	Level               string `name:"level" default:"" desc:"target segment level"`
}

type segStats struct {
	binlogLogSize map[int64]int64
	binlogMemSize map[int64]int64
	deltaLogSize  int64
	deltaMemSize  int64
	statsLogSize  int64
	statsMemSize  int64
}

// SegmentCommand returns show segments command.
func (c *ComponentShow) SegmentCommand(ctx context.Context, p *SegmentParam) error {
	segments, err := common.ListSegmentsVersion(ctx, c.client, c.basePath, etcdversion.GetVersion(), func(segment *models.Segment) bool {
		return (p.CollectionID == 0 || segment.CollectionID == p.CollectionID) &&
			(p.PartitionID == 0 || segment.PartitionID == p.PartitionID) &&
			(p.SegmentID == 0 || segment.ID == p.SegmentID) &&
			(p.State == "" || strings.EqualFold(segment.State.String(), p.State)) &&
			(p.Level == "" || strings.EqualFold(segment.Level.String(), p.Level))
	})
	if err != nil {
		fmt.Println("failed to list segments", err.Error())
		return nil
	}

	totalRC := int64(0)
	healthy := 0

	var growing, sealed, flushed, dropped int
	var small, other int
	var smallCnt, otherCnt int64

	collectionID2SegStats := make(map[int64]*segStats)
	collectionID2Segments := lo.GroupBy(segments, func(s *models.Segment) int64 {
		return s.CollectionID
	})

	for collectionID, segs := range collectionID2Segments {
		fmt.Printf("===============================CollectionID: %d===========================\n", collectionID)
		collectionID2SegStats[collectionID] = &segStats{
			binlogLogSize: make(map[int64]int64),
			binlogMemSize: make(map[int64]int64),
		}

		for _, info := range segs {
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
				if float64(info.NumOfRows)/float64(info.MaxRowNum) < 0.2 {
					small++
					smallCnt += info.NumOfRows
				} else {
					other++
					otherCnt += info.NumOfRows
				}
			case models.SegmentStateDropped:
				dropped++
			}

			switch p.Format {
			case "table":
				PrintSegmentInfo(info, p.Detail)
			case "line":
				fmt.Printf("SegmentID: %d PartitionID: %d State: %s, Level: %s, Row Count:%d,  PartitionStatsVersion:%d \n",
					info.ID, info.PartitionID, info.State.String(), info.Level.String(), info.NumOfRows, info.PartitionStatsVersion)
			case "statistics":
				if info.State != models.SegmentStateDropped {
					for _, binlog := range info.GetBinlogs() {
						for _, log := range binlog.Binlogs {
							collectionID2SegStats[collectionID].binlogLogSize[binlog.FieldID] += log.LogSize
							collectionID2SegStats[collectionID].binlogMemSize[binlog.FieldID] += log.MemSize
						}
					}
					for _, delta := range info.GetDeltalogs() {
						for _, log := range delta.Binlogs {
							collectionID2SegStats[collectionID].deltaLogSize += log.LogSize
							collectionID2SegStats[collectionID].deltaMemSize += log.MemSize
						}
					}
					for _, statslog := range info.GetStatslogs() {
						for _, binlog := range statslog.Binlogs {
							collectionID2SegStats[collectionID].statsLogSize += binlog.LogSize
							collectionID2SegStats[collectionID].statsMemSize += binlog.MemSize
						}
					}
				}
			default:
				err := fmt.Errorf("unsupport format:%s\n", p.Format)
				return err
			}
		}
		if p.Format == "statistics" {
			outputStats("Collection", collectionID2SegStats[collectionID])
		}
		fmt.Printf("\n")
	}

	if p.Format == "statistics" {
		outputStats("Total", lo.Values(collectionID2SegStats)...)
	}

	fmt.Printf("--- Growing: %d, Sealed: %d, Flushed: %d, Dropped: %d\n", growing, sealed, flushed, dropped)
	fmt.Printf("--- Small Segments: %d, row count: %d\t Other Segments: %d, row count: %d\n", small, smallCnt, other, otherCnt)
	fmt.Printf("--- Total Segments: %d, row count: %d\n", healthy, totalRC)
	return nil
}

func outputStats(scope string, stats ...*segStats) {
	var totalBinlogLogSize int64
	var totalBinlogMemSize int64
	var TotalDeltaLogSize int64
	var TotalDeltaMemSize int64
	var TotalStatsLogSize int64
	var TotalStatsMemSize int64
	for _, s := range stats {
		for fieldID, logSize := range s.binlogLogSize {
			memSize := s.binlogMemSize[fieldID]
			if scope != "Total" {
				fmt.Printf("field[%d] binlog size: %s, mem size: %s\n", fieldID, hrSize(logSize), hrSize(memSize))
			}
			totalBinlogLogSize += logSize
			totalBinlogMemSize += memSize
		}

		TotalDeltaLogSize += s.deltaLogSize
		TotalDeltaMemSize += s.deltaMemSize
		TotalStatsLogSize += s.statsLogSize
		TotalStatsMemSize += s.statsMemSize
	}

	fmt.Printf("--- %s binlog size: %s, mem size: %s\n", scope, hrSize(totalBinlogLogSize), hrSize(totalBinlogMemSize))
	fmt.Printf("--- %s deltalog size: %s, mem size: %s\n", scope, hrSize(TotalDeltaLogSize), hrSize(TotalDeltaMemSize))
	fmt.Printf("--- %s statslog size: %s, mem size: %s\n", scope, hrSize(TotalStatsLogSize), hrSize(TotalStatsMemSize))
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
	fmt.Printf("Segment State: %v", info.State)
	if info.State == models.SegmentStateDropped {
		dropTime := time.Unix(0, int64(info.DroppedAt))
		fmt.Printf("\tDropped Time: %s", dropTime.Format(tsPrintFormat))
	}
	fmt.Printf("\tSegment Level: %s", info.Level.String())
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
		var insertmemSize int64
		fmt.Println("**************************************")
		fmt.Println("Binlogs:")
		sort.Slice(info.GetBinlogs(), func(i, j int) bool {
			return info.GetBinlogs()[i].FieldID < info.GetBinlogs()[j].FieldID
		})
		for _, log := range info.GetBinlogs() {
			var fieldLogSize int64
			fmt.Printf("Field %d:\n", log.FieldID)
			for _, binlog := range log.Binlogs {
				fmt.Printf("Path: %s\n", binlog.LogPath)
				tf, _ := utils.ParseTS(binlog.TimestampFrom)
				tt, _ := utils.ParseTS(binlog.TimestampTo)
				fmt.Printf("LogID: %d \t Mem Size: %d \t Log Size: %d \t Entry Num: %d\t TimeRange:%s-%s\n",
					binlog.LogID, binlog.MemSize,
					binlog.LogSize, binlog.EntriesNum,
					tf.Format(tsPrintFormat), tt.Format(tsPrintFormat))
				binlogSize += binlog.LogSize
				insertmemSize += binlog.MemSize
				fieldLogSize += binlog.LogSize
			}
			fmt.Println("--- Field Log Size:", hrSize(fieldLogSize))
		}
		fmt.Println("=== Segment Total Binlog Size: ", hrSize(binlogSize))
		fmt.Println("=== Segment Total Binlog Mem Size: ", hrSize(insertmemSize))

		fmt.Println("**************************************")
		fmt.Println("Statslogs:")
		sort.Slice(info.GetStatslogs(), func(i, j int) bool {
			return info.GetStatslogs()[i].FieldID < info.GetStatslogs()[j].FieldID
		})
		var statsLogSize int64
		for _, log := range info.GetStatslogs() {
			fmt.Printf("Field %d:\n", log.FieldID)
			for _, binlog := range log.Binlogs {
				fmt.Printf("Path: %s\n", binlog.LogPath)
				tf, _ := utils.ParseTS(binlog.TimestampFrom)
				tt, _ := utils.ParseTS(binlog.TimestampTo)
				fmt.Printf("LogID: %d \t Log Size: %d \t Entry Num: %d\t TimeRange:%s-%s\n",
					binlog.LogID, binlog.LogSize, binlog.EntriesNum,
					tf.Format(tsPrintFormat), tt.Format(tsPrintFormat))
				statsLogSize += binlog.LogSize
			}
		}
		fmt.Println("=== Segment Total Statslog Size: ", hrSize(statsLogSize))

		fmt.Println("**************************************")
		fmt.Println("Delta Logs:")
		var deltaLogSize int64
		var memSize int64
		for _, log := range info.GetDeltalogs() {
			for _, l := range log.Binlogs {
				fmt.Printf("Entries: %d From: %v - To: %v\n", l.EntriesNum, l.TimestampFrom, l.TimestampTo)
				fmt.Printf("LogID: %d, Path: %v LogSize: %s, MemSize: %s\n", l.LogID, l.LogPath, hrSize(l.LogSize), hrSize(l.MemSize))
				deltaLogSize += l.LogSize
				memSize += l.MemSize
			}
		}
		fmt.Println("=== Segment Total Deltalog Size: ", hrSize(deltaLogSize))
		fmt.Println("=== Segment Total Deltalog Mem Size: ", hrSize(memSize))
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
