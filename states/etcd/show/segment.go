package show

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/samber/lo"

	"github.com/milvus-io/birdwatcher/framework"
	"github.com/milvus-io/birdwatcher/models"
	"github.com/milvus-io/birdwatcher/states/etcd/common"
	"github.com/milvus-io/birdwatcher/utils"
	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
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
	Sorted              string `name:"sorted" default:"" desc:"flags indicating whether sort segments by segmentID"`
	StorageVersion      int64  `name:"storageVersion" default:"-1" desc:"segment storage version to filter"`
}

type segStats struct {
	// field id => log size
	binlogLogSize map[int64]int64
	// field id => mem size
	binlogMemSize map[int64]int64
	deltaLogSize  int64
	deltaMemSize  int64
	deltaEntryNum int64
	statsLogSize  int64
	statsMemSize  int64
}

// SegmentCommand returns show segments command.
func (c *ComponentShow) SegmentCommand(ctx context.Context, p *SegmentParam) (*framework.PresetResultSet, error) {
	segments, err := common.ListSegments(ctx, c.client, c.metaPath, func(segment *models.Segment) bool {
		return (p.CollectionID == 0 || segment.CollectionID == p.CollectionID) &&
			(p.PartitionID == 0 || segment.PartitionID == p.PartitionID) &&
			(p.SegmentID == 0 || segment.ID == p.SegmentID) &&
			(p.State == "" || strings.EqualFold(segment.State.String(), p.State)) &&
			(p.Level == "" || strings.EqualFold(segment.Level.String(), p.Level)) &&
			(p.Sorted == "" || strings.EqualFold(strconv.FormatBool(segment.IsSorted), p.Sorted)) &&
			(p.StorageVersion == -1 || segment.StorageVersion == p.StorageVersion)
	})
	if err != nil {
		return nil, fmt.Errorf("failed to list segments: %w", err)
	}

	rs := &Segments{
		segments: segments,
		format:   p.Format,
		detail:   p.Detail,
	}
	return framework.NewPresetResultSet(rs, framework.NameFormat(p.Format)), nil
}

type Segments struct {
	segments []*models.Segment
	format   string
	detail   bool
}

func (rs *Segments) Entities() any {
	return rs.segments
}

func (rs *Segments) PrintAs(format framework.Format) string {
	switch format {
	case framework.FormatJSON:
		return rs.printAsJSON()
	default:
		return rs.printDefault()
	}
}

func (rs *Segments) printAsJSON() string {
	type SegmentJSON struct {
		SegmentID      int64  `json:"segment_id"`
		CollectionID   int64  `json:"collection_id"`
		PartitionID    int64  `json:"partition_id"`
		State          string `json:"state"`
		Level          string `json:"level"`
		NumOfRows      int64  `json:"num_of_rows"`
		MaxRowNum      int64  `json:"max_row_num"`
		StorageVersion int64  `json:"storage_version"`
		IsSorted       bool   `json:"is_sorted"`
		InsertChannel  string `json:"insert_channel"`
	}

	type SummaryJSON struct {
		Growing      int   `json:"growing"`
		Sealed       int   `json:"sealed"`
		Flushed      int   `json:"flushed"`
		Dropped      int   `json:"dropped"`
		TotalHealthy int   `json:"total_healthy"`
		TotalRows    int64 `json:"total_rows"`
	}

	type OutputJSON struct {
		Segments []SegmentJSON `json:"segments"`
		Summary  SummaryJSON   `json:"summary"`
	}

	var growing, sealed, flushed, dropped, healthy int
	var totalRC int64

	output := OutputJSON{
		Segments: make([]SegmentJSON, 0, len(rs.segments)),
	}

	for _, info := range rs.segments {
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
		case commonpb.SegmentState_Dropped:
			dropped++
		}

		output.Segments = append(output.Segments, SegmentJSON{
			SegmentID:      info.ID,
			CollectionID:   info.CollectionID,
			PartitionID:    info.PartitionID,
			State:          info.State.String(),
			Level:          info.Level.String(),
			NumOfRows:      info.NumOfRows,
			MaxRowNum:      info.MaxRowNum,
			StorageVersion: info.StorageVersion,
			IsSorted:       info.IsSorted,
			InsertChannel:  info.InsertChannel,
		})
	}

	output.Summary = SummaryJSON{
		Growing:      growing,
		Sealed:       sealed,
		Flushed:      flushed,
		Dropped:      dropped,
		TotalHealthy: healthy,
		TotalRows:    totalRC,
	}

	return framework.MarshalJSON(output)
}

func (rs *Segments) printDefault() string {
	sb := &strings.Builder{}

	totalRC := int64(0)
	healthy := 0

	var growing, sealed, flushed, dropped int
	var small, other int
	var smallCnt, otherCnt int64

	collectionID2SegStats := make(map[int64]*segStats)
	collectionID2Segments := lo.GroupBy(rs.segments, func(s *models.Segment) int64 {
		return s.CollectionID
	})

	for collectionID, segs := range collectionID2Segments {
		fmt.Fprintf(sb, "===============================CollectionID: %d===========================\n", collectionID)
		collectionID2SegStats[collectionID] = &segStats{
			binlogLogSize: make(map[int64]int64),
			binlogMemSize: make(map[int64]int64),
		}

		for _, info := range segs {
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
				if float64(info.NumOfRows)/float64(info.MaxRowNum) < 0.2 {
					small++
					smallCnt += info.NumOfRows
				} else {
					other++
					otherCnt += info.NumOfRows
				}
			case commonpb.SegmentState_Dropped:
				dropped++
			}

			switch rs.format {
			case "table":
				printSegmentInfoToBuilder(sb, info, rs.detail)
			case "statistics":
				if info.State != commonpb.SegmentState_Dropped {
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
							collectionID2SegStats[collectionID].deltaEntryNum += log.EntriesNum
						}
					}
					for _, statslog := range info.GetStatslogs() {
						for _, binlog := range statslog.Binlogs {
							collectionID2SegStats[collectionID].statsLogSize += binlog.LogSize
							collectionID2SegStats[collectionID].statsMemSize += binlog.MemSize
						}
					}
				}
			default: // line format
				fmt.Fprintf(sb, "SegmentID: %d PartitionID: %d State: %s, Level: %s, Row Count:%d,  StorageVersion:%d, IsSorted: %v \n",
					info.ID, info.PartitionID, info.State.String(), info.Level.String(), info.NumOfRows, info.StorageVersion, info.IsSorted)
			}
		}
		if rs.format == "statistics" {
			outputStatsToBuilder(sb, "Collection", collectionID2SegStats[collectionID])
		}
		fmt.Fprintf(sb, "\n")
	}

	if rs.format == "statistics" {
		outputStatsToBuilder(sb, "Total", lo.Values(collectionID2SegStats)...)
	}

	fmt.Fprintf(sb, "--- Growing: %d, Sealed: %d, Flushed: %d, Dropped: %d\n", growing, sealed, flushed, dropped)
	fmt.Fprintf(sb, "--- Small Segments: %d, row count: %d\t Other Segments: %d, row count: %d\n", small, smallCnt, other, otherCnt)
	fmt.Fprintf(sb, "--- Total Segments: %d, row count: %d\n", healthy, totalRC)
	return sb.String()
}

func outputStats(scope string, stats ...*segStats) {
	var totalBinlogLogSize int64
	var totalBinlogMemSize int64
	var totalDeltaLogSize int64
	var totalDeltaMemSize int64
	var totalDeltaEntryNum int64
	var totalStatsLogSize int64
	var totalStatsMemSize int64
	for _, s := range stats {
		for fieldID, logSize := range s.binlogLogSize {
			memSize := s.binlogMemSize[fieldID]
			if scope != "Total" {
				fmt.Printf("field[%d] binlog size: %s, mem size: %s\n", fieldID, hrSize(logSize), hrSize(memSize))
			}
			totalBinlogLogSize += logSize
			totalBinlogMemSize += memSize
		}

		totalDeltaLogSize += s.deltaLogSize
		totalDeltaMemSize += s.deltaMemSize
		totalDeltaEntryNum += s.deltaEntryNum
		totalStatsLogSize += s.statsLogSize
		totalStatsMemSize += s.statsMemSize
	}

	fmt.Printf("--- %s binlog size: %s, mem size: %s\n", scope, hrSize(totalBinlogLogSize), hrSize(totalBinlogMemSize))
	fmt.Printf("--- %s deltalog size: %s, mem size: %s, delta entry number: %d\n", scope, hrSize(totalDeltaLogSize), hrSize(totalDeltaMemSize), totalDeltaEntryNum)
	fmt.Printf("--- %s statslog size: %s, mem size: %s\n", scope, hrSize(totalStatsLogSize), hrSize(totalStatsMemSize))
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
	if info.State == commonpb.SegmentState_Dropped {
		dropTime := time.Unix(0, int64(info.DroppedAt))
		fmt.Printf("\tDropped Time: %s", dropTime.Format(tsPrintFormat))
	}
	fmt.Printf("\tSegment Level: %s", info.Level.String())
	fmt.Printf("\tStorage Version: %d", info.GetStorageVersion())
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
	fmt.Printf("Manifest Path: %s\n", info.ManifestPath)
	fmt.Printf("Binlog Nums %d\tStatsLog Nums: %d\tDeltaLog Nums:%d\n",
		countBinlogNum(info.GetBinlogs()), countBinlogNum(info.GetStatslogs()), countBinlogNum(info.GetDeltalogs()))

	if info.JsonKeyStats != nil {
		fmt.Println("Json Key Stats:")
		for _, keyStats := range info.JsonKeyStats {
			fmt.Printf("  Stats info: %v\n", keyStats)
		}
	}

	if detailBinlog {
		var binlogSize int64
		var insertmemSize int64
		var binlogNumber int64
		fmt.Println("**************************************")
		fmt.Println("Binlogs:")
		sort.Slice(info.GetBinlogs(), func(i, j int) bool {
			return info.GetBinlogs()[i].FieldID < info.GetBinlogs()[j].FieldID
		})
		for _, log := range info.GetBinlogs() {
			var fieldLogSize int64
			fmt.Printf("Field %d:\n", log.FieldID)
			if info.GetStorageVersion() != 0 {
				fmt.Printf("[stv2]Child Fields: %v\n", log.ChildFields)
			}
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
			binlogNumber += int64(len(log.Binlogs))
			fmt.Printf("--- Field Log Size: %s\t, Log Count: %d\n", hrSize(fieldLogSize), len(log.Binlogs))
		}
		fmt.Println("=== Segment Total Binlog Number: ", binlogNumber)
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
		var deltaLogNumber int64
		for _, log := range info.GetDeltalogs() {
			for _, l := range log.Binlogs {
				fmt.Printf("Entries: %d From: %v - To: %v\n", l.EntriesNum, l.TimestampFrom, l.TimestampTo)
				fmt.Printf("LogID: %d, Path: %v LogSize: %s, MemSize: %s\n", l.LogID, l.LogPath, hrSize(l.LogSize), hrSize(l.MemSize))
				deltaLogSize += l.LogSize
				memSize += l.MemSize
			}
			deltaLogNumber += int64(len(log.Binlogs))
		}
		fmt.Println("=== Segment Total Deltalog Number: ", deltaLogNumber)
		fmt.Println("=== Segment Total Deltalog Size: ", hrSize(deltaLogSize))
		fmt.Println("=== Segment Total Deltalog Mem Size: ", hrSize(memSize))

		fmt.Println("**************************************")
		fmt.Println("Text stats:")
		var textStatsSize int64
		var textStatsMemSize int64
		for field, textLogInfo := range info.GetTextStatsLogs() {
			fmt.Printf("Field %d\tLogSize: %d\tMemSize: %d\n", field, textLogInfo.GetLogSize(), textLogInfo.GetMemorySize())
			textStatsSize += textLogInfo.GetLogSize()
			textStatsMemSize += textLogInfo.GetMemorySize()
			for _, logFile := range textLogInfo.GetFiles() {
				fmt.Println("\t", logFile)
			}
		}
		fmt.Println("=== Segment Total Textlog Size: ", hrSize(deltaLogSize))
		fmt.Println("=== Segment Total Textlog Mem Size: ", hrSize(memSize))
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

func outputStatsToBuilder(sb *strings.Builder, scope string, stats ...*segStats) {
	var totalBinlogLogSize int64
	var totalBinlogMemSize int64
	var totalDeltaLogSize int64
	var totalDeltaMemSize int64
	var totalDeltaEntryNum int64
	var totalStatsLogSize int64
	var totalStatsMemSize int64
	for _, s := range stats {
		for fieldID, logSize := range s.binlogLogSize {
			memSize := s.binlogMemSize[fieldID]
			if scope != "Total" {
				fmt.Fprintf(sb, "field[%d] binlog size: %s, mem size: %s\n", fieldID, hrSize(logSize), hrSize(memSize))
			}
			totalBinlogLogSize += logSize
			totalBinlogMemSize += memSize
		}

		totalDeltaLogSize += s.deltaLogSize
		totalDeltaMemSize += s.deltaMemSize
		totalDeltaEntryNum += s.deltaEntryNum
		totalStatsLogSize += s.statsLogSize
		totalStatsMemSize += s.statsMemSize
	}

	fmt.Fprintf(sb, "--- %s binlog size: %s, mem size: %s\n", scope, hrSize(totalBinlogLogSize), hrSize(totalBinlogMemSize))
	fmt.Fprintf(sb, "--- %s deltalog size: %s, mem size: %s, delta entry number: %d\n", scope, hrSize(totalDeltaLogSize), hrSize(totalDeltaMemSize), totalDeltaEntryNum)
	fmt.Fprintf(sb, "--- %s statslog size: %s, mem size: %s\n", scope, hrSize(totalStatsLogSize), hrSize(totalStatsMemSize))
}

func printSegmentInfoToBuilder(sb *strings.Builder, info *models.Segment, detailBinlog bool) {
	fmt.Fprintln(sb, "================================================================================")
	fmt.Fprintf(sb, "Segment ID: %d\n", info.ID)
	fmt.Fprintf(sb, "Segment State: %v", info.State)
	if info.State == commonpb.SegmentState_Dropped {
		dropTime := time.Unix(0, int64(info.DroppedAt))
		fmt.Fprintf(sb, "\tDropped Time: %s", dropTime.Format(tsPrintFormat))
	}
	fmt.Fprintf(sb, "\tSegment Level: %s", info.Level.String())
	fmt.Fprintf(sb, "\tStorage Version: %d", info.GetStorageVersion())
	fmt.Fprintln(sb)
	fmt.Fprintf(sb, "Collection ID: %d\t\tPartitionID: %d\n", info.CollectionID, info.PartitionID)
	fmt.Fprintf(sb, "Insert Channel:%s\n", info.InsertChannel)
	fmt.Fprintf(sb, "Num of Rows: %d\t\tMax Row Num: %d\n", info.NumOfRows, info.MaxRowNum)
	lastExpireTime, _ := utils.ParseTS(info.LastExpireTime)
	fmt.Fprintf(sb, "Last Expire Time: %s\n", lastExpireTime.Format(tsPrintFormat))
	fmt.Fprintf(sb, "Compact from %v \n", info.CompactionFrom)
	if info.StartPosition != nil {
		startTime, _ := utils.ParseTS(info.GetStartPosition().GetTimestamp())
		fmt.Fprintf(sb, "Start Position ID: %v, time: %s, channel name %s\n", info.GetStartPosition().MsgID, startTime.Format(tsPrintFormat), info.GetStartPosition().GetChannelName())
	} else {
		fmt.Fprintln(sb, "Start Position: nil")
	}
	if info.DmlPosition != nil {
		dmlTime, _ := utils.ParseTS(info.DmlPosition.Timestamp)
		fmt.Fprintf(sb, "Dml Position ID: %v, time: %s, channel name: %s\n", info.DmlPosition.MsgID, dmlTime.Format(tsPrintFormat), info.GetDmlPosition().GetChannelName())
	} else {
		fmt.Fprintln(sb, "Dml Position: nil")
	}
	fmt.Fprintf(sb, "Manifest Path: %s\n", info.ManifestPath)
	fmt.Fprintf(sb, "Binlog Nums %d\tStatsLog Nums: %d\tDeltaLog Nums:%d\n",
		countBinlogNum(info.GetBinlogs()), countBinlogNum(info.GetStatslogs()), countBinlogNum(info.GetDeltalogs()))

	if info.JsonKeyStats != nil {
		fmt.Fprintln(sb, "Json Key Stats:")
		for _, keyStats := range info.JsonKeyStats {
			fmt.Fprintf(sb, "  Stats info: %v\n", keyStats)
		}
	}
	fmt.Fprintln(sb, "================================================================================")
}
