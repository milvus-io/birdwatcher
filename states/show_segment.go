package states

import (
	"context"
	"fmt"
	"path"
	"sort"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/milvus-io/birdwatcher/proto/v2.0/commonpb"
	"github.com/milvus-io/birdwatcher/proto/v2.0/datapb"
	"github.com/milvus-io/birdwatcher/proto/v2.0/internalpb"
	"github.com/milvus-io/birdwatcher/proto/v2.0/querypb"
	"github.com/milvus-io/birdwatcher/storage"
	"github.com/spf13/cobra"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func getEtcdShowSegments(cli *clientv3.Client, basePath string) *cobra.Command {
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

			segments, err := listSegments(cli, basePath, func(info *datapb.SegmentInfo) bool {
				return (collID == 0 || info.CollectionID == collID) &&
					(segmentID == 0 || info.ID == segmentID)
			})
			if err != nil {
				fmt.Println("failed to list segments", err.Error())
				return nil
			}

			totalRC := int64(0)
			healthy := 0
			for _, info := range segments {

				if info.State != commonpb.SegmentState_Dropped {

					totalRC += info.NumOfRows
					healthy++
				}
				switch format {
				case "table":
					printSegmentInfo(info, detail)
				case "line":
					fmt.Printf("SegmentID:%d State: %s, Row Count:%d\n", info.ID, info.State.String(), info.NumOfRows)
				}

			}

			fmt.Printf("--- Total Segments: %d , row count: %d\n", healthy, totalRC)
			return nil
		},
	}
	cmd.Flags().Int64("collection", 0, "collection id to filter with")
	cmd.Flags().String("format", "line", "segment display format")
	cmd.Flags().Bool("detail", false, "flags indicating whether pring detail binlog info")
	cmd.Flags().Int64("segment", 0, "segment id to filter with")
	return cmd
}

func getLoadedSegmentsCmd(cli *clientv3.Client, basePath string) *cobra.Command {
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

			segments, err := listLoadedSegments(cli, basePath, func(info *querypb.SegmentInfo) bool {
				return (collID == 0 || info.CollectionID == collID) &&
					(segmentID == 0 || info.SegmentID == segmentID)
			})
			if err != nil {
				fmt.Println("failed to list segments", err.Error())
				return nil
			}

			for _, info := range segments {
				fmt.Printf("Segment ID: %d LegacyNodeID: %d NodeIds: %v,DmlChannel: %s\n", info.SegmentID, info.NodeID, info.NodeIds, info.DmChannel)
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

func getCheckpointCmd(cli *clientv3.Client, basePath string) *cobra.Command {
	cmd := &cobra.Command{
		Use:     "checkpoint",
		Short:   "list checkpoint collection vchannels",
		Aliases: []string{"checkpoints", "cp"},
		Run: func(cmd *cobra.Command, args []string) {

			collID, err := cmd.Flags().GetInt64("collection")
			if err != nil {
				fmt.Println(err.Error())
				return
			}

			coll, err := getCollectionByID(cli, basePath, collID)
			if err != nil {
				fmt.Println("failed to get collection", err.Error())
				return
			}

			for _, vchannel := range coll.GetVirtualChannelNames() {
				segments, err := listSegments(cli, basePath, func(info *datapb.SegmentInfo) bool {
					return info.CollectionID == collID && info.InsertChannel == vchannel
				})
				if err != nil {
					fmt.Printf("fail to list segment for channel %s, err: %s\n", vchannel, err.Error())
					continue
				}
				fmt.Printf("find segments to list checkpoint for %s, segment found %d\n", vchannel, len(segments))
				var segmentID int64
				var pos *internalpb.MsgPosition
				for _, segment := range segments {
					// skip all empty segment
					if segment.GetDmlPosition() == nil && segment.GetStartPosition() == nil {
						continue
					}
					var segPos *internalpb.MsgPosition

					if segment.GetDmlPosition() != nil {
						segPos = segment.GetDmlPosition()
					} else {
						segPos = segment.GetStartPosition()
					}

					if pos == nil || segPos.GetTimestamp() < pos.GetTimestamp() {
						pos = segPos
						segmentID = segment.GetID()
					}
				}

				if pos == nil {
					fmt.Printf("vchannel %s position nil\n", vchannel)
				} else {
					t, _ := ParseTS(pos.GetTimestamp())
					fmt.Printf("vchannel %s seek to %v, for segment ID:%d \n", vchannel, t, segmentID)
				}
			}
		},
	}
	cmd.Flags().Int64("collection", 0, "collection id to filter with")
	return cmd
}

func cleanEmptySegments(cli *clientv3.Client, basePath string) *cobra.Command {
	cmd := &cobra.Command{
		Use:     "clean-empty-segment",
		Short:   "Remove empty segment from meta",
		Aliases: []string{"segments-loaded"},
		RunE: func(cmd *cobra.Command, args []string) error {
			run, err := cmd.Flags().GetBool("run")
			if err != nil {
				return err
			}
			segments, err := listSegments(cli, basePath, func(info *datapb.SegmentInfo) bool {
				return info.GetState() == commonpb.SegmentState_Flushed || info.GetState() == commonpb.SegmentState_Flushing || info.GetState() == commonpb.SegmentState_Sealed
			})
			if err != nil {
				fmt.Println("failed to list segments", err.Error())
				return nil
			}

			for _, info := range segments {
				if isEmptySegment(info) {
					fmt.Printf("suspect segment %d found:\n", info.GetID())
					printSegmentInfo(info, false)
					if run {
						err := removeSegment(cli, basePath, info)
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

	cmd.Flags().Bool("run", false, "flags indicating whether pring detail binlog info")
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

func listSegments(cli *clientv3.Client, basePath string, filter func(*datapb.SegmentInfo) bool) ([]*datapb.SegmentInfo, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
	defer cancel()
	resp, err := cli.Get(ctx, path.Join(basePath, "datacoord-meta/s")+"/", clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}
	segments := make([]*datapb.SegmentInfo, 0, len(resp.Kvs))
	for _, kv := range resp.Kvs {
		info := &datapb.SegmentInfo{}
		err = proto.Unmarshal(kv.Value, info)
		if err != nil {
			continue
		}
		if filter == nil || filter(info) {
			segments = append(segments, info)
		}
	}
	return segments, nil
}

func removeSegment(cli *clientv3.Client, basePath string, info *datapb.SegmentInfo) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
	defer cancel()

	path := path.Join(basePath, "datacoord-meta/s", fmt.Sprintf("%d/%d/%d", info.CollectionID, info.PartitionID, info.ID))
	_, err := cli.Delete(ctx, path)

	return err
}

func listLoadedSegments(cli *clientv3.Client, basePath string, filter func(*querypb.SegmentInfo) bool) ([]*querypb.SegmentInfo, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
	defer cancel()
	resp, err := cli.Get(ctx, path.Join(basePath, "queryCoord-segmentMeta"), clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}
	segments := make([]*querypb.SegmentInfo, 0, len(resp.Kvs))
	for _, kv := range resp.Kvs {
		info := &querypb.SegmentInfo{}
		err = proto.Unmarshal(kv.Value, info)
		if err != nil {
			continue
		}
		if filter == nil || filter(info) {
			segments = append(segments, info)
		}
	}
	return segments, nil

}

const (
	tsPrintFormat = "2006-01-02 15:04:05.999 -0700"
)

func printSegmentInfo(info *datapb.SegmentInfo, detailBinlog bool) {
	fmt.Println("================================================================================")
	fmt.Printf("Segment ID: %d\n", info.ID)
	fmt.Printf("Segment State:%v\n", info.State)
	fmt.Printf("Collection ID: %d\t\tPartitionID: %d\n", info.CollectionID, info.PartitionID)
	fmt.Printf("Insert Channel:%s\n", info.InsertChannel)
	fmt.Printf("Num of Rows: %d\t\tMax Row Num: %d\n", info.NumOfRows, info.MaxRowNum)
	lastExpireTime, _ := ParseTS(info.LastExpireTime)
	fmt.Printf("Last Expire Time: %s\n", lastExpireTime.Format(tsPrintFormat))
	fmt.Printf("Compact from %v \n", info.CompactionFrom)
	if info.StartPosition != nil {
		startTime, _ := ParseTS(info.GetStartPosition().GetTimestamp())
		fmt.Printf("Start Position ID: %v, time: %s\n", info.StartPosition.MsgID, startTime.Format(tsPrintFormat))
	} else {
		fmt.Println("Start Position: nil")
	}
	if info.DmlPosition != nil {
		dmlTime, _ := ParseTS(info.DmlPosition.Timestamp)
		fmt.Printf("Dml Position ID: %v, time: %s\n", info.DmlPosition.MsgID, dmlTime.Format(tsPrintFormat))
	} else {
		fmt.Println("Dml Position: nil")
	}
	fmt.Printf("Binlog Nums %d\tStatsLog Nums: %d\tDeltaLog Nums:%d\n",
		countBinlogNum(info.Binlogs), countBinlogNum(info.Statslogs), countBinlogNum(info.Deltalogs))

	if detailBinlog {
		fmt.Println("**************************************")
		fmt.Println("Binlogs:")
		sort.Slice(info.Binlogs, func(i, j int) bool {
			return info.Binlogs[i].FieldID < info.Binlogs[j].FieldID
		})
		for _, log := range info.Binlogs {
			fmt.Printf("Field %d: %v\n", log.FieldID, log.Binlogs)
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

func analysisBinlog() {
	r := &storage.ParquetPayloadReader{}
	fmt.Println(r)
}
