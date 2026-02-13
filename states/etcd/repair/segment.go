package repair

import (
	"context"
	"fmt"
	"path"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/birdwatcher/framework"
	"github.com/milvus-io/birdwatcher/states/kv"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
)

type RepairSegmentParam struct {
	framework.ExecutionParam `use:"repair segment" desc:"do segment & index meta check and try to repair"`

	Collection int64 `name:"collection" default:"0" desc:"collection id to filter with"`
	Segment    int64 `name:"segment" default:"0" desc:"segment id to filter with"`
}

// old logic backup
/*
	if !skipDownload {
		minioClient, bucketName, err := getMinioAccess()

		if err != nil {
			fmt.Println("failed to get minio access", err.Error())
		}

		folder := fmt.Sprintf("repair_segment_%s", time.Now().Format("20060102150406"))
		for segmentID, segment := range target {
			fileName := fmt.Sprintf("segment_%d", segmentID)
			oldSeg := targetOld[segmentID]
			f, err := os.Create(path.Join(folder, fileName))
			if err != nil {
				fmt.Println("failed to open file", fileName, err.Error())
				return
			}
			bs, err := proto.Marshal(oldSeg)
			w := bufio.NewWriter(f)
			w.Write(bs)
			w.Flush()
			f.Close()

			index, ok := targetIndex[segmentID]
			if ok {
				fileName = fmt.Sprintf("index_%d", segmentID)
				f, err = os.Create(path.Join(folder, fileName))
				if err != nil {
					fmt.Println("failed to open file", fileName, err.Error())
					return
				}
				bs, err = proto.Marshal(index)
				w := bufio.NewWriter(f)
				w.Write(bs)
				w.Flush()
				f.Close()

			}

			err = downloadSegment(minioClient, bucketName, segment, targetIndex[segmentID], folder)
			if err != nil {
				fmt.Println("failed to download segment", err.Error())
				return
			}
		}
	}*/

// RepairSegmentCommand defines repair segment command.
func (c *ComponentRepair) RepairSegmentCommand(ctx context.Context, p *RepairSegmentParam) error {
	// segments, err := common.ListSegments(ctx, c.client, c.basePath, func(info *datapb.SegmentInfo) bool {
	// 	return (p.Collection == 0 || info.CollectionID == p.Collection) &&
	// 		(p.Segment == 0 || info.ID == p.Segment)
	// })
	// if err != nil {
	// 	return errors.Wrap(err, "failed to list segments")
	// }

	// indexBuildInfo, err := common.ListIndex(ctx, c.client, c.basePath, func(index *models.FieldIndex) bool {
	// 	return (p.Collection == 0 || p.Collection == index.GetProto().GetIndexInfo().GetCollectionID())
	// })
	// if err != nil {
	// 	return errors.Wrap(err, "failed to list indexes")
	// }

	// segmentIndexes, err := common.ListSegmentIndex(ctx, c.client, c.basePath, func(segIdx *models.SegmentIndex) bool {
	// 	return (p.Collection == 0 || p.Collection == segIdx.GetProto().GetCollectionID()) &&
	// 		(p.Segment == 0 || p.Segment == segIdx.GetProto().GetSegmentID())
	// })
	// if err != nil {
	// 	return errors.Wrap(err, "failed to list segment indexes")
	// }

	// seg2Idx := lo.GroupBy(segmentIndexes, func(segIdx *models.SegmentIndex) int64 {
	// 	return segIdx.GetProto().GetSegmentID()
	// }) //make(map[int64][]etcdpb.SegmentIndexInfo)

	// buildID2Info := lo.SliceToMap(segmentIndexes, func(index *models.SegmentIndex) (int64, *models.SegmentIndex) {
	// 	return index.GetProto().GetBuildID(), index
	// })

	// collections := make(map[int64]*models.Collection)

	// targetOld := make(map[int64]*datapb.SegmentInfo)
	// target := make(map[int64]*datapb.SegmentInfo)
	// targetIndex := make(map[int64]*indexpb.IndexMeta)

	// for _, segment := range segments {
	// 	if segment.State != commonpb.SegmentState_Flushed {
	// 		continue
	// 	}
	// 	segIdxs, ok := seg2Idx[segment.GetID()]
	// 	if !ok {
	// 		continue
	// 	}

	// 	coll, ok := collections[segment.CollectionID]
	// 	if !ok {
	// 		coll, err = common.GetCollectionByIDVersion(ctx, c.client, c.basePath, p.Collection)
	// 		if err != nil {
	// 			fmt.Printf("failed to query collection(id=%d) info error: %s", segment.CollectionID, err.Error())
	// 			continue
	// 		}
	// 		collections[segment.CollectionID] = coll
	// 	}

	// 	common.FillFieldsIfV2(c.client, c.basePath, segment.SegmentInfo)

	// 	// for _, segIdx := range segIdxs {
	// 	// 	var valid bool
	// 	// 	// for _, field := range coll.GetProto().Schema.Fields {
	// 	// 	// 	if field.FieldID == segIdx.GetProto(). {
	// 	// 	// 		if field.DataType == models.DataTypeFloatVector || field.DataType == models.DataTypeBinaryVector {
	// 	// 	// 			valid = true
	// 	// 	// 		}
	// 	// 	// 		break
	// 	// 	// 	}
	// 	// 	// }
	// 	// 	// if !valid {
	// 	// 	// 	// skip index if not found or not Vector index
	// 	// 	// 	continue
	// 	// 	// }
	// 	// 	// info, ok := buildID2Info[segIdx.BuildID]
	// 	// 	// if !ok {
	// 	// 	// 	// skip index check for no index found
	// 	// 	// 	continue
	// 	// 	// }
	// 	// 	// updated, found := checkBinlogIndex(segment, &info)
	// 	// 	// if found {
	// 	// 	// 	target[segment.ID] = updated
	// 	// 	// 	targetIndex[segment.ID] = &info
	// 	// 	// 	targetOld[segment.ID] = segment
	// 	// 	// }
	// 	// }
	// }
	// if len(target) == 0 {
	// 	fmt.Println("no error found")
	// 	return nil
	// }
	// if !p.Run {
	// 	return nil
	// }

	// // row count check
	// for segmentID, segment := range target {
	// 	if !integrityCheck(segment) {
	// 		fmt.Printf("generated segment[id=%d] row count not match, repair failed.", segmentID)
	// 		delete(target, segmentID)
	// 	}
	// }

	// // update segment meta, v1 format for now
	// for _, segment := range target {
	// 	err := writeRepairedSegment(c.client, c.basePath, segment)
	// 	if err != nil {
	// 		errors.Wrap(err, "failed to write back repaired segment meta")
	// 	}
	// }
	return nil
}

// func checkBinlogIndex(segment *datapb.SegmentInfo, indexMeta *indexpb.IndexMeta) (*datapb.SegmentInfo, bool) {
// 	indexed := make(map[string]struct{})
// 	for _, path := range indexMeta.GetReq().GetDataPaths() {
// 		indexed[path] = struct{}{}
// 	}

// 	excludedIdx := make(map[int]struct{})

// 	for _, fieldLog := range segment.GetBinlogs() {
// 		if fieldLog.FieldID == indexMeta.GetReq().GetFieldSchema().GetFieldID() {
// 			for idx, binlog := range fieldLog.GetBinlogs() {
// 				_, ok := indexed[binlog.GetLogPath()]
// 				if !ok {
// 					excludedIdx[idx] = struct{}{}
// 					fmt.Printf("[segment=%d]binlog %s in segment but not indexed\n", segment.ID, binlog.LogPath)
// 				}
// 			}
// 		}
// 	}

// 	if len(excludedIdx) == 0 {
// 		return nil, false
// 	}

// 	// remove binlog from field logs
// 	update := proto.Clone(segment).(*datapb.SegmentInfo)
// 	for _, fieldLog := range update.GetBinlogs() {
// 		newbinlogs := make([]*datapb.Binlog, 0, len(fieldLog.GetBinlogs()))
// 		for idx, binlog := range fieldLog.GetBinlogs() {
// 			_, exclude := excludedIdx[idx]
// 			if !exclude {
// 				newbinlogs = append(newbinlogs, binlog)
// 			}
// 		}
// 		fieldLog.Binlogs = newbinlogs
// 	}

// 	return update, true
// }

func integrityCheck(segment *datapb.SegmentInfo) bool {
	var rowCount int64
	// use 0-th field as base
	for _, binlog := range segment.GetBinlogs()[0].GetBinlogs() {
		rowCount += binlog.EntriesNum
	}

	for i := 1; i < len(segment.GetBinlogs()); i++ {
		var frc int64
		for _, binlog := range segment.GetBinlogs()[i].GetBinlogs() {
			frc += binlog.EntriesNum
		}

		if frc != rowCount {
			return false
		}
	}

	return true
}

func writeRepairedSegment(cli kv.MetaKV, basePath string, segment *datapb.SegmentInfo) error {
	p := path.Join(basePath, fmt.Sprintf("datacoord-meta/s/%d/%d/%d", segment.CollectionID, segment.PartitionID, segment.ID))

	bs, err := proto.Marshal(segment)
	if err != nil {
		fmt.Println("failed to marshal segment info", err.Error())
	}
	err = cli.Save(context.Background(), p, string(bs))
	return err
}
