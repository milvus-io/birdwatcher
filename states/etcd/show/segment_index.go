package show

import (
	"context"
	"fmt"

	"github.com/samber/lo"

	"github.com/milvus-io/birdwatcher/framework"
	"github.com/milvus-io/birdwatcher/models"
	"github.com/milvus-io/birdwatcher/states/etcd/common"
	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
)

type SegmentIndexParam struct {
	framework.ParamBase `use:"show segment-index" desc:"display segment index information" alias:"segments-index,segment-indexes,segments-indexes"`
	CollectionID        int64 `name:"collection" default:"0" desc:"collection id to filter with"`
	SegmentID           int64 `name:"segment" default:"0" desc:"segment id to filter with"`
	FieldID             int64 `name:"field" default:"0" desc:"field id to filter with"`
}

// SegmentIndexCommand returns show segment-index command.
func (c *ComponentShow) SegmentIndexCommand(ctx context.Context, p *SegmentIndexParam) error {
	segments, err := common.ListSegments(ctx, c.client, c.metaPath, func(info *models.Segment) bool {
		return (p.CollectionID == 0 || info.CollectionID == p.CollectionID) &&
			(p.SegmentID == 0 || info.ID == p.SegmentID)
	})
	if err != nil {
		return err
	}

	segmentIndexes, err := common.ListSegmentIndex(ctx, c.client, c.metaPath, func(segIdx *models.SegmentIndex) bool {
		return (p.CollectionID == 0 || p.CollectionID == segIdx.GetProto().GetCollectionID()) &&
			(p.SegmentID == 0 || p.SegmentID == segIdx.GetProto().GetSegmentID()) &&
			(p.FieldID == 0 || p.FieldID == segIdx.GetProto().GetIndexID())
	})
	if err != nil {
		return err
	}

	indexBuildInfo, err := common.ListIndex(ctx, c.client, c.metaPath, func(index *models.FieldIndex) bool {
		return p.CollectionID == 0 || p.CollectionID == index.GetProto().GetIndexInfo().GetCollectionID() &&
			(p.SegmentID == 0 || p.SegmentID == index.GetProto().GetIndexInfo().GetFieldID()) &&
			(p.FieldID == 0 || p.FieldID == index.GetProto().GetIndexInfo().GetFieldID())
	})
	if err != nil {
		return err
	}

	seg2Idx := lo.GroupBy(segmentIndexes, func(segIdx *models.SegmentIndex) int64 {
		return segIdx.GetProto().GetSegmentID()
	})

	idIdx := lo.SliceToMap(indexBuildInfo, func(info *models.FieldIndex) (int64, *models.FieldIndex) {
		return info.GetProto().GetIndexInfo().GetIndexID(), info
	})

	count := make(map[string]int)

	for _, segment := range segments {
		if segment.State != commonpb.SegmentState_Flushed && segment.GetState() != commonpb.SegmentState_Flushing {
			continue
		}
		fmt.Printf("SegmentID: %d\t State: %s", segment.GetID(), segment.GetState().String())
		segIdxs, ok := seg2Idx[segment.GetID()]
		if !ok {
			continue
		}
		for _, info := range segIdxs {
			segIdx := info.GetProto()
			index, ok := idIdx[segIdx.GetIndexID()]
			if !ok {
				continue
			}
			fmt.Printf("\n\tIndex build ID: %d, states %s", segIdx.GetBuildID(), segIdx.GetState().String())
			count[segIdx.GetState().String()]++

			fmt.Printf("\t Index Type:%v on Field ID: %d", common.GetKVPair(index.GetProto().GetIndexInfo().GetIndexParams(), "index_type"), index.GetProto().GetIndexInfo().GetFieldID())
			fmt.Printf("\tSerialized Size: %d\n", segIdx.GetSerializeSize())
			fmt.Printf("\tCurrent Index Version: %d\n", segIdx.GetCurrentIndexVersion())
			fmt.Printf("\t Index Files: %v\n", segIdx.IndexFileKeys)
		}
		fmt.Println()
	}
	// Print count statistics
	for idxSta, cnt := range count {
		fmt.Printf("[%s]: %d\t", idxSta, cnt)
	}
	fmt.Println()
	return nil
}
