package repair

import (
	"context"
	"fmt"

	"github.com/milvus-io/birdwatcher/framework"
	"github.com/milvus-io/birdwatcher/models"
	"github.com/milvus-io/birdwatcher/states/etcd/common"
	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
)

type RepairEmptySegmentParam struct {
	framework.ParamBase `use:"repair empty-segment" desc:"Remove empty segment from meta"`

	Run bool `name:"run" default:"false" desc:"flags indicating whether to remove segments from meta"`
}

// EmptySegmentCommand returns repair empty-segment command.
func (c *ComponentRepair) RepairEmptySegmentCommand(ctx context.Context, p *RepairEmptySegmentParam) error {
	segments, err := common.ListSegments(ctx, c.client, c.basePath, func(info *models.Segment) bool {
		return info.GetState() == commonpb.SegmentState_Flushed ||
			info.GetState() == commonpb.SegmentState_Flushing ||
			info.GetState() == commonpb.SegmentState_Sealed
	})
	if err != nil {
		fmt.Println("failed to list segments", err.Error())
		return nil
	}

	for _, info := range segments {
		common.FillFieldsIfV2(c.client, c.basePath, info.SegmentInfo)
		if isEmptySegment(info.SegmentInfo) {
			fmt.Printf("suspect segment %d found:\n", info.GetID())
			fmt.Printf("SegmentID: %d State: %s, Row Count:%d\n", info.ID, info.State.String(), info.NumOfRows)
			if p.Run {
				err := common.RemoveSegment(ctx, c.client, c.basePath, info.SegmentInfo)
				if err == nil {
					fmt.Printf("remove segment %d from meta succeed\n", info.GetID())
				} else {
					fmt.Printf("remove segment %d failed, err: %s\n", info.GetID(), err.Error())
				}
			}
		}
	}

	return nil
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
